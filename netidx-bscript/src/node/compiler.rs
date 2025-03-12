use crate::{
    env::{Bind, LambdaBind},
    expr::{Expr, ExprId, ExprKind, ModPath},
    node::{
        lambda,
        pattern::{PatternNode, StructPatternNode},
        Cached, Node, NodeKind,
    },
    typ::Type,
    Ctx, ExecCtx, UserEvent,
};
use anyhow::{bail, Result};
use arcstr::ArcStr;
use compact_str::{format_compact, CompactString};
use fxhash::FxHashMap;
use netidx::publisher::Typ;
use smallvec::{smallvec, SmallVec};
use std::{fmt::Debug, hash::Hash, marker::PhantomData};
use triomphe::Arc;

atomic_id!(SelectId);

fn compile_apply_args<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    scope: &ModPath,
    top_id: ExprId,
    args: Arc<[(Option<ArcStr>, Expr)]>,
    lb: &LambdaBind<C, E>,
) -> Result<Box<[Node<C, E>]>> {
    let mut nodes: SmallVec<[Node<C, E>; 16]> = smallvec![];
    let mut named = FxHashMap::default();
    for (name, e) in args.iter() {
        if let Some(name) = name {
            if named.contains_key(name) {
                bail!("duplicate labeled argument {name}")
            }
            named.insert(name.clone(), e.clone());
        }
    }
    for a in lb.argspec.iter() {
        match &a.labeled {
            None => break,
            Some(def) => match named.remove(&a.name) {
                Some(e) => {
                    let n = compile(ctx, e, scope, top_id);
                    if let Some(e) = n.extract_err() {
                        bail!(e);
                    }
                    nodes.push(n)
                }
                None => match def {
                    None => bail!("missing required argument {}", a.name),
                    Some(e) => {
                        let orig_env = ctx.env.restore_lexical_env(&lb.env);
                        let n = compile(ctx, e.clone(), &lb.scope, top_id);
                        ctx.env = ctx.env.merge_lexical(&orig_env);
                        if let Some(e) = n.extract_err() {
                            bail!(e)
                        }
                        nodes.push(n);
                    }
                },
            },
        }
    }
    if named.len() != 0 {
        let s = named.keys().fold(CompactString::new(""), |mut s, n| {
            if s != "" {
                s.push_str(", ");
            }
            s.push_str(n);
            s
        });
        bail!("unknown labeled arguments passed, {s}")
    }
    for (name, e) in args.iter() {
        if name.is_none() {
            let n = compile(ctx, e.clone(), scope, top_id);
            if let Some(e) = n.extract_err() {
                bail!(e)
            }
            nodes.push(n);
        }
    }
    if nodes.len() < lb.argspec.len() {
        bail!("missing required argument {}", lb.argspec[nodes.len()].name)
    }
    Ok(Box::from_iter(nodes))
}

fn compile_apply<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    spec: Expr,
    scope: &ModPath,
    top_id: ExprId,
    args: Arc<[(Option<ArcStr>, Expr)]>,
    f: ModPath,
) -> Node<C, E> {
    macro_rules! error {
        ("", $children:expr) => {{
            let kind = NodeKind::Error { error: None, children: Box::from_iter($children) };
            Node { spec: Box::new(spec), kind, typ: ctx.env.bottom() }
        }};
        ($fmt:expr, $children:expr, $($arg:expr),*) => {{
            let e = ArcStr::from(format_compact!($fmt, $($arg),*).as_str());
            let kind = NodeKind::Error { error: Some(e), children: Box::from_iter($children) };
            Node { spec: Box::new(spec), kind, typ: Type::Bottom(PhantomData) }
        }};
        ($fmt:expr) => { error!($fmt, [],) };
        ($fmt:expr, $children:expr) => { error!($fmt, $children,) };
    }
    match ctx.env.lookup_bind(scope, &f) {
        None => error!("{f} is undefined"),
        Some((_, Bind { fun: None, .. })) => {
            error!("{f} is not a function")
        }
        Some((_, Bind { fun: Some(lb), id, .. })) => {
            let varid = *id;
            let lb = lb.clone();
            let args = match compile_apply_args(ctx, scope, top_id, args, &lb) {
                Err(e) => return error!("{e}"),
                Ok(a) => a,
            };
            match (lb.init)(ctx, &args, top_id) {
                Err(e) => error!("error in function {f} {e:?}"),
                Ok(function) => {
                    ctx.user.ref_var(varid, top_id);
                    let typ = function.rtype().clone();
                    let kind = NodeKind::Apply { args, function };
                    Node { spec: Box::new(spec), typ, kind }
                }
            }
        }
    }
}

pub(super) fn compile<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    spec: Expr,
    scope: &ModPath,
    top_id: ExprId,
) -> Node<C, E> {
    macro_rules! subexprs {
        ($scope:expr, $exprs:expr) => {
            $exprs.iter().fold((false, vec![]), |(e, mut nodes), spec| {
                let n = compile(ctx, spec.clone(), &$scope, top_id);
                let e = e || n.is_err();
                nodes.push(n);
                (e, nodes)
            })
        };
    }
    macro_rules! error {
        ("", $children:expr) => {{
            let kind = NodeKind::Error { error: None, children: Box::from_iter($children) };
            Node { spec: Box::new(spec), kind, typ: Type::Bottom(PhantomData) }
        }};
        ($fmt:expr, $children:expr, $($arg:expr),*) => {{
            let e = ArcStr::from(format_compact!($fmt, $($arg),*).as_str());
            let kind = NodeKind::Error { error: Some(e), children: Box::from_iter($children) };
            Node { spec: Box::new(spec), kind, typ: Type::Bottom(PhantomData) }
        }};
        ($fmt:expr) => { error!($fmt, [],) };
        ($fmt:expr, $children:expr) => { error!($fmt, $children,) };
    }
    macro_rules! binary_op {
        ($op:ident, $lhs:expr, $rhs:expr) => {{
            let lhs = compile(ctx, (**$lhs).clone(), scope, top_id);
            let rhs = compile(ctx, (**$rhs).clone(), scope, top_id);
            if lhs.is_err() || rhs.is_err() {
                return error!("", [lhs, rhs]);
            }
            let lhs = Box::new(Cached::new(lhs));
            let rhs = Box::new(Cached::new(rhs));
            Node {
                spec: Box::new(spec),
                typ: Type::empty_tvar(),
                kind: NodeKind::$op { lhs, rhs },
            }
        }};
    }
    match &spec {
        Expr { kind: ExprKind::Constant(v), id: _ } => {
            let typ = Type::Primitive(Typ::get(&v).into());
            Node { kind: NodeKind::Constant(v.clone()), spec: Box::new(spec), typ }
        }
        Expr { kind: ExprKind::Do { exprs }, id } => {
            let scope = ModPath(scope.append(&format_compact!("do{}", id.inner())));
            let (error, exp) = subexprs!(scope, exprs);
            if error {
                error!("", exp)
            } else {
                let typ = exp
                    .last()
                    .map(|n| n.typ.clone())
                    .unwrap_or_else(|| Type::Bottom(PhantomData));
                Node { kind: NodeKind::Do(Box::from(exp)), spec: Box::new(spec), typ }
            }
        }
        Expr { kind: ExprKind::Array { args }, id: _ } => {
            let (error, args) = subexprs!(scope, args);
            if error {
                error!("", args)
            } else {
                let typ = Type::empty_tvar();
                let args = Box::from_iter(args.into_iter().map(|n| Cached::new(n)));
                Node { kind: NodeKind::Array { args }, spec: Box::new(spec), typ }
            }
        }
        Expr { kind: ExprKind::Tuple { args }, id: _ } => {
            let (error, args) = subexprs!(scope, args);
            if error {
                error!("", args)
            } else {
                let typ = Type::Tuple(Arc::from_iter(args.iter().map(|n| n.typ.clone())));
                let args = Box::from_iter(args.into_iter().map(|n| Cached::new(n)));
                Node { kind: NodeKind::Tuple { args }, spec: Box::new(spec), typ }
            }
        }
        Expr { kind: ExprKind::Struct { args }, id: _ } => {
            let mut error = false;
            let mut names = Vec::with_capacity(args.len());
            let mut nodes = Vec::with_capacity(args.len());
            for (name, spec) in args.iter() {
                let n = compile(ctx, spec.clone(), &scope, top_id);
                error |= n.is_err();
                names.push(name.clone());
                nodes.push(Cached::new(n));
            }
            if error {
                return error!("", nodes.into_iter().map(|c| c.node).collect::<Vec<_>>());
            }
            let names: Box<[ArcStr]> = Box::from(names);
            let args: Box<[Cached<C, E>]> = Box::from(nodes);
            let typs = names
                .iter()
                .zip(args.iter())
                .map(|(n, a)| (n.clone(), a.node.typ.clone()));
            let typ = Type::Struct(Arc::from_iter(typs));
            Node { kind: NodeKind::Struct { args, names }, spec: Box::new(spec), typ }
        }
        Expr { kind: ExprKind::Module { name, export: _, value }, id: _ } => {
            let scope = ModPath(scope.append(&name));
            match value {
                None => error!("module loading is not implemented"),
                Some(exprs) => {
                    let (error, children) = subexprs!(scope, exprs);
                    if error {
                        error!("", children)
                    } else {
                        ctx.env.modules.insert_cow(scope.clone());
                        let typ = Type::Bottom(PhantomData);
                        let kind = NodeKind::Module(Box::from(children));
                        Node { spec: Box::new(spec), typ, kind }
                    }
                }
            }
        }
        Expr { kind: ExprKind::Use { name }, id: _ } => {
            if !ctx.env.modules.contains(name) {
                error!("no such module {name}")
            } else {
                let used = ctx.env.used.get_or_default_cow(scope.clone());
                Arc::make_mut(used).push(name.clone());
                let kind = NodeKind::Use;
                Node { spec: Box::new(spec), typ: Type::Bottom(PhantomData), kind }
            }
        }
        Expr { kind: ExprKind::Connect { name, value }, id: _ } => {
            let id = match ctx.env.lookup_bind(scope, name) {
                None => return error!("{name} is undefined"),
                Some((_, Bind { fun: Some(_), .. })) => {
                    return error!("{name} is a function")
                }
                Some((_, Bind { id, fun: None, .. })) => *id,
            };
            let node = compile(ctx, (**value).clone(), scope, top_id);
            if node.is_err() {
                return error!("", vec![node]);
            }
            let kind = NodeKind::Connect(id, Box::new(node));
            let typ = Type::Bottom(PhantomData);
            Node { spec: Box::new(spec), typ, kind }
        }
        Expr { kind: ExprKind::Lambda { args, vargs, rtype, constraints, body }, id } => {
            let (args, vargs, rtype, constraints, body, id) = (
                args.clone(),
                vargs.clone(),
                rtype.clone(),
                constraints.clone(),
                (*body).clone(),
                *id,
            );
            lambda::compile(ctx, spec, args, vargs, rtype, constraints, scope, body, id)
        }
        Expr { kind: ExprKind::Apply { args, function: f }, id: _ } => {
            let (args, f) = (args.clone(), f.clone());
            compile_apply(ctx, spec, scope, top_id, args, f)
        }
        Expr { kind: ExprKind::Bind { pattern, typ, export: _, value }, id: _ } => {
            let node = compile(ctx, (**value).clone(), &scope, top_id);
            if node.is_err() {
                return error!("", vec![node]);
            }
            let typ = match typ {
                None => pattern.infer_type_predicate(),
                Some(typ) => match typ.resolve_typrefs(scope, &ctx.env) {
                    Ok(typ) => typ.clone(),
                    Err(e) => return error!("{e}", vec![node]),
                },
            };
            let pn = match StructPatternNode::compile(ctx, &typ, pattern, scope) {
                Ok(p) => p,
                Err(e) => return error!("{e:?}", vec![node]),
            };
            if pn.is_refutable() {
                return error!("refutable patterns are not allowed in let", vec![node]);
            }
            if let Some(l) = node.find_lambda() {
                match pn.lambda_ok() {
                    Some(id) => ctx.env.by_id[&id].fun = Some(l),
                    None => return error!("can't bind lambda to {pattern}", vec![node]),
                }
            };
            let kind = NodeKind::Bind(Box::new(pn), Box::new(node));
            Node { spec: Box::new(spec), typ, kind }
        }
        Expr { kind: ExprKind::Qop(e), id: _ } => {
            let n = compile(ctx, (**e).clone(), scope, top_id);
            if n.is_err() {
                return error!("", vec![n]);
            }
            match ctx.env.lookup_bind(scope, &ModPath::from(["errors"])) {
                None => error!("BUG: errors is undefined"),
                Some((_, bind)) => {
                    let typ = Type::empty_tvar();
                    let spec = Box::new(spec);
                    Node { spec, typ, kind: NodeKind::Qop(bind.id, Box::new(n)) }
                }
            }
        }
        Expr { kind: ExprKind::Ref { name }, id: _ } => {
            match ctx.env.lookup_bind(scope, name) {
                None => error!("{name} not defined"),
                Some((_, bind)) => {
                    ctx.user.ref_var(bind.id, top_id);
                    let typ = bind.typ.clone();
                    let spec = Box::new(spec);
                    match &bind.fun {
                        None => Node { spec, typ, kind: NodeKind::Ref(bind.id) },
                        Some(i) => Node { spec, typ, kind: NodeKind::Lambda(i.clone()) },
                    }
                }
            }
        }
        Expr { kind: ExprKind::TupleRef { name, field }, id: _ } => {
            match ctx.env.lookup_bind(scope, name) {
                None => error!("{name} not defined"),
                Some((_, bind)) => match &bind.fun {
                    Some(_) => error!("can't deref a function"),
                    None => {
                        ctx.user.ref_var(bind.id, top_id);
                        let field = *field;
                        let typ = Type::empty_tvar();
                        let spec = Box::new(spec);
                        Node { spec, typ, kind: NodeKind::TupleRef(bind.id, field) }
                    }
                },
            }
        }
        Expr { kind: ExprKind::StructRef { name, field: _ }, id: _ } => {
            match ctx.env.lookup_bind(scope, name) {
                None => error!("{name} not defined"),
                Some((_, bind)) => match &bind.fun {
                    Some(_) => error!("can't deref a function"),
                    None => {
                        ctx.user.ref_var(bind.id, top_id);
                        let typ = Type::empty_tvar();
                        let spec = Box::new(spec);
                        // typcheck will resolve the field index
                        Node { spec, typ, kind: NodeKind::StructRef(bind.id, 0) }
                    }
                },
            }
        }
        Expr { kind: ExprKind::StructWith { name, replace }, id: _ } => {
            let mut error = false;
            let mut nodes = Vec::with_capacity(replace.len());
            for (_, e) in replace.iter() {
                let n = compile(ctx, e.clone(), scope, top_id);
                error |= n.is_err();
                nodes.push((0, Cached::new(n)));
            }
            if error {
                let nodes: Vec<_> = nodes.into_iter().map(|(_, c)| c.node).collect();
                return error!("", nodes);
            }
            match ctx.env.lookup_bind(scope, name) {
                None => error!("{name} not defined"),
                Some((_, bind)) => match &bind.fun {
                    Some(_) => error!("expected a struct, got a lambda"),
                    None => {
                        ctx.user.ref_var(bind.id, top_id);
                        let name = bind.id;
                        let typ = Type::empty_tvar();
                        let spec = Box::new(spec);
                        let replace = Box::from(nodes);
                        let kind = NodeKind::StructWith { current: None, name, replace };
                        Node { spec, typ, kind }
                    }
                },
            }
        }
        Expr { kind: ExprKind::Select { arg, arms }, id: _ } => {
            let arg = compile(ctx, (**arg).clone(), scope, top_id);
            if let Some(e) = arg.extract_err() {
                return error!("{e}");
            }
            let arg = Box::new(Cached::new(arg));
            let (error, arms) =
                arms.iter().fold((false, vec![]), |(e, mut nodes), (pat, spec)| {
                    let scope = ModPath(
                        scope.append(&format_compact!("sel{}", SelectId::new().0)),
                    );
                    let pat = PatternNode::compile(ctx, pat, &scope, top_id);
                    let n = compile(ctx, spec.clone(), &scope, top_id);
                    let e = e
                        || pat.is_err()
                        || pat.as_ref().unwrap().extract_err().is_some()
                        || n.is_err();
                    nodes.push((pat, Cached::new(n)));
                    (e, nodes)
                });
            use std::fmt::Write;
            let mut err = CompactString::new("");
            if error {
                let mut v = vec![];
                for (pat, n) in arms {
                    match pat {
                        Err(e) => write!(err, "{e}, ").unwrap(),
                        Ok(p) => {
                            if let Some(e) = p.extract_err() {
                                write!(err, "{e}, ").unwrap();
                            }
                            if let Some(g) = p.guard {
                                v.push(g.node);
                            }
                        }
                    }
                    v.push(n.node)
                }
                return error!("{err}", v);
            }
            let arms = Box::from_iter(arms.into_iter().map(|(p, n)| (p.unwrap(), n)));
            let kind = NodeKind::Select { selected: None, arg, arms };
            Node { spec: Box::new(spec), typ: Type::empty_tvar(), kind }
        }
        Expr { kind: ExprKind::TypeCast { expr, typ }, id: _ } => {
            let n = compile(ctx, (**expr).clone(), scope, top_id);
            if n.is_err() {
                return error!("", vec![n]);
            }
            let typ = match typ.resolve_typrefs(scope, &ctx.env) {
                Err(e) => return error!("{e}", vec![n]),
                Ok(typ) => typ,
            };
            if let Err(e) = typ.check_cast() {
                return error!("{e}", vec![n]);
            }
            let rtyp = typ.union(&Type::Primitive(Typ::Error.into()));
            let kind = NodeKind::TypeCast { target: typ, n: Box::new(n) };
            Node { spec: Box::new(spec), typ: rtyp, kind }
        }
        Expr { kind: ExprKind::TypeDef { name, typ }, id: _ } => {
            match typ.resolve_typrefs(scope, &ctx.env) {
                Err(e) => error!("{e}"),
                Ok(typ) => match ctx.env.deftype(scope, name, typ) {
                    Err(e) => error!("{e}"),
                    Ok(()) => {
                        let spec = Box::new(spec);
                        let typ = Type::Bottom(PhantomData);
                        Node { spec, typ, kind: NodeKind::TypeDef }
                    }
                },
            }
        }
        Expr { kind: ExprKind::Not { expr }, id: _ } => {
            let node = compile(ctx, (**expr).clone(), scope, top_id);
            if node.is_err() {
                return error!("", vec![node]);
            }
            let node = Box::new(node);
            let spec = Box::new(spec);
            let typ = Type::Primitive(Typ::Bool.into());
            Node { spec, typ, kind: NodeKind::Not { node } }
        }
        Expr { kind: ExprKind::Eq { lhs, rhs }, id: _ } => binary_op!(Eq, lhs, rhs),
        Expr { kind: ExprKind::Ne { lhs, rhs }, id: _ } => binary_op!(Ne, lhs, rhs),
        Expr { kind: ExprKind::Lt { lhs, rhs }, id: _ } => binary_op!(Lt, lhs, rhs),
        Expr { kind: ExprKind::Gt { lhs, rhs }, id: _ } => binary_op!(Gt, lhs, rhs),
        Expr { kind: ExprKind::Lte { lhs, rhs }, id: _ } => binary_op!(Lte, lhs, rhs),
        Expr { kind: ExprKind::Gte { lhs, rhs }, id: _ } => binary_op!(Gte, lhs, rhs),
        Expr { kind: ExprKind::And { lhs, rhs }, id: _ } => binary_op!(And, lhs, rhs),
        Expr { kind: ExprKind::Or { lhs, rhs }, id: _ } => binary_op!(Or, lhs, rhs),
        Expr { kind: ExprKind::Add { lhs, rhs }, id: _ } => binary_op!(Add, lhs, rhs),
        Expr { kind: ExprKind::Sub { lhs, rhs }, id: _ } => binary_op!(Sub, lhs, rhs),
        Expr { kind: ExprKind::Mul { lhs, rhs }, id: _ } => binary_op!(Mul, lhs, rhs),
        Expr { kind: ExprKind::Div { lhs, rhs }, id: _ } => binary_op!(Div, lhs, rhs),
    }
}
