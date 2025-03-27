use crate::{
    env::{Bind, LambdaBind},
    expr::{ApplyKind, Expr, ExprId, ExprKind, ModPath},
    node::{
        lambda,
        pattern::{PatternNode, StructPatternNode},
        ApplyLate, Cached, Node, NodeKind,
    },
    typ::{FnType, NoRefs, Type},
    Ctx, ExecCtx, UserEvent,
};
use anyhow::{bail, Result};
use arcstr::{literal, ArcStr};
use compact_str::{format_compact, CompactString};
use fxhash::FxHashMap;
use netidx::publisher::{Typ, Value};
use smallvec::{smallvec, SmallVec};
use std::{
    collections::hash_map::Entry, fmt::Debug, hash::Hash, marker::PhantomData,
    sync::Arc as SArc,
};
use triomphe::Arc;

atomic_id!(SelectId);

fn check_named_args(
    named: &mut FxHashMap<ArcStr, Expr>,
    args: &[(Option<ArcStr>, Expr)],
) -> Result<()> {
    for (name, e) in args.iter() {
        if let Some(name) = name {
            match named.entry(name.clone()) {
                Entry::Occupied(e) => bail!("duplicate labeled argument {}", e.key()),
                Entry::Vacant(en) => en.insert(e.clone()),
            };
        }
    }
    Ok(())
}

fn check_extra_named(named: &FxHashMap<ArcStr, Expr>) -> Result<()> {
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
    Ok(())
}

fn compile_late_apply_args<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    scope: &ModPath,
    top_id: ExprId,
    typ: &FnType<NoRefs>,
    args: &Arc<[(Option<ArcStr>, Expr)]>,
) -> Result<(Vec<Node<C, E>>, FxHashMap<ArcStr, bool>)> {
    macro_rules! compile {
        ($e:expr) => {{
            let n = compile(ctx, $e, scope, top_id);
            if let Some(e) = n.extract_err() {
                bail!(e)
            }
            n
        }};
    }
    let mut named = FxHashMap::default();
    let mut nodes: Vec<Node<C, E>> = vec![];
    let mut arg_spec: FxHashMap<ArcStr, bool> = FxHashMap::default();
    named.clear();
    check_named_args(&mut named, args)?;
    for a in typ.args.iter() {
        match &a.label {
            None => break,
            Some((n, required)) => match named.remove(n) {
                Some(e) => {
                    nodes.push(compile!(e));
                    arg_spec.insert(n.clone(), false);
                }
                None if *required => bail!("missing required argument {n}"),
                None => {
                    let node = Node {
                        spec: Box::new(
                            ExprKind::Constant(Value::String(literal!("nop"))).to_expr(),
                        ),
                        kind: NodeKind::Error {
                            error: None,
                            children: Box::from_iter([]),
                        },
                        typ: a.typ.clone(),
                    };
                    nodes.push(node);
                    arg_spec.insert(n.clone(), true);
                }
            },
        }
    }
    check_extra_named(&named)?;
    for (name, e) in args.iter() {
        if name.is_none() {
            nodes.push(compile!(e.clone()));
        }
    }
    if nodes.len() < typ.args.len() {
        bail!("missing required argument")
    }
    Ok((nodes, arg_spec))
}

fn compile_apply_args<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    scope: &ModPath,
    top_id: ExprId,
    args: Arc<[(Option<ArcStr>, Expr)]>,
    lb: &LambdaBind<C, E>,
) -> Result<Box<[Node<C, E>]>> {
    macro_rules! compile {
        ($e:expr) => {{
            let n = compile(ctx, $e, scope, top_id);
            if let Some(e) = n.extract_err() {
                bail!(e)
            }
            n
        }};
    }
    let mut named = FxHashMap::default();
    let mut nodes: SmallVec<[Node<C, E>; 16]> = smallvec![];
    named.clear();
    check_named_args(&mut named, &args)?;
    for a in lb.argspec.iter() {
        match &a.labeled {
            None => break,
            Some(def) => match a.pattern.single_bind().and_then(|n| named.remove(n)) {
                Some(e) => nodes.push(compile!(e)),
                None => match def {
                    None => {
                        bail!("missing required argument {}", a.pattern)
                    }
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
    check_extra_named(&named)?;
    for (name, e) in args.iter() {
        if name.is_none() {
            nodes.push(compile!(e.clone()))
        }
    }
    if nodes.len() < lb.argspec.len() {
        bail!("missing required argument {}", lb.argspec[nodes.len()].pattern)
    }
    Ok(Box::from_iter(nodes))
}

fn compile_apply<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    spec: Expr,
    scope: &ModPath,
    top_id: ExprId,
    args: Arc<[(Option<ArcStr>, Expr)]>,
    f: ApplyKind,
) -> Node<C, E> {
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
    macro_rules! not_a_function {
        ($typ:expr) => {
            return error!(
                "{f} of type {} is not known to be a function, type annotations needed?",
                vec![],
                $typ
            )
        };
    }
    let fnode = compile(ctx, f.to_expr(), scope, top_id);
    if fnode.is_err() {
        return error!("", [fnode]);
    }
    let (typ, fun) = match &fnode.kind {
        NodeKind::Lambda(lb) => (Type::Fn(lb.typ.clone()), Some(lb.clone())),
        NodeKind::Ref { id, .. } => match ctx.env.by_id.get(id) {
            None => return error!("unknown function {f}"),
            Some(b) => (b.typ.clone(), b.fun.as_ref().and_then(|f| f.upgrade())),
        },
        NodeKind::StructRef { id, field: fid, .. } => match ctx.env.by_id.get(id) {
            None => return error!("unknown function {f}"),
            Some(b) => match &b.typ {
                Type::Struct(flds) => match flds.get(*fid) {
                    None => not_a_function!(&b.typ),
                    Some((_, typ)) => (typ.clone(), None),
                },
                _ => not_a_function!(&b.typ),
            },
        },
        NodeKind::TupleRef { id, field: fid, .. } => match ctx.env.by_id.get(&id) {
            None => return error!("unknown function {f}"),
            Some(b) => match &b.typ {
                Type::Tuple(flds) => match flds.get(*fid) {
                    None => not_a_function!(&b.typ),
                    Some(typ) => (typ.clone(), None),
                },
                _ => not_a_function!(&b.typ),
            },
        },
        _ => {
            not_a_function!(&fnode.typ)
        }
    };
    match (typ, fun) {
        (Type::Fn(ftype), None) => {
            match compile_late_apply_args(ctx, scope, top_id, &ftype, &args) {
                Err(e) => error!("{e}"),
                Ok((args, arg_spec)) => {
                    let late = Box::new(ApplyLate {
                        ftype: ftype.clone(),
                        args,
                        arg_spec,
                        fnode,
                        function: None,
                        top_id,
                    });
                    let typ = Type::Fn(ftype);
                    Node { spec: Box::new(spec), typ, kind: NodeKind::ApplyLate(late) }
                }
            }
        }
        (typ, None) => not_a_function!(typ),
        (_, Some(lb)) => {
            let args = match compile_apply_args(ctx, scope, top_id, args, &lb) {
                Err(e) => return error!("{e}"),
                Ok(a) => a,
            };
            match (lb.init)(ctx, &args, top_id) {
                Err(e) => error!("error in function {f} {e:?}"),
                Ok(function) => {
                    let typ = function.typ().rtype.clone();
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
            $exprs.into_iter().fold((false, vec![]), |(e, mut nodes), spec| {
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
        Expr { kind: ExprKind::Variant { tag, args }, id: _ } => {
            let (error, args) = subexprs!(scope, args);
            if error {
                error!("", args)
            } else {
                let typs = Arc::from_iter(args.iter().map(|n| n.typ.clone()));
                let typ = Type::Variant(tag.clone(), typs);
                let args = Box::from_iter(args.into_iter().map(|n| Cached::new(n)));
                let tag = tag.clone();
                Node { kind: NodeKind::Variant { tag, args }, spec: Box::new(spec), typ }
            }
        }
        Expr { kind: ExprKind::Struct { args }, id: _ } => {
            let mut names = Vec::with_capacity(args.len());
            let args = args.iter().map(|(n, s)| {
                names.push(n.clone());
                s
            });
            let (error, nodes) = subexprs!(scope, args);
            if error {
                return error!("", nodes);
            }
            let names: Box<[ArcStr]> = Box::from(names);
            let args: Box<[Cached<C, E>]> =
                Box::from_iter(nodes.into_iter().map(|n| Cached::new(n)));
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
                let kind = NodeKind::Use { scope: scope.clone(), name: name.clone() };
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
        Expr {
            kind: ExprKind::Lambda { args, vargs, rtype, constraints, body },
            id: _,
        } => {
            let (args, vargs, rtype, constraints, body) = (
                args.clone(),
                vargs.clone(),
                rtype.clone(),
                constraints.clone(),
                (*body).clone(),
            );
            lambda::compile(ctx, spec, args, vargs, rtype, constraints, scope, body)
        }
        Expr { kind: ExprKind::Any { args }, id: _ } => {
            let (error, children) = subexprs!(scope, args);
            if error {
                error!("", children)
            } else {
                let kind = NodeKind::Any { args: Box::from(children) };
                Node { spec: Box::new(spec), typ: Type::empty_tvar(), kind }
            }
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
                Some(typ) => match typ.resolve_typerefs(scope, &ctx.env) {
                    Ok(typ) => typ.clone(),
                    Err(e) => return error!("{e}", vec![node]),
                },
                None => {
                    let typ = node.typ.clone();
                    let ptyp = pattern.infer_type_predicate();
                    if !ptyp.contains(&typ) {
                        return error!(
                            "match error {typ} can't be matched by {ptyp}",
                            vec![node]
                        );
                    }
                    typ
                }
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
                    Some(id) => ctx.env.by_id[&id].fun = Some(SArc::downgrade(&l)),
                    None => return error!("can't bind lambda to {pattern}", vec![node]),
                }
            };
            let kind = NodeKind::Bind { pattern: Box::new(pn), node: Box::new(node) };
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
                    Node {
                        spec: Box::new(spec),
                        typ,
                        kind: NodeKind::Ref { id: bind.id, top_id },
                    }
                }
            }
        }
        Expr { kind: ExprKind::TupleRef { name, field }, id: _ } => {
            match ctx.env.lookup_bind(scope, name) {
                None => error!("{name} not defined"),
                Some((_, bind)) => {
                    ctx.user.ref_var(bind.id, top_id);
                    let field = *field;
                    let typ = match &bind.typ {
                        Type::Tuple(ts) => ts
                            .get(field)
                            .map(|t| t.clone())
                            .unwrap_or_else(Type::empty_tvar),
                        _ => Type::empty_tvar(),
                    };
                    let spec = Box::new(spec);
                    let kind = NodeKind::TupleRef { id: bind.id, field, top_id };
                    Node { spec, typ, kind }
                }
            }
        }
        Expr { kind: ExprKind::StructRef { name, field }, id: _ } => {
            match ctx.env.lookup_bind(scope, name) {
                None => error!("{name} not defined"),
                Some((_, bind)) => {
                    ctx.user.ref_var(bind.id, top_id);
                    let (typ, fid) = match &bind.typ {
                        Type::Struct(flds) => flds
                            .iter()
                            .enumerate()
                            .find_map(|(i, (n, t))| {
                                if field == n {
                                    Some((t.clone(), i))
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_else(|| (Type::empty_tvar(), 0)),
                        _ => (Type::empty_tvar(), 0),
                    };
                    let spec = Box::new(spec);
                    // typcheck will resolve the field index if we didn't find it already
                    let kind = NodeKind::StructRef { id: bind.id, field: fid, top_id };
                    Node { spec, typ, kind }
                }
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
            let typ = match typ.resolve_typerefs(scope, &ctx.env) {
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
            match typ.resolve_typerefs(scope, &ctx.env) {
                Err(e) => error!("{e}"),
                Ok(typ) => match ctx.env.deftype(scope, name, typ) {
                    Err(e) => error!("{e}"),
                    Ok(()) => {
                        let name = name.clone();
                        let spec = Box::new(spec);
                        let typ = Type::Bottom(PhantomData);
                        Node {
                            spec,
                            typ,
                            kind: NodeKind::TypeDef { scope: scope.clone(), name },
                        }
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
