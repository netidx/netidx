use crate::{
    env::Bind,
    expr::{self, Expr, ExprId, ExprKind, Lambda, ModPath, ModuleKind},
    node::{
        lambda,
        pattern::{PatternNode, StructPatternNode},
        ArrayRefNode, ArraySliceNode, Cached, CallSite, Node, NodeKind, SelectNode,
    },
    typ::{FnType, NoRefs, Type},
    Ctx, ExecCtx, UserEvent,
};
use anyhow::{bail, Context, Result};
use arcstr::{literal, ArcStr};
use compact_str::{format_compact, CompactString};
use fxhash::FxHashMap;
use netidx::publisher::{Typ, Value};
use std::{collections::hash_map::Entry, fmt::Debug, hash::Hash, marker::PhantomData};
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

fn compile_apply_args<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    scope: &ModPath,
    top_id: ExprId,
    typ: &FnType<NoRefs>,
    args: &Arc<[(Option<ArcStr>, Expr)]>,
) -> Result<(Vec<Node<C, E>>, FxHashMap<ArcStr, bool>)> {
    macro_rules! compile {
        ($e:expr) => {
            compile(ctx, $e, scope, top_id)?
        };
    }
    let mut named = FxHashMap::default();
    let mut nodes: Vec<Node<C, E>> = vec![];
    let mut arg_spec: FxHashMap<ArcStr, bool> = FxHashMap::default();
    named.clear();
    check_named_args(&mut named, args)?;
    for a in typ.args.iter() {
        match &a.label {
            None => break,
            Some((n, optional)) => match named.remove(n) {
                Some(e) => {
                    nodes.push(compile!(e));
                    arg_spec.insert(n.clone(), false);
                }
                None if !optional => bail!("missing required argument {n}"),
                None => {
                    let spec = Box::new(
                        ExprKind::Constant(Value::String(literal!("nop")))
                            .to_expr(Default::default()),
                    );
                    let node = Node { spec, kind: NodeKind::Nop, typ: a.typ.clone() };
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

pub(super) fn compile<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    spec: Expr,
    scope: &ModPath,
    top_id: ExprId,
) -> Result<Node<C, E>> {
    macro_rules! subexprs {
        ($scope:expr, $exprs:expr, $map:expr) => {{
            $exprs
                .into_iter()
                .map(|spec| compile(ctx, spec.clone(), &$scope, top_id).map($map))
                .collect::<Result<Vec<_>>>()
        }};
    }
    macro_rules! binary_op {
        ($op:ident, $lhs:expr, $rhs:expr) => {{
            let lhs = compile(ctx, (**$lhs).clone(), scope, top_id)?;
            let rhs = compile(ctx, (**$rhs).clone(), scope, top_id)?;
            let lhs = Box::new(Cached::new(lhs));
            let rhs = Box::new(Cached::new(rhs));
            Ok(Node {
                spec: Box::new(spec),
                typ: Type::empty_tvar(),
                kind: NodeKind::$op { lhs, rhs },
            })
        }};
    }
    match &spec {
        Expr { kind: ExprKind::Constant(v), id: _, pos: _ } => {
            let typ = Type::Primitive(Typ::get(&v).into());
            Ok(Node { kind: NodeKind::Constant(v.clone()), spec: Box::new(spec), typ })
        }
        Expr { kind: ExprKind::Do { exprs }, id, pos: _ } => {
            let scope = ModPath(scope.append(&format_compact!("do{}", id.inner())));
            let exp = subexprs!(scope, exprs, |n| n)?;
            let typ = exp
                .last()
                .map(|n| n.typ.clone())
                .unwrap_or_else(|| Type::Bottom(PhantomData));
            Ok(Node { kind: NodeKind::Do(Box::from(exp)), spec: Box::new(spec), typ })
        }
        Expr { kind: ExprKind::Array { args }, id: _, pos: _ } => {
            let args = Box::from(subexprs!(scope, args, Cached::new)?);
            let typ = Type::Array(Arc::new(Type::empty_tvar()));
            Ok(Node { kind: NodeKind::Array { args }, spec: Box::new(spec), typ })
        }
        Expr { kind: ExprKind::ArrayRef { source, i }, id: _, pos: _ } => {
            let source = compile(ctx, (**source).clone(), scope, top_id)?;
            let i = compile(ctx, (**i).clone(), scope, top_id)?;
            let ert = Type::Primitive(Typ::Error.into());
            let typ = match &source.typ {
                Type::Array(et) => {
                    Type::Set(Arc::from_iter([(**et).clone(), ert.clone()]))
                }
                _ => Type::Set(Arc::from_iter([Type::empty_tvar(), ert.clone()])),
            };
            let n = ArrayRefNode { source: Cached::new(source), i: Cached::new(i) };
            Ok(Node { kind: NodeKind::ArrayRef(Box::new(n)), spec: Box::new(spec), typ })
        }
        Expr { kind: ExprKind::ArraySlice { source, start, end }, id: _, pos: _ } => {
            let source = compile(ctx, (**source).clone(), scope, top_id)?;
            let start = start
                .as_ref()
                .map(|e| compile(ctx, (**e).clone(), scope, top_id).map(Cached::new))
                .transpose()?;
            let end = end
                .as_ref()
                .map(|e| compile(ctx, (**e).clone(), scope, top_id).map(Cached::new))
                .transpose()?;
            let typ = Type::Set(Arc::from_iter([
                source.typ.clone(),
                Type::Primitive(Typ::Error.into()),
            ]));
            let n = ArraySliceNode { source: Cached::new(source), start, end };
            Ok(Node {
                kind: NodeKind::ArraySlice(Box::new(n)),
                spec: Box::new(spec),
                typ,
            })
        }
        Expr { kind: ExprKind::StringInterpolate { args }, id: _, pos: _ } => {
            let args = Box::from(subexprs!(scope, args, Cached::new)?);
            let typ = Type::Primitive(Typ::String.into());
            let spec = Box::new(spec);
            Ok(Node { kind: NodeKind::StringInterpolate { args }, spec, typ })
        }
        Expr { kind: ExprKind::Tuple { args }, id: _, pos: _ } => {
            let args: Box<[_]> = Box::from(subexprs!(scope, args, Cached::new)?);
            let typ =
                Type::Tuple(Arc::from_iter(args.iter().map(|n| n.node.typ.clone())));
            Ok(Node { kind: NodeKind::Tuple { args }, spec: Box::new(spec), typ })
        }
        Expr { kind: ExprKind::Variant { tag, args }, id: _, pos: _ } => {
            let args: Box<[_]> = Box::from(subexprs!(scope, args, Cached::new)?);
            let typs = Arc::from_iter(args.iter().map(|n| n.node.typ.clone()));
            let typ = Type::Variant(tag.clone(), typs);
            let tag = tag.clone();
            Ok(Node { kind: NodeKind::Variant { tag, args }, spec: Box::new(spec), typ })
        }
        Expr { kind: ExprKind::Struct { args }, id: _, pos: _ } => {
            let mut names = Vec::with_capacity(args.len());
            let args = args.iter().map(|(n, s)| {
                names.push(n.clone());
                s
            });
            let args: Box<[_]> = Box::from(subexprs!(scope, args, Cached::new)?);
            let names: Box<[ArcStr]> = Box::from(names);
            let typs = names
                .iter()
                .zip(args.iter())
                .map(|(n, a)| (n.clone(), a.node.typ.clone()));
            let typ = Type::Struct(Arc::from_iter(typs));
            Ok(Node { kind: NodeKind::Struct { args, names }, spec: Box::new(spec), typ })
        }
        Expr { kind: ExprKind::Module { name, export: _, value }, id: _, pos } => {
            let scope = ModPath(scope.append(&name));
            match value {
                ModuleKind::Unresolved => {
                    bail!("at {} you must resolve external modules", pos)
                }
                ModuleKind::Resolved(ori) => {
                    let exprs = subexprs!(scope, ori.exprs, |n| n)
                        .with_context(|| ori.clone())?;
                    let kind = NodeKind::Module(Box::from(exprs));
                    ctx.env.modules.insert_cow(scope.clone());
                    let typ = Type::Bottom(PhantomData);
                    Ok(Node { spec: Box::new(spec), typ, kind })
                }
                ModuleKind::Inline(exprs) => {
                    let kind =
                        NodeKind::Module(Box::from(subexprs!(scope, exprs, |n| n)?));
                    ctx.env.modules.insert_cow(scope.clone());
                    let typ = Type::Bottom(PhantomData);
                    Ok(Node { spec: Box::new(spec), typ, kind })
                }
            }
        }
        Expr { kind: ExprKind::Use { name }, id: _, pos } => {
            if !ctx.env.modules.contains(name) {
                bail!("at {pos} no such module {name}")
            } else {
                let used = ctx.env.used.get_or_default_cow(scope.clone());
                Arc::make_mut(used).push(name.clone());
                let kind = NodeKind::Use { scope: scope.clone(), name: name.clone() };
                Ok(Node { spec: Box::new(spec), typ: Type::Bottom(PhantomData), kind })
            }
        }
        Expr { kind: ExprKind::Connect { name, value }, id: _, pos } => {
            let id = match ctx.env.lookup_bind(scope, name) {
                None => bail!("at {pos} {name} is undefined"),
                Some((_, Bind { id, .. })) => *id,
            };
            let node = compile(ctx, (**value).clone(), scope, top_id)?;
            let kind = NodeKind::Connect(id, Box::new(node));
            let typ = Type::Bottom(PhantomData);
            Ok(Node { spec: Box::new(spec), typ, kind })
        }
        Expr { kind: ExprKind::Lambda(l), id: _, pos: _ } => {
            let Lambda { args, vargs, rtype, constraints, body } = &**l;
            let (args, vargs, rtype, constraints, body) = (
                args.clone(),
                vargs.clone(),
                rtype.clone(),
                constraints.clone(),
                (*body).clone(),
            );
            lambda::compile(ctx, spec, args, vargs, rtype, constraints, scope, body)
        }
        Expr { kind: ExprKind::Any { args }, id: _, pos: _ } => {
            let children = subexprs!(scope, args, |n| n)?;
            let kind = NodeKind::Any { args: Box::from(children) };
            Ok(Node { spec: Box::new(spec), typ: Type::empty_tvar(), kind })
        }
        Expr { kind: ExprKind::Apply { args, function: f }, id: _, pos } => {
            let fnode = compile(ctx, (**f).clone(), scope, top_id)?;
            let ftype = match &fnode.typ {
                Type::Fn(ftype) => ftype.clone(),
                typ => bail!("at {pos} {f} has {typ}, expected a function"),
            };
            let (args, arg_spec) = compile_apply_args(ctx, scope, top_id, &ftype, &args)
                .with_context(|| format!("in apply at {pos}"))?;
            let site = Box::new(CallSite {
                ftype: ftype.clone(),
                args,
                arg_spec,
                fnode,
                function: None,
                top_id,
            });
            let typ = ftype.rtype.clone();
            Ok(Node { spec: Box::new(spec), typ, kind: NodeKind::Apply(site) })
        }
        Expr { kind: ExprKind::Bind(b), id: _, pos } => {
            let expr::Bind { doc, pattern, typ, export: _, value } = &**b;
            let node = compile(ctx, value.clone(), &scope, top_id)?;
            let typ = match typ {
                Some(typ) => typ.resolve_typerefs(scope, &ctx.env)?,
                None => {
                    let typ = node.typ.clone();
                    let ptyp = pattern.infer_type_predicate();
                    if !ptyp.contains(&typ) {
                        bail!("at {pos} match error {typ} can't be matched by {ptyp}");
                    }
                    typ
                }
            };
            let pn = StructPatternNode::compile(ctx, &typ, pattern, scope)
                .with_context(|| format!("at {pos}"))?;
            if pn.is_refutable() {
                bail!("at {pos} refutable patterns are not allowed in let");
            }
            if let Some(doc) = doc {
                pn.ids(&mut |id| {
                    if let Some(b) = ctx.env.by_id.get_mut_cow(&id) {
                        b.doc = Some(doc.clone());
                    }
                });
            }
            let kind = NodeKind::Bind { pattern: Box::new(pn), node: Box::new(node) };
            Ok(Node { spec: Box::new(spec), typ, kind })
        }
        Expr { kind: ExprKind::Qop(e), id: _, pos } => {
            let n = compile(ctx, (**e).clone(), scope, top_id)?;
            match ctx.env.lookup_bind(scope, &ModPath::from(["errors"])) {
                None => bail!("at {pos} BUG: errors is undefined"),
                Some((_, bind)) => {
                    let typ = Type::empty_tvar();
                    let spec = Box::new(spec);
                    Ok(Node { spec, typ, kind: NodeKind::Qop(bind.id, Box::new(n)) })
                }
            }
        }
        Expr { kind: ExprKind::Ref { name }, id: _, pos } => {
            match ctx.env.lookup_bind(scope, name) {
                None => bail!("at {pos} {name} not defined"),
                Some((_, bind)) => {
                    ctx.user.ref_var(bind.id, top_id);
                    let typ = bind.typ.clone();
                    let spec = Box::new(spec);
                    Ok(Node { spec, typ, kind: NodeKind::Ref { id: bind.id, top_id } })
                }
            }
        }
        Expr { kind: ExprKind::TupleRef { source, field }, id: _, pos: _ } => {
            let source = compile(ctx, (**source).clone(), scope, top_id)?;
            let field = *field;
            let typ = match &source.typ {
                Type::Tuple(ts) => {
                    ts.get(field).map(|t| t.clone()).unwrap_or_else(Type::empty_tvar)
                }
                _ => Type::empty_tvar(),
            };
            let spec = Box::new(spec);
            let source = Box::new(source);
            Ok(Node { spec, typ, kind: NodeKind::TupleRef { source, field, top_id } })
        }
        Expr { kind: ExprKind::StructRef { source, field }, id: _, pos: _ } => {
            let source = compile(ctx, (**source).clone(), scope, top_id)?;
            let (typ, fid) = match &source.typ {
                Type::Struct(flds) => flds
                    .iter()
                    .enumerate()
                    .find_map(
                        |(i, (n, t))| {
                            if field == n {
                                Some((t.clone(), i))
                            } else {
                                None
                            }
                        },
                    )
                    .unwrap_or_else(|| (Type::empty_tvar(), 0)),
                _ => (Type::empty_tvar(), 0),
            };
            let source = Box::new(source);
            // typcheck will resolve the field index if we didn't find it already
            let kind = NodeKind::StructRef { source, field: fid, top_id };
            Ok(Node { spec: Box::new(spec), typ, kind })
        }
        Expr { kind: ExprKind::StructWith { source, replace }, id: _, pos: _ } => {
            let source = compile(ctx, (**source).clone(), scope, top_id)?;
            let replace = subexprs!(scope, replace.iter().map(|(_, e)| e), Cached::new)?;
            let replace = Box::from_iter(replace.into_iter().map(|n| (0, n)));
            let source = Box::new(source);
            let kind = NodeKind::StructWith { current: None, source, replace };
            Ok(Node { spec: Box::new(spec), typ: Type::empty_tvar(), kind })
        }
        Expr { kind: ExprKind::Select { arg, arms }, id: _, pos } => {
            let arg = Cached::new(compile(ctx, (**arg).clone(), scope, top_id)?);
            let arms = arms
                .iter()
                .map(|(pat, spec)| {
                    let scope = ModPath(
                        scope.append(&format_compact!("sel{}", SelectId::new().0)),
                    );
                    let pat = PatternNode::compile(ctx, pat, &scope, top_id)
                        .with_context(|| format!("in select at {pos}"))?;
                    let n = Cached::new(compile(ctx, spec.clone(), &scope, top_id)?);
                    Ok((pat, n))
                })
                .collect::<Result<Vec<_>>>()
                .with_context(|| format!("in select at {pos}"))?;
            let sn = Box::new(SelectNode { selected: None, arg, arms: Box::from(arms) });
            let kind = NodeKind::Select(sn);
            Ok(Node { spec: Box::new(spec), typ: Type::empty_tvar(), kind })
        }
        Expr { kind: ExprKind::TypeCast { expr, typ }, id: _, pos } => {
            let n = compile(ctx, (**expr).clone(), scope, top_id)?;
            let typ = match typ.resolve_typerefs(scope, &ctx.env) {
                Err(e) => bail!("in cast at {pos} {e}"),
                Ok(typ) => typ,
            };
            if let Err(e) = typ.check_cast() {
                bail!("in cast at {pos} {e}");
            }
            let rtyp = typ.union(&Type::Primitive(Typ::Error.into()));
            let kind = NodeKind::TypeCast { target: typ, n: Box::new(n) };
            Ok(Node { spec: Box::new(spec), typ: rtyp, kind })
        }
        Expr { kind: ExprKind::TypeDef { name, typ }, id: _, pos } => {
            let typ = typ
                .resolve_typerefs(scope, &ctx.env)
                .with_context(|| format!("in typedef at {pos}"))?;
            ctx.env
                .deftype(scope, name, typ)
                .with_context(|| format!("in typedef at {pos}"))?;
            let name = name.clone();
            let spec = Box::new(spec);
            let typ = Type::Bottom(PhantomData);
            Ok(Node { spec, typ, kind: NodeKind::TypeDef { scope: scope.clone(), name } })
        }
        Expr { kind: ExprKind::Not { expr }, id: _, pos: _ } => {
            let node = compile(ctx, (**expr).clone(), scope, top_id)?;
            let node = Box::new(node);
            let spec = Box::new(spec);
            let typ = Type::Primitive(Typ::Bool.into());
            Ok(Node { spec, typ, kind: NodeKind::Not { node } })
        }
        Expr { kind: ExprKind::Eq { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Eq, lhs, rhs)
        }
        Expr { kind: ExprKind::Ne { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Ne, lhs, rhs)
        }
        Expr { kind: ExprKind::Lt { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Lt, lhs, rhs)
        }
        Expr { kind: ExprKind::Gt { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Gt, lhs, rhs)
        }
        Expr { kind: ExprKind::Lte { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Lte, lhs, rhs)
        }
        Expr { kind: ExprKind::Gte { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Gte, lhs, rhs)
        }
        Expr { kind: ExprKind::And { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(And, lhs, rhs)
        }
        Expr { kind: ExprKind::Or { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Or, lhs, rhs)
        }
        Expr { kind: ExprKind::Add { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Add, lhs, rhs)
        }
        Expr { kind: ExprKind::Sub { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Sub, lhs, rhs)
        }
        Expr { kind: ExprKind::Mul { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Mul, lhs, rhs)
        }
        Expr { kind: ExprKind::Div { lhs, rhs }, id: _, pos: _ } => {
            binary_op!(Div, lhs, rhs)
        }
    }
}
