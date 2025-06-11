use crate::{
    env::Bind,
    expr::{self, Expr, ExprId, ExprKind, ModPath, ModuleKind},
    lambda,
    node::{
        callsite::CallSite, Array, ArrayRef, ArraySlice, Block, Connect, Constant,
        Lambda, Nop, StringInterpolate, Struct, Tuple, Use, Variant,
    },
    pattern::{PatternNode, StructPatternNode},
    typ::{FnType, Type},
    Ctx, ExecCtx, Node, UserEvent,
};
use anyhow::{bail, Context, Result};
use arcstr::{literal, ArcStr};
use compact_str::{format_compact, CompactString};
use fxhash::FxHashMap;
use netidx::publisher::{Typ, Value};
use std::{collections::hash_map::Entry, fmt::Debug, hash::Hash};
use triomphe::Arc;

atomic_id!(SelectId);

pub fn compile<C: Ctx, E: UserEvent>(
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
        Expr { kind: ExprKind::Constant(v), id: _, pos: _ } => Constant::compile(spec, v),
        Expr { kind: ExprKind::Do { exprs }, id, pos: _ } => {
            let scope = ModPath(scope.append(&format_compact!("do{}", id.inner())));
            Block::compile(ctx, spec, &scope, top_id, exprs)
        }
        Expr { kind: ExprKind::Array { args }, id: _, pos: _ } => {
            Array::compile(ctx, spec, scope, top_id, args)
        }
        Expr { kind: ExprKind::ArrayRef { source, i }, id: _, pos: _ } => {
            ArrayRef::compile(ctx, spec, scope, top_id, source, i)
        }
        Expr { kind: ExprKind::ArraySlice { source, start, end }, id: _, pos: _ } => {
            ArraySlice::compile(ctx, spec, scope, top_id, source, start, end)
        }
        Expr { kind: ExprKind::StringInterpolate { args }, id: _, pos: _ } => {
            StringInterpolate::compile(ctx, spec, scope, top_id, args)
        }
        Expr { kind: ExprKind::Tuple { args }, id: _, pos: _ } => {
            Tuple::compile(ctx, spec, scope, top_id, args)
        }
        Expr { kind: ExprKind::Variant { tag, args }, id: _, pos: _ } => {
            Variant::compile(ctx, spec, scope, top_id, tag, args)
        }
        Expr { kind: ExprKind::Struct { args }, id: _, pos: _ } => {
            Struct::compile(ctx, spec, scope, top_id, args)
        }
        Expr { kind: ExprKind::Module { name, export: _, value }, id: _, pos } => {
            let scope = ModPath(scope.append(&name));
            match value {
                ModuleKind::Unresolved => {
                    bail!("at {} you must resolve external modules", pos)
                }
                ModuleKind::Resolved(ori) => {
                    Block::compile(ctx, spec, &scope, top_id, &ori.exprs)
                        .with_context(|| ori.clone())
                }
                ModuleKind::Inline(exprs) => {
                    Block::compile(ctx, spec, &scope, top_id, exprs)
                }
            }
        }
        Expr { kind: ExprKind::Use { name }, id: _, pos } => {
            Use::compile(ctx, spec, scope, name, pos)
        }
        Expr { kind: ExprKind::Connect { name, value }, id: _, pos } => {
            Connect::compile(ctx, spec, scope, top_id, name, value, pos)
        }
        Expr { kind: ExprKind::Lambda(l), id: _, pos: _ } => {
            Lambda::compile(ctx, spec, scope, l)
        }
        Expr { kind: ExprKind::Any { args }, id: _, pos: _ } => {
            let children = subexprs!(scope, args, |n| n)?;
            let kind = NodeKind::Any { args: Box::from(children) };
            Ok(Node { spec: Box::new(spec), typ: Type::empty_tvar(), kind })
        }
        Expr { kind: ExprKind::Apply { args, function: f }, id: _, pos } => {
            CallSite::compile(ctx, spec, scope, top_id, args, f, pos)
        }
        Expr { kind: ExprKind::Bind(b), id: _, pos } => {
            let expr::Bind { doc, pattern, typ, export: _, value } = &**b;
            let node = compile(ctx, value.clone(), &scope, top_id)?;
            let typ = match typ {
                Some(typ) => typ.scope_refs(scope),
                None => {
                    let typ = node.typ.clone();
                    let ptyp = pattern.infer_type_predicate();
                    if !ptyp.contains(&ctx.env, &typ)? {
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
            let typ = typ.scope_refs(scope);
            if let Err(e) = typ.check_cast(&ctx.env) {
                bail!("in cast at {pos} {e}");
            }
            let rtyp = typ.union(&Type::Primitive(Typ::Error.into()));
            let kind = NodeKind::TypeCast { target: typ, n: Box::new(n) };
            Ok(Node { spec: Box::new(spec), typ: rtyp, kind })
        }
        Expr { kind: ExprKind::TypeDef { name, params, typ }, id: _, pos } => {
            let typ = typ.scope_refs(scope);
            ctx.env
                .deftype(scope, name, params.clone(), typ)
                .with_context(|| format!("in typedef at {pos}"))?;
            let name = name.clone();
            let spec = Box::new(spec);
            let typ = Type::Bottom;
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
