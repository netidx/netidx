use super::{
    array::{Array, ArrayRef, ArraySlice},
    callsite::CallSite,
    data::{Struct, StructRef, StructWith, Tuple, TupleRef, Variant},
    lambda::Lambda,
    op::{Add, And, Div, Eq, Gt, Gte, Lt, Lte, Mod, Mul, Ne, Not, Or, Sub},
    select::Select,
    Any, Bind, Block, ByRef, Connect, ConnectDeref, Constant, Deref, Qop, Ref, Sample,
    StringInterpolate, TypeCast, TypeDef, Use,
};
use crate::{
    expr::{Expr, ExprId, ExprKind, ModPath, ModuleKind},
    Ctx, ExecCtx, Node, UserEvent,
};
use anyhow::{bail, Context, Result};
use compact_str::format_compact;

pub(crate) fn compile<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    spec: Expr,
    scope: &ModPath,
    top_id: ExprId,
) -> Result<Node<C, E>> {
    match &spec {
        Expr { kind: ExprKind::Constant(v), id: _, pos: _ } => {
            Constant::compile(spec.clone(), v)
        }
        Expr { kind: ExprKind::Do { exprs }, id, pos: _ } => {
            let scope = ModPath(scope.append(&format_compact!("do{}", id.inner())));
            Block::compile(ctx, spec.clone(), &scope, top_id, exprs)
        }
        Expr { kind: ExprKind::Array { args }, id: _, pos: _ } => {
            Array::compile(ctx, spec.clone(), scope, top_id, args)
        }
        Expr { kind: ExprKind::ArrayRef { source, i }, id: _, pos: _ } => {
            ArrayRef::compile(ctx, spec.clone(), scope, top_id, source, i)
        }
        Expr { kind: ExprKind::ArraySlice { source, start, end }, id: _, pos: _ } => {
            ArraySlice::compile(ctx, spec.clone(), scope, top_id, source, start, end)
        }
        Expr { kind: ExprKind::StringInterpolate { args }, id: _, pos: _ } => {
            StringInterpolate::compile(ctx, spec.clone(), scope, top_id, args)
        }
        Expr { kind: ExprKind::Tuple { args }, id: _, pos: _ } => {
            Tuple::compile(ctx, spec.clone(), scope, top_id, args)
        }
        Expr { kind: ExprKind::Variant { tag, args }, id: _, pos: _ } => {
            Variant::compile(ctx, spec.clone(), scope, top_id, tag, args)
        }
        Expr { kind: ExprKind::Struct { args }, id: _, pos: _ } => {
            Struct::compile(ctx, spec.clone(), scope, top_id, args)
        }
        Expr { kind: ExprKind::Module { name, export: _, value }, id: _, pos } => {
            let scope = ModPath(scope.append(&name));
            match value {
                ModuleKind::Unresolved => {
                    bail!("at {} you must resolve external modules", pos)
                }
                ModuleKind::Resolved(ori) => {
                    let res =
                        Block::compile(ctx, spec.clone(), &scope, top_id, &ori.exprs)
                            .with_context(|| ori.clone())?;
                    ctx.env.modules.insert_cow(scope.clone());
                    Ok(res)
                }
                ModuleKind::Inline(exprs) => {
                    let res = Block::compile(ctx, spec.clone(), &scope, top_id, exprs)?;
                    ctx.env.modules.insert_cow(scope.clone());
                    Ok(res)
                }
            }
        }
        Expr { kind: ExprKind::Use { name }, id: _, pos } => {
            Use::compile(ctx, spec.clone(), scope, name, pos)
        }
        Expr { kind: ExprKind::Connect { name, value, deref: true }, id: _, pos } => {
            ConnectDeref::compile(ctx, spec.clone(), scope, top_id, name, value, pos)
        }
        Expr { kind: ExprKind::Connect { name, value, deref: false }, id: _, pos } => {
            Connect::compile(ctx, spec.clone(), scope, top_id, name, value, pos)
        }
        Expr { kind: ExprKind::Lambda(l), id: _, pos: _ } => {
            Lambda::compile(ctx, spec.clone(), scope, l, top_id)
        }
        Expr { kind: ExprKind::Any { args }, id: _, pos: _ } => {
            Any::compile(ctx, spec.clone(), scope, top_id, args)
        }
        Expr { kind: ExprKind::Apply { args, function: f }, id: _, pos } => {
            CallSite::compile(ctx, spec.clone(), scope, top_id, args, f, pos)
        }
        Expr { kind: ExprKind::Bind(b), id: _, pos } => {
            Bind::compile(ctx, spec.clone(), scope, top_id, b, pos)
        }
        Expr { kind: ExprKind::Qop(e), id: _, pos } => {
            Qop::compile(ctx, spec.clone(), scope, top_id, e, pos)
        }
        Expr { kind: ExprKind::ByRef(e), id: _, pos: _ } => {
            ByRef::compile(ctx, spec.clone(), scope, top_id, e)
        }
        Expr { kind: ExprKind::Deref(e), id: _, pos: _ } => {
            Deref::compile(ctx, spec.clone(), scope, top_id, e)
        }
        Expr { kind: ExprKind::Ref { name }, id: _, pos } => {
            Ref::compile(ctx, spec.clone(), scope, top_id, name, pos)
        }
        Expr { kind: ExprKind::TupleRef { source, field }, id: _, pos: _ } => {
            TupleRef::compile(ctx, spec.clone(), scope, top_id, source, field)
        }
        Expr { kind: ExprKind::StructRef { source, field }, id: _, pos: _ } => {
            StructRef::compile(ctx, spec.clone(), scope, top_id, source, field)
        }
        Expr { kind: ExprKind::StructWith { source, replace }, id: _, pos: _ } => {
            StructWith::compile(ctx, spec.clone(), scope, top_id, source, replace)
        }
        Expr { kind: ExprKind::Select { arg, arms }, id: _, pos } => {
            Select::compile(ctx, spec.clone(), scope, top_id, arg, arms, pos)
        }
        Expr { kind: ExprKind::TypeCast { expr, typ }, id: _, pos } => {
            TypeCast::compile(ctx, spec.clone(), scope, top_id, expr, typ, pos)
        }
        Expr { kind: ExprKind::TypeDef { name, params, typ }, id: _, pos } => {
            TypeDef::compile(ctx, spec.clone(), scope, name, params, typ, pos)
        }
        Expr { kind: ExprKind::Not { expr }, id: _, pos: _ } => {
            Not::compile(ctx, spec.clone(), scope, top_id, expr)
        }
        Expr { kind: ExprKind::Eq { lhs, rhs }, id: _, pos: _ } => {
            Eq::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Ne { lhs, rhs }, id: _, pos: _ } => {
            Ne::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Lt { lhs, rhs }, id: _, pos: _ } => {
            Lt::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Gt { lhs, rhs }, id: _, pos: _ } => {
            Gt::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Lte { lhs, rhs }, id: _, pos: _ } => {
            Lte::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Gte { lhs, rhs }, id: _, pos: _ } => {
            Gte::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::And { lhs, rhs }, id: _, pos: _ } => {
            And::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Or { lhs, rhs }, id: _, pos: _ } => {
            Or::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Add { lhs, rhs }, id: _, pos: _ } => {
            Add::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Sub { lhs, rhs }, id: _, pos: _ } => {
            Sub::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Mul { lhs, rhs }, id: _, pos: _ } => {
            Mul::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Div { lhs, rhs }, id: _, pos: _ } => {
            Div::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Mod { lhs, rhs }, id: _, pos: _ } => {
            Mod::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
        Expr { kind: ExprKind::Sample { lhs, rhs }, id: _, pos: _ } => {
            Sample::compile(ctx, spec.clone(), scope, top_id, lhs, rhs)
        }
    }
}
