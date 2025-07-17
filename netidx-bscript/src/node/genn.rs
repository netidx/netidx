use super::{callsite::CallSite, Constant, Nop, Ref, NOP};
use crate::{
    expr::{ExprId, ModPath},
    typ::{FnType, Type},
    BindId, Ctx, ExecCtx, Node, UserEvent,
};
use netidx::publisher::{Typ, Value};
use std::collections::HashMap;
use triomphe::Arc;

/// generate a no op with the specific type
pub fn nop<C: Ctx, E: UserEvent>(typ: Type) -> Node<C, E> {
    Nop::new(typ)
}

/// bind a variable and return a node referencing it
pub fn bind<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    scope: &ModPath,
    name: &str,
    typ: Type,
    top_id: ExprId,
) -> (BindId, Node<C, E>) {
    let id = ctx.env.bind_variable(scope, name, typ.clone()).id;
    ctx.user.ref_var(id, top_id);
    (id, Box::new(Ref { spec: NOP.clone(), typ, id, top_id }))
}

/// generate a reference to a bind id
pub fn reference<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    id: BindId,
    typ: Type,
    top_id: ExprId,
) -> Node<C, E> {
    ctx.user.ref_var(id, top_id);
    Box::new(Ref { spec: NOP.clone(), typ, id, top_id })
}

pub fn constant<C: Ctx, E: UserEvent>(v: Value) -> Node<C, E> {
    Box::new(Constant {
        spec: NOP.clone(),
        typ: Type::Primitive(Typ::get(&v).into()),
        value: v,
    })
}

/// generate and return an apply node for the given lambda
pub fn apply<C: Ctx, E: UserEvent>(
    fnode: Node<C, E>,
    args: Vec<Node<C, E>>,
    typ: Arc<FnType>,
    top_id: ExprId,
) -> Node<C, E> {
    Box::new(CallSite {
        spec: NOP.clone(),
        ftype: typ.clone(),
        args,
        arg_spec: HashMap::default(),
        fnode,
        function: None,
        top_id,
    })
}
