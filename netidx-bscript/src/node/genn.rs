use super::{callsite::CallSite, Nop, Ref};
use crate::{
    expr::{ExprId, ExprKind, ModPath},
    typ::{FnType, Type},
    BindId, Ctx, ExecCtx, Node, UserEvent,
};
use arcstr::literal;
use combine::stream::position::SourcePosition;
use netidx::publisher::Value;
use smallvec::SmallVec;
use std::collections::HashMap;
use triomphe::Arc;

/// generate a no op with the specific type
pub fn nop<C: Ctx, E: UserEvent>(typ: Type) -> Node<C, E> {
    Box::new(Nop {
        spec: ExprKind::Constant(Value::String(literal!("nop")))
            .to_expr(Default::default()),
        typ,
    })
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
    let pos: SourcePosition = Default::default();
    let spec = ExprKind::Ref { name: ModPath(scope.0.append(name)) }.to_expr(pos);
    (id, Box::new(Ref { spec, typ, id, top_id }))
}

/// generate a reference to a bind id
pub fn reference<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    id: BindId,
    typ: Type,
    top_id: ExprId,
) -> Node<C, E> {
    ctx.user.ref_var(id, top_id);
    let spec = ExprKind::Ref { name: ModPath::from(["x"]) }.to_expr(Default::default());
    Box::new(Ref { spec, typ, id, top_id })
}

/// generate and return an apply node for the given lambda
pub fn apply<C: Ctx, E: UserEvent>(
    fnode: Node<C, E>,
    args: SmallVec<[Node<C, E>; 8]>,
    typ: Arc<FnType>,
    top_id: ExprId,
) -> Node<C, E> {
    let spec = ExprKind::Apply {
        args: Arc::from_iter(args.iter().map(|n| (None, n.spec().clone()))),
        function: Arc::new(fnode.spec().clone()),
    }
    .to_expr(Default::default());
    Box::new(CallSite {
        spec,
        ftype: typ.clone(),
        args,
        arg_spec: HashMap::default(),
        fnode,
        function: None,
        top_id,
    })
}
