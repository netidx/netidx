use crate::{
    env::LambdaBind,
    expr::{Expr, ModPath},
    node::pattern::PatternNode,
    typ::{NoRefs, Type},
    ApplyTyped, BindId, Ctx, Event, ExecCtx, UserEvent,
};
use arcstr::{literal, ArcStr};
use compact_str::{format_compact, CompactString};
use netidx::{publisher::Typ, subscriber::Value};
use netidx_netproto::valarray::ValArray;
use smallvec::{smallvec, SmallVec};
use std::{fmt, marker::PhantomData};
use triomphe::Arc;

mod compiler;
mod lambda;
pub mod pattern;
mod typecheck;

pub struct Cached<C: Ctx, E: UserEvent> {
    pub cached: Option<Value>,
    pub node: Node<C, E>,
}

impl<C: Ctx, E: UserEvent> Cached<C, E> {
    pub fn new(node: Node<C, E>) -> Self {
        Self { cached: None, node }
    }

    /// update the node, return whether the node updated. If it did,
    /// the updated value will be stored in the cached field, if not,
    /// the previous value will remain there.
    pub fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> bool {
        match self.node.update(ctx, event) {
            None => false,
            Some(v) => {
                self.cached = Some(v);
                true
            }
        }
    }

    /// update the node, return true if the node updated AND the new
    /// value is different from the old value. The cached field will
    /// only be updated if the value changed.
    pub fn update_changed(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        event: &mut Event<E>,
    ) -> bool {
        match self.node.update(ctx, event) {
            v @ Some(_) if v != self.cached => {
                self.cached = v;
                true
            }
            Some(_) | None => false,
        }
    }
}

pub enum NodeKind<C: Ctx, E: UserEvent> {
    Use,
    TypeDef,
    Constant(Value),
    Module(Box<[Node<C, E>]>),
    Do(Box<[Node<C, E>]>),
    Bind(Option<BindId>, Box<Node<C, E>>),
    BindTuple(Box<[Option<BindId>]>, Box<Node<C, E>>),
    BindStruct(Box<[Option<BindId>]>, Box<Node<C, E>>),
    Ref(BindId),
    StructRef(BindId, usize),
    TupleRef(BindId, usize),
    Connect(BindId, Box<Node<C, E>>),
    Lambda(Arc<LambdaBind<C, E>>),
    Qop(BindId, Box<Node<C, E>>),
    TypeCast {
        target: Type<NoRefs>,
        n: Box<Node<C, E>>,
    },
    Array {
        args: Box<[Cached<C, E>]>,
    },
    Tuple {
        args: Box<[Cached<C, E>]>,
    },
    Struct {
        names: Box<[ArcStr]>,
        args: Box<[Cached<C, E>]>,
    },
    StructWith {
        name: BindId,
        current: Option<ValArray>,
        replace: Box<[(usize, Cached<C, E>)]>,
    },
    Apply {
        args: Box<[Node<C, E>]>,
        function: Box<dyn ApplyTyped<C, E> + Send + Sync>,
    },
    Select {
        selected: Option<usize>,
        arg: Box<Cached<C, E>>,
        arms: Box<[(PatternNode<C, E>, Cached<C, E>)]>,
    },
    Eq {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Ne {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Lt {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Gt {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Lte {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Gte {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    And {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Or {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Not {
        node: Box<Node<C, E>>,
    },
    Add {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Sub {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Mul {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Div {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Error {
        error: Option<ArcStr>,
        children: Box<[Node<C, E>]>,
    },
}

pub struct Node<C: Ctx, E: UserEvent> {
    pub spec: Box<Expr>,
    pub typ: Type<NoRefs>,
    pub kind: NodeKind<C, E>,
}

impl<C: Ctx, E: UserEvent> fmt::Display for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.spec)
    }
}

impl<C: Ctx, E: UserEvent> Node<C, E> {
    pub fn is_err(&self) -> bool {
        match &self.kind {
            NodeKind::Error { .. } => true,
            _ => false,
        }
    }

    /// extracts the full set of errors
    pub fn extract_err(&self) -> Option<ArcStr> {
        match &self.kind {
            NodeKind::Error { error, children, .. } => {
                let mut s = CompactString::new("");
                if let Some(e) = error {
                    s.push_str(e);
                    s.push_str(", ");
                }
                for node in children {
                    if let Some(e) = node.extract_err() {
                        s.push_str(e.as_str());
                        s.push_str(", ");
                    }
                }
                if s.len() > 0 {
                    Some(ArcStr::from(s.as_str()))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn compile(ctx: &mut ExecCtx<C, E>, scope: &ModPath, spec: Expr) -> Self {
        let top_id = spec.id;
        let env = ctx.env.clone();
        let mut node = compiler::compile(ctx, spec, scope, top_id);
        let node = match node.typecheck(ctx) {
            Ok(()) => node,
            Err(e) => Node {
                spec: node.spec.clone(),
                typ: Type::Bottom(PhantomData),
                kind: NodeKind::Error {
                    error: Some(format_compact!("{e}").as_str().into()),
                    children: Box::from_iter([node]),
                },
            },
        };
        if node.is_err() {
            ctx.env = env;
        }
        node
    }

    fn update_select(
        ctx: &mut ExecCtx<C, E>,
        selected: &mut Option<usize>,
        arg: &mut Cached<C, E>,
        arms: &mut [(PatternNode<C, E>, Cached<C, E>)],
        event: &mut Event<E>,
    ) -> Option<Value> {
        let mut val_up: SmallVec<[bool; 64]> = smallvec![];
        let arg_up = arg.update(ctx, event);
        macro_rules! bind {
            ($i:expr) => {{
                if let Some(arg) = arg.cached.as_ref() {
                    arms[$i].0.bind_event(event, arg);
                }
            }};
        }
        macro_rules! update {
            () => {
                for (_, val) in arms.iter_mut() {
                    val_up.push(val.update(ctx, event));
                }
            };
        }
        macro_rules! val {
            ($i:expr) => {{
                if val_up[$i] {
                    arms[$i].1.cached.clone()
                } else {
                    None
                }
            }};
        }
        let mut pat_up = false;
        for (pat, _) in arms.iter_mut() {
            if arg_up && pat.guard.is_some() {
                if let Some(arg) = arg.cached.as_ref() {
                    pat.bind_event(event, arg);
                }
            }
            pat_up |= pat.update(ctx, event);
            if arg_up && pat.guard.is_some() {
                pat.unbind_event(event);
            }
        }
        if !arg_up && !pat_up {
            update!();
            selected.and_then(|i| val!(i))
        } else {
            let sel = match arg.cached.as_ref() {
                None => None,
                Some(v) => {
                    let typ = Typ::get(v);
                    arms.iter().enumerate().find_map(|(i, (pat, _))| {
                        if pat.is_match(typ, v) {
                            Some(i)
                        } else {
                            None
                        }
                    })
                }
            };
            match (sel, *selected) {
                (Some(i), Some(j)) if i == j => {
                    if arg_up {
                        bind!(i);
                    }
                    update!();
                    val!(i)
                }
                (Some(i), Some(_) | None) => {
                    bind!(i);
                    update!();
                    *selected = Some(i);
                    val!(i)
                }
                (None, Some(_)) => {
                    update!();
                    *selected = None;
                    None
                }
                (None, None) => {
                    update!();
                    None
                }
            }
        }
    }

    pub fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        event: &mut Event<E>,
    ) -> Option<Value> {
        macro_rules! binary_op {
            ($op:tt, $lhs:expr, $rhs:expr) => {{
                let lhs_up = $lhs.update(ctx, event);
                let rhs_up = $rhs.update(ctx, event);
                if lhs_up || rhs_up {
                    return $lhs.cached.as_ref().and_then(|lhs| {
                        $rhs.cached.as_ref().map(|rhs| (lhs $op rhs).into())
                    })
                }
                None
            }}
        }
        macro_rules! binary_op_clone {
            ($op:tt, $lhs:expr, $rhs:expr) => {{
                let lhs_up = $lhs.update(ctx, event);
                let rhs_up = $rhs.update(ctx, event);
                if lhs_up || rhs_up {
                    return $lhs.cached.as_ref().and_then(|lhs| {
                        $rhs.cached.as_ref().map(|rhs| (lhs.clone() $op rhs.clone()).into())
                    })
                }
                None
            }}
        }
        macro_rules! cast_bool {
            ($v:expr) => {
                match $v.cached.as_ref().map(|v| v.clone().get_as::<bool>()) {
                    None => return None,
                    Some(None) => return Some(Value::Error(literal!("expected bool"))),
                    Some(Some(lhs)) => lhs,
                }
            };
        }
        macro_rules! binary_boolean_op {
            ($op:tt, $lhs:expr, $rhs:expr) => {{
                let lhs_up = $lhs.update(ctx, event);
                let rhs_up = $rhs.update(ctx, event);
                if lhs_up || rhs_up {
                    let lhs = cast_bool!($lhs);
                    let rhs = cast_bool!($rhs);
                    Some((lhs $op rhs).into())
                } else {
                    None
                }
            }}
        }
        let eid = self.spec.id;
        let res = match &mut self.kind {
            NodeKind::Error { .. } => None,
            NodeKind::Constant(v) => {
                if event.init {
                    Some(v.clone())
                } else {
                    None
                }
            }
            NodeKind::Array { args } | NodeKind::Tuple { args } => {
                let mut updated = false;
                let mut determined = true;
                for n in args.iter_mut() {
                    updated |= n.update(ctx, event);
                    determined &= n.cached.is_some();
                }
                if updated && determined {
                    let iter = args.iter().map(|n| n.cached.clone().unwrap());
                    Some(Value::Array(ValArray::from_iter_exact(iter)))
                } else {
                    None
                }
            }
            NodeKind::Struct { names, args } => {
                let mut updated = false;
                let mut determined = true;
                for n in args.iter_mut() {
                    updated |= n.update(ctx, event);
                    determined &= n.cached.is_some();
                }
                if updated && determined {
                    let iter = names.iter().zip(args.iter()).map(|(name, n)| {
                        let name = Value::String(name.clone());
                        let v = n.cached.clone().unwrap();
                        Value::Array(ValArray::from_iter_exact([name, v].into_iter()))
                    });
                    Some(Value::Array(ValArray::from_iter_exact(iter)))
                } else {
                    None
                }
            }
            NodeKind::StructWith { name, current, replace } => {
                let mut updated = event
                    .variables
                    .get(name)
                    .map(|v| match v {
                        Value::Array(a) => {
                            *current = Some(a.clone());
                            true
                        }
                        _ => false,
                    })
                    .unwrap_or(false);
                let mut determined = current.is_some();
                for (_, n) in replace.iter_mut() {
                    updated |= n.update(ctx, event);
                    determined &= n.cached.is_some();
                }
                if updated && determined {
                    let mut si = 0;
                    let iter = current.as_ref().unwrap().iter().enumerate().map(
                        |(i, v)| match v {
                            Value::Array(v) if v.len() == 2 => {
                                if si < replace.len() && i == replace[si].0 {
                                    let r = replace[si].1.cached.clone().unwrap();
                                    si += 1;
                                    Value::Array(ValArray::from_iter_exact(
                                        [v[0].clone(), r].into_iter(),
                                    ))
                                } else {
                                    Value::Array(v.clone())
                                }
                            }
                            _ => v.clone(),
                        },
                    );
                    Some(Value::Array(ValArray::from_iter_exact(iter)))
                } else {
                    None
                }
            }
            NodeKind::Apply { args, function } => function.update(ctx, args, event),
            NodeKind::BindTuple(binds, rhs) => {
                if let Some(v) = rhs.update(ctx, event) {
                    if ctx.dbg_ctx.trace {
                        ctx.dbg_ctx.add_event(eid, Some(event.clone()), v.clone())
                    }
                    if let Value::Array(a) = v {
                        if binds.len() == a.len() {
                            for (id, v) in binds.iter().zip(a.iter()) {
                                if let Some(id) = id {
                                    ctx.user.set_var(*id, v.clone());
                                }
                            }
                        }
                    }
                }
                None
            }
            NodeKind::BindStruct(binds, rhs) => {
                if let Some(v) = rhs.update(ctx, event) {
                    if ctx.dbg_ctx.trace {
                        ctx.dbg_ctx.add_event(eid, Some(event.clone()), v.clone())
                    }
                    if let Value::Array(a) = v {
                        if binds.len() == a.len() {
                            for (id, v) in binds.iter().zip(a.iter()) {
                                match (id, v) {
                                    (Some(id), Value::Array(a)) if a.len() == 2 => {
                                        ctx.user.set_var(*id, a[1].clone());
                                    }
                                    _ => (),
                                }
                            }
                        }
                    }
                }
                None
            }
            NodeKind::Connect(id, rhs) | NodeKind::Bind(Some(id), rhs) => {
                if let Some(v) = rhs.update(ctx, event) {
                    if ctx.dbg_ctx.trace {
                        ctx.dbg_ctx.add_event(eid, Some(event.clone()), v.clone())
                    }
                    ctx.user.set_var(*id, v)
                }
                None
            }
            NodeKind::Bind(None, rhs) => {
                rhs.update(ctx, event);
                None
            }
            NodeKind::Ref(bid) => event.variables.get(bid).map(|v| v.clone()),
            NodeKind::TupleRef(bid, i) => {
                event.variables.get(bid).and_then(|v| match v {
                    Value::Array(a) => a.get(*i).map(|v| v.clone()),
                    _ => None,
                })
            }
            NodeKind::StructRef(bid, i) => event
                .variables
                .get(bid)
                .and_then(|v| match v {
                    Value::Array(a) => a.get(*i),
                    _ => None,
                })
                .and_then(|v| match v {
                    Value::Array(a) => a.get(1).map(|v| v.clone()),
                    _ => None,
                }),
            NodeKind::Qop(id, n) => match n.update(ctx, event) {
                None => None,
                Some(e @ Value::Error(_)) => {
                    ctx.user.set_var(*id, e);
                    None
                }
                Some(v) => Some(v),
            },
            NodeKind::Module(children) => {
                for n in children {
                    n.update(ctx, event);
                }
                None
            }
            NodeKind::Do(children) => {
                children.into_iter().fold(None, |_, n| n.update(ctx, event))
            }
            NodeKind::TypeCast { target, n } => {
                n.update(ctx, event).map(|v| target.cast_value(v))
            }
            NodeKind::Not { node } => node.update(ctx, event).map(|v| !v),
            NodeKind::Eq { lhs, rhs } => binary_op!(==, lhs, rhs),
            NodeKind::Ne { lhs, rhs } => binary_op!(!=, lhs, rhs),
            NodeKind::Lt { lhs, rhs } => binary_op!(<, lhs, rhs),
            NodeKind::Gt { lhs, rhs } => binary_op!(>, lhs, rhs),
            NodeKind::Lte { lhs, rhs } => binary_op!(<=, lhs, rhs),
            NodeKind::Gte { lhs, rhs } => binary_op!(>=, lhs, rhs),
            NodeKind::And { lhs, rhs } => binary_boolean_op!(&&, lhs, rhs),
            NodeKind::Or { lhs, rhs } => binary_boolean_op!(||, lhs, rhs),
            NodeKind::Add { lhs, rhs } => binary_op_clone!(+, lhs, rhs),
            NodeKind::Sub { lhs, rhs } => binary_op_clone!(-, lhs, rhs),
            NodeKind::Mul { lhs, rhs } => binary_op_clone!(*, lhs, rhs),
            NodeKind::Div { lhs, rhs } => binary_op_clone!(/, lhs, rhs),
            NodeKind::Select { selected, arg, arms } => {
                Node::update_select(ctx, selected, arg, arms, event)
            }
            NodeKind::Use | NodeKind::Lambda(_) | NodeKind::TypeDef => None,
        };
        if ctx.dbg_ctx.trace {
            if let Some(v) = &res {
                ctx.dbg_ctx.add_event(eid, Some(event.clone()), v.clone())
            }
        }
        res
    }
}
