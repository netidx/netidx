use crate::{
    env::LambdaBind,
    expr::{Expr, ExprId, ExprKind, ModPath},
    node::pattern::PatternNode,
    typ::{FnType, NoRefs, Type},
    Apply, BindId, Ctx, Event, ExecCtx, LambdaId, UserEvent,
};
use anyhow::{bail, Result};
use arcstr::{literal, ArcStr};
use compact_str::{format_compact, CompactString};
use fxhash::FxHashMap;
use netidx::{publisher::Typ, subscriber::Value};
use netidx_netproto::valarray::ValArray;
use pattern::StructPatternNode;
use smallvec::{smallvec, SmallVec};
use std::{fmt, iter, marker::PhantomData, mem, sync::Arc};
use triomphe::Arc as TArc;

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

pub struct ApplyLate<C: Ctx, E: UserEvent> {
    ftype: TArc<FnType<NoRefs>>,
    fnode: Node<C, E>,
    args: Vec<Node<C, E>>,
    arg_spec: FxHashMap<ArcStr, bool>, // true if arg is using the default value
    function: Option<(LambdaId, Box<dyn Apply<C, E> + Send + Sync>)>,
    top_id: ExprId,
}

pub enum NodeKind<C: Ctx, E: UserEvent> {
    Nop,
    Use {
        scope: ModPath,
        name: ModPath,
    },
    TypeDef {
        scope: ModPath,
        name: ArcStr,
    },
    Constant(Value),
    Module(Box<[Node<C, E>]>),
    Do(Box<[Node<C, E>]>),
    Bind {
        pattern: Box<StructPatternNode>,
        node: Box<Node<C, E>>,
    },
    Ref {
        id: BindId,
        top_id: ExprId,
    },
    StructRef {
        id: BindId,
        field: usize,
        top_id: ExprId,
    },
    TupleRef {
        id: BindId,
        field: usize,
        top_id: ExprId,
    },
    Connect(BindId, Box<Node<C, E>>),
    Lambda(Arc<LambdaBind<C, E>>),
    Qop(BindId, Box<Node<C, E>>),
    TypeCast {
        target: Type<NoRefs>,
        n: Box<Node<C, E>>,
    },
    Any {
        args: Box<[Node<C, E>]>,
    },
    Array {
        args: Box<[Cached<C, E>]>,
    },
    Tuple {
        args: Box<[Cached<C, E>]>,
    },
    Variant {
        tag: ArcStr,
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
        function: Box<dyn Apply<C, E> + Send + Sync>,
    },
    ApplyLate(Box<ApplyLate<C, E>>),
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

    pub fn delete(self, ctx: &mut ExecCtx<C, E>) {
        let mut ids: SmallVec<[BindId; 8]> = smallvec![];
        match self.kind {
            NodeKind::Constant(_) | NodeKind::Nop => (),
            NodeKind::Ref { id, top_id }
            | NodeKind::StructRef { id, field: _, top_id }
            | NodeKind::TupleRef { id, field: _, top_id } => {
                ctx.user.unref_var(id, top_id)
            }
            NodeKind::Add { mut lhs, mut rhs }
            | NodeKind::Sub { mut lhs, mut rhs }
            | NodeKind::Mul { mut lhs, mut rhs }
            | NodeKind::Div { mut lhs, mut rhs }
            | NodeKind::Eq { mut lhs, mut rhs }
            | NodeKind::Ne { mut lhs, mut rhs }
            | NodeKind::Lte { mut lhs, mut rhs }
            | NodeKind::Lt { mut lhs, mut rhs }
            | NodeKind::Gt { mut lhs, mut rhs }
            | NodeKind::Gte { mut lhs, mut rhs }
            | NodeKind::And { mut lhs, mut rhs }
            | NodeKind::Or { mut lhs, mut rhs } => {
                mem::replace(&mut lhs.node, gen::nop()).delete(ctx);
                mem::replace(&mut rhs.node, gen::nop()).delete(ctx);
            }
            NodeKind::Use { scope, name } => {
                if let Some(used) = ctx.env.used.get_mut_cow(&scope) {
                    TArc::make_mut(used).retain(|n| n != &name);
                    if used.is_empty() {
                        ctx.env.used.remove_cow(&scope);
                    }
                }
            }
            NodeKind::TypeDef { scope, name } => ctx.env.undeftype(&scope, &name),
            NodeKind::Module(nodes)
            | NodeKind::Do(nodes)
            | NodeKind::Any { args: nodes }
            | NodeKind::Error { error: _, children: nodes } => {
                for n in nodes {
                    n.delete(ctx)
                }
            }
            NodeKind::Connect(_, mut n)
            | NodeKind::TypeCast { target: _, mut n }
            | NodeKind::Qop(_, mut n)
            | NodeKind::Not { node: mut n } => {
                mem::replace(&mut *n, gen::nop()).delete(ctx)
            }
            NodeKind::Variant { tag: _, args }
            | NodeKind::Array { args }
            | NodeKind::Tuple { args }
            | NodeKind::Struct { names: _, args } => {
                for n in args {
                    n.node.delete(ctx)
                }
            }
            NodeKind::StructWith { name: _, current: _, replace } => {
                for (_, n) in replace {
                    n.node.delete(ctx)
                }
            }
            NodeKind::Bind { pattern, node } => {
                pattern.ids(&mut |id| ids.push(id));
                node.delete(ctx);
                for id in ids.drain(..) {
                    ctx.env.unbind_variable(id)
                }
            }
            NodeKind::Select { selected: _, mut arg, arms } => {
                mem::replace(&mut arg.node, gen::nop()).delete(ctx);
                for (pat, arg) in arms {
                    arg.node.delete(ctx);
                    pat.structure_predicate.ids(&mut |id| ids.push(id));
                    if let Some(n) = pat.guard {
                        n.node.delete(ctx);
                    }
                    for id in ids.drain(..) {
                        ctx.env.unbind_variable(id);
                    }
                }
            }
            NodeKind::Lambda(lb) => {
                ctx.env.lambdas.remove_cow(&lb.id);
            }
            NodeKind::Apply { args, mut function } => {
                function.delete(ctx);
                for n in args {
                    n.delete(ctx)
                }
            }
            NodeKind::ApplyLate(late) => {
                let ApplyLate { ftype: _, fnode, args, arg_spec: _, function, top_id: _ } =
                    *late;
                if let Some((_, mut f)) = function {
                    f.delete(ctx)
                }
                fnode.delete(ctx);
                for n in args {
                    n.delete(ctx)
                }
            }
        }
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
                    val_up[i] = true;
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

    fn init_late_bound_lambda(
        ctx: &mut ExecCtx<C, E>,
        late: &mut ApplyLate<C, E>,
        lb: Arc<LambdaBind<C, E>>,
        eid: ExprId,
    ) -> Result<()> {
        macro_rules! compile_default {
            ($i:expr, $lb:expr) => {{
                match &$lb.argspec[$i].labeled {
                    None | Some(None) => bail!("expected default value"),
                    Some(Some(expr)) => {
                        let orig_env = ctx.env.restore_lexical_env(&$lb.env);
                        let n =
                            compiler::compile(ctx, expr.clone(), &$lb.scope, late.top_id);
                        ctx.env = ctx.env.merge_lexical(&orig_env);
                        if let Some(e) = n.extract_err() {
                            bail!("default arg compile error {e}")
                        }
                        n
                    }
                }
            }};
        }
        for (name, map) in late.ftype.map_argpos(&lb.typ) {
            let is_default = *late.arg_spec.get(&name).unwrap_or(&false);
            match map {
                (Some(si), Some(oi)) if si == oi => {
                    if is_default {
                        late.args[si] = compile_default!(si, lb);
                    }
                }
                (Some(si), Some(oi)) if si < oi => {
                    let mut i = si;
                    while i < oi {
                        late.args.swap(i, i + 1);
                        i += 1;
                    }
                    if is_default {
                        late.args[i] = compile_default!(si, lb);
                    }
                }
                (Some(si), Some(oi)) if oi < si => {
                    let mut i = si;
                    while i > oi {
                        late.args.swap(i, i - 1);
                        i -= 1
                    }
                    if is_default {
                        late.args[i] = compile_default!(i, lb);
                    }
                }
                (Some(_), Some(_)) => unreachable!(),
                (Some(i), None) => {
                    late.args.remove(i);
                }
                (None, Some(i)) => late.args.insert(i, compile_default!(i, lb)),
                (None, None) => bail!("unexpected args"),
            }
        }
        let mut f = (lb.init)(ctx, &late.args, eid)?;
        f.typecheck(ctx, &mut late.args)?;
        late.ftype = lb.typ.clone();
        late.function = Some((lb.id, f));
        Ok(())
    }

    fn update_apply_late(
        ctx: &mut ExecCtx<C, E>,
        late: &mut ApplyLate<C, E>,
        eid: ExprId,
        event: &mut Event<E>,
    ) -> Option<Value> {
        macro_rules! error {
            ($m:literal) => {{
                let m = format_compact!($m);
                return Some(Value::Error(m.as_str().into()));
            }};
        }
        let init = match (&late.function, late.fnode.update(ctx, event)) {
            (_, None) => false,
            (Some((cid, _)), Some(Value::U64(id))) if cid.0 == id => false,
            (_, Some(Value::U64(id))) => match ctx.env.lambdas.get(&LambdaId(id)) {
                None => error!("no such function {id:?}"),
                Some(lb) => match lb.upgrade() {
                    None => error!("function {id:?} is no longer callable"),
                    Some(lb) => {
                        if let Err(e) = Self::init_late_bound_lambda(ctx, late, lb, eid) {
                            error!("failed to init lambda {e}")
                        }
                        true
                    }
                },
            },
            (_, Some(v)) => error!("invalid function {v}"),
        };
        match &mut late.function {
            None => None,
            Some((_, f)) if !init => f.update(ctx, &mut late.args, event),
            Some((_, f)) => {
                let init = mem::replace(&mut event.init, true);
                let res = f.update(ctx, &mut late.args, event);
                event.init = init;
                res
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
        macro_rules! update_args {
            ($args:expr) => {{
                let mut updated = false;
                let mut determined = true;
                for n in $args.iter_mut() {
                    updated |= n.update(ctx, event);
                    determined &= n.cached.is_some();
                }
                (updated, determined)
            }};
        }
        let eid = self.spec.id;
        let res = match &mut self.kind {
            NodeKind::Error { .. } => self.extract_err().map(|e| Value::Error(e)),
            NodeKind::Constant(v) => {
                if event.init {
                    Some(v.clone())
                } else {
                    None
                }
            }
            NodeKind::Array { args } | NodeKind::Tuple { args } => {
                let (updated, determined) = update_args!(args);
                if updated && determined {
                    let iter = args.iter().map(|n| n.cached.clone().unwrap());
                    Some(Value::Array(ValArray::from_iter_exact(iter)))
                } else {
                    None
                }
            }
            NodeKind::Variant { tag, args } if args.len() == 0 => {
                if event.init {
                    Some(Value::String(tag.clone()))
                } else {
                    None
                }
            }
            NodeKind::Variant { tag, args } => {
                let (updated, determined) = update_args!(args);
                if updated && determined {
                    let a = iter::once(Value::String(tag.clone()))
                        .chain(args.iter().map(|n| n.cached.clone().unwrap()))
                        .collect::<SmallVec<[_; 8]>>();
                    Some(Value::Array(ValArray::from_iter_exact(a.into_iter())))
                } else {
                    None
                }
            }
            NodeKind::Any { args } => args
                .iter_mut()
                .filter_map(|s| s.update(ctx, event))
                .fold(None, |r, v| r.or(Some(v))),
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
            NodeKind::ApplyLate(late) => Node::update_apply_late(ctx, late, eid, event),
            NodeKind::Bind { pattern, node } => {
                if let Some(v) = node.update(ctx, event) {
                    pattern.bind(&v, &mut |id, v| ctx.user.set_var(id, v))
                }
                None
            }
            NodeKind::Connect(id, rhs) => {
                if let Some(v) = rhs.update(ctx, event) {
                    ctx.user.set_var(*id, v)
                }
                None
            }
            NodeKind::Ref { id: bid, .. } => event.variables.get(bid).map(|v| v.clone()),
            NodeKind::TupleRef { id: bid, field: i, .. } => {
                event.variables.get(bid).and_then(|v| match v {
                    Value::Array(a) => a.get(*i).map(|v| v.clone()),
                    _ => None,
                })
            }
            NodeKind::StructRef { id: bid, field: i, .. } => event
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
            NodeKind::Lambda(lb) if event.init => Some(Value::U64(lb.id.0)),
            NodeKind::Use { .. }
            | NodeKind::Lambda(_)
            | NodeKind::TypeDef { .. }
            | NodeKind::Nop => None,
        };
        if ctx.dbg_ctx.trace {
            if let Some(v) = &res {
                ctx.dbg_ctx.add_event(eid, Some(event.clone()), v.clone())
            }
        }
        res
    }
}

/// helpers for dynamically generating code in built-in functions. Not used by the compiler
pub mod gen {
    use super::*;

    /// return a no op node
    pub fn nop<C: Ctx, E: UserEvent>() -> Node<C, E> {
        Node {
            spec: Box::new(ExprKind::Constant(Value::String(literal!("nop"))).to_expr()),
            typ: Type::Bottom(PhantomData),
            kind: NodeKind::Nop,
        }
    }

    /// bind a variable and return a node referencing it
    pub fn bind<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        scope: &ModPath,
        name: &str,
        typ: Type<NoRefs>,
        top_id: ExprId,
    ) -> (BindId, Node<C, E>) {
        let id = ctx.env.bind_variable(scope, name, typ.clone()).id;
        ctx.user.ref_var(id, top_id);
        let spec =
            Box::new(ExprKind::Ref { name: ModPath(scope.0.append(name)) }.to_expr());
        let kind = NodeKind::Ref { id, top_id };
        (id, Node { spec, kind, typ })
    }

    /// generate and return an error node
    pub fn error<C: Ctx, E: UserEvent>(spec: Box<Expr>, msg: &str) -> Node<C, E> {
        Node {
            spec,
            kind: NodeKind::Error {
                error: Some(msg.into()),
                children: Box::from_iter([]),
            },
            typ: Type::Bottom(PhantomData),
        }
    }

    /// generate and return an apply node for the given lambda
    pub fn apply<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        lb: &LambdaBind<C, E>,
        args: Box<[Node<C, E>]>,
        typ: Type<NoRefs>,
        top_id: ExprId,
    ) -> Node<C, E> {
        let spec = Box::new(
            ExprKind::Constant(Value::String(literal!("'generated_lambda"))).to_expr(),
        );
        match (lb.init)(ctx, &args, top_id) {
            Err(e) => error(spec, format_compact!("{e:?}").as_str()),
            Ok(function) => {
                let kind = NodeKind::Apply { args, function };
                Node { spec, kind, typ }
            }
        }
    }
}
