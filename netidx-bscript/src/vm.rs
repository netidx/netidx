pub use crate::stdfn::{RpcCallId, TimerId};
use crate::{
    expr::{Expr, ExprId, ExprKind},
    stdfn,
};
use arcstr::ArcStr;
use chrono::prelude::*;
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{Dval, SubId, UpdatesFlags, Value},
    utils::Either,
};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    intrinsics::atomic_cxchg_acquire_relaxed,
    sync::{Arc, Weak},
    time::Duration,
};

pub struct DbgCtx<E> {
    pub trace: bool,
    events: VecDeque<(ExprId, (DateTime<Local>, Option<Event<E>>, Value))>,
    watch: HashMap<
        ExprId,
        Vec<Weak<dyn Fn(&DateTime<Local>, &Option<Event<E>>, &Value) + Send + Sync>>,
        FxBuildHasher,
    >,
    current: HashMap<ExprId, (Option<Event<E>>, Value), FxBuildHasher>,
}

impl<E: Clone> DbgCtx<E> {
    fn new() -> Self {
        DbgCtx {
            trace: false,
            events: VecDeque::new(),
            watch: HashMap::with_hasher(FxBuildHasher::default()),
            current: HashMap::with_hasher(FxBuildHasher::default()),
        }
    }

    pub fn iter_events(
        &self,
    ) -> impl Iterator<Item = &(ExprId, (DateTime<Local>, Option<Event<E>>, Value))> {
        self.events.iter()
    }

    pub fn get_current(&self, id: &ExprId) -> Option<&(Option<Event<E>>, Value)> {
        self.current.get(id)
    }

    pub fn add_watch(
        &mut self,
        id: ExprId,
        watch: &Arc<dyn Fn(&DateTime<Local>, &Option<Event<E>>, &Value) + Send + Sync>,
    ) {
        let watches = self.watch.entry(id).or_insert_with(Vec::new);
        watches.push(Arc::downgrade(watch));
    }

    pub fn add_event(&mut self, id: ExprId, event: Option<Event<E>>, value: Value) {
        const MAX: usize = 1000;
        let now = Local::now();
        if let Some(watch) = self.watch.get_mut(&id) {
            let mut i = 0;
            while i < watch.len() {
                match Weak::upgrade(&watch[i]) {
                    None => {
                        watch.remove(i);
                    }
                    Some(f) => {
                        f(&now, &event, &value);
                        i += 1;
                    }
                }
            }
        }
        self.events.push_back((id, (now, event.clone(), value.clone())));
        self.current.insert(id, (event, value));
        if self.events.len() > MAX {
            self.events.pop_front();
            if self.watch.len() > MAX {
                self.watch.retain(|_, vs| {
                    vs.retain(|v| Weak::upgrade(v).is_some());
                    !vs.is_empty()
                });
            }
        }
    }

    pub fn clear(&mut self) {
        self.events.clear();
        self.current.clear();
        self.watch.retain(|_, v| {
            v.retain(|w| Weak::strong_count(w) > 0);
            v.len() > 0
        });
    }
}

#[derive(Clone, Debug)]
pub enum Event<E> {
    Init,
    Variable { scope: Path, name: Chars, index: usize, value: Value },
    Netidx(SubId, Value),
    Rpc(RpcCallId, Value),
    Timer(TimerId),
    User(E),
}

pub type InitFn<C, E> = Arc<
    dyn Fn(
            &mut ExecCtx<C, E>,
            &[Node<C, E>],
            &Path,
            ExprId,
        ) -> Box<dyn Apply<C, E> + Send + Sync>
        + Send
        + Sync,
>;

pub trait Register<C: Ctx, E> {
    fn register(ctx: &mut ExecCtx<C, E>);
}

pub trait Apply<C: Ctx, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value>;
}

pub trait Ctx {
    fn clear(&mut self);
    fn durable_subscribe(
        &mut self,
        flags: UpdatesFlags,
        path: Path,
        ref_by: ExprId,
    ) -> Dval;
    fn unsubscribe(&mut self, path: Path, dv: Dval, ref_by: ExprId);
    fn ref_var(&mut self, name: Chars, scope: Path, ref_by: ExprId);
    fn unref_var(&mut self, name: Chars, scope: Path, ref_by: ExprId);
    fn register_fn(&mut self, name: Chars, scope: Path);
    fn set_var(&mut self, scope: Path, name: Chars, index: usize, value: Value);

    /// For a given name, this must have at most one outstanding call
    /// at a time, and must preserve the order of the calls. Calls to
    /// different names may execute concurrently.
    fn call_rpc(
        &mut self,
        name: Path,
        args: Vec<(Chars, Value)>,
        ref_by: ExprId,
        id: RpcCallId,
    );

    /// arrange to have a Timer event delivered after timeout
    fn set_timer(&mut self, id: TimerId, timeout: Duration, ref_by: ExprId);
}

pub fn store_var(
    variables: &mut FxHashMap<Path, FxHashMap<Chars, Value>>,
    local: bool,
    scope: &Path,
    name: &Chars,
    value: Value,
) -> (bool, Path) {
    if local {
        let mut new = false;
        variables
            .entry(scope.clone())
            .or_insert_with(|| {
                new = true;
                HashMap::with_hasher(FxBuildHasher::default())
            })
            .insert(name.clone(), value);
        (new, scope.clone())
    } else {
        let mut iter = Path::dirnames(scope);
        loop {
            match iter.next_back() {
                None => break store_var(variables, true, &Path::root(), name, value),
                Some(scope) => {
                    if let Some(vars) = variables.get_mut(scope) {
                        if let Some(var) = vars.get_mut(name) {
                            *var = value;
                            break (false, Path::from(ArcStr::from(scope)));
                        }
                    }
                }
            }
        }
    }
}

pub struct ExecCtx<C: Ctx + 'static, E: 'static> {
    pub functions: FxHashMap<Chars, InitFn<C, E>>,
    pub variables: FxHashMap<Path, FxHashMap<Chars, usize>>,
    pub dbg_ctx: DbgCtx<E>,
    pub user: C,
}

impl<C: Ctx, E: Clone> ExecCtx<C, E> {
    pub fn lookup_var(&self, scope: &Path, name: &Chars) -> Option<(&Path, usize)> {
        for scope in Path::dirnames(scope).rev() {
            if let Some((scope, vars)) = self.variables.get_key_value(scope) {
                if let Some(index) = vars.get(name) {
                    return Some((scope, *index));
                }
            }
        }
        None
    }

    pub fn clear(&mut self) {
        self.variables.clear();
        self.dbg_ctx.clear();
        self.user.clear();
    }

    pub fn no_std(user: C) -> Self {
        ExecCtx {
            functions: HashMap::default(),
            variables: HashMap::default(),
            dbg_ctx: DbgCtx::new(),
            user,
        }
    }

    pub fn new(user: C) -> Self {
        let mut t = ExecCtx::no_std(user);
        stdfn::AfterIdle::register(&mut t);
        stdfn::All::register(&mut t);
        stdfn::And::register(&mut t);
        stdfn::Any::register(&mut t);
        stdfn::Array::register(&mut t);
        stdfn::Basename::register(&mut t);
        stdfn::Cast::register(&mut t);
        stdfn::Cmp::register(&mut t);
        stdfn::Contains::register(&mut t);
        stdfn::Count::register(&mut t);
        stdfn::Dirname::register(&mut t);
        stdfn::Divide::register(&mut t);
        stdfn::Do::register(&mut t);
        stdfn::EndsWith::register(&mut t);
        stdfn::Eval::register(&mut t);
        stdfn::FilterErr::register(&mut t);
        stdfn::Filter::register(&mut t);
        stdfn::Get::register(&mut t);
        stdfn::If::register(&mut t);
        stdfn::Index::register(&mut t);
        stdfn::Isa::register(&mut t);
        stdfn::IsErr::register(&mut t);
        stdfn::Load::register(&mut t);
        stdfn::Max::register(&mut t);
        stdfn::Mean::register(&mut t);
        stdfn::Min::register(&mut t);
        stdfn::Not::register(&mut t);
        stdfn::Once::register(&mut t);
        stdfn::Or::register(&mut t);
        stdfn::Product::register(&mut t);
        stdfn::Replace::register(&mut t);
        stdfn::RpcCall::register(&mut t);
        stdfn::Sample::register(&mut t);
        stdfn::Set::register(&mut t);
        stdfn::StartsWith::register(&mut t);
        stdfn::Store::register(&mut t);
        stdfn::StringConcat::register(&mut t);
        stdfn::StringJoin::register(&mut t);
        stdfn::StripPrefix::register(&mut t);
        stdfn::StripSuffix::register(&mut t);
        stdfn::Sum::register(&mut t);
        stdfn::Timer::register(&mut t);
        stdfn::TrimEnd::register(&mut t);
        stdfn::Trim::register(&mut t);
        stdfn::TrimStart::register(&mut t);
        stdfn::Uniq::register(&mut t);
        t
    }
}

pub enum Node<C: Ctx, E> {
    Error {
        spec: Expr,
        error: Option<Value>,
        children: Vec<Node<C, E>>,
    },
    Constant(Expr, Value),
    Bind {
        spec: Expr,
        scope: Path,
        name: Chars,
        index: usize,
        node: Box<Node<C, E>>,
    },
    Ref {
        spec: Expr,
        scope: Path,
        name: Chars,
        index: usize,
    },
    Apply {
        spec: Expr,
        args: Vec<Node<C, E>>,
        function: Box<dyn Apply<C, E> + Send + Sync>,
    },
}

impl<C: Ctx, E> fmt::Display for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Node::Error { spec: s, .. }
            | Node::Constant(s, _)
            | Node::Bind { spec: s, .. }
            | Node::Ref { spec: s, .. }
            | Node::Apply { spec: s, .. } => {
                write!(f, "{}", s)
            }
        }
    }
}

impl<C: Ctx, E: Clone> Node<C, E> {
    pub fn is_err(&self) -> bool {
        match self {
            Node::Error { .. } => true,
            Node::Apply { .. }
            | Node::Bind { .. }
            | Node::Constant(_, _)
            | Node::Ref { .. } => false,
        }
    }

    pub fn compile_int(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &Path,
        top_id: ExprId,
    ) -> Self {
        match &spec {
            Expr { kind: ExprKind::Constant(v), id: _ } => {
                Node::Constant(spec, v.clone())
            }
            Expr { kind: ExprKind::Apply { args, function }, id } => {
                let scope = if &**function == "do" && id != &top_id {
                    &scope.append(&format!("do{:?}", id))
                } else {
                    scope
                };
                let (error, args): (bool, Vec<Node<C, E>>) =
                    args.iter().fold((false, vec![]), |(e, mut nodes), spec| {
                        let n = Node::compile_int(ctx, spec.clone(), scope, top_id);
                        let e = e || n.is_err();
                        nodes.push(n);
                        (e, nodes)
                    });
                match ctx.functions.get(function).map(Arc::clone) {
                    None => {
                        let e = Value::Error(Chars::from(format!(
                            "unknown function {}",
                            function
                        )));
                        Node::Error { spec, error: Some(e), children: args }
                    }
                    Some(init) => {
                        if error {
                            Node::Error { spec, error: None, children: args }
                        } else {
                            let function = init(ctx, &args, scope, top_id);
                            Node::Apply { spec, args, function }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Bind { name, value }, id: _ } => {
                let index_ref = ctx
                    .variables
                    .entry(scope.clone())
                    .or_insert_with(HashMap::default)
                    .entry(name.clone())
                    .or_insert(0);
                let index = *index_ref;
                *index_ref += 1;
                let node = Node::compile_int(ctx, (**value).clone(), &scope, top_id);
                if node.is_err() {
                    Node::Error { spec, error: None, children: vec![node] }
                } else {
                    let name = name.clone();
                    let scope = scope.clone();
                    Node::Bind { spec, scope, name, index, node: Box::new(node) }
                }
            }
            Expr { kind: ExprKind::Ref { name }, id: _ } => {
                match ctx.lookup_var(scope, name) {
                    None => {
                        let error = Some(Value::Error(Chars::from(format!(
                            "variable {name} is not defined"
                        ))));
                        Node::Error { spec, error, children: vec![] }
                    }
                    Some((scope, index)) => Node::Ref {
                        spec: spec.clone(),
                        scope: scope.clone(),
                        name: name.clone(),
                        index,
                    },
                }
            }
        }
    }

    pub fn compile(ctx: &mut ExecCtx<C, E>, scope: &Path, spec: Expr) -> Self {
        let top_id = spec.id;
        Self::compile_int(ctx, spec, scope, top_id)
    }

    pub fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> Option<Value> {
        match self {
            Node::Error(_, _) => None,
            Node::Constant(spec, v) => match event {
                Event::Init => {
                    if ctx.dbg_ctx.trace {
                        ctx.dbg_ctx.add_event(spec.id, Some(event.clone()), v.clone())
                    }
                    Some(v.clone())
                }
                Event::Netidx(_, _)
                | Event::Rpc(_, _)
                | Event::Timer(_)
                | Event::User(_)
                | Event::Variable { .. } => None,
            },
            Node::Apply { spec, args, function } => {
                let res = function.update(ctx, args, event);
                if ctx.dbg_ctx.trace {
                    if let Some(v) = &res {
                        ctx.dbg_ctx.add_event(spec.id, Some(event.clone()), v.clone())
                    }
                }
                res
            }
            Node::Bind { spec, scope, name, index, node } => {
                if let Some(v) = node.update(ctx, event) {
                    if ctx.dbg_ctx.trace {
                        ctx.dbg_ctx.add_event(spec.id, Some(event.clone()), v.clone())
                    }
                    ctx.user.set_var(scope.clone(), name.clone(), *index, v)
                }
                None
            }
            Node::Ref { spec, scope: s, name: n, index: i } => match event {
                Event::Variable { scope, name, index, value }
                    if index == i && name == n && scope == s =>
                {
                    if ctx.dbg_ctx.trace {
                        ctx.dbg_ctx.add_event(spec.id, Some(event.clone()), value.clone())
                    }
                    Some(value.clone())
                }
                Event::Init
                | Event::Netidx(_, _)
                | Event::Rpc(_, _)
                | Event::Timer(_)
                | Event::User(_)
                | Event::Variable { .. } => None,
            },
        }
    }
}
