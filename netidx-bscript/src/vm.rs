pub use crate::stdfn::{RpcCallId, TimerId};
use crate::{
    expr::{Expr, ExprId, ExprKind, ModPath},
    stdfn,
};
use arcstr::ArcStr;
use chrono::prelude::*;
use compact_str::{format_compact, CompactString};
use fxhash::{FxBuildHasher, FxHashMap};
use immutable_chunkmap::map::MapS as Map;
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{Dval, SubId, UpdatesFlags, Value},
};
use std::{
    collections::{HashMap, VecDeque},
    fmt, iter,
    sync::{self, Weak},
    time::Duration,
};
use triomphe::Arc;

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
        watch: &sync::Arc<
            dyn Fn(&DateTime<Local>, &Option<Event<E>>, &Value) + Send + Sync,
        >,
    ) {
        let watches = self.watch.entry(id).or_insert_with(Vec::new);
        watches.push(sync::Arc::downgrade(watch));
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
    Variable { name: ModPath, index: usize, value: Value },
    Netidx(SubId, Value),
    Rpc(RpcCallId, Value),
    Timer(TimerId, Value),
    User(E),
}

pub trait Register<C: Ctx, E> {
    fn register(ctx: &mut ExecCtx<C, E>);
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
    fn ref_var(&mut self, name: ModPath, index: usize, ref_by: ExprId);
    fn unref_var(&mut self, name: ModPath, index: usize, ref_by: ExprId);
    fn register_fn(&mut self, name: ModPath);
    fn set_var(&mut self, name: ModPath, index: usize, value: Value);

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

struct Bind<C: Ctx, E> {
    index: usize,
    export: bool,
    fun: Option<InitFn<C, E>>,
}

impl<C: Ctx, E> Default for Bind<C, E> {
    fn default() -> Self {
        Self { index: 0, export: false, fun: None }
    }
}

impl<C: Ctx, E> Clone for Bind<C, E> {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            export: self.export,
            fun: self.fun.as_ref().map(|fun| Arc::clone(fun)),
        }
    }
}

pub struct ExecCtx<C: Ctx, E> {
    binds: Map<ModPath, Map<CompactString, Bind<C, E>>>,
    used: Map<ModPath, Arc<Vec<ModPath>>>,
    root: Node<C, E>,
    pub dbg_ctx: DbgCtx<E>,
    pub user: C,
}

impl<C: Ctx, E: Clone> ExecCtx<C, E> {
    pub fn lookup_bind(
        &self,
        scope: &ModPath,
        name: &ModPath,
    ) -> Option<(&ModPath, &Bind<C, E>)> {
        let mut buf = CompactString::from("");
        let name_scope = Path::dirname(&**name);
        let name = Path::basename(&**name)?;
        for scope in Path::dirnames(&**scope).rev() {
            let used = self.used.get(scope);
            let used = iter::once(scope)
                .chain(used.iter().flat_map(|s| s.iter().map(|p| &***p)));
            for scope in used {
                let scope = name_scope
                    .map(|ns| {
                        buf.clear();
                        buf.push_str(scope);
                        buf.push_str(ns);
                        buf.as_str()
                    })
                    .unwrap_or(scope);
                if let Some((scope, vars)) = self.binds.get_full(scope) {
                    if let Some(bind) = vars.get(name) {
                        return Some((scope, bind));
                    }
                }
            }
        }
        None
    }

    pub fn clear(&mut self) {
        self.binds = Map::new();
        self.dbg_ctx.clear();
        self.user.clear();
    }

    pub fn no_std(user: C) -> Self {
        ExecCtx {
            binds: Map::new(),
            used: Map::new(),
            root: Node {
                spec: ExprKind::Module {
                    export: false,
                    name: "root".into(),
                    value: None,
                }
                .to_expr(),
                kind: NodeKind::Module { name: Chars::from("root"), children: vec![] },
            },
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

pub enum NodeKind<C: Ctx, E> {
    Constant(Value),
    Module { name: Chars, children: Vec<Node<C, E>> },
    Do { children: Vec<Node<C, E>> },
    Bind { name: Chars, index: usize, node: Box<Node<C, E>> },
    Ref { name: ModPath, index: usize },
    Connect { name: ModPath, index: usize, node: Box<Node<C, E>> },
    Lambda(InitFn<C, E>),
    Apply { args: Vec<Node<C, E>>, function: Box<dyn Apply<C, E> + Send + Sync> },
    Error { error: Option<Chars>, children: Vec<Node<C, E>> },
}

pub struct Node<C: Ctx, E> {
    spec: Expr,
    kind: NodeKind<C, E>,
}

impl<C: Ctx, E> Node<C, E> {
    fn find_lambda(&self) -> Option<&InitFn<C, E>> {
        match &self.kind {
            NodeKind::Constant(_)
            | NodeKind::Bind { .. }
            | NodeKind::Ref { .. }
            | NodeKind::Connect { .. }
            | NodeKind::Apply { .. }
            | NodeKind::Error { .. }
            | NodeKind::Module { .. } => None,
            NodeKind::Lambda(init) => Some(init),
            NodeKind::Do { children } => children.last().and_then(|t| t.find_lambda()),
        }
    }
}

impl<C: Ctx, E> fmt::Display for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.spec)
    }
}

impl<C: Ctx, E: Clone> Node<C, E> {
    pub fn is_err(&self) -> bool {
        match &self.kind {
            NodeKind::Error { .. } => true,
            NodeKind::Apply { .. }
            | NodeKind::Do { .. }
            | NodeKind::Module { .. }
            | NodeKind::Lambda(_)
            | NodeKind::Connect { .. }
            | NodeKind::Bind { .. }
            | NodeKind::Constant(_)
            | NodeKind::Ref { .. } => false,
        }
    }

    pub fn compile_int(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
    ) -> Self {
        macro_rules! compile_subexprs {
            ($exprs:expr) => {
                $exprs.iter().fold((false, vec![]), |(e, nodes), spec| {
                    let n = Node::compile_int(ctx, spec.clone(), &scope, top_id);
                    let e = e || n.is_err();
                    nodes.push(n);
                    (e, nodes)
                })
            };
        }
        match &spec {
            Expr { kind: ExprKind::Constant(v), id: _ } => {
                Node { kind: NodeKind::Constant(v.clone()), spec }
            }
            Expr { kind: ExprKind::Do { exprs }, id } => {
                let scope = scope.append(&format_compact!("{id:?}"));
                let (error, children) = compile_subexprs!(exprs);
                if error {
                    Node { kind: NodeKind::Error { error: None, children }, spec }
                } else {
                    Node { kind: NodeKind::Do { children }, spec }
                }
            }
            Expr { kind: ExprKind::Module { name, export, value }, id } => {
                unimplemented!()
            }
            Expr { kind: ExprKind::Use { name }, id } => {
                unimplemented!()
            }
            Expr { kind: ExprKind::Connect { name, value }, id } => {
                unimplemented!()
            }
            Expr { kind: ExprKind::Lambda { args, vargs, body }, id } => {
                unimplemented!()
            }
            Expr { kind: ExprKind::Apply { args, function }, id } => {
                let (error, args) = compile_subexprs!(args);
                let kind = match ctx.lookup_bind(scope, function) {
                    Some((scope, Bind { fun: None, .. })) => {
                        let e =
                            Chars::from(format!("{scope}::{function} is not a function"));
                        NodeKind::Error { error: Some(e), children: args }
                    }
                    None => {
                        let e = Chars::from(format!("unknown function {}", function));
                        NodeKind::Error { error: Some(e), children: args }
                    }
                    Some((_, Bind { fun: Some(init), .. })) => {
                        if error {
                            NodeKind::Error { error: None, children: args }
                        } else {
                            let function = init(ctx, &args, scope, top_id);
                            NodeKind::Apply { args, function }
                        }
                    }
                };
                Node { spec, kind }
            }
            Expr { kind: ExprKind::Bind { export, name, value }, id: _ } => {
                let bind = ctx
                    .binds
                    .get_or_default_cow(scope.clone())
                    .get_or_default_cow(CompactString::from(&**name));
                let node = Node::compile_int(ctx, (**value).clone(), &scope, top_id);
                let index = bind.index;
                bind.index += 1;
                bind.fun = node.find_lambda().map(Arc::clone);
                if node.is_err() {
                    let kind = NodeKind::Error { error: None, children: vec![node] };
                    Node { spec, kind }
                } else {
                    let node = Box::new(node);
                    let kind = NodeKind::Bind { index, name: name.clone(), node };
                    Node { spec, kind }
                }
            }
            Expr { kind: ExprKind::Ref { name }, id } => {
                match ctx.lookup_var(scope, name) {
                    None => {
                        let m = format!("variable {name} is not defined");
                        let error = Some(Value::Error(Chars::from(m)));
                        Node::Error { spec, error, children: vec![] }
                    }
                    Some((scope, index)) => {
                        let scope = scope.clone();
                        ctx.user.ref_var(scope.clone(), name.clone(), index, *id);
                        Node::Ref { spec: spec.clone(), scope, name: name.clone(), index }
                    }
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
            Node::Error { .. } => None,
            Node::Constant(spec, v) => match event {
                Event::Init => {
                    if ctx.dbg_ctx.trace {
                        ctx.dbg_ctx.add_event(spec.id, Some(event.clone()), v.clone())
                    }
                    Some(v.clone())
                }
                Event::Netidx(_, _)
                | Event::Rpc(_, _)
                | Event::Timer(_, _)
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
                | Event::Timer(_, _)
                | Event::User(_)
                | Event::Variable { .. } => None,
            },
        }
    }
}
