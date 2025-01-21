use crate::{
    expr::{Expr, ExprId, ExprKind, ModPath},
    stdfn,
};
use anyhow::{bail, Result};
use chrono::prelude::*;
use compact_str::{format_compact, CompactString};
use fxhash::FxHashMap;
use immutable_chunkmap::{map::MapS as Map, set::SetS as Set};
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{Dval, SubId, UpdatesFlags, Value},
    utils::Either,
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
    watch: FxHashMap<
        ExprId,
        Vec<Weak<dyn Fn(&DateTime<Local>, &Option<Event<E>>, &Value) + Send + Sync>>,
    >,
    current: FxHashMap<ExprId, (Option<Event<E>>, Value)>,
}

impl<E: Clone> DbgCtx<E> {
    fn new() -> Self {
        DbgCtx {
            trace: false,
            events: VecDeque::new(),
            watch: HashMap::default(),
            current: HashMap::default(),
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

atomic_id!(TimerId);
atomic_id!(RpcCallId);
atomic_id!(BindId);
atomic_id!(LambdaId);

#[derive(Clone, Debug)]
pub enum Event<E> {
    Init,
    Variable(BindId, Value),
    Netidx(SubId, Value),
    Rpc(RpcCallId, Value),
    Timer(TimerId, Value),
    User(E),
}

pub type InitFn<C, E> = sync::Arc<
    dyn for<'a, 'b, 'c> Fn(
            &'a mut ExecCtx<C, E>,
            &'b [Node<C, E>],
            &'c ModPath,
            ExprId,
        ) -> Result<Box<dyn Apply<C, E> + Send + Sync>>
        + Send
        + Sync,
>;

pub trait Init<C: Ctx, E: Clone> {
    const ARITY: Arity;
    const NAME: &str;

    fn init(ctx: &mut ExecCtx<C, E>) -> InitFn<C, E>;
}

pub trait Apply<C: Ctx, E: Clone> {
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
    fn ref_var(&mut self, id: BindId, ref_by: ExprId);
    fn unref_var(&mut self, id: BindId, ref_by: ExprId);
    fn register_fn(&mut self, scope: ModPath, name: ModPath);
    fn set_var(&mut self, id: BindId, value: Value);

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

#[derive(Debug, Clone, Copy)]
pub enum Arity {
    Any,
    AtLeast(usize),
    Exactly(usize),
}

struct Bind<C: Ctx + 'static, E: Clone + 'static> {
    id: BindId,
    export: bool,
    fun: Option<InitFn<C, E>>,
}

impl<C: Ctx + 'static, E: Clone + 'static> Default for Bind<C, E> {
    fn default() -> Self {
        Self { id: BindId::new(), export: false, fun: None }
    }
}

impl<C: Ctx + 'static, E: Clone + 'static> Clone for Bind<C, E> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            export: self.export,
            fun: self.fun.as_ref().map(|fun| sync::Arc::clone(fun)),
        }
    }
}

pub struct ExecCtx<C: Ctx + 'static, E: Clone + 'static> {
    binds: Map<ModPath, Map<CompactString, Bind<C, E>>>,
    used: Map<ModPath, Arc<Vec<ModPath>>>,
    modules: Set<ModPath>,
    builtins: FxHashMap<&'static str, (Arity, InitFn<C, E>)>,
    pub dbg_ctx: DbgCtx<E>,
    pub user: C,
}

impl<C: Ctx + 'static, E: Clone + 'static> ExecCtx<C, E> {
    fn find_visible<R, F: FnMut(&str, &str) -> Option<R>>(
        &self,
        scope: &ModPath,
        name: &ModPath,
        mut f: F,
    ) -> Option<R> {
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
                if let Some(res) = f(scope, name) {
                    return Some(res);
                }
            }
        }
        None
    }

    fn lookup_bind(
        &self,
        scope: &ModPath,
        name: &ModPath,
    ) -> Option<(&ModPath, &Bind<C, E>)> {
        self.find_visible(scope, name, |scope, name| {
            self.binds
                .get_full(scope)
                .and_then(|(scope, vars)| vars.get(name).map(|bind| (scope, bind)))
        })
    }

    pub fn clear(&mut self) {
        self.binds = Map::new();
        self.used = Map::new();
        self.modules = Set::new();
        self.dbg_ctx.clear();
        self.user.clear();
    }

    /// build a new context with only the core library
    pub fn new_no_std(user: C) -> Self {
        let mut t = ExecCtx {
            binds: Map::new(),
            used: Map::new(),
            modules: Set::new(),
            builtins: FxHashMap::default(),
            dbg_ctx: DbgCtx::new(),
            user,
        };
        let core = stdfn::core::register(&mut t);
        let root = ModPath(Path::root());
        let node = Node::compile(&mut t, &root, core);
        if let Some(e) = node.extract_err() {
            panic!("error compiling core {e}")
        }
        let node = Node::compile(
            &mut t,
            &root,
            ExprKind::Use { name: ModPath::from(["core"]) }.to_expr(),
        );
        if let Some(e) = node.extract_err() {
            panic!("error using core {e}")
        }
        t
    }

    /// build a new context with the full standard library
    pub fn new(user: C) -> Self {
        let mut t = Self::new_no_std(user);
        let root = ModPath(Path::root());
        let net = stdfn::net::register(&mut t);
        let node = Node::compile(&mut t, &root, net);
        if let Some(e) = node.extract_err() {
            panic!("failed to compile the net module {e}")
        }
        let str = stdfn::str::register(&mut t);
        let node = Node::compile(&mut t, &root, str);
        if let Some(e) = node.extract_err() {
            panic!("failed to compile the str module {e}")
        }
        let time = stdfn::time::register(&mut t);
        let node = Node::compile(&mut t, &root, time);
        if let Some(e) = node.extract_err() {
            panic!("failed to compile the time module {e}")
        }
        t
    }

    pub fn register_builtin<T: Init<C, E>>(&mut self) {
        let f = T::init(self);
        self.builtins.insert(T::NAME, (T::ARITY, f));
    }
}

struct Lambda<C: Ctx + 'static, E: Clone + 'static> {
    eid: ExprId,
    argids: Vec<BindId>,
    body: Node<C, E>,
}

impl<C: Ctx + 'static, E: Clone + 'static> Apply<C, E> for Lambda<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        for (arg, id) in from.iter_mut().zip(&self.argids) {
            if let Some(v) = arg.update(ctx, event) {
                if ctx.dbg_ctx.trace {
                    ctx.dbg_ctx.add_event(self.eid, Some(event.clone()), v.clone())
                }
                ctx.user.set_var(*id, v)
            }
        }
        self.body.update(ctx, event)
    }
}

impl<C: Ctx + 'static, E: Clone + 'static> Lambda<C, E> {
    fn new(
        ctx: &mut ExecCtx<C, E>,
        argspec: Arc<[Chars]>,
        args: &[Node<C, E>],
        scope: &ModPath,
        eid: ExprId,
        tid: ExprId,
        body: Expr,
    ) -> Result<Self> {
        if args.len() != argspec.len() {
            bail!("arity mismatch, expected {} arguments", argspec.len())
        }
        let id = LambdaId::new();
        let mut argids = vec![];
        let scope = ModPath(scope.0.append(&format_compact!("{id:?}")));
        let binds = ctx.binds.get_or_default_cow(scope.clone());
        for (name, node) in argspec.iter().zip(args.iter()) {
            let bind = binds.get_or_default_cow(CompactString::from(&**name));
            argids.push(bind.id);
            bind.fun = node.find_lambda();
        }
        let body = Node::compile_int(ctx, body, &scope, tid);
        match body.extract_err() {
            Some(e) => bail!("{e}"),
            None => Ok(Self { eid, argids, body }),
        }
    }
}

pub struct Cached<C: Ctx + 'static, E: Clone + 'static> {
    pub cached: Option<Value>,
    pub node: Node<C, E>,
}

impl<C: Ctx + 'static, E: Clone + 'static> Cached<C, E> {
    pub fn new(node: Node<C, E>) -> Self {
        Self { cached: None, node }
    }

    pub fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> bool {
        match self.node.update(ctx, event) {
            None => false,
            Some(v) => {
                self.cached = Some(v);
                true
            }
        }
    }
}

pub enum NodeKind<C: Ctx + 'static, E: Clone + 'static> {
    Constant(Value),
    Module(Vec<Node<C, E>>),
    Use,
    Do(Vec<Node<C, E>>),
    Bind(BindId, Box<Node<C, E>>),
    Ref(BindId),
    Connect(BindId, Box<Node<C, E>>),
    Lambda(InitFn<C, E>),
    Apply { args: Vec<Node<C, E>>, function: Box<dyn Apply<C, E> + Send + Sync> },
    Select { arms: Vec<(Cached<C, E>, Cached<C, E>)> },
    Eq { lhs: Box<Cached<C, E>>, rhs: Box<Cached<C, E>> },
    Ne { lhs: Box<Cached<C, E>>, rhs: Box<Cached<C, E>> },
    Lt { lhs: Box<Cached<C, E>>, rhs: Box<Cached<C, E>> },
    Gt { lhs: Box<Cached<C, E>>, rhs: Box<Cached<C, E>> },
    Lte { lhs: Box<Cached<C, E>>, rhs: Box<Cached<C, E>> },
    Gte { lhs: Box<Cached<C, E>>, rhs: Box<Cached<C, E>> },
    And { lhs: Box<Cached<C, E>>, rhs: Box<Cached<C, E>> },
    Or { lhs: Box<Cached<C, E>>, rhs: Box<Cached<C, E>> },
    Not { node: Box<Cached<C, E>> },
    Error { error: Option<Chars>, children: Vec<Node<C, E>> },
}

pub struct Node<C: Ctx + 'static, E: Clone + 'static> {
    spec: Expr,
    kind: NodeKind<C, E>,
}

impl<C: Ctx + 'static, E: Clone + 'static> fmt::Display for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.spec)
    }
}

impl<C: Ctx + 'static, E: Clone + 'static> Node<C, E> {
    fn find_lambda(&self) -> Option<InitFn<C, E>> {
        match &self.kind {
            NodeKind::Constant(_)
            | NodeKind::Use
            | NodeKind::Bind(_, _)
            | NodeKind::Ref(_)
            | NodeKind::Connect(_, _)
            | NodeKind::Apply { .. }
            | NodeKind::Error { .. }
            | NodeKind::Module(_)
            | NodeKind::Eq { .. }
            | NodeKind::Ne { .. }
            | NodeKind::Lt { .. }
            | NodeKind::Gt { .. }
            | NodeKind::Gte { .. }
            | NodeKind::Lte { .. }
            | NodeKind::And { .. }
            | NodeKind::Or { .. }
            | NodeKind::Not { .. }
            | NodeKind::Select { .. } => None,
            NodeKind::Lambda(init) => Some(sync::Arc::clone(init)),
            NodeKind::Do(children) => children.last().and_then(|t| t.find_lambda()),
        }
    }

    pub fn is_err(&self) -> bool {
        match &self.kind {
            NodeKind::Error { .. } => true,
            NodeKind::Constant(_)
            | NodeKind::Lambda(_)
            | NodeKind::Do(_)
            | NodeKind::Use
            | NodeKind::Bind(_, _)
            | NodeKind::Ref(_)
            | NodeKind::Connect(_, _)
            | NodeKind::Apply { .. }
            | NodeKind::Module(_)
            | NodeKind::Eq { .. }
            | NodeKind::Ne { .. }
            | NodeKind::Lt { .. }
            | NodeKind::Gt { .. }
            | NodeKind::Gte { .. }
            | NodeKind::Lte { .. }
            | NodeKind::And { .. }
            | NodeKind::Or { .. }
            | NodeKind::Not { .. }
            | NodeKind::Select { .. } => false,
        }
    }

    /// extracts the first error
    pub fn extract_err(&self) -> Option<Chars> {
        match &self.kind {
            NodeKind::Error { error: Some(e), .. } => Some(e.clone()),
            NodeKind::Error { children, .. } => {
                for node in children {
                    if let Some(e) = node.extract_err() {
                        return Some(e);
                    }
                }
                None
            }
            NodeKind::Constant(_)
            | NodeKind::Lambda(_)
            | NodeKind::Do(_)
            | NodeKind::Use
            | NodeKind::Bind(_, _)
            | NodeKind::Ref(_)
            | NodeKind::Connect(_, _)
            | NodeKind::Apply { .. }
            | NodeKind::Module(_)
            | NodeKind::Eq { .. }
            | NodeKind::Ne { .. }
            | NodeKind::Lt { .. }
            | NodeKind::Gt { .. }
            | NodeKind::Gte { .. }
            | NodeKind::Lte { .. }
            | NodeKind::And { .. }
            | NodeKind::Or { .. }
            | NodeKind::Not { .. }
            | NodeKind::Select { .. } => None,
        }
    }

    pub fn bind_rhs_ok(&self) -> bool {
        match &self.kind {
            NodeKind::Use
            | NodeKind::Connect(_, _)
            | NodeKind::Bind(_, _)
            | NodeKind::Module(_)
            | NodeKind::Error { .. } => false,
            NodeKind::Do(_)
            | NodeKind::Eq { .. }
            | NodeKind::Ne { .. }
            | NodeKind::Lt { .. }
            | NodeKind::Gt { .. }
            | NodeKind::Gte { .. }
            | NodeKind::Lte { .. }
            | NodeKind::And { .. }
            | NodeKind::Or { .. }
            | NodeKind::Not { .. }
            | NodeKind::Select { .. }
            | NodeKind::Constant(_)
            | NodeKind::Ref(_)
            | NodeKind::Lambda(_)
            | NodeKind::Apply { .. } => true,
        }
    }

    pub fn connect_rhs_ok(&self) -> bool {
        match &self.kind {
            NodeKind::Use
            | NodeKind::Connect(_, _)
            | NodeKind::Bind(_, _)
            | NodeKind::Module(_)
            | NodeKind::Lambda(_)
            | NodeKind::Error { .. } => false,
            NodeKind::Do(_)
            | NodeKind::Eq { .. }
            | NodeKind::Ne { .. }
            | NodeKind::Lt { .. }
            | NodeKind::Gt { .. }
            | NodeKind::Gte { .. }
            | NodeKind::Lte { .. }
            | NodeKind::And { .. }
            | NodeKind::Or { .. }
            | NodeKind::Not { .. }
            | NodeKind::Select { .. }
            | NodeKind::Constant(_)
            | NodeKind::Ref(_)
            | NodeKind::Apply { .. } => true,
        }
    }

    pub fn do_expr_ok(&self) -> bool {
        match &self.kind {
            NodeKind::Module(_) | NodeKind::Error { .. } => false,
            NodeKind::Use
            | NodeKind::Lambda(_)
            | NodeKind::Connect(_, _)
            | NodeKind::Bind(_, _)
            | NodeKind::Do(_)
            | NodeKind::Constant(_)
            | NodeKind::Ref(_)
            | NodeKind::Eq { .. }
            | NodeKind::Ne { .. }
            | NodeKind::Lt { .. }
            | NodeKind::Gt { .. }
            | NodeKind::Gte { .. }
            | NodeKind::Lte { .. }
            | NodeKind::And { .. }
            | NodeKind::Or { .. }
            | NodeKind::Not { .. }
            | NodeKind::Select { .. }
            | NodeKind::Apply { .. } => true,
        }
    }

    pub fn args_ok(&self) -> bool {
        match &self.kind {
            NodeKind::Use
            | NodeKind::Connect(_, _)
            | NodeKind::Bind(_, _)
            | NodeKind::Module(_)
            | NodeKind::Error { .. } => false,
            NodeKind::Lambda(_)
            | NodeKind::Eq { .. }
            | NodeKind::Ne { .. }
            | NodeKind::Lt { .. }
            | NodeKind::Gt { .. }
            | NodeKind::Gte { .. }
            | NodeKind::Lte { .. }
            | NodeKind::And { .. }
            | NodeKind::Or { .. }
            | NodeKind::Not { .. }
            | NodeKind::Select { .. }
            | NodeKind::Do(_)
            | NodeKind::Constant(_)
            | NodeKind::Ref(_)
            | NodeKind::Apply { .. } => true,
        }
    }

    fn compile_lambda(
        spec: Expr,
        argspec: Arc<[Chars]>,
        body: Either<Expr, Chars>,
        eid: ExprId,
    ) -> Node<C, E> {
        use sync::Arc;
        if argspec.len() != argspec.iter().collect::<Set<_>>().len() {
            let e = Chars::from("arguments must have unique names");
            let kind = NodeKind::Error { error: Some(e), children: vec![] };
            return Node { spec, kind };
        }
        let f: InitFn<C, E> = Arc::new(move |ctx, args, scope, tid| match body.clone() {
            Either::Left(body) => Ok(Box::new(Lambda::new(
                ctx,
                argspec.clone(),
                args,
                scope,
                eid,
                tid,
                body,
            )?)),
            Either::Right(builtin) => match ctx.builtins.get(&*builtin) {
                None => bail!("unknown builtin function {builtin}"),
                Some((arity, init)) => {
                    let init = Arc::clone(init);
                    let l = args.len();
                    match *arity {
                        Arity::Any => init(ctx, args, scope, tid),
                        Arity::AtLeast(n) if l >= n => init(ctx, args, scope, tid),
                        Arity::AtLeast(n) => {
                            bail!("expected at least {n} args")
                        }
                        Arity::Exactly(n) if l == n => init(ctx, args, scope, tid),
                        Arity::Exactly(n) => bail!("expected {n} arguments"),
                    }
                }
            },
        });
        Node { spec, kind: NodeKind::Lambda(f) }
    }

    pub fn compile_int(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
    ) -> Self {
        macro_rules! subexprs {
            ($scope:expr, $exprs:expr) => {
                $exprs.iter().fold((false, vec![]), |(e, mut nodes), spec| {
                    let n = Node::compile_int(ctx, spec.clone(), &$scope, top_id);
                    let e = e || n.is_err();
                    nodes.push(n);
                    (e, nodes)
                })
            };
        }
        macro_rules! error {
            ($fmt:expr, $children:expr, $($arg:expr),*) => {{
                let e = Chars::from(format!($fmt, $($arg),*));
                let kind = NodeKind::Error { error: Some(e), children: $children };
                Node { spec, kind }
            }};
            ($fmt:expr) => { error!($fmt, vec![],) };
            ($fmt:expr, $children:expr) => { error!($fmt, $children,) };
            ("", $children:expr) => {{
                let kind = NodeKind::Error { error: None, children: $children }
                Node { spec, kind }
            }}
        }
        macro_rules! binary_op {
            ($op:ident, $lhs:expr, $rhs:expr) => {{
                let lhs = Node::compile_int(ctx, (**$lhs).clone(), scope, top_id);
                let rhs = Node::compile_int(ctx, (**$rhs).clone(), scope, top_id);
                if lhs.is_err() || rhs.is_err() {
                    return error!("", vec![lhs, rhs]);
                }
                let lhs = Box::new(Cached::new(lhs));
                let rhs = Box::new(Cached::new(rhs));
                Node { spec, kind: NodeKind::$op { lhs, rhs } }
            }};
        }
        match &spec {
            Expr { kind: ExprKind::Constant(v), id: _ } => {
                Node { kind: NodeKind::Constant(v.clone()), spec }
            }
            Expr { kind: ExprKind::Do { exprs }, id } => {
                let scope = ModPath(scope.append(&format_compact!("{id:?}")));
                let (error, exp) = subexprs!(scope, exprs);
                if error {
                    error!("", exp)
                } else if let Some(e) = exp.iter().find(|e| !e.do_expr_ok()) {
                    error!("\"{e}\" cannot appear inside do")
                } else {
                    Node { kind: NodeKind::Do(exp), spec }
                }
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
                            ctx.modules.insert_cow(scope.clone());
                            Node { spec, kind: NodeKind::Module(children) }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Use { name }, id: _ } => {
                if !ctx.modules.contains(name) {
                    error!("no such module {name}")
                } else {
                    let used = ctx.used.get_or_default_cow(scope.clone());
                    Arc::make_mut(used).push(name.clone());
                    Node { spec, kind: NodeKind::Use }
                }
            }
            Expr { kind: ExprKind::Connect { name, value }, id: _ } => {
                match ctx.lookup_bind(scope, name) {
                    None => error!("{name} is undefined"),
                    Some((_, Bind { fun: Some(_), .. })) => {
                        error!("{name} is a function")
                    }
                    Some((_, Bind { id, fun: None, export: _ })) => {
                        let id = *id;
                        let node =
                            Node::compile_int(ctx, (**value).clone(), scope, top_id);
                        if node.is_err() {
                            error!("", vec![node])
                        } else if !node.connect_rhs_ok() {
                            error!("{name} cannot be connected to \"{node}\"")
                        } else {
                            let kind = NodeKind::Connect(id, Box::new(node));
                            Node { spec, kind }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Lambda { args, vargs: _, body }, id } => {
                Node::compile_lambda(spec, args.clone(), (**body).clone(), *id)
            }
            Expr { kind: ExprKind::Apply { args, function }, id: _ } => {
                let (error, args) = subexprs!(scope, args);
                match ctx.lookup_bind(scope, function) {
                    None => error!("{function} is undefined"),
                    Some((_, Bind { fun: None, .. })) => {
                        error!("{function} is not a function")
                    }
                    Some((_, Bind { fun: Some(init), .. })) => {
                        let init = sync::Arc::clone(init);
                        if error {
                            error!("", args)
                        } else if let Some(e) = args.iter().find(|e| !e.args_ok()) {
                            error!("invalid argument \"{e}\"")
                        } else {
                            match init(ctx, &args, scope, top_id) {
                                Err(e) => error!("error in function {function} {e:?}"),
                                Ok(function) => Node {
                                    spec,
                                    kind: NodeKind::Apply { args, function },
                                },
                            }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Bind { export: _, name, value }, id: _ } => {
                let node = Node::compile_int(ctx, (**value).clone(), &scope, top_id);
                let mut existing = true;
                let f = || {
                    existing = false;
                    Bind::default()
                };
                let k = CompactString::from(&**name);
                let bind =
                    ctx.binds.get_or_default_cow(scope.clone()).get_or_insert_cow(k, f);
                if existing {
                    bind.id = BindId::new();
                }
                bind.fun = node.find_lambda();
                if node.is_err() {
                    error!("", vec![node])
                } else {
                    Node { spec, kind: NodeKind::Bind(bind.id, Box::new(node)) }
                }
            }
            Expr { kind: ExprKind::Ref { name }, id: eid } => {
                match ctx.lookup_bind(scope, name) {
                    None => error!("{name} not defined"),
                    Some((_, bind)) => match &bind.fun {
                        Some(_) => error!("{name} is a function"),
                        None => {
                            let id = bind.id;
                            ctx.user.ref_var(id, *eid);
                            Node { spec, kind: NodeKind::Ref(id) }
                        }
                    },
                }
            }
            Expr { kind: ExprKind::Not { expr }, id: _ } => {
                let node = Node::compile_int(ctx, spec, scope, top_id);
                if node.is_err() {
                    return error!("", vec![node]);
                }
                let node = Box::new(Cached::new(node));
                Node { spec, kind: NodeKind::Not { node } }
            }
            Expr { kind: ExprKind::Eq { lhs, rhs }, id: _ } => binary_op!(Eq, lhs, rhs),
            Expr { kind: ExprKind::Ne { lhs, rhs }, id: _ } => binary_op!(Ne, lhs, rhs),
            Expr { kind: ExprKind::Lt { lhs, rhs }, id: _ } => binary_op!(Lt, lhs, rhs),
            Expr { kind: ExprKind::Gt { lhs, rhs }, id: _ } => binary_op!(Gt, lhs, rhs),
            Expr { kind: ExprKind::Lte { lhs, rhs }, id: _ } => binary_op!(Lte, lhs, rhs),
            Expr { kind: ExprKind::Gte { lhs, rhs }, id: _ } => binary_op!(Gte, lhs, rhs),
            Expr { kind: ExprKind::And { lhs, rhs }, id: _ } => binary_op!(And, lhs, rhs),
            Expr { kind: ExprKind::Or { lhs, rhs }, id: _ } => binary_op!(Or, lhs, rhs),
        }
    }

    pub fn compile(ctx: &mut ExecCtx<C, E>, scope: &ModPath, spec: Expr) -> Self {
        let top_id = spec.id;
        let binds = ctx.binds.clone();
        let used = ctx.used.clone();
        let modules = ctx.modules.clone();
        let node = Self::compile_int(ctx, spec, scope, top_id);
        if node.is_err() {
            ctx.binds = binds;
            ctx.used = used;
            ctx.modules = modules;
        }
        node
    }

    pub fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> Option<Value> {
        let eid = self.spec.id;
        let res = match &mut self.kind {
            NodeKind::Error { .. } => None,
            NodeKind::Constant(v) => match event {
                Event::Init => Some(v.clone()),
                Event::Netidx(_, _)
                | Event::Rpc(_, _)
                | Event::Timer(_, _)
                | Event::User(_)
                | Event::Variable(_, _) => None,
            },
            NodeKind::Apply { args, function } => function.update(ctx, args, event),
            NodeKind::Connect(id, rhs) | NodeKind::Bind(id, rhs) => {
                if let Some(v) = rhs.update(ctx, event) {
                    if ctx.dbg_ctx.trace {
                        ctx.dbg_ctx.add_event(eid, Some(event.clone()), v.clone())
                    }
                    ctx.user.set_var(*id, v)
                }
                None
            }
            NodeKind::Ref(bid) => match event {
                Event::Variable(id, v) if bid == id => Some(v.clone()),
                Event::Init
                | Event::Netidx(_, _)
                | Event::Rpc(_, _)
                | Event::Timer(_, _)
                | Event::User(_)
                | Event::Variable { .. } => None,
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
            NodeKind::Use | NodeKind::Lambda(_) => None,
        };
        if ctx.dbg_ctx.trace {
            if let Some(v) = &res {
                ctx.dbg_ctx.add_event(eid, Some(event.clone()), v.clone())
            }
        }
        res
    }
}
