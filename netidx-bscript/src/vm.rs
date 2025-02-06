use crate::{
    expr::{Expr, ExprId, ExprKind, FnType, ModPath, Pattern, Type},
    stdfn,
};
use anyhow::{bail, Result};
use arcstr::{literal, ArcStr};
use chrono::prelude::*;
use compact_str::{format_compact, CompactString};
use enumflags2::BitFlags;
use fxhash::FxHashMap;
use immutable_chunkmap::{map::MapS as Map, set::SetS as Set};
use netidx::{
    path::Path,
    publisher::Typ,
    subscriber::{Dval, SubId, UpdatesFlags, Value},
    utils::Either,
};
use smallvec::{smallvec, SmallVec};
use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    fmt::{self, Debug},
    iter, mem,
    sync::{self, LazyLock, Weak},
    time::Duration,
};
use triomphe::Arc;

pub struct DbgCtx<E: Debug> {
    pub trace: bool,
    events: VecDeque<(ExprId, (DateTime<Local>, Option<Event<E>>, Value))>,
    watch: FxHashMap<
        ExprId,
        Vec<Weak<dyn Fn(&DateTime<Local>, &Option<Event<E>>, &Value) + Send + Sync>>,
    >,
    current: FxHashMap<ExprId, (Option<Event<E>>, Value)>,
}

impl<E: Debug + Clone> DbgCtx<E> {
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

atomic_id!(BindId);
atomic_id!(LambdaId);
atomic_id!(SelectId);

#[derive(Clone, Debug)]
pub enum Event<E: Debug> {
    Init,
    Variable(BindId, Value),
    Netidx(SubId, Value),
    User(E),
}

pub type InitFn<C, E> = sync::Arc<
    dyn for<'a, 'b, 'c> Fn(
            &'a mut ExecCtx<C, E>,
            &'b [Node<C, E>],
            ExprId,
        ) -> Result<Box<dyn Apply<C, E> + Send + Sync>>
        + Send
        + Sync,
>;

pub trait Init<C: Ctx, E: Debug + Clone> {
    const NAME: &str;

    fn init(ctx: &mut ExecCtx<C, E>) -> InitFn<C, E>;
}

pub trait Apply<C: Ctx, E: Debug + Clone> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value>;
}

pub trait Ctx {
    fn clear(&mut self);

    /// Subscribe to the specified netidx path. When the subscription
    /// updates you are expected to deliver Netidx events to the
    /// expression specified by ref_by.
    fn durable_subscribe(
        &mut self,
        flags: UpdatesFlags,
        path: Path,
        ref_by: ExprId,
    ) -> Dval;

    /// Called when a subscription is no longer needed
    fn unsubscribe(&mut self, path: Path, dv: Dval, ref_by: ExprId);

    /// This will be called by the compiler whenever a bound variable
    /// is referenced. The ref_by is the toplevel expression that
    /// contains the variable reference. When a variable event
    /// happens, you should update all the toplevel expressions that
    /// ref that variable.
    ///
    /// ref_var will also be called when a bound lambda expression is
    /// referenced, in that case the ref_by id will be the toplevel
    /// expression containing the call site.
    fn ref_var(&mut self, id: BindId, ref_by: ExprId);
    fn unref_var(&mut self, id: BindId, ref_by: ExprId);

    /// Called when a variable updates. All expressions that ref the
    /// id should be updated with a Variable event when this happens.
    fn set_var(&mut self, id: BindId, value: Value);

    /// For a given name, this must have at most one outstanding call
    /// at a time, and must preserve the order of the calls. Calls to
    /// different names may execute concurrently.
    ///
    /// when the rpc returns you are expected to deliver a Variable
    /// event with the specified id to the expression specified by
    /// ref_by.
    fn call_rpc(
        &mut self,
        name: Path,
        args: Vec<(ArcStr, Value)>,
        ref_by: ExprId,
        id: BindId,
    );

    /// arrange to have a Timer event delivered after timeout. When
    /// the timer expires you are expected to deliver a Variable event
    /// for the id, containing the current time.
    fn set_timer(&mut self, id: BindId, timeout: Duration, ref_by: ExprId);
}

struct Bind<C: Ctx + 'static, E: Debug + Clone + 'static> {
    id: BindId,
    scope: ModPath,
    name: CompactString,
    export: bool,
    typ: Option<Type>,
    fun: Option<InitFn<C, E>>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> fmt::Debug for Bind<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Bind {{ id: {:?}, export: {}, fun: {} }}",
            self.id,
            self.export,
            self.fun.is_some()
        )
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Clone for Bind<C, E> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            scope: self.scope.clone(),
            name: self.name.clone(),
            export: self.export,
            typ: self.typ.clone(),
            fun: self.fun.as_ref().map(|fun| sync::Arc::clone(fun)),
        }
    }
}

struct Env<C: Ctx + 'static, E: Debug + Clone + 'static> {
    by_id: Map<BindId, Bind<C, E>>,
    binds: Map<ModPath, Map<CompactString, BindId>>,
    used: Map<ModPath, Arc<Vec<ModPath>>>,
    modules: Set<ModPath>,
    typedefs: Map<ModPath, Map<CompactString, Type>>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Clone for Env<C, E> {
    fn clone(&self) -> Self {
        Self {
            by_id: self.by_id.clone(),
            binds: self.binds.clone(),
            used: self.used.clone(),
            modules: self.modules.clone(),
            typedefs: self.typedefs.clone(),
        }
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Env<C, E> {
    fn new() -> Self {
        Self {
            by_id: Map::new(),
            binds: Map::new(),
            used: Map::new(),
            modules: Set::new(),
            typedefs: Map::new(),
        }
    }

    fn clear(&mut self) {
        let Self { by_id, binds, used, modules, typedefs } = self;
        *by_id = Map::new();
        *binds = Map::new();
        *used = Map::new();
        *modules = Set::new();
        *typedefs = Map::new();
    }

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
                        if let Some(Path::SEP) = buf.chars().next_back() {
                            buf.pop();
                        }
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
            self.binds.get_full(scope).and_then(|(scope, vars)| {
                vars.get(name)
                    .and_then(|bid| self.by_id.get(bid).map(|bind| (scope, bind)))
            })
        })
    }

    // alias orig_id -> new_id. orig_id will be removed, and it's in
    // scope entry will point to new_id
    fn alias(&mut self, orig_id: BindId, new_id: BindId) {
        if let Some(bind) = self.by_id.remove_cow(&orig_id) {
            if let Some(binds) = self.binds.get_mut_cow(&bind.scope) {
                if let Some(id) = binds.get_mut_cow(bind.name.as_str()) {
                    *id = new_id
                }
            }
        }
    }

    // create a new binding. If an existing bind exists in the same
    // scope shadow it.
    fn bind_variable(&mut self, scope: &ModPath, name: &str) -> &mut Bind<C, E> {
        let binds = self.binds.get_or_default_cow(scope.clone());
        let mut existing = true;
        let id = binds.get_or_insert_cow(CompactString::from(name), || {
            existing = false;
            BindId::new()
        });
        if existing {
            *id = BindId::new();
        }
        self.by_id.get_or_insert_cow(*id, || Bind {
            export: true,
            id: *id,
            scope: scope.clone(),
            name: CompactString::from(name),
            typ: None,
            fun: None,
        })
    }

    fn resolve_typrefs<'a>(
        &self,
        scope: &ModPath,
        typ: &'a Type,
    ) -> Result<Cow<'a, Type>> {
        match typ {
            Type::Bottom => Ok(Cow::Borrowed(typ)),
            Type::Primitive(_) => Ok(Cow::Borrowed(typ)),
            Type::Ref(name) => {
                let scope = match Path::dirname(&**name) {
                    None => scope,
                    Some(dn) => &*scope.append(dn),
                };
                let name = Path::basename(&**name).unwrap_or("");
                match self.typedefs.get(scope) {
                    None => bail!("undefined type {name} in scope {scope}"),
                    Some(defs) => match defs.get(name) {
                        None => bail!("undefined type {name} in scope {scope}"),
                        Some(typ) => Ok(Cow::Owned(typ.clone())),
                    },
                }
            }
            Type::Set(ts) => {
                let mut res: SmallVec<[Cow<Type>; 20]> = smallvec![];
                for t in ts.iter() {
                    res.push(self.resolve_typrefs(scope, t)?)
                }
                let borrowed = res.iter().all(|t| match t {
                    Cow::Borrowed(_) => true,
                    Cow::Owned(_) => false,
                });
                if borrowed {
                    Ok(Cow::Borrowed(typ))
                } else {
                    let iter = res.into_iter().map(|t| t.into_owned());
                    Ok(Cow::Owned(Type::Set(Arc::from_iter(iter))))
                }
            }
            Type::Fn(f) => {
                let vargs = self.resolve_typrefs(scope, &f.vargs)?;
                let rtype = self.resolve_typrefs(scope, &f.rtype)?;
                let mut res: SmallVec<[Cow<Type>; 20]> = smallvec![];
                for t in f.args.iter() {
                    res.push(self.resolve_typrefs(scope, t)?);
                }
                let borrowed =
                    res.iter().chain(iter::once(&vargs)).chain(iter::once(&rtype)).all(
                        |t| match t {
                            Cow::Borrowed(_) => true,
                            Cow::Owned(_) => false,
                        },
                    );
                if borrowed {
                    Ok(Cow::Borrowed(typ))
                } else {
                    Ok(Cow::Owned(Type::Fn(Arc::new(FnType {
                        args: Arc::from_iter(res.into_iter().map(|t| t.into_owned())),
                        rtype: rtype.into_owned(),
                        vargs: vargs.into_owned(),
                    }))))
                }
            }
        }
    }
}

pub struct ExecCtx<C: Ctx + 'static, E: Debug + Clone + 'static> {
    env: Env<C, E>,
    builtins: FxHashMap<&'static str, InitFn<C, E>>,
    pub dbg_ctx: DbgCtx<E>,
    pub user: C,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> ExecCtx<C, E> {
    pub fn clear(&mut self) {
        self.env.clear();
        self.dbg_ctx.clear();
        self.user.clear();
    }

    /// build a new context with only the core library
    pub fn new_no_std(user: C) -> Self {
        let mut t = ExecCtx {
            env: Env::new(),
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
        self.builtins.insert(T::NAME, f);
    }
}

struct Lambda<C: Ctx + 'static, E: Debug + Clone + 'static> {
    eid: ExprId,
    argids: Vec<BindId>,
    body: Node<C, E>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Apply<C, E> for Lambda<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        for (arg, id) in from.iter_mut().zip(&self.argids) {
            match arg {
                Node { kind: NodeKind::Ref(_), .. } => (), // the bind is forwarded into the body
                arg => {
                    if let Some(v) = arg.update(ctx, event) {
                        if ctx.dbg_ctx.trace {
                            ctx.dbg_ctx.add_event(
                                self.eid,
                                Some(event.clone()),
                                v.clone(),
                            )
                        }
                        ctx.user.set_var(*id, v)
                    }
                }
            }
        }
        self.body.update(ctx, event)
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Lambda<C, E> {
    fn new(
        ctx: &mut ExecCtx<C, E>,
        argspec: Arc<[ArcStr]>,
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
        for (name, node) in argspec.iter().zip(args.iter()) {
            let bind = ctx.env.bind_variable(&scope, &**name);
            match &node.kind {
                NodeKind::Ref(id) => {
                    argids.push(*id);
                    let old_id = bind.id;
                    ctx.env.alias(old_id, *id)
                }
                _ => {
                    argids.push(bind.id);
                    bind.fun = node.find_lambda();
                }
            }
        }
        let body = Node::compile_int(ctx, body, &scope, tid);
        match body.extract_err() {
            None => Ok(Self { argids, eid, body }),
            Some(e) => bail!("{e}"),
        }
    }
}

pub struct Cached<C: Ctx + 'static, E: Debug + Clone + 'static> {
    pub cached: Option<Value>,
    pub node: Node<C, E>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Cached<C, E> {
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

pub enum PatternNode<C: Ctx + 'static, E: Debug + Clone + 'static> {
    Underscore,
    Typ { tag: BitFlags<Typ>, bind: BindId, guard: Option<Cached<C, E>> },
    Error(ArcStr),
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> PatternNode<C, E> {
    fn extract_err(&self) -> Option<&ArcStr> {
        match self {
            PatternNode::Underscore
            | PatternNode::Typ { tag: _, bind: _, guard: None } => None,
            PatternNode::Typ { tag: _, bind: _, guard: Some(c) } => c.node.extract_err(),
            PatternNode::Error(e) => Some(e),
        }
    }

    fn extract_guard(self) -> Option<Node<C, E>> {
        match self {
            PatternNode::Typ { tag: _, bind: _, guard: Some(g) } => Some(g.node),
            PatternNode::Underscore
            | PatternNode::Error(_)
            | PatternNode::Typ { tag: _, bind: _, guard: None } => None,
        }
    }

    fn ptype(&self) -> Type {
        match self {
            PatternNode::Typ { tag, bind: _, guard: None } => Type::Primitive(*tag),
            PatternNode::Underscore => Type::any(),
            PatternNode::Typ { tag: _, bind: _, guard: Some(_) }
            | PatternNode::Error(_) => Type::Bottom,
        }
    }

    fn compile(
        ctx: &mut ExecCtx<C, E>,
        atyp: &Type,
        spec: &Pattern,
        scope: &ModPath,
        top_id: ExprId,
    ) -> Self {
        match spec {
            Pattern::Underscore => PatternNode::Underscore,
            Pattern::Typ { tag, bind, guard } => {
                let tag = if tag.len() == 0 { Typ::any() } else { *tag };
                let ptyp = Type::Primitive(tag);
                if atyp.contains(&ptyp) {
                    return PatternNode::Error(
                        format_compact!("{ptyp} will never match {atyp}").as_str().into(),
                    );
                }
                let bind = ctx.env.bind_variable(scope, &**bind);
                let id = bind.id;
                bind.typ = Some(ptyp);
                let guard = guard.as_ref().map(|g| {
                    Cached::new(Node::compile_int(ctx, g.clone(), &scope, top_id))
                });
                PatternNode::Typ { tag, bind: id, guard }
            }
        }
    }

    fn bind(&self) -> Option<BindId> {
        match self {
            PatternNode::Underscore | PatternNode::Error(_) => None,
            PatternNode::Typ { tag: _, bind, guard: _ } => Some(*bind),
        }
    }

    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> bool {
        match self {
            PatternNode::Underscore
            | PatternNode::Typ { tag: _, bind: _, guard: None }
            | PatternNode::Error(_) => false,
            PatternNode::Typ { tag: _, bind: _, guard: Some(g) } => g.update(ctx, event),
        }
    }

    fn is_match(&self, arg: Option<&Value>) -> (bool, Option<BindId>) {
        match (arg, self) {
            (_, PatternNode::Underscore) => (true, None),
            (Some(arg), PatternNode::Typ { tag, bind, guard: None }) => {
                let typ = BitFlags::from_iter([Typ::get(arg)]);
                (tag.contains(typ), Some(*bind))
            }
            (Some(arg), PatternNode::Typ { tag, bind, guard: Some(g) }) => {
                let typ = BitFlags::from_iter([Typ::get(arg)]);
                let is_match = tag.contains(typ)
                    && g.cached
                        .as_ref()
                        .and_then(|v| v.clone().get_as::<bool>())
                        .unwrap_or(false);
                (is_match, Some(*bind))
            }
            (Some(_), PatternNode::Error(_)) | (None, _) => (false, None),
        }
    }
}

pub enum NodeKind<C: Ctx + 'static, E: Debug + Clone + 'static> {
    Constant(Value),
    Module(Vec<Node<C, E>>),
    Use,
    Do(Vec<Node<C, E>>),
    Bind(BindId, Box<Node<C, E>>),
    Ref(BindId),
    Connect(BindId, Box<Node<C, E>>),
    Lambda(InitFn<C, E>),
    TypeCast {
        target: Typ,
        n: Box<Node<C, E>>,
    },
    TypeDef,
    Apply {
        args: Vec<Node<C, E>>,
        function: Box<dyn Apply<C, E> + Send + Sync>,
    },
    Select {
        selected: Option<usize>,
        arg: Box<Cached<C, E>>,
        arms: Vec<(PatternNode<C, E>, Cached<C, E>)>,
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
        node: Box<Cached<C, E>>,
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
        children: Vec<Node<C, E>>,
    },
}

pub struct Node<C: Ctx + 'static, E: Debug + Clone + 'static> {
    pub spec: Expr,
    pub typ: Option<Type>,
    pub kind: NodeKind<C, E>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> fmt::Display for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.spec)
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Node<C, E> {
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
            | NodeKind::Add { .. }
            | NodeKind::Sub { .. }
            | NodeKind::Mul { .. }
            | NodeKind::Div { .. }
            | NodeKind::TypeCast { .. }
            | NodeKind::TypeDef
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
            | NodeKind::Add { .. }
            | NodeKind::Sub { .. }
            | NodeKind::Mul { .. }
            | NodeKind::Div { .. }
            | NodeKind::TypeCast { .. }
            | NodeKind::TypeDef
            | NodeKind::Select { .. } => false,
        }
    }

    /// extracts the first error
    pub fn extract_err(&self) -> Option<&ArcStr> {
        match &self.kind {
            NodeKind::Error { error: Some(e), .. } => Some(e),
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
            | NodeKind::Add { .. }
            | NodeKind::Sub { .. }
            | NodeKind::Mul { .. }
            | NodeKind::Div { .. }
            | NodeKind::TypeCast { .. }
            | NodeKind::TypeDef
            | NodeKind::Select { .. } => None,
        }
    }

    fn compile_lambda(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        argspec: Arc<[ArcStr]>,
        scope: &ModPath,
        body: Either<Arc<Expr>, ArcStr>,
        eid: ExprId,
    ) -> Node<C, E> {
        use sync::Arc;
        if argspec.len() != argspec.iter().collect::<Set<_>>().len() {
            let e = literal!("arguments must have unique names");
            let kind = NodeKind::Error { error: Some(e), children: vec![] };
            return Node { spec, kind };
        }
        // compile the lambda at the call site with the env at the
        // definition site this ensures all variables and modules are
        // lexically scoped.
        let env = ctx.env.clone();
        let scope = scope.clone();
        let f: InitFn<C, E> = Arc::new(move |ctx, args, tid| match body.clone() {
            Either::Left(body) => {
                let old_env = mem::replace(&mut ctx.env, env.clone());
                let res = Lambda::new(
                    ctx,
                    argspec.clone(),
                    args,
                    &scope,
                    eid,
                    tid,
                    (*body).clone(),
                );
                // it's ok that definitions in the lambda are dropped, they can't be used
                // outside the scope of the lambda expression anyway
                ctx.env = old_env;
                Ok(Box::new(res?))
            }
            Either::Right(builtin) => match ctx.builtins.get(&*builtin) {
                None => bail!("unknown builtin function {builtin}"),
                Some((arity, init)) => {
                    let init = Arc::clone(init);
                    let l = args.len();
                    match *arity {
                        Arity::Any => init(ctx, args, tid),
                        Arity::AtLeast(n) if l >= n => init(ctx, args, tid),
                        Arity::AtLeast(n) => {
                            bail!("expected at least {n} args")
                        }
                        Arity::Exactly(n) if l == n => init(ctx, args, tid),
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
            ("", $children:expr) => {{
                let kind = NodeKind::Error { error: None, children: $children };
                Node { spec, kind, typ: Some(Type::Primitive(Typ::Result.into())) }
            }};
            ($fmt:expr, $children:expr, $($arg:expr),*) => {{
                let e = ArcStr::from(format_compact!($fmt, $($arg),*).as_str());
                let kind = NodeKind::Error { error: Some(e), children: $children };
                Node { spec, kind, typ: Some(Type::Primitive(Typ::Result.into())) }
            }};
            ($fmt:expr) => { error!($fmt, vec![],) };
            ($fmt:expr, $children:expr) => { error!($fmt, $children,) };
        }
        macro_rules! typ {
            ($n:expr) => {
                match &$n.typ {
                    None => return error!("type must be known", vec![$n]),
                    Some(t) => t.clone(),
                }
            };
        }
        macro_rules! typchk {
            ($n:expr, $t:expr) => {{
                match &$n.typ {
                    Some(typ) => {
                        if !$t.contains(&typ) {
                            return error!(
                                "type mismatch. got {} expected {}",
                                vec![$n],
                                typ,
                                $t
                            );
                        }
                    }
                    None => match &$n.kind {
                        NodeKind::Ref(id) => match ctx.env.by_id.get_mut_cow(id) {
                            None => return error!("type must be known", vec![$n]),
                            Some(bind) => {
                                bind.typ = Some($t.clone());
                                $n.typ = Some($t.clone());
                            }
                        },
                        _ => return error!("type must be known", vec![$n]),
                    },
                }
            }};
        }
        macro_rules! binary_op {
            ($op:ident, $lhs:expr, $rhs:expr, $at:expr, $rt:expr) => {{
                let lhs = Node::compile_int(ctx, (**$lhs).clone(), scope, top_id);
                let rhs = Node::compile_int(ctx, (**$rhs).clone(), scope, top_id);
                if lhs.is_err() || rhs.is_err() {
                    return error!("", vec![lhs, rhs]);
                }
                typchk!(lhs, $at);
                typchk!(rhs, $at);
                let lhs = Box::new(Cached::new(lhs));
                let rhs = Box::new(Cached::new(rhs));
                Node { spec, typ: Some($rt), kind: NodeKind::$op { lhs, rhs } }
            }};
        }
        match &spec {
            Expr { kind: ExprKind::Constant(v), id: _ } => Node {
                kind: NodeKind::Constant(v.clone()),
                spec,
                typ: Some(Type::Primitive(Typ::get(&v).into())),
            },
            Expr { kind: ExprKind::Do { exprs }, id } => {
                let scope = ModPath(scope.append(&format_compact!("do{}", id.inner())));
                let (error, exp) = subexprs!(scope, exprs);
                if error {
                    error!("", exp)
                } else {
                    let typ = match exp.pop() {
                        None => Type::Bottom,
                        Some(n) => {
                            let t = typ!(n);
                            exp.push(n);
                            t
                        }
                    };
                    Node { kind: NodeKind::Do(exp), spec, typ: Some(typ) }
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
                            ctx.env.modules.insert_cow(scope.clone());
                            Node {
                                spec,
                                typ: Some(Type::Bottom),
                                kind: NodeKind::Module(children),
                            }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Use { name }, id: _ } => {
                if !ctx.env.modules.contains(name) {
                    error!("no such module {name}")
                } else {
                    let used = ctx.env.used.get_or_default_cow(scope.clone());
                    Arc::make_mut(used).push(name.clone());
                    Node { spec, typ: Some(Type::Bottom), kind: NodeKind::Use }
                }
            }
            Expr { kind: ExprKind::Connect { name, value }, id: _ } => {
                match ctx.env.lookup_bind(scope, name) {
                    None => error!("{name} is undefined"),
                    Some((_, Bind { fun: Some(_), .. })) => {
                        error!("{name} is a function")
                    }
                    Some((_, Bind { fun: None, typ: None, .. })) => {
                        error!("bind type must be known")
                    }
                    Some((_, Bind { id, fun: None, typ: Some(typ), .. })) => {
                        let id = *id;
                        let node =
                            Node::compile_int(ctx, (**value).clone(), scope, top_id);
                        if node.is_err() {
                            error!("", vec![node])
                        } else {
                            typchk!(node, typ);
                            let kind = NodeKind::Connect(id, Box::new(node));
                            Node { spec, typ: Some(Type::Bottom), kind }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Lambda { args, vargs, rtype, body }, id } => {
                let (args, body, id) = (args.clone(), (*body).clone(), *id);
                Node::compile_lambda(ctx, spec, args, scope, body, id)
            }
            Expr { kind: ExprKind::Apply { args, function: f }, id: _ } => {
                let (error, mut args) = subexprs!(scope, args);
                match ctx.env.lookup_bind(scope, f) {
                    None => error!("{f} is undefined"),
                    Some((_, Bind { fun: None, .. })) => {
                        error!("{f} is not a function")
                    }
                    Some((_, Bind { typ: None, .. })) => {
                        error!("type unknown")
                    }
                    Some((_, Bind { fun: Some(i), typ: Some(Type::Fn(ft)), id, .. })) => {
                        let varid = *id;
                        let init = sync::Arc::clone(i);
                        if error {
                            return error!("", args);
                        }
                        if (args.len() > ft.args.len() && ft.vargs.is_bot())
                            || args.len() < ft.args.len()
                        {
                            return error!("{f} expected {} args", args, ft.args.len());
                        }
                        for i in 0..args.len() {
                            let ftyp =
                                if i < ft.args.len() { &ft.args[i] } else { &ft.vargs };
                            let atyp = match &args[i].typ {
                                Some(typ) => typ,
                                None => match &args[i].kind {
                                    NodeKind::Ref(id) => {
                                        match ctx.env.by_id.get_mut_cow(id) {
                                            None => return error!("missing ref", args),
                                            Some(bind) => {
                                                bind.typ = Some(ftyp.clone());
                                                args[i].typ = Some(ftyp.clone());
                                                &ftyp
                                            }
                                        }
                                    }
                                    _ => return error!("{f} arg {i} type unknown", args),
                                },
                            };
                            if !ftyp.contains(atyp) {
                                return error!("type mismatch {f} arg {i} expected {ftyp} got {atyp}");
                            }
                        }
                        match init(ctx, &args, top_id) {
                            Err(e) => error!("error in function {f} {e:?}"),
                            Ok(function) => {
                                ctx.user.ref_var(varid, top_id);
                                Node {
                                    spec,
                                    typ: Some(ft.rtype),
                                    kind: NodeKind::Apply { args, function },
                                }
                            }
                        }
                    }
                    Some((_, Bind { typ: Some(t), .. })) => {
                        error!("{f} has type {t} which is not a function")
                    }
                }
            }
            Expr { kind: ExprKind::Bind { export: _, name, typ, value }, id: _ } => {
                let node = Node::compile_int(ctx, (**value).clone(), &scope, top_id);
                if let Some(typ) = typ {
                    match ctx.env.resolve_typrefs(scope, typ) {
                        Ok(t) => typchk!(node, &*t),
                        Err(e) => return error!("{e}", vec![node]),
                    }
                }
                let bind = ctx.env.bind_variable(scope, &**name);
                bind.typ = Some(typ!(node));
                bind.fun = node.find_lambda();
                if node.is_err() {
                    error!("", vec![node])
                } else {
                    Node {
                        spec,
                        typ: Some(Type::Bottom),
                        kind: NodeKind::Bind(bind.id, Box::new(node)),
                    }
                }
            }
            Expr { kind: ExprKind::Ref { name }, id: _ } => {
                match ctx.env.lookup_bind(scope, name) {
                    None => error!("{name} not defined"),
                    Some((_, bind)) => {
                        ctx.user.ref_var(bind.id, top_id);
                        let typ = bind.typ.clone();
                        match &bind.fun {
                            None => Node { spec, typ, kind: NodeKind::Ref(bind.id) },
                            Some(i) => {
                                Node { spec, typ, kind: NodeKind::Lambda(i.clone()) }
                            }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Select { arg, arms }, id: _ } => {
                let arg = Node::compile_int(ctx, (**arg).clone(), scope, top_id);
                if let Some(e) = arg.extract_err() {
                    return error!("{e}");
                }
                let atyp = typ!(arg);
                let arg = Box::new(Cached::new(arg));
                let (mut error, arms, ptyp, typ) = arms.iter().fold(
                    (false, vec![], Type::Bottom, Type::Bottom),
                    |(e, mut nodes, ptyp, rtyp), (pat, spec)| {
                        let scope = ModPath(
                            scope.append(&format_compact!("sel{}", SelectId::new().0)),
                        );
                        let pat = PatternNode::compile(ctx, &atyp, pat, &scope, top_id);
                        let n = Node::compile_int(ctx, spec.clone(), &scope, top_id);
                        let rtyp = n.typ.as_ref().unwrap_or(&Type::Bottom).union(&rtyp);
                        let ptyp = ptyp.union(&pat.ptype());
                        let e = e
                            || pat.extract_err().is_some()
                            || n.is_err() | n.typ.is_none();
                        nodes.push((pat, Cached::new(n)));
                        (e, nodes, ptyp, rtyp)
                    },
                );
                let mut err = CompactString::new("");
                if !atyp.contains(&ptyp) {
                    error = true;
                    let e =
                        format!("select expected to match {atyp}, only matches {ptyp}");
                    err.push_str(e.as_str())
                }
                if error {
                    let mut v = vec![];
                    for (pat, n) in arms {
                        if n.node.typ.is_none() {
                            err.push_str("type must be known, ");
                        }
                        if let Some(e) = pat.extract_err() {
                            err.push_str(e.as_str());
                            err.push_str(", ");
                        }
                        if let Some(g) = pat.extract_guard() {
                            v.push(g);
                        }
                        v.push(n.node)
                    }
                    return error!("{err}", v);
                }
                Node {
                    spec,
                    typ: Some(typ),
                    kind: NodeKind::Select { selected: None, arg, arms },
                }
            }
            Expr { kind: ExprKind::Not { expr }, id: _ } => {
                let node = Node::compile_int(ctx, (**expr).clone(), scope, top_id);
                if node.is_err() {
                    return error!("", vec![node]);
                }
                let at = Type::boolean();
                typchk!(node, at);
                let node = Box::new(Cached::new(node));
                Node { spec, typ: Some(at), kind: NodeKind::Not { node } }
            }
            Expr { kind: ExprKind::Eq { lhs, rhs }, id: _ } => {
                binary_op!(Eq, lhs, rhs, Type::any(), Type::boolean())
            }
            Expr { kind: ExprKind::Ne { lhs, rhs }, id: _ } => {
                binary_op!(Ne, lhs, rhs, Type::any(), Type::boolean())
            }
            Expr { kind: ExprKind::Lt { lhs, rhs }, id: _ } => {
                binary_op!(Lt, lhs, rhs, Type::any(), Type::boolean())
            }
            Expr { kind: ExprKind::Gt { lhs, rhs }, id: _ } => {
                binary_op!(Gt, lhs, rhs, Type::any(), Type::boolean())
            }
            Expr { kind: ExprKind::Lte { lhs, rhs }, id: _ } => {
                binary_op!(Lte, lhs, rhs, Type::any(), Type::boolean())
            }
            Expr { kind: ExprKind::Gte { lhs, rhs }, id: _ } => {
                binary_op!(Gte, lhs, rhs, Type::any(), Type::boolean())
            }
            Expr { kind: ExprKind::And { lhs, rhs }, id: _ } => {
                binary_op!(And, lhs, rhs, Type::boolean(), Type::boolean())
            }
            Expr { kind: ExprKind::Or { lhs, rhs }, id: _ } => {
                binary_op!(Or, lhs, rhs, Type::boolean(), Type::boolean())
            }
            Expr { kind: ExprKind::Add { lhs, rhs }, id: _ } => {
                binary_op!(Add, lhs, rhs, Type::number(), Type::number())
            }
            Expr { kind: ExprKind::Sub { lhs, rhs }, id: _ } => {
                binary_op!(Sub, lhs, rhs, Type::number(), Type::number())
            }
            Expr { kind: ExprKind::Mul { lhs, rhs }, id: _ } => {
                binary_op!(Mul, lhs, rhs, Type::number(), Type::number())
            }
            Expr { kind: ExprKind::Div { lhs, rhs }, id: _ } => {
                binary_op!(Div, lhs, rhs, Type::number(), Type::number())
            }
        }
    }

    pub fn compile(ctx: &mut ExecCtx<C, E>, scope: &ModPath, spec: Expr) -> Self {
        let top_id = spec.id;
        let env = ctx.env.clone();
        let node = Self::compile_int(ctx, spec, scope, top_id);
        if node.is_err() {
            ctx.env = env;
        }
        node
    }

    fn update_select(
        ctx: &mut ExecCtx<C, E>,
        selected: &mut Option<usize>,
        arg: &mut Cached<C, E>,
        arms: &mut Vec<(PatternNode<C, E>, Cached<C, E>)>,
        event: &Event<E>,
    ) -> Option<Value> {
        let mut val_up: SmallVec<[bool; 16]> = smallvec![];
        let arg_up = arg.update(ctx, event);
        macro_rules! set_arg {
            ($i:expr) => {
                if let Some(id) = arms[$i].0.bind() {
                    if let Some(arg) = arg.cached.as_ref() {
                        val_up[$i] |=
                            arms[$i].1.update(ctx, &Event::Variable(id, arg.clone()));
                    }
                }
            };
        }
        macro_rules! val {
            ($i:expr) => {{
                if arg_up {
                    set_arg!($i)
                }
                if val_up[$i] {
                    arms[$i].1.cached.clone()
                } else {
                    None
                }
            }};
        }
        let mut pat_up = false;
        for (pat, val) in arms.iter_mut() {
            pat_up |= pat.update(ctx, event);
            if arg_up {
                if let Some(id) = pat.bind() {
                    if let Some(arg) = arg.cached.as_ref() {
                        pat_up |= pat.update(ctx, &Event::Variable(id, arg.clone()));
                    }
                }
            }
            val_up.push(val.update(ctx, event));
        }
        if !pat_up {
            selected.and_then(|i| val!(i))
        } else {
            let sel = arms.iter().enumerate().find_map(|(i, (pat, _))| {
                match pat.is_match(arg.cached.as_ref()) {
                    (true, id) => Some((i, id)),
                    _ => None,
                }
            });
            match (sel, *selected) {
                (Some((i, _)), Some(j)) if i == j => val!(i),
                (Some((i, _)), Some(_) | None) => {
                    set_arg!(i);
                    *selected = Some(i);
                    arms[i].1.cached.clone()
                }
                (None, Some(_)) => {
                    *selected = None;
                    None
                }
                (None, None) => None,
            }
        }
    }

    pub fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> Option<Value> {
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
            NodeKind::Constant(v) => match event {
                Event::Init => Some(v.clone()),
                Event::Netidx(_, _) | Event::User(_) | Event::Variable(_, _) => None,
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
            NodeKind::Eq { lhs, rhs } => binary_op!(==, lhs, rhs),
            NodeKind::Ne { lhs, rhs } => binary_op!(!=, lhs, rhs),
            NodeKind::Lt { lhs, rhs } => binary_op!(<, lhs, rhs),
            NodeKind::Gt { lhs, rhs } => binary_op!(>, lhs, rhs),
            NodeKind::Lte { lhs, rhs } => binary_op!(<=, lhs, rhs),
            NodeKind::Gte { lhs, rhs } => binary_op!(>=, lhs, rhs),
            NodeKind::And { lhs, rhs } => binary_boolean_op!(&&, lhs, rhs),
            NodeKind::Or { lhs, rhs } => binary_boolean_op!(||, lhs, rhs),
            NodeKind::Not { node } => {
                if node.update(ctx, event) {
                    node.cached.clone()
                } else {
                    None
                }
            }
            NodeKind::Add { lhs, rhs } => binary_op_clone!(+, lhs, rhs),
            NodeKind::Sub { lhs, rhs } => binary_op_clone!(-, lhs, rhs),
            NodeKind::Mul { lhs, rhs } => binary_op_clone!(*, lhs, rhs),
            NodeKind::Div { lhs, rhs } => binary_op_clone!(/, lhs, rhs),
            NodeKind::Select { selected, arg, arms } => {
                Node::update_select(ctx, selected, arg, arms, event)
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
