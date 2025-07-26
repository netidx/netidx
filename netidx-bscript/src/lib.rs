#[macro_use]
extern crate netidx_core;
#[macro_use]
extern crate combine;
#[macro_use]
extern crate serde_derive;

pub mod env;
pub mod expr;
pub mod node;
pub mod rt;
pub mod typ;

use crate::{
    env::Env,
    expr::{ExprId, ModPath},
    typ::{FnType, Type},
};
use anyhow::{bail, Result};
use arcstr::ArcStr;
use expr::Expr;
use fxhash::{FxHashMap, FxHashSet};
use log::info;
use netidx::{
    path::Path,
    publisher::{Id, Val, WriteRequest},
    subscriber::{self, Dval, SubId, UpdatesFlags, Value},
};
use netidx_protocols::rpc::server::{ArgSpec, RpcCall};
use node::compiler;
use parking_lot::RwLock;
use std::{
    any::Any,
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    fmt::Debug,
    mem,
    sync::{
        self,
        atomic::{AtomicBool, Ordering},
        LazyLock,
    },
    time::Duration,
};
use tokio::time::Instant;
use triomphe::Arc;

#[allow(dead_code)]
static TRACE: AtomicBool = AtomicBool::new(false);

#[allow(dead_code)]
fn set_trace(b: bool) {
    TRACE.store(b, Ordering::Relaxed)
}

#[allow(dead_code)]
fn trace() -> bool {
    TRACE.load(Ordering::Relaxed)
}

#[macro_export]
macro_rules! tdbg {
    ($e:expr) => {
        if $crate::trace() {
            dbg!($e)
        } else {
            $e
        }
    };
}

thread_local! {
    /// thread local shared refs structure
    pub static REFS: RefCell<Refs> = RefCell::new(Refs::new());
}

atomic_id!(LambdaId);

impl From<u64> for LambdaId {
    fn from(v: u64) -> Self {
        LambdaId(v)
    }
}

atomic_id!(BindId);

impl From<u64> for BindId {
    fn from(v: u64) -> Self {
        BindId(v)
    }
}

impl TryFrom<Value> for BindId {
    type Error = anyhow::Error;

    fn try_from(value: Value) -> Result<Self> {
        match value {
            Value::U64(id) => Ok(BindId(id)),
            v => bail!("invalid bind id {v}"),
        }
    }
}

#[macro_export]
macro_rules! errf {
    ($pat:expr, $($arg:expr),*) => {
        Some(Value::Error(ArcStr::from(format_compact!($pat, $($arg),*).as_str())))
    };
    ($pat:expr) => { Some(Value::Error(ArcStr::from(format_compact!($pat).as_str()))) };
}

#[macro_export]
macro_rules! err {
    ($pat:literal) => {
        Some(Value::Error(literal!($pat)))
    };
}

pub trait UserEvent: Clone + Debug + Any {
    fn clear(&mut self);
}

#[derive(Debug, Clone)]
pub struct NoUserEvent;

impl UserEvent for NoUserEvent {
    fn clear(&mut self) {}
}

/// Event represents all the things that happened simultaneously in a
/// given execution cycle. Event may contain only one update for each
/// variable and netidx subscription in a given cycle, if more updates
/// happen simultaneously they must be queued and deferred to later
/// cycles.
#[derive(Debug)]
pub struct Event<E: UserEvent> {
    pub init: bool,
    pub variables: FxHashMap<BindId, Value>,
    pub netidx: FxHashMap<SubId, subscriber::Event>,
    pub writes: FxHashMap<Id, WriteRequest>,
    pub rpc_calls: FxHashMap<BindId, RpcCall>,
    pub user: E,
}

impl<E: UserEvent> Event<E> {
    pub fn new(user: E) -> Self {
        Event {
            init: false,
            variables: HashMap::default(),
            netidx: HashMap::default(),
            writes: HashMap::default(),
            rpc_calls: HashMap::default(),
            user,
        }
    }

    pub fn clear(&mut self) {
        let Self { init, variables, netidx, rpc_calls, writes, user } = self;
        *init = false;
        variables.clear();
        netidx.clear();
        rpc_calls.clear();
        writes.clear();
        user.clear();
    }
}

#[derive(Debug, Clone)]
pub struct Refs {
    refed: FxHashSet<BindId>,
    bound: FxHashSet<BindId>,
}

impl Refs {
    pub fn new() -> Self {
        Self { refed: FxHashSet::default(), bound: FxHashSet::default() }
    }

    pub fn clear(&mut self) {
        self.refed.clear();
        self.bound.clear();
    }

    pub fn with_external_refs(&self, mut f: impl FnMut(BindId)) {
        for id in &self.refed {
            if !self.bound.contains(id) {
                f(*id);
            }
        }
    }
}

pub type Node<C, E> = Box<dyn Update<C, E>>;

pub type BuiltInInitFn<C, E> = sync::Arc<
    dyn for<'a, 'b, 'c> Fn(
            &'a mut ExecCtx<C, E>,
            &'a FnType,
            &'b ModPath,
            &'c [Node<C, E>],
            ExprId,
        ) -> Result<Box<dyn Apply<C, E>>>
        + Send
        + Sync
        + 'static,
>;

pub type InitFn<C, E> = sync::Arc<
    dyn for<'a, 'b> Fn(
            &'a mut ExecCtx<C, E>,
            &'b [Node<C, E>],
            ExprId,
        ) -> Result<Box<dyn Apply<C, E>>>
        + Send
        + Sync
        + 'static,
>;

/// Apply is a kind of node that represents a function application. It
/// does not hold ownership of it's arguments, instead those are held
/// by a CallSite node. This allows us to change the function called
/// at runtime without recompiling the arguments.
pub trait Apply<C: Ctx, E: UserEvent>: Debug + Send + Sync + Any {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value>;

    /// delete any internally generated nodes, only needed for
    /// builtins that dynamically generate code at runtime
    fn delete(&mut self, _ctx: &mut ExecCtx<C, E>) {
        ()
    }

    /// apply custom typechecking to the lambda, only needed for
    /// builtins that take lambdas as arguments
    fn typecheck(
        &mut self,
        _ctx: &mut ExecCtx<C, E>,
        _from: &mut [Node<C, E>],
    ) -> Result<()> {
        Ok(())
    }

    /// return the lambdas type, builtins do not need to implement
    /// this, it is implemented by the BuiltIn wrapper
    fn typ(&self) -> Arc<FnType> {
        const EMPTY: LazyLock<Arc<FnType>> = LazyLock::new(|| {
            Arc::new(FnType {
                args: Arc::from_iter([]),
                constraints: Arc::new(RwLock::new(vec![])),
                rtype: Type::Bottom,
                vargs: None,
            })
        });
        Arc::clone(&*EMPTY)
    }

    /// Populate the Refs structure with all the ids bound and refed by this
    /// node. It is only necessary for builtins to implement this if they create
    /// nodes, such as call sites.
    fn refs<'a>(&self, _refs: &mut Refs) {}

    /// put the node to sleep, used in conditions like select for branches that
    /// are not selected. Any cached values should be cleared on sleep.
    fn sleep(&mut self, _ctx: &mut ExecCtx<C, E>);
}

/// Update represents a regular graph node, as opposed to a function
/// application represented by Apply. Regular graph nodes are used for
/// every built in node except for builtin functions.
pub trait Update<C: Ctx, E: UserEvent>: Debug + Send + Sync + Any + 'static {
    /// update the node with the specified event and return any output
    /// it might generate
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value>;

    /// delete the node and it's children from the specified context
    fn delete(&mut self, ctx: &mut ExecCtx<C, E>);

    /// type check the node and it's children
    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()>;

    /// return the node type
    fn typ(&self) -> &Type;

    /// Populate the Refs structure with all the bind ids either refed or bound
    /// by the node and it's children
    fn refs(&self, refs: &mut Refs);

    /// return the original expression used to compile this node
    fn spec(&self) -> &Expr;

    /// put the node to sleep, called on unselected branches
    fn sleep(&mut self, ctx: &mut ExecCtx<C, E>);
}

pub trait BuiltIn<C: Ctx, E: UserEvent> {
    const NAME: &str;
    const TYP: LazyLock<FnType>;

    fn init(ctx: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E>;
}

pub trait Ctx: Debug + 'static {
    fn clear(&mut self);

    /// Subscribe to the specified netidx path. When the subscription
    /// updates you are expected to deliver Netidx events to the
    /// expression specified by ref_by.
    fn subscribe(&mut self, flags: UpdatesFlags, path: Path, ref_by: ExprId) -> Dval;

    /// Called when a subscription is no longer needed
    fn unsubscribe(&mut self, path: Path, dv: Dval, ref_by: ExprId);

    /// List the netidx path, return Value::Null if the path did not
    /// change. When the path did update you should send the output
    /// back as a properly formatted struct with two fields, rows and
    /// columns both containing string arrays.
    fn list(&mut self, id: BindId, path: Path);

    /// List the table at path, return Value::Null if the path did not
    /// change
    fn list_table(&mut self, id: BindId, path: Path);

    /// list or table will no longer be called on this BindId, and
    /// related resources can be cleaned up.
    fn stop_list(&mut self, id: BindId);

    /// Publish the specified value, returning it's Id, which must be
    /// used to update the value and unpublish it. If the path is
    /// already published, return an error.
    fn publish(&mut self, path: Path, value: Value, ref_by: ExprId) -> Result<Val>;

    /// Update the specified value
    fn update(&mut self, id: &Val, value: Value);

    /// Stop publishing the specified id
    fn unpublish(&mut self, id: Val, ref_by: ExprId);

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

    /// Called by the ExecCtx when set_var is called on it. All
    /// expressions that ref the id should be updated when this
    /// happens.
    ///
    /// The runtime must deliver all set_vars in a single event except
    /// that set_vars for the same variable in the same cycle must be
    /// queued and deferred to the next cycle.
    ///
    /// The runtime MUST NOT change event while a cycle is in
    /// progress. set_var must be queued until the cycle ends and then
    /// presented as a new batch.
    fn set_var(&mut self, id: BindId, value: Value);

    /// This must return results from the same path in the call order.
    ///
    /// when the rpc returns you are expected to deliver a Variable
    /// event with the specified id to the expression specified by
    /// ref_by.
    fn call_rpc(&mut self, name: Path, args: Vec<(ArcStr, Value)>, id: BindId);

    /// Publish an rpc at the specified path with the specified
    /// procedure level doc and arg spec.
    ///
    /// When the RPC is called the rpc table in event will be
    /// populated under the specified bind id.
    ///
    /// If the procedure is already published an error will be
    /// returned
    fn publish_rpc(
        &mut self,
        name: Path,
        doc: Value,
        spec: Vec<ArgSpec>,
        id: BindId,
    ) -> Result<()>;

    /// unpublish the rpc identified by the bind id.
    fn unpublish_rpc(&mut self, name: Path);

    /// arrange to have a Timer event delivered after timeout. When
    /// the timer expires you are expected to deliver a Variable event
    /// for the id, containing the current time.
    fn set_timer(&mut self, id: BindId, timeout: Duration);
}

pub struct ExecCtx<C: Ctx, E: UserEvent> {
    builtins: FxHashMap<&'static str, (FnType, BuiltInInitFn<C, E>)>,
    tags: FxHashSet<ArcStr>,
    pub env: Env<C, E>,
    pub cached: FxHashMap<BindId, Value>,
    pub user: C,
}

impl<C: Ctx, E: UserEvent> ExecCtx<C, E> {
    pub fn clear(&mut self) {
        self.env.clear();
        self.user.clear();
    }

    /// Build a new execution context.
    ///
    /// This is a very low level interface that you can use to build a
    /// custom runtime with deep integration to your code. It is very
    /// difficult to use, and if you don't implement everything
    /// correctly the semantics of the language can be wrong.
    ///
    /// Most likely you want to use the `rt` module instead.
    pub fn new(user: C) -> Self {
        Self {
            env: Env::new(),
            builtins: FxHashMap::default(),
            tags: FxHashSet::default(),
            cached: HashMap::default(),
            user,
        }
    }

    pub fn register_builtin<T: BuiltIn<C, E>>(&mut self) -> Result<()> {
        let f = T::init(self);
        match self.builtins.entry(T::NAME) {
            Entry::Vacant(e) => {
                e.insert((T::TYP.clone(), f));
            }
            Entry::Occupied(_) => bail!("builtin {} is already registered", T::NAME),
        }
        Ok(())
    }

    /// Built in functions should call this when variables are set
    /// unless they are sure the variable does not need to be
    /// cached. This will also call the user ctx set_var.
    pub fn set_var(&mut self, id: BindId, v: Value) {
        self.cached.insert(id, v.clone());
        self.user.set_var(id, v)
    }

    fn tag(&mut self, s: &ArcStr) -> ArcStr {
        match self.tags.get(s) {
            Some(s) => s.clone(),
            None => {
                self.tags.insert(s.clone());
                s.clone()
            }
        }
    }

    /// Restore the lexical environment to the snapshot `env` for the
    /// duration of `f` restoring it to it's original value
    /// afterwords. `by_id` and `lambdas` defined by the closure will
    /// be retained.
    pub fn with_restored<R, F: FnOnce(&mut Self) -> R>(
        &mut self,
        env: Env<C, E>,
        f: F,
    ) -> R {
        let snap = self.env.restore_lexical_env(env);
        let orig = mem::replace(&mut self.env, snap);
        let r = f(self);
        self.env = self.env.restore_lexical_env(orig);
        r
    }
}

/// compile the expression into a node graph in the specified context
/// and scope, return the root node or an error if compilation failed.
pub fn compile<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    scope: &ModPath,
    spec: Expr,
) -> Result<Node<C, E>> {
    let top_id = spec.id;
    let env = ctx.env.clone();
    let st = Instant::now();
    let mut node = match compiler::compile(ctx, spec, scope, top_id) {
        Ok(n) => n,
        Err(e) => {
            ctx.env = env;
            return Err(e);
        }
    };
    info!("compile time {:?}", st.elapsed());
    let st = Instant::now();
    if let Err(e) = node.typecheck(ctx) {
        ctx.env = env;
        return Err(e);
    }
    info!("typecheck time {:?}", st.elapsed());
    Ok(node)
}
