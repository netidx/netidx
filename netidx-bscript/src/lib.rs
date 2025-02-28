#[macro_use]
extern crate netidx_core;
#[macro_use]
extern crate combine;
#[macro_use]
extern crate serde_derive;

pub mod dbg;
pub mod env;
pub mod expr;
pub mod node;
pub mod stdfn;
pub mod typ;

use crate::{
    dbg::DbgCtx,
    env::Env,
    expr::{Arg, ExprId, ExprKind, ModPath},
    node::Node,
    typ::{FnType, NoRefs, Refs, TVar, Type},
};
use anyhow::Result;
use arcstr::ArcStr;
use fxhash::FxHashMap;
use netidx::{
    path::Path,
    pool::{Pool, Pooled},
    subscriber::{Dval, SubId, UpdatesFlags, Value},
};
use std::{
    fmt::Debug,
    sync::{self, LazyLock},
    time::Duration,
};
use triomphe::Arc;

#[cfg(test)]
mod tests;

atomic_id!(BindId);

pub const VAR_BATCH: LazyLock<Pool<Vec<(BindId, Value)>>> =
    LazyLock::new(|| Pool::new(1024, 128));
pub const NET_BATCH: LazyLock<Pool<Vec<(SubId, Value)>>> =
    LazyLock::new(|| Pool::new(1024, 1024));

#[derive(Clone, Debug)]
pub enum Event<E: Debug> {
    Init,
    Variable(BindId, Value),
    VarBatch(Pooled<Vec<(BindId, Value)>>),
    Netidx(Pooled<Vec<(SubId, Value)>>),
    User(E),
}

pub type InitFn<C, E> = sync::Arc<
    dyn for<'a, 'b, 'c> Fn(
            &'a mut ExecCtx<C, E>,
            &'b ModPath,
            &'c [Node<C, E>],
            ExprId,
        ) -> Result<Box<dyn Apply<C, E> + Send + Sync>>
        + Send
        + Sync,
>;

#[derive(Debug, Clone)]
pub struct LambdaTVars {
    pub argspec: Arc<[(Arg, Type<NoRefs>)]>,
    pub vargs: Option<Type<NoRefs>>,
    pub rtype: Type<NoRefs>,
    pub constraints: Arc<[(TVar<NoRefs>, Type<NoRefs>)]>,
}

impl LambdaTVars {
    fn setup_aliases(&self) {
        let Self { argspec, vargs, rtype, constraints } = self;
        let mut known = FxHashMap::default();
        for (_, typ) in argspec.iter() {
            typ.alias_unbound(&mut known)
        }
        if let Some(typ) = vargs {
            typ.alias_unbound(&mut known)
        }
        rtype.alias_unbound(&mut known);
        for (tv, tc) in constraints.iter() {
            Type::TVar(tv.clone()).alias_unbound(&mut known);
            tc.alias_unbound(&mut known);
        }
    }
}

pub type InitFnTyped<C, E> = sync::Arc<
    dyn for<'a, 'b> Fn(
            &'a mut ExecCtx<C, E>,
            &'b [Node<C, E>],
            ExprId,
        ) -> Result<Box<dyn ApplyTyped<C, E> + Send + Sync>>
        + Send
        + Sync,
>;

pub trait BuiltIn<C: Ctx, E: Debug + Clone> {
    const NAME: &str;
    const TYP: LazyLock<FnType<Refs>>;

    fn init(ctx: &mut ExecCtx<C, E>) -> InitFn<C, E>;

    fn typecheck(
        &mut self,
        _ctx: &mut ExecCtx<C, E>,
        _from: &mut [Node<C, E>],
    ) -> Result<()> {
        Ok(())
    }
}

pub trait Apply<C: Ctx, E: Debug + Clone> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value>;
}

pub trait ApplyTyped<C: Ctx, E: Debug + Clone>: Apply<C, E> {
    fn typecheck(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
    ) -> Result<()>;

    fn rtype(&self) -> &Type<NoRefs>;
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

pub struct ExecCtx<C: Ctx + 'static, E: Debug + Clone + 'static> {
    pub env: Env<C, E>,
    builtins: FxHashMap<&'static str, (FnType<Refs>, InitFn<C, E>)>,
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

    pub fn register_builtin<T: BuiltIn<C, E>>(&mut self) {
        let f = T::init(self);
        self.builtins.insert(T::NAME, (T::TYP.clone(), f));
    }
}
