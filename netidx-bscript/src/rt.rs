/// A general purpose bscript runtime
///
/// This module implements a generic bscript runtime suitable for most
/// applications, including applications that implement custom bscript
/// builtins. The bscript interperter is run in a background task, and
/// can be interacted with via a handle. All features of the standard
/// library are supported by this runtime.
///
/// In special cases where this runtime is not suitable for your
/// application you can implement your own, see the [Ctx] and
/// [UserEvent] traits.
use crate::{
    env::Env,
    expr::{self, ExprId, ExprKind, ModPath, ModuleKind, ModuleResolver, Origin},
    node::{Node, NodeKind},
    typ::{NoRefs, Type},
    BindId, BuiltIn, Ctx, Event, ExecCtx, NoUserEvent,
};
use anyhow::{anyhow, bail, Context, Result};
use arcstr::{literal, ArcStr};
use chrono::prelude::*;
use compact_str::format_compact;
use core::fmt;
use derive_builder::Builder;
use futures::{channel::mpsc, future::join_all, FutureExt, SinkExt, StreamExt};
use fxhash::{FxBuildHasher, FxHashMap};
use indexmap::IndexMap;
use log::error;
use netidx::{
    path::Path,
    pool::Pooled,
    protocol::valarray::ValArray,
    publisher::{self, Id, PublishFlags, Publisher, Val, Value, WriteRequest},
    resolver_client::ChangeTracker,
    subscriber::{self, Dval, SubId, Subscriber, UpdatesFlags},
};
use netidx_protocols::rpc::{
    self,
    server::{ArgSpec, RpcCall},
};
use smallvec::{smallvec, SmallVec};
use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    future, mem,
    os::unix::ffi::OsStrExt,
    path::{Component, PathBuf},
    time::Duration,
};
use tokio::{
    fs, select,
    sync::{oneshot, Mutex},
    task::{self, JoinSet},
    time::{self, Instant},
};
use triomphe::Arc;

type UpdateBatch = Pooled<Vec<(SubId, subscriber::Event)>>;
type WriteBatch = Pooled<Vec<WriteRequest>>;

#[derive(Debug)]
pub struct CouldNotResolve;

impl fmt::Display for CouldNotResolve {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "could not resolve module")
    }
}

#[derive(Debug)]
struct RpcClient {
    proc: rpc::client::Proc,
    last_used: Instant,
}

#[derive(Debug)]
pub struct BSCtx {
    by_ref: FxHashMap<BindId, FxHashMap<ExprId, usize>>,
    subscribed: FxHashMap<SubId, FxHashMap<ExprId, usize>>,
    published: FxHashMap<Id, FxHashMap<ExprId, usize>>,
    var_updates: VecDeque<(BindId, Value)>,
    net_updates: VecDeque<(SubId, subscriber::Event)>,
    net_writes: VecDeque<(Id, WriteRequest)>,
    rpc_overflow: VecDeque<(BindId, RpcCall)>,
    rpc_clients: FxHashMap<Path, RpcClient>,
    published_rpcs: FxHashMap<Path, rpc::server::Proc>,
    pending_unsubscribe: VecDeque<(Instant, Dval)>,
    change_trackers: FxHashMap<BindId, Arc<Mutex<ChangeTracker>>>,
    tasks: JoinSet<(BindId, Value)>,
    batch: publisher::UpdateBatch,
    publisher: Publisher,
    subscriber: Subscriber,
    updates_tx: mpsc::Sender<UpdateBatch>,
    updates: mpsc::Receiver<UpdateBatch>,
    writes_tx: mpsc::Sender<WriteBatch>,
    writes: mpsc::Receiver<WriteBatch>,
    rpcs_tx: mpsc::Sender<(BindId, RpcCall)>,
    rpcs: mpsc::Receiver<(BindId, RpcCall)>,
}

impl BSCtx {
    fn new(publisher: Publisher, subscriber: Subscriber) -> Self {
        let (updates_tx, updates) = mpsc::channel(3);
        let (writes_tx, writes) = mpsc::channel(100);
        let (rpcs_tx, rpcs) = mpsc::channel(100);
        let batch = publisher.start_batch();
        let mut tasks = JoinSet::new();
        tasks.spawn(async { future::pending().await });
        Self {
            by_ref: HashMap::default(),
            var_updates: VecDeque::new(),
            net_updates: VecDeque::new(),
            net_writes: VecDeque::new(),
            rpc_overflow: VecDeque::new(),
            rpc_clients: HashMap::default(),
            subscribed: HashMap::default(),
            pending_unsubscribe: VecDeque::new(),
            published: HashMap::default(),
            change_trackers: HashMap::default(),
            published_rpcs: HashMap::default(),
            tasks,
            batch,
            publisher,
            subscriber,
            updates,
            updates_tx,
            writes,
            writes_tx,
            rpcs_tx,
            rpcs,
        }
    }
}

macro_rules! or_err {
    ($bindid:expr, $e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => {
                let e = ArcStr::from(format_compact!("{e:?}").as_str());
                let e = Value::Error(e);
                return ($bindid, e);
            }
        }
    };
}

macro_rules! check_changed {
    ($id:expr, $resolver:expr, $path:expr, $ct:expr) => {
        let mut ct = $ct.lock().await;
        if ct.path() != &$path {
            *ct = ChangeTracker::new($path.clone());
        }
        if !or_err!($id, $resolver.check_changed(&mut *ct).await) {
            return ($id, Value::Null);
        }
    };
}

impl Ctx for BSCtx {
    fn clear(&mut self) {
        let Self {
            by_ref,
            var_updates,
            net_updates,
            net_writes,
            rpc_clients,
            rpc_overflow,
            subscribed,
            published,
            published_rpcs,
            pending_unsubscribe,
            change_trackers,
            tasks,
            batch,
            publisher,
            subscriber: _,
            updates_tx,
            updates,
            writes_tx,
            writes,
            rpcs,
            rpcs_tx,
        } = self;
        by_ref.clear();
        var_updates.clear();
        net_updates.clear();
        net_writes.clear();
        rpc_overflow.clear();
        rpc_clients.clear();
        subscribed.clear();
        published.clear();
        published_rpcs.clear();
        pending_unsubscribe.clear();
        change_trackers.clear();
        *tasks = JoinSet::new();
        tasks.spawn(async { future::pending().await });
        *batch = publisher.start_batch();
        let (tx, rx) = mpsc::channel(3);
        *updates_tx = tx;
        *updates = rx;
        let (tx, rx) = mpsc::channel(100);
        *writes_tx = tx;
        *writes = rx;
        let (tx, rx) = mpsc::channel(100);
        *rpcs_tx = tx;
        *rpcs = rx
    }

    fn call_rpc(&mut self, name: Path, args: Vec<(ArcStr, Value)>, id: BindId) {
        let now = Instant::now();
        let proc = match self.rpc_clients.entry(name) {
            Entry::Occupied(mut e) => {
                let cl = e.get_mut();
                cl.last_used = now;
                Ok(cl.proc.clone())
            }
            Entry::Vacant(e) => {
                match rpc::client::Proc::new(&self.subscriber, e.key().clone()) {
                    Err(e) => Err(e),
                    Ok(proc) => {
                        let cl = RpcClient { last_used: now, proc: proc.clone() };
                        e.insert(cl);
                        Ok(proc)
                    }
                }
            }
        };
        self.tasks.spawn(async move {
            macro_rules! err {
                ($e:expr) => {{
                    let e = format_compact!("{:?}", $e);
                    (id, Value::Error(e.as_str().into()))
                }};
            }
            match proc {
                Err(e) => err!(e),
                Ok(proc) => match proc.call(args).await {
                    Err(e) => err!(e),
                    Ok(res) => (id, res),
                },
            }
        });
    }

    fn publish_rpc(
        &mut self,
        name: Path,
        doc: Value,
        spec: Vec<ArgSpec>,
        id: BindId,
    ) -> Result<()> {
        use rpc::server::Proc;
        let e = match self.published_rpcs.entry(name) {
            Entry::Vacant(e) => e,
            Entry::Occupied(_) => bail!("already published"),
        };
        let proc = Proc::new(
            &self.publisher,
            e.key().clone(),
            doc,
            spec,
            move |c| Some((id, c)),
            Some(self.rpcs_tx.clone()),
        )?;
        e.insert(proc);
        Ok(())
    }

    fn unpublish_rpc(&mut self, name: Path) {
        self.published_rpcs.remove(&name);
    }

    fn subscribe(&mut self, flags: UpdatesFlags, path: Path, ref_by: ExprId) -> Dval {
        let dval =
            self.subscriber.subscribe_updates(path, [(flags, self.updates_tx.clone())]);
        *self.subscribed.entry(dval.id()).or_default().entry(ref_by).or_default() += 1;
        dval
    }

    fn unsubscribe(&mut self, _path: Path, dv: Dval, ref_by: ExprId) {
        if let Some(exprs) = self.subscribed.get_mut(&dv.id()) {
            if let Some(cn) = exprs.get_mut(&ref_by) {
                *cn -= 1;
                if *cn == 0 {
                    exprs.remove(&ref_by);
                }
            }
            if exprs.is_empty() {
                self.subscribed.remove(&dv.id());
            }
        }
        self.pending_unsubscribe.push_back((Instant::now(), dv));
    }

    fn list(&mut self, id: BindId, path: Path) {
        let ct = self
            .change_trackers
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(ChangeTracker::new(path.clone()))));
        let ct = Arc::clone(ct);
        let resolver = self.subscriber.resolver();
        self.tasks.spawn(async move {
            check_changed!(id, resolver, path, ct);
            let mut paths = or_err!(id, resolver.list(path).await);
            let paths = paths.drain(..).map(|p| Value::String(p.into()));
            (id, Value::Array(ValArray::from_iter_exact(paths)))
        });
    }

    fn list_table(&mut self, id: BindId, path: Path) {
        let ct = self
            .change_trackers
            .entry(id)
            .or_insert_with(|| Arc::new(Mutex::new(ChangeTracker::new(path.clone()))));
        let ct = Arc::clone(ct);
        let resolver = self.subscriber.resolver();
        self.tasks.spawn(async move {
            check_changed!(id, resolver, path, ct);
            let mut tbl = or_err!(id, resolver.table(path).await);
            let cols = tbl.cols.drain(..).map(|(name, count)| {
                Value::Array(ValArray::from([
                    Value::String(name.into()),
                    Value::V64(count.0),
                ]))
            });
            let cols = Value::Array(ValArray::from_iter_exact(cols));
            let rows = tbl.rows.drain(..).map(|name| Value::String(name.into()));
            let rows = Value::Array(ValArray::from_iter_exact(rows));
            let tbl = Value::Array(ValArray::from([
                Value::Array(ValArray::from([Value::String(literal!("columns")), cols])),
                Value::Array(ValArray::from([Value::String(literal!("rows")), rows])),
            ]));
            (id, tbl)
        });
    }

    fn stop_list(&mut self, id: BindId) {
        self.change_trackers.remove(&id);
    }

    fn publish(&mut self, path: Path, value: Value, ref_by: ExprId) -> Result<Val> {
        let val = self.publisher.publish_with_flags_and_writes(
            PublishFlags::empty(),
            path,
            value,
            Some(self.writes_tx.clone()),
        )?;
        let id = val.id();
        *self.published.entry(id).or_default().entry(ref_by).or_default() += 1;
        Ok(val)
    }

    fn update(&mut self, val: &Val, value: Value) {
        val.update(&mut self.batch, value);
    }

    fn unpublish(&mut self, val: Val, ref_by: ExprId) {
        if let Some(refs) = self.published.get_mut(&val.id()) {
            if let Some(cn) = refs.get_mut(&ref_by) {
                *cn -= 1;
                if *cn == 0 {
                    refs.remove(&ref_by);
                }
            }
            if refs.is_empty() {
                self.published.remove(&val.id());
            }
        }
    }

    fn set_timer(&mut self, id: BindId, timeout: Duration) {
        self.tasks
            .spawn(time::sleep(timeout).map(move |()| (id, Value::DateTime(Utc::now()))));
    }

    fn ref_var(&mut self, id: BindId, ref_by: ExprId) {
        *self.by_ref.entry(id).or_default().entry(ref_by).or_default() += 1;
    }

    fn unref_var(&mut self, id: BindId, ref_by: ExprId) {
        if let Some(refs) = self.by_ref.get_mut(&id) {
            if let Some(cn) = refs.get_mut(&ref_by) {
                *cn -= 1;
                if *cn == 0 {
                    refs.remove(&ref_by);
                }
            }
            if refs.is_empty() {
                self.by_ref.remove(&id);
            }
        }
    }

    fn set_var(&mut self, id: BindId, value: Value) {
        self.var_updates.push_back((id, value.clone()));
    }
}

fn is_output(n: &Node<BSCtx, NoUserEvent>) -> bool {
    match &n.kind {
        NodeKind::Bind { .. }
        | NodeKind::Lambda(_)
        | NodeKind::Use { .. }
        | NodeKind::Connect(_, _)
        | NodeKind::Module(_)
        | NodeKind::TypeDef { .. } => false,
        _ => true,
    }
}

async fn or_never(b: bool) {
    if !b {
        future::pending().await
    }
}

async fn maybe_next<T>(go: bool, ch: &mut mpsc::Receiver<T>) -> T {
    if go {
        match ch.next().await {
            None => future::pending().await,
            Some(v) => v,
        }
    } else {
        future::pending().await
    }
}

async fn unsubscribe_ready(pending: &VecDeque<(Instant, Dval)>, now: Instant) {
    if pending.len() == 0 {
        future::pending().await
    } else {
        let (ts, _) = pending.front().unwrap();
        let one = Duration::from_secs(1);
        let elapsed = now - *ts;
        if elapsed < one {
            time::sleep(one - elapsed).await
        }
    }
}

#[derive(Clone)]
pub struct CompExp {
    pub id: ExprId,
    pub typ: Type<NoRefs>,
    pub output: bool,
}

#[derive(Clone)]
pub struct CompRes {
    pub exprs: SmallVec<[CompExp; 1]>,
    pub env: Env<BSCtx, NoUserEvent>,
}

#[derive(Clone)]
pub enum RtEvent {
    Updated(ExprId, Value),
}

enum ToRt {
    GetEnv { res: oneshot::Sender<Env<BSCtx, NoUserEvent>> },
    Delete { id: ExprId, res: oneshot::Sender<Result<Env<BSCtx, NoUserEvent>>> },
    Load { path: PathBuf, res: oneshot::Sender<Result<CompRes>> },
    Compile { text: ArcStr, res: oneshot::Sender<Result<CompRes>> },
    Subscribe { ch: mpsc::Sender<RtEvent> },
}

// setting expectations is one of the most under rated skills in
// software engineering
struct BS {
    ctx: ExecCtx<BSCtx, NoUserEvent>,
    event: Event<NoUserEvent>,
    updated: FxHashMap<ExprId, bool>,
    nodes: IndexMap<ExprId, Node<BSCtx, NoUserEvent>, FxBuildHasher>,
    subs: Vec<mpsc::Sender<RtEvent>>,
    resolvers: Vec<ModuleResolver>,
    publish_timeout: Option<Duration>,
    last_rpc_gc: Instant,
}

impl BS {
    fn new(mut rt: BSConfig) -> Self {
        let resolvers_default = || match dirs::data_dir() {
            None => vec![ModuleResolver::Files("".into())],
            Some(dd) => vec![
                ModuleResolver::Files("".into()),
                ModuleResolver::Files(dd.join("bscript")),
            ],
        };
        let resolvers = match std::env::var("BSCRIPT_MODPATH") {
            Err(_) => resolvers_default(),
            Ok(mp) => match ModuleResolver::parse_env(
                rt.subscriber.clone(),
                rt.subscribe_timeout,
                &mp,
            ) {
                Ok(r) => r,
                Err(e) => {
                    error!("failed to parse BSCRIPT_MODPATH, using default {e:?}");
                    resolvers_default()
                }
            },
        };
        let mut event = Event::new(NoUserEvent);
        let mut ctx = match rt.ctx.take() {
            Some(ctx) => ctx,
            None => ExecCtx::new(BSCtx::new(rt.publisher, rt.subscriber)),
        };
        event.init = true;
        let mut std = mem::take(&mut ctx.std);
        for n in std.iter_mut() {
            let _ = n.update(&mut ctx, &mut event);
        }
        ctx.std = std;
        event.init = false;
        Self {
            ctx,
            event,
            updated: HashMap::default(),
            nodes: IndexMap::default(),
            subs: vec![],
            resolvers,
            publish_timeout: rt.publish_timeout,
            last_rpc_gc: Instant::now(),
        }
    }

    async fn do_cycle(
        &mut self,
        updates: Option<UpdateBatch>,
        writes: Option<WriteBatch>,
        tasks: &mut Vec<(BindId, Value)>,
        rpcs: &mut Vec<(BindId, RpcCall)>,
    ) {
        macro_rules! push_event {
            ($id:expr, $v:expr, $event:ident, $refed:ident, $overflow:ident) => {
                match self.event.$event.entry($id) {
                    Entry::Vacant(e) => {
                        e.insert($v);
                        if let Some(exps) = self.ctx.user.$refed.get(&$id) {
                            for id in exps.keys() {
                                self.updated.entry(*id).or_insert(false);
                            }
                        }
                    }
                    Entry::Occupied(_) => {
                        self.ctx.user.$overflow.push_back(($id, $v));
                    }
                }
            };
        }
        for _ in 0..self.ctx.user.var_updates.len() {
            let (id, v) = self.ctx.user.var_updates.pop_front().unwrap();
            push_event!(id, v, variables, by_ref, var_updates)
        }
        for (id, v) in tasks.drain(..) {
            push_event!(id, v, variables, by_ref, var_updates)
        }
        for _ in 0..self.ctx.user.rpc_overflow.len() {
            let (id, v) = self.ctx.user.rpc_overflow.pop_front().unwrap();
            push_event!(id, v, rpc_calls, by_ref, rpc_overflow)
        }
        for (id, v) in rpcs.drain(..) {
            push_event!(id, v, rpc_calls, by_ref, rpc_overflow)
        }
        for _ in 0..self.ctx.user.net_updates.len() {
            let (id, v) = self.ctx.user.net_updates.pop_front().unwrap();
            push_event!(id, v, netidx, subscribed, net_updates)
        }
        if let Some(mut updates) = updates {
            for (id, v) in updates.drain(..) {
                push_event!(id, v, netidx, subscribed, net_updates)
            }
        }
        for _ in 0..self.ctx.user.net_writes.len() {
            let (id, v) = self.ctx.user.net_writes.pop_front().unwrap();
            push_event!(id, v, writes, published, net_writes)
        }
        if let Some(mut writes) = writes {
            for wr in writes.drain(..) {
                let id = wr.id;
                push_event!(id, wr, writes, published, net_writes)
            }
        }
        for (id, n) in self.nodes.iter_mut() {
            if let Some(init) = self.updated.get(id) {
                let mut clear: SmallVec<[BindId; 16]> = smallvec![];
                self.event.init = *init;
                if self.event.init {
                    n.refs(&mut |id| {
                        if let Some(v) = self.ctx.cached.get(&id) {
                            if let Entry::Vacant(e) = self.event.variables.entry(id) {
                                e.insert(v.clone());
                                clear.push(id);
                            }
                        }
                    });
                }
                let res = n.update(&mut self.ctx, &mut self.event);
                for id in clear {
                    self.event.variables.remove(&id);
                }
                if let Some(v) = res {
                    let mut i = 0;
                    while i < self.subs.len() {
                        if let Err(_) =
                            self.subs[i].send(RtEvent::Updated(*id, v.clone())).await
                        {
                            self.subs.remove(i);
                        }
                        i += 1;
                    }
                }
            }
        }
        self.event.clear();
        self.updated.clear();
        if self.ctx.user.batch.len() > 0 {
            let batch = mem::replace(
                &mut self.ctx.user.batch,
                self.ctx.user.publisher.start_batch(),
            );
            let timeout = self.publish_timeout;
            task::spawn(async move { batch.commit(timeout).await });
        }
    }

    fn cycle_ready(&self) -> bool {
        self.ctx.user.var_updates.len() > 0
            || self.ctx.user.net_updates.len() > 0
            || self.ctx.user.net_writes.len() > 0
            || self.ctx.user.rpc_overflow.len() > 0
    }

    async fn compile(&mut self, text: ArcStr) -> Result<CompRes> {
        let scope = ModPath::root();
        let ori = match expr::parser::parse(None, text.clone()) {
            Ok(ori) => ori,
            Err(_) => expr::parser::parse_expr(None, text)?,
        };
        let exprs = join_all(
            ori.exprs.iter().map(|e| e.resolve_modules(&scope, &self.resolvers)),
        )
        .await
        .into_iter()
        .collect::<Result<SmallVec<[_; 4]>>>()
        .context(CouldNotResolve)?;
        let ori = Origin { exprs: Arc::from_iter(exprs), ..ori };
        let nodes = ori
            .exprs
            .iter()
            .map(|e| Node::compile(&mut self.ctx, &scope, e.clone()))
            .collect::<Result<SmallVec<[_; 4]>>>()
            .with_context(|| ori.clone())?;
        let exprs = ori
            .exprs
            .iter()
            .zip(nodes.into_iter())
            .map(|(e, n)| {
                let output = is_output(&n);
                let typ = n.typ.clone();
                self.updated.insert(e.id, true);
                self.nodes.insert(e.id, n);
                CompExp { id: e.id, output, typ }
            })
            .collect::<SmallVec<[_; 1]>>();
        Ok(CompRes { exprs, env: self.ctx.env.clone() })
    }

    async fn load(&mut self, file: &PathBuf) -> Result<CompRes> {
        let scope = ModPath::root();
        let (scope, ori) = match file.extension() {
            Some(e) if e.as_bytes() == b"bs" => {
                let scope = match file.file_name() {
                    None => scope,
                    Some(name) => ModPath(scope.append(&*name.to_string_lossy())),
                };
                let s = fs::read_to_string(file).await?;
                let s = if s.starts_with("#!") {
                    if let Some(i) = s.find('\n') {
                        &s[i..]
                    } else {
                        s.as_str()
                    }
                } else {
                    s.as_str()
                };
                let s = ArcStr::from(s);
                let name = ArcStr::from(file.to_string_lossy());
                (scope, expr::parser::parse(Some(name), s)?)
            }
            Some(e) => bail!("invalid file extension {e:?}"),
            None => {
                let name = file
                    .components()
                    .map(|c| match c {
                        Component::RootDir
                        | Component::CurDir
                        | Component::ParentDir
                        | Component::Prefix(_) => bail!("invalid module name {file:?}"),
                        Component::Normal(s) => Ok(s),
                    })
                    .collect::<Result<Box<[_]>>>()?;
                if name.len() != 1 {
                    bail!("invalid module name {file:?}")
                }
                let name = String::from_utf8_lossy(name[0].as_bytes());
                let name = name
                    .parse::<ModPath>()
                    .with_context(|| "parsing module name {file:?}")?;
                let scope =
                    ModPath(Path::from_str(Path::dirname(&*name).unwrap_or(&*scope)));
                let name = Path::basename(&*name)
                    .ok_or_else(|| anyhow!("invalid module name {file:?}"))?;
                let name = ArcStr::from(name);
                let e = ExprKind::Module {
                    export: true,
                    name: name.clone(),
                    value: ModuleKind::Unresolved,
                }
                .to_expr(Default::default());
                let ori = Origin {
                    name: Some(name),
                    source: literal!(""),
                    exprs: Arc::from_iter([e]),
                };
                (scope, ori)
            }
        };
        let mut es: SmallVec<[CompExp; 1]> = smallvec![];
        for expr in &*ori.exprs {
            let expr = expr
                .resolve_modules(&scope, &self.resolvers)
                .await
                .with_context(|| ori.clone())?;
            let top_id = expr.id;
            let n = Node::compile(&mut self.ctx, &scope, expr)
                .with_context(|| ori.clone())?;
            let has_out = is_output(&n);
            let typ = n.typ.clone();
            self.nodes.insert(top_id, n);
            self.updated.insert(top_id, true);
            es.push(CompExp { id: top_id, output: has_out, typ })
        }
        Ok(CompRes { exprs: es, env: self.ctx.env.clone() })
    }

    async fn run(mut self, mut to_rt: mpsc::UnboundedReceiver<ToRt>) -> Result<()> {
        let mut tasks = vec![];
        let mut rpcs = vec![];
        let onemin = Duration::from_secs(60);
        'main: loop {
            let now = Instant::now();
            let ready = self.cycle_ready();
            let mut updates = None;
            let mut writes = None;
            let mut input = None;
            macro_rules! peek {
                (updates) => {
                    if self.ctx.user.net_updates.is_empty() {
                        if let Ok(Some(up)) = self.ctx.user.updates.try_next() {
                            updates = Some(up);
                        }
                    }
                };
                (writes) => {
                    if self.ctx.user.net_writes.is_empty() {
                        if let Ok(Some(wr)) = self.ctx.user.writes.try_next() {
                            writes = Some(wr);
                        }
                    }
                };
                (tasks) => {
                    while let Some(Ok(up)) = self.ctx.user.tasks.try_join_next() {
                        tasks.push(up);
                    }
                };
                (rpcs) => {
                    if self.ctx.user.rpc_overflow.is_empty() {
                        while let Ok(Some(up)) = self.ctx.user.rpcs.try_next() {
                            rpcs.push(up);
                        }
                    }
                };
                ($($item:tt),+) => {{
                    $(peek!($item));+
                }};
            }
            select! {
                rp = maybe_next(self.ctx.user.rpc_overflow.is_empty(), &mut self.ctx.user.rpcs) => {
                    rpcs.push(rp);
                    peek!(updates, tasks, writes, rpcs)
                }
                wr = maybe_next(self.ctx.user.net_writes.is_empty(), &mut self.ctx.user.writes) => {
                    writes = Some(wr);
                    peek!(updates, tasks, rpcs);
                },
                up = maybe_next(self.ctx.user.net_updates.is_empty(), &mut self.ctx.user.updates) => {
                    updates = Some(up);
                    peek!(writes, tasks, rpcs);
                },
                up = self.ctx.user.tasks.join_next() => {
                    if let Some(Ok(up)) = up {
                        tasks.push(up);
                    }
                    peek!(updates, writes, tasks, rpcs)
                },
                n = to_rt.next() => match n {
                    None => break 'main Ok(()),
                    Some(i) => {
                        peek!(updates, writes, tasks, rpcs);
                        input = Some(i);
                    }
                },
                _ = or_never(ready) => peek!(updates, writes, tasks, rpcs),
                () = unsubscribe_ready(&self.ctx.user.pending_unsubscribe, now) => {
                    while let Some((ts, _)) = self.ctx.user.pending_unsubscribe.front() {
                        if ts.elapsed() >= Duration::from_secs(1) {
                            self.ctx.user.pending_unsubscribe.pop_front();
                        } else {
                            break
                        }
                    }
                    continue 'main
                }
            }
            match input {
                None => (),
                Some(ToRt::GetEnv { res }) => {
                    let _ = res.send(self.ctx.env.clone());
                }
                Some(ToRt::Compile { text, res }) => {
                    let _ = res.send(self.compile(text).await);
                }
                Some(ToRt::Load { path, res }) => {
                    let _ = res.send(self.load(&path).await);
                }
                Some(ToRt::Subscribe { ch }) => {
                    self.subs.push(ch);
                }
                Some(ToRt::Delete { id, res }) => {
                    // CR estokes: Check dependencies
                    if let Some(n) = self.nodes.shift_remove(&id) {
                        n.delete(&mut self.ctx);
                    }
                    let _ = res.send(Ok(self.ctx.env.clone()));
                }
            }
            self.do_cycle(updates, writes, &mut tasks, &mut rpcs).await;
            if !self.ctx.user.rpc_clients.is_empty() {
                if now - self.last_rpc_gc >= onemin {
                    self.last_rpc_gc = now;
                    self.ctx.user.rpc_clients.retain(|_, c| now - c.last_used <= onemin);
                }
            }
        }
    }
}

/// A handle to a running BS instance. Drop the handle to shutdown the
/// associated background tasks.
pub struct BSHandle(mpsc::UnboundedSender<ToRt>);

impl BSHandle {
    async fn exec<R, F: FnOnce(oneshot::Sender<R>) -> ToRt>(&self, f: F) -> Result<R> {
        let (tx, rx) = oneshot::channel();
        self.0.unbounded_send(f(tx)).map_err(|_| anyhow!("runtime is dead"))?;
        Ok(rx.await.map_err(|_| anyhow!("runtime did not respond"))?)
    }

    /// Get a copy of the current bscript environment
    pub async fn get_env(&self) -> Result<Env<BSCtx, NoUserEvent>> {
        self.exec(|res| ToRt::GetEnv { res }).await
    }

    /// Compile and execute the specified bscript expression. It can
    /// either be a module expression or a single expression. If it
    /// generates results, they will be sent to all the channels that
    /// are subscribed.
    pub async fn compile(&self, text: ArcStr) -> Result<CompRes> {
        Ok(self.exec(|tx| ToRt::Compile { text, res: tx }).await??)
    }

    /// Load and execute the specified bscript module. The path may
    /// have one of two forms. If it is the path to a file with
    /// extension .bs then the rt will load the file directly. If it
    /// is a modpath (e.g. foo::bar::baz) then the module resolver
    /// will look for a matching module in the modpath.
    pub async fn load(&self, path: PathBuf) -> Result<CompRes> {
        Ok(self.exec(|tx| ToRt::Load { path, res: tx }).await??)
    }

    /// Delete the specified expression
    pub async fn delete(&self, id: ExprId) -> Result<Env<BSCtx, NoUserEvent>> {
        Ok(self.exec(|tx| ToRt::Delete { id, res: tx }).await??)
    }

    /// The specified channel will receive events generated by all
    /// bscript expressions being run by the runtime. To unsubscribe,
    /// just drop the receiver.
    pub fn subscribe(&self, ch: mpsc::Sender<RtEvent>) -> Result<()> {
        self.0
            .unbounded_send(ToRt::Subscribe { ch })
            .map_err(|_| anyhow!("runtime is dead"))
    }
}

#[derive(Builder)]
pub struct BSConfig {
    publisher: Publisher,
    subscriber: Subscriber,
    #[builder(setter(strip_option), default)]
    subscribe_timeout: Option<Duration>,
    #[builder(setter(strip_option), default)]
    publish_timeout: Option<Duration>,
    #[builder(setter(skip))]
    ctx: Option<ExecCtx<BSCtx, NoUserEvent>>,
}

impl BSConfig {
    /// Register a builtin bscript function
    pub fn register_builtin<T: BuiltIn<BSCtx, NoUserEvent>>(&mut self) -> Result<()> {
        let ctx = match self.ctx.as_mut() {
            Some(ctx) => ctx,
            None => {
                self.ctx = Some(ExecCtx::new(BSCtx::new(
                    self.publisher.clone(),
                    self.subscriber.clone(),
                )));
                self.ctx.as_mut().unwrap()
            }
        };
        ctx.register_builtin::<T>()
    }

    /// Start the BS runtime with the specified config, return a
    /// handle capable of interacting with it.
    pub fn start(self) -> BSHandle {
        let (tx, rx) = mpsc::unbounded();
        task::spawn(async move {
            let bs = BS::new(self);
            if let Err(e) = bs.run(rx).await {
                error!("run loop exited with error {e:?}")
            }
        });
        BSHandle(tx)
    }
}
