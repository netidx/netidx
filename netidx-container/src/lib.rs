#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate netidx_protocols;

mod db;
mod rpcs;
mod stats;

use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
pub use db::{Datum, DatumKind, Db, Reply, Sendable, Txn};
use futures::{
    self,
    channel::{mpsc, oneshot},
    future::{self, FusedFuture},
    prelude::*,
    select_biased,
    stream::FusedStream,
};
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use log::{error, info};
use netidx::{
    chars::Chars,
    config::Config,
    pack::Pack,
    path::Path,
    pool::{Pool, Pooled},
    publisher::{
        BindCfg, DefaultHandle, Event as PEvent, Id, PublishFlags, Publisher,
        PublisherBuilder, UpdateBatch, Val, WriteRequest,
    },
    resolver_client::DesiredAuth,
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags, Value},
    utils::BatchItem,
};
use netidx_bscript::{
    expr::{Expr, ExprId},
    vm::{self, Apply, Ctx, ExecCtx, InitFn, Node, Register, RpcCallId, TimerId},
};
use netidx_protocols::rpc;
use parking_lot::Mutex;
use rpcs::{RpcRequest, RpcRequestKind};
use stats::Stats;
use std::{
    collections::{
        hash_map::Entry,
        BTreeMap,
        Bound::{self, *},
        HashMap, HashSet,
    },
    hash::Hash,
    mem,
    ops::{Deref, DerefMut},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use structopt::StructOpt;
use tokio::{
    task,
    time::Instant,
};
use lltimer as time;

use crate::rpcs::RpcApi;

#[derive(StructOpt, Debug)]
pub struct Params {
    #[structopt(
        short = "b",
        long = "bind",
        help = "configure the bind address e.g. local, 192.168.0.0/16, 127.0.0.1:5000"
    )]
    pub bind: Option<BindCfg>,
    #[structopt(
        long = "timeout",
        help = "require subscribers to consume values before timeout (seconds)"
    )]
    pub timeout: Option<u64>,
    #[structopt(long = "api-path", help = "the netidx path of the container api")]
    pub api_path: Option<Path>,
    #[structopt(long = "db", help = "the db file")]
    pub db: Option<String>,
    #[structopt(long = "cache-size", help = "db page cache size in bytes")]
    pub cache_size: Option<u64>,
    #[structopt(long = "sparse", help = "don't even advertise the contents of the db")]
    pub sparse: bool,
}

impl Params {
    pub fn default_db_path() -> Option<PathBuf> {
        dirs::data_dir().map(|mut p| {
            p.push("netidx");
            p.push("container");
            p.push("db");
            p
        })
    }
}

lazy_static! {
    static ref VARS: Pool<Vec<(Path, Chars, Value)>> = Pool::new(8, 2048);
    static ref REFS: Pool<Vec<(Path, Value)>> = Pool::new(8, 16384);
    static ref REFIDS: Pool<Vec<ExprId>> = Pool::new(8, 2048);
    static ref PKBUF: Pool<Vec<u8>> = Pool::new(8, 16384);
    static ref RELS: Pool<FxHashSet<Path>> = Pool::new(8, 2048);
}

macro_rules! or_reply {
    ($reply:expr, $r:expr) => {
        match $r {
            Ok(r) => r,
            Err(e) => {
                if let Some(reply) = $reply {
                    let e = Value::Error(Chars::from(format!("{}", e)));
                    reply.send(e);
                }
                return;
            }
        }
    };
}

struct Refs {
    refs: FxHashSet<Path>,
    rpcs: FxHashSet<Path>,
    subs: FxHashSet<SubId>,
    vars: FxHashSet<Chars>,
    timers: FxHashSet<TimerId>,
}

impl Refs {
    fn new() -> Self {
        Refs {
            refs: HashSet::default(),
            rpcs: HashSet::default(),
            subs: HashSet::default(),
            vars: HashSet::default(),
            timers: HashSet::default(),
        }
    }
}

struct Fifo {
    data_path: Path,
    data: Val,
    src_path: Path,
    src: Val,
    on_write_path: Path,
    on_write: Val,
    expr_id: Mutex<ExprId>,
    on_write_expr_id: Mutex<ExprId>,
}

struct PublishedVal {
    path: Path,
    val: Val,
}

#[derive(Clone)]
enum Published {
    Formula(Arc<Fifo>),
    Data(Arc<PublishedVal>),
}

impl Published {
    fn val(&self) -> &Val {
        match self {
            Published::Formula(fi) => &fi.data,
            Published::Data(p) => &p.val,
        }
    }

    fn path(&self) -> &Path {
        match self {
            Published::Formula(fi) => &fi.data_path,
            Published::Data(p) => &p.path,
        }
    }
}

#[derive(Debug, Clone)]
enum UserEv {
    OnWriteEvent(Value),
    Ref(Path, Value),
    Rel,
}

enum LcEvent {
    Refs,
    RpcCall { name: Path, args: Vec<(Chars, Value)>, id: RpcCallId },
    RpcReply { name: Path, id: RpcCallId, result: Value },
    Timer(TimerId, Duration),
}

struct Lc {
    current_path: Path,
    db: Db,
    var: FxHashMap<Chars, FxHashMap<ExprId, usize>>,
    sub: FxHashMap<SubId, FxHashMap<ExprId, usize>>,
    rpc: FxHashMap<Path, FxHashSet<ExprId>>,
    timer: FxHashMap<TimerId, FxHashMap<ExprId, usize>>,
    refs: FxHashMap<Path, FxHashMap<ExprId, usize>>,
    rels: FxHashMap<Path, FxHashSet<ExprId>>,
    forward_refs: FxHashMap<ExprId, Refs>,
    subscriber: Subscriber,
    publisher: Publisher,
    sub_updates: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
    var_updates: Pooled<Vec<(Path, Chars, Value)>>,
    ref_updates: Pooled<Vec<(Path, Value)>>,
    by_id: FxHashMap<Id, Published>,
    by_path: HashMap<Path, Published>,
    events: mpsc::UnboundedSender<LcEvent>,
}

fn remove_eid_from_map<K: Hash + Eq>(
    tbl: &mut FxHashMap<K, FxHashMap<ExprId, usize>>,
    key: K,
    expr_id: &ExprId,
) {
    if let Entry::Occupied(mut e) = tbl.entry(key) {
        let set = e.get_mut();
        set.remove(expr_id);
        if set.is_empty() {
            e.remove();
        }
    }
}

fn remove_eid_from_set<K: Hash + Eq>(
    tbl: &mut FxHashMap<K, FxHashSet<ExprId>>,
    key: K,
    expr_id: &ExprId,
) {
    if let Entry::Occupied(mut e) = tbl.entry(key) {
        let set = e.get_mut();
        set.remove(expr_id);
        if set.is_empty() {
            e.remove();
        }
    }
}

impl Lc {
    fn new(
        db: Db,
        subscriber: Subscriber,
        publisher: Publisher,
        sub_updates: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
        events: mpsc::UnboundedSender<LcEvent>,
    ) -> Self {
        Self {
            current_path: Path::from("/"),
            var: HashMap::default(),
            sub: HashMap::default(),
            rpc: HashMap::default(),
            timer: HashMap::default(),
            refs: HashMap::default(),
            rels: HashMap::default(),
            forward_refs: HashMap::default(),
            db,
            subscriber,
            publisher,
            sub_updates,
            var_updates: VARS.take(),
            ref_updates: REFS.take(),
            by_id: HashMap::default(),
            by_path: HashMap::new(),
            events,
        }
    }

    fn unref(&mut self, expr_id: ExprId) {
        if let Some(refs) = self.forward_refs.remove(&expr_id) {
            for path in refs.refs {
                remove_eid_from_map(&mut self.refs, path, &expr_id);
            }
            for path in refs.rpcs {
                remove_eid_from_set(&mut self.rpc, path, &expr_id);
            }
            for id in refs.subs {
                remove_eid_from_map(&mut self.sub, id, &expr_id);
            }
            for name in refs.vars {
                remove_eid_from_map(&mut self.var, name, &expr_id);
            }
            for id in refs.timers {
                remove_eid_from_map(&mut self.timer, id, &expr_id);
            }
        }
    }

    fn remove_rel(&mut self, path: &Path, id: ExprId) {
        if let Some(table) = Path::dirname(path).and_then(Path::dirname) {
            let remove = match self.rels.get_mut(table) {
                None => false,
                Some(rels) => {
                    rels.remove(&id);
                    rels.is_empty()
                }
            };
            if remove {
                self.rels.remove(table);
            }
        }
    }
}

impl Ctx for Lc {
    fn clear(&mut self) {}

    fn durable_subscribe(
        &mut self,
        flags: UpdatesFlags,
        path: Path,
        ref_id: ExprId,
    ) -> Dval {
        let dv = self.subscriber.subscribe(path);
        dv.updates(flags, self.sub_updates.clone());
        *self
            .sub
            .entry(dv.id())
            .or_insert_with(|| HashMap::default())
            .entry(ref_id)
            .or_insert(0) += 1;
        self.forward_refs.entry(ref_id).or_insert_with(Refs::new).subs.insert(dv.id());
        dv
    }

    fn register_fn(&mut self, _: netidx::chars::Chars, _: netidx::path::Path) {}

    fn unsubscribe(&mut self, _path: Path, dv: Dval, ref_id: ExprId) {
        if let Entry::Occupied(mut etbl) = self.sub.entry(dv.id()) {
            let tbl = etbl.get_mut();
            if let Entry::Occupied(mut ecnt) = tbl.entry(ref_id) {
                let cnt = ecnt.get_mut();
                *cnt -= 1;
                if *cnt == 0 {
                    ecnt.remove();
                    if tbl.is_empty() {
                        etbl.remove();
                    }
                    if let Some(refs) = self.forward_refs.get_mut(&ref_id) {
                        refs.subs.remove(&dv.id());
                    }
                }
            }
        }
    }

    // CR estokes: future optimization, use scope to avoid sending unecessary events
    fn ref_var(&mut self, name: Chars, _scope: Path, ref_id: ExprId) {
        *self
            .var
            .entry(name.clone())
            .or_insert_with(|| HashMap::default())
            .entry(ref_id)
            .or_insert(0) += 1;
        self.forward_refs.entry(ref_id).or_insert_with(Refs::new).vars.insert(name);
    }

    fn unref_var(&mut self, name: Chars, _scope: Path, ref_id: ExprId) {
        if let Entry::Occupied(mut etbl) = self.var.entry(name.clone()) {
            let tbl = etbl.get_mut();
            if let Entry::Occupied(mut ecnt) = tbl.entry(ref_id) {
                let cnt = ecnt.get_mut();
                *cnt -= 1;
                if *cnt == 0 {
                    ecnt.remove();
                    if tbl.is_empty() {
                        etbl.remove();
                    }
                    if let Some(refs) = self.forward_refs.get_mut(&ref_id) {
                        refs.vars.remove(&name);
                    }
                }
            }
        }
    }

    fn set_var(
        &mut self,
        variables: &mut FxHashMap<Path, FxHashMap<Chars, Value>>,
        local: bool,
        scope: Path,
        name: Chars,
        value: Value,
    ) {
        vm::store_var(variables, local, &scope, &name, value.clone());
        self.var_updates.push((scope, name, value));
    }

    fn call_rpc(
        &mut self,
        name: Path,
        args: Vec<(Chars, Value)>,
        ref_id: ExprId,
        id: RpcCallId,
    ) {
        self.rpc.entry(name.clone()).or_insert_with(|| HashSet::default()).insert(ref_id);
        self.forward_refs
            .entry(ref_id)
            .or_insert_with(Refs::new)
            .rpcs
            .insert(name.clone());
        let _: Result<_, _> =
            self.events.unbounded_send(LcEvent::RpcCall { name, args, id });
    }

    fn set_timer(&mut self, id: TimerId, timeout: Duration, ref_by: ExprId) {
        *self
            .timer
            .entry(id)
            .or_insert_with(|| HashMap::default())
            .entry(ref_by)
            .or_insert(0) += 1;
        self.forward_refs.entry(ref_by).or_insert_with(Refs::new).timers.insert(id);
        let _: Result<_, _> = self.events.unbounded_send(LcEvent::Timer(id, timeout));
    }
}

struct Ref {
    id: ExprId,
    path: Option<Chars>,
    current: Value,
}

impl Ref {
    fn get_path(
        from: &[Node<Lc, UserEv>],
        ctx: &mut ExecCtx<Lc, UserEv>,
    ) -> Option<Chars> {
        match from {
            [path] => path.current(ctx).and_then(|v| v.get_as::<Chars>()),
            _ => None,
        }
    }

    fn get_current(ctx: &ExecCtx<Lc, UserEv>, path: &Option<Chars>) -> Value {
        macro_rules! or_ref {
            ($e:expr) => {
                match $e {
                    Some(e) => e,
                    None => return Value::Error(Chars::from("#REF")),
                }
            };
        }
        let path = or_ref!(path);
        let is_fpath = path.ends_with(".formula");
        let is_wpath = path.ends_with(".on-write");
        let dbpath = if is_fpath || is_wpath {
            or_ref!(Path::basename(path))
        } else {
            path.as_ref()
        };
        match or_ref!(ctx.user.db.lookup(dbpath).ok().flatten()) {
            Datum::Deleted => Value::Error(Chars::from("#REF")),
            Datum::Data(v) => v,
            Datum::Formula(fv, wv) => {
                if is_fpath {
                    fv
                } else if is_wpath {
                    wv
                } else {
                    match or_ref!(ctx.user.by_path.get(dbpath)) {
                        Published::Data(_) => return Value::Error(Chars::from("#REF")),
                        Published::Formula(fifo) => {
                            or_ref!(ctx.user.publisher.current(&fifo.data.id()))
                        }
                    }
                }
            }
        }
    }

    fn apply_ev(&mut self, ev: &vm::Event<UserEv>) -> Option<Value> {
        match ev {
            vm::Event::User(UserEv::Ref(path, value)) => {
                if Some(path.as_ref()) == self.path.as_ref().map(|c| c.as_ref()) {
                    self.current = value.clone();
                    Some(self.current.clone())
                } else {
                    None
                }
            }
            vm::Event::User(UserEv::OnWriteEvent(_))
            | vm::Event::User(UserEv::Rel)
            | vm::Event::Netidx(_, _)
            | vm::Event::Rpc(_, _)
            | vm::Event::Timer(_)
            | vm::Event::Variable(_, _, _) => None,
        }
    }

    fn set_ref(&mut self, ctx: &mut ExecCtx<Lc, UserEv>, path: Option<Chars>) {
        if let Some(path) = self.path.take() {
            let path = Path::from(path);
            if let Entry::Occupied(mut etbl) = ctx.user.refs.entry(path.clone()) {
                let tbl = etbl.get_mut();
                if let Entry::Occupied(mut ecnt) = tbl.entry(self.id) {
                    let cnt = ecnt.get_mut();
                    *cnt -= 1;
                    if *cnt == 0 {
                        ecnt.remove();
                        if tbl.is_empty() {
                            etbl.remove();
                        }
                        if let Some(refs) = ctx.user.forward_refs.get_mut(&self.id) {
                            refs.refs.remove(&path);
                        }
                    }
                }
            }
        }
        if let Some(path) = path.clone() {
            let path = Path::from(path);
            *ctx.user
                .refs
                .entry(path.clone())
                .or_insert_with(|| HashMap::with_hasher(FxBuildHasher::default()))
                .entry(self.id)
                .or_insert(0) += 1;
            ctx.user
                .forward_refs
                .entry(self.id)
                .or_insert_with(Refs::new)
                .refs
                .insert(path.clone());
        }
        self.path = path;
    }
}

impl Register<Lc, UserEv> for Ref {
    fn register(ctx: &mut ExecCtx<Lc, UserEv>) {
        let f: InitFn<Lc, UserEv> = Arc::new(|ctx, from, _, id| {
            let path = Ref::get_path(from, ctx);
            let current = Ref::get_current(ctx, &path);
            let mut t = Box::new(Ref { id, path: None, current });
            t.set_ref(ctx, path);
            t
        });
        ctx.functions.insert("ref".into(), f);
    }
}

impl Apply<Lc, UserEv> for Ref {
    fn current(&self, _ctx: &mut ExecCtx<Lc, UserEv>) -> Option<Value> {
        Some(self.current.clone())
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<Lc, UserEv>,
        from: &mut [Node<Lc, UserEv>],
        event: &vm::Event<UserEv>,
    ) -> Option<Value> {
        match from {
            [path] => match path.update(ctx, event).map(|p| p.get_as::<Chars>()) {
                None => self.apply_ev(event),
                Some(new_path) => {
                    if new_path != self.path {
                        self.set_ref(ctx, new_path);
                        match self.apply_ev(event) {
                            v @ Some(_) => v,
                            None => {
                                self.current = Ref::get_current(ctx, &self.path);
                                Some(self.current.clone())
                            }
                        }
                    } else {
                        self.apply_ev(event)
                    }
                }
            },
            from => {
                for n in from {
                    n.update(ctx, event);
                }
                None
            }
        }
    }
}

struct Rel {
    loc: Path,
    current: Value,
}

impl Rel {
    fn eval(&self, ctx: &mut ExecCtx<Lc, UserEv>, from: &[Node<Lc, UserEv>]) -> Value {
        let (row, col, invalid) = match from {
            [] => (None, None, false),
            [v] => (None, v.current(ctx).and_then(|v| v.cast_to::<i32>().ok()), false),
            [v0, v1] => {
                let row = v0.current(ctx).and_then(|v| v.cast_to::<i32>().ok());
                let col = v1.current(ctx).and_then(|v| v.cast_to::<i32>().ok());
                (row, col, false)
            }
            _ => (None, None, true),
        };
        let invalid = match (row, col) {
            (None, None) => invalid,
            (Some(r), Some(c)) => invalid || r.abs() > 255 || c.abs() > 255,
            (Some(r), None) => invalid || r.abs() > 255,
            (None, Some(c)) => invalid || c.abs() > 255,
        };
        if invalid {
            let e = "rel(), rel([col]), rel([row], [col]): expected at most 2 args";
            Value::Error(Chars::from(e))
        } else {
            let loc = &self.loc;
            let loc = match row {
                None | Some(0) => Some(loc.clone()),
                Some(offset) => ctx.user.db.relative_row(&loc, offset).ok().flatten(),
            };
            let loc = loc.and_then(|loc| match col {
                None | Some(0) => Some(loc),
                Some(offset) => ctx.user.db.relative_column(&loc, offset).ok().flatten(),
            });
            match loc {
                None => Value::Error(Chars::from("#LOC")),
                Some(p) => Value::String(Chars::from(String::from(p.as_ref()))),
            }
        }
    }
}

impl Register<Lc, UserEv> for Rel {
    fn register(ctx: &mut ExecCtx<Lc, UserEv>) {
        let f: InitFn<Lc, UserEv> = Arc::new(|ctx, from, _, id| {
            let loc = ctx.user.current_path.clone();
            if let Some(table) = Path::dirname(&loc).and_then(Path::dirname) {
                ctx.user
                    .rels
                    .entry(Path::from(ArcStr::from(table)))
                    .or_insert_with(|| HashSet::with_hasher(FxBuildHasher::default()))
                    .insert(id);
            }
            let mut t = Rel { loc, current: Value::Error(Chars::from("#LOC")) };
            t.current = t.eval(ctx, from);
            Box::new(t)
        });
        ctx.functions.insert("rel".into(), f);
    }
}

impl Apply<Lc, UserEv> for Rel {
    fn current(&self, _ctx: &mut ExecCtx<Lc, UserEv>) -> Option<Value> {
        Some(self.current.clone())
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<Lc, UserEv>,
        from: &mut [Node<Lc, UserEv>],
        event: &vm::Event<UserEv>,
    ) -> Option<Value> {
        for s in from.iter_mut() {
            s.update(ctx, event);
        }
        let v = self.eval(ctx, from);
        if v != self.current {
            self.current = v.clone();
            Some(v)
        } else {
            None
        }
    }
}

pub(crate) struct OnWriteEvent {
    cur: Option<Value>,
    invalid: bool,
}

impl Register<Lc, UserEv> for OnWriteEvent {
    fn register(ctx: &mut ExecCtx<Lc, UserEv>) {
        let f: InitFn<Lc, UserEv> = Arc::new(|_, from, _, _| {
            Box::new(OnWriteEvent { cur: None, invalid: from.len() > 0 })
        });
        ctx.functions.insert("event".into(), f);
    }
}

impl Apply<Lc, UserEv> for OnWriteEvent {
    fn current(&self, _ctx: &mut ExecCtx<Lc, UserEv>) -> Option<Value> {
        if self.invalid {
            OnWriteEvent::err()
        } else {
            self.cur.as_ref().cloned()
        }
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<Lc, UserEv>,
        from: &mut [Node<Lc, UserEv>],
        event: &vm::Event<UserEv>,
    ) -> Option<Value> {
        self.invalid = from.len() > 0;
        match event {
            vm::Event::Variable(_, _, _)
            | vm::Event::Netidx(_, _)
            | vm::Event::Rpc(_, _)
            | vm::Event::Timer(_)
            | vm::Event::User(UserEv::Ref(_, _))
            | vm::Event::User(UserEv::Rel) => None,
            vm::Event::User(UserEv::OnWriteEvent(value)) => {
                self.cur = Some(value.clone());
                self.current(ctx)
            }
        }
    }
}

impl OnWriteEvent {
    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from("event(): expected 0 arguments")))
    }
}

fn to_chars(value: Value) -> Chars {
    value.cast_to::<Chars>().ok().unwrap_or_else(|| Chars::from("null"))
}

#[must_use = "streams do nothing unless polled"]
struct Roots(BTreeMap<Path, DefaultHandle>);

impl Deref for Roots {
    type Target = BTreeMap<Path, DefaultHandle>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Roots {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> Stream for Roots {
    type Item = (Path, oneshot::Sender<()>);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        for dh in self.values_mut() {
            match Pin::new(&mut *dh).poll_next(cx) {
                Poll::Pending => (),
                r @ Poll::Ready(_) => return r,
            }
        }
        Poll::Pending
    }
}

impl<'a> FusedStream for Roots {
    fn is_terminated(&self) -> bool {
        self.values().all(|ch| ch.is_terminated())
    }
}

enum Compiled {
    Formula { node: Node<Lc, UserEv>, data_id: Id },
    OnWrite(Node<Lc, UserEv>),
}

struct OptTimer {
    timer: Option<Pin<Box<time::Sleep>>>,
}

impl Future for OptTimer {
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.timer.as_mut() {
            None => std::task::Poll::Pending,
            Some(timer) => timer.as_mut().poll(cx),
        }
    }
}

impl FusedFuture for OptTimer {
    fn is_terminated(&self) -> bool {
        match self.timer.as_ref() {
            None => true,
            Some(timer) => timer.is_elapsed(),
        }
    }
}

struct ContainerInner {
    params: Params,
    api_path: Option<Path>,
    stats: Option<Stats>,
    locked: BTreeMap<Path, bool>,
    ctx: ExecCtx<Lc, UserEv>,
    compiled: FxHashMap<ExprId, Compiled>,
    sub_updates: mpsc::Receiver<Pooled<Vec<(SubId, Event)>>>,
    write_updates_tx: mpsc::Sender<Pooled<Vec<WriteRequest>>>,
    write_updates_rx: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
    publish_events: mpsc::UnboundedReceiver<PEvent>,
    roots: Roots,
    db_updates: mpsc::UnboundedReceiver<db::Update>,
    api: Option<rpcs::RpcApi>,
    bscript_event: mpsc::UnboundedReceiver<LcEvent>,
    rpcs: FxHashMap<
        Path,
        (Instant, mpsc::UnboundedSender<(Vec<(Chars, Value)>, RpcCallId)>),
    >,
    timer: OptTimer,
    timers: BTreeMap<Instant, TimerId>,
}

impl ContainerInner {
    async fn new(cfg: Config, auth: DesiredAuth, params: Params) -> Result<Self> {
        let (publish_events_tx, publish_events) = mpsc::unbounded();
        let publisher = PublisherBuilder::new(cfg.clone())
            .desired_auth(auth.clone())
            .bind_cfg(params.bind)
            .build()
            .await?;
        publisher.events(publish_events_tx);
        let (db, db_updates) =
            Db::new(&params, publisher.clone(), params.api_path.clone())?;
        let subscriber = Subscriber::new(cfg, auth)?;
        let (sub_updates_tx, sub_updates) = mpsc::channel(3);
        let (write_updates_tx, write_updates_rx) = mpsc::channel(3);
        let (bs_tx, bs_rx) = mpsc::unbounded();
        let (api_path, api) = match params.api_path.as_ref() {
            None => (None, None),
            Some(api_path) => {
                let api_path = api_path.append("rpcs");
                let api = rpcs::RpcApi::new(&publisher, &api_path)?;
                (Some(api_path), Some(api))
            }
        };
        let stats = match params.api_path.as_ref() {
            None => None,
            Some(p) => Some(Stats::new(publisher.clone(), p.clone())),
        };
        let mut ctx =
            ExecCtx::new(Lc::new(db, subscriber, publisher, sub_updates_tx, bs_tx));
        Ref::register(&mut ctx);
        Rel::register(&mut ctx);
        OnWriteEvent::register(&mut ctx);
        Ok(Self {
            params,
            api_path,
            stats,
            locked: BTreeMap::new(),
            roots: Roots(BTreeMap::new()),
            ctx,
            sub_updates,
            write_updates_tx,
            write_updates_rx,
            publish_events,
            db_updates,
            api,
            bscript_event: bs_rx,
            rpcs: HashMap::with_hasher(FxBuildHasher::default()),
            compiled: HashMap::with_hasher(FxBuildHasher::default()),
            timer: OptTimer { timer: None },
            timers: BTreeMap::new(),
        })
    }

    fn get_root(&self, path: &Path) -> Option<(&Path, &DefaultHandle)> {
        match self
            .roots
            .range::<str, (Bound<&str>, Bound<&str>)>((
                Unbounded,
                Included(path.as_ref()),
            ))
            .next_back()
        {
            None => None,
            Some((prev, def)) => {
                if path.starts_with(prev.as_ref()) {
                    Some((prev, def))
                } else {
                    None
                }
            }
        }
    }

    fn check_path(&self, path: Path) -> Result<Path> {
        if self.get_root(&path).is_some() {
            Ok(path)
        } else {
            bail!("non root path")
        }
    }

    fn publish_formula(
        &mut self,
        path: Path,
        batch: &mut UpdateBatch,
        formula_txt: Value,
        on_write_txt: Value,
    ) -> Result<()> {
        let expr = formula_txt
            .clone()
            .cast_to::<Chars>()
            .map_err(|e| anyhow!(e))
            .and_then(|c| c.parse::<Expr>());
        let on_write_expr = on_write_txt
            .clone()
            .cast_to::<Chars>()
            .map_err(|e| anyhow!(e))
            .and_then(|c| c.parse::<Expr>());
        let expr_id = expr.as_ref().map(|e| e.id).unwrap_or_else(|_| ExprId::new());
        let on_write_expr_id =
            on_write_expr.as_ref().map(|e| e.id).unwrap_or_else(|_| ExprId::new());
        self.ctx.user.current_path = path.clone();
        let scope = Path::dirname(&path)
            .map(|s| Path::from(String::from(s)))
            .unwrap_or_else(Path::root);
        let formula_node = expr.map(|e| Node::compile(&mut self.ctx, scope.clone(), e));
        let on_write_node =
            on_write_expr.map(|e| Node::compile(&mut self.ctx, scope.clone(), e));
        let value = formula_node
            .as_ref()
            .ok()
            .and_then(|n| n.current(&mut self.ctx))
            .unwrap_or(Value::Null);
        let src_path = path.append(".formula");
        let on_write_path = path.append(".on-write");
        let data = self.ctx.user.publisher.publish(path.clone(), value.clone())?;
        let src =
            self.ctx.user.publisher.publish(src_path.clone(), formula_txt.clone())?;
        let on_write = self
            .ctx
            .user
            .publisher
            .publish(on_write_path.clone(), on_write_txt.clone())?;
        let data_id = data.id();
        let src_id = src.id();
        let on_write_id = on_write.id();
        self.ctx.user.publisher.writes(data.id(), self.write_updates_tx.clone());
        self.ctx.user.publisher.writes(src.id(), self.write_updates_tx.clone());
        self.ctx.user.publisher.writes(on_write.id(), self.write_updates_tx.clone());
        let fifo = Arc::new(Fifo {
            data_path: path.clone(),
            data,
            src_path: src_path.clone(),
            src,
            on_write_path: on_write_path.clone(),
            on_write,
            expr_id: Mutex::new(expr_id),
            on_write_expr_id: Mutex::new(on_write_expr_id),
        });
        let published = Published::Formula(fifo.clone());
        self.ctx.user.by_id.insert(data_id, published.clone());
        self.ctx.user.by_path.insert(path.clone(), published.clone());
        self.ctx.user.by_id.insert(src_id, published.clone());
        self.ctx.user.by_path.insert(src_path.clone(), published.clone());
        self.ctx.user.by_id.insert(on_write_id, published.clone());
        self.ctx.user.by_path.insert(on_write_path.clone(), published);
        let formula_node = formula_node?;
        let on_write_node = on_write_node?;
        let c = Compiled::Formula { node: formula_node, data_id };
        self.compiled.insert(expr_id, c);
        self.compiled.insert(on_write_expr_id, Compiled::OnWrite(on_write_node));
        fifo.data.update(batch, value.clone());
        self.ctx.user.ref_updates.push((path, value));
        self.ctx.user.ref_updates.push((src_path, Value::from(formula_txt)));
        self.ctx.user.ref_updates.push((on_write_path, Value::from(on_write_txt)));
        self.update_refs(batch);
        Ok(())
    }

    fn publish_data(&mut self, path: Path, value: Value) -> Result<()> {
        self.advertise_path(path.clone());
        let val = self.ctx.user.publisher.publish_with_flags(
            PublishFlags::DESTROY_ON_IDLE,
            path.clone(),
            value.clone(),
        )?;
        let id = val.id();
        self.ctx.user.publisher.writes(val.id(), self.write_updates_tx.clone());
        let published =
            Published::Data(Arc::new(PublishedVal { path: path.clone(), val }));
        self.ctx.user.by_id.insert(id, published.clone());
        self.ctx.user.by_path.insert(path.clone(), published);
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        let mut batch = self.ctx.user.publisher.start_batch();
        for res in self.ctx.user.db.roots() {
            let path = res?;
            let def = self.ctx.user.publisher.publish_default(path.clone())?;
            self.roots.insert(path, def);
        }
        if let Some(stats) = &mut self.stats {
            let _ = stats.set_roots(&mut batch, &self.roots);
        }
        for res in self.ctx.user.db.locked() {
            let (path, locked) = res?;
            let path = self.check_path(path)?;
            self.locked.insert(path, locked);
        }
        if let Some(stats) = &mut self.stats {
            let _ = stats.set_locked(&mut batch, &self.locked);
        }
        for res in self.ctx.user.db.iter() {
            let (path, kind, raw) = res?;
            match kind {
                DatumKind::Data => {
                    let path = self.check_path(path)?;
                    self.advertise_path(path);
                }
                DatumKind::Formula => match Datum::decode(&mut &*raw)? {
                    Datum::Formula(fv, wv) => {
                        let path = self.check_path(path)?;
                        let _: Result<()> =
                            self.publish_formula(path, &mut batch, fv, wv);
                    }
                    Datum::Deleted | Datum::Data(_) => unreachable!(),
                },
                DatumKind::Deleted | DatumKind::Invalid => (),
            }
        }
        Ok(batch.commit(self.params.timeout.map(Duration::from_secs)).await)
    }

    fn update_expr_ids(
        &mut self,
        batch: &mut UpdateBatch,
        refs: &mut Pooled<Vec<ExprId>>,
        event: &vm::Event<UserEv>,
    ) {
        for expr_id in refs.drain(..) {
            match self.compiled.get_mut(&expr_id) {
                None => (),
                Some(Compiled::OnWrite(node)) => {
                    node.update(&mut self.ctx, &event);
                }
                Some(Compiled::Formula { node, data_id }) => {
                    if let Some(value) = node.update(&mut self.ctx, &event) {
                        if let Some(val) = self.ctx.user.by_id.get(data_id) {
                            val.val().update(batch, value.clone());
                            self.ctx.user.ref_updates.push((val.path().clone(), value));
                        }
                    }
                }
            }
        }
    }

    fn update_refs(&mut self, batch: &mut UpdateBatch) {
        use mem::replace;
        let mut refs = REFIDS.take();
        let mut n = 0;
        while n < 10
            && (!self.ctx.user.ref_updates.is_empty()
                || !self.ctx.user.var_updates.is_empty())
        {
            // update ref() formulas
            let r = REFS.take();
            for (path, value) in replace(&mut self.ctx.user.ref_updates, r).drain(..) {
                if let Some(expr_ids) = self.ctx.user.refs.get(&path) {
                    refs.extend(expr_ids.keys().copied());
                }
                self.update_expr_ids(
                    batch,
                    &mut refs,
                    &vm::Event::User(UserEv::Ref(path, value)),
                );
            }
            // update variable references
            let v = VARS.take();
            for (scope, name, value) in
                replace(&mut self.ctx.user.var_updates, v).drain(..)
            {
                if let Some(expr_ids) = self.ctx.user.var.get(&name) {
                    refs.extend(expr_ids.keys().copied());
                }
                self.update_expr_ids(
                    batch,
                    &mut refs,
                    &vm::Event::Variable(scope, name, value),
                );
            }
            n += 1;
        }
        if !self.ctx.user.ref_updates.is_empty() || !self.ctx.user.var_updates.is_empty()
        {
            let _: Result<_, _> = self.ctx.user.events.unbounded_send(LcEvent::Refs);
        }
    }

    fn update_rels(
        &mut self,
        mut rels: Pooled<FxHashSet<Path>>,
        batch: &mut UpdateBatch,
    ) {
        let mut refs = REFIDS.take();
        for table in rels.drain() {
            if let Some(rels) = self.ctx.user.rels.get(&table) {
                refs.extend(rels.iter().copied());
            }
        }
        self.update_expr_ids(batch, &mut refs, &vm::Event::User(UserEv::Rel));
    }

    fn process_subscriptions(
        &mut self,
        batch: &mut UpdateBatch,
        mut updates: Pooled<Vec<(SubId, Event)>>,
    ) {
        let mut refs = REFIDS.take();
        for (id, event) in updates.drain(..) {
            if let Event::Update(value) = event {
                if let Some(expr_ids) = self.ctx.user.sub.get(&id) {
                    refs.extend(expr_ids.keys().copied());
                }
                self.update_expr_ids(batch, &mut refs, &vm::Event::Netidx(id, value));
            }
        }
        self.update_refs(batch)
    }

    fn change_formula(
        &mut self,
        batch: &mut UpdateBatch,
        fifo: &Arc<Fifo>,
        value: Chars,
    ) -> Result<()> {
        fifo.src.update(batch, Value::String(value.clone()));
        let mut expr_id = fifo.expr_id.lock();
        self.ctx.user.unref(*expr_id);
        self.ctx.user.remove_rel(&fifo.data_path, *expr_id);
        self.compiled.remove(&expr_id);
        let dv = match value.parse::<Expr>() {
            Ok(expr) => {
                *expr_id = expr.id;
                self.ctx.user.current_path = fifo.data_path.clone();
                let scope = Path::dirname(&fifo.data_path)
                    .map(|s| Path::from(String::from(s)))
                    .unwrap_or_else(Path::root);
                let node = Node::compile(&mut self.ctx, scope, expr);
                let dv = node.current(&mut self.ctx).unwrap_or(Value::Null);
                let c = Compiled::Formula { node, data_id: fifo.data.id() };
                self.compiled.insert(*expr_id, c);
                dv
            }
            Err(e) => {
                let e = Chars::from(format!("{}", e));
                Value::Error(e)
            }
        };
        fifo.data.update(batch, dv.clone());
        self.ctx.user.ref_updates.push((fifo.data_path.clone(), dv));
        let v = Value::String(value.clone());
        self.ctx.user.ref_updates.push((fifo.src_path.clone(), v));
        Ok(self.update_refs(batch))
    }

    fn change_on_write(
        &mut self,
        batch: &mut UpdateBatch,
        fifo: &Arc<Fifo>,
        value: Chars,
    ) -> Result<()> {
        fifo.on_write.update(batch, Value::String(value.clone()));
        let mut expr_id = fifo.on_write_expr_id.lock();
        self.ctx.user.unref(*expr_id);
        self.ctx.user.remove_rel(&fifo.data_path, *expr_id);
        self.compiled.remove(&expr_id);
        match value.parse::<Expr>() {
            Ok(expr) => {
                *expr_id = expr.id;
                self.ctx.user.current_path = fifo.data_path.clone();
                let scope = Path::dirname(&fifo.data_path)
                    .map(|s| Path::from(String::from(s)))
                    .unwrap_or_else(Path::root);
                let node = Node::compile(&mut self.ctx, scope, expr);
                self.compiled.insert(*expr_id, Compiled::OnWrite(node));
            }
            Err(_) => (), // CR estokes: log and report to user
        }
        let v = Value::String(value.clone());
        self.ctx.user.ref_updates.push((fifo.on_write_path.clone(), v));
        Ok(self.update_refs(batch))
    }

    fn process_writes(
        &mut self,
        batch: &mut UpdateBatch,
        txn: &mut Txn,
        mut writes: Pooled<Vec<WriteRequest>>,
    ) {
        let mut refs = REFS.take();
        // CR estokes: log this
        for req in writes.drain(..) {
            let reply = req.send_result.map(Sendable::Write);
            refs.clear();
            match self.ctx.user.by_id.get(&req.id) {
                None => (), // CR estokes: log
                Some(Published::Data(p)) => {
                    txn.set_data(true, p.path.clone(), req.value, reply);
                }
                Some(Published::Formula(fifo)) => {
                    let fifo = fifo.clone();
                    if fifo.src.id() == req.id {
                        txn.set_formula(fifo.data_path.clone(), req.value, reply);
                    } else if fifo.on_write.id() == req.id {
                        txn.set_on_write(fifo.data_path.clone(), req.value, reply);
                    } else if fifo.data.id() == req.id {
                        if let Some(Compiled::OnWrite(node)) =
                            self.compiled.get_mut(&fifo.on_write_expr_id.lock())
                        {
                            let ev = vm::Event::User(UserEv::OnWriteEvent(req.value));
                            node.update(&mut self.ctx, &ev);
                            self.update_refs(batch);
                        }
                    }
                }
            }
        }
    }

    fn is_locked_gen(&self, path: &Path, parent_only: bool) -> bool {
        let mut iter = self.locked.range::<str, (Bound<&str>, Bound<&str>)>((
            Bound::Unbounded,
            if parent_only {
                Bound::Excluded(path.as_ref())
            } else {
                Bound::Included(path.as_ref())
            },
        ));
        loop {
            match iter.next_back() {
                None => break false,
                Some((p, locked)) if Path::is_parent(p, &path) => break *locked,
                Some(_) => (),
            }
        }
    }

    fn is_locked(&self, path: &Path) -> bool {
        self.is_locked_gen(path, false)
    }

    fn process_publish_request(&mut self, path: Path, reply: oneshot::Sender<()>) {
        match self.check_path(path) {
            Err(_) => {
                // this should not be possible, but in case of a bug, just do nothing
                let _: Result<_, _> = reply.send(());
            }
            Ok(path) => {
                let name = Path::basename(&path);
                if name != Some(".formula") && name != Some(".on-write") {
                    match self.ctx.user.db.lookup(path.as_ref()) {
                        Ok(Some(Datum::Data(v))) => {
                            let _: Result<()> = self.publish_data(path, v);
                        }
                        Ok(Some(Datum::Formula(_, _))) => unreachable!(),
                        Err(_) | Ok(Some(Datum::Deleted)) | Ok(None) => {
                            let locked = self.is_locked(&path);
                            let api = self
                                .api_path
                                .as_ref()
                                .map(|p| Path::is_parent(p, &path))
                                .unwrap_or(false);
                            if !locked && !api {
                                let _: Result<()> = self.publish_data(path, Value::Null);
                            };
                        }
                    }
                }
                let _: Result<_, _> = reply.send(());
            }
        }
    }

    fn process_publish_event(&mut self, e: PEvent) {
        match e {
            PEvent::Subscribe(_, _) | PEvent::Unsubscribe(_, _) => (),
            PEvent::Destroyed(id) => {
                match self.ctx.user.by_id.remove(&id) {
                    None => (),
                    Some(Published::Data(p)) => {
                        self.ctx.user.by_path.remove(&p.path);
                    }
                    Some(Published::Formula(_)) => {
                        // formulas aren't published with destroy on
                        // idle, and should be cleaned up before they
                        // are dropped.
                        // CR estokes: log this as an error
                        ()
                    }
                }
            }
        }
    }

    fn get_rpc_proc(
        &mut self,
        name: &Path,
    ) -> mpsc::UnboundedSender<(Vec<(Chars, Value)>, RpcCallId)> {
        async fn rpc_task(
            reply: mpsc::UnboundedSender<LcEvent>,
            subscriber: Subscriber,
            name: Path,
            mut rx: mpsc::UnboundedReceiver<(Vec<(Chars, Value)>, RpcCallId)>,
        ) -> Result<()> {
            let proc = rpc::client::Proc::new(&subscriber, name.clone())?;
            while let Some((args, id)) = rx.next().await {
                let name = name.clone();
                let result = proc.call(args).await?;
                reply.unbounded_send(LcEvent::RpcReply { name, id, result })?
            }
            Ok(())
        }
        match self.rpcs.get_mut(name) {
            Some((ref mut last, ref proc)) => {
                *last = Instant::now();
                proc.clone()
            }
            None => {
                let (tx, rx) = mpsc::unbounded();
                task::spawn({
                    let to_gui = self.ctx.user.events.clone();
                    let sub = self.ctx.user.subscriber.clone();
                    let name = name.clone();
                    async move {
                        let _: Result<_, _> =
                            rpc_task(to_gui.clone(), sub, name.clone(), rx).await;
                    }
                });
                self.rpcs.insert(name.clone(), (Instant::now(), tx.clone()));
                tx
            }
        }
    }

    fn gc_rpcs(&mut self) {
        static MAX_RPC_AGE: Duration = Duration::from_secs(120);
        let now = Instant::now();
        self.rpcs.retain(|_, (last, _)| now - *last < MAX_RPC_AGE);
    }

    fn reschedule_timer(&mut self) {
        match self.timers.iter().next() {
            None => {
                self.timer.timer = None;
            }
            Some((deadline, _)) => match &mut self.timer.timer {
                None => {
                    self.timer.timer = Some(Box::pin(time::sleep_until(*deadline)));
                }
                Some(timer) => {
                    timer.as_mut().reset(*deadline);
                }
            },
        }
    }

    fn set_timer(&mut self, id: TimerId, duration: Duration) {
        let deadline = Instant::now() + duration;
        self.timers.insert(deadline, id);
        self.reschedule_timer()
    }

    fn process_timer_tick(&mut self, batch: &mut UpdateBatch) {
        if let Some((k, _)) = self.timers.iter().next() {
            let k = *k;
            let id = self.timers.remove(&k).unwrap();
            let mut refs = REFIDS.take();
            if let Some(expr_ids) = self.ctx.user.timer.get(&id) {
                refs.extend(expr_ids.keys().copied());
            }
            self.update_expr_ids(batch, &mut refs, &vm::Event::Timer(id));
            self.update_refs(batch);
        }
        self.reschedule_timer()
    }

    fn process_bscript_event(&mut self, batch: &mut UpdateBatch, event: LcEvent) {
        match event {
            LcEvent::Refs => self.update_refs(batch),
            LcEvent::RpcReply { name, id, result } => {
                let mut refs = REFIDS.take();
                if let Some(expr_ids) = self.ctx.user.rpc.get(&name) {
                    refs.extend(expr_ids);
                }
                self.update_expr_ids(batch, &mut refs, &vm::Event::Rpc(id, result));
                self.update_refs(batch);
            }
            LcEvent::RpcCall { name, mut args, id } => {
                for _ in 1..3 {
                    let proc = self.get_rpc_proc(&name);
                    match proc.unbounded_send((mem::replace(&mut args, vec![]), id)) {
                        Ok(()) => return (),
                        Err(e) => {
                            self.rpcs.remove(&name);
                            args = e.into_inner().0;
                        }
                    }
                }
                let result = Value::Error(Chars::from("failed to call rpc"));
                let _: Result<_, _> = self
                    .ctx
                    .user
                    .events
                    .unbounded_send(LcEvent::RpcReply { id, name, result });
            }
            LcEvent::Timer(id, duration) => {
                self.set_timer(id, duration);
            }
        }
    }

    fn delete_path(&mut self, txn: &mut Txn, path: Path, reply: Reply) {
        let path = or_reply!(reply, self.check_path(path));
        let bn = Path::basename(&path);
        if bn == Some(".formula") || bn == Some(".on-write") {
            if let Some(path) = Path::dirname(&path) {
                txn.remove(Path::from(ArcStr::from(path)), reply);
            }
        } else {
            txn.remove(path, reply);
        }
    }

    fn delete_subtree(&mut self, txn: &mut Txn, path: Path, reply: Reply) {
        let path = or_reply!(reply, self.check_path(path));
        txn.remove_subtree(path, reply);
    }

    fn lock_subtree(&mut self, txn: &mut Txn, path: Path, reply: Reply) {
        let path = or_reply!(reply, self.check_path(path));
        txn.set_locked(path, reply);
    }

    fn unlock_subtree(&mut self, txn: &mut Txn, path: Path, reply: Reply) {
        let path = or_reply!(reply, self.check_path(path));
        txn.set_unlocked(path, reply);
    }

    fn set_data(&mut self, txn: &mut Txn, path: Path, value: Value, reply: Reply) {
        let path = or_reply!(reply, self.check_path(path));
        txn.set_data(true, path, value, reply);
    }

    fn set_formula(
        &mut self,
        txn: &mut Txn,
        path: Path,
        formula: Option<Chars>,
        on_write: Option<Chars>,
        reply: Reply,
    ) {
        let path = or_reply!(reply, self.check_path(path));
        if let Some(formula) = formula {
            txn.set_formula(path.clone(), Value::from(formula), reply);
        }
        if let Some(on_write) = on_write {
            txn.set_on_write(path, Value::from(on_write), None);
        }
    }

    fn create_sheet(
        &self,
        txn: &mut Txn,
        path: Path,
        rows: usize,
        columns: usize,
        max_rows: usize,
        max_columns: usize,
        lock: bool,
        reply: Reply,
    ) {
        let path = or_reply!(reply, self.check_path(path));
        if rows > max_rows || columns > max_columns {
            let m = "rows <= max_rows && columns <= max_columns";
            let e = Value::Error(Chars::from(m));
            if let Some(reply) = reply {
                reply.send(e);
            }
        } else {
            txn.create_sheet(path, rows, columns, max_rows, max_columns, lock, reply);
        }
    }

    fn create_table(
        &self,
        txn: &mut Txn,
        path: Path,
        rows: Vec<Chars>,
        columns: Vec<Chars>,
        lock: bool,
        reply: Reply,
    ) {
        let path = or_reply!(reply, self.check_path(path));
        txn.create_table(path, rows, columns, lock, reply);
    }

    fn process_rpc_requests(&mut self, txn: &mut Txn, reqs: &mut Vec<RpcRequest>) {
        let mut process_non_packed = |reply: Sendable, req: RpcRequestKind| match req {
            RpcRequestKind::Delete(path) => self.delete_path(txn, path, Some(reply)),
            RpcRequestKind::DeleteSubtree(path) => {
                self.delete_subtree(txn, path, Some(reply))
            }
            RpcRequestKind::LockSubtree(path) => {
                self.lock_subtree(txn, path, Some(reply))
            }
            RpcRequestKind::UnlockSubtree(path) => {
                self.unlock_subtree(txn, path, Some(reply))
            }
            RpcRequestKind::SetData { path, value } => {
                self.set_data(txn, path, value, Some(reply))
            }
            RpcRequestKind::SetFormula { path, formula, on_write } => {
                self.set_formula(txn, path, formula, on_write, Some(reply))
            }
            RpcRequestKind::CreateSheet {
                path,
                rows,
                columns,
                max_rows,
                max_columns,
                lock,
            } => self.create_sheet(
                txn,
                path,
                rows,
                columns,
                max_rows,
                max_columns,
                lock,
                Some(reply),
            ),
            RpcRequestKind::AddSheetRows(path, rows) => {
                txn.add_sheet_rows(path, rows, Some(reply));
            }
            RpcRequestKind::AddSheetCols(path, cols) => {
                txn.add_sheet_columns(path, cols, Some(reply));
            }
            RpcRequestKind::DelSheetRows(path, rows) => {
                txn.del_sheet_rows(path, rows, Some(reply));
            }
            RpcRequestKind::DelSheetCols(path, cols) => {
                txn.del_sheet_columns(path, cols, Some(reply));
            }
            RpcRequestKind::CreateTable { path, rows, columns, lock } => {
                self.create_table(txn, path, rows, columns, lock, Some(reply))
            }
            RpcRequestKind::AddTableRows(path, rows) => {
                txn.add_table_rows(path, rows, Some(reply));
            }
            RpcRequestKind::AddTableCols(path, cols) => {
                txn.add_table_columns(path, cols, Some(reply));
            }
            RpcRequestKind::DelTableRows(path, rows) => {
                txn.del_table_rows(path, rows, Some(reply));
            }
            RpcRequestKind::DelTableCols(path, cols) => {
                txn.del_table_columns(path, cols, Some(reply));
            }
            RpcRequestKind::AddRoot(path) => {
                txn.add_root(path, Some(reply));
            }
            RpcRequestKind::DelRoot(path) => {
                txn.del_root(path, Some(reply));
            }
            RpcRequestKind::Packed(_) => unreachable!(),
        };
        for mut req in reqs.drain(..) {
            match req.kind {
                RpcRequestKind::Packed(reqs) => {
                    let res = Arc::new(Mutex::new(Value::Null));
                    for req in reqs {
                        let reply = Sendable::Packed(res.clone());
                        process_non_packed(reply, req)
                    }
                    req.reply.send(mem::replace(&mut *res.lock(), Value::Null));
                }
                k => {
                    let reply = Sendable::Rpc(req.reply);
                    process_non_packed(reply, k)
                }
            }
        }
    }

    fn remove_deleted_published(&mut self, batch: &mut UpdateBatch, path: &Path) {
        let ref_err = Value::Error(Chars::from("#REF"));
        self.remove_advertisement(&path);
        match self.ctx.user.by_path.remove(path) {
            None => (),
            Some(Published::Data(p)) => {
                self.ctx.user.by_id.remove(&p.val.id());
            }
            Some(Published::Formula(fifo)) => {
                let expr_id = fifo.expr_id.lock();
                let on_write_expr_id = fifo.on_write_expr_id.lock();
                self.ctx.user.unref(*expr_id);
                self.ctx.user.unref(*on_write_expr_id);
                self.compiled.remove(&expr_id);
                self.compiled.remove(&on_write_expr_id);
                let fpath = path.append(".formula");
                let opath = path.append(".on-write");
                self.ctx.user.remove_rel(path, *expr_id);
                self.ctx.user.remove_rel(path, *on_write_expr_id);
                self.ctx.user.by_id.remove(&fifo.data.id());
                self.ctx.user.by_path.remove(&fpath);
                self.ctx.user.by_id.remove(&fifo.src.id());
                self.ctx.user.by_path.remove(&opath);
                self.ctx.user.by_id.remove(&fifo.on_write.id());
                self.ctx.user.ref_updates.push((fpath, ref_err.clone()));
                self.ctx.user.ref_updates.push((opath, ref_err.clone()));
            }
        }
        self.ctx.user.ref_updates.push((path.clone(), ref_err));
        self.update_refs(batch)
    }

    fn remove_advertisement(&self, path: &Path) {
        if let Some((_, def)) = self.get_root(&path) {
            def.remove_advertisement(&path);
        }
    }

    fn advertise_path(&self, path: Path) {
        if !self.params.sparse {
            if let Some((_, def)) = self.get_root(&path) {
                let _: Result<_> = def.advertise(path);
            }
        }
    }

    fn process_update(&mut self, batch: &mut UpdateBatch, mut update: db::Update) {
        use db::UpdateKind;
        let mut locked = false;
        let mut roots = false;
        let mut rels = RELS.take();
        fn add_rel(rels: &mut Pooled<FxHashSet<Path>>, rel: &Path) {
            if let Some(table) = Path::dirname(rel).and_then(Path::dirname) {
                if !rels.contains(table) {
                    rels.insert(Path::from(ArcStr::from(table)));
                }
            }
        }
        for path in update.added_roots.drain(..) {
            roots = true;
            match self.ctx.user.publisher.publish_default(path.clone()) {
                Err(_) => (), // CR estokes: log this
                Ok(dh) => {
                    self.roots.insert(path, dh);
                }
            }
        }
        for path in update.removed_roots.drain(..) {
            roots = true;
            self.roots.remove(&path);
        }
        for (path, value) in update.data.drain(..) {
            match value {
                UpdateKind::Updated(v) => {
                    match self.ctx.user.by_path.get(&path) {
                        None => (),
                        Some(Published::Data(p)) => p.val.update(batch, v.clone()),
                        Some(Published::Formula(_)) => unreachable!(),
                    }
                    self.ctx.user.ref_updates.push((path, v));
                }
                UpdateKind::Inserted(v) => {
                    if self.ctx.user.by_path.contains_key(&path) {
                        self.remove_deleted_published(batch, &path);
                    }
                    self.ctx.user.ref_updates.push((path.clone(), v));
                    add_rel(&mut rels, &path);
                    self.advertise_path(path);
                }
                UpdateKind::Deleted => {
                    self.remove_deleted_published(batch, &path);
                    add_rel(&mut rels, &path);
                }
            }
        }
        for (path, value) in update.formula.drain(..) {
            match value {
                UpdateKind::Updated(v) => match self.ctx.user.by_path.get(&path) {
                    None => unreachable!(),
                    Some(Published::Data(_)) => unreachable!(),
                    Some(Published::Formula(fifo)) => {
                        let fifo = fifo.clone();
                        // CR estokes: log
                        let _: Result<_> = self.change_formula(batch, &fifo, to_chars(v));
                    }
                },
                UpdateKind::Inserted(v) => {
                    if self.ctx.user.by_path.contains_key(&path) {
                        self.remove_deleted_published(batch, &path);
                    }
                    // CR estokes: log
                    add_rel(&mut rels, &path);
                    let _: Result<_> = self.publish_formula(path, batch, v, Value::Null);
                }
                UpdateKind::Deleted => {
                    self.remove_deleted_published(batch, &path);
                    add_rel(&mut rels, &path);
                }
            }
        }
        for (path, value) in update.on_write.drain(..) {
            match value {
                UpdateKind::Updated(v) => match self.ctx.user.by_path.get(&path) {
                    None => unreachable!(),
                    Some(Published::Data(_)) => unreachable!(),
                    Some(Published::Formula(fifo)) => {
                        let fifo = fifo.clone();
                        // CR estokes: log
                        let _: Result<_> =
                            self.change_on_write(batch, &fifo, to_chars(v));
                    }
                },
                UpdateKind::Inserted(v) => {
                    if self.ctx.user.by_path.contains_key(&path) {
                        self.remove_deleted_published(batch, &path);
                    }
                    // CR estokes: log
                    add_rel(&mut rels, &path);
                    let _: Result<_> = self.publish_formula(path, batch, Value::Null, v);
                }
                UpdateKind::Deleted => {
                    self.remove_deleted_published(batch, &path);
                    add_rel(&mut rels, &path);
                }
            }
        }
        for path in update.locked.drain(..) {
            locked = true;
            if self.is_locked_gen(&path, true) {
                self.locked.remove(&path);
            } else {
                self.locked.insert(path, true);
            }
        }
        for path in update.unlocked.drain(..) {
            locked = true;
            if !self.is_locked_gen(&path, true) {
                self.locked.remove(&path);
            } else {
                self.locked.insert(path, false);
            }
        }
        self.update_refs(batch);
        if !rels.is_empty() {
            self.update_rels(rels, batch);
        }
        if locked {
            // CR estokes: log this
            if let Some(stats) = &mut self.stats {
                let _: Result<_> = stats.set_locked(batch, &self.locked);
            }
        }
        if roots {
            // CR estokes: log this
            if let Some(stats) = &mut self.stats {
                let _: Result<_> = stats.set_roots(batch, &self.roots);
            }
        }
    }

    fn process_command(&mut self, txn: &mut Txn, c: ToInner) {
        match c {
            ToInner::GetDb(s) => {
                let _ = s.send(self.ctx.user.db.clone());
            }
            ToInner::GetPub(s) => {
                let _ = s.send(self.ctx.user.publisher.clone());
            }
            ToInner::Commit(t, s) => {
                *txn = t;
                let _ = s.send(());
            }
        }
    }

    async fn run(mut self, mut cmd: mpsc::UnboundedReceiver<ToInner>) -> Result<()> {
        let mut gc_rpcs = time::interval(Duration::from_secs(60));
        let mut rpcbatch = Vec::new();
        let mut batch = self.ctx.user.publisher.start_batch();
        let mut txn = Txn::new();
        async fn api_rx(api: &mut Option<RpcApi>) -> BatchItem<RpcRequest> {
            match api {
                Some(api) => api.rx.select_next_some().await,
                None => future::pending().await,
            }
        }
        loop {
            select_biased! {
                r = self.publish_events.select_next_some() => {
                    self.process_publish_event(r);
                },
                r = self.roots.select_next_some() => {
                    self.process_publish_request(r.0, r.1)
                },
                r = self.sub_updates.select_next_some() => {
                    self.process_subscriptions(&mut batch, r);
                },
                r = self.write_updates_rx.select_next_some() => {
                    self.process_writes(&mut batch, &mut txn, r)
                },
                r = api_rx(&mut self.api).fuse() => match r {
                    BatchItem::InBatch(v) => rpcbatch.push(v),
                    BatchItem::EndBatch => self.process_rpc_requests(&mut txn, &mut rpcbatch)
                },
                r = self.bscript_event.select_next_some() => {
                    self.process_bscript_event(&mut batch, r)
                },
                _ = gc_rpcs.tick().fuse() => {
                    self.gc_rpcs();
                },
                u = self.db_updates.select_next_some() => {
                    self.process_update(&mut batch, u);
                },
                c = cmd.select_next_some() => {
                    self.process_command(&mut txn, c);
                },
                () = &mut self.timer => {
                    self.process_timer_tick(&mut batch);
                },
                complete => break,
            }
            if txn.dirty() {
                self.ctx.user.db.commit(mem::replace(&mut txn, Txn::new()));
            }
            if batch.len() > 0 {
                let timeout = self.params.timeout.map(Duration::from_secs);
                let new_batch = self.ctx.user.publisher.start_batch();
                mem::replace(&mut batch, new_batch).commit(timeout).await;
            }
        }
        self.ctx.user.publisher.clone().shutdown().await;
        self.ctx.user.db.flush_async().await?;
        Ok(())
    }
}

enum ToInner {
    GetDb(oneshot::Sender<Db>),
    GetPub(oneshot::Sender<Publisher>),
    Commit(Txn, oneshot::Sender<()>),
}

pub struct Container(mpsc::UnboundedSender<ToInner>);

impl Container {
    /// Start the container with the specified
    /// parameters. Initialization is performed on the calling task,
    /// and an error is returned if it fails. Once initialization is
    /// complete, the container continues to run on a background task,
    /// and errors are reported by log::error!.
    pub async fn start(
        cfg: Config,
        auth: DesiredAuth,
        params: Params,
    ) -> Result<Container> {
        let (w, r) = mpsc::unbounded();
        let mut c = ContainerInner::new(cfg, auth, params).await?;
        c.init().await?;
        task::spawn(async move {
            match c.run(r).await {
                Err(e) => error!("container stopped with error {}", e),
                Ok(()) => info!("container stopped gracefully"),
            }
        });
        Ok(Container(w))
    }

    /// Fetch the database associated with the container. You can
    /// perform any read operations you like on the database, however
    /// keep in mind that clients can write while you are
    /// reading. Transactions must be submitted to the main task to
    /// ensure some level of coordination between client writes and
    /// application transactions.
    pub async fn db(&self) -> Result<Db> {
        let (w, r) = oneshot::channel();
        self.0.unbounded_send(ToInner::GetDb(w))?;
        Ok(r.await?)
    }

    /// Submit a database transaction to be processed, wait for it to
    /// be accepted.
    pub async fn commit(&self, txn: Txn) -> Result<()> {
        let (w, r) = oneshot::channel();
        self.0.unbounded_send(ToInner::Commit(txn, w))?;
        Ok(r.await?)
    }

    /// Submit a database transaction without waiting
    pub fn commit_unbounded(&self, txn: Txn) -> Result<()> {
        let (w, _r) = oneshot::channel();
        Ok(self.0.unbounded_send(ToInner::Commit(txn, w))?)
    }

    /// Retreive the container's publisher.
    pub async fn publisher(&self) -> Result<Publisher> {
        let (w, r) = oneshot::channel();
        self.0.unbounded_send(ToInner::GetPub(w))?;
        Ok(r.await?)
    }
}
