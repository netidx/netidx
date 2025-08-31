#[macro_use]
extern crate netidx_protocols;

mod db;
mod rpcs;
mod stats;

use crate::rpcs::RpcApi;
use anyhow::{bail, Result};
use arcstr::{literal, ArcStr};
pub use db::{Datum, DatumKind, Db, Reply, Sendable, Txn};
use futures::{
    self,
    channel::{mpsc, oneshot},
    future,
    prelude::*,
    select_biased,
    stream::FusedStream,
};
use fxhash::FxHashMap;
use log::{error, info};
use netidx::{
    config::Config,
    path::Path,
    publisher::{
        BindCfg, DefaultHandle, Event as PEvent, Id, PublishFlags, Publisher,
        PublisherBuilder, UpdateBatch, Val, Value, WriteRequest,
    },
    resolver_client::DesiredAuth,
    utils::BatchItem,
};
use parking_lot::Mutex;
use poolshark::global::GPooled;
use rpcs::{RpcRequest, RpcRequestKind};
use stats::Stats;
use std::{
    collections::{
        BTreeMap,
        Bound::{self, *},
    },
    mem,
    ops::{Deref, DerefMut},
    path::PathBuf,
    pin::Pin,
    time::Duration,
};
use structopt::StructOpt;
use tokio::task;
use triomphe::Arc;

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
    #[structopt(long = "slack", help = "set the publisher slack (default 3 batches)")]
    pub slack: Option<usize>,
    #[structopt(
        long = "max_clients",
        help = "set the maximum number of clients (default 768)"
    )]
    pub max_clients: Option<usize>,
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

macro_rules! or_reply {
    ($reply:expr, $r:expr) => {
        match $r {
            Ok(r) => r,
            Err(e) => {
                if let Some(reply) = $reply {
                    let e = Value::Error(format!("{}", e).into());
                    reply.send(e);
                }
                return;
            }
        }
    };
}

struct Published {
    path: Path,
    val: Val,
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

struct ContainerInner {
    params: Params,
    db: Db,
    publisher: Publisher,
    api_path: Option<Path>,
    stats: Option<Stats>,
    locked: BTreeMap<Path, bool>,
    write_updates_tx: mpsc::Sender<GPooled<Vec<WriteRequest>>>,
    write_updates_rx: mpsc::Receiver<GPooled<Vec<WriteRequest>>>,
    by_id: FxHashMap<Id, Arc<Published>>,
    by_path: FxHashMap<Path, Arc<Published>>,
    publish_events: mpsc::UnboundedReceiver<PEvent>,
    roots: Roots,
    db_updates: mpsc::UnboundedReceiver<db::Update>,
    api: Option<rpcs::RpcApi>,
}

impl ContainerInner {
    async fn new(cfg: Config, auth: DesiredAuth, params: Params) -> Result<Self> {
        let (publish_events_tx, publish_events) = mpsc::unbounded();
        let mut publisher = PublisherBuilder::new(cfg.clone());
        publisher.desired_auth(auth.clone()).bind_cfg(params.bind);
        if let Some(n) = params.slack {
            publisher.slack(n);
        }
        if let Some(n) = params.max_clients {
            publisher.max_clients(n);
        }
        let publisher = publisher.build().await?;
        publisher.events(publish_events_tx);
        let (db, db_updates) =
            Db::new(&params, publisher.clone(), params.api_path.clone())?;
        let (write_updates_tx, write_updates_rx) = mpsc::channel(3);
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
        Ok(Self {
            params,
            api_path,
            stats,
            locked: BTreeMap::new(),
            roots: Roots(BTreeMap::new()),
            db,
            publisher,
            write_updates_tx,
            write_updates_rx,
            by_id: FxHashMap::default(),
            by_path: FxHashMap::default(),
            publish_events,
            db_updates,
            api,
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

    fn publish_data(&mut self, path: Path, value: Value) -> Result<()> {
        self.advertise_path(path.clone());
        let val = self.publisher.publish_with_flags(
            PublishFlags::DESTROY_ON_IDLE,
            path.clone(),
            value.clone(),
        )?;
        let id = val.id();
        self.publisher.writes(val.id(), self.write_updates_tx.clone());
        let published = Arc::new(Published { path: path.clone(), val });
        self.by_id.insert(id, published.clone());
        self.by_path.insert(path.clone(), published);
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        let mut batch = self.publisher.start_batch();
        for res in self.db.roots() {
            let path = res?;
            let def = self.publisher.publish_default(path.clone())?;
            self.roots.insert(path, def);
        }
        if let Some(stats) = &mut self.stats {
            let _ = stats.set_roots(&mut batch, &self.roots);
        }
        for res in self.db.locked() {
            let (path, locked) = res?;
            let path = self.check_path(path)?;
            self.locked.insert(path, locked);
        }
        if let Some(stats) = &mut self.stats {
            let _ = stats.set_locked(&mut batch, &self.locked);
        }
        for res in self.db.iter() {
            let (path, kind, _) = res?;
            match kind {
                DatumKind::Data | DatumKind::Formula => {
                    let path = self.check_path(path)?;
                    self.advertise_path(path);
                }
                DatumKind::Deleted | DatumKind::Invalid => (),
            }
        }
        Ok(batch.commit(self.params.timeout.map(Duration::from_secs)).await)
    }

    fn process_writes(&mut self, txn: &mut Txn, mut writes: GPooled<Vec<WriteRequest>>) {
        // CR estokes: log this
        for req in writes.drain(..) {
            let reply = req.send_result.map(Sendable::Write);
            if let Some(p) = self.by_id.get(&req.id) {
                txn.set_data(true, p.path.clone(), req.value, reply);
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
                match self.db.lookup(path.as_ref()) {
                    Ok(Some(Datum::Data(v) | Datum::Formula(v, _))) => {
                        let _: Result<()> = self.publish_data(path, v);
                    }
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
                let _: Result<_, _> = reply.send(());
            }
        }
    }

    fn process_publish_event(&mut self, e: PEvent) {
        match e {
            PEvent::Subscribe(_, _) | PEvent::Unsubscribe(_, _) => (),
            PEvent::Destroyed(id) => {
                if let Some(p) = self.by_id.remove(&id) {
                    self.by_path.remove(&p.path);
                }
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
            let m = literal!("rows <= max_rows && columns <= max_columns");
            if let Some(reply) = reply {
                reply.send(Value::Error(m));
            }
        } else {
            txn.create_sheet(path, rows, columns, max_rows, max_columns, lock, reply);
        }
    }

    fn create_table(
        &self,
        txn: &mut Txn,
        path: Path,
        rows: Vec<ArcStr>,
        columns: Vec<ArcStr>,
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

    fn remove_deleted_published(&mut self, path: &Path) {
        self.remove_advertisement(&path);
        if let Some(p) = self.by_path.remove(path) {
            self.by_id.remove(&p.val.id());
        }
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
        for path in update.added_roots.drain(..) {
            roots = true;
            match self.publisher.publish_default(path.clone()) {
                Err(e) => error!("failed to publish_default {path} {e:?}"),
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
                    if let Some(p) = self.by_path.get(&path) {
                        p.val.update(batch, v.clone())
                    }
                }
                UpdateKind::Inserted => {
                    if self.by_path.contains_key(&path) {
                        self.remove_deleted_published(&path);
                    }
                    self.advertise_path(path);
                }
                UpdateKind::Deleted => self.remove_deleted_published(&path),
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
                let _ = s.send(self.db.clone());
            }
            ToInner::GetPub(s) => {
                let _ = s.send(self.publisher.clone());
            }
            ToInner::Commit(t, s) => {
                *txn = t;
                let _ = s.send(());
            }
        }
    }

    async fn run(mut self, mut cmd: mpsc::UnboundedReceiver<ToInner>) -> Result<()> {
        let mut rpcbatch = Vec::new();
        let mut batch = self.publisher.start_batch();
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
                r = api_rx(&mut self.api).fuse() => match r {
                    BatchItem::InBatch(v) => rpcbatch.push(v),
                    BatchItem::EndBatch => self.process_rpc_requests(&mut txn, &mut rpcbatch)
                },
                u = self.db_updates.select_next_some() => {
                    self.process_update(&mut batch, u);
                },
                c = cmd.select_next_some() => {
                    self.process_command(&mut txn, c);
                },
                w = self.write_updates_rx.select_next_some() => {
                    self.process_writes(&mut txn, w);
                }
                complete => break,
            }
            if txn.dirty() {
                self.db.commit(mem::replace(&mut txn, Txn::new()));
            }
            if batch.len() > 0 {
                let timeout = self.params.timeout.map(Duration::from_secs);
                let new_batch = self.publisher.start_batch();
                mem::replace(&mut batch, new_batch).commit(timeout).await;
            }
        }
        self.publisher.clone().shutdown().await;
        self.db.flush_async().await?;
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
