use crate::{
    auth::{Permissions, UserInfo},
    channel::Channel,
    os::ServerCtx,
    path::Path,
    pool::{Pool, Poolable, Pooled},
    protocol::resolver::v1::{
        FromRead, FromWrite, Referral, Resolved, Table, ToRead, ToWrite,
    },
    resolver_store::{self, MAX_READ_BATCH, MAX_WRITE_BATCH},
    secstore::SecStore,
};
use anyhow::Result;
use futures::{future::join_all, prelude::*, select};
use fxhash::FxBuildHasher;
use log::{debug, info};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    hash::{BuildHasher, Hash, Hasher},
    net::SocketAddr,
    result,
    sync::Arc,
    time::SystemTime,
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot::{self, error::RecvError},
    },
    task,
    time::Instant,
};

type ReadB = Vec<(u64, ToRead)>;
type ReadR = VecDeque<(u64, FromRead)>;
type WriteB = Vec<(u64, ToWrite)>;
type WriteR = VecDeque<(u64, FromWrite)>;

lazy_static! {
    static ref TO_READ_POOL: Pool<ReadB> = Pool::new(640);
    static ref FROM_READ_POOL: Pool<ReadR> = Pool::new(640);
    static ref TO_WRITE_POOL: Pool<WriteB> = Pool::new(640);
    static ref FROM_WRITE_POOL: Pool<WriteR> = Pool::new(640);
}

struct ReadRequest {
    uifo: Arc<UserInfo>,
    batch: Pooled<ReadB>,
}

struct WriteRequest {
    uifo: Arc<UserInfo>,
    write_addr: SocketAddr,
    batch: Pooled<WriteB>,
}

#[derive(Clone)]
struct Shard {
    read: UnboundedSender<(ReadRequest, oneshot::Sender<Pooled<ReadR>>)>,
    write: UnboundedSender<(WriteRequest, oneshot::Sender<Pooled<WriteR>>)>,
    internal: UnboundedSender<(SocketAddr, oneshot::Sender<HashSet<Path>>)>,
}

impl Shard {
    fn new(
        parent: Option<Referral>,
        children: BTreeMap<Path, Referral>,
        secstore: Option<SecStore>,
        resolver: SocketAddr,
    ) -> Self {
        let (read, read_rx) = unbounded_channel();
        let (write, write_rx) = unbounded_channel();
        let (internal, internal_rx) = unbounded_channel();
        let t = Shard { read, write, internal };
        task::spawn(async move {
            let mut store = resolver_store::Store::new(parent, children);
            let mut read_rx = read_rx.fuse();
            let mut write_rx = write_rx.fuse();
            let mut internal_rx = internal_rx.fuse();
            loop {
                select! {
                    batch = read_rx.next() => match batch {
                        None => break,
                        Some((req, reply)) => {
                            let r = Shard::process_read_batch(
                                &mut store,
                                secstore.as_ref(),
                                resolver,
                                req
                            );
                            let _ = reply.send(r);
                        }
                    },
                    batch = write_rx.next() => match batch {
                        None => break,
                        Some((req, reply)) => {
                            let r = Shard::process_write_batch(
                                &mut store,
                                secstore.as_ref(),
                                req
                            );
                            let _ = reply.send(r);
                        }
                    },
                    addr = internal_rx.next() => match addr {
                        None => break,
                        Some((addr, reply)) => {
                            let _ = reply.send(store.published_for_addr(&addr));
                        }
                    }
                }
            }
            info!("shard loop finished")
        });
        t
    }

    fn process_read_batch(
        store: &mut resolver_store::Store,
        secstore: Option<&SecStore>,
        resolver: SocketAddr,
        mut req: ReadRequest,
    ) -> Pooled<ReadR> {
        // things would need to be massively screwed for this to fail
        let now =
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let mut resp = FROM_READ_POOL.take();
        let sec = secstore.map(|s| s.store.read());
        let uifo = req.uifo;
        resp.extend(req.batch.drain(..).map(|(id, m)| match m {
            ToRead::Resolve(path) => {
                if let Some(r) = store.check_referral(&path) {
                    (id, FromRead::Referral(r))
                } else {
                    match secstore {
                        None => {
                            let a = Resolved {
                                krb5_spns: Pooled::orphan(HashMap::with_hasher(
                                    FxBuildHasher::default(),
                                )),
                                resolver,
                                addrs: store.resolve(&path),
                                timestamp: now,
                                permissions: Permissions::all().bits(),
                            };
                            (id, FromRead::Resolved(a))
                        }
                        Some(ref secstore) => {
                            let perm = secstore.pmap().permissions(&*path, &*uifo);
                            if !perm.contains(Permissions::SUBSCRIBE) {
                                (id, FromRead::Denied)
                            } else {
                                let (krb5_spns, addrs) = store.resolve_and_sign(
                                    &**sec.as_ref().unwrap(),
                                    now,
                                    perm,
                                    &path,
                                );
                                let a = Resolved {
                                    krb5_spns,
                                    resolver,
                                    addrs,
                                    timestamp: now,
                                    permissions: perm.bits(),
                                };
                                (id, FromRead::Resolved(a))
                            }
                        }
                    }
                }
            }
            ToRead::List(path) => {
                if let Some(r) = store.check_referral(&path) {
                    (id, FromRead::Referral(r))
                } else {
                    let allowed = secstore
                        .map(|s| s.pmap().allowed(&*path, Permissions::LIST, &*uifo))
                        .unwrap_or(true);
                    if allowed {
                        (id, FromRead::List(store.list(&path)))
                    } else {
                        (id, FromRead::Denied)
                    }
                }
            }
            ToRead::Table(path) => {
                if let Some(r) = store.check_referral(&path) {
                    (id, FromRead::Referral(r))
                } else {
                    let allowed = secstore
                        .map(|s| s.pmap().allowed(&*path, Permissions::LIST, &*uifo))
                        .unwrap_or(true);
                    if !allowed {
                        (id, FromRead::Denied)
                    } else {
                        let rows = store.list(&path);
                        let cols = store.columns(&path);
                        (id, FromRead::Table(Table { rows, cols }))
                    }
                }
            }
        }));
        resp
    }

    fn process_write_batch(
        store: &mut resolver_store::Store,
        secstore: Option<&SecStore>,
        mut req: WriteRequest,
    ) -> Pooled<WriteR> {
        let uifo = &*req.uifo;
        let write_addr = req.write_addr;
        let publish = |s: &mut resolver_store::Store,
                       path: Path,
                       default: bool|
         -> FromWrite {
            if !Path::is_absolute(&*path) {
                FromWrite::Error("absolute paths required".into())
            } else if let Some(r) = s.check_referral(&path) {
                FromWrite::Referral(r)
            } else {
                let perm = if default {
                    Permissions::PUBLISH_DEFAULT
                } else {
                    Permissions::PUBLISH
                };
                if secstore.map(|s| s.pmap().allowed(&*path, perm, uifo)).unwrap_or(true)
                {
                    s.publish(path, write_addr, default);
                    FromWrite::Published
                } else {
                    FromWrite::Denied
                }
            }
        };
        let mut resp = FROM_WRITE_POOL.take();
        resp.extend(req.batch.drain(..).map(|(id, m)| match m {
            ToWrite::Heartbeat | ToWrite::Clear => unreachable!(),
            ToWrite::Publish(path) => (id, publish(store, path, false)),
            ToWrite::PublishDefault(path) => (id, publish(store, path, true)),
            ToWrite::Unpublish(path) => {
                if !Path::is_absolute(&*path) {
                    (id, FromWrite::Error("absolute paths required".into()))
                } else if let Some(r) = store.check_referral(&path) {
                    (id, FromWrite::Referral(r))
                } else {
                    store.unpublish(path, write_addr);
                    (id, FromWrite::Unpublished)
                }
            }
        }));
        resp
    }
}

pub(crate) struct Store {
    shards: Vec<Shard>,
    build_hasher: FxBuildHasher,
}

impl Store {
    pub(crate) fn new(
        parent: Option<Referral>,
        children: BTreeMap<Path, Referral>,
        secstore: Option<SecStore>,
        resolver: SocketAddr,
    ) -> Arc<Self> {
        let shards = (0..num_cpus::get())
            .into_iter()
            .map(|_| {
                Shard::new(parent.clone(), children.clone(), secstore.clone(), resolver)
            })
            .collect();
        Arc::new(Store { shards, build_hasher: FxBuildHasher::default() })
    }

    fn shard(&self, path: &Path) -> usize {
        let mut hasher = self.build_hasher.build_hasher();
        path.hash(&mut hasher);
        hasher.finish() as usize % self.shards.len()
    }

    fn shard_batch<T: Poolable + Send + Sync + 'static>(
        &self,
        pool: &Pool<T>,
    ) -> Vec<Pooled<T>> {
        (0..self.shards.len()).into_iter().map(|_| pool.take()).collect::<Vec<_>>()
    }

    pub(crate) async fn handle_batch_read(
        &self,
        con: &mut Channel<ServerCtx>,
        uifo: Arc<UserInfo>,
        mut msgs: impl Iterator<Item = ToRead>,
    ) -> Result<()> {
        let mut finished = false;
        loop {
            let mut n = 0;
            let mut by_shard = self.shard_batch(&*TO_READ_POOL);
            for _ in 0..MAX_READ_BATCH {
                match msgs.next() {
                    None => {
                        finished = true;
                        break;
                    }
                    Some(ToRead::Resolve(path)) => {
                        let s = self.shard(&path);
                        by_shard[s].push((n, ToRead::Resolve(path)));
                    }
                    Some(ToRead::List(path)) => {
                        for b in by_shard.iter_mut() {
                            b.push((n, ToRead::List(path.clone())));
                        }
                    }
                    Some(ToRead::Table(path)) => {
                        for b in by_shard.iter_mut() {
                            b.push((n, ToRead::Table(path.clone())));
                        }
                    }
                }
                n += 1;
            }
            if by_shard.iter().all(|v| v.is_empty()) {
                assert!(finished);
                break Ok(());
            }
            let mut replies =
                join_all(by_shard.drain(..).enumerate().map(|(i, batch)| {
                    let (tx, rx) = oneshot::channel();
                    let req = ReadRequest { uifo: uifo.clone(), batch };
                    let _ = self.shards[i].read.send((req, tx));
                    rx
                }))
                .await
                .into_iter()
                .collect::<result::Result<Vec<Pooled<ReadR>>, RecvError>>()?;
            for i in 0..n {
                if !replies.iter().all(|v| v.front().map(|v| i == v.0).unwrap_or(false)) {
                    let r = replies
                        .iter_mut()
                        .find(|r| r.front().map(|v| v.0 == i).unwrap_or(false))
                        .unwrap()
                        .pop_front()
                        .unwrap()
                        .1;
                    con.queue_send(&r)?;
                } else {
                    match replies[0].pop_front().unwrap() {
                        (_, FromRead::Resolved(_)) => unreachable!(),
                        (_, FromRead::Referral(r)) => {
                            for i in 1..replies.len() {
                                match replies[i].pop_front().unwrap() {
                                    (_, FromRead::Referral(r_)) if r == r_ => (),
                                    _ => panic!("desynced referral"),
                                }
                            }
                            con.queue_send(&FromRead::Referral(r))?;
                        }
                        (_, FromRead::Denied) => {
                            for i in 1..replies.len() {
                                match replies[i].pop_front().unwrap() {
                                    (_, FromRead::Denied) => (),
                                    _ => panic!("desynced permissions"),
                                }
                            }
                            con.queue_send(&FromRead::Denied)?;
                        }
                        (_, FromRead::Error(e)) => {
                            for i in 1..replies.len() {
                                replies[i].pop_front().unwrap();
                            }
                            con.queue_send(&FromRead::Error(e))?;
                        }
                        (_, FromRead::List(mut paths)) => {
                            for i in 1..replies.len() {
                                if let (_, FromRead::List(mut p)) =
                                    replies[i].pop_front().unwrap()
                                {
                                    paths.extend(p.drain(..));
                                } else {
                                    panic!("desynced list")
                                }
                            }
                            con.queue_send(&FromRead::List(paths))?;
                        }
                        (_, FromRead::Table(Table { mut rows, mut cols })) => {
                            for i in 1..replies.len() {
                                if let (
                                    _,
                                    FromRead::Table(Table { rows: mut rs, cols: mut cs }),
                                ) = replies[i].pop_front().unwrap()
                                {
                                    rows.extend(rs.drain(..));
                                    cols.extend(cs.drain(..));
                                } else {
                                    panic!("desynced table")
                                }
                            }
                            rows.sort();
                            rows.dedup();
                            cols.sort();
                            cols.dedup();
                            con.queue_send(&FromRead::Table(Table { rows, cols }))?;
                        }
                    }
                }
            }
            if finished {
                break Ok(());
            }
        }
    }

    pub(crate) async fn handle_batch_write_no_clear(
        &self,
        mut con: Option<&mut Channel<ServerCtx>>,
        uifo: Arc<UserInfo>,
        write_addr: SocketAddr,
        mut msgs: impl Iterator<Item = ToWrite>,
    ) -> Result<()> {
        let mut finished = false;
        loop {
            let mut n = 0;
            let mut by_shard = self.shard_batch(&*TO_WRITE_POOL);
            let start = Instant::now();
            for _ in 0..MAX_WRITE_BATCH {
                match msgs.next() {
                    None => {
                        finished = true;
                        break;
                    }
                    Some(ToWrite::Heartbeat) => continue,
                    Some(ToWrite::Clear) => unreachable!("call process_clear instead"),
                    Some(ToWrite::Publish(path)) => {
                        let s = self.shard(&path);
                        by_shard[s].push((n, ToWrite::Publish(path)));
                    }
                    Some(ToWrite::Unpublish(path)) => {
                        let s = self.shard(&path);
                        by_shard[s].push((n, ToWrite::Unpublish(path)));
                    }
                    Some(ToWrite::PublishDefault(path)) => {
                        let s = self.shard(&path);
                        by_shard[s].push((n, ToWrite::PublishDefault(path)));
                    }
                }
                n += 1;
            }
            if by_shard.iter().all(|v| v.is_empty()) {
                assert!(finished);
                break Ok(());
            }
            debug!(
                "submitting write batch of size: {} build: {}s shards: {:?}",
                n,
                start.elapsed().as_secs_f32(),
                by_shard.iter().map(|v| v.len()).collect::<Vec<_>>()
            );
            let mut replies =
                join_all(by_shard.drain(..).enumerate().map(|(i, batch)| {
                    let (tx, rx) = oneshot::channel();
                    let req = WriteRequest { uifo: uifo.clone(), write_addr, batch };
                    let _ = self.shards[i].write.send((req, tx));
                    rx
                }))
                .await
                .into_iter()
                .collect::<result::Result<Vec<Pooled<WriteR>>, RecvError>>()?;
            if let Some(ref mut c) = con {
                for i in 0..n {
                    let r = replies
                        .iter_mut()
                        .find(|v| v.front().map(|v| v.0 == i).unwrap_or(false))
                        .unwrap()
                        .pop_front()
                        .unwrap()
                        .1;
                    c.queue_send(&r)?;
                }
            }
            if finished {
                break Ok(());
            }
        }
    }

    pub(crate) async fn handle_clear(
        &self,
        uifo: Arc<UserInfo>,
        write_addr: SocketAddr,
    ) -> Result<()> {
        use rand::{prelude::*, thread_rng};
        let mut published_paths = join_all(self.shards.iter().map(|shard| {
            let (tx, rx) = oneshot::channel();
            let _ = shard.internal.send((write_addr, tx));
            rx
        }))
        .await
        .into_iter()
        .flat_map(|s| s.unwrap().into_iter().map(ToWrite::Unpublish))
        .collect::<Vec<_>>();
        published_paths.shuffle(&mut thread_rng());
        self.handle_batch_write_no_clear(
            None,
            uifo,
            write_addr,
            published_paths.into_iter(),
        )
        .await?;
        Ok(())
    }
}
