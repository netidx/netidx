use super::{
    auth::{Permissions, UserInfo},
    secctx::{SecCtx, SecCtxDataReadGuard},
    store::{self, COLS_POOL, MAX_READ_BATCH, MAX_WRITE_BATCH, PATH_POOL, REF_POOL},
};
use crate::{
    channel::Channel,
    pack::Z64,
    path::Path,
    pool::{Pool, Pooled},
    protocol::{
        glob::Scope,
        resolver::{
            FromRead, FromWrite, GetChangeNr, ListMatching, Publisher, PublisherId,
            Referral, Resolved, Table, ToRead, ToWrite,
        },
    },
};
use anyhow::Result;
use futures::{
    channel::{
        mpsc::{unbounded, UnboundedSender},
        oneshot::{self, Canceled},
    },
    future::join_all,
    prelude::*,
    select,
};
use fxhash::FxHashMap;
use log::{info, trace};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, HashMap, HashSet, VecDeque},
    hash::{Hash, Hasher},
    iter,
    net::SocketAddr,
    result,
    sync::Arc,
    time::SystemTime,
};
use tokio::task;

type ReadB = Vec<(u64, ToRead)>;
type ReadR = VecDeque<(u64, FromRead)>;
type WriteB = Vec<(u64, ToWrite)>;
type WriteR = VecDeque<(u64, FromWrite)>;

lazy_static! {
    static ref PUBLISHERS_POOL: Pool<FxHashMap<PublisherId, Publisher>> =
        Pool::new(1024, 1000);
    static ref TO_READ_POOL: Pool<ReadB> = Pool::new(640, 150000);
    static ref FROM_READ_POOL: Pool<ReadR> = Pool::new(640, 150000);
    static ref TO_WRITE_POOL: Pool<WriteB> = Pool::new(640, 15000);
    static ref REPLIES: Pool<Vec<Pooled<ReadR>>> = Pool::new(10, 1024);
    static ref FROM_WRITE_POOL: Pool<WriteR> = Pool::new(640, 15000);
    static ref COLS_HPOOL: Pool<HashMap<Path, Z64>> = Pool::new(32, 10000);
    static ref PATH_HPOOL: Pool<HashSet<Path>> = Pool::new(32, 10000);
    static ref PATH_BPOOL: Pool<Vec<Pooled<Vec<Path>>>> = Pool::new(32, 1024);
    static ref READ_SHARD_BATCH: Pool<Vec<Pooled<ReadB>>> = Pool::new(1000, 1024);
    static ref WRITE_SHARD_BATCH: Pool<Vec<Pooled<WriteB>>> = Pool::new(1000, 1024);
}

struct ReadRequest {
    uifo: Arc<UserInfo>,
    batch: Pooled<ReadB>,
}

struct ReadResponse {
    publishers: Pooled<FxHashMap<PublisherId, Publisher>>,
    batch: Pooled<ReadR>,
}

struct WriteRequest {
    uifo: Arc<UserInfo>,
    publisher: Arc<Publisher>,
    batch: Pooled<WriteB>,
}

#[derive(Clone)]
struct Shard {
    read: UnboundedSender<(ReadRequest, oneshot::Sender<ReadResponse>)>,
    write: UnboundedSender<(WriteRequest, oneshot::Sender<Pooled<WriteR>>)>,
    internal: UnboundedSender<(PublisherId, oneshot::Sender<HashSet<Path>>)>,
}

impl Shard {
    fn new(
        shard: usize,
        parent: Option<Referral>,
        children: BTreeMap<Path, Referral>,
        secctx: SecCtx,
        resolver: SocketAddr,
    ) -> Self {
        let (read, read_rx) = unbounded();
        let (write, write_rx) = unbounded();
        let (internal, mut internal_rx) = unbounded();
        let mut read_rx = read_rx.fuse();
        let mut write_rx = write_rx.fuse();
        let t = Shard { read, write, internal };
        task::spawn(async move {
            let mut store = store::Store::new(parent, children);
            loop {
                select! {
                    batch = read_rx.next() => match batch {
                        None => break,
                        Some((req, reply)) => {
			    let secctx = secctx.read().await;
                            let r = Shard::process_read_batch(
                                shard,
                                &mut store,
                                &secctx,
                                resolver,
                                req
                            ).await;
                            let _ = reply.send(r);
                        }
                    },
                    batch = write_rx.next() => match batch {
                        None => break,
                        Some((req, reply)) => {
			    let secctx = secctx.read().await;
                            let r = Shard::process_write_batch(
                                &mut store,
                                &secctx,
                                req
                            ).await;
                            let _ = reply.send(r);
                        }
                    },
                    id = internal_rx.next() => match id {
                        None => break,
                        Some((id, reply)) => {
                            let _ = reply.send(store.published_for_id(&id));
                        }
                    }
                }
            }
            info!("shard loop finished")
        });
        t
    }

    async fn process_read_batch<'a>(
        shard: usize,
        store: &mut store::Store,
        secctx: &SecCtxDataReadGuard<'a>,
        resolver: SocketAddr,
        mut req: ReadRequest,
    ) -> ReadResponse {
        // things would need to be massively screwed for this to fail
        let now =
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
        let mut resp = ReadResponse {
            publishers: PUBLISHERS_POOL.take(),
            batch: FROM_READ_POOL.take(),
        };
        let uifo = req.uifo;
        let pmap = secctx.pmap();
	let mut n = 0;
	for (id, m) in req.batch.drain(..) {
	    if n > 10_000 {
		n = 0;
		task::yield_now().await
	    }
	    resp.batch.push_back(match m {
		ToRead::Resolve(path) => {
		    n += 1;
                    if let Some(r) = store.check_referral(&path) {
			(id, FromRead::Referral(r))
                    } else {
			match pmap {
                            None => {
				let (flags, publishers) =
                                    store.resolve(&mut resp.publishers, &path);
				let a = Resolved {
                                    resolver,
                                    publishers,
                                    timestamp: now,
                                    permissions: Permissions::all().bits(),
                                    flags,
				};
				(id, FromRead::Resolved(a))
                            }
                            Some(pmap) => {
				let perm = pmap.permissions(&*path, &*uifo);
				if !perm.contains(Permissions::SUBSCRIBE) {
                                    (id, FromRead::Denied)
				} else {
                                    let (flags, publishers) = store.resolve_and_sign(
					&mut resp.publishers,
					&secctx,
					&uifo,
					now,
					perm,
					&path,
                                    );
                                    let a = Resolved {
					resolver,
					publishers,
					timestamp: now,
					permissions: perm.bits(),
					flags,
                                    };
                                    (id, FromRead::Resolved(a))
				}
                            }
			}
                    }
		}
		ToRead::List(path) => {
		    n += 10;
                    if let Some(r) = store.check_referral(&path) {
			(id, FromRead::Referral(r))
                    } else {
			let allowed = pmap
                            .map(|pmap| pmap.allowed(&*path, Permissions::LIST, &*uifo))
                            .unwrap_or(true);
			if allowed {
                            (id, FromRead::List(store.list(&path)))
			} else {
                            (id, FromRead::Denied)
			}
                    }
		}
		ToRead::ListMatching(set) => {
		    n += 1000;
                    let mut referrals = REF_POOL.take();
                    if shard == 0 {
			for glob in set.iter() {
                            store.referrals_in_scope(
				&mut *referrals,
				glob.base(),
				glob.scope(),
                            )
			}
                    }
                    let allowed = pmap
			.map(|pmap| {
                            set.iter().all(|g| {
				pmap.allowed_in_scope(
                                    g.base(),
                                    g.scope(),
                                    Permissions::LIST,
                                    &*uifo,
				)
                            })
			})
			.unwrap_or(true);
                    if !allowed {
			let lm = ListMatching { referrals, matched: PATH_BPOOL.take() };
			(id, FromRead::ListMatching(lm))
                    } else {
			let mut matched = PATH_BPOOL.take();
			matched.push(store.list_matching(&set));
			let lm = ListMatching { referrals, matched };
			(id, FromRead::ListMatching(lm))
                    }
		}
		ToRead::GetChangeNr(path) => {
		    n += 1;
                    let mut referrals = REF_POOL.take();
                    if shard == 0 {
			store.referrals_in_scope(&mut referrals, &*path, &Scope::Subtree);
                    }
                    let allowed = pmap
			.map(|pmap| pmap.allowed(&*path, Permissions::LIST, &*uifo))
			.unwrap_or(true);
                    if !allowed {
			let change_number = Z64(0);
			let cn = GetChangeNr { change_number, referrals, resolver };
			(id, FromRead::GetChangeNr(cn))
                    } else {
			let change_number = store.get_change_nr(&path);
			let cn = GetChangeNr { change_number, referrals, resolver };
			(id, FromRead::GetChangeNr(cn))
                    }
		}
		ToRead::Table(path) => {
		    n += 10;
                    if let Some(r) = store.check_referral(&path) {
			(id, FromRead::Referral(r))
                    } else {
			let allowed = pmap
                            .map(|pmap| pmap.allowed(&*path, Permissions::LIST, &*uifo))
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
            })
	}
        resp
    }

    async fn process_write_batch<'a>(
        store: &mut store::Store,
        secctx: &SecCtxDataReadGuard<'a>,
        mut req: WriteRequest,
    ) -> Pooled<WriteR> {
        let uifo = &*req.uifo;
        let publisher = req.publisher;
        let pmap = secctx.pmap();
        let publish = |s: &mut store::Store,
                       path: Path,
                       default: bool,
                       flags: Option<u32>|
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
                if pmap.map(|p| p.allowed(&*path, perm, uifo)).unwrap_or(true) {
                    s.publish(path, &publisher, default, flags);
                    FromWrite::Published
                } else {
                    FromWrite::Denied
                }
            }
        };
        let mut resp = FROM_WRITE_POOL.take();
	let mut n = 0;
	for (id, m) in req.batch.drain(..) {
	    if n > 5_000 {
		n = 0;
		task::yield_now().await;
	    }
	    resp.push_back(match m {
		ToWrite::Heartbeat => unreachable!(),
		ToWrite::Clear => {
		    n += 1000;
                    store.clear(&publisher);
                    (id, FromWrite::Unpublished)
		}
		ToWrite::Publish(path) => {
		    n += 1;
		    (id, publish(store, path, false, None))
		},
		ToWrite::PublishDefault(path) => {
		    n += 1;
		    (id, publish(store, path, true, None))
		},
		ToWrite::PublishWithFlags(path, flags) => {
		    n += 1;
                    (id, publish(store, path, false, Some(flags)))
		}
		ToWrite::PublishDefaultWithFlags(path, flags) => {
		    n += 1;
                    (id, publish(store, path, true, Some(flags)))
		}
		ToWrite::Unpublish(path) => {
		    n += 5;
                    if !Path::is_absolute(&*path) {
			(id, FromWrite::Error("absolute paths required".into()))
                    } else if let Some(r) = store.check_referral(&path) {
			(id, FromWrite::Referral(r))
                    } else {
			store.unpublish(&publisher, false, path);
			(id, FromWrite::Unpublished)
                    }
		}
		ToWrite::UnpublishDefault(path) => {
		    n += 5;
                    if !Path::is_absolute(&*path) {
			(id, FromWrite::Error("absolute paths required".into()))
                    } else if let Some(r) = store.check_referral(&path) {
			(id, FromWrite::Referral(r))
                    } else {
			store.unpublish(&publisher, true, path);
			(id, FromWrite::Unpublished)
                    }
		}
            })
	}
        resp
    }
}

macro_rules! same {
    ($con:expr, $replies:expr, $res:expr, $msg:expr) => {
        for i in 1..$replies.len() {
            let (_, v) = $replies[i].pop_front().unwrap();
            if &v != $res {
                panic!($msg);
            }
        }
        $con.queue_send($res)?;
    };
}

#[derive(Clone)]
pub(super) struct Store {
    shards: Vec<Shard>,
    shard_mask: usize,
}

impl Store {
    pub(super) fn new(
        parent: Option<Referral>,
        children: BTreeMap<Path, Referral>,
        secctx: SecCtx,
        resolver: SocketAddr,
    ) -> Self {
        let shards = std::cmp::max(1, num_cpus::get().next_power_of_two());
        let shard_mask = shards - 1;
        let shards = (0..shards)
            .into_iter()
            .map(|i| {
                Shard::new(i, parent.clone(), children.clone(), secctx.clone(), resolver)
            })
            .collect();
        Store { shards, shard_mask }
    }

    fn shard(&self, path: &Path) -> usize {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        hasher.finish() as usize & self.shard_mask
    }

    fn read_shard_batch(&self) -> Pooled<Vec<Pooled<ReadB>>> {
        let mut b = READ_SHARD_BATCH.take();
        b.extend((0..self.shards.len()).into_iter().map(|_| TO_READ_POOL.take()));
        b
    }

    fn write_shard_batch(&self) -> Pooled<Vec<Pooled<WriteB>>> {
        let mut b = WRITE_SHARD_BATCH.take();
        b.extend((0..self.shards.len()).into_iter().map(|_| TO_WRITE_POOL.take()));
        b
    }

    pub(super) async fn handle_batch_read(
        &self,
        con: &mut Channel,
        uifo: Arc<UserInfo>,
        mut msgs: impl Iterator<Item = ToRead>,
    ) -> Result<()> {
        let mut finished = false;
        loop {
            let mut n = 0;
            let mut c = 0;
            let mut by_shard = self.read_shard_batch();
            while c < MAX_READ_BATCH {
                match msgs.next() {
                    None => {
                        finished = true;
                        break;
                    }
                    Some(ToRead::Resolve(path)) => {
                        let s = self.shard(&path);
                        by_shard[s].push((n, ToRead::Resolve(path)));
                        c += 1;
                    }
                    Some(ToRead::GetChangeNr(path)) => {
                        for b in by_shard.iter_mut() {
                            b.push((n, ToRead::GetChangeNr(path.clone())));
                        }
                        c += 1;
                    }
                    Some(ToRead::List(path)) => {
                        for b in by_shard.iter_mut() {
                            b.push((n, ToRead::List(path.clone())));
                        }
                        c += 10000;
                    }
                    Some(ToRead::Table(path)) => {
                        for b in by_shard.iter_mut() {
                            b.push((n, ToRead::Table(path.clone())));
                        }
                        c += 10000;
                    }
                    Some(ToRead::ListMatching(set)) => {
                        for b in by_shard.iter_mut() {
                            b.push((n, ToRead::ListMatching(set.clone())));
                        }
                        c += 100000;
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
                    let _ = self.shards[i].read.unbounded_send((req, tx));
                    rx
                }))
                .await
                .into_iter()
                .collect::<result::Result<Vec<ReadResponse>, Canceled>>()?;
            let mut publishers = PUBLISHERS_POOL.take();
            for r in replies.iter_mut() {
                publishers.extend(r.publishers.drain());
            }
            for (_, p) in publishers.drain() {
                con.queue_send(&FromRead::Publisher(p))?;
            }
            let mut replies = {
                let mut r = REPLIES.take();
                r.extend(replies.into_iter().map(|r| r.batch));
                r
            };
            for i in 0..n {
                if replies.len() == 1
                    || !replies
                        .iter()
                        .all(|v| v.front().map(|v| i == v.0).unwrap_or(false))
                {
                    let r = replies
                        .iter_mut()
                        .find_map(|r| {
                            if r.front().map(|v| v.0 == i).unwrap_or(false) {
                                r.pop_front()
                            } else {
                                None
                            }
                        })
                        .unwrap()
                        .1;
                    con.queue_send(&r)?;
                } else {
                    match replies[0].pop_front().unwrap() {
                        (_, FromRead::Publisher(_)) => unreachable!(),
                        (_, FromRead::Resolved(_)) => unreachable!(),
                        (_, m @ FromRead::Referral(_)) => {
                            same!(con, replies, &m, "desynced referral");
                        }
                        (_, m @ FromRead::Denied) => {
                            same!(con, replies, &m, "desynced permissions");
                        }
                        (_, FromRead::Error(e)) => {
                            for i in 1..replies.len() {
                                replies[i].pop_front().unwrap();
                            }
                            con.queue_send(&FromRead::Error(e))?;
                        }
                        (_, FromRead::List(mut paths)) => {
                            let mut hpaths = PATH_HPOOL.take();
                            hpaths.extend(paths.drain(..));
                            for i in 1..replies.len() {
                                if let (_, FromRead::List(mut p)) =
                                    replies[i].pop_front().unwrap()
                                {
                                    hpaths.extend(p.drain(..));
                                } else {
                                    panic!("desynced list")
                                }
                            }
                            let mut paths = PATH_POOL.take();
                            paths.extend(hpaths.drain());
                            con.queue_send(&FromRead::List(paths))?;
                        }
                        (_, FromRead::ListMatching(mut lm)) => {
                            let referrals = lm.referrals;
                            let mut matched = PATH_BPOOL.take();
                            matched.extend(lm.matched.drain(..));
                            for i in 1..replies.len() {
                                if let (_, FromRead::ListMatching(mut lm)) =
                                    replies[i].pop_front().unwrap()
                                {
                                    matched.extend(lm.matched.drain(..));
                                } else {
                                    panic!("desynced listmatching")
                                }
                            }
                            con.queue_send(&FromRead::ListMatching(ListMatching {
                                matched,
                                referrals,
                            }))?;
                        }
                        (_, FromRead::GetChangeNr(cn)) => {
                            let referrals = cn.referrals;
                            let resolver = cn.resolver;
                            let mut change_number = cn.change_number;
                            for i in 1..replies.len() {
                                if let (_, FromRead::GetChangeNr(cn)) =
                                    replies[i].pop_front().unwrap()
                                {
                                    *change_number += *cn.change_number;
                                } else {
                                    panic!("desynced getchangenumber")
                                }
                            }
                            con.queue_send(&FromRead::GetChangeNr(GetChangeNr {
                                referrals,
                                resolver,
                                change_number,
                            }))?;
                        }
                        (_, FromRead::Table(Table { mut rows, mut cols })) => {
                            let mut hrows = PATH_HPOOL.take();
                            let mut hcols = COLS_HPOOL.take();
                            hrows.extend(rows.drain(..));
                            hcols.extend(cols.drain(..));
                            for i in 1..replies.len() {
                                if let (
                                    _,
                                    FromRead::Table(Table { rows: mut rs, cols: mut cs }),
                                ) = replies[i].pop_front().unwrap()
                                {
                                    hrows.extend(rs.drain(..));
                                    for (p, c) in cs.drain(..) {
                                        hcols.entry(p).or_insert(Z64(0)).0 += c.0;
                                    }
                                } else {
                                    panic!("desynced table")
                                }
                            }
                            let mut rows = PATH_POOL.take();
                            let mut cols = COLS_POOL.take();
                            rows.extend(hrows.drain());
                            cols.extend(hcols.drain());
                            con.queue_send(&FromRead::Table(Table { rows, cols }))?;
                        }
                    }
                }
            }
            con.flush().await?;
            if finished {
                break Ok(());
            }
        }
    }

    pub(super) async fn handle_batch_write(
        &self,
        mut con: Option<&mut Channel>,
        uifo: Arc<UserInfo>,
        publisher: Arc<Publisher>,
        mut msgs: impl Iterator<Item = ToWrite>,
    ) -> Result<()> {
	trace!("handling write from {:?}", &publisher);
        let mut finished = false;
        loop {
            let mut n = 0;
            let mut by_shard = self.write_shard_batch();
            for _ in 0..MAX_WRITE_BATCH {
                match msgs.next() {
                    None => {
                        finished = true;
                        break;
                    }
                    Some(ToWrite::Heartbeat) => continue,
                    Some(ToWrite::Clear) => {
                        for b in by_shard.iter_mut() {
                            b.push((n, ToWrite::Clear));
                        }
                    }
                    Some(ToWrite::Publish(path)) => {
                        let s = self.shard(&path);
                        by_shard[s].push((n, ToWrite::Publish(path)));
                    }
                    Some(ToWrite::Unpublish(path)) => {
                        let s = self.shard(&path);
                        by_shard[s].push((n, ToWrite::Unpublish(path)));
                    }
                    Some(ToWrite::UnpublishDefault(path)) => {
                        for b in by_shard.iter_mut() {
                            b.push((n, ToWrite::UnpublishDefault(path.clone())));
                        }
                    }
                    Some(ToWrite::PublishDefault(path)) => {
                        for b in by_shard.iter_mut() {
                            b.push((n, ToWrite::PublishDefault(path.clone())));
                        }
                    }
                    Some(ToWrite::PublishWithFlags(path, flags)) => {
                        let s = self.shard(&path);
                        by_shard[s].push((n, ToWrite::PublishWithFlags(path, flags)));
                    }
                    Some(ToWrite::PublishDefaultWithFlags(path, flags)) => {
                        for b in by_shard.iter_mut() {
                            b.push((
                                n,
                                ToWrite::PublishDefaultWithFlags(path.clone(), flags),
                            ));
                        }
                    }
                }
                n += 1;
            }
	    trace!("handle_write_batch dispatching {} messages to shards", n);
            if by_shard.iter().all(|v| v.is_empty()) {
                assert!(finished);
                break Ok(());
            }
            let mut replies =
                join_all(by_shard.drain(..).enumerate().map(|(i, batch)| {
                    let (tx, rx) = oneshot::channel();
                    let publisher = publisher.clone();
                    let req = WriteRequest { uifo: uifo.clone(), publisher, batch };
                    let _ = self.shards[i].write.unbounded_send((req, tx));
                    rx
                }))
                .await
                .into_iter()
                .collect::<result::Result<Vec<Pooled<WriteR>>, Canceled>>()?;
	    trace!("handle_write_batch {} shards replied", replies.len());
            if let Some(ref mut c) = con {
                for i in 0..n {
                    if replies.len() == 1
                        || !replies
                            .iter()
                            .all(|v| v.front().map(|v| i == v.0).unwrap_or(false))
                    {
                        let r = replies
                            .iter_mut()
                            .find(|v| v.front().map(|v| v.0 == i).unwrap_or(false))
                            .unwrap()
                            .pop_front()
                            .unwrap()
                            .1;
                        c.queue_send(&r)?;
                    } else {
                        match replies[0].pop_front().unwrap() {
                            (_, m @ FromWrite::Denied) => {
                                same!(c, replies, &m, "desynced permissions");
                            }
                            (_, FromWrite::Error(e)) => {
                                for i in 1..replies.len() {
                                    replies[i].pop_front().unwrap();
                                }
                                c.queue_send(&FromWrite::Error(e))?;
                            }
                            (_, m @ FromWrite::Published) => {
                                same!(c, replies, &m, "desynced publish");
                            }
                            (_, m @ FromWrite::Referral(_)) => {
                                same!(c, replies, &m, "desynced referrals");
                            }
                            (_, m @ FromWrite::Unpublished) => {
                                same!(c, replies, &m, "desynced unpublish");
                            }
                        }
                    }
                }
                c.flush().await?;
            }
	    trace!("handle_write_batch processed replies");
            if finished {
		trace!("handle_write_batch finished");
                break Ok(());
            }
        }
    }

    pub(super) async fn handle_clear(
        &self,
        uifo: Arc<UserInfo>,
        publisher: Arc<Publisher>,
    ) -> Result<()> {
        use rand::prelude::*;
	trace!("clearing publisher {:?}", &publisher);
        let mut published_paths = join_all(self.shards.iter().map(|shard| {
            let (tx, rx) = oneshot::channel();
            let _ = shard.internal.unbounded_send((publisher.id, tx));
            rx
        }))
        .await
        .into_iter()
        .flat_map(|s| s.unwrap().into_iter().map(ToWrite::Unpublish))
        .collect::<Vec<_>>();
        published_paths.shuffle(&mut thread_rng());
        let iter = published_paths.into_iter();
        // clear the vast majority of published paths using resources fairly
        self.handle_batch_write(None, uifo.clone(), publisher.clone(), iter).await?;
        // clear out anything left over that was sent to all shards,
        // e.g. default publishers.
        self.handle_batch_write(None, uifo, publisher, iter::once(ToWrite::Clear))
            .await?;
        Ok(())
    }
}
