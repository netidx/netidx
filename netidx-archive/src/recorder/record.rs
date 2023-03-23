use super::BCastMsg;
use crate::logfile::{
    ArchiveWriter, BatchItem, Id, MonotonicTimestamper, RecordTooLarge, BATCH_POOL,
};
use anyhow::Result;
use arcstr::ArcStr;
use futures::{
    channel::{mpsc, oneshot},
    future::{self, Fuse},
    prelude::*,
    select_biased,
};
use fxhash::FxHashMap;
use log::{error, info, warn};
use netidx::{
    config::Config,
    path::Path,
    pool::Pooled,
    protocol::glob::{Glob, GlobSet},
    resolver_client::{ChangeTracker, DesiredAuth, ResolverRead},
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags},
    utils,
};
use std::{
    collections::{BTreeMap, HashMap},
    ops::Bound,
    sync::Arc,
};
use tokio::{sync::broadcast, task, time};

#[derive(Debug)]
struct CTS(BTreeMap<Path, ChangeTracker>);

impl CTS {
    fn new(globs: &Vec<Glob>) -> CTS {
        let mut btm = BTreeMap::new();
        for glob in globs {
            let base = glob.base();
            match btm
                .range::<str, (Bound<&str>, Bound<&str>)>((
                    Bound::Unbounded,
                    Bound::Excluded(base),
                ))
                .next_back()
            {
                Some((p, _)) if Path::is_parent(p, base) => (),
                None | Some(_) => {
                    let base = Path::from(ArcStr::from(base));
                    let ct = ChangeTracker::new(base.clone());
                    btm.insert(base, ct);
                }
            }
        }
        CTS(btm)
    }

    async fn changed(&mut self, r: &ResolverRead) -> Result<bool> {
        let res =
            future::join_all(self.0.iter_mut().map(|(_, ct)| r.check_changed(ct))).await;
        for r in res {
            if r? {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

async fn maybe_interval(poll: &mut Option<time::Interval>) {
    match poll {
        None => future::pending().await,
        Some(poll) => {
            poll.tick().await;
        }
    }
}

type Lst = Option<Pooled<Vec<Pooled<Vec<Path>>>>>;

async fn list_task(
    mut rx: mpsc::UnboundedReceiver<oneshot::Sender<Lst>>,
    resolver: ResolverRead,
    spec: Vec<Glob>,
) -> Result<()> {
    let mut cts = CTS::new(&spec);
    let spec = GlobSet::new(true, spec)?;
    while let Some(reply) = rx.next().await {
        match cts.changed(&resolver).await {
            Ok(true) => match resolver.list_matching(&spec).await {
                Ok(lst) => {
                    let _ = reply.send(Some(lst));
                }
                Err(e) => {
                    warn!("list_task: list_matching failed {}, will retry", e);
                    let _ = reply.send(None);
                }
            },
            Ok(false) => {
                let _ = reply.send(None);
            }
            Err(e) => {
                warn!("list_task: check_changed failed {}, will retry", e);
                let _ = reply.send(None);
            }
        }
    }
    Ok(())
}

fn start_list_task(
    rx: mpsc::UnboundedReceiver<oneshot::Sender<Lst>>,
    resolver: ResolverRead,
    spec: Vec<Glob>,
) {
    task::spawn(async move {
        let r = list_task(rx, resolver, spec).await;
        match r {
            Err(e) => error!("list task exited with error {}", e),
            Ok(()) => info!("list task exited"),
        }
    });
}

async fn wait_list(pending: &mut Option<Fuse<oneshot::Receiver<Lst>>>) -> Lst {
    match pending {
        None => future::pending().await,
        Some(r) => match r.await {
            Ok(r) => r,
            Err(_) => None,
        },
    }
}

pub(super) async fn run(
    bcast: broadcast::Sender<BCastMsg>,
    mut archive: ArchiveWriter,
    resolver: Config,
    desired_auth: DesiredAuth,
    poll_interval: Option<time::Duration>,
    image_frequency: Option<usize>,
    flush_frequency: Option<usize>,
    flush_interval: Option<time::Duration>,
    spec: Vec<Glob>,
) -> Result<()> {
    let (tx_batch, rx_batch) = mpsc::channel(10);
    let (tx_list, rx_list) = mpsc::unbounded();
    let mut rx_batch = utils::Batched::new(rx_batch.fuse(), 10);
    let mut by_subid: FxHashMap<SubId, Id> = HashMap::default();
    let mut image: FxHashMap<SubId, Event> = HashMap::default();
    let mut subscribed: HashMap<Path, Dval> = HashMap::new();
    let subscriber = Subscriber::new(resolver, desired_auth)?;
    let flush_frequency = flush_frequency.map(|f| archive.block_size() * f);
    let mut bcast_rx = bcast.subscribe();
    let mut poll = poll_interval.map(time::interval);
    let mut flush = flush_interval.map(time::interval);
    let mut to_add = Vec::new();
    let mut timest = MonotonicTimestamper::new();
    let mut last_image = archive.len();
    let mut last_flush = archive.len();
    let mut pending_list: Option<Fuse<oneshot::Receiver<Lst>>> = None;
    let mut pending_batches: Vec<Pooled<Vec<(SubId, Event)>>> = Vec::new();
    start_list_task(rx_list, subscriber.resolver(), spec);
    loop {
        select_biased! {
            m = bcast_rx.recv().fuse() => match m {
                Err(_) | Ok(BCastMsg::Batch(_, _)) => (),
                Ok(BCastMsg::Stop) => break,
            },
            _ = maybe_interval(&mut poll).fuse() => {
                if pending_list.is_none() {
                    let (tx, rx) = oneshot::channel();
                    let _ = tx_list.unbounded_send(tx);
                    pending_list = Some(rx.fuse());
                }
            },
            _ = maybe_interval(&mut flush).fuse() => {
                if archive.len() > last_flush {
                    task::block_in_place(|| -> Result<()> {
                        archive.flush()?;
                        Ok(last_flush = archive.len())
                    })?;
                }
            }
            r = wait_list(&mut pending_list).fuse() => {
                pending_list = None;
                if let Some(mut batches) = r {
                    for mut batch in batches.drain(..) {
                        for path in batch.drain(..) {
                            if !subscribed.contains_key(&path) {
                                let dv = subscriber.subscribe(path.clone());
                                let id = dv.id();
                                dv.updates(
                                    UpdatesFlags::BEGIN_WITH_LAST
                                        | UpdatesFlags::STOP_COLLECTING_LAST,
                                    tx_batch.clone()
                                );
                                subscribed.insert(path.clone(), dv);
                                to_add.push((path, id));
                            }
                        }
                    }
                    task::block_in_place(|| {
                        let i = to_add.iter().map(|(ref p, _)| p);
                        archive.add_paths(i)
                    })?;
                    for (path, subid) in to_add.drain(..) {
                        if !by_subid.contains_key(&subid) {
                            let id = archive.id_for_path(&path).unwrap();
                            by_subid.insert(subid, id);
                        }
                    }
                }
            },
            batch = rx_batch.next() => match batch {
                None => break,
                Some(utils::BatchItem::InBatch(batch)) => {
                    pending_batches.push(batch);
                },
                Some(utils::BatchItem::EndBatch) => {
                    let mut overflow = Vec::new();
                    let mut tbatch = BATCH_POOL.take();
                    task::block_in_place(|| -> Result<()> {
                        for mut batch in pending_batches.drain(..) {
                            for (subid, ev) in batch.drain(..) {
                                if image_frequency.is_some() {
                                    image.insert(subid, ev.clone());
                                }
                                tbatch.push(BatchItem(by_subid[&subid], ev));
                            }
                        }
                        loop { // handle batches >4 GiB
                            let ts = timest.timestamp();
                            match archive.add_batch(false, ts, &tbatch) {
                                Err(e) if e.is::<RecordTooLarge>() => {
                                    let at = tbatch.len() >> 1;
                                    overflow.push(tbatch.split_off(at));
                                }
                                Err(e) => bail!(e),
                                Ok(()) => {
                                    let m = BCastMsg::Batch(ts, Arc::new(tbatch));
                                    let _ = bcast.send(m);
                                    match overflow.pop() {
                                        None => break,
                                        Some(b) => { tbatch = Pooled::orphan(b); }
                                    }
                                }
                            }
                        }
                        match image_frequency {
                            None => (),
                            Some(freq) if archive.len() - last_image < freq => (),
                            Some(_) => {
                                let mut b = BATCH_POOL.take();
                                let ts = timest.timestamp();
                                for (id, ev) in image.iter() {
                                    b.push(BatchItem(by_subid[id], ev.clone()));
                                }
                                archive.add_batch(true, ts, &b)?;
                                last_image = archive.len();
                            }
                        }
                        match flush_frequency {
                            None => (),
                            Some(freq) if archive.len() - last_flush < freq => (),
                            Some(_) => {
                                archive.flush()?;
                                last_flush = archive.len();
                            }
                        }
                        Ok(())
                    })?;
                }
            }
        }
    }
    Ok(())
}
