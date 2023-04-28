use super::{ArchiveCmds, BCastMsg, Config, RecordConfig, RotateDirective, ShardId};
use crate::logfile::{ArchiveWriter, BatchItem, Id, BATCH_POOL};
use anyhow::Result;
use arcstr::ArcStr;
use chrono::prelude::*;
use futures::{
    channel::{mpsc, oneshot},
    future::{self, Fuse},
    prelude::*,
    select_biased,
};
use fxhash::{FxHashMap, FxHashSet};
use log::{debug, error, info, warn};
use netidx::{
    path::Path,
    pool::Pooled,
    protocol::glob::{Glob, GlobSet},
    resolver_client::{ChangeTracker, ResolverRead},
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags},
    utils::{self, Batched},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::Bound,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::broadcast,
    task,
    time::{self, Instant},
};

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
    interval: Duration,
    mut rx: mpsc::UnboundedReceiver<oneshot::Sender<Lst>>,
    resolver: ResolverRead,
    spec: Vec<Glob>,
) -> Result<()> {
    use rand::{thread_rng, Rng};
    let mut cts = CTS::new(&spec);
    let spec = GlobSet::new(true, spec)?;
    let max_jitter = interval.as_secs_f64() * 0.1;
    while let Some(reply) = rx.next().await {
        let wait = thread_rng().gen_range(0. ..max_jitter);
        time::sleep(Duration::from_secs_f64(wait)).await;
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
    poll_interval: Duration,
    rx: mpsc::UnboundedReceiver<oneshot::Sender<Lst>>,
    resolver: ResolverRead,
    spec: Vec<Glob>,
) {
    task::spawn(async move {
        let r = list_task(poll_interval, rx, resolver, spec).await;
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

fn rotate_log_file(
    archive: ArchiveWriter,
    path: &PathBuf,
    shard_name: &ArcStr,
    cmds: &Option<ArchiveCmds>,
    now: DateTime<Utc>,
) -> Result<ArchiveWriter> {
    use std::{fs, iter};
    info!("rotating log file {}", now);
    drop(archive); // ensure the current file is closed
    let current_name = path.join(&**shard_name).join("current");
    let new_name = path.join(&**shard_name).join(now.to_rfc3339());
    fs::rename(&current_name, &new_name)?;
    debug!("would run put, cmd config {:?}", cmds);
    if let Some(cmds) = cmds {
        use std::process::Command;
        let now = now.to_rfc3339();
        info!("running put {:?}", &cmds.put);
        let args = cmds.put.1.iter().cloned().map(|arg| {
            if &arg == "{shard}" {
                String::from(&**shard_name)
            } else {
                arg
            }
        });
        let out =
            Command::new(&cmds.put.0).args(args.chain(iter::once(now.clone()))).output();
        match out {
            Err(e) => warn!("archive put failed for {}, {}", now, e),
            Ok(o) if !o.status.success() => {
                warn!("archive put failed for {}, {:?}", now, o)
            }
            Ok(out) => {
                if out.stdout.len() > 0 {
                    warn!("archive put stdout {}", String::from_utf8_lossy(&out.stdout));
                }
                if out.stderr.len() > 0 {
                    warn!("archive put stderr {}", String::from_utf8_lossy(&out.stderr));
                }
                info!("put completed successfully");
            }
        }
    }
    ArchiveWriter::open(current_name)
}

fn write_pathmap(
    pathindex: &mut ArchiveWriter,
    to_add: &mut Vec<(Path, SubId)>,
    by_subid: &mut FxHashMap<SubId, Id>,
) -> Result<()> {
    task::block_in_place(|| {
        let i = to_add.iter().map(|(ref p, _)| p);
        pathindex.add_paths(i)
    })?;
    for (path, subid) in to_add.drain(..) {
        if !by_subid.contains_key(&subid) {
            let id = pathindex.id_for_path(&path).unwrap();
            by_subid.insert(subid, id);
        }
    }
    pathindex.flush()?;
    Ok(())
}

fn write_image(
    archive: &mut ArchiveWriter,
    by_subid: &FxHashMap<SubId, Id>,
    image: &FxHashMap<SubId, Event>,
    ts: DateTime<Utc>,
) -> Result<()> {
    let mut b = BATCH_POOL.take();
    for (id, ev) in image.iter() {
        if let Some(id) = by_subid.get(id) {
            b.push(BatchItem(*id, ev.clone()));
        }
    }
    archive.add_batch(true, ts, &b)?;
    Ok(())
}

pub(super) async fn run(
    bcast: broadcast::Sender<BCastMsg>,
    mut bcast_rx: broadcast::Receiver<BCastMsg>,
    mut pathindex: ArchiveWriter,
    subscriber: Subscriber,
    config: Arc<Config>,
    record_config: Arc<RecordConfig>,
    shard_id: ShardId,
    shard_name: ArcStr,
) -> Result<()> {
    let (tx_batch, rx_batch) = mpsc::channel(10000);
    let mut rx_batch = Batched::new(rx_batch, 10000);
    let (tx_list, rx_list) = mpsc::unbounded();
    let mut by_subid: FxHashMap<SubId, Id> = HashMap::default();
    let mut image: FxHashMap<SubId, Event> = HashMap::default();
    let mut subscribed: HashMap<Path, Dval> = HashMap::new();
    let mut archive = task::block_in_place(|| {
        ArchiveWriter::open(config.archive_directory.join(&*shard_name).join("current"))
    })?;
    let _ = bcast.send(BCastMsg::NewCurrent(shard_id, archive.reader()?));
    let flush_frequency = record_config.flush_frequency.map(|f| archive.block_size() * f);
    let mut poll = record_config.poll_interval.map(time::interval);
    let mut flush =
        record_config.flush_interval.map(|d| time::interval_at(Instant::now() + d, d));
    let mut rotate = match record_config.rotate_interval {
        RotateDirective::Never => None,
        RotateDirective::Interval(d) => Some(time::interval_at(Instant::now() + d, d)),
        RotateDirective::Size(_) => {
            let d = Duration::from_secs(1);
            Some(time::interval_at(Instant::now() + d, d))
        }
    };
    let mut to_add: Vec<(Path, SubId)> = Vec::new();
    let mut all_paths: FxHashSet<Path> = HashSet::default();
    let mut remove_paths: Vec<Path> = vec![];
    let mut last_image = archive.len();
    let mut last_flush = archive.len();
    let mut pending_list: Option<Fuse<oneshot::Receiver<Lst>>> = None;
    let mut batches = 0;
    let mut last_batches = Instant::now();
    let mut queued = Vec::new();
    if let Some(interval) = record_config.poll_interval {
        start_list_task(
            interval,
            rx_list,
            subscriber.resolver(),
            record_config.spec.clone(),
        );
    }
    loop {
        select_biased! {
            m = bcast_rx.recv().fuse() => match m {
                Ok(BCastMsg::Stop) => break,
                Err(_)
                    | Ok(BCastMsg::Batch(_, _, _))
                    | Ok(BCastMsg::LogRotated(_, _))
                    | Ok(BCastMsg::NewCurrent(_, _)) => (),
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
                let now = Instant::now();
                let elapsed = now - last_batches;
                info!("recorded: {} batches/s", batches as f32 / elapsed.as_secs_f32());
                last_batches = now;
                batches = 0;
            },
            _ = maybe_interval(&mut rotate).fuse() => {
                let rotate = match record_config.rotate_interval {
                    RotateDirective::Never => false,
                    RotateDirective::Size(sz) => archive.len() >= sz,
                    RotateDirective::Interval(_) => true,
                };
                if rotate {
                    let now = Utc::now();
                    archive = task::block_in_place(|| {
                        rotate_log_file(
                            archive,
                            &config.archive_directory,
                            &shard_name,
                            &config.archive_cmds,
                            now
                        )
                    })?;
                    last_image = 0;
                    last_flush = 0;
                    task::block_in_place(|| write_image(&mut archive, &by_subid, &image, now))?;
                    let _ = bcast.send(BCastMsg::LogRotated(shard_id, now));
                    let _ = bcast.send(BCastMsg::NewCurrent(shard_id, archive.reader()?));
                }
            },
            r = wait_list(&mut pending_list).fuse() => {
                pending_list = None;
                if let Some(mut batches) = r {
                    for mut batch in batches.drain(..) {
                        for path in batch.drain(..) {
                            all_paths.insert(path.clone());
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
                    for path in subscribed.keys() {
                        if !all_paths.contains(path) {
                            remove_paths.push(path.clone());
                        }
                    }
                    all_paths.clear();
                    for path in remove_paths.drain(..) {
                        if let Some(dv) = subscribed.remove(&path) {
                            image.remove(&dv.id());
                            by_subid.remove(&dv.id());
                        }
                    }
                    write_pathmap(&mut pathindex, &mut to_add, &mut by_subid)?
                }
            },
            batch = rx_batch.next() => match batch {
                None => break,
                Some(utils::BatchItem::InBatch(batch)) => queued.push(batch),
                Some(utils::BatchItem::EndBatch) => {
                    batches += 1;
                    let now = Utc::now();
                    let mut tbatch = BATCH_POOL.take();
                    task::block_in_place(|| -> Result<()> {
                        for mut batch in queued.drain(..) {
                            for (subid, ev) in batch.drain(..) {
                                if record_config.image_frequency.is_some() {
                                    image.insert(subid, ev.clone());
                                }
                                if let Some(id) = by_subid.get(&subid) {
                                    tbatch.push(BatchItem(*id, ev));
                                }
                            }
                        }
                        archive.add_batch(false, now, &tbatch)?;
                        let _ = bcast.send(BCastMsg::Batch(shard_id, now, Arc::new(tbatch)));
                        match record_config.image_frequency {
                            None => (),
                            Some(freq) if archive.len() - last_image < freq => (),
                            Some(_) => {
                                write_image(&mut archive, &by_subid, &image, Utc::now())?;
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
