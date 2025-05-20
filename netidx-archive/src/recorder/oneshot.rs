use super::{
    logfile_collection::index::ArchiveIndex,
    logfile_collection::reader::ArchiveCollectionReader,
    publish::controls::{parse_bound, parse_filter},
    Shards,
};
use crate::{
    config::{Config, PublishConfig},
    logfile::{ArchiveReader, BatchItem, Id, Seek, CURSOR_BATCH_POOL, IMG_POOL},
    recorder::publish::controls::{END_DOC, FILTER_DOC, START_DOC},
    recorder_client::{OneshotReply, OneshotReplyShard, PATHMAPS, SHARDS},
};
use anyhow::Result;
use arcstr::{ArcStr, literal};
use chrono::prelude::*;
use futures::{channel::mpsc, future, prelude::*, select_biased};
use fxhash::{FxHashMap, FxHashSet};
use log::{debug, error};
use netidx::{
    path::Path,
    pool::Pool,
    publisher::{Publisher, Value},
    resolver_client::GlobSet,
    subscriber::Subscriber,
};
use netidx_core::{atomic_id, utils::pack};
use netidx_derive::Pack;
use netidx_protocols::{
    cluster::Cluster,
    define_rpc,
    rpc::server::{ArgSpec, Proc, RpcCall, RpcReply},
    rpc_err,
};
use serde_derive::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{
    collections::{
        hash_map::{Entry, OccupiedEntry},
        HashMap,
    },
    ops::Bound,
    sync::Arc,
};
use tokio::task::JoinSet;

#[derive(Debug, Clone, Pack)]
pub(crate) struct OneshotConfig {
    pub(crate) start: Bound<DateTime<Utc>>,
    pub(crate) end: Bound<DateTime<Utc>>,
    pub(crate) filter: GlobSet,
}

atomic_id!(Oid);

lazy_static! {
    pub(crate) static ref FILTER: Pool<FxHashSet<Id>> = Pool::new(100, 10000);
}

#[derive(Debug, Pack)]
enum ClusterCmd {
    NewOneshot(Oid, Path, OneshotConfig),
    Reply(Oid, Result<OneshotReply>),
}

impl OneshotConfig {
    fn new(
        mut req: RpcCall,
        start: Value,
        end: Value,
        filter: Vec<ArcStr>,
    ) -> Option<(Self, RpcReply)> {
        let start = match parse_bound(start) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid start {}", e)),
        };
        let end = match parse_bound(end) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid end {}", e)),
        };
        match (start, end) {
            (
                Bound::Excluded(st) | Bound::Included(st),
                Bound::Excluded(en) | Bound::Included(en),
            ) if en <= st => rpc_err!(
                req.reply,
                format!("start time {} is after the end time {}", st, en)
            ),
            (
                Bound::Excluded(_) | Bound::Included(_) | Bound::Unbounded,
                Bound::Excluded(_) | Bound::Included(_) | Bound::Unbounded,
            ) => (),
        }
        let filter = match parse_filter(filter) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("could not parse filter {}", e)),
        };
        Some((Self { start, end, filter }, req.reply))
    }
}

async fn do_oneshot(
    shard: ArcStr,
    head: Option<ArchiveReader>,
    index: ArchiveIndex,
    pathindex: ArchiveReader,
    config: Arc<Config>,
    limit: usize,
    args: OneshotConfig,
) -> Result<OneshotReplyShard> {
    debug!("logfile index has {} files", index.len());
    pathindex.check_remap_rescan(false)?;
    let mut filterset = FILTER.take();
    let mut pathmap = PATHMAPS.take();
    pathmap.extend(pathindex.index().iter_pathmap().filter_map(|(id, path)| {
        if args.filter.is_match(path) {
            filterset.insert(*id);
            Some((*id, path.clone()))
        } else {
            None
        }
    }));
    if pathmap.is_empty() {
        return Ok(OneshotReplyShard {
            deltas: CURSOR_BATCH_POOL.take(),
            image: IMG_POOL.take(),
            pathmap,
        });
    }
    debug!("opening logfile collection");
    let mut log =
        ArchiveCollectionReader::new(index, config, shard, head, args.start, args.end);
    debug!("seeking to beginning");
    log.seek(Seek::Beginning)?;
    debug!("reimaging");
    let mut idx = log.reimage(Some(&*filterset))?;
    idx.retain(|id, _| pathmap.contains_key(id));
    let mut data =
        OneshotReplyShard { pathmap, image: idx, deltas: CURSOR_BATCH_POOL.take() };
    let mut total = 0;
    loop {
        debug!("reading archive batch");
        let (len, mut batches) = log.read_deltas(Some(&*filterset), 1000)?;
        debug!("read archive batch len={len}");
        if batches.is_empty() {
            break Ok(data);
        } else {
            batches.retain_mut(|(_, batch)| {
                batch.retain(|BatchItem(id, _)| data.pathmap.contains_key(id));
                !batch.is_empty()
            });
            data.deltas.extend(batches.drain(..));
        }
        total += len;
        if total > limit {
            bail!("too large")
        }
    }
}

async fn wait_complete(
    set: &mut JoinSet<(Oid, Path, Result<OneshotReply>)>,
) -> Result<(Oid, Path, Result<OneshotReply>)> {
    if set.is_empty() {
        future::pending().await
    } else {
        Ok(set.join_next().await.unwrap()?)
    }
}

struct PendingOneshot {
    reply: RpcReply,
    replies: SmallVec<[Result<OneshotReply>; 64]>,
    expected: usize,
}

impl PendingOneshot {
    fn new(reply: RpcReply, expected: usize) -> Self {
        assert!(expected > 0);
        Self { reply, replies: SmallVec::new(), expected }
    }

    fn register_reply(&mut self, r: Result<OneshotReply>) -> Result<bool> {
        self.replies.push(r);
        if self.replies.len() < self.expected {
            Ok(false)
        } else {
            let replies = self.replies.drain(..).collect::<Result<SmallVec<[_; 64]>>>();
            match replies {
                Err(e) => self.reply.send(Value::Error(e.to_string().into())),
                Ok(mut replies) => {
                    let mut reply = replies.pop().unwrap();
                    for mut r in replies.drain(..) {
                        reply.0.extend(r.0.drain(..));
                    }
                    self.reply.send(Value::Bytes(pack(&reply)?.freeze().into()));
                }
            }
            Ok(true)
        }
    }
}

fn handle_reply_for_us(
    mut e: OccupiedEntry<Oid, PendingOneshot>,
    res: Result<OneshotReply>,
) {
    match e.get_mut().register_reply(res) {
        Ok(false) => (),
        Ok(true) => {
            e.remove();
        }
        Err(err) => {
            error!("failed to send result {}", err);
            e.remove();
        }
    }
}

async fn start_oneshot(
    shards: Arc<Shards>,
    config: Arc<Config>,
    limit: usize,
    args: OneshotConfig,
) -> Result<OneshotReply> {
    let mut set = JoinSet::new();
    for (id, pathindex) in shards.pathindexes.iter() {
        if let Some(spec) = shards.spec.get(id) {
            if args.filter.disjoint(spec) {
                debug!("skipping shard {:?} because filter is disjoint", id);
                continue;
            }
        }
        debug!("starting oneshot subtask for shard {:?}", id);
        let shard = shards.by_id[id].clone();
        let head = shards.heads.read().get(id).cloned();
        let index = shards.indexes.read()[id].clone();
        let pathindex = pathindex.clone();
        set.spawn(do_oneshot(
            shard,
            head,
            index,
            pathindex,
            config.clone(),
            limit,
            args.clone(),
        ));
    }
    let mut res = OneshotReply(SHARDS.take());
    loop {
        match set.join_next().await {
            None => return Ok(res),
            Some(r) => res.0.push(r??),
        }
    }
}

pub(super) async fn run(
    shards: Arc<Shards>,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    publisher: Publisher,
    subscriber: Subscriber,
    init: futures::channel::oneshot::Sender<()>,
) -> Result<()> {
    let cluster_shards = publish_config.cluster_shards.unwrap_or(0);
    let mut cluster: Cluster<ClusterCmd> = Cluster::new(
        &publisher,
        subscriber,
        publish_config.base.append(&publish_config.cluster).append("oneshot"),
        cluster_shards,
    )
    .await?;
    let our_path = cluster.path();
    let (control_tx, mut control_rx) = mpsc::channel(3);
    let mut pending: JoinSet<(Oid, Path, Result<OneshotReply>)> = JoinSet::new();
    let mut we_initiated: FxHashMap<Oid, PendingOneshot> = HashMap::default();
    let _proc = define_rpc!(
        &publisher,
        publish_config.base.append("oneshot"),
        "read data in one shot",
        OneshotConfig::new,
        Some(control_tx),
        start: Value = "Unbounded"; START_DOC,
        end: Value = "Unbounded"; END_DOC,
        filter: Vec<ArcStr> = vec![literal!("/**")]; FILTER_DOC
    )?;
    publisher.flushed().await;
    let _ = init.send(());
    loop {
        select_biased! {
            r = wait_complete(&mut pending).fuse() => {
                match r {
                    Err(e) => error!("could not join pending oneshot {}", e),
                    Ok((oid, path, res)) => match we_initiated.entry(oid) {
                        Entry::Occupied(e) => handle_reply_for_us(e, res),
                        Entry::Vacant(_) => if cluster_shards > 0 {
                            cluster.send_cmd_to_one(
                                &path,
                                &ClusterCmd::Reply(oid, res)
                            )
                        },
                    }
                }
            },
            cmds = cluster.wait_cmds().fuse() => {
                let cmds = match cmds {
                    Ok(cmds) => cmds,
                    Err(e) => {
                        error!("failed to read from the cluster {}", e);
                        continue
                    }
                };
                for cmd in cmds {
                    match cmd {
                        ClusterCmd::Reply(oid, res) => match we_initiated.entry(oid) {
                            Entry::Vacant(_) => error!("reply we didn't initiate"),
                            Entry::Occupied(e) => handle_reply_for_us(e, res),
                        }
                        ClusterCmd::NewOneshot(id, path, args) => {
                            let shards = shards.clone();
                            let config = config.clone();
                            let limit = publish_config.oneshot_data_limit;
                            pending.spawn(async move {
                                (id, path, start_oneshot(shards, config, limit, args).await)
                            });
                        }
                    }
                }
            },
            (args, mut reply) = control_rx.select_next_some() => {
                if pending.len() > publish_config.max_sessions {
                    // your call is important to us. please stay on
                    // the line until the next available representive
                    // is ready to assist you. Goodbye.
                    reply.send(Value::Error(literal!("busy")));
                } else {
                    let id = Oid::new();
                    let path = our_path.clone();
                    we_initiated.insert(id, PendingOneshot::new(reply, cluster_shards + 1));
                    if cluster_shards > 0 {
                        cluster.send_cmd(&ClusterCmd::NewOneshot(id, path.clone(), args.clone()));
                    }
                    let shards = shards.clone();
                    let config = config.clone();
                    let limit = publish_config.oneshot_data_limit;
                    pending.spawn(async move {
                        (id, path, start_oneshot(shards, config, limit, args).await)
                    });
                }
            }
        }
    }
}
