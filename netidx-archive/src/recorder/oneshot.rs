use super::{
    logfile_collection::LogfileCollection,
    publish::{parse_bound, parse_filter},
};
use crate::{
    logfile::{ArchiveReader, BatchItem, Id, Seek, CURSOR_BATCH_POOL, IMG_POOL},
    recorder::{
        publish::{END_DOC, FILTER_DOC, START_DOC},
        BCastMsg, Config, PublishConfig,
    },
    recorder_client::{OneshotReply, PATHMAPS},
};
use anyhow::Result;
use arcstr::ArcStr;
use chrono::prelude::*;
use futures::{channel::mpsc, future, prelude::*, select_biased};
use fxhash::{FxHashMap, FxHashSet};
use log::{debug, error};
use netidx::{
    chars::Chars,
    pack::Pack,
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
use tokio::{sync::broadcast, task::JoinSet};

#[derive(Debug, Clone, Pack)]
struct OneshotConfig {
    start: Bound<DateTime<Utc>>,
    end: Bound<DateTime<Utc>>,
    filter: GlobSet,
}

atomic_id!(Oid);

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
        filter: Vec<Chars>,
    ) -> Option<(Self, RpcReply)> {
        let start = match parse_bound(start) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid start {}", e)),
        };
        let end = match parse_bound(end) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("invalid end {}", e)),
        };
        let filter = match parse_filter(filter) {
            Ok(s) => s,
            Err(e) => rpc_err!(req.reply, format!("could not parse filter {}", e)),
        };
        Some((Self { start, end, filter }, req.reply))
    }
}

lazy_static! {
    static ref MATCHING_IDS: Pool<FxHashSet<Id>> = Pool::new(1000, 1000000);
}

async fn do_oneshot(
    head: Option<ArchiveReader>,
    pathindex: ArchiveReader,
    config: Arc<Config>,
    limit: usize,
    args: OneshotConfig,
) -> Result<OneshotReply> {
    pathindex.check_remap_rescan()?;
    let mut matching_ids = MATCHING_IDS.take();
    matching_ids.extend(pathindex.index().iter_pathmap().filter_map(|(id, path)| {
        if args.filter.is_match(path) {
            Some(*id)
        } else {
            None
        }
    }));
    if matching_ids.is_empty() {
        return Ok(OneshotReply {
            deltas: CURSOR_BATCH_POOL.take(),
            image: IMG_POOL.take(),
            pathmap: PATHMAPS.take(),
        });
    }
    debug!("opening logfile collection");
    let mut log = LogfileCollection::new(config, head, args.start, args.end).await?;
    debug!("seeking to the beginning");
    log.seek(Seek::Beginning)?;
    debug!("reimaging");
    let mut idx = log.reimage()?;
    let mut path_by_id = PATHMAPS.take();
    let index = pathindex.index();
    path_by_id.extend(idx.keys().filter_map(|id| {
        if matching_ids.contains(id) {
            index.path_for_id(id).map(|path| (*id, path.clone()))
        } else {
            None
        }
    }));
    idx.retain(|id, _| path_by_id.contains_key(id));
    let mut data = OneshotReply {
        pathmap: path_by_id,
        image: idx,
        deltas: CURSOR_BATCH_POOL.take(),
    };
    loop {
        debug!("reading archive batch");
        let mut batches = log.read_deltas(100)??;
        if batches.is_empty() {
            break Ok(data);
        } else {
            batches.retain_mut(|(_, batch)| {
                batch.retain(|BatchItem(id, _)| matching_ids.contains(id));
                !batch.is_empty()
            });
            data.deltas.extend(batches.drain(..));
            if data.encoded_len() > limit {
                bail!("data is too large");
            }
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
                Err(e) => self.reply.send(Value::Error(Chars::from(e.to_string()))),
                Ok(mut replies) => {
                    let mut reply = replies.pop().unwrap();
                    for mut r in replies.drain(..) {
                        let OneshotReply { pathmap, image, deltas } = &mut reply;
                        pathmap.extend(r.pathmap.drain());
                        image.extend(r.image.drain());
                        deltas.extend(r.deltas.drain(..));
                    }
                    reply.deltas.make_contiguous().sort_by_key(|(ts, _)| *ts);
                    self.reply.send(Value::Bytes(pack(&reply)?.freeze()));
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

pub(super) async fn run(
    mut bcast_rx: broadcast::Receiver<BCastMsg>,
    pathindex: ArchiveReader,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    publisher: Publisher,
    subscriber: Subscriber,
) -> Result<()> {
    let shards = publish_config.shards.unwrap_or(0);
    let mut cluster: Cluster<ClusterCmd> = Cluster::new(
        &publisher,
        subscriber,
        publish_config.base.append(&publish_config.cluster).append("oneshot"),
        shards,
    )
    .await?;
    let our_path = cluster.path();
    let (control_tx, mut control_rx) = mpsc::channel(3);
    let mut archive: Option<ArchiveReader> = None;
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
        filter: Vec<Chars> = vec![Chars::from("/**")]; FILTER_DOC
    )?;
    loop {
        select_biased! {
            r = wait_complete(&mut pending).fuse() => {
                match r {
                    Err(e) => error!("could not join pending oneshot {}", e),
                    Ok((oid, path, res)) => match we_initiated.entry(oid) {
                        Entry::Vacant(_) => cluster.send_cmd_to_one(&path, &ClusterCmd::Reply(oid, res)),
                        Entry::Occupied(e) => handle_reply_for_us(e, res),
                    }
                }
            },
            m = bcast_rx.recv().fuse() => match m {
                Err(_) => (),
                Ok(m) => match m {
                    BCastMsg::Batch(_, _) | BCastMsg::LogRotated(_) => (),
                    BCastMsg::NewCurrent(rdr) => archive = Some(rdr),
                    BCastMsg::Stop => break Ok(())
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
                            let archive_ = archive.clone();
                            let pathindex_ = pathindex.clone();
                            let config_ = config.clone();
                            let limit = publish_config.oneshot_data_limit;
                            pending.spawn(async move {
                                (id, path, do_oneshot(archive_, pathindex_, config_, limit, args).await)
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
                    reply.send(Value::Error(Chars::from("busy")));
                } else {
                    let id = Oid::new();
                    let path = our_path.clone();
                    we_initiated.insert(id, PendingOneshot::new(reply, shards + 1));
                    cluster.send_cmd(&ClusterCmd::NewOneshot(id, path.clone(), args.clone()));
                    let archive_ = archive.clone();
                    let pathindex_ = pathindex.clone();
                    let config_ = config.clone();
                    let limit = publish_config.oneshot_data_limit;
                    pending.spawn(async move {
                        (id, path, do_oneshot(archive_, pathindex_, config_, limit, args).await)
                    });
                }
            }
        }
    }
}
