use super::{
    logfile_collection::{Image, LogfileCollection},
    publish::{parse_bound, parse_filter},
};
use crate::{
    logfile::{ArchiveReader, BatchItem, Id, Seek},
    recorder::{
        publish::{END_DOC, FILTER_DOC, START_DOC},
        BCastMsg, Config, PublishConfig,
    },
};
use anyhow::{Error, Result};
use arcstr::ArcStr;
use chrono::prelude::*;
use futures::{channel::mpsc, future, prelude::*, select_biased, stream::FuturesOrdered};
use fxhash::{FxHashMap, FxHashSet};
use log::{error, info, warn};
use netidx::{
    chars::Chars,
    path::Path,
    pool::Pooled,
    protocol::value::FromValue,
    publisher::{
        self, ClId, PublishFlags, Publisher, PublisherBuilder, UpdateBatch, Val, Value,
        WriteRequest,
    },
    resolver_client::{Glob, GlobSet},
    subscriber::{Event, Subscriber},
};
use netidx_protocols::{
    define_rpc,
    pack_channel::server::Singleton,
    rpc::server::{ArgSpec, Proc, RpcCall, RpcReply},
    rpc_err,
};
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem,
    ops::Bound,
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::broadcast, task, time};
use uuid::Uuid;

struct OneshotConfig {
    start: Bound<DateTime<Utc>>,
    end: Bound<DateTime<Utc>>,
    filter: GlobSet,
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

async fn do_oneshot(
    head: Option<ArchiveReader>,
    config: Arc<Config>,
    args: OneshotConfig,
    reply: &mut RpcReply,
) -> Result<()> {
    let mut log = LogfileCollection::new(config, head, args.start, args.end).await?;
    let mut total = 0;
    log.seek(Seek::Beginning)?;
    let img = log.reimage().ok_or_else(|| anyhow!("no data source"))??;
    let mut data = vec![];

    Ok(())
}

pub(super) async fn run(
    mut bcast_rx: broadcast::Receiver<BCastMsg>,
    config: Arc<Config>,
    publish_config: Arc<PublishConfig>,
    publisher: Publisher,
) -> Result<()> {
    let (control_tx, mut control_rx) = mpsc::channel(3);
    let mut archive: Option<ArchiveReader> = None;
    let mut pending: FuturesOrdered<Pin<Box<dyn Future<Output = ()>>>> =
        FuturesOrdered::new();
    pending.push_back(Box::pin(async { future::pending().await }));
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
            _ = pending.select_next_some() => (),
            m = bcast_rx.recv().fuse() => match m {
                Err(_) => (),
                Ok(m) => match m {
                    BCastMsg::Batch(_, _) | BCastMsg::LogRotated(_) => (),
                    BCastMsg::NewCurrent(rdr) => archive = Some(rdr),
                    BCastMsg::Stop => break Ok(())
                }
            },
            (args, mut reply) = control_rx.select_next_some() => {
                if pending.len() > publish_config.max_sessions {
                    reply.send(Value::Error(Chars::from("servie busy")));
                } else {
                    let archive = archive.clone();
                    let config = config.clone();
                    pending.push_back(Box::pin(async move {
                        let r = do_oneshot(archive.clone(), config.clone(), args, &mut reply).await;
                        if let Err(e) = r {
                            reply.send(Value::Error(Chars::from(format!("internal error {}", e))));
                        }
                    }));
                }
            }
        }
    }
}
