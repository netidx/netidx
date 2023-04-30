use crate::logfile::{BatchItem, Id};
use anyhow::Result;
use chrono::prelude::*;
use fxhash::FxHashMap;
use netidx::{
    chars::Chars,
    pack::Pack,
    path::Path,
    pool::{Pool, Pooled},
    resolver_client::GlobSet,
    subscriber::{Event, Subscriber, Value},
};
use netidx_derive::Pack;
use netidx_protocols::{call_rpc, rpc::client::Proc};
use std::{collections::VecDeque, sync::Arc};

lazy_static! {
    pub(crate) static ref PATHMAPS: Pool<FxHashMap<Id, Path>> = Pool::new(100, 100_000);
    pub(crate) static ref SHARDS: Pool<Vec<OneshotReplyShard>> = Pool::new(100, 128);
}

#[derive(Debug, Clone, Pack)]
pub struct OneshotReplyShard {
    pub pathmap: Pooled<FxHashMap<Id, Path>>,
    pub image: Pooled<FxHashMap<Id, Event>>,
    pub deltas: Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>,
}

#[derive(Debug, Clone, Pack)]
pub struct OneshotReply(pub Pooled<Vec<OneshotReplyShard>>);

fn encode_bound(bound: &Option<DateTime<Utc>>) -> Value {
    match bound {
        None => Value::String(Chars::from("unbounded")),
        Some(dt) => Value::DateTime(*dt),
    }
}

fn encode_filter(filter: &GlobSet) -> Value {
    let v =
        filter.iter().map(|glob| Value::String(glob.raw().clone())).collect::<Vec<_>>();
    Value::Array(Arc::from(v))
}

#[derive(Debug, Clone)]
pub struct Client {
    oneshot: Proc,
}

impl Client {
    pub async fn new(subscriber: &Subscriber, base: &Path) -> Result<Client> {
        let oneshot = Proc::new(subscriber, base.append("oneshot")).await?;
        Ok(Self { oneshot })
    }

    pub async fn oneshot(
        &self,
        start: &Option<DateTime<Utc>>,
        end: &Option<DateTime<Utc>>,
        filter: &GlobSet,
    ) -> Result<OneshotReply> {
        let res = call_rpc!(
            &self.oneshot,
            start: encode_bound(start),
            end: encode_bound(end),
            filter: encode_filter(filter)
        )
        .await?;
        match res {
            Value::Error(e) => bail!("{}", e.to_string()),
            Value::Bytes(buf) => Ok(Pack::decode(&mut &*buf)?),
            _ => bail!("unexpected response type"),
        }
    }
}
