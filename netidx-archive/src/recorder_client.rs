use std::collections::{HashMap, VecDeque};
use netidx::{pool::Pooled, path::Path, subscriber::Event};
use netidx_protocols::{call_rpc, rpc::client::Proc};
use netidx_derive::Pack;
use chrono::prelude::*;
use crate::logfile::{Id, BatchItem};


#[derive(Debug, Clone, Pack)]
pub struct OneshotReply {
    pub pathmap: Pooled<Vec<(Id, Path)>>,
    pub image: Pooled<HashMap<Id, Event>>,
    pub deltas: Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>,
}
