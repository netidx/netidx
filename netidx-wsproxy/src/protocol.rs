use netidx::{
    path::Path,
    protocol::value::Value,
    publisher::Id as PubId,
    subscriber::{Event, SubId},
};
use poolshark::Pooled;
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchItem {
    pub id: PubId,
    pub data: Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum Request {
    Subscribe {
        path: Path,
    },
    Unsubscribe {
        id: SubId,
    },
    Write {
        id: SubId,
        val: Value,
    },
    Publish {
        path: Path,
        init: Value,
    },
    Update {
        updates: Pooled<Vec<BatchItem>>,
    },
    Unpublish {
        id: PubId,
    },
    Call {
        id: u64,
        path: Path,
        args: Pooled<Vec<(Pooled<String>, Value)>>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Update {
    pub id: SubId,
    pub event: Event,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum Response {
    Subscribed { id: SubId },
    Update { updates: Pooled<Vec<Update>> },
    Unsubscribed,
    Wrote,
    Published { id: PubId },
    Updated,
    Unpublished,
    CallSuccess { id: u64, result: Value },
    CallFailed { id: u64, error: String },
    Error { error: String },
}
