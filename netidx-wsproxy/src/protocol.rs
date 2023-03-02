use netidx::{
    path::Path, protocol::value::Value, publisher::Id as PubId, subscriber::{SubId, Event},
};
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        id: PubId,
        val: Value,
    },
    Unpublish {
        id: PubId,
    },
    Call {
        id: u64,
        path: Path,
        args: Vec<(String, Value)>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Response {
    Subscribed {
        id: SubId,
    },
    Update {
        id: SubId,
        event: Event,
    },
    Unsubscribed,
    Wrote,
    Published {
        id: PubId,
    },
    Updated,
    Unpublished,
    CallSuccess {
        id: u64,
        result: Value,
    },
    CallFailed {
        id: u64,
        error: String,
    },
    Error {
        error: String,
    },
    #[serde(other)]
    Unknown,
}
