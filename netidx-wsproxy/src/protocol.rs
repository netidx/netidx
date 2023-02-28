use netidx::{
    path::Path, protocol::value::Value, publisher::Id as PubId, subscriber::SubId,
};
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ToWs {
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
        path: Path,
        args: Vec<(String, Value)>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FromWs {
    Subscribed {
        id: SubId,
    },
    Update {
        id: SubId,
        val: Value,
    },
    Unsubscribed,
    Wrote,
    Published {
        id: PubId,
    },
    Updated,
    Unpublished,
    Called {
        result: Value,
    },
    Error {
        error: String,
    },
    #[serde(other)]
    Unknown,
}
