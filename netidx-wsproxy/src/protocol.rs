use netidx::{protocol::value::Value, publisher::Id as PubId, subscriber::SubId};
use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ToWs {
    Subscribe {
        path: String,
    },
    Unsubscribe {
        id: SubId,
    },
    Write {
        id: SubId,
        val: Value,
    },
    Publish {
        path: String,
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
        path: String,
        args: Vec<(String, Value)>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FromWs {
    Subscribed { id: SubId },
    Update { id: SubId, val: Value },
    Unsubscribed,
    Wrote,
    Published { id: PubId },
    Updated,
    Unpublished,
    Called { result: Value },
    Error { message: String },
    #[serde(other)]
    Unknown,
}
