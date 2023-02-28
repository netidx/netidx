use netidx::{protocol::value::Value, publisher::Id as PubId, subscriber::SubId};
use serde_derive::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ToWs {
    Subscribe(String),
    Unsubscribe(SubId),
    Write(SubId, Value),
    Publish(String, Value),
    Update(PubId, Value),
    Unpublish(PubId),
    Call(String, Vec<(String, Value)>),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum FromWs {
    Subscribed(String, SubId),
    Update(SubId, Value),
    Unsubscribed,
    Wrote,
    Published(String, PubId),
    Updated,
    Unpublished,
    Called(Value),
    Error(String),
    #[serde(other)]
    Unknown,
}
