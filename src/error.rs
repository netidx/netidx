use crate::path::Path;
use std;
use futures::channel::{mpsc, oneshot};
use rmp_serde::{encode, decode};

#[derive(Fail, Debug)]
enum Error {
    #[fail(display = "msgpack encode error {}", _0)]
    MPEncodeError(#[fail(cause)] encode::Error),
    #[fail(display = "msgpack decode error {}", _0)]
    MPDecodeError(#[fail(cause)] decode::Error),
    #[fail(display = "msgpack msg too large {}", _0)]
    MessageTooLarge(#[fail(cause)] std::num::TryFromIntError),
    #[fail(display = "io error {}", _0)]
    IOErr(#[fail(cause)] std::io::Error),
    OneshotCanceled(oneshot::Canceled),
    ChannelIO(mpsc::SendError),
    
    Unsubscribed {
        description("the publisher canceled the subscription"),
        display("the publisher canceled the subscription"),
    }
    SubscriptionIsDead {
        description("the subscription is dead, it can no longer be used"),
        display("the subscription is dead, it can no longer be used")
    }
    PathNotFound(p: Path) {
        description("the specified path could not be found"),
        display("the path '{:?}' could not be found", p)
    }
    AlreadyPublished(s: Path) {
        description("already published"),
        display("{:?} is already published", s)
    }
    ResolverError(s: String) {
        description("resolver error"),
        display("resolver error {}", s)
    }
    ResolverUnexpected {
        description("unexpected response from the resolver"),
        display("unexpected response from the resolver")
    }
}
