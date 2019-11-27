use crate::path::Path;
use std;
use futures::channel::{mpsc, oneshot};
use rmp_serde::{encode, decode};

error_chain! {
    foreign_links {
        MPEncodeError(encode::Error);
        MPDecodeError(decode::Error);
        MessageTooLarge(std::num::TryFromIntError);
        IOErr(std::io::Error);
        OneshotCanceled(oneshot::Canceled);
        ChannelIO(mpsc::SendError);
    }

    errors {
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
}
