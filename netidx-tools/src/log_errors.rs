//! Put the publisher's and subscriber's error channels on stderr.
//!
//! Both channels answer one question — is this path published, is this
//! subscription subscribed — and neither answers it unless someone is
//! listening. A command line tool has no other way to say so: without this a
//! path the resolver refused looks exactly like a path nobody has asked for.

use futures::{channel::mpsc, prelude::*};
use log::{error, info};
use netidx::{
    path::Path,
    publisher::Publisher,
    subscriber::{SubId, SubscribeErrors, Subscriber},
};
use std::collections::HashMap;
use tokio::task;

/// Enough that a tool reporting a million paths at once doesn't wake the task
/// a million times, and small enough that it is not a buffer worth thinking
/// about. The sender waits when it's full, which costs nothing here.
const QUEUE: usize = 3;

/// Log the condition of this publisher's paths as it changes.
pub(crate) fn publisher(publisher: &Publisher) {
    let (tx, mut rx) = mpsc::channel(QUEUE);
    publisher.errors(tx);
    task::spawn(async move {
        while let Some(mut batch) = rx.next().await {
            for (path, errors) in batch.drain(..) {
                if errors.is_empty() {
                    info!("{path} is published");
                } else {
                    error!("{path}: {errors}");
                }
            }
        }
    });
}

/// Say what a subscription's condition is now.
///
/// Separate from the loop below because the subscriber's channel is keyed by
/// `SubId`, and a tool whose subscriptions come and go keeps the id to path
/// mapping it already needs for its own output rather than a second copy here.
pub(crate) fn subscription(path: &Path, errors: SubscribeErrors) {
    if errors.is_empty() {
        info!("{path} is subscribed");
    } else {
        error!("{path}: {errors}");
    }
}

/// Log the condition of a fixed set of durable subscriptions as it changes.
///
/// `paths` pairs each subscription's id with the path it was for; a subscriber
/// has both, because it must hold the `Dval` to keep the subscription alive
/// and it asked for the path in the first place. An id it does not cover is
/// one this tool did not make, and is not this tool's to explain.
pub(crate) fn subscriber(subscriber: &Subscriber, paths: HashMap<SubId, Path>) {
    let (tx, mut rx) = mpsc::channel(QUEUE);
    subscriber.errors(tx);
    task::spawn(async move {
        while let Some(mut batch) = rx.next().await {
            for (id, errors) in batch.drain(..) {
                if let Some(path) = paths.get(&id) {
                    subscription(path, errors)
                }
            }
        }
    });
}
