//! Put the publisher's and subscriber's error channels on stderr.
//!
//! Both channels answer one question — is this path published, is this
//! subscription subscribed — and neither answers it unless someone is
//! listening. A command line tool has no other way to say so: without this a
//! path the resolver refused looks exactly like a path nobody has asked for.

use futures::{channel::mpsc, prelude::*};
use log::{error, info};
use netidx::{publisher::Publisher, subscriber::Subscriber};
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

/// Log the condition of this subscriber's durable subscriptions as it changes.
pub(crate) fn subscriber(subscriber: &Subscriber) {
    let (tx, mut rx) = mpsc::channel(QUEUE);
    subscriber.errors(tx);
    task::spawn(async move {
        while let Some(mut batch) = rx.next().await {
            for (path, errors) in batch.drain(..) {
                if errors.is_empty() {
                    info!("{path} is subscribed");
                } else {
                    error!("{path}: {errors}");
                }
            }
        }
    });
}
