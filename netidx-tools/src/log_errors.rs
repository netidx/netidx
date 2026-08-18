//! Put the publisher's and subscriber's error channels on stderr.

use futures::{channel::mpsc, prelude::*};
use log::{error, info};
use netidx::{
    path::Path,
    publisher::Publisher,
    subscriber::{SubId, SubscribeErrors, Subscriber},
};
use std::collections::HashMap;
use tokio::task;

/// The sender waits when it is full, which costs nothing here.
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
pub(crate) fn subscription(path: &Path, errors: SubscribeErrors) {
    if errors.is_empty() {
        info!("{path} is subscribed");
    } else {
        error!("{path}: {errors}");
    }
}

/// Log the condition of a fixed set of durable subscriptions as it changes.
/// An id `paths` does not cover is one this tool did not make.
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
