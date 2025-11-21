//! Simple subscriber example
//!
//! This example subscribes to values published by simple_publisher.rs
//!
//! Run the publisher first:
//! ```
//! cargo run --example simple_publisher
//! ```
//!
//! Then run this subscriber:
//! ```
//! cargo run --example simple_subscriber
//! ```

use futures::{channel::mpsc, prelude::*};
use netidx::{
    config::Config,
    path::Path,
    subscriber::{DesiredAuth, Subscriber, UpdatesFlags},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load the netidx config
    let cfg = Config::load_default()?;

    // Create a subscriber with anonymous auth
    let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous)?;

    println!("Subscribing to /example/counter and /example/timestamp");

    // Subscribe to the counter
    let counter = subscriber.subscribe(Path::from("/example/counter"));

    // Subscribe to the timestamp
    let timestamp = subscriber.subscribe(Path::from("/example/timestamp"));

    // Wait for the subscriptions to be established
    counter.wait_subscribed().await?;
    timestamp.wait_subscribed().await?;

    println!("Subscribed! Initial values:");
    println!("  Counter: {:?}", counter.last());
    println!("  Timestamp: {:?}", timestamp.last());

    // Set up a channel to receive updates
    let (tx, mut rx) = mpsc::channel(10);

    // Register for updates from both subscriptions
    counter.updates(UpdatesFlags::empty(), tx.clone());
    timestamp.updates(UpdatesFlags::empty(), tx);

    println!("\nWaiting for updates...");

    // Process updates as they arrive
    while let Some(mut batch) = rx.next().await {
        for (sub_id, event) in batch.drain(..) {
            // Check which subscription this update is for
            if sub_id == counter.id() {
                println!("Counter updated: {:?}", event);
            } else if sub_id == timestamp.id() {
                println!("Timestamp updated: {:?}", event);
            }
        }
    }

    Ok(())
}
