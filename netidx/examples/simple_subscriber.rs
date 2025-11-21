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

use anyhow::Result;
use futures::{channel::mpsc, future::try_join_all, prelude::*};
use netidx::{
    config::Config,
    path::Path,
    subscriber::{DesiredAuth, Subscriber, UpdatesFlags},
};

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    let base = Path::from("/local/example");
    // Create a subscriber with anonymous auth
    let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous)?;
    println!("Subscribing to /local/example/counter and /local/example/timestamp");
    // Subscribe to the counter
    let counter = subscriber.subscribe(base.join("counter"));
    // Subscribe to the timestamp
    let timestamp = subscriber.subscribe(base.join("timestamp"));
    // Wait for the subscriptions to be established
    try_join_all([counter.wait_subscribed(), timestamp.wait_subscribed()]).await?;
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

fn main() -> Result<()> {
    Config::maybe_run_machine_local_resolver()?;
    tokio_main(Config::load_default_or_local_only()?)
}
