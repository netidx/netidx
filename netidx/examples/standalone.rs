//! Standalone netidx example with no external resolver
//!
//! This example demonstrates using InternalOnly for a self-contained
//! netidx setup. Perfect for testing or when you don't want to run
//! a separate resolver server.
//!
//! Run this with:
//! ```
//! cargo run --example standalone
//! ```

use futures::{channel::mpsc, prelude::*};
use netidx::{
    path::Path,
    publisher::Value,
    subscriber::UpdatesFlags,
    InternalOnly,
};
use tokio::time::{self, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Standalone Netidx Example");
    println!("=========================\n");

    // Create a complete internal netidx setup
    // This includes an embedded resolver server, publisher, and subscriber
    let netidx = InternalOnly::new().await?;

    println!("Internal netidx instance created");
    println!("No external resolver needed!\n");

    // Get references to publisher and subscriber
    let publisher = netidx.publisher();
    let subscriber = netidx.subscriber();

    // Publish some values
    let counter = publisher.publish(Path::from("/counter"), Value::U64(0))?;
    let message = publisher.publish(
        Path::from("/message"),
        Value::String("Hello, Netidx!".into()),
    )?;

    println!("Published values at /counter and /message");

    // Subscribe to the values we just published
    let counter_sub = subscriber.subscribe(Path::from("/counter"));
    let message_sub = subscriber.subscribe(Path::from("/message"));

    // Wait for subscriptions to establish
    counter_sub.wait_subscribed().await?;
    message_sub.wait_subscribed().await?;

    println!("Subscribed to values\n");

    // Set up update channel
    let (tx, mut rx) = mpsc::channel(10);
    counter_sub.updates(UpdatesFlags::empty(), tx.clone());
    message_sub.updates(UpdatesFlags::empty(), tx);

    // Spawn a task to print updates
    tokio::spawn(async move {
        while let Some(batch) = rx.next().await {
            for (sub_id, event) in batch {
                if sub_id == counter_sub.id() {
                    println!("  Counter updated: {:?}", event);
                } else if sub_id == message_sub.id() {
                    println!("  Message updated: {:?}", event);
                }
            }
        }
    });

    println!("Starting update loop...\n");

    // Update values periodically
    let mut count = 0u64;
    let messages = vec![
        "Hello, Netidx!",
        "Publishing is easy",
        "Subscribing too!",
        "All in one process",
    ];

    for i in 0..10 {
        time::sleep(Duration::from_secs(1)).await;

        count += 1;

        let mut batch = publisher.start_batch();
        counter.update(&mut batch, Value::U64(count));

        if i % 2 == 0 {
            let msg = messages[i / 2 % messages.len()];
            message.update(&mut batch, Value::String(msg.into()));
        }

        batch.commit(None).await;
    }

    println!("\nExample complete!");
    println!("This demonstrated publishing and subscribing within a single process");
    println!("using InternalOnly - no external resolver server needed.");

    Ok(())
}
