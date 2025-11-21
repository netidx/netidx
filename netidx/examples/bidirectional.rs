//! Bidirectional communication example
//!
//! This example demonstrates how subscribers can write values back to
//! publishers, enabling request/reply patterns and remote control.
//!
//! Run this with:
//! ```
//! cargo run --example bidirectional
//! ```

use futures::{channel::mpsc, prelude::*};
use netidx::{
    config::Config,
    path::Path,
    publisher::{DesiredAuth, PublisherBuilder, Value, WriteRequest},
    subscriber::Subscriber,
};
use tokio::{task, time};

async fn run_publisher() -> anyhow::Result<()> {
    let cfg = Config::load_default()?;

    let publisher = PublisherBuilder::new(cfg)
        .desired_auth(DesiredAuth::Anonymous)
        .build()
        .await?;

    println!("[Publisher] Started");

    // Publish a control value
    let brightness =
        publisher.publish(Path::from("/device/light/brightness"), Value::U32(50))?;

    let power = publisher.publish(Path::from("/device/light/power"), Value::Bool(false))?;

    // Set up a channel to receive write requests
    let (tx, mut rx) = mpsc::channel::<poolshark::global::GPooled<Vec<WriteRequest>>>(10);

    // Register to receive writes for our published values
    publisher.writes(brightness.id(), tx.clone());
    publisher.writes(power.id(), tx.clone());

    println!("[Publisher] Ready to receive commands");

    // Process write requests
    task::spawn(async move {
        while let Some(mut batch) = rx.next().await {
            for req in batch.drain(..) {
                println!(
                    "[Publisher] Received write request: path={}, value={:?}",
                    req.path, req.value
                );

                // Update the published value with the written value
                let mut update_batch = publisher.start_batch();

                if req.id == brightness.id() {
                    brightness.update(&mut update_batch, req.value.clone());
                } else if req.id == power.id() {
                    power.update(&mut update_batch, req.value.clone());
                }

                update_batch.commit(None).await;

                // Send a reply if the client requested one
                if let Some(send_result) = req.send_result {
                    send_result.send(Value::String("OK".into()));
                }
            }
        }
    });

    // Keep the publisher running
    loop {
        time::sleep(time::Duration::from_secs(1)).await;
    }
}

async fn run_subscriber() -> anyhow::Result<()> {
    // Give the publisher time to start
    time::sleep(time::Duration::from_secs(1)).await;

    let cfg = Config::load_default()?;
    let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous)?;

    println!("[Subscriber] Connecting...");

    // Subscribe to the values
    let brightness = subscriber.subscribe(Path::from("/device/light/brightness"));
    let power = subscriber.subscribe(Path::from("/device/light/power"));

    brightness.wait_subscribed().await?;
    power.wait_subscribed().await?;

    println!("[Subscriber] Connected");
    println!("[Subscriber] Initial brightness: {:?}", brightness.last());
    println!("[Subscriber] Initial power: {:?}", power.last());

    // Simulate controlling the device
    time::sleep(time::Duration::from_secs(2)).await;

    println!("[Subscriber] Writing power=true");
    power.write(Value::Bool(true));

    time::sleep(time::Duration::from_secs(1)).await;

    println!("[Subscriber] Writing brightness=75");
    brightness.write(Value::U32(75));

    time::sleep(time::Duration::from_secs(1)).await;

    println!("[Subscriber] Writing brightness=100");
    brightness.write(Value::U32(100));

    time::sleep(time::Duration::from_secs(1)).await;

    println!("[Subscriber] Writing power=false");
    power.write(Value::Bool(false));

    // Keep running to see any updates
    time::sleep(time::Duration::from_secs(2)).await;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Bidirectional Communication Example");
    println!("===================================\n");

    // Run publisher and subscriber concurrently
    tokio::try_join!(run_publisher(), run_subscriber())?;

    Ok(())
}
