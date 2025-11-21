//! Bidirectional communication example
//!
//! This example demonstrates how subscribers can write values back to
//! publishers, enabling request/reply patterns and remote control.
//!
//! Run this with:
//! ```
//! cargo run --example bidirectional
//! ```

use anyhow::Result;
use futures::{channel::mpsc, prelude::*};
use netidx::{
    config::Config,
    path::Path,
    publisher::{DesiredAuth, PublishFlags, PublisherBuilder, Value, WriteRequest},
    subscriber::{Event, Subscriber, UpdatesFlags},
};
use poolshark::global::GPooled;
use tokio::{task, time};

async fn run_publisher(cfg: Config, base: Path) -> Result<()> {
    let publisher =
        PublisherBuilder::new(cfg).desired_auth(DesiredAuth::Anonymous).build().await?;
    // Set up a channel to receive write requests
    let (tx, mut rx) = mpsc::channel::<GPooled<Vec<WriteRequest>>>(10);
    println!("[Publisher] Started");
    // Publish writable values
    let brightness = publisher.publish_with_flags_and_writes(
        PublishFlags::empty(),
        base.join("brightness"),
        50,
        Some(tx.clone()),
    )?;
    let power = publisher.publish_with_flags_and_writes(
        PublishFlags::empty(),
        base.join("power"),
        false,
        Some(tx),
    )?;
    println!("[Publisher] Ready to receive commands");
    // Process write requests. The stream will not end as long as brightness and
    // power remain published.
    while let Some(mut batch) = rx.next().await {
        for req in batch.drain(..) {
            println!(
                "[Publisher] Received write request: path={}, value={:?}",
                req.path, req.value
            );
            // Update the published value with the written value
            let mut update_batch = publisher.start_batch();
            if req.id == brightness.id() {
                brightness.update_changed(&mut update_batch, req.value);
            } else if req.id == power.id() {
                power.update_changed(&mut update_batch, req.value);
            }
            update_batch.commit(None).await;
            // Send a reply if the client requested one
            if let Some(send_result) = req.send_result {
                send_result.send("Ok".into());
            }
        }
    }
    Ok(())
}

async fn run_subscriber(cfg: Config, base: Path) -> anyhow::Result<()> {
    let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous)?;
    let (tx, mut rx) = mpsc::channel(10);
    println!("[Subscriber] Connecting...");
    // Subscribe to the values
    let brightness = subscriber.subscribe_updates(
        base.join("brightness"),
        [(UpdatesFlags::empty(), tx.clone())],
    );
    let power =
        subscriber.subscribe_updates(base.join("power"), [(UpdatesFlags::empty(), tx)]);
    let watch_updates = {
        let br_id = brightness.id();
        let pw_id = power.id();
        task::spawn(async move {
            while let Some(mut batch) = rx.next().await {
                for (sub_id, ev) in batch.drain(..) {
                    if let Event::Update(value) = ev {
                        if sub_id == br_id {
                            println!("[Subscriber] brightness: {value}")
                        } else if sub_id == pw_id {
                            println!("[Subscriber] power: {value}")
                        }
                    }
                }
            }
        })
    };
    brightness.wait_subscribed().await?;
    power.wait_subscribed().await?;
    println!("[Subscriber] Connected");
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
    Ok(watch_updates.await?)
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    println!("Bidirectional Communication Example");
    println!("===================================\n");
    let base = Path::from("/local/device/light");
    // Run publisher and subscriber concurrently
    tokio::try_join!(
        run_publisher(cfg.clone(), base.clone()),
        run_subscriber(cfg, base)
    )?;
    Ok(())
}

fn main() -> Result<()> {
    Config::maybe_run_machine_local_resolver()?;
    tokio_main(Config::load_default_or_local_only()?)
}
