//! Resilient Subscriptions Example
//!
//! This example demonstrates the critical difference between Val (non-durable)
//! and Dval (durable) subscriptions by simulating a publisher that periodically
//! restarts.
//!
//! ## What You'll See
//!
//! - A publisher that restarts every 5 seconds (simulating firmware updates,
//!   power cycles, or crashes)
//! - A Val subscription that dies permanently on first disconnect
//! - A Dval subscription that automatically reconnects using linear backoff
//!
//! ## Key Takeaways
//!
//! - **Val**: Fast, lightweight, but not resilient. Use for non-critical data
//!   or when you have manual recovery logic.
//! - **Dval**: Slightly more memory overhead, but handles all common failure
//!   scenarios automatically. Recommended for production use.
//!
//! ## Running the Example
//!
//! ```bash
//! cargo run --example resilient_subscriptions
//! ```
//!
//! The example runs for 20 seconds and shows 4 restart cycles.

use anyhow::Result;
use futures::{channel::mpsc, prelude::*};
use netidx::{
    config::Config,
    path::Path,
    publisher::{PublisherBuilder, Val as PubVal},
    subscriber::{Event, Subscriber, SubscriberBuilder, UpdatesFlags},
};
use tokio::{task, time};

/// Publisher task that restarts every 5 seconds
async fn run_publisher(cfg: Config, base: Path) -> Result<()> {
    loop {
        // Create publisher instance
        let publisher = PublisherBuilder::new(cfg.clone()).build().await?;
        let sensor: PubVal = publisher.publish(base.join("temperature"), 20.0)?;

        println!("[Publisher] Started - publishing at {}", base.join("temperature"));

        // Publish for 5 seconds
        let publish_duration = time::Duration::from_secs(5);
        let start = time::Instant::now();

        while start.elapsed() < publish_duration {
            let temp = 20.0 + (start.elapsed().as_secs_f64() * 0.5);
            let mut batch = publisher.start_batch();
            sensor.update(&mut batch, temp);
            batch.commit(None).await;
            time::sleep(time::Duration::from_millis(500)).await;
        }

        // Simulate restart
        println!("[Publisher] Simulating restart (firmware update)...");
        drop(sensor);
        drop(publisher);
        println!("[Publisher] Stopped");

        // Wait before restarting
        time::sleep(time::Duration::from_secs(2)).await;
        println!("[Publisher] Restarting...\n");
    }
}

/// Val (non-durable) subscriber - dies on disconnect
async fn run_val_subscriber(subscriber: Subscriber, base: Path) -> Result<()> {
    let (tx, mut rx) = mpsc::channel(10);

    println!("[Val]  Subscribing (non-durable)...");

    // Use subscribe_nondurable_one_updates to get a Val
    // we may not succeed the on the first try, if the publisher isn't up yet, so
    // we do this in a loop.
    let _val = loop {
        let val = subscriber
            .subscribe_nondurable_one_updates(
                base.join("temperature"),
                [(UpdatesFlags::empty(), tx.clone())],
                None,
            )
            .await;
        match val {
            Err(_) => (),
            Ok(val) => break val,
        }
    };

    println!("[Val]  Subscribed");

    let mut msg_count = 0;
    while let Some(mut batch) = rx.next().await {
        for (_, event) in batch.drain(..) {
            match event {
                Event::Update(value) => {
                    msg_count += 1;
                    println!(
                        "[Val]  Received: {} (total: {})",
                        format_value(&value),
                        msg_count
                    );
                }
                Event::Unsubscribed => {
                    println!("[Val]  ❌ CONNECTION LOST - Val will NOT reconnect");
                    println!("[Val]  ❌ NO MORE UPDATES WILL BE RECEIVED");
                    println!("[Val]  (Val dies here - this task exits)\n");
                    return Ok(()); // Val dies here
                }
            }
        }
    }
    Ok(())
}

/// Dval (durable) subscriber - auto-reconnects
async fn run_dval_subscriber(subscriber: Subscriber, base: Path) -> Result<()> {
    let (tx, mut rx) = mpsc::channel(10);

    println!("[Dval] Subscribing (durable)...");

    // Use subscribe_updates to set up subscription and updates in one call
    let _dval = subscriber
        .subscribe_updates(base.join("temperature"), [(UpdatesFlags::empty(), tx)]);

    println!("[Dval] Subscribed");

    let mut msg_count = 0;
    let mut last_dead_count = 0;

    // Monitor durable stats periodically
    let stats_task = {
        let subscriber_clone = subscriber.clone();
        task::spawn(async move {
            loop {
                time::sleep(time::Duration::from_secs(1)).await;
                let stats = subscriber_clone.durable_stats();
                if stats.dead > last_dead_count {
                    println!("[Dval] 🔄 Connection lost - attempting reconnection...");
                    last_dead_count = stats.dead;
                } else if stats.dead < last_dead_count {
                    println!("[Dval] ✅ Reconnected successfully!");
                    last_dead_count = stats.dead;
                }
            }
        })
    };

    while let Some(mut batch) = rx.next().await {
        for (_, event) in batch.drain(..) {
            match event {
                Event::Update(value) => {
                    msg_count += 1;
                    println!(
                        "[Dval] Received: {} (total: {})",
                        format_value(&value),
                        msg_count
                    );
                }
                Event::Unsubscribed => {
                    // Dval temporarily disconnected but will reconnect
                    println!("[Dval] 🔄 Temporarily disconnected (reconnecting...)");
                }
            }
        }
    }

    stats_task.abort();
    Ok(())
}

fn format_value(value: &netidx_value::Value) -> String {
    use netidx_value::Value;
    match value {
        Value::F64(v) => format!("{:.1}°C", v),
        v => format!("{:?}", v),
    }
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    println!("╔═════════════════════════════════════════════════╗");
    println!("║  Resilient Subscriptions: Val vs Dval Demo     ║");
    println!("╚═════════════════════════════════════════════════╝\n");
    println!("This example demonstrates the difference between:");
    println!("  • Val (non-durable): Dies on disconnect");
    println!("  • Dval (durable): Auto-reconnects with linear backoff\n");
    println!("The publisher will restart every 5 seconds to simulate");
    println!("connection failures. Watch how each subscription type");
    println!("handles the disruption.\n");
    println!("─────────────────────────────────────────────────\n");

    let base = Path::from("/local/resilience/sensor");

    // Spawn all components
    let publisher_task = task::spawn(run_publisher(cfg.clone(), base.clone()));

    // Give publisher time to start
    time::sleep(time::Duration::from_millis(500)).await;

    let subscriber = SubscriberBuilder::new(cfg).build()?;
    let val_task = task::spawn(run_val_subscriber(subscriber.clone(), base.clone()));
    let dval_task = task::spawn(run_dval_subscriber(subscriber, base.clone()));

    // Run for 22 seconds to show 4+ restart cycles
    time::sleep(time::Duration::from_secs(22)).await;

    println!("\n─────────────────────────────────────────────────");
    println!("Demo complete! Summary:");
    println!("  Val:  Died on first disconnect, received no more data");
    println!("  Dval: Survived all restarts, received continuous data");
    println!("═════════════════════════════════════════════════");

    publisher_task.abort();
    val_task.abort();
    dval_task.abort();

    Ok(())
}

fn main() -> Result<()> {
    // init logger
    env_logger::init();
    // start local resolver if necessary
    Config::maybe_run_machine_local_resolver()?;
    // go
    tokio_main(Config::load_default_or_local_only()?)
}
