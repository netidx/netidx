//! Failover Subscriber Example
//!
//! Demonstrates automatic failover when multiple publishers publish the same paths.
//!
//! ## Failover Behavior
//!
//! - Clients randomly select from available publishers with same priority
//! - Higher priority publishers are always preferred
//! - If current publisher dies, clients automatically try others
//! - Failover is transparent - no application code changes needed
//! - When primary recovers, new subscriptions will use it (existing ones stay on backup)
//!
//! ## Running
//!
//! Start this after starting multiple publishers with different priorities:
//!
//! ```bash
//! # In separate terminals, start publishers
//! PRIORITY=2 cargo run --example failover_publisher  # Primary
//! PRIORITY=0 cargo run --example failover_publisher  # Backup
//!
//! # Then start subscriber
//! cargo run --example failover_subscriber
//!
//! # Kill primary publisher (Ctrl+C) and watch automatic failover
//! # Restart primary and create new subscription to see priority preference
//! ```
//!
//! ## Important Note
//!
//! Netidx keeps track of recently failed publishers and will not try
//! them again for 60 seconds (unless all publishers recently
//! failed). This means if you fail high, and then start it back up
//! before the 60 second window, and then fail normal, subscriber will
//! fail to LOW not HIGH.

use anyhow::Result;
use futures::{channel::mpsc, StreamExt};
use netidx::{
    config::Config,
    path::Path,
    subscriber::{Event, SubscriberBuilder, UpdatesFlags},
};
use netidx_value::Value;
use tokio::signal;

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    println!("╔════════════════════════════════════════════════╗");
    println!("║  Failover Subscriber - Automatic Failover      ║");
    println!("╚════════════════════════════════════════════════╝\n");

    let subscriber = SubscriberBuilder::new(cfg).build()?;
    let base = Path::from("/local/failover/sensor");

    println!("Subscribing to sensor data...\n");

    // Subscribe to sensor paths
    let (tx, mut rx) = mpsc::channel(10);
    let temp_sub = subscriber.subscribe_updates(
        base.join("temperature"),
        [(UpdatesFlags::empty(), tx.clone())],
    );
    let status_sub = subscriber
        .subscribe_updates(base.join("status"), [(UpdatesFlags::empty(), tx.clone())]);
    let id_sub = subscriber.subscribe_updates(
        base.join("publisher_id"),
        [(UpdatesFlags::empty(), tx.clone())],
    );

    // Wait for initial subscriptions
    tokio::try_join![
        temp_sub.wait_subscribed(),
        status_sub.wait_subscribed(),
        id_sub.wait_subscribed()
    ]?;

    println!("✓ Subscribed successfully");
    if let Event::Update(value) = id_sub.last() {
        println!("  Connected to publisher: {}", value);
    }
    println!("\nReceiving updates (will automatically failover if publisher dies)...\n");

    // Track which publisher we're connected to
    let mut current_publisher = Value::Null;

    loop {
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => {
                println!("\n\nShutting down subscriber...\n");
                break;
            }
            Some(mut batch) = rx.next() => {
                for (id, event) in batch.drain(..) {
                    match event {
                        Event::Update(value) => {
                            if id == temp_sub.id() {
                                println!("  Temperature: {value}");
                            } else if id == status_sub.id() {
                                println!("  Status: {value}");
                            } else if id == id_sub.id() {
                                if value == current_publisher {
                                    println!("  Publisher: {value}")
                                } else {
                                    if current_publisher == Value::Null {
                                        println!("🔌 Connected to: {value}");
                                    } else {
                                        println!("\n🔄 FAILOVER DETECTED!");
                                        println!("  Previous: {current_publisher}");
                                        println!("  Current:  {value}\n");
                                    }
                                    current_publisher = value;
                                }
                            }
                        }
                        Event::Unsubscribed => {
                            println!("\n⚠️  Subscription lost - attempting failover...");
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    // start logging
    env_logger::init();
    // maybe run the local machine resolver
    Config::maybe_run_machine_local_resolver()?;
    // load config, start async, and go!
    tokio_main(Config::load_default_or_local_only()?)
}
