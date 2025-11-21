//! Temperature display subscriber
//!
//! This example subscribes to the temperature sensors published by
//! temperature_monitor.rs and displays them in a table.
//!
//! First run the monitor:
//! ```
//! cargo run --example temperature_monitor
//! ```
//!
//! Then run this display:
//! ```
//! cargo run --example temperature_display
//! ```

use futures::{channel::mpsc, prelude::*};
use netidx::{
    config::Config,
    path::Path,
    subscriber::{DesiredAuth, Event, Subscriber, UpdatesFlags},
};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::load_default()?;
    let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous)?;

    println!("Temperature Display");
    println!("==================\n");

    // Subscribe to all the temperature sensors
    let sensors = vec!["living_room", "bedroom", "kitchen", "outside"];

    let mut subscriptions = HashMap::new();

    for sensor in &sensors {
        let path = format!("/sensors/temperature/{}/current", sensor);
        let sub = subscriber.subscribe(Path::from(path));
        sub.wait_subscribed().await?;
        subscriptions.insert(sensor.to_string(), sub);
    }

    // Subscribe to system status
    let status = subscriber.subscribe(Path::from("/sensors/temperature/system/status"));
    status.wait_subscribed().await?;

    println!("Subscribed to {} sensors\n", sensors.len());

    // Display initial values
    println!("Initial readings:");
    for (name, sub) in &subscriptions {
        if let Event::Update(value) = sub.last() {
            println!("  {}: {:?}", name, value);
        }
    }
    println!();

    // Set up updates channel
    let (tx, mut rx) = mpsc::channel(10);

    for sub in subscriptions.values() {
        sub.updates(UpdatesFlags::empty(), tx.clone());
    }
    status.updates(UpdatesFlags::empty(), tx.clone());

    println!("Live updates:");

    // Process updates
    while let Some(batch) = rx.next().await {
        for (sub_id, event) in batch {
            // Find which sensor this update is for
            for (name, sub) in &subscriptions {
                if sub.id() == sub_id {
                    if let Event::Update(value) = event {
                        println!("{:15} {:?}", format!("{}:", name), value);
                    }
                    break;
                }
            }

            // Check if it's the status update
            if sub_id == status.id() {
                if let Event::Update(value) = event {
                    println!("{:15} {:?}", "status:", value);
                }
            }
        }
    }

    Ok(())
}
