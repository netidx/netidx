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

use anyhow::Result;
use futures::{channel::mpsc, prelude::*};
use netidx::{
    config::Config,
    path::Path,
    publisher::Value,
    subscriber::{Event, SubscriberBuilder, UpdatesFlags},
};
use std::collections::HashMap;

#[tokio::main]
async fn tokio_main(cfg: Config) -> anyhow::Result<()> {
    let base = Path::from("/local/sensors/temperature");
    let subscriber = SubscriberBuilder::new(cfg).build()?;
    let (tx, mut rx) = mpsc::channel(10);
    println!("Temperature Display");
    println!("==================\n");
    // Subscribe to all the temperature sensors
    let sensors = vec!["living_room", "bedroom", "kitchen", "outside"];
    let mut subscriptions = HashMap::new();
    for sensor in &sensors {
        let path = base.join("zones").join(sensor).join("current");
        let up = [(UpdatesFlags::empty(), tx.clone())];
        // subscribe and set up the updates channel atomically
        let sub = subscriber.subscribe_updates(path, up);
        subscriptions.insert(sub.id(), (sensor.to_string(), sub));
    }
    // Subscribe to system status
    let status = subscriber.subscribe_updates(
        base.join("system/status"),
        [(UpdatesFlags::empty(), tx.clone())],
    );
    println!("Subscribed to {} sensors\n", sensors.len());
    // Process updates
    while let Some(mut batch) = rx.next().await {
        for (sub_id, event) in batch.drain(..) {
            if let Event::Update(value) = event {
                // Check what kind of update it is
                if sub_id == status.id() {
                    println!("{:15} {}", "status:", value);
                } else if let Some((name, _)) = subscriptions.get(&sub_id)
                    && let Value::F64(v) = value
                {
                    println!("{:15} {:.1}°C", format!("{}:", name), v);
                }
            } else {
                // report loss of connection
                if let Some((name, _)) = subscriptions.get(&sub_id) {
                    println!("{:15} lost", format!("{}:", name))
                }
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    // init logging
    env_logger::init();
    // maybe start the local machine resolver
    Config::maybe_run_machine_local_resolver()?;
    // load the config and go
    tokio_main(Config::load_default_or_local_only()?)
}
