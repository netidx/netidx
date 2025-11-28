//! Failover Publisher Example
//!
//! Demonstrates netidx's built-in failover capabilities where multiple publishers
//! can publish to the same paths. Subscribers will automatically try all available
//! publishers and failover transparently if one goes down.
//!
//! ## Priority Levels
//!
//! Publishers can specify priority (0-2, higher = preferred):
//! - **2 (High)**: Primary publisher, tried first
//! - **1 (Medium)**: Secondary publisher
//! - **0 (Low)**: Backup publisher, only used if others unavailable
//!
//! Common use case: Low priority for expensive/slow backup networks
//!
//! ## Running
//!
//! Start multiple publishers with different priorities:
//!
//! ```bash
//! # Terminal 1 - Primary (high priority)
//! PRIORITY=2 cargo run --example failover_publisher
//!
//! # Terminal 2 - Backup (low priority)
//! PRIORITY=0 cargo run --example failover_publisher
//!
//! # Terminal 3 - Subscriber (will prefer primary)
//! cargo run --example failover_subscriber
//! ```
//!
//! Then kill the primary (Ctrl+C) and watch subscriber automatically
//! failover to backup without any interruption.

use anyhow::{bail, Result};
use arcstr::ArcStr;
use netidx::{config::Config, path::Path, publisher::PublisherBuilder};
use netidx_netproto::resolver::PublisherPriority;
use std::{env, time::Duration};
use tokio::{signal, time};

fn name(pri: &str) -> ArcStr {
    let pid = std::process::id();
    ArcStr::from(format!("{pri} ({pid})"))
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    // Get priority from environment (default to Normal)
    let priority_str = env::var("PRIORITY").unwrap_or_else(|_| "normal".to_string());
    let (priority, priority_name) = match priority_str.to_lowercase().as_str() {
        "high" | "2" => (PublisherPriority::High, name("HIGH")),
        "normal" | "1" => (PublisherPriority::Normal, name("NORMAL")),
        "low" | "0" => (PublisherPriority::Low, name("LOW")),
        p => bail!("priority \"{p}\" not recognized"),
    };

    println!("╔════════════════════════════════════════════════╗");
    println!("║  Failover Publisher - Priority: {}  ║", priority_name);
    println!("╚════════════════════════════════════════════════╝\n");
    // Build publisher with specified priority
    let publisher = PublisherBuilder::new(cfg).priority(priority).build().await?;

    let base = Path::from("/local/failover/sensor");

    // Publish temperature and status
    let temperature = publisher.publish(base.join("temperature"), 20.0)?;
    let status = publisher.publish(base.join("status"), "online")?;
    let publisher_id =
        publisher.publish(base.join("publisher_id"), priority_name.clone())?;

    println!("Publishing sensor data at:");
    println!("  {}/temperature", base);
    println!("  {}/status", base);
    println!("  {}/publisher_id", base);
    println!("\nPriority: {}", priority_name);
    println!("\nSimulating sensor readings (updates every 2s)...");
    println!("Press Ctrl+C to stop and trigger failover\n");

    // Simulate sensor readings
    let mut counter = 0u64;
    let mut interval = time::interval(Duration::from_secs(2));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                counter += 1;
                let temp = 20.0 + (counter as f64 * 0.1) % 10.0;

                let mut batch = publisher.start_batch();
                temperature.update(&mut batch, temp);
                status.update(&mut batch, "online");
                publisher_id.update(&mut batch, priority_name.clone());
                batch.commit(None).await;

                println!(
                    "[{}] Update #{}: temp={:.1}°C",
                    priority_name, counter, temp
                );
            }
            _ = signal::ctrl_c() => {
                println!("\n\nShutting down publisher ({})...", priority_name);
                println!("Subscribers will failover to other publishers\n");
                break;
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
