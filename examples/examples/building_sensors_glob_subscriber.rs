//! Building Sensors Glob Subscriber - Dynamic Discovery Example
//!
//! This example demonstrates glob pattern subscription with dynamic discovery
//! using change trackers. It shows how to:
//!
//! 1. Use glob patterns to discover multiple sensors at once
//! 2. Subscribe to all discovered sensors
//! 3. Monitor for structural changes (new sensors being added)
//! 4. Automatically discover and subscribe to new sensors
//!
//! ## Two-Step Discovery Pattern
//!
//! Netidx glob subscriptions use a two-step process:
//! 1. **Discovery**: Use `resolver.list_matching(&GlobSet)` to find matching paths
//! 2. **Subscribe**: Call `subscribe()` on each discovered path
//!
//! ## Dynamic Discovery with Change Tracker
//!
//! This example goes further by using a `ChangeTracker` to monitor for
//! structural changes and automatically re-discover and subscribe to new sensors.
//!
//! ## Running the Example
//!
//! First, start the publisher:
//! ```bash
//! cargo run --example building_sensors_publisher
//! ```
//!
//! Then run this subscriber with a glob pattern:
//! ```bash
//! # All temperature sensors
//! cargo run --example building_sensors_glob_subscriber "/local/building/**/temperature"
//!
//! # All sensors on floor 2
//! cargo run --example building_sensors_glob_subscriber "/local/building/floor2/**"
//!
//! # Temperature and humidity only
//! cargo run --example building_sensors_glob_subscriber "/local/building/**/{temperature,humidity}"
//! ```
//!
//! Watch as the subscriber automatically discovers new sensors when the publisher
//! adds them after 10 seconds!
//!
//! ## Glob Pattern Reference
//!
//! - `*` - matches any chars except path separator
//! - `**` - recursively matches any path depth
//! - `?` - matches single character
//! - `{a,b}` - matches either a or b
//! - `[ab]` - matches character a or b

use anyhow::Result;
use arcstr::ArcStr;
use futures::{channel::mpsc, prelude::*};
use netidx::{
    config::Config,
    path::Path,
    resolver_client::ChangeTracker,
    subscriber::{Dval, Event, SubId, Subscriber, SubscriberBuilder, UpdatesFlags},
};
use netidx_netproto::glob::{Glob, GlobSet};
use poolshark::global::GPooled;
use std::{collections::HashMap, env};
use tokio::{signal, time};

/// Manages subscriptions with bidirectional lookup tables
struct SubscriptionManager {
    by_path: HashMap<Path, SubId>,
    by_id: HashMap<SubId, Dval>,
}

impl SubscriptionManager {
    fn new() -> Self {
        Self { by_path: HashMap::new(), by_id: HashMap::new() }
    }

    /// Subscribe to new paths (skips already-subscribed paths)
    fn subscribe_to_paths(
        &mut self,
        subscriber: &Subscriber,
        paths: impl IntoIterator<Item = Path>,
        tx: &mpsc::Sender<GPooled<Vec<(SubId, Event)>>>,
    ) -> usize {
        let mut count = 0;

        for path in paths.into_iter() {
            // Skip if already subscribed
            if self.by_path.contains_key(&path) {
                continue;
            }

            let dval = subscriber
                .subscribe_updates(path.clone(), [(UpdatesFlags::empty(), tx.clone())]);
            let sub_id = dval.id();
            self.by_path.insert(path, sub_id);
            self.by_id.insert(sub_id, dval);
            count += 1;
        }

        count
    }

    /// Look up path by subscription ID
    fn get_path(&self, sub_id: &SubId) -> Option<&Path> {
        self.by_path.iter().find(|(_, id)| *id == sub_id).map(|(path, _)| path)
    }

    /// Number of active subscriptions
    fn len(&self) -> usize {
        self.by_id.len()
    }
}

#[tokio::main]
async fn tokio_main(cfg: Config, pattern: String) -> Result<()> {
    println!("╔════════════════════════════════════════════════╗");
    println!("║  Building Sensors Glob Subscriber              ║");
    println!("║  Dynamic Discovery Demonstration               ║");
    println!("╚════════════════════════════════════════════════╝\n");

    // Create subscriber
    let subscriber = SubscriberBuilder::new(cfg).build()?;
    let resolver = subscriber.resolver();

    // Create glob set (published_only: true means match only actual published values)
    let glob = Glob::new(ArcStr::from(&pattern))?;
    let globset = GlobSet::new(true, [glob])?;

    println!("Pattern: {}", pattern);
    println!("Starting discovery and monitoring...\n");

    // Set up channels
    let (updates_tx, mut updates_rx) = mpsc::channel(100);
    let (mut discovery_tx, mut discovery_rx) = mpsc::channel(100);
    let mut subscriptions = SubscriptionManager::new();

    // Spawn background discovery task
    let base = Path::from("/local/building");
    let discovery_task = tokio::spawn(async move {
        let mut tracker = ChangeTracker::new(base);
        let mut check_interval = time::interval(time::Duration::from_secs(2));

        loop {
            check_interval.tick().await;

            // Change tracker returns true on first check, so no special initial case needed
            match resolver.check_changed(&mut tracker).await {
                Ok(false) => (),
                Err(e) => eprintln!("Change tracker error: {}", e),
                Ok(true) => {
                    // Discover matching paths
                    match resolver.list_matching(&globset).await {
                        Err(e) => eprintln!("Discovery error: {}", e),
                        Ok(mut results) => {
                            for batch in results.drain(..) {
                                if discovery_tx.send(batch).await.is_err() {
                                    return; // Channel closed, exit task
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    // Main event loop
    loop {
        tokio::select! {
            biased;
            // Handle Ctrl+C
            _ = signal::ctrl_c() => {
                println!("\n\n=== Summary ===");
                println!("Total subscriptions: {}", subscriptions.len());
                println!("\nShutting down...");
                discovery_task.abort();
                break;
            }

            // Handle newly discovered paths
            Some(mut batch) = discovery_rx.next() => {
                let count = subscriptions.subscribe_to_paths(&subscriber, batch.drain(..), &updates_tx);
                if count > 0 {
                    println!("🔔 Discovered and subscribed to new sensor");
                }
            }

            // Handle incoming updates
            Some(mut batch) = updates_rx.next() => {
                for (sub_id, event) in batch.drain(..) {
                    if let Some(path) = subscriptions.get_path(&sub_id) {
                        match event {
                            Event::Update(value) => {
                                println!("{} = {}", path, format_value(&value));
                            }
                            Event::Unsubscribed => {
                                println!("❌ {} unsubscribed", path);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn format_value(value: &netidx_value::Value) -> String {
    use netidx_value::Value;
    match value {
        Value::F64(v) => format!("{:.1}", v),
        Value::Bool(true) => "true".to_string(),
        Value::Bool(false) => "false".to_string(),
        v => format!("{:?}", v),
    }
}

fn main() -> Result<()> {
    env_logger::init();
    Config::maybe_run_machine_local_resolver()?;

    // Get pattern from command line or use default
    let args: Vec<String> = env::args().collect();
    let pattern = if args.len() > 1 {
        args[1].clone()
    } else {
        println!("No pattern specified, using default: /local/building/**/temperature");
        println!("You can also try:");
        println!("  /local/building/**/humidity");
        println!("  /local/building/**/{{temperature,humidity}}");
        println!("  /local/building/floor2/**");
        println!("  /local/building/**/occupancy");
        println!();
        "/local/building/**/temperature".to_string()
    };

    tokio_main(Config::load_default_or_local_only()?, pattern)
}
