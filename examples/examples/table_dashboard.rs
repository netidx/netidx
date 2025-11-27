//! Server Fleet Dashboard - Table Visualization Example
//!
//! This example demonstrates how to publish data in a table structure that
//! the netidx browser can automatically detect and visualize as a table.
//!
//! ## Table Pattern
//!
//! The browser detects tables using the path pattern: `/root/row/column`
//!
//! This example publishes server metrics at:
//! `/local/servers/fleet/{server_name}/{metric}`
//!
//! Where:
//! - root = `/local/servers/fleet`
//! - row = server name (web-01, web-02, db-01, cache-01, api-01)
//! - column = metric name (hostname, status, cpu_percent, memory_mb, etc.)
//!
//! ## Running the Example
//!
//! 1. Start the publisher:
//! ```bash
//! cargo run --example table_dashboard
//! ```
//!
//! 2. View in the browser:
//! ```bash
//! netidx browser
//! ```
//! Navigate to `/local/servers/fleet` to see the table view.
//!
//! 3. Or query the table structure:
//! ```bash
//! netidx resolver table /local/servers/fleet
//! ```
//!
//! ## What You'll See
//!
//! The browser will automatically display a table with:
//! - **Rows**: 5 servers (web-01, web-02, db-01, cache-01, api-01)
//! - **Columns**: 7 metrics (hostname, status, cpu_percent, memory_mb, disk_gb, uptime_hrs, last_updated)
//! - **Updates**: Metrics update every 2 seconds with simulated data
//!
//! ## Table Detection
//!
//! The resolver's `table()` method analyzes the path structure:
//! 1. Lists all paths under `/local/servers/fleet`
//! 2. Identifies rows as immediate children (depth 1)
//! 3. Identifies columns as grandchildren (depth 2)
//! 4. Returns a Table descriptor with rows and column counts

use anyhow::Result;
use chrono::Utc;
use netidx::{
    config::Config,
    path::Path,
    publisher::{Publisher, PublisherBuilder, UpdateBatch, Val, Value},
};
use rand::random_range;
use tokio::{
    signal,
    time::{self, Duration},
};

/// Helper struct for publishing values with change tracking
struct Pv<T> {
    cur: T,
    val: Val,
}

impl<T: Into<Value> + Clone> Pv<T> {
    fn new(publisher: &Publisher, path: Path, v: T) -> Result<Self> {
        let val = publisher.publish(path, v.clone())?;
        Ok(Self { cur: v, val })
    }

    fn update(&mut self, batch: &mut UpdateBatch, v: T) {
        self.cur = v.clone();
        self.val.update_changed(batch, v);
    }

    fn update_with<F: FnOnce(&T) -> T>(&mut self, batch: &mut UpdateBatch, f: F) {
        self.cur = f(&self.cur);
        self.val.update_changed(batch, self.cur.clone());
    }
}

/// Represents a single server with all its metrics
struct Server {
    name: String,
    _hostname: Pv<String>,
    status: Pv<String>,
    cpu_percent: Pv<f64>,
    cpu_base: f64, // Base CPU for simulation
    memory_mb: Pv<u64>,
    disk_gb: Pv<u64>,
    uptime_hrs: Pv<u64>,
    last_updated: Pv<String>,
}

impl Server {
    fn new(
        publisher: &Publisher,
        base: &Path,
        name: &str,
        hostname: &str,
        cpu_base: f64,
    ) -> Result<Self> {
        let server_path = base.join(name);

        // Publish all metrics following the /root/row/column pattern
        Ok(Server {
            name: name.to_string(),
            _hostname: Pv::new(
                publisher,
                server_path.join("hostname"),
                hostname.to_string(),
            )?,
            status: Pv::new(
                publisher,
                server_path.join("status"),
                "running".to_string(),
            )?,
            cpu_percent: Pv::new(publisher, server_path.join("cpu_percent"), cpu_base)?,
            cpu_base,
            memory_mb: Pv::new(
                publisher,
                server_path.join("memory_mb"),
                random_range(2000..8000),
            )?,
            disk_gb: Pv::new(
                publisher,
                server_path.join("disk_gb"),
                random_range(100..500),
            )?,
            uptime_hrs: Pv::new(
                publisher,
                server_path.join("uptime_hrs"),
                random_range(1..720),
            )?,
            last_updated: Pv::new(
                publisher,
                server_path.join("last_updated"),
                Utc::now().to_rfc3339(),
            )?,
        })
    }

    fn update(&mut self, batch: &mut UpdateBatch) {
        // Simulate CPU variation (random walk around base)
        let cpu_delta = random_range(-5.0..5.0);
        let new_cpu = (self.cpu_base + cpu_delta).clamp(0.0, 100.0);
        self.cpu_percent.update(batch, new_cpu);

        // Update status based on CPU
        let new_status =
            if new_cpu > 80.0 { "degraded".to_string() } else { "running".to_string() };
        self.status.update(batch, new_status);

        // Simulate memory fluctuation
        self.memory_mb.update_with(batch, |mem| {
            ((*mem as i64) + random_range(-50..50)).max(1000) as u64
        });

        // Simulate disk growth (logs growing over time)
        if random_range(0..100) < 5 {
            self.disk_gb.update_with(batch, |disk| disk + 1);
        }

        // Increment uptime
        self.uptime_hrs.update_with(batch, |hrs| hrs + 1);

        // Update timestamp
        self.last_updated.update(batch, Utc::now().to_rfc3339());

        // Update internal state for next iteration
        self.cpu_base = new_cpu;
    }
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    println!("╔════════════════════════════════════════════════╗");
    println!("║  Server Fleet Dashboard                        ║");
    println!("║  Table Visualization Example                   ║");
    println!("╚════════════════════════════════════════════════╝\n");

    let base = Path::from("/local/servers/fleet");
    let publisher = PublisherBuilder::new(cfg).build().await?;

    println!("Creating server fleet dashboard...\n");

    // Create fleet of 5 servers with different characteristics
    let mut servers = vec![
        Server::new(&publisher, &base, "web-01", "web-01.example.com", 35.0)?,
        Server::new(&publisher, &base, "web-02", "web-02.example.com", 42.0)?,
        Server::new(&publisher, &base, "db-01", "db-01.example.com", 68.0)?,
        Server::new(&publisher, &base, "cache-01", "cache-01.example.com", 28.0)?,
        Server::new(&publisher, &base, "api-01", "api-01.example.com", 51.0)?,
    ];

    println!("✓ Published {} servers", servers.len());
    println!("\n=== Table Structure ===");
    println!("Root: {}", base);
    println!("Rows (servers): web-01, web-02, db-01, cache-01, api-01");
    println!(
        "Columns (metrics): hostname, status, cpu_percent, memory_mb, disk_gb, uptime_hrs, last_updated"
    );

    println!("\n=== Viewing the Table ===");
    println!("Option 1 - Browser:");
    println!("  netidx browser");
    println!("  (Navigate to {})", base);
    println!("\nOption 2 - Command line:");
    println!("  netidx resolver table {}", base);

    println!("\n=== Publishing Updates Every 2 Seconds ===");
    println!("Press Ctrl+C to exit\n");

    let mut update_interval = time::interval(Duration::from_secs(2));

    loop {
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => {
                println!("\n\nShutting down...");
                break;
            }
            _ = update_interval.tick() => {
                // Update all servers
                let mut batch = publisher.start_batch();
                for server in &mut servers {
                    server.update(&mut batch);
                }
                batch.commit(None).await;

                // Print status summary
                print!("Status: ");
                for server in &servers {
                    let indicator = if server.cpu_percent.cur > 80.0 {
                        "⚠"
                    } else {
                        "✓"
                    };
                    print!("{} {}  ", indicator, server.name);
                }
                println!();
            }
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    // init logging
    env_logger::init();
    // start the machine local resolver if necessary
    Config::maybe_run_machine_local_resolver()?;
    // go!
    tokio_main(Config::load_default_or_local_only()?)
}
