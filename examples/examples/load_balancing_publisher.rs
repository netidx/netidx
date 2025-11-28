//! Load Balancing Publisher Example
//!
//! Demonstrates automatic load balancing across multiple publishers.
//!
//! ## How Load Balancing Works
//!
//! When multiple publishers publish to the same paths:
//! - **Random selection**: Each subscriber randomly picks one publisher
//! - **Automatic distribution**: Load spreads evenly across all publishers
//! - **USE_EXISTING flag**: Ensures related values stay together
//!
//! ## USE_EXISTING Flag for Atomic Batches
//!
//! For data that must stay together (e.g., stock quotes), use `USE_EXISTING`:
//! - All fields of a client come from same publisher
//! - Guarantees consistency within atomic batches
//! - Different clients can still go to different publishers
//! - Overall load from many clients is still distributed
//!
//! ## Running
//!
//! Start multiple publishers (each gets a random ID):
//!
//! ```bash
//! # Terminal 1 - Publisher instance 1
//! cargo run --example load_balancing_publisher
//!
//! # Terminal 2 - Publisher instance 2
//! cargo run --example load_balancing_publisher
//!
//! # Terminal 3 - Publisher instance 3
//! cargo run --example load_balancing_publisher
//!
//! # Terminal 4+ - Start multiple subscribers to see load distribution
//! cargo run --example load_balancing_subscriber
//! ```

use anyhow::Result;
use netidx::{
    config::Config,
    path::Path,
    publisher::{PublishFlags, Publisher, PublisherBuilder, UpdateBatch, Val, Value},
};
use std::time::Duration;
use tokio::{signal, time};
use uuid::Uuid;

/// A stock with all its published fields
struct Stock {
    bid: Val,
    ask: Val,
    last: Val,
    volume: Val,
    _publisher_id: Val,
}

impl Stock {
    /// Create a new stock and publish all its fields with USE_EXISTING flag
    fn new(
        publisher: &Publisher,
        base: &Path,
        symbol: &str,
        publisher_id: &str,
    ) -> Result<Self> {
        let base = base.append(symbol);
        // Publish stock quote fields with USE_EXISTING flag
        // This ensures all fields for a given client come from the same publisher
        let flags = PublishFlags::USE_EXISTING;
        Ok(Stock {
            bid: publisher.publish_with_flags(
                flags,
                base.append("bid"),
                Value::F64(100.0),
            )?,
            ask: publisher.publish_with_flags(
                flags,
                base.append("ask"),
                Value::F64(100.5),
            )?,
            last: publisher.publish_with_flags(
                flags,
                base.append("last"),
                Value::F64(100.25),
            )?,
            volume: publisher.publish_with_flags(
                flags,
                base.append("volume"),
                Value::V64(1000000),
            )?,
            _publisher_id: publisher.publish_with_flags(
                flags,
                base.append("publisher_id"),
                Value::String(publisher_id.into()),
            )?,
        })
    }

    /// Update all fields of the stock with new market data
    fn update(&self, batch: &mut UpdateBatch, i: usize, counter: u64) {
        let base_price = 100.0 + (i as f64 * 50.0);
        let movement = ((counter + i as u64) as f64 * 0.1).sin() * 5.0;
        let vol = 1000000 + ((counter * (i as u64 + 1)) % 500000);
        let bid_price = base_price + movement - 0.25;
        let ask_price = base_price + movement + 0.25;
        let last_price = base_price + movement;

        self.bid.update(batch, Value::F64(bid_price));
        self.ask.update(batch, Value::F64(ask_price));
        self.last.update(batch, Value::F64(last_price));
        self.volume.update(batch, Value::V64(vol));
    }
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    // Generate unique publisher ID
    let publisher_id = Uuid::new_v4().to_string()[..8].to_string();

    println!("╔════════════════════════════════════════════════╗");
    println!("║  Load Balancing Publisher                      ║");
    println!("╚════════════════════════════════════════════════╝\n");
    let publisher = PublisherBuilder::new(cfg).build().await?;

    let base = Path::from("/local/quotes");

    // Stock symbols to publish
    let stocks = ["IBM", "AAPL", "MSFT", "GOOG", "TSLA"];

    println!("Publisher ID: {}\n", publisher_id);
    println!("Publishing stock quotes with USE_EXISTING flag:");
    println!("  - All fields of a stock use same connection");
    println!("  - Different stocks can use different publishers");
    println!("  - Load automatically balanced across publishers\n");

    let mut stocks_data = Vec::new();

    for symbol in &stocks {
        let stock = Stock::new(&publisher, &base, symbol, &publisher_id)?;
        println!("Published {symbol}");
        stocks_data.push(stock);
    }

    println!("\nSimulating market data updates (every 1s)...");
    println!("Watch subscribers connect to different publishers!\n");
    println!("Press Ctrl+C to stop\n");

    let mut counter = 0u64;
    let mut interval = time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => {
                println!("\n\nShutting down publisher {}...\n", publisher_id);
                break;
            }
            _ = interval.tick() => {
                counter += 1;

                let mut batch = publisher.start_batch();

                // Update each stock with simulated price movement
                for (i, stock) in stocks_data.iter().enumerate() {
                    stock.update(&mut batch, i, counter);
                }

                batch.commit(None).await;

                if counter % 10 == 0 {
                    println!(
                        "[Publisher {}] Update #{} - Market tick",
                        publisher_id, counter
                    );
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
