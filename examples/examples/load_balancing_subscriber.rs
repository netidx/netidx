//! Load Balancing Subscriber Example
//!
//! Demonstrates how subscribers are automatically distributed across publishers.
//!
//! ## What You'll See
//!
//! - Each subscriber randomly picks ONE publisher on first subscription
//! - USE_EXISTING ensures all subsequent subscriptions use that same publisher
//! - All stocks come from the same publisher (single connection per subscriber)
//! - Load balancing happens because different subscribers pick different publishers
//!
//! ## Running
//!
//! Start this after starting multiple publishers:
//!
//! ```bash
//! # Start 3+ publishers first
//! cargo run --example load_balancing_publisher  # In 3+ terminals
//!
//! # Then start multiple subscribers
//! cargo run --example load_balancing_subscriber  # Run this multiple times
//! ```
//!
//! Run multiple instances to see how different subscribers connect to
//! different publishers, demonstrating automatic load balancing.

use anyhow::Result;
use futures::{channel::mpsc, StreamExt};
use netidx::{
    config::Config,
    path::Path,
    subscriber::{
        Dval, Event, SubId, Subscriber, SubscriberBuilder, UpdatesFlags, Value,
    },
};
use poolshark::global::GPooled;
use std::collections::HashMap;
use tokio::signal;

/// A stock subscription with all its fields
struct Stock {
    symbol: &'static str,
    bid: Dval,
    ask: Dval,
    last: Dval,
    volume: Dval,
    publisher_id: Dval,
}

impl Stock {
    /// Subscribe to all fields of a stock
    fn new(
        subscriber: &Subscriber,
        base: &Path,
        symbol: &'static str,
        tx: mpsc::Sender<GPooled<Vec<(SubId, Event)>>>,
    ) -> Self {
        let stock_base = base.append(symbol);
        Stock {
            symbol,
            bid: subscriber.subscribe_updates(
                stock_base.append("bid"),
                [(UpdatesFlags::empty(), tx.clone())],
            ),
            ask: subscriber.subscribe_updates(
                stock_base.append("ask"),
                [(UpdatesFlags::empty(), tx.clone())],
            ),
            last: subscriber.subscribe_updates(
                stock_base.append("last"),
                [(UpdatesFlags::empty(), tx.clone())],
            ),
            volume: subscriber.subscribe_updates(
                stock_base.append("volume"),
                [(UpdatesFlags::empty(), tx.clone())],
            ),
            publisher_id: subscriber.subscribe_updates(
                stock_base.append("publisher_id"),
                [(UpdatesFlags::empty(), tx.clone())],
            ),
        }
    }

    /// Wait for all subscriptions to complete
    async fn wait_subscribed(&self) -> Result<()> {
        tokio::try_join![
            self.bid.wait_subscribed(),
            self.ask.wait_subscribed(),
            self.last.wait_subscribed(),
            self.volume.wait_subscribed(),
            self.publisher_id.wait_subscribed()
        ]?;
        Ok(())
    }

    /// Get the publisher ID this stock is connected to
    fn publisher(&self) -> Option<Value> {
        if let Event::Update(value) = self.publisher_id.last() {
            Some(value)
        } else {
            None
        }
    }

    /// Get a snapshot of current market data
    fn print_snapshot(&self) {
        if let Event::Update(bid) = self.bid.last()
            && let Event::Update(ask) = self.ask.last()
            && let Event::Update(last) = self.last.last()
            && let Event::Update(volume) = self.volume.last()
            && let Ok(bid) = bid.cast_to::<f64>()
            && let Ok(ask) = ask.cast_to::<f64>()
            && let Ok(last) = last.cast_to::<f64>()
        {
            println!(
                "  {} | Bid: {:7.2} Ask: {:7.2} Last: {:7.2} Vol: {}",
                self.symbol, bid, ask, last, volume
            )
        }
    }

    /// Check if this subscription ID is the publisher_id for this stock
    fn is_publisher_id(&self, id: SubId) -> bool {
        id == self.publisher_id.id()
    }
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    println!("╔════════════════════════════════════════════════╗");
    println!("║  Load Balancing Subscriber                     ║");
    println!("╚════════════════════════════════════════════════╝\n");
    let subscriber = SubscriberBuilder::new(cfg).build()?;
    let base = Path::from("/local/quotes");

    let stocks = ["IBM", "AAPL", "MSFT", "GOOG", "TSLA"];

    println!("Subscribing to stock quotes...\n");

    let (tx, mut rx) = mpsc::channel(100);
    let mut subscriptions = HashMap::new();

    for symbol in &stocks {
        let stock = Stock::new(&subscriber, &base, symbol, tx.clone());
        subscriptions.insert(symbol, stock);
    }

    // Wait for all subscriptions to complete
    println!("Waiting for subscriptions to connect...\n");
    for (symbol, stock) in &subscriptions {
        stock.wait_subscribed().await?;
        if let Some(pub_id) = stock.publisher() {
            println!("  {} → Publisher: {}", symbol, pub_id);
        }
    }

    println!("\n╔════════════════════════════════════════════════╗");
    println!("║  Load Balancing Active                        ║");
    println!("╚════════════════════════════════════════════════╝\n");

    println!("\nReceiving live market data...");
    println!("(Showing periodic snapshots)\n");

    let mut update_count = 0u64;

    loop {
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => {
                println!("\n\nShutting down subscriber...\n");
                break;
            }
            Some(mut batch) = rx.next() => {
                update_count += 1;

                // Show periodic snapshots
                if update_count % 10 == 0 {
                    println!("=== Market Snapshot ===");
                    for symbol in &stocks {
                        if let Some(stock) = subscriptions.get(symbol) {
                            stock.print_snapshot()
                        }
                    }
                    println!();
                }

                // Detect publisher changes (shouldn't happen unless publisher dies)
                for (id, event) in batch.drain(..) {
                    for (symbol, stock) in &subscriptions {
                        if stock.is_publisher_id(id) {
                            if let Event::Update(ref new_pub) = event {
                                println!("🔄 {} using publisher: {}", symbol, new_pub);
                            } else if matches!(event, Event::Unsubscribed) {
                                println!("⚠️  {} publisher disconnected, retrying...", symbol);
                            }
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
