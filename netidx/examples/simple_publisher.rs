//! Simple publisher example
//!
//! This example publishes a counter value that increments every second.
//!
//! Run this with:
//! ```
//! cargo run --example simple_publisher
//! ```
//!
//! Then in another terminal, run the subscriber:
//! ```
//! cargo run --example simple_subscriber
//! ```

use anyhow::Result;
use chrono::prelude::*;
use netidx::{
    config::Config,
    path::Path,
    publisher::{DesiredAuth, PublisherBuilder},
};
use tokio::time::{self, Duration};

#[tokio::main]
async fn main() -> Result<()> {
    // Load the netidx config from the default location
    let cfg = Config::load_default()?;
    // Create a publisher with anonymous auth (no security)
    let publisher =
        PublisherBuilder::new(cfg).desired_auth(DesiredAuth::Anonymous).build().await?;
    println!("Publisher started. Publishing to /local/example/counter");
    // by convention /local is always mapped to the local machine
    // Publish a counter value
    let counter = publisher.publish(Path::from("/local/example/counter"), 0)?;
    // Publish a timestamp
    let timestamp =
        publisher.publish(Path::from("/local/example/timestamp"), Utc::now())?;
    let mut count = 0u64;
    loop {
        time::sleep(Duration::from_secs(1)).await;
        let now = Utc::now();
        count += 1;
        // In netidx all updates are done in batches
        let mut batch = publisher.start_batch();
        counter.update(&mut batch, count);
        timestamp.update(&mut batch, now);
        // Updates are sent out to subscribers only when a batch is committed
        batch.commit(None).await;
        println!("Published: count={}, timestamp={}", count, now);
    }
}
