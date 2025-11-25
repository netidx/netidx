//! Channel Client Example
//!
//! This example demonstrates how to connect to a channel and exchange messages.
//!
//! First, start the channel server:
//! ```
//! cargo run --example channel_server
//! ```
//!
//! Then run this client:
//! ```
//! cargo run --example channel_client
//! ```

use anyhow::Result;
use netidx::{config::Config, path::Path, subscriber::SubscriberBuilder};
use netidx_protocols::channel::client::Connection;
use netidx_value::Value;
use tokio::time::Duration;

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    // Create a subscriber
    let subscriber = SubscriberBuilder::new(cfg).build()?;
    println!("Channel client started\n");

    let base = Path::from("/local/example/channel");

    // Connect to the channel
    println!("Connecting to {}...", base);
    let connection = Connection::connect(&subscriber, base).await?;
    println!("Connected!\n");

    // Receive the welcome message
    let welcome = connection.recv_one().await?;
    println!("Server says: {}\n", welcome);

    // Send some messages
    let messages = vec![
        "Hello from the client!",
        "How are you?",
        "This is message 3",
        "Testing bidirectional communication",
    ];

    for msg in messages {
        println!("Sending: {}", msg);
        connection.send(Value::from(msg))?;

        // Wait for the echo
        let response = connection.recv_one().await?;
        println!("Received: {}\n", response);

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Demonstrate batch sending
    println!("Sending a batch of numbers...");
    for i in 1..=5 {
        connection.send(Value::U64(i))?;
    }
    // Flush to ensure all messages are sent
    connection.flush().await?;

    // Receive the echoes
    for _ in 1..=5 {
        let response = connection.recv_one().await?;
        println!("Received: {}", response);
    }

    println!("\nSending quit message...");
    connection.send(Value::from("quit"))?;
    connection.flush().await?;

    let goodbye = connection.recv_one().await?;
    println!("Server says: {}", goodbye);

    println!("\nChannel communication completed successfully!");
    Ok(())
}

fn main() -> Result<()> {
    // init logging
    env_logger::init();
    // Load the netidx config
    tokio_main(Config::load_default_or_local_only()?)
}
