//! Typed Channel Client Example
//!
//! This example demonstrates how to connect to a typed channel and exchange
//! type-safe messages using the Pack trait.
//!
//! First, start the typed channel server:
//! ```
//! cargo run --example typed_channel_server
//! ```
//!
//! Then run this client:
//! ```
//! cargo run --example typed_channel_client
//! ```

use anyhow::Result;
use netidx::{config::Config, path::Path, subscriber::SubscriberBuilder};
use netidx_derive::Pack;
use netidx_protocols::pack_channel::client::Connection;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Duration;

// Define the same message types as the server
// In a real application, these would be in a shared library
#[derive(Debug, Clone, Pack)]
struct ChatMessage {
    username: String,
    content: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Pack)]
enum ServerResponse {
    Welcome { client_id: u64 },
    Echo { message: ChatMessage },
    Stats { messages_received: u64 },
    Goodbye,
}

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    // Create a subscriber
    let subscriber = SubscriberBuilder::new(cfg).build()?;
    println!("Typed channel client started\n");

    let base = Path::from("/local/example/typed_channel");

    // Connect to the typed channel
    println!("Connecting to {}...", base);
    let connection = Connection::connect(&subscriber, base).await?;
    println!("Connected!\n");

    // Receive the welcome message
    let welcome = connection.recv_one::<ServerResponse>().await?;
    match welcome {
        ServerResponse::Welcome { client_id } => {
            println!("Server assigned client ID: {}\n", client_id);
        }
        _ => {
            eprintln!("Unexpected welcome message: {:?}", welcome);
        }
    }

    // Send some chat messages
    let messages = vec![
        "Hello from typed channel client!",
        "This uses the Pack trait for efficient serialization",
        "Type safety is enforced at compile time",
    ];

    for content in messages {
        let msg = ChatMessage {
            username: "Alice".to_string(),
            content: content.to_string(),
            timestamp: current_timestamp(),
        };

        println!("Sending: {}", content);
        connection.send_one(&msg)?;

        // Receive the echo
        let response = connection.recv_one::<ServerResponse>().await?;
        match response {
            ServerResponse::Echo { message } => {
                println!("Server echoed: {}\n", message.content);
            }
            _ => {
                eprintln!("Unexpected response: {:?}", response);
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Request stats
    println!("Requesting stats...");
    let stats_msg = ChatMessage {
        username: "Alice".to_string(),
        content: "stats".to_string(),
        timestamp: current_timestamp(),
    };
    connection.send_one(&stats_msg)?;

    let response = connection.recv_one::<ServerResponse>().await?;
    match response {
        ServerResponse::Stats { messages_received } => {
            println!("Server stats: {} messages received\n", messages_received);
        }
        _ => {
            eprintln!("Unexpected response: {:?}", response);
        }
    }

    // Demonstrate batch sending
    println!("Sending a batch of messages...");
    let mut batch = connection.start_batch();
    for i in 1..=3 {
        let msg = ChatMessage {
            username: "Alice".to_string(),
            content: format!("Batch message {}", i),
            timestamp: current_timestamp(),
        };
        batch.queue(&msg)?;
    }
    connection.send(batch)?;
    connection.flush().await?;

    // Receive the batch echoes
    for _ in 1..=3 {
        let response = connection.recv_one::<ServerResponse>().await?;
        match response {
            ServerResponse::Echo { message } => {
                println!("Server echoed: {}", message.content);
            }
            _ => {
                eprintln!("Unexpected response: {:?}", response);
            }
        }
    }

    // Send quit message
    println!("\nSending quit message...");
    let quit_msg = ChatMessage {
        username: "Alice".to_string(),
        content: "quit".to_string(),
        timestamp: current_timestamp(),
    };
    connection.send_one(&quit_msg)?;

    let goodbye = connection.recv_one::<ServerResponse>().await?;
    match goodbye {
        ServerResponse::Goodbye => {
            println!("Server said goodbye");
        }
        _ => {
            eprintln!("Unexpected response: {:?}", goodbye);
        }
    }

    println!("\nTyped channel communication completed successfully!");
    Ok(())
}

fn main() -> Result<()> {
    // init logging
    env_logger::init();
    // Load the netidx config
    tokio_main(Config::load_default_or_local_only()?)
}
