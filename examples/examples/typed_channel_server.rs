//! Typed Channel Server Example
//!
//! This example demonstrates how to create a bidirectional typed channel server
//! using netidx-protocols pack_channel. Typed channels use the Pack trait for
//! efficient, type-safe serialization instead of generic Value types.
//!
//! Run this with:
//! ```
//! cargo run --example typed_channel_server
//! ```
//!
//! Then in another terminal, run the client:
//! ```
//! cargo run --example typed_channel_client
//! ```

use anyhow::Result;
use netidx::{config::Config, path::Path, publisher::PublisherBuilder};
use netidx_derive::Pack;
use netidx_protocols::pack_channel::server::Listener;
use tokio::{signal, time::Duration};

// Define custom message types using the Pack derive macro
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

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    // Create a publisher
    let publisher = PublisherBuilder::new(cfg).build().await?;
    println!("Typed channel server started");

    let base = Path::from("/local/example/typed_channel");

    // Create a listener at the base path
    let mut listener =
        Listener::new(&publisher, Some(Duration::from_secs(30)), base.clone()).await?;

    println!("Typed channel listener available at: {}", base);
    println!("Waiting for connections...\n");

    // Spawn a task to accept connections
    tokio::spawn(async move {
        let mut client_count = 0;
        loop {
            match listener.accept().await {
                Ok(connection) => {
                    client_count += 1;
                    let client_id = client_count;
                    println!("Client {} connected", client_id);

                    // Spawn a task to handle this connection
                    tokio::spawn(async move {
                        let mut message_count = 0u64;

                        // Send welcome message
                        let welcome = ServerResponse::Welcome { client_id };
                        if let Err(e) = connection.send_one(&welcome).await {
                            eprintln!("Error sending welcome: {}", e);
                            return;
                        }

                        // Receive and process messages
                        loop {
                            match connection.recv_one::<ChatMessage>().await {
                                Ok(msg) => {
                                    message_count += 1;
                                    println!(
                                        "Client {} | {}: {}",
                                        client_id, msg.username, msg.content
                                    );

                                    // Check for quit message
                                    if msg.content == "quit" {
                                        println!(
                                            "Client {} requested disconnect",
                                            client_id
                                        );
                                        let _ = connection
                                            .send_one(&ServerResponse::Goodbye)
                                            .await;
                                        break;
                                    }

                                    // Check for stats request
                                    if msg.content == "stats" {
                                        let stats = ServerResponse::Stats {
                                            messages_received: message_count,
                                        };
                                        if let Err(e) = connection.send_one(&stats).await
                                        {
                                            eprintln!("Error sending stats: {}", e);
                                            break;
                                        }
                                        continue;
                                    }

                                    // Echo the message back
                                    let response = ServerResponse::Echo { message: msg };
                                    if let Err(e) = connection.send_one(&response).await {
                                        eprintln!("Error sending response: {}", e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    println!("Client {} disconnected: {}", client_id, e);
                                    break;
                                }
                            }
                        }
                        println!(
                            "Client {} connection closed ({} messages processed)",
                            client_id, message_count
                        );
                    });
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                }
            }
        }
    });

    println!("Press Ctrl+C to exit");
    signal::ctrl_c().await?;
    println!("\nShutting down...");
    Ok(())
}

fn main() -> Result<()> {
    // init logging
    env_logger::init();
    // maybe start the machine local resolver
    Config::maybe_run_machine_local_resolver()?;
    // Load the netidx config
    tokio_main(Config::load_default_or_local_only()?)
}
