//! Channel Server Example
//!
//! This example demonstrates how to create a bidirectional channel server using
//! netidx-protocols. Channels allow streaming communication between endpoints.
//!
//! Run this with:
//! ```
//! cargo run --example channel_server
//! ```
//!
//! Then in another terminal, run the client:
//! ```
//! cargo run --example channel_client
//! ```

use anyhow::Result;
use netidx::{config::Config, path::Path, publisher::PublisherBuilder};
use netidx_protocols::channel::server::Listener;
use netidx_value::Value;
use tokio::{signal, time::Duration};

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    // Create a publisher
    let publisher = PublisherBuilder::new(cfg).build().await?;
    println!("Channel server started");

    let base = Path::from("/local/example/channel");

    // Create a listener at the base path
    // The listener will accept connections from multiple clients
    let mut listener = Listener::new(&publisher, Some(Duration::from_secs(30)), base.clone()).await?;

    println!("Channel listener available at: {}", base);
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
                        // Send a welcome message
                        if let Err(e) = connection.send_one(Value::from(format!("Welcome, client {}!", client_id))).await {
                            eprintln!("Error sending welcome: {}", e);
                            return;
                        }

                        // Echo messages back to the client
                        loop {
                            match connection.recv_one().await {
                                Ok(msg) => {
                                    println!("Client {} sent: {}", client_id, msg);

                                    // Check for disconnect message
                                    if let Value::String(s) = &msg {
                                        if &**s == "quit" {
                                            println!("Client {} requested disconnect", client_id);
                                            let _ = connection.send_one(Value::from("Goodbye!")).await;
                                            break;
                                        }
                                    }

                                    // Echo the message back
                                    let response = Value::from(format!("Echo: {}", msg));
                                    if let Err(e) = connection.send_one(response).await {
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
                        println!("Client {} connection closed", client_id);
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
