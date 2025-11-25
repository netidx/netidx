//! RPC Client Example
//!
//! This example demonstrates how to call RPC procedures using netidx-protocols.
//!
//! First, start the RPC server:
//! ```
//! cargo run --example rpc_server
//! ```
//!
//! Then run this client:
//! ```
//! cargo run --example rpc_client
//! ```

#[macro_use]
extern crate netidx_protocols;

use anyhow::Result;
use netidx::{config::Config, path::Path, subscriber::SubscriberBuilder};
use netidx_protocols::rpc::client::Proc;

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    // Create a subscriber
    let subscriber = SubscriberBuilder::new(cfg).build()?;
    println!("RPC client started\n");

    let base = Path::from("/local/example/rpc");

    // Subscribe to the echo procedure
    let echo = Proc::new(&subscriber, base.join("echo"))?;
    println!("Calling echo RPC...");
    let result = call_rpc!(echo, message: "Hello from RPC client!").await?;
    println!("Echo result: {}\n", result);

    // Subscribe to the add procedure
    let add = Proc::new(&subscriber, base.join("add"))?;
    println!("Calling add RPC...");
    let result = call_rpc!(add, a: 42.0, b: 13.5).await?;
    println!("Add result: {}\n", result);

    // Subscribe to the process procedure
    let process = Proc::new(&subscriber, base.join("process"))?;
    println!("Calling process RPC...");
    let result = call_rpc!(process, text: "make this uppercase").await?;
    println!("Process result: {}\n", result);

    // Call multiple times to show RPCs can be reused
    println!("Calling echo again...");
    let result = call_rpc!(echo, message: "Second call works too!").await?;
    println!("Echo result: {}\n", result);

    println!("All RPC calls completed successfully!");
    Ok(())
}

fn main() -> Result<()> {
    // init logging
    env_logger::init();
    // Load the netidx config from the default location, or use the zero config
    // machine local resolver config
    tokio_main(Config::load_default_or_local_only()?)
}
