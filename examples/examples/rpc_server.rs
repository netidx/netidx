//! RPC Server Example
//!
//! This example demonstrates how to create RPC procedures using netidx-protocols.
//! It publishes several RPC procedures that clients can call.
//!
//! Run this with:
//! ```
//! cargo run --example rpc_server
//! ```
//!
//! Then in another terminal, run the client:
//! ```
//! cargo run --example rpc_client
//! ```

#[macro_use]
extern crate netidx_protocols;

use anyhow::Result;
use arcstr::ArcStr;
use futures::{channel::mpsc, StreamExt};
use netidx::{config::Config, path::Path, publisher::PublisherBuilder};
use netidx_protocols::rpc::server::{ArgSpec, Proc, RpcCall};
use netidx_value::Value;
use tokio::signal;

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    // Create a publisher with default auth
    let publisher = PublisherBuilder::new(cfg).build().await?;
    println!("RPC server started");

    let base = Path::from("/local/example/rpc");

    // Define a simple echo procedure that returns its argument
    let _echo = define_rpc!(
        &publisher,
        base.join("echo"),
        "Echo the argument back to the caller",
        |mut c: RpcCall, message: String| -> Option<()> {
            println!("Echo called with: {}", message);
            c.reply.send(Value::from(message));
            None
        },
        None,
        message: String = "".to_string(); "The message to echo"
    )?;

    // Define an add procedure that adds two numbers
    let _add = define_rpc!(
        &publisher,
        base.join("add"),
        "Add two numbers together",
        |mut c: RpcCall, a: f64, b: f64| -> Option<()> {
            let result = a + b;
            println!("Add called: {} + {} = {}", a, b, result);
            c.reply.send(Value::F64(result));
            None
        },
        None,
        a: f64 = 0.0; "First number",
        b: f64 = 0.0; "Second number"
    )?;

    // Define a procedure that uses async processing via a channel
    let (tx, mut rx) = mpsc::channel(10);
    let _process = define_rpc!(
        &publisher,
        base.join("process"),
        "Process a string asynchronously",
        |c: RpcCall, text: String| -> Option<(RpcCall, String)> {
            println!("Process called with: {}", text);
            Some((c, text))
        },
        Some(tx),
        text: String = "".to_string(); "Text to process"
    )?;

    // Spawn async task to handle the process RPC
    tokio::spawn(async move {
        while let Some((mut call, text)) = rx.next().await {
            // Simulate async processing
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            let processed = text.to_uppercase();
            println!("Processed '{}' -> '{}'", text, processed);
            call.reply.send(Value::from(processed));
        }
    });

    println!("RPC procedures available at:");
    println!("  {}/echo", base);
    println!("  {}/add", base);
    println!("  {}/process", base);
    println!("\nPress Ctrl+C to exit");

    // Wait for Ctrl+C
    signal::ctrl_c().await?;
    println!("\nShutting down...");
    Ok(())
}

fn main() -> Result<()> {
    // init logging
    env_logger::init();
    // maybe start the machine local resolver
    Config::maybe_run_machine_local_resolver()?;
    // Load the netidx config from the default location, or use the zero config
    // machine local resolver config
    tokio_main(Config::load_default_or_local_only()?)
}
