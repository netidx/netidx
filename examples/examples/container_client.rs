//! Container Client Example - CRUD Operations
//!
//! This example demonstrates container CRUD operations including:
//! - CREATE: Adding new rows and columns
//! - READ: Subscribing to data values
//! - UPDATE: Modifying existing data
//! - DELETE: Removing data
//!
//! ## Important: Container Atomicity Model
//!
//! The container provides per-operation atomicity via sled:
//! - ✅ Each individual operation (set_data, remove, etc.) is atomic
//! - ❌ NO multi-operation atomicity
//! - ❌ NO rollback if operations fail
//! - ✅ Durable storage via sled
//!
//! This is NOT like traditional ACID transactions!
//!
//! ## Running
//!
//! First start the server:
//! ```bash
//! cargo run --example container_server
//! ```
//!
//! Then run this client:
//! ```bash
//! cargo run --example container_client
//! ```

#[macro_use]
extern crate netidx_protocols;

use std::time::Duration;

use anyhow::Result;
use futures::{channel::mpsc, StreamExt};
use netidx::{
    config::Config,
    path::Path,
    subscriber::{Event, SubscriberBuilder, UpdatesFlags},
};
use netidx_protocols::rpc::client::Proc;
use netidx_value::{NakedValue, Value};
use tokio::{task, time};

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    println!("╔════════════════════════════════════════════════╗");
    println!("║  Container Client - CRUD Operations            ║");
    println!("╚════════════════════════════════════════════════╝\n");

    let subscriber = SubscriberBuilder::new(cfg).build()?;
    let base = Path::from("/local/employees");
    let rpc_base = base.join("rpcs");

    println!("=== 1. READ Operations ===\n");
    println!("Reading existing employee data via subscription...\n");

    // Subscribe to some employee data
    let (tx, mut rx) = mpsc::channel(10);
    let emp001_name = subscriber.subscribe_updates(
        base.join("directory/emp001/name"),
        [(UpdatesFlags::empty(), tx.clone())],
    );
    let emp001_salary = subscriber.subscribe_updates(
        base.join("directory/emp001/salary"),
        [(UpdatesFlags::empty(), tx.clone())],
    );

    // print changes to subscribed values
    task::spawn({
        let emp001_name = emp001_name.id();
        let emp001_salary = emp001_salary.id();
        async move {
            while let Some(mut batch) = rx.next().await {
                for (id, ev) in batch.drain(..) {
                    if id == emp001_name
                        && let Event::Update(value) = &ev
                    {
                        println!("  emp001 name: {}", value);
                    }
                    if id == emp001_salary
                        && let Event::Update(value) = &ev
                    {
                        println!("  emp001 salary: {}", NakedValue(value));
                    }
                }
            }
        }
    });

    // Wait for the server to start
    tokio::try_join![emp001_name.wait_subscribed(), emp001_salary.wait_subscribed()]?;

    println!("\n=== 2. CREATE Operations ===\n");

    // Get RPC procedures
    let set_employee = Proc::new(&subscriber, rpc_base.join("set-employee"))?;

    println!("Creating new employee emp004 (using atomic set-employee RPC)...");
    call_rpc!(
        set_employee,
        id: "emp004",
        name: "Dan Lee",
        email: "dan@company.com",
        department: "HR",
        salary: 68000u64,
        phone: "555-0003"
    )
    .await?;
    println!("✓ Created emp004\n");

    println!("Creating new employee emp005 (using atomic set-employee RPC)...");
    call_rpc!(
        set_employee,
        id: "emp005",
        name: "James Woods",
        email: "jimbo@company.com",
        department: "Facilities",
        salary: 90000u64,
        phone: "555-1212"
    )
    .await?;
    println!("✓ Created emp005\n");

    println!("Adding new column 'phone' to existing employees...");
    call_rpc!(
        set_employee,
        id: "emp001",
        phone: "555-0001"
    )
    .await?;
    call_rpc!(
        set_employee,
        id: "emp002",
        phone: "555-0002"
    )
    .await?;
    println!("✓ Added phone column\n");

    // NOTE: set-employee RPC is fully atomic, all fields succeed or
    // fail together, and there can be server side validation and
    // business logic. Individual writes or set_data calls are atomic
    // per-operation (no rollback between calls)

    println!("\n=== 3. UPDATE Operations ===\n");

    println!("Updating emp001 salary: 75000 -> 80000");
    println!("Writing directly to the subscription (no RPC needed)...");
    emp001_salary.write(Value::V64(80000));
    println!("✓ Write sent\n");
    time::sleep(Duration::from_millis(100)).await;

    println!("\n=== 4. DELETE Operations ===\n");

    let delete = Proc::new(&subscriber, rpc_base.join("delete"))?;

    println!("Deleting emp004/phone...");
    call_rpc!(delete, path: base.join("directory/emp004/phone")).await?;
    println!("✓ Deleted\n");

    println!("Deleting entire emp005 row...");
    let delete_subtree = Proc::new(&subscriber, rpc_base.join("delete-subtree"))?;
    call_rpc!(delete_subtree, path: base.join("directory/emp005")).await?;
    println!("✓ Deleted emp005 and all its columns\n");

    println!("\n╔════════════════════════════════════════════════╗");
    println!("║  All demonstrations complete!                  ║");
    println!("╚════════════════════════════════════════════════╝");

    Ok(())
}

fn main() -> Result<()> {
    // Init logging
    env_logger::init();
    // Start the machine-local resolver if needed
    Config::maybe_run_machine_local_resolver()?;
    // Run the client
    tokio_main(Config::load_default_or_local_only()?)
}
