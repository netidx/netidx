//! Clustering Example
//!
//! This example demonstrates how to create a distributed cluster using netidx-protocols.
//! Cluster members discover each other, elect a primary, and exchange commands.
//!
//! Run multiple instances of this example to see clustering in action:
//! ```
//! # Terminal 1
//! cargo run --example clustering
//!
//! # Terminal 2
//! cargo run --example clustering
//!
//! # Terminal 3
//! cargo run --example clustering
//! ```
//!
//! The cluster requires at least 2 members to start operations (configurable via SHARDS).
//! Members will elect a primary, and you'll see commands being broadcast between members.

use anyhow::Result;
use netidx::{
    config::Config, path::Path, publisher::PublisherBuilder,
    subscriber::SubscriberBuilder,
};
use netidx_derive::Pack;
use netidx_protocols::cluster::Cluster;
use std::env;
use tokio::{
    signal,
    time::{self, Duration},
};

// Define the command type that cluster members will exchange
#[derive(Debug, Clone, Pack)]
enum ClusterCommand {
    Heartbeat { from: String, sequence: u64 },
    WorkRequest { task_id: u64, data: String },
    WorkComplete { task_id: u64, result: String },
}

#[tokio::main]
async fn tokio_main(cfg: Config) -> Result<()> {
    // Get the number of required shards from environment or use default
    let shards = env::var("SHARDS").ok().and_then(|s| s.parse().ok()).unwrap_or(2);

    println!("Starting cluster member...");
    println!("Waiting for {} other member(s) to join...", shards);

    // Create publisher and subscriber
    let publisher = PublisherBuilder::new(cfg.clone()).build().await?;
    let subscriber = SubscriberBuilder::new(cfg).build()?;

    // Create the cluster at a common base path
    let base = Path::from("/local/example/cluster");
    let mut cluster: Cluster<ClusterCommand> =
        Cluster::new(&publisher, subscriber, base, shards).await?;

    let our_path = cluster.path();
    println!("\n=== Cluster Member Ready ===");
    println!("Our path: {}", our_path);
    println!("Primary: {}", cluster.primary());
    println!("Other members: {}", cluster.others());
    println!("===========================\n");

    // Note: Heartbeats are sent in the main loop below

    // Main event loop
    let mut sequence = 0u64;
    let mut task_id = 0u64;
    let mut poll_members_interval = time::interval(Duration::from_secs(5));
    let mut heartbeat_interval = time::interval(Duration::from_secs(3));
    let mut work_interval = time::interval(Duration::from_secs(7));
    let mut cmd_buf = vec![];

    loop {
        tokio::select! {
            // Handle incoming commands from other cluster members
            cmds = cluster.wait_cmds_buf(&mut cmd_buf) => {
                match cmds {
                    Ok(()) => {
                        for cmd in cmd_buf.drain(..) {
                            match cmd {
                                ClusterCommand::Heartbeat { from, sequence } => {
                                    println!("[CMD] Heartbeat from {} (seq: {})", from, sequence);
                                }
                                ClusterCommand::WorkRequest { task_id, data } => {
                                    println!("[CMD] Work request {} received: {}", task_id, data);
                                    // Simulate some work
                                    let result = format!("Processed: {}", data.to_uppercase());
                                    // Send completion back
                                    cluster.send_cmd(&ClusterCommand::WorkComplete {
                                        task_id,
                                        result,
                                    });
                                }
                                ClusterCommand::WorkComplete { task_id, result } => {
                                    println!("[CMD] Work {} completed: {}", task_id, result);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error receiving commands: {}", e);
                        break;
                    }
                }
            }

            // Poll for new cluster members
            _ = poll_members_interval.tick() => {
                match cluster.poll_members().await {
                    Ok(changed) => {
                        if changed {
                            println!("\n=== Cluster Membership Changed ===");
                            println!("Primary: {}", cluster.primary());
                            println!("Other members: {}", cluster.others());
                            println!("==================================\n");
                        }
                    }
                    Err(e) => {
                        eprintln!("Error polling members: {}", e);
                    }
                }
            }

            // Send periodic heartbeats
            _ = heartbeat_interval.tick() => {
                sequence += 1;
                cluster.send_cmd(&ClusterCommand::Heartbeat {
                    from: format!("{}", our_path),
                    sequence,
                });
            }

            // Primary member sends work requests
            _ = work_interval.tick() => {
                if cluster.primary() {
                    task_id += 1;
                    let work = ClusterCommand::WorkRequest {
                        task_id,
                        data: format!("task-{}", task_id),
                    };
                    println!("[PRIMARY] Broadcasting work request {}", task_id);
                    cluster.send_cmd(&work);
                }
            }

            // Handle Ctrl+C
            _ = signal::ctrl_c() => {
                println!("\nShutting down cluster member...");
                break;
            }
        }
    }

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
