//! # Netidx - Real-time data sharing for distributed systems
//!
//! **High-performance pub/sub middleware without the message broker.**
//!
//! Netidx lets you publish and subscribe to live data across your network using a
//! simple, hierarchical namespace. Data flows directly between publishers and
//! subscribers over TCP—no central broker bottleneck.
//!
//! ## Why Netidx?
//!
//! - **Fast**: Direct connections, zero-copy where possible, handles tens of millions
//!   of updates/sec
//! - **Discoverable**: Hierarchical namespace like a filesystem, browse and query
//!   what exists
//! - **Flexible**: Optional persistence (netidx-container), event logging (netidx-archive)
//! - **Secure**: Built-in Kerberos v5 and TLS with centralized authorization
//! - **Production-tested**: Years in production in financial trading systems
//!
//! ## Quick Start
//!
//! Publishing data:
//!
//! ```no_run
//! # fn get_cpu_temp() -> f32 { 42. }
//! use netidx::{
//!     publisher::{PublisherBuilder, Value, BindCfg, DesiredAuth},
//!     config::Config, path::Path,
//! };
//! use tokio::time;
//! use std::time::Duration;
//! # use anyhow::Result;
//! # async fn run() -> Result<()> {
//! let cfg = Config::load_default()?;
//! let publisher = PublisherBuilder::new(cfg)
//!     .desired_auth(DesiredAuth::Anonymous)
//!     .bind_cfg(Some("192.168.0.0/16".parse()?))
//!     .build()
//!     .await?;
//!
//! let temp = publisher.publish(
//!     Path::from("/hw/cpu-temp"),
//!     get_cpu_temp()
//! )?;
//!
//! loop {
//!     time::sleep(Duration::from_millis(500)).await;
//!     let mut batch = publisher.start_batch();
//!     temp.update(&mut batch, get_cpu_temp());
//!     batch.commit(None).await;
//! }
//! # Ok(()) }
//! ```
//!
//! Subscribing to data:
//!
//! ```no_run
//! use netidx::{
//!     subscriber::{Subscriber, UpdatesFlags, DesiredAuth},
//!     config::Config, path::Path,
//! };
//! use futures::{prelude::*, channel::mpsc};
//! # use anyhow::Result;
//! # async fn run() -> Result<()> {
//! let cfg = Config::load_default()?;
//! let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous)?;
//! let temp = subscriber.subscribe(Path::from("/hw/cpu-temp"));
//! temp.wait_subscribed().await;
//!
//! println!("Current: {:?}", temp.last());
//!
//! let (tx, mut rx) = mpsc::channel(10);
//! temp.updates(UpdatesFlags::empty(), tx);
//! while let Some(mut batch) = rx.next().await {
//!     for (_, value) in batch.drain(..) {
//!         println!("Updated: {:?}", value);
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! ## How It Works
//!
//! Netidx has three components:
//!
//! - **Resolver Server**: Directory service mapping paths to publisher addresses
//! - **Publishers**: Create values and serve subscribers directly
//! - **Subscribers**: Connect directly to publishers for live data
//!
//! Unlike traditional message brokers, the resolver only stores *addresses*, not data.
//! This eliminates the broker as a bottleneck and single point of failure.
//!
//! ## Key Features
//!
//! - **Direct TCP connections** - No broker bottleneck, connection pooling
//! - **Type-safe values** - Rich types including primitives, strings, bytes, arrays, maps
//! - **User-defined types** - Publish your own custom types by implementing a few traits
//! - **Bi-directional** - Subscribers can write values back to publishers
//! - **Durable subscriptions** - Automatic reconnection and state recovery
//! - **Authorization** - Centralized permissions with Kerberos v5 or TLS
//! - **Discovery** - Browse namespace, glob patterns, structural queries
//!
//! ## Optional Components
//!
//! - **[netidx-container](https://docs.rs/netidx-container)** - Redis-like NoSQL storage
//!   for persistence and guaranteed delivery
//! - **[netidx-archive](https://docs.rs/netidx-archive)** - Event logging, replay, and
//!   tiered storage for historical data
//! - **[netidx-protocols](https://docs.rs/netidx-protocols)** - RPC framework and
//!   clustering primitives
//!
//! ## Documentation
//!
//! - [Netidx Book](https://netidx.github.io/netidx-book/) - Complete guide and tutorials
//! - [`Publisher`] - Publish data and accept subscriptions
//! - [`Subscriber`] - Subscribe to live data
//! - [`config::Config`] - Configuration management
//! - [`path::Path`] - Hierarchical path handling
//!
//! ## Architecture
//!
//! Values are transmitted as a reliable, ordered stream over TCP (like the connection
//! itself, but with [`publisher::Value`] as the unit instead of bytes). Published
//! values always have a current value that new subscribers receive immediately, followed
//! by live updates.
//!
//! For best discoverability, structure your data hierarchically with multiple published
//! values rather than complex nested structures. This is both efficient and makes your
//! system browsable.
#![recursion_limit = "1024"]
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate netidx_core;

pub use netidx_core::pack;
pub use netidx_core::path;
pub use netidx_core::utils;
pub use netidx_netproto as protocol;
pub use poolshark as pool;

mod batch_channel;
mod channel;
pub mod config;
mod os;
pub mod publisher;
pub mod resolver_client;
pub mod resolver_server;
pub mod subscriber;
#[cfg(test)]
mod test;
pub mod tls;

use anyhow::Result;
use publisher::{Publisher, PublisherBuilder};
use subscriber::{Subscriber, SubscriberBuilder};

/// A self-contained netidx setup for testing or standalone use.
///
/// This will allow the current program to talk to itself (and child processes) over netidx.
/// This is useful for tests, as well as a fallback of last resort when
/// there is no usable netidx config, but a program could still be
/// useful without netidx.
///
/// Note, when you drop this the internal resolver server will shut
/// down and your publisher/subscriber will no longer be usable.
pub struct InternalOnly {
    _resolver: resolver_server::Server,
    subscriber: Subscriber,
    publisher: Publisher,
    cfg: config::Config,
}

impl InternalOnly {
    /// pre built subscriber
    pub fn subscriber(&self) -> &Subscriber {
        &self.subscriber
    }

    /// pre built publisher
    pub fn publisher(&self) -> &Publisher {
        &self.publisher
    }

    /// the client configuration
    pub fn cfg(&self) -> config::Config {
        self.cfg.clone()
    }

    /// gracefully stop the system
    pub async fn shutdown(self) {
        self.publisher.shutdown().await;
        drop(self.subscriber);
    }

    async fn new_inner() -> Result<Self> {
        let resolver = {
            use resolver_server::config::{self, file};
            let cfg = file::ConfigBuilder::default()
                .member_servers(vec![file::MemberServerBuilder::default()
                    .auth(file::Auth::Anonymous)
                    .addr("127.0.0.1:0".parse()?)
                    .bind_addr("127.0.0.1".parse()?)
                    .build()?])
                .build()?;
            let cfg = config::Config::from_file(cfg)?;
            resolver_server::Server::new(cfg.clone(), false, 0).await?
        };
        let addr = *resolver.local_addr();
        let cfg = {
            use config::{self, file, DefaultAuthMech};
            let cfg = file::ConfigBuilder::default()
                .addrs(vec![(addr, file::Auth::Anonymous)])
                .default_auth(DefaultAuthMech::Anonymous)
                .default_bind_config("local")
                .build()?;
            config::Config::from_file(cfg)?
        };
        let publisher = PublisherBuilder::new(cfg.clone()).build().await?;
        let subscriber = SubscriberBuilder::new(cfg.clone()).build()?;
        Ok(Self { _resolver: resolver, publisher, subscriber, cfg })
    }

    #[cfg(unix)]
    pub async fn new() -> Result<Self> {
        Self::new_inner().await
    }

    // work around tokio not setting SO_REUSEADDR on windows
    #[cfg(windows)]
    pub async fn new() -> Result<Self> {
        use std::time::Duration;
        use tokio::time;
        let mut tries = 0;
        loop {
            match Self::new_inner().await {
                Ok(r) => break Ok(r),
                Err(e) if tries >= 3 => bail!(e),
                Err(_) => {
                    time::sleep(Duration::from_millis(500)).await;
                    tries += 1
                }
            }
        }
    }
}
