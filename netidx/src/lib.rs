//! What is netidx?
//!
//! - It's a directory service; like LDAP or X.500
//!   - It keeps track of a hierarchical directory of things
//!   - It's browsable and queryable
//!   - It's distributed, lightweight, and scalable
//!
//! - It's a tuple space; like JavaSpaces, zookeeper, or memcached
//!   - Except it's distributed. The directory server keeps track of
//!   where things are, publishers keep the data.
//!   - Each tuple is identified by a unique path in the directory
//!   server, and holds a flexible set of primitive data types
//!
//! - It's a publish/subscribe messaging system; like MQTT
//!   - Except there is no centralized broker. Communication happens
//!   directly between publishers and subscribers
//!   - Message archiving and other services provided by MQTT brokers
//!   can be provided by normal publishers, or omitted if they aren't
//!   needed
//!  - Decentralization allows it to scale to huge message rates
//!
//! For more details see the [netidx book](https://estokes.github.io/netidx-book/)
//!
//! Here is an example service that publishes a cpu temperature, along
//! with the corresponding subscriber that consumes the data.
//!
//! # Publisher
//! ```no_run
//! # fn get_cpu_temp() -> f32 { 42. }
//! use netidx::{
//!     publisher::{Publisher, Value, BindCfg},
//!     config::Config,
//!     resolver::Auth,
//!     path::Path,
//! };
//! use tokio::time;
//! use std::time::Duration;
//!
//! # use anyhow::Result;
//! # async fn run() -> Result<()> {
//! // load the site cluster config. You can also just use a file.
//! let cfg = Config::load_default()?;
//!
//! // no authentication (kerberos v5 is the other option)
//! // listen on any unique address matching 192.168.0.0/16
//! let publisher = Publisher::new(cfg, Auth::Anonymous, "192.168.0.0/16".parse()?).await?;
//!
//! let temp = publisher.publish(
//!     Path::from("/hw/washu-chan/cpu-temp"),
//!     Value::F32(get_cpu_temp())
//! )?;
//!
//! loop {
//!     time::sleep(Duration::from_millis(500)).await;
//!     let mut batch = publisher.start_batch();
//!     temp.update(&mut batch, Value::F32(get_cpu_temp()));
//!     batch.commit(None).await;
//! }
//! # Ok(())
//! # };
//! ```
//!
//! # Subscriber
//! ```no_run
//! use netidx::{
//!     subscriber::{Subscriber, UpdatesFlags},
//!     config::Config,
//!     resolver::Auth,
//!     path::Path,
//! };
//! use futures::{prelude::*, channel::mpsc};
//! # use anyhow::Result;
//!
//! # async fn run() -> Result<()> {
//! let cfg = Config::load_default()?;
//! let subscriber = Subscriber::new(cfg, Auth::Anonymous)?;
//! let path = Path::from("/hw/washu-chan/cpu-temp");
//! let temp = subscriber.subscribe_one(path, None).await?;
//! println!("washu-chan cpu temp is: {:?}", temp.last());
//!
//! let (tx, mut rx) = mpsc::channel(10);
//! temp.updates(UpdatesFlags::empty(), tx);
//! while let Some(mut batch) = rx.next().await {
//!     for (_, v) in batch.drain(..) {
//!         println!("washu-chan cpu temp is: {:?}", v);
//!     }
//! }
//! # Ok(())
//! # };
//! ```
//!
//! Published things always have a value, which new subscribers receive
//! initially. Thereafter a subscription is a lossless ordered stream,
//! just like a tcp connection, except that instead of bytes
//! `publisher::Value` is the unit of transmission. Since the subscriber
//! can write values back to the publisher, the connection is
//! bidirectional, also like a Tcp stream.
//!
//! Values include many useful primitives, including zero copy bytes
//! buffers (using the awesome bytes crate), so you can easily use
//! netidx to efficiently send any kind of message you like. However
//! it's advised to stick to primitives and express structure with
//! multiple published values in a hierarchy, since this makes your
//! system more discoverable, and is also quite efficient.
//!
//! netidx includes optional support for kerberos v5 (including Active
//! Directory). If enabled, all components will do mutual
//! authentication between the resolver, subscriber, and publisher as
//! well as encryption of all data on the wire.
//!
//! In krb5 mode the resolver server maintains and enforces a set of
//! authorization permissions for the entire namespace. The system
//! administrator can centrally enforce who can publish where, and who
//! can subscribe to what.
//!
//! * Publish with a [`Publisher`](publisher/struct.Publisher.html)
//! * Subscribe with a [`Subscriber`](subscriber/struct.Subscriber.html)
#![recursion_limit = "1024"]
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate bitflags;
#[macro_use] extern crate anyhow;
#[macro_use] extern crate netidx_core;

pub use netidx_core::{chars, pack, pool, path, utils};
pub use netidx_netproto as protocol;

mod batch_channel;
mod auth;
mod channel;
pub mod config;
mod os;
pub mod publisher;
pub mod resolver;
pub mod resolver_server;
mod resolver_single;
mod shard_resolver_store;
mod resolver_store;
mod secstore;
pub mod subscriber;
#[cfg(test)]
mod test;
