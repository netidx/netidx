[![Crates.io][crates-badge]][crates-url]
[![docs.rs][docs-badge]][docs-url]
[![MIT licensed][mit-badge]][mit-url]

[crates-badge]: https://img.shields.io/crates/v/netidx.svg?style=flat-square
[crates-url]: https://crates.io/crates/netidx
[docs-badge]: https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square
[docs-url]: https://docs.rs/netidx/latest/netidx/
[mit-badge]: https://img.shields.io/badge/license-MIT-lightgray.svg?style=flat-square
[mit-url]: https://github.com/netidx/netidx/blob/master/LICENSE
[Follow me on X](https://x.com/eestokesOSS)

[Join us on Discord](https://discord.gg/bQv4gNR8WK)

# What is Netidx

Netidx is middleware that enables publishing a value, like 42, in one
program and consuming it in another program, either on the same
machine or across the network.

Values are given globally unique names in a hierarchical
namespace. For example our published 42 might be named
/the-ultimate-answer (normally we wouldn't put values directly under
the root, but in this case it's appropriate). Any other program on the
network can refer to 42 by that name, and will receive updates in the
(unlikely) event that /the-ultimate-answer changes.

## Comparison With Other Systems

- Like LDAP
  - Netidx keeps track of a hierarchical directory of values
  - Netidx is browsable and queryable to some extent
  - Netidx supports authentication, authorization, and encryption
  - Netidx values can be written as well as read.
  - Larger Netidx systems can be constructed by adding referrals
    between smaller systems. Resolver server clusters may have parents
    and children.

- Unlike LDAP
  - In Netidx the resolver server (like slapd) only keeps the location
    of the publisher that has the data, not the data iself.
  - There are no 'entries', 'attributes', 'ldif records', etc. Every
    name in the system is either structural, or a single value. Entry
    like structure is created using hierarchy. As a result there is
    also no schema checking.
  - One can subscribe to a value, and will then be notified immediatly
    if it changes.
  - There are no global filters on data, e.g. you can't query for
    (&(cn=bob)(uid=foo)), because netidx isn't a database. Whether and
    what query mechanisms exist are up to the publishers. You can,
    however, query the structure, e.g. /foo/**/bar would return any
    path under foo that ends in bar.

- Like MQTT
  - Netidx values are publish/subscribe
  - A single Netidx value may have multiple subscribers
  - All Netidx subscribers receive an update when a value they are
    subscribed to changes.
  - Netidx Message delivery is reliable and ordered.

- Unlike MQTT
  - In Netidx there is no centralized message broker. Messages flow
    directly over TCP from the publishers to the subscribers. The
    resolver server only stores the address of the publisher/s
    publishing a value.

For more details see the [netidx book](https://netidx.github.io/netidx-book/)

Here is an example service that publishes a cpu temperature, along
with the corresponding subscriber that consumes the data.

# Publisher
```rust
use netidx::{
    publisher::{Publisher, Value, BindCfg},
    config::Config,
    resolver::Auth,
    path::Path,
};
use tokio::time;
use std::time::Duration;
use anyhow::Result;

fn get_cpu_temp() -> f32 { 42. }

async fn run() -> Result<()> {
    // load the site cluster config. You can also just use a file.
    let cfg = Config::load_default()?;

    // no authentication (kerberos v5 or TLS are other options)
    // listen on any unique address matching 192.168.0.0/16
    let publisher = Publisher::new(cfg, Auth::Anonymous, "192.168.0.0/16".parse()?).await?;

    let temp = publisher.publish(
        Path::from("/hw/washu-chan/cpu-temp"),
        Value::F32(get_cpu_temp())
    )?;

    loop {
        time::sleep(Duration::from_millis(500)).await;
        let mut batch = publisher.start_batch();
        temp.update(&mut batch, Value::F32(get_cpu_temp()));
        batch.commit(None).await;
    }
    Ok(())
}
```

# Subscriber
```rust
use netidx::{
    subscriber::{Subscriber, UpdatesFlags},
    config::Config,
    resolver::Auth,
    path::Path,
};
use futures::{prelude::*, channel::mpsc};
use anyhow::Result;

async fn run() -> Result<()> {
    let cfg = Config::load_default()?;
    let subscriber = Subscriber::new(cfg, Auth::Anonymous)?;
    let path = Path::from("/hw/washu-chan/cpu-temp");
    let temp = subscriber.subscribe_one(path, None).await?;
    println!("washu-chan cpu temp is: {:?}", temp.last());

    let (tx, mut rx) = mpsc::channel(10);
    temp.updates(UpdatesFlags::empty(), tx);
    while let Some(mut batch) = rx.next().await {
        for (_, v) in batch.drain(..) {
            println!("washu-chan cpu temp is: {:?}", v);
        }
    }
    Ok(())
}
```

Published things always have a value, which new subscribers receive
initially. Thereafter a subscription is a lossless ordered stream,
just like a tcp connection, except that instead of bytes
`publisher::Value` is the unit of transmission. Since the subscriber
can write values back to the publisher, the connection is
bidirectional, also like a Tcp stream.

Values include many useful primitives, including zero copy bytes
buffers (using the awesome bytes crate), so you can easily use
netidx to efficiently send any kind of message you like. However
it's advised to stick to primitives and express structure with
multiple published values in a hierarchy, since this makes your
system more discoverable, and is also quite efficient.

netidx includes optional support for kerberos v5 (including Active
Directory), and TLS. If enabled, all components will do mutual
authentication between the resolver, subscriber, and publisher as
well as encryption of all data on the wire.

In krb5 mode the resolver server maintains and enforces a set of
authorization permissions for the entire namespace. The system
administrator can centrally enforce who can publish where, and who
can subscribe to what.
