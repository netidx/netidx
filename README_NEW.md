[![Crates.io][crates-badge]][crates-url]
[![docs.rs][docs-badge]][docs-url]
[![MIT licensed][mit-badge]][mit-url]

[crates-badge]: https://img.shields.io/crates/v/netidx.svg?style=flat-square
[crates-url]: https://crates.io/crates/netidx
[docs-badge]: https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square
[docs-url]: https://docs.rs/netidx/latest/netidx/
[mit-badge]: https://img.shields.io/badge/license-MIT-lightgray.svg?style=flat-square
[mit-url]: https://github.com/netidx/netidx/blob/master/LICENSE

[Follow me on X](https://x.com/eestokesOSS) • [Join us on Discord](https://discord.gg/bQv4gNR8WK)

# Netidx

**Real-time data sharing for distributed systems, without the message broker.**

Netidx is a high-performance Rust middleware that lets you publish and
subscribe to live data across your network using a simple,
hierarchical namespace. Think of it as a distributed filesystem for
streaming data, where values update in real-time and programs can both
read and write.

## Why Netidx?

**🚀 Built for Performance**
- Direct TCP connections between publishers and subscribers—no central broker bottleneck
- Zero-copy where possible, efficient binary encoding everywhere else
- Capable of handling tens of millions of updates per second between a single publisher and subscriber

**🔐 Secure by Default**
- Built-in Kerberos v5 and TLS support with mutual authentication
- Centralized authorization policies
- Encryption for all network traffic

**🎯 Simple Mental Model**
- Everything is a path like `/sensors/temperature` or `/trading/prices/AAPL`
- Subscribe to get current value + live updates
- Write to publish or send commands
- Discover what exists by browsing the namespace

**🛠️ Production Ready**
- Used in financial trading systems for years
- Automatic reconnection and state recovery
- Integration with corporate auth systems (AD, Kerberos v5, etc)
- Tools for debugging and administration

## Quick Example

Publishing sensor data is just a few lines:

```rust
use netidx::{
    publisher::{Publisher, Value, BindCfg},
    config::Config, path::Path, resolver::Auth,
};
use tokio::time::{self, Duration};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::load_default()?;
    let publisher = PublisherBuilder::new(cfg)
        .desired_auth(DesiredAuth::Anonymous)
        .bind_cfg(Some("192.168.0.0/16".parse()?))
        .build()
        .await?;

    let temp = publisher.publish(
        Path::from("/sensors/lab/temperature"),
        get_temperature_reading().await
    )?;

    loop {
        time::sleep(Duration::from_secs(1)).await;
        let mut batch = publisher.start_batch();
        temp.update(&mut batch, get_temperature_reading().await);
        batch.commit(None).await;
    }
}
```

Subscribing is equally straightforward:

```rust
use netidx::{
    subscriber::{Subscriber, UpdatesFlags},
    config::Config, path::Path, resolver::Auth,
};
use futures::{prelude::*, channel::mpsc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::load_default()?;
    let subscriber = Subscriber::new(cfg, Auth::Anonymous)?;

    let temp = subscriber.subscribe(
        Path::from("/sensors/lab/temperature")
    ).await?;

    println!("Current temperature: {:?}", temp.last());

    let (tx, mut rx) = mpsc::channel(10);
    temp.updates(UpdatesFlags::empty(), tx);

    while let Some(batch) = rx.next().await {
        for (_, value) in batch {
            println!("Temperature updated: {:?}", value);
        }
    }
    Ok(())
}
```

## What Can You Build?

Netidx shines in scenarios where you need:

- **Live dashboards** - Monitor systems in real-time without polling
- **Distributed control systems** - Read sensor data, issue commands, all with the same simple API
- **Trading systems** - Low-latency market data distribution (our original use case)
- **IoT networks** - Hierarchically organized device data with discovery
- **Microservice coordination** - Share configuration, feature flags, and metrics
- **Process monitoring** - Expose application internals for debugging and observability

## Key Features

- **Hierarchical namespace** - Organize data naturally like a filesystem
- **Bi-directional** - Both pub/sub and request/reply patterns work seamlessly
- **Type-safe values** - Rich built-in types (primitives, strings, bytes, arrays, maps, datetimes)
- **User Defined Types** - Publish your own custom types by implementing a few traits
- **Efficient** - Direct TCP, connection pooling, batching, zero-copy where possible
- **Durable subscriptions** - Automatic reconnection with state recovery
- **Powerful queries** - Glob patterns, structural queries, browsing
- **Enterprise auth** - Kerberos v5 (including Active Directory) and TLS
- **Batteries included** - CLI tools, archiving, RPC framework, browser GUI

## Getting Started

1. **Install the tools:**
   ```bash
   cargo install netidx-tools
   ```

2. **Start a resolver server** (like DNS for netidx):
   ```bash
   netidx resolver-server --config /path/to/resolver.json
   ```

3. **Explore with the browser:**
   ```bash
   netidx browser
   ```

4. **Add to your project:**
   ```bash
   cargo add netidx
   ```

See the [netidx book](https://netidx.github.io/netidx-book/) for complete documentation, tutorials, and deployment guides.

## How It Works

Netidx has three components:

1. **Resolver Server** - Central directory mapping paths to publisher addresses (not data)
2. **Publishers** - Programs that create values and accept subscriptions
3. **Subscribers** - Programs that read values and receive live updates

```
┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│  Subscriber  │─────▶│   Resolver   │◀─────│  Publisher   │
│              │       │    Server    │       │              │
└──────────────┘       └──────────────┘       └──────────────┘
        │                                            │
        │          Direct TCP Connection             │
        └────────────────────────────────────────────┘
                    (data flows here)
```

Unlike traditional message brokers, **the resolver only stores
addresses**. Data flows directly from publishers to subscribers over
TCP, eliminating the broker as a bottleneck and single point of
failure. The resolver can be replicated and is federated to support
fault tolerance, load, and site autonomy.

## When Should You Use Netidx?

**Great fit:**
- You need low-latency live data distribution
- You want to browse/discover what data exists
- You're building systems (not just passing messages)
- You care about type safety and Rust's guarantees
- You need built-in security and authorization

**Maybe not:**
- You want complex event processing/filtering (the network is not a database)
- You need exactly-once processing guarantees (we provide reliable streaming with at-most-once or at-least-once depending on durability configuration)
- You're looking for simple point-to-point RPC (just use gRPC)

## Optional Components

**netidx-container** - NoSQL database (Redis-like)

Need persistence and guaranteed delivery? **netidx-container**
provides Redis-like NoSQL storage that integrates seamlessly with the
rest of the netidx ecosystem:

- **Key-value storage** with persistence to disk
- **Guaranteed delivery** even through crashes and restarts
- **Live updates** - changes propagate in real-time to subscribers

The key difference from Redis or MQTT: it's **not a required central component**. You can:
- Use direct peer-to-peer pub/sub where you need low latency
- Route through netidx-container where you need durability
- Mix both approaches in the same system, using the same API

This architectural flexibility means you're not forced to choose
between performance and reliability—you can have both where you need
them.

**netidx-archive** - Time-series event logging and replay

Need to record and replay historical data? **netidx-archive** provides high-performance event logging with smart storage management:

- **Network Event logging** - Capture all updates to subscribed paths to disk
- **Event logging at the source** - Use the netidx-archive crate directly in your publisher for the cleanest possible historical record
- **On-demand replay** - Replay historical data for any time range
- **Tail the stream** - Start where you left off after an interruption and continue following the event stream when you catch up
- **Transparent compression** - Automatic compression reduces storage costs
- **Optional tiered storage** - Configure an storage hierarchy (e.g., flash for recent data, S3 for deep archive)

Real-world example: Market data logging with 2TB of local flash for
frequently accessed periods, automatically going out to S3 when users
request historical data.

## Comparison to Alternatives

| Feature | Netidx | MQTT | Redis Pub/Sub | gRPC |
|---------|--------|------|---------------|------|
| Broker required | No* | Yes | Yes | No |
| Hierarchical namespace | Yes | Yes (topics) | Limited | No |
| Discovery/browsing | Yes | No | No | Via reflection |
| Bi-directional | Yes | No | No | Yes (streaming) |
| Guaranteed delivery | Optional* | Yes | No | No |
| Persistence | Optional* | Depends | Yes | No |
| Authorization | Central | Broker-level | Broker-level | Custom |
| Type system | Rich | Opaque | Limited | Protobuf |
| Primary use case | Live data systems | IoT messaging | Caching + pubsub | RPC |

**\*** Use **netidx-container** and **netidx-archive** when you need
guaranteed delivery and persistence—but unlike MQTT/Redis, it's not
required for basic pub/sub.

**vs MQTT**: No mandatory broker bottleneck, richer types, bi-directional, better discovery. Add persistence only where needed.

**vs Redis**: Direct connections for low latency, hierarchical organization, stronger authorization. Use netidx-container for Redis-like storage without making it central to your architecture.

**vs gRPC**: Pub/sub is native (not bolted-on streaming), namespace discovery, connection pooling, optimized for live updates.

## License

MIT - see [LICENSE](LICENSE) for details.

## Community

- **Documentation**: [netidx book](https://netidx.github.io/netidx-book/)
- **API docs**: [docs.rs/netidx](https://docs.rs/netidx)
- **Discord**: [Join the conversation](https://discord.gg/bQv4gNR8WK)
- **X/Twitter**: [@eestokesOSS](https://x.com/eestokesOSS)

## Contributing

Contributions welcome! Netidx has been production-tested for years but
we're always looking to improve. Check out the issues or jump into
Discord to discuss ideas.
