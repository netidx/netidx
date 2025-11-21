# Netidx Examples

This directory contains examples demonstrating key netidx functionality.

## Examples

### Basic Examples

**[simple_publisher.rs](simple_publisher.rs)** - Basic publisher

Publishes a counter and timestamp that update every second.

```bash
cargo run --example simple_publisher
```

**[simple_subscriber.rs](simple_subscriber.rs)** - Basic subscriber

Subscribes to values published by `simple_publisher` and prints updates.

```bash
# In another terminal (after starting simple_publisher)
cargo run --example simple_subscriber
```

### Realistic Examples

**[temperature_monitor.rs](temperature_monitor.rs)** - Multi-sensor publisher

Simulates a temperature monitoring system with multiple sensors publishing data.

```bash
cargo run --example temperature_monitor
```

**[temperature_display.rs](temperature_display.rs)** - Multi-subscription subscriber

Displays temperature data from multiple sensors in real-time.

```bash
# In another terminal (after starting temperature_monitor)
cargo run --example temperature_display
```

### Advanced Examples

**[bidirectional.rs](bidirectional.rs)** - Writes and request/reply

Demonstrates bidirectional communication where subscribers can write values
back to publishers, enabling remote control and request/reply patterns.

```bash
cargo run --example bidirectional
```

## Example Patterns

### Publishing Values

```rust
let publisher = PublisherBuilder::new(cfg)
    .desired_auth(DesiredAuth::Anonymous)
    .build()
    .await?;

let val = publisher.publish(Path::from("/my/path"), Value::U64(42))?;

// Update in batches for efficiency
let mut batch = publisher.start_batch();
val.update(&mut batch, Value::U64(43));
batch.commit(None).await;
```

### Subscribing to Values

```rust
let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous)?;
let sub = subscriber.subscribe(Path::from("/my/path"));

// Wait for subscription to establish
sub.wait_subscribed().await?;

// Get current value
println!("Current: {:?}", sub.last());

// Register for updates
let (tx, mut rx) = mpsc::channel(10);
sub.updates(UpdatesFlags::empty(), tx);

while let Some(batch) = rx.next().await {
    for (sub_id, event) in batch {
        println!("Update: {:?}", event);
    }
}
```

### Writing Values

```rust
// Subscriber can write back to publisher
sub.write(Value::U64(100));

// Or request a reply
let reply = sub.write_with_recipt(Value::U64(100));
match reply.await {
    Ok(response) => println!("Got reply: {:?}", response),
    Err(_) => println!("No reply received"),
}
```

### Publisher Handling Writes

```rust
let (tx, mut rx) = mpsc::channel(10);
publisher.writes(val.id(), tx);

while let Some(mut batch) = rx.next().await {
    for req in batch.drain(..) {
        println!("Write request: {:?}", req.value);

        // Update the published value
        let mut update = publisher.start_batch();
        val.update(&mut update, req.value);
        update.commit(None).await;

        // Send optional reply
        if let Some(reply) = req.send_result {
            reply.send(Value::String("OK".into()));
        }
    }
}
```

## Tips

- Use `InternalOnly` for testing without a resolver server
- Batch updates for efficiency when publishing multiple values
- Use durable subscriptions (`Dval`) for automatic reconnection
- Register write handlers before publishing values to avoid race conditions
- Use `UpdatesFlags::BEGIN_WITH_LAST` to immediately receive the current value

## More Information

- [Netidx Book](https://netidx.github.io/netidx-book/)
- [API Documentation](https://docs.rs/netidx)
- [Main README](../README.md)
