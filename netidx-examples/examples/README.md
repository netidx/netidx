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

**[custom_type.rs](custom_type.rs)** - Custom Value types

Demonstrates registering and using custom value types, and shows that they can
be transparently sent through systems that don't understand them.

## More Information

- [Netidx Book](https://netidx.github.io/netidx-book/)
- [API Documentation](https://docs.rs/netidx)
- [Main README](../README.md)
