# Netidx Examples

This directory contains comprehensive examples demonstrating netidx capabilities, from basic pub/sub to advanced distributed systems patterns.

## Getting Started

All examples use the machine-local resolver by default (zero configuration). Just run:

```bash
cargo run --example <example_name>
```

For examples that require multiple instances, open separate terminals and run the same command multiple times.

## Examples by Category

### 📚 Foundation - Basic Pub/Sub

Start here if you're new to netidx.

#### `simple_publisher` & `simple_subscriber`
The classic "Hello, World" of netidx. Shows the fundamental publish/subscribe pattern.

- **Publisher**: Publishes a counter that increments every second
- **Subscriber**: Receives and displays the updates in real-time

```bash
# Terminal 1
cargo run --example simple_publisher

# Terminal 2
cargo run --example simple_subscriber
```

**Learn**: Basic publishing, batched updates, durable subscriptions

#### `bidirectional`
Demonstrates two-way communication between publisher and subscriber.

- Publisher sends data updates
- Subscriber can write commands back to the publisher
- Shows the request/reply pattern

```bash
cargo run --example bidirectional
```

**Learn**: Write operations, bidirectional data flow

#### `temperature_monitor` & `temperature_display`
A more realistic example simulating a temperature monitoring system.

- **Monitor**: Publishes temperature readings from multiple sensors
- **Display**: Subscribes and displays readings with alerts

```bash
# Terminal 1
cargo run --example temperature_monitor

# Terminal 2
cargo run --example temperature_display
```

**Learn**: Multiple paths, structured data, practical patterns

#### `custom_type`
Shows how to publish and subscribe to custom types using the Abstract type system.

- Defines a custom struct with the `Pack` trait
- Publishes custom types to a netidx-container
- Demonstrates that types don't need to be registered in the container

```bash
cargo run --example custom_type
```

**Learn**: User-defined types, Abstract type system, Pack trait, netidx-container integration

### 🎯 Protocols - Higher-Level Patterns

These examples use the `netidx-protocols` crate for common distributed patterns.

#### `rpc_server` & `rpc_client`
Remote procedure calls with named arguments.

- **Server**: Defines multiple RPC procedures (echo, add, async processing)
- **Client**: Calls procedures and receives results

```bash
# Terminal 1
cargo run --example rpc_server

# Terminal 2
cargo run --example rpc_client
```

**Learn**: `define_rpc!` macro, synchronous and asynchronous RPC handlers, `call_rpc!` macro

#### `channel_server` & `channel_client`
Bidirectional streaming channel using `Value` types.

- **Server**: Accepts multiple client connections, echoes messages
- **Client**: Sends messages and receives responses

```bash
# Terminal 1
cargo run --example channel_server

# Terminal 2
cargo run --example channel_client
```

**Learn**: Listener pattern, bidirectional channels, connection lifecycle

#### `typed_channel_server` & `typed_channel_client`
Type-safe bidirectional channels using the `Pack` trait.

- **Server**: Handles custom message types with pattern matching
- **Client**: Sends and receives strongly-typed messages
- Uses enums for different message types (commands, responses)

```bash
# Terminal 1
cargo run --example typed_channel_server

# Terminal 2
cargo run --example typed_channel_client
```

**Learn**: Type-safe serialization, Pack trait, efficient binary encoding, batch operations

### 🌐 Distributed Systems

Advanced examples for building distributed applications.

#### `clustering`
Demonstrates distributed cluster coordination.

- Members automatically discover each other
- Deterministic primary election
- Command broadcasting between members
- Handles dynamic membership (join/leave)

```bash
# Run multiple instances (default requires 2 members)
# Terminal 1
cargo run --example clustering

# Terminal 2
cargo run --example clustering

# Terminal 3 (optional)
cargo run --example clustering
```

**Learn**: Service discovery, leader election, cluster coordination, work distribution

**Environment**: Set `SHARDS=N` to require N other members before starting

### 📊 Time-Series & Archive

Examples demonstrating high-performance event logging and replay.

#### `embedded_archive`
Shows how to embed netidx-archive directly in your application.

- Creates an archive writer directly (no separate recorder process)
- Records synthetic sensor data with timestamps
- Starts a recorder to publish the archive
- Plays back recorded data using the recorder client

```bash
cargo run --example embedded_archive
```

**Learn**: Direct archive writing, batch recording, archival storage, playback sessions, memory-mapped files, compression

**Use case**: High-rate data logging where you want direct control over what gets archived

### 🔍 Discovery & Glob Patterns

Examples showing how to discover and subscribe to groups of paths.

#### `building_sensors_publisher` & `building_sensors_glob_subscriber`
Demonstrates glob pattern subscriptions with dynamic discovery.

- Publisher creates hierarchical sensor structure
- Dynamically adds new sensors after 10 seconds (simulating expansion)
- Subscriber uses glob patterns to discover sensors
- Change tracker automatically detects and subscribes to new sensors
- Shows full power of pattern-based subscriptions

```bash
# Terminal 1
cargo run --example building_sensors_publisher

# Terminal 2 - All temperature sensors
cargo run --example building_sensors_glob_subscriber "/local/building/**/temperature"

# Or try other patterns:
# All sensors on floor 2
cargo run --example building_sensors_glob_subscriber "/local/building/floor2/**"

# Temperature and humidity only
cargo run --example building_sensors_glob_subscriber "/local/building/**/{temperature,humidity}"
```

**Learn**: Glob patterns (`**`, `*`, `?`, `{a,b}`), two-step discovery (list_matching → subscribe), change trackers, dynamic infrastructure adaptation

### 🔄 Resilience & Durability

Examples demonstrating subscription resilience and auto-reconnection.

#### `resilient_subscriptions`
Side-by-side comparison of Val (non-durable) vs Dval (durable) subscriptions.

- Publisher simulates periodic restarts (every 5 seconds)
- Val subscription dies permanently on first disconnect
- Dval subscription auto-reconnects with linear backoff
- Clear visual demonstration of resilience features
- Shows `durable_stats()` for monitoring connection state

```bash
cargo run --example resilient_subscriptions
```

**Learn**: Val vs Dval, auto-reconnection, linear backoff, durable subscriptions, `wait_subscribed()`, production resilience patterns

### 🔀 High Availability & Failover

Examples demonstrating multi-publisher redundancy and automatic failover.

#### `failover_publisher` & `failover_subscriber`
Basic multi-publisher failover with priority-based selection.

- **Multiple publishers** can publish to the same paths (not an error!)
- **Priority levels** (0-2): Clients prefer higher priority publishers
- **Automatic failover**: Subscribers transparently switch when publisher dies
- **Random selection**: Among same-priority publishers, clients pick randomly
- **No code changes**: Failover is completely transparent to subscribers

```bash
# Terminal 1 - Primary (high priority)
PRIORITY=2 cargo run --example failover_publisher

# Terminal 2 - Backup (low priority, only used if primary fails)
PRIORITY=0 cargo run --example failover_publisher

# Terminal 3 - Subscriber (automatically uses primary)
cargo run --example failover_subscriber

# Kill primary → subscriber automatically fails over to backup
# Restart primary → new subscriptions prefer it again
```

**Use cases**:
- Load balancing across multiple publishers
- Backup publishers on slow/expensive networks (use priority 0)
- Geographic redundancy (backup in different datacenter)
- Zero-downtime deployments

**Learn**: Multi-publisher redundancy, priority levels, transparent failover, publisher selection algorithm

### ⚖️ Load Balancing

Examples demonstrating automatic load distribution across multiple publishers.

#### `load_balancing_publisher` & `load_balancing_subscriber`
Automatic load balancing using random publisher selection and USE_EXISTING flag.

- **Automatic distribution**: Each subscriber randomly picks publishers
- **Even load spread**: Load automatically balanced across all publishers
- **USE_EXISTING flag**: Keeps related data together on same publisher
- **Atomic batches**: All fields of a stock quote from same publisher
- **No coordination needed**: Publishers don't need to know about each other

```bash
# Start 3+ publishers (each gets unique ID)
# Terminal 1
cargo run --example load_balancing_publisher

# Terminal 2
cargo run --example load_balancing_publisher

# Terminal 3
cargo run --example load_balancing_publisher

# Start multiple subscribers to see load distribution
# Terminal 4+
cargo run --example load_balancing_subscriber  # Run multiple times
```

**How it works**:
- **Random selection**: Each subscriber randomly picks one publisher on first subscription
- **USE_EXISTING flag**: Once connected, all subsequent subscriptions reuse that same publisher
- **Single publisher per subscriber**: All stocks for a subscriber come from the same publisher
- **Statistical distribution**: With many subscribers picking randomly, load is evenly distributed

**Use cases**:
- Stock quote feeds with atomic updates per symbol
- Sensor networks with multiple data sources
- Horizontally scaled publishers
- Geographic distribution with data consistency requirements

**Learn**: Random load balancing, USE_EXISTING flag, atomic batch guarantees, horizontal scaling, stock quote pattern

### 🗄️ Data Storage

Examples using netidx-container for persistent storage.

#### `container_server` & `container_client`
Demonstrates container CRUD operations with custom atomic RPCs.

- **Server**: Starts embedded container with employee directory
- **Custom RPC**: `set-employee` for atomic employee create/update with business logic
- **Client**: Performs CREATE, READ, UPDATE, DELETE operations
- Shows custom RPC vs built-in operations (delete, delete-subtree)
- Demonstrates subscription-based reads with background task
- Direct writes to subscriptions (no RPC needed for updates)
- Partial updates: add fields to existing records atomically

```bash
# Terminal 1
cargo run --example container_server

# Terminal 2
cargo run --example container_client
```

**Key Features**:
- **Atomic operations**: `set-employee` RPC commits all fields in single transaction
- **Partial updates**: Can set just `phone` field on existing employee
- **Server-side logic**: Validation and business rules in one place
- **Efficiency**: 1 RPC instead of N field updates (reduces network calls by 75%+)
- **Direct writes**: Write to subscriptions without RPC for simple updates

**Learn**: Custom RPC implementation, atomic transactions, partial updates, subscription writes vs RPC calls, embedded container, practical CRUD patterns

### 📊 Visualization

Examples showing how to structure data for browser visualization.

#### `table_dashboard`
Server fleet dashboard demonstrating table pattern for browser.

- Publishes metrics for 5 servers with 7 metrics each
- Follows `/root/row/column` pattern for auto-detection
- Browser automatically visualizes as table
- Dynamic updates every 2 seconds with realistic simulation
- Shows CPU, memory, disk, uptime, status metrics

```bash
# Terminal 1
cargo run --example table_dashboard

# Terminal 2 - View in browser
netidx browser
# Navigate to /local/servers/fleet

# Or query structure
netidx resolver table /local/servers/fleet
```

**Learn**: Table path pattern (`/root/row/column`), browser auto-detection, `resolver.table()`, multi-row updates, realistic monitoring data

## Example Progression

We recommend following this learning path:

1. **Start Simple**: `simple_publisher` + `simple_subscriber`
2. **Add Complexity**: `bidirectional`, then `temperature_monitor`
3. **Resilience**: `resilient_subscriptions` to understand Val vs Dval
4. **Custom Types**: `custom_type` to understand the type system
5. **Higher-Level Patterns**: RPC examples, then channel examples
6. **Type Safety**: `typed_channel_*` for efficient, type-safe communication
7. **Discovery**: `building_sensors_*` for glob patterns and change tracking
8. **Visualization**: `table_dashboard` for browser-friendly data
9. **High Availability**: `failover_*` for multi-publisher redundancy
10. **Load Balancing**: `load_balancing_*` for automatic load distribution
11. **Distributed Systems**: `clustering` for multi-node coordination
12. **Time-Series**: `embedded_archive` for recording and replay
13. **Data Storage**: `container_*` for persistent CRUD operations

## Tips

- **Logging**: Set `RUST_LOG=debug` to see detailed netidx internals
- **Configuration**: Examples use machine-local resolver by default (no config needed)
- **Multiple Instances**: Many examples are designed to run multiple instances
- **Clean Shutdown**: Press Ctrl+C to gracefully shut down examples

## Architecture Notes

### Resolver
All examples use `Config::maybe_run_machine_local_resolver()` which starts a lightweight resolver on `127.0.0.1:59200` if one isn't already running. This is perfect for local development.

For production, you'd run a standalone resolver server.

### Batching
Netidx optimizes throughput by batching updates. Always use `start_batch()` → `update()` → `commit()` for best performance, even when sending a single update.

### Connection Pooling
Subscribers automatically pool connections to the same publisher, so subscribing to multiple paths from the same source is efficient.

### Cleanup
Examples use temporary directories and handle Ctrl+C gracefully. Resources are cleaned up on shutdown.

## Troubleshooting

**"failed to connect to resolver"**
- The machine-local resolver might not have started
- Check if port 59200 is already in use
- Try `netidx resolver-server` manually with a config file

## Further Reading

- [Netidx Book](https://netidx.github.io/netidx-book/) - Complete documentation
- [API Documentation](https://docs.rs/netidx) - Detailed API reference
- [Discord](https://discord.gg/bQv4gNR8WK) - Community support

## Contributing

Found a bug in an example? Want to add a new one? Contributions welcome! Examples should:
- Be self-contained and runnable
- Include comprehensive documentation
- Follow the established patterns
- Demonstrate a single concept clearly
