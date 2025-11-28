# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Netidx is a high-performance, distributed publish-subscribe middleware for Rust. It enables publishing values in one program and consuming them in another, either locally or across a network, using a hierarchical namespace with globally unique paths.

## Build Commands

### Building
```bash
# Build entire workspace
cargo build

# Build in release mode (with LTO optimization)
cargo build --release

# Build specific package
cargo build -p netidx
cargo build -p netidx-tools

# Build all targets
cargo build --all-targets
```

### Testing
```bash
# Run all tests
cargo test

# Run tests for specific package
cargo test -p netidx-core
cargo test -p netidx-value

# Run specific test by name
cargo test <test_name>

# Run tests and show output
cargo test -- --nocapture

# Run single test in specific package
cargo test -p netidx <test_name>
```

### Linting
```bash
# Run clippy on entire workspace
cargo clippy

# Run clippy with no deps (faster)
cargo clippy --no-deps

# Run clippy and auto-fix issues
cargo clippy --fix
```

### Formatting
```bash
# Check formatting (uses rustfmt.toml config)
cargo fmt --check

# Apply formatting
cargo fmt
```

## Architecture

Netidx is a Rust workspace with multiple crates organized in three layers:

### Foundation Layer
- **netidx-core**: Core abstractions including Pack (binary serialization), Path (hierarchical namespace), and utilities
- **netidx-value**: Universal value type system with 27+ variants, smart clone optimization, and wire-efficient encoding (LEB128 varints)
- **netidx-netproto**: Wire protocol definitions for resolver and publisher-subscriber communication

### Core Implementation Layer
- **netidx**: Main library implementing the complete system
  - Publisher (`publisher/mod.rs`, `publisher/server.rs`): Publishes data at named paths, serves subscribers
  - Subscriber (`subscriber/mod.rs`, `subscriber/connection.rs`): Consumes published data, manages subscriptions
  - Resolver Client (`resolver_client/`): Queries resolver server for publisher locations
  - Resolver Server (`resolver_server/`): Central directory service tracking publisher locations (NOT data)
  - Channel (`channel.rs`): Async TCP with optional Kerberos/TLS encryption
  - Config (`config.rs`): Configuration file management

### Higher-Level Layer
- **netidx-protocols**: RPC framework, bi-directional channels, clustering primitives
- **netidx-archive**: Time-series archiving with compression (zstd, memory-mapped files)
- **netidx-tools**: CLI tools for administration and debugging
- **netidx-container**: GUI container/nosql database support
- **netidx-wsproxy**: WebSocket proxy for browser access

### Supporting Crates
- **netidx-derive**: Procedural macros for deriving Pack trait

## Three-Component Architecture

### 1. Resolver Server
- **Role**: Central directory service that maps Paths → Publisher addresses (not data itself)
- **Responsibilities**: Publisher registration, authentication/authorization, TTL/heartbeats, referrals for federated resolvers
- **Key files**: `netidx/src/resolver_server/mod.rs`, `store.rs`, `auth.rs`
- **Protocol**: Write side (publishers register), Read side (subscribers query)

### 2. Publisher
- **Role**: Publishes data values at named paths, serves subscribers
- **Responsibilities**: Register with resolver, accept subscriber connections, send updates, handle writes
- **Key files**: `netidx/src/publisher/mod.rs`, `server.rs`
- **Bind modes**: Local (127.0.0.1), Match (CIDR), Elastic (NAT mapping)

### 3. Subscriber
- **Role**: Consumes published data, manages subscriptions
- **Responsibilities**: Resolve paths, connect to publishers, multiplex updates
- **Key files**: `netidx/src/subscriber/mod.rs`, `connection.rs`
- **Types**: Val (non-durable), Dval (durable with auto-reconnect)

## Key Design Patterns

### Hash Consing
De-duplicates immutable data structures to save memory. Used in `resolver_server/store.rs` and `publisher/server.rs` for sets of publishers and subscribers.

### Pool-Based Allocation
Object pools using `poolshark` crate reduce allocation pressure. Pools used for batches, paths, publishers, and responses (e.g., `BATCHES`, `WRITE_BATCHES`, `PATH_POOL`).

### Token-Based Authorization
Resolver issues signed tokens for subscriptions. Subscribers present tokens to publishers for validation, preventing spoofing and enforcing centralized policy.

### Optimized Value Clone
Values with discriminant ≤ COPY_MAX use bitwise copy; others use Arc clone. See `netidx-value/src/lib.rs`.

### Abstract Type System
UUID-based registry for user-defined types with runtime registration. Enables protocol evolution. See `netidx-value/src/abstract_type.rs`.

## Pack Serialization

The Pack trait (`netidx-core/src/pack.rs`) provides efficient binary encoding:

```rust
trait Pack {
    fn encoded_len(&self) -> usize;
    fn encode(&self, buf: &mut impl BufMut) -> Result<()>;
    fn decode(buf: &mut impl Buf) -> Result<Self>;
}
```

Features:
- LEB128 varint encoding (V32, V64) for compact integers
- Zigzag encoding for signed integers (Z32, Z64)
- Length-prefixed strings/bytes
- Zero-copy for Bytes type
- Pool integration for zero-alloc decoding

## Authentication & Security

Four authentication modes (see `netidx/src/auth.rs`):
- **Anonymous**: No authentication (testing/development)
- **Kerberos v5**: Full mutual authentication + encryption
- **TLS**: Certificate-based authentication
- **Local**: Unix socket peer credentials

Permissions: SUBSCRIBE, WRITE, LIST, PUBLISH, PUBLISH_DEFAULT, DENY

Three-way handshake: Resolver ↔ Publisher ↔ Subscriber with token validation.

## Code Review Process

When performing code review, add comments in this format:

```rust
// CR <your-name> for <addressee>: comment text
```

Example:
```rust
// CR claude for estokes: This use of unsafe does not seem safe because...
```

The reviewer will change CR to XCR when addressed and may add explanation. On follow-up review, delete XCR if satisfied or convert back to CR with additional comments.

**Code quality philosophy**: Keep code quality very high. No shortcuts. Think through all implications carefully.

## Development Notes

- **Working directory**: Operations should maintain current directory; prefer absolute paths over `cd`
- **Batching**: Messages are batched throughout the codebase for performance
- **Connection pooling**: Subscriber reuses connections to same publisher
- **Configuration**: JSON files at `~/.config/netidx.json` or system-wide
- **Async runtime**: Uses tokio for all async operations

## Transport & Networking

- **Channel abstraction** (`channel.rs`): Length-prefixed message framing (4-byte BE header) with optional encryption
- Encryption bit in MSB of length field
- Background flush task for async writes
- Kerberos IOV for encryption when enabled

## Recent Changes

- Abstract type system with UUID-based registry
- Bug fixes for get_unchecked and browser compatibility
- Upgraded to Rust 2024 edition

## Common Patterns in Codebase

- Use `GPooled<Vec<T>>` for globally-accessible pools
- Use `LPooled<T>` for thread-local pools
- Batch operations with `start_batch()` → `update()` → `commit()`
- Hash-cons shared data structures to reduce memory
- Length-prefix wire protocol messages
- Token-based auth for subscriptions
- When using subscribe_updates you don't need BEGIN_WITH_LAST, you will get every update from the first one automatically