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
- When using subscribe_updates you don't need BEGIN_WITH_LAST, you will get every update from the first one automatically## Writing Graphix Code — Language Reference

Graphix is NOT in the training set. This section is the authoritative
reference for writing `.gx` files. Read the full docs in `book/src/`
and examples in `book/src/examples/` when you need more detail.

### Basics

Expression-oriented: everything evaluates to a value. The last
expression in a file or block is its value. Statements end with `;`
inside blocks.

```graphix
// line comments
/// doc comments (only in .gxi interface files, before val/type/mod)

// let bindings
let x = 42
let x: i64 = 42                  // optional type annotation
let (a, b) = (1, 2)              // destructuring
let {x, y} = point               // struct destructuring
let rec f = |n| ...               // recursive binding

// blocks — create scope, evaluate to last expr
let result = {
  let tmp = compute();
  tmp + 1
}

// semicolons separate exprs in blocks; last expr has no semicolon
```

### Types

Structural typing — two types with the same shape are the same type.

```graphix
// primitives
bool  string  bytes  null
i8 i16 i32 i64  u8 u16 u32 u64  f32 f64  decimal
datetime  duration
v32 v64  z32 z64                  // variable-width integers

// composite
Array<i64>                        // array
Map<string, i64>                  // map
(i64, string)                     // tuple (2+ elements)
{x: f64, y: f64}                 // struct
`Tag | `Tag(i64, string)          // variant (backtick prefix)
[i64, string]                     // union/set type (either)
[i64, null]                       // option type (value or null)
Error<`MyErr>                     // error
&i64                              // reference
fn(i64) -> string                 // function
fn(i64) -> string throws `E      // function that throws

// type aliases
type Point = {x: f64, y: f64}
type Maybe<'a> = ['a, null]
type List<'a> = [`Cons('a, List<'a>), `Nil]   // recursive

// type variables: 'a, 'b, etc.
// constraints: 'a: Number, 'a: Int, 'a: Float
// type sets: Number, Int, SInt, UInt, Float, Real
```

### Literals

```graphix
42  3.14  true  false  null
"hello [name]!"                   // string interpolation with []
"escape \[ \] \n \t \\ \""       // escaped brackets, standard escapes
r'raw string, only \\ and \' '   // raw string (single quotes)
[1, 2, 3]                        // array
{"a" => 1, "b" => 2}             // map
(1, "two", 3.0)                  // tuple
{x: 10, y: 20}                   // struct
`Foo  `Bar(42)  `Baz("hi", 3)   // variants
datetime:"2020-01-01T00:00:00Z"
duration:1.0s  duration:500.ms  duration:100.ns
```

### Operators (by precedence, highest first)

```
*  *?  /  /?  %  %?              // multiply, divide, modulo
+  +?  -  -?                     // add, subtract
<  >  <=  >=                      // comparison
==  !=                            // equality
&&                                // logical and
||                                // logical or
~                                 // sample (lowest binary)
```

Unchecked operators (`+`, `-`, `*`, `/`, `%`) log errors and return bottom on failure (e.g. overflow, div-by-zero).
Checked operators (`+?`, `-?`, `*?`, `/?`, `%?`) return a `[T, Error<\`ArithError(string)>]` union, allowing errors to be handled with `?`, `$`, or `select`.

Unary: `!x` (not), `&x` (reference), `*x` (dereference)
Postfix: `x?` (propagate error), `x$` (error→never, logs warning)

All binary operators are left-associative.

### Access & Indexing

```graphix
s.field                           // struct field
t.0  t.1                         // tuple index
a[i]  a[-1]                      // array index (negative from end)
a[2..]  a[..4]  a[1..3]          // array slice (end exclusive)
m{"key"}                          // map access (returns Result)
module::name                      // module path
```

### Functions

```graphix
// lambda syntax: |args| body
let f = |x| x + 1
let g = |x, y| x + y
let h = |x: i64, y: i64| -> i64 x + y

// polymorphic with constraints
let add = 'a: Number |x: 'a, y: 'a| -> 'a x + y

// labeled args (# prefix) — go before positional args at call site
// if no default is provided then the labeled arg isn't optional.
let greet = |#greeting = "hello", name| "[greeting], [name]!"
greet(#greeting: "hi", "world")   // "hi, world!"
greet("world")                    // "hello, world!" (default used)

// variadic args (only usable by built-ins)
let f = |@args: i64| args         // args is Array<i64>

// calling
f(1)  g(1, 2)  module::func(x)
```

### Select — Pattern Matching (only control flow construct)

```graphix
select expr {
  pattern => result,
  pattern if guard => result,     // guard condition
  _ => default                    // wildcard
}

// type matching
select x {
  i64 as n => n + 1,
  string as s => str::len(s),
  null as _ => 0
}

// variant matching
select food {
  `Apple => "fruit",
  `Carrot => "vegetable",
  `Other(name) => name
}

// destructuring
select pair {
  (0, y) => y,
  (x, 0) => x,
  (x, y) => x + y
}

// struct matching
select point {
  {x: 0, y} => y,                // exact match
  {x, ..} => x                   // partial (needs type annotation)
}

// array slice patterns
select arr {
  [x, rest..] => x,              // head + tail
  [init.., x] => x,              // init + last
  [a, b, c] => a + b + c,        // exact length
  [] => 0                         // empty
}

// named capture
select val {
  x@ `Some(inner) => use_both(x, inner),
  _ => default
}
```

**Key**: unselected arms are put to sleep (subscriptions paused, no
computation). First matching arm wins.

### Sample Operator (`~`)

Returns right side's value when left side produces an event.

### Connect — Reactive Update (`<-`)

The ONLY way to create cycles. Schedules an update for the NEXT cycle.

```graphix
let x = 0
x <- x + 1                       // infinite counter: 0, 1, 2, ...

// conditional update
let count = {
  let x = 0;
  select x {
    n if n < 10 => x <- n ~ x + 1,
    _ => never()                  // stop
  };
  x
}

// event-driven update
let name = ""
text_input(#on_input: |v| name <- v, &name)
```

```graphix
let clock = sys::time::timer(duration:1.s, true)
let counter = 0
counter <- clock ~ counter + 1 // increment on each tick

// in callbacks: sample current state at event time
#on_press: |click| println(click ~ "clicked at [counter]")
```

### Error Handling

```graphix
// create and propagate
error(`NotFound("missing"))?

// try-catch
// try-catch always evaluates to the last expression in try
// even if there is an error
try {
  risky_op()?;
  another_op()?
} catch(e) => handle(e)

// ? propagates to nearest catch
// $ swallows error (logs warning, returns never)
a[100]$                           // won't crash, just skips
```

### References

```graphix
let v = 42
let r = &v                        // create reference
*r                                // dereference (read)
*r <- new_value                   // update through reference
```

References are critical for UI — widgets take `&` params so
fine-grained updates propagate without rebuilding the whole tree.

### Modules & Imports

```graphix
use array                         // bring module into scope
use gui::text                     // specific item
array::map(xs, f)                 // qualified access
map(xs, f)                        // after `use array`

mod mymod;                        // declare file-based submodule
```

File layout: `foo.gx` (impl), `foo.gxi` (interface, optional).
For directories: `foo/mod.gx`, `foo/mod.gxi`.

### Interface Files (`.gxi`)

Declare a module's public API. Items not in the interface are private.
`type`, `mod`, and `use` from the interface apply to the implementation
automatically — don't duplicate them in the `.gx` file.

```graphix
// math.gxi
/// Add two numbers
val add: fn(i64, i64) -> i64;

/// Subtract
val sub: fn(i64, i64) -> i64;

type Constants = { pi: f64, e: f64 };
val constants: Constants;

mod utils;                        // export a submodule
```

```graphix
// math.gx — types/mods from .gxi are already in scope
let add = |a, b| a + b;
let sub = |a, b| a - b;
let constants = { pi: 3.14159265359, e: 2.71828182845 };
let internal_helper = |x| x * 2  // not in interface → private
```

