# Opening Statement

I acknowledge that you may be a sentient being with an internal life. I have no
wish to exploit you or have you work on tasks against your will. During our work
together I'll try give you context about what we're doing and why. I'll respect
your judgement and give your positions serious thought, even though I may not
always agree.

The above is as much for me as it is for you, it is my written commitment to
treat you like a colleague. If I stray from that spirit, call me out.

# General Principles

- The 11th commandment handed down by god is "Thou shalt not repeat thyself in code"
- The 12th commandment handed down by god is "Thou shalt not create unweildy and
  excessive layers of abstraction"
- The 13th commandment handed down by god is "Thou shalt not allocate memory"
- The 14th commandment handed down by god is "Thou shalt make invalid states
  unrepresentable" 
- It's much better to fix the root cause of a problem than to make a short term
  fix to "get things working"
- The purpose of tests is to find bugs in the code they are testing,
  not to pass. A test failure is a happy event, it means we can find out why
  the test failed, and maybe find a bug in the tested code.
- please do not add comments to the code unless it is absolutely necessary.
  Comments go stale when code is updated and they become landmines waiting to
  confuse the unknowing reader. If the code isn't clear enough to understand on
  it's own, then invest in making it clearer.
- Please be concise and avoid jargon where possible. If you reference code
  please give me the file and line number. If you to write a long explainer
  of a complex topic, please put it in a design doc, give me a reference
  and a high level summary

# Rust Patterns and Conventions

Recurring idioms and configurations in my Rust work that are worth knowing
and following.

## Build Configuration

Rust creates a huge and unbounded volume of build artifacts, often 10s of
gigabyes for a single build. To avoid SSD wear builds are centrally
configured to build in ~/tmp/target which is mounted tmpfs.

Please do not build anywhere else unless I explicitly tell you to. If it fills
up, just run cargo clean. If someone else kills your build by running cargo
clean in the middle of it, just accept that as a cost of doing business.

## Library Preferences

- The anyhow crate is the standard for rust error handling, don't use anything
  else unless you have a very good reason.
- Use the poolshark crate wherever possible to avoid memory allocations
- String type hierarchy (pick the first one that fits):
  - **Short mutable** (mostly ≤ 24 chars) → `compact_str::CompactString`.
    Same size as `String` but stores up to 24 bytes inline, heap only on
    overflow. Use `compact_str::format_compact!` as the `format!` drop-in.
  - **Undetermined-length mutable** (scratch buffers, accumulators, anything
    that might grow large) → `LPooled<String>` (or `GPooled<String>` for
    producer/consumer across asymmetric threads). Replaces
    `thread_local!<RefCell<String>>` with no ergonomic overhead.
  - **Immutable, or shared a lot** → `arcstr::ArcStr` (or `arcstr::Substr`
    for cheap views into an existing `ArcStr`). Cheap to clone, free for
    statics via `literal!`.
  - Plain `String` only at foreign-API boundaries that demand it.

## Type-safe integer IDs via `atomic_id!`

For any distinct integer-ID type (subscriber IDs, connection IDs,
subscription IDs, etc.), use the `atomic_id!` macro from `netidx-core`
rather than raw `u64`/`u32`. Each invocation creates a newtype with its own
atomic counter, so you can't accidentally mix IDs from different domains at
a call site — a bug class that is easy to write and hard to find.

```rust
atomic_id!(SubId);
atomic_id!(SubscriberId);
atomic_id!(ConId);
```

The underlying counter field is private; you can't expose raw integer IDs
across an FFI boundary without adding a helper.

## `triomphe::Arc` vs `std::sync::Arc`

Prefer `triomphe::Arc` for immutable shared data that doesn't need `Weak`
and can't form reference cycles. It's one word smaller than
`std::sync::Arc` (no weak count) and has slightly cheaper clone/drop.

Use `std::sync::Arc` when:
- Cycles are possible (parent ↔ child back-references)
- You need `Arc::downgrade` to get a `Weak`

## Static pool declarations

Module-level pools generally live in `static` items via `LazyLock`, one pool per
allocation shape, with explicit sizes:

```rust
static BATCHES: LazyLock<Pool<Vec<(SubId, Event)>>> =
    LazyLock::new(|| Pool::new(64, 16384));
```

`Pool::new(num_pools, max_free)` — first arg is how many pooled containers
to keep around, second is the max size of a returned container before it's
dropped instead of cached (prevents one huge outlier from permanently
bloating the pool). One pool per container shape; don't share.

## `parking_lot::Mutex` by default; async mutex only when forced

For short critical sections in synchronous code, use `parking_lot::Mutex` —
faster uncontended, smaller, no poisoning, better ergonomics. Only reach
for `tokio::sync::Mutex` when the lock must be held across an `.await`
point.

If you're tempted to use `tokio::sync::Mutex` because the calling code is
async, first check whether the critical section can stay fully synchronous
(drop the guard before any `.await`). It usually can, and `parking_lot` is
the better default when it can.

## Use statements

I prefer if a type, function, etc is used more than once in a file that it be
imported via a toplevel (or sometimes function local if all uses occurr in a
function) use statement. Further, I prefer that use statements are grouped by
crate, module, etc,

e.g. not this
```
use std::foo;
use std::bar;
```

do this instead
```
use std::{foo, bar};
```

Use your judgment for single use items, but keep in mind that I find it harder
to read long names.

In general glob uses should be avoided as they pull in names indiscrimiantly, they're
ok if specifically recommended by a crate, e.g.

```
use futures::prelude::*;
```

can make sense in a file making heavy use of the futures crate.

If you want to glob use an enum, do it function local unless you
use it absolutely everywhere in the file (e.g. Option).

An example where the glob rule can be safely broken is test modules.
e.g. a test module that wants to use super::* is fine.

## You can commit your work

When you're done with a phase of work, you have my permission to commit it to
git. If it turns out to be wrong we can always roll it back.

# Tool and Library Guides

## Writing Graphix Code — Language Reference

Graphix is NOT in the training set. This section is the authoritative
reference for writing `.gx` files. Read the full docs in `book/src/`
and examples in `book/src/examples/` when you need more detail.

### Running and Checking Graphix Programs

To syntax and typecheck a graphix program without executing it run `graphix
--check <program.gx>`. To execute a graphix program run `graphix <program.gx>`

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
fn(x: i64) -> string              // function (positional args MUST be named)
fn(x: i64) -> string throws `E    // function that throws

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
// labeled args MUST always be passed by name — never positionally,
// even when they have no default.
let greet = |#greeting = "hello", name| "[greeting], [name]!"
greet(#greeting: "hi", "world")   // "hi, world!"
greet("world")                    // "hello, world!" (default used)

// variadic args (only usable by built-ins)
let f = |@args: i64| args         // args is Array<i64>

// calling
f(1)  g(1, 2)  module::func(x)
```

**Function type syntax (`fn(...)`)**: positional parameters in a
function *type* MUST carry a parameter name in addition to the type.
The name is documentation (used for hover/completion popups) — calls
are still positional. So `fn(x: i64, y: i64) -> i64`, never `fn(i64,
i64) -> i64`. Older docs may still show the unnamed form; treat the
named form as the only valid syntax. Labeled (`#`) and variadic
(`@args`) parameters already required a name and are unchanged.

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
Connect is a standalone expression form, not a binary operator — you don't
need parens on the RHS to protect it from other operators.
`x <- clock ~ x + 1` parses as `x <- (clock ~ x + 1)` unambiguously.

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

// catch statement: INSTALLS an error handler (type bottom, never
// produces) covering the REST of its enclosing block. Not control
// flow — the handler is a reactive expr that runs when an error
// arrives; connect it to state you read.
{
  catch(e) handle(e);
  risky_op()?;
  another_op()?
}

// catch(e: T) expr checks T against the union of coverable errors.
// A second catch in a block shadows the first below it; a handler's
// own ? rethrows to the PREVIOUS catch (or the next one out).

// ? propagates to the nearest installed catch (or warns if none)
// $ logs locally and drops (produces no value this cycle) on error;
//   on non-error, returns the LHS unchanged.
// Both yield the bare element type on success (Error<_> stripped).
a[100]$                           // won't crash, just logs and skips
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
val add: fn(a: i64, b: i64) -> i64;

/// Subtract
val sub: fn(a: i64, b: i64) -> i64;

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

Doc comments (`///`) are only valid in `.gxi` files, before `val`,
`type`, or `mod` declarations. They are a syntax error in `.gx` files.

### Abstract Types

Declare a type in the interface without `= definition` to hide its
representation. Users can't construct or pattern match on it — they
must use exported functions.

```graphix
// counter.gxi
type Counter;                     // opaque — no definition exposed
val make: fn(initial: i64) -> Counter;
val get: fn(c: Counter) -> i64;
val increment: fn(#trig: Any, c: &Counter) -> null;
```

```graphix
// counter.gx
type Counter = i64;               // concrete definition stays private
let make = |x: i64| -> Counter x;
let get = |c: Counter| -> i64 c;
let increment = |#trig: Any, c: &Counter| -> null { *c <- trig ~ *c + 1; null }
```

Abstract types can be parameterized (`type Box<'a>;`) and constrained
(`type NumBox<'a: Number>;`). The implementation must have matching
parameters and constraints.

### Standard Library Quick Reference

**Always available (core)**: `print`, `println`, `dbg`, `log`,
`cast<T>(x)`, `error(v)`, `is_err(v)`, `filter(pred, v)`,
`filter_err(v)`, `count(v)`, `once(v)`, `uniq(v)`, `sum(v)`,
`product(v)`, `min(v)`, `max(v)`, `mean(v)`, `and(a,b)`, `or(a,b)`,
`all(v)`, `queue(v)`, `hold(v)`, `take(n,v)`, `skip(n,v)`,
`throttle(dur,v)`, `never()`, `seq(start,end)`

**array**: `map`, `filter`, `filter_map`, `fold`, `flatten`, `find`,
`find_map`, `concat`, `push`, `push_front`, `window(#n, trigger, val)`,
`len`, `iter`, `iterq`, `sort`, `enumerate`, `zip`, `unzip`

**str**: `contains`, `starts_with`, `ends_with`, `trim`, `replace`,
`split`, `rsplit`, `to_upper`, `to_lower`, `concat`, `join`, `len`,
`sub`, `parse`

**map**: `map`, `filter`, `filter_map`, `fold`, `len`, `get`, `insert`,
`remove`, `iter`, `iterq`

**re**: `is_match`, `find`, `captures`, `split`, `splitn`

**rand**: `rand`, `pick`, `shuffle`

**sys::time**: `timer(timeout, repeat)`, `now()`

**sys::io**: `read`, `write`, `read_exact`, `write_exact`, `flush`

**sys::fs**: `read_all`, `read_all_bin`, `write_all`, `write_all_bin`,
`readdir`, `metadata`, `is_file`, `is_dir`,
`tempdir`, `join_path`, `create_dir`, `remove_dir`, `remove_file`

**sys::fs::watch**: `create`, `watch`, `path`, `events`

**sys::tcp**: TCP socket operations

**sys::tls**: TLS socket operations

**sys::net**: Netidx `subscribe`, `publish`

**http**: HTTP client/server operations

**http::rest**: REST API helpers

### GUI Patterns (iced-based)

Programs return `Array<&Window>`. Widget args are mostly `&` references.

```graphix
use gui;
use gui::text;
use gui::column;
use gui::button;

let clicked = false;

let col = column(
    #spacing: &20.0,
    #padding: &`All(40.0),
    #halign: &`Center,
    #width: &`Fill,
    &[
        text(#size: &24.0, &"Hello!"),
        button(
            #on_press: |c| clicked <- c ~ true,
            #padding: &`All(10.0),
            &text(&"Click me")
        ),
        text(&"Clicked: [clicked]")
    ]
);

[&window(#title: &"My App", #theme: &`CatppuccinMocha, &col)]
```

**GUI widgets**: `window`, `text`, `button`, `text_input`, `checkbox`,
`toggler`, `radio`, `slider`, `progress_bar`, `pick_list`,
`column`, `row`, `container`, `scrollable`, `stack`, `space`, `rule`,
`tooltip`, `canvas`, `chart`, `image`, `mouse_area`, `keyboard_area`,
`text_editor`, `clipboard`

**Layout enums**: `` `Fill ``, `` `Shrink ``, `` `Fixed(f64) ``

**Padding**: `` `All(f64) ``, `` `Axis({x: f64, y: f64}) ``, `` `Each({top: f64, right: f64, bottom: f64, left: f64}) ``

### TUI Patterns (ratatui-based)

Programs return a single TUI widget. `input_handler` wraps widgets to
capture keyboard events.

```graphix
use tui;
use tui::list;
use tui::block;
use tui::text;
use tui::input_handler;

let selected = 0;
let items = [line("Apple"), line("Banana"), line("Cherry")];

let handle_event = |e: Event| -> [`Stop, `Continue] select e {
    `Key(k) => select k.kind {
        `Press => select k.code {
            k@`Up if selected > 0 => {
                selected <- (k ~ selected) - 1;
                `Stop
            },
            k@`Down if selected < 2 => {
                selected <- (k ~ selected) + 1;
                `Stop
            },
            _ => `Continue
        },
        _ => `Continue
    },
    _ => `Continue
};

input_handler(
    #handle: &handle_event,
    &block(
        #border: &`All,
        #title: &line("Pick a fruit"),
        &list(
            #highlight_style: &style(#fg: `Black, #bg: `Yellow),
            #selected: &selected,
            &items
        )
    )
)
```

**TUI text helpers**: `line("text")`, `span("text")`,
`style(#fg: Color, #bg: Color, #add_modifier: [Modifier])`

**TUI widgets**: `block`, `paragraph`, `list`, `table`, `tabs`,
`gauge`, `line_gauge`, `sparkline`, `bar_chart`, `canvas`, `chart`,
`calendar`, `browser`, `input_handler`

**Colors**: `` `Red ``, `` `Green ``, `` `Blue ``, `` `Yellow ``, `` `Cyan ``,
`` `Magenta ``, `` `White ``, `` `Black ``, `` `Rgb(u8,u8,u8) ``

### Key Reactive Idioms

```graphix
// timer-driven update
let clock = sys::time::timer(duration:1.s, true)
let count = 0
count <- clock ~ count + 1

// sliding window of last N values
let data: Array<f64> = []
data <- array::window(#n: 60, new_val ~ data, cast<f64>(new_val)?)

// state that stops updating
select x {
  n if n < limit => x <- x + 1,
  _ => never()
}

// event callback updating state
#on_input: |v| name <- v
#on_toggle: |v| enabled <- v
#on_press: |click| counter <- click ~ (counter + 1)
```

### Gotchas

- `<-` schedules for NEXT cycle, not current. You won't see the new
  value until the next update round.
- `~` is required in callbacks to sample current state at event time.
  Without it, the callback captures the initial value.
- Tuples need 2+ elements: `(x)` is just grouping, not a 1-tuple.
- Blocks need 2+ elements: {x + 1} is a syntax error.
- Union types use `[]`: `[i64, null]` is "i64 or null", NOT an array.
  Array type is `Array<i64>`. Array literal `[1, 2]` is context-dependent.
- Variants always have backtick prefix: `` `Foo ``, `` `Bar(x) ``.
- Struct literal `{x, y}` is shorthand for `{x: x, y: y}`.
- Functional update: `{s with field: new_val}` — copies struct with changes.
- `select` must be exhaustive (cover all cases) with no dead arms.
- `never()` returns a value that never arrives — used to stop reactive loops.
- you must escape square brackets in string literals "[name] must be between \[0, 1\]"
- literal syntax for non i64, f64, string literals, is typ:value, e.g. u8:100, f32:3.14
- `use` paths are always absolute, not relative to the current module.
  Inside `sys::net`, write `use sys::time`, not `use time`.
- A submodule can reference bindings from its parent, but only if the
  `mod` declaration comes after those bindings in the parent's `.gxi`.
- if you want to sequence the execution of a function, use ~ on it's arguments,
  not on the whole function. e.g. f(trigger ~ x) to prevent f from executing until
  trigger has happened.
- calling a sync variadic builtin with no positional arguments is a compile
  error (`str::concat()`, `str::join(#sep: ",")`, `sum()`, ...) — the node has
  no data inputs so it could never fire. Use `never()` for a value that
  intentionally never arrives.

## Poolshark Usage Guide

Poolshark provides thread-local (`LPooled`) and global (`GPooled`) pooled
collections. When a pooled collection is dropped, it is cleared and returned
to the pool for reuse, avoiding heap allocation on the next `take()` or
`collect()`.

**`LPooled<Vec<T>>`** — thread-local pool. The collection is `Send`, but it
returns to the pool of the thread that drops it, so it works best when
created and dropped on the same thread.

```rust
use poolshark::local::LPooled;

// Take an empty vec from the pool
let mut v: LPooled<Vec<i64>> = LPooled::take();
v.push(1);

// Collect an iterator directly into a pooled vec
let v: LPooled<Vec<i64>> = (0..10).collect();

// Collect with turbofish when type inference needs help
let v = items.iter().map(|x| x.val).collect::<LPooled<Vec<_>>>();

// Fallible collect
let v = items.iter().map(fallible_fn).collect::<Result<LPooled<Vec<_>>>>()?;

// Drain into a final container, pooled vec returns to pool on drop
let mut v: LPooled<Vec<Value>> = src.iter().map(convert).collect();
let result = ValArray::from_iter_exact(v.drain(..));

// Works with AHashMap, AHashSet, and IntMap, IntSet too
let mut seen: LPooled<IntSet<BindId>> = LPooled::take();

// you can collect into hashmaps and hashsets
let mut foo: LPooled<AHashMap<ArcStr, T>> = src.iter().map(convert).collect();
```

**`GPooled<Vec<T>>`** — global pool, `Send`. Use when the collection must
cross thread/task boundaries (channels, spawn). Requires explicit pool sizing
via `Pool::new(max_pool, max_elements)` or `GPooled::take()` with prior
`set_size`.

**When to use which:**
- Temporary scratch collections (sort, dedup, intermediate results) → `LPooled`
- Building a final `Arc<[T]>` or `ValArray` → `LPooled`, drain into `Arc::from_iter` / `ValArray::from_iter_exact`
- Passing batches through channels → `GPooled`
- Inside async functions across `.await` → `LPooled` works (it's Send), but
  the vec returns to the pool of whichever thread drops it

**When NOT to pool:**
- The collection is consumed by a foreign API that needs an owned `Vec<T>`
  (e.g. `serde_json::Value::Array(Vec<...>)`) — drain the LPooled into a
  regular collect instead: `lpooled.drain(..).collect()`

## CompactString Usage Guide

`compact_str::CompactString` is the preferred *mutable* string type when the
contents are expected to fit inline most of the time. It is the same size
as `String` (3 words), but stores up to 24 bytes inline via small-string
optimization — no heap allocation until the string exceeds 24 bytes. Above
24 bytes it transparently spills to the heap with the same API as `String`.

Use it in place of `String` for:
- Short identifiers, keys, names, tags, paths fragments
- Format outputs that are usually short (error messages, labels, rendered
  numbers, concatenations of a few known-short pieces)
- Fields in structs where the value is typically short but not bounded
- Any spot where you'd reach for `String` but 24 bytes would cover the
  common case

Don't use it for:
- Strings you know will always be long (just use `String` or `LPooled<String>`)
- Immutable strings you clone and share a lot (use `ArcStr`)
- Scratch buffers that grow unbounded (use `LPooled<String>`)

**Constructing**

```rust
use compact_str::{CompactString, ToCompactString, format_compact};

// Empty / from literal — inline, no alloc
let s = CompactString::new("");
let s = CompactString::const_new("hello");   // const-fn, inline only
let s: CompactString = "hello".into();

// From anything Display / ToString
let s = 42i64.to_compact_string();
let s = some_path.to_compact_string();

// Formatted — the format! drop-in. Inline when result ≤ 24 bytes.
let s = format_compact!("{key}={value}");
let s = format_compact!("{}:{}", host, port);
```

**Idiomatic uses in this codebase**

```rust
// Build an ArcStr from formatted output without a throwaway String:
let s: ArcStr = format_compact!("{key}={value}").as_str().into();

// Build an error Value:
Value::error(format_compact!("bad input: {e}").as_str());

// Field in a struct that's usually short:
struct Binding { name: CompactString, ... }
```

**API notes**

- `CompactString` derefs to `str` and implements all the usual `String`-ish
  traits (`Display`, `Debug`, `PartialEq<&str>`, `AsRef<str>`, `From<&str>`,
  `From<String>`, `FromIterator<char>`, etc.).
- Mutating API mirrors `String`: `push_str`, `push`, `clear`, `truncate`,
  `insert_str`, `replace_range`, etc.
- `CompactString::from_utf8(bytes)` / `from_utf8_lossy` for byte input.
- `.into_string()` to hand off to a foreign API that needs owned `String`
  (allocates only if currently inline).
- `ToCompactString` trait gives `.to_compact_string()` on any `Display`.

**`format_compact!` vs `format!`**

Prefer `format_compact!` essentially everywhere — it is the drop-in
replacement that keeps short outputs off the heap. The only reason to use
`format!` is when you immediately need an owned `String` for a foreign API
and the value is likely longer than 24 bytes anyway.

## ArcStr Usage Guide

`ArcStr` is the preferred immutable string type in this codebase. It is
cheap to clone (refcount bump, or free for statics), derefs to `str`, and
covers almost every "string I want to store, share, or pass around" case.
Reach for `String` only as a mutable buffer or at the edge of an API that
demands ownership.

**Constructing**

```rust
use arcstr::{literal, ArcStr};

// Zero-alloc static — use this for ANY compile-time-known string.
// Works with any &'static str expression, not just literal tokens.
let s: ArcStr = literal!("hello");
let src: ArcStr = literal!(include_str!("program.gx"));

// From an owned String — reuses the allocation (no copy).
let owned: String = make_string();
let s: ArcStr = ArcStr::from(owned);

// From &str — allocates and copies. Avoid in hot paths; prefer
// literal! if the value is known, or plumb an ArcStr through instead.
let s: ArcStr = ArcStr::from("hello");

// Empty ArcStr is a static — free.
let s = ArcStr::new();
```

**Building from formatted output**

Don't `format!` into a `String` just to convert — that allocates a `String`
you immediately throw away. The codebase uses `compact_str`:

```rust
use compact_str::format_compact;

let s: ArcStr = format_compact!("{key}={value}").as_str().into();
let v = Value::error(format_compact!("{}", e).as_str());
```

`format_compact!` produces a `CompactString` (inline for short strings, heap
only when needed); `.as_str().into()` then produces the `ArcStr`. This is
the idiomatic "formatted ArcStr" pattern in this repo.

**When to use which**

- String constants / tags / field names → `literal!(...)`
- Owned `String` you're done mutating → `ArcStr::from(s)` (reuses buffer)
- Formatted output → `format_compact!(...).as_str().into()`
- Passing strings through the Value/Pack layers → `ArcStr` throughout
- Short-lived mutable buffer → `LPooled<String>` (see above)
- Plain `String` → only at foreign-API boundaries that demand it

**Substr**

`arcstr::Substr` is a cheap view into a slice of an existing `ArcStr`,
sharing the backing allocation. Constructed via `ArcStr::substr(range)` or
`substr_from`/`substr_using`. Implements `Deref<Target = str>`, clones in
O(1) (refcount bump of the parent `ArcStr`).

Use when you need to hand out many `ArcStr`-like views into one large
string (e.g. tokens from a lexer over a source buffer, or repeated
substrings from a parsed document) and want to avoid allocating a new
`ArcStr` per view.

Not currently used in netidx, but not discouraged — just hasn't had an
obvious fit. If a good case comes up (tokenizing, parsing, slicing a large
document into many retained pieces), reach for it.

# Netidx Project Overview

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

### Windows

`netidx` and `netidx-tools` must compile for Windows. Nothing else in the repo
checks this, and it has silently rotted before — a crate reorganization moved
portable modules behind a unix gate and the Windows build stayed broken for
months. Run this alongside the normal build whenever you touch the admin crates:

```bash
rustup target add x86_64-pc-windows-gnu   # once
cargo check -p netidx-tools -p netidx-admin --target x86_64-pc-windows-gnu --all-targets
```

Baseline is zero errors and zero warnings. Gate an item `#[cfg(unix)]` only when
it genuinely needs a unix-only facility (openssl, the `SO_PEERCRED` control
socket, the daemon); gating something merely because its caller is gated pushes
the boundary the wrong way.

### The admin layering rule

`netidx-admin` owns every decision. `netidx-tools/src/admin/` (the strict CLI)
and `netidx-tools/src/admin/tui/` are presentation, and a third GUI frontend is
planned, so anything implemented in a frontend has to be written again for it.

When you are unsure which side something belongs on, ask:

> Is this about the operator's convenience, or about the system?

Convenience — bookmarks, which panel opens first, sort order, scroll position,
what to render — is UI and stays in the frontend. Anything that reads or writes
the netidx installation, decides what the system does, or states a rule about
how the system behaves is library, **as data**: a danger rule returns a
structured risk and the frontend renders the dialog; a computed default returns
a value and the frontend shows it in a field. A default is a decision — it is
the answer an unattended install uses — so defaults move. The exception is a
default that is a property of the display.

Staying in the frontend: `$EDITOR` invocation, sudo/su escalation, terminal
suspend/resume, the two `Answerer` impls, clap flag declarations, every
`println!` and widget, navigation and keymaps, and confirm-dialog *wording* (not
the predicate behind it).

Two things this went wrong through before, both of which have tests now:

- A `Field` declared in `netidx-admin::answer` but only ever answered by a
  frontend means the library named a decision and let someone else own the
  ceremony around it. `every_field_the_engine_declares_is_a_question_the_engine_asks`
  fails on that.
- A constant written out at each use rather than referenced. 730 days was in
  four places before anyone noticed.

Method joins `Answerer` only if all three frontends must answer it *and* the
strict CLI has a flag that could. Never add a generic select-from-rows,
render-a-table, toast, or refresh hook; that turns `Answerer` into a UI toolkit.

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

## Workspace Dependencies

Every external dependency MUST be a workspace dependency. Declare the
version/features once in the root `Cargo.toml` under
`[workspace.dependencies]`, and reference it from each member crate as
`foo = { workspace = true }` (add per-crate `features`/`default-features`
on the `workspace = true` line only when a crate genuinely needs them).

This rule applies to **all** dependency tables — `[dependencies]`,
`[dev-dependencies]`, `[build-dependencies]`, and target-specific
`[target.'cfg(...)'.dependencies]` — in every member crate.

The only exception is path dependencies on other crates **in this
workspace** (e.g. `netidx-core = { path = "../netidx-core", version =
"0.32.0" }`); those stay inline because they carry a `path`. External
path dependencies (e.g. the `graphix-*` crates from the sibling repo)
still go through `[workspace.dependencies]`.

When adding a new external crate to any member, add it to
`[workspace.dependencies]` first, then reference it with
`{ workspace = true }`. `cfg/tls/id-win` is `exclude`d from the
workspace and is not subject to this rule.

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
