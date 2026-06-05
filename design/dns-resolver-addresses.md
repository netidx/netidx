# DNS hostname support for netidx resolver-server addresses

**Status:** Deferred — designed, not scheduled. Too large to land at the end of the
current release cycle; intended for a future release. Design verified against the
codebase (file:line references below are accurate as of 2026-06, branch `netidx-conf`).

## Context

netidx is a name server for *values*, and resolver-server addresses are configured
as raw IP `SocketAddr` everywhere — client config, resolver-server config, and the
referral wire type. There's no technical reason resolver servers can't be named in
DNS; some sites would prefer it (change a resolver's IP without re-editing every
client/server config, round-robin / multi-A failover, human-friendly config). This
plan adds DNS hostname support end-to-end (client config, resolver-server config,
**and** federated referrals over the wire), with hostnames resolved at connection
time so long-running daemons pick up DNS changes on reconnect.

## Key enabling mechanism (verified)

The wire `Referral` is a **derived** `Pack` struct, which is length-wrapped
(`netidx-derive/src/lib.rs:113,374`; encode via `len_wrapped_encode`). Two facts make
adding a trailing field fully backward- *and* forward-compatible with **no version
bump**:

- `len_wrapped_decode` (`netidx-core/src/pack.rs:551-553`) skips any trailing bytes
  after the known fields are decoded → **old code reading a new `Referral` ignores
  the new field.**
- The derive's `#[pack(default)]` field attribute (`netidx-derive/src/lib.rs:392-401,
  415-416`) decodes a field as `Default::default()` on `BufferShort` → **new code
  reading an old `Referral` gets an empty value for the new field.**

So we add a trailing `#[pack(default)] dnsaddrs: ...` to `Referral`. This is the
existing netidx protocol-evolution idiom; the hand-written `SocketAddr` Pack impl
(`pack.rs:187-238`, not length-wrapped, hard `UnknownTag`) is deliberately *not*
touched — we add a sibling field rather than extending `SocketAddr`. The version
handshake (`== 3`, e.g. `read_client.rs:81-82`) stays as-is.

## Design overview

A resolver address is now "an IP **or** a hostname:port".

**Config: one unified list.** The config keeps its single `addrs` list; we only
*widen* the address element. Config already accepts `addrs` as `["ip:port", auth]`
where the `"ip:port"` is string-parsed into `SocketAddr` — we widen that string to
also accept `"host:port"`. Operators see no new field; existing config files are
unchanged (an `"ip:port"` string still parses, now into the `Ip` case).

**Wire: two parallel lists, for backward compat only.** The wire `Referral` can't widen
`addrs` in place (its `SocketAddr` Pack impl is hand-written and non-extensible), so the
hostname entries ride in a *separate* new trailing field. Config never exposes this
split — it's produced by partitioning the unified config list at the config→wire
boundary:

- `addrs: Vec<(SocketAddr, Auth)>` — existing IP list (unchanged; what old peers read).
- `dnsaddrs: Vec<(ArcStr, Auth)>` — NEW, hostname list where the `ArcStr` is
  `"host:port"` (parses directly via `tokio::net::lookup_host(&str)`).

Auth (`Auth::Tls { name }`, `Auth::Krb5 { spn }`) is unchanged and already independent
of the address (confirmed: TLS SNI comes from `Auth::Tls{name}` at
`read_client.rs:151`, not from the connect IP) — so DNS does **not** touch TLS/Kerberos
identity verification.

Resolution happens **at connect time** in the resolver client: each `dnsaddrs` entry
is resolved to one-or-more `SocketAddr` and merged with `addrs` into the existing
shuffle / round-robin / `bad_addrs` candidate loop. Multiple A records → multiple
candidates → free failover; reconnects re-resolve → DNS changes are picked up.

## Changes

### 1. Wire type — `netidx-netproto/src/resolver.rs`

Add the trailing field to `Referral` (currently lines 210-215):

```rust
#[derive(Clone, Debug, Pack)]
pub struct Referral {
    pub path: Path,
    pub ttl: Option<u16>,
    pub addrs: GPooled<Vec<(SocketAddr, Auth)>>,
    #[pack(default)]
    pub dnsaddrs: Vec<(ArcStr, Auth)>,   // "host:port" + auth
}
```

Update the hand-written `Hash`/`PartialEq` (lines 217-231) to also fold in `dnsaddrs`,
so the resolver client's `by_server: HashMap<Arc<Referral>, _>` cache treats referrals
that differ only by hostname as distinct.

Update the proptest generator `referral()` in `netidx-netproto/src/test.rs` (~line 89,
251-262) to populate `dnsaddrs` in some cases so the round-trip test exercises it.

### 2. Config types — unified address that parses IP or hostname

Introduce a small `ResolverAddr` enum (in `netidx-core`, alongside `utils::check_addr`,
so both client and resolver-server config reuse it):

```rust
enum ResolverAddr { Ip(SocketAddr), Dns { hostport: ArcStr } }
```

- `FromStr`/`Display`/serde-as-string: try `SocketAddr::from_str` first (covers
  `1.2.3.4:4564` and `[::1]:4564`); on failure, validate a `host:port` shape
  (non-empty host, parseable `u16` port) and store as `Dns`. Existing config files
  (`"1.2.3.4:4564"`) deserialize to `Ip` and re-serialize byte-identically — preserves
  the `netidx-conf` round-trip test.
- Use it for the *config* address lists (operators get one friendly `addrs` list that
  accepts both forms):
  - **Client**: `netidx/src/config/mod.rs` `file::Config.addrs` and `Config.addrs`
    (lines 103, 312) → `Vec<(ResolverAddr, Auth)>`.
  - **Resolver-server**: `netidx/src/resolver_server/config.rs` `file::Referral.addrs`
    (parent/children) and `MemberServer.addr`. (`bind_addr` stays `IpAddr` — you bind a
    concrete interface.)

At the config→wire boundary, **partition** `Vec<(ResolverAddr, Auth)>` into the wire's
`addrs` (the `Ip` entries) and `dnsaddrs` (the `Dns` entries):
- Client: `Config::to_referral()` (`config/mod.rs:404`) builds both lists for the
  client's own default referral; the client then resolves `dnsaddrs` at connect.
- Server: `file::Referral::check()` (`resolver_server/config.rs:286-315`) and referral
  construction (`mod.rs:874-879`, `store.rs:257-279`) build both lists for federation
  referrals it hands out.

### 3. Connect-time resolution — `netidx/src/resolver_client/`

In `read_client.rs::connect()` (lines 38-79) and the `write_client.rs` connect path,
before the existing shuffle/round-robin loop, build the candidate list:
- start from `resolver.addrs` (IP, as today), then
- for each `(hostport, auth)` in `resolver.dnsaddrs`, `tokio::net::lookup_host(&*hostport)`
  (async — **never** `std::net::ToSocketAddrs`, which blocks) and push each resolved
  `(SocketAddr, auth.clone())`. Wrap the lookup in a short timeout; on failure log a
  warning and skip that entry (a mix of one bad name + one good addr still connects).
- if the merged candidate list is empty, bail (existing retry semantics apply).

`bad_addrs: AHashSet<SocketAddr>` keeps keying on the resolved `SocketAddr` — works
unchanged and gives per-resolved-IP failover. The per-attempt connect body
(version handshake + auth) is unchanged; factor it into a helper taking
`&[(SocketAddr, Auth)]` if it reduces duplication between read/write paths.

Write path note: `secrets`/ownership are keyed on the server-returned `resolver_id`
(an IP, `write_client.rs:385`), not the pre-resolution address, so re-resolution to a
new IP doesn't corrupt the secret cache. Keep that keying.

### 4. Validation — `netidx/src/config/mod.rs::from_file` and `resolver_server/config.rs::check_addrs`

For `ResolverAddr::Dns` entries:
- skip the IP-level `utils::check_addr` (no IP at config time);
- **disallow `Local` auth with a `Dns` address** (local auth is a same-host unix-socket
  token mechanism — a hostname is semantically wrong); clear bail message;
- the existing "can't mix loopback with non-loopback" check (`config/mod.rs:365-369`)
  applies only across the `Ip` entries; treat a `Dns` entry as non-loopback intent
  (so `Dns` + loopback `Ip` bails, matching the existing local-only-vs-remote intent).
- TLS identity match (`config/mod.rs:345-356`) is unchanged — keyed on `Auth::Tls{name}`.

### 5. Backward-compat for federation (note, minor)

Old clients only read `addrs`. For a hostname-only **federation referral** to be
followable by an old client during a rolling upgrade, either: (a) operators also list
an IP alongside the hostname in the server's referral config, or (b) the server
resolves `Dns` entries into `addrs` at referral-build time. Recommend (a) for
simplicity (keeps the server free of DNS); document it. New clients always prefer
`dnsaddrs`. The client-bootstrap case (the primary ask) has no such concern — the
client consumes its own config directly.

## Critical files

- `netidx-netproto/src/resolver.rs` — `Referral` field + `Hash`/`PartialEq`; `test.rs` generator.
- `netidx-core/src/utils.rs` (or new `netidx-core/src/resolver_addr.rs`) — `ResolverAddr` type + serde + resolve helper; `check_addr` relaxations.
- `netidx/src/config/mod.rs` — client `addrs` type, `from_file` validation, `to_referral` partition.
- `netidx/src/resolver_server/config.rs` — `file::Referral.addrs` / `MemberServer.addr` type, `check_addrs`, `check()` partition.
- `netidx/src/resolver_client/read_client.rs` + `write_client.rs` — connect-time resolution + candidate merge.
- `netidx/src/resolver_server/store.rs` / `mod.rs` — referral construction populating both lists.
- `netidx-conf/src/client.rs`, `resolver.rs` (+ `template/`) — config tests/builders that use `"...".parse()` now target `ResolverAddr` (still works via `FromStr`); add hostname round-trip/validate tests.

## Verification

- **Unit**: `ResolverAddr` parse/serde round-trip for `1.2.3.4:4564`, `[::1]:4564`,
  `host.example.com:4564`, and error cases (no port, bad port, empty host). Config
  `from_file` accepts a `Dns` addr with Anonymous/Krb5/Tls and rejects `Dns` + Local
  and `Dns` + loopback-IP mixing.
- **Wire compat**: a Pack round-trip test proving (i) an old-shaped `Referral` (no
  `dnsaddrs`) decodes into a new `Referral` with empty `dnsaddrs`, and (ii) a new
  `Referral` with `dnsaddrs` encodes and an old-shaped decoder (decode only path/ttl/
  addrs within the len-wrap) ignores the trailing bytes. Extend the existing
  `netidx-netproto` proptest.
- **End-to-end**: run a resolver on `127.0.0.1:<port>` (anonymous); point a client
  config `addrs` at `localhost:<port>` (exercises multi-A: `127.0.0.1` + `::1`); run a
  publisher + subscriber over the hostname and confirm data flows. Negative: a
  `nonexistent.invalid:<port>` entry yields a clean "resolving … failed" warning and
  retry, not a panic/hang. Federation: configure a parent/child referral with a
  hostname and confirm a new client follows it.
- `cargo build`/`cargo test`/`cargo clippy` on `netidx-core`, `netidx-netproto`,
  `netidx`, `netidx-conf`.

## Explicitly out of scope / risks

- No protocol version bump (verified unnecessary). Keep the `== 3` handshake.
- DNS lookups must be async (`tokio::net::lookup_host`); a hung resolver must not stall
  the connect cycle (wrap in a timeout).
- `Referral` `Hash`/`PartialEq` MUST include `dnsaddrs` or the `by_server` connection
  cache will conflate distinct referrals.
