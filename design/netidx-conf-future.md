# `netidx-conf` — design notes for future work

This document captures design for features that were scoped out of the
`netidx-conf` v1 (which ships templates, basic config manipulation, the
CLI, and resolver-server SIGHUP-based perms reload). The intent is to
preserve the design direction so future PRs can pick it up without
re-litigating the broader vision.

Status legend (left column of each section):
- **Designed** — detailed enough to start implementing.
- **Sketched** — directional, expect more design work before code.

---

## CA module (`netidx-conf::ca`) — partially landed

A minimal openssl-backed CA module shipped in v1:
`Ca::init` / `Ca::open` / `Ca::sign_request` / `Ca::issue` plus a
free `generate_csr` helper. Just enough to stand up a CA, generate
keys + CSRs with SANs, and sign — the typed replacement for the
hand-rolled shell scripts in `cfg/tls/*/gen.sh`. The cert profiles
(extensions, key usage, basic constraints) match the shell scripts
verbatim so the resulting certs continue to load through the existing
`netidx::tls` validators.

### Deferred bits (not in v1)

- **CRL generation + revocation.** v1 has no `revoke()` and no
  `crl()`. The CA serial counter is tracked but no revocation list is
  produced.
- **Hardware token / PKCS#11 backing.** v1 supports optional PKCS#8
  AES-256-CBC encryption of the on-disk root key (password-prompted
  by the CLI), but the key still has to land in memory plaintext when
  the CA signs. Hardware-token backing keeps the key off the host
  entirely.
- **Remote-network CA operations.** v1 CA is local-machine only.
- **`Ca::trust_into_*` config-wiring helpers.** v1 callers wire the
  CA's `certificate.pem` path into configs manually. A future tidy-up
  can add the convenience method.
- **CA inventory / index file.** v1 has no `index.txt` tracking
  every issued cert; the serial counter alone is kept. Re-issuance
  via `Ca::issue` works without an index, and a future audit log
  (for "what did this CA sign") is a separate piece.
- **`--tls-auto` flag on `init` templates.** v1 templates still
  require explicit `--tls-cert/--tls-key/--tls-trusted`. Wiring
  the CA module into `--tls-auto` is a small follow-up — the API
  is now in place.

---

## id-map daemon (`netidx-id-map`) — landed

The id-mapper daemon shipped: `netidx id-map serve` listens on a unix
socket, answers `IdMapType::Socket` queries from the resolver, and
holds the parsed map in memory behind an `RwLock<Arc<IdMap>>` with
SIGHUP-triggered reload. `netidx conf id-map …` edits the JSON; the
`standalone-resolver --auth tls` template auto-installs an `id-map.unit`
and a starter `id-map.json` alongside the resolver (opt out with
`--no-id-map`).

The runtime trade with `arc_swap`-based file-watching (the original
design) was traded for SIGHUP reload to match the resolver and avoid
adding the `notify` dep. If at-edit-time reload turns out to matter,
switch the reload trigger to a `notify` watcher; the swap mechanism
is already in place.

Deferred bits:

- **Group membership transitivity.** Groups today are flat; adding a
  group to a group (LDAP-style nesting) is not supported.

Note on uid lookups: the daemon's `format_id_line_for_uid` path is
preserved for wire-protocol compatibility with the resolver's
`Mapper::user(uid)` helper, but in every shipped deployment that path
is unreachable. The resolver's request handling only ever feeds names
into `Mapper::groups(name)`; the only `Mapper::user(uid)` call site is
the Local-auth peer-credentials translator (`os/unix.rs`), which the
workstation template wires through `IdMapType::Command` (`/bin/id`),
not the Socket id-mapper. Duplicate uids in the JSON map are therefore
harmless — `lookup_by_uid` returns the first match but no resolver
code path queries by uid.

### Historical design (kept for context)

The resolver's existing `IdMapType::Command` mode forks `/bin/id` (or a
configured binary) per TLS handshake. That doesn't scale: every TLS
handshake pays fork + exec + JSON parse + process exit. The daemon
design parses once, holds the map in memory behind `arc_swap::ArcSwap`,
watches the config file for edits, and answers queries over a unix
socket using the resolver's existing `IdMapType::Socket` protocol.

**The wire protocol is identical to the existing socket protocol** the
resolver already implements (`netidx/src/os/unix.rs:57-63` and `:82-88`):

```
Resolver opens UnixStream::connect(socket).
Resolver writes "<query>\n"  where <query> is a name (TLS SAN) or a
  decimal uid.
Daemon writes a /bin/id-style line:
  uid=N(name) gid=M(primary) groups=M(primary),G1(g1),G2(g2),...
Daemon closes the connection. Resolver reads to EOF.
```

The daemon distinguishes name-vs-uid queries by trying
`query.parse::<u32>()`. No schema change to the resolver's
`id_map_command` is required — the existing `Option<ArcStr>` carries
the socket path in `Socket` mode.

### JSON schema (`~/.config/netidx/id-map.json`)

```json
{
  "$default_uid": 65534,
  "$default_gid": 65534,
  "groups": {
    "wheel": { "gid": 10 },
    "adm":   { "gid": 4 },
    "users": { "gid": 100 }
  },
  "identities": {
    "alice.example.com":    { "uid": 1000, "primary_group": "users", "groups": ["wheel", "adm"] },
    "resolver.example.com": { "uid": 100,  "primary_group": "users", "groups": [] }
  }
}
```

### Crate layout (mirrors `netidx-activation`)

```
netidx-id-map/
  Cargo.toml
  src/
    lib.rs           — re-exports + module decls
    file.rs          — JSON schema (IdMap struct, derive_builder)
    runtime.rs       — Server / ServerParams, the unix socket loop
                       and the file-watch task
```

`netidx-conf::id_map` re-exports `netidx_id_map::file::*` and adds the
engine-side conveniences (atomic save in the right location, builder
shells, structural validation).

### Daemon CLI

```
netidx id-map serve [--socket <path>] [--config <path>] [-f] [--pid-file <path>]
```

Defaults: `--socket` → `~/.config/netidx/id-map.sock` (or
`/var/run/netidx/id-map.sock` for system installs); `--config` →
`$NETIDX_ID_MAP_FILE` or `~/.config/netidx/id-map.json`.

### Engine API (`netidx-conf::id_map`)

```rust
pub struct IdMap { /* parsed schema, plus a uid → name reverse index */ }

impl IdMap {
    pub fn load(path: &Path) -> Result<Self>;
    pub fn save(&self, path: &Path) -> Result<()>;
    pub fn lookup_by_name(&self, name: &str) -> Option<IdRecord>;
    pub fn lookup_by_uid(&self, uid: u32)  -> Option<(ArcStr, IdRecord)>;
    pub fn set(&mut self, name: &str, rec: IdRecord);
    pub fn remove(&mut self, name: &str);
    pub fn add_group(&mut self, name: &str, group: &str) -> Result<()>;
    pub fn remove_group(&mut self, name: &str, group: &str) -> Result<()>;
    pub fn format_id_line_for_name(&self, name: &str) -> String;
    pub fn format_id_line_for_uid(&self, uid: u32) -> String;
}
```

### File-watching

The daemon owns the parsed `IdMap` behind `arc_swap::ArcSwap`. A
separate task watches the directory containing the config (not the file
inode — atomic save replaces the inode) using the `notify` crate,
debounces events ~200ms, then reloads:

```rust
loop {
    tokio::select! {
        _ = shutdown.recv() => break,
        ev = watch_rx.recv() => {
            if affects_config(ev) {
                debounce.tick().await;
                match IdMap::load(&config_path) {
                    Ok(new) => map.store(Arc::new(new)),
                    Err(e)  => warn!("reload failed, keeping old: {e}"),
                }
            }
        }
    }
}
```

Reload errors *do not* swap the map — a malformed edit can't lock out
every TLS identity. Last-known-good keeps serving.

### Bootstrap

The daemon needs to be running before the resolver accepts its first
TLS connection. Standard path: `netidx conf init standalone-resolver
--auth tls` drops `resolver.unit` and `id-map.unit` into the activation
unit directory; the activation supervisor starts both.

The workstation template does **not** need the id-map daemon — its
local resolver uses Local auth (peer credentials over a unix socket),
and uid → group lookup goes through `/bin/id`.

---

## Designed: OS service install (`netidx-conf::service`)

The activation supervisor (Layer 0, done) is great at supervising netidx
daemons but it has the same chicken-and-egg problem every supervisor
has: *who supervises the supervisor*. The OS does — `systemd` on linux,
`launchd` on macOS, the Service Control Manager on windows. The engine's
`service.rs` module installs `netidx-activation` as a service unit at
the right scope and trigger so the user doesn't have to write
init-system files by hand.

Per-template policy:
- **Workstation** ⇒ user scope, login-triggered. Activation supervisor
  comes up at user login, supervises the local resolver (and, if and
  when the id-map daemon ships, the id-map daemon).
- **Standalone resolver** ⇒ system scope, boot-triggered. Supervisor
  comes up at boot, runs as the netidx service user.

### Engine API

```rust
pub enum Scope { System, User }
pub enum Trigger { MachineStartup, UserLogin }

pub struct ServicePlan {
    pub scope: Scope,
    pub trigger: Trigger,
    pub binary: PathBuf,            // path to `netidx`; default: $PATH then /proc/self/exe
    pub units_dir: PathBuf,         // passed to `netidx activation -u`
    pub run_as_user: Option<String>, // system scope only; engine never creates users
}

impl ServicePlan {
    pub fn install(&self) -> Result<()>;
    pub fn uninstall(scope: Scope) -> Result<()>;
    pub fn status(scope: Scope) -> Result<ServiceStatus>;
}

pub enum ServiceStatus {
    NotInstalled,
    Installed { enabled: bool, active: bool },
}
```

### Per-platform implementation

- **Linux (systemd).** Write `netidx-activation.service`; user scope to
  `${XDG_CONFIG_HOME:-~/.config}/systemd/user/`, system scope to
  `/etc/systemd/system/`. `ExecStart` is `${binary} activation -f -u
  ${units_dir}`. System scope adds `User=${run_as_user}` and
  `Group=${run_as_user}`. Then `systemctl [--user] daemon-reload`
  followed by `systemctl [--user] enable --now`. User triggering uses
  `WantedBy=default.target` with the systemd-user session started at
  PAM login; system uses `WantedBy=multi-user.target`.
- **macOS (launchd).** Write `com.netidx.activation.plist`; user scope
  to `~/Library/LaunchAgents/`, system scope to
  `/Library/LaunchDaemons/`. Activate with `launchctl load -w`.
- **Windows (SCM).** v1.x is best-effort: `sc create
  netidx-activation binPath= "..." start= auto` for system scope; user
  scope via Task Scheduler `AT LOGON` trigger.

`service-manager` (the cross-platform abstraction crate) is **not**
adopted — it doesn't capture the Launch Agent vs Launch Daemon
distinction we need.

### Idempotence

`install` is idempotent: same content ⇒ no-op; different content ⇒
overwrite (engine owns the well-known `netidx-activation` name).
`uninstall` removes + disables + deletes, also idempotent. The engine
never touches services it didn't install.

---

## Sketched: configuration server (Layer 4)

A single daemon (`netidx conf serve`) that does three things, intended
to run **one instance per resolver-server machine**, coordinating with
peers via the `netidx-protocols` cluster protocol so any one of them
can serve a config-bootstrap request:

1. **HMAC-authenticated certificate issuance.** Administrators
   pre-configure `(username, password)` credentials. A new machine
   running `netidx conf init` either uses an explicit
   `--conf-server <addr>` or — by default — performs multicast
   discovery. The CLI exchanges SCRAM-SHA-256 with the server; on
   success the server signs a CSR generated locally on the new machine
   and returns a signed cert + the CA certificate.
2. **Broadcast / multicast bootstrap discovery.** `init` tries this by
   default whenever an explicit `--conf-server` was not supplied. The
   server listens on a well-known multicast address; the new machine
   multicasts `Discover` and the first listener whose random timer fires
   answers with `Announce { server_addr, server_cert_sha256 }`.
3. **Resolver permissions publisher + RPCs.** The conf server is also
   the long-running owner of the resolver perms file: it publishes the
   current perms tree under netidx, exposes RPCs for editing them, and
   publishes a Graphix permissions editor at `<base>/.view`. The
   resolver server picks up changes via SIGHUP (Part B of v1) when the
   conf server writes the perms file. The runtime PMap-swap path
   already exists.

### Wire protocol (sketch)

**Discovery** (UDP, multicast `239.255.x.y` link-local; port TBD):
- `Discover { client_id, kind: Workstation|Resolver|Client, version }`
- `Announce { client_id, server_addr, server_cert_sha256 }` — first
  listener wins; others cancel.

**Issuance** (TCP, TLS with self-signed conf-server cert, pinned via
the SHA-256 from `Announce`):
- `Hello { protocol_version }`
- `BeginAuth { username }`
- `AuthChallenge { salt, iter_count, server_nonce }`  — SCRAM s/iter/nonce
- `AuthClientProof { client_nonce, client_proof, csr }`
- `AuthServerSig { server_signature, signed_cert, ca_cert }` on success,
  or `AuthFail { reason }`.

### Admin credential storage

`~/.config/netidx/conf-server/admins.json`, mode 0600:

```json
{
  "admins": {
    "alice": {
      "scram_salt": "<base64>",
      "scram_iter": 600000,
      "scram_stored_key": "<base64>",
      "scram_server_key": "<base64>",
      "allowed_san_patterns": ["*.example.com"],
      "max_validity_days": 730
    }
  }
}
```

`netidx conf admin add <user>` prompts for the password and derives
all four SCRAM fields per RFC 5802. The plaintext password is never
written to disk.

### Trust model and what's deferred

- **Bootstrap trust on first connect.** Conf-server's own cert is
  delivered via the discovery `Announce` (fingerprint). On a hostile
  network the discovery channel can be spoofed; `--conf-server-pin
  <sha256>` short-circuits this.
- **Replay defenses.** SCRAM nonces handle authentication replay; CSR
  reuse is prevented by binding the cert's serial to the authenticated
  session.
- **Online revocation / OCSP.** Out of scope; revocation via CRL only.
- **Conf-server clustering.** One per resolver-server machine,
  coordinating via the existing cluster protocol in `netidx-protocols`.
- **Cross-subnet discovery.** Multicast only crosses subnets with an
  explicit relay. Multi-subnet networks use explicit `--conf-server
  <addr>`.

`init`-time discovery flags:
- `--no-discovery` — skip the multicast probe.
- `--conf-server <addr>` — use this address explicitly, no probing.
- `--conf-server-pin <sha>` — only accept a discovered (or explicit)
  conf-server whose cert SHA-256 matches.

---

## Sketched: publishers + admin GUIs (Layer 5)

- **Perms publisher.** This is the conf server (Layer 4). Exposes the
  resolver's PMap under a configurable subtree, accepts writes, persists
  to disk, triggers SIGHUP. Auth via the resolver's TLS / Krb5 —
  admin group only.
- **Activation publisher.** Built into the activation supervisor itself
  rather than running as a separate publisher. Mirrors the unit dir on
  a host; same RPC shape.
- **Graphix browser admin screens.** Text-mode (TUI) and iced. Consumes
  the perms / activation publishers and drives `netidx-conf` for
  client-side editing.
- **CA management publisher.** Not recommended for remote use — CA
  private key handling over the network is a separate trust problem.
  Local-only GUI on top of `netidx-conf::ca` is fine.

---

## Cross-cutting: identity-management migration path

The id-map JSON + conf-server issuance scheme is "poor man's IdM" —
sufficient for small organizations (tens of machines, dozens of users).
Larger organizations migrate to Kerberos backed by FreeIPA / Active
Directory / OpenIDM, which netidx already supports natively. The plan
is designed so that migration is a config swap (auth scheme +
per-resolver `id_map_type`) rather than a re-architecture.
