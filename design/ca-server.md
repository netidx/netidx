# CA server — design

> **Superseded in part by [`admin-server.md`](admin-server.md).** The CA
> server has been generalized into the **admin server**: one per-host
> daemon with roles (`ca`, `resolver`, `id-map`), mDNS discovery, a
> `GetInfo` protocol, trust domain enrollment, and CA-pushed id-map
> registration. Module renames: `ca_proto` → `conf_proto`, `ca_join` →
> `conf_client`, `ca_server` → `admin_server`; the reserved serving SAN
> is now `netidx-admin-server`; the daemon config is `admin-server.json`
> (`netidx admin component server run`). The vault (§ keyslots), signing engine,
> issuance policy, and the two-connection fingerprint trust model below
> are unchanged and remain authoritative.

**Status: Engine + CLI + activation/service setup + composite
resolver-install flow implemented and tested end-to-end.** Remaining is
only `ca migrate` (v1 → vault). `netidx admin ca init` and the
`netidx admin resolver install` "create a new CA" branch now share one
entry point, `create_vaulted_ca` (§15), so creating a CA is identical
either way — vault, identicon, and the "set up the CA server?" prompt.

Built and tested:
- **Engine** (`netidx-admin-server`, real-TLS e2e): `fingerprint` (identicon),
  `ca_vault` (LUKS keyslots), `ca_proto`, `ca_join` (TOFU client + CSR),
  `ca_server` (policy + sign + TLS serve), plus `Ca::from_pem` /
  `Ca::init_vaulted` and an algorithm-aware CSR strength check. The
  server returns the full **trusted bundle**, so a join installs trust
  with no file copying.
- **CLI** (`netidx admin ca`): `init` (vaulted CA + identicon + serving
  cert + `server.json`), `serve`, `admin add|remove|list`,
  `fingerprint`, `join`. Smoke-tested via PTY: init → serve → join
  installs `certificate.pem` / `private.key` / `trusted.pem` and writes
  an audit line; the identicon shown at join matches the one at init.
- **Template integration** (`admin init … --auth tls`): the generate
  paths (`resolver_tls_generate`, `prompt_tls_client_identity`) first
  offer "get this cert from a CA server?", seeded with the upstream
  resolver IP; yes → join over the network, no → the existing local-CA /
  CSR flow. Cross-platform, so it's also how a Windows node gets a TLS
  identity.

`ca init --with-server` now also drops the `ca` activation unit and,
standalone, offers to register the activation supervisor as a system
service through the **one** shared entry point all the install
templates use. All sub-decisions locked (§13); implementation notes in
§14; the modular service-setup design in §15.

## 15. Service-setup composition

Multiple setup steps can each install activation units that need
unattended supervision, but the activation supervisor should be
registered as an OS service **once** per top-level process. The design:

- **`ServiceNeed`** (`admin/service.rs`): `None | User | System`, with a
  `merge` that ranks `System > User > None`. A setup step returns the
  need its units imply (a CA server → `System`; a resolver → `System`;
  a workstation → `User`; a client-only publisher → `None`).
- **One entry point**, `service::offer(need, gate)`: given the merged
  need and the `--dry-run/--no-service/--with-service` gate, it offers
  (or installs, or skips) the OS service exactly once. Both the
  `admin install` templates (via `finish`) and `ca init` call it.
- **Composition**: a flow that stands up several daemons drops all their
  units into one activation dir and offers a single service.
  **Implemented**: `netidx admin resolver install` creating a CA calls
  the shared `create_vaulted_ca` (the same entry point as
  `netidx admin ca init`), which writes the `ca.unit` into the resolver's
  *own* activation dir; the resolver install then makes its single
  system-service offer, and that one supervisor runs the resolver,
  id-map, and CA-server units together. The resolver always needs a
  system service (it installs `resolver.unit`), so it passes
  `ServiceNeed::at(System)` directly and discards the CA's returned
  need — `System` dominates regardless. `ServiceNeed::merge` remains the
  seam (tested, `allow(dead_code)`) for a future flow whose dominant
  scope isn't fixed up front.

### One entry point for CA creation

`create_vaulted_ca(opts) -> (Ca, ServiceNeed)` is the single new-CA
workflow. `ca init` and the resolver-install "create a new CA" branch
both call it, so the operator gets the identical experience (admin +
policy, identicon, "set up the CA server?"). It returns the in-memory
signer (the resolver issues its own identity from it before it drops)
and the service need (which `ca init` offers and the resolver folds into
its own). `open_default_ca` / `default_ca_present` are vault-aware, so
each command sees the other's CAs.

A daemon on the CA box that receives a CSR over the wire, signs it, and
returns the signed cert plus the CA cert — so a node joining a netidx
deployment with TLS auth gets its identity issued in one shot, with no
manual shuttling of CSR / cert files between machines.

This supersedes the *certificate-issuance* half of the "Layer 4
configuration server" sketch in
[`netidx-admin-future.md`](netidx-admin-future.md) (multicast discovery +
per-admin SCRAM). Perms-publishing and admin RPCs remain future Layer-4
work; this design is a **standalone CA-signing daemon** only.

---

## 1. What it replaces

> **Update:** the manual-CSR wizard path described below
> (`generate_csr_and_wait_for_cert` and the BYO-cert prompts) has since
> been **removed entirely** — the `admin install` wizard now always uses
> the netidx CA and enrolls over the admin plane. Bringing your own cert is
> a self-managed setup done outside the wizard. This section is kept as
> design history for how the CA server subsumed the old flow. For chaining
> the netidx CA to an existing PKI, see *Externally-signed CA* at the end.

Today the TLS "generate" path without a local CA on the joining box is
`generate_csr_and_wait_for_cert` (`netidx-tools/src/admin/init.rs`): it
writes a key + CSR locally, prints

```
Send <name>.csr to your CA admin to sign.
Place the signed certificate at: ~/.config/netidx/tls/<name>/certificate.pem
Place the trusted-CA bundle at:   ~/.config/netidx/tls/<name>/trusted.pem
```

…and **blocks** until the operator carries the CSR to the CA box, gets
it signed, and copies two files back. The CA daemon turns that whole
out-of-band loop into one prompt:

```
$ netidx admin init resolver --auth tls
resolver TLS name: resolver.ryu-oh.org
resolver certificate [local-ca, ca-server, manual-csr]: ca-server
CA server address [192.168.1.10:4565]:           # default = upstream resolver IP : CA port

The CA presented this identity — verify it matches what your CA admin gave you:

    SHA256  MFRGG ZDFMZ TWQ2L K5JEU ...
    ┌────────────────┐
    │ ▞▞    ██    ▞▞ │     (colored identicon)
    │   ████    ████ │
    │ ▞▞  ██  ██  ▞▞ │
    │   ██  ████  ██ │
    └────────────────┘

Does this match? [y/N]: y
CA admin password: ********
issued resolver certificate 'resolver.ryu-oh.org' from ca-server (192.168.1.10:4565)
```

The signed cert, key, and CA bundle land directly in the canonical TLS
identity dir via the existing install machinery — no files moved by hand.

---

## 2. Architecture

Three pieces:

- **CA box.** Holds the CA directory (cert + vault-encrypted key +
  keyslots + serial + audit log) and runs `netidx admin ca serve`,
  installed as an activation unit. Unix-only (the signing engine is
  openssl-backed, like the rest of `netidx-admin::ca`).
- **Joining node** (the CLI). Runs cross-platform — **including
  Windows**. It generates the keypair + CSR locally, connects to the CA
  daemon over TLS, verifies the CA identity by eyeball (identicon),
  sends the CSR + an admin password, and installs the returned cert.
  Because the joining node must run on Windows, **its half uses
  `rustls` + `sha2`, never openssl.**
- **The wire** between them: a bespoke length-prefixed TLS protocol
  (not netidx pub/sub — that would be circular: you have no cert yet).

Trust bootstrap is **TOFU verified by a human-comparable fingerprint**:
the joining node accepts the daemon's cert chain on first contact,
extracts the self-signed CA cert at its root, shows the operator its
fingerprint + identicon, and proceeds only after the operator confirms
it matches what the CA admin displayed at `ca init`. The CA cert is then
pinned locally; the admin password is only ever sent *after* that
confirmation, so it can't be harvested by an impostor daemon.

```
                        ┌──────────────────────── CA box (unix) ─────────┐
  joining node          │  netidx admin ca serve  (activation unit)        │
  (any OS)              │     ├─ certificate.pem      (CA cert, public)   │
    │  TLS connect      │     ├─ key.enc              (CA key, MK-wrapped) │
    │ ───────────────►  │     ├─ keyslots.json        (LUKS-style slots)   │
    │  cert chain       │     ├─ serial               (counter)            │
    │ ◄───────────────  │     ├─ audit.log            (append-only)        │
    │  [verify identicon, confirm]                                         │
    │  SignRequest{csr, admin, password, san, validity}                   │
    │ ───────────────►  │     unlock slot → MK → decrypt CA key → sign →   │
    │  SignResponse{cert, ca_cert}        zeroize; append audit line       │
    │ ◄───────────────  │                                                  │
    └─ install into ~/.config/netidx/tls/<name>/   └────────────────────────┘
```

---

## 3. Key storage: the keyslot vault (LUKS-style)

The CA private key is **never** encrypted directly with an admin
password. Instead, exactly like LUKS:

- At `ca init` we generate a random 32-byte **master key (MK)** from the
  CSPRNG.
- The CA private key (PKCS#8) is encrypted **with MK** → `key.enc`.
- MK is then **wrapped** (encrypted) once per admin, each wrap keyed by
  a password-derived key, and stored as a **slot** in `keyslots.json`.

This buys exactly what LUKS slots buy:

- **Multiple admins**, each with their own password.
- **Revoke an admin** by deleting their slot — *without* re-encrypting
  the CA key or redistributing anything to the other admins.
- The CA key plaintext only ever exists in daemon memory for the
  duration of one signing operation, then is zeroized. Nothing on the CA
  box, at rest, is sufficient to sign without an admin password.

### On-disk layout (`<ca-dir>/`)

```
certificate.pem     CA cert (public, unchanged from today)
key.enc             CA private key (PKCS#8), AES-256-GCM under MK
keyslots.json       the header: cipher params + one slot per admin
serial              serial counter (unchanged)
audit.log           append-only: who issued what, when (new)
```

`key.enc` (JSON or a small binary frame):

```jsonc
{ "cipher": "aes-256-gcm", "nonce": "<b64>", "ct": "<b64+tag>" }   // = AES-256-GCM(MK, nonce, ca_key_pkcs8_pem)
```

`keyslots.json`:

```jsonc
{
  "version": 1,
  "slots": [
    {
      "admin": "alice",
      "kdf":   { "type": "argon2id", "salt": "<b64>", "m_cost_kib": 65536, "t_cost": 3, "p_cost": 4 },
      "wrap":  { "nonce": "<b64>", "ct": "<b64+tag>" },   // = AES-256-GCM(KEK, nonce, MK)
      "policy": { "allowed_san": ["*.ryu-oh.org"], "max_validity_days": 730 }
    }
  ]
}
```

- **KEK** = `Argon2id(password, slot.salt, m/t/p)` → 32 bytes.
- **wrap.ct** = `AES-256-GCM(KEK, wrap.nonce, MK)`. The GCM auth tag *is*
  the password-correctness check — a wrong password produces a KEK that
  fails the tag, so there is no separate password verifier to leak.
- Per-slot **policy** (see §4) travels with the admin identity, so
  authority and what-they-can-issue are one record.

### Unlock (the only hot operation, per sign request)

```
for slot in keyslots.slots:
    KEK = Argon2id(password, slot.kdf)
    if MK = aead_open(KEK, slot.wrap):          # tag verifies?
        ca_key = aead_open(MK, key.enc)
        return (slot, ca_key)                    # matched admin + policy + key
return WrongPassword                             # no slot accepted it
```

`MK`, `KEK`, `ca_key`, and the password buffer are `zeroize::Zeroizing`
— wiped on drop. The decrypted `ca_key` lives only long enough to build
a transient `Ca` (`Ca::from_pem`, a new in-memory constructor), sign one
CSR via the existing `Ca::sign_request`, and drop.

### Slot operations (CLI, on the CA box)

- `ca init` → create MK, encrypt key, write slot 0 for the first admin.
- `ca admin add <name>` → unlock MK with an **existing** admin password,
  derive a KEK from the **new** admin's password, write a new slot +
  policy. Never touches `key.enc`.
- `ca admin remove <name>` → delete that slot. Requires unlocking with a
  *different* admin's password (proof of authority); refuses to remove
  the last slot unless `--force` (don't lock the CA permanently).
- `ca admin list` → admins + their policies (no secrets).

### Migration from the v1 single-password CA

v1 wrote the CA key as a single PKCS#8 EncryptedPrivateKeyInfo. A
one-shot `netidx admin ca migrate` opens it with the old password,
generates an MK, rewraps into the vault, and writes slot 0 — **the CA
cert and key material are unchanged**, so the fingerprint/identicon and
every already-issued cert stay valid. Unencrypted v1 CAs (plaintext key)
stay as-is; the daemon simply refuses to serve an un-vaulted CA.

---

## 4. Issuance policy (per slot)

Knowing an admin password authorizes issuance, but **not for any name**.
Each slot carries:

- `allowed_san`: glob patterns the requested SANs must all match (e.g.
  `*.ryu-oh.org`). Empty ⇒ deny all (must be set explicitly; no
  accidental allow-all).
- `max_validity_days`: caps `requested_validity_days`.

The daemon, after identifying the slot, checks every requested SAN
against `allowed_san` and clamps validity, **before** decrypting the CA
key. A request outside policy is rejected with a clear reason and never
reaches the signer. The CA remains the sole authority on SAN content
(the existing `sign_request` already overrides whatever the CSR claims).
Every issuance appends `{ts, admin, subject, san, serial, validity}` to
`audit.log`.

This is the LUKS analogy paying off twice: a slot is *both* an
authentication factor and an authorization scope, and revoking an admin
revokes their issuing rights atomically.

---

## 5. Visual identity verification (identicon)

The linchpin that makes "send the admin password over the wire" safe is
the operator confirming, out of band, that they're talking to the real
CA. We borrow syncthing's trick: a **colored identicon** plus the text
fingerprint — distinct CAs look obviously different at a glance.

- **Input**: `SHA-256(CA cert DER)`. Computed with the `sha2` crate (not
  openssl) so it runs identically on the joining node, Windows included.
- **Fingerprint text**: base32, uppercase, space-grouped, shown in full
  for careful compares and as a short head for casual ones.
- **Identicon**: an 8×8 grid, left half generated from the hash bits and
  mirrored to the right (so it reads as a symmetric "sigil"); each cell
  on/off from a hash bit; a dominant color derived from hash bytes so
  each CA also has a recognizable *color*. Rendered with Unicode blocks,
  two columns per cell to square the aspect ratio.
- **Color tiers**: 24-bit truecolor when the terminal supports it,
  256-color fallback, monochrome (`█`/space) when `NO_COLOR` is set or
  output isn't a TTY. The fingerprint text is always shown, so
  verification never *depends* on color.

Shown at three moments, all rendering the same artifact for the same CA:
1. `ca init` — "share this with anyone joining."
2. `ca fingerprint` — reprint on demand.
3. The join prompt — "does this match?"

Lives in a new pure-Rust, openssl-free module `netidx-admin::fingerprint`
(`fn fingerprint(cert_der) -> Fingerprint` with `text()` and
`identicon(ColorMode)` renderers), so both the daemon side and the
cross-platform client side use the exact same code.

---

## 6. Wire protocol

Bespoke, over a `tokio-rustls` TLS stream, messages `Pack`-encoded with a
4-byte length prefix (reuse `netidx-core` `Pack`; do **not** reuse
netidx `channel.rs`, which is bound to netidx auth). `rustls` on both
ends keeps the client cross-platform.

```rust
struct ClientHello { protocol_version: u32, kind: NodeKind }     // NodeKind: Resolver|Publisher|Client|Workstation
struct ServerHello { protocol_version: u32 }                     // CA cert comes from the TLS chain, not echoed here

struct SignRequest {
    admin: String,
    password: Secret,                 // zeroize; never Debug-printed
    csr_pem: Bytes,
    requested_san: Vec<SanEntry>,
    requested_validity_days: u32,
}

enum SignResponse {
    Ok  { signed_cert_pem: Bytes, ca_cert_pem: Bytes },
    Err { reason: ArcStr },           // wrong password, policy violation, malformed CSR, …
}
```

**TLS bootstrap (TOFU).** The client connects with a custom
`rustls::client::danger::ServerCertVerifier` that *captures* the
presented chain and returns `Ok` on first contact. The root of that
chain is the self-signed CA cert; the client fingerprints it (§5),
prompts the operator, and only on confirmation (a) pins the CA cert into
the canonical trusted location and (b) sends `SignRequest`. On later
runs against an already-pinned CA, the normal rustls verifier is used
and there's no prompt.

**Daemon serving identity.** The daemon serves with a leaf cert **signed
by the CA** (SAN = the CA box's hostname/IP), issued at setup time. A
stolen serving key lets someone impersonate the daemon *only on a box
that also has `key.enc` + can capture admin passwords* — i.e. only if
the CA box itself is compromised, in which case all bets are off anyway.
So the serving key is stored unencrypted, mode 0600, alongside the
daemon config; the CA key remains the only vault-protected secret.

---

## 7. Join flow, end to end

On the joining node (`netidx admin init {resolver,publisher,workstation}
--auth tls`, "ca-server" option — and the client-identity cascade
`prompt_tls_client_identity`):

1. Generate keypair + CSR locally with **`rcgen`** — pure Rust, so the
   join client is one cross-platform code path (the openssl
   `generate_csr` stays only for the unix local-CA flows).
2. Prompt **CA server address**, defaulting to *the upstream resolver's
   IP* + the CA port (§9). No discovery.
3. TLS-connect (TOFU verifier), receive chain, **show fingerprint +
   identicon, confirm** (§5). Pin the CA cert.
4. Prompt **admin password**; send `SignRequest` over the verified
   channel.
5. Receive `SignResponse::Ok { signed_cert_pem, ca_cert_pem }` (or a
   clear `Err` → re-prompt password / fix name / abort).
6. Install cert + local key + CA bundle into the canonical
   `~/.config/netidx/tls/<name>/` via the existing install path — which,
   after the recent staging-tmpdir work, validates and writes
   atomically with the correct `check_no_overwrite` semantics. **No
   files moved by hand.**

This replaces `generate_csr_and_wait_for_cert`'s blocking wait entirely;
that function stays only as the fully-offline fallback (`manual-csr`).

---

## 8. CLI surface

CA box:
```
netidx admin ca init            # create CA → identicon → optionally install+start the daemon
netidx admin ca serve   [-c <ca-server.json>] [-f]     # the daemon (activation unit calls this)
netidx admin ca fingerprint     # reprint fingerprint + identicon
netidx admin ca admin add <name> [--allow-san <glob>...] [--max-validity-days N]
netidx admin ca admin remove <name>
netidx admin ca admin list
netidx admin ca migrate         # v1 single-password key → vault slot 0
```

Joining node: no new top-level command — the `ca-server` option in the
existing `admin init … --auth tls` cascade, plus flags for scripting:
`--ca-server <addr>`, `--ca-admin <name>`, `--ca-pin <sha256>` (skip the
interactive identicon confirm for non-TTY installs).

---

## 9. Deployment, discovery, and the daemon config

**Activation unit.** `ca init` ends with an optional prompt: *install
the CA server daemon? [Y/n]*. Yes ⇒ write a `ca.unit` (system scope,
boot-triggered, run as the netidx service user — same machinery as
`resolver.unit` / `id-map.unit`) whose `ExecStart` is
`netidx admin ca serve -c <ca-server.json> -f`, and offer to start it via
the OS-service installer. Declining leaves a fully-formed offline CA.

**Daemon config** `~/.config/netidx/ca-server.json` (or the system
path), transport-only — issuance policy lives per-slot:
```jsonc
{ "ca_dir": "~/.config/netidx/ca",
  "listen": "0.0.0.0:4565",
  "serving_cert": ".../ca-server/cert.pem",
  "serving_key":  ".../ca-server/key.pem",
  "audit_log":    "~/.config/netidx/ca/audit.log" }
```

**Discovery: none.** The init flow asks for the resolver **IP and port
separately** — the IP is the one value the operator must know, the port
defaults to **4564** with a keystroke. The CA-server prompt then
**suggests that same resolver IP** as its default, with the **CA port
4565** suggested separately, so a complete TLS bring-up only needs the
operator to type a single IP address. In the common small-org case the
CA daemon runs on the resolver box, so the suggested IP is usually
correct; the multicast machinery from the old sketch is dropped
entirely. (The resolver-address IP/port split has already landed across
all the init flows that take one — `admin {resolver,publisher} install`
and the parent-referral cascade the workstation uses; the CA-server
prompt arrives with this daemon.)

---

## 10. Crate layout & new dependencies

The current implementation separates shared wire data, cross-platform client
code, and Unix authority code:

```
netidx-admin-proto/   Pack wire model, policy, identity, fingerprint, config DTOs
netidx-admin-client/  rustls client transport, discovery, remote ops, config tooling
netidx-admin-server/  daemon, OpenSSL CA, vault, stores, authority provisioning
```

The dependency direction is protocol ← client ← server. Windows builds only
the first two crates and therefore do not depend on OpenSSL.

CLI glue in `netidx-tools/src/admin/ca.rs` (subcommands) and
`init.rs` (the `ca-server` branch of the TLS cascade).

New workspace deps: `argon2` (Argon2id KDF), `sha2` (fingerprint,
cross-platform), `zeroize` (wipe MK/KEK/passwords), `aes-gcm` (vault
AEAD — pure Rust, so the on-disk format isn't openssl-coupled), and
`rcgen` (the join client's cross-platform key + CSR generation).
Already present and reused: `rustls`, `tokio-rustls`, `rustls-pemfile`,
`rand`.

---

## 11. Security model

- **CA key at rest:** encrypted under a random MK; MK only recoverable
  via an admin password (Argon2id-derived KEK, GCM-authenticated). The
  CA box at rest holds nothing sufficient to sign.
- **CA key in use:** decrypted into zeroized memory for one signing op
  per request, then wiped.
- **Impostor CA daemon:** defeated by the operator comparing the
  identicon/fingerprint before any password is sent. `--ca-pin` automates
  it for scripted joins.
- **Password on the wire:** plaintext, but only inside the TLS channel
  *after* the CA cert is human-verified — so only the genuine CA (the
  one holding the CA key) can ever receive it. A legit-but-compromised CA
  box can already harvest everything, so protecting the password from it
  is moot; that's the boundary of the threat model.
- **Authorization:** per-slot `allowed_san` + validity cap; full audit
  log. Revoke = delete slot.
- **Argon2 DoS amplification:** every `SignRequest` triggers an
  Argon2id derivation per slot tried (64 MiB + CPU each), so a flood of
  wrong-password requests is an amplification vector. Mitigate with a
  small per-connection / per-source-IP concurrency + rate limit on the
  accept loop, and try slots in a fixed order with an early constant-time
  reject for malformed requests *before* the KDF. Cheap to add; worth
  doing from day one.
- **Out of scope (unchanged from v1):** CRL/OCSP revocation of *issued
  leaf* certs, HSM/PKCS#11 backing, online cert transparency.

---

## 12. Deferred / not in this piece

- Perms publishing + admin RPCs + the Graphix perms editor (the rest of
  Layer 4 — stays in `netidx-admin-future.md`).
- Leaf-cert revocation / CRL (CA-wide; orthogonal).
- Admin-server clustering / HA for the CA daemon (one CA box for now;
  re-issue is idempotent, so a cold standby that shares the CA dir is the
  poor-man's HA).
- Rotating the CA itself (new CA cert) and cross-signing for rollover.

---

## 13. Decisions locked

- **CA default port: 4565** (resolver is 4564).
- **Join-client CSR: `rcgen`**, used on all platforms for one
  cross-platform code path; openssl `generate_csr` stays for the unix
  local-CA flows.
- **Vault AEAD: `aes-gcm`** (AES-256-GCM, pure Rust).
- **Identicon: 8×8 symmetric** colored grid.
- **Argon2id cost (default, tunable): m = 64 MiB, t = 3, p = 4.** Not
  load-bearing for the format — each slot stores its own KDF params, so
  cost can be raised per-slot later without touching the others.

The init UX change that this design relies on — asking for a
resolver-server address as an **IP plus a separately-prompted port**
(default 4564) so the operator types one IP and the CA-server prompt can
reuse it — has already shipped in `netidx-tools/src/admin/init.rs` at
every site that takes such an address: `run_resolver` (advertised
address), `run_publisher` (resolver address), and `prompt_parent_referral`
(the upstream-resolver address the workstation and a resolver-with-parent
ask for), all sharing the `prompt_resolver_port` helper.

## 14. Implementation notes (decided while building the engine)

- **Join-issued leaf certs are ECDSA P-256, not RSA.** rcgen (the
  cross-platform, openssl-free keygen the join client needs for Windows)
  can generate ECDSA/Ed25519 but **not** RSA — RSA keys can only be
  *supplied*, not generated, by ring/aws-lc-rs. netidx's TLS runtime is
  algorithm-agnostic (it loads PKCS#8 → rustls, confirmed in
  `netidx/src/tls.rs::load_private_key`), so ECDSA leaves work
  end-to-end. The CA's old flat `bits >= 2048` strength check (RSA-
  centric) would have wrongly rejected a 256-bit EC key, so
  `sign_request` now uses an algorithm-aware `check_pubkey_strength`
  (RSA ≥ 2048, EC P-256+, Edwards accepted). Net effect: a deployment
  ends up with RSA-4096 for locally-issued identities and ECDSA P-256
  for daemon-issued ones — mixed, which rustls handles fine. If
  homogeneity is wanted instead, the alternative is to pull in the pure-
  Rust `rsa` crate for client keygen (slower) — not done.
- **Serving-cert trust binding.** The operator confirms the **CA cert**
  fingerprint (as intended). To bind the TLS channel to that CA without
  webpki's serverAuth-EKU requirement (the CA's leaf certs carry no
  EKU), the client verifies — via `x509-parser` (ring), cross-platform —
  that the presented serving leaf is *signed by the confirmed CA* and
  carries the reserved SAN `netidx-ca-server`. The daemon's serving cert
  is just a normal CA-issued leaf with that reserved SAN; issuance
  policy must never grant that name to a normal join. The control
  protocol uses the same length-prefixed Pack framing as the remote admin
  protocol. `tokio` was promoted from an optional to a
  base dependency of `netidx-admin` for the daemon/client.

---

## Externally-signed CA (intermediate mode)

An advanced, non-default option runs the netidx CA as an **intermediate**
whose certificate is signed by an external PKI, instead of self-signing
it. The CA *key* is still netidx-generated and vault-sealed exactly as
usual — only the CA cert's issuer changes. This keeps the entire admin
plane (discovery, enrollment, renewal, RBAC, remote management) while
letting the netidx CA chain up to an organization's existing root, so
third parties that trust that root also trust netidx-issued leaves.

Because netidx does not hold the external issuer's key, it cannot re-sign
its own CA cert: **CA-cert auto-renewal is disabled** in this mode
(`CaLifetimes.externally_signed`, gated in `admin_server`'s approve path
and defended in `maybe_renew_ca_cert`). The operator re-signs out of band
when it approaches expiry; the admin server warns during the renewal
window. Leaf issuance and leaf/serving-cert renewal are unaffected — the
CA still holds its key and signs normally.

### Two-phase ceremony

The TUI exposes the complete ceremony. On a fresh dedicated host choose
**Controller / CA**, answer yes to external-root signing, and save the recovery
password. The result screen identifies the subordinate-CA CSR and makes clear
that the controller is not running yet. After the external PKI returns a signed
CA certificate, reopen the TUI and choose **Install Signed Certificate (External
CA)**. Supply the signed certificate and, unless it is included in the returned
chain, the external root certificate. Only then is the controller configured
and its OS service registered.

The equivalent strict CLI flow is:

```
$ netidx admin ca init --external-sign        # phase 1: bootstrap
  ...generates the CA key, seals the vault (recovery slot; for a served
  CA also the box autorenew slot + the superuser role slot), writes
  ca.<domain>.csr, and STOPS. No certificate.pem is written — its
  absence is the "awaiting external cert" state.

# get ca.<domain>.csr signed by your PKI as a subordinate CA, then:

$ netidx admin ca external install <signed-cert.pem> [--root <root.pem>]
  ...validates the signed cert (its key matches the vaulted CA key, it is
  a CA cert, and it chains to the external root), installs it, and — on
  the first install — finishes the served-CA setup (serving cert, config)
  that phase 1 could not do without the cert. Phase 2 unlocks the key
  passwordlessly via the box autorenew keytab.
```

For renewal, **Emit Renewal CSR (External CA)** in the TUI (or `netidx admin ca
external emit-csr`) emits a CSR over the existing CA key. After the external PKI
signs it, **Install Renewed Certificate (External CA)** (or the same `external
install` CLI command) swaps the certificate through the protected local control
socket while the controller remains online. The CA glyph is the intermediate
key fingerprint, so it does not change. `ca external renew` remains only as a
compatibility alias for the explicit emit/install commands.

### On-disk layout and trust distribution

- `certificate.pem` is the **intermediate alone** — never a chain. The
  trust domain glyph is `split_chain(chain).last()`'s SPKI, which stays the
  netidx CA's key; a chain here would flip the glyph to the external
  root's key and break `verify_serving_cert`.
- `trusted.pem` is `[external root, intermediate]`, so netidx nodes hold
  the external root as an anchor and can validate a future re-signed CA
  cert.
- `reconcile_trusted_bundle` accepts a same-SPKI CA refresh when it is
  validly self-signed **or** validly signed by an anchor already held
  (the external root) — so a re-signed intermediate propagates. It still
  refuses to introduce a new anchor (a new SPKI), so a compromised
  renewal peer cannot move trust; the pinned key never changes.

### Known follow-up

For a third party to chain a netidx **data-plane** serving cert
(resolver/publisher) up to the external root, that leaf must be
*presented* as `[leaf, intermediate]`. The admin serving chain already is
(`server.rs`); extending data-plane serving certs to present the
intermediate when the CA is externally signed is a scoped, third-party-
only follow-up (netidx-internal validation pins the CA by key and does
not need it).
