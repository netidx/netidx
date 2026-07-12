# Admin server — design

**Status: implemented.** The CA server (`design/ca-server.md`) has been
generalized into the **admin server**: a per-host daemon that makes
netidx setup discovery-driven. Every host running a netidx server
component (resolver, CA, id-map) runs one; it advertises its roles over
mDNS and answers setup questions over TLS. The CA is now a *role* of
the admin server rather than a separate daemon. The vault, signing
engine, and join trust model from the CA-server design are unchanged
and remain documented there.

## Why

Interactive Q&A doesn't scale to a distributed system: every setup
improvement before this (resolver TLS-name probe, `user.domain` SAN
suggestion, CA join with fingerprint confirm) was an ad-hoc special
case of "ask the network instead of the human." The admin server is the
generalization. The UX benchmark is syncthing; the trust model is
better than syncthing's because netidx has a CA: the human confirms
**one** fingerprint glyph per network, ever, instead of one per device
pair.

## Trust model

1. **The admin plane is always TLS, rooted at the CA — regardless of
   data-plane auth.** On a Kerberos (or anonymous) network the CA's
   scope shrinks to admin-server serving certs; the glyph confirm,
   server-to-server PKI, and pushes work identically everywhere.
2. **mDNS beacons are hints, never trusted.** TXT carries the domain,
   roles, and a short fingerprint purely for candidate addresses and
   display grouping. Nothing security-relevant is decided from a
   beacon; anyone on the LAN can broadcast anything and it buys them
   nothing.
3. **One glyph per network.** The CA fingerprint confirm (text +
   identicon, `fingerprint.rs`) is the only human trust decision.
   Inspection is its own connection that sends nothing secret and
   closes before credentials are typed; everything after it pins the
   presented CA cert to the confirmed fingerprint.
4. **Reserved serving SAN.** Every admin server serves TLS with a
   CA-issued cert whose single DNS SAN is `netidx-admin-server`
   (`conf_proto::SERVING_SAN`). The Sign path refuses that name
   unconditionally (even under a `*` policy glob); it is minted only by
   the local setup path on the CA host or by the policy-gated network
   **Enroll**.
5. **Roles are claimed inside TLS.** The `ServerHello` carries the
   host's domain + roles; it's trustworthy because the serving chain
   roots at the confirmed CA and the leaf carries the reserved SAN.
6. **Server-to-server uses real PKI, not TOFU.** Server hosts have the
   CA bundle installed, so a admin server connecting to a peer verifies
   it with webpki (`ServerName = "netidx-admin-server"`) and presents
   its own serving cert as the client certificate. The receiving
   daemon's client-cert verification is *optional* (join clients have
   no cert yet); requests that mutate host state (`AddIdentity`)
   require a verified reserved-SAN client cert.

## Protocol (`netidx-admin/src/conf_proto.rs`)

Length-prefixed JSON over TLS, port 4565. A connection is: TLS accept,
`ClientHello`/`ServerHello` exchange, then exactly **one** `Request`
and its response — human think-time never holds a connection (the
server bounds each connection's lifetime at 30s).

```text
ServerHello { protocol_version, domain, roles: [ca|resolver|id-map] }

Request::GetInfo                 → GetInfoResponse
Request::Sign(SignRequest)       → SignResponse        (ca role)
Request::Enroll(EnrollRequest)   → SignResponse        (ca role, policy-gated)
Request::AddIdentity(…)          → AddIdentityResponse (id-map role, peer-cert-gated)
Request::Enqueue(…)              → EnqueueResponse     (ca role, no credentials)
Request::Poll(…)                 → PollResponse        (ca role, no credentials)
Request::ListQueue(…)            → ListQueueResponse   (ca role, admin-authenticated)
Request::Approve(…)              → ApproveResponse     (ca role, admin-authenticated)
Request::Deny(…)                 → DenyResponse        (ca role, admin-authenticated)
```

- **GetInfo** returns *local facts + known peers*: the network domain,
  where the CA is, this host's resolver (address + data-plane auth:
  anonymous / krb5 `spn` / tls `name`), and the other admin servers this
  one knows of. Servers never fan out to answer it — the **client**
  walks `peers` (deduped, cycle-safe, every hop pinned) and aggregates.
  One reachable admin server maps the whole network, so the manual
  fallback on mDNS-hostile networks is a single address.
- **Sign** is the CA-server join, unchanged at its core (vault unlock,
  per-admin policy, audit). The request carries the id-map groups the
  admin *chose* for this identity (they know who they're enrolling);
  the server validates the choice against the admin's allowed set —
  a disallowed group refuses the whole sign (silently dropping the
  registration would produce a node whose cert works but whose perms
  mysteriously don't). On success with groups, the server pushes the
  identity to every id-map host it knows (configured peers ∪ a 2s mDNS
  browse; the local map is written directly). Push failures degrade to
  `warnings` in the response — the cert is valid regardless.
- **Enroll** mints a new admin server: admin password + the
  `may_enroll_servers` policy bit authorize issuing the reserved SAN.
  The request carries the enrollee's listen address; the CA appends it
  to its own `peers` (a local write — no push), which makes the CA host
  the well-known starting point for peer walks.
- **AddIdentity** registers `{san, primary_group, groups}` on the
  receiving host's id-map. The uid is allocated *locally* (max+1,
  clamped to ≥1000) — id-map perms key on **names**, uids are a
  per-host detail, so no cross-host uid coordination exists. Missing
  groups are created with allocated gids (zero-touch joins must work on
  a fresh map); re-registration keeps the uid (idempotent re-joins).
  The id-map daemon live-reloads on the atomic write — no restart.

## Queued enrollment (the default join path)

Synchronous `Sign` assumes an admin standing at the enrolling machine —
the 1997 walk-around model. The default is asynchronous: the enrollee
**enqueues** its CSR (no credentials), the admin approves from wherever
they are, and the enrollee picks the cert up by **polling** (one short
pinned connection per poll — hours of waiting hold no socket and no
server timeout). This is also a security upgrade: the admin's CA
password is never typed on a machine that isn't theirs.

What replaces physical presence is the **mutual glyph**: the enrollee
verifies the *network* by the CA fingerprint, and the admin verifies
the *enrollee* by the request code — the fingerprint of the CSR's
public key, computed independently on both sides (never trusted from
the wire), relayed over whatever channel the two humans already trust
(chat, phone). A request the admin can't match gets denied. Even a
mis-approved request only yields a cert for the key its holder
generated — the social-engineering boundary every pairing scheme has,
and no worse.

Mechanics: the queue is files under `<ca-dir>/queue/` (CSRs are public;
ids are random hex, validated on every lookup — they're wire-supplied
strings that become file names). Entries are capped, TTL-pruned, and
survive daemon restarts. `Approve` runs the *identical* checks as a
synchronous sign (SAN globs, validity cap, id-map allowed-set — the
admin chooses the groups at approval, which is where the knowledge
lives), audited as `op=approve`; the outcome is deposited as a sidecar
the daemon serves to `Poll` idempotently. `Deny` carries a reason the
enrollee sees.

The admin works the queue with `netidx admin ca approve`:
list → pick → match the code → choose groups → approve, or deny. It
finds the admin server via `--server`, the host's own
`admin-server.json`, or discovery — an enrollment admin needs no shell
access to the CA host, just an admin keyslot.

**Admin-server enrollment queues too** (`EnqueueRequest.enroll_listen =
Some(addr)`): a second resolver's install enqueues its reserved-SAN
serving-cert request under the same code ceremony instead of demanding
an admin password at its keyboard. The entry lists as `CONF-SERVER
ENROLLMENT at <addr>` — a bigger trust decision than a user cert, and
labelled as one. Approval requires the **approving** admin's
`may_enroll_servers` (the same gate the synchronous `Enroll` applies),
signs the reserved serving SAN at the standard validity, never touches
the id-map, records the new server as a peer, and audits `op=enroll`.
A denied or expired enrollment is a note on the enrollee, not a failed
install — its resolver works, it just isn't advertised to discovery.

## Revocation

The CA keeps an append-only index of everything it has ever signed
(`<ca-dir>/issued.jsonl`: serial, name, SPKI glyph, expiry — written
inside `Ca::sign_request`, so it is complete by construction). The
index is what makes certificates manageable by *name*:

- **`netidx admin ca revoke`** lists live identities, shows the glyph
  the enrollment showed, revokes every live serial for the chosen name
  (or `--serial` for one cert), records the reason, re-signs the CRL
  with the admin's password, installs it beside the local resolver's
  trust bundle immediately, and offers to drop the id-map entry.
- **One live certificate per name**: `Sign` and `Enqueue` refuse a name
  that already has an unexpired, unrevoked cert — which closes the
  impostor race on existing identities outright, and makes the
  rebuilt-laptop flow explicit (revoke first, eyes open). Verified
  renewals are the exception (they prove possession of the live key).
- **CRL lifecycle under the vault**: signing needs a password, so the
  CRL is (re-)signed at every revocation and *opportunistically in
  every authenticated admin session* (sign/approve/deny/list all
  refresh a CRL nearing its `nextUpdate` — 90d validity, 30d refresh
  window). A network where literally nothing is signed for months gets
  staleness warnings.
- **Distribution**: `Request::GetCrl` (public — a CRL is a public
  document); the renewal daemon drops `crl.pem` beside each resolver's
  trusted bundle.
- **Enforcement** is by convention at the choke point: netidx's TLS
  acceptor loads `crl.pem` from beside the trust-bundle path, and the
  resolver uses a CRL-*watching* acceptor that rebuilds when the file
  changes — a revocation takes effect on the next accepted connection,
  no restart. A revoked cert can't authenticate to the resolver, so it
  gets no tokens and publishers never see it. Revoked-on-CRL fails
  closed; unknown status stays permitted (federated bundles may carry
  CAs whose CRLs we don't hold; an absent CRL must not lock the
  network out).

## Renewal (the moving part that removes the moving parts)

Renewal is **silent request, human-or-bot approval** — the only shape
the vault permits (no signing capability at rest) and the right one
anyway. The pieces:

- **Verified renewals**: a renewal `Enqueue` arrives on a connection
  authenticated by the node's *current* certificate; the server checks
  the SAN equals the requested name **and the serial is live in its
  own issuance index** (stronger than a CRL lookup). Such entries are
  `verified_renewal`: cryptographic continuation, no glyph. Approval
  skips the SAN globs and the one-live-cert rule, ignores groups
  (the identity already exists in the id-map), allows the reserved
  serving SAN (admin servers renew themselves), and audits `op=renew`.
  A revoked serial never verifies — a thief with stolen cert+key falls
  through to the glyph-gated queue, in front of an admin's eyes.
- **The renewal daemon** (`netidx admin component tls auto-renew run`, installed by every
  TLS install — workstation, publisher, resolver, CA host): scans this
  host's identities (client config, resolver config, admin-server
  serving cert), and inside the window — `min(30d, validity/3)` —
  queues a renewal with a **fresh key**, polls, and installs
  atomically. Request ids persist beside the cert (`renewal.id`) so
  restarts resume rather than duplicate. The daemon also pulls the CRL
  and installs it beside every trust bundle (on change only). All of
  it over real PKI against the installed bundle — no TOFU, no human.
  Applications stay completely oblivious: netidx proper knows nothing
  about renewal; only the daemon writes key material.
- **CA rollover rides the same rails**: accepting the *returned* trust
  bundle on a renewal (over the PKI-verified channel) is how a renewed
  CA certificate reaches the fleet. The CA renews **itself, same key**,
  automatically during any admin session once its remaining life drops
  below a leaf validity + grace — same key + subject means existing
  leaves still chain and the network glyph (a key hash) is unchanged.
  Leaf validity is clamped to the CA's remaining life, so nothing the
  CA signs ever outlives it.
- **`ca sign` queue UI**: verified renewals list separately with a
  one-keystroke "approve all"; new identities keep the full per-entry
  code-matching ceremony.
- **Auto-approving renewals** (`ca auto-approve`, the lazy-correct default,
  asked at CA creation, default yes): a dedicated `autorenew` keyslot
  with an **empty policy** — its password can approve continuations and
  nothing else (no SANs, no groups, no enrollment). The admin-server
  daemon, the CA's sole owner, does the approving **in-process**: when
  the CA role's `autorenew` field (in `admin-server.json`) names the
  keytab, each sweep it reads the keytab, unlocks the slot, and approves
  every pending *verified renewal* the same way a human admin would
  (audited `op=renew`) — there is no separate approval process. The `ca
  autorenew` command just creates or rotates that slot and points the
  config at its keytab, which lives in
  `${config}/netidx/autorenew.keytab` (0600, deliberately outside the CA
  dir — never back it up); `--rotate` is the one-command kill-and-replace
  (restart the admin server to pick up the new keytab). Invariant: **no
  new identity without a human; continuations are automatic.**

### The keytab at rest (TPM sealing)

The empty policy bounds what the keytab can *authorize*; it
does nothing at rest — the vault is flat, so any slot password
recovers the master key, making the keytab plus a copy of the CA dir
an offline CA-key compromise. So `setup_autorenew_slot` seals the
password to the host's TPM 2.0 when one is usable (the `netidx-tpm`
crate): the keytab file becomes a sealed blob that only this
machine's TPM will open, and a stolen disk, leaked backup, or
decommissioned drive recovers nothing. No TPM ⇒ plaintext fallback
with a printed note about what that costs.

Mechanics: pure Rust (the `netidx-tpm` crate on a pinned
`tpm2-protocol` — no C tss stack, so sealing exists in every build
and is detected at runtime). The marshalling is platform-independent;
the transport sits behind a trait with two implementations: the linux
kernel resource manager (`/dev/tpmrm0`) and Windows TPM Base Services
(`Tbsip_Submit_Command` exchanges the same raw frames; raw-dylib
linkage, so it cross-compiles from linux with no SDK). The secret is a
`KeyedHash` sealed-data object under the TCG-standard ECC P-256 SRK
template on the owner hierarchy; `CreatePrimary` deterministically
re-derives the SRK each time, so the TPM holds no persistent state.
Deliberately **no PCR binding**: a firmware update must not silently
stop renewal (an outage on a delay timer); the honest threat model is
at-rest/offline theft, not live-host compromise — root on the running
box can unseal, exactly as it could have read the plaintext. Unseal
failure (TPM cleared, board swapped) is a screaming error whose
message names the fix: `netidx admin ca auto-approve --rotate`.
Operational note: on linux the device node is root:tss, so the CA
user needs `tss` group membership — without it, setup falls back to
plaintext and says so. On Windows, TBS brokers access for any user;
no group dance.

**macOS** has no TPM; the same `seal`/`unseal` contract rides the
**Secure Enclave** with the same blob philosophy: each seal generates
a fresh transient SE P-256 key, ECIES-encrypts the secret under it
(`SecKeyCreateEncryptedData`, the X9.63-SHA256/AES-GCM variant
CryptoKit uses), and embeds the SEP-wrapped private key — the CTK
token object id, what CryptoKit calls `dataRepresentation` — in the
blob itself. Nothing touches the keychain, deliberately: SE keys can
only persist in the data-protection keychain, which demands an
application-identifier entitlement that cargo-installed (ad-hoc
signed) binaries don't have — discovered empirically as
errSecMissingEntitlement; the in-blob design sidesteps the problem
and has no system state to lose. Access policy: after-first-unlock,
this-device-only, **no user-presence gate** (a daemon stuck on a
biometric prompt is the same outage-on-a-delay-timer as PCR
brittleness). SE blobs carry a distinct magic, so a sidecar carried
across platforms fails with "sealed elsewhere — re-issue", not a
parse error. Pure Rust via `security-framework` (OS frameworks only —
no Swift, no C library); messages name the mechanism via
`netidx_tpm::MECHANISM` so a Mac operator reads "Secure Enclave", not
"TPM".

### TLS private keys at rest (seal | password | none)

The same machinery protects every TLS private key netidx issues. The
key is **never sealed directly** (TPM sealed data is ≤128 bytes; and a
proprietary key format would lock operators out): the key file stays a
standard **encrypted PKCS#8** (PBES2 scrypt + AES-256-CBC, pure Rust,
openssl-3-decryptable), and what's sealed is its random password,
written beside the key as the **`<key>.tpm` sidecar**. Convention, not
config — no schema changed anywhere.

At load, `netidx::tls::load_key_password` treats a sidecar as
authoritative: present ⇒ unseal or **hard error** (no fallthrough to
keychain/askpass — a daemon hanging on a password prompt nobody will
answer is worse than a clear failure). Absent ⇒ the existing
keychain → askpass chain. Every daemon and client inherits this
through the one loader.

At issue, every flow asks once — `choose_key_protection`, also the
`--key-protection seal|password|none` flag:

- **seal** (default whenever sealing hardware is usable — TPM on
  linux/windows, Secure Enclave on macOS — including headless):
  random password, encrypted key, sealed sidecar. The identity is
  machine-bound; a stolen disk or backup holds nothing usable.
- **password**: typed at issue, saved to the system keychain (keyed on
  the canonical key path), askpass as the client-config fallback — the
  pre-TPM behavior.
- **none**: plaintext, file modes only.

Daemon serving keys (admin server, local and enrolled) skip the
question — a daemon can't type, so they're sealed-or-plaintext
automatically with a printed note. The identity installer copies
sidecars with their keys (and clears stale ones — a leftover sidecar
would shadow the new key's password source); `renewd` preserves the
seal across renewals: fresh key ⇒ fresh password ⇒ fresh seal, and a
failed re-seal aborts the install loudly rather than degrading to
plaintext. Recovery from a cleared TPM is re-issue — one command,
which is the point of the whole renewal chapter: keys are disposable.

### Restoring a controller on replacement hardware

The CA directory is portable even though daemon credentials are not. Its
`vault.json` contains a `recovery` signing slot whose password is kept off-box;
that slot unwraps the CA master key without the old TPM. Create a live,
point-in-time-consistent bundle over the protected local control socket:

```text
netidx admin ca backup /srv/backups/netidx-2026-07-12
```

The daemon briefly blocks durable mutations while capturing the vault,
certificate/trust chain, issuance and revocation records, CRL, authoritative
map, delegation records, lifetime settings, audit log, admin-server config,
and referenced resolver/id-map configuration into memory. It resumes
administration before writing the target. A versioned, hashed manifest signed
by the CA key is published with the files through a new sibling directory and atomic rename;
an existing target is never overwritten. The local-only RPC cannot be invoked
with any network certificate. Sealed `autorenew.keytab` and TLS keys, sessions,
locks, and temporary files are deliberately not recovery assets.

After fencing the old controller, restore and recover the bundle directly:

```text
netidx admin ca recover-controller \
    --backup /srv/backups/netidx-2026-07-12 \
    --ca-dir /etc/netidx/ca \
    --config /etc/netidx/admin-server.json \
    --listen 10.0.0.20:4565 \
    --recovery-password-stdin
```

The command accepts only the off-box `recovery` slot, verifies the config's CA
fingerprint and controller UUID against `netmap.json`, and then:

- generates and seals a fresh serving key on the replacement machine;
- issues a serving certificate with the **same** controller UUID and controller
  URI (clients retain the same administrative identity);
- atomically replaces the `autorenew` vault slot and seals its new keytab on
  the replacement machine;
- revokes every superseded live serving certificate for that controller and
  republishes the CRL;
- updates the controller's authoritative map address and rewrites
  `admin-server.json` to the restored CA's canonical paths.

On first start the recovered controller sends its current immutable identity,
possibly changed address, authoritative map, and CRL to every registered admin
server. Each satellite accepts this only from the exact home-CA controller,
persists the new `ca_addr`, and resumes normal map refresh and renewal. A node
that was unavailable is retried explicitly from the CLI or the Admin Servers
TUI panel (`c`):

```text
netidx admin ca reconcile-controller --server <controller> ...
```

The response identifies every target by immutable server ID and address. The
operation is idempotent and never restarts a service. Other co-located TLS
identities whose keys were sealed to the failed machine are re-enrolled
normally after the controller is back.
`--insecure-no-tpm` is an explicit test-only fallback and leaves both new
machine credentials in plaintext.

## Per-admin policy (vault slots)

`Policy` gained two fields:

- `id_map_groups: Vec<String>` — the groups this admin **may assign**
  when enrolling a node; the actual choice is made per-enrollment in
  the `SignRequest` and validated against this set. Empty ⇒ this
  admin's signs never register identities. Default suggestion: `users`.
- `may_enroll_servers: bool` — whether this admin can grow the admin
  plane. More privileged than any SAN glob (a rogue admin server can
  impersonate the network), so it defaults on only for the founding
  admin and off for added admins.

## Discovery (`netidx-admin/src/discovery.rs`)

mDNS/DNS-SD via the pure-Rust `mdns-sd` crate (no avahi/Bonjour
dependency; a Windows workstation browses with the same stack).
Service type `_netidx-admin._tcp.local.`; TXT: `v=1`, `domain`, `roles`
(csv), `fp` (short fingerprint, display hint). The daemon advertises
unless `mdns: false`; installers browse for ~3s and group results by
domain. Networks that filter multicast set `mdns: false` and rely on
`peers` / the manual-address prompt.

## Config (`admin-server.json`)

Written by the install flows, read by `netidx admin component server run`. Roles
are explicit — the daemon never guesses from what's lying around:

```json
{
  "domain": "ryu-oh.org",
  "listen": "192.168.0.5:4565",
  "serving_cert": "…/cert.pem",        // chain [leaf, ca]
  "serving_key": "…/key.pem",
  "trusted": "…/certificate.pem",      // CA bundle: client-auth roots + peer verification
  "roles": {
    "ca": { "dir": "…/ca" },
    "resolver": { "config": "…/resolver.json" },
    "id_map": { "map": "…/id-map.json" }
  },
  "ca_addr": null,                      // where Sign/Enroll go when not the CA host
  "peers": ["192.168.0.6:4565"],
  "mdns": true
}
```

Canonical locations: `${config}/netidx/admin-server.json`, then
`/etc/netidx/admin-server.json`.

## Install flows

The probe outcome is a three-state value threaded through every
sub-flow that could offer a network join: `Have(network)` (discovered
and glyph-confirmed — use it, ask nothing), `DontHave` (probed and/or
declined — never re-offer), `NotProbed` (CLI-flag path, non-TTY — a
sub-flow that wants a admin server probes itself; this is also how
`netidx admin component tls join` without `--server` finds the network). The
operator answers the admin-server question at most once per install.

- **Workstation / publisher**: browse → pick the domain (asked only if
  more than one is found; manual address fallback when none) →
  fetch + glyph-confirm the network identity → aggregate GetInfo across
  its admin servers → the parent referral gets **every** resolver with
  its per-address auth. Then:
  - TLS network: one join (suggested SAN `user.<domain>`, id-map
    groups for the new identity — default `users` — then admin +
    password); registration arrives via the CA push.
  - Krb5 network: configs are written with per-address `Krb5 { spn }`;
    no CSR, no client cert, no id-map push (krb5 sites use the system
    IdM). The glyph confirm is the only human input.
- **First resolver**: per-box single-member resolver config (the
  members list remains a hand-managed central-config convenience; the
  admin-server flows never produce multi-member configs, and peer
  resolvers stay mutually unaware). The CA is created for TLS networks
  without asking — it signs the data plane anyway — and for krb5
  networks too, scoped to the admin plane. `setup_server` writes a
  ca-role `admin-server.json`; after the template applies, the install
  adds the resolver / id-map roles to it.
- **Second resolver**: discovers the network, imports its settings
  (auth scheme, domain), CA-joins for its resolver identity (suggested
  `resolver.<domain>`), prompts for an SPN on krb5 networks, then
  **enrolls** a admin server here: the network CA signs its reserved-SAN
  serving cert over the wire, the host writes `admin-server.json` with
  its roles + the admin servers it found as peers, and drops the
  activation unit. Nothing is pushed to existing resolvers.

### Install profiles

The installer asks for *intent* (what auth scheme, what network) and
derives the components; it never offers a choice whose "no" produces a
broken network. The matrix is `conf_plane_decision` +
`resolve_id_map_choice` in `netidx-tools/src/admin/init.rs` (both
exhaustively tested there); this table mirrors them — change all three
together.

The admin-server column applies to fresh networks and joins alike:
enrolling on an existing network queues for remote approval (the
approving admin's `may_enroll_servers` is the gate), so no admin needs
to be at the keyboard and the join side has no reason to differ.

| data plane         | CA               | admin server      | id-mapper        | renew daemon |
|--------------------|------------------|------------------|------------------|--------------|
| TLS                | always (fresh: signs the data plane; joining: exists upstream) | always | always | always |
| krb5               | always (admin plane only; joining: exists upstream) | always | asked (default no: krb5 sites have a system IdM) | with the admin server |
| anonymous          | with the admin server | asked (default yes — labs may not want the machinery) | never | with the admin server |
| local (workstation)| —                | —                | never            | only with TLS parent identities |

Expert escapes, all warned about where they're used:

- `--no-id-map` — a TLS resolver without it maps every cert SAN to
  nobody and perms deny everything; the template emits a render-time
  coherence warning (visible on `--dry-run` too).
- `--no-admin-server` — skips the admin plane entirely; the host is
  invisible to discovery, and a network with no admin server anywhere
  has no enrollment and no certificate renewal.
- External PKI / bring-your-own cert is **not** a wizard option: the
  `admin install` wizard always uses the netidx CA (that is the point of
  the control plane). To run TLS with your own certs, skip the wizard
  and manage the resolver/publisher/subscriber TLS config by hand. To
  chain the netidx CA to your existing PKI while keeping the admin plane,
  use `netidx admin ca init --external-sign` (the CA runs as an
  intermediate; its cert does not auto-renew — see ca-server.md).

## Future capabilities (out of v1, design kept compatible)

Coordinated remote changes (perms updates, etc.) follow the pattern v1
establishes with `AddIdentity`: CLI/lib → ca-role admin server (admin
authorizes) → PKI push to the non-local admin servers that own the
affected files. The `Request` enum and the reserved-SAN client-cert
gate are the extension points; nothing assumes the request set is
closed.

Explicitly out of scope for v1: global discovery / relays (WAN setups
use the manual admin-server address), and a Windows admin-server daemon
(the CA signer is openssl/unix; browsing, joining, and GetInfo
consumption all work on Windows).
