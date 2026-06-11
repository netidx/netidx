# Conf server — design

**Status: implemented.** The CA server (`design/ca-server.md`) has been
generalized into the **conf server**: a per-host daemon that makes
netidx setup discovery-driven. Every host running a netidx server
component (resolver, CA, id-map) runs one; it advertises its roles over
mDNS and answers setup questions over TLS. The CA is now a *role* of
the conf server rather than a separate daemon. The vault, signing
engine, and join trust model from the CA-server design are unchanged
and remain documented there.

## Why

Interactive Q&A doesn't scale to a distributed system: every setup
improvement before this (resolver TLS-name probe, `user.domain` SAN
suggestion, CA join with fingerprint confirm) was an ad-hoc special
case of "ask the network instead of the human." The conf server is the
generalization. The UX benchmark is syncthing; the trust model is
better than syncthing's because netidx has a CA: the human confirms
**one** fingerprint glyph per network, ever, instead of one per device
pair.

## Trust model

1. **The conf plane is always TLS, rooted at the CA — regardless of
   data-plane auth.** On a Kerberos (or anonymous) network the CA's
   scope shrinks to conf-server serving certs; the glyph confirm,
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
4. **Reserved serving SAN.** Every conf server serves TLS with a
   CA-issued cert whose single DNS SAN is `netidx-conf-server`
   (`conf_proto::SERVING_SAN`). The Sign path refuses that name
   unconditionally (even under a `*` policy glob); it is minted only by
   the local setup path on the CA host or by the policy-gated network
   **Enroll**.
5. **Roles are claimed inside TLS.** The `ServerHello` carries the
   host's domain + roles; it's trustworthy because the serving chain
   roots at the confirmed CA and the leaf carries the reserved SAN.
6. **Server-to-server uses real PKI, not TOFU.** Server hosts have the
   CA bundle installed, so a conf server connecting to a peer verifies
   it with webpki (`ServerName = "netidx-conf-server"`) and presents
   its own serving cert as the client certificate. The receiving
   daemon's client-cert verification is *optional* (join clients have
   no cert yet); requests that mutate host state (`AddIdentity`)
   require a verified reserved-SAN client cert.

## Protocol (`netidx-conf/src/conf_proto.rs`)

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
  anonymous / krb5 `spn` / tls `name`), and the other conf servers this
  one knows of. Servers never fan out to answer it — the **client**
  walks `peers` (deduped, cycle-safe, every hop pinned) and aggregates.
  One reachable conf server maps the whole network, so the manual
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
- **Enroll** mints a new conf server: admin password + the
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

The admin works the queue with `netidx conf ca sign` (no arguments):
list → pick → match the code → choose groups → approve, or deny. It
finds the conf server via `--server`, the host's own
`conf-server.json`, or discovery — an enrollment admin needs no shell
access to the CA host, just an admin keyslot.

## Per-admin policy (vault slots)

`Policy` gained two fields:

- `id_map_groups: Vec<String>` — the groups this admin **may assign**
  when enrolling a node; the actual choice is made per-enrollment in
  the `SignRequest` and validated against this set. Empty ⇒ this
  admin's signs never register identities. Default suggestion: `users`.
- `may_enroll_servers: bool` — whether this admin can grow the conf
  plane. More privileged than any SAN glob (a rogue conf server can
  impersonate the network), so it defaults on only for the founding
  admin and off for added admins.

## Discovery (`netidx-conf/src/discovery.rs`)

mDNS/DNS-SD via the pure-Rust `mdns-sd` crate (no avahi/Bonjour
dependency; a Windows workstation browses with the same stack).
Service type `_netidx-conf._tcp.local.`; TXT: `v=1`, `domain`, `roles`
(csv), `fp` (short fingerprint, display hint). The daemon advertises
unless `mdns: false`; installers browse for ~3s and group results by
domain. Networks that filter multicast set `mdns: false` and rely on
`peers` / the manual-address prompt.

## Config (`conf-server.json`)

Written by the install flows, read by `netidx conf server run`. Roles
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

Canonical locations: `${config}/netidx/conf-server.json`, then
`/etc/netidx/conf-server.json`.

## Install flows

The probe outcome is a three-state value threaded through every
sub-flow that could offer a network join: `Have(network)` (discovered
and glyph-confirmed — use it, ask nothing), `DontHave` (probed and/or
declined — never re-offer), `NotProbed` (CLI-flag path, non-TTY — a
sub-flow that wants a conf server probes itself; this is also how
`netidx conf ca join` without `--server` finds the network). The
operator answers the conf-server question at most once per install.

- **Workstation / publisher**: browse → pick the domain (asked only if
  more than one is found; manual address fallback when none) →
  fetch + glyph-confirm the network identity → aggregate GetInfo across
  its conf servers → the parent referral gets **every** resolver with
  its per-address auth. Then:
  - TLS network: one join (suggested SAN `user.<domain>`, id-map
    groups for the new identity — default `users` — then admin +
    password); registration arrives via the CA push.
  - Krb5 network: configs are written with per-address `Krb5 { spn }`;
    no CSR, no client cert, no id-map push (krb5 sites use the system
    IdM). The glyph confirm is the only human input.
- **First resolver**: per-box single-member resolver config (the
  members list remains a hand-managed central-config convenience; the
  conf-server flows never produce multi-member configs, and peer
  resolvers stay mutually unaware). The CA is created for TLS networks
  as before — and offered for krb5/anonymous networks too, scoped to
  the conf plane. `setup_server` writes a ca-role `conf-server.json`;
  after the template applies, the install adds the resolver / id-map
  roles to it.
- **Second resolver**: discovers the network, imports its settings
  (auth scheme, domain), CA-joins for its resolver identity (suggested
  `resolver.<domain>`), prompts for an SPN on krb5 networks, then
  **enrolls** a conf server here: the network CA signs its reserved-SAN
  serving cert over the wire, the host writes `conf-server.json` with
  its roles + the conf servers it found as peers, and drops the
  activation unit. Nothing is pushed to existing resolvers.

## Future capabilities (out of v1, design kept compatible)

Coordinated remote changes (perms updates, etc.) follow the pattern v1
establishes with `AddIdentity`: CLI/lib → ca-role conf server (admin
authorizes) → PKI push to the non-local conf servers that own the
affected files. The `Request` enum and the reserved-SAN client-cert
gate are the extension points; nothing assumes the request set is
closed.

Explicitly out of scope for v1: global discovery / relays (WAN setups
use the manual conf-server address), and a Windows conf-server daemon
(the CA signer is openssl/unix; browsing, joining, and GetInfo
consumption all work on Windows).
