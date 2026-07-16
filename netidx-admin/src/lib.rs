//! `netidx-admin` — configuration tooling engine for netidx.
//!
//! This crate is the library half of the `netidx admin …` CLI. It owns
//! the load/edit/save story for the various netidx config files (client,
//! resolver-server, perms, activation, id-map), the CA module, and the
//! OS service installer. The CLI in `netidx-tools` is a thin shell over
//! the API exposed here.
//!
//! Synchronous wherever possible. The only network surfaces are both
//! optional and feature-gated: the `cloud-detect` feature (cloud
//! metadata probes for [`netshape`]) and the `confsrv` Layer 4 module
//! (gated behind a feature flag, TBD).

#[macro_use]
extern crate anyhow;

pub mod activation;
/// Admin-server client: fetch a network's identity and info, join it
/// (key + CSR + signature over TLS), and enroll new admin servers —
/// verifying the CA identity by fingerprint first. Cross-platform
/// (rcgen + rustls, no openssl).
pub mod admin_client;
/// Local control-socket client: the on-box `ca` CLI drives the running admin
/// daemon over its `0600` unix socket (no TLS / no password — the daemon
/// trusts the local peer by `SO_PEERCRED`). Unix-only — the socket is a
/// daemon feature and the daemon is unix.
#[cfg(unix)]
pub mod admin_local;
/// Remote-admin operations behind the [`answer::Answerer`] seam: the
/// query/action (code-as-id) orchestrators — enrollment queue, delegation,
/// revocation, admin roster, service control, perms, offline sign/issue, CA
/// slots — that the strict CLI, the TUI, and Atlas all drive.
#[cfg(unix)]
pub mod admin_ops;
/// Wire protocol (message types + framing) shared by the admin server
/// and its clients. Cross-platform — a Windows node speaks it to a unix
/// admin server. See [`design/ca-server.md`].
pub mod admin_proto;
/// Admin server: answers network-info queries, validates sign/enroll
/// requests against per-admin policy, and pushes id-map registrations
/// to peers. Unix-only (openssl signer). See [`design/ca-server.md`].
#[cfg(unix)]
pub mod admin_server;
/// On-disk config (`admin-server.json`) for the admin-server daemon:
/// domain, listen address, serving identity, roles, peers. Unix-only —
/// only the daemon and its installer read or write it.
#[cfg(unix)]
pub mod admin_server_config;
/// The `Answerer` seam: how the admin engine asks the operator questions and
/// reports progress, abstracted over the frontend (strict CLI / TUI / Atlas).
pub mod answer;
pub mod atomic;
/// Point-in-time, daemon-mediated CA/controller recovery bundles.
#[cfg(unix)]
pub mod backup;
/// CA / CSR / cert generation. Unix-only because it depends on
/// `openssl`, which on Windows requires a mingw build of OpenSSL
/// that is impractical to install. netidx itself uses rustls
/// everywhere, so on Windows you have working TLS auth via
/// rustls — you just can't issue or sign certificates locally; bring
/// them from elsewhere.
#[cfg(unix)]
pub mod ca;
/// The CA's RBAC policy model (`Policy`, `SlotKind`, `AdminInfo`) — pure
/// data. Cross-platform: the unix vault stores it, but a Windows admin
/// client carries these types over the admin plane (see [`admin_proto`]).
pub mod ca_policy;
/// The CA's request store — one atomic JSON record per request, owned by
/// the daemon (`queue/`, `issued/`, `denied/` under the CA dir). Backs
/// the admin server's Enqueue / Poll / Approve / Deny, revoke-by-name, and
/// duplicate-name refusal. Unix-only — it lives in the CA dir.
#[cfg(unix)]
pub mod ca_store;
/// LUKS-style keyslot vault protecting the CA private key (multiple
/// revocable admin passwords). Unix-only — it only ever guards a CA
/// key, and the CA module is unix-only. See [`design/ca-server.md`].
#[cfg(unix)]
pub mod ca_vault;
pub mod client;
/// Internal cloud-metadata / container detection backing [`netshape`].
#[cfg(feature = "cloud-detect")]
mod cloud;
pub mod config_lock;
/// The admin server's resolver-hierarchy delegation request store (the
/// `add-parent` / `review-delegation` ceremony), parallel to [`ca_store`].
/// Unix-only — it lives in the CA dir.
#[cfg(unix)]
pub mod delegation_store;
/// mDNS/DNS-SD advertisement + browsing for admin servers. The beacon is
/// a *hint* (candidate addresses, display grouping) — nothing
/// security-relevant is decided from it. Cross-platform: a Windows
/// workstation browses; the unix daemon advertises.
pub mod discovery;
/// Human-comparable CA-cert fingerprint (base32 text + colored
/// identicon) for out-of-band CA identity verification. Cross-platform
/// and openssl-free so the join client renders the same artifact
/// everywhere — see [`design/ca-server.md`].
pub mod fingerprint;
pub mod id_map;
/// Portable, role-level backup bundles and clean-target config restoration.
pub mod install_bundle;
/// Canonical operating-system identity names used by netidx Local auth.
pub mod local_identity;
pub mod netmap;
/// Deployment-environment network-shape detection (`--listen` /
/// `--bind` suggestions) for the `admin install` flow. Behind the
/// `cloud-detect` feature because it pulls an HTTP client + interface
/// enumeration that config-only consumers don't need.
#[cfg(feature = "cloud-detect")]
pub mod netshape;
/// Offline (pre-daemon) CA issuance glue — the non-interactive half of
/// `ca sign` / `ca issue`, shared with the install flow and the daemon's own
/// sign path (serial allocation under the config-directory guard, issuance recording, SAN
/// parsing). Unix-only — it operates directly on the CA dir. The
/// Answerer-driven orchestration lives in [`admin_ops::offline`].
#[cfg(unix)]
pub mod offline_ca;
pub mod paths;
pub mod perms;
/// The install planner: the decision logic behind `admin <role> install`,
/// driven through the [`answer::Answerer`] seam so every frontend shares it.
pub mod plan;
pub mod provenance;
pub mod reconcile;
/// The certificate renewal daemon: queues verified renewals for this
/// host's TLS identities and distributes the CRL. Cross-platform —
/// Windows workstations renew too.
pub mod renewd;
pub mod resolver;
/// Probe a running resolver's served TLS name (its cert's DNS SAN) to
/// prefill the "resolver TLS name" setup prompt. Cross-platform (rustls,
/// no openssl).
pub mod resolver_probe;
pub mod service;
#[cfg(unix)]
pub mod session;
pub mod session_cache;
pub mod template;
pub mod tls;
/// Shared trust-on-first-use rustls verifier for [`admin_client`] and
/// [`resolver_probe`].
mod tls_tofu;
pub mod uninstall;
