//! Administrative clients and configuration tooling for netidx.
//!
//! This crate is the library half of the `netidx admin …` CLI. It owns
//! the load/edit/save story for the various netidx config files (client,
//! resolver-server, perms, activation, id-map), remote administration,
//! enrollment, and the OS service installer. The CLI in `netidx-tools` is a thin shell over
//! the API exposed here.
//!
//! Synchronous wherever possible. Cloud metadata probes for [`netshape`] are
//! optional behind the `cloud-detect` feature.

#[macro_use]
extern crate anyhow;

pub mod activation;
/// On-disk config (`admin-server.json`) for the admin-server daemon:
/// domain, listen address, serving identity, roles, and peers. Its data model
/// is shared through `netidx-admin-proto`; this module owns lock-aware I/O.
pub mod admin_server_config;
/// The `Answerer` seam: how the admin engine asks the operator questions and
/// reports progress, abstracted over the frontend (strict CLI / TUI / Atlas).
pub mod answer;
pub mod atomic;
pub mod client;
/// Internal cloud-metadata / container detection backing [`netshape`].
#[cfg(feature = "cloud-detect")]
mod cloud;
pub mod config_lock;
/// mDNS/DNS-SD advertisement + browsing for admin servers. The beacon is
/// a *hint* (candidate addresses, display grouping) — nothing
/// security-relevant is decided from it. Cross-platform: a Windows
/// workstation browses; the unix daemon advertises.
pub mod discovery;
pub mod id_map;
/// Local control-socket client: the on-box `ca` CLI drives the running admin
/// daemon over its `0600` unix socket (no TLS / no password — the daemon
/// trusts the local peer by `SO_PEERCRED`). Unix-only — the socket is a
/// daemon feature and the daemon is unix.
#[cfg(unix)]
pub mod local;
/// Canonical operating-system identity names used by netidx Local auth.
pub mod local_identity;
/// Deployment-environment network-shape detection (`--listen` /
/// `--bind` suggestions) for the `admin install` flow. Behind the
/// `cloud-detect` feature because it pulls an HTTP client + interface
/// enumeration that config-only consumers don't need.
#[cfg(feature = "cloud-detect")]
pub mod netshape;
/// Remote-admin operations behind the [`answer::Answerer`] seam: the
/// query/action (code-as-id) orchestrators — enrollment queue, delegation,
/// revocation, admin roster, service control, and permissions — that the
/// strict CLI, the TUI, and Atlas all drive.
pub mod ops;
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
pub mod session_cache;
pub mod template;
pub mod tls;
/// Shared trust-on-first-use rustls verifier for [`transport`] and
/// [`resolver_probe`].
mod tls_tofu;
/// Admin-server client: fetch an admin domain's identity and info, join it
/// (key + CSR + signature over TLS), and enroll new admin servers —
/// verifying the CA identity by fingerprint first. Cross-platform
/// (rcgen + rustls, no openssl).
pub mod transport;
pub mod uninstall;

pub(crate) use netidx_admin_proto as admin_proto;
pub(crate) use netidx_admin_proto::fingerprint;
