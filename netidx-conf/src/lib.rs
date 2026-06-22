//! `netidx-conf` — configuration tooling engine for netidx.
//!
//! This crate is the library half of the `netidx conf …` CLI. It owns
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
pub mod atomic;
/// CA / CSR / cert generation. Unix-only because it depends on
/// `openssl`, which on Windows requires a mingw build of OpenSSL
/// that is impractical to install. netidx itself uses rustls
/// everywhere, so on Windows you have working TLS auth via
/// rustls — you just can't issue or sign certificates locally; bring
/// them from elsewhere.
#[cfg(unix)]
pub mod ca;
/// LUKS-style keyslot vault protecting the CA private key (multiple
/// revocable admin passwords). Unix-only — it only ever guards a CA
/// key, and the CA module is unix-only. See [`design/ca-server.md`].
#[cfg(unix)]
pub mod ca_vault;
/// The CA's request store — one atomic JSON record per request, owned by
/// the daemon (`queue/`, `issued/`, `denied/` under the CA dir). Backs
/// the conf server's Enqueue / Poll / Approve / Deny, revoke-by-name, and
/// duplicate-name refusal. Unix-only — it lives in the CA dir.
#[cfg(unix)]
pub mod ca_store;
/// The conf server's resolver-hierarchy delegation request store (the
/// `add-parent` / `review-delegation` ceremony), parallel to [`ca_store`].
/// Unix-only — it lives in the CA dir.
#[cfg(unix)]
pub mod delegation_store;
/// The CA's published CRL (`<ca-dir>/crl.pem`), built from
/// [`ca_store`]'s revoked set. Unix-only — it lives in the CA dir.
#[cfg(unix)]
pub mod ca_index;
/// Wire protocol (message types + framing) shared by the conf server
/// and its clients. Cross-platform — a Windows node speaks it to a unix
/// conf server. See [`design/ca-server.md`].
pub mod conf_proto;
/// Conf-server client: fetch a network's identity and info, join it
/// (key + CSR + signature over TLS), and enroll new conf servers —
/// verifying the CA identity by fingerprint first. Cross-platform
/// (rcgen + rustls, no openssl).
pub mod conf_client;
/// Conf server: answers network-info queries, validates sign/enroll
/// requests against per-admin policy, and pushes id-map registrations
/// to peers. Unix-only (openssl signer). See [`design/ca-server.md`].
#[cfg(unix)]
pub mod conf_server;
/// On-disk config (`conf-server.json`) for the conf-server daemon:
/// domain, listen address, serving identity, roles, peers. Unix-only —
/// only the daemon and its installer read or write it.
#[cfg(unix)]
pub mod conf_server_config;
/// mDNS/DNS-SD advertisement + browsing for conf servers. The beacon is
/// a *hint* (candidate addresses, display grouping) — nothing
/// security-relevant is decided from it. Cross-platform: a Windows
/// workstation browses; the unix daemon advertises.
pub mod discovery;
/// The certificate renewal daemon: queues verified renewals for this
/// host's TLS identities and distributes the CRL. Cross-platform —
/// Windows workstations renew too.
pub mod renewd;
pub mod client;
/// Internal cloud-metadata / container detection backing [`netshape`].
#[cfg(feature = "cloud-detect")]
mod cloud;
/// Human-comparable CA-cert fingerprint (base32 text + colored
/// identicon) for out-of-band CA identity verification. Cross-platform
/// and openssl-free so the join client renders the same artifact
/// everywhere — see [`design/ca-server.md`].
pub mod fingerprint;
pub mod id_map;
/// Deployment-environment network-shape detection (`--listen` /
/// `--bind` suggestions) for the `conf install` flow. Behind the
/// `cloud-detect` feature because it pulls an HTTP client + interface
/// enumeration that config-only consumers don't need.
#[cfg(feature = "cloud-detect")]
pub mod netshape;
pub mod paths;
pub mod provenance;
pub mod reconcile;
pub mod service;
pub mod perms;
pub mod resolver;
/// Probe a running resolver's served TLS name (its cert's DNS SAN) to
/// prefill the "resolver TLS name" setup prompt. Cross-platform (rustls,
/// no openssl).
pub mod resolver_probe;
pub mod template;
pub mod tls;
/// Shared trust-on-first-use rustls verifier for [`conf_client`] and
/// [`resolver_probe`].
mod tls_tofu;
pub mod uninstall;
