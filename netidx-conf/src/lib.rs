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
/// Wire protocol (message types + framing) shared by the CA server and
/// the join client. Cross-platform — a Windows node speaks it to a unix
/// CA. See [`design/ca-server.md`].
pub mod ca_proto;
/// CA join client: generate a key + CSR and request a signature over
/// TLS, verifying the CA identity by fingerprint first. Cross-platform
/// (rcgen + rustls, no openssl).
pub mod ca_join;
/// CA server: validates a sign request against per-admin policy and
/// signs it. Unix-only (openssl signer). See [`design/ca-server.md`].
#[cfg(unix)]
pub mod ca_server;
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
pub mod service;
pub mod perms;
pub mod resolver;
/// Probe a running resolver's served TLS name (its cert's DNS SAN) to
/// prefill the "resolver TLS name" setup prompt. Cross-platform (rustls,
/// no openssl).
pub mod resolver_probe;
pub mod template;
pub mod tls;
/// Shared trust-on-first-use rustls verifier for [`ca_join`] and
/// [`resolver_probe`].
mod tls_tofu;
pub mod uninstall;
