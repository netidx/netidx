//! `netidx-conf` — configuration tooling engine for netidx.
//!
//! This crate is the library half of the `netidx conf …` CLI. It owns
//! the load/edit/save story for the various netidx config files (client,
//! resolver-server, perms, activation, id-map), the CA module, and the
//! OS service installer. The CLI in `netidx-tools` is a thin shell over
//! the API exposed here.
//!
//! Synchronous wherever possible; the only network surface is the
//! optional `confsrv` Layer 4 module (gated behind a feature flag,
//! TBD).

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
pub mod client;
pub mod id_map;
pub mod paths;
pub mod service;
pub mod perms;
pub mod resolver;
pub mod template;
pub mod tls;
