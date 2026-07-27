//! Unix administrative daemon and certificate-authority implementation.

#[cfg(not(unix))]
compile_error!("netidx-admin-server is supported only on unix");

#[macro_use]
extern crate anyhow;

pub mod admin_domain;
mod admin_server;
pub mod backup;
pub mod ca;
pub mod ca_store;
pub mod ca_vault;
pub mod delegation_store;
pub mod install_bundle;
pub mod offline_ca;
pub mod ops;
pub mod plan;
pub mod session;

pub use admin_server::{AUTORENEW_ADMIN, load_roots, serve};

pub(crate) use netidx_admin_client::{
    activation, admin_server_config, answer, atomic, config_lock, discovery, id_map,
    local, paths, perms, provenance, resolver, service, template, tls, transport,
};
pub(crate) use netidx_admin_proto as admin_proto;
pub(crate) use netidx_admin_proto::fingerprint;
