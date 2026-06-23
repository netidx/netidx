//! `netidx conf …` CLI surface. A thin presentation layer over
//! `netidx-conf`.
//!
//! Two levels: **system roles** (`workstation` / `resolver` /
//! `publisher`, each with `install` and lifecycle actions) plus `ca`,
//! are top-level; the low-level single-component commands live under
//! `component`. `uninstall` tears an install down.

use anyhow::Result;
use clap::Subcommand;

mod activation;
// `ca` subcommand and its supporting CLI helpers depend on the
// `netidx_conf::ca` engine module, which is unix-only (it pulls
// openssl). On Windows the subcommand is simply not exposed.
#[cfg(unix)]
mod ca;
mod client;
mod component;
// `delegation` (resolver hierarchy add-parent / review-delegation) drives
// the conf server's CA admin auth + the delegation queue, both unix-only.
#[cfg(unix)]
mod delegation;
mod editor;
mod id_map;
mod init;
mod lifecycle;
mod perms;
// `perms` admin (remote, map-routed perms show/edit) drives the conf
// server's CA admin auth + cluster push, both unix-only (the engine
// pulls openssl), same as `delegation`.
#[cfg(unix)]
mod perms_admin;
mod prompt;
mod renew;
mod resolver;
mod roles;
// `server` (the conf-server daemon CLI) depends on the
// `netidx_conf::conf_server` engine module, which is unix-only (the
// CA signer pulls openssl). Browsing/joining from Windows still works
// via `workstation install` — only the daemon is unix-gated.
#[cfg(unix)]
mod server;
mod service;
mod tls;
mod uninstall;

#[derive(Subcommand, Debug)]
pub(crate) enum Params {
    /// workstation role: a local resolver + matching client
    Workstation {
        #[command(subcommand)]
        cmd: roles::workstation::Cmd,
    },
    /// resolver role: a network-facing resolver server
    Resolver {
        #[command(subcommand)]
        cmd: roles::resolver::Cmd,
    },
    /// publisher role: a client config for a publisher host
    Publisher {
        #[command(subcommand)]
        cmd: roles::publisher::Cmd,
    },
    /// manage a local certificate authority
    // Unix-only — the engine module (`netidx_conf::ca`) needs
    // openssl, which we don't ship to Windows.
    #[cfg(unix)]
    Ca {
        #[command(subcommand)]
        cmd: ca::Cmd,
    },
    /// remotely show or edit a cluster's permissions through the conf
    /// server, routed by the network map (no SSH).
    // Unix-only — like `ca`/`delegation`, the admin path needs openssl.
    #[cfg(unix)]
    Perms {
        #[command(subcommand)]
        cmd: perms_admin::Cmd,
    },
    /// tear down a netidx install (config dir + OS service)
    Uninstall(uninstall::Params),
    /// low-level single-component commands (client / resolver config /
    /// perms / units / server / tls / id-map / service)
    Component {
        #[command(subcommand)]
        cmd: component::Cmd,
    },
}

pub(crate) fn run(p: Params) -> Result<()> {
    match p {
        Params::Workstation { cmd } => roles::workstation::run(cmd),
        Params::Resolver { cmd } => roles::resolver::run(cmd),
        Params::Publisher { cmd } => roles::publisher::run(cmd),
        #[cfg(unix)]
        Params::Ca { cmd } => ca::run(cmd),
        #[cfg(unix)]
        Params::Perms { cmd } => perms_admin::run(cmd),
        Params::Uninstall(p) => uninstall::run(p),
        Params::Component { cmd } => component::run(cmd),
    }
}
