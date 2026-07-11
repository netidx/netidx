//! `netidx admin …` CLI surface. A thin presentation layer over
//! `netidx-admin`.
//!
//! Two levels: **system roles** (`workstation` / `resolver` /
//! `publisher`, each with `install` and lifecycle actions) plus `ca`,
//! are top-level; the low-level single-component commands live under
//! `component`. `uninstall` tears an install down.

use anyhow::Result;
use clap::Subcommand;

// `activation` (edit + control the supervisor's units) drives the
// activation supervisor and its local control transport, both available
// on unix and Windows. The remote, admin-plane control path (`--server`)
// is unix-only — it needs the openssl-backed CA admin auth — and is gated
// inside `service_control`.
#[cfg(any(unix, windows))]
mod activation;
// `ca` subcommand and its supporting CLI helpers depend on the
// `netidx_admin::ca` engine module, which is unix-only (it pulls
// openssl). On Windows the subcommand is simply not exposed.
/// The strict-CLI `Answerer`: turns each subcommand into a non-interactive
/// command that takes its values from flags or errors naming the flag.
mod answer_cli;
#[cfg(unix)]
mod ca;
mod client;
mod component;
// `discover` (browse mDNS for networks + print their glyphs) is a read-only
// query over the cross-platform discovery + admin-client layers.
mod discover;
// `delegation` (resolver hierarchy add-parent / review-delegation) drives
// the admin server's CA admin auth + the delegation queue, both unix-only.
#[cfg(unix)]
mod delegation;
mod editor;
mod id_map;
mod init;
mod lifecycle;
mod perms;
// `perms` admin (remote, map-routed perms show/edit) drives the admin
// server's CA admin auth + cluster push, both unix-only (the engine
// pulls openssl), same as `delegation`.
#[cfg(unix)]
mod perms_admin;
mod renew;
mod resolver;
mod roles;
// `server` (the admin-server daemon CLI) depends on the
// `netidx_admin::admin_server` engine module, which is unix-only (the
// CA signer pulls openssl). On Windows, put a host on a network by
// installing a publisher client config (`netidx admin publisher
// install`); the `workstation` role is unix-only too (Local auth +
// activation supervisor) until full Windows support lands.
#[cfg(unix)]
mod server;
mod service;
mod session;
mod tls;
/// The interactive ratatui admin TUI, launched by bare `netidx admin`.
mod tui;
mod uninstall;

#[derive(Subcommand, Debug)]
pub(crate) enum Params {
    /// authenticate once and persist a platform-sealed administrator session
    Login(answer_cli::RemoteAuthFlags),
    /// revoke and remove a cached administrator session
    Logout(session::LogoutArgs),
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
    // Unix-only — the engine module (`netidx_admin::ca`) needs
    // openssl, which we don't ship to Windows.
    #[cfg(unix)]
    Ca {
        #[command(subcommand)]
        cmd: ca::Cmd,
    },
    /// remotely show or edit a cluster's permissions through the admin
    /// server, routed by the network map (no SSH).
    // Unix-only — like `ca`/`delegation`, the admin path needs openssl.
    #[cfg(unix)]
    Perms {
        #[command(subcommand)]
        cmd: perms_admin::Cmd,
    },
    /// discover netidx networks on the local network (mDNS) and print each
    /// one's admin-server address + CA glyph — a read-only query a script can
    /// feed to `--admin-server` / `--accept-glyph`.
    Discover(discover::DiscoverArgs),
    /// tear down a netidx install (config dir + OS service)
    Uninstall(uninstall::Params),
    /// low-level single-component commands (client / resolver config /
    /// perms / units / server / tls / id-map / service)
    Component {
        #[command(subcommand)]
        cmd: component::Cmd,
    },
}

pub(crate) fn run(p: Option<Params>) -> Result<()> {
    let p = match p {
        Some(p) => p,
        None => return tui::run(),
    };
    match p {
        Params::Login(flags) => session::login(flags),
        Params::Logout(args) => session::logout(args),
        Params::Workstation { cmd } => roles::workstation::run(cmd),
        Params::Resolver { cmd } => roles::resolver::run(cmd),
        Params::Publisher { cmd } => roles::publisher::run(cmd),
        #[cfg(unix)]
        Params::Ca { cmd } => ca::run(cmd),
        #[cfg(unix)]
        Params::Perms { cmd } => perms_admin::run(cmd),
        Params::Discover(a) => discover::run(a),
        Params::Uninstall(p) => uninstall::run(p),
        Params::Component { cmd } => component::run(cmd),
    }
}
