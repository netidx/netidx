//! `netidx admin …` CLI surface. A thin presentation layer over
//! `netidx-admin`.
//!
//! Two bases: **system roles** (`workstation` / `resolver` /
//! `publisher`, each with `install` and lifecycle actions) plus `ca`,
//! are top-level; the low-level single-component commands live under
//! `component`. `uninstall` tears an install down.

use anyhow::Result;
use clap::Subcommand;

// `activation` (edit + control the supervisor's units) drives the activation
// supervisor: its local control socket on-box, or an admin server's over the
// admin plane. Both are available on unix and Windows.
#[cfg(any(unix, windows))]
mod activation;
/// The strict-CLI `Answerer`: turns each subcommand into a non-interactive
/// command that takes its values from flags or errors naming the flag.
mod answer_cli;
mod backup_restore;
mod ca;
mod component;
// `discover` (browse mDNS for admin domains + print their glyphs) is a read-only
// query over the cross-platform discovery + admin-client layers.
mod agent;
mod delegation;
mod discover;
mod editor;
mod id_map;
mod id_map_admin;
mod init;
mod lifecycle;
mod perms;
mod propagation;
mod read_gate;
mod roles;
// `server` runs the admin-server daemon, which holds the CA vault and the
// local control socket. There is no daemon off unix; a Windows host joins an
// admin domain as a client and administers a remote one over TLS.
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
    /// back up the currently installed netidx role
    Backup(backup_restore::BackupArgs),
    /// restore a netidx role from a backup bundle
    Restore(backup_restore::RestoreArgs),
    /// authenticate once and persist a platform-sealed administrator session
    Login(answer_cli::RemoteAuthFlags),
    /// revoke and remove a cached administrator session
    Logout(session::LogoutArgs),
    /// workstation role: a local resolver + matching client
    Workstation {
        #[command(subcommand)]
        cmd: roles::workstation::Cmd,
    },
    /// resolver role: an network-facing resolver server
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
    Ca {
        #[command(subcommand)]
        cmd: ca::Cmd,
    },
    /// show or edit a resolver cluster's permissions through the admin server,
    /// routed by the admin domain map (no SSH). Whole-document (`show` /
    /// `edit`) or one entry at a time (`set` / `remove`); every form
    /// propagates to the whole resolver cluster.
    Perms {
        #[command(subcommand)]
        cmd: perms::Cmd,
    },
    /// show or edit the admin domain's id-map (the TLS name → uid/group table
    /// the resolver keys permissions on) through the admin server. Every edit
    /// propagates to each id-map host.
    IdMap {
        #[command(subcommand)]
        cmd: id_map_admin::Cmd,
    },
    /// discover netidx admin domains on the local network (mDNS) and print each
    /// one's admin-server address + CA glyph — a read-only query a script can
    /// feed to `--admin-server` / `--accept-glyph`.
    Discover(discover::DiscoverArgs),
    /// tear down a netidx install (config dir + OS service)
    Uninstall(uninstall::Params),
    /// low-level single-component commands (units / server / tls / id-map /
    /// service)
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
        Params::Backup(args) => backup_restore::backup(args),
        Params::Restore(args) => backup_restore::restore(args),
        Params::Login(flags) => session::login(flags),
        Params::Logout(args) => session::logout(args),
        Params::Workstation { cmd } => roles::workstation::run(cmd),
        Params::Resolver { cmd } => roles::resolver::run(cmd),
        Params::Publisher { cmd } => roles::publisher::run(cmd),
        Params::Ca { cmd } => ca::run(cmd),
        Params::Perms { cmd } => perms::run(cmd),
        Params::IdMap { cmd } => id_map_admin::run(cmd),
        Params::Discover(a) => discover::run(a),
        Params::Uninstall(p) => uninstall::run(p),
        Params::Component { cmd } => component::run(cmd),
    }
}
