//! `netidx conf …` CLI surface. A thin presentation layer over
//! `netidx-conf`.
//!
//! Two levels: **system roles** (`workstation` / `resolver` /
//! `publisher`, each with `install` and lifecycle actions) plus `ca`,
//! are top-level; the low-level single-component commands live under
//! `component`. `uninstall` tears an install down.

use anyhow::Result;
use clap::Subcommand;

/// The current Windows user's canonical down-level (SAM-compatible) name,
/// `DOMAIN\username`, from `GetUserNameEx(NameSamCompatible)`. This is the
/// single source of truth for every place that must agree on the user's
/// identity — the perms owner, the OS-service principal, and the logon
/// task's `UserId`. It matches what the Local-auth resolver attributes to
/// the peer (`LookupAccountSidW` yields the same string), and unlike the
/// `%USERDOMAIN%`/`%USERNAME%` env vars it never names a stale or
/// trust-broken domain (which makes `schtasks` reject the principal).
#[cfg(windows)]
pub(crate) fn windows_sam_name() -> Result<String> {
    use windows::{
        Win32::Security::Authentication::Identity::{GetUserNameExW, NameSamCompatible},
        core::PWSTR,
    };
    // DOMAIN\username fits comfortably (UNLEN 256 + DNLEN 15 + 1); one
    // generously-sized call avoids the size-query dance.
    let mut buf = vec![0u16; 1024];
    let mut len = buf.len() as u32;
    let ok =
        unsafe { GetUserNameExW(NameSamCompatible, Some(PWSTR(buf.as_mut_ptr())), &mut len) };
    if !ok {
        anyhow::bail!(
            "could not determine the current Windows user via \
             GetUserNameEx(NameSamCompatible)"
        );
    }
    Ok(String::from_utf16_lossy(&buf[..len as usize]))
}

// `activation` (edit + control the supervisor's units) drives the
// activation supervisor and its local control transport, both available
// on unix and Windows. The remote, conf-plane control path (`--server`)
// is unix-only — it needs the openssl-backed CA admin auth — and is gated
// inside `service_control`.
#[cfg(any(unix, windows))]
mod activation;
// `ca` subcommand and its supporting CLI helpers depend on the
// `netidx_admin::ca` engine module, which is unix-only (it pulls
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
// `netidx_admin::conf_server` engine module, which is unix-only (the
// CA signer pulls openssl). On Windows, put a host on a network by
// installing a publisher client config (`netidx conf publisher
// install`); the `workstation` role is unix-only too (Local auth +
// activation supervisor) until full Windows support lands.
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
    // Unix-only — the engine module (`netidx_admin::ca`) needs
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
