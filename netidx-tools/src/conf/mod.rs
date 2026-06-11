//! `netidx conf …` CLI surface. A thin presentation layer over
//! `netidx-conf`.

use anyhow::Result;
use clap::Subcommand;

mod activation;
// `ca` subcommand and its supporting CLI helpers depend on the
// `netidx_conf::ca` engine module, which is unix-only (it pulls
// openssl). On Windows the subcommand is simply not exposed.
#[cfg(unix)]
mod ca;
mod client;
mod editor;
mod id_map;
mod init;
mod perms;
mod prompt;
mod resolver;
// `server` (the conf-server daemon CLI) depends on the
// `netidx_conf::conf_server` engine module, which is unix-only (the
// CA signer pulls openssl). Browsing/joining from Windows still works
// via `install workstation` — only the daemon is unix-gated.
#[cfg(unix)]
mod server;
mod service;
mod uninstall;

#[derive(Subcommand, Debug)]
pub(crate) enum Params {
    /// install a templated netidx setup
    Install {
        #[command(subcommand)]
        params: init::Params,
    },
    /// tear down a netidx install (config dir + OS service)
    Uninstall(uninstall::Params),
    /// show or edit the client config
    Client {
        #[command(subcommand)]
        cmd: client::Cmd,
    },
    /// show or edit the resolver-server config
    Resolver {
        #[command(subcommand)]
        cmd: resolver::Cmd,
    },
    /// edit resolver-server perms
    Perms {
        #[command(subcommand)]
        cmd: perms::Cmd,
    },
    /// edit netidx-activation units
    Activation {
        #[command(subcommand)]
        cmd: activation::Cmd,
    },
    /// manage a local certificate authority
    // Unix-only — the engine module (`netidx_conf::ca`) needs
    // openssl, which we don't ship to Windows.
    #[cfg(unix)]
    Ca {
        #[command(subcommand)]
        cmd: ca::Cmd,
    },
    /// run the conf server (network discovery + setup daemon)
    #[cfg(unix)]
    Server {
        #[command(subcommand)]
        cmd: server::Cmd,
    },
    /// edit the netidx id-map (TLS uid/group lookups)
    IdMap {
        #[command(subcommand)]
        cmd: id_map::Cmd,
    },
    /// install netidx as an OS service
    Service {
        #[command(subcommand)]
        cmd: service::Cmd,
    },
}

pub(crate) fn run(p: Params) -> Result<()> {
    match p {
        Params::Install { params } => init::run(params),
        Params::Uninstall(p) => uninstall::run(p),
        Params::Client { cmd } => client::run(cmd),
        Params::Resolver { cmd } => resolver::run(cmd),
        Params::Perms { cmd } => perms::run(cmd),
        Params::Activation { cmd } => activation::run(cmd),
        #[cfg(unix)]
        Params::Ca { cmd } => ca::run(cmd),
        #[cfg(unix)]
        Params::Server { cmd } => server::run(cmd),
        Params::IdMap { cmd } => id_map::run(cmd),
        Params::Service { cmd } => service::run(cmd),
    }
}
