//! `netidx conf component …` — the low-level, single-component commands:
//! show/edit one config file, manage activation units, run the
//! conf-server daemon, or drive this host's TLS identity.
//!
//! The role templates (`workstation` / `resolver` / `publisher`) and
//! `ca` wire these pieces together; reach for `component` when you want
//! to operate on one piece directly. This is a pure grouping layer — the
//! implementations live in the sibling modules it routes to.

use anyhow::Result;
use clap::Subcommand;

#[cfg(unix)]
use super::server;
use super::{activation, client, id_map, perms, resolver, service, tls};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
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
    /// run the conf server (network discovery + setup daemon)
    #[cfg(unix)]
    Server {
        #[command(subcommand)]
        cmd: server::Cmd,
    },
    /// this host's TLS identity (request a CSR, join a CA, auto-renew)
    Tls {
        #[command(subcommand)]
        cmd: tls::Cmd,
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

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Client { cmd } => client::run(cmd),
        Cmd::Resolver { cmd } => resolver::run(cmd),
        Cmd::Perms { cmd } => perms::run(cmd),
        Cmd::Activation { cmd } => activation::run(cmd),
        #[cfg(unix)]
        Cmd::Server { cmd } => server::run(cmd),
        Cmd::Tls { cmd } => tls::run(cmd),
        Cmd::IdMap { cmd } => id_map::run(cmd),
        Cmd::Service { cmd } => service::run(cmd),
    }
}
