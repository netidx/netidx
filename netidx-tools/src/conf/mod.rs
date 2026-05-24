//! `netidx conf …` CLI surface. A thin presentation layer over
//! `netidx-conf`.

use anyhow::Result;
use structopt::StructOpt;

mod activation;
// `ca` subcommand and its supporting CLI helpers depend on the
// `netidx_conf::ca` engine module, which is unix-only (it pulls
// openssl). On Windows the subcommand is simply not exposed.
#[cfg(unix)]
mod ca;
mod client;
mod cloud;
mod editor;
mod id_map;
mod init;
mod perms;
mod prompt;
mod resolver;
mod service;
mod uninstall;

#[derive(StructOpt, Debug)]
pub(crate) enum Params {
    #[structopt(name = "install", about = "install a templated netidx setup")]
    Install(init::Params),
    #[structopt(
        name = "uninstall",
        about = "tear down a netidx install (config dir + OS service)"
    )]
    Uninstall(uninstall::Params),
    #[structopt(name = "client", about = "show or edit the client config")]
    Client {
        #[structopt(subcommand)]
        cmd: client::Cmd,
    },
    #[structopt(name = "resolver", about = "show or edit the resolver-server config")]
    Resolver {
        #[structopt(subcommand)]
        cmd: resolver::Cmd,
    },
    #[structopt(name = "perms", about = "edit resolver-server perms")]
    Perms {
        #[structopt(subcommand)]
        cmd: perms::Cmd,
    },
    #[structopt(name = "activation", about = "edit netidx-activation units")]
    Activation {
        #[structopt(subcommand)]
        cmd: activation::Cmd,
    },
    /// Unix-only — the engine module (`netidx_conf::ca`) needs
    /// openssl, which we don't ship to Windows.
    #[cfg(unix)]
    #[structopt(name = "ca", about = "manage a local certificate authority")]
    Ca {
        #[structopt(subcommand)]
        cmd: ca::Cmd,
    },
    #[structopt(name = "id-map", about = "edit the netidx id-map (TLS uid/group lookups)")]
    IdMap {
        #[structopt(subcommand)]
        cmd: id_map::Cmd,
    },
    #[structopt(name = "service", about = "install netidx as an OS service")]
    Service {
        #[structopt(subcommand)]
        cmd: service::Cmd,
    },
}

pub(crate) fn run(p: Params) -> Result<()> {
    match p {
        Params::Install(p) => init::run(p),
        Params::Uninstall(p) => uninstall::run(p),
        Params::Client { cmd } => client::run(cmd),
        Params::Resolver { cmd } => resolver::run(cmd),
        Params::Perms { cmd } => perms::run(cmd),
        Params::Activation { cmd } => activation::run(cmd),
        #[cfg(unix)]
        Params::Ca { cmd } => ca::run(cmd),
        Params::IdMap { cmd } => id_map::run(cmd),
        Params::Service { cmd } => service::run(cmd),
    }
}
