//! `netidx conf …` CLI surface. A thin presentation layer over
//! `netidx-conf`.

use anyhow::Result;
use structopt::StructOpt;

mod activation;
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

#[derive(StructOpt, Debug)]
pub(crate) enum Params {
    #[structopt(name = "install", about = "install a templated netidx setup")]
    Install(init::Params),
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
        Params::Client { cmd } => client::run(cmd),
        Params::Resolver { cmd } => resolver::run(cmd),
        Params::Perms { cmd } => perms::run(cmd),
        Params::Activation { cmd } => activation::run(cmd),
        Params::Ca { cmd } => ca::run(cmd),
        Params::IdMap { cmd } => id_map::run(cmd),
        Params::Service { cmd } => service::run(cmd),
    }
}
