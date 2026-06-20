//! `netidx conf resolver …` — the resolver role: a network-facing
//! resolver-server (with optional conf server / CA).

use anyhow::Result;
use clap::Subcommand;

use crate::conf::init;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install a network-facing resolver server
    Install(init::ResolverFlags),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Install(f) => init::run_resolver(f),
    }
}
