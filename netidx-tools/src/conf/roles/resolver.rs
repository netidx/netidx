//! `netidx conf resolver …` — the resolver role: a network-facing
//! resolver-server (with optional conf server / CA).

use anyhow::Result;
use clap::Subcommand;

use crate::conf::init;
#[cfg(unix)]
use crate::conf::delegation;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install a network-facing resolver server
    Install(init::ResolverFlags),
    /// attach this standalone resolver under a parent by delegation
    /// (queues a request; the parent admin approves a subtree for it)
    #[cfg(unix)]
    AddParent(delegation::AddParentFlags),
    /// review pending delegation requests as the parent admin: match the
    /// code, approve (cluster-wide) or deny
    #[cfg(unix)]
    ReviewDelegation(delegation::ReviewFlags),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Install(f) => init::run_resolver(f),
        #[cfg(unix)]
        Cmd::AddParent(f) => delegation::add_parent(f),
        #[cfg(unix)]
        Cmd::ReviewDelegation(f) => delegation::review_delegation(f),
    }
}
