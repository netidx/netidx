//! `netidx admin resolver …` — the resolver role: a network-facing
//! resolver-server (with optional admin server / CA).

use anyhow::Result;
use clap::Subcommand;

#[cfg(unix)]
use crate::admin::delegation;
use crate::admin::{init, lifecycle};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install a network-facing resolver server
    Install(init::ResolverFlags),
    /// report what this resolver is and whether its config is in sync with
    /// the network map
    Status,
    /// reconcile this resolver's client config + parent referral to the
    /// network map (never its member_servers)
    Update(lifecycle::UpdateFlags),
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
        Cmd::Status => lifecycle::resolver_status(),
        Cmd::Update(f) => lifecycle::resolver_update(f),
        #[cfg(unix)]
        Cmd::AddParent(f) => delegation::add_parent(f),
        #[cfg(unix)]
        Cmd::ReviewDelegation(f) => delegation::review_delegation(f),
    }
}
