//! `netidx admin resolver …` — the resolver role: an network-facing
//! resolver-server (with optional admin server / CA).

use anyhow::Result;
use clap::Subcommand;

#[cfg(unix)]
use crate::admin::delegation;
use crate::admin::{init, lifecycle};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install an network-facing resolver server
    Install(init::ResolverFlags),
    /// report what this resolver is and whether its config is in sync with
    /// the admin domain map
    Status,
    /// reconcile this resolver's client config + parent referral to the
    /// admin domain map (never its member_servers)
    Update(lifecycle::UpdateFlags),
    /// attach this standalone resolver under a parent by delegation
    /// (queues a request; the parent admin approves a subtree for it)
    #[cfg(unix)]
    AddParent(delegation::AddParentFlags),
    /// list pending delegation requests as the parent admin (each keyed by its
    /// request code, to match out of band before approving)
    #[cfg(unix)]
    ListDelegations(delegation::ListDelegationFlags),
    /// approve one pending delegation request by its code (resolver cluster-wide)
    #[cfg(unix)]
    ApproveDelegation(delegation::ApproveDelegationFlags),
    /// deny one pending delegation request by its code
    #[cfg(unix)]
    DenyDelegation(delegation::DenyDelegationFlags),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Install(f) => init::run_resolver(f),
        Cmd::Status => lifecycle::resolver_status(),
        Cmd::Update(f) => lifecycle::resolver_update(f),
        #[cfg(unix)]
        Cmd::AddParent(f) => delegation::add_parent(f),
        #[cfg(unix)]
        Cmd::ListDelegations(f) => delegation::list_delegations(f),
        #[cfg(unix)]
        Cmd::ApproveDelegation(f) => delegation::approve_delegation(f),
        #[cfg(unix)]
        Cmd::DenyDelegation(f) => delegation::deny_delegation(f),
    }
}
