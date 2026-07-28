//! `netidx admin workstation …` — the workstation role: a local-auth
//! resolver + matching client, optionally referred up to an admin domain.

use anyhow::Result;
use clap::Subcommand;

use crate::admin::{init, lifecycle};
use netidx_admin_client::provenance::InstallRole;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install a workstation (local resolver + matching client)
    Install(init::WorkstationFlags),
    /// report what this workstation is and whether it's in sync with
    /// its admin domain
    Status,
    /// add resolver peers the admin domain has gained since install (additive
    /// reconcile of the parent referral)
    Update(lifecycle::UpdateFlags),
    /// attach this local-only workstation to an existing admin domain (add a
    /// parent referral; enroll a cert if the admin domain is TLS)
    Join(init::WorkstationJoinFlags),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Install(f) => init::run_workstation(f),
        Cmd::Status => lifecycle::status(InstallRole::Workstation),
        Cmd::Update(f) => lifecycle::update(InstallRole::Workstation, f),
        Cmd::Join(f) => init::run_workstation_join(f),
    }
}
