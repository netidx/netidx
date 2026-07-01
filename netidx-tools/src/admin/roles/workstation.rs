//! `netidx admin workstation …` — the workstation role: a local-auth
//! resolver + matching client, optionally referred up to a network.

use anyhow::Result;
use clap::Subcommand;

use crate::admin::{init, lifecycle};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install a workstation (local resolver + matching client)
    Install(init::WorkstationFlags),
    /// report what this workstation is and whether it's in sync with
    /// its network
    Status,
    /// add resolver peers the network has gained since install (additive
    /// reconcile of the parent referral)
    Update(lifecycle::UpdateFlags),
    /// attach this local-only workstation to an existing network (add a
    /// parent referral; enroll a cert if the network is TLS)
    Join(init::WorkstationJoinFlags),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Install(f) => init::run_workstation(f),
        Cmd::Status => lifecycle::workstation_status(),
        Cmd::Update(f) => lifecycle::workstation_update(f),
        Cmd::Join(f) => init::run_workstation_join(f),
    }
}
