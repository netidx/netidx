//! `netidx admin publisher …` — the publisher role: a client config for
//! a publisher host pointing at a remote resolver admin domain.

use anyhow::Result;
use clap::Subcommand;

use crate::admin::{init, lifecycle};
use netidx_admin::provenance::InstallRole;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install a publisher-host config pointing at a remote admin domain
    Install(init::PublisherFlags),
    /// report what this publisher is and whether its client config is in
    /// sync with the admin domain map
    Status,
    /// reconcile this publisher's client config to its resolver admin domain
    Update(lifecycle::UpdateFlags),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Install(f) => init::run_publisher(f),
        Cmd::Status => lifecycle::status(InstallRole::Publisher),
        Cmd::Update(f) => lifecycle::update(InstallRole::Publisher, f),
    }
}
