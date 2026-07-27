//! `netidx admin publisher …` — the publisher role: a client config for
//! a publisher host pointing at a remote resolver trust domain.

use anyhow::Result;
use clap::Subcommand;

use crate::admin::{init, lifecycle};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install a publisher-host config pointing at a remote trust domain
    Install(init::PublisherFlags),
    /// report what this publisher is and whether its client config is in
    /// sync with the trust domain map
    Status,
    /// reconcile this publisher's client config to its resolver trust domain
    Update(lifecycle::UpdateFlags),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Install(f) => init::run_publisher(f),
        Cmd::Status => lifecycle::publisher_status(),
        Cmd::Update(f) => lifecycle::publisher_update(f),
    }
}
