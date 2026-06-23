//! `netidx conf publisher …` — the publisher role: a client config for
//! a publisher host pointing at a remote resolver cluster.

use anyhow::Result;
use clap::Subcommand;

use crate::conf::{init, lifecycle};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install a publisher-host config pointing at a remote cluster
    Install(init::PublisherFlags),
    /// report what this publisher is and whether its client config is in
    /// sync with the network map
    Status,
    /// reconcile this publisher's client config to its resolver cluster
    Update(lifecycle::UpdateFlags),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Install(f) => init::run_publisher(f),
        Cmd::Status => lifecycle::publisher_status(),
        Cmd::Update(f) => lifecycle::publisher_update(f),
    }
}
