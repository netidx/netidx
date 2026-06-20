//! `netidx conf publisher …` — the publisher role: a client config for
//! a publisher host pointing at a remote resolver cluster.

use anyhow::Result;
use clap::Subcommand;

use crate::conf::init;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install a publisher-host config pointing at a remote cluster
    Install(init::PublisherFlags),
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Install(f) => init::run_publisher(f),
    }
}
