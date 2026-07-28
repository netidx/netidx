//! `netidx admin component admin-agent …` — the client-side half of the
//! admin plane. The engine lives in `netidx_admin_client::agent`; the
//! templates install the `run` form as an activation unit on every client
//! joined to an admin domain.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use netidx_admin_client::{agent, renewd, sync};
use std::{net::SocketAddr, time::Duration};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// run the agent (sync the admin domain, renew certificates, forever)
    Run(RunArgs),
    /// one pass of each task right now, then exit
    Now(NowArgs),
}

#[derive(Args, Debug)]
pub(crate) struct RunArgs {
    /// Admin server to use. Defaults to the ones recorded at install, then
    /// mDNS discovery (both verified — no prompts).
    #[arg(long)]
    pub server: Option<SocketAddr>,
    /// Minimum seconds between admin domain syncs. Each wait is drawn from
    /// this to twice this, so hosts don't sync in lockstep. Default 12
    /// hours, giving 12–24.
    #[arg(long, default_value_t = sync::DEFAULT_SYNC_INTERVAL.as_secs())]
    pub sync_interval: u64,
    /// Seconds between certificate renewal scans. Default 6 hours.
    #[arg(long, default_value_t = renewd::DEFAULT_INTERVAL.as_secs())]
    pub renew_interval: u64,
    /// Don't daemonize (run in the foreground).
    #[arg(short, long)]
    #[allow(dead_code)]
    pub foreground: bool,
}

#[derive(Args, Debug)]
pub(crate) struct NowArgs {
    /// Admin server to use. Defaults to the ones recorded at install, then
    /// mDNS discovery.
    #[arg(long)]
    pub server: Option<SocketAddr>,
}

fn config(
    server: Option<SocketAddr>,
    sync_secs: u64,
    renew_secs: u64,
) -> agent::AgentConfig {
    agent::AgentConfig {
        server,
        sync_interval: Duration::from_secs(sync_secs).max(sync::MIN_SYNC_INTERVAL),
        renew_interval: Duration::from_secs(renew_secs.max(60)),
    }
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    env_logger::init();
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    match cmd {
        Cmd::Run(p) => {
            let cfg = config(p.server, p.sync_interval, p.renew_interval);
            log::info!("agent: will {}", agent::describe(&cfg)?);
            rt.block_on(async { match agent::run(cfg).await {} })
        }
        Cmd::Now(p) => rt.block_on(agent::run_once(config(
            p.server,
            sync::DEFAULT_SYNC_INTERVAL.as_secs(),
            renewd::DEFAULT_INTERVAL.as_secs(),
        ))),
    }
}
