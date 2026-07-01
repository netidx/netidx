//! The certificate renewal daemon CLI, surfaced as `netidx conf component tls
//! auto-renew …` (see the `tls` module). The engine lives in
//! `netidx_admin::renewd`; templates install the `run` form as an
//! activation unit on every TLS host.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use netidx_admin::renewd;
use std::{net::SocketAddr, time::Duration};

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// run the renewal daemon (scan + renew + distribute the CRL,
    /// forever)
    Run(RunArgs),
    /// one renewal pass right now, then exit
    Now(NowArgs),
}

#[derive(Args, Debug)]
pub(crate) struct RunArgs {
    /// Conf server holding the CA. Defaults to this host's conf-server
    /// config, then mDNS discovery (PKI-verified — no prompts).
    #[arg(long)]
    pub server: Option<SocketAddr>,
    /// Seconds between scans. Default 6 hours.
    #[arg(long, default_value_t = renewd::DEFAULT_INTERVAL.as_secs())]
    pub interval: u64,
    /// Don't daemonize (run in the foreground).
    #[arg(short, long)]
    #[allow(dead_code)]
    pub foreground: bool,
}

#[derive(Args, Debug)]
pub(crate) struct NowArgs {
    /// Conf server holding the CA. Defaults to this host's conf-server
    /// config, then mDNS discovery.
    #[arg(long)]
    pub server: Option<SocketAddr>,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    env_logger::init();
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    match cmd {
        Cmd::Run(p) => {
            rt.block_on(renewd::run(p.server, Duration::from_secs(p.interval.max(60))))
        }
        Cmd::Now(p) => rt.block_on(renewd::run_once(p.server)),
    }
}
