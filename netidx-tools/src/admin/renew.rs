//! The certificate renewal daemon CLI, surfaced as `netidx admin component tls
//! auto-renew …` (see the `tls` module). The engine lives in
//! `netidx_admin_client::renewd`; templates install the `run` form as an
//! activation unit on every TLS host.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use netidx_admin_client::renewd;
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
    /// Admin server holding the CA. Defaults to this host's admin-server
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
    /// Admin server holding the CA. Defaults to this host's admin-server
    /// config, then mDNS discovery.
    #[arg(long)]
    pub server: Option<SocketAddr>,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    env_logger::init();
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    match cmd {
        Cmd::Run(p) => {
            let cfg = renewd::RenewalConfig {
                server: p.server,
                interval: Duration::from_secs(p.interval.max(60)),
                ..Default::default()
            };
            rt.block_on(async { match renewd::run(cfg).await {} })
        }
        Cmd::Now(p) => {
            let cfg = renewd::RenewalConfig { server: p.server, ..Default::default() };
            let report = rt.block_on(renewd::run_once(cfg));
            match report.summary() {
                Some(summary) => println!("{summary}"),
                None => println!("nothing to renew"),
            }
            for (cert, why) in &report.failed {
                println!("  {}: {why}", cert.display());
            }
            if report.failed.is_empty() {
                Ok(())
            } else {
                bail!("{} identit(ies) could not be renewed", report.failed.len())
            }
        }
    }
}
