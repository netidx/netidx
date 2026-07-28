//! The admin agent: a client's half of the admin plane.
//!
//! A resolver host runs an admin server, and the CA pushes to it. A client
//! — a publisher or a workstation — has no admin server, so nothing can
//! push to it and nothing renews its certificates. This is the small daemon
//! that closes both gaps, and it runs on every client joined to an admin
//! domain regardless of data-plane auth: an anonymous or Kerberos client has
//! no certificates to renew but still has to stay in touch.
//!
//! Two independent tasks, one loop:
//!
//! - **info sync** ([`crate::sync`]) — roughly daily, ask the nearest admin
//!   server for the admin domain map and apply it: the admin-server list in
//!   the install record and this host's resolver addresses. Always runs.
//! - **certificate renewal** ([`crate::renewd`]) — only where there are TLS
//!   identities to renew. The same task the admin server runs in-process on
//!   hosts that have one.
//!
//! Neither can fail the other: a pass reports rather than returns an error,
//! and the loop is the only thing that decides when to run again.

use crate::{provenance::InstallRecord, renewd, sync};
use anyhow::{Context, Result};
use log::{info, warn};
use std::{net::SocketAddr, time::Duration};

/// How the agent was told to run.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Explicit admin server, for both tasks. `None` runs the normal
    /// recorded-then-mDNS cascade.
    pub server: Option<SocketAddr>,
    /// Minimum wait between info syncs; the actual wait is drawn from
    /// `sync_interval..=2 * sync_interval`.
    pub sync_interval: Duration,
    /// How often to scan for certificates due for renewal.
    pub renew_interval: Duration,
}

impl Default for AgentConfig {
    fn default() -> Self {
        AgentConfig {
            server: None,
            sync_interval: sync::DEFAULT_SYNC_INTERVAL,
            renew_interval: renewd::DEFAULT_INTERVAL,
        }
    }
}

/// One info-sync pass, reported rather than returned: a host that cannot
/// reach its admin domain right now must keep trying, not exit.
async fn sync_once() {
    let record = match InstallRecord::load_default() {
        Ok(Some(record)) => record,
        Ok(None) => {
            info!("agent: no install record on this host — nothing to sync");
            return;
        }
        Err(e) => {
            warn!("agent: could not read the install record: {e:#}");
            return;
        }
    };
    if record.admin_domain.is_none() {
        info!("agent: this host is local-only — nothing to sync");
        return;
    }
    let path = match crate::paths::discover_install_record() {
        Ok(path) => path,
        Err(e) => {
            warn!("agent: could not locate the install record: {e:#}");
            return;
        }
    };
    match sync::pass(record, path).await {
        Ok(true) => info!("agent: applied admin domain changes"),
        Ok(false) => (),
        Err(e) => warn!("agent: sync failed (will retry): {e:#}"),
    }
}

/// Run until killed. Both tasks start with an immediate pass — a host that
/// has just booted is the one most likely to be stale — and then keep their
/// own cadence.
///
/// Renewal is included only when this host actually has TLS identities. That
/// is decided once, at startup: a host does not grow certificates without an
/// install or a join, and both of those restart the agent.
pub async fn run(cfg: AgentConfig) -> std::convert::Infallible {
    let renewal = renewd::RenewalConfig {
        server: cfg.server,
        interval: cfg.renew_interval,
        ..Default::default()
    };
    let renews = !renewd::host_identities(None).is_empty();
    if !renews {
        info!("agent: no TLS identities on this host — info sync only");
    }
    let mut renewer = renewd::Renewer::new(renewal);
    let mut next_sync = tokio::time::Instant::now();
    let mut next_renew = tokio::time::Instant::now();
    loop {
        let now = tokio::time::Instant::now();
        if now >= next_sync {
            sync_once().await;
            next_sync =
                tokio::time::Instant::now() + sync::next_interval(cfg.sync_interval);
        }
        if renews && tokio::time::Instant::now() >= next_renew {
            let report = renewer.pass().await;
            if let Some(summary) = report.summary() {
                info!("agent: renewal — {summary}");
            }
            next_renew = tokio::time::Instant::now() + renewer.wait_after(&report);
        }
        let wake = if renews { next_sync.min(next_renew) } else { next_sync };
        tokio::time::sleep_until(wake).await;
    }
}

/// One pass of each task, for `admin-agent now`.
pub async fn run_once(cfg: AgentConfig) -> Result<()> {
    sync_once().await;
    let renewal = renewd::RenewalConfig {
        server: cfg.server,
        interval: cfg.renew_interval,
        ..Default::default()
    };
    let report = renewd::run_once(renewal).await;
    for (cert, why) in &report.failed {
        warn!("agent: {} — {why}", cert.display());
    }
    if !report.failed.is_empty() {
        anyhow::bail!("{} identit(ies) could not be renewed", report.failed.len());
    }
    Ok(())
}

/// Everything the agent would do, described without doing any of it — for
/// an installer that wants to say what it just set up.
pub fn describe(cfg: &AgentConfig) -> Result<String> {
    let renews = !renewd::host_identities(None).is_empty();
    let record = InstallRecord::load_default()
        .context("reading the install record")?
        .filter(|r| r.admin_domain.is_some());
    // humantime, not hours: a lab runs this at `--sync-interval 20` and
    // integer hours renders that as "every 0-0 hours".
    let every = |d: Duration| {
        format!("{}–{}", humantime::format_duration(d), humantime::format_duration(d * 2))
    };
    Ok(match (record.is_some(), renews) {
        (false, _) => "nothing to do (this host has not joined an admin domain)".into(),
        (true, false) => {
            format!("sync the admin domain every {}", every(cfg.sync_interval))
        }
        (true, true) => format!(
            "sync the admin domain every {}, renew certificates every {}",
            every(cfg.sync_interval),
            humantime::format_duration(cfg.renew_interval),
        ),
    })
}
