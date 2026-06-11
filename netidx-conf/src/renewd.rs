//! The certificate renewal daemon: the moving part that removes all
//! the other moving parts.
//!
//! One small daemon per TLS host (installed by the templates as an
//! activation unit) owns certificate lifecycle so that nothing else
//! has to: applications never know renewal exists, no
//! publisher/subscriber needs write access to key material, and
//! there's no mDNS chatter in every process. Each cycle it:
//!
//! 1. **Scans** this host's TLS identities — every client-config
//!    identity, the resolver's (if one runs here), and the conf
//!    server's serving cert (unix) — and for any certificate inside
//!    its renewal window, queues a **verified renewal**: a fresh key +
//!    CSR enqueued over a connection authenticated by the *current*
//!    cert. The conf server marks it proof-of-possession; an admin (or
//!    the `autorenew` daemon) approves without ceremony; we poll and
//!    install the result atomically.
//! 2. **Distributes the CRL**: pulls it from the CA and writes
//!    `crl.pem` beside each trusted bundle (only on change) — beside
//!    the resolver's bundle this is what makes revocation take effect,
//!    via the CRL-watching acceptor.
//!
//! Everything here runs unattended over real PKI: the daemon verifies
//! conf servers with webpki against the trust bundles it already has.
//! No TOFU, no glyphs — those are for humans establishing trust;
//! renewal is continuation under trust already established.

use crate::{atomic, conf_client, conf_proto::NodeKind, paths};
use anyhow::{anyhow, bail, Context, Result};
use log::{info, warn};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// How often the daemon wakes to scan (the first scan is immediate).
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(6 * 3600);

/// How long a queued renewal is polled within one cycle before leaving
/// it for the next (the admin may simply not have approved yet — the
/// request id is persisted and picked back up).
const APPROVAL_POLL: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// A TLS identity this host owns: where its files live. The renewal
/// window and SAN come from the certificate itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub trusted: PathBuf,
}

/// Scan the standard configs for every TLS identity on this host:
/// client-config identities, resolver members (TLS auth), and — on
/// unix — the conf server's serving identity. Deduped by certificate
/// path. Missing configs are skipped silently: a krb5 workstation has
/// nothing to renew and that's fine.
pub fn host_identities() -> Vec<Identity> {
    let mut out: Vec<Identity> = Vec::new();
    let mut push = |i: Identity| {
        if !out.contains(&i) {
            out.push(i);
        }
    };
    if let Ok(path) = paths::discover_client_config() {
        let cfg = std::fs::read(&path).map_err(anyhow::Error::from).and_then(|bytes| {
            serde_json::from_slice::<netidx::config::file::Config>(&bytes)
                .map_err(anyhow::Error::from)
        });
        match cfg {
            Ok(cfg) => {
                if let Some(tls) = cfg.tls {
                    for (_, id) in tls.identities {
                        push(Identity {
                            certificate: PathBuf::from(id.certificate),
                            private_key: PathBuf::from(id.private_key),
                            trusted: PathBuf::from(id.trusted),
                        });
                    }
                }
            }
            Err(e) => warn!("renewd: could not read client config {path:?}: {e:#}"),
        }
    }
    if let Ok(path) = paths::discover_resolver_config() {
        match crate::resolver::ResolverConfig::load(&path) {
            Ok(cfg) => {
                use netidx::resolver_server::config::file::Auth;
                for m in &cfg.0.member_servers {
                    if let Auth::Tls { trusted, certificate, private_key, .. } = &m.auth
                    {
                        push(Identity {
                            certificate: PathBuf::from(certificate.as_str()),
                            private_key: PathBuf::from(private_key.as_str()),
                            trusted: PathBuf::from(trusted.as_str()),
                        });
                    }
                }
            }
            Err(e) => warn!("renewd: could not read resolver config {path:?}: {e:#}"),
        }
    }
    #[cfg(unix)]
    if let Ok(path) = paths::discover_conf_server_config() {
        match crate::conf_server_config::ConfServerConfig::load(&path) {
            Ok(cfg) => push(Identity {
                certificate: cfg.serving_cert,
                private_key: cfg.serving_key,
                trusted: cfg.trusted,
            }),
            Err(e) => warn!("renewd: could not read conf-server config {path:?}: {e:#}"),
        }
    }
    out
}

/// The first certificate in a PEM file, parsed: (first DNS SAN,
/// notBefore, notAfter) as unix seconds.
fn cert_facts(certificate: &Path) -> Result<(String, u64, u64)> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let pem = std::fs::read(certificate)
        .with_context(|| format!("reading {}", certificate.display()))?;
    let der = rustls_pemfile::certs(&mut std::io::Cursor::new(&pem))
        .next()
        .ok_or_else(|| anyhow!("no certificate in {}", certificate.display()))?
        .context("parsing certificate PEM")?;
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| anyhow!("parsing certificate: {e}"))?;
    let name = crate::tls::first_dns_san_from_der(der.as_ref())
        .ok_or_else(|| anyhow!("certificate has no DNS SAN"))?;
    let nb = cert.validity().not_before.timestamp() as u64;
    let na = cert.validity().not_after.timestamp() as u64;
    Ok((name, nb, na))
}

/// Inside the renewal window: less than `min(30d, validity/3)`
/// remaining. A third of a short-lived cert's life is plenty of slack
/// for an absent admin; 30 days caps the head start on long ones.
pub fn needs_renewal(not_before: u64, not_after: u64, now: u64) -> bool {
    let validity = not_after.saturating_sub(not_before);
    let window = (validity / 3).min(30 * 24 * 3600);
    not_after.saturating_sub(now) < window
}

fn load_roots(trusted: &Path) -> Result<rustls::RootCertStore> {
    let pem = std::fs::read(trusted)
        .with_context(|| format!("reading trust bundle {}", trusted.display()))?;
    let mut roots = rustls::RootCertStore::empty();
    for der in rustls_pemfile::certs(&mut std::io::Cursor::new(&pem)) {
        roots.add(der.context("parsing trust bundle")?).context("adding trust anchor")?;
    }
    anyhow::ensure!(!roots.is_empty(), "{} contains no certificates", trusted.display());
    Ok(roots)
}

/// Find the network's CA conf server, verified against `roots`:
/// an explicit override, the local conf-server config (unix), or mDNS
/// discovery + a PKI-verified peer walk. Unattended-safe — candidates
/// that don't verify against our trust bundle are just skipped.
async fn find_ca_addr(
    server: Option<SocketAddr>,
    roots: &rustls::RootCertStore,
) -> Result<SocketAddr> {
    if let Some(s) = server {
        return Ok(s);
    }
    #[cfg(unix)]
    if let Ok(path) = paths::discover_conf_server_config()
        && let Ok(cfg) = crate::conf_server_config::ConfServerConfig::load(&path)
    {
        if cfg.roles.ca.is_some() {
            let mut addr = cfg.listen;
            if addr.ip().is_unspecified() {
                addr.set_ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            }
            return Ok(addr);
        }
        if let Some(ca) = cfg.ca_addr {
            return Ok(ca);
        }
    }
    // Discovery: browse, then walk peers over verified connections
    // until someone reports the CA.
    let found = crate::discovery::browse(Duration::from_secs(3)).await?;
    let mut queue: Vec<SocketAddr> =
        found.iter().flat_map(|d| d.socket_addrs()).collect();
    let mut visited: Vec<SocketAddr> = Vec::new();
    while let Some(addr) = queue.pop() {
        if visited.contains(&addr) || visited.len() >= 64 {
            continue;
        }
        visited.push(addr);
        match conf_client::get_info_pki(addr, NodeKind::Client, roots.clone()).await {
            Ok(info) => {
                if let Some(ca) = info.ca_addr {
                    return Ok(ca);
                }
                queue.extend(info.peers);
            }
            Err(e) => {
                // Wrong network or down — either way, not ours.
                log::debug!("renewd: conf server {addr} not usable: {e:#}");
            }
        }
    }
    bail!("no conf server holding the CA could be found")
}

/// The persisted request id of an in-flight renewal, kept beside the
/// certificate so a daemon restart resumes polling instead of queueing
/// a duplicate.
fn pending_path(certificate: &Path) -> PathBuf {
    certificate.with_file_name("renewal.id")
}

/// One renewal pass over a single identity. Returns a short
/// human-readable status.
async fn renew_identity(
    id: &Identity,
    server: Option<SocketAddr>,
) -> Result<&'static str> {
    let (name, nb, na) = cert_facts(&id.certificate)?;
    let now = now_unix();
    if !needs_renewal(nb, na, now) {
        // Outside the window: clear any stale pending marker and move on.
        let _ = std::fs::remove_file(pending_path(&id.certificate));
        return Ok("current");
    }
    let roots = load_roots(&id.trusted)?;
    let ca_addr = find_ca_addr(server, &roots).await?;
    let cert_pem = std::fs::read(&id.certificate)?;
    let key_pem = std::fs::read(&id.private_key)?;
    // The original validity is what we re-request (capped by the
    // approving admin's policy server-side).
    let validity_days = ((na.saturating_sub(nb)) / 86_400).max(1) as u32;
    let pending = conf_client::enqueue_renewal(
        ca_addr,
        NodeKind::Client,
        &name,
        validity_days,
        &cert_pem,
        &key_pem,
        roots.clone(),
    )
    .await
    .with_context(|| format!("queueing renewal of {name}"))?;
    atomic::write_atomic(
        &pending_path(&id.certificate),
        pending.request_id.as_bytes(),
        0o644,
    )?;
    info!(
        "renewd: queued renewal of {name} (request {}); waiting for approval",
        pending.request_id
    );
    // Poll within this cycle for a bounded while — `autorenew` answers
    // in seconds; a human admin may take until some later cycle.
    let deadline = tokio::time::Instant::now() + APPROVAL_POLL;
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        match conf_client::poll_renewal(ca_addr, NodeKind::Client, &pending, roots.clone())
            .await?
        {
            conf_client::PollOutcome::Pending => {
                if tokio::time::Instant::now() >= deadline {
                    return Ok("awaiting approval");
                }
            }
            conf_client::PollOutcome::Issued(issued) => {
                install(id, &issued)?;
                let _ = std::fs::remove_file(pending_path(&id.certificate));
                info!(
                    "renewd: renewed {name}; running processes pick it up on restart"
                );
                return Ok("renewed");
            }
            conf_client::PollOutcome::Denied(reason) => {
                let _ = std::fs::remove_file(pending_path(&id.certificate));
                bail!("renewal of {name} was denied: {reason}");
            }
            conf_client::PollOutcome::Expired => {
                let _ = std::fs::remove_file(pending_path(&id.certificate));
                bail!("renewal request for {name} expired before approval");
            }
        }
    }
}

/// Install an issued renewal over the identity's files. Key first
/// (0600), then certificate, then the (possibly rolled) trust bundle —
/// each write atomic.
///
/// Chain preservation: a conf server's serving certificate file is a
/// chain `[leaf, ca]` (clients read the CA from the end of it); plain
/// client identities hold just the leaf. The renewal response carries
/// the bare leaf, so whichever shape the file had is rebuilt — the
/// issuing CA appended from the returned bundle when the old file was
/// a chain.
fn install(id: &Identity, issued: &conf_client::Issued) -> Result<()> {
    let was_chain = std::fs::read(&id.certificate)
        .map(|pem| {
            rustls_pemfile::certs(&mut std::io::Cursor::new(pem)).flatten().count() > 1
        })
        .unwrap_or(false);
    let cert_payload = if was_chain {
        let ca = conf_client::issuing_ca_pem(&issued.trusted_pem, &issued.cert_pem)
            .context("rebuilding the serving chain")?;
        format!("{}{}", issued.cert_pem, ca)
    } else {
        issued.cert_pem.clone()
    };
    atomic::write_atomic(&id.private_key, issued.private_key_pem.as_bytes(), 0o600)?;
    atomic::write_atomic(&id.certificate, cert_payload.as_bytes(), 0o644)?;
    atomic::write_atomic(&id.trusted, issued.trusted_pem.as_bytes(), 0o644)?;
    for w in &issued.warnings {
        warn!("renewd: server warning: {w}");
    }
    Ok(())
}

/// Pull the network CRL and install it beside every trust bundle on
/// this host (only when changed — the resolver's CRL-watching acceptor
/// rebuilds on mtime, so gratuitous writes would churn it).
async fn distribute_crl(ids: &[Identity], server: Option<SocketAddr>) -> Result<bool> {
    let mut updated = false;
    let mut done: Vec<&Path> = Vec::new();
    for id in ids {
        if done.contains(&id.trusted.as_path()) {
            continue;
        }
        done.push(id.trusted.as_path());
        let roots = load_roots(&id.trusted)?;
        let ca_addr = find_ca_addr(server, &roots).await?;
        let crl = match conf_client::get_crl_pki(ca_addr, NodeKind::Client, roots).await?
        {
            Some(pem) => pem,
            None => continue, // nothing ever revoked
        };
        let dest = id.trusted.with_file_name("crl.pem");
        let current = std::fs::read(&dest).unwrap_or_default();
        if current != crl.as_bytes() {
            atomic::write_atomic(&dest, crl.as_bytes(), 0o644)?;
            info!("renewd: installed updated CRL at {}", dest.display());
            updated = true;
        }
    }
    Ok(updated)
}

/// One full pass: renew what needs renewing, distribute the CRL.
/// Errors on individual identities are reported, not fatal — one
/// broken identity must not stop the others from renewing.
pub async fn run_once(server: Option<SocketAddr>) -> Result<()> {
    let ids = host_identities();
    if ids.is_empty() {
        info!("renewd: no TLS identities on this host");
        return Ok(());
    }
    for id in &ids {
        match renew_identity(id, server).await {
            Ok(status) => {
                info!("renewd: {} — {status}", id.certificate.display())
            }
            Err(e) => warn!("renewd: {} — {e:#}", id.certificate.display()),
        }
    }
    if let Err(e) = distribute_crl(&ids, server).await {
        warn!("renewd: CRL distribution failed: {e:#}");
    }
    Ok(())
}

/// Run forever: an immediate pass, then one per `interval`.
pub async fn run(server: Option<SocketAddr>, interval: Duration) -> Result<()> {
    loop {
        if let Err(e) = run_once(server).await {
            warn!("renewd: pass failed: {e:#}");
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_renewal_window_is_a_third_capped_at_30_days() {
        let day = 86_400;
        // 90-day cert: window = 30d. At 31d remaining: fine; at 29d: renew.
        assert!(!needs_renewal(0, 90 * day, 59 * day));
        assert!(needs_renewal(0, 90 * day, 61 * day));
        // 2-year cert: window capped at 30d, not 8 months.
        assert!(!needs_renewal(0, 730 * day, 690 * day));
        assert!(needs_renewal(0, 730 * day, 701 * day));
        // 9-day cert: window = 3d.
        assert!(!needs_renewal(0, 9 * day, 5 * day));
        assert!(needs_renewal(0, 9 * day, 7 * day));
        // Expired is definitely inside the window.
        assert!(needs_renewal(0, 9 * day, 10 * day));
    }
}
