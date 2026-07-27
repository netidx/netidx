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
//!    identity, the resolver's (if one runs here), and the admin
//!    server's serving cert (unix) — and for any certificate inside
//!    its renewal window, queues a **verified renewal**: a fresh key +
//!    CSR enqueued over a connection authenticated by the *current*
//!    cert. The admin server marks it proof-of-possession; an admin (or
//!    the `autorenew` daemon) approves without ceremony; we poll and
//!    install the result atomically.
//! 2. **Distributes the CRL**: pulls it from the CA and writes
//!    `crl.pem` beside each trusted bundle (only on change) — beside
//!    the resolver's bundle this is what makes revocation take effect,
//!    via the CRL-watching acceptor.
//!
//! Everything here runs unattended over real PKI: the daemon verifies
//! admin servers with webpki against the trust bundles it already has.
//! No TOFU, no glyphs — those are for humans establishing trust;
//! renewal is continuation under trust already established.

use crate::{admin_proto::NodeKind, atomic, paths, transport};
// The `admin_proto` module alias is only needed by the unix-only renewal
// path below (SERVING_SAN / SignResponse); `NodeKind` is cross-platform.
#[cfg(unix)]
use crate::{admin_proto, local};
use anyhow::{Context, Result, anyhow, bail};
use log::{info, warn};
use serde_derive::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use zeroize::Zeroizing;

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// How often the daemon wakes to scan (the first scan is immediate).
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(6 * 3600);

/// How long a queued renewal is polled within one cycle before leaving
/// it for the next (the admin may simply not have approved yet — the
/// full pending state, sealed fresh key included, is persisted and
/// resumed next cycle).
const APPROVAL_POLL: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Clients {
    pki: Vec<CachedPkiClient>,
    authenticated: Vec<CachedAuthenticatedClient>,
}

struct CachedPkiClient {
    trusted: PathBuf,
    digest: [u8; 32],
    roots: rustls::RootCertStore,
    client: transport::PkiClient,
}

struct CachedAuthenticatedClient {
    certificate: PathBuf,
    private_key: PathBuf,
    trust_digest: [u8; 32],
    identity_digest: [u8; 32],
    client: transport::AuthenticatedPkiClient,
}

impl Clients {
    fn pki(
        &mut self,
        trusted: &Path,
    ) -> Result<(transport::PkiClient, rustls::RootCertStore, [u8; 32])> {
        let pem = std::fs::read(trusted)
            .with_context(|| format!("reading trust bundle {}", trusted.display()))?;
        let digest: [u8; 32] = Sha256::digest(&pem).into();
        if let Some(cached) = self
            .pki
            .iter()
            .find(|cached| cached.trusted == trusted && cached.digest == digest)
        {
            return Ok((cached.client.clone(), cached.roots.clone(), digest));
        }
        let roots = load_roots_from_pem(&pem, trusted)?;
        let client = transport::PkiClient::new(roots.clone())?;
        let cached = CachedPkiClient {
            trusted: trusted.to_path_buf(),
            digest,
            roots: roots.clone(),
            client: client.clone(),
        };
        match self.pki.iter().position(|cached| cached.trusted == trusted) {
            Some(i) => self.pki[i] = cached,
            None => self.pki.push(cached),
        }
        Ok((client, roots, digest))
    }

    fn authenticated(
        &mut self,
        identity: &Identity,
        roots: rustls::RootCertStore,
        trust_digest: [u8; 32],
    ) -> Result<transport::AuthenticatedPkiClient> {
        let cert = std::fs::read(&identity.certificate)
            .with_context(|| format!("reading {}", identity.certificate.display()))?;
        let key =
            netidx::tls::load_private_key(None, &identity.private_key.to_string_lossy())
                .with_context(|| {
                    format!("loading private key {}", identity.private_key.display())
                })?;
        let mut digest = Sha256::new();
        digest.update(cert.len().to_le_bytes());
        digest.update(&cert);
        digest.update(key.secret_der().len().to_le_bytes());
        digest.update(key.secret_der());
        let identity_digest = digest.finalize().into();
        if let Some(cached) = self.authenticated.iter().find(|cached| {
            cached.certificate == identity.certificate
                && cached.private_key == identity.private_key
                && cached.trust_digest == trust_digest
                && cached.identity_digest == identity_digest
        }) {
            return Ok(cached.client.clone());
        }
        let client = transport::AuthenticatedPkiClient::new(roots, &cert, key)?;
        let cached = CachedAuthenticatedClient {
            certificate: identity.certificate.clone(),
            private_key: identity.private_key.clone(),
            trust_digest,
            identity_digest,
            client: client.clone(),
        };
        match self.authenticated.iter().position(|cached| {
            cached.certificate == identity.certificate
                && cached.private_key == identity.private_key
        }) {
            Some(i) => self.authenticated[i] = cached,
            None => self.authenticated.push(cached),
        }
        Ok(client)
    }
}

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
/// unix — the admin server's serving identity. Deduped by certificate
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
                    if let Auth::Tls { trusted, certificate, private_key, .. } = &m.auth {
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
    if let Ok(path) = paths::discover_admin_server_config() {
        match crate::admin_server_config::load(&path) {
            Ok(cfg) => push(Identity {
                certificate: cfg.serving_cert,
                private_key: cfg.serving_key,
                trusted: cfg.trusted,
            }),
            Err(e) => warn!("renewd: could not read admin-server config {path:?}: {e:#}"),
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

fn load_roots_from_pem(pem: &[u8], trusted: &Path) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for der in rustls_pemfile::certs(&mut std::io::Cursor::new(pem)) {
        roots.add(der.context("parsing trust bundle")?).context("adding trust anchor")?;
    }
    anyhow::ensure!(!roots.is_empty(), "{} contains no certificates", trusted.display());
    Ok(roots)
}

/// Find the admin domain's CA admin server, verified against `roots`:
/// an explicit override, the local admin-server config (unix), or mDNS
/// discovery + a PKI-verified peer walk. Unattended-safe — candidates
/// that don't verify against our trust bundle are just skipped.
async fn find_ca_addr(
    server: Option<SocketAddr>,
    client: &transport::PkiClient,
) -> Result<SocketAddr> {
    if let Some(s) = server {
        return Ok(s);
    }
    #[cfg(unix)]
    if let Ok(path) = paths::discover_admin_server_config()
        && let Ok(cfg) = crate::admin_server_config::load(&path)
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
        match transport::get_info_pki(client, addr, NodeKind::Client).await {
            Ok(info) => {
                if let Some(ca) = info.ca_addr {
                    let ca = if ca.ip().is_unspecified() {
                        SocketAddr::new(addr.ip(), ca.port())
                    } else {
                        ca
                    };
                    return Ok(ca);
                }
                queue.extend(info.peers);
            }
            Err(e) => {
                // Wrong admin domain or down — either way, not ours.
                log::debug!("renewd: admin server {addr} not usable: {e:#}");
            }
        }
    }
    bail!("no admin server holding the CA could be found")
}

/// Where a not-yet-installed renewal is persisted beside the
/// certificate so a slow approval (or a daemon restart) resumes instead
/// of queueing a duplicate: the non-secret state in `renewal.json`, the
/// fresh key in `renewal.key` (sealed exactly like an identity key where
/// a TPM exists, 0600 plaintext otherwise) with its sealed-password
/// sidecar.
fn pending_meta_path(certificate: &Path) -> PathBuf {
    certificate.with_file_name("renewal.json")
}

fn pending_key_path(certificate: &Path) -> PathBuf {
    certificate.with_file_name("renewal.key")
}

/// The non-secret half of a persisted renewal. The fresh private key is
/// NOT here — it lives sealed at [`pending_key_path`].
#[derive(Serialize, Deserialize)]
struct PersistedRenewal {
    request_id: String,
    name: String,
    our_spki: Vec<u8>,
    csr_pem: String,
}

/// Persist a queued renewal so a later cycle can resume polling and
/// install it. The fresh key is sealed the same way identity keys are.
fn persist_pending(
    certificate: &Path,
    pending: &transport::PendingRenewal,
) -> Result<()> {
    let key_path = pending_key_path(certificate);
    let _ =
        crate::tls::write_private_key_maybe_sealed(&key_path, pending.private_key_pem())
            .with_context(|| format!("persisting renewal key {}", key_path.display()))?;
    let meta = PersistedRenewal {
        request_id: pending.request_id.clone(),
        name: pending.name().to_string(),
        our_spki: pending.our_spki().to_vec(),
        csr_pem: pending.csr_pem().to_string(),
    };
    let bytes = serde_json::to_vec_pretty(&meta).context("serializing renewal state")?;
    atomic::write_atomic(&pending_meta_path(certificate), &bytes, 0o600)
}

/// Reconstruct a renewal persisted by an earlier cycle, if any.
fn load_pending(certificate: &Path) -> Result<Option<transport::PendingRenewal>> {
    let meta_path = pending_meta_path(certificate);
    let bytes = match std::fs::read(&meta_path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| format!("reading {}", meta_path.display()));
        }
    };
    let meta: PersistedRenewal =
        serde_json::from_slice(&bytes).context("parsing persisted renewal state")?;
    let private_key_pem = read_pending_key(&pending_key_path(certificate))?;
    Ok(Some(transport::PendingRenewal::resume(
        meta.request_id,
        meta.name,
        meta.our_spki,
        private_key_pem,
        meta.csr_pem,
    )))
}

/// Read back the persisted fresh key, unsealing it when a `.tpm`
/// sidecar is present (the same test [`install`] uses to decide whether
/// to re-seal).
fn read_pending_key(key_path: &Path) -> Result<Zeroizing<String>> {
    let enc = std::fs::read_to_string(key_path)
        .with_context(|| format!("reading renewal key {}", key_path.display()))?;
    let sidecar = crate::tls::sealed_sidecar(key_path);
    if sidecar.exists() {
        let blob = std::fs::read(&sidecar)
            .with_context(|| format!("reading sealed password {}", sidecar.display()))?;
        crate::tls::unseal_private_key(&enc, &blob)
            .context("unsealing the persisted renewal key")
    } else {
        Ok(Zeroizing::new(enc))
    }
}

/// Remove all persisted renewal state for an identity (on install, on
/// denial/expiry, or when the cert no longer needs renewing).
fn clear_pending(certificate: &Path) {
    let _ = std::fs::remove_file(pending_meta_path(certificate));
    let key_path = pending_key_path(certificate);
    let _ = std::fs::remove_file(crate::tls::sealed_sidecar(&key_path));
    let _ = std::fs::remove_file(&key_path);
}

/// One renewal pass over a single identity. Returns a short
/// human-readable status.
async fn renew_identity(
    id: &Identity,
    server: Option<SocketAddr>,
    clients: &mut Clients,
) -> Result<&'static str> {
    let (name, nb, na) = cert_facts(&id.certificate)?;
    let now = now_unix();
    if !needs_renewal(nb, na, now) {
        // Outside the window: clear any stale pending state and move on.
        clear_pending(&id.certificate);
        return Ok("current");
    }
    let (pki_client, roots, trust_digest) = clients.pki(&id.trusted)?;
    let installed_pem = std::fs::read_to_string(&id.trusted)
        .with_context(|| format!("reading trust bundle {}", id.trusted.display()))?;
    // Co-located serving-cert re-mint over the local control socket. The
    // serving cert is the linchpin of TLS-to-self: once it expires, a TLS
    // renewal to our own admin server can't connect to renew it — a permanent
    // deadlock that bricks the whole renewal chain. On the CA host we re-mint
    // it locally over admin.sock (SO_PEERCRED superuser, no TLS), which works
    // even when the current serving cert is already expired. Scope is the
    // serving cert only; every other co-located identity recovers on TLS
    // once the serving cert is fresh again.
    #[cfg(unix)]
    {
        if name == admin_proto::SERVING_SAN
            && let Ok(cfg_path) = paths::discover_admin_server_config()
            && let Ok(cfg) = crate::admin_server_config::load(&cfg_path)
            && cfg.roles.ca.is_some()
            && local::daemon_running(&cfg_path).await
        {
            let kc = transport::generate_key_and_csr(admin_proto::SERVING_SAN)?;
            let our_spki = transport::csr_spki(&kc.csr_pem)?;
            return match local::enroll(&cfg_path, &kc.csr_pem, cfg.listen).await? {
                admin_proto::SignResponse::Ok(admin_proto::SignOk {
                    signed_cert_pem,
                    trusted_pem,
                    warnings,
                    ..
                }) => {
                    transport::verify_issued_any(
                        &installed_pem,
                        name.as_str(),
                        &our_spki,
                        &signed_cert_pem,
                    )
                    .context("verifying the locally re-minted serving cert")?;
                    install(
                        id,
                        &transport::Issued {
                            cert_pem: signed_cert_pem,
                            private_key_pem: kc.private_key_pem,
                            trusted_pem,
                            warnings,
                        },
                    )?;
                    clear_pending(&id.certificate);
                    info!(
                        "renewd: re-minted serving cert {name} locally over admin.sock"
                    );
                    Ok("renewed (local)")
                }
                admin_proto::SignResponse::Err { reason } => {
                    bail!("local re-mint of the serving cert was refused: {reason}")
                }
            };
        }
    }
    let ca_addr = find_ca_addr(server, &pki_client).await?;
    let pending = match load_pending(&id.certificate)? {
        Some(pending) => {
            info!("renewd: resuming renewal of {name} (request {})", pending.request_id);
            pending
        }
        None => {
            // The original validity is what we re-request (capped by the
            // approving admin's policy server-side). Kept at second
            // resolution so a short-lived cert renews to the same short
            // window rather than silently rounding up to a day.
            let validity = Duration::from_secs(na.saturating_sub(nb).max(1));
            let authenticated = clients.authenticated(id, roots.clone(), trust_digest)?;
            let pending = transport::enqueue_renewal(
                &authenticated,
                ca_addr,
                NodeKind::Client,
                &name,
                validity,
            )
            .await
            .with_context(|| format!("queueing renewal of {name}"))?;
            persist_pending(&id.certificate, &pending)
                .with_context(|| format!("persisting renewal state for {name}"))?;
            info!(
                "renewd: queued renewal of {name} (request {}); waiting for approval",
                pending.request_id
            );
            pending
        }
    };
    // Poll within this cycle for a bounded while — `autorenew` answers in
    // seconds; a human admin may take until some later cycle, when the
    // persisted state above is resumed.
    let deadline = tokio::time::Instant::now() + APPROVAL_POLL;
    loop {
        tokio::time::sleep(POLL_INTERVAL).await;
        match transport::poll_renewal(
            &pki_client,
            ca_addr,
            NodeKind::Client,
            &pending,
            &installed_pem,
        )
        .await?
        {
            transport::PollOutcome::Pending => {
                if tokio::time::Instant::now() >= deadline {
                    return Ok("awaiting approval");
                }
            }
            transport::PollOutcome::Issued(issued) => {
                install(id, &issued)?;
                clear_pending(&id.certificate);
                info!("renewd: renewed {name}; running processes pick it up on restart");
                return Ok("renewed");
            }
            transport::PollOutcome::Denied(reason) => {
                clear_pending(&id.certificate);
                bail!("renewal of {name} was denied: {reason}");
            }
            transport::PollOutcome::Expired => {
                clear_pending(&id.certificate);
                bail!("renewal request for {name} expired before approval");
            }
        }
    }
}

/// Install an issued renewal over the identity's files. Key first
/// (0600), then certificate, then the (possibly rolled) trust bundle —
/// each write atomic.
///
/// Chain preservation: an admin server's serving certificate file is a
/// chain `[leaf, ca]` (clients read the CA from the end of it); plain
/// client identities hold just the leaf. The renewal response carries
/// the bare leaf, so whichever shape the file had is rebuilt — the
/// issuing CA appended from the returned bundle when the old file was
/// a chain.
///
/// Seal preservation: an identity whose key carries a TPM-sealed
/// password sidecar (`<key>.tpm`) was deliberately bound to this
/// machine — the renewal's fresh key gets a fresh password, sealed the
/// same way. If sealing fails (TPM gone) this errors rather than
/// silently degrading to a plaintext key; the old identity stays
/// intact and the daemon retries next tick.
///
/// Trust anchoring: the trust bundle is never replaced wholesale with
/// what the peer returned. [`transport::reconcile_trusted_bundle`]
/// keeps the installed roots and folds in only same-key CA-cert refreshes
/// that are either validly self-signed or signed by the installed cert's
/// own issuer (an externally-signed intermediate's root) — a compromised
/// peer cannot introduce a new trust anchor through renewal, nor overwrite
/// an anchor whose issuer it does not control.
fn install(id: &Identity, issued: &transport::Issued) -> Result<()> {
    let was_chain = std::fs::read(&id.certificate)
        .map(|pem| {
            rustls_pemfile::certs(&mut std::io::Cursor::new(pem)).flatten().count() > 1
        })
        .unwrap_or(false);
    let cert_payload = if was_chain {
        let ca = transport::issuing_ca_pem(&issued.trusted_pem, &issued.cert_pem)
            .context("rebuilding the serving chain")?;
        format!("{}{}", issued.cert_pem, ca)
    } else {
        issued.cert_pem.clone()
    };
    let sidecar = crate::tls::sealed_sidecar(&id.private_key);
    if sidecar.exists() {
        let (enc_pem, blob) = crate::tls::seal_private_key(&issued.private_key_pem)
            .context("re-sealing the renewed key (old key left in place)")?;
        // Two files, two renames: a crash exactly between them leaves a
        // mismatched key/sidecar pair (unloadable until the next renewal
        // or a re-issue). The window is microseconds inside one process
        // and the daemon only reads keys at startup — accepted.
        atomic::write_atomic(&sidecar, &blob, 0o600)?;
        atomic::write_atomic(&id.private_key, enc_pem.as_bytes(), 0o600)?;
    } else {
        atomic::write_atomic(&id.private_key, issued.private_key_pem.as_bytes(), 0o600)?;
    }
    atomic::write_atomic(&id.certificate, cert_payload.as_bytes(), 0o644)?;
    let installed_pem = std::fs::read_to_string(&id.trusted).with_context(|| {
        format!("reading current trust bundle {}", id.trusted.display())
    })?;
    let reconciled =
        transport::reconcile_trusted_bundle(&installed_pem, &issued.trusted_pem)
            .context("reconciling the renewed trust bundle")?;
    atomic::write_atomic(&id.trusted, reconciled.as_bytes(), 0o644)?;
    for w in &issued.warnings {
        warn!("renewd: server warning: {w}");
    }
    Ok(())
}

/// Pull the admin domain CRL and install it beside every trust bundle on
/// this host (only when changed — the resolver's CRL-watching acceptor
/// rebuilds on mtime, so gratuitous writes would churn it).
async fn distribute_crl(
    ids: &[Identity],
    server: Option<SocketAddr>,
    clients: &mut Clients,
) -> Result<bool> {
    let mut updated = false;
    let mut done: Vec<&Path> = Vec::new();
    for id in ids {
        if done.contains(&id.trusted.as_path()) {
            continue;
        }
        done.push(id.trusted.as_path());
        let (client, _, _) = clients.pki(&id.trusted)?;
        let ca_addr = find_ca_addr(server, &client).await?;
        let crl = match transport::get_crl_pki(&client, ca_addr, NodeKind::Client).await?
        {
            Some(pem) => pem,
            None => continue, // nothing ever revoked
        };
        // Never install a CRL we haven't verified. `find_ca_addr` accepts any
        // peer that validates against our bundle and claims to hold the CA, so
        // without this a compromised peer could feed arbitrary bytes to every
        // renewing host and break its resolver's TLS config rebuild. Same rule
        // the server-to-server apply path uses, and the same
        // signed-by-some-CA-in-our-bundle rule `verify_issued` uses for leaves.
        let installed = std::fs::read_to_string(&id.trusted)
            .with_context(|| format!("reading trust bundle {}", id.trusted.display()))?;
        if let Err(e) = transport::validate_crl_against_bundle(&crl, &installed) {
            warn!("renewd: refusing the CRL offered for {}: {e:#}", id.trusted.display());
            continue;
        }
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
async fn run_once_with(server: Option<SocketAddr>, clients: &mut Clients) -> Result<()> {
    let ids = host_identities();
    if ids.is_empty() {
        info!("renewd: no TLS identities on this host");
        return Ok(());
    }
    for id in &ids {
        match renew_identity(id, server, clients).await {
            Ok(status) => {
                info!("renewd: {} — {status}", id.certificate.display())
            }
            Err(e) => warn!("renewd: {} — {e:#}", id.certificate.display()),
        }
    }
    if let Err(e) = distribute_crl(&ids, server, clients).await {
        warn!("renewd: CRL distribution failed: {e:#}");
    }
    Ok(())
}

pub async fn run_once(server: Option<SocketAddr>) -> Result<()> {
    run_once_with(server, &mut Clients::default()).await
}

/// Run forever: an immediate pass, then one per `interval`.
pub async fn run(server: Option<SocketAddr>, interval: Duration) -> Result<()> {
    let mut clients = Clients::default();
    loop {
        if let Err(e) = run_once_with(server, &mut clients).await {
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
