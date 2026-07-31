//! The certificate renewal task: the moving part that removes all
//! the other moving parts.
//!
//! Certificate lifecycle belongs to one task per TLS host, so nothing else
//! has to think about it: applications never know renewal exists, no
//! publisher/subscriber needs write access to key material, and there's no
//! mDNS chatter in every process. It is spawned by whichever process owns
//! the host's housekeeping — the admin server where one runs, otherwise
//! [`crate::agent`] — and never spawns itself, so its owner keeps
//! cancellation. Each cycle it:
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
use arcstr::ArcStr;
use compact_str::{CompactString, format_compact};
use log::{info, warn};
use serde_derive::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    future::Future,
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

/// Everything the renewal task needs to know about the host it runs on.
///
/// The path fields are unconditional even though only unix consumes them:
/// a `#[cfg(unix)]` field would force every constructor to be cfg'd, which
/// is how a windows build breaks without anyone noticing.
#[derive(Debug, Clone)]
pub struct RenewalConfig {
    /// Explicit CA admin-server address. `None` runs the discovery cascade.
    pub server: Option<SocketAddr>,
    /// This host's admin-server config. Set by the in-server caller so the
    /// task targets the config actually being served rather than whatever
    /// the standard search order finds first.
    pub admin_server_config: Option<PathBuf>,
    /// The CA directory, when this host holds the CA. Trust bundles inside
    /// it are CA-owned; [`distribute_crl`] must not write beside them.
    pub ca_dir: Option<PathBuf>,
    pub interval: Duration,
}

impl Default for RenewalConfig {
    fn default() -> Self {
        RenewalConfig {
            server: None,
            admin_server_config: None,
            ca_dir: None,
            interval: DEFAULT_INTERVAL,
        }
    }
}

/// What renewing one identity did.
enum Outcome {
    /// Outside its renewal window.
    Current,
    /// Installed. `local` means it was re-minted over the CA host's own
    /// control socket rather than renewed over TLS.
    Renewed { local: bool },
    /// Queued, still waiting on an admin; resumes next pass.
    AwaitingApproval,
}

/// This host's admin-server config: the caller's override, else the
/// standard search order.
#[cfg(unix)]
fn admin_server_config(override_: Option<&Path>) -> Option<PathBuf> {
    match override_ {
        Some(path) => Some(path.to_path_buf()),
        None => paths::discover_admin_server_config().ok(),
    }
}

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
pub fn host_identities(admin_server_config_path: Option<&Path>) -> Vec<Identity> {
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
    if let Some(path) = admin_server_config(admin_server_config_path) {
        match crate::admin_server_config::load(&path) {
            Ok(cfg) => push(Identity {
                certificate: cfg.serving_cert,
                private_key: cfg.serving_key,
                trusted: cfg.trusted,
            }),
            Err(e) => warn!("renewd: could not read admin-server config {path:?}: {e:#}"),
        }
    }
    #[cfg(not(unix))]
    let _ = admin_server_config_path;
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

/// How many admin servers one search will contact before giving up.
const MAX_WALK: usize = 64;

/// Walk `queue` until an admin server reports where the CA lives,
/// following each one's peers. `visited` carries across calls so a later
/// seeding doesn't redo an earlier one's work.
/// `info` is spelled `FnMut -> Future` rather than `AsyncFnMut` on purpose:
/// the sugar gives no way to require the returned future be `Send`, and this
/// runs inside a `tokio::spawn`ed task in the admin server.
async fn walk<F, Fut>(
    queue: &mut Vec<SocketAddr>,
    visited: &mut Vec<SocketAddr>,
    mut info: F,
) -> Option<SocketAddr>
where
    F: FnMut(SocketAddr) -> Fut,
    Fut: Future<Output = Result<crate::admin_proto::GetInfoResponse>> + Send,
{
    while let Some(addr) = queue.pop() {
        if visited.len() >= MAX_WALK {
            break;
        }
        if visited.contains(&addr) {
            continue;
        }
        visited.push(addr);
        match info(addr).await {
            Ok(info) => {
                if let Some(ca) = info.ca_addr {
                    return Some(if ca.ip().is_unspecified() {
                        SocketAddr::new(addr.ip(), ca.port())
                    } else {
                        ca
                    });
                }
                queue.extend(info.peers);
            }
            Err(e) => {
                // Wrong admin domain or down — either way, not ours.
                log::debug!("renewd: admin server {addr} not usable: {e:#}");
            }
        }
    }
    None
}

/// [`walk`] over verified connections.
async fn walk_for_ca(
    client: &transport::PkiClient,
    queue: &mut Vec<SocketAddr>,
    visited: &mut Vec<SocketAddr>,
) -> Option<SocketAddr> {
    walk(queue, visited, |addr| transport::get_info_pki(client, addr, NodeKind::Client))
        .await
}

/// Find the admin domain's CA admin server, verified against `roots`: an
/// explicit override, the local admin-server config (unix), then a
/// PKI-verified walk over [`crate::discovery::admin_servers`]. Each
/// candidate's peers extend the walk, so reaching any admin server is
/// enough to reach the CA. Unattended-safe — candidates that don't verify
/// against our trust bundle are just skipped.
async fn find_ca_addr(
    cfg: &RenewalConfig,
    client: &transport::PkiClient,
) -> Result<SocketAddr> {
    if let Some(s) = cfg.server {
        return Ok(s);
    }
    #[cfg(unix)]
    if let Some(path) = admin_server_config(cfg.admin_server_config.as_deref())
        && let Ok(local) = crate::admin_server_config::load(&path)
    {
        if local.roles.ca.is_some() {
            let mut addr = local.listen;
            if addr.ip().is_unspecified() {
                addr.set_ip(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
            }
            return Ok(addr);
        }
        if let Some(ca) = local.ca_addr {
            return Ok(ca);
        }
    }
    // Reversed: the walk pops from the back, and the earlier a candidate
    // is in the list the better it is.
    let mut queue: Vec<SocketAddr> =
        crate::discovery::admin_servers().await.into_iter().rev().collect();
    let mut visited: Vec<SocketAddr> = Vec::new();
    if let Some(ca) = walk_for_ca(client, &mut queue, &mut visited).await {
        return Ok(ca);
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

/// One renewal pass over a single identity.
async fn renew_identity(
    id: &Identity,
    cfg: &RenewalConfig,
    clients: &mut Clients,
) -> Result<Outcome> {
    let (name, nb, na) = cert_facts(&id.certificate)?;
    let now = now_unix();
    if !needs_renewal(nb, na, now) {
        // Outside the window: clear any stale pending state and move on.
        clear_pending(&id.certificate);
        return Ok(Outcome::Current);
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
            && let Some(cfg_path) =
                admin_server_config(cfg.admin_server_config.as_deref())
            && let Ok(local_cfg) = crate::admin_server_config::load(&cfg_path)
            && local_cfg.roles.ca.is_some()
            && local::daemon_running(&cfg_path).await
        {
            let kc = transport::generate_key_and_csr(admin_proto::SERVING_SAN)?;
            let our_spki = transport::csr_spki(&kc.csr_pem)?;
            return match local::enroll(&cfg_path, &kc.csr_pem, local_cfg.listen).await? {
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
                    install_blocking(
                        id.clone(),
                        transport::Issued {
                            cert_pem: signed_cert_pem,
                            private_key_pem: kc.private_key_pem,
                            trusted_pem,
                            warnings,
                        },
                    )
                    .await?;
                    clear_pending(&id.certificate);
                    Ok(Outcome::Renewed { local: true })
                }
                admin_proto::SignResponse::Err { reason } => {
                    bail!("local re-mint of the serving cert was refused: {reason}")
                }
            };
        }
    }
    let ca_addr = find_ca_addr(cfg, &pki_client).await?;
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
                    return Ok(Outcome::AwaitingApproval);
                }
            }
            transport::PollOutcome::Issued(issued) => {
                install_blocking(id.clone(), issued).await?;
                clear_pending(&id.certificate);
                return Ok(Outcome::Renewed { local: false });
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
/// chain `[leaf, CA]` (clients read the CA from the end of it); plain
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
/// [`install`] off the runtime. It seals to the TPM and fsyncs both the
/// file and its parent directory, which is fine in a process that does
/// nothing else and not fine on an admin-server worker.
async fn install_blocking(id: Identity, issued: transport::Issued) -> Result<()> {
    tokio::task::spawn_blocking(move || install(&id, &issued))
        .await
        .context("certificate install task panicked")?
}

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
    cfg: &RenewalConfig,
    clients: &mut Clients,
) -> Result<bool> {
    let mut updated = false;
    for (i, id) in ids.iter().enumerate() {
        // One CRL per distinct trust bundle. Looking back over `ids` rather
        // than accumulating a seen-set keeps the future free of borrows
        // (`tokio::spawn` can't prove a `Vec<&Path>` one is Send).
        if ids[..i].iter().any(|seen| seen.trusted == id.trusted) {
            continue;
        }
        // A trust bundle inside the CA directory is CA-owned, and the
        // `crl.pem` beside it *is* the CA's authoritative signed CRL. On the
        // CA host this would fetch our own bytes over TLS-to-self and write
        // them back over the original.
        if let Some(ca_dir) = &cfg.ca_dir
            && id.trusted.starts_with(ca_dir)
        {
            continue;
        }
        let (client, _, _) = clients.pki(&id.trusted)?;
        let ca_addr = find_ca_addr(cfg, &client).await?;
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

/// What one pass did. A pass has no failure of its own — every error
/// belongs to one identity and lands in `failed`, so one broken identity
/// can't stop the others from renewing. Callers report from this rather
/// than assuming success.
#[derive(Debug, Default)]
pub struct PassReport {
    pub renewed: Vec<PathBuf>,
    pub awaiting_approval: Vec<PathBuf>,
    pub current: usize,
    pub failed: Vec<(PathBuf, ArcStr)>,
    pub crl_updated: bool,
    /// How long until the soonest certificate on this host falls due.
    /// `None` when there is nothing to renew.
    pub next_due: Option<Duration>,
}

impl PassReport {
    /// One line an operator can read, or `None` when there was nothing to
    /// do at all.
    pub fn summary(&self) -> Option<CompactString> {
        if self.renewed.is_empty()
            && self.awaiting_approval.is_empty()
            && self.failed.is_empty()
            && !self.crl_updated
        {
            return None;
        }
        let mut s = CompactString::const_new("");
        let mut sep = "";
        for (n, what) in [
            (self.renewed.len(), "renewed"),
            (self.awaiting_approval.len(), "awaiting approval"),
            (self.failed.len(), "failed"),
        ] {
            if n > 0 {
                s.push_str(sep);
                s.push_str(&format_compact!("{n} {what}"));
                sep = ", ";
            }
        }
        if self.crl_updated {
            s.push_str(sep);
            s.push_str("CRL updated");
        }
        Some(s)
    }

    fn record(&mut self, id: &Identity, outcome: Result<Outcome>) {
        match outcome {
            Ok(Outcome::Current) => self.current += 1,
            Ok(Outcome::Renewed { local }) => {
                info!(
                    "renewd: renewed {}{}",
                    id.certificate.display(),
                    if local { " (local)" } else { "" }
                );
                self.renewed.push(id.certificate.clone());
            }
            Ok(Outcome::AwaitingApproval) => {
                self.awaiting_approval.push(id.certificate.clone())
            }
            Err(e) => {
                warn!("renewd: {} — {e:#}", id.certificate.display());
                self.failed.push((
                    id.certificate.clone(),
                    format_compact!("{e:#}").as_str().into(),
                ));
            }
        }
    }
}

/// Never sleep less than this between passes, however soon the next
/// certificate is due. A cert stuck awaiting approval is due *now* on every
/// pass, and that must not become a spin.
const MIN_SCAN_INTERVAL: Duration = Duration::from_secs(30);

/// How long until the soonest certificate falls due for renewal.
///
/// The scan cadence follows this rather than a fixed interval, because a
/// fixed one silently fails the short-lived case: a certificate whose whole
/// validity is under three scan intervals can pass from "not due yet" to
/// expired between two consecutive scans, and nothing would ever notice.
fn earliest_due(ids: &[Identity], now: u64) -> Option<Duration> {
    ids.iter()
        .filter_map(|id| cert_facts(&id.certificate).ok())
        .map(|(_, nb, na)| {
            let validity = na.saturating_sub(nb);
            let window = (validity / 3).min(30 * 24 * 3600);
            Duration::from_secs(na.saturating_sub(window).saturating_sub(now))
        })
        .min()
}

/// The renewal task. Holds the rustls client cache across passes, which
/// is why it is an object rather than a free function.
pub struct Renewer {
    cfg: RenewalConfig,
    clients: Clients,
}

impl Renewer {
    pub fn new(cfg: RenewalConfig) -> Self {
        Renewer { cfg, clients: Clients::default() }
    }

    /// One full pass: renew what needs renewing, distribute the CRL.
    pub async fn pass(&mut self) -> PassReport {
        let mut report = PassReport::default();
        let cfg = self.cfg.clone();
        let ids = tokio::task::spawn_blocking(move || {
            host_identities(cfg.admin_server_config.as_deref())
        })
        .await
        .unwrap_or_else(|e| {
            warn!("renewd: identity scan panicked: {e}");
            Vec::new()
        });
        if ids.is_empty() {
            info!("renewd: no TLS identities on this host");
            return report;
        }
        for id in &ids {
            let outcome = renew_identity(id, &self.cfg, &mut self.clients).await;
            report.record(id, outcome);
        }
        match distribute_crl(&ids, &self.cfg, &mut self.clients).await {
            Ok(updated) => report.crl_updated = updated,
            Err(e) => warn!("renewd: CRL distribution failed: {e:#}"),
        }
        // Recomputed from disk so a certificate this pass just installed is
        // accounted for at its new expiry, not its old one.
        report.next_due = earliest_due(&ids, now_unix());
        report
    }

    /// How long to wait before the next pass: the configured interval, or
    /// sooner if a certificate falls due before that.
    pub fn wait_after(&self, report: &PassReport) -> Duration {
        match report.next_due {
            Some(due) => self.cfg.interval.min(due).max(MIN_SCAN_INTERVAL),
            None => self.cfg.interval,
        }
    }
}

/// One pass, for `renew now` and the TUI's manual action.
pub async fn run_once(cfg: RenewalConfig) -> PassReport {
    Renewer::new(cfg).pass().await
}

/// Run forever: an immediate pass, then one per `cfg.interval`. Spawned
/// by the caller — the agent selects it against the sync task, and the
/// admin server keys it to its own lifetime — so it never spawns itself.
///
/// Cancellation is drop. That is safe because the one network-then-persist
/// sequence (`enqueue_renewal` then `persist_pending`) has no await
/// between the two, so a dropped pass loses at most an in-flight poll.
pub async fn run(cfg: RenewalConfig) -> std::convert::Infallible {
    let mut renewer = Renewer::new(cfg);
    loop {
        let report = renewer.pass().await;
        if let Some(summary) = report.summary() {
            info!("renewd: {summary}");
        }
        tokio::time::sleep(renewer.wait_after(&report)).await;
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

    /// A pass in which everything failed must not read as success — the
    /// TUI used to report "Scanned and renewed certificates" regardless.
    #[test]
    fn a_pass_that_renewed_nothing_says_so() {
        assert_eq!(PassReport::default().summary(), None);

        let mut all_current = PassReport::default();
        all_current.current = 3;
        assert_eq!(all_current.summary(), None);

        let mut failed = PassReport::default();
        failed.failed.push((PathBuf::from("/a.pem"), "TPM gone".into()));
        let summary = failed.summary().unwrap();
        assert!(summary.contains("1 failed"), "{summary}");
        assert!(!summary.contains("renewed"), "{summary}");

        let mut mixed = PassReport::default();
        mixed.renewed.push(PathBuf::from("/a.pem"));
        mixed.awaiting_approval.push(PathBuf::from("/b.pem"));
        mixed.crl_updated = true;
        assert_eq!(
            mixed.summary().unwrap(),
            "1 renewed, 1 awaiting approval, CRL updated"
        );
    }

    /// A fixed scan interval silently fails short-lived certificates: with
    /// a 6h interval and a 10m cert, a pass sees "8 minutes left, not due"
    /// and the next pass is five hours after it expired. The wait has to
    /// follow the deadline.
    #[test]
    fn the_wait_follows_the_soonest_deadline() {
        let renewer = Renewer::new(RenewalConfig {
            interval: Duration::from_secs(6 * 3600),
            ..Default::default()
        });
        let mut report = PassReport::default();

        // Nothing to renew: the configured interval stands.
        assert_eq!(renewer.wait_after(&report), Duration::from_secs(6 * 3600));

        // A 10 minute cert issued now falls due in 6m40s — long before the
        // 6h interval, so we must wake for it.
        report.next_due = Some(Duration::from_secs(400));
        assert_eq!(renewer.wait_after(&report), Duration::from_secs(400));

        // A long-lived cert doesn't stretch the interval past its setting.
        report.next_due = Some(Duration::from_secs(700 * 86400));
        assert_eq!(renewer.wait_after(&report), Duration::from_secs(6 * 3600));

        // Due now (stuck awaiting approval) must not become a spin.
        report.next_due = Some(Duration::ZERO);
        assert_eq!(renewer.wait_after(&report), MIN_SCAN_INTERVAL);
    }

    fn addr(n: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, n], 4565))
    }

    fn info(
        ca_addr: Option<SocketAddr>,
        peers: Vec<SocketAddr>,
    ) -> crate::admin_proto::GetInfoResponse {
        crate::admin_proto::GetInfoResponse {
            domain: "example.com".into(),
            ca_addr,
            resolver: None,
            peers,
        }
    }

    /// The second seeding must not re-contact what the first already
    /// tried: the install-record walk and the mDNS walk overlap whenever
    /// the recorded server is also advertising.
    #[tokio::test]
    async fn a_later_seeding_does_not_revisit() {
        let mut asked: Vec<SocketAddr> = Vec::new();
        let mut visited: Vec<SocketAddr> = Vec::new();
        let mut queue = vec![addr(1)];
        let found = walk(&mut queue, &mut visited, |a| {
            asked.push(a);
            std::future::ready(Ok(info(None, vec![])))
        })
        .await;
        assert_eq!(found, None);
        let mut queue = vec![addr(2), addr(1)];
        let found = walk(&mut queue, &mut visited, |a| {
            asked.push(a);
            std::future::ready(Ok(if a == addr(2) {
                info(Some(addr(9)), vec![])
            } else {
                info(None, vec![])
            }))
        })
        .await;
        assert_eq!(found, Some(addr(9)));
        assert_eq!(asked, vec![addr(1), addr(2)]);
    }

    /// A peer graph with a cycle must terminate, and a walk that hits the
    /// cap must stop rather than drain the rest of the queue.
    #[tokio::test]
    async fn the_walk_terminates() {
        let mut asked = 0usize;
        let mut visited: Vec<SocketAddr> = Vec::new();
        let mut queue = vec![addr(1)];
        let found = walk(&mut queue, &mut visited, |a| {
            asked += 1;
            let next = if a == addr(1) { addr(2) } else { addr(1) };
            std::future::ready(Ok(info(None, vec![next])))
        })
        .await;
        assert_eq!(found, None);
        assert_eq!(asked, 2);

        let mut asked = 0usize;
        let mut visited: Vec<SocketAddr> = Vec::new();
        let mut queue: Vec<SocketAddr> = (1..=200u8).map(addr).collect();
        let found = walk(&mut queue, &mut visited, |_| {
            asked += 1;
            std::future::ready(Ok(info(None, vec![])))
        })
        .await;
        assert_eq!(found, None);
        assert_eq!(asked, MAX_WALK);
        assert!(!queue.is_empty());
    }

    /// An admin server that reports the CA on an unspecified address means
    /// "me, on this port" — the walk has to substitute the peer's own IP.
    #[tokio::test]
    async fn an_unspecified_ca_addr_resolves_to_the_reporting_peer() {
        let mut visited: Vec<SocketAddr> = Vec::new();
        let mut queue = vec![addr(7)];
        let found = walk(&mut queue, &mut visited, |_| {
            std::future::ready(Ok(info(
                Some(SocketAddr::from(([0, 0, 0, 0], 4565))),
                vec![],
            )))
        })
        .await;
        assert_eq!(found, Some(addr(7)));
    }
}
