use super::{
    AUTORENEW_ADMIN, AUTORENEW_POLL, CONN_TIMEOUT, MAX_CONCURRENT_SIGNS, MAX_CONNECTIONS,
    Server,
    auth::{PreparedAdminAuthentication, PreparedServerUnlock, run_signing},
    issuance::{PushPlan, leaf_serial, push_registrations},
    queue::autorenew_sweep,
    request::{PeerIdent, cert_signed_by, serve_request},
    topology::{local_resolver_data, reconcile_controller_state_on_start},
};
use crate::{
    admin_proto::{NodeKind, RegisterRequest},
    admin_server_config,
    config_lock::ConfigDirLock,
    discovery, transport,
};
use anyhow::{Context, Result, anyhow, bail};
use log::{debug, info, warn};
use rustls::{
    RootCertStore, ServerConfig as RustlsServerConfig,
    server::{ServerSessionMemoryCache, WebPkiClientVerifier},
};
use rustls_pki_types::CertificateDer;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, UnixListener},
    sync::{Semaphore, mpsc as tokio_mpsc},
};
use tokio_rustls::TlsAcceptor;
use triomphe::Arc as TArc;

/// Build a [`RootCertStore`] from a PEM trust bundle.
pub fn load_roots(trusted_pem: &[u8]) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for der in rustls_pemfile::certs(&mut std::io::Cursor::new(trusted_pem)) {
        roots.add(der.context("parsing trust bundle")?).context("adding trust anchor")?;
    }
    if roots.is_empty() {
        bail!("no certificates in trust bundle");
    }
    Ok(roots)
}

const MAP_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// On a non-CA admin server, keep the cached trust domain map current and keep
/// our own entry registered with the CA. The CA owns the map; we are a
/// read-replica — if the CA is unreachable we keep serving the last copy
/// we cached, and the re-register self-heals a push lost while it was down.
async fn spawn_map_refresh(state: &Arc<Server>) {
    if state.read(move |state| state.cfg.roles.ca.is_some()).await {
        return; // the CA owns the map — nothing to refresh
    }
    let weak = Arc::downgrade(state);
    tokio::spawn(async move {
        loop {
            let Some(state) = weak.upgrade() else { break };
            let client = match state.outbound_client().await {
                Ok(client) => client,
                Err(e) => {
                    warn!("admin-server: loading outbound identity failed: {e:#}");
                    drop(state);
                    tokio::time::sleep(MAP_REFRESH_INTERVAL).await;
                    continue;
                }
            };
            let cfg = state.read(move |state| state.cfg.clone()).await;
            let (_, resolver) = local_resolver_data(&cfg).await;
            let ca_addr = cfg.ca_addr;
            let req = RegisterRequest { addr: cfg.listen, resolver };
            let Some(ca_addr) = ca_addr else {
                drop(state);
                tokio::time::sleep(MAP_REFRESH_INTERVAL).await;
                continue;
            };
            // Self-heal: (re)register our own facts. Idempotent at the CA.
            if let Err(e) =
                transport::register(&client, ca_addr, state.home_ca_der.clone(), &req)
                    .await
            {
                warn!(
                    "admin-server: registering with the CA {ca_addr} failed (will retry): {e:#}"
                );
            }
            // Refresh the cache: cheap version check, full pull only when changed.
            match transport::get_map_version_from_controller(
                &state.pki_client,
                ca_addr,
                state.home_ca_der.clone(),
                NodeKind::AdminServer,
            )
            .await
            {
                Ok(v) => {
                    let stale = state.read(move |state| state.map.version != v).await;
                    if stale {
                        match transport::get_map_from_controller(
                            &state.pki_client,
                            ca_addr,
                            state.home_ca_der.clone(),
                            NodeKind::AdminServer,
                        )
                        .await
                        {
                            Ok(map) => state.write(move |state| state.map = map).await,
                            Err(e) => warn!(
                                "admin-server: pulling the trust domain map from {ca_addr} failed \
                                 (serving the cached copy): {e:#}"
                            ),
                        }
                    }
                }
                Err(e) => warn!(
                    "admin-server: map version check against {ca_addr} failed \
                     (serving the cached copy): {e:#}"
                ),
            }
            drop(state);
            tokio::time::sleep(MAP_REFRESH_INTERVAL).await;
        }
    });
}

/// Approve pending verified renewals in-process. The vault is copied only
/// when work exists, and Argon2 runs on the bounded blocking pool. Capturing
/// a `Weak` makes dropping the server stop the loop.
async fn spawn_autorenew(state: &Arc<Server>, signs: Arc<Semaphore>) {
    if !state.has_ca().await {
        return;
    }
    // Enable autorenew only if the CA currently holds a signing credential.
    // The sweep re-reads it each iteration (see below), so this is just the
    // initial gate; `None` ⇒ a read-only CA with nothing to auto-approve.
    if state
        .read(move |state| {
            state.ca.as_ref().expect("CA role held").autorenew_pw.is_none()
        })
        .await
    {
        return;
    }
    info!(
        "admin-server: autorenew enabled (approving verified renewals as {AUTORENEW_ADMIN:?})"
    );
    let weak = Arc::downgrade(state);
    tokio::spawn(async move {
        loop {
            let Some(state) = weak.upgrade() else { break };
            // Re-read the box credential each sweep so a live rotation over
            // the local control socket (`recovery rotate` / `auto-approve`)
            // is honored without restarting the daemon. A transient `None`
            // (mid-rotation, or a retired CA) just skips this sweep.
            let captured = state
                .read_async(async move |state| {
                    let ca = state.ca.as_ref().expect("CA role held");
                    if !ca
                        .store
                        .pending()
                        .await?
                        .iter()
                        .any(|request| request.renewal_of.is_some())
                    {
                        return Ok::<_, anyhow::Error>(None);
                    }
                    Ok(Some((
                        ca.autorenew_pw.clone().context("no autorenew credential")?,
                        ca.vault.snapshot()?,
                    )))
                })
                .await;
            let Ok(Some((pw, snapshot))) = captured else {
                tokio::time::sleep(AUTORENEW_POLL).await;
                continue;
            };
            let worker_pw = pw.clone();
            let proofs = run_signing(&signs, move || {
                let authenticated = snapshot
                    .authenticate(AUTORENEW_ADMIN, &worker_pw)
                    .context("authenticating the autorenew slot")?;
                let unlocked = snapshot
                    .unlock(&worker_pw)
                    .map(TArc::new)
                    .context("unlocking the autorenew slot")?;
                Ok::<_, anyhow::Error>((authenticated, unlocked))
            })
            .await;
            match proofs {
                Ok(Ok((authenticated, unlocked))) => {
                    let authentication =
                        PreparedAdminAuthentication::Password(Ok(authenticated));
                    let prepared_server_unlock =
                        PreparedServerUnlock::from_result(Ok(unlocked));
                    state
                        .write_async(async move |state| {
                            autorenew_sweep(
                                state.ca.as_mut().expect("CA role held"),
                                &pw,
                                &authentication,
                                &prepared_server_unlock,
                            )
                            .await;
                        })
                        .await;
                }
                Ok(Err(e)) => warn!("autorenew: preparing sweep credentials: {e:#}"),
                Err(e) => warn!("autorenew: credential task panicked: {e:#}"),
            }
            tokio::time::sleep(AUTORENEW_POLL).await;
        }
    });
}

fn build_server_config(
    cert_pem: &[u8],
    key_pem: &[u8],
    roots: RootCertStore,
    crl_pem: Option<&[u8]>,
) -> Result<RustlsServerConfig> {
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem))
            .collect::<std::result::Result<_, _>>()
            .context("parsing serving certificate chain")?;
    if certs.is_empty() {
        bail!("serving certificate PEM contained no certificates");
    }
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(key_pem))
        .context("parsing serving private key")?
        .ok_or_else(|| anyhow!("no private key found in serving key PEM"))?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone());
    // Client certs are *optional*: join clients have none yet; peer admin
    // servers authenticate with theirs (verified against the CA bundle) to
    // authorize server-to-server requests. When we hold a CRL, a *presented*
    // peer cert is additionally checked for revocation, so a revoked but
    // unexpired serving cert is refused at the handshake — closing the peer
    // gate (Register/Deregister/ApplyPermsEdit/…) against decommissioned or
    // compromised admin servers. Unknown status stays permitted: absence of a
    // CRL must not lock the plane out, presence on one must. (The CRL is read
    // when the acceptor is built; a revocation takes effect on the next
    // admin-server restart, the same coarseness as a serving-cert rotation.)
    let crls: Vec<rustls_pki_types::CertificateRevocationListDer<'static>> = match crl_pem
    {
        Some(pem) => rustls_pemfile::crls(&mut std::io::Cursor::new(pem))
            .collect::<std::result::Result<_, _>>()
            .context("parsing CRL")?,
        None => Vec::new(),
    };
    let verifier = if crls.is_empty() {
        builder
            .allow_unauthenticated()
            .build()
            .context("building client cert verifier")?
    } else {
        builder
            .with_crls(crls)
            .allow_unknown_revocation_status()
            .allow_unauthenticated()
            .build()
            .context("building client cert verifier with CRL")?
    };
    let mut config = RustlsServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("selecting TLS versions")?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .context("building TLS server config")?;
    config.session_storage = ServerSessionMemoryCache::new(256);
    config.send_tls13_tickets = 2;
    config.max_early_data_size = 0;
    Ok(config)
}

/// The CRL this admin server enforces on inbound peer certs: the CA's own
/// authoritative `crl.pem` when we hold the CA, else the copy the renewal
/// daemon places beside our trust bundle (the convention netidx's own
/// acceptor watches). Absent ⇒ `None` — a missing CRL must never lock the
/// admin plane out; it just means no revocation is enforced yet.
async fn serving_crl_path(state: &Server) -> PathBuf {
    state
        .read(move |state| match state.ca.as_ref() {
            Some(ca) => ca.store.crl_path(),
            None => state.cfg.trusted.with_file_name("crl.pem"),
        })
        .await
}

async fn load_serving_crl(state: &Server) -> Option<Vec<u8>> {
    let path = serving_crl_path(state).await;
    match tokio::fs::read(&path).await {
        Ok(bytes) => Some(bytes),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            warn!("admin-server: reading CRL {path:?}: {e:#} (revocation NOT enforced)");
            None
        }
    }
}

/// Read the serving cert + key from disk, decrypting a TPM-sealed key in
/// memory. Shared by startup and the live reload below, so a renewed serving
/// cert (and its possibly TPM-sealed key) is picked up identically either way.
pub(super) async fn load_serving_keypair(
    serving_cert: &Path,
    serving_key: &Path,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let cert_pem = tokio::fs::read(serving_cert)
        .await
        .with_context(|| format!("reading serving cert {}", serving_cert.display()))?;
    let key_pem = tokio::fs::read(serving_key)
        .await
        .with_context(|| format!("reading serving key {}", serving_key.display()))?;
    let key_pem = {
        let sidecar = crate::tls::sealed_sidecar(serving_key);
        if tokio::fs::try_exists(&sidecar).await? {
            let blob = tokio::fs::read(&sidecar)
                .await
                .with_context(|| format!("reading sealed password {sidecar:?}"))?;
            tokio::task::spawn_blocking(move || {
                let pw = netidx_tpm::unseal(&blob).with_context(|| {
                    format!(
                        "unsealing {sidecar:?} — if this host's TPM was cleared or \
                         the board was replaced, re-enroll this admin server"
                    )
                })?;
                let pw =
                    std::str::from_utf8(&pw).context("sealed password is not utf8")?;
                let pem =
                    std::str::from_utf8(&key_pem).context("serving key is not utf8")?;
                Ok::<_, anyhow::Error>(
                    netidx::tls::decrypt_private_key(pem, pw)
                        .context("decrypting the serving key")?
                        .as_bytes()
                        .to_vec(),
                )
            })
            .await
            .context("serving-key decryption task panicked")??
        } else {
            key_pem
        }
    };
    Ok((cert_pem, key_pem))
}

/// Build the inbound TLS acceptor from a serving cert/key and the current
/// CRL. Used at startup and on every live reload.
async fn build_serving_acceptor(
    state: &Server,
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<TlsAcceptor> {
    let crl = load_serving_crl(state).await;
    Ok(TlsAcceptor::from(Arc::new(build_server_config(
        cert_pem,
        key_pem,
        state.roots.clone(),
        crl.as_deref(),
    )?)))
}

/// How often the daemon re-stats its serving cert + CRL on disk. Both were
/// previously read once at startup, so a serving cert the renewal daemon
/// renewed *on disk* sat unused until the running daemon's in-memory copy
/// expired — taking the admin plane down trust domain-wide. Now a long-running
/// daemon picks up a renewal (or a fresh CRL / revocation) within one poll,
/// no restart needed. Cheap: a stat, and a rebuild only when an mtime moves.
const SERVING_RELOAD_POLL: Duration = Duration::from_secs(30);

/// Watch the serving cert + CRL files and swap a freshly-built acceptor into
/// `acceptor` when either changes. New connections pick up the new acceptor;
/// in-flight handshakes keep the one they started with.
async fn spawn_serving_reload(
    state: &Arc<Server>,
    acceptor: tokio_mpsc::UnboundedSender<TlsAcceptor>,
) {
    let (cert_path, key_path) = state
        .read(move |state| {
            (state.cfg.serving_cert.clone(), state.cfg.serving_key.clone())
        })
        .await;
    let crl_path = serving_crl_path(state).await;
    let weak = Arc::downgrade(state);
    // Start from a sentinel and check before the first sleep, so the first
    // pass always rebuilds from the *current* on-disk cert. The startup
    // acceptor was built from content read earlier in `serve`, before this
    // task captures its baseline; a renewal landing in that gap (which spans
    // bind + mDNS + pending-push I/O) would otherwise be missed forever.
    let mut last: (Option<SystemTime>, Option<SystemTime>) = (None, None);
    tokio::spawn(async move {
        loop {
            let Some(state) = weak.upgrade() else { break };
            let now = (
                tokio::fs::metadata(&cert_path).await.and_then(|m| m.modified()).ok(),
                tokio::fs::metadata(&crl_path).await.and_then(|m| m.modified()).ok(),
            );
            if now != last {
                let rebuilt = match load_serving_keypair(&cert_path, &key_path).await {
                    Ok((cert, key)) => build_serving_acceptor(&state, &cert, &key).await,
                    Err(e) => Err(e),
                };
                match rebuilt {
                    Ok(acc) => {
                        if acceptor.send(acc).is_err() {
                            break;
                        }
                        last = now;
                        info!(
                            "admin-server: reloaded serving cert / CRL from disk \
                             (renewal or revocation installed, no restart)"
                        );
                    }
                    Err(e) => warn!(
                        "admin-server: serving cert/CRL reload failed, keeping current: {e:#}"
                    ),
                }
            }
            drop(state);
            tokio::time::sleep(SERVING_RELOAD_POLL).await;
        }
    });
}

async fn handle_conn(
    acceptor: &TlsAcceptor,
    tcp: TcpStream,
    peer: SocketAddr,
    state: &Arc<Server>,
    signs: Arc<Semaphore>,
) -> Result<()> {
    let mut tls = acceptor.accept(tcp).await.context("TLS handshake")?;
    tls.flush().await.context("flushing TLS session tickets")?;
    // If the client presented a cert, the verifier already validated it
    // against our roots. What remains is identifying *who*: the SAN
    // authorizes server-to-server requests (the reserved name) and
    // marks renewals (SAN == requested name); the serial lets the
    // enqueue path confirm the presented cert is the live one in our
    // own index — stronger than a CRL check, since the index is the
    // source of truth on the CA host.
    let peer_ident: Option<PeerIdent> = {
        let (_io, conn) = tls.get_ref();
        conn.peer_certificates().and_then(|certs| certs.first()).and_then(|leaf| {
            let san = crate::tls::first_dns_san_from_der(leaf.as_ref())?;
            let spki_fp = crate::fingerprint::Fingerprint::of_cert_der(leaf.as_ref())
                .ok()
                .map(|f| f.text());
            let admin = crate::tls::admin_cert_identity_from_der(leaf.as_ref()).ok();
            let home_ca = cert_signed_by(leaf.as_ref(), state.home_ca_der.as_ref());
            Some(PeerIdent {
                san,
                serial: leaf_serial(leaf.as_ref()),
                spki_fp,
                admin,
                home_ca,
            })
        })
    };
    serve_request(tls, peer, peer_ident, false, state, signs).await
}

/// Run the admin server described by the config at `cfg_path` until the
/// process is killed.
pub async fn serve(cfg_path: PathBuf) -> Result<()> {
    let config_lock = ConfigDirLock::acquire_for_file_async(&cfg_path).await?;
    let cfg = admin_server_config::load_async(&cfg_path).await?;
    let offline_ca_lock_guard = cfg
        .roles
        .ca
        .as_ref()
        .map(|role| config_lock.ca_alias_root(&role.dir))
        .transpose()?
        .flatten()
        .map(ConfigDirLock::acquire)
        .transpose()?;
    // A TPM-sealed serving key has its password in `<key>.tpm`, sealed to
    // this machine; `load_serving_keypair` unseals + decrypts in memory.
    // Failure is a hard error (a admin server silently down means no discovery
    // and no renewals for the whole trust domain).
    let (serving_cert_pem, serving_key_pem) =
        load_serving_keypair(&cfg.serving_cert, &cfg.serving_key).await?;
    let listen = cfg.listen;
    let mdns = cfg.mdns;
    let state = Server::new(
        config_lock,
        offline_ca_lock_guard,
        cfg,
        Some(cfg_path),
        serving_cert_pem,
        serving_key_pem,
    )
    .await?;
    let acceptor =
        build_serving_acceptor(&state, &state.serving_cert_pem, &state.serving_key_pem)
            .await?;
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding admin server to {listen}"))?;
    info!("admin-server: listening on {listen}");
    // Advertise over mDNS. The beacon is a hint only — fingerprint +
    // roles ride in TXT purely for pre-connect display/grouping.
    let _advert = if mdns {
        let domain = state.read(move |state| state.cfg.domain.clone()).await;
        let fp_short = ca_fingerprint_short(&state.serving_cert_pem)?;
        match discovery::advertise(listen, &domain, state.roles().await, &fp_short) {
            Ok(ad) => Some(ad),
            Err(e) => {
                warn!("admin-server: mDNS advertisement failed (continuing): {e:#}");
                None
            }
        }
    } else {
        None
    };
    let signs = Arc::new(Semaphore::new(MAX_CONCURRENT_SIGNS));
    // Reconcile the current CRL on every controller start. This is especially
    // important after offline disaster recovery: the superseded controller
    // certificate was revoked before the replacement daemon existed to do the
    // ordinary immediate fanout. Startup is the first safe moment to push it.
    if state.has_ca().await {
        let state = state.clone();
        let signs = signs.clone();
        tokio::spawn(
            async move { reconcile_controller_state_on_start(state, signs).await },
        );
    }
    serve_on(listener, acceptor, state, signs).await
}

/// Short fingerprint of the CA cert at the end of the serving chain —
/// what the mDNS beacon carries as a display hint.
fn ca_fingerprint_short(serving_cert_pem: &[u8]) -> Result<String> {
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(serving_cert_pem))
            .collect::<std::result::Result<_, _>>()
            .context("parsing serving certificate chain")?;
    let ca = certs.last().ok_or_else(|| anyhow!("serving chain is empty"))?;
    Ok(crate::fingerprint::Fingerprint::of_cert_der(ca.as_ref())
        .context("fingerprinting the CA certificate")?
        .short())
}

/// The accept loop, split out so tests can drive it on an ephemeral
/// port they bound themselves.
async fn serve_on(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    state: Arc<Server>,
    signs: Arc<Semaphore>,
) -> Result<()> {
    // If we hold the CA, re-run any id-map pushes that committed an
    // issuance but never confirmed the registration (a crash between the
    // sign and the push). The records carry their groups, so this needs no
    // admin. Best-effort: a failure stays in the recovery set and is
    // retried on the enrollee's next poll. (No issuance reconcile is
    // needed — each issuance commits as one atomic record.)
    if state.has_ca().await {
        match state
            .read_async(async move |state| {
                state.ca.as_ref().expect("CA role held").store.pending_pushes().await
            })
            .await
        {
            Ok(pending) => {
                for r in pending {
                    let plan = PushPlan { id: r.req.id, name: r.name, groups: r.groups };
                    let _ = push_registrations(&state, &plan).await;
                }
            }
            Err(e) => warn!("admin-server: listing pending id-map pushes: {e:#}"),
        }
    }
    // If the CA role names an autorenew keytab, approve verified renewals
    // in-process from here on (a no-op when it doesn't).
    spawn_map_refresh(&state).await;
    let conns = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    spawn_autorenew(&state, signs.clone()).await;
    spawn_local_control(&state, signs.clone()).await;
    let (acceptor_tx, mut acceptor_rx) = tokio_mpsc::unbounded_channel();
    spawn_serving_reload(&state, acceptor_tx).await;
    let mut acceptor = acceptor;
    loop {
        let accepted = tokio::select! {
            biased;
            Some(reloaded) = acceptor_rx.recv() => {
                acceptor = reloaded;
                continue;
            }
            accepted = listener.accept() => accepted,
        };
        let (tcp, peer) = match accepted {
            Ok(x) => x,
            Err(e) => {
                warn!("admin-server: accept failed: {e:#}");
                continue;
            }
        };
        // Gate the connection count at the door, so we don't even spawn
        // a task for one we'd immediately have to drop.
        let conn_permit = match conns.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                warn!("admin-server: at connection limit, dropping {peer}");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        let state = state.clone();
        let signs = signs.clone();
        tokio::spawn(async move {
            let _conn_permit = conn_permit; // released when the task ends
            // Bound the whole connection so a stalled peer can't hold a
            // connection slot (the handshake and every read are
            // otherwise deadline-less). The sign permit is *not* tied to
            // this cancellable future — see `handle_conn`.
            match tokio::time::timeout(
                CONN_TIMEOUT,
                handle_conn(&acceptor, tcp, peer, &state, signs),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => debug!("admin-server: connection from {peer} ended: {e:#}"),
                Err(_) => debug!("admin-server: connection from {peer} timed out"),
            }
        });
    }
}

/// `admin.sock` beside the admin-server config file. The daemon binds it and
/// the local `ca` CLI connects to it; both derive it from the same config
/// path, so they always agree on the location.
pub(crate) fn local_socket_path(cfg_path: &Path) -> PathBuf {
    cfg_path.parent().unwrap_or_else(|| Path::new(".")).join("admin.sock")
}

/// Bind the local control socket `0600` so only the daemon's uid / root can
/// reach it (defence in depth on top of the per-connection `SO_PEERCRED`
/// check). We hold the installation guard, so any socket file here is stale from a
/// prior run and safe to replace.
async fn bind_local_control(
    config_lock: &ConfigDirLock,
    path: &Path,
) -> Result<UnixListener> {
    use std::os::unix::fs::PermissionsExt;
    let path = config_lock.require_contained(path)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let _ = tokio::fs::remove_file(&path).await;
    let listener = UnixListener::bind(&path)
        .with_context(|| format!("binding {}", path.display()))?;
    tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        .await
        .with_context(|| format!("setting 0600 on {}", path.display()))?;
    Ok(listener)
}

/// Allow a local control peer only if it is root or the daemon's own euid —
/// the kernel-supplied `SO_PEERCRED`, which a client cannot forge. This is
/// what makes a local request safe to treat as a superuser: the OS attests
/// the caller already has on-box authority.
fn local_peer_allowed(stream: &tokio::net::UnixStream) -> bool {
    match stream.peer_cred() {
        Ok(cred) => cred.uid() == 0 || cred.uid() == nix::unistd::geteuid().as_raw(),
        Err(e) => {
            debug!("admin-server: cannot read local control peer creds: {e}");
            false
        }
    }
}

/// If this daemon has a config path (always, outside tests), bind the local
/// control socket and serve admin / recovery / service requests from it.
/// Best-effort: a bind failure is logged and the daemon keeps serving the
/// network admin plane. Local connections share the sign semaphore (the
/// Argon2 budget) but not the trust domain connection limit — the socket is a
/// privileged, peer-cred-gated local channel.
async fn spawn_local_control(state: &Arc<Server>, signs: Arc<Semaphore>) {
    let Some(cfg_path) = state.cfg_path.clone() else { return };
    let path = local_socket_path(&cfg_path);
    let listener = match bind_local_control(state.config_lock(), &path).await {
        Ok(l) => l,
        Err(e) => {
            warn!(
                "admin-server: local control socket disabled ({}): {e:#}",
                path.display()
            );
            return;
        }
    };
    info!("admin-server: local control socket at {}", path.display());
    let weak = Arc::downgrade(state);
    tokio::spawn(async move {
        loop {
            let Some(state) = weak.upgrade() else { break };
            let (stream, _addr) = match listener.accept().await {
                Ok(x) => x,
                Err(e) => {
                    warn!("admin-server: local control accept failed: {e:#}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            if !local_peer_allowed(&stream) {
                debug!(
                    "admin-server: refusing local control peer (uid not root / daemon)"
                );
                continue;
            }
            let signs = signs.clone();
            tokio::spawn(async move {
                match tokio::time::timeout(
                    CONN_TIMEOUT,
                    handle_local_conn(stream, &state, signs),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        debug!("admin-server: local control connection ended: {e:#}")
                    }
                    Err(_) => debug!("admin-server: local control connection timed out"),
                }
            });
        }
    });
}

/// Serve one local control connection. No TLS, no peer cert — `local = true`
/// authorizes admin-management ops as a superuser (the `SO_PEERCRED` check at
/// accept is the gate). The synthetic `peer` address is only ever recorded
/// by enqueue / delegation handlers, which this socket isn't used for.
async fn handle_local_conn(
    stream: tokio::net::UnixStream,
    state: &Arc<Server>,
    signs: Arc<Semaphore>,
) -> Result<()> {
    let peer = SocketAddr::from(([0, 0, 0, 0], 0));
    serve_request(stream, peer, None, true, state, signs).await
}
