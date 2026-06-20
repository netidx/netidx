//! Conf server: the per-host daemon behind discovery-driven setup. It
//! answers [`Request::GetInfo`] with this host's local facts + known
//! peers, and — on the host holding the CA — turns a join client's
//! [`Request::Sign`] into a signed cert and a [`Request::Enroll`] into
//! a new conf server's reserved-SAN serving cert. After a successful
//! sign it pushes id-map registrations to every id-map-role peer.
//!
//! The request-handling cores ([`handle_sign_request`],
//! [`handle_enroll_request`], [`handle_add_identity`]) are pure of TLS
//! and directly testable: the sign/enroll paths unlock the vault with
//! the admin password, enforce that admin's per-slot issuance
//! [`Policy`], sign via the existing [`Ca::sign_request`], and append
//! an audit line. The TLS accept loop is a thin shell over them.
//!
//! Unix-only — the signer is openssl-backed.

use crate::{
    ca::{Ca, SanEntry},
    ca_store, ca_vault, conf_client,
    conf_proto::{
        self, AddIdentityRequest, AddIdentityResponse, ApproveRequest, ApproveResponse,
        ClientHello, DenyRequest, DenyResponse, EnqueueRequest, EnqueueResponse,
        EnrollRequest, GetCrlResponse, GetInfoResponse, InfoAuth, IssuedEntry,
        ListIssuedRequest, ListIssuedResponse, ListQueueRequest, ListQueueResponse,
        PollResponse, QueueEntry, Request, ResolverAddr, RevokeRequest, RevokeResponse,
        Role, ServerHello, SignRequest, SignResponse, PROTOCOL_VERSION, SERVING_SAN,
    },
    conf_server_config::ConfServerConfig,
    discovery, id_map,
};
use anyhow::{anyhow, bail, Context, Result};
use globset::Glob;
use log::{debug, info, warn};
use parking_lot::Mutex;
use rustls::{server::WebPkiClientVerifier, RootCertStore, ServerConfig as RustlsServerConfig};
use rustls_pki_types::CertificateDer;
use std::{
    fs::OpenOptions,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_rustls::TlsAcceptor;
use zeroize::Zeroizing;

/// Max simultaneous connections. These are cheap (a TLS handshake and a
/// few small messages), so this can be generous — it just bounds socket
/// / task fan-out.
const MAX_CONNECTIONS: usize = 768;

/// Max simultaneous *signs*. Each sign runs an Argon2id derivation
/// (~64 MiB) to unlock the vault, so this — not `MAX_CONNECTIONS` — is
/// what bounds the memory a flood of (even wrong-password) requests can
/// pin: roughly `MAX_CONCURRENT_SIGNS × 64 MiB`. Tune for the CA box's
/// RAM (e.g. 64 ≈ 4 GiB). Signing runs on `spawn_blocking`, so this also
/// keeps Argon2/openssl off the async worker threads.
const MAX_CONCURRENT_SIGNS: usize = 64;

/// Upper bound on a single connection's whole lifetime (handshake +
/// request + sign + push + response). Without it a client that connects
/// and stalls holds a connection slot indefinitely.
const CONN_TIMEOUT: Duration = Duration::from_secs(30);

/// Budget for the whole post-sign id-map push fan-out. Bounded well
/// under [`CONN_TIMEOUT`] so a dead id-map host degrades a join to a
/// warning instead of timing the connection out.
const PUSH_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the push fan-out browses mDNS for id-map hosts not in the
/// configured peer list.
const PUSH_BROWSE_TIMEOUT: Duration = Duration::from_secs(2);

/// The dedicated auto-renewal admin: a vault slot with an empty issuance
/// policy whose only over-the-wire power is approving verified renewals.
/// When [`CaRole::autorenew`](crate::conf_server_config::CaRole) names its
/// keytab, the daemon authenticates as this slot to approve renewals
/// in-process — the same narrow principal the separate `conf ca auto-approve`
/// process used to be, now without the extra process.
pub const AUTORENEW_ADMIN: &str = "autorenew";

/// How often the in-process autorenew approver scans the queue. Renewals
/// are submitted by the per-host `renewd` on a multi-hour cadence and are
/// not latency-sensitive, so a slow poll is plenty.
const AUTORENEW_POLL: Duration = Duration::from_secs(60);

/// The daemon's in-memory CA issuance state. The daemon is the sole
/// owner of the CA (a single exclusive flock at startup), so this mutex
/// is the only mutual exclusion issuance needs: it is held across the
/// one-live scan, serial allocation, sign, and atomic record commit, and
/// across the approve/deny terminal transition (so exactly one lands).
pub struct CaIssuer {
    /// The next X.509 serial to mint, seeded at startup from
    /// `max(ca_store::max_serial, CA-cert-serial) + 1`.
    next_serial: u64,
}

impl CaIssuer {
    fn alloc(&mut self) -> u64 {
        let s = self.next_serial;
        self.next_serial += 1;
        s
    }
}

/// Shared state of a running conf server.
pub struct Server {
    /// The config, mutable because [`Request::Enroll`] appends the
    /// enrollee to `peers`.
    cfg: Mutex<ConfServerConfig>,
    /// Where to persist peer updates. `None` (tests) keeps them
    /// in-memory only.
    cfg_path: Option<PathBuf>,
    /// The CA directory, if this host holds the CA role.
    ca_dir: Option<PathBuf>,
    /// In-memory CA issuance state (serial counter + the issuance lock).
    /// Only meaningful when `ca_dir` is `Some`.
    ca: Mutex<CaIssuer>,
    /// The exclusive flock on `<ca-dir>/ca.lock`, held for the daemon's
    /// lifetime so exactly one daemon owns the CA. `None` when roleless.
    _ca_lock: Option<std::fs::File>,
    /// Serving chain + key, doubling as the client identity for
    /// outbound server-to-server pushes.
    serving_cert_pem: Vec<u8>,
    serving_key_pem: Vec<u8>,
    /// Trust anchors (the CA bundle) for verifying peers — both their
    /// serving certs outbound and their client certs inbound.
    roots: RootCertStore,
    /// Serializes read-modify-write cycles on the local id-map file.
    id_map_lock: Mutex<()>,
}

impl Server {
    pub fn new(
        cfg: ConfServerConfig,
        cfg_path: Option<PathBuf>,
        serving_cert_pem: Vec<u8>,
        serving_key_pem: Vec<u8>,
    ) -> Result<Arc<Self>> {
        let trusted = std::fs::read(&cfg.trusted)
            .with_context(|| format!("reading trust bundle {}", cfg.trusted.display()))?;
        let mut roots = RootCertStore::empty();
        for der in rustls_pemfile::certs(&mut std::io::Cursor::new(&trusted)) {
            roots.add(der.context("parsing trust bundle")?).context("adding trust anchor")?;
        }
        if roots.is_empty() {
            bail!("trust bundle {} contains no certificates", cfg.trusted.display());
        }
        // If we hold the CA, take the singleton flock and seed the serial
        // counter before serving — a second daemon for the same CA fails
        // here, and the counter starts beyond every serial ever issued.
        let ca_dir = cfg.roles.ca.as_ref().map(|r| r.dir.clone());
        let (ca_lock, next_serial) = match &ca_dir {
            Some(dir) => {
                let lock = ca_store::lock_ca_exclusive(dir)?;
                (Some(lock), ca_store::next_serial(dir)?)
            }
            None => (None, 0),
        };
        Ok(Arc::new(Server {
            cfg: Mutex::new(cfg),
            cfg_path,
            ca_dir,
            ca: Mutex::new(CaIssuer { next_serial }),
            _ca_lock: ca_lock,
            serving_cert_pem,
            serving_key_pem,
            roots,
            id_map_lock: Mutex::new(()),
        }))
    }

    /// The CA directory, if this host holds the CA role.
    pub fn ca_dir(&self) -> Option<&Path> {
        self.ca_dir.as_deref()
    }

    fn roles(&self) -> Vec<Role> {
        let cfg = self.cfg.lock();
        let mut roles = Vec::new();
        if cfg.roles.ca.is_some() {
            roles.push(Role::Ca);
        }
        if cfg.roles.resolver.is_some() {
            roles.push(Role::Resolver);
        }
        if cfg.roles.id_map.is_some() {
            roles.push(Role::IdMap);
        }
        roles
    }
}

/// Run the conf server described by the config at `cfg_path` until the
/// process is killed.
pub async fn serve(cfg_path: PathBuf) -> Result<()> {
    let cfg = ConfServerConfig::load(&cfg_path)?;
    let serving_cert_pem = std::fs::read(&cfg.serving_cert)
        .with_context(|| format!("reading serving cert {}", cfg.serving_cert.display()))?;
    let serving_key_pem = std::fs::read(&cfg.serving_key)
        .with_context(|| format!("reading serving key {}", cfg.serving_key.display()))?;
    // A TPM-sealed serving key: `<key>.tpm` holds the password, sealed
    // to this machine. Unseal + decrypt here, in memory; failure is a
    // hard error naming the fix (a conf server silently down means no
    // discovery and no renewals for the whole network).
    let serving_key_pem = {
        let sidecar = crate::tls::sealed_sidecar(&cfg.serving_key);
        if sidecar.exists() {
            let blob = std::fs::read(&sidecar)
                .with_context(|| format!("reading sealed password {sidecar:?}"))?;
            let pw = netidx_tpm::unseal(&blob).with_context(|| {
                format!(
                    "unsealing {sidecar:?} — if this host's TPM was cleared or \
                     the board was replaced, re-enroll this conf server"
                )
            })?;
            let pw = std::str::from_utf8(&pw).context("sealed password is not utf8")?;
            let pem = std::str::from_utf8(&serving_key_pem)
                .context("serving key is not utf8")?;
            netidx::tls::decrypt_private_key(pem, pw)
                .context("decrypting the serving key")?
                .as_bytes()
                .to_vec()
        } else {
            serving_key_pem
        }
    };
    let listen = cfg.listen;
    let mdns = cfg.mdns;
    let state = Server::new(cfg, Some(cfg_path), serving_cert_pem, serving_key_pem)?;
    let acceptor = TlsAcceptor::from(Arc::new(build_server_config(
        &state.serving_cert_pem,
        &state.serving_key_pem,
        state.roots.clone(),
    )?));
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding conf server to {listen}"))?;
    info!("conf-server: listening on {listen}");
    // Advertise over mDNS. The beacon is a hint only — fingerprint +
    // roles ride in TXT purely for pre-connect display/grouping.
    let _advert = if mdns {
        let domain = state.cfg.lock().domain.clone();
        let fp_short = ca_fingerprint_short(&state.serving_cert_pem)?;
        match discovery::advertise(listen, &domain, &state.roles(), &fp_short) {
            Ok(ad) => Some(ad),
            Err(e) => {
                warn!("conf-server: mDNS advertisement failed (continuing): {e:#}");
                None
            }
        }
    } else {
        None
    };
    serve_on(listener, acceptor, state).await
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
pub async fn serve_on(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    state: Arc<Server>,
) -> Result<()> {
    // If we hold the CA, re-run any id-map pushes that committed an
    // issuance but never confirmed the registration (a crash between the
    // sign and the push). The records carry their groups, so this needs no
    // admin. Best-effort: a failure stays in the recovery set and is
    // retried on the enrollee's next poll. (No issuance reconcile is
    // needed — each issuance commits as one atomic record.)
    if let Some(dir) = state.ca_dir() {
        match tokio::task::spawn_blocking({
            let dir = dir.to_path_buf();
            move || ca_store::pending_pushes(&dir)
        })
        .await
        {
            Ok(Ok(pending)) => {
                for r in pending {
                    let plan = PushPlan { id: r.req.id, name: r.name, groups: r.groups };
                    let _ = push_registrations(&state, &plan).await;
                }
            }
            Ok(Err(e)) => warn!("conf-server: listing pending id-map pushes: {e:#}"),
            Err(e) => warn!("conf-server: pending-push task panicked: {e}"),
        }
    }
    // If the CA role names an autorenew keytab, approve verified renewals
    // in-process from here on (a no-op when it doesn't).
    spawn_autorenew(&state);
    let conns = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let signs = Arc::new(Semaphore::new(MAX_CONCURRENT_SIGNS));
    loop {
        let (tcp, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("conf-server: accept failed: {e:#}");
                continue;
            }
        };
        // Gate the connection count at the door, so we don't even spawn
        // a task for one we'd immediately have to drop.
        let conn_permit = match conns.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                warn!("conf-server: at connection limit, dropping {peer}");
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
                Ok(Err(e)) => debug!("conf-server: connection from {peer} ended: {e:#}"),
                Err(_) => debug!("conf-server: connection from {peer} timed out"),
            }
        });
    }
}

async fn handle_conn(
    acceptor: &TlsAcceptor,
    tcp: TcpStream,
    peer: SocketAddr,
    state: &Arc<Server>,
    signs: Arc<Semaphore>,
) -> Result<()> {
    let mut tls = acceptor.accept(tcp).await.context("TLS handshake")?;
    // If the client presented a cert, the verifier already validated it
    // against our roots. What remains is identifying *who*: the SAN
    // authorizes server-to-server requests (the reserved name) and
    // marks renewals (SAN == requested name); the serial lets the
    // enqueue path confirm the presented cert is the live one in our
    // own index — stronger than a CRL check, since the index is the
    // source of truth on the CA host.
    let peer_ident: Option<(String, Option<u64>)> = {
        let (_io, conn) = tls.get_ref();
        conn.peer_certificates().and_then(|certs| certs.first()).and_then(|leaf| {
            let san = crate::tls::first_dns_san_from_der(leaf.as_ref())?;
            Some((san, leaf_serial(leaf.as_ref())))
        })
    };
    let peer_is_conf_server = peer_ident
        .as_ref()
        .map(|(san, _)| san.eq_ignore_ascii_case(SERVING_SAN))
        .unwrap_or(false);
    let hello: ClientHello =
        conf_proto::read_msg(&mut tls).await.context("reading ClientHello")?;
    let domain = state.cfg.lock().domain.clone();
    conf_proto::write_msg(
        &mut tls,
        &ServerHello {
            protocol_version: PROTOCOL_VERSION,
            domain,
            roles: state.roles(),
        },
    )
    .await
    .context("writing ServerHello")?;
    anyhow::ensure!(
        hello.protocol_version == PROTOCOL_VERSION,
        "client speaks protocol version {} but we speak {PROTOCOL_VERSION}",
        hello.protocol_version
    );
    let req: Request = conf_proto::read_msg(&mut tls).await.context("reading Request")?;
    match req {
        Request::GetInfo => {
            let resp = get_info(state);
            conf_proto::write_msg(&mut tls, &resp).await.context("writing GetInfoResponse")
        }
        Request::Sign(req) => {
            let resp = match ca_dir(state) {
                None => reject("this host does not hold the CA"),
                Some(dir) => {
                    let signed = run_signing(&signs, {
                        let state = state.clone();
                        let dir = dir.clone();
                        move || handle_sign_request(&state.ca, &dir, &req)
                    })
                    .await?;
                    match (signed.resp, signed.push) {
                        (resp @ SignResponse::Err { .. }, _) => resp,
                        (resp @ SignResponse::Ok { .. }, None) => resp,
                        (
                            SignResponse::Ok { signed_cert_pem, trusted_pem, mut warnings },
                            Some(plan),
                        ) => {
                            warnings.extend(push_registrations(state, &plan).await);
                            SignResponse::Ok { signed_cert_pem, trusted_pem, warnings }
                        }
                    }
                }
            };
            conf_proto::write_msg(&mut tls, &resp).await.context("writing SignResponse")
        }
        Request::Enroll(req) => {
            let resp = match ca_dir(state) {
                None => reject("this host does not hold the CA"),
                Some(dir) => {
                    let listen = req.listen;
                    let resp = run_signing(&signs, {
                        let state = state.clone();
                        let dir = dir.clone();
                        move || handle_enroll_request(&state.ca, &dir, &req)
                    })
                    .await?;
                    if matches!(resp, SignResponse::Ok { .. }) {
                        record_peer(state, listen);
                    }
                    resp
                }
            };
            conf_proto::write_msg(&mut tls, &resp).await.context("writing SignResponse")
        }
        Request::AddIdentity(req) => {
            let resp = if !peer_is_conf_server {
                AddIdentityResponse::Err {
                    reason: "identity registration requires a conf-server peer \
                             certificate"
                        .to_string(),
                }
            } else {
                let map_path = { state.cfg.lock().roles.id_map.as_ref().map(|r| r.map.clone()) };
                match map_path {
                    None => AddIdentityResponse::Err {
                        reason: "this host has no id-map role".to_string(),
                    },
                    Some(path) => {
                        let state = state.clone();
                        tokio::task::spawn_blocking(move || {
                            let _guard = state.id_map_lock.lock();
                            handle_add_identity(&path, &req)
                        })
                        .await
                        .context("id-map registration task panicked")?
                    }
                }
            };
            conf_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing AddIdentityResponse")
        }
        Request::Enqueue(req) => {
            let resp = match ca_dir(state) {
                None => EnqueueResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(dir) => {
                    tokio::task::spawn_blocking(move || {
                        handle_enqueue(&dir, &req, peer, peer_ident.as_ref())
                    })
                    .await
                    .context("enqueue task panicked")?
                }
            };
            conf_proto::write_msg(&mut tls, &resp).await.context("writing EnqueueResponse")
        }
        Request::Poll(req) => {
            let resp = match ca_dir(state) {
                None => PollResponse::Unknown,
                Some(dir) => {
                    // The blocking part reads the status (a Pending request
                    // genuinely has no cert — the commit is atomic); on a
                    // Signed record whose id-map push never landed it also
                    // hands back a re-push plan, which the async part runs.
                    let (resp, repush) = {
                        let dir = dir.clone();
                        let id = req.request_id.clone();
                        tokio::task::spawn_blocking(move || match ca_store::status(&dir, &id)
                        {
                            Ok(ca_store::Status::Pending(_)) => {
                                (PollResponse::Pending, None)
                            }
                            Ok(ca_store::Status::Signed(s)) => {
                                let repush = match ca_store::read_issued(&dir, &id) {
                                    Ok(Some(r))
                                        if !r.groups.is_empty() && !r.push_done =>
                                    {
                                        Some(PushPlan {
                                            id,
                                            name: r.name,
                                            groups: r.groups,
                                        })
                                    }
                                    _ => None,
                                };
                                let o = PollResponse::Signed {
                                    signed_cert_pem: s.signed_cert_pem,
                                    trusted_pem: s.trusted_pem,
                                    warnings: s.warnings,
                                };
                                (o, repush)
                            }
                            Ok(ca_store::Status::Denied(d)) => {
                                (PollResponse::Denied { reason: d.reason }, None)
                            }
                            Ok(ca_store::Status::Unknown) => (PollResponse::Unknown, None),
                            Err(e) => {
                                warn!("conf-server: queue status failed: {e:#}");
                                (PollResponse::Unknown, None)
                            }
                        })
                        .await
                        .context("poll task panicked")?
                    };
                    if let Some(plan) = repush {
                        // Best-effort id-map recovery (sets push_done on
                        // success); the enrollee gets its cert regardless.
                        let _ = push_registrations(state, &plan).await;
                    }
                    resp
                }
            };
            conf_proto::write_msg(&mut tls, &resp).await.context("writing PollResponse")
        }
        Request::ListQueue(req) => {
            let resp = match ca_dir(state) {
                None => ListQueueResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(dir) => {
                    run_signing(&signs, move || handle_list_queue(&dir, &req)).await?
                }
            };
            conf_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ListQueueResponse")
        }
        Request::Approve(req) => {
            let resp = match ca_dir(state) {
                None => ApproveResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(dir) => {
                    // Authenticate + sign on the blocking pool (Argon2 +
                    // openssl); the issuance commits atomically under the
                    // issuer lock inside the task, then we run the
                    // best-effort side effects.
                    let signed = run_signing(&signs, {
                        let state = state.clone();
                        let dir = dir.clone();
                        move || handle_approve(&state.ca, &dir, &req)
                    })
                    .await?;
                    match signed {
                        Err(reason) => ApproveResponse::Err { reason },
                        Ok(Approved { resp: SignResponse::Err { reason }, .. }) => {
                            // The sign itself failed (policy etc.) —
                            // the request stays pending; the admin can
                            // retry with different groups or deny it.
                            ApproveResponse::Err { reason }
                        }
                        Ok(Approved {
                            resp: SignResponse::Ok { mut warnings, .. },
                            push,
                            enroll_listen,
                            ..
                        }) => {
                            // The cert is issued and committed atomically in
                            // the signing task; these are best-effort side
                            // effects (the enrollee polls the cert back
                            // regardless). Push-registration warnings go to
                            // the approving admin's reply only.
                            if let Some(plan) = push {
                                warnings.extend(push_registrations(state, &plan).await);
                            }
                            // An approved enrollment makes the new conf
                            // server a peer — same side effect as the
                            // synchronous Enroll, deferred to approval.
                            if let Some(listen) = enroll_listen {
                                record_peer(state, listen);
                            }
                            ApproveResponse::Ok { warnings }
                        }
                    }
                }
            };
            conf_proto::write_msg(&mut tls, &resp).await.context("writing ApproveResponse")
        }
        Request::Deny(req) => {
            let resp = match ca_dir(state) {
                None => DenyResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(dir) => run_signing(&signs, {
                    let state = state.clone();
                    move || handle_deny(&state.ca, &dir, &req)
                })
                .await?,
            };
            conf_proto::write_msg(&mut tls, &resp).await.context("writing DenyResponse")
        }
        Request::Revoke(req) => {
            let resp = match ca_dir(state) {
                None => RevokeResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(dir) => {
                    let state = state.clone();
                    run_signing(&signs, move || handle_revoke(&state.ca, &dir, &req)).await?
                }
            };
            conf_proto::write_msg(&mut tls, &resp).await.context("writing RevokeResponse")
        }
        Request::ListIssued(req) => {
            let resp = match ca_dir(state) {
                None => ListIssuedResponse::Err {
                    reason: "this host does not hold the CA".to_string(),
                },
                Some(dir) => {
                    run_signing(&signs, move || handle_list_issued(&dir, &req)).await?
                }
            };
            conf_proto::write_msg(&mut tls, &resp)
                .await
                .context("writing ListIssuedResponse")
        }
        Request::GetCrl => {
            let resp = match ca_dir(state) {
                None => GetCrlResponse { crl_pem: None },
                Some(dir) => tokio::task::spawn_blocking(move || {
                    match std::fs::read_to_string(crate::ca_index::crl_path(&dir)) {
                        Ok(pem) => GetCrlResponse { crl_pem: Some(pem) },
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            GetCrlResponse { crl_pem: None }
                        }
                        Err(e) => {
                            warn!("conf-server: reading the CRL failed: {e:#}");
                            GetCrlResponse { crl_pem: None }
                        }
                    }
                })
                .await
                .context("CRL read task panicked")?,
            };
            conf_proto::write_msg(&mut tls, &resp).await.context("writing GetCrlResponse")
        }
    }
}

/// Run an Argon2-bound vault operation on `spawn_blocking`, bounded by
/// the sign semaphore.
///
/// The permit is moved *into* the blocking task and held for its whole
/// duration. A `spawn_blocking` task can't be cancelled, so if the
/// connection times out, the `JoinHandle` await below is dropped while
/// the task keeps running — releasing the permit there (not in the
/// cancellable future) is what keeps `MAX_CONCURRENT_SIGNS` honest:
/// otherwise a timeout would free the permit while the 64 MiB Argon2 is
/// still live, and repeated timeouts would exceed the bound.
async fn run_signing<T, F>(signs: &Arc<Semaphore>, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let permit =
        signs.clone().acquire_owned().await.expect("sign semaphore is never closed");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        f()
    })
    .await
    .context("CA signing task panicked")
}

fn ca_dir(state: &Server) -> Option<PathBuf> {
    state.ca_dir.clone()
}

/// Append a freshly enrolled conf server to our peer list (and persist
/// it when we have a config path). The CA host thereby becomes the
/// well-known starting point for peer walks.
fn record_peer(state: &Server, peer: SocketAddr) {
    let mut cfg = state.cfg.lock();
    if peer == cfg.listen || cfg.peers.contains(&peer) {
        return;
    }
    cfg.peers.push(peer);
    if let Some(path) = &state.cfg_path
        && let Err(e) = cfg.save(path)
    {
        warn!("conf-server: failed to persist enrolled peer {peer}: {e:#}");
    }
}

/// Local facts + known peers. Cheap and network-free: the resolver
/// address/auth is read fresh from the resolver config so config edits
/// show up without a daemon restart; everything else is our own config.
fn get_info(state: &Server) -> GetInfoResponse {
    let cfg = state.cfg.lock();
    let resolver = cfg.roles.resolver.as_ref().and_then(|r| {
        match resolver_info(&r.config) {
            Ok(x) => x,
            Err(e) => {
                warn!(
                    "conf-server: could not derive resolver info from {}: {e:#}",
                    r.config.display()
                );
                None
            }
        }
    });
    GetInfoResponse {
        domain: cfg.domain.clone(),
        ca_addr: if cfg.roles.ca.is_some() { Some(cfg.listen) } else { cfg.ca_addr },
        resolver,
        peers: cfg.peers.clone(),
    }
}

/// Derive this host's advertised resolver address + data-plane auth
/// from its resolver config. `Local` auth is host-local by definition —
/// nothing to advertise.
fn resolver_info(config: &Path) -> Result<Option<ResolverAddr>> {
    use netidx::resolver_server::config::file::Auth;
    let rc = crate::resolver::ResolverConfig::load(config)?;
    let member = rc
        .0
        .member_servers
        .first()
        .ok_or_else(|| anyhow!("resolver config has no member servers"))?;
    let auth = match &member.auth {
        Auth::Anonymous => InfoAuth::Anonymous,
        Auth::Local(_) => return Ok(None),
        Auth::Krb5(spn) => InfoAuth::Krb5 { spn: spn.to_string() },
        Auth::Tls { name, .. } => InfoAuth::Tls { name: name.to_string() },
    };
    Ok(Some(ResolverAddr { addr: member.addr, auth }))
}

/// Fan the freshly signed identity out to every id-map host we know of:
/// the local map directly, configured peers and mDNS-discovered conf
/// servers over authenticated TLS. Returns warnings for the failures —
/// the sign itself already succeeded.
async fn push_registrations(state: &Arc<Server>, plan: &PushPlan) -> Vec<String> {
    let Some((primary, secondary)) = plan.groups.split_first() else {
        return Vec::new();
    };
    let req = AddIdentityRequest {
        san: plan.name.clone(),
        primary_group: primary.clone(),
        groups: secondary.to_vec(),
    };
    let mut warnings = Vec::new();
    // Local id-map first (no TLS loopback).
    let local_map = { state.cfg.lock().roles.id_map.as_ref().map(|r| r.map.clone()) };
    if let Some(path) = local_map {
        let r = tokio::task::spawn_blocking({
            let state = state.clone();
            let req = req.clone();
            move || {
                let _guard = state.id_map_lock.lock();
                handle_add_identity(&path, &req)
            }
        })
        .await;
        match r {
            Ok(AddIdentityResponse::Ok { .. }) => (),
            Ok(AddIdentityResponse::Err { reason }) => {
                warnings.push(format!("local id-map registration failed: {reason}"))
            }
            Err(e) => warnings.push(format!("local id-map registration panicked: {e}")),
        }
    }
    // Remote peers: configured ∪ mDNS-discovered, excluding ourselves.
    let (own_listen, mut targets, mdns) = {
        let cfg = state.cfg.lock();
        (cfg.listen, cfg.peers.clone(), cfg.mdns)
    };
    if mdns {
        match discovery::browse(PUSH_BROWSE_TIMEOUT).await {
            Ok(found) => {
                for d in found {
                    for a in d.socket_addrs() {
                        if !targets.contains(&a) {
                            targets.push(a);
                        }
                    }
                }
            }
            Err(e) => warn!("conf-server: push-time mDNS browse failed: {e:#}"),
        }
    }
    targets.retain(|a| *a != own_listen);
    // Spawn the pushes so they run concurrently, then collect under a
    // single shared deadline — a dead host degrades the join to a
    // warning, never a connection timeout.
    let handles: Vec<(SocketAddr, tokio::task::JoinHandle<Result<Option<u32>>>)> =
        targets
            .into_iter()
            .map(|addr| {
                let req = req.clone();
                let state = state.clone();
                let h = tokio::spawn(async move {
                    conf_client::push_identity(
                        addr,
                        &state.serving_cert_pem,
                        &state.serving_key_pem,
                        state.roots.clone(),
                        &req,
                    )
                    .await
                });
                (addr, h)
            })
            .collect();
    let deadline = tokio::time::Instant::now() + PUSH_TIMEOUT;
    for (addr, h) in handles {
        let abort = h.abort_handle();
        match tokio::time::timeout_at(deadline, h).await {
            Ok(Ok(Ok(Some(uid)))) => {
                info!("conf-server: registered {} (uid {uid}) on {addr}", plan.name)
            }
            Ok(Ok(Ok(None))) => (), // peer has no id-map role
            Ok(Ok(Err(e))) => {
                warnings.push(format!("id-map registration on {addr} failed: {e:#}"))
            }
            Ok(Err(e)) => {
                warnings.push(format!("id-map registration on {addr} panicked: {e}"))
            }
            Err(_) => {
                abort.abort();
                warnings.push(format!(
                    "id-map registration on {addr} timed out after {}s",
                    PUSH_TIMEOUT.as_secs()
                ));
            }
        }
    }
    // Mark the issuance's id-map push complete only when nothing failed at
    // all — local *or* remote (a local failure is a real failure, not part
    // of the baseline). A partial push leaves a warning, so the record
    // stays in the recovery set (`pending_pushes`) and is retried on the
    // next poll or daemon restart. The `set_push_done` read-modify-write
    // shares the issuer mutex with `handle_revoke` so the two can't clobber
    // each other's field on the same record.
    if warnings.is_empty()
        && let Some(dir) = state.ca_dir()
    {
        let _guard = state.ca.lock();
        let _ = ca_store::set_push_done(dir, &plan.id);
    }
    warnings
}

fn build_server_config(
    cert_pem: &[u8],
    key_pem: &[u8],
    roots: RootCertStore,
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
    // Client certs are *optional*: join clients have none yet; peer
    // conf servers authenticate with theirs (verified against the CA
    // bundle) to authorize server-to-server requests.
    let verifier = WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .allow_unauthenticated()
    .build()
    .context("building client cert verifier")?;
    RustlsServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("selecting TLS versions")?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .context("building TLS server config")
}

/// A sign outcome: the wire response plus, on success, what the id-map
/// push fan-out needs.
pub struct Signed {
    pub resp: SignResponse,
    /// `Some` only when the sign succeeded *and* the request asked for
    /// id-map registration: the issued name and the admin's chosen
    /// (policy-validated) groups.
    pub push: Option<PushPlan>,
}

/// What [`push_registrations`] needs after a successful sign.
pub struct PushPlan {
    /// The issued record's id, so a confirmed push can mark its record
    /// `push_done` (closing the issue→push recovery loop).
    pub id: String,
    pub name: String,
    pub groups: Vec<String>,
}

/// Unlock the vault with the request's password and require the named
/// admin to be the slot it unlocks. The `Err` is a safe wire reason.
/// Every admin-authenticated request (Sign, Enroll, ListQueue,
/// Approve, Deny) starts here.
fn authenticate(
    ca_dir: &Path,
    admin: &str,
    password: &str,
) -> std::result::Result<ca_vault::Unlocked, String> {
    let unlocked = match ca_vault::unlock(ca_dir, password) {
        Ok(u) => u,
        Err(_) => return Err("authentication failed".to_string()),
    };
    if admin != unlocked.admin {
        return Err("admin name does not match the password".to_string());
    }
    // Opportunistic CRL re-signing while we hold the key (best-effort).
    // CA-cert renewal also needs the key but additionally needs a serial,
    // so it lives in the issuance path (`issue_locked`), which holds the
    // issuer counter.
    match crate::ca_index::refresh_crl_if_stale(ca_dir, &unlocked.ca_key_pem) {
        Ok(true) => info!("conf-server: re-signed the CRL (was nearing nextUpdate)"),
        Ok(false) => (),
        Err(e) => warn!("conf-server: opportunistic CRL refresh failed: {e:#}"),
    }
    Ok(unlocked)
}

/// Handle a sign request against the CA rooted at `ca_dir`. Auth and
/// policy failures become a `SignResponse::Err` carrying a safe reason
/// for the client; only an internal fault (e.g. the CA cert can't be
/// read) maps to a generic error response — never a panic.
pub fn handle_sign_request(
    issuer: &Mutex<CaIssuer>,
    ca_dir: &Path,
    req: &SignRequest,
) -> Signed {
    // A direct Sign has no queue entry; synthesize a request to carry in
    // the issued record (its fresh id keys the `issued/` file).
    let record_req = ca_store::QueuedReq::new(
        conf_proto::NodeKind::Client,
        req.csr_pem.clone(),
        req.requested_name.clone(),
        req.requested_validity_days,
        "(direct sign)".to_string(),
        false,
        None,
    );
    handle_sign_request_op(issuer, ca_dir, req, "sign", &record_req, None)
}

/// [`handle_sign_request`] with the audit-log operation name, the
/// record's originating request, and (for the approve path) the id to
/// re-check Pending under the lock. The approve path signs through the
/// identical checks but audits as `op=approve` and keys the record by the
/// *queued* request id.
fn handle_sign_request_op(
    issuer: &Mutex<CaIssuer>,
    ca_dir: &Path,
    req: &SignRequest,
    op: &str,
    record_req: &ca_store::QueuedReq,
    recheck_id: Option<&str>,
) -> Signed {
    match try_handle(issuer, ca_dir, req, op, record_req, recheck_id) {
        Ok(signed) => signed,
        Err(e) => Signed {
            resp: SignResponse::Err { reason: format!("internal error: {e:#}") },
            push: None,
        },
    }
}

fn try_handle(
    issuer: &Mutex<CaIssuer>,
    ca_dir: &Path,
    req: &SignRequest,
    op: &str,
    record_req: &ca_store::QueuedReq,
    recheck_id: Option<&str>,
) -> Result<Signed> {
    let failed = |resp: SignResponse| Signed { resp, push: None };
    // 1. Authenticate: the password must unlock a slot, and the named
    //    admin must be the one it unlocks.
    let unlocked = match authenticate(ca_dir, &req.admin, &req.password.0) {
        Ok(u) => u,
        Err(reason) => return Ok(failed(reject(&reason))),
    };

    // 2. Authorize: the requested name must match the admin's policy.
    let name = req.requested_name.trim();
    if name.is_empty() {
        return Ok(failed(reject("requested name is empty")));
    }
    // The conf server's own serving name is reserved: the trust model
    // hinges on *only* genuine daemons holding a CA-signed cert with it.
    // Issuing it via Sign — even to an admin whose policy glob (e.g. "*")
    // happens to match — would let that admin stand up an impostor
    // daemon. Refuse it unconditionally; conf servers are minted only by
    // the local setup path or the `may_enroll_servers`-gated enroll.
    if name.eq_ignore_ascii_case(SERVING_SAN) {
        return Ok(failed(reject(
            "that name is reserved for the conf server and cannot be issued",
        )));
    }
    if !name_permitted(name, &unlocked.policy.allowed_san)? {
        return Ok(failed(reject(&format!(
            "name {name:?} is not permitted for admin {}",
            unlocked.admin
        ))));
    }
    let validity = req.requested_validity_days.min(unlocked.policy.max_validity_days);
    if validity == 0 {
        return Ok(failed(reject("validity_days must be > 0 and within policy")));
    }
    // The id-map groups are *chosen* by the admin at enrollment time, but
    // bounded by the policy's allowed set. Refusing the whole sign on a
    // disallowed group is deliberate: silently dropping the registration
    // would produce a node whose cert works but whose perms don't.
    let groups: Vec<String> = {
        let mut gs: Vec<String> = req
            .id_map_groups
            .iter()
            .map(|g| g.trim().to_string())
            .filter(|g| !g.is_empty())
            .collect();
        gs.dedup();
        gs
    };
    for g in &groups {
        if !unlocked.policy.id_map_groups.iter().any(|a| a == g) {
            return Ok(failed(reject(&format!(
                "id-map group {g:?} is not permitted for admin {}; allowed: {:?}",
                unlocked.admin, unlocked.policy.id_map_groups,
            ))));
        }
    }
    // The one-live-cert check, serial allocation, sign, and atomic record
    // commit are one critical section under the issuance lock.
    issue_locked(
        issuer, ca_dir, &unlocked, record_req, name, validity, groups, true, op, recheck_id,
    )
}

/// The locked issuance core, shared by sign / approve / enroll / renewal.
/// Holds the in-process issuance lock across the one-live scan, serial
/// allocation, sign, and the single atomic `commit_signed` — so the
/// one-live invariant holds and the issuance is committed by one write.
/// `one_live` is false for serving certs / verified renewals, where
/// multiple live certs for a name are legitimate.
#[allow(clippy::too_many_arguments)]
fn issue_locked(
    issuer: &Mutex<CaIssuer>,
    ca_dir: &Path,
    unlocked: &ca_vault::Unlocked,
    record_req: &ca_store::QueuedReq,
    name: &str,
    validity: u32,
    groups: Vec<String>,
    one_live: bool,
    audit_op: &str,
    // For the approve path: re-check the queue entry is still Pending
    // under the lock, so two approvals (or an approve racing a deny) can't
    // both transition it. `None` for direct (non-queued) issuance.
    recheck_id: Option<&str>,
) -> Result<Signed> {
    let mut issuer = issuer.lock();
    if let Some(id) = recheck_id {
        match ca_store::status(ca_dir, id) {
            Ok(ca_store::Status::Pending(_)) => {}
            Ok(ca_store::Status::Signed(_)) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "that request was already approved".to_string(),
                    },
                    push: None,
                })
            }
            Ok(ca_store::Status::Denied(_)) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "that request was already denied".to_string(),
                    },
                    push: None,
                })
            }
            Ok(ca_store::Status::Unknown) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "no such pending request (expired or never queued)"
                            .to_string(),
                    },
                    push: None,
                })
            }
            Err(e) => return Err(e).context("re-checking the queue under the lock"),
        }
    }
    if one_live {
        let live = ca_store::live_for_name(ca_dir, name)
            .context("checking the issuance index")?;
        if !live.is_empty() {
            return Ok(Signed {
                resp: reject(&format!(
                    "an unexpired certificate already exists for {name:?}; an admin \
                     must revoke it first (`netidx conf ca revoke`)"
                )),
                push: None,
            });
        }
    }
    // Opportunistic CA-cert renewal — rare, and only allocates a serial
    // when actually renewing, so the common path burns nothing.
    if crate::ca::ca_cert_needs_renewal(ca_dir) {
        let rs = issuer.alloc();
        match crate::ca::maybe_renew_ca_cert(ca_dir, &unlocked.ca_key_pem, rs) {
            Ok(true) => info!(
                "conf-server: renewed the CA certificate (same key; glyph unchanged)"
            ),
            Ok(false) => (),
            Err(e) => warn!("conf-server: CA renewal check failed: {e:#}"),
        }
    }
    let serial = issuer.alloc();
    let resp = sign_csr(ca_dir, &unlocked.ca_key_pem, &record_req.csr_pem, name, validity, serial)?;
    if let SignResponse::Ok { ref signed_cert_pem, .. } = resp {
        ca_store::commit_issuance(
            ca_dir,
            record_req,
            serial,
            name,
            signed_cert_pem,
            &groups,
        )
        .context("committing the issuance")?;
    }
    drop(issuer);
    audit(ca_dir, &unlocked.admin, audit_op, name, validity);
    Ok(Signed {
        resp,
        push: if groups.is_empty() {
            None
        } else {
            Some(PushPlan { id: record_req.id.clone(), name: name.to_string(), groups })
        },
    })
}

/// Handle a conf-server enrollment: authenticate the admin, require the
/// `may_enroll_servers` policy bit, and sign the CSR with the reserved
/// [`SERVING_SAN`].
pub fn handle_enroll_request(
    issuer: &Mutex<CaIssuer>,
    ca_dir: &Path,
    req: &EnrollRequest,
) -> SignResponse {
    match try_enroll(issuer, ca_dir, req) {
        Ok(resp) => resp,
        Err(e) => SignResponse::Err { reason: format!("internal error: {e:#}") },
    }
}

fn try_enroll(
    issuer: &Mutex<CaIssuer>,
    ca_dir: &Path,
    req: &EnrollRequest,
) -> Result<SignResponse> {
    let unlocked = match authenticate(ca_dir, &req.admin, &req.password.0) {
        Ok(u) => u,
        Err(reason) => return Ok(reject(&reason)),
    };
    if !unlocked.policy.may_enroll_servers {
        return Ok(reject(&format!(
            "admin {} may not enroll conf servers",
            unlocked.admin
        )));
    }
    let record_req = ca_store::QueuedReq::new(
        conf_proto::NodeKind::ConfServer,
        req.csr_pem.clone(),
        SERVING_SAN.to_string(),
        crate::ca::DEFAULT_LEAF_VALIDITY_DAYS,
        "(enroll)".to_string(),
        false,
        Some(req.listen),
    );
    // Serving certs aren't subject to the one-live check (many conf
    // servers legitimately hold the reserved SAN), and carry no groups.
    let signed = issue_locked(
        issuer,
        ca_dir,
        &unlocked,
        &record_req,
        SERVING_SAN,
        crate::ca::DEFAULT_LEAF_VALIDITY_DAYS,
        Vec::new(),
        false,
        "enroll",
        None,
    )?;
    Ok(signed.resp)
}

/// The shared signing tail of every issuance path: build a transient
/// [`Ca`] from the decrypted key, sign the CSR for exactly `name` with
/// the caller-allocated `serial`, and bundle the trust anchors.
fn sign_csr(
    ca_dir: &Path,
    ca_key_pem: &[u8],
    csr_pem: &str,
    name: &str,
    validity: u32,
    serial: u64,
) -> Result<SignResponse> {
    let cert_pem =
        std::fs::read(ca_dir.join("certificate.pem")).context("reading CA certificate")?;
    let ca = Ca::from_pem(ca_dir.to_path_buf(), ca_key_pem, &cert_pem)
        .context("loading CA from vault")?;
    let san = [SanEntry::Dns(name.to_string())];
    let signed = ca
        .sign_request(csr_pem.as_bytes(), &san, validity, serial)
        .context("signing CSR")?;
    let trusted_pem = ca_store::read_trusted_bundle(ca_dir)?;
    Ok(SignResponse::Ok {
        signed_cert_pem: String::from_utf8(signed).context("signed cert not utf8")?,
        trusted_pem,
        warnings: Vec::new(),
    })
}

/// Handle an id-map registration against the map at `map_path`. A
/// missing file starts from the seeded empty map — zero-touch joins
/// must work on a host whose id-map daemon hasn't registered anyone
/// yet. The caller holds the id-map lock.
pub fn handle_add_identity(map_path: &Path, req: &AddIdentityRequest) -> AddIdentityResponse {
    let mut map = if map_path.exists() {
        match id_map::load(map_path) {
            Ok(m) => m,
            Err(e) => {
                return AddIdentityResponse::Err {
                    reason: format!("loading id-map: {e:#}"),
                }
            }
        }
    } else {
        id_map::empty()
    };
    let groups: Vec<&str> = req.groups.iter().map(|s| s.as_str()).collect();
    match id_map::register_identity(&mut map, &req.san, &req.primary_group, &groups) {
        Ok(uid) => match id_map::save(map_path, &map) {
            Ok(()) => AddIdentityResponse::Ok { uid },
            Err(e) => AddIdentityResponse::Err { reason: format!("saving id-map: {e:#}") },
        },
        Err(e) => AddIdentityResponse::Err { reason: format!("{e:#}") },
    }
}

/// Extract an X.509 certificate's serial as u64 (this CA issues from a
/// u64 counter; foreign certs with big serials yield `None`, which
/// simply never matches the index).
fn leaf_serial(der: &[u8]) -> Option<u64> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let (_, cert) = X509Certificate::from_der(der).ok()?;
    cert.tbs_certificate.serial.to_string().parse().ok()
}

/// Queue a signing request for later admin approval. Unauthenticated
/// by design — the requester has no credentials yet; trust is
/// established when the admin matches the request's CSR-key
/// fingerprint before approving. Only cheap structural checks happen
/// here (the policy checks run at approval, under the approving
/// admin's slot).
///
/// The exception is a **verified renewal**: the connection presented a
/// valid client cert whose SAN is exactly the requested name and whose
/// serial is live in our own index. That's cryptographic continuation
/// of an identity the admin already approved once — it bypasses the
/// reserved-name and one-live-cert rules (a renewal's name *does* have
/// a live cert; that's the point) and is flagged for glyph-free,
/// batchable (or automatic) approval.
fn handle_enqueue(
    ca_dir: &Path,
    req: &EnqueueRequest,
    peer: SocketAddr,
    peer_ident: Option<&(String, Option<u64>)>,
) -> EnqueueResponse {
    // Conf-server enrollment: the name is the reserved serving SAN by
    // definition, so none of the name rules below apply — not the
    // reserved-name refusal (this is the sanctioned way to request it)
    // and not one-live-cert (every conf server on the network holds the
    // same name). The real gate — the approving admin's
    // `may_enroll_servers` — runs at approval; this entry just waits in
    // the queue under the same code-matching ceremony as any other.
    if let Some(listen) = req.enroll_listen {
        let queued = ca_store::QueuedReq::new(
            req.kind,
            req.csr_pem.clone(),
            SERVING_SAN.to_string(),
            req.requested_validity_days,
            peer.to_string(),
            false,
            Some(listen),
        );
        return match ca_store::enqueue(ca_dir, &queued) {
            Ok(()) => {
                info!(
                    "conf-server: queued enrollment {} (listen {listen}) from {peer}",
                    queued.id
                );
                EnqueueResponse::Ok { request_id: queued.id }
            }
            Err(e) => EnqueueResponse::Err { reason: format!("{e:#}") },
        };
    }
    let name = req.requested_name.trim();
    if name.is_empty() {
        return EnqueueResponse::Err { reason: "requested name is empty".to_string() };
    }
    if req.requested_validity_days == 0 {
        return EnqueueResponse::Err {
            reason: "validity_days must be > 0".to_string(),
        };
    }
    let verified_renewal = match peer_ident {
        Some((san, Some(serial))) if san.eq_ignore_ascii_case(name) => {
            match ca_store::live_for_name(ca_dir, name) {
                Ok(live) => live.iter().any(|s| s.serial == *serial),
                Err(e) => {
                    warn!("conf-server: index lookup during enqueue failed: {e:#}");
                    false
                }
            }
        }
        _ => false,
    };
    if !verified_renewal {
        // Fail fast on the reserved name — approval would refuse it
        // anyway, but the enrollee should hear it now, not after the
        // admin clicked through. (A conf server renewing its own
        // serving cert is the legitimate exception above.)
        if name.eq_ignore_ascii_case(SERVING_SAN) {
            return EnqueueResponse::Err {
                reason: "that name is reserved for the conf server and cannot be \
                         issued"
                    .to_string(),
            };
        }
        // Same one-live-cert-per-name rule as the sign path, checked
        // here too so the enrollee hears it immediately instead of
        // after the admin clicked through an approval that would only
        // be refused.
        match ca_store::live_for_name(ca_dir, name) {
            Ok(live) if !live.is_empty() => {
                return EnqueueResponse::Err {
                    reason: format!(
                        "an unexpired certificate already exists for {name:?}; an \
                         admin must revoke it first (`netidx conf ca revoke`)"
                    ),
                }
            }
            Ok(_) => (),
            Err(e) => {
                return EnqueueResponse::Err {
                    reason: format!("checking the issuance index: {e:#}"),
                }
            }
        }
    }
    let queued = ca_store::QueuedReq::new(
        req.kind,
        req.csr_pem.clone(),
        name.to_string(),
        req.requested_validity_days,
        peer.to_string(),
        verified_renewal,
        None,
    );
    match ca_store::enqueue(ca_dir, &queued) {
        Ok(()) => {
            info!(
                "conf-server: queued {} {} for {name:?} from {peer}",
                if verified_renewal { "verified renewal" } else { "signing request" },
                queued.id
            );
            EnqueueResponse::Ok { request_id: queued.id }
        }
        Err(e) => EnqueueResponse::Err { reason: format!("{e:#}") },
    }
}

/// List the pending queue for an authenticated admin.
fn handle_list_queue(ca_dir: &Path, req: &ListQueueRequest) -> ListQueueResponse {
    if let Err(reason) = authenticate(ca_dir, &req.admin, &req.password.0) {
        return ListQueueResponse::Err { reason };
    }
    match ca_store::pending(ca_dir) {
        Ok(reqs) => ListQueueResponse::Ok {
            requests: reqs
                .into_iter()
                .map(|q| QueueEntry {
                    age_secs: q.age_secs(),
                    id: q.id,
                    kind: q.kind,
                    requested_name: q.requested_name,
                    requested_validity_days: q.requested_validity_days,
                    peer: q.peer,
                    csr_pem: q.csr_pem,
                    verified_renewal: q.verified_renewal,
                    enroll_listen: q.enroll_listen,
                })
                .collect(),
        },
        Err(e) => ListQueueResponse::Err { reason: format!("listing the queue: {e:#}") },
    }
}

/// Revoke certificates by serial and re-sign the CRL (admin-authenticated;
/// the daemon owns the index, so the `ca` CLI sends this rather than
/// touching the files). The fresh CRL is served via `GetCrl` and pulled by
/// the renewal daemon to each resolver's trust bundle.
fn handle_revoke(
    issuer: &Mutex<CaIssuer>,
    ca_dir: &Path,
    req: &RevokeRequest,
) -> RevokeResponse {
    let unlocked = match authenticate(ca_dir, &req.admin, &req.password.0) {
        Ok(u) => u,
        Err(reason) => return RevokeResponse::Err { reason },
    };
    if req.serials.is_empty() {
        return RevokeResponse::Err { reason: "no serials to revoke".to_string() };
    }
    let now = ca_store::now_unix();
    let mut warnings = Vec::new();
    // Each revocation is a read-modify-write of an `issued/<id>` record.
    // Hold the issuer mutex across the loop so a concurrent `set_push_done`
    // (the other read-modify-write on an issued record) can't read a stale
    // record and clobber the `revoked` flag. `set_push_done` takes the same
    // lock in `push_registrations`. The CRL rewrite below has its own lock
    // (`ca_index::CRL_LOCK`), so it stays outside this guard.
    {
        let _guard = issuer.lock();
        for serial in &req.serials {
            let rev = ca_store::Revocation {
                serial: *serial,
                revoked_unix: now,
                reason: req.reason.clone(),
            };
            match ca_store::revoke(ca_dir, *serial, rev) {
                Ok(true) => audit(
                    ca_dir,
                    &unlocked.admin,
                    "revoke",
                    &format!("serial {serial}"),
                    0,
                ),
                Ok(false) => warnings.push(format!(
                    "serial {serial} was not live (unknown or already revoked)"
                )),
                Err(e) => warnings.push(format!("revoking serial {serial}: {e:#}")),
            }
        }
    }
    if let Err(e) = crate::ca_index::write_crl(ca_dir, &unlocked.ca_key_pem) {
        warnings.push(format!("re-signing the CRL: {e:#}"));
    }
    RevokeResponse::Ok { warnings }
}

/// List every issued certificate (admin-authenticated) — the revoke UI
/// and inspection. The daemon owns the index.
fn handle_list_issued(ca_dir: &Path, req: &ListIssuedRequest) -> ListIssuedResponse {
    if let Err(reason) = authenticate(ca_dir, &req.admin, &req.password.0) {
        return ListIssuedResponse::Err { reason };
    }
    match ca_store::list_signed(ca_dir) {
        Ok(records) => ListIssuedResponse::Ok {
            entries: records
                .into_iter()
                .map(|r| IssuedEntry {
                    serial: r.serial,
                    name: r.name,
                    spki_fp: r.spki_fp,
                    not_after_unix: r.not_after_unix,
                    revoked: r.revoked.is_some(),
                })
                .collect(),
        },
        Err(e) => ListIssuedResponse::Err { reason: format!("listing issued certs: {e:#}") },
    }
}

/// A successful [`handle_approve`]: the signed outcome plus what the
/// dispatch arm needs to finish the job — the push plan for id-map
/// registration, and the peer address to record when the approved
/// entry was a conf-server enrollment.
struct Approved {
    resp: SignResponse,
    push: Option<PushPlan>,
    enroll_listen: Option<SocketAddr>,
}

/// Approve a queued request: look it up, then sign it through the
/// exact same checks a synchronous [`SignRequest`] goes through (the
/// admin's SAN globs, validity cap, id-map group allowed-set), audited
/// as `op=approve`. The outer `Err` is a safe wire reason for
/// before-the-sign failures.
fn handle_approve(
    issuer: &Mutex<CaIssuer>,
    ca_dir: &Path,
    req: &ApproveRequest,
) -> std::result::Result<Approved, String> {
    // This cheap precheck (no auth) rejects an already-terminal request;
    // the authoritative re-check happens under the lock.
    let queued = match ca_store::status(ca_dir, &req.request_id) {
        Ok(ca_store::Status::Pending(q)) => q,
        Ok(ca_store::Status::Signed(_)) => {
            return Err("that request was already approved".to_string())
        }
        Ok(ca_store::Status::Denied(_)) => {
            return Err("that request was already denied".to_string())
        }
        Ok(ca_store::Status::Unknown) => {
            return Err("no such pending request (expired or never queued)".to_string())
        }
        Err(e) => return Err(format!("reading the queue: {e:#}")),
    };
    approve_locked(issuer, ca_dir, req, queued)
}

/// Sign a queued request through the same checks a synchronous Sign goes
/// through (or, for a verified renewal / conf-server enrollment, the
/// narrower continuation gate), committing the issuance atomically under
/// the issuer lock. `queued` came from the cheap precheck; `issue_locked`
/// re-checks it is still Pending under the lock.
fn approve_locked(
    issuer: &Mutex<CaIssuer>,
    ca_dir: &Path,
    req: &ApproveRequest,
    queued: ca_store::QueuedReq,
) -> std::result::Result<Approved, String> {
    // A queued conf-server enrollment: gated on the approving admin's
    // `may_enroll_servers`; signs the reserved serving SAN; no one-live
    // check and no id-map groups (a conf server isn't a user).
    if let Some(listen) = queued.enroll_listen {
        let unlocked = authenticate(ca_dir, &req.admin, &req.password.0)?;
        if !unlocked.policy.may_enroll_servers {
            return Err(format!("admin {} may not enroll conf servers", unlocked.admin));
        }
        let signed = issue_locked(
            issuer,
            ca_dir,
            &unlocked,
            &queued,
            SERVING_SAN,
            crate::ca::DEFAULT_LEAF_VALIDITY_DAYS,
            Vec::new(),
            false,
            "enroll",
            Some(&req.request_id),
        )
        .map_err(|e| format!("internal error: {e:#}"))?;
        return Ok(Approved {
            resp: signed.resp,
            push: signed.push,
            enroll_listen: Some(listen),
        });
    }
    // A verified renewal: continuation of an already-approved identity —
    // possession of the live key was proven at enqueue. The SAN-scope and
    // one-live-cert checks don't apply (a renewal's name *does* have a live
    // cert), the reserved serving name is allowed (conf servers renew
    // themselves), and the id-map is untouched (requested groups ignored).
    if queued.verified_renewal {
        let unlocked = authenticate(ca_dir, &req.admin, &req.password.0)?;
        let validity = queued
            .requested_validity_days
            .min(unlocked.policy.max_validity_days)
            .max(1);
        let signed = issue_locked(
            issuer,
            ca_dir,
            &unlocked,
            &queued,
            &queued.requested_name,
            validity,
            Vec::new(),
            false,
            "renew",
            Some(&req.request_id),
        )
        .map_err(|e| format!("internal error: {e:#}"))?;
        return Ok(Approved {
            resp: signed.resp,
            push: signed.push,
            enroll_listen: None,
        });
    }
    // Ordinary approval: the full policy checks under the *approving*
    // admin's slot, audited as `op=approve`, the record keyed by the
    // queued request's id so the enrollee polls it back.
    let sign_req = SignRequest {
        admin: req.admin.clone(),
        password: req.password.clone(),
        csr_pem: queued.csr_pem.clone(),
        requested_name: queued.requested_name.clone(),
        requested_validity_days: queued.requested_validity_days,
        id_map_groups: req.id_map_groups.clone(),
    };
    let signed = handle_sign_request_op(
        issuer,
        ca_dir,
        &sign_req,
        "approve",
        &queued,
        Some(&req.request_id),
    );
    Ok(Approved {
        resp: signed.resp,
        push: signed.push,
        enroll_listen: None,
    })
}

/// Read the autorenew slot's password from its keytab, unsealing if this
/// host sealed it to its TPM (the install path seals when it can). A
/// sealed keytab that won't unseal is a hard error here — but the caller
/// only logs it and skips spawning the approver, so the rest of the
/// daemon serves regardless; renewals just fall back to human approval.
fn read_autorenew_password(keytab: &Path) -> Result<Zeroizing<String>> {
    let raw = std::fs::read(keytab)
        .with_context(|| format!("reading autorenew keytab {}", keytab.display()))?;
    if netidx_tpm::is_sealed(&raw) {
        let secret = netidx_tpm::unseal(&raw).with_context(|| {
            format!(
                "unsealing autorenew keytab {} — if this host's TPM was cleared \
                 or the board was replaced, mint a fresh keytab with \
                 `netidx conf ca auto-approve --rotate`",
                keytab.display()
            )
        })?;
        Ok(Zeroizing::new(
            String::from_utf8(secret.to_vec())
                .context("sealed autorenew keytab payload is not utf8")?,
        ))
    } else {
        let pw = String::from_utf8(raw)
            .with_context(|| format!("autorenew keytab {} is not utf8", keytab.display()))?;
        Ok(Zeroizing::new(pw.trim().to_string()))
    }
}

/// One autorenew pass: approve every pending **verified renewal** as the
/// [`AUTORENEW_ADMIN`] slot, returning the count approved. Only verified
/// renewals — continuations of an identity an admin already approved once,
/// proven by possession of the live key at enqueue — are auto-approved;
/// new enrollments always wait for a human (and the slot's empty policy
/// would refuse them anyway). Runs on the blocking pool: each approval
/// does an Argon2 unlock + openssl sign, committed atomically under the
/// issuer lock by the same [`handle_approve`] the wire path uses, so it is
/// audited as `op=renew` by the `autorenew` admin (the verified-renewal
/// continuation gate) — exactly the trail the separate daemon left.
fn autorenew_sweep(issuer: &Mutex<CaIssuer>, ca_dir: &Path, password: &str) -> usize {
    let pending = match ca_store::pending(ca_dir) {
        Ok(p) => p,
        Err(e) => {
            warn!("autorenew: scanning the queue failed: {e:#}");
            return 0;
        }
    };
    let mut approved = 0;
    for q in pending.iter().filter(|q| q.verified_renewal) {
        let req = ApproveRequest {
            admin: AUTORENEW_ADMIN.to_string(),
            password: conf_proto::Secret(password.to_string()),
            request_id: q.id.clone(),
            id_map_groups: Vec::new(),
        };
        match handle_approve(issuer, ca_dir, &req) {
            Ok(Approved { resp: SignResponse::Ok { .. }, .. }) => {
                approved += 1;
                info!("autorenew: approved renewal of {:?}", q.requested_name);
            }
            Ok(Approved { resp: SignResponse::Err { reason }, .. }) => {
                warn!("autorenew: signing renewal of {:?} failed: {reason}", q.requested_name)
            }
            Err(reason) => {
                warn!("autorenew: approving renewal of {:?} failed: {reason}", q.requested_name)
            }
        }
    }
    approved
}

/// If this host's CA role names an autorenew keytab, spawn the in-process
/// approver: every [`AUTORENEW_POLL`] it approves pending verified
/// renewals. The credential is unsealed once here and held for the
/// daemon's life — the same exposure the old separate autorenew process
/// had, on the same host, now without the extra process. A keytab that
/// won't unseal is logged and the task simply isn't spawned (the daemon
/// keeps serving). Captures a `Weak`, so a dropped server stops the loop.
fn spawn_autorenew(state: &Arc<Server>) {
    let keytab = {
        let cfg = state.cfg.lock();
        cfg.roles.ca.as_ref().and_then(|c| c.autorenew.clone())
    };
    let Some(keytab) = keytab else { return };
    let Some(ca_dir) = state.ca_dir().map(|d| d.to_path_buf()) else { return };
    let password = match read_autorenew_password(&keytab) {
        Ok(pw) => pw,
        Err(e) => {
            warn!("conf-server: autorenew disabled — {e:#}");
            return;
        }
    };
    info!("conf-server: autorenew enabled (approving verified renewals as {AUTORENEW_ADMIN:?})");
    let weak = Arc::downgrade(state);
    tokio::spawn(async move {
        loop {
            let Some(state) = weak.upgrade() else { break };
            let dir = ca_dir.clone();
            let pw = password.clone();
            // Hand the Arc to the blocking task and let it drop there, so
            // we never hold the server alive across the sleep below.
            if let Err(e) = tokio::task::spawn_blocking(move || {
                autorenew_sweep(&state.ca, &dir, &pw);
            })
            .await
            {
                warn!("autorenew: sweep task panicked: {e}");
            }
            tokio::time::sleep(AUTORENEW_POLL).await;
        }
    });
}

/// Deny a queued request (any authenticated admin). Holds the issuer lock
/// across the status re-check and the denial write, so a deny and an
/// approve can never both transition the same request.
fn handle_deny(issuer: &Mutex<CaIssuer>, ca_dir: &Path, req: &DenyRequest) -> DenyResponse {
    // Cheap precheck (no auth) for an already-terminal/unknown request.
    let queued = match ca_store::status(ca_dir, &req.request_id) {
        Ok(ca_store::Status::Pending(q)) => q,
        Ok(ca_store::Status::Signed(_)) => {
            return DenyResponse::Err {
                reason: "that request was already approved".to_string(),
            }
        }
        Ok(ca_store::Status::Denied(_)) => {
            return DenyResponse::Err {
                reason: "that request was already denied".to_string(),
            }
        }
        Ok(ca_store::Status::Unknown) => {
            return DenyResponse::Err {
                reason: "no such pending request (expired or never queued)".to_string(),
            }
        }
        Err(e) => return DenyResponse::Err { reason: format!("reading the queue: {e:#}") },
    };
    let unlocked = match authenticate(ca_dir, &req.admin, &req.password.0) {
        Ok(u) => u,
        Err(reason) => return DenyResponse::Err { reason },
    };
    let _guard = issuer.lock();
    // Authoritative re-check under the lock (mutually exclusive with the
    // approve commit, which also holds this lock).
    match ca_store::status(ca_dir, &req.request_id) {
        Ok(ca_store::Status::Pending(_)) => {}
        Ok(ca_store::Status::Signed(_)) => {
            return DenyResponse::Err {
                reason: "that request was already approved".to_string(),
            }
        }
        Ok(ca_store::Status::Denied(_)) => {
            return DenyResponse::Err {
                reason: "that request was already denied".to_string(),
            }
        }
        Ok(ca_store::Status::Unknown) => {
            return DenyResponse::Err {
                reason: "no such pending request (expired or never queued)".to_string(),
            }
        }
        Err(e) => return DenyResponse::Err { reason: format!("reading the queue: {e:#}") },
    }
    match ca_store::deny(ca_dir, &queued, &req.reason) {
        Ok(()) => {
            audit(ca_dir, &unlocked.admin, "deny", &queued.requested_name, 0);
            DenyResponse::Ok
        }
        Err(e) => DenyResponse::Err { reason: format!("storing the denial: {e:#}") },
    }
}

/// True if `name` matches any of the admin's `allowed` glob patterns.
/// An empty pattern list denies everything (issuance scope is granted
/// explicitly).
fn name_permitted(name: &str, allowed: &[String]) -> Result<bool> {
    for pat in allowed {
        let glob = Glob::new(pat).with_context(|| format!("bad policy glob {pat:?}"))?;
        if glob.compile_matcher().is_match(name) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn reject(reason: &str) -> SignResponse {
    SignResponse::Err { reason: reason.to_string() }
}

/// Append a line to the CA's audit log (best-effort — a failed write
/// must not fail the operation it records). Public because the CLI's
/// revoke writes the same trail the daemon's sign/approve/deny do.
pub fn audit(ca_dir: &Path, admin: &str, op: &str, name: &str, validity: u32) {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let line = format!("ts={ts} admin={admin} op={op} name={name} validity_days={validity}\n");
    let r = OpenOptions::new()
        .create(true)
        .append(true)
        .open(ca_dir.join("audit.log"))
        .and_then(|mut f| f.write_all(line.as_bytes()));
    if let Err(e) = r {
        // Audit is best-effort; a failed write must not fail issuance.
        eprintln!("conf-server: WARNING failed to append audit log: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ca::{Ca, CaParams, Subject, MIN_KEY_BITS},
        ca_vault::{self, Policy},
        conf_client,
        conf_proto::{NodeKind, Secret},
        conf_server_config::{CaRole, ConfServerConfig, IdMapRole, Roles},
        fingerprint::Fingerprint,
        tls_tofu::TofuVerifier,
    };
    use zeroize::Zeroizing;

    fn policy() -> Policy {
        Policy {
            allowed_san: vec!["*.ryu-oh.org".to_string()],
            max_validity_days: 30,
            id_map_groups: vec!["users".to_string()],
            may_enroll_servers: true,
        }
    }

    /// A fresh in-process issuer seeded from the CA dir exactly as
    /// [`Server::new`] does — the direct-call handler tests pass `&this`
    /// where the daemon would pass its own `Server::ca`.
    fn issuer(dir: &Path) -> Mutex<CaIssuer> {
        Mutex::new(CaIssuer { next_serial: ca_store::next_serial(dir).unwrap() })
    }

    /// Issue the daemon's TLS serving cert from the (vault-protected)
    /// CA and record it in the store the way the real bootstrap
    /// (`setup_server`) does — so the serving cert is a known issuance
    /// (its serial seeds the daemon's counter, and it can verify-renew
    /// itself). Returns `([leaf_pem ++ ca_pem], serving_key_pem)`.
    fn issue_serving_cert(dir: &Path) -> (Vec<u8>, Vec<u8>) {
        let unlocked = ca_vault::unlock(dir, "apw").unwrap();
        let ca_cert = std::fs::read(dir.join("certificate.pem")).unwrap();
        let ca = Ca::from_pem(dir.to_path_buf(), &unlocked.ca_key_pem, &ca_cert).unwrap();
        let kc = conf_client::generate_key_and_csr(SERVING_SAN).unwrap();
        let serial = ca_store::next_serial(dir).unwrap();
        let leaf = ca
            .sign_request(
                kc.csr_pem.as_bytes(),
                &[SanEntry::Dns(SERVING_SAN.into())],
                365,
                serial,
            )
            .unwrap();
        let req = ca_store::QueuedReq::new(
            NodeKind::ConfServer,
            kc.csr_pem.clone(),
            SERVING_SAN.to_string(),
            365,
            "(test serving cert)".to_string(),
            false,
            None,
        );
        ca_store::commit_issuance(
            dir,
            &req,
            serial,
            SERVING_SAN,
            std::str::from_utf8(&leaf).unwrap(),
            &[],
        )
        .unwrap();
        let mut chain = leaf;
        chain.extend_from_slice(&ca_cert);
        (chain, kc.private_key_pem.as_bytes().to_vec())
    }

    /// Issue an arbitrary leaf (chain `[leaf, ca]`) — for serving certs
    /// and for the "wrong client cert" authz test.
    fn issue_client_cert(dir: &Path, san: &str) -> (Vec<u8>, Vec<u8>) {
        let unlocked = ca_vault::unlock(dir, "apw").unwrap();
        let ca_cert = std::fs::read(dir.join("certificate.pem")).unwrap();
        let ca = Ca::from_pem(dir.to_path_buf(), &unlocked.ca_key_pem, &ca_cert).unwrap();
        let kc = conf_client::generate_key_and_csr(san).unwrap();
        let leaf = ca
            .sign_request(kc.csr_pem.as_bytes(), &[SanEntry::Dns(san.into())], 365, 1000)
            .unwrap();
        let mut chain = leaf;
        chain.extend_from_slice(&ca_cert);
        (chain, kc.private_key_pem.as_bytes().to_vec())
    }

    /// Bind an ephemeral port and run a conf server with the given
    /// roles + peers over the test CA at `dir`. Returns the address
    /// and the live state (so tests can assert on e.g. learned peers).
    async fn spawn_server_with(
        dir: &Path,
        roles: Roles,
        peers: Vec<SocketAddr>,
    ) -> (SocketAddr, Arc<Server>) {
        let (cert, key) = issue_serving_cert(dir);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = ConfServerConfig {
            domain: "ryu-oh.org".to_string(),
            listen: addr,
            // serve_on uses the in-memory PEMs; these paths are only
            // read by `serve()`, which tests don't go through.
            serving_cert: dir.join("unused-cert.pem"),
            serving_key: dir.join("unused-key.pem"),
            trusted: dir.join("certificate.pem"),
            roles,
            ca_addr: None,
            peers,
            mdns: false,
        };
        let state = Server::new(cfg, None, cert, key).unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(
            build_server_config(
                &state.serving_cert_pem,
                &state.serving_key_pem,
                state.roots.clone(),
            )
            .unwrap(),
        ));
        tokio::spawn(serve_on(listener, acceptor, state.clone()));
        (addr, state)
    }

    async fn spawn_ca_server(dir: &Path) -> (SocketAddr, Arc<Server>) {
        let roles =
            Roles {
                ca: Some(CaRole { dir: dir.to_path_buf(), autorenew: None }),
                resolver: None,
                id_map: None,
            };
        spawn_server_with(dir, roles, vec![]).await
    }

    /// Build a vault-protected CA in `dir`: a real (RSA) CA whose key is
    /// moved into a 1-admin vault, no `private.key` left behind.
    fn setup_ca(dir: &Path) {
        setup_ca_with_policy(dir, policy());
    }

    fn setup_ca_with_policy(dir: &Path, policy: Policy) {
        let params = CaParams {
            directory: dir.to_path_buf(),
            subject: Subject::cn("Test CA".to_string()),
            san: vec![],
            key_bits: MIN_KEY_BITS,
            validity_days: 30,
        };
        Ca::init(&params, None).unwrap();
        let key = std::fs::read(dir.join("private.key")).unwrap();
        ca_vault::create(dir, &key, "alice", "apw", policy).unwrap();
        std::fs::remove_file(dir.join("private.key")).unwrap();
    }

    fn request(name: &str, admin: &str, pw: &str, days: u32) -> SignRequest {
        let kc = conf_client::generate_key_and_csr(name).unwrap();
        SignRequest {
            admin: admin.to_string(),
            password: Secret(pw.to_string()),
            csr_pem: kc.csr_pem,
            requested_name: name.to_string(),
            requested_validity_days: days,
            id_map_groups: vec!["users".to_string()],
        }
    }

    #[test]
    fn signs_a_permitted_name() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let req = request("resolver.ryu-oh.org", "alice", "apw", 30);
        let signed = handle_sign_request(&issuer(dir.path()), dir.path(), &req);
        match signed.resp {
            SignResponse::Ok { signed_cert_pem, trusted_pem, warnings } => {
                assert!(signed_cert_pem.contains("BEGIN CERTIFICATE"));
                assert!(trusted_pem.contains("BEGIN CERTIFICATE"));
                assert!(warnings.is_empty());
                // The signed leaf parses as a real X.509 cert.
                openssl::x509::X509::from_pem(signed_cert_pem.as_bytes()).unwrap();
                // An audit line was written.
                let log = std::fs::read_to_string(dir.path().join("audit.log")).unwrap();
                assert!(log.contains("admin=alice"));
                assert!(log.contains("op=sign"));
                assert!(log.contains("name=resolver.ryu-oh.org"));
                // And the issuance landed in the index (revoke-by-name /
                // duplicate-refusal source of truth).
                let live = ca_store::live_for_name(
                    dir.path(),
                    "resolver.ryu-oh.org",
                )
                .unwrap();
                assert_eq!(live.len(), 1);
                assert!(!live[0].spki_fp.is_empty());
            }
            SignResponse::Err { reason } => panic!("expected Ok, got: {reason}"),
        }
        // A successful sign carries the push plan: the issued name and
        // the groups the admin chose in the request.
        let plan = signed.push.expect("successful sign must carry a push plan");
        assert_eq!(plan.name, "resolver.ryu-oh.org");
        assert_eq!(plan.groups, vec!["users".to_string()]);
    }

    #[test]
    fn id_map_groups_are_chosen_per_request_within_policy() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca_with_policy(
            dir.path(),
            Policy {
                allowed_san: vec!["*.ryu-oh.org".to_string()],
                max_validity_days: 30,
                // The *allowed set* — what this admin may assign.
                id_map_groups: vec!["users".to_string(), "dev".to_string()],
                may_enroll_servers: false,
            },
        );
        let iss = issuer(dir.path());
        // Choosing an allowed subset works, and the plan carries the
        // request's choice, not the whole policy.
        let mut req = request("a.ryu-oh.org", "alice", "apw", 30);
        req.id_map_groups = vec!["dev".to_string()];
        let signed = handle_sign_request(&iss, dir.path(), &req);
        assert!(matches!(signed.resp, SignResponse::Ok { .. }));
        assert_eq!(signed.push.unwrap().groups, vec!["dev".to_string()]);
        // Choosing no groups skips registration without failing the
        // sign.
        let mut req = request("b.ryu-oh.org", "alice", "apw", 30);
        req.id_map_groups = vec![];
        let signed = handle_sign_request(&iss, dir.path(), &req);
        assert!(matches!(signed.resp, SignResponse::Ok { .. }));
        assert!(signed.push.is_none());
        // Choosing a group outside the allowed set refuses the whole
        // sign — silently dropping the registration would produce a
        // node whose cert works but whose perms mysteriously don't.
        let mut req = request("c.ryu-oh.org", "alice", "apw", 30);
        req.id_map_groups = vec!["wheel".to_string()];
        let signed = handle_sign_request(&iss, dir.path(), &req);
        assert!(signed.push.is_none());
        match signed.resp {
            SignResponse::Err { reason } => {
                assert!(reason.contains("not permitted"), "got: {reason}")
            }
            SignResponse::Ok { .. } => panic!("disallowed group was accepted"),
        }
    }

    #[test]
    fn rejects_wrong_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let req = request("resolver.ryu-oh.org", "alice", "WRONG", 30);
        let signed = handle_sign_request(&issuer(dir.path()), dir.path(), &req);
        assert!(signed.push.is_none());
        match signed.resp {
            SignResponse::Err { reason } => assert!(reason.contains("authentication")),
            SignResponse::Ok { .. } => panic!("wrong password was accepted"),
        }
    }

    #[test]
    fn rejects_name_outside_policy() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let req = request("evil.example.com", "alice", "apw", 30);
        match handle_sign_request(&issuer(dir.path()), dir.path(), &req).resp {
            SignResponse::Err { reason } => assert!(reason.contains("not permitted")),
            SignResponse::Ok { .. } => panic!("out-of-policy name was signed"),
        }
    }

    #[test]
    fn caps_validity_to_policy() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // Ask for 9999 days; policy caps at 30. Should still succeed.
        let req = request("a.ryu-oh.org", "alice", "apw", 9999);
        let SignResponse::Ok { signed_cert_pem, .. } =
            handle_sign_request(&issuer(dir.path()), dir.path(), &req).resp
        else {
            panic!("expected Ok");
        };
        let cert = openssl::x509::X509::from_pem(signed_cert_pem.as_bytes()).unwrap();
        // notAfter should be ~30 days out, well under the 9999 requested.
        let in_60_days = openssl::asn1::Asn1Time::days_from_now(60).unwrap();
        assert!(cert.not_after() < in_60_days);
    }

    #[test]
    fn refuses_to_issue_the_reserved_serving_name() {
        let dir = tempfile::tempdir().unwrap();
        // A wide-open "*" policy — which WOULD match the reserved name —
        // must still not be able to mint a conf server's serving cert
        // via Sign (that would enable impersonating the daemon). Only
        // the `may_enroll_servers`-gated Enroll path may.
        setup_ca_with_policy(
            dir.path(),
            Policy {
                allowed_san: vec!["*".to_string()],
                max_validity_days: 30,
                id_map_groups: vec![],
                may_enroll_servers: true,
            },
        );
        let req = request(SERVING_SAN, "alice", "apw", 30);
        match handle_sign_request(&issuer(dir.path()), dir.path(), &req).resp {
            SignResponse::Err { reason } => assert!(reason.contains("reserved")),
            SignResponse::Ok { .. } => panic!("issued the reserved serving name"),
        }
    }

    #[test]
    fn admin_name_must_match_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // Right password, wrong admin name.
        let req = request("a.ryu-oh.org", "bob", "apw", 30);
        match handle_sign_request(&issuer(dir.path()), dir.path(), &req).resp {
            SignResponse::Err { reason } => assert!(reason.contains("does not match")),
            SignResponse::Ok { .. } => panic!("admin/password mismatch accepted"),
        }
    }

    #[test]
    fn enroll_requires_the_policy_bit() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca_with_policy(
            dir.path(),
            Policy {
                allowed_san: vec!["*".to_string()],
                max_validity_days: 30,
                id_map_groups: vec![],
                may_enroll_servers: false,
            },
        );
        let kc = conf_client::generate_key_and_csr(SERVING_SAN).unwrap();
        let req = EnrollRequest {
            admin: "alice".to_string(),
            password: Secret("apw".to_string()),
            csr_pem: kc.csr_pem,
            listen: "127.0.0.1:4565".parse().unwrap(),
        };
        match handle_enroll_request(&issuer(dir.path()), dir.path(), &req) {
            SignResponse::Err { reason } => assert!(reason.contains("may not enroll")),
            SignResponse::Ok { .. } => panic!("enrolled without the policy bit"),
        }
    }

    #[test]
    fn enroll_signs_the_reserved_san() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path()); // policy(): may_enroll_servers = true
        let kc = conf_client::generate_key_and_csr(SERVING_SAN).unwrap();
        let req = EnrollRequest {
            admin: "alice".to_string(),
            password: Secret("apw".to_string()),
            csr_pem: kc.csr_pem,
            listen: "127.0.0.1:4565".parse().unwrap(),
        };
        let SignResponse::Ok { signed_cert_pem, .. } =
            handle_enroll_request(&issuer(dir.path()), dir.path(), &req)
        else {
            panic!("expected Ok");
        };
        let cert = openssl::x509::X509::from_pem(signed_cert_pem.as_bytes()).unwrap();
        let san = cert.subject_alt_names().unwrap();
        assert!(san.iter().any(|n| n.dnsname() == Some(SERVING_SAN)));
        let log = std::fs::read_to_string(dir.path().join("audit.log")).unwrap();
        assert!(log.contains("op=enroll"));
    }

    #[tokio::test]
    async fn end_to_end_join_over_tls() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _state) = spawn_ca_server(dir.path()).await;

        // Inspect first — the operator is shown the CA fingerprint plus
        // the TLS-attested domain and roles.
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        assert!(!identity.fingerprint.text().is_empty());
        assert_eq!(identity.domain, "ryu-oh.org");
        assert_eq!(identity.roles, vec![Role::Ca]);
        // Then sign, pinned to the confirmed identity.
        let issued = conf_client::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            30,
            vec!["users".to_string()],
            &identity,
        )
        .await
        .unwrap();

        // The signed leaf parses and is for our name.
        let cert = openssl::x509::X509::from_pem(issued.cert_pem.as_bytes()).unwrap();
        let san = cert.subject_alt_names().unwrap();
        assert!(san.iter().any(|n| n.dnsname() == Some("resolver.ryu-oh.org")));
        // The returned trust bundle is the CA cert (the default bundle).
        let ca_disk = std::fs::read(dir.path().join("certificate.pem")).unwrap();
        assert_eq!(issued.trusted_pem.as_bytes(), ca_disk.as_slice());
        // No id-map hosts in this network ⇒ no warnings.
        assert!(issued.warnings.is_empty());
    }

    #[tokio::test]
    async fn mismatched_ca_identity_aborts_before_the_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        // Inspect, then tamper the confirmed fingerprint to simulate a CA
        // that swapped its cert between inspection and signing. The pin in
        // `request_cert` must reject it before the password is sent.
        let mut identity =
            conf_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        identity.fingerprint = Fingerprint::of_der(b"not the real CA cert");
        let err = conf_client::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            30,
            vec![],
            &identity,
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("fingerprint mismatch"));
    }

    #[tokio::test]
    async fn wrong_password_over_the_wire_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        let err = conf_client::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("WRONG".to_string()),
            30,
            vec![],
            &identity,
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("refused"));
    }

    #[tokio::test]
    async fn get_info_returns_local_facts_and_peers() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let peer: SocketAddr = "192.168.0.9:4565".parse().unwrap();
        let roles = Roles {
            ca: Some(CaRole { dir: dir.path().to_path_buf(), autorenew: None }),
            resolver: None,
            id_map: None,
        };
        let (addr, _state) = spawn_server_with(dir.path(), roles, vec![peer]).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        let info = conf_client::get_info(addr, NodeKind::Client, &identity).await.unwrap();
        assert_eq!(info.domain, "ryu-oh.org");
        assert_eq!(info.ca_addr, Some(addr));
        assert_eq!(info.peers, vec![peer]);
        assert!(info.resolver.is_none());
    }

    #[tokio::test]
    async fn aggregation_walks_peers() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // B: a roleless stepping stone; A: the CA, which knows about B.
        let (b_addr, _b) = spawn_server_with(dir.path(), Roles::default(), vec![]).await;
        let roles = Roles {
            ca: Some(CaRole { dir: dir.path().to_path_buf(), autorenew: None }),
            resolver: None,
            id_map: None,
        };
        let (a_addr, _a) = spawn_server_with(dir.path(), roles, vec![b_addr]).await;
        let identity =
            conf_client::fetch_identity(a_addr, NodeKind::Client).await.unwrap();
        let info =
            conf_client::aggregate(&[a_addr], NodeKind::Client, &identity).await.unwrap();
        assert_eq!(info.ca_addr, Some(a_addr));
        assert!(info.reached.contains(&a_addr));
        assert!(info.reached.contains(&b_addr), "peer walk must reach B via A");
    }

    #[tokio::test]
    async fn sign_pushes_the_identity_to_id_map_peers() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let map_path = dir.path().join("id-map.json");
        // B: an id-map host (no map file yet — zero-touch must seed it).
        let roles_b = Roles {
            ca: None,
            resolver: None,
            id_map: Some(IdMapRole { map: map_path.clone() }),
        };
        let (b_addr, _b) = spawn_server_with(dir.path(), roles_b, vec![]).await;
        // A: the CA, with B as a configured peer.
        let roles_a = Roles {
            ca: Some(CaRole { dir: dir.path().to_path_buf(), autorenew: None }),
            resolver: None,
            id_map: None,
        };
        let (a_addr, _a) = spawn_server_with(dir.path(), roles_a, vec![b_addr]).await;
        let identity =
            conf_client::fetch_identity(a_addr, NodeKind::Workstation).await.unwrap();
        let issued = conf_client::request_cert(
            a_addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            30,
            vec!["users".to_string()],
            &identity,
        )
        .await
        .unwrap();
        assert!(issued.warnings.is_empty(), "push should succeed: {:?}", issued.warnings);
        // The CA pushed the registration to B, which allocated a uid and
        // wrote its map.
        let map = crate::id_map::load(&map_path).unwrap();
        let ident = map.lookup_by_name("eric.ryu-oh.org").expect("identity registered");
        assert_eq!(ident.primary_group.as_str(), "users");
        assert!(ident.uid >= 1000);
    }

    #[tokio::test]
    async fn sign_warns_when_an_id_map_peer_is_down() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // Reserve a port and drop the listener — connecting to it fails
        // fast with ECONNREFUSED.
        let dead = {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap()
        };
        let roles = Roles {
            ca: Some(CaRole { dir: dir.path().to_path_buf(), autorenew: None }),
            resolver: None,
            id_map: None,
        };
        let (a_addr, _a) = spawn_server_with(dir.path(), roles, vec![dead]).await;
        let identity =
            conf_client::fetch_identity(a_addr, NodeKind::Workstation).await.unwrap();
        let issued = conf_client::request_cert(
            a_addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            30,
            vec!["users".to_string()],
            &identity,
        )
        .await
        .unwrap();
        // The cert is valid regardless; the dead id-map host surfaces as
        // a warning, not a failure.
        assert!(!issued.warnings.is_empty());
        assert!(issued.warnings[0].contains("id-map registration"));
    }

    #[tokio::test]
    async fn enroll_over_the_wire_and_peer_recording() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (a_addr, a_state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(a_addr, NodeKind::ConfServer).await.unwrap();
        let new_listen: SocketAddr = "192.168.0.42:4565".parse().unwrap();
        let issued = conf_client::enroll(
            a_addr,
            "alice",
            Zeroizing::new("apw".to_string()),
            new_listen,
            &identity,
        )
        .await
        .unwrap();
        // The leaf carries the reserved SAN and chains to the confirmed
        // CA (verify_issued_leaf inside enroll already checked both, but
        // assert the SAN end-to-end here too).
        let cert = openssl::x509::X509::from_pem(issued.cert_pem.as_bytes()).unwrap();
        let san = cert.subject_alt_names().unwrap();
        assert!(san.iter().any(|n| n.dnsname() == Some(SERVING_SAN)));
        // The CA recorded the enrollee as a peer (and serves it in
        // GetInfo, making the CA host the well-known walk seed).
        assert!(a_state.cfg.lock().peers.contains(&new_listen));
        let info =
            conf_client::get_info(a_addr, NodeKind::Client, &identity).await.unwrap();
        assert!(info.peers.contains(&new_listen));
    }

    #[tokio::test]
    async fn enroll_without_the_bit_is_refused_over_the_wire() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // bob may sign but not enroll.
        ca_vault::add_admin(
            dir.path(),
            "apw",
            "bob",
            "bpw",
            Policy {
                allowed_san: vec!["*".to_string()],
                max_validity_days: 30,
                id_map_groups: vec![],
                may_enroll_servers: false,
            },
        )
        .unwrap();
        let (a_addr, a_state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(a_addr, NodeKind::ConfServer).await.unwrap();
        let new_listen: SocketAddr = "192.168.0.43:4565".parse().unwrap();
        let err = conf_client::enroll(
            a_addr,
            "bob",
            Zeroizing::new("bpw".to_string()),
            new_listen,
            &identity,
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("may not enroll"));
        // A refused enrollment must not record the peer.
        assert!(!a_state.cfg.lock().peers.contains(&new_listen));
    }

    /// Hand-rolled AddIdentity sender with NO client certificate — what
    /// an unauthenticated (or merely CA-issued-but-not-conf-server)
    /// peer looks like on the wire.
    async fn send_add_identity_no_cert(
        addr: SocketAddr,
        req: &AddIdentityRequest,
    ) -> AddIdentityResponse {
        use rustls_pki_types::ServerName;
        use tokio_rustls::TlsConnector;
        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(TofuVerifier::new(provider)))
            .with_no_client_auth();
        let tcp = TcpStream::connect(addr).await.unwrap();
        let mut tls = TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from(SERVING_SAN).unwrap(), tcp)
            .await
            .unwrap();
        conf_proto::write_msg(
            &mut tls,
            &ClientHello {
                protocol_version: PROTOCOL_VERSION,
                kind: NodeKind::ConfServer,
            },
        )
        .await
        .unwrap();
        let _: ServerHello = conf_proto::read_msg(&mut tls).await.unwrap();
        conf_proto::write_msg(&mut tls, &Request::AddIdentity(req.clone())).await.unwrap();
        conf_proto::read_msg(&mut tls).await.unwrap()
    }

    #[tokio::test]
    async fn add_identity_requires_a_conf_server_peer_cert() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let map_path = dir.path().join("id-map.json");
        let roles = Roles {
            ca: None,
            resolver: None,
            id_map: Some(IdMapRole { map: map_path.clone() }),
        };
        let (b_addr, _b) = spawn_server_with(dir.path(), roles, vec![]).await;
        let req = AddIdentityRequest {
            san: "mallory.ryu-oh.org".to_string(),
            primary_group: "users".to_string(),
            groups: vec![],
        };
        // 1. No client cert at all.
        match send_add_identity_no_cert(b_addr, &req).await {
            AddIdentityResponse::Err { reason } => {
                assert!(reason.contains("peer certificate"), "got: {reason}")
            }
            AddIdentityResponse::Ok { .. } => panic!("unauthenticated AddIdentity accepted"),
        }
        // 2. A CA-issued client cert that is NOT a conf-server serving
        //    cert (an ordinary node identity) must also be refused —
        //    holding *some* cert from the CA is not authority to edit
        //    the id-map.
        let (eve_chain, eve_key) = issue_client_cert(dir.path(), "eve.ryu-oh.org");
        let mut roots = rustls::RootCertStore::empty();
        let ca_pem = std::fs::read(dir.path().join("certificate.pem")).unwrap();
        for der in rustls_pemfile::certs(&mut std::io::Cursor::new(&ca_pem)) {
            roots.add(der.unwrap()).unwrap();
        }
        let err = conf_client::push_identity(b_addr, &eve_chain, &eve_key, roots, &req)
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(format!("{err:#}").contains("peer certificate"), "got: {err:#}");
        // Nothing was registered either way.
        assert!(!map_path.exists());
    }

    #[tokio::test]
    async fn queued_enrollment_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let map_path = dir.path().join("id-map.json");
        // One server wearing both hats: signs the queue AND hosts the
        // id-map the approval registers into.
        let roles = Roles {
            ca: Some(CaRole { dir: dir.path().to_path_buf(), autorenew: None }),
            resolver: None,
            id_map: Some(IdMapRole { map: map_path.clone() }),
        };
        let (addr, _state) = spawn_server_with(dir.path(), roles, vec![]).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        // Enrollee queues (no credentials) and starts polling.
        let pending = conf_client::enqueue(
            addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            30,
            &identity,
        )
        .await
        .unwrap();
        assert!(matches!(
            conf_client::poll(addr, NodeKind::Workstation, &pending, &identity)
                .await
                .unwrap(),
            conf_client::PollOutcome::Pending
        ));
        // Admin lists the queue and matches the request code — computed
        // locally from the queued CSR, equal to the one the enrollee
        // displays.
        let queue = conf_client::list_queue(addr, "alice", "apw", &identity)
            .await
            .unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].requested_name, "eric.ryu-oh.org");
        assert_eq!(
            conf_client::csr_fingerprint(&queue[0].csr_pem).unwrap(),
            pending.fingerprint,
            "admin-side and enrollee-side request codes must agree",
        );
        // Admin approves, choosing the groups at approval time.
        let warnings = conf_client::approve(
            addr,
            "alice",
            "apw",
            &queue[0].id,
            vec!["users".to_string()],
            &identity,
        )
        .await
        .unwrap();
        assert!(warnings.is_empty(), "local id-map push should succeed: {warnings:?}");
        // The waiting enrollee's next poll delivers the verified cert.
        let issued = match conf_client::poll(addr, NodeKind::Workstation, &pending, &identity)
            .await
            .unwrap()
        {
            conf_client::PollOutcome::Issued(i) => i,
            _ => panic!("expected Issued after approval"),
        };
        let cert = openssl::x509::X509::from_pem(issued.cert_pem.as_bytes()).unwrap();
        let san = cert.subject_alt_names().unwrap();
        assert!(san.iter().any(|n| n.dnsname() == Some("eric.ryu-oh.org")));
        // The approval registered the identity with the chosen groups.
        let map = crate::id_map::load(&map_path).unwrap();
        let ident = map.lookup_by_name("eric.ryu-oh.org").expect("identity registered");
        assert_eq!(ident.primary_group.as_str(), "users");
        // Re-polls stay Issued (idempotent); double-approve is refused.
        assert!(matches!(
            conf_client::poll(addr, NodeKind::Workstation, &pending, &identity)
                .await
                .unwrap(),
            conf_client::PollOutcome::Issued(_)
        ));
        let err =
            conf_client::approve(addr, "alice", "apw", &queue[0].id, vec![], &identity)
                .await
                .map(|_| ())
                .unwrap_err();
        assert!(format!("{err:#}").contains("already approved"));
        // The audit trail says approve, not sign.
        let log = std::fs::read_to_string(dir.path().join("audit.log")).unwrap();
        assert!(log.contains("op=approve"));
    }

    /// Auto-renewal in-process: with an `autorenew` slot present, one
    /// sweep approves a pending **verified renewal** but leaves an
    /// ordinary pending request for a human — the exact split the
    /// separate autorenew daemon enforced, now inside the conf server. The
    /// slot's empty policy is a second line of defense: it would refuse a
    /// non-renewal even if the filter let one through.
    #[test]
    fn autorenew_sweep_approves_only_verified_renewals() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // The dedicated empty-scope slot. The daemon needs only the vault
        // admin + its password; the keytab/TPM plumbing lives in the CLI.
        ca_vault::add_admin(
            dir.path(),
            "apw",
            AUTORENEW_ADMIN,
            "renew-secret",
            Policy {
                allowed_san: vec![],
                max_validity_days: 730,
                id_map_groups: vec![],
                may_enroll_servers: false,
            },
        )
        .unwrap();
        // A verified renewal and an ordinary new request, both pending.
        let renew = conf_client::generate_key_and_csr("host.ryu-oh.org").unwrap();
        let fresh = conf_client::generate_key_and_csr("newcomer.ryu-oh.org").unwrap();
        let renew_req = ca_store::QueuedReq::new(
            NodeKind::Workstation,
            renew.csr_pem,
            "host.ryu-oh.org".to_string(),
            30,
            "test".to_string(),
            true,
            None,
        );
        let fresh_req = ca_store::QueuedReq::new(
            NodeKind::Workstation,
            fresh.csr_pem,
            "newcomer.ryu-oh.org".to_string(),
            30,
            "test".to_string(),
            false,
            None,
        );
        let renew_id = renew_req.id.clone();
        let fresh_id = fresh_req.id.clone();
        ca_store::enqueue(dir.path(), &renew_req).unwrap();
        ca_store::enqueue(dir.path(), &fresh_req).unwrap();

        let approved = autorenew_sweep(&issuer(dir.path()), dir.path(), "renew-secret");
        assert_eq!(approved, 1, "only the verified renewal is auto-approved");
        assert!(
            matches!(
                ca_store::status(dir.path(), &renew_id).unwrap(),
                ca_store::Status::Signed(_)
            ),
            "the verified renewal should now be signed",
        );
        assert!(
            matches!(
                ca_store::status(dir.path(), &fresh_id).unwrap(),
                ca_store::Status::Pending(_)
            ),
            "the ordinary request must still wait for a human",
        );
        // Audited as a renewal by the autorenew admin — the same trail
        // the separate autorenew daemon's approvals left (a verified
        // renewal signs through the `op=renew` continuation gate).
        let log = std::fs::read_to_string(dir.path().join("audit.log")).unwrap();
        assert!(log.contains("op=renew"));
        assert!(log.contains(&format!("admin={AUTORENEW_ADMIN}")));
    }

    /// Approve and deny are mutually exclusive terminal transitions: once
    /// one lands, the other is refused and no second sidecar appears. The
    /// queue lock + under-lock status recheck is what enforces this.
    #[tokio::test]
    async fn approve_and_deny_are_mutually_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();

        // Approve first, then a deny is refused; only the signed sidecar exists.
        let a = conf_client::enqueue(
            addr,
            NodeKind::Workstation,
            "approved.ryu-oh.org",
            30,
            &identity,
        )
        .await
        .unwrap();
        conf_client::approve(addr, "alice", "apw", &a.request_id, vec![], &identity)
            .await
            .unwrap();
        let derr = conf_client::deny(addr, "alice", "apw", &a.request_id, "no", &identity)
            .await
            .unwrap_err();
        assert!(format!("{derr:#}").contains("already approved"), "got: {derr:#}");
        assert!(matches!(
            ca_store::status(dir.path(), &a.request_id).unwrap(),
            ca_store::Status::Signed(_)
        ));

        // Deny first, then an approve is refused.
        let b = conf_client::enqueue(
            addr,
            NodeKind::Workstation,
            "denied.ryu-oh.org",
            30,
            &identity,
        )
        .await
        .unwrap();
        conf_client::deny(addr, "alice", "apw", &b.request_id, "nope", &identity)
            .await
            .unwrap();
        let aerr =
            conf_client::approve(addr, "alice", "apw", &b.request_id, vec![], &identity)
                .await
                .map(|_| ())
                .unwrap_err();
        assert!(format!("{aerr:#}").contains("already denied"), "got: {aerr:#}");
        assert!(matches!(
            ca_store::status(dir.path(), &b.request_id).unwrap(),
            ca_store::Status::Denied(_)
        ));
    }

    /// A second conf server enrolls with no admin at its keyboard: the
    /// enrollment queues, the admin approves remotely (their
    /// `may_enroll_servers` is the gate), the poll delivers a
    /// reserved-SAN serving cert, and the CA records the new server as
    /// a peer — everything the synchronous Enroll does, minus the
    /// password-at-the-keyboard requirement.
    #[tokio::test]
    async fn queued_server_enrollment_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::ConfServer).await.unwrap();
        let listen: SocketAddr = "10.0.0.9:4565".parse().unwrap();
        let pending =
            conf_client::enqueue_enroll(addr, listen, &identity).await.unwrap();
        assert!(matches!(
            conf_client::poll(addr, NodeKind::ConfServer, &pending, &identity)
                .await
                .unwrap(),
            conf_client::PollOutcome::Pending
        ));
        // The admin's list shows what this really is — an enrollment at
        // a stated address, not a user cert — and the request code
        // matches the enrollee's.
        let queue =
            conf_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].requested_name, SERVING_SAN);
        assert_eq!(queue[0].enroll_listen, Some(listen));
        assert_eq!(
            conf_client::csr_fingerprint(&queue[0].csr_pem).unwrap(),
            pending.fingerprint,
        );
        conf_client::approve(addr, "alice", "apw", &queue[0].id, vec![], &identity)
            .await
            .unwrap();
        let issued = match conf_client::poll(
            addr,
            NodeKind::ConfServer,
            &pending,
            &identity,
        )
        .await
        .unwrap()
        {
            conf_client::PollOutcome::Issued(i) => i,
            _ => panic!("expected Issued after approval"),
        };
        let cert = openssl::x509::X509::from_pem(issued.cert_pem.as_bytes()).unwrap();
        let san = cert.subject_alt_names().unwrap();
        assert!(san.iter().any(|n| n.dnsname() == Some(SERVING_SAN)));
        // The CA now knows the new conf server as a peer (the start of
        // future installs' peer walks).
        assert!(state.cfg.lock().peers.contains(&listen));
        let log = std::fs::read_to_string(dir.path().join("audit.log")).unwrap();
        assert!(log.contains("op=enroll"));
    }

    /// The `may_enroll_servers` gate binds to the *approving* admin: an
    /// admin without it can approve user certs all day but cannot mint
    /// a conf server; the entry stays pending for someone who can.
    #[tokio::test]
    async fn enrollment_approval_requires_the_policy_bit() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let restricted = Policy { may_enroll_servers: false, ..policy() };
        ca_vault::add_admin(dir.path(), "apw", "bob", "bpw", restricted).unwrap();
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::ConfServer).await.unwrap();
        let listen: SocketAddr = "10.0.0.10:4565".parse().unwrap();
        let pending =
            conf_client::enqueue_enroll(addr, listen, &identity).await.unwrap();
        let queue =
            conf_client::list_queue(addr, "bob", "bpw", &identity).await.unwrap();
        let err =
            conf_client::approve(addr, "bob", "bpw", &queue[0].id, vec![], &identity)
                .await
                .map(|_| ())
                .unwrap_err();
        assert!(format!("{err:#}").contains("may not enroll"));
        // Still pending — bob's failed approval consumed nothing.
        assert!(matches!(
            conf_client::poll(addr, NodeKind::ConfServer, &pending, &identity)
                .await
                .unwrap(),
            conf_client::PollOutcome::Pending
        ));
        conf_client::approve(addr, "alice", "apw", &queue[0].id, vec![], &identity)
            .await
            .unwrap();
        assert!(matches!(
            conf_client::poll(addr, NodeKind::ConfServer, &pending, &identity)
                .await
                .unwrap(),
            conf_client::PollOutcome::Issued(_)
        ));
    }

    #[tokio::test]
    async fn denied_requests_reach_the_enrollee() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        let pending = conf_client::enqueue(
            addr,
            NodeKind::Workstation,
            "mallory.ryu-oh.org",
            30,
            &identity,
        )
        .await
        .unwrap();
        let queue =
            conf_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
        conf_client::deny(
            addr,
            "alice",
            "apw",
            &queue[0].id,
            "request code mismatch",
            &identity,
        )
        .await
        .unwrap();
        match conf_client::poll(addr, NodeKind::Workstation, &pending, &identity)
            .await
            .unwrap()
        {
            conf_client::PollOutcome::Denied(reason) => {
                assert!(reason.contains("mismatch"))
            }
            _ => panic!("expected Denied"),
        }
    }

    #[tokio::test]
    async fn queue_ops_require_admin_auth_and_respect_policy() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path()); // policy: allowed groups = ["users"]
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        let pending = conf_client::enqueue(
            addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            30,
            &identity,
        )
        .await
        .unwrap();
        // Wrong password: list and approve both refuse.
        let err = conf_client::list_queue(addr, "alice", "WRONG", &identity)
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(format!("{err:#}").contains("authentication"));
        let err = conf_client::approve(
            addr,
            "alice",
            "WRONG",
            &pending.request_id,
            vec![],
            &identity,
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("authentication"));
        // A disallowed group refuses the approval — and the request
        // stays pending, so the admin can retry with allowed groups.
        let err = conf_client::approve(
            addr,
            "alice",
            "apw",
            &pending.request_id,
            vec!["wheel".to_string()],
            &identity,
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("not permitted"));
        assert!(matches!(
            conf_client::poll(addr, NodeKind::Workstation, &pending, &identity)
                .await
                .unwrap(),
            conf_client::PollOutcome::Pending
        ));
        conf_client::approve(
            addr,
            "alice",
            "apw",
            &pending.request_id,
            vec!["users".to_string()],
            &identity,
        )
        .await
        .unwrap();
        assert!(matches!(
            conf_client::poll(addr, NodeKind::Workstation, &pending, &identity)
                .await
                .unwrap(),
            conf_client::PollOutcome::Issued(_)
        ));
    }

    #[tokio::test]
    async fn enqueue_rejects_the_reserved_name() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::ConfServer).await.unwrap();
        let err =
            conf_client::enqueue(addr, NodeKind::ConfServer, SERVING_SAN, 30, &identity)
                .await
                .map(|_| ())
                .unwrap_err();
        assert!(format!("{err:#}").contains("reserved"));
    }

    #[test]
    fn one_live_certificate_per_name() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let iss = issuer(dir.path());
        let req = request("eric.ryu-oh.org", "alice", "apw", 30);
        assert!(matches!(
            handle_sign_request(&iss, dir.path(), &req).resp,
            SignResponse::Ok { .. }
        ));
        // Same name again: refused while the first cert lives — the
        // impostor race on an existing identity is closed.
        let req2 = request("eric.ryu-oh.org", "alice", "apw", 30);
        match handle_sign_request(&iss, dir.path(), &req2).resp {
            SignResponse::Err { reason } => {
                assert!(reason.contains("already exists"), "got: {reason}")
            }
            SignResponse::Ok { .. } => panic!("duplicate name was signed"),
        }
        // Revoking clears the way — the rebuilt-laptop flow.
        let serial =
            ca_store::live_for_name(dir.path(), "eric.ryu-oh.org").unwrap()[0].serial;
        ca_store::revoke(
            dir.path(),
            serial,
            ca_store::Revocation {
                serial,
                revoked_unix: ca_store::now_unix(),
                reason: "laptop rebuilt".into(),
            },
        )
        .unwrap();
        let req3 = request("eric.ryu-oh.org", "alice", "apw", 30);
        assert!(matches!(
            handle_sign_request(&iss, dir.path(), &req3).resp,
            SignResponse::Ok { .. }
        ));
    }

    #[tokio::test]
    async fn enqueue_refuses_a_live_name() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let req = request("eric.ryu-oh.org", "alice", "apw", 30);
        assert!(matches!(
            handle_sign_request(&issuer(dir.path()), dir.path(), &req).resp,
            SignResponse::Ok { .. }
        ));
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        let err = conf_client::enqueue(
            addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            30,
            &identity,
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("already exists"), "got: {err:#}");
    }

    #[tokio::test]
    async fn crl_round_trips_over_the_wire() {
        use x509_parser::prelude::{CertificateRevocationList, FromDer};
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        // Nothing revoked yet ⇒ no CRL.
        assert!(conf_client::get_crl(addr, NodeKind::Client, &identity)
            .await
            .unwrap()
            .is_none());
        // Sign a victim, revoke it, sign the CRL with the vault key.
        let req = request("victim.ryu-oh.org", "alice", "apw", 30);
        assert!(matches!(
            handle_sign_request(&issuer(dir.path()), dir.path(), &req).resp,
            SignResponse::Ok { .. }
        ));
        let serial =
            ca_store::live_for_name(dir.path(), "victim.ryu-oh.org").unwrap()[0].serial;
        ca_store::revoke(
            dir.path(),
            serial,
            ca_store::Revocation {
                serial,
                revoked_unix: ca_store::now_unix(),
                reason: "test".into(),
            },
        )
        .unwrap();
        let unlocked = ca_vault::unlock(dir.path(), "apw").unwrap();
        crate::ca_index::write_crl(dir.path(), &unlocked.ca_key_pem).unwrap();
        // The daemon serves it; it parses; the revoked serial is on it;
        // and it is genuinely signed by the CA.
        let pem = conf_client::get_crl(addr, NodeKind::Client, &identity)
            .await
            .unwrap()
            .expect("a CRL after the first revocation");
        let der = rustls_pemfile::crls(&mut std::io::Cursor::new(pem.as_bytes()))
            .next()
            .unwrap()
            .unwrap();
        let (_, crl) = CertificateRevocationList::from_der(der.as_ref()).unwrap();
        let serials: Vec<u64> = crl
            .iter_revoked_certificates()
            .map(|rc| {
                rc.user_certificate
                    .to_string()
                    .parse::<u64>()
                    .expect("small test serials fit in u64")
            })
            .collect();
        assert_eq!(serials, vec![serial]);
        let ca_pem = std::fs::read(dir.path().join("certificate.pem")).unwrap();
        let ca_der = rustls_pemfile::certs(&mut std::io::Cursor::new(&ca_pem))
            .next()
            .unwrap()
            .unwrap();
        let (_, ca_cert) =
            x509_parser::prelude::X509Certificate::from_der(ca_der.as_ref()).unwrap();
        crl.verify_signature(ca_cert.public_key())
            .expect("CRL must verify against the CA");
        // nextUpdate is ~CRL_VALIDITY out.
        let nu = crate::ca_index::crl_next_update(&crate::ca_index::crl_path(dir.path()))
            .unwrap()
            .unwrap();
        let expect = ca_store::now_unix() + crate::ca_index::CRL_VALIDITY.as_secs();
        assert!(nu.abs_diff(expect) < 3600, "nextUpdate {nu} vs expected {expect}");
    }

    #[tokio::test]
    async fn verified_renewal_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // An autorenew-shaped admin: empty issuance scope. It must be
        // able to approve *renewals* and nothing else.
        ca_vault::add_admin(
            dir.path(),
            "apw",
            "bot",
            "botpw",
            Policy {
                allowed_san: vec![],
                max_validity_days: 730,
                id_map_groups: vec![],
                may_enroll_servers: false,
            },
        )
        .unwrap();
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        // 1. Initial enrollment (the trust ceremony happened here).
        let issued = conf_client::request_cert(
            addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            30,
            vec![],
            &identity,
        )
        .await
        .unwrap();
        // 2. Renewal: enqueue over a connection authenticated by the
        //    live cert — fully unattended, PKI-verified, no TOFU.
        let mut roots = RootCertStore::empty();
        for der in
            rustls_pemfile::certs(&mut std::io::Cursor::new(issued.trusted_pem.as_bytes()))
        {
            roots.add(der.unwrap()).unwrap();
        }
        let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(
            issued.private_key_pem.as_bytes(),
        ))
        .unwrap()
        .unwrap();
        let pending = conf_client::enqueue_renewal(
            addr,
            NodeKind::Client,
            "eric.ryu-oh.org",
            30,
            issued.cert_pem.as_bytes(),
            key,
            roots.clone(),
        )
        .await
        .unwrap();
        // 3. The server marked it verified — proof of possession of the
        //    live key, checked against the issuance index.
        let q = conf_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
        assert_eq!(q.len(), 1);
        assert!(q[0].verified_renewal, "renewal must be marked verified");
        // 4. The empty-scope bot approves it — renewals skip the SAN
        //    globs (the name was approved at enrollment; this is
        //    continuation).
        conf_client::approve(addr, "bot", "botpw", &pending.request_id, vec![], &identity)
            .await
            .unwrap();
        let renewed = match conf_client::poll_renewal(
            addr,
            NodeKind::Client,
            &pending,
            &issued.trusted_pem,
            roots.clone(),
        )
        .await
        .unwrap()
        {
            conf_client::PollOutcome::Issued(i) => i,
            _ => panic!("expected the renewed cert"),
        };
        assert_ne!(renewed.cert_pem, issued.cert_pem, "fresh cert (and fresh key)");
        let cert = openssl::x509::X509::from_pem(renewed.cert_pem.as_bytes()).unwrap();
        let san = cert.subject_alt_names().unwrap();
        assert!(san.iter().any(|n| n.dnsname() == Some("eric.ryu-oh.org")));
        // Both generations are live in the index until revoked/expired.
        assert_eq!(
            ca_store::live_for_name(dir.path(), "eric.ryu-oh.org").unwrap().len(),
            2
        );
        // The audit trail distinguishes renewals.
        let log = std::fs::read_to_string(dir.path().join("audit.log")).unwrap();
        assert!(log.contains("op=renew"));
        // 5. The bot CANNOT approve a *new* identity: queue one without
        //    a client cert and watch the empty SAN scope refuse it.
        let new_req = conf_client::enqueue(
            addr,
            NodeKind::Workstation,
            "bob.ryu-oh.org",
            30,
            &identity,
        )
        .await
        .unwrap();
        let err = conf_client::approve(
            addr,
            "bot",
            "botpw",
            &new_req.request_id,
            vec![],
            &identity,
        )
        .await
        .map(|_| ())
        .unwrap_err();
        assert!(format!("{err:#}").contains("not permitted"), "got: {err:#}");
    }

    #[tokio::test]
    async fn the_serving_cert_renews_itself_and_the_chain_rebuilds() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::ConfServer).await.unwrap();
        // Renew the daemon's own serving identity, authenticated by the
        // serving chain itself. The reserved name is legitimate here —
        // possession of the live serving key IS the authority.
        let mut roots = RootCertStore::empty();
        let ca_pem = std::fs::read(dir.path().join("certificate.pem")).unwrap();
        for der in rustls_pemfile::certs(&mut std::io::Cursor::new(&ca_pem)) {
            roots.add(der.unwrap()).unwrap();
        }
        let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(
            state.serving_key_pem.as_slice(),
        ))
        .unwrap()
        .unwrap();
        let pending = conf_client::enqueue_renewal(
            addr,
            NodeKind::ConfServer,
            SERVING_SAN,
            365,
            &state.serving_cert_pem,
            key,
            roots.clone(),
        )
        .await
        .unwrap();
        let q = conf_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
        assert!(q[0].verified_renewal, "serving-cert renewal must verify");
        conf_client::approve(addr, "alice", "apw", &pending.request_id, vec![], &identity)
            .await
            .unwrap();
        let installed_pem = std::str::from_utf8(&ca_pem).unwrap();
        let renewed = match conf_client::poll_renewal(
            addr,
            NodeKind::ConfServer,
            &pending,
            installed_pem,
            roots,
        )
        .await
        .unwrap()
        {
            conf_client::PollOutcome::Issued(i) => i,
            _ => panic!("expected the renewed serving cert"),
        };
        // The chain rebuild: leaf + the issuing CA from the returned
        // bundle parses back as a ≥2-cert chain — what `split_chain`
        // on every future enrollee requires.
        let ca = conf_client::issuing_ca_pem(&renewed.trusted_pem, &renewed.cert_pem)
            .unwrap();
        let chain = format!("{}{}", renewed.cert_pem, ca);
        let n = rustls_pemfile::certs(&mut std::io::Cursor::new(chain.as_bytes()))
            .flatten()
            .count();
        assert_eq!(n, 2, "serving chain must be [leaf, ca]");
    }

    #[tokio::test]
    async fn a_revoked_certificate_cannot_self_renew() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            conf_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        let issued = conf_client::request_cert(
            addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            30,
            vec![],
            &identity,
        )
        .await
        .unwrap();
        // Revoke it (e.g. the laptop was stolen).
        let serial =
            ca_store::live_for_name(dir.path(), "eric.ryu-oh.org").unwrap()[0].serial;
        ca_store::revoke(
            dir.path(),
            serial,
            ca_store::Revocation {
                serial,
                revoked_unix: ca_store::now_unix(),
                reason: "stolen".into(),
            },
        )
        .unwrap();
        // The thief still holds cert+key; the TLS handshake may even
        // succeed (this test installs no CRL beside the server's trust
        // bundle) — but the index says the serial is dead, so the
        // request is NOT a verified renewal, and with no live cert for
        // the name it falls through to the normal queue: glyph-gated,
        // admin's eyes open.
        let mut roots = RootCertStore::empty();
        for der in
            rustls_pemfile::certs(&mut std::io::Cursor::new(issued.trusted_pem.as_bytes()))
        {
            roots.add(der.unwrap()).unwrap();
        }
        let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(
            issued.private_key_pem.as_bytes(),
        ))
        .unwrap()
        .unwrap();
        conf_client::enqueue_renewal(
            addr,
            NodeKind::Client,
            "eric.ryu-oh.org",
            30,
            issued.cert_pem.as_bytes(),
            key,
            roots,
        )
        .await
        .unwrap();
        let q = conf_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
        assert_eq!(q.len(), 1);
        assert!(
            !q[0].verified_renewal,
            "a revoked serial must not produce a verified renewal"
        );
    }

    #[test]
    fn resolver_info_maps_data_plane_auth() {
        use netidx::resolver_server::config::file as rfile;
        let dir = tempfile::tempdir().unwrap();
        let write_cfg = |auth: rfile::Auth, name: &str| -> PathBuf {
            let member = rfile::MemberServerBuilder::default()
                .addr("192.168.0.7:4564".parse::<SocketAddr>().unwrap())
                .bind_addr("192.168.0.7".parse::<std::net::IpAddr>().unwrap())
                .auth(auth)
                .build()
                .unwrap();
            let cfg = rfile::ConfigBuilder::default()
                .member_servers(vec![member])
                .build()
                .unwrap();
            let path = dir.path().join(name);
            std::fs::write(&path, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();
            path
        };
        // TLS: the advertised auth carries the cert's name.
        let p = write_cfg(
            rfile::Auth::Tls {
                name: "resolver.ryu-oh.org".into(),
                trusted: "/x/trusted.pem".into(),
                certificate: "/x/cert.pem".into(),
                private_key: "/x/key.pem".into(),
            },
            "tls.json",
        );
        let r = resolver_info(&p).unwrap().unwrap();
        assert_eq!(r.addr, "192.168.0.7:4564".parse::<SocketAddr>().unwrap());
        assert_eq!(r.auth, InfoAuth::Tls { name: "resolver.ryu-oh.org".to_string() });
        // Krb5: the SPN rides along — a krb5 data plane is first-class.
        let p = write_cfg(rfile::Auth::Krb5("svc/resolver@REALM".into()), "krb5.json");
        let r = resolver_info(&p).unwrap().unwrap();
        assert_eq!(r.auth, InfoAuth::Krb5 { spn: "svc/resolver@REALM".to_string() });
        // Anonymous advertises as such; Local is host-only — nothing to
        // advertise.
        let p = write_cfg(rfile::Auth::Anonymous, "anon.json");
        assert_eq!(resolver_info(&p).unwrap().unwrap().auth, InfoAuth::Anonymous);
        let p = write_cfg(rfile::Auth::Local("/run/x.sock".into()), "local.json");
        assert!(resolver_info(&p).unwrap().is_none());
    }
}
