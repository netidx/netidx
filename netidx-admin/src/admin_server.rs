//! Admin server: the per-host daemon behind discovery-driven setup. It
//! answers [`Request::GetInfo`] with this host's local facts + known
//! peers, and — on the host holding the CA — turns a join client's
//! [`Request::Sign`] into a signed cert and a [`Request::Enroll`] into
//! a new admin server's reserved-SAN serving cert. After a successful
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
    admin_client,
    admin_proto::{
        self, AddIdentityRequest, AddIdentityResponse, AddRoleAdminRequest,
        AdminListResponse, AdminMgmtResponse, ApplyControllerStateRequest,
        ApplyControllerStateResponse, ApplyCrlRequest, ApplyCrlResponse,
        ApplyPermsEditRequest, ApplyPermsEditResponse, ApplyReferralEditRequest,
        ApplyReferralEditResponse, ApplyServiceControlRequest,
        ApplyServiceControlResponse, ApproveDelegationRequest, ApproveDelegationResponse,
        ApproveRequest, ApproveResponse, BackupResponse, ClientHello,
        ControlServiceRequest, ControlServiceResponse, DelegationEntry,
        DelegationPollResponse, DelegationRequest, DelegationResponse,
        DenyDelegationRequest, DenyDelegationResponse, DenyRequest, DenyResponse,
        EditPermsRequest, EditPermsResponse, EnqueueRequest, EnqueueResponse,
        EnrollRequest, GetCrlResponse, GetInfoResponse, GetMapResponse,
        GetMapVersionResponse, GetPermsResponse, InfoAuth, IssuedEntry,
        ListAdminsRequest, ListDelegationsRequest, ListDelegationsResponse,
        ListIssuedRequest, ListIssuedResponse, ListQueueRequest, ListQueueResponse,
        NetworkMap, NodeKind, PROTOCOL_VERSION, PeerResult, PollRequest, PollResponse,
        QueueEntry, ReadPermsRequest, ReadPermsResponse, ReconcileControllerResponse,
        ReferralEdit, RegisterRequest, RegisterResponse, RemoveAdminRequest,
        RemoveServerRequest, RemoveServerResponse, Request, ResolverAddr, RevokeRequest,
        RevokeResponse, Role, RotateAutorenewResponse, RotateRecoveryResponse,
        SERVING_SAN, Secret, ServerEntry, ServerHello, ServiceUnit, ServiceUnitDef,
        SetAdminPolicyRequest, SignRequest, SignResponse,
    },
    admin_server_config::AdminServerConfig,
    ca::{Ca, SanEntry},
    ca_store, ca_vault, delegation_store, discovery, id_map, netmap,
};
use anyhow::{Context, Result, anyhow, bail};
use futures::{StreamExt, stream};
use globset::Glob;
use log::{debug, error, info, warn};
use parking_lot::{Mutex, RwLock};
use rustls::{
    RootCertStore, ServerConfig as RustlsServerConfig, server::WebPkiClientVerifier,
};
use rustls_pki_types::CertificateDer;
use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap, VecDeque},
    fs::OpenOptions,
    io::Write,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream, UnixListener},
    sync::Semaphore,
};
use tokio_rustls::TlsAcceptor;
use zeroize::Zeroizing;

/// Max simultaneous connections. These are cheap (a TLS handshake and a
/// few small messages), so this can be generous — it just bounds socket
/// / task fan-out.
const MAX_CONNECTIONS: usize = 768;

/// Max simultaneous Argon2/signing operations. The per-source password limiter
/// below is the normal brute-force/resource-exhaustion defence; this semaphore
/// is the hard global memory ceiling for genuinely distributed traffic.
const MAX_CONCURRENT_SIGNS: usize = 64;

/// Failed password attempts remain in the source's sliding window this long.
const PASSWORD_FAILURE_WINDOW: Duration = Duration::from_secs(10 * 60);
/// Linear backoff: N recent failures require N seconds before the next KDF.
const PASSWORD_BACKOFF_STEP: Duration = Duration::from_secs(1);
/// Bound unauthenticated source tracking. At the connection limit there can be
/// at most 768 live entries; the rest are recent failures retained for the
/// sliding window. Eviction affects throttling only, never authentication.
const PASSWORD_LIMIT_MAX_SOURCES: usize = 16_384;

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

/// The dedicated auto-renewal admin: a vault slot with an empty issuance
/// policy whose only over-the-wire power is approving verified renewals.
/// When [`CaRole::autorenew`](crate::admin_server_config::CaRole) names its
/// keytab, the daemon authenticates as this slot to approve renewals
/// in-process — the same narrow principal the separate `admin ca auto-approve`
/// process used to be, now without the extra process.
pub const AUTORENEW_ADMIN: &str = "autorenew";

/// How often the in-process autorenew approver scans the queue. Renewals
/// are submitted by the per-host `renewd` on a multi-hour cadence and are
/// not latency-sensitive, so a slow poll is plenty.
const AUTORENEW_POLL: Duration = Duration::from_secs(60);

/// Source key for password throttling. IPv4 is keyed by address; IPv6 is
/// keyed by /64 so rotating privacy/interface addresses cannot bypass the
/// limiter cheaply.
fn password_source(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(ip) => IpAddr::V4(ip),
        IpAddr::V6(ip) => {
            let mut octets = ip.octets();
            octets[8..].fill(0);
            IpAddr::V6(Ipv6Addr::from(octets))
        }
    }
}

#[derive(Debug)]
struct PasswordLimitEntry {
    failures: VecDeque<Instant>,
    in_flight: bool,
    last_seen: Instant,
}

#[derive(Debug, Default)]
struct PasswordLimiter {
    entries: Mutex<HashMap<IpAddr, PasswordLimitEntry>>,
}

impl PasswordLimiter {
    /// Reserve this source's one password attempt. The returned duration is
    /// slept asynchronously before acquiring the global Argon2 semaphore.
    fn reserve_at(&self, source: IpAddr, now: Instant) -> Result<Duration> {
        let source = password_source(source);
        let mut entries = self.entries.lock();
        if entries.len() >= PASSWORD_LIMIT_MAX_SOURCES && !entries.contains_key(&source) {
            entries.retain(|_, entry| {
                entry.in_flight
                    || now.saturating_duration_since(entry.last_seen)
                        < PASSWORD_FAILURE_WINDOW
            });
            if entries.len() >= PASSWORD_LIMIT_MAX_SOURCES
                && let Some(oldest) = entries
                    .iter()
                    .filter(|(_, entry)| !entry.in_flight)
                    .min_by_key(|(_, entry)| entry.last_seen)
                    .map(|(source, _)| *source)
            {
                entries.remove(&oldest);
            }
        }
        let entry = entries.entry(source).or_insert_with(|| PasswordLimitEntry {
            failures: VecDeque::new(),
            in_flight: false,
            last_seen: now,
        });
        while entry.failures.front().is_some_and(|failure| {
            now.saturating_duration_since(*failure) >= PASSWORD_FAILURE_WINDOW
        }) {
            entry.failures.pop_front();
        }
        if entry.in_flight {
            bail!("another password attempt from this source is already in progress");
        }
        let delay = entry
            .failures
            .back()
            .map(|last| {
                let penalty = PASSWORD_BACKOFF_STEP
                    .saturating_mul(entry.failures.len().min(u32::MAX as usize) as u32)
                    .min(PASSWORD_FAILURE_WINDOW);
                last.checked_add(penalty)
                    .map(|eligible| eligible.saturating_duration_since(now))
                    .unwrap_or(PASSWORD_FAILURE_WINDOW)
            })
            .unwrap_or(Duration::ZERO);
        entry.in_flight = true;
        entry.last_seen = now;
        Ok(delay)
    }

    fn complete_at(&self, source: IpAddr, success: Option<bool>, now: Instant) {
        let source = password_source(source);
        let mut entries = self.entries.lock();
        let Some(entry) = entries.get_mut(&source) else { return };
        entry.in_flight = false;
        entry.last_seen = now;
        while entry.failures.front().is_some_and(|failure| {
            now.saturating_duration_since(*failure) >= PASSWORD_FAILURE_WINDOW
        }) {
            entry.failures.pop_front();
        }
        if success == Some(false) {
            entry.failures.push_back(now);
        }
    }

    async fn begin(self: &Arc<Self>, source: IpAddr) -> Result<PasswordAttempt> {
        let delay = self.reserve_at(source, Instant::now())?;
        let attempt = PasswordAttempt(Arc::new(PasswordAttemptInner {
            limiter: self.clone(),
            source: password_source(source),
            finished: AtomicBool::new(false),
        }));
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        Ok(attempt)
    }
}

#[derive(Clone)]
struct PasswordAttempt(Arc<PasswordAttemptInner>);

struct PasswordAttemptInner {
    limiter: Arc<PasswordLimiter>,
    source: IpAddr,
    finished: AtomicBool,
}

impl PasswordAttempt {
    fn finish(&self, success: bool) {
        if !self.0.finished.swap(true, Ordering::AcqRel) {
            self.0.limiter.complete_at(self.0.source, Some(success), Instant::now());
        }
    }
}

impl Drop for PasswordAttemptInner {
    fn drop(&mut self) {
        if !self.finished.swap(true, Ordering::AcqRel) {
            self.limiter.complete_at(self.source, None, Instant::now());
        }
    }
}

tokio::task_local! {
    /// The network request's password reservation. `run_signing` copies it
    /// into the blocking thread where the actual vault authentication occurs.
    static REQUEST_PASSWORD_ATTEMPT: Option<PasswordAttempt>;
}

thread_local! {
    static CURRENT_PASSWORD_ATTEMPT: RefCell<Option<PasswordAttempt>> =
        const { RefCell::new(None) };
}

struct CurrentPasswordAttemptGuard(Option<PasswordAttempt>);

impl Drop for CurrentPasswordAttemptGuard {
    fn drop(&mut self) {
        let previous = self.0.take();
        CURRENT_PASSWORD_ATTEMPT.with(|slot| *slot.borrow_mut() = previous);
    }
}

fn with_password_attempt<T>(
    attempt: Option<PasswordAttempt>,
    f: impl FnOnce() -> T,
) -> T {
    let previous = CURRENT_PASSWORD_ATTEMPT.with(|slot| slot.replace(attempt));
    let _guard = CurrentPasswordAttemptGuard(previous);
    f()
}

fn record_password_auth_result(success: bool) {
    let attempt = CURRENT_PASSWORD_ATTEMPT.with(|slot| slot.borrow().clone());
    if let Some(attempt) = attempt {
        attempt.finish(success);
    }
}

#[cfg(test)]
mod password_limiter_tests {
    use super::*;

    #[test]
    fn failures_apply_linear_backoff_and_expire_on_the_sliding_window() {
        let limiter = PasswordLimiter::default();
        let source: IpAddr = "192.0.2.10".parse().unwrap();
        let start = Instant::now();

        assert_eq!(limiter.reserve_at(source, start).unwrap(), Duration::ZERO);
        assert!(limiter.reserve_at(source, start).is_err(), "only one KDF per IP");
        limiter.complete_at(source, Some(false), start);

        assert_eq!(limiter.reserve_at(source, start).unwrap(), Duration::from_secs(1));
        limiter.complete_at(source, Some(false), start + Duration::from_secs(1));
        assert_eq!(
            limiter.reserve_at(source, start + Duration::from_secs(1)).unwrap(),
            Duration::from_secs(2)
        );
        // A success releases the source but does not erase recent failures.
        limiter.complete_at(source, Some(true), start + Duration::from_secs(3));
        assert_eq!(
            limiter.reserve_at(source, start + Duration::from_secs(3)).unwrap(),
            Duration::ZERO
        );
        limiter.complete_at(source, Some(false), start + Duration::from_secs(3));
        assert_eq!(
            limiter.reserve_at(source, start + Duration::from_secs(3)).unwrap(),
            Duration::from_secs(3)
        );
        limiter.complete_at(source, Some(true), start + Duration::from_secs(6));

        let expired = start + PASSWORD_FAILURE_WINDOW + Duration::from_secs(4);
        assert_eq!(limiter.reserve_at(source, expired).unwrap(), Duration::ZERO);
        limiter.complete_at(source, Some(true), expired);
    }

    #[test]
    fn ipv6_privacy_addresses_share_a_64_bit_source_key() {
        let limiter = PasswordLimiter::default();
        let start = Instant::now();
        let first: IpAddr = "2001:db8:1234:5678::1".parse().unwrap();
        let same_64: IpAddr = "2001:db8:1234:5678:ffff::2".parse().unwrap();
        let other_64: IpAddr = "2001:db8:1234:5679::1".parse().unwrap();

        limiter.reserve_at(first, start).unwrap();
        assert!(limiter.reserve_at(same_64, start).is_err());
        assert_eq!(limiter.reserve_at(other_64, start).unwrap(), Duration::ZERO);
        limiter.complete_at(first, Some(true), start);
        limiter.complete_at(other_64, Some(true), start);
    }

    #[tokio::test]
    async fn abandoned_attempt_releases_the_source_without_recording_a_failure() {
        let limiter = Arc::new(PasswordLimiter::default());
        let source: IpAddr = "198.51.100.7".parse().unwrap();
        let attempt = limiter.begin(source).await.unwrap();
        drop(attempt);
        let next = limiter.begin(source).await.unwrap();
        next.finish(true);
    }

    #[tokio::test]
    async fn request_scope_reaches_the_blocking_authentication_task() {
        let limiter = Arc::new(PasswordLimiter::default());
        let source: IpAddr = "203.0.113.9".parse().unwrap();
        let attempt = limiter.begin(source).await.unwrap();
        let signs = Arc::new(Semaphore::new(1));
        REQUEST_PASSWORD_ATTEMPT
            .scope(Some(attempt), async {
                run_signing(&signs, || record_password_auth_result(false)).await.unwrap();
            })
            .await;

        let delay = limiter.reserve_at(source, Instant::now()).unwrap();
        assert!(
            delay > Duration::from_millis(800) && delay <= Duration::from_secs(1),
            "the blocking authentication failure must reach the source limiter: {delay:?}"
        );
        limiter.complete_at(source, Some(true), Instant::now() + delay);
    }
}

/// Shared state of a running admin server.
pub struct Server {
    /// The config, mutable because [`Request::Enroll`] appends the
    /// enrollee to `peers`.
    cfg: Mutex<AdminServerConfig>,
    /// Where to persist peer updates. `None` (tests) keeps them
    /// in-memory only.
    cfg_path: Option<PathBuf>,
    /// The CA directory path, if this host holds the CA role — for the
    /// netmap, the CA cert, and other dir files that are neither the request
    /// store nor the vault (a lockless accessor that avoids taking the CA
    /// mutex just to read a path).
    ca_dir: Option<PathBuf>,
    /// The locked CA directory: the request store, the key vault, the serial
    /// counter, and the box-held signing credential, all under one mutex —
    /// which also owns the `<ca-dir>/ca.lock` flock, so exactly one daemon
    /// owns the CA. `None` when this host holds no CA role.
    ca: Option<ca_store::CaDir>,
    /// Serving chain + key, doubling as the client identity for
    /// outbound server-to-server pushes.
    serving_cert_pem: Vec<u8>,
    serving_key_pem: Vec<u8>,
    /// Trust anchors (the CA bundle) for verifying peers — both their
    /// serving certs outbound and their client certs inbound.
    roots: RootCertStore,
    /// The one home CA for application-level admin authorization. Other
    /// certificates in `trusted.pem` remain data-plane federation anchors.
    home_ca_der: CertificateDer<'static>,
    /// Serializes read-modify-write cycles on the local id-map file.
    id_map_lock: Mutex<()>,
    /// Serializes read-modify-write cycles on the local resolver config
    /// (delegation children/parent edits) and the delegation queue's
    /// approve/deny transitions — so exactly one of a concurrent
    /// approve/deny lands and edits don't interleave.
    resolver_edit_lock: Mutex<()>,
    /// The network map: on the CA host the authoritative, persisted copy
    /// (the CA is its only writer); on every other host an in-memory cache
    /// the refresh loop keeps current. Served whole to clients in one shot.
    map: Mutex<NetworkMap>,
    /// Failed-password sliding windows and one-in-flight gates, keyed by the
    /// network source. Used only for network requests handled by a CA.
    password_limiter: Arc<PasswordLimiter>,
    /// A live-backup barrier. Every durable mutation takes a shared guard;
    /// backup takes the exclusive guard only while capturing bytes into
    /// memory, so the published bundle is one controller-state instant without
    /// stopping read-only service or holding the pause across target I/O.
    mutation_gate: Arc<tokio::sync::RwLock<()>>,
}

impl Server {
    pub fn new(
        cfg: AdminServerConfig,
        cfg_path: Option<PathBuf>,
        serving_cert_pem: Vec<u8>,
        serving_key_pem: Vec<u8>,
    ) -> Result<Arc<Self>> {
        let trusted = std::fs::read(&cfg.trusted)
            .with_context(|| format!("reading trust bundle {}", cfg.trusted.display()))?;
        let roots = load_roots(&trusted)
            .with_context(|| format!("trust bundle {}", cfg.trusted.display()))?;
        let home_fp =
            crate::fingerprint::Fingerprint::parse_text(&cfg.home_ca_fingerprint)?;
        let home_ca_der = rustls_pemfile::certs(&mut std::io::Cursor::new(&trusted))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .find(|der| {
                crate::fingerprint::Fingerprint::of_cert_der(der.as_ref()).ok()
                    == Some(home_fp)
            })
            .context("home CA fingerprint is not present in trusted.pem")?;
        let serving_identity =
            crate::tls::admin_cert_identity_from_pem(&serving_cert_pem)?;
        if serving_identity.server_id != cfg.server_id
            || serving_identity.controller != cfg.roles.ca.is_some()
        {
            bail!("admin-server config identity does not match its serving certificate");
        }
        // If we hold the CA, take the singleton flock and seed the serial
        // counter before serving — a second daemon for the same CA fails
        // here, and the counter starts beyond every serial ever issued.
        let ca_dir = cfg.roles.ca.as_ref().map(|r| r.dir.clone());
        // The server's signing credential: the box-held autorenew password,
        // read + unsealed once. In the server-only model this is the only key
        // to the CA. Without it the CA authenticates + serves read-only but
        // signs nothing — loud, because that is a degraded CA, not a crash.
        let autorenew_pw = match cfg.roles.ca.as_ref() {
            None => None,
            Some(ca) => match ca.autorenew.as_ref() {
                Some(keytab) => match read_autorenew_password(keytab) {
                    Ok(pw) => Some(pw),
                    Err(e) => {
                        error!(
                            "admin-server: CANNOT SIGN — the autorenew credential is \
                             unavailable: {e:#}. The CA serves read-only (auth/list/deny \
                             work; issue/enroll/approve/revoke fail). Recover with \
                             `netidx admin ca recovery rotate`."
                        );
                        None
                    }
                },
                None => {
                    error!(
                        "admin-server: CANNOT SIGN — this CA has no autorenew credential. \
                         Set one up with `netidx admin ca auto-approve`; until then it serves \
                         read-only."
                    );
                    None
                }
            },
        };
        // The locked CA directory: take the flock (one daemon per CA),
        // seed the serial counter from the store, and hold the box-held
        // signing credential. A second daemon for the same CA fails here.
        let ca = match &ca_dir {
            Some(dir) => {
                let cadir = ca_store::CaDir::open(dir)?;
                if let Some(role) = cfg.roles.ca.as_ref() {
                    cadir.sessions.configure(
                        role.session_absolute_lifetime,
                        role.session_idle_timeout,
                    );
                }
                *cadir.autorenew_pw.write() = autorenew_pw;
                Some(cadir)
            }
            None => None,
        };
        // The network map: the CA owns + persists it, seeded with the CA's
        // own entry + address so it's never empty of itself; every other
        // host starts with an empty cache the refresh loop fills.
        let map = match &ca_dir {
            Some(dir) => {
                let mut m = netmap::load(dir, cfg.server_id)?;
                let existing_cluster = m
                    .servers
                    .iter()
                    .find(|s| s.id == cfg.server_id)
                    .and_then(|s| s.cluster);
                let facts = local_cluster_facts(&cfg);
                let cluster = facts.as_ref().map(|_| {
                    existing_cluster.unwrap_or_else(admin_proto::ResolverClusterId::new)
                });
                netmap::upsert_controller(
                    &mut m,
                    ServerEntry {
                        id: cfg.server_id,
                        addr: cfg.listen,
                        roles: roles_of(&cfg),
                        resolver: cfg
                            .roles
                            .resolver
                            .as_ref()
                            .and_then(|role| resolver_info(&role.config).ok().flatten()),
                        cluster,
                        state: admin_proto::ServerState::Registered,
                    },
                    facts,
                )?;
                netmap::save(dir, &m)?;
                m
            }
            None => NetworkMap::default(),
        };
        Ok(Arc::new(Server {
            cfg: Mutex::new(cfg),
            cfg_path,
            ca_dir,
            ca,
            serving_cert_pem,
            serving_key_pem,
            roots,
            home_ca_der,
            id_map_lock: Mutex::new(()),
            resolver_edit_lock: Mutex::new(()),
            map: Mutex::new(map),
            password_limiter: Arc::new(PasswordLimiter::default()),
            mutation_gate: Arc::new(tokio::sync::RwLock::new(())),
        }))
    }

    /// The CA directory, if this host holds the CA role.
    pub fn ca_dir(&self) -> Option<&Path> {
        self.ca_dir.as_deref()
    }

    fn roles(&self) -> Vec<Role> {
        roles_of(&self.cfg.lock())
    }

    /// The serving cert + key to present as a *client* on outbound
    /// server-to-server pushes, re-read from disk so a renewal the renewal
    /// daemon installed is used without a restart — the in-memory
    /// `serving_cert_pem`/`serving_key_pem` are only the startup copy (kept
    /// for the acceptor seed, the mDNS fingerprint, and as the fallback
    /// here). Pushes are rare, so two file reads are cheap; reuses the same
    /// `load_serving_keypair` (incl. TPM unseal) the inbound reloader uses.
    fn outbound_identity(&self) -> (Vec<u8>, Vec<u8>) {
        let (cert_path, key_path) = {
            let cfg = self.cfg.lock();
            (cfg.serving_cert.clone(), cfg.serving_key.clone())
        };
        load_serving_keypair(&cert_path, &key_path).unwrap_or_else(|e| {
            warn!(
                "admin-server: re-reading serving identity for push failed, \
                 using startup copy: {e:#}"
            );
            (self.serving_cert_pem.clone(), self.serving_key_pem.clone())
        })
    }
}

/// Run the admin server described by the config at `cfg_path` until the
/// process is killed.
pub async fn serve(cfg_path: PathBuf) -> Result<()> {
    let cfg = AdminServerConfig::load(&cfg_path)?;
    // A TPM-sealed serving key has its password in `<key>.tpm`, sealed to
    // this machine; `load_serving_keypair` unseals + decrypts in memory.
    // Failure is a hard error (a admin server silently down means no discovery
    // and no renewals for the whole network).
    let (serving_cert_pem, serving_key_pem) =
        load_serving_keypair(&cfg.serving_cert, &cfg.serving_key)?;
    let listen = cfg.listen;
    let mdns = cfg.mdns;
    let state = Server::new(cfg, Some(cfg_path), serving_cert_pem, serving_key_pem)?;
    let acceptor =
        build_serving_acceptor(&state, &state.serving_cert_pem, &state.serving_key_pem)?;
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding admin server to {listen}"))?;
    info!("admin-server: listening on {listen}");
    // Advertise over mDNS. The beacon is a hint only — fingerprint +
    // roles ride in TXT purely for pre-connect display/grouping.
    let _advert = if mdns {
        let domain = state.cfg.lock().domain.clone();
        let fp_short = ca_fingerprint_short(&state.serving_cert_pem)?;
        match discovery::advertise(listen, &domain, &state.roles(), &fp_short) {
            Ok(ad) => Some(ad),
            Err(e) => {
                warn!("admin-server: mDNS advertisement failed (continuing): {e:#}");
                None
            }
        }
    } else {
        None
    };
    // Reconcile the current CRL on every controller start. This is especially
    // important after offline disaster recovery: the superseded controller
    // certificate was revoked before the replacement daemon existed to do the
    // ordinary immediate fanout. Startup is the first safe moment to push it.
    if state.ca.is_some() {
        let state = state.clone();
        tokio::spawn(async move { reconcile_controller_state_on_start(state).await });
    }
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
    if state.ca.is_some() {
        match tokio::task::spawn_blocking({
            let state = state.clone();
            move || state.ca.as_ref().expect("CA role held").store.lock().pending_pushes()
        })
        .await
        {
            Ok(Ok(pending)) => {
                for r in pending {
                    let plan = PushPlan { id: r.req.id, name: r.name, groups: r.groups };
                    let _ = push_registrations(&state, &plan).await;
                }
            }
            Ok(Err(e)) => warn!("admin-server: listing pending id-map pushes: {e:#}"),
            Err(e) => warn!("admin-server: pending-push task panicked: {e}"),
        }
    }
    // If the CA role names an autorenew keytab, approve verified renewals
    // in-process from here on (a no-op when it doesn't).
    spawn_autorenew(&state);
    spawn_map_refresh(&state);
    let conns = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let signs = Arc::new(Semaphore::new(MAX_CONCURRENT_SIGNS));
    spawn_local_control(&state, signs.clone());
    // Make the serving cert / CRL hot-reloadable: the watcher swaps in a
    // freshly-built acceptor when the renewal daemon installs a new cert,
    // so a long-running daemon never serves its expired startup cert.
    let acceptor = Arc::new(RwLock::new(acceptor));
    spawn_serving_reload(&state, acceptor.clone());
    loop {
        let (tcp, peer) = match listener.accept().await {
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
        let acceptor = acceptor.read().clone();
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
pub fn local_socket_path(cfg_path: &Path) -> PathBuf {
    cfg_path.parent().unwrap_or_else(|| Path::new(".")).join("admin.sock")
}

/// Bind the local control socket `0600` so only the daemon's uid / root can
/// reach it (defence in depth on top of the per-connection `SO_PEERCRED`
/// check). We hold the CA flock, so any socket file here is stale from a
/// prior run and safe to replace.
fn bind_local_control(path: &Path) -> Result<UnixListener> {
    use std::os::unix::fs::PermissionsExt;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path)
        .with_context(|| format!("binding {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
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
/// Argon2 budget) but not the network connection limit — the socket is a
/// privileged, peer-cred-gated local channel.
fn spawn_local_control(state: &Arc<Server>, signs: Arc<Semaphore>) {
    let Some(cfg_path) = state.cfg_path.clone() else { return };
    let path = local_socket_path(&cfg_path);
    let listener = match bind_local_control(&path) {
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

/// Who the TLS peer is, derived from its presented (already root-validated)
/// client cert: the first DNS SAN, the serial, and the SPKI fingerprint of
/// the leaf's public key. The serial *and fingerprint together* are what let
/// the enqueue path confirm a renewal presents **our** live cert: a serial
/// match alone is forgeable across a co-trusted CA whose serials collide,
/// but the key fingerprint binds to the exact record we issued.
struct PeerIdent {
    san: String,
    serial: Option<u64>,
    spki_fp: Option<String>,
    admin: Option<crate::tls::AdminCertIdentity>,
    home_ca: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestAuthorization {
    Public,
    ControllerOnly,
    NodeSelf,
    AdminAuthenticated,
    LocalOnly,
}

fn request_authorization(req: &Request) -> RequestAuthorization {
    use Request::*;
    match req {
        GetInfo | Enqueue(_) | Poll(_) | GetCrl | RequestDelegation(_)
        | PollDelegation(_) | GetMapVersion | GetMap => RequestAuthorization::Public,
        AddIdentity(_)
        | ApplyCrl(_)
        | ApplyControllerState(_)
        | GetPerms
        | ApplyPermsEdit(_)
        | ApplyReferralEdit(_)
        | ApplyServiceControl(_) => RequestAuthorization::ControllerOnly,
        Register(_) | Deregister => RequestAuthorization::NodeSelf,
        RotateRecovery | RotateAutorenew | Backup(_) => RequestAuthorization::LocalOnly,
        Login(_)
        | Logout(_)
        | Sign(_)
        | Enroll(_)
        | ListQueue(_)
        | Approve(_)
        | Deny(_)
        | Revoke(_)
        | ListIssued(_)
        | ListDelegations(_)
        | ApproveDelegation(_)
        | DenyDelegation(_)
        | RemoveServer(_)
        | ReadPerms(_)
        | EditPerms(_)
        | AddRoleAdmin(_)
        | SetAdminPolicy(_)
        | RemoveAdmin(_)
        | ListAdmins(_)
        | ControlService(_)
        | ReconcileController(_) => RequestAuthorization::AdminAuthenticated,
    }
}

/// Durable controller/node state changes participate in the live-backup
/// barrier. The backup request itself takes the exclusive side in its handler;
/// read-only requests and in-memory login sessions do not delay a snapshot.
fn request_mutates_durable_state(req: &Request) -> bool {
    use Request::*;
    matches!(
        req,
        Sign(_)
            | Enroll(_)
            | AddIdentity(_)
            | Enqueue(_)
            | Approve(_)
            | Deny(_)
            | Revoke(_)
            | RequestDelegation(_)
            | ApproveDelegation(_)
            | DenyDelegation(_)
            | ApplyReferralEdit(_)
            | ApplyCrl(_)
            | ApplyControllerState(_)
            | Register(_)
            | Deregister
            | RemoveServer(_)
            | EditPerms(_)
            | ApplyPermsEdit(_)
            | AddRoleAdmin(_)
            | SetAdminPolicy(_)
            | RemoveAdmin(_)
            | RotateRecovery
            | RotateAutorenew
    )
}

/// The credential of a request that can actually invoke the password KDF.
/// `Logout` deliberately is not included: it accepts only a session token and
/// rejects a password without consulting the vault.
fn password_credential(req: &Request) -> Option<&admin_proto::AdminCredential> {
    use Request::*;
    let credential = match req {
        Login(req) => &req.credential,
        Sign(req) => &req.credential,
        Enroll(req) => &req.credential,
        ListQueue(req) => &req.credential,
        Approve(req) => &req.credential,
        Deny(req) => &req.credential,
        Revoke(req) => &req.credential,
        ListIssued(req) => &req.credential,
        ListDelegations(req) => &req.credential,
        ApproveDelegation(req) => &req.credential,
        DenyDelegation(req) => &req.credential,
        RemoveServer(req) => &req.credential,
        ReadPerms(req) => &req.credential,
        EditPerms(req) => &req.credential,
        AddRoleAdmin(req) => &req.credential,
        SetAdminPolicy(req) => &req.credential,
        RemoveAdmin(req) => &req.credential,
        ListAdmins(req) => &req.credential,
        ControlService(req) => &req.credential,
        GetInfo
        | Logout(_)
        | AddIdentity(_)
        | Enqueue(_)
        | Poll(_)
        | GetCrl
        | RequestDelegation(_)
        | PollDelegation(_)
        | ApplyReferralEdit(_)
        | ApplyCrl(_)
        | Backup(_)
        | ApplyControllerState(_)
        | Register(_)
        | Deregister
        | GetMapVersion
        | GetMap
        | GetPerms
        | ApplyPermsEdit(_)
        | ApplyServiceControl(_)
        | RotateRecovery
        | RotateAutorenew => return None,
        ReconcileController(req) => &req.credential,
    };
    matches!(credential, admin_proto::AdminCredential::Password { .. })
        .then_some(credential)
}

fn authorize_request_class(
    class: RequestAuthorization,
    local: bool,
    peer_is_admin_server: bool,
    peer_is_controller: bool,
) -> std::result::Result<(), &'static str> {
    match class {
        RequestAuthorization::Public | RequestAuthorization::AdminAuthenticated => Ok(()),
        RequestAuthorization::ControllerOnly if peer_is_controller => Ok(()),
        RequestAuthorization::NodeSelf if peer_is_admin_server => Ok(()),
        RequestAuthorization::LocalOnly if local => Ok(()),
        RequestAuthorization::ControllerOnly => {
            Err("request requires the home CA controller certificate")
        }
        RequestAuthorization::NodeSelf => {
            Err("request requires a protocol-v6 home-CA node certificate")
        }
        RequestAuthorization::LocalOnly => {
            Err("request is available only over the protected local control socket")
        }
    }
}

fn cert_signed_by(leaf_der: &[u8], ca_der: &[u8]) -> bool {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let Ok((_, leaf)) = X509Certificate::from_der(leaf_der) else { return false };
    let Ok((_, ca)) = X509Certificate::from_der(ca_der) else { return false };
    leaf.verify_signature(Some(ca.public_key())).is_ok()
}

async fn handle_conn(
    acceptor: &TlsAcceptor,
    tcp: TcpStream,
    peer: SocketAddr,
    state: &Arc<Server>,
    signs: Arc<Semaphore>,
) -> Result<()> {
    let tls = acceptor.accept(tcp).await.context("TLS handshake")?;
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

/// The transport-agnostic body of a admin-plane connection: the hello
/// exchange and the single request/response, dispatched against the same
/// handlers regardless of how the bytes arrived. Shared by the TLS listener
/// ([`handle_conn`]) and the local control socket ([`handle_local_conn`]).
///
/// `local` marks a request that arrived over the trusted on-box control
/// socket. Reaching that socket already proves on-box authority (it is
/// `0600` + `SO_PEERCRED`-gated), so a local request authorizes
/// admin-management ops as a superuser — no password, the way a signing slot
/// does. `peer_ident` is the TLS peer's cert identity and is always `None`
/// for a local connection: there is no certificate, and a local caller is
/// deliberately *not* treated as a admin-server peer, so the peer-cert-gated
/// server-to-server requests stay refused locally.
async fn serve_request<S>(
    mut tls: S,
    peer: SocketAddr,
    peer_ident: Option<PeerIdent>,
    local: bool,
    state: &Arc<Server>,
    signs: Arc<Semaphore>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let peer_admin = peer_ident.as_ref().filter(|p| p.home_ca).and_then(|p| p.admin);
    let peer_is_admin_server = peer_admin.is_some();
    let peer_is_controller = peer_admin.is_some_and(|p| p.controller);
    let hello: ClientHello =
        admin_proto::read_msg(&mut tls).await.context("reading ClientHello")?;
    let domain = state.cfg.lock().domain.clone();
    let (server_id, controller) = {
        let cfg = state.cfg.lock();
        (cfg.server_id, cfg.roles.ca.is_some())
    };
    admin_proto::write_msg(
        &mut tls,
        &ServerHello {
            protocol_version: PROTOCOL_VERSION,
            domain,
            roles: state.roles(),
            server_id,
            controller,
        },
    )
    .await
    .context("writing ServerHello")?;
    anyhow::ensure!(
        hello.protocol_version == PROTOCOL_VERSION,
        "client speaks protocol version {} but we speak {PROTOCOL_VERSION}",
        hello.protocol_version
    );
    let req: Request =
        admin_proto::read_msg(&mut tls).await.context("reading Request")?;
    if let Err(reason) = authorize_request_class(
        request_authorization(&req),
        local,
        peer_is_admin_server,
        peer_is_controller,
    ) {
        bail!(reason);
    }
    // Only a CA can run a password KDF. Reserve the source before dispatch,
    // sleeping outside the global Argon2 semaphore when recent failures impose
    // a delay. Local-control requests are kernel-credential authorized and do
    // not participate in network throttling.
    let password_attempt =
        if !local && state.ca.is_some() && password_credential(&req).is_some() {
            Some(state.password_limiter.begin(peer.ip()).await?)
        } else {
            None
        };
    // A waiting backup writer prevents new mutations from entering; once all
    // in-flight shared guards drain it captures one durable instant. Acquire
    // after the per-source delay so a throttled attacker cannot stall a local
    // backup without even reaching authentication. Keep the guard across the
    // complete operation, including post-commit fanout.
    let _mutation_guard = if request_mutates_durable_state(&req) {
        Some(state.mutation_gate.read().await)
    } else {
        None
    };
    REQUEST_PASSWORD_ATTEMPT
        .scope(password_attempt, async move {
            match req {
                Request::GetInfo => {
                    let resp = get_info(state);
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing GetInfoResponse")
                }
                Request::Login(req) => {
                    let resp = match state.ca.as_ref() {
                        None => admin_proto::LoginResponse::Err {
                            reason: "login must be sent to the CA controller".to_string(),
                        },
                        Some(_) => {
                            let state = state.clone();
                            run_signing(&signs, move || {
                                let ca = state.ca.as_ref().expect("CA role held");
                                let is_password = matches!(
                                    &req.credential,
                                    admin_proto::AdminCredential::Password { .. }
                                );
                                let response =
                                    ca.sessions.login(&ca.vault.read(), &req.credential);
                                if is_password {
                                    record_password_auth_result(matches!(
                                        &response,
                                        admin_proto::LoginResponse::Ok { .. }
                                    ));
                                }
                                response
                            })
                            .await?
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing LoginResponse")
                }
                Request::Logout(req) => {
                    let resp = match state.ca.as_ref() {
                        None => admin_proto::LogoutResponse::Err {
                            reason: "logout must be sent to the CA controller"
                                .to_string(),
                        },
                        Some(ca) => ca.sessions.logout(&req.credential),
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing LogoutResponse")
                }
                Request::Sign(req) => {
                    let resp = match ca_dir(state) {
                        None => reject("this host does not hold the CA"),
                        Some(_) => {
                            let signed = run_signing(&signs, {
                                let state = state.clone();
                                move || {
                                    handle_sign_request(
                                        state.ca.as_ref().expect("CA role held"),
                                        &req,
                                    )
                                }
                            })
                            .await?;
                            match (signed.resp, signed.push) {
                                (resp @ SignResponse::Err { .. }, _) => resp,
                                (resp @ SignResponse::Ok { .. }, None) => resp,
                                (
                                    SignResponse::Ok {
                                        signed_cert_pem,
                                        trusted_pem,
                                        mut warnings,
                                        ..
                                    },
                                    Some(plan),
                                ) => {
                                    let (operation_id, push_warnings) =
                                        push_registrations(state, &plan).await;
                                    warnings.extend(push_warnings);
                                    SignResponse::Ok {
                                        signed_cert_pem,
                                        trusted_pem,
                                        warnings,
                                        operation_id: Some(operation_id),
                                    }
                                }
                            }
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing SignResponse")
                }
                Request::Enroll(req) => {
                    let resp = match ca_dir(state) {
                        None => reject("this host does not hold the CA"),
                        Some(_) => {
                            let enrollment = admin_proto::EnrollmentRequest {
                                listen: req.listen,
                                roles: req.roles.clone(),
                                resolver_member: req.resolver_member.clone(),
                                resolver_members: req.resolver_members.clone(),
                                cluster: req.cluster.clone(),
                            };
                            let resp = run_signing(&signs, {
                                let state = state.clone();
                                move || {
                                    handle_enroll_request(
                                        state.ca.as_ref().expect("CA role held"),
                                        &req,
                                        local,
                                        Some(&state.map.lock()),
                                    )
                                }
                            })
                            .await?;
                            if let SignResponse::Ok { signed_cert_pem, .. } = &resp {
                                if !local {
                                    match crate::tls::admin_cert_identity_from_pem(
                                        signed_cert_pem.as_bytes(),
                                    )
                                    .and_then(|id| {
                                        grant_enrollment(state, id.server_id, &enrollment)
                                    }) {
                                        Ok(_) => record_peer(state, enrollment.listen),
                                        Err(e) => {
                                            return admin_proto::write_msg(
                                        &mut tls,
                                        &SignResponse::Err {
                                            reason: format!(
                                                "recording enrollment grant: {e:#}"
                                            ),
                                        },
                                    )
                                    .await
                                    .context("writing SignResponse");
                                        }
                                    }
                                }
                            }
                            resp
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing SignResponse")
                }
                Request::AddIdentity(req) => {
                    let resp = if !peer_is_admin_server {
                        AddIdentityResponse::Err {
                            reason: "identity registration requires a admin-server peer \
                             certificate"
                                .to_string(),
                        }
                    } else {
                        let map_path = {
                            state.cfg.lock().roles.id_map.as_ref().map(|r| r.map.clone())
                        };
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
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing AddIdentityResponse")
                }
                Request::Enqueue(req) => {
                    let resp = match ca_dir(state) {
                        None => EnqueueResponse::Err {
                            reason: "this host does not hold the CA".to_string(),
                        },
                        Some(_) => {
                            let state = state.clone();
                            tokio::task::spawn_blocking(move || {
                                handle_enqueue(
                                    state.ca.as_ref().expect("CA role held"),
                                    &req,
                                    peer,
                                    peer_ident.as_ref(),
                                )
                            })
                            .await
                            .context("enqueue task panicked")?
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing EnqueueResponse")
                }
                Request::Poll(req) => {
                    let resp = match ca_dir(state) {
                        None => PollResponse::Unknown,
                        Some(_) => {
                            // The blocking part reads the status (a Pending request
                            // genuinely has no cert — the commit is atomic); on a
                            // Signed record whose id-map push never landed it also
                            // hands back a re-push plan, which the async part runs.
                            let (mut resp, repush) =
                                {
                                    let state = state.clone();
                                    let id = req.request_id.clone();
                                    tokio::task::spawn_blocking(move || {
                            let store =
                                state.ca.as_ref().expect("CA role held").store.lock();
                            match store.status(&id) {
                                Ok(ca_store::Status::Pending(_)) => {
                                    (PollResponse::Pending, None)
                                }
                                Ok(ca_store::Status::Signed(s)) => {
                                    let repush = match store.read_issued(&id) {
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
                                        operation_id: None,
                                    };
                                    (o, repush)
                                }
                                Ok(ca_store::Status::Denied(d)) => {
                                    (PollResponse::Denied { reason: d.reason }, None)
                                }
                                Ok(ca_store::Status::Unknown) => {
                                    (PollResponse::Unknown, None)
                                }
                                Err(e) => {
                                    warn!("admin-server: queue status failed: {e:#}");
                                    (PollResponse::Unknown, None)
                                }
                            }
                        })
                        .await
                        .context("poll task panicked")?
                                };
                            if let Some(plan) = repush {
                                // Best-effort id-map recovery (sets push_done on
                                // success); the enrollee gets its cert regardless.
                                let (operation_id, push_warnings) =
                                    push_registrations(state, &plan).await;
                                if let PollResponse::Signed {
                                    warnings,
                                    operation_id: response_operation_id,
                                    ..
                                } = &mut resp
                                {
                                    warnings.extend(push_warnings);
                                    *response_operation_id = Some(operation_id);
                                }
                            }
                            resp
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing PollResponse")
                }
                Request::ListQueue(req) => {
                    let resp = match ca_dir(state) {
                        None => ListQueueResponse::Err {
                            reason: "this host does not hold the CA".to_string(),
                        },
                        Some(_) => {
                            let state = state.clone();
                            run_signing(&signs, move || handle_list_queue(&state, &req))
                                .await?
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ListQueueResponse")
                }
                Request::Approve(req) => {
                    let resp = match ca_dir(state) {
                        None => ApproveResponse::Err {
                            reason: "this host does not hold the CA".to_string(),
                        },
                        Some(_) => {
                            // Authenticate + sign on the blocking pool (Argon2 +
                            // openssl); the issuance commits atomically under the
                            // CA lock inside the task, then we run the best-effort
                            // side effects.
                            let signed = run_signing(&signs, {
                                let state = state.clone();
                                move || {
                                    handle_approve(
                                        state.ca.as_ref().expect("CA role held"),
                                        &req,
                                        Some(&state.map.lock()),
                                    )
                                }
                            })
                            .await?;
                            match signed {
                                Err(reason) => ApproveResponse::Err { reason },
                                Ok(Approved {
                                    resp: SignResponse::Err { reason },
                                    ..
                                }) => {
                                    // The sign itself failed (policy etc.) —
                                    // the request stays pending; the admin can
                                    // retry with different groups or deny it.
                                    ApproveResponse::Err { reason }
                                }
                                Ok(Approved {
                                    resp: SignResponse::Ok { mut warnings, .. },
                                    push,
                                    enrollment,
                                    ..
                                }) => {
                                    // The cert is issued and committed atomically in
                                    // the signing task; these are best-effort side
                                    // effects (the enrollee polls the cert back
                                    // regardless). Push-registration warnings go to
                                    // the approving admin's reply only.
                                    let operation_id = if let Some(plan) = push {
                                        let (operation_id, push_warnings) =
                                            push_registrations(state, &plan).await;
                                        warnings.extend(push_warnings);
                                        Some(operation_id)
                                    } else {
                                        None
                                    };
                                    // An approved enrollment makes the new admin
                                    // server a peer — same side effect as the
                                    // synchronous Enroll, deferred to approval.
                                    if let Some((server_id, enrollment)) = enrollment {
                                        match grant_enrollment(
                                            state,
                                            server_id,
                                            &enrollment,
                                        ) {
                                            Ok(_) => {
                                                record_peer(state, enrollment.listen)
                                            }
                                            Err(e) => warnings.push(format!(
                                                "recording enrollment grant: {e:#}"
                                            )),
                                        }
                                    }
                                    ApproveResponse::Ok { operation_id, warnings }
                                }
                            }
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ApproveResponse")
                }
                Request::Deny(req) => {
                    let resp = match ca_dir(state) {
                        None => DenyResponse::Err {
                            reason: "this host does not hold the CA".to_string(),
                        },
                        Some(_) => {
                            run_signing(&signs, {
                                let state = state.clone();
                                move || {
                                    handle_deny(
                                        state.ca.as_ref().expect("CA role held"),
                                        &req,
                                        Some(&state.map.lock()),
                                    )
                                }
                            })
                            .await?
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing DenyResponse")
                }
                Request::Revoke(req) => {
                    let resp = match ca_dir(state) {
                        None => RevokeResponse::Err {
                            reason: "this host does not hold the CA".to_string(),
                        },
                        Some(_) => handle_revoke(state, &signs, &req).await,
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing RevokeResponse")
                }
                Request::ListIssued(req) => {
                    let resp = match ca_dir(state) {
                        None => ListIssuedResponse::Err {
                            reason: "this host does not hold the CA".to_string(),
                        },
                        Some(_) => {
                            let state = state.clone();
                            run_signing(&signs, move || {
                                handle_list_issued(
                                    state.ca.as_ref().expect("CA role held"),
                                    &req,
                                )
                            })
                            .await?
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ListIssuedResponse")
                }
                Request::RequestDelegation(req) => {
                    let resp = match ca_dir(state) {
                        None => DelegationResponse::Err {
                            reason: "this host does not hold the CA".to_string(),
                        },
                        Some(_) => {
                            let state = state.clone();
                            tokio::task::spawn_blocking(move || {
                                handle_request_delegation(&state, &req, peer)
                            })
                            .await
                            .context("delegation request task panicked")?
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing DelegationResponse")
                }
                Request::PollDelegation(req) => {
                    let resp = match ca_dir(state) {
                        None => DelegationPollResponse::Unknown,
                        Some(dir) => tokio::task::spawn_blocking(move || {
                            handle_poll_delegation(&dir, &req)
                        })
                        .await
                        .context("delegation poll task panicked")?,
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing DelegationPollResponse")
                }
                Request::ListDelegations(req) => {
                    let resp = match ca_dir(state) {
                        None => ListDelegationsResponse::Err {
                            reason: "this host does not hold the CA".to_string(),
                        },
                        Some(_) => {
                            let state = state.clone();
                            run_signing(&signs, move || {
                                handle_list_delegations(&state, &req)
                            })
                            .await?
                        }
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ListDelegationsResponse")
                }
                Request::ApproveDelegation(req) => {
                    let resp = handle_approve_delegation(state, &signs, req).await;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ApproveDelegationResponse")
                }
                Request::DenyDelegation(req) => {
                    // Argon2-bound (vault auth) — keep it under the sign semaphore.
                    let state = state.clone();
                    let resp =
                        run_signing(&signs, move || handle_deny_delegation(&state, &req))
                            .await?;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing DenyDelegationResponse")
                }
                Request::ApplyReferralEdit(req) => {
                    let resp = if !peer_is_admin_server {
                        ApplyReferralEditResponse::Err {
                            reason:
                                "a referral edit requires a admin-server peer certificate"
                                    .to_string(),
                        }
                    } else {
                        let state = state.clone();
                        tokio::task::spawn_blocking(move || {
                            handle_apply_referral_edit(&state, &req)
                        })
                        .await
                        .context("apply referral edit task panicked")?
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ApplyReferralEditResponse")
                }
                Request::GetCrl => {
                    let resp =
                        match ca_dir(state) {
                            None => GetCrlResponse { crl_pem: None },
                            Some(_) => {
                                let state = state.clone();
                                tokio::task::spawn_blocking(move || {
                        let path = state
                            .ca
                            .as_ref()
                            .expect("CA role held")
                            .store
                            .lock()
                            .crl_path();
                        match std::fs::read_to_string(path) {
                            Ok(pem) => GetCrlResponse { crl_pem: Some(pem) },
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                GetCrlResponse { crl_pem: None }
                            }
                            Err(e) => {
                                warn!("admin-server: reading the CRL failed: {e:#}");
                                GetCrlResponse { crl_pem: None }
                            }
                        }
                    })
                    .await
                    .context("CRL read task panicked")?
                            }
                        };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing GetCrlResponse")
                }
                Request::Register(req) => {
                    let resp = if !peer_is_admin_server {
                        RegisterResponse::Err {
                            reason: "register requires a admin-server peer certificate"
                                .to_string(),
                        }
                    } else {
                        let state = state.clone();
                        let server_id =
                            peer_admin.expect("NodeSelf authorized").server_id;
                        tokio::task::spawn_blocking(move || {
                            handle_register(&state, server_id, &req)
                        })
                        .await
                        .context("register task panicked")?
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing RegisterResponse")
                }
                Request::Deregister => {
                    let resp = if !peer_is_admin_server {
                        RegisterResponse::Err {
                            reason: "deregister requires a admin-server peer certificate"
                                .to_string(),
                        }
                    } else {
                        let state = state.clone();
                        let server_id =
                            peer_admin.expect("NodeSelf authorized").server_id;
                        tokio::task::spawn_blocking(move || {
                            handle_deregister(&state, server_id)
                        })
                        .await
                        .context("deregister task panicked")?
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing RegisterResponse")
                }
                Request::GetMapVersion => {
                    let resp =
                        GetMapVersionResponse::Ok { version: state.map.lock().version };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing GetMapVersionResponse")
                }
                Request::GetMap => {
                    let resp = GetMapResponse::Ok { map: state.map.lock().clone() };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing GetMapResponse")
                }
                Request::RemoveServer(req) => {
                    let resp = handle_remove_server(&state, &signs, req).await;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing RemoveServerResponse")
                }
                Request::ReadPerms(req) => {
                    let resp = handle_read_perms(state, &signs, &req, local).await;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ReadPermsResponse")
                }
                Request::GetPerms => {
                    let state = state.clone();
                    let resp =
                        tokio::task::spawn_blocking(move || handle_get_perms(&state))
                            .await
                            .context("get-perms task panicked")?;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing GetPermsResponse")
                }
                Request::EditPerms(req) => {
                    let resp = handle_edit_perms(state, &signs, &req, local).await;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing EditPermsResponse")
                }
                Request::ApplyPermsEdit(req) => {
                    let resp = if !peer_is_admin_server {
                        ApplyPermsEditResponse::Err {
                            reason:
                                "a perms edit requires a admin-server peer certificate"
                                    .to_string(),
                        }
                    } else {
                        let state = state.clone();
                        tokio::task::spawn_blocking(move || {
                            handle_apply_perms_edit(&state, &req)
                        })
                        .await
                        .context("apply perms edit task panicked")?
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ApplyPermsEditResponse")
                }
                Request::ApplyCrl(req) => {
                    let resp = if !peer_is_controller {
                        ApplyCrlResponse::Err {
                            reason:
                                "a CRL update requires the home CA controller certificate"
                                    .to_string(),
                        }
                    } else {
                        let state = state.clone();
                        tokio::task::spawn_blocking(move || {
                            handle_apply_crl(&state, &req)
                        })
                        .await
                        .context("apply CRL task panicked")?
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ApplyCrlResponse")
                }
                Request::ApplyControllerState(req) => {
                    let resp = if !peer_is_controller {
                        ApplyControllerStateResponse::Err {
                            reason: "controller-state reconciliation requires the exact home CA \
                                     controller certificate"
                                .to_string(),
                        }
                    } else {
                        let state = state.clone();
                        tokio::task::spawn_blocking(move || {
                            handle_apply_controller_state(&state, &req)
                        })
                        .await
                        .context("apply controller state task panicked")?
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ApplyControllerStateResponse")
                }
                Request::ReconcileController(req) => {
                    let resp = handle_reconcile_controller(state, &signs, &req, local).await;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ReconcileControllerResponse")
                }
                Request::AddRoleAdmin(req) => {
                    // Bound the Argon2 in `authenticate` by the sign semaphore, like
                    // every other vault-auth handler — a bare `spawn_blocking` would
                    // let an anonymous flood pin unbounded 64 MiB derivations.
                    let state = state.clone();
                    let resp = run_signing(&signs, move || {
                        handle_add_role_admin(&state, &req, local)
                    })
                    .await?;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing AdminMgmtResponse")
                }
                Request::SetAdminPolicy(req) => {
                    let state = state.clone();
                    let resp = run_signing(&signs, move || {
                        handle_set_admin_policy(&state, &req, local)
                    })
                    .await?;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing AdminMgmtResponse")
                }
                Request::RemoveAdmin(req) => {
                    let state = state.clone();
                    let resp = run_signing(&signs, move || {
                        handle_remove_admin(&state, &req, local)
                    })
                    .await?;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing AdminMgmtResponse")
                }
                Request::ListAdmins(req) => {
                    let state = state.clone();
                    let resp = run_signing(&signs, move || {
                        handle_list_admins(&state, &req, local)
                    })
                    .await?;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing AdminListResponse")
                }
                Request::ControlService(req) => {
                    let resp = handle_control_service(state, &signs, &req).await;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ControlServiceResponse")
                }
                Request::ApplyServiceControl(req) => {
                    let resp = if !peer_is_admin_server {
                        ApplyServiceControlResponse::Err {
                            reason:
                                "service control requires a admin-server peer certificate"
                                    .to_string(),
                        }
                    } else {
                        handle_apply_service_control(state, &req).await
                    };
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing ApplyServiceControlResponse")
                }
                Request::RotateRecovery => {
                    // Argon2-bound (recover_mk + a fresh slot KDF), so run it under
                    // the sign semaphore like the other vault-write handlers.
                    let state = state.clone();
                    let resp = run_signing(&signs, move || {
                        handle_rotate_recovery(&state, local)
                    })
                    .await?;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing RotateRecoveryResponse")
                }
                Request::RotateAutorenew => {
                    let state = state.clone();
                    let resp = run_signing(&signs, move || {
                        handle_rotate_autorenew(&state, local)
                    })
                    .await?;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing RotateAutorenewResponse")
                }
                Request::Backup(req) => {
                    let resp = handle_backup(state, &signs, &req).await;
                    admin_proto::write_msg(&mut tls, &resp)
                        .await
                        .context("writing BackupResponse")
                }
            }
        })
        .await
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
    let password_attempt =
        REQUEST_PASSWORD_ATTEMPT.try_with(Clone::clone).unwrap_or(None);
    let permit =
        signs.clone().acquire_owned().await.expect("sign semaphore is never closed");
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        with_password_attempt(password_attempt, f)
    })
    .await
    .context("CA signing task panicked")
}

fn ca_dir(state: &Server) -> Option<PathBuf> {
    state.ca_dir.clone()
}

async fn handle_backup(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    req: &admin_proto::BackupRequest,
) -> BackupResponse {
    let Some(ca) = state.ca.as_ref() else {
        return BackupResponse::Err {
            reason: "backup must be run on the CA controller".to_string(),
        };
    };
    let Some(cfg_path) = state.cfg_path.clone() else {
        return BackupResponse::Err {
            reason: "the running controller has no persistent config path".to_string(),
        };
    };
    let ca_dir = ca.dir().to_path_buf();
    let target = PathBuf::from(&req.target);
    let signing_state = state.clone();
    let unlocked = match run_signing(signs, move || {
        server_unlock(signing_state.ca.as_ref().expect("CA role held"))
    })
    .await
    {
        Ok(Ok(unlocked)) => unlocked,
        Ok(Err(reason)) => return BackupResponse::Err { reason },
        Err(e) => {
            return BackupResponse::Err {
                reason: format!("backup signing task failed: {e:#}"),
            };
        }
    };
    let gate = state.mutation_gate.clone().write_owned().await;
    let cfg = state.cfg.lock().clone();
    let map_version = state.map.lock().version;
    let highest_serial = match ca.store.lock().max_serial() {
        Ok(serial) => serial.unwrap_or(0),
        Err(e) => {
            return BackupResponse::Err {
                reason: format!("reading serial state: {e:#}"),
            };
        }
    };
    let snapshot = match crate::backup::capture(
        &cfg,
        &cfg_path,
        &ca_dir,
        map_version,
        highest_serial,
        &unlocked.ca_key_pem,
    ) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            return BackupResponse::Err { reason: format!("capturing backup: {e:#}") };
        }
    };
    // The consistent bytes are fixed; let administration resume before the
    // potentially slow target filesystem is touched.
    drop(gate);
    let ca_dir_for_publish = ca_dir.clone();
    let outcome = match tokio::task::spawn_blocking(move || {
        crate::backup::publish(snapshot, &target, &ca_dir_for_publish)
    })
    .await
    {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(e)) => {
            return BackupResponse::Err { reason: format!("publishing backup: {e:#}") };
        }
        Err(e) => {
            return BackupResponse::Err { reason: format!("backup task panicked: {e}") };
        }
    };
    audit(
        &ca_dir,
        "local",
        "backup",
        &format!("{} manifest {}", outcome.target.display(), outcome.manifest_sha256),
        Duration::ZERO,
    );
    BackupResponse::Ok {
        target: outcome.target.to_string_lossy().into_owned(),
        ca_fingerprint: outcome.ca_fingerprint,
        controller: outcome.controller,
        map_version: outcome.map_version,
        highest_serial: outcome.highest_serial,
        files: outcome.files,
        bytes: outcome.bytes,
        manifest_sha256: outcome.manifest_sha256,
    }
}

/// Append a freshly enrolled admin server to our peer list (and persist
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
        warn!("admin-server: failed to persist enrolled peer {peer}: {e:#}");
    }
}

/// Local facts + known peers. Cheap and network-free: the resolver
/// address/auth is read fresh from the resolver config so config edits
/// show up without a daemon restart; everything else is our own config.
fn get_info(state: &Server) -> GetInfoResponse {
    let cfg = state.cfg.lock();
    let resolver =
        cfg.roles.resolver.as_ref().and_then(|r| match resolver_info(&r.config) {
            Ok(x) => x,
            Err(e) => {
                warn!(
                    "admin-server: could not derive resolver info from {}: {e:#}",
                    r.config.display()
                );
                None
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
/// from its resolver config — the first advertisable (non-`Local`)
/// member, the representative `GetInfo` reports. See
/// [`ResolverConfig::resolver_addrs`](crate::resolver::ResolverConfig::resolver_addrs)
/// for the full cluster set (used by delegation).
fn resolver_info(config: &Path) -> Result<Option<ResolverAddr>> {
    let rc = crate::resolver::ResolverConfig::load(config)?;
    Ok(rc.resolver_addrs().into_iter().next())
}

/// Fan the freshly signed identity out to every id-map host we know of:
/// the local map directly, configured peers and mDNS-discovered admin
/// servers over authenticated TLS. Returns warnings for the failures —
/// the sign itself already succeeded.
async fn push_registrations(
    state: &Arc<Server>,
    plan: &PushPlan,
) -> (admin_proto::OperationId, Vec<String>) {
    let Some((primary, secondary)) = plan.groups.split_first() else {
        return (admin_proto::OperationId::new(), Vec::new());
    };
    let operation_id = admin_proto::OperationId::new();
    if let Some(ca) = state.ca.as_ref() {
        audit(
            ca.dir(),
            "controller",
            "fanout-id-map",
            &format!("operation {operation_id}: {}", plan.name),
            Duration::ZERO,
        );
    }
    let req = AddIdentityRequest {
        operation_id,
        san: plan.name.clone(),
        primary_group: primary.clone(),
        groups: secondary.to_vec(),
    };
    let mut warnings = Vec::new();
    let (my_id, controller_id, targets) = {
        let cfg = state.cfg.lock();
        let map = state.map.lock();
        let mut targets: Vec<_> = map
            .servers
            .iter()
            .filter(|s| {
                s.roles.contains(&Role::IdMap)
                    && s.state == admin_proto::ServerState::Registered
            })
            .map(|s| (s.id, s.addr))
            .collect();
        targets.sort_by_key(|(id, _)| *id);
        (cfg.server_id, map.controller, targets)
    };
    // Local id-map first (no TLS loopback).
    let local_map = { state.cfg.lock().roles.id_map.as_ref().map(|r| r.map.clone()) };
    if targets.iter().any(|(id, _)| *id == my_id)
        && let Some(path) = local_map
    {
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
    let (cert, key) = state.outbound_identity();
    let roots = state.roots.clone();
    let home_ca = state.home_ca_der.clone();
    let mut results: Vec<_> = stream::iter(
        targets.into_iter().filter(|(id, _)| *id != my_id).map(|(id, addr)| {
            let req = req.clone();
            let cert = cert.clone();
            let key = key.clone();
            let roots = roots.clone();
            let home_ca = home_ca.clone();
            async move {
                let result = tokio::time::timeout(
                    PUSH_TIMEOUT,
                    admin_client::push_identity(
                        addr,
                        id,
                        id == controller_id,
                        home_ca,
                        &cert,
                        &key,
                        roots,
                        &req,
                    ),
                )
                .await
                .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
                .and_then(|r| r);
                (id, addr, result)
            }
        }),
    )
    .buffer_unordered(32)
    .collect()
    .await;
    results.sort_by_key(|(id, _, _)| *id);
    for (id, addr, result) in results {
        match result {
            Ok(Some(uid)) => {
                info!("admin-server: registered {} (uid {uid}) on {addr}", plan.name)
            }
            Ok(None) => (),
            Err(e) => warnings.push(format!(
                "id-map registration on server {id} at {addr} failed: {e:#}"
            )),
        }
    }
    // Mark the issuance's id-map push complete only when nothing failed at
    // all — local *or* remote (a local failure is a real failure, not part
    // of the baseline). A partial push leaves a warning, so the record
    // stays in the recovery set (`pending_pushes`) and is retried on the
    // next poll or daemon restart. The `set_push_done` read-modify-write
    // shares the CA mutex with `handle_revoke` so the two can't clobber
    // each other's field on the same record.
    if warnings.is_empty()
        && let Some(ca) = state.ca.as_ref()
    {
        let _ = ca.store.lock().set_push_done(&plan.id);
    }
    (operation_id, warnings)
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
    RustlsServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("selecting TLS versions")?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .context("building TLS server config")
}

/// The CRL this admin server enforces on inbound peer certs: the CA's own
/// authoritative `crl.pem` when we hold the CA, else the copy the renewal
/// daemon places beside our trust bundle (the convention netidx's own
/// acceptor watches). Absent ⇒ `None` — a missing CRL must never lock the
/// admin plane out; it just means no revocation is enforced yet.
fn serving_crl_path(state: &Server) -> PathBuf {
    match state.ca.as_ref() {
        Some(ca) => ca.store.lock().crl_path(),
        None => state.cfg.lock().trusted.with_file_name("crl.pem"),
    }
}

fn load_serving_crl(state: &Server) -> Option<Vec<u8>> {
    let path = serving_crl_path(state);
    match std::fs::read(&path) {
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
fn load_serving_keypair(
    serving_cert: &Path,
    serving_key: &Path,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let cert_pem = std::fs::read(serving_cert)
        .with_context(|| format!("reading serving cert {}", serving_cert.display()))?;
    let key_pem = std::fs::read(serving_key)
        .with_context(|| format!("reading serving key {}", serving_key.display()))?;
    let key_pem = {
        let sidecar = crate::tls::sealed_sidecar(serving_key);
        if sidecar.exists() {
            let blob = std::fs::read(&sidecar)
                .with_context(|| format!("reading sealed password {sidecar:?}"))?;
            let pw = netidx_tpm::unseal(&blob).with_context(|| {
                format!(
                    "unsealing {sidecar:?} — if this host's TPM was cleared or \
                     the board was replaced, re-enroll this admin server"
                )
            })?;
            let pw = std::str::from_utf8(&pw).context("sealed password is not utf8")?;
            let pem = std::str::from_utf8(&key_pem).context("serving key is not utf8")?;
            netidx::tls::decrypt_private_key(pem, pw)
                .context("decrypting the serving key")?
                .as_bytes()
                .to_vec()
        } else {
            key_pem
        }
    };
    Ok((cert_pem, key_pem))
}

/// Build the inbound TLS acceptor from a serving cert/key and the current
/// CRL. Used at startup and on every live reload.
fn build_serving_acceptor(
    state: &Server,
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<TlsAcceptor> {
    let crl = load_serving_crl(state);
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
/// expired — taking the admin plane down network-wide. Now a long-running
/// daemon picks up a renewal (or a fresh CRL / revocation) within one poll,
/// no restart needed. Cheap: a stat, and a rebuild only when an mtime moves.
const SERVING_RELOAD_POLL: Duration = Duration::from_secs(30);

/// Watch the serving cert + CRL files and swap a freshly-built acceptor into
/// `acceptor` when either changes. New connections pick up the new acceptor;
/// in-flight handshakes keep the one they started with.
fn spawn_serving_reload(state: &Arc<Server>, acceptor: Arc<RwLock<TlsAcceptor>>) {
    let cert_path = state.cfg.lock().serving_cert.clone();
    let key_path = state.cfg.lock().serving_key.clone();
    let crl_path = serving_crl_path(state);
    let weak = Arc::downgrade(state);
    let mtime = |p: &Path| std::fs::metadata(p).and_then(|m| m.modified()).ok();
    // Start from a sentinel and check before the first sleep, so the first
    // pass always rebuilds from the *current* on-disk cert. The startup
    // acceptor was built from content read earlier in `serve`, before this
    // task captures its baseline; a renewal landing in that gap (which spans
    // bind + mDNS + pending-push I/O) would otherwise be missed forever.
    let mut last: (Option<SystemTime>, Option<SystemTime>) = (None, None);
    tokio::spawn(async move {
        loop {
            let Some(state) = weak.upgrade() else { break };
            let now = (mtime(&cert_path), mtime(&crl_path));
            if now != last {
                match load_serving_keypair(&cert_path, &key_path)
                    .and_then(|(c, k)| build_serving_acceptor(&state, &c, &k))
                {
                    Ok(acc) => {
                        *acceptor.write() = acc;
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

/// Authenticate the requesting admin — role-capable, and crucially **no CA
/// key**. Every admin-authenticated request starts here: the non-signing
/// ops (list, deny, remove-server, delegation review) end here, and the
/// signing ops authenticate this way too, then obtain the key separately
/// via [`server_unlock`]. The `Err` is a safe wire reason.
fn authenticate(
    ca: &ca_store::CaDir,
    credential: &admin_proto::AdminCredential,
) -> std::result::Result<ca_vault::Authenticated, String> {
    let result = ca.sessions.authenticate(&ca.vault.read(), credential);
    if matches!(credential, admin_proto::AdminCredential::Password { .. }) {
        record_password_auth_result(result.is_ok());
    }
    result
}

fn safe_auth_failure(
    credential: &admin_proto::AdminCredential,
    reason: String,
) -> String {
    match credential {
        // Session failures carry no password-verification oracle and tell the
        // client to discard its now-useless sealed cache.
        admin_proto::AdminCredential::Session { .. } => reason,
        admin_proto::AdminCredential::Password { .. } => {
            "authentication failed".to_string()
        }
    }
}

/// The server's OWN signing key: unlock the vault with the box-held
/// `autorenew` credential the issuer holds. This is how the server signs on
/// an authenticated, authorized admin's behalf — no admin password ever
/// reaches the key. The opportunistic CRL re-sign rides here (it needs the
/// key, and every signing op passes through). `Err` (a safe wire reason)
/// when the CA holds no autorenew credential (read-only CA). The Argon2
/// unlock runs under the vault READ lock, so it stays off the issuance
/// critical path (the store mutex) — exactly the concurrency the separate
/// store/vault locks buy.
fn server_unlock(
    ca: &ca_store::CaDir,
) -> std::result::Result<ca_vault::Unlocked, String> {
    // Read the box credential and unlock the vault under ONE vault read guard
    // (then drop it before taking the store lock for the CRL). A concurrent
    // `RotateAutorenew` re-wraps the slot and swaps the in-process credential
    // while holding the vault WRITE lock, so reading the password under the
    // read lock keeps the two consistent — we never unlock a freshly re-keyed
    // vault with the stale password. Lock order is vault-then-autorenew_pw on
    // both sides.
    let unlocked = {
        let vault = ca.vault.read();
        let pw = ca.autorenew_pw.read().clone().ok_or_else(|| {
            "this CA cannot sign: it holds no autorenew credential (the server holds the \
             only signing key). Recover with `netidx admin ca recovery rotate`."
                .to_string()
        })?;
        vault.unlock(&pw).map_err(|e| {
            format!("the CA's autorenew credential failed to unlock the key: {e:#}")
        })?
    };
    match ca.store.lock().refresh_crl_if_stale(&unlocked.ca_key_pem) {
        Ok(true) => info!("admin-server: re-signed the CRL (was nearing nextUpdate)"),
        Ok(false) => (),
        Err(e) => warn!("admin-server: opportunistic CRL refresh failed: {e:#}"),
    }
    Ok(unlocked)
}

/// Handle a sign request against the CA rooted at `ca_dir`. Auth and
/// policy failures become a `SignResponse::Err` carrying a safe reason
/// for the client; only an internal fault (e.g. the CA cert can't be
/// read) maps to a generic error response — never a panic.
pub fn handle_sign_request(ca: &ca_store::CaDir, req: &SignRequest) -> Signed {
    // A direct Sign has no queue entry; synthesize a request to carry in
    // the issued record (its fresh id keys the `issued/` file).
    let record_req = ca_store::QueuedReq::new(
        req.kind,
        req.csr_pem.clone(),
        req.requested_name.clone(),
        req.requested_validity,
        "(direct sign)".to_string(),
        None,
        None,
    );
    handle_sign_request_op(ca, req, "sign", &record_req, None)
}

/// [`handle_sign_request`] with the audit-log operation name, the
/// record's originating request, and (for the approve path) the id to
/// re-check Pending under the lock. The approve path signs through the
/// identical checks but audits as `op=approve` and keys the record by the
/// *queued* request id.
fn handle_sign_request_op(
    ca: &ca_store::CaDir,
    req: &SignRequest,
    op: &str,
    record_req: &ca_store::QueuedReq,
    recheck_id: Option<&str>,
) -> Signed {
    match try_handle(ca, req, op, record_req, recheck_id) {
        Ok(signed) => signed,
        Err(e) => Signed {
            resp: SignResponse::Err { reason: format!("internal error: {e:#}") },
            push: None,
        },
    }
}

fn try_handle(
    ca: &ca_store::CaDir,
    req: &SignRequest,
    op: &str,
    record_req: &ca_store::QueuedReq,
    recheck_id: Option<&str>,
) -> Result<Signed> {
    let failed = |resp: SignResponse| Signed { resp, push: None };
    // 1. Authenticate the REQUESTING admin — no CA key (a role admin is a
    //    first-class issuer here; the server, not the admin, holds the key).
    let authd = match authenticate(ca, &req.credential) {
        Ok(a) => a,
        Err(reason) => {
            return Ok(failed(reject(&safe_auth_failure(&req.credential, reason))));
        }
    };

    // 2. Authorize against the REQUESTER's policy. Since the server can sign
    //    anything once it unlocks (step 3), this gate is the whole boundary.
    let name = req.requested_name.trim();
    if name.is_empty() {
        return Ok(failed(reject("requested name is empty")));
    }
    // The admin server's own serving name is reserved: the trust model
    // hinges on *only* genuine daemons holding a CA-signed cert with it.
    // Issuing it via Sign — even to an admin whose policy glob (e.g. "*")
    // happens to match — would let that admin stand up an impostor
    // daemon. Refuse it unconditionally; admin servers are minted only by
    // the local setup path or a scoped server-enrollment grant.
    if name.eq_ignore_ascii_case(SERVING_SAN) {
        return Ok(failed(reject(
            "that name is reserved for the admin server and cannot be issued",
        )));
    }
    if !name_permitted(name, &authd.policy.allowed_san)? {
        return Ok(failed(reject(&format!(
            "name {name:?} is not permitted for admin {}",
            authd.admin
        ))));
    }
    let validity = req.requested_validity.min(authd.policy.max_validity);
    if validity.is_zero() {
        return Ok(failed(reject("validity must be > 0 and within policy")));
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
        if !authd.policy.id_map_groups.iter().any(|a| a == g) {
            return Ok(failed(reject(&format!(
                "id-map group {g:?} is not permitted for admin {}; allowed: {:?}",
                authd.admin, authd.policy.id_map_groups,
            ))));
        }
    }
    // 3. The server signs with its OWN credential; the requester is audited.
    let signing = match server_unlock(ca) {
        Ok(u) => u,
        Err(reason) => return Ok(failed(reject(&reason))),
    };
    // The one-live-cert check, serial allocation, sign, and atomic record
    // commit are one critical section under the issuance lock.
    issue_locked(
        ca,
        &signing,
        &authd.admin,
        record_req,
        name,
        validity,
        groups,
        one_live_name(record_req.kind),
        None,
        None,
        op,
        recheck_id,
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
    ca: &ca_store::CaDir,
    // The SERVER's autorenew-unlocked key (does the crypto)…
    signing: &ca_vault::Unlocked,
    // …vs the REQUESTING admin (named in the audit trail). They differ now:
    // the server signs, the human authorized.
    audit_admin: &str,
    record_req: &ca_store::QueuedReq,
    name: &str,
    validity: Duration,
    groups: Vec<String>,
    one_live: bool,
    // For a verified renewal: the serial of the cert being renewed. It was
    // proven live (and key-matched) at enqueue, but a revocation can land
    // between then and now — so we re-check it is STILL live under this
    // lock, and refuse the renewal if it isn't. `None` for any
    // non-renewal issuance.
    renewal_of: Option<u64>,
    serving_identity: Option<crate::tls::AdminCertIdentity>,
    audit_op: &str,
    // For the approve path: re-check the queue entry is still Pending
    // under the lock, so two approvals (or an approve racing a deny) can't
    // both transition it. `None` for direct (non-queued) issuance.
    recheck_id: Option<&str>,
) -> Result<Signed> {
    let dir = ca.dir().to_path_buf();
    let mut store = ca.store.lock();
    if let Some(id) = recheck_id {
        match store.status(id) {
            Ok(ca_store::Status::Pending(_)) => {}
            Ok(ca_store::Status::Signed(_)) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "that request was already approved".to_string(),
                    },
                    push: None,
                });
            }
            Ok(ca_store::Status::Denied(_)) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "that request was already denied".to_string(),
                    },
                    push: None,
                });
            }
            Ok(ca_store::Status::Unknown) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "no such pending request (expired or never queued)"
                            .to_string(),
                    },
                    push: None,
                });
            }
            Err(e) => return Err(e).context("re-checking the queue under the lock"),
        }
    }
    if one_live {
        let live = store.live_for_name(name).context("checking the issuance index")?;
        if !live.is_empty() {
            return Ok(Signed {
                resp: reject(&one_live_refusal(name, &live)),
                push: None,
            });
        }
    }
    // A verified renewal is a continuation of an identity that was live at
    // enqueue. Re-validate under the lock that the cert it renews is still
    // live: if it was revoked (or expired) in between, the renewal must NOT
    // re-mint it — otherwise revocation, the only containment tool, could be
    // outrun by an in-flight renewal (worst case re-minting the serving
    // SAN). Refusing here also covers the auto-renew sweep, which signs
    // through this same path.
    if let Some(serial) = renewal_of {
        let live = store.live_for_name(name).context("checking the issuance index")?;
        if !live.iter().any(|r| r.serial == serial) {
            return Ok(Signed {
                resp: reject(&format!(
                    "the certificate being renewed (serial {serial} for {name:?}) is no \
                     longer live — it may have been revoked or expired; this renewal is \
                     refused"
                )),
                push: None,
            });
        }
    }
    // Opportunistic CA-cert renewal — rare, and only allocates a serial
    // when actually renewing, so the common path burns nothing. An
    // externally-signed CA cert cannot be self-renewed (netidx doesn't
    // hold the external issuer's key); warn instead so the operator
    // re-signs out of band. (Warning fires only within the renewal
    // window; a rate limit could reduce it further if it proves noisy.)
    if crate::ca::ca_cert_needs_renewal(&dir, ca.lifetimes.ca_renew_threshold) {
        if ca.lifetimes.externally_signed {
            warn!(
                "admin-server: the externally-signed CA certificate is within its \
                 renewal threshold and will NOT auto-renew — obtain a re-signed \
                 cert from your PKI and run `netidx admin ca external install`"
            );
        } else {
            let rs = store.alloc_serial();
            match crate::ca::maybe_renew_ca_cert(
                &dir,
                &signing.ca_key_pem,
                rs,
                ca.lifetimes.ca_renew_threshold,
            ) {
                Ok(true) => info!(
                    "admin-server: renewed the CA certificate (same key; glyph unchanged)"
                ),
                Ok(false) => (),
                Err(e) => warn!("admin-server: CA renewal check failed: {e:#}"),
            }
        }
    }
    let serial = store.alloc_serial();
    let serving_identity = serving_identity.or_else(|| {
        if renewal_of.is_some() && name.eq_ignore_ascii_case(SERVING_SAN) {
            let serial = renewal_of?;
            store
                .live_for_name(name)
                .ok()?
                .into_iter()
                .find(|r| r.serial == serial)
                .and_then(|r| {
                    crate::tls::admin_cert_identity_from_pem(r.cert_pem.as_bytes()).ok()
                })
        } else {
            None
        }
    });
    if renewal_of.is_some()
        && name.eq_ignore_ascii_case(SERVING_SAN)
        && serving_identity.is_none()
    {
        return Ok(Signed {
            resp: reject(
                "the serving certificate being renewed has no valid protocol-v6 identity",
            ),
            push: None,
        });
    }
    let mut san = vec![SanEntry::Dns(name.to_string())];
    if let Some(identity) = serving_identity {
        san.push(SanEntry::Uri(identity.server_id.uri()));
        if identity.controller {
            san.push(SanEntry::Uri(admin_proto::CONTROLLER_ROLE_URI.to_string()));
        }
    }
    let resp = sign_csr(
        &dir,
        &store,
        &signing.ca_key_pem,
        &record_req.csr_pem,
        &san,
        validity,
        serial,
    )?;
    if let SignResponse::Ok { ref signed_cert_pem, .. } = resp {
        store
            .commit_issuance(record_req, serial, name, signed_cert_pem, &groups)
            .context("committing the issuance")?;
    }
    drop(store);
    audit(&dir, audit_admin, audit_op, name, validity);
    Ok(Signed {
        resp,
        push: if groups.is_empty() {
            None
        } else {
            Some(PushPlan { id: record_req.id.clone(), name: name.to_string(), groups })
        },
    })
}

/// Handle a admin-server enrollment: authenticate the admin, require the
/// scoped enrollment policy, and sign the CSR with the reserved
/// [`SERVING_SAN`].
pub fn handle_enroll_request(
    ca: &ca_store::CaDir,
    req: &EnrollRequest,
    local: bool,
    map: Option<&NetworkMap>,
) -> SignResponse {
    match try_enroll(ca, req, local, map) {
        Ok(resp) => resp,
        Err(e) => SignResponse::Err { reason: format!("internal error: {e:#}") },
    }
}

fn enrollment_cert_identity(
    local: bool,
    renew_identity: Option<admin_proto::AdminServerId>,
    map: Option<&NetworkMap>,
) -> std::result::Result<crate::tls::AdminCertIdentity, String> {
    if !local {
        return Ok(crate::tls::AdminCertIdentity {
            server_id: admin_proto::AdminServerId::new(),
            controller: false,
        });
    }
    let server_id = renew_identity.ok_or_else(|| {
        "local controller enrollment is renewal-only and requires its existing identity"
            .to_string()
    })?;
    if map.is_none_or(|map| map.controller != server_id) {
        return Err("the requested local renewal identity is not the active controller"
            .to_string());
    }
    Ok(crate::tls::AdminCertIdentity { server_id, controller: true })
}

fn try_enroll(
    ca: &ca_store::CaDir,
    req: &EnrollRequest,
    local: bool,
    map: Option<&NetworkMap>,
) -> Result<SignResponse> {
    // A request over the local control socket is already authorized as a
    // signing-tier superuser (`SO_PEERCRED` root / the daemon's own uid),
    // which carries full local authority. This is the path renewd uses to
    // re-mint the admin server's OWN serving cert without TLS — the only way
    // to recover from an already-expired serving cert, since renewing it
    // over TLS-to-self can't connect once it's expired.
    let authd = if local {
        local_superuser()
    } else {
        let authd = match authenticate(ca, &req.credential) {
            Ok(a) => a,
            Err(reason) => {
                return Ok(reject(&safe_auth_failure(&req.credential, reason)));
            }
        };
        let enrollment = admin_proto::EnrollmentRequest {
            listen: req.listen,
            roles: req.roles.clone(),
            resolver_member: req.resolver_member.clone(),
            resolver_members: req.resolver_members.clone(),
            cluster: req.cluster.clone(),
        };
        if let Err(reason) = authorize_enrollment(&authd, &enrollment, map) {
            return Ok(reject(&reason));
        }
        authd
    };
    let record_req = ca_store::QueuedReq::new(
        admin_proto::NodeKind::AdminServer,
        req.csr_pem.clone(),
        SERVING_SAN.to_string(),
        ca.lifetimes.leaf_validity,
        "(enroll)".to_string(),
        None,
        Some(admin_proto::EnrollmentRequest {
            listen: req.listen,
            roles: req.roles.clone(),
            resolver_member: req.resolver_member.clone(),
            resolver_members: req.resolver_members.clone(),
            cluster: req.cluster.clone(),
        }),
    );
    let signing = match server_unlock(ca) {
        Ok(u) => u,
        Err(reason) => return Ok(reject(&reason)),
    };
    // Serving certs aren't subject to the one-live check (many admin
    // servers legitimately hold the reserved SAN), and carry no groups.
    let identity = match enrollment_cert_identity(local, req.renew_identity, map) {
        Ok(identity) => identity,
        Err(reason) => return Ok(reject(&reason)),
    };
    if !local {
        let enrollment = admin_proto::EnrollmentRequest {
            listen: req.listen,
            roles: req.roles.clone(),
            resolver_member: req.resolver_member.clone(),
            resolver_members: req.resolver_members.clone(),
            cluster: req.cluster.clone(),
        };
        let Some(map) = map else {
            return Ok(reject("the CA-owned network map is unavailable"));
        };
        let mut staged = map.clone();
        if let Err(e) = netmap::enroll(&mut staged, identity.server_id, &enrollment) {
            return Ok(reject(&format!("invalid enrollment grant: {e:#}")));
        }
    }
    let signed = issue_locked(
        ca,
        &signing,
        &authd.admin,
        &record_req,
        SERVING_SAN,
        ca.lifetimes.leaf_validity,
        Vec::new(),
        false,
        None,
        Some(identity),
        "enroll",
        None,
    )?;
    Ok(signed.resp)
}

/// The shared signing tail of every issuance path: build a transient
/// [`Ca`] from the decrypted key, sign the CSR for exactly `name` with
/// the caller-allocated `serial`, and bundle the trust anchors.
fn sign_csr(
    dir: &Path,
    store: &ca_store::CAStore,
    ca_key_pem: &[u8],
    csr_pem: &str,
    san: &[SanEntry],
    validity: Duration,
    serial: u64,
) -> Result<SignResponse> {
    let cert_pem =
        std::fs::read(dir.join("certificate.pem")).context("reading CA certificate")?;
    let ca = Ca::from_pem(dir.to_path_buf(), ca_key_pem, &cert_pem)
        .context("loading CA from vault")?;
    let signed = ca
        .sign_request(csr_pem.as_bytes(), san, validity, serial)
        .context("signing CSR")?;
    let trusted_pem = store.read_trusted_bundle()?;
    Ok(SignResponse::Ok {
        signed_cert_pem: String::from_utf8(signed).context("signed cert not utf8")?,
        trusted_pem,
        warnings: Vec::new(),
        operation_id: None,
    })
}

/// Handle an id-map registration against the map at `map_path`. A
/// missing file starts from the seeded empty map — zero-touch joins
/// must work on a host whose id-map daemon hasn't registered anyone
/// yet. The caller holds the id-map lock.
pub fn handle_add_identity(
    map_path: &Path,
    req: &AddIdentityRequest,
) -> AddIdentityResponse {
    info!(
        "admin-server: applying id-map registration operation {} for {:?}",
        req.operation_id, req.san
    );
    let mut map = if map_path.exists() {
        match id_map::load(map_path) {
            Ok(m) => m,
            Err(e) => {
                return AddIdentityResponse::Err {
                    reason: format!("loading id-map: {e:#}"),
                };
            }
        }
    } else {
        id_map::empty()
    };
    let groups: Vec<&str> = req.groups.iter().map(|s| s.as_str()).collect();
    match id_map::register_identity(&mut map, &req.san, &req.primary_group, &groups) {
        Ok(uid) => match id_map::save(map_path, &map) {
            Ok(()) => AddIdentityResponse::Ok { uid },
            Err(e) => {
                AddIdentityResponse::Err { reason: format!("saving id-map: {e:#}") }
            }
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
    ca: &ca_store::CaDir,
    req: &EnqueueRequest,
    peer: SocketAddr,
    peer_ident: Option<&PeerIdent>,
) -> EnqueueResponse {
    // Every queued request's security code IS its CSR's SPKI fingerprint, so a
    // CSR that doesn't parse yields a code-less queue entry that no admin can
    // ever select — to approve OR deny — leaving only TTL expiry to clear it.
    // Reject an unparseable CSR now (covers both the admin-server enrollment and
    // the plain signing paths below) so the enrollee hears it immediately and
    // the queue can't be clogged with un-actionable entries.
    if let Err(e) = admin_client::csr_fingerprint(&req.csr_pem) {
        return EnqueueResponse::Err {
            reason: format!("the certificate request (CSR) could not be parsed: {e:#}"),
        };
    }
    let mut store = ca.store.lock();
    // Admin-server enrollment: the name is the reserved serving SAN by
    // definition, so none of the name rules below apply — not the
    // reserved-name refusal (this is the sanctioned way to request it)
    // and not one-live-cert (every admin server on the network holds the
    // same name). The real gate — the approving admin's
    // scoped enrollment authorization runs at approval; this entry just waits in
    // the queue under the same code-matching ceremony as any other.
    if let Some(enrollment) = &req.enrollment {
        let queued = ca_store::QueuedReq::new(
            req.kind,
            req.csr_pem.clone(),
            SERVING_SAN.to_string(),
            req.requested_validity,
            peer.to_string(),
            None,
            Some(enrollment.clone()),
        );
        return match store.enqueue(&queued) {
            Ok(()) => {
                info!(
                    "admin-server: queued enrollment {} (listen {}) from {peer}",
                    queued.id, enrollment.listen,
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
    if req.requested_validity.is_zero() {
        return EnqueueResponse::Err { reason: "validity must be > 0".to_string() };
    }
    // A renewal must prove possession of *our* live cert for this exact
    // name: the presented leaf's serial AND its key fingerprint must match
    // a live record in our index. Binding the key (not just the serial)
    // stops a co-trusted foreign CA's cert with a colliding serial from
    // passing as a renewal of ours. `renewal_of` carries the originating
    // serial forward so approval can re-check it is still live.
    let renewal_of: Option<u64> = match peer_ident {
        Some(PeerIdent {
            san,
            serial: Some(serial),
            spki_fp: Some(fp),
            admin,
            home_ca,
        }) if san.eq_ignore_ascii_case(name)
            && (!name.eq_ignore_ascii_case(SERVING_SAN)
                || (*home_ca && admin.is_some())) =>
        {
            match store.live_for_name(name) {
                Ok(live)
                    if live.iter().any(|s| s.serial == *serial && &s.spki_fp == fp) =>
                {
                    Some(*serial)
                }
                Ok(_) => None,
                Err(e) => {
                    warn!("admin-server: index lookup during enqueue failed: {e:#}");
                    None
                }
            }
        }
        _ => None,
    };
    let verified_renewal = renewal_of.is_some();
    if !verified_renewal {
        // Fail fast on the reserved name — approval would refuse it
        // anyway, but the enrollee should hear it now, not after the
        // admin clicked through. (A admin server renewing its own
        // serving cert is the legitimate exception above.)
        if name.eq_ignore_ascii_case(SERVING_SAN) {
            return EnqueueResponse::Err {
                reason: "that name is reserved for the admin server and cannot be \
                         issued"
                    .to_string(),
            };
        }
        // Same one-live-cert-per-name rule as the sign path, checked
        // here too so the enrollee hears it immediately instead of
        // after the admin clicked through an approval that would only
        // be refused. Resolver replicas are the deliberate exception: each
        // has its own key but presents the cluster's shared TLS server name.
        if one_live_name(req.kind) {
            match store.live_for_name(name) {
                Ok(live) if !live.is_empty() => {
                    return EnqueueResponse::Err {
                        reason: one_live_refusal(name, &live),
                    };
                }
                Ok(_) => (),
                Err(e) => {
                    return EnqueueResponse::Err {
                        reason: format!("checking the issuance index: {e:#}"),
                    };
                }
            }
        }
    }
    let queued = ca_store::QueuedReq::new(
        req.kind,
        req.csr_pem.clone(),
        name.to_string(),
        req.requested_validity,
        peer.to_string(),
        renewal_of,
        None,
    );
    match store.enqueue(&queued) {
        Ok(()) => {
            info!(
                "admin-server: queued {} {} for {name:?} from {peer}",
                if verified_renewal { "verified renewal" } else { "signing request" },
                queued.id
            );
            EnqueueResponse::Ok { request_id: queued.id }
        }
        Err(e) => EnqueueResponse::Err { reason: format!("{e:#}") },
    }
}

/// Human/user identities are one-live-per-name. Resolver replicas instead
/// share one verified TLS server name while retaining independent private keys.
fn one_live_name(kind: NodeKind) -> bool {
    kind != NodeKind::Resolver
}

/// List the pending queue for an authenticated admin.
fn handle_list_queue(state: &Server, req: &ListQueueRequest) -> ListQueueResponse {
    let ca = state.ca.as_ref().expect("CA role held");
    if let Err(reason) = authenticate(ca, &req.credential) {
        return ListQueueResponse::Err { reason };
    }
    let map = state.map.lock().clone();
    match ca.store.lock().pending() {
        Ok(reqs) => ListQueueResponse::Ok {
            requests: reqs
                .into_iter()
                .map(|q| {
                    let cluster_base =
                        q.enrollment.as_ref().and_then(|e| match &e.cluster {
                            admin_proto::ClusterPlacement::Create { base } => {
                                Some(base.clone())
                            }
                            admin_proto::ClusterPlacement::Join { cluster } => map
                                .clusters
                                .iter()
                                .find(|c| c.id == *cluster)
                                .map(|c| c.base.clone()),
                        });
                    QueueEntry {
                        age_secs: q.age_secs(),
                        id: q.id,
                        kind: q.kind,
                        requested_name: q.requested_name,
                        requested_validity: q.requested_validity,
                        peer: q.peer,
                        csr_pem: q.csr_pem,
                        verified_renewal: q.renewal_of.is_some(),
                        enrollment: q.enrollment,
                        cluster_base,
                    }
                })
                .collect(),
        },
        Err(e) => ListQueueResponse::Err { reason: format!("listing the queue: {e:#}") },
    }
}

struct PreparedRevoke {
    admin: String,
    warnings: Vec<String>,
    crl_pem: Option<String>,
}

/// Authenticate, apply the requested serial revocations, and re-sign the CRL.
/// The async wrapper below performs the network fanout after this
/// Argon2/signing-bound phase releases the signing semaphore.
fn prepare_revoke(
    state: &Server,
    req: &RevokeRequest,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<PreparedRevoke, String> {
    let ca = state.ca.as_ref().expect("CA role held");
    let authd = match authenticate(ca, &req.credential) {
        Ok(a) => a,
        Err(reason) => {
            return Err(safe_auth_failure(&req.credential, reason));
        }
    };
    // Revocation is privileged — revoking serving certs or another region's
    // leaves is a denial of service — and it must be **scope-bound** exactly
    // like issuance: an admin may revoke only what it could have signed.
    // Reject up front any admin with no issuance/management authority at all;
    // the per-serial check below confines the rest to their own scope.
    let broad = matches!(authd.kind, ca_vault::SlotKind::Signing)
        || authd.policy.may_manage_admins;
    if !broad
        && authd.policy.allowed_san.is_empty()
        && authd.policy.server_enroll_scopes.is_empty()
    {
        return Err(format!(
            "admin {} is not authorized to revoke certificates",
            authd.admin
        ));
    }
    if req.serials.is_empty() {
        return Err("no serials to revoke".to_string());
    }
    // Resolve each serial to the name it was issued for, so we can confine a
    // scoped admin to revoking only certs within its `allowed_san`. Read once
    // (names are immutable for a serial); the actual revoke loop runs under
    // the issuer lock.
    let records: std::collections::HashMap<u64, ca_store::IssuedRecord> =
        match ca.store.lock().list_signed() {
            Ok(records) => records.into_iter().map(|r| (r.serial, r)).collect(),
            Err(e) => {
                return Err(format!("reading the issuance index: {e:#}"));
            }
        };
    let now = ca_store::now_unix();
    let mut warnings = Vec::new();
    // Scope-check before taking the issuance lock. Removal takes the map lock
    // before the issuance lock, so looking up topology while holding the store
    // would invert that order and permit a deadlock.
    let mut serials = Vec::new();
    for serial in &req.serials {
        let mut authorized = true;
        if !broad && let Some(record) = records.get(serial) {
            authorized = if record.name.eq_ignore_ascii_case(SERVING_SAN) {
                crate::tls::admin_cert_identity_from_pem(record.cert_pem.as_bytes())
                    .ok()
                    .and_then(|identity| {
                        let map = state.map.lock();
                        let cluster = map
                            .servers
                            .iter()
                            .find(|server| server.id == identity.server_id)?
                            .cluster?;
                        map.clusters.iter().find(|entry| entry.id == cluster).map(
                            |entry| {
                                perms_scope_covers(
                                    &authd.policy.server_enroll_scopes,
                                    &entry.base,
                                )
                            },
                        )
                    })
                    .unwrap_or(false)
            } else {
                match admin_authority_over(&authd, &record.name) {
                    Ok(authorized) => authorized,
                    Err(e) => {
                        warnings.push(format!(
                            "evaluating authority for serial {serial}: {e:#}"
                        ));
                        false
                    }
                }
            };
            if !authorized {
                warnings.push(format!(
                    "serial {serial} ({:?}) is outside admin {}'s authority; skipped",
                    record.name, authd.admin
                ));
            }
        }
        if authorized {
            serials.push(*serial);
        }
    }
    // Each revocation is a read-modify-write of an `issued/<id>` record.
    // Hold the CA lock across the loop so a concurrent `set_push_done` (the
    // other read-modify-write on an issued record) can't read a stale record
    // and clobber the `revoked` flag. The CRL rewrite below re-takes the same
    // lock, so it stays outside this guard.
    {
        let mut store = ca.store.lock();
        for serial in serials {
            let rev = ca_store::Revocation {
                serial,
                revoked_unix: now,
                reason: req.reason.clone(),
            };
            match store.revoke(serial, rev) {
                Ok(true) => audit(
                    ca.dir(),
                    &authd.admin,
                    "revoke",
                    &format!("operation {operation_id}: serial {serial}"),
                    Duration::ZERO,
                ),
                Ok(false) => warnings.push(format!(
                    "serial {serial} was not live (unknown or already revoked)"
                )),
                Err(e) => warnings.push(format!("revoking serial {serial}: {e:#}")),
            }
        }
    }
    // Re-sign the CRL with the server's own key (the autorenew credential).
    let crl_pem = match server_unlock(ca) {
        Ok(signing) => {
            let path = {
                let mut store = ca.store.lock();
                match store.write_crl(&signing.ca_key_pem) {
                    Ok(()) => store.crl_path(),
                    Err(e) => {
                        warnings.push(format!(
                            "re-signing the CRL failed; immediate enforcement is unavailable: {e:#}"
                        ));
                        return Ok(PreparedRevoke {
                            admin: authd.admin,
                            warnings,
                            crl_pem: None,
                        });
                    }
                }
            };
            match std::fs::read_to_string(&path) {
                Ok(pem) => Some(pem),
                Err(e) => {
                    warnings.push(format!(
                        "reading the signed CRL for immediate distribution: {e:#}"
                    ));
                    None
                }
            }
        }
        Err(reason) => {
            warnings.push(format!(
                "re-signing the CRL failed; immediate enforcement is unavailable: {reason}"
            ));
            None
        }
    };
    Ok(PreparedRevoke { admin: authd.admin, warnings, crl_pem })
}

/// Validate that `crl_pem` is exactly one CRL signed by this node's immutable
/// home CA. Controller-only transport is the authorization boundary, while
/// this signature check prevents a corrupted payload from replacing working
/// revocation state.
fn validate_home_crl(crl_pem: &str, home_ca_der: &[u8]) -> Result<()> {
    use x509_parser::prelude::{CertificateRevocationList, FromDer, X509Certificate};
    let crls = rustls_pemfile::crls(&mut std::io::Cursor::new(crl_pem.as_bytes()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("parsing CRL PEM")?;
    let [der] = crls.as_slice() else {
        bail!("expected exactly one CRL, got {}", crls.len());
    };
    let (remaining, crl) = CertificateRevocationList::from_der(der.as_ref())
        .map_err(|e| anyhow!("parsing CRL DER: {e}"))?;
    if !remaining.is_empty() {
        bail!("CRL DER contains trailing bytes");
    }
    let (remaining, ca) = X509Certificate::from_der(home_ca_der)
        .map_err(|e| anyhow!("parsing home CA certificate: {e}"))?;
    if !remaining.is_empty() {
        bail!("home CA certificate contains trailing bytes");
    }
    crl.verify_signature(ca.public_key())
        .context("CRL signature does not verify against the home CA")
}

/// Every local trust bundle whose inbound TLS authentication is administered
/// by this daemon. The admin-plane bundle is always present; a resolver role
/// may name the same bundle more than once, so destinations are deduplicated.
fn local_crl_destinations(state: &Server) -> Result<BTreeSet<PathBuf>> {
    use netidx::resolver_server::config::file::Auth;
    let (admin_trusted, resolver_config) = {
        let cfg = state.cfg.lock();
        (cfg.trusted.clone(), cfg.roles.resolver.as_ref().map(|role| role.config.clone()))
    };
    let mut destinations = BTreeSet::new();
    destinations.insert(admin_trusted.with_file_name("crl.pem"));
    if let Some(path) = resolver_config {
        let cfg = crate::resolver::ResolverConfig::load(&path)
            .with_context(|| format!("loading resolver config {}", path.display()))?;
        for member in &cfg.as_file().member_servers {
            if let Auth::Tls { trusted, .. } = &member.auth {
                destinations
                    .insert(Path::new(trusted.as_str()).with_file_name("crl.pem"));
            }
        }
    }
    Ok(destinations)
}

/// Install a verified CRL atomically beside all local trust bundles. Identical
/// content is left untouched so file watchers do not rebuild TLS state twice.
fn apply_crl_to_destinations(
    crl_pem: &str,
    home_ca_der: &[u8],
    destinations: BTreeSet<PathBuf>,
) -> Result<()> {
    validate_home_crl(crl_pem, home_ca_der)?;
    let mut failures = Vec::new();
    for destination in destinations {
        match std::fs::read(&destination) {
            Ok(current) if current == crl_pem.as_bytes() => continue,
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                failures.push(format!("reading {}: {e}", destination.display()));
                continue;
            }
        }
        match crate::atomic::write_atomic(&destination, crl_pem.as_bytes(), 0o644) {
            Ok(()) => info!(
                "admin-server: installed immediate CRL at {}",
                destination.display()
            ),
            Err(e) => failures.push(format!("writing {}: {e:#}", destination.display())),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("one or more CRL destinations failed: {}", failures.join("; "))
    }
}

fn apply_crl_local(state: &Server, crl_pem: &str) -> Result<()> {
    apply_crl_to_destinations(
        crl_pem,
        state.home_ca_der.as_ref(),
        local_crl_destinations(state)?,
    )
}

fn handle_apply_crl(state: &Server, req: &ApplyCrlRequest) -> ApplyCrlResponse {
    info!("admin-server: applying CRL operation {}", req.operation_id);
    match apply_crl_local(state, &req.crl_pem) {
        Ok(()) => ApplyCrlResponse::Ok,
        Err(e) => ApplyCrlResponse::Err { reason: format!("{e:#}") },
    }
}

fn handle_apply_controller_state(
    state: &Server,
    req: &ApplyControllerStateRequest,
) -> ApplyControllerStateResponse {
    let err = |reason: String| ApplyControllerStateResponse::Err { reason };
    let installed_controller = state.map.lock().controller;
    if req.controller != installed_controller
        || req.map.controller != installed_controller
    {
        return err(format!(
            "controller identity mismatch (installed {}, request {}, map {})",
            installed_controller, req.controller, req.map.controller
        ));
    }
    let Some(controller) = req.map.controller_entry() else {
        return err("authoritative map has no controller entry".to_string());
    };
    if controller.id != req.controller
        || controller.addr != req.addr
        || controller.state != admin_proto::ServerState::Registered
        || !controller.roles.contains(&Role::Ca)
    {
        return err("authoritative map does not bind the claimed registered CA controller address"
            .to_string());
    }
    if req.map.version < state.map.lock().version {
        return err(format!(
            "refusing controller-state rollback from map version {} to {}",
            state.map.lock().version,
            req.map.version
        ));
    }
    if let Err(e) = validate_home_crl(&req.crl_pem, state.home_ca_der.as_ref()) {
        return err(format!("validating controller CRL: {e:#}"));
    }

    // Persist the new route before publishing the map in memory. The
    // controller itself deliberately keeps `ca_addr = None`.
    if state.ca.is_none() {
        let Some(cfg_path) = state.cfg_path.as_ref() else {
            return err(
                "this node has no persistent admin-server config path".to_string()
            );
        };
        let mut next = state.cfg.lock().clone();
        next.ca_addr = Some(req.addr);
        if let Err(e) = next.save(cfg_path) {
            return err(format!("persisting the relocated controller address: {e:#}"));
        }
        *state.cfg.lock() = next;
    }
    if let Err(e) = apply_crl_local(state, &req.crl_pem) {
        return err(format!("installing reconciled CRL: {e:#}"));
    }
    *state.map.lock() = req.map.clone();
    ApplyControllerStateResponse::Ok
}

fn registered_crl_targets(
    state: &Server,
) -> Vec<(admin_proto::AdminServerId, SocketAddr)> {
    let mut targets: Vec<_> = state
        .map
        .lock()
        .servers
        .iter()
        .filter(|server| server.state == admin_proto::ServerState::Registered)
        .map(|server| (server.id, server.addr))
        .collect();
    targets.sort_by_key(|(server, _)| *server);
    targets
}

async fn collect_peer_results<I, F, Fut>(targets: I, apply: F) -> Vec<PeerResult>
where
    I: IntoIterator<Item = (admin_proto::AdminServerId, SocketAddr)>,
    F: Fn(admin_proto::AdminServerId, SocketAddr) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let mut results: Vec<_> = stream::iter(targets.into_iter().map(|(server, addr)| {
        let future = apply(server, addr);
        async move {
            PeerResult {
                server,
                addr,
                error: future.await.err().map(|error| format!("{error:#}")),
            }
        }
    }))
    .buffer_unordered(32)
    .collect()
    .await;
    results.sort_by_key(|result| result.server);
    results
}

/// Immediately distribute a newly signed CRL to every registered node. The
/// local controller uses the same application core without a loopback TLS
/// connection; remote targets are exact-ID/home-CA pinned and bounded exactly
/// like the other controller mutation fanouts.
async fn push_crl_to_peers(
    state: &Arc<Server>,
    crl_pem: &str,
    operation_id: admin_proto::OperationId,
) -> Vec<PeerResult> {
    let targets = registered_crl_targets(state);
    let my_id = state.cfg.lock().server_id;
    let controller = state.map.lock().controller;
    let mut results = Vec::new();
    if let Some((server, addr)) = targets.iter().copied().find(|(id, _)| *id == my_id) {
        let state = state.clone();
        let crl_pem = crl_pem.to_string();
        let error =
            tokio::task::spawn_blocking(move || apply_crl_local(&state, &crl_pem))
                .await
                .map_err(|e| anyhow!("local CRL task panicked: {e}"))
                .and_then(|result| result)
                .err()
                .map(|e| format!("{e:#}"));
        results.push(PeerResult { server, addr, error });
    }
    let (cert, key) = state.outbound_identity();
    let roots = state.roots.clone();
    let home_ca = state.home_ca_der.clone();
    let mut remote = collect_peer_results(
        targets.into_iter().filter(|(server, _)| *server != my_id),
        |server, addr| {
            let cert = cert.clone();
            let key = key.clone();
            let roots = roots.clone();
            let home_ca = home_ca.clone();
            let crl_pem = crl_pem.to_string();
            async move {
                tokio::time::timeout(
                    PUSH_TIMEOUT,
                    admin_client::push_crl(
                        addr,
                        server,
                        server == controller,
                        home_ca,
                        &cert,
                        &key,
                        roots,
                        operation_id,
                        &crl_pem,
                    ),
                )
                .await
                .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
                .and_then(|result| result)
            }
        },
    )
    .await;
    results.append(&mut remote);
    results.sort_by_key(|result| result.server);
    results
}

async fn push_controller_state_to_peers(
    state: &Arc<Server>,
    operation_id: admin_proto::OperationId,
) -> Result<Vec<PeerResult>> {
    let map = state.map.lock().clone();
    let controller = map
        .controller_entry()
        .filter(|entry| entry.state == admin_proto::ServerState::Registered)
        .cloned()
        .context("the authoritative map has no registered controller")?;
    let ca =
        state.ca.as_ref().context("controller reconciliation requires the CA role")?;
    let crl_path = ca.store.lock().crl_path();
    let crl_pem = std::fs::read_to_string(&crl_path)
        .with_context(|| format!("reading current CRL {}", crl_path.display()))?;
    let request = ApplyControllerStateRequest {
        operation_id,
        controller: controller.id,
        addr: controller.addr,
        map: map.clone(),
        crl_pem,
    };
    let targets = registered_crl_targets(state);
    let my_id = state.cfg.lock().server_id;
    let mut results = Vec::new();
    if let Some((server, addr)) = targets.iter().copied().find(|(id, _)| *id == my_id) {
        let state = state.clone();
        let request = request.clone();
        let error = tokio::task::spawn_blocking(move || {
            match handle_apply_controller_state(&state, &request) {
                ApplyControllerStateResponse::Ok => Ok(()),
                ApplyControllerStateResponse::Err { reason } => bail!(reason),
            }
        })
        .await
        .map_err(|e| anyhow!("local controller-state task panicked: {e}"))
        .and_then(|result| result)
        .err()
        .map(|e| format!("{e:#}"));
        results.push(PeerResult { server, addr, error });
    }
    let (cert, key) = state.outbound_identity();
    let roots = state.roots.clone();
    let home_ca = state.home_ca_der.clone();
    let mut remote = collect_peer_results(
        targets.into_iter().filter(|(server, _)| *server != my_id),
        |server, addr| {
            let cert = cert.clone();
            let key = key.clone();
            let roots = roots.clone();
            let home_ca = home_ca.clone();
            let request = request.clone();
            async move {
                tokio::time::timeout(
                    PUSH_TIMEOUT,
                    admin_client::push_controller_state(
                        addr,
                        server,
                        server == controller.id,
                        home_ca,
                        &cert,
                        &key,
                        roots,
                        request,
                    ),
                )
                .await
                .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
                .and_then(|result| result)
            }
        },
    )
    .await;
    results.append(&mut remote);
    results.sort_by_key(|result| result.server);
    Ok(results)
}

async fn reconcile_controller_state_on_start(state: Arc<Server>) {
    let operation_id = admin_proto::OperationId::new();
    let Some(ca) = state.ca.as_ref() else { return };
    audit(
        ca.dir(),
        "(startup)",
        "reconcile-controller",
        &format!("operation {operation_id}: startup reconciliation"),
        Duration::ZERO,
    );
    match push_controller_state_to_peers(&state, operation_id).await {
        Ok(results) => {
            for result in results {
                if let Some(error) = result.error {
                    warn!(
                        "admin-server: startup controller reconciliation {} at {} failed: {}",
                        result.server, result.addr, error
                    );
                }
            }
        }
        Err(e) => warn!("admin-server: startup controller reconciliation failed: {e:#}"),
    }
}

async fn handle_reconcile_controller(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    req: &admin_proto::ReconcileControllerRequest,
    local: bool,
) -> ReconcileControllerResponse {
    let Some(ca) = state.ca.as_ref() else {
        return ReconcileControllerResponse::Err {
            reason: "controller reconciliation must be sent to the CA controller"
                .to_string(),
        };
    };
    let admin = if local {
        "local".to_string()
    } else {
        let state = state.clone();
        let credential = req.credential.clone();
        match run_signing(signs, move || {
            authenticate(state.ca.as_ref().expect("CA role held"), &credential)
                .map(|authd| authd.admin)
        })
        .await
        {
            Ok(Ok(admin)) => admin,
            Ok(Err(reason)) => return ReconcileControllerResponse::Err { reason },
            Err(e) => {
                return ReconcileControllerResponse::Err {
                    reason: format!("authentication task failed: {e:#}"),
                };
            }
        }
    };
    let operation_id = admin_proto::OperationId::new();
    audit(
        ca.dir(),
        &admin,
        "reconcile-controller",
        &format!("operation {operation_id}"),
        Duration::ZERO,
    );
    match push_controller_state_to_peers(state, operation_id).await {
        Ok(peers) => ReconcileControllerResponse::Ok { operation_id, peers },
        Err(e) => ReconcileControllerResponse::Err { reason: format!("{e:#}") },
    }
}

async fn handle_revoke(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    req: &RevokeRequest,
) -> RevokeResponse {
    let operation_id = admin_proto::OperationId::new();
    let prepared = {
        let state = state.clone();
        let req = req.clone();
        run_signing(signs, move || prepare_revoke(&state, &req, operation_id)).await
    };
    let PreparedRevoke { admin, mut warnings, crl_pem } = match prepared {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(reason)) => return RevokeResponse::Err { reason },
        Err(e) => {
            return RevokeResponse::Err { reason: format!("revoke task panicked: {e}") };
        }
    };
    audit(
        state.ca.as_ref().expect("CA role held").dir(),
        &admin,
        "fanout-crl",
        &format!("operation {operation_id}"),
        Duration::ZERO,
    );
    let peers = match crl_pem {
        Some(crl_pem) => push_crl_to_peers(state, &crl_pem, operation_id).await,
        None => {
            let reason =
                "fresh CRL unavailable; immediate distribution was not attempted";
            warnings.push(reason.to_string());
            registered_crl_targets(state)
                .into_iter()
                .map(|(server, addr)| PeerResult {
                    server,
                    addr,
                    error: Some(reason.to_string()),
                })
                .collect()
        }
    };
    RevokeResponse::Ok { warnings, operation_id: Some(operation_id), peers }
}

/// List every issued certificate (admin-authenticated) — the revoke UI
/// and inspection. The daemon owns the index.
fn handle_list_issued(
    ca: &ca_store::CaDir,
    req: &ListIssuedRequest,
) -> ListIssuedResponse {
    if let Err(reason) = authenticate(ca, &req.credential) {
        return ListIssuedResponse::Err { reason };
    }
    match ca.store.lock().list_signed() {
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
        Err(e) => {
            ListIssuedResponse::Err { reason: format!("listing issued certs: {e:#}") }
        }
    }
}

/// A successful [`handle_approve`]: the signed outcome plus what the
/// dispatch arm needs to finish the job — the push plan for id-map
/// registration, and the peer address to record when the approved
/// entry was a admin-server enrollment.
struct Approved {
    resp: SignResponse,
    push: Option<PushPlan>,
    enrollment: Option<(admin_proto::AdminServerId, admin_proto::EnrollmentRequest)>,
}

/// Approve a queued request: look it up, then sign it through the
/// exact same checks a synchronous [`SignRequest`] goes through (the
/// admin's SAN globs, validity cap, id-map group allowed-set), audited
/// as `op=approve`. The outer `Err` is a safe wire reason for
/// before-the-sign failures.
fn handle_approve(
    ca: &ca_store::CaDir,
    req: &ApproveRequest,
    map: Option<&NetworkMap>,
) -> std::result::Result<Approved, String> {
    // This cheap precheck (no auth) rejects an already-terminal request;
    // the authoritative re-check happens under the lock.
    let queued = match ca.store.lock().status(&req.request_id) {
        Ok(ca_store::Status::Pending(q)) => q,
        Ok(ca_store::Status::Signed(_)) => {
            return Err("that request was already approved".to_string());
        }
        Ok(ca_store::Status::Denied(_)) => {
            return Err("that request was already denied".to_string());
        }
        Ok(ca_store::Status::Unknown) => {
            return Err("no such pending request (expired or never queued)".to_string());
        }
        Err(e) => return Err(format!("reading the queue: {e:#}")),
    };
    approve_locked(ca, req, queued, map)
}

/// Sign a queued request through the same checks a synchronous Sign goes
/// through (or, for a verified renewal / admin-server enrollment, the
/// narrower continuation gate), committing the issuance atomically under
/// the issuer lock. `queued` came from the cheap precheck; `issue_locked`
/// re-checks it is still Pending under the lock.
fn approve_locked(
    ca: &ca_store::CaDir,
    req: &ApproveRequest,
    queued: ca_store::QueuedReq,
    map: Option<&NetworkMap>,
) -> std::result::Result<Approved, String> {
    // A queued admin-server enrollment: gated on the approving admin's
    // scoped enrollment authority; signs the reserved serving SAN; no one-live
    // check and no id-map groups (a admin server isn't a user).
    if let Some(enrollment) = queued.enrollment.clone() {
        let authd = authenticate(ca, &req.credential)?;
        authorize_enrollment(&authd, &enrollment, map)?;
        let server_id = admin_proto::AdminServerId::new();
        let mut staged = map
            .cloned()
            .ok_or_else(|| "the CA-owned network map is unavailable".to_string())?;
        netmap::enroll(&mut staged, server_id, &enrollment)
            .map_err(|e| format!("invalid enrollment grant: {e:#}"))?;
        let signing = server_unlock(ca)?;
        let signed = issue_locked(
            ca,
            &signing,
            &authd.admin,
            &queued,
            SERVING_SAN,
            ca.lifetimes.leaf_validity,
            Vec::new(),
            false,
            None,
            Some(crate::tls::AdminCertIdentity { server_id, controller: false }),
            "enroll",
            Some(&req.request_id),
        )
        .map_err(|e| format!("internal error: {e:#}"))?;
        let enrollment = matches!(&signed.resp, SignResponse::Ok { .. })
            .then_some((server_id, enrollment));
        return Ok(Approved { resp: signed.resp, push: signed.push, enrollment });
    }
    // A verified renewal: continuation of an already-approved identity —
    // possession of the live key was proven at enqueue. The SAN-scope and
    // one-live-cert checks don't apply (a renewal's name *does* have a live
    // cert), the reserved serving name is allowed (admin servers renew
    // themselves), and the id-map is untouched (requested groups ignored).
    // `issue_locked` re-checks the renewed serial is STILL live under the
    // lock, so a revocation since enqueue refuses the renewal.
    if let Some(orig_serial) = queued.renewal_of {
        let authd = authenticate(ca, &req.credential)?;
        let validity = queued
            .requested_validity
            .min(authd.policy.max_validity)
            .max(Duration::from_secs(1));
        let signing = server_unlock(ca)?;
        let signed = issue_locked(
            ca,
            &signing,
            &authd.admin,
            &queued,
            &queued.requested_name,
            validity,
            Vec::new(),
            false,
            Some(orig_serial),
            None,
            "renew",
            Some(&req.request_id),
        )
        .map_err(|e| format!("internal error: {e:#}"))?;
        return Ok(Approved { resp: signed.resp, push: signed.push, enrollment: None });
    }
    // Ordinary approval: the full policy checks under the *approving*
    // admin's slot, audited as `op=approve`, the record keyed by the
    // queued request's id so the enrollee polls it back.
    let sign_req = SignRequest {
        kind: queued.kind,
        credential: req.credential.clone(),
        csr_pem: queued.csr_pem.clone(),
        requested_name: queued.requested_name.clone(),
        requested_validity: queued.requested_validity,
        id_map_groups: req.id_map_groups.clone(),
    };
    let signed =
        handle_sign_request_op(ca, &sign_req, "approve", &queued, Some(&req.request_id));
    Ok(Approved { resp: signed.resp, push: signed.push, enrollment: None })
}

/// Read the autorenew slot's password from its keytab, unsealing if this
/// host sealed it to its TPM (the install path seals when it can). A
/// sealed keytab that won't unseal is a hard error here — but the caller
/// only logs it and skips spawning the approver, so the rest of the
/// daemon serves regardless; renewals just fall back to human approval.
pub fn read_autorenew_password(keytab: &Path) -> Result<Zeroizing<String>> {
    let raw = std::fs::read(keytab)
        .with_context(|| format!("reading autorenew keytab {}", keytab.display()))?;
    if netidx_tpm::is_sealed(&raw) {
        let secret = netidx_tpm::unseal(&raw).with_context(|| {
            format!(
                "unsealing autorenew keytab {} — if this host's TPM was cleared \
                 or the board was replaced, mint a fresh keytab with \
                 `netidx admin ca auto-approve --rotate`",
                keytab.display()
            )
        })?;
        Ok(Zeroizing::new(
            String::from_utf8(secret.to_vec())
                .context("sealed autorenew keytab payload is not utf8")?,
        ))
    } else {
        let pw = String::from_utf8(raw).with_context(|| {
            format!("autorenew keytab {} is not utf8", keytab.display())
        })?;
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
fn autorenew_sweep(ca: &ca_store::CaDir, password: &str) -> usize {
    let pending = match ca.store.lock().pending() {
        Ok(p) => p,
        Err(e) => {
            warn!("autorenew: scanning the queue failed: {e:#}");
            return 0;
        }
    };
    let mut approved = 0;
    for q in pending.iter().filter(|q| q.renewal_of.is_some()) {
        let req = ApproveRequest {
            credential: admin_proto::AdminCredential::password(AUTORENEW_ADMIN, password),
            request_id: q.id.clone(),
            id_map_groups: Vec::new(),
        };
        match handle_approve(ca, &req, None) {
            Ok(Approved { resp: SignResponse::Ok { .. }, .. }) => {
                approved += 1;
                info!("autorenew: approved renewal of {:?}", q.requested_name);
            }
            Ok(Approved { resp: SignResponse::Err { reason }, .. }) => {
                warn!(
                    "autorenew: signing renewal of {:?} failed: {reason}",
                    q.requested_name
                )
            }
            Err(reason) => {
                warn!(
                    "autorenew: approving renewal of {:?} failed: {reason}",
                    q.requested_name
                )
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
/// How often a non-CA admin server re-asserts its own facts to the CA and
/// refreshes its cached map (a cheap version probe; a full pull only when
/// it changed). Short enough that `update` sees recent changes, infrequent
/// enough to be free on a control plane.
const MAP_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// On a non-CA admin server, keep the cached network map current and keep
/// our own entry registered with the CA. The CA owns the map; we are a
/// read-replica — if the CA is unreachable we keep serving the last copy
/// we cached, and the re-register self-heals a push lost while it was down.
fn spawn_map_refresh(state: &Arc<Server>) {
    if state.cfg.lock().roles.ca.is_some() {
        return; // the CA owns the map — nothing to refresh
    }
    let weak = Arc::downgrade(state);
    tokio::spawn(async move {
        loop {
            let Some(state) = weak.upgrade() else { break };
            let (cert, key) = state.outbound_identity();
            let (ca_addr, req, roots) = {
                let cfg = state.cfg.lock();
                (
                    cfg.ca_addr,
                    RegisterRequest {
                        addr: cfg.listen,
                        resolver: local_cluster_facts(&cfg),
                    },
                    state.roots.clone(),
                )
            };
            let Some(ca_addr) = ca_addr else {
                drop(state);
                tokio::time::sleep(MAP_REFRESH_INTERVAL).await;
                continue;
            };
            // Self-heal: (re)register our own facts. Idempotent at the CA.
            if let Err(e) =
                admin_client::register(ca_addr, &cert, &key, roots.clone(), &req).await
            {
                warn!(
                    "admin-server: registering with the CA {ca_addr} failed (will retry): {e:#}"
                );
            }
            // Refresh the cache: cheap version check, full pull only when changed.
            match admin_client::get_map_version_from_controller(
                ca_addr,
                roots.clone(),
                state.home_ca_der.clone(),
                NodeKind::AdminServer,
            )
            .await
            {
                Ok(v) => {
                    let stale = state.map.lock().version != v;
                    if stale {
                        match admin_client::get_map_from_controller(
                            ca_addr,
                            roots,
                            state.home_ca_der.clone(),
                            NodeKind::AdminServer,
                        )
                        .await
                        {
                            Ok(map) => *state.map.lock() = map,
                            Err(e) => warn!(
                                "admin-server: pulling the network map from {ca_addr} failed \
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

fn spawn_autorenew(state: &Arc<Server>) {
    if state.ca.is_none() {
        return;
    }
    // Enable autorenew only if the CA currently holds a signing credential.
    // The sweep re-reads it each iteration (see below), so this is just the
    // initial gate; `None` ⇒ a read-only CA with nothing to auto-approve.
    if state.ca.as_ref().expect("CA role held").autorenew_pw.read().is_none() {
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
            let Some(pw) =
                state.ca.as_ref().expect("CA role held").autorenew_pw.read().clone()
            else {
                tokio::time::sleep(AUTORENEW_POLL).await;
                continue;
            };
            let gate = state.mutation_gate.clone().read_owned().await;
            // Hand the Arc to the blocking task and let it drop there, so
            // we never hold the server alive across the sleep below.
            if let Err(e) = tokio::task::spawn_blocking(move || {
                let _gate = gate;
                autorenew_sweep(state.ca.as_ref().expect("CA role held"), &pw);
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
fn handle_deny(
    ca: &ca_store::CaDir,
    req: &DenyRequest,
    map: Option<&NetworkMap>,
) -> DenyResponse {
    // Cheap precheck (no auth) for an already-terminal/unknown request.
    let queued = match ca.store.lock().status(&req.request_id) {
        Ok(ca_store::Status::Pending(q)) => q,
        Ok(ca_store::Status::Signed(_)) => {
            return DenyResponse::Err {
                reason: "that request was already approved".to_string(),
            };
        }
        Ok(ca_store::Status::Denied(_)) => {
            return DenyResponse::Err {
                reason: "that request was already denied".to_string(),
            };
        }
        Ok(ca_store::Status::Unknown) => {
            return DenyResponse::Err {
                reason: "no such pending request (expired or never queued)".to_string(),
            };
        }
        Err(e) => {
            return DenyResponse::Err { reason: format!("reading the queue: {e:#}") };
        }
    };
    let authd = match authenticate(ca, &req.credential) {
        Ok(a) => a,
        Err(reason) => return DenyResponse::Err { reason },
    };
    // Denying a queued request blocks an issuance, so — like revoke — it is
    // scope-bound: an admin may deny only a request for a name it could have
    // signed (a serving-cert enrollment needs scoped enrollment authority).
    let authority = if let Some(enrollment) = &queued.enrollment {
        if matches!(authd.kind, ca_vault::SlotKind::Signing) {
            Ok(true)
        } else {
            authorize_enrollment(&authd, enrollment, map).map(|()| true)
        }
    } else {
        admin_authority_over(&authd, &queued.requested_name).map_err(|e| format!("{e:#}"))
    };
    match authority {
        Ok(true) => {}
        Ok(false) => {
            return DenyResponse::Err {
                reason: format!(
                    "admin {} is not authorized to deny requests for {:?}",
                    authd.admin, queued.requested_name
                ),
            };
        }
        Err(e) => {
            return DenyResponse::Err { reason: format!("evaluating authority: {e}") };
        }
    }
    let mut store = ca.store.lock();
    // Authoritative re-check under the lock (mutually exclusive with the
    // approve commit, which also holds this lock).
    match store.status(&req.request_id) {
        Ok(ca_store::Status::Pending(_)) => {}
        Ok(ca_store::Status::Signed(_)) => {
            return DenyResponse::Err {
                reason: "that request was already approved".to_string(),
            };
        }
        Ok(ca_store::Status::Denied(_)) => {
            return DenyResponse::Err {
                reason: "that request was already denied".to_string(),
            };
        }
        Ok(ca_store::Status::Unknown) => {
            return DenyResponse::Err {
                reason: "no such pending request (expired or never queued)".to_string(),
            };
        }
        Err(e) => {
            return DenyResponse::Err { reason: format!("reading the queue: {e:#}") };
        }
    }
    match store.deny(&queued, &req.reason) {
        Ok(()) => {
            audit(ca.dir(), &authd.admin, "deny", &queued.requested_name, Duration::ZERO);
            DenyResponse::Ok
        }
        Err(e) => DenyResponse::Err { reason: format!("storing the denial: {e:#}") },
    }
}

// -- resolver hierarchy delegation -------------------------------------------

/// Structural check on a proposed delegation subtree (the authoritative
/// children-constraint check happens at approval, via `validate_for_path`).
fn validate_delegation_path(path: &str) -> Result<()> {
    use netidx::path::Path as NPath;
    let p = NPath::from(String::from(path));
    if !NPath::is_absolute(&p) {
        bail!("delegation path must be absolute (got {path:?})");
    }
    if p.as_ref() == "/" {
        bail!("the root path cannot be delegated");
    }
    Ok(())
}

/// `RequestDelegation` (unauthenticated): structural checks then enqueue.
fn handle_request_delegation(
    state: &Server,
    req: &DelegationRequest,
    peer: SocketAddr,
) -> DelegationResponse {
    let Some(ca_dir) = state.ca_dir() else {
        return DelegationResponse::Err {
            reason: "this host is not the controller".into(),
        };
    };
    if let Err(e) = validate_delegation_path(&req.proposed_path) {
        return DelegationResponse::Err { reason: format!("{e:#}") };
    }
    let pending = delegation_store::PendingDelegation::new(
        req.proposed_path.clone(),
        req.parent_servers.clone(),
        req.child_servers.clone(),
        peer.to_string(),
    );
    // Validate the complete proposal against one snapshot without changing the
    // authoritative map. Approval re-validates under the resolver-edit lock.
    let mut staged = state.map.lock().clone();
    if let Err(e) = netmap::delegate(
        &mut staged,
        &pending.proposed_path,
        pending.proposed_child,
        &pending.parent_servers,
        &pending.child_servers,
    ) {
        return DelegationResponse::Err { reason: format!("invalid delegation: {e:#}") };
    }
    match delegation_store::enqueue(ca_dir, &pending) {
        Ok(()) => DelegationResponse::Ok { request_id: pending.id },
        Err(e) => DelegationResponse::Err { reason: format!("{e:#}") },
    }
}

/// `PollDelegation` (unauthenticated): map the stored status to the wire.
fn handle_poll_delegation(ca_dir: &Path, req: &PollRequest) -> DelegationPollResponse {
    match delegation_store::status(ca_dir, &req.request_id) {
        Ok(delegation_store::Status::Pending(_)) => DelegationPollResponse::Pending,
        Ok(delegation_store::Status::Approved { parent }) => {
            DelegationPollResponse::Approved { parent }
        }
        Ok(delegation_store::Status::Denied { reason }) => {
            DelegationPollResponse::Denied { reason }
        }
        Ok(delegation_store::Status::Unknown) | Err(_) => DelegationPollResponse::Unknown,
    }
}

/// `ListDelegations` (admin-authenticated): pending requests plus approved
/// requests that remain available for idempotent reconciliation.
fn handle_list_delegations(
    state: &Server,
    req: &ListDelegationsRequest,
) -> ListDelegationsResponse {
    let Some(ca) = state.ca.as_ref() else {
        return ListDelegationsResponse::Err {
            reason: "this host is not the controller".into(),
        };
    };
    if let Err(reason) = authenticate(ca, &req.credential) {
        return ListDelegationsResponse::Err { reason };
    }
    let dir = ca.dir().to_path_buf();
    let map = state.map.lock();
    let reqs = delegation_store::pending(&dir).and_then(|pending| {
        let mut reqs: Vec<_> = pending.into_iter().map(|req| (req, false)).collect();
        reqs.extend(
            delegation_store::approved(&dir)?.into_iter().map(|rec| (rec.req, true)),
        );
        reqs.sort_by_key(|(req, _)| req.received_unix);
        Ok(reqs)
    });
    match reqs {
        Ok(reqs) => ListDelegationsResponse::Ok {
            requests: reqs
                .into_iter()
                .filter_map(|(r, approved)| {
                    let mut staged = map.clone();
                    let change = netmap::delegate(
                        &mut staged,
                        &r.proposed_path,
                        r.proposed_child,
                        &r.parent_servers,
                        &r.child_servers,
                    )
                    .ok()?;
                    Some(DelegationEntry {
                        age_secs: r.age_secs(),
                        id: r.id,
                        proposed_path: r.proposed_path,
                        parent_servers: r.parent_servers,
                        child_servers: r.child_servers,
                        parent: change.parent.id,
                        child: change.child.id,
                        parent_base: change.parent.base,
                        child_base: change.child.base,
                        parent_members: change.parent.members,
                        child_members: change.child.members,
                        approved,
                        peer: r.peer,
                    })
                })
                .collect(),
        },
        Err(e) => ListDelegationsResponse::Err { reason: format!("{e:#}") },
    }
}

/// `DenyDelegation` (admin-authenticated): deny a pending request, under
/// the resolver-edit lock so it's mutually exclusive with approve.
fn handle_deny_delegation(
    state: &Server,
    req: &DenyDelegationRequest,
) -> DenyDelegationResponse {
    let ca_dir = match state.ca_dir() {
        Some(d) => d.to_path_buf(),
        None => {
            return DenyDelegationResponse::Err {
                reason: "this host does not hold the CA".to_string(),
            };
        }
    };
    let authd =
        match authenticate(state.ca.as_ref().expect("CA role held"), &req.credential) {
            Ok(a) => a,
            Err(reason) => return DenyDelegationResponse::Err { reason },
        };
    let _guard = state.resolver_edit_lock.lock();
    match delegation_store::read_pending(&ca_dir, &req.request_id) {
        Ok(Some(pending)) => {
            if !delegation_authority(&authd, &pending.proposed_path) {
                return DenyDelegationResponse::Err {
                    reason: format!(
                        "admin {} is not authorized to decide delegations at {:?}",
                        authd.admin, pending.proposed_path
                    ),
                };
            }
            match delegation_store::deny(&ca_dir, &pending, &req.reason) {
                Ok(()) => DenyDelegationResponse::Ok,
                Err(e) => DenyDelegationResponse::Err { reason: format!("{e:#}") },
            }
        }
        Ok(None) => DenyDelegationResponse::Err {
            reason: "no such pending delegation request (expired, never queued, or \
                     already decided)"
                .to_string(),
        },
        Err(e) => DenyDelegationResponse::Err { reason: format!("{e:#}") },
    }
}

fn info_to_refauth(a: &InfoAuth) -> netidx::resolver_server::config::file::RefAuth {
    use netidx::resolver_server::config::file::RefAuth;
    match a {
        InfoAuth::Anonymous => RefAuth::Anonymous,
        InfoAuth::Krb5 { spn } => RefAuth::Krb5(arcstr::ArcStr::from(spn.as_str())),
        InfoAuth::Tls { name } => RefAuth::Tls(arcstr::ArcStr::from(name.as_str())),
    }
}

/// Apply a referral edit to the local resolver config — add/replace a
/// child, or set the parent. Idempotent (re-applying the same edit is a
/// no-op); validated via `validate_for_path` (so children-constraint
/// violations fail here) before the atomic save.
fn apply_referral_edit_local(
    resolver_config_path: &Path,
    edit: &ReferralEdit,
) -> Result<()> {
    use netidx::resolver_server::config::file::Referral;
    let mut rc = crate::resolver::ResolverConfig::load(resolver_config_path)
        .with_context(|| {
            format!("loading resolver config {}", resolver_config_path.display())
        })?;
    match edit {
        ReferralEdit::SetTopology { local_member, members, parent, children } => {
            if !members.contains(local_member) {
                bail!(
                    "local resolver member {} is absent from its assigned cluster",
                    local_member.addr
                );
            }
            let old_members = rc.as_file().member_servers.clone();
            let configured = rc.resolver_addrs();
            let local_block = old_members
                .iter()
                .find(|configured| configured.addr == local_member.addr)
                .filter(|_| configured.contains(local_member))
                .cloned()
                .context(
                    "the target's owned resolver member does not match its local config",
                )?;
            // `member_servers` are independent local launch choices, not a
            // replica roster. Preserve whichever in-cluster convenience
            // blocks this file already has, drop blocks moved to the other
            // side of a split, and never synthesize missing peers.
            let mut ordered = vec![local_block];
            for existing in
                old_members.iter().filter(|existing| existing.addr != local_member.addr)
            {
                if let Some(desired) =
                    members.iter().find(|member| member.addr == existing.addr)
                {
                    if !configured.contains(desired) {
                        bail!(
                            "resolver member {} does not match its local configured authentication",
                            desired.addr
                        );
                    }
                    ordered.push(existing.clone());
                }
            }
            rc.as_file_mut().member_servers = ordered;
            rc.as_file_mut().parent = parent.as_ref().map(|edge| Referral {
                path: arcstr::ArcStr::from(edge.path.as_str()),
                ttl: None,
                addrs: edge
                    .addrs
                    .iter()
                    .map(|r| (r.addr, info_to_refauth(&r.auth)))
                    .collect(),
            });
            rc.as_file_mut().children = children
                .iter()
                .map(|edge| Referral {
                    path: arcstr::ArcStr::from(edge.path.as_str()),
                    ttl: None,
                    addrs: edge
                        .addrs
                        .iter()
                        .map(|r| (r.addr, info_to_refauth(&r.auth)))
                        .collect(),
                })
                .collect();
        }
    }
    rc.save(resolver_config_path)
        .context("the referral edit would make the resolver config invalid")
}

/// `ApplyReferralEdit` (server-to-server, peer-cert-gated): the receive
/// side of cluster-wide delegation propagation. Requires a resolver role
/// (the config to edit).
fn handle_apply_referral_edit(
    state: &Server,
    req: &ApplyReferralEditRequest,
) -> ApplyReferralEditResponse {
    info!(
        "admin-server: applying referral operation {}: {:?}",
        req.operation_id, req.edit
    );
    let path = match state.cfg.lock().roles.resolver.as_ref().map(|r| r.config.clone()) {
        Some(p) => p,
        None => {
            return ApplyReferralEditResponse::Err {
                reason: "this host has no resolver role to edit".to_string(),
            };
        }
    };
    let _guard = state.resolver_edit_lock.lock();
    match apply_referral_edit_local(&path, &req.edit) {
        Ok(()) => ApplyReferralEditResponse::Ok,
        Err(e) => ApplyReferralEditResponse::Err { reason: format!("{e:#}") },
    }
}

/// The role list a admin-server config implies.
fn roles_of(cfg: &AdminServerConfig) -> Vec<Role> {
    let mut out = Vec::new();
    if cfg.roles.ca.is_some() {
        out.push(Role::Ca);
    }
    if cfg.roles.resolver.is_some() {
        out.push(Role::Resolver);
    }
    if cfg.roles.id_map.is_some() {
        out.push(Role::IdMap);
    }
    out
}

/// This host's own [`ServerEntry`] for the network map: its listen
/// address, its roles, and — if it runs a resolver — its cluster facts.
fn local_cluster_facts(cfg: &AdminServerConfig) -> Option<admin_proto::ClusterFacts> {
    cfg.roles.resolver.as_ref().and_then(|r| match crate::resolver::ResolverConfig::load(
        &r.config,
    ) {
        Ok(rc) => Some(rc.cluster_facts()),
        Err(e) => {
            warn!(
                "admin-server: deriving own cluster facts from {}: {e:#}",
                r.config.display()
            );
            None
        }
    })
}

/// This host's own resolver base (the single level a local, control-socket
/// caller may edit permissions at). `None` when this host serves no resolver.
fn own_base(state: &Server) -> Option<String> {
    let id = state.cfg.lock().server_id;
    let map = state.map.lock();
    let cluster = map.servers.iter().find(|s| s.id == id)?.cluster?;
    map.clusters.iter().find(|c| c.id == cluster).map(|c| c.base.clone())
}

fn grant_enrollment(
    state: &Server,
    server_id: admin_proto::AdminServerId,
    enrollment: &admin_proto::EnrollmentRequest,
) -> Result<admin_proto::ResolverClusterId> {
    let ca_dir = state.ca_dir().context("this host does not hold the CA")?;
    let mut map = state.map.lock();
    let mut staged = map.clone();
    let cluster = netmap::enroll(&mut staged, server_id, enrollment)?;
    netmap::save(ca_dir, &staged).context("persisting the enrollment grant")?;
    *map = staged;
    Ok(cluster)
}

/// CA-side: register/update a admin server's facts in the authoritative
/// map and persist it. Peer-cert-gated at the dispatch. The CA is the
/// map's only writer, so a non-CA host refuses.
fn handle_register(
    state: &Server,
    server_id: admin_proto::AdminServerId,
    req: &RegisterRequest,
) -> RegisterResponse {
    let ca_dir = match state.ca_dir() {
        Some(d) => d.to_path_buf(),
        None => {
            return RegisterResponse::Err {
                reason: "this host does not hold the CA — register with the CA"
                    .to_string(),
            };
        }
    };
    let mut map = state.map.lock();
    let updated =
        match netmap::register(&mut map, server_id, req.addr, req.resolver.as_ref()) {
            Ok(updated) => updated,
            Err(e) => return RegisterResponse::Err { reason: format!("{e:#}") },
        };
    if updated {
        if let Err(e) = netmap::save(&ca_dir, &map) {
            return RegisterResponse::Err {
                reason: format!("persisting the network map: {e:#}"),
            };
        }
    }
    RegisterResponse::Ok { version: map.version }
}

/// CA-side: drop a admin server from the map on uninstall.
fn handle_deregister(
    state: &Server,
    server_id: admin_proto::AdminServerId,
) -> RegisterResponse {
    let ca_dir = match state.ca_dir() {
        Some(d) => d.to_path_buf(),
        None => {
            return RegisterResponse::Err {
                reason: "this host does not hold the CA".to_string(),
            };
        }
    };
    let mut map = state.map.lock();
    let updated = match netmap::deregister(&mut map, server_id) {
        Ok(updated) => updated,
        Err(e) => return RegisterResponse::Err { reason: format!("{e:#}") },
    };
    if updated {
        if let Err(e) = netmap::save(&ca_dir, &map) {
            return RegisterResponse::Err {
                reason: format!("persisting the network map: {e:#}"),
            };
        }
    }
    RegisterResponse::Ok { version: map.version }
}

struct RemoveServerPrepare {
    version: u64,
    revoked: u64,
    removed: bool,
    affected_clusters: Vec<String>,
    fanout: TopologyFanout,
    crl_pem: Option<String>,
}

/// The blocking half of permanent server removal: authenticate, validate the
/// transition, revoke every certificate for the immutable identity, and commit
/// the new authoritative map. The returned topology fanout is deliberately
/// separate: network I/O must not hold the map lock or signing semaphore.
fn remove_server_prepare(
    state: &Server,
    req: &RemoveServerRequest,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<RemoveServerPrepare, RemoveServerResponse> {
    let err = |reason: String| RemoveServerResponse::Err { reason };
    let ca_dir = match state.ca_dir() {
        Some(d) => d.to_path_buf(),
        None => return Err(err("this host does not hold the CA".to_string())),
    };
    let authd =
        match authenticate(state.ca.as_ref().expect("CA role held"), &req.credential) {
            Ok(a) => a,
            Err(reason) => return Err(err(reason)),
        };
    // Evicting a admin server from the authoritative map cascades that host's
    // resolver-cluster facts out of the map — a privileged, network-affecting
    // edit. Gate it on the admin-server lifecycle capability (the same bit
    // that authorizes enrolling one) or a broad admin.
    let broad = matches!(authd.kind, ca_vault::SlotKind::Signing)
        || authd.policy.may_manage_admins;
    // Validate the authoritative-map transition on a copy first. Certificate
    // revocation is irreversible, so do not begin it for an invalid removal
    // (notably, removal of the active controller).
    let mut map = state.map.lock();
    let target_base = map
        .servers
        .iter()
        .find(|server| server.id == req.server)
        .and_then(|server| server.cluster)
        .and_then(|cluster| map.clusters.iter().find(|entry| entry.id == cluster))
        .map(|cluster| cluster.base.as_str());
    let scoped = match target_base {
        Some(base) => perms_scope_covers(&authd.policy.server_enroll_scopes, base),
        // On an idempotent repeat the removed entry no longer tells us its
        // cluster. A scoped admin may safely reconcile topology only inside its
        // own enrollment scopes.
        None => !authd.policy.server_enroll_scopes.is_empty(),
    };
    if !broad && !scoped {
        return Err(err(format!(
            "admin {} is not authorized to remove server {} at {}",
            authd.admin,
            req.server,
            target_base.unwrap_or("<unknown>")
        )));
    }
    // Keep the removed cluster and each directly connected cluster in the
    // reconciliation set. If the last member disappears, `netmap::remove`
    // deletes that cluster and detaches its children; the surviving parent and
    // children still need fresh topology.
    let mut affected_ids = BTreeSet::new();
    if let Some(cluster_id) = map
        .servers
        .iter()
        .find(|server| server.id == req.server)
        .and_then(|server| server.cluster)
    {
        affected_ids.insert(cluster_id);
        if let Some(cluster) = map.clusters.iter().find(|entry| entry.id == cluster_id) {
            affected_ids.extend(cluster.parent);
            affected_ids.extend(cluster.children.iter().copied());
        }
    }
    let mut affected_clusters: Vec<_> = map
        .clusters
        .iter()
        .filter(|cluster| affected_ids.contains(&cluster.id))
        .map(|cluster| cluster.base.clone())
        .collect();
    affected_clusters.sort();
    affected_clusters.dedup();
    let mut next = map.clone();
    let removed = match netmap::remove(&mut next, req.server) {
        Ok(removed) => removed,
        Err(e) => return Err(err(format!("{e:#}"))),
    };
    // A repeat after partial fanout cannot recover the removed identity's
    // cluster from the current map (there is deliberately no durable job
    // record). Broad administrators therefore reconcile every surviving
    // cluster on an idempotent repeat; scoped administrators reconcile only
    // clusters their enrollment policy covers.
    if !removed {
        affected_ids.extend(
            map.clusters
                .iter()
                .filter(|cluster| {
                    broad
                        || perms_scope_covers(
                            &authd.policy.server_enroll_scopes,
                            &cluster.base,
                        )
                })
                .map(|cluster| cluster.id),
        );
        affected_clusters = map
            .clusters
            .iter()
            .filter(|cluster| affected_ids.contains(&cluster.id))
            .map(|cluster| cluster.base.clone())
            .collect();
        affected_clusters.sort();
        affected_clusters.dedup();
    }
    let mut revoked = 0;
    if removed {
        let ca = state.ca.as_ref().expect("CA role held");
        revoked =
            revoke_server_certificates(ca, req.server, &authd.admin).map_err(|e| {
                err(format!("revoking the server's serving certificates: {e:#}"))
            })?;
        if let Err(e) = netmap::save(&ca_dir, &next) {
            return Err(err(format!("persisting the network map: {e:#}")));
        }
        *map = next;
        audit(
            &ca_dir,
            &authd.admin,
            "remove-server",
            &format!("operation {operation_id}: {}", req.server),
            Duration::ZERO,
        );
    } else {
        audit(
            &ca_dir,
            &authd.admin,
            "reconcile-server-removal",
            &format!("operation {operation_id}: {}", req.server),
            Duration::ZERO,
        );
    }
    // Always carry the current CRL on an idempotent retry as well: a previous
    // removal may have committed the revocation but only partially delivered
    // it. Repeating force-remove is the manual reconciliation path for both
    // topology and revocation state.
    let crl_pem = {
        let path = state.ca.as_ref().expect("CA role held").store.lock().crl_path();
        match std::fs::read_to_string(&path) {
            Ok(pem) => Some(pem),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(err(format!(
                    "reading the current CRL for immediate distribution: {e:#}"
                )));
            }
        }
    };
    let mut targets = Vec::new();
    for cluster in
        map.clusters.iter().filter(|cluster| affected_ids.contains(&cluster.id))
    {
        for server in map.servers.iter().filter(|server| {
            server.cluster == Some(cluster.id)
                && server.state == admin_proto::ServerState::Registered
        }) {
            let Some(local_member) = server.resolver.clone() else {
                continue;
            };
            targets.push((
                server.id,
                server.addr,
                topology_edit(&map, cluster, local_member),
            ));
        }
    }
    Ok(RemoveServerPrepare {
        version: map.version,
        revoked: revoked as u64,
        removed,
        affected_clusters,
        fanout: TopologyFanout { targets },
        crl_pem,
    })
}

/// Permanently remove a dead identity, then reconcile only the surviving
/// clusters whose referral topology changed. This never restarts a resolver;
/// administrators retain control of the rolling restart sequence.
async fn handle_remove_server(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    req: RemoveServerRequest,
) -> RemoveServerResponse {
    let operation_id = admin_proto::OperationId::new();
    let prepared = {
        let state = state.clone();
        run_signing(signs, move || remove_server_prepare(&state, &req, operation_id))
            .await
    };
    let prepared = match prepared {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(response)) => return response,
        Err(e) => {
            return RemoveServerResponse::Err {
                reason: format!("remove server task panicked: {e}"),
            };
        }
    };
    let crl_peers = match prepared.crl_pem {
        Some(crl_pem) => push_crl_to_peers(&state, &crl_pem, operation_id).await,
        None => Vec::new(),
    };
    let peers = push_topology(&state, prepared.fanout, operation_id).await;
    RemoveServerResponse::Ok {
        version: prepared.version,
        operation_id: Some(operation_id),
        revoked: prepared.revoked,
        removed: prepared.removed,
        affected_clusters: prepared.affected_clusters,
        peers,
        crl_peers,
    }
}

/// Revoke every still-live serving certificate carrying `server_id`, including
/// overlapping certificates left by key-rotating renewal, then publish one CRL
/// containing the complete result. The identity URI, not the shared DNS SAN or
/// mutable address, selects the records.
fn revoke_server_certificates(
    ca: &ca_store::CaDir,
    server_id: admin_proto::AdminServerId,
    admin: &str,
) -> Result<usize> {
    let signing = server_unlock(ca).map_err(|reason| anyhow!(reason))?;
    let now = ca_store::now_unix();
    let mut store = ca.store.lock();
    let serials: Vec<_> = store
        .list_signed()
        .context("reading the issuance index")?
        .into_iter()
        .filter(|record| {
            record.live(now)
                && record.name.eq_ignore_ascii_case(SERVING_SAN)
                && crate::tls::admin_cert_identity_from_pem(record.cert_pem.as_bytes())
                    .is_ok_and(|identity| identity.server_id == server_id)
        })
        .map(|record| record.serial)
        .collect();
    let mut revoked = 0;
    for serial in serials {
        let revocation = ca_store::Revocation {
            serial,
            revoked_unix: now,
            reason: format!("admin server {server_id} removed by {admin}"),
        };
        if store.revoke(serial, revocation)? {
            revoked += 1;
        }
    }
    if revoked > 0 {
        store.write_crl(&signing.ca_key_pem).context("publishing the updated CRL")?;
    }
    Ok(revoked)
}

/// The local resolver's permissions file, resolved against its config dir.
fn local_perms_path(state: &Server) -> Result<PathBuf> {
    let rconfig = state
        .cfg
        .lock()
        .roles
        .resolver
        .as_ref()
        .map(|r| r.config.clone())
        .context("this host has no resolver role — no perms to read or edit")?;
    let rc = crate::resolver::ResolverConfig::load(&rconfig)?;
    let inc = rc.as_file().include_permissions.first().cloned().context(
        "this resolver has no permissions file (include_permissions is empty — an \
         anonymous network has no perms)",
    )?;
    let base = rconfig.parent().unwrap_or_else(|| Path::new("."));
    Ok(base.join(inc.as_str()))
}

/// Read the local resolver's perms file, serialized for the wire.
fn handle_get_perms(state: &Server) -> GetPermsResponse {
    let read = || -> Result<String> {
        let path = local_perms_path(state)?;
        let pmap = crate::perms::load_perms(&path)?;
        serde_json::to_string(&pmap).context("serializing perms")
    };
    match read() {
        Ok(perms_json) => GetPermsResponse::Ok { perms_json },
        Err(e) => GetPermsResponse::Err { reason: format!("{e:#}") },
    }
}

/// Authenticate a permissions operation. Remote callers use the CA vault or a
/// live session; the protected local socket is the on-box signing superuser.
/// Shared by reads and edits so their scope semantics cannot drift.
async fn authenticate_perms_caller(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    credential: &admin_proto::AdminCredential,
    local: bool,
) -> std::result::Result<ca_vault::Authenticated, String> {
    if local {
        return Ok(local_superuser());
    }
    let state = state.clone();
    let auth_credential = credential.clone();
    let failure_credential = credential.clone();
    match run_signing(signs, move || {
        authenticate(state.ca.as_ref().expect("CA role held"), &auth_credential)
    })
    .await
    {
        Ok(Ok(authd)) => Ok(authd),
        Ok(Err(reason)) => Err(safe_auth_failure(&failure_credential, reason)),
        Err(e) => Err(format!("auth task panicked: {e}")),
    }
}

fn authorize_perms_scope(
    authd: &ca_vault::Authenticated,
    target_path: &str,
    operation: &str,
) -> std::result::Result<(), String> {
    if authd.kind == ca_vault::SlotKind::Signing
        || perms_scope_covers(&authd.policy.perms_edit_scopes, target_path)
    {
        Ok(())
    } else {
        Err(format!(
            "admin {:?} ({}) is not authorized to {operation} perms at {:?}",
            authd.admin,
            match authd.kind {
                ca_vault::SlotKind::Signing => "signing",
                ca_vault::SlotKind::Role => "role",
            },
            target_path
        ))
    }
}

fn confine_local_perms(state: &Server, target_path: &str, operation: &str) -> Result<()> {
    let base = own_base(state);
    if base.as_deref() == Some(target_path) {
        Ok(())
    } else {
        bail!(
            "local perms {operation}s are confined to this host's own level ({}); \
             refusing to {operation} {:?}",
            base.as_deref().unwrap_or("<none>"),
            target_path,
        )
    }
}

/// Admin → controller permissions read. Authentication and policy are checked
/// once at the controller, then registered cluster members are tried in stable
/// server-ID order using the controller certificate and exact target pinning.
async fn handle_read_perms(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    req: &ReadPermsRequest,
    local: bool,
) -> ReadPermsResponse {
    let err = |reason: String| ReadPermsResponse::Err { reason };
    if !local && state.ca.is_none() {
        return err("a remote perms read must be sent to the CA controller".to_string());
    }
    let authd =
        match authenticate_perms_caller(state, signs, &req.credential, local).await {
            Ok(authd) => authd,
            Err(reason) => return err(reason),
        };
    if local && let Err(e) = confine_local_perms(state, &req.target_path, "read") {
        return err(format!("{e:#}"));
    }
    if let Err(reason) = authorize_perms_scope(&authd, &req.target_path, "read") {
        return err(reason);
    }
    // The protected local socket is deliberately useful on every resolver,
    // including satellites that do not have the CA role. It may read only the
    // host's own level and never consults or trusts remote map hints.
    if local {
        let (server, addr) = {
            let cfg = state.cfg.lock();
            (cfg.server_id, cfg.listen)
        };
        return match handle_get_perms(state) {
            GetPermsResponse::Ok { perms_json } => {
                ReadPermsResponse::Ok { server, addr, perms_json }
            }
            GetPermsResponse::Err { reason } => err(reason),
        };
    }
    let targets = {
        let map = state.map.lock();
        match cluster_members_for(&map, &req.target_path) {
            Some(targets) => targets,
            None => {
                return err(format!(
                    "no registered resolver cluster serving {:?} in the network map",
                    req.target_path
                ));
            }
        }
    };
    audit(
        state.ca.as_ref().expect("CA role held").dir(),
        &authd.admin,
        "read-perms",
        &req.target_path,
        Duration::ZERO,
    );
    let (cert, key) = state.outbound_identity();
    let roots = state.roots.clone();
    let controller = state.map.lock().controller;
    let home_ca = state.home_ca_der.clone();
    let mut failures = Vec::new();
    for (server, addr) in targets {
        let result = tokio::time::timeout(
            PUSH_TIMEOUT,
            admin_client::pull_perms(
                addr,
                server,
                server == controller,
                home_ca.clone(),
                &cert,
                &key,
                roots.clone(),
            ),
        )
        .await;
        match result {
            Ok(Ok(perms_json)) => {
                return ReadPermsResponse::Ok { server, addr, perms_json };
            }
            Ok(Err(e)) => failures.push(format!("{server} at {addr}: {e:#}")),
            Err(_) => failures.push(format!(
                "{server} at {addr}: timed out after {}s",
                PUSH_TIMEOUT.as_secs()
            )),
        }
    }
    err(format!(
        "no registered member of cluster {:?} could provide permissions: {}",
        req.target_path,
        failures.join("; ")
    ))
}

/// Write `perms_json` to the local perms file under the resolver-edit lock,
/// validating the resolver config and reverting the file if it goes invalid.
fn apply_perms_local(state: &Server, perms_json: &str) -> Result<()> {
    let path = local_perms_path(state)?;
    let pmap: crate::perms::PMap =
        serde_json::from_str(perms_json).context("parsing the new perms")?;
    // Validate the permission bits before touching the file — `load_perms`
    // and `validate_for_path` both keep bits as opaque strings, so without
    // this an edit with unparseable bits (only `!swlpd` are valid) would be
    // written and only blow up when the resolver next loads it.
    for (p, e, bits) in crate::perms::iter(&pmap) {
        netidx::resolver_server::auth::Permissions::try_from(bits.as_str())
            .with_context(|| {
                format!("invalid permission bits {bits:?} for {e:?} at {p:?}")
            })?;
    }
    let rconfig = state
        .cfg
        .lock()
        .roles
        .resolver
        .as_ref()
        .map(|r| r.config.clone())
        .context("no resolver role")?;
    let _guard = state.resolver_edit_lock.lock();
    let backup = std::fs::read(&path).ok();
    crate::perms::save_perms(&path, &pmap)?;
    if let Err(e) = crate::resolver::ResolverConfig::load(&rconfig)
        .and_then(|rc| rc.validate_for_path(&rconfig))
    {
        // Roll back to *exactly* the prior state — restore the old bytes
        // (atomically), or remove the file we just created if there was
        // none before. A failed rollback is itself reported: never claim
        // "reverted" while leaving an invalid perms file in place.
        let rolled_back = match &backup {
            Some(old) => crate::atomic::write_atomic(&path, old, 0o644)
                .context("restoring the previous perms file"),
            None => {
                std::fs::remove_file(&path).context("removing the rejected perms file")
            }
        };
        return match rolled_back {
            Ok(()) => Err(e)
                .context("the edited perms made the resolver config invalid; reverted"),
            Err(re) => Err(e).context(format!(
                "the edited perms made the resolver config invalid AND the revert \
                 failed ({re:#}); the perms file at {} may be left invalid",
                path.display()
            )),
        };
    }
    Ok(())
}

/// Server-to-server receive side of a perms edit (peer-cert-gated).
fn handle_apply_perms_edit(
    state: &Server,
    req: &ApplyPermsEditRequest,
) -> ApplyPermsEditResponse {
    info!("admin-server: applying permissions operation {}", req.operation_id);
    match apply_perms_local(state, &req.perms_json) {
        Ok(()) => ApplyPermsEditResponse::Ok,
        Err(e) => ApplyPermsEditResponse::Err { reason: format!("{e:#}") },
    }
}

/// Whether any granted scope covers `target` — target equals or descends
/// from a scope (`/` covers the whole tree). The path-aware prefix test
/// (`Path::is_parent`) won't let `/eu` match `/europe`.
fn perms_scope_covers(scopes: &[String], target: &str) -> bool {
    scopes.iter().any(|s| netidx::path::Path::is_parent(s, target))
}

fn authorize_enrollment(
    authd: &ca_vault::Authenticated,
    enrollment: &admin_proto::EnrollmentRequest,
    map: Option<&NetworkMap>,
) -> std::result::Result<(), String> {
    if enrollment.roles.contains(&Role::Ca) {
        return Err("an enrollee may never request the Ca role".to_string());
    }
    if !enrollment.roles.contains(&Role::Resolver) {
        return Err("every non-controller enrollment must include Resolver".to_string());
    }
    if matches!(authd.kind, ca_vault::SlotKind::Signing) {
        return Ok(());
    }
    if enrollment
        .roles
        .iter()
        .any(|role| !authd.policy.server_enroll_roles.contains(role))
    {
        return Err(format!(
            "requested roles {:?} exceed admin {}'s allowed enrollment roles {:?}",
            enrollment.roles, authd.admin, authd.policy.server_enroll_roles
        ));
    }
    let base = match enrollment.cluster {
        admin_proto::ClusterPlacement::Create { ref base } => base.as_str(),
        admin_proto::ClusterPlacement::Join { cluster } => map
            .and_then(|m| m.clusters.iter().find(|c| c.id == cluster))
            .map(|c| c.base.as_str())
            .ok_or_else(|| {
                "the requested cluster is not in the authoritative map".to_string()
            })?,
    };
    if !perms_scope_covers(&authd.policy.server_enroll_scopes, base) {
        return Err(format!(
            "admin {} is not authorized to enroll servers at {base:?}",
            authd.admin
        ));
    }
    Ok(())
}

/// The resolver member addresses of the cluster whose base path is
/// `target_path`, from the map. `None` if no such cluster. Used by the
/// (correctly) path-scoped perms-edit fanout.
fn cluster_members_for(
    map: &NetworkMap,
    target_path: &str,
) -> Option<Vec<(admin_proto::AdminServerId, SocketAddr)>> {
    let cluster = map.clusters.iter().find(|c| {
        c.base == target_path && c.state == admin_proto::ClusterState::Active
    })?;
    let mut members: Vec<_> = map
        .servers
        .iter()
        .filter(|s| {
            s.cluster == Some(cluster.id)
                && s.state == admin_proto::ServerState::Registered
        })
        .map(|s| (s.id, s.addr))
        .collect();
    members.sort_by_key(|(id, _)| *id);
    (!members.is_empty()).then_some(members)
}

/// Push a perms edit to every registered server in the target cluster, using
/// each CA-owned routing address. Every unreachable/erroring target is returned
/// as a `PeerResult` carrying its immutable identity and current address.
async fn push_perms_edit_to_peers(
    state: &Arc<Server>,
    perms_json: &str,
    targets: &[(admin_proto::AdminServerId, SocketAddr)],
    operation_id: admin_proto::OperationId,
) -> Vec<PeerResult> {
    let (cert, key) = state.outbound_identity();
    let roots = state.roots.clone();
    let controller = state.map.lock().controller;
    let home_ca = state.home_ca_der.clone();
    let mut results: Vec<_> =
        stream::iter(targets.iter().copied().map(|(server, addr)| {
            let cert = cert.clone();
            let key = key.clone();
            let roots = roots.clone();
            let home_ca = home_ca.clone();
            async move {
                let res = tokio::time::timeout(
                    PUSH_TIMEOUT,
                    admin_client::push_perms_edit(
                        addr,
                        server,
                        server == controller,
                        home_ca,
                        &cert,
                        &key,
                        roots,
                        operation_id,
                        perms_json,
                    ),
                )
                .await
                .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
                .and_then(|r| r);
                PeerResult { server, addr, error: res.err().map(|e| format!("{e:#}")) }
            }
        }))
        .buffer_unordered(32)
        .collect()
        .await;
    results.sort_by_key(|result| result.server);
    results
}

/// CA-side: authenticate the admin, find the target cluster in the map, and
/// propagate the perms edit to its admin servers (peer-cert-gated). The CA
/// never edits a foreign cluster's files directly — it pushes.
async fn handle_edit_perms(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    req: &EditPermsRequest,
    local: bool,
) -> EditPermsResponse {
    let err = |reason: String| EditPermsResponse::Err { reason };
    if state.ca.is_none() {
        return err("a perms edit must be sent to the CA host".to_string());
    }
    let authd =
        match authenticate_perms_caller(state, signs, &req.credential, local).await {
            Ok(authd) => authd,
            Err(reason) => return err(reason),
        };
    if local && let Err(e) = confine_local_perms(state, &req.target_path, "edit") {
        return err(format!("{e:#}"));
    }
    if let Err(reason) = authorize_perms_scope(&authd, &req.target_path, "edit") {
        return err(reason);
    }
    let members = {
        let map = state.map.lock();
        match cluster_members_for(&map, &req.target_path) {
            Some(m) => m,
            None => {
                return err(format!(
                    "no resolver cluster serving {:?} in the network map",
                    req.target_path
                ));
            }
        }
    };
    let operation_id = admin_proto::OperationId::new();
    audit(
        state.ca.as_ref().expect("CA role held").dir(),
        &authd.admin,
        "edit-perms",
        &format!("operation {operation_id} at {}", req.target_path),
        Duration::ZERO,
    );
    let peers =
        push_perms_edit_to_peers(state, &req.perms_json, &members, operation_id).await;
    EditPermsResponse::Ok { operation_id, peers }
}

// -- remote service control ---------------------------------------------------

/// Whether `authd` may control services on an admin server whose cluster base
/// is `base`: a signing slot (founding authority) or a role admin whose
/// `service_control_scopes` cover `base`. (Distinct from perms-edit and
/// admin-management authority — restarting services is its own grant.)
///
/// Authorization is still path-scoped (a `/eu` service-control admin may
/// control any server in a cluster based under `/eu`), but the *action* it
/// authorizes is per-server: one `ControlService` call touches exactly one
/// admin server, so it can never take a whole level down at once.
fn service_control_authority(authd: &ca_vault::Authenticated, base: &str) -> bool {
    matches!(authd.kind, ca_vault::SlotKind::Signing)
        || perms_scope_covers(&authd.policy.service_control_scopes, base)
}

/// The cluster base of the admin server whose listen address is `addr`, from
/// the map — the authorization scope for controlling that server's services.
/// `None` if the server isn't in the map or runs no resolver cluster.
fn base_for_server(map: &NetworkMap, id: admin_proto::AdminServerId) -> Option<String> {
    let cluster = map
        .servers
        .iter()
        .find(|s| s.id == id && s.state == admin_proto::ServerState::Registered)?
        .cluster?;
    map.clusters.iter().find(|c| c.id == cluster).map(|c| c.base.clone())
}

/// Resolve only an immutable registered identity to its current routing
/// address. Addresses are deliberately never accepted as lookup keys here.
fn registered_server_addr(
    map: &NetworkMap,
    id: admin_proto::AdminServerId,
) -> Option<SocketAddr> {
    map.servers
        .iter()
        .find(|server| {
            server.id == id && server.state == admin_proto::ServerState::Registered
        })
        .map(|server| server.addr)
}

/// CA-side: authenticate the admin, authorize by service-control scope, and
/// apply the op to **one** admin server (`req.target_server`) — forwarding a
/// single peer-cert-gated [`Request::ApplyServiceControl`], or applying locally
/// when the target is the CA itself. Per-server by design: restart is never
/// cluster-wide, so a careful operator restarts one resolver at a time.
async fn handle_control_service(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    req: &ControlServiceRequest,
) -> ControlServiceResponse {
    let err = |reason: String| ControlServiceResponse::Err { reason };
    if state.ca.is_none() {
        return err("a service-control request must be sent to the CA host".to_string());
    }
    let auth = {
        let state = state.clone();
        let credential = req.credential.clone();
        run_signing(signs, move || {
            authenticate(state.ca.as_ref().expect("CA role held"), &credential)
        })
        .await
    };
    let authd = match auth {
        Ok(Ok(a)) => a,
        Ok(Err(reason)) => return err(safe_auth_failure(&req.credential, reason)),
        Err(e) => return err(format!("auth task panicked: {e}")),
    };
    // The target server's cluster base is its authorization scope. A server not
    // in the map (or running no resolver) has no base — only a signing slot may
    // control it, so an unknown target can't be reached by a scoped role admin.
    let base = base_for_server(&state.map.lock(), req.target_server);
    let authorized = match &base {
        Some(base) => service_control_authority(&authd, base),
        None => matches!(authd.kind, ca_vault::SlotKind::Signing),
    };
    if !authorized {
        return err(format!(
            "admin {:?} is not authorized to control services on {}",
            authd.admin, req.target_server
        ));
    }
    // start/stop/restart must name explicit units, so a fat-finger can't take a
    // server down blind; read-only `status` may omit them (the supervisor
    // expands an empty unit list to all its units).
    if req.units.is_empty()
        && !matches!(req.op, netidx_activation::control::ControlOp::Status)
    {
        return err("no units specified — start/stop/restart require explicit units \
             (status with no units reports them all)"
            .to_string());
    }
    let operation_id = admin_proto::OperationId::new();
    // Audit the *intent* before acting — a slow op can outlive the connection.
    let ca_dir = state.ca.as_ref().expect("CA role held").dir().to_path_buf();
    audit(
        &ca_dir,
        &authd.admin,
        "control-service",
        &format!(
            "operation {operation_id}: {:?} {} on {}",
            req.op,
            req.units.join(","),
            req.target_server
        ),
        Duration::ZERO,
    );
    let (my_id, target_addr) = {
        let map = state.map.lock();
        let addr = registered_server_addr(&map, req.target_server);
        (state.cfg.lock().server_id, addr)
    };
    let Some(target_addr) = target_addr else {
        return err(
            "target server is not registered in the authoritative map".to_string()
        );
    };
    // Apply to the one target: locally when it's this CA host, else one
    // peer-cert-gated hop to that admin server (its real map listen address).
    let applied = if req.target_server == my_id {
        let apply = ApplyServiceControlRequest {
            operation_id,
            units: req.units.clone(),
            op: req.op,
        };
        match handle_apply_service_control(state, &apply).await {
            ApplyServiceControlResponse::Ok { units } => Ok(units),
            ApplyServiceControlResponse::Err { reason } => Err(reason),
        }
    } else {
        let (cert, key) = state.outbound_identity();
        let roots = state.roots.clone();
        let target_is_controller = req.target_server == state.map.lock().controller;
        tokio::time::timeout(
            PUSH_TIMEOUT,
            admin_client::push_service_control(
                target_addr,
                req.target_server,
                target_is_controller,
                state.home_ca_der.clone(),
                &cert,
                &key,
                roots,
                operation_id,
                req.units.clone(),
                req.op,
            ),
        )
        .await
        .map_err(|_| format!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
        .and_then(|result| result.map_err(|e| format!("{e:#}")))
    };
    match applied {
        Ok(units) => ControlServiceResponse::Ok { operation_id, units },
        Err(reason) => err(format!(
            "operation {operation_id} on server {} at {target_addr}: {reason}",
            req.target_server
        )),
    }
}

/// Server-to-server (peer-cert-gated): apply a service-control op to THIS
/// host's local activation supervisor, via its control socket.
async fn handle_apply_service_control(
    state: &Server,
    req: &ApplyServiceControlRequest,
) -> ApplyServiceControlResponse {
    info!(
        "admin-server: applying service-control operation {}: {:?} {:?}",
        req.operation_id, req.op, req.units
    );
    use netidx_activation::control;
    let err = |reason: String| ApplyServiceControlResponse::Err { reason };
    // Use the configured activation unit directory if set (the supervisor may
    // run with a custom `--units` dir), else the default search location —
    // the same resolution the supervisor itself uses to place the socket.
    let units_dir = state
        .cfg
        .lock()
        .activation_units_dir
        .clone()
        .or_else(netidx_activation::runtime::default_units_dir);
    let dir = match units_dir {
        Some(dir) => dir,
        None => {
            return err(
                "no activation supervisor on this host (no unit directory found)"
                    .to_string(),
            );
        }
    };
    let creq = control::ControlRequest { op: req.op, units: req.units.clone() };
    let statuses = match control::control(&dir, &creq).await {
        Ok(control::ControlResponse::Ok { units }) => units,
        Ok(control::ControlResponse::Err { reason }) => return err(reason),
        Err(e) => {
            return err(format!("contacting the local activation supervisor: {e:#}"));
        }
    };
    // Merge each reported unit's on-disk definition (this member holds the unit
    // files) so the operator's panel shows the same status + definition the
    // local Services surface does. A read failure just omits definitions.
    let defs = crate::activation::ActivationDir::open(Some(&dir))
        .and_then(|ad| ad.list())
        .unwrap_or_default();
    let units = statuses
        .into_iter()
        .map(|u| {
            let definition = defs.get(&u.unit).map(|unit| ServiceUnitDef {
                exe: unit.process.exe.clone(),
                args: unit.process.args.clone(),
                trigger: unit.trigger.to_string(),
                restart: unit.process.restart.to_string(),
            });
            ServiceUnit { unit: u.unit, state: u.state, definition }
        })
        .collect();
    ApplyServiceControlResponse::Ok { units }
}

// -- remote admin management --------------------------------------------------

/// Authenticate `admin`/`password` and require the authority to manage
/// admins: a signing slot (the founding recovery / autorenew credentials,
/// which hold the master key) or a role admin whose policy carries
/// `may_manage_admins`. The returned identity's policy bounds what it may
/// grant (the no-escalation rule); `Err` is a safe wire reason.
fn authorize_admin_mgmt(
    ca: &ca_store::CaDir,
    credential: &admin_proto::AdminCredential,
    local: bool,
) -> std::result::Result<ca_vault::Authenticated, String> {
    if local {
        return Ok(local_superuser());
    }
    let authd = authenticate(ca, credential)?;
    if matches!(authd.kind, ca_vault::SlotKind::Signing) || authd.policy.may_manage_admins
    {
        Ok(authd)
    } else {
        Err(format!(
            "admin {:?} is not authorized to manage admins (needs may_manage_admins)",
            authd.admin
        ))
    }
}

/// The synthetic identity for a request over the local control socket: a
/// signing-tier superuser. The signing tier is what every admin-management
/// gate ([`authorize_admin_mgmt`], the `kind == Signing` no-escalation
/// bypass) checks, so this authorizes exactly the way a real recovery /
/// autorenew signing slot does — without a password. Reaching the socket is
/// the authorization (`0600` + `SO_PEERCRED`, root / the daemon's own uid).
fn local_superuser() -> ca_vault::Authenticated {
    ca_vault::Authenticated {
        slot_id: uuid::Uuid::nil(),
        credential_revision: 0,
        admin: "local".to_string(),
        policy: crate::ca_policy::superuser_policy(),
        kind: ca_vault::SlotKind::Signing,
    }
}

/// Whether the caller's issuance glob `caller` covers the granted glob
/// `granted` — i.e. every name `granted` could match is also matched by
/// `caller`. Decidable and **sound** for the realistic DNS patterns (`*`,
/// `*.<suffix>`, and literal names): it never reports coverage that does not
/// hold, so it can't permit an escalation. Patterns it can't prove
/// containment for (a mid-string `*`, a `?`) are conservatively *not* covered
/// — grant those on-box with `ca admin add-role`, which carries no subset
/// check.
fn glob_covers(caller: &str, granted: &str) -> bool {
    if caller == granted {
        return true; // identical pattern
    }
    if caller == "*" {
        return true; // matches every (slash-free) name
    }
    if let Some(suffix) = caller.strip_prefix("*.") {
        // `*.<suffix>` matches exactly the names ending in `.<suffix>`. The
        // granted pattern is covered iff its tail is the literal `.<suffix>`:
        // then every name it matches ends in `.<suffix>`, whatever globbing
        // precedes that tail.
        return granted.ends_with(&format!(".{suffix}"));
    }
    false // a literal (or a pattern we don't reason about) only covers itself
}

/// The no-escalation rule: a managing role admin may grant a target only
/// capabilities that are a subset of its own. Returns the first violated
/// field as a safe reason, or `Ok(())`. The founding signing slots bypass
/// this entirely (they hold the key — the caller checks `kind` first).
fn policy_within(
    caller: &ca_vault::Policy,
    granted: &ca_vault::Policy,
) -> std::result::Result<(), String> {
    for g in &granted.allowed_san {
        if !caller.allowed_san.iter().any(|c| glob_covers(c, g)) {
            return Err(format!(
                "cannot grant issuance scope {g:?}: it is not within your own scope {:?}",
                caller.allowed_san
            ));
        }
    }
    if granted.max_validity > caller.max_validity {
        return Err(format!(
            "cannot grant max_validity {} — yours is {}",
            humantime::format_duration(granted.max_validity),
            humantime::format_duration(caller.max_validity)
        ));
    }
    for g in &granted.id_map_groups {
        if !caller.id_map_groups.contains(g) {
            return Err(format!(
                "cannot grant id-map group {g:?}: it is not in your own set {:?}",
                caller.id_map_groups
            ));
        }
    }
    for scope in &granted.server_enroll_scopes {
        if !perms_scope_covers(&caller.server_enroll_scopes, scope) {
            return Err(format!(
                "cannot grant server enrollment scope {scope:?} — it is outside your scopes"
            ));
        }
    }
    for role in &granted.server_enroll_roles {
        if !caller.server_enroll_roles.contains(role) {
            return Err(format!(
                "cannot grant server enrollment role {role:?} — you do not have it"
            ));
        }
    }
    if granted.may_manage_admins && !caller.may_manage_admins {
        return Err("cannot grant may_manage_admins — you do not have it".to_string());
    }
    for s in &granted.perms_edit_scopes {
        if !perms_scope_covers(&caller.perms_edit_scopes, s) {
            return Err(format!(
                "cannot grant perms scope {s:?}: it is not within your own scopes {:?}",
                caller.perms_edit_scopes
            ));
        }
    }
    for s in &granted.service_control_scopes {
        if !perms_scope_covers(&caller.service_control_scopes, s) {
            return Err(format!(
                "cannot grant service-control scope {s:?}: it is not within your own \
                 scopes {:?}",
                caller.service_control_scopes
            ));
        }
    }
    Ok(())
}

/// Whether `target` is the only ROLE admin carrying `may_manage_admins`.
/// Removing or demoting it would strand remote admin management — only the
/// off-box recovery password (a signing slot) could restore it. We refuse
/// that footgun by default; the recovery credential remains the backstop.
fn last_role_manager(vault: &ca_vault::CAVault, target: &str) -> Result<bool> {
    let admins = vault.list_admins()?;
    let is_role_manager = |a: &ca_vault::AdminInfo| {
        a.kind == ca_vault::SlotKind::Role && a.policy.may_manage_admins
    };
    let managers = admins.iter().filter(|a| is_role_manager(a)).count();
    let target_is_manager =
        admins.iter().any(|a| a.admin == target && is_role_manager(a));
    Ok(target_is_manager && managers <= 1)
}

/// `AddRoleAdmin`: mint a new role admin (CA-only, admin-authenticated).
fn handle_add_role_admin(
    state: &Server,
    req: &AddRoleAdminRequest,
    local: bool,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_ref() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    let authd = match authorize_admin_mgmt(ca, &req.credential, local) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    // The reserved signing-slot names are off-limits (add_role_slot also
    // refuses them, but a clear message beats a generic one).
    if ca_vault::is_reserved_admin(&req.name) {
        return err(format!(
            "{:?} is a reserved signing-slot name and cannot be a role admin",
            req.name
        ));
    }
    // No escalation — the founding signing credentials are exempt.
    if !matches!(authd.kind, ca_vault::SlotKind::Signing)
        && let Err(reason) = policy_within(&authd.policy, &req.policy)
    {
        return err(reason);
    }
    // Serialize the vault read-modify-write: admin-mgmt ops run concurrently
    // (one per connection on the blocking pool), and the vault file is a
    // read-modify-write. Hold the same lock the issuance path uses so two
    // concurrent writes can't clobber each other (or both pass a guard that
    // a single op would have failed). The slow Argon2 auth already ran above,
    // outside the lock.
    let mut vault = ca.vault.write();
    match vault.add_role_slot(&req.name, &req.new_password.0, req.policy.clone()) {
        Ok(()) => {
            audit(ca.dir(), &authd.admin, "add-role-admin", &req.name, Duration::ZERO);
            AdminMgmtResponse::Ok
        }
        Err(e) => err(format!("{e:#}")),
    }
}

/// `SetAdminPolicy`: rescope an existing role admin (CA-only).
fn handle_set_admin_policy(
    state: &Server,
    req: &SetAdminPolicyRequest,
    local: bool,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_ref() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    let authd = match authorize_admin_mgmt(ca, &req.credential, local) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    if ca_vault::is_reserved_admin(&req.target) {
        return err(format!("{:?} is a system-managed signing slot", req.target));
    }
    if !matches!(authd.kind, ca_vault::SlotKind::Signing)
        && let Err(reason) = policy_within(&authd.policy, &req.policy)
    {
        return err(reason);
    }
    // Hold the vault lock across the look-up, the last-manager guard, AND the
    // write, so a concurrent op can't change the slot's tier or the manager
    // count between the checks and the mutation (close the TOCTOU).
    let mut vault = ca.vault.write();
    // The target must exist and be a role slot — remote ops never touch the
    // master-key-holding signing slots.
    let (kind, current) = match vault.slot_policy(&req.target) {
        Ok(kp) => kp,
        Err(_) => return err(format!("no admin named {:?}", req.target)),
    };
    if kind != ca_vault::SlotKind::Role {
        return err("remote admin management operates on role admins only".to_string());
    }
    // Don't let a rescope strand admin management by demoting the last role
    // manager.
    if current.may_manage_admins && !req.policy.may_manage_admins {
        match last_role_manager(&vault, &req.target) {
            Ok(true) => {
                return err(format!(
                    "refusing to drop may_manage_admins from {:?}: it is the last role \
                     admin that can manage admins (restoring it would need the recovery \
                     password)",
                    req.target
                ));
            }
            Ok(false) => (),
            Err(e) => return err(format!("checking admin roster: {e:#}")),
        }
    }
    match vault.set_policy(&req.target, req.policy.clone()) {
        Ok(()) => {
            audit(
                ca.dir(),
                &authd.admin,
                "set-admin-policy",
                &req.target,
                Duration::ZERO,
            );
            AdminMgmtResponse::Ok
        }
        Err(e) => err(format!("{e:#}")),
    }
}

/// `RemoveAdmin`: remove a role admin (CA-only).
fn handle_remove_admin(
    state: &Server,
    req: &RemoveAdminRequest,
    local: bool,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_ref() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    let authd = match authorize_admin_mgmt(ca, &req.credential, local) {
        Ok(a) => a,
        Err(reason) => return err(reason),
    };
    if ca_vault::is_reserved_admin(&req.target) {
        return err(format!(
            "{:?} is a system-managed signing slot — rotate it with `recovery rotate` \
             / `auto-approve --rotate`, it cannot be removed",
            req.target
        ));
    }
    // Hold the vault lock across the look-up, the last-manager guard, and the
    // removal so two concurrent removes can't both pass the guard and strand
    // management (close the TOCTOU).
    let mut vault = ca.vault.write();
    let (kind, current) = match vault.slot_policy(&req.target) {
        Ok(kp) => kp,
        Err(_) => return err(format!("no admin named {:?}", req.target)),
    };
    if kind != ca_vault::SlotKind::Role {
        return err("remote admin management operates on role admins only".to_string());
    }
    if current.may_manage_admins {
        match last_role_manager(&vault, &req.target) {
            Ok(true) => {
                return err(format!(
                    "refusing to remove {:?}: it is the last role admin that can manage \
                     admins (restoring it would need the recovery password)",
                    req.target
                ));
            }
            Ok(false) => (),
            Err(e) => return err(format!("checking admin roster: {e:#}")),
        }
    }
    match vault.remove_slot(&req.target, false) {
        Ok(()) => {
            audit(ca.dir(), &authd.admin, "remove-admin", &req.target, Duration::ZERO);
            AdminMgmtResponse::Ok
        }
        Err(e) => err(format!("{e:#}")),
    }
}

/// `ListAdmins`: the admin roster (CA-only; gated on management authority so
/// a lower-tier role can't read everyone's capabilities).
fn handle_list_admins(
    state: &Server,
    req: &ListAdminsRequest,
    local: bool,
) -> AdminListResponse {
    let err = |reason: String| AdminListResponse::Err { reason };
    let Some(ca) = state.ca.as_ref() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    if let Err(reason) = authorize_admin_mgmt(ca, &req.credential, local) {
        return err(reason);
    }
    match ca.vault.read().list_admins() {
        Ok(admins) => AdminListResponse::Ok { admins },
        Err(e) => err(format!("listing admins: {e:#}")),
    }
}

/// `RotateRecovery` (local control socket ONLY): mint a fresh recovery
/// (off-box break-glass) password using the box's own autorenew credential
/// to unlock MK and re-wrap the recovery slot. Returns the new password in
/// grouped display form — shown to the operator once, never stored. Refused
/// over the network admin plane: reaching the local socket is itself the
/// authority, and a recovery password must never travel the network.
fn handle_rotate_recovery(state: &Server, local: bool) -> RotateRecoveryResponse {
    let err = |reason: String| RotateRecoveryResponse::Err { reason };
    if !local {
        return err(
            "rotating the recovery password is allowed only over the local control \
             socket on the CA box"
                .to_string(),
        );
    }
    let Some(ca) = state.ca.as_ref() else {
        return err("this host does not hold the CA".to_string());
    };
    let Some(autorenew_pw) = ca.autorenew_pw.read().clone() else {
        return err(
            "this CA holds no autorenew credential, so a fresh recovery slot cannot be \
             minted on-box; rotate offline with the daemon stopped (`ca recovery rotate`)"
                .to_string(),
        );
    };
    // Confirm the credential unlocks BEFORE touching the old slot, so a stale
    // credential can't strand the CA with no recovery slot. The recovered key
    // is dropped immediately. (The read guard drops at the `;`, before the
    // write below — no reentrant lock.)
    if let Err(e) = ca.vault.read().unlock(&autorenew_pw) {
        return err(format!("the autorenew credential did not unlock the CA: {e:#}"));
    }
    let new_pw = ca_vault::gen_recovery_password();
    // Remove-then-re-mint under one write guard; autorenew stays the signing
    // slot throughout, so MK is never orphaned and a failed re-mint can be
    // retried.
    let mut vault = ca.vault.write();
    let exists = match vault.list_admins() {
        Ok(a) => a.iter().any(|i| i.admin == ca_vault::RECOVERY_ADMIN),
        Err(e) => return err(format!("reading the admin roster: {e:#}")),
    };
    if exists {
        if let Err(e) = vault.remove_slot(ca_vault::RECOVERY_ADMIN, false) {
            return err(format!("removing the old recovery slot: {e:#}"));
        }
    }
    if let Err(e) = vault.add_signing_slot(
        &autorenew_pw,
        ca_vault::RECOVERY_ADMIN,
        &new_pw,
        crate::ca_policy::recovery_policy(),
    ) {
        return err(format!("minting the new recovery slot: {e:#}"));
    }
    drop(vault);
    audit(ca.dir(), "local", "rotate-recovery", ca_vault::RECOVERY_ADMIN, Duration::ZERO);
    // Return the canonical password; the CLI groups it for display the same
    // way the offline `recovery rotate` path does.
    RotateRecoveryResponse::Ok { recovery_password: Secret(new_pw.as_str().to_string()) }
}

/// `RotateAutorenew` (local control socket ONLY): rotate the box's OWN
/// autorenew signing credential and reseal its keytab, then hot-swap the
/// in-process credential — no downtime, and no second signing slot needed
/// (the daemon re-wraps the slot in place with the password it already
/// holds). Preserves the keytab's sealing posture: a sealed keytab is
/// resealed (a seal failure rolls the vault change back); a plaintext keytab
/// (an `--insecure-no-tpm` CA) is rewritten in plaintext, with a warning.
fn handle_rotate_autorenew(state: &Server, local: bool) -> RotateAutorenewResponse {
    let err = |reason: String| RotateAutorenewResponse::Err { reason };
    if !local {
        return err(
            "rotating the autorenew credential is allowed only over the local control \
             socket on the CA box"
                .to_string(),
        );
    }
    let Some(ca) = state.ca.as_ref() else {
        return err("this host does not hold the CA".to_string());
    };
    let Some(old_pw) = ca.autorenew_pw.read().clone() else {
        return err(
            "this CA holds no autorenew credential to rotate; set one up offline with \
             the daemon stopped (`ca auto-approve`)"
                .to_string(),
        );
    };
    let Some(keytab) =
        state.cfg.lock().roles.ca.as_ref().and_then(|c| c.autorenew.clone())
    else {
        return err(
            "this CA's config names no autorenew keytab, so there is nowhere to write \
             the rotated credential"
                .to_string(),
        );
    };
    // Preserve the existing keytab's posture (reseal a sealed keytab, keep a
    // plaintext one plaintext). Read it first so a missing/unreadable keytab
    // fails before we touch the vault.
    let current = match std::fs::read(&keytab) {
        Ok(b) => b,
        Err(e) => {
            return err(format!(
                "reading the autorenew keytab {}: {e:#}",
                keytab.display()
            ));
        }
    };
    let sealed = netidx_tpm::is_sealed(&current);
    let new_pw = ca_vault::random_signing_password();
    // Stage the NEW keytab content to a temp file FIRST — the slow,
    // failure-prone step (TPM seal + write) happens here, BEFORE any vault
    // change, so a failure leaves the vault, the live keytab, and the
    // in-process credential all untouched and needs no rollback. Posture is
    // preserved: reseal a sealed keytab, keep a plaintext one plaintext.
    let (payload, warning): (Zeroizing<Vec<u8>>, Option<String>) = if sealed {
        match netidx_tpm::seal(new_pw.as_bytes()) {
            Ok(blob) => (Zeroizing::new(blob), None),
            Err(e) => {
                return err(format!(
                    "resealing the autorenew keytab to this host's {} failed: {e:#}; the \
                     credential was left unchanged",
                    netidx_tpm::MECHANISM
                ));
            }
        }
    } else {
        (
            Zeroizing::new(new_pw.as_bytes().to_vec()),
            Some(format!(
                "the autorenew keytab {} is PLAINTEXT (this CA was set up \
                 --insecure-no-tpm); any backup of this host now contains a CA-key \
                 credential",
                keytab.display()
            )),
        )
    };
    let staged = keytab.with_extension("rotating");
    if let Err(e) = crate::atomic::write_atomic(&staged, &payload, 0o600) {
        return err(format!("staging the rotated keytab {}: {e:#}", staged.display()));
    }
    // Commit under ONE vault write guard so a concurrent `server_unlock`
    // (which reads the credential under the vault READ lock) can never observe
    // a half-rotated state: re-wrap the slot, swap the staged keytab into
    // place with an atomic same-dir rename, then hot-swap the in-process
    // credential. The only step after the vault is mutated is that rename —
    // atomic and all-but-infallible — so the vault and keytab can't diverge
    // except under a double fault, which startup degrades to a read-only CA
    // (with recovery instructions), never a lost one.
    let mut vault = ca.vault.write();
    if let Err(e) = vault.rekey_signing_slot(AUTORENEW_ADMIN, &old_pw, &new_pw) {
        let _ = std::fs::remove_file(&staged);
        return err(format!("re-wrapping the autorenew slot: {e:#}"));
    }
    if let Err(e) = std::fs::rename(&staged, &keytab) {
        // Same-dir rename essentially never fails; if it does, roll the vault
        // back so it still matches the unchanged live keytab.
        if let Err(re) = vault.rekey_signing_slot(AUTORENEW_ADMIN, &new_pw, &old_pw) {
            error!(
                "admin-server: CRITICAL — autorenew rotation failed AND rollback failed: \
                 {re:#}. The vault's autorenew slot may no longer match the keytab; \
                 recover with `ca auto-approve` (daemon stopped) before the next restart."
            );
        }
        let _ = std::fs::remove_file(&staged);
        return err(format!("installing the rotated keytab {}: {e:#}", keytab.display()));
    }
    // Still under the vault write guard: a concurrent unlock is serialized
    // behind it, so the vault and the in-process credential swap atomically.
    *ca.autorenew_pw.write() = Some(new_pw);
    drop(vault);
    audit(ca.dir(), "local", "rotate-autorenew", AUTORENEW_ADMIN, Duration::ZERO);
    RotateAutorenewResponse::Ok { warning }
}

/// The blocking half of `ApproveDelegation`: authenticate, validate the
/// proposed subtree, apply the child edit to the **local** resolver
/// config, and (if the request was Pending) commit the approval so the
/// child can poll. Idempotent on an already-`Approved` request — re-runs
/// re-sync the cluster. Returns `(edit, cluster member resolver addresses)`
/// for the async peer push; the `Err` arm carries the response to send.
fn approve_delegation_prepare(
    state: &Server,
    req: &ApproveDelegationRequest,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<TopologyFanout, ApproveDelegationResponse> {
    let err = |reason: String| ApproveDelegationResponse::Err { reason };
    let ca_dir = state
        .ca_dir()
        .map(|d| d.to_path_buf())
        .ok_or_else(|| err("this host does not hold the CA".to_string()))?;
    let ca = state.ca.as_ref().expect("CA role held");
    let authd = authenticate(ca, &req.credential).map_err(err)?;
    // The resolver edit + commit happen under this lock — mutually
    // exclusive with deny and other approves.
    let _guard = state.resolver_edit_lock.lock();
    // Pending ⇒ approve + commit; Approved ⇒ re-sync (re-apply + re-push,
    // already committed); else an error.
    let (pending, commit) = match delegation_store::status(&ca_dir, &req.request_id) {
        Ok(delegation_store::Status::Pending(p)) => (p, true),
        Ok(delegation_store::Status::Approved { .. }) => {
            match delegation_store::read_approved(&ca_dir, &req.request_id) {
                Ok(Some(rec)) => (rec.req, false),
                _ => return Err(err("the approved record vanished".to_string())),
            }
        }
        Ok(delegation_store::Status::Denied { .. }) => {
            return Err(err("that delegation was already denied".to_string()));
        }
        Ok(delegation_store::Status::Unknown) => {
            return Err(err(
                "no such pending delegation request (expired or never queued)"
                    .to_string(),
            ));
        }
        Err(e) => return Err(err(format!("{e:#}"))),
    };
    let mut map = state.map.lock();
    if !delegation_authority(&authd, &pending.proposed_path) {
        return Err(err(format!(
            "admin {} is not authorized to decide delegations at {:?}",
            authd.admin, pending.proposed_path
        )));
    }
    // Persist a staged snapshot before publishing it in memory. A failed disk
    // write must not leave the live controller map ahead of its durable map.
    let mut staged = map.clone();
    let change = netmap::delegate(
        &mut staged,
        &pending.proposed_path,
        pending.proposed_child,
        &pending.parent_servers,
        &pending.child_servers,
    )
    .map_err(|e| err(format!("updating authoritative topology: {e:#}")))?;
    let parent = change.parent;
    let child = change.child;
    netmap::save(&ca_dir, &staged)
        .map_err(|e| err(format!("persisting authoritative topology: {e:#}")))?;
    *map = staged;
    if commit {
        delegation_store::approve(&ca_dir, &pending, parent.members.clone())
            .map_err(|e| err(format!("committing the approval: {e:#}")))?;
    }
    audit(
        &ca_dir,
        &authd.admin,
        if commit { "approve-delegation" } else { "reconcile-delegation" },
        &format!("operation {operation_id}: {}", child.base),
        Duration::ZERO,
    );
    let mut targets = Vec::new();
    for cluster in [&parent, &child] {
        for server in map.servers.iter().filter(|server| {
            server.cluster == Some(cluster.id)
                && server.state == admin_proto::ServerState::Registered
        }) {
            let Some(local_member) = server.resolver.clone() else {
                continue;
            };
            targets.push((
                server.id,
                server.addr,
                topology_edit(&map, cluster, local_member),
            ));
        }
    }
    Ok(TopologyFanout { targets })
}

struct TopologyFanout {
    targets: Vec<(admin_proto::AdminServerId, SocketAddr, ReferralEdit)>,
}

fn topology_edit(
    map: &NetworkMap,
    cluster: &admin_proto::ClusterEntry,
    local_member: ResolverAddr,
) -> ReferralEdit {
    let parent = cluster.parent.and_then(|id| {
        map.clusters.iter().find(|parent| parent.id == id).map(|parent| {
            admin_proto::ClusterEdge {
                path: cluster.base.clone(),
                addrs: parent.members.clone(),
            }
        })
    });
    let mut children: Vec<_> = cluster
        .children
        .iter()
        .filter_map(|id| {
            map.clusters.iter().find(|child| child.id == *id).map(|child| {
                admin_proto::ClusterEdge {
                    path: child.base.clone(),
                    addrs: child.members.clone(),
                }
            })
        })
        .collect();
    children.sort_by(|a, b| a.path.cmp(&b.path));
    ReferralEdit::SetTopology {
        local_member,
        members: cluster.members.clone(),
        parent,
        children,
    }
}

/// Propagate the two delegation edits to every registered server in the parent
/// and child clusters using CA-owned routing addresses. A down or rejecting
/// target is reported, and re-approval idempotently re-runs reconciliation.
async fn push_topology(
    state: &Arc<Server>,
    fanout: TopologyFanout,
    operation_id: admin_proto::OperationId,
) -> Vec<PeerResult> {
    let (cert, key) = state.outbound_identity();
    let controller = state.map.lock().controller;
    let mut targets = fanout.targets;
    targets.sort_by_key(|(id, _, _)| *id);
    let home_ca = state.home_ca_der.clone();
    let roots = state.roots.clone();
    let mut results: Vec<_> =
        stream::iter(targets.into_iter().map(|(server, addr, edit)| {
            let cert = cert.clone();
            let key = key.clone();
            let home_ca = home_ca.clone();
            let roots = roots.clone();
            async move {
                let res = tokio::time::timeout(
                    PUSH_TIMEOUT,
                    admin_client::push_referral_edit(
                        addr,
                        server,
                        server == controller,
                        home_ca,
                        &cert,
                        &key,
                        roots,
                        operation_id,
                        &edit,
                    ),
                )
                .await
                .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
                .and_then(|r| r);
                PeerResult { server, addr, error: res.err().map(|e| format!("{e:#}")) }
            }
        }))
        .buffer_unordered(32)
        .collect()
        .await;
    results.sort_by_key(|r| r.server);
    results
}

/// Build a [`RootCertStore`] from a PEM trust bundle (one or more CA
/// certificates). Shared by the daemon's [`Server::new`] and the
/// `add-parent` CLI, which both need to trust the same network CA to talk
/// to cluster peers.
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

/// `ApproveDelegation` (admin-authenticated): the blocking prepare (auth,
/// validate, local edit, commit) followed by the async cluster-wide push.
/// Requires both the ca role (admin auth) and the resolver role.
async fn handle_approve_delegation(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    req: ApproveDelegationRequest,
) -> ApproveDelegationResponse {
    let operation_id = admin_proto::OperationId::new();
    let prepared = {
        let state = state.clone();
        run_signing(signs, move || approve_delegation_prepare(&state, &req, operation_id))
            .await
    };
    let fanout = match prepared {
        Ok(Ok(p)) => p,
        Ok(Err(resp)) => return resp,
        Err(e) => {
            return ApproveDelegationResponse::Err {
                reason: format!("approve delegation task panicked: {e}"),
            };
        }
    };
    let peers = push_topology(state, fanout, operation_id).await;
    ApproveDelegationResponse::Ok { operation_id, peers }
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

/// Whether `authd` is authorized to act destructively (revoke / deny) on a
/// certificate or request for `name`. This MUST mirror the issuance gate in
/// [`try_handle`] — otherwise an admin could destroy (revoke/deny) a name it
/// could never have signed, the exact scope-confinement break the role tier
/// exists to prevent.
///
/// - A `Signing` slot (the on-box recovery / autorenew credentials) or a
///   `may_manage_admins` superuser holds authority over everything.
/// - The reserved serving name needs a scoped server-enrollment grant
///   that authorizes minting it.
/// - Any other name must fall within the admin's issuance scope
///   (`allowed_san`).
fn admin_authority_over(authd: &ca_vault::Authenticated, name: &str) -> Result<bool> {
    if matches!(authd.kind, ca_vault::SlotKind::Signing) || authd.policy.may_manage_admins
    {
        return Ok(true);
    }
    if name.eq_ignore_ascii_case(SERVING_SAN) {
        return Ok(!authd.policy.server_enroll_scopes.is_empty());
    }
    name_permitted(name, &authd.policy.allowed_san)
}

/// Whether `authd` may approve or deny a delegation of the hierarchy subtree
/// at `path`. A delegation restructures the resolver hierarchy under `path`,
/// so — like a perms edit (see [`handle_edit_perms`]) — it needs authority
/// over that subtree: a broad admin (signing slot / `may_manage_admins`) or a
/// `perms_edit_scopes` entry covering the path.
fn delegation_authority(authd: &ca_vault::Authenticated, path: &str) -> bool {
    matches!(authd.kind, ca_vault::SlotKind::Signing)
        || authd.policy.may_manage_admins
        || perms_scope_covers(&authd.policy.perms_edit_scopes, path)
}

fn reject(reason: &str) -> SignResponse {
    SignResponse::Err { reason: reason.to_string() }
}

/// The one-live-cert refusal message, shared by the enqueue and sign paths.
/// `live` is the non-empty set of live certs already issued for `name`.
/// Naming the existing cert (serial/dates/glyph) makes clear this is a
/// *previously-issued* certificate, not one this request created — the point
/// of confusion when re-enrolling a node that was enrolled before.
fn one_live_refusal(name: &str, live: &[ca_store::IssuedRecord]) -> String {
    let existing = match live {
        [rec] => format!("The existing certificate — {}.", rec.describe()),
        [rec, ..] => {
            format!("{} live certificates exist, e.g. {}.", live.len(), rec.describe())
        }
        [] => "The existing certificate is already live.".to_string(),
    };
    format!(
        "an unexpired certificate already exists for {name:?} — this is a \
         previously-issued certificate, not one this request created. {existing} \
         An admin must revoke it first (`netidx admin ca revoke {name}`), then re-enroll."
    )
}

/// Append a line to the CA's audit log (best-effort — a failed write
/// must not fail the operation it records). Public because the CLI's
/// revoke writes the same trail the daemon's sign/approve/deny do.
pub fn audit(ca_dir: &Path, admin: &str, op: &str, name: &str, validity: Duration) {
    let ts =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let validity = humantime::format_duration(validity);
    let line = format!("ts={ts} admin={admin} op={op} name={name} validity={validity}\n");
    let r = OpenOptions::new()
        .create(true)
        .append(true)
        .open(ca_dir.join("audit.log"))
        .and_then(|mut f| f.write_all(line.as_bytes()));
    if let Err(e) = r {
        // Audit is best-effort; a failed write must not fail issuance.
        eprintln!("admin-server: WARNING failed to append audit log: {e}");
    }
}

// Protocol-v5 integration fixtures below construct the removed shared-SAN
// requests directly. They are retained temporarily as historical scenarios;
// protocol-v6 coverage lives in the focused modules and `v6_tests` below.
#[cfg(all(test, any()))]
mod tests {
    use super::*;
    use crate::{
        admin_client,
        admin_proto::{NodeKind, Secret},
        admin_server_config::{
            AdminServerConfig, CaRole, IdMapRole, ResolverRole, Roles,
        },
        ca::{Ca, CaParams, MIN_KEY_BITS, Subject},
        ca_vault::{self, Policy},
        fingerprint::Fingerprint,
        tls_tofu::TofuVerifier,
    };
    use zeroize::Zeroizing;

    fn policy() -> Policy {
        Policy {
            allowed_san: vec!["*.ryu-oh.org".to_string()],
            max_validity: std::time::Duration::from_secs(30 * 86400),
            id_map_groups: vec!["users".to_string()],
            may_enroll_servers: true,
            perms_edit_scopes: vec![],
            may_manage_admins: false,
            service_control_scopes: vec![],
        }
    }

    /// A fresh in-process issuer seeded from the CA dir exactly as
    /// [`Server::new`] does — the direct-call handler tests pass `&this`
    /// where the daemon would pass its own `Server::ca`.
    fn issuer(dir: &Path) -> ca_store::CaDir {
        let ca = ca_store::CaDir::open(dir).unwrap();
        *ca.autorenew_pw.write() = read_test_autorenew(dir);
        ca
    }

    /// Issue the daemon's TLS serving cert from the (vault-protected)
    /// CA and record it in the store the way the real bootstrap
    /// (`setup_server`) does — so the serving cert is a known issuance
    /// (its serial seeds the daemon's counter, and it can verify-renew
    /// itself). Returns `([leaf_pem ++ ca_pem], serving_key_pem)`.
    fn issue_serving_cert(dir: &Path) -> (Vec<u8>, Vec<u8>) {
        let mut cadir = ca_store::CaDir::open(dir).unwrap();
        issue_serving_cert_into(&mut cadir)
    }

    /// Mint + record a serving cert against an already-open CA dir — the
    /// one whose exclusive lock the caller already holds (a running CA
    /// server's `state.ca`, or a transient one [`issue_serving_cert`]
    /// opened). A second `CaDir::open` of the same dir would deadlock the
    /// flock, so a peer minted while a CA server is up must route through
    /// this, not open its own.
    fn issue_serving_cert_into(cadir: &mut ca_store::CaDir) -> (Vec<u8>, Vec<u8>) {
        let dir = cadir.dir().to_path_buf();
        let unlocked = cadir.vault.read().unlock("apw").unwrap();
        let ca_cert = std::fs::read(dir.join("certificate.pem")).unwrap();
        let ca = Ca::from_pem(dir.clone(), &unlocked.ca_key_pem, &ca_cert).unwrap();
        let kc = admin_client::generate_key_and_csr(SERVING_SAN).unwrap();
        let serial = cadir.store.lock().next_serial().unwrap();
        let leaf = ca
            .sign_request(
                kc.csr_pem.as_bytes(),
                &[SanEntry::Dns(SERVING_SAN.into())],
                std::time::Duration::from_secs(365 * 86400),
                serial,
            )
            .unwrap();
        let req = ca_store::QueuedReq::new(
            NodeKind::AdminServer,
            kc.csr_pem.clone(),
            SERVING_SAN.to_string(),
            std::time::Duration::from_secs(365 * 86400),
            "(test serving cert)".to_string(),
            None,
            None,
        );
        cadir
            .store
            .lock()
            .commit_issuance(
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
        let unlocked = ca_vault::CAVault::new(dir.to_path_buf()).unlock("apw").unwrap();
        let ca_cert = std::fs::read(dir.join("certificate.pem")).unwrap();
        let ca = Ca::from_pem(dir.to_path_buf(), &unlocked.ca_key_pem, &ca_cert).unwrap();
        let kc = admin_client::generate_key_and_csr(san).unwrap();
        let leaf = ca
            .sign_request(
                kc.csr_pem.as_bytes(),
                &[SanEntry::Dns(san.into())],
                std::time::Duration::from_secs(365 * 86400),
                1000,
            )
            .unwrap();
        let mut chain = leaf;
        chain.extend_from_slice(&ca_cert);
        (chain, kc.private_key_pem.as_bytes().to_vec())
    }

    /// Mint and index a live leaf for `san`, returning its serial — the
    /// "originating" cert a verified renewal continues. (A renewal's
    /// approval re-checks this serial is still live, so the originating
    /// cert must really be in the index.)
    fn commit_live_cert(dir: &Path, san: &str) -> u64 {
        let cadir = ca_store::CaDir::open(dir).unwrap();
        let unlocked = cadir.vault.read().unlock("apw").unwrap();
        let ca_cert = std::fs::read(dir.join("certificate.pem")).unwrap();
        let ca = Ca::from_pem(dir.to_path_buf(), &unlocked.ca_key_pem, &ca_cert).unwrap();
        let kc = admin_client::generate_key_and_csr(san).unwrap();
        let serial = cadir.store.lock().next_serial().unwrap();
        let leaf = ca
            .sign_request(
                kc.csr_pem.as_bytes(),
                &[SanEntry::Dns(san.into())],
                std::time::Duration::from_secs(365 * 86400),
                serial,
            )
            .unwrap();
        let req = ca_store::QueuedReq::new(
            NodeKind::Workstation,
            kc.csr_pem.clone(),
            san.to_string(),
            std::time::Duration::from_secs(365 * 86400),
            "(test live cert)".to_string(),
            None,
            None,
        );
        cadir
            .store
            .lock()
            .commit_issuance(&req, serial, san, std::str::from_utf8(&leaf).unwrap(), &[])
            .unwrap();
        serial
    }

    /// Bind an ephemeral port and run a admin server with the given
    /// roles + peers over the test CA at `dir`. Returns the address
    /// and the live state (so tests can assert on e.g. learned peers).
    async fn spawn_server_with(
        dir: &Path,
        roles: Roles,
        peers: Vec<SocketAddr>,
    ) -> (SocketAddr, Arc<Server>) {
        spawn_server_at(dir, "127.0.0.1:0", roles, peers).await
    }

    /// Like [`spawn_server_with`] but binds an explicit address and takes
    /// its serving cert + roots from `ca_dir` (which may differ from
    /// where this server's resolver config lives) — for multi-peer
    /// cluster tests where several admin servers share one CA but each
    /// holds its own resolver.json.
    async fn spawn_server_at(
        ca_dir: &Path,
        bind: &str,
        roles: Roles,
        peers: Vec<SocketAddr>,
    ) -> (SocketAddr, Arc<Server>) {
        let (cert, key) = issue_serving_cert(ca_dir);
        spawn_server_at_with_cert(ca_dir, bind, roles, peers, cert, key).await
    }

    /// Like [`spawn_server_at`] but takes a pre-minted serving cert + key
    /// instead of opening the CA dir to mint one. A peer spawned while a CA
    /// server already holds the dir's exclusive flock can't open it again,
    /// so the caller mints (or reuses) the cert via the held `CaDir` and
    /// hands it in here.
    async fn spawn_server_at_with_cert(
        ca_dir: &Path,
        bind: &str,
        roles: Roles,
        peers: Vec<SocketAddr>,
        cert: Vec<u8>,
        key: Vec<u8>,
    ) -> (SocketAddr, Arc<Server>) {
        let listener = TcpListener::bind(bind).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cfg = AdminServerConfig {
            domain: "ryu-oh.org".to_string(),
            listen: addr,
            // serve_on uses the in-memory PEMs; these paths are only
            // read by `serve()`, which tests don't go through.
            serving_cert: ca_dir.join("unused-cert.pem"),
            serving_key: ca_dir.join("unused-key.pem"),
            trusted: ca_dir.join("certificate.pem"),
            roles,
            ca_addr: None,
            peers,
            mdns: false,
            activation_units_dir: None,
        };
        let state = Server::new(cfg, None, cert, key).unwrap();
        let crl = load_serving_crl(&state);
        let acceptor = TlsAcceptor::from(Arc::new(
            build_server_config(
                &state.serving_cert_pem,
                &state.serving_key_pem,
                state.roots.clone(),
                crl.as_deref(),
            )
            .unwrap(),
        ));
        tokio::spawn(serve_on(listener, acceptor, state.clone()));
        (addr, state)
    }

    async fn spawn_ca_server(dir: &Path) -> (SocketAddr, Arc<Server>) {
        let roles = Roles {
            ca: Some(CaRole {
                dir: dir.to_path_buf(),
                autorenew: Some(autorenew_keytab(dir)),
            }),
            resolver: None,
            id_map: None,
        };
        spawn_server_with(dir, roles, vec![]).await
    }

    #[tokio::test]
    async fn register_then_get_map_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // Mint the resolver admin server's serving cert BEFORE the CA daemon
        // takes the dir's exclusive flock (it holds it for the test's life).
        let (cert, key) = issue_serving_cert(dir.path());
        let (ca_addr, ca_state) = spawn_ca_server(dir.path()).await;
        let roots = ca_state.roots.clone();

        // The CA seeded its own entry + address.
        let map0 = admin_client::get_map(ca_addr, roots.clone(), NodeKind::AdminServer)
            .await
            .unwrap();
        assert_eq!(map0.servers.len(), 1);
        assert_eq!(map0.ca_addr, Some(ca_addr));
        let v0 = map0.version;

        // A resolver admin server registers its facts (peer-cert-gated).
        let target: SocketAddr = "10.9.9.9:4565".parse().unwrap();
        let req =
            RegisterRequest { addr: target, roles: vec![Role::Resolver], cluster: None };
        let v1 = admin_client::register(ca_addr, &cert, &key, roots.clone(), &req)
            .await
            .unwrap();
        assert!(v1 > v0);

        // Served whole to a plain reader: both servers now present.
        let map1 = admin_client::get_map(ca_addr, roots.clone(), NodeKind::AdminServer)
            .await
            .unwrap();
        assert_eq!(map1.servers.len(), 2);
        assert!(map1.servers.iter().any(|s| s.addr == target));
        assert_eq!(map1.version, v1);

        // Idempotent re-register of identical facts: no version churn.
        let v2 = admin_client::register(ca_addr, &cert, &key, roots.clone(), &req)
            .await
            .unwrap();
        assert_eq!(v2, v1);

        // Deregister drops it and bumps.
        let v3 = admin_client::deregister(ca_addr, &cert, &key, roots.clone(), target)
            .await
            .unwrap();
        assert!(v3 > v1);
        let map2 =
            admin_client::get_map(ca_addr, roots, NodeKind::AdminServer).await.unwrap();
        assert_eq!(map2.servers.len(), 1);
    }

    /// A admin server enforces the CA's CRL on inbound peer certs: a
    /// revoked-but-unexpired serving cert is refused at the handshake, so
    /// the peer-gated endpoints (here Register) are closed to it — while a
    /// freshly issued cert is still accepted (the CRL only adds denials).
    #[tokio::test(flavor = "multi_thread")]
    async fn revoked_peer_cert_is_refused_at_the_handshake() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());

        // Issue a peer serving cert, capture its serial, revoke it, and
        // publish the CRL — all BEFORE the CA admin server starts (it holds
        // the dir's exclusive flock for the test's life), so its acceptor
        // loads the CRL with this serial already revoked. `next_serial` goes
        // through a transient CaDir that drops before `issue_serving_cert`
        // opens its own (a second live open of the same dir would deadlock).
        let victim_serial = ca_store::CaDir::open(dir.path())
            .unwrap()
            .store
            .lock()
            .next_serial()
            .unwrap();
        let (vcert, vkey) = issue_serving_cert(dir.path());
        let now = ca_store::now_unix();
        let unlocked =
            ca_vault::CAVault::new(dir.path().to_path_buf()).unlock("apw").unwrap();
        {
            let cadir = ca_store::CaDir::open(dir.path()).unwrap();
            let revoked = cadir
                .store
                .lock()
                .revoke(
                    victim_serial,
                    ca_store::Revocation {
                        serial: victim_serial,
                        revoked_unix: now,
                        reason: "test".into(),
                    },
                )
                .unwrap();
            assert!(revoked, "the issued serial should be live, then revoked");
            cadir.store.lock().write_crl(&unlocked.ca_key_pem).unwrap();
        }
        // A fresh (un-revoked) serving cert — also minted before the daemon
        // takes the lock; it must still be accepted (the CRL only adds
        // denials).
        let (gcert, gkey) = issue_serving_cert(dir.path());

        let (ca_addr, ca_state) = spawn_ca_server(dir.path()).await;
        let roots = ca_state.roots.clone();
        let req = RegisterRequest {
            addr: "10.1.1.1:4565".parse().unwrap(),
            roles: vec![Role::Resolver],
            cluster: None,
        };

        // The revoked cert is refused at the TLS handshake — Register never
        // reaches the app layer.
        let denied =
            admin_client::register(ca_addr, &vcert, &vkey, roots.clone(), &req).await;
        assert!(denied.is_err(), "a revoked peer cert must be refused at the handshake");

        // A fresh (un-revoked) serving cert is still accepted — the CRL adds
        // denials without locking valid peers out.
        admin_client::register(ca_addr, &gcert, &gkey, roots, &req)
            .await
            .expect("a valid peer cert is still accepted");
    }

    /// Build a vault-protected CA in `dir`: a real (RSA) CA whose key is
    /// moved into a 1-admin vault, no `private.key` left behind.
    /// The server's signing credential in tests: a real (plaintext) keytab
    /// at `<ca-dir>/autorenew.keytab` whose password unlocks an `autorenew`
    /// signing slot. `setup_ca` mints both, the spawn helpers point
    /// `CaRole::autorenew` at the keytab, and `issuer()` reads it — so the
    /// server can sign on an authenticated admin's behalf exactly as in prod.
    const AUTORENEW_PW: &str = "renew-secret";

    fn autorenew_keytab(dir: &Path) -> PathBuf {
        dir.join("autorenew.keytab")
    }

    fn read_test_autorenew(dir: &Path) -> Option<Zeroizing<String>> {
        std::fs::read_to_string(autorenew_keytab(dir)).ok().map(Zeroizing::new)
    }

    fn setup_ca(dir: &Path) {
        setup_ca_with_policy(dir, policy());
    }

    fn setup_ca_with_policy(dir: &Path, policy: Policy) {
        let params = CaParams {
            directory: dir.to_path_buf(),
            subject: Subject::cn("Test CA".to_string()),
            san: vec![],
            key_bits: MIN_KEY_BITS,
            validity: std::time::Duration::from_secs(30 * 86400),
        };
        Ca::init(&params, None).unwrap();
        let key = std::fs::read(dir.join("private.key")).unwrap();
        let mut vault = ca_vault::CAVault::new(dir.to_path_buf());
        // "alice" stands in for the recovery / first signing admin; the
        // requester in signing tests authenticates as her.
        vault.create(&key, "alice", "apw", policy).unwrap();
        std::fs::remove_file(dir.join("private.key")).unwrap();
        // The box's autorenew signing slot + its keytab — the credential the
        // server signs with. Empty policy (it only unlocks; authorization is
        // the requester's).
        vault
            .add_signing_slot(
                "apw",
                AUTORENEW_ADMIN,
                AUTORENEW_PW,
                Policy {
                    allowed_san: vec![],
                    max_validity: std::time::Duration::from_secs(730 * 86400),
                    id_map_groups: vec![],
                    may_enroll_servers: false,
                    perms_edit_scopes: vec![],
                    may_manage_admins: false,
                    service_control_scopes: vec![],
                },
            )
            .unwrap();
        std::fs::write(autorenew_keytab(dir), AUTORENEW_PW).unwrap();
    }

    fn request(name: &str, admin: &str, pw: &str, days: u32) -> SignRequest {
        let kc = admin_client::generate_key_and_csr(name).unwrap();
        SignRequest {
            admin: admin.to_string(),
            password: Secret(pw.to_string()),
            csr_pem: kc.csr_pem,
            requested_name: name.to_string(),
            requested_validity: std::time::Duration::from_secs(days as u64 * 86400),
            id_map_groups: vec!["users".to_string()],
        }
    }

    #[test]
    fn signs_a_permitted_name() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let req = request("resolver.ryu-oh.org", "alice", "apw", 30);
        let signed = handle_sign_request(&issuer(dir.path()), &req);
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
                let live = ca_store::CaDir::open(dir.path())
                    .unwrap()
                    .store
                    .lock()
                    .live_for_name("resolver.ryu-oh.org")
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
                max_validity: std::time::Duration::from_secs(30 * 86400),
                // The *allowed set* — what this admin may assign.
                id_map_groups: vec!["users".to_string(), "dev".to_string()],
                may_enroll_servers: false,
                perms_edit_scopes: vec![],
                may_manage_admins: false,
                service_control_scopes: vec![],
            },
        );
        let iss = issuer(dir.path());
        // Choosing an allowed subset works, and the plan carries the
        // request's choice, not the whole policy.
        let mut req = request("a.ryu-oh.org", "alice", "apw", 30);
        req.id_map_groups = vec!["dev".to_string()];
        let signed = handle_sign_request(&iss, &req);
        assert!(matches!(signed.resp, SignResponse::Ok { .. }));
        assert_eq!(signed.push.unwrap().groups, vec!["dev".to_string()]);
        // Choosing no groups skips registration without failing the
        // sign.
        let mut req = request("b.ryu-oh.org", "alice", "apw", 30);
        req.id_map_groups = vec![];
        let signed = handle_sign_request(&iss, &req);
        assert!(matches!(signed.resp, SignResponse::Ok { .. }));
        assert!(signed.push.is_none());
        // Choosing a group outside the allowed set refuses the whole
        // sign — silently dropping the registration would produce a
        // node whose cert works but whose perms mysteriously don't.
        let mut req = request("c.ryu-oh.org", "alice", "apw", 30);
        req.id_map_groups = vec!["wheel".to_string()];
        let signed = handle_sign_request(&iss, &req);
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
        let signed = handle_sign_request(&issuer(dir.path()), &req);
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
        match handle_sign_request(&issuer(dir.path()), &req).resp {
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
            handle_sign_request(&issuer(dir.path()), &req).resp
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
        // must still not be able to mint a admin server's serving cert
        // via Sign (that would enable impersonating the daemon). Only
        // the `may_enroll_servers`-gated Enroll path may.
        setup_ca_with_policy(
            dir.path(),
            Policy {
                allowed_san: vec!["*".to_string()],
                max_validity: std::time::Duration::from_secs(30 * 86400),
                id_map_groups: vec![],
                may_enroll_servers: true,
                perms_edit_scopes: vec![],
                may_manage_admins: false,
                service_control_scopes: vec![],
            },
        );
        let req = request(SERVING_SAN, "alice", "apw", 30);
        match handle_sign_request(&issuer(dir.path()), &req).resp {
            SignResponse::Err { reason } => assert!(reason.contains("reserved")),
            SignResponse::Ok { .. } => panic!("issued the reserved serving name"),
        }
    }

    #[test]
    fn admin_name_must_match_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // A valid password under the wrong admin name: authentication keys
        // on the (name, password) pair, so a name with no matching slot is
        // rejected outright.
        let req = request("a.ryu-oh.org", "bob", "apw", 30);
        match handle_sign_request(&issuer(dir.path()), &req).resp {
            SignResponse::Err { reason } => {
                assert!(reason.contains("authentication failed"))
            }
            SignResponse::Ok { .. } => panic!("admin/password mismatch accepted"),
        }
    }

    #[test]
    fn role_admin_issues_via_server_signing() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // A ROLE admin: it carries an issuance scope but wraps no MK, so its
        // password can never unlock the CA key.
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot(
                "eu-ops",
                "eupw",
                Policy {
                    allowed_san: vec!["*.eu.ryu-oh.org".to_string()],
                    max_validity: std::time::Duration::from_secs(30 * 86400),
                    id_map_groups: vec!["users".to_string()],
                    may_enroll_servers: false,
                    perms_edit_scopes: vec![],
                    may_manage_admins: false,
                    service_control_scopes: vec![],
                },
            )
            .unwrap();
        assert!(
            ca_vault::CAVault::new(dir.path().to_path_buf()).unlock("eupw").is_err(),
            "role can't unlock the key"
        );

        // Yet the SERVER signs an in-scope name on its behalf.
        let ok = request("host.eu.ryu-oh.org", "eu-ops", "eupw", 30);
        assert!(matches!(
            handle_sign_request(&issuer(dir.path()), &ok).resp,
            SignResponse::Ok { .. }
        ));
        // Out of scope is still refused (the authz gate is the whole boundary).
        let bad = request("host.us.ryu-oh.org", "eu-ops", "eupw", 30);
        assert!(matches!(
            handle_sign_request(&issuer(dir.path()), &bad).resp,
            SignResponse::Err { .. }
        ));
        // The audit names the REQUESTER, not the server's autorenew credential.
        let log = std::fs::read_to_string(dir.path().join("audit.log")).unwrap();
        assert!(log.contains("admin=eu-ops"), "audit names the requester: {log}");
        assert!(
            !log.contains("admin=autorenew"),
            "audit must not name the server cred: {log}"
        );
    }

    #[test]
    fn ca_without_autorenew_credential_is_read_only() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // An issuer holding NO signing credential (a CA whose keytab is
        // missing) authenticates + authorizes but cannot reach the key.
        // `CaDir::open` leaves `autorenew_pw` None — exactly that state.
        let iss = ca_store::CaDir::open(dir.path()).unwrap();
        let req = request("host.ryu-oh.org", "alice", "apw", 30);
        match handle_sign_request(&iss, &req).resp {
            SignResponse::Err { reason } => {
                assert!(reason.contains("cannot sign"), "{reason}")
            }
            SignResponse::Ok { .. } => {
                panic!("a CA with no signing credential must not sign")
            }
        }
    }

    /// Revocation is scope-bound exactly like issuance: a role admin scoped
    /// to `*.eu` can revoke its own `*.eu` leaves but NOT another region's
    /// `*.us` leaf nor a admin-server serving cert — otherwise the lowest
    /// issuance privilege could take the whole network's TLS offline via the
    /// CRL. (Adversarial-review finding: the old gate only checked "has any
    /// issuance authority", not per-serial scope.)
    #[test]
    fn revoke_is_scope_bound() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path()); // alice = signing slot (broad)
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot(
                "eu-ops",
                "eupw",
                Policy {
                    allowed_san: vec!["*.eu.ryu-oh.org".to_string()],
                    max_validity: std::time::Duration::from_secs(30 * 86400),
                    id_map_groups: vec![],
                    may_enroll_servers: false,
                    perms_edit_scopes: vec![],
                    may_manage_admins: false,
                    service_control_scopes: vec![],
                },
            )
            .unwrap();
        // A perms-only role admin with no issuance authority at all.
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot(
                "pat",
                "patpw",
                Policy {
                    allowed_san: vec![],
                    max_validity: std::time::Duration::from_secs(0 * 86400),
                    id_map_groups: vec![],
                    may_enroll_servers: false,
                    perms_edit_scopes: vec!["/eu".to_string()],
                    may_manage_admins: false,
                    service_control_scopes: vec![],
                },
            )
            .unwrap();
        let us = commit_live_cert(dir.path(), "host.us.ryu-oh.org");
        let eu = commit_live_cert(dir.path(), "host.eu.ryu-oh.org");
        let serving = commit_live_cert(dir.path(), SERVING_SAN);
        let revoked = |serial: u64| {
            ca_store::CaDir::open(dir.path())
                .unwrap()
                .store
                .lock()
                .list_signed()
                .unwrap()
                .into_iter()
                .find(|r| r.serial == serial)
                .unwrap()
                .revoked
                .is_some()
        };

        // A perms-only admin can't revoke anything — rejected up front.
        let pat = handle_revoke(
            &issuer(dir.path()),
            &RevokeRequest {
                admin: "pat".to_string(),
                password: Secret("patpw".to_string()),
                serials: vec![eu],
                reason: "x".to_string(),
            },
        );
        assert!(
            matches!(pat, RevokeResponse::Err { reason } if reason.contains("not authorized"))
        );
        assert!(!revoked(eu), "a rejected revoke must not have revoked anything");

        // eu-ops asks to revoke all three; only its in-scope leaf is revoked.
        let resp = handle_revoke(
            &issuer(dir.path()),
            &RevokeRequest {
                admin: "eu-ops".to_string(),
                password: Secret("eupw".to_string()),
                serials: vec![us, eu, serving],
                reason: "x".to_string(),
            },
        );
        let RevokeResponse::Ok { warnings } = resp else {
            panic!("expected Ok with warnings")
        };
        assert!(revoked(eu), "eu-ops may revoke its own *.eu leaf");
        assert!(!revoked(us), "eu-ops must NOT revoke another region's *.us leaf");
        assert!(!revoked(serving), "eu-ops must NOT revoke a serving cert");
        assert_eq!(
            warnings.len(),
            2,
            "the two out-of-scope serials are reported: {warnings:?}"
        );

        // The broad signing admin (alice) can revoke the *.us leaf.
        let resp = handle_revoke(
            &issuer(dir.path()),
            &RevokeRequest {
                admin: "alice".to_string(),
                password: Secret("apw".to_string()),
                serials: vec![us],
                reason: "x".to_string(),
            },
        );
        assert!(matches!(resp, RevokeResponse::Ok { .. }));
        assert!(revoked(us), "a broad admin may revoke any cert");
    }

    /// Denying a queued request is scope-bound like revocation: a `*.eu` role
    /// admin can't deny another region's pending request (a queue-DoS the old
    /// keyless-authenticate path left wide open).
    #[test]
    fn deny_is_scope_bound() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot(
                "eu-ops",
                "eupw",
                Policy {
                    allowed_san: vec!["*.eu.ryu-oh.org".to_string()],
                    max_validity: std::time::Duration::from_secs(30 * 86400),
                    id_map_groups: vec![],
                    may_enroll_servers: false,
                    perms_edit_scopes: vec![],
                    may_manage_admins: false,
                    service_control_scopes: vec![],
                },
            )
            .unwrap();
        let peer = "1.2.3.4:5".parse().unwrap();
        let enqueue = |name: &str| -> String {
            let kc = admin_client::generate_key_and_csr(name).unwrap();
            let req = EnqueueRequest {
                kind: NodeKind::Client,
                csr_pem: kc.csr_pem,
                requested_name: name.to_string(),
                requested_validity: std::time::Duration::from_secs(30 * 86400),
                enroll_listen: None,
            };
            match handle_enqueue(&issuer(dir.path()), &req, peer, None) {
                EnqueueResponse::Ok { request_id } => request_id,
                EnqueueResponse::Err { reason } => panic!("enqueue {name}: {reason}"),
            }
        };
        let us_id = enqueue("host.us.ryu-oh.org");
        let eu_id = enqueue("host.eu.ryu-oh.org");

        // Out of scope: refused, and the request stays pending.
        let denied = handle_deny(
            &issuer(dir.path()),
            &DenyRequest {
                admin: "eu-ops".to_string(),
                password: Secret("eupw".to_string()),
                request_id: us_id.clone(),
                reason: "x".to_string(),
            },
        );
        assert!(
            matches!(denied, DenyResponse::Err { reason } if reason.contains("not authorized"))
        );
        assert!(matches!(
            ca_store::CaDir::open(dir.path())
                .unwrap()
                .store
                .lock()
                .status(&us_id)
                .unwrap(),
            ca_store::Status::Pending(_)
        ));

        // In scope: eu-ops may deny its own region's request.
        let ok = handle_deny(
            &issuer(dir.path()),
            &DenyRequest {
                admin: "eu-ops".to_string(),
                password: Secret("eupw".to_string()),
                request_id: eu_id.clone(),
                reason: "x".to_string(),
            },
        );
        assert!(matches!(ok, DenyResponse::Ok));
        assert!(matches!(
            ca_store::CaDir::open(dir.path())
                .unwrap()
                .store
                .lock()
                .status(&eu_id)
                .unwrap(),
            ca_store::Status::Denied(_)
        ));
    }

    /// A revocation that lands while a verified renewal is queued must win:
    /// the renewal is re-validated against the live index UNDER THE ISSUER
    /// LOCK at approval, so the originating serial being revoked refuses the
    /// renewal instead of auto-minting a fresh (attacker-keyed) cert. The
    /// enqueue-then-revoke race the existing `a_revoked_certificate_cannot_
    /// self_renew` (revoke-then-enqueue) didn't cover.
    #[test]
    fn a_renewal_loses_to_a_revocation_in_flight() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let orig = commit_live_cert(dir.path(), "host.ryu-oh.org");
        let fp = ca_store::CaDir::open(dir.path())
            .unwrap()
            .store
            .lock()
            .list_signed()
            .unwrap()
            .into_iter()
            .find(|r| r.serial == orig)
            .unwrap()
            .spki_fp;
        // Enqueue a verified renewal (presenting our live cert's identity).
        let kc = admin_client::generate_key_and_csr("host.ryu-oh.org").unwrap();
        let enq = EnqueueRequest {
            kind: NodeKind::Client,
            csr_pem: kc.csr_pem,
            requested_name: "host.ryu-oh.org".to_string(),
            requested_validity: std::time::Duration::from_secs(30 * 86400),
            enroll_listen: None,
        };
        let pid = PeerIdent {
            san: "host.ryu-oh.org".to_string(),
            serial: Some(orig),
            spki_fp: Some(fp),
        };
        let rid = match handle_enqueue(
            &issuer(dir.path()),
            &enq,
            "1.2.3.4:5".parse().unwrap(),
            Some(&pid),
        ) {
            EnqueueResponse::Ok { request_id } => request_id,
            EnqueueResponse::Err { reason } => panic!("renewal enqueue: {reason}"),
        };
        assert!(
            ca_store::CaDir::open(dir.path())
                .unwrap()
                .store
                .lock()
                .pending()
                .unwrap()
                .iter()
                .any(|q| q.id == rid && q.renewal_of == Some(orig)),
            "must be queued as a verified renewal"
        );
        // The admin revokes the originating cert (laptop stolen).
        ca_store::CaDir::open(dir.path())
            .unwrap()
            .store
            .lock()
            .revoke(
                orig,
                ca_store::Revocation {
                    serial: orig,
                    revoked_unix: ca_store::now_unix(),
                    reason: "stolen".to_string(),
                },
            )
            .unwrap();
        // The in-flight renewal is now refused, not silently re-minted.
        let approved = handle_approve(
            &issuer(dir.path()),
            &ApproveRequest {
                admin: "alice".to_string(),
                password: Secret("apw".to_string()),
                request_id: rid.clone(),
                id_map_groups: vec![],
            },
        )
        .unwrap();
        match approved.resp {
            SignResponse::Err { reason } => {
                assert!(reason.contains("no longer live"), "{reason}")
            }
            SignResponse::Ok { .. } => {
                panic!("a renewal of a revoked cert must be refused")
            }
        }
        assert!(matches!(
            ca_store::CaDir::open(dir.path()).unwrap().store.lock().status(&rid).unwrap(),
            ca_store::Status::Pending(_)
        ));
    }

    /// A verified renewal binds to our cert's KEY, not just its serial: a
    /// co-trusted foreign CA's cert with a colliding serial (and a different
    /// key) for the same name is NOT treated as a renewal of ours.
    #[test]
    fn a_colliding_serial_with_a_foreign_key_is_not_a_renewal() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let orig = commit_live_cert(dir.path(), "host.ryu-oh.org");
        let real_fp = ca_store::CaDir::open(dir.path())
            .unwrap()
            .store
            .lock()
            .list_signed()
            .unwrap()
            .into_iter()
            .find(|r| r.serial == orig)
            .unwrap()
            .spki_fp;
        let kc = admin_client::generate_key_and_csr("host.ryu-oh.org").unwrap();
        let enq = EnqueueRequest {
            kind: NodeKind::Client,
            csr_pem: kc.csr_pem,
            requested_name: "host.ryu-oh.org".to_string(),
            requested_validity: std::time::Duration::from_secs(30 * 86400),
            enroll_listen: None,
        };
        let peer = "1.2.3.4:5".parse().unwrap();
        // SAN + serial match our live record, but the key fingerprint does
        // not → not a renewal; treated as an ordinary request, which the
        // one-live-cert rule then refuses (there is already a live cert).
        let foreign = PeerIdent {
            san: "host.ryu-oh.org".to_string(),
            serial: Some(orig),
            spki_fp: Some("AAAA BBBB CCCC".to_string()),
        };
        match handle_enqueue(&issuer(dir.path()), &enq, peer, Some(&foreign)) {
            EnqueueResponse::Err { reason } => {
                assert!(reason.contains("already exists"), "{reason}")
            }
            EnqueueResponse::Ok { .. } => {
                panic!("a foreign cert with a colliding serial must not renew")
            }
        }
        // Our own cert's fingerprint IS a verified renewal.
        let ours = PeerIdent {
            san: "host.ryu-oh.org".to_string(),
            serial: Some(orig),
            spki_fp: Some(real_fp),
        };
        let rid = match handle_enqueue(&issuer(dir.path()), &enq, peer, Some(&ours)) {
            EnqueueResponse::Ok { request_id } => request_id,
            EnqueueResponse::Err { reason } => {
                panic!("our own cert should renew: {reason}")
            }
        };
        assert!(
            ca_store::CaDir::open(dir.path())
                .unwrap()
                .store
                .lock()
                .pending()
                .unwrap()
                .iter()
                .any(|q| q.id == rid && q.renewal_of == Some(orig))
        );
    }

    #[test]
    fn enroll_requires_the_policy_bit() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca_with_policy(
            dir.path(),
            Policy {
                allowed_san: vec!["*".to_string()],
                max_validity: std::time::Duration::from_secs(30 * 86400),
                id_map_groups: vec![],
                may_enroll_servers: false,
                perms_edit_scopes: vec![],
                may_manage_admins: false,
                service_control_scopes: vec![],
            },
        );
        let kc = admin_client::generate_key_and_csr(SERVING_SAN).unwrap();
        let req = EnrollRequest {
            admin: "alice".to_string(),
            password: Secret("apw".to_string()),
            csr_pem: kc.csr_pem,
            listen: "127.0.0.1:4565".parse().unwrap(),
        };
        match handle_enroll_request(&issuer(dir.path()), &req, false) {
            SignResponse::Err { reason } => assert!(reason.contains("may not enroll")),
            SignResponse::Ok { .. } => panic!("enrolled without the policy bit"),
        }
    }

    /// A request over the local control socket (`local = true`) authorizes
    /// as the `SO_PEERCRED` superuser, so it re-mints the reserved serving
    /// SAN even with empty credentials and an admin that lacks
    /// `may_enroll_servers`. This is the path renewd uses to recover an
    /// expired serving cert locally without TLS (which would deadlock).
    /// Counterpart to `enroll_requires_the_policy_bit`, which pins the TLS
    /// (`local = false`) path.
    #[test]
    fn local_enroll_bypasses_admin_auth() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca_with_policy(
            dir.path(),
            Policy {
                allowed_san: vec!["*".to_string()],
                max_validity: std::time::Duration::from_secs(30 * 86400),
                id_map_groups: vec![],
                may_enroll_servers: false,
                perms_edit_scopes: vec![],
                may_manage_admins: false,
                service_control_scopes: vec![],
            },
        );
        let kc = admin_client::generate_key_and_csr(SERVING_SAN).unwrap();
        let req = EnrollRequest {
            admin: String::new(),
            password: Secret(String::new()),
            csr_pem: kc.csr_pem,
            listen: "127.0.0.1:4565".parse().unwrap(),
        };
        match handle_enroll_request(&issuer(dir.path()), &req, true) {
            SignResponse::Ok { signed_cert_pem, .. } => assert!(
                signed_cert_pem.contains("BEGIN CERTIFICATE"),
                "expected a signed serving cert, got: {signed_cert_pem}"
            ),
            SignResponse::Err { reason } => {
                panic!("local enroll refused despite SO_PEERCRED superuser: {reason}")
            }
        }
    }

    #[test]
    fn enroll_signs_the_reserved_san() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path()); // policy(): may_enroll_servers = true
        let kc = admin_client::generate_key_and_csr(SERVING_SAN).unwrap();
        let req = EnrollRequest {
            admin: "alice".to_string(),
            password: Secret("apw".to_string()),
            csr_pem: kc.csr_pem,
            listen: "127.0.0.1:4565".parse().unwrap(),
        };
        let SignResponse::Ok { signed_cert_pem, .. } =
            handle_enroll_request(&issuer(dir.path()), &req, false)
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
            admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        assert!(!identity.fingerprint.text().is_empty());
        assert_eq!(identity.domain, "ryu-oh.org");
        assert_eq!(identity.roles, vec![Role::Ca]);
        // Then sign, pinned to the confirmed identity.
        let issued = admin_client::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            std::time::Duration::from_secs(30 * 86400),
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
            admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        identity.fingerprint = Fingerprint::of_der(b"not the real CA cert");
        let err = admin_client::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            std::time::Duration::from_secs(30 * 86400),
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
            admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        let err = admin_client::request_cert(
            addr,
            NodeKind::Resolver,
            "resolver.ryu-oh.org",
            "alice",
            Zeroizing::new("WRONG".to_string()),
            std::time::Duration::from_secs(30 * 86400),
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
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: None,
            id_map: None,
        };
        let (addr, _state) = spawn_server_with(dir.path(), roles, vec![peer]).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        let info =
            admin_client::get_info(addr, NodeKind::Client, &identity).await.unwrap();
        assert_eq!(info.domain, "ryu-oh.org");
        assert_eq!(info.ca_addr, Some(addr));
        assert_eq!(info.peers, vec![peer]);
        assert!(info.resolver.is_none());
    }

    #[tokio::test]
    async fn aggregation_stops_after_an_authoritative_map() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // B: a roleless stepping stone; A: the CA, which knows about B.
        let (b_addr, _b) = spawn_server_with(dir.path(), Roles::default(), vec![]).await;
        let roles = Roles {
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: None,
            id_map: None,
        };
        let (a_addr, _a) = spawn_server_with(dir.path(), roles, vec![b_addr]).await;
        let identity =
            admin_client::fetch_identity(a_addr, NodeKind::Client).await.unwrap();
        let info = admin_client::aggregate(&[a_addr], NodeKind::Client, &identity)
            .await
            .unwrap();
        assert_eq!(info.ca_addr, Some(a_addr));
        assert_eq!(info.reached.first(), Some(&a_addr), "seed order is preserved");
        assert!(info.reached.contains(&a_addr));
        assert!(
            !info.reached.contains(&b_addr),
            "a discovery-hint peer must not be contacted after the controller map is available"
        );
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
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: None,
            id_map: None,
        };
        let (a_addr, _a) = spawn_server_with(dir.path(), roles_a, vec![b_addr]).await;
        let identity =
            admin_client::fetch_identity(a_addr, NodeKind::Workstation).await.unwrap();
        let issued = admin_client::request_cert(
            a_addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            std::time::Duration::from_secs(30 * 86400),
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
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: None,
            id_map: None,
        };
        let (a_addr, _a) = spawn_server_with(dir.path(), roles, vec![dead]).await;
        let identity =
            admin_client::fetch_identity(a_addr, NodeKind::Workstation).await.unwrap();
        let issued = admin_client::request_cert(
            a_addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            std::time::Duration::from_secs(30 * 86400),
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
            admin_client::fetch_identity(a_addr, NodeKind::AdminServer).await.unwrap();
        let new_listen: SocketAddr = "192.168.0.42:4565".parse().unwrap();
        let issued = admin_client::enroll(
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
            admin_client::get_info(a_addr, NodeKind::Client, &identity).await.unwrap();
        assert!(info.peers.contains(&new_listen));
    }

    #[tokio::test]
    async fn enroll_without_the_bit_is_refused_over_the_wire() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // bob may sign but not enroll.
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_signing_slot(
                "apw",
                "bob",
                "bpw",
                Policy {
                    allowed_san: vec!["*".to_string()],
                    max_validity: std::time::Duration::from_secs(30 * 86400),
                    id_map_groups: vec![],
                    may_enroll_servers: false,
                    perms_edit_scopes: vec![],
                    may_manage_admins: false,
                    service_control_scopes: vec![],
                },
            )
            .unwrap();
        let (a_addr, a_state) = spawn_ca_server(dir.path()).await;
        let identity =
            admin_client::fetch_identity(a_addr, NodeKind::AdminServer).await.unwrap();
        let new_listen: SocketAddr = "192.168.0.43:4565".parse().unwrap();
        let err = admin_client::enroll(
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
    /// an unauthenticated (or merely CA-issued-but-not-admin-server)
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
        admin_proto::write_msg(
            &mut tls,
            &ClientHello {
                protocol_version: PROTOCOL_VERSION,
                kind: NodeKind::AdminServer,
            },
        )
        .await
        .unwrap();
        let _: ServerHello = admin_proto::read_msg(&mut tls).await.unwrap();
        admin_proto::write_msg(&mut tls, &Request::AddIdentity(req.clone()))
            .await
            .unwrap();
        admin_proto::read_msg(&mut tls).await.unwrap()
    }

    #[tokio::test]
    async fn add_identity_requires_a_admin_server_peer_cert() {
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
            AddIdentityResponse::Ok { .. } => {
                panic!("unauthenticated AddIdentity accepted")
            }
        }
        // 2. A CA-issued client cert that is NOT a admin-server serving
        //    cert (an ordinary node identity) must also be refused —
        //    holding *some* cert from the CA is not authority to edit
        //    the id-map.
        let (eve_chain, eve_key) = issue_client_cert(dir.path(), "eve.ryu-oh.org");
        let mut roots = rustls::RootCertStore::empty();
        let ca_pem = std::fs::read(dir.path().join("certificate.pem")).unwrap();
        for der in rustls_pemfile::certs(&mut std::io::Cursor::new(&ca_pem)) {
            roots.add(der.unwrap()).unwrap();
        }
        let err = admin_client::push_identity(b_addr, &eve_chain, &eve_key, roots, &req)
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
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: None,
            id_map: Some(IdMapRole { map: map_path.clone() }),
        };
        let (addr, _state) = spawn_server_with(dir.path(), roles, vec![]).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        // Enrollee queues (no credentials) and starts polling.
        let pending = admin_client::enqueue(
            addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            std::time::Duration::from_secs(30 * 86400),
            &identity,
        )
        .await
        .unwrap();
        assert!(matches!(
            admin_client::poll(addr, NodeKind::Workstation, &pending, &identity)
                .await
                .unwrap(),
            admin_client::PollOutcome::Pending
        ));
        // Admin lists the queue and matches the request code — computed
        // locally from the queued CSR, equal to the one the enrollee
        // displays.
        let queue =
            admin_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].requested_name, "eric.ryu-oh.org");
        assert_eq!(
            admin_client::csr_fingerprint(&queue[0].csr_pem).unwrap(),
            pending.fingerprint,
            "admin-side and enrollee-side request codes must agree",
        );
        // Admin approves, choosing the groups at approval time.
        let warnings = admin_client::approve(
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
        let issued =
            match admin_client::poll(addr, NodeKind::Workstation, &pending, &identity)
                .await
                .unwrap()
            {
                admin_client::PollOutcome::Issued(i) => i,
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
            admin_client::poll(addr, NodeKind::Workstation, &pending, &identity)
                .await
                .unwrap(),
            admin_client::PollOutcome::Issued(_)
        ));
        let err =
            admin_client::approve(addr, "alice", "apw", &queue[0].id, vec![], &identity)
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
    /// separate autorenew daemon enforced, now inside the admin server. The
    /// slot's empty policy is a second line of defense: it would refuse a
    /// non-renewal even if the filter let one through.
    #[test]
    fn autorenew_sweep_approves_only_verified_renewals() {
        let dir = tempfile::tempdir().unwrap();
        // setup_ca already mints the `autorenew` signing slot + keytab
        // (password AUTORENEW_PW); the sweep authenticates + signs with it.
        setup_ca(dir.path());
        // The renewal continues a live identity — mint+index its originating
        // cert so the approval's still-live re-check passes.
        let orig_serial = commit_live_cert(dir.path(), "host.ryu-oh.org");
        // A verified renewal and an ordinary new request, both pending.
        let renew = admin_client::generate_key_and_csr("host.ryu-oh.org").unwrap();
        let fresh = admin_client::generate_key_and_csr("newcomer.ryu-oh.org").unwrap();
        let renew_req = ca_store::QueuedReq::new(
            NodeKind::Workstation,
            renew.csr_pem,
            "host.ryu-oh.org".to_string(),
            std::time::Duration::from_secs(30 * 86400),
            "test".to_string(),
            Some(orig_serial),
            None,
        );
        let fresh_req = ca_store::QueuedReq::new(
            NodeKind::Workstation,
            fresh.csr_pem,
            "newcomer.ryu-oh.org".to_string(),
            std::time::Duration::from_secs(30 * 86400),
            "test".to_string(),
            None,
            None,
        );
        let renew_id = renew_req.id.clone();
        let fresh_id = fresh_req.id.clone();
        {
            let cadir = ca_store::CaDir::open(dir.path()).unwrap();
            cadir.store.lock().enqueue(&renew_req).unwrap();
            cadir.store.lock().enqueue(&fresh_req).unwrap();
        }

        let approved = autorenew_sweep(&issuer(dir.path()), "renew-secret");
        assert_eq!(approved, 1, "only the verified renewal is auto-approved");
        assert!(
            matches!(
                ca_store::CaDir::open(dir.path())
                    .unwrap()
                    .store
                    .lock()
                    .status(&renew_id)
                    .unwrap(),
                ca_store::Status::Signed(_)
            ),
            "the verified renewal should now be signed",
        );
        assert!(
            matches!(
                ca_store::CaDir::open(dir.path())
                    .unwrap()
                    .store
                    .lock()
                    .status(&fresh_id)
                    .unwrap(),
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
        let (addr, state) = spawn_ca_server(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();

        // Approve first, then a deny is refused; only the signed sidecar exists.
        let a = admin_client::enqueue(
            addr,
            NodeKind::Workstation,
            "approved.ryu-oh.org",
            std::time::Duration::from_secs(30 * 86400),
            &identity,
        )
        .await
        .unwrap();
        admin_client::approve(addr, "alice", "apw", &a.request_id, vec![], &identity)
            .await
            .unwrap();
        let derr =
            admin_client::deny(addr, "alice", "apw", &a.request_id, "no", &identity)
                .await
                .unwrap_err();
        assert!(format!("{derr:#}").contains("already approved"), "got: {derr:#}");
        assert!(matches!(
            state.ca.as_ref().unwrap().store.lock().status(&a.request_id).unwrap(),
            ca_store::Status::Signed(_)
        ));

        // Deny first, then an approve is refused.
        let b = admin_client::enqueue(
            addr,
            NodeKind::Workstation,
            "denied.ryu-oh.org",
            std::time::Duration::from_secs(30 * 86400),
            &identity,
        )
        .await
        .unwrap();
        admin_client::deny(addr, "alice", "apw", &b.request_id, "nope", &identity)
            .await
            .unwrap();
        let aerr =
            admin_client::approve(addr, "alice", "apw", &b.request_id, vec![], &identity)
                .await
                .map(|_| ())
                .unwrap_err();
        assert!(format!("{aerr:#}").contains("already denied"), "got: {aerr:#}");
        assert!(matches!(
            state.ca.as_ref().unwrap().store.lock().status(&b.request_id).unwrap(),
            ca_store::Status::Denied(_)
        ));
    }

    /// A second admin server enrolls with no admin at its keyboard: the
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
            admin_client::fetch_identity(addr, NodeKind::AdminServer).await.unwrap();
        let listen: SocketAddr = "10.0.0.9:4565".parse().unwrap();
        let pending =
            admin_client::enqueue_enroll(addr, listen, &identity).await.unwrap();
        assert!(matches!(
            admin_client::poll(addr, NodeKind::AdminServer, &pending, &identity)
                .await
                .unwrap(),
            admin_client::PollOutcome::Pending
        ));
        // The admin's list shows what this really is — an enrollment at
        // a stated address, not a user cert — and the request code
        // matches the enrollee's.
        let queue =
            admin_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].requested_name, SERVING_SAN);
        assert_eq!(queue[0].enroll_listen, Some(listen));
        assert_eq!(
            admin_client::csr_fingerprint(&queue[0].csr_pem).unwrap(),
            pending.fingerprint,
        );
        admin_client::approve(addr, "alice", "apw", &queue[0].id, vec![], &identity)
            .await
            .unwrap();
        let issued =
            match admin_client::poll(addr, NodeKind::AdminServer, &pending, &identity)
                .await
                .unwrap()
            {
                admin_client::PollOutcome::Issued(i) => i,
                _ => panic!("expected Issued after approval"),
            };
        let cert = openssl::x509::X509::from_pem(issued.cert_pem.as_bytes()).unwrap();
        let san = cert.subject_alt_names().unwrap();
        assert!(san.iter().any(|n| n.dnsname() == Some(SERVING_SAN)));
        // The CA now knows the new admin server as a peer (the start of
        // future installs' peer walks).
        assert!(state.cfg.lock().peers.contains(&listen));
        let log = std::fs::read_to_string(dir.path().join("audit.log")).unwrap();
        assert!(log.contains("op=enroll"));
    }

    /// The `may_enroll_servers` gate binds to the *approving* admin: an
    /// admin without it can approve user certs all day but cannot mint
    /// a admin server; the entry stays pending for someone who can.
    #[tokio::test]
    async fn enrollment_approval_requires_the_policy_bit() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let restricted = Policy { may_enroll_servers: false, ..policy() };
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_signing_slot("apw", "bob", "bpw", restricted)
            .unwrap();
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::AdminServer).await.unwrap();
        let listen: SocketAddr = "10.0.0.10:4565".parse().unwrap();
        let pending =
            admin_client::enqueue_enroll(addr, listen, &identity).await.unwrap();
        let queue =
            admin_client::list_queue(addr, "bob", "bpw", &identity).await.unwrap();
        let err =
            admin_client::approve(addr, "bob", "bpw", &queue[0].id, vec![], &identity)
                .await
                .map(|_| ())
                .unwrap_err();
        assert!(format!("{err:#}").contains("may not enroll"));
        // Still pending — bob's failed approval consumed nothing.
        assert!(matches!(
            admin_client::poll(addr, NodeKind::AdminServer, &pending, &identity)
                .await
                .unwrap(),
            admin_client::PollOutcome::Pending
        ));
        admin_client::approve(addr, "alice", "apw", &queue[0].id, vec![], &identity)
            .await
            .unwrap();
        assert!(matches!(
            admin_client::poll(addr, NodeKind::AdminServer, &pending, &identity)
                .await
                .unwrap(),
            admin_client::PollOutcome::Issued(_)
        ));
    }

    #[tokio::test]
    async fn denied_requests_reach_the_enrollee() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        let pending = admin_client::enqueue(
            addr,
            NodeKind::Workstation,
            "mallory.ryu-oh.org",
            std::time::Duration::from_secs(30 * 86400),
            &identity,
        )
        .await
        .unwrap();
        let queue =
            admin_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
        admin_client::deny(
            addr,
            "alice",
            "apw",
            &queue[0].id,
            "request code mismatch",
            &identity,
        )
        .await
        .unwrap();
        match admin_client::poll(addr, NodeKind::Workstation, &pending, &identity)
            .await
            .unwrap()
        {
            admin_client::PollOutcome::Denied(reason) => {
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
            admin_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        let pending = admin_client::enqueue(
            addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            std::time::Duration::from_secs(30 * 86400),
            &identity,
        )
        .await
        .unwrap();
        // Wrong password: list and approve both refuse.
        let err = admin_client::list_queue(addr, "alice", "WRONG", &identity)
            .await
            .map(|_| ())
            .unwrap_err();
        assert!(format!("{err:#}").contains("authentication"));
        let err = admin_client::approve(
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
        let err = admin_client::approve(
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
            admin_client::poll(addr, NodeKind::Workstation, &pending, &identity)
                .await
                .unwrap(),
            admin_client::PollOutcome::Pending
        ));
        admin_client::approve(
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
            admin_client::poll(addr, NodeKind::Workstation, &pending, &identity)
                .await
                .unwrap(),
            admin_client::PollOutcome::Issued(_)
        ));
    }

    #[tokio::test]
    async fn enqueue_rejects_the_reserved_name() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::AdminServer).await.unwrap();
        let err = admin_client::enqueue(
            addr,
            NodeKind::AdminServer,
            SERVING_SAN,
            std::time::Duration::from_secs(30 * 86400),
            &identity,
        )
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
        assert!(matches!(handle_sign_request(&iss, &req).resp, SignResponse::Ok { .. }));
        // Same name again: refused while the first cert lives — the
        // impostor race on an existing identity is closed.
        let req2 = request("eric.ryu-oh.org", "alice", "apw", 30);
        match handle_sign_request(&iss, &req2).resp {
            SignResponse::Err { reason } => {
                assert!(reason.contains("already exists"), "got: {reason}")
            }
            SignResponse::Ok { .. } => panic!("duplicate name was signed"),
        }
        // Revoking clears the way — the rebuilt-laptop flow. Route through
        // the one `iss` CaDir the test already holds (a fresh `CaDir::open`
        // of the same dir would deadlock its flock).
        let serial = iss.store.lock().live_for_name("eric.ryu-oh.org").unwrap()[0].serial;
        iss.store
            .lock()
            .revoke(
                serial,
                ca_store::Revocation {
                    serial,
                    revoked_unix: ca_store::now_unix(),
                    reason: "laptop rebuilt".into(),
                },
            )
            .unwrap();
        let req3 = request("eric.ryu-oh.org", "alice", "apw", 30);
        assert!(matches!(handle_sign_request(&iss, &req3).resp, SignResponse::Ok { .. }));
    }

    #[tokio::test]
    async fn enqueue_refuses_a_live_name() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let req = request("eric.ryu-oh.org", "alice", "apw", 30);
        assert!(matches!(
            handle_sign_request(&issuer(dir.path()), &req).resp,
            SignResponse::Ok { .. }
        ));
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        let err = admin_client::enqueue(
            addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            std::time::Duration::from_secs(30 * 86400),
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
        let (addr, state) = spawn_ca_server(dir.path()).await;
        // The CA daemon holds the dir's exclusive flock; route every direct
        // store/sign op through its `state.ca` (a second `CaDir::open` would
        // deadlock). Each `lock()` here is a short, awaitless critical section
        // — never held across a wire call that the server itself must lock.
        let ca = state.ca.as_ref().expect("CA role held");
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        // Nothing revoked yet ⇒ no CRL.
        assert!(
            admin_client::get_crl(addr, NodeKind::Client, &identity)
                .await
                .unwrap()
                .is_none()
        );
        // Sign a victim, revoke it, sign the CRL with the vault key.
        let req = request("victim.ryu-oh.org", "alice", "apw", 30);
        assert!(matches!(handle_sign_request(ca, &req).resp, SignResponse::Ok { .. }));
        let serial =
            ca.store.lock().live_for_name("victim.ryu-oh.org").unwrap()[0].serial;
        ca.store
            .lock()
            .revoke(
                serial,
                ca_store::Revocation {
                    serial,
                    revoked_unix: ca_store::now_unix(),
                    reason: "test".into(),
                },
            )
            .unwrap();
        let unlocked =
            ca_vault::CAVault::new(dir.path().to_path_buf()).unlock("apw").unwrap();
        ca.store.lock().write_crl(&unlocked.ca_key_pem).unwrap();
        // The daemon serves it; it parses; the revoked serial is on it;
        // and it is genuinely signed by the CA.
        let pem = admin_client::get_crl(addr, NodeKind::Client, &identity)
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
        let nu = ca.store.lock().crl_next_update().unwrap().unwrap();
        let expect = ca_store::now_unix() + ca_store::CRL_VALIDITY.as_secs();
        assert!(nu.abs_diff(expect) < 3600, "nextUpdate {nu} vs expected {expect}");
    }

    #[tokio::test]
    async fn verified_renewal_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // An autorenew-shaped admin: empty issuance scope. It must be
        // able to approve *renewals* and nothing else.
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_signing_slot(
                "apw",
                "bot",
                "botpw",
                Policy {
                    allowed_san: vec![],
                    max_validity: std::time::Duration::from_secs(730 * 86400),
                    id_map_groups: vec![],
                    may_enroll_servers: false,
                    perms_edit_scopes: vec![],
                    may_manage_admins: false,
                    service_control_scopes: vec![],
                },
            )
            .unwrap();
        let (addr, state) = spawn_ca_server(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        // 1. Initial enrollment (the trust ceremony happened here).
        let issued = admin_client::request_cert(
            addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            std::time::Duration::from_secs(30 * 86400),
            vec![],
            &identity,
        )
        .await
        .unwrap();
        // 2. Renewal: enqueue over a connection authenticated by the
        //    live cert — fully unattended, PKI-verified, no TOFU.
        let mut roots = RootCertStore::empty();
        for der in rustls_pemfile::certs(&mut std::io::Cursor::new(
            issued.trusted_pem.as_bytes(),
        )) {
            roots.add(der.unwrap()).unwrap();
        }
        let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(
            issued.private_key_pem.as_bytes(),
        ))
        .unwrap()
        .unwrap();
        let pending = admin_client::enqueue_renewal(
            addr,
            NodeKind::Client,
            "eric.ryu-oh.org",
            std::time::Duration::from_secs(30 * 86400),
            issued.cert_pem.as_bytes(),
            key,
            roots.clone(),
        )
        .await
        .unwrap();
        // 3. The server marked it verified — proof of possession of the
        //    live key, checked against the issuance index.
        let q = admin_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
        assert_eq!(q.len(), 1);
        assert!(q[0].verified_renewal, "renewal must be marked verified");
        // 4. The empty-scope bot approves it — renewals skip the SAN
        //    globs (the name was approved at enrollment; this is
        //    continuation).
        admin_client::approve(
            addr,
            "bot",
            "botpw",
            &pending.request_id,
            vec![],
            &identity,
        )
        .await
        .unwrap();
        let renewed = match admin_client::poll_renewal(
            addr,
            NodeKind::Client,
            &pending,
            &issued.trusted_pem,
            roots.clone(),
        )
        .await
        .unwrap()
        {
            admin_client::PollOutcome::Issued(i) => i,
            _ => panic!("expected the renewed cert"),
        };
        assert_ne!(renewed.cert_pem, issued.cert_pem, "fresh cert (and fresh key)");
        let cert = openssl::x509::X509::from_pem(renewed.cert_pem.as_bytes()).unwrap();
        let san = cert.subject_alt_names().unwrap();
        assert!(san.iter().any(|n| n.dnsname() == Some("eric.ryu-oh.org")));
        // Both generations are live in the index until revoked/expired.
        assert_eq!(
            state
                .ca
                .as_ref()
                .unwrap()
                .store
                .lock()
                .live_for_name("eric.ryu-oh.org")
                .unwrap()
                .len(),
            2
        );
        // The audit trail distinguishes renewals.
        let log = std::fs::read_to_string(dir.path().join("audit.log")).unwrap();
        assert!(log.contains("op=renew"));
        // 5. The bot CANNOT approve a *new* identity: queue one without
        //    a client cert and watch the empty SAN scope refuse it.
        let new_req = admin_client::enqueue(
            addr,
            NodeKind::Workstation,
            "bob.ryu-oh.org",
            std::time::Duration::from_secs(30 * 86400),
            &identity,
        )
        .await
        .unwrap();
        let err = admin_client::approve(
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
            admin_client::fetch_identity(addr, NodeKind::AdminServer).await.unwrap();
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
        let pending = admin_client::enqueue_renewal(
            addr,
            NodeKind::AdminServer,
            SERVING_SAN,
            std::time::Duration::from_secs(365 * 86400),
            &state.serving_cert_pem,
            key,
            roots.clone(),
        )
        .await
        .unwrap();
        let q = admin_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
        assert!(q[0].verified_renewal, "serving-cert renewal must verify");
        admin_client::approve(
            addr,
            "alice",
            "apw",
            &pending.request_id,
            vec![],
            &identity,
        )
        .await
        .unwrap();
        let installed_pem = std::str::from_utf8(&ca_pem).unwrap();
        let renewed = match admin_client::poll_renewal(
            addr,
            NodeKind::AdminServer,
            &pending,
            installed_pem,
            roots,
        )
        .await
        .unwrap()
        {
            admin_client::PollOutcome::Issued(i) => i,
            _ => panic!("expected the renewed serving cert"),
        };
        // The chain rebuild: leaf + the issuing CA from the returned
        // bundle parses back as a ≥2-cert chain — what `split_chain`
        // on every future enrollee requires.
        let ca = admin_client::issuing_ca_pem(&renewed.trusted_pem, &renewed.cert_pem)
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
        let (addr, state) = spawn_ca_server(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Workstation).await.unwrap();
        let issued = admin_client::request_cert(
            addr,
            NodeKind::Workstation,
            "eric.ryu-oh.org",
            "alice",
            Zeroizing::new("apw".to_string()),
            std::time::Duration::from_secs(30 * 86400),
            vec![],
            &identity,
        )
        .await
        .unwrap();
        // Revoke it (e.g. the laptop was stolen). Route through the running
        // CA daemon's held `state.ca` (it owns the dir's exclusive flock).
        let serial = state
            .ca
            .as_ref()
            .unwrap()
            .store
            .lock()
            .live_for_name("eric.ryu-oh.org")
            .unwrap()[0]
            .serial;
        state
            .ca
            .as_ref()
            .unwrap()
            .store
            .lock()
            .revoke(
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
        for der in rustls_pemfile::certs(&mut std::io::Cursor::new(
            issued.trusted_pem.as_bytes(),
        )) {
            roots.add(der.unwrap()).unwrap();
        }
        let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(
            issued.private_key_pem.as_bytes(),
        ))
        .unwrap()
        .unwrap();
        admin_client::enqueue_renewal(
            addr,
            NodeKind::Client,
            "eric.ryu-oh.org",
            std::time::Duration::from_secs(30 * 86400),
            issued.cert_pem.as_bytes(),
            key,
            roots,
        )
        .await
        .unwrap();
        let q = admin_client::list_queue(addr, "alice", "apw", &identity).await.unwrap();
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

    /// A single anonymous resolver member on 127.0.0.1 — when this is the
    /// admin server's own member, single-host delegation pushes to no
    /// remote peer (the member is recognized as self).
    fn write_anon_resolver_cfg(dir: &Path, name: &str) -> PathBuf {
        use netidx::resolver_server::config::file as rfile;
        let member = rfile::MemberServerBuilder::default()
            .addr("127.0.0.1:4564".parse::<SocketAddr>().unwrap())
            .bind_addr("127.0.0.1".parse::<std::net::IpAddr>().unwrap())
            .auth(rfile::Auth::Anonymous)
            .build()
            .unwrap();
        let cfg =
            rfile::ConfigBuilder::default().member_servers(vec![member]).build().unwrap();
        let p = dir.join(name);
        std::fs::write(&p, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();
        p
    }

    /// Spawn a single-host parent: a admin server holding both ca +
    /// resolver roles over the anon resolver config at `dir/resolver.json`.
    /// Returns its address, the resolver config path, and the live state.
    async fn spawn_anon_parent(dir: &Path) -> (SocketAddr, PathBuf, Arc<Server>) {
        let rpath = write_anon_resolver_cfg(dir, "resolver.json");
        let roles = Roles {
            ca: Some(CaRole {
                dir: dir.to_path_buf(),
                autorenew: Some(autorenew_keytab(dir)),
            }),
            resolver: Some(ResolverRole { config: rpath.clone() }),
            id_map: None,
        };
        let (addr, state) = spawn_server_with(dir, roles, vec![]).await;
        (addr, rpath, state)
    }

    /// A resolver config: one anon member + a relative perms file.
    fn write_resolver_with_perms(dir: &Path) -> PathBuf {
        use netidx::resolver_server::config::file as rfile;
        let member = rfile::MemberServerBuilder::default()
            .addr("127.0.0.1:4564".parse::<SocketAddr>().unwrap())
            .bind_addr("127.0.0.1".parse::<std::net::IpAddr>().unwrap())
            .auth(rfile::Auth::Anonymous)
            .build()
            .unwrap();
        let cfg = rfile::ConfigBuilder::default()
            .member_servers(vec![member])
            .include_permissions(vec!["perms.json".into()])
            .build()
            .unwrap();
        let p = dir.join("resolver.json");
        std::fs::write(&p, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();
        p
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn perms_edit_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        std::fs::write(dir.path().join("perms.json"), r#"{"/":{"users":"swl"}}"#)
            .unwrap();
        let rpath = write_resolver_with_perms(dir.path());
        let roles = Roles {
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: Some(ResolverRole { config: rpath }),
            id_map: None,
        };
        let (addr, _state) = spawn_server_with(dir.path(), roles, vec![]).await;
        let id = admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();

        // Read the current perms (no credentials — readable in-domain).
        let p0 = admin_client::get_perms(addr, NodeKind::Client, &id).await.unwrap();
        assert!(p0.contains("users"), "got {p0}");

        // Edit the root cluster's perms (admin-authed at the CA, propagated
        // to the cluster — here a single self-member loopback push).
        let new_perms = r#"{"/foo":{"bob":"swl"}}"#;
        let peers = admin_client::edit_perms(
            addr,
            NodeKind::Client,
            &id,
            "alice",
            "apw",
            "/",
            new_perms,
        )
        .await
        .unwrap();
        assert!(peers.iter().all(|p| p.error.is_none()), "all peers applied: {peers:?}");

        // The edit is reflected.
        let p1 = admin_client::get_perms(addr, NodeKind::Client, &id).await.unwrap();
        assert!(p1.contains("bob") && !p1.contains("users"), "got {p1}");

        // Wrong password is refused (the file is unchanged).
        assert!(
            admin_client::edit_perms(
                addr,
                NodeKind::Client,
                &id,
                "alice",
                "wrong",
                "/",
                new_perms
            )
            .await
            .is_err()
        );
    }

    /// A role keyslot may edit perms only within its granted scope, and
    /// never unlocks the CA key. Two role admins on the root-cluster CA:
    /// `eve` scoped to `/eu` (does NOT cover the `/` cluster) and `rod`
    /// scoped to `/` (does). The CA authorizes by scope before touching
    /// the map.
    #[tokio::test(flavor = "multi_thread")]
    async fn role_keyslot_perms_scope_enforced() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        // Mint two role keyslots off the signing admin (apw).
        let role = |scope: &str| ca_vault::Policy {
            allowed_san: vec![],
            max_validity: std::time::Duration::from_secs(0 * 86400),
            id_map_groups: vec![],
            may_enroll_servers: false,
            perms_edit_scopes: vec![scope.to_string()],
            may_manage_admins: false,
            service_control_scopes: vec![],
        };
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot("eve", "epw", role("/eu"))
            .unwrap();
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot("rod", "rpw", role("/"))
            .unwrap();

        std::fs::write(dir.path().join("perms.json"), r#"{"/":{"users":"swl"}}"#)
            .unwrap();
        let rpath = write_resolver_with_perms(dir.path());
        let roles = Roles {
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: Some(ResolverRole { config: rpath }),
            id_map: None,
        };
        let (addr, _state) = spawn_server_with(dir.path(), roles, vec![]).await;
        let id = admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        let new_perms = r#"{"/foo":{"bob":"swl"}}"#;

        // eve's scope `/eu` does not cover the `/` cluster — refused on
        // authorization, before the map is even consulted, and the file
        // is untouched.
        let denied = admin_client::edit_perms(
            addr,
            NodeKind::Client,
            &id,
            "eve",
            "epw",
            "/",
            new_perms,
        )
        .await;
        let msg = format!("{:#}", denied.unwrap_err());
        assert!(msg.contains("not authorized"), "got {msg}");
        let p = admin_client::get_perms(addr, NodeKind::Client, &id).await.unwrap();
        assert!(p.contains("users") && !p.contains("bob"), "deny must not mutate: {p}");

        // A wrong password for a real role admin is also refused.
        assert!(
            admin_client::edit_perms(
                addr,
                NodeKind::Client,
                &id,
                "eve",
                "nope",
                "/eu",
                new_perms
            )
            .await
            .is_err()
        );

        // rod's scope `/` covers the root cluster — authorized, and the
        // edit propagates (a role admin edits perms with no CA key).
        let peers = admin_client::edit_perms(
            addr,
            NodeKind::Client,
            &id,
            "rod",
            "rpw",
            "/",
            new_perms,
        )
        .await
        .unwrap();
        assert!(peers.iter().all(|p| p.error.is_none()), "all peers applied: {peers:?}");
        let p = admin_client::get_perms(addr, NodeKind::Client, &id).await.unwrap();
        assert!(p.contains("bob") && !p.contains("users"), "in-scope edit applied: {p}");
    }

    /// An edit that parses as JSON but makes the resolver config invalid is
    /// rejected by the receiving peer and rolled back — the prior perms
    /// survive intact.
    #[tokio::test(flavor = "multi_thread")]
    async fn invalid_perms_edit_is_rejected_and_reverted() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        std::fs::write(dir.path().join("perms.json"), r#"{"/":{"users":"swl"}}"#)
            .unwrap();
        let rpath = write_resolver_with_perms(dir.path());
        let roles = Roles {
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: Some(ResolverRole { config: rpath }),
            id_map: None,
        };
        let (addr, _state) = spawn_server_with(dir.path(), roles, vec![]).await;
        let id = admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();

        // Valid JSON, but `xyz` are not permission bits (only `!swlpd`).
        // The peer writes it, validation fails, and it rolls back.
        let bad = r#"{"/foo":{"bob":"xyz"}}"#;
        let peers = admin_client::edit_perms(
            addr,
            NodeKind::Client,
            &id,
            "alice",
            "apw",
            "/",
            bad,
        )
        .await
        .unwrap();
        assert!(
            peers.iter().any(|p| p.error.is_some()),
            "peer must reject the invalid perms: {peers:?}"
        );

        // The prior perms survive — the bad edit was reverted.
        let p = admin_client::get_perms(addr, NodeKind::Client, &id).await.unwrap();
        assert!(
            p.contains("users") && !p.contains("bob"),
            "reverted to prior perms: {p}"
        );
    }

    /// End-to-end delegation over the real pinned-TLS protocol: a admin
    /// server wearing both ca + resolver hats hosts a parent
    /// resolver.json (root `/`, one anon member, no children). A child
    /// requests `/eu`, the admin lists then approves, and we assert the
    /// parent config gained `children[/eu]` and the child's poll returns
    /// `Approved{parent}` carrying the parent's own resolver address.
    #[tokio::test(flavor = "multi_thread")]
    async fn delegation_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, rpath, _state) = spawn_anon_parent(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();

        // The child site queues a request for /eu carrying its own
        // (concrete) resolver address.
        let child = vec![ResolverAddr {
            addr: "203.0.113.9:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        }];
        let request_id =
            admin_client::request_delegation(addr, "/eu", child.clone(), &identity)
                .await
                .unwrap();
        assert!(matches!(
            admin_client::poll_delegation(addr, &request_id, &identity).await.unwrap(),
            DelegationPollResponse::Pending
        ));

        // The parent admin lists the queue; the code it computes locally
        // matches the child's (same canonical (path, child) bytes).
        let queue = admin_client::list_delegations(addr, "alice", "apw", &identity)
            .await
            .unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].proposed_path, "/eu");
        assert_eq!(
            admin_client::delegation_code(&queue[0].proposed_path, &queue[0].child),
            admin_client::delegation_code("/eu", &child)
        );

        // Approve. Single-member cluster ⇒ no remote peer push.
        let peers = admin_client::approve_delegation(
            addr,
            "alice",
            "apw",
            &request_id,
            &identity,
        )
        .await
        .unwrap();
        assert!(peers.is_empty(), "single-member cluster should push to no peers");

        // The parent resolver.json now mounts the child at /eu.
        let rc = crate::resolver::ResolverConfig::load(&rpath).unwrap();
        assert_eq!(rc.as_file().children.len(), 1);
        let ch = &rc.as_file().children[0];
        assert_eq!(&*ch.path, "/eu");
        assert_eq!(ch.addrs.len(), 1);
        assert_eq!(ch.addrs[0].0, "203.0.113.9:4564".parse::<SocketAddr>().unwrap());

        // The child's poll flips to Approved, carrying the parent's own
        // resolver address for its `parent` referral.
        match admin_client::poll_delegation(addr, &request_id, &identity).await.unwrap() {
            DelegationPollResponse::Approved { parent } => {
                assert_eq!(parent.len(), 1);
                assert_eq!(
                    parent[0].addr,
                    "127.0.0.1:4564".parse::<SocketAddr>().unwrap()
                );
                assert_eq!(parent[0].auth, InfoAuth::Anonymous);
            }
            other => panic!("expected Approved, got {other:?}"),
        }

        // Re-approving an already-approved request is an idempotent
        // re-sync — still Ok, children unchanged (no duplicate mount).
        let peers = admin_client::approve_delegation(
            addr,
            "alice",
            "apw",
            &request_id,
            &identity,
        )
        .await
        .unwrap();
        assert!(peers.is_empty());
        let rc = crate::resolver::ResolverConfig::load(&rpath).unwrap();
        assert_eq!(
            rc.as_file().children.len(),
            1,
            "re-approve must not duplicate the child"
        );
    }

    /// Two anonymous resolver members on distinct loopback IPs (same
    /// resolver port). Each peer keeps its own copy of this config; a
    /// delegation must reach both. Linux-only: the cluster push derives a
    /// peer's admin address from its member IP, so the peers must live on
    /// separate loopback IPs (127.0.0.0/8, all loopback on Linux).
    #[cfg(target_os = "linux")]
    fn write_cluster_cfg(ca_dir: &Path, name: &str) -> PathBuf {
        use netidx::resolver_server::config::file as rfile;
        let member = |ip: &str| {
            rfile::MemberServerBuilder::default()
                .addr(SocketAddr::new(ip.parse().unwrap(), 4564))
                .bind_addr(ip.parse::<std::net::IpAddr>().unwrap())
                .auth(rfile::Auth::Anonymous)
                .build()
                .unwrap()
        };
        let cfg = rfile::ConfigBuilder::default()
            .member_servers(vec![member("127.0.0.2"), member("127.0.0.3")])
            .build()
            .unwrap();
        let p = ca_dir.join(name);
        std::fs::write(&p, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();
        p
    }

    /// A 2-peer parent cluster: approving a delegation on peer A must
    /// push the `AddChild` edit to peer B so BOTH members mount the
    /// child — the cluster stays consistent under one admin action.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn delegation_cluster_propagation() {
        let dir = tempfile::tempdir().unwrap();
        let ca_dir = dir.path();
        setup_ca(ca_dir);
        let ra = write_cluster_cfg(ca_dir, "resolver_a.json");
        let rb = write_cluster_cfg(ca_dir, "resolver_b.json");

        // Peer A holds the CA (it approves); peer B only receives pushes.
        let roles_a = Roles {
            ca: Some(CaRole {
                dir: ca_dir.to_path_buf(),
                autorenew: Some(autorenew_keytab(ca_dir)),
            }),
            resolver: Some(ResolverRole { config: ra.clone() }),
            id_map: None,
        };
        let roles_b = Roles {
            ca: None,
            resolver: Some(ResolverRole { config: rb.clone() }),
            id_map: None,
        };
        let (addr_a, a) = spawn_server_at(ca_dir, "127.0.0.2:0", roles_a, vec![]).await;
        // B shares A's admin port on the other loopback IP — the cluster
        // push derives a peer's admin address as member.ip : my_admin_port.
        // A (the CA) already holds the dir's exclusive flock, so B can't open
        // it to mint its own serving cert; it reuses A's (every admin server
        // shares the reserved serving SAN, so A's cert is a valid identity
        // for B too).
        let port = addr_a.port();
        let (addr_b, _b) = spawn_server_at_with_cert(
            ca_dir,
            &format!("127.0.0.3:{port}"),
            roles_b,
            vec![],
            a.serving_cert_pem.clone(),
            a.serving_key_pem.clone(),
        )
        .await;

        let identity =
            admin_client::fetch_identity(addr_a, NodeKind::Client).await.unwrap();
        let child = vec![ResolverAddr {
            addr: "203.0.113.9:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        }];
        let request_id =
            admin_client::request_delegation(addr_a, "/eu", child, &identity)
                .await
                .unwrap();
        let peers = admin_client::approve_delegation(
            addr_a,
            "alice",
            "apw",
            &request_id,
            &identity,
        )
        .await
        .unwrap();
        // Exactly one remote peer (B), pushed cleanly.
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].addr, addr_b);
        assert!(peers[0].error.is_none(), "peer B push failed: {:?}", peers[0].error);
        // BOTH cluster members now mount the child at /eu.
        for p in [&ra, &rb] {
            let rc = crate::resolver::ResolverConfig::load(p).unwrap();
            assert_eq!(rc.as_file().children.len(), 1, "{p:?} missing the child");
            assert_eq!(&*rc.as_file().children[0].path, "/eu");
        }
    }

    /// A cluster peer whose admin server is down must be reported LOUDLY
    /// (the cluster is now inconsistent), and a re-approve once it
    /// recovers must re-sync it idempotently.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn delegation_cluster_down_peer_then_resync() {
        let dir = tempfile::tempdir().unwrap();
        let ca_dir = dir.path();
        setup_ca(ca_dir);
        let ra = write_cluster_cfg(ca_dir, "resolver_a.json");
        let rb = write_cluster_cfg(ca_dir, "resolver_b.json");

        let roles_a = Roles {
            ca: Some(CaRole {
                dir: ca_dir.to_path_buf(),
                autorenew: Some(autorenew_keytab(ca_dir)),
            }),
            resolver: Some(ResolverRole { config: ra.clone() }),
            id_map: None,
        };
        let (addr_a, a) = spawn_server_at(ca_dir, "127.0.0.2:0", roles_a, vec![]).await;
        let port = addr_a.port();

        let identity =
            admin_client::fetch_identity(addr_a, NodeKind::Client).await.unwrap();
        let child = vec![ResolverAddr {
            addr: "203.0.113.9:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        }];
        let request_id =
            admin_client::request_delegation(addr_a, "/eu", child, &identity)
                .await
                .unwrap();

        // Peer B is down — approve still commits locally on A, but the
        // push to B is reported as a failure (the cluster is now split).
        let peers = admin_client::approve_delegation(
            addr_a,
            "alice",
            "apw",
            &request_id,
            &identity,
        )
        .await
        .unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].addr.port(), port);
        assert!(peers[0].error.is_some(), "a down peer must be reported as failed");
        // A applied locally; B's config is untouched.
        let rc_a = crate::resolver::ResolverConfig::load(&ra).unwrap();
        assert_eq!(rc_a.as_file().children.len(), 1);
        let rc_b = crate::resolver::ResolverConfig::load(&rb).unwrap();
        assert!(rc_b.as_file().children.is_empty(), "down peer must not be edited");

        // B recovers; re-approving the (already-approved) request is an
        // idempotent re-sync that converges the cluster.
        let roles_b = Roles {
            ca: None,
            resolver: Some(ResolverRole { config: rb.clone() }),
            id_map: None,
        };
        // A (the CA) holds the dir's exclusive flock; B reuses A's serving
        // cert (shared reserved SAN) rather than opening the dir to mint one.
        let (_addr_b, _b) = spawn_server_at_with_cert(
            ca_dir,
            &format!("127.0.0.3:{port}"),
            roles_b,
            vec![],
            a.serving_cert_pem.clone(),
            a.serving_key_pem.clone(),
        )
        .await;
        let peers = admin_client::approve_delegation(
            addr_a,
            "alice",
            "apw",
            &request_id,
            &identity,
        )
        .await
        .unwrap();
        assert_eq!(peers.len(), 1);
        assert!(peers[0].error.is_none(), "re-sync push failed: {:?}", peers[0].error);
        let rc_b = crate::resolver::ResolverConfig::load(&rb).unwrap();
        assert_eq!(rc_b.as_file().children.len(), 1, "re-sync must mount the child on B");
        assert_eq!(&*rc_b.as_file().children[0].path, "/eu");
        // And A stays single-mounted (idempotent, no duplicate).
        let rc_a = crate::resolver::ResolverConfig::load(&ra).unwrap();
        assert_eq!(rc_a.as_file().children.len(), 1);
    }

    /// The child-cluster direction: `push_referral_edit_to_peers` with a
    /// `SetParent` edit (what `add-parent` runs after writing the local
    /// referral) reaches the other child member's admin server and sets its
    /// `parent` referral, while self is excluded.
    #[cfg(target_os = "linux")]
    #[tokio::test(flavor = "multi_thread")]
    async fn set_parent_pushes_to_child_cluster_peers() {
        let dir = tempfile::tempdir().unwrap();
        let ca_dir = dir.path();
        setup_ca(ca_dir);
        let ra = write_cluster_cfg(ca_dir, "child_a.json");
        let rb = write_cluster_cfg(ca_dir, "child_b.json");
        // A holds the admin identity used to push; B only receives.
        let roles = |cfg: PathBuf| Roles {
            ca: None,
            resolver: Some(ResolverRole { config: cfg }),
            id_map: None,
        };
        let (addr_a, state_a) =
            spawn_server_at(ca_dir, "127.0.0.2:0", roles(ra.clone()), vec![]).await;
        let port = addr_a.port();
        let (_addr_b, _b) = spawn_server_at(
            ca_dir,
            &format!("127.0.0.3:{port}"),
            roles(rb.clone()),
            vec![],
        )
        .await;

        let member_addrs = vec![
            "127.0.0.2:4564".parse::<SocketAddr>().unwrap(),
            "127.0.0.3:4564".parse::<SocketAddr>().unwrap(),
        ];
        let edit = ReferralEdit::SetParent {
            path: "/eu".to_string(),
            parent: vec![ResolverAddr {
                addr: "203.0.113.1:4564".parse().unwrap(),
                auth: InfoAuth::Anonymous,
            }],
        };
        let peers = push_referral_edit_to_peers(
            &edit,
            &member_addrs,
            addr_a,
            &state_a.serving_cert_pem,
            &state_a.serving_key_pem,
            state_a.roots.clone(),
        )
        .await;
        // Exactly one remote peer (B), pushed cleanly; A is self-excluded.
        assert_eq!(peers.len(), 1);
        assert!(peers[0].error.is_none(), "push to B failed: {:?}", peers[0].error);
        let rc_b = crate::resolver::ResolverConfig::load(&rb).unwrap();
        let par =
            rc_b.as_file().parent.as_ref().expect("B should have a parent referral");
        assert_eq!(&*par.path, "/eu");
        assert_eq!(par.addrs[0].0, "203.0.113.1:4564".parse::<SocketAddr>().unwrap());
        let rc_a = crate::resolver::ResolverConfig::load(&ra).unwrap();
        assert!(
            rc_a.as_file().parent.is_none(),
            "A is self-excluded; its config must be untouched"
        );
    }

    /// Approving requires BOTH ca (admin auth) and resolver (the config to
    /// edit) roles. A ca-only host can queue a request but must refuse to
    /// approve it — it has no resolver config to mount the child into.
    #[tokio::test(flavor = "multi_thread")]
    async fn delegation_approve_requires_resolver_role() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let roles = Roles {
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: None,
            id_map: None,
        };
        let (addr, _state) = spawn_server_with(dir.path(), roles, vec![]).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        let child = vec![ResolverAddr {
            addr: "203.0.113.9:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        }];
        let request_id = admin_client::request_delegation(addr, "/eu", child, &identity)
            .await
            .unwrap();
        let err = admin_client::approve_delegation(
            addr,
            "alice",
            "apw",
            &request_id,
            &identity,
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("resolver role"), "got: {err:#}");
    }

    /// A subtree that overlaps an already-mounted child is rejected at
    /// approve time by the children-constraint validation — the cluster
    /// never commits an ambiguous routing table, and nothing changes.
    #[tokio::test(flavor = "multi_thread")]
    async fn delegation_overlapping_subtree_rejected() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, rpath, _state) = spawn_anon_parent(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        // First child mounts /eu.
        let a = vec![ResolverAddr {
            addr: "203.0.113.9:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        }];
        let id_a =
            admin_client::request_delegation(addr, "/eu", a, &identity).await.unwrap();
        admin_client::approve_delegation(addr, "alice", "apw", &id_a, &identity)
            .await
            .unwrap();
        // A second site asks for /eu/sub — inside the first child's
        // subtree. Overlapping mounts are invalid; approve must refuse.
        let b = vec![ResolverAddr {
            addr: "203.0.113.10:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        }];
        let id_b = admin_client::request_delegation(addr, "/eu/sub", b, &identity)
            .await
            .unwrap();
        let err =
            admin_client::approve_delegation(addr, "alice", "apw", &id_b, &identity)
                .await
                .unwrap_err();
        assert!(format!("{err:#}").to_lowercase().contains("below"), "got: {err:#}");
        // The parent's mount table is unchanged: still just /eu.
        let rc = crate::resolver::ResolverConfig::load(&rpath).unwrap();
        assert_eq!(rc.as_file().children.len(), 1);
        assert_eq!(&*rc.as_file().children[0].path, "/eu");
    }

    /// Deny path + status precedence: a denied request polls `Denied`,
    /// and a later approve of the same request is refused (a terminal
    /// decision is final).
    #[tokio::test(flavor = "multi_thread")]
    async fn delegation_deny_then_approve_refused() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (addr, _rpath, _state) = spawn_anon_parent(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        let child = vec![ResolverAddr {
            addr: "203.0.113.9:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        }];
        let request_id = admin_client::request_delegation(addr, "/eu", child, &identity)
            .await
            .unwrap();
        admin_client::deny_delegation(
            addr,
            "alice",
            "apw",
            &request_id,
            "not your subtree",
            &identity,
        )
        .await
        .unwrap();
        match admin_client::poll_delegation(addr, &request_id, &identity).await.unwrap() {
            DelegationPollResponse::Denied { reason } => {
                assert_eq!(reason, "not your subtree")
            }
            other => panic!("expected Denied, got {other:?}"),
        }
        let err = admin_client::approve_delegation(
            addr,
            "alice",
            "apw",
            &request_id,
            &identity,
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("denied"), "got: {err:#}");
    }

    /// The server-to-server `SetParent` receive path (used when a child
    /// is itself a cluster) sets the `parent` referral and is idempotent.
    #[test]
    fn apply_referral_edit_set_parent_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_anon_resolver_cfg(dir.path(), "child.json");
        let edit = ReferralEdit::SetParent {
            path: "/eu".to_string(),
            parent: vec![ResolverAddr {
                addr: "203.0.113.1:4564".parse().unwrap(),
                auth: InfoAuth::Anonymous,
            }],
        };
        apply_referral_edit_local(&p, &edit).unwrap();
        let rc = crate::resolver::ResolverConfig::load(&p).unwrap();
        let par = rc.as_file().parent.as_ref().expect("parent referral set");
        assert_eq!(&*par.path, "/eu");
        assert_eq!(par.addrs.len(), 1);
        assert_eq!(par.addrs[0].0, "203.0.113.1:4564".parse::<SocketAddr>().unwrap());
        // Re-applying the same edit is a no-op, not an error.
        apply_referral_edit_local(&p, &edit).unwrap();
        let rc = crate::resolver::ResolverConfig::load(&p).unwrap();
        assert_eq!(&*rc.as_file().parent.as_ref().unwrap().path, "/eu");
    }

    // -- Phase 4: remote admin management ------------------------------------

    /// The glob-subset check is SOUND: it never reports coverage that doesn't
    /// hold (which would be an escalation), and it handles the realistic
    /// delegation patterns. Exotic patterns it can't reason about are
    /// conservatively rejected (fail closed).
    #[test]
    fn glob_covers_is_sound() {
        // `*` covers everything.
        assert!(glob_covers("*", "anything.example.com"));
        assert!(glob_covers("*", "*.eu.example.com"));
        // `*.suffix` covers sub-delegations and literals under it…
        assert!(glob_covers("*.ryu-oh.org", "*.eu.ryu-oh.org"));
        assert!(glob_covers("*.ryu-oh.org", "host.eu.ryu-oh.org"));
        assert!(glob_covers("*.ryu-oh.org", "*.ryu-oh.org")); // identical
        // …but NOT a different suffix, a broader pattern, the bare suffix, a
        // dash-boundary near-match, or a deeper-domain trick.
        assert!(!glob_covers("*.ryu-oh.org", "*.us.example.com"));
        assert!(!glob_covers("*.ryu-oh.org", "*"));
        assert!(!glob_covers("*.ryu-oh.org", "ryu-oh.org"));
        assert!(!glob_covers("*.ryu-oh.org", "evil-ryu-oh.org"));
        assert!(!glob_covers("*.ryu-oh.org", "*.ryu-oh.org.attacker.com"));
        // A literal only covers itself.
        assert!(glob_covers("host.example.com", "host.example.com"));
        assert!(!glob_covers("host.example.com", "*.example.com"));
        // Exotic caller patterns are conservatively rejected even when a true
        // subset (fail closed — grant those on-box).
        assert!(!glob_covers("a*c", "abc"));
        assert!(!glob_covers("a?c", "abc"));
    }

    fn full_policy(
        sans: &[&str],
        days: u32,
        groups: &[&str],
        enroll: bool,
        scopes: &[&str],
        manage: bool,
    ) -> Policy {
        Policy {
            allowed_san: sans.iter().map(|s| s.to_string()).collect(),
            max_validity: std::time::Duration::from_secs(days as u64 * 86400),
            id_map_groups: groups.iter().map(|s| s.to_string()).collect(),
            may_enroll_servers: enroll,
            perms_edit_scopes: scopes.iter().map(|s| s.to_string()).collect(),
            may_manage_admins: manage,
            service_control_scopes: vec![],
        }
    }

    /// `policy_within` enforces the no-escalation rule on every field.
    #[test]
    fn policy_within_enforces_every_field() {
        let caller = full_policy(&["*.ryu-oh.org"], 30, &["users"], true, &["/eu"], true);
        // A strict subset is allowed.
        assert!(
            policy_within(
                &caller,
                &full_policy(
                    &["*.eu.ryu-oh.org"],
                    30,
                    &["users"],
                    false,
                    &["/eu"],
                    false
                )
            )
            .is_ok()
        );
        // The empty policy is a subset of anything.
        assert!(
            policy_within(&caller, &full_policy(&[], 0, &[], false, &[], false)).is_ok()
        );
        // Each field, widened past the caller, is refused:
        let bad = |p: Policy, needle: &str| {
            let e = policy_within(&caller, &p).unwrap_err();
            assert!(e.contains(needle), "got: {e}");
        };
        bad(
            full_policy(&["*.us.example.com"], 30, &[], false, &[], false),
            "issuance scope",
        );
        bad(full_policy(&[], 31, &[], false, &[], false), "max_validity");
        bad(full_policy(&[], 30, &["admins"], false, &[], false), "id-map group");
        bad(full_policy(&[], 30, &[], false, &["/us"], false), "perms scope");
        // Booleans only when the caller has them.
        let no_enroll = full_policy(&["*.ryu-oh.org"], 30, &[], false, &[], true);
        assert!(
            policy_within(&no_enroll, &full_policy(&[], 30, &[], true, &[], false))
                .unwrap_err()
                .contains("may_enroll_servers")
        );
        let no_manage = full_policy(&["*.ryu-oh.org"], 30, &[], false, &[], false);
        assert!(
            policy_within(&no_manage, &full_policy(&[], 30, &[], false, &[], true))
                .unwrap_err()
                .contains("may_manage_admins")
        );
        // service-control scope is bounded the same way as perms scope.
        let base = full_policy(&[], 0, &[], false, &[], false);
        let caller_svc =
            Policy { service_control_scopes: vec!["/eu".to_string()], ..base.clone() };
        let too_wide =
            Policy { service_control_scopes: vec!["/us".to_string()], ..base.clone() };
        assert!(
            policy_within(&caller_svc, &too_wide)
                .unwrap_err()
                .contains("service-control scope")
        );
        let in_scope =
            Policy { service_control_scopes: vec!["/eu/west".to_string()], ..base };
        assert!(policy_within(&caller_svc, &in_scope).is_ok());
    }

    /// Service control is gated by `service_control_scopes` (path-scoped),
    /// with a signing slot as the founding authority — independent of perms
    /// or admin-management authority.
    #[test]
    fn service_control_authority_is_path_scoped() {
        let base = full_policy(&[], 0, &[], false, &[], false);
        // A signing slot can control services anywhere.
        let signing = ca_vault::Authenticated {
            admin: "recovery".to_string(),
            policy: base.clone(),
            kind: ca_vault::SlotKind::Signing,
        };
        assert!(service_control_authority(&signing, "/anything"));
        // A role admin needs a covering service_control_scope — and perms
        // scope alone does NOT confer it (distinct authority).
        let role = ca_vault::Authenticated {
            admin: "eu-ops".to_string(),
            policy: Policy {
                service_control_scopes: vec!["/eu".to_string()],
                perms_edit_scopes: vec!["/us".to_string()],
                ..base.clone()
            },
            kind: ca_vault::SlotKind::Role,
        };
        assert!(service_control_authority(&role, "/eu"));
        assert!(service_control_authority(&role, "/eu/west"));
        assert!(!service_control_authority(&role, "/us")); // perms scope ≠ service scope
        assert!(!service_control_authority(&role, "/"));
        // No service scope ⇒ no authority.
        let plain = ca_vault::Authenticated {
            admin: "p".to_string(),
            policy: base,
            kind: ca_vault::SlotKind::Role,
        };
        assert!(!service_control_authority(&plain, "/eu"));
    }

    /// The remote admin-mgmt handlers gate on management authority, enforce
    /// no-escalation, refuse the reserved signing slots, and won't touch a
    /// signing slot — while a signing caller (the founding authority) bypasses
    /// the subset check.
    #[tokio::test]
    async fn remote_admin_mgmt_authz() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path()); // alice = signing slot (founding authority)
        // A role admin that can manage admins, scoped to *.ryu-oh.org and "/".
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot(
                "boss",
                "bosspw",
                full_policy(&["*.ryu-oh.org"], 30, &["users"], true, &["/"], true),
            )
            .unwrap();
        // A plain role admin with no management authority.
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot(
                "plain",
                "plainpw",
                full_policy(&[], 0, &[], false, &["/eu"], false),
            )
            .unwrap();
        let (_addr, state) = spawn_ca_server(dir.path()).await;

        let in_scope =
            full_policy(&["*.eu.ryu-oh.org"], 30, &["users"], false, &["/eu"], false);
        let add =
            |admin: &str, pw: &str, name: &str, policy: Policy| AddRoleAdminRequest {
                admin: admin.to_string(),
                password: Secret(pw.to_string()),
                name: name.to_string(),
                new_password: Secret("np".to_string()),
                policy,
            };

        // boss mints an in-scope sub-role.
        assert!(matches!(
            handle_add_role_admin(
                &state,
                &add("boss", "bosspw", "eu-ops", in_scope.clone()),
                false
            ),
            AdminMgmtResponse::Ok
        ));
        assert_eq!(
            ca_vault::CAVault::new(dir.path().to_path_buf())
                .slot_policy("eu-ops")
                .unwrap()
                .0,
            ca_vault::SlotKind::Role
        );
        // Out of scope is refused.
        assert!(matches!(
            handle_add_role_admin(
                &state,
                &add("boss", "bosspw", "us-ops",
                     full_policy(&["*.us.example.com"], 30, &[], false, &[], false)),
                false,
            ),
            AdminMgmtResponse::Err { reason } if reason.contains("not within")
        ));
        // A reserved name is refused.
        assert!(matches!(
            handle_add_role_admin(&state, &add("boss", "bosspw", "recovery", in_scope.clone()), false),
            AdminMgmtResponse::Err { reason } if reason.contains("reserved")
        ));
        // A non-managing role admin can't manage.
        assert!(matches!(
            handle_add_role_admin(&state, &add("plain", "plainpw", "nope", in_scope.clone()), false),
            AdminMgmtResponse::Err { reason } if reason.contains("not authorized")
        ));
        // A role that lacks may_enroll_servers can't grant it (no escalation).
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot(
                "eu-boss",
                "ebpw",
                full_policy(&["*.eu.ryu-oh.org"], 30, &[], false, &["/eu"], true),
            )
            .unwrap();
        assert!(matches!(
            handle_add_role_admin(
                &state,
                &add("eu-boss", "ebpw", "x",
                     full_policy(&["*.eu.ryu-oh.org"], 30, &[], true, &[], false)),
                false,
            ),
            AdminMgmtResponse::Err { reason } if reason.contains("may_enroll_servers")
        ));
        // A SIGNING slot (alice) bypasses the subset check — founding authority.
        assert!(matches!(
            handle_add_role_admin(
                &state,
                &add(
                    "alice",
                    "apw",
                    "broadrole",
                    full_policy(&["*"], 9999, &["anything"], true, &["/"], true)
                ),
                false,
            ),
            AdminMgmtResponse::Ok
        ));

        // set-policy / remove never touch a signing slot, even by name.
        assert!(matches!(
            handle_set_admin_policy(
                &state,
                &SetAdminPolicyRequest {
                    admin: "boss".to_string(),
                    password: Secret("bosspw".to_string()),
                    target: "alice".to_string(),
                    policy: in_scope.clone(),
                },
                false,
            ),
            AdminMgmtResponse::Err { reason } if reason.contains("role admins only")
        ));
        assert!(matches!(
            handle_remove_admin(
                &state,
                &RemoveAdminRequest {
                    admin: "boss".to_string(),
                    password: Secret("bosspw".to_string()),
                    target: "autorenew".to_string(),
                },
                false,
            ),
            AdminMgmtResponse::Err { reason } if reason.contains("signing slot")
        ));
        // boss removes a plain (non-manager) role admin: fine.
        assert!(matches!(
            handle_remove_admin(
                &state,
                &RemoveAdminRequest {
                    admin: "boss".to_string(),
                    password: Secret("bosspw".to_string()),
                    target: "eu-ops".to_string(),
                },
                false,
            ),
            AdminMgmtResponse::Ok
        ));
        // list is gated: plain can't, boss can.
        assert!(matches!(
            handle_list_admins(
                &state,
                &ListAdminsRequest {
                    admin: "plain".to_string(),
                    password: Secret("plainpw".to_string()),
                },
                false,
            ),
            AdminListResponse::Err { .. }
        ));
        match handle_list_admins(
            &state,
            &ListAdminsRequest {
                admin: "boss".to_string(),
                password: Secret("bosspw".to_string()),
            },
            false,
        ) {
            AdminListResponse::Ok { admins } => {
                assert!(admins.iter().any(|a| a.admin == "boss"));
                // The signing slots are visible to a manager (informative).
                assert!(admins.iter().any(|a| a.admin == AUTORENEW_ADMIN));
            }
            AdminListResponse::Err { reason } => panic!("{reason}"),
        }
    }

    /// Admin management can't be stranded: the last role admin that can manage
    /// admins cannot be removed or demoted remotely (the off-box recovery
    /// credential remains the backstop, but we don't force a safe-trip).
    #[tokio::test]
    async fn remote_admin_cannot_strand_management() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let boss = full_policy(&["*.ryu-oh.org"], 30, &[], true, &["/"], true);
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot("boss", "bosspw", boss.clone())
            .unwrap();
        let (_addr, state) = spawn_ca_server(dir.path()).await;

        // boss is the only ROLE manager — it can't remove itself…
        assert!(matches!(
            handle_remove_admin(
                &state,
                &RemoveAdminRequest {
                    admin: "boss".to_string(),
                    password: Secret("bosspw".to_string()),
                    target: "boss".to_string(),
                },
                false,
            ),
            AdminMgmtResponse::Err { reason } if reason.contains("last role admin")
        ));
        // …nor demote itself out of may_manage_admins.
        let demoted = Policy { may_manage_admins: false, ..boss };
        assert!(matches!(
            handle_set_admin_policy(
                &state,
                &SetAdminPolicyRequest {
                    admin: "boss".to_string(),
                    password: Secret("bosspw".to_string()),
                    target: "boss".to_string(),
                    policy: demoted,
                },
                false,
            ),
            AdminMgmtResponse::Err { reason } if reason.contains("last role admin")
        ));
    }

    /// End to end over TLS: a managing role admin mints a sub-role through the
    /// pinned admin plane, and it shows up in the wire `list`.
    #[tokio::test]
    async fn remote_add_role_admin_over_tls() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        ca_vault::CAVault::new(dir.path().to_path_buf())
            .add_role_slot(
                "boss",
                "bosspw",
                full_policy(&["*.ryu-oh.org"], 30, &["users"], true, &["/"], true),
            )
            .unwrap();
        let (addr, _state) = spawn_ca_server(dir.path()).await;
        let identity =
            admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        admin_client::add_role_admin(
            addr,
            NodeKind::Client,
            &identity,
            "boss",
            "bosspw",
            "eu-ops",
            "eupw",
            full_policy(&["*.eu.ryu-oh.org"], 30, &["users"], false, &["/eu"], false),
        )
        .await
        .unwrap();
        let admins = admin_client::list_admins(
            addr,
            NodeKind::Client,
            &identity,
            "boss",
            "bosspw",
        )
        .await
        .unwrap();
        let eu = admins.iter().find(|a| a.admin == "eu-ops").expect("eu-ops minted");
        assert_eq!(eu.kind, ca_vault::SlotKind::Role);
        assert_eq!(eu.policy.allowed_san, vec!["*.eu.ryu-oh.org".to_string()]);
        // The new admin authenticates and is scoped (can't unlock the key).
        assert!(ca_vault::CAVault::new(dir.path().to_path_buf()).unlock("eupw").is_err());
    }

    /// A request over the local control socket (`local = true`) authorizes
    /// admin management as a superuser with NO password — the SO_PEERCRED gate
    /// at accept is the authorization — while the same request over the
    /// network (`local = false`) with bogus credentials is refused.
    #[tokio::test]
    async fn local_socket_authorizes_admin_mgmt_without_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (_addr, state) = spawn_ca_server(dir.path()).await;
        let req = AddRoleAdminRequest {
            admin: "nobody".to_string(),
            password: Secret("wrong".to_string()),
            name: "eu-ops".to_string(),
            new_password: Secret("eupw".to_string()),
            policy: full_policy(&["*.eu.example.com"], 30, &[], false, &["/eu"], false),
        };
        // Network path: bogus credentials are refused.
        assert!(matches!(
            handle_add_role_admin(&state, &req, false),
            AdminMgmtResponse::Err { .. }
        ));
        // Local path: no password needed, authorized as a signing-tier superuser.
        assert!(matches!(
            handle_add_role_admin(&state, &req, true),
            AdminMgmtResponse::Ok
        ));
        // It still mints a scoped ROLE slot (never an MK-wrapping signing slot).
        let admins =
            ca_vault::CAVault::new(dir.path().to_path_buf()).list_admins().unwrap();
        let eu = admins.iter().find(|a| a.admin == "eu-ops").expect("eu-ops minted");
        assert_eq!(eu.kind, ca_vault::SlotKind::Role);
    }

    /// A perms edit over the local control socket (`local = true`) is
    /// authorized as a signing-tier superuser with NO password — the
    /// SO_PEERCRED gate is the authorization — while the same request over the
    /// network (`local = false`) with bogus credentials is refused and leaves
    /// the file untouched. The local path still routes by the map and
    /// propagates to the cluster (a loopback apply here), so a local edit is
    /// as cluster-consistent as a remote signing-admin edit.
    #[tokio::test(flavor = "multi_thread")]
    async fn local_socket_authorizes_perms_edit_without_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        std::fs::write(dir.path().join("perms.json"), r#"{"/":{"users":"swl"}}"#)
            .unwrap();
        let rpath = write_resolver_with_perms(dir.path());
        let roles = Roles {
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: Some(ResolverRole { config: rpath }),
            id_map: None,
        };
        let (addr, state) = spawn_server_with(dir.path(), roles, vec![]).await;
        let id = admin_client::fetch_identity(addr, NodeKind::Client).await.unwrap();
        let signs = Arc::new(Semaphore::new(MAX_CONCURRENT_SIGNS));
        let req = EditPermsRequest {
            admin: "nobody".to_string(),
            password: Secret("wrong".to_string()),
            target_path: "/".to_string(),
            perms_json: r#"{"/foo":{"bob":"swl"}}"#.to_string(),
        };

        // Network path with bogus credentials is refused, file untouched.
        match handle_edit_perms(&state, &signs, &req, false).await {
            EditPermsResponse::Err { reason } => {
                assert!(reason.contains("authentication failed"), "got {reason}")
            }
            EditPermsResponse::Ok { .. } => panic!("bogus creds must be refused"),
        }
        let p = admin_client::get_perms(addr, NodeKind::Client, &id).await.unwrap();
        assert!(p.contains("users") && !p.contains("bob"), "deny must not mutate: {p}");

        // Local path: no password needed, authorized as a signing-tier
        // superuser, and the edit routes + propagates (loopback to self).
        match handle_edit_perms(&state, &signs, &req, true).await {
            EditPermsResponse::Ok { peers } => {
                assert!(
                    peers.iter().all(|p| p.error.is_none()),
                    "all peers applied: {peers:?}"
                )
            }
            EditPermsResponse::Err { reason } => panic!("local edit refused: {reason}"),
        }
        let p = admin_client::get_perms(addr, NodeKind::Client, &id).await.unwrap();
        assert!(p.contains("bob") && !p.contains("users"), "local edit applied: {p}");
    }

    /// A local (control-socket) caller is the on-box superuser, but confined to
    /// this host's own resolver level — it cannot reach across the map to edit
    /// another cluster's permissions.
    #[tokio::test(flavor = "multi_thread")]
    async fn local_perms_edit_is_confined_to_own_level() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        std::fs::write(dir.path().join("perms.json"), r#"{"/":{"users":"swl"}}"#)
            .unwrap();
        let rpath = write_resolver_with_perms(dir.path());
        let roles = Roles {
            ca: Some(CaRole {
                dir: dir.path().to_path_buf(),
                autorenew: Some(autorenew_keytab(dir.path())),
            }),
            resolver: Some(ResolverRole { config: rpath }),
            id_map: None,
        };
        let (_addr, state) = spawn_server_with(dir.path(), roles, vec![]).await;
        let signs = Arc::new(Semaphore::new(MAX_CONCURRENT_SIGNS));
        // This host's own resolver base is "/"; a local edit at another level is
        // refused even though local auth is a signing-tier superuser.
        let req = EditPermsRequest {
            admin: String::new(),
            password: Secret(String::new()),
            target_path: "/somewhere/else".to_string(),
            perms_json: r#"{"/x":{"bob":"swl"}}"#.to_string(),
        };
        match handle_edit_perms(&state, &signs, &req, true).await {
            EditPermsResponse::Err { reason } => {
                assert!(
                    reason.contains("confined to this host's own level"),
                    "got {reason}"
                )
            }
            EditPermsResponse::Ok { .. } => {
                panic!("a local edit at a non-own level must be refused")
            }
        }
    }

    /// `RotateRecovery` is refused over the network and, over the local
    /// socket, mints a fresh recovery slot (using the box's own autorenew
    /// credential) whose returned password unlocks the CA.
    #[tokio::test]
    async fn rotate_recovery_is_local_only_and_mints_a_working_password() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (_addr, state) = spawn_ca_server(dir.path()).await;
        assert!(matches!(
            handle_rotate_recovery(&state, false),
            RotateRecoveryResponse::Err { reason } if reason.contains("local control socket")
        ));
        let pw = match handle_rotate_recovery(&state, true) {
            RotateRecoveryResponse::Ok { recovery_password } => {
                recovery_password.0.clone()
            }
            RotateRecoveryResponse::Err { reason } => panic!("{reason}"),
        };
        let ca = state.ca.as_ref().unwrap();
        // The returned (canonical) password unlocks the CA via the new slot.
        assert!(ca.vault.read().unlock(&pw).is_ok());
        let admins = ca.vault.read().list_admins().unwrap();
        let rec = admins
            .iter()
            .find(|a| a.admin == ca_vault::RECOVERY_ADMIN)
            .expect("recovery slot minted");
        assert_eq!(rec.kind, ca_vault::SlotKind::Signing);
    }

    /// `RotateAutorenew` is refused over the network and, over the local
    /// socket, hot-swaps the box's own signing credential: the rekeyed vault,
    /// the rewritten keytab, and the in-process credential all stay
    /// consistent, the old password stops working, and the server can still
    /// sign. No staging file is left behind.
    #[tokio::test]
    async fn rotate_autorenew_hot_swaps_the_box_credential() {
        let dir = tempfile::tempdir().unwrap();
        setup_ca(dir.path());
        let (_addr, state) = spawn_ca_server(dir.path()).await;
        let ca = state.ca.as_ref().unwrap();
        assert!(ca.vault.read().unlock(AUTORENEW_PW).is_ok());
        assert!(matches!(
            handle_rotate_autorenew(&state, false),
            RotateAutorenewResponse::Err { reason } if reason.contains("local control socket")
        ));
        match handle_rotate_autorenew(&state, true) {
            RotateAutorenewResponse::Ok { .. } => {}
            RotateAutorenewResponse::Err { reason } => panic!("{reason}"),
        }
        // The keytab now holds a NEW password that unlocks the rekeyed vault;
        // the old one no longer does.
        let new_pw = std::fs::read_to_string(autorenew_keytab(dir.path())).unwrap();
        assert_ne!(new_pw, AUTORENEW_PW);
        assert!(ca.vault.read().unlock(&new_pw).is_ok());
        assert!(ca.vault.read().unlock(AUTORENEW_PW).is_err());
        // The in-process credential was swapped to match, so signing works.
        let mem = ca.autorenew_pw.read().as_ref().map(|z| z.to_string());
        assert_eq!(mem, Some(new_pw.clone()));
        assert!(server_unlock(ca).is_ok());
        // The staging file was renamed into place, not left behind.
        assert!(!autorenew_keytab(dir.path()).with_extension("rotating").exists());
    }
}

#[cfg(test)]
mod v6_tests {
    use super::*;
    use crate::fingerprint::Fingerprint;

    #[test]
    fn resolver_replicas_may_share_a_tls_name_with_distinct_keys() {
        assert!(!one_live_name(NodeKind::Resolver));
        assert!(one_live_name(NodeKind::Client));
        assert!(one_live_name(NodeKind::Publisher));
        assert!(one_live_name(NodeKind::Workstation));
    }

    #[test]
    fn topology_fanout_writes_the_split_config_without_service_control() {
        use netidx::resolver_server::config::file as rfile;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ap1.json");
        let member = |addr: &str| {
            let addr = addr.parse::<SocketAddr>().unwrap();
            rfile::MemberServerBuilder::default()
                .addr(addr)
                .bind_addr(addr.ip())
                .auth(rfile::Auth::Anonymous)
                .build()
                .unwrap()
        };
        let cfg = rfile::ConfigBuilder::default()
            .member_servers(vec![
                member("10.0.60.1:4564"),
                member("10.0.60.2:4564"),
                member("10.0.0.1:4564"),
                member("10.0.0.2:4564"),
            ])
            .build()
            .unwrap();
        std::fs::write(&p, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();
        let resolver = |addr: &str| ResolverAddr {
            addr: addr.parse().unwrap(),
            auth: InfoAuth::Anonymous,
        };
        let edit = ReferralEdit::SetTopology {
            local_member: resolver("10.0.60.1:4564"),
            members: vec![resolver("10.0.60.2:4564"), resolver("10.0.60.1:4564")],
            parent: Some(admin_proto::ClusterEdge {
                path: "/ap".into(),
                addrs: vec![resolver("10.0.0.1:4564"), resolver("10.0.0.2:4564")],
            }),
            children: vec![],
        };

        apply_referral_edit_local(&p, &edit).unwrap();
        let rc = crate::resolver::ResolverConfig::load(&p).unwrap();
        assert_eq!(
            rc.as_file().member_servers.iter().map(|m| m.addr).collect::<Vec<_>>(),
            vec![
                "10.0.60.1:4564".parse::<SocketAddr>().unwrap(),
                "10.0.60.2:4564".parse::<SocketAddr>().unwrap(),
            ]
        );
        let parent = rc.as_file().parent.as_ref().unwrap();
        assert_eq!(&*parent.path, "/ap");
        assert_eq!(
            parent.addrs.iter().map(|(addr, _)| *addr).collect::<Vec<_>>(),
            vec![
                "10.0.0.1:4564".parse::<SocketAddr>().unwrap(),
                "10.0.0.2:4564".parse::<SocketAddr>().unwrap(),
            ]
        );

        // A local-only member menu is equally valid. The authoritative AP2
        // member stays in the CA map/referrals but is not synthesized into
        // this host's resolver.json.
        let local_only = dir.path().join("ap-local-only.json");
        let cfg = rfile::ConfigBuilder::default()
            .member_servers(vec![member("10.0.60.1:4564")])
            .build()
            .unwrap();
        std::fs::write(&local_only, serde_json::to_vec_pretty(&cfg).unwrap()).unwrap();
        apply_referral_edit_local(&local_only, &edit).unwrap();
        let rc = crate::resolver::ResolverConfig::load(&local_only).unwrap();
        assert_eq!(rc.as_file().member_servers.len(), 1);
        assert_eq!(rc.as_file().member_servers[0].addr, resolver("10.0.60.1:4564").addr);

        let wrong_member = ResolverAddr {
            addr: "10.0.60.1:4564".parse().unwrap(),
            auth: InfoAuth::Tls { name: "wrong.example".into() },
        };
        let wrong_auth = ReferralEdit::SetTopology {
            local_member: wrong_member.clone(),
            members: vec![wrong_member],
            parent: None,
            children: vec![],
        };
        assert!(apply_referral_edit_local(&p, &wrong_auth).is_err());
    }

    #[test]
    fn centralized_authorization_matrix_protects_all_mutations() {
        let operation_id = admin_proto::OperationId::new();
        assert_eq!(
            request_authorization(&Request::GetInfo),
            RequestAuthorization::Public
        );
        assert_eq!(
            request_authorization(&Request::Register(RegisterRequest {
                addr: "127.0.0.1:4565".parse().unwrap(),
                resolver: None,
            })),
            RequestAuthorization::NodeSelf,
        );
        assert_eq!(
            request_authorization(&Request::AddIdentity(AddIdentityRequest {
                operation_id,
                san: "alice.example".into(),
                primary_group: "users".into(),
                groups: vec![],
            })),
            RequestAuthorization::ControllerOnly,
        );
        assert_eq!(
            request_authorization(&Request::ApplyPermsEdit(ApplyPermsEditRequest {
                operation_id,
                perms_json: "{}".into(),
            })),
            RequestAuthorization::ControllerOnly,
        );
        assert_eq!(
            request_authorization(&Request::GetPerms),
            RequestAuthorization::ControllerOnly,
        );
        assert_eq!(
            request_authorization(&Request::ReadPerms(ReadPermsRequest {
                credential: admin_proto::AdminCredential::password("alice", "pw"),
                target_path: "/eu".into(),
            })),
            RequestAuthorization::AdminAuthenticated,
        );
        assert_eq!(
            request_authorization(&Request::ApplyCrl(ApplyCrlRequest {
                operation_id,
                crl_pem: "crl".into(),
            })),
            RequestAuthorization::ControllerOnly,
        );
        let controller = admin_proto::AdminServerId::new();
        let mut map = NetworkMap::empty(controller);
        map.servers.push(ServerEntry {
            id: controller,
            addr: "127.0.0.1:4565".parse().unwrap(),
            roles: vec![Role::Ca],
            resolver: None,
            cluster: None,
            state: admin_proto::ServerState::Registered,
        });
        assert_eq!(
            request_authorization(&Request::ApplyControllerState(
                ApplyControllerStateRequest {
                    operation_id,
                    controller,
                    addr: "127.0.0.1:4565".parse().unwrap(),
                    map,
                    crl_pem: "crl".into(),
                }
            )),
            RequestAuthorization::ControllerOnly,
        );
        assert_eq!(
            request_authorization(&Request::Backup(admin_proto::BackupRequest {
                target: "/backup".into(),
            })),
            RequestAuthorization::LocalOnly,
        );
        assert_eq!(
            request_authorization(&Request::ReconcileController(
                admin_proto::ReconcileControllerRequest {
                    credential: admin_proto::AdminCredential::password("admin", "pw"),
                }
            )),
            RequestAuthorization::AdminAuthenticated,
        );
        assert_eq!(
            request_authorization(&Request::ApplyReferralEdit(
                ApplyReferralEditRequest {
                    operation_id,
                    edit: ReferralEdit::SetTopology {
                        local_member: ResolverAddr {
                            addr: "127.0.0.1:4564".parse().unwrap(),
                            auth: InfoAuth::Anonymous,
                        },
                        members: vec![],
                        parent: None,
                        children: vec![],
                    },
                }
            )),
            RequestAuthorization::ControllerOnly,
        );
        assert_eq!(
            request_authorization(&Request::ApplyServiceControl(
                ApplyServiceControlRequest {
                    operation_id,
                    units: vec![],
                    op: netidx_activation::control::ControlOp::Status,
                },
            )),
            RequestAuthorization::ControllerOnly,
        );
        assert_eq!(
            request_authorization(&Request::RotateRecovery),
            RequestAuthorization::LocalOnly,
        );

        // A home-CA ordinary node may self-register, but it cannot invoke any
        // controller mutation. A foreign co-trusted certificate is represented
        // by both peer flags being false and receives neither authority.
        assert!(
            authorize_request_class(
                RequestAuthorization::ControllerOnly,
                false,
                true,
                false,
            )
            .is_err()
        );
        assert!(
            authorize_request_class(
                RequestAuthorization::ControllerOnly,
                false,
                true,
                true,
            )
            .is_ok()
        );
        assert!(
            authorize_request_class(
                RequestAuthorization::ControllerOnly,
                false,
                false,
                false,
            )
            .is_err()
        );
        assert!(
            authorize_request_class(RequestAuthorization::NodeSelf, false, true, false,)
                .is_ok()
        );
        assert!(
            authorize_request_class(RequestAuthorization::NodeSelf, false, false, false,)
                .is_err()
        );
        assert!(
            authorize_request_class(RequestAuthorization::LocalOnly, false, true, true)
                .is_err(),
            "even the controller certificate cannot invoke a local-only backup"
        );
        assert!(
            authorize_request_class(RequestAuthorization::LocalOnly, true, false, false)
                .is_ok()
        );
    }

    #[test]
    fn perms_reads_and_edits_share_the_same_scope_authorization() {
        let role = ca_vault::Authenticated {
            slot_id: uuid::Uuid::new_v4(),
            credential_revision: 0,
            admin: "eu-ops".into(),
            policy: ca_vault::Policy {
                allowed_san: vec![],
                max_validity: Duration::from_secs(60),
                id_map_groups: vec![],
                server_enroll_scopes: vec![],
                server_enroll_roles: vec![],
                perms_edit_scopes: vec!["/eu".into()],
                may_manage_admins: false,
                service_control_scopes: vec![],
            },
            kind: ca_vault::SlotKind::Role,
        };
        for operation in ["read", "edit"] {
            assert!(authorize_perms_scope(&role, "/eu", operation).is_ok());
            assert!(authorize_perms_scope(&role, "/eu/ap", operation).is_ok());
            assert!(authorize_perms_scope(&role, "/", operation).is_err());
            assert!(authorize_perms_scope(&role, "/us", operation).is_err());
        }
        let signing =
            ca_vault::Authenticated { kind: ca_vault::SlotKind::Signing, ..role };
        assert!(authorize_perms_scope(&signing, "/", "read").is_ok());
        assert!(authorize_perms_scope(&signing, "/us", "edit").is_ok());
    }

    #[test]
    fn service_control_routing_uses_identity_across_address_reuse() {
        let controller = admin_proto::AdminServerId::new();
        let selected = admin_proto::AdminServerId::new();
        let replacement = admin_proto::AdminServerId::new();
        let old_addr = "10.0.0.10:4565".parse().unwrap();
        let new_addr = "10.0.0.20:4565".parse().unwrap();
        let entry = |id, addr| ServerEntry {
            id,
            addr,
            roles: vec![Role::Resolver],
            resolver: None,
            cluster: None,
            state: admin_proto::ServerState::Registered,
        };
        let mut map = NetworkMap::empty(controller);
        // The selected identity moved, and a different identity reused its old
        // address. Routing by address would now hit `replacement`.
        map.servers.push(entry(selected, new_addr));
        map.servers.push(entry(replacement, old_addr));

        assert_eq!(registered_server_addr(&map, selected), Some(new_addr));
        assert_eq!(registered_server_addr(&map, replacement), Some(old_addr));
        assert_eq!(registered_server_addr(&map, admin_proto::AdminServerId::new()), None);

        map.servers[0].state = admin_proto::ServerState::Enrolled;
        assert_eq!(registered_server_addr(&map, selected), None);
    }

    #[test]
    fn controller_identity_is_renewal_only_and_preserved() {
        let controller = admin_proto::AdminServerId::new();
        let map = NetworkMap::empty(controller);
        let identity =
            enrollment_cert_identity(true, Some(controller), Some(&map)).unwrap();
        assert_eq!(identity.server_id, controller);
        assert!(identity.controller);
        assert!(enrollment_cert_identity(true, None, Some(&map)).is_err());
        assert!(
            enrollment_cert_identity(
                true,
                Some(admin_proto::AdminServerId::new()),
                Some(&map),
            )
            .is_err()
        );
        let satellite =
            enrollment_cert_identity(false, Some(controller), Some(&map)).unwrap();
        assert!(!satellite.controller);
        assert_ne!(satellite.server_id, controller);
    }

    #[test]
    fn enrollment_policy_enforces_scope_roles_and_invariants() {
        let role_admin = ca_vault::Authenticated {
            slot_id: uuid::Uuid::new_v4(),
            credential_revision: 0,
            admin: "eu-ops".into(),
            policy: ca_vault::Policy {
                allowed_san: vec![],
                max_validity: Duration::from_secs(60),
                id_map_groups: vec![],
                server_enroll_scopes: vec!["/eu".into()],
                server_enroll_roles: vec![Role::Resolver],
                perms_edit_scopes: vec![],
                may_manage_admins: false,
                service_control_scopes: vec![],
            },
            kind: ca_vault::SlotKind::Role,
        };
        let request = |base: &str, roles: Vec<Role>| admin_proto::EnrollmentRequest {
            listen: "127.0.0.1:4565".parse().unwrap(),
            roles,
            resolver_member: Some(ResolverAddr {
                addr: "127.0.0.1:4564".parse().unwrap(),
                auth: InfoAuth::Anonymous,
            }),
            resolver_members: vec![ResolverAddr {
                addr: "127.0.0.1:4564".parse().unwrap(),
                auth: InfoAuth::Anonymous,
            }],
            cluster: admin_proto::ClusterPlacement::Create { base: base.into() },
        };
        assert!(
            authorize_enrollment(
                &role_admin,
                &request("/eu/one", vec![Role::Resolver]),
                None,
            )
            .is_ok()
        );
        assert!(
            authorize_enrollment(
                &role_admin,
                &request("/us", vec![Role::Resolver]),
                None,
            )
            .is_err()
        );
        assert!(
            authorize_enrollment(
                &role_admin,
                &request("/eu", vec![Role::Resolver, Role::IdMap]),
                None,
            )
            .is_err()
        );

        let signing =
            ca_vault::Authenticated { kind: ca_vault::SlotKind::Signing, ..role_admin };
        assert!(
            authorize_enrollment(
                &signing,
                &request("/", vec![Role::Ca, Role::Resolver]),
                None,
            )
            .is_err()
        );
        assert!(authorize_enrollment(&signing, &request("/", vec![]), None).is_err());
    }

    fn signed_empty_crl(dir: &Path, name: &str) -> (String, Vec<u8>) {
        let ca = Ca::init(
            &crate::ca::CaParams {
                directory: dir.to_path_buf(),
                subject: crate::ca::Subject::cn(name),
                san: vec![],
                key_bits: crate::ca::MIN_KEY_BITS,
                validity: Duration::from_secs(30 * 86400),
            },
            None,
        )
        .unwrap();
        drop(ca);
        let key = std::fs::read(dir.join("private.key")).unwrap();
        let ca_pem = std::fs::read(dir.join("certificate.pem")).unwrap();
        let ca_der = rustls_pemfile::certs(&mut std::io::Cursor::new(ca_pem))
            .next()
            .unwrap()
            .unwrap()
            .to_vec();
        let cadir = ca_store::CaDir::open(dir).unwrap();
        cadir.store.lock().write_crl(&key).unwrap();
        let pem = std::fs::read_to_string(cadir.store.lock().crl_path()).unwrap();
        (pem, ca_der)
    }

    #[test]
    fn immediate_crl_install_is_signed_atomic_and_all_or_nothing_on_validation() {
        let home = tempfile::tempdir().unwrap();
        let foreign = tempfile::tempdir().unwrap();
        let (home_crl, home_ca) = signed_empty_crl(home.path(), "home-ca");
        let (foreign_crl, _) = signed_empty_crl(foreign.path(), "foreign-ca");
        let root = tempfile::tempdir().unwrap();
        let destinations = BTreeSet::from([
            root.path().join("admin/crl.pem"),
            root.path().join("resolver/crl.pem"),
        ]);

        apply_crl_to_destinations(&home_crl, &home_ca, destinations.clone()).unwrap();
        for path in &destinations {
            assert_eq!(std::fs::read_to_string(path).unwrap(), home_crl);
        }

        let error =
            apply_crl_to_destinations(&foreign_crl, &home_ca, destinations.clone())
                .unwrap_err();
        assert!(format!("{error:#}").contains("does not verify"));
        for path in &destinations {
            assert_eq!(
                std::fs::read_to_string(path).unwrap(),
                home_crl,
                "signature validation happens before any destination is replaced"
            );
        }
    }

    #[test]
    fn controller_state_relocation_persists_route_map_and_crl_without_rollback() {
        use crate::admin_server_config::Roles;
        let home = tempfile::tempdir().unwrap();
        let (crl, home_ca) = signed_empty_crl(home.path(), "home-ca");
        let root = tempfile::tempdir().unwrap();
        let cfg_path = root.path().join("admin-server.json");
        let trusted = root.path().join("trusted.pem");
        std::fs::write(
            &trusted,
            std::fs::read(home.path().join("certificate.pem")).unwrap(),
        )
        .unwrap();
        let controller = admin_proto::AdminServerId::new();
        let node = admin_proto::AdminServerId::new();
        let old_addr = "10.0.0.1:4565".parse().unwrap();
        let new_addr = "10.0.0.2:14565".parse().unwrap();
        let cfg = AdminServerConfig {
            domain: "example.com".into(),
            server_id: node,
            home_ca_fingerprint: Fingerprint::of_cert_der(&home_ca).unwrap().text(),
            listen: "10.1.0.2:4565".parse().unwrap(),
            serving_cert: root.path().join("unused-cert.pem"),
            serving_key: root.path().join("unused-key.pem"),
            trusted: trusted.clone(),
            roles: Roles::default(),
            ca_addr: Some(old_addr),
            peers: vec![],
            mdns: false,
            activation_units_dir: None,
        };
        cfg.save(&cfg_path).unwrap();
        let entry = |id, addr, roles| ServerEntry {
            id,
            addr,
            roles,
            resolver: None,
            cluster: None,
            state: admin_proto::ServerState::Registered,
        };
        let mut old_map = NetworkMap::empty(controller);
        old_map.version = 4;
        old_map.servers.push(entry(controller, old_addr, vec![Role::Ca]));
        old_map.servers.push(entry(node, cfg.listen, vec![Role::Resolver]));
        let mut new_map = old_map.clone();
        new_map.version = 5;
        new_map.servers.iter_mut().find(|s| s.id == controller).unwrap().addr = new_addr;
        let state = Server {
            cfg: Mutex::new(cfg),
            cfg_path: Some(cfg_path.clone()),
            ca_dir: None,
            ca: None,
            serving_cert_pem: vec![],
            serving_key_pem: vec![],
            roots: RootCertStore::empty(),
            home_ca_der: CertificateDer::from(home_ca),
            id_map_lock: Mutex::new(()),
            resolver_edit_lock: Mutex::new(()),
            map: Mutex::new(old_map),
            password_limiter: Arc::new(PasswordLimiter::default()),
            mutation_gate: Arc::new(tokio::sync::RwLock::new(())),
        };
        let req = ApplyControllerStateRequest {
            operation_id: admin_proto::OperationId::new(),
            controller,
            addr: new_addr,
            map: new_map.clone(),
            crl_pem: crl.clone(),
        };
        assert!(matches!(
            handle_apply_controller_state(&state, &req),
            ApplyControllerStateResponse::Ok
        ));
        let persisted = AdminServerConfig::load_for_recovery(&cfg_path).unwrap();
        assert_eq!(persisted.ca_addr, Some(new_addr));
        assert_eq!(*state.map.lock(), new_map);
        assert_eq!(std::fs::read_to_string(root.path().join("crl.pem")).unwrap(), crl);

        let mut stale = req.clone();
        stale.map.version = 3;
        stale.addr = old_addr;
        stale.map.servers.iter_mut().find(|s| s.id == controller).unwrap().addr =
            old_addr;
        assert!(matches!(
            handle_apply_controller_state(&state, &stale),
            ApplyControllerStateResponse::Err { .. }
        ));
        assert_eq!(
            AdminServerConfig::load_for_recovery(&cfg_path).unwrap().ca_addr,
            Some(new_addr)
        );
    }

    #[tokio::test]
    async fn immediate_crl_partial_results_are_target_identifying_and_sorted() {
        let failed = admin_proto::AdminServerId::new();
        let ok = admin_proto::AdminServerId::new();
        let failed_addr = "127.0.0.1:41001".parse().unwrap();
        let ok_addr = "127.0.0.1:41002".parse().unwrap();
        let results = collect_peer_results(
            vec![(failed, failed_addr), (ok, ok_addr)],
            |server, _addr| async move {
                if server == failed {
                    bail!("satellite link down")
                }
                Ok(())
            },
        )
        .await;

        assert_eq!(results.len(), 2);
        assert!(results.windows(2).all(|pair| pair[0].server < pair[1].server));
        let failed_result =
            results.iter().find(|result| result.server == failed).unwrap();
        assert_eq!(failed_result.addr, failed_addr);
        assert!(failed_result.error.as_deref().unwrap().contains("satellite link down"));
        let ok_result = results.iter().find(|result| result.server == ok).unwrap();
        assert_eq!(ok_result.addr, ok_addr);
        assert!(ok_result.error.is_none());
    }
}
