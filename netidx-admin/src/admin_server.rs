//! Admin server: the per-host daemon behind discovery-driven setup. It
//! answers [`Request::GetInfo`] with this host's local facts + known
//! peers, and — on the host holding the CA — turns a join client's
//! [`Request::Sign`] into a signed cert and a [`Request::Enroll`] into
//! a new admin server's reserved-SAN serving cert. After a successful
//! sign it pushes id-map registrations to every id-map-role peer.
//!
//! The request-handling cores ([`handle_sign_request`],
//! [`handle_enroll_request`], [`handle_add_identity`]) are pure of TLS
//! and directly testable: the sign/enroll paths authenticate the supplied
//! administrator credential, enforce that administrator's live [`Policy`],
//! sign with the controller's own credential, and append an audit line. The
//! TLS accept loop is a thin shell over them.

mod password_limiter;

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
        EnrollRequest, ExternalCaCsrResponse, ExternalCaInstallRequest,
        ExternalCaInstallResponse, GetCrlResponse, GetInfoResponse, GetMapResponse,
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
    ca_store, ca_vault,
    config_lock::ConfigDirLock,
    delegation_store, discovery, id_map, netmap,
};
use anyhow::{Context, Result, anyhow, bail};
use futures::{StreamExt, stream};
use globset::Glob;
use log::{debug, error, info, warn};
use parking_lot::Mutex;
use password_limiter::PasswordLimiter;
use rustls::{
    RootCertStore, ServerConfig as RustlsServerConfig,
    server::{ServerSessionMemoryCache, WebPkiClientVerifier},
};
use rustls_pki_types::CertificateDer;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, UnixListener},
    sync::{RwLock, Semaphore, mpsc as tokio_mpsc},
};
use tokio_rustls::TlsAcceptor;
use triomphe::Arc as TArc;
use zeroize::Zeroizing;

/// Max simultaneous connections. These are cheap (a TLS handshake and a
/// few small messages), so this can be generous — it just bounds socket
/// / task fan-out.
const MAX_CONNECTIONS: usize = 768;

/// Max simultaneous Argon2/signing operations. The per-source password limiter
/// below is the normal brute-force/resource-exhaustion defence; this semaphore
/// is the hard global memory ceiling for genuinely distributed traffic.
const MAX_CONCURRENT_SIGNS: usize = 64;

/// Upper bound on a single connection's whole lifetime (handshake +
/// request + sign + push + response). Without it a client that connects
/// and stalls holds a connection slot indefinitely.
const CONN_TIMEOUT: Duration = Duration::from_secs(30);

/// Budget for the whole post-sign id-map push fan-out. Bounded well
/// under [`CONN_TIMEOUT`] so a dead id-map host degrades a join to a
/// warning instead of timing the connection out.
const PUSH_TIMEOUT: Duration = Duration::from_secs(10);

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

#[derive(Clone)]
struct PasswordAttempt(Arc<PasswordAttemptInner>);

struct PasswordAttemptInner {
    state: Weak<Server>,
    source: IpAddr,
    result: AtomicU8,
    runtime: tokio::runtime::Handle,
}

impl PasswordAttemptInner {
    fn result(&self) -> Option<bool> {
        match self.result.load(Ordering::Acquire) {
            0 => None,
            1 => Some(true),
            2 => Some(false),
            _ => unreachable!(),
        }
    }
}

impl PasswordAttempt {
    fn new(state: &Arc<Server>, source: IpAddr) -> Self {
        Self(Arc::new(PasswordAttemptInner {
            state: Arc::downgrade(state),
            source,
            result: AtomicU8::new(0),
            runtime: tokio::runtime::Handle::current(),
        }))
    }

    fn finish(&self, success: bool) {
        let result = if success { 1 } else { 2 };
        let _ = self.0.result.compare_exchange(
            0,
            result,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

impl Drop for PasswordAttemptInner {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let source = self.source;
            let result = self.result();
            self.runtime.spawn(async move {
                state
                    .write(move |state| state.password_limiter.complete(source, result))
                    .await;
            });
        }
    }
}

tokio::task_local! {
    static REQUEST_AUTHENTICATION: Option<std::result::Result<
        ca_vault::Authenticated,
        String,
    >>;
    static REQUEST_SERVER_UNLOCK: Option<std::result::Result<
        TArc<ca_vault::Unlocked>,
        String,
    >>;
}

fn prepared_authentication(
    ca: &ca_store::CaDir,
) -> Option<std::result::Result<ca_vault::Authenticated, String>> {
    REQUEST_AUTHENTICATION.try_with(Clone::clone).unwrap_or(None).map(|result| {
        result.and_then(|authenticated| {
            ca.vault
                .resolve_session_slot(
                    authenticated.slot_id,
                    authenticated.credential_revision,
                )
                .map_err(|_| "authentication failed".to_string())
        })
    })
}

#[cfg(test)]
mod state_tests {
    use super::*;

    #[test]
    fn custom_config_root_retains_the_offline_ca_lock() {
        let root = tempfile::tempdir().unwrap();
        let ca_dir = root.path().join("ca");
        let config_lock = ConfigDirLock::acquire(root.path()).unwrap();
        let alias = acquire_ca_alias_lock(&config_lock, Some(&ca_dir)).unwrap();
        assert!(alias.is_some());
        assert!(ConfigDirLock::acquire(&ca_dir).is_err());
    }

    fn test_server(ca: Option<ca_store::CaDir>) -> Arc<Server> {
        let id = admin_proto::AdminServerId::new();
        let config_lock =
            ca.as_ref().map(ca_store::CaDir::config_lock).unwrap_or_else(|| {
                let root = tempfile::tempdir().unwrap().keep().join("config");
                ConfigDirLock::acquire(root).unwrap()
            });
        Server::from_state(
            config_lock,
            None,
            MutableState {
                cfg: AdminServerConfig {
                    domain: String::new(),
                    server_id: id,
                    home_ca_fingerprint: String::new(),
                    listen: "127.0.0.1:0".parse().unwrap(),
                    serving_cert: PathBuf::new(),
                    serving_key: PathBuf::new(),
                    trusted: PathBuf::new(),
                    roles: crate::admin_server_config::Roles::default(),
                    ca_addr: Some("127.0.0.1:0".parse().unwrap()),
                    peers: Vec::new(),
                    mdns: false,
                    activation_units_dir: None,
                },
                map: NetworkMap::empty(id),
                ca,
                password_limiter: PasswordLimiter::default(),
            },
            None,
            Vec::new(),
            Vec::new(),
            RootCertStore::empty(),
            CertificateDer::from(Vec::new()),
        )
        .unwrap()
    }

    fn limiter_server() -> Arc<Server> {
        test_server(None)
    }

    #[tokio::test]
    async fn dropping_a_request_does_not_release_an_attempt_still_owned_by_argon() {
        let state = limiter_server();
        let source = "203.0.113.10".parse().unwrap();
        let (request_attempt, delay) =
            state.begin_password_attempt(source).await.unwrap();
        assert_eq!(delay, Duration::ZERO);
        let argon_attempt = request_attempt.clone();
        drop(request_attempt);
        assert!(state.begin_password_attempt(source).await.is_err());

        argon_attempt.finish(false);
        drop(argon_attempt);
        let (next, delay) = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(attempt) = state.begin_password_attempt(source).await {
                    break attempt;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            delay > Duration::from_millis(800) && delay <= Duration::from_secs(1),
            "the completed Argon2 attempt must record its failure: {delay:?}"
        );
        drop(next);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn rwlock_serializes_writers() {
        let state = limiter_server();
        let callers: [_; 8] = std::array::from_fn(|_| {
            let state = state.clone();
            tokio::spawn(async move {
                for _ in 0..100 {
                    state.write(move |state| state.map.version += 1).await;
                }
            })
        });
        for caller in callers {
            caller.await.unwrap();
        }
        assert_eq!(state.read(move |state| state.map.version).await, 800);
    }

    #[tokio::test]
    async fn rwlock_state_revalidates_worker_authentication() {
        let dir = tempfile::tempdir().unwrap();
        let lock = ConfigDirLock::acquire_for_ca_dir(dir.path()).await.unwrap();
        let mut ca = ca_store::CaDir::open(lock, dir.path()).await.unwrap();
        ca.vault
            .create(b"mock-ca-key", "alice", "old", crate::ca_policy::recovery_policy())
            .await
            .unwrap();
        let state = test_server(Some(ca));
        let snapshot = state
            .read(move |state| state.ca.as_ref().unwrap().vault.snapshot())
            .await
            .unwrap();
        let authenticated = snapshot.authenticate("alice", "old").unwrap();
        let credential = admin_proto::AdminCredential::password("alice", "old");
        let first = REQUEST_AUTHENTICATION
            .scope(
                Some(Ok(authenticated.clone())),
                state.write({
                    let credential = credential.clone();
                    move |state| authenticate(state.ca.as_mut().unwrap(), &credential)
                }),
            )
            .await;
        assert_eq!(first.unwrap().admin, "alice");

        state
            .write_async(async move |state| {
                let vault = &mut state.ca.as_mut().unwrap().vault;
                vault.remove_slot("alice", true).await.unwrap();
                vault
                    .add_role_slot("alice", "new", crate::ca_policy::recovery_policy())
                    .await
                    .unwrap();
            })
            .await;
        let stale = REQUEST_AUTHENTICATION
            .scope(
                Some(Ok(authenticated)),
                state.write(move |state| {
                    authenticate(state.ca.as_mut().unwrap(), &credential)
                }),
            )
            .await;
        assert!(stale.is_err());
    }
}

struct MutableState {
    cfg: AdminServerConfig,
    map: NetworkMap,
    ca: Option<ca_store::CaDir>,
    password_limiter: PasswordLimiter,
}

struct CachedOutboundClient {
    digest: [u8; 32],
    client: admin_client::AuthenticatedPkiClient,
}

fn outbound_identity_digest(cert_pem: &[u8], key_pem: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(cert_pem.len().to_le_bytes());
    digest.update(cert_pem);
    digest.update(key_pem.len().to_le_bytes());
    digest.update(key_pem);
    digest.finalize().into()
}

fn acquire_ca_alias_lock(
    config_lock: &ConfigDirLock,
    ca_dir: Option<&Path>,
) -> Result<Option<ConfigDirLock>> {
    let Some(ca_dir) = ca_dir else { return Ok(None) };
    let root = ConfigDirLock::root_for_ca_dir(ca_dir)?;
    if root == config_lock.root() {
        Ok(None)
    } else {
        ConfigDirLock::acquire(root).map(Some)
    }
}

pub struct Server {
    state: RwLock<MutableState>,
    config_lock: ConfigDirLock,
    _ca_alias_lock: Option<ConfigDirLock>,
    /// Where to persist peer updates. `None` (tests) keeps them
    /// in-memory only.
    cfg_path: Option<PathBuf>,
    /// Startup serving chain and key, retained for the initial acceptor.
    serving_cert_pem: Vec<u8>,
    serving_key_pem: Vec<u8>,
    /// Trust anchors (the CA bundle) for verifying peers — both their
    /// serving certs outbound and their client certs inbound.
    roots: RootCertStore,
    pki_client: admin_client::PkiClient,
    outbound_tls: Mutex<Option<CachedOutboundClient>>,
    /// The one home CA for application-level admin authorization. Other
    /// certificates in `trusted.pem` remain data-plane federation anchors.
    home_ca_der: CertificateDer<'static>,
}

impl Server {
    pub async fn new(
        config_lock: ConfigDirLock,
        cfg: AdminServerConfig,
        cfg_path: Option<PathBuf>,
        serving_cert_pem: Vec<u8>,
        serving_key_pem: Vec<u8>,
    ) -> Result<Arc<Self>> {
        let trusted = tokio::fs::read(&cfg.trusted)
            .await
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
        // The installation guard was acquired before parsing the config. If we
        // hold the CA, it must be nested below that guarded root.
        let ca_dir = cfg.roles.ca.as_ref().map(|r| r.dir.clone());
        let ca_alias_lock = acquire_ca_alias_lock(&config_lock, ca_dir.as_deref())?;
        // The server's signing credential: the box-held autorenew password,
        // read + unsealed once. In the server-only model this is the only key
        // to the CA. Without it the CA authenticates + serves read-only but
        // signs nothing — loud, because that is a degraded CA, not a crash.
        let autorenew_pw = match cfg.roles.ca.as_ref() {
            None => None,
            Some(ca) => match ca.autorenew.as_ref() {
                Some(keytab) => match read_autorenew_password_async(keytab).await {
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
        // Open the CA state under the installation guard and retain the
        // box-held signing credential.
        let ca = match &ca_dir {
            Some(dir) => {
                config_lock.require_descendant(dir)?;
                let mut cadir = ca_store::CaDir::open(config_lock.clone(), dir).await?;
                if let Some(role) = cfg.roles.ca.as_ref() {
                    cadir.sessions.configure(
                        role.session_absolute_lifetime,
                        role.session_idle_timeout,
                    );
                }
                cadir.autorenew_pw = autorenew_pw;
                Some(cadir)
            }
            None => None,
        };
        // The network map: the CA owns + persists it, seeded with the CA's
        // own entry + address so it's never empty of itself; every other
        // host starts with an empty cache the refresh loop fills.
        let map = match &ca_dir {
            Some(dir) => {
                let mut m = netmap::load_async(dir, cfg.server_id).await?;
                let (resolver, facts) = local_resolver_data(&cfg).await;
                let existing_cluster = m
                    .servers
                    .iter()
                    .find(|s| s.id == cfg.server_id)
                    .and_then(|s| s.cluster);
                let cluster = facts.as_ref().map(|_| {
                    existing_cluster.unwrap_or_else(admin_proto::ResolverClusterId::new)
                });
                netmap::upsert_controller(
                    &mut m,
                    ServerEntry {
                        id: cfg.server_id,
                        addr: cfg.listen,
                        roles: roles_of(&cfg),
                        resolver,
                        cluster,
                        state: admin_proto::ServerState::Registered,
                    },
                    facts,
                )?;
                netmap::save_async(&config_lock, dir, &m).await?;
                m
            }
            None => NetworkMap::default(),
        };
        Server::from_state(
            config_lock,
            ca_alias_lock,
            MutableState { cfg, map, ca, password_limiter: PasswordLimiter::default() },
            cfg_path,
            serving_cert_pem,
            serving_key_pem,
            roots,
            home_ca_der,
        )
    }

    fn from_state(
        config_lock: ConfigDirLock,
        ca_alias_lock: Option<ConfigDirLock>,
        state: MutableState,
        cfg_path: Option<PathBuf>,
        serving_cert_pem: Vec<u8>,
        serving_key_pem: Vec<u8>,
        roots: RootCertStore,
        home_ca_der: CertificateDer<'static>,
    ) -> Result<Arc<Self>> {
        let pki_client = admin_client::PkiClient::new(roots.clone())?;
        let outbound_tls = if serving_cert_pem.is_empty() || serving_key_pem.is_empty() {
            None
        } else {
            Some(CachedOutboundClient {
                digest: outbound_identity_digest(&serving_cert_pem, &serving_key_pem),
                client: admin_client::AuthenticatedPkiClient::from_pem(
                    roots.clone(),
                    &serving_cert_pem,
                    &serving_key_pem,
                )?,
            })
        };
        Ok(Arc::new(Server {
            state: RwLock::new(state),
            config_lock,
            _ca_alias_lock: ca_alias_lock,
            cfg_path,
            serving_cert_pem,
            serving_key_pem,
            roots,
            pki_client,
            outbound_tls: Mutex::new(outbound_tls),
            home_ca_der,
        }))
    }

    async fn read<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&MutableState) -> T,
    {
        f(&*self.state.read().await)
    }

    async fn read_async<T, F>(&self, f: F) -> T
    where
        F: AsyncFnOnce(&MutableState) -> T,
    {
        f(&*self.state.read().await).await
    }

    async fn write<T, F>(&self, f: F) -> T
    where
        F: FnOnce(&mut MutableState) -> T,
    {
        f(&mut *self.state.write().await)
    }

    async fn write_async<T, F>(&self, f: F) -> T
    where
        F: AsyncFnOnce(&mut MutableState) -> T,
    {
        f(&mut *self.state.write().await).await
    }

    fn config_lock(&self) -> &ConfigDirLock {
        &self.config_lock
    }

    async fn has_ca(&self) -> bool {
        self.read(move |state| state.ca.is_some()).await
    }

    pub async fn ca_dir(&self) -> Option<PathBuf> {
        self.read(move |state| state.ca.as_ref().map(|ca| ca.dir().to_path_buf())).await
    }

    async fn begin_password_attempt(
        self: &Arc<Self>,
        source: IpAddr,
    ) -> Result<(PasswordAttempt, Duration)> {
        let delay =
            self.write(move |state| state.password_limiter.reserve(source)).await?;
        Ok((PasswordAttempt::new(self, source), delay))
    }

    async fn add_identity(&self, req: &AddIdentityRequest) -> AddIdentityResponse {
        let req = req.clone();
        let config_lock = self.config_lock.clone();
        self.write_async(async move |state| match state.cfg.roles.id_map.as_ref() {
            Some(role) => handle_add_identity(&config_lock, &role.map, &req).await,
            None => AddIdentityResponse::Err {
                reason: "this host has no id-map role".to_string(),
            },
        })
        .await
    }

    async fn apply_referral_edit(
        &self,
        req: &ApplyReferralEditRequest,
    ) -> ApplyReferralEditResponse {
        let req = req.clone();
        let config_lock = self.config_lock.clone();
        self.write_async(async move |state| match state.cfg.roles.resolver.as_ref() {
            Some(role) => {
                match apply_referral_edit_local(&config_lock, &role.config, &req.edit)
                    .await
                {
                    Ok(()) => ApplyReferralEditResponse::Ok,
                    Err(e) => ApplyReferralEditResponse::Err { reason: format!("{e:#}") },
                }
            }
            None => ApplyReferralEditResponse::Err {
                reason: "this host has no resolver role to edit".to_string(),
            },
        })
        .await
    }

    async fn roles(&self) -> Vec<Role> {
        self.read(move |state| roles_of(&state.cfg)).await
    }

    async fn outbound_client(&self) -> Result<admin_client::AuthenticatedPkiClient> {
        let (cert_path, key_path) = self
            .read(move |state| {
                (state.cfg.serving_cert.clone(), state.cfg.serving_key.clone())
            })
            .await;
        let (cert, key) = match load_serving_keypair(&cert_path, &key_path).await {
            Ok(identity) => identity,
            Err(e) => {
                warn!(
                    "admin-server: re-reading serving identity for push failed, \
                     using cached identity: {e:#}"
                );
                return self
                    .outbound_tls
                    .lock()
                    .as_ref()
                    .map(|cached| cached.client.clone())
                    .context("no cached outbound serving identity");
            }
        };
        let digest = outbound_identity_digest(&cert, &key);
        let mut cached = self.outbound_tls.lock();
        if let Some(cached) = &*cached
            && cached.digest == digest
        {
            return Ok(cached.client.clone());
        }
        let client = admin_client::AuthenticatedPkiClient::from_pem(
            self.roots.clone(),
            &cert,
            &key,
        )
        .context("building outbound TLS client")?;
        *cached = Some(CachedOutboundClient { digest, client: client.clone() });
        Ok(client)
    }
}

/// Run the admin server described by the config at `cfg_path` until the
/// process is killed.
pub async fn serve(cfg_path: PathBuf) -> Result<()> {
    let config_lock = ConfigDirLock::acquire_for_file_async(&cfg_path).await?;
    let cfg = AdminServerConfig::load_async(&cfg_path).await?;
    // A TPM-sealed serving key has its password in `<key>.tpm`, sealed to
    // this machine; `load_serving_keypair` unseals + decrypts in memory.
    // Failure is a hard error (a admin server silently down means no discovery
    // and no renewals for the whole network).
    let (serving_cert_pem, serving_key_pem) =
        load_serving_keypair(&cfg.serving_cert, &cfg.serving_key).await?;
    let listen = cfg.listen;
    let mdns = cfg.mdns;
    let state =
        Server::new(config_lock, cfg, Some(cfg_path), serving_cert_pem, serving_key_pem)
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
        match discovery::advertise(listen, &domain, &state.roles().await, &fp_short) {
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
    if state.has_ca().await {
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
    let signs = Arc::new(Semaphore::new(MAX_CONCURRENT_SIGNS));
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
pub fn local_socket_path(cfg_path: &Path) -> PathBuf {
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
/// Argon2 budget) but not the network connection limit — the socket is a
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
        RotateRecovery | RotateAutorenew | Backup(_) | ExternalCaCsr
        | ExternalCaInstall(_) | CaStatus => RequestAuthorization::LocalOnly,
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
        | RotateAutorenew
        | ExternalCaCsr
        | ExternalCaInstall(_)
        | CaStatus => return None,
        ReconcileController(req) => &req.credential,
    };
    matches!(credential, admin_proto::AdminCredential::Password { .. })
        .then_some(credential)
}

fn request_needs_server_unlock(req: &Request) -> bool {
    matches!(
        req,
        Request::Sign(_)
            | Request::Enroll(_)
            | Request::Approve(_)
            | Request::Revoke(_)
            | Request::RemoveServer(_)
            | Request::ReconcileController(_)
            | Request::Backup(_)
            | Request::ExternalCaCsr
            | Request::ExternalCaInstall(_)
    )
}

async fn prepare_password_authentication(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    credential: Option<admin_proto::AdminCredential>,
    attempt: Option<PasswordAttempt>,
) -> Option<std::result::Result<ca_vault::Authenticated, String>> {
    let Some(admin_proto::AdminCredential::Password { admin, password }) = credential
    else {
        return None;
    };
    let snapshot = match state
        .read(move |state| {
            state.ca.as_ref().context("this host does not hold the CA")?.vault.snapshot()
        })
        .await
    {
        Ok(snapshot) => snapshot,
        Err(e) => return Some(Err(format!("authentication failed: {e:#}"))),
    };
    Some(
        run_signing(signs, move || {
            let result = snapshot
                .authenticate(&admin, password.as_str())
                .map_err(|_| "authentication failed".to_string());
            if let Some(attempt) = attempt {
                attempt.finish(result.is_ok());
            }
            result
        })
        .await
        .unwrap_or_else(|e| Err(format!("authentication task failed: {e:#}"))),
    )
}

async fn prepare_server_unlock(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    needed: bool,
) -> Option<std::result::Result<TArc<ca_vault::Unlocked>, String>> {
    if !needed {
        return None;
    }
    let captured = state
        .read(move |state| {
            let ca = state.ca.as_ref().context("this host does not hold the CA")?;
            let password = ca
                .autorenew_pw
                .clone()
                .context("this CA cannot sign: it holds no autorenew credential")?;
            Ok::<_, anyhow::Error>((ca.vault.snapshot()?, password))
        })
        .await;
    let (snapshot, password) = match captured {
        Ok(captured) => captured,
        Err(e) => return Some(Err(format!("{e:#}"))),
    };
    Some(
        run_signing(signs, move || {
            snapshot.unlock(&password).map(TArc::new).map_err(|e| {
                format!("the CA's autorenew credential failed to unlock the key: {e:#}")
            })
        })
        .await
        .unwrap_or_else(|e| Err(format!("CA unlock task failed: {e:#}"))),
    )
}

async fn prepare_role_slot(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    req: &AddRoleAdminRequest,
) -> std::result::Result<ca_vault::PreparedRoleSlot, String> {
    let snapshot = state
        .read(move |state| {
            state.ca.as_ref().context("this host does not hold the CA")?.vault.snapshot()
        })
        .await
        .map_err(|e| format!("reading the CA vault: {e:#}"))?;
    let name = req.name.clone();
    let password = req.new_password.clone();
    let policy = req.policy.clone();
    run_signing(signs, move || {
        snapshot
            .prepare_role_slot(&name, password.as_str(), policy)
            .map_err(|e| format!("{e:#}"))
    })
    .await
    .map_err(|e| format!("preparing the administrator slot: {e:#}"))?
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
    let (domain, server_id, controller) = state
        .read(move |state| {
            (state.cfg.domain.clone(), state.cfg.server_id, state.cfg.roles.ca.is_some())
        })
        .await;
    admin_proto::write_msg(
        &mut tls,
        &ServerHello {
            protocol_version: PROTOCOL_VERSION,
            domain,
            roles: state.roles().await,
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
        if !local && state.has_ca().await && password_credential(&req).is_some() {
            let (attempt, delay) = state.begin_password_attempt(peer.ip()).await?;
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Some(attempt)
        } else {
            None
        };
    let credential = password_credential(&req).cloned();
    let needs_server_unlock = request_needs_server_unlock(&req);
    let authentication = prepare_password_authentication(
        state,
        &signs,
        credential,
        password_attempt.clone(),
    )
    .await;
    let unlock = if matches!(&authentication, Some(Err(_))) {
        None
    } else {
        prepare_server_unlock(state, &signs, needs_server_unlock).await
    };
    let request = async move {
        match req {
            Request::GetInfo => {
                let resp = get_info(state).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing GetInfoResponse")
            }
            Request::Login(req) => {
                let resp = if !state.has_ca().await {
                    admin_proto::LoginResponse::Err {
                        reason: "login must be sent to the CA controller".to_string(),
                    }
                } else {
                    state
                        .write(move |state| {
                            let ca = state.ca.as_mut().expect("CA role held");
                            let response = match &req.credential {
                                admin_proto::AdminCredential::Password { .. } => {
                                    match prepared_authentication(ca) {
                                        Some(Ok(authenticated)) => {
                                            ca.sessions.login_authenticated(authenticated)
                                        }
                                        Some(Err(_)) | None => {
                                            admin_proto::LoginResponse::Err {
                                                reason: "authentication failed".into(),
                                            }
                                        }
                                    }
                                }
                                admin_proto::AdminCredential::Session { .. } => {
                                    admin_proto::LoginResponse::Err {
                                        reason:
                                            "login requires an administrator password"
                                                .into(),
                                    }
                                }
                            };
                            response
                        })
                        .await
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing LoginResponse")
            }
            Request::Logout(req) => {
                let resp = state
                    .write(move |state| match state.ca.as_mut() {
                        None => admin_proto::LogoutResponse::Err {
                            reason: "logout must be sent to the CA controller"
                                .to_string(),
                        },
                        Some(ca) => ca.sessions.logout(&req.credential),
                    })
                    .await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing LogoutResponse")
            }
            Request::Sign(req) => {
                let resp = match ca_dir(state).await {
                    None => reject("this host does not hold the CA"),
                    Some(_) => {
                        let signed = state
                            .write_async(async move |state| {
                                handle_sign_request(
                                    state.ca.as_mut().expect("CA role held"),
                                    &req,
                                )
                                .await
                            })
                            .await;
                        match signed.resp {
                            resp @ SignResponse::Err { .. } => resp,
                            SignResponse::Ok {
                                signed_cert_pem,
                                trusted_pem,
                                mut warnings,
                                ..
                            } => {
                                let mut operation_id = None;
                                if let Some(plan) = signed.push {
                                    let (op, push_warnings) =
                                        push_registrations(state, &plan).await;
                                    warnings.extend(push_warnings);
                                    operation_id = Some(op);
                                }
                                if let Some(crl) = signed.replacement_crl {
                                    let op = operation_id
                                        .unwrap_or_else(admin_proto::OperationId::new);
                                    for result in push_crl_to_peers(state, &crl, op).await
                                    {
                                        if let Some(error) = result.error {
                                            warnings.push(format!(
                                                "CRL push to {} at {}: {error}",
                                                result.server, result.addr
                                            ));
                                        }
                                    }
                                    operation_id = Some(op);
                                }
                                SignResponse::Ok {
                                    signed_cert_pem,
                                    trusted_pem,
                                    warnings,
                                    operation_id,
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
                let resp = match ca_dir(state).await {
                    None => reject("this host does not hold the CA"),
                    Some(_) => {
                        let enrollment = admin_proto::EnrollmentRequest {
                            listen: req.listen,
                            roles: req.roles.clone(),
                            resolver_member: req.resolver_member.clone(),
                            resolver_members: req.resolver_members.clone(),
                            cluster: req.cluster.clone(),
                            replaces: req.replaces,
                        };
                        let resp = state
                            .write_async(async move |state| {
                                let map = state.map.clone();
                                handle_enroll_request(
                                    state.ca.as_mut().expect("CA role held"),
                                    &req,
                                    local,
                                    Some(&map),
                                )
                                .await
                            })
                            .await;
                        if let SignResponse::Ok { signed_cert_pem, .. } = &resp
                            && !local
                        {
                            let identity = crate::tls::admin_cert_identity_from_pem(
                                signed_cert_pem.as_bytes(),
                            );
                            let grant = match identity {
                                Ok(identity) => {
                                    grant_enrollment(
                                        state,
                                        identity.server_id,
                                        &enrollment,
                                    )
                                    .await?;
                                    record_peer(state, enrollment.listen).await;
                                    Ok(())
                                }
                                Err(e) => Err(e),
                            };
                            if let Err(e) = grant {
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
                    state.add_identity(&req).await
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing AddIdentityResponse")
            }
            Request::Enqueue(req) => {
                let resp = match ca_dir(state).await {
                    None => EnqueueResponse::Err {
                        reason: "this host does not hold the CA".to_string(),
                    },
                    Some(_) => {
                        state
                            .write_async(async move |state| {
                                handle_enqueue(
                                    state.ca.as_mut().expect("CA role held"),
                                    &req,
                                    peer,
                                    peer_ident.as_ref(),
                                )
                                .await
                            })
                            .await
                    }
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing EnqueueResponse")
            }
            Request::Poll(req) => {
                let resp = match ca_dir(state).await {
                    None => PollResponse::Unknown,
                    Some(_) => {
                        // The blocking part reads the status (a Pending request
                        // genuinely has no cert — the commit is atomic); on a
                        // Signed record whose id-map push never landed it also
                        // hands back a re-push plan, which the async part runs.
                        let (mut resp, repush) =
                            {
                                let id = req.request_id.clone();
                                state.read_async(async move |state| {
                            let store = &state.ca.as_ref().expect("CA role held").store;
                            match store.status(&id).await {
                                Ok(ca_store::Status::Pending(_)) => {
                                    (PollResponse::Pending, None)
                                }
                                Ok(ca_store::Status::Signed(s)) => {
                                    let repush = match store.read_issued(&id).await {
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
                            }}).await
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
                let resp = match ca_dir(state).await {
                    None => ListQueueResponse::Err {
                        reason: "this host does not hold the CA".to_string(),
                    },
                    Some(_) => handle_list_queue(state, &req).await,
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ListQueueResponse")
            }
            Request::Approve(req) => {
                let resp = match ca_dir(state).await {
                    None => ApproveResponse::Err {
                        reason: "this host does not hold the CA".to_string(),
                    },
                    Some(_) => {
                        // The write transaction commits the issuance before we run
                        // its best-effort side effects.
                        let signed = state
                            .write_async(async move |state| {
                                let map = state.map.clone();
                                handle_approve(
                                    state.ca.as_mut().expect("CA role held"),
                                    &req,
                                    Some(&map),
                                )
                                .await
                            })
                            .await;
                        match signed {
                            Err(reason) => ApproveResponse::Err { reason },
                            Ok(Approved {
                                resp: SignResponse::Err { reason }, ..
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
                                replacement_crl,
                                ..
                            }) => {
                                // The cert is issued and committed atomically in
                                // the signing task; these are best-effort side
                                // effects (the enrollee polls the cert back
                                // regardless). Push-registration warnings go to
                                // the approving admin's reply only.
                                let mut operation_id = if let Some(plan) = push {
                                    let (operation_id, push_warnings) =
                                        push_registrations(state, &plan).await;
                                    warnings.extend(push_warnings);
                                    Some(operation_id)
                                } else {
                                    None
                                };
                                if let Some(crl) = replacement_crl {
                                    let op = operation_id
                                        .unwrap_or_else(admin_proto::OperationId::new);
                                    for result in push_crl_to_peers(state, &crl, op).await
                                    {
                                        if let Some(error) = result.error {
                                            warnings.push(format!(
                                                "CRL push to {} at {}: {error}",
                                                result.server, result.addr
                                            ));
                                        }
                                    }
                                    operation_id = Some(op);
                                }
                                // An approved enrollment makes the new admin
                                // server a peer — same side effect as the
                                // synchronous Enroll, deferred to approval.
                                if let Some((server_id, enrollment)) = enrollment {
                                    let grant = async {
                                        grant_enrollment(state, server_id, &enrollment)
                                            .await?;
                                        record_peer(state, enrollment.listen).await;
                                        Ok::<(), anyhow::Error>(())
                                    }
                                    .await;
                                    match grant {
                                        Ok(()) => {}
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
                let resp = match ca_dir(state).await {
                    None => DenyResponse::Err {
                        reason: "this host does not hold the CA".to_string(),
                    },
                    Some(_) => {
                        state
                            .write_async(async move |state| {
                                let map = state.map.clone();
                                handle_deny(
                                    state.ca.as_mut().expect("CA role held"),
                                    &req,
                                    Some(&map),
                                )
                                .await
                            })
                            .await
                    }
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing DenyResponse")
            }
            Request::Revoke(req) => {
                let resp = match ca_dir(state).await {
                    None => RevokeResponse::Err {
                        reason: "this host does not hold the CA".to_string(),
                    },
                    Some(_) => handle_revoke(state, &req).await,
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing RevokeResponse")
            }
            Request::ListIssued(req) => {
                let mut records = match state
                    .write_async(async move |state| {
                        let Some(ca) = state.ca.as_mut() else {
                            return Err("this host does not hold the CA".to_string());
                        };
                        start_list_issued(ca, &req).await
                    })
                    .await
                {
                    Ok(records) => records,
                    Err(reason) => {
                        return admin_proto::write_msg(
                            &mut tls,
                            &ListIssuedResponse::Err { reason },
                        )
                        .await
                        .context("writing ListIssuedResponse");
                    }
                };
                let now = ca_store::now_unix();
                loop {
                    match records.next().await {
                        Ok(Some(record)) if record.not_after_unix > now => {
                            let entry = IssuedEntry {
                                serial: record.serial,
                                name: record.name,
                                spki_fp: record.spki_fp,
                                not_after_unix: record.not_after_unix,
                                revoked: record.revoked.is_some(),
                            };
                            admin_proto::write_msg_unflushed(
                                &mut tls,
                                &ListIssuedResponse::Entry { entry },
                            )
                            .await
                            .context("writing ListIssuedResponse entry")?;
                        }
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            break admin_proto::write_msg(
                                &mut tls,
                                &ListIssuedResponse::End,
                            )
                            .await
                            .context("writing ListIssuedResponse end");
                        }
                        Err(e) => {
                            break admin_proto::write_msg(
                                &mut tls,
                                &ListIssuedResponse::Err {
                                    reason: format!("listing issued certs: {e:#}"),
                                },
                            )
                            .await
                            .context("writing ListIssuedResponse error");
                        }
                    }
                }
            }
            Request::RequestDelegation(req) => {
                let resp = match ca_dir(state).await {
                    None => DelegationResponse::Err {
                        reason: "this host does not hold the CA".to_string(),
                    },
                    Some(_) => handle_request_delegation(state, &req, peer).await,
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing DelegationResponse")
            }
            Request::PollDelegation(req) => {
                let resp = match ca_dir(state).await {
                    None => DelegationPollResponse::Unknown,
                    Some(dir) => handle_poll_delegation(&dir, &req).await,
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing DelegationPollResponse")
            }
            Request::ListDelegations(req) => {
                let resp = match ca_dir(state).await {
                    None => ListDelegationsResponse::Err {
                        reason: "this host does not hold the CA".to_string(),
                    },
                    Some(_) => handle_list_delegations(state, &req).await,
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ListDelegationsResponse")
            }
            Request::ApproveDelegation(req) => {
                let resp = handle_approve_delegation(state, req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ApproveDelegationResponse")
            }
            Request::DenyDelegation(req) => {
                let resp = handle_deny_delegation(state, &req).await;
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
                    handle_apply_referral_edit(state, &req).await
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ApplyReferralEditResponse")
            }
            Request::GetCrl => {
                let resp = match ca_dir(state).await {
                    None => GetCrlResponse { crl_pem: None },
                    Some(_) => {
                        let path = state
                            .read(move |state| {
                                state.ca.as_ref().expect("CA role held").store.crl_path()
                            })
                            .await;
                        match tokio::fs::read_to_string(path).await {
                            Ok(pem) => GetCrlResponse { crl_pem: Some(pem) },
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                GetCrlResponse { crl_pem: None }
                            }
                            Err(e) => {
                                warn!("admin-server: reading the CRL failed: {e:#}");
                                GetCrlResponse { crl_pem: None }
                            }
                        }
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
                    let server_id = peer_admin.expect("NodeSelf authorized").server_id;
                    handle_register(state, server_id, &req).await
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
                    let server_id = peer_admin.expect("NodeSelf authorized").server_id;
                    handle_deregister(state, server_id).await
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing RegisterResponse")
            }
            Request::GetMapVersion => {
                let resp = GetMapVersionResponse::Ok {
                    version: state.read(move |state| state.map.version).await,
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing GetMapVersionResponse")
            }
            Request::GetMap => {
                let resp = GetMapResponse::Ok {
                    map: state.read(move |state| state.map.clone()).await,
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing GetMapResponse")
            }
            Request::RemoveServer(req) => {
                let resp = handle_remove_server(state, req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing RemoveServerResponse")
            }
            Request::ReadPerms(req) => {
                let resp = handle_read_perms(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ReadPermsResponse")
            }
            Request::GetPerms => {
                let resp = handle_get_perms(state).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing GetPermsResponse")
            }
            Request::EditPerms(req) => {
                let resp = handle_edit_perms(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing EditPermsResponse")
            }
            Request::ApplyPermsEdit(req) => {
                let resp = if !peer_is_admin_server {
                    ApplyPermsEditResponse::Err {
                        reason: "a perms edit requires a admin-server peer certificate"
                            .to_string(),
                    }
                } else {
                    handle_apply_perms_edit(state, &req).await
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
                    handle_apply_crl(state, &req).await
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ApplyCrlResponse")
            }
            Request::ApplyControllerState(req) => {
                let resp = if !peer_is_controller {
                    ApplyControllerStateResponse::Err {
                        reason:
                            "controller-state reconciliation requires the exact home CA \
                                     controller certificate"
                                .to_string(),
                    }
                } else {
                    handle_apply_controller_state(state, &req).await
                };
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ApplyControllerStateResponse")
            }
            Request::ReconcileController(req) => {
                let resp = handle_reconcile_controller(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ReconcileControllerResponse")
            }
            Request::AddRoleAdmin(req) => {
                let prepared =
                    match REQUEST_AUTHENTICATION.try_with(Clone::clone).unwrap_or(None) {
                        Some(Err(reason)) => Err(reason),
                        _ => prepare_role_slot(state, &signs, &req).await,
                    };
                let resp =
                    handle_add_role_admin_prepared(state, &req, local, prepared).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing AdminMgmtResponse")
            }
            Request::SetAdminPolicy(req) => {
                let resp = handle_set_admin_policy(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing AdminMgmtResponse")
            }
            Request::RemoveAdmin(req) => {
                let resp = handle_remove_admin(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing AdminMgmtResponse")
            }
            Request::ListAdmins(req) => {
                let resp = handle_list_admins(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing AdminListResponse")
            }
            Request::ControlService(req) => {
                let resp = handle_control_service(state, &req).await;
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
                let resp = rotate_recovery(state, &signs, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing RotateRecoveryResponse")
            }
            Request::RotateAutorenew => {
                let resp = rotate_autorenew(state, &signs, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing RotateAutorenewResponse")
            }
            Request::Backup(req) => {
                let resp = handle_backup(state, &req).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing BackupResponse")
            }
            Request::ExternalCaCsr => {
                let resp = handle_external_ca_csr(state, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ExternalCaCsrResponse")
            }
            Request::ExternalCaInstall(req) => {
                let resp = handle_external_ca_install(state, &req, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing ExternalCaInstallResponse")
            }
            Request::CaStatus => {
                let resp = handle_ca_status(state, local).await;
                admin_proto::write_msg(&mut tls, &resp)
                    .await
                    .context("writing CaStatusResponse")
            }
        }
    };
    let request = REQUEST_SERVER_UNLOCK.scope(unlock, request);
    let result = REQUEST_AUTHENTICATION.scope(authentication, request).await;
    drop(password_attempt);
    result
}

async fn handle_ca_status(state: &Server, local: bool) -> admin_proto::CaStatusResponse {
    use admin_proto::CaStatusResponse;
    if !local {
        return CaStatusResponse::Err {
            reason: "CA status is local-control-only".to_string(),
        };
    }
    let snapshot = state
        .read(move |state| {
            let ca = state.ca.as_ref().context("this host does not hold the CA")?;
            let names = ca.vault.signing_slot_names()?;
            Ok::<_, anyhow::Error>((
                ca.dir().to_path_buf(),
                ca.lifetimes.externally_signed,
                names.iter().any(|name| name == AUTORENEW_ADMIN),
                names.iter().any(|name| name == ca_vault::RECOVERY_ADMIN),
            ))
        })
        .await;
    let (dir, externally_signed, autorenew_slot_present, recovery_slot_present) =
        match snapshot {
            Ok(snapshot) => snapshot,
            Err(e) => return CaStatusResponse::Err { reason: format!("{e:#}") },
        };
    let cert_installed = match tokio::fs::try_exists(dir.join("certificate.pem")).await {
        Ok(installed) => installed,
        Err(e) => {
            return CaStatusResponse::Err {
                reason: format!("checking the CA certificate: {e}"),
            };
        }
    };
    let pending = if cert_installed {
        None
    } else {
        crate::admin_ops::slots::ExternalPending::load_async(&dir)
            .await
            .ok()
            .map(|pending| (pending.cn, pending.domain))
    };
    CaStatusResponse::Ok {
        status: admin_proto::CaStatus {
            autorenew_slot_present,
            recovery_slot_present,
            externally_signed,
            cert_installed,
            pending,
        },
    }
}

async fn handle_external_ca_csr(state: &Server, local: bool) -> ExternalCaCsrResponse {
    state
        .write_async(async move |state| handle_external_ca_csr_inner(state, local).await)
        .await
}

async fn handle_external_ca_csr_inner(
    state: &mut MutableState,
    local: bool,
) -> ExternalCaCsrResponse {
    let err = |reason: String| ExternalCaCsrResponse::Err { reason };
    if !local {
        return err("external-CA CSR emission is local-control-only".to_string());
    }
    let Some(ca) = state.ca.as_mut() else {
        return err("this host is not the controller CA".to_string());
    };
    let dir = ca.dir().to_path_buf();
    let signing = match server_unlock(ca).await {
        Ok(s) => s,
        Err(e) => return err(e),
    };
    match crate::admin_ops::slots::external_csr_with_key(&dir, &signing.ca_key_pem).await
    {
        Ok((common_name, csr)) => match String::from_utf8(csr) {
            Ok(csr_pem) => {
                audit(&dir, "local", "external-ca-csr", &common_name, Duration::ZERO)
                    .await;
                ExternalCaCsrResponse::Ok { common_name, csr_pem }
            }
            Err(e) => err(format!("encoding the generated CSR: {e}")),
        },
        Err(e) => err(format!("generating the external-CA CSR: {e:#}")),
    }
}

async fn handle_external_ca_install(
    state: &Server,
    req: &ExternalCaInstallRequest,
    local: bool,
) -> ExternalCaInstallResponse {
    let req = req.clone();
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |state| {
            handle_external_ca_install_inner(&config_lock, state, &req, local).await
        })
        .await
}

async fn handle_external_ca_install_inner(
    config_lock: &ConfigDirLock,
    state: &mut MutableState,
    req: &ExternalCaInstallRequest,
    local: bool,
) -> ExternalCaInstallResponse {
    let err = |reason: String| ExternalCaInstallResponse::Err { reason };
    if !local {
        return err("external-CA certificate installation is local-control-only".into());
    }
    let Some(ca) = state.ca.as_mut() else {
        return err("this host is not the controller CA".into());
    };
    let dir = match config_lock.require_contained(ca.dir()) {
        Ok(dir) => dir,
        Err(e) => return err(format!("{e:#}")),
    };
    match crate::ca::CaLifetimes::load_async(&dir).await {
        Ok(l) if l.externally_signed => {}
        Ok(_) => return err("this controller CA is not externally signed".into()),
        Err(e) => return err(format!("reading CA lifetime policy: {e:#}")),
    }
    let signing = match server_unlock(ca).await {
        Ok(s) => s,
        Err(e) => return err(e),
    };
    let signed_cert_pem = req.signed_cert_pem.as_bytes().to_vec();
    let root_pem = req.root_pem.as_ref().map(|root| root.as_bytes().to_vec());
    let ca_key_pem = signing.ca_key_pem.to_vec();
    let validated = tokio::task::spawn_blocking(move || {
        crate::ca::validate_external_ca_cert(
            &signed_cert_pem,
            root_pem.as_deref(),
            &ca_key_pem,
        )
    })
    .await;
    let (intermediate, root) = match validated {
        Err(e) => return err(format!("external-CA validation task panicked: {e}")),
        Ok(Err(e)) => {
            return err(format!("validating the signed CA certificate: {e:#}"));
        }
        Ok(Ok(v)) => v,
    };
    let (trusted_path, serving_path) = match (
        config_lock.require_contained(&state.cfg.trusted),
        config_lock.require_contained(&state.cfg.serving_cert),
    ) {
        (Ok(trusted), Ok(serving)) => (trusted, serving),
        (Err(e), _) | (_, Err(e)) => return err(format!("{e:#}")),
    };
    // A live renewal may refresh the netidx intermediate and its existing
    // issuer, but it may not smuggle in a new trust root. The ordinary renewal
    // reconciler implements exactly that same-key/same-issuer rule.
    let installed = match tokio::fs::read_to_string(&trusted_path).await {
        Ok(p) => p,
        Err(e) => return err(format!("reading {}: {e:#}", trusted_path.display())),
    };
    let candidate = format!(
        "{}{}",
        String::from_utf8_lossy(&root),
        String::from_utf8_lossy(&intermediate)
    );
    let reconciled = match admin_client::reconcile_trusted_bundle(&installed, &candidate)
    {
        Ok(p) => p,
        Err(e) => return err(format!("reconciling the existing trust bundle: {e:#}")),
    };
    let new_fp = match crate::fingerprint::Fingerprint::of_cert_pem(&intermediate) {
        Ok(fp) => fp,
        Err(e) => return err(format!("fingerprinting the signed CA certificate: {e:#}")),
    };
    let new_der = match rustls_pemfile::certs(&mut std::io::Cursor::new(&intermediate))
        .next()
        .transpose()
    {
        Ok(Some(d)) => d,
        Ok(None) => return err("the signed CA certificate is empty".into()),
        Err(e) => return err(format!("parsing the signed CA certificate: {e}")),
    };
    let accepted =
        rustls_pemfile::certs(&mut std::io::Cursor::new(reconciled.as_bytes()))
            .flatten()
            .any(|d| d.as_ref() == new_der.as_ref());
    if !accepted {
        return err(
            "the renewed CA certificate was not signed by the controller's already-pinned external issuer"
                .into(),
        );
    }
    let serving = match tokio::fs::read_to_string(&serving_path).await {
        Ok(p) => p,
        Err(e) => return err(format!("reading {}: {e:#}", serving_path.display())),
    };
    let marker = "-----END CERTIFICATE-----";
    let Some(end) = serving.find(marker).map(|i| i + marker.len()) else {
        return err(format!(
            "{} contains no serving certificate",
            serving_path.display()
        ));
    };
    let mut refreshed_chain = serving[..end].to_string();
    refreshed_chain.push('\n');
    refreshed_chain.push_str(&String::from_utf8_lossy(&intermediate));
    if let Err(e) = crate::atomic::write_atomic_async(
        &dir.join("certificate.pem"),
        &intermediate,
        0o644,
    )
    .await
    {
        return err(format!("installing the renewed CA certificate: {e:#}"));
    }
    if let Err(e) =
        crate::atomic::write_atomic_async(&trusted_path, reconciled.as_bytes(), 0o644)
            .await
    {
        return err(format!("installing the reconciled trust bundle: {e:#}"));
    }
    if let Err(e) = crate::atomic::write_atomic_async(
        &serving_path,
        refreshed_chain.as_bytes(),
        0o644,
    )
    .await
    {
        return err(format!("refreshing the controller serving chain: {e:#}"));
    }
    audit(&dir, "local", "external-ca-install", &new_fp.text(), Duration::ZERO).await;
    ExternalCaInstallResponse::Ok { ca_fingerprint: new_fp.text() }
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

async fn ca_dir(state: &Server) -> Option<PathBuf> {
    state.ca_dir().await
}

async fn handle_backup(
    state: &Arc<Server>,
    req: &admin_proto::BackupRequest,
) -> BackupResponse {
    if !state.has_ca().await {
        return BackupResponse::Err {
            reason: "backup must be run on the CA controller".to_string(),
        };
    }
    let Some(cfg_path) = state.cfg_path.clone() else {
        return BackupResponse::Err {
            reason: "the running controller has no persistent config path".to_string(),
        };
    };
    let ca_dir = state.ca_dir().await.expect("CA role held");
    let target = PathBuf::from(&req.target);
    let unlocked = match state
        .write_async(async move |state| {
            server_unlock(state.ca.as_mut().expect("CA role held")).await
        })
        .await
    {
        Ok(unlocked) => unlocked,
        Err(reason) => return BackupResponse::Err { reason },
    };
    let capture_ca_dir = ca_dir.clone();
    let captured = state
        .read_async(async move |state| {
            Ok::<_, anyhow::Error>((
                state.cfg.clone(),
                state.map.version,
                state
                    .ca
                    .as_ref()
                    .expect("CA role held")
                    .store
                    .max_serial()
                    .await?
                    .unwrap_or(0),
            ))
        })
        .await;
    let (cfg, map_version, highest_serial) = match captured {
        Ok(captured) => captured,
        Err(e) => {
            return BackupResponse::Err {
                reason: format!("capturing backup state: {e:#}"),
            };
        }
    };
    let snapshot = match tokio::task::spawn_blocking(move || {
        crate::backup::capture(
            &cfg,
            &cfg_path,
            &capture_ca_dir,
            map_version,
            highest_serial,
            &unlocked.ca_key_pem,
        )
    })
    .await
    {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(e)) => {
            return BackupResponse::Err { reason: format!("capturing backup: {e:#}") };
        }
        Err(e) => {
            return BackupResponse::Err {
                reason: format!("backup capture task panicked: {e}"),
            };
        }
    };
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
    )
    .await;
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
async fn record_peer(state: &Server, peer: SocketAddr) {
    let cfg_path = state.cfg_path.clone();
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |inner| {
            let cfg = &mut inner.cfg;
            if peer == cfg.listen || cfg.peers.contains(&peer) {
                return;
            }
            cfg.peers.push(peer);
            if let Some(path) = &cfg_path
                && let Err(e) = cfg.save_async(&config_lock, path).await
            {
                warn!("admin-server: failed to persist enrolled peer {peer}: {e:#}");
            }
        })
        .await;
}

/// Local facts + known peers. Cheap and network-free: the resolver
/// address/auth is read fresh from the resolver config so config edits
/// show up without a daemon restart; everything else is our own config.
async fn get_info(state: &Server) -> GetInfoResponse {
    let cfg = state.read(move |state| state.cfg.clone()).await;
    let resolver = match cfg.roles.resolver.as_ref() {
        Some(role) => match resolver_info(&role.config).await {
            Ok(resolver) => resolver,
            Err(e) => {
                warn!(
                    "admin-server: could not derive resolver info from {}: {e:#}",
                    role.config.display()
                );
                None
            }
        },
        None => None,
    };
    GetInfoResponse {
        domain: cfg.domain,
        ca_addr: if cfg.roles.ca.is_some() { Some(cfg.listen) } else { cfg.ca_addr },
        resolver,
        peers: cfg.peers,
    }
}

/// Derive this host's advertised resolver address + data-plane auth
/// from its resolver config — the first advertisable (non-`Local`)
/// member, the representative `GetInfo` reports. See
/// [`ResolverConfig::resolver_addrs`](crate::resolver::ResolverConfig::resolver_addrs)
/// for the full cluster set (used by delegation).
async fn resolver_info(config: &Path) -> Result<Option<ResolverAddr>> {
    let rc = crate::resolver::ResolverConfig::load_async(config).await?;
    Ok(rc.resolver_addrs().into_iter().next())
}

async fn local_resolver_data(
    cfg: &AdminServerConfig,
) -> (Option<ResolverAddr>, Option<admin_proto::ClusterFacts>) {
    let Some(role) = cfg.roles.resolver.as_ref() else { return (None, None) };
    match crate::resolver::ResolverConfig::load_async(&role.config).await {
        Ok(config) => {
            (config.resolver_addrs().into_iter().next(), Some(config.cluster_facts()))
        }
        Err(e) => {
            warn!(
                "admin-server: deriving resolver facts from {}: {e:#}",
                role.config.display()
            );
            (None, None)
        }
    }
}

#[derive(Clone)]
struct IdentityPusher {
    controller: admin_proto::AdminServerId,
    client: admin_client::AuthenticatedPkiClient,
    home_ca: CertificateDer<'static>,
}

impl IdentityPusher {
    async fn new(state: &Server) -> Result<Self> {
        Ok(IdentityPusher {
            controller: state.read(move |state| state.map.controller).await,
            client: state.outbound_client().await?,
            home_ca: state.home_ca_der.clone(),
        })
    }

    async fn push(
        &self,
        server: admin_proto::AdminServerId,
        addr: SocketAddr,
        req: &AddIdentityRequest,
    ) -> Result<Option<u32>> {
        tokio::time::timeout(
            PUSH_TIMEOUT,
            admin_client::push_identity(
                &self.client,
                addr,
                server,
                server == self.controller,
                self.home_ca.clone(),
                req,
            ),
        )
        .await
        .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))?
    }
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
    if let Some(dir) = state.ca_dir().await {
        audit(
            &dir,
            "controller",
            "fanout-id-map",
            &format!("operation {operation_id}: {}", plan.name),
            Duration::ZERO,
        )
        .await;
    }
    let req = AddIdentityRequest {
        operation_id,
        san: plan.name.clone(),
        primary_group: primary.clone(),
        groups: secondary.to_vec(),
    };
    let mut warnings = Vec::new();
    let (my_id, targets) = state
        .read(move |state| {
            let mut targets: Vec<_> = state
                .map
                .servers
                .iter()
                .filter(|s| {
                    s.roles.contains(&Role::IdMap)
                        && s.state == admin_proto::ServerState::Registered
                })
                .map(|s| (s.id, s.addr))
                .collect();
            targets.sort_by_key(|(id, _)| *id);
            (state.cfg.server_id, targets)
        })
        .await;
    // Local id-map first (no TLS loopback).
    if targets.iter().any(|(id, _)| *id == my_id) {
        match state.add_identity(&req).await {
            AddIdentityResponse::Ok { .. } => (),
            AddIdentityResponse::Err { reason } => {
                warnings.push(format!("local id-map registration failed: {reason}"))
            }
        }
    }
    let pusher = match IdentityPusher::new(state).await {
        Ok(pusher) => pusher,
        Err(e) => {
            warnings.push(format!("loading outbound identity failed: {e:#}"));
            return (operation_id, warnings);
        }
    };
    let mut results: Vec<_> = stream::iter(
        targets.into_iter().filter(|(id, _)| *id != my_id).map(|(id, addr)| {
            let req = req.clone();
            let pusher = pusher.clone();
            async move {
                let result = pusher.push(id, addr, &req).await;
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
    // is serialized with `handle_revoke` by the state write lock, so the two
    // can't clobber each other's field on the same record.
    if warnings.is_empty() {
        let id = plan.id.clone();
        state
            .write_async(async move |state| {
                if let Some(ca) = state.ca.as_mut() {
                    let _ = ca.store.set_push_done(&id).await;
                }
            })
            .await;
    }
    (operation_id, warnings)
}

fn reconcile_identity_at(
    records: &[ca_store::IssuedRecord],
    index: usize,
    now: u64,
) -> bool {
    let record = &records[index];
    !record.groups.is_empty()
        && records.iter().any(|candidate| {
            candidate.name.eq_ignore_ascii_case(&record.name) && candidate.live(now)
        })
        && !records[index + 1..].iter().any(|candidate| {
            candidate.name.eq_ignore_ascii_case(&record.name)
                && !candidate.groups.is_empty()
        })
}

async fn reconcile_identities_to_target(
    state: &Server,
    server: admin_proto::AdminServerId,
    addr: SocketAddr,
) -> Result<()> {
    let mut records = state
        .read_async(async move |state| {
            state
                .ca
                .as_ref()
                .context("this host does not hold the CA")?
                .store
                .list_signed()
                .await
        })
        .await?;
    records.sort_by_key(|record| record.serial);
    let now = ca_store::now_unix();
    let operation_id = admin_proto::OperationId::new();
    let pusher = IdentityPusher::new(state).await?;
    let mut audited = false;
    for (index, record) in records.iter().enumerate() {
        if !reconcile_identity_at(&records, index, now) {
            continue;
        }
        let (primary, secondary) = record
            .groups
            .split_first()
            .expect("reconciliation predicate requires groups");
        if !audited {
            if let Some(dir) = state.ca_dir().await {
                audit(
                    &dir,
                    "controller",
                    "reconcile-id-map",
                    &format!("operation {operation_id}: server {server} at {addr}"),
                    Duration::ZERO,
                )
                .await;
            }
            audited = true;
        }
        let req = AddIdentityRequest {
            operation_id,
            san: record.name.clone(),
            primary_group: primary.clone(),
            groups: secondary.to_vec(),
        };
        match pusher.push(server, addr, &req).await? {
            Some(uid) => info!(
                "admin-server: reconciled {} (uid {uid}) on server {server} at {addr}",
                record.name
            ),
            None => bail!(
                "server {server} at {addr} was granted IdMap but does not advertise that role"
            ),
        }
    }
    Ok(())
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
async fn load_serving_keypair(
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
/// expired — taking the admin plane down network-wide. Now a long-running
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

/// A sign outcome: the wire response plus, on success, what the id-map
/// push fan-out needs.
pub struct Signed {
    pub resp: SignResponse,
    /// `Some` only when the sign succeeded *and* the request asked for
    /// id-map registration: the issued name and the admin's chosen
    /// (policy-validated) groups.
    pub push: Option<PushPlan>,
    /// A restore replacement revoked its old machine certificate and produced
    /// this fresh CRL for immediate fanout.
    pub replacement_crl: Option<String>,
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
    ca: &mut ca_store::CaDir,
    credential: &admin_proto::AdminCredential,
) -> std::result::Result<ca_vault::Authenticated, String> {
    match credential {
        admin_proto::AdminCredential::Password { .. } => prepared_authentication(ca)
            .unwrap_or_else(|| Err("authentication failed".to_string())),
        admin_proto::AdminCredential::Session { token } => {
            ca.sessions.authenticate_session(&ca.vault, token)
        }
    }
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
/// when the CA holds no autorenew credential (read-only CA). The caller
/// prepares the key from a vault snapshot on the bounded blocking pool; this
/// function revalidates the slot revision against the live vault.
async fn server_unlock(
    ca: &mut ca_store::CaDir,
) -> std::result::Result<TArc<ca_vault::Unlocked>, String> {
    let result = REQUEST_SERVER_UNLOCK
        .try_with(Clone::clone)
        .unwrap_or(None)
        .ok_or_else(|| "the CA key was not prepared for this operation".to_string())?;
    let unlocked = result?;
    let current = ca
        .vault
        .resolve_session_slot(unlocked.slot_id, unlocked.credential_revision)
        .map_err(|_| {
            "the CA's autorenew credential changed while its key was unlocking"
                .to_string()
        })?;
    if current.admin != AUTORENEW_ADMIN || current.kind != ca_vault::SlotKind::Signing {
        return Err("the prepared CA key did not come from the autorenew slot".into());
    }
    match ca.store.refresh_crl_if_stale(&unlocked.ca_key_pem).await {
        Ok(true) => info!("admin-server: created or refreshed the CRL"),
        Ok(false) => (),
        Err(e) => warn!("admin-server: opportunistic CRL refresh failed: {e:#}"),
    }
    Ok(unlocked)
}

/// Handle a sign request against the CA rooted at `ca_dir`. Auth and
/// policy failures become a `SignResponse::Err` carrying a safe reason
/// for the client; only an internal fault (e.g. the CA cert can't be
/// read) maps to a generic error response — never a panic.
pub async fn handle_sign_request(ca: &mut ca_store::CaDir, req: &SignRequest) -> Signed {
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
    handle_sign_request_op(ca, req, "sign", &record_req, None).await
}

/// [`handle_sign_request`] with the audit-log operation name, the
/// record's originating request, and (for the approve path) the id to
/// re-check while the write transaction still owns the CA. The approve path
/// signs through the identical checks but audits as `op=approve` and keys
/// the record by the *queued* request id.
async fn handle_sign_request_op(
    ca: &mut ca_store::CaDir,
    req: &SignRequest,
    op: &str,
    record_req: &ca_store::QueuedReq,
    recheck_id: Option<&str>,
) -> Signed {
    match try_handle(ca, req, op, record_req, recheck_id).await {
        Ok(signed) => signed,
        Err(e) => Signed {
            resp: SignResponse::Err { reason: format!("internal error: {e:#}") },
            push: None,
            replacement_crl: None,
        },
    }
}

async fn try_handle(
    ca: &mut ca_store::CaDir,
    req: &SignRequest,
    op: &str,
    record_req: &ca_store::QueuedReq,
    recheck_id: Option<&str>,
) -> Result<Signed> {
    let failed = |resp: SignResponse| Signed { resp, push: None, replacement_crl: None };
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
    let signing = match server_unlock(ca).await {
        Ok(u) => u,
        Err(reason) => return Ok(failed(reject(&reason))),
    };
    // The one-live-cert check, serial allocation, sign, and atomic record
    // commit are one write transaction.
    issue_serialized(
        ca,
        &signing,
        &authd.admin,
        record_req,
        name,
        validity,
        groups,
        one_live_name(record_req.kind),
        None,
        req.replaces_serial,
        None,
        op,
        recheck_id,
    )
    .await
}

/// The serialized issuance core, shared by sign / approve / enroll / renewal.
/// The state write lock keeps exclusive access across the one-live scan, serial
/// allocation, sign, and the single atomic `commit_signed` — so the
/// one-live invariant holds and the issuance is committed by one write.
/// `one_live` is false for serving certs / verified renewals, where
/// multiple live certs for a name are legitimate.
#[allow(clippy::too_many_arguments)]
async fn issue_serialized(
    ca: &mut ca_store::CaDir,
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
    // between then and now — so we re-check it is still live in this
    // write transaction, and refuse the renewal if it isn't. `None` for any
    // non-renewal issuance.
    renewal_of: Option<u64>,
    // Restore replacement approved for this exact old serial.
    replacement_of: Option<u64>,
    serving_identity: Option<crate::tls::AdminCertIdentity>,
    audit_op: &str,
    // For the approve path: re-check the queue entry is still Pending
    // in the same write transaction, so two approvals (or an approve racing a deny) can't
    // both transition it. `None` for direct (non-queued) issuance.
    recheck_id: Option<&str>,
) -> Result<Signed> {
    let dir = ca.dir().to_path_buf();
    let config_lock = ca.config_lock();
    let store = &mut ca.store;
    if let Some(id) = recheck_id {
        match store.status(id).await {
            Ok(ca_store::Status::Pending(_)) => {}
            Ok(ca_store::Status::Signed(_)) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "that request was already approved".to_string(),
                    },
                    push: None,
                    replacement_crl: None,
                });
            }
            Ok(ca_store::Status::Denied(_)) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "that request was already denied".to_string(),
                    },
                    push: None,
                    replacement_crl: None,
                });
            }
            Ok(ca_store::Status::Unknown) => {
                return Ok(Signed {
                    resp: SignResponse::Err {
                        reason: "no such pending request (expired or never queued)"
                            .to_string(),
                    },
                    push: None,
                    replacement_crl: None,
                });
            }
            Err(e) => return Err(e).context("re-checking the queue before issuance"),
        }
    }
    let replacement_record = if let Some(serial) = replacement_of {
        let records = store.list_signed().await.context("checking replacement serial")?;
        match records.into_iter().find(|record| record.serial == serial) {
            Some(record)
                if record.name.eq_ignore_ascii_case(name)
                    && restore_kind_matches(record.req.kind, record_req.kind)
                    && record.live(ca_store::now_unix()) =>
            {
                Some(record)
            }
            Some(record)
                if record.name.eq_ignore_ascii_case(name)
                    && restore_kind_matches(record.req.kind, record_req.kind) =>
            {
                // An interrupted/manual recovery may already have revoked or
                // outlived the exact old cert. Issuing is now an ordinary
                // one-live-checked enrollment, so there is nothing left to
                // revoke a second time.
                None
            }
            Some(record) => {
                return Ok(Signed {
                    resp: reject(&format!(
                        "replacement serial {serial} belongs to {:?} {:?}, not {:?} {:?}",
                        record.req.kind, record.name, record_req.kind, name
                    )),
                    push: None,
                    replacement_crl: None,
                });
            }
            None => {
                return Ok(Signed {
                    resp: reject(&format!(
                        "replacement serial {serial} is not a live certificate for {name:?}"
                    )),
                    push: None,
                    replacement_crl: None,
                });
            }
        }
    } else {
        None
    };
    if one_live {
        let live =
            store.live_for_name(name).await.context("checking the issuance index")?;
        if live.iter().any(|record| Some(record.serial) != replacement_of) {
            return Ok(Signed {
                resp: reject(&one_live_refusal(name, &live)),
                push: None,
                replacement_crl: None,
            });
        }
    }
    // A verified renewal is a continuation of an identity that was live at
    // enqueue. Re-validate before committing that the cert it renews is still
    // live: if it was revoked (or expired) in between, the renewal must NOT
    // re-mint it — otherwise revocation, the only containment tool, could be
    // outrun by an in-flight renewal (worst case re-minting the serving
    // SAN). Refusing here also covers the auto-renew sweep, which signs
    // through this same path.
    if let Some(serial) = renewal_of {
        let live =
            store.live_for_name(name).await.context("checking the issuance index")?;
        if !live.iter().any(|r| r.serial == serial) {
            return Ok(Signed {
                resp: reject(&format!(
                    "the certificate being renewed (serial {serial} for {name:?}) is no \
                     longer live — it may have been revoked or expired; this renewal is \
                     refused"
                )),
                push: None,
                replacement_crl: None,
            });
        }
    }
    // Opportunistic CA-cert renewal — rare, and only allocates a serial
    // when actually renewing, so the common path burns nothing. An
    // externally-signed CA cert cannot be self-renewed (netidx doesn't
    // hold the external issuer's key); warn instead so the operator
    // re-signs out of band. (Warning fires only within the renewal
    // window; a rate limit could reduce it further if it proves noisy.)
    let renewal_dir = dir.clone();
    let renewal_threshold = ca.lifetimes.ca_renew_threshold;
    let needs_renewal = tokio::task::spawn_blocking(move || {
        crate::ca::ca_cert_needs_renewal(&renewal_dir, renewal_threshold)
    })
    .await
    .unwrap_or(false);
    if needs_renewal {
        if ca.lifetimes.externally_signed {
            warn!(
                "admin-server: the externally-signed CA certificate is within its \
                 renewal threshold and will NOT auto-renew — obtain a re-signed \
                 cert from your PKI and run `netidx admin ca external install`"
            );
        } else {
            let rs = store.alloc_serial();
            let renewal_dir = dir.clone();
            let config_lock = config_lock.clone();
            let ca_key_pem = signing.ca_key_pem.to_vec();
            let renewed = tokio::task::spawn_blocking(move || {
                crate::ca::maybe_renew_ca_cert(
                    &config_lock,
                    &renewal_dir,
                    &ca_key_pem,
                    rs,
                    renewal_threshold,
                )
            })
            .await;
            match renewed {
                Err(e) => warn!("admin-server: CA renewal task panicked: {e}"),
                Ok(result) => match result {
                    Ok(true) => info!(
                        "admin-server: renewed the CA certificate (same key; glyph unchanged)"
                    ),
                    Ok(false) => (),
                    Err(e) => warn!("admin-server: CA renewal check failed: {e:#}"),
                },
            }
        }
    }
    let serial = store.alloc_serial();
    let serving_identity = match (serving_identity, renewal_of) {
        (Some(identity), _) => Some(identity),
        (None, Some(serial)) if name.eq_ignore_ascii_case(SERVING_SAN) => store
            .live_for_name(name)
            .await
            .ok()
            .and_then(|records| records.into_iter().find(|r| r.serial == serial))
            .and_then(|record| {
                crate::tls::admin_cert_identity_from_pem(record.cert_pem.as_bytes()).ok()
            }),
        (None, _) => None,
    };
    if renewal_of.is_some()
        && name.eq_ignore_ascii_case(SERVING_SAN)
        && serving_identity.is_none()
    {
        return Ok(Signed {
            resp: reject(
                "the serving certificate being renewed has no valid protocol-v6 identity",
            ),
            push: None,
            replacement_crl: None,
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
        store,
        &signing.ca_key_pem,
        &record_req.csr_pem,
        &san,
        validity,
        serial,
    )
    .await?;
    let mut replacement_crl = None;
    if let SignResponse::Ok { ref signed_cert_pem, .. } = resp {
        store
            .commit_issuance(record_req, serial, name, signed_cert_pem, &groups)
            .await
            .context("committing the issuance")?;
        if let Some(old) = replacement_record {
            let revoked = store
                .revoke(
                    old.serial,
                    ca_store::Revocation {
                        serial: old.serial,
                        revoked_unix: ca_store::now_unix(),
                        reason: format!("replaced during restore by serial {serial}"),
                    },
                )
                .await?;
            anyhow::ensure!(revoked, "replacement certificate stopped being live");
            store.write_crl(&signing.ca_key_pem).await?;
            replacement_crl = Some(tokio::fs::read_to_string(store.crl_path()).await?);
        }
    }
    audit(&dir, audit_admin, audit_op, name, validity).await;
    Ok(Signed {
        resp,
        push: if groups.is_empty() {
            None
        } else {
            Some(PushPlan { id: record_req.id.clone(), name: name.to_string(), groups })
        },
        replacement_crl,
    })
}

/// Handle a admin-server enrollment: authenticate the admin, require the
/// scoped enrollment policy, and sign the CSR with the reserved
/// [`SERVING_SAN`].
pub async fn handle_enroll_request(
    ca: &mut ca_store::CaDir,
    req: &EnrollRequest,
    local: bool,
    map: Option<&NetworkMap>,
) -> SignResponse {
    match try_enroll(ca, req, local, map).await {
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

async fn try_enroll(
    ca: &mut ca_store::CaDir,
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
            replaces: req.replaces,
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
            replaces: req.replaces,
        }),
    );
    let signing = match server_unlock(ca).await {
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
            replaces: req.replaces,
        };
        let Some(map) = map else {
            return Ok(reject("the CA-owned network map is unavailable"));
        };
        let mut staged = map.clone();
        if let Err(e) = stage_enrollment(&mut staged, identity.server_id, &enrollment) {
            return Ok(reject(&format!("invalid enrollment grant: {e:#}")));
        }
    }
    let signed = issue_serialized(
        ca,
        &signing,
        &authd.admin,
        &record_req,
        SERVING_SAN,
        ca.lifetimes.leaf_validity,
        Vec::new(),
        false,
        None,
        None,
        Some(identity),
        "enroll",
        None,
    )
    .await?;
    Ok(signed.resp)
}

/// The shared signing tail of every issuance path: build a transient
/// [`Ca`] from the decrypted key, sign the CSR for exactly `name` with
/// the caller-allocated `serial`, and bundle the trust anchors.
async fn sign_csr(
    dir: &Path,
    store: &ca_store::CAStore,
    ca_key_pem: &[u8],
    csr_pem: &str,
    san: &[SanEntry],
    validity: Duration,
    serial: u64,
) -> Result<SignResponse> {
    let cert_pem = tokio::fs::read(dir.join("certificate.pem"))
        .await
        .context("reading CA certificate")?;
    let dir = dir.to_path_buf();
    let ca_key_pem = ca_key_pem.to_vec();
    let csr_pem = csr_pem.as_bytes().to_vec();
    let san = san.to_vec();
    let signed = tokio::task::spawn_blocking(move || {
        let ca =
            Ca::from_pem(dir, &ca_key_pem, &cert_pem).context("loading CA from vault")?;
        ca.sign_request(&csr_pem, &san, validity, serial).context("signing CSR")
    })
    .await
    .context("CA signing task panicked")??;
    let trusted_pem = store.read_trusted_bundle().await?;
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
/// yet.
pub async fn handle_add_identity(
    config_lock: &ConfigDirLock,
    map_path: &Path,
    req: &AddIdentityRequest,
) -> AddIdentityResponse {
    info!(
        "admin-server: applying id-map registration operation {} for {:?}",
        req.operation_id, req.san
    );
    let map_path = match config_lock.require_contained(map_path) {
        Ok(path) => path,
        Err(e) => return AddIdentityResponse::Err { reason: format!("{e:#}") },
    };
    let mut map = if tokio::fs::try_exists(&map_path).await.unwrap_or(false) {
        match id_map::load_async(&map_path).await {
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
        Ok(uid) => match id_map::save_async(&map_path, &map).await {
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
async fn handle_enqueue(
    ca: &mut ca_store::CaDir,
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
    let store = &mut ca.store;
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
        return match store.enqueue(&queued).await {
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
            match store.live_for_name(name).await {
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
    let replacement_of = if verified_renewal {
        None
    } else if let Some(serial) = req.replaces_serial {
        match store.list_signed().await {
            Ok(records) => {
                match records.into_iter().find(|record| record.serial == serial) {
                    Some(record)
                        if record.name.eq_ignore_ascii_case(name)
                            && restore_kind_matches(record.req.kind, req.kind)
                            && record.live(ca_store::now_unix()) =>
                    {
                        Some(serial)
                    }
                    Some(record)
                        if record.name.eq_ignore_ascii_case(name)
                            && restore_kind_matches(record.req.kind, req.kind) =>
                    {
                        None
                    }
                    Some(record) => {
                        return EnqueueResponse::Err {
                            reason: format!(
                                "replacement serial {serial} belongs to {:?} {:?}, not {:?} {:?}",
                                record.req.kind, record.name, req.kind, name
                            ),
                        };
                    }
                    None => {
                        return EnqueueResponse::Err {
                            reason: format!(
                                "replacement serial {serial} is not a live certificate for {name:?}"
                            ),
                        };
                    }
                }
            }
            Err(e) => {
                return EnqueueResponse::Err {
                    reason: format!("checking replacement serial: {e:#}"),
                };
            }
        }
    } else {
        None
    };
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
        if one_live_name(req.kind) && replacement_of.is_none() {
            match store.live_for_name(name).await {
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
    let mut queued = ca_store::QueuedReq::new(
        req.kind,
        req.csr_pem.clone(),
        name.to_string(),
        req.requested_validity,
        peer.to_string(),
        renewal_of,
        None,
    );
    queued.replaces_serial = replacement_of;
    match store.enqueue(&queued).await {
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

fn restore_kind_matches(old: NodeKind, new: NodeKind) -> bool {
    old == new
        || matches!(
            (old, new),
            (NodeKind::Client, NodeKind::Resolver)
                | (NodeKind::Resolver, NodeKind::Client)
        )
}

/// List the pending queue for an authenticated admin.
async fn handle_list_queue(state: &Server, req: &ListQueueRequest) -> ListQueueResponse {
    let req = req.clone();
    state.write_async(async move |state| handle_list_queue_inner(state, &req).await).await
}

async fn handle_list_queue_inner(
    state: &mut MutableState,
    req: &ListQueueRequest,
) -> ListQueueResponse {
    let MutableState { map, ca, .. } = state;
    let ca = ca.as_mut().expect("CA role held");
    if let Err(reason) = authenticate(ca, &req.credential) {
        return ListQueueResponse::Err { reason };
    }
    match ca.store.pending().await {
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
                        replaces_serial: q.replaces_serial,
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
async fn prepare_revoke(
    state: &Server,
    req: &RevokeRequest,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<PreparedRevoke, String> {
    let req = req.clone();
    state
        .write_async(async move |state| {
            prepare_revoke_inner(state, &req, operation_id).await
        })
        .await
}

async fn prepare_revoke_inner(
    state: &mut MutableState,
    req: &RevokeRequest,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<PreparedRevoke, String> {
    let MutableState { map, ca, .. } = state;
    let ca = ca.as_mut().expect("CA role held");
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
    // (names are immutable for a serial); the state write lock serializes the
    // actual revoke loop with every other CA operation.
    let records: std::collections::HashMap<u64, ca_store::IssuedRecord> =
        match ca.store.list_signed().await {
            Ok(records) => records.into_iter().map(|r| (r.serial, r)).collect(),
            Err(e) => {
                return Err(format!("reading the issuance index: {e:#}"));
            }
        };
    let now = ca_store::now_unix();
    let mut warnings = Vec::new();
    // Scope-check before mutating any records.
    let mut serials = Vec::new();
    for serial in &req.serials {
        let mut authorized = true;
        if !broad && let Some(record) = records.get(serial) {
            authorized = if record.name.eq_ignore_ascii_case(SERVING_SAN) {
                crate::tls::admin_cert_identity_from_pem(record.cert_pem.as_bytes())
                    .ok()
                    .and_then(|identity| {
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
    // Each revocation is a read-modify-write of an `issued/<id>` record. The
    // state write lock keeps the whole loop serialized with `set_push_done`.
    {
        let ca_dir = ca.dir().to_path_buf();
        let store = &mut ca.store;
        for serial in serials {
            let rev = ca_store::Revocation {
                serial,
                revoked_unix: now,
                reason: req.reason.clone(),
            };
            match store.revoke(serial, rev).await {
                Ok(true) => {
                    audit(
                        &ca_dir,
                        &authd.admin,
                        "revoke",
                        &format!("operation {operation_id}: serial {serial}"),
                        Duration::ZERO,
                    )
                    .await
                }
                Ok(false) => warnings.push(format!(
                    "serial {serial} was not live (unknown or already revoked)"
                )),
                Err(e) => warnings.push(format!("revoking serial {serial}: {e:#}")),
            }
        }
    }
    // Re-sign the CRL with the server's own key (the autorenew credential).
    let crl_pem = match server_unlock(ca).await {
        Ok(signing) => {
            let path = {
                let store = &mut ca.store;
                match store.write_crl(&signing.ca_key_pem).await {
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
            match tokio::fs::read_to_string(&path).await {
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
async fn local_crl_destinations(cfg: &AdminServerConfig) -> Result<BTreeSet<PathBuf>> {
    use netidx::resolver_server::config::file::Auth;
    let mut destinations = BTreeSet::new();
    destinations.insert(cfg.trusted.with_file_name("crl.pem"));
    if let Some(path) = cfg.roles.resolver.as_ref().map(|role| &role.config) {
        let cfg = crate::resolver::ResolverConfig::load_async(path)
            .await
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
async fn apply_crl_to_destinations(
    config_lock: &ConfigDirLock,
    crl_pem: &str,
    home_ca_der: &[u8],
    destinations: BTreeSet<PathBuf>,
) -> Result<()> {
    validate_home_crl(crl_pem, home_ca_der)?;
    let destinations = destinations
        .into_iter()
        .map(|destination| config_lock.require_contained(destination))
        .collect::<Result<Vec<_>>>()?;
    let mut failures = Vec::new();
    for destination in destinations.iter() {
        match tokio::fs::read(&destination).await {
            Ok(current) if current == crl_pem.as_bytes() => continue,
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                failures.push(format!("reading {}: {e}", destination.display()));
                continue;
            }
        }
        match crate::atomic::write_atomic_async(&destination, crl_pem.as_bytes(), 0o644)
            .await
        {
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

async fn apply_crl_local(state: &Server, crl_pem: &str) -> Result<()> {
    let crl_pem = crl_pem.to_string();
    let home_ca_der = state.home_ca_der.clone();
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |mutable| {
            let destinations = local_crl_destinations(&mutable.cfg).await?;
            apply_crl_to_destinations(
                &config_lock,
                &crl_pem,
                home_ca_der.as_ref(),
                destinations,
            )
            .await
        })
        .await
}

async fn handle_apply_crl(state: &Server, req: &ApplyCrlRequest) -> ApplyCrlResponse {
    info!("admin-server: applying CRL operation {}", req.operation_id);
    match apply_crl_local(state, &req.crl_pem).await {
        Ok(()) => ApplyCrlResponse::Ok,
        Err(e) => ApplyCrlResponse::Err { reason: format!("{e:#}") },
    }
}

async fn handle_apply_controller_state(
    state: &Server,
    req: &ApplyControllerStateRequest,
) -> ApplyControllerStateResponse {
    let err = |reason: String| ApplyControllerStateResponse::Err { reason };
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
    if let Err(e) = validate_home_crl(&req.crl_pem, state.home_ca_der.as_ref()) {
        return err(format!("validating controller CRL: {e:#}"));
    }
    let req = req.clone();
    let cfg_path = state.cfg_path.clone();
    let config_lock = state.config_lock.clone();
    let home_ca_der = state.home_ca_der.clone();
    state
        .write_async(async move |mutable| {
            let installed_controller = mutable.map.controller;
            if req.controller != installed_controller
                || req.map.controller != installed_controller
            {
                return err(format!(
                    "controller identity mismatch (installed {}, request {}, map {})",
                    installed_controller, req.controller, req.map.controller
                ));
            }
            if req.map.version < mutable.map.version {
                return err(format!(
                    "refusing controller-state rollback from map version {} to {}",
                    mutable.map.version, req.map.version
                ));
            }
            if mutable.ca.is_none() {
                let Some(cfg_path) = cfg_path.as_ref() else {
                    return err("this node has no persistent admin-server config path"
                        .to_string());
                };
                let mut next = mutable.cfg.clone();
                next.ca_addr = Some(req.addr);
                if let Err(e) = next.save_async(&config_lock, cfg_path).await {
                    return err(format!(
                        "persisting the relocated controller address: {e:#}"
                    ));
                }
                mutable.cfg = next;
            }
            let destinations = match local_crl_destinations(&mutable.cfg).await {
                Ok(destinations) => destinations,
                Err(e) => {
                    return err(format!("locating reconciled CRL destinations: {e:#}"));
                }
            };
            if let Err(e) = apply_crl_to_destinations(
                &config_lock,
                &req.crl_pem,
                home_ca_der.as_ref(),
                destinations,
            )
            .await
            {
                return err(format!("installing reconciled CRL: {e:#}"));
            }
            mutable.map = req.map.clone();
            ApplyControllerStateResponse::Ok
        })
        .await
}

async fn registered_crl_targets(
    state: &Server,
) -> Vec<(admin_proto::AdminServerId, SocketAddr)> {
    let mut targets: Vec<_> = state
        .read(move |state| {
            state
                .map
                .servers
                .iter()
                .filter(|server| server.state == admin_proto::ServerState::Registered)
                .map(|server| (server.id, server.addr))
                .collect()
        })
        .await;
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
    let targets = registered_crl_targets(state).await;
    let (my_id, controller) =
        state.read(move |state| (state.cfg.server_id, state.map.controller)).await;
    let mut results = Vec::new();
    if let Some((server, addr)) = targets.iter().copied().find(|(id, _)| *id == my_id) {
        let crl_pem = crl_pem.to_string();
        let error =
            apply_crl_local(state, &crl_pem).await.err().map(|e| format!("{e:#}"));
        results.push(PeerResult { server, addr, error });
    }
    let client = match state.outbound_client().await {
        Ok(client) => client,
        Err(e) => {
            results.extend(
                targets.into_iter().filter(|(server, _)| *server != my_id).map(
                    |(server, addr)| PeerResult {
                        server,
                        addr,
                        error: Some(format!("loading outbound identity failed: {e:#}")),
                    },
                ),
            );
            return results;
        }
    };
    let home_ca = state.home_ca_der.clone();
    let mut remote = collect_peer_results(
        targets.into_iter().filter(|(server, _)| *server != my_id),
        |server, addr| {
            let client = client.clone();
            let home_ca = home_ca.clone();
            let crl_pem = crl_pem.to_string();
            async move {
                tokio::time::timeout(
                    PUSH_TIMEOUT,
                    admin_client::push_crl(
                        &client,
                        addr,
                        server,
                        server == controller,
                        home_ca,
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
    let (map, my_id) =
        state.read(move |state| (state.map.clone(), state.cfg.server_id)).await;
    let controller = map
        .controller_entry()
        .filter(|entry| entry.state == admin_proto::ServerState::Registered)
        .cloned()
        .context("the authoritative map has no registered controller")?;
    let crl_pem = state
        .write_async(async move |state| {
            let ca = state
                .ca
                .as_mut()
                .context("controller reconciliation requires the CA role")?;
            let crl_path = ca.store.crl_path();
            match tokio::fs::read_to_string(&crl_path).await {
                Ok(crl_pem) => Ok(crl_pem),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    server_unlock(ca).await.map_err(|reason| anyhow!(reason))?;
                    tokio::fs::read_to_string(&crl_path).await.with_context(|| {
                        format!("reading newly initialized CRL {}", crl_path.display())
                    })
                }
                Err(e) => Err(e).with_context(|| {
                    format!("reading current CRL {}", crl_path.display())
                }),
            }
        })
        .await?;
    let request = ApplyControllerStateRequest {
        operation_id,
        controller: controller.id,
        addr: controller.addr,
        map: map.clone(),
        crl_pem,
    };
    let targets = registered_crl_targets(state).await;
    let mut results = Vec::new();
    if let Some((server, addr)) = targets.iter().copied().find(|(id, _)| *id == my_id) {
        let request = request.clone();
        let local = match handle_apply_controller_state(state, &request).await {
            ApplyControllerStateResponse::Ok => Ok(()),
            ApplyControllerStateResponse::Err { reason } => Err(anyhow!(reason)),
        };
        let error = local.err().map(|e| format!("{e:#}"));
        results.push(PeerResult { server, addr, error });
    }
    let client = state.outbound_client().await?;
    let home_ca = state.home_ca_der.clone();
    let mut remote = collect_peer_results(
        targets.into_iter().filter(|(server, _)| *server != my_id),
        |server, addr| {
            let client = client.clone();
            let home_ca = home_ca.clone();
            let request = request.clone();
            async move {
                tokio::time::timeout(
                    PUSH_TIMEOUT,
                    admin_client::push_controller_state(
                        &client,
                        addr,
                        server,
                        server == controller.id,
                        home_ca,
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
    let Some(ca_dir) = state.ca_dir().await else { return };
    audit(
        &ca_dir,
        "(startup)",
        "reconcile-controller",
        &format!("operation {operation_id}: startup reconciliation"),
        Duration::ZERO,
    )
    .await;
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
    req: &admin_proto::ReconcileControllerRequest,
    local: bool,
) -> ReconcileControllerResponse {
    let Some(ca_dir) = state.ca_dir().await else {
        return ReconcileControllerResponse::Err {
            reason: "controller reconciliation must be sent to the CA controller"
                .to_string(),
        };
    };
    let admin = if local {
        "local".to_string()
    } else {
        let credential = req.credential.clone();
        match state
            .write(move |state| {
                authenticate(state.ca.as_mut().expect("CA role held"), &credential)
                    .map(|authd| authd.admin)
            })
            .await
        {
            Ok(admin) => admin,
            Err(reason) => return ReconcileControllerResponse::Err { reason },
        }
    };
    let operation_id = admin_proto::OperationId::new();
    audit(
        &ca_dir,
        &admin,
        "reconcile-controller",
        &format!("operation {operation_id}"),
        Duration::ZERO,
    )
    .await;
    let mut peers = match push_controller_state_to_peers(state, operation_id).await {
        Ok(peers) => peers,
        Err(e) => {
            return ReconcileControllerResponse::Err { reason: format!("{e:#}") };
        }
    };
    let topology = {
        let map = state.read(move |state| state.map.clone()).await;
        topology_fanout(&map, map.clusters.iter())
    };
    merge_topology_results(
        &mut peers,
        push_topology(state, topology, operation_id).await,
    );
    ReconcileControllerResponse::Ok { operation_id, peers }
}

fn merge_topology_results(peers: &mut Vec<PeerResult>, topology: Vec<PeerResult>) {
    for mut result in topology {
        let Some(existing) = peers.iter_mut().find(|peer| peer.server == result.server)
        else {
            if let Some(error) = result.error.as_mut() {
                *error = format!("resolver topology: {error}");
            }
            peers.push(result);
            continue;
        };
        let Some(error) = result.error else { continue };
        existing.error = Some(match existing.error.take() {
            Some(state_error) => {
                format!("controller state: {state_error}; resolver topology: {error}")
            }
            None => format!("resolver topology: {error}"),
        });
    }
    peers.sort_by_key(|peer| peer.server);
}

async fn handle_revoke(state: &Arc<Server>, req: &RevokeRequest) -> RevokeResponse {
    let operation_id = admin_proto::OperationId::new();
    let prepared = prepare_revoke(state, req, operation_id).await;
    let PreparedRevoke { admin, mut warnings, crl_pem } = match prepared {
        Ok(prepared) => prepared,
        Err(reason) => return RevokeResponse::Err { reason },
    };
    audit(
        &state.ca_dir().await.expect("CA role held"),
        &admin,
        "fanout-crl",
        &format!("operation {operation_id}"),
        Duration::ZERO,
    )
    .await;
    let peers = match crl_pem {
        Some(crl_pem) => push_crl_to_peers(state, &crl_pem, operation_id).await,
        None => {
            let reason =
                "fresh CRL unavailable; immediate distribution was not attempted";
            warnings.push(reason.to_string());
            registered_crl_targets(state)
                .await
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

async fn start_list_issued(
    ca: &mut ca_store::CaDir,
    req: &ListIssuedRequest,
) -> std::result::Result<ca_store::IssuedRecords, String> {
    authenticate(ca, &req.credential)?;
    ca.store
        .compact_issued(ca_store::now_unix())
        .await
        .map_err(|e| format!("compacting issued certs: {e:#}"))?;
    ca.store.issued_records().await.map_err(|e| format!("listing issued certs: {e:#}"))
}

/// A successful [`handle_approve`]: the signed outcome plus what the
/// dispatch arm needs to finish the job — the push plan for id-map
/// registration, and the peer address to record when the approved
/// entry was a admin-server enrollment.
struct Approved {
    resp: SignResponse,
    push: Option<PushPlan>,
    enrollment: Option<(admin_proto::AdminServerId, admin_proto::EnrollmentRequest)>,
    replacement_crl: Option<String>,
}

/// Approve a queued request: look it up, then sign it through the
/// exact same checks a synchronous [`SignRequest`] goes through (the
/// admin's SAN globs, validity cap, id-map group allowed-set), audited
/// as `op=approve`. The outer `Err` is a safe wire reason for
/// before-the-sign failures.
async fn handle_approve(
    ca: &mut ca_store::CaDir,
    req: &ApproveRequest,
    map: Option<&NetworkMap>,
) -> std::result::Result<Approved, String> {
    // This cheap precheck (no auth) rejects an already-terminal request;
    // issuance checks it again before committing.
    let queued = match ca.store.status(&req.request_id).await {
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
    approve_serialized(ca, req, queued, map).await
}

/// Sign a queued request through the same checks a synchronous Sign goes
/// through (or, for a verified renewal / admin-server enrollment, the
/// narrower continuation gate), committing the issuance atomically.
/// `queued` came from the cheap precheck; `issue_serialized` re-checks it
/// is still pending before commit.
async fn approve_serialized(
    ca: &mut ca_store::CaDir,
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
        stage_enrollment(&mut staged, server_id, &enrollment)
            .map_err(|e| format!("invalid enrollment grant: {e:#}"))?;
        let signing = server_unlock(ca).await?;
        let signed = issue_serialized(
            ca,
            &signing,
            &authd.admin,
            &queued,
            SERVING_SAN,
            ca.lifetimes.leaf_validity,
            Vec::new(),
            false,
            None,
            None,
            Some(crate::tls::AdminCertIdentity { server_id, controller: false }),
            "enroll",
            Some(&req.request_id),
        )
        .await
        .map_err(|e| format!("internal error: {e:#}"))?;
        let enrollment = matches!(&signed.resp, SignResponse::Ok { .. })
            .then_some((server_id, enrollment));
        return Ok(Approved {
            resp: signed.resp,
            push: signed.push,
            enrollment,
            replacement_crl: signed.replacement_crl,
        });
    }
    // A verified renewal: continuation of an already-approved identity —
    // possession of the live key was proven at enqueue. The SAN-scope and
    // one-live-cert checks don't apply (a renewal's name *does* have a live
    // cert), the reserved serving name is allowed (admin servers renew
    // themselves), and the id-map is untouched (requested groups ignored).
    // `issue_serialized` re-checks the renewed serial is still live before
    // commit, so a revocation since enqueue refuses the renewal.
    if let Some(orig_serial) = queued.renewal_of {
        let authd = authenticate(ca, &req.credential)?;
        let validity = queued
            .requested_validity
            .min(authd.policy.max_validity)
            .max(Duration::from_secs(1));
        let signing = server_unlock(ca).await?;
        let signed = issue_serialized(
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
            None,
            "renew",
            Some(&req.request_id),
        )
        .await
        .map_err(|e| format!("internal error: {e:#}"))?;
        return Ok(Approved {
            resp: signed.resp,
            push: signed.push,
            enrollment: None,
            replacement_crl: signed.replacement_crl,
        });
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
        replaces_serial: queued.replaces_serial,
    };
    let signed =
        handle_sign_request_op(ca, &sign_req, "approve", &queued, Some(&req.request_id))
            .await;
    Ok(Approved {
        resp: signed.resp,
        push: signed.push,
        enrollment: None,
        replacement_crl: signed.replacement_crl,
    })
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

pub(crate) async fn read_autorenew_password_async(
    keytab: &Path,
) -> Result<Zeroizing<String>> {
    let keytab = keytab.to_path_buf();
    tokio::task::spawn_blocking(move || read_autorenew_password(&keytab))
        .await
        .context("autorenew keytab task panicked")?
}

/// One autorenew pass: approve every pending **verified renewal** as the
/// [`AUTORENEW_ADMIN`] slot, returning the count approved. Only verified
/// renewals — continuations of an identity an admin already approved once,
/// proven by possession of the live key at enqueue — are auto-approved;
/// new enrollments always wait for a human (and the slot's empty policy
/// would refuse them anyway). Uses proofs prepared on the bounded blocking
/// pool and commits through the same [`handle_approve`] path as the wire, so it is
/// audited as `op=renew` by the `autorenew` admin (the verified-renewal
/// continuation gate) — exactly the trail the separate daemon left.
async fn autorenew_sweep(ca: &mut ca_store::CaDir, password: &str) -> usize {
    let pending = match ca.store.pending().await {
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
        let result = handle_approve(ca, &req, None).await;
        match result {
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

/// How often a non-CA admin server re-asserts its own facts to the CA and
/// refreshes its cached map (a cheap version probe; a full pull only when
/// it changed). Short enough that `update` sees recent changes, infrequent
/// enough to be free on a control plane.
const MAP_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// On a non-CA admin server, keep the cached network map current and keep
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
                admin_client::register(&client, ca_addr, state.home_ca_der.clone(), &req)
                    .await
            {
                warn!(
                    "admin-server: registering with the CA {ca_addr} failed (will retry): {e:#}"
                );
            }
            // Refresh the cache: cheap version check, full pull only when changed.
            match admin_client::get_map_version_from_controller(
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
                        match admin_client::get_map_from_controller(
                            &state.pki_client,
                            ca_addr,
                            state.home_ca_der.clone(),
                            NodeKind::AdminServer,
                        )
                        .await
                        {
                            Ok(map) => state.write(move |state| state.map = map).await,
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
                    let sweep = REQUEST_SERVER_UNLOCK.scope(Some(Ok(unlocked)), async {
                        state
                            .write_async(async move |state| {
                                autorenew_sweep(
                                    state.ca.as_mut().expect("CA role held"),
                                    &pw,
                                )
                                .await;
                            })
                            .await;
                    });
                    REQUEST_AUTHENTICATION.scope(Some(Ok(authenticated)), sweep).await;
                }
                Ok(Err(e)) => warn!("autorenew: preparing sweep credentials: {e:#}"),
                Err(e) => warn!("autorenew: credential task panicked: {e:#}"),
            }
            tokio::time::sleep(AUTORENEW_POLL).await;
        }
    });
}

/// Deny a queued request (any authenticated admin). The state write lock keeps the
/// status re-check and denial write serialized with approval.
async fn handle_deny(
    ca: &mut ca_store::CaDir,
    req: &DenyRequest,
    map: Option<&NetworkMap>,
) -> DenyResponse {
    // Cheap precheck (no auth) for an already-terminal/unknown request.
    let queued = match ca.store.status(&req.request_id).await {
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
    let store = &mut ca.store;
    // Authoritative re-check immediately before the denial commit.
    match store.status(&req.request_id).await {
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
    match store.deny(&queued, &req.reason).await {
        Ok(()) => {
            audit(ca.dir(), &authd.admin, "deny", &queued.requested_name, Duration::ZERO)
                .await;
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
async fn handle_request_delegation(
    state: &Server,
    req: &DelegationRequest,
    peer: SocketAddr,
) -> DelegationResponse {
    let Some(ca_dir) = state.ca_dir().await else {
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
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |state| {
            let mut staged = state.map.clone();
            if let Err(e) = netmap::delegate(
                &mut staged,
                &pending.proposed_path,
                pending.proposed_child,
                &pending.parent_servers,
                &pending.child_servers,
            ) {
                return DelegationResponse::Err {
                    reason: format!("invalid delegation: {e:#}"),
                };
            }
            match delegation_store::enqueue(&config_lock, &ca_dir, &pending).await {
                Ok(()) => DelegationResponse::Ok { request_id: pending.id },
                Err(e) => DelegationResponse::Err { reason: format!("{e:#}") },
            }
        })
        .await
}

/// `PollDelegation` (unauthenticated): map the stored status to the wire.
async fn handle_poll_delegation(
    ca_dir: &Path,
    req: &PollRequest,
) -> DelegationPollResponse {
    match delegation_store::status(ca_dir, &req.request_id).await {
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
async fn handle_list_delegations(
    state: &Server,
    req: &ListDelegationsRequest,
) -> ListDelegationsResponse {
    let req = req.clone();
    state
        .write_async(async move |state| handle_list_delegations_inner(state, &req).await)
        .await
}

async fn handle_list_delegations_inner(
    state: &mut MutableState,
    req: &ListDelegationsRequest,
) -> ListDelegationsResponse {
    let MutableState { map, ca, .. } = state;
    let Some(ca) = ca.as_mut() else {
        return ListDelegationsResponse::Err {
            reason: "this host is not the controller".into(),
        };
    };
    if let Err(reason) = authenticate(ca, &req.credential) {
        return ListDelegationsResponse::Err { reason };
    }
    let dir = ca.dir().to_path_buf();
    let reqs = async {
        let mut reqs: Vec<_> = delegation_store::pending(&dir)
            .await?
            .into_iter()
            .map(|req| (req, false))
            .collect();
        reqs.extend(
            delegation_store::approved(&dir)
                .await?
                .into_iter()
                .map(|rec| (rec.req, true)),
        );
        reqs.sort_by_key(|(req, _)| req.received_unix);
        Ok::<_, anyhow::Error>(reqs)
    }
    .await;
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

async fn handle_deny_delegation(
    state: &Server,
    req: &DenyDelegationRequest,
) -> DenyDelegationResponse {
    let req = req.clone();
    state
        .write_async(async move |state| handle_deny_delegation_inner(state, &req).await)
        .await
}

async fn handle_deny_delegation_inner(
    state: &mut MutableState,
    req: &DenyDelegationRequest,
) -> DenyDelegationResponse {
    let Some(ca) = state.ca.as_mut() else {
        return DenyDelegationResponse::Err {
            reason: "this host does not hold the CA".to_string(),
        };
    };
    let ca_dir = ca.dir().to_path_buf();
    let config_lock = ca.config_lock();
    let authd = match authenticate(ca, &req.credential) {
        Ok(a) => a,
        Err(reason) => return DenyDelegationResponse::Err { reason },
    };
    match delegation_store::read_pending(&ca_dir, &req.request_id).await {
        Ok(Some(pending)) => {
            if !delegation_authority(&authd, &pending.proposed_path) {
                return DenyDelegationResponse::Err {
                    reason: format!(
                        "admin {} is not authorized to decide delegations at {:?}",
                        authd.admin, pending.proposed_path
                    ),
                };
            }
            match delegation_store::deny(&config_lock, &ca_dir, &pending, &req.reason)
                .await
            {
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
async fn apply_referral_edit_local(
    config_lock: &ConfigDirLock,
    resolver_config_path: &Path,
    edit: &ReferralEdit,
) -> Result<()> {
    let resolver_config_path = config_lock.require_contained(resolver_config_path)?;
    use netidx::resolver_server::config::file::Referral;
    let mut rc = crate::resolver::ResolverConfig::load_async(&resolver_config_path)
        .await
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
    rc.save_async(&resolver_config_path)
        .await
        .context("the referral edit would make the resolver config invalid")
}

/// `ApplyReferralEdit` (server-to-server, peer-cert-gated): the receive
/// side of cluster-wide delegation propagation. Requires a resolver role
/// (the config to edit).
async fn handle_apply_referral_edit(
    state: &Server,
    req: &ApplyReferralEditRequest,
) -> ApplyReferralEditResponse {
    info!(
        "admin-server: applying referral operation {}: {:?}",
        req.operation_id, req.edit
    );
    state.apply_referral_edit(req).await
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
/// This host's own resolver base (the single level a local, control-socket
/// caller may edit permissions at). `None` when this host serves no resolver.
async fn own_base(state: &Server) -> Option<String> {
    state
        .read(move |state| {
            let cluster = state
                .map
                .servers
                .iter()
                .find(|server| server.id == state.cfg.server_id)?
                .cluster?;
            state
                .map
                .clusters
                .iter()
                .find(|cluster_entry| cluster_entry.id == cluster)
                .map(|cluster_entry| cluster_entry.base.clone())
        })
        .await
}

async fn grant_enrollment(
    state: &Server,
    server_id: admin_proto::AdminServerId,
    enrollment: &admin_proto::EnrollmentRequest,
) -> Result<admin_proto::ResolverClusterId> {
    let cfg_path = state.cfg_path.clone();
    let config_lock = state.config_lock.clone();
    let enrollment = enrollment.clone();
    state
        .write_async(async move |mutable| {
            let MutableState { cfg, map, ca, .. } = mutable;
            let ca = ca.as_mut().context("this host does not hold the CA")?;
            let ca_dir = ca.dir().to_path_buf();
            let replaced_addr = enrollment.replaces.and_then(|old| {
                map.servers
                    .iter()
                    .find(|server| server.id == old)
                    .map(|server| server.addr)
            });
            let mut staged = map.clone();
            let cluster = stage_enrollment(&mut staged, server_id, &enrollment)?;
            if let Some(old) = enrollment.replaces {
                revoke_server_certificates(ca, old, "approved restore")
                    .await
                    .context("revoking the replaced server identity")?;
            }
            netmap::save_async(&config_lock, &ca_dir, &staged)
                .await
                .context("persisting the enrollment grant")?;
            *map = staged;
            if let Some(old_addr) = replaced_addr {
                cfg.peers.retain(|peer| *peer != old_addr);
                if let Some(path) = &cfg_path {
                    cfg.save_async(&config_lock, path)
                        .await
                        .context("persisting removal of the replaced peer hint")?;
                }
            }
            Ok(cluster)
        })
        .await
}

/// Stage a new enrollment and, for restore, atomically replace the failed
/// satellite named by the bundle. Enroll first so replacing the sole member of
/// a cluster cannot transiently delete that stable cluster ID.
fn stage_enrollment(
    map: &mut NetworkMap,
    server_id: admin_proto::AdminServerId,
    enrollment: &admin_proto::EnrollmentRequest,
) -> Result<admin_proto::ResolverClusterId> {
    let replaced_cluster = match enrollment.replaces {
        Some(old) if old == map.controller => {
            bail!("the active controller cannot be replaced by satellite enrollment")
        }
        Some(old) => Some(
            map.servers
                .iter()
                .find(|server| server.id == old)
                .with_context(|| {
                    format!(
                        "replacement server {old} is absent from the authoritative map"
                    )
                })?
                .cluster,
        ),
        None => None,
    };
    if let Some(old) = enrollment.replaces {
        // The replacement normally owns the same resolver endpoint. Release
        // that ownership on the staged copy before enrolling the fresh ID;
        // the old server remains a cluster member until the new grant exists,
        // so the stable cluster itself can never disappear in between.
        if let Some(server) = map.servers.iter_mut().find(|server| server.id == old) {
            server.resolver = None;
        }
    }
    let cluster = netmap::enroll(map, server_id, enrollment)?;
    if let Some(old) = enrollment.replaces {
        if replaced_cluster.flatten() != Some(cluster) {
            bail!("a restored server must rejoin the same resolver cluster it replaces");
        }
        netmap::remove(map, old)?;
    }
    Ok(cluster)
}

/// CA-side: register/update a admin server's facts in the authoritative
/// map and persist it. Peer-cert-gated at the dispatch. The CA is the
/// map's only writer, so a non-CA host refuses.
async fn handle_register(
    state: &Arc<Server>,
    server_id: admin_proto::AdminServerId,
    req: &RegisterRequest,
) -> RegisterResponse {
    let req = req.clone();
    let ca_dir = match state.ca_dir().await {
        Some(d) => d,
        None => {
            return RegisterResponse::Err {
                reason: "this host does not hold the CA — register with the CA"
                    .to_string(),
            };
        }
    };
    let validation_req = req.clone();
    let reconcile_id_map = match state
        .read(move |state| {
            let current = state
                .map
                .servers
                .iter()
                .find(|server| server.id == server_id)
                .with_context(|| {
                    format!("server {server_id} has no approved enrollment grant")
                })?;
            let reconcile = current.state == admin_proto::ServerState::Enrolled
                && current.roles.contains(&Role::IdMap);
            let mut staged = state.map.clone();
            netmap::register(
                &mut staged,
                server_id,
                validation_req.addr,
                validation_req.resolver.as_ref(),
            )?;
            Ok::<_, anyhow::Error>(reconcile)
        })
        .await
    {
        Ok(reconcile) => reconcile,
        Err(e) => return RegisterResponse::Err { reason: format!("{e:#}") },
    };
    if reconcile_id_map
        && let Err(e) = reconcile_identities_to_target(state, server_id, req.addr).await
    {
        return RegisterResponse::Err {
            reason: format!("reconciling existing identities before registration: {e:#}"),
        };
    }
    let config_lock = state.config_lock.clone();
    let (response, fanout) = state
        .write_async(async move |state| {
            let updated = match netmap::register(
                &mut state.map,
                server_id,
                req.addr,
                req.resolver.as_ref(),
            ) {
                Ok(updated) => updated,
                Err(e) => {
                    return (RegisterResponse::Err { reason: format!("{e:#}") }, None);
                }
            };
            if updated
                && let Err(e) =
                    netmap::save_async(&config_lock, &ca_dir, &state.map).await
            {
                return (
                    RegisterResponse::Err {
                        reason: format!("persisting the network map: {e:#}"),
                    },
                    None,
                );
            }
            let fanout = updated
                .then(|| registration_topology_fanout(&state.map, server_id))
                .flatten();
            (RegisterResponse::Ok { version: state.map.version }, fanout)
        })
        .await;
    if let Some(fanout) = fanout {
        let operation_id = admin_proto::OperationId::new();
        for result in push_topology(state, fanout, operation_id).await {
            if let Some(error) = result.error {
                warn!(
                    "admin-server: registration topology push {} at {} failed: {}",
                    result.server, result.addr, error
                );
            }
        }
    }
    response
}

/// CA-side: drop a admin server from the map on uninstall.
async fn handle_deregister(
    state: &Server,
    server_id: admin_proto::AdminServerId,
) -> RegisterResponse {
    let ca_dir = match state.ca_dir().await {
        Some(d) => d,
        None => {
            return RegisterResponse::Err {
                reason: "this host does not hold the CA".to_string(),
            };
        }
    };
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |state| {
            let updated = match netmap::deregister(&mut state.map, server_id) {
                Ok(updated) => updated,
                Err(e) => return RegisterResponse::Err { reason: format!("{e:#}") },
            };
            if updated
                && let Err(e) =
                    netmap::save_async(&config_lock, &ca_dir, &state.map).await
            {
                return RegisterResponse::Err {
                    reason: format!("persisting the network map: {e:#}"),
                };
            }
            RegisterResponse::Ok { version: state.map.version }
        })
        .await
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
/// separate: network I/O must not hold mutable state or the signing semaphore.
async fn remove_server_prepare(
    state: &Server,
    req: &RemoveServerRequest,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<RemoveServerPrepare, RemoveServerResponse> {
    let req = req.clone();
    state
        .write_async(async move |state| {
            remove_server_prepare_inner(state, &req, operation_id).await
        })
        .await
}

async fn remove_server_prepare_inner(
    state: &mut MutableState,
    req: &RemoveServerRequest,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<RemoveServerPrepare, RemoveServerResponse> {
    let err = |reason: String| RemoveServerResponse::Err { reason };
    let MutableState { map, ca, .. } = state;
    let ca = match ca.as_mut() {
        Some(ca) => ca,
        None => return Err(err("this host does not hold the CA".to_string())),
    };
    let ca_dir = ca.dir().to_path_buf();
    let authd = match authenticate(ca, &req.credential) {
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
    {
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
            if let Some(cluster) =
                map.clusters.iter().find(|entry| entry.id == cluster_id)
            {
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
            revoked =
                revoke_server_certificates(ca, req.server, &authd.admin).await.map_err(
                    |e| err(format!("revoking the server's serving certificates: {e:#}")),
                )?;
            let config_lock = ca.config_lock();
            if let Err(e) = netmap::save_async(&config_lock, &ca_dir, &next).await {
                return Err(err(format!("persisting the network map: {e:#}")));
            }
            *map = next;
            audit(
                &ca_dir,
                &authd.admin,
                "remove-server",
                &format!("operation {operation_id}: {}", req.server),
                Duration::ZERO,
            )
            .await;
        } else {
            audit(
                &ca_dir,
                &authd.admin,
                "reconcile-server-removal",
                &format!("operation {operation_id}: {}", req.server),
                Duration::ZERO,
            )
            .await;
        }
        // Always carry the current CRL on an idempotent retry as well: a previous
        // removal may have committed the revocation but only partially delivered
        // it. Repeating force-remove is the manual reconciliation path for both
        // topology and revocation state.
        let crl_pem = {
            let path = ca.store.crl_path();
            match tokio::fs::read_to_string(&path).await {
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
                    topology_edit(map, cluster, local_member),
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
}

/// Permanently remove a dead identity, then reconcile only the surviving
/// clusters whose referral topology changed. This never restarts a resolver;
/// administrators retain control of the rolling restart sequence.
async fn handle_remove_server(
    state: &Arc<Server>,
    req: RemoveServerRequest,
) -> RemoveServerResponse {
    let operation_id = admin_proto::OperationId::new();
    let prepared = remove_server_prepare(state, &req, operation_id).await;
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(response) => return response,
    };
    let crl_peers = match prepared.crl_pem {
        Some(crl_pem) => push_crl_to_peers(state, &crl_pem, operation_id).await,
        None => Vec::new(),
    };
    let peers = push_topology(state, prepared.fanout, operation_id).await;
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
async fn revoke_server_certificates(
    ca: &mut ca_store::CaDir,
    server_id: admin_proto::AdminServerId,
    admin: &str,
) -> Result<usize> {
    let signing = server_unlock(ca).await.map_err(|reason| anyhow!(reason))?;
    let now = ca_store::now_unix();
    let store = &mut ca.store;
    let serials: Vec<_> = store
        .list_signed()
        .await
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
        if store.revoke(serial, revocation).await? {
            revoked += 1;
        }
    }
    if revoked > 0 {
        store
            .write_crl(&signing.ca_key_pem)
            .await
            .context("publishing the updated CRL")?;
    }
    Ok(revoked)
}

/// The local resolver's permissions file, resolved against its config dir.
async fn local_perms_path(state: &Server) -> Result<PathBuf> {
    let rconfig = state
        .read(move |state| {
            state.cfg.roles.resolver.as_ref().map(|role| role.config.clone())
        })
        .await
        .context("this host has no resolver role — no perms to read or edit")?;
    perms_path(&rconfig).await
}

async fn perms_path(rconfig: &Path) -> Result<PathBuf> {
    let rc = crate::resolver::ResolverConfig::load_async(rconfig).await?;
    perms_path_from_config(rconfig, &rc)
}

fn perms_path_from_config(
    rconfig: &Path,
    rc: &crate::resolver::ResolverConfig,
) -> Result<PathBuf> {
    let inc = rc.as_file().include_permissions.first().cloned().context(
        "this resolver has no permissions file (include_permissions is empty — an \
         anonymous network has no perms)",
    )?;
    let base = rconfig.parent().unwrap_or_else(|| Path::new("."));
    Ok(base.join(inc.as_str()))
}

/// Read the local resolver's perms file, serialized for the wire.
async fn handle_get_perms(state: &Server) -> GetPermsResponse {
    let read = async {
        let path = local_perms_path(state).await?;
        let pmap = crate::perms::load_perms_async(&path).await?;
        serde_json::to_string(&pmap).context("serializing perms")
    };
    match read.await {
        Ok(perms_json) => GetPermsResponse::Ok { perms_json },
        Err(e) => GetPermsResponse::Err { reason: format!("{e:#}") },
    }
}

/// Authenticate a permissions operation. Remote callers use the CA vault or a
/// live session; the protected local socket is the on-box signing superuser.
/// Shared by reads and edits so their scope semantics cannot drift.
async fn authenticate_perms_caller(
    state: &Arc<Server>,
    credential: &admin_proto::AdminCredential,
    local: bool,
) -> std::result::Result<ca_vault::Authenticated, String> {
    if local {
        return Ok(local_superuser());
    }
    let auth_credential = credential.clone();
    let failure_credential = credential.clone();
    match state
        .write(move |state| {
            authenticate(state.ca.as_mut().expect("CA role held"), &auth_credential)
        })
        .await
    {
        Ok(authd) => Ok(authd),
        Err(reason) => Err(safe_auth_failure(&failure_credential, reason)),
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

async fn confine_local_perms(
    state: &Server,
    target_path: &str,
    operation: &str,
) -> Result<()> {
    let base = own_base(state).await;
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
    req: &ReadPermsRequest,
    local: bool,
) -> ReadPermsResponse {
    let err = |reason: String| ReadPermsResponse::Err { reason };
    if !local && !state.has_ca().await {
        return err("a remote perms read must be sent to the CA controller".to_string());
    }
    let authd = match authenticate_perms_caller(state, &req.credential, local).await {
        Ok(authd) => authd,
        Err(reason) => return err(reason),
    };
    if local && let Err(e) = confine_local_perms(state, &req.target_path, "read").await {
        return err(format!("{e:#}"));
    }
    if let Err(reason) = authorize_perms_scope(&authd, &req.target_path, "read") {
        return err(reason);
    }
    // The protected local socket is deliberately useful on every resolver,
    // including satellites that do not have the CA role. It may read only the
    // host's own level and never consults or trusts remote map hints.
    if local {
        let (server, addr) =
            state.read(move |state| (state.cfg.server_id, state.cfg.listen)).await;
        let read = handle_get_perms(state).await;
        return match read {
            GetPermsResponse::Ok { perms_json } => {
                ReadPermsResponse::Ok { server, addr, perms_json }
            }
            GetPermsResponse::Err { reason } => err(reason),
        };
    }
    let targets = {
        let map = state.read(move |state| state.map.clone()).await;
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
        &state.ca_dir().await.expect("CA role held"),
        &authd.admin,
        "read-perms",
        &req.target_path,
        Duration::ZERO,
    )
    .await;
    let client = match state.outbound_client().await {
        Ok(client) => client,
        Err(e) => return err(format!("loading outbound identity: {e:#}")),
    };
    let controller = state.read(move |state| state.map.controller).await;
    let home_ca = state.home_ca_der.clone();
    let mut failures = Vec::new();
    for (server, addr) in targets {
        let result = tokio::time::timeout(
            PUSH_TIMEOUT,
            admin_client::pull_perms(
                &client,
                addr,
                server,
                server == controller,
                home_ca.clone(),
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

async fn apply_perms_local(state: &Server, perms_json: &str) -> Result<()> {
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
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |state| {
            let rconfig = state
                .cfg
                .roles
                .resolver
                .as_ref()
                .map(|role| role.config.clone())
                .context("no resolver role")?;
            let rc = crate::resolver::ResolverConfig::load_async(&rconfig).await?;
            let path =
                config_lock.require_contained(perms_path_from_config(&rconfig, &rc)?)?;
            let check = rc.clone();
            let check_config = rconfig.clone();
            let check_path = path.clone();
            let prospective = pmap.clone();
            tokio::task::spawn_blocking(move || {
                check.preflight_permission_topology(
                    &check_config,
                    Some((&check_path, &prospective)),
                )
            })
            .await
            .context("permissions preflight task panicked")?
            .context("the edited perms would make the resolver config invalid")?;
            crate::perms::save_perms_async(&path, &pmap).await
        })
        .await
}

/// Server-to-server receive side of a perms edit (peer-cert-gated).
async fn handle_apply_perms_edit(
    state: &Server,
    req: &ApplyPermsEditRequest,
) -> ApplyPermsEditResponse {
    info!("admin-server: applying permissions operation {}", req.operation_id);
    match apply_perms_local(state, &req.perms_json).await {
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
    let client = match state.outbound_client().await {
        Ok(client) => client,
        Err(e) => {
            return targets
                .iter()
                .map(|(server, addr)| PeerResult {
                    server: *server,
                    addr: *addr,
                    error: Some(format!("loading outbound identity failed: {e:#}")),
                })
                .collect();
        }
    };
    let controller = state.read(move |state| state.map.controller).await;
    let home_ca = state.home_ca_der.clone();
    let mut results: Vec<_> =
        stream::iter(targets.iter().copied().map(|(server, addr)| {
            let client = client.clone();
            let home_ca = home_ca.clone();
            async move {
                let res = tokio::time::timeout(
                    PUSH_TIMEOUT,
                    admin_client::push_perms_edit(
                        &client,
                        addr,
                        server,
                        server == controller,
                        home_ca,
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
    req: &EditPermsRequest,
    local: bool,
) -> EditPermsResponse {
    let err = |reason: String| EditPermsResponse::Err { reason };
    if !state.has_ca().await {
        return err("a perms edit must be sent to the CA host".to_string());
    }
    let authd = match authenticate_perms_caller(state, &req.credential, local).await {
        Ok(authd) => authd,
        Err(reason) => return err(reason),
    };
    if local && let Err(e) = confine_local_perms(state, &req.target_path, "edit").await {
        return err(format!("{e:#}"));
    }
    if let Err(reason) = authorize_perms_scope(&authd, &req.target_path, "edit") {
        return err(reason);
    }
    let members = {
        let map = state.read(move |state| state.map.clone()).await;
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
        &state.ca_dir().await.expect("CA role held"),
        &authd.admin,
        "edit-perms",
        &format!("operation {operation_id} at {}", req.target_path),
        Duration::ZERO,
    )
    .await;
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
    req: &ControlServiceRequest,
) -> ControlServiceResponse {
    let err = |reason: String| ControlServiceResponse::Err { reason };
    if !state.has_ca().await {
        return err("a service-control request must be sent to the CA host".to_string());
    }
    let auth = {
        let credential = req.credential.clone();
        state
            .write(move |state| {
                authenticate(state.ca.as_mut().expect("CA role held"), &credential)
            })
            .await
    };
    let authd = match auth {
        Ok(a) => a,
        Err(reason) => return err(safe_auth_failure(&req.credential, reason)),
    };
    // The target server's cluster base is its authorization scope. A server not
    // in the map (or running no resolver) has no base — only a signing slot may
    // control it, so an unknown target can't be reached by a scoped role admin.
    let target_server = req.target_server;
    let base = state.read(move |state| base_for_server(&state.map, target_server)).await;
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
    let ca_dir = state.ca_dir().await.expect("CA role held");
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
    )
    .await;
    let (my_id, target_addr) = state
        .read(move |state| {
            (state.cfg.server_id, registered_server_addr(&state.map, target_server))
        })
        .await;
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
        let client = match state.outbound_client().await {
            Ok(client) => client,
            Err(e) => return err(format!("loading outbound identity: {e:#}")),
        };
        let target_is_controller =
            state.read(move |state| target_server == state.map.controller).await;
        tokio::time::timeout(
            PUSH_TIMEOUT,
            admin_client::push_service_control(
                &client,
                target_addr,
                req.target_server,
                target_is_controller,
                state.home_ca_der.clone(),
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
        .read(move |state| state.cfg.activation_units_dir.clone())
        .await
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
    ca: &mut ca_store::CaDir,
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

async fn handle_add_role_admin_prepared(
    state: &Server,
    req: &AddRoleAdminRequest,
    local: bool,
    prepared: std::result::Result<ca_vault::PreparedRoleSlot, String>,
) -> AdminMgmtResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_add_role_admin_inner(state, &req, local, prepared).await
        })
        .await
}

async fn handle_add_role_admin_inner(
    state: &mut MutableState,
    req: &AddRoleAdminRequest,
    local: bool,
    prepared: std::result::Result<ca_vault::PreparedRoleSlot, String>,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
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
    // The KDF completed before this write transaction; revalidate and commit the
    // prepared slot while the lock has exclusive access.
    let dir = ca.dir().to_path_buf();
    let vault = &mut ca.vault;
    let added = match prepared {
        Ok(prepared) => vault.add_prepared_role_slot(prepared).await,
        Err(reason) => return err(reason),
    };
    match added {
        Ok(()) => {
            audit(&dir, &authd.admin, "add-role-admin", &req.name, Duration::ZERO).await;
            AdminMgmtResponse::Ok
        }
        Err(e) => err(format!("{e:#}")),
    }
}

/// `SetAdminPolicy`: rescope an existing role admin (CA-only).
async fn handle_set_admin_policy(
    state: &Server,
    req: &SetAdminPolicyRequest,
    local: bool,
) -> AdminMgmtResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_set_admin_policy_inner(state, &req, local).await
        })
        .await
}

async fn handle_set_admin_policy_inner(
    state: &mut MutableState,
    req: &SetAdminPolicyRequest,
    local: bool,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
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
    // The state write lock keeps the look-up, last-manager guard, and write one
    // operation, closing the TOCTOU.
    let dir = ca.dir().to_path_buf();
    let vault = &mut ca.vault;
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
        match last_role_manager(vault, &req.target) {
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
    match vault.set_policy(&req.target, req.policy.clone()).await {
        Ok(()) => {
            audit(&dir, &authd.admin, "set-admin-policy", &req.target, Duration::ZERO)
                .await;
            AdminMgmtResponse::Ok
        }
        Err(e) => err(format!("{e:#}")),
    }
}

/// `RemoveAdmin`: remove a role admin (CA-only).
async fn handle_remove_admin(
    state: &Server,
    req: &RemoveAdminRequest,
    local: bool,
) -> AdminMgmtResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_remove_admin_inner(state, &req, local).await
        })
        .await
}

async fn handle_remove_admin_inner(
    state: &mut MutableState,
    req: &RemoveAdminRequest,
    local: bool,
) -> AdminMgmtResponse {
    let err = |reason: String| AdminMgmtResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
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
    // The state write lock keeps the look-up, last-manager guard, and removal one
    // operation, so concurrent removals cannot strand management.
    let dir = ca.dir().to_path_buf();
    let vault = &mut ca.vault;
    let (kind, current) = match vault.slot_policy(&req.target) {
        Ok(kp) => kp,
        Err(_) => return err(format!("no admin named {:?}", req.target)),
    };
    if kind != ca_vault::SlotKind::Role {
        return err("remote admin management operates on role admins only".to_string());
    }
    if current.may_manage_admins {
        match last_role_manager(vault, &req.target) {
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
    match vault.remove_slot(&req.target, false).await {
        Ok(()) => {
            audit(&dir, &authd.admin, "remove-admin", &req.target, Duration::ZERO).await;
            AdminMgmtResponse::Ok
        }
        Err(e) => err(format!("{e:#}")),
    }
}

/// `ListAdmins`: the admin roster (CA-only; gated on management authority so
/// a lower-tier role can't read everyone's capabilities).
async fn handle_list_admins(
    state: &Server,
    req: &ListAdminsRequest,
    local: bool,
) -> AdminListResponse {
    let req = req.clone();
    state.write(move |state| handle_list_admins_inner(state, &req, local)).await
}

fn handle_list_admins_inner(
    state: &mut MutableState,
    req: &ListAdminsRequest,
    local: bool,
) -> AdminListResponse {
    let err = |reason: String| AdminListResponse::Err { reason };
    let Some(ca) = state.ca.as_mut() else {
        return err("admin management must be sent to the CA host".to_string());
    };
    if let Err(reason) = authorize_admin_mgmt(ca, &req.credential, local) {
        return err(reason);
    }
    match ca.vault.list_admins() {
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
async fn rotate_recovery(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    local: bool,
) -> RotateRecoveryResponse {
    let err = |reason: String| RotateRecoveryResponse::Err { reason };
    if !local {
        return err(
            "rotating the recovery password is allowed only over the local control \
             socket on the CA box"
                .to_string(),
        );
    }
    let captured = state.read(move |state| {
        let ca = state.ca.as_ref().context("this host does not hold the CA")?;
        Ok::<_, anyhow::Error>((
            ca.vault.snapshot()?,
            ca.autorenew_pw.clone().context(
                "this CA holds no autorenew credential, so a fresh recovery slot cannot \
                 be minted on-box; rotate offline with the daemon stopped",
            )?,
        ))
    });
    let (snapshot, autorenew_pw) = match captured.await {
        Ok(captured) => captured,
        Err(e) => return err(format!("{e:#}")),
    };
    let new_pw = ca_vault::gen_recovery_password();
    let worker_new_pw = new_pw.clone();
    let prepared = match run_signing(signs, move || {
        snapshot.prepare_signing_replacement(
            &autorenew_pw,
            ca_vault::RECOVERY_ADMIN,
            &worker_new_pw,
            crate::ca_policy::recovery_policy(),
        )
    })
    .await
    {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(e)) => return err(format!("preparing the recovery slot: {e:#}")),
        Err(e) => return err(format!("recovery credential task failed: {e:#}")),
    };
    let canonical = new_pw.as_str().to_string();
    state
        .write_async(async move |state| {
            let ca = state.ca.as_mut().expect("CA role held");
            if let Err(e) = ca.vault.install_signing_replacement(prepared).await {
                return err(format!("installing the recovery slot: {e:#}"));
            }
            audit(
                ca.dir(),
                "local",
                "rotate-recovery",
                ca_vault::RECOVERY_ADMIN,
                Duration::ZERO,
            )
            .await;
            RotateRecoveryResponse::Ok { recovery_password: Secret(canonical) }
        })
        .await
}

/// `RotateAutorenew` (local control socket ONLY): rotate the box's OWN
/// autorenew signing credential and reseal its keytab, then hot-swap the
/// in-process credential — no downtime, and no second signing slot needed
/// (the daemon re-wraps the slot in place with the password it already
/// holds). Preserves the keytab's sealing posture: a sealed keytab is
/// resealed (a seal failure rolls the vault change back); a plaintext keytab
/// (an `--insecure-no-tpm` CA) is rewritten in plaintext, with a warning.
struct PreparedAutorenewRotation {
    prepared: ca_vault::PreparedSigningRekey,
    new_pw: Zeroizing<String>,
    payload: Zeroizing<Vec<u8>>,
    staged: PathBuf,
    keytab: PathBuf,
    warning: Option<String>,
}

async fn rotate_autorenew(
    state: &Arc<Server>,
    signs: &Arc<Semaphore>,
    local: bool,
) -> RotateAutorenewResponse {
    let err = |reason: String| RotateAutorenewResponse::Err { reason };
    if !local {
        return err(
            "rotating the autorenew credential is allowed only over the local control \
             socket on the CA box"
                .to_string(),
        );
    }
    let captured = state.read(move |state| {
        let ca = state.ca.as_ref().context("this host does not hold the CA")?;
        let keytab = state
            .cfg
            .roles
            .ca
            .as_ref()
            .and_then(|role| role.autorenew.clone())
            .context("this CA's config names no autorenew keytab")?;
        Ok::<_, anyhow::Error>((
            ca.vault.snapshot()?,
            ca.autorenew_pw
                .clone()
                .context("this CA holds no autorenew credential to rotate")?,
            keytab,
        ))
    });
    let (snapshot, old_pw, keytab) = match captured.await {
        Ok(captured) => captured,
        Err(e) => return err(format!("{e:#}")),
    };
    let keytab = match state.config_lock.require_contained(keytab) {
        Ok(keytab) => keytab,
        Err(e) => return err(format!("{e:#}")),
    };
    let current = match tokio::fs::read(&keytab).await {
        Ok(current) => current,
        Err(e) => {
            return err(format!(
                "reading the autorenew keytab {}: {e:#}",
                keytab.display()
            ));
        }
    };
    let prepared = match run_signing(signs, move || {
        let sealed = netidx_tpm::is_sealed(&current);
        let new_pw = ca_vault::random_signing_password();
        let prepared =
            snapshot.prepare_signing_rekey(AUTORENEW_ADMIN, &old_pw, &new_pw)?;
        let (payload, warning): (Zeroizing<Vec<u8>>, Option<String>) = if sealed {
            (
                Zeroizing::new(netidx_tpm::seal(new_pw.as_bytes()).with_context(
                    || {
                        format!(
                            "resealing the autorenew keytab to this host's {}",
                            netidx_tpm::MECHANISM
                        )
                    },
                )?),
                None,
            )
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
        Ok::<_, anyhow::Error>(PreparedAutorenewRotation {
            prepared,
            new_pw,
            payload,
            staged,
            keytab,
            warning,
        })
    })
    .await
    {
        Ok(Ok(prepared)) => prepared,
        Ok(Err(e)) => return err(format!("preparing autorenew rotation: {e:#}")),
        Err(e) => return err(format!("autorenew credential task failed: {e:#}")),
    };
    if let Err(e) =
        crate::atomic::write_atomic_async(&prepared.staged, &prepared.payload, 0o600)
            .await
    {
        return err(format!(
            "staging the rotated keytab {}: {e:#}",
            prepared.staged.display()
        ));
    }
    state
        .write_async(async move |state| {
            let ca = state.ca.as_mut().expect("CA role held");
            let rollback = match ca.vault.install_signing_rekey(prepared.prepared).await {
                Ok(rollback) => rollback,
                Err(e) => {
                    let _ = tokio::fs::remove_file(&prepared.staged).await;
                    return err(format!("installing the autorenew slot: {e:#}"));
                }
            };
            if let Err(e) = tokio::fs::rename(&prepared.staged, &prepared.keytab).await {
                if let Err(rollback_error) =
                    ca.vault.rollback_signing_rekey(rollback).await
                {
                    error!(
                        "admin-server: CRITICAL — autorenew rotation failed AND rollback \
                     failed: {rollback_error:#}"
                    );
                }
                let _ = tokio::fs::remove_file(&prepared.staged).await;
                return err(format!(
                    "installing the rotated keytab {}: {e:#}",
                    prepared.keytab.display()
                ));
            }
            ca.autorenew_pw = Some(prepared.new_pw);
            audit(ca.dir(), "local", "rotate-autorenew", AUTORENEW_ADMIN, Duration::ZERO)
                .await;
            RotateAutorenewResponse::Ok { warning: prepared.warning }
        })
        .await
}

/// The blocking half of `ApproveDelegation`: authenticate, validate the
/// proposed subtree, apply the child edit to the **local** resolver
/// config, and (if the request was Pending) commit the approval so the
/// child can poll. Idempotent on an already-`Approved` request — re-runs
/// re-sync the cluster. Returns `(edit, cluster member resolver addresses)`
/// for the async peer push; the `Err` arm carries the response to send.
async fn approve_delegation_prepare(
    state: &Server,
    req: &ApproveDelegationRequest,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<TopologyFanout, ApproveDelegationResponse> {
    let req = req.clone();
    state
        .write_async(async move |state| {
            approve_delegation_prepare_inner(state, &req, operation_id).await
        })
        .await
}

async fn approve_delegation_prepare_inner(
    state: &mut MutableState,
    req: &ApproveDelegationRequest,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<TopologyFanout, ApproveDelegationResponse> {
    let err = |reason: String| ApproveDelegationResponse::Err { reason };
    let MutableState { map, ca, .. } = state;
    let ca =
        ca.as_mut().ok_or_else(|| err("this host does not hold the CA".to_string()))?;
    let ca_dir = ca.dir().to_path_buf();
    let authd = authenticate(ca, &req.credential).map_err(err)?;
    {
        // Pending ⇒ approve + commit; Approved ⇒ re-sync (re-apply + re-push,
        // already committed); else an error.
        let (pending, commit) = match delegation_store::status(&ca_dir, &req.request_id)
            .await
        {
            Ok(delegation_store::Status::Pending(p)) => (p, true),
            Ok(delegation_store::Status::Approved { .. }) => {
                match delegation_store::read_approved(&ca_dir, &req.request_id).await {
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
        let config_lock = ca.config_lock();
        netmap::save_async(&config_lock, &ca_dir, &staged)
            .await
            .map_err(|e| err(format!("persisting authoritative topology: {e:#}")))?;
        *map = staged;
        if commit {
            delegation_store::approve(
                &config_lock,
                &ca_dir,
                &pending,
                parent.members.clone(),
            )
            .await
            .map_err(|e| err(format!("committing the approval: {e:#}")))?;
        }
        audit(
            &ca_dir,
            &authd.admin,
            if commit { "approve-delegation" } else { "reconcile-delegation" },
            &format!("operation {operation_id}: {}", child.base),
            Duration::ZERO,
        )
        .await;
        Ok(topology_fanout(map, [&parent, &child]))
    }
}

struct TopologyFanout {
    targets: Vec<(admin_proto::AdminServerId, SocketAddr, ReferralEdit)>,
}

fn registration_topology_fanout(
    map: &NetworkMap,
    server_id: admin_proto::AdminServerId,
) -> Option<TopologyFanout> {
    let cluster = map
        .servers
        .iter()
        .find(|server| server.id == server_id)?
        .cluster
        .and_then(|id| map.clusters.iter().find(|cluster| cluster.id == id))?;
    Some(topology_fanout(
        map,
        map.clusters.iter().filter(|candidate| {
            candidate.id == cluster.id
                || cluster.parent == Some(candidate.id)
                || cluster.children.contains(&candidate.id)
        }),
    ))
}

fn topology_fanout<'a>(
    map: &NetworkMap,
    clusters: impl IntoIterator<Item = &'a admin_proto::ClusterEntry>,
) -> TopologyFanout {
    let mut targets = Vec::new();
    for cluster in clusters {
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
                topology_edit(map, cluster, local_member),
            ));
        }
    }
    TopologyFanout { targets }
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

/// Propagate topology edits to every registered server in the affected
/// clusters using CA-owned routing addresses.
async fn push_topology(
    state: &Arc<Server>,
    fanout: TopologyFanout,
    operation_id: admin_proto::OperationId,
) -> Vec<PeerResult> {
    let client = match state.outbound_client().await {
        Ok(client) => client,
        Err(e) => {
            return fanout
                .targets
                .into_iter()
                .map(|(server, addr, _)| PeerResult {
                    server,
                    addr,
                    error: Some(format!("loading outbound identity failed: {e:#}")),
                })
                .collect();
        }
    };
    let controller = state.read(move |state| state.map.controller).await;
    let mut targets = fanout.targets;
    targets.sort_by_key(|(id, _, _)| *id);
    let home_ca = state.home_ca_der.clone();
    let mut results: Vec<_> =
        stream::iter(targets.into_iter().map(|(server, addr, edit)| {
            let client = client.clone();
            let home_ca = home_ca.clone();
            async move {
                let res = tokio::time::timeout(
                    PUSH_TIMEOUT,
                    admin_client::push_referral_edit(
                        &client,
                        addr,
                        server,
                        server == controller,
                        home_ca,
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
    req: ApproveDelegationRequest,
) -> ApproveDelegationResponse {
    let operation_id = admin_proto::OperationId::new();
    let prepared = approve_delegation_prepare(state, &req, operation_id).await;
    let fanout = match prepared {
        Ok(fanout) => fanout,
        Err(response) => return response,
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
pub async fn audit(ca_dir: &Path, admin: &str, op: &str, name: &str, validity: Duration) {
    let ts =
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let validity = humantime::format_duration(validity);
    let line = format!("ts={ts} admin={admin} op={op} name={name} validity={validity}\n");
    let r = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(ca_dir.join("audit.log"))
        .await;
    let r = match r {
        Ok(mut file) => file.write_all(line.as_bytes()).await,
        Err(e) => Err(e),
    };
    if let Err(e) = r {
        // Audit is best-effort; a failed write must not fail issuance.
        eprintln!("admin-server: WARNING failed to append audit log: {e}");
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

    #[tokio::test]
    async fn topology_fanout_writes_the_split_config_without_service_control() {
        use netidx::resolver_server::config::file as rfile;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("ap1.json");
        let config_lock = ConfigDirLock::acquire(dir.path()).unwrap();
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

        apply_referral_edit_local(&config_lock, &p, &edit).await.unwrap();
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
        apply_referral_edit_local(&config_lock, &local_only, &edit).await.unwrap();
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
        assert!(apply_referral_edit_local(&config_lock, &p, &wrong_auth).await.is_err());
    }

    #[test]
    fn controller_reconciliation_fanout_covers_the_complete_hierarchy() {
        let controller = admin_proto::AdminServerId::new();
        let satellite = admin_proto::AdminServerId::new();
        let root = admin_proto::ResolverClusterId::new();
        let child = admin_proto::ResolverClusterId::new();
        let resolver = |addr: &str| ResolverAddr {
            addr: addr.parse().unwrap(),
            auth: InfoAuth::Anonymous,
        };
        let root_member = resolver("10.1.0.1:4564");
        let child_member = resolver("10.2.0.1:4564");
        let map = NetworkMap {
            version: 9,
            controller,
            servers: vec![
                ServerEntry {
                    id: controller,
                    addr: "10.1.0.1:4565".parse().unwrap(),
                    roles: vec![Role::Ca, Role::Resolver],
                    resolver: Some(root_member.clone()),
                    cluster: Some(root),
                    state: admin_proto::ServerState::Registered,
                },
                ServerEntry {
                    id: satellite,
                    addr: "10.2.0.1:4565".parse().unwrap(),
                    roles: vec![Role::Resolver],
                    resolver: Some(child_member.clone()),
                    cluster: Some(child),
                    state: admin_proto::ServerState::Registered,
                },
            ],
            clusters: vec![
                admin_proto::ClusterEntry {
                    id: root,
                    base: "/".into(),
                    state: admin_proto::ClusterState::Active,
                    members: vec![root_member.clone()],
                    parent: None,
                    children: vec![child],
                },
                admin_proto::ClusterEntry {
                    id: child,
                    base: "/eu".into(),
                    state: admin_proto::ClusterState::Active,
                    members: vec![child_member.clone()],
                    parent: Some(root),
                    children: vec![],
                },
            ],
        };

        let fanout = topology_fanout(&map, map.clusters.iter());
        assert_eq!(fanout.targets.len(), 2);
        let (_, _, ReferralEdit::SetTopology { parent, children, .. }) =
            fanout.targets.iter().find(|(server, _, _)| *server == controller).unwrap();
        assert!(parent.is_none());
        assert_eq!(children[0].path, "/eu");
        assert_eq!(children[0].addrs, vec![child_member]);
        let (_, _, ReferralEdit::SetTopology { parent, children, .. }) =
            fanout.targets.iter().find(|(server, _, _)| *server == satellite).unwrap();
        assert!(children.is_empty());
        assert_eq!(parent.as_ref().unwrap().path, "/eu");
        assert_eq!(parent.as_ref().unwrap().addrs, vec![root_member]);
    }

    #[test]
    fn registration_fanout_updates_its_cluster_and_both_adjacent_levels() {
        let controller = admin_proto::AdminServerId::new();
        let joining = admin_proto::AdminServerId::new();
        let peer = admin_proto::AdminServerId::new();
        let grandchild_server = admin_proto::AdminServerId::new();
        let sibling_server = admin_proto::AdminServerId::new();
        let root = admin_proto::ResolverClusterId::new();
        let child = admin_proto::ResolverClusterId::new();
        let grandchild = admin_proto::ResolverClusterId::new();
        let sibling = admin_proto::ResolverClusterId::new();
        let resolver = |addr: &str| ResolverAddr {
            addr: addr.parse().unwrap(),
            auth: InfoAuth::Anonymous,
        };
        let server = |id, admin_addr: &str, member: ResolverAddr, cluster| ServerEntry {
            id,
            addr: admin_addr.parse().unwrap(),
            roles: vec![Role::Resolver],
            resolver: Some(member),
            cluster: Some(cluster),
            state: admin_proto::ServerState::Registered,
        };
        let root_member = resolver("10.1.0.1:4564");
        let joining_member = resolver("10.2.0.1:4564");
        let peer_member = resolver("10.2.0.2:4564");
        let grandchild_member = resolver("10.3.0.1:4564");
        let sibling_member = resolver("10.4.0.1:4564");
        let map = NetworkMap {
            version: 12,
            controller,
            servers: vec![
                server(controller, "10.1.0.1:4565", root_member.clone(), root),
                server(joining, "10.2.0.1:4565", joining_member.clone(), child),
                server(peer, "10.2.0.2:4565", peer_member.clone(), child),
                server(
                    grandchild_server,
                    "10.3.0.1:4565",
                    grandchild_member.clone(),
                    grandchild,
                ),
                server(sibling_server, "10.4.0.1:4565", sibling_member.clone(), sibling),
            ],
            clusters: vec![
                admin_proto::ClusterEntry {
                    id: root,
                    base: "/".into(),
                    state: admin_proto::ClusterState::Active,
                    members: vec![root_member],
                    parent: None,
                    children: vec![child, sibling],
                },
                admin_proto::ClusterEntry {
                    id: child,
                    base: "/eu".into(),
                    state: admin_proto::ClusterState::Active,
                    members: vec![joining_member, peer_member],
                    parent: Some(root),
                    children: vec![grandchild],
                },
                admin_proto::ClusterEntry {
                    id: grandchild,
                    base: "/eu/fr".into(),
                    state: admin_proto::ClusterState::Active,
                    members: vec![grandchild_member],
                    parent: Some(child),
                    children: vec![],
                },
                admin_proto::ClusterEntry {
                    id: sibling,
                    base: "/us".into(),
                    state: admin_proto::ClusterState::Active,
                    members: vec![sibling_member],
                    parent: Some(root),
                    children: vec![],
                },
            ],
        };

        let mut targets: Vec<_> = registration_topology_fanout(&map, joining)
            .unwrap()
            .targets
            .into_iter()
            .map(|(server, _, _)| server)
            .collect();
        targets.sort();
        let mut expected = vec![controller, joining, peer, grandchild_server];
        expected.sort();
        assert_eq!(targets, expected);
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
        assert_eq!(
            request_authorization(&Request::ExternalCaCsr),
            RequestAuthorization::LocalOnly,
        );
        assert_eq!(
            request_authorization(&Request::ExternalCaInstall(
                ExternalCaInstallRequest {
                    signed_cert_pem: "certificate".into(),
                    root_pem: None,
                },
            )),
            RequestAuthorization::LocalOnly,
        );
        assert_eq!(
            request_authorization(&Request::CaStatus),
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
    fn id_map_reconciliation_uses_latest_groups_for_every_live_name() {
        let now = 10_000;
        let record = |serial: u64, name: &str, groups: &[&str], not_after_unix| {
            ca_store::IssuedRecord {
                req: ca_store::QueuedReq::new(
                    NodeKind::Client,
                    String::new(),
                    name.to_string(),
                    Duration::from_secs(60),
                    "test".to_string(),
                    None,
                    None,
                ),
                serial,
                name: name.to_string(),
                spki_fp: String::new(),
                cert_pem: String::new(),
                groups: groups.iter().map(|group| (*group).to_string()).collect(),
                not_after_unix,
                issued_unix: serial,
                warnings: Vec::new(),
                revoked: None,
                push_done: true,
            }
        };
        let mut records = vec![
            record(1, "alice.example", &["users"], now - 1),
            record(2, "alice.example", &[], now + 100),
            record(3, "bob.example", &["old"], now + 100),
            record(4, "bob.example", &["users"], now + 100),
            record(5, "expired.example", &["users"], now - 1),
        ];
        records[1].req.renewal_of = Some(1);

        let selected: Vec<_> = records
            .iter()
            .enumerate()
            .filter(|(index, _)| reconcile_identity_at(&records, *index, now))
            .map(|(_, record)| (record.name.as_str(), record.groups[0].as_str()))
            .collect();
        assert_eq!(selected, vec![("alice.example", "users"), ("bob.example", "users")]);
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
    fn restore_enrollment_atomically_replaces_only_the_same_cluster_satellite() {
        let controller = admin_proto::AdminServerId::new();
        let old = admin_proto::AdminServerId::new();
        let fresh = admin_proto::AdminServerId::new();
        let mut map = NetworkMap::empty(controller);
        let member = ResolverAddr {
            addr: "10.0.0.10:4564".parse().unwrap(),
            auth: InfoAuth::Anonymous,
        };
        let initial = admin_proto::EnrollmentRequest {
            listen: "10.0.0.10:4565".parse().unwrap(),
            roles: vec![Role::Resolver],
            resolver_member: Some(member.clone()),
            resolver_members: vec![member.clone()],
            cluster: admin_proto::ClusterPlacement::Create { base: "/eu".into() },
            replaces: None,
        };
        let cluster = stage_enrollment(&mut map, old, &initial).unwrap();
        let mut replacement = initial.clone();
        replacement.cluster = admin_proto::ClusterPlacement::Join { cluster };
        replacement.replaces = Some(old);
        assert_eq!(stage_enrollment(&mut map, fresh, &replacement).unwrap(), cluster);
        assert!(map.servers.iter().all(|server| server.id != old));
        assert!(map.servers.iter().any(|server| server.id == fresh));
        assert!(map.clusters.iter().any(|entry| entry.id == cluster));

        replacement.replaces = Some(controller);
        assert!(
            stage_enrollment(&mut map, admin_proto::AdminServerId::new(), &replacement,)
                .is_err()
        );
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
            replaces: None,
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

    async fn signed_empty_crl(dir: &Path, name: &str) -> (String, Vec<u8>) {
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
        let lock = ConfigDirLock::acquire_for_ca_dir(dir).await.unwrap();
        let mut cadir = ca_store::CaDir::open(lock, dir).await.unwrap();
        cadir.store.write_crl(&key).await.unwrap();
        let pem = std::fs::read_to_string(cadir.store.crl_path()).unwrap();
        (pem, ca_der)
    }

    #[tokio::test]
    async fn immediate_crl_install_is_signed_atomic_and_all_or_nothing_on_validation() {
        let home = tempfile::tempdir().unwrap();
        let foreign = tempfile::tempdir().unwrap();
        let (home_crl, home_ca) = signed_empty_crl(home.path(), "home-ca").await;
        let (foreign_crl, _) = signed_empty_crl(foreign.path(), "foreign-ca").await;
        let root = tempfile::tempdir().unwrap();
        let config_lock = ConfigDirLock::acquire(root.path()).unwrap();
        let destinations = BTreeSet::from([
            root.path().join("admin/crl.pem"),
            root.path().join("resolver/crl.pem"),
        ]);

        apply_crl_to_destinations(
            &config_lock,
            &home_crl,
            &home_ca,
            destinations.clone(),
        )
        .await
        .unwrap();
        for path in &destinations {
            assert_eq!(std::fs::read_to_string(path).unwrap(), home_crl);
        }

        let error = apply_crl_to_destinations(
            &config_lock,
            &foreign_crl,
            &home_ca,
            destinations.clone(),
        )
        .await
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

    #[tokio::test]
    async fn controller_state_relocation_persists_route_map_and_crl_without_rollback() {
        use crate::admin_server_config::Roles;
        let home = tempfile::tempdir().unwrap();
        let (crl, home_ca) = signed_empty_crl(home.path(), "home-ca").await;
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
        let config_lock = ConfigDirLock::acquire(root.path()).unwrap();
        cfg.save(&config_lock, &cfg_path).unwrap();
        drop(config_lock);
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
        let state = Server::from_state(
            ConfigDirLock::acquire(root.path()).unwrap(),
            None,
            MutableState {
                cfg,
                map: old_map,
                ca: None,
                password_limiter: PasswordLimiter::default(),
            },
            Some(cfg_path.clone()),
            vec![],
            vec![],
            RootCertStore::empty(),
            CertificateDer::from(home_ca),
        )
        .unwrap();
        let req = ApplyControllerStateRequest {
            operation_id: admin_proto::OperationId::new(),
            controller,
            addr: new_addr,
            map: new_map.clone(),
            crl_pem: crl.clone(),
        };
        assert!(matches!(
            handle_apply_controller_state(&state, &req).await,
            ApplyControllerStateResponse::Ok
        ));
        let persisted = AdminServerConfig::load_for_recovery(&cfg_path).unwrap();
        assert_eq!(persisted.ca_addr, Some(new_addr));
        assert_eq!(state.read(move |state| state.map.clone()).await, new_map);
        assert_eq!(std::fs::read_to_string(root.path().join("crl.pem")).unwrap(), crl);

        let mut stale = req.clone();
        stale.map.version = 3;
        stale.addr = old_addr;
        stale.map.servers.iter_mut().find(|s| s.id == controller).unwrap().addr =
            old_addr;
        assert!(matches!(
            handle_apply_controller_state(&state, &stale).await,
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
