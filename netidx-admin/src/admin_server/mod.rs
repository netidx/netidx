//! Admin server: the per-host daemon behind discovery-driven setup. It
//! answers [`admin_proto::Request::GetInfo`] with this host's local facts + known
//! peers, and — on the host holding the CA — turns a join client's
//! [`admin_proto::Request::Sign`] into a signed cert and an
//! [`admin_proto::Request::Enroll`] into
//! a new admin server's reserved-SAN serving cert. After a successful
//! sign it pushes id-map registrations to every id-map-role peer.
//!
//! The request-handling cores ([`issuance::handle_sign_request`],
//! [`enrollment::handle_enroll_request`], [`issuance::handle_add_identity`]) are pure of TLS
//! and directly testable: the sign/enroll paths authenticate the supplied
//! administrator credential, enforce that administrator's live [`ca_vault::Policy`],
//! sign with the controller's own credential, and append an audit line. The
//! TLS accept loop is a thin shell over them.

mod admins;
mod auth;
mod ca_ops;
mod enrollment;
mod issuance;
mod password_limiter;
mod permissions;
mod queue;
mod request;
mod revocation;
mod runtime;
mod service_control;
#[cfg(test)]
mod test_support;
mod topology;

use auth::PasswordAttempt;
use issuance::handle_add_identity;
use runtime::load_serving_keypair;
use topology::{apply_referral_edit_local, local_resolver_data, roles_of};

#[cfg(test)]
pub(crate) use ca_ops::read_autorenew_password;
pub(crate) use ca_ops::read_autorenew_password_async;
pub(crate) use runtime::local_socket_path;
pub use runtime::{load_roots, serve};

use crate::{
    admin_client,
    admin_proto::{
        self, AddIdentityRequest, AddIdentityResponse, ApplyReferralEditRequest,
        ApplyReferralEditResponse, NetworkMap, Role, ServerEntry,
    },
    admin_server_config::AdminServerConfig,
    ca_store,
    config_lock::ConfigDirLock,
    netmap,
};
use anyhow::{Context, Result, bail};
use enumflags2::BitFlags;
use log::{error, warn};
use parking_lot::Mutex;
use password_limiter::{PasswordLimiter, Reservation};
use rustls::RootCertStore;
use rustls_pki_types::CertificateDer;
use sha2::{Digest, Sha256};
use std::{
    net::IpAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    sync::{Notify, RwLock},
};

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

#[cfg(test)]
mod state_tests {
    use super::auth::{
        PreparedAdminAuthentication, PreparedServerUnlock, authenticate, server_unlock,
    };
    use super::*;

    #[test]
    fn custom_config_root_retains_the_offline_ca_lock() {
        let root = tempfile::tempdir().unwrap();
        let ca_dir = root.path().join("ca");
        let config_lock = ConfigDirLock::acquire(root.path()).unwrap();
        let alias = config_lock
            .ca_alias_root(&ca_dir)
            .unwrap()
            .map(ConfigDirLock::acquire)
            .transpose()
            .unwrap();
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
    async fn concurrent_password_attempt_waits_for_the_argon_owner() {
        let state = limiter_server();
        let source = "203.0.113.10".parse().unwrap();
        let (request_attempt, delay) = state.begin_password_attempt(source).await;
        assert_eq!(delay, Duration::ZERO);
        let argon_attempt = request_attempt.clone();
        drop(request_attempt);
        let waiting = tokio::spawn({
            let state = state.clone();
            async move { state.begin_password_attempt(source).await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());

        argon_attempt.finish(false);
        drop(argon_attempt);
        let (next, delay) =
            tokio::time::timeout(Duration::from_secs(1), waiting).await.unwrap().unwrap();
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
        let prepared = PreparedAdminAuthentication::Password(Ok(authenticated.clone()));
        let first = state
            .write(|state| {
                authenticate(state.ca.as_mut().unwrap(), &credential, &prepared)
            })
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
        let prepared = PreparedAdminAuthentication::Password(Ok(authenticated));
        let stale = state
            .write(|state| {
                authenticate(state.ca.as_mut().unwrap(), &credential, &prepared)
            })
            .await;
        assert!(stale.is_err());
    }

    #[tokio::test]
    async fn prepared_authentication_must_match_the_credential_kind() {
        let dir = tempfile::tempdir().unwrap();
        let lock = ConfigDirLock::acquire_for_ca_dir(dir.path()).await.unwrap();
        let mut ca = ca_store::CaDir::open(lock, dir.path()).await.unwrap();
        ca.vault
            .create(b"mock-ca-key", "alice", "pw", crate::ca_policy::recovery_policy())
            .await
            .unwrap();
        let authenticated = ca.vault.authenticate("alice", "pw").unwrap();
        let password = admin_proto::AdminCredential::password("alice", "pw");
        let session = admin_proto::AdminCredential::Session {
            token: admin_proto::Secret("not-a-session".into()),
        };

        assert!(
            authenticate(&mut ca, &password, &PreparedAdminAuthentication::Session)
                .is_err()
        );
        assert!(
            authenticate(
                &mut ca,
                &session,
                &PreparedAdminAuthentication::Password(Ok(authenticated)),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn rwlock_state_revalidates_worker_server_unlock() {
        let dir = tempfile::tempdir().unwrap();
        let lock = ConfigDirLock::acquire_for_ca_dir(dir.path()).await.unwrap();
        let mut ca = ca_store::CaDir::open(lock, dir.path()).await.unwrap();
        ca.vault
            .create(
                b"mock-ca-key",
                "recovery",
                "rpw",
                crate::ca_policy::recovery_policy(),
            )
            .await
            .unwrap();
        ca.vault
            .add_signing_slot(
                "rpw",
                AUTORENEW_ADMIN,
                "apw",
                crate::ca_policy::autorenew_policy(),
            )
            .await
            .unwrap();
        let unlocked = ca.vault.snapshot().unwrap().unlock("apw").unwrap();
        let prepared =
            PreparedServerUnlock::from_result(Ok(triomphe::Arc::new(unlocked)));
        let state = test_server(Some(ca));

        state
            .write_async(async move |state| {
                state
                    .ca
                    .as_mut()
                    .unwrap()
                    .vault
                    .remove_slot(AUTORENEW_ADMIN, false)
                    .await
                    .unwrap();
            })
            .await;
        let stale = state
            .write_async(async |state| {
                server_unlock(state.ca.as_mut().unwrap(), &prepared).await
            })
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

struct Server {
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
    password_attempt_completed: Notify,
    /// The one home CA for application-level admin authorization. Other
    /// certificates in `trusted.pem` remain data-plane federation anchors.
    home_ca_der: CertificateDer<'static>,
}

impl Server {
    async fn new(
        config_lock: ConfigDirLock,
        ca_alias_lock: Option<ConfigDirLock>,
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
            password_attempt_completed: Notify::new(),
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

    async fn ca_dir(&self) -> Option<PathBuf> {
        self.read(move |state| state.ca.as_ref().map(|ca| ca.dir().to_path_buf())).await
    }

    async fn begin_password_attempt(
        self: &Arc<Self>,
        source: IpAddr,
    ) -> (PasswordAttempt, Duration) {
        loop {
            let completed = self.password_attempt_completed.notified();
            tokio::pin!(completed);
            completed.as_mut().enable();
            match self.write(move |state| state.password_limiter.reserve(source)).await {
                Reservation::Ready(delay) => {
                    return (PasswordAttempt::new(self, source), delay);
                }
                Reservation::InFlight => completed.await,
            }
        }
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
                    Ok(()) => ApplyReferralEditResponse::Ok(()),
                    Err(e) => ApplyReferralEditResponse::Err { reason: format!("{e:#}") },
                }
            }
            None => ApplyReferralEditResponse::Err {
                reason: "this host has no resolver role to edit".to_string(),
            },
        })
        .await
    }

    async fn roles(&self) -> BitFlags<Role> {
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

async fn ca_dir(state: &Server) -> Option<PathBuf> {
    state.ca_dir().await
}

async fn audit(ca_dir: &Path, admin: &str, op: &str, name: &str, validity: Duration) {
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
