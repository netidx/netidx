use super::{
    AUTORENEW_ADMIN, MutableState, Server,
    auth::{PreparedAdminAuthentication, PreparedServerUnlock},
    password_limiter::PasswordLimiter,
};
use crate::{
    admin_proto::{self, AdminDomainMap},
    admin_server_config::AdminServerConfig,
    ca::{Ca, CaParams, MIN_KEY_BITS, Subject},
    ca_store,
    config_lock::ConfigDirLock,
};
use rustls::RootCertStore;
use rustls_pki_types::CertificateDer;
use std::{io::Cursor, path::Path, path::PathBuf, sync::Arc, time::Duration};

/// A [`Server`] with no listener and no identity, for testing the request
/// handlers directly. `ca` decides whether this host holds the CA, which is
/// the axis most of the interesting behaviour turns on.
pub(super) fn test_server(ca: Option<ca_store::CaDir>) -> Arc<Server> {
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
            map: AdminDomainMap::empty(id),
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

/// A real signing CA plus prepared credentials, for handler tests that
/// have to issue a leaf.
pub(super) struct SigningCa {
    pub dir: tempfile::TempDir,
    pub ca: ca_store::CaDir,
    pub server_unlock: PreparedServerUnlock,
    pub authentication: PreparedAdminAuthentication,
    pub credential: admin_proto::AdminCredential,
}

pub(super) async fn signing_ca() -> SigningCa {
    let dir = tempfile::tempdir().unwrap();
    Ca::init(
        &CaParams {
            directory: dir.path().to_path_buf(),
            subject: Subject::cn("test-ca"),
            san: vec![],
            key_bits: MIN_KEY_BITS,
            validity: Duration::from_secs(30 * 86400),
        },
        None,
    )
    .unwrap();
    let key = std::fs::read(dir.path().join("private.key")).unwrap();
    let lock = ConfigDirLock::acquire_for_ca_dir(dir.path()).await.unwrap();
    let mut ca = ca_store::CaDir::open(lock, dir.path()).await.unwrap();
    ca.vault
        .create(&key, "recovery", "rpw", netidx_admin_proto::policy::recovery_policy())
        .await
        .unwrap();
    ca.vault
        .add_signing_slot(
            "rpw",
            AUTORENEW_ADMIN,
            "apw",
            netidx_admin_proto::policy::autorenew_policy(),
        )
        .await
        .unwrap();
    ca.autorenew_pw = Some(zeroize::Zeroizing::new("apw".to_string()));
    let snapshot = ca.vault.snapshot().unwrap();
    let authenticated = snapshot.authenticate(AUTORENEW_ADMIN, "apw").unwrap();
    let unlocked = snapshot.unlock("apw").unwrap();
    SigningCa {
        dir,
        ca,
        server_unlock: PreparedServerUnlock::from_result(Ok(triomphe::Arc::new(
            unlocked,
        ))),
        authentication: PreparedAdminAuthentication::Password(Ok(authenticated)),
        credential: admin_proto::AdminCredential::password(AUTORENEW_ADMIN, "apw"),
    }
}

pub(super) async fn signed_empty_crl(dir: &Path, name: &str) -> (String, Vec<u8>) {
    let ca = Ca::init(
        &CaParams {
            directory: dir.to_path_buf(),
            subject: Subject::cn(name),
            san: vec![],
            key_bits: MIN_KEY_BITS,
            validity: Duration::from_secs(30 * 86400),
        },
        None,
    )
    .unwrap();
    drop(ca);
    let key = std::fs::read(dir.join("private.key")).unwrap();
    let ca_pem = std::fs::read(dir.join("certificate.pem")).unwrap();
    let ca_der =
        rustls_pemfile::certs(&mut Cursor::new(ca_pem)).next().unwrap().unwrap().to_vec();
    let lock = ConfigDirLock::acquire_for_ca_dir(dir).await.unwrap();
    let mut ca = ca_store::CaDir::open(lock, dir).await.unwrap();
    ca.store.write_crl(&key).await.unwrap();
    let pem = std::fs::read_to_string(ca.store.crl_path()).unwrap();
    (pem, ca_der)
}
