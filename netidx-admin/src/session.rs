use crate::{
    admin_proto::{AdminCredential, LoginResponse, LogoutResponse, Secret},
    ca_vault::{Authenticated, CAVault},
};
use base64::Engine;
use parking_lot::{Mutex, RwLock};
use rand::RngExt;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const DEFAULT_ABSOLUTE_LIFETIME: Duration = Duration::from_secs(8 * 60 * 60);
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MAX_SESSIONS: usize = 4096;

#[derive(Clone, Copy)]
struct Settings {
    absolute: Duration,
    idle: Duration,
}

#[derive(Clone)]
struct Session {
    slot_id: uuid::Uuid,
    credential_revision: u64,
    _admin: String,
    _issued: u64,
    last_use: u64,
    absolute_deadline: u64,
}

pub struct SessionStore {
    settings: RwLock<Settings>,
    sessions: Mutex<HashMap<[u8; 32], Session>>,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self {
            settings: RwLock::new(Settings {
                absolute: DEFAULT_ABSOLUTE_LIFETIME,
                idle: DEFAULT_IDLE_TIMEOUT,
            }),
            sessions: Mutex::new(HashMap::new()),
        }
    }
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn token_hash(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

impl SessionStore {
    pub fn configure(&self, absolute: Option<Duration>, idle: Option<Duration>) {
        let mut settings = self.settings.write();
        settings.absolute = absolute.unwrap_or(DEFAULT_ABSOLUTE_LIFETIME);
        settings.idle = idle.unwrap_or(DEFAULT_IDLE_TIMEOUT);
    }

    pub fn login(&self, vault: &CAVault, credential: &AdminCredential) -> LoginResponse {
        let AdminCredential::Password { admin, password } = credential else {
            return LoginResponse::Err {
                reason: "login requires an administrator password".to_string(),
            };
        };
        let authenticated = match vault.authenticate(admin, password.as_str()) {
            Ok(a) => a,
            Err(_) => {
                return LoginResponse::Err { reason: "authentication failed".into() };
            }
        };
        let mut raw = [0u8; 32];
        rand::rng().fill(&mut raw);
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
        let hash = token_hash(&token);
        let issued = now();
        let settings = *self.settings.read();
        let absolute_deadline = issued.saturating_add(settings.absolute.as_secs());
        let record = Session {
            slot_id: authenticated.slot_id,
            credential_revision: authenticated.credential_revision,
            _admin: authenticated.admin.clone(),
            _issued: issued,
            last_use: issued,
            absolute_deadline,
        };
        let mut sessions = self.sessions.lock();
        if sessions.len() >= MAX_SESSIONS
            && let Some(oldest) =
                sessions.iter().min_by_key(|(_, s)| s.last_use).map(|(h, _)| *h)
        {
            sessions.remove(&oldest);
        }
        sessions.insert(hash, record);
        LoginResponse::Ok {
            admin: authenticated.admin,
            token: Secret(token),
            issued_unix: issued,
            absolute_deadline_unix: absolute_deadline,
            idle_timeout_secs: settings.idle.as_secs(),
        }
    }

    pub fn authenticate(
        &self,
        vault: &CAVault,
        credential: &AdminCredential,
    ) -> Result<Authenticated, String> {
        match credential {
            AdminCredential::Password { admin, password } => vault
                .authenticate(admin, password.as_str())
                .map_err(|_| "authentication failed".to_string()),
            AdminCredential::Session { token } => {
                let hash = token_hash(token.as_str());
                let time = now();
                let idle = self.settings.read().idle.as_secs();
                let record = {
                    let mut sessions = self.sessions.lock();
                    let Some(record) = sessions.get(&hash).cloned() else {
                        return Err("login required: session is unknown".to_string());
                    };
                    if time >= record.absolute_deadline
                        || time.saturating_sub(record.last_use) >= idle
                    {
                        sessions.remove(&hash);
                        return Err("login required: session expired".to_string());
                    }
                    record
                };
                let authenticated = vault
                    .resolve_session_slot(record.slot_id, record.credential_revision)
                    .map_err(|_| {
                        self.sessions.lock().remove(&hash);
                        "login required: administrator credential was revoked".to_string()
                    })?;
                let mut sessions = self.sessions.lock();
                if let Some(current) = sessions.get_mut(&hash) {
                    current.last_use = time;
                }
                Ok(authenticated)
            }
        }
    }

    pub fn logout(&self, credential: &AdminCredential) -> LogoutResponse {
        let AdminCredential::Session { token } = credential else {
            return LogoutResponse::Err {
                reason: "logout requires a session token".into(),
            };
        };
        self.sessions.lock().remove(&token_hash(token.as_str()));
        LogoutResponse::Ok
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.sessions.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        admin_proto::Role,
        ca_policy::{Policy, SlotKind},
    };

    fn policy(scope: &str) -> Policy {
        Policy {
            allowed_san: vec![],
            max_validity: Duration::from_secs(60),
            id_map_groups: vec![],
            server_enroll_scopes: vec![scope.into()],
            server_enroll_roles: vec![Role::Resolver],
            perms_edit_scopes: vec![scope.into()],
            may_manage_admins: false,
            service_control_scopes: vec![],
        }
    }

    fn setup() -> (tempfile::TempDir, CAVault) {
        let dir = tempfile::tempdir().unwrap();
        let mut vault = CAVault::new(dir.path().to_path_buf());
        vault.create(b"mock-ca-key", "recovery", "rpw", policy("/")).unwrap();
        vault.add_role_slot("alice", "pw", policy("/eu")).unwrap();
        (dir, vault)
    }

    fn login(store: &SessionStore, vault: &CAVault) -> Secret {
        match store.login(vault, &AdminCredential::password("alice", "pw")) {
            LoginResponse::Ok { token, .. } => token,
            LoginResponse::Err { reason } => panic!("{reason}"),
        }
    }

    #[test]
    fn login_use_logout_and_restart() {
        let (_dir, vault) = setup();
        let store = SessionStore::default();
        let token = login(&store, &vault);
        let credential = AdminCredential::Session { token: token.clone() };
        let auth = store.authenticate(&vault, &credential).unwrap();
        assert_eq!(auth.admin, "alice");
        assert_eq!(auth.kind, SlotKind::Role);
        assert!(matches!(store.logout(&credential), LogoutResponse::Ok));
        assert!(store.authenticate(&vault, &credential).is_err());

        let token = login(&store, &vault);
        let restarted = SessionStore::default();
        assert!(
            restarted.authenticate(&vault, &AdminCredential::Session { token }).is_err()
        );
    }

    #[test]
    fn expiry_slot_recreation_and_live_policy_changes_invalidate_correctly() {
        let (_dir, mut vault) = setup();
        let store = SessionStore::default();
        let token = login(&store, &vault);
        let credential = AdminCredential::Session { token: token.clone() };
        let mut reduced = policy("/eu/narrow");
        reduced.may_manage_admins = true;
        vault.set_policy("alice", reduced.clone()).unwrap();
        assert_eq!(store.authenticate(&vault, &credential).unwrap().policy, reduced);

        vault.remove_slot("alice", false).unwrap();
        vault.add_role_slot("alice", "pw", policy("/eu")).unwrap();
        assert!(store.authenticate(&vault, &credential).is_err());

        let expiring = SessionStore::default();
        expiring.configure(Some(Duration::ZERO), Some(Duration::ZERO));
        let expired = login(&expiring, &vault);
        assert!(
            expiring
                .authenticate(&vault, &AdminCredential::Session { token: expired })
                .is_err()
        );
    }

    #[test]
    fn capacity_prunes_the_least_recently_used_session() {
        let (_dir, vault) = setup();
        let store = SessionStore::default();
        {
            let mut sessions = store.sessions.lock();
            for i in 0..MAX_SESSIONS {
                let mut hash = [0u8; 32];
                hash[..8].copy_from_slice(&(i as u64).to_be_bytes());
                sessions.insert(
                    hash,
                    Session {
                        slot_id: uuid::Uuid::nil(),
                        credential_revision: 0,
                        _admin: "old".into(),
                        _issued: 1,
                        last_use: i as u64,
                        absolute_deadline: u64::MAX,
                    },
                );
            }
        }
        let _ = login(&store, &vault);
        assert_eq!(store.len(), MAX_SESSIONS);
        assert!(!store.sessions.lock().contains_key(&[0u8; 32]));
    }
}
