use crate::{
    admin_proto::{AdminCredential, LoginOk, LoginResponse, LogoutResponse, Secret},
    ca_vault::{Authenticated, CAVault},
};
use base64::Engine;
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
    settings: Settings,
    sessions: HashMap<[u8; 32], Session>,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self {
            settings: Settings {
                absolute: DEFAULT_ABSOLUTE_LIFETIME,
                idle: DEFAULT_IDLE_TIMEOUT,
            },
            sessions: HashMap::new(),
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
    pub fn configure(&mut self, absolute: Option<Duration>, idle: Option<Duration>) {
        self.settings.absolute = absolute.unwrap_or(DEFAULT_ABSOLUTE_LIFETIME);
        self.settings.idle = idle.unwrap_or(DEFAULT_IDLE_TIMEOUT);
    }

    pub fn login_authenticated(&mut self, authenticated: Authenticated) -> LoginResponse {
        let mut raw = [0u8; 32];
        rand::rng().fill(&mut raw);
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
        let hash = token_hash(&token);
        let issued = now();
        let settings = self.settings;
        let absolute_deadline = issued.saturating_add(settings.absolute.as_secs());
        let record = Session {
            slot_id: authenticated.slot_id,
            credential_revision: authenticated.credential_revision,
            _admin: authenticated.admin.clone(),
            _issued: issued,
            last_use: issued,
            absolute_deadline,
        };
        if self.sessions.len() >= MAX_SESSIONS
            && let Some(oldest) =
                self.sessions.iter().min_by_key(|(_, s)| s.last_use).map(|(h, _)| *h)
        {
            self.sessions.remove(&oldest);
        }
        self.sessions.insert(hash, record);
        LoginResponse::Ok(LoginOk {
            admin: authenticated.admin,
            token: Secret(token),
            issued_unix: issued,
            absolute_deadline_unix: absolute_deadline,
            idle_timeout_secs: settings.idle.as_secs(),
        })
    }

    pub fn authenticate_session(
        &mut self,
        vault: &CAVault,
        token: &Secret,
    ) -> Result<Authenticated, String> {
        let hash = token_hash(token.as_str());
        let time = now();
        let idle = self.settings.idle.as_secs();
        let record = {
            let Some(record) = self.sessions.get(&hash).cloned() else {
                return Err("login required: session is unknown".to_string());
            };
            if time >= record.absolute_deadline
                || time.saturating_sub(record.last_use) >= idle
            {
                self.sessions.remove(&hash);
                return Err("login required: session expired".to_string());
            }
            record
        };
        let authenticated = vault
            .resolve_session_slot(record.slot_id, record.credential_revision)
            .map_err(|_| {
                self.sessions.remove(&hash);
                "login required: administrator credential was revoked".to_string()
            })?;
        if let Some(current) = self.sessions.get_mut(&hash) {
            current.last_use = time;
        }
        Ok(authenticated)
    }

    pub fn logout(&mut self, credential: &AdminCredential) -> LogoutResponse {
        let AdminCredential::Session { token } = credential else {
            return LogoutResponse::Err {
                reason: "logout requires a session token".into(),
            };
        };
        self.sessions.remove(&token_hash(token.as_str()));
        LogoutResponse::Ok(())
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_proto::Role;
    use netidx_admin_proto::policy::{Policy, SlotKind};

    fn policy(scope: &str) -> Policy {
        Policy {
            allowed_san: vec![],
            max_validity: Duration::from_secs(60),
            id_map_groups: vec![],
            server_enroll_scopes: vec![scope.into()],
            server_enroll_roles: Role::Resolver.into(),
            perms_edit_scopes: vec![scope.into()],
            may_manage_admins: false,
            service_control_scopes: vec![],
        }
    }

    async fn setup() -> (tempfile::TempDir, CAVault) {
        let dir = tempfile::tempdir().unwrap();
        let mut vault = CAVault::new(dir.path().to_path_buf());
        vault.create(b"mock-ca-key", "recovery", "rpw", policy("/")).await.unwrap();
        vault.add_role_slot("alice", "pw", policy("/eu")).await.unwrap();
        (dir, vault)
    }

    fn login(store: &mut SessionStore, vault: &CAVault) -> Secret {
        let authenticated = vault.authenticate("alice", "pw").unwrap();
        match store.login_authenticated(authenticated) {
            LoginResponse::Ok(LoginOk { token, .. }) => token,
            LoginResponse::Err { reason } => panic!("{reason}"),
        }
    }

    #[tokio::test]
    async fn login_use_logout_and_restart() {
        let (_dir, vault) = setup().await;
        let mut store = SessionStore::default();
        let token = login(&mut store, &vault);
        let credential = AdminCredential::Session { token: token.clone() };
        let AdminCredential::Session { token } = &credential else { unreachable!() };
        let auth = store.authenticate_session(&vault, token).unwrap();
        assert_eq!(auth.admin, "alice");
        assert_eq!(auth.kind, SlotKind::Role);
        assert!(matches!(store.logout(&credential), LogoutResponse::Ok(())));
        assert!(store.authenticate_session(&vault, token).is_err());

        let token = login(&mut store, &vault);
        let mut restarted = SessionStore::default();
        assert!(restarted.authenticate_session(&vault, &token).is_err());
    }

    #[tokio::test]
    async fn expiry_slot_recreation_and_live_policy_changes_invalidate_correctly() {
        let (_dir, mut vault) = setup().await;
        let mut store = SessionStore::default();
        let token = login(&mut store, &vault);
        let credential = AdminCredential::Session { token: token.clone() };
        let mut reduced = policy("/eu/narrow");
        reduced.may_manage_admins = true;
        vault.set_policy("alice", reduced.clone()).await.unwrap();
        let AdminCredential::Session { token } = &credential else { unreachable!() };
        assert_eq!(store.authenticate_session(&vault, token).unwrap().policy, reduced);

        vault.remove_slot("alice", false).await.unwrap();
        vault.add_role_slot("alice", "pw", policy("/eu")).await.unwrap();
        assert!(store.authenticate_session(&vault, token).is_err());

        let mut expiring = SessionStore::default();
        expiring.configure(Some(Duration::ZERO), Some(Duration::ZERO));
        let expired = login(&mut expiring, &vault);
        assert!(expiring.authenticate_session(&vault, &expired).is_err());
    }

    #[tokio::test]
    async fn capacity_prunes_the_least_recently_used_session() {
        let (_dir, vault) = setup().await;
        let mut store = SessionStore::default();
        for i in 0..MAX_SESSIONS {
            let mut hash = [0u8; 32];
            hash[..8].copy_from_slice(&(i as u64).to_be_bytes());
            store.sessions.insert(
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
        let _ = login(&mut store, &vault);
        assert_eq!(store.len(), MAX_SESSIONS);
        assert!(!store.sessions.contains_key(&[0u8; 32]));
    }
}
