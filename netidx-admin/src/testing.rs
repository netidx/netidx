//! An in-process admin domain for integration tests.
//!
//! Every frontend of the admin plane — the strict CLI, the ratatui
//! TUI, and the graphix package — was manually tested against real
//! installs until this module existed: the e2e tests cover templates
//! and resolvers, the `admin_server` unit tests drive handlers with no
//! listener, and the TUI tests fake at the row level. This harness
//! stands up the real thing: it drives [`create_vaulted_ca`] (the same
//! founding ceremony `ca init` runs) with a rule-based [`Answerer`],
//! then serves the resulting config with [`crate::serve`] on a
//! loopback port — so a test exercises the genuine TLS transport,
//! password auth, session cache, and question flows.
//!
//! One domain per process: the founding flow writes
//! `admin-server.json` at the (test-redirected) user config path, and
//! the daemon locks the config root. [`TestAdminDomain::start`]
//! serializes on a global lock, so concurrent tests queue rather than
//! collide.
//!
//! Gated behind the `testing` feature; unix-only like the daemon.

use crate::{
    admin_proto::Secret,
    answer::{AdminDomainChoice, AdminDomainOption, Answerer, Field, OneTimeSecret},
    ca,
    config_lock::ConfigDirLock,
    fingerprint::Fingerprint,
    paths,
    plan::ca_setup::{NewCaOpts, create_vaulted_ca},
    serve,
    transport::CaIdentity,
};
use anyhow::{Context, Result, bail};
use enumflags2::BitFlags;
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        LazyLock, OnceLock,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use tempfile::TempDir;
use tokio::{net::TcpStream, sync::Mutex, task::JoinHandle};

/// Redirect the user config dir at a per-process tempdir so tests never
/// touch the developer's real `~/.config/netidx`. Held in a `OnceLock`
/// so it is cleaned up at process exit on the happy path.
fn ensure_xdg_redirect() {
    static GUARD: OnceLock<TempDir> = OnceLock::new();
    GUARD.get_or_init(|| {
        let td = TempDir::new().expect("test xdg tempdir");
        // SAFETY: env mutation before any test thread reads it is the
        // established e2e-test compromise; `start` serializes callers.
        unsafe {
            std::env::set_var("XDG_CONFIG_HOME", td.path());
            #[cfg(target_os = "macos")]
            std::env::set_var("HOME", td.path());
        }
        td
    });
}

/// A rule-based [`Answerer`] for driving setup ceremonies in tests:
/// provided values and defaults win, identity is confirmed, secrets
/// come from `password`, and shown-once secrets are captured rather
/// than lost. Strict where a rule can't answer — a question with no
/// provided value, no default, and no rule is a test failure, not a
/// silent guess.
pub struct SetupAnswerer {
    /// The answer to every password question (and its confirmation).
    pub password: String,
    /// The captured CA recovery password, once shown.
    pub recovery: Option<String>,
    /// Captured one-time admin passwords, `(admin, password)`.
    pub one_time: Vec<(String, String)>,
    pub notes: Vec<String>,
    pub warnings: Vec<String>,
}

impl SetupAnswerer {
    pub fn new(password: impl Into<String>) -> Self {
        SetupAnswerer {
            password: password.into(),
            recovery: None,
            one_time: vec![],
            notes: vec![],
            warnings: vec![],
        }
    }
}

#[async_trait::async_trait]
impl Answerer for SetupAnswerer {
    fn interactive(&self) -> bool {
        true
    }

    async fn text(
        &mut self,
        field: Field,
        provided: Option<String>,
        default: Option<&str>,
        required: bool,
    ) -> Result<Option<String>> {
        match provided.or_else(|| default.map(String::from)) {
            Some(v) => Ok(Some(v)),
            None if required => {
                bail!("SetupAnswerer has no answer for required {field:?}")
            }
            None => Ok(None),
        }
    }

    async fn choice(
        &mut self,
        field: Field,
        provided: Option<String>,
        choices: &[&str],
        default: Option<&str>,
    ) -> Result<String> {
        provided
            .or_else(|| default.map(String::from))
            .or_else(|| choices.first().map(|s| s.to_string()))
            .with_context(|| format!("SetupAnswerer has no answer for {field:?}"))
    }

    async fn select_admin_domain(
        &mut self,
        _domains: &[AdminDomainOption],
    ) -> Result<AdminDomainChoice> {
        bail!("SetupAnswerer does not select admin domains")
    }

    async fn confirm(
        &mut self,
        _field: Field,
        provided: Option<bool>,
        default: bool,
    ) -> Result<bool> {
        Ok(provided.unwrap_or(default))
    }

    async fn secret(
        &mut self,
        _field: Field,
        provided: Option<Secret>,
    ) -> Result<Secret> {
        Ok(provided.unwrap_or_else(|| Secret(self.password.clone())))
    }

    async fn announce(&mut self, _title: &str, _body: &str) -> Result<()> {
        Ok(())
    }

    async fn announce_identity(
        &mut self,
        _body: &str,
        _code: &Fingerprint,
    ) -> Result<()> {
        Ok(())
    }

    async fn confirm_identity(&mut self, _identity: &CaIdentity) -> Result<bool> {
        Ok(true)
    }

    fn show_verification_code(&mut self, _purpose: &str, _code: &Fingerprint) {}

    fn clear_verification_code(&mut self) {}

    fn progress(&mut self, _progress: crate::answer::Progress) {}

    fn note(&mut self, message: &str) {
        self.notes.push(message.to_string());
    }

    fn warn(&mut self, message: &str) {
        self.warnings.push(message.to_string());
    }

    async fn show_one_time_secret(
        &mut self,
        secret: OneTimeSecret,
        password: &str,
    ) -> Result<()> {
        match secret {
            OneTimeSecret::CaRecovery => self.recovery = Some(password.to_string()),
            OneTimeSecret::AdminPassword { admin } => {
                self.one_time.push((admin, password.to_string()))
            }
        }
        Ok(())
    }
}

/// A live admin domain: a founded CA and a serving daemon on loopback.
/// Dropping it stops the daemon; the on-disk state lives inside the
/// process's redirected config tempdir and is cleaned at process exit.
pub struct TestAdminDomain {
    /// The CA directory.
    pub ca_dir: PathBuf,
    /// The served `admin-server.json`.
    pub cfg_path: PathBuf,
    /// Where the daemon listens.
    pub listen: SocketAddr,
    /// The admin domain's TLS domain.
    pub domain: String,
    /// The founding superuser role admin.
    pub admin: String,
    /// Its password.
    pub password: String,
    /// The CA certificate's fingerprint (what `confirm_identity` shows).
    pub fingerprint: Fingerprint,
    /// The one-time CA recovery password minted at founding.
    pub recovery_password: String,
    server: JoinHandle<Result<()>>,
    _serial: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for TestAdminDomain {
    fn drop(&mut self) {
        self.server.abort();
    }
}

static SERIAL: LazyLock<std::sync::Arc<Mutex<()>>> =
    LazyLock::new(|| std::sync::Arc::new(Mutex::new(())));
static NEXT_DOMAIN: AtomicU32 = AtomicU32::new(0);

impl TestAdminDomain {
    /// Found a fresh admin domain and serve it. `TEST_PASSWORD` is the
    /// superuser's password.
    pub async fn start() -> Result<Self> {
        Self::start_with_password(Self::TEST_PASSWORD).await
    }

    pub const TEST_PASSWORD: &str = "correct-horse-battery-staple";

    pub async fn start_with_password(password: &str) -> Result<Self> {
        let serial = SERIAL.clone().lock_owned().await;
        ensure_xdg_redirect();
        let n = NEXT_DOMAIN.fetch_add(1, Ordering::Relaxed);
        let domain = format!("test{n}.netidx");
        let admin = "root".to_string();
        let listen: SocketAddr = {
            let l = std::net::TcpListener::bind("127.0.0.1:0")?;
            l.local_addr()?
        };
        let root = paths::user_config_root()?;
        // the previous domain's daemon holds this lock until its aborted
        // task has unwound, which outlives that domain's drop
        let config_lock = {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            loop {
                match ConfigDirLock::acquire(&root) {
                    Ok(l) => break l,
                    Err(_) if tokio::time::Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(50)).await
                    }
                    Err(e) => return Err(e).context("acquiring the test config lock"),
                }
            }
        };
        tokio::fs::create_dir_all(config_lock.root()).await?;
        let ca_dir = config_lock.root().join(format!("test-ca-{n}"));
        let mut ans = SetupAnswerer::new(password);
        let opts = NewCaOpts {
            dir: ca_dir.clone(),
            common_name: Some(format!("ca.{domain}")),
            domain: Some(domain.clone()),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec![],
            key_bits: ca::MIN_KEY_BITS,
            ca_validity: Duration::from_secs(30 * 86400),
            leaf_validity: Duration::from_secs(7 * 86400),
            ca_renew_threshold: Duration::from_secs(86400),
            admin: Some(admin.clone()),
            allowed_san: vec!["*".to_string()],
            max_validity: Duration::from_secs(7 * 86400),
            id_map_groups: vec!["users".to_string()],
            server_enroll_scopes: vec!["/".to_string()],
            server_enroll_roles: BitFlags::all(),
            insecure_no_tpm: true,
            setup_server: Some(true),
            listen: Some(listen),
            listen_hint: None,
            units_dir: None,
        };
        create_vaulted_ca(&mut ans, &config_lock, opts)
            .await
            .context("founding the test admin domain")?;
        let recovery_password = ans
            .recovery
            .clone()
            .context("founding never showed the CA recovery password")?;
        let cfg_path = paths::user_admin_server_config()?;
        // The founding flow advertises over mDNS by default — a test
        // domain must not beacon the LAN.
        {
            let mut cfg = crate::admin_server_config::load(&cfg_path)?;
            cfg.mdns = false;
            crate::admin_server_config::save(&config_lock, &cfg_path, &cfg)?;
        }
        let fingerprint = {
            let pem = tokio::fs::read(ca_dir.join("certificate.pem")).await?;
            Fingerprint::of_cert_pem(&pem)?
        };
        // The daemon takes its own lock on the same root.
        drop(config_lock);
        let server = tokio::spawn(serve(cfg_path.clone()));
        let t = TestAdminDomain {
            ca_dir,
            cfg_path,
            listen,
            domain,
            admin,
            password: password.to_string(),
            fingerprint,
            recovery_password,
            server,
            _serial: serial,
        };
        t.await_ready().await?;
        Ok(t)
    }

    /// Whether a connect to this domain asks the operator to confirm the
    /// CA's identity: it does unless this domain's certificate is the one
    /// in the user CA directory, which a resolve verifies against silently.
    pub fn gesture_expected(&self) -> bool {
        let local = paths::user_ca_dir()
            .ok()
            .and_then(|d| std::fs::read(d.join("certificate.pem")).ok())
            .and_then(|pem| Fingerprint::of_cert_pem(&pem).ok());
        local.as_ref() != Some(&self.fingerprint)
    }

    /// Mint a role admin that may manage admins, over a password session
    /// of the founding superuser; returns the one-time password the CA
    /// requires to be changed at first login. Nothing is left in the
    /// session cache.
    pub async fn mint_role_admin(&self, name: &str) -> Result<String> {
        let mut ans = SetupAnswerer::new(&self.password);
        let session = crate::ops::open_admin_password_session(
            &mut ans,
            Some(self.listen),
            None,
            Some(self.admin.clone()),
            Some(Secret(self.password.clone())),
        )
        .await?;
        let target = crate::ops::AdminTarget::Remote { session };
        let policy = netidx_admin_proto::policy::Policy {
            allowed_san: vec![],
            max_validity: Duration::from_secs(3600),
            id_map_groups: vec![],
            server_enroll_scopes: vec![],
            server_enroll_roles: BitFlags::empty(),
            perms_edit_scopes: vec![],
            may_manage_admins: true,
            service_control_scopes: vec![],
        };
        let pw = crate::ops::roster::add_role_admin(&target, name, policy).await?;
        Ok(pw.to_string())
    }

    /// Wait until the daemon accepts connections (or fails to start).
    async fn await_ready(&self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if self.server.is_finished() {
                bail!("the admin server exited during startup");
            }
            match TcpStream::connect(self.listen).await {
                Ok(_) => return Ok(()),
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await
                }
                Err(e) => bail!("admin server never came up on {}: {e}", self.listen),
            }
        }
    }
}
