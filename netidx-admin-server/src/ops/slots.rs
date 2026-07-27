//! Local CA slot & external-PKI management behind the Answerer seam.
//!
//! These are the **local** admin ops — no remote admin server, no glyph. Their
//! caller supplies either a running-daemon endpoint or an already-held offline
//! installation guard. The live path hot-swaps over the control socket
//! (SO_PEERCRED superuser, no secret); the offline path unlocks with the box's
//! autorenew keytab or, failing that, the operator's recovery password (via the Answerer, always

//! folded to canonical form by [`offline_ca::unlock_held`] — which is the fix for
//! the external path's former raw-password unlock). The only secrets are the
//! recovery password ([`Field::RecoveryPassword`]) and a freshly minted one to
//! show ([`ca_setup::show_recovery_password`]); everything else is a note.
//!
//! The heavy lifting (TPM gate + seal, slot minting, server setup) reuses the
//! install flow's [`ca_setup`]/[`server_setup`] helpers, so there is one
//! implementation of the security-critical parts.

#[cfg(test)]
use crate::admin_server_config::AdminServerConfig;
use crate::{
    admin_domain,
    admin_proto::{AdminServerId, CONTROLLER_ROLE_URI, NodeKind, SERVING_SAN},
    admin_server::read_autorenew_password_async,
    admin_server_config,
    answer::{Answerer, Field},
    atomic,
    ca::{self, Ca, CaLifetimes, SanEntry, Subject},
    ca_store::CaDir,
    ca_vault::{self, CAVault},
    config_lock::ConfigDirLock,
    fingerprint::Fingerprint,
    local, offline_ca, paths,
    plan::{ca_setup, server_setup, service::ServiceNeed},
    tls, transport,
};
use anyhow::{Context, Result, bail};
use netidx_admin_proto::policy::recovery_policy;
use serde_derive::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

pub enum CaAccess {
    Running {
        config: PathBuf,
    },
    Offline {
        config: Option<PathBuf>,
        lock: ConfigDirLock,
        ca_alias_lock: Option<ConfigDirLock>,
    },
}

// -- ca auto-approve ---------------------------------------------------------

/// The outcome of [`auto_approve`], for the CLI to narrate.
pub enum AutoApproveOutcome {
    /// The running daemon rotated the box credential in-process (no downtime).
    HotSwapped { warning: Option<String> },
    /// The slot was (re-)minted offline under the installation guard.
    Offline {
        /// True when framed as a rotation (`--rotate`) vs first enable.
        rotate: bool,
        keytab: PathBuf,
        wiring: AutorenewWiring,
    },
}

pub enum AutorenewWiring {
    Updated(PathBuf),
    NoControllerConfig,
    Failed(String),
}

/// Set up (or `--rotate`) the autorenew slot so the admin server approves
/// verified renewals in-process. Hot-swaps on the running daemon; otherwise
/// uses the supplied installation guard, TPM-gates, unlocks with the recovery
/// password, (re-)mints the slot + keytab, and points the config at it.
pub async fn auto_approve(
    ans: &mut dyn Answerer,
    access: &CaAccess,
    ca_dir: PathBuf,
    rotate: bool,
    insecure_no_tpm: bool,
) -> Result<AutoApproveOutcome> {
    match access {
        CaAccess::Running { config } => {
            let warning = local::rotate_autorenew(config).await?;
            Ok(AutoApproveOutcome::HotSwapped { warning })
        }
        CaAccess::Offline { config, lock, .. } => {
            let insecure_no_tpm = ca_setup::tpm_gate(ans, insecure_no_tpm).await?;
            let typed = ans.secret(Field::RecoveryPassword, None).await?;
            let recovery = ca_vault::normalize_recovery_password(typed.as_str());
            let mut cadir = CaDir::open(lock.clone(), &ca_dir).await.context(
                "setting up autorenew needs exclusive access; the admin server must be stopped",
            )?;
            let keytab = ca_setup::setup_autorenew_slot(
                ans,
                &mut cadir,
                &recovery,
                insecure_no_tpm,
            )
            .await?;
            let config_lock = cadir.config_lock();
            let wiring = match config {
                Some(path) => match server_setup::set_ca_autorenew_at(
                    &config_lock,
                    path,
                    &ca_dir,
                    &keytab,
                )
                .await
                {
                    Ok(path) => AutorenewWiring::Updated(path),
                    Err(e) => AutorenewWiring::Failed(format!("{e:#}")),
                },
                None => AutorenewWiring::NoControllerConfig,
            };
            Ok(AutoApproveOutcome::Offline { rotate, keytab, wiring })
        }
    }
}

/// A read-only report of the autorenew credential's state.
pub struct AutorenewStatus {
    pub slot_present: bool,
    pub keytab: PathBuf,
    pub keytab_present: bool,
    pub keytab_sealed: bool,
    /// The daemon config points `roles.ca.autorenew` at the keytab.
    pub wired_in_config: bool,
}

// -- ca recovery -------------------------------------------------------------

/// The outcome of [`recovery_rotate`] (the new password was shown via the
/// Answerer, never returned here).
pub enum RecoveryRotateOutcome {
    HotSwapped,
    Offline { ca_dir: PathBuf },
}

/// Mint a fresh recovery password on the CA box. Hot-swaps on the running
/// daemon; otherwise the box's autorenew keytab is the authority (a lost
/// recovery password is recoverable while the machine lives). The new password
/// is shown exactly once via [`ca_setup::show_recovery_password`].
pub async fn recovery_rotate(
    ans: &mut dyn Answerer,
    access: &CaAccess,
    ca_dir: PathBuf,
) -> Result<RecoveryRotateOutcome> {
    match access {
        CaAccess::Running { config } => {
            let new_pw = local::rotate_recovery(config).await?;
            ca_setup::show_recovery_password(ans, &new_pw).await?;
            Ok(RecoveryRotateOutcome::HotSwapped)
        }
        CaAccess::Offline { lock, .. } => {
            if !CAVault::exists_async(&ca_dir).await {
                bail!("no vault-protected CA at {}", ca_dir.display());
            }
            let keytab = offline_ca::autorenew_keytab_path()?;
            let autorenew_pw =
                read_autorenew_password_async(&keytab).await.with_context(|| {
                    format!(
                        "rotating the recovery password needs the autorenew keytab ({}); it \
                         authorizes the re-mint on the CA box. (Set one up with \
                         `netidx admin ca auto-approve`.)",
                        keytab.display()
                    )
                })?;
            let mut cadir = CaDir::open(lock.clone(), &ca_dir).await.context(
                "rotating recovery needs exclusive access; stop the admin server first",
            )?;
            cadir.vault.unlock_async(&autorenew_pw).await.with_context(|| {
                format!(
                    "the autorenew keytab ({}) did not unlock this CA — its credential is \
                     stale. Re-mint it with `netidx admin ca auto-approve --rotate` (needs \
                     the recovery password) and try again.",
                    keytab.display()
                )
            })?;
            let new_pw = ca_vault::gen_recovery_password();
            cadir
                .vault
                .replace_signing_slot(
                    &autorenew_pw,
                    netidx_admin_proto::policy::RECOVERY_ADMIN,
                    &new_pw,
                    recovery_policy(),
                )
                .await?;
            ca_setup::show_recovery_password(ans, &new_pw).await?;
            Ok(RecoveryRotateOutcome::Offline { ca_dir })
        }
    }
}

/// A read-only report of the recovery slot's state (never the password).
pub struct RecoveryStatus {
    pub slot_present: bool,
    /// Whether the box's autorenew keytab (the offline re-mint authority) exists.
    pub keytab_present: bool,
}

// -- controller disaster recovery -------------------------------------------

/// Result of rebinding a restored controller CA to replacement hardware.
#[derive(Debug)]
pub struct RecoverControllerOutcome {
    pub server_id: AdminServerId,
    pub listen: SocketAddr,
    pub revoked_serving_certificates: usize,
    pub serving_certificate: PathBuf,
    pub serving_key: PathBuf,
    pub autorenew_keytab: PathBuf,
    pub serving_key_sealed: bool,
    pub autorenew_keytab_sealed: bool,
}

/// Recover a served CA from a backup on replacement hardware. This operation
/// is deliberately offline and accepts only the off-box `recovery` slot: old
/// TPM-sealed serving/autorenew material is ignored and replaced.
pub async fn recover_controller(
    ans: &mut dyn Answerer,
    lock: &ConfigDirLock,
    ca_dir: PathBuf,
    config_path: PathBuf,
    listen: Option<SocketAddr>,
    resolver_listen: Option<SocketAddr>,
    insecure_no_tpm: bool,
) -> Result<RecoverControllerOutcome> {
    lock.require_descendant(&ca_dir)?;
    lock.require_contained(&config_path)?;
    let typed = ans.secret(Field::RecoveryPassword, None).await?;
    let recovery = ca_vault::normalize_recovery_password(typed.as_str());
    let insecure_no_tpm = ca_setup::tpm_gate(ans, insecure_no_tpm).await?;
    let autorenew_keytab = offline_ca::autorenew_keytab_path()?;
    recover_controller_with_password_and_lock(
        &ca_dir,
        &config_path,
        listen,
        resolver_listen,
        &recovery,
        insecure_no_tpm,
        &autorenew_keytab,
        lock.clone(),
    )
    .await
}

struct ProtectedBytes {
    bytes: Zeroizing<Vec<u8>>,
    sidecar: Option<Vec<u8>>,
    sealed: bool,
}

fn protect_serving_key(plain_pem: &str, insecure_no_tpm: bool) -> Result<ProtectedBytes> {
    match tls::seal_private_key(plain_pem) {
        Ok((encrypted, blob)) => Ok(ProtectedBytes {
            bytes: Zeroizing::new(encrypted.into_bytes()),
            sidecar: Some(blob),
            sealed: true,
        }),
        Err(_e) if insecure_no_tpm => Ok(ProtectedBytes {
            bytes: Zeroizing::new(plain_pem.as_bytes().to_vec()),
            sidecar: None,
            sealed: false,
        }),
        Err(e) => bail!(
            "could not seal the recovered controller serving key to this host's {}: \
             {e:#}",
            netidx_tpm::MECHANISM
        ),
    }
}

fn protect_autorenew_keytab(
    password: &str,
    insecure_no_tpm: bool,
) -> Result<ProtectedBytes> {
    match netidx_tpm::seal(password.as_bytes()) {
        Ok(blob) => Ok(ProtectedBytes {
            bytes: Zeroizing::new(blob),
            sidecar: None,
            sealed: true,
        }),
        Err(_e) if insecure_no_tpm => Ok(ProtectedBytes {
            bytes: Zeroizing::new(password.as_bytes().to_vec()),
            sidecar: None,
            sealed: false,
        }),
        Err(e) => bail!(
            "could not seal the recovered controller autorenew credential to this \
             host's {}: {e:#}",
            netidx_tpm::MECHANISM
        ),
    }
}

async fn write_protected_key(
    config_lock: &ConfigDirLock,
    path: &Path,
    protected: &ProtectedBytes,
) -> Result<()> {
    let path = config_lock.require_contained(path)?;
    atomic::write_atomic_async(&path, &protected.bytes, 0o600).await?;
    let sidecar = tls::sealed_sidecar(&path);
    match &protected.sidecar {
        Some(blob) => atomic::write_atomic_async(&sidecar, blob, 0o600).await,
        None => {
            if tokio::fs::try_exists(&sidecar).await? {
                tokio::fs::remove_file(&sidecar)
                    .await
                    .with_context(|| format!("removing stale sidecar {sidecar:?}"))?;
            }
            Ok(())
        }
    }
}

/// Recovery core split from the Answerer seam for deterministic
/// backup/restore testing. The caller has already normalized the password and
/// made the explicit TPM fallback decision.
#[cfg(test)]
async fn recover_controller_with_password(
    ca_dir: &Path,
    config_path: &Path,
    listen: Option<SocketAddr>,
    resolver_listen: Option<SocketAddr>,
    recovery_password: &str,
    insecure_no_tpm: bool,
    autorenew_keytab: &Path,
) -> Result<RecoverControllerOutcome> {
    let lock = ConfigDirLock::acquire_for_file(config_path)?;
    recover_controller_with_password_and_lock(
        ca_dir,
        config_path,
        listen,
        resolver_listen,
        recovery_password,
        insecure_no_tpm,
        autorenew_keytab,
        lock,
    )
    .await
}

async fn recover_controller_with_password_and_lock(
    ca_dir: &Path,
    config_path: &Path,
    listen: Option<SocketAddr>,
    resolver_listen: Option<SocketAddr>,
    recovery_password: &str,
    insecure_no_tpm: bool,
    autorenew_keytab: &Path,
    lock: ConfigDirLock,
) -> Result<RecoverControllerOutcome> {
    lock.require_descendant(ca_dir)?;
    lock.require_contained(config_path)?;
    if !CAVault::exists_async(ca_dir).await {
        bail!("no vault-protected CA at {}", ca_dir.display());
    }
    let mut cfg = admin_server_config::load_for_recovery_async(config_path).await?;
    if cfg.roles.ca.is_none() {
        bail!("the restored admin-server config does not carry the CA role");
    }

    let ca_cert =
        tokio::fs::read(ca_dir.join("certificate.pem")).await.with_context(|| {
            format!("reading restored CA certificate in {}", ca_dir.display())
        })?;
    let restored_fingerprint = Fingerprint::of_cert_pem(&ca_cert)?;
    let configured_fingerprint = Fingerprint::parse_text(&cfg.home_ca_fingerprint)
        .context("the restored config has an invalid home CA fingerprint")?;
    if configured_fingerprint != restored_fingerprint {
        bail!(
            "the restored config belongs to a different CA (configured {}, backup {})",
            configured_fingerprint.text(),
            restored_fingerprint.text()
        );
    }

    let mut cadir = CaDir::open(lock, ca_dir).await.context(
        "controller recovery requires exclusive CA access; stop the admin server",
    )?;
    let config_lock = cadir.config_lock();
    let autorenew_keytab = config_lock.require_contained(autorenew_keytab)?;
    let unlocked = cadir
        .vault
        .unlock_async(recovery_password)
        .await
        .context("the recovery password did not unlock the restored CA")?;
    if unlocked.admin != netidx_admin_proto::policy::RECOVERY_ADMIN {
        bail!(
            "controller recovery requires the off-box {:?} credential, not slot {:?}",
            netidx_admin_proto::policy::RECOVERY_ADMIN,
            unlocked.admin
        );
    }

    let mut map = admin_domain::load_async(ca_dir, cfg.server_id).await?;
    if map.controller != cfg.server_id {
        bail!(
            "restored controller mismatch: config {}, map {}",
            cfg.server_id,
            map.controller
        );
    }
    let mut controller = map
        .controller_entry()
        .cloned()
        .context("the restored authoritative map has no controller entry")?;
    if !controller.roles.contains(netidx_admin_proto::Role::Ca) {
        bail!("the restored map's controller entry does not carry the CA role");
    }
    let listen = listen.unwrap_or(cfg.listen);
    if listen.ip().is_unspecified() {
        bail!(
            "controller recovery needs a routable admin address, not {listen}; pass \
             --listen <address:port>"
        );
    }
    controller.addr = listen;
    admin_domain::upsert_controller(&mut map, controller, None)?;
    if let Some(resolver_listen) = resolver_listen {
        admin_domain::relocate_resolver(&mut map, cfg.server_id, resolver_listen)?;
    }

    // Prepare both replacement secrets before touching the vault or issuance
    // index. A sealing failure therefore leaves the backup byte-for-byte usable
    // for another attempt.
    let (serving, protected_serving, new_autorenew, protected_autorenew) =
        tokio::task::spawn_blocking(move || {
            let serving = transport::generate_key_and_csr(SERVING_SAN)?;
            let protected_serving =
                protect_serving_key(&serving.private_key_pem, insecure_no_tpm)?;
            let new_autorenew = ca_vault::random_signing_password();
            let protected_autorenew =
                protect_autorenew_keytab(&new_autorenew, insecure_no_tpm)?;
            Ok::<_, anyhow::Error>((
                serving,
                protected_serving,
                new_autorenew,
                protected_autorenew,
            ))
        })
        .await
        .context("controller credential generation task panicked")??;

    let signer = offline_ca::load_ca_from_unlocked(ca_dir, &unlocked).await?;
    let validity = cadir.lifetimes.leaf_validity;
    let serial = cadir.store.next_serial().await?;
    let sans = [
        SanEntry::Dns(SERVING_SAN.to_string()),
        SanEntry::Uri(cfg.server_id.uri()),
        SanEntry::Uri(CONTROLLER_ROLE_URI.to_string()),
    ];
    let csr = serving.csr_pem.as_bytes().to_vec();
    let leaf = tokio::task::spawn_blocking(move || {
        signer.sign_request(&csr, &sans, validity, serial)
    })
    .await
    .context("controller certificate signing task panicked")?
    .context("signing the recovered controller serving certificate")?;
    let leaf_text =
        std::str::from_utf8(&leaf).context("serving certificate is not utf8")?;
    let identity = tls::admin_cert_identity_from_pem(leaf_text.as_bytes())?;
    if identity.server_id != cfg.server_id || !identity.controller {
        bail!("the recovered serving certificate did not preserve controller identity");
    }

    // Atomically replace the machine credential in the vault using the
    // recovery slot as authority. The recovery slot itself is unchanged.
    cadir
        .vault
        .replace_signing_slot(
            recovery_password,
            crate::admin_server::AUTORENEW_ADMIN,
            &new_autorenew,
            netidx_admin_proto::policy::autorenew_policy(),
        )
        .await?;

    let now = crate::ca_store::now_unix();
    let store = &mut cadir.store;
    let old_serials: Vec<_> = store
        .list_signed()
        .await?
        .into_iter()
        .filter(|record| {
            record.live(now)
                && record.name.eq_ignore_ascii_case(SERVING_SAN)
                && tls::admin_cert_identity_from_pem(record.cert_pem.as_bytes())
                    .is_ok_and(|old| old.server_id == cfg.server_id)
        })
        .map(|record| record.serial)
        .collect();
    offline_ca::record_offline_issuance(
        store,
        serial,
        NodeKind::AdminServer,
        SERVING_SAN,
        &serving.csr_pem,
        leaf_text,
        validity,
        &[],
    )
    .await?;
    let mut revoked = 0;
    for old_serial in old_serials {
        let revocation = crate::ca_store::Revocation {
            serial: old_serial,
            revoked_unix: now,
            reason: format!(
                "controller {} rebound to replacement hardware during disaster recovery",
                cfg.server_id
            ),
        };
        if store.revoke(old_serial, revocation).await? {
            revoked += 1;
        }
    }
    store.write_crl(&unlocked.ca_key_pem).await?;

    let server_dir = ca_dir.join("server");
    tokio::fs::create_dir_all(&server_dir)
        .await
        .with_context(|| format!("creating {}", server_dir.display()))?;
    let serving_certificate = server_dir.join("cert.pem");
    let serving_key = server_dir.join("key.pem");
    let mut chain = leaf;
    chain.extend_from_slice(&ca_cert);
    atomic::write_atomic_async(&serving_certificate, &chain, 0o644).await?;
    write_protected_key(&config_lock, &serving_key, &protected_serving).await?;

    atomic::write_atomic_async(&autorenew_keytab, &protected_autorenew.bytes, 0o600)
        .await?;

    admin_domain::save_async(&config_lock, ca_dir, &map).await?;

    cfg.listen = listen;
    cfg.home_ca_fingerprint = restored_fingerprint.text();
    cfg.serving_cert = serving_certificate.clone();
    cfg.serving_key = serving_key.clone();
    cfg.trusted = {
        let trusted = ca_dir.join("trusted.pem");
        if tokio::fs::try_exists(&trusted).await? {
            trusted
        } else {
            ca_dir.join("certificate.pem")
        }
    };
    cfg.ca_addr = None;
    let role = cfg.roles.ca.as_mut().expect("CA role checked above");
    role.dir = ca_dir.to_path_buf();
    role.autorenew = Some(autorenew_keytab.clone());
    admin_server_config::save_async(&config_lock, config_path, &cfg).await?;
    admin_server_config::load_async(config_path)
        .await
        .context("the recovered admin-server configuration failed validation")?;

    Ok(RecoverControllerOutcome {
        server_id: cfg.server_id,
        listen,
        revoked_serving_certificates: revoked,
        serving_certificate,
        serving_key,
        autorenew_keytab: autorenew_keytab.to_path_buf(),
        serving_key_sealed: protected_serving.sealed,
        autorenew_keytab_sealed: protected_autorenew.sealed,
    })
}

// -- ca external (intermediate CA signed by an external PKI) ------------------

/// Written by `ca init --external-sign`, read by the `ca external` ops. Holds
/// what installing a signed cert needs but cannot re-derive before the cert
/// exists: the CA's subject/SANs (to re-emit a CSR) and the served-CA tail
/// inputs. Public so the (tools) init flow can write it.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExternalPending {
    pub cn: String,
    pub domain: String,
    #[serde(default)]
    pub country: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub locality: Option<String>,
    #[serde(default)]
    pub organization: Option<String>,
    #[serde(default)]
    pub san: Vec<String>,
    pub setup_server: bool,
    #[serde(default)]
    pub listen: Option<SocketAddr>,
    #[serde(default)]
    pub units_dir: Option<PathBuf>,
}

impl ExternalPending {
    const FILE: &'static str = "external_pending.json";

    pub fn store(&self, config_lock: &ConfigDirLock, dir: &Path) -> Result<()> {
        let dir = config_lock.require_contained(dir)?;
        let bytes = serde_json::to_vec_pretty(self)
            .context("encoding the external-sign marker")?;
        atomic::write_atomic(&dir.join(Self::FILE), &bytes, 0o644)
    }

    pub async fn store_async(
        &self,
        config_lock: &ConfigDirLock,
        dir: &Path,
    ) -> Result<()> {
        let dir = config_lock.require_contained(dir)?;
        let bytes = serde_json::to_vec_pretty(self)
            .context("encoding the external-sign marker")?;
        atomic::write_atomic_async(&dir.join(Self::FILE), &bytes, 0o644).await
    }

    pub fn load(dir: &Path) -> Result<Self> {
        let bytes = std::fs::read(dir.join(Self::FILE)).context(
            "reading the external-sign marker — was this CA created with \
             `ca init --external-sign`?",
        )?;
        serde_json::from_slice(&bytes).context("parsing the external-sign marker")
    }

    pub async fn load_async(dir: &Path) -> Result<Self> {
        let bytes = tokio::fs::read(dir.join(Self::FILE)).await.context(
            "reading the external-sign marker — was this CA created with \
             `ca init --external-sign`?",
        )?;
        serde_json::from_slice(&bytes).context("parsing the external-sign marker")
    }
}

/// Unlock the CA key while holding the installation guard, then hand back both — the fixed
/// unlock (via [`offline_ca::unlock_held`]) folds a typed recovery password to
/// canonical form, so the grouped displayed form now works (it did not before).
async fn external_ca_key(
    ans: &mut dyn Answerer,
    lock: ConfigDirLock,
    dir: &Path,
) -> Result<(Zeroizing<Vec<u8>>, CaDir)> {
    let cadir = CaDir::open(lock, dir)
        .await
        .context("opening the CA (stop the admin server first if it is running)")?;
    let unlocked = offline_ca::unlock_held(ans, &cadir, dir).await?;
    Ok((unlocked.ca_key_pem, cadir))
}

/// (Re-)emit a CSR for the CA cert over the existing key — for renewing an
/// externally-signed CA cert (same key ⇒ glyph unchanged). Returns the CSR path.
pub async fn external_emit_csr(
    ans: &mut dyn Answerer,
    lock: &ConfigDirLock,
    ca_dir: PathBuf,
) -> Result<PathBuf> {
    let (key, _cadir) = external_ca_key(ans, lock.clone(), &ca_dir).await?;
    let (common_name, csr) = external_csr_with_key(&ca_dir, &key).await?;
    let csr_path = offline_ca::default_csr_filename(&common_name);
    atomic::write_atomic_async(&csr_path, &csr, 0o644)
        .await
        .with_context(|| format!("writing CSR to {}", csr_path.display()))?;
    Ok(csr_path)
}

/// Build an external-CA renewal CSR using a key the running controller has
/// already unlocked. This is the live/local-control counterpart of
/// [`external_emit_csr`]; it performs no locking or vault access itself.
pub async fn external_csr_with_key(
    ca_dir: &Path,
    key_pem: &[u8],
) -> Result<(String, Vec<u8>)> {
    let m = ExternalPending::load_async(ca_dir).await?;
    anyhow::ensure!(
        CaLifetimes::load_async(ca_dir).await?.externally_signed,
        "this controller CA is not externally signed"
    );
    let san = if m.san.is_empty() {
        vec![SanEntry::Dns(m.cn.clone())]
    } else {
        offline_ca::parse_sans(&m.san, &m.cn)?
    };
    let subject = Subject {
        common_name: m.cn.clone(),
        country: m.country,
        state: m.state,
        locality: m.locality,
        organization: m.organization,
    };
    let key_pem = key_pem.to_vec();
    let csr = tokio::task::spawn_blocking(move || {
        ca::ca_csr_from_key(&key_pem, &subject, &san)
    })
    .await
    .context("external CA CSR task panicked")??;
    Ok((m.cn, csr))
}

/// The outcome of [`external_install_cert`], for the CLI to narrate + offer.
pub enum ExternalInstallOutcome {
    /// Offline external CA — nothing further to set up.
    OfflineCa,
    /// First install of a served external CA: the served-CA tail ran; the
    /// caller offers `need` and reports `cfg_path`.
    FirstInstall { need: ServiceNeed, cfg_path: PathBuf },
    /// A renewal of an already-served external CA.
    Renewal,
}

/// Install an externally-signed CA cert: validate it binds our key, is a CA
/// cert, and chains to the external root; write `certificate.pem` (the
/// intermediate alone) + `trusted.pem` (`[root, intermediate]`). On the first
/// install of a served CA also run the served-CA tail (serving cert + config)
/// that phase 1 could not do without the cert, returning its [`ServiceNeed`].
pub async fn external_install_cert(
    ans: &mut dyn Answerer,
    lock: &ConfigDirLock,
    ca_dir: PathBuf,
    signed: &Path,
    root: Option<&Path>,
) -> Result<ExternalInstallOutcome> {
    lock.require_contained(&ca_dir)?;
    let m = ExternalPending::load_async(&ca_dir).await?;
    let signed_pem = tokio::fs::read(signed)
        .await
        .with_context(|| format!("reading {}", signed.display()))?;
    let root_pem = match root {
        Some(p) => Some(
            tokio::fs::read(p)
                .await
                .with_context(|| format!("reading {}", p.display()))?,
        ),
        None => None,
    };
    let (key, mut cadir) = external_ca_key(ans, lock.clone(), &ca_dir).await?;
    let (intermediate_pem, external_root_pem) =
        ca::validate_external_ca_cert(&signed_pem, root_pem.as_deref(), &key)?;
    // certificate.pem is the intermediate ALONE (the admin domain glyph is its key);
    // trusted.pem is [external root, intermediate].
    atomic::write_atomic_async(&ca_dir.join("certificate.pem"), &intermediate_pem, 0o644)
        .await
        .context("installing certificate.pem")?;
    cadir.store.write_crl(&key).await.context("creating the initial empty CRL")?;
    let mut trusted = external_root_pem;
    trusted.extend_from_slice(&intermediate_pem);
    atomic::write_atomic_async(&ca_dir.join("trusted.pem"), &trusted, 0o644)
        .await
        .context("installing trusted.pem")?;
    ca_setup::show_ca_identity(ans, &ca_dir).await?;
    if !m.setup_server {
        return Ok(ExternalInstallOutcome::OfflineCa);
    }
    // Served CA. Decide "first install vs renewal" on whether the admin server
    // is configured yet — NOT on certificate.pem (which we just wrote), so a
    // failed/interrupted first-install tail is retriable, not reclassified.
    if paths::discover_admin_server_config_async().await.is_err() {
        let ca = tokio::task::spawn_blocking({
            let ca_dir = ca_dir.clone();
            let key = key.to_vec();
            let intermediate_pem = intermediate_pem.clone();
            move || Ca::from_pem(ca_dir, &key, &intermediate_pem)
        })
        .await
        .context("CA reconstruction task panicked")?
        .context("reconstructing the CA from the installed certificate")?;
        let config_lock = cadir.config_lock();
        // setup_server reopens CA state with the same installation guard.
        drop(cadir);
        let need = server_setup::setup_server(
            ans,
            server_setup::SetupArgs {
                ca_dir: &ca_dir,
                ca: &ca,
                config_lock: config_lock.clone(),
                domain: &m.domain,
                listen: m.listen,
                listen_hint: None,
                units_dir: m.units_dir.as_deref(),
            },
        )
        .await?;
        let cfg_path = server_setup::set_ca_autorenew(
            &config_lock,
            &ca_dir,
            &offline_ca::autorenew_keytab_path()?,
        )
        .await?;
        let cfg = crate::admin_server_config::load_async(&cfg_path).await?;
        crate::provenance::InstallRecord::new(
            crate::provenance::InstallRole::Controller,
            "/",
            "admin-tls",
            Some(crate::provenance::AdminDomainIdentity::new(
                m.domain.clone(),
                &netidx_admin_proto::fingerprint::Fingerprint::of_cert_pem(
                    &intermediate_pem,
                )?,
            )),
            Some(cfg.listen),
        )
        .save_default_async(&config_lock)
        .await
        .context("recording the controller install")?;
        return Ok(ExternalInstallOutcome::FirstInstall { need, cfg_path });
    }
    // Already configured: this is a renewal. Keep autorenew wired (idempotent).
    let keytab = offline_ca::autorenew_keytab_path()?;
    if tokio::fs::try_exists(&keytab).await.unwrap_or(false) {
        let config_lock = cadir.config_lock();
        let _ = server_setup::set_ca_autorenew(&config_lock, &ca_dir, &keytab).await;
    }
    Ok(ExternalInstallOutcome::Renewal)
}

/// A read-only report of the external-CA state.
pub struct ExternalStatus {
    pub externally_signed: bool,
    pub cert_installed: bool,
    /// `Some((cn, domain))` when a `ca init --external-sign` is awaiting its
    /// signed certificate.
    pub pending: Option<(String, String)>,
}

pub struct LocalCaStatus {
    pub auto_approve: AutorenewStatus,
    pub recovery: RecoveryStatus,
    pub external: ExternalStatus,
}

pub async fn local_ca_status(access: &CaAccess, ca_dir: &Path) -> Result<LocalCaStatus> {
    let keytab = offline_ca::autorenew_keytab_path()?;
    let keytab_present = tokio::fs::try_exists(&keytab).await.unwrap_or(false);
    let keytab_sealed = keytab_present
        && tokio::fs::read(&keytab)
            .await
            .ok()
            .map(|bytes| netidx_tpm::is_sealed(&bytes))
            .unwrap_or(false);
    let (
        autorenew_slot_present,
        recovery_slot_present,
        externally_signed,
        cert_installed,
        pending,
        config,
    ) = match access {
        CaAccess::Running { config } => {
            let status = local::ca_status(config).await?;
            (
                status.autorenew_slot_present,
                status.recovery_slot_present,
                status.externally_signed,
                status.cert_installed,
                status.pending,
                Some(config.as_path()),
            )
        }
        CaAccess::Offline { config, lock, .. } => {
            lock.require_contained(ca_dir)?;
            let (autorenew, recovery) = if CAVault::exists_async(ca_dir).await {
                let ca = CaDir::open(lock.clone(), ca_dir).await?;
                let names = ca.vault.signing_slot_names()?;
                (
                    names.iter().any(|name| name == crate::admin_server::AUTORENEW_ADMIN),
                    names
                        .iter()
                        .any(|name| name == netidx_admin_proto::policy::RECOVERY_ADMIN),
                )
            } else {
                (false, false)
            };
            let externally_signed = CaLifetimes::load_async(ca_dir)
                .await
                .map(|lifetimes| lifetimes.externally_signed)
                .unwrap_or(false);
            let cert_installed = tokio::fs::try_exists(ca_dir.join("certificate.pem"))
                .await
                .unwrap_or(false);
            let pending = if cert_installed {
                None
            } else {
                ExternalPending::load_async(ca_dir)
                    .await
                    .ok()
                    .map(|pending| (pending.cn, pending.domain))
            };
            (
                autorenew,
                recovery,
                externally_signed,
                cert_installed,
                pending,
                config.as_deref(),
            )
        }
    };
    let wired_in_config = match config {
        Some(path) => admin_server_config::load_async(path)
            .await
            .ok()
            .and_then(|config| config.roles.ca.map(|ca| ca.autorenew.is_some()))
            .unwrap_or(false),
        None => false,
    };
    Ok(LocalCaStatus {
        auto_approve: AutorenewStatus {
            slot_present: autorenew_slot_present,
            keytab: keytab.clone(),
            keytab_present,
            keytab_sealed,
            wired_in_config,
        },
        recovery: RecoveryStatus { slot_present: recovery_slot_present, keytab_present },
        external: ExternalStatus { externally_signed, cert_installed, pending },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        admin_server::read_autorenew_password,
        ca::{CaParams, Subject},
    };

    struct ControllerFixture {
        _root: tempfile::TempDir,
        ca_dir: PathBuf,
        config: PathBuf,
        keytab: PathBuf,
        recovery: Zeroizing<String>,
        server_id: AdminServerId,
        old_serial: u64,
        old_autorenew: String,
    }

    async fn recoverable_controller() -> ControllerFixture {
        use crate::admin_server_config::{CaRole, Roles};
        use netidx_admin_proto::{
            AdminDomainMap, AdminServerEntry, ResolverClusterEntry, ResolverClusterId,
            ResolverClusterState, Role, ServerState,
        };

        let root = tempfile::tempdir().unwrap();
        let ca_dir = root.path().join("ca");
        let params = CaParams {
            directory: ca_dir.clone(),
            subject: Subject::cn("backup-ca"),
            san: vec![SanEntry::Dns("backup-ca".to_string())],
            key_bits: 2048,
            validity: std::time::Duration::from_secs(30 * 86400),
        };
        let ca = Ca::init(&params, None).unwrap();
        let key = std::fs::read(ca_dir.join("private.key")).unwrap();
        let recovery = ca_vault::gen_recovery_password();
        let old_autorenew = "old-machine-sealed-autorenew".to_string();
        let mut vault = CAVault::new(ca_dir.clone());
        vault
            .create(
                &key,
                netidx_admin_proto::policy::RECOVERY_ADMIN,
                &recovery,
                recovery_policy(),
            )
            .await
            .unwrap();
        std::fs::remove_file(ca_dir.join("private.key")).unwrap();
        vault
            .add_signing_slot(
                &recovery,
                crate::admin_server::AUTORENEW_ADMIN,
                &old_autorenew,
                netidx_admin_proto::policy::autorenew_policy(),
            )
            .await
            .unwrap();

        let server_id = AdminServerId::new();
        let old = transport::generate_key_and_csr(SERVING_SAN).unwrap();
        let config_lock =
            crate::config_lock::ConfigDirLock::acquire(root.path()).unwrap();
        let old_leaf = offline_ca::sign_and_record(
            &config_lock,
            &ca,
            NodeKind::AdminServer,
            old.csr_pem.as_bytes(),
            &[
                SanEntry::Dns(SERVING_SAN.to_string()),
                SanEntry::Uri(server_id.uri()),
                SanEntry::Uri(CONTROLLER_ROLE_URI.to_string()),
            ],
            SERVING_SAN,
            std::time::Duration::from_secs(7 * 86400),
        )
        .await
        .unwrap();
        let server_dir = ca_dir.join("server");
        std::fs::create_dir_all(&server_dir).unwrap();
        let serving_cert = server_dir.join("cert.pem");
        let serving_key = server_dir.join("key.pem");
        let mut chain = old_leaf;
        chain.extend_from_slice(&std::fs::read(ca_dir.join("certificate.pem")).unwrap());
        std::fs::write(&serving_cert, chain).unwrap();
        // The backup contains the old machine's encrypted/sidecar-shaped files;
        // recovery must never try to reuse them.
        std::fs::write(&serving_key, b"old machine key").unwrap();
        std::fs::write(tls::sealed_sidecar(&serving_key), b"old machine seal").unwrap();

        let cluster = ResolverClusterId::new();
        let listen = "10.0.0.10:4565".parse().unwrap();
        let mut map = AdminDomainMap::empty(server_id);
        map.admin_servers.push(AdminServerEntry {
            id: server_id,
            addr: listen,
            roles: Role::Ca | Role::Resolver,
            resolver: None,
            cluster: Some(cluster),
            state: ServerState::Registered,
        });
        map.resolver_clusters.push(ResolverClusterEntry {
            id: cluster,
            base: "/".to_string(),
            state: ResolverClusterState::Active,
            members: vec![],
            parent: None,
            children: vec![],
        });
        admin_domain::save(&config_lock, &ca_dir, &map).unwrap();

        let config = root.path().join("admin-server.json");
        let keytab = root.path().join("replacement-autorenew.keytab");
        let ca_cert = std::fs::read(ca_dir.join("certificate.pem")).unwrap();
        let restored_config = AdminServerConfig {
            domain: "example.com".to_string(),
            server_id,
            home_ca_fingerprint: Fingerprint::of_cert_pem(&ca_cert).unwrap().text(),
            listen,
            // A backup restored at a different mount point retains dead-machine
            // paths. `load_for_recovery` must not need these files in order to
            // authenticate the backup and rewrite them canonically.
            serving_cert: PathBuf::from("/dead-machine/ca/server/cert.pem"),
            serving_key: PathBuf::from("/dead-machine/ca/server/key.pem"),
            trusted: PathBuf::from("/dead-machine/ca/trusted.pem"),
            roles: Roles {
                ca: Some(CaRole {
                    dir: PathBuf::from("/dead-machine/ca"),
                    autorenew: Some(PathBuf::from(
                        "/dead-machine/config/autorenew.keytab",
                    )),
                    session_absolute_lifetime: None,
                    session_idle_timeout: None,
                }),
                resolver: None,
                id_map: None,
            },
            ca_addr: None,
            peers: vec![],
            mdns: true,
            activation_units_dir: None,
        };
        atomic::write_atomic(
            &config,
            &serde_json::to_vec_pretty(&restored_config).unwrap(),
            0o600,
        )
        .unwrap();
        drop(config_lock);
        let old_serial = {
            let lock = crate::config_lock::ConfigDirLock::acquire(root.path()).unwrap();
            let cadir = CaDir::open(lock, &ca_dir).await.unwrap();
            cadir
                .store
                .list_signed()
                .await
                .unwrap()
                .into_iter()
                .find(|record| record.name == SERVING_SAN)
                .unwrap()
                .serial
        };
        ControllerFixture {
            _root: root,
            ca_dir,
            config,
            keytab,
            recovery,
            server_id,
            old_serial,
            old_autorenew,
        }
    }

    /// A vaulted CA in `dir` with recovery + autorenew slots (as the install
    /// path leaves it), for the read-only status queries.
    async fn vaulted_ca(dir: &Path) {
        let params = CaParams {
            directory: dir.to_path_buf(),
            subject: Subject::cn("test-ca"),
            san: vec![SanEntry::Dns("test-ca".to_string())],
            key_bits: 2048,
            validity: std::time::Duration::from_secs(30 * 86400),
        };
        Ca::init(&params, None).unwrap();
        let key = std::fs::read(dir.join("private.key")).unwrap();
        let mut vault = CAVault::new(dir.to_path_buf());
        let recovery_pw = ca_vault::gen_recovery_password();
        vault
            .create(
                &key,
                netidx_admin_proto::policy::RECOVERY_ADMIN,
                &recovery_pw,
                recovery_policy(),
            )
            .await
            .unwrap();
        std::fs::remove_file(dir.join("private.key")).unwrap();
        vault
            .add_signing_slot(
                &recovery_pw,
                crate::admin_server::AUTORENEW_ADMIN,
                "renew-secret-01234",
                netidx_admin_proto::policy::autorenew_policy(),
            )
            .await
            .unwrap();
    }

    async fn offline_status(ca_dir: &Path) -> LocalCaStatus {
        let lock = ConfigDirLock::acquire_for_ca_dir(ca_dir).await.unwrap();
        local_ca_status(
            &CaAccess::Offline { config: None, lock, ca_alias_lock: None },
            ca_dir,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn recovery_and_autorenew_slots_are_reported_present() {
        let dir = tempfile::tempdir().unwrap();
        vaulted_ca(dir.path()).await;
        let status = offline_status(dir.path()).await;
        assert!(status.recovery.slot_present);
        // No cfg ⇒ wired_in_config is false, but the slot is read from the vault.
        let a = status.auto_approve;
        assert!(a.slot_present);
        assert!(!a.wired_in_config);
    }

    #[tokio::test]
    async fn restored_controller_rebinds_to_new_machine_without_changing_identity() {
        let fixture = recoverable_controller().await;
        let new_listen: SocketAddr = "10.0.0.20:14565".parse().unwrap();
        let old_resolver = netidx_admin_proto::ResolverAddr {
            addr: "10.0.0.10:4564".parse().unwrap(),
            auth: netidx_admin_proto::InfoAuth::Tls {
                name: "resolver.example.com".into(),
            },
        };
        let new_resolver: SocketAddr = "10.0.0.20:14564".parse().unwrap();
        let mut map =
            admin_domain::load(fixture.ca_dir.as_path(), fixture.server_id).unwrap();
        map.admin_servers[0].resolver = Some(old_resolver.clone());
        map.resolver_clusters[0].members = vec![old_resolver];
        let lock = ConfigDirLock::acquire_for_ca_dir(&fixture.ca_dir).await.unwrap();
        admin_domain::save(&lock, fixture.ca_dir.as_path(), &map).unwrap();
        drop(lock);
        let out = recover_controller_with_password(
            fixture.ca_dir.as_path(),
            &fixture.config,
            Some(new_listen),
            Some(new_resolver),
            &fixture.recovery,
            true,
            &fixture.keytab,
        )
        .await
        .unwrap();
        assert_eq!(out.server_id, fixture.server_id);
        assert_eq!(out.listen, new_listen);
        assert_eq!(out.revoked_serving_certificates, 1);

        let cfg = admin_server_config::load(&fixture.config).unwrap();
        assert_eq!(cfg.server_id, fixture.server_id);
        assert_eq!(cfg.listen, new_listen);
        assert_eq!(cfg.roles.ca.as_ref().unwrap().dir, fixture.ca_dir.as_path());
        assert_eq!(
            cfg.roles.ca.as_ref().unwrap().autorenew.as_deref(),
            Some(fixture.keytab.as_path())
        );
        let leaf = std::fs::read(&cfg.serving_cert).unwrap();
        let identity = tls::admin_cert_identity_from_pem(&leaf).unwrap();
        assert_eq!(identity.server_id, fixture.server_id);
        assert!(identity.controller);
        let map =
            admin_domain::load(fixture.ca_dir.as_path(), fixture.server_id).unwrap();
        assert_eq!(
            map.controller_entry().unwrap().resolver.as_ref().unwrap().addr,
            new_resolver
        );
        assert_eq!(map.resolver_clusters[0].members[0].addr, new_resolver);
        netidx::tls::load_private_key(None, &cfg.serving_key.to_string_lossy()).unwrap();

        let map =
            admin_domain::load(fixture.ca_dir.as_path(), fixture.server_id).unwrap();
        assert_eq!(map.controller_entry().unwrap().addr, new_listen);

        let lock = crate::config_lock::ConfigDirLock::acquire_for_ca_dir(
            fixture.ca_dir.as_path(),
        )
        .await
        .unwrap();
        let cadir = CaDir::open(lock, fixture.ca_dir.as_path()).await.unwrap();
        let records = cadir.store.list_signed().await.unwrap();
        let old = records.iter().find(|r| r.serial == fixture.old_serial).unwrap();
        assert!(old.revoked.is_some());
        let live: Vec<_> = records
            .iter()
            .filter(|r| {
                r.live(crate::ca_store::now_unix())
                    && r.name == SERVING_SAN
                    && tls::admin_cert_identity_from_pem(r.cert_pem.as_bytes())
                        .is_ok_and(|id| id.server_id == fixture.server_id)
            })
            .collect();
        assert_eq!(live.len(), 1);
        assert!(cadir.store.crl_path().is_file());
        assert!(cadir.vault.unlock(&fixture.recovery).is_ok());
        assert!(cadir.vault.unlock(&fixture.old_autorenew).is_err());
        let replacement = read_autorenew_password(&fixture.keytab).unwrap();
        let unlocked = cadir.vault.unlock(&replacement).unwrap();
        assert_eq!(unlocked.admin, crate::admin_server::AUTORENEW_ADMIN);
    }

    #[tokio::test]
    async fn live_backup_bundle_verifies_restores_and_recovers_without_machine_keys() {
        let fixture = recoverable_controller().await;
        let cfg = admin_server_config::load_for_recovery(&fixture.config).unwrap();
        let (map_version, highest_serial, ca_key) = {
            let map =
                admin_domain::load(fixture.ca_dir.as_path(), fixture.server_id).unwrap();
            let lock = crate::config_lock::ConfigDirLock::acquire_for_ca_dir(
                fixture.ca_dir.as_path(),
            )
            .await
            .unwrap();
            let cadir = CaDir::open(lock, fixture.ca_dir.as_path()).await.unwrap();
            let highest = cadir.store.max_serial().await.unwrap().unwrap();
            let key = cadir.vault.unlock(&fixture.recovery).unwrap().ca_key_pem;
            (map.version, highest, key)
        };
        let snapshot = crate::backup::capture(
            &cfg,
            &fixture.config,
            fixture.ca_dir.as_path(),
            map_version,
            highest_serial,
            &ca_key,
        )
        .unwrap();
        let backup_parent = tempfile::tempdir().unwrap();
        let bundle = backup_parent.path().join("controller-backup");
        let outcome =
            crate::backup::publish(snapshot, &bundle, fixture.ca_dir.as_path()).unwrap();
        assert_eq!(outcome.controller, fixture.server_id);
        let manifest = crate::backup::verify(&bundle).unwrap();
        assert_eq!(manifest.highest_serial, highest_serial);
        assert!(
            manifest.files.iter().all(|file| !file.path.starts_with("ca/server/")),
            "machine-bound serving material must not be backed up"
        );

        let restore = tempfile::tempdir().unwrap();
        let ca = restore.path().join("ca");
        let config = restore.path().join("admin-server.json");
        let restore_lock = ConfigDirLock::acquire(restore.path()).unwrap();
        crate::backup::restore(&restore_lock, &bundle, &ca, &config).unwrap();
        crate::backup::restore(&restore_lock, &bundle, &ca, &config)
            .expect("a pristine restore is retryable after a mistyped password");
        drop(restore_lock);
        let keytab = restore.path().join("autorenew.keytab");
        let new_listen: SocketAddr = "10.0.0.30:24565".parse().unwrap();
        let recovered = recover_controller_with_password(
            &ca,
            &config,
            Some(new_listen),
            None,
            &fixture.recovery,
            true,
            &keytab,
        )
        .await
        .unwrap();
        assert_eq!(recovered.server_id, fixture.server_id);
        assert_eq!(recovered.listen, new_listen);
        assert!(keytab.is_file());
        assert!(admin_server_config::load(&config).is_ok());
    }

    #[tokio::test]
    async fn backup_manifest_detects_tampering_and_restore_never_overwrites() {
        let fixture = recoverable_controller().await;
        let cfg = admin_server_config::load_for_recovery(&fixture.config).unwrap();
        let (map_version, highest_serial, ca_key) = {
            let map =
                admin_domain::load(fixture.ca_dir.as_path(), fixture.server_id).unwrap();
            let lock = crate::config_lock::ConfigDirLock::acquire_for_ca_dir(
                fixture.ca_dir.as_path(),
            )
            .await
            .unwrap();
            let cadir = CaDir::open(lock, fixture.ca_dir.as_path()).await.unwrap();
            let highest = cadir.store.max_serial().await.unwrap().unwrap();
            let key = cadir.vault.unlock(&fixture.recovery).unwrap().ca_key_pem;
            (map.version, highest, key)
        };
        let snapshot = crate::backup::capture(
            &cfg,
            &fixture.config,
            fixture.ca_dir.as_path(),
            map_version,
            highest_serial,
            &ca_key,
        )
        .unwrap();
        let parent = tempfile::tempdir().unwrap();
        let bundle = parent.path().join("backup");
        crate::backup::publish(snapshot, &bundle, fixture.ca_dir.as_path()).unwrap();
        let original_map = std::fs::read(bundle.join("ca/admin-domain.json")).unwrap();
        std::fs::write(bundle.join("ca/admin-domain.json"), b"tampered").unwrap();
        assert!(crate::backup::verify(&bundle).is_err());
        std::fs::write(bundle.join("ca/admin-domain.json"), original_map).unwrap();
        let manifest_path = bundle.join(crate::backup::MANIFEST_FILE);
        let mut manifest: crate::backup::Manifest =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        manifest.map_version += 1;
        std::fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest).unwrap())
            .unwrap();
        let error = crate::backup::verify(&bundle).unwrap_err();
        assert!(format!("{error:#}").contains("signature"));

        let destination = parent.path().join("existing-ca");
        std::fs::create_dir(&destination).unwrap();
        let config = parent.path().join("new-config.json");
        let restore_lock = ConfigDirLock::acquire(parent.path()).unwrap();
        assert!(
            crate::backup::restore(&restore_lock, &bundle, &destination, &config)
                .is_err()
        );
        assert!(!config.exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn running_controller_backs_up_over_the_protected_local_rpc() {
        let fixture = recoverable_controller().await;
        let listen: SocketAddr = "127.0.0.1:0".parse().unwrap();
        recover_controller_with_password(
            fixture.ca_dir.as_path(),
            &fixture.config,
            Some(listen),
            None,
            &fixture.recovery,
            true,
            &fixture.keytab,
        )
        .await
        .unwrap();
        let mut cfg = admin_server_config::load(&fixture.config).unwrap();
        cfg.mdns = false;
        let lock = ConfigDirLock::acquire_for_file(&fixture.config).unwrap();
        admin_server_config::save_async(&lock, &fixture.config, &cfg).await.unwrap();
        drop(lock);
        let config = fixture.config.clone();
        let daemon = tokio::spawn(crate::admin_server::serve(config.clone()));
        for _ in 0..500 {
            if crate::local::daemon_running(&config).await {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        if !crate::local::daemon_running(&config).await {
            if daemon.is_finished() {
                panic!(
                    "recovered controller exited before local RPC: {:?}",
                    daemon.await
                );
            }
            panic!("recovered controller never opened its local RPC socket");
        }
        let status = crate::local::ca_status(&config).await.unwrap();
        assert!(status.autorenew_slot_present);
        assert!(status.recovery_slot_present);
        assert!(status.cert_installed);
        assert!(!status.externally_signed);
        let second = crate::admin_server::serve(config.clone()).await.unwrap_err();
        assert!(format!("{second:#}").contains("another netidx administrative process"));
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("live-backup");
        let outcome = crate::local::backup(&config, &target).await.unwrap();
        assert_eq!(outcome.controller, fixture.server_id);
        assert_eq!(outcome.target, target);
        assert!(crate::backup::verify(&target).is_ok());
        daemon.abort();
    }

    fn test_ca_cert(
        subject_cn: &str,
        issuer: Option<&openssl::x509::X509>,
        public_key: &openssl::pkey::PKey<openssl::pkey::Private>,
        signer: &openssl::pkey::PKey<openssl::pkey::Private>,
        serial: u32,
    ) -> openssl::x509::X509 {
        use openssl::{
            asn1::Asn1Time,
            bn::BigNum,
            hash::MessageDigest,
            x509::{
                X509Builder, X509NameBuilder,
                extension::{BasicConstraints, KeyUsage},
            },
        };
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", subject_cn).unwrap();
        let name = name.build();
        let mut b = X509Builder::new().unwrap();
        b.set_version(2).unwrap();
        b.set_serial_number(
            &BigNum::from_u32(serial).unwrap().to_asn1_integer().unwrap(),
        )
        .unwrap();
        b.set_subject_name(&name).unwrap();
        b.set_issuer_name(issuer.map(|c| c.subject_name()).unwrap_or(&name)).unwrap();
        b.set_pubkey(public_key).unwrap();
        b.set_not_before(&Asn1Time::days_from_now(0).unwrap()).unwrap();
        b.set_not_after(&Asn1Time::days_from_now(365).unwrap()).unwrap();
        b.append_extension(BasicConstraints::new().critical().ca().build().unwrap())
            .unwrap();
        b.append_extension(
            KeyUsage::new().critical().key_cert_sign().crl_sign().build().unwrap(),
        )
        .unwrap();
        b.sign(signer, MessageDigest::sha256()).unwrap();
        b.build()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn running_external_controller_renews_through_local_rpc_only() {
        use openssl::{pkey::PKey, rsa::Rsa};
        let fixture = recoverable_controller().await;
        recover_controller_with_password(
            fixture.ca_dir.as_path(),
            &fixture.config,
            Some("127.0.0.1:0".parse().unwrap()),
            None,
            &fixture.recovery,
            true,
            &fixture.keytab,
        )
        .await
        .unwrap();
        let mut cfg = admin_server_config::load(&fixture.config).unwrap();
        cfg.mdns = false;
        let lock = ConfigDirLock::acquire_for_file(&fixture.config).unwrap();
        admin_server_config::save(&lock, &fixture.config, &cfg).unwrap();
        let unlocked = CAVault::open(fixture.ca_dir.clone())
            .await
            .unwrap()
            .unlock(&fixture.recovery)
            .unwrap();
        let ca_key = PKey::private_key_from_pem(&unlocked.ca_key_pem).unwrap();
        let root_key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let root = test_ca_cert("hardware-root", None, &root_key, &root_key, 100);
        let intermediate =
            test_ca_cert("netidx-controller", Some(&root), &ca_key, &root_key, 101);
        let root_pem = root.to_pem().unwrap();
        let intermediate_pem = intermediate.to_pem().unwrap();
        atomic::write_atomic(
            &fixture.ca_dir.join("certificate.pem"),
            &intermediate_pem,
            0o644,
        )
        .unwrap();
        let mut trusted = root_pem.clone();
        trusted.extend_from_slice(&intermediate_pem);
        cfg.trusted = fixture.ca_dir.join("trusted.pem");
        atomic::write_atomic(&cfg.trusted, &trusted, 0o644).unwrap();
        admin_server_config::save(&lock, &fixture.config, &cfg).unwrap();
        drop(lock);
        let serving = std::fs::read_to_string(&cfg.serving_cert).unwrap();
        let marker = "-----END CERTIFICATE-----";
        let end = serving.find(marker).unwrap() + marker.len();
        let chain = format!(
            "{}\n{}",
            &serving[..end],
            String::from_utf8_lossy(&intermediate_pem)
        );
        atomic::write_atomic(&cfg.serving_cert, chain.as_bytes(), 0o644).unwrap();
        let lock = ConfigDirLock::acquire_for_file(&fixture.config).unwrap();
        CaLifetimes { externally_signed: true, ..CaLifetimes::default() }
            .store_async(&lock, fixture.ca_dir.as_path())
            .await
            .unwrap();
        ExternalPending {
            cn: "netidx-controller".into(),
            domain: "example.com".into(),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec!["dns:netidx-controller".into()],
            setup_server: true,
            listen: Some(cfg.listen),
            units_dir: None,
        }
        .store(&lock, fixture.ca_dir.as_path())
        .unwrap();
        drop(lock);
        let config = fixture.config.clone();
        let daemon = tokio::spawn(crate::admin_server::serve(config.clone()));
        for _ in 0..500 {
            if crate::local::daemon_running(&config).await {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(crate::local::daemon_running(&config).await);
        let (_, csr) = crate::local::external_ca_csr(&config).await.unwrap();
        let csr = openssl::x509::X509Req::from_pem(csr.as_bytes()).unwrap();
        assert_eq!(
            csr.public_key().unwrap().public_key_to_der().unwrap(),
            ca_key.public_key_to_der().unwrap()
        );
        let renewed =
            test_ca_cert("netidx-controller", Some(&root), &ca_key, &root_key, 102);
        let before_fp = Fingerprint::of_cert_pem(&intermediate_pem).unwrap();
        let returned = crate::local::external_ca_install(
            &config,
            String::from_utf8(renewed.to_pem().unwrap()).unwrap(),
            Some(String::from_utf8(root_pem.clone()).unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(returned, before_fp.text());
        let live_ca_dir = cfg.roles.ca.as_ref().unwrap().dir.clone();
        assert_eq!(
            Fingerprint::of_cert_pem(
                &std::fs::read(live_ca_dir.join("certificate.pem")).unwrap(),
            )
            .unwrap(),
            before_fp
        );
        // A different hardware root cannot replace the pinned external issuer,
        // even when it signs the existing netidx intermediate key.
        let rogue_key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let rogue_root = test_ca_cert("rogue-root", None, &rogue_key, &rogue_key, 200);
        let rogue = test_ca_cert(
            "netidx-controller",
            Some(&rogue_root),
            &ca_key,
            &rogue_key,
            201,
        );
        assert!(
            crate::local::external_ca_install(
                &config,
                String::from_utf8(rogue.to_pem().unwrap()).unwrap(),
                Some(String::from_utf8(rogue_root.to_pem().unwrap()).unwrap()),
            )
            .await
            .is_err()
        );
        assert!(!daemon.is_finished(), "the controller stayed online throughout");
        daemon.abort();
    }

    #[tokio::test]
    async fn controller_recovery_rejects_wrong_password_and_identity_mismatch_before_writes()
     {
        let fixture = recoverable_controller().await;
        let vault_path = fixture.ca_dir.join(ca_vault::VAULT_FILE);
        let before_vault = std::fs::read(&vault_path).unwrap();
        let before_config = std::fs::read(&fixture.config).unwrap();
        let before_map =
            std::fs::read(admin_domain::path(fixture.ca_dir.as_path())).unwrap();
        assert!(
            recover_controller_with_password(
                fixture.ca_dir.as_path(),
                &fixture.config,
                None,
                None,
                "definitely-wrong",
                true,
                &fixture.keytab,
            )
            .await
            .is_err()
        );
        assert_eq!(std::fs::read(&vault_path).unwrap(), before_vault);
        assert_eq!(std::fs::read(&fixture.config).unwrap(), before_config);
        assert_eq!(
            std::fs::read(admin_domain::path(fixture.ca_dir.as_path())).unwrap(),
            before_map
        );
        assert!(!fixture.keytab.exists());

        let error = recover_controller_with_password(
            fixture.ca_dir.as_path(),
            &fixture.config,
            None,
            None,
            &fixture.old_autorenew,
            true,
            &fixture.keytab,
        )
        .await
        .expect_err("the on-box slot must not authorize disaster recovery");
        assert!(error.to_string().contains("off-box"));
        assert_eq!(std::fs::read(&vault_path).unwrap(), before_vault);
        assert_eq!(std::fs::read(&fixture.config).unwrap(), before_config);
        assert!(!fixture.keytab.exists());

        let mut wrong = admin_server_config::load_for_recovery(&fixture.config).unwrap();
        wrong.server_id = AdminServerId::new();
        atomic::write_atomic(
            &fixture.config,
            &serde_json::to_vec_pretty(&wrong).unwrap(),
            0o600,
        )
        .unwrap();
        let after_tamper = std::fs::read(&fixture.config).unwrap();
        assert!(
            recover_controller_with_password(
                fixture.ca_dir.as_path(),
                &fixture.config,
                None,
                None,
                &fixture.recovery,
                true,
                &fixture.keytab,
            )
            .await
            .is_err()
        );
        assert_eq!(std::fs::read(&vault_path).unwrap(), before_vault);
        assert_eq!(std::fs::read(&fixture.config).unwrap(), after_tamper);
        assert_eq!(
            std::fs::read(admin_domain::path(fixture.ca_dir.as_path())).unwrap(),
            before_map
        );
        assert!(!fixture.keytab.exists());
    }

    #[tokio::test]
    async fn external_status_distinguishes_self_signed_and_pending() {
        let dir = tempfile::tempdir().unwrap();
        vaulted_ca(dir.path()).await;
        // A plain vaulted CA: self-signed, cert on disk, nothing pending.
        let s = offline_status(dir.path()).await.external;
        assert!(!s.externally_signed);
        assert!(s.cert_installed);
        assert!(s.pending.is_none());
        // Drop a pending marker as `ca init --external-sign` would.
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        ExternalPending {
            cn: "ca.example.com".to_string(),
            domain: "example.com".to_string(),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec![],
            setup_server: false,
            listen: None,
            units_dir: None,
        }
        .store(&lock, dir.path())
        .unwrap();
        drop(lock);
        // With the cert still on disk, the CA is installed — NOT pending — even
        // though the marker persists for future renewals.
        assert!(offline_status(dir.path()).await.external.pending.is_none());
        // Simulate the true pre-install state (`ca init --external-sign` leaves a
        // key + CSR + marker but no cert yet): remove the cert, and now it is
        // pending its signed certificate.
        std::fs::remove_file(dir.path().join("certificate.pem")).unwrap();
        let s = offline_status(dir.path()).await.external;
        assert!(!s.cert_installed);
        assert_eq!(s.pending.as_ref().map(|(cn, _)| cn.as_str()), Some("ca.example.com"));
    }

    #[tokio::test]
    async fn live_external_csr_preserves_the_controller_ca_key() {
        use openssl::{pkey::PKey, x509::X509Req};
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("ca");
        let lock = ConfigDirLock::acquire(root.path()).unwrap();
        let (key, _csr) = Ca::init_vaulted_external(&crate::ca::CaParams {
            directory: dir.clone(),
            subject: Subject::cn("ca.example.com"),
            san: vec![SanEntry::Dns("ca.example.com".into())],
            key_bits: crate::ca::MIN_KEY_BITS,
            validity: std::time::Duration::from_secs(86400),
        })
        .unwrap();
        CaLifetimes { externally_signed: true, ..CaLifetimes::default() }
            .store(&lock, &dir)
            .unwrap();
        ExternalPending {
            cn: "ca.example.com".into(),
            domain: "example.com".into(),
            country: None,
            state: None,
            locality: None,
            organization: None,
            san: vec!["dns:ca.example.com".into()],
            setup_server: true,
            listen: None,
            units_dir: None,
        }
        .store(&lock, &dir)
        .unwrap();
        let (cn, csr) = external_csr_with_key(&dir, &key).await.unwrap();
        assert_eq!(cn, "ca.example.com");
        let request = X509Req::from_pem(&csr).unwrap();
        let csr_spki = request.public_key().unwrap().public_key_to_der().unwrap();
        let key_spki =
            PKey::private_key_from_pem(&key).unwrap().public_key_to_der().unwrap();
        assert_eq!(csr_spki, key_spki);
    }
}
