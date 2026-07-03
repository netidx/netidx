//! Local CA slot & external-PKI management behind the Answerer seam.
//!
//! These are the **local** admin ops — no admin server, no glyph. Each picks
//! one of two paths internally: when the daemon is up it hot-swaps in-process
//! over the control socket (SO_PEERCRED superuser, no secret); when it is down
//! the op takes the CA flock itself and unlocks with the box's autorenew keytab
//! or, failing that, the operator's recovery password (via the Answerer, always
//! folded to canonical form by [`offline_ca::unlock_held`] — which is the fix for
//! the external path's former raw-password unlock). The only secrets are the
//! recovery password ([`Field::RecoveryPassword`]) and a freshly minted one to
//! show ([`ca_setup::show_recovery_password`]); everything else is a note.
//!
//! The heavy lifting (TPM gate + seal, slot minting, server setup) reuses the
//! install flow's [`ca_setup`]/[`server_setup`] helpers, so there is one
//! implementation of the security-critical parts.

use crate::{
    admin_local,
    admin_server::read_autorenew_password,
    answer::{Answerer, Field},
    atomic,
    ca::{self, Ca, CaLifetimes, SanEntry, Subject},
    ca_policy::recovery_policy,
    ca_store::CaDir,
    ca_vault::{self, CAVault},
    offline_ca, paths,
    plan::{ca_setup, server_setup, service::ServiceNeed},
};
use anyhow::{Context, Result, bail};
use serde_derive::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

// -- ca auto-approve ---------------------------------------------------------

/// The outcome of [`auto_approve`], for the CLI to narrate.
pub enum AutoApproveOutcome {
    /// The running daemon rotated the box credential in-process (no downtime).
    HotSwapped { warning: Option<String> },
    /// The slot was (re-)minted offline under the flock.
    Offline {
        /// True when framed as a rotation (`--rotate`) vs first enable.
        rotate: bool,
        keytab: PathBuf,
        /// `Some` ⇒ the admin-server config was pointed at the keytab;
        /// `None` with `cfg_error` ⇒ that update failed (non-fatal).
        cfg_path: Option<PathBuf>,
        cfg_error: Option<String>,
    },
}

/// Set up (or `--rotate`) the autorenew slot so the admin server approves
/// verified renewals in-process. Hot-swaps on the running daemon; otherwise
/// takes the flock, TPM-gates, unlocks with the recovery password (folded to
/// canonical form), (re-)mints the slot + keytab, and points the config at it.
pub async fn auto_approve(
    ans: &mut dyn Answerer,
    ca_dir: PathBuf,
    cfg: Option<PathBuf>,
    rotate: bool,
    insecure_no_tpm: bool,
) -> Result<AutoApproveOutcome> {
    // Hot-swap path: the running daemon owns the CA and re-wraps the slot,
    // swaps the live key, and rewrites the keytab — no flock, no secret.
    if let Some(cfg) = &cfg
        && admin_local::daemon_running(cfg).await
    {
        let warning = admin_local::rotate_autorenew(cfg).await?;
        return Ok(AutoApproveOutcome::HotSwapped { warning });
    }
    // Offline / first-time setup: same TPM gate as init (a plaintext keytab is
    // a CA-key-equivalent credential, so refuse without a TPM unless opted in).
    ca_setup::tpm_gate(ans, insecure_no_tpm)?;
    // Re-minting autorenew removes the old slot first, so the recovery password
    // (not the old keytab) authorizes it — and after a TPM clear the old keytab
    // is unsealable anyway, so recovery is the only way in.
    let typed = ans.secret(Field::RecoveryPassword, None).await?;
    let recovery = ca_vault::normalize_recovery_password(typed.as_str());
    let cadir = CaDir::open(&ca_dir).context(
        "setting up autorenew needs exclusive access; the admin server must be stopped",
    )?;
    let keytab = ca_setup::setup_autorenew_slot(ans, &cadir, &recovery, insecure_no_tpm)?;
    let (cfg_path, cfg_error) = match server_setup::set_ca_autorenew(&keytab) {
        Ok(p) => (Some(p), None),
        Err(e) => (None, Some(format!("{e:#}"))),
    };
    Ok(AutoApproveOutcome::Offline { rotate, keytab, cfg_path, cfg_error })
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

/// Report the autorenew credential's state without taking the flock (a read
/// query, safe to run against a live daemon): whether the slot exists, whether
/// the keytab is present and sealed, and whether the config wires it in.
pub fn auto_approve_status(ca_dir: &Path, cfg: Option<&Path>) -> Result<AutorenewStatus> {
    let keytab = offline_ca::autorenew_keytab_path()?;
    let keytab_present = keytab.exists();
    let keytab_sealed = keytab_present
        && std::fs::read(&keytab).ok().map(|b| netidx_tpm::is_sealed(&b)).unwrap_or(false);
    let slot_present = CAVault::exists(ca_dir)
        && CAVault::new(ca_dir.to_path_buf())
            .signing_slot_names()
            .map(|names| names.iter().any(|n| n == crate::admin_server::AUTORENEW_ADMIN))
            .unwrap_or(false);
    let wired_in_config = cfg
        .and_then(|p| crate::admin_server_config::AdminServerConfig::load(p).ok())
        .and_then(|c| c.roles.ca.map(|ca| ca.autorenew.is_some()))
        .unwrap_or(false);
    Ok(AutorenewStatus { slot_present, keytab, keytab_present, keytab_sealed, wired_in_config })
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
    ca_dir: PathBuf,
    cfg: Option<PathBuf>,
) -> Result<RecoveryRotateOutcome> {
    if let Some(cfg) = &cfg
        && admin_local::daemon_running(cfg).await
    {
        let new_pw = admin_local::rotate_recovery(cfg).await?;
        ca_setup::show_recovery_password(ans, &new_pw);
        return Ok(RecoveryRotateOutcome::HotSwapped);
    }
    if !CAVault::exists(&ca_dir) {
        bail!("no vault-protected CA at {}", ca_dir.display());
    }
    let keytab = offline_ca::autorenew_keytab_path()?;
    let autorenew_pw = read_autorenew_password(&keytab).with_context(|| {
        format!(
            "rotating the recovery password needs the autorenew keytab ({}); it \
             authorizes the re-mint on the CA box. (Set one up with \
             `netidx admin ca auto-approve`.)",
            keytab.display()
        )
    })?;
    let cadir = CaDir::open(&ca_dir).context(
        "rotating recovery needs exclusive access; stop the admin server first",
    )?;
    // Confirm the keytab credential unlocks this CA BEFORE removing the old
    // recovery slot — a stale keytab must not leave the CA with no recovery
    // slot. (The recovered key is dropped/zeroized immediately.)
    cadir.vault.read().unlock(&autorenew_pw).with_context(|| {
        format!(
            "the autorenew keytab ({}) did not unlock this CA — its credential is \
             stale. Re-mint it with `netidx admin ca auto-approve --rotate` (needs \
             the recovery password) and try again.",
            keytab.display()
        )
    })?;
    // Atomic re-key: the old recovery slot is dropped and the new one added in a
    // single vault write, authorized by the box's autorenew credential. A failed
    // write leaves the old recovery slot intact — the CA is never momentarily
    // left with no recovery credential (the former remove-then-add window).
    let new_pw = ca_vault::gen_recovery_password();
    cadir.vault.write().replace_signing_slot(
        &autorenew_pw,
        ca_vault::RECOVERY_ADMIN,
        &new_pw,
        recovery_policy(),
    )?;
    ca_setup::show_recovery_password(ans, &new_pw);
    Ok(RecoveryRotateOutcome::Offline { ca_dir })
}

/// A read-only report of the recovery slot's state (never the password).
pub struct RecoveryStatus {
    pub slot_present: bool,
    /// Whether the box's autorenew keytab (the offline re-mint authority) exists.
    pub keytab_present: bool,
}

/// Report whether the CA has a recovery slot and whether the on-box authority
/// (the autorenew keytab) needed to rotate it offline is present.
pub fn recovery_status(ca_dir: &Path) -> Result<RecoveryStatus> {
    let slot_present = CAVault::exists(ca_dir)
        && CAVault::new(ca_dir.to_path_buf())
            .signing_slot_names()
            .map(|names| names.iter().any(|n| n == ca_vault::RECOVERY_ADMIN))
            .unwrap_or(false);
    let keytab_present = offline_ca::autorenew_keytab_path().map(|k| k.exists()).unwrap_or(false);
    Ok(RecoveryStatus { slot_present, keytab_present })
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

    pub fn store(&self, dir: &Path) -> Result<()> {
        let bytes =
            serde_json::to_vec_pretty(self).context("encoding the external-sign marker")?;
        atomic::write_atomic(&dir.join(Self::FILE), &bytes, 0o644)
    }

    pub fn load(dir: &Path) -> Result<Self> {
        let bytes = std::fs::read(dir.join(Self::FILE)).context(
            "reading the external-sign marker — was this CA created with \
             `ca init --external-sign`?",
        )?;
        serde_json::from_slice(&bytes).context("parsing the external-sign marker")
    }
}

/// Unlock the CA key while holding the flock, then hand back both — the fixed
/// unlock (via [`offline_ca::unlock_held`]) folds a typed recovery password to
/// canonical form, so the grouped displayed form now works (it did not before).
async fn external_ca_key(
    ans: &mut dyn Answerer,
    dir: &Path,
) -> Result<(Zeroizing<Vec<u8>>, CaDir)> {
    let cadir = CaDir::open(dir)
        .context("opening the CA (stop the admin server first if it is running)")?;
    let unlocked = offline_ca::unlock_held(ans, &cadir, dir).await?;
    Ok((unlocked.ca_key_pem, cadir))
}

/// (Re-)emit a CSR for the CA cert over the existing key — for renewing an
/// externally-signed CA cert (same key ⇒ glyph unchanged). Returns the CSR path.
pub async fn external_emit_csr(ans: &mut dyn Answerer, ca_dir: PathBuf) -> Result<PathBuf> {
    let m = ExternalPending::load(&ca_dir)?;
    let san = if m.san.is_empty() {
        vec![SanEntry::Dns(m.cn.clone())]
    } else {
        offline_ca::parse_sans(&m.san, &m.cn)?
    };
    // Rebuild the full subject so a renewal CSR carries the same DN as the
    // original CA cert.
    let subject = Subject {
        common_name: m.cn.clone(),
        country: m.country.clone(),
        state: m.state.clone(),
        locality: m.locality.clone(),
        organization: m.organization.clone(),
    };
    let (key, _cadir) = external_ca_key(ans, &ca_dir).await?;
    let csr = ca::ca_csr_from_key(&key, &subject, &san)?;
    let csr_path = offline_ca::default_csr_filename(&m.cn);
    atomic::write_atomic(&csr_path, &csr, 0o644)
        .with_context(|| format!("writing CSR to {}", csr_path.display()))?;
    Ok(csr_path)
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
    ca_dir: PathBuf,
    signed: &Path,
    root: Option<&Path>,
) -> Result<ExternalInstallOutcome> {
    let m = ExternalPending::load(&ca_dir)?;
    let signed_pem =
        std::fs::read(signed).with_context(|| format!("reading {}", signed.display()))?;
    let root_pem = match root {
        Some(p) => {
            Some(std::fs::read(p).with_context(|| format!("reading {}", p.display()))?)
        }
        None => None,
    };
    let (key, cadir) = external_ca_key(ans, &ca_dir).await?;
    let (intermediate_pem, external_root_pem) =
        ca::validate_external_ca_cert(&signed_pem, root_pem.as_deref(), &key)?;
    // certificate.pem is the intermediate ALONE (the network glyph is its key);
    // trusted.pem is [external root, intermediate].
    atomic::write_atomic(&ca_dir.join("certificate.pem"), &intermediate_pem, 0o644)
        .context("installing certificate.pem")?;
    let mut trusted = external_root_pem;
    trusted.extend_from_slice(&intermediate_pem);
    atomic::write_atomic(&ca_dir.join("trusted.pem"), &trusted, 0o644)
        .context("installing trusted.pem")?;
    ca_setup::show_ca_identity(ans, &ca_dir)?;
    if !m.setup_server {
        return Ok(ExternalInstallOutcome::OfflineCa);
    }
    // Served CA. Decide "first install vs renewal" on whether the admin server
    // is configured yet — NOT on certificate.pem (which we just wrote), so a
    // failed/interrupted first-install tail is retriable, not reclassified.
    if paths::discover_admin_server_config().is_err() {
        let ca = Ca::from_pem(ca_dir.clone(), &key, &intermediate_pem)
            .context("reconstructing the CA from the installed certificate")?;
        // setup_server takes the CA flock itself — release ours first.
        drop(cadir);
        let need = server_setup::setup_server(
            ans,
            server_setup::SetupArgs {
                ca_dir: &ca_dir,
                ca: &ca,
                domain: &m.domain,
                listen: m.listen,
                listen_hint: None,
                units_dir: m.units_dir.as_deref(),
            },
        )
        .await?;
        let cfg_path = server_setup::set_ca_autorenew(&offline_ca::autorenew_keytab_path()?)?;
        return Ok(ExternalInstallOutcome::FirstInstall { need, cfg_path });
    }
    // Already configured: this is a renewal. Keep autorenew wired (idempotent).
    let keytab = offline_ca::autorenew_keytab_path()?;
    if keytab.exists() {
        let _ = server_setup::set_ca_autorenew(&keytab);
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

/// Report whether this CA is externally signed, whether its cert is installed,
/// and whether a bootstrap is awaiting a signed certificate.
pub fn external_status(ca_dir: &Path) -> Result<ExternalStatus> {
    let externally_signed =
        CaLifetimes::load(ca_dir).map(|l| l.externally_signed).unwrap_or(false);
    let cert_installed = ca_dir.join("certificate.pem").is_file();
    // The bootstrap marker persists past install (a served external CA reads its
    // `setup_server`/domain/listen on every re-install / renewal), so "awaiting a
    // signed cert" is specifically the init→install gap: a marker with no cert yet.
    let pending = if cert_installed {
        None
    } else {
        ExternalPending::load(ca_dir).ok().map(|m| (m.cn, m.domain))
    };
    Ok(ExternalStatus { externally_signed, cert_installed, pending })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ca::{CaParams, Subject};

    /// A vaulted CA in `dir` with recovery + autorenew slots (as the install
    /// path leaves it), for the read-only status queries.
    fn vaulted_ca(dir: &Path) {
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
            .create(&key, ca_vault::RECOVERY_ADMIN, &recovery_pw, recovery_policy())
            .unwrap();
        std::fs::remove_file(dir.join("private.key")).unwrap();
        vault
            .add_signing_slot(
                &recovery_pw,
                crate::admin_server::AUTORENEW_ADMIN,
                "renew-secret-01234",
                crate::ca_policy::autorenew_policy(),
            )
            .unwrap();
    }

    #[test]
    fn recovery_and_autorenew_slots_are_reported_present() {
        let dir = tempfile::tempdir().unwrap();
        vaulted_ca(dir.path());
        assert!(recovery_status(dir.path()).unwrap().slot_present);
        // No cfg ⇒ wired_in_config is false, but the slot is read from the vault.
        let a = auto_approve_status(dir.path(), None).unwrap();
        assert!(a.slot_present);
        assert!(!a.wired_in_config);
    }

    #[test]
    fn external_status_distinguishes_self_signed_and_pending() {
        let dir = tempfile::tempdir().unwrap();
        vaulted_ca(dir.path());
        // A plain vaulted CA: self-signed, cert on disk, nothing pending.
        let s = external_status(dir.path()).unwrap();
        assert!(!s.externally_signed);
        assert!(s.cert_installed);
        assert!(s.pending.is_none());
        // Drop a pending marker as `ca init --external-sign` would.
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
        .store(dir.path())
        .unwrap();
        // With the cert still on disk, the CA is installed — NOT pending — even
        // though the marker persists for future renewals.
        assert!(external_status(dir.path()).unwrap().pending.is_none());
        // Simulate the true pre-install state (`ca init --external-sign` leaves a
        // key + CSR + marker but no cert yet): remove the cert, and now it is
        // pending its signed certificate.
        std::fs::remove_file(dir.path().join("certificate.pem")).unwrap();
        let s = external_status(dir.path()).unwrap();
        assert!(!s.cert_installed);
        assert_eq!(s.pending.as_ref().map(|(cn, _)| cn.as_str()), Some("ca.example.com"));
    }
}
