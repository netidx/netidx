use super::{
    AUTORENEW_ADMIN, MutableState, Server, audit,
    auth::{PreparedServerUnlock, run_signing, server_unlock},
};
use crate::{
    admin_proto::{
        self, BackupOk, BackupResponse, ExternalCaCsrOk, ExternalCaCsrResponse,
        ExternalCaInstallOk, ExternalCaInstallRequest, ExternalCaInstallResponse,
        RotateAutorenewResponse, RotateRecoveryResponse, Secret,
    },
    ca_vault,
    config_lock::ConfigDirLock,
    transport,
};
use anyhow::{Context, Result};
use log::error;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;
use zeroize::Zeroizing;

pub(crate) fn read_autorenew_password(keytab: &Path) -> Result<Zeroizing<String>> {
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

pub(super) async fn handle_backup(
    state: &Arc<Server>,
    req: &admin_proto::BackupRequest,
    prepared_server_unlock: &PreparedServerUnlock,
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
            server_unlock(
                state.ca.as_mut().expect("CA role held"),
                prepared_server_unlock,
            )
            .await
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
    BackupResponse::Ok(BackupOk {
        target: outcome.target.to_string_lossy().into_owned(),
        ca_fingerprint: outcome.ca_fingerprint,
        controller: outcome.controller,
        map_version: outcome.map_version,
        highest_serial: outcome.highest_serial,
        files: outcome.files,
        bytes: outcome.bytes,
        manifest_sha256: outcome.manifest_sha256,
    })
}

/// Append a freshly enrolled admin server to our peer list (and persist
/// it when we have a config path). The CA host thereby becomes the
/// well-known starting point for peer walks.

pub(super) async fn handle_ca_status(
    state: &Server,
    local: bool,
) -> admin_proto::CaStatusResponse {
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
                names
                    .iter()
                    .any(|name| name == netidx_admin_proto::policy::RECOVERY_ADMIN),
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
        crate::ops::slots::ExternalPending::load_async(&dir)
            .await
            .ok()
            .map(|pending| (pending.cn, pending.domain))
    };
    CaStatusResponse::Ok(admin_proto::CaStatus {
        autorenew_slot_present,
        recovery_slot_present,
        externally_signed,
        cert_installed,
        pending,
    })
}

pub(super) async fn handle_external_ca_csr(
    state: &Server,
    prepared_server_unlock: &PreparedServerUnlock,
    local: bool,
) -> ExternalCaCsrResponse {
    state
        .write_async(async move |state| {
            handle_external_ca_csr_inner(state, prepared_server_unlock, local).await
        })
        .await
}

async fn handle_external_ca_csr_inner(
    state: &mut MutableState,
    prepared_server_unlock: &PreparedServerUnlock,
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
    let signing = match server_unlock(ca, prepared_server_unlock).await {
        Ok(s) => s,
        Err(e) => return err(e),
    };
    match crate::ops::slots::external_csr_with_key(&dir, &signing.ca_key_pem).await {
        Ok((common_name, csr)) => match String::from_utf8(csr) {
            Ok(csr_pem) => {
                audit(&dir, "local", "external-ca-csr", &common_name, Duration::ZERO)
                    .await;
                ExternalCaCsrResponse::Ok(ExternalCaCsrOk { common_name, csr_pem })
            }
            Err(e) => err(format!("encoding the generated CSR: {e}")),
        },
        Err(e) => err(format!("generating the external-CA CSR: {e:#}")),
    }
}

pub(super) async fn handle_external_ca_install(
    state: &Server,
    req: &ExternalCaInstallRequest,
    prepared_server_unlock: &PreparedServerUnlock,
    local: bool,
) -> ExternalCaInstallResponse {
    let req = req.clone();
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |state| {
            handle_external_ca_install_inner(
                &config_lock,
                state,
                &req,
                prepared_server_unlock,
                local,
            )
            .await
        })
        .await
}

async fn handle_external_ca_install_inner(
    config_lock: &ConfigDirLock,
    state: &mut MutableState,
    req: &ExternalCaInstallRequest,
    prepared_server_unlock: &PreparedServerUnlock,
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
    let signing = match server_unlock(ca, prepared_server_unlock).await {
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
    let reconciled = match transport::reconcile_trusted_bundle(&installed, &candidate) {
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
    ExternalCaInstallResponse::Ok(ExternalCaInstallOk { ca_fingerprint: new_fp.text() })
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

/// `RotateRecovery` (local control socket ONLY): mint a fresh recovery
/// (off-box break-glass) password using the box's own autorenew credential
/// to unlock MK and re-wrap the recovery slot. Returns the new password in
/// grouped display form — shown to the operator once, never stored. Refused
/// over the network admin plane: reaching the local socket is itself the
/// authority, and a recovery password must never travel the network.
pub(super) async fn rotate_recovery(
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
            netidx_admin_proto::policy::RECOVERY_ADMIN,
            &worker_new_pw,
            netidx_admin_proto::policy::recovery_policy(),
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
                netidx_admin_proto::policy::RECOVERY_ADMIN,
                Duration::ZERO,
            )
            .await;
            RotateRecoveryResponse::Ok(Secret(canonical))
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

pub(super) async fn rotate_autorenew(
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
            RotateAutorenewResponse::Ok(prepared.warning)
        })
        .await
}
