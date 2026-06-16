//! The CA's published Certificate Revocation List: `<ca-dir>/crl.pem`.
//!
//! The issuance index and the revoked set now live in [`crate::ca_store`]
//! (one self-contained record per certificate). This module is just the
//! CRL: build a signed `crl.pem` from `ca_store`'s revoked-but-unexpired
//! set, serve it (`GetCrl`), and re-sign it opportunistically whenever an
//! admin session has the vault unlocked.

use crate::ca_store;
use anyhow::{Context, Result};
use parking_lot::Mutex;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

/// Serializes CRL regeneration. `write_crl` reads the revoked set then
/// atomically renames `crl.pem`; two concurrent writers (a revoke and the
/// opportunistic `refresh_crl_if_stale` an admin session triggers, or two
/// revokes) would otherwise let a writer that scanned the staler index land
/// its rename last and drop a just-revoked serial from the *published* CRL.
/// Holding this across the whole scan→sign→rename makes the last writer
/// always reflect the current committed revocation set. Process-wide is
/// correct: one daemon owns one CA.
static CRL_LOCK: Mutex<()> = Mutex::new(());

/// How long a signed CRL remains valid (`nextUpdate`). Generous,
/// because re-signing needs an admin password (the vault has no
/// signing capability at rest) — but every admin session re-signs
/// opportunistically (see [`refresh_crl_if_stale`]), so a CRL only
/// approaches this age on a network where no admin has signed,
/// approved, or revoked anything for months.
pub const CRL_VALIDITY: Duration = Duration::from_secs(90 * 24 * 3600);

/// Re-sign the CRL when less than this much of its validity remains.
pub const CRL_REFRESH: Duration = Duration::from_secs(30 * 24 * 3600);

/// Canonical CRL location: `<ca-dir>/crl.pem`. The conf server serves
/// it (`GetCrl`); the renewal daemon copies it to `crl.pem` beside each
/// resolver's trusted bundle, where netidx's TLS acceptor picks it up.
pub fn crl_path(ca_dir: &Path) -> PathBuf {
    ca_dir.join("crl.pem")
}

/// Build and atomically store a CRL signed by the CA: every
/// revoked-but-unexpired serial from the store, `nextUpdate` =
/// now + [`CRL_VALIDITY`], CRL number = now (monotonic enough — one
/// CRL per second per CA). `ca_key_pem` is the vault-decrypted CA key;
/// the CA cert is read from the dir.
pub fn write_crl(ca_dir: &Path, ca_key_pem: &[u8]) -> Result<()> {
    use rcgen::{
        CertificateRevocationListParams, Issuer, KeyIdMethod, KeyPair,
        RevokedCertParams, SerialNumber,
    };
    use time::OffsetDateTime;
    // Serialize the scan→sign→rename so concurrent writers can't publish a
    // CRL missing a just-revoked serial (see CRL_LOCK).
    let _crl_guard = CRL_LOCK.lock();
    // Normalize the CA key to PKCS#8 through openssl — the vault may
    // hold PKCS#1 ("BEGIN RSA PRIVATE KEY") from older generations, and
    // rcgen's ring backend only reads PKCS#8.
    let pkey = openssl::pkey::PKey::private_key_from_pem(ca_key_pem)
        .context("parsing CA key")?;
    let pkcs8 = pkey.private_key_to_pem_pkcs8().context("normalizing CA key")?;
    let key = KeyPair::from_pem(
        std::str::from_utf8(&pkcs8).context("CA key pem not utf8")?,
    )
    .context("loading CA key for CRL signing")?;
    let ca_cert_pem = std::fs::read_to_string(ca_dir.join("certificate.pem"))
        .context("reading CA certificate")?;
    let issuer =
        Issuer::from_ca_cert_pem(&ca_cert_pem, key).context("loading CRL issuer")?;
    let now = ca_store::now_unix();
    let ts = |unix: u64| {
        OffsetDateTime::from_unix_timestamp(unix as i64)
            .context("timestamp out of range")
    };
    let revoked_certs = ca_store::revoked_unexpired(ca_dir)?
        .into_iter()
        .map(|s| {
            let r = s.revoked.expect("revoked_unexpired returns revoked certs");
            Ok(RevokedCertParams {
                serial_number: SerialNumber::from(s.serial),
                revocation_time: ts(r.revoked_unix)?,
                reason_code: None,
                invalidity_date: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let crl = CertificateRevocationListParams {
        this_update: ts(now)?,
        next_update: ts(now + CRL_VALIDITY.as_secs())?,
        crl_number: SerialNumber::from(now),
        issuing_distribution_point: None,
        revoked_certs,
        key_identifier_method: KeyIdMethod::Sha256,
    }
    .signed_by(&issuer)
    .context("signing the CRL")?;
    let pem = crl.pem().context("encoding the CRL")?;
    crate::atomic::write_atomic(&crl_path(ca_dir), pem.as_bytes(), 0o644)
}

/// The `nextUpdate` of the CRL at `path`, unix seconds. `Ok(None)` if
/// there is no CRL.
pub fn crl_next_update(path: &Path) -> Result<Option<u64>> {
    use x509_parser::prelude::{CertificateRevocationList, FromDer};
    let pem = match std::fs::read(path) {
        Ok(p) => p,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let der = rustls_pemfile::crls(&mut std::io::Cursor::new(&pem))
        .next()
        .ok_or_else(|| anyhow::anyhow!("no CRL in {}", path.display()))?
        .context("parsing CRL PEM")?;
    let (_, crl) = CertificateRevocationList::from_der(der.as_ref())
        .map_err(|e| anyhow::anyhow!("parsing CRL: {e}"))?;
    Ok(crl.next_update().map(|t| t.timestamp() as u64))
}

/// Re-sign the CRL if one exists and is nearing its `nextUpdate`.
/// Called opportunistically wherever the vault is already unlocked (an
/// admin password is the only thing that can sign) — best-effort; a
/// failure is logged by the caller, never fatal.
pub fn refresh_crl_if_stale(ca_dir: &Path, ca_key_pem: &[u8]) -> Result<bool> {
    match crl_next_update(&crl_path(ca_dir))? {
        None => Ok(false), // no CRL until the first revocation
        Some(next_update) => {
            if next_update.saturating_sub(ca_store::now_unix()) < CRL_REFRESH.as_secs() {
                write_crl(ca_dir, ca_key_pem)?;
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}
