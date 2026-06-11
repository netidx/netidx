//! Append-only log of every certificate this CA has issued (and later,
//! revoked): `<ca-dir>/issued.jsonl`, one JSON event per line.
//!
//! The index is what makes certificates *manageable by name*: it backs
//! revoke-by-name (an admin picks `eric.ryu-oh.org` from a list instead
//! of hunting serial numbers), duplicate-name refusal (a new enrollment
//! for a name with a live certificate is rejected — which closes the
//! impostor race on existing identities), and CRL construction (the
//! revoked-and-unexpired set).
//!
//! Appends take an exclusive OS file lock (same discipline as the
//! serial counter — the conf server builds a fresh `Ca` per request and
//! CLI signs can run alongside the daemon), readers a shared one. An
//! event log rather than a mutable table: issuance history is audit
//! material, and revocation is a fact about history, not an edit to it.

use anyhow::{Context, Result};
use serde_derive::{Deserialize, Serialize};
use std::{
    io::{BufRead, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuedCert {
    /// The X.509 serial (this CA issues monotonically from a counter).
    pub serial: u64,
    /// The identity — the cert's first DNS SAN (empty for the rare
    /// no-DNS-SAN cert; still revocable by serial).
    pub name: String,
    /// Identity fingerprint of the cert's public key (grouped base32,
    /// as displayed) — lets the revoke UI show the same glyph the
    /// enrollment showed.
    pub spki_fp: String,
    pub not_after_unix: u64,
    pub issued_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Revocation {
    pub serial: u64,
    pub revoked_unix: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    Issued(IssuedCert),
    Revoked(Revocation),
}

/// A certificate's current state: the issuance record plus its
/// revocation, if any.
#[derive(Debug, Clone)]
pub struct CertState {
    pub cert: IssuedCert,
    pub revoked: Option<Revocation>,
}

impl CertState {
    pub fn live(&self, now_unix: u64) -> bool {
        self.revoked.is_none() && self.cert.not_after_unix > now_unix
    }
}

fn index_path(ca_dir: &Path) -> PathBuf {
    ca_dir.join("issued.jsonl")
}

pub fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

/// Append one event under an exclusive inter-process lock (std's
/// native advisory file locks — flock on unix, same discipline as the
/// serial counter's lock).
pub fn append(ca_dir: &Path, event: &Event) -> Result<()> {
    let path = index_path(ca_dir);
    let mut line = serde_json::to_vec(event).context("serializing index event")?;
    line.push(b'\n');
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    f.lock().context("locking the issuance index")?;
    let r = f.write_all(&line).and_then(|()| f.flush());
    let _ = f.unlock();
    r.with_context(|| format!("appending to {}", path.display()))
}

/// Load and fold the event log into per-certificate state, oldest
/// first. Unparseable lines are skipped (the file is hand-editable in
/// an emergency; one bad line shouldn't brick the CA).
pub fn all(ca_dir: &Path) -> Result<Vec<CertState>> {
    let path = index_path(ca_dir);
    let f = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("opening {}", path.display())),
    };
    f.lock_shared().context("locking the issuance index")?;
    let mut issued: Vec<CertState> = Vec::new();
    for line in std::io::BufReader::new(&f).lines() {
        let line = line.with_context(|| format!("reading {}", path.display()))?;
        match serde_json::from_str::<Event>(&line) {
            Ok(Event::Issued(c)) => issued.push(CertState { cert: c, revoked: None }),
            Ok(Event::Revoked(r)) => {
                for st in issued.iter_mut().filter(|s| s.cert.serial == r.serial) {
                    st.revoked = Some(r.clone());
                }
            }
            Err(_) => continue,
        }
    }
    let _ = f.unlock();
    Ok(issued)
}

/// Every live (unexpired, unrevoked) certificate for `name` (DNS names
/// compare case-insensitively).
pub fn live_for_name(ca_dir: &Path, name: &str) -> Result<Vec<CertState>> {
    let now = now_unix();
    Ok(all(ca_dir)?
        .into_iter()
        .filter(|s| s.live(now) && s.cert.name.eq_ignore_ascii_case(name))
        .collect())
}

/// Every revoked-but-unexpired certificate — the CRL set. Expired
/// revocations fall off naturally, keeping the CRL small.
pub fn revoked_unexpired(ca_dir: &Path) -> Result<Vec<CertState>> {
    let now = now_unix();
    Ok(all(ca_dir)?
        .into_iter()
        .filter(|s| s.revoked.is_some() && s.cert.not_after_unix > now)
        .collect())
}

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
/// revoked-but-unexpired serial from the index, `nextUpdate` =
/// now + [`CRL_VALIDITY`], CRL number = now (monotonic enough — one
/// CRL per second per CA). `ca_key_pem` is the vault-decrypted CA key;
/// the CA cert is read from the dir.
pub fn write_crl(ca_dir: &Path, ca_key_pem: &[u8]) -> Result<()> {
    use rcgen::{
        CertificateRevocationListParams, Issuer, KeyIdMethod, KeyPair,
        RevokedCertParams, SerialNumber,
    };
    use time::OffsetDateTime;
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
    let now = now_unix();
    let ts = |unix: u64| {
        OffsetDateTime::from_unix_timestamp(unix as i64)
            .context("timestamp out of range")
    };
    let revoked_certs = revoked_unexpired(ca_dir)?
        .into_iter()
        .map(|s| {
            let r = s.revoked.expect("revoked_unexpired returns revoked certs");
            Ok(RevokedCertParams {
                serial_number: SerialNumber::from(s.cert.serial),
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
            if next_update.saturating_sub(now_unix()) < CRL_REFRESH.as_secs() {
                write_crl(ca_dir, ca_key_pem)?;
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issued(serial: u64, name: &str, not_after: u64) -> Event {
        Event::Issued(IssuedCert {
            serial,
            name: name.to_string(),
            spki_fp: "FP".to_string(),
            not_after_unix: not_after,
            issued_unix: 1000,
        })
    }

    #[test]
    fn fold_issuance_and_revocation() {
        let dir = tempfile::tempdir().unwrap();
        let now = now_unix();
        append(dir.path(), &issued(1, "eric.ryu-oh.org", now + 1000)).unwrap();
        append(dir.path(), &issued(2, "bob.ryu-oh.org", now + 1000)).unwrap();
        append(dir.path(), &issued(3, "old.ryu-oh.org", now.saturating_sub(10))).unwrap();
        // eric has a live cert; bob gets revoked; old is expired.
        append(
            dir.path(),
            &Event::Revoked(Revocation {
                serial: 2,
                revoked_unix: now,
                reason: "laptop stolen".to_string(),
            }),
        )
        .unwrap();
        let all = all(dir.path()).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(live_for_name(dir.path(), "eric.ryu-oh.org").unwrap().len(), 1);
        // Case-insensitive: DNS names.
        assert_eq!(live_for_name(dir.path(), "ERIC.RYU-OH.ORG").unwrap().len(), 1);
        assert!(live_for_name(dir.path(), "bob.ryu-oh.org").unwrap().is_empty());
        assert!(live_for_name(dir.path(), "old.ryu-oh.org").unwrap().is_empty());
        let crl = revoked_unexpired(dir.path()).unwrap();
        assert_eq!(crl.len(), 1);
        assert_eq!(crl[0].cert.serial, 2);
        assert_eq!(crl[0].revoked.as_ref().unwrap().reason, "laptop stolen");
    }

    #[test]
    fn renewal_history_keeps_every_cert_visible() {
        // The same name issued twice (a renewal): both serials are
        // tracked; revoking the name means revoking every live serial.
        let dir = tempfile::tempdir().unwrap();
        let now = now_unix();
        append(dir.path(), &issued(1, "eric.ryu-oh.org", now + 1000)).unwrap();
        append(dir.path(), &issued(2, "eric.ryu-oh.org", now + 2000)).unwrap();
        let live = live_for_name(dir.path(), "eric.ryu-oh.org").unwrap();
        assert_eq!(live.len(), 2);
        assert_eq!(
            live.iter().map(|s| s.cert.serial).collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn missing_index_is_empty_and_bad_lines_are_skipped() {
        let dir = tempfile::tempdir().unwrap();
        assert!(all(dir.path()).unwrap().is_empty());
        std::fs::write(dir.path().join("issued.jsonl"), b"not json\n").unwrap();
        assert!(all(dir.path()).unwrap().is_empty());
        append(dir.path(), &issued(1, "a.example.com", now_unix() + 100)).unwrap();
        assert_eq!(all(dir.path()).unwrap().len(), 1);
    }
}
