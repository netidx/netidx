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
    time::{SystemTime, UNIX_EPOCH},
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

/// Append one event under an exclusive inter-process lock.
pub fn append(ca_dir: &Path, event: &Event) -> Result<()> {
    use fs3::FileExt;
    let path = index_path(ca_dir);
    let mut line = serde_json::to_vec(event).context("serializing index event")?;
    line.push(b'\n');
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    f.lock_exclusive().context("locking the issuance index")?;
    let r = f.write_all(&line).and_then(|()| f.flush());
    let _ = fs3::FileExt::unlock(&f);
    r.with_context(|| format!("appending to {}", path.display()))
}

/// Load and fold the event log into per-certificate state, oldest
/// first. Unparseable lines are skipped (the file is hand-editable in
/// an emergency; one bad line shouldn't brick the CA).
pub fn all(ca_dir: &Path) -> Result<Vec<CertState>> {
    use fs3::FileExt;
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
    let _ = fs3::FileExt::unlock(&f);
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
