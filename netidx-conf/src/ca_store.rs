//! The CA's request store — one self-contained JSON record per request,
//! one atomic write per state transition. The daemon is the sole owner
//! (a single exclusive flock on the CA dir, taken at startup), so the
//! only mutual exclusion needed for issuance is an in-process mutex; the
//! filesystem supplies the rest, because each fact that must be atomic
//! lives in exactly one file.
//!
//! Three directories keep the active working set away from history:
//!   - `queue/<id>.json`  — Pending requests (the active queue).
//!   - `issued/<id>.json` — Signed records: the queue outcome **and** the
//!     issuance index **and** the id-map groups **and** the revocation/push
//!     state, all in one record. The permanent history.
//!   - `denied/<id>.json` — Denied requests, pruned with the queue by TTL.
//!
//! A terminal transition *moves* the record out of `queue/` (write the
//! `issued/`/`denied/` file — the atomic commit — then remove the `queue/`
//! file). `status` checks `issued/`→`denied/`→`queue/`, so a stale `queue/`
//! file left by a crash between the two steps loses to the committed
//! terminal record and is swept by [`prune`].
//!
//! Request ids are random hex, validated on every lookup (an id is a
//! wire-supplied string that becomes a file name): anything but
//! `[0-9a-f]{32}` is rejected (no path traversal).

use crate::{atomic, conf_proto::NodeKind};
use anyhow::{Context, Result};
use serde_derive::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Pending/denied records older than this are pruned. Generous — the
/// admin may be at lunch — but bounded. `issued/` is never pruned.
pub const TTL: Duration = Duration::from_secs(24 * 3600);

/// Cap on pending requests — a bound on unauthenticated disk writes,
/// far above any real enrollment burst.
pub const MAX_PENDING: usize = 64;

pub fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedReq {
    pub id: String,
    pub kind: NodeKind,
    pub csr_pem: String,
    pub requested_name: String,
    pub requested_validity_days: u32,
    /// Unix seconds when the request was queued.
    pub received_unix: u64,
    /// Socket address the request arrived from (display context).
    pub peer: String,
    /// `Some(serial)` ⇒ the enqueue connection was authenticated by the
    /// live, in-index certificate of serial `serial` for exactly
    /// `requested_name` (and its presented key matched that record) — a
    /// proof-of-possession renewal. The serial is re-checked still-live
    /// under the issuer lock at approval, so a revocation between enqueue
    /// and approval cannot be outrun. `None` ⇒ an ordinary request.
    #[serde(default)]
    pub renewal_of: Option<u64>,
    /// `Some` ⇒ a conf-server enrollment.
    #[serde(default)]
    pub enroll_listen: Option<SocketAddr>,
}

impl QueuedReq {
    /// A fresh request: random id, stamped now.
    pub fn new(
        kind: NodeKind,
        csr_pem: String,
        requested_name: String,
        requested_validity_days: u32,
        peer: String,
        renewal_of: Option<u64>,
        enroll_listen: Option<SocketAddr>,
    ) -> Self {
        QueuedReq {
            id: new_id(),
            kind,
            csr_pem,
            requested_name,
            requested_validity_days,
            received_unix: now_unix(),
            peer,
            renewal_of,
            enroll_listen,
        }
    }

    /// A verified renewal: it continues an identity already approved once,
    /// proven by possession of its live key at enqueue.
    pub fn is_verified_renewal(&self) -> bool {
        self.renewal_of.is_some()
    }

    pub fn age_secs(&self) -> u64 {
        now_unix().saturating_sub(self.received_unix)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Revocation {
    pub serial: u64,
    pub revoked_unix: u64,
    pub reason: String,
}

/// A signed (issued) record — the permanent issuance entry. Carries the
/// originating request, the cert, the id-map groups, and the
/// revocation/push state, so "request `id` is Signed as serial `S`, here
/// is its cert and groups" is one atomic file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuedRecord {
    pub req: QueuedReq,
    pub serial: u64,
    /// The DNS SAN actually signed (== `req.requested_name`, or the
    /// reserved serving name for a conf-server enrollment).
    pub name: String,
    /// SPKI fingerprint of the leaf public key (revoke UI glyph).
    pub spki_fp: String,
    pub cert_pem: String,
    #[serde(default)]
    pub groups: Vec<String>,
    pub not_after_unix: u64,
    pub issued_unix: u64,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub revoked: Option<Revocation>,
    /// Whether the id-map registration for this identity has been pushed.
    #[serde(default)]
    pub push_done: bool,
}

impl IssuedRecord {
    pub fn live(&self, now_unix: u64) -> bool {
        self.revoked.is_none() && self.not_after_unix > now_unix
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeniedRecord {
    req: QueuedReq,
    reason: String,
}

/// The poll outcome derived from a Signed record + the current trust
/// bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedOutcome {
    pub signed_cert_pem: String,
    pub trusted_pem: String,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeniedOutcome {
    pub reason: String,
}

/// The state of a request id.
pub enum Status {
    Pending(QueuedReq),
    Signed(SignedOutcome),
    Denied(DeniedOutcome),
    Unknown,
}

fn queue_dir(ca_dir: &Path) -> PathBuf {
    ca_dir.join("queue")
}
fn issued_dir(ca_dir: &Path) -> PathBuf {
    ca_dir.join("issued")
}
fn denied_dir(ca_dir: &Path) -> PathBuf {
    ca_dir.join("denied")
}
fn queue_path(ca_dir: &Path, id: &str) -> PathBuf {
    queue_dir(ca_dir).join(format!("{id}.json"))
}
fn issued_path(ca_dir: &Path, id: &str) -> PathBuf {
    issued_dir(ca_dir).join(format!("{id}.json"))
}
fn denied_path(ca_dir: &Path, id: &str) -> PathBuf {
    denied_dir(ca_dir).join(format!("{id}.json"))
}

/// Generate a fresh random request id (16 bytes, lowercase hex).
pub fn new_id() -> String {
    use rand::Rng;
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    let mut s = String::with_capacity(32);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// True iff `id` has exactly the shape [`new_id`] produces. Everything
/// that touches the filesystem goes through this — ids arrive over the
/// wire.
pub fn valid_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The trust bundle handed to joining nodes: the admin's `trusted.pem`
/// federation bundle if present, else the CA's own cert.
pub fn read_trusted_bundle(ca_dir: &Path) -> Result<String> {
    let bundle = ca_dir.join("trusted.pem");
    let path = if bundle.exists() { bundle } else { ca_dir.join("certificate.pem") };
    let bytes =
        std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    String::from_utf8(bytes).context("trust bundle is not utf8")
}

/// Read the issued record for `id`, if it exists (the request was
/// signed). The daemon uses this on a Signed poll to decide whether the
/// id-map push still needs to run (`groups` non-empty and `!push_done`).
pub fn read_issued(ca_dir: &Path, id: &str) -> Result<Option<IssuedRecord>> {
    if !valid_id(id) {
        return Ok(None);
    }
    match std::fs::read(issued_path(ca_dir, id)) {
        Ok(b) => Ok(Some(
            serde_json::from_slice(&b).context("parsing issued record")?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading issued record {id}")),
    }
}

/// Take the exclusive, daemon-lifetime lock on the CA directory
/// (`<ca-dir>/ca.lock`). The conf-server daemon is the sole owner of the
/// CA state; a second daemon for the same CA fails here. The returned
/// file must be held for the process lifetime — dropping it releases the
/// lock.
pub fn lock_ca_exclusive(ca_dir: &Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(ca_dir)
        .with_context(|| format!("creating {}", ca_dir.display()))?;
    let path = ca_dir.join("ca.lock");
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("opening CA lock {}", path.display()))?;
    // Non-blocking: a contended (or otherwise unacquirable) lock means
    // another daemon owns this CA — fail fast rather than hang.
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(_) => anyhow::bail!(
            "another conf server already owns this CA ({}); only one daemon \
             may hold it",
            ca_dir.display()
        ),
    }
}

fn outcome_of(ca_dir: &Path, rec: &IssuedRecord) -> Result<SignedOutcome> {
    Ok(SignedOutcome {
        signed_cert_pem: rec.cert_pem.clone(),
        trusted_pem: read_trusted_bundle(ca_dir)?,
        warnings: rec.warnings.clone(),
    })
}

/// All issued records (a scan of `issued/`). Unparseable files are
/// skipped (an operator might hand-edit in an emergency).
fn all_issued(ca_dir: &Path) -> Result<Vec<IssuedRecord>> {
    let dir = issued_dir(ca_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("listing {}", dir.display())),
    };
    let mut out = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let Some(id) = path.file_stem().and_then(|s| s.to_str()) else { continue };
        if !valid_id(id) {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else { continue };
        let Ok(rec) = serde_json::from_slice::<IssuedRecord>(&bytes) else { continue };
        out.push(rec);
    }
    Ok(out)
}

/// Add a request to the active queue. Prunes first; refuses at
/// [`MAX_PENDING`].
pub fn enqueue(ca_dir: &Path, req: &QueuedReq) -> Result<()> {
    anyhow::ensure!(valid_id(&req.id), "malformed request id");
    let dir = queue_dir(ca_dir);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    prune(ca_dir)?;
    anyhow::ensure!(
        pending(ca_dir)?.len() < MAX_PENDING,
        "the signing queue is full ({MAX_PENDING} pending requests)"
    );
    let bytes = serde_json::to_vec_pretty(req).context("serializing queued request")?;
    atomic::write_atomic(&queue_path(ca_dir, &req.id), &bytes, 0o644)
}

/// Every pending request (active, not expired, not already terminal),
/// oldest first.
pub fn pending(ca_dir: &Path) -> Result<Vec<QueuedReq>> {
    let dir = queue_dir(ca_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("listing {}", dir.display())),
    };
    let now = now_unix();
    let mut out: Vec<QueuedReq> = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let Some(id) = path.file_stem().and_then(|s| s.to_str()) else { continue };
        if !valid_id(id) {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else { continue };
        let Ok(req) = serde_json::from_slice::<QueuedReq>(&bytes) else { continue };
        let expired = now.saturating_sub(req.received_unix) > TTL.as_secs();
        let terminal =
            issued_path(ca_dir, id).exists() || denied_path(ca_dir, id).exists();
        if !expired && !terminal {
            out.push(req);
        }
    }
    out.sort_by_key(|r| r.received_unix);
    Ok(out)
}

/// The state of request `id` — terminal records win over a stale queue
/// entry.
pub fn status(ca_dir: &Path, id: &str) -> Result<Status> {
    if !valid_id(id) {
        return Ok(Status::Unknown);
    }
    if let Some(rec) = read_issued(ca_dir, id)? {
        return Ok(Status::Signed(outcome_of(ca_dir, &rec)?));
    }
    if let Ok(bytes) = std::fs::read(denied_path(ca_dir, id)) {
        let d: DeniedRecord =
            serde_json::from_slice(&bytes).context("parsing denied record")?;
        return Ok(Status::Denied(DeniedOutcome { reason: d.reason }));
    }
    match std::fs::read(queue_path(ca_dir, id)) {
        Ok(bytes) => {
            let req: QueuedReq =
                serde_json::from_slice(&bytes).context("parsing queued request")?;
            if now_unix().saturating_sub(req.received_unix) > TTL.as_secs() {
                Ok(Status::Unknown)
            } else {
                Ok(Status::Pending(req))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Status::Unknown),
        Err(e) => Err(e).context("reading queued request"),
    }
}

/// Commit an issuance: write the `issued/` record (the atomic commit),
/// then remove the `queue/` entry (cleanup). The single write is the
/// transaction; a crash before it leaves the request Pending, after it
/// leaves a complete Signed record.
pub fn commit_signed(ca_dir: &Path, rec: &IssuedRecord) -> Result<()> {
    anyhow::ensure!(valid_id(&rec.req.id), "malformed request id");
    let dir = issued_dir(ca_dir);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let bytes = serde_json::to_vec_pretty(rec).context("serializing issued record")?;
    atomic::write_atomic(&issued_path(ca_dir, &rec.req.id), &bytes, 0o644)?;
    let _ = std::fs::remove_file(queue_path(ca_dir, &rec.req.id));
    Ok(())
}

/// Record a denial and move it out of the active queue.
pub fn deny(ca_dir: &Path, req: &QueuedReq, reason: &str) -> Result<()> {
    anyhow::ensure!(valid_id(&req.id), "malformed request id");
    let dir = denied_dir(ca_dir);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let rec = DeniedRecord { req: req.clone(), reason: reason.to_string() };
    let bytes = serde_json::to_vec_pretty(&rec).context("serializing denied record")?;
    atomic::write_atomic(&denied_path(ca_dir, &req.id), &bytes, 0o644)?;
    let _ = std::fs::remove_file(queue_path(ca_dir, &req.id));
    Ok(())
}

/// Every live (unexpired, unrevoked) issuance for `name` — the one-live
/// check and revoke-by-name source. DNS names compare case-insensitively.
pub fn live_for_name(ca_dir: &Path, name: &str) -> Result<Vec<IssuedRecord>> {
    let now = now_unix();
    Ok(all_issued(ca_dir)?
        .into_iter()
        .filter(|r| r.live(now) && r.name.eq_ignore_ascii_case(name))
        .collect())
}

/// Every revoked-but-unexpired issuance — the CRL set.
pub fn revoked_unexpired(ca_dir: &Path) -> Result<Vec<IssuedRecord>> {
    let now = now_unix();
    Ok(all_issued(ca_dir)?
        .into_iter()
        .filter(|r| r.revoked.is_some() && r.not_after_unix > now)
        .collect())
}

/// Every Signed record (for the admin `list` / revoke UI).
pub fn list_signed(ca_dir: &Path) -> Result<Vec<IssuedRecord>> {
    all_issued(ca_dir)
}

/// Mark serial `serial` revoked (rewrites its one `issued/` record).
/// Returns true if it was live and is now revoked, false if not found or
/// already revoked.
pub fn revoke(ca_dir: &Path, serial: u64, rev: Revocation) -> Result<bool> {
    for mut rec in all_issued(ca_dir)? {
        if rec.serial == serial {
            if rec.revoked.is_some() {
                return Ok(false);
            }
            rec.revoked = Some(rev);
            let bytes =
                serde_json::to_vec_pretty(&rec).context("serializing issued record")?;
            atomic::write_atomic(&issued_path(ca_dir, &rec.req.id), &bytes, 0o644)?;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Record that the id-map registration for this issuance has been
/// pushed (rewrites its `issued/` record).
pub fn set_push_done(ca_dir: &Path, id: &str) -> Result<()> {
    if let Some(mut rec) = read_issued(ca_dir, id)?
        && !rec.push_done
    {
        rec.push_done = true;
        let bytes =
            serde_json::to_vec_pretty(&rec).context("serializing issued record")?;
        atomic::write_atomic(&issued_path(ca_dir, id), &bytes, 0o644)?;
    }
    Ok(())
}

/// Live issuances whose id-map groups were never confirmed pushed — the
/// startup/poll id-map recovery set.
pub fn pending_pushes(ca_dir: &Path) -> Result<Vec<IssuedRecord>> {
    let now = now_unix();
    Ok(all_issued(ca_dir)?
        .into_iter()
        .filter(|r| r.live(now) && !r.groups.is_empty() && !r.push_done)
        .collect())
}

/// The highest serial ever issued (to seed the in-memory counter at
/// startup). `None` if nothing has been issued.
pub fn max_serial(ca_dir: &Path) -> Result<Option<u64>> {
    Ok(all_issued(ca_dir)?.into_iter().map(|r| r.serial).max())
}

/// The next X.509 serial to mint: one past every serial the store knows
/// and the CA cert's own serial. The daemon seeds its in-memory counter
/// from this once at startup; offline bootstrap issuance (before the
/// daemon owns the CA) allocates from it per issuance — and records the
/// result, so the next allocation and the daemon both move past it.
pub fn next_serial(ca_dir: &Path) -> Result<u64> {
    let max_issued = max_serial(ca_dir)?.unwrap_or(0);
    let ca_cert = crate::ca::ca_cert_serial(ca_dir).unwrap_or(1);
    Ok(max_issued.max(ca_cert) + 1)
}

/// The `notAfter` of the first (leaf) certificate in a signed PEM, as
/// unix seconds. The record's liveness has to reflect the cert the CA
/// actually signed: [`crate::ca::Ca::sign_request`] clamps a leaf to the
/// CA's remaining lifetime, so the *requested* validity can overstate it —
/// and an overstated `not_after_unix` would keep an already-expired cert
/// "live" (blocking its replacement) and pinned in the CRL too long.
fn cert_not_after_unix(cert_pem: &str) -> Result<u64> {
    use x509_parser::prelude::{FromDer, X509Certificate};
    let der = rustls_pemfile::certs(&mut std::io::Cursor::new(cert_pem.as_bytes()))
        .next()
        .ok_or_else(|| anyhow::anyhow!("no certificate in signed PEM"))?
        .context("parsing signed certificate PEM")?;
    let (_, cert) = X509Certificate::from_der(der.as_ref())
        .map_err(|e| anyhow::anyhow!("parsing signed certificate: {e}"))?;
    Ok(cert.validity().not_after.timestamp() as u64)
}

/// Build and commit the `Signed` record for an issuance — the single
/// source of "this CA issued serial `S` as `name`, here is its cert and
/// id-map groups." Shared by the daemon's issuance path and offline
/// bootstrap issuance so every issued cert lands in the one index.
pub fn commit_issuance(
    ca_dir: &Path,
    req: &QueuedReq,
    serial: u64,
    name: &str,
    cert_pem: &str,
    groups: &[String],
) -> Result<()> {
    let now = now_unix();
    let spki_fp = crate::conf_client::csr_fingerprint(&req.csr_pem)
        .map(|f| f.text())
        .unwrap_or_default();
    let record = IssuedRecord {
        req: req.clone(),
        serial,
        name: name.to_string(),
        spki_fp,
        cert_pem: cert_pem.to_string(),
        groups: groups.to_vec(),
        not_after_unix: cert_not_after_unix(cert_pem)?,
        issued_unix: now,
        warnings: Vec::new(),
        revoked: None,
        push_done: groups.is_empty(),
    };
    commit_signed(ca_dir, &record)
}

/// Remove expired `queue/`/`denied/` entries (and any `queue/` entry
/// already shadowed by a terminal record). Never touches `issued/`.
pub fn prune(ca_dir: &Path) -> Result<()> {
    let now = now_unix();
    if let Ok(entries) = std::fs::read_dir(queue_dir(ca_dir)) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(id) = path.file_stem().and_then(|s| s.to_str()) else { continue };
            if !valid_id(id) {
                continue;
            }
            let shadowed =
                issued_path(ca_dir, id).exists() || denied_path(ca_dir, id).exists();
            let expired = match std::fs::read(&path) {
                Ok(b) => match serde_json::from_slice::<QueuedReq>(&b) {
                    Ok(req) => now.saturating_sub(req.received_unix) > TTL.as_secs(),
                    Err(_) => true,
                },
                Err(_) => true,
            };
            if shadowed || expired {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    if let Ok(entries) = std::fs::read_dir(denied_dir(ca_dir)) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(id) = path.file_stem().and_then(|s| s.to_str()) else { continue };
            if !valid_id(id) {
                continue;
            }
            let expired = match std::fs::read(&path) {
                Ok(b) => match serde_json::from_slice::<DeniedRecord>(&b) {
                    Ok(rec) => now.saturating_sub(rec.req.received_unix) > TTL.as_secs(),
                    Err(_) => true,
                },
                Err(_) => true,
            };
            if expired {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(name: &str) -> QueuedReq {
        QueuedReq::new(
            NodeKind::Workstation,
            "CSR".to_string(),
            name.to_string(),
            30,
            "10.0.0.7:51000".to_string(),
            None,
            None,
        )
    }

    fn issued(req: QueuedReq, serial: u64, name: &str, not_after: u64) -> IssuedRecord {
        IssuedRecord {
            req,
            serial,
            name: name.to_string(),
            spki_fp: format!("fp{serial}"),
            cert_pem: format!("CERT{serial}"),
            groups: vec![],
            not_after_unix: not_after,
            issued_unix: now_unix(),
            warnings: vec![],
            revoked: None,
            push_done: false,
        }
    }

    #[test]
    fn ids_are_validated() {
        assert!(valid_id(&new_id()));
        assert_ne!(new_id(), new_id());
        assert!(!valid_id("../../etc/passwd"));
        assert!(!valid_id(""));
        assert!(!valid_id("ABCDEF00112233445566778899aabbcc"));
        assert!(!valid_id("0123456789abcdef"));
    }

    #[test]
    fn enqueue_approve_moves_out_of_queue() {
        let dir = tempfile::tempdir().unwrap();
        // Need a CA cert for the trust bundle in the Signed outcome.
        std::fs::write(dir.path().join("certificate.pem"), b"CA-CERT").unwrap();
        let r = req("alice.example.com");
        enqueue(dir.path(), &r).unwrap();
        assert_eq!(pending(dir.path()).unwrap().len(), 1);
        assert!(matches!(status(dir.path(), &r.id).unwrap(), Status::Pending(_)));

        commit_signed(dir.path(), &issued(r.clone(), 5, "alice.example.com", now_unix() + 1000))
            .unwrap();
        // Moved out of the active queue, status now Signed with the cert + bundle.
        assert!(pending(dir.path()).unwrap().is_empty());
        match status(dir.path(), &r.id).unwrap() {
            Status::Signed(o) => {
                assert_eq!(o.signed_cert_pem, "CERT5");
                assert_eq!(o.trusted_pem, "CA-CERT");
            }
            _ => panic!("expected Signed"),
        }
        assert!(!queue_path(dir.path(), &r.id).exists());
        assert!(issued_path(dir.path(), &r.id).exists());
    }

    #[test]
    fn deny_moves_out_and_status_is_denied() {
        let dir = tempfile::tempdir().unwrap();
        let r = req("bob.example.com");
        enqueue(dir.path(), &r).unwrap();
        deny(dir.path(), &r, "ask your manager").unwrap();
        assert!(pending(dir.path()).unwrap().is_empty());
        match status(dir.path(), &r.id).unwrap() {
            Status::Denied(d) => assert_eq!(d.reason, "ask your manager"),
            _ => panic!("expected Denied"),
        }
        assert!(!queue_path(dir.path(), &r.id).exists());
    }

    #[test]
    fn one_live_and_revoke_and_crl() {
        let dir = tempfile::tempdir().unwrap();
        let now = now_unix();
        let a = req("eric.ryu-oh.org");
        commit_signed(dir.path(), &issued(a.clone(), 2, "eric.ryu-oh.org", now + 1000))
            .unwrap();
        let b = req("bob.ryu-oh.org");
        commit_signed(dir.path(), &issued(b, 3, "bob.ryu-oh.org", now + 1000)).unwrap();
        let old = req("old.ryu-oh.org");
        commit_signed(dir.path(), &issued(old, 4, "old.ryu-oh.org", now.saturating_sub(10)))
            .unwrap();

        assert_eq!(live_for_name(dir.path(), "ERIC.RYU-OH.ORG").unwrap().len(), 1);
        assert!(live_for_name(dir.path(), "old.ryu-oh.org").unwrap().is_empty());
        assert!(revoked_unexpired(dir.path()).unwrap().is_empty());

        assert!(revoke(dir.path(), 2, Revocation {
            serial: 2,
            revoked_unix: now,
            reason: "laptop stolen".into()
        })
        .unwrap());
        assert!(live_for_name(dir.path(), "eric.ryu-oh.org").unwrap().is_empty());
        let crl = revoked_unexpired(dir.path()).unwrap();
        assert_eq!(crl.len(), 1);
        assert_eq!(crl[0].serial, 2);
        // Revoking an unknown / already-revoked serial is a no-op false.
        assert!(!revoke(dir.path(), 2, Revocation { serial: 2, revoked_unix: now, reason: "x".into() }).unwrap());
        assert!(!revoke(dir.path(), 999, Revocation { serial: 999, revoked_unix: now, reason: "x".into() }).unwrap());

        assert_eq!(max_serial(dir.path()).unwrap(), Some(4));
    }

    #[test]
    fn push_done_recovery_set() {
        let dir = tempfile::tempdir().unwrap();
        let now = now_unix();
        let r = req("u.example.com");
        let mut rec = issued(r.clone(), 7, "u.example.com", now + 1000);
        rec.groups = vec!["users".into()];
        commit_signed(dir.path(), &rec).unwrap();
        // Has groups, not pushed → in the recovery set.
        assert_eq!(pending_pushes(dir.path()).unwrap().len(), 1);
        set_push_done(dir.path(), &r.id).unwrap();
        assert!(pending_pushes(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn prune_keeps_issued_clears_old_queue_and_denied() {
        let dir = tempfile::tempdir().unwrap();
        let old = now_unix() - TTL.as_secs() - 10;
        let mut stale = req("stale.example.com");
        stale.received_unix = old;
        std::fs::create_dir_all(queue_dir(dir.path())).unwrap();
        atomic::write_atomic(
            &queue_path(dir.path(), &stale.id),
            &serde_json::to_vec_pretty(&stale).unwrap(),
            0o644,
        )
        .unwrap();
        // A live issued record survives prune.
        let live = req("live.example.com");
        commit_signed(dir.path(), &issued(live.clone(), 9, "live.example.com", now_unix() + 1000))
            .unwrap();
        prune(dir.path()).unwrap();
        assert!(!queue_path(dir.path(), &stale.id).exists());
        assert!(issued_path(dir.path(), &live.id).exists());
    }

    #[test]
    fn queue_cap_enforced() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..MAX_PENDING {
            enqueue(dir.path(), &req(&format!("n{i}.example.com"))).unwrap();
        }
        let err = enqueue(dir.path(), &req("overflow.example.com")).unwrap_err();
        assert!(format!("{err:#}").contains("full"));
    }
}
