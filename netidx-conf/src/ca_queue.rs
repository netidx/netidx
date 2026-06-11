//! File-backed queue of pending signing requests (`<ca-dir>/queue/`).
//!
//! Each queued request is one JSON file, `<id>.req.json`; its outcome
//! is a sidecar — `<id>.signed.json` or `<id>.denied.json` — written
//! at approval/denial and read back by the daemon to answer
//! [`Poll`](crate::conf_proto::Request::Poll). Files survive daemon
//! restarts, are atomically written, and contain no secrets (a CSR is
//! public by construction).
//!
//! Request ids are random hex and validated on every lookup — an id is
//! a wire-supplied string that becomes part of a file name, so anything
//! but `[0-9a-f]{32}` is rejected outright (no path traversal).

use crate::{atomic, conf_proto::NodeKind};
use anyhow::{Context, Result};
use serde_derive::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Queued requests older than this are pruned (and their sidecars with
/// them). Generous — the admin may be at lunch — but bounded: a stale
/// request whose enrollee gave up shouldn't sit in the queue for days
/// inviting an absent-minded approval.
pub const TTL: Duration = Duration::from_secs(24 * 3600);

/// Cap on pending requests — a bound on unauthenticated disk writes,
/// far above any real enrollment burst.
pub const MAX_PENDING: usize = 64;

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
    /// The enqueue connection was authenticated by a live certificate
    /// for exactly `requested_name` — a proof-of-possession renewal.
    /// Approval skips the SAN-scope and one-live-cert checks (the name
    /// was admin-approved at enrollment; this is continuation) and
    /// never touches the id-map.
    #[serde(default)]
    pub verified_renewal: bool,
    /// `Some` ⇒ a conf-server enrollment: approval signs the reserved
    /// serving SAN, requires the approving admin's
    /// `may_enroll_servers`, and records this address as a peer.
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
        verified_renewal: bool,
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
            verified_renewal,
            enroll_listen,
        }
    }

    /// Seconds since this request was queued.
    pub fn age_secs(&self) -> u64 {
        now_unix().saturating_sub(self.received_unix)
    }
}

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

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
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

fn req_path(ca_dir: &Path, id: &str) -> PathBuf {
    queue_dir(ca_dir).join(format!("{id}.req.json"))
}

fn signed_path(ca_dir: &Path, id: &str) -> PathBuf {
    queue_dir(ca_dir).join(format!("{id}.signed.json"))
}

fn denied_path(ca_dir: &Path, id: &str) -> PathBuf {
    queue_dir(ca_dir).join(format!("{id}.denied.json"))
}

/// Add a request to the queue. Prunes expired entries first; refuses
/// when the pending count is at [`MAX_PENDING`].
pub fn enqueue(ca_dir: &Path, req: &QueuedReq) -> Result<()> {
    anyhow::ensure!(valid_id(&req.id), "malformed request id");
    let dir = queue_dir(ca_dir);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    prune(ca_dir)?;
    let pending = pending(ca_dir)?;
    anyhow::ensure!(
        pending.len() < MAX_PENDING,
        "the signing queue is full ({MAX_PENDING} pending requests)"
    );
    let bytes = serde_json::to_vec_pretty(req).context("serializing queued request")?;
    atomic::write_atomic(&req_path(ca_dir, &req.id), &bytes, 0o644)
}

/// Every pending request (queued, not yet signed/denied, not expired),
/// oldest first.
pub fn pending(ca_dir: &Path) -> Result<Vec<QueuedReq>> {
    let dir = queue_dir(ca_dir);
    let mut out: Vec<QueuedReq> = Vec::new();
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => {
            return Err(e).with_context(|| format!("listing queue {}", dir.display()))
        }
    };
    let now = now_unix();
    for entry in entries {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else { continue };
        let Some(id) = name.strip_suffix(".req.json") else { continue };
        if !valid_id(id) {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else { continue };
        let Ok(req) = serde_json::from_slice::<QueuedReq>(&bytes) else { continue };
        let expired = now.saturating_sub(req.received_unix) > TTL.as_secs();
        let handled =
            signed_path(ca_dir, id).exists() || denied_path(ca_dir, id).exists();
        if !expired && !handled {
            out.push(req);
        }
    }
    out.sort_by_key(|r| r.received_unix);
    Ok(out)
}

/// The state of request `id`.
pub fn status(ca_dir: &Path, id: &str) -> Result<Status> {
    if !valid_id(id) {
        return Ok(Status::Unknown);
    }
    if let Ok(bytes) = std::fs::read(signed_path(ca_dir, id)) {
        return Ok(Status::Signed(
            serde_json::from_slice(&bytes).context("parsing signed outcome")?,
        ));
    }
    if let Ok(bytes) = std::fs::read(denied_path(ca_dir, id)) {
        return Ok(Status::Denied(
            serde_json::from_slice(&bytes).context("parsing denied outcome")?,
        ));
    }
    match std::fs::read(req_path(ca_dir, id)) {
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

/// Record an approval outcome. The enrollee picks it up via Poll; the
/// file is kept (idempotent re-polls) until TTL pruning clears it with
/// its request.
pub fn store_signed(ca_dir: &Path, id: &str, outcome: &SignedOutcome) -> Result<()> {
    anyhow::ensure!(valid_id(id), "malformed request id");
    let bytes = serde_json::to_vec_pretty(outcome).context("serializing signed outcome")?;
    atomic::write_atomic(&signed_path(ca_dir, id), &bytes, 0o644)
}

/// Record a denial outcome.
pub fn store_denied(ca_dir: &Path, id: &str, reason: &str) -> Result<()> {
    anyhow::ensure!(valid_id(id), "malformed request id");
    let outcome = DeniedOutcome { reason: reason.to_string() };
    let bytes = serde_json::to_vec_pretty(&outcome).context("serializing denied outcome")?;
    atomic::write_atomic(&denied_path(ca_dir, id), &bytes, 0o644)
}

/// Remove requests older than [`TTL`], with their sidecars.
pub fn prune(ca_dir: &Path) -> Result<()> {
    let dir = queue_dir(ca_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(e).with_context(|| format!("listing queue {}", dir.display()))
        }
    };
    let now = now_unix();
    for entry in entries {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else { continue };
        let Some(id) = name.strip_suffix(".req.json") else { continue };
        if !valid_id(id) {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else { continue };
        let Ok(req) = serde_json::from_slice::<QueuedReq>(&bytes) else { continue };
        if now.saturating_sub(req.received_unix) > TTL.as_secs() {
            let _ = std::fs::remove_file(signed_path(ca_dir, id));
            let _ = std::fs::remove_file(denied_path(ca_dir, id));
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(id: &str, name: &str, received_unix: u64) -> QueuedReq {
        QueuedReq {
            id: id.to_string(),
            kind: NodeKind::Workstation,
            csr_pem: "CSR".to_string(),
            requested_name: name.to_string(),
            requested_validity_days: 30,
            received_unix,
            peer: "10.0.0.7:51000".to_string(),
            verified_renewal: false,
            enroll_listen: None,
        }
    }

    #[test]
    fn ids_are_well_formed_and_validated() {
        let id = new_id();
        assert!(valid_id(&id), "generated id should validate: {id}");
        assert_ne!(new_id(), new_id());
        // Anything that could escape the queue dir is rejected.
        assert!(!valid_id("../../../etc/passwd"));
        assert!(!valid_id(""));
        assert!(!valid_id("ABCDEF00112233445566778899aabbcc")); // uppercase
        assert!(!valid_id("0123456789abcdef")); // too short
    }

    #[test]
    fn enqueue_pending_approve_deny_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let now = now_unix();
        let a = new_id();
        let b = new_id();
        enqueue(dir.path(), &req(&a, "alice.example.com", now)).unwrap();
        enqueue(dir.path(), &req(&b, "bob.example.com", now)).unwrap();
        let p = pending(dir.path()).unwrap();
        assert_eq!(p.len(), 2);
        // a gets signed, b denied; neither is pending afterwards but
        // both still answer their status (idempotent re-polls).
        store_signed(
            dir.path(),
            &a,
            &SignedOutcome {
                signed_cert_pem: "CERT".into(),
                trusted_pem: "CA".into(),
                warnings: vec![],
            },
        )
        .unwrap();
        store_denied(dir.path(), &b, "ask your manager").unwrap();
        assert!(pending(dir.path()).unwrap().is_empty());
        assert!(matches!(status(dir.path(), &a).unwrap(), Status::Signed(_)));
        match status(dir.path(), &b).unwrap() {
            Status::Denied(d) => assert_eq!(d.reason, "ask your manager"),
            _ => panic!("expected denied"),
        }
        assert!(matches!(status(dir.path(), &new_id()).unwrap(), Status::Unknown));
    }

    #[test]
    fn expired_requests_vanish_and_prune_removes_them() {
        let dir = tempfile::tempdir().unwrap();
        let old = now_unix() - TTL.as_secs() - 10;
        let id = new_id();
        enqueue(dir.path(), &req(&id, "stale.example.com", old)).unwrap();
        // Expired: not pending, status Unknown.
        assert!(pending(dir.path()).unwrap().is_empty());
        assert!(matches!(status(dir.path(), &id).unwrap(), Status::Unknown));
        prune(dir.path()).unwrap();
        assert!(!req_path(dir.path(), &id).exists());
    }

    #[test]
    fn the_queue_is_capped() {
        let dir = tempfile::tempdir().unwrap();
        let now = now_unix();
        for i in 0..MAX_PENDING {
            enqueue(dir.path(), &req(&new_id(), &format!("n{i}.example.com"), now))
                .unwrap();
        }
        let err = enqueue(dir.path(), &req(&new_id(), "overflow.example.com", now))
            .unwrap_err();
        assert!(format!("{err:#}").contains("full"));
    }
}
