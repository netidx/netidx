//! The admin server's **delegation** request store — the resolver-hierarchy
//! analogue of [`ca_store`](crate::ca_store). One self-contained JSON
//! record per request, one atomic write per state transition. The daemon
//! is the sole owner of the CA dir, so an in-process mutex plus the
//! filesystem's single-file atomicity is all the mutual exclusion needed.
//!
//! Records live under `<ca-dir>/delegations/`, in three dirs that keep the
//! active queue away from history:
//!   - `queue/<id>.json`    — Pending delegation requests.
//!   - `approved/<id>.json` — Approved records (carry the parent cluster's
//!     address(es) for the child to poll). TTL-pruned with the queue.
//!   - `denied/<id>.json`   — Denied requests, with the reason.
//!
//! A terminal transition *moves* the record out of `queue/` (write the
//! terminal file — the atomic commit — then remove the `queue/` file).
//! [`status`] checks `approved/`→`denied/`→`queue/`, so a stale `queue/`
//! file left by a crash loses to the committed terminal record and is swept
//! by [`prune`]. Ids are `[0-9a-f]{32}` (validated on every lookup — they
//! become file names; no path traversal).

use crate::{
    atomic,
    ca_store::{TTL, new_id, now_unix, valid_id},
    admin_proto::ResolverAddr,
};
use anyhow::{Context, Result};
use serde_derive::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Cap on pending delegation requests — a bound on unauthenticated disk
/// writes. Delegations are rare, so this is small.
pub const MAX_PENDING: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingDelegation {
    pub id: String,
    /// The subtree the child proposes to own (e.g. `/eu`).
    pub proposed_path: String,
    /// The child resolver cluster's advertised address(es).
    pub child: Vec<ResolverAddr>,
    pub received_unix: u64,
    /// Socket address the request arrived from (display context).
    pub peer: String,
}

impl PendingDelegation {
    pub fn new(proposed_path: String, child: Vec<ResolverAddr>, peer: String) -> Self {
        PendingDelegation {
            id: new_id(),
            proposed_path,
            child,
            received_unix: now_unix(),
            peer,
        }
    }

    pub fn age_secs(&self) -> u64 {
        now_unix().saturating_sub(self.received_unix)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApprovedRecord {
    pub req: PendingDelegation,
    /// The parent resolver cluster's address(es) — what the child writes
    /// into its `parent` referral.
    pub parent: Vec<ResolverAddr>,
    pub approved_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeniedRecord {
    pub req: PendingDelegation,
    pub reason: String,
}

/// The state of a delegation request — terminal records win over a stale
/// queue entry.
#[derive(Debug, Clone)]
pub enum Status {
    Pending(PendingDelegation),
    Approved {
        parent: Vec<ResolverAddr>,
    },
    Denied {
        reason: String,
    },
    /// Never seen, expired, or already cleaned up.
    Unknown,
}

fn root(ca_dir: &Path) -> PathBuf {
    ca_dir.join("delegations")
}
fn queue_dir(ca_dir: &Path) -> PathBuf {
    root(ca_dir).join("queue")
}
fn approved_dir(ca_dir: &Path) -> PathBuf {
    root(ca_dir).join("approved")
}
fn denied_dir(ca_dir: &Path) -> PathBuf {
    root(ca_dir).join("denied")
}
fn queue_path(ca_dir: &Path, id: &str) -> PathBuf {
    queue_dir(ca_dir).join(format!("{id}.json"))
}
fn approved_path(ca_dir: &Path, id: &str) -> PathBuf {
    approved_dir(ca_dir).join(format!("{id}.json"))
}
fn denied_path(ca_dir: &Path, id: &str) -> PathBuf {
    denied_dir(ca_dir).join(format!("{id}.json"))
}

/// Add a delegation request to the active queue. Prunes first; refuses at
/// [`MAX_PENDING`].
pub fn enqueue(ca_dir: &Path, req: &PendingDelegation) -> Result<()> {
    anyhow::ensure!(valid_id(&req.id), "malformed request id");
    let dir = queue_dir(ca_dir);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    prune(ca_dir)?;
    anyhow::ensure!(
        pending(ca_dir)?.len() < MAX_PENDING,
        "the delegation queue is full ({MAX_PENDING} pending requests)"
    );
    let bytes =
        serde_json::to_vec_pretty(req).context("serializing delegation request")?;
    atomic::write_atomic(&queue_path(ca_dir, &req.id), &bytes, 0o644)
}

/// Every pending delegation (active, not expired, not already terminal),
/// oldest first.
pub fn pending(ca_dir: &Path) -> Result<Vec<PendingDelegation>> {
    let dir = queue_dir(ca_dir);
    let entries = match std::fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("listing {}", dir.display())),
    };
    let now = now_unix();
    let mut out: Vec<PendingDelegation> = Vec::new();
    for entry in entries {
        let path = entry?.path();
        let Some(id) = path.file_stem().and_then(|s| s.to_str()) else { continue };
        if !valid_id(id) {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else { continue };
        let Ok(req) = serde_json::from_slice::<PendingDelegation>(&bytes) else {
            continue;
        };
        let expired = now.saturating_sub(req.received_unix) > TTL.as_secs();
        let terminal =
            approved_path(ca_dir, id).exists() || denied_path(ca_dir, id).exists();
        if !expired && !terminal {
            out.push(req);
        }
    }
    out.sort_by_key(|r| r.received_unix);
    Ok(out)
}

/// The state of delegation request `id` — terminal records win over a
/// stale queue entry.
pub fn status(ca_dir: &Path, id: &str) -> Result<Status> {
    if !valid_id(id) {
        return Ok(Status::Unknown);
    }
    if let Ok(bytes) = std::fs::read(approved_path(ca_dir, id)) {
        let rec: ApprovedRecord =
            serde_json::from_slice(&bytes).context("parsing approved delegation")?;
        return Ok(Status::Approved { parent: rec.parent });
    }
    if let Ok(bytes) = std::fs::read(denied_path(ca_dir, id)) {
        let rec: DeniedRecord =
            serde_json::from_slice(&bytes).context("parsing denied delegation")?;
        return Ok(Status::Denied { reason: rec.reason });
    }
    match std::fs::read(queue_path(ca_dir, id)) {
        Ok(bytes) => {
            let req: PendingDelegation =
                serde_json::from_slice(&bytes).context("parsing delegation request")?;
            if now_unix().saturating_sub(req.received_unix) > TTL.as_secs() {
                Ok(Status::Unknown)
            } else {
                Ok(Status::Pending(req))
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Status::Unknown),
        Err(e) => Err(e).context("reading delegation request"),
    }
}

/// Read a pending request by id (for the under-lock re-check at approve).
/// `None` if it isn't currently Pending.
pub fn read_pending(ca_dir: &Path, id: &str) -> Result<Option<PendingDelegation>> {
    match status(ca_dir, id)? {
        Status::Pending(req) => Ok(Some(req)),
        _ => Ok(None),
    }
}

/// Read the approved record by id — the original request + the recorded
/// parent address(es). Used to idempotently re-sync an already-approved
/// delegation (re-apply the child edit + re-push to cluster peers).
pub fn read_approved(ca_dir: &Path, id: &str) -> Result<Option<ApprovedRecord>> {
    if !valid_id(id) {
        return Ok(None);
    }
    match std::fs::read(approved_path(ca_dir, id)) {
        Ok(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).context("parsing approved delegation")?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context("reading approved delegation"),
    }
}

/// Commit an approval: write the `approved/` record (the atomic commit),
/// then remove the `queue/` entry. The single write is the transaction.
pub fn approve(
    ca_dir: &Path,
    req: &PendingDelegation,
    parent: Vec<ResolverAddr>,
) -> Result<()> {
    anyhow::ensure!(valid_id(&req.id), "malformed request id");
    let dir = approved_dir(ca_dir);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    let rec = ApprovedRecord { req: req.clone(), parent, approved_unix: now_unix() };
    let bytes =
        serde_json::to_vec_pretty(&rec).context("serializing approved delegation")?;
    atomic::write_atomic(&approved_path(ca_dir, &req.id), &bytes, 0o644)?;
    let _ = std::fs::remove_file(queue_path(ca_dir, &req.id));
    Ok(())
}

/// Commit a denial: write the `denied/` record, then remove the queue
/// entry.
pub fn deny(ca_dir: &Path, req: &PendingDelegation, reason: &str) -> Result<()> {
    anyhow::ensure!(valid_id(&req.id), "malformed request id");
    let dir = denied_dir(ca_dir);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    let rec = DeniedRecord { req: req.clone(), reason: reason.to_string() };
    let bytes =
        serde_json::to_vec_pretty(&rec).context("serializing denied delegation")?;
    atomic::write_atomic(&denied_path(ca_dir, &req.id), &bytes, 0o644)?;
    let _ = std::fs::remove_file(queue_path(ca_dir, &req.id));
    Ok(())
}

/// Drop expired/terminal-shadowed queue entries and expired denied/approved
/// records. Best-effort; called on every enqueue.
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
                approved_path(ca_dir, id).exists() || denied_path(ca_dir, id).exists();
            let expired = match std::fs::read(&path) {
                Ok(b) => match serde_json::from_slice::<PendingDelegation>(&b) {
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
    for dir in [denied_dir(ca_dir), approved_dir(ca_dir)] {
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(id) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                if !valid_id(id) {
                    continue;
                }
                // Expiry keyed on the original request's receipt time.
                let received = std::fs::read(&path).ok().and_then(|b| {
                    serde_json::from_slice::<DeniedRecord>(&b)
                        .map(|r| r.req.received_unix)
                        .or_else(|_| {
                            serde_json::from_slice::<ApprovedRecord>(&b)
                                .map(|r| r.req.received_unix)
                        })
                        .ok()
                });
                let expired = match received {
                    Some(t) => now.saturating_sub(t) > TTL.as_secs(),
                    None => true,
                };
                if expired {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_proto::InfoAuth;

    fn ra(s: &str) -> ResolverAddr {
        ResolverAddr { addr: s.parse().unwrap(), auth: InfoAuth::Anonymous }
    }

    #[test]
    fn enqueue_pending_approve_status() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path();
        let req = PendingDelegation::new(
            "/eu".into(),
            vec![ra("10.0.0.2:4564")],
            "10.0.0.2:5000".into(),
        );
        enqueue(ca, &req).unwrap();
        assert_eq!(pending(ca).unwrap().len(), 1);
        assert!(matches!(status(ca, &req.id).unwrap(), Status::Pending(_)));
        assert!(read_pending(ca, &req.id).unwrap().is_some());

        approve(ca, &req, vec![ra("10.0.0.1:4564")]).unwrap();
        // Terminal wins; queue entry removed.
        assert!(pending(ca).unwrap().is_empty());
        match status(ca, &req.id).unwrap() {
            Status::Approved { parent } => assert_eq!(parent, vec![ra("10.0.0.1:4564")]),
            s => panic!("expected Approved, got {s:?}"),
        }
        assert!(read_pending(ca, &req.id).unwrap().is_none());
    }

    #[test]
    fn deny_is_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path();
        let req =
            PendingDelegation::new("/asia".into(), vec![ra("10.0.0.3:4564")], "p".into());
        enqueue(ca, &req).unwrap();
        deny(ca, &req, "not authorized").unwrap();
        assert!(pending(ca).unwrap().is_empty());
        match status(ca, &req.id).unwrap() {
            Status::Denied { reason } => assert_eq!(reason, "not authorized"),
            s => panic!("expected Denied, got {s:?}"),
        }
    }

    #[test]
    fn unknown_id() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(status(dir.path(), "deadbeef").unwrap(), Status::Unknown));
        assert!(matches!(
            status(dir.path(), "00000000000000000000000000000000").unwrap(),
            Status::Unknown
        ));
    }
}
