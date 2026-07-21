//! The admin server's **delegation** request store — the resolver-hierarchy
//! analogue of [`ca_store`](crate::ca_store). One self-contained JSON
//! record per request, one atomic write per state transition. The daemon
//! is the sole owner of the CA dir, and its state write guard serializes
//! mutations while single-file atomicity supplies crash consistency.
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
    admin_proto::{AdminServerId, ResolverAddr, ResolverClusterId},
    atomic,
    ca_store::{TTL, new_id, now_unix, valid_id},
    config_lock::ConfigDirLock,
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
    pub proposed_path: String,
    pub parent_servers: Vec<AdminServerId>,
    pub child_servers: Vec<AdminServerId>,
    /// Stable CA allocation used if approval splits one active cluster.
    pub proposed_child: ResolverClusterId,
    pub received_unix: u64,
    /// Socket address the request arrived from (display context).
    pub peer: String,
}

impl PendingDelegation {
    pub fn new(
        proposed_path: String,
        mut parent_servers: Vec<AdminServerId>,
        mut child_servers: Vec<AdminServerId>,
        peer: String,
    ) -> Self {
        parent_servers.sort();
        parent_servers.dedup();
        child_servers.sort();
        child_servers.dedup();
        PendingDelegation {
            id: new_id(),
            proposed_path,
            parent_servers,
            child_servers,
            proposed_child: ResolverClusterId::new(),
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
pub async fn enqueue(
    config_lock: &ConfigDirLock,
    ca_dir: &Path,
    req: &PendingDelegation,
) -> Result<()> {
    let ca_dir = config_lock.require_contained(ca_dir)?;
    anyhow::ensure!(valid_id(&req.id), "malformed request id");
    let dir = queue_dir(&ca_dir);
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("creating {}", dir.display()))?;
    prune(config_lock, &ca_dir).await?;
    anyhow::ensure!(
        pending(&ca_dir).await?.len() < MAX_PENDING,
        "the delegation queue is full ({MAX_PENDING} pending requests)"
    );
    let bytes =
        serde_json::to_vec_pretty(req).context("serializing delegation request")?;
    atomic::write_atomic_async(&queue_path(&ca_dir, &req.id), &bytes, 0o644).await
}

/// Every pending delegation (active, not expired, not already terminal),
/// oldest first.
pub async fn pending(ca_dir: &Path) -> Result<Vec<PendingDelegation>> {
    let dir = queue_dir(ca_dir);
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("listing {}", dir.display())),
    };
    let now = now_unix();
    let mut out: Vec<PendingDelegation> = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(id) = path.file_stem().and_then(|s| s.to_str()) else { continue };
        if !valid_id(id) {
            continue;
        }
        let Ok(bytes) = tokio::fs::read(&path).await else { continue };
        let Ok(req) = serde_json::from_slice::<PendingDelegation>(&bytes) else {
            continue;
        };
        let expired = now.saturating_sub(req.received_unix) > TTL.as_secs();
        let terminal = tokio::fs::try_exists(approved_path(ca_dir, id)).await?
            || tokio::fs::try_exists(denied_path(ca_dir, id)).await?;
        if !expired && !terminal {
            out.push(req);
        }
    }
    out.sort_by_key(|r| r.received_unix);
    Ok(out)
}

/// The state of delegation request `id` — terminal records win over a
/// stale queue entry.
pub async fn status(ca_dir: &Path, id: &str) -> Result<Status> {
    if !valid_id(id) {
        return Ok(Status::Unknown);
    }
    if let Ok(bytes) = tokio::fs::read(approved_path(ca_dir, id)).await {
        let rec: ApprovedRecord =
            serde_json::from_slice(&bytes).context("parsing approved delegation")?;
        return Ok(Status::Approved { parent: rec.parent });
    }
    if let Ok(bytes) = tokio::fs::read(denied_path(ca_dir, id)).await {
        let rec: DeniedRecord =
            serde_json::from_slice(&bytes).context("parsing denied delegation")?;
        return Ok(Status::Denied { reason: rec.reason });
    }
    match tokio::fs::read(queue_path(ca_dir, id)).await {
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
pub async fn read_pending(ca_dir: &Path, id: &str) -> Result<Option<PendingDelegation>> {
    match status(ca_dir, id).await? {
        Status::Pending(req) => Ok(Some(req)),
        _ => Ok(None),
    }
}

/// Read the approved record by id — the original request + the recorded
/// parent address(es). Used to idempotently re-sync an already-approved
/// delegation (re-apply the child edit + re-push to cluster peers).
pub async fn read_approved(ca_dir: &Path, id: &str) -> Result<Option<ApprovedRecord>> {
    if !valid_id(id) {
        return Ok(None);
    }
    match tokio::fs::read(approved_path(ca_dir, id)).await {
        Ok(bytes) => Ok(Some(
            serde_json::from_slice(&bytes).context("parsing approved delegation")?,
        )),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context("reading approved delegation"),
    }
}

/// Every unexpired approved delegation, oldest first. These stay visible to
/// administrator tooling for the lifetime of the record so approval can be
/// re-run as the explicit, idempotent reconciliation path.
pub async fn approved(ca_dir: &Path) -> Result<Vec<ApprovedRecord>> {
    let dir = approved_dir(ca_dir);
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("listing {}", dir.display())),
    };
    let now = now_unix();
    let mut out = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(id) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        if !valid_id(id) {
            continue;
        }
        let Ok(bytes) = tokio::fs::read(&path).await else {
            continue;
        };
        let Ok(rec) = serde_json::from_slice::<ApprovedRecord>(&bytes) else {
            continue;
        };
        if now.saturating_sub(rec.req.received_unix) <= TTL.as_secs() {
            out.push(rec);
        }
    }
    out.sort_by_key(|r| r.req.received_unix);
    Ok(out)
}

/// Commit an approval: write the `approved/` record (the atomic commit),
/// then remove the `queue/` entry. The single write is the transaction.
pub async fn approve(
    config_lock: &ConfigDirLock,
    ca_dir: &Path,
    req: &PendingDelegation,
    parent: Vec<ResolverAddr>,
) -> Result<()> {
    let ca_dir = config_lock.require_contained(ca_dir)?;
    anyhow::ensure!(valid_id(&req.id), "malformed request id");
    let dir = approved_dir(&ca_dir);
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("creating {}", dir.display()))?;
    let rec = ApprovedRecord { req: req.clone(), parent, approved_unix: now_unix() };
    let bytes =
        serde_json::to_vec_pretty(&rec).context("serializing approved delegation")?;
    atomic::write_atomic_async(&approved_path(&ca_dir, &req.id), &bytes, 0o644).await?;
    let _ = tokio::fs::remove_file(queue_path(&ca_dir, &req.id)).await;
    Ok(())
}

/// Commit a denial: write the `denied/` record, then remove the queue
/// entry.
pub async fn deny(
    config_lock: &ConfigDirLock,
    ca_dir: &Path,
    req: &PendingDelegation,
    reason: &str,
) -> Result<()> {
    let ca_dir = config_lock.require_contained(ca_dir)?;
    anyhow::ensure!(valid_id(&req.id), "malformed request id");
    let dir = denied_dir(&ca_dir);
    tokio::fs::create_dir_all(&dir)
        .await
        .with_context(|| format!("creating {}", dir.display()))?;
    let rec = DeniedRecord { req: req.clone(), reason: reason.to_string() };
    let bytes =
        serde_json::to_vec_pretty(&rec).context("serializing denied delegation")?;
    atomic::write_atomic_async(&denied_path(&ca_dir, &req.id), &bytes, 0o644).await?;
    let _ = tokio::fs::remove_file(queue_path(&ca_dir, &req.id)).await;
    Ok(())
}

/// Drop expired/terminal-shadowed queue entries and expired denied/approved
/// records. Best-effort; called on every enqueue.
pub async fn prune(config_lock: &ConfigDirLock, ca_dir: &Path) -> Result<()> {
    let ca_dir = config_lock.require_contained(ca_dir)?;
    let now = now_unix();
    if let Ok(mut entries) = tokio::fs::read_dir(queue_dir(&ca_dir)).await {
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let Some(id) = path.file_stem().and_then(|s| s.to_str()) else { continue };
            if !valid_id(id) {
                continue;
            }
            let shadowed = tokio::fs::try_exists(approved_path(&ca_dir, id)).await?
                || tokio::fs::try_exists(denied_path(&ca_dir, id)).await?;
            let expired = match tokio::fs::read(&path).await {
                Ok(b) => match serde_json::from_slice::<PendingDelegation>(&b) {
                    Ok(req) => now.saturating_sub(req.received_unix) > TTL.as_secs(),
                    Err(_) => true,
                },
                Err(_) => true,
            };
            if shadowed || expired {
                let _ = tokio::fs::remove_file(&path).await;
            }
        }
    }
    for dir in [denied_dir(&ca_dir), approved_dir(&ca_dir)] {
        if let Ok(mut entries) = tokio::fs::read_dir(&dir).await {
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let Some(id) = path.file_stem().and_then(|s| s.to_str()) else {
                    continue;
                };
                if !valid_id(id) {
                    continue;
                }
                // Expiry keyed on the original request's receipt time.
                let received = tokio::fs::read(&path).await.ok().and_then(|b| {
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
                    let _ = tokio::fs::remove_file(&path).await;
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

    #[tokio::test]
    async fn enqueue_pending_approve_status() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path();
        let lock = ConfigDirLock::acquire(ca).unwrap();
        let req = PendingDelegation::new(
            "/ap".into(),
            vec![AdminServerId::new()],
            vec![AdminServerId::new()],
            "10.0.0.2:5000".into(),
        );
        enqueue(&lock, ca, &req).await.unwrap();
        assert_eq!(pending(ca).await.unwrap().len(), 1);
        assert!(matches!(status(ca, &req.id).await.unwrap(), Status::Pending(_)));
        assert!(read_pending(ca, &req.id).await.unwrap().is_some());

        approve(&lock, ca, &req, vec![ra("10.0.0.1:4564")]).await.unwrap();
        // Terminal wins; queue entry removed.
        assert!(pending(ca).await.unwrap().is_empty());
        match status(ca, &req.id).await.unwrap() {
            Status::Approved { parent } => assert_eq!(parent, vec![ra("10.0.0.1:4564")]),
            s => panic!("expected Approved, got {s:?}"),
        }
        assert!(read_pending(ca, &req.id).await.unwrap().is_none());
        let reviewable = approved(ca).await.unwrap();
        assert_eq!(reviewable.len(), 1);
        assert_eq!(reviewable[0].req.id, req.id);
    }

    #[tokio::test]
    async fn deny_is_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path();
        let lock = ConfigDirLock::acquire(ca).unwrap();
        let req = PendingDelegation::new(
            "/ap".into(),
            vec![AdminServerId::new()],
            vec![AdminServerId::new()],
            "p".into(),
        );
        enqueue(&lock, ca, &req).await.unwrap();
        deny(&lock, ca, &req, "not authorized").await.unwrap();
        assert!(pending(ca).await.unwrap().is_empty());
        match status(ca, &req.id).await.unwrap() {
            Status::Denied { reason } => assert_eq!(reason, "not authorized"),
            s => panic!("expected Denied, got {s:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_id() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(status(dir.path(), "deadbeef").await.unwrap(), Status::Unknown));
        assert!(matches!(
            status(dir.path(), "00000000000000000000000000000000").await.unwrap(),
            Status::Unknown
        ));
    }
}
