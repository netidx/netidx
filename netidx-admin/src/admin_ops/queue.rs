//! The enrollment queue as query + action.
//!
//! An enrollee (with no admin present) queues a signing request and polls; an
//! enrollment admin lists the pending requests, matches the out-of-band code —
//! the CSR public-key fingerprint the enrollee's terminal showed — and approves
//! (choosing id-map groups) or denies. The old one-at-a-time ceremony becomes
//! [`list_queue`] (query) + [`approve`]/[`deny`] (actions keyed by the request
//! code, recomputed locally from the CSR, never trusted from the wire) +
//! [`approve_renewals`] (a code-free batch, below).
//!
//! **Verified renewals are code-free.** A `verified_renewal` arrived on a
//! connection authenticated by a live certificate for the same name — a
//! cryptographic proof of possession — so there is nothing to match out of
//! band. [`approve_renewals`] approves them all with empty id-map groups; it
//! never selects by code. Server enrollments (`enroll_listen.is_some()`) always
//! force empty id-map groups: an admin server is not a user.

use super::{find_by_code, open_admin_session};
use crate::{
    admin_client::{self, csr_fingerprint},
    admin_proto::{NodeKind, QueueEntry, Secret},
    answer::Answerer,
    fingerprint::Fingerprint,
    plan::enroll::{default_id_map_groups, prompt_id_map_groups},
};
use anyhow::Result;
use std::{net::SocketAddr, path::PathBuf, time::Duration};

/// A pending enrollment-queue entry, keyed by its **code** — the CSR public-key
/// fingerprint recomputed locally from the CSR (never trusted from the wire). A
/// CSR that doesn't parse yields `code: None`; it can only be denied. `id` is
/// the opaque server request id an action reuses — private, so callers address
/// a request only by its code.
pub struct QueueItem {
    /// The request code an admin matches out of band (the CSR SPKI
    /// fingerprint). `None` when the queued CSR doesn't parse.
    pub code: Option<Fingerprint>,
    /// The kind of node enrolling (informational; also picks the id-map
    /// default).
    pub kind: NodeKind,
    /// The identity name the certificate is requested for.
    pub requested_name: String,
    /// The validity the enrollee asked for (capped by the admin's policy).
    pub requested_validity: Duration,
    /// Seconds since the request was queued.
    pub age_secs: u64,
    /// The address the request arrived from.
    pub peer: String,
    /// A cryptographically-verified renewal (proof of possession) — no code to
    /// match; batch-approvable via [`approve_renewals`].
    pub verified_renewal: bool,
    /// `Some` ⇒ a admin-server enrollment (approval signs the reserved serving
    /// name and registers a peer); id-map groups are forced empty.
    pub enroll_listen: Option<SocketAddr>,
    pub requested_roles: Vec<crate::admin_proto::Role>,
    pub resolver_members: Vec<crate::admin_proto::ResolverAddr>,
    pub cluster: Option<crate::admin_proto::ClusterPlacement>,
    pub cluster_base: Option<String>,
    id: String,
}

fn to_item(e: QueueEntry) -> QueueItem {
    let enrollment = e.enrollment;
    QueueItem {
        code: csr_fingerprint(&e.csr_pem).ok(),
        kind: e.kind,
        requested_name: e.requested_name,
        requested_validity: e.requested_validity,
        age_secs: e.age_secs,
        peer: e.peer,
        verified_renewal: e.verified_renewal,
        enroll_listen: enrollment.as_ref().map(|e| e.listen),
        requested_roles: enrollment
            .as_ref()
            .map(|e| e.roles.clone())
            .unwrap_or_default(),
        resolver_members: enrollment
            .as_ref()
            .map(|e| e.resolver_members.clone())
            .unwrap_or_default(),
        cluster: enrollment.map(|e| e.cluster),
        cluster_base: e.cluster_base,
        id: e.id,
    }
}

async fn fetch_queue(sess: &super::AdminSession) -> Result<Vec<QueueItem>> {
    Ok(admin_client::list_queue(
        sess.server,
        sess.credential.clone(),
        &sess.identity,
    )
    .await?
    .into_iter()
    .map(to_item)
    .collect())
}

/// The `ca queue` query: the admin server's pending enrollment queue, each row
/// keyed by its code. Read-only, admin-authenticated.
pub async fn list_queue(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
) -> Result<Vec<QueueItem>> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    fetch_queue(&sess).await
}

/// The outcome of approving one request.
pub struct ApproveOutcome {
    /// The identity name that was signed.
    pub requested_name: String,
    /// `Some` ⇒ the approved request was a admin-server enrollment.
    pub enroll_listen: Option<SocketAddr>,
    /// The id-map groups the new identity was registered with (empty for a
    /// server enrollment or `--no-id-map`).
    pub id_map_groups: Vec<String>,
    /// Cluster-push warnings returned by the server.
    pub warnings: Vec<String>,
}

/// The `ca approve <code>` action: re-list the queue, select the one request
/// whose recomputed code matches ([`find_by_code`] refuses no-match /
/// ambiguity), and approve it. id-map groups come from `provided_groups`
/// (or, when empty and not `no_id_map`, the answerer's default for the kind).
/// A server enrollment or `no_id_map` forces empty groups.
pub async fn approve(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    code: &str,
    provided_groups: &[String],
    no_id_map: bool,
) -> Result<ApproveOutcome> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let items = fetch_queue(&sess).await?;
    let item = find_by_code(&items, code, |i| i.code)?;
    let (id, requested_name, enroll_listen, kind, verified_renewal) = (
        item.id.clone(),
        item.requested_name.clone(),
        item.enroll_listen,
        item.kind,
        item.verified_renewal,
    );
    // id-map groups are assigned only for a *new*, human identity. A server
    // enrollment isn't a user; a verified renewal re-issues an existing
    // identity (matching one here by code must not silently rewrite its
    // groups — that's what `--renewals` deliberately leaves untouched);
    // `--no-id-map` is the explicit opt-out.
    let groups = if enroll_listen.is_some() || verified_renewal || no_id_map {
        Vec::new()
    } else {
        prompt_id_map_groups(ans, provided_groups, default_id_map_groups(kind)).await?
    };
    let warnings = admin_client::approve(
        sess.server,
        sess.credential.clone(),
        &id,
        groups.clone(),
        &sess.identity,
    )
    .await?;
    Ok(ApproveOutcome { requested_name, enroll_listen, id_map_groups: groups, warnings })
}

/// The per-renewal result of an [`approve_renewals`] batch.
pub struct RenewalResult {
    /// The identity name of the renewal.
    pub requested_name: String,
    /// The failure, if this one couldn't be approved (the batch continues).
    pub error: Option<String>,
}

/// The `ca approve --renewals` batch: approve every verified renewal with empty
/// id-map groups. **Code-free by construction** — a verified renewal is a
/// cryptographic proof of possession of the live key for the same name, so
/// there is nothing to match out of band (this is what the in-process
/// autorenew slot approves). Returns one result per renewal; a failure records
/// its error and the batch continues.
pub async fn approve_renewals(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
) -> Result<Vec<RenewalResult>> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let items = fetch_queue(&sess).await?;
    let mut out = Vec::new();
    for item in items.iter().filter(|i| i.verified_renewal) {
        let r = admin_client::approve(
            sess.server,
            sess.credential.clone(),
            &item.id,
            Vec::new(),
            &sess.identity,
        )
        .await;
        out.push(RenewalResult {
            requested_name: item.requested_name.clone(),
            error: r.err().map(|e| format!("{e:#}")),
        });
    }
    Ok(out)
}

/// The `ca deny <code> --reason <text>` action: select the one request whose
/// recomputed code matches and deny it. Returns the denied identity name.
pub async fn deny(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    code: &str,
    reason: &str,
) -> Result<String> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let items = fetch_queue(&sess).await?;
    let item = find_by_code(&items, code, |i| i.code)?;
    let (id, requested_name) = (item.id.clone(), item.requested_name.clone());
    admin_client::deny(
        sess.server,
        sess.credential.clone(),
        &id,
        reason,
        &sess.identity,
    )
    .await?;
    Ok(requested_name)
}
