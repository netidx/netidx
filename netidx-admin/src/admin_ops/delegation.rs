//! Resolver-hierarchy delegation as query + action.
//!
//! A child resolver requests delegation of a subtree from a parent's admin
//! server (glyph-confirming it first) and polls; the parent admin lists the
//! pending requests, matches the out-of-band code, and approves (which edits
//! the parent's `children` cluster-wide) or denies. The review ceremony becomes
//! `list_pending_delegations` (query) + `approve_delegation`/`deny_delegation`
//! (actions keyed by the request code — recomputed locally from
//! `(proposed_path, child)`, never trusted from the wire).

use super::{find_by_code, open_admin_session};
use crate::{
    admin_client, admin_server, admin_server_config,
    admin_proto::{InfoAuth, PeerResult, ReferralEdit, ResolverAddr},
    answer::Answerer,
    fingerprint::Fingerprint,
    paths,
    plan::delegation::delegate_under_parent,
    resolver::ResolverConfig,
    template::{self, ParentRef, ReferralAuth},
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// The referral auth a child records to reach a resolver the parent reported.
pub fn info_to_referral_auth(a: &InfoAuth) -> ReferralAuth {
    match a {
        InfoAuth::Anonymous => ReferralAuth::Anonymous,
        InfoAuth::Krb5 { spn } => ReferralAuth::Krb5(ArcStr::from(spn.as_str())),
        InfoAuth::Tls { name } => ReferralAuth::Tls(ArcStr::from(name.as_str())),
    }
}

/// A pending delegation request, keyed by its **code** (the delegation
/// fingerprint recomputed locally from `proposed_path` + `child`). `id` is the
/// opaque server request id an action reuses — private, so callers address a
/// request only by its code.
pub struct PendingDelegation {
    /// The request code an operator matches out of band (`code IS the id`).
    pub code: Fingerprint,
    /// The subtree the child asks to own.
    pub proposed_path: String,
    /// The child cluster's resolver address(es).
    pub child: Vec<ResolverAddr>,
    /// Seconds since the request was queued.
    pub age_secs: u64,
    /// The address the request arrived from.
    pub peer: String,
    id: String,
}

fn to_pending(e: crate::admin_proto::DelegationEntry) -> PendingDelegation {
    PendingDelegation {
        code: admin_client::delegation_code(&e.proposed_path, &e.child),
        proposed_path: e.proposed_path,
        child: e.child,
        age_secs: e.age_secs,
        peer: e.peer,
        id: e.id,
    }
}

/// The `resolver list-delegations` query: the parent's pending delegation
/// queue, each row keyed by its code. Read-only, admin-authenticated.
pub async fn list_pending_delegations(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<crate::admin_proto::Secret>,
) -> Result<Vec<PendingDelegation>> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let entries = admin_client::list_delegations(
        sess.server,
        &sess.admin,
        sess.password.as_str(),
        &sess.identity,
    )
    .await?;
    Ok(entries.into_iter().map(to_pending).collect())
}

/// The outcome of approving a delegation: the subtree, plus the per-peer
/// cluster-propagation results (an already-approved request re-applies and
/// re-pushes idempotently, so re-running converges a cluster whose peer was
/// unreachable).
pub struct DelegationDecision {
    /// The subtree that was delegated.
    pub proposed_path: String,
    /// Per-cluster-member propagation results.
    pub peers: Vec<PeerResult>,
}

/// The `resolver approve-delegation <code>` action. Re-lists the queue,
/// [`find_by_code`]s the one request whose recomputed code matches (refusing on
/// no-match / ambiguity), and approves it cluster-wide.
pub async fn approve_delegation(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<crate::admin_proto::Secret>,
    code: &str,
) -> Result<DelegationDecision> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let items: Vec<PendingDelegation> = admin_client::list_delegations(
        sess.server,
        &sess.admin,
        sess.password.as_str(),
        &sess.identity,
    )
    .await?
    .into_iter()
    .map(to_pending)
    .collect();
    let item = find_by_code(&items, code, |i| Some(i.code))?;
    let (id, proposed_path) = (item.id.clone(), item.proposed_path.clone());
    let peers = admin_client::approve_delegation(
        sess.server,
        &sess.admin,
        sess.password.as_str(),
        &id,
        &sess.identity,
    )
    .await?;
    Ok(DelegationDecision { proposed_path, peers })
}

/// The `resolver deny-delegation <code> --reason <text>` action.
pub async fn deny_delegation(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<crate::admin_proto::Secret>,
    code: &str,
    reason: &str,
) -> Result<PendingDelegation> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let items: Vec<PendingDelegation> = admin_client::list_delegations(
        sess.server,
        &sess.admin,
        sess.password.as_str(),
        &sess.identity,
    )
    .await?
    .into_iter()
    .map(to_pending)
    .collect();
    let item = find_by_code(&items, code, |i| Some(i.code))?;
    admin_client::deny_delegation(
        sess.server,
        &sess.admin,
        sess.password.as_str(),
        &item.id,
        reason,
        &sess.identity,
    )
    .await?;
    // Return the matched item (owned) for the caller to report.
    let PendingDelegation { code, proposed_path, child, age_secs, peer, id } = item;
    Ok(PendingDelegation {
        code: *code,
        proposed_path: proposed_path.clone(),
        child: child.clone(),
        age_secs: *age_secs,
        peer: peer.clone(),
        id: id.clone(),
    })
}

/// Whether the child's freshly-written parent referral reached the rest of its
/// cluster.
pub enum ClusterPropagation {
    /// Single-member child — nothing to propagate.
    SingleMember,
    /// Multi-member, but this host runs no admin server, so it couldn't
    /// authenticate as a cluster peer — the operator must copy the `parent`
    /// block into every other member by hand.
    NoAdminServer { members: usize },
    /// Pushed to the other members; per-peer results.
    Pushed(Vec<PeerResult>),
}

/// The outcome of `resolver add-parent`.
pub struct AddParentOutcome {
    /// The subtree this resolver now owns under the parent.
    pub proposed_path: String,
    /// Whether/how the referral propagated to the child's other cluster members.
    pub propagation: ClusterPropagation,
}

/// The `resolver add-parent` action: attach a standalone resolver under a
/// parent by delegation, then write its `parent` referral (and propagate it to
/// the child's other cluster members). The parent glyph confirm + queue + poll
/// runs through [`delegate_under_parent`] (already Answerer-driven).
pub async fn add_parent(
    ans: &mut dyn Answerer,
    resolver_config: &Path,
    parent_server: SocketAddr,
    proposed_path: &str,
) -> Result<AddParentOutcome> {
    let rcfg = ResolverConfig::load(resolver_config)?;
    if rcfg.as_file().parent.is_some() {
        bail!(
            "this resolver already has a parent referral — re-parenting isn't \
             supported (uninstall + reinstall to switch networks)."
        );
    }
    let child = rcfg.resolver_addrs();
    if child.is_empty() {
        bail!(
            "this resolver advertises no network address (Local-only?) — it cannot \
             be delegated a subtree."
        );
    }
    let n_members = child.len();
    // Queue the request, glyph-confirm the parent, and poll until approved.
    let parent =
        delegate_under_parent(ans, parent_server, proposed_path, child, None).await?;
    let parent_ref = ParentRef {
        path: ArcStr::from(proposed_path),
        ttl: None,
        addrs: parent.iter().map(|r| (r.addr, info_to_referral_auth(&r.auth))).collect(),
    };
    let rt = template::set_parent_referral(resolver_config, parent_ref)?;
    ans.note(&rt.describe());
    rt.apply().context("writing the parent referral")?;
    let propagation = if n_members > 1 {
        propagate_parent_to_child_cluster(&rcfg, proposed_path, &parent).await?
    } else {
        ClusterPropagation::SingleMember
    };
    Ok(AddParentOutcome { proposed_path: proposed_path.to_string(), propagation })
}

/// Push the child's freshly-written `parent` referral to every other member of
/// the child cluster (symmetric to the parent-side `AddChild` push). Needs this
/// host's admin-server serving cert to authenticate as a cluster peer; without
/// one, reports [`ClusterPropagation::NoAdminServer`] for the caller to warn on.
async fn propagate_parent_to_child_cluster(
    rcfg: &ResolverConfig,
    proposed_path: &str,
    parent: &[ResolverAddr],
) -> Result<ClusterPropagation> {
    let admin_path = match paths::discover_admin_server_config() {
        Ok(p) => p,
        Err(_) => {
            return Ok(ClusterPropagation::NoAdminServer {
                members: rcfg.as_file().member_servers.len(),
            });
        }
    };
    let cfg = admin_server_config::AdminServerConfig::load(&admin_path).context(
        "loading this host's admin-server config to propagate the parent referral",
    )?;
    let cert = std::fs::read(&cfg.serving_cert)
        .with_context(|| format!("reading serving cert {}", cfg.serving_cert.display()))?;
    let key = std::fs::read(&cfg.serving_key)
        .with_context(|| format!("reading serving key {}", cfg.serving_key.display()))?;
    let trusted = std::fs::read(&cfg.trusted)
        .with_context(|| format!("reading trust bundle {}", cfg.trusted.display()))?;
    let roots = admin_server::load_roots(&trusted)?;
    let member_addrs: Vec<SocketAddr> =
        rcfg.as_file().member_servers.iter().map(|m| m.addr).collect();
    let edit =
        ReferralEdit::SetParent { path: proposed_path.to_string(), parent: parent.to_vec() };
    let peers = admin_server::push_referral_edit_to_peers(
        &edit,
        &member_addrs,
        cfg.listen,
        &cert,
        &key,
        roots,
    )
    .await;
    Ok(ClusterPropagation::Pushed(peers))
}
