//! Resolver-hierarchy delegation as query + action.
//!
//! A child resolver requests delegation of a subtree from a parent's admin
//! server (glyph-confirming it first) and polls; the parent admin lists the
//! pending requests, matches the out-of-band code, and approves the CA-owned
//! topology transaction or denies it. The review ceremony becomes
//! `list_pending_delegations` (query) + `approve_delegation`/`deny_delegation`
//! (actions keyed by the request code — recomputed locally from the path and
//! stable parent/child server sets, never trusted from the wire).

use super::{find_by_code, open_admin_session};
use crate::{
    admin_proto::{InfoAuth, PeerResult, ResolverAddr},
    answer::Answerer,
    config_lock::ConfigDirLock,
    fingerprint::Fingerprint,
    plan::delegation::delegate_under_parent,
    resolver::ResolverConfig,
    template::{self, ParentRef, ReferralAuth},
    transport,
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

/// A pending or approved delegation request, keyed by its **code** (the delegation
/// fingerprint recomputed locally from its path and server sets). `id` is the
/// opaque server request id an action reuses — private, so callers address a
/// request only by its code.
pub struct PendingDelegation {
    /// The request code an operator matches out of band (`code IS the id`).
    pub code: Fingerprint,
    /// The subtree the child asks to own.
    pub proposed_path: String,
    /// The proposed/final parent resolver cluster's resolver address(es).
    pub parent: Vec<ResolverAddr>,
    /// The proposed/final child resolver cluster's resolver address(es).
    pub child: Vec<ResolverAddr>,
    /// Immutable identities covered by the out-of-band request code.
    pub parent_servers: Vec<netidx_admin_proto::AdminServerId>,
    pub child_servers: Vec<netidx_admin_proto::AdminServerId>,
    pub parent_cluster: netidx_admin_proto::ResolverClusterId,
    pub child_cluster: netidx_admin_proto::ResolverClusterId,
    pub parent_base: String,
    pub child_base: String,
    /// Already approved; approving again runs idempotent reconciliation.
    pub approved: bool,
    /// Seconds since the request was queued.
    pub age_secs: u64,
    /// The address the request arrived from.
    pub peer: String,
    id: String,
}

fn to_pending(e: netidx_admin_proto::DelegationEntry) -> PendingDelegation {
    PendingDelegation {
        code: transport::delegation_code(
            &e.proposed_path,
            &e.parent_servers,
            &e.child_servers,
        ),
        proposed_path: e.proposed_path,
        parent: e.parent_members,
        child: e.child_members,
        parent_servers: e.parent_servers,
        child_servers: e.child_servers,
        parent_cluster: e.parent,
        child_cluster: e.child,
        parent_base: e.parent_base,
        child_base: e.child_base,
        approved: e.approved,
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
    password: Option<netidx_admin_proto::Secret>,
) -> Result<Vec<PendingDelegation>> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let entries =
        transport::list_delegations(sess.server, sess.credential.clone(), &sess.identity)
            .await?;
    Ok(entries.into_iter().map(to_pending).collect())
}

/// The outcome of approving a delegation: the subtree, plus the per-peer
/// resolver cluster-propagation results (an already-approved request re-applies and
/// re-pushes idempotently, so re-running converges a resolver cluster whose peer was
/// unreachable).
pub struct DelegationDecision {
    /// The subtree that was delegated.
    pub proposed_path: String,
    /// Per-resolver cluster-member propagation results.
    pub peers: Vec<PeerResult>,
}

/// The `resolver approve-delegation <code>` action. Re-lists the queue,
/// [`find_by_code`]s the one request whose recomputed code matches (refusing on
/// no-match / ambiguity), and approves it resolver cluster-wide.
pub async fn approve_delegation(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<netidx_admin_proto::Secret>,
    code: &str,
) -> Result<DelegationDecision> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let items: Vec<PendingDelegation> =
        transport::list_delegations(sess.server, sess.credential.clone(), &sess.identity)
            .await?
            .into_iter()
            .map(to_pending)
            .collect();
    let item = find_by_code(&items, code, |i| Some(i.code))?;
    let (id, proposed_path) = (item.id.clone(), item.proposed_path.clone());
    let peers = transport::approve_delegation(
        sess.server,
        sess.credential.clone(),
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
    password: Option<netidx_admin_proto::Secret>,
    code: &str,
    reason: &str,
) -> Result<PendingDelegation> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let items: Vec<PendingDelegation> =
        transport::list_delegations(sess.server, sess.credential.clone(), &sess.identity)
            .await?
            .into_iter()
            .map(to_pending)
            .collect();
    let item = find_by_code(&items, code, |i| Some(i.code))?;
    transport::deny_delegation(
        sess.server,
        sess.credential.clone(),
        &item.id,
        reason,
        &sess.identity,
    )
    .await?;
    // Return the matched item (owned) for the caller to report.
    let PendingDelegation {
        code,
        proposed_path,
        parent,
        child,
        parent_servers,
        child_servers,
        parent_cluster,
        child_cluster,
        parent_base,
        child_base,
        approved,
        age_secs,
        peer,
        id,
    } = item;
    Ok(PendingDelegation {
        code: *code,
        proposed_path: proposed_path.clone(),
        parent: parent.clone(),
        child: child.clone(),
        parent_servers: parent_servers.clone(),
        child_servers: child_servers.clone(),
        parent_cluster: *parent_cluster,
        child_cluster: *child_cluster,
        parent_base: parent_base.clone(),
        child_base: child_base.clone(),
        approved: *approved,
        age_secs: *age_secs,
        peer: peer.clone(),
        id: id.clone(),
    })
}

/// Whether the child's freshly-written parent referral reached the rest of its
/// resolver cluster.
pub enum ResolverClusterPropagation {
    /// The CA controller updated every registered parent and child member.
    ControllerManaged,
}

/// The outcome of `resolver add-parent`.
pub struct AddParentOutcome {
    /// The subtree this resolver now owns under the parent.
    pub proposed_path: String,
    /// Whether/how the referral propagated to the child's other resolver cluster members.
    pub propagation: ResolverClusterPropagation,
}

pub enum AddParentCompletion {
    Complete(AddParentOutcome),
    LocalWrite(PendingParentReferral),
}

pub struct PendingParentReferral {
    resolver_config: PathBuf,
    expected_child: Vec<ResolverAddr>,
    parent_ref: ParentRef,
    outcome: AddParentOutcome,
}

impl PendingParentReferral {
    pub fn apply(
        self,
        config_lock: &ConfigDirLock,
        ans: &mut dyn Answerer,
    ) -> Result<AddParentOutcome> {
        self.apply_with_note(config_lock, |message| ans.note(message))
    }

    fn apply_with_note(
        self,
        config_lock: &ConfigDirLock,
        mut note: impl FnMut(&str),
    ) -> Result<AddParentOutcome> {
        let Self { resolver_config, expected_child, parent_ref, outcome } = self;
        let resolver_config = config_lock.require_contained(resolver_config)?;
        let current = ResolverConfig::load(&resolver_config)?;
        if template::parent_referral_matches_config(&current, &parent_ref) {
            note("the controller already wrote this resolver's approved topology");
            return Ok(outcome);
        }
        if current.as_file().parent.is_some() {
            bail!(
                "the resolver's parent changed while delegation approval was pending; \
                 no local changes were written. Re-run add-parent against the current \
                 configuration"
            );
        }
        if current.resolver_addrs() != expected_child {
            bail!(
                "the resolver's advertised addresses or authentication changed while \
                 delegation approval was pending; no local changes were written. \
                 Re-run add-parent so the approval covers the current configuration"
            );
        }
        let rt = template::set_parent_referral_on(&resolver_config, current, parent_ref)?;
        note(&rt.describe());
        rt.apply(config_lock).context("writing the parent referral")?;
        Ok(outcome)
    }
}

fn validate_existing_parent(
    existing_path: Option<&str>,
    proposed_path: &str,
    has_complete_parent_selection: bool,
) -> Result<()> {
    let Some(existing_path) = existing_path else { return Ok(()) };
    if existing_path != proposed_path {
        bail!(
            "this resolver is already attached at {existing_path:?}; re-parenting \
             to {proposed_path:?} isn't supported (uninstall + reinstall to switch \
             admin domains)"
        );
    }
    if !has_complete_parent_selection {
        bail!(
            "refreshing an existing delegation requires selecting every resolver \
             in the parent resolver cluster (`--parent-resolver` once per member), so the \
             new approval code covers the complete current parent and child server \
             sets"
        );
    }
    Ok(())
}

/// The `resolver add-parent` action: attach a standalone resolver under a
/// parent by delegation, then write this installer's local `parent` referral.
/// All remote parent/child propagation is owned by the CA controller. The
/// parent glyph confirm + queue + poll runs through [`delegate_under_parent`].
pub async fn prepare_add_parent(
    ans: &mut dyn Answerer,
    resolver_config: &Path,
    parent_server: SocketAddr,
    proposed_path: &str,
    selection: Option<crate::plan::delegation::DelegationSelection>,
) -> Result<AddParentCompletion> {
    let existing_cluster_change = selection.is_some();
    let rcfg = ResolverConfig::load(resolver_config)?;
    validate_existing_parent(
        rcfg.as_file().parent.as_ref().map(|parent| parent.path.as_str()),
        proposed_path,
        existing_cluster_change,
    )?;
    let child = rcfg.resolver_addrs();
    if child.is_empty() {
        bail!(
            "this resolver advertises no admin domain address (Local-only?) — it cannot \
             be delegated a subtree."
        );
    }
    // Queue the request, glyph-confirm the parent, and poll until approved. The
    // approval is authoritative; the returned members are the exact parent
    // resolver cluster produced by the CA-owned split/attach transaction.
    let approved =
        delegate_under_parent(ans, parent_server, proposed_path, &child, selection, None)
            .await?;
    let parent = approved;
    let parent_ref = ParentRef {
        path: ArcStr::from(proposed_path),
        ttl: None,
        addrs: parent.iter().map(|r| (r.addr, info_to_referral_auth(&r.auth))).collect(),
    };
    // The controller fanout normally reaches this registered child while the
    // requestor is polling, so its full topology may already be on disk. Treat
    // that exact state as successful completion; a different pre-existing
    // parent still fails in `set_parent_referral` as a real reparent attempt.
    let outcome = AddParentOutcome {
        proposed_path: proposed_path.to_string(),
        propagation: ResolverClusterPropagation::ControllerManaged,
    };
    if template::parent_referral_matches(resolver_config, &parent_ref)? {
        ans.note("the controller already wrote this resolver's approved topology");
        Ok(AddParentCompletion::Complete(outcome))
    } else if existing_cluster_change {
        bail!(
            "the delegation was approved, but the controller did not write this resolver's complete topology; do not apply a parent-only edit. Re-approve the delegation to reconcile the failed target"
        );
    } else {
        Ok(AddParentCompletion::LocalWrite(PendingParentReferral {
            resolver_config: resolver_config.to_path_buf(),
            expected_child: child,
            parent_ref,
            outcome,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_resolver(dir: &Path, parent: &str) -> PathBuf {
        let path = dir.join("resolver.json");
        std::fs::write(
            &path,
            format!(
                r#"{{
                    "children": [],
                    "parent": {parent},
                    "member_servers": [{{
                        "addr": "127.0.0.1:4654",
                        "bind_addr": "127.0.0.1",
                        "auth": "Anonymous",
                        "hello_timeout": 10,
                        "max_connections": 768,
                        "pid_file": "",
                        "reader_ttl": 60,
                        "writer_ttl": 120,
                        "id_map_command": null,
                        "id_map_type": "DoNotMap",
                        "id_map_timeout": 3600
                    }}],
                    "perms": {{}},
                    "include_permissions": []
                }}"#
            ),
        )
        .unwrap();
        path
    }

    fn pending(resolver_config: PathBuf) -> PendingParentReferral {
        PendingParentReferral {
            resolver_config,
            expected_child: vec![ResolverAddr {
                addr: "127.0.0.1:4654".parse().unwrap(),
                auth: InfoAuth::Anonymous,
            }],
            parent_ref: ParentRef {
                path: ArcStr::from("/eu"),
                ttl: None,
                addrs: vec![("10.0.0.1:4564".parse().unwrap(), ReferralAuth::Anonymous)],
            },
            outcome: AddParentOutcome {
                proposed_path: "/eu".to_string(),
                propagation: ResolverClusterPropagation::ControllerManaged,
            },
        }
    }

    #[test]
    fn existing_parent_allows_only_membership_bound_same_path_refresh() {
        validate_existing_parent(None, "/eu", false).unwrap();
        validate_existing_parent(Some("/eu"), "/eu", true).unwrap();
        assert!(validate_existing_parent(Some("/eu"), "/ap", true).is_err());
        assert!(validate_existing_parent(Some("/eu"), "/eu", false).is_err());
    }

    #[test]
    fn add_parent_preserves_an_unrelated_concurrent_edit() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_resolver(dir.path(), "null");
        let pending = pending(path.clone());

        let mut concurrent = ResolverConfig::load(&path).unwrap();
        concurrent.as_file_mut().member_servers[0].reader_ttl = 777;
        concurrent.save(&path).unwrap();

        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        pending.apply_with_note(&lock, |_| {}).unwrap();
        let current = ResolverConfig::load(&path).unwrap();
        assert_eq!(current.as_file().member_servers[0].reader_ttl, 777);
        assert_eq!(current.as_file().parent.as_ref().unwrap().path.as_str(), "/eu");
    }

    #[test]
    fn add_parent_rejects_a_changed_child_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_resolver(dir.path(), "null");
        let pending = pending(path.clone());

        let mut concurrent = ResolverConfig::load(&path).unwrap();
        concurrent.as_file_mut().member_servers[0].addr =
            "127.0.0.1:4655".parse().unwrap();
        concurrent.save(&path).unwrap();

        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        let err = pending.apply_with_note(&lock, |_| {}).err().unwrap();
        assert!(
            format!("{err:#}").contains("advertised addresses or authentication changed")
        );
        assert!(ResolverConfig::load(&path).unwrap().as_file().parent.is_none());
    }

    #[test]
    fn add_parent_accepts_the_exact_controller_written_topology() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_resolver(
            dir.path(),
            r#"{"path":"/eu","ttl":null,"addrs":[["10.0.0.1:4564","Anonymous"]]}"#,
        );
        let pending = pending(path);
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        let mut noted = false;
        pending
            .apply_with_note(&lock, |message| {
                noted = message.contains("controller already wrote")
            })
            .unwrap();
        assert!(noted);
    }
}
