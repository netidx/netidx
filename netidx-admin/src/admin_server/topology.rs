#[cfg(test)]
#[path = "topology_tests.rs"]
mod tests;

use super::{
    MutableState, PUSH_TIMEOUT, Server, audit,
    auth::{
        PreparedAdminAuthentication, PreparedServerUnlock, authenticate, broad_admin,
        delegation_authority, prepare_server_unlock, scope_covers, server_unlock,
    },
    revocation::{
        apply_crl_to_destinations, collect_peer_results, local_crl_destinations,
        push_crl_to_peers, registered_crl_targets, revoke_server_certificates,
        validate_home_crl,
    },
};
use crate::{
    admin_domain,
    admin_proto::{
        self, AdminDomainMap, ApplyCaStateRequest, ApplyCaStateResponse,
        ApplyReferralEditRequest, ApplyReferralEditResponse, ApproveDelegationRequest,
        ApproveDelegationResponse, DelegationEntry, DelegationPollResponse,
        DelegationRequest, DelegationResponse, DenyDelegationRequest,
        DenyDelegationResponse, GetInfoResponse, InfoAuth, ListDelegationsRequest,
        ListDelegationsResponse, MapVersion, PeerResult, PollRequest, PropagationOk,
        QueuedOk, ReconcileCaResponse, ReferralEdit, RegisterRequest, RegisterResponse,
        RemoveServerOk, RemoveServerRequest, RemoveServerResponse, ResolverAddr, Role,
    },
    admin_server_config::AdminServerConfig,
    config_lock::ConfigDirLock,
    delegation_store, transport,
};
use anyhow::{Context, Result, anyhow, bail};
use enumflags2::BitFlags;
use futures::{StreamExt, stream};
use log::{info, warn};
use std::{
    collections::BTreeSet, net::SocketAddr, path::Path, sync::Arc, time::Duration,
};
use tokio::sync::Semaphore;

async fn push_ca_state_to_peers(
    state: &Arc<Server>,
    crl_pem: String,
    operation_id: admin_proto::OperationId,
) -> Result<Vec<PeerResult>> {
    let (map, my_id) =
        state.read(move |state| (state.map.clone(), state.cfg.server_id)).await;
    let ca = map
        .ca_entry()
        .filter(|entry| entry.state == admin_proto::ServerState::Registered)
        .cloned()
        .context("the authoritative map has no registered CA")?;
    let request = ApplyCaStateRequest {
        operation_id,
        ca: ca.id,
        addr: ca.addr,
        map: map.clone(),
        crl_pem,
    };
    let targets = registered_crl_targets(state).await;
    let mut results = Vec::new();
    if let Some((server, addr)) = targets.iter().copied().find(|(id, _)| *id == my_id) {
        let request = request.clone();
        let local = match handle_apply_ca_state(state, &request).await {
            ApplyCaStateResponse::Ok(()) => Ok(()),
            ApplyCaStateResponse::Err { reason } => Err(anyhow!(reason)),
        };
        let error = local.err().map(|e| format!("{e:#}"));
        results.push(PeerResult { server, addr, error });
    }
    let client = state.outbound_client().await?;
    let home_ca = state.home_ca_der.clone();
    let mut remote = collect_peer_results(
        targets.into_iter().filter(|(server, _)| *server != my_id),
        |server, addr| {
            let client = client.clone();
            let home_ca = home_ca.clone();
            let request = request.clone();
            async move {
                tokio::time::timeout(
                    PUSH_TIMEOUT,
                    transport::push_ca_state(
                        &client,
                        addr,
                        server,
                        server == ca.id,
                        home_ca,
                        request,
                    ),
                )
                .await
                .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
                .and_then(|result| result)
            }
        },
    )
    .await;
    results.append(&mut remote);
    results.sort_by_key(|result| result.server);
    Ok(results)
}

async fn read_ca_crl(state: &Server) -> Result<Option<String>> {
    let crl_path = state
        .read(|state| {
            state
                .ca
                .as_ref()
                .context("CA reconciliation requires the CA role")
                .map(|ca| ca.store.crl_path())
        })
        .await?;
    match tokio::fs::read_to_string(&crl_path).await {
        Ok(crl_pem) => Ok(Some(crl_pem)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => {
            Err(e).with_context(|| format!("reading current CRL {}", crl_path.display()))
        }
    }
}

async fn initialize_ca_crl(
    state: &Server,
    prepared_server_unlock: &PreparedServerUnlock,
) -> Result<String> {
    state
        .write_async(async move |state| {
            let ca =
                state.ca.as_mut().context("CA reconciliation requires the CA role")?;
            let crl_path = ca.store.crl_path();
            match tokio::fs::read_to_string(&crl_path).await {
                Ok(crl_pem) => Ok(crl_pem),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    server_unlock(ca, prepared_server_unlock)
                        .await
                        .map_err(|reason| anyhow!(reason))?;
                    tokio::fs::read_to_string(&crl_path).await.with_context(|| {
                        format!("reading newly initialized CRL {}", crl_path.display())
                    })
                }
                Err(e) => Err(e).with_context(|| {
                    format!("reading current CRL {}", crl_path.display())
                }),
            }
        })
        .await
}

pub(super) async fn reconcile_ca_state_on_start(
    state: Arc<Server>,
    signs: Arc<Semaphore>,
) {
    let operation_id = admin_proto::OperationId::new();
    let Some(ca_dir) = state.ca_dir().await else { return };
    audit(
        &ca_dir,
        "(startup)",
        "reconcile-ca",
        &format!("operation {operation_id}: startup reconciliation"),
        Duration::ZERO,
    )
    .await;
    let crl_pem = match read_ca_crl(&state).await {
        Ok(Some(crl_pem)) => crl_pem,
        Ok(None) => {
            let prepared_server_unlock = prepare_server_unlock(&state, &signs).await;
            match initialize_ca_crl(&state, &prepared_server_unlock).await {
                Ok(crl_pem) => crl_pem,
                Err(e) => {
                    warn!("admin-server: startup CA reconciliation failed: {e:#}");
                    return;
                }
            }
        }
        Err(e) => {
            warn!("admin-server: startup CA reconciliation failed: {e:#}");
            return;
        }
    };
    match push_ca_state_to_peers(&state, crl_pem, operation_id).await {
        Ok(results) => {
            for result in results {
                if let Some(error) = result.error {
                    warn!(
                        "admin-server: startup CA reconciliation {} at {} failed: {}",
                        result.server, result.addr, error
                    );
                }
            }
        }
        Err(e) => warn!("admin-server: startup CA reconciliation failed: {e:#}"),
    }
}

pub(super) async fn handle_reconcile_ca(
    state: &Arc<Server>,
    req: &admin_proto::ReconcileCaRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    local: bool,
) -> ReconcileCaResponse {
    let Some(ca_dir) = state.ca_dir().await else {
        return ReconcileCaResponse::Err {
            reason: "CA reconciliation must be sent to the CA".to_string(),
        };
    };
    let admin = if local {
        "local".to_string()
    } else {
        let credential = req.credential.clone();
        match state
            .write(move |state| {
                authenticate(
                    state.ca.as_mut().expect("CA role held"),
                    &credential,
                    authentication,
                )
                .map(|authd| authd.admin)
            })
            .await
        {
            Ok(admin) => admin,
            Err(reason) => return ReconcileCaResponse::Err { reason },
        }
    };
    let operation_id = admin_proto::OperationId::new();
    audit(
        &ca_dir,
        &admin,
        "reconcile-ca",
        &format!("operation {operation_id}"),
        Duration::ZERO,
    )
    .await;
    let crl_pem = match read_ca_crl(state).await {
        Ok(Some(crl_pem)) => crl_pem,
        Ok(None) => match initialize_ca_crl(state, prepared_server_unlock).await {
            Ok(crl_pem) => crl_pem,
            Err(e) => {
                return ReconcileCaResponse::Err { reason: format!("{e:#}") };
            }
        },
        Err(e) => {
            return ReconcileCaResponse::Err { reason: format!("{e:#}") };
        }
    };
    let mut peers = match push_ca_state_to_peers(state, crl_pem, operation_id).await {
        Ok(peers) => peers,
        Err(e) => {
            return ReconcileCaResponse::Err { reason: format!("{e:#}") };
        }
    };
    let topology = {
        let map = state.read(move |state| state.map.clone()).await;
        topology_fanout(&map, map.resolver_clusters.iter())
    };
    merge_topology_results(
        &mut peers,
        push_topology(state, topology, operation_id).await,
    );
    ReconcileCaResponse::Ok(PropagationOk { operation_id, peers })
}

fn merge_topology_results(peers: &mut Vec<PeerResult>, topology: Vec<PeerResult>) {
    for mut result in topology {
        let Some(existing) = peers.iter_mut().find(|peer| peer.server == result.server)
        else {
            if let Some(error) = result.error.as_mut() {
                *error = format!("resolver topology: {error}");
            }
            peers.push(result);
            continue;
        };
        let Some(error) = result.error else { continue };
        existing.error = Some(match existing.error.take() {
            Some(state_error) => {
                format!("CA state: {state_error}; resolver topology: {error}")
            }
            None => format!("resolver topology: {error}"),
        });
    }
    peers.sort_by_key(|peer| peer.server);
}

pub(super) async fn handle_apply_ca_state(
    state: &Server,
    req: &ApplyCaStateRequest,
) -> ApplyCaStateResponse {
    let err = |reason: String| ApplyCaStateResponse::Err { reason };
    let Some(ca) = req.map.ca_entry() else {
        return err("authoritative map has no CA entry".to_string());
    };
    if ca.id != req.ca
        || ca.addr != req.addr
        || ca.state != admin_proto::ServerState::Registered
        || !ca.roles.contains(Role::Ca)
    {
        return err("authoritative map does not bind the claimed registered CA address"
            .to_string());
    }
    if let Err(e) = validate_home_crl(&req.crl_pem, state.home_ca_der.as_ref()) {
        return err(format!("validating CA CRL: {e:#}"));
    }
    let req = req.clone();
    let cfg_path = state.cfg_path.clone();
    let config_lock = state.config_lock.clone();
    let home_ca_der = state.home_ca_der.clone();
    state
        .write_async(async move |mutable| {
            let installed_ca = mutable.map.ca;
            if req.ca != installed_ca || req.map.ca != installed_ca {
                return err(format!(
                    "CA identity mismatch (installed {}, request {}, map {})",
                    installed_ca, req.ca, req.map.ca
                ));
            }
            if req.map.version < mutable.map.version {
                return err(format!(
                    "refusing ca-state rollback from map version {} to {}",
                    mutable.map.version, req.map.version
                ));
            }
            if mutable.ca.is_none() {
                let Some(cfg_path) = cfg_path.as_ref() else {
                    return err("this node has no persistent admin-server config path"
                        .to_string());
                };
                let mut next = mutable.cfg.clone();
                next.ca_addr = Some(req.addr);
                if let Err(e) =
                    crate::admin_server_config::save_async(&config_lock, cfg_path, &next)
                        .await
                {
                    return err(format!("persisting the relocated CA address: {e:#}"));
                }
                mutable.cfg = next;
            }
            let destinations = match local_crl_destinations(&mutable.cfg).await {
                Ok(destinations) => destinations,
                Err(e) => {
                    return err(format!("locating reconciled CRL destinations: {e:#}"));
                }
            };
            if let Err(e) = apply_crl_to_destinations(
                &config_lock,
                &req.crl_pem,
                home_ca_der.as_ref(),
                destinations,
            )
            .await
            {
                return err(format!("installing reconciled CRL: {e:#}"));
            }
            mutable.map = req.map.clone();
            ApplyCaStateResponse::Ok(())
        })
        .await
}

pub(super) async fn get_info(state: &Server) -> GetInfoResponse {
    let cfg = state.read(move |state| state.cfg.clone()).await;
    let resolver = match cfg.roles.resolver.as_ref() {
        Some(role) => match resolver_info(&role.config).await {
            Ok(resolver) => resolver,
            Err(e) => {
                warn!(
                    "admin-server: could not derive resolver info from {}: {e:#}",
                    role.config.display()
                );
                None
            }
        },
        None => None,
    };
    GetInfoResponse {
        domain: cfg.domain,
        ca_addr: if cfg.roles.ca.is_some() { Some(cfg.listen) } else { cfg.ca_addr },
        resolver,
        peers: cfg.peers,
    }
}

/// Derive this host's advertised resolver address + data-plane auth
/// from its resolver config — the first advertisable (non-`Local`)
/// member, the representative `GetInfo` reports. See
/// [`ResolverConfig::resolver_addrs`](crate::resolver::ResolverConfig::resolver_addrs)
/// for the full resolver cluster set (used by delegation).
async fn resolver_info(config: &Path) -> Result<Option<ResolverAddr>> {
    let rc = crate::resolver::ResolverConfig::load_async(config).await?;
    Ok(rc.resolver_addrs().into_iter().next())
}

pub(super) async fn local_resolver_data(
    cfg: &AdminServerConfig,
) -> (Option<ResolverAddr>, Option<admin_proto::ResolverClusterFacts>) {
    let Some(role) = cfg.roles.resolver.as_ref() else { return (None, None) };
    match crate::resolver::ResolverConfig::load_async(&role.config).await {
        Ok(config) => {
            (config.resolver_addrs().into_iter().next(), Some(config.cluster_facts()))
        }
        Err(e) => {
            warn!(
                "admin-server: deriving resolver facts from {}: {e:#}",
                role.config.display()
            );
            (None, None)
        }
    }
}

/// Structural check on a proposed delegation subtree.
fn validate_delegation_path(path: &str) -> Result<()> {
    use netidx::path::Path as NPath;
    let p = NPath::from(String::from(path));
    if !NPath::is_absolute(&p) {
        bail!("delegation path must be absolute (got {path:?})");
    }
    if p.as_ref() == "/" {
        bail!("the root path cannot be delegated");
    }
    Ok(())
}

/// `RequestDelegation` (unauthenticated): structural checks then enqueue.
pub(super) async fn handle_request_delegation(
    state: &Server,
    req: &DelegationRequest,
    peer: SocketAddr,
) -> DelegationResponse {
    let Some(ca_dir) = state.ca_dir().await else {
        return DelegationResponse::Err { reason: "this host is not the CA".into() };
    };
    if let Err(e) = validate_delegation_path(&req.proposed_path) {
        return DelegationResponse::Err { reason: format!("{e:#}") };
    }
    let pending = delegation_store::PendingDelegation::new(
        req.proposed_path.clone(),
        req.parent_servers.clone(),
        req.child_servers.clone(),
        peer.to_string(),
    );
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |state| {
            let mut staged = state.map.clone();
            if let Err(e) = admin_domain::delegate(
                &mut staged,
                &pending.proposed_path,
                pending.proposed_child,
                &pending.parent_servers,
                &pending.child_servers,
            ) {
                return DelegationResponse::Err {
                    reason: format!("invalid delegation: {e:#}"),
                };
            }
            match delegation_store::enqueue(&config_lock, &ca_dir, &pending).await {
                Ok(()) => DelegationResponse::Ok(QueuedOk { request_id: pending.id }),
                Err(e) => DelegationResponse::Err { reason: format!("{e:#}") },
            }
        })
        .await
}

/// `PollDelegation` (unauthenticated): map the stored status to the wire.
pub(super) async fn handle_poll_delegation(
    ca_dir: &Path,
    req: &PollRequest,
) -> DelegationPollResponse {
    match delegation_store::status(ca_dir, &req.request_id).await {
        Ok(delegation_store::Status::Pending(_)) => DelegationPollResponse::Pending,
        Ok(delegation_store::Status::Approved { parent }) => {
            DelegationPollResponse::Approved { parent }
        }
        Ok(delegation_store::Status::Denied { reason }) => {
            DelegationPollResponse::Denied { reason }
        }
        Ok(delegation_store::Status::Unknown) | Err(_) => DelegationPollResponse::Unknown,
    }
}

/// `ListDelegations` (admin-authenticated): pending requests plus approved
/// requests that remain available for idempotent reconciliation.
pub(super) async fn handle_list_delegations(
    state: &Server,
    req: &ListDelegationsRequest,
    authentication: &PreparedAdminAuthentication,
) -> ListDelegationsResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_list_delegations_inner(state, &req, authentication).await
        })
        .await
}

async fn handle_list_delegations_inner(
    state: &mut MutableState,
    req: &ListDelegationsRequest,
    authentication: &PreparedAdminAuthentication,
) -> ListDelegationsResponse {
    let MutableState { map, ca, .. } = state;
    let Some(ca) = ca.as_mut() else {
        return ListDelegationsResponse::Err { reason: "this host is not the CA".into() };
    };
    if let Err(reason) = authenticate(ca, &req.credential, authentication) {
        return ListDelegationsResponse::Err { reason };
    }
    let dir = ca.dir().to_path_buf();
    let reqs = async {
        let mut reqs: Vec<_> = delegation_store::pending(&dir)
            .await?
            .into_iter()
            .map(|req| (req, false))
            .collect();
        reqs.extend(
            delegation_store::approved(&dir)
                .await?
                .into_iter()
                .map(|rec| (rec.req, true)),
        );
        reqs.sort_by_key(|(req, _)| req.received_unix);
        Ok::<_, anyhow::Error>(reqs)
    }
    .await;
    match reqs {
        Ok(reqs) => ListDelegationsResponse::Ok(
            reqs.into_iter()
                .filter_map(|(r, approved)| {
                    let mut staged = map.clone();
                    let change = admin_domain::delegate(
                        &mut staged,
                        &r.proposed_path,
                        r.proposed_child,
                        &r.parent_servers,
                        &r.child_servers,
                    )
                    .ok()?;
                    Some(DelegationEntry {
                        age_secs: r.age_secs(),
                        id: r.id,
                        proposed_path: r.proposed_path,
                        parent_servers: r.parent_servers,
                        child_servers: r.child_servers,
                        parent: change.parent.id,
                        child: change.child.id,
                        parent_base: change.parent.base,
                        child_base: change.child.base,
                        parent_members: change.parent.members,
                        child_members: change.child.members,
                        approved,
                        peer: r.peer,
                    })
                })
                .collect(),
        ),
        Err(e) => ListDelegationsResponse::Err { reason: format!("{e:#}") },
    }
}

/// Whether `authd` may approve or deny `pending`.
///
/// A delegation restructures the resolver hierarchy on **both** sides of the split: it
/// mounts the child subtree, and it rewrites the *parent* resolver cluster's referrals.
/// So it needs authority over the parent resolver cluster's base as well as over the
/// proposed child path — an admin scoped to `/eu` must not be able to decide a
/// delegation whose parent is the root resolver cluster just because the child lands
/// under `/eu`. [`admin_domain::delegate`] only enforces that the child path is
/// under the parent base, and only after authorization has already run.
///
/// Deny is held to the same rule as approve, per [`admin_authority_over`]:
/// authority to destroy must mirror authority to create.
fn decide_delegation_authority(
    authd: &crate::ca_vault::Authenticated,
    map: &AdminDomainMap,
    pending: &delegation_store::PendingDelegation,
) -> std::result::Result<(), String> {
    if !delegation_authority(authd, &pending.proposed_path) {
        return Err(format!(
            "admin {} is not authorized to decide delegations at {:?}",
            authd.admin, pending.proposed_path
        ));
    }
    let base = admin_domain::parent_base(map, &pending.parent_servers).map_err(|e| {
        format!("resolving the delegation's parent resolver cluster: {e:#}")
    })?;
    if !delegation_authority(authd, &base) {
        return Err(format!(
            "admin {} is not authorized to restructure the parent resolver cluster at {base:?}",
            authd.admin
        ));
    }
    Ok(())
}

pub(super) async fn handle_deny_delegation(
    state: &Server,
    req: &DenyDelegationRequest,
    authentication: &PreparedAdminAuthentication,
) -> DenyDelegationResponse {
    let req = req.clone();
    state
        .write_async(async move |state| {
            handle_deny_delegation_inner(state, &req, authentication).await
        })
        .await
}

async fn handle_deny_delegation_inner(
    state: &mut MutableState,
    req: &DenyDelegationRequest,
    authentication: &PreparedAdminAuthentication,
) -> DenyDelegationResponse {
    let MutableState { map, ca, .. } = state;
    let Some(ca) = ca.as_mut() else {
        return DenyDelegationResponse::Err {
            reason: "this host does not hold the CA".to_string(),
        };
    };
    let ca_dir = ca.dir().to_path_buf();
    let config_lock = ca.config_lock();
    let authd = match authenticate(ca, &req.credential, authentication) {
        Ok(a) => a,
        Err(reason) => return DenyDelegationResponse::Err { reason },
    };
    match delegation_store::read_pending(&ca_dir, &req.request_id).await {
        Ok(Some(pending)) => {
            if let Err(reason) = decide_delegation_authority(&authd, map, &pending) {
                return DenyDelegationResponse::Err { reason };
            }
            match delegation_store::deny(&config_lock, &ca_dir, &pending, &req.reason)
                .await
            {
                Ok(()) => DenyDelegationResponse::Ok(()),
                Err(e) => DenyDelegationResponse::Err { reason: format!("{e:#}") },
            }
        }
        Ok(None) => DenyDelegationResponse::Err {
            reason: "no such pending delegation request (expired, never queued, or \
                     already decided)"
                .to_string(),
        },
        Err(e) => DenyDelegationResponse::Err { reason: format!("{e:#}") },
    }
}

fn info_to_refauth(a: &InfoAuth) -> netidx::resolver_server::config::file::RefAuth {
    use netidx::resolver_server::config::file::RefAuth;
    match a {
        InfoAuth::Anonymous => RefAuth::Anonymous,
        InfoAuth::Krb5 { spn } => RefAuth::Krb5(arcstr::ArcStr::from(spn.as_str())),
        InfoAuth::Tls { name } => RefAuth::Tls(arcstr::ArcStr::from(name.as_str())),
    }
}

/// Apply a referral edit to the local resolver config — add/replace a
/// child, or set the parent. Idempotent (re-applying the same edit is a
/// no-op); validated via `validate_for_path` (so children-constraint
/// violations fail here) before the atomic save.
pub(super) async fn apply_referral_edit_local(
    config_lock: &ConfigDirLock,
    resolver_config_path: &Path,
    edit: &ReferralEdit,
) -> Result<()> {
    let resolver_config_path = config_lock.require_contained(resolver_config_path)?;
    use netidx::resolver_server::config::file::Referral;
    let mut rc = crate::resolver::ResolverConfig::load_async(&resolver_config_path)
        .await
        .with_context(|| {
            format!("loading resolver config {}", resolver_config_path.display())
        })?;
    match edit {
        ReferralEdit::SetTopology { local_member, members, parent, children } => {
            if !members.contains(local_member) {
                bail!(
                    "local resolver member {} is absent from its assigned resolver cluster",
                    local_member.addr
                );
            }
            let old_members = rc.as_file().member_servers.clone();
            let configured = rc.resolver_addrs();
            let local_block = old_members
                .iter()
                .find(|configured| configured.addr == local_member.addr)
                .filter(|_| configured.contains(local_member))
                .cloned()
                .context(
                    "the target's owned resolver member does not match its local config",
                )?;
            // `member_servers` are independent local launch choices, not a
            // replica roster. Preserve whichever in-resolver cluster convenience
            // blocks this file already has, drop blocks moved to the other
            // side of a split, and never synthesize missing peers.
            let mut ordered = vec![local_block];
            for existing in
                old_members.iter().filter(|existing| existing.addr != local_member.addr)
            {
                if let Some(desired) =
                    members.iter().find(|member| member.addr == existing.addr)
                {
                    if !configured.contains(desired) {
                        bail!(
                            "resolver member {} does not match its local configured authentication",
                            desired.addr
                        );
                    }
                    ordered.push(existing.clone());
                }
            }
            rc.as_file_mut().member_servers = ordered;
            // The ttl is what lets an edit reach clients that have already
            // been told where a neighbouring cluster lives; without one a
            // referral is cached for the life of the process.
            let ttl = Some(crate::template::REFERRAL_TTL);
            rc.as_file_mut().parent = parent.as_ref().map(|edge| Referral {
                path: arcstr::ArcStr::from(edge.path.as_str()),
                ttl,
                addrs: edge
                    .addrs
                    .iter()
                    .map(|r| (r.addr, info_to_refauth(&r.auth)))
                    .collect(),
            });
            rc.as_file_mut().children = children
                .iter()
                .map(|edge| Referral {
                    path: arcstr::ArcStr::from(edge.path.as_str()),
                    ttl,
                    addrs: edge
                        .addrs
                        .iter()
                        .map(|r| (r.addr, info_to_refauth(&r.auth)))
                        .collect(),
                })
                .collect();
        }
    }
    rc.save_async(&resolver_config_path)
        .await
        .context("the referral edit would make the resolver config invalid")
}

/// `ApplyReferralEdit` (server-to-server, peer-cert-gated): the receive
/// side of resolver cluster-wide delegation propagation. Requires a resolver role
/// (the config to edit).
pub(super) async fn handle_apply_referral_edit(
    state: &Server,
    req: &ApplyReferralEditRequest,
) -> ApplyReferralEditResponse {
    info!(
        "admin-server: applying referral operation {}: {:?}",
        req.operation_id, req.edit
    );
    state.apply_referral_edit(req).await
}

/// The role list an admin-server config implies.
pub(super) fn roles_of(cfg: &AdminServerConfig) -> BitFlags<Role> {
    let mut out = BitFlags::empty();
    if cfg.roles.ca.is_some() {
        out.insert(Role::Ca);
    }
    if cfg.roles.resolver.is_some() {
        out.insert(Role::Resolver);
    }
    if cfg.roles.id_map.is_some() {
        out.insert(Role::IdMap);
    }
    out
}

/// The CA host's own map entry. Only the CA writes its own entry directly;
/// every other host gets there through [`handle_register`], and this holds to
/// the same rule that registration enforces: a host's report may move its
/// address and its status, never its grant.
///
/// So the owned resolver member and its resolver cluster are read back out of
/// the map, and the local config is consulted only to seed a grant that does
/// not exist yet. [`relocate_resolver`](admin_domain::relocate_resolver) is how
/// an owned member moves, and it keeps the cluster roster consistent with the
/// entry — re-deriving the entry from disk on every poll would route around
/// that, and would drop the grant entirely on any poll where the resolver
/// config failed to parse.
pub(super) fn own_ca_entry(
    map: &AdminDomainMap,
    cfg: &AdminServerConfig,
    resolver: Option<ResolverAddr>,
    has_resolver: bool,
) -> admin_proto::AdminServerEntry {
    let existing = map.admin_servers.iter().find(|s| s.id == cfg.server_id);
    admin_proto::AdminServerEntry {
        id: cfg.server_id,
        addr: cfg.listen,
        roles: roles_of(cfg),
        resolver: existing.and_then(|s| s.resolver.clone()).or(resolver),
        cluster: existing
            .and_then(|s| s.cluster)
            .or_else(|| has_resolver.then(admin_proto::ResolverClusterId::new)),
        state: admin_proto::ServerState::Registered,
        reported_read_gate: None,
        reported_id_map_version: None,
    }
}

/// This host's own resolver base (the single resolver cluster a local, control-socket
/// caller may edit permissions at). `None` when this host serves no resolver.
pub(super) async fn own_base(state: &Server) -> Option<String> {
    state
        .read(move |state| {
            let cluster = state
                .map
                .admin_servers
                .iter()
                .find(|server| server.id == state.cfg.server_id)?
                .cluster?;
            state
                .map
                .resolver_clusters
                .iter()
                .find(|cluster_entry| cluster_entry.id == cluster)
                .map(|cluster_entry| cluster_entry.base.clone())
        })
        .await
}
/// Whether a host reporting `reported` is behind the CA's id-map model.
///
/// `None` — a host that has never applied anything, including one that has
/// just enrolled — is behind anything established. A CA with no model
/// established yet leaves every host alone; see [`IdMapModel::established`].
async fn id_map_behind(state: &Arc<Server>, reported: Option<u64>) -> bool {
    let model = state
        .read_async(async move |state| match state.ca.as_ref() {
            Some(ca) => ca.store.id_map_model().await.ok(),
            None => None,
        })
        .await;
    let Some(model) = model else { return false };
    model.established() && reported.is_none_or(|have| have < model.version)
}

pub(super) async fn handle_register(
    state: &Arc<Server>,
    server_id: admin_proto::AdminServerId,
    req: &RegisterRequest,
) -> RegisterResponse {
    let req = req.clone();
    let ca_dir = match state.ca_dir().await {
        Some(d) => d,
        None => {
            return RegisterResponse::Err {
                reason: "this host does not hold the CA — register with the CA"
                    .to_string(),
            };
        }
    };
    let validation_req = req.clone();
    let reconcile_id_map = match state
        .read(move |state| {
            let current = state
                .map
                .admin_servers
                .iter()
                .find(|server| server.id == server_id)
                .with_context(|| {
                    format!("server {server_id} has no approved enrollment grant")
                })?;
            // Reconcile any id-map host that is behind the model, not just
            // one registering for the first time. A host that was down for an
            // edit, or restored from a backup taken before one, is behind by
            // exactly the same measure as a brand-new one — which reports no
            // version at all — so there is one rule rather than a general case
            // and a special case that can disagree.
            let reconcile = current.roles.contains(Role::IdMap);
            let mut staged = state.map.clone();
            admin_domain::register(
                &mut staged,
                server_id,
                validation_req.addr,
                validation_req.resolver.as_ref(),
                validation_req.id_map_version,
            )?;
            Ok::<_, anyhow::Error>(reconcile)
        })
        .await
    {
        Ok(reconcile) => reconcile,
        Err(e) => return RegisterResponse::Err { reason: format!("{e:#}") },
    };
    // Cheap in the steady state: a host whose reported version matches the
    // model costs one integer comparison, not a map fetch. This runs on the
    // facts poll every host already makes, which is what bounds how long a
    // host that came back stays out of agreement.
    if reconcile_id_map && id_map_behind(state, req.id_map_version).await {
        if let Err(e) =
            crate::admin_server::id_map::reconcile_to_target(state, server_id, req.addr)
                .await
        {
            return RegisterResponse::Err {
                reason: format!("reconciling this host's id-map: {e:#}"),
            };
        }
    }
    let config_lock = state.config_lock.clone();
    let (response, fanout) = state
        .write_async(async move |state| {
            let updated = match admin_domain::register(
                &mut state.map,
                server_id,
                req.addr,
                req.resolver.as_ref(),
                req.id_map_version,
            ) {
                Ok(updated) => updated,
                Err(e) => {
                    return (RegisterResponse::Err { reason: format!("{e:#}") }, None);
                }
            };
            if updated
                && let Err(e) =
                    admin_domain::save_async(&config_lock, &ca_dir, &state.map).await
            {
                return (
                    RegisterResponse::Err {
                        reason: format!("persisting the admin domain map: {e:#}"),
                    },
                    None,
                );
            }
            let fanout = updated
                .then(|| registration_topology_fanout(&state.map, server_id))
                .flatten();
            (RegisterResponse::Ok(MapVersion { version: state.map.version }), fanout)
        })
        .await;
    if let Some(fanout) = fanout {
        let operation_id = admin_proto::OperationId::new();
        for result in push_topology(state, fanout, operation_id).await {
            if let Some(error) = result.error {
                warn!(
                    "admin-server: registration topology push {} at {} failed: {}",
                    result.server, result.addr, error
                );
            }
        }
    }
    response
}

/// CA-side: drop an admin server from the map on uninstall.
pub(super) async fn handle_deregister(
    state: &Server,
    server_id: admin_proto::AdminServerId,
) -> RegisterResponse {
    let ca_dir = match state.ca_dir().await {
        Some(d) => d,
        None => {
            return RegisterResponse::Err {
                reason: "this host does not hold the CA".to_string(),
            };
        }
    };
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |state| {
            let updated = match admin_domain::deregister(&mut state.map, server_id) {
                Ok(updated) => updated,
                Err(e) => return RegisterResponse::Err { reason: format!("{e:#}") },
            };
            if updated
                && let Err(e) =
                    admin_domain::save_async(&config_lock, &ca_dir, &state.map).await
            {
                return RegisterResponse::Err {
                    reason: format!("persisting the admin domain map: {e:#}"),
                };
            }
            RegisterResponse::Ok(MapVersion { version: state.map.version })
        })
        .await
}

struct RemoveServerPrepare {
    version: u64,
    revoked: u64,
    removed: bool,
    affected_clusters: Vec<String>,
    fanout: TopologyFanout,
    crl_pem: Option<String>,
}

/// The blocking half of permanent server removal: authenticate, validate the
/// transition, revoke every certificate for the immutable identity, and commit
/// the new authoritative map. The returned topology fanout is deliberately
/// separate: admin domain I/O must not hold mutable state or the signing semaphore.
async fn remove_server_prepare(
    state: &Server,
    req: &RemoveServerRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<RemoveServerPrepare, RemoveServerResponse> {
    let req = req.clone();
    state
        .write_async(async move |state| {
            remove_server_prepare_inner(
                state,
                &req,
                authentication,
                prepared_server_unlock,
                operation_id,
            )
            .await
        })
        .await
}

async fn remove_server_prepare_inner(
    state: &mut MutableState,
    req: &RemoveServerRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<RemoveServerPrepare, RemoveServerResponse> {
    let err = |reason: String| RemoveServerResponse::Err { reason };
    let MutableState { map, ca, .. } = state;
    let ca = match ca.as_mut() {
        Some(ca) => ca,
        None => return Err(err("this host does not hold the CA".to_string())),
    };
    let ca_dir = ca.dir().to_path_buf();
    let authd = match authenticate(ca, &req.credential, authentication) {
        Ok(a) => a,
        Err(reason) => return Err(err(reason)),
    };
    // Evicting an admin server from the authoritative map cascades that host's
    // resolver cluster facts out of the map — a privileged, admin domain-affecting
    // edit. Gate it on the admin-server lifecycle capability (the same bit
    // that authorizes enrolling one) or a broad admin.
    let broad = broad_admin(&authd);
    // Validate the authoritative-map transition on a copy first. Certificate
    // revocation is irreversible, so do not begin it for an invalid removal
    // (notably, removal of the active CA).
    {
        let target_base = map
            .admin_servers
            .iter()
            .find(|server| server.id == req.server)
            .and_then(|server| server.cluster)
            .and_then(|cluster| {
                map.resolver_clusters.iter().find(|entry| entry.id == cluster)
            })
            .map(|cluster| cluster.base.as_str());
        let scoped = match target_base {
            Some(base) => scope_covers(&authd.policy.server_enroll_scopes, base),
            // On an idempotent repeat the removed entry no longer tells us its
            // resolver cluster. A scoped admin may safely reconcile topology only inside its
            // own enrollment scopes.
            None => !authd.policy.server_enroll_scopes.is_empty(),
        };
        if !broad && !scoped {
            return Err(err(format!(
                "admin {} is not authorized to remove server {} at {}",
                authd.admin,
                req.server,
                target_base.unwrap_or("<unknown>")
            )));
        }
        // Keep the removed resolver cluster and each directly connected resolver cluster in the
        // reconciliation set. If the last member disappears, `admin_domain::remove`
        // deletes that resolver cluster and detaches its children; the surviving parent and
        // children still need fresh topology.
        let mut affected_ids = BTreeSet::new();
        if let Some(cluster_id) = map
            .admin_servers
            .iter()
            .find(|server| server.id == req.server)
            .and_then(|server| server.cluster)
        {
            affected_ids.insert(cluster_id);
            if let Some(cluster) =
                map.resolver_clusters.iter().find(|entry| entry.id == cluster_id)
            {
                affected_ids.extend(cluster.parent);
                affected_ids.extend(cluster.children.iter().copied());
            }
        }
        let mut affected_clusters: Vec<_> = map
            .resolver_clusters
            .iter()
            .filter(|cluster| affected_ids.contains(&cluster.id))
            .map(|cluster| cluster.base.clone())
            .collect();
        affected_clusters.sort();
        affected_clusters.dedup();
        let mut next = map.clone();
        let removed = match admin_domain::remove(&mut next, req.server) {
            Ok(removed) => removed,
            Err(e) => return Err(err(format!("{e:#}"))),
        };
        // A repeat after partial fanout cannot recover the removed identity's
        // resolver cluster from the current map (there is deliberately no durable job
        // record). Broad administrators therefore reconcile every surviving
        // resolver cluster on an idempotent repeat; scoped administrators reconcile only
        // resolver clusters their enrollment policy covers.
        if !removed {
            affected_ids.extend(
                map.resolver_clusters
                    .iter()
                    .filter(|cluster| {
                        broad
                            || scope_covers(
                                &authd.policy.server_enroll_scopes,
                                &cluster.base,
                            )
                    })
                    .map(|cluster| cluster.id),
            );
            affected_clusters = map
                .resolver_clusters
                .iter()
                .filter(|cluster| affected_ids.contains(&cluster.id))
                .map(|cluster| cluster.base.clone())
                .collect();
            affected_clusters.sort();
            affected_clusters.dedup();
        }
        let mut revoked = 0;
        if removed {
            revoked = revoke_server_certificates(
                ca,
                req.server,
                &authd.admin,
                prepared_server_unlock,
            )
            .await
            .map_err(|e| {
                err(format!("revoking the server's serving certificates: {e:#}"))
            })?;
            let config_lock = ca.config_lock();
            if let Err(e) = admin_domain::save_async(&config_lock, &ca_dir, &next).await {
                return Err(err(format!("persisting the admin domain map: {e:#}")));
            }
            *map = next;
            audit(
                &ca_dir,
                &authd.admin,
                "remove-server",
                &format!("operation {operation_id}: {}", req.server),
                Duration::ZERO,
            )
            .await;
        } else {
            audit(
                &ca_dir,
                &authd.admin,
                "reconcile-server-removal",
                &format!("operation {operation_id}: {}", req.server),
                Duration::ZERO,
            )
            .await;
        }
        // Always carry the current CRL on an idempotent retry as well: a previous
        // removal may have committed the revocation but only partially delivered
        // it. Repeating force-remove is the manual reconciliation path for both
        // topology and revocation state.
        let crl_pem = {
            let path = ca.store.crl_path();
            match tokio::fs::read_to_string(&path).await {
                Ok(pem) => Some(pem),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => {
                    return Err(err(format!(
                        "reading the current CRL for immediate distribution: {e:#}"
                    )));
                }
            }
        };
        let mut targets = Vec::new();
        for cluster in map
            .resolver_clusters
            .iter()
            .filter(|cluster| affected_ids.contains(&cluster.id))
        {
            for server in map.admin_servers.iter().filter(|server| {
                server.cluster == Some(cluster.id)
                    && server.state == admin_proto::ServerState::Registered
            }) {
                let Some(local_member) = server.resolver.clone() else {
                    continue;
                };
                targets.push((
                    server.id,
                    server.addr,
                    topology_edit(map, cluster, local_member),
                ));
            }
        }
        Ok(RemoveServerPrepare {
            version: map.version,
            revoked: revoked as u64,
            removed,
            affected_clusters,
            fanout: TopologyFanout { targets },
            crl_pem,
        })
    }
}

/// Permanently remove a dead identity, then reconcile only the surviving
/// resolver clusters whose referral topology changed. This never restarts a resolver;
/// administrators retain control of the rolling restart sequence.
pub(super) async fn handle_remove_server(
    state: &Arc<Server>,
    req: RemoveServerRequest,
    authentication: &PreparedAdminAuthentication,
    prepared_server_unlock: &PreparedServerUnlock,
) -> RemoveServerResponse {
    let operation_id = admin_proto::OperationId::new();
    // Captured before the commit takes it out of the map — we still have to
    // reach it afterwards.
    let target = req.server;
    let (my_id, target_addr) = state
        .read(move |state| {
            (
                state.cfg.server_id,
                super::service_control::registered_server_addr(&state.map, target),
            )
        })
        .await;
    let prepared = remove_server_prepare(
        state,
        &req,
        authentication,
        prepared_server_unlock,
        operation_id,
    )
    .await;
    let prepared = match prepared {
        Ok(prepared) => prepared,
        Err(response) => return response,
    };
    // Removing a member from everyone's configuration does not stop the
    // clients that have not synced yet from asking it, and it has no reason to
    // refuse them, so it would keep answering from a snapshot that only
    // decays. Tell it to stop. Best effort — a host that is already down is
    // not answering anyway, and the result says so either way.
    //
    // After the commit, not before: the caller has to be authorized first, and
    // the CA can still reach a host whose certificate it just revoked, because
    // outbound peer connections do not consult the CRL.
    let gated = match target_addr {
        None => None,
        Some(addr) => {
            let error = super::read_gate::push_to(
                state,
                target,
                addr,
                my_id,
                netidx::resolver_server::config::ReadGate::Yes,
                operation_id,
            )
            .await
            .err();
            Some(admin_proto::PeerResult { server: target, addr, error })
        }
    };
    let crl_peers = match prepared.crl_pem {
        Some(crl_pem) => push_crl_to_peers(state, &crl_pem, operation_id).await,
        None => Vec::new(),
    };
    let peers = push_topology(state, prepared.fanout, operation_id).await;
    RemoveServerResponse::Ok(RemoveServerOk {
        version: prepared.version,
        operation_id: Some(operation_id),
        revoked: prepared.revoked,
        removed: prepared.removed,
        affected_clusters: prepared.affected_clusters,
        peers,
        crl_peers,
        gated,
    })
}

/// Authenticate, validate, commit, and prepare the resolver fanout for a
/// delegation approval.
async fn approve_delegation_prepare(
    state: &Server,
    req: &ApproveDelegationRequest,
    authentication: &PreparedAdminAuthentication,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<TopologyFanout, ApproveDelegationResponse> {
    let req = req.clone();
    state
        .write_async(async move |state| {
            approve_delegation_prepare_inner(state, &req, authentication, operation_id)
                .await
        })
        .await
}

async fn approve_delegation_prepare_inner(
    state: &mut MutableState,
    req: &ApproveDelegationRequest,
    authentication: &PreparedAdminAuthentication,
    operation_id: admin_proto::OperationId,
) -> std::result::Result<TopologyFanout, ApproveDelegationResponse> {
    let err = |reason: String| ApproveDelegationResponse::Err { reason };
    let MutableState { map, ca, .. } = state;
    let ca =
        ca.as_mut().ok_or_else(|| err("this host does not hold the CA".to_string()))?;
    let ca_dir = ca.dir().to_path_buf();
    let authd = authenticate(ca, &req.credential, authentication).map_err(err)?;
    {
        // Pending ⇒ approve + commit; Approved ⇒ re-sync (re-apply + re-push,
        // already committed); else an error.
        let (pending, commit) = match delegation_store::status(&ca_dir, &req.request_id)
            .await
        {
            Ok(delegation_store::Status::Pending(p)) => (p, true),
            Ok(delegation_store::Status::Approved { .. }) => {
                match delegation_store::read_approved(&ca_dir, &req.request_id).await {
                    Ok(Some(rec)) => (rec.req, false),
                    Ok(None) => {
                        return Err(err("the approved record vanished".to_string()));
                    }
                    Err(e) => {
                        return Err(err(format!("reading the approved record: {e:#}")));
                    }
                }
            }
            Ok(delegation_store::Status::Denied { .. }) => {
                return Err(err("that delegation was already denied".to_string()));
            }
            Ok(delegation_store::Status::Unknown) => {
                return Err(err(
                    "no such pending delegation request (expired or never queued)"
                        .to_string(),
                ));
            }
            Err(e) => return Err(err(format!("{e:#}"))),
        };
        decide_delegation_authority(&authd, map, &pending).map_err(err)?;
        // Persist a staged snapshot before publishing it in memory. A failed disk
        // write must not leave the live CA map ahead of its durable map.
        let mut staged = map.clone();
        let change = admin_domain::delegate(
            &mut staged,
            &pending.proposed_path,
            pending.proposed_child,
            &pending.parent_servers,
            &pending.child_servers,
        )
        .map_err(|e| err(format!("updating authoritative topology: {e:#}")))?;
        let parent = change.parent;
        let child = change.child;
        let config_lock = ca.config_lock();
        admin_domain::save_async(&config_lock, &ca_dir, &staged)
            .await
            .map_err(|e| err(format!("persisting authoritative topology: {e:#}")))?;
        *map = staged;
        if commit {
            delegation_store::approve(
                &config_lock,
                &ca_dir,
                &pending,
                parent.members.clone(),
            )
            .await
            .map_err(|e| err(format!("committing the approval: {e:#}")))?;
        }
        audit(
            &ca_dir,
            &authd.admin,
            if commit { "approve-delegation" } else { "reconcile-delegation" },
            &format!("operation {operation_id}: {}", child.base),
            Duration::ZERO,
        )
        .await;
        Ok(topology_fanout(map, [&parent, &child]))
    }
}

struct TopologyFanout {
    targets: Vec<(admin_proto::AdminServerId, SocketAddr, ReferralEdit)>,
}

fn registration_topology_fanout(
    map: &AdminDomainMap,
    server_id: admin_proto::AdminServerId,
) -> Option<TopologyFanout> {
    let cluster = map
        .admin_servers
        .iter()
        .find(|server| server.id == server_id)?
        .cluster
        .and_then(|id| map.resolver_clusters.iter().find(|cluster| cluster.id == id))?;
    Some(topology_fanout(
        map,
        map.resolver_clusters.iter().filter(|candidate| {
            candidate.id == cluster.id
                || cluster.parent == Some(candidate.id)
                || cluster.children.contains(&candidate.id)
        }),
    ))
}

fn topology_fanout<'a>(
    map: &AdminDomainMap,
    clusters: impl IntoIterator<Item = &'a admin_proto::ResolverClusterEntry>,
) -> TopologyFanout {
    let mut targets = Vec::new();
    for cluster in clusters {
        for server in map.admin_servers.iter().filter(|server| {
            server.cluster == Some(cluster.id)
                && server.state == admin_proto::ServerState::Registered
        }) {
            let Some(local_member) = server.resolver.clone() else {
                continue;
            };
            targets.push((
                server.id,
                server.addr,
                topology_edit(map, cluster, local_member),
            ));
        }
    }
    TopologyFanout { targets }
}

fn topology_edit(
    map: &AdminDomainMap,
    cluster: &admin_proto::ResolverClusterEntry,
    local_member: ResolverAddr,
) -> ReferralEdit {
    let parent = cluster.parent.and_then(|id| {
        map.resolver_clusters.iter().find(|parent| parent.id == id).map(|parent| {
            admin_proto::ResolverClusterEdge {
                path: cluster.base.clone(),
                addrs: parent.members.clone(),
            }
        })
    });
    let mut children: Vec<_> = cluster
        .children
        .iter()
        .filter_map(|id| {
            map.resolver_clusters.iter().find(|child| child.id == *id).map(|child| {
                admin_proto::ResolverClusterEdge {
                    path: child.base.clone(),
                    addrs: child.members.clone(),
                }
            })
        })
        .collect();
    children.sort_by(|a, b| a.path.cmp(&b.path));
    ReferralEdit::SetTopology {
        local_member,
        members: cluster.members.clone(),
        parent,
        children,
    }
}

/// Propagate topology edits to every registered server in the affected
/// resolver clusters using CA-owned routing addresses.
async fn push_topology(
    state: &Arc<Server>,
    fanout: TopologyFanout,
    operation_id: admin_proto::OperationId,
) -> Vec<PeerResult> {
    let client = match state.outbound_client().await {
        Ok(client) => client,
        Err(e) => {
            return fanout
                .targets
                .into_iter()
                .map(|(server, addr, _)| PeerResult {
                    server,
                    addr,
                    error: Some(format!("loading outbound identity failed: {e:#}")),
                })
                .collect();
        }
    };
    let ca = state.read(move |state| state.map.ca).await;
    let mut targets = fanout.targets;
    targets.sort_by_key(|(id, _, _)| *id);
    let home_ca = state.home_ca_der.clone();
    let mut results: Vec<_> =
        stream::iter(targets.into_iter().map(|(server, addr, edit)| {
            let client = client.clone();
            let home_ca = home_ca.clone();
            async move {
                let res = tokio::time::timeout(
                    PUSH_TIMEOUT,
                    transport::push_referral_edit(
                        &client,
                        addr,
                        server,
                        server == ca,
                        home_ca,
                        operation_id,
                        &edit,
                    ),
                )
                .await
                .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
                .and_then(|r| r);
                PeerResult { server, addr, error: res.err().map(|e| format!("{e:#}")) }
            }
        }))
        .buffer_unordered(32)
        .collect()
        .await;
    results.sort_by_key(|r| r.server);
    results
}

pub(super) async fn handle_approve_delegation(
    state: &Arc<Server>,
    req: ApproveDelegationRequest,
    authentication: &PreparedAdminAuthentication,
) -> ApproveDelegationResponse {
    let operation_id = admin_proto::OperationId::new();
    let prepared =
        approve_delegation_prepare(state, &req, authentication, operation_id).await;
    let fanout = match prepared {
        Ok(fanout) => fanout,
        Err(response) => return response,
    };
    let peers = push_topology(state, fanout, operation_id).await;
    ApproveDelegationResponse::Ok(PropagationOk { operation_id, peers })
}
