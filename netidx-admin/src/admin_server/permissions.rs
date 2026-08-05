#[cfg(test)]
#[path = "permissions_tests.rs"]
mod tests;

use super::{
    PUSH_TIMEOUT, Server, audit,
    auth::{
        PreparedAdminAuthentication, authenticate, local_superuser, safe_auth_failure,
        scope_covers,
    },
    topology::own_base,
};
use crate::{
    admin_proto::{
        self, AdminDomainMap, ApplyPermsEditRequest, ApplyPermsEditResponse,
        EditPermsRequest, EditPermsResponse, GetPermsResponse, PeerResult, PropagationOk,
        ReadPermsOk, ReadPermsRequest, ReadPermsResponse,
    },
    ca_vault, transport,
};
use anyhow::{Context, Result, anyhow, bail};
use futures::{StreamExt, stream};
use log::info;
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

async fn local_perms_path(state: &Server) -> Result<PathBuf> {
    let rconfig = state
        .read(move |state| {
            state.cfg.roles.resolver.as_ref().map(|role| role.config.clone())
        })
        .await
        .context("this host has no resolver role — no perms to read or edit")?;
    perms_path(&rconfig).await
}

async fn perms_path(rconfig: &Path) -> Result<PathBuf> {
    let rc = crate::resolver::ResolverConfig::load_async(rconfig).await?;
    perms_path_from_config(rconfig, &rc)
}

fn perms_path_from_config(
    rconfig: &Path,
    rc: &crate::resolver::ResolverConfig,
) -> Result<PathBuf> {
    let inc = rc.as_file().include_permissions.first().cloned().context(
        "this resolver has no permissions file (include_permissions is empty — an \
         anonymous admin domain has no perms)",
    )?;
    let base = rconfig.parent().unwrap_or_else(|| Path::new("."));
    Ok(base.join(inc.as_str()))
}

/// Read the local resolver's perms file, serialized for the wire.
pub(super) async fn handle_get_perms(state: &Server) -> GetPermsResponse {
    let read = async {
        let path = local_perms_path(state).await?;
        let pmap = crate::perms::load_perms_async(&path).await?;
        serde_json::to_string(&pmap).context("serializing perms")
    };
    match read.await {
        Ok(perms_json) => GetPermsResponse::Ok(perms_json),
        Err(e) => GetPermsResponse::Err { reason: format!("{e:#}") },
    }
}

/// Authenticate a permissions operation. Remote callers use the CA vault or a
/// live session; the protected local socket is the on-box signing superuser.
/// Shared by reads and edits so their scope semantics cannot drift.
async fn authenticate_perms_caller(
    state: &Arc<Server>,
    credential: &admin_proto::AdminCredential,
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> std::result::Result<ca_vault::Authenticated, String> {
    if local {
        return Ok(local_superuser());
    }
    let auth_credential = credential.clone();
    let failure_credential = credential.clone();
    match state
        .write(move |state| {
            authenticate(
                state.ca.as_mut().expect("CA role held"),
                &auth_credential,
                authentication,
            )
        })
        .await
    {
        Ok(authd) => Ok(authd),
        Err(reason) => Err(safe_auth_failure(&failure_credential, reason)),
    }
}

fn authorize_perms_scope(
    authd: &ca_vault::Authenticated,
    target_path: &str,
    operation: &str,
) -> std::result::Result<(), String> {
    if authd.kind == netidx_admin_proto::policy::SlotKind::Signing
        || scope_covers(&authd.policy.perms_edit_scopes, target_path)
    {
        Ok(())
    } else {
        Err(format!(
            "admin {:?} ({}) is not authorized to {operation} perms at {:?}",
            authd.admin,
            match authd.kind {
                netidx_admin_proto::policy::SlotKind::Signing => "signing",
                netidx_admin_proto::policy::SlotKind::Role => "role",
            },
            target_path
        ))
    }
}

async fn confine_local_perms(
    state: &Server,
    target_path: &str,
    operation: &str,
) -> Result<()> {
    let base = own_base(state).await;
    if base.as_deref() == Some(target_path) {
        Ok(())
    } else {
        bail!(
            "local perms {operation}s are confined to this host's own resolver cluster ({}); \
             refusing to {operation} {:?}",
            base.as_deref().unwrap_or("<none>"),
            target_path,
        )
    }
}

/// Admin → CA permissions read. Authentication and policy are checked
/// once at the CA, then registered resolver cluster members are tried in stable
/// server-ID order using the CA certificate and exact target pinning.
pub(super) async fn handle_read_perms(
    state: &Arc<Server>,
    req: &ReadPermsRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> ReadPermsResponse {
    let err = |reason: String| ReadPermsResponse::Err { reason };
    if !local && !state.has_ca().await {
        return err("a remote perms read must be sent to the CA".to_string());
    }
    let authd =
        match authenticate_perms_caller(state, &req.credential, authentication, local)
            .await
        {
            Ok(authd) => authd,
            Err(reason) => return err(reason),
        };
    if local && let Err(e) = confine_local_perms(state, &req.target_path, "read").await {
        return err(format!("{e:#}"));
    }
    if let Err(reason) = authorize_perms_scope(&authd, &req.target_path, "read") {
        return err(reason);
    }
    // The protected local socket is deliberately useful on every resolver,
    // including satellites that do not have the CA role. It may read only the
    // host's own resolver cluster and never consults or trusts remote map hints.
    if local {
        let (server, addr) =
            state.read(move |state| (state.cfg.server_id, state.cfg.listen)).await;
        let read = handle_get_perms(state).await;
        return match read {
            GetPermsResponse::Ok(perms_json) => {
                ReadPermsResponse::Ok(ReadPermsOk { server, addr, perms_json })
            }
            GetPermsResponse::Err { reason } => err(reason),
        };
    }
    let targets = {
        let map = state.read(move |state| state.map.clone()).await;
        match cluster_members_for(&map, &req.target_path) {
            Some(targets) => targets,
            None => {
                return err(format!(
                    "no registered resolver cluster serving {:?} in the admin domain map",
                    req.target_path
                ));
            }
        }
    };
    audit(
        &state.ca_dir().await.expect("CA role held"),
        &authd.admin,
        "read-perms",
        &req.target_path,
        Duration::ZERO,
    )
    .await;
    let client = match state.outbound_client().await {
        Ok(client) => client,
        Err(e) => return err(format!("loading outbound identity: {e:#}")),
    };
    let ca = state.read(move |state| state.map.ca).await;
    let home_ca = state.home_ca_der.clone();
    let mut failures = Vec::new();
    for (server, addr) in targets {
        let result = tokio::time::timeout(
            PUSH_TIMEOUT,
            transport::pull_perms(&client, addr, server, server == ca, home_ca.clone()),
        )
        .await;
        match result {
            Ok(Ok(perms_json)) => {
                return ReadPermsResponse::Ok(ReadPermsOk { server, addr, perms_json });
            }
            Ok(Err(e)) => failures.push(format!("{server} at {addr}: {e:#}")),
            Err(_) => failures.push(format!(
                "{server} at {addr}: timed out after {}s",
                PUSH_TIMEOUT.as_secs()
            )),
        }
    }
    err(format!(
        "no registered member of resolver cluster {:?} could provide permissions: {}",
        req.target_path,
        failures.join("; ")
    ))
}

/// This member's perms path, for the version stamp and for reading back what
/// it holds.
pub(super) async fn local_perms_file(state: &Server) -> Result<PathBuf> {
    local_perms_path(state).await
}

async fn apply_perms_local(
    state: &Server,
    perms_json: &str,
    version: Option<u64>,
) -> Result<()> {
    let pmap = crate::perms::validate(perms_json).context("parsing the new perms")?;
    let config_lock = state.config_lock.clone();
    state
        .write_async(async move |state| {
            let rconfig = state
                .cfg
                .roles
                .resolver
                .as_ref()
                .map(|role| role.config.clone())
                .context("no resolver role")?;
            let rc = crate::resolver::ResolverConfig::load_async(&rconfig).await?;
            let path =
                config_lock.require_contained(perms_path_from_config(&rconfig, &rc)?)?;
            let check = rc.clone();
            let check_config = rconfig.clone();
            let check_path = path.clone();
            let prospective = pmap.clone();
            tokio::task::spawn_blocking(move || {
                check.preflight_permission_topology(
                    &check_config,
                    Some((&check_path, &prospective)),
                )
            })
            .await
            .context("permissions preflight task panicked")?
            .context("the edited perms would make the resolver config invalid")?;
            crate::perms::save_perms_async(&path, &pmap).await?;
            // Stamped only on success, so a member that failed to write stays
            // visibly behind and is picked up by the next reconcile rather
            // than claiming to be current.
            if let Some(version) = version {
                crate::version_stamp::record(&path, version).await?;
            }
            Ok(())
        })
        .await
}

/// Server-to-server receive side of a perms edit (peer-cert-gated).
pub(super) async fn handle_apply_perms_edit(
    state: &Server,
    req: &ApplyPermsEditRequest,
) -> ApplyPermsEditResponse {
    info!("admin-server: applying permissions operation {}", req.operation_id);
    match apply_perms_local(state, &req.perms_json, req.version).await {
        Ok(()) => ApplyPermsEditResponse::Ok(()),
        Err(e) => ApplyPermsEditResponse::Err { reason: format!("{e:#}") },
    }
}

/// Whether any granted scope covers `target` — target equals or descends
/// from a scope (`/` covers the whole tree). The path-aware prefix test
/// (`Path::is_parent`) won't let `/eu` match `/europe`.
fn cluster_for(
    map: &AdminDomainMap,
    target_path: &str,
) -> Option<(admin_proto::ResolverClusterId, Vec<(admin_proto::AdminServerId, SocketAddr)>)>
{
    let cluster = map.resolver_clusters.iter().find(|c| {
        c.base == target_path && c.state == admin_proto::ResolverClusterState::Active
    })?;
    let mut members: Vec<_> = map
        .admin_servers
        .iter()
        .filter(|s| {
            s.cluster == Some(cluster.id)
                && s.state == admin_proto::ServerState::Registered
        })
        .map(|s| (s.id, s.addr))
        .collect();
    members.sort_by_key(|(id, _)| *id);
    (!members.is_empty()).then_some((cluster.id, members))
}

/// The members of the cluster mounted at `target_path`.
fn cluster_members_for(
    map: &AdminDomainMap,
    target_path: &str,
) -> Option<Vec<(admin_proto::AdminServerId, SocketAddr)>> {
    cluster_for(map, target_path).map(|(_, members)| members)
}

/// Push a perms edit to every registered server in the target resolver cluster, using
/// each CA-owned routing address. Every unreachable/erroring target is returned
/// as a `PeerResult` carrying its immutable identity and current address.
async fn push_perms_edit_to_peers(
    state: &Arc<Server>,
    perms_json: &str,
    targets: &[(admin_proto::AdminServerId, SocketAddr)],
    operation_id: admin_proto::OperationId,
    version: u64,
) -> Vec<PeerResult> {
    let client = match state.outbound_client().await {
        Ok(client) => client,
        Err(e) => {
            return targets
                .iter()
                .map(|(server, addr)| PeerResult {
                    server: *server,
                    addr: *addr,
                    error: Some(format!("loading outbound identity failed: {e:#}")),
                })
                .collect();
        }
    };
    let ca = state.read(move |state| state.map.ca).await;
    let home_ca = state.home_ca_der.clone();
    let mut results: Vec<_> =
        stream::iter(targets.iter().copied().map(|(server, addr)| {
            let client = client.clone();
            let home_ca = home_ca.clone();
            async move {
                let res = tokio::time::timeout(
                    PUSH_TIMEOUT,
                    transport::push_perms_edit(
                        &client,
                        addr,
                        server,
                        server == ca,
                        home_ca,
                        operation_id,
                        perms_json,
                        Some(version),
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
    results.sort_by_key(|result| result.server);
    results
}

/// Record `perms_json` as `cluster`'s authoritative permissions, returning the
/// version it is now at.
///
/// Under the state write lock, so two concurrent edits cannot both read the
/// same version and write over each other. That lock is also what will let the
/// read-modify-write behind `perms set` collapse into the CA.
pub(super) async fn record_in_model(
    state: &Arc<Server>,
    cluster: admin_proto::ResolverClusterId,
    perms_json: &str,
) -> Result<u64> {
    let perms_json = perms_json.to_string();
    state
        .write_async(async move |state| {
            let store =
                &state.ca.as_ref().context("this host does not hold the CA")?.store;
            let mut model = store.perms_model().await?;
            let version = model.set(cluster, &perms_json)?;
            store.save_perms_model(&model).await?;
            Ok(version)
        })
        .await
}

/// CA-side: authenticate the admin, find the target resolver cluster in the map, and
/// propagate the perms edit to its admin servers (peer-cert-gated). The CA
/// never edits a foreign resolver cluster's files directly — it pushes.
pub(super) async fn handle_edit_perms(
    state: &Arc<Server>,
    req: &EditPermsRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> EditPermsResponse {
    let err = |reason: String| EditPermsResponse::Err { reason };
    if !state.has_ca().await {
        return err("a perms edit must be sent to the CA host".to_string());
    }
    let authd =
        match authenticate_perms_caller(state, &req.credential, authentication, local)
            .await
        {
            Ok(authd) => authd,
            Err(reason) => return err(reason),
        };
    if local && let Err(e) = confine_local_perms(state, &req.target_path, "edit").await {
        return err(format!("{e:#}"));
    }
    if let Err(reason) = authorize_perms_scope(&authd, &req.target_path, "edit") {
        return err(reason);
    }
    let (cluster, members) = {
        let map = state.read(move |state| state.map.clone()).await;
        match cluster_for(&map, &req.target_path) {
            Some(found) => found,
            None => {
                return err(format!(
                    "no resolver cluster serving {:?} in the admin domain map",
                    req.target_path
                ));
            }
        }
    };
    // Record what this cluster is supposed to hold before propagating any of
    // it. Until now the CA kept no copy, so a member that missed a push was a
    // candidate source of truth for the *next* edit — which is how a stale
    // document gets read back and propagated over everyone else's correct one.
    let version = match record_in_model(state, cluster, &req.perms_json).await {
        Ok(version) => version,
        Err(e) => return err(format!("{e:#}")),
    };
    let operation_id = admin_proto::OperationId::new();
    audit(
        &state.ca_dir().await.expect("CA role held"),
        &authd.admin,
        "edit-perms",
        &format!("operation {operation_id} at {}", req.target_path),
        Duration::ZERO,
    )
    .await;
    let peers =
        push_perms_edit_to_peers(state, &req.perms_json, &members, operation_id, version)
            .await;
    EditPermsResponse::Ok(PropagationOk { operation_id, peers })
}
