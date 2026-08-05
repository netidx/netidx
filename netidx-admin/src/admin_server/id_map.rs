//! Map-routed id-map administration.
//!
//! The shape mirrors [`super::permissions`] — authenticate at the CA, push to
//! every host that holds the state, report per-peer results — with one
//! deliberate difference: perms propagate as a whole document, an id-map
//! propagates as an *operation*.
//!
//! That is forced by the data: a uid is allocated locally by the receiving
//! host, because perms are keyed on *names* and the number is a per-host
//! detail. Shipping a document would impose one host's uids on every other.
//! Shipping the operation lets each host allocate its own, and costs nothing:
//! there is no read-modify-write, so no lost update, and a retry after a
//! partial push is a no-op where it landed.
//!
//! The fanout target is every registered admin server holding `Role::IdMap`,
//! which is the same set the CA pushes a registration to after it signs an
//! identity — see [`super::issuance::id_map_targets`], shared so the two
//! cannot drift.

#[cfg(test)]
#[path = "id_map_tests.rs"]
mod tests;

use super::{
    PUSH_TIMEOUT, Server, audit,
    auth::{
        PreparedAdminAuthentication, authenticate, local_superuser, name_permitted,
        safe_auth_failure,
    },
    issuance::id_map_targets,
};
use crate::{
    admin_proto::{
        self, ApplyIdMapEditOk, ApplyIdMapEditRequest, ApplyIdMapEditResponse,
        EditIdMapRequest, EditIdMapResponse, GetIdMapOk, GetIdMapRequest,
        GetIdMapResponse, IdMapEdit, IdMapPropagationOk, PeerResult,
    },
    ca_vault,
    config_lock::ConfigDirLock,
    id_map, transport,
};
use anyhow::{Context, Result, anyhow};
use futures::{StreamExt, stream};
use log::info;
use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};

/// Apply `edit` to the map at `map_path`. The single local mutation path:
/// the CA's registration push after it signs an identity, an operator's edit,
/// and the CA's own apply all reach the map through here.
///
/// A missing file starts from the empty map — a zero-touch join must work on
/// a host whose id-map daemon has never registered anyone.
pub(super) async fn apply_edit_local(
    config_lock: &ConfigDirLock,
    map_path: &Path,
    edit: &IdMapEdit,
) -> Result<ApplyIdMapEditOk> {
    let map_path = config_lock.require_contained(map_path)?;
    let mut map =
        id_map::load_or_empty_async(&map_path).await.context("loading id-map")?;
    let changed = id_map::apply_edit(&mut map, edit)?;
    if changed {
        id_map::save_async(&map_path, &map).await.context("saving id-map")?;
    }
    // Read back rather than trusting the edit: on a re-registration the host
    // keeps the uid it already had, which is the number worth reporting.
    let uid = edit.identity().and_then(|san| map.identities.get(san)).map(|i| i.uid);
    Ok(ApplyIdMapEditOk { changed, uid })
}

/// Server-to-server receive side (peer-cert-gated).
pub(super) async fn handle_apply_id_map_edit(
    state: &Server,
    req: &ApplyIdMapEditRequest,
) -> ApplyIdMapEditResponse {
    info!("admin-server: applying id-map operation {}", req.operation_id);
    match state.apply_id_map_edit(&req.edit).await {
        Ok(ok) => ApplyIdMapEditResponse::Ok(ok),
        Err(e) => ApplyIdMapEditResponse::Err { reason: format!("{e:#}") },
    }
}

async fn authenticate_id_map_caller(
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

/// An id-map edit is bounded by the two scopes the admin already has for
/// identities: every group it names must be within `id_map_groups`, and the
/// identity it touches within `allowed_san`.
///
/// The second is what stops a scoped admin from rewriting the group
/// membership — and therefore the permissions — of a node it could never have
/// signed. A signing slot (the on-box superuser, recovery, autorenew) holds
/// authority over everything, as everywhere else.
fn authorize_id_map_edit(
    authd: &ca_vault::Authenticated,
    edit: &IdMapEdit,
) -> std::result::Result<(), String> {
    if authd.kind == netidx_admin_proto::policy::SlotKind::Signing {
        return Ok(());
    }
    for g in edit.groups() {
        match name_permitted(g, &authd.policy.id_map_groups) {
            Ok(true) => (),
            Ok(false) => {
                return Err(format!(
                    "id-map group {g:?} is not permitted for admin {}; allowed: {:?}",
                    authd.admin, authd.policy.id_map_groups
                ));
            }
            Err(e) => return Err(format!("{e:#}")),
        }
    }
    if let Some(san) = edit.identity() {
        match name_permitted(san, &authd.policy.allowed_san) {
            Ok(true) => (),
            Ok(false) => {
                return Err(format!(
                    "identity {san:?} is not within the issuance scope of admin {}",
                    authd.admin
                ));
            }
            Err(e) => return Err(format!("{e:#}")),
        }
    }
    Ok(())
}

/// Admin → CA id-map read. One host's map: see the module docs on why a
/// merged view would be a lie.
pub(super) async fn handle_get_id_map(
    state: &Arc<Server>,
    req: &GetIdMapRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> GetIdMapResponse {
    let err = |reason: String| GetIdMapResponse::Err { reason };
    if let Err(reason) =
        authenticate_id_map_caller(state, &req.credential, authentication, local).await
    {
        return err(reason);
    }
    let (server, addr) =
        state.read(|state| (state.cfg.server_id, state.cfg.listen)).await;
    match state.read_id_map().await {
        Ok(id_map_json) => GetIdMapResponse::Ok(GetIdMapOk { server, addr, id_map_json }),
        Err(e) => err(format!("{e:#}")),
    }
}

/// Admin → CA id-map edit: authorize, apply locally when this host holds the
/// role, and push to every other id-map host.
pub(super) async fn handle_edit_id_map(
    state: &Arc<Server>,
    req: &EditIdMapRequest,
    authentication: &PreparedAdminAuthentication,
    local: bool,
) -> EditIdMapResponse {
    let err = |reason: String| EditIdMapResponse::Err { reason };
    if !state.has_ca().await {
        return err("an id-map edit must be sent to the CA host".to_string());
    }
    let authd =
        match authenticate_id_map_caller(state, &req.credential, authentication, local)
            .await
        {
            Ok(authd) => authd,
            Err(reason) => return err(reason),
        };
    if let Err(reason) = authorize_id_map_edit(&authd, &req.edit) {
        return err(reason);
    }
    let (my_id, targets) = {
        let map = state.read(move |state| state.map.clone()).await;
        let my_id = state.read(|state| state.cfg.server_id).await;
        (my_id, id_map_targets(&map))
    };
    if targets.is_empty() {
        return err("no registered id-map host in the admin domain map".to_string());
    }
    let operation_id = admin_proto::OperationId::new();
    audit(
        &state.ca_dir().await.expect("CA role held"),
        &authd.admin,
        "edit-id-map",
        &format!("operation {operation_id}: {:?}", req.edit),
        Duration::ZERO,
    )
    .await;
    let mut peers = Vec::with_capacity(targets.len());
    let mut changed = false;
    // Local first, with no TLS loopback — the same order enrollment uses.
    if let Some((server, addr)) = targets.iter().copied().find(|(id, _)| *id == my_id) {
        let error = match state.apply_id_map_edit(&req.edit).await {
            Ok(ok) => {
                changed |= ok.changed;
                None
            }
            Err(e) => Some(format!("{e:#}")),
        };
        peers.push(PeerResult { server, addr, error });
    }
    let remote = push_id_map_edit_to_peers(
        state,
        &req.edit,
        &targets.iter().copied().filter(|(id, _)| *id != my_id).collect::<Vec<_>>(),
        operation_id,
    )
    .await;
    changed |= remote.iter().any(|(_, c)| *c);
    peers.extend(remote.into_iter().map(|(p, _)| p));
    peers.sort_by_key(|p| p.server);
    EditIdMapResponse::Ok(IdMapPropagationOk { operation_id, peers, changed })
}

/// Push `edit` to each target, pairing every result with whether that host
/// changed. A host that failed reports `false` — it is not evidence either way,
/// and its `PeerResult` already says the propagation is incomplete.
async fn push_id_map_edit_to_peers(
    state: &Arc<Server>,
    edit: &IdMapEdit,
    targets: &[(admin_proto::AdminServerId, SocketAddr)],
    operation_id: admin_proto::OperationId,
) -> Vec<(PeerResult, bool)> {
    let client = match state.outbound_client().await {
        Ok(client) => client,
        Err(e) => {
            return targets
                .iter()
                .map(|(server, addr)| {
                    (
                        PeerResult {
                            server: *server,
                            addr: *addr,
                            error: Some(format!(
                                "loading outbound identity failed: {e:#}"
                            )),
                        },
                        false,
                    )
                })
                .collect();
        }
    };
    let ca = state.read(move |state| state.map.ca).await;
    let home_ca = state.home_ca_der.clone();
    stream::iter(targets.iter().copied().map(|(server, addr)| {
        let client = client.clone();
        let home_ca = home_ca.clone();
        async move {
            let res = tokio::time::timeout(
                PUSH_TIMEOUT,
                transport::push_id_map_edit(
                    &client,
                    addr,
                    server,
                    server == ca,
                    home_ca,
                    operation_id,
                    edit,
                ),
            )
            .await
            .map_err(|_| anyhow!("timed out after {}s", PUSH_TIMEOUT.as_secs()))
            .and_then(|r| r);
            match res {
                Ok(Some(ok)) => (PeerResult { server, addr, error: None }, ok.changed),
                // The map said this host holds the id-map role and its hello
                // says otherwise. Report it rather than counting it as applied.
                Ok(None) => (
                    PeerResult {
                        server,
                        addr,
                        error: Some(
                            "the admin domain map grants this server the id-map role \
                             but it does not advertise it"
                                .to_string(),
                        ),
                    },
                    false,
                ),
                Err(e) => {
                    (PeerResult { server, addr, error: Some(format!("{e:#}")) }, false)
                }
            }
        }
    }))
    .buffer_unordered(32)
    .collect()
    .await
}
