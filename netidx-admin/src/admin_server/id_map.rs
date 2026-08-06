//! Map-routed id-map administration.
//!
//! The shape mirrors [`super::permissions`]: an operator's edit is authorized
//! at the CA and recorded in a model, and every host holding the id-map role
//! converges on that model at its next register. Nothing is pushed.
//!
//! An edit names an *operation* ([`IdMapEdit`]) rather than a document, but
//! only as the operator's verb — it is applied to the model here, once and
//! centrally, and what reaches a host is the resulting document. That means an
//! impossible edit is refused in one place instead of being discovered
//! separately by every host.

#[cfg(test)]
#[path = "id_map_tests.rs"]
mod tests;

use super::{
    Server, audit,
    auth::{
        PreparedAdminAuthentication, authenticate, local_superuser, name_permitted,
        safe_auth_failure,
    },
};
use crate::{
    admin_proto::{
        self, ApplyIdMapEditOk, ApplyIdMapEditRequest, ApplyIdMapEditResponse,
        EditIdMapRequest, EditIdMapResponse, GetIdMapOk, GetIdMapRequest,
        GetIdMapResponse, IdMapEdit,
    },
    ca_vault,
    config_lock::ConfigDirLock,
    id_map,
};
use anyhow::{Context, Result};
use log::info;
use std::{path::Path, sync::Arc, time::Duration};

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
    version: Option<u64>,
) -> Result<ApplyIdMapEditOk> {
    let map_path = config_lock.require_contained(map_path)?;
    let mut map =
        id_map::load_or_empty_async(&map_path).await.context("loading id-map")?;
    let changed = id_map::apply_edit(&mut map, edit)?;
    if changed {
        id_map::save_async(&map_path, &map).await.context("saving id-map")?;
    }
    // Record the version even when nothing changed: this host now reflects
    // that model version either way, and leaving it behind would earn it a
    // reconcile every poll forever. Only on success — a host that failed to
    // apply must stay behind so the next reconcile picks it up.
    if let Some(version) = version {
        crate::version_stamp::record(&map_path, version).await?;
    }
    Ok(ApplyIdMapEditOk { changed })
}

/// Write `shape` into this host's id-map, stamping `version` on success.
///
/// The whole document, not a diff. A map holds names only — see the schema
/// docs on [`netidx_id_map::file::IdMap`] — so every host that holds the
/// id-map role holds the identical file, and being behind is fully described
/// by a version rather than by working out which operations are missing.
pub(crate) async fn install_id_map(
    state: &Server,
    shape: &id_map::IdMap,
    version: u64,
) -> Result<()> {
    let path = state
        .read(move |state| state.cfg.roles.id_map.as_ref().map(|r| r.map.clone()))
        .await
        .context("this host has no id-map role — nothing to install into")?;
    let path = state.config_lock.require_contained(path)?;
    id_map::save_async(&path, shape).await.context("saving id-map")?;
    crate::version_stamp::record(&path, version).await
}

/// Apply `edit` to the CA's authoritative model and persist it, returning the
/// version the admin domain is now at.
///
/// The read-modify-write runs under the state write lock so two concurrent
/// edits can't both read the same version and write over each other — the
/// model is the one piece of id-map state that *is* shared, so it is the one
/// place the operation-based design still needs serializing.
pub(super) async fn record_in_model(
    state: &Arc<Server>,
    edit: &IdMapEdit,
) -> Result<(u64, bool)> {
    let edit = edit.clone();
    let config_lock = state.config_lock.clone();
    let ca_dir = state.ca_dir().await.context("this host does not hold the CA")?;
    state
        .write_async(async move |state| {
            let store =
                &state.ca.as_ref().context("this host does not hold the CA")?.store;
            let mut model = store.id_map_model().await?;
            let changed = model.apply(&edit)?;
            let version = model.version;
            if changed {
                store.save_id_map_model(&model).await?;
                if crate::admin_domain::set_id_map_version(&mut state.map, version) {
                    crate::admin_domain::save_async(&config_lock, &ca_dir, &state.map)
                        .await?;
                }
            }
            Ok((version, changed))
        })
        .await
}

/// CA → node id-map read (peer-cert-gated), the receive side of a reconcile.
pub(super) async fn handle_get_local_id_map(
    state: &Server,
) -> admin_proto::GetLocalIdMapResponse {
    match state.read_id_map().await {
        Ok(map) => admin_proto::GetLocalIdMapResponse::Ok(map),
        Err(e) => admin_proto::GetLocalIdMapResponse::Err { reason: format!("{e:#}") },
    }
}

/// Server-to-server receive side (peer-cert-gated).
pub(super) async fn handle_apply_id_map_edit(
    state: &Server,
    req: &ApplyIdMapEditRequest,
) -> ApplyIdMapEditResponse {
    info!("admin-server: applying id-map operation {}", req.operation_id);
    match state.apply_id_map_edit(&req.edit, req.version).await {
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
        Ok(id_map) => GetIdMapResponse::Ok(GetIdMapOk { server, addr, id_map }),
        Err(e) => err(format!("{e:#}")),
    }
}

/// Admin → CA id-map edit: authorize it and record it in the model.
///
/// Recording it is the whole edit. Every host holding the id-map role reports
/// the version it has on the register it already makes, and the CA hands back
/// the document when it is behind — so a host that was down for this edit is
/// afterwards just a host behind a version.
///
/// Applying to the model here also refuses an impossible edit once, centrally,
/// rather than leaving every host to discover it separately.
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
    let (version, changed) = match record_in_model(state, &req.edit).await {
        Ok(recorded) => recorded,
        Err(e) => return err(format!("{e:#}")),
    };
    audit(
        &state.ca_dir().await.expect("CA role held"),
        &authd.admin,
        "edit-id-map",
        &format!("version {version}: {:?}", req.edit),
        Duration::ZERO,
    )
    .await;
    EditIdMapResponse::Ok(admin_proto::RecordedOk { version, changed })
}
