//! What the CA says a server's configuration should be, and how a server gets
//! there.
//!
//! The admin plane has one direction: the CA is authoritative, and a member
//! converges on it by polling. That poll already exists — every admin server
//! registers with the CA every 30s, reporting the versions of the state it
//! holds — so this module is only the two halves that hang off it.
//! [`updates_for`] runs at the CA and answers "what is this server missing";
//! [`apply`] runs at the member and installs it.
//!
//! Nothing is pushed. A member that was down for an edit is afterwards just a
//! member behind a version, and the next register repairs it without an
//! operator doing anything and without the CA needing to reach inward.

use super::Server;
use crate::admin_proto::{
    AdminServerId, DesiredUpdate, RegisterRequest, Role, VersionedIdMap, VersionedPerms,
};
use anyhow::{Context, Result};
use std::sync::Arc;

/// CA-side: what `server` does not yet have, given what it reported.
///
/// Cheap in the steady state — a server whose reported versions match the
/// models costs two integer comparisons and no file reads. That matters
/// because this runs on every register from every server.
pub(crate) async fn updates_for(
    state: &Arc<Server>,
    server: AdminServerId,
    req: &RegisterRequest,
) -> Result<DesiredUpdate> {
    let (cluster, holds_id_map) = state
        .read(move |state| {
            state
                .map
                .admin_servers
                .iter()
                .find(|s| s.id == server)
                .map(|s| (s.cluster, s.roles.contains(Role::IdMap)))
        })
        .await
        .with_context(|| format!("server {server} has no approved enrollment grant"))?;
    let mut updates = DesiredUpdate::default();
    if let Some(cluster) = cluster {
        let reported = req.perms_version;
        updates.perms = state
            .read_async(async move |state| match state.ca.as_ref() {
                Some(ca) => ca.store.perms_model().await.ok(),
                None => None,
            })
            .await
            .and_then(|model| {
                model.behind(cluster, reported).then(|| {
                    let p = model.get(cluster).expect("behind implies established");
                    VersionedPerms { version: p.version, perms: p.perms.clone() }
                })
            });
    }
    if holds_id_map {
        let reported = req.id_map_version;
        updates.id_map = state
            .read_async(async move |state| match state.ca.as_ref() {
                Some(ca) => ca.store.id_map_model().await.ok(),
                None => None,
            })
            .await
            .and_then(|model| {
                let behind = model.established()
                    && reported.is_none_or(|have| have < model.version);
                behind.then(|| VersionedIdMap {
                    version: model.version,
                    id_map: model.shape().clone(),
                })
            });
    }
    Ok(updates)
}

/// The CA bringing itself up to its own models.
///
/// Same two halves as everyone else — work out what is missing, install it —
/// with the round trip removed. Going through [`updates_for`] and [`apply`]
/// rather than a shortcut is the point: a second path here is how the host
/// holding the models would come to disagree with them.
pub(crate) async fn converge_self(state: &Arc<Server>) -> Result<()> {
    let (server, addr) =
        state.read(|state| (state.cfg.server_id, state.cfg.listen)).await;
    let req = RegisterRequest {
        addr,
        resolver: None,
        id_map_version: state.applied_id_map_version().await,
        perms_version: state.applied_perms_version().await,
    };
    let updates = updates_for(state, server, &req).await?;
    if updates.is_empty() { Ok(()) } else { apply(state, &updates).await }
}

/// Derive this host's resolver topology from the map and write it.
///
/// No wire message and no payload: the topology block of a resolver config is
/// a pure function of the admin domain map, and every admin server already
/// polls that map. So a member computes its own — which means a member that
/// was down for a topology change picks it up on the poll it was making
/// anyway, and a CA that cannot reach inward is no longer a problem for it.
///
/// Idempotent, and cheap when nothing changed: the apply is a no-op that
/// rewrites nothing when the config already says this.
pub(crate) async fn apply_topology_from_map(state: &Arc<Server>) -> Result<()> {
    let (server, resolver_config) = state
        .read(|state| {
            (
                state.cfg.server_id,
                state.cfg.roles.resolver.as_ref().map(|r| r.config.clone()),
            )
        })
        .await;
    let Some(resolver_config) = resolver_config else { return Ok(()) };
    let edit = state
        .read(move |state| super::topology::topology_for_server(&state.map, server))
        .await;
    let Some(edit) = edit else { return Ok(()) };
    super::apply_referral_edit_local(&state.config_lock, &resolver_config, &edit)
        .await
        .context("applying the topology this host's map says it should have")
}

/// Member-side: install what the CA handed back.
///
/// Each part is applied and stamped on its own. A part that fails leaves its
/// version unstamped, so the server reports itself still behind on that part
/// and only that part — the next register brings it again. Reporting the
/// failure rather than swallowing it is what gets an operator an error instead
/// of a server that quietly never converges.
pub(crate) async fn apply(state: &Arc<Server>, updates: &DesiredUpdate) -> Result<()> {
    let DesiredUpdate { perms, id_map } = updates;
    let mut failures = Vec::new();
    if let Some(p) = perms
        && let Err(e) =
            super::permissions::install_perms(state, &p.perms, Some(p.version)).await
    {
        failures.push(format!("permissions (version {}): {e:#}", p.version));
    }
    if let Some(m) = id_map
        && let Err(e) = super::id_map::install_id_map(state, &m.id_map, m.version).await
    {
        failures.push(format!("id-map (version {}): {e:#}", m.version));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("applying the configuration the CA handed back: {}", failures.join("; "))
    }
}
