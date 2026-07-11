//! Remote service control over the admin plane.
//!
//! `netidx admin activation {restart,start,stop,status} --server …` controls the
//! activation units of the cluster serving a netidx path, RBAC-gated by the
//! caller's `service_control_scopes` covering that path. Only this pinned,
//! admin-authenticated **remote** path lives here — the **local** path (this
//! host's own supervisor, over its cross-platform control socket in
//! `netidx-activation`) needs no CA and works on Windows, so it stays in the
//! tools `activation` module rather than this unix-only group.

use super::{open_admin_session, resolve_identity};
use crate::{
    admin_client,
    admin_proto::{NodeKind, Role, Secret},
    answer::Answerer,
};
use anyhow::{Context, Result};
use netidx_activation::control::ControlOp;
use std::{net::SocketAddr, path::PathBuf};

/// One admin server the operator can control services on — its listen address,
/// its resolver cluster's base path (the authorization scope), and its roles.
/// The `addr` is what the UI shows and what a service-control op targets.
#[derive(Debug, Clone)]
pub struct ServiceServer {
    pub id: crate::admin_proto::AdminServerId,
    pub addr: SocketAddr,
    pub base: String,
    pub roles: Vec<Role>,
}

/// Every admin server that runs a resolver, read from the CA network map — the
/// pick list for cluster service control. Sourced entirely from the map, so the
/// operator picks a server by identity rather than typing a namespace path.
pub async fn list_service_servers(
    ans: &mut dyn Answerer,
    server: SocketAddr,
    ca_dir: Option<PathBuf>,
) -> Result<Vec<ServiceServer>> {
    let (addr, id) = resolve_identity(ans, Some(server), ca_dir.as_deref()).await?;
    let map = admin_client::get_map_pinned(addr, NodeKind::Client, &id)
        .await
        .context("fetching the network map")?;
    let mut out = Vec::new();
    for s in map.servers.iter().filter(|s| {
        s.state == crate::admin_proto::ServerState::Registered
    }) {
        if let Some(cluster) = s.cluster
            && let Some(cluster) = map.clusters.iter().find(|c| c.id == cluster)
        {
            out.push(ServiceServer {
                id: s.id,
                addr: s.addr,
                base: cluster.base.clone(),
                roles: s.roles.clone(),
            });
        }
    }
    out.sort_by(|a, b| (&a.base, a.addr).cmp(&(&b.base, b.addr)));
    Ok(out)
}

/// Remote service control over the admin plane: glyph-confirm + authenticate to
/// `server` (the CA), then apply `op` to `units` on the single admin server
/// `target_server`. The CA enforces the caller's `service_control_scopes`
/// covering that server's cluster base. Returns that server's per-unit statuses.
#[allow(clippy::too_many_arguments)]
pub async fn control_remote(
    ans: &mut dyn Answerer,
    server: SocketAddr,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    target_server: SocketAddr,
    units: Vec<String>,
    op: ControlOp,
) -> Result<Vec<crate::admin_proto::ServiceUnit>> {
    let sess = open_admin_session(ans, Some(server), ca_dir, admin, password).await?;
    let map = admin_client::get_map_pinned(
        sess.server,
        NodeKind::Client,
        &sess.identity,
    )
    .await?;
    let target_server = map
        .servers
        .iter()
        .find(|s| {
            s.addr == target_server
                && s.state == crate::admin_proto::ServerState::Registered
        })
        .map(|s| s.id)
        .context("the selected service target is not registered")?;
    admin_client::control_service(
        sess.server,
        NodeKind::Client,
        &sess.identity,
        sess.credential.clone(),
        target_server,
        units,
        op,
    )
    .await
}
