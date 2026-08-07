//! Remote service control over the admin plane.
//!
//! `netidx admin activation {restart,start,stop,status} --server …` controls the
//! activation units of the resolver cluster serving a netidx path, RBAC-gated by the
//! caller's `service_control_scopes` covering that path. Only this pinned,
//! admin-authenticated **remote** path lives here — the **local** path (this
//! host's own supervisor, over its cross-platform control socket in
//! `netidx-activation`) needs no CA and works on Windows, so it stays in the
//! tools `activation` module rather than this unix-only group.

use super::open_admin_session;
use crate::{
    admin_proto::{AdminServerId, Role, Secret, ServerState},
    answer::Answerer,
    transport,
};
use anyhow::{Context, Result};
use enumflags2::BitFlags;
use netidx_activation::control::ControlOp;
use std::{net::SocketAddr, path::PathBuf};

/// One admin server the operator can control services on — its listen address,
/// its resolver cluster's base path (the authorization scope), and its roles.
/// The UI shows `addr`, but a service-control op always targets immutable `id`.
#[derive(Debug, Clone)]
pub struct ServiceServer {
    pub id: netidx_admin_proto::AdminServerId,
    pub addr: SocketAddr,
    pub base: String,
    pub roles: BitFlags<Role>,
}

/// Every admin server that runs a resolver, read from the CA admin domain map — the
/// pick list for resolver cluster service control. Sourced entirely from the map, so the
/// operator picks a server by identity rather than typing a namespace path.
pub async fn list_service_servers(
    ans: &mut dyn Answerer,
    server: SocketAddr,
    ca_dir: Option<PathBuf>,
) -> Result<Vec<ServiceServer>> {
    let servers = super::servers::list_servers(ans, Some(server), ca_dir).await?;
    let mut out: Vec<_> = servers
        .into_iter()
        .filter_map(|s| {
            if s.state != ServerState::Registered || !s.roles.contains(Role::Resolver) {
                return None;
            }
            Some(ServiceServer {
                id: s.id,
                addr: s.addr,
                base: s.cluster_base?,
                roles: s.roles,
            })
        })
        .collect();
    out.sort_by(|a, b| (&a.base, a.addr, a.id).cmp(&(&b.base, b.addr, b.id)));
    Ok(out)
}

/// Remote service control over the admin plane: glyph-confirm + authenticate to
/// `server` (the CA), then apply `op` to `units` on the single immutable admin
/// server identity `target_server`. The CA enforces the caller's `service_control_scopes`
/// covering that server's resolver cluster base. Returns that server's per-unit statuses.
#[allow(clippy::too_many_arguments)]
pub async fn control_remote(
    ans: &mut dyn Answerer,
    server: SocketAddr,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    target_server: AdminServerId,
    units: Vec<String>,
    op: ControlOp,
) -> Result<Vec<netidx_admin_proto::ServiceUnit>> {
    let sess = open_admin_session(ans, Some(server), ca_dir, admin, password).await?;
    transport::control_service(
        sess.server,
        netidx_admin_proto::NodeKind::Client,
        &sess.identity,
        sess.credential.clone(),
        target_server,
        units,
        op,
    )
    .await
}

/// Open or shut one member's read gate over the admin plane. Same session,
/// same target, same scope as `control_remote` — a resolver that is running
/// but not answering is a service-control state like any other.
pub async fn set_read_gate(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<Secret>,
    target_server: AdminServerId,
    gate: netidx::resolver_server::config::ReadGate,
) -> Result<()> {
    // Which admin server to ask is the library's decision, not a frontend's,
    // and it is the same decision for all three of them.
    let server = crate::ops::own_admin_server_addr(server)?;
    let sess = open_admin_session(ans, Some(server), ca_dir, admin, password).await?;
    // State the risk here, where the member's *current* gate is actually
    // known. A caller that addresses a member by id has no way to compute it
    // without this round trip, and the reading that matters most — opening one
    // that is still filling — is invisible without it.
    let map = transport::get_map_pinned(
        sess.server,
        netidx_admin_proto::NodeKind::Client,
        &sess.identity,
    )
    .await
    .context("fetching the admin domain map to describe the gate change")?;
    if let Some(entry) = map.admin_servers.iter().find(|s| s.id == target_server)
        && let Some(risk) = crate::ops::servers::read_gate_warning(
            target_server,
            Some(entry.addr),
            gate,
            entry.reported_read_gate,
        )
    {
        ans.warn(risk.split("\n\n").next().unwrap_or(&risk));
    }
    transport::set_read_gate(
        sess.server,
        netidx_admin_proto::NodeKind::Client,
        &sess.identity,
        sess.credential.clone(),
        target_server,
        gate,
    )
    .await
}
