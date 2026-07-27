//! CA-authoritative admin-server inventory and permanent removal.

use super::{open_admin_session, resolve_ca};
use crate::{
    admin_proto::{
        AdminServerId, NodeKind, ResolverAddr, ResolverClusterId, ResolverClusterState,
        Role, ServerState,
    },
    answer::Answerer,
    transport,
};
use anyhow::{Context, Result, bail};
use enumflags2::BitFlags;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub id: AdminServerId,
    pub addr: SocketAddr,
    pub roles: BitFlags<Role>,
    pub state: ServerState,
    pub resolver: Option<ResolverAddr>,
    pub cluster: Option<ResolverClusterId>,
    pub cluster_base: Option<String>,
    pub cluster_state: Option<ResolverClusterState>,
    pub ca: bool,
}

/// List every server grant in the verified CA map, including enrolled
/// (not currently routing) nodes and the CA itself.
pub async fn list_servers(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
) -> Result<Vec<ServerInfo>> {
    // The bootstrap node's map is only a discovery hint. Follow it to the
    // CA and require the CA URI + exact home CA before treating
    // the returned inventory as authoritative.
    let (addr, identity) = resolve_ca(ans, server, ca_dir.as_deref()).await?;
    let map = transport::get_map_pinned(addr, NodeKind::Client, &identity)
        .await
        .context("fetching the authoritative server map")?;
    let mut rows: Vec<_> = map
        .admin_servers
        .iter()
        .map(|entry| {
            let cluster = entry.cluster.and_then(|id| {
                map.resolver_clusters.iter().find(|cluster| cluster.id == id)
            });
            ServerInfo {
                id: entry.id,
                addr: entry.addr,
                roles: entry.roles,
                state: entry.state,
                resolver: entry.resolver.clone(),
                cluster: entry.cluster,
                cluster_base: cluster.map(|cluster| cluster.base.clone()),
                cluster_state: cluster.map(|cluster| cluster.state),
                ca: entry.id == map.ca,
            }
        })
        .collect();
    rows.sort_by(|a, b| {
        (a.cluster_base.as_deref().unwrap_or("~"), a.id)
            .cmp(&(b.cluster_base.as_deref().unwrap_or("~"), b.id))
    });
    Ok(rows)
}

/// Permanently remove one server ID. Re-fetch the map after authentication so
/// a stale UI/CLI selection cannot silently target an identity that has become
/// the CA. An already-absent identity is allowed: repeating the exact
/// UUID is the manual reconciliation path after partial fanout.
pub async fn remove_server(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<netidx_admin_proto::Secret>,
    target: AdminServerId,
) -> Result<transport::RemoveServerOutcome> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    let map = transport::get_map_pinned(sess.server, NodeKind::Client, &sess.identity)
        .await
        .context("refreshing the authoritative server map")?;
    if target == map.ca {
        bail!("the active CA cannot be removed")
    }
    transport::remove_server(
        sess.server,
        NodeKind::Client,
        &sess.identity,
        sess.credential,
        target,
    )
    .await
}

/// Re-send the CA's current address, authoritative map, and CRL to all
/// registered servers. Safe to repeat after any partial result.
pub async fn reconcile_ca(
    ans: &mut dyn Answerer,
    server: Option<SocketAddr>,
    ca_dir: Option<PathBuf>,
    admin: Option<String>,
    password: Option<netidx_admin_proto::Secret>,
) -> Result<(netidx_admin_proto::OperationId, Vec<netidx_admin_proto::PeerResult>)> {
    let sess = open_admin_session(ans, server, ca_dir, admin, password).await?;
    transport::reconcile_ca(
        sess.server,
        NodeKind::Client,
        &sess.identity,
        sess.credential,
    )
    .await
}
