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
use chrono::Utc;
use enumflags2::BitFlags;
use netidx::resolver_server::config::ReadGate;
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
    /// What this host last told the CA its own resolver config says, or `None`
    /// if it runs no resolver or has not reported yet. Reported state, not the
    /// CA's intent — a gate the CA pushed but that never landed shows here as
    /// it really is.
    pub read_gate: Option<ReadGate>,
}

/// One column's worth of read-gate status: short enough for a list row, and
/// relative, because the question an operator has is "how long until it starts
/// answering", not "at what instant".
pub fn read_gate_label(gate: Option<ReadGate>) -> String {
    match gate {
        None => "-".to_string(),
        Some(ReadGate::No) => "open".to_string(),
        Some(ReadGate::Yes) => "shut".to_string(),
        Some(ReadGate::Until(t)) => match (t - Utc::now()).to_std() {
            Err(_) => "open (expired)".to_string(),
            Ok(left) => format!("shut {}", humantime::format_duration(round(left))),
        },
    }
}

/// The same status in full, for a detail pane: the exact instant the gate
/// opens is what you need when deciding whether to open it early.
pub fn read_gate_detail(gate: Option<ReadGate>) -> String {
    match gate {
        None => "-".to_string(),
        Some(ReadGate::No) => "open — answering read clients".to_string(),
        Some(ReadGate::Yes) => "shut — refusing read clients until opened".to_string(),
        Some(g @ ReadGate::Until(t)) => {
            let t = t.format("%Y-%m-%d %H:%M:%SZ");
            if g.is_open() {
                format!("open — the gate expired at {t}")
            } else {
                format!("shut — refusing read clients until {t}")
            }
        }
    }
}

/// Whole seconds, then whole minutes above an hour: `humantime` will otherwise
/// render every remaining nanosecond.
fn round(d: std::time::Duration) -> std::time::Duration {
    let secs = d.as_secs();
    std::time::Duration::from_secs(if secs >= 3600 { secs - secs % 60 } else { secs })
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
                read_gate: entry.reported_read_gate,
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn a_gate_is_described_as_open_or_shut_never_as_yes_or_no() {
        // "read gated: yes" leaves the reader working out whether the gate is
        // on or the reads are. Neither surface says it.
        for gate in [None, Some(ReadGate::No), Some(ReadGate::Yes)] {
            let (label, detail) = (read_gate_label(gate), read_gate_detail(gate));
            for s in [&label, &detail] {
                assert!(!s.contains("yes") && !s.contains("no "), "{s:?}");
            }
        }
        assert_eq!(read_gate_label(None), "-");
        assert_eq!(read_gate_label(Some(ReadGate::No)), "open");
        assert_eq!(read_gate_label(Some(ReadGate::Yes)), "shut");
    }

    #[test]
    fn a_live_deadline_reads_as_time_left_and_a_passed_one_as_open() {
        let live = Some(ReadGate::Until(Utc::now() + Duration::minutes(30)));
        let label = read_gate_label(live);
        assert!(label.starts_with("shut "), "{label:?}");
        assert!(label.contains("29m") || label.contains("30m"), "{label:?}");
        assert!(
            read_gate_detail(live).contains("until 20"),
            "{:?}",
            read_gate_detail(live)
        );

        // An expired Until stays in the config — the resolver never rewrites
        // its own file — so both surfaces have to read it as open.
        let past = Some(ReadGate::Until(Utc::now() - Duration::minutes(1)));
        assert_eq!(read_gate_label(past), "open (expired)");
        assert!(
            read_gate_detail(past).starts_with("open"),
            "{:?}",
            read_gate_detail(past)
        );
    }
}
