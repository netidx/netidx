//! The Cluster tab's known-cluster registry: admin clusters this machine has
//! connected to or discovered, persisted to the config dir so commonly-used
//! clusters reappear without retyping an address.
//!
//! A cluster is identified by its **CA fingerprint** (one CA identity, reachable
//! at one or more admin-server addresses). Before a saved cluster is shown, an
//! on-entry poll re-fetches the identity at its saved addresses and confirms the
//! fingerprint still matches — so an address reused by a *different* CA on a
//! different network (`192.168.1.1:4565` is the same on every LAN) can never
//! masquerade as a cluster you trusted elsewhere.

use futures::future::join_all;
use netidx_admin::{admin_client::fetch_identity, admin_proto::NodeKind, fingerprint::Fingerprint, paths};
use serde_derive::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

/// Per-address poll timeout — a down or firewalled cluster must not stall the
/// whole poll.
const POLL_TIMEOUT: Duration = Duration::from_secs(3);

/// One known admin cluster — a single CA identity reachable at one or more
/// admin-server addresses. Persisted as JSON; the fingerprint is stored in its
/// grouped-base32 text form ([`Fingerprint`] has no serde derive).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct KnownCluster {
    pub(super) domain: String,
    /// The CA fingerprint text (the cluster's identity). Compared against a live
    /// fetch before the cluster is shown or connected to.
    pub(super) fingerprint: String,
    /// Admin-server addresses seen for this cluster's members, tried in order.
    pub(super) addrs: Vec<SocketAddr>,
}

impl KnownCluster {
    /// The parsed CA fingerprint, or `None` if the stored text is corrupt.
    pub(super) fn fp(&self) -> Option<Fingerprint> {
        Fingerprint::parse_text(&self.fingerprint).ok()
    }
}

/// The persisted set of known clusters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct KnownClusters {
    #[serde(default)]
    pub(super) clusters: Vec<KnownCluster>,
}

impl KnownClusters {
    fn path() -> anyhow::Result<PathBuf> {
        Ok(paths::user_config_root()?.join("admin-clusters.json"))
    }

    /// Load the saved clusters, or an empty set if the file is missing or
    /// unreadable — a corrupt registry must never break the Cluster tab.
    pub(super) fn load() -> KnownClusters {
        let Ok(path) = Self::path() else { return KnownClusters::default() };
        match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => KnownClusters::default(),
        }
    }

    /// Persist atomically: write a sibling temp file, then rename over.
    pub(super) fn save(&self) -> anyhow::Result<()> {
        use anyhow::Context;
        let path = Self::path()?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("creating {}", dir.display()))?;
        }
        let tmp = path.with_extension("json.tmp");
        let body = serde_json::to_vec_pretty(self).context("serializing known clusters")?;
        std::fs::write(&tmp, &body).with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("renaming into {}", path.display()))?;
        Ok(())
    }

    /// Record a confirmed cluster: merge `addr` into the entry with a matching
    /// fingerprint (one cluster = one CA identity, possibly several members), or
    /// append a new entry. Returns whether anything changed (worth saving).
    pub(super) fn upsert(&mut self, domain: &str, addr: SocketAddr, fp: Fingerprint) -> bool {
        let fp_text = fp.text();
        match self.clusters.iter_mut().find(|c| c.fingerprint == fp_text) {
            Some(c) => {
                let mut changed = false;
                if !c.addrs.contains(&addr) {
                    c.addrs.push(addr);
                    changed = true;
                }
                if c.domain != domain {
                    c.domain = domain.to_string();
                    changed = true;
                }
                changed
            }
            None => {
                self.clusters.push(KnownCluster {
                    domain: domain.to_string(),
                    fingerprint: fp_text,
                    addrs: vec![addr],
                });
                true
            }
        }
    }
}

/// If this host runs its own admin server — i.e. it founded or joined a cluster
/// and hosts a member of it — make sure that cluster is in the saved set, so it
/// appears on the Cluster tab without a manual discover. The identity is the one
/// recorded in this host's install record (`network`); the admin-server address
/// is this host's own listen address (a CA host records no upstream one). The
/// on-entry poll then verifies it live like any other saved cluster. Returns
/// whether the saved set changed (worth saving).
#[cfg(unix)]
pub(super) fn seed_local_cluster(clusters: &mut KnownClusters) -> bool {
    let Some(addr) = netidx_admin::admin_ops::local_admin_server_listen() else {
        return false;
    };
    match local_cluster_identity() {
        Some((domain, fp)) => clusters.upsert(&domain, addr, fp),
        None => false,
    }
}

/// The admin cluster this host belongs to (domain + CA fingerprint), from its
/// install record — the user-scope record, else the system-scope one.
#[cfg(unix)]
fn local_cluster_identity() -> Option<(String, Fingerprint)> {
    use netidx_admin::provenance::InstallRecord;
    let sys = paths::system_install_record();
    let records = [
        InstallRecord::load_default().ok().flatten(),
        sys.exists().then(|| InstallRecord::load(&sys).ok()).flatten(),
    ];
    records
        .into_iter()
        .flatten()
        .find_map(|r| r.network)
        .and_then(|n| Fingerprint::parse_text(&n.ca_fingerprint).ok().map(|fp| (n.domain, fp)))
}

#[cfg(not(unix))]
pub(super) fn seed_local_cluster(_clusters: &mut KnownClusters) -> bool {
    false
}

/// Where an on-entry poll of a known cluster landed.
#[derive(Debug, Clone, Copy)]
pub(super) enum PollState {
    /// Not yet polled — the event loop launches one poll pass per pending set.
    Unpolled,
    /// A poll is in flight.
    Polling,
    /// Reachable at `addr` with the saved CA identity — safe to show + connect.
    Present { addr: SocketAddr },
    /// No address answered with the saved fingerprint (down, or the address is
    /// now a different cluster). Hidden from the list.
    Absent,
}

impl PollState {
    /// The verified address to connect to, when this cluster polled `Present`.
    pub(super) fn present_addr(&self) -> Option<SocketAddr> {
        match self {
            PollState::Present { addr } => Some(*addr),
            _ => None,
        }
    }
}

/// Poll each pending known cluster concurrently: try its addresses in order and
/// accept the first that answers with the saved CA fingerprint. A live-but-
/// different fingerprint (an address reused by another CA) is skipped, so a
/// cluster is only `Present` when its identity actually verifies. Self-contained
/// (owns its inputs) so the event loop can poll it as a background future.
pub(super) async fn poll_clusters(
    pending: Vec<(usize, KnownCluster)>,
) -> Vec<(usize, PollState)> {
    join_all(pending.into_iter().map(|(i, cluster)| async move {
        let want = cluster.fp();
        let mut state = PollState::Absent;
        for addr in &cluster.addrs {
            let fetched =
                tokio::time::timeout(POLL_TIMEOUT, fetch_identity(*addr, NodeKind::Client)).await;
            if let Ok(Ok(id)) = fetched
                && want == Some(id.fingerprint)
            {
                state = PollState::Present { addr: *addr };
                break;
            }
        }
        (i, state)
    }))
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_dedups_by_fingerprint() {
        let a = Fingerprint::of_der(b"cluster a spki");
        let b = Fingerprint::of_der(b"cluster b spki");
        let mut kc = KnownClusters::default();
        let addr1: SocketAddr = "10.0.0.1:4565".parse().unwrap();
        let addr2: SocketAddr = "10.0.0.2:4565".parse().unwrap();
        // First sighting of cluster a.
        assert!(kc.upsert("hq.local", addr1, a));
        assert_eq!(kc.clusters.len(), 1);
        // A second member of the *same* cluster merges its address in.
        assert!(kc.upsert("hq.local", addr2, a));
        assert_eq!(kc.clusters.len(), 1);
        assert_eq!(kc.clusters[0].addrs, vec![addr1, addr2]);
        // Re-seeing a known member is a no-op.
        assert!(!kc.upsert("hq.local", addr2, a));
        // A different CA identity is a distinct cluster, even at a shared address.
        assert!(kc.upsert("eu.local", addr1, b));
        assert_eq!(kc.clusters.len(), 2);
    }
}
