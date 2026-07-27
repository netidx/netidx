//! The Admin domain tab's known-admin domain registry: admin admin domains this machine has
//! connected to or discovered, persisted to the config dir so commonly-used
//! admin domains reappear without retyping an address.
//!
//! A admin domain is identified by its **CA fingerprint** (one CA identity, reachable
//! at one or more admin-server addresses). Before a saved admin domain is shown, an
//! on-entry poll re-fetches the identity at its saved addresses and confirms the
//! fingerprint still matches — so an address reused by a *different* CA on a
//! different admin domain (`192.168.1.1:4565` is the same on every LAN) can never
//! masquerade as a admin domain you trusted elsewhere.

use futures::future::join_all;
use netidx_admin_client::{paths, transport::fetch_identity};
use netidx_admin_proto::{NodeKind, fingerprint::Fingerprint};
use serde_derive::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

/// Per-address poll timeout — a down or firewalled admin domain must not stall the
/// whole poll.
const POLL_TIMEOUT: Duration = Duration::from_secs(3);

/// One known admin admin domain — a single CA identity reachable at one or more
/// admin-server addresses. Persisted as JSON; the fingerprint is stored in its
/// grouped-base32 text form ([`Fingerprint`] has no serde derive).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct KnownAdminDomain {
    pub(super) domain: String,
    /// The CA fingerprint text (the admin domain's identity). Compared against a live
    /// fetch before the admin domain is shown or connected to.
    pub(super) fingerprint: String,
    /// Admin-server addresses seen for this admin domain's members, tried in order.
    pub(super) addrs: Vec<SocketAddr>,
}

impl KnownAdminDomain {
    /// The parsed CA fingerprint, or `None` if the stored text is corrupt.
    pub(super) fn fp(&self) -> Option<Fingerprint> {
        Fingerprint::parse_text(&self.fingerprint).ok()
    }
}

/// The persisted set of known admin domains.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct KnownAdminDomains {
    #[serde(default)]
    pub(super) domains: Vec<KnownAdminDomain>,
}

impl KnownAdminDomains {
    fn path() -> anyhow::Result<PathBuf> {
        Ok(paths::user_config_root()?.join("admin-admin domains.json"))
    }

    /// Load the saved admin domains, or an empty set if the file is missing or
    /// unreadable — a corrupt registry must never break the Admin domain tab.
    pub(super) fn load() -> KnownAdminDomains {
        let Ok(path) = Self::path() else { return KnownAdminDomains::default() };
        match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
            Err(_) => KnownAdminDomains::default(),
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
        let body =
            serde_json::to_vec_pretty(self).context("serializing known admin domains")?;
        std::fs::write(&tmp, &body)
            .with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("renaming into {}", path.display()))?;
        Ok(())
    }

    /// Record a confirmed admin domain: merge `addr` into the entry with a matching
    /// fingerprint (one admin domain = one CA identity, possibly several members), or
    /// append a new entry. Returns whether anything changed (worth saving).
    pub(super) fn upsert(
        &mut self,
        domain: &str,
        addr: SocketAddr,
        fp: Fingerprint,
    ) -> bool {
        let fp_text = fp.text();
        match self.domains.iter_mut().find(|c| c.fingerprint == fp_text) {
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
                self.domains.push(KnownAdminDomain {
                    domain: domain.to_string(),
                    fingerprint: fp_text,
                    addrs: vec![addr],
                });
                true
            }
        }
    }
}

/// If this host runs its own admin server — i.e. it founded or joined a admin domain
/// and hosts a member of it — make sure that admin domain is in the saved set, so it
/// appears on the Admin domain tab without a manual discover. The identity is the one
/// recorded in this host's install record (`admin domain`); the admin-server address
/// is this host's own listen address (a CA host records no upstream one). The
/// on-entry poll then verifies it live like any other saved admin domain. Returns
/// whether the saved set changed (worth saving).
#[cfg(unix)]
pub(super) fn seed_local_cluster(clusters: &mut KnownAdminDomains) -> bool {
    let Some((domain, fp, recorded)) = local_cluster_identity() else { return false };
    // The address to reach this admin domain's CA: this host's own admin-server listen
    // if it hosts a member (a CA / resolver), else the upstream admin server this
    // host enrolled against, recorded at join (a workstation / publisher runs no
    // admin server of its own). Either reaches the same CA; the on-entry poll
    // verifies the fingerprint live.
    let Some(addr) = netidx_admin_client::ops::local_admin_server_listen().or(recorded)
    else {
        return false;
    };
    clusters.upsert(&domain, addr, fp)
}

/// The admin admin domain this host belongs to — domain, CA fingerprint, and the
/// admin-server address recorded at install (the upstream one it joined, if
/// any) — from its install record: the user-scope record, else the system one.
#[cfg(unix)]
fn local_cluster_identity() -> Option<(String, Fingerprint, Option<SocketAddr>)> {
    use netidx_admin_client::provenance::InstallRecord;
    let sys = paths::system_install_record();
    let records = [
        InstallRecord::load_default().ok().flatten(),
        sys.exists().then(|| InstallRecord::load(&sys).ok()).flatten(),
    ];
    records.into_iter().flatten().find_map(|r| {
        let net = r.admin_domain?;
        let fp = Fingerprint::parse_text(&net.ca_fingerprint).ok()?;
        Some((net.domain, fp, r.admin_server))
    })
}

#[cfg(not(unix))]
pub(super) fn seed_local_cluster(_clusters: &mut KnownAdminDomains) -> bool {
    false
}

/// Where an on-entry poll of a known admin domain landed.
#[derive(Debug, Clone, Copy)]
pub(super) enum PollState {
    /// Not yet polled — the event loop launches one poll pass per pending set.
    Unpolled,
    /// A poll is in flight.
    Polling,
    /// Reachable at `addr` with the saved CA identity — safe to show + connect.
    Present { addr: SocketAddr },
    /// No address answered with the saved fingerprint (down, or the address is
    /// now a different admin domain). Hidden from the list.
    Absent,
}

impl PollState {
    /// The verified address to connect to, when this admin domain polled `Present`.
    pub(super) fn present_addr(&self) -> Option<SocketAddr> {
        match self {
            PollState::Present { addr } => Some(*addr),
            PollState::Unpolled | PollState::Polling | PollState::Absent => None,
        }
    }
}

/// Poll each pending known admin domain concurrently: try its addresses in order and
/// accept the first that answers with the saved CA fingerprint. A live-but-
/// different fingerprint (an address reused by another CA) is skipped, so a
/// admin domain is only `Present` when its identity actually verifies. Self-contained
/// (owns its inputs) so the event loop can poll it as a background future.
pub(super) async fn poll_clusters(
    pending: Vec<(usize, KnownAdminDomain)>,
) -> Vec<(usize, PollState)> {
    join_all(pending.into_iter().map(|(i, cluster)| async move {
        let want = cluster.fp();
        let mut state = PollState::Absent;
        for addr in &cluster.addrs {
            let fetched = tokio::time::timeout(
                POLL_TIMEOUT,
                fetch_identity(*addr, NodeKind::Client),
            )
            .await;
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
        let a = Fingerprint::of_der(b"admin domain a spki");
        let b = Fingerprint::of_der(b"admin domain b spki");
        let mut kc = KnownAdminDomains::default();
        let addr1: SocketAddr = "10.0.0.1:4565".parse().unwrap();
        let addr2: SocketAddr = "10.0.0.2:4565".parse().unwrap();
        // First sighting of admin domain a.
        assert!(kc.upsert("hq.local", addr1, a));
        assert_eq!(kc.domains.len(), 1);
        // A second member of the *same* admin domain merges its address in.
        assert!(kc.upsert("hq.local", addr2, a));
        assert_eq!(kc.domains.len(), 1);
        assert_eq!(kc.domains[0].addrs, vec![addr1, addr2]);
        // Re-seeing a known member is a no-op.
        assert!(!kc.upsert("hq.local", addr2, a));
        // A different CA identity is a distinct admin domain, even at a shared address.
        assert!(kc.upsert("eu.local", addr1, b));
        assert_eq!(kc.domains.len(), 2);
    }
}
