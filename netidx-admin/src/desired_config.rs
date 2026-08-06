//! The CA's copy of each server's resolver configuration.
//!
//! The last piece of the admin plane the CA did not own. Topology it always
//! knew; the rest of a resolver config — the bind address, the paths to that
//! host's certificate and key, its pid file, its id-map socket, its tuning —
//! it did not, which is why the old propagation patched the topology in and
//! preserved everything else by matching addresses. A host tells the CA its
//! block once, at enrollment, and from then on the CA renders the whole
//! document.
//!
//! Rendering is a pure function of the stored block and the map, so it is
//! redone on every register rather than maintained by fanning out over
//! affected servers when the topology moves. The version advances only when
//! the rendered document actually differs, which is what a member converging
//! on it is chasing — a version that moved on every render would never be
//! reached.

use crate::admin_proto::{
    AdminDomainMap, AdminServerId, ResolverClusterEntry, ServerState,
};
use netidx::resolver_server::config::file;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One server's rendered resolver config, and how many times it has changed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredConfig {
    /// Bumped once per render that produced a different document. A server
    /// reports the version its file is at; anything lower is behind.
    ///
    /// Starts at 1 — a stored config always describes something, unlike the
    /// perms and id-map models where version 0 means "never established". The
    /// difference is real: those exist only once an operator has edited them,
    /// this exists as soon as a server enrolls.
    pub version: u64,
    pub config: file::Config,
}

/// Every server's resolver config, by server. One file: servers are few, and
/// one atomic write keeps the set consistent with itself.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DesiredConfigs(pub BTreeMap<AdminServerId, StoredConfig>);

impl DesiredConfigs {
    pub fn get(&self, server: AdminServerId) -> Option<&StoredConfig> {
        self.0.get(&server)
    }

    /// Record `config` for `server`, returning the version it is now at and
    /// whether that changed.
    ///
    /// An identical document does not advance the version. Rendering runs on
    /// every register, so without this every poll would move the target a
    /// converging member is chasing and no member could ever arrive.
    pub fn set(&mut self, server: AdminServerId, config: file::Config) -> (u64, bool) {
        match self.0.get_mut(&server) {
            Some(stored) if stored.config == config => (stored.version, false),
            Some(stored) => {
                stored.version += 1;
                stored.config = config;
                (stored.version, true)
            }
            None => {
                self.0.insert(server, StoredConfig { version: 1, config });
                (1, true)
            }
        }
    }

    /// Drop a server the admin domain no longer has.
    pub fn forget(&mut self, server: AdminServerId) -> bool {
        self.0.remove(&server).is_some()
    }
}

/// Render `server`'s resolver config: its own member block, from what it told
/// the CA at enrollment, plus the topology its cluster currently has.
///
/// `member_servers` carries this host's block and nothing else. That is not a
/// simplification — `Config::from_file` runs `check_member_server_auth` over
/// every block in the list, opening the certificate and key each names, so a
/// config listing another host's paths would fail to load on this one.
///
/// `None` when there is nothing to render: no stored block (a server with no
/// resolver, or one that enrolled before the CA kept them), or no active
/// cluster to take topology from.
pub fn render(
    map: &AdminDomainMap,
    server: AdminServerId,
    stored: &DesiredConfigs,
    topology: impl FnOnce(
        &ResolverClusterEntry,
    ) -> (Option<file::Referral>, Vec<file::Referral>),
) -> Option<file::Config> {
    let previous = stored.get(server)?;
    let entry = map.admin_servers.iter().find(|s| s.id == server)?;
    if entry.state != ServerState::Registered && entry.state != ServerState::Enrolled {
        return None;
    }
    let cluster =
        entry.cluster.and_then(|id| map.resolver_clusters.iter().find(|c| c.id == id))?;
    let (parent, children) = topology(cluster);
    Some(file::Config {
        children,
        parent,
        member_servers: previous.config.member_servers.clone(),
        perms: previous.config.perms.clone(),
        include_permissions: previous.config.include_permissions.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A config as a host would have handed it over at enrollment: its own
    /// member block and where its perms live, with no topology — the first
    /// render fills that in from the map.
    fn installed(addr: &str) -> file::Config {
        file::Config {
            children: Vec::new(),
            parent: None,
            member_servers: vec![
                file::MemberServerBuilder::default()
                    .addr(addr.parse().unwrap())
                    .bind_addr("0.0.0.0".parse().unwrap())
                    .auth(file::Auth::Anonymous)
                    .build()
                    .unwrap(),
            ],
            perms: crate::perms::empty(),
            include_permissions: vec!["perms.json".into()],
        }
    }

    /// Re-rendering the same document must not advance the version. Rendering
    /// happens on every register, so a version that moved each time would be a
    /// target no member could reach.
    #[test]
    fn an_identical_render_does_not_advance_the_version() {
        let s = AdminServerId::new();
        let mut d = DesiredConfigs::default();
        let c = installed("10.0.0.1:4564");
        assert_eq!(d.set(s, c.clone()), (1, true));
        assert_eq!(d.set(s, c.clone()), (1, false));
        let mut changed = c.clone();
        changed.member_servers[0].max_connections = 42;
        assert_eq!(d.set(s, changed), (2, true));
    }

    /// A stored config starts at version 1, not 0. Unlike the perms and id-map
    /// models there is no "never established" state — a server has a config
    /// from the moment it enrolls.
    #[test]
    fn a_stored_config_is_established_immediately() {
        let s = AdminServerId::new();
        let mut d = DesiredConfigs::default();
        d.set(s, installed("10.0.0.1:4564"));
        assert_eq!(d.get(s).unwrap().version, 1);
    }

    /// Each server is versioned on its own: retuning one must not make every
    /// other server look behind.
    #[test]
    fn servers_are_versioned_independently() {
        let (a, b) = (AdminServerId::new(), AdminServerId::new());
        let mut d = DesiredConfigs::default();
        d.set(a, installed("10.0.0.1:4564"));
        d.set(b, installed("10.0.0.2:4564"));
        let mut next = d.get(a).unwrap().config.clone();
        next.member_servers[0].reader_ttl = 90;
        d.set(a, next);
        assert_eq!(d.get(a).unwrap().version, 2);
        assert_eq!(d.get(b).unwrap().version, 1);
    }

    #[test]
    fn a_forgotten_server_is_unknown_again() {
        let s = AdminServerId::new();
        let mut d = DesiredConfigs::default();
        d.set(s, installed("10.0.0.1:4564"));
        assert!(d.forget(s));
        assert!(d.get(s).is_none());
        assert!(!d.forget(s));
    }
}
