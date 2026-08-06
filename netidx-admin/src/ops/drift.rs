//! Who has caught up, and who hasn't.
//!
//! The counterpart to an edit no longer reporting per-peer results. An edit
//! records a version at the CA; every server reaches it on the register it
//! already makes. Whether a given server has got there is therefore a question
//! about the admin domain *now*, and the map already answers it — every server
//! reports the versions it holds, and the CA records them.
//!
//! That is strictly better than what a push could report. A push result
//! described one moment and was stale the instant it printed; this can be
//! asked again, and the answer changes as hosts converge.

use crate::{
    admin_proto::{AdminDomainMap, AdminServerId, Role, ServerState},
    ops::AdminTarget,
    transport,
};
use anyhow::{Context, Result};

/// One server's agreement with the CA, per kind of state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerDrift {
    pub server: AdminServerId,
    pub addr: std::net::SocketAddr,
    /// `None` when the server holds no resolver, so no cluster perms apply.
    pub perms: Option<Agreement>,
    /// `None` when the server does not hold the id-map role.
    pub id_map: Option<Agreement>,
}

/// How one piece of state compares to what the CA recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agreement {
    /// Reported the version the CA holds.
    Current { version: u64 },
    /// Reported a lower version, or none at all. `reported: None` is a server
    /// that has never applied this state — a fresh member, or one whose file
    /// was replaced out from under it.
    Behind { reported: Option<u64>, want: u64 },
    /// The CA has never recorded this state, so nothing can be behind it. Not
    /// the same as being current, and worth saying so: it means no edit has
    /// ever been made.
    Unrecorded,
    /// Reported a version *above* the CA's. Something is wrong — a restored
    /// backup, or two CAs — and the honest answer is to say so rather than
    /// call it current or push an older document at it.
    Ahead { reported: u64, ca: u64 },
}

impl Agreement {
    pub fn is_current(self) -> bool {
        matches!(self, Agreement::Current { .. } | Agreement::Unrecorded)
    }

    fn of(reported: Option<u64>, ca: Option<u64>) -> Self {
        match (reported, ca) {
            (_, None) => Agreement::Unrecorded,
            (Some(have), Some(want)) if have == want => {
                Agreement::Current { version: have }
            }
            (Some(have), Some(ca)) if have > ca => {
                Agreement::Ahead { reported: have, ca }
            }
            (reported, Some(want)) => Agreement::Behind { reported, want },
        }
    }
}

/// Every registered server's agreement, in stable server-id order.
///
/// Reads the map, which any admin server serves — this asks no server about
/// itself, because a server that cannot be reached is exactly the one whose
/// answer matters and the map has its last report either way.
pub async fn drift(target: &AdminTarget) -> Result<Vec<ServerDrift>> {
    let map = match target {
        AdminTarget::Remote { session } => transport::get_map_pinned(
            session.server,
            crate::admin_proto::NodeKind::Client,
            &session.identity,
        )
        .await
        .context("fetching the admin domain map")?,
        #[cfg(unix)]
        AdminTarget::Local { cfg_path } => crate::local::get_map(cfg_path)
            .await
            .context("reading the admin domain map")?,
    };
    Ok(from_map(&map))
}

/// The comparison itself, over a map. Separated so it can be tested without a
/// daemon — the interesting cases are all shapes of map.
pub fn from_map(map: &AdminDomainMap) -> Vec<ServerDrift> {
    let mut out: Vec<_> = map
        .admin_servers
        .iter()
        .filter(|s| s.state == ServerState::Registered)
        .map(|s| {
            let perms = s.cluster.map(|_| {
                Agreement::of(s.reported_perms_version, map.perms_version_for(s.cluster))
            });
            let id_map = s
                .roles
                .contains(Role::IdMap)
                .then(|| Agreement::of(s.reported_id_map_version, map.id_map_version));
            ServerDrift { server: s.id, addr: s.addr, perms, id_map }
        })
        .collect();
    out.sort_by_key(|d| d.server);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_proto::{
        AdminServerEntry, ResolverClusterEntry, ResolverClusterState, ServerState,
    };

    fn server(
        cluster: Option<crate::admin_proto::ResolverClusterId>,
        roles: enumflags2::BitFlags<Role>,
        perms: Option<u64>,
        id_map: Option<u64>,
    ) -> AdminServerEntry {
        AdminServerEntry {
            id: AdminServerId::new(),
            addr: "10.0.0.1:4565".parse().unwrap(),
            roles,
            resolver: None,
            cluster,
            state: ServerState::Registered,
            reported_read_gate: None,
            reported_id_map_version: id_map,
            reported_perms_version: perms,
        }
    }

    fn map(
        servers: Vec<AdminServerEntry>,
        cluster: Option<(crate::admin_proto::ResolverClusterId, Option<u64>)>,
        id_map_version: Option<u64>,
    ) -> AdminDomainMap {
        let mut m = AdminDomainMap::empty(AdminServerId::new());
        m.id_map_version = id_map_version;
        m.admin_servers = servers;
        if let Some((id, perms_version)) = cluster {
            m.resolver_clusters.push(ResolverClusterEntry {
                id,
                base: "/".into(),
                state: ResolverClusterState::Active,
                members: vec![],
                parent: None,
                children: vec![],
                perms_version,
            });
        }
        m
    }

    /// The case the whole mechanism exists for: a member that was down for an
    /// edit reports a lower version and is named as behind.
    #[test]
    fn a_member_below_the_recorded_version_is_behind() {
        let c = crate::admin_proto::ResolverClusterId::new();
        let m = map(
            vec![server(Some(c), Role::Resolver.into(), Some(3), None)],
            Some((c, Some(7))),
            None,
        );
        let d = from_map(&m);
        assert_eq!(d[0].perms, Some(Agreement::Behind { reported: Some(3), want: 7 }));
        assert_eq!(d[0].id_map, None, "no id-map role, so nothing to say");
    }

    /// A member that has never applied anything is behind, not current. `None`
    /// has to read as "further behind than any number", or a fresh member
    /// would look caught up and never be repaired.
    #[test]
    fn never_applied_is_behind_not_current() {
        let c = crate::admin_proto::ResolverClusterId::new();
        let m = map(
            vec![server(Some(c), Role::Resolver.into(), None, None)],
            Some((c, Some(1))),
            None,
        );
        assert_eq!(
            from_map(&m)[0].perms,
            Some(Agreement::Behind { reported: None, want: 1 })
        );
    }

    /// Nothing recorded means nobody can be behind it. Distinct from current,
    /// because "no edit has ever been made" is a different fact from
    /// "everyone has the latest edit".
    #[test]
    fn nothing_recorded_is_not_the_same_as_current() {
        let c = crate::admin_proto::ResolverClusterId::new();
        let m = map(
            vec![server(Some(c), Role::Resolver.into(), None, None)],
            Some((c, None)),
            None,
        );
        let a = from_map(&m)[0].perms.unwrap();
        assert_eq!(a, Agreement::Unrecorded);
        assert!(a.is_current(), "an unrecorded state is nothing to chase");
    }

    /// A version above the CA's is a contradiction, not a lag, and must not be
    /// reported as current — pushing an older document at it would make it
    /// worse, and calling it current would hide a restored backup or a second
    /// CA.
    #[test]
    fn a_member_ahead_of_the_ca_is_called_out() {
        let m = map(vec![server(None, Role::IdMap.into(), None, Some(9))], None, Some(4));
        let a = from_map(&m)[0].id_map.unwrap();
        assert_eq!(a, Agreement::Ahead { reported: 9, ca: 4 });
        assert!(!a.is_current());
    }

    /// Roles decide what is even asked. A host with no resolver has no cluster
    /// perms; one with no id-map role has no id-map — reporting either as
    /// "behind" would invent an obligation it does not have.
    #[test]
    fn a_server_is_only_measured_against_state_it_holds() {
        let m = map(vec![server(None, Role::Ca.into(), None, None)], None, Some(2));
        let d = &from_map(&m)[0];
        assert_eq!(d.perms, None);
        assert_eq!(d.id_map, None);
    }

    /// Only registered servers. One that is enrolled but has never registered
    /// has reported nothing, and listing it as behind would put a permanent
    /// row in the drift view for a host that may never exist.
    #[test]
    fn unregistered_servers_are_not_listed() {
        let c = crate::admin_proto::ResolverClusterId::new();
        let mut s = server(Some(c), Role::Resolver.into(), None, None);
        s.state = ServerState::Enrolled;
        assert!(from_map(&map(vec![s], Some((c, Some(1))), None)).is_empty());
    }
}
