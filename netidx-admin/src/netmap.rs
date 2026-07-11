//! Durable CA-owned admin-plane topology and pure mutation helpers.

use crate::{
    admin_proto::{
        AdminServerId, ClusterEdge, ClusterEntry, ClusterFacts, ClusterPlacement,
        ClusterState, EnrollmentRequest, NetworkMap, ResolverAddr, ResolverClusterId,
        Role, ServerEntry, ServerState,
    },
    atomic,
};
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};

pub fn path(ca_dir: &Path) -> PathBuf {
    ca_dir.join("netmap.json")
}

pub fn load(ca_dir: &Path, controller: AdminServerId) -> Result<NetworkMap> {
    let p = path(ca_dir);
    match std::fs::read(&p) {
        Ok(bytes) => {
            let map: NetworkMap = serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing network map {p:?}"))?;
            if map.controller != controller {
                bail!(
                    "network map controller {} does not match installed controller certificate {}",
                    map.controller,
                    controller
                );
            }
            Ok(map)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(NetworkMap::empty(controller))
        }
        Err(e) => Err(e).with_context(|| format!("reading network map {p:?}")),
    }
}

pub fn save(ca_dir: &Path, map: &NetworkMap) -> Result<()> {
    atomic::write_atomic_pretty_json(&path(ca_dir), map)
}

fn changed(map: &mut NetworkMap) {
    map.servers.sort_by_key(|s| s.id);
    map.clusters.sort_by_key(|c| c.id);
    map.version = map.version.saturating_add(1);
}

pub fn upsert_controller(
    map: &mut NetworkMap,
    mut entry: ServerEntry,
    cluster: Option<ClusterFacts>,
) -> Result<bool> {
    if entry.id != map.controller || !entry.roles.contains(&Role::Ca) {
        bail!("controller entry must use the map controller id and carry the Ca role");
    }
    entry.state = ServerState::Registered;
    let mut did_change = match map.servers.iter_mut().find(|s| s.id == entry.id) {
        Some(old) if *old == entry => false,
        Some(old) => {
            *old = entry.clone();
            true
        }
        None => {
            map.servers.push(entry.clone());
            true
        }
    };
    if let (Some(cluster_id), Some(facts)) = (entry.cluster, cluster) {
        match map.clusters.iter().find(|c| c.id == cluster_id) {
            // Startup facts are observations, not topology authority. They
            // seed a fresh map, but may be stale while a delegation fanout is
            // incomplete or while the controller awaits its manual rolling
            // resolver restart. Never let them undo a CA-owned split,
            // reparenting, or member assignment on an existing map.
            Some(_) => {}
            None => {
                map.clusters.push(ClusterEntry {
                    id: cluster_id,
                    base: facts.base,
                    state: ClusterState::Active,
                    members: entry.resolver.clone().into_iter().collect(),
                    parent: None,
                    children: Vec::new(),
                });
                did_change = true;
            }
        }
    }
    if did_change {
        changed(map);
    }
    Ok(did_change)
}

/// Insert the immutable grant at certificate issuance time. The enrollee does
/// not choose either identity: `server_id` and a created cluster id come from
/// the CA.
pub fn enroll(
    map: &mut NetworkMap,
    server_id: AdminServerId,
    request: &EnrollmentRequest,
) -> Result<ResolverClusterId> {
    if map.servers.iter().any(|s| s.id == server_id) {
        bail!("server identity {server_id} is already enrolled");
    }
    if request.roles.contains(&Role::Ca) {
        bail!("an enrollee may never request the Ca role");
    }
    if !request.roles.contains(&Role::Resolver) {
        bail!("every non-controller admin server must have the Resolver role");
    }
    let resolver_member = request
        .resolver_member
        .as_ref()
        .context("a resolver enrollment must identify its owned member")?;
    let mut requested_addrs = std::collections::BTreeSet::new();
    if request.resolver_members.iter().any(|member| !requested_addrs.insert(member.addr))
    {
        bail!("a resolver cluster may not contain duplicate member addresses");
    }
    if !request.resolver_members.contains(resolver_member) {
        bail!("the owned resolver member must appear in the requested member set");
    }
    if map.servers.iter().any(|s| {
        s.resolver.as_ref().is_some_and(|member| member.addr == resolver_member.addr)
    }) {
        bail!(
            "resolver member {} is already owned by another admin server",
            resolver_member.addr
        );
    }
    let cluster = match &request.cluster {
        ClusterPlacement::Create { base } => {
            if base.is_empty() || !base.starts_with('/') {
                bail!("cluster base must be an absolute netidx path");
            }
            if map.clusters.iter().any(|c| c.base == *base) {
                bail!("a cluster already exists at base {base:?}");
            }
            let id = ResolverClusterId::new();
            map.clusters.push(ClusterEntry {
                id,
                base: base.clone(),
                state: ClusterState::Pending,
                // Cluster membership is the union of CA-issued server
                // identities, not the local resolver.json launch menu.
                members: vec![resolver_member.clone()],
                parent: None,
                children: Vec::new(),
            });
            id
        }
        ClusterPlacement::Join { cluster } => {
            let approved = map
                .clusters
                .iter_mut()
                .find(|c| c.id == *cluster)
                .context("requested resolver cluster does not exist")?;
            for configured in &request.resolver_members {
                if configured != resolver_member && !approved.members.contains(configured)
                {
                    bail!(
                        "local resolver config names unowned member {} outside the approved cluster",
                        configured.addr
                    );
                }
            }
            approved.members.push(resolver_member.clone());
            normalize_addrs(&mut approved.members);
            *cluster
        }
    };
    map.servers.push(ServerEntry {
        id: server_id,
        addr: request.listen,
        roles: request.roles.clone(),
        resolver: Some(resolver_member.clone()),
        cluster: Some(cluster),
        state: ServerState::Enrolled,
    });
    changed(map);
    Ok(cluster)
}

fn server_set(
    map: &NetworkMap,
    ids: &[AdminServerId],
    require_registered: bool,
) -> Result<(ResolverClusterId, Vec<ResolverAddr>)> {
    if ids.is_empty() {
        bail!("a delegation server set may not be empty");
    }
    let mut cluster = None;
    let mut members = Vec::with_capacity(ids.len());
    for id in ids {
        let server = map
            .servers
            .iter()
            .find(|s| s.id == *id)
            .with_context(|| format!("admin server {id} does not exist"))?;
        if require_registered && server.state != ServerState::Registered {
            bail!("parent admin server {id} is not registered");
        }
        if !server.roles.contains(&Role::Resolver) {
            bail!("admin server {id} has no Resolver grant");
        }
        let id_cluster = server
            .cluster
            .with_context(|| format!("resolver admin server {id} has no cluster"))?;
        match cluster {
            None => cluster = Some(id_cluster),
            Some(expected) if expected == id_cluster => {}
            Some(_) => bail!("the selected servers do not belong to one cluster"),
        }
        members.push(server.resolver.clone().with_context(|| {
            format!("admin server {id} has no owned resolver member")
        })?);
    }
    normalize_addrs(&mut members);
    Ok((cluster.expect("nonempty ids"), members))
}

fn assigned_servers(map: &NetworkMap, cluster: ResolverClusterId) -> Vec<AdminServerId> {
    let mut ids: Vec<_> = map
        .servers
        .iter()
        .filter(|s| s.cluster == Some(cluster) && s.roles.contains(&Role::Resolver))
        .map(|s| s.id)
        .collect();
    ids.sort();
    ids
}

fn canonical_ids(ids: &[AdminServerId]) -> Vec<AdminServerId> {
    let mut ids = ids.to_vec();
    ids.sort();
    ids.dedup();
    ids
}

/// The authoritative result of attaching or splitting resolver clusters.
#[derive(Debug, Clone)]
pub struct DelegationChange {
    pub parent: ClusterEntry,
    pub child: ClusterEntry,
    pub changed: bool,
}

/// Apply a delegation proposal expressed as immutable server sets. If both
/// sets currently belong to one cluster, split it: the parent set retains the
/// old cluster ID and the child set receives `proposed_child`. If they belong
/// to distinct clusters, attach/rebase the complete child cluster. Reapplying
/// the final proposal is idempotent.
pub fn delegate(
    map: &mut NetworkMap,
    proposed_path: &str,
    proposed_child: ResolverClusterId,
    parent_servers: &[AdminServerId],
    child_servers: &[AdminServerId],
) -> Result<DelegationChange> {
    use netidx::path::Path as NPath;

    let parent_servers = canonical_ids(parent_servers);
    let child_servers = canonical_ids(child_servers);
    if parent_servers.iter().any(|id| child_servers.contains(id)) {
        bail!("parent and child resolver server sets must be disjoint");
    }
    let path = NPath::from(proposed_path.to_string());
    if !NPath::is_absolute(&path) || path.as_ref() == "/" {
        bail!("delegated path must be absolute and below root");
    }
    let (parent_id, parent_members) = server_set(map, &parent_servers, true)?;
    let (current_child_id, child_members) = server_set(map, &child_servers, false)?;
    let parent_pos = map
        .clusters
        .iter()
        .position(|c| c.id == parent_id)
        .context("parent cluster does not exist")?;
    if map.clusters[parent_pos].state != ClusterState::Active {
        bail!("the parent cluster is not active");
    }
    if !NPath::is_parent(&map.clusters[parent_pos].base, &path) {
        bail!(
            "delegated path {proposed_path:?} is outside parent base {:?}",
            map.clusters[parent_pos].base
        );
    }

    // An already-applied split/attach resolves the two sets to different
    // clusters and lands here. The ordinary attach path below recognizes it
    // and returns without version churn.
    if parent_id != current_child_id {
        let child_pos = map
            .clusters
            .iter()
            .position(|c| c.id == current_child_id)
            .context("child cluster does not exist")?;
        let mut descendants = std::collections::BTreeSet::new();
        let mut pending = map.clusters[child_pos].children.clone();
        while let Some(id) = pending.pop() {
            if !descendants.insert(id) {
                continue;
            }
            let cluster = map
                .clusters
                .iter()
                .find(|cluster| cluster.id == id)
                .context("child topology references a missing cluster")?;
            pending.extend(cluster.children.iter().copied());
        }
        if descendants.contains(&parent_id) {
            bail!("attaching these clusters would create a topology cycle");
        }
        for id in &descendants {
            let descendant =
                map.clusters.iter().find(|cluster| cluster.id == *id).unwrap();
            if !NPath::is_parent(&path, &descendant.base) {
                bail!(
                    "existing descendant {:?} is outside the proposed child base {proposed_path:?}",
                    descendant.base
                );
            }
        }
        let expected_parent_ids = assigned_servers(map, parent_id);
        let expected_child_ids = assigned_servers(map, current_child_id);
        if expected_parent_ids != parent_servers {
            bail!(
                "the parent selection must include every resolver server in its cluster"
            );
        }
        if expected_child_ids != child_servers {
            bail!(
                "the child selection must include every resolver server in its cluster"
            );
        }
        let already = map.clusters[child_pos].base == proposed_path
            && map.clusters[child_pos].parent == Some(parent_id)
            && map.clusters[child_pos].state == ClusterState::Active
            && map.clusters[parent_pos].children.contains(&current_child_id);
        if already {
            return Ok(DelegationChange {
                parent: map.clusters[parent_pos].clone(),
                child: map.clusters[child_pos].clone(),
                changed: false,
            });
        }
        if map.clusters[child_pos].parent.is_some() {
            bail!("the selected child cluster is already attached");
        }
        if map
            .clusters
            .iter()
            .any(|c| c.id != current_child_id && c.base == proposed_path)
        {
            bail!("another resolver cluster already owns {proposed_path:?}");
        }
        map.clusters[child_pos].base = proposed_path.to_string();
        map.clusters[child_pos].members = child_members;
        map.clusters[child_pos].parent = Some(parent_id);
        map.clusters[child_pos].state = ClusterState::Active;
        map.clusters[parent_pos].members = parent_members;
        if !map.clusters[parent_pos].children.contains(&current_child_id) {
            map.clusters[parent_pos].children.push(current_child_id);
            map.clusters[parent_pos].children.sort();
        }
        changed(map);
        return Ok(DelegationChange {
            parent: map.clusters.iter().find(|c| c.id == parent_id).unwrap().clone(),
            child: map
                .clusters
                .iter()
                .find(|c| c.id == current_child_id)
                .unwrap()
                .clone(),
            changed: true,
        });
    }

    // Split one active peer cluster. The proposal must partition every
    // resolver identity assigned to it; otherwise approval would silently
    // orphan an unmentioned member.
    let mut union = parent_servers.clone();
    union.extend(child_servers.iter().copied());
    union.sort();
    if union != assigned_servers(map, parent_id) {
        bail!("a cluster split must assign every resolver server to parent or child");
    }
    if map.clusters.iter().any(|c| c.id == proposed_child) {
        bail!("the proposed child cluster identity is already in use");
    }
    if map.clusters.iter().any(|c| c.base == proposed_path) {
        bail!("another resolver cluster already owns {proposed_path:?}");
    }

    let old_children = map.clusters[parent_pos].children.clone();
    let mut parent_children = Vec::new();
    let mut child_children = Vec::new();
    for id in old_children {
        let Some(existing) = map.clusters.iter().find(|c| c.id == id) else {
            continue;
        };
        if NPath::is_parent(&path, &existing.base) {
            child_children.push(id);
        } else {
            parent_children.push(id);
        }
    }
    parent_children.push(proposed_child);
    parent_children.sort();
    child_children.sort();
    map.clusters[parent_pos].members = parent_members;
    map.clusters[parent_pos].children = parent_children;
    for id in &child_children {
        if let Some(cluster) = map.clusters.iter_mut().find(|c| c.id == *id) {
            cluster.parent = Some(proposed_child);
        }
    }
    map.clusters.push(ClusterEntry {
        id: proposed_child,
        base: proposed_path.to_string(),
        state: ClusterState::Active,
        members: child_members,
        parent: Some(parent_id),
        children: child_children,
    });
    for server in &mut map.servers {
        if child_servers.contains(&server.id) {
            server.cluster = Some(proposed_child);
        }
    }
    changed(map);
    Ok(DelegationChange {
        parent: map.clusters.iter().find(|c| c.id == parent_id).unwrap().clone(),
        child: map.clusters.iter().find(|c| c.id == proposed_child).unwrap().clone(),
        changed: true,
    })
}

fn edge_for(map: &NetworkMap, id: ResolverClusterId) -> Option<ClusterEdge> {
    map.clusters
        .iter()
        .find(|c| c.id == id)
        .map(|c| ClusterEdge { path: c.base.clone(), addrs: c.members.clone() })
}

fn normalize_addrs(addrs: &mut Vec<ResolverAddr>) {
    addrs.sort_by(|a, b| a.addr.cmp(&b.addr));
    addrs.dedup_by(|a, b| a == b);
}

fn facts_match(
    map: &NetworkMap,
    cluster: &ClusterEntry,
    owned: Option<&ResolverAddr>,
    facts: &ClusterFacts,
) -> bool {
    let mut expected_members = cluster.members.clone();
    let mut got_members = facts.members.clone();
    normalize_addrs(&mut expected_members);
    normalize_addrs(&mut got_members);
    // A child's parent referral is mounted at the *child's* base (for example
    // `/eu`) while its addresses come from the parent cluster. Using the
    // parent's own base here (`/` for the root) rejects every correctly
    // configured non-root server as topology drift.
    let expected_parent = cluster.parent.and_then(|id| {
        map.clusters.iter().find(|c| c.id == id).map(|parent| ClusterEdge {
            path: cluster.base.clone(),
            addrs: parent.members.clone(),
        })
    });
    let mut expected_children: Vec<ClusterEdge> =
        cluster.children.iter().filter_map(|id| edge_for(map, *id)).collect();
    expected_children.sort_by(|a, b| a.path.cmp(&b.path));
    let mut got_children = facts.children.clone();
    got_children.sort_by(|a, b| a.path.cmp(&b.path));
    facts.base == cluster.base
        && owned.is_some_and(|owned| got_members.contains(owned))
        && got_members.iter().all(|member| expected_members.contains(member))
        && facts.parent == expected_parent
        && got_children == expected_children
}

/// Activate only the authenticated node's existing grant and allow it to
/// update only its own routing address.
pub fn register(
    map: &mut NetworkMap,
    server_id: AdminServerId,
    addr: std::net::SocketAddr,
    resolver: Option<&ClusterFacts>,
) -> Result<bool> {
    let pos = map
        .servers
        .iter()
        .position(|s| s.id == server_id)
        .context("the authenticated server has no enrollment grant")?;
    let cluster_id = map.servers[pos].cluster;
    let owned = map.servers[pos].resolver.clone();
    match (cluster_id, resolver) {
        (Some(id), Some(facts)) => {
            let cluster = map
                .clusters
                .iter()
                .find(|c| c.id == id)
                .context("the server grant references a missing cluster")?;
            if !facts_match(map, cluster, owned.as_ref(), facts) {
                bail!(
                    "reported resolver configuration drifts from the CA-approved cluster"
                );
            }
        }
        (Some(_), None) => bail!("the approved Resolver role must report resolver facts"),
        (None, Some(_)) => {
            bail!("resolver facts were reported without an approved cluster")
        }
        (None, None) => {}
    }
    let server = &mut map.servers[pos];
    let did_change = server.addr != addr || server.state != ServerState::Registered;
    if did_change {
        server.addr = addr;
        server.state = ServerState::Registered;
        changed(map);
    }
    Ok(did_change)
}

pub fn deregister(map: &mut NetworkMap, server_id: AdminServerId) -> Result<bool> {
    let server = map
        .servers
        .iter_mut()
        .find(|s| s.id == server_id)
        .context("the authenticated server has no enrollment grant")?;
    if server.state == ServerState::Enrolled {
        return Ok(false);
    }
    server.state = ServerState::Enrolled;
    changed(map);
    Ok(true)
}

pub fn remove(map: &mut NetworkMap, server_id: AdminServerId) -> Result<bool> {
    if server_id == map.controller {
        bail!("the active controller cannot be removed; replace and revoke it first");
    }
    let before = map.servers.len();
    map.servers.retain(|s| s.id != server_id);
    if before == map.servers.len() {
        return Ok(false);
    }
    let used: std::collections::BTreeSet<_> =
        map.servers.iter().filter_map(|s| s.cluster).collect();
    let removed_clusters: std::collections::BTreeSet<_> =
        map.clusters.iter().filter(|c| !used.contains(&c.id)).map(|c| c.id).collect();
    map.clusters.retain(|c| !removed_clusters.contains(&c.id));
    for cluster in &mut map.clusters {
        let mut owned: Vec<_> = map
            .servers
            .iter()
            .filter(|server| server.cluster == Some(cluster.id))
            .filter_map(|server| server.resolver.clone())
            .collect();
        normalize_addrs(&mut owned);
        cluster.members = owned;
        if cluster.parent.is_some_and(|id| removed_clusters.contains(&id)) {
            cluster.parent = None;
            cluster.state = ClusterState::Pending;
        }
        cluster.children.retain(|id| !removed_clusters.contains(id));
    }
    changed(map);
    Ok(true)
}

pub fn reparent(
    map: &mut NetworkMap,
    child: ResolverClusterId,
    parent: ResolverClusterId,
) -> Result<bool> {
    if child == parent {
        bail!("a resolver cluster cannot be its own parent");
    }
    let child_pos = map
        .clusters
        .iter()
        .position(|c| c.id == child)
        .context("child cluster does not exist")?;
    let parent_pos = map
        .clusters
        .iter()
        .position(|c| c.id == parent)
        .context("parent cluster does not exist")?;
    if map.clusters[parent_pos].state != ClusterState::Active {
        bail!("the parent cluster is not active");
    }
    let old_parent = map.clusters[child_pos].parent;
    let already = old_parent == Some(parent)
        && map.clusters[child_pos].state == ClusterState::Active
        && map.clusters[parent_pos].children.contains(&child);
    if already {
        return Ok(false);
    }
    if let Some(old) = old_parent
        && let Some(old) = map.clusters.iter_mut().find(|c| c.id == old)
    {
        old.children.retain(|id| *id != child);
    }
    map.clusters[child_pos].parent = Some(parent);
    map.clusters[child_pos].state = ClusterState::Active;
    if !map.clusters[parent_pos].children.contains(&child) {
        map.clusters[parent_pos].children.push(child);
        map.clusters[parent_pos].children.sort();
    }
    changed(map);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_proto::{InfoAuth, ServerState};

    fn addr(s: &str) -> ResolverAddr {
        ResolverAddr { addr: s.parse().unwrap(), auth: InfoAuth::Anonymous }
    }

    fn enrollment(base: &str, member: &str) -> EnrollmentRequest {
        let member = addr(member);
        EnrollmentRequest {
            listen: "10.0.0.10:4565".parse().unwrap(),
            roles: vec![Role::Resolver],
            resolver_member: Some(member.clone()),
            resolver_members: vec![member],
            cluster: ClusterPlacement::Create { base: base.into() },
        }
    }

    #[test]
    fn enrollment_registration_and_self_only_address_update() {
        let controller = AdminServerId::new();
        let server = AdminServerId::new();
        let mut map = NetworkMap::empty(controller);
        let request = enrollment("/eu", "10.0.0.10:4564");
        let cluster = enroll(&mut map, server, &request).unwrap();
        assert_eq!(map.servers[0].state, ServerState::Enrolled);
        assert_eq!(map.clusters[0].state, ClusterState::Pending);
        let facts = ClusterFacts {
            members: request.resolver_members.clone(),
            base: "/eu".into(),
            parent: None,
            children: vec![],
        };
        assert!(
            register(&mut map, server, "10.0.0.20:4565".parse().unwrap(), Some(&facts),)
                .unwrap()
        );
        assert_eq!(map.servers[0].id, server);
        assert_eq!(map.servers[0].cluster, Some(cluster));
        assert_eq!(map.servers[0].roles, vec![Role::Resolver]);
        assert_eq!(map.servers[0].addr, "10.0.0.20:4565".parse().unwrap());
        let mut drift = facts.clone();
        drift.base = "/us".into();
        assert!(
            register(&mut map, server, "10.0.0.30:4565".parse().unwrap(), Some(&drift),)
                .is_err()
        );
        assert_eq!(map.servers[0].addr, "10.0.0.20:4565".parse().unwrap());
    }

    #[test]
    fn controller_registration_preserves_delegated_children() {
        let controller = AdminServerId::new();
        let root = ResolverClusterId::new();
        let child = ResolverClusterId::new();
        let controller_entry = ServerEntry {
            id: controller,
            addr: "10.0.0.1:4565".parse().unwrap(),
            roles: vec![Role::Ca, Role::Resolver],
            resolver: Some(addr("10.0.0.1:4564")),
            cluster: Some(root),
            state: ServerState::Registered,
        };
        let facts = ClusterFacts {
            members: vec![addr("10.0.0.1:4564")],
            base: "/".into(),
            parent: None,
            children: vec![ClusterEdge {
                path: "/eu".into(),
                addrs: vec![addr("10.0.0.2:4564")],
            }],
        };
        let mut map = NetworkMap {
            version: 1,
            controller,
            servers: vec![controller_entry.clone()],
            clusters: vec![
                ClusterEntry {
                    id: root,
                    base: "/".into(),
                    state: ClusterState::Active,
                    members: facts.members.clone(),
                    parent: None,
                    children: vec![child],
                },
                ClusterEntry {
                    id: child,
                    base: "/eu".into(),
                    state: ClusterState::Active,
                    members: vec![addr("10.0.0.2:4564")],
                    parent: Some(root),
                    children: vec![],
                },
            ],
        };

        assert!(!upsert_controller(&mut map, controller_entry, Some(facts)).unwrap());
        let root = map.clusters.iter().find(|c| c.id == root).unwrap();
        assert_eq!(root.children, vec![child]);
    }

    #[test]
    fn controller_startup_does_not_undo_its_child_assignment() {
        let controller = AdminServerId::new();
        let root_server = AdminServerId::new();
        let root = ResolverClusterId::new();
        let child = ResolverClusterId::new();
        let controller_entry = ServerEntry {
            id: controller,
            addr: "10.0.60.1:4565".parse().unwrap(),
            roles: vec![Role::Ca, Role::Resolver],
            resolver: Some(addr("10.0.60.1:4564")),
            cluster: Some(child),
            state: ServerState::Registered,
        };
        let authoritative_child = ClusterEntry {
            id: child,
            base: "/ap".into(),
            state: ClusterState::Active,
            members: vec![addr("10.0.60.1:4564")],
            parent: Some(root),
            children: vec![],
        };
        let mut map = NetworkMap {
            version: 3,
            controller,
            servers: vec![
                controller_entry.clone(),
                ServerEntry {
                    id: root_server,
                    addr: "10.0.0.1:4565".parse().unwrap(),
                    roles: vec![Role::Resolver],
                    resolver: Some(addr("10.0.0.1:4564")),
                    cluster: Some(root),
                    state: ServerState::Registered,
                },
            ],
            clusters: vec![
                ClusterEntry {
                    id: root,
                    base: "/".into(),
                    state: ClusterState::Active,
                    members: vec![addr("10.0.0.1:4564")],
                    parent: None,
                    children: vec![child],
                },
                authoritative_child.clone(),
            ],
        };
        // The resolver file may still contain its pre-split peer topology
        // until the controller fanout and manual rolling restart complete.
        let stale = ClusterFacts {
            members: vec![addr("10.0.0.1:4564"), addr("10.0.60.1:4564")],
            base: "/".into(),
            parent: None,
            children: vec![],
        };

        assert!(!upsert_controller(&mut map, controller_entry, Some(stale)).unwrap());
        assert_eq!(
            map.clusters.iter().find(|c| c.id == child),
            Some(&authoritative_child)
        );
        assert_eq!(map.version, 3);
    }

    #[test]
    fn stable_cluster_join_deregister_remove_and_reparent() {
        let controller = AdminServerId::new();
        let first = AdminServerId::new();
        let second = AdminServerId::new();
        let parent_server = AdminServerId::new();
        let mut map = NetworkMap::empty(controller);
        let parent =
            enroll(&mut map, parent_server, &enrollment("/", "10.0.0.1:4564")).unwrap();
        map.clusters.iter_mut().find(|c| c.id == parent).unwrap().state =
            ClusterState::Active;
        let request = enrollment("/eu", "10.0.0.10:4564");
        let child = enroll(&mut map, first, &request).unwrap();
        let second_member = addr("10.0.0.11:4564");
        let mut expanded_members = request.resolver_members.clone();
        expanded_members.push(second_member.clone());
        let join = EnrollmentRequest {
            listen: "10.0.0.11:4565".parse().unwrap(),
            roles: vec![Role::Resolver],
            resolver_member: Some(second_member.clone()),
            resolver_members: vec![second_member],
            cluster: ClusterPlacement::Join { cluster: child },
        };
        assert_eq!(enroll(&mut map, second, &join).unwrap(), child);
        assert!(reparent(&mut map, child, parent).unwrap());
        assert_eq!(
            map.clusters.iter().find(|c| c.id == child).unwrap().state,
            ClusterState::Active
        );
        let child_facts = ClusterFacts {
            members: expanded_members,
            base: "/eu".into(),
            parent: Some(ClusterEdge {
                path: "/eu".into(),
                addrs: vec![addr("10.0.0.1:4564")],
            }),
            children: vec![],
        };
        assert!(
            register(
                &mut map,
                first,
                "10.0.0.10:4565".parse().unwrap(),
                Some(&child_facts),
            )
            .unwrap()
        );
        let mut wrong_mount = child_facts.clone();
        wrong_mount.parent.as_mut().unwrap().path = "/".into();
        assert!(
            register(
                &mut map,
                first,
                "10.0.0.12:4565".parse().unwrap(),
                Some(&wrong_mount),
            )
            .is_err()
        );
        let second_local_only =
            ClusterFacts { members: vec![addr("10.0.0.11:4564")], ..child_facts.clone() };
        assert!(
            register(
                &mut map,
                second,
                "10.0.0.11:4565".parse().unwrap(),
                Some(&second_local_only),
            )
            .unwrap()
        );
        assert!(deregister(&mut map, first).unwrap());
        assert!(remove(&mut map, first).unwrap());
        let remaining = map.clusters.iter().find(|c| c.id == child).unwrap();
        assert_eq!(remaining.members, vec![addr("10.0.0.11:4564")]);
        assert!(remove(&mut map, second).unwrap());
        assert!(!map.clusters.iter().any(|c| c.id == child));
    }

    #[test]
    fn split_peer_cluster_by_stable_server_identity() {
        let us1 = AdminServerId::new();
        let us2 = AdminServerId::new();
        let ap1 = AdminServerId::new();
        let ap2 = AdminServerId::new();
        let root = ResolverClusterId::new();
        let proposed_child = ResolverClusterId::new();
        let specs = [
            (us1, "10.0.0.1:4565", "10.0.0.1:4564", true),
            (us2, "10.0.0.2:4565", "10.0.0.2:4564", false),
            (ap1, "10.0.60.1:4565", "10.0.60.1:4564", false),
            (ap2, "10.0.60.2:4565", "10.0.60.2:4564", false),
        ];
        let servers: Vec<_> = specs
            .iter()
            .map(|(id, admin, member, ca)| ServerEntry {
                id: *id,
                addr: admin.parse().unwrap(),
                roles: if *ca {
                    vec![Role::Ca, Role::Resolver]
                } else {
                    vec![Role::Resolver]
                },
                resolver: Some(addr(member)),
                cluster: Some(root),
                state: ServerState::Registered,
            })
            .collect();
        let mut members: Vec<_> =
            specs.iter().map(|(_, _, member, _)| addr(member)).collect();
        normalize_addrs(&mut members);
        let mut map = NetworkMap {
            version: 7,
            controller: us1,
            servers,
            clusters: vec![ClusterEntry {
                id: root,
                base: "/".into(),
                state: ClusterState::Active,
                members,
                parent: None,
                children: vec![],
            }],
        };

        let change =
            delegate(&mut map, "/ap", proposed_child, &[us1, us2], &[ap1, ap2]).unwrap();
        assert!(change.changed);
        assert_eq!(change.parent.id, root);
        assert_eq!(change.parent.base, "/");
        assert_eq!(
            change.parent.members,
            vec![addr("10.0.0.1:4564"), addr("10.0.0.2:4564")]
        );
        assert_eq!(change.parent.children, vec![proposed_child]);
        assert_eq!(change.child.id, proposed_child);
        assert_eq!(change.child.base, "/ap");
        assert_eq!(change.child.parent, Some(root));
        assert_eq!(
            change.child.members,
            vec![addr("10.0.60.1:4564"), addr("10.0.60.2:4564")]
        );
        assert!(
            map.servers
                .iter()
                .filter(|s| [us1, us2].contains(&s.id))
                .all(|s| { s.cluster == Some(root) })
        );
        assert!(
            map.servers
                .iter()
                .filter(|s| [ap1, ap2].contains(&s.id))
                .all(|s| { s.cluster == Some(proposed_child) })
        );

        let version = map.version;
        let again =
            delegate(&mut map, "/ap", proposed_child, &[us2, us1], &[ap2, ap1]).unwrap();
        assert!(!again.changed);
        assert_eq!(map.version, version);
    }

    #[test]
    fn split_requires_a_complete_disjoint_partition() {
        let controller = AdminServerId::new();
        let second = AdminServerId::new();
        let third = AdminServerId::new();
        let cluster = ResolverClusterId::new();
        let mut map = NetworkMap {
            version: 0,
            controller,
            servers: [
                (controller, "10.0.0.1:4565", "10.0.0.1:4564"),
                (second, "10.0.0.2:4565", "10.0.0.2:4564"),
                (third, "10.0.0.3:4565", "10.0.0.3:4564"),
            ]
            .into_iter()
            .map(|(id, admin, member)| ServerEntry {
                id,
                addr: admin.parse().unwrap(),
                roles: vec![Role::Resolver],
                resolver: Some(addr(member)),
                cluster: Some(cluster),
                state: ServerState::Registered,
            })
            .collect(),
            clusters: vec![ClusterEntry {
                id: cluster,
                base: "/".into(),
                state: ClusterState::Active,
                members: vec![
                    addr("10.0.0.1:4564"),
                    addr("10.0.0.2:4564"),
                    addr("10.0.0.3:4564"),
                ],
                parent: None,
                children: vec![],
            }],
        };
        let before = map.clone();
        assert!(
            delegate(
                &mut map,
                "/ap",
                ResolverClusterId::new(),
                &[controller],
                &[second],
            )
            .unwrap_err()
            .to_string()
            .contains("assign every resolver server")
        );
        assert_eq!(map, before);
        assert!(
            delegate(
                &mut map,
                "/ap",
                ResolverClusterId::new(),
                &[controller, second],
                &[second, third],
            )
            .unwrap_err()
            .to_string()
            .contains("disjoint")
        );
        assert_eq!(map, before);
    }
}
