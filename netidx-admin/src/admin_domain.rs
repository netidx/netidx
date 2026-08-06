//! Durable CA-owned admin-plane topology and pure mutation helpers.

use crate::{
    admin_proto::{
        AdminDomainMap, AdminServerEntry, AdminServerId, EnrollmentRequest, ResolverAddr,
        ResolverClusterEdge, ResolverClusterEntry, ResolverClusterFacts,
        ResolverClusterId, ResolverClusterPlacement, ResolverClusterState, Role,
        ServerState,
    },
    atomic,
    config_lock::ConfigDirLock,
};
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};

pub fn path(ca_dir: &Path) -> PathBuf {
    ca_dir.join("admin-domain.json")
}

pub fn load(ca_dir: &Path, ca: AdminServerId) -> Result<AdminDomainMap> {
    let p = path(ca_dir);
    match std::fs::read(&p) {
        Ok(bytes) => {
            let map: AdminDomainMap = serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing admin domain map {p:?}"))?;
            if map.ca != ca {
                bail!(
                    "admin domain map CA {} does not match installed CA certificate {}",
                    map.ca,
                    ca
                );
            }
            Ok(map)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(AdminDomainMap::empty(ca))
        }
        Err(e) => Err(e).with_context(|| format!("reading admin domain map {p:?}")),
    }
}

pub fn save(
    config_lock: &ConfigDirLock,
    ca_dir: &Path,
    map: &AdminDomainMap,
) -> Result<()> {
    let ca_dir = config_lock.require_contained(ca_dir)?;
    atomic::write_atomic_pretty_json(&path(&ca_dir), map)
}

pub async fn load_async(ca_dir: &Path, ca: AdminServerId) -> Result<AdminDomainMap> {
    let p = path(ca_dir);
    match tokio::fs::read(&p).await {
        Ok(bytes) => {
            let map: AdminDomainMap = serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing admin domain map {p:?}"))?;
            if map.ca != ca {
                bail!(
                    "admin domain map CA {} does not match installed CA certificate {}",
                    map.ca,
                    ca
                );
            }
            Ok(map)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(AdminDomainMap::empty(ca))
        }
        Err(e) => Err(e).with_context(|| format!("reading admin domain map {p:?}")),
    }
}

pub async fn save_async(
    config_lock: &ConfigDirLock,
    ca_dir: &Path,
    map: &AdminDomainMap,
) -> Result<()> {
    let ca_dir = config_lock.require_contained(ca_dir)?;
    atomic::write_atomic_pretty_json_async(&path(&ca_dir), map).await
}

fn changed(map: &mut AdminDomainMap) {
    map.admin_servers.sort_by_key(|s| s.id);
    map.resolver_clusters.sort_by_key(|c| c.id);
    map.version = map.version.saturating_add(1);
}

pub fn upsert_ca(
    map: &mut AdminDomainMap,
    mut entry: AdminServerEntry,
    cluster: Option<ResolverClusterFacts>,
) -> Result<bool> {
    if entry.id != map.ca || !entry.roles.contains(Role::Ca) {
        bail!("CA entry must use the map CA id and carry the CA role");
    }
    entry.state = ServerState::Registered;
    entry.reported_read_gate = cluster.as_ref().map(|facts| facts.read_gated);
    let mut did_change = match map.admin_servers.iter_mut().find(|s| s.id == entry.id) {
        Some(old) if *old == entry => false,
        Some(old) => {
            *old = entry.clone();
            true
        }
        None => {
            map.admin_servers.push(entry.clone());
            true
        }
    };
    if let (Some(cluster_id), Some(facts)) = (entry.cluster, cluster) {
        match map.resolver_clusters.iter().find(|c| c.id == cluster_id) {
            // Startup facts are observations, not topology authority. They
            // seed a fresh map, but may be stale while a delegation fanout is
            // incomplete or while the CA awaits its manual rolling
            // resolver restart. Never let them undo a CA-owned split,
            // reparenting, or member assignment on an existing map.
            Some(_) => {}
            None => {
                map.resolver_clusters.push(ResolverClusterEntry {
                    id: cluster_id,
                    base: facts.base,
                    state: ResolverClusterState::Active,
                    members: entry.resolver.clone().into_iter().collect(),
                    parent: None,
                    children: Vec::new(),
                    perms_version: None,
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

/// Move one server's resolver endpoint without changing its identity, grant,
/// resolver cluster placement, or data-plane authentication.
pub fn relocate_resolver(
    map: &mut AdminDomainMap,
    server_id: AdminServerId,
    addr: std::net::SocketAddr,
) -> Result<bool> {
    let server = map
        .admin_servers
        .iter()
        .find(|server| server.id == server_id)
        .with_context(|| format!("admin server {server_id} does not exist"))?;
    if !server.roles.contains(Role::Resolver) {
        bail!("admin server {server_id} has no Resolver grant");
    }
    let old = server.resolver.clone().with_context(|| {
        format!("admin server {server_id} has no owned resolver member")
    })?;
    if old.addr == addr {
        return Ok(false);
    }
    if map.admin_servers.iter().any(|server| {
        server.id != server_id
            && server.resolver.as_ref().is_some_and(|resolver| resolver.addr == addr)
    }) {
        bail!("resolver member {addr} is already owned by another admin server");
    }
    let cluster_id = server.cluster.with_context(|| {
        format!("resolver admin server {server_id} has no resolver cluster")
    })?;
    let cluster = map
        .resolver_clusters
        .iter()
        .find(|cluster| cluster.id == cluster_id)
        .context("the server's assigned resolver cluster does not exist")?;
    if cluster.members.iter().filter(|member| **member == old).count() != 1 {
        bail!(
            "resolver cluster {} does not contain exactly one copy of server {}'s owned member",
            cluster.id,
            server_id,
        );
    }

    let replacement = ResolverAddr { addr, auth: old.auth.clone() };
    map.admin_servers
        .iter_mut()
        .find(|server| server.id == server_id)
        .expect("server checked above")
        .resolver = Some(replacement.clone());
    let cluster = map
        .resolver_clusters
        .iter_mut()
        .find(|cluster| cluster.id == cluster_id)
        .expect("resolver cluster checked above");
    *cluster
        .members
        .iter_mut()
        .find(|member| **member == old)
        .expect("member checked above") = replacement;
    normalize_addrs(&mut cluster.members);
    changed(map);
    Ok(true)
}

/// Insert the immutable grant at certificate issuance time. The enrollee does
/// not choose either identity: `server_id` and a created resolver cluster id come from
/// the CA.
pub fn enroll(
    map: &mut AdminDomainMap,
    server_id: AdminServerId,
    request: &EnrollmentRequest,
) -> Result<ResolverClusterId> {
    if map.admin_servers.iter().any(|s| s.id == server_id) {
        bail!("server identity {server_id} is already enrolled");
    }
    if request.roles.contains(Role::Ca) {
        bail!("an enrollee may never request the CA role");
    }
    if !request.roles.contains(Role::Resolver) {
        bail!("every non-ca admin server must have the Resolver role");
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
    if map.admin_servers.iter().any(|s| {
        s.resolver.as_ref().is_some_and(|member| member.addr == resolver_member.addr)
    }) {
        bail!(
            "resolver member {} is already owned by another admin server",
            resolver_member.addr
        );
    }
    let cluster = match &request.cluster {
        ResolverClusterPlacement::Create { base } => {
            if base.is_empty() || !base.starts_with('/') {
                bail!("resolver cluster base must be an absolute netidx path");
            }
            if map.resolver_clusters.iter().any(|c| c.base == *base) {
                bail!("a resolver cluster already exists at base {base:?}");
            }
            let id = ResolverClusterId::new();
            map.resolver_clusters.push(ResolverClusterEntry {
                id,
                base: base.clone(),
                state: ResolverClusterState::Pending,
                // Resolver cluster membership is the union of CA-issued server
                // identities, not the local resolver.json launch menu.
                members: vec![resolver_member.clone()],
                parent: None,
                children: Vec::new(),
                perms_version: None,
            });
            id
        }
        ResolverClusterPlacement::Join { cluster } => {
            let approved = map
                .resolver_clusters
                .iter_mut()
                .find(|c| c.id == *cluster)
                .context("requested resolver cluster does not exist")?;
            for configured in &request.resolver_members {
                if configured != resolver_member && !approved.members.contains(configured)
                {
                    bail!(
                        "local resolver config names unowned member {} outside the approved resolver cluster",
                        configured.addr
                    );
                }
            }
            approved.members.push(resolver_member.clone());
            normalize_addrs(&mut approved.members);
            *cluster
        }
    };
    map.admin_servers.push(AdminServerEntry {
        id: server_id,
        addr: request.listen,
        roles: request.roles,
        resolver: Some(resolver_member.clone()),
        cluster: Some(cluster),
        state: ServerState::Enrolled,
        reported_read_gate: None,
        reported_id_map_version: None,
        reported_perms_version: None,
    });
    changed(map);
    Ok(cluster)
}

fn server_set(
    map: &AdminDomainMap,
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
            .admin_servers
            .iter()
            .find(|s| s.id == *id)
            .with_context(|| format!("admin server {id} does not exist"))?;
        if require_registered && server.state != ServerState::Registered {
            bail!("parent admin server {id} is not registered");
        }
        if !server.roles.contains(Role::Resolver) {
            bail!("admin server {id} has no Resolver grant");
        }
        let id_cluster = server.cluster.with_context(|| {
            format!("resolver admin server {id} has no resolver cluster")
        })?;
        match cluster {
            None => cluster = Some(id_cluster),
            Some(expected) if expected == id_cluster => {}
            Some(_) => {
                bail!("the selected servers do not belong to one resolver cluster")
            }
        }
        members.push(server.resolver.clone().with_context(|| {
            format!("admin server {id} has no owned resolver member")
        })?);
    }
    normalize_addrs(&mut members);
    Ok((cluster.expect("nonempty ids"), members))
}

/// The namespace base of the resolver cluster `parent_servers` belong to — the subtree
/// a delegation under them would restructure. Authorization needs this before
/// [`delegate`] runs, because `delegate` only checks that the proposed child
/// path lies *under* this base, which says nothing about who owns the base.
pub fn parent_base(
    map: &AdminDomainMap,
    parent_servers: &[AdminServerId],
) -> Result<String> {
    let (cluster, _) = server_set(map, &canonical_ids(parent_servers), true)?;
    Ok(map
        .resolver_clusters
        .iter()
        .find(|entry| entry.id == cluster)
        .context("parent resolver cluster does not exist")?
        .base
        .clone())
}

fn assigned_servers(
    map: &AdminDomainMap,
    cluster: ResolverClusterId,
) -> Vec<AdminServerId> {
    let mut ids: Vec<_> = map
        .admin_servers
        .iter()
        .filter(|s| s.cluster == Some(cluster) && s.roles.contains(Role::Resolver))
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
    pub parent: ResolverClusterEntry,
    pub child: ResolverClusterEntry,
    pub changed: bool,
}

/// Apply a delegation proposal expressed as immutable server sets. If both
/// sets currently belong to one resolver cluster, split it: the parent set retains the
/// old resolver cluster ID and the child set receives `proposed_child`. If they belong
/// to distinct resolver clusters, attach/rebase the complete child resolver cluster. Reapplying
/// the final proposal is idempotent.
pub fn delegate(
    map: &mut AdminDomainMap,
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
        .resolver_clusters
        .iter()
        .position(|c| c.id == parent_id)
        .context("parent resolver cluster does not exist")?;
    if map.resolver_clusters[parent_pos].state != ResolverClusterState::Active {
        bail!("the parent resolver cluster is not active");
    }
    if !NPath::is_parent(&map.resolver_clusters[parent_pos].base, &path) {
        bail!(
            "delegated path {proposed_path:?} is outside parent base {:?}",
            map.resolver_clusters[parent_pos].base
        );
    }

    // An already-applied split/attach resolves the two sets to different
    // resolver clusters and lands here. The ordinary attach path below recognizes it
    // and returns without version churn.
    if parent_id != current_child_id {
        let child_pos = map
            .resolver_clusters
            .iter()
            .position(|c| c.id == current_child_id)
            .context("child resolver cluster does not exist")?;
        let mut descendants = std::collections::BTreeSet::new();
        let mut pending = map.resolver_clusters[child_pos].children.clone();
        while let Some(id) = pending.pop() {
            if !descendants.insert(id) {
                continue;
            }
            let cluster =
                map.resolver_clusters
                    .iter()
                    .find(|cluster| cluster.id == id)
                    .context("child topology references a missing resolver cluster")?;
            pending.extend(cluster.children.iter().copied());
        }
        if descendants.contains(&parent_id) {
            bail!("attaching these resolver clusters would create a topology cycle");
        }
        for id in &descendants {
            let descendant =
                map.resolver_clusters.iter().find(|cluster| cluster.id == *id).unwrap();
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
                "the parent selection must include every resolver server in its resolver cluster"
            );
        }
        if expected_child_ids != child_servers {
            bail!(
                "the child selection must include every resolver server in its resolver cluster"
            );
        }
        let already = map.resolver_clusters[child_pos].base == proposed_path
            && map.resolver_clusters[child_pos].parent == Some(parent_id)
            && map.resolver_clusters[child_pos].state == ResolverClusterState::Active
            && map.resolver_clusters[parent_pos].children.contains(&current_child_id);
        if already {
            return Ok(DelegationChange {
                parent: map.resolver_clusters[parent_pos].clone(),
                child: map.resolver_clusters[child_pos].clone(),
                changed: false,
            });
        }
        if map.resolver_clusters[child_pos].parent.is_some() {
            bail!("the selected child resolver cluster is already attached");
        }
        if map
            .resolver_clusters
            .iter()
            .any(|c| c.id != current_child_id && c.base == proposed_path)
        {
            bail!("another resolver cluster already owns {proposed_path:?}");
        }
        map.resolver_clusters[child_pos].base = proposed_path.to_string();
        map.resolver_clusters[child_pos].members = child_members;
        map.resolver_clusters[child_pos].parent = Some(parent_id);
        map.resolver_clusters[child_pos].state = ResolverClusterState::Active;
        map.resolver_clusters[parent_pos].members = parent_members;
        if !map.resolver_clusters[parent_pos].children.contains(&current_child_id) {
            map.resolver_clusters[parent_pos].children.push(current_child_id);
            map.resolver_clusters[parent_pos].children.sort();
        }
        changed(map);
        return Ok(DelegationChange {
            parent: map
                .resolver_clusters
                .iter()
                .find(|c| c.id == parent_id)
                .unwrap()
                .clone(),
            child: map
                .resolver_clusters
                .iter()
                .find(|c| c.id == current_child_id)
                .unwrap()
                .clone(),
            changed: true,
        });
    }

    // Split one active peer resolver cluster. The proposal must partition every
    // resolver identity assigned to it; otherwise approval would silently
    // orphan an unmentioned member.
    let mut union = parent_servers.clone();
    union.extend(child_servers.iter().copied());
    union.sort();
    if union != assigned_servers(map, parent_id) {
        bail!(
            "a resolver cluster split must assign every resolver server to parent or child"
        );
    }
    if map.resolver_clusters.iter().any(|c| c.id == proposed_child) {
        bail!("the proposed child resolver cluster identity is already in use");
    }
    if map.resolver_clusters.iter().any(|c| c.base == proposed_path) {
        bail!("another resolver cluster already owns {proposed_path:?}");
    }

    let old_children = map.resolver_clusters[parent_pos].children.clone();
    let mut parent_children = Vec::new();
    let mut child_children = Vec::new();
    for id in old_children {
        let Some(existing) = map.resolver_clusters.iter().find(|c| c.id == id) else {
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
    map.resolver_clusters[parent_pos].members = parent_members;
    map.resolver_clusters[parent_pos].children = parent_children;
    for id in &child_children {
        if let Some(cluster) = map.resolver_clusters.iter_mut().find(|c| c.id == *id) {
            cluster.parent = Some(proposed_child);
        }
    }
    map.resolver_clusters.push(ResolverClusterEntry {
        id: proposed_child,
        base: proposed_path.to_string(),
        state: ResolverClusterState::Active,
        members: child_members,
        parent: Some(parent_id),
        children: child_children,
        perms_version: None,
    });
    for server in &mut map.admin_servers {
        if child_servers.contains(&server.id) {
            server.cluster = Some(proposed_child);
        }
    }
    changed(map);
    Ok(DelegationChange {
        parent: map.resolver_clusters.iter().find(|c| c.id == parent_id).unwrap().clone(),
        child: map
            .resolver_clusters
            .iter()
            .find(|c| c.id == proposed_child)
            .unwrap()
            .clone(),
        changed: true,
    })
}

fn edge_for(map: &AdminDomainMap, id: ResolverClusterId) -> Option<ResolverClusterEdge> {
    map.resolver_clusters
        .iter()
        .find(|c| c.id == id)
        .map(|c| ResolverClusterEdge { path: c.base.clone(), addrs: c.members.clone() })
}

fn normalize_addrs(addrs: &mut Vec<ResolverAddr>) {
    addrs.sort_by(|a, b| a.addr.cmp(&b.addr));
    addrs.dedup_by(|a, b| a == b);
}

fn facts_match(
    map: &AdminDomainMap,
    cluster: &ResolverClusterEntry,
    owned: Option<&ResolverAddr>,
    facts: &ResolverClusterFacts,
) -> bool {
    let mut expected_members = cluster.members.clone();
    let mut got_members = facts.members.clone();
    normalize_addrs(&mut expected_members);
    normalize_addrs(&mut got_members);
    // A child's parent referral is mounted at the *child's* base (for example
    // `/eu`) while its addresses come from the parent resolver cluster. Using the
    // parent's own base here (`/` for the root) rejects every correctly
    // configured non-root server as topology drift.
    let expected_parent = cluster.parent.and_then(|id| {
        map.resolver_clusters.iter().find(|c| c.id == id).map(|parent| {
            ResolverClusterEdge {
                path: cluster.base.clone(),
                addrs: parent.members.clone(),
            }
        })
    });
    let mut expected_children: Vec<ResolverClusterEdge> =
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
    map: &mut AdminDomainMap,
    server_id: AdminServerId,
    addr: std::net::SocketAddr,
    resolver: Option<&ResolverClusterFacts>,
    id_map_version: Option<u64>,
    perms_version: Option<u64>,
) -> Result<bool> {
    let pos = map
        .admin_servers
        .iter()
        .position(|s| s.id == server_id)
        .context("the authenticated server has no enrollment grant")?;
    let cluster_id = map.admin_servers[pos].cluster;
    let owned = map.admin_servers[pos].resolver.clone();
    match (cluster_id, resolver) {
        (Some(id), Some(facts)) => {
            let cluster = map
                .resolver_clusters
                .iter()
                .find(|c| c.id == id)
                .context("the server grant references a missing resolver cluster")?;
            if !facts_match(map, cluster, owned.as_ref(), facts) {
                bail!(
                    "reported resolver configuration drifts from the CA-approved resolver cluster"
                );
            }
        }
        (Some(_), None) => bail!("the approved Resolver role must report resolver facts"),
        (None, Some(_)) => {
            bail!("resolver facts were reported without an approved resolver cluster")
        }
        (None, None) => {}
    }
    // A newly granted non-root resolver cluster stays pending until delegation attaches it
    // to an active parent. The first `/` resolver cluster below a dedicated CA has
    // no such ceremony: registration of its approved first member is what makes
    // the admin domain routable.
    let activate_root = cluster_id.is_some_and(|id| {
        map.resolver_clusters.iter().any(|cluster| {
            cluster.id == id
                && cluster.base == "/"
                && cluster.parent.is_none()
                && cluster.state == ResolverClusterState::Pending
        })
    });
    // Status, deliberately outside `facts_match`: a host that has taken itself
    // out of service has not drifted from its grant, it is still exactly the
    // member the CA approved. Recording it is how an operator sees that a gate
    // the CA pushed actually landed.
    let gate = resolver.map(|facts| facts.read_gated);
    let server = &mut map.admin_servers[pos];
    let server_changed = server.addr != addr
        || server.state != ServerState::Registered
        || server.reported_read_gate != gate
        || server.reported_id_map_version != id_map_version
        || server.reported_perms_version != perms_version;
    if server_changed {
        server.addr = addr;
        server.state = ServerState::Registered;
        server.reported_read_gate = gate;
        server.reported_id_map_version = id_map_version;
        server.reported_perms_version = perms_version;
    }
    if activate_root {
        map.resolver_clusters
            .iter_mut()
            .find(|cluster| Some(cluster.id) == cluster_id)
            .unwrap()
            .state = ResolverClusterState::Active;
    }
    if server_changed || activate_root {
        changed(map);
    }
    Ok(server_changed || activate_root)
}

pub fn deregister(map: &mut AdminDomainMap, server_id: AdminServerId) -> Result<bool> {
    let server = map
        .admin_servers
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

pub fn remove(map: &mut AdminDomainMap, server_id: AdminServerId) -> Result<bool> {
    if server_id == map.ca {
        bail!("the active CA cannot be removed; replace and revoke it first");
    }
    let before = map.admin_servers.len();
    map.admin_servers.retain(|s| s.id != server_id);
    if before == map.admin_servers.len() {
        return Ok(false);
    }
    let used: std::collections::BTreeSet<_> =
        map.admin_servers.iter().filter_map(|s| s.cluster).collect();
    let removed_clusters: std::collections::BTreeSet<_> = map
        .resolver_clusters
        .iter()
        .filter(|c| !used.contains(&c.id))
        .map(|c| c.id)
        .collect();
    map.resolver_clusters.retain(|c| !removed_clusters.contains(&c.id));
    for cluster in &mut map.resolver_clusters {
        let mut owned: Vec<_> = map
            .admin_servers
            .iter()
            .filter(|server| server.cluster == Some(cluster.id))
            .filter_map(|server| server.resolver.clone())
            .collect();
        normalize_addrs(&mut owned);
        cluster.members = owned;
        if cluster.parent.is_some_and(|id| removed_clusters.contains(&id)) {
            cluster.parent = None;
            cluster.state = ResolverClusterState::Pending;
        }
        cluster.children.retain(|id| !removed_clusters.contains(id));
    }
    changed(map);
    Ok(true)
}

pub fn reparent(
    map: &mut AdminDomainMap,
    child: ResolverClusterId,
    parent: ResolverClusterId,
) -> Result<bool> {
    if child == parent {
        bail!("a resolver cluster cannot be its own parent");
    }
    let child_pos = map
        .resolver_clusters
        .iter()
        .position(|c| c.id == child)
        .context("child resolver cluster does not exist")?;
    let parent_pos = map
        .resolver_clusters
        .iter()
        .position(|c| c.id == parent)
        .context("parent resolver cluster does not exist")?;
    if map.resolver_clusters[parent_pos].state != ResolverClusterState::Active {
        bail!("the parent resolver cluster is not active");
    }
    let old_parent = map.resolver_clusters[child_pos].parent;
    let already = old_parent == Some(parent)
        && map.resolver_clusters[child_pos].state == ResolverClusterState::Active
        && map.resolver_clusters[parent_pos].children.contains(&child);
    if already {
        return Ok(false);
    }
    if let Some(old) = old_parent
        && let Some(old) = map.resolver_clusters.iter_mut().find(|c| c.id == old)
    {
        old.children.retain(|id| *id != child);
    }
    map.resolver_clusters[child_pos].parent = Some(parent);
    map.resolver_clusters[child_pos].state = ResolverClusterState::Active;
    if !map.resolver_clusters[parent_pos].children.contains(&child) {
        map.resolver_clusters[parent_pos].children.push(child);
        map.resolver_clusters[parent_pos].children.sort();
    }
    changed(map);
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_proto::{InfoAuth, ServerState};
    use netidx::resolver_server::config::ReadGate;

    fn addr(s: &str) -> ResolverAddr {
        ResolverAddr { addr: s.parse().unwrap(), auth: InfoAuth::Anonymous }
    }

    fn enrollment(base: &str, member: &str) -> EnrollmentRequest {
        let member = addr(member);
        EnrollmentRequest {
            resolver_config: None,
            listen: "10.0.0.10:4565".parse().unwrap(),
            roles: Role::Resolver.into(),
            resolver_member: Some(member.clone()),
            resolver_members: vec![member],
            cluster: ResolverClusterPlacement::Create { base: base.into() },
            replaces: None,
        }
    }

    #[test]
    fn resolver_relocation_changes_only_the_owned_endpoint() {
        let ca = AdminServerId::new();
        let peer = AdminServerId::new();
        let cluster = ResolverClusterId::new();
        let old = ResolverAddr {
            addr: "10.0.0.1:4564".parse().unwrap(),
            auth: InfoAuth::Tls { name: "resolver.example.com".into() },
        };
        let peer_addr = addr("10.0.0.2:4564");
        let ca_entry = AdminServerEntry {
            id: ca,
            addr: "10.0.0.1:4565".parse().unwrap(),
            roles: Role::Ca | Role::Resolver,
            resolver: Some(old.clone()),
            cluster: Some(cluster),
            state: ServerState::Registered,
            reported_read_gate: None,
            reported_id_map_version: None,
            reported_perms_version: None,
        };
        let peer_entry = AdminServerEntry {
            id: peer,
            addr: "10.0.0.2:4565".parse().unwrap(),
            roles: Role::Resolver.into(),
            resolver: Some(peer_addr.clone()),
            cluster: Some(cluster),
            state: ServerState::Registered,
            reported_read_gate: None,
            reported_id_map_version: None,
            reported_perms_version: None,
        };
        let mut map = AdminDomainMap {
            version: 7,
            ca,
            admin_servers: vec![ca_entry.clone(), peer_entry.clone()],
            resolver_clusters: vec![ResolverClusterEntry {
                id: cluster,
                base: "/".into(),
                state: ResolverClusterState::Active,
                members: vec![old.clone(), peer_addr.clone()],
                parent: None,
                children: vec![],
                perms_version: None,
            }],
            id_map_version: None,
        };

        let new_addr = "10.1.0.1:5564".parse().unwrap();
        assert!(relocate_resolver(&mut map, ca, new_addr).unwrap());
        assert_eq!(map.version, 8);
        let moved = map.ca_entry().unwrap();
        assert_eq!(moved.addr, ca_entry.addr);
        assert_eq!(moved.roles, ca_entry.roles);
        assert_eq!(moved.cluster, ca_entry.cluster);
        assert_eq!(moved.state, ca_entry.state);
        assert_eq!(
            moved.resolver,
            Some(ResolverAddr { addr: new_addr, auth: old.auth.clone() })
        );
        assert_eq!(
            map.admin_servers.iter().find(|server| server.id == peer),
            Some(&peer_entry)
        );
        assert_eq!(
            map.resolver_clusters[0].members,
            vec![peer_addr, ResolverAddr { addr: new_addr, auth: old.auth }]
        );
        assert!(!relocate_resolver(&mut map, ca, new_addr).unwrap());
        assert_eq!(map.version, 8);
    }

    #[test]
    fn resolver_relocation_rejects_another_servers_endpoint() {
        let ca = AdminServerId::new();
        let peer = AdminServerId::new();
        let cluster = ResolverClusterId::new();
        let old = addr("10.0.0.1:4564");
        let peer_addr = addr("10.0.0.2:4564");
        let mut map = AdminDomainMap {
            id_map_version: None,
            version: 3,
            ca,
            admin_servers: vec![
                AdminServerEntry {
                    id: ca,
                    addr: "10.0.0.1:4565".parse().unwrap(),
                    roles: Role::Ca | Role::Resolver,
                    resolver: Some(old.clone()),
                    cluster: Some(cluster),
                    state: ServerState::Registered,
                    reported_read_gate: None,
                    reported_id_map_version: None,
                    reported_perms_version: None,
                },
                AdminServerEntry {
                    id: peer,
                    addr: "10.0.0.2:4565".parse().unwrap(),
                    roles: Role::Resolver.into(),
                    resolver: Some(peer_addr.clone()),
                    cluster: Some(cluster),
                    state: ServerState::Registered,
                    reported_read_gate: None,
                    reported_id_map_version: None,
                    reported_perms_version: None,
                },
            ],
            resolver_clusters: vec![ResolverClusterEntry {
                id: cluster,
                base: "/".into(),
                state: ResolverClusterState::Active,
                members: vec![old, peer_addr.clone()],
                parent: None,
                children: vec![],
                perms_version: None,
            }],
        };
        let before = map.clone();

        assert!(relocate_resolver(&mut map, ca, peer_addr.addr).is_err());
        assert_eq!(map, before);
    }

    #[test]
    fn permanent_removal_never_accepts_the_active_ca() {
        let ca = AdminServerId::new();
        let mut map = AdminDomainMap::empty(ca);
        let before = map.clone();
        let error = remove(&mut map, ca).unwrap_err().to_string();
        assert!(error.contains("active CA"));
        assert_eq!(map.ca, before.ca);
        assert_eq!(map.version, before.version);
        assert_eq!(map.admin_servers.len(), before.admin_servers.len());
    }

    #[test]
    fn enrollment_registration_and_self_only_address_update() {
        let ca = AdminServerId::new();
        let server = AdminServerId::new();
        let mut map = AdminDomainMap::empty(ca);
        let request = enrollment("/eu", "10.0.0.10:4564");
        let cluster = enroll(&mut map, server, &request).unwrap();
        assert_eq!(map.admin_servers[0].state, ServerState::Enrolled);
        assert_eq!(map.resolver_clusters[0].state, ResolverClusterState::Pending);
        let facts = ResolverClusterFacts {
            members: request.resolver_members.clone(),
            base: "/eu".into(),
            parent: None,
            children: vec![],
            read_gated: ReadGate::No,
        };
        assert!(
            register(
                &mut map,
                server,
                "10.0.0.20:4565".parse().unwrap(),
                Some(&facts),
                None,
                None
            )
            .unwrap()
        );
        assert_eq!(map.admin_servers[0].id, server);
        assert_eq!(map.admin_servers[0].cluster, Some(cluster));
        assert_eq!(map.admin_servers[0].roles, Role::Resolver);
        assert_eq!(map.admin_servers[0].addr, "10.0.0.20:4565".parse().unwrap());
        assert_eq!(map.resolver_clusters[0].state, ResolverClusterState::Pending);
        let mut drift = facts.clone();
        drift.base = "/us".into();
        assert!(
            register(
                &mut map,
                server,
                "10.0.0.30:4565".parse().unwrap(),
                Some(&drift),
                None,
                None
            )
            .is_err()
        );
        assert_eq!(map.admin_servers[0].addr, "10.0.0.20:4565".parse().unwrap());
    }

    #[test]
    fn registration_records_the_reported_read_gate_without_calling_it_drift() {
        let ca = AdminServerId::new();
        let server = AdminServerId::new();
        let mut map = AdminDomainMap::empty(ca);
        let request = enrollment("/eu", "10.0.0.10:4564");
        enroll(&mut map, server, &request).unwrap();
        assert_eq!(map.admin_servers[0].reported_read_gate, None);
        let addr = "10.0.0.20:4565".parse().unwrap();
        let mut facts = ResolverClusterFacts {
            members: request.resolver_members.clone(),
            base: "/eu".into(),
            parent: None,
            children: vec![],
            read_gated: ReadGate::No,
        };
        assert!(register(&mut map, server, addr, Some(&facts), None, None).unwrap());
        assert_eq!(map.admin_servers[0].reported_read_gate, Some(ReadGate::No));
        let version = map.version;
        // A host that has taken itself out of service has not drifted from
        // its grant — it is still exactly the member the CA approved.
        facts.read_gated = ReadGate::Yes;
        assert!(register(&mut map, server, addr, Some(&facts), None, None).unwrap());
        assert_eq!(map.admin_servers[0].reported_read_gate, Some(ReadGate::Yes));
        assert_eq!(map.admin_servers[0].state, ServerState::Registered);
        assert!(map.version > version, "a gate change has to reach map readers");
        // ... and reporting the same gate again is not a change.
        assert!(!register(&mut map, server, addr, Some(&facts), None, None).unwrap());
    }

    /// The versions a host reports are recorded the same way its read gate
    /// is: facts about the host, not grants. They are what lets the CA tell a
    /// host that missed a change from one that is current, without asking it
    /// for its whole state every poll.
    #[test]
    fn registration_records_the_reported_versions() {
        let ca = AdminServerId::new();
        let server = AdminServerId::new();
        let mut map = AdminDomainMap::empty(ca);
        let request = enrollment("/eu", "10.0.0.10:4564");
        enroll(&mut map, server, &request).unwrap();
        let addr = "10.0.0.20:4565".parse().unwrap();
        let facts = ResolverClusterFacts {
            members: request.resolver_members.clone(),
            base: "/eu".into(),
            parent: None,
            children: vec![],
            read_gated: ReadGate::No,
        };
        assert_eq!(map.admin_servers[0].reported_id_map_version, None);
        assert_eq!(map.admin_servers[0].reported_perms_version, None);
        assert!(
            register(&mut map, server, addr, Some(&facts), Some(4), Some(2)).unwrap()
        );
        assert_eq!(map.admin_servers[0].reported_id_map_version, Some(4));
        assert_eq!(map.admin_servers[0].reported_perms_version, Some(2));
        // Reporting the same versions again is not a change — a host that is
        // current must not churn the map version on every poll.
        assert!(
            !register(&mut map, server, addr, Some(&facts), Some(4), Some(2)).unwrap()
        );
        let version = map.version;
        assert!(
            register(&mut map, server, addr, Some(&facts), Some(5), Some(2)).unwrap()
        );
        assert_eq!(map.admin_servers[0].reported_id_map_version, Some(5));
        assert!(map.version > version, "map readers have to see it move");
        // Either one moving on its own is a change.
        let version = map.version;
        assert!(
            register(&mut map, server, addr, Some(&facts), Some(5), Some(3)).unwrap()
        );
        assert_eq!(map.admin_servers[0].reported_perms_version, Some(3));
        assert!(map.version > version);
    }

    #[test]
    fn first_root_resolver_below_a_dedicated_ca_activates_on_registration() {
        let ca = AdminServerId::new();
        let server = AdminServerId::new();
        let mut map = AdminDomainMap::empty(ca);
        let request = enrollment("/", "10.0.0.10:4564");
        let cluster = enroll(&mut map, server, &request).unwrap();
        assert_eq!(map.resolver_clusters[0].state, ResolverClusterState::Pending);
        let facts = ResolverClusterFacts {
            members: request.resolver_members.clone(),
            base: "/".into(),
            parent: None,
            children: vec![],
            read_gated: ReadGate::No,
        };
        assert!(
            register(
                &mut map,
                server,
                "10.0.0.10:4565".parse().unwrap(),
                Some(&facts),
                None,
                None
            )
            .unwrap()
        );
        assert_eq!(map.resolver_clusters[0].id, cluster);
        assert_eq!(map.resolver_clusters[0].state, ResolverClusterState::Active);
        assert_eq!(map.admin_servers[0].state, ServerState::Registered);
        assert!(
            !register(
                &mut map,
                server,
                "10.0.0.10:4565".parse().unwrap(),
                Some(&facts),
                None,
                None
            )
            .unwrap()
        );
    }

    #[test]
    fn ca_registration_preserves_delegated_children() {
        let ca = AdminServerId::new();
        let root = ResolverClusterId::new();
        let child = ResolverClusterId::new();
        let ca_entry = AdminServerEntry {
            id: ca,
            addr: "10.0.0.1:4565".parse().unwrap(),
            roles: Role::Ca | Role::Resolver,
            resolver: Some(addr("10.0.0.1:4564")),
            cluster: Some(root),
            state: ServerState::Registered,
            reported_read_gate: Some(ReadGate::No),
            reported_id_map_version: None,
            reported_perms_version: None,
        };
        let facts = ResolverClusterFacts {
            members: vec![addr("10.0.0.1:4564")],
            base: "/".into(),
            parent: None,
            children: vec![ResolverClusterEdge {
                path: "/eu".into(),
                addrs: vec![addr("10.0.0.2:4564")],
            }],
            read_gated: ReadGate::No,
        };
        let mut map = AdminDomainMap {
            version: 1,
            ca,
            admin_servers: vec![ca_entry.clone()],
            resolver_clusters: vec![
                ResolverClusterEntry {
                    id: root,
                    base: "/".into(),
                    state: ResolverClusterState::Active,
                    members: facts.members.clone(),
                    parent: None,
                    children: vec![child],
                    perms_version: None,
                },
                ResolverClusterEntry {
                    id: child,
                    base: "/eu".into(),
                    state: ResolverClusterState::Active,
                    members: vec![addr("10.0.0.2:4564")],
                    parent: Some(root),
                    children: vec![],
                    perms_version: None,
                },
            ],
            id_map_version: None,
        };

        assert!(!upsert_ca(&mut map, ca_entry, Some(facts)).unwrap());
        let root = map.resolver_clusters.iter().find(|c| c.id == root).unwrap();
        assert_eq!(root.children, vec![child]);
    }

    #[test]
    fn ca_startup_does_not_undo_its_child_assignment() {
        let ca = AdminServerId::new();
        let root_server = AdminServerId::new();
        let root = ResolverClusterId::new();
        let child = ResolverClusterId::new();
        let ca_entry = AdminServerEntry {
            id: ca,
            addr: "10.0.60.1:4565".parse().unwrap(),
            roles: Role::Ca | Role::Resolver,
            resolver: Some(addr("10.0.60.1:4564")),
            cluster: Some(child),
            state: ServerState::Registered,
            reported_read_gate: Some(ReadGate::No),
            reported_id_map_version: None,
            reported_perms_version: None,
        };
        let authoritative_child = ResolverClusterEntry {
            id: child,
            base: "/ap".into(),
            state: ResolverClusterState::Active,
            members: vec![addr("10.0.60.1:4564")],
            parent: Some(root),
            children: vec![],
            perms_version: None,
        };
        let mut map = AdminDomainMap {
            id_map_version: None,
            version: 3,
            ca,
            admin_servers: vec![
                ca_entry.clone(),
                AdminServerEntry {
                    id: root_server,
                    addr: "10.0.0.1:4565".parse().unwrap(),
                    roles: Role::Resolver.into(),
                    resolver: Some(addr("10.0.0.1:4564")),
                    cluster: Some(root),
                    state: ServerState::Registered,
                    reported_read_gate: None,
                    reported_id_map_version: None,
                    reported_perms_version: None,
                },
            ],
            resolver_clusters: vec![
                ResolverClusterEntry {
                    id: root,
                    base: "/".into(),
                    state: ResolverClusterState::Active,
                    members: vec![addr("10.0.0.1:4564")],
                    parent: None,
                    children: vec![child],
                    perms_version: None,
                },
                authoritative_child.clone(),
            ],
        };
        // The resolver file may still contain its pre-split peer topology
        // until the CA fanout and manual rolling restart complete.
        let stale = ResolverClusterFacts {
            members: vec![addr("10.0.0.1:4564"), addr("10.0.60.1:4564")],
            base: "/".into(),
            parent: None,
            children: vec![],
            read_gated: ReadGate::No,
        };

        assert!(!upsert_ca(&mut map, ca_entry, Some(stale)).unwrap());
        assert_eq!(
            map.resolver_clusters.iter().find(|c| c.id == child),
            Some(&authoritative_child)
        );
        assert_eq!(map.version, 3);
    }

    #[test]
    fn stable_cluster_join_deregister_remove_and_reparent() {
        let ca = AdminServerId::new();
        let first = AdminServerId::new();
        let second = AdminServerId::new();
        let parent_server = AdminServerId::new();
        let mut map = AdminDomainMap::empty(ca);
        let parent =
            enroll(&mut map, parent_server, &enrollment("/", "10.0.0.1:4564")).unwrap();
        map.resolver_clusters.iter_mut().find(|c| c.id == parent).unwrap().state =
            ResolverClusterState::Active;
        let request = enrollment("/eu", "10.0.0.10:4564");
        let child = enroll(&mut map, first, &request).unwrap();
        let second_member = addr("10.0.0.11:4564");
        let mut expanded_members = request.resolver_members.clone();
        expanded_members.push(second_member.clone());
        let join = EnrollmentRequest {
            resolver_config: None,
            listen: "10.0.0.11:4565".parse().unwrap(),
            roles: Role::Resolver.into(),
            resolver_member: Some(second_member.clone()),
            resolver_members: vec![second_member],
            cluster: ResolverClusterPlacement::Join { cluster: child },
            replaces: None,
        };
        assert_eq!(enroll(&mut map, second, &join).unwrap(), child);
        assert!(reparent(&mut map, child, parent).unwrap());
        assert_eq!(
            map.resolver_clusters.iter().find(|c| c.id == child).unwrap().state,
            ResolverClusterState::Active
        );
        let child_facts = ResolverClusterFacts {
            members: expanded_members,
            base: "/eu".into(),
            parent: Some(ResolverClusterEdge {
                path: "/eu".into(),
                addrs: vec![addr("10.0.0.1:4564")],
            }),
            children: vec![],
            read_gated: ReadGate::No,
        };
        assert!(
            register(
                &mut map,
                first,
                "10.0.0.10:4565".parse().unwrap(),
                Some(&child_facts),
                None,
                None,
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
                None,
                None,
            )
            .is_err()
        );
        let second_local_only = ResolverClusterFacts {
            members: vec![addr("10.0.0.11:4564")],
            ..child_facts.clone()
        };
        assert!(
            register(
                &mut map,
                second,
                "10.0.0.11:4565".parse().unwrap(),
                Some(&second_local_only),
                None,
                None,
            )
            .unwrap()
        );
        assert!(deregister(&mut map, first).unwrap());
        assert!(remove(&mut map, first).unwrap());
        let remaining = map.resolver_clusters.iter().find(|c| c.id == child).unwrap();
        assert_eq!(remaining.members, vec![addr("10.0.0.11:4564")]);
        assert!(remove(&mut map, second).unwrap());
        assert!(!map.resolver_clusters.iter().any(|c| c.id == child));
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
        let admin_servers: Vec<_> = specs
            .iter()
            .map(|(id, admin, member, ca)| AdminServerEntry {
                id: *id,
                addr: admin.parse().unwrap(),
                roles: if *ca {
                    Role::Ca | Role::Resolver
                } else {
                    Role::Resolver.into()
                },
                resolver: Some(addr(member)),
                cluster: Some(root),
                state: ServerState::Registered,
                reported_read_gate: None,
                reported_id_map_version: None,
                reported_perms_version: None,
            })
            .collect();
        let mut members: Vec<_> =
            specs.iter().map(|(_, _, member, _)| addr(member)).collect();
        normalize_addrs(&mut members);
        let mut map = AdminDomainMap {
            version: 7,
            ca: us1,
            admin_servers,
            resolver_clusters: vec![ResolverClusterEntry {
                id: root,
                base: "/".into(),
                state: ResolverClusterState::Active,
                members,
                parent: None,
                children: vec![],
                perms_version: None,
            }],
            id_map_version: None,
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
            map.admin_servers
                .iter()
                .filter(|s| [us1, us2].contains(&s.id))
                .all(|s| { s.cluster == Some(root) })
        );
        assert!(
            map.admin_servers
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
        let ca = AdminServerId::new();
        let second = AdminServerId::new();
        let third = AdminServerId::new();
        let cluster = ResolverClusterId::new();
        let mut map = AdminDomainMap {
            id_map_version: None,
            version: 0,
            ca,
            admin_servers: [
                (ca, "10.0.0.1:4565", "10.0.0.1:4564"),
                (second, "10.0.0.2:4565", "10.0.0.2:4564"),
                (third, "10.0.0.3:4565", "10.0.0.3:4564"),
            ]
            .into_iter()
            .map(|(id, admin, member)| AdminServerEntry {
                id,
                addr: admin.parse().unwrap(),
                roles: Role::Resolver.into(),
                resolver: Some(addr(member)),
                cluster: Some(cluster),
                state: ServerState::Registered,
                reported_read_gate: None,
                reported_id_map_version: None,
                reported_perms_version: None,
            })
            .collect(),
            resolver_clusters: vec![ResolverClusterEntry {
                id: cluster,
                base: "/".into(),
                state: ResolverClusterState::Active,
                members: vec![
                    addr("10.0.0.1:4564"),
                    addr("10.0.0.2:4564"),
                    addr("10.0.0.3:4564"),
                ],
                parent: None,
                children: vec![],
                perms_version: None,
            }],
        };
        let before = map.clone();
        assert!(
            delegate(&mut map, "/ap", ResolverClusterId::new(), &[ca], &[second],)
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
                &[ca, second],
                &[second, third],
            )
            .unwrap_err()
            .to_string()
            .contains("disjoint")
        );
        assert_eq!(map, before);
    }
}

/// Record the perms version the CA now holds for `cluster`, so a reader of the
/// map can tell which members are behind without a privileged call. Returns
/// whether it changed.
pub fn set_perms_version(
    map: &mut AdminDomainMap,
    cluster: ResolverClusterId,
    version: u64,
) -> bool {
    match map.resolver_clusters.iter_mut().find(|c| c.id == cluster) {
        Some(c) if c.perms_version != Some(version) => {
            c.perms_version = Some(version);
            map.version += 1;
            true
        }
        _ => false,
    }
}

/// Record the id-map version the CA now holds. Returns whether it changed.
pub fn set_id_map_version(map: &mut AdminDomainMap, version: u64) -> bool {
    if map.id_map_version == Some(version) {
        return false;
    }
    map.id_map_version = Some(version);
    map.version += 1;
    true
}
