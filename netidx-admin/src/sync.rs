//! Keeping a client current with its admin domain.
//!
//! Resolver hosts don't need this: they run an admin server, and the CA
//! pushes topology to it. A client — a publisher or a workstation — has no
//! admin server, so nothing can push to it, and it must ask. That is the
//! whole difference, and the reason this exists.
//!
//! One pass fetches the CA-authoritative map, refreshes the admin-server
//! list in the install record, and reconciles this host's resolver
//! addresses against it. [`crate::agent`] runs it on a timer; `netidx admin
//! <role> update` runs exactly the same code to force it now.
//!
//! Auth-independent by construction. The map is fetched over a TOFU
//! connection pinned to the CA fingerprint recorded at install, and `GetMap`
//! is a public request, so a host with no client certificate and no trust
//! bundle — an anonymous or Kerberos client — syncs like any other. Pinning
//! fails closed: an admin server that answers with a different CA is
//! refused, never used.

use crate::{
    admin_proto::{
        AdminDomainMap, AdminServerEntry, NodeKind, ResolverClusterId, ServerState,
    },
    config_lock::ConfigDirLock,
    discovery, paths,
    provenance::{AdminDomainIdentity, InstallRecord, InstallRole},
    reconcile::{self, EditPlan},
    resolver::ResolverConfig,
    transport::{self, CaIdentity},
};
use anyhow::{Context, Result, bail};
use log::warn;
use rand::{RngExt, rng, seq::SliceRandom};
use std::path::Path;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, time::Duration};

/// The smallest sync interval we will honour. The actual wait is drawn from
/// `interval..=2 * interval` each cycle, so a fleet installed together does
/// not stay in step. Floored so a mistyped flag can't turn a fleet into a
/// load generator.
pub const MIN_SYNC_INTERVAL: Duration = Duration::from_secs(10);

/// The default minimum, giving a 30–60 minute spread.
///
/// This is the bound on how long an administrative change takes to reach a
/// client, and so on how long a newly added resolver has to be held back from
/// serving reads before every publisher can be expected to have found it. A
/// map fetch is small and infrequent enough that halving the day it used to
/// be costs nothing worth counting.
pub const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(1800);

/// How long to wait before the next pass: somewhere in `interval..=2 *
/// interval`, redrawn every cycle so hosts spread out and stay spread.
pub fn next_interval(interval: Duration) -> Duration {
    let secs = interval.max(MIN_SYNC_INTERVAL).as_secs();
    Duration::from_secs(rng().random_range(secs..=secs.saturating_mul(2)))
}

/// The first admin server that proves it belongs to `net_id`'s admin
/// domain, from [`discovery::admin_servers`]. Fails closed: a reachable
/// server whose CA fingerprint doesn't match the pin is refused, not used.
async fn pinned_admin_server(
    net_id: &AdminDomainIdentity,
    kind: NodeKind,
) -> Result<(SocketAddr, CaIdentity)> {
    let mut saw_mismatch = false;
    for addr in discovery::admin_servers().await {
        let id = match transport::fetch_identity(addr, kind).await {
            Ok(id) => id,
            // Unreachable / not an admin server — try the next candidate.
            Err(_) => continue,
        };
        // Fail closed on a malformed stored fingerprint (corrupt record).
        if net_id.matches(&id.fingerprint)? {
            return Ok((addr, id));
        }
        saw_mismatch = true;
    }
    if saw_mismatch {
        bail!(
            "reached an admin server, but its CA fingerprint did not match this \
             install's pinned admin domain identity (admin domain {:?}). Refusing to \
             trust it — if your admin domain's CA legitimately changed, re-join.",
            net_id.domain,
        )
    }
    bail!(
        "could not reach any admin server for admin domain {:?} (the recorded \
         addresses and mDNS both failed). Is the resolver / admin-server host up?",
        net_id.domain,
    )
}

/// The CA-authoritative map, via [`pinned_admin_server`].
pub async fn fetch_map(
    net_id: &AdminDomainIdentity,
    kind: NodeKind,
) -> Result<AdminDomainMap> {
    let (addr, id) = pinned_admin_server(net_id, kind).await?;
    transport::get_map_pinned(addr, kind, &id)
        .await
        .context("fetching the admin domain map")
}

/// Hops from `from` to every cluster reachable in the resolver hierarchy.
///
/// A real walk over the parent/children edges rather than arithmetic on
/// base paths: the hierarchy need not have a cluster at every path level,
/// so `/` to `/eu/west` may be one hop even though the paths differ by two.
/// `visited` doubles as the cycle guard — the CA rejects cycles, but a map
/// we merely received is not a map we get to assume is well formed.
fn hops_from(
    map: &AdminDomainMap,
    from: ResolverClusterId,
) -> HashMap<ResolverClusterId, usize> {
    let by_id: HashMap<_, _> = map.resolver_clusters.iter().map(|c| (c.id, c)).collect();
    let mut hops = HashMap::new();
    hops.insert(from, 0usize);
    let mut frontier = vec![from];
    let mut depth = 0usize;
    while !frontier.is_empty() {
        depth += 1;
        let mut next = Vec::new();
        for id in frontier.drain(..) {
            let Some(cluster) = by_id.get(&id) else { continue };
            for peer in cluster.parent.iter().chain(cluster.children.iter()) {
                if !hops.contains_key(peer) {
                    hops.insert(*peer, depth);
                    next.push(*peer);
                }
            }
        }
        frontier = next;
    }
    hops
}

/// The admin servers this host should try, best first.
///
/// Near in the resolver hierarchy beats far, and among equals the CA goes
/// last — it is the one server every enrollment in the admin domain has to
/// talk to, so a routine map refresh should not be one more thing it
/// answers. `anchor` is the client's own cluster; without one (nothing in
/// its config overlaps a live cluster) every server is equally far and only
/// the CA rule applies.
///
/// Shuffled *before* a **stable** sort, so the randomness survives only
/// inside each equal (distance, is-CA) bucket. Two clients in one cluster
/// therefore pick different servers first while both still prefer a near
/// one; shuffling afterwards would throw away the locality that is the
/// point of sorting at all.
pub fn order_admin_servers(
    map: &AdminDomainMap,
    anchor: Option<ResolverClusterId>,
) -> Vec<SocketAddr> {
    let hops = anchor.map(|id| hops_from(map, id));
    // `Registered` is live and routable; `Enrolled` merely holds a grant.
    let mut live: Vec<&AdminServerEntry> =
        map.admin_servers.iter().filter(|s| s.state == ServerState::Registered).collect();
    live.shuffle(&mut rng());
    live.sort_by_key(|s| {
        // Unreachable or clusterless sorts with the far end rather than the
        // near one; a dedicated CA has no cluster at all.
        let distance = match (&hops, s.cluster) {
            (Some(hops), Some(cluster)) => {
                hops.get(&cluster).copied().unwrap_or(usize::MAX)
            }
            (Some(_), None) => usize::MAX,
            (None, _) => 0,
        };
        (distance, s.id == map.ca)
    });
    let mut out: Vec<SocketAddr> = Vec::new();
    for s in live {
        if !out.contains(&s.addr) {
            out.push(s.addr);
        }
    }
    out
}

/// Which resolver cluster this host belongs to, or `None` when nothing in
/// its config overlaps a live one.
///
/// A publisher's client config points at real cluster members. A
/// workstation's points at its own loopback resolver, so what places it is
/// the parent referral of that resolver. `InstallRecord::base` is no help:
/// it is `/local` for a workstation and a CLI-defaulted `/` for a
/// publisher.
fn anchor_cluster(role: InstallRole, map: &AdminDomainMap) -> Option<ResolverClusterId> {
    let clusters = reconcile::clusters(map);
    let addrs: Vec<SocketAddr> = match role {
        InstallRole::Ca => return None,
        InstallRole::Publisher => {
            let path = paths::discover_client_config().ok()?;
            let cfg = crate::client::ClientConfig::load(&path).ok()?;
            cfg.as_file().addrs.iter().map(|(a, _)| *a).collect()
        }
        InstallRole::Workstation | InstallRole::Resolver => {
            let path = paths::discover_resolver_config().ok()?;
            let cfg = ResolverConfig::load(&path).ok()?;
            let parent = cfg.as_file().parent.clone()?;
            parent.addrs.iter().map(|(a, _)| *a).collect()
        }
    };
    let (matched, _) = reconcile::match_cluster(&clusters, &addrs);
    matched.map(|c| c.id)
}

/// The config edits this role's sync applies.
fn edits_for(role: InstallRole, map: &AdminDomainMap) -> Result<EditPlan> {
    match role {
        InstallRole::Ca => Ok(EditPlan::default()),
        InstallRole::Publisher => {
            let path = paths::discover_client_config()
                .context("no client config found at the standard locations")?;
            reconcile::reconcile_client_peers(&path, map)
        }
        InstallRole::Workstation => {
            let path = paths::discover_resolver_config()
                .context("no resolver config found at the standard locations")?;
            reconcile::reconcile_parent_peers(&path, map)
        }
        InstallRole::Resolver => {
            let rpath = paths::discover_resolver_config()
                .context("no resolver config found at the standard locations")?;
            let mut plan = match paths::discover_client_config() {
                Ok(cpath) => reconcile::reconcile_client_peers(&cpath, map)?,
                Err(_) => EditPlan::default(),
            };
            // The parent referral if this resolver is a child. NEVER
            // member_servers — those are local launch choices, and on a host
            // with an admin server the CA maintains them by push.
            if ResolverConfig::load(&rpath)?.as_file().parent.is_some() {
                plan = plan.merge(reconcile::reconcile_parent_peers(&rpath, map)?);
            }
            Ok(plan)
        }
    }
}

/// What one pass found, before anything is written. Holding this without a
/// lock is the point: the network work is done, so [`SyncPlan::apply`] can
/// take the config-dir lock for the length of a few file writes.
pub struct SyncPlan {
    pub map: AdminDomainMap,
    /// The record with its refreshed admin-server list, saved by `apply`.
    record: InstallRecord,
    record_path: PathBuf,
    /// Whether that list actually changed — an unchanged record is not
    /// rewritten.
    pub admin_servers_changed: bool,
    pub edits: EditPlan,
}

impl SyncPlan {
    /// Nothing to write: the recorded servers already match and no config
    /// edit is pending.
    pub fn is_empty(&self) -> bool {
        !self.admin_servers_changed && self.edits.is_empty()
    }

    pub fn admin_servers(&self) -> &[SocketAddr] {
        &self.record.admin_servers
    }

    /// The record this plan reconciles — its role, base, and admin domain, for
    /// a frontend to name what it is about to change.
    pub fn record(&self) -> &InstallRecord {
        &self.record
    }

    /// Write it. The caller owns the lock so it can decide how to react to
    /// contention — an operator's command should fail loudly, a daemon
    /// should shrug and try again next cycle.
    pub fn apply(self, config_lock: &ConfigDirLock) -> Result<()> {
        if self.admin_servers_changed {
            self.record
                .save(config_lock, &self.record_path)
                .context("recording the admin server list")?;
        }
        self.edits.apply(config_lock)
    }

    /// Apply under this plan's own record-directory lock, taken now that the
    /// network work is done — holding it across a round trip would fail a
    /// concurrent command for the length of a network call.
    ///
    /// The directory comes from the record the plan was built from, so no
    /// caller can lock a different one. Contention is an error here; the
    /// timer-driven [`pass`] tolerates it instead.
    pub async fn apply_locked(self) -> Result<()> {
        let lock = ConfigDirLock::acquire_for_file_async(&self.record_path).await?;
        self.apply(&lock)
    }

    /// What the operator must do for this plan to take effect. Nothing
    /// re-reads its configuration at runtime, so every edit lands at the next
    /// process start — which is why nothing here ever restarts a service.
    pub fn restart_hint(&self) -> &'static str {
        match self.record.role {
            InstallRole::Ca => "The CA requires no resolver restart.",
            InstallRole::Workstation => {
                "No service was restarted. Restart the local resolver to serve the \
                 new peers."
            }
            InstallRole::Resolver if self.edits.changes_resolver_config() => {
                "No service was restarted. Restart this resolver manually at its \
                 place in the resolver cluster's rolling sequence; re-run client \
                 processes if their resolver addresses changed."
            }
            InstallRole::Resolver => {
                "Re-run client processes to use the new resolver addresses; no \
                 resolver service restart is needed."
            }
            InstallRole::Publisher => "Re-run publishers to use the new resolvers.",
        }
    }
}

/// How this host presents itself to an admin server, by what it runs.
fn node_kind(role: InstallRole) -> NodeKind {
    match role {
        InstallRole::Publisher => NodeKind::Publisher,
        InstallRole::Resolver => NodeKind::Resolver,
        InstallRole::Ca | InstallRole::Workstation => NodeKind::Client,
    }
}

/// Load the install record to reconcile, with the path it came from.
///
/// `config_root` names the install — the TUI enumerates both scopes and must
/// write back to the one it read; `None` discovers it, which is what a one-shot
/// command wants. The role is asserted rather than inferred: a plan writes to
/// the record it was built from, so being handed the wrong one is an error.
pub fn record_for(
    role: InstallRole,
    config_root: Option<&Path>,
) -> Result<(InstallRecord, PathBuf)> {
    let path = match config_root {
        Some(root) => root.join("install.json"),
        None => crate::paths::discover_install_record().context(
            "no install record (install.json) found — this host has no netidx \
             install managed by `netidx admin`, or the install predates the record",
        )?,
    };
    let record = InstallRecord::load(&path)?;
    if record.role != role {
        bail!(
            "this host is a {} install, not a {} — use `netidx admin {} …`",
            record.role.as_str(),
            role.as_str(),
            record.role.as_str(),
        );
    }
    if role == InstallRole::Ca {
        bail!("the CA has no resolver configuration to reconcile");
    }
    Ok((record, path))
}

/// The admin domain map as this host sees it, pinned to the CA identity
/// recorded at install — for map-driven UI such as the parent picker.
pub async fn fetch_map_for(
    role: InstallRole,
    config_root: Option<&Path>,
) -> Result<AdminDomainMap> {
    let (record, _) = record_for(role, config_root)?;
    let net_id = record
        .admin_domain
        .as_ref()
        .context("this host is not part of an admin domain (local-only)")?;
    fetch_map(net_id, node_kind(role)).await
}

/// [`record_for`] then [`plan`] — what both frontends' "update" does.
pub async fn plan_for(role: InstallRole, config_root: Option<&Path>) -> Result<SyncPlan> {
    let (record, path) = record_for(role, config_root)?;
    plan(record, path).await
}

/// Fetch the map and work out everything this host would change. Does no
/// writing and takes no lock.
///
/// `record_path` is passed rather than rediscovered: a caller that loaded a
/// specific scope's record (the TUI enumerates both) must write back to that
/// same one.
pub async fn plan(mut record: InstallRecord, record_path: PathBuf) -> Result<SyncPlan> {
    let net_id = record
        .admin_domain
        .clone()
        .context("this host is local-only — it has not joined an admin domain")?;
    let kind = node_kind(record.role);
    let map = fetch_map(&net_id, kind).await?;
    let anchor = anchor_cluster(record.role, &map);
    let admin_servers_changed =
        record.set_admin_servers(order_admin_servers(&map, anchor));
    let edits = edits_for(record.role, &map)?;
    Ok(SyncPlan { map, record, record_path, admin_servers_changed, edits })
}

/// One full pass: plan, then apply under a briefly-held lock.
///
/// The lock is taken only once the network work is done, and only when
/// there is something to write. Contention means an operator is running a
/// command right now; that is normal, so it is reported as `false` rather
/// than an error and the next cycle will pick the work up.
pub async fn pass(record: InstallRecord, record_path: PathBuf) -> Result<bool> {
    let plan = plan(record, record_path.clone()).await?;
    if plan.is_empty() {
        return Ok(false);
    }
    let lock = match ConfigDirLock::acquire_for_file_async(&record_path).await {
        Ok(lock) => lock,
        Err(e) => {
            log::debug!("sync: config directory busy, retrying next cycle: {e:#}");
            return Ok(false);
        }
    };
    for w in &plan.edits.warnings {
        warn!("sync: {w}");
    }
    plan.apply(&lock)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin_proto::{
        AdminServerId, ResolverClusterEntry, ResolverClusterState, Role,
    };
    use uuid::Uuid;

    /// A resolver whose own configuration is untouched needs no restart — only
    /// its clients do. The hint is the thing an operator acts on, so it must
    /// not send them to roll a cluster that has not changed.
    #[test]
    fn a_client_only_change_asks_for_no_resolver_restart() {
        let plan = SyncPlan {
            map: AdminDomainMap::empty(AdminServerId::new()),
            record: InstallRecord::new(InstallRole::Resolver, "/", "tls", None, None),
            record_path: PathBuf::from("/nonexistent/install.json"),
            admin_servers_changed: false,
            edits: EditPlan::default(),
        };
        let hint = plan.restart_hint();
        assert!(hint.contains("no resolver service restart is needed"));
        assert!(!hint.contains("Restart this resolver"));
    }

    fn cluster(
        n: u128,
        base: &str,
        parent: Option<u128>,
        children: &[u128],
    ) -> ResolverClusterEntry {
        ResolverClusterEntry {
            id: ResolverClusterId(Uuid::from_u128(n)),
            base: base.into(),
            state: ResolverClusterState::Active,
            members: vec![],
            parent: parent.map(|p| ResolverClusterId(Uuid::from_u128(p))),
            children: children
                .iter()
                .map(|c| ResolverClusterId(Uuid::from_u128(*c)))
                .collect(),
        }
    }

    fn server(n: u128, cluster: Option<u128>, state: ServerState) -> AdminServerEntry {
        AdminServerEntry {
            id: AdminServerId(Uuid::from_u128(n)),
            addr: SocketAddr::from(([10, 0, 0, n as u8], 4565)),
            roles: Role::Resolver.into(),
            resolver: None,
            cluster: cluster.map(|c| ResolverClusterId(Uuid::from_u128(c))),
            state,
            reported_read_gate: None,
        }
    }

    /// `/` — `/eu` — `/eu/west`, plus a sibling `/ap` under the root. The CA
    /// is server 1, in the root cluster.
    fn map() -> AdminDomainMap {
        AdminDomainMap {
            version: 1,
            ca: AdminServerId(Uuid::from_u128(1)),
            admin_servers: vec![
                server(1, Some(10), ServerState::Registered),
                server(2, Some(10), ServerState::Registered),
                server(3, Some(11), ServerState::Registered),
                server(4, Some(12), ServerState::Registered),
                server(5, Some(13), ServerState::Registered),
            ],
            resolver_clusters: vec![
                cluster(10, "/", None, &[11, 13]),
                cluster(11, "/eu", Some(10), &[12]),
                cluster(12, "/eu/west", Some(11), &[]),
                cluster(13, "/ap", Some(10), &[]),
            ],
        }
    }

    fn addr(n: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, n], 4565))
    }

    #[test]
    fn distance_is_hops_not_path_depth() {
        let map = map();
        let hops = hops_from(&map, ResolverClusterId(Uuid::from_u128(12)));
        let at = |n: u128| hops[&ResolverClusterId(Uuid::from_u128(n))];
        assert_eq!(at(12), 0);
        assert_eq!(at(11), 1);
        assert_eq!(at(10), 2);
        assert_eq!(at(13), 3); // /ap is via the root, not a path neighbour
    }

    /// A malformed map must not hang the daemon that received it.
    #[test]
    fn a_cycle_terminates() {
        let mut map = map();
        map.resolver_clusters =
            vec![cluster(10, "/", Some(11), &[11]), cluster(11, "/eu", Some(10), &[10])];
        let hops = hops_from(&map, ResolverClusterId(Uuid::from_u128(10)));
        assert_eq!(hops.len(), 2);
    }

    #[test]
    fn nearest_first_and_the_ca_last_among_equals() {
        let map = map();
        // A client in /eu/west: its own cluster's server, then /eu, then the
        // root — where the CA loses to its non-CA peer — then /ap.
        let order =
            order_admin_servers(&map, Some(ResolverClusterId(Uuid::from_u128(12))));
        assert_eq!(order, vec![addr(4), addr(3), addr(2), addr(1), addr(5)]);
    }

    /// Without an anchor there is no notion of near, so the only rule left
    /// is that the CA is not the first thing a client bothers.
    #[test]
    fn an_unanchored_client_still_leaves_the_ca_last() {
        let map = map();
        let order = order_admin_servers(&map, None);
        assert_eq!(order.len(), 5);
        assert_eq!(order[4], addr(1));
    }

    /// The ordering has to survive being *stored*. It didn't: the record
    /// used to pin whichever address this host enrolled through to the
    /// front, which on a small admin domain is the CA — exactly the server
    /// the ordering exists to leave until last. Testing `order_admin_servers`
    /// alone never saw it.
    #[test]
    fn the_order_survives_being_recorded() {
        let map = map();
        let order =
            order_admin_servers(&map, Some(ResolverClusterId(Uuid::from_u128(10))));
        // Both are in the root cluster; the CA (server 1) loses the tie.
        assert_eq!(order[0], addr(2));
        assert_eq!(order[1], addr(1));

        let mut rec =
            InstallRecord::new(InstallRole::Publisher, "/", "tls", None, Some(addr(1)));
        assert_eq!(rec.admin_servers, vec![addr(1)], "enrolled through the CA");
        rec.set_admin_servers(order.clone());
        assert_eq!(rec.admin_servers, order, "the record must not reorder");
        assert_ne!(rec.admin_servers[0], addr(1), "the CA must not be tried first");
    }

    #[test]
    fn a_server_that_is_not_live_is_not_a_candidate() {
        let mut map = map();
        map.admin_servers[3].state = ServerState::Enrolled;
        let order =
            order_admin_servers(&map, Some(ResolverClusterId(Uuid::from_u128(12))));
        assert!(!order.contains(&addr(4)), "{order:?}");
        assert_eq!(order.len(), 4);
    }

    /// The shuffle must survive the sort *within* a bucket and never leak
    /// across one. Catches both a `sort_unstable` and a shuffle that drifts
    /// after the sort.
    #[test]
    fn randomness_stays_inside_its_bucket() {
        let mut map = map();
        // Four peers in the client's own cluster, none of them the CA.
        map.admin_servers = vec![
            server(1, Some(10), ServerState::Registered),
            server(2, Some(12), ServerState::Registered),
            server(3, Some(12), ServerState::Registered),
            server(4, Some(12), ServerState::Registered),
            server(5, Some(12), ServerState::Registered),
        ];
        let anchor = Some(ResolverClusterId(Uuid::from_u128(12)));
        let mut firsts = std::collections::HashSet::new();
        for _ in 0..200 {
            let order = order_admin_servers(&map, anchor);
            // The root's server is two hops away and must never be first.
            assert_ne!(order[0], addr(1), "a distant server outran a local one");
            assert_eq!(order[4], addr(1));
            firsts.insert(order[0]);
        }
        assert!(firsts.len() > 1, "the order never varied: the shuffle is not working");
    }

    #[test]
    fn the_interval_is_drawn_from_one_to_two_times_the_minimum() {
        for base in [MIN_SYNC_INTERVAL, DEFAULT_SYNC_INTERVAL] {
            for _ in 0..100 {
                let d = next_interval(base);
                assert!(d >= base && d <= base * 2, "{d:?} outside {base:?}..2x");
            }
        }
        // Below the floor is raised to it, not honoured.
        let d = next_interval(Duration::from_secs(1));
        assert!(d >= MIN_SYNC_INTERVAL, "{d:?}");
    }
}
