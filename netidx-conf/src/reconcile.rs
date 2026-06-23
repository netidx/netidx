//! Idempotent, additive edits to an **existing** config, for the
//! installed-template lifecycle ops (`update`).
//!
//! The template system renders configs from scratch; this is its
//! counterpart for evolving an install in place. An [`EditPlan`] mirrors
//! the template `describe()`/`apply()` shape — a previewable, atomically
//! applied set of edits — but starts from a loaded config and touches
//! only the fields it changes, leaving every other field (and any
//! operator customization) intact.
//!
//! Reconciles are **additive**: they add what the network has that the
//! local config lacks, and never remove what the config has that the
//! network doesn't (so operator-added peers survive). An empty plan is
//! the in-sync / idempotent result.

use crate::{
    client::ClientConfig,
    conf_client::NetworkInfo,
    conf_proto::{InfoAuth, NetworkMap, ResolverAddr},
    resolver::ResolverConfig,
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use netidx::resolver_server::config::file as rfile;
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// One planned edit, carrying its direction so the preview can't mislabel
/// a removal as an add (the verb is derived from the variant, never
/// hard-coded). The payload is the change subject without the verb.
#[derive(Debug, Clone)]
pub enum Change {
    Add(String),
    Del(String),
}

impl Change {
    /// The change subject, without the add/remove verb.
    pub fn text(&self) -> &str {
        match self {
            Change::Add(s) | Change::Del(s) => s,
        }
    }
}

impl std::fmt::Display for Change {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Change::Add(s) => write!(f, "+ add {s}"),
            Change::Del(s) => write!(f, "- remove {s}"),
        }
    }
}

/// A planned set of edits to existing config files. An empty plan
/// ([`is_empty`](Self::is_empty)) is the "already in sync" result.
#[derive(Debug, Default)]
pub struct EditPlan {
    pub resolver_edit: Option<(PathBuf, ResolverConfig)>,
    pub client_edit: Option<(PathBuf, ClientConfig)>,
    /// One change per edit — the `--dry-run`/`status` body.
    pub changes: Vec<Change>,
    pub warnings: Vec<ArcStr>,
}

impl EditPlan {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Human-readable preview of the edits.
    pub fn describe(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();
        if self.changes.is_empty() {
            out.push_str("  (already in sync — no changes)\n");
        } else {
            for c in &self.changes {
                let _ = writeln!(out, "  {c}");
            }
        }
        for w in &self.warnings {
            let _ = writeln!(out, "  warning: {w}");
        }
        out
    }

    /// Validate + atomically save each edited config. The configs'
    /// `save` re-validates (the resolver via `validate_for_path`), so an
    /// edit that would yield an invalid config fails here, before any
    /// write touches disk.
    pub fn apply(&self) -> Result<()> {
        if let Some((path, cfg)) = &self.resolver_edit {
            cfg.save(path).with_context(|| {
                format!("saving resolver config {}", path.display())
            })?;
        }
        if let Some((path, cfg)) = &self.client_edit {
            cfg.save(path).with_context(|| {
                format!("saving client config {}", path.display())
            })?;
        }
        Ok(())
    }

    /// Combine two plans that touch disjoint config files — the client
    /// plan and the parent-referral plan, for `resolver update`.
    pub fn merge(mut self, other: EditPlan) -> EditPlan {
        if other.resolver_edit.is_some() {
            self.resolver_edit = other.resolver_edit;
        }
        if other.client_edit.is_some() {
            self.client_edit = other.client_edit;
        }
        self.changes.extend(other.changes);
        self.warnings.extend(other.warnings);
        self
    }
}

/// Map a network-reported data-plane auth (from a conf server's
/// `GetInfo`) to a resolver-referral auth.
fn info_auth_to_ref(a: &InfoAuth) -> rfile::RefAuth {
    match a {
        InfoAuth::Anonymous => rfile::RefAuth::Anonymous,
        InfoAuth::Krb5 { spn } => rfile::RefAuth::Krb5(ArcStr::from(spn.as_str())),
        InfoAuth::Tls { name } => rfile::RefAuth::Tls(ArcStr::from(name.as_str())),
    }
}

fn describe_info_auth(a: &InfoAuth) -> &'static str {
    match a {
        InfoAuth::Anonymous => "anonymous",
        InfoAuth::Krb5 { .. } => "krb5",
        InfoAuth::Tls { .. } => "tls",
    }
}

/// Reconcile a resolver's **parent referral** against the network's
/// current resolver set: add every resolver `net` reports that the
/// config's parent doesn't already list (matched by `SocketAddr`),
/// mapping each one's auth.
///
/// This is the workstation/child-resolver `update`: when the network
/// grows from one resolver to several, the local parent referral learns
/// the new peers (so client referrals can fail over) without a
/// reinstall or a hand-edit.
///
/// Additive: peers the config already lists — including operator-added
/// ones absent from the network — are left untouched. It *will* re-add a
/// network peer the operator deleted; suppressing that is a later design.
/// Idempotent: a config already carrying every network peer yields an
/// empty plan.
pub fn reconcile_resolver_peers(path: &Path, net: &NetworkInfo) -> Result<EditPlan> {
    let mut cfg = ResolverConfig::load(path)
        .with_context(|| format!("loading resolver config {}", path.display()))?;
    let mut changes = Vec::new();
    {
        let file = cfg.as_file_mut();
        let parent = file.parent.as_mut().context(
            "this resolver has no parent referral to reconcile — it isn't \
             attached to a network (run `join` to attach one)",
        )?;
        for r in &net.resolvers {
            if parent.addrs.iter().any(|(a, _)| *a == r.addr) {
                continue;
            }
            parent.addrs.push((r.addr, info_auth_to_ref(&r.auth)));
            changes.push(Change::Add(format!(
                "resolver peer {} ({})",
                r.addr,
                describe_info_auth(&r.auth),
            )));
        }
    }
    if changes.is_empty() {
        return Ok(EditPlan::default());
    }
    Ok(EditPlan {
        resolver_edit: Some((path.to_path_buf(), cfg)),
        client_edit: None,
        changes,
        warnings: Vec::new(),
    })
}

// ---- network-map-driven reconcile (Phase B) ----
//
// These reconcile a host's config to exactly ONE level of the hierarchy —
// the cluster its current addrs already belong to — from the CA-authoritative
// network map. Unlike `reconcile_resolver_peers` above (additive-only, over
// the flat legacy `NetworkInfo`), these both ADD missing cluster members and
// AUTO-REMOVE entries the cluster no longer lists, with no consent: the CA is
// the source of truth, so an addr absent from its authoritative cluster is
// authoritatively gone. Host-local entries are never removed.

/// Map a network-reported data-plane auth to a client-config auth.
fn info_auth_to_client(a: &InfoAuth) -> netidx::config::file::Auth {
    use netidx::config::file::Auth;
    match a {
        InfoAuth::Anonymous => Auth::Anonymous,
        InfoAuth::Krb5 { spn } => Auth::Krb5(ArcStr::from(spn.as_str())),
        InfoAuth::Tls { name } => Auth::Tls(ArcStr::from(name.as_str())),
    }
}

/// One distinct resolver cluster in the network map: where it attaches and
/// its full member roster.
pub struct ClusterView {
    pub base: String,
    pub members: Vec<ResolverAddr>,
}

/// The distinct resolver clusters in `map`, grouped by base path. A
/// cluster's members each report the same roster; union them by address.
pub fn clusters(map: &NetworkMap) -> Vec<ClusterView> {
    let mut by_base: BTreeMap<String, Vec<ResolverAddr>> = BTreeMap::new();
    for s in &map.servers {
        if let Some(c) = &s.cluster {
            let members = by_base.entry(c.base.clone()).or_default();
            for m in &c.members {
                if !members.iter().any(|x| x.addr == m.addr) {
                    members.push(m.clone());
                }
            }
        }
    }
    by_base.into_iter().map(|(base, members)| ClusterView { base, members }).collect()
}

/// The cluster whose members overlap `addrs` — the config's "one level".
/// Lenient: a config whose addrs straddle clusters (e.g. one polluted by
/// the old flat reconcile) matches the maximally-overlapping cluster, with
/// a warning. `None` when nothing overlaps (the cluster may be transiently
/// unreachable — the caller no-ops rather than wiping the config).
pub fn match_cluster<'a>(
    clusters: &'a [ClusterView],
    addrs: &[SocketAddr],
) -> (Option<&'a ClusterView>, Option<ArcStr>) {
    let mut best: Option<(&ClusterView, usize)> = None;
    let mut overlapping = 0usize;
    for c in clusters {
        let n = c.members.iter().filter(|m| addrs.contains(&m.addr)).count();
        if n == 0 {
            continue;
        }
        overlapping += 1;
        if best.map(|(_, bn)| n > bn).unwrap_or(true) {
            best = Some((c, n));
        }
    }
    match best {
        None => (None, None),
        Some((c, _)) => {
            let warn = (overlapping > 1).then(|| {
                ArcStr::from(
                    format!(
                        "config addrs span {overlapping} clusters; reconciling to the \
                         most-overlapping one ({})",
                        c.base
                    )
                    .as_str(),
                )
            });
            (Some(c), warn)
        }
    }
}

/// Reconcile a peer list to the matched cluster's roster: add every member
/// the config lacks, remove every entry the cluster no longer lists —
/// except host-local entries, which are never network peers. Returns the
/// `+`/`-` change lines.
fn reconcile_peer_list<A: Clone>(
    current: &mut Vec<(SocketAddr, A)>,
    members: &[ResolverAddr],
    map_auth: impl Fn(&InfoAuth) -> A,
    is_local: impl Fn(&A) -> bool,
    add_line: impl Fn(SocketAddr, &InfoAuth) -> String,
    rm_line: impl Fn(SocketAddr) -> String,
) -> Vec<Change> {
    let mut changes = Vec::new();
    for m in members {
        if !current.iter().any(|(a, _)| *a == m.addr) {
            current.push((m.addr, map_auth(&m.auth)));
            changes.push(Change::Add(add_line(m.addr, &m.auth)));
        }
    }
    let member_addrs: Vec<SocketAddr> = members.iter().map(|m| m.addr).collect();
    let mut removed = Vec::new();
    current.retain(|(a, auth)| {
        if is_local(auth) || member_addrs.contains(a) {
            true
        } else {
            removed.push(*a);
            false
        }
    });
    for a in removed {
        changes.push(Change::Del(rm_line(a)));
    }
    changes
}

/// Reconcile a host's **client config** addrs to its own resolver cluster
/// (the cluster its current addrs belong to) from the network map. Add +
/// auto-remove; a config matching no cluster is left untouched (warned).
pub fn reconcile_client_peers(path: &Path, map: &NetworkMap) -> Result<EditPlan> {
    let mut cfg = ClientConfig::load(path)
        .with_context(|| format!("loading client config {}", path.display()))?;
    let cur: Vec<SocketAddr> = cfg.as_file().addrs.iter().map(|(a, _)| *a).collect();
    let cls = clusters(map);
    let (cluster, warn) = match_cluster(&cls, &cur);
    let mut warnings: Vec<ArcStr> = warn.into_iter().collect();
    let Some(cluster) = cluster else {
        warnings.push(ArcStr::from(
            "this client's resolvers match no cluster in the network map \
             (unreachable?) — leaving the config unchanged",
        ));
        return Ok(EditPlan { warnings, ..EditPlan::default() });
    };
    let members = cluster.members.clone();
    let changes = reconcile_peer_list(
        &mut cfg.as_file_mut().addrs,
        &members,
        info_auth_to_client,
        |a| matches!(a, netidx::config::file::Auth::Local(_)),
        |addr, auth| format!("client resolver {addr} ({})", describe_info_auth(auth)),
        |addr| format!("client resolver {addr}"),
    );
    if changes.is_empty() {
        return Ok(EditPlan { warnings, ..EditPlan::default() });
    }
    Ok(EditPlan {
        client_edit: Some((path.to_path_buf(), cfg)),
        changes,
        warnings,
        ..EditPlan::default()
    })
}

/// Reconcile a resolver's **parent referral** to the parent cluster (the
/// cluster the referral already points at) from the network map. Add +
/// auto-remove. Errors if the resolver has no parent referral.
pub fn reconcile_parent_peers(path: &Path, map: &NetworkMap) -> Result<EditPlan> {
    let mut cfg = ResolverConfig::load(path)
        .with_context(|| format!("loading resolver config {}", path.display()))?;
    let cur: Vec<SocketAddr> = {
        let parent = cfg.as_file().parent.as_ref().context(
            "this resolver has no parent referral to reconcile — it isn't \
             attached to a network",
        )?;
        parent.addrs.iter().map(|(a, _)| *a).collect()
    };
    let cls = clusters(map);
    let (cluster, warn) = match_cluster(&cls, &cur);
    let mut warnings: Vec<ArcStr> = warn.into_iter().collect();
    let Some(cluster) = cluster else {
        warnings.push(ArcStr::from(
            "this resolver's parent referral matches no cluster in the network \
             map (unreachable?) — leaving it unchanged",
        ));
        return Ok(EditPlan { warnings, ..EditPlan::default() });
    };
    let members = cluster.members.clone();
    let parent = cfg
        .as_file_mut()
        .parent
        .as_mut()
        .expect("parent referral present — checked above");
    let changes = reconcile_peer_list(
        &mut parent.addrs,
        &members,
        info_auth_to_ref,
        |a| matches!(a, rfile::RefAuth::Local(_)),
        |addr, auth| format!("parent resolver {addr} ({})", describe_info_auth(auth)),
        |addr| format!("parent resolver {addr}"),
    );
    if changes.is_empty() {
        return Ok(EditPlan { warnings, ..EditPlan::default() });
    }
    Ok(EditPlan {
        resolver_edit: Some((path.to_path_buf(), cfg)),
        changes,
        warnings,
        ..EditPlan::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf_proto::{ClusterFacts, ResolverAddr, Role, ServerEntry};
    use std::net::SocketAddr;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    // A minimal, valid resolver config with a one-peer anonymous parent
    // referral and an anonymous member server — enough to load, reconcile,
    // and round-trip through `Config::from_file` validation on `apply`.
    fn resolver_json(parent_addrs: &str) -> String {
        format!(
            r#"{{
              "children": [],
              "parent": {{ "path": "/local", "ttl": null, "addrs": [{parent_addrs}] }},
              "member_servers": [{{
                "addr": "127.0.0.1:4654",
                "bind_addr": "127.0.0.1",
                "auth": "Anonymous",
                "hello_timeout": 10,
                "max_connections": 768,
                "pid_file": "",
                "reader_ttl": 60,
                "writer_ttl": 120,
                "id_map_command": null,
                "id_map_type": "DoNotMap",
                "id_map_timeout": 3600
              }}],
              "perms": {{}},
              "include_permissions": []
            }}"#
        )
    }

    fn write_resolver(dir: &Path, parent_addrs: &str) -> PathBuf {
        let path = dir.join("resolver.json");
        std::fs::write(&path, resolver_json(parent_addrs)).unwrap();
        path
    }

    fn net(resolvers: Vec<ResolverAddr>) -> NetworkInfo {
        NetworkInfo { domain: "local".into(), ca_addr: None, resolvers, reached: vec![] }
    }

    #[test]
    fn auth_mapping_covers_every_variant() {
        assert!(matches!(
            info_auth_to_ref(&InfoAuth::Anonymous),
            rfile::RefAuth::Anonymous
        ));
        assert!(matches!(
            info_auth_to_ref(&InfoAuth::Krb5 { spn: "netidx/r@R".into() }),
            rfile::RefAuth::Krb5(s) if s == "netidx/r@R"
        ));
        assert!(matches!(
            info_auth_to_ref(&InfoAuth::Tls { name: "r.example".into() }),
            rfile::RefAuth::Tls(s) if s == "r.example"
        ));
    }

    #[test]
    fn adds_missing_peers_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        // Config knows about A; the network has A and B.
        let path = write_resolver(&dir.path(), r#"["10.0.0.1:4564", "Anonymous"]"#);
        let network = net(vec![
            ResolverAddr { addr: addr("10.0.0.1:4564"), auth: InfoAuth::Anonymous },
            ResolverAddr { addr: addr("10.0.0.2:4564"), auth: InfoAuth::Anonymous },
        ]);
        let plan = reconcile_resolver_peers(&path, &network).unwrap();
        assert_eq!(plan.changes.len(), 1, "exactly one new peer (B)");
        assert!(plan.changes[0].text().contains("10.0.0.2:4564"));
        plan.apply().unwrap();
        // Both peers now present on disk.
        let cfg = ResolverConfig::load(&path).unwrap();
        let parent = cfg.as_file().parent.as_ref().unwrap();
        assert_eq!(parent.addrs.len(), 2);
        // Second run is a no-op — additive reconcile converged.
        let plan2 = reconcile_resolver_peers(&path, &network).unwrap();
        assert!(plan2.is_empty(), "re-run must be empty: {:?}", plan2.changes);
    }

    #[test]
    fn preserves_operator_added_peer() {
        let dir = tempfile::tempdir().unwrap();
        // Config has the network peer A plus an operator-added X the
        // network doesn't report.
        let path = write_resolver(
            &dir.path(),
            r#"["10.0.0.1:4564", "Anonymous"], ["10.9.9.9:4564", "Anonymous"]"#,
        );
        let network =
            net(vec![ResolverAddr { addr: addr("10.0.0.1:4564"), auth: InfoAuth::Anonymous }]);
        let plan = reconcile_resolver_peers(&path, &network).unwrap();
        // Nothing to add (A present); X is NOT removed.
        assert!(plan.is_empty());
        let cfg = ResolverConfig::load(&path).unwrap();
        let parent = cfg.as_file().parent.as_ref().unwrap();
        assert!(parent.addrs.iter().any(|(a, _)| *a == addr("10.9.9.9:4564")));
    }

    #[test]
    fn no_parent_referral_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resolver.json");
        // Same shape but parent: null.
        let json = resolver_json("").replacen(
            "\"parent\": { \"path\": \"/local\", \"ttl\": null, \"addrs\": [] }",
            "\"parent\": null",
            1,
        );
        std::fs::write(&path, json).unwrap();
        let network =
            net(vec![ResolverAddr { addr: addr("10.0.0.1:4564"), auth: InfoAuth::Anonymous }]);
        assert!(reconcile_resolver_peers(&path, &network).is_err());
    }

    // ---- map-driven reconcile (Phase B) ----

    fn srv(addr: &str, base: &str, members: &[&str]) -> ServerEntry {
        ServerEntry {
            addr: addr.parse().unwrap(),
            roles: vec![Role::Resolver],
            cluster: Some(ClusterFacts {
                members: members
                    .iter()
                    .map(|m| ResolverAddr { addr: m.parse().unwrap(), auth: InfoAuth::Anonymous })
                    .collect(),
                base: base.to_string(),
                parent: None,
                children: vec![],
            }),
        }
    }

    fn map_of(servers: Vec<ServerEntry>) -> NetworkMap {
        NetworkMap { version: 1, ca_addr: None, servers }
    }

    fn write_client(dir: &Path, addrs: &[&str]) -> PathBuf {
        use netidx::config::file::{Auth, ConfigBuilder};
        let cfg = ClientConfig(
            ConfigBuilder::default()
                .addrs(addrs.iter().map(|a| (addr(a), Auth::Anonymous)).collect::<Vec<_>>())
                .build()
                .unwrap(),
        );
        let path = dir.join("client.json");
        cfg.save(&path).unwrap();
        path
    }

    #[test]
    fn match_cluster_picks_the_one_level() {
        let m = map_of(vec![
            srv("10.0.0.11:4565", "/", &["10.0.0.11:4564", "10.0.0.12:4564"]),
            srv("10.0.0.12:4565", "/", &["10.0.0.11:4564", "10.0.0.12:4564"]),
            srv("10.0.0.15:4565", "/eu", &["10.0.0.15:4564", "10.0.0.16:4564"]),
        ]);
        let cls = clusters(&m);
        assert_eq!(cls.len(), 2, "two distinct clusters by base");
        let (c, w) = match_cluster(&cls, &[addr("10.0.0.15:4564")]);
        assert_eq!(c.unwrap().base, "/eu");
        assert!(w.is_none());
        let (c, _) = match_cluster(&cls, &[addr("10.9.9.9:4564")]);
        assert!(c.is_none(), "no overlap → None");
        // Straddling both clusters → max-overlap (root wins 2:1) + a warning.
        let (c, w) = match_cluster(
            &cls,
            &[addr("10.0.0.11:4564"), addr("10.0.0.12:4564"), addr("10.0.0.15:4564")],
        );
        assert_eq!(c.unwrap().base, "/");
        assert!(w.is_some());
    }

    #[test]
    fn client_reconcile_adds_removes_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        // Client points at one /eu member plus a stale addr the cluster
        // no longer lists.
        let path = write_client(&dir.path(), &["10.0.0.15:4564", "10.0.0.99:4564"]);
        let m = map_of(vec![srv("10.0.0.15:4565", "/eu", &["10.0.0.15:4564", "10.0.0.16:4564"])]);
        let plan = reconcile_client_peers(&path, &m).unwrap();
        assert_eq!(plan.changes.len(), 2, "add .16, remove .99");
        // .16 is the addition, .99 is the removal — and the verbs must match.
        assert!(plan.changes.iter().any(
            |c| matches!(c, Change::Add(_)) && c.text().contains("10.0.0.16:4564")
        ));
        assert!(plan.changes.iter().any(
            |c| matches!(c, Change::Del(_)) && c.text().contains("10.0.0.99:4564")
        ));
        // The preview must label each edit by its real direction — a stale
        // peer being removed must read "- remove", never "+ add".
        let body = plan.describe();
        assert!(body.contains("+ add") && body.contains("10.0.0.16:4564"), "{body}");
        assert!(body.contains("- remove") && body.contains("10.0.0.99:4564"), "{body}");
        plan.apply().unwrap();
        let cfg = ClientConfig::load(&path).unwrap();
        let addrs: Vec<_> = cfg.as_file().addrs.iter().map(|(a, _)| *a).collect();
        assert!(addrs.contains(&addr("10.0.0.15:4564")));
        assert!(addrs.contains(&addr("10.0.0.16:4564")));
        assert!(!addrs.contains(&addr("10.0.0.99:4564")), "stale peer auto-removed");
        let plan2 = reconcile_client_peers(&path, &m).unwrap();
        assert!(plan2.is_empty(), "re-run must be empty: {:?}", plan2.changes);
    }

    #[test]
    fn client_reconcile_no_matching_cluster_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_client(&dir.path(), &["10.0.0.99:4564"]);
        let m = map_of(vec![srv("10.0.0.15:4565", "/eu", &["10.0.0.15:4564"])]);
        let plan = reconcile_client_peers(&path, &m).unwrap();
        assert!(plan.changes.is_empty());
        assert!(!plan.warnings.is_empty(), "warns rather than wiping");
        plan.apply().unwrap();
        assert_eq!(ClientConfig::load(&path).unwrap().as_file().addrs.len(), 1, "untouched");
    }

    #[test]
    fn parent_reconcile_adds_and_removes_against_parent_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_resolver(
            &dir.path(),
            r#"["10.0.0.11:4564", "Anonymous"], ["10.0.0.99:4564", "Anonymous"]"#,
        );
        let m = map_of(vec![srv("10.0.0.11:4565", "/", &["10.0.0.11:4564", "10.0.0.12:4564"])]);
        let plan = reconcile_parent_peers(&path, &m).unwrap();
        assert_eq!(plan.changes.len(), 2, "add .12, remove .99");
        plan.apply().unwrap();
        let cfg = ResolverConfig::load(&path).unwrap();
        let addrs: Vec<_> =
            cfg.as_file().parent.as_ref().unwrap().addrs.iter().map(|(a, _)| *a).collect();
        assert!(addrs.contains(&addr("10.0.0.12:4564")));
        assert!(!addrs.contains(&addr("10.0.0.99:4564")));
        assert!(reconcile_parent_peers(&path, &m).unwrap().is_empty(), "idempotent");
    }
}
