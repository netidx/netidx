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
//! Reconciles are **additive**: they add what the admin domain has that the
//! local config lacks, and never remove what the config has that the
//! admin domain doesn't (so operator-added peers survive). An empty plan is
//! the in-sync / idempotent result.

use crate::{
    admin_proto::{AdminDomainMap, InfoAuth, ResolverAddr},
    client::ClientConfig,
    config_lock::ConfigDirLock,
    resolver::ResolverConfig,
    transport::AdminDomainInfo,
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use netidx::{config::file as cfile, resolver_server::config::file as rfile};
use std::{
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
    resolver_edit: Option<ResolverPeerEdit>,
    client_edit: Option<ClientPeerEdit>,
    /// One change per edit — the `--dry-run`/`status` body.
    pub changes: Vec<Change>,
    pub warnings: Vec<ArcStr>,
}

#[derive(Debug)]
struct ResolverPeerEdit {
    path: PathBuf,
    expected: rfile::Referral,
    replacement: Vec<(SocketAddr, rfile::RefAuth)>,
}

#[derive(Debug)]
struct ClientPeerEdit {
    path: PathBuf,
    expected: Vec<(SocketAddr, cfile::Auth)>,
    replacement: Vec<(SocketAddr, cfile::Auth)>,
}

impl EditPlan {
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn changes_resolver_config(&self) -> bool {
        self.resolver_edit.is_some()
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
    pub fn apply(self, config_lock: &ConfigDirLock) -> Result<()> {
        let Self { resolver_edit, client_edit, changes: _, warnings: _ } = self;
        let mut resolver = match resolver_edit {
            Some(ResolverPeerEdit { path, expected, replacement }) => {
                let path = config_lock.require_contained(path)?;
                let mut current = ResolverConfig::load(&path)?;
                let parent = current.as_file_mut().parent.as_mut().with_context(|| {
                    format!(
                        "resolver config {} lost its parent while the update was pending; \
                         no changes were written. Re-run the update",
                        path.display()
                    )
                })?;
                if !referral_eq(parent, &expected) {
                    bail!(
                        "resolver config {} changed its parent referral while the update \
                         was pending; no changes were written. Re-run the update",
                        path.display()
                    );
                }
                parent.addrs = replacement;
                Some((path, current))
            }
            None => None,
        };
        let mut client = match client_edit {
            Some(ClientPeerEdit { path, expected, replacement }) => {
                let path = config_lock.require_contained(path)?;
                let mut current = ClientConfig::load(&path)?;
                if !client_addrs_eq(&current.as_file().addrs, &expected) {
                    bail!(
                        "client config {} changed its resolver list while the update was \
                         pending; no changes were written. Re-run the update",
                        path.display()
                    );
                }
                current.as_file_mut().addrs = replacement;
                Some((path, current))
            }
            None => None,
        };
        if let Some((path, cfg)) = resolver.as_ref() {
            cfg.validate_for_path(path).with_context(|| {
                format!("validating resolver config {}", path.display())
            })?;
        }
        if let Some((path, cfg)) = client.as_ref() {
            cfg.validate().with_context(|| {
                format!("validating client config {}", path.display())
            })?;
        }
        if let Some((path, cfg)) = resolver.take() {
            cfg.save(&path)
                .with_context(|| format!("saving resolver config {}", path.display()))?;
        }
        if let Some((path, cfg)) = client.take() {
            cfg.save(&path)
                .with_context(|| format!("saving client config {}", path.display()))?;
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

fn client_auth_eq(a: &cfile::Auth, b: &cfile::Auth) -> bool {
    match a {
        cfile::Auth::Anonymous => match b {
            cfile::Auth::Anonymous => true,
            cfile::Auth::Krb5(_) | cfile::Auth::Local(_) | cfile::Auth::Tls(_) => false,
        },
        cfile::Auth::Krb5(a) => match b {
            cfile::Auth::Krb5(b) => a == b,
            cfile::Auth::Anonymous | cfile::Auth::Local(_) | cfile::Auth::Tls(_) => false,
        },
        cfile::Auth::Local(a) => match b {
            cfile::Auth::Local(b) => a == b,
            cfile::Auth::Anonymous | cfile::Auth::Krb5(_) | cfile::Auth::Tls(_) => false,
        },
        cfile::Auth::Tls(a) => match b {
            cfile::Auth::Tls(b) => a == b,
            cfile::Auth::Anonymous | cfile::Auth::Krb5(_) | cfile::Auth::Local(_) => {
                false
            }
        },
    }
}

fn ref_auth_eq(a: &rfile::RefAuth, b: &rfile::RefAuth) -> bool {
    match a {
        rfile::RefAuth::Anonymous => match b {
            rfile::RefAuth::Anonymous => true,
            rfile::RefAuth::Krb5(_)
            | rfile::RefAuth::Local(_)
            | rfile::RefAuth::Tls(_) => false,
        },
        rfile::RefAuth::Krb5(a) => match b {
            rfile::RefAuth::Krb5(b) => a == b,
            rfile::RefAuth::Anonymous
            | rfile::RefAuth::Local(_)
            | rfile::RefAuth::Tls(_) => false,
        },
        rfile::RefAuth::Local(a) => match b {
            rfile::RefAuth::Local(b) => a == b,
            rfile::RefAuth::Anonymous
            | rfile::RefAuth::Krb5(_)
            | rfile::RefAuth::Tls(_) => false,
        },
        rfile::RefAuth::Tls(a) => match b {
            rfile::RefAuth::Tls(b) => a == b,
            rfile::RefAuth::Anonymous
            | rfile::RefAuth::Krb5(_)
            | rfile::RefAuth::Local(_) => false,
        },
    }
}

fn client_addrs_eq(
    a: &[(SocketAddr, cfile::Auth)],
    b: &[(SocketAddr, cfile::Auth)],
) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|((aa, aauth), (ba, bauth))| aa == ba && client_auth_eq(aauth, bauth))
}

fn ref_addrs_eq(
    a: &[(SocketAddr, rfile::RefAuth)],
    b: &[(SocketAddr, rfile::RefAuth)],
) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|((aa, aauth), (ba, bauth))| aa == ba && ref_auth_eq(aauth, bauth))
}

fn referral_eq(a: &rfile::Referral, b: &rfile::Referral) -> bool {
    a.path == b.path && a.ttl == b.ttl && ref_addrs_eq(&a.addrs, &b.addrs)
}

/// Map an admin domain-reported data-plane auth (from an admin server's
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

/// Reconcile a resolver's **parent referral** against the admin domain's
/// current resolver set: add every resolver `net` reports that the
/// config's parent doesn't already list (matched by `SocketAddr`),
/// mapping each one's auth.
///
/// This is the workstation/child-resolver `update`: when the admin domain
/// grows from one resolver to several, the local parent referral learns
/// the new peers (so client referrals can fail over) without a
/// reinstall or a hand-edit.
///
/// Additive: peers the config already lists — including operator-added
/// ones absent from the admin domain — are left untouched. It *will* re-add a
/// admin domain peer the operator deleted; suppressing that is a later design.
/// Idempotent: a config already carrying every admin domain peer yields an
/// empty plan.
pub fn reconcile_resolver_peers(path: &Path, net: &AdminDomainInfo) -> Result<EditPlan> {
    let cfg = ResolverConfig::load(path)
        .with_context(|| format!("loading resolver config {}", path.display()))?;
    let expected = cfg.as_file().parent.clone().context(
        "this resolver has no parent referral to reconcile — it isn't \
         attached to an admin domain (run `join` to attach one)",
    )?;
    let mut replacement = expected.addrs.clone();
    let mut changes = Vec::new();
    for r in &net.resolvers {
        if replacement.iter().any(|(a, _)| *a == r.addr) {
            continue;
        }
        replacement.push((r.addr, info_auth_to_ref(&r.auth)));
        changes.push(Change::Add(format!(
            "resolver peer {} ({})",
            r.addr,
            describe_info_auth(&r.auth),
        )));
    }
    if changes.is_empty() {
        return Ok(EditPlan::default());
    }
    Ok(EditPlan {
        resolver_edit: Some(ResolverPeerEdit {
            path: path.to_path_buf(),
            expected,
            replacement,
        }),
        client_edit: None,
        changes,
        warnings: Vec::new(),
    })
}

// ---- admin domain-map-driven reconcile (Phase B) ----
//
// These reconcile a host's config to exactly ONE level of the hierarchy —
// the resolver cluster its current addrs already belong to — from the CA-authoritative
// admin domain map. Unlike `reconcile_resolver_peers` above (additive-only, over
// the flat legacy `AdminDomainInfo`), these both ADD missing resolver cluster members and
// AUTO-REMOVE entries the resolver cluster no longer lists, with no consent: the CA is
// the source of truth, so an addr absent from its authoritative resolver cluster is
// authoritatively gone. Host-local entries are never removed.

/// Map an admin domain-reported data-plane auth to a client-config auth.
fn info_auth_to_client(a: &InfoAuth) -> netidx::config::file::Auth {
    use netidx::config::file::Auth;
    match a {
        InfoAuth::Anonymous => Auth::Anonymous,
        InfoAuth::Krb5 { spn } => Auth::Krb5(ArcStr::from(spn.as_str())),
        InfoAuth::Tls { name } => Auth::Tls(ArcStr::from(name.as_str())),
    }
}

/// One distinct resolver cluster in the admin domain map: where it attaches and
/// its full member roster.
pub struct ResolverClusterView {
    pub base: String,
    pub members: Vec<ResolverAddr>,
}

/// The distinct resolver clusters in `map`, grouped by base path. A
/// resolver cluster's members each report the same roster; union them by address.
pub fn clusters(map: &AdminDomainMap) -> Vec<ResolverClusterView> {
    map.resolver_clusters
        .iter()
        .filter(|c| c.state == netidx_admin_proto::ResolverClusterState::Active)
        .map(|c| ResolverClusterView { base: c.base.clone(), members: c.members.clone() })
        .collect()
}

/// The resolver cluster whose members overlap `addrs` — the config's "one level".
/// Lenient: a config whose addrs straddle resolver clusters (e.g. one polluted by
/// the old flat reconcile) matches the maximally-overlapping resolver cluster, with
/// a warning. `None` when nothing overlaps (the resolver cluster may be transiently
/// unreachable — the caller no-ops rather than wiping the config).
pub fn match_cluster<'a>(
    clusters: &'a [ResolverClusterView],
    addrs: &[SocketAddr],
) -> (Option<&'a ResolverClusterView>, Option<ArcStr>) {
    let mut best: Option<(&ResolverClusterView, usize)> = None;
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
                        "config addrs span {overlapping} resolver clusters; reconciling to the \
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

/// Reconcile a peer list to the matched resolver cluster's roster: add every member
/// the config lacks, remove every entry the resolver cluster no longer lists —
/// except host-local entries, which are never admin domain peers. Returns the
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
/// (the resolver cluster its current addrs belong to) from the admin domain map. Add +
/// auto-remove; a config matching no resolver cluster is left untouched (warned).
pub fn reconcile_client_peers(path: &Path, map: &AdminDomainMap) -> Result<EditPlan> {
    let cfg = ClientConfig::load(path)
        .with_context(|| format!("loading client config {}", path.display()))?;
    let expected = cfg.as_file().addrs.clone();
    let cur: Vec<SocketAddr> = expected.iter().map(|(a, _)| *a).collect();
    let cls = clusters(map);
    let (cluster, warn) = match_cluster(&cls, &cur);
    let mut warnings: Vec<ArcStr> = warn.into_iter().collect();
    let Some(cluster) = cluster else {
        warnings.push(ArcStr::from(
            "this client's resolvers match no resolver cluster in the admin domain map \
             (unreachable?) — leaving the config unchanged",
        ));
        return Ok(EditPlan { warnings, ..EditPlan::default() });
    };
    let members = cluster.members.clone();
    let mut replacement = expected.clone();
    let changes = reconcile_peer_list(
        &mut replacement,
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
        client_edit: Some(ClientPeerEdit {
            path: path.to_path_buf(),
            expected,
            replacement,
        }),
        changes,
        warnings,
        ..EditPlan::default()
    })
}

/// Reconcile a resolver's **parent referral** to the parent resolver cluster (the
/// resolver cluster the referral already points at) from the admin domain map. Add +
/// auto-remove. Errors if the resolver has no parent referral.
pub fn reconcile_parent_peers(path: &Path, map: &AdminDomainMap) -> Result<EditPlan> {
    let cfg = ResolverConfig::load(path)
        .with_context(|| format!("loading resolver config {}", path.display()))?;
    let expected = cfg.as_file().parent.clone().context(
        "this resolver has no parent referral to reconcile — it isn't \
         attached to an admin domain",
    )?;
    let cur: Vec<SocketAddr> = expected.addrs.iter().map(|(a, _)| *a).collect();
    let cls = clusters(map);
    let (cluster, warn) = match_cluster(&cls, &cur);
    let mut warnings: Vec<ArcStr> = warn.into_iter().collect();
    let Some(cluster) = cluster else {
        warnings.push(ArcStr::from(
            "this resolver's parent referral matches no resolver cluster in the admin domain \
             map (unreachable?) — leaving it unchanged",
        ));
        return Ok(EditPlan { warnings, ..EditPlan::default() });
    };
    let members = cluster.members.clone();
    let mut replacement = expected.addrs.clone();
    let changes = reconcile_peer_list(
        &mut replacement,
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
        resolver_edit: Some(ResolverPeerEdit {
            path: path.to_path_buf(),
            expected,
            replacement,
        }),
        changes,
        warnings,
        ..EditPlan::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use netidx_admin_proto::{
        AdminServerId, ResolverAddr, ResolverClusterEntry, ResolverClusterId,
        ResolverClusterState,
    };
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

    fn net(resolvers: Vec<ResolverAddr>) -> AdminDomainInfo {
        AdminDomainInfo {
            domain: "local".into(),
            ca_addr: None,
            resolvers,
            resolver_base: Some("/".into()),
            resolver_parent: None,
            resolver_children: vec![],
            reached: vec![],
        }
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
        // Config knows about A; the admin domain has A and B.
        let path = write_resolver(&dir.path(), r#"["10.0.0.1:4564", "Anonymous"]"#);
        let admin_domain = net(vec![
            ResolverAddr { addr: addr("10.0.0.1:4564"), auth: InfoAuth::Anonymous },
            ResolverAddr { addr: addr("10.0.0.2:4564"), auth: InfoAuth::Anonymous },
        ]);
        let plan = reconcile_resolver_peers(&path, &admin_domain).unwrap();
        assert_eq!(plan.changes.len(), 1, "exactly one new peer (B)");
        assert!(plan.changes[0].text().contains("10.0.0.2:4564"));
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        plan.apply(&lock).unwrap();
        // Both peers now present on disk.
        let cfg = ResolverConfig::load(&path).unwrap();
        let parent = cfg.as_file().parent.as_ref().unwrap();
        assert_eq!(parent.addrs.len(), 2);
        // Second run is a no-op — additive reconcile converged.
        let plan2 = reconcile_resolver_peers(&path, &admin_domain).unwrap();
        assert!(plan2.is_empty(), "re-run must be empty: {:?}", plan2.changes);
    }

    #[test]
    fn preserves_operator_added_peer() {
        let dir = tempfile::tempdir().unwrap();
        // Config has the admin domain peer A plus an operator-added X the
        // admin domain doesn't report.
        let path = write_resolver(
            &dir.path(),
            r#"["10.0.0.1:4564", "Anonymous"], ["10.9.9.9:4564", "Anonymous"]"#,
        );
        let admin_domain = net(vec![ResolverAddr {
            addr: addr("10.0.0.1:4564"),
            auth: InfoAuth::Anonymous,
        }]);
        let plan = reconcile_resolver_peers(&path, &admin_domain).unwrap();
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
        let admin_domain = net(vec![ResolverAddr {
            addr: addr("10.0.0.1:4564"),
            auth: InfoAuth::Anonymous,
        }]);
        assert!(reconcile_resolver_peers(&path, &admin_domain).is_err());
    }

    // ---- map-driven reconcile (Phase B) ----

    fn srv(_addr: &str, base: &str, members: &[&str]) -> ResolverClusterEntry {
        ResolverClusterEntry {
            id: ResolverClusterId::new(),
            base: base.to_string(),
            state: ResolverClusterState::Active,
            members: members
                .iter()
                .map(|m| ResolverAddr {
                    addr: m.parse().unwrap(),
                    auth: InfoAuth::Anonymous,
                })
                .collect(),
            parent: None,
            children: vec![],
        }
    }

    fn map_of(resolver_clusters: Vec<ResolverClusterEntry>) -> AdminDomainMap {
        AdminDomainMap {
            version: 1,
            controller: AdminServerId::new(),
            admin_servers: vec![],
            resolver_clusters,
        }
    }

    fn write_client(dir: &Path, addrs: &[&str]) -> PathBuf {
        use netidx::config::file::{Auth, ConfigBuilder};
        let cfg = ClientConfig(
            ConfigBuilder::default()
                .addrs(
                    addrs.iter().map(|a| (addr(a), Auth::Anonymous)).collect::<Vec<_>>(),
                )
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
            srv("10.0.0.15:4565", "/eu", &["10.0.0.15:4564", "10.0.0.16:4564"]),
        ]);
        let cls = clusters(&m);
        assert_eq!(cls.len(), 2, "two distinct resolver clusters by base");
        let (c, w) = match_cluster(&cls, &[addr("10.0.0.15:4564")]);
        assert_eq!(c.unwrap().base, "/eu");
        assert!(w.is_none());
        let (c, _) = match_cluster(&cls, &[addr("10.9.9.9:4564")]);
        assert!(c.is_none(), "no overlap → None");
        // Straddling both resolver clusters → max-overlap (root wins 2:1) + a warning.
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
        // Client points at one /eu member plus a stale addr the resolver cluster
        // no longer lists.
        let path = write_client(&dir.path(), &["10.0.0.15:4564", "10.0.0.99:4564"]);
        let m = map_of(vec![srv(
            "10.0.0.15:4565",
            "/eu",
            &["10.0.0.15:4564", "10.0.0.16:4564"],
        )]);
        let plan = reconcile_client_peers(&path, &m).unwrap();
        assert_eq!(plan.changes.len(), 2, "add .16, remove .99");
        // .16 is the addition, .99 is the removal — and the verbs must match.
        assert!(
            plan.changes
                .iter()
                .any(|c| matches!(c, Change::Add(_))
                    && c.text().contains("10.0.0.16:4564"))
        );
        assert!(
            plan.changes
                .iter()
                .any(|c| matches!(c, Change::Del(_))
                    && c.text().contains("10.0.0.99:4564"))
        );
        // The preview must label each edit by its real direction — a stale
        // peer being removed must read "- remove", never "+ add".
        let body = plan.describe();
        assert!(body.contains("+ add") && body.contains("10.0.0.16:4564"), "{body}");
        assert!(body.contains("- remove") && body.contains("10.0.0.99:4564"), "{body}");
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        plan.apply(&lock).unwrap();
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
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        plan.apply(&lock).unwrap();
        assert_eq!(
            ClientConfig::load(&path).unwrap().as_file().addrs.len(),
            1,
            "untouched"
        );
    }

    #[test]
    fn parent_reconcile_adds_and_removes_against_parent_cluster() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_resolver(
            &dir.path(),
            r#"["10.0.0.11:4564", "Anonymous"], ["10.0.0.99:4564", "Anonymous"]"#,
        );
        let m = map_of(vec![srv(
            "10.0.0.11:4565",
            "/",
            &["10.0.0.11:4564", "10.0.0.12:4564"],
        )]);
        let plan = reconcile_parent_peers(&path, &m).unwrap();
        assert_eq!(plan.changes.len(), 2, "add .12, remove .99");
        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        plan.apply(&lock).unwrap();
        let cfg = ResolverConfig::load(&path).unwrap();
        let addrs: Vec<_> = cfg
            .as_file()
            .parent
            .as_ref()
            .unwrap()
            .addrs
            .iter()
            .map(|(a, _)| *a)
            .collect();
        assert!(addrs.contains(&addr("10.0.0.12:4564")));
        assert!(!addrs.contains(&addr("10.0.0.99:4564")));
        assert!(reconcile_parent_peers(&path, &m).unwrap().is_empty(), "idempotent");
    }

    #[test]
    fn resolver_reconcile_preserves_unrelated_concurrent_edits() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_resolver(
            dir.path(),
            r#"["10.0.0.11:4564", "Anonymous"], ["10.0.0.99:4564", "Anonymous"]"#,
        );
        let map = map_of(vec![srv(
            "10.0.0.11:4565",
            "/",
            &["10.0.0.11:4564", "10.0.0.12:4564"],
        )]);
        let plan = reconcile_parent_peers(&path, &map).unwrap();

        let mut concurrent = ResolverConfig::load(&path).unwrap();
        concurrent.as_file_mut().member_servers[0].reader_ttl = 777;
        concurrent.save(&path).unwrap();

        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        plan.apply(&lock).unwrap();
        let current = ResolverConfig::load(&path).unwrap();
        assert_eq!(current.as_file().member_servers[0].reader_ttl, 777);
        let parent = current.as_file().parent.as_ref().unwrap();
        assert!(parent.addrs.iter().any(|(a, _)| *a == addr("10.0.0.12:4564")));
        assert!(!parent.addrs.iter().any(|(a, _)| *a == addr("10.0.0.99:4564")));
    }

    #[test]
    fn client_reconcile_preserves_unrelated_concurrent_edits() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_client(dir.path(), &["10.0.0.15:4564", "10.0.0.99:4564"]);
        let map = map_of(vec![srv(
            "10.0.0.15:4565",
            "/eu",
            &["10.0.0.15:4564", "10.0.0.16:4564"],
        )]);
        let plan = reconcile_client_peers(&path, &map).unwrap();

        let mut concurrent = ClientConfig::load(&path).unwrap();
        concurrent.as_file_mut().base = "/operator".to_string();
        concurrent.save(&path).unwrap();

        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        plan.apply(&lock).unwrap();
        let current = ClientConfig::load(&path).unwrap();
        assert_eq!(current.as_file().base.as_str(), "/operator");
        assert!(
            current.as_file().addrs.iter().any(|(a, _)| *a == addr("10.0.0.16:4564"))
        );
    }

    #[test]
    fn reconcile_rejects_a_concurrent_relevant_edit() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_resolver(dir.path(), r#"["10.0.0.11:4564", "Anonymous"]"#);
        let map = map_of(vec![srv(
            "10.0.0.11:4565",
            "/",
            &["10.0.0.11:4564", "10.0.0.12:4564"],
        )]);
        let plan = reconcile_parent_peers(&path, &map).unwrap();

        let mut concurrent = ResolverConfig::load(&path).unwrap();
        concurrent
            .as_file_mut()
            .parent
            .as_mut()
            .unwrap()
            .addrs
            .push((addr("10.0.0.13:4564"), rfile::RefAuth::Anonymous));
        concurrent.save(&path).unwrap();

        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        let err = plan.apply(&lock).unwrap_err();
        assert!(format!("{err:#}").contains("changed its parent referral"));
        let current = ResolverConfig::load(&path).unwrap();
        let parent = current.as_file().parent.as_ref().unwrap();
        assert!(parent.addrs.iter().any(|(a, _)| *a == addr("10.0.0.13:4564")));
        assert!(!parent.addrs.iter().any(|(a, _)| *a == addr("10.0.0.12:4564")));
    }

    #[test]
    fn merged_reconcile_validates_every_target_before_writing() {
        let dir = tempfile::tempdir().unwrap();
        let resolver_path =
            write_resolver(dir.path(), r#"["10.0.0.11:4564", "Anonymous"]"#);
        let client_path = write_client(dir.path(), &["10.0.0.11:4564"]);
        let map = map_of(vec![srv(
            "10.0.0.11:4565",
            "/",
            &["10.0.0.11:4564", "10.0.0.12:4564"],
        )]);
        let plan = reconcile_parent_peers(&resolver_path, &map)
            .unwrap()
            .merge(reconcile_client_peers(&client_path, &map).unwrap());

        let mut concurrent = ClientConfig::load(&client_path).unwrap();
        concurrent
            .as_file_mut()
            .addrs
            .push((addr("10.0.0.13:4564"), cfile::Auth::Anonymous));
        concurrent.save(&client_path).unwrap();
        let resolver_before = std::fs::read(&resolver_path).unwrap();

        let lock = ConfigDirLock::acquire(dir.path()).unwrap();
        assert!(plan.apply(&lock).is_err());
        assert_eq!(std::fs::read(&resolver_path).unwrap(), resolver_before);
    }
}
