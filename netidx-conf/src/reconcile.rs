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
    conf_proto::InfoAuth,
    resolver::ResolverConfig,
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use netidx::resolver_server::config::file as rfile;
use std::path::{Path, PathBuf};

/// A planned set of edits to existing config files. An empty plan
/// ([`is_empty`](Self::is_empty)) is the "already in sync" result.
#[derive(Debug, Default)]
pub struct EditPlan {
    pub resolver_edit: Option<(PathBuf, ResolverConfig)>,
    pub client_edit: Option<(PathBuf, ClientConfig)>,
    /// One human-readable line per change — the `--dry-run`/`status` body.
    pub changes: Vec<String>,
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
                let _ = writeln!(out, "  + add {c}");
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
            changes.push(format!(
                "resolver peer {} ({})",
                r.addr,
                describe_info_auth(&r.auth),
            ));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf_proto::ResolverAddr;
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
        assert!(plan.changes[0].contains("10.0.0.2:4564"));
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
}
