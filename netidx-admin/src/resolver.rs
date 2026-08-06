//! Resolver-server config: load, edit, save, validate.
//!
//! Wraps [`netidx::resolver_server::config::file::Config`] in a newtype
//! that adds atomic save and a `validate` shortcut that round-trips
//! through `Config::from_file`.

use crate::{
    admin_proto::{InfoAuth, ResolverAddr, ResolverClusterEdge, ResolverClusterFacts},
    atomic, paths,
};
use anyhow::{Context, Result, bail};
use netidx::path::Path as NetidxPath;
use netidx::resolver_server::config::{self, Config, ReadGate, file};
use std::path::{Path, PathBuf};

/// A loaded resolver-server config. Wraps
/// `netidx::resolver_server::config::file::Config`.
#[derive(Debug, Clone)]
pub struct ResolverConfig(pub file::Config);

impl ResolverConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading resolver config {path:?}"))?;
        let cfg: file::Config = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing resolver config {path:?}"))?;
        Ok(Self(cfg))
    }

    /// Wrap a config the CA rendered. Nothing is validated here — the caller
    /// validates against the path it is about to write, which is the only
    /// place relative `include_permissions` entries resolve correctly.
    pub fn from_file(cfg: file::Config) -> Self {
        Self(cfg)
    }

    pub fn load_default() -> Result<Self> {
        Self::load(paths::discover_resolver_config()?)
    }

    pub async fn load_async<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let bytes = tokio::fs::read(path)
            .await
            .with_context(|| format!("reading resolver config {path:?}"))?;
        let cfg: file::Config = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing resolver config {path:?}"))?;
        Ok(Self(cfg))
    }

    /// Validate, then atomically save to `path` at mode 0o644.
    ///
    /// Validation resolves relative `include_permissions` entries
    /// against the target file's parent directory — mirroring runtime
    /// startup (`Config::load_file`). That way `netidx admin resolver
    /// show/edit` run from an unrelated cwd doesn't misvalidate the
    /// same config that the server happily loads.
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        self.validate_for_path(path).context("resolver config failed validation")?;
        atomic::write_atomic_pretty_json(path, &self.0)
    }

    pub fn save_default(&self) -> Result<()> {
        self.save(paths::user_resolver_config()?)
    }

    pub async fn save_async<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref().to_path_buf();
        let this = self.clone();
        tokio::task::spawn_blocking({
            let path = path.clone();
            move || this.validate_for_path(&path)
        })
        .await
        .context("resolver config validation task panicked")?
        .context("resolver config failed validation")?;
        atomic::write_atomic_pretty_json_async(&path, &self.0).await
    }

    /// Validate as if the config were stored at `path`: relative
    /// `include_permissions` entries are resolved against `path`'s
    /// parent dir before being handed to `Config::from_file`.
    ///
    /// Use this instead of [`Self::validate`] anywhere the caller knows
    /// the on-disk location — `save`, `edit`, etc.
    pub fn validate_for_path(&self, path: &Path) -> Result<()> {
        // Resolve relative include_permissions against `path`'s parent
        // using netidx's own canonicalizer, so pre-save validation
        // matches runtime startup exactly (see
        // `config::resolve_relative_includes`). Clone so the on-disk
        // form keeps its relative paths.
        let mut resolved = self.0.clone();
        config::resolve_relative_includes(&mut resolved, path)?;
        let permissions = config::merge_perms_only(&resolved)?;
        validate_permission_topology(&resolved, &permissions)?;
        Config::from_file(resolved).map(|_| ())
    }

    /// Preflight the hierarchy-dependent permission checks against a rendered
    /// permissions file that has not been written yet. Other include files are
    /// read normally; the matching prospective path is supplied from memory.
    pub fn preflight_permission_topology(
        &self,
        path: &Path,
        prospective: Option<(&Path, &config::PMap)>,
    ) -> Result<()> {
        let mut resolved = self.0.clone();
        config::resolve_relative_includes(&mut resolved, path)?;
        let config_parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let prospective = prospective.map(|(perms_path, perms)| {
            let path = if perms_path.is_absolute() {
                perms_path.to_path_buf()
            } else {
                config_parent.join(perms_path)
            };
            (path, perms)
        });
        let mut permissions = config::PMap::default();
        for include in &resolved.include_permissions {
            let include = Path::new(include.as_str());
            let pmap = match prospective.as_ref() {
                Some((path, perms)) if include == path => (*perms).clone(),
                _ => config::load_perms(include)?,
            };
            config::merge_pmap(&mut permissions, pmap);
        }
        config::merge_pmap(&mut permissions, resolved.perms.clone());
        validate_permission_topology(&resolved, &permissions)
    }

    /// Re-runs the existing `Config::from_file` validator on a clone.
    /// Relative `include_permissions` entries are resolved against the
    /// process cwd — only correct when the caller has no path context.
    /// Prefer [`Self::validate_for_path`] when the on-disk location is
    /// known.
    pub fn validate(&self) -> Result<()> {
        let permissions = config::merge_perms_only(&self.0)?;
        validate_permission_topology(&self.0, &permissions)?;
        Config::from_file(self.0.clone()).map(|_| ())
    }

    pub fn builder() -> file::ConfigBuilder {
        file::ConfigBuilder::default()
    }

    pub fn as_file(&self) -> &file::Config {
        &self.0
    }

    pub fn as_file_mut(&mut self) -> &mut file::Config {
        &mut self.0
    }

    pub fn into_file(self) -> file::Config {
        self.0
    }

    /// This resolver cluster's advertised member addresses + data-plane
    /// auth, as [`ResolverAddr`]s — the form delegation exchanges and
    /// `GetInfo` reports. `Local`-auth members are host-local by
    /// definition and omitted (nothing to advertise to the admin domain).
    pub fn resolver_addrs(&self) -> Vec<ResolverAddr> {
        self.0
            .member_servers
            .iter()
            .filter_map(|m| {
                let auth = match &m.auth {
                    file::Auth::Anonymous => InfoAuth::Anonymous,
                    file::Auth::Local(_) => return None,
                    file::Auth::Krb5(spn) => InfoAuth::Krb5 { spn: spn.to_string() },
                    file::Auth::Tls { name, .. } => {
                        InfoAuth::Tls { name: name.to_string() }
                    }
                };
                Some(ResolverAddr { addr: m.addr, auth })
            })
            .collect()
    }

    /// Where this resolver cluster attaches in the namespace — the parent
    /// referral's path, or `/` for a root resolver cluster with no parent.
    pub fn base_path(&self) -> String {
        self.0
            .parent
            .as_ref()
            .map(|r| r.path.to_string())
            .unwrap_or_else(|| "/".to_string())
    }

    /// The parent resolver cluster this resolver attaches under, as a map edge, if any.
    pub fn parent_edge(&self) -> Option<ResolverClusterEdge> {
        self.0.parent.as_ref().map(referral_to_edge)
    }

    /// The child resolver clusters delegated below this resolver, as map edges.
    pub fn children_edges(&self) -> Vec<ResolverClusterEdge> {
        self.0.children.iter().map(referral_to_edge).collect()
    }

    /// Whether this host is refusing read clients, across all its member
    /// blocks. Reported to the CA so `netidx admin ca servers` and the TUI can
    /// show what each host is *doing* rather than what it was last told.
    pub fn read_gate(&self) -> ReadGate {
        self.0
            .member_servers
            .iter()
            .fold(ReadGate::No, |acc, m| acc.strictest(m.read_gated))
    }

    /// This resolver cluster's [`ResolverClusterFacts`] for the admin domain map:
    /// advertisable members + base path + resolver hierarchy edges.
    pub fn cluster_facts(&self) -> ResolverClusterFacts {
        ResolverClusterFacts {
            members: self.resolver_addrs(),
            base: self.base_path(),
            parent: self.parent_edge(),
            children: self.children_edges(),
            read_gated: self.read_gate(),
        }
    }
}

/// Check the part of resolver permission validation that depends on the
/// resolver's place in the resolver hierarchy. `Config::from_file` validates the
/// resolver topology and loads the permission files, but the resolver does
/// not normally apply these containment checks until it constructs its auth
/// map during startup. Admin topology fanout must perform them before writing
/// a config, otherwise a successful delegation can leave a resolver unable to
/// restart.
fn validate_permission_topology(
    cfg: &file::Config,
    permissions: &config::PMap,
) -> Result<()> {
    let root = cfg.parent.as_ref().map(|r| r.path.as_ref()).unwrap_or("/");
    for entry in permissions.0.keys() {
        let entry = NetidxPath::from(entry);
        if !NetidxPath::is_parent(root, &entry) {
            bail!("permission entry for parent: {root}, entry: {entry}")
        }
        for child in &cfg.children {
            let child = NetidxPath::from(&child.path);
            if NetidxPath::is_parent(&child, &entry) {
                bail!("permission entry for child: {child}, entry: {entry}")
            }
        }
    }
    Ok(())
}

/// Map a referral (parent / child) to an admin domain-map [`ResolverClusterEdge`],
/// dropping `Local`-auth addrs (host-local, nothing to advertise).
fn referral_to_edge(r: &file::Referral) -> ResolverClusterEdge {
    ResolverClusterEdge {
        path: r.path.to_string(),
        addrs: r
            .addrs
            .iter()
            .filter_map(|(addr, auth)| {
                Some(ResolverAddr { addr: *addr, auth: refauth_to_info(auth)? })
            })
            .collect(),
    }
}

/// Inverse of `reconcile::info_auth_to_ref`, dropping `Local` (mirrors
/// [`ResolverConfig::resolver_addrs`]).
fn refauth_to_info(a: &file::RefAuth) -> Option<InfoAuth> {
    match a {
        file::RefAuth::Anonymous => Some(InfoAuth::Anonymous),
        file::RefAuth::Local(_) => None,
        file::RefAuth::Krb5(spn) => Some(InfoAuth::Krb5 { spn: spn.to_string() }),
        file::RefAuth::Tls(name) => Some(InfoAuth::Tls { name: name.to_string() }),
    }
}

impl From<file::Config> for ResolverConfig {
    fn from(c: file::Config) -> Self {
        Self(c)
    }
}

pub fn default_save_path() -> Result<PathBuf> {
    paths::user_resolver_config()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arcstr::ArcStr;

    fn minimal() -> ResolverConfig {
        ResolverConfig(
            file::ConfigBuilder::default()
                .member_servers(vec![
                    file::MemberServerBuilder::default()
                        .addr("127.0.0.1:0".parse().unwrap())
                        .bind_addr("127.0.0.1".parse().unwrap())
                        .auth(file::Auth::Anonymous)
                        .build()
                        .unwrap(),
                ])
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("resolver.json");
        let cfg = minimal();
        cfg.save(&p).unwrap();
        let back = ResolverConfig::load(&p).unwrap();
        assert_eq!(back.0.member_servers.len(), 1);
        let first = std::fs::read(&p).unwrap();
        back.save(&p).unwrap();
        let second = std::fs::read(&p).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn cluster_facts_reports_members_base_and_edges() {
        // A child resolver cluster: two members (one Local, dropped), a parent
        // referral at /eu, and one child edge.
        let json = r#"{
            "children": [
                {"path":"/eu/sub","ttl":null,"addrs":[["10.0.0.9:4564","Anonymous"]]}
            ],
            "parent": {"path":"/eu","ttl":null,"addrs":[["10.0.0.1:4564","Anonymous"]]},
            "member_servers": [
                {"addr":"10.0.0.2:4564","bind_addr":"10.0.0.2","auth":"Anonymous","hello_timeout":10,"max_connections":768,"pid_file":"","reader_ttl":60,"writer_ttl":120,"id_map_command":null,"id_map_type":"Command","id_map_timeout":3600},
                {"addr":"10.0.0.3:4564","bind_addr":"10.0.0.3","auth":{"Local":"/tmp/sock"},"hello_timeout":10,"max_connections":768,"pid_file":"","reader_ttl":60,"writer_ttl":120,"id_map_command":null,"id_map_type":"Command","id_map_timeout":3600}
            ],
            "perms": {},
            "include_permissions": []
        }"#;
        let cfg = ResolverConfig(serde_json::from_str(json).unwrap());
        let facts = cfg.cluster_facts();
        assert_eq!(facts.members.len(), 1, "Local member dropped");
        assert_eq!(facts.members[0].addr, "10.0.0.2:4564".parse().unwrap());
        assert_eq!(facts.base, "/eu");
        let parent = facts.parent.expect("parent edge");
        assert_eq!(parent.path, "/eu");
        assert_eq!(parent.addrs.len(), 1);
        assert_eq!(facts.children.len(), 1);
        assert_eq!(facts.children[0].path, "/eu/sub");
    }

    #[test]
    fn validate_rejects_bad_config() {
        // No member servers → invalid.
        let cfg = ResolverConfig(
            file::ConfigBuilder::default().member_servers(vec![]).build().unwrap(),
        );
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn save_succeeds_when_parent_dir_does_not_yet_exist() {
        // Regression: `validate_for_path` used to unconditionally
        // canonicalize the target's parent dir; on a fresh install
        // where `~/.config/netidx/` doesn't exist yet, save() would
        // fail before the atomic write would have created the dir.
        // With no relative entries to resolve there is nothing to
        // canonicalize against.
        let dir = tempfile::tempdir().unwrap();
        let missing_parent = dir.path().join("not-yet-created/sub");
        let target = missing_parent.join("resolver.json");
        let cfg = minimal();
        cfg.save(&target).expect(
            "save into a not-yet-created parent dir must succeed when there are no \
             relative include_permissions",
        );
        assert!(target.exists());
    }

    #[test]
    fn save_resolves_relative_includes_against_target_dir() {
        // Regression: previously `save` ran `Config::from_file` against
        // the in-memory value with no path context, so a relative
        // `include_permissions` entry would be opened relative to the
        // process cwd and the save would fail when run from anywhere
        // but the config directory.
        let dir = tempfile::tempdir().unwrap();
        let perms_file = dir.path().join("perms.d").join("main.json");
        std::fs::create_dir_all(perms_file.parent().unwrap()).unwrap();
        std::fs::write(&perms_file, "{}").unwrap();

        let mut cfg = minimal();
        cfg.0.include_permissions = vec![ArcStr::from("perms.d/main.json")];

        // Move cwd somewhere that doesn't contain perms.d to prove the
        // relative path is resolved against the target dir, not cwd.
        let scratch_cwd = tempfile::tempdir().unwrap();
        let saved_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(scratch_cwd.path()).unwrap();
        let result = cfg.save(dir.path().join("resolver.json"));
        std::env::set_current_dir(saved_cwd).unwrap();
        result.expect("save must succeed even when cwd differs from config dir");
    }

    #[test]
    fn validation_rejects_permissions_outside_delegated_root() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("resolver.json");
        let permissions_path = dir.path().join("perms.json");
        std::fs::write(
            &permissions_path,
            r#"{"/":{"users":"swl"},"/users/$[user]":{"$[user]":"swlpd"}}"#,
        )
        .unwrap();

        let mut cfg = minimal();
        cfg.0.parent = Some(file::Referral {
            path: ArcStr::from("/eu"),
            ttl: None,
            addrs: vec![("10.0.0.1:4564".parse().unwrap(), file::RefAuth::Anonymous)],
        });
        cfg.0.include_permissions = vec![ArcStr::from("perms.json")];

        let err = cfg.validate_for_path(&config_path).unwrap_err();
        assert!(
            format!("{err:#}").contains("permission entry for parent: /eu, entry: /"),
            "unexpected error: {err:#}"
        );

        std::fs::write(
            &permissions_path,
            r#"{"/eu":{"users":"swl"},"/eu/users/$[user]":{"$[user]":"swlpd"}}"#,
        )
        .unwrap();
        cfg.validate_for_path(&config_path)
            .expect("permissions rooted at the delegated base must validate");
    }

    #[test]
    fn validation_rejects_permissions_below_a_child_referral() {
        let mut cfg = minimal();
        cfg.0.children = vec![file::Referral {
            path: ArcStr::from("/eu"),
            ttl: None,
            addrs: vec![("10.0.0.1:4564".parse().unwrap(), file::RefAuth::Anonymous)],
        }];
        crate::perms::add_entry(&mut cfg.0.perms, "/eu/private", "users", "swl").unwrap();

        let err = cfg.validate().unwrap_err();
        assert!(
            format!("{err:#}")
                .contains("permission entry for child: /eu, entry: /eu/private"),
            "unexpected error: {err:#}"
        );
    }
}
