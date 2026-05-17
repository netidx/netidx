//! Resolver-server config: load, edit, save, validate.
//!
//! Wraps [`netidx::resolver_server::config::file::Config`] in a newtype
//! that adds atomic save and a `validate` shortcut that round-trips
//! through `Config::from_file`.

use crate::{atomic, paths};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use netidx::resolver_server::config::{Config, file};
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

    pub fn load_default() -> Result<Self> {
        Self::load(paths::discover_resolver_config()?)
    }

    /// Validate, then atomically save to `path` at mode 0o644.
    ///
    /// Validation resolves relative `include_permissions` entries
    /// against the target file's parent directory — mirroring runtime
    /// startup (`Config::load_file`). That way `netidx conf resolver
    /// show/edit` run from an unrelated cwd doesn't misvalidate the
    /// same config that the server happily loads.
    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        self.validate_for_path(path)
            .context("resolver config failed validation")?;
        atomic::write_atomic_pretty_json(path, &self.0)
    }

    pub fn save_default(&self) -> Result<()> {
        self.save(paths::user_resolver_config()?)
    }

    /// Validate as if the config were stored at `path`: relative
    /// `include_permissions` entries are resolved against `path`'s
    /// parent dir before being handed to `Config::from_file`.
    ///
    /// Use this instead of [`Self::validate`] anywhere the caller knows
    /// the on-disk location — `save`, `edit`, etc.
    pub fn validate_for_path(&self, path: &Path) -> Result<()> {
        let resolved = resolve_includes_against(&self.0, path)?;
        Config::from_file(resolved).map(|_| ())
    }

    /// Re-runs the existing `Config::from_file` validator on a clone.
    /// Relative `include_permissions` entries are resolved against the
    /// process cwd — only correct when the caller has no path context.
    /// Prefer [`Self::validate_for_path`] when the on-disk location is
    /// known.
    pub fn validate(&self) -> Result<()> {
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
}

/// Clone `cfg` with relative `include_permissions` entries rewritten
/// to be absolute, joined against the canonicalized parent dir of
/// `target_path`. Mirrors the rewrite in
/// `netidx::resolver_server::config::Config::load_file` so validation
/// before save matches validation at runtime startup.
///
/// The original `cfg` is left untouched — relative paths on disk are
/// preserved.
///
/// Skips the canonicalize entirely when there is nothing to resolve
/// (no relative entries) — otherwise saving to a path under a
/// not-yet-created parent dir (e.g. fresh install before
/// `~/.config/netidx/` exists) would error out before the atomic
/// write would have created the dir.
fn resolve_includes_against(cfg: &file::Config, target_path: &Path) -> Result<file::Config> {
    let mut resolved = cfg.clone();
    let has_relative = resolved
        .include_permissions
        .iter()
        .any(|e| Path::new(e.as_str()).is_relative());
    if !has_relative {
        return Ok(resolved);
    }
    let parent = match target_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => Some(p.to_path_buf()),
        _ => None,
    };
    let parent_canon = match parent {
        Some(p) => Some(
            p.canonicalize()
                .with_context(|| format!("canonicalizing parent dir {p:?}"))?,
        ),
        None => None,
    };
    if let Some(parent) = parent_canon.as_deref() {
        for entry in resolved.include_permissions.iter_mut() {
            let p = Path::new(entry.as_str());
            if p.is_relative() {
                let abs = parent.join(p);
                *entry = ArcStr::from(abs.to_string_lossy().as_ref());
            }
        }
    }
    Ok(resolved)
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
    fn validate_rejects_bad_config() {
        // No member servers → invalid.
        let cfg = ResolverConfig(
            file::ConfigBuilder::default()
                .member_servers(vec![])
                .build()
                .unwrap(),
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
}
