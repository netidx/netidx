//! Wholesale teardown of a netidx install: stop / remove the OS
//! service, then delete the contents of the canonical config root.
//!
//! The CA dir (`<config_root>/ca/`) is treated as **data**, not
//! config — it holds the root private key, and any cert signed by
//! that key cannot be re-issued from scratch if the key is destroyed.
//! Preserved by default; opt in to its removal via
//! [`UninstallParams::remove_ca`].
//!
//! The engine only knows about the canonical config root. If the
//! install put files at non-default paths (e.g. `--id-map-socket
//! /var/run/netidx.sock`), those are **not** removed — the engine
//! doesn't introspect `resolver.json` to discover them. Operators
//! with non-default layouts must clean up the extras by hand.
//!
//! Idempotent: re-running on an already-clean host is a no-op.
//! Mirror of the install side's "engine never touches what it didn't
//! install" contract — we only remove things under the conventional
//! root and the canonical service-unit path.

use crate::{
    config_lock::ConfigDirLock,
    paths,
    service::{self, ServiceParams, ServiceScope, ServiceStatus},
};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Inputs for [`preview`] and [`prepare`].
#[derive(Debug, Clone)]
pub struct UninstallParams {
    /// Scope to tear down. The config root and the OS service are
    /// both per-scope; we don't cross over (e.g. a user-scope
    /// uninstall won't touch a system-scope install).
    pub scope: ServiceScope,
    /// Service name passed to [`service::uninstall`]. Default:
    /// [`ServiceParams::DEFAULT_NAME`] (`"netidx"`).
    pub service_name: String,
    /// For system-scope only: the user the templated systemd unit
    /// was instantiated as. Mirrors [`ServiceParams::for_user`].
    pub for_user: Option<String>,
    /// Override the canonical config root. `None` ⇒
    /// [`paths::user_config_root`] for [`ServiceScope::User`] or
    /// [`paths::system_config_root`] for [`ServiceScope::System`].
    pub config_dir: Option<PathBuf>,
    /// Also delete `<config_root>/ca/`. **DANGEROUS** — loss of the
    /// CA private key is permanent. Default `false`.
    pub remove_ca: bool,
}

/// Why a path was kept rather than removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepReason {
    /// `ca/` preserved because [`UninstallParams::remove_ca`] was
    /// false (the default).
    CaPreserved,
}

impl KeepReason {
    pub fn as_str(self) -> &'static str {
        match self {
            KeepReason::CaPreserved => "CA root key (pass --with-ca to remove)",
        }
    }
}

/// What an uninstall did, or what [`preview`] reports it would do.
#[derive(Debug, Default)]
pub struct UninstallReport {
    /// Whether the OS service was found installed and we attempted
    /// teardown (or would have, under dry-run). False ⇒ nothing
    /// service-side to do.
    pub service_was_installed: bool,
    /// Paths removed (or "would be removed" under dry-run), in the
    /// order they were processed.
    pub removed: Vec<PathBuf>,
    /// Paths intentionally kept, with the reason.
    pub kept: Vec<(PathBuf, KeepReason)>,
}

impl UninstallReport {
    /// True if both the service and the config root were already
    /// gone — the uninstall had nothing to do.
    pub fn is_empty(&self) -> bool {
        !self.service_was_installed && self.removed.is_empty() && self.kept.is_empty()
    }
}

pub enum PreparedUninstall {
    Complete(UninstallReport),
    RemoveConfig(ConfigRemoval),
}

pub struct ConfigRemoval {
    params: UninstallParams,
    root: PathBuf,
    report: UninstallReport,
}

impl ConfigRemoval {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn finish(self, config_lock: &ConfigDirLock) -> Result<UninstallReport> {
        config_lock.require_contained(&self.root)?;
        cleanup(&self.params, &self.root, self.report, CleanupMode::Apply(config_lock))
    }
}

pub fn preview(p: &UninstallParams) -> Result<UninstallReport> {
    preview_with_service(p, service::status)
}

pub fn prepare(p: &UninstallParams) -> Result<PreparedUninstall> {
    prepare_with_service(p, service::status, service::uninstall)
}

fn preview_with_service(
    p: &UninstallParams,
    status: impl FnOnce(&ServiceParams) -> Result<ServiceStatus>,
) -> Result<UninstallReport> {
    let (root, report) = service_state(p, status)?;
    if !root.exists() {
        return Ok(report);
    }
    cleanup(p, &root, report, CleanupMode::Preview)
}

fn prepare_with_service(
    p: &UninstallParams,
    status: impl FnOnce(&ServiceParams) -> Result<ServiceStatus>,
    remove_service: impl FnOnce(&ServiceParams) -> Result<()>,
) -> Result<PreparedUninstall> {
    let (root, report) = service_state(p, status)?;
    if report.service_was_installed {
        remove_service(&service_params(p)).context(
            "service teardown was not verified; configuration has been preserved",
        )?;
    }
    if root.exists() {
        Ok(PreparedUninstall::RemoveConfig(ConfigRemoval {
            params: p.clone(),
            root,
            report,
        }))
    } else {
        Ok(PreparedUninstall::Complete(report))
    }
}

fn service_params(p: &UninstallParams) -> ServiceParams {
    let sparams = ServiceParams {
        scope: p.scope,
        for_user: p.for_user.clone(),
        // binary / activation_dir aren't read by uninstall — placeholders.
        binary: PathBuf::new(),
        service_name: p.service_name.clone(),
        activation_dir: None,
    };
    sparams
}

fn service_state(
    p: &UninstallParams,
    status: impl FnOnce(&ServiceParams) -> Result<ServiceStatus>,
) -> Result<(PathBuf, UninstallReport)> {
    let sparams = service_params(p);
    let pre = status(&sparams)
        .context("could not determine service state; refusing to remove configuration")?;
    let report = UninstallReport {
        service_was_installed: pre != ServiceStatus::NotInstalled,
        ..UninstallReport::default()
    };
    let root = match &p.config_dir {
        Some(d) => d.clone(),
        None => match p.scope {
            ServiceScope::User => paths::user_config_root()?,
            ServiceScope::System => paths::system_config_root(),
        },
    };
    Ok((root, report))
}

#[derive(Clone, Copy)]
enum CleanupMode<'a> {
    Preview,
    Apply(&'a ConfigDirLock),
}

fn cleanup(
    p: &UninstallParams,
    root: &Path,
    mut report: UninstallReport,
    mode: CleanupMode<'_>,
) -> Result<UninstallReport> {
    if let CleanupMode::Apply(config_lock) = mode {
        config_lock.require_contained(root)?;
    }

    // Snapshot entries first so an error mid-walk doesn't leave us in
    // a half-known state — and so the report ordering is stable.
    let entries: Vec<PathBuf> = std::fs::read_dir(&root)
        .with_context(|| format!("listing {root:?}"))?
        .map(|e| {
            e.with_context(|| format!("reading entry in {root:?}")).map(|e| e.path())
        })
        .collect::<Result<_>>()?;

    for path in entries {
        let is_ca = path.file_name().and_then(|s| s.to_str()) == Some("ca");
        if is_ca && !p.remove_ca {
            report.kept.push((path, KeepReason::CaPreserved));
            continue;
        }
        if matches!(mode, CleanupMode::Apply(_)) {
            remove_any(&path)?;
        }
        report.removed.push(path);
    }

    // 3. If the root itself has no kept entries left, remove it too
    //    so a `ls ~/.config | grep netidx` post-uninstall returns
    //    nothing.
    if report.kept.is_empty() {
        if matches!(mode, CleanupMode::Apply(_)) {
            match std::fs::remove_dir(&root) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(anyhow::Error::from(e))
                        .with_context(|| format!("removing root {root:?}"));
                }
            }
        }
        report.removed.push(root.to_path_buf());
    }

    Ok(report)
}

fn remove_any(path: &Path) -> Result<()> {
    let meta =
        std::fs::symlink_metadata(path).with_context(|| format!("stat {path:?}"))?;
    if meta.file_type().is_symlink() {
        // Always unlink symlinks rather than walking through them —
        // a symlink pointing at /home or / would otherwise be a
        // catastrophic remove_dir_all target.
        std::fs::remove_file(path).with_context(|| format!("removing symlink {path:?}"))
    } else if meta.is_dir() {
        std::fs::remove_dir_all(path).with_context(|| format!("removing {path:?}"))
    } else {
        std::fs::remove_file(path).with_context(|| format!("removing {path:?}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Build a fake config root populated with the files / dirs a
    /// realistic install lays down. Service-related state lives
    /// elsewhere (systemd unit dir, launchd plist dir) and isn't
    /// touched by these tests — we use a unique service_name so the
    /// best-effort service::uninstall call no-ops.
    fn populate_root(root: &Path) {
        fs::create_dir_all(root).unwrap();
        fs::write(root.join("resolver.json"), b"{}").unwrap();
        fs::write(root.join("perms.json"), b"{}").unwrap();
        fs::write(root.join("client.json"), b"{}").unwrap();
        fs::write(root.join("id-map.json"), b"{}").unwrap();
        fs::create_dir_all(root.join("activation")).unwrap();
        fs::write(root.join("activation").join("resolver.unit"), b"unit").unwrap();
        fs::create_dir_all(root.join("tls").join("resolver")).unwrap();
        fs::write(root.join("tls").join("resolver").join("certificate.pem"), b"cert")
            .unwrap();
        fs::create_dir_all(root.join("ca")).unwrap();
        fs::write(root.join("ca").join("certificate.pem"), b"ca cert").unwrap();
        fs::write(root.join("ca").join("private.key"), b"ca key").unwrap();
    }

    /// Service name unlikely to clash with anything the developer has
    /// actually installed via `netidx admin component service install`. The
    /// per-OS uninstall is best-effort + idempotent — with this name
    /// in a test environment it's a no-op.
    fn test_service_name() -> String {
        format!("netidx-uninstall-test-{}", std::process::id())
    }

    fn params(config_dir: PathBuf) -> UninstallParams {
        UninstallParams {
            scope: ServiceScope::User,
            service_name: test_service_name(),
            for_user: None,
            config_dir: Some(config_dir),
            remove_ca: false,
        }
    }

    fn apply(p: &UninstallParams) -> Result<UninstallReport> {
        match prepare(p)? {
            PreparedUninstall::Complete(report) => Ok(report),
            PreparedUninstall::RemoveConfig(removal) => {
                let lock = ConfigDirLock::acquire(removal.root())?;
                removal.finish(&lock)
            }
        }
    }

    #[test]
    fn nothing_to_do_when_root_missing() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("never-existed");
        let r = apply(&params(root.clone())).unwrap();
        assert!(!r.service_was_installed);
        assert!(r.removed.is_empty());
        assert!(r.kept.is_empty());
        assert!(r.is_empty());
    }

    #[test]
    fn removes_files_and_subdirs_keeping_ca_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);

        let r = apply(&params(root.clone())).unwrap();

        // CA dir survives + root survives (because something was kept).
        assert!(root.join("ca").exists());
        assert!(root.join("ca").join("private.key").exists());
        assert!(root.exists());
        // Everything else is gone.
        assert!(!root.join("resolver.json").exists());
        assert!(!root.join("perms.json").exists());
        assert!(!root.join("client.json").exists());
        assert!(!root.join("id-map.json").exists());
        assert!(!root.join("activation").exists());
        assert!(!root.join("tls").exists());

        // Report matches.
        assert_eq!(r.kept.len(), 1);
        assert_eq!(r.kept[0].0, root.join("ca"));
        assert_eq!(r.kept[0].1, KeepReason::CaPreserved);
        // No root in `removed` because we kept ca/.
        assert!(!r.removed.contains(&root));
    }

    #[test]
    fn removes_ca_when_opted_in() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);

        let mut p = params(root.clone());
        p.remove_ca = true;
        let r = apply(&p).unwrap();

        assert!(!root.join("ca").exists());
        // With nothing kept, the root itself is removed.
        assert!(!root.exists());
        assert!(r.kept.is_empty());
        assert!(r.removed.contains(&root));
    }

    #[test]
    fn dry_run_makes_no_changes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);

        let p = params(root.clone());
        let r = preview(&p).unwrap();

        // All files still on disk.
        assert!(root.join("resolver.json").exists());
        assert!(root.join("ca").join("private.key").exists());
        assert!(root.join("tls").join("resolver").join("certificate.pem").exists());
        // But the report reflects the intent.
        assert!(!r.removed.is_empty());
        assert_eq!(r.kept.len(), 1);
    }

    #[test]
    fn dry_run_with_remove_ca_reports_root_removal() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);

        let mut p = params(root.clone());
        p.remove_ca = true;
        let r = preview(&p).unwrap();

        // Nothing actually deleted ...
        assert!(root.join("ca").join("private.key").exists());
        // ... but ca/ AND the root itself appear in `removed`.
        assert!(r.removed.contains(&root.join("ca")));
        assert!(r.removed.contains(&root));
        assert!(r.kept.is_empty());
    }

    /// A symlink inside the config root must be unlinked (not
    /// followed). Otherwise an operator-staged symlink pointing at
    /// `~/Documents` would have `remove_dir_all` walk into it.
    #[test]
    #[cfg(unix)]
    fn symlinks_are_unlinked_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        fs::create_dir_all(&root).unwrap();
        // The "real" directory we want to make sure isn't touched.
        let outside = dir.path().join("not-netidx");
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("important.txt"), b"keep me").unwrap();
        // A symlink inside the config root pointing at it.
        std::os::unix::fs::symlink(&outside, root.join("link-out")).unwrap();

        let r = apply(&params(root.clone())).unwrap();

        // Symlink is gone, target is untouched.
        assert!(!root.join("link-out").exists());
        assert!(outside.join("important.txt").exists());
        assert!(r.removed.contains(&root.join("link-out")));
    }

    #[test]
    fn idempotent_second_run_is_clean() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);
        let mut p = params(root.clone());
        p.remove_ca = true;

        let r1 = apply(&p).unwrap();
        assert!(!r1.is_empty());
        let r2 = apply(&p).unwrap();
        assert!(r2.is_empty());
    }

    #[test]
    fn service_status_error_preserves_all_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);
        let mut p = params(root.clone());
        p.remove_ca = true;

        let result =
            preview_with_service(&p, |_| anyhow::bail!("service manager unavailable"));
        assert!(result.is_err());
        assert!(root.join("resolver.json").exists());
        assert!(root.join("ca/private.key").exists());
    }

    #[test]
    fn service_teardown_error_preserves_all_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);
        let mut p = params(root.clone());
        p.remove_ca = true;

        let result = prepare_with_service(
            &p,
            |_| Ok(ServiceStatus::Active),
            |_| anyhow::bail!("unit is still active"),
        );
        assert!(result.is_err());
        assert!(root.join("resolver.json").exists());
        assert!(root.join("ca/private.key").exists());
    }
}
