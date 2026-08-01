//! Wholesale teardown of a netidx install: stop / remove the OS
//! service, then delete the contents of the canonical config root.
//!
//! The CA dir (`<config_root>/CA/`) is treated as **data**, not
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
    answer::Answerer,
    config_lock::ConfigDirLock,
    paths,
    provenance::InstallRecord,
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
    /// Also delete `<config_root>/CA/`. **DANGEROUS** — loss of the
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
#[derive(Debug, Default, Clone)]
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
    /// The CA directory this teardown destroys, when it destroys one.
    /// Recorded where the decision is made, so no caller has to
    /// re-derive it by matching a path's basename against `"ca"`.
    pub ca_destroyed: Option<PathBuf>,
}

impl UninstallReport {
    /// True if both the service and the config root were already
    /// gone — the uninstall had nothing to do.
    pub fn is_empty(&self) -> bool {
        !self.service_was_installed && self.removed.is_empty() && self.kept.is_empty()
    }

    fn absorb(&mut self, other: UninstallReport) {
        self.service_was_installed |= other.service_was_installed;
        self.removed.extend(other.removed);
        self.kept.extend(other.kept);
        self.ca_destroyed = self.ca_destroyed.take().or(other.ca_destroyed);
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

/// A whole-host teardown, as the operator asked for it. Everything optional
/// is resolved by [`plan`]: the scope from the install record, the service
/// name from the platform default, whether root is needed from an actual
/// probe rather than a guess about the role.
#[derive(Debug, Clone)]
pub struct UninstallInput {
    /// Tear down this scope only. `None` detects it from the install record.
    pub scope: Option<ServiceScope>,
    /// Service name to disable and remove. `None` ⇒
    /// [`ServiceParams::DEFAULT_NAME`].
    pub service_name: Option<String>,
    /// The user a templated system-scope unit was instantiated as.
    pub for_user: Option<String>,
    /// Override the config root. `None` ⇒ the canonical root for the scope.
    pub config_dir: Option<PathBuf>,
    /// Also destroy the CA private key. Irreversible.
    pub remove_ca: bool,
    /// Also look for the system-scope service a *user*-scope install
    /// registers. Default `true`.
    ///
    /// A caller sets this `false` only once it has reported a system-scope
    /// remnant it has no way to remove — on a platform where this process
    /// cannot become root, an unremovable service must not also cost the
    /// operator the unprivileged half of the teardown.
    pub cross_scope: bool,
}

impl Default for UninstallInput {
    fn default() -> Self {
        UninstallInput {
            scope: None,
            service_name: None,
            for_user: None,
            config_dir: None,
            remove_ca: false,
            cross_scope: true,
        }
    }
}

/// The elevated run a frontend must perform. Everything the elevated process
/// needs is named here, so no frontend invents its own argument list.
#[derive(Debug, Clone)]
pub struct Escalation {
    pub scope: ServiceScope,
    pub service_name: String,
    pub for_user: String,
    pub config_dir: Option<PathBuf>,
    pub remove_ca: bool,
    /// How much of the teardown the elevated run performs.
    pub covers: Covers,
    /// What the elevated run will do, so the operator can see it first.
    pub plan: UninstallReport,
}

/// How much of a teardown an [`Escalation`] performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Covers {
    /// The whole teardown. Nothing is left for the unprivileged caller.
    Everything,
    /// Only the system-scope service that a *user*-scope install registered
    /// (the templated resolver / publisher layout). The caller must call
    /// [`plan`] again afterwards to remove the user-scope configuration.
    SystemServiceOnly,
}

/// What a frontend must do next.
pub enum Next {
    /// Nothing is installed at this scope; the report says so.
    Nothing(UninstallReport),
    /// Perform this elevated step, then call [`plan`] again with the same
    /// input — the second pass sees its effect. Being told to escalate twice
    /// means the elevated step did not take effect.
    Escalate(Escalation),
    /// Confirm this plan with the operator, then hand it to [`apply`].
    Apply(Prepared),
}

/// This host's registration with the CA, which teardown must withdraw *before*
/// deleting the credentials that authenticate the withdrawal.
#[derive(Debug, Clone, Copy)]
pub struct Deregistration {
    /// The admin server leaving the map.
    pub server: std::net::SocketAddr,
    /// The CA it registered with.
    pub ca: std::net::SocketAddr,
}

/// A verified teardown, ready to apply. Holding one means every question the
/// engine can answer has been answered: what is installed, what root is
/// needed, what will be destroyed. All that is left is the operator's consent.
pub struct Prepared {
    params: UninstallParams,
    root: Option<PathBuf>,
    record: Option<InstallRecord>,
    plan: UninstallReport,
    /// A system-scope service a user-scope install registered, removable
    /// here because this process already holds root.
    cross: Option<UninstallParams>,
    cross_plan: Option<UninstallReport>,
    deregister: Option<Deregistration>,
}

impl Prepared {
    /// What this teardown will do at its primary scope.
    pub fn plan(&self) -> &UninstallReport {
        &self.plan
    }

    /// A system-scope service this run will also remove, when a user-scope
    /// install registered one and this process holds root.
    pub fn cross_scope_plan(&self) -> Option<&UninstallReport> {
        self.cross_plan.as_ref()
    }

    /// The install being torn down, when it left a provenance marker.
    pub fn record(&self) -> Option<&InstallRecord> {
        self.record.as_ref()
    }

    /// The registration this teardown will withdraw from the CA.
    pub fn deregistration(&self) -> Option<Deregistration> {
        self.deregister
    }

    /// Whether applying destroys the CA private key — the one irreversible
    /// thing an uninstall can do.
    pub fn destroys_ca(&self) -> bool {
        self.plan.ca_destroyed.is_some()
            || self.cross_plan.as_ref().is_some_and(|p| p.ca_destroyed.is_some())
    }
}

/// Resolve the teardown: what scope, what is installed, whether root is
/// needed, and what will be destroyed. Writes nothing.
pub fn plan(input: &UninstallInput) -> Result<Next> {
    plan_with(input, service::is_elevated()?, preview)
}

fn plan_with(
    input: &UninstallInput,
    elevated: bool,
    preview: impl Fn(&UninstallParams) -> Result<UninstallReport>,
) -> Result<Next> {
    let scope = match input.scope {
        Some(scope) => scope,
        None => detect_primary_scope(),
    };
    let params = params_for(input, scope, input.config_dir.clone());
    // Elevate before previewing anything, so the plan the operator sees is the
    // one root sees — an unprivileged process may not be able to list /etc.
    if scope == ServiceScope::System && !elevated {
        return Ok(Next::Escalate(escalation(
            input,
            &params,
            Covers::Everything,
            UninstallReport::default(),
        )?));
    }
    let plan = preview(&params)?;
    // A templated install (resolver, publisher) registers a *system*-scope
    // service even though the config it writes is user-scope, so it never
    // appears in the user-scope probe. A user-scope teardown must still catch
    // it, or `install` then `uninstall` leaves the daemons running. Probe
    // uncertainty is fatal: that service may be reading the configuration we
    // are about to delete.
    let cross = match scope {
        // Never inherit a user-scope --config-dir override: it named the user
        // directory, not /etc.
        ServiceScope::User if input.cross_scope => {
            Some(params_for(input, ServiceScope::System, None))
        }
        ServiceScope::User | ServiceScope::System => None,
    };
    let cross_plan = match &cross {
        Some(cross) => {
            let found = preview(cross).context(
                "could not verify the system-scope service; refusing to remove user \
                 configuration",
            )?;
            (!found.is_empty()).then_some(found)
        }
        None => None,
    };
    if let Some(cross_plan) = &cross_plan
        && !elevated
    {
        return Ok(Next::Escalate(escalation(
            input,
            cross.as_ref().expect("a cross plan implies a cross probe"),
            Covers::SystemServiceOnly,
            cross_plan.clone(),
        )?));
    }
    if plan.is_empty() && cross_plan.is_none() {
        return Ok(Next::Nothing(plan));
    }
    let root = config_root(input, scope);
    Ok(Next::Apply(Prepared {
        record: root.as_deref().and_then(load_install_record),
        deregister: root.as_deref().and_then(pending_deregistration),
        cross: cross.filter(|_| cross_plan.is_some()),
        cross_plan,
        plan,
        root,
        params,
    }))
}

/// Tear the install down, in the one order that works: stop every supervisor
/// that may be reading the configuration, withdraw this host from the CA's map
/// while the credentials that authenticate the withdrawal still exist, and
/// only then delete the configuration.
pub async fn apply(
    ans: &mut dyn Answerer,
    prepared: Prepared,
) -> Result<UninstallReport> {
    let Prepared { params, root, plan: _, cross, cross_plan: _, deregister, record: _ } =
        prepared;
    let mut report = UninstallReport::default();
    if let Some(cross) = cross {
        report.absorb(finish(&cross)?);
    }
    if let Some(deregister) = deregister {
        deregister_admin_server(ans, root.as_deref(), deregister).await;
    }
    report.absorb(finish(&params)?);
    Ok(report)
}

fn finish(params: &UninstallParams) -> Result<UninstallReport> {
    match prepare(params)? {
        PreparedUninstall::Complete(report) => Ok(report),
        PreparedUninstall::RemoveConfig(removal) => {
            let lock = ConfigDirLock::acquire(removal.root())?;
            removal.finish(&lock)
        }
    }
}

fn params_for(
    input: &UninstallInput,
    scope: ServiceScope,
    config_dir: Option<PathBuf>,
) -> UninstallParams {
    UninstallParams {
        scope,
        service_name: input
            .service_name
            .clone()
            .unwrap_or_else(|| ServiceParams::DEFAULT_NAME.to_string()),
        for_user: input.for_user.clone(),
        config_dir,
        remove_ca: input.remove_ca,
    }
}

fn escalation(
    input: &UninstallInput,
    params: &UninstallParams,
    covers: Covers,
    plan: UninstallReport,
) -> Result<Escalation> {
    Ok(Escalation {
        scope: ServiceScope::System,
        service_name: params.service_name.clone(),
        // Resolve the account *before* escalating: the elevated child sees
        // root, not the user whose templated unit this is.
        for_user: service::resolve_for_user(input.for_user.clone())?,
        config_dir: params.config_dir.clone(),
        remove_ca: input.remove_ca,
        covers,
        plan,
    })
}

/// The scope to tear down when the operator named none: prefer a user-scope
/// install record (the common templated case — user config plus a system
/// service the cross-probe catches), else a system-scope record, else user.
fn detect_primary_scope() -> ServiceScope {
    let user_record = paths::user_config_root()
        .map(|r| r.join("install.json").exists())
        .unwrap_or(false);
    if user_record {
        return ServiceScope::User;
    }
    if paths::system_config_root().join("install.json").exists() {
        return ServiceScope::System;
    }
    ServiceScope::User
}

/// The config root this teardown targets, honouring a `config_dir` override.
fn config_root(input: &UninstallInput, scope: ServiceScope) -> Option<PathBuf> {
    match &input.config_dir {
        Some(d) => Some(d.clone()),
        None => match scope {
            ServiceScope::User => paths::user_config_root().ok(),
            ServiceScope::System => Some(paths::system_config_root()),
        },
    }
}

/// The install provenance marker, when there is one. Reporting the role is a
/// convenience, never a gate: a hand-rolled config simply has none.
fn load_install_record(root: &Path) -> Option<InstallRecord> {
    let path = root.join("install.json");
    path.exists().then(|| InstallRecord::load(&path).ok()).flatten()
}

/// Whether this host has a registration to withdraw. A non-CA admin server
/// registers its facts with the CA, so on teardown it should deregister or
/// the CA keeps a dead entry until `admin ca remove-server`. The CA host owns
/// the map and has nothing to deregister from. Unix-only: the admin-server
/// daemon is, so only a unix host ever has one.
#[cfg(unix)]
fn pending_deregistration(root: &Path) -> Option<Deregistration> {
    let cfg = crate::admin_server_config::load(&root.join("admin-server.json")).ok()?;
    if cfg.roles.ca.is_some() {
        return None;
    }
    Some(Deregistration { server: cfg.listen, ca: cfg.ca_addr? })
}

#[cfg(not(unix))]
fn pending_deregistration(_root: &Path) -> Option<Deregistration> {
    None
}

/// Withdraw this host from the CA's map. Best-effort and reported, never
/// fatal: a CA that cannot be reached must not strand an operator with a
/// half-removed install.
#[cfg(unix)]
async fn deregister_admin_server(
    ans: &mut dyn Answerer,
    root: Option<&Path>,
    what: Deregistration,
) {
    use crate::transport;
    let result = async {
        let root = root.context("deregistration requires a config root")?;
        let cfg = crate::admin_server_config::load(&root.join("admin-server.json"))?;
        let cert = std::fs::read(&cfg.serving_cert).with_context(|| {
            format!("reading serving cert {}", cfg.serving_cert.display())
        })?;
        let key = std::fs::read(&cfg.serving_key).with_context(|| {
            format!("reading serving key {}", cfg.serving_key.display())
        })?;
        let trusted = std::fs::read(&cfg.trusted)
            .with_context(|| format!("reading trust bundle {}", cfg.trusted.display()))?;
        let roots = crate::load_roots(&trusted)?;
        let home_ca = transport::home_ca_from_chain(&cert)?;
        let client = transport::AuthenticatedPkiClient::from_pem(roots, &cert, &key)?;
        transport::deregister(&client, what.ca, home_ca).await?;
        anyhow::Ok(())
    }
    .await;
    match result {
        Ok(()) => ans.note(&format!(
            "admin server: deregistered {} from the CA at {}",
            what.server, what.ca
        )),
        Err(e) => ans.warn(&format!(
            "admin server: could not deregister from the CA at {} ({e:#}); the CA will \
             keep this server in its map until `netidx admin ca remove-server`",
            what.ca
        )),
    }
}

#[cfg(not(unix))]
async fn deregister_admin_server(
    _ans: &mut dyn Answerer,
    _root: Option<&Path>,
    _what: Deregistration,
) {
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
        if path.file_name().and_then(|s| s.to_str()) == Some("ca") {
            if !p.remove_ca {
                report.kept.push((path, KeepReason::CaPreserved));
                continue;
            }
            report.ca_destroyed = Some(path.clone());
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
    use crate::provenance::InstallRole;
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
        fs::write(root.join("ca").join("certificate.pem"), b"CA cert").unwrap();
        fs::write(root.join("ca").join("private.key"), b"CA key").unwrap();
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
        finish(p)
    }

    fn input(config_dir: PathBuf) -> UninstallInput {
        UninstallInput {
            scope: Some(ServiceScope::User),
            service_name: Some(test_service_name()),
            config_dir: Some(config_dir),
            ..Default::default()
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
        // No root in `removed` because we kept CA/.
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
        // ... but CA/ AND the root itself appear in `removed`.
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

    /// Nothing else in the system-scope probe is under test, so drive `plan`
    /// with a stub that reports a clean /etc — otherwise the result depends on
    /// whether the machine running the tests happens to have netidx installed.
    fn nothing_at_system_scope(
        p: &UninstallParams,
    ) -> impl Fn(&UninstallParams) -> Result<UninstallReport> + use<> {
        let primary = p.clone();
        move |q: &UninstallParams| {
            if q.config_dir == primary.config_dir {
                preview(q)
            } else {
                Ok(UninstallReport::default())
            }
        }
    }

    #[test]
    fn a_teardown_that_keeps_the_ca_does_not_claim_to_destroy_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);

        let keep = input(root.clone());
        let stub = nothing_at_system_scope(&params(root.clone()));
        let Next::Apply(prepared) = plan_with(&keep, true, &stub).unwrap() else {
            panic!("a populated root has something to remove")
        };
        assert!(!prepared.destroys_ca());
        assert_eq!(prepared.plan().ca_destroyed, None);

        let mut destroy = keep;
        destroy.remove_ca = true;
        let Next::Apply(prepared) = plan_with(&destroy, true, &stub).unwrap() else {
            panic!("a populated root has something to remove")
        };
        // Recorded where the decision was made, so nothing has to re-derive it
        // by matching a path's basename.
        assert!(prepared.destroys_ca());
        assert_eq!(prepared.plan().ca_destroyed, Some(root.join("ca")));
    }

    #[test]
    fn an_empty_host_has_nothing_to_tear_down() {
        let dir = tempfile::tempdir().unwrap();
        let absent = input(dir.path().join("never-existed"));
        let stub = nothing_at_system_scope(&params(dir.path().join("never-existed")));
        let Next::Nothing(report) = plan_with(&absent, true, &stub).unwrap() else {
            panic!("an absent root has nothing to remove")
        };
        assert!(report.is_empty());
    }

    #[test]
    fn a_teardown_names_the_role_it_is_removing() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);
        let record = InstallRecord::new(InstallRole::Resolver, "/", "tls", None, None);
        fs::write(root.join("install.json"), serde_json::to_vec(&record).unwrap())
            .unwrap();

        let stub = nothing_at_system_scope(&params(root.clone()));
        let Next::Apply(prepared) = plan_with(&input(root), true, &stub).unwrap() else {
            panic!("a populated root has something to remove")
        };
        assert_eq!(prepared.record().map(|r| r.role), Some(InstallRole::Resolver));
    }

    /// A templated resolver writes user-scope config but registers a
    /// system-scope service, and only root can remove that service. The
    /// engine learns this from the probe, not from the role: a workstation
    /// with the same role would not escalate, and a hand-rolled publisher
    /// with no system service must not either.
    #[test]
    fn root_is_required_because_a_system_service_is_there_not_because_of_the_role() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);
        let system_service = |q: &UninstallParams| match q.scope {
            ServiceScope::System => Ok(UninstallReport {
                service_was_installed: true,
                ..UninstallReport::default()
            }),
            ServiceScope::User => preview(q),
        };

        let Next::Escalate(escalation) =
            plan_with(&input(root.clone()), false, system_service).unwrap()
        else {
            panic!("an unprivileged teardown must escalate for a system service")
        };
        assert_eq!(escalation.covers, Covers::SystemServiceOnly);
        assert_eq!(escalation.scope, ServiceScope::System);
        // The elevated run must not inherit the user-scope --config-dir: it
        // named the user directory, not /etc.
        assert_eq!(escalation.config_dir, None);

        // Already root: the same host removes both scopes in one pass.
        let Next::Apply(prepared) =
            plan_with(&input(root), true, system_service).unwrap()
        else {
            panic!("a root teardown removes the system service directly")
        };
        assert!(prepared.cross_scope_plan().is_some());
    }

    #[test]
    fn a_system_scope_teardown_escalates_before_it_previews_anything() {
        let dir = tempfile::tempdir().unwrap();
        let mut system = input(dir.path().join("netidx"));
        system.scope = Some(ServiceScope::System);
        let Next::Escalate(escalation) = plan_with(&system, false, |_| {
            panic!("previewing before escalation shows the unprivileged view")
        })
        .unwrap() else {
            panic!("an unprivileged system-scope teardown must escalate")
        };
        assert_eq!(escalation.covers, Covers::Everything);
    }

    /// The report a caller sees after a teardown that spans two scopes must
    /// account for both, or the operator is told less was removed than was.
    #[test]
    fn a_combined_report_accounts_for_every_scope() {
        let mut report = UninstallReport {
            service_was_installed: false,
            removed: vec![PathBuf::from("/etc/netidx")],
            kept: vec![],
            ca_destroyed: None,
        };
        report.absorb(UninstallReport {
            service_was_installed: true,
            removed: vec![PathBuf::from("/home/u/.config/netidx")],
            kept: vec![(
                PathBuf::from("/home/u/.config/netidx/ca"),
                KeepReason::CaPreserved,
            )],
            ca_destroyed: Some(PathBuf::from("/etc/netidx/ca")),
        });
        assert!(report.service_was_installed);
        assert_eq!(report.removed.len(), 2);
        assert_eq!(report.kept.len(), 1);
        assert_eq!(report.ca_destroyed, Some(PathBuf::from("/etc/netidx/ca")));
    }

    /// A host whose system-scope service cannot be removed — no way to become
    /// root from here — must still be able to tear down its user scope.
    /// Suppressing the cross-scope probe is how a frontend says so, and
    /// without it a Windows workstation uninstall aborts over a service it was
    /// never going to be able to remove.
    #[test]
    fn the_user_scope_can_be_torn_down_without_the_system_one() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("netidx");
        populate_root(&root);
        let system_service = |q: &UninstallParams| match q.scope {
            ServiceScope::System => Ok(UninstallReport {
                service_was_installed: true,
                ..UninstallReport::default()
            }),
            ServiceScope::User => preview(q),
        };

        // With the probe on, an unprivileged caller is sent to escalate.
        assert!(matches!(
            plan_with(&input(root.clone()), false, system_service).unwrap(),
            Next::Escalate(_)
        ));

        // With it off, the same caller gets the user-scope work it can do.
        let alone = UninstallInput { cross_scope: false, ..input(root.clone()) };
        let Next::Apply(prepared) = plan_with(&alone, false, system_service).unwrap()
        else {
            panic!("the user scope is removable without root")
        };
        assert!(prepared.cross_scope_plan().is_none());
        assert!(!prepared.plan().removed.is_empty());
    }
}
