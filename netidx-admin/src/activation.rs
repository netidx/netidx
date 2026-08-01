//! Activation-unit-directory engine wrapper.
//!
//! The schema and the daemon runtime live in the `netidx-activation`
//! crate (Layer 0). This module is the engine-side veneer: a typed
//! `ActivationDir` that opens a directory (default search or explicit
//! path), lists / fetches / writes / removes `<name>.unit` files via
//! atomic writes, and a structural `validate` that catches cross-unit
//! trigger conflicts before they land on disk.
//!
//! Naming: the on-disk file is `<name>.unit`; the API keys (in `list`'s
//! returned map and in the `name` argument to `save` / `get` / `remove`)
//! are bare basenames *without* the `.unit` suffix, which is what the
//! CLI exposes (`netidx admin component activation add my-publisher …`).

use crate::{atomic, paths};
use anyhow::{Context, Result};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub use netidx_activation::file::{
    Environment, ProcessCfg, ProcessCfgBuilder, Restart, Trigger, Unit, UnitBuilder,
};

const UNIT_SUFFIX: &str = ".unit";

/// Tell a running supervisor to re-read `units_dir`.
///
/// It has no directory watch — it re-reads on SIGHUP or this control op —
/// so a unit added to a *live* host is inert until something calls this. At
/// install time nothing needs to: the install ends by starting the service,
/// which reads the directory anyway. `Ok(false)` means no supervisor was
/// listening, which is the normal case on a host whose service hasn't been
/// registered yet.
pub async fn reload(units_dir: &Path) -> Result<bool> {
    use netidx_activation::control::{
        ControlOp, ControlRequest, ControlResponse, control,
    };
    let req = ControlRequest { op: ControlOp::Reload, units: Vec::new() };
    match control(units_dir, &req).await {
        Ok(ControlResponse::Ok { .. }) => Ok(true),
        Ok(ControlResponse::Err { reason }) => {
            anyhow::bail!("the activation supervisor refused to reload: {reason}")
        }
        Err(_) => Ok(false),
    }
}

/// A handle to an activation unit directory.
#[derive(Debug, Clone)]
pub struct ActivationDir {
    dir: PathBuf,
}

impl ActivationDir {
    /// Open a unit directory. If `path` is `Some`, use it (creating it
    /// if needed). If `None`, use the user default at
    /// [`paths::user_activation_dir`] (creating it if needed).
    pub fn open<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let dir = match path {
            Some(p) => p.as_ref().to_path_buf(),
            None => paths::user_activation_dir()?,
        };
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("creating activation dir {dir:?}"))?;
        Ok(Self { dir })
    }

    /// Open at the user default location, without creating.
    pub fn open_default() -> Result<Self> {
        let dir = paths::user_activation_dir()?;
        if !dir.is_dir() {
            bail!("activation dir {:?} does not exist", dir);
        }
        Ok(Self { dir })
    }

    /// The directory this handle points at.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// List all units. Keys are basenames without the `.unit` suffix.
    /// Errors encountered while parsing individual files are propagated
    /// — the engine refuses to silently drop corrupt units (the daemon
    /// can be more lenient at runtime; the engine, called by a human
    /// editor or by `init`, wants the full error).
    pub fn list(&self) -> Result<BTreeMap<String, Unit>> {
        let mut out = BTreeMap::new();
        for ent in std::fs::read_dir(&self.dir)
            .with_context(|| format!("reading {:?}", self.dir))?
        {
            let ent = ent?;
            let typ = ent.file_type()?;
            if !typ.is_file() && !typ.is_symlink() {
                continue;
            }
            let file_name = ent.file_name();
            let name_str = file_name.to_string_lossy();
            if !name_str.ends_with(UNIT_SUFFIX) {
                continue;
            }
            let basename = name_str
                .strip_suffix(UNIT_SUFFIX)
                .expect("ends_with returned true")
                .to_string();
            let bytes = std::fs::read(ent.path())
                .with_context(|| format!("reading {:?}", ent.path()))?;
            let unit: Unit = serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing {:?}", ent.path()))?;
            out.insert(basename, unit);
        }
        Ok(out)
    }

    /// Read a single unit by basename (without the `.unit` suffix).
    pub fn get(&self, name: &str) -> Result<Unit> {
        let path = self.unit_path(name);
        let bytes =
            std::fs::read(&path).with_context(|| format!("reading {:?}", path))?;
        let unit = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing {:?}", path))?;
        Ok(unit)
    }

    /// Save a unit atomically at `<dir>/<name>.unit`, mode 0o644.
    /// Validates only the basename (no path separators, no
    /// `.unit` suffix). Cross-unit invariants (e.g. trigger
    /// conflicts) are NOT checked here — call [`validate`] on the
    /// full set first if you care; per-unit runtime invariants
    /// (e.g. executable exists) are also out of scope on the
    /// engine side and live in `netidx-activation::runtime`.
    pub fn save(&self, name: &str, unit: &Unit) -> Result<()> {
        ensure_valid_basename(name)?;
        atomic::write_atomic_pretty_json(&self.unit_path(name), unit)
    }

    /// Remove a unit. No-op if it doesn't exist.
    pub fn remove(&self, name: &str) -> Result<()> {
        ensure_valid_basename(name)?;
        let path = self.unit_path(name);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(anyhow!("removing {:?}: {e}", path)),
        }
    }

    /// Resolve the on-disk path for a unit basename in this directory.
    /// Does not check whether the file exists.
    pub fn unit_path(&self, name: &str) -> PathBuf {
        unit_path_in(&self.dir, name)
    }

    /// Every unit in this directory *except* `name` — the set a candidate for
    /// `name` is validated against, so an editor loop can re-check on each
    /// re-edit without a unit conflicting with its own previous definition.
    pub fn others(&self, name: &str) -> Result<BTreeMap<String, Unit>> {
        let mut units = self.list()?;
        units.remove(name);
        Ok(units)
    }

    /// Install or replace `name`: check the whole resulting set for cross-unit
    /// trigger conflicts, write it, then tell a running supervisor to re-read
    /// the directory.
    ///
    /// The reload is not optional. The supervisor has no directory watch, so
    /// without it a unit added to a live host sits inert until the next SIGHUP
    /// — which is what `netidx admin component activation add` used to do.
    /// `Ok(false)` means nothing was listening, the normal case at install time.
    pub async fn install(&self, name: &str, unit: &Unit) -> Result<bool> {
        let mut set = self.others(name)?;
        set.insert(name.to_string(), unit.clone());
        validate(&set)?;
        self.save(name, unit)?;
        reload(&self.dir).await
    }

    /// Remove `name` and reload, with the same meaning for `Ok(false)`.
    pub async fn uninstall(&self, name: &str) -> Result<bool> {
        self.remove(name)?;
        reload(&self.dir).await
    }
}

/// A unit definition merged with the run-state the supervisor reports.
///
/// The union of both sources, because a mismatch is exactly what an operator
/// needs to see: a unit defined on disk the supervisor has not loaded, and one
/// it reports with no file behind it, both get a row.
pub struct UnitStatus {
    pub name: String,
    /// The on-disk definition, absent when only the supervisor knows this unit.
    pub unit: Option<Unit>,
    /// The live state, absent when the supervisor has not loaded it.
    pub state: Option<netidx_activation::control::UnitState>,
}

/// Every unit this directory defines or the supervisor is running, by name.
pub async fn list_with_state(units_dir: &Path) -> Result<Vec<UnitStatus>> {
    use netidx_activation::control::{
        ControlOp, ControlRequest, ControlResponse, control,
    };
    let defs = ActivationDir::open(Some(units_dir))?.list()?;
    let req = ControlRequest { op: ControlOp::Status, units: Vec::new() };
    let reported = match control(units_dir, &req).await? {
        ControlResponse::Ok { units } => units,
        ControlResponse::Err { reason } => bail!("{reason}"),
    };
    let mut states: BTreeMap<String, _> =
        reported.into_iter().map(|u| (u.unit, u.state)).collect();
    let mut names: std::collections::BTreeSet<String> = defs.keys().cloned().collect();
    names.extend(states.keys().cloned());
    Ok(names
        .into_iter()
        .map(|name| UnitStatus {
            unit: defs.get(&name).cloned(),
            state: states.remove(&name),
            name,
        })
        .collect())
}

/// Drive this host's own supervisor over its control socket — start, stop,
/// restart, or status one unit or all of them. The remote counterpart, over an
/// admin server, is [`crate::ops::service::control_remote`].
pub async fn control_local(
    units_dir: &Path,
    op: netidx_activation::control::ControlOp,
    units: Vec<String>,
) -> Result<Vec<netidx_activation::control::UnitStatus>> {
    use netidx_activation::control::{ControlRequest, ControlResponse, control};
    match control(units_dir, &ControlRequest { op, units }).await? {
        ControlResponse::Ok { units } => Ok(units),
        ControlResponse::Err { reason } => bail!("{reason}"),
    }
}

/// The starter unit a create flow seeds an editor with. Built through the
/// builders so it round-trips the `deny_unknown_fields` decoder.
pub fn template_unit() -> Unit {
    UnitBuilder::default()
        .process(
            ProcessCfgBuilder::default()
                .exe("/path/to/executable")
                .build()
                .expect("template process cfg"),
        )
        .build()
        .expect("template unit")
}

/// Free-function form of [`ActivationDir::unit_path`] for callers that
/// know the directory but don't want to `open()` it (e.g. pre-write
/// existence checks against a `RenderedTemplate`).
pub fn unit_path_in(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}{UNIT_SUFFIX}"))
}

fn ensure_valid_basename(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("unit name must not be empty");
    }
    if name.contains('/') || name.contains('\\') {
        bail!("unit name may not contain path separators: {name:?}");
    }
    if name.ends_with(UNIT_SUFFIX) {
        bail!("unit name should be the basename without `.unit`: got {name:?}");
    }
    Ok(())
}

/// Structural validation across a set of units. Currently checks for
/// conflicting `OnAccess` triggers — no two units may claim the same
/// path — which mirrors the daemon's startup check
/// (`netidx-activation/src/runtime.rs`). Filesystem checks (exe exists,
/// is executable) belong to the daemon and are deliberately *not* run
/// here.
pub fn validate(units: &BTreeMap<String, Unit>) -> Result<()> {
    netidx_activation::file::check_trigger_conflicts(
        units.iter().map(|(name, unit)| (name.as_str(), unit)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use netidx::path::Path as NetidxPath;
    use std::collections::BTreeSet;

    /// Writing a unit tells a live supervisor to re-read the directory.
    ///
    /// There is no supervisor here, which is the case that must not be an
    /// error: at install time nothing is listening yet, and the service reads
    /// the directory when it starts. `netidx admin component activation add`
    /// used to stop at the write, so on a host that *was* live the unit sat
    /// inert until the next SIGHUP.
    #[tokio::test]
    async fn writing_a_unit_reloads_and_tolerates_no_supervisor() {
        let dir = tempfile::tempdir().unwrap();
        let ad = ActivationDir::open(Some(dir.path())).unwrap();
        assert!(!ad.install("one", &unit("/bin/true")).await.unwrap());
        assert!(ad.unit_path("one").is_file());
        assert!(!ad.uninstall("one").await.unwrap());
        assert!(!ad.unit_path("one").exists());
    }

    /// A unit is validated against the rest of the set, but not against its
    /// own previous definition — re-saving a unit unchanged must not read as a
    /// conflict with itself.
    #[tokio::test]
    async fn a_unit_does_not_conflict_with_its_own_previous_definition() {
        let dir = tempfile::tempdir().unwrap();
        let ad = ActivationDir::open(Some(dir.path())).unwrap();
        let mut claim = unit("/bin/true");
        claim.trigger = Trigger::OnAccess(
            [NetidxPath::from("/a")].into_iter().collect::<BTreeSet<_>>(),
        );
        ad.install("mine", &claim).await.unwrap();
        ad.install("mine", &claim).await.expect("re-saving is not a self-conflict");
        // A *different* unit claiming the same path still is one.
        let e = ad.install("other", &claim).await.unwrap_err();
        assert!(format!("{e:#}").contains("conflicting OnAccess trigger"), "{e:#}");
        assert!(!ad.unit_path("other").exists(), "a refused unit is not written");
    }

    fn unit(exe: &str) -> Unit {
        UnitBuilder::default()
            .process(ProcessCfgBuilder::default().exe(exe).build().unwrap())
            .build()
            .unwrap()
    }

    #[test]
    fn save_get_list_remove_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let ad = ActivationDir::open(Some(dir.path())).unwrap();
        ad.save("foo", &unit("/bin/true")).unwrap();
        ad.save("bar", &unit("/bin/false")).unwrap();

        let listing = ad.list().unwrap();
        assert_eq!(listing.len(), 2);
        assert!(listing.contains_key("foo"));
        assert!(listing.contains_key("bar"));

        let foo = ad.get("foo").unwrap();
        assert_eq!(foo.process.exe, "/bin/true");

        ad.remove("foo").unwrap();
        let listing = ad.list().unwrap();
        assert_eq!(listing.len(), 1);
        assert!(!listing.contains_key("foo"));

        // remove of non-existent unit is a no-op
        ad.remove("does-not-exist").unwrap();
    }

    #[test]
    fn basename_validation() {
        let dir = tempfile::tempdir().unwrap();
        let ad = ActivationDir::open(Some(dir.path())).unwrap();
        let u = unit("/bin/true");
        assert!(ad.save("", &u).is_err());
        assert!(ad.save("a/b", &u).is_err());
        assert!(ad.save("foo.unit", &u).is_err());
    }

    #[test]
    fn list_ignores_non_unit_files() {
        let dir = tempfile::tempdir().unwrap();
        let ad = ActivationDir::open(Some(dir.path())).unwrap();
        ad.save("real", &unit("/bin/true")).unwrap();
        std::fs::write(dir.path().join("README"), "not a unit").unwrap();
        std::fs::write(dir.path().join("config.json"), "{}").unwrap();
        let listing = ad.list().unwrap();
        assert_eq!(listing.len(), 1);
        assert!(listing.contains_key("real"));
    }

    #[test]
    fn list_propagates_parse_errors() {
        let dir = tempfile::tempdir().unwrap();
        let ad = ActivationDir::open(Some(dir.path())).unwrap();
        std::fs::write(dir.path().join("bad.unit"), "this is not json").unwrap();
        assert!(ad.list().is_err());
    }

    #[test]
    fn validate_flags_trigger_conflict() {
        let p = NetidxPath::from("/foo");
        let mut units = BTreeMap::new();
        let mut paths_a = BTreeSet::new();
        paths_a.insert(p.clone());
        let mut a = unit("/bin/true");
        a.trigger = Trigger::OnAccess(paths_a);
        units.insert("a".to_string(), a);

        let mut paths_b = BTreeSet::new();
        paths_b.insert(p.clone());
        let mut b = unit("/bin/false");
        b.trigger = Trigger::OnAccess(paths_b);
        units.insert("b".to_string(), b);

        assert!(validate(&units).is_err());
    }

    #[test]
    fn validate_allows_distinct_triggers() {
        let mut units = BTreeMap::new();
        let mut a_paths = BTreeSet::new();
        a_paths.insert(NetidxPath::from("/foo"));
        let mut a = unit("/bin/true");
        a.trigger = Trigger::OnAccess(a_paths);
        units.insert("a".to_string(), a);

        let mut b_paths = BTreeSet::new();
        b_paths.insert(NetidxPath::from("/bar"));
        let mut b = unit("/bin/false");
        b.trigger = Trigger::OnAccess(b_paths);
        units.insert("b".to_string(), b);

        // Plus a third unit on OnStart, which never conflicts.
        units.insert("c".to_string(), unit("/bin/echo"));

        assert!(validate(&units).is_ok());
    }
}
