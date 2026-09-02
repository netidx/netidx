//! The Local tab's surface: what is installed on THIS host, the facts
//! about it that take a round trip (admin-domain sync, the CA's
//! credential state), and the lifecycle actions that run in-process.
//! Bridging only — which actions an install offers, the status card,
//! and every message the operator reads are the Graphix side's.

use crate::{
    admin_err,
    ceremony::{self, BoxOp, Trigger},
    ops::opt_string,
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use graphix_compiler::{
    Apply, BuiltIn, Event, ExecCtx, Node, Rt, Scope, TagValue, UserEvent,
    effects::Effect, errf, expr::ExprId, typ::FnType,
};
use graphix_package_core::{CachedArgsAsync, CachedVals, EvalCachedAsync};
use netidx_activation::control::{ControlOp, UnitState};
use netidx_admin::{
    activation::{
        self, ActivationDir, Environment, ProcessCfg, Restart, Trigger as UnitTrigger,
        Unit,
    },
    answer::{Answerer, Field},
    install_bundle::{BundleScope, Component},
    paths,
    plan::bundle,
    provenance::{InstallRecord, InstallRole},
    reconcile::Change,
    renewd,
    service::{self, ServiceParams, ServiceScope, ServiceStatus},
    sync,
};
use netidx_derive::{FromValue, IntoValue};
use netidx_value::{FromValue, ValArray, Value};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    marker::PhantomData,
    path::{Path, PathBuf},
};

// ── the data model ───────────────────────────────────────────────

#[derive(Debug, Clone, Copy, FromValue, IntoValue)]
enum InstallRoleV {
    Ca,
    Workstation,
    Resolver,
    Publisher,
}

impl From<InstallRole> for InstallRoleV {
    fn from(r: InstallRole) -> Self {
        match r {
            InstallRole::Ca => InstallRoleV::Ca,
            InstallRole::Workstation => InstallRoleV::Workstation,
            InstallRole::Resolver => InstallRoleV::Resolver,
            InstallRole::Publisher => InstallRoleV::Publisher,
        }
    }
}

impl From<InstallRoleV> for InstallRole {
    fn from(r: InstallRoleV) -> Self {
        match r {
            InstallRoleV::Ca => InstallRole::Ca,
            InstallRoleV::Workstation => InstallRole::Workstation,
            InstallRoleV::Resolver => InstallRole::Resolver,
            InstallRoleV::Publisher => InstallRole::Publisher,
        }
    }
}

#[derive(Debug, Clone, Copy, IntoValue)]
enum ServiceStateV {
    Active,
    Inactive,
    NotInstalled,
}

#[derive(Debug, Clone, Copy, FromValue, IntoValue)]
enum ScopeV {
    User,
    System,
}

impl From<ServiceScope> for ScopeV {
    fn from(s: ServiceScope) -> Self {
        match s {
            ServiceScope::User => ScopeV::User,
            ServiceScope::System => ScopeV::System,
        }
    }
}

impl From<ScopeV> for ServiceScope {
    fn from(s: ScopeV) -> Self {
        match s {
            ScopeV::User => ServiceScope::User,
            ScopeV::System => ServiceScope::System,
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
struct LocalCaV {
    ca_dir: String,
    cfg: Option<String>,
}

#[derive(Debug, Clone, IntoValue)]
struct AdminDomainRefV {
    domain: String,
    fingerprint: String,
}

#[derive(Debug, Clone, IntoValue)]
struct InstallV {
    role: InstallRoleV,
    base: String,
    auth: String,
    admin_domain: Option<AdminDomainRefV>,
    admin_servers: Vec<String>,
    created_unix: u64,
    service: ServiceStateV,
    scope: ScopeV,
    config_dir: String,
    local_ca: Option<LocalCaV>,
    renewable: bool,
}

/// Resolver / publisher / CA register a system-scope `netidx@<user>`
/// service even when their config is user-scope; a workstation uses a
/// user-scope service. Probe the scope that matches the role.
fn probe_service(record: &InstallRecord) -> ServiceStateV {
    let (scope, for_user) = match record.role {
        InstallRole::Workstation => (ServiceScope::User, None),
        InstallRole::Ca | InstallRole::Resolver | InstallRole::Publisher => {
            (ServiceScope::System, service::resolve_for_user(None).ok())
        }
    };
    let params = ServiceParams {
        scope,
        for_user,
        binary: PathBuf::new(),
        service_name: ServiceParams::DEFAULT_NAME.to_string(),
        activation_dir: None,
    };
    match service::status(&params).unwrap_or(ServiceStatus::NotInstalled) {
        ServiceStatus::Active => ServiceStateV::Active,
        ServiceStatus::Inactive => ServiceStateV::Inactive,
        ServiceStatus::NotInstalled => ServiceStateV::NotInstalled,
    }
}

/// Whether this config root owns a CA, and where its tooling lives.
/// The root's admin-server config names the CA directory it serves;
/// a config root without one owns a CA only at the conventional
/// `<root>/ca`. Local files only.
#[cfg(unix)]
fn local_ca(config_dir: &Path) -> Option<LocalCaV> {
    let cfg = config_dir.join("admin-server.json");
    let cfg = cfg.is_file().then_some(cfg);
    let configured = cfg
        .as_ref()
        .and_then(|p| netidx_admin::admin_server_config::load_for_recovery(p).ok())
        .and_then(|c| c.roles.ca.map(|role| role.dir));
    let ca_dir = match configured {
        Some(dir) if dir.is_dir() => dir,
        _ => {
            let conventional = config_dir.join("ca");
            if !conventional.is_dir() {
                return None;
            }
            conventional
        }
    };
    Some(LocalCaV {
        ca_dir: ca_dir.display().to_string(),
        cfg: cfg.map(|p| p.display().to_string()),
    })
}

#[cfg(not(unix))]
fn local_ca(_config_dir: &Path) -> Option<LocalCaV> {
    None
}

fn install(
    record: InstallRecord,
    scope: ServiceScope,
    config_dir: PathBuf,
    renewable: bool,
) -> InstallV {
    InstallV {
        service: probe_service(&record),
        local_ca: local_ca(&config_dir),
        role: record.role.into(),
        base: record.base,
        auth: record.auth,
        admin_domain: record
            .admin_domain
            .map(|n| AdminDomainRefV { domain: n.domain, fingerprint: n.ca_fingerprint }),
        admin_servers: record.admin_servers.iter().map(|a| a.to_string()).collect(),
        created_unix: record.created_unix,
        scope: scope.into(),
        config_dir: config_dir.display().to_string(),
        renewable,
    }
}

/// Every install recorded on this machine, user scope first. What the
/// renew action would operate on — the identities renewd discovers
/// across this host's configs — is one scan, the same for every
/// install.
fn detect() -> Vec<InstallV> {
    let renewable = !renewd::host_identities(None).is_empty();
    let mut out = Vec::new();
    let user_path = paths::user_install_record().ok();
    if let Ok(Some(record)) = InstallRecord::load_default() {
        let dir = paths::user_config_root().unwrap_or_default();
        out.push(install(record, ServiceScope::User, dir, renewable));
    }
    let sys_path = paths::system_install_record();
    if sys_path.exists()
        && user_path.as_deref() != Some(sys_path.as_path())
        && let Ok(record) = InstallRecord::load(&sys_path)
    {
        out.push(install(
            record,
            ServiceScope::System,
            paths::system_config_root(),
            renewable,
        ));
    }
    out
}

#[derive(Debug, Default)]
pub(crate) struct InstallsEv;

impl EvalCachedAsync for InstallsEv {
    type Args = ();

    const NAME: &str = "netidx_admin_installs";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        cached.0.first()?.as_ref()?;
        Some(())
    }

    fn eval(_: Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            match tokio::task::spawn_blocking(detect).await {
                Ok(installs) => Value::Array(ValArray::from_iter_exact(
                    installs.into_iter().map(Value::from),
                )),
                Err(e) => errf!("Admin", "detecting installs: {e}"),
            }
        }
    }
}

pub(crate) type Installs = CachedArgsAsync<InstallsEv>;

// ── the admin-domain sync check ──────────────────────────────────

#[derive(Debug, Clone, IntoValue)]
enum ChangeV {
    Add(String),
    Del(String),
    Reauth(String),
}

impl From<Change> for ChangeV {
    fn from(c: Change) -> Self {
        match c {
            Change::Add(s) => ChangeV::Add(s.to_string()),
            Change::Del(s) => ChangeV::Del(s.to_string()),
            Change::Reauth(s) => ChangeV::Reauth(s.to_string()),
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
enum SyncV {
    InSync,
    OutOfSync(Vec<ChangeV>),
}

fn role_arg(v: Option<&Value>) -> Option<InstallRole> {
    InstallRoleV::from_value(v?.clone()).ok().map(InstallRole::from)
}

#[derive(Debug, Default)]
pub(crate) struct SyncCheckEv;

impl EvalCachedAsync for SyncCheckEv {
    type Args = (PathBuf, InstallRole);

    const NAME: &str = "netidx_admin_sync_check";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        let root = PathBuf::from(cached.get::<String>(0)?);
        Some((root, role_arg(cached.0.get(1)?.as_ref())?))
    }

    fn eval((root, role): Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            match sync::plan_for(role, Some(&root)).await {
                Ok(plan) if plan.edits.is_empty() => SyncV::InSync.into(),
                Ok(plan) => SyncV::OutOfSync(
                    plan.edits.changes().into_iter().map(ChangeV::from).collect(),
                )
                .into(),
                Err(e) => admin_err(e),
            }
        }
    }
}

pub(crate) type SyncCheck = CachedArgsAsync<SyncCheckEv>;

// ── the CA credential probe ──────────────────────────────────────

#[derive(Debug, Clone, IntoValue)]
struct CaCredentialsV {
    auto_approve_present: bool,
    auto_approve_wired: bool,
    recovery_present: bool,
    external_signed: bool,
    external_installed: bool,
}

fn opt_path(v: Option<&Value>) -> Result<Option<PathBuf>> {
    Ok(opt_string(v)?.map(PathBuf::from))
}

#[derive(Debug, Default)]
pub(crate) struct CaCredentialsEv;

impl EvalCachedAsync for CaCredentialsEv {
    type Args = (Option<PathBuf>, PathBuf);

    const NAME: &str = "netidx_admin_ca_credentials";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        let cfg = opt_path(cached.0.first()?.as_ref()).ok()?;
        Some((cfg, PathBuf::from(cached.get::<String>(1)?)))
    }

    #[cfg(unix)]
    fn eval((cfg, ca_dir): Self::Args) -> impl Future<Output = Value> + Send {
        use netidx_admin::ops::slots;
        async move {
            let probed = async {
                let access = slots::CaAccess::open(&ca_dir, cfg).await?;
                slots::local_ca_status(&access, &ca_dir).await
            }
            .await;
            match probed {
                Ok(s) => CaCredentialsV {
                    auto_approve_present: s.auto_approve.slot_present,
                    auto_approve_wired: s.auto_approve.wired_in_config,
                    recovery_present: s.recovery.slot_present,
                    external_signed: s.external.externally_signed,
                    external_installed: s.external.cert_installed,
                }
                .into(),
                Err(e) => admin_err(e),
            }
        }
    }

    #[cfg(not(unix))]
    fn eval(_: Self::Args) -> impl Future<Output = Value> + Send {
        async move { errf!("Unsupported", "a local CA is unix-only") }
    }
}

pub(crate) type CaCredentials = CachedArgsAsync<CaCredentialsEv>;

// ── the activation supervisor's unit directory ───────────────────

#[derive(Debug, Default)]
pub(crate) struct UnitsDirEv;

impl EvalCachedAsync for UnitsDirEv {
    type Args = ();

    const NAME: &str = "netidx_admin_units_dir";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        cached.0.first()?.as_ref()?;
        Some(())
    }

    fn eval(_: Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            match netidx_activation::runtime::default_units_dir() {
                Some(p) => Value::from(p.display().to_string()),
                None => Value::Null,
            }
        }
    }
}

pub(crate) type UnitsDir = CachedArgsAsync<UnitsDirEv>;

// ── this host's own resolver base ────────────────────────────────

#[derive(Debug, Default)]
pub(crate) struct LocalResolverBaseEv;

impl EvalCachedAsync for LocalResolverBaseEv {
    type Args = ();

    const NAME: &str = "netidx_admin_local_resolver_base";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        cached.0.first()?.as_ref()?;
        Some(())
    }

    fn eval(_: Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            match netidx_admin::resolver::ResolverConfig::load_default() {
                Ok(c) => Value::from(c.base_path()),
                Err(_) => Value::Null,
            }
        }
    }
}

pub(crate) type LocalResolverBase = CachedArgsAsync<LocalResolverBaseEv>;

// ── this host's activation units ─────────────────────────────────

#[derive(Debug, Clone, FromValue, IntoValue)]
enum UnitTriggerV {
    OnStart,
    OnAccess(Vec<String>),
}

#[derive(Debug, Clone, FromValue, IntoValue)]
enum UnitRestartV {
    No,
    Yes,
    RateLimited(f64),
}

#[derive(Debug, Clone, FromValue, IntoValue)]
struct EnvVarV {
    name: String,
    value: String,
}

#[derive(Debug, Clone, FromValue, IntoValue)]
enum UnitEnvironmentV {
    Inherit(Vec<EnvVarV>),
    Replace(Vec<EnvVarV>),
}

/// A unit's definition as the form edits it — every field of the
/// on-disk unit, none of them a rendered string.
#[derive(Debug, Clone, FromValue, IntoValue)]
struct UnitDefV {
    exe: String,
    args: Vec<String>,
    trigger: UnitTriggerV,
    restart: UnitRestartV,
    working_directory: Option<String>,
    uid: Option<u32>,
    gid: Option<u32>,
    stdin: Option<String>,
    stdout: Option<String>,
    stderr: Option<String>,
    environment: UnitEnvironmentV,
}

fn env_vars(m: BTreeMap<String, String>) -> Vec<EnvVarV> {
    m.into_iter().map(|(name, value)| EnvVarV { name, value }).collect()
}

fn env_map(vs: Vec<EnvVarV>) -> BTreeMap<String, String> {
    vs.into_iter().map(|v| (v.name, v.value)).collect()
}

impl From<Unit> for UnitDefV {
    fn from(u: Unit) -> Self {
        let p = u.process;
        UnitDefV {
            exe: p.exe,
            args: p.args,
            trigger: match u.trigger {
                UnitTrigger::OnStart => UnitTriggerV::OnStart,
                UnitTrigger::OnAccess(paths) => {
                    UnitTriggerV::OnAccess(paths.iter().map(|p| p.to_string()).collect())
                }
            },
            restart: match p.restart {
                Restart::No => UnitRestartV::No,
                Restart::Yes => UnitRestartV::Yes,
                Restart::RateLimited(s) => UnitRestartV::RateLimited(s),
            },
            working_directory: p.working_directory.map(|d| d.display().to_string()),
            uid: p.uid,
            gid: p.gid,
            stdin: p.stdin.map(|d| d.display().to_string()),
            stdout: p.stdout.map(|d| d.display().to_string()),
            stderr: p.stderr.map(|d| d.display().to_string()),
            environment: match p.environment {
                Environment::Inherit(m) => UnitEnvironmentV::Inherit(env_vars(m)),
                Environment::Replace(m) => UnitEnvironmentV::Replace(env_vars(m)),
            },
        }
    }
}

impl TryFrom<UnitDefV> for Unit {
    type Error = anyhow::Error;

    fn try_from(d: UnitDefV) -> Result<Unit> {
        let trigger = match d.trigger {
            UnitTriggerV::OnStart => UnitTrigger::OnStart,
            UnitTriggerV::OnAccess(paths) => UnitTrigger::OnAccess(
                paths.into_iter().map(netidx::path::Path::from).collect(),
            ),
        };
        let restart = match d.restart {
            UnitRestartV::No => Restart::No,
            UnitRestartV::Yes => Restart::Yes,
            UnitRestartV::RateLimited(s) => {
                if !s.is_finite() || s <= 0.0 {
                    bail!("rate-limit seconds must be finite and positive, got {s}");
                }
                Restart::RateLimited(s)
            }
        };
        if d.exe.trim().is_empty() {
            bail!("a unit needs an executable");
        }
        let process = ProcessCfg {
            exe: d.exe,
            args: d.args,
            working_directory: d.working_directory.map(PathBuf::from),
            uid: d.uid,
            gid: d.gid,
            restart,
            stdin: d.stdin.map(PathBuf::from),
            stdout: d.stdout.map(PathBuf::from),
            stderr: d.stderr.map(PathBuf::from),
            environment: match d.environment {
                UnitEnvironmentV::Inherit(vs) => Environment::Inherit(env_map(vs)),
                UnitEnvironmentV::Replace(vs) => Environment::Replace(env_map(vs)),
            },
        };
        Ok(Unit { trigger, process })
    }
}

#[derive(Debug, Clone, IntoValue)]
enum LocalUnitStateV {
    NotStarted,
    Running(Option<u32>),
    Stopped,
    Died,
}

impl From<UnitState> for LocalUnitStateV {
    fn from(s: UnitState) -> Self {
        match s {
            UnitState::NotStarted => LocalUnitStateV::NotStarted,
            UnitState::Running { pid } => LocalUnitStateV::Running(pid),
            UnitState::Stopped => LocalUnitStateV::Stopped,
            UnitState::Died => LocalUnitStateV::Died,
        }
    }
}

/// One unit on this host: what is on disk and what the supervisor
/// reports, either side absent when only the other knows it.
#[derive(Debug, Clone, IntoValue)]
struct LocalUnitV {
    name: String,
    state: Option<LocalUnitStateV>,
    definition: Option<UnitDefV>,
}

async fn local_units(units_dir: &Path) -> Result<Value> {
    let rows = activation::list_with_state(units_dir).await?;
    Ok(Value::Array(ValArray::from_iter_exact(rows.into_iter().map(|u| {
        Value::from(LocalUnitV {
            name: u.name,
            state: u.state.map(LocalUnitStateV::from),
            definition: u.unit.map(UnitDefV::from),
        })
    }))))
}

#[derive(Debug, Default)]
pub(crate) struct ListUnitsEv;

impl EvalCachedAsync for ListUnitsEv {
    type Args = PathBuf;

    const NAME: &str = "netidx_admin_list_units";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        let dir = PathBuf::from(cached.get::<String>(0)?);
        cached.0.get(1)?.as_ref()?;
        Some(dir)
    }

    fn eval(dir: Self::Args) -> impl Future<Output = Value> + Send {
        async move { local_units(&dir).await.unwrap_or_else(admin_err) }
    }
}

pub(crate) type ListUnits = CachedArgsAsync<ListUnitsEv>;

#[derive(Debug, Clone, FromValue)]
enum LocalOpV {
    Start,
    Stop,
    Restart,
    Status,
}

#[derive(Debug, Default)]
pub(crate) struct ControlUnitsEv;

impl EvalCachedAsync for ControlUnitsEv {
    type Args = (PathBuf, Option<String>, ControlOp);

    const NAME: &str = "netidx_admin_control_units";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        let dir = PathBuf::from(cached.get::<String>(0)?);
        let unit = opt_string(cached.0.get(1)?.as_ref()).ok()?;
        let op = match LocalOpV::from_value(cached.0.get(2)?.clone()?).ok()? {
            LocalOpV::Start => ControlOp::Start,
            LocalOpV::Stop => ControlOp::Stop,
            LocalOpV::Restart => ControlOp::Restart,
            LocalOpV::Status => ControlOp::Status,
        };
        Some((dir, unit, op))
    }

    fn eval((dir, unit, op): Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            match activation::control_local(&dir, op, unit.into_iter().collect()).await {
                Ok(states) => {
                    Value::Array(ValArray::from_iter_exact(states.into_iter().map(|u| {
                        Value::from(LocalUnitV {
                            name: u.unit,
                            state: Some(u.state.into()),
                            definition: None,
                        })
                    })))
                }
                Err(e) => admin_err(e),
            }
        }
    }
}

pub(crate) type ControlUnits = CachedArgsAsync<ControlUnitsEv>;

#[derive(Debug, Default)]
pub(crate) struct InstallUnitEv;

impl EvalCachedAsync for InstallUnitEv {
    type Args = (PathBuf, String, Result<Unit>);

    const NAME: &str = "netidx_admin_install_unit";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        let dir = PathBuf::from(cached.get::<String>(0)?);
        let name = cached.get::<String>(1)?;
        let def = UnitDefV::from_value(cached.0.get(2)?.clone()?).ok()?;
        Some((dir, name, Unit::try_from(def)))
    }

    fn eval((dir, name, unit): Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            let installed = async {
                let unit = unit?;
                ActivationDir::open(Some(&dir))?.install(&name, &unit).await
            }
            .await;
            match installed {
                Ok(reloaded) => Value::Bool(reloaded),
                Err(e) => admin_err(e),
            }
        }
    }
}

pub(crate) type InstallUnit = CachedArgsAsync<InstallUnitEv>;

#[derive(Debug, Default)]
pub(crate) struct RemoveUnitEv;

impl EvalCachedAsync for RemoveUnitEv {
    type Args = (PathBuf, String);

    const NAME: &str = "netidx_admin_remove_unit";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        let dir = PathBuf::from(cached.get::<String>(0)?);
        Some((dir, cached.get::<String>(1)?))
    }

    fn eval((dir, name): Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            let removed =
                async { ActivationDir::open(Some(&dir))?.uninstall(&name).await }.await;
            match removed {
                Ok(reloaded) => Value::Bool(reloaded),
                Err(e) => admin_err(e),
            }
        }
    }
}

pub(crate) type RemoveUnit = CachedArgsAsync<RemoveUnitEv>;

#[derive(Debug, Default)]
pub(crate) struct UnitTemplateEv;

impl EvalCachedAsync for UnitTemplateEv {
    type Args = ();

    const NAME: &str = "netidx_admin_unit_template";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        cached.0.first()?.as_ref()?;
        Some(())
    }

    fn eval(_: Self::Args) -> impl Future<Output = Value> + Send {
        async move { UnitDefV::from(activation::template_unit()).into() }
    }
}

pub(crate) type UnitTemplate = CachedArgsAsync<UnitTemplateEv>;

// ── the local-ceremony builtin family ────────────────────────────

/// A ceremony over this host's own files and daemons: no target, no
/// pre-confirmed glyph. The trailing positional is the trigger.
pub(crate) trait LocalOp: Debug + Send + Sync + 'static {
    const NAME: &'static str;
    /// Build the ceremony's op from the argument values, every one
    /// present. An error becomes the mint site's error value.
    fn op(args: &[Option<Value>]) -> Result<BoxOp>;
}

#[derive(Debug)]
pub(crate) struct LocalCeremony<T: LocalOp> {
    trigger: Trigger,
    out: TagValue,
    ph: PhantomData<T>,
}

impl<R: Rt, E: UserEvent, T: LocalOp> BuiltIn<R, E> for LocalCeremony<T> {
    const EFFECT: Effect = Effect::Async;
    const NAME: &str = T::NAME;

    fn init<'a, 'b, 'c, 'd>(
        _ctx: &'a mut ExecCtx<R, E>,
        _typ: &'a FnType,
        _resolved: Option<&'d FnType>,
        _scope: &'b Scope,
        from: &'c [Node<R, E>],
        _top_id: ExprId,
    ) -> Result<Box<dyn Apply<R, E>>> {
        Ok(Box::new(LocalCeremony::<T> {
            trigger: Trigger::new(from),
            out: TagValue::phantom(),
            ph: PhantomData,
        }))
    }
}

impl<R: Rt, E: UserEvent, T: LocalOp> Apply<R, E> for LocalCeremony<T> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<R, E>,
        from: &mut [Node<R, E>],
        event: &mut Event<E>,
    ) -> &TagValue {
        let Some(args) = self.trigger.tick(ctx, from, event) else {
            return self.out.ride();
        };
        match T::op(args) {
            Ok(op) => {
                self.out.set(TagValue::fired(ceremony::start_ceremony(ctx, None, op)))
            }
            Err(e) => self.out.set(TagValue::fired(admin_err(e))),
        }
    }

    fn sleep(&mut self, _ctx: &mut ExecCtx<R, E>) {}

    fn reset_replay(&mut self, _ctx: &mut ExecCtx<R, E>) {}
}

fn string_arg(v: Option<&Value>, what: &str) -> Result<String> {
    match v {
        Some(Value::String(s)) => Ok(s.to_string()),
        Some(v) => bail!("{what}: expected a string, got {v}"),
        None => bail!("{what}: missing"),
    }
}

fn path_arg(v: Option<&Value>, what: &str) -> Result<PathBuf> {
    string_arg(v, what).map(PathBuf::from)
}

fn bool_arg(v: Option<&Value>, what: &str) -> Result<bool> {
    match v {
        Some(Value::Bool(b)) => Ok(*b),
        Some(v) => bail!("{what}: expected a bool, got {v}"),
        None => bail!("{what}: missing"),
    }
}

fn paths_value(ps: &[PathBuf]) -> Vec<String> {
    ps.iter().map(|p| p.display().to_string()).collect()
}

// ── renew ────────────────────────────────────────────────────────

#[derive(Debug, Clone, IntoValue)]
struct RenewFailureV {
    cert: String,
    why: String,
}

#[derive(Debug, Clone, IntoValue)]
struct RenewReportV {
    summary: Option<String>,
    renewed: Vec<String>,
    awaiting_approval: Vec<String>,
    failed: Vec<RenewFailureV>,
}

#[derive(Debug)]
pub(crate) struct RenewOp;

impl LocalOp for RenewOp {
    const NAME: &'static str = "netidx_admin_renew";

    fn op(_args: &[Option<Value>]) -> Result<BoxOp> {
        Ok(Box::new(|_ans| {
            Box::pin(async move {
                let report = renewd::run_once(renewd::RenewalConfig::default()).await;
                Ok(RenewReportV {
                    summary: report.summary().map(|s| s.to_string()),
                    renewed: paths_value(&report.renewed),
                    awaiting_approval: paths_value(&report.awaiting_approval),
                    failed: report
                        .failed
                        .iter()
                        .map(|(cert, why)| RenewFailureV {
                            cert: cert.display().to_string(),
                            why: why.to_string(),
                        })
                        .collect(),
                }
                .into())
            })
        }))
    }
}

pub(crate) type Renew = LocalCeremony<RenewOp>;

// ── update: reconcile with the admin domain and apply ────────────

#[derive(Debug, Clone, IntoValue)]
enum UpdateOutcomeV {
    UpToDate,
    Applied { changes: Vec<ChangeV>, warnings: Vec<String>, restart_hint: String },
}

#[derive(Debug)]
pub(crate) struct UpdateOp;

impl LocalOp for UpdateOp {
    const NAME: &'static str = "netidx_admin_update";

    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        let root = path_arg(args[0].as_ref(), "config_root")?;
        let role = role_arg(args[1].as_ref()).context("role")?;
        Ok(Box::new(move |_ans| {
            Box::pin(async move {
                let plan = sync::plan_for(role, Some(&root)).await?;
                if plan.edits.is_empty() {
                    return Ok(UpdateOutcomeV::UpToDate.into());
                }
                let changes =
                    plan.edits.changes().into_iter().map(ChangeV::from).collect();
                let warnings =
                    plan.edits.warnings.iter().map(|w| w.to_string()).collect();
                let restart_hint = plan.restart_hint().to_string();
                plan.apply_locked().await?;
                Ok(UpdateOutcomeV::Applied { changes, warnings, restart_hint }.into())
            })
        }))
    }
}

pub(crate) type Update = LocalCeremony<UpdateOp>;

// ── backup ───────────────────────────────────────────────────────

#[derive(Debug, Clone, IntoValue)]
struct BackupOutcomeV {
    target: String,
    role: InstallRoleV,
    components: Vec<String>,
    files: u64,
    bytes: u64,
    identities_to_reenroll: u64,
    manifest_sha256: String,
}

fn component_name(c: &Component) -> &'static str {
    match c {
        Component::Ca => "CA",
        Component::Workstation => "workstation",
        Component::Resolver => "resolver",
        Component::Publisher => "publisher",
        Component::IdMap => "id-map",
    }
}

#[derive(Debug)]
pub(crate) struct BackupOp;

impl LocalOp for BackupOp {
    const NAME: &'static str = "netidx_admin_backup";

    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        let root = path_arg(args[0].as_ref(), "config_root")?;
        let target = opt_path(args[1].as_ref())?;
        let scope = ScopeV::from_value(args[2].clone().context("scope")?)?;
        let scope = match ServiceScope::from(scope) {
            ServiceScope::User => BundleScope::User,
            ServiceScope::System => BundleScope::System,
        };
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let out = bundle::backup(
                    ans,
                    bundle::BackupInput {
                        target,
                        scope: Some(scope),
                        config_dir: Some(root),
                        ..Default::default()
                    },
                )
                .await?;
                Ok(BackupOutcomeV {
                    target: out.target.display().to_string(),
                    role: out.role.into(),
                    components: out
                        .components
                        .iter()
                        .map(|c| component_name(c).into())
                        .collect(),
                    files: out.files,
                    bytes: out.bytes,
                    identities_to_reenroll: out.identities_to_reenroll as u64,
                    manifest_sha256: out.manifest_sha256,
                }
                .into())
            })
        }))
    }
}

pub(crate) type Backup = LocalCeremony<BackupOp>;

// ── the local CA ops (unix only) ─────────────────────────────────

#[derive(Debug, Clone, IntoValue)]
enum AutorenewWiringV {
    Updated(String),
    NoCaConfig,
    Failed(String),
}

#[derive(Debug, Clone, IntoValue)]
enum AutoApproveOutcomeV {
    HotSwapped { warning: Option<String> },
    Offline { rotate: bool, keytab: String, wiring: AutorenewWiringV },
}

#[derive(Debug, Clone, IntoValue)]
enum RecoveryRotateOutcomeV {
    HotSwapped,
    Offline,
}

#[derive(Debug, Clone, IntoValue)]
enum ExternalInstallOutcomeV {
    FirstInstall { cfg_path: String, service_scope: Option<ScopeV> },
    OfflineCa,
    Renewal,
    HotRenewed { ca_fingerprint: String },
}

#[cfg(not(unix))]
fn unix_only(what: &str) -> Result<BoxOp> {
    bail!("{what} drives the local CA's control socket, which is unix-only")
}

#[derive(Debug)]
pub(crate) struct AutoApproveOp;

impl LocalOp for AutoApproveOp {
    const NAME: &'static str = "netidx_admin_auto_approve";

    #[cfg(unix)]
    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        use netidx_admin::ops::slots::{
            AutoApproveOutcome, AutorenewWiring, CaAccess, auto_approve,
        };
        let rotate = bool_arg(args[0].as_ref(), "rotate")?;
        let cfg = opt_path(args[1].as_ref())?;
        let ca_dir = path_arg(args[2].as_ref(), "ca_dir")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let access = CaAccess::open(&ca_dir, cfg).await?;
                let out = auto_approve(ans, &access, ca_dir, rotate, false).await?;
                Ok(match out {
                    AutoApproveOutcome::HotSwapped { warning } => {
                        AutoApproveOutcomeV::HotSwapped { warning }
                    }
                    AutoApproveOutcome::Offline { rotate, keytab, wiring } => {
                        AutoApproveOutcomeV::Offline {
                            rotate,
                            keytab: keytab.display().to_string(),
                            wiring: match wiring {
                                AutorenewWiring::Updated(p) => {
                                    AutorenewWiringV::Updated(p.display().to_string())
                                }
                                AutorenewWiring::NoCaConfig => {
                                    AutorenewWiringV::NoCaConfig
                                }
                                AutorenewWiring::Failed(e) => AutorenewWiringV::Failed(e),
                            },
                        }
                    }
                }
                .into())
            })
        }))
    }

    #[cfg(not(unix))]
    fn op(_args: &[Option<Value>]) -> Result<BoxOp> {
        unix_only("auto-approve")
    }
}

pub(crate) type AutoApprove = LocalCeremony<AutoApproveOp>;

#[derive(Debug)]
pub(crate) struct RecoveryRotateOp;

impl LocalOp for RecoveryRotateOp {
    const NAME: &'static str = "netidx_admin_recovery_rotate";

    #[cfg(unix)]
    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        use netidx_admin::ops::slots::{
            CaAccess, RecoveryRotateOutcome, recovery_rotate,
        };
        let cfg = opt_path(args[0].as_ref())?;
        let ca_dir = path_arg(args[1].as_ref(), "ca_dir")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let access = CaAccess::open(&ca_dir, cfg).await?;
                Ok(match recovery_rotate(ans, &access, ca_dir).await? {
                    RecoveryRotateOutcome::HotSwapped => {
                        RecoveryRotateOutcomeV::HotSwapped
                    }
                    RecoveryRotateOutcome::Offline { .. } => {
                        RecoveryRotateOutcomeV::Offline
                    }
                }
                .into())
            })
        }))
    }

    #[cfg(not(unix))]
    fn op(_args: &[Option<Value>]) -> Result<BoxOp> {
        unix_only("rotating the recovery password")
    }
}

pub(crate) type RecoveryRotate = LocalCeremony<RecoveryRotateOp>;

#[derive(Debug)]
pub(crate) struct ExternalCsrOp;

impl LocalOp for ExternalCsrOp {
    const NAME: &'static str = "netidx_admin_external_csr";

    #[cfg(unix)]
    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        use netidx_admin::ops::slots::{CaAccess, external_csr};
        let ca_dir = path_arg(args[0].as_ref(), "ca_dir")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let access = CaAccess::open(&ca_dir, None).await?;
                let csr = external_csr(ans, &access, ca_dir).await?;
                let csr = std::fs::canonicalize(&csr).unwrap_or(csr);
                Ok(Value::from(csr.display().to_string()))
            })
        }))
    }

    #[cfg(not(unix))]
    fn op(_args: &[Option<Value>]) -> Result<BoxOp> {
        unix_only("emitting a CSR")
    }
}

pub(crate) type ExternalCsr = LocalCeremony<ExternalCsrOp>;

#[derive(Debug)]
pub(crate) struct ExternalInstallOp;

impl LocalOp for ExternalInstallOp {
    const NAME: &'static str = "netidx_admin_external_install";

    #[cfg(unix)]
    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        use netidx_admin::ops::slots::{
            CaAccess, ExternalInstallOutcome, external_install,
        };
        let signed = opt_path(args[0].as_ref())?;
        let root = opt_path(args[1].as_ref())?;
        let ca_dir = path_arg(args[2].as_ref(), "ca_dir")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let signed = match signed {
                    Some(p) => p,
                    None => PathBuf::from(
                        ans.text(Field::SignedCert, None, None, true)
                            .await?
                            .context("a signed certificate path is required")?,
                    ),
                };
                let root = match root {
                    Some(p) => Some(p),
                    None => ans
                        .text(Field::ExternalRoot, None, None, false)
                        .await?
                        .filter(|s| !s.trim().is_empty())
                        .map(PathBuf::from),
                };
                let access = CaAccess::open(&ca_dir, None).await?;
                let out =
                    external_install(ans, &access, ca_dir, &signed, root.as_deref())
                        .await?;
                Ok(match out {
                    ExternalInstallOutcome::FirstInstall { need, cfg_path } => {
                        ExternalInstallOutcomeV::FirstInstall {
                            cfg_path: cfg_path.display().to_string(),
                            service_scope: need.scope().map(ScopeV::from),
                        }
                    }
                    ExternalInstallOutcome::OfflineCa => {
                        ExternalInstallOutcomeV::OfflineCa
                    }
                    ExternalInstallOutcome::Renewal => ExternalInstallOutcomeV::Renewal,
                    ExternalInstallOutcome::HotRenewed { ca_fingerprint } => {
                        ExternalInstallOutcomeV::HotRenewed { ca_fingerprint }
                    }
                }
                .into())
            })
        }))
    }

    #[cfg(not(unix))]
    fn op(_args: &[Option<Value>]) -> Result<BoxOp> {
        unix_only("installing a signed certificate")
    }
}

pub(crate) type ExternalInstall = LocalCeremony<ExternalInstallOp>;
