//! The ceremonies that set a host up or tear it down: install a role,
//! join an admin domain, attach a resolver under a parent, restore a
//! backup, uninstall. Every one drives the engine's plan through the
//! ceremony bridge; the OS-service step a role needs afterwards is
//! reported, and registered here only at user scope — the system-scope
//! step needs root and the terminal (the privileged handoff).

use crate::{
    admin_err,
    ceremony::BoxOp,
    local::{
        InstallRoleV, LocalCeremony, LocalOp, ScopeV, bool_arg, opt_path, path_arg,
        role_arg, string_arg,
    },
    ops::opt_string,
};
use anyhow::{Context, Result, bail};
use arcstr::ArcStr;
use graphix_package_core::{CachedArgsAsync, CachedVals, EvalCachedAsync};
use netidx_admin::{
    config_lock::ConfigDirLock,
    ops::delegation::{self, AddParentCompletion},
    paths,
    plan::{
        bundle::{self, RestoreInput, RestoreOutcome},
        delegation::DelegationSelection,
        install::{
            InstallCommon, InstallMode,
            publisher::{PublisherInput, run_publisher},
            resolver::{ResolverInput, run_resolver},
            workstation::{
                WorkstationInput, WorkstationJoinInput, run_workstation,
                run_workstation_join,
            },
        },
        resolve_admin_server_addr,
    },
    provenance::InstallRole,
    service::{self, ServiceParams, ServiceScope},
    uninstall::{self, Covers, KeepReason, UninstallInput, UninstallReport},
};
use netidx_derive::IntoValue;
use netidx_value::{Abstract, FromValue, Value, abstract_type::AbstractWrapper};
use parking_lot::Mutex;
use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, LazyLock},
};

// ── install ──────────────────────────────────────────────────────

#[derive(Debug, Clone, IntoValue)]
enum InstallOutcomeV {
    Preview { service: Option<ScopeV> },
    Installed { service: Option<ScopeV> },
    CaAwaitingSignature { csr: String },
}

fn resolver_input(common: InstallCommon) -> ResolverInput {
    ResolverInput {
        read_gate: None,
        auth: None,
        spn: None,
        tls_name: None,
        listen: None,
        bind: None,
        base: "/".to_string(),
        perms_seed: None,
        perms_path: None,
        no_perms: false,
        explicit_parent: None,
        resolver_config_path: None,
        units_dir: None,
        netidx_binary: None,
        no_id_map: false,
        id_map_mode: None,
        no_admin_server: false,
        with_admin_server: false,
        insecure_no_tpm: false,
        parent_admin_server: None,
        admin_server: None,
        delegate_subtree: None,
        key_protection: None,
        id_map_socket: None,
        id_map_path: None,
        no_client: false,
        client_config_path: None,
        common,
    }
}

fn publisher_input(common: InstallCommon) -> PublisherInput {
    PublisherInput {
        addrs: Vec::new(),
        auth: None,
        admin_server: None,
        spn: None,
        socket: None,
        tls_server_name: None,
        default_auth: None,
        base: "/".to_string(),
        config_path: None,
        bind: None,
        units_dir: None,
        key_protection: None,
        common,
    }
}

async fn install_mode(dry_run: bool) -> Result<InstallMode> {
    if dry_run { Ok(InstallMode::DryRun) } else { InstallMode::apply_user_config().await }
}

#[derive(Debug)]
pub(crate) struct InstallOp;

impl LocalOp for InstallOp {
    const NAME: &'static str = "netidx_admin_install";

    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        let role = role_arg(args[0].as_ref()).context("role")?;
        let dry_run = bool_arg(args[1].as_ref(), "dry_run")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let common = InstallCommon {
                    mode: install_mode(dry_run).await?,
                    force: false,
                    no_units: false,
                    with_service: false,
                    no_service: false,
                };
                let service = match role {
                    #[cfg(unix)]
                    InstallRole::Ca => {
                        use netidx_admin::plan::install::ca::{CaInput, run_ca};
                        let out = run_ca(ans, CaInput::defaults(common)).await?;
                        if let Some(csr) = out.pending_external {
                            return Ok(InstallOutcomeV::CaAwaitingSignature {
                                csr: csr.display().to_string(),
                            }
                            .into());
                        }
                        out.service
                    }
                    #[cfg(not(unix))]
                    InstallRole::Ca => bail!("the CA role is supported only on unix"),
                    InstallRole::Resolver => {
                        run_resolver(ans, resolver_input(common)).await?
                    }
                    InstallRole::Publisher => {
                        run_publisher(ans, publisher_input(common)).await?
                    }
                    InstallRole::Workstation => {
                        run_workstation(ans, WorkstationInput::defaults(common)).await?
                    }
                };
                let service = service.map(ScopeV::from);
                Ok(if dry_run {
                    InstallOutcomeV::Preview { service }
                } else {
                    InstallOutcomeV::Installed { service }
                }
                .into())
            })
        }))
    }
}

pub(crate) type Install = LocalCeremony<InstallOp>;

// ── join ─────────────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct JoinOp;

impl LocalOp for JoinOp {
    const NAME: &'static str = "netidx_admin_join";

    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        let dry_run = bool_arg(args[0].as_ref(), "dry_run")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let input = WorkstationJoinInput {
                    mode: install_mode(dry_run).await?,
                    key_protection: None,
                    admin_server: None,
                };
                run_workstation_join(ans, input).await?;
                Ok(Value::Null)
            })
        }))
    }
}

pub(crate) type Join = LocalCeremony<JoinOp>;

// ── a parent for this resolver ───────────────────────────────────

#[derive(Debug, Clone, IntoValue)]
struct ParentCandidateV {
    admin: String,
    resolver: String,
    base: String,
}

#[derive(Debug, Default)]
pub(crate) struct ParentCandidatesEv;

impl EvalCachedAsync for ParentCandidatesEv {
    type Args = PathBuf;

    const NAME: &str = "netidx_admin_parent_candidates";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        let root = PathBuf::from(cached.get::<String>(0)?);
        cached.0.get(1)?.as_ref()?;
        Some(root)
    }

    fn eval(root: Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            match delegation::parent_candidates(&root).await {
                Ok(cs) => crate::rows_value::<_, ParentCandidateV>(
                    cs.into_iter()
                        .map(|c| ParentCandidateV {
                            admin: c.admin.to_string(),
                            resolver: c.resolver.addr.to_string(),
                            base: c.base,
                        })
                        .collect(),
                ),
                Err(e) => admin_err(e),
            }
        }
    }
}

pub(crate) type ParentCandidates = CachedArgsAsync<ParentCandidatesEv>;

#[derive(Debug, Clone, IntoValue)]
struct AddParentOutcomeV {
    proposed_path: String,
}

#[derive(Debug)]
pub(crate) struct AddParentOp;

impl LocalOp for AddParentOp {
    const NAME: &'static str = "netidx_admin_add_parent";

    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        let root = path_arg(args[0].as_ref(), "config_root")?;
        let parent = string_arg(args[1].as_ref(), "parent")?;
        let resolvers = match args[2].as_ref() {
            None | Some(Value::Null) => None,
            Some(v) => Some(Vec::<String>::from_value(v.clone()).context("resolvers")?),
        };
        let path = string_arg(args[3].as_ref(), "path")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let rpath = paths::discover_resolver_config()?;
                let parent = resolve_admin_server_addr(&parent)?;
                let selection = match resolvers {
                    None => None,
                    Some(rs) => Some(DelegationSelection {
                        parent_resolvers: rs
                            .iter()
                            .map(|s| {
                                s.parse::<SocketAddr>()
                                    .with_context(|| format!("resolver address {s:?}"))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    }),
                };
                let out = match delegation::prepare_add_parent(
                    ans, &rpath, parent, &path, selection,
                )
                .await?
                {
                    AddParentCompletion::Complete(out) => out,
                    AddParentCompletion::LocalWrite(pending) => {
                        let lock = ConfigDirLock::acquire_async(&root).await?;
                        pending.apply(&lock, ans)?
                    }
                };
                Ok(AddParentOutcomeV { proposed_path: out.proposed_path }.into())
            })
        }))
    }
}

pub(crate) type AddParent = LocalCeremony<AddParentOp>;

// ── restore, in two steps around the service a recovered CA needs ─

#[derive(Debug, Clone, IntoValue)]
struct RestoreOutcomeV {
    role: InstallRoleV,
    service: Option<ScopeV>,
    reconciled: Option<String>,
}

impl From<RestoreOutcome> for RestoreOutcomeV {
    fn from(o: RestoreOutcome) -> Self {
        RestoreOutcomeV {
            role: o.role.into(),
            service: o.service_needed.map(ScopeV::from),
            reconciled: o.reconciled.map(|id| id.to_string()),
        }
    }
}

/// A staged restore parked while the operator registers the CA's
/// service. Taken by `restore_finish`.
#[derive(Clone)]
pub(crate) struct StagedValue(Arc<Mutex<Option<bundle::Staged>>>);

impl fmt::Debug for StagedValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Staged({:p})", Arc::as_ptr(&self.0))
    }
}

impl PartialEq for StagedValue {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for StagedValue {}

impl PartialOrd for StagedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StagedValue {
    fn cmp(&self, other: &Self) -> Ordering {
        Arc::as_ptr(&self.0).cmp(&Arc::as_ptr(&other.0))
    }
}

impl Hash for StagedValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state)
    }
}

graphix_package_core::impl_no_pack!(StagedValue);

static STAGED_WRAPPER: LazyLock<AbstractWrapper<StagedValue>> = LazyLock::new(|| {
    let id = uuid::Uuid::from_bytes([
        0x2b, 0x7e, 0x9c, 0x41, 0x5d, 0x0a, 0x4f, 0x63, 0xb8, 0x1c, 0xe4, 0x27, 0x93,
        0x5a, 0x70, 0x0e,
    ]);
    Abstract::register::<StagedValue>(id).expect("failed to register StagedValue")
});

#[derive(Debug, Clone, IntoValue)]
struct ServiceIntentV {
    name: String,
    for_user: Option<String>,
}

fn restore_staged_value(
    scope: ServiceScope,
    service: ServiceIntentV,
    staged: bundle::Staged,
) -> Value {
    let staged = STAGED_WRAPPER.wrap(StagedValue(Arc::new(Mutex::new(Some(staged)))));
    let payload: Value = [
        (ArcStr::from("scope"), Value::from(ScopeV::from(scope))),
        (ArcStr::from("service"), Value::from(service)),
        (ArcStr::from("staged"), staged),
    ]
    .into_iter()
    .collect::<Vec<_>>()
    .into();
    Value::Array(netidx_value::ValArray::from_iter_exact(
        [Value::from("ServiceThenFinish"), payload].into_iter(),
    ))
}

#[derive(Debug)]
pub(crate) struct RestoreStageOp;

impl LocalOp for RestoreStageOp {
    const NAME: &'static str = "netidx_admin_restore_stage";

    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        let bundle_dir = opt_path(args[0].as_ref())?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let input = RestoreInput { bundle: bundle_dir, ..Default::default() };
                let staged = bundle::restore_stage(ans, &input).await?;
                match staged.next {
                    bundle::Next::Finish => {
                        let out = bundle::restore_finish(ans, staged).await?;
                        Ok(Value::Array(netidx_value::ValArray::from_iter_exact(
                            [Value::from("Finish"), RestoreOutcomeV::from(out).into()]
                                .into_iter(),
                        )))
                    }
                    bundle::Next::ServiceThenFinish(scope) => {
                        let (name, for_user) =
                            bundle::restored_service(&staged.manifest, None, None);
                        Ok(restore_staged_value(
                            scope,
                            ServiceIntentV { name, for_user },
                            staged,
                        ))
                    }
                }
            })
        }))
    }
}

pub(crate) type RestoreStage = LocalCeremony<RestoreStageOp>;

#[derive(Debug)]
pub(crate) struct RestoreFinishOp;

impl LocalOp for RestoreFinishOp {
    const NAME: &'static str = "netidx_admin_restore_finish";

    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        let staged = match args[0].as_ref() {
            Some(Value::Abstract(a)) => a.downcast_ref::<StagedValue>().cloned(),
            _ => None,
        }
        .context("a staged restore is required")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let staged = staged
                    .0
                    .lock()
                    .take()
                    .context("this staged restore was already finished")?;
                let out = bundle::restore_finish(ans, staged).await?;
                Ok(RestoreOutcomeV::from(out).into())
            })
        }))
    }
}

pub(crate) type RestoreFinish = LocalCeremony<RestoreFinishOp>;

// ── uninstall ────────────────────────────────────────────────────

#[derive(Debug, Clone, IntoValue)]
struct KeptV {
    path: String,
    why: String,
}

#[derive(Debug, Clone, IntoValue)]
struct UninstallReportV {
    service_was_installed: bool,
    removed: Vec<String>,
    kept: Vec<KeptV>,
    ca_destroyed: Option<String>,
}

impl From<UninstallReport> for UninstallReportV {
    fn from(r: UninstallReport) -> Self {
        UninstallReportV {
            service_was_installed: r.service_was_installed,
            removed: r.removed.iter().map(|p| p.display().to_string()).collect(),
            kept: r
                .kept
                .iter()
                .map(|(p, why)| KeptV {
                    path: p.display().to_string(),
                    why: match why {
                        KeepReason::CaPreserved => "the CA root key is kept".to_string(),
                    },
                })
                .collect(),
            ca_destroyed: r.ca_destroyed.map(|p| p.display().to_string()),
        }
    }
}

#[derive(Debug, Clone, IntoValue)]
enum CoversV {
    Everything,
    SystemServiceOnly,
}

#[derive(Debug, Clone, IntoValue)]
enum UninstallOutcomeV {
    Nothing,
    Removed(UninstallReportV),
    Escalate { scope: ScopeV, covers: CoversV, argv: Vec<String>, plan: UninstallReportV },
}

#[derive(Debug)]
pub(crate) struct UninstallOp;

impl LocalOp for UninstallOp {
    const NAME: &'static str = "netidx_admin_uninstall";

    fn op(args: &[Option<Value>]) -> Result<BoxOp> {
        let scope = ScopeV::from_value(args[0].clone().context("scope")?)?;
        let config_dir = path_arg(args[1].as_ref(), "config_dir")?;
        let remove_ca = bool_arg(args[2].as_ref(), "remove_ca")?;
        Ok(Box::new(move |ans| {
            Box::pin(async move {
                let input = UninstallInput {
                    scope: Some(scope.into()),
                    config_dir: Some(config_dir),
                    remove_ca,
                    ..Default::default()
                };
                Ok(match uninstall::plan(&input)? {
                    uninstall::Next::Nothing(_) => UninstallOutcomeV::Nothing,
                    uninstall::Next::Apply(prepared) => UninstallOutcomeV::Removed(
                        uninstall::apply(ans, prepared).await?.into(),
                    ),
                    uninstall::Next::Escalate(e) => UninstallOutcomeV::Escalate {
                        scope: e.scope.into(),
                        covers: match e.covers {
                            Covers::Everything => CoversV::Everything,
                            Covers::SystemServiceOnly => CoversV::SystemServiceOnly,
                        },
                        argv: uninstall::elevated_argv(&e),
                        plan: e.plan.into(),
                    },
                }
                .into())
            })
        }))
    }
}

pub(crate) type Uninstall = LocalCeremony<UninstallOp>;

// ── the OS service a role runs under ─────────────────────────────

fn current_exe() -> Result<PathBuf> {
    std::env::current_exe().context("could not determine the current netidx binary")
}

fn install_service(
    scope: ServiceScope,
    name: Option<String>,
    for_user: Option<String>,
) -> Result<String> {
    let name = name.unwrap_or_else(|| ServiceParams::DEFAULT_NAME.to_string());
    let for_user = match for_user {
        Some(u) => u,
        None => service::resolve_for_user(None)?,
    };
    let exe = current_exe()?;
    let activation_dir = netidx_activation::runtime::default_units_dir()
        .context("no default activation unit directory was found")?;
    match scope {
        ServiceScope::User => {
            let params = ServiceParams {
                scope: ServiceScope::User,
                for_user: Some(for_user),
                binary: exe,
                service_name: name,
                activation_dir: Some(activation_dir),
            };
            let installed = service::install(&params)?;
            Ok(format!("registered the user service ({})", installed.service_id))
        }
        ServiceScope::System => bail!(
            "registering the system service needs administrator privileges; run: sudo {} \
             admin host service install --scope system --for-user {for_user} \
             --service-name {name} --netidx-binary {} --activation-dir {}",
            exe.display(),
            exe.display(),
            activation_dir.display()
        ),
    }
}

#[derive(Debug, Default)]
pub(crate) struct InstallServiceEv;

impl EvalCachedAsync for InstallServiceEv {
    type Args = (ServiceScope, Option<String>, Option<String>);

    const NAME: &str = "netidx_admin_install_service";

    fn prepare_args(&mut self, cached: &CachedVals) -> Option<Self::Args> {
        let scope = ScopeV::from_value(cached.0.first()?.clone()?).ok()?;
        let name = opt_string(cached.0.get(1)?.as_ref()).ok()?;
        let for_user = opt_string(cached.0.get(2)?.as_ref()).ok()?;
        cached.0.get(3)?.as_ref()?;
        Some((scope.into(), name, for_user))
    }

    fn eval((scope, name, for_user): Self::Args) -> impl Future<Output = Value> + Send {
        async move {
            match tokio::task::spawn_blocking(move || {
                install_service(scope, name, for_user)
            })
            .await
            {
                Ok(Ok(line)) => Value::from(line),
                Ok(Err(e)) => admin_err(e),
                Err(e) => admin_err(e.into()),
            }
        }
    }
}

pub(crate) type InstallService = CachedArgsAsync<InstallServiceEv>;
