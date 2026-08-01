//! Actions the TUI runs against the library, and the [`Outcome`] it shows when
//! one finishes.
//!
//! Every action is an async library op driven through a
//! [`TuiAnswerer`](super::answer::TuiAnswerer): [`run_owned`] is the
//! self-contained future the UI loop polls. A step that needs the terminal —
//! registering a system OS service, or escalating for a teardown — is handed
//! back on the [`Outcome`] as a [`Privileged`] and performed by the loop,
//! which can suspend the terminal for the password prompt; see
//! [`super::privileged`].

use super::answer::TuiAnswerer;
#[cfg(unix)]
use anyhow::Context;
use anyhow::{Result, bail};
#[cfg(unix)]
use netidx_admin::{
    answer::Field,
    offline_ca,
    plan::install::ca::{CaInput, run_ca},
};
use netidx_admin::{
    answer::{Answerer, Progress, Stage},
    config_lock::ConfigDirLock,
    install_bundle, paths,
    plan::{
        bundle,
        install::{
            InstallCommon, InstallMode,
            publisher::{PublisherInput, run_publisher},
            resolver::{ResolverInput, run_resolver},
        },
    },
    provenance::InstallRole,
    renewd,
    service::ServiceScope,
    uninstall,
};
#[cfg(unix)]
use std::net::SocketAddr;
use std::path::PathBuf;

/// What the UI shows after an action completes.
pub(super) struct Outcome {
    pub(super) title: String,
    pub(super) lines: Vec<String>,
    /// Re-detect local installs after showing this (an install changed on-disk
    /// state).
    pub(super) refresh_local: bool,
    /// A privileged, terminal-owning follow-up the UI loop performs — it can
    /// suspend the terminal for a password prompt. `None` ⇒ nothing to do.
    pub(super) privileged: Option<Privileged>,
    /// Continue a multi-phase operation once the privileged step has
    /// completed: CA+resolver restore re-enrolls local identities against the
    /// newly started CA, and a user-scope teardown removes its configuration
    /// after the elevated step took the system service down.
    pub(super) after_privileged: Option<Action>,
    /// A result to apply to the Remote tab's state (connection / panel rows).
    pub(super) remote: Option<super::remote::RemoteUpdate>,
    /// A result to apply to the Local tab's Services surface (refreshed rows).
    pub(super) services: Option<super::services::ServicesUpdate>,
    /// Suppress the result overlay (used by silent panel re-queries).
    pub(super) quiet: bool,
}

/// A step that needs the terminal (a sudo / su password prompt), so the UI
/// loop performs it between an op finishing and its continuation running.
pub(super) enum Privileged {
    /// Register the OS service at this scope.
    InstallService(ServiceInstall),
    /// Run the engine's elevated teardown step.
    Uninstall(uninstall::Escalation),
}

#[derive(Debug, Clone)]
pub(super) struct ServiceInstall {
    pub(super) scope: ServiceScope,
    pub(super) name: String,
    pub(super) for_user: Option<String>,
}

impl ServiceInstall {
    fn defaults(scope: ServiceScope) -> Self {
        Self {
            scope,
            name: netidx_admin::service::ServiceParams::DEFAULT_NAME.to_string(),
            for_user: None,
        }
    }
}

impl Outcome {
    fn plain(
        title: impl Into<String>,
        lines: Vec<String>,
        refresh_local: bool,
    ) -> Outcome {
        Outcome {
            title: title.into(),
            lines,
            refresh_local,
            privileged: None,
            after_privileged: None,
            remote: None,
            services: None,
            quiet: false,
        }
    }

    /// A remote-tab result with a toast + a state update.
    pub(super) fn remote_toast(
        title: impl Into<String>,
        lines: Vec<String>,
        update: super::remote::RemoteUpdate,
    ) -> Outcome {
        Outcome {
            title: title.into(),
            lines,
            refresh_local: false,
            privileged: None,
            after_privileged: None,
            remote: Some(update),
            services: None,
            quiet: false,
        }
    }

    /// A silent Admin domain-tab discovery result: refresh the known-admin domain list and
    /// return to the landing screen with no overlay — the list is the result, and
    /// the landing screen re-polls it to show the verified admin domains.
    pub(super) fn remote_clusters(
        clusters: Vec<super::admin_domains::KnownAdminDomain>,
    ) -> Outcome {
        Outcome {
            title: String::new(),
            lines: Vec::new(),
            refresh_local: false,
            privileged: None,
            after_privileged: None,
            remote: Some(super::remote::RemoteUpdate::AdminDomains(clusters)),
            services: None,
            quiet: true,
        }
    }

    /// A silent remote-tab result: apply the panel rows, no overlay.
    pub(super) fn remote_rows(
        panel: super::remote::Panel,
        rows: Vec<super::remote::PanelRow>,
    ) -> Outcome {
        Outcome {
            title: String::new(),
            lines: Vec::new(),
            refresh_local: false,
            privileged: None,
            after_privileged: None,
            remote: Some(super::remote::RemoteUpdate::Rows { panel, rows }),
            services: None,
            quiet: true,
        }
    }

    /// An action result: a toast plus the refreshed panel rows.
    pub(super) fn remote_after(
        title: impl Into<String>,
        lines: Vec<String>,
        panel: super::remote::Panel,
        rows: Vec<super::remote::PanelRow>,
    ) -> Outcome {
        Outcome {
            title: title.into(),
            lines,
            refresh_local: false,
            privileged: None,
            after_privileged: None,
            remote: Some(super::remote::RemoteUpdate::Rows { panel, rows }),
            services: None,
            quiet: false,
        }
    }

    /// A silent result that opens a panel's resolver cluster picker (admin domain perms).
    pub(super) fn resolver_clusters(
        panel: super::remote::Panel,
        bases: Vec<String>,
    ) -> Outcome {
        Outcome {
            title: String::new(),
            lines: Vec::new(),
            refresh_local: false,
            privileged: None,
            after_privileged: None,
            remote: Some(super::remote::RemoteUpdate::ResolverClusters { panel, bases }),
            services: None,
            quiet: true,
        }
    }

    /// A silent result that opens the service-control server picker (admin domain
    /// services): the map's admin servers, one to pick and control.
    pub(super) fn service_servers(
        servers: Vec<super::remote::ServiceServerRow>,
    ) -> Outcome {
        Outcome {
            title: String::new(),
            lines: Vec::new(),
            refresh_local: false,
            privileged: None,
            after_privileged: None,
            remote: Some(super::remote::RemoteUpdate::ServiceServers { servers }),
            services: None,
            quiet: true,
        }
    }

    /// A silent Admin domain-tab services listing: apply the rows and open the
    /// services panel with no overlay — selecting a server drops straight into
    /// the units, and a refresh doesn't flash a toast.
    pub(super) fn remote_service_rows(rows: Vec<super::services::ServiceRow>) -> Outcome {
        Outcome {
            title: String::new(),
            lines: Vec::new(),
            refresh_local: false,
            privileged: None,
            after_privileged: None,
            remote: Some(super::remote::RemoteUpdate::ServiceRows { rows }),
            services: None,
            quiet: true,
        }
    }

    /// A Admin domain-tab service-control result: a toast plus the refreshed units.
    pub(super) fn remote_service_after(
        title: impl Into<String>,
        lines: Vec<String>,
        rows: Vec<super::services::ServiceRow>,
    ) -> Outcome {
        Outcome {
            title: title.into(),
            lines,
            refresh_local: false,
            privileged: None,
            after_privileged: None,
            remote: Some(super::remote::RemoteUpdate::ServiceRows { rows }),
            services: None,
            quiet: false,
        }
    }

    /// A silent Services-surface refresh: apply the rows, no overlay.
    pub(super) fn services_rows(rows: Vec<super::services::ServiceRow>) -> Outcome {
        Outcome {
            title: String::new(),
            lines: Vec::new(),
            refresh_local: false,
            privileged: None,
            after_privileged: None,
            remote: None,
            services: Some(super::services::ServicesUpdate { rows }),
            quiet: true,
        }
    }

    /// A Services action result: a toast plus the refreshed unit rows.
    pub(super) fn services_after(
        title: impl Into<String>,
        lines: Vec<String>,
        rows: Vec<super::services::ServiceRow>,
    ) -> Outcome {
        Outcome {
            title: title.into(),
            lines,
            refresh_local: false,
            privileged: None,
            after_privileged: None,
            remote: None,
            services: Some(super::services::ServicesUpdate { rows }),
            quiet: false,
        }
    }
}

/// A unit of work the TUI runs.
pub(super) enum Action {
    /// Install a role. `dry_run` previews the plan without writing anything.
    Install { role: InstallRole, dry_run: bool },
    /// Renew this host's certificates now.
    Renew,
    /// Reconcile this host's config with the admin domain (add/remove peers).
    Update { role: InstallRole, config_root: PathBuf },
    /// Graduate a local-only workstation onto an admin domain. `dry_run` previews.
    Join { dry_run: bool },
    /// Attach this resolver under a parent by delegation (resolver only).
    AddParent { config_root: PathBuf },
    /// A Tab-2 remote-admin op (connect / list / approve / …).
    Remote(super::remote::RemoteAction),
    /// Tear down an install (config + OS service). The engine decides what is
    /// installed and whether root is needed; the loop performs only the
    /// elevated step the engine hands back.
    Uninstall { config_scope: ServiceScope, config_dir: PathBuf, remove_ca: bool },
    /// Rotate (or first-enable) this box's local admin server auto-approve
    /// credential — a local, no-auth CA op over the control socket.
    #[cfg(unix)]
    AutoApprove { rotate: bool, ca_dir: PathBuf, cfg: Option<PathBuf> },
    /// Mint a fresh CA recovery password on this box (local, no-auth).
    #[cfg(unix)]
    RecoveryRotate { ca_dir: PathBuf, cfg: Option<PathBuf> },
    /// Back up the complete managed installation. A CA component is
    /// captured through its protected local control socket.
    Backup { config_root: PathBuf, scope: ServiceScope },
    /// Select and restore an installation bundle on a fresh machine.
    Restore,
    /// Resume CA+resolver restore after the privileged service step, carrying
    /// the engine's staged restore.
    FinishRestore(Box<bundle::Staged>),
    /// Re-emit a renewal CSR for this box's externally-signed CA (local).
    #[cfg(unix)]
    ExternalEmitCsr { ca_dir: PathBuf },
    /// Install an externally-signed CA certificate on this box (local).
    #[cfg(unix)]
    ExternalInstall { ca_dir: PathBuf },
    /// Open the Local tab's Services surface over the local activation
    /// supervisor's control socket (no auth). Pure navigation — the UI loop opens
    /// the surface and hands back the initial refresh op.
    OpenServices { units_dir: PathBuf },
    /// A local activation-services op (refresh / control / create / edit /
    /// delete), run from within the Services surface. Unit definition is
    /// local-only: this is the sole path to `ActivationDir`.
    Services(super::services::ServicesAction),
    /// Open the Local tab's local admin-server panel surface (roster, perms) over
    /// the control socket, landing directly on `panel`. Pure navigation — handled
    /// by the UI loop, not an op.
    #[cfg(unix)]
    ManageLocalAdmins { cfg_path: PathBuf, panel: super::remote::Panel },
}

impl Action {
    /// A short label for the progress header while the action runs.
    pub(super) fn label(&self) -> String {
        match self {
            Action::Install { role, dry_run } => {
                let verb = if *dry_run { "Previewing" } else { "Installing" };
                format!("{verb} {}", role.as_str())
            }
            Action::Renew => "Renewing certificates".to_string(),
            Action::Update { .. } => "Updating".to_string(),
            Action::Join { dry_run } => {
                if *dry_run {
                    "Previewing join".to_string()
                } else {
                    "Joining an admin domain".to_string()
                }
            }
            Action::AddParent { .. } => "Adding a parent".to_string(),
            Action::Remote(ra) => ra.label(),
            Action::Uninstall { .. } => "Uninstalling".to_string(),
            #[cfg(unix)]
            Action::AutoApprove { rotate, .. } => if *rotate {
                "Rotating auto-approve credential"
            } else {
                "Enabling auto-approve"
            }
            .to_string(),
            #[cfg(unix)]
            Action::RecoveryRotate { .. } => "Rotating recovery password".to_string(),
            Action::Backup { .. } => "Backing up this install".to_string(),
            Action::Restore => "Restoring an install".to_string(),
            Action::FinishRestore(_) => "Finishing restore".to_string(),
            #[cfg(unix)]
            Action::ExternalEmitCsr { .. } => "Emitting renewal CSR".to_string(),
            #[cfg(unix)]
            Action::ExternalInstall { .. } => "Installing signed certificate".to_string(),
            Action::OpenServices { .. } => "Services".to_string(),
            Action::Services(sa) => sa.label(),
            #[cfg(unix)]
            Action::ManageLocalAdmins { .. } => "Managing admins".to_string(),
        }
    }

    /// The pre-confirmed CA fingerprint the answerer should auto-accept (remote
    /// panel ops after connect), or `None` to prompt.
    pub(super) fn accept_glyph(
        &self,
    ) -> Option<netidx_admin_proto::fingerprint::Fingerprint> {
        match self {
            Action::Remote(ra) => ra.glyph(),
            _ => None,
        }
    }

    /// A yes/no confirmation to require before running, or `None` to run
    /// immediately. Installs are their own interactive cascade; uninstall is
    /// destructive and must be confirmed.
    pub(super) fn confirm_message(&self) -> Option<String> {
        match self {
            Action::Install { .. }
            | Action::Renew
            | Action::Update { .. }
            | Action::Join { .. }
            | Action::AddParent { .. }
            | Action::Backup { .. }
            | Action::Restore
            | Action::FinishRestore { .. } => None,
            #[cfg(unix)]
            Action::AutoApprove { rotate: false, .. }
            | Action::ExternalEmitCsr { .. }
            | Action::ExternalInstall { .. }
            | Action::ManageLocalAdmins { .. } => None,
            #[cfg(unix)]
            Action::AutoApprove { rotate: true, .. } => Some(
                "Rotate the auto-approve credential? The current keytab stops \
                 working; the admin server re-mints and re-seals it."
                    .to_string(),
            ),
            #[cfg(unix)]
            Action::RecoveryRotate { .. } => Some(
                "Rotate the CA recovery password? The current recovery password \
                 stops working and a new one is shown once — save it."
                    .to_string(),
            ),
            Action::OpenServices { .. } => None,
            Action::Services(sa) => sa.confirm_message(),
            Action::Remote(ra) => ra.confirm_message(),
            Action::Uninstall { remove_ca, .. } => Some(if *remove_ca {
                "Remove this install AND DESTROY THE CA? This stops and removes the \
                 OS service, deletes the configuration, and irreversibly deletes \
                 the CA directory — the admin domain's trust root. Every enrolled node's \
                 certificate becomes unverifiable and unrenewable. There is no undo."
                    .to_string()
            } else {
                "Remove this install? This stops and removes the OS service and \
                 deletes its configuration (the CA directory is kept)."
                    .to_string()
            }),
        }
    }

    /// The request glyph to show alongside a confirmation (the approve dialogs),
    /// so the admin verifies the identicon against the screenshot. `None` for
    /// confirmations with no associated glyph.
    pub(super) fn confirm_glyph(
        &self,
    ) -> Option<netidx_admin_proto::fingerprint::Fingerprint> {
        match self {
            Action::Remote(ra) => ra.confirm_glyph(),
            _ => None,
        }
    }
}

/// Run an action to completion on an owned answerer — the self-contained future
/// the UI loop drives. [`Action::Uninstall`] never reaches here (the loop runs
/// it directly).
pub(super) async fn run_owned(mut ans: TuiAnswerer, action: Action) -> Result<Outcome> {
    match action {
        Action::Install { role, dry_run } => install(&mut ans, role, dry_run).await,
        Action::Renew => renew(&mut ans).await,
        Action::Update { role, config_root } => update(&mut ans, role, config_root).await,
        Action::Join { dry_run } => join(&mut ans, dry_run).await,
        Action::AddParent { config_root } => add_parent(&mut ans, config_root).await,
        Action::Remote(ra) => super::remote::run(&mut ans, ra).await,
        Action::Uninstall { config_scope, config_dir, remove_ca } => {
            uninstall(&mut ans, config_scope, config_dir, remove_ca).await
        }
        #[cfg(unix)]
        Action::ManageLocalAdmins { .. } => {
            bail!("internal error: manage-local-admins is navigation, not an op future")
        }
        Action::OpenServices { .. } => {
            bail!("internal error: open-services is navigation, not an op future")
        }
        #[cfg(unix)]
        a @ (Action::AutoApprove { .. }
        | Action::RecoveryRotate { .. }
        | Action::ExternalEmitCsr { .. }
        | Action::ExternalInstall { .. }) => local_ca_op(&mut ans, a).await,
        Action::Backup { config_root, scope } => {
            backup(&mut ans, config_root, scope).await
        }
        Action::Restore => restore(&mut ans).await,
        Action::FinishRestore(staged) => finish_restore(&mut ans, staged).await,
        Action::Services(sa) => super::services::run(&mut ans, sa).await,
    }
}

/// Dispatch a local (no-auth, control-socket) CA op. These drive
/// `netidx_admin::ops::slots`, which is unix-only.
#[cfg(unix)]
async fn local_ca_op(ans: &mut TuiAnswerer, action: Action) -> Result<Outcome> {
    match action {
        Action::AutoApprove { rotate, ca_dir, cfg } => {
            auto_approve(ans, rotate, ca_dir, cfg).await
        }
        Action::RecoveryRotate { ca_dir, cfg } => recovery_rotate(ans, ca_dir, cfg).await,
        Action::ExternalEmitCsr { ca_dir } => external_emit_csr(ans, ca_dir).await,
        Action::ExternalInstall { ca_dir } => external_install(ans, ca_dir).await,
        _ => unreachable!("local_ca_op called with a non-CA action"),
    }
}

/// Rotate/enable the local admin server's auto-approve credential.
#[cfg(unix)]
async fn auto_approve(
    ans: &mut TuiAnswerer,
    rotate: bool,
    ca_dir: PathBuf,
    cfg: Option<PathBuf>,
) -> Result<Outcome> {
    use netidx_admin::ops::slots::{
        AutoApproveOutcome, AutorenewWiring, CaAccess, auto_approve,
    };
    let access = CaAccess::open(&ca_dir, cfg).await?;
    let out = auto_approve(ans, &access, ca_dir, rotate, false).await?;
    let lines = match out {
        AutoApproveOutcome::HotSwapped { warning } => {
            let mut l = vec![
                "The running admin server rotated its auto-approve credential in place \
                 (no downtime)."
                    .to_string(),
            ];
            l.extend(warning);
            l
        }
        AutoApproveOutcome::Offline { rotate, keytab, wiring } => {
            let verb = if rotate { "rotated" } else { "enabled" };
            let mut l =
                vec![format!("Auto-approve {verb}. Keytab: {}", keytab.display())];
            match wiring {
                AutorenewWiring::Updated(p) => {
                    l.push(format!("Config updated: {}", p.display()))
                }
                AutorenewWiring::NoCaConfig => {
                    l.push("No CA config owns this CA.".to_string())
                }
                AutorenewWiring::Failed(e) => {
                    l.push(format!("Config update failed (non-fatal): {e}"))
                }
            }
            l
        }
    };
    let title = if rotate { "Auto-approve rotated" } else { "Auto-approve enabled" };
    Ok(Outcome::plain(title, lines, true))
}

/// Mint a fresh CA recovery password on this box.
#[cfg(unix)]
async fn recovery_rotate(
    ans: &mut TuiAnswerer,
    ca_dir: PathBuf,
    cfg: Option<PathBuf>,
) -> Result<Outcome> {
    use netidx_admin::ops::slots::{CaAccess, RecoveryRotateOutcome, recovery_rotate};
    let access = CaAccess::open(&ca_dir, cfg).await?;
    let out = recovery_rotate(ans, &access, ca_dir).await?;
    let lines = match out {
        RecoveryRotateOutcome::HotSwapped => vec![
            "The running admin server minted a new recovery password (shown above) in \
             place."
                .to_string(),
        ],
        RecoveryRotateOutcome::Offline { .. } => {
            vec!["Minted a new recovery password (shown above).".to_string()]
        }
    };
    Ok(Outcome::plain("Recovery password rotated", lines, true))
}

async fn backup(
    ans: &mut TuiAnswerer,
    config_root: PathBuf,
    scope: ServiceScope,
) -> Result<Outcome> {
    let out = bundle::backup(
        ans,
        bundle::BackupInput {
            config_dir: Some(config_root),
            scope: Some(match scope {
                ServiceScope::User => install_bundle::BundleScope::User,
                ServiceScope::System => install_bundle::BundleScope::System,
            }),
            ..Default::default()
        },
    )
    .await?;
    Ok(Outcome::plain(
        "Installation backup created",
        vec![
            format!("Target: {}", out.target.display()),
            format!("Components: {:?}", out.components),
            format!("{} files · {} bytes", out.files, out.bytes),
            format!(
                "Fresh enrollment on restore: {} credential(s)",
                out.identities_to_reenroll
            ),
            format!("Manifest SHA-256: {}", out.manifest_sha256),
        ],
        false,
    ))
}

async fn restore(ans: &mut TuiAnswerer) -> Result<Outcome> {
    let staged = bundle::restore_stage(ans, &bundle::RestoreInput::default()).await?;
    match staged.next {
        bundle::Next::Finish => finish_restore(ans, Box::new(staged)).await,
        bundle::Next::ServiceThenFinish(scope) => Ok(Outcome {
            title: "CA restored".to_string(),
            lines: vec![
                "Starting the CA before re-enrolling its co-located TLS roles\u{2026}"
                    .to_string(),
            ],
            refresh_local: false,
            privileged: Some(Privileged::InstallService(restored_service(
                &staged.manifest,
                scope,
            ))),
            after_privileged: Some(Action::FinishRestore(Box::new(staged))),
            remote: None,
            services: None,
            quiet: true,
        }),
    }
}

fn restored_service(
    manifest: &install_bundle::Manifest,
    scope: ServiceScope,
) -> ServiceInstall {
    let (name, for_user) = bundle::restored_service(manifest, None, None);
    ServiceInstall { scope, name, for_user }
}

async fn finish_restore(
    ans: &mut TuiAnswerer,
    staged: Box<bundle::Staged>,
) -> Result<Outcome> {
    let manifest = staged.manifest.clone();
    let out = bundle::restore_finish(ans, *staged).await?;
    let mut lines = vec![format!("{} is installed and ready.", out.role.as_str())];
    if let Some(operation) = out.reconciled {
        lines.push(format!("Resolver hierarchy reconciled (operation {operation})."));
    }
    Ok(Outcome {
        title: "Restore complete".to_string(),
        lines,
        refresh_local: true,
        privileged: out
            .service_needed
            .map(|scope| Privileged::InstallService(restored_service(&manifest, scope))),
        after_privileged: None,
        remote: None,
        services: None,
        quiet: false,
    })
}

/// Re-emit a renewal CSR for an externally-signed CA.
#[cfg(unix)]
async fn external_emit_csr(ans: &mut TuiAnswerer, ca_dir: PathBuf) -> Result<Outcome> {
    use netidx_admin::ops::slots::{CaAccess, external_csr};
    let access = CaAccess::open(&ca_dir, None).await?;
    let csr = external_csr(ans, &access, ca_dir).await?;
    let csr = std::fs::canonicalize(&csr).unwrap_or(csr);
    Ok(Outcome::plain(
        "CSR emitted",
        vec![format!(
            "Wrote a renewal CSR to {}. Have your external PKI sign it, then install the \
             signed certificate.",
            csr.display()
        )],
        false,
    ))
}

/// Install an externally-signed CA certificate.
#[cfg(unix)]
async fn external_install(ans: &mut TuiAnswerer, ca_dir: PathBuf) -> Result<Outcome> {
    let signed = ans
        .text(Field::SignedCert, None, None, true)
        .await?
        .context("a signed certificate path is required")?;
    let root = ans
        .text(Field::ExternalRoot, None, None, false)
        .await?
        .filter(|s| !s.trim().is_empty());
    use netidx_admin::ops::slots::{CaAccess, ExternalInstallOutcome, external_install};
    let access = CaAccess::open(&ca_dir, None).await?;
    let out = external_install(
        ans,
        &access,
        ca_dir,
        PathBuf::from(&signed).as_path(),
        root.as_deref().map(std::path::Path::new),
    )
    .await?;
    match out {
        ExternalInstallOutcome::FirstInstall { need, cfg_path } => Ok(Outcome {
            title: "CA certificate installed".to_string(),
            lines: vec![format!(
                "Installed the externally-signed CA certificate; CA configured at {}.",
                cfg_path.display()
            )],
            refresh_local: true,
            privileged: need
                .scope()
                .map(|s| Privileged::InstallService(ServiceInstall::defaults(s))),
            after_privileged: None,
            remote: None,
            services: None,
            quiet: false,
        }),
        ExternalInstallOutcome::OfflineCa => Ok(Outcome::plain(
            "Certificate installed",
            vec!["Installed the externally-signed offline CA certificate.".into()],
            true,
        )),
        ExternalInstallOutcome::Renewal => Ok(Outcome::plain(
            "Certificate renewed",
            vec![
                "Renewed the CA certificate. Its admin server was not running; start \
                 it to serve the new certificate."
                    .to_string(),
            ],
            true,
        )),
        ExternalInstallOutcome::HotRenewed { ca_fingerprint } => Ok(Outcome::plain(
            "Certificate renewed",
            vec![
                "Renewed the externally-signed CA without stopping it.".to_string(),
                format!("CA identity: {ca_fingerprint}"),
            ],
            true,
        )),
    }
}

/// Reconcile config with the admin domain and apply the resulting edit plan.
async fn update(
    ans: &mut TuiAnswerer,
    role: InstallRole,
    config_root: PathBuf,
) -> Result<Outcome> {
    ans.progress(Progress::new(
        Stage::Discovering,
        "checking the admin domain for changes…",
    ));
    // Plan first, lock second: the config-directory lock never waits, so
    // holding it across the admin domain round trip would fail a concurrent
    // command for the duration of a network call.
    let plan = netidx_admin::sync::plan_for(role, Some(&config_root)).await?;
    if plan.is_empty() {
        return Ok(Outcome::plain(
            "Up to date",
            vec!["Already in sync with the admin domain — no changes.".to_string()],
            false,
        ));
    }
    let mut lines: Vec<String> =
        plan.edits.describe().lines().map(str::to_string).collect();
    let restart_hint = plan.restart_hint().to_string();
    plan.apply_locked().await?;
    lines.push(String::new());
    lines.push(restart_hint);
    Ok(Outcome {
        title: "Updated".to_string(),
        lines,
        refresh_local: true,
        privileged: None,
        after_privileged: None,
        remote: None,
        services: None,
        quiet: false,
    })
}

/// Graduate a local-only workstation onto an admin domain.
async fn join(ans: &mut TuiAnswerer, dry_run: bool) -> Result<Outcome> {
    use netidx_admin::plan::install::workstation::{
        WorkstationJoinInput, run_workstation_join,
    };
    let mode = if dry_run {
        InstallMode::DryRun
    } else {
        InstallMode::Apply {
            config_lock: ConfigDirLock::acquire_async(paths::user_config_root()?).await?,
        }
    };
    let input = WorkstationJoinInput { mode, key_protection: None, admin_server: None };
    run_workstation_join(ans, input).await?;
    let (title, lines) = if dry_run {
        ("Join preview", vec!["Preview only — nothing was written.".to_string()])
    } else {
        (
            "Joined",
            vec![
                "Joined the admin domain. Restart the local resolver to use it."
                    .to_string(),
            ],
        )
    };
    Ok(Outcome {
        title: title.to_string(),
        lines,
        refresh_local: !dry_run,
        privileged: None,
        after_privileged: None,
        remote: None,
        services: None,
        quiet: false,
    })
}

/// Prompt for the parent's admin-server address (the manual fallback when the
/// parent isn't in this host's admin domain map).
#[cfg(unix)]
async fn prompt_parent_admin(ans: &mut TuiAnswerer) -> Result<SocketAddr> {
    use netidx_admin::plan::resolve_admin_server_addr;
    loop {
        let s = ans.text(Field::ParentAddr, None, None, true).await?.unwrap_or_default();
        match resolve_admin_server_addr(&s) {
            Ok(a) => break Ok(a),
            Err(_) if ans.interactive() => ans.warn(&format!(
                "{s:?} is not a valid admin-server address — enter host or host:port"
            )),
            Err(e) => break Err(e),
        }
    }
}

/// Attach this resolver under a parent by delegation (child side).
#[cfg(unix)]
async fn add_parent(ans: &mut TuiAnswerer, config_root: PathBuf) -> Result<Outcome> {
    use super::answer::{ParentRow, ParentSelection};
    use netidx_admin::{
        ops::delegation::{
            AddParentCompletion, ResolverClusterPropagation, prepare_add_parent,
        },
        paths,
        plan::delegation::DelegationSelection,
        resolver::ResolverConfig,
    };
    use netidx_admin_proto::{ResolverAddr, ResolverClusterId, ServerState};
    let rpath = paths::discover_resolver_config()?;

    // Candidate parents come from the admin domain map (each resolver + its resolver cluster base),
    // minus this host's own resolvers. If the map is unreachable or offers no
    // other resolver, fall back to typing an admin-server address.
    // Only a resolver install can take a parent (local.rs offers the action
    // for that role alone), and fetch_map_for asserts it.
    let map =
        netidx_admin::sync::fetch_map_for(InstallRole::Resolver, Some(&config_root))
            .await
            .ok();
    // netidx-admin owns the first advertisable member in this host's resolver
    // config (the same member GetInfo has always reported). Unlike excluding
    // the whole config roster, excluding only this identity still shows AP2
    // when AP1 is splitting a four-peer root into US and /ap sets.
    let local_member = ResolverConfig::load(&rpath)
        .ok()
        .and_then(|c| c.resolver_addrs().into_iter().next());
    let local_server = map.as_ref().and_then(|map| {
        let local = local_member.as_ref()?;
        map.admin_servers.iter().find(|s| s.resolver.as_ref() == Some(local))
    });
    if map.is_some() && local_server.is_none() {
        bail!(
            "this resolver's locally owned member is absent from the CA admin domain map"
        );
    }
    // (admin addr, resolver addr+auth, current admin domain, base)
    // — the global source of truth for the picker.
    let cand: Vec<(SocketAddr, ResolverAddr, ResolverClusterId, String)> = match &map {
        Some(map) => {
            let mut cand = Vec::new();
            for server in map.admin_servers.iter().filter(|s| {
                s.state == ServerState::Registered
                    && s.roles.contains(netidx_admin_proto::Role::Resolver)
                    && Some(s.id) != local_server.map(|local| local.id)
            }) {
                let (Some(cluster_id), Some(resolver)) =
                    (server.cluster, server.resolver.clone())
                else {
                    continue;
                };
                let Some(cluster) =
                    map.resolver_clusters.iter().find(|c| c.id == cluster_id)
                else {
                    continue;
                };
                cand.push((server.addr, resolver, cluster_id, cluster.base.clone()));
            }
            cand
        }
        None => Vec::new(),
    };

    // (parent admin-server addr, optional referral override) — the two things the
    // delegation op needs.
    let (parent, selection): (SocketAddr, Option<DelegationSelection>) = if cand
        .is_empty()
    {
        (prompt_parent_admin(ans).await?, None)
    } else {
        loop {
            let rows: Vec<ParentRow> = cand
                .iter()
                .map(|(_, resolver, _, base)| ParentRow {
                    label: resolver.addr.to_string(),
                    base: base.clone(),
                })
                .collect();
            match ans.select_parent(rows).await? {
                ParentSelection::Manual => break (prompt_parent_admin(ans).await?, None),
                ParentSelection::Resolvers(idxs) => {
                    let picked: Vec<_> =
                        idxs.iter().filter_map(|&i| cand.get(i).cloned()).collect();
                    if picked.is_empty() {
                        continue;
                    }
                    let parent_cluster = picked[0].2;
                    if picked.iter().any(|candidate| candidate.2 != parent_cluster) {
                        ans.warn(
                            "those resolvers aren't all in one parent admin domain — pick \
                             resolvers served by a single admin domain",
                        );
                        continue;
                    }
                    break (
                        picked[0].0,
                        Some(DelegationSelection {
                            parent_resolvers: picked
                                .iter()
                                .map(|candidate| candidate.1.addr)
                                .collect(),
                        }),
                    );
                }
            }
        }
    };
    let path =
        ans.text(Field::DelegateSubtree, None, None, true).await?.unwrap_or_default();
    let out = match prepare_add_parent(ans, &rpath, parent, &path, selection).await? {
        AddParentCompletion::Complete(outcome) => outcome,
        AddParentCompletion::LocalWrite(pending) => {
            let lock =
                netidx_admin::config_lock::ConfigDirLock::acquire_async(&config_root)
                    .await?;
            pending.apply(&lock, ans)?
        }
    };
    let mut lines =
        vec![format!("Delegation of {:?} requested and approved.", out.proposed_path)];
    match out.propagation {
        ResolverClusterPropagation::CaManaged => {}
    }
    lines.push(
        "Configuration is written; do not restart the whole admin domain at once. Restart one member, wait the resolver delay-reads period for publishers to republish, then restart the next member."
            .to_string(),
    );
    Ok(Outcome {
        title: "Parent added".to_string(),
        lines,
        refresh_local: true,
        privileged: None,
        after_privileged: None,
        remote: None,
        services: None,
        quiet: false,
    })
}

#[cfg(not(unix))]
async fn add_parent(_ans: &mut TuiAnswerer, _config_root: PathBuf) -> Result<Outcome> {
    bail!("adding a parent (delegation) is only available on unix hosts")
}

/// Tear down this host's install. The operator already confirmed (see
/// `App::arm_action`), so all that is left is to let the engine resolve what
/// is installed and either apply it here or hand back the one elevated step.
async fn uninstall(
    ans: &mut TuiAnswerer,
    config_scope: ServiceScope,
    config_dir: PathBuf,
    remove_ca: bool,
) -> Result<Outcome> {
    let input = uninstall::UninstallInput {
        scope: Some(config_scope),
        config_dir: Some(config_dir.clone()),
        remove_ca,
        ..Default::default()
    };
    match uninstall::plan(&input)? {
        uninstall::Next::Nothing(_) => Ok(Outcome::plain(
            "Uninstalled",
            vec!["Nothing to remove.".to_string()],
            true,
        )),
        uninstall::Next::Apply(prepared) => {
            let report = uninstall::apply(ans, prepared).await?;
            Ok(Outcome::plain("Uninstalled", removal_lines(&report), true))
        }
        uninstall::Next::Escalate(escalation) => {
            // Only the elevated half needs the terminal. When it covers the
            // whole teardown there is nothing left; otherwise it removes the
            // system-scope service a user-scope install registered and we
            // plan again for the configuration.
            let done = escalation.covers == uninstall::Covers::Everything;
            Ok(Outcome {
                title: "Uninstalling".to_string(),
                lines: vec![
                    "Administrator privileges are needed to remove the system service\u{2026}"
                        .to_string(),
                ],
                refresh_local: done,
                privileged: Some(Privileged::Uninstall(escalation)),
                after_privileged: (!done).then(|| Action::Uninstall {
                    config_scope,
                    config_dir,
                    remove_ca,
                }),
                remote: None,
                services: None,
                quiet: true,
            })
        }
    }
}

fn removal_lines(report: &uninstall::UninstallReport) -> Vec<String> {
    let mut lines = vec![format!(
        "Removed {} path(s){}.",
        report.removed.len(),
        if report.service_was_installed { " and the OS service" } else { "" }
    )];
    for (path, why) in &report.kept {
        lines.push(format!("Kept {} ({}).", path.display(), why.as_str()));
    }
    lines
}

async fn renew(_ans: &mut TuiAnswerer) -> Result<Outcome> {
    let report = renewd::run_once(renewd::RenewalConfig::default()).await;
    let mut lines: Vec<String> = Vec::new();
    for cert in report.renewed.iter().chain(report.awaiting_approval.iter()) {
        lines.push(format!("{}", cert.display()));
    }
    for (cert, why) in &report.failed {
        lines.push(format!("{}: {why}", cert.display()));
    }
    match report.summary() {
        None => Ok(Outcome::plain(
            "Nothing to renew",
            vec![
                "Every certificate on this host is outside its renewal window."
                    .to_string(),
            ],
            false,
        )),
        Some(summary) => {
            lines.insert(0, summary.to_string());
            Ok(Outcome::plain(
                if report.failed.is_empty() { "Renewed" } else { "Renewal incomplete" },
                lines,
                !report.failed.is_empty(),
            ))
        }
    }
}

async fn install(
    ans: &mut TuiAnswerer,
    role: InstallRole,
    dry_run: bool,
) -> Result<Outcome> {
    let mode = if dry_run {
        InstallMode::DryRun
    } else {
        InstallMode::Apply {
            config_lock: ConfigDirLock::acquire_async(paths::user_config_root()?).await?,
        }
    };
    let common = InstallCommon {
        mode,
        force: false,
        no_units: false,
        with_service: false,
        no_service: false,
    };
    let scope = match role {
        #[cfg(unix)]
        InstallRole::Ca => run_ca(ans, CaInput::defaults(common)).await?,
        #[cfg(not(unix))]
        InstallRole::Ca => bail!("the CA role is supported only on unix"),
        InstallRole::Resolver => run_resolver(ans, resolver_input(common)).await?,
        InstallRole::Publisher => run_publisher(ans, publisher_input(common)).await?,
        InstallRole::Workstation => run_workstation(ans, common).await?,
    };
    #[cfg(unix)]
    if !dry_run && matches!(role, InstallRole::Ca) && scope.is_none() {
        let ca_dir = paths::user_ca_dir()?;
        let access = netidx_admin::ops::slots::CaAccess::open(&ca_dir, None).await?;
        let status = netidx_admin::ops::slots::local_ca_status(&access, &ca_dir).await?;
        if let Some((common_name, _)) = status.external.pending {
            let relative = offline_ca::default_csr_filename(&common_name);
            let csr = std::env::current_dir()?.join(relative);
            return Ok(Outcome::plain(
                "CA awaiting external signature",
                vec![
                    "The CA is not running yet; no OS service was registered."
                        .into(),
                    format!("Subordinate-CA CSR: {}", csr.display()),
                    "Have the external PKI sign that CSR, return to this TUI, and choose \"Install Signed Certificate (External CA)\"."
                        .into(),
                ],
                true,
            ));
        }
    }
    Ok(install_outcome(role, dry_run, scope))
}

#[cfg(any(unix, windows))]
async fn run_workstation(
    ans: &mut TuiAnswerer,
    common: InstallCommon,
) -> Result<Option<ServiceScope>> {
    use netidx_admin::plan::install::workstation::run_workstation;
    run_workstation(ans, guided_workstation_input(common)).await
}

#[cfg(any(unix, windows))]
fn guided_workstation_input(
    common: InstallCommon,
) -> netidx_admin::plan::install::workstation::WorkstationInput {
    netidx_admin::plan::install::workstation::WorkstationInput::defaults(common)
}

#[cfg(not(any(unix, windows)))]
async fn run_workstation(
    _ans: &mut TuiAnswerer,
    _common: InstallCommon,
) -> Result<Option<ServiceScope>> {
    bail!("the workstation role is not supported on this platform")
}

fn resolver_input(common: InstallCommon) -> ResolverInput {
    ResolverInput {
        // Let the install decide from whether it is joining a cluster that is
        // already serving.
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

fn install_outcome(
    role: InstallRole,
    dry_run: bool,
    scope: Option<ServiceScope>,
) -> Outcome {
    if dry_run {
        let mut lines = vec!["Preview only — nothing was written.".to_string()];
        if scope.is_some() {
            lines.push("A real install would then register the OS service.".to_string());
        }
        Outcome::plain(format!("{} preview", role.as_str()), lines, false)
    } else {
        let lines = match scope {
            Some(_) => {
                vec!["Configuration written. Registering the OS service…".to_string()]
            }
            None => {
                vec!["Configuration written. No OS service was registered.".to_string()]
            }
        };
        Outcome {
            title: format!("{} installed", role.as_str()),
            lines,
            refresh_local: true,
            privileged: scope
                .map(|s| Privileged::InstallService(ServiceInstall::defaults(s))),
            after_privileged: None,
            remote: None,
            services: None,
            quiet: false,
        }
    }
}

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use super::*;

    #[test]
    fn guided_workstation_uses_shared_safe_defaults() {
        let input = guided_workstation_input(InstallCommon {
            mode: InstallMode::DryRun,
            force: false,
            no_units: false,
            with_service: false,
            no_service: false,
        });
        assert_eq!(input.base, "/local");
        assert!(input.with_perms_file);
        assert!(input.with_container);
    }
}
