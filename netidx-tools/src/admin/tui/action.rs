//! Actions the TUI runs against the library, and the [`Outcome`] it shows when
//! one finishes.
//!
//! Most actions are async library ops driven through a [`TuiAnswerer`](super::answer::TuiAnswerer):
//! [`run_owned`] is the self-contained future the UI loop polls. The one
//! privileged, terminal-owning follow-up (registering a system OS service) is
//! handed back on the [`Outcome`] and performed by the loop, which can suspend
//! the terminal for the password prompt — see [`super::privileged`].
//!
//! [`Action::Uninstall`] is the exception: it is privileged from the start, so
//! the loop runs it directly rather than as an op future.

use super::answer::TuiAnswerer;
use anyhow::{Context, Result, bail};
use netidx_admin::{
    answer::{Answerer, Field, Progress, Stage},
    plan::install::{
        InstallCommon,
        publisher::{PublisherInput, run_publisher},
        resolver::{ResolverInput, run_resolver},
    },
    provenance::InstallRole,
    renewd,
    service::ServiceScope,
};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

/// What the UI shows after an action completes.
pub(super) struct Outcome {
    pub(super) title: String,
    pub(super) lines: Vec<String>,
    /// Re-detect local installs after showing this (an install changed on-disk
    /// state).
    pub(super) refresh_local: bool,
    /// A privileged follow-up the UI loop performs with the terminal: register
    /// the OS service at this scope. `None` ⇒ nothing to do.
    pub(super) install_service: Option<ServiceScope>,
    /// A result to apply to the Remote tab's state (connection / panel rows).
    pub(super) remote: Option<super::remote::RemoteUpdate>,
    /// A result to apply to the Local tab's Services surface (refreshed rows).
    pub(super) services: Option<super::services::ServicesUpdate>,
    /// Suppress the result overlay (used by silent panel re-queries).
    pub(super) quiet: bool,
}

impl Outcome {
    fn plain(title: impl Into<String>, lines: Vec<String>, refresh_local: bool) -> Outcome {
        Outcome {
            title: title.into(),
            lines,
            refresh_local,
            install_service: None,
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
            install_service: None,
            remote: Some(update),
            services: None,
            quiet: false,
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
            install_service: None,
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
            install_service: None,
            remote: Some(super::remote::RemoteUpdate::Rows { panel, rows }),
            services: None,
            quiet: false,
        }
    }

    /// A silent result that opens a panel's level picker (cluster perms).
    pub(super) fn levels(panel: super::remote::Panel, levels: Vec<String>) -> Outcome {
        Outcome {
            title: String::new(),
            lines: Vec::new(),
            refresh_local: false,
            install_service: None,
            remote: Some(super::remote::RemoteUpdate::Levels { panel, levels }),
            services: None,
            quiet: true,
        }
    }

    /// A silent Services-surface refresh: apply the rows, no overlay.
    pub(super) fn services_rows(rows: Vec<super::services::ServiceRow>) -> Outcome {
        Outcome {
            title: String::new(),
            lines: Vec::new(),
            refresh_local: false,
            install_service: None,
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
            install_service: None,
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
    Renew { server: Option<SocketAddr> },
    /// Reconcile this host's config with the network (add/remove peers).
    Update { role: InstallRole },
    /// Graduate a local-only workstation onto a network. `dry_run` previews.
    Join { dry_run: bool },
    /// Attach this resolver under a parent by delegation (resolver only).
    AddParent,
    /// Jump to the Remote tab's delegation panel to review this resolver's
    /// pending delegation requests (resolver-with-admin-server only). Pure
    /// navigation — handled by the UI loop, never an op future.
    ReviewDelegations,
    /// A Tab-2 remote-admin op (connect / list / approve / …).
    Remote(super::remote::RemoteAction),
    /// Tear down an install (config + OS service). Terminal-owning; handled
    /// directly by the UI loop, not as an op future. `needs_root` when a
    /// system-scope service must be removed.
    Uninstall {
        config_scope: ServiceScope,
        config_dir: PathBuf,
        needs_root: bool,
        remove_ca: bool,
    },
    /// Rotate (or first-enable) this box's local admin server auto-approve
    /// credential — a local, no-auth CA op over the control socket.
    AutoApprove { rotate: bool, ca_dir: PathBuf, cfg: Option<PathBuf> },
    /// Mint a fresh CA recovery password on this box (local, no-auth).
    RecoveryRotate { ca_dir: PathBuf, cfg: Option<PathBuf> },
    /// Re-emit a renewal CSR for this box's externally-signed CA (local).
    ExternalEmitCsr { ca_dir: PathBuf },
    /// Install an externally-signed CA certificate on this box (local).
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
    /// by the UI loop, not an op. `ca_dir` lets the perms read auto-verify against
    /// the local CA cert.
    ManageLocalAdmins { cfg_path: PathBuf, ca_dir: PathBuf, panel: super::remote::Panel },
}

impl Action {
    /// A short label for the progress header while the action runs.
    pub(super) fn label(&self) -> String {
        match self {
            Action::Install { role, dry_run } => {
                let verb = if *dry_run { "Previewing" } else { "Installing" };
                format!("{verb} {}", role.as_str())
            }
            Action::Renew { .. } => "Renewing certificates".to_string(),
            Action::Update { .. } => "Updating".to_string(),
            Action::Join { dry_run } => {
                if *dry_run { "Previewing join".to_string() } else { "Joining a cluster".to_string() }
            }
            Action::AddParent => "Adding a parent".to_string(),
            Action::ReviewDelegations => "Reviewing delegations".to_string(),
            Action::Remote(ra) => ra.label(),
            Action::Uninstall { .. } => "Uninstalling".to_string(),
            Action::AutoApprove { rotate, .. } => {
                if *rotate { "Rotating auto-approve credential" } else { "Enabling auto-approve" }
                    .to_string()
            }
            Action::RecoveryRotate { .. } => "Rotating recovery password".to_string(),
            Action::ExternalEmitCsr { .. } => "Emitting renewal CSR".to_string(),
            Action::ExternalInstall { .. } => "Installing signed certificate".to_string(),
            Action::OpenServices { .. } => "Services".to_string(),
            Action::Services(sa) => sa.label(),
            Action::ManageLocalAdmins { .. } => "Managing admins".to_string(),
        }
    }

    /// The pre-confirmed CA fingerprint the answerer should auto-accept (remote
    /// panel ops after connect), or `None` to prompt.
    pub(super) fn accept_glyph(&self) -> Option<netidx_admin::fingerprint::Fingerprint> {
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
            | Action::Renew { .. }
            | Action::Update { .. }
            | Action::Join { .. }
            | Action::AddParent
            | Action::ReviewDelegations
            | Action::AutoApprove { rotate: false, .. }
            | Action::ExternalEmitCsr { .. }
            | Action::ExternalInstall { .. }
            | Action::ManageLocalAdmins { .. } => None,
            Action::AutoApprove { rotate: true, .. } => Some(
                "Rotate the auto-approve credential? The current keytab stops \
                 working; the admin server re-mints and re-seals it."
                    .to_string(),
            ),
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
                 the CA directory — the network's trust root. Every enrolled node's \
                 certificate becomes unverifiable and unrenewable. There is no undo."
                    .to_string()
            } else {
                "Remove this install? This stops and removes the OS service and \
                 deletes its configuration (the CA directory is kept)."
                    .to_string()
            }),
        }
    }
}

/// Run an action to completion on an owned answerer — the self-contained future
/// the UI loop drives. [`Action::Uninstall`] never reaches here (the loop runs
/// it directly).
pub(super) async fn run_owned(mut ans: TuiAnswerer, action: Action) -> Result<Outcome> {
    match action {
        Action::Install { role, dry_run } => install(&mut ans, role, dry_run).await,
        Action::Renew { server } => renew(&mut ans, server).await,
        Action::Update { role } => update(&mut ans, role).await,
        Action::Join { dry_run } => join(&mut ans, dry_run).await,
        Action::AddParent => add_parent(&mut ans).await,
        Action::Remote(ra) => super::remote::run(&mut ans, ra).await,
        Action::ReviewDelegations => {
            bail!("internal error: review-delegations is navigation, not an op future")
        }
        Action::Uninstall { .. } => bail!("internal error: uninstall is not an op future"),
        Action::ManageLocalAdmins { .. } => {
            bail!("internal error: manage-local-admins is navigation, not an op future")
        }
        Action::OpenServices { .. } => {
            bail!("internal error: open-services is navigation, not an op future")
        }
        a @ (Action::AutoApprove { .. }
        | Action::RecoveryRotate { .. }
        | Action::ExternalEmitCsr { .. }
        | Action::ExternalInstall { .. }) => local_ca_op(&mut ans, a).await,
        Action::Services(sa) => super::services::run(&mut ans, sa).await,
    }
}

/// Dispatch a local (no-auth, control-socket) CA op. These drive
/// `netidx_admin::admin_ops::slots`, which is unix-only.
#[cfg(unix)]
async fn local_ca_op(ans: &mut TuiAnswerer, action: Action) -> Result<Outcome> {
    match action {
        Action::AutoApprove { rotate, ca_dir, cfg } => auto_approve(ans, rotate, ca_dir, cfg).await,
        Action::RecoveryRotate { ca_dir, cfg } => recovery_rotate(ans, ca_dir, cfg).await,
        Action::ExternalEmitCsr { ca_dir } => external_emit_csr(ans, ca_dir).await,
        Action::ExternalInstall { ca_dir } => external_install(ans, ca_dir).await,
        _ => unreachable!("local_ca_op called with a non-CA action"),
    }
}

#[cfg(not(unix))]
async fn local_ca_op(_ans: &mut TuiAnswerer, _action: Action) -> Result<Outcome> {
    bail!("local admin-server CA operations are only available on unix hosts")
}

/// Rotate/enable the local admin server's auto-approve credential.
#[cfg(unix)]
async fn auto_approve(
    ans: &mut TuiAnswerer,
    rotate: bool,
    ca_dir: PathBuf,
    cfg: Option<PathBuf>,
) -> Result<Outcome> {
    use netidx_admin::admin_ops::slots::{AutoApproveOutcome, auto_approve};
    let out = auto_approve(ans, ca_dir, cfg, rotate, false).await?;
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
        AutoApproveOutcome::Offline { rotate, keytab, cfg_path, cfg_error } => {
            let verb = if rotate { "rotated" } else { "enabled" };
            let mut l = vec![format!("Auto-approve {verb}. Keytab: {}", keytab.display())];
            match (cfg_path, cfg_error) {
                (Some(p), _) => l.push(format!("Config updated: {}", p.display())),
                (None, Some(e)) => l.push(format!("Config update failed (non-fatal): {e}")),
                (None, None) => {}
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
    use netidx_admin::admin_ops::slots::{RecoveryRotateOutcome, recovery_rotate};
    let out = recovery_rotate(ans, ca_dir, cfg).await?;
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

/// Re-emit a renewal CSR for an externally-signed CA.
#[cfg(unix)]
async fn external_emit_csr(ans: &mut TuiAnswerer, ca_dir: PathBuf) -> Result<Outcome> {
    let csr = netidx_admin::admin_ops::slots::external_emit_csr(ans, ca_dir).await?;
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
    use netidx_admin::admin_ops::slots::{ExternalInstallOutcome, external_install_cert};
    let signed = ans
        .text(Field::SignedCert, None, None, true)
        .await?
        .context("a signed certificate path is required")?;
    let root =
        ans.text(Field::ExternalRoot, None, None, false).await?.filter(|s| !s.trim().is_empty());
    let out = external_install_cert(
        ans,
        ca_dir,
        Path::new(&signed),
        root.as_deref().map(Path::new),
    )
    .await?;
    match out {
        ExternalInstallOutcome::OfflineCa => Ok(Outcome::plain(
            "Certificate installed",
            vec!["Installed the externally-signed CA certificate (offline CA).".to_string()],
            true,
        )),
        ExternalInstallOutcome::Renewal => Ok(Outcome::plain(
            "Certificate renewed",
            vec![
                "Renewed the CA certificate — enrolled nodes adopt it on their next \
                 renewal (glyph unchanged; existing certificates stay valid)."
                    .to_string(),
            ],
            true,
        )),
        ExternalInstallOutcome::FirstInstall { need, cfg_path } => Ok(Outcome {
            title: "Certificate installed".to_string(),
            lines: vec![
                format!("Installed the externally-signed CA certificate; admin server configured ({}).", cfg_path.display()),
                "CA-cert auto-renewal is DISABLED (external issuer); re-run install when your PKI re-signs it.".to_string(),
            ],
            refresh_local: true,
            install_service: need.scope(),
            remote: None,
            services: None,
            quiet: false,
        }),
    }
}

/// Reconcile config with the network and apply the resulting edit plan.
async fn update(ans: &mut TuiAnswerer, role: InstallRole) -> Result<Outcome> {
    ans.progress(Progress::new(Stage::Discovering, "checking the cluster for changes…"));
    let plan = super::lifecycle::update_plan(role).await?;
    if plan.is_empty() {
        return Ok(Outcome::plain(
            "Up to date",
            vec!["Already in sync with the cluster — no changes.".to_string()],
            false,
        ));
    }
    let mut lines: Vec<String> = plan.describe().lines().map(str::to_string).collect();
    plan.apply()?;
    lines.push(String::new());
    lines.push(super::lifecycle::restart_hint(role).to_string());
    Ok(Outcome {
        title: "Updated".to_string(),
        lines,
        refresh_local: true,
        install_service: None,
        remote: None,
        services: None,
        quiet: false,
    })
}

/// Graduate a local-only workstation onto a network.
async fn join(ans: &mut TuiAnswerer, dry_run: bool) -> Result<Outcome> {
    use netidx_admin::plan::install::workstation::{WorkstationJoinInput, run_workstation_join};
    let input = WorkstationJoinInput { dry_run, key_protection: None, admin_server: None };
    run_workstation_join(ans, input).await?;
    let (title, lines) = if dry_run {
        ("Join preview", vec!["Preview only — nothing was written.".to_string()])
    } else {
        ("Joined", vec!["Joined the cluster. Restart the local resolver to use it.".to_string()])
    };
    Ok(Outcome {
        title: title.to_string(),
        lines,
        refresh_local: !dry_run,
        install_service: None,
        remote: None,
        services: None,
        quiet: false,
    })
}

/// The admin server that can approve a delegation to the ticked resolvers: the
/// one whose cluster contains ALL of them. `None` when the ticks span clusters
/// (no single cluster holds them all) — the operator must pick resolvers served
/// by one parent cluster.
#[cfg(unix)]
fn parent_admin_for(
    map: &netidx_admin::admin_proto::NetworkMap,
    picked: &[SocketAddr],
) -> Option<SocketAddr> {
    map.servers
        .iter()
        .find(|s| {
            s.cluster.as_ref().is_some_and(|c| {
                picked.iter().all(|a| c.members.iter().any(|m| m.addr == *a))
            })
        })
        .map(|s| s.addr)
}

/// Prompt for the parent's admin-server address (the manual fallback when the
/// parent isn't in this host's network map).
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
async fn add_parent(ans: &mut TuiAnswerer) -> Result<Outcome> {
    use super::answer::{ParentRow, ParentSelection};
    use netidx_admin::{
        admin_ops::delegation::{ClusterPropagation, add_parent as do_add_parent},
        admin_proto::ResolverAddr,
        paths,
        resolver::ResolverConfig,
    };
    use std::collections::HashSet;
    let rpath = paths::discover_resolver_config()?;

    // Candidate parents come from the network map (each resolver + its level),
    // minus this host's own resolvers. If the map is unreachable or offers no
    // other resolver, fall back to typing an admin-server address.
    let map = super::lifecycle::fetch_local_map().await.ok();
    let own: HashSet<SocketAddr> = ResolverConfig::load(&rpath)
        .map(|c| c.resolver_addrs().into_iter().map(|r| r.addr).collect())
        .unwrap_or_default();
    // (resolver addr+auth, its cluster base) — the source of truth for the picker.
    let cand: Vec<(ResolverAddr, String)> = match &map {
        Some(map) => {
            let mut seen = HashSet::new();
            let mut cand = Vec::new();
            for s in &map.servers {
                if let Some(c) = &s.cluster {
                    for m in &c.members {
                        if own.contains(&m.addr) || !seen.insert(m.addr) {
                            continue;
                        }
                        cand.push((m.clone(), c.base.clone()));
                    }
                }
            }
            cand
        }
        None => Vec::new(),
    };

    // (parent admin-server addr, optional referral override) — the two things the
    // delegation op needs.
    let (parent, referral): (SocketAddr, Option<Vec<ResolverAddr>>) = if cand.is_empty() {
        (prompt_parent_admin(ans).await?, None)
    } else {
        let map = map.as_ref().expect("cand non-empty implies a map");
        loop {
            let rows: Vec<ParentRow> = cand
                .iter()
                .map(|(r, level)| ParentRow { label: r.addr.to_string(), level: level.clone() })
                .collect();
            match ans.select_parent(rows).await? {
                ParentSelection::Manual => break (prompt_parent_admin(ans).await?, None),
                ParentSelection::Resolvers(idxs) => {
                    let picked: Vec<ResolverAddr> =
                        idxs.iter().filter_map(|&i| cand.get(i).map(|(r, _)| r.clone())).collect();
                    if picked.is_empty() {
                        continue;
                    }
                    // The approving admin server is the one whose cluster contains
                    // ALL the ticked resolvers (any of a multi-admin cluster works).
                    let want: Vec<SocketAddr> = picked.iter().map(|r| r.addr).collect();
                    match parent_admin_for(map, &want) {
                        Some(admin) => break (admin, Some(picked)),
                        None => ans.warn(
                            "those resolvers aren't all in one parent cluster — pick \
                             resolvers served by a single cluster",
                        ),
                    }
                }
            }
        }
    };
    let path = ans.text(Field::DelegateSubtree, None, None, true).await?.unwrap_or_default();
    let out = do_add_parent(ans, &rpath, parent, &path, referral).await?;
    let mut lines = vec![format!("Delegation of {:?} requested and approved.", out.proposed_path)];
    match out.propagation {
        ClusterPropagation::SingleMember => {}
        ClusterPropagation::NoAdminServer { members } => lines.push(format!(
            "{members}-member cluster with no admin server — hand-copy the new parent \
             block to the other members."
        )),
        ClusterPropagation::Pushed(peers) => {
            let failed = peers.iter().filter(|p| p.error.is_some()).count();
            if failed == 0 {
                lines.push(format!("Propagated to {} cluster peer(s).", peers.len()));
            } else {
                lines.push(format!(
                    "{failed} of {} cluster peer(s) could NOT be updated — re-run to converge:",
                    peers.len()
                ));
                for p in peers.iter().filter(|p| p.error.is_some()) {
                    lines.push(format!("  ! {} : {}", p.addr, p.error.as_deref().unwrap_or("")));
                }
            }
        }
    }
    lines.push("Restart your resolver server(s) to attach under the parent.".to_string());
    Ok(Outcome {
        title: "Parent added".to_string(),
        lines,
        refresh_local: true,
        install_service: None,
        remote: None,
        services: None,
        quiet: false,
    })
}

#[cfg(not(unix))]
async fn add_parent(_ans: &mut TuiAnswerer) -> Result<Outcome> {
    bail!("adding a parent (delegation) is only available on unix hosts")
}

async fn renew(_ans: &mut TuiAnswerer, server: Option<SocketAddr>) -> Result<Outcome> {
    renewd::run_once(server).await?;
    Ok(Outcome::plain(
        "Renewed",
        vec!["Scanned and renewed certificates due for renewal.".to_string()],
        false,
    ))
}

async fn install(ans: &mut TuiAnswerer, role: InstallRole, dry_run: bool) -> Result<Outcome> {
    let common = InstallCommon {
        dry_run,
        force: false,
        no_units: false,
        with_service: false,
        no_service: false,
    };
    let scope = match role {
        InstallRole::Resolver => run_resolver(ans, resolver_input(common)).await?,
        InstallRole::Publisher => run_publisher(ans, publisher_input(common)).await?,
        InstallRole::Workstation => run_workstation(ans, common).await?,
    };
    Ok(install_outcome(role, dry_run, scope))
}

#[cfg(any(unix, windows))]
async fn run_workstation(
    ans: &mut TuiAnswerer,
    common: InstallCommon,
) -> Result<Option<ServiceScope>> {
    use netidx_admin::plan::install::workstation::{WorkstationInput, run_workstation};
    let input = WorkstationInput {
        explicit_parent: None,
        admin_server: None,
        default_auth: None,
        base: "/local".to_string(),
        listen_port: None,
        local_socket: None,
        client_config_path: None,
        resolver_config_path: None,
        units_dir: None,
        netidx_binary: None,
        key_protection: None,
        with_container: false,
        owner: None,
        with_perms_file: false,
        perms_path: None,
        common,
    };
    run_workstation(ans, input).await
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use netidx_admin::admin_proto::{
        ClusterFacts, InfoAuth, NetworkMap, ResolverAddr, ServerEntry,
    };

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn resolver(a: &str) -> ResolverAddr {
        ResolverAddr { addr: addr(a), auth: InfoAuth::Anonymous }
    }

    fn server(admin: &str, base: &str, members: &[&str]) -> ServerEntry {
        ServerEntry {
            addr: addr(admin),
            roles: vec![],
            cluster: Some(ClusterFacts {
                members: members.iter().map(|m| resolver(m)).collect(),
                base: base.to_string(),
                parent: None,
                children: vec![],
            }),
        }
    }

    fn map(servers: Vec<ServerEntry>) -> NetworkMap {
        NetworkMap { version: 0, ca_addr: None, servers }
    }

    #[test]
    fn parent_admin_is_the_cluster_holding_all_ticks() {
        // Two independent clusters, both rooted at `/` (the base-collision case).
        let m = map(vec![
            server("10.0.0.1:4565", "/", &["10.0.0.1:4564", "10.0.0.2:4564"]),
            server("10.0.60.1:4565", "/", &["10.0.60.1:4564"]),
        ]);
        // Ticking both US root resolvers resolves to the US admin server.
        let admin = parent_admin_for(&m, &[addr("10.0.0.1:4564"), addr("10.0.0.2:4564")]);
        assert_eq!(admin, Some(addr("10.0.0.1:4565")));
        // Ticking the lone Asia resolver resolves to the Asia admin server.
        let admin = parent_admin_for(&m, &[addr("10.0.60.1:4564")]);
        assert_eq!(admin, Some(addr("10.0.60.1:4565")));
    }

    #[test]
    fn ticks_spanning_clusters_have_no_single_admin() {
        let m = map(vec![
            server("10.0.0.1:4565", "/", &["10.0.0.1:4564"]),
            server("10.0.60.1:4565", "/", &["10.0.60.1:4564"]),
        ]);
        // One resolver from each cluster: no single cluster holds both.
        let admin = parent_admin_for(&m, &[addr("10.0.0.1:4564"), addr("10.0.60.1:4564")]);
        assert_eq!(admin, None);
    }
}

fn install_outcome(role: InstallRole, dry_run: bool, scope: Option<ServiceScope>) -> Outcome {
    if dry_run {
        let mut lines = vec!["Preview only — nothing was written.".to_string()];
        if scope.is_some() {
            lines.push("A real install would then register the OS service.".to_string());
        }
        Outcome::plain(format!("{} preview", role.as_str()), lines, false)
    } else {
        let lines = match scope {
            Some(_) => vec!["Configuration written. Registering the OS service…".to_string()],
            None => vec!["Configuration written. No OS service was registered.".to_string()],
        };
        Outcome {
            title: format!("{} installed", role.as_str()),
            lines,
            refresh_local: true,
            install_service: scope,
            remote: None,
            services: None,
            quiet: false,
        }
    }
}
