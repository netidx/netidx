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
use anyhow::{Result, bail};
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
use std::{net::SocketAddr, path::PathBuf};

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
                if *dry_run { "Previewing join".to_string() } else { "Joining a network".to_string() }
            }
            Action::AddParent => "Adding a parent".to_string(),
            Action::ReviewDelegations => "Reviewing delegations".to_string(),
            Action::Remote(ra) => ra.label(),
            Action::Uninstall { .. } => "Uninstalling".to_string(),
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
            | Action::ReviewDelegations => None,
            Action::Remote(ra) => ra.confirm_message(),
            Action::Uninstall { .. } => Some(
                "Remove this install? This stops and removes the OS service and \
                 deletes its configuration (the CA directory is kept)."
                    .to_string(),
            ),
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
    }
}

/// Reconcile config with the network and apply the resulting edit plan.
async fn update(ans: &mut TuiAnswerer, role: InstallRole) -> Result<Outcome> {
    ans.progress(Progress::new(Stage::Discovering, "checking the network for changes…"));
    let plan = super::lifecycle::update_plan(role).await?;
    if plan.is_empty() {
        return Ok(Outcome::plain(
            "Up to date",
            vec!["Already in sync with the network — no changes.".to_string()],
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
        ("Joined", vec!["Joined the network. Restart the local resolver to use it.".to_string()])
    };
    Ok(Outcome {
        title: title.to_string(),
        lines,
        refresh_local: !dry_run,
        install_service: None,
        remote: None,
        quiet: false,
    })
}

/// Attach this resolver under a parent by delegation (child side).
#[cfg(unix)]
async fn add_parent(ans: &mut TuiAnswerer) -> Result<Outcome> {
    use netidx_admin::{
        admin_ops::delegation::{ClusterPropagation, add_parent as do_add_parent},
        paths,
        plan::resolve_admin_server_addr,
    };
    let rpath = paths::discover_resolver_config()?;
    let parent = loop {
        let s = ans.text(Field::ParentAddr, None, None, true).await?.unwrap_or_default();
        match resolve_admin_server_addr(&s) {
            Ok(a) => break a,
            Err(_) if ans.interactive() => ans.warn(&format!(
                "{s:?} is not a valid admin-server address — enter host or host:port"
            )),
            Err(e) => return Err(e),
        }
    };
    let path = ans.text(Field::DelegateSubtree, None, None, true).await?.unwrap_or_default();
    let out = do_add_parent(ans, &rpath, parent, &path).await?;
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
            quiet: false,
        }
    }
}
