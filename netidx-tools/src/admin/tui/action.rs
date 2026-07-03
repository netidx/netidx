//! Actions the TUI runs against the library, and the [`Outcome`] it shows when
//! one finishes.
//!
//! An [`Action`] is spawned on the tokio runtime with a fresh
//! [`TuiAnswerer`](super::answer::TuiAnswerer); every decision the op needs is
//! answered through the modals the answerer raises. The op's final `Result` is
//! sent back to the UI loop as [`UiRequest::OpDone`](super::answer::UiRequest).

use super::answer::TuiAnswerer;
use anyhow::Result;
use netidx_admin::{
    plan::install::{
        InstallCommon,
        publisher::{PublisherInput, run_publisher},
        resolver::{ResolverInput, run_resolver},
    },
    provenance::InstallRole,
    service::ServiceScope,
};

/// What the UI shows after an action completes.
pub(super) struct Outcome {
    pub(super) title: String,
    pub(super) lines: Vec<String>,
    /// Re-detect local installs after showing this (an install/uninstall
    /// changed on-disk state).
    pub(super) refresh_local: bool,
}

/// A unit of work the TUI runs on the runtime, driven by the answerer's modals.
pub(super) enum Action {
    /// Install a role. `dry_run` previews the plan without writing anything.
    Install { role: InstallRole, dry_run: bool },
}

impl Action {
    /// A short label for the progress header while the action runs.
    pub(super) fn label(&self) -> String {
        match self {
            Action::Install { role, dry_run } => {
                let verb = if *dry_run { "Previewing" } else { "Installing" };
                format!("{verb} {}", role.as_str())
            }
        }
    }
}

/// Run an action to completion on an owned answerer — the self-contained future
/// the UI loop drives (no borrows of UI state, so it needs no lifetime plumbing).
pub(super) async fn run_owned(mut ans: TuiAnswerer, action: Action) -> Result<Outcome> {
    run(&mut ans, action).await
}

/// Run an action to completion, raising modals through `ans` as it goes.
pub(super) async fn run(ans: &mut TuiAnswerer, action: Action) -> Result<Outcome> {
    match action {
        Action::Install { role, dry_run } => install(ans, role, dry_run).await,
    }
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
    anyhow::bail!("the workstation role is not supported on this platform")
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
    let mut lines = Vec::new();
    if dry_run {
        lines.push("Preview only — nothing was written.".to_string());
        if scope.is_some() {
            lines.push("A real install would then offer to register the OS service.".to_string());
        }
        Outcome { title: format!("{} preview", role.as_str()), lines, refresh_local: false }
    } else {
        lines.push("Install complete.".to_string());
        match scope {
            Some(ServiceScope::System) => lines.push(
                "Register the OS service (needs root):\n  \
                 sudo netidx admin component service install --scope system"
                    .to_string(),
            ),
            Some(ServiceScope::User) => lines.push(
                "Register the OS service:\n  \
                 netidx admin component service install --scope user"
                    .to_string(),
            ),
            None => {}
        }
        Outcome { title: format!("{} installed", role.as_str()), lines, refresh_local: true }
    }
}
