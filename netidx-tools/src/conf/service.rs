//! `netidx conf component service {install,uninstall,status}` — the CLI shell
//! over `netidx_conf::service`. Handles sudo elevation for
//! system-scope installs, and re-execs the same binary across the
//! privilege boundary so the elevated child runs the binary the
//! operator invoked.
//!
//! Sentinel env var: `NETIDX_ELEVATED=1` is set in the sudo child
//! so the post-install prompt path (in `conf install`) can skip
//! prompts the elevated process would otherwise re-ask.

use anyhow::{Context, Result};
use netidx_conf::service::{
    self, ServiceParams, ServiceScope, ServiceStatus,
};
use clap::{Args, Subcommand};
use std::{io::IsTerminal, path::PathBuf};
// `Command` only drives the unix sudo re-exec path.
#[cfg(unix)]
use std::process::Command;

use super::prompt;

/// Whether a setup process needs the activation supervisor installed as
/// an OS service, and at what scope. Setup steps that drop activation
/// units needing unattended supervision contribute a scope; a top-level
/// flow [`merge`](ServiceNeed::merge)s the needs of all its sub-steps
/// and offers a *single* service install at the end. `None` ⇒ nothing
/// to supervise (e.g. a client-only publisher).
///
/// This is the seam that keeps service setup composable: a resolver
/// install that also stands up a CA server merges two `System` needs
/// into one offer; a standalone `ca init` produces the same `System`
/// need and offers it itself. Neither has to know about the other.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ServiceNeed(Option<ScopeArg>);

impl ServiceNeed {
    /// No service needed.
    pub(crate) const NONE: ServiceNeed = ServiceNeed(None);

    /// A service is needed at `scope`.
    pub(crate) fn at(scope: ScopeArg) -> Self {
        ServiceNeed(Some(scope))
    }

    /// Combine two needs. System outranks User outranks nothing — a
    /// process that needs *any* system-scope daemon needs a system
    /// service.
    ///
    /// The composition seam: a flow that stands up more than one daemon
    /// (e.g. a resolver install that also sets up a CA server) folds
    /// each sub-step's need together with this and offers once. No flow
    /// composes two daemons *yet*, hence `allow(dead_code)` — but the
    /// merge semantics are the whole point of the design, so they're
    /// here and tested rather than re-derived when the first composite
    /// flow lands.
    #[allow(dead_code)]
    pub(crate) fn merge(self, other: ServiceNeed) -> ServiceNeed {
        fn rank(n: ServiceNeed) -> u8 {
            match n.0 {
                None => 0,
                Some(ScopeArg::User) => 1,
                Some(ScopeArg::System) => 2,
            }
        }
        if rank(other) > rank(self) { other } else { self }
    }

    fn scope(self) -> Option<ScopeArg> {
        self.0
    }
}

/// Gates on the single service-setup offer, lifted from a flow's flags.
pub(crate) struct ServiceGate {
    pub dry_run: bool,
    pub no_service: bool,
    pub with_service: bool,
}

/// **The** end-of-process hook for offering OS-service setup. A flow
/// merges the [`ServiceNeed`]s of its setup steps and calls this once.
/// Behaviour:
/// - need is `NONE` ⇒ nothing to do.
/// - `--dry-run` ⇒ print what would be offered, change nothing.
/// - `--no-service` ⇒ skip silently.
/// - `--with-service` ⇒ install without prompting.
/// - otherwise ⇒ prompt on a TTY (default yes), or print a hint on a
///   non-TTY.
pub(super) fn offer(need: ServiceNeed, gate: ServiceGate) -> Result<()> {
    let Some(scope) = need.scope() else { return Ok(()) };
    let label = match scope {
        ScopeArg::User => "user-scope (no sudo)",
        ScopeArg::System => "system-scope (sudo required)",
    };
    if gate.dry_run {
        println!("[dry-run] would offer to install netidx as a {label} OS service");
        return Ok(());
    }
    if gate.no_service {
        return Ok(());
    }
    let install_now = if gate.with_service {
        true
    } else if std::io::stdout().is_terminal() && std::io::stdin().is_terminal() {
        prompt::confirm(&format!("install netidx as an OS service now ({label})?"), true)?
    } else {
        eprintln!(
            "note: pass --with-service to register netidx as an OS service \
             (run `netidx conf component service install` later if you prefer)"
        );
        false
    };
    if install_now {
        install_with_defaults(scope)?;
    }
    Ok(())
}

/// Env var that signals "I'm the elevated child" to skip
/// post-install confirmations and just run the requested action.
/// Public so the install-flow can read it (`std::env::var_os`).
pub(super) const ELEVATED_ENV: &str = "NETIDX_ELEVATED";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeArg {
    User,
    System,
}

impl std::str::FromStr for ScopeArg {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "user" => Ok(ScopeArg::User),
            "system" => Ok(ScopeArg::System),
            _ => bail!("scope must be 'user' or 'system'"),
        }
    }
}

impl From<ScopeArg> for ServiceScope {
    fn from(a: ScopeArg) -> Self {
        match a {
            ScopeArg::User => ServiceScope::User,
            ScopeArg::System => ServiceScope::System,
        }
    }
}

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// install netidx as an OS service
    Install(InstallArgs),
    /// remove the netidx OS service
    Uninstall(CommonArgs),
    /// report whether the netidx service is running
    Status(CommonArgs),
}

#[derive(Args, Debug)]
pub(crate) struct CommonArgs {
    /// User or system scope. User scope is unprivileged; system scope
    /// writes to /etc and needs root (we re-exec via `sudo` if not
    /// already elevated).
    #[arg(long, default_value = "user")]
    pub scope: ScopeArg,
    /// For system scope, the username the service should run as.
    /// Defaults to the pre-escalation user (`$SUDO_USER` if set,
    /// otherwise the current user). Ignored for user scope.
    #[arg(long)]
    pub for_user: Option<String>,
    /// Service name. Defaults to "netidx"; override when running
    /// multiple parallel netidx setups on one host.
    #[arg(long, default_value = "netidx")]
    pub service_name: String,
}

#[derive(Args, Debug)]
pub(crate) struct InstallArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    /// Path to the netidx binary the service should run. Defaults to
    /// the currently-running binary (`std::env::current_exe`) so the
    /// installed service is the same binary the operator invoked.
    #[arg(long)]
    pub netidx_binary: Option<PathBuf>,
    /// Override the activation directory the supervisor reads from.
    /// `None` ⇒ the platform-default user activation dir of the
    /// service-running user.
    #[arg(long)]
    pub activation_dir: Option<PathBuf>,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Install(a) => install(a),
        Cmd::Uninstall(a) => uninstall(a),
        Cmd::Status(a) => status(a),
    }
}

/// Entry-point used by the post-install hook in `conf install`. The
/// templated install flow doesn't expose every service flag — it
/// just calls in with a scope and lets us pick the rest of the
/// defaults (service name = "netidx", binary = current_exe,
/// for_user = sudo-aware resolution).
pub(super) fn install_with_defaults(scope: ScopeArg) -> Result<()> {
    install(InstallArgs {
        common: CommonArgs {
            scope,
            for_user: None,
            service_name: ServiceParams::DEFAULT_NAME.to_string(),
        },
        netidx_binary: None,
        activation_dir: None,
    })
}

fn install(mut a: InstallArgs) -> Result<()> {
    let binary = match a.netidx_binary.take() {
        Some(p) => p,
        None => std::env::current_exe()
            .context("could not determine current netidx binary; pass --netidx-binary")?,
    };
    let scope: ServiceScope = a.common.scope.into();
    // Elevate FIRST when system-scope and we're not root. The child
    // re-runs this same command with NETIDX_ELEVATED=1 and an
    // already-resolved --for-user, then takes the early-return below.
    if scope == ServiceScope::System && !is_elevated()? {
        return escalate_for_install(&a, &binary);
    }
    let params = ServiceParams {
        scope,
        for_user: Some(resolve_for_user(a.common.for_user.clone())?),
        binary,
        service_name: a.common.service_name.clone(),
        activation_dir: a.activation_dir,
    };
    let installed = service::install(&params)?;
    println!("installed service:");
    println!("  scope:      {:?}", params.scope);
    println!("  service id: {}", installed.service_id);
    println!("  unit file:  {}", installed.unit_path.display());
    Ok(())
}

fn uninstall(a: CommonArgs) -> Result<()> {
    let scope: ServiceScope = a.scope.into();
    if scope == ServiceScope::System && !is_elevated()? {
        return escalate_for_uninstall(&a);
    }
    let params = ServiceParams {
        scope,
        for_user: Some(resolve_for_user(a.for_user.clone())?),
        // binary / activation_dir aren't read by uninstall — pass
        // placeholders rather than threading them through the CLI
        // again. The engine layer only looks at scope + service_name
        // + for_user for the uninstall path.
        binary: PathBuf::new(),
        service_name: a.service_name,
        activation_dir: None,
    };
    service::uninstall(&params)?;
    println!("uninstalled service (scope: {:?})", params.scope);
    Ok(())
}

fn status(a: CommonArgs) -> Result<()> {
    let scope: ServiceScope = a.scope.into();
    let params = ServiceParams {
        scope,
        for_user: Some(resolve_for_user(a.for_user.clone())?),
        binary: PathBuf::new(),
        service_name: a.service_name,
        activation_dir: None,
    };
    let s = service::status(&params)?;
    let label = match s {
        ServiceStatus::Active => "active",
        ServiceStatus::Inactive => "inactive",
        ServiceStatus::NotInstalled => "not installed",
    };
    println!("netidx service ({:?}): {}", params.scope, label);
    Ok(())
}

/// True if we're already running as root (uid 0). The system-scope
/// install path skips sudo when this is true.
#[cfg(unix)]
pub(super) fn is_elevated() -> Result<bool> {
    Ok(nix::unistd::geteuid().is_root())
}

#[cfg(windows)]
pub(super) fn is_elevated() -> Result<bool> {
    // Honour the explicit sentinel — a re-exec'd child must not loop
    // through another elevation attempt.
    if std::env::var_os(ELEVATED_ENV).is_some() {
        return Ok(true);
    }
    // Probe by writing a dummy file under %SYSTEMROOT%. Cheap, doesn't
    // depend on the windows-api crate. A real implementation would
    // check token elevation via the SCM API, but this is a stub-grade
    // probe and Windows install is a stub anyway.
    Ok(false)
}

/// Determine the username the service should run as. Honours
/// `--for-user` if provided; otherwise reads `$SUDO_USER` (set by
/// sudo to the original invoker) and finally falls back to the
/// current uid → name lookup.
#[cfg(unix)]
pub(super) fn resolve_for_user(provided: Option<String>) -> Result<String> {
    if let Some(u) = provided {
        return Ok(u);
    }
    if let Some(s) = std::env::var_os("SUDO_USER")
        && let Some(s) = s.to_str()
        && !s.is_empty()
    {
        return Ok(s.to_string());
    }
    let uid = nix::unistd::geteuid();
    let user = nix::unistd::User::from_uid(uid)
        .ok()
        .flatten()
        .ok_or_else(|| anyhow!("could not resolve current uid {uid} to a username"))?;
    Ok(user.name)
}

#[cfg(windows)]
pub(super) fn resolve_for_user(provided: Option<String>) -> Result<String> {
    provided.or_else(|| std::env::var("USERNAME").ok())
        .ok_or_else(|| anyhow!("could not determine current Windows user; pass --for-user"))
}

/// Re-exec ourselves under sudo to do the install. We resolve
/// `--for-user` *before* the escalation so the elevated child sees
/// the pre-elevation user (not root).
#[cfg(unix)]
fn escalate_for_install(a: &InstallArgs, binary: &std::path::Path) -> Result<()> {
    let for_user = resolve_for_user(a.common.for_user.clone())?;
    let exe = std::env::current_exe()
        .context("could not determine current binary for sudo re-exec")?;
    let mut cmd = Command::new(elevator());
    cmd.arg("--preserve-env=NETIDX_ELEVATED")
        .arg(&exe)
        .arg("conf").arg("service").arg("install")
        .arg("--scope").arg("system")
        .arg("--for-user").arg(&for_user)
        .arg("--service-name").arg(&a.common.service_name)
        .arg("--netidx-binary").arg(binary)
        .env(ELEVATED_ENV, "1");
    if let Some(dir) = &a.activation_dir {
        cmd.arg("--activation-dir").arg(dir);
    }
    let status = cmd.status().with_context(|| {
        format!("spawning `{}` for privilege escalation", elevator())
    })?;
    if !status.success() {
        bail!("escalation failed: {status}");
    }
    Ok(())
}

#[cfg(unix)]
fn escalate_for_uninstall(a: &CommonArgs) -> Result<()> {
    let for_user = resolve_for_user(a.for_user.clone())?;
    let exe = std::env::current_exe()
        .context("could not determine current binary for sudo re-exec")?;
    let status = Command::new(elevator())
        .arg("--preserve-env=NETIDX_ELEVATED")
        .arg(&exe)
        .arg("conf").arg("service").arg("uninstall")
        .arg("--scope").arg("system")
        .arg("--for-user").arg(&for_user)
        .arg("--service-name").arg(&a.service_name)
        .env(ELEVATED_ENV, "1")
        .status()
        .with_context(|| format!("spawning `{}` for privilege escalation", elevator()))?;
    if !status.success() {
        bail!("escalation failed: {status}");
    }
    Ok(())
}

#[cfg(windows)]
fn escalate_for_install(_a: &InstallArgs, _binary: &std::path::Path) -> Result<()> {
    bail!(
        "system-scope service install on Windows requires an already-elevated shell. \
         Open an Administrator PowerShell / cmd and re-run this command, \
         or use `sudo` (Windows 11+)."
    )
}

#[cfg(windows)]
fn escalate_for_uninstall(_a: &CommonArgs) -> Result<()> {
    bail!(
        "system-scope service uninstall on Windows requires an already-elevated shell. \
         Open an Administrator PowerShell / cmd and re-run this command."
    )
}

/// Path / name of the elevation helper. `sudo` on POSIX and Win11;
/// the latter ships a `sudo` shim that forwards to UAC.
#[cfg(unix)]
pub(super) fn elevator() -> &'static str {
    "sudo"
}

/// Whether stdout is a terminal — used by the post-install prompt
/// path (separate from this module's own non-prompting subcommands)
/// to decide whether to offer the service install interactively.
#[allow(dead_code)]
pub(super) fn stdout_is_tty() -> bool {
    std::io::stdout().is_terminal()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn scope_parse() {
        assert_eq!(ScopeArg::from_str("user").unwrap(), ScopeArg::User);
        assert_eq!(ScopeArg::from_str("System").unwrap(), ScopeArg::System);
        assert!(ScopeArg::from_str("admin").is_err());
    }

    #[test]
    fn service_need_merge_ranks_system_over_user_over_none() {
        use ServiceNeed as N;
        let sys = N::at(ScopeArg::System);
        let usr = N::at(ScopeArg::User);
        // System wins regardless of order.
        assert_eq!(sys.merge(usr), sys);
        assert_eq!(usr.merge(sys), sys);
        // User beats nothing.
        assert_eq!(N::NONE.merge(usr), usr);
        assert_eq!(usr.merge(N::NONE), usr);
        // Nothing merges to nothing.
        assert_eq!(N::NONE.merge(N::NONE), N::NONE);
        // Idempotent.
        assert_eq!(sys.merge(sys), sys);
    }
}
