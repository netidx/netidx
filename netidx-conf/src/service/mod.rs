//! OS service install for `netidx activation`.
//!
//! The activation supervisor is great at running netidx daemons, but
//! it needs something to *run it* at boot. This module writes the
//! native OS init-system unit file and registers it with the local
//! service manager. The CLI (`netidx conf service install`) layers
//! interactive prompting and `sudo` escalation on top.
//!
//! Per-platform plans:
//!
//! - **Linux (systemd).** User scope writes
//!   `${XDG_CONFIG_HOME:-~/.config}/systemd/user/<name>.service` and
//!   runs `systemctl --user daemon-reload && enable --now`. System
//!   scope writes a templated `/etc/systemd/system/<name>@.service`
//!   with `User=%i`, then `systemctl daemon-reload && enable --now
//!   <name>@<for_user>`. The `@` form lets one unit file serve any
//!   number of unprivileged service accounts.
//!
//! - **macOS (launchd).** User scope writes
//!   `~/Library/LaunchAgents/<label>.plist` and bootstraps it with
//!   `launchctl bootstrap gui/<uid>`. System scope writes
//!   `/Library/LaunchDaemons/<label>.plist` with the `UserName` key
//!   set to `for_user`, and `launchctl bootstrap system`.
//!
//! - **Windows (SCM).** Stub: the install function returns an error
//!   pointing the operator at the manual `sc.exe` recipe. A real
//!   implementation needs to handle "log on as a service" rights for
//!   per-user accounts (and the password capture that goes with it),
//!   which is a different shape than systemd / launchd.
//!
//! The unit-file *content* is pure (it's just a formatted string)
//! and is what's covered by tests; the actual file write and the
//! `systemctl` / `launchctl` invocation are not — they need a real
//! init system to be meaningful, which a `cargo test` run doesn't
//! have.

use anyhow::Result;
use std::path::PathBuf;

#[cfg(target_os = "linux")]
mod systemd;
#[cfg(target_os = "macos")]
mod launchd;
#[cfg(target_os = "windows")]
mod scm;

/// Whether the service runs at user scope (per-login) or system
/// scope (boot-triggered, root-owned).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceScope {
    User,
    System,
}

/// Inputs for [`install`] / [`uninstall`] / [`status`]. The CLI
/// layer is responsible for resolving `for_user` and absolutising
/// `binary` before calling in.
#[derive(Debug, Clone)]
pub struct ServiceParams {
    /// User or system scope.
    pub scope: ServiceScope,
    /// Username the service should run as. For [`ServiceScope::User`]
    /// this is informational and the OS picks the user from the
    /// session; for [`ServiceScope::System`] it's the value that
    /// goes into `User=` (systemd) / `UserName` (launchd) /
    /// service-account fields. Defaults to the invoking user at the
    /// CLI layer.
    pub for_user: Option<String>,
    /// Absolute path to the `netidx` binary the service should
    /// `ExecStart`. Always supplied by the CLI from
    /// [`std::env::current_exe`] so the installed service runs the
    /// same binary the operator invoked.
    pub binary: PathBuf,
    /// Service / unit name. Defaults to `"netidx"`; overrideable so
    /// operators running multiple netidx setups on one host (a
    /// development scenario) can install distinct services.
    pub service_name: String,
    /// Override the activation directory the supervisor reads its
    /// unit files from. `None` ⇒ the activation supervisor's own
    /// default (which is `${XDG_CONFIG_HOME}/netidx/activation/` for
    /// the user that runs the service).
    pub activation_dir: Option<PathBuf>,
}

impl ServiceParams {
    /// Default service name — fixed so tooling and operators agree
    /// on what to look for.
    pub const DEFAULT_NAME: &'static str = "netidx";
}

/// Where an installed service's unit file lives.
#[derive(Debug, Clone)]
pub struct InstalledService {
    pub unit_path: PathBuf,
    /// The full name systemctl / launchctl / sc.exe address the
    /// service by — e.g. `netidx@alice` for a system-scope systemd
    /// install.
    pub service_id: String,
}

/// Whether the service is currently active. Platform-mapped onto the
/// underlying status query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceStatus {
    /// Unit file is installed and the service is running.
    Active,
    /// Unit file is installed but the service isn't running.
    Inactive,
    /// No unit file installed for this name + scope.
    NotInstalled,
}

#[cfg(target_os = "linux")]
pub fn install(p: &ServiceParams) -> Result<InstalledService> {
    systemd::install(p)
}
#[cfg(target_os = "linux")]
pub fn uninstall(p: &ServiceParams) -> Result<()> {
    systemd::uninstall(p)
}
#[cfg(target_os = "linux")]
pub fn status(p: &ServiceParams) -> Result<ServiceStatus> {
    systemd::status(p)
}

#[cfg(target_os = "macos")]
pub fn install(p: &ServiceParams) -> Result<InstalledService> {
    launchd::install(p)
}
#[cfg(target_os = "macos")]
pub fn uninstall(p: &ServiceParams) -> Result<()> {
    launchd::uninstall(p)
}
#[cfg(target_os = "macos")]
pub fn status(p: &ServiceParams) -> Result<ServiceStatus> {
    launchd::status(p)
}

#[cfg(target_os = "windows")]
pub fn install(p: &ServiceParams) -> Result<InstalledService> {
    scm::install(p)
}
#[cfg(target_os = "windows")]
pub fn uninstall(p: &ServiceParams) -> Result<()> {
    scm::uninstall(p)
}
#[cfg(target_os = "windows")]
pub fn status(p: &ServiceParams) -> Result<ServiceStatus> {
    scm::status(p)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
pub fn install(_p: &ServiceParams) -> Result<InstalledService> {
    anyhow::bail!(
        "OS service install is only implemented on Linux, macOS, and Windows; \
         run `netidx activation` from your platform's init system manually"
    )
}
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
pub fn uninstall(_p: &ServiceParams) -> Result<()> {
    anyhow::bail!("OS service uninstall is not implemented on this platform")
}
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
pub fn status(_p: &ServiceParams) -> Result<ServiceStatus> {
    Ok(ServiceStatus::NotInstalled)
}
