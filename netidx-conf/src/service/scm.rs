//! Windows SCM stub for the netidx activation service.
//!
//! A real implementation needs to drive `sc.exe` (or the Service
//! Control Manager API directly) and handle "log on as a service"
//! rights for per-user service accounts, which is a different shape
//! than systemd / launchd. For v1 we ship a clear "not yet
//! implemented" error and point the operator at the manual recipe.

use super::{InstalledService, ServiceParams, ServiceStatus};
use anyhow::Result;

pub(super) fn install(_p: &ServiceParams) -> Result<InstalledService> {
    bail!(
        "OS service install is not yet implemented on Windows. \
         Run `netidx activation` from a startup script, or register \
         it manually with `sc.exe create`. Track the gap at the \
         project's issue tracker."
    )
}

pub(super) fn uninstall(_p: &ServiceParams) -> Result<()> {
    bail!("OS service uninstall is not yet implemented on Windows")
}

pub(super) fn status(_p: &ServiceParams) -> Result<ServiceStatus> {
    // Without an install path we can't usefully check status; report
    // not-installed so the CLI's status command doesn't claim
    // anything misleading.
    Ok(ServiceStatus::NotInstalled)
}
