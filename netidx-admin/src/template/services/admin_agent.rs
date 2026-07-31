//! Admin-agent service template: an activation unit that runs
//! `<netidx> admin component admin-agent run -f`.
//!
//! Installed on every client joined to an admin domain — publisher or
//! workstation, any data-plane auth. It keeps the host's admin-server list
//! and resolver addresses current, and renews certificates where there are
//! any. A host that runs an admin server needs no unit: that process does
//! both jobs itself.

use crate::activation::{ProcessCfgBuilder, Unit, UnitBuilder};
use anyhow::Result;
use std::path::PathBuf;

/// The unit name.
pub const UNIT: &str = "admin-agent";

/// Parameters for [`unit`].
#[derive(Debug, Clone)]
pub struct AgentServiceParams {
    /// Path to the `netidx` binary the unit will exec.
    pub netidx_binary: PathBuf,
}

/// Render the admin-agent activation unit. `-f` keeps it in the foreground
/// under the supervisor (same reasoning as the other daemon units).
pub fn unit(p: &AgentServiceParams) -> Result<Unit> {
    let args: Vec<String> = ["admin", "component", "admin-agent", "run", "-f"]
        .into_iter()
        .map(String::from)
        .collect();
    let process = ProcessCfgBuilder::default()
        .exe(p.netidx_binary.to_string_lossy().into_owned())
        .args(args)
        .build()?;
    Ok(UnitBuilder::default().process(process).build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runs_the_agent_in_foreground() {
        let u = unit(&AgentServiceParams {
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
        })
        .unwrap();
        assert_eq!(
            u.process.args,
            ["admin", "component", "admin-agent", "run", "-f"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
        );
    }
}
