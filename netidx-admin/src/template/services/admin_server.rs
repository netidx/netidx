//! Admin-server service template: an activation unit that runs
//! `<netidx> admin server run -c <config> -f`.
//!
//! Mirrors [`crate::template::services::id_map`] in shape. The admin
//! server answers discovery/info queries (and signs CSRs on the CA
//! host); it must run unattended, so it's supervised by the activation
//! supervisor like the resolver and id-map daemons.

use crate::activation::{ProcessCfgBuilder, Unit, UnitBuilder};
use anyhow::Result;
use std::path::PathBuf;

/// Parameters for [`unit`].
#[derive(Debug, Clone)]
pub struct AdminServerServiceParams {
    /// Path to the `netidx` binary the unit will exec. Resolved at the
    /// CLI layer via `std::env::current_exe()` so the unit runs the
    /// same binary that wrote it.
    pub netidx_binary: PathBuf,
    /// Path to the admin-server config (`admin-server.json`).
    pub config: PathBuf,
}

/// Render the admin-server activation unit. `-f` keeps it in the
/// foreground under the supervisor (same reasoning as the id-map unit:
/// a backgrounding daemon's parent exits 0 and the supervisor loops).
pub fn unit(p: &AdminServerServiceParams) -> Result<Unit> {
    let args: Vec<String> = vec![
        "admin".to_string(),
        "component".to_string(),
        "server".to_string(),
        "run".to_string(),
        "-c".to_string(),
        p.config.to_string_lossy().into_owned(),
        "-f".to_string(),
    ];
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
    fn runs_admin_server_in_foreground() {
        let u = unit(&AdminServerServiceParams {
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
            config: PathBuf::from("/etc/netidx/admin-server.json"),
        })
        .unwrap();
        assert_eq!(
            u.process.args,
            [
                "admin",
                "component",
                "server",
                "run",
                "-c",
                "/etc/netidx/admin-server.json",
                "-f"
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>(),
        );
    }
}
