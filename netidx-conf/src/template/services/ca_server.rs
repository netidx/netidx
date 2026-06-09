//! CA-server service template: an activation unit that runs
//! `<netidx> conf ca serve -c <config> -f`.
//!
//! Mirrors [`crate::template::services::id_map`] in shape. The CA
//! server signs CSRs received over TLS; it must run unattended, so it's
//! supervised by the activation supervisor like the resolver and
//! id-map daemons.

use crate::activation::{ProcessCfgBuilder, Unit, UnitBuilder};
use anyhow::Result;
use std::path::PathBuf;

/// Parameters for [`unit`].
#[derive(Debug, Clone)]
pub struct CaServerServiceParams {
    /// Path to the `netidx` binary the unit will exec. Resolved at the
    /// CLI layer via `std::env::current_exe()` so the unit runs the
    /// same binary that wrote it.
    pub netidx_binary: PathBuf,
    /// Path to the CA server config (`<ca-dir>/server.json`).
    pub config: PathBuf,
}

/// Render the CA-server activation unit. `-f` keeps it in the
/// foreground under the supervisor (same reasoning as the id-map unit:
/// a backgrounding daemon's parent exits 0 and the supervisor loops).
pub fn unit(p: &CaServerServiceParams) -> Result<Unit> {
    let args: Vec<String> = vec![
        "conf".to_string(),
        "ca".to_string(),
        "serve".to_string(),
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
    fn runs_ca_serve_in_foreground() {
        let u = unit(&CaServerServiceParams {
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
            config: PathBuf::from("/etc/netidx/ca/server.json"),
        })
        .unwrap();
        assert_eq!(
            u.process.args,
            ["conf", "ca", "serve", "-c", "/etc/netidx/ca/server.json", "-f"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
        );
    }
}
