//! Renewal-daemon service template: an activation unit that runs
//! `<netidx> conf renew run -f`.
//!
//! Installed on every TLS host (workstation, publisher, resolver, CA)
//! so certificate lifecycle is nobody's chore: the daemon queues
//! verified renewals before expiry and distributes the CRL — and no
//! application process needs write access to key material.

use crate::activation::{ProcessCfgBuilder, Unit, UnitBuilder};
use anyhow::Result;
use std::path::PathBuf;

/// Parameters for [`unit`].
#[derive(Debug, Clone)]
pub struct RenewServiceParams {
    /// Path to the `netidx` binary the unit will exec.
    pub netidx_binary: PathBuf,
}

/// Render the renewal-daemon activation unit. `-f` keeps it in the
/// foreground under the supervisor (same reasoning as the other
/// daemon units).
pub fn unit(p: &RenewServiceParams) -> Result<Unit> {
    let args: Vec<String> = ["conf", "renew", "run", "-f"]
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
    fn runs_renew_in_foreground() {
        let u = unit(&RenewServiceParams {
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
        })
        .unwrap();
        assert_eq!(
            u.process.args,
            ["conf", "renew", "run", "-f"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
        );
    }
}
