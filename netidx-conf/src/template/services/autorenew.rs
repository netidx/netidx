//! Autorenew service template: an activation unit that runs
//! `<netidx> conf ca autorenew --keytab <file> -f` on the CA host —
//! automatic approval of verified renewals, authorized by a dedicated
//! empty-policy admin slot whose password lives in the keytab.

use crate::activation::{ProcessCfgBuilder, Unit, UnitBuilder};
use anyhow::Result;
use std::path::PathBuf;

/// Parameters for [`unit`].
#[derive(Debug, Clone)]
pub struct AutorenewServiceParams {
    /// Path to the `netidx` binary the unit will exec.
    pub netidx_binary: PathBuf,
    /// Path to the keytab holding the autorenew slot's password.
    pub keytab: PathBuf,
}

/// Render the autorenew activation unit.
pub fn unit(p: &AutorenewServiceParams) -> Result<Unit> {
    let args: Vec<String> = vec![
        "conf".to_string(),
        "ca".to_string(),
        "autorenew".to_string(),
        "--keytab".to_string(),
        p.keytab.to_string_lossy().into_owned(),
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
    fn runs_autorenew_with_the_keytab() {
        let u = unit(&AutorenewServiceParams {
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
            keytab: PathBuf::from("/home/x/.config/netidx/autorenew.keytab"),
        })
        .unwrap();
        assert_eq!(
            u.process.args,
            [
                "conf",
                "ca",
                "autorenew",
                "--keytab",
                "/home/x/.config/netidx/autorenew.keytab",
                "-f"
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>(),
        );
    }
}
