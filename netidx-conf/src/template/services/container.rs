//! Container service template: an activation unit that runs
//! `<netidx> container [args]`.
//!
//! Defaults are tuned for the local-workstation case: no `--db`
//! (container picks `$XDG_DATA_HOME/netidx/container/db`), no
//! `--bind` (container picks its own default). The caller is expected
//! to fill in `api_path` with a netidx path appropriate to whatever
//! resolver this machine talks to — the engine layer does **not**
//! pick a default api path itself (it can't reach the operator's
//! client config from here without coupling).

use crate::activation::{ProcessCfgBuilder, Unit, UnitBuilder};
use anyhow::Result;
use arcstr::ArcStr;
use std::path::PathBuf;

/// Parameters for [`unit`].
#[derive(Debug, Clone)]
pub struct ContainerServiceParams {
    /// Path to the `netidx` binary the unit will exec. The CLI
    /// resolves this via `std::env::current_exe()` so the unit runs
    /// the same binary that wrote it; callers driving the engine
    /// directly pick whatever they want.
    pub netidx_binary: PathBuf,
    /// Netidx path the container should publish its API at. Always
    /// supplied — the engine does not invent one.
    pub api_path: ArcStr,
    /// Override the on-disk db directory. `None` ⇒ container's own
    /// default (`$XDG_DATA_HOME/netidx/container/db`).
    pub db: Option<PathBuf>,
    /// Pass `--compress` to enable on-disk compression. Currently a
    /// no-op at the container layer (sled 0.34's compression feature
    /// conflicts with netidx-archive's zstd version); the flag is
    /// plumbed so unit files don't have to be regenerated when the
    /// container backend is upgraded.
    pub compress: bool,
    /// Pass `--bind <cfg>` to the container. `None` ⇒ container's
    /// own default.
    pub bind: Option<ArcStr>,
}

/// Render the container unit. The unit's trigger is `OnStart` — the
/// container is a long-running publisher, not an on-access service.
pub fn unit(p: &ContainerServiceParams) -> Result<Unit> {
    let mut args: Vec<String> = vec!["container".to_string()];
    args.push("--api-path".to_string());
    args.push(p.api_path.to_string());
    if let Some(db) = &p.db {
        args.push("--db".to_string());
        args.push(db.to_string_lossy().into_owned());
    }
    if p.compress {
        args.push("--compress".to_string());
    }
    if let Some(bind) = &p.bind {
        args.push("--bind".to_string());
        args.push(bind.to_string());
    }
    let process = ProcessCfgBuilder::default()
        .exe(p.netidx_binary.to_string_lossy().into_owned())
        .args(args)
        .build()?;
    Ok(UnitBuilder::default().process(process).build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activation::Trigger;

    #[test]
    fn minimal_args() {
        let u = unit(&ContainerServiceParams {
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
            api_path: ArcStr::from("/container/api"),
            db: None,
            compress: false,
            bind: None,
        })
        .unwrap();
        assert!(matches!(u.trigger, Trigger::OnStart));
        assert_eq!(u.process.exe, "/usr/local/bin/netidx");
        assert_eq!(
            u.process.args,
            vec!["container", "--api-path", "/container/api"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn full_args() {
        let u = unit(&ContainerServiceParams {
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
            api_path: ArcStr::from("/local/container/api"),
            db: Some(PathBuf::from("/var/lib/netidx/db")),
            compress: true,
            bind: Some(ArcStr::from("local")),
        })
        .unwrap();
        // Argument order is stable so the rendered unit file diffs
        // cleanly across re-runs.
        assert_eq!(
            u.process.args,
            vec![
                "container",
                "--api-path",
                "/local/container/api",
                "--db",
                "/var/lib/netidx/db",
                "--compress",
                "--bind",
                "local",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>(),
        );
    }
}
