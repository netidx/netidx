//! Id-map service template: an activation unit that runs
//! `<netidx> id-map serve --socket <p> --config <p>`.
//!
//! Mirrors [`crate::template::services::container`] in shape. The
//! engine doesn't invent a default socket / config path — the CLI
//! resolves those via `netidx_conf::id_map::user_id_map_socket()` /
//! `user_id_map_path()` so the workstation and standalone-resolver
//! templates can decide independently.

use crate::activation::{ProcessCfgBuilder, Unit, UnitBuilder};
use anyhow::Result;
use std::path::PathBuf;

/// Parameters for [`unit`].
#[derive(Debug, Clone)]
pub struct IdMapServiceParams {
    /// Path to the `netidx` binary the unit will exec. Defaults
    /// resolve at the CLI layer via `std::env::current_exe()` so the
    /// unit runs the same binary that wrote it.
    pub netidx_binary: PathBuf,
    /// Where the daemon will bind the unix socket the resolver
    /// connects to.
    pub socket: PathBuf,
    /// Path to the id-map JSON config the daemon will load and watch
    /// (SIGHUP-reload).
    pub config: PathBuf,
    /// Octal socket mode (e.g. 0o600). Default 0o600 keeps the
    /// socket per-user.
    pub socket_mode: Option<u32>,
}

/// Render the id-map activation unit. Trigger is `OnStart` — the
/// id-map daemon must already be up before the resolver accepts its
/// first TLS connection.
pub fn unit(p: &IdMapServiceParams) -> Result<Unit> {
    let mut args: Vec<String> = vec![
        "id-map".to_string(),
        "serve".to_string(),
        "--socket".to_string(),
        p.socket.to_string_lossy().into_owned(),
        "--config".to_string(),
        p.config.to_string_lossy().into_owned(),
    ];
    if let Some(m) = p.socket_mode {
        args.push("--socket-mode".to_string());
        args.push(format!("{m:o}"));
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
        let u = unit(&IdMapServiceParams {
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
            socket: PathBuf::from("/var/run/netidx/id-map.sock"),
            config: PathBuf::from("/etc/netidx/id-map.json"),
            socket_mode: None,
        })
        .unwrap();
        assert!(matches!(u.trigger, Trigger::OnStart));
        assert_eq!(
            u.process.args,
            vec![
                "id-map",
                "serve",
                "--socket",
                "/var/run/netidx/id-map.sock",
                "--config",
                "/etc/netidx/id-map.json",
            ]
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>(),
        );
    }

    #[test]
    fn socket_mode_emitted_in_octal() {
        let u = unit(&IdMapServiceParams {
            netidx_binary: PathBuf::from("/usr/local/bin/netidx"),
            socket: PathBuf::from("/tmp/s"),
            config: PathBuf::from("/tmp/c"),
            socket_mode: Some(0o660),
        })
        .unwrap();
        let i = u.process.args.iter().position(|a| a == "--socket-mode").unwrap();
        // Match the CLI's accepted octal form (no `0o` prefix, no
        // leading 0) — the daemon's parser handles all three but
        // the rendered unit should be human-friendly.
        assert_eq!(u.process.args[i + 1], "660");
    }
}
