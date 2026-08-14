use anyhow::{Context, Result};
use clap::Args;
#[cfg(unix)]
use daemonize::Daemonize;
use netidx::{
    config::Config,
    publisher::{BindCfg, DesiredAuth},
};
use netidx_activation::runtime::{Server, ServerParams};
use std::path::PathBuf;

#[derive(Args, Debug)]
pub(crate) struct Params {
    /// configure the bind address e.g. local, 192.168.0.0/16
    #[arg(short, long)]
    bind: Option<BindCfg>,
    /// path to directory containing the unit files to manage
    #[arg(short, long)]
    units: Option<PathBuf>,
    /// don't background the supervisor (unix: don't daemonize; Windows:
    /// run the supervisor in this console instead of delegating to the
    /// windowless `netidx-activation.exe` background binary)
    #[arg(short, long)]
    foreground: bool,
    /// write the pid to file (unix only)
    #[cfg_attr(not(unix), allow(dead_code))]
    #[arg(long)]
    pid_file: Option<PathBuf>,
}

#[tokio::main]
async fn tokio_run(cfg: Config, auth: DesiredAuth, params: ServerParams) -> Result<()> {
    let server = Server::new(cfg, auth, params).await.context("activation startup")?;
    server.run().await.context("activation")
}

/// Run the supervisor in *this* process and block until it exits. Shared by
/// the unix daemon/foreground path and the Windows background binary
/// (`netidx-activation.exe`). The caller initializes logging first — to a
/// console (unix / `--foreground`) or to a file (the windowless Windows
/// binary, which has no console).
pub(crate) fn run_supervisor(
    cfg: Config,
    auth: DesiredAuth,
    params: Params,
) -> Result<()> {
    let server_params = ServerParams { bind: params.bind, units_dir: params.units };
    tokio_run(cfg, auth, server_params)
}

/// The `netidx activation` subcommand.
///
/// - unix: run the supervisor here, daemonizing unless `--foreground`.
/// - Windows: `--foreground` runs the supervisor in this console (for
///   debugging); otherwise delegate to `netidx-activation.exe` — a
///   GUI-subsystem sibling that never allocates a console — so no terminal
///   window appears regardless of the default terminal (conhost vs Windows
///   Terminal). The per-user logon Scheduled Task points at that binary
///   directly; this delegation keeps `netidx activation` a uniform entry
///   point across platforms.
pub(crate) fn run(cfg: Config, auth: DesiredAuth, params: Params) -> Result<()> {
    env_logger::init();
    #[cfg(unix)]
    {
        if !params.foreground {
            let mut d = Daemonize::new();
            if let Some(pid_file) = params.pid_file.as_ref() {
                d = d.pid_file(pid_file);
            }
            d.start().context("failed to daemonize")?
        }
        run_supervisor(cfg, auth, params)
    }
    #[cfg(windows)]
    {
        if params.foreground {
            run_supervisor(cfg, auth, params)
        } else {
            // The background binary re-derives cfg/auth from the forwarded
            // args, so the copies we loaded here are unused.
            let _ = (cfg, auth);
            spawn_background_supervisor()
        }
    }
}

/// Windows: launch the GUI-subsystem `netidx-activation.exe` sibling,
/// detached and windowless, forwarding the user's arguments (minus the
/// `activation` subcommand token), then return — the analog of unix
/// daemonization.
#[cfg(windows)]
fn spawn_background_supervisor() -> Result<()> {
    use std::{ffi::OsString, os::windows::process::CommandExt, process::Command};
    use windows::Win32::System::Threading::{CREATE_NO_WINDOW, DETACHED_PROCESS};
    let exe = std::env::current_exe()
        .context("locating the running netidx.exe")?
        .with_file_name("netidx-activation.exe");
    if !exe.exists() {
        anyhow::bail!(
            "background supervisor binary not found at {} — reinstall netidx so \
             netidx-activation.exe ships alongside netidx.exe, or run with \
             --foreground",
            exe.display()
        );
    }
    // `netidx --config c activation --units u` -> `netidx-activation.exe
    // --config c --units u`: forward everything but the subcommand token.
    let mut forwarded: Vec<OsString> = Vec::new();
    let mut dropped = false;
    for a in std::env::args_os().skip(1) {
        if !dropped && a == "activation" {
            dropped = true;
            continue;
        }
        forwarded.push(a);
    }
    Command::new(&exe)
        .args(&forwarded)
        .creation_flags(CREATE_NO_WINDOW.0 | DETACHED_PROCESS.0)
        .spawn()
        .with_context(|| format!("launching {}", exe.display()))?;
    println!("netidx activation supervisor started in the background");
    Ok(())
}
