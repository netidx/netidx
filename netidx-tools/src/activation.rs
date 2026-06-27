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
pub(super) struct Params {
    /// configure the bind address e.g. local, 192.168.0.0/16
    #[arg(short, long)]
    bind: Option<BindCfg>,
    /// path to directory containing the unit files to manage
    #[arg(short, long)]
    units: Option<PathBuf>,
    /// don't background the supervisor (unix: don't daemonize; Windows:
    /// keep the console window visible)
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

/// Hide the console window of a Windows supervisor started in the
/// background (e.g. by the per-user logon Scheduled Task). There is no
/// daemonization on Windows; the supervisor runs in the user's session,
/// just without a visible window.
#[cfg(windows)]
fn hide_console_window() {
    use windows::Win32::{
        System::Console::GetConsoleWindow,
        UI::WindowsAndMessaging::{SW_HIDE, ShowWindow},
    };
    unsafe {
        let hwnd = GetConsoleWindow();
        if !hwnd.is_invalid() {
            let _ = ShowWindow(hwnd, SW_HIDE);
        }
    }
}

pub(super) fn run(cfg: Config, auth: DesiredAuth, params: Params) -> Result<()> {
    env_logger::init();
    if !params.foreground {
        #[cfg(unix)]
        {
            let mut d = Daemonize::new();
            if let Some(pid_file) = params.pid_file.as_ref() {
                d = d.pid_file(pid_file);
            }
            d.start().context("failed to daemonize")?
        }
        #[cfg(windows)]
        hide_console_window();
    }
    let server_params = ServerParams { bind: params.bind, units_dir: params.units };
    tokio_run(cfg, auth, server_params)
}
