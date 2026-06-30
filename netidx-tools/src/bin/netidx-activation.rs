//! Windows background activation supervisor.
//!
//! On Windows it is built as a GUI-subsystem binary
//! (`#![windows_subsystem = "windows"]`) so the per-user logon Scheduled Task
//! can start it with NO console window — regardless of whether the default
//! terminal is the classic console host or Windows Terminal (under which the
//! old `ShowWindow(SW_HIDE)` hack could not reach the window). On unix it runs
//! the supervisor in the foreground (the same engine as
//! `netidx activation --foreground`).
//!
//! Built on every platform so `cargo install netidx-tools` always produces it
//! — on Windows it must sit next to `netidx.exe` for the logon task. `netidx
//! activation` stays the uniform entry point: embedded on unix, delegating to
//! this binary on Windows when not run with `--foreground`.
#![cfg_attr(windows, windows_subsystem = "windows")]

use anyhow::Result;
use clap::Parser;
use netidx_tools_core::ClientParams;

// Share the supervisor entry with the `netidx` binary verbatim — no source
// duplication. `run`/`spawn_background_supervisor` are unused here (this
// binary only needs `run_supervisor`), hence the dead_code allowance scoped
// to this include.
#[allow(dead_code)]
#[path = "../activation.rs"]
mod activation;

#[derive(Parser, Debug)]
#[command(
    name = "netidx-activation",
    about = "netidx activation supervisor (standalone; windowless on Windows, \
             foreground on unix). `netidx activation` is the usual entry point."
)]
struct Opt {
    #[command(flatten)]
    common: ClientParams,
    #[command(flatten)]
    params: activation::Params,
}

fn main() -> Result<()> {
    let opt = Opt::parse();
    init_logging();
    let (cfg, auth) = opt.common.load();
    activation::run_supervisor(cfg, auth, opt.params)
}

/// On Windows this binary has no console (GUI subsystem), so env_logger's
/// default stderr goes nowhere — append to a log file under
/// `%LOCALAPPDATA%\netidx`. If the file can't be opened, fall back to the
/// default initializer (harmless when there's no console).
#[cfg(windows)]
fn init_logging() {
    use std::path::PathBuf;
    let base = std::env::var_os("LOCALAPPDATA")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join("netidx");
    let _ = std::fs::create_dir_all(&dir);
    let log = dir.join("activation.log");
    match std::fs::OpenOptions::new().create(true).append(true).open(&log) {
        Ok(file) => {
            let _ = env_logger::Builder::from_default_env()
                .target(env_logger::Target::Pipe(Box::new(file)))
                .try_init();
        }
        Err(_) => {
            let _ = env_logger::try_init();
        }
    }
}

#[cfg(not(windows))]
fn init_logging() {
    env_logger::init();
}
