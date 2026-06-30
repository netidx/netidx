//! Background activation supervisor binary.
//!
//! On Windows it is built as a GUI-subsystem binary
//! (`#![windows_subsystem = "windows"]`) so the per-user logon Scheduled Task
//! can start it with NO console window — regardless of whether the default
//! terminal is the classic console host or Windows Terminal (under which the
//! old `ShowWindow(SW_HIDE)` hack could not reach the window). `netidx
//! activation` delegates here on Windows when not run with `--foreground`; the
//! logon task runs it directly.
//!
//! It is declared on every platform only because Cargo can't build a binary
//! for just one OS (rust-lang/cargo#3138, #9208) and `cargo install
//! netidx-tools` must drop `netidx-activation.exe` next to `netidx.exe` on
//! Windows. On unix it is unused — `netidx activation` is the supervisor there,
//! in-process — so the unix build is an inert stub that just points at it.
#![cfg_attr(windows, windows_subsystem = "windows")]

#[cfg(windows)]
use anyhow::Result;
#[cfg(windows)]
use clap::Parser;
#[cfg(windows)]
use netidx_tools_core::ClientParams;

// Share the supervisor entry with the `netidx` binary verbatim — no source
// duplication. `run`/`spawn_background_supervisor` are unused here (this binary
// only needs `run_supervisor`), hence the dead_code allowance scoped to this
// include.
#[cfg(windows)]
#[allow(dead_code)]
#[path = "../activation.rs"]
mod activation;

#[cfg(windows)]
#[derive(Parser, Debug)]
#[command(
    name = "netidx-activation",
    about = "netidx activation supervisor (Windows GUI-subsystem helper, started \
             by the logon task; use `netidx activation` normally)"
)]
struct Opt {
    #[command(flatten)]
    common: ClientParams,
    #[command(flatten)]
    params: activation::Params,
}

#[cfg(windows)]
fn main() -> Result<()> {
    let opt = Opt::parse();
    init_logging();
    let (cfg, auth) = opt.common.load();
    activation::run_supervisor(cfg, auth, opt.params)
}

/// Windows: this binary has no console (GUI subsystem), so env_logger's default
/// stderr goes nowhere — append to a log file under `%LOCALAPPDATA%\netidx`.
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

// On unix the supervisor runs in-process as `netidx activation`; this binary
// exists only so the Windows build/install produces it (see the header). Say so
// rather than silently doing nothing.
#[cfg(not(windows))]
fn main() {
    eprintln!(
        "netidx-activation is the Windows-only background activation supervisor \
         and is not used on unix — run `netidx activation` instead."
    );
    std::process::exit(2);
}
