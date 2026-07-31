//! `netidx admin component server …` — run the admin-server daemon.
//!
//! The daemon itself lives in `netidx_admin`; this module is the
//! CLI shell that runs it. Standing an admin server *up* (issuing its serving
//! cert, writing its config, dropping its unit) lives in the library —
//! `netidx_admin::plan::server_setup::setup_server` — shared by `ca init` and
//! the install flows.

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use netidx_admin::paths;
use std::path::PathBuf;

#[derive(Subcommand, Debug)]
pub(crate) enum Cmd {
    /// run the admin server (answers discovery/info queries and, on the
    /// CA host, signs CSRs received over TLS)
    Run(RunArgs),
}

#[derive(Args, Debug)]
pub(crate) struct RunArgs {
    /// Path to `admin-server.json`. Defaults to the standard search
    /// order (user config dir, then the system location).
    #[arg(short, long)]
    pub config: Option<PathBuf>,
    /// Don't daemonize (run in the foreground).
    #[arg(short, long)]
    #[allow(dead_code)]
    pub foreground: bool,
}

pub(crate) fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Run(p) => run_server(p),
    }
}

fn run_server(p: RunArgs) -> Result<()> {
    env_logger::init();
    let cfg_path = match p.config {
        Some(c) => c,
        None => paths::discover_admin_server_config()?,
    };
    let rt = tokio::runtime::Runtime::new().context("starting tokio runtime")?;
    rt.block_on(netidx_admin::serve(cfg_path))
}
