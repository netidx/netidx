//! `netidx id-map serve` — the id-mapper daemon.
//!
//! Unix-only; mirrors the resolver-server's run shape (daemonize +
//! `tokio::main` adapter, foreground via `-f`, SIGTERM/Ctrl-C exit).
//! See `netidx-id-map` for the protocol and config.

use anyhow::{Context, Result};
use daemonize::Daemonize;
use log::info;
use netidx_conf::id_map as id_map_engine;
use netidx_id_map::runtime::{Server, ServerParams};
use clap::Args;
use std::path::PathBuf;
use tokio::signal::unix::{signal, SignalKind};

#[derive(Args, Debug)]
pub(crate) struct Params {
    /// Path to the unix socket the daemon will bind. Resolver
    /// connects here when its config has
    /// `id_map_type: Socket` + `id_map_command: <this path>`.
    #[arg(short, long)]
    socket: PathBuf,
    /// Path to the id-map JSON config.
    #[arg(short, long)]
    config: PathBuf,
    /// File mode applied to the bound socket. 0o600 keeps it
    /// per-user; widen to 0o660 (and chgrp afterward) for a shared
    /// service account.
    #[arg(long, default_value = "0600")]
    socket_mode: String,
    /// don't daemonize
    #[arg(short, long)]
    foreground: bool,
    /// write pid here when daemonized
    #[arg(long)]
    pid_file: Option<PathBuf>,
}

pub(crate) fn run(params: Params) -> Result<()> {
    env_logger::init();
    let mode = id_map_engine::parse_octal_mode(&params.socket_mode)?;
    // Resolve paths against the pre-daemon cwd. `daemonize` chdirs
    // the process to `/` before our tokio runtime starts; without
    // this step a relative `--config` would be read as `/id-map.json`
    // and a relative `--socket` bound as `/id-map.sock`. We
    // canonicalize the config (it must already exist — startup
    // reads it) but only absolutize the socket path (which the
    // daemon creates fresh).
    let config = params
        .config
        .canonicalize()
        .with_context(|| format!("canonicalizing config path {:?}", params.config))?;
    let socket = absolutize(&params.socket)
        .with_context(|| format!("resolving socket path {:?}", params.socket))?;
    if !params.foreground {
        let mut d = Daemonize::new();
        if let Some(p) = params.pid_file.as_ref() {
            // Same logic: daemonize writes the pid file after chdir,
            // so a relative pid path lands at /.
            let abs_pid = absolutize(p)
                .with_context(|| format!("resolving pid file path {p:?}"))?;
            d = d.pid_file(abs_pid);
        }
        d.start().context("daemonize")?;
    }
    tokio_run(ServerParams { socket, socket_mode: mode, config })
}

/// Make `path` absolute without requiring it to exist. Used for
/// the socket and pid-file paths where the daemon will create the
/// target fresh — `canonicalize` would fail with ENOENT.
fn absolutize(path: &std::path::Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        let cwd = std::env::current_dir().context("getting cwd")?;
        Ok(cwd.join(path))
    }
}

#[tokio::main]
async fn tokio_run(params: ServerParams) -> Result<()> {
    let server = Server::start(params).await.context("id-map daemon startup")?;
    info!("id-map daemon up");
    // Block on SIGINT/SIGTERM so the daemon stays alive when run in
    // the foreground; the activation supervisor sends SIGTERM on
    // shutdown.
    let mut sigterm =
        signal(SignalKind::terminate()).context("registering SIGTERM")?;
    let mut sigint =
        signal(SignalKind::interrupt()).context("registering SIGINT")?;
    tokio::select! {
        _ = sigterm.recv() => info!("id-map: SIGTERM"),
        _ = sigint.recv()  => info!("id-map: SIGINT"),
    }
    drop(server);
    Ok(())
}
