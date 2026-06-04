use anyhow::{Context, Result};
use daemonize::Daemonize;
use netidx::{
    config::Config,
    publisher::{BindCfg, DesiredAuth},
};
use clap::Args;
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
    /// don't daemonize
    #[arg(short, long)]
    foreground: bool,
    /// write the pid to file
    #[arg(long)]
    pid_file: Option<PathBuf>,
}

#[tokio::main]
async fn tokio_run(
    cfg: Config,
    auth: DesiredAuth,
    params: ServerParams,
) -> Result<()> {
    let server = Server::new(cfg, auth, params).await.context("activation startup")?;
    server.run().await.context("activation")
}

pub(super) fn run(cfg: Config, auth: DesiredAuth, params: Params) -> Result<()> {
    env_logger::init();
    if !params.foreground {
        let mut d = Daemonize::new();
        if let Some(pid_file) = params.pid_file.as_ref() {
            d = d.pid_file(pid_file);
        }
        d.start().context("failed to daemonize")?
    }
    let server_params =
        ServerParams { bind: params.bind, units_dir: params.units };
    tokio_run(cfg, auth, server_params)
}
