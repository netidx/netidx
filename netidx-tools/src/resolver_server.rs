use anyhow::{Context, Result};
use daemonize::Daemonize;
use futures::future;
use netidx::resolver_server::{
    config::{file, Config},
    Server,
};
use std::fs::File;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub(crate) struct Params {
    #[structopt(short = "c", long = "config", help = "path to the server config")]
    config: String,
    #[structopt(short = "f", long = "foreground", help = "don't daemonize")]
    foreground: bool,
    #[structopt(
        long = "delay-reads",
        help = "don't allow read clients until 1 writer ttl has passed"
    )]
    delay_reads: bool,
    #[structopt(
        long = "id",
        help = "index of the member server to run",
        default_value = "0"
    )]
    id: usize,
}

#[tokio::main]
async fn tokio_run(config: Config, params: Params) -> Result<()> {
    let _server = Server::new(config, params.delay_reads, params.id)
        .await
        .context("starting server")?;
    future::pending::<Result<()>>().await
}

pub(crate) fn run(params: Params) -> Result<()> {
    if params.foreground {
        let config = Config::load(params.config.clone())
            .context("failed to load resolver server config")?;
        tokio_run(config, params)
    } else {
        let config: file::Config = serde_json::from_reader(File::open(&params.config)?)?;
        let member = config.member_servers[params.id];
        Daemonize::new()
            .stdout(stdout)
            .stderr(stderr)
            .pid_file(member.pid_file.join(format!("{}.pid", params.id)))
            .start()
            .context("failed to daemonize")?;
        let config = Config::load(params.config.clone())
            .context("failed to load resolver server config")?;
        tokio_run(config, params)
    }
}
