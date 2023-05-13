use anyhow::{Context, Result};
use daemonize::Daemonize;
use futures::future;
use netidx::resolver_server::{config::Config, Server};
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
    let config = Config::load(params.config.clone())
        .context("failed to load resolver server config")?;
    let member = &config.member_servers[params.id];
    if !params.foreground {
        let mut file = member.pid_file.clone();
        file.push_str(&format!("{}.pid", params.id));
        Daemonize::new().pid_file(file).start().context("failed to daemonize")?;
    }
    tokio_run(config, params)
}
