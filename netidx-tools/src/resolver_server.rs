use anyhow::{Context, Result};
#[cfg(unix)]
use daemonize::Daemonize;
use futures::future;
#[cfg(unix)]
use netidx::resolver_server::config::file;
use netidx::resolver_server::{config::Config, Server};
#[cfg(unix)]
use std::fs::File;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub(crate) struct Params {
    #[structopt(short = "c", long = "config", help = "path to the server config")]
    config: String,
    #[structopt(short = "f", long = "foreground", help = "don't daemonize")]
    #[allow(dead_code)]
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
    #[cfg(unix)]
    {
        if params.foreground {
            let config = Config::load(params.config.clone())
                .context("failed to load resolver server config")?;
            tokio_run(config, params)
        } else {
            let mut config: file::Config =
                serde_json::from_reader(File::open(&params.config)?)?;
            let member = &mut config.member_servers[params.id];
            member.pid_file.set_extension(params.id.to_string());
            Daemonize::new()
                .pid_file(&member.pid_file)
                .start()
                .context("failed to daemonize")?;
            let config = Config::load(params.config.clone())
                .context("failed to load resolver server config")?;
            tokio_run(config, params)
        }
    }
    #[cfg(windows)]
    {
        let config = Config::load(params.config.clone())
            .context("failed to load resolver server config")?;
        tokio_run(config, params)
    }
}
