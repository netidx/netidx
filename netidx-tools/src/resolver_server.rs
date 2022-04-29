use daemonize::Daemonize;
use futures::future;
use netidx::resolver_server::{config::Config, Server};
use structopt::StructOpt;
use tokio::runtime::Runtime;

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
        help = "index of the address to bind to",
        default_value = "0"
    )]
    id: usize,
}

pub(crate) fn run(params: Params) {
    let config =
        Config::load(params.config).expect("failed to load resolver server config");
    if !params.foreground {
        let mut file = config.pid_file.clone();
        file.push_str(&format!("{}.pid", params.id));
        Daemonize::new().pid_file(file).start().expect("failed to daemonize");
    }
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let server =
            Server::new(config, params.delay_reads).await.expect("starting server");
        future::pending::<()>().await;
        drop(server)
    });
}
