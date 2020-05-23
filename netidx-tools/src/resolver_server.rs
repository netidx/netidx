use daemonize::Daemonize;
use futures::future;
use json_pubsub::{config, resolver_server::Server};
use tokio::runtime::Runtime;

pub(crate) fn run(
    config: config::Config,
    permissions: config::PMap,
    daemonize: bool,
    delay_reads: bool,
    id: usize,
) {
    if daemonize {
        let mut file = config.pid_file.clone();
        file.push_str(&format!("{}.pid", id));
        Daemonize::new().pid_file(file).start().expect("failed to daemonize");
    }
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let server = Server::new(config, permissions, delay_reads, id)
            .await
            .expect("starting server");
        future::pending::<()>().await;
        drop(server)
    });
}
