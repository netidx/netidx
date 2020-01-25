use json_pubsub::resolver_server::Server;
use futures::future;
use tokio::runtime::Runtime;
use daemonize::Daemonize;
use super::ResolverConfig;

pub(crate) fn run(config: ResolverConfig, daemonize: bool) {
    if daemonize {
        Daemonize::new()
            .pid_file(&*config.pid_file)
            .start()
            .expect("failed to daemonize");
    }
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let server =
            Server::new(config.bind, config.max_clients).await
            .expect("starting server");
        future::pending::<()>().await;
        drop(server)
    });
}
