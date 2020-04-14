use json_pubsub::{
    resolver_server::Server,
    config::resolver_server::Config,
};
use futures::future;
use tokio::runtime::Runtime;
use daemonize::Daemonize;
use std::{
    path::PathBuf,
    fs::read,
    net::SocketAddr,
};

pub(crate) fn run(config: PathBuf, daemonize: bool) {
    let cfg = Config::load(&config).expect("failed to load config");
    if daemonize {
        Daemonize::new()
            .pid_file(&*cfg.pid_file)
            .start()
            .expect("failed to daemonize");
    }
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let server = Server::new(cfg).await.expect("starting server");
        future::pending::<()>().await;
        drop(server)
    });
}
