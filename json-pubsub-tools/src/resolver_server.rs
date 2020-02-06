use json_pubsub::{
    resolver_server::Server,
};
use futures::future;
use tokio::runtime::Runtime;
use daemonize::Daemonize;
use std::{
    path::PathBuf,
    fs::read,
    net::SocketAddr,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cfg {
    pub bind: SocketAddr,
    pub max_clients: usize,
    pub pid_file: PathBuf,
}

pub(crate) fn run(config: PathBuf, daemonize: bool) {
    let cfg: Cfg = serde_json::from_slice(
        &*read(config).expect("reading server config")
    ).expect("parsing server config");
    if daemonize {
        Daemonize::new()
            .pid_file(&*cfg.pid_file)
            .start()
            .expect("failed to daemonize");
    }
    let mut rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        let server =
            Server::new(cfg.bind, cfg.max_clients).await
            .expect("starting server");
        future::pending::<()>().await;
        drop(server)
    });
}
