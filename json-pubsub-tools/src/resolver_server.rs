use json_pubsub::resolver_server::Server;
use std::net::SocketAddr;
use futures::future;
use async_std::{
    prelude::*,
    task,
};
use daemonize::Daemonize;
use super::ResolverConfig;

pub(crate) fn run(config: ResolverConfig, daemonize: bool) {
    if daemonize {
        Daemonize::new()
            .pid_file(&*config.pid_file)
            .start()
            .expect("failed to daemonize");
    }
    task::block_on(async {
        let server =
            Server::new(config.bind, config.max_clients).await
            .expect("starting server");
        future::pending::<()>().await;
        drop(server)
    });
}
