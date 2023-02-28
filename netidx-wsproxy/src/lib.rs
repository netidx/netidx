use anyhow::{bail, Result};
use log::warn;
use netidx::{publisher::Publisher, subscriber::Subscriber};
use std::net::SocketAddr;
use tokio::task;
use warp::{
    ws::{WebSocket, Ws},
    Filter,
};

mod config;
mod protocol;

async fn handle_client(ws: WebSocket) -> Result<()> {
    bail!("not implemented")
}

/// This will not return unless the server crashes, you should
/// probably run it in a task.
pub async fn run(
    config: config::Config,
    publisher: Publisher,
    subscriber: Subscriber,
) -> Result<()> {
    let routes = warp::path("ws").and(warp::ws()).map(|ws: Ws| {
        ws.on_upgrade(|ws| async {
            if let Err(e) = handle_client(ws).await {
                warn!("client handler exited: {}", e)
            }
        })
    });
    match (&config.cert, &config.key) {
        (_, None) | (None, _) => {
            warp::serve(routes).run(config.bind.parse::<SocketAddr>()?).await
        }
        (Some(cert), Some(key)) => {
            warp::serve(routes)
                .tls()
                .cert_path(cert)
                .key_path(key)
                .run(config.bind.parse::<SocketAddr>()?)
                .await
        }
    }
    Ok(())
}
