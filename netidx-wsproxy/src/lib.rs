use crate::protocol::{FromWs, ToWs};
use anyhow::{bail, Result};
use futures::{channel::mpsc, prelude::*, select_biased, stream::SplitSink, StreamExt};
use fxhash::FxHashMap;
use log::warn;
use netidx::{
    path::Path,
    pool::Pooled,
    publisher::{Id as PubId, Publisher, Val as Pub},
    subscriber::{Event, SubId, Subscriber, Val as Sub},
};
use std::{collections::HashMap, net::SocketAddr};
use warp::{
    ws::{Message, WebSocket, Ws},
    Filter,
};

mod config;
mod protocol;

struct ClientCtx {
    publisher: Publisher,
    subscriber: Subscriber,
    subs: FxHashMap<SubId, Sub>,
    pubs: FxHashMap<PubId, Pub>,
    subs_by_path: FxHashMap<Path, SubId>,
    pubs_by_path: FxHashMap<Path, PubId>,
    tx_up: mpsc::Sender<Pooled<Vec<Event>>>,
}

impl ClientCtx {
    fn new(
        publisher: Publisher,
        subscriber: Subscriber,
        tx_up: mpsc::Sender<Pooled<Vec<Event>>>,
    ) -> Self {
        Self {
            publisher,
            subscriber,
            tx_up,
            subs: HashMap::default(),
            pubs: HashMap::default(),
            subs_by_path: HashMap::default(),
            pubs_by_path: HashMap::default(),
        }
    }
}

async fn handle_client(
    publisher: Publisher,
    subscriber: Subscriber,
    ws: WebSocket,
) -> Result<()> {
    let (tx_up, mut rx_up) = mpsc::channel::<Pooled<Vec<Event>>>(3);
    let mut ctx = ClientCtx::new(publisher, subscriber, tx_up);
    async fn reply(tx: &mut SplitSink<WebSocket, Message>, m: FromWs) -> Result<()> {
        let s = serde_json::to_string(&m)?;
        Ok(tx.send(Message::text(s)).await?)
    }
    async fn err(
        tx: &mut SplitSink<WebSocket, Message>,
        message: impl Into<String>,
    ) -> Result<()> {
        reply(tx, FromWs::Error { message: message.into() }).await
    }
    let (mut tx_ws, rx_ws) = ws.split();
    let mut rx_ws = rx_ws.fuse();
    loop {
        select_biased! {
            r = rx_ws.select_next_some() => match r?.to_str() {
                Err(_) => err(&mut tx_ws, "expected text").await?,
                Ok(txt) => match serde_json::from_str::<ToWs>(txt) {
                    Err(e) => err(&mut tx_ws, format!("could not parse message {}", e)).await?,
                    Ok(req) => match req {
                        ToWs::Subscribe {path} => (),
                        ToWs::Unsubscribe {id} => (),
                        ToWs::Write {id, val} => (),
                        ToWs::Publish {path, init} => (),
                        ToWs::Unpublish {id} => (),
                        ToWs::Update {id, val} => (),
                        ToWs::Call {path, args} => (),
                        ToWs::Unknown => err(&mut tx_ws, "unknown request").await?
                    }
                }
            },
            r = rx_up.select_next_some() => {
            },
        }
    }
    bail!("not implemented")
}

/// This will not return unless the server crashes, you should
/// probably run it in a task.
pub async fn run(
    config: config::Config,
    publisher: Publisher,
    subscriber: Subscriber,
) -> Result<()> {
    let routes = warp::path("ws").and(warp::ws()).map(move |ws: Ws| {
        let (publisher, subscriber) = (publisher.clone(), subscriber.clone());
        ws.on_upgrade(move |ws| {
            let (publisher, subscriber) = (publisher.clone(), subscriber.clone());
            async move {
                if let Err(e) = handle_client(publisher, subscriber, ws).await {
                    warn!("client handler exited: {}", e)
                }
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
