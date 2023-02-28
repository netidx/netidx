use crate::protocol::{FromWs, ToWs};
use anyhow::{bail, Result};
use futures::{channel::mpsc, prelude::*, select_biased, stream::SplitSink, StreamExt};
use fxhash::FxHashMap;
use log::warn;
use netidx::{
    path::Path,
    pool::Pooled,
    protocol::value::Value,
    publisher::{Id as PubId, Publisher, UpdateBatch, Val as Pub},
    subscriber::{Dval as Sub, Event, SubId, Subscriber, UpdatesFlags},
    utils::{BatchItem, Batched},
};
use netidx_protocols::{rpc::client::Proc, call_rpc};
use std::{
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
    result,
};
use warp::{
    ws::{Message, WebSocket, Ws},
    Filter,
};

mod config;
mod protocol;

struct SubEntry {
    count: usize,
    path: Path,
    val: Sub,
}

struct PubEntry {
    path: Path,
    val: Pub,
}

async fn reply(tx: &mut SplitSink<WebSocket, Message>, m: FromWs) -> Result<()> {
    let s = serde_json::to_string(&m)?;
    Ok(tx.send(Message::text(s)).await?)
}
async fn err(
    tx: &mut SplitSink<WebSocket, Message>,
    message: impl Into<String>,
) -> Result<()> {
    reply(tx, FromWs::Error { error: message.into() }).await
}

struct ClientCtx {
    publisher: Publisher,
    subscriber: Subscriber,
    subs: FxHashMap<SubId, SubEntry>,
    pubs: FxHashMap<PubId, PubEntry>,
    subs_by_path: HashMap<Path, SubId>,
    pubs_by_path: HashMap<Path, PubId>,
    rpcs: HashMap<Path, Proc>,
    tx_up: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
}

impl ClientCtx {
    fn new(
        publisher: Publisher,
        subscriber: Subscriber,
        tx_up: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
    ) -> Self {
        Self {
            publisher,
            subscriber,
            tx_up,
            subs: HashMap::default(),
            pubs: HashMap::default(),
            subs_by_path: HashMap::default(),
            pubs_by_path: HashMap::default(),
            rpcs: HashMap::default(),
        }
    }

    fn subscribe(&mut self, path: Path) -> SubId {
        match self.subs_by_path.entry(path) {
            Entry::Occupied(e) => {
                let se = self.subs.get_mut(e.get()).unwrap();
                se.count += 1;
                se.val.id()
            }
            Entry::Vacant(e) => {
                let path = e.key().clone();
                let val = self.subscriber.subscribe(path.clone());
                let id = val.id();
                val.updates(UpdatesFlags::BEGIN_WITH_LAST, self.tx_up.clone());
                self.subs.insert(id, SubEntry { count: 1, path, val });
                e.insert(id);
                id
            }
        }
    }

    fn unsubscribe(&mut self, id: SubId) -> Result<()> {
        match self.subs.get_mut(&id) {
            None => bail!("not subscribed"),
            Some(se) => {
                se.count -= 1;
                if se.count == 0 {
                    let path = se.path.clone();
                    self.subs.remove(&id);
                    self.subs_by_path.remove(&path);
                }
                Ok(())
            }
        }
    }

    fn write(&mut self, id: SubId, val: Value) -> Result<()> {
        match self.subs.get(&id) {
            None => bail!("not subscribed"),
            Some(se) => {
                se.val.write(val);
                Ok(())
            }
        }
    }

    fn publish(&mut self, path: Path, val: Value) -> Result<PubId> {
        match self.pubs_by_path.entry(path) {
            Entry::Occupied(_) => bail!("already published"),
            Entry::Vacant(e) => {
                let path = e.key().clone();
                let val = self.publisher.publish(path.clone(), val)?;
                let id = val.id();
                e.insert(id);
                self.pubs.insert(id, PubEntry { val, path });
                Ok(id)
            }
        }
    }

    fn unpublish(&mut self, id: PubId) -> Result<()> {
        match self.pubs.remove(&id) {
            None => bail!("not published"),
            Some(pe) => {
                self.pubs_by_path.remove(&pe.path);
                Ok(())
            }
        }
    }

    fn update(&mut self, batch: &mut UpdateBatch, id: PubId, value: Value) -> Result<()> {
        match self.pubs.get(&id) {
            None => bail!("not published"),
            Some(pe) => Ok(pe.val.update(batch, value)),
        }
    }

    async fn process_from_client(
        &mut self,
        tx: &mut SplitSink<WebSocket, Message>,
        queued: &mut Vec<result::Result<Message, warp::Error>>,
    ) -> Result<()> {
        let mut batch = self.publisher.start_batch();
        for r in queued.drain(..) {
            match r?.to_str() {
                Err(_) => err(tx, "expected text").await?,
                Ok(txt) => match serde_json::from_str::<ToWs>(txt) {
                    Err(e) => err(tx, format!("could not parse message {}", e)).await?,
                    Ok(req) => match req {
                        ToWs::Subscribe { path } => {
                            let id = self.subscribe(path);
                            reply(tx, FromWs::Subscribed { id }).await?
                        }
                        ToWs::Unsubscribe { id } => match self.unsubscribe(id) {
                            Err(e) => err(tx, e.to_string()).await?,
                            Ok(()) => reply(tx, FromWs::Unsubscribed).await?,
                        },
                        ToWs::Write { id, val } => match self.write(id, val) {
                            Err(e) => err(tx, e.to_string()).await?,
                            Ok(()) => reply(tx, FromWs::Wrote).await?,
                        },
                        ToWs::Publish { path, init } => match self.publish(path, init) {
                            Err(e) => err(tx, e.to_string()).await?,
                            Ok(id) => reply(tx, FromWs::Published { id }).await?,
                        },
                        ToWs::Unpublish { id } => match self.unpublish(id) {
                            Err(e) => err(tx, e.to_string()).await?,
                            Ok(()) => reply(tx, FromWs::Unpublished).await?,
                        },
                        ToWs::Update { id, val } => {
                            match self.update(&mut batch, id, val) {
                                Err(e) => err(tx, e.to_string()).await?,
                                Ok(()) => reply(tx, FromWs::Updated).await?,
                            }
                        }
                        ToWs::Call { path, args } => (),
                        ToWs::Unknown => err(tx, "unknown request").await?,
                    },
                },
            }
        }
        Ok(batch.commit(None).await)
    }
}

async fn handle_client(
    publisher: Publisher,
    subscriber: Subscriber,
    ws: WebSocket,
) -> Result<()> {
    let (tx_up, mut rx_up) = mpsc::channel::<Pooled<Vec<(SubId, Event)>>>(3);
    let mut ctx = ClientCtx::new(publisher, subscriber, tx_up);
    let (mut tx_ws, rx_ws) = ws.split();
    let mut queued: Vec<result::Result<Message, warp::Error>> = Vec::new();
    let mut rx_ws = Batched::new(rx_ws.fuse(), 10_000);
    loop {
        select_biased! {
            r = rx_ws.select_next_some() => match r {
                BatchItem::InBatch(r) => queued.push(r),
                BatchItem::EndBatch => {
                    ctx.process_from_client(&mut tx_ws, &mut queued).await?
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
