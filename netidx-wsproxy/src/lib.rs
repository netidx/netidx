use crate::protocol::{Request, Response, Update};
use anyhow::{bail, Result};
use futures::{
    channel::mpsc,
    prelude::*,
    select_biased,
    stream::{FuturesUnordered, SplitSink},
    StreamExt,
};
use fxhash::FxHashMap;
use log::warn;
use netidx::{
    path::Path,
    pool::{Pool, Pooled},
    protocol::value::Value,
    publisher::{Id as PubId, Publisher, UpdateBatch, Val as Pub},
    subscriber::{Dval as Sub, Event, SubId, Subscriber, UpdatesFlags},
    utils::{BatchItem, Batched},
};
use netidx_protocols::rpc::client::Proc;
use once_cell::sync::Lazy;
use std::{
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
    pin::Pin,
    result,
};
use warp::{
    filters::BoxedFilter,
    ws::{Message, WebSocket, Ws},
    Filter, Reply,
};
use tokio::time::Duration;

pub mod config;
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

type PendingCall =
    Pin<Box<dyn Future<Output = (u64, Result<Value>)> + Send + Sync + 'static>>;

async fn reply<'a>(tx: &mut SplitSink<WebSocket, Message>, m: &Response, timeout: Option<Duration>) -> Result<()> {
    let s = serde_json::to_string(m)?;
    // CR base1172 for estokes: Here we're only enforcing that the SplitSink write completes within
    // [timeout], with no guarantee on how long it takes to actually flush the message to the client.
    // In a perfect world we'd probably want a proper flush timeout (similar to what [WriteChannel] does).
    // For now, just requiring that [tx.send(..)] completes within [timeout] is probably good enough.
    // DUR
    let fut = tx.send(Message::text(s));
    match timeout {
        None => Ok(fut.await?),
        Some(timeout) => Ok(tokio::time::timeout(timeout, fut).await??)
    }
}
async fn err(
    tx: &mut SplitSink<WebSocket, Message>,
    message: impl Into<String>,
    timeout: Option<Duration>,
) -> Result<()> {
    reply(tx, &Response::Error { error: message.into() }, timeout).await
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

    fn update(
        &mut self,
        batch: &mut UpdateBatch,
        mut updates: Pooled<Vec<protocol::BatchItem>>,
    ) -> Result<()> {
        for up in updates.drain(..) {
            match self.pubs.get(&up.id) {
                None => bail!("not published"),
                Some(pe) => pe.val.update(batch, up.data),
            }
        }
        Ok(())
    }

    fn call(
        &mut self,
        id: u64,
        path: Path,
        mut args: Pooled<Vec<(Pooled<String>, Value)>>,
    ) -> Result<PendingCall> {
        let proc = match self.rpcs.entry(path) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let proc = Proc::new(&self.subscriber, e.key().clone())?;
                e.insert(proc)
            }
        }
        .clone();
        Ok(Box::pin(async move { (id, proc.call(args.drain(..)).await) }) as PendingCall)
    }

    async fn process_from_client(
        &mut self,
        tx: &mut SplitSink<WebSocket, Message>,
        queued: &mut Vec<result::Result<Message, warp::Error>>,
        calls_pending: &mut FuturesUnordered<PendingCall>,
        timeout: Option<Duration>
    ) -> Result<()> {
        let mut batch = self.publisher.start_batch();
        for r in queued.drain(..) {
            let m = r?;
            if m.is_ping() {
                continue;
            }
            match m.to_str() {
                Err(_) => err(tx, "expected text", timeout).await?,
                Ok(txt) => match serde_json::from_str::<Request>(txt) {
                    Err(e) => err(tx, format!("could not parse message {}", e), timeout).await?,
                    Ok(req) => match req {
                        Request::Subscribe { path } => {
                            let id = self.subscribe(path);
                            reply(tx, &Response::Subscribed { id }, timeout).await?
                        }
                        Request::Unsubscribe { id } => match self.unsubscribe(id) {
                            Err(e) => err(tx, e.to_string(), timeout).await?,
                            Ok(()) => reply(tx, &Response::Unsubscribed, timeout).await?,
                        },
                        Request::Write { id, val } => match self.write(id, val) {
                            Err(e) => err(tx, e.to_string(), timeout).await?,
                            Ok(()) => reply(tx, &Response::Wrote, timeout).await?,
                        },
                        Request::Publish { path, init } => match self.publish(path, init)
                        {
                            Err(e) => err(tx, e.to_string(), timeout).await?,
                            Ok(id) => reply(tx, &Response::Published { id }, timeout).await?,
                        },
                        Request::Unpublish { id } => match self.unpublish(id) {
                            Err(e) => err(tx, e.to_string(), timeout).await?,
                            Ok(()) => reply(tx, &Response::Unpublished, timeout).await?,
                        },
                        Request::Update { updates } => {
                            match self.update(&mut batch, updates) {
                                Err(e) => err(tx, e.to_string(), timeout).await?,
                                Ok(()) => reply(tx, &Response::Updated, timeout).await?,
                            }
                        }
                        Request::Call { id, path, args } => {
                            match self.call(id, path, args) {
                                Ok(pending) => calls_pending.push(pending),
                                Err(e) => {
                                    let error = format!("rpc call failed {}", e);
                                    reply(tx, &Response::CallFailed { id, error }, timeout).await?
                                }
                            }
                        }
                        Request::Unknown => err(tx, "unknown request", timeout).await?,
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
    timeout: Option<Duration>
) -> Result<()> {
    static UPDATES: Lazy<Pool<Vec<Update>>> = Lazy::new(|| Pool::new(50, 10000));
    let (tx_up, mut rx_up) = mpsc::channel::<Pooled<Vec<(SubId, Event)>>>(3);
    let mut ctx = ClientCtx::new(publisher, subscriber, tx_up);
    let (mut tx_ws, rx_ws) = ws.split();
    let mut queued: Vec<result::Result<Message, warp::Error>> = Vec::new();
    let mut rx_ws = Batched::new(rx_ws.fuse(), 10_000);
    let mut calls_pending: FuturesUnordered<PendingCall> = FuturesUnordered::new();
    calls_pending.push(Box::pin(async { future::pending().await }) as PendingCall);
    loop {
        select_biased! {
            (id, res) = calls_pending.select_next_some() => match res {
                Ok(result) => {
                    reply(&mut tx_ws, &Response::CallSuccess { id, result }, timeout).await?
                }
                Err(e) => {
                    let error = format!("rpc call failed {}", e);
                    reply(&mut tx_ws, &Response::CallFailed { id, error }, timeout).await?
                }
            },
            r = rx_ws.select_next_some() => match r {
                BatchItem::InBatch(r) => queued.push(r),
                BatchItem::EndBatch => {
                    ctx.process_from_client(
                        &mut tx_ws,
                        &mut queued,
                        &mut calls_pending,
                        timeout
                    ).await?
                }
            },
            mut batch = rx_up.select_next_some() => {
                let mut updates = UPDATES.take();
                for (id, event) in batch.drain(..) {
                    updates.push(Update {id, event});
                }
                reply(&mut tx_ws, &Response::Update { updates }, timeout).await?
            },
        }
    }
    Ok(())
}

/// If you want to integrate the netidx api server into your own warp project
/// this will return the filter path will be the http path where the websocket
/// lives
pub fn filter(
    publisher: Publisher,
    subscriber: Subscriber,
    path: &'static str,
    timeout: Option<Duration>,
) -> BoxedFilter<(impl Reply,)> {
    warp::path(path)
        .and(warp::ws())
        .map(move |ws: Ws| {
            let (publisher, subscriber) = (publisher.clone(), subscriber.clone());
            ws.on_upgrade(move |ws| {
                let (publisher, subscriber) = (publisher.clone(), subscriber.clone());
                async move {
                    if let Err(e) = handle_client(publisher, subscriber, ws, timeout).await {
                        warn!("client handler exited: {}", e)
                    }
                }
            })
        })
        .boxed()
}

/// If you want to embed the websocket api in your own process, but you don't
/// want to serve any other warp filters then you can just call this in a task.
/// This will not return unless the server crashes, you should
/// probably run it in a task.
pub async fn run(
    config: config::Config,
    publisher: Publisher,
    subscriber: Subscriber,
) -> Result<()> {
    let routes = filter(publisher, subscriber, "ws", config.timeout.map(Duration::from_secs));
    match (&config.cert, &config.key) {
        (_, None) | (None, _) => {
            warp::serve(routes).run(config.listen.parse::<SocketAddr>()?).await
        }
        (Some(cert), Some(key)) => {
            warp::serve(routes)
                .tls()
                .cert_path(cert)
                .key_path(key)
                .run(config.listen.parse::<SocketAddr>()?)
                .await
        }
    }
    Ok(())
}
