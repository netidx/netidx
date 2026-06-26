use crate::protocol::{Request, Response, Update};
use ahash::AHashMap;
use anyhow::{Result, bail};
use futures::{
    channel::mpsc,
    prelude::*,
    select_biased,
    stream::{FuturesUnordered, SplitSink},
};
use log::warn;
use netidx::{
    path::Path,
    protocol::value::Value,
    publisher::{Id as PubId, Publisher, UpdateBatch, Val as Pub},
    subscriber::{Dval as Sub, Event, SubId, Subscriber, UpdatesFlags},
    utils::{BatchItem, Batched},
};
use netidx_protocols::rpc::client::Proc;
use nohash::IntMap;
use poolshark::global::{GPooled, Pool};
use std::{
    collections::hash_map::Entry, net::SocketAddr, pin::Pin, result, sync::LazyLock,
    time::Duration,
};
use tokio::time;
use warp::{
    Filter, Reply,
    filters::BoxedFilter,
    ws::{Message, WebSocket, Ws},
};
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

/// Serialize and send a single response to the client. If `timeout` is set the
/// send must complete within it or the client is disconnected. A full websocket
/// send buffer pushes back here, which (because every client has its own
/// subscriber) propagates back to the publishers this client is reading without
/// touching any other client.
async fn send(
    tx: &mut SplitSink<WebSocket, Message>,
    m: &Response,
    timeout: Option<Duration>,
) -> Result<()> {
    let s = serde_json::to_string(m)?;
    let fut = tx.send(Message::text(s));
    match timeout {
        None => Ok(fut.await?),
        Some(timeout) => Ok(time::timeout(timeout, fut).await??),
    }
}

struct ClientCtx {
    publisher: Publisher,
    subscriber: Subscriber,
    subs: IntMap<SubId, SubEntry>,
    pubs: IntMap<PubId, PubEntry>,
    subs_by_path: AHashMap<Path, SubId>,
    pubs_by_path: AHashMap<Path, PubId>,
    rpcs: AHashMap<Path, Proc>,
    tx_up: mpsc::Sender<GPooled<Vec<(SubId, Event)>>>,
}

impl ClientCtx {
    fn new(
        publisher: Publisher,
        subscriber: Subscriber,
        tx_up: mpsc::Sender<GPooled<Vec<(SubId, Event)>>>,
    ) -> Self {
        Self {
            publisher,
            subscriber,
            tx_up,
            subs: IntMap::default(),
            pubs: IntMap::default(),
            subs_by_path: AHashMap::default(),
            pubs_by_path: AHashMap::default(),
            rpcs: AHashMap::default(),
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
        mut updates: GPooled<Vec<protocol::BatchItem>>,
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
        mut args: GPooled<Vec<(GPooled<String>, Value)>>,
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
        input_batch: &mut Vec<result::Result<Message, warp::Error>>,
        calls_pending: &mut FuturesUnordered<PendingCall>,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let mut batch = self.publisher.start_batch();
        for r in input_batch.drain(..) {
            let m = r?;
            if m.is_ping() {
                continue;
            }
            let resp = match m.to_str() {
                Err(_) => Some(Response::Error { error: "expected text".into() }),
                Ok(txt) => match serde_json::from_str::<Request>(txt) {
                    Err(e) => Some(Response::Error {
                        error: format!("could not parse message {e}"),
                    }),
                    Ok(req) => match req {
                        Request::Subscribe { path } => {
                            Some(Response::Subscribed { id: self.subscribe(path) })
                        }
                        Request::Unsubscribe { id } => Some(match self.unsubscribe(id) {
                            Err(e) => Response::Error { error: e.to_string() },
                            Ok(()) => Response::Unsubscribed,
                        }),
                        Request::Write { id, val } => Some(match self.write(id, val) {
                            Err(e) => Response::Error { error: e.to_string() },
                            Ok(()) => Response::Wrote,
                        }),
                        Request::Publish { path, init } => {
                            Some(match self.publish(path, init) {
                                Err(e) => Response::Error { error: e.to_string() },
                                Ok(id) => Response::Published { id },
                            })
                        }
                        Request::Unpublish { id } => Some(match self.unpublish(id) {
                            Err(e) => Response::Error { error: e.to_string() },
                            Ok(()) => Response::Unpublished,
                        }),
                        Request::Update { updates } => {
                            Some(match self.update(&mut batch, updates) {
                                Err(e) => Response::Error { error: e.to_string() },
                                Ok(()) => Response::Updated,
                            })
                        }
                        Request::Call { id, path, args } => {
                            match self.call(id, path, args) {
                                Ok(pending) => {
                                    calls_pending.push(pending);
                                    None
                                }
                                Err(e) => Some(Response::CallFailed {
                                    id,
                                    error: format!("rpc call failed {e}"),
                                }),
                            }
                        }
                        Request::Unknown => {
                            Some(Response::Error { error: "unknown request".into() })
                        }
                    },
                },
            };
            if let Some(resp) = resp {
                send(tx, &resp, timeout).await?;
            }
        }
        batch.commit(timeout).await;
        Ok(())
    }
}

async fn handle_client(
    publisher: Publisher,
    subscriber: Subscriber,
    ws: WebSocket,
    timeout: Option<Duration>,
) -> Result<()> {
    static UPDATES: LazyLock<Pool<Vec<Update>>> = LazyLock::new(|| Pool::new(50, 10000));
    let (tx_up, mut rx_up) = mpsc::channel::<GPooled<Vec<(SubId, Event)>>>(3);
    let mut ctx = ClientCtx::new(publisher, subscriber, tx_up);
    let (mut tx_ws, rx_ws) = ws.split();
    let mut input_batch: Vec<result::Result<Message, warp::Error>> = Vec::new();
    let mut rx_ws = Batched::new(rx_ws.fuse(), 10_000);
    let mut calls_pending: FuturesUnordered<PendingCall> = FuturesUnordered::new();
    calls_pending.push(Box::pin(async { future::pending().await }) as PendingCall);
    loop {
        select_biased! {
            r = rx_ws.next() => match r {
                None => return Ok(()),
                Some(BatchItem::InBatch(r)) => input_batch.push(r),
                Some(BatchItem::EndBatch) => {
                    ctx.process_from_client(
                        &mut tx_ws,
                        &mut input_batch,
                        &mut calls_pending,
                        timeout,
                    )
                    .await?
                }
            },
            (cid, res) = calls_pending.select_next_some() => {
                let m = match res {
                    Ok(result) => Response::CallSuccess { id: cid, result },
                    Err(e) => Response::CallFailed {
                        id: cid,
                        error: format!("rpc call failed {e}"),
                    },
                };
                send(&mut tx_ws, &m, timeout).await?;
            },
            mut batch = rx_up.select_next_some() => {
                let mut updates = UPDATES.take();
                for (id, event) in batch.drain(..) {
                    updates.push(Update { id, event });
                }
                send(&mut tx_ws, &Response::Update { updates }, timeout).await?;
            }
        }
    }
}

/// Build a warp filter serving the netidx websocket api at `path`, minting a
/// fresh publisher and subscriber for every client by calling `make`. Because
/// each client gets its own netidx session, a slow client only pushes back on
/// its own subscriptions and can never block another client, and you can give
/// each client distinct credentials by capturing per connection state in
/// `make`.
pub fn filter_with<F, Fut>(
    make: F,
    path: &'static str,
    timeout: Option<Duration>,
) -> BoxedFilter<(impl Reply,)>
where
    F: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<(Publisher, Subscriber)>> + Send + 'static,
{
    warp::path(path)
        .and(warp::ws())
        .map(move |ws: Ws| {
            let make = make.clone();
            ws.on_upgrade(move |ws| async move {
                match make().await {
                    Err(e) => warn!("could not create netidx session for client: {e:#}"),
                    Ok((publisher, subscriber)) => {
                        if let Err(e) =
                            handle_client(publisher, subscriber, ws, timeout).await
                        {
                            warn!("client handler exited: {e}")
                        }
                    }
                }
            })
        })
        .boxed()
}

/// Build a warp filter serving the netidx websocket api at `path`. Every client
/// shares the passed in `publisher` and `subscriber` (they are cloned per
/// client). Convenient when you already have a session to share, but note that
/// a slow client can then push back on the shared subscriber; use [filter_with]
/// if you need per client isolation.
pub fn filter(
    publisher: Publisher,
    subscriber: Subscriber,
    path: &'static str,
    timeout: Option<Duration>,
) -> BoxedFilter<(impl Reply,)> {
    filter_with(
        move || {
            let publisher = publisher.clone();
            let subscriber = subscriber.clone();
            async move { Ok((publisher, subscriber)) }
        },
        path,
        timeout,
    )
}

/// Serve the netidx websocket api on its own warp server, minting a fresh
/// publisher and subscriber for each client via `make`. This does not return
/// unless the server fails, so you probably want to run it in a task.
pub async fn run<F, Fut>(
    config: config::Config,
    make: F,
    timeout: Option<Duration>,
) -> Result<()>
where
    F: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<(Publisher, Subscriber)>> + Send + 'static,
{
    let routes = filter_with(make, "ws", timeout);
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
