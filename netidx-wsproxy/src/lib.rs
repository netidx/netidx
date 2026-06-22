use crate::{
    config::DisconnectPfactor,
    protocol::{Request, Response, Update},
};
use ahash::AHashMap;
use anyhow::{bail, Result};
use futures::{
    channel::mpsc,
    prelude::*,
    select_biased,
    stream::{FuturesUnordered, SplitSink},
    StreamExt,
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
    collections::hash_map::Entry,
    net::SocketAddr,
    pin::Pin,
    result,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, LazyLock,
    },
};
use std::{collections::VecDeque, time::Duration};
use tokio::{
    sync::mpsc as tmpsc,
    task,
    time::{self, Instant, Sleep},
};
use warp::{
    filters::BoxedFilter,
    ws::{Message, WebSocket, Ws},
    Filter, Reply,
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

/// reply to a client with variable pushback imposed based on pfactor.
/// if pfactor >= disconnect_pfactor then raise an error unless infinite_queue
/// is set
/// if this function returns false then that means the client was not
/// ready to accept the message and it must be retried at a later time.
async fn reply(
    tx: &mut tmpsc::Sender<Message>,
    qsend: &QSend,
    timeout: Option<Duration>,
    disconnect_pfactor: DisconnectPfactor,
) -> Result<bool> {
    async fn send(
        tx: &mut tmpsc::Sender<Message>,
        m: &Response,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let s = serde_json::to_string(m)?;
        // XCR base1172 for estokes: Here we're only enforcing that the SplitSink write completes within
        // [timeout], with no guarantee on how long it takes to actually flush the message to the client.
        // In a perfect world we'd probably want a proper flush timeout (similar to what [WriteChannel] does).
        // For now, just requiring that [tx.send(..)] completes within [timeout] is probably good enough.
        // DUR
        //
        // estokes: If the websocket buffer fills up this will push back, which is all we really want it for.
        let fut = tx.send(Message::text(s));
        match timeout {
            None => Ok(fut.await?),
            Some(timeout) => Ok(time::timeout(timeout, fut).await??),
        }
    }
    async fn wait(
        pfactor: f32,
        qdepth: f32,
        disconnect_pfactor: DisconnectPfactor,
    ) -> Result<bool> {
        if disconnect_pfactor.disconnect(pfactor) {
            bail!("client queue is too large")
        } else {
            let wait = f32::trunc(qdepth / f32::max(1., pfactor)) as u64;
            if wait == 0 {
                Ok(false)
            } else {
                time::sleep(Duration::from_millis(wait)).await;
                Ok(false)
            }
        }
    }
    select_biased! {
        r = send(tx, &qsend.m, timeout).fuse() => r.map(|()| true),
        r = wait(qsend.pfactor, qsend.qdepth, disconnect_pfactor).fuse() => r
    }
}

fn err(tx: &mut Queued, message: impl Into<String>) {
    tx.queue_send(Response::Error { error: message.into() })
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
        tx: &mut Queued,
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
            match m.to_str() {
                Err(_) => err(tx, "expected text"),
                Ok(txt) => match serde_json::from_str::<Request>(txt) {
                    Err(e) => err(tx, format!("could not parse message {}", e)),
                    Ok(req) => match req {
                        Request::Subscribe { path } => {
                            let id = self.subscribe(path);
                            tx.queue_send(Response::Subscribed { id })
                        }
                        Request::Unsubscribe { id } => match self.unsubscribe(id) {
                            Err(e) => err(tx, e.to_string()),
                            Ok(()) => tx.queue_send(Response::Unsubscribed),
                        },
                        Request::Write { id, val } => match self.write(id, val) {
                            Err(e) => err(tx, e.to_string()),
                            Ok(()) => tx.queue_send(Response::Wrote),
                        },
                        Request::Publish { path, init } => match self.publish(path, init)
                        {
                            Err(e) => err(tx, e.to_string()),
                            Ok(id) => tx.queue_send(Response::Published { id }),
                        },
                        Request::Unpublish { id } => match self.unpublish(id) {
                            Err(e) => err(tx, e.to_string()),
                            Ok(()) => tx.queue_send(Response::Unpublished),
                        },
                        Request::Update { updates } => {
                            match self.update(&mut batch, updates) {
                                Err(e) => err(tx, e.to_string()),
                                Ok(()) => tx.queue_send(Response::Updated),
                            }
                        }
                        Request::Call { id, path, args } => {
                            match self.call(id, path, args) {
                                Ok(pending) => calls_pending.push(pending),
                                Err(e) => {
                                    let error = format!("rpc call failed {}", e);
                                    let m = Response::CallFailed { id, error };
                                    tx.queue_send(m)
                                }
                            }
                        }
                        Request::Unknown => err(tx, "unknown request"),
                    },
                },
            }
        }
        batch.commit(timeout).await;
        Ok(())
    }
}

#[derive(Default)]
struct QStats {
    // Queue depth is intentionally measured in pending websocket responses,
    // not bytes or contained updates. Large batches, such as initial values for
    // many subscriptions, should not by themselves make a client look hung.
    nq: AtomicUsize,
    sum: AtomicUsize,
}

impl QStats {
    fn add_queue(&self) {
        self.nq.fetch_add(1, Ordering::Release);
    }

    fn remove_queue(&self, n: usize) {
        self.nq.fetch_sub(1, Ordering::Release);
        self.sum.fetch_sub(n, Ordering::Release);
    }

    fn add_queued(&self) {
        self.sum.fetch_add(1, Ordering::Release);
    }

    fn remove_queued(&self) {
        self.sum.fetch_sub(1, Ordering::Release);
    }

    fn pushback_factor(&self, len: usize) -> (f32, f32) {
        let sum = self.sum.load(Ordering::Acquire);
        let nq = self.nq.load(Ordering::Acquire);
        let mean = if nq <= 1 {
            sum as f32
        } else {
            // take() has already popped locally, but sum still includes that item.
            let own_before_take = len.saturating_add(1);
            let other_sum = sum.saturating_sub(own_before_take);
            other_sum as f32 / (nq - 1) as f32
        };
        let mean = f32::max(1., mean);
        let pfactor = len as f32 / mean;
        (pfactor, mean)
    }
}

struct QSend {
    m: Response,
    pfactor: f32,
    qdepth: f32,
}

struct Queued {
    queue: VecDeque<Response>,
    stats: Arc<QStats>,
}

impl Drop for Queued {
    fn drop(&mut self) {
        self.stats.remove_queue(self.queue.len())
    }
}

impl Queued {
    fn new(stats: Arc<QStats>) -> Self {
        stats.add_queue();
        Self { queue: VecDeque::new(), stats }
    }

    fn queue_send(&mut self, m: Response) {
        self.queue.push_front(m);
        self.stats.add_queued();
    }

    fn take(&mut self) -> Option<QSend> {
        let m = self.queue.pop_back()?;
        let (pfactor, qdepth) = self.stats.pushback_factor(self.queue.len());
        self.stats.remove_queued();
        Some(QSend { pfactor, qdepth, m })
    }

    fn put_back(&mut self, m: Response) {
        self.queue.push_back(m);
        self.stats.add_queued();
    }

    async fn take_sendable(&mut self, sleep: &mut Pin<&mut Sleep>) -> QSend {
        sleep.await;
        match self.take() {
            None => future::pending().await,
            Some(m) => m,
        }
    }
}

async fn ws_tx_task(
    mut msgs: tmpsc::Receiver<Message>,
    mut ws: SplitSink<WebSocket, Message>,
) {
    loop {
        let Some(m) = msgs.recv().await else { return };
        if let Err(e) = ws.send(m).await {
            warn!("failed to send to client {e:?}");
            return;
        }
    }
}

async fn handle_client(
    stats: Arc<QStats>,
    publisher: Publisher,
    subscriber: Subscriber,
    ws: WebSocket,
    timeout: Option<Duration>,
    disconnect_pfactor: DisconnectPfactor,
) -> Result<()> {
    static UPDATES: LazyLock<Pool<Vec<Update>>> = LazyLock::new(|| Pool::new(50, 10000));
    let mut queue = Queued::new(stats);
    let (tx_up, mut rx_up) = mpsc::channel::<GPooled<Vec<(SubId, Event)>>>(3);
    let mut ctx = ClientCtx::new(publisher, subscriber, tx_up);
    let (tx_ws, rx_ws) = ws.split();
    let (mut tx_cl, rx_cl) = tmpsc::channel(3);
    task::spawn(ws_tx_task(rx_cl, tx_ws));
    let mut input_batch: Vec<result::Result<Message, warp::Error>> = Vec::new();
    let mut rx_ws = Batched::new(rx_ws.fuse(), 10_000);
    let mut calls_pending: FuturesUnordered<PendingCall> = FuturesUnordered::new();
    let sleep = time::sleep(Duration::from_millis(0));
    tokio::pin!(sleep);
    calls_pending.push(Box::pin(async { future::pending().await }) as PendingCall);
    loop {
        select_biased! {
            qsend = queue.take_sendable(&mut sleep).fuse() => {
                if !reply(
                    &mut tx_cl,
                    &qsend,
                    timeout,
                    disconnect_pfactor
                ).await? {
                    sleep.as_mut().reset(Instant::now() + Duration::from_millis(10));
                    queue.put_back(qsend.m)
                }
            },
            (cid, res) = calls_pending.select_next_some() => match res {
                Ok(result) => queue.queue_send(Response::CallSuccess { id: cid, result }),
                Err(e) => {
                    let error = format!("rpc call failed {}", e);
                    queue.queue_send(Response::CallFailed { id: cid, error })
                }
            },
            r = rx_ws.next() => match r {
                None => return Ok(()),
                Some(BatchItem::InBatch(r)) => input_batch.push(r),
                Some(BatchItem::EndBatch) => {
                    ctx.process_from_client(
                        &mut queue,
                        &mut input_batch,
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
                queue.queue_send(Response::Update { updates });
            }
        }
    }
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
    filter_with_options(
        publisher,
        subscriber,
        path,
        timeout,
        config::DEFAULT_DISCONNECT_PFACTOR,
    )
}

fn filter_with_options(
    publisher: Publisher,
    subscriber: Subscriber,
    path: &'static str,
    timeout: Option<Duration>,
    disconnect_pfactor: DisconnectPfactor,
) -> BoxedFilter<(impl Reply,)> {
    let stats = Arc::new(QStats::default());
    warp::path(path)
        .and(warp::ws())
        .map(move |ws: Ws| {
            let (publisher, subscriber) = (publisher.clone(), subscriber.clone());
            let stats = stats.clone();
            ws.on_upgrade(move |ws| {
                let (publisher, subscriber) = (publisher.clone(), subscriber.clone());
                let stats = stats.clone();
                async move {
                    if let Err(e) = handle_client(
                        stats,
                        publisher,
                        subscriber,
                        ws,
                        timeout,
                        disconnect_pfactor,
                    )
                    .await
                    {
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
    timeout: Option<Duration>,
) -> Result<()> {
    let routes = filter_with_options(
        publisher,
        subscriber,
        "ws",
        timeout,
        config.disconnect_pfactor,
    );
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::error::TryRecvError;

    const DISCONNECT_PFACTOR: DisconnectPfactor = config::DEFAULT_DISCONNECT_PFACTOR;

    fn response() -> Response {
        Response::Error { error: String::new() }
    }

    fn full_channel() -> (tmpsc::Sender<Message>, tmpsc::Receiver<Message>) {
        let (tx, rx) = tmpsc::channel(1);
        tx.try_send(Message::text("full")).unwrap();
        (tx, rx)
    }

    fn assert_only_prefilled_message(rx: &mut tmpsc::Receiver<Message>) {
        assert_eq!(rx.try_recv().unwrap().to_str().unwrap(), "full");
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    fn queue_responses(queue: &mut Queued, n: usize) {
        for _ in 0..n {
            queue.queue_send(response());
        }
    }

    #[tokio::test]
    async fn cancelled_reply_to_full_channel_does_not_duplicate_message() {
        let (mut tx, mut rx) = full_channel();
        let qsend = QSend { m: response(), pfactor: 100., qdepth: 1. };

        assert!(!reply(&mut tx, &qsend, None, DISCONNECT_PFACTOR).await.unwrap());
        assert_only_prefilled_message(&mut rx);
    }

    #[tokio::test]
    async fn pathological_queue_disconnect_does_not_enqueue_message() {
        let (mut tx, mut rx) = full_channel();
        let qsend = QSend { m: response(), pfactor: 200., qdepth: 1. };

        assert!(reply(&mut tx, &qsend, None, DISCONNECT_PFACTOR).await.is_err());
        assert_only_prefilled_message(&mut rx);
    }

    #[tokio::test]
    async fn configured_disconnect_pfactor_sets_disconnect_threshold() {
        let (mut tx, mut rx) = full_channel();
        let qsend = QSend { m: response(), pfactor: 50., qdepth: 1. };

        assert!(reply(&mut tx, &qsend, None, DisconnectPfactor::Threshold(50.))
            .await
            .is_err());
        assert_only_prefilled_message(&mut rx);
    }

    #[tokio::test]
    async fn infinite_queue_does_not_disconnect_pathological_queue() {
        let (mut tx, mut rx) = full_channel();
        let qsend = QSend { m: response(), pfactor: 200., qdepth: 1. };

        assert!(!reply(&mut tx, &qsend, None, DisconnectPfactor::Never).await.unwrap());
        assert_only_prefilled_message(&mut rx);
    }

    #[tokio::test]
    async fn pfactor_above_100_still_uses_calculated_wait() {
        let (mut tx, mut rx) = full_channel();
        let qsend = QSend { m: response(), pfactor: 150., qdepth: 1000. };

        assert!(time::timeout(
            Duration::from_millis(1),
            reply(&mut tx, &qsend, None, DisconnectPfactor::Threshold(2000.))
        )
        .await
        .is_err());
        assert_only_prefilled_message(&mut rx);
    }

    #[tokio::test]
    async fn two_client_outlier_retries_without_waiting() {
        let stats = Arc::new(QStats::default());
        let mut slow = Queued::new(stats.clone());
        let _fast = Queued::new(stats.clone());
        queue_responses(&mut slow, 50);

        let qsend = slow.take().unwrap();
        assert_eq!(qsend.qdepth, 1.);
        assert_eq!(qsend.pfactor, 49.);
        let (mut tx, mut rx) = full_channel();

        let sent = time::timeout(
            Duration::from_millis(5),
            reply(&mut tx, &qsend, None, DISCONNECT_PFACTOR),
        )
        .await
        .expect("outlier retry should not wait")
        .unwrap();
        assert!(!sent);
        assert_only_prefilled_message(&mut rx);
    }

    #[tokio::test]
    async fn two_client_global_backlog_waits_before_retrying() {
        let stats = Arc::new(QStats::default());
        let mut slow = Queued::new(stats.clone());
        let mut also_slow = Queued::new(stats.clone());
        queue_responses(&mut slow, 50);
        queue_responses(&mut also_slow, 50);

        let qsend = slow.take().unwrap();
        assert!(qsend.pfactor < 1.);
        assert_eq!(qsend.qdepth, 50.);
        let (mut tx, mut rx) = full_channel();

        assert!(
            time::timeout(
                Duration::from_millis(5),
                reply(&mut tx, &qsend, None, DISCONNECT_PFACTOR),
            )
            .await
            .is_err(),
            "global backlog should impose a nonzero wait"
        );
        assert_only_prefilled_message(&mut rx);
    }

    #[tokio::test]
    async fn single_client_backlog_waits_before_retrying() {
        let stats = Arc::new(QStats::default());
        let mut queue = Queued::new(stats);
        queue_responses(&mut queue, 50);

        let qsend = queue.take().unwrap();
        assert!(qsend.pfactor < 1.);
        assert_eq!(qsend.qdepth, 50.);
        let (mut tx, mut rx) = full_channel();

        assert!(
            time::timeout(
                Duration::from_millis(5),
                reply(&mut tx, &qsend, None, DISCONNECT_PFACTOR),
            )
            .await
            .is_err(),
            "a single client backlog is a global backlog"
        );
        assert_only_prefilled_message(&mut rx);
    }

    #[tokio::test]
    async fn two_client_pathological_outlier_disconnects() {
        let stats = Arc::new(QStats::default());
        let mut slow = Queued::new(stats.clone());
        let _fast = Queued::new(stats.clone());
        queue_responses(&mut slow, 201);

        let qsend = slow.take().unwrap();
        assert_eq!(qsend.qdepth, 1.);
        assert_eq!(qsend.pfactor, 200.);
        let (mut tx, mut rx) = full_channel();

        assert!(reply(&mut tx, &qsend, None, DISCONNECT_PFACTOR).await.is_err());
        assert_only_prefilled_message(&mut rx);
    }

    #[tokio::test]
    async fn retry_sleep_survives_cancelled_take_attempts() {
        let stats = Arc::new(QStats::default());
        let mut queue = Queued::new(stats);
        queue.queue_send(response());
        let sleep = time::sleep(Duration::from_millis(30));
        tokio::pin!(sleep);

        assert!(time::timeout(Duration::from_millis(5), queue.take_sendable(&mut sleep))
            .await
            .is_err());
        assert_eq!(queue.queue.len(), 1);

        let qsend =
            time::timeout(Duration::from_millis(100), queue.take_sendable(&mut sleep))
                .await
                .expect("retry sleep should keep running after cancellation");
        assert!(matches!(qsend.m, Response::Error { .. }));
        assert_eq!(queue.queue.len(), 0);
    }
}
