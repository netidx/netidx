use crate::{
    path::Path,
    utils::{BatchItem, Batched},
    resolver::{Resolver, ReadOnly},
    channel::Channel,
    protocol::publisher::*,
    config,
};
use std::{
    mem, iter,
    result::Result,
    marker::PhantomData,
    collections::{HashMap, hash_map::Entry},
    net::SocketAddr,
    sync::{Arc, Weak},
    cmp::min,
    time::Duration,
};
use tokio::{
    task,
    sync::oneshot,
    net::TcpStream,
    time::{self, Instant, Delay},
};
use fxhash::FxBuildHasher;
use futures::{
    prelude::*,
    select,
    channel::mpsc::{self, Sender, UnboundedReceiver, UnboundedSender},
};
use rand::Rng;
use serde::de::DeserializeOwned;
use failure::Error;
use bytes::Bytes;
use parking_lot::Mutex;

const BATCH: usize = 100_000;

#[derive(Debug)]
struct SubscribeRequest {
    path: Path,
    finished: oneshot::Sender<Result<SubscriptionUt, Error>>,
    con: UnboundedSender<ToCon>,
}

#[derive(Debug)]
enum ToCon {
    Subscribe(SubscribeRequest),
    Unsubscribe(Id),
    Last(Id, oneshot::Sender<Bytes>),
    Stream {
        id: Id,
        tx: Sender<Bytes>,
        last: bool,
    }
}

#[derive(Debug)]
struct SubscriptionUtInner {
    id: Id,
    addr: SocketAddr,
    connection: UnboundedSender<ToCon>,
}

impl Drop for SubscriptionUtInner {
    fn drop(&mut self) {
        let _ = self.connection.send(ToCon::Unsubscribe(self.id));
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionUtWeak(Weak<SubscriptionUtInner>);

impl SubscriptionUtWeak {
    pub fn upgrade(&self) -> Option<SubscriptionUt> {
        Weak::upgrade(&self.0).map(|r| SubscriptionUt(r))
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionUt(Arc<SubscriptionUtInner>);

impl SubscriptionUt {
    pub fn typed<T: DeserializeOwned>(self) -> Subscription<T> {
        Subscription(self, PhantomData)
    }

    pub fn downgrade(&self) -> SubscriptionUtWeak {
        SubscriptionUtWeak(Arc::downgrade(&self.0))
    }

    /// Get the last published value.
    pub async fn last(&self) -> Option<Bytes> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.connection.unbounded_send(ToCon::Last(self.0.id, tx));
        match rx.await {
            Ok(b) => Some(b),
            Err(_) => None
        }
    }

    /// Get a stream of published values. Values will arrive in the
    /// order they are published. No value will be omitted. If
    /// `begin_with_last` is true, then the stream will start with the
    /// last published value at the time `updates` is called, and will
    /// then receive any updated values. Otherwise the stream will
    /// only receive updated values. If you only want to get the last
    /// value one time, it's cheaper to call `last`.
    ///
    /// If the subscription dies the stream will end.
    pub fn updates(
        &self,
        begin_with_last: bool
    ) -> impl Stream<Item = Bytes> {
        let (tx, rx) = mpsc::channel(10);
        let m = ToCon::Stream { tx, last: begin_with_last, id: self.0.id };
        let _ = self.0.connection.unbounded_send(m);
        rx
    }
}

/// A typed version of SubscriptionUt
#[derive(Debug, Clone)]
pub struct Subscription<T: DeserializeOwned>(SubscriptionUt, PhantomData<T>);

impl<T: DeserializeOwned> Subscription<T> {
    /// Get the `SubscriptionUt`
    pub fn untyped(self) -> SubscriptionUt {
        self.0
    }

    /// Get and decode the last published value.
    pub async fn last(&self) -> Option<Result<T, rmp_serde::decode::Error>> {
        self.0.last().await.map(|v| Ok(rmp_serde::decode::from_read(&*v)?))
    }

    /// Same as `SubscriptionUt::updates` but it decodes the value
    pub fn updates(
        &self,
        begin_with_last: bool
    ) -> impl Stream<Item = Result<T, rmp_serde::decode::Error>> {
        self.0.updates(begin_with_last).map(|v| rmp_serde::decode::from_read(&*v))
    }
}

#[derive(Debug, Copy, Clone)]
pub enum DSState {
    Subscribed,
    Unsubscribed,
}

#[derive(Debug)]
struct DurableSubscriptionUtInner {
    sub: Option<SubscriptionUt>,
    streams: Vec<Sender<Bytes>>,
    states: Vec<UnboundedSender<DSState>>,
    tries: usize,
    next_try: Instant,
}

#[derive(Debug, Clone)]
struct DurableSubscriptionUtWeak(Weak<Mutex<DurableSubscriptionUtInner>>);

impl DurableSubscriptionUtWeak {
    fn upgrade(&self) -> Option<DurableSubscriptionUt> {
        Weak::upgrade(&self.0).map(|s| DurableSubscriptionUt(s))
    }
}

#[derive(Debug, Clone)]
pub struct DurableSubscriptionUt(Arc<Mutex<DurableSubscriptionUtInner>>);

impl DurableSubscriptionUt {
    pub fn typed<T: DeserializeOwned>(self) -> DurableSubscription<T> {
        DurableSubscription(self, PhantomData)
    }

    fn downgrade(&self) -> DurableSubscriptionUtWeak {
        DurableSubscriptionUtWeak(Arc::downgrade(&self.0))
    }

    pub async fn last(&self) -> Option<Bytes> {
        let sub = self.0.lock().sub.clone();
        match sub {
            None => None,
            Some(sub) => sub.last().await
        }
    }

    /// Return a stream that produces a value when the state of the
    /// subscription changes. The initial state is unsubscribed.
    pub fn state(&self) -> impl Stream<Item = DSState> {
        let (tx, rx) = mpsc::unbounded();
        let mut t = self.0.lock();
        t.states.retain(|c| !c.is_closed());
        t.states.push(tx);
        rx
    }

    pub fn updates(
        &self,
        begin_with_last: bool
    ) -> impl Stream<Item = Bytes> {
        let mut t = self.0.lock();
        let (tx, rx) = mpsc::channel(10);
        t.streams.retain(|c| !c.is_closed());
        t.streams.push(tx.clone());
        if let Some(ref sub) = t.sub {
            let m = ToCon::Stream {tx, last: begin_with_last, id: sub.0.id };
            let _ = sub.0.connection.unbounded_send(m);
        }
        rx
    }
}

#[derive(Debug, Clone)]
pub struct DurableSubscription<T: DeserializeOwned>(
    DurableSubscriptionUt,
    PhantomData<T>
);

impl<T: DeserializeOwned> DurableSubscription<T> {
    pub fn untyped(self) -> DurableSubscriptionUt {
        self.0
    }

    pub async fn last(&self) -> Option<Result<T, rmp_serde::decode::Error>> {
        self.0.last().await.map(|v| Ok(rmp_serde::decode::from_read(&*v)?))
    }

    pub fn updates(
        &self,
        begin_with_last: bool
    ) -> impl Stream<Item = Result<T, rmp_serde::decode::Error>> {
        self.0.updates(begin_with_last).map(|v| rmp_serde::decode::from_read(&*v))
    }
}

enum SubStatus {
    Subscribed(SubscriptionUtWeak),
    Pending(Vec<oneshot::Sender<Result<SubscriptionUt, Error>>>),
}

struct SubscriberInner {
    resolver: Resolver<ReadOnly>,
    connections: HashMap<SocketAddr, UnboundedSender<ToCon>, FxBuildHasher>,
    subscribed: HashMap<Path, SubStatus>,
    durable_dead: HashMap<Path, DurableSubscriptionUtWeak>,
    durable_alive: HashMap<Path, DurableSubscriptionUtWeak>,
    trigger_resub: UnboundedSender<()>,
}

struct SubscriberWeak(Weak<Mutex<SubscriberInner>>);

impl SubscriberWeak {
    fn upgrade(&self) -> Option<Subscriber> {
        Weak::upgrade(&self.0).map(|s| Subscriber(s))
    }
}

#[derive(Clone)]
pub struct Subscriber(Arc<Mutex<SubscriberInner>>);

impl Subscriber {
    pub fn new(resolver: config::Resolver) -> Result<Subscriber, Error> {
        let (tx, rx) = mpsc::unbounded();
        let t = Subscriber(Arc::new(Mutex::new(SubscriberInner {
            resolver: Resolver::<ReadOnly>::new_r(resolver)?,
            connections: HashMap::with_hasher(FxBuildHasher::default()),
            subscribed: HashMap::new(),
            durable_dead: HashMap::new(),
            durable_alive: HashMap::new(),
            trigger_resub: tx,
        })));
        t.start_resub_task(rx);
        Ok(t)
    }

    fn downgrade(&self) -> SubscriberWeak {
        SubscriberWeak(Arc::downgrade(&self.0))
    }

    fn start_resub_task(&self, incoming: UnboundedReceiver<()>) {
        async fn wait_retry(retry: &mut Option<Delay>) {
            match retry {
                None => future::pending().await,
                Some(d) => d.await,
            }
        }
        fn update_retry(subscriber: &mut SubscriberInner, retry: &mut Option<Delay>) {
            *retry = subscriber.durable_dead.values()
                .filter_map(|w| w.upgrade())
                .map(|ds| ds.0.lock().next_try)
                .fold(None, |min, v| match min {
                    None => Some(v),
                    Some(min) => if v < min {
                        Some(v)
                    } else {
                        Some(min)
                    }
                })
                .map(|t| time::delay_until(t + Duration::from_secs(1)));
        }
        async fn do_resub(subscriber: &SubscriberWeak, retry: &mut Option<Delay>) {
            if let Some(subscriber) = subscriber.upgrade() {
                let now = Instant::now();
                let mut batch = {
                    let mut b = HashMap::new();
                    let mut gc = Vec::new();
                    let mut subscriber = subscriber.0.lock();
                    for (p, w) in &subscriber.durable_dead {
                        match w.upgrade() {
                            None => { gc.push(p.clone()); }
                            Some(s) => {
                                let next_try = s.0.lock().next_try;
                                if next_try <= now {
                                    b.insert(p.clone(), s);
                                }
                            }
                        }
                    }
                    for p in gc {
                        subscriber.durable_dead.remove(&p);
                    }
                    b
                };
                if batch.len() == 0 {
                    let mut subscriber = subscriber.0.lock();
                    update_retry(&mut *subscriber, retry);
                } else {
                    let r = subscriber.subscribe_ut(batch.keys().cloned()).await;
                    let mut subscriber = subscriber.0.lock();
                    let now = Instant::now();
                    for (p, r) in r {
                        let mut ds = batch.get_mut(&p).unwrap().0.lock();
                        match r {
                            Err(_) => { // CR estokes: log this error?
                                ds.tries += 1;
                                ds.next_try = now + Duration::from_secs(ds.tries as u64);
                            },
                            Ok(sub) => {
                                ds.tries = 0;
                                let mut i = 0;
                                while i < ds.states.len() {
                                    match
                                        ds.states[i].unbounded_send(DSState::Subscribed)
                                    {
                                        Ok(()) => { i += 1; }
                                        Err(_) => { ds.states.remove(i); }
                                    }
                                }
                                ds.streams.retain(|c| !c.is_closed());
                                for tx in ds.streams.iter().cloned() {
                                    let _ =
                                        sub.0.connection.unbounded_send(ToCon::Stream {
                                        tx, last: true, id: sub.0.id
                                    });
                                }
                                ds.sub = Some(sub);
                                let w = subscriber.durable_dead.remove(&p).unwrap();
                                subscriber.durable_alive.insert(p.clone(), w.clone());
                            },
                        }
                    }
                    update_retry(&mut *subscriber, retry);
                }
            }
        }
        let subscriber = self.downgrade();
        task::spawn(async move {
            let mut incoming = Batched::new(incoming, 100_000);
            let mut retry: Option<Delay> = None;
            loop {
                select! {
                    _ = wait_retry(&mut retry).fuse() => {
                        do_resub(&subscriber, &mut retry).await;
                    },
                    m = incoming.next() => match m {
                        None => break,
                        Some(BatchItem::InBatch(())) => (),
                        Some(BatchItem::EndBatch) => {
                            do_resub(&subscriber, &mut retry).await;
                        }
                    },
                }
            }
        });
    }
    
    /// Subscribe to the specified set of paths.
    ///
    /// Path resolution and subscription are done in parallel, so the
    /// lowest latency per subscription will be achieved with larger
    /// batches.
    ///
    /// In case you are already subscribed to one or more of the paths
    /// in the batch, you will receive a reference to the existing
    /// subscription, no additional messages will be sent.
    ///
    /// It is safe to call this function concurrently with the same or
    /// overlapping sets of paths in the batch, only one subscription
    /// attempt will be made concurrently, and the result of that one
    /// attempt will be given to each concurrent caller upon success
    /// or failure.
    pub async fn subscribe_ut(
        &self, batch: impl IntoIterator<Item = Path>,
    ) -> Vec<(Path, Result<SubscriptionUt, Error>)> {
        enum St {
            Resolve,
            Subscribing(oneshot::Receiver<Result<SubscriptionUt, Error>>),
            WaitingOther(oneshot::Receiver<Result<SubscriptionUt, Error>>),
            Subscribed(SubscriptionUt),
            Error(Error),
        }
        let paths = batch.into_iter().collect::<Vec<_>>();
        let mut pending: HashMap<Path, St> = HashMap::new();
        let mut r = { // Init
            let mut t = self.0.lock();
            for p in paths.clone() {
                match t.subscribed.entry(p.clone()) {
                    Entry::Vacant(e) => {
                        e.insert(SubStatus::Pending(vec![]));
                        pending.insert(p, St::Resolve);
                    }
                    Entry::Occupied(mut e) => match e.get_mut() {
                        SubStatus::Pending(ref mut v) => {
                            let (tx, rx) = oneshot::channel();
                            v.push(tx);
                            pending.insert(p, St::WaitingOther(rx));
                        }
                        SubStatus::Subscribed(r) => match r.upgrade() {
                            Some(r) => { pending.insert(p, St::Subscribed(r)); }
                            None => {
                                e.insert(SubStatus::Pending(vec![]));
                                pending.insert(p, St::Resolve);
                            }
                        },
                    }
                }
            }
            t.resolver.clone()
        };
        fn pick(n: usize) -> usize {
            let mut rng = rand::thread_rng();
            rng.gen_range(0, n)
        }
        { // Resolve, Connect, Subscribe
            let to_resolve =
                pending.iter()
                .filter(|(_, s)| match s { St::Resolve => true, _ => false })
                .map(|(p, _)| p.clone())
                .collect::<Vec<_>>();
            match r.resolve(to_resolve.clone()).await {
                Err(e) => for p in to_resolve {
                    pending.insert(p.clone(), St::Error(
                        format_err!("resolving path: {} failed: {}", p, e)
                    ));
                }
                Ok(addrs) => {
                    let mut t = self.0.lock();
                    for (p, addrs) in to_resolve.into_iter().zip(addrs.into_iter()) {
                        if addrs.len() == 0 {
                            pending.insert(p, St::Error(format_err!("path not found")));
                        } else {
                            let addr = {
                                if addrs.len() == 1 {
                                    addrs[0]
                                } else {
                                    addrs[pick(addrs.len())]
                                }
                            };
                            let con =
                                t.connections.entry(addr)
                                .or_insert_with(|| {
                                    let (tx, rx) = mpsc::unbounded();
                                    task::spawn(connection(self.downgrade(), addr, rx));
                                    tx
                                });
                            let (tx, rx) = oneshot::channel();
                            let con_ = con.clone();
                            let r =
                                con.unbounded_send(ToCon::Subscribe(SubscribeRequest {
                                    con: con_,
                                    path: p.clone(),
                                    finished: tx,
                                }));
                            match r {
                                Ok(()) => { pending.insert(p, St::Subscribing(rx)); }
                                Err(e) => {
                                    pending.insert(p, St::Error(Error::from(e)));
                                }
                            }
                        }
                    }
                }
            }
        }
        // Wait
        for (path, st) in pending.iter_mut() {
            match st {
                St::Resolve => unreachable!(),
                St::Subscribed(_) | St::Error(_) => (),
                St::WaitingOther(w) => match w.await {
                    Err(_) => *st = St::Error(format_err!("other side died")),
                    Ok(Err(e)) => *st = St::Error(e),
                    Ok(Ok(raw)) => *st = St::Subscribed(raw),
                }
                St::Subscribing(w) => {
                    let res = match w.await {
                        Err(_) => Err(format_err!("connection died")),
                        Ok(Err(e)) => Err(e),
                        Ok(Ok(raw)) => Ok(raw),
                    };
                    let mut t = self.0.lock();
                    match t.subscribed.entry(path.clone()) {
                        Entry::Vacant(_) => unreachable!(),
                        Entry::Occupied(mut e) => match res {
                            Err(err) => match e.remove() {
                                SubStatus::Subscribed(_) => unreachable!(),
                                SubStatus::Pending(waiters) => {
                                    for w in waiters {
                                        let err = Err(format_err!("{}", err));
                                        let _ = w.send(err);
                                    }
                                    *st = St::Error(err);
                                }
                            }
                            Ok(raw) => {
                                let s = mem::replace(
                                    e.get_mut(),
                                    SubStatus::Subscribed(raw.downgrade())
                                );
                                match s {
                                    SubStatus::Subscribed(_) => unreachable!(),
                                    SubStatus::Pending(waiters) => {
                                        for w in waiters {
                                            let _ = w.send(Ok(raw.clone()));
                                        }
                                        *st = St::Subscribed(raw);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        paths.into_iter().map(|p| match pending.remove(&p).unwrap() {
            St::Resolve | St::Subscribing(_) | St::WaitingOther(_) => unreachable!(),
            St::Subscribed(raw) => (p, Ok(raw)),
            St::Error(e) => (p, Err(e))
        }).collect()
    }

    pub async fn subscribe<T: DeserializeOwned>(
        &self, batch: impl IntoIterator<Item = Path>,
    ) -> Vec<(Path, Result<Subscription<T>, Error>)> {
        self.subscribe_ut(batch).await.into_iter().map(|(p, r)| {
            (p, r.map(|r| r.typed()))
        }).collect()
    }

    pub async fn subscribe_one_ut(&self, path: Path) -> Result<SubscriptionUt, Error> {
        self.subscribe_ut(iter::once(path)).await.pop().unwrap().1
    }

    /// Subscribe to one path. This is sufficient for a small number
    /// of paths, but if you need to subscribe to a lot of things use
    /// `subscribe`
    pub async fn subscribe_one<T: DeserializeOwned>(
        &self,
        path: Path
    ) -> Result<Subscription<T>, Error> {
        self.subscribe_one_ut(path).await.map(|v| v.typed())
    }

    /// A durable subscription will subscribe to `path` and attempt to
    /// remain subscribed until dropped. As such, unlike a regular
    /// subscription the stream returned by
    /// `DurableSubscritionUt::updates` will never end, but it also
    /// may not contain all values the publisher publishes, or it may
    /// contain duplicate values, though both of these things will
    /// only happen if the connection is lost and reestablished.
    ///
    /// A background task in subscriber will attempt to resubscribe to
    /// any subscription that becomes unsubscribed. One attempt will
    /// be made immediatly upon the subscription being dropped, and
    /// after that linear backoff will be used.
    ///
    /// Batching of subscriptions is automatic, if you create a lot of
    /// durable subscriptions all at once batching will minimize the
    /// number of messages exchanged with both the resolver server and
    /// the publishers.
    ///
    /// Durable subscriptions do use more memory, and have some small
    /// overhead when fetching the last, however the overhead of
    /// `updates` is exactly the same as a regular subscription.
    ///
    /// As with regular subscriptions there is really only ever one
    /// subscription for a given path, calling `subscribe_durable_ut`
    /// again for the same path will just return another pointer to it.
    pub fn subscribe_durable_ut(&self, path: Path) -> DurableSubscriptionUt {
        let mut t = self.0.lock();
        if let Some(s) =
            t.durable_dead.get(&path).or_else(|| t.durable_alive.get(&path))
        {
            if let Some(s) = s.upgrade() {
                return s;
            }
        }
        let s = DurableSubscriptionUt(Arc::new(Mutex::new(DurableSubscriptionUtInner {
            sub: None,
            streams: Vec::new(),
            states: Vec::new(),
            tries: 0,
            next_try: Instant::now(),
        })));
        t.durable_dead.insert(path, s.downgrade());
        let _ = t.trigger_resub.unbounded_send(());
        s
    }

    pub fn subscribe_durable<T: DeserializeOwned>(
        &self, path: Path
    ) -> DurableSubscription<T> {
        self.subscribe_durable_ut(path).typed()
    }
}

struct Sub {
    path: Path,
    streams: Vec<Sender<Bytes>>,
    last: Bytes,
}

async fn handle_val(
    subscriptions: &mut HashMap<Id, Sub, FxBuildHasher>,
    next_sub: &mut Option<SubscribeRequest>,
    id: Id,
    addr: SocketAddr,
    msg: Bytes,
) {
    match subscriptions.entry(id) {
        Entry::Occupied(mut e) => {
            let sub = e.get_mut();
            let mut i = 0;
            while i < sub.streams.len() {
                match sub.streams[i].send(msg.clone()).await {
                    Ok(()) => { i += 1; }
                    Err(_) => { sub.streams.remove(i); }
                }
            }
            sub.last = msg;
        }
        Entry::Vacant(e) => if let Some(req) = next_sub.take() {
            e.insert(Sub {
                path: req.path,
                last: msg,
                streams: Vec::new(),
            });
            let s = SubscriptionUtInner { id, addr, connection: req.con };
            let _ = req.finished.send(Ok(SubscriptionUt(Arc::new(s))));
        }
    }
}

fn unsubscribe(
    subscriber: &mut SubscriberInner,
    sub: Sub,
    id: Id,
    addr: SocketAddr,
) {
    if let Some(dsw) = subscriber.durable_alive.remove(&sub.path) {
        if let Some(ds) = dsw.upgrade() {
            let mut inner = ds.0.lock();
            inner.sub = None;
            let mut i = 0;
            while i < inner.states.len() {
                match inner.states[i].unbounded_send(DSState::Unsubscribed) {
                    Ok(()) => { i+= 1; }
                    Err(_) => { inner.states.remove(i); }
                }
            }
            subscriber.durable_dead.insert(sub.path.clone(), dsw);
            let _ = subscriber.trigger_resub.unbounded_send(());
        }
    }
    match subscriber.subscribed.entry(sub.path) {
        Entry::Vacant(_) => (),
        Entry::Occupied(e) => match e.get() {
            SubStatus::Pending(_) => (),
            SubStatus::Subscribed(s) => match s.upgrade() {
                None => { e.remove(); }
                Some(s) => if s.0.id == id && s.0.addr == addr { e.remove(); }
            }
        }
    }
}

fn handle_control(
    addr: SocketAddr,
    subscriber: &Subscriber,
    pending: &mut HashMap<Path, SubscribeRequest>,
    outstanding: &mut Vec<ToPublisher>,
    subscriptions: &mut HashMap<Id, Sub, FxBuildHasher>,
    next_val: &mut Option<Id>,
    next_sub: &mut Option<SubscribeRequest>,
    msg: &[u8]
) -> Result<(), Error> {
    match rmp_serde::decode::from_read::<&[u8], FromPublisher>(&*msg) {
        Err(e) => return Err(Error::from(e)),
        Ok(FromPublisher::Message(id)) => { *next_val = Some(id); }
        Ok(FromPublisher::NoSuchValue(path)) => {
            outstanding.pop();
            if let Some(r) = pending.remove(&path) {
                let _ = r.finished.send(Err(format_err!("no such value")));
            }
        }
        Ok(FromPublisher::Subscribed(path, id)) => {
            outstanding.pop();
            match pending.remove(&path) {
                None => return Err(format_err!("unsolicited: {}", path)),
                Some(req) => {
                    *next_val = Some(id);
                    *next_sub = Some(req);
                }
            }
        }
        Ok(FromPublisher::Unsubscribed(id)) => {
            // CR estokes: only do this if we asked to unsubscribe
            outstanding.pop();
            if let Some(s) = subscriptions.remove(&id) {
                let mut t = subscriber.0.lock();
                unsubscribe(&mut *t, s, id, addr);
            }
        }
    }
    Ok(())
}

macro_rules! try_brk {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => break Err(Error::from(e))
        }
    }
}

async fn connection(
    subscriber: SubscriberWeak,
    to: SocketAddr,
    from_sub: UnboundedReceiver<ToCon>
) -> Result<(), Error> {
    let mut from_sub = Batched::new(from_sub, BATCH);
    let mut pending: HashMap<Path, SubscribeRequest> = HashMap::new();
    let mut subscriptions: HashMap<Id, Sub, FxBuildHasher> =
        HashMap::with_hasher(FxBuildHasher::default());
    let mut next_val: Option<Id> = None;
    let mut next_sub: Option<SubscribeRequest> = None;
    let mut con = Channel::new(TcpStream::connect(to).await?);
    let mut batch: Vec<Bytes> = Vec::new();
    let mut outstanding: Vec<ToPublisher> = Vec::new();
    let mut queued: Vec<ToPublisher> = Vec::new();
    async fn flush(
        outstanding: &mut Vec<ToPublisher>,
        queued: &mut Vec<ToPublisher>,
        con: &mut Channel,
    ) -> Result<(), Error> {
        if outstanding.len() == 0 && queued.len() > 0 {
            outstanding.extend(queued.drain(0..min(BATCH, queued.len())));
            for m in outstanding.iter() {
                con.queue_send(m)?
            }
            con.flush().await?;
        }
        Ok(())
    }
    let res = 'main: loop {
        select! {
            r = con.receive_batch_raw(&mut batch).fuse() => match r {
                Err(e) => break Err(Error::from(e)),
                Ok(()) => if let Some(subscriber) = subscriber.upgrade() {
                    for msg in batch.drain(..) {
                        match next_val.take() {
                            Some(id) => {
                                handle_val(
                                    &mut subscriptions, &mut next_sub, id, to, msg
                                ).await;
                            }
                            None => {
                                match handle_control(
                                    to, &subscriber, &mut pending, &mut outstanding,
                                    &mut subscriptions, &mut next_val, &mut next_sub,
                                    &*msg
                                ) {
                                    Ok(()) => (),
                                    Err(e) => break 'main Err(Error::from(e)),
                                }
                            }
                        }
                    }
                    try_brk!(flush(&mut outstanding, &mut queued, &mut con).await);
                }
            },
            msg = from_sub.next() => match msg {
                None => break Err(format_err!("dropped")),
                Some(BatchItem::EndBatch) => {
                    try_brk!(flush(&mut outstanding, &mut queued, &mut con).await);
                }
                Some(BatchItem::InBatch(ToCon::Subscribe(req))) => {
                    let path = req.path.clone();
                    pending.insert(path.clone(), req);
                    queued.push(ToPublisher::Subscribe(path));
                }
                Some(BatchItem::InBatch(ToCon::Unsubscribe(id))) => {
                    queued.push(ToPublisher::Unsubscribe(id));
                }
                Some(BatchItem::InBatch(ToCon::Last(id, tx))) => {
                    if let Some(sub) = subscriptions.get(&id) {
                        let _ = tx.send(sub.last.clone());
                    }
                }
                Some(BatchItem::InBatch(ToCon::Stream { id, mut tx, last })) => {
                    if let Some(sub) = subscriptions.get_mut(&id) {
                        if last {
                            let last = sub.last.clone();
                            let _ = tx.send(last).await;
                        }
                        sub.streams.retain(|c| !c.is_closed());
                        sub.streams.push(tx);
                    }
                }
            },
        }
    };
    if let Some(subscriber) = subscriber.upgrade() {
        let mut t = subscriber.0.lock();
        for (id, sub) in subscriptions {
            unsubscribe(&mut *t, sub, id, to);
        }
    }
    res
}

#[cfg(test)]
mod test {
    use std::{
        net::SocketAddr,
        time::Duration,
    };
    use tokio::{task, time, sync::oneshot, runtime::Runtime};
    use futures::{prelude::*, future::{self, Either}};
    use crate::{
        resolver_server::Server,
        publisher::{Publisher, BindCfg},
        subscriber::Subscriber,
        config,
    };

    async fn init_server() -> Server {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        Server::new(addr, 100).await.expect("start server")
    }

    #[test]
    fn publish_subscribe() {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct V {
            id: usize,
            v: String,
        };
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = init_server().await;
            let cfg = config::Resolver { addr: *server.local_addr()};
            let (tx, ready) = oneshot::channel();
            task::spawn(async move {
                let publisher = Publisher::new(cfg, BindCfg::Local).await.unwrap();
                let vp0 = publisher.publish(
                    "/app/v0".into(),
                    &V {id: 0, v: "foo".into()}
                ).unwrap();
                let vp1 = publisher.publish(
                    "/app/v1".into(),
                    &V {id: 0, v: "bar".into()}
                ).unwrap();
                publisher.flush(None).await.unwrap();
                tx.send(()).unwrap();
                let mut c = 1;
                loop {
                    time::delay_for(Duration::from_millis(100)).await;
                    vp0.update(&V {id: c, v: "foo".into()})
                        .unwrap();
                    vp1.update(&V {id: c, v: "bar".into()})
                        .unwrap();
                    publisher.flush(None).await.unwrap();
                    c += 1
                }
            });
            time::timeout(Duration::from_secs(1), ready).await.unwrap().unwrap();
            let subscriber = Subscriber::new(cfg).unwrap();
            let vs0 = subscriber.subscribe_one::<V>("/app/v0".into()).await.unwrap();
            let vs1 = subscriber.subscribe_one::<V>("/app/v1".into()).await.unwrap();
            let mut c0: Option<usize> = None;
            let mut c1: Option<usize> = None;
            let mut vs0s = vs0.updates(true);
            let mut vs1s = vs1.updates(true);
            loop {
                let r = Either::factor_first(
                    future::select(vs0s.next(), vs1s.next()).await
                ).0;
                match r {
                    None => panic!("publishers died"),
                    Some(Err(e)) => panic!("publisher error: {}", e),
                    Some(Ok(v)) => {
                        let c = match &*v.v {
                            "foo" => &mut c0,
                            "bar" => &mut c1,
                            _ => panic!("unexpected v"),
                        };
                        match c {
                            None => { *c = Some(v.id); },
                            Some(c_id) => {
                                assert_eq!(*c_id + 1, v.id);
                                if *c_id >= 50 {
                                    break;
                                }
                                *c = Some(v.id);
                            }
                        }
                    }
                }
            }
            drop(server);
        });
    }
}
