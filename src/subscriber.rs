pub use crate::protocol::publisher::v1::Value;
use crate::{
    channel::{Channel, ReadChannel, WriteChannel},
    chars::Chars,
    config::Config,
    os::{self, ClientCtx, Krb5Ctx},
    path::Path,
    pool::{Pool, Pooled},
    protocol::publisher::v1::{From, Id, To},
    resolver::{Auth, ResolverRead},
    utils::{self, BatchItem, Batched, ChanId, ChanWrap},
};
use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::{
    channel::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender},
    prelude::*,
    select, select_biased,
};
use fxhash::FxBuildHasher;
use log::{info, warn};
use parking_lot::Mutex;
use rand::Rng;
use std::{
    cmp::{max, Eq, PartialEq},
    collections::{hash_map::Entry, HashMap},
    error, fmt,
    hash::Hash,
    iter, mem,
    net::SocketAddr,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc::error::SendTimeoutError, oneshot},
    task,
    time::{self, Delay, Instant},
};

const BATCH: usize = 100_000;

#[derive(Debug)]
pub struct PermissionDenied;

impl fmt::Display for PermissionDenied {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "permission denied")
    }
}

impl error::Error for PermissionDenied {}

#[derive(Debug)]
pub struct NoSuchValue;

impl fmt::Display for NoSuchValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no such value")
    }
}

impl error::Error for NoSuchValue {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubId(u64);

impl SubId {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        SubId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug)]
struct SubscribeValRequest {
    path: Path,
    timestamp: u64,
    permissions: u32,
    token: Bytes,
    resolver: SocketAddr,
    finished: oneshot::Sender<Result<Val>>,
    con: UnboundedSender<ToCon>,
    deadline: Option<Instant>,
}

#[derive(Debug)]
enum ToCon {
    Subscribe(SubscribeValRequest),
    Unsubscribe(Id),
    Stream { id: Id, sub_id: SubId, tx: Sender<Pooled<Vec<(SubId, Value)>>>, last: bool },
    Write(Id, Value),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug)]
struct ValInner {
    sub_id: SubId,
    id: Id,
    addr: SocketAddr,
    connection: UnboundedSender<ToCon>,
    last: Arc<Mutex<Value>>,
}

impl Drop for ValInner {
    fn drop(&mut self) {
        let _ = self.connection.send(ToCon::Unsubscribe(self.id));
    }
}

#[derive(Debug, Clone)]
pub struct ValWeak(Weak<ValInner>);

impl ValWeak {
    pub fn upgrade(&self) -> Option<Val> {
        Weak::upgrade(&self.0).map(|r| Val(r))
    }
}

/// A non durable subscription to a value. If all user held references
/// to `Val` are dropped then it will be unsubscribed.
#[derive(Debug, Clone)]
pub struct Val(Arc<ValInner>);

impl Val {
    pub fn downgrade(&self) -> ValWeak {
        ValWeak(Arc::downgrade(&self.0))
    }

    /// Get the last published value, or None if the subscription is
    /// dead. Will only return `None` if the subscription is dead,
    /// otherwise there WILL be a value.
    pub fn last(&self) -> Value {
        self.0.last.lock().clone()
    }

    /// Register `tx` to receive updates to this `Val`. If
    /// `begin_with_last` is true, then an immediate update will be
    /// sent consisting of the last value received from the publisher.
    ///
    /// You may register multiple different channels to receive
    /// updates from a `Val`, and you may register one channel to
    /// receive updates from multiple `Val`s.
    pub fn updates(
        &self,
        begin_with_last: bool,
        tx: Sender<Pooled<Vec<(SubId, Value)>>>,
    ) {
        let m = ToCon::Stream {
            tx,
            sub_id: self.0.sub_id,
            last: begin_with_last,
            id: self.0.id,
        };
        let _ = self.0.connection.unbounded_send(m);
    }

    /// Write a value back to the publisher. This will start going out
    /// as soon as this method returns, and you can call `flush` on
    /// the subscriber to get pushback in case of a slow publisher.
    ///
    /// The publisher will receive multiple writes in the order you
    /// call `write`.
    pub fn write(&self, v: Value) {
        let _ = self.0.connection.unbounded_send(ToCon::Write(self.0.id, v));
    }

    /// Get the unique id of this subscription.
    pub fn id(&self) -> SubId {
        self.0.sub_id
    }
}

/// The durable subsiption state
#[derive(Debug)]
pub enum DvState {
    /// The Dval is currently subscribed
    Subscribed,
    /// The Dval is not currently subscribed
    Unsubscribed,
    /// The Dval is permanently dead and will not ever resubscribe
    FatalError(String),
}

#[derive(Debug)]
enum SubState {
    Unsubscribed,
    Subscribed(Val),
    #[allow(dead_code)] // I want the future option
    FatalError(Error),
}

#[derive(Debug)]
struct DvalInner {
    sub_id: SubId,
    sub: SubState,
    streams: Vec<Sender<Pooled<Vec<(SubId, Value)>>>>,
    states: Vec<UnboundedSender<(SubId, DvState)>>,
    tries: usize,
    next_try: Instant,
}

#[derive(Debug, Clone)]
pub struct DvalWeak(Weak<Mutex<DvalInner>>);

impl DvalWeak {
    pub fn new() -> Self {
        DvalWeak(Weak::new())
    }

    pub fn upgrade(&self) -> Option<Dval> {
        Weak::upgrade(&self.0).map(|s| Dval(s))
    }
}

/// `Dval` is a durable value subscription. it behaves just like
/// `Val`, except that if it dies a task within subscriber will
/// attempt to resubscribe. The resubscription process goes through
/// the entire resolution and connection process again, so `Dval` is
/// robust to many failures. For example,
///
/// - multiple publishers are publishing on a path and one of them dies.
///   `Dval` will transparently move to another one.
///
/// - a publisher is restarted (possibly on a different
///   machine). `Dval` will wait using linear backoff for the publisher
///   to come back, and then it will resubscribe.
///
/// - The resolver server cluster is restarted. In this case existing
///   subscriptions won't die, but new ones will fail while the
///   cluster is down. However once it is back up, and the publishers
///   have republished all their data, which they will do
///   automatically, `Dval` will resubscribe to anything it couldn't
///   find while the resolver server cluster was down.
///
/// A `Dval` uses a bit more memory than a `Val` subscription, but
/// other than that the performance is the same. It is therefore
/// recommended that you use `Dval` as the default kind of value
/// subscription.
///
/// If all user held references to `Dval` are dropped it will be
/// unsubscribed.
#[derive(Debug, Clone)]
pub struct Dval(Arc<Mutex<DvalInner>>);

impl Dval {
    pub fn downgrade(&self) -> DvalWeak {
        DvalWeak(Arc::downgrade(&self.0))
    }

    /// Get the last value published by the publisher, or None if the
    /// subscription is currently dead.
    pub fn last(&self) -> Option<Value> {
        let sub = match self.0.lock().sub {
            SubState::Unsubscribed | SubState::FatalError(_) => return None,
            SubState::Subscribed(ref sub) => sub.clone(),
        };
        Some(sub.last())
    }

    /// Register `tx` to receive messages about subscription state
    /// changes of this `Dval`. You can register multiple channels to
    /// receive state updates, and you can register one channel to
    /// receive updates about multiple `Dval`s.
    pub fn state_updates(
        &self,
        include_current: bool,
        tx: UnboundedSender<(SubId, DvState)>,
    ) {
        let mut t = self.0.lock();
        t.states.retain(|c| !c.is_closed());
        if include_current {
            let current = match t.sub {
                SubState::Unsubscribed => DvState::Unsubscribed,
                SubState::Subscribed(_) => DvState::Subscribed,
                SubState::FatalError(ref e) => DvState::FatalError(format!("{}", e)),
            };
            let _ = tx.unbounded_send((t.sub_id, current));
        }
        t.states.push(tx);
    }

    /// return the current subscription state
    pub fn state(&self) -> DvState {
        match self.0.lock().sub {
            SubState::Unsubscribed => DvState::Unsubscribed,
            SubState::Subscribed(_) => DvState::Subscribed,
            SubState::FatalError(ref e) => DvState::FatalError(format!("{}", e)),
        }
    }

    /// Register `tx` to receive updates to this `Dval`. If
    /// `begin_with_last` is true, then an immediate update will be
    /// sent consisting of the last value received from the publisher.
    ///
    /// You may register multiple different channels to receive
    /// updates from a `Dval`, and you may register one channel to
    /// receive updates from multiple `Dval`s.
    pub fn updates(
        &self,
        begin_with_last: bool,
        tx: mpsc::Sender<Pooled<Vec<(SubId, Value)>>>,
    ) {
        let mut t = self.0.lock();
        t.streams.retain(|c| !c.is_closed());
        if let SubState::FatalError(_) = t.sub {
            return;
        }
        t.streams.push(tx.clone());
        if let SubState::Subscribed(ref sub) = t.sub {
            let m = ToCon::Stream {
                tx,
                sub_id: t.sub_id,
                last: begin_with_last,
                id: sub.0.id,
            };
            let _ = sub.0.connection.unbounded_send(m);
        }
    }

    /// Write a value back to the publisher, see `Val::write`. If we
    /// aren't currently connected the write will be dropped and this
    /// method will return `false`
    pub fn write(&self, v: Value) -> bool {
        let t = self.0.lock();
        match t.sub {
            SubState::Unsubscribed | SubState::FatalError(_) => false,
            SubState::Subscribed(ref sub) => {
                sub.write(v);
                true
            }
        }
    }

    /// return the unique id of this `Dval`
    pub fn id(&self) -> SubId {
        self.0.lock().sub_id
    }
}

#[derive(Debug)]
enum SubStatus {
    Subscribed(ValWeak),
    Pending(Vec<oneshot::Sender<Result<Val>>>),
}

#[derive(Debug)]
struct SubscriberInner {
    resolver: ResolverRead,
    connections: HashMap<SocketAddr, UnboundedSender<ToCon>, FxBuildHasher>,
    subscribed: HashMap<Path, SubStatus>,
    durable_dead: HashMap<Path, DvalWeak>,
    durable_alive: HashMap<Path, DvalWeak>,
    trigger_resub: UnboundedSender<()>,
    desired_auth: Auth,
}

struct SubscriberWeak(Weak<Mutex<SubscriberInner>>);

impl SubscriberWeak {
    fn upgrade(&self) -> Option<Subscriber> {
        Weak::upgrade(&self.0).map(|s| Subscriber(s))
    }
}

/// create subscriptions
#[derive(Clone, Debug)]
pub struct Subscriber(Arc<Mutex<SubscriberInner>>);

impl Subscriber {
    /// create a new subscriber with the specified config and desired auth
    pub fn new(resolver: Config, desired_auth: Auth) -> Result<Subscriber> {
        let (tx, rx) = mpsc::unbounded();
        let resolver = ResolverRead::new(resolver, desired_auth.clone());
        let t = Subscriber(Arc::new(Mutex::new(SubscriberInner {
            resolver,
            desired_auth,
            connections: HashMap::with_hasher(FxBuildHasher::default()),
            subscribed: HashMap::new(),
            durable_dead: HashMap::new(),
            durable_alive: HashMap::new(),
            trigger_resub: tx,
        })));
        t.start_resub_task(rx);
        Ok(t)
    }

    pub fn resolver(&self) -> ResolverRead {
        self.0.lock().resolver.clone()
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
            *retry = subscriber
                .durable_dead
                .values()
                .filter_map(|w| w.upgrade())
                .map(|ds| ds.0.lock().next_try)
                .fold(None, |min, v| match min {
                    None => Some(v),
                    Some(min) => {
                        if v < min {
                            Some(v)
                        } else {
                            Some(min)
                        }
                    }
                })
                .map(|t| time::delay_until(t + Duration::from_secs(1)));
        }
        async fn do_resub(subscriber: &SubscriberWeak, retry: &mut Option<Delay>) {
            if let Some(subscriber) = subscriber.upgrade() {
                info!("doing resubscriptions");
                let now = Instant::now();
                let (mut batch, timeout) = {
                    let mut b = HashMap::new();
                    let mut gc = Vec::new();
                    let mut subscriber = subscriber.0.lock();
                    let mut max_tries = 0;
                    for (p, w) in &subscriber.durable_dead {
                        match w.upgrade() {
                            None => {
                                gc.push(p.clone());
                            }
                            Some(s) => {
                                let (next_try, tries) = {
                                    let s = s.0.lock();
                                    (s.next_try, s.tries)
                                };
                                if next_try <= now {
                                    b.insert(p.clone(), s);
                                    max_tries = max(max_tries, tries);
                                }
                            }
                        }
                    }
                    for p in gc {
                        subscriber.durable_dead.remove(&p);
                    }
                    (b, Duration::from_secs(10 + max_tries as u64))
                };
                if batch.len() == 0 {
                    let mut subscriber = subscriber.0.lock();
                    update_retry(&mut *subscriber, retry);
                } else {
                    let r =
                        subscriber.subscribe(batch.keys().cloned(), Some(timeout)).await;
                    let mut subscriber = subscriber.0.lock();
                    let now = Instant::now();
                    for (p, r) in r {
                        let mut ds = batch.get_mut(&p).unwrap().0.lock();
                        match r {
                            Err(e) => {
                                ds.tries += 1;
                                ds.next_try = now + Duration::from_secs(ds.tries as u64);
                                warn!(
                                    "resubscription error {}: {}, next try: {:?}",
                                    p, e, ds.next_try
                                );
                            }
                            Ok(sub) => {
                                info!("resubscription success {}", p);
                                ds.tries = 0;
                                let mut i = 0;
                                while i < ds.states.len() {
                                    match ds.states[i]
                                        .unbounded_send((ds.sub_id, DvState::Subscribed))
                                    {
                                        Ok(()) => {
                                            i += 1;
                                        }
                                        Err(_) => {
                                            ds.states.remove(i);
                                        }
                                    }
                                }
                                ds.streams.retain(|c| !c.is_closed());
                                for tx in ds.streams.iter().cloned() {
                                    let _ =
                                        sub.0.connection.unbounded_send(ToCon::Stream {
                                            tx,
                                            sub_id: ds.sub_id,
                                            last: true,
                                            id: sub.0.id,
                                        });
                                }
                                ds.sub = SubState::Subscribed(sub);
                                let w = subscriber.durable_dead.remove(&p).unwrap();
                                subscriber.durable_alive.insert(p.clone(), w.clone());
                            }
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

    /// Subscribe to the specified set of values.
    ///
    /// To minimize round trips and amortize locking path resolution
    /// and subscription are done in batches. Best performance will be
    /// achieved with larger batches.
    ///
    /// In case you are already subscribed to one or more of the paths
    /// in the batch, you will receive a reference to the existing
    /// subscription and no additional messages will be sent. However
    /// subscriber does not retain strong references to subscribed
    /// values, so if you drop all of them it will be unsubscribed.
    ///
    /// It is safe to call this function concurrently with the same or
    /// overlapping sets of paths in the batch, only one subscription
    /// attempt will be made, and the result of that one attempt will
    /// be given to each concurrent caller upon success or failure.
    ///
    /// The timeout, if specified, will apply to each subscription
    /// individually. Any subscription that does not complete
    /// successfully before the specified timeout will result in an
    /// error, but that error will not effect other subscriptions in
    /// the batch, which may complete successfully. If you need all or
    /// nothing behavior, specify None for timeout and wrap the
    /// `subscribe` future in a `time::timeout`.
    pub async fn subscribe(
        &self,
        batch: impl IntoIterator<Item = Path>,
        timeout: Option<Duration>,
    ) -> Vec<(Path, Result<Val>)> {
        enum St {
            Resolve,
            Subscribing(oneshot::Receiver<Result<Val>>),
            WaitingOther(oneshot::Receiver<Result<Val>>),
            Subscribed(Val),
            Error(Error),
        }
        let now = Instant::now();
        let paths = batch.into_iter().collect::<Vec<_>>();
        let mut pending: HashMap<Path, St> = HashMap::new();
        let r = {
            // Init
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
                            Some(r) => {
                                pending.insert(p, St::Subscribed(r));
                            }
                            None => {
                                e.insert(SubStatus::Pending(vec![]));
                                pending.insert(p, St::Resolve);
                            }
                        },
                    },
                }
            }
            t.resolver.clone()
        };
        fn pick(n: usize) -> usize {
            let mut rng = rand::thread_rng();
            rng.gen_range(0, n)
        }
        {
            // Resolve, Connect, Subscribe
            let to_resolve = pending
                .iter()
                .filter(|(_, s)| match s {
                    St::Resolve => true,
                    _ => false,
                })
                .map(|(p, _)| p.clone())
                .collect::<Vec<_>>();
            let r = match timeout {
                None => Ok(r.resolve(to_resolve.iter().cloned()).await),
                Some(d) => time::timeout(d, r.resolve(to_resolve.iter().cloned())).await,
            };
            match r {
                Err(_) => {
                    for p in to_resolve {
                        let e = anyhow!("resolving {} timed out", p);
                        pending.insert(p, St::Error(e));
                    }
                }
                Ok(Err(e)) => {
                    for p in to_resolve {
                        let s = St::Error(anyhow!("resolving {} failed {}", p, e));
                        pending.insert(p, s);
                    }
                }
                Ok(Ok(mut res)) => {
                    let mut t = self.0.lock();
                    let deadline = timeout.map(|t| now + t);
                    let desired_auth = t.desired_auth.clone();
                    for (p, resolved) in to_resolve.into_iter().zip(res.drain(..)) {
                        if resolved.addrs.len() == 0 {
                            pending.insert(p, St::Error(anyhow!("path not found")));
                        } else {
                            let addr = {
                                if resolved.addrs.len() == 1 {
                                    resolved.addrs[0].clone()
                                } else {
                                    resolved.addrs[pick(resolved.addrs.len())].clone()
                                }
                            };
                            let con = t.connections.entry(addr.0).or_insert_with(|| {
                                let (tx, rx) = mpsc::unbounded();
                                let target_spn = match resolved.krb5_spns.get(&addr.0) {
                                    None => Chars::new(),
                                    Some(p) => p.clone(),
                                };
                                let subscriber = self.downgrade();
                                let desired_auth = desired_auth.clone();
                                let addr = addr.0;
                                task::spawn(async move {
                                    let res = connection(
                                        subscriber,
                                        addr,
                                        target_spn,
                                        rx,
                                        desired_auth,
                                    )
                                    .await;
                                    info!("connection to {} shutdown {:?}", addr, res);
                                });
                                tx
                            });
                            let (tx, rx) = oneshot::channel();
                            let con_ = con.clone();
                            let r = con.unbounded_send(ToCon::Subscribe(
                                SubscribeValRequest {
                                    path: p.clone(),
                                    timestamp: resolved.timestamp,
                                    permissions: resolved.permissions,
                                    token: addr.1,
                                    resolver: resolved.resolver,
                                    finished: tx,
                                    con: con_,
                                    deadline,
                                },
                            ));
                            match r {
                                Ok(()) => {
                                    pending.insert(p, St::Subscribing(rx));
                                }
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
                St::Subscribed(_) => (),
                St::Error(e) => {
                    let mut t = self.0.lock();
                    if let Some(sub) = t.subscribed.remove(path.as_ref()) {
                        match sub {
                            SubStatus::Subscribed(_) => unreachable!(),
                            SubStatus::Pending(waiters) => {
                                for w in waiters {
                                    let err = Err(anyhow!("{}", e));
                                    let _ = w.send(err);
                                }
                            }
                        }
                    }
                }
                St::WaitingOther(w) => match w.await {
                    Err(_) => *st = St::Error(anyhow!("other side died")),
                    Ok(Err(e)) => *st = St::Error(e),
                    Ok(Ok(raw)) => *st = St::Subscribed(raw),
                },
                St::Subscribing(w) => {
                    let res = match w.await {
                        Err(_) => Err(anyhow!("connection died")),
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
                                        let err = Err(anyhow!("{}", err));
                                        let _ = w.send(err);
                                    }
                                    *st = St::Error(err);
                                }
                            },
                            Ok(raw) => {
                                let s = mem::replace(
                                    e.get_mut(),
                                    SubStatus::Subscribed(raw.downgrade()),
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
                        },
                    }
                }
            }
        }
        paths
            .into_iter()
            .map(|p| match pending.remove(&p).unwrap() {
                St::Resolve | St::Subscribing(_) | St::WaitingOther(_) => unreachable!(),
                St::Subscribed(raw) => (p, Ok(raw)),
                St::Error(e) => (p, Err(e)),
            })
            .collect()
    }

    /// Subscribe to just one value. This is sufficient for a small
    /// number of paths, but if you need to subscribe to a lot of
    /// values it is more efficent to use `subscribe`. The semantics
    /// of this method are the same as `subscribe` called with 1 path.
    pub async fn subscribe_one(
        &self,
        path: Path,
        timeout: Option<Duration>,
    ) -> Result<Val> {
        self.subscribe(iter::once(path), timeout).await.pop().unwrap().1
    }

    /// Create a durable value subscription to `path`.
    ///
    /// Batching of durable subscriptions is automatic, if you create
    /// a lot of durable subscriptions all at once they will batch.
    ///
    /// The semantics of `durable_subscribe` are the same as
    /// subscribe, except that certain errors are caught, and
    /// resubscriptions are attempted. see `Dval`.
    pub fn durable_subscribe(&self, path: Path) -> Dval {
        let mut t = self.0.lock();
        if let Some(s) = t.durable_dead.get(&path).or_else(|| t.durable_alive.get(&path))
        {
            if let Some(s) = s.upgrade() {
                return s;
            }
        }
        let s = Dval(Arc::new(Mutex::new(DvalInner {
            sub_id: SubId::new(),
            sub: SubState::Unsubscribed,
            streams: Vec::new(),
            states: Vec::new(),
            tries: 0,
            next_try: Instant::now(),
        })));
        t.durable_dead.insert(path, s.downgrade());
        let _ = t.trigger_resub.unbounded_send(());
        s
    }

    /// This will return when all pending operations are flushed out
    /// to the publishers. This is primarially used to provide
    /// pushback in the case you want to do a lot of writes, and you
    /// need pushback in case a publisher is slow to process them,
    /// however it applies to durable_subscribe and unsubscribe as well.
    pub async fn flush(&self) {
        let flushes = {
            let t = self.0.lock();
            t.connections
                .values()
                .map(|c| {
                    let (tx, rx) = oneshot::channel();
                    let _ = c.unbounded_send(ToCon::Flush(tx));
                    rx
                })
                .collect::<Vec<_>>()
        };
        for flush in flushes {
            let _ = flush.await;
        }
    }
}

struct Sub {
    path: Path,
    streams: Vec<(SubId, ChanId, Sender<Pooled<Vec<(SubId, Value)>>>)>,
    last: Arc<Mutex<Value>>,
}

fn unsubscribe(subscriber: &mut SubscriberInner, sub: Sub, id: Id, addr: SocketAddr) {
    if let Some(dsw) = subscriber.durable_alive.remove(&sub.path) {
        if let Some(ds) = dsw.upgrade() {
            let mut inner = ds.0.lock();
            inner.sub = SubState::Unsubscribed;
            let mut i = 0;
            while i < inner.states.len() {
                match inner.states[i]
                    .unbounded_send((inner.sub_id, DvState::Unsubscribed))
                {
                    Ok(()) => {
                        i += 1;
                    }
                    Err(_) => {
                        inner.states.remove(i);
                    }
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
                None => {
                    e.remove();
                }
                Some(s) => {
                    if s.0.id == id && s.0.addr == addr {
                        e.remove();
                    }
                }
            },
        },
    }
}

async fn hello_publisher(
    con: &mut Channel<ClientCtx>,
    auth: &Auth,
    target_spn: &Chars,
) -> Result<()> {
    use crate::protocol::publisher::v1::Hello;
    // negotiate protocol version
    con.send_one(&1u64).await?;
    let _ver: u64 = con.receive().await?;
    match auth {
        Auth::Anonymous => {
            con.send_one(&Hello::Anonymous).await?;
            let reply: Hello = con.receive().await?;
            match reply {
                Hello::Anonymous => (),
                _ => bail!("unexpected response from publisher"),
            }
        }
        Auth::Krb5 { upn, .. } => {
            let p = upn.as_ref().map(|p| p.as_str());
            let ctx = os::create_client_ctx(p, target_spn)?;
            let tok = ctx
                .step(None)?
                .map(|b| utils::bytes(&*b))
                .ok_or_else(|| anyhow!("expected step to generate a token"))?;
            con.send_one(&Hello::Token(tok)).await?;
            match con.receive().await? {
                Hello::Anonymous => bail!("publisher failed mutual authentication"),
                Hello::ResolverAuthenticate(_, _) => bail!("protocol error"),
                Hello::Token(tok) => {
                    if ctx.step(Some(&*tok))?.is_some() {
                        bail!("unexpected second token from step");
                    }
                }
            }
            con.set_ctx(ctx.clone()).await;
        }
    }
    Ok(())
}

const PERIOD: Duration = Duration::from_secs(10);
const FLUSH: Duration = Duration::from_secs(1);

lazy_static! {
    static ref BATCHES: Pool<Vec<(SubId, Value)>> = Pool::new(1000);
    static ref DECODE_BATCHES: Pool<Vec<From>> = Pool::new(1000);
}

// This is the fast path for the common case where the batch contains
// only updates. As of 2020-04-30, sending to an mpsc channel is
// pretty slow, about 250ns, so we go to great lengths to avoid it.
async fn process_updates_batch(
    by_chan: &mut HashMap<
        ChanId,
        (Sender<Pooled<Vec<(SubId, Value)>>>, Pooled<Vec<(SubId, Value)>>),
        FxBuildHasher,
    >,
    mut batch: Pooled<Vec<From>>,
    subscriptions: &mut HashMap<Id, Sub, FxBuildHasher>,
) {
    for m in batch.drain(..) {
        if let From::Update(i, m) = m {
            if let Some(sub) = subscriptions.get_mut(&i) {
                for (sub_id, chan_id, c) in sub.streams.iter() {
                    by_chan
                        .entry(*chan_id)
                        .or_insert_with(|| (c.clone(), BATCHES.take()))
                        .1
                        .push((*sub_id, m.clone()))
                }
                *sub.last.lock() = m;
            }
        }
    }
    for (_, (mut c, batch)) in by_chan.drain() {
        let _ = c.send(batch).await;
    }
}

async fn process_batch(
    mut batch: Pooled<Vec<From>>,
    subscriptions: &mut HashMap<Id, Sub, FxBuildHasher>,
    pending: &mut HashMap<Path, SubscribeValRequest>,
    con: &mut WriteChannel<ClientCtx>,
    subscriber: &Subscriber,
    addr: SocketAddr,
) -> Result<()> {
    for m in batch.drain(..) {
        match m {
            From::Update(i, m) => match subscriptions.get_mut(&i) {
                Some(sub) => {
                    for (id, _, c) in sub.streams.iter_mut() {
                        let mut b = BATCHES.take();
                        b.push((*id, m.clone()));
                        let _ = c.send(b).await;
                    }
                    *sub.last.lock() = m;
                }
                None => con.queue_send(&To::Unsubscribe(i))?,
            },
            From::Heartbeat => (),
            From::NoSuchValue(path) => {
                if let Some(r) = pending.remove(&path) {
                    let _ = r.finished.send(Err(Error::from(NoSuchValue)));
                }
            }
            From::Denied(path) => {
                if let Some(r) = pending.remove(&path) {
                    let _ = r.finished.send(Err(Error::from(PermissionDenied)));
                }
            }
            From::Unsubscribed(id) => {
                if let Some(s) = subscriptions.remove(&id) {
                    let mut t = subscriber.0.lock();
                    unsubscribe(&mut *t, s, id, addr);
                }
            }
            From::Subscribed(p, id, m) => match pending.remove(&p) {
                None => con.queue_send(&To::Unsubscribe(id))?,
                Some(req) => {
                    let sub_id = SubId::new();
                    let last = Arc::new(Mutex::new(m));
                    let s = Ok(Val(Arc::new(ValInner {
                        sub_id,
                        id,
                        addr,
                        connection: req.con,
                        last: last.clone(),
                    })));
                    match req.finished.send(s) {
                        Err(_) => con.queue_send(&To::Unsubscribe(id))?,
                        Ok(()) => {
                            subscriptions.insert(
                                id,
                                Sub { path: req.path, last, streams: Vec::new() },
                            );
                        }
                    }
                }
            },
        }
    }
    Ok(())
}

async fn try_flush(con: &mut WriteChannel<ClientCtx>) -> Result<()> {
    if con.bytes_queued() > 0 {
        match con.flush_timeout(FLUSH).await {
            Ok(()) => Ok(()),
            Err(SendTimeoutError::Timeout(())) => Ok(()),
            Err(SendTimeoutError::Closed(())) => bail!("connection died"),
        }
    } else {
        Ok(())
    }
}

fn decode_task(
    mut con: ReadChannel<ClientCtx>,
    stop: oneshot::Receiver<()>,
) -> Receiver<Result<(Pooled<Vec<From>>, bool)>> {
    let (mut send, recv) = mpsc::channel(3);
    let mut stop = stop.fuse();
    task::spawn(async move {
        let mut buf = DECODE_BATCHES.take();
        let r: Result<(), anyhow::Error> = loop {
            select_biased! {
                _ = stop => { break Ok(()); },
                r = con.receive_batch(&mut buf).fuse() => match r {
                    Err(e) => {
                        buf.clear();
                        try_cf!(send.send(Err(e)).await)
                    }
                    Ok(()) => {
                        let batch = mem::replace(&mut buf, DECODE_BATCHES.take());
                        let only_updates = batch.iter().all(|v| match v {
                            From::Update(_, _) => true,
                            _ => false
                        });
                        try_cf!(send.send(Ok((batch, only_updates))).await)
                    }
                }
            }
        };
        info!("decode task shutting down {:?}", r);
    });
    recv
}

async fn connection(
    subscriber: SubscriberWeak,
    addr: SocketAddr,
    target_spn: Chars,
    from_sub: UnboundedReceiver<ToCon>,
    auth: Auth,
) -> Result<()> {
    let mut pending: HashMap<Path, SubscribeValRequest> = HashMap::new();
    let mut subscriptions: HashMap<Id, Sub, FxBuildHasher> =
        HashMap::with_hasher(FxBuildHasher::default());
    let mut idle: usize = 0;
    let mut msg_recvd = false;
    let mut from_sub = Batched::new(from_sub, BATCH);
    let soc = time::timeout(PERIOD, TcpStream::connect(addr)).await??;
    soc.set_nodelay(true)?;
    let mut con = Channel::new(soc);
    hello_publisher(&mut con, &auth, &target_spn).await?;
    let (read_con, mut write_con) = con.split();
    let (tx_stop, rx_stop) = oneshot::channel();
    let mut batches = decode_task(read_con, rx_stop);
    let mut periodic = time::interval_at(Instant::now() + PERIOD, PERIOD).fuse();
    let mut by_receiver: HashMap<ChanWrap<Pooled<Vec<(SubId, Value)>>>, ChanId> =
        HashMap::new();
    let mut by_chan: HashMap<
        ChanId,
        (Sender<Pooled<Vec<(SubId, Value)>>>, Pooled<Vec<(SubId, Value)>>),
        FxBuildHasher,
    > = HashMap::with_hasher(FxBuildHasher::default());
    let res = 'main: loop {
        select_biased! {
            now = periodic.next() => if let Some(now) = now {
                if !msg_recvd {
                    break 'main Err(anyhow!("hung publisher"));
                } else {
                    msg_recvd = false;
                }
                if subscriptions.len() == 0 && pending.len() == 0 {
                    idle += 1;
                    if idle == 2 { break 'main Ok(()); }
                } else {
                    idle = 0;
                }
                let mut timed_out = Vec::new();
                for (path, req) in pending.iter() {
                    if let Some(deadline) = req.deadline {
                        if deadline < now {
                            timed_out.push(path.clone());
                        }
                    }
                }
                for path in timed_out {
                    if let Some(req) = pending.remove(&path) {
                        let _ = req.finished.send(Err(anyhow!("timed out")));
                    }
                }
                try_cf!(try_flush(&mut write_con).await)
            },
            msg = from_sub.next() => match msg {
                None => break Err(anyhow!("dropped")),
                Some(BatchItem::EndBatch) => {
                    try_cf!(try_flush(&mut write_con).await)
                }
                Some(BatchItem::InBatch(ToCon::Subscribe(req))) => {
                    let path = req.path.clone();
                    let resolver = req.resolver;
                    let token = req.token.clone();
                    let permissions = req.permissions;
                    let timestamp = req.timestamp;
                    pending.insert(path.clone(), req);
                    try_cf!(write_con.queue_send(&To::Subscribe {
                        path,
                        resolver,
                        timestamp,
                        permissions,
                        token,
                    }))
                }
                Some(BatchItem::InBatch(ToCon::Unsubscribe(id))) => {
                    try_cf!(write_con.queue_send(&To::Unsubscribe(id)))
                }
                Some(BatchItem::InBatch(ToCon::Stream { id, sub_id, mut tx, last })) => {
                    if let Some(sub) = subscriptions.get_mut(&id) {
                        sub.streams.retain(|(_, _, c)| {
                            if c.is_closed() {
                                by_receiver.remove(&ChanWrap(c.clone()));
                                false
                            } else {
                                true
                            }
                        });
                        if last {
                            let m = sub.last.lock().clone();
                            let mut b = BATCHES.take();
                            b.push((sub_id, m));
                            match tx.send(b).await {
                                Err(_) => continue,
                                Ok(()) => ()
                            }
                        }
                        let id = by_receiver.entry(ChanWrap(tx.clone()))
                            .or_insert_with(ChanId::new);
                        sub.streams.push((sub_id, *id, tx));
                    }
                }
                Some(BatchItem::InBatch(ToCon::Write(id, v))) => {
                    try_cf!(write_con.queue_send(&To::Write(id, v)))
                }
                Some(BatchItem::InBatch(ToCon::Flush(tx))) => {
                    let _ = tx.send(());
                }
            },
            r = batches.next() => match r {
                Some(Ok((mut batch, true))) => {
                    msg_recvd = true;
                    process_updates_batch(
                        &mut by_chan,
                        batch,
                        &mut subscriptions
                    ).await;
                    try_cf!(try_flush(&mut write_con).await)
                },
                Some(Ok((mut batch, false))) =>
                    if let Some(subscriber) = subscriber.upgrade() {
                        msg_recvd = true;
                        try_cf!(process_batch(
                            batch,
                            &mut subscriptions,
                            &mut pending,
                            &mut write_con,
                            &subscriber,
                            addr).await);
                        try_cf!(try_flush(&mut write_con).await)
                    }
                Some(Err(e)) => break Err(Error::from(e)),
                None => break Err(anyhow!("EOF")),
            },
        }
    };
    if let Some(subscriber) = subscriber.upgrade() {
        let mut t = subscriber.0.lock();
        t.connections.remove(&addr);
        for (id, sub) in subscriptions {
            unsubscribe(&mut *t, sub, id, addr);
        }
        for (_, req) in pending {
            let _ = req.finished.send(Err(anyhow!("connection died")));
        }
    }
    let _ = tx_stop.send(());
    res
}
