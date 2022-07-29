pub use crate::protocol::value::{FromValue, Typ, Value};
pub use crate::resolver_client::DesiredAuth;
use crate::{
    batch_channel::{self, BatchReceiver, BatchSender},
    channel::{Channel, K5CtxWrap, ReadChannel, WriteChannel},
    config::Config,
    pack::{Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
    protocol::{
        self,
        publisher::{From, Id, To},
        resolver::{Publisher, PublisherId, Resolved, TargetAuth},
    },
    publisher::PublishFlags,
    resolver_client::{common::krb5_authentication, ResolverRead},
    utils::{BatchItem, Batched, ChanId, ChanWrap},
};
use anyhow::{anyhow, Error, Result};
use bytes::{Buf, BufMut, Bytes};
use cross_krb5::ClientCtx;
use futures::{
    channel::{
        mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    prelude::*,
    select_biased,
    stream::FuturesUnordered,
};
use fxhash::FxHashMap;
use log::{info, warn};
use parking_lot::Mutex;
use rand::Rng;
use std::{
    cmp::{max, Eq, PartialEq},
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    error, fmt,
    hash::Hash,
    iter, mem,
    net::SocketAddr,
    result,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    task,
    time::{self, Instant},
};
use triomphe::Arc as TArc;

type StreamsInner<T> = Arc<Vec<(T, ChanWrap<Pooled<Vec<(SubId, Event)>>>)>>;

lazy_static! {
    static ref HCSTREAMS: Mutex<HashSet<StreamsInner<ChanId>>> =
        Mutex::new(HashSet::new());
    static ref HCDVSTREAMS: Mutex<HashSet<StreamsInner<UpdatesFlags>>> =
        Mutex::new(HashSet::new());
    static ref BATCHES: Pool<Vec<(SubId, Event)>> = Pool::new(64, 16384);
    static ref DECODE_BATCHES: Pool<Vec<From>> = Pool::new(64, 16384);
}

macro_rules! hcstreams {
    ($name:ident, $static:ident, $typ:ty) => {
        #[derive(Debug)]
        struct $name(StreamsInner<$typ>);

        impl $name {
            fn new() -> $name {
                let mut inner = $static.lock();
                match inner.get(&vec![]) {
                    Some(empty) => $name(empty.clone()),
                    None => {
                        let r = Arc::new(vec![]);
                        inner.insert(r.clone());
                        $name(r)
                    }
                }
            }

            fn add(
                &self,
                chanid: $typ,
                chan: ChanWrap<Pooled<Vec<(SubId, Event)>>>,
            ) -> $name {
                let mut dead = false;
                let mut vec = Vec::clone(&self.0);
                vec.retain(|(_, c)| {
                    if c.0.is_closed() {
                        dead = true;
                        false
                    } else {
                        true
                    }
                });
                vec.push((chanid, chan));
                vec.sort_by_key(|(id, _)| *id);
                let mut inner = $static.lock();
                if dead {
                    inner.remove(&self.0);
                }
                match inner.get(&vec) {
                    Some(cur) => $name(Arc::clone(cur)),
                    None => {
                        let t = Arc::new(vec);
                        inner.insert(t.clone());
                        $name(t)
                    }
                }
            }
        }
    };
}

hcstreams!(Streams, HCSTREAMS, ChanId);
hcstreams!(DvStreams, HCDVSTREAMS, UpdatesFlags);

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

atomic_id!(SubId);
atomic_id!(SubscriberId);
atomic_id!(ConId);

bitflags! {
    pub struct SubscribeFlags: u32 {
        /// if set, then if an existing connection exists to any
        /// publisher that publishes the value we are subscribing,
        /// then subscriber will use that connection instead of
        /// picking a random publisher and potentially creating a new
        /// connection.
        ///
        /// Because the creation of connections is atomic, this flag
        /// guarantees that all subscriptions that can will use the
        /// same connection.
        ///
        /// This can be important for control interfaces, and is used
        /// by the RPC protocol to ensure that all function parameters
        /// are written to the same publisher, even if a procedure is
        /// published by multiple publishers.
        const USE_EXISTING = 0x01;
    }
}

bitflags! {
    pub struct UpdatesFlags: u32 {
        /// if set, then an immediate update will be sent consisting
        /// of the last value received from the publisher. If you
        /// reregister the same channel the last will be sent to that
        /// channel again, even though other events will not be sent
        /// twice. If you don't want this behavior you must also set
        /// NO_SPURIOUS.
        const BEGIN_WITH_LAST      = 0x01;

        /// If set then the subscriber will stop storing the last
        /// value. The `last` method will return whatever last was
        /// before this method was called, and the passed in channel
        /// will be the only way of getting data from the
        /// subscription. This improves performance at the expense of
        /// flexibility.
        const STOP_COLLECTING_LAST = 0x02;

        /// If BEGIN_WITH_LAST is set, and you reregister the same
        /// channel, do not send the last again to that
        /// channel.
        const NO_SPURIOUS          = 0x04;
    }
}

#[derive(Debug)]
struct SubscribeValRequest {
    path: Path,
    sub_id: SubId,
    timestamp: u64,
    permissions: u32,
    token: Bytes,
    resolver: SocketAddr,
    finished: oneshot::Sender<Result<Val>>,
    con: BatchSender<ToCon>,
    deadline: Option<Instant>,
}

#[derive(Debug)]
enum ToCon {
    Subscribe(SubscribeValRequest),
    Unsubscribe(Id),
    Stream {
        id: Id,
        sub_id: SubId,
        tx: Sender<Pooled<Vec<(SubId, Event)>>>,
        flags: UpdatesFlags,
    },
    Write(Id, Value, Option<oneshot::Sender<Value>>),
    Flush(oneshot::Sender<()>),
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Event {
    Unsubscribed,
    Update(Value),
}

impl Pack for Event {
    fn encoded_len(&self) -> usize {
        match self {
            Event::Unsubscribed => 1,
            Event::Update(v) => Pack::encoded_len(v),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> result::Result<(), PackError> {
        match self {
            Event::Unsubscribed => Ok(buf.put_u8(0x40)),
            Event::Update(v) => Pack::encode(v, buf),
        }
    }

    fn decode(buf: &mut impl Buf) -> result::Result<Self, PackError> {
        if buf.chunk()[0] == 0x40 {
            buf.advance(1);
            Ok(Event::Unsubscribed)
        } else {
            Ok(Event::Update(Pack::decode(buf)?))
        }
    }
}

#[derive(Debug)]
struct ValInner {
    sub_id: SubId,
    id: Id,
    conid: ConId,
    connection: BatchSender<ToCon>,
    last: TArc<Mutex<Event>>,
}

impl Drop for ValInner {
    fn drop(&mut self) {
        self.connection.send(ToCon::Unsubscribe(self.id));
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

    /// Get the last event value.
    pub fn last(&self) -> Event {
        self.0.last.lock().clone()
    }

    /// Register `tx` to receive updates to this `Val`.
    ///
    /// You may register multiple different channels to receive
    /// updates from a `Val`, and you may register one channel to
    /// receive updates from multiple `Val`s.
    ///
    /// If you register multiple channels pointing to the same
    /// receiver you will not get duplicate updates. However, if you
    /// register a duplicate channel and begin_with_last is true you
    /// will get an update with the current state, even though the
    /// channel registration will be ignored.
    pub fn updates(&self, flags: UpdatesFlags, tx: Sender<Pooled<Vec<(SubId, Event)>>>) {
        let m = ToCon::Stream { tx, sub_id: self.0.sub_id, id: self.0.id, flags };
        self.0.connection.send(m);
    }

    /// Write a value back to the publisher. This will start going out
    /// as soon as this method returns, and you can call `flush` on
    /// the subscriber to get pushback in case of a slow publisher.
    ///
    /// The publisher will receive multiple writes in the order you
    /// call `write`.
    ///
    /// The publisher will not reply to your write, except that it may
    /// update values you are subscribed to, or trigger some other
    /// observable action.
    pub fn write(&self, v: Value) {
        self.0.connection.send(ToCon::Write(self.0.id, v, None));
    }

    /// This does the same thing as `write` except that it requires
    /// the publisher send a reply indicating the outcome of the
    /// request. The reply can be read from the returned oneshot
    /// channel.
    ///
    /// Note that compared to `write` this function has higher
    /// overhead, avoid it in situations where high message volumes
    /// are required.
    pub fn write_with_recipt(&self, v: Value) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        self.0.connection.send(ToCon::Write(self.0.id, v, Some(tx)));
        rx
    }

    /// Get the unique id of this subscription.
    pub fn id(&self) -> SubId {
        self.0.sub_id
    }
}

#[derive(Debug)]
struct DvDead {
    queued_writes: Vec<(Value, Option<oneshot::Sender<Value>>)>,
    tries: usize,
    next_try: Instant,
}

#[derive(Debug)]
enum DvState {
    Subscribed(Val),
    Dead(Box<DvDead>), // the box ensures that DvState is tag + 1 word
}

#[derive(Debug)]
struct DvalInner {
    sub_id: SubId,
    sub: DvState,
    streams: DvStreams,
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
/// If `stop_collecting_last` is true then the subscriber will
/// stop storing the last value in addition to giving it to this
/// channel. The `last` method will return null from now on, and
/// the passed in channel will be the only way of getting data
/// from the subscription. This improves performance at the
/// expense of flexibility.

///
/// If all user held references to `Dval` are dropped it will be
/// unsubscribed.
#[derive(Debug, Clone)]
pub struct Dval(Arc<Mutex<DvalInner>>);

impl Dval {
    pub fn downgrade(&self) -> DvalWeak {
        DvalWeak(Arc::downgrade(&self.0))
    }

    /// Get the last value published by the publisher, or Unsubscribed
    /// if the subscription is currently dead.
    pub fn last(&self) -> Event {
        match &self.0.lock().sub {
            DvState::Dead(_) => Event::Unsubscribed,
            DvState::Subscribed(val) => val.last(),
        }
    }

    /// Register `tx` to receive updates to this `Dval`.
    ///
    /// You may register multiple different channels to receive
    /// updates from a `Dval`, and you may register one channel to
    /// receive updates from multiple `Dval`s.
    pub fn updates(
        &self,
        flags: UpdatesFlags,
        tx: mpsc::Sender<Pooled<Vec<(SubId, Event)>>>,
    ) {
        let mut t = self.0.lock();
        let c = ChanWrap(tx.clone());
        if !t.streams.0.iter().any(|(_, s)| &c == s) {
            t.streams = t.streams.add(flags, c);
        }
        if let DvState::Subscribed(ref sub) = t.sub {
            let m = ToCon::Stream { tx, sub_id: t.sub_id, id: sub.0.id, flags };
            sub.0.connection.send(m);
        }
    }

    /// Wait until the `Dval` is subscribed and then return. This is
    /// not a guarantee that the `Dval` will stay subscribed for any
    /// length of time, just that at the moment this method returns
    /// the `Dval` is subscribed. If the `Dval` is subscribed when
    /// this method is called, it will return immediatly without
    /// allocating any resources.
    pub async fn wait_subscribed(&self) -> Result<()> {
        match &self.0.lock().sub {
            DvState::Subscribed(_) => return Ok(()),
            DvState::Dead(_) => (),
        }
        let (tx, mut rx) = mpsc::channel(2);
        self.updates(UpdatesFlags::BEGIN_WITH_LAST, tx);
        loop {
            match rx.next().await {
                None => bail!("unexpected resub error"),
                Some(mut batch) => {
                    let mut subed = false;
                    for (_, ev) in batch.drain(..) {
                        match ev {
                            Event::Unsubscribed => {
                                subed = false;
                            }
                            Event::Update(_) => {
                                subed = true;
                            }
                        }
                    }
                    if subed {
                        break Ok(());
                    }
                }
            }
        }
    }

    /// Write a value back to the publisher, see `Val::write`. If we
    /// aren't currently subscribed the write will be queued and sent
    /// when we are. The return value will be `true` if the write was
    /// sent immediatly, and false if it was queued. It is still
    /// possible that a write will be dropped e.g. if the connection
    /// dies while we are writing it.
    pub fn write(&self, v: Value) -> bool {
        let mut t = self.0.lock();
        match &mut t.sub {
            DvState::Subscribed(ref val) => {
                val.write(v);
                true
            }
            DvState::Dead(dead) => {
                dead.queued_writes.push((v, None));
                false
            }
        }
    }

    /// This does the same thing as `write` except that it requires
    /// the publisher send a reply indicating the outcome of the
    /// request. The reply can be read from the returned oneshot
    /// channel.
    ///
    /// Note that compared to `write` this function has higher
    /// overhead, avoid it in situations where high message volumes
    /// are required.
    ///
    /// If we are not currently subscribed then the write will be
    /// queued until we are. It is still possible that a write will be
    /// dropped e.g. if the connection dies while we are writing it.
    pub fn write_with_recipt(&self, v: Value) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        let mut t = self.0.lock();
        match &mut t.sub {
            DvState::Subscribed(ref sub) => {
                sub.0.connection.send(ToCon::Write(sub.0.id, v, Some(tx)));
            }
            DvState::Dead(dead) => {
                dead.queued_writes.push((v, Some(tx)));
            }
        }
        rx
    }

    /// Clear the write queue
    pub fn clear_queued_writes(&self) {
        let mut t = self.0.lock();
        if let DvState::Dead(dead) = &mut t.sub {
            dead.queued_writes.clear();
        }
    }

    /// Return the number of queued writes
    pub fn queued_writes(&self) -> usize {
        match &mut self.0.lock().sub {
            DvState::Subscribed(_) => 0,
            DvState::Dead(dead) => dead.queued_writes.len(),
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
    Pending(Box<Vec<oneshot::Sender<Result<Val>>>>), // the box ensures SubStatus is tag + 1 word
}

const REMEBER_FAILED: Duration = Duration::from_secs(60);

fn pick(n: usize) -> usize {
    let mut rng = rand::thread_rng();
    rng.gen_range(0..n)
}

#[derive(Debug)]
struct SubscriberInner {
    id: SubscriberId,
    resolver: ResolverRead,
    connections: FxHashMap<SocketAddr, BatchSender<ToCon>>,
    recently_failed: FxHashMap<SocketAddr, Instant>,
    subscribed: HashMap<Path, SubStatus>,
    durable_dead: HashMap<Path, DvalWeak>,
    durable_pending: HashMap<Path, DvalWeak>,
    durable_alive: HashMap<Path, DvalWeak>,
    trigger_resub: UnboundedSender<()>,
    desired_auth: DesiredAuth,
}

impl SubscriberInner {
    fn durable_id(&self, path: &Path) -> Option<SubId> {
        self.durable_dead
            .get(path)
            .or_else(|| self.durable_pending.get(path))
            .or_else(|| self.durable_alive.get(path))
            .and_then(|w| w.upgrade())
            .map(|d| d.id())
    }

    fn choose_addr(
        &mut self,
        publishers: &Pooled<FxHashMap<PublisherId, Publisher>>,
        resolved: &Resolved,
    ) -> Option<(SocketAddr, TargetAuth, Bytes)> {
        use rand::seq::IteratorRandom;
        let flags = PublishFlags::from_bits(resolved.flags)?;
        if flags.contains(PublishFlags::USE_EXISTING) {
            for pref in &*resolved.publishers {
                if let Some(pb) = publishers.get(&pref.id) {
                    if self.connections.contains_key(&pb.addr) {
                        return Some((
                            pb.addr,
                            pb.target_auth.clone(),
                            pref.token.clone(),
                        ));
                    }
                }
            }
        }
        let res = resolved
            .publishers
            .iter()
            .filter_map(|pref| {
                publishers
                    .get(&pref.id)
                    .filter(|pb| !self.recently_failed.contains_key(&pb.addr))
                    .map(|pb| (pref, pb))
            })
            .choose(&mut rand::thread_rng())
            .map(|(pref, pb)| (pb.addr, pb.target_auth.clone(), pref.token.clone()));
        if res.is_some() {
            res
        } else {
            resolved
                .publishers
                .iter()
                .filter_map(|pref| publishers.get(&pref.id).map(|pb| (pref, pb)))
                .choose(&mut rand::thread_rng())
                .map(|(pref, pb)| (pb.addr, pb.target_auth.clone(), pref.token.clone()))
        }
    }

    fn gc_recently_failed(&mut self) {
        let now = Instant::now();
        self.recently_failed.retain(|_, v| (now - *v) < REMEBER_FAILED)
    }
}

#[derive(Debug, Clone)]
struct SubscriberWeak(Weak<Mutex<SubscriberInner>>);

impl SubscriberWeak {
    fn upgrade(&self) -> Option<Subscriber> {
        Weak::upgrade(&self.0).map(|s| Subscriber(s))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DurableStats {
    pub alive: usize,
    pub pending: usize,
    pub dead: usize,
}

/// create subscriptions
#[derive(Clone, Debug)]
pub struct Subscriber(Arc<Mutex<SubscriberInner>>);

impl Subscriber {
    /// create a new subscriber with the specified config and desired auth
    pub fn new(resolver: Config, desired_auth: DesiredAuth) -> Result<Subscriber> {
        let (tx, rx) = mpsc::unbounded();
        let resolver = ResolverRead::new(resolver, desired_auth.clone());
        let t = Subscriber(Arc::new(Mutex::new(SubscriberInner {
            id: SubscriberId::new(),
            resolver,
            desired_auth,
            connections: HashMap::default(),
            recently_failed: HashMap::default(),
            subscribed: HashMap::default(),
            durable_dead: HashMap::default(),
            durable_pending: HashMap::default(),
            durable_alive: HashMap::default(),
            trigger_resub: tx,
        })));
        t.start_resub_task(rx);
        Ok(t)
    }

    /// Return a unique identifier for this subscriber instance. The
    /// identifier will be unique across all subscribers created in
    /// this process, but not across processes or machines.
    pub fn id(&self) -> SubscriberId {
        self.0.lock().id
    }

    /// return stats about durable subscriptions
    pub fn durable_stats(&self) -> DurableStats {
        let t = self.0.lock();
        DurableStats {
            alive: t.durable_alive.len(),
            pending: t.durable_pending.len(),
            dead: t.durable_dead.len(),
        }
    }

    pub fn resolver(&self) -> ResolverRead {
        self.0.lock().resolver.clone()
    }

    fn downgrade(&self) -> SubscriberWeak {
        SubscriberWeak(Arc::downgrade(&self.0))
    }

    fn start_resub_task(&self, incoming: UnboundedReceiver<()>) {
        async fn wait_retry(retry: Option<Instant>) {
            match retry {
                None => future::pending().await,
                Some(d) => time::sleep_until(d).await,
            }
        }
        fn update_retry(subscriber: &mut SubscriberInner, retry: &mut Option<Instant>) {
            let now = Instant::now();
            task::block_in_place(|| {
                *retry = None;
                for w in subscriber.durable_dead.values() {
                    if let Some(dv) = w.upgrade() {
                        let next_try = match &dv.0.lock().sub {
                            DvState::Dead(dead) => dead.next_try,
                            DvState::Subscribed(_) => unreachable!(),
                        };
                        match retry {
                            None => {
                                *retry = Some(next_try);
                            }
                            Some(retry) => {
                                if next_try < *retry {
                                    *retry = next_try;
                                }
                            }
                        }
                        if next_try <= now {
                            break;
                        }
                    }
                }
            })
        }
        async fn do_resub(
            subscriber: &SubscriberWeak,
            retry: &mut Option<Instant>,
        ) -> Option<FuturesUnordered<impl Future<Output = (Path, Result<Val>)>>> {
            let subscriber = subscriber.upgrade()?;
            info!("doing resubscriptions");
            let now = Instant::now();
            let (batch, timeout) = {
                let mut dead = Vec::new();
                let mut batch = Vec::new();
                let mut subscriber = subscriber.0.lock();
                let subscriber = &mut *subscriber;
                let durable_dead = &mut subscriber.durable_dead;
                let durable_pending = &mut subscriber.durable_pending;
                let mut max_tries = 1;
                let mut total_retries = 0;
                task::block_in_place(|| {
                    for (p, w) in durable_dead.iter() {
                        match w.upgrade() {
                            None => {
                                dead.push(p.clone());
                            }
                            Some(s) => {
                                let (next_try, tries) = {
                                    let mut dv = s.0.lock();
                                    match &mut dv.sub {
                                        DvState::Dead(d) => (d.next_try, d.tries),
                                        DvState::Subscribed(_) => unreachable!(),
                                    }
                                };
                                if next_try <= now {
                                    batch.push(p.clone());
                                    durable_pending.insert(p.clone(), w.clone());
                                    max_tries = max(max_tries, tries);
                                    total_retries += 1;
                                    if total_retries >= 100_000 {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    for p in dead.iter().chain(batch.iter()) {
                        durable_dead.remove(p);
                    }
                });
                let timeout = 30 + max(10, batch.len() / 10000) * max_tries;
                (batch, Duration::from_secs(timeout as u64))
            };
            if batch.len() == 0 {
                let mut subscriber = subscriber.0.lock();
                update_retry(&mut *subscriber, retry);
                None
            } else {
                update_retry(&mut *subscriber.0.lock(), retry);
                Some(subscriber.subscribe(batch, Some(timeout)).await)
            }
        }
        fn finish_resubscription_batch(
            subscriber: &SubscriberWeak,
            batch: &mut Vec<(Path, Result<Val>)>,
            retry: &mut Option<Instant>,
        ) {
            if let Some(subscriber) = subscriber.upgrade() {
                let mut subscriber = subscriber.0.lock();
                let now = Instant::now();
                for (p, r) in batch.drain(..) {
                    if let Some(ds) =
                        subscriber.durable_pending.remove(&p).and_then(|ds| ds.upgrade())
                    {
                        let dsw = ds.downgrade();
                        let mut dv = ds.0.lock();
                        match r {
                            Err(e) => match &mut dv.sub {
                                DvState::Subscribed(_) => unreachable!(),
                                DvState::Dead(d) => {
                                    d.tries += 1;
                                    let wait = Duration::from_secs(pick(d.tries) as u64);
                                    d.next_try = now + wait;
                                    let s = wait.as_secs();
                                    warn!(
                                        "resubscription error {}: {}, next try: {}s",
                                        p, e, s
                                    );
                                    subscriber.durable_dead.insert(p.clone(), dsw);
                                }
                            },
                            Ok(sub) => {
                                info!("resubscription success {}", p);
                                for (flags, tx) in dv.streams.0.iter().cloned() {
                                    sub.0.connection.send(ToCon::Stream {
                                        tx: tx.0,
                                        sub_id: dv.sub_id,
                                        id: sub.0.id,
                                        flags: flags | UpdatesFlags::BEGIN_WITH_LAST,
                                    });
                                }
                                if let DvState::Dead(d) = &mut dv.sub {
                                    for (v, resp) in d.queued_writes.drain(..) {
                                        sub.0
                                            .connection
                                            .send(ToCon::Write(sub.0.id, v, resp));
                                    }
                                }
                                dv.sub = DvState::Subscribed(sub);
                                subscriber.durable_alive.insert(p.clone(), dsw);
                            }
                        }
                    }
                }
                update_retry(&mut *subscriber, retry);
            }
        }
        async fn next_subscription_result(
            subscriptions: &mut VecDeque<
                Batched<FuturesUnordered<impl Future<Output = (Path, Result<Val>)>>>,
            >,
        ) -> BatchItem<(Path, Result<Val>)> {
            loop {
                if subscriptions.is_empty() {
                    return future::pending().await;
                } else if subscriptions.len() == 1 {
                    match subscriptions[0].next().await {
                        Some(v) => return v,
                        None => {
                            subscriptions.pop_front();
                        }
                    }
                } else {
                    let i = subscriptions
                        .iter_mut()
                        .enumerate()
                        .map(|(i, f)| f.next().map(move |r| (r, i)));
                    let ((r, i), _, _) = future::select_all(i).await;
                    match r {
                        Some(v) => return v,
                        None => {
                            subscriptions.remove(i);
                        }
                    }
                }
            }
        }
        let subscriber = self.downgrade();
        task::spawn(async move {
            let mut incoming = Batched::new(incoming.fuse(), 1_000_000_000);
            let mut subscriptions = VecDeque::new();
            let mut subscription_batch = Vec::new();
            let mut retry: Option<Instant> = None;
            loop {
                select_biased! {
                    m = incoming.next() => match m {
                        None => break,
                        Some(BatchItem::InBatch(())) => (),
                        Some(BatchItem::EndBatch) => {
                            if let Some(set) = do_resub(&subscriber, &mut retry).await {
                                subscriptions.push_back(Batched::new(set, 100_000));
                            }
                        }
                    },
                    m = next_subscription_result(&mut subscriptions).fuse() => match m {
                        BatchItem::InBatch((p, r)) => subscription_batch.push((p, r)),
                        BatchItem::EndBatch => {
                            finish_resubscription_batch(
                                &subscriber,
                                &mut subscription_batch,
                                &mut retry
                            );
                            if let Some(t) = retry {
                                if Instant::now() >= t {
                                    if let Some(set) = do_resub(&subscriber, &mut retry).await {
                                        subscriptions.push_back(Batched::new(set, 100_000));
                                    }
                                }
                            }
                        }
                    },
                    _ = wait_retry(retry).fuse() => {
                        if let Some(set) = do_resub(&subscriber, &mut retry).await {
                            subscriptions.push_back(Batched::new(set, 100_000));
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
    /// or aliases of the paths in the batch, you will receive a
    /// reference to the existing subscription. However subscriber
    /// does not retain strong references to subscribed values, so if
    /// you drop all of them it will be unsubscribed.
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
    /// `subscribe` future in a `tokio::time::timeout`.
    pub async fn subscribe(
        &self,
        batch: impl IntoIterator<Item = Path>,
        timeout: Option<Duration>,
    ) -> FuturesUnordered<impl Future<Output = (Path, Result<Val>)>> {
        #[derive(Debug)]
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
        // Init
        let r = {
            let mut t = self.0.lock();
            t.gc_recently_failed();
            for p in paths.clone() {
                match t.subscribed.entry(p.clone()) {
                    Entry::Vacant(e) => {
                        e.insert(SubStatus::Pending(Box::new(vec![])));
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
                                e.insert(SubStatus::Pending(Box::new(vec![])));
                                pending.insert(p, St::Resolve);
                            }
                        },
                    },
                }
            }
            t.resolver.clone()
        };
        // Resolve, Connect, Subscribe
        {
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
                Ok(Ok((publishers, mut res))) => {
                    let mut t = self.0.lock();
                    let deadline = timeout.map(|t| now + t);
                    let desired_auth = t.desired_auth.clone();
                    for (p, resolved) in to_resolve.into_iter().zip(res.drain(..)) {
                        if resolved.publishers.len() == 0 {
                            pending.insert(p, St::Error(anyhow!("path not found")));
                        } else if let Some((addr, target_auth, token)) =
                            t.choose_addr(&publishers, &resolved)
                        {
                            let sub_id = t.durable_id(&p).unwrap_or_else(SubId::new);
                            let con = t.connections.entry(addr).or_insert_with(|| {
                                let (tx, rx) = batch_channel::channel();
                                let subscriber = self.downgrade();
                                let desired_auth = desired_auth.clone();
                                task::spawn(async move {
                                    let res = connection(
                                        subscriber.clone(),
                                        addr,
                                        target_auth,
                                        rx,
                                        desired_auth,
                                    )
                                    .await;
                                    if let Some(subscriber) = subscriber.upgrade() {
                                        subscriber.0.lock().connections.remove(&addr);
                                        match res {
                                            Ok(()) => {
                                                info!("connection to {} closed", addr)
                                            }
                                            Err(e) => {
                                                subscriber
                                                    .0
                                                    .lock()
                                                    .recently_failed
                                                    .insert(addr, Instant::now());
                                                warn!(
                                                    "connection to {} failed {}",
                                                    addr, e
                                                )
                                            }
                                        }
                                    }
                                });
                                tx
                            });
                            let (tx, rx) = oneshot::channel();
                            let con_ = con.clone();
                            let r = con.send(ToCon::Subscribe(SubscribeValRequest {
                                path: p.clone(),
                                sub_id,
                                timestamp: resolved.timestamp,
                                permissions: resolved.permissions as u32,
                                token,
                                resolver: resolved.resolver,
                                finished: tx,
                                con: con_,
                                deadline,
                            }));
                            if r {
                                pending.insert(p, St::Subscribing(rx));
                            } else {
                                pending.insert(
                                    p,
                                    St::Error(Error::from(anyhow!("connection closed"))),
                                );
                            }
                        } else {
                            let e = anyhow!("missing publisher record");
                            pending.insert(p, St::Error(e));
                        }
                    }
                }
            }
        }
        // Wait
        async fn wait_result(sub: Subscriber, path: Path, st: St) -> (Path, Result<Val>) {
            match st {
                St::Resolve => unreachable!(),
                St::Subscribed(raw) => (path, Ok(raw)),
                St::Error(e) => {
                    let mut t = sub.0.lock();
                    if let Some(sub) = t.subscribed.remove(path.as_ref()) {
                        match sub {
                            SubStatus::Subscribed(_) => unreachable!(),
                            SubStatus::Pending(waiters) => {
                                for w in waiters.into_iter() {
                                    let err = Err(anyhow!("{}", e));
                                    let _ = w.send(err);
                                }
                            }
                        }
                    }
                    (path, Err(e))
                }
                St::WaitingOther(w) => match w.await {
                    Err(_) => (path, Err(anyhow!("other side died"))),
                    Ok(Err(e)) => (path, Err(e)),
                    Ok(Ok(raw)) => (path, Ok(raw)),
                },
                St::Subscribing(w) => {
                    let res = match w.await {
                        Err(_) => Err(anyhow!("connection died")),
                        Ok(Err(e)) => Err(e),
                        Ok(Ok(raw)) => Ok(raw),
                    };
                    let mut t = sub.0.lock();
                    match t.subscribed.entry(path.clone()) {
                        Entry::Vacant(_) => unreachable!(),
                        Entry::Occupied(mut e) => match res {
                            Err(err) => match e.remove() {
                                SubStatus::Subscribed(_) => unreachable!(),
                                SubStatus::Pending(waiters) => {
                                    for w in waiters.into_iter() {
                                        let err = Err(anyhow!("{}", err));
                                        let _ = w.send(err);
                                    }
                                    (path, Err(err))
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
                                        for w in waiters.into_iter() {
                                            let _ = w.send(Ok(raw.clone()));
                                        }
                                        (path, Ok(raw))
                                    }
                                }
                            }
                        },
                    }
                }
            }
        }
        pending.drain().map(|(path, st)| wait_result(self.clone(), path, st)).collect()
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
        self.subscribe(iter::once(path), timeout).await.next().await.unwrap().1
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
        if let Some(s) = t
            .durable_dead
            .get(&path)
            .or_else(|| t.durable_pending.get(&path))
            .or_else(|| t.durable_alive.get(&path))
        {
            if let Some(s) = s.upgrade() {
                return s;
            }
        }
        let s = Dval(Arc::new(Mutex::new(DvalInner {
            sub_id: SubId::new(),
            sub: DvState::Dead(Box::new(DvDead {
                queued_writes: Vec::new(),
                tries: 0,
                next_try: Instant::now(),
            })),
            streams: DvStreams::new(),
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
                    c.send(ToCon::Flush(tx));
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
    sub_id: SubId,
    streams: Streams,
    last: Option<TArc<Mutex<Event>>>,
    val: ValWeak,
}

type ByChan = FxHashMap<
    ChanId,
    (ChanWrap<Pooled<Vec<(SubId, Event)>>>, Pooled<Vec<(SubId, Event)>>),
>;

fn unsubscribe(
    subscriber: &mut SubscriberInner,
    by_chan: &mut ByChan,
    sub: Sub,
    id: Id,
    conid: ConId,
) {
    for (chan_id, c) in sub.streams.0.iter() {
        by_chan
            .entry(*chan_id)
            .or_insert_with(|| (c.clone(), BATCHES.take()))
            .1
            .push((sub.sub_id, Event::Unsubscribed))
    }
    if let Some(last) = &sub.last {
        *last.lock() = Event::Unsubscribed;
    }
    if let Some(dsw) = subscriber
        .durable_alive
        .remove(&sub.path)
        .or_else(|| subscriber.durable_pending.remove(&sub.path))
    {
        if let Some(ds) = dsw.upgrade() {
            let mut inner = ds.0.lock();
            inner.sub = DvState::Dead(Box::new(DvDead {
                queued_writes: Vec::new(),
                tries: 0,
                next_try: Instant::now(),
            }));
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
                    if s.0.id == id && s.0.conid == conid {
                        e.remove();
                    }
                }
            },
        },
    }
}

async fn hello_publisher(
    con: &mut Channel<ClientCtx>,
    desired_auth: &DesiredAuth,
    target_auth: &TargetAuth,
) -> Result<()> {
    use protocol::publisher::Hello;
    con.send_one(&2u64).await?;
    if con.receive::<u64>().await? != 2 {
        bail!("incompatible protocol version")
    }
    match (desired_auth, target_auth) {
        (DesiredAuth::Anonymous, TargetAuth::Anonymous) => {
            con.send_one(&Hello::Anonymous).await?;
            match con.receive().await? {
                Hello::Anonymous => (),
                _ => bail!("unexpected response from publisher"),
            }
        }
        (DesiredAuth::Anonymous, TargetAuth::Local { .. } | TargetAuth::Krb5 { .. }) => {
            bail!("anonymous access not allowed")
        }
        (DesiredAuth::Local | DesiredAuth::Krb5 { .. }, TargetAuth::Anonymous) => {
            bail!("authentication not supported")
        }
        (DesiredAuth::Local | DesiredAuth::Krb5 { .. }, TargetAuth::Local) => {
            con.send_one(&Hello::Local).await?;
            match con.receive().await? {
                Hello::Local => (),
                _ => bail!("unexpected response from publisher"),
            }
        }
        (DesiredAuth::Local, TargetAuth::Krb5 { .. }) => {
            bail!("local auth not supported")
        }
        (DesiredAuth::Krb5 { upn, .. }, TargetAuth::Krb5 { spn }) => {
            let upn = upn.as_ref().map(|p| p.as_str());
            con.send_one(&Hello::Krb5).await?;
            let ctx = krb5_authentication(upn, spn, con).await?;
            con.set_ctx(K5CtxWrap::new(ctx)).await;
            if con.receive::<Hello>().await? != Hello::Krb5 {
                bail!("protocol error")
            }
        }
    }
    Ok(())
}

const PERIOD: Duration = Duration::from_secs(100);

// This is the fast path for the common case where the batch contains
// only updates. As of 2020-04-30, sending to an mpsc channel is
// pretty slow, about 250ns, so we go to great lengths to avoid it.
async fn process_updates_batch(
    by_chan: &mut ByChan,
    mut batch: Pooled<Vec<From>>,
    subscriptions: &mut FxHashMap<Id, Sub>,
) {
    for m in batch.drain(..) {
        if let From::Update(i, m) = m {
            if let Some(sub) = subscriptions.get(&i) {
                for (chan_id, c) in sub.streams.0.iter() {
                    by_chan
                        .entry(*chan_id)
                        .or_insert_with(|| (c.clone(), BATCHES.take()))
                        .1
                        .push((sub.sub_id, Event::Update(m.clone())))
                }
                if let Some(last) = &sub.last {
                    *last.lock() = Event::Update(m);
                }
            }
        }
    }
    for (_, (mut c, batch)) in by_chan.drain() {
        let _ = c.0.send(batch).await;
    }
}

async fn process_batch(
    mut batch: Pooled<Vec<From>>,
    by_chan: &mut ByChan,
    subscriptions: &mut FxHashMap<Id, Sub>,
    pending: &mut HashMap<Path, SubscribeValRequest>,
    pending_writes: &mut FxHashMap<Id, VecDeque<oneshot::Sender<Value>>>,
    con: &mut WriteChannel<ClientCtx>,
    subscriber: &Subscriber,
    conid: ConId,
) -> Result<()> {
    for m in batch.drain(..) {
        match m {
            From::Update(i, m) => match subscriptions.get(&i) {
                Some(sub) => {
                    for (chan_id, c) in sub.streams.0.iter() {
                        by_chan
                            .entry(*chan_id)
                            .or_insert_with(|| (c.clone(), BATCHES.take()))
                            .1
                            .push((sub.sub_id, Event::Update(m.clone())));
                    }
                    if let Some(last) = &sub.last {
                        *last.lock() = Event::Update(m);
                    }
                }
                None => con.queue_send(&To::Unsubscribe(i))?,
            },
            From::Heartbeat => (),
            From::WriteResult(id, v) => {
                let mut empty = false;
                if let Some(q) = pending_writes.get_mut(&id) {
                    if let Some(tx) = q.pop_front() {
                        let _ = tx.send(v);
                    }
                    empty = q.is_empty();
                }
                if empty {
                    pending_writes.remove(&id);
                }
            }
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
                    unsubscribe(&mut *t, by_chan, s, id, conid);
                }
            }
            From::Subscribed(p, id, m) => match pending.remove(&p) {
                None => con.queue_send(&To::Unsubscribe(id))?,
                Some(req) => match subscriptions.get_mut(&id) {
                    Some(sub) => match sub.val.upgrade() {
                        Some(val) => {
                            let _ = req.finished.send(Ok(val));
                        }
                        None => {
                            let _ = req.finished.send(Err(anyhow!(
                                "subscribe alias while unsubscribing"
                            )));
                        }
                    },
                    None => {
                        let last = TArc::new(Mutex::new(Event::Update(m)));
                        let s = Val(Arc::new(ValInner {
                            sub_id: req.sub_id,
                            id,
                            conid,
                            connection: req.con,
                            last: last.clone(),
                        }));
                        match req.finished.send(Ok(s.clone())) {
                            Err(_) => con.queue_send(&To::Unsubscribe(id))?,
                            Ok(()) => {
                                subscriptions.insert(
                                    id,
                                    Sub {
                                        path: req.path,
                                        sub_id: req.sub_id,
                                        last: Some(last),
                                        streams: Streams::new(),
                                        val: s.downgrade(),
                                    },
                                );
                            }
                        }
                    }
                },
            },
        }
    }
    for (_, (mut c, batch)) in by_chan.drain() {
        let _ = c.0.send(batch).await;
    }
    Ok(())
}

fn try_flush(con: &mut WriteChannel<ClientCtx>) -> Result<()> {
    if con.bytes_queued() > 0 {
        con.try_flush()?;
        Ok(())
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
    target_auth: TargetAuth,
    from_sub: BatchReceiver<ToCon>,
    auth: DesiredAuth,
) -> Result<()> {
    let mut pending: HashMap<Path, SubscribeValRequest> = HashMap::new();
    let mut subscriptions: FxHashMap<Id, Sub> = HashMap::default();
    let mut msg_recvd = false;
    let soc = time::timeout(PERIOD, TcpStream::connect(addr)).await??;
    let conid = ConId::new();
    soc.set_nodelay(true)?;
    let mut con = Channel::new(soc);
    const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
    time::timeout(HELLO_TIMEOUT, hello_publisher(&mut con, &auth, &target_auth))
        .await??;
    let (read_con, mut write_con) = con.split();
    let (tx_stop, rx_stop) = oneshot::channel();
    let mut pending_writes: FxHashMap<Id, VecDeque<oneshot::Sender<Value>>> =
        HashMap::default();
    let mut batches = decode_task(read_con, rx_stop);
    let mut periodic = time::interval_at(Instant::now() + PERIOD, PERIOD);
    let mut by_receiver: FxHashMap<ChanWrap<Pooled<Vec<(SubId, Event)>>>, ChanId> =
        HashMap::default();
    let mut by_chan: ByChan = FxHashMap::default();
    let res = 'main: loop {
        select_biased! {
            now = periodic.tick().fuse() => {
                if !msg_recvd {
                    break 'main Err(anyhow!("hung publisher"));
                } else {
                    msg_recvd = false;
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
                try_cf!(try_flush(&mut write_con))
            },
            batch = from_sub.recv().fuse() => match batch {
                None => break Err(anyhow!("dropped")),
                Some(mut batch) => {
                    for msg in batch.drain(..) {
                        match msg {
                            ToCon::Subscribe(req) => {
                                let path = req.path.clone();
                                let resolver = req.resolver;
                                let token = req.token.clone();
                                let permissions = req.permissions;
                                let timestamp = req.timestamp;
                                pending.insert(path.clone(), req);
                                try_cf!(break, 'main, write_con.queue_send(&To::Subscribe {
                                    path,
                                    resolver,
                                    timestamp,
                                    permissions,
                                    token,
                                }))
                            }
                            ToCon::Unsubscribe(id) => {
                                info!("unsubscribe {:?}", id);
                                try_cf!(
                                    break,
                                    'main,
                                    write_con.queue_send(&To::Unsubscribe(id))
                                )
                            }
                            ToCon::Stream { id, sub_id, mut tx, flags } => {
                                if let Some(sub) = subscriptions.get_mut(&id) {
                                    let mut already_have = false;
                                    for (_, c) in sub.streams.0.iter() {
                                        if tx.same_receiver(&c.0) {
                                            already_have = true;
                                        }
                                        if c.0.is_closed() {
                                            by_receiver.remove(&c);
                                        }
                                    }
                                    if flags.contains(UpdatesFlags::BEGIN_WITH_LAST)
                                        && !(already_have && flags.contains(UpdatesFlags::NO_SPURIOUS))
                                    {
                                        if let Some(last) = &sub.last {
                                            let m = last.lock().clone();
                                            let mut b = BATCHES.take();
                                            b.push((sub_id, m));
                                            match tx.send(b).await {
                                                Err(_) => continue,
                                                Ok(()) => ()
                                            }
                                        }
                                    }
                                    if flags.contains(UpdatesFlags::STOP_COLLECTING_LAST) {
                                        sub.last = None;
                                    }
                                    if !already_have {
                                        let tx = ChanWrap(tx);
                                        let id = by_receiver.entry(tx.clone())
                                            .or_insert_with(ChanId::new);
                                        sub.streams = sub.streams.add(*id, tx);
                                    }
                                }
                            }
                            ToCon::Write(id, v, tx) => {
                                try_cf!(
                                    break,
                                    'main,
                                    write_con.queue_send(&To::Write(id, v, tx.is_some()))
                                );
                                if let Some(tx) = tx {
                                    pending_writes
                                        .entry(id)
                                        .or_insert_with(VecDeque::new)
                                        .push_back(tx);
                                }
                            }
                            ToCon::Flush(tx) => {
                                let _ = tx.send(());
                            }
                        }
                    }
                    try_cf!(try_flush(&mut write_con));
                }
            },
            r = batches.next() => match r {
                Some(Ok((batch, true))) => {
                    msg_recvd = true;
                    process_updates_batch(
                        &mut by_chan,
                        batch,
                        &mut subscriptions
                    ).await;
                    try_cf!(try_flush(&mut write_con))
                },
                Some(Ok((batch, false))) =>
                    if let Some(subscriber) = subscriber.upgrade() {
                        msg_recvd = true;
                        try_cf!(process_batch(
                            batch,
                            &mut by_chan,
                            &mut subscriptions,
                            &mut pending,
                            &mut pending_writes,
                            &mut write_con,
                            &subscriber,
                            conid).await);
                        try_cf!(try_flush(&mut write_con));
                        if subscriptions.is_empty() && pending.is_empty() {
                            let mut inner = subscriber.0.lock();
                            if from_sub.len() == 0 {
                                inner.connections.remove(&addr);
                                let _ = tx_stop.send(());
                                return Ok(())
                            }
                        }
                    }
                Some(Err(e)) => break Err(Error::from(e)),
                None => break Err(anyhow!("EOF")),
            },
        }
    };
    if let Some(subscriber) = subscriber.upgrade() {
        let mut batch = DECODE_BATCHES.take();
        batch.extend(subscriptions.keys().map(|id| From::Unsubscribed(*id)));
        let _ = process_batch(
            batch,
            &mut by_chan,
            &mut subscriptions,
            &mut pending,
            &mut pending_writes,
            &mut write_con,
            &subscriber,
            conid,
        )
        .await;
        for (_, req) in pending {
            let _ = req.finished.send(Err(anyhow!("connection died")));
        }
    }
    let _ = tx_stop.send(());
    res
}
