use crate::{
    path::Path,
    utils::{BatchItem, Batched},
    publisher::{ToPublisher, FromPublisher, Id},
    resolver::{Resolver, ReadOnly},
    channel::Channel,
};
use std::{
    mem, iter,
    result::Result,
    marker::PhantomData,
    collections::{HashMap, hash_map::Entry},
    net::{SocketAddr, ToSocketAddrs},
    sync::{Arc, Weak, atomic::{Ordering, AtomicBool}},
    cmp::min,
    time::{Instant, Duration},
};
use tokio::{
    task,
    sync::{oneshot, mpsc::{self, Sender, UnboundedReceiver, UnboundedSender}},
    net::TcpStream,
};
use fxhash::FxBuildHasher;
use futures::{
    prelude::*,
    select,
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
    finished: oneshot::Sender<Result<RawSubscription, Error>>,
    con: UnboundedSender<ToCon>,
}

#[derive(Debug)]
enum ToCon {
    Subscribe(SubscribeRequest),
    Unsubscribe(Id),
    Stream {
        id: Id,
        tx: Sender<Bytes>,
        last: bool,
    },
    NotifyDead(Id, oneshot::Sender<()>),
}

#[derive(Debug)]
enum RawSubIntInner {
    Dropped,
    Subscribing {
        queued: Vec<(bool, Sender<Bytes>)>,
        tries: usize,
        next: Instant,
    },
    Subscribed {
        path: Path,
        id: Id,
        addr: SocketAddr,
        connection: UnboundedSender<ToCon>,
        last: Bytes,
    },
}

#[derive(Debug, Clone)]
pub enum Update<T> {
    Update(T),
    Failed(Option<Error>),
}

#[derive(Debug, Clone)]
struct RawSubInt(Arc<Mutex<RawSubIntInner>>);

struct RawSubInner(RawSubInt);

impl Drop for RawSubInner {
    fn drop(&mut self) {
        let mut s = self.0.lock();
        match s {
            RawSubIntInner::Dropped | RawSubIntInner::Subscribing {..} => (),
            RawSubIntInner::Subscribed {id, connection, ..} => {
                let _ = connection.send(ToCon::Unsubscribe(id));
            },
        }
        *s = RawSubIntInner::Dropped;
        // CR add cleanup task
    }
}

#[derive(Debug, Clone)]
pub struct RawSubWeak(Weak<RawSubInner>);

impl RawSubWeak {
    fn upgrade(&self) -> Option<RawSub> {
        Arc::upgrade(&self.0).map(|s| RawSub(s))
    }
}

#[derive(Debug, Clone)]
pub struct RawSub(Arc<RawSubInner>);

impl RawSub {
    pub fn typed<T: DeserializeOwned>(self) -> Sub<T> {
        Sub(self, PhantomData)
    }

    pub fn downgrade(&self) -> RawSubscriptionWeak {
        RawSubWeak(Arc::downgrade(&self.0))
    }

    /// Get the last published value if there is one.
    pub fn last(&self) -> Option<Bytes> {
        match self.0.0.lock() {
            RawSubIntInner::Subscribed {last, ..} => Some(last.clone()),
            RawSubIntInner::Subscribing {..} | RawSubIntInner::Dropped => None,
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
    /// When the subscription dies the stream will end.
    pub fn updates(&self, begin_with_last: bool) -> impl Stream<Item = Update<Bytes>> {
        let (tx, rx) = mpsc::channel(10);
        match *self.0.0.lock() {
            RawSubIntInner::Dropped => unreachable!(),
            RawSubIntInner::Subscribing {ref mut queued, ..} => {
                queued.push((begin_with_last, tx));
            },
            RawSubIntInner::Subscribed { ref mut connection, id, ..} => {
                let m = ToCon::Stream { tx, last: begin_with_last, id };
                let _ = connection.send(m);
            },
        }
        rx
    }
}

/// A typed version of RawSub
#[derive(Debug, Clone)]
pub struct Sub<T: DeserializeOwned>(RawSub, PhantomData<T>);

impl<T: DeserializeOwned> Sub<T> {
    /// Get the `RawSub`
    pub fn raw(self) -> RawSub {
        self.0
    }

    /// Get and decode the last published value.
    pub fn last(&self) -> Option<Result<T, Error>> {
        self.0.last().map(|b| Ok(rmp_serde::decode::from_read(&*b)?))
    }

    /// Same as `RawSubscription::updates` but it decodes the value
    pub fn updates(
        &self,
        begin_with_last: bool
    ) -> impl Stream<Item = Update<Result<T, rmp_serde::decode::Error>>> {
        self.0.updates(begin_with_last).map(|u| match u {
            Update::Update(v) => Update::Update(rmp_serde::decode::from_read(&*v)),
            Update::Failed(e) => Update::Failed(e),
        })
    }
}

struct SubscriberInner {
    resolver: Resolver<ReadOnly>,
    connections: HashMap<SocketAddr, UnboundedSender<ToCon>, FxBuildHasher>,
    subscribed: HashMap<Path, RawSubInt>,
    dirty: HashMap<Path, RawSubInt>,
}

#[derive(Clone)]
pub struct Subscriber(Arc<Mutex<SubscriberInner>>);

impl Subscriber {
    pub fn new<T: ToSocketAddrs>(addrs: T) -> Result<Subscriber, Error> {
        Ok(Subscriber(Arc::new(Mutex::new(SubscriberInner {
            resolver: Resolver::<ReadOnly>::new_r(addrs)?,
            connections: HashMap::with_hasher(FxBuildHasher::default()),
            subscribed: HashMap::new(),
            dirty: HashMap::new(),
        }))))
    }

    async fn subscribe_and_cleanup(&self, now: Instant) {
        let (to_resolve, mut r) = {
            let mut to_resolve = Vec::new();
            let mut t = self.0.lock();
            t.dirty.retain(|p, s| match *s.0.lock() {
                RawSubIntInner::Subscribed {..} => false,
                RawSubIntInner::Dropped => { t.subscribed.remove(p); false }
                RawSubIntInner::Subscribing {queued, next, ..} => {
                    if next > now {
                        true
                    } else {
                        to_resolve.push(p);
                        false
                    }
                }
            });
            (to_resolve, t.resolver.clone())
        };
        async fn reset_on_error(
            sub: &Subscriber,
            paths: Vec<(Path, Error)>,
            now: Instant
        ) {
            let notify = Vec::new();
            {
                let mut t = sub.0.lock();
                for (p, e) in paths {
                    let sub = t.subscribed[&p];
                    match *sub.0.lock() {
                        RawSubIntInner::Subscribed {..} => unreachable!(),
                        RawSubIntInner::Dropped => { t.subscribed.remove(&p); }
                        RawSubIntInner::Subscribing {
                            ref queued, ref mut next, ref mut tries
                        } => {
                            notify.push((queued.clone(), e));
                            *tries += 1;
                            *next = now + Duration::from_secs(*tries as u64);
                            t.dirty.insert(p, sub.clone());
                        }
                    }
                }
            }
            for (q, e) in notify {
                for (_, mut s) in q {
                    let m = Update::Failed(Some(format_err!("{}", e)));
                    let _ = s.send(m).await;
                }
            }
        }
        let mut rng = rand::thread_rng();
        match r.resolve(to_resolve.clone()).await {
            Err(e) => {
                let paths =
                    to_resolve.into_iter()
                    .map(|p| (p, format_err!("{}", e)))
                    .collect::<Vec<_>>();
                reset_on_error(self, paths).await;
            }
            Ok(addrs) => {
                let mut errors = Vec::new();
                {
                    let mut t = self.0.lock();
                    for (p, addrs) in to_resolve.into_iter().zip(addrs.into_iter()) {
                        if addrs.len() == 0 {
                            errors.push((p, format_err!("path not found")));
                        } else {
                            let addr = {
                                if addrs.len() == 1 {
                                    addrs[0]
                                } else {
                                    addrs[rng.gen_range(0, addrs.len())]
                                }
                            };
                            let con =
                                t.connections.entry(addr)
                                .or_insert_with(|| {
                                    let (tx, rx) = mpsc::unbounded_channel();
                                    task::spawn(connection(self.clone(), addr, rx));
                                    tx
                                });
                            let con_ = con.clone();
                            let r = con.send(ToCon::Subscribe(SubscribeRequest {
                                con: con_,
                                path: p.clone(),
                                finished: tx,
                            }));
                            match r {
                                Ok(()) => (),
                                Err(e) => { errors.push((p, Error::from(e))); }
                            }
                        }
                    }
                }
                if errors.len() > 0 {
                    reset_on_error(self, errors).await;
                }
            }
        }
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
    pub async fn subscribe<T: DeserializeOwned>(
        &self, batch: impl IntoIterator<Item = Path>,
    ) -> Vec<(Path, Result<Subscription<T>, Error>)> {
        self.subscribe_raw(batch).await.into_iter().map(|(p, r)| {
            (p, r.map(|r| r.typed()))
        }).collect()
    }

    /// Subscribe to one path. This is sufficient for a small number
    /// of paths, but if you need to subscribe to a lot of things use
    /// `subscribe`
    pub async fn subscribe_one<T: DeserializeOwned>(
        &self,
        path: Path
    ) -> Result<Subscription<T>, Error> {
        self.subscribe_raw(iter::once(path)).await.pop().unwrap().1.map(|v| v.typed())
    }
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
            *sub.last.lock() = msg;
        }
        Entry::Vacant(e) => if let Some(req) = next_sub.take() {
            let dead = Arc::new(AtomicBool::new(false));
            let last = Arc::new(Mutex::new(msg));
            e.insert(Sub {
                path: req.path,
                last: last.clone(),
                dead: dead.clone(),
                deads: Vec::new(),
                streams: Vec::new(),
            });
            let s = RawSubscriptionInner { id, addr, dead, connection: req.con, last };
            let _ = req.finished.send(Ok(RawSubscription(Arc::new(s))));
        }
    }
}

fn unsubscribe(
    sub: Sub,
    id: Id,
    addr: SocketAddr,
    subscribed: &mut HashMap<Path, SubStatus>
) {
    sub.dead.store(true, Ordering::Relaxed);
    match subscribed.entry(sub.path) {
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
            outstanding.pop();
            if let Some(s) = subscriptions.remove(&id) {
                let mut t = subscriber.0.lock();
                unsubscribe(s, id, addr, &mut t.subscribed);
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
    subscriber: Subscriber,
    to: SocketAddr,
    from_sub: UnboundedReceiver<ToCon>
) -> Result<(), Error> {
    let mut from_sub = Batched::new(from_sub, BATCH).fuse();
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
                Ok(()) => {
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
                Some(BatchItem::InBatch(ToCon::Stream { id, mut tx, last })) => {
                    if let Some(sub) = subscriptions.get_mut(&id) {
                        let mut add = true;
                        if last {
                            let last = sub.last.lock().clone();
                            if let Err(_) = tx.send(last).await {
                                add = false;
                            }
                        }
                        if add {
                            sub.streams.push(tx);
                        }
                    }
                }
                Some(BatchItem::InBatch(ToCon::NotifyDead(id, tx))) => {
                    if let Some(sub) = subscriptions.get_mut(&id) {
                        sub.deads.push(tx);
                    }
                }
            },
        }
    };
    let mut t = subscriber.0.lock();
    for (id, sub) in subscriptions {
        unsubscribe(sub, id, to, &mut t.subscribed);
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
            let addr = *server.local_addr();
            let (tx, ready) = oneshot::channel();
            task::spawn(async move {
                let publisher = Publisher::new(addr, BindCfg::Local).await.unwrap();
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
            let subscriber = Subscriber::new(addr).unwrap();
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
