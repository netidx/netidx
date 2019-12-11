use std::{
    result, sync::{Arc, Weak}, io::BufReader, convert::AsRef, net::SocketAddr,
    collections::{hash_map::{Entry, OccupiedEntry}, HashMap, VecDeque},
    marker::PhantomData, mem
};
use rand;
use futures::{self, prelude::*, Poll, Async, channel::oneshot};
use serde::{Serialize, de::DeserializeOwned};
use serde_json;
use crate::{
    path::Path;
    publisher::{FromPublisher, ToPublisher},
    resolver::Resolver,
};
parking_lot::RwLock;
use bytes::Bytes;

struct UntypedSubscriptionInner {
    path: Path,
    current: Bytes,
    subscriber: Subscriber,
    connection: Connection,
    streams: Vec<UntypedUpdatesWeak>,
    nexts: Vec<oneshot::Sender<Result<()>>>,
    deads: Vec<oneshot::Sender<()>>,
    dead: bool
}

impl UntypedSubscriptionInner {
    fn unsubscribe(&mut self) {
        if !self.dead {
            self.dead = true;
            self.subscriber.fail_pending(&self.path, "dead");
            for dead in self.deads.drain(0..) {
                let _ = dead.send(());
            }
            for strm in self.streams.iter() {
                if let Some(strm) = strm.upgrade() {
                    let mut strm = strm.0.write().unwrap();
                    strm.ready.push_back(None);
                    if let Some(ref notify) = strm.notify { (notify)() }
                    strm.notify = None
                }
            }
        }
    }
}

impl Drop for UntypedSubscriptionInner {
    fn drop(&mut self) {
        self.unsubscribe();
    }
}

#[derive(Clone)]
struct UntypedSubscriptionWeak(Weak<RwLock<UntypedSubscriptionInner>>);

impl UntypedSubscriptionWeak {
    fn upgrade(&self) -> Option<UntypedSubscription> {
        Weak::upgrade(&self.0).map(|r| UntypedSubscription(r))
    }
}

#[derive(Clone)]
struct UntypedSubscription(Arc<RwLock<UntypedSubscriptionInner>>);

impl UntypedSubscription {
    fn new(
        path: Path,
        current: Bytes,
        connection: Connection,
        subscriber: Subscriber
    ) -> UntypedSubscription {
        let inner = UntypedSubscriptionInner {
            path, current, connection, subscriber,
            dead: false, streams: Vec::new(), nexts: Vec::new(),
            deads: Vec::new()
        };
        UntypedSubscription(Arc::new(RwLock::new(inner)))
    }

    fn downgrade(&self) -> UntypedSubscriptionWeak {
        UntypedSubscriptionWeak(Arc::downgrade(&self.0))
    }

    async fn next(self) -> Result<(), Error> {
        let rx = {
            let mut t = self.0.write();
            let (tx, rx) = oneshot::channel();
            if t.dead {
                let _ = tx.send(Err(Error::from(ErrorKind::SubscriptionIsDead))); rx
            } else {
                t.nexts.push(tx);
                rx
            }
        };
        Ok(rx.await??)
    }

    fn updates(&self, max_q: usize) -> UntypedUpdates {
        let inner = UntypedUpdatesInner {
            notify: None,
            hold: None,
            ready: VecDeque::new(),
            max_q,
        };
        let t = UntypedUpdates(Arc::new(RwLock::new(inner)));
        self.0.write().streams.push(t.downgrade());
        t
    }
}

struct UntypedUpdatesInner {
    notify: Option<Box<Fn() -> () + Send + Sync + 'static>>,
    hold: Option<oneshot::Sender<()>>,
    ready: VecDeque<Option<Bytes>>,
    max_q: usize,
}

#[derive(Clone)]
struct UntypedUpdates(Arc<RwLock<UntypedUpdatesInner>>);

impl UntypedUpdates {
    fn downgrade(&self) -> UntypedUpdatesWeak {
        UntypedUpdatesWeak(Arc::downgrade(&self.0))
    }
}

#[derive(Clone)]
struct UntypedUpdatesWeak(Weak<RwLock<UntypedUpdatesInner>>);

impl UntypedUpdatesWeak {
    fn upgrade(&self) -> Option<UntypedUpdates> {
        Weak::upgrade(&self.0).map(|v| UntypedUpdates(v))
    }
}

impl Stream for UntypedUpdates {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut t = self.0.write().unwrap();
        let res =
            match t.ready.pop_front() {
                Some(v) => Ok(Async::Ready(v)),
                None => {
                    let task = futures::task::current();
                    t.notify = Some(Box::new(move || task.notify()));
                    Ok(Async::NotReady)
                }
            };
        if t.ready.len() < t.max_q / 2 {
            let mut hold = None;
            mem::swap(&mut t.hold, &mut hold);
            if let Some(c) = hold { let _ = c.send(()); }
        }
        res
    }
}

/// This represents a subscription to one value. Internally this type
/// is wrapped in an Arc, so cloning it is nearly free. When all
/// references to a subscription are dropped the subscriber will
/// unsubscribe from the value.
pub struct Subscription<T> {
    untyped: UntypedSubscription,
    phantom: PhantomData<T>
}

impl<T> ::std::fmt::Debug for Subscription<T> {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> result::Result<(), ::std::fmt::Error> {
        let ut = self.untyped.0.read().unwrap();
        write!(f, "Subscription (path = {:?})", ut.path)
    }
}

impl<T> Clone for Subscription<T> {
    fn clone(&self) -> Self {
        Subscription { untyped: self.untyped.clone(), phantom: PhantomData }
    }
}

impl<T> Subscription<T> where T: DeserializeOwned {
    fn new(untyped: UntypedSubscription) -> Subscription<T> {
        Subscription { untyped, phantom: PhantomData }
    }

    /// get the current value, return an error if the value can't be
    /// deserialized as a `T`. There are no other possible errors, a
    /// subscription always has a current value. 
    pub fn get(&self) -> Result<T> {
        let cur = {
            let ut = self.untyped.0.read();
            ut.current.clone()
        };
        Ok(rmp_serde::decode::from_read(&*cur)?)
    }

    /// get the raw value.
    pub fn get_raw(&self) -> Bytes {
        let ut = self.untyped.0.read();
        ut.current.clone()
    }

    /// return true if the subscription is dead, false otherwise.
    pub fn is_dead(&self) -> bool {
        let ut = self.untyped.0.read();
        ut.dead
    }

    /// return a future that will become ready if the subscription dies.
    pub fn dead(&self) -> impl Future<Item = Result<(), Error>> {
        let rx = {
            let (tx, rx) = oneshot::channel();
            let mut ut = self.untyped.0.write();
            if ut.dead { let _ = tx.send(()); rx }
            else {
                ut.deads.push(tx);
                rx
            }
        };
        async move { Ok(rx.await?) }
    }

    /// return a future that will become ready when the value is
    /// updated. Interest in the update will be recorded when you call
    /// `next`, not when you `await!` the returned future. This can be
    /// important in some cases, e.g. when using `write` to ask the
    /// publisher to do something and then update.
    ///
    /// ```
    /// let s0_next = s0.next(); // don't await yet, but do queue our interest
    /// await!(s0.write(PleaseDoSomething)).unwrap();
    ///
    /// // wait for the publisher to do something, and update. If we had
    /// // started the `next` call after the write we might have missed the
    /// // new value, and ended up waiting forever.
    ///
    /// await!(s0_next).unwrap();
    /// println!("{}", s0.get().unwrap())
    /// ```
    pub fn next(self) -> impl Future<Item=(), Error=Error> {
        self.untyped.clone().next()
    }

    /// return a stream of updates to the value. All updates will arrive
    /// in the sequence they were sent. No update will be skipped. The
    /// current value will NOT be included in the stream, only
    /// subsuquent values. `get` followed by `updates` cannot be relied
    /// upon not to miss an update between the two calls, as such
    /// protocols requiring strict ordering and no skipping should rely
    /// only on `updates`, and should not use `get`.
    /// # Example
    /// ```
    /// #[async]
    /// for v in s0.updates() {
    ///     process_update(v)
    /// }
    /// ```
    pub fn updates(&self, max_q: usize) -> impl Stream<Item=T, Error=Error> {
        self.untyped.updates(max_q).and_then(|v| {
            Ok(serde_json::from_str(v.as_ref())?)
        })
    }

    /// return a stream of raw JSON string updates to the value,
    /// otherwise semantically the same as `updates`.
    pub fn updates_raw(&self, max_q: usize) -> impl Stream<Item=Arc<String>, Error=Error> {
        self.untyped.updates(max_q)
    }
}

struct ConnectionInner {
    pending: HashMap<Path, oneshot::Sender<Result<UntypedSubscription>>>,
    subscriptions: HashMap<Path, UntypedSubscriptionWeak>,
    writer: LineWriter<Vec<u8>, TcpStream>,
    subscriber: Subscriber,
    addr: SocketAddr
}

impl Drop for ConnectionInner {
    fn drop(&mut self) {
        self.writer.shutdown();
        self.subscriber.0.write().unwrap().connections.remove(&self.addr);
    }
}

/// This is only public because of rustc/#50865, it will be private
/// when that bug is fixed
#[derive(Clone)]
pub struct ConnectionWeak(Weak<RwLock<ConnectionInner>>);

impl ConnectionWeak {
    fn upgrade(&self) -> Option<Connection> {
        Weak::upgrade(&self.0).map(|r| Connection(r))
    }
}

#[derive(Clone)]
struct Connection(Arc<RwLock<ConnectionInner>>);

impl Connection {
    #[async]
    fn new(
        addr: SocketAddr,
        subscriber: Subscriber
    ) -> Result<(ReadHalf<TcpStream>, Connection)> {
        let s = await!(TcpStream::connect(&addr))?;
        s.set_nodelay(true)?;
        let (rd, wr) = s.split();
        let inner = ConnectionInner {
            subscriber, addr,
            pending: HashMap::new(),
            subscriptions: HashMap::new(),
            writer: LineWriter::new(wr),
        };
        Ok((rd, Connection(Arc::new(RwLock::new(inner)))))
    }

    fn downgrade(&self) -> ConnectionWeak { ConnectionWeak(Arc::downgrade(&self.0)) }

    fn write<T: Serialize>(&self, v: &T) -> Result<()> {
        let d = serde_json::to_vec(v)?;
        Ok(self.0.read().unwrap().writer.write_one(d))
    }

    fn flush(&self) -> impl Future<Item=(), Error=Error> {
        let w = self.0.read().unwrap().writer.clone();
        w.flush()
    }
}

enum ConnectionState {
    Pending(Vec<oneshot::Sender<Result<Connection>>>),
    Connected(ConnectionWeak)
}

enum SubscriptionState {
    Pending(Vec<oneshot::Sender<Result<UntypedSubscription>>>),
    Subscribed(UntypedSubscriptionWeak),
}

struct SubscriberInner {
    resolver: Resolver,
    connections: HashMap<SocketAddr, ConnectionState>,
    subscriptions: HashMap<Path, SubscriptionState>,
}

/// This is only public because of rustc/#50865, it will be private
/// when that bug is fixed
#[derive(Clone)]
pub struct SubscriberWeak(Weak<RwLock<SubscriberInner>>);

impl SubscriberWeak {
    fn upgrade(&self) -> Option<Subscriber> {
        Weak::upgrade(&self.0).map(|r| Subscriber(r))
    }
}

/// This encapsulates the subscriber. Internally it is wrapped in an
/// Arc, so cloning it is virtually free. If you are done subscribing
/// you can safely drop references to it, it will be kept alive as
/// long as there are living subscriptions.
#[derive(Clone)]
pub struct Subscriber(Arc<RwLock<SubscriberInner>>);

fn get_pending<'a>(
    e: &'a mut OccupiedEntry<SocketAddr, ConnectionState>
) -> &'a mut Vec<oneshot::Sender<Result<Connection>>> {
    match e.get_mut() {
        ConnectionState::Connected(_) => unreachable!("bug"),
        ConnectionState::Pending(ref mut q) => q
    }
}

fn choose_address(addrs: &mut Vec<SocketAddr>) -> Option<SocketAddr> {
    let len = addrs.len();
    if len == 0 { None }
    else if len == 1 { addrs.pop() }
    else {
        use rand::distributions::range::Range;
        use rand::distributions::IndependentSample;
        let mut rng = rand::thread_rng();
        let r = Range::new(0usize, len);
        Some(addrs.remove(r.ind_sample(&mut rng)))
    }
}

impl Subscriber {
    /// Create a new subscriber.
    pub fn new(resolver: Resolver) -> Subscriber {
        let inner = SubscriberInner {
            resolver,
            connections: HashMap::new(),
            subscriptions: HashMap::new(),
        };
        Subscriber(Arc::new(RwLock::new(inner)))
    }

    fn downgrade(&self) -> SubscriberWeak {
        SubscriberWeak(Arc::downgrade(&self.0))
    }

    fn fail_pending<E: Into<Error> + ToString>(&self, path: &Path, e: E) -> Error {
        let mut t = self.0.write().unwrap();
        match t.subscriptions.get_mut(path) {
            None | Some(SubscriptionState::Subscribed(_)) => (),
            Some(SubscriptionState::Pending(ref mut q)) =>
                for c in q.drain(0..) { let _ = c.send(Err(Error::from(e.to_string()))); }
        }
        t.subscriptions.remove(path);
        e.into()
    }

    // CR estokes: add a timeout to prevent various network events from
    // making subscribe hang forever?
    /// Subscribe to the specified path, expecting the resulting values
    /// to have type `T`. Will fail if the value is not available, the
    /// publisher can't be reached, or the resolver server can't be
    /// reached.
    ///
    /// # Multiple Publishers for `path`
    ///
    /// In the case where multiple publishers publish values to `path`,
    /// `subscribe` will create a rendom permutation of the list of
    /// publishers of `path`, and will try to subscribe to each until
    /// one succeeds or all of them have failed. In the case where one
    /// succeeds `subscribe` returns normally, otherwise it returns the
    /// last error it encountered.
    ///
    /// # Already Subscribed to `path`
    ///
    /// If you are already subscribed to `path` then calling subscribe
    /// again will not cause any additional message to be sent to the
    /// publisher, you will just get another handle to the subscription,
    /// as if you had called `clone` on it. However because each call to
    /// `subscribe` can have a different `T` you can have multiple
    /// subscriptions to the same value that are deserialized to
    /// different types. Again, this won't cause the publisher to send
    /// you twice as much data, you'll just deserialize the same data
    /// into two or more different types. In some cases that is a
    /// perfectly reasonable thing to want to do (e.g. deserialize into
    /// a `serde_json::Value` and also a concrete type).
    ///
    /// # When Deserialization Happens
    ///
    /// Regarding deserialization, you only pay for it when you look at
    /// the value. E.G. when you call `get`, or when you call
    /// `updates`. If you never do either of these things then the
    /// values will not be deserialized at all. So, for example, if you
    /// only care to occasionally sample a quickly updating value, don't
    /// call `updates`, call `get` on a timer, and you won't have to pay
    /// to deserialize every value in the stream, just the ones you look
    /// at.
    #[async]
    pub fn subscribe<T: DeserializeOwned>(
        self,
        path: Path
    ) -> Result<Subscription<T>> {
        enum Action {
            Subscribed(UntypedSubscription),
            Subscribe(Resolver),
            Wait(oneshot::Receiver<Result<UntypedSubscription>>)
        }
        let action = {
            let mut t = self.0.write().unwrap();
            let resolver = t.resolver.clone();
            match t.subscriptions.entry(path.clone()) {
                Entry::Vacant(e) => {
                    e.insert(SubscriptionState::Pending(Vec::new()));
                    Action::Subscribe(resolver)
                },
                Entry::Occupied(mut e) => {
                    let action =
                        match e.get_mut() {
                            SubscriptionState::Subscribed(ref ut) => {
                                match ut.upgrade() {
                                    Some(ut) => Action::Subscribed(ut),
                                    None => Action::Subscribe(resolver),
                                }
                            },
                            SubscriptionState::Pending(ref mut q) => {
                                let (send, recv) = oneshot::channel();
                                q.push(send);
                                Action::Wait(recv)
                            }
                        };
                    match action {
                        action @ Action::Wait(_) | action @ Action::Subscribed(_) =>
                            action,
                        action @ Action::Subscribe(_) => {
                            *e.get_mut() = SubscriptionState::Pending(Vec::new());
                            action
                        },
                    }
                },
            }
        };
        match action {
            Action::Subscribed(ut) => Ok(Subscription::new(ut)),
            Action::Wait(rx) => Ok(Subscription::new(await!(rx)??)),
            Action::Subscribe(resolver) => {
                let mut addrs =
                    match await!(resolver.resolve(path.clone())) {
                        Ok(addrs) => addrs,
                        Err(e) => bail!(self.fail_pending(&path, e))
                    };
                let mut last_error = None;
                while let Some(addr) = choose_address(&mut addrs) {
                    match await!(self.clone().subscribe_addr(addr, path.clone())) {
                        Err(e) => last_error = Some(e),
                        Ok(s) => {
                            let mut t = self.0.write().unwrap();
                            match t.subscriptions.get_mut(&path) {
                                None | Some(SubscriptionState::Subscribed(_)) =>
                                    unreachable!("bug"),
                                Some(SubscriptionState::Pending(ref mut q)) =>
                                    for c in q.drain(0..) {
                                        let _ = c.send(Ok(s.untyped.clone()));
                                    },
                            }
                            *t.subscriptions.get_mut(&path).unwrap() =
                                SubscriptionState::Subscribed(s.untyped.downgrade());
                            return Ok(s)
                        }
                    }
                }
                let e = last_error.unwrap_or(
                    Error::from(ErrorKind::PathNotFound(path.clone()))
                );
                bail!(self.fail_pending(&path, e))
            }
        }
    }

    #[async]
    fn initiate_connection(self, addr: SocketAddr) -> Result<Connection> {
        match await!(Connection::new(addr, self.clone())) {
            Err(err) => {
                let mut t = self.0.write().unwrap();
                match t.connections.entry(addr) {
                    Entry::Vacant(_) => unreachable!("bug"),
                    Entry::Occupied(mut e) => {
                        {
                            let q = get_pending(&mut e);
                            for s in q.drain(0..) {
                                let _ = s.send(Err(Error::from(err.to_string())));
                            }
                        }
                        e.remove();
                        bail!(err)
                    }
                }
            },
            Ok((reader, con)) => {
                spawn(start_connection(self.downgrade(), reader, con.downgrade()));
                let mut t = self.0.write().unwrap();
                match t.connections.entry(addr) {
                    Entry::Vacant(_) => unreachable!("bug"),
                    Entry::Occupied(mut e) => {
                        {
                            let q = get_pending(&mut e);
                            for s in q.drain(0..) { let _ = s.send(Ok(con.clone())); }
                        }
                        *e.get_mut() = ConnectionState::Connected(con.downgrade());
                        Ok(con)
                    },
                }
            }
        }
    }

    #[async]
    fn subscribe_addr<T: DeserializeOwned>(
        self,
        addr: SocketAddr,
        path: Path
    ) -> Result<Subscription<T>> {
        enum Action {
            Connected(Connection),
            Wait(oneshot::Receiver<Result<Connection>>),
            Connect
        }
        let action = {
            let mut t = self.0.write().unwrap();
            match t.connections.entry(addr) {
                Entry::Occupied(mut e) => {
                    let action = 
                        match e.get_mut() {
                            ConnectionState::Connected(ref con) =>
                                match con.upgrade() {
                                    Some(con) => Action::Connected(con),
                                    None => Action::Connect,
                                },
                            ConnectionState::Pending(ref mut q) => {
                                let (send, recv) = oneshot::channel();
                                q.push(send);
                                Action::Wait(recv)
                            },
                        };
                    match action {
                        a @ Action::Wait(_) | a @ Action::Connected(_) => a,
                        Action::Connect => { e.remove(); Action::Connect },
                    }
                },
                Entry::Vacant(e) => {
                    e.insert(ConnectionState::Pending(vec![]));
                    Action::Connect
                },
            }
        };
        let con =
            match action {
                Action::Connected(con) => con,
                Action::Wait(rx) => await!(rx)??,
                Action::Connect => await!(self.clone().initiate_connection(addr))?
            };
        let (send, recv) = oneshot::channel();
        let msg = serde_json::to_vec(&ToPublisher::Subscribe(path.clone()))?;
        {
            let mut c = con.0.write().unwrap();
            c.pending.insert(path, send);
            c.writer.write_one(msg);
            c.writer.flush_nowait();
        }
        let ut = await!(recv)??;
        Ok(Subscription::new(ut))
    }
}

fn process_batch(
    t: &SubscriberWeak,
    con: &ConnectionWeak,
    msgs: &mut HashMap<Path, Vec<Arc<String>>>,
    novalue: &mut Vec<Path>,
    unsubscribed: &mut Vec<Path>,
    holds: &mut Vec<oneshot::Receiver<()>>,
    strms: &mut Vec<UntypedUpdates>,
    nexts: &mut Vec<oneshot::Sender<Result<()>>>
) -> Result<()> {
    let con = con.upgrade().ok_or_else(|| Error::from("connection closed"))?;
    // process msgs
    for (path, updates) in msgs.iter_mut() {
        let ut = con.0.read().unwrap().subscriptions.get(path).map(|ut| ut.clone());
        match ut {
            Some(ref ut) => {
                if let Some(ut) = ut.upgrade() {
                    {
                        let mut ut = ut.0.write().unwrap();
                        if let Some(ref up) = updates.last() {
                            ut.current = Arc::clone(up);
                        }
                        let mut i = 0;
                        while i < ut.streams.len() {
                            match ut.streams[i].upgrade() {
                                None => { ut.streams.remove(i); },
                                Some(s) => {
                                    i += 1;
                                    strms.push(s)
                                }
                            }
                        }
                        for next in ut.nexts.drain(0..) { nexts.push(next); }
                    }
                    for next in nexts.drain(0..) { let _ = next.send(Ok(())); }
                    for strm in strms.drain(0..) {
                        let mut strm = strm.0.write().unwrap();
                        strm.ready.extend(updates.iter().map(|v| Some(v.clone())));
                        if strm.ready.len() > strm.max_q {
                            let (tx, rx) = oneshot::channel();
                            strm.hold = Some(tx);
                            holds.push(rx)
                        }
                        if let Some(ref notify) = strm.notify { (notify)(); }
                        strm.notify = None;
                    }
                    updates.clear();
                }
            },
            None => {
                if let Some(ref up) = updates.last() {
                    let t = t.upgrade().ok_or_else(|| Error::from("subscriber dropped"))?;
                    let ut = UntypedSubscription::new(
                        path.clone(), Arc::clone(up), con.clone(), t
                    );
                    let mut c = con.0.write().unwrap();
                    match c.pending.remove(path) {
                        None => bail!("unsolicited"),
                        Some(chan) => {
                            c.subscriptions.insert(path.clone(), ut.downgrade());
                            chan.send(Ok(ut)).map_err(|_| Error::from("ipc err"))?;
                        }
                    }
                }
            },
        }
    }
    // process no value control msgs
    for path in novalue.drain(0..) {
        msgs.remove(&path);
        let p = {
            let mut con = con.0.write().unwrap();
            con.pending.remove(&path)
        };
        match p {
            None => bail!("unsolicited"),
            Some(chan) =>
                chan.send(Err(Error::from(ErrorKind::PathNotFound(path))))
                .map_err(|_| Error::from("ipc err"))?
        }
    }
    // process unsubscribed control msgs
    for path in unsubscribed.drain(0..) {
        msgs.remove(&path);
        let sub = {
            let mut con = con.0.write().unwrap();
            con.subscriptions.remove(&path)
        };
        if let Some(ref ut) = sub {
            if let Some(ut) = ut.upgrade() { ut.0.write().unwrap().unsubscribe() }
        }
    }
    Ok(())
}


/// This is only public because of rustc/#50865, it will be private
/// when that bug is fixed
#[async]
pub fn connection_loop(
    t: SubscriberWeak,
    reader: ReadHalf<TcpStream>,
    con: ConnectionWeak
) -> Result<()> {
    let mut msg_pending = None;
    let mut msgs : HashMap<Path, Vec<Arc<String>>> = HashMap::new();
    let mut novalue : Vec<Path> = Vec::new();
    let mut unsubscribed: Vec<Path> = Vec::new();
    let mut holds : Vec<oneshot::Receiver<()>> = Vec::new();
    let mut strms : Vec<UntypedUpdates> = Vec::new();
    let mut nexts : Vec<oneshot::Sender<Result<()>>> = Vec::new();
    let batched = {
        let lines = tokio::io::lines(BufReader::new(reader)).map_err(|e| Error::from(e));
        utils::batched(lines, 1000000)
    };
    #[async]
    for item in batched {
        match item {
            BatchItem::InBatch(line) => {
                match msg_pending {
                    Some(path) => {
                        msg_pending = None;
                        msgs.entry(path).or_insert(Vec::new()).push(Arc::new(line))
                    },
                    None => {
                        match serde_json::from_str::<FromPublisher>(line.as_ref())? {
                            FromPublisher::Message(path) => msg_pending = Some(path),
                            FromPublisher::NoSuchValue(path) => novalue.push(path),
                            FromPublisher::Unsubscribed(path) => unsubscribed.push(path),
                        }
                    },
                }
            },
            BatchItem::EndBatch => {
                process_batch(
                    &t, &con, &mut msgs, &mut novalue, &mut unsubscribed,
                    &mut holds, &mut strms, &mut nexts
                )?;
                while let Some(hold) = holds.pop() {
                    let _ = await!(hold);
                }
            }
        }
    }
    Ok(())
}

#[async]
fn start_connection(
    t: SubscriberWeak,
    reader: ReadHalf<TcpStream>,
    con: ConnectionWeak
) -> result::Result<(), ()> {
    let _ = await!(connection_loop(t.clone(), reader, con.clone()));
    if let Some(con) = con.upgrade() {
        let c = con.0.read().unwrap();
        c.writer.shutdown();
        for (_, s) in c.subscriptions.iter() {
            if let Some(s) = s.upgrade() {
                s.0.write().unwrap().unsubscribe()
            }
        }
    }
    Ok(())
}
