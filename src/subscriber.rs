
struct SubscribeRequest {
    path: Path,
    finished: oneshot::Sender<Result<RawSubscription, Error>>,
    con: UnboundedSender<ToConnection>,
}

enum ToCon {
    Subscribe(SubscribeRequest),
    Unsubscribe(Id),
    Stream(Id, Sender<Bytes>),
    NotifyDead(Id, oneshot::Sender<()>),
}

struct RawSubscriptionInner {
    id: Id,
    addr: SocketAddr,
    last: Arc<RwLock<Bytes>>,
    dead: Arc<AtomicBool>,
    connection: UnboundedSender<ToCon>,
}

impl Drop for RawSubscriptionInner {
    fn drop(&mut self) {
        let _ = self.con.unbounded_send(ToCon::Unsubscribe(id));
    }
}

#[derive(Clone)]
struct RawSubscriptionWeak(Weak<RawSubscriptionInner>);

impl RawSubscriptionWeak {
    fn upgrade(&self) -> Option<RawSubscription> {
        Weak::upgrade(&self.0).map(|r| RawSubscription(r))
    }
}

#[derive(Clone)]
pub struct RawSubscription(Arc<RawSubscriptionInner>);

impl RawSubscription {
    fn typed<T: DeserializeOwned>(self) -> Subscription {
        Subscription(self, PhantomData)
    }

    fn downgrade(&self) -> UntypedSubscriptionWeak {
        RawSubscriptionWeak(Arc::downgrade(self.0))
    }

    pub fn last(&self) -> Bytes {
        self.0.last.read().clone()
    }

    pub fn is_dead(&self) -> bool {
        self.0.dead.load(Ordering::Relaxed)
    }

    pub async fn dead(&self) -> Error {
        let mut c = {
            match self.0.connection.lock() {
                None => return format_err!("already dead"),
                Some(c) => c.clone()
            }
        };
        let (tx, rx) = oneshot::channel();
        match c.send(ToCon::OnDead(tx)).await {
            Err(e) => Error::from(e),
            Ok(()) =>
                rx.await.unwrap_or_else(|| format_err!("connection died"))
        }
    }

    pub fn updates(&self) -> impl Stream<Item = Bytes> {
        let (tx, rx) = channel(100);
        if let Some(c) = &*self.0.connection.lock() {
            let id = t.id;
            let c = c.clone();
            task::spawn(async move {
                let _ = c.send(ToCon::Stream(id, tx)).await;
            });
        }
        rx
    }
}

#[derive(Clone)]
pub struct Subscription<T: DeserializeOwned>(RawSubscription, PhantomData<T>);

impl Subscription {
    pub fn last(&self) -> Result<T, rmp_serde::decode::Error> {
        rmp_serde::decode::from_read::<T>(&*self.0.last())
    }

    pub fn is_dead(&self) -> bool {
        self.0.is_dead()
    }

    pub async fn dead(&self) -> Error {
        self.0.dead()
    }

    pub fn updates(&self) -> impl Stream<Item = Result<T, rmp_serde::decode::Error>> {
        self.0.updates().map(|v| rmp_serde::decode::from_read::<T>(&*v))
    }
}

enum SubStatus {
    Subscribed(RawSubscriptionWeak),
    Pending(Vec<oneshot::Sender<Result<RawSubscription, Error>>>),
}

struct SubscriberInner {
    resolver: Resolver<ReadOnly>,
    connections: HashMap<SocketAddr, UnboundedSender<ToCon>, FxBuildHasher>,
    subscribed: HashMap<Path, SubStatus>,
}

pub struct Subscriber(Arc<Mutex<SubscriberInner>>);

impl Subscriber {
    fn new<T: ToSocketAddrs>(addrs: T) -> Result<Subscriber, Error> {
        Ok(Arc::new(Mutex::new(Subscriber {
            resolver: Resolver::new_ro(addrs)?,
            connection: HashMap::with_hasher(FxBuildHasher::default()),
            subscribed: HashMap::new(),
        })))
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
    async fn subscribe(
        &self, batch: impl IntoIterator<Item = Path>,
    ) -> Vec<(Path, Result<RawSubscription, Error>)> {
        use std::collections::hash_map::Entry;
        enum St {
            Resolve,
            Subscribing(oneshot::Receiver<Result<RawSubscription, Error>>),
            WaitingOther(oneshot::Receiver<Result<RawSubscription, Error>>),
            Subscribed(RawSubscription),
            Error(Error),
        }
        let paths = batch.into_iter().collect::<Vec<_>>();
        let mut pending: HashMap<Path, St> = HashMap::new();
        let r = { // Init
            let mut t = self.0.lock();
            for p in paths.clone() {
                match t.subscribed.entry(p.clone()) {
                    Entry::Vacant(e) => {
                        e.insert(SubStatus::Pending(vec![]));
                        pending.insert(p, St::Resolve);
                    }
                    Entry::Occupied(e) => match e.get_mut() {
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
        { // Resolve, Connect, Subscribe
            let mut rng = rand::thread_rng();
            let to_resolve =
                pending.iter()
                .filter(|(_, s)| s == St::Resolve)
                .map(|(p, _)| p.clone())
                .collect::<Vec<_>>();
            match r.resolve(to_resolve.clone()).await {
                Err(e) => for p in to_resolve {
                    *pending.[&p] = St::Error(
                        format_err!("resolving path: {} failed: {}", p, e)
                    );
                }
                Ok(addrs) => {
                    let mut t = t.0.lock();
                    for (p, addrs) in to_resolve.into_iter().zip(addrs.into_iter()) {
                        if addrs.len() == 0 {
                            *pending[&p] = St::Error(format_error!("path not found"));
                        } else {
                            let addr = {
                                if addrs.len() == 1 {
                                    addrs[0];
                                } else {
                                    addrs[rng.gen_range(0, addrs.len())]
                                }
                            };
                            let con =
                                t.connections.entry(addr)
                                .or_insert_with(|| {
                                    let (tx, rx) = mpsc::unbounded();
                                    task::spawn(connection(self.clone(), addr, rx));
                                    tx
                                });
                            let (tx, rx) = oneshot::channel();
                            let con_ = con.clone();
                            let r = con.send_unbounded(ToCon::Subscribe {
                                con: con_,
                                path: p.clone(),
                                finished: tx,
                            });
                            match r {
                                Ok(()) => { *pending[&p] = St::Subscribing(rx); }
                                Err(e) => { *pending[&p] = St::Error(Error::from(e)); }
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
                    let sub = t.subscribed.get_mut(&path).unwrap();
                    match t.subscribed.entry(path.clone()) {
                        Entry::Vacant(_) => unreachable!(),
                        Entry::Occupied(e) => match res {
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
                                    SubStatus::Subscribed(raw.clone())
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
            St::Resolve | St::Connected(_) | St::Subscribing(_)
                | St::WaitingOther(_) => unreachable!(),
            St::Subscribed(raw) => (p, Ok(raw)),
            St::Error(e) => (p, Err(e))
        }).collect()
    }
}

struct Sub {
    path: Path,
    streams: SmallVec<[Sender<Bytes>; 4]>,
    deads: SmallVec<[oneshot::Sender<()>; 4]>,
    last: Arc<RwLock<Bytes>>,
    dead: Arc<AtomicBool>,
}

async fn handle_val(
    pending: &mut HashMap<Path, SubscribeRequest>,
    subscriptions: &mut HashMap<Id, Sub, FxBuildHasher>,
    next_sub: &mut Option<SubscribeRequest>,
    id: Id,
    msg: Bytes,
) {
    use std::collections::hash_map::Entry;
    match subscriptions.entry(id) {
        Entry::Occupied(e) => {
            let sub = e.get_mut();
            let mut i = 0;
            while i < sub.streams.len() {
                match sub.streams[i].send(msg.clone()).await {
                    Ok(()) => { i += 1; }
                    Err(_) => { sub.streams.remove(i); }
                }
            }
            *sub.last.write() = msg;
        }
        Entry::Vacant(e) => if let Some(req) = next_sub.take() {
            let last = Arc::new(RwLock::new(msg));
            let dead = Arc::new(AtomicBool::new(false));
            e.insert(Sub {
                last: last.clone(),
                dead: dead.clone(),
                deads: SmallVec::new();
                streams: SmallVec::new();
            });
            let s = RawSubscriptionInner { id, last, dead, connection: req.con };
            let _ = req.finished.send(RawSubscription(Arc::new(s)));
        }
    }
}

fn unsubscribe(
    sub: Sub,
    id: Id,
    addr: &SocketAddr,
    subscribed: &mut HashMap<Path, SubStatus>
) {
    sub.dead.store(true, Ordering::Relaxed);
    match subscribed.entry(sub.path) {
        Entry::Vacant(_) => (),
        Entry::Occupied(e) => match e.get() {
            SubStatus::Pending(_) => (),
            SubStatus::Subscribed(s) => match s.upgrade() {
                None => { e.remove(); }
                Some(s) => if s.id == id && &s.addr == addr { e.remove(); }
            }
        }
    }
}

fn handle_control(
    addr: &SocketAddr,
    subscriber: &Subscriber,
    pending: &mut HashMap<Path, SubscribeRequest>,
    subscriptions: &mut HashMap<Id, Sub, FxBuildHasher>,
    next_val: &mut Option<Id>,
    next_sub: &mut Option<SubscribeRequest>,
    msg: &[u8]
) -> Result<(), Error> {
    match rmp_serde::decode::from_read::<FromPublisher>(&*msg) {
        Err(e) => return Err(Error::from(e)),
        Ok(Message(id)) => { *next_val = Some(id); }
        Ok(FromPublisher::NoSuchValue(path)) =>
            if let Some(r) = pending.remove(&path) {
                let _ = r.finished.send(Err(format_err!("no such value")));
            }
        Ok(FromPublisher::Subscribed(path, id)) => match pending.remove(&path) {
            None => return Err(format_err!("unsolicited: {}", path)),
            Some(req) => {
                *next_id = Some(id);
                *next_sub = Some(req);
            }
        }
        Ok(FromPublisher::Unsubscribed(id)) =>
            if let Some(s) = subscriptions.remove(id) {
                let mut t = subscriber.lock();
                unsubscribe(s, id, addr, &mut t.subscribed);
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
    mut from_sub: Receiver<ToCon>
) -> Result<(), Error> {
    enum M {
        FromPub(Option<Result<Bytes, io::Error>>),
        FromSub(BatchItem<ToCon>),
    }
    let mut from_sub = Batched::new(from_sub);
    let mut pending: HashMap<Path, SubscribeRequest> = HashMap::new();
    let mut subscriptions: HashMap<Id, Sub, FxBuildHasher> =
        HashMap::with_hasher(FxBuildHasher::default());
    let mut next_val: Option<Id> = None;
    let mut next_sub: Option<SubscribeRequest> = None;
    let mut con = Framed::new(TcpStream::connect(to).await?, LengthCodec);
    let mut batched = Vec::new();
    let mut buf = BytesMut::new();
    let enc = |buf: &mut BytesMut, m: &ToPublisher| {
        rmp_serde::encode::write_named(&mut BytesWriter(&mut **buf), m).map(|()| {
            buf.take().freeze()
        })
    };
    let res = loop {
        let from_pub = con.next().map(|m| M::FromPub(m));
        let from_sub = from_sub.next().map(|m| M::FromSub(m));
        match from_pub.race(from_sub).await {
            M::FromPub(None) => break Err(format_err!("connection closed")),
            M::FromPub(Some(Err(e))) => break Err(Error::from(e)),
            M::FromPub(Some(Ok(msg))) => match next_val.take() {
                Some(id) => {
                    handle_val(
                        &mut pending, &mut subscriptions, &mut next_sub, id, msg
                    ).await;
                }
                None => {
                    try_brk!(handle_control(
                        &to, &subscriber, &mut pending, &mut subscriptions,
                        &mut next_id, &mut next_sub, msg
                    ));
                }
            }
            M::FromSub(BatchItem::InBatch(ToCon::Subscribe(req))) => {
                let path = req.path.clone();
                pending.insert(path.clone(), req);
                batched.push(try_brk!(enc(&mut buf, &ToPublisher::Subscribe(path))));
            }
            M::FromSub(BatchItem::InBatch(ToCon::Unsubscribe(id))) => {
                batched.push(try_brk!(enc(&mut buf, &ToPublisher::Unsubscribe(id))));
            }
            M::FromSub(BatchItem::InBatch(ToCon::Stream(id, tx))) => {
                if let Some(sub) = subscriptions.get_mut(&id) {
                    sub.streams.push(tx);
                }
            }
            M::FromSub(BatchItem::InBatch(ToCon::NotifyDead(id, tx))) => {
                if let Some(sub) = subscriptions.get_mut(&id) {
                    sub.deads.push(tx);
                }
            }
            M::FromSub(BatchItem::EndBatch) => if batched.len() > 0 {
                let mut s = stream::iter(batched.drain(..).map(|v| Ok(v)));
                try_brk!(con.send_all(&mut s).await);
            }
        }
    };
    let mut t = subscriber.lock();
    for (id, sub) in subscriptions {
        unsubscribe(sub, id, &addr, &mut t.subscribed);
    }
    res
}
