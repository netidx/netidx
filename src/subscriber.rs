
enum ToConnection {
    Subscribe {
        path: Path,
        finished: oneshot::Sender<Result<RawSubscription, Error>>,
        timeout: Option<Duration>,
        con: Sender<ToConnection>,
    },
    Unsubscribe(Id),
    Stream(Id, Sender<Bytes>),
    NotifyDead(Id, oneshot::Sender<()>),
}

struct RawSubscriptionInner {
    id: Id,
    last: Bytes,
    connection: Option<Sender<ToConnection>>,
}

impl Drop for RawSubscriptionInner {
    fn drop(&mut self) {
        if let Some(c) = self.connection {
            task::spawn({
                let c = c.clone();
                let id = self.id;
                async move { c.send(ToConnection::Unsubscribe(id)).await; }
            });
        }
    }
}

#[derive(Clone)]
struct RawSubscriptionWeak(Weak<RwLock<RawSubscriptionInner>>);

impl RawSubscriptionWeak {
    fn upgrade(&self) -> Option<RawSubscription> {
        Weak::upgrade(&self.0).map(|r| RawSubscription(r))
    }
}

#[derive(Clone)]
pub struct RawSubscription(Arc<RwLock<RawSubscriptionInner>>);

impl RawSubscription {
    fn typed<T: DeserializeOwned>(self) -> Subscription {
        Subscription(self, PhantomData)
    }

    fn downgrade(&self) -> UntypedSubscriptionWeak {
        UntypedSubscriptionWeak(Arc::downgrade(self.0))
    }

    pub fn last(&self) -> Bytes {
        let t = self.0.read();
        t.last.clone()
    }

    pub fn with_last<T, F: FnOnce(&[u8]) -> T>(&self, f: F) -> T {
        let t = self.0.read();
        f(&*t.last)
    }

    pub fn is_dead(&self) -> bool {
        let t = self.0.read();
        t.connection.is_none()
    }

    pub async fn dead(&self) -> Error {
        let mut c = {
            let t = self.0.read();
            match t.connection {
                None => return format_err!("already dead"),
                Some(c) => c.clone()
            }
        };
        let (tx, rx) = oneshot::channel();
        c.send(ToConnection::OnDead(tx)).await;
        rx.await.unwrap_or_else(|| format_err!("connection died"))
    }

    pub fn updates(&self) -> impl Stream<Item = Bytes> {
        let (tx, rx) = channel(100);
        let t = self.0.read();
        if let Some(c) = t.connection {
            let id = t.id;
            let c = c.clone();
            task::spawn(async move { c.send(ToConnection::Stream(id, tx)) });
        }
        rx
    }
}

#[derive(Clone)]
pub struct Subscription<T: DeserializeOwned>(RawSubscription, PhantomData<T>);

impl Subscription {
    pub fn last(&self) -> Result<T, rmp_serde::decode::Error> {
        self.0.with_last(|v| rmp_serde::decode::from_read::<T>(v))
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
    connections: HashMap<SocketAddr, Sender<ToConnection>, FxBuildHasher>,
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

    async fn subscribe(
        &self,
        batch: impl IntoIterator<Item = Path>,
        timeout: Option<Duration>,
    ) -> Vec<(Path, Result<RawSubscription, Error>)> {
        use std::collections::hash_map::Entry;
        enum St {
            Resolve,
            Resolved(Vec<SocketAddrs>),
            Connected(Sender<ToConnection>),
            Subscribing(Option<oneshot::Receiver<Result<RawSubscription, Error>>>),
            Subscribed(RawSubscription),
            Error(Error),
        }
        let mut pending: HashMap<Path, St> = HashMap::new();
        let r = { // Init
            let mut t = self.0.lock();
            for p in batch {
                match t.subscribed.entry(p.clone()) {
                    Entry::Vacant(e) => {
                        e.insert(SubStatus::Pending(vec![]));
                        pending.insert(p, St::Resolve);
                        true
                    }
                    Entry::Occupied(e) => match e.get_mut() {
                        SubStatus::Subscribed(r) => {
                            pending.insert(p, St::Subscribed(r.clone()));
                            false
                        },
                        SubStatus::Pending(ref mut v) => {
                            let (tx, rx) = oneshot::channel();
                            v.push(tx);
                            pending.insert(p, St::Subscribing(Some(rx)));
                            false
                        }
                    }
                }
            }
            t.resolver.clone();
        };
        { // Resolve
            let to_resolve =
                pending.iter()
                .filter(|(_, s)| s == St::Resolve)
                .map(|(p, _)| p.clone())
                .collect::<Vec<_>>();
            match r.resolve(to_resolve.clone()).await {
                Ok(addrs) => 
                    for (p, addrs) in to_resolve.into_iter().zip(addrs.into_iter()) {
                        *(pending.get_mut(&p).unwrap()) = St::Resolved(addrs);
                    },
                Err(e) =>
                    for p in to_resolve {
                        *(pending.get_mut(&p).unwrap()) = St::Error(
                            format_err!("resolving path: {} failed: {}", p, e)
                        );
                    }
            }
        }
        { // Connect
            let mut rng = rand::thread_rng();
            let mut t = t.0.lock();
            for (path, st) in pending.iter_mut() {
                match st {
                    St::Resolve
                        | St::Connected(_)
                        | St::Subscribing(_)
                        | St::Subscribed(_)
                        | St::Error(_) => (),
                    St::Resolved(addrs) => {
                        if addrs.len() == 0 {
                            *st = St::Error(format_error!("path not found"));
                        } else {
                            let addr = {
                                if addrs.len() == 1 {
                                    addrs[0];
                                } else {
                                    addrs[rng.gen_range(0, addrs.len())]
                                }
                            };
                            let con = t.connections.entry(addr).or_insert_with(|| {
                                start_connection(addr)
                            });
                            *st = St::Connected(con.clone());
                        }
                    }
                }
            }
        }
        // Subscribe
        for (path, st) in pending.iter_mut() {
            match st {
                St::Resolve
                    | St::Resolved(_)
                    | St::Subscribing(_)
                    | St::Subscribed(_)
                    | St::Error(_) => (),
                St::Connected(con) => {
                    let con = con.clone();
                    let (tx, rx) = oneshot::channel();
                    *st = St::Subscribing(Some(rx));
                    con.send(ToConnection::Subscribe {
                        timeout, con,
                        path: path.clone(),
                        finished: tx,
                    }).await;                    
                }
            }
        }
        // wait
        for (path, st) in pending.iter_mut() {
            match st {
                St::Resolve
                    | St::Resolved(_)
                    | St::Connected(_) =>
                    *st = St::Error(format_err!("bug: invalid state")),
                St::Subscribed(_) => (),
                St::Subscribing(wait) => {
                    if let Some(wait) = mem::replace(wait, None) {
                        match reply.await {
                            Err(_) => *st = St::Error(format_err!("connection died")),
                            Ok(Err(e)) => *st = St::Error(e),
                            Ok(Ok(raw)) => *st = St::Subscribed(raw),
                        }
                    }
                }
            }
        }
    }
}
