
enum ToConnection {
    Subscribe {
        path: Path,
        finished: oneshot::Sender<Result<RawSubscription, Error>>,
        timeout: Option<Duration>
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
    subscribed: HashMap<Path, RawSubscriptionWeak>,
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
        let r = {
            let t = self.0.lock();
            t.resolver.clone();
        }
        let mut rng = rand::thread_rng();
        let paths: Vec<Path> = batch.into_iter().collect();
        let addrs = match r.resolve(paths.clone()).await {
            Ok(addrs) => addrs
            Err(e) => {
                return paths.iter().map(|p| {
                    Err(format_err!("resolving path: {} failed: {}", p, e))
                }).collect::<Vec<_>>()
            }
        };
        let with_con = {
            let mut t = t.0.lock();
            paths.into_iter().zip(addrs.into_iter()).map(|(path, addrs)| {
                if addrs.len() == 0 {
                    (path, Err(format_error!("path not found")))
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
                    (path, Ok(con.clone()))
                }
            }).collect::<Vec<_>>()
        };
        let mut pending = Vec::with_capacity(with_con.len());
        for (path, r) in with_con {
            match r {
                r@ Err(_) => pending.push(r),
                Ok(con) => {
                    let (tx, rx) = oneshot::channel();
                    pending.push((path.clone(), Ok((c.clone(), rx))));
                    con.send(ToConnection::Subscribe {
                        path,
                        finished: tx,
                        timeout
                    }).await;                    
                }
            }
        }
        let mut result = Vec::with_capacity(pending.len());
        for (path, r) in pending {
            match r {
                r@ Err(_) => result.push((path, r)),
                Ok((connection, reply)) => {
                    match reply.await {
                        Err(_) => result.push(Err(format_err!("connection died"))),
                        Ok(Err(e)) => result.push((path, Err(e))),
                        Ok(Ok((id, last))) => {
                            let r = RawSubscriptionInner { id, last, connection };
                            let s = RawSubscription(Arc::new(RwLock::new(r)));
                            result.push((path, Ok(s)))
                        }
                    }
                }
            }
        }
        let mut t = self.0.lock();
        for (path, r) in result.iter() {
            if let Ok(s) = r {
                t.subscriptions.insert
            }
        }
    }
}
