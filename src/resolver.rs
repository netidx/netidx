pub use crate::resolver_single::{Auth, ResolverError};
use crate::{
    os::ClientCtx,
    path::Path,
    protocol::resolver::v1::{FromRead, FromWrite, Referral, ToRead, ToWrite},
    resolver_single::{
        FromReadBatch as IFromReadBatch, FromWriteBatch as IFromWriteBatch,
        ResolverRead as SingleRead, ResolverWrite as SingleWrite,
        ToReadBatch as IToReadBatch, ToWriteBatch as IToWriteBatch,
    },
    utils::Batch,
};
use anyhow::Result;
use futures::future;
use fxhash::FxBuildHasher;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{
        BTreeMap,
        Bound::{self, Included, Unbounded},
        HashMap,
    },
    marker::PhantomData,
    mem,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::{Deref, DerefMut},
    result,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::oneshot, time::Instant};

const MAX_REFERRALS: usize = 128;

trait ToPath {
    fn path(&self) -> Option<&Path>;
}

impl ToPath for ToRead {
    fn path(&self) -> Option<&Path> {
        match self {
            ToRead::List(p) | ToRead::Resolve(p) => Some(p),
        }
    }
}

impl ToPath for ToWrite {
    fn path(&self) -> Option<&Path> {
        match self {
            ToWrite::Clear | ToWrite::Heartbeat => None,
            ToWrite::Publish(p) | ToWrite::Unpublish(p) | ToWrite::PublishDefault(p) => {
                Some(p)
            }
        }
    }
}

#[derive(Debug)]
struct Router {
    default: Referral,
    cached: BTreeMap<Path, (Instant, Referral)>,
}

impl Router {
    fn new(default: Referral) -> Self {
        Router { default, cached: BTreeMap::new() }
    }

    fn route_batch<B: Batch<T>, O: Batch<(usize, T)>, T: ToPath + Clone>(
        &mut self,
        batch: &B,
    ) -> Vec<(Option<Referral>, O)> {
        let now = Instant::now();
        let mut batches = HashMap::new();
        let mut gc = Vec::new();
        let mut id = 0;
        for v in batch.iter() {
            let v = v.clone();
            match v.path() {
                None => batches.entry(None).or_insert_with(O::new).push((id, v)),
                Some(path) => {
                    let mut r = self.cached.range::<str, (Bound<&str>, Bound<&str>)>((
                        Unbounded,
                        Included(&*path),
                    ));
                    match r.next_back() {
                        None => batches.entry(None).or_insert_with(O::new).push((id, v)),
                        Some((p, (exp, _))) => {
                            if !path.starts_with(p.as_ref()) {
                                batches.entry(None).or_insert_with(O::new).push((id, v))
                            } else {
                                if &now < exp {
                                    batches
                                        .entry(Some(p.clone()))
                                        .or_insert_with(O::new)
                                        .push((id, v))
                                } else {
                                    gc.push(p.clone());
                                    batches
                                        .entry(None)
                                        .or_insert_with(O::new)
                                        .push((id, v))
                                }
                            }
                        }
                    }
                }
            }
            id += 1;
        }
        for p in gc {
            self.cached.remove(p.as_ref());
        }
        batches
            .into_iter()
            .map(|(p, batch)| match p {
                None => (None, batch),
                Some(p) => (Some(self.cached[p.as_ref()].1.clone()), batch),
            })
            .collect()
    }

    fn add_referral(&mut self, r: Referral) {
        let exp = Instant::now() + Duration::from_secs(r.ttl);
        self.cached.insert(r.path.clone(), (exp, r));
    }
}

trait ToReferral: Sized {
    fn referral(self) -> result::Result<Referral, Self>;
}

impl ToReferral for FromRead {
    fn referral(self) -> result::Result<Referral, Self> {
        match self {
            FromRead::Referral(r) => Ok(r),
            m => Err(m),
        }
    }
}

impl ToReferral for FromWrite {
    fn referral(self) -> result::Result<Referral, Self> {
        match self {
            FromWrite::Referral(r) => Ok(r),
            m => Err(m),
        }
    }
}

trait Connection<T, F, B, O>
where
    T: ToPath,
    F: ToReferral,
    B: Batch<(usize, T)>,
    O: Batch<(usize, F)>,
{
    fn new(
        resolver: Referral,
        desired_auth: Auth,
        writer_addr: SocketAddr,
        ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
    ) -> Self;
    fn send(&mut self, batch: B) -> oneshot::Receiver<O>;
}

impl Connection<ToRead, FromRead, IToReadBatch, IFromReadBatch> for SingleRead {
    fn new(
        resolver: Referral,
        desired_auth: Auth,
        _writer_addr: SocketAddr,
        _ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
    ) -> Self {
        SingleRead::new(resolver, desired_auth)
    }

    fn send(&mut self, batch: IToReadBatch) -> oneshot::Receiver<IFromReadBatch> {
        self.send(batch)
    }
}

impl Connection<ToWrite, FromWrite, IToWriteBatch, IFromWriteBatch> for SingleWrite {
    fn new(
        resolver: Referral,
        desired_auth: Auth,
        writer_addr: SocketAddr,
        ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
    ) -> Self {
        SingleWrite::new(resolver, desired_auth, writer_addr, ctxts)
    }

    fn send(&mut self, batch: IToWriteBatch) -> oneshot::Receiver<IFromWriteBatch> {
        self.send(batch)
    }
}

make_pool!(pub, TOREADPOOL, ToReadBatch, ToRead, 1000);
make_pool!(pub, FROMREADPOOL, FromReadBatch, FromRead, 1000);
make_pool!(pub, TOWRITEPOOL, ToWriteBatch, ToWrite, 1000);
make_pool!(pub, FROMWRITEPOOL, FromWriteBatch, FromWrite, 1000);

#[derive(Debug)]
struct ResolverWrapInner<C, T, F, B, O, Bi, Oi> {
    router: Router,
    desired_auth: Auth,
    default: C,
    by_path: HashMap<Path, C>,
    writer_addr: SocketAddr,
    ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
    phantom: PhantomData<(T, F, B, O, Bi, Oi)>,
}

#[derive(Debug, Clone)]
struct ResolverWrap<C, T, F, B, O, Bi, Oi>(
    Arc<Mutex<ResolverWrapInner<C, T, F, B, O, Bi, Oi>>>,
);

impl<C, T, F, B, O, Bi, Oi> ResolverWrap<C, T, F, B, O, Bi, Oi>
where
    C: Connection<T, F, Bi, Oi> + Clone + 'static,
    T: ToPath + Clone + 'static,
    F: ToReferral + Clone + 'static,
    B: Batch<T> + 'static,
    O: Batch<F> + 'static,
    Bi: Batch<(usize, T)> + 'static,
    Oi: Batch<(usize, F)> + 'static,
{
    fn new(
        default: Referral,
        desired_auth: Auth,
        writer_addr: SocketAddr,
    ) -> ResolverWrap<C, T, F, B, O, Bi, Oi> {
        let ctxts = Arc::new(RwLock::new(HashMap::with_hasher(FxBuildHasher::default())));
        let router = Router::new(default.clone());
        let default = C::new(default, desired_auth.clone(), writer_addr, ctxts.clone());
        ResolverWrap(Arc::new(Mutex::new(ResolverWrapInner {
            router,
            desired_auth,
            default,
            by_path: HashMap::new(),
            writer_addr,
            ctxts,
            phantom: PhantomData,
        })))
    }

    fn ctxts(&self) -> Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>> {
        self.0.lock().ctxts.clone()
    }

    async fn send(&self, batch: &B) -> Result<O> {
        let mut referrals = 0;
        loop {
            let mut waiters = Vec::new();
            {
                let mut guard = self.0.lock();
                let inner = &mut *guard;
                for (r, batch) in inner.router.route_batch(batch).into_iter() {
                    match r {
                        None => waiters.push(inner.default.send(batch)),
                        Some(r) => match inner.by_path.get_mut(&r.path) {
                            Some(con) => waiters.push(con.send(batch)),
                            None => {
                                let path = r.path.clone();
                                let mut con = C::new(
                                    r.clone(),
                                    inner.desired_auth.clone(),
                                    inner.writer_addr,
                                    inner.ctxts.clone(),
                                );
                                inner.by_path.insert(path, con.clone());
                                waiters.push(con.send(batch))
                            }
                        },
                    }
                }
            }
            let res = future::join_all(waiters).await;
            let mut finished = Oi::new();
            let mut referral = false;
            for r in res {
                let mut r = r?;
                for (id, reply) in r.drain(..) {
                    match reply.referral() {
                        Ok(r) => {
                            self.0.lock().router.add_referral(r);
                            referral = true;
                        }
                        Err(m) => finished.push((id, m)),
                    }
                }
            }
            if !referral {
                let mut res = O::new();
                finished.sort_by_key(|(id, _)| *id);
                res.extend(finished.drain(..).map(|(_, m)| m));
                break Ok(res);
            }
            referrals += 1;
            if referrals > MAX_REFERRALS {
                bail!("maximum referral depth {} reached, giving up", MAX_REFERRALS);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolverRead(
    ResolverWrap<
        SingleRead,
        ToRead,
        FromRead,
        ToReadBatch,
        FromReadBatch,
        IToReadBatch,
        IFromReadBatch,
    >,
);

impl ResolverRead {
    fn new(default: Referral, desired_auth: Auth) -> Self {
        ResolverRead(ResolverWrap::new(
            default,
            desired_auth,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
        ))
    }

    async fn send(&self, batch: &ToReadBatch) -> Result<FromReadBatch> {
        self.0.send(batch).await
    }
}

#[derive(Debug, Clone)]
pub struct ResolverWrite(
    ResolverWrap<
        SingleWrite,
        ToWrite,
        FromWrite,
        ToWriteBatch,
        FromWriteBatch,
        IToWriteBatch,
        IFromWriteBatch,
    >,
);

impl ResolverWrite {
    fn new(default: Referral, desired_auth: Auth, writer_addr: SocketAddr) -> Self {
        ResolverWrite(ResolverWrap::new(default, desired_auth, writer_addr))
    }

    async fn send(&self, batch: &ToWriteBatch) -> Result<FromWriteBatch> {
        self.0.send(batch).await
    }

    fn ctxts(&self) -> Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>> {
        self.0.ctxts()
    }
}
