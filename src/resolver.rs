pub use crate::resolver_single::{Auth, ResolverError};
use crate::{
    os::ClientCtx,
    path::Path,
    protocol::resolver::v1::{FromRead, FromWrite, Referral, ToRead, ToWrite},
    resolver_single::{ResolverRead as SingleRead, ResolverWrite as SingleWrite},
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

    fn route_batch<T: ToPath + Clone>(
        &mut self,
        batch: &Vec<T>,
    ) -> Vec<(Option<Referral>, Vec<(usize, T)>)> {
        let now = Instant::now();
        let mut batches = HashMap::new();
        let mut gc = Vec::new();
        let mut id = 0;
        for v in batch {
            let v = v.clone();
            match v.path() {
                None => batches.entry(None).or_insert_with(Vec::new).push((id, v)),
                Some(path) => {
                    let mut r = self.cached.range::<str, (Bound<&str>, Bound<&str>)>((
                        Unbounded,
                        Included(&*path),
                    ));
                    match r.next_back() {
                        None => {
                            batches.entry(None).or_insert_with(Vec::new).push((id, v))
                        }
                        Some((p, (exp, r))) => {
                            if !path.starts_with(p.as_ref()) {
                                batches.entry(None).or_insert_with(Vec::new).push((id, v))
                            } else {
                                if &now < exp {
                                    batches
                                        .entry(Some(p.clone()))
                                        .or_insert_with(Vec::new)
                                        .push((id, v))
                                } else {
                                    gc.push(p.clone());
                                    batches
                                        .entry(None)
                                        .or_insert_with(Vec::new)
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

trait Connection<T, F>
where
    T: ToPath,
    F: ToReferral,
{
    fn new(
        resolver: Referral,
        desired_auth: Auth,
        writer_addr: SocketAddr,
        ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
    ) -> Self;
    fn send(&mut self, batch: Vec<(usize, T)>) -> oneshot::Receiver<Vec<(usize, F)>>;
}

impl Connection<ToRead, FromRead> for SingleRead {
    fn new(
        resolver: Referral,
        desired_auth: Auth,
        _writer_addr: SocketAddr,
        _ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
    ) -> Self {
        SingleRead::new(resolver, desired_auth)
    }

    fn send(
        &mut self,
        batch: Vec<(usize, ToRead)>,
    ) -> oneshot::Receiver<Vec<(usize, FromRead)>> {
        self.send(batch)
    }
}

impl Connection<ToWrite, FromWrite> for SingleWrite {
    fn new(
        resolver: Referral,
        desired_auth: Auth,
        writer_addr: SocketAddr,
        ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
    ) -> Self {
        SingleWrite::new(resolver, desired_auth, writer_addr, ctxts)
    }

    fn send(
        &mut self,
        batch: Vec<(usize, ToWrite)>,
    ) -> oneshot::Receiver<Vec<(usize, FromWrite)>> {
        self.send(batch)
    }
}

#[derive(Debug)]
struct ResolverWrapInner<C> {
    router: Router,
    desired_auth: Auth,
    default: C,
    by_path: HashMap<Path, C>,
    writer_addr: SocketAddr,
    ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
}

#[derive(Debug, Clone)]
struct ResolverWrap<C, T, F>(Arc<Mutex<ResolverWrapInner<C>>>, PhantomData<(T, F)>);

impl<C, T, F> ResolverWrap<C, T, F>
where
    C: Connection<T, F> + Clone + 'static,
    T: ToPath + Clone + 'static,
    F: ToReferral + Clone + 'static,
{
    fn new(
        default: Referral,
        desired_auth: Auth,
        writer_addr: SocketAddr,
    ) -> ResolverWrap<C, T, F> {
        let ctxts = Arc::new(RwLock::new(HashMap::with_hasher(FxBuildHasher::default())));
        let router = Router::new(default.clone());
        let default = C::new(default, desired_auth.clone(), writer_addr, ctxts.clone());
        ResolverWrap(
            Arc::new(Mutex::new(ResolverWrapInner {
                router,
                desired_auth,
                default,
                by_path: HashMap::new(),
                writer_addr,
                ctxts,
            })),
            PhantomData,
        )
    }

    fn ctxts(&self) -> Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>> {
        let inner = self.0.lock();
        inner.ctxts.clone()
    }

    async fn send(&self, batch: Vec<T>) -> Result<Vec<F>> {
        let mut referrals = 0;
        loop {
            let mut batches = {
                let mut inner = self.0.lock();
                inner
                    .router
                    .route_batch(&batch)
                    .into_iter()
                    .map(|(r, batch)| match r {
                        None => (inner.default.clone(), batch),
                        Some(r) => match inner.by_path.get(&r.path) {
                            Some(con) => (con.clone(), batch),
                            None => {
                                let path = r.path.clone();
                                let con = C::new(
                                    r,
                                    inner.desired_auth.clone(),
                                    inner.writer_addr,
                                    inner.ctxts.clone(),
                                );
                                inner.by_path.insert(path, con.clone());
                                (con, batch)
                            }
                        },
                    })
                    .collect::<Vec<_>>()
            };
            let mut waiters = Vec::with_capacity(batches.len());
            for (c, b) in batches.iter_mut() {
                let batch = mem::replace(b, Vec::new());
                waiters.push(c.send(batch))
            }
            let res = future::join_all(waiters).await;
            let mut finished = Vec::with_capacity(batch.len());
            let mut referral = false;
            for r in res {
                let r = r?;
                for (id, reply) in r {
                    match reply.referral() {
                        Ok(r) => {
                            let mut inner = self.0.lock();
                            inner.router.add_referral(r);
                            referral = true;
                        }
                        Err(m) => finished.push((id, m)),
                    }
                }
            }
            if !referral {
                finished.sort_by_key(|(id, _)| *id);
                break Ok(finished.into_iter().map(|(_, m)| m).collect());
            }
            referrals += 1;
            if referrals > MAX_REFERRALS {
                bail!("maximum referral depth {} reached, giving up", MAX_REFERRALS);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolverRead(ResolverWrap<SingleRead, ToRead, FromRead>);

impl ResolverRead {
    fn new(default: Referral, desired_auth: Auth) -> Self {
        ResolverRead(ResolverWrap::new(
            default,
            desired_auth,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
        ))
    }

    async fn send(&self, batch: Vec<ToRead>) -> Result<Vec<FromRead>> {
        self.0.send(batch).await
    }
}

#[derive(Debug, Clone)]
pub struct ResolverWrite(ResolverWrap<SingleWrite, ToWrite, FromWrite>);

impl ResolverWrite {
    fn new(default: Referral, desired_auth: Auth, writer_addr: SocketAddr) -> Self {
        ResolverWrite(ResolverWrap::new(default, desired_auth, writer_addr))
    }

    async fn send(&self, batch: Vec<ToWrite>) -> Result<Vec<FromWrite>> {
        self.0.send(batch).await
    }

    fn ctxts(&self) -> Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>> {
        self.0.ctxts()
    }
}
