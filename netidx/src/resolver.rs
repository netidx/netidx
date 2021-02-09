use crate::{
    config::Config,
    pack::Z64,
    path::Path,
    pool::{Pool, Pooled},
    protocol::resolver::{FromRead, FromWrite, Referral, ToRead, ToWrite},
    resolver_single::{
        ResolverRead as SingleRead, ResolverWrite as SingleWrite, RAWFROMREADPOOL,
        RAWFROMWRITEPOOL,
    },
};
pub use crate::{
    protocol::{
        glob::{Glob, GlobSet},
        resolver::{Resolved, Table},
    },
    resolver_single::Auth,
};
use anyhow::Result;
use futures::{channel::oneshot, future};
use fxhash::FxBuildHasher;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{
        hash_map::Entry,
        BTreeMap,
        Bound::{self, Included, Unbounded},
        HashMap, HashSet,
    },
    iter::IntoIterator,
    marker::PhantomData,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    result,
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;

const MAX_REFERRALS: usize = 128;

trait ToPath {
    fn path(&self) -> Option<&Path>;
}

impl ToPath for ToRead {
    fn path(&self) -> Option<&Path> {
        match self {
            ToRead::List(p) | ToRead::Table(p) | ToRead::Resolve(p) => Some(p),
            ToRead::ListMatching(_) | ToRead::GetChangeNr(_) => None,
        }
    }
}

impl ToPath for ToWrite {
    fn path(&self) -> Option<&Path> {
        match self {
            ToWrite::Clear | ToWrite::Heartbeat => None,
            ToWrite::Publish(p)
            | ToWrite::Unpublish(p)
            | ToWrite::PublishDefault(p)
            | ToWrite::PublishWithFlags(p, _)
            | ToWrite::PublishDefaultWithFlags(p, _) => Some(p),
        }
    }
}

#[derive(Debug)]
struct Router {
    cached: BTreeMap<Path, (Instant, Arc<Referral>)>,
}

impl Router {
    fn new() -> Self {
        Router { cached: BTreeMap::new() }
    }

    fn route_batch<T>(
        &mut self,
        pool: &Pool<Vec<(usize, T)>>,
        batch: &Pooled<Vec<T>>,
    ) -> impl Iterator<Item = (Option<Arc<Referral>>, Pooled<Vec<(usize, T)>>)>
    where
        T: ToPath + Clone + Send + Sync + 'static,
    {
        let now = Instant::now();
        let mut batches = HashMap::new();
        let mut gc = Vec::new();
        let mut id = 0;
        for v in batch.iter() {
            let v = v.clone();
            match v.path() {
                None => batches.entry(None).or_insert_with(|| pool.take()).push((id, v)),
                Some(path) => {
                    let mut r = self.cached.range::<str, (Bound<&str>, Bound<&str>)>((
                        Unbounded,
                        Included(path.as_ref()),
                    ));
                    loop {
                        match r.next_back() {
                            None => {
                                batches
                                    .entry(None)
                                    .or_insert_with(|| pool.take())
                                    .push((id, v));
                                break;
                            }
                            Some((p, (exp, r))) => {
                                if !Path::is_parent(p, path) {
                                    continue;
                                } else {
                                    if &now < exp {
                                        batches
                                            .entry(Some(r.clone()))
                                            .or_insert_with(|| pool.take())
                                            .push((id, v))
                                    } else {
                                        gc.push(p.clone());
                                        batches
                                            .entry(None)
                                            .or_insert_with(|| pool.take())
                                            .push((id, v))
                                    }
                                    break;
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
        batches.into_iter().map(|(p, batch)| match p {
            None => (None, batch),
            Some(p) => (Some(p), batch),
        })
    }

    fn add_referral(&mut self, r: Arc<Referral>) -> Arc<Referral> {
        let exp = Instant::now() + Duration::from_secs(r.ttl);
        let key = r.path.clone();
        self.cached.insert(key, (exp, r.clone()));
        r
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
    T: ToPath + Send + Sync + 'static,
    F: ToReferral + Send + Sync + 'static,
{
    fn new(
        resolver: Arc<Referral>,
        desired_auth: Auth,
        writer_addr: SocketAddr,
        secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    ) -> Self;
    fn send(
        &mut self,
        batch: Pooled<Vec<(usize, T)>>,
    ) -> oneshot::Receiver<Pooled<Vec<(usize, F)>>>;
}

impl Connection<ToRead, FromRead> for SingleRead {
    fn new(
        resolver: Arc<Referral>,
        desired_auth: Auth,
        _writer_addr: SocketAddr,
        _secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    ) -> Self {
        SingleRead::new(resolver, desired_auth)
    }

    fn send(
        &mut self,
        batch: Pooled<Vec<(usize, ToRead)>>,
    ) -> oneshot::Receiver<Pooled<Vec<(usize, FromRead)>>> {
        SingleRead::send(self, batch)
    }
}

impl Connection<ToWrite, FromWrite> for SingleWrite {
    fn new(
        resolver: Arc<Referral>,
        desired_auth: Auth,
        writer_addr: SocketAddr,
        secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    ) -> Self {
        SingleWrite::new(resolver, desired_auth, writer_addr, secrets)
    }

    fn send(
        &mut self,
        batch: Pooled<Vec<(usize, ToWrite)>>,
    ) -> oneshot::Receiver<Pooled<Vec<(usize, FromWrite)>>> {
        SingleWrite::send(self, batch)
    }
}

lazy_static! {
    static ref RAWTOREADPOOL: Pool<Vec<ToRead>> = Pool::new(1000, 10000);
    static ref TOREADPOOL: Pool<Vec<(usize, ToRead)>> = Pool::new(1000, 10000);
    static ref FROMREADPOOL: Pool<Vec<(usize, FromRead)>> = Pool::new(1000, 10000);
    static ref RAWTOWRITEPOOL: Pool<Vec<ToWrite>> = Pool::new(1000, 10000);
    static ref TOWRITEPOOL: Pool<Vec<(usize, ToWrite)>> = Pool::new(1000, 10000);
    static ref FROMWRITEPOOL: Pool<Vec<(usize, FromWrite)>> = Pool::new(1000, 10000);
    static ref RESOLVEDPOOL: Pool<Vec<Resolved>> = Pool::new(1000, 10000);
    static ref LISTPOOL: Pool<Vec<Pooled<Vec<Path>>>> = Pool::new(1000, 10000);
}

#[derive(Debug)]
struct ResolverWrapInner<C, T, F>
where
    T: Send + Sync + 'static,
    F: Send + Sync + 'static,
{
    router: Router,
    desired_auth: Auth,
    default: Arc<Referral>,
    by_referral: HashMap<Arc<Referral>, C>,
    writer_addr: SocketAddr,
    secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    phantom: PhantomData<(T, F)>,
    f_pool: Pool<Vec<F>>,
    fi_pool: Pool<Vec<(usize, F)>>,
    ti_pool: Pool<Vec<(usize, T)>>,
}

impl<C, T, F> ResolverWrapInner<C, T, F>
where
    C: Connection<T, F> + Clone + 'static,
    T: ToPath + Clone + Send + Sync + 'static,
    F: ToReferral + Clone + Send + Sync + 'static,
{
    fn send_to_server(
        &mut self,
        server: Option<Arc<Referral>>,
        batch: Pooled<Vec<(usize, T)>>,
    ) -> oneshot::Receiver<Pooled<Vec<(usize, F)>>> {
        let r = server.unwrap_or_else(|| self.default.clone());
        match self.by_referral.get_mut(&r) {
            Some(con) => con.send(batch),
            None => {
                let mut con = C::new(
                    r.clone(),
                    self.desired_auth.clone(),
                    self.writer_addr,
                    self.secrets.clone(),
                );
                self.by_referral.insert(r, con.clone());
                con.send(batch)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ResolverWrap<C, T: Send + Sync + 'static, F: Send + Sync + 'static>(
    Arc<Mutex<ResolverWrapInner<C, T, F>>>,
);

impl<C, T, F> ResolverWrap<C, T, F>
where
    C: Connection<T, F> + Clone + 'static,
    T: ToPath + Clone + Send + Sync + 'static,
    F: ToReferral + Clone + Send + Sync + 'static,
{
    fn new(
        default: Config,
        desired_auth: Auth,
        writer_addr: SocketAddr,
        f_pool: Pool<Vec<F>>,
        fi_pool: Pool<Vec<(usize, F)>>,
        ti_pool: Pool<Vec<(usize, T)>>,
    ) -> ResolverWrap<C, T, F> {
        let secrets =
            Arc::new(RwLock::new(HashMap::with_hasher(FxBuildHasher::default())));
        let router = Router::new();
        ResolverWrap(Arc::new(Mutex::new(ResolverWrapInner {
            router,
            desired_auth,
            default: Arc::new(default.into()),
            by_referral: HashMap::new(),
            writer_addr,
            secrets,
            f_pool,
            fi_pool,
            ti_pool,
            phantom: PhantomData,
        })))
    }

    fn secrets(&self) -> Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>> {
        self.0.lock().secrets.clone()
    }

    async fn send(&self, batch: &Pooled<Vec<T>>) -> Result<Pooled<Vec<F>>> {
        let mut referrals = 0;
        loop {
            let mut waiters = Vec::new();
            let (mut finished, mut res) = {
                let mut guard = self.0.lock();
                let inner = &mut *guard;
                if inner.by_referral.len() > MAX_REFERRALS {
                    inner.by_referral.clear(); // a workable sledgehammer
                }
                for (r, batch) in inner.router.route_batch(&inner.ti_pool, batch) {
                    waiters.push(inner.send_to_server(r, batch))
                }
                (inner.fi_pool.take(), inner.f_pool.take())
            };
            let mut referral = false;
            for r in future::join_all(waiters).await {
                let mut r = r?;
                for (id, reply) in r.drain(..) {
                    match reply.referral() {
                        Err(m) => finished.push((id, m)),
                        Ok(r) => {
                            self.0.lock().router.add_referral(Arc::new(r));
                            referral = true;
                        }
                    }
                }
            }
            if !referral {
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
pub struct ChangeTracker {
    path: Path,
    by_resolver: HashMap<SocketAddr, Z64, FxBuildHasher>,
}

impl ChangeTracker {
    pub fn new(path: Path) -> Self {
        ChangeTracker {
            path,
            by_resolver: HashMap::with_hasher(FxBuildHasher::default()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone)]
pub struct ResolverRead(ResolverWrap<SingleRead, ToRead, FromRead>);

impl ResolverRead {
    pub fn new(default: Config, desired_auth: Auth) -> Self {
        ResolverRead(ResolverWrap::new(
            default,
            desired_auth,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
            RAWFROMREADPOOL.clone(),
            FROMREADPOOL.clone(),
            TOREADPOOL.clone(),
        ))
    }

    /// send the specified messages to the resolver, and return the answers (in send order)
    pub async fn send(
        &self,
        batch: &Pooled<Vec<ToRead>>,
    ) -> Result<Pooled<Vec<FromRead>>> {
        self.0.send(batch).await
    }

    /// resolve the specified paths, results are in send order
    pub async fn resolve<I>(&self, batch: I) -> Result<Pooled<Vec<Resolved>>>
    where
        I: IntoIterator<Item = Path>,
    {
        let mut to = RAWTOREADPOOL.take();
        to.extend(batch.into_iter().map(ToRead::Resolve));
        let mut result = self.send(&to).await?;
        if result.len() != to.len() {
            bail!(
                "unexpected number of resolve results {} expected {}",
                result.len(),
                to.len()
            )
        } else {
            let mut out = RESOLVEDPOOL.take();
            for r in result.drain(..) {
                match r {
                    FromRead::Resolved(r) => {
                        out.push(r);
                    }
                    m => bail!("unexpected resolve response {:?}", m),
                }
            }
            Ok(out)
        }
    }

    /// list children of the specified path. Order is unspecified.
    pub async fn list(&self, path: Path) -> Result<Pooled<Vec<Path>>> {
        let mut to = RAWTOREADPOOL.take();
        to.push(ToRead::List(path));
        let mut result = self.send(&to).await?;
        if result.len() != 1 {
            bail!("expected 1 result from list got {}", result.len());
        } else {
            match result.pop().unwrap() {
                FromRead::List(paths) => Ok(paths),
                m => bail!("unexpected result from list {:?}", m),
            }
        }
    }

    async fn send_and_aggregate<F: FnMut(FromRead) -> Result<Pooled<Vec<Referral>>>>(
        &self,
        message: ToRead,
        mut process_reply: F,
    ) -> Result<()> {
        let mut pending: Vec<Option<Arc<Referral>>> = vec![None];
        let mut done: HashSet<Arc<Referral>> = HashSet::new();
        let mut referral_cycles = 0;
        while pending.len() > 0 {
            let mut waiters = Vec::new();
            {
                let mut inner = self.0 .0.lock();
                for server in pending.drain(..) {
                    let server = server.unwrap_or_else(|| inner.default.clone());
                    if !done.contains(&server) {
                        done.insert(server.clone());
                        let server = inner.router.add_referral(server);
                        let mut to = TOREADPOOL.take();
                        to.push((0, message.clone()));
                        waiters.push(inner.send_to_server(Some(server), to));
                    }
                }
            }
            for r in future::join_all(waiters).await {
                let mut r = r?;
                for (_, reply) in r.drain(..) {
                    let mut referrals = process_reply(reply)?;
                    for r in referrals.drain(..) {
                        pending.push(Some(Arc::new(r)));
                    }
                }
            }
            referral_cycles += 1;
            if referral_cycles > MAX_REFERRALS {
                bail!("max referrals reached")
            }
        }
        Ok(())
    }

    /// list all paths in the cluster matching the specified
    /// globset. You will get a list of batches of paths. If your
    /// globset is configured to match only published paths, then the
    /// batches should be disjoint, otherwise there may be some
    /// duplicate structural elements.
    pub async fn list_matching(
        &self,
        globset: &GlobSet,
    ) -> Result<Pooled<Vec<Pooled<Vec<Path>>>>> {
        let mut results = LISTPOOL.take();
        let m = ToRead::ListMatching(globset.clone());
        self.send_and_aggregate(m, |reply| match reply {
            FromRead::ListMatching(mut lm) => {
                results.extend(lm.matched.drain(..));
                Ok(lm.referrals)
            }
            m => bail!("unexpected list_matching response {:?}", m),
        })
        .await?;
        Ok(results)
    }

    /// Check whether that have been any changes to the specified path
    /// or any of it's children on any server in the resolver
    /// cluster. A change in this context consists of,
    ///
    /// * A new publisher publishing an existing path
    /// * A publisher publishing a new path
    /// * A publisher no longer publishing a path
    ///
    /// Changes to the value of already published paths is not a
    /// change in this context.
    ///
    /// This method is meant to be used as a light alternative to
    /// list, or list_matching in order to discover when structural
    /// changes are made by publishers that result in the need to
    /// adjust subscriptions. It is much cheaper and faster to call
    /// this method than `list` or `list_matching`.
    ///
    /// The first call with a new `ChangeTracker` will always result
    /// in `true`. If `true` is returned at any point it is not a
    /// guarantee that there were changes, but it is a strong
    /// possibility. If `false` is returned it is guaranteed that
    /// there was no change.
    pub async fn check_changed(&self, tracker: &mut ChangeTracker) -> Result<bool> {
        let m = ToRead::GetChangeNr(tracker.path.clone());
        let mut res = false;
        self.send_and_aggregate(m, |reply| match reply {
            FromRead::GetChangeNr(cn) => match tracker.by_resolver.entry(cn.resolver) {
                Entry::Vacant(e) => {
                    res = true;
                    e.insert(cn.change_number);
                    Ok(cn.referrals)
                }
                Entry::Occupied(mut e) => {
                    if **e.get() < *cn.change_number {
                        res = true;
                    }
                    *e.get_mut() = cn.change_number;
                    Ok(cn.referrals)
                }
            },
            m => bail!("unexpected response to GetChangeNr, {:?}", m),
        })
        .await?;
        Ok(res)
    }

    pub async fn table(&self, path: Path) -> Result<Table> {
        let mut to = RAWTOREADPOOL.take();
        to.push(ToRead::Table(path));
        let mut result = self.send(&to).await?;
        if result.len() != 1 {
            bail!("expected 1 result from table got {}", result.len());
        } else {
            match result.pop().unwrap() {
                FromRead::Table(table) => Ok(table),
                m => bail!("unexpected result from table {:?}", m),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolverWrite(ResolverWrap<SingleWrite, ToWrite, FromWrite>);

impl ResolverWrite {
    pub fn new(default: Config, desired_auth: Auth, writer_addr: SocketAddr) -> Self {
        ResolverWrite(ResolverWrap::new(
            default,
            desired_auth,
            writer_addr,
            RAWFROMWRITEPOOL.clone(),
            FROMWRITEPOOL.clone(),
            TOWRITEPOOL.clone(),
        ))
    }

    pub async fn send(
        &self,
        batch: &Pooled<Vec<ToWrite>>,
    ) -> Result<Pooled<Vec<FromWrite>>> {
        self.0.send(batch).await
    }

    async fn send_expect<V, F, I>(&self, batch: I, expected: FromWrite, f: F) -> Result<()>
    where
        F: Fn(V) -> ToWrite,
        I: IntoIterator<Item = V>,
    {
        let mut to = RAWTOWRITEPOOL.take();
        let len = to.len();
        to.extend(batch.into_iter().map(f));
        let mut from = self.0.send(&to).await?;
        if from.len() != to.len() {
            bail!("unexpected number of responses {} vs expected {}", from.len(), len);
        }
        for (i, reply) in from.drain(..).enumerate() {
            if reply != expected {
                bail!("unexpected response to {:?}, {:?}", &to[i], reply)
            }
        }
        Ok(())
    }

    pub async fn publish<I: IntoIterator<Item = Path>>(&self, batch: I) -> Result<()> {
        self.send_expect(batch, FromWrite::Published, ToWrite::Publish).await
    }

    pub async fn publish_with_flags<I: IntoIterator<Item = (Option<u16>, Path)>>(
        &self,
        batch: I,
    ) -> Result<()> {
        self.send_expect(batch, FromWrite::Published, |(flags, path)| match flags {
            Some(flags) => ToWrite::PublishWithFlags(path, flags),
            None => ToWrite::Publish(path)
        }).await
    }

    pub async fn publish_default<I: IntoIterator<Item = Path>>(
        &self,
        batch: I,
    ) -> Result<()> {
        self.send_expect(batch, FromWrite::Published, ToWrite::PublishDefault).await
    }

    pub async fn publish_default_with_flags<I: IntoIterator<Item = (Option<u16>, Path)>>(
        &self,
        batch: I,
    ) -> Result<()> {
        self.send_expect(batch, FromWrite::Published, |(flags, path)| match flags {
            Some(flags) => ToWrite::PublishDefaultWithFlags(path, flags),
            None => ToWrite::Publish(path)
        }).await
    }

    pub async fn unpublish<I: IntoIterator<Item = Path>>(&self, batch: I) -> Result<()> {
        self.send_expect(batch, FromWrite::Unpublished, ToWrite::Unpublish).await
    }

    // CR estokes: this is broken on complex clusters, but it's also
    // redundant, consider removing it.
    pub async fn clear(&self) -> Result<()> {
        let mut batch = RAWTOWRITEPOOL.take();
        batch.push(ToWrite::Clear);
        let r = self.0.send(&batch).await?;
        if r.len() != 1 {
            bail!("unexpected response to clear command {:?}", r)
        } else {
            match &r[0] {
                FromWrite::Unpublished => Ok(()),
                m => bail!("unexpected response to clear command {:?}", m),
            }
        }
    }

    pub(crate) fn secrets(
        &self,
    ) -> Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>> {
        self.0.secrets()
    }
}
