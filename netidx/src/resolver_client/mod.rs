pub(crate) mod common;
mod read_client;
mod write_client;

pub use crate::protocol::{
    glob::{Glob, GlobSet},
    resolver::{Resolved, Table},
};
use crate::{
    config::Config,
    pack::Z64,
    path::Path,
    pool::{Pool, Pooled},
    protocol::resolver::{
        FromRead, FromWrite, Publisher, PublisherId, Referral, ToRead, ToWrite,
    }, tls,
};
use anyhow::Result;
use arcstr::ArcStr;
pub use common::DesiredAuth;
use common::{
    ResponseChan, FROMREADPOOL, FROMWRITEPOOL, LISTPOOL, PATHPOOL, PUBLISHERPOOL,
    RAWFROMREADPOOL, RAWFROMWRITEPOOL, RAWTOREADPOOL, RAWTOWRITEPOOL, RESOLVEDPOOL,
    TOREADPOOL, TOWRITEPOOL,
};
use futures::future;
use fxhash::FxHashMap;
use parking_lot::{Mutex, RwLock};
use read_client::ReadClient;
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
use write_client::WriteClient;

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
            | ToWrite::UnpublishDefault(p)
            | ToWrite::PublishDefault(p)
            | ToWrite::PublishWithFlags(p, _)
            | ToWrite::PublishDefaultWithFlags(p, _) => Some(p),
        }
    }
}

#[derive(Debug)]
struct Router {
    cached: BTreeMap<Path, (Option<Instant>, Arc<Referral>)>,
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
                                    if exp.is_none() || now < exp.unwrap() {
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
        let exp = r.ttl.map(|ttl| Instant::now() + Duration::from_secs(ttl as u64));
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
            FromWrite::Referral(r) => Ok(r.into()),
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
        desired_auth: DesiredAuth,
        writer_addr: SocketAddr,
        secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
        tls: tls::CachedConnector,
    ) -> Self;
    fn send(&mut self, batch: Pooled<Vec<(usize, T)>>) -> ResponseChan<F>;
}

impl Connection<ToRead, FromRead> for ReadClient {
    fn new(
        resolver: Arc<Referral>,
        desired_auth: DesiredAuth,
        _writer_addr: SocketAddr,
        _secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
        tls: tls::CachedConnector,
    ) -> Self {
        ReadClient::new(resolver, desired_auth, tls)
    }

    fn send(&mut self, batch: Pooled<Vec<(usize, ToRead)>>) -> ResponseChan<FromRead> {
        ReadClient::send(self, batch)
    }
}

impl Connection<ToWrite, FromWrite> for WriteClient {
    fn new(
        resolver: Arc<Referral>,
        desired_auth: DesiredAuth,
        writer_addr: SocketAddr,
        secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
        tls: tls::CachedConnector,
    ) -> Self {
        WriteClient::new(resolver, desired_auth, writer_addr, secrets, tls)
    }

    fn send(&mut self, batch: Pooled<Vec<(usize, ToWrite)>>) -> ResponseChan<FromWrite> {
        WriteClient::send(self, batch)
    }
}

#[derive(Debug)]
struct ResolverWrapInner<C, T, F>
where
    T: Send + Sync + 'static,
    F: Send + Sync + 'static,
{
    router: Router,
    desired_auth: DesiredAuth,
    default: Arc<Referral>,
    by_server: HashMap<Arc<Referral>, C>,
    writer_addr: SocketAddr,
    secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
    tls: tls::CachedConnector,
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
    ) -> ResponseChan<F> {
        let r = server.unwrap_or_else(|| self.default.clone());
        match self.by_server.get_mut(&r) {
            Some(con) => con.send(batch),
            None => {
                let mut con = C::new(
                    r.clone(),
                    self.desired_auth.clone(),
                    self.writer_addr,
                    self.secrets.clone(),
                    self.tls.clone(),
                );
                self.by_server.insert(r, con.clone());
                con.send(batch)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ResolverWrap<C, T, F>(Arc<Mutex<ResolverWrapInner<C, T, F>>>)
where
    T: Send + Sync + 'static,
    F: Send + Sync + 'static;

impl<C, T, F> ResolverWrap<C, T, F>
where
    C: Connection<T, F> + Clone + 'static,
    T: ToPath + Clone + Send + Sync + 'static,
    F: ToReferral + Clone + Send + Sync + 'static,
{
    fn new(
        default: Config,
        desired_auth: DesiredAuth,
        writer_addr: SocketAddr,
        f_pool: Pool<Vec<F>>,
        fi_pool: Pool<Vec<(usize, F)>>,
        ti_pool: Pool<Vec<(usize, T)>>,
    ) -> ResolverWrap<C, T, F> {
        let secrets = Arc::new(RwLock::new(HashMap::default()));
        let mut router = Router::new();
        let default: Arc<Referral> = Arc::new(default.to_referral());
        router.add_referral(default.clone());
        ResolverWrap(Arc::new(Mutex::new(ResolverWrapInner {
            router,
            desired_auth,
            default,
            by_server: HashMap::new(),
            writer_addr,
            secrets,
            tls: tls::CachedConnector::new(),
            f_pool,
            fi_pool,
            ti_pool,
            phantom: PhantomData,
        })))
    }

    fn secrets(&self) -> Arc<RwLock<FxHashMap<SocketAddr, u128>>> {
        Arc::clone(&self.0.lock().secrets)
    }

    async fn send(
        &self,
        batch: &Pooled<Vec<T>>,
    ) -> Result<(Pooled<FxHashMap<PublisherId, Publisher>>, Pooled<Vec<F>>)> {
        let mut referrals = 0;
        loop {
            let mut waiters = Vec::new();
            let (mut finished, mut res) = {
                let mut guard = self.0.lock();
                let inner = &mut *guard;
                if inner.by_server.len() > MAX_REFERRALS {
                    inner.by_server.clear(); // a workable sledgehammer
                }
                for (r, batch) in inner.router.route_batch(&inner.ti_pool, batch) {
                    waiters.push(inner.send_to_server(r, batch))
                }
                (inner.fi_pool.take(), inner.f_pool.take())
            };
            let mut referral = false;
            //: Option<Pooled<FxHashMap<PublisherId, Publisher>>>
            let mut publishers = None;
            for r in future::join_all(waiters).await {
                let (mut p, mut r) = r?;
                match publishers.as_mut() {
                    None => {
                        publishers = Some(p);
                    }
                    Some(publishers) => {
                        publishers.extend(p.drain());
                    }
                };
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
                let publishers = publishers.unwrap_or_else(|| PUBLISHERPOOL.take());
                break Ok((publishers, res));
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
    by_resolver: FxHashMap<SocketAddr, Z64>,
}

impl ChangeTracker {
    pub fn new(path: Path) -> Self {
        ChangeTracker { path, by_resolver: HashMap::default() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone)]
pub struct ResolverRead(ResolverWrap<ReadClient, ToRead, FromRead>);

impl ResolverRead {
    pub fn new(default: Config, desired_auth: DesiredAuth) -> Self {
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
    ) -> Result<(Pooled<FxHashMap<PublisherId, Publisher>>, Pooled<Vec<FromRead>>)> {
        self.0.send(batch).await
    }

    /// resolve the specified paths, results are in send order
    pub async fn resolve<I>(
        &self,
        batch: I,
    ) -> Result<(Pooled<FxHashMap<PublisherId, Publisher>>, Pooled<Vec<Resolved>>)>
    where
        I: IntoIterator<Item = Path>,
    {
        let mut to = RAWTOREADPOOL.take();
        to.extend(batch.into_iter().map(ToRead::Resolve));
        let (publishers, mut result) = self.send(&to).await?;
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
            Ok((publishers, out))
        }
    }

    /// list children of the specified path. Order is unspecified.
    pub async fn list(&self, path: Path) -> Result<Pooled<Vec<Path>>> {
        let mut to = RAWTOREADPOOL.take();
        to.push(ToRead::List(path.clone()));
        let (_, mut result) = self.send(&to).await?;
        if result.len() != 1 {
            bail!("expected 1 result from list got {}", result.len());
        } else {
            let mut from_server = match result.pop().unwrap() {
                FromRead::List(paths) => paths,
                m => bail!("unexpected result from list {:?}", m),
            };
            from_server.sort();
            for p in (self.0).0.lock().router.cached.keys() {
                if Path::is_immediate_parent(&path, p) {
                    if let Err(i) = from_server.binary_search(p) {
                        from_server.insert(i, p.clone())
                    }
                }
            }
            Ok(from_server)
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
                for referral in pending.drain(..) {
                    let referral = referral.unwrap_or_else(|| inner.default.clone());
                    if !done.contains(&referral) {
                        done.insert(referral.clone());
                        let referral = inner.router.add_referral(referral);
                        let mut to = TOREADPOOL.take();
                        to.push((0, message.clone()));
                        waiters.push(inner.send_to_server(Some(referral), to));
                    }
                }
            }
            for r in future::join_all(waiters).await {
                let (_, mut r) = r?;
                for (_, reply) in r.drain(..) {
                    let mut referrals = process_reply(reply)?;
                    for r in referrals.drain(..) {
                        pending.push(Some(Arc::new(r.into())));
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
        if !globset.published_only() {
            let mut refs = PATHPOOL.take();
            for p in (self.0).0.lock().router.cached.keys() {
                if globset.is_match(p) {
                    refs.push(p.clone());
                }
            }
            if refs.len() > 0 {
                results.push(refs);
            }
        }
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
        to.push(ToRead::Table(path.clone()));
        let (_, mut result) = self.send(&to).await?;
        if result.len() != 1 {
            bail!("expected 1 result from table got {}", result.len());
        } else {
            match result.pop().unwrap() {
                FromRead::Table(mut table) => {
                    let skip = Path::levels(&path) + 1;
                    table.rows.sort();
                    for p in (self.0).0.lock().router.cached.keys() {
                        if Path::is_immediate_parent(&path, p) {
                            if let Some(part) = Path::dirnames(p).skip(skip).next() {
                                let part = Path::from(ArcStr::from(part));
                                if let Err(i) = table.rows.binary_search(&part) {
                                    table.rows.insert(i, Path::from(part));
                                }
                            }
                        }
                    }
                    Ok(table)
                }
                m => bail!("unexpected result from table {:?}", m),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolverWrite(ResolverWrap<WriteClient, ToWrite, FromWrite>);

impl ResolverWrite {
    pub fn new(
        default: Config,
        desired_auth: DesiredAuth,
        writer_addr: SocketAddr,
    ) -> Self {
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
        let (_, r) = self.0.send(batch).await?;
        Ok(r)
    }

    async fn send_expect<V, F, I>(
        &self,
        batch: I,
        expected: FromWrite,
        f: F,
    ) -> Result<()>
    where
        F: Fn(V) -> ToWrite,
        I: IntoIterator<Item = V>,
    {
        let mut to = RAWTOWRITEPOOL.take();
        let len = to.len();
        to.extend(batch.into_iter().map(f));
        let (_, mut from) = self.0.send(&to).await?;
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

    pub async fn publish_with_flags<I: IntoIterator<Item = (Path, Option<u32>)>>(
        &self,
        batch: I,
    ) -> Result<()> {
        self.send_expect(batch, FromWrite::Published, |(path, flags)| match flags {
            Some(flags) => ToWrite::PublishWithFlags(path, flags),
            None => ToWrite::Publish(path),
        })
        .await
    }

    pub async fn publish_default<I: IntoIterator<Item = Path>>(
        &self,
        batch: I,
    ) -> Result<()> {
        self.send_expect(batch, FromWrite::Published, ToWrite::PublishDefault).await
    }

    pub async fn publish_default_with_flags<
        I: IntoIterator<Item = (Path, Option<u32>)>,
    >(
        &self,
        batch: I,
    ) -> Result<()> {
        self.send_expect(batch, FromWrite::Published, |(path, flags)| match flags {
            Some(flags) => ToWrite::PublishDefaultWithFlags(path, flags),
            None => ToWrite::PublishDefault(path),
        })
        .await
    }

    pub async fn unpublish<I: IntoIterator<Item = Path>>(&self, batch: I) -> Result<()> {
        self.send_expect(batch, FromWrite::Unpublished, ToWrite::Unpublish).await
    }

    pub async fn unpublish_default<I: IntoIterator<Item = Path>>(
        &self,
        batch: I,
    ) -> Result<()> {
        self.send_expect(batch, FromWrite::Unpublished, ToWrite::UnpublishDefault).await
    }

    // CR estokes: this is broken on complex clusters, but it's also
    // redundant, consider removing it.
    pub async fn clear(&self) -> Result<()> {
        let mut batch = RAWTOWRITEPOOL.take();
        batch.push(ToWrite::Clear);
        let (_, r) = self.0.send(&batch).await?;
        if r.len() != 1 {
            bail!("unexpected response to clear command {:?}", r)
        } else {
            match &r[0] {
                FromWrite::Unpublished => Ok(()),
                m => bail!("unexpected response to clear command {:?}", m),
            }
        }
    }

    pub(crate) fn secrets(&self) -> Arc<RwLock<FxHashMap<SocketAddr, u128>>> {
        self.0.secrets()
    }
}
