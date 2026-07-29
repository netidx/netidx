//! Client for querying the resolver server.
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
    protocol::resolver::{
        FromRead, FromWrite, Publisher, PublisherId, Referral, ToRead, ToWrite,
    },
    tls,
};
use ahash::{AHashMap, AHashSet};
use anyhow::Result;
use arcstr::ArcStr;
pub use common::DesiredAuth;
use common::{
    FROMREADPOOL, FROMWRITEPOOL, LISTPOOL, PATHPOOL, PUBLISHERPOOL, RAWFROMREADPOOL,
    RAWFROMWRITEPOOL, RAWTOREADPOOL, RAWTOWRITEPOOL, RESOLVEDPOOL, ResponseChan,
    TOREADPOOL, TOWRITEPOOL,
};
use futures::future;
use netidx_netproto::resolver::PublisherPriority;
use parking_lot::{Mutex, RwLock};
use poolshark::{
    global::{GPooled, Pool},
    local::LPooled,
};
use read_client::ReadClient;
use std::{
    collections::{
        BTreeMap,
        Bound::{self, Included, Unbounded},
        HashMap,
        hash_map::Entry,
    },
    iter::IntoIterator,
    marker::PhantomData,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    result,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, time::Instant};
use write_client::WriteClient;

const MAX_REFERRALS: usize = 128;

/// The identity of a publisher record returned by a resolver.
///
/// [`PublisherId`] values are allocated independently by each resolver process,
/// so the resolver address is part of the identifier whenever publisher tables
/// from multiple referrals are combined.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PublisherKey {
    pub resolver: SocketAddr,
    pub id: PublisherId,
}

impl PublisherKey {
    pub fn new(resolver: SocketAddr, id: PublisherId) -> Self {
        Self { resolver, id }
    }
}

impl From<&Publisher> for PublisherKey {
    fn from(publisher: &Publisher) -> Self {
        Self::new(publisher.resolver, publisher.id)
    }
}

/// Publisher records returned by resolver read operations, keyed by the
/// resolver process that allocated the publisher ID.
pub type PublisherTable = AHashMap<PublisherKey, Publisher>;

fn insert_publisher(table: &mut PublisherTable, publisher: Publisher) {
    table.insert(PublisherKey::from(&publisher), publisher);
}

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
        batch: &GPooled<Vec<T>>,
    ) -> impl Iterator<Item = (Option<Arc<Referral>>, GPooled<Vec<(usize, T)>>)> + use<T>
    where
        T: ToPath + Clone + Send + Sync + 'static,
    {
        let now = Instant::now();
        let mut batches = AHashMap::default();
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

/// A referral that will never change. The channel is created, the value
/// published, and the sender dropped: `borrow` keeps working forever and
/// `changed` never fires, so a connection to a referral learned at runtime
/// needs no special case.
fn constant(referral: Arc<Referral>) -> watch::Receiver<Arc<Referral>> {
    let (tx, rx) = watch::channel(referral);
    drop(tx);
    rx
}

trait Connection<T, F>
where
    T: ToPath + Send + Sync + 'static,
    F: ToReferral + Send + Sync + 'static,
{
    fn new(
        resolver: watch::Receiver<Arc<Referral>>,
        desired_auth: DesiredAuth,
        writer_addr: SocketAddr,
        priority: PublisherPriority,
        secrets: Arc<RwLock<AHashMap<SocketAddr, u128>>>,
        tls: Option<tls::CachedConnector>,
    ) -> Self;
    fn send(&mut self, batch: GPooled<Vec<(usize, T)>>) -> ResponseChan<F>;
}

impl Connection<ToRead, FromRead> for ReadClient {
    fn new(
        resolver: watch::Receiver<Arc<Referral>>,
        desired_auth: DesiredAuth,
        _writer_addr: SocketAddr,
        _priority: PublisherPriority,
        _secrets: Arc<RwLock<AHashMap<SocketAddr, u128>>>,
        tls: Option<tls::CachedConnector>,
    ) -> Self {
        ReadClient::new(resolver, desired_auth, tls)
    }

    fn send(&mut self, batch: GPooled<Vec<(usize, ToRead)>>) -> ResponseChan<FromRead> {
        ReadClient::send(self, batch)
    }
}

impl Connection<ToWrite, FromWrite> for WriteClient {
    fn new(
        resolver: watch::Receiver<Arc<Referral>>,
        desired_auth: DesiredAuth,
        writer_addr: SocketAddr,
        priority: PublisherPriority,
        secrets: Arc<RwLock<AHashMap<SocketAddr, u128>>>,
        tls: Option<tls::CachedConnector>,
    ) -> Self {
        WriteClient::new(resolver, desired_auth, writer_addr, priority, secrets, tls)
    }

    fn send(&mut self, batch: GPooled<Vec<(usize, ToWrite)>>) -> ResponseChan<FromWrite> {
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
    /// The local cluster, which can change under us as members are added and
    /// removed. Its connection is kept out of `by_server` — that map is keyed
    /// by referral, so re-keying it on every address change would drop the
    /// connection and everything it knows.
    default: watch::Receiver<Arc<Referral>>,
    default_con: Option<C>,
    by_server: HashMap<Arc<Referral>, C>,
    writer_addr: SocketAddr,
    priority: PublisherPriority,
    secrets: Arc<RwLock<AHashMap<SocketAddr, u128>>>,
    tls: Option<tls::CachedConnector>,
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
    /// Pick up an address change before routing anything. The router caches
    /// the default referral by path, so a new one has to replace it there too
    /// or batches would keep routing to the copy it already has.
    fn refresh_default(&mut self) -> Arc<Referral> {
        if self.default.has_changed().unwrap_or(false) {
            let referral = self.default.borrow_and_update().clone();
            self.router.add_referral(referral.clone());
            referral
        } else {
            self.default.borrow().clone()
        }
    }

    fn send_to_server(
        &mut self,
        server: Option<Arc<Referral>>,
        batch: GPooled<Vec<(usize, T)>>,
    ) -> ResponseChan<F> {
        let default = self.default.borrow().clone();
        let r = match server {
            None => None,
            Some(r) if Arc::ptr_eq(&r, &default) => None,
            Some(r) => Some(r),
        };
        match r {
            None => {
                if self.default_con.is_none() {
                    self.default_con = Some(C::new(
                        self.default.clone(),
                        self.desired_auth.clone(),
                        self.writer_addr,
                        self.priority,
                        self.secrets.clone(),
                        self.tls.clone(),
                    ));
                }
                self.default_con.as_mut().unwrap().send(batch)
            }
            Some(r) => match self.by_server.get_mut(&r) {
                Some(con) => con.send(batch),
                None => {
                    let mut con = C::new(
                        constant(r.clone()),
                        self.desired_auth.clone(),
                        self.writer_addr,
                        self.priority,
                        self.secrets.clone(),
                        self.tls.clone(),
                    );
                    self.by_server.insert(r, con.clone());
                    con.send(batch)
                }
            },
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
        priority: PublisherPriority,
        f_pool: Pool<Vec<F>>,
        fi_pool: Pool<Vec<(usize, F)>>,
        ti_pool: Pool<Vec<(usize, T)>>,
    ) -> ResolverWrap<C, T, F> {
        let secrets = Arc::new(RwLock::new(AHashMap::default()));
        let tls = default.tls.clone().map(tls::CachedConnector::new);
        let mut router = Router::new();
        let referral: Arc<Referral> = Arc::new(default.clone().to_referral());
        router.add_referral(referral.clone());
        let (tx, rx) = watch::channel(referral);
        // Dropped immediately for a config that has no origin, which leaves
        // `rx` frozen on the value it was created with.
        crate::config::watch::follow(&default, tx);
        ResolverWrap(Arc::new(Mutex::new(ResolverWrapInner {
            router,
            desired_auth,
            default: rx,
            default_con: None,
            by_server: HashMap::new(),
            writer_addr,
            priority,
            secrets,
            tls,
            f_pool,
            fi_pool,
            ti_pool,
            phantom: PhantomData,
        })))
    }

    fn secrets(&self) -> Arc<RwLock<AHashMap<SocketAddr, u128>>> {
        Arc::clone(&self.0.lock().secrets)
    }

    async fn send(
        &self,
        batch: &GPooled<Vec<T>>,
    ) -> Result<(GPooled<PublisherTable>, GPooled<Vec<F>>)> {
        let mut referrals = 0;
        loop {
            let mut waiters = Vec::new();
            let (mut finished, mut res) = {
                let mut guard = self.0.lock();
                let inner = &mut *guard;
                inner.refresh_default();
                if inner.by_server.len() > MAX_REFERRALS {
                    inner.by_server.clear(); // a workable sledgehammer
                }
                for (r, batch) in inner.router.route_batch(&inner.ti_pool, batch) {
                    waiters.push(inner.send_to_server(r, batch))
                }
                (inner.fi_pool.take(), inner.f_pool.take())
            };
            let mut referral = false;
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

/// Track changes in the resolver cluster.
///
/// Used with `ResolverRead::check_changed()` to efficiently detect when
/// publishers register or unregister under a path without repeatedly listing.
#[derive(Debug, Clone)]
pub struct ChangeTracker {
    path: Path,
    by_resolver: AHashMap<SocketAddr, Z64>,
}

impl ChangeTracker {
    pub fn new(path: Path) -> Self {
        ChangeTracker { path, by_resolver: AHashMap::default() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Client for querying the resolver server (read operations).
///
/// Used by subscribers to resolve paths to publisher addresses, list published
/// paths, and query the namespace structure. Handles referrals and cluster
/// topology automatically.
#[derive(Debug, Clone)]
pub struct ResolverRead(ResolverWrap<ReadClient, ToRead, FromRead>);

impl ResolverRead {
    pub fn new(default: Config, desired_auth: DesiredAuth) -> Self {
        ResolverRead(ResolverWrap::new(
            default,
            desired_auth,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
            PublisherPriority::Normal,
            RAWFROMREADPOOL.clone(),
            FROMREADPOOL.clone(),
            TOREADPOOL.clone(),
        ))
    }

    /// Send the specified messages to the resolver and return answers in send order.
    pub async fn send(
        &self,
        batch: &GPooled<Vec<ToRead>>,
    ) -> Result<(GPooled<PublisherTable>, GPooled<Vec<FromRead>>)> {
        self.0.send(batch).await
    }

    /// Resolve the specified paths to publisher addresses.
    ///
    /// Results are in send order. Each [`PublisherRef`](crate::protocol::resolver::PublisherRef)
    /// in a result addresses the returned [`PublisherTable`] with a
    /// [`PublisherKey`] constructed from `Resolved::resolver` and
    /// `PublisherRef::id`.
    pub async fn resolve<I>(
        &self,
        batch: I,
    ) -> Result<(GPooled<PublisherTable>, GPooled<Vec<Resolved>>)>
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

    /// List immediate children of the specified path.
    ///
    /// Order is unspecified.
    pub async fn list(&self, path: Path) -> Result<GPooled<Vec<Path>>> {
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

    async fn send_and_aggregate<F: FnMut(FromRead) -> Result<GPooled<Vec<Referral>>>>(
        &self,
        message: ToRead,
        mut process_reply: F,
    ) -> Result<()> {
        let mut pending: LPooled<Vec<Option<Arc<Referral>>>> = LPooled::take();
        pending.push(None);
        let mut done: LPooled<AHashSet<Arc<Referral>>> = LPooled::take();
        let mut referral_cycles = 0;
        while pending.len() > 0 {
            let mut waiters = Vec::new();
            {
                let mut inner = self.0.0.lock();
                let default = inner.refresh_default();
                for referral in pending.drain(..) {
                    let referral = referral.unwrap_or_else(|| default.clone());
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

    /// List all paths in the cluster matching the specified globset.
    ///
    /// Returns a list of batches of paths. If your
    /// globset is configured to match only published paths, then the
    /// batches should be disjoint, otherwise there may be some
    /// duplicate structural elements.
    pub async fn list_matching(
        &self,
        globset: &GlobSet,
    ) -> Result<GPooled<Vec<GPooled<Vec<Path>>>>> {
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

    /// Check whether there have been any changes under the specified path.
    ///
    /// This checks all servers in the resolver cluster. A change in this context consists of:
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
                    let n = e.get_mut();
                    if **n != *cn.change_number {
                        res = true;
                    }
                    *n = cn.change_number;
                    Ok(cn.referrals)
                }
            },
            m => bail!("unexpected response to GetChangeNr, {:?}", m),
        })
        .await?;
        Ok(res)
    }

    /// Interpret `path` as a table and return it's description.
    ///
    /// The contents of the resolver server is a tree, however a
    /// tree's structure can describe a table if at a single level
    /// multiple paths are published that share common children. Then
    /// each level 1 path is a row, and the common children are the
    /// columns.
    ///
    /// # Example
    ///
    /// ```ignore
    /// /table/a/1
    /// /table/a/2
    /// /table/b/1
    /// /table/b/2
    /// ```
    ///
    /// is a table with two rows, a and b, and two columns 1 and 2.
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

/// Client for updating the resolver server (write operations).
///
/// Used by publishers to register and unregister published paths, including
/// default publishers. Handles authentication and referrals automatically.
#[derive(Debug, Clone)]
pub struct ResolverWrite(ResolverWrap<WriteClient, ToWrite, FromWrite>);

impl ResolverWrite {
    pub fn new(
        default: Config,
        desired_auth: DesiredAuth,
        writer_addr: SocketAddr,
        priority: PublisherPriority,
    ) -> Result<Self> {
        match &desired_auth {
            DesiredAuth::Local
            | DesiredAuth::Anonymous
            | DesiredAuth::Krb5 { .. }
            | DesiredAuth::Tls { identity: None } => (),
            DesiredAuth::Tls { identity: Some(id) } => match &default.tls {
                None => bail!("tls auth selected an no tls config"),
                Some(tls) => {
                    if !tls.identities.contains_key(id) {
                        bail!("specified identity is invalid")
                    }
                }
            },
        }
        Ok(ResolverWrite(ResolverWrap::new(
            default,
            desired_auth,
            writer_addr,
            priority,
            RAWFROMWRITEPOOL.clone(),
            FROMWRITEPOOL.clone(),
            TOWRITEPOOL.clone(),
        )))
    }

    /// Send the specified messages to the resolver and return responses.
    pub async fn send(
        &self,
        batch: &GPooled<Vec<ToWrite>>,
    ) -> Result<GPooled<Vec<FromWrite>>> {
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

    /// Publish a batch of paths to the resolver.
    pub async fn publish<I: IntoIterator<Item = Path>>(&self, batch: I) -> Result<()> {
        self.send_expect(batch, FromWrite::Published, ToWrite::Publish).await
    }

    /// Publish a batch of paths with optional flags.
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

    /// Publish a batch of default publisher paths to the resolver.
    pub async fn publish_default<I: IntoIterator<Item = Path>>(
        &self,
        batch: I,
    ) -> Result<()> {
        self.send_expect(batch, FromWrite::Published, ToWrite::PublishDefault).await
    }

    /// Publish a batch of default publisher paths with optional flags.
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

    /// Unpublish a batch of paths from the resolver.
    pub async fn unpublish<I: IntoIterator<Item = Path>>(&self, batch: I) -> Result<()> {
        self.send_expect(batch, FromWrite::Unpublished, ToWrite::Unpublish).await
    }

    /// Unpublish a batch of default publisher paths from the resolver.
    pub async fn unpublish_default<I: IntoIterator<Item = Path>>(
        &self,
        batch: I,
    ) -> Result<()> {
        self.send_expect(batch, FromWrite::Unpublished, ToWrite::UnpublishDefault).await
    }

    /// Clear all published paths from this publisher.
    ///
    // CR estokes: this is broken on complex clusters
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

    pub(crate) fn secrets(&self) -> Arc<RwLock<AHashMap<SocketAddr, u128>>> {
        self.0.secrets()
    }
}
