pub use crate::protocol::{
    publisher::Id,
    value::{FromValue, Typ, Value},
};
pub use crate::resolver_client::DesiredAuth;
use crate::{
    channel::{self, Channel, K5CtxWrap},
    chars::Chars,
    config::Config,
    pack::BoundedBytes,
    path::Path,
    pool::{Pool, Pooled},
    protocol::{self, publisher},
    resolver_client::ResolverWrite,
    resolver_server::{auth::Permissions, krb5_authentication},
    tls,
    utils::{self, BatchItem, Batched, ChanId, ChanWrap},
};
use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::{
    channel::{
        mpsc::{
            channel, unbounded, Receiver, Sender, UnboundedReceiver, UnboundedSender,
        },
        oneshot,
    },
    prelude::*,
    select_biased,
    stream::{FusedStream, SelectAll},
};
use fxhash::{FxHashMap, FxHashSet};
use get_if_addrs::get_if_addrs;
use log::{debug, error, info};
use parking_lot::{Mutex, RwLock};
use protocol::resolver::{AuthChallenge, HashMethod};
use rand::{self, Rng};
use std::{
    boxed::Box,
    collections::{hash_map::Entry, BTreeMap, BTreeSet, Bound, HashMap, HashSet},
    convert::From,
    default::Default,
    iter::{self, FromIterator},
    mem,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    pin::Pin,
    str::FromStr,
    sync::{Arc, Weak},
    time::{Duration, SystemTime},
};
use tokio::{
    net::{TcpListener, TcpStream},
    task, time,
};

/// Control how the publisher picks a bind address. The address we
/// give to the resolver server must be uniquely routable back to us,
/// otherwise clients will not be able to subscribe. In the
/// furtherance of this goal there are a number of address rules to
/// follow,
///
/// - no unspecified (0.0.0.0)
/// - no broadcast (255.255.255.255)
/// - no multicast addresses (224.0.0.0/8)
/// - no link local addresses (169.254.0.0/16)
/// - loopback (127.0.0.1, or ::1) is only allowed if the default resolver is local
/// - private addresses (192.168.0.0/16, 10.0.0.0/8, 172.16.0.0/12)
/// are only allowed if the default resolver is also using a private
/// address
///
/// As well as the above rules we will enumerate all the network
/// interface addresses present at startup time and check that the
/// specified bind address (or specification in case of Addr) matches
/// exactly 1 of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindCfg {
    /// Bind to 127.0.0.1
    Local,
    /// Bind to the interface who's address matches `addr` when masked
    /// with `netmask`, e.g.
    ///
    /// `192.168.0.0/16`
    ///
    /// will match interfaces with addresses 192.168.1.1, 192.168.10.234, ... etc,
    ///
    /// `192.168.0.0/24`
    ///
    /// will match interfaces with addresses 192.168.0.[1-254]
    ///
    /// # Examples
    /// ```
    /// use netidx::publisher::BindCfg;
    /// "ffff:1c00:2700:3c00::/64".parse::<BindCfg>().unwrap();
    /// "127.0.0.1/32".parse::<BindCfg>().unwrap();
    /// "192.168.2.0/24".parse::<BindCfg>().unwrap();
    /// ```
    Match { addr: IpAddr, netmask: IpAddr },

    /// Bind to the specifed `SocketAddr`, error if it is in use. If
    /// you want to OS to pick a port for you, use Exact with port
    /// 0. The ip address you specify must obey all the rules, and
    /// must be the ip address of one of the network interfaces
    /// present on the machine at the time of publisher creation.
    ///
    /// # Examples
    /// ```
    /// use netidx::publisher::BindCfg;
    /// "[ffff:1c00:2700:3c00::]:1234".parse::<BindCfg>().unwrap();
    /// "192.168.0.1:1234".parse::<BindCfg>().unwrap();
    /// ```
    Exact(SocketAddr),
}

impl FromStr for BindCfg {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim() == "local" {
            Ok(BindCfg::Local)
        } else {
            match s.find("/") {
                None => Ok(BindCfg::Exact(s.parse()?)),
                Some(_) => {
                    let mut parts = s.splitn(2, '/');
                    let addr: IpAddr =
                        parts.next().ok_or_else(|| anyhow!("expected ip"))?.parse()?;
                    let bits: u32 = parts
                        .next()
                        .ok_or_else(|| anyhow!("expected netmask"))?
                        .parse()?;
                    if parts.next().is_some() {
                        bail!("parse error, trailing garbage after netmask")
                    }
                    let netmask = match addr {
                        IpAddr::V4(_) => {
                            if bits > 32 {
                                bail!("invalid netmask");
                            }
                            IpAddr::V4(Ipv4Addr::from(u32::MAX.wrapping_shl(32 - bits)))
                        }
                        IpAddr::V6(_) => {
                            if bits > 128 {
                                bail!("invalid netmask");
                            }
                            IpAddr::V6(Ipv6Addr::from(u128::MAX.wrapping_shl(128 - bits)))
                        }
                    };
                    Ok(BindCfg::Match { addr, netmask })
                }
            }
        }
    }
}

impl BindCfg {
    fn select(&self) -> Result<IpAddr> {
        match self {
            BindCfg::Local => Ok(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            BindCfg::Exact(addr) => {
                if get_if_addrs()?.iter().any(|i| i.ip() == addr.ip()) {
                    Ok(addr.ip())
                } else {
                    bail!("no interface matches the bind address {:?}", addr);
                }
            }
            BindCfg::Match { addr, netmask } => {
                let selected = get_if_addrs()?
                    .iter()
                    .filter_map(|i| match (i.ip(), addr, netmask) {
                        (IpAddr::V4(ip), IpAddr::V4(addr), IpAddr::V4(nm)) => {
                            let masked = Ipv4Addr::from(
                                u32::from_be_bytes(ip.octets())
                                    & u32::from_be_bytes(nm.octets()),
                            );
                            if &masked == addr {
                                Some(IpAddr::V4(ip))
                            } else {
                                None
                            }
                        }
                        (IpAddr::V6(ip), IpAddr::V6(addr), IpAddr::V6(nm)) => {
                            let masked = Ipv6Addr::from(
                                u128::from_be_bytes(ip.octets())
                                    & u128::from_be_bytes(nm.octets()),
                            );
                            if &masked == addr {
                                Some(IpAddr::V6(ip))
                            } else {
                                None
                            }
                        }
                        (_, _, _) => None,
                    })
                    .collect::<Vec<_>>();
                if selected.len() == 1 {
                    Ok(selected[0])
                } else if selected.len() == 0 {
                    bail!("no interface matches {:?}", self);
                } else {
                    bail!("ambigous specification {:?} matches {:?}", self, selected);
                }
            }
        }
    }
}

atomic_id!(ClId);

static MAX_CLIENTS: usize = 768;

lazy_static! {
    static ref BATCHES: Pool<Vec<WriteRequest>> = Pool::new(100, 10_000);
    static ref TOPUB: Pool<HashMap<Path, Option<u32>>> = Pool::new(10, 10_000);
    static ref TOUPUB: Pool<HashSet<Path>> = Pool::new(5, 10_000);
    static ref TOUSUB: Pool<HashMap<Id, Subscribed>> = Pool::new(5, 10_000);
    static ref RAWBATCH: Pool<Vec<BatchMsg>> = Pool::new(100, 100_000);
    static ref UPDATES: Pool<Vec<publisher::From>> = Pool::new(100, 100_000);
    static ref RAWUNSUBS: Pool<Vec<(ClId, Id)>> = Pool::new(100, 100_000);
    static ref UNSUBS: Pool<Vec<Id>> = Pool::new(100, 100_000);
    static ref BATCH: Pool<FxHashMap<ClId, Update>> = Pool::new(100, 1000);

    // estokes 2021: This is reasonable because there will never be
    // that many publishers in a process. Since a publisher wraps
    // actual expensive OS resources, users will hit other constraints
    // (e.g. file descriptor exhaustion) long before N^2 drop becomes
    // a problem.
    //
    // Moreover publisher is architected such that there should not be
    // very many applications that require more than one publisher in
    // a process.
    //
    // The payback on the other hand is saving a word in EVERY
    // published value, and that adds up quite fast to a lot of saved
    // memory.
    static ref PUBLISHERS: Mutex<Vec<PublisherWeak>> = Mutex::new(Vec::new());
}

bitflags! {
    pub struct PublishFlags: u32 {
        /// if set, then subscribers will be forced to use an existing
        /// connection, if it exists, when subscribing to this value.
        ///
        /// Because the creation of connections is atomic, this flag
        /// guarantees that all subscriptions that can will use the
        /// same connection.
        ///
        /// The net effect is that a group of published values that,
        ///
        /// 1. all have this flag set
        /// 2. are published by multiple publishers
        ///
        /// will behave differently from the default, in that a
        /// subscriber that subscribes to all of them will connect to
        /// exactly one of the publishers.
        ///
        /// This can be important for control interfaces, and is used
        /// by the RPC protocol to ensure that all function parameters
        /// are written to the same publisher, even if a procedure is
        /// published by multiple publishers.
        const USE_EXISTING = 0x01;

        /// if set, then the publisher will destroy it's internal Val
        /// when the subscriber count transitions from 1 to 0. A
        /// message will be sent to the events channel signaling the
        /// destruction. updates to internally destroyed values will
        /// be silenty ignored. If the path is advertised the
        /// advertisement will not be removed.
        ///
        /// This flag is intended to make it possible to advertise a
        /// large sparse namespace where the actual values are
        /// retreivable from e.g. a database and would not all fit in
        /// memory at the same time.
        const DESTROY_ON_IDLE = 0x02;
    }
}

/// Used to signal a write result
#[derive(Clone, Debug)]
pub struct SendResult(Arc<Mutex<Option<oneshot::Sender<Value>>>>);

impl SendResult {
    fn new() -> (Self, oneshot::Receiver<Value>) {
        let (tx, rx) = oneshot::channel();
        (SendResult(Arc::new(Mutex::new(Some(tx)))), rx)
    }

    pub fn send(self, v: Value) {
        if let Some(s) = self.0.lock().take() {
            let _ = s.send(v);
        }
    }
}

#[derive(Debug)]
pub struct WriteRequest {
    /// the Id of the value being written
    pub id: Id,
    /// the path of the value being written
    pub path: Path,
    /// the unique id of the client requesting the write
    pub client: ClId,
    /// the value being written
    pub value: Value,
    pub send_result: Option<SendResult>,
}

#[derive(Debug, Clone, Copy)]
pub enum Event {
    Destroyed(Id),
    Subscribe(Id, ClId),
    Unsubscribe(Id, ClId),
}

struct Update {
    updates: Pooled<Vec<publisher::From>>,
    unsubscribes: Option<Pooled<Vec<Id>>>,
}

impl Update {
    fn new() -> Self {
        Self { updates: UPDATES.take(), unsubscribes: None }
    }
}

type MsgQ = Sender<(Option<Duration>, Update)>;

// The set of clients subscribed to a given value is hashconsed.
// Instead of having a seperate hash table for each published value,
// we can just keep a pointer to a set shared by other published
// values. Since in the case where we care about memory usage there
// are many more published values than clients we can save a lot of
// memory this way. Roughly the size of a hashmap, plus it's
// keys/values, replaced by 1 word.
type Subscribed = Arc<FxHashSet<ClId>>;

/// This represents a published value. When it is dropped the value
/// will be unpublished.
pub struct Val(Id);

impl Drop for Val {
    fn drop(&mut self) {
        PUBLISHERS.lock().retain(|t| match t.upgrade() {
            None => false,
            Some(t) => {
                t.0.lock().destroy_val(self.0);
                true
            }
        })
    }
}

impl Val {
    /// Queue an update to the published value in the specified
    /// batch. Multiple updates to multiple Vals can be queued in a
    /// batch before it is committed, and updates may be concurrently
    /// queued in different batches. Queuing updates in a batch has no
    /// effect until the batch is committed. On commit, subscribers
    /// will receive all the queued values in the batch in the order
    /// they were queued. If multiple batches have queued values, then
    /// subscribers will receive the queued values in the order the
    /// batches are committed.
    ///
    /// Clients that subscribe after an update is queued, but before
    /// the batch is committed will still receive the update.
    pub fn update(&self, batch: &mut UpdateBatch, v: Value) {
        batch.updates.push(BatchMsg::Update(None, self.0, v));
    }

    /// update the current value only if the new value is different
    /// from the existing one. Otherwise exactly the same as update.
    pub fn update_changed(&self, batch: &mut UpdateBatch, v: Value) {
        batch.updates.push(BatchMsg::UpdateChanged(self.0, v));
    }

    /// Queue sending `v` as an update ONLY to the specified
    /// subscriber, and do not update `current`.
    pub fn update_subscriber(&self, batch: &mut UpdateBatch, dst: ClId, v: Value) {
        batch.updates.push(BatchMsg::Update(Some(dst), self.0, v));
    }

    /// Queue unsubscribing the specified client. Like update, this
    /// will only take effect when the specified batch is committed.
    pub fn unsubscribe(&self, batch: &mut UpdateBatch, dst: ClId) {
        match &mut batch.unsubscribes {
            Some(u) => u.push((dst, self.0)),
            None => {
                let mut u = RAWUNSUBS.take();
                u.push((dst, self.0));
                batch.unsubscribes = Some(u);
            }
        }
    }

    /// Get the unique `Id` of this `Val`
    pub fn id(&self) -> Id {
        self.0
    }
}

/// A handle to the channel that will receive notifications about
/// subscriptions to paths in a subtree with a default publisher.
pub struct DefaultHandle {
    chan: UnboundedReceiver<(Path, oneshot::Sender<()>)>,
    path: Path,
    publisher: PublisherWeak,
}

impl Stream for DefaultHandle {
    type Item = (Path, oneshot::Sender<()>);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.chan).poll_next(cx)
    }
}

impl FusedStream for DefaultHandle {
    fn is_terminated(&self) -> bool {
        self.chan.is_terminated()
    }
}

impl DefaultHandle {
    /// Advertising is a middle way between fully publishing and a
    /// completely sparse namespace.
    ///
    /// When a path is advertised it exists in the resolver, just as a
    /// normally published value, but is not fully published on the
    /// publisher side, and as such it uses much less memory. When a
    /// user subscribes to an advertised path the request comes down
    /// the `DefaultHandle` just like a subscription to a default
    /// publisher.
    ///
    /// # Notes
    ///
    /// * Returns an error if the `path` is not under the base path of
    /// the default publisher.
    ///
    /// * DESTROY_ON_IDLE has no effect on advertisements, it is only
    /// relevant for publish.
    ///
    /// * Advertising is idempotent.
    ///
    /// * If the path is currently published, then calling advertise
    /// will merely record that it is now advertised. If the published
    /// value is destroyed it will not be removed from the resolver
    /// server. In this case the flags of the original publish will be
    /// the ones used, the flags passed to advertise will be ignored.
    pub fn advertise_with_flags(
        &self,
        mut flags: PublishFlags,
        path: Path,
    ) -> Result<()> {
        if !Path::is_parent(&*self.path, &path) {
            bail!("advertisements must be under the default publisher path")
        }
        if let Some(pb) = self.publisher.upgrade() {
            let mut pbl = pb.0.lock();
            let inserted = match pbl.advertised.get_mut(&self.path) {
                Some(set) => set.insert(path.clone()),
                None => {
                    pbl.advertised
                        .insert(self.path.clone(), iter::once(path.clone()).collect());
                    true
                }
            };
            if inserted && !pbl.by_path.contains_key(&path) {
                flags.remove(PublishFlags::DESTROY_ON_IDLE);
                let flags = if flags.is_empty() { None } else { Some(flags.bits) };
                pbl.to_unpublish.remove(&path);
                pbl.to_publish.insert(path, flags);
                pbl.trigger_publish()
            }
        }
        Ok(())
    }

    /// Advertise the specified path with an empty set of flags. see
    /// `advertise_with_flags`.
    pub fn advertise(&self, path: Path) -> Result<()> {
        self.advertise_with_flags(PublishFlags::empty(), path)
    }

    /// Stop advertising the specified path. If the path is currently
    /// published, this will merely record that it is no longer
    /// advertised. However if the path is not currently published
    /// then it will also be removed from the resolver server.
    ///
    /// if the path is not advertised then this function does nothing.
    pub fn remove_advertisement(&self, path: &Path) {
        if let Some(pb) = self.publisher.upgrade() {
            let mut pbl = pb.0.lock();
            let removed = match pbl.advertised.get_mut(&self.path) {
                None => false,
                Some(set) => {
                    let res = set.remove(path);
                    if set.is_empty() {
                        pbl.advertised.remove(&self.path);
                    }
                    res
                }
            };
            if removed && !pbl.by_path.contains_key(path) {
                pbl.to_unpublish.insert(path.clone());
                pbl.to_publish.remove(path);
                pbl.trigger_publish()
            }
        }
    }
}

impl Drop for DefaultHandle {
    fn drop(&mut self) {
        if let Some(t) = self.publisher.upgrade() {
            let mut pb = t.0.lock();
            pb.default.remove(self.path.as_ref());
            pb.to_unpublish_default.insert(self.path.clone());
            if let Some(paths) = pb.advertised.remove(&self.path) {
                for path in paths {
                    if !pb.by_path.contains_key(&path) {
                        pb.to_publish.remove(&path);
                        pb.to_unpublish.insert(path);
                    }
                }
            }
            pb.trigger_publish()
        }
    }
}

#[derive(Debug, Clone)]
enum BatchMsg {
    UpdateChanged(Id, Value),
    Update(Option<ClId>, Id, Value),
}

/// A batch of updates to Vals
#[must_use = "update batches do nothing unless committed"]
pub struct UpdateBatch {
    origin: Publisher,
    updates: Pooled<Vec<BatchMsg>>,
    unsubscribes: Option<Pooled<Vec<(ClId, Id)>>>,
}

impl UpdateBatch {
    /// return the number of queued updates in the batch
    pub fn len(&self) -> usize {
        self.updates.len()
    }

    /// merge all the updates from `other` into `self` assuming they
    /// are batches from the same publisher, if they are not, do
    /// nothing.
    pub fn merge_from(&mut self, other: &mut UpdateBatch) -> Result<()> {
        if Arc::as_ptr(&self.origin.0) != Arc::as_ptr(&other.origin.0) {
            bail!("can't merge batches from different publishers");
        } else {
            self.updates.extend(other.updates.drain(..));
            match (&mut self.unsubscribes, &mut other.unsubscribes) {
                (None, None) | (Some(_), None) => (),
                (None, Some(_)) => {
                    self.unsubscribes = other.unsubscribes.take();
                }
                (Some(l), Some(r)) => {
                    l.extend(r.drain(..));
                }
            }
            Ok(())
        }
    }

    /// Commit this batch, triggering all queued values to be
    /// sent. Any subscriber that can't accept all the updates within
    /// `timeout` will be disconnected.
    pub async fn commit(mut self, timeout: Option<Duration>) {
        let empty = self.updates.is_empty()
            && self.unsubscribes.as_ref().map(|v| v.len()).unwrap_or(0) == 0;
        if empty {
            return;
        }
        let fut = {
            let mut batch = BATCH.take();
            let mut pb = self.origin.0.lock();
            for m in self.updates.drain(..) {
                match m {
                    BatchMsg::Update(None, id, v) => {
                        if let Some(pbl) = pb.by_id.get_mut(&id) {
                            for cl in pbl.subscribed.iter() {
                                batch
                                    .entry(*cl)
                                    .or_insert_with(Update::new)
                                    .updates
                                    .push(publisher::From::Update(id, v.clone()));
                            }
                            pbl.current = v;
                        }
                    }
                    BatchMsg::UpdateChanged(id, v) => {
                        if let Some(pbl) = pb.by_id.get_mut(&id) {
                            if pbl.current != v {
                                for cl in pbl.subscribed.iter() {
                                    batch
                                        .entry(*cl)
                                        .or_insert_with(Update::new)
                                        .updates
                                        .push(publisher::From::Update(id, v.clone()));
                                }
                                pbl.current = v;
                            }
                        }
                    }
                    BatchMsg::Update(Some(cl), id, v) => batch
                        .entry(cl)
                        .or_insert_with(Update::new)
                        .updates
                        .push(publisher::From::Update(id, v)),
                }
            }
            if let Some(usubs) = &mut self.unsubscribes {
                for (cl, id) in usubs.drain(..) {
                    let update = batch.entry(cl).or_insert_with(Update::new);
                    match &mut update.unsubscribes {
                        Some(u) => u.push(id),
                        None => {
                            let mut u = UNSUBS.take();
                            u.push(id);
                            update.unsubscribes = Some(u);
                        }
                    }
                }
            }
            future::join_all(
                batch
                    .drain()
                    .filter_map(|(cl, batch)| {
                        pb.clients.get(&cl).map(move |cl| (cl.msg_queue.clone(), batch))
                    })
                    .map(|(mut q, batch)| async move {
                        let _: Result<_, _> = q.send((timeout, batch)).await;
                    }),
            )
        };
        fut.await;
    }
}

struct Client {
    msg_queue: MsgQ,
    subscribed: FxHashMap<Id, Permissions>,
}

pub struct Published {
    current: Value,
    subscribed: Subscribed,
    path: Path,
    aliases: Option<Box<FxHashSet<Path>>>,
}

impl Published {
    pub fn current(&self) -> &Value {
        &self.current
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn subscribed(&self) -> &FxHashSet<ClId> {
        &self.subscribed
    }
}

struct PublisherInner {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    clients: FxHashMap<ClId, Client>,
    hc_subscribed: FxHashMap<BTreeSet<ClId>, Subscribed>,
    by_path: HashMap<Path, Id>,
    by_id: FxHashMap<Id, Published>,
    destroy_on_idle: FxHashSet<Id>,
    on_write_chans: FxHashMap<ChanWrap<Pooled<Vec<WriteRequest>>>, (ChanId, HashSet<Id>)>,
    on_event_chans: Vec<UnboundedSender<Event>>,
    on_write: FxHashMap<Id, Vec<(ChanId, Sender<Pooled<Vec<WriteRequest>>>)>>,
    resolver: ResolverWrite,
    advertised: HashMap<Path, HashSet<Path>>,
    to_publish: Pooled<HashMap<Path, Option<u32>>>,
    to_publish_default: Pooled<HashMap<Path, Option<u32>>>,
    to_unpublish: Pooled<HashSet<Path>>,
    to_unpublish_default: Pooled<HashSet<Path>>,
    to_unsubscribe: Pooled<HashMap<Id, Subscribed>>,
    publish_triggered: bool,
    trigger_publish: UnboundedSender<Option<oneshot::Sender<()>>>,
    wait_clients: FxHashMap<Id, Vec<oneshot::Sender<()>>>,
    wait_any_client: Vec<oneshot::Sender<()>>,
    default: BTreeMap<Path, UnboundedSender<(Path, oneshot::Sender<()>)>>,
}

impl PublisherInner {
    fn cleanup(&mut self) -> bool {
        match mem::replace(&mut self.stop, None) {
            None => false,
            Some(stop) => {
                let _ = stop.send(());
                self.clients.clear();
                self.by_id.clear();
                true
            }
        }
    }

    fn is_advertised(&self, path: &Path) -> bool {
        self.advertised
            .iter()
            .any(|(b, set)| Path::is_parent(&**b, &**path) && set.contains(path))
    }

    pub fn publish(&mut self, id: Id, flags: PublishFlags, path: Path) -> Result<()> {
        if !Path::is_absolute(&path) {
            bail!("can't publish a relative path")
        }
        if self.stop.is_none() {
            bail!("publisher is dead")
        }
        if self.by_path.contains_key(&path) {
            bail!("already published")
        }
        self.by_path.insert(path.clone(), id);
        self.to_unpublish.remove(&path);
        self.to_publish
            .insert(path.clone(), if flags.is_empty() { None } else { Some(flags.bits) });
        self.trigger_publish();
        Ok(())
    }

    fn unpublish(&mut self, path: &Path) {
        self.by_path.remove(path);
        if !self.is_advertised(path) {
            self.to_publish.remove(path);
            self.to_unpublish.insert(path.clone());
            self.trigger_publish();
        }
    }

    fn destroy_val(&mut self, id: Id) {
        if let Some(pbl) = self.by_id.remove(&id) {
            let path = pbl.path;
            for path in iter::once(&path).chain(pbl.aliases.iter().flat_map(|v| v.iter()))
            {
                self.unpublish(path)
            }
            self.wait_clients.remove(&id);
            if let Some(chans) = self.on_write.remove(&id) {
                for (_, c) in chans {
                    match self.on_write_chans.entry(ChanWrap(c)) {
                        Entry::Vacant(_) => (),
                        Entry::Occupied(mut e) => {
                            e.get_mut().1.remove(&id);
                            if e.get().1.is_empty() {
                                e.remove();
                            }
                        }
                    }
                }
            }
            self.send_event(Event::Destroyed(id));
            if pbl.subscribed.len() > 0 {
                self.to_unsubscribe.insert(id, pbl.subscribed);
            }
        }
    }

    fn send_event(&mut self, event: Event) {
        self.on_event_chans.retain(|chan| chan.unbounded_send(event).is_ok());
    }

    fn trigger_publish(&mut self) {
        if !self.publish_triggered {
            self.publish_triggered = true;
            let _: Result<_, _> = self.trigger_publish.unbounded_send(None);
        }
    }
}

impl Drop for PublisherInner {
    fn drop(&mut self) {
        if self.cleanup() {
            let resolver = self.resolver.clone();
            tokio::spawn(async move {
                let _ = resolver.clear().await;
            });
        }
    }
}

#[derive(Clone)]
struct PublisherWeak(Weak<Mutex<PublisherInner>>);

impl PublisherWeak {
    fn upgrade(&self) -> Option<Publisher> {
        Weak::upgrade(&self.0).map(|r| Publisher(r))
    }
}

fn rand_port(current: u16) -> u16 {
    let mut rng = rand::thread_rng();
    current + rng.gen_range(0u16..10u16)
}

/// Publish values. Publisher is internally wrapped in an Arc, so
/// cloning it is virtually free. When all references to to the
/// publisher have been dropped the publisher will shutdown the
/// listener, and remove all published paths from the resolver server.
#[derive(Clone)]
pub struct Publisher(Arc<Mutex<PublisherInner>>);

impl Publisher {
    fn downgrade(&self) -> PublisherWeak {
        PublisherWeak(Arc::downgrade(&self.0))
    }

    /// Create a new publisher using the specified resolver, desired
    /// auth, and bind config.
    pub async fn new(
        resolver: Config,
        desired_auth: DesiredAuth,
        bind_cfg: BindCfg,
    ) -> Result<Publisher> {
        let ip = bind_cfg.select()?;
        utils::check_addr(ip, &resolver.addrs)?;
        let (addr, listener) = match bind_cfg {
            BindCfg::Exact(addr) => {
                let l = TcpListener::bind(&addr).await?;
                (l.local_addr()?, l)
            }
            BindCfg::Match { .. } | BindCfg::Local => {
                let mkaddr = |ip: IpAddr, port: u16| -> Result<SocketAddr> {
                    Ok((ip, port)
                        .to_socket_addrs()?
                        .next()
                        .ok_or_else(|| anyhow!("socketaddrs bug"))?)
                };
                let mut port = 5000;
                loop {
                    if port >= 32768 {
                        bail!("couldn't allocate a port");
                    }
                    port = rand_port(port);
                    let addr = mkaddr(ip, port)?;
                    match TcpListener::bind(&addr).await {
                        Ok(l) => break (l.local_addr()?, l),
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::AddrInUse {
                                bail!(e)
                            }
                        }
                    }
                }
            }
        };
        let resolver = ResolverWrite::new(resolver, desired_auth.clone(), addr);
        let (stop, receive_stop) = oneshot::channel();
        let (tx_trigger, rx_trigger) = unbounded();
        let pb = Publisher(Arc::new(Mutex::new(PublisherInner {
            addr,
            stop: Some(stop),
            clients: HashMap::default(),
            hc_subscribed: HashMap::default(),
            by_path: HashMap::new(),
            by_id: HashMap::default(),
            destroy_on_idle: HashSet::default(),
            on_write_chans: HashMap::default(),
            on_event_chans: Vec::new(),
            on_write: HashMap::default(),
            resolver,
            advertised: HashMap::new(),
            to_publish: TOPUB.take(),
            to_publish_default: TOPUB.take(),
            to_unpublish: TOUPUB.take(),
            to_unpublish_default: TOUPUB.take(),
            to_unsubscribe: TOUSUB.take(),
            publish_triggered: false,
            trigger_publish: tx_trigger,
            wait_clients: HashMap::default(),
            wait_any_client: Vec::new(),
            default: BTreeMap::new(),
        })));
        task::spawn({
            let pb_weak = pb.downgrade();
            async move {
                accept_loop(pb_weak.clone(), listener, receive_stop, desired_auth).await;
                info!("accept loop shutdown");
            }
        });
        task::spawn({
            let pb_weak = pb.downgrade();
            async move {
                publish_loop(pb_weak, rx_trigger).await;
                info!("publish loop shutdown")
            }
        });
        PUBLISHERS.lock().push(pb.downgrade());
        Ok(pb)
    }

    /// Perform a clean shutdown of the publisher, remove all
    /// published paths from the resolver server, shutdown the
    /// listener, and close the connection to all clients. Dropping
    /// all references to the publisher also calls this function,
    /// however because async Drop is not yet implemented it is not
    /// guaranteed that a clean shutdown will be achieved before the
    /// Runtime itself is Dropped, as such it is necessary to call
    /// this function directly in the case you are tearing down the
    /// whole program. In the case where you have multiple publishers
    /// in your process, or you are for some reason tearing down the
    /// publisher but will continue to run async jobs on the same
    /// Runtime, then there is no need to call this function, you can
    /// just Drop all references to the Publisher.
    pub async fn shutdown(self) {
        let resolver = {
            let mut inner = self.0.lock();
            inner.cleanup();
            inner.resolver.clone()
        };
        let _: Result<_> = resolver.clear().await;
    }

    /// get the `SocketAddr` that publisher is bound to
    pub fn addr(&self) -> SocketAddr {
        self.0.lock().addr
    }

    /// Publish `Path` with initial value `init` and flags `flags`. It
    /// is an error for the same publisher to publish the same path
    /// twice, however different publishers may publish a given path
    /// as many times as they like. Subscribers will then pick
    /// randomly among the advertised publishers when subscribing. See
    /// `subscriber`
    pub fn publish_with_flags(
        &self,
        mut flags: PublishFlags,
        path: Path,
        init: Value,
    ) -> Result<Val> {
        let id = Id::new();
        let destroy_on_idle = flags.contains(PublishFlags::DESTROY_ON_IDLE);
        flags.remove(PublishFlags::DESTROY_ON_IDLE);
        let mut pb = self.0.lock();
        pb.publish(id, flags, path.clone())?;
        let subscribed = pb
            .hc_subscribed
            .entry(BTreeSet::new())
            .or_insert_with(|| Arc::new(HashSet::default()))
            .clone();
        pb.by_id.insert(
            id,
            Published { current: init, subscribed, path: path.clone(), aliases: None },
        );
        if destroy_on_idle {
            pb.destroy_on_idle.insert(id);
        }
        Ok(Val(id))
    }

    /// Create an alias to an already published value at `path`. This
    /// takes much less memory than publishing the same value twice at
    /// different paths. Just as with publishing `path` cannot already
    /// be published by this publisher, and you must still have
    /// permission to publish at `path` in the resolver. When the val
    /// is dropped all aliases for it will be cleaned up. All flags
    /// are supported except `DESTROY_ON_IDLE`, it will be ignored. If
    /// you wish the val to be destroyed on idle you must set
    /// `DESTROY_ON_IDLE` as part of the initial publish operation.
    pub fn alias_with_flags(
        &self,
        id: Id,
        mut flags: PublishFlags,
        path: Path,
    ) -> Result<()> {
        flags.remove(PublishFlags::DESTROY_ON_IDLE);
        let mut pb = self.0.lock();
        if !pb.by_id.contains_key(&id) {
            bail!("no such value published by this publisher")
        }
        pb.publish(id, flags, path.clone())?;
        let v = pb.by_id.get_mut(&id).unwrap();
        match &mut v.aliases {
            Some(a) => {
                a.insert(path);
            }
            None => {
                let mut set = HashSet::default();
                set.insert(path);
                v.aliases = Some(Box::new(set))
            }
        }
        Ok(())
    }

    /// Publish `Path` with initial value `init` and no flags. It is
    /// an error for the same publisher to publish the same path
    /// twice, however different publishers may publish a given path
    /// as many times as they like. Subscribers will then pick
    /// randomly among the advertised publishers when subscribing. See
    /// `subscriber`
    pub fn publish(&self, path: Path, init: Value) -> Result<Val> {
        self.publish_with_flags(PublishFlags::empty(), path, init)
    }

    /// Create an alias for an already published path
    pub fn alias(&self, id: Id, path: Path) -> Result<()> {
        self.alias_with_flags(id, PublishFlags::empty(), path)
    }

    /// remove the specified alias for `val` if it exists
    pub fn remove_alias(&self, id: Id, path: &Path) {
        let mut pb = self.0.lock();
        if let Some(pbv) = pb.by_id.get_mut(&id) {
            if let Some(al) = &mut pbv.aliases {
                if al.remove(path) {
                    pb.unpublish(path)
                }
            }
        }
    }

    /// remove all aliases (if any) for the specified value
    pub fn remove_all_aliases(&self, id: Id) {
        let mut pb = self.0.lock();
        if let Some(pbv) = pb.by_id.get_mut(&id) {
            if let Some(mut al) = pbv.aliases.take() {
                for path in al.drain() {
                    pb.unpublish(&path)
                }
            }
        }
    }

    /// Install a default publisher rooted at `base` with flags
    /// `flags`. Once installed, any subscription request for a child
    /// of `base`, regardless if it doesn't exist in the resolver,
    /// will be routed to this publisher or one of it's peers in the
    /// case of multiple default publishers.
    ///
    /// You must listen for requests on the returned channel handle,
    /// and if they are valid, you should publish the requested value,
    /// and signal the subscriber by sending () to the oneshot channel
    /// that comes with the request. In the case the request is not
    /// valid, just send to the oneshot channel and the subscriber
    /// will be told the value doesn't exist.
    ///
    /// This functionality is useful if, for example, you have a huge
    /// namespace and you know your subscribers will only want small
    /// parts of it, but you can't predict ahead of time which
    /// parts. It can also be used to implement e.g. a database query
    /// by appending the escaped query to the base path, with the
    /// added bonus that the result will be automatically cached and
    /// distributed to anyone making the same query again.
    ///
    /// # notes
    ///
    /// At the moment none of the `PublishFlags` are relevant to
    /// default publishers.
    pub fn publish_default_with_flags(
        &self,
        flags: PublishFlags,
        base: Path,
    ) -> Result<DefaultHandle> {
        if !Path::is_absolute(base.as_ref()) {
            bail!("can't publish a relative path")
        }
        let (tx, rx) = unbounded();
        let mut pb = self.0.lock();
        if pb.stop.is_none() {
            bail!("publisher is dead")
        }
        pb.to_unpublish.remove(base.as_ref());
        pb.to_publish_default
            .insert(base.clone(), if flags.is_empty() { None } else { Some(flags.bits) });
        pb.default.insert(base.clone(), tx);
        pb.trigger_publish();
        Ok(DefaultHandle { chan: rx, path: base, publisher: self.downgrade() })
    }

    /// Install a default publisher rooted at `base` with no
    /// flags. Once installed, any subscription request for a child of
    /// `base`, regardless if it doesn't exist in the resolver, will
    /// be routed to this publisher or one of it's peers in the case
    /// of multiple default publishers.
    ///
    /// You must listen for requests on the returned channel handle,
    /// and if they are valid, you should publish the requested value,
    /// and signal the subscriber by sending () to the oneshot channel
    /// that comes with the request. In the case the request is not
    /// valid, just send to the oneshot channel and the subscriber
    /// will be told the value doesn't exist.
    ///
    /// This functionality is useful if, for example, you have a huge
    /// namespace and you know your subscribers will only want small
    /// parts of it, but you can't predict ahead of time which
    /// parts. It can also be used to implement e.g. a database query
    /// by appending the escaped query to the base path, with the
    /// added bonus that the result will be automatically cached and
    /// distributed to anyone making the same query again.
    pub fn publish_default(&self, base: Path) -> Result<DefaultHandle> {
        self.publish_default_with_flags(PublishFlags::empty(), base)
    }

    /// Start a new update batch. Updates are queued in the batch (see
    /// `Val::update`), and then the batch can be either discarded, or
    /// committed. If discarded then none of the updates will have any
    /// effect, otherwise once committed the queued updates will be
    /// sent out to subscribers and also will effect the current value
    /// given to new subscribers.
    ///
    /// Multiple batches may be started concurrently.
    pub fn start_batch(&self) -> UpdateBatch {
        UpdateBatch { origin: self.clone(), updates: RAWBATCH.take(), unsubscribes: None }
    }

    /// Wait until all previous publish or unpublish commands have
    /// been processed by the resolver server. e.g. if you just
    /// published 100 values, and you want to know when they have been
    /// uploaded to the resolver, you can call `flushed`.
    pub async fn flushed(&self) {
        let (tx, rx) = oneshot::channel();
        let _: Result<_, _> = self.0.lock().trigger_publish.unbounded_send(Some(tx));
        let _ = rx.await;
    }

    /// Returns the number of subscribers subscribing to at least one value.
    pub fn clients(&self) -> usize {
        self.0.lock().clients.len()
    }

    /// Wait for at least one client to subscribe to at least one
    /// value. Returns immediately if there is already a client.
    pub async fn wait_any_client(&self) {
        let wait = {
            let mut inner = self.0.lock();
            if inner.clients.len() > 0 {
                return;
            } else {
                let (tx, rx) = oneshot::channel();
                inner.wait_any_client.push(tx);
                rx
            }
        };
        let _ = wait.await;
    }

    /// Wait for any new client to connect. This will always wait for
    /// a new client, even if there are clients connected.
    pub async fn wait_any_new_client(&self) {
        let (tx, rx) = oneshot::channel();
        self.0.lock().wait_any_client.push(tx);
        let _ = rx.await;
    }

    /// Wait for at least one client to subscribe to the specified
    /// published value. Returns immediatly if there is a client, or
    /// if the published value has been dropped.
    pub async fn wait_client(&self, id: Id) {
        let wait = {
            let mut inner = self.0.lock();
            match inner.by_id.get(&id) {
                None => return,
                Some(ut) => {
                    if ut.subscribed.len() > 0 {
                        return;
                    }
                    let (tx, rx) = oneshot::channel();
                    inner.wait_clients.entry(id).or_insert_with(Vec::new).push(tx);
                    rx
                }
            }
        };
        let _ = wait.await;
    }

    /// Retreive the Id of path if it is published
    pub fn id<S: AsRef<str>>(&self, path: S) -> Option<Id> {
        self.0.lock().by_path.get(path.as_ref()).map(|id| *id)
    }

    /// Get the `Path` of a published value.
    pub fn path(&self, id: Id) -> Option<Path> {
        self.0.lock().by_id.get(&id).map(|pbl| pbl.path.clone())
    }

    /// Return all the aliases for the specified `id`
    pub fn aliases(&self, id: Id) -> Vec<Path> {
        let pb = self.0.lock();
        match pb.by_id.get(&id) {
            None => vec![],
            Some(pbv) => match &pbv.aliases {
                None => vec![],
                Some(al) => al.iter().cloned().collect(),
            },
        }
    }

    /// Get a copy of the current value of a published `Val`
    pub fn current(&self, id: &Id) -> Option<Value> {
        self.0.lock().by_id.get(&id).map(|p| p.current.clone())
    }

    /// Get a list of clients subscribed to a published `Val`
    pub fn subscribed(&self, id: &Id) -> Vec<ClId> {
        self.0
            .lock()
            .by_id
            .get(&id)
            .map(|p| p.subscribed.iter().copied().collect::<Vec<_>>())
            .unwrap_or_else(Vec::new)
    }

    /// Put the list of clients subscribed to a published `Val` into
    /// the specified collection.
    pub fn put_subscribed(&self, id: &Id, into: &mut impl Extend<ClId>) {
        if let Some(p) = self.0.lock().by_id.get(&id) {
            into.extend(p.subscribed.iter().copied())
        }
    }

    /// Get the number of clients subscribed to a published `Val`
    pub fn subscribed_len(&self, id: &Id) -> usize {
        self.0.lock().by_id.get(&id).map(|p| p.subscribed.len()).unwrap_or(0)
    }

    /// Register `tx` to receive writes to the specified published
    /// value. You can register multiple channels, and you can
    /// register the same channel on multiple ids. If no channels are
    /// registered to receive writes for an id, then an attempt to
    /// write to that id will return an error to the subscriber.
    ///
    /// If the `send_result` struct member of `WriteRequest` is set
    /// then the client has requested that an explicit reply be made
    /// to the write. In that case the included `SendReply` object can
    /// be used to send the reply back to the write client. If the
    /// `SendReply` object is dropped without any reply being sent
    /// then `Value::Ok` will be sent. `SendReply::send` may only be
    /// called once, further calls will be silently ignored.
    ///
    /// If you no longer wish to accept writes for an id you can drop
    /// all registered channels, or call `stop_writes`.
    pub fn writes(&self, id: Id, tx: Sender<Pooled<Vec<WriteRequest>>>) {
        let mut pb = self.0.lock();
        if pb.by_id.contains_key(&id) {
            let e = pb
                .on_write_chans
                .entry(ChanWrap(tx.clone()))
                .or_insert_with(|| (ChanId::new(), HashSet::new()));
            e.1.insert(id);
            let cid = e.0;
            let mut gc = Vec::new();
            let ow = pb.on_write.entry(id).or_insert_with(Vec::new);
            ow.retain(|(_, c)| {
                if c.is_closed() {
                    gc.push(ChanWrap(c.clone()));
                    false
                } else {
                    true
                }
            });
            ow.push((cid, tx));
            for c in gc {
                pb.on_write_chans.remove(&c);
            }
        }
    }

    /// Stop accepting writes to the specified id
    pub fn stop_writes(&self, id: Id) {
        let mut pb = self.0.lock();
        pb.on_write.remove(&id);
    }

    /// Register `tx` to receive a message about publisher events
    ///
    /// if you don't want to receive events on a given channel anymore
    /// you can just drop it.
    pub fn events(&self, tx: UnboundedSender<Event>) {
        self.0.lock().on_event_chans.push(tx)
    }
}

const MAX_DEFERRED: usize = 1000000;
type DeferredSubs =
    Batched<SelectAll<Box<dyn Stream<Item = (Path, Permissions)> + Send + Sync + Unpin>>>;

fn subscribe(
    t: &mut PublisherInner,
    con: &mut Channel,
    client: ClId,
    path: Path,
    permissions: Permissions,
    deferred_subs: &mut DeferredSubs,
) -> Result<()> {
    match t.by_path.get(&path) {
        None => {
            let mut r = t.default.range_mut::<str, (Bound<&str>, Bound<&str>)>((
                Bound::Unbounded,
                Bound::Included(path.as_ref()),
            ));
            loop {
                match r.next_back() {
                    Some((base, chan))
                        if path.starts_with(base.as_ref())
                            && deferred_subs.inner().len() < MAX_DEFERRED =>
                    {
                        let (tx, rx) = oneshot::channel();
                        if let Ok(()) = chan.unbounded_send((path.clone(), tx)) {
                            let path = path.clone();
                            let s = rx.map(move |_| (path, permissions));
                            deferred_subs.inner_mut().push(Box::new(s.into_stream()));
                            break;
                        }
                    }
                    _ => {
                        con.queue_send(&publisher::From::NoSuchValue(path.clone()))?;
                        break;
                    }
                }
            }
        }
        Some(id) => {
            let id = *id;
            if let Some(ut) = t.by_id.get_mut(&id) {
                if let Some(cl) = t.clients.get_mut(&client) {
                    cl.subscribed.insert(id, permissions);
                }
                let subs = BTreeSet::from_iter(
                    iter::once(client).chain(ut.subscribed.iter().copied()),
                );
                match t.hc_subscribed.entry(subs) {
                    Entry::Occupied(e) => {
                        ut.subscribed = Arc::clone(e.get());
                    }
                    Entry::Vacant(e) => {
                        let mut s = HashSet::clone(&ut.subscribed);
                        s.insert(client);
                        ut.subscribed = Arc::new(s);
                        e.insert(Arc::clone(&ut.subscribed));
                    }
                }
                let m = publisher::From::Subscribed(path, id, ut.current.clone());
                con.queue_send(&m)?;
                if let Some(waiters) = t.wait_clients.remove(&id) {
                    for tx in waiters {
                        let _ = tx.send(());
                    }
                }
                t.send_event(Event::Subscribe(id, client));
            }
        }
    }
    Ok(())
}

fn unsubscribe(t: &mut PublisherInner, client: ClId, id: Id) {
    if let Some(ut) = t.by_id.get_mut(&id) {
        let subs =
            BTreeSet::from_iter(ut.subscribed.iter().filter(|a| *a != &client).copied());
        match t.hc_subscribed.entry(subs) {
            Entry::Occupied(e) => {
                ut.subscribed = e.get().clone();
            }
            Entry::Vacant(e) => {
                let mut h = HashSet::clone(&ut.subscribed);
                h.remove(&client);
                ut.subscribed = Arc::new(h);
                e.insert(Arc::clone(&ut.subscribed));
            }
        }
        let nsubs = ut.subscribed.len();
        if let Some(cl) = t.clients.get_mut(&client) {
            cl.subscribed.remove(&id);
        }
        t.send_event(Event::Unsubscribe(id, client));
        if nsubs == 0 && t.destroy_on_idle.remove(&id) {
            t.destroy_val(id)
        }
    }
}

fn write(
    t: &mut PublisherInner,
    con: &mut Channel,
    client: ClId,
    gc_on_write: &mut Vec<ChanWrap<Pooled<Vec<WriteRequest>>>>,
    wait_write_res: &mut Vec<(Id, oneshot::Receiver<Value>)>,
    write_batches: &mut FxHashMap<
        ChanId,
        (Pooled<Vec<WriteRequest>>, Sender<Pooled<Vec<WriteRequest>>>),
    >,
    id: Id,
    v: Value,
    r: bool,
) -> Result<()> {
    macro_rules! or_qwe {
        ($v:expr, $m:expr) => {
            match $v {
                Some(v) => v,
                None => {
                    if r {
                        let m = Value::Error(Chars::from($m));
                        con.queue_send(&From::WriteResult(id, m))?
                    }
                    return Ok(());
                }
            }
        };
    }
    use protocol::publisher::From;
    let cl = or_qwe!(t.clients.get(&client), "cannot write to unsubscribed value");
    let perms = or_qwe!(cl.subscribed.get(&id), "cannot write to unsubscribed value");
    if !perms.contains(Permissions::WRITE) {
        or_qwe!(None, "write permission denied")
    }
    let ow = or_qwe!(t.on_write.get_mut(&id), "writes not accepted");
    ow.retain(|(_, c)| {
        if c.is_closed() {
            gc_on_write.push(ChanWrap(c.clone()));
            false
        } else {
            true
        }
    });
    if ow.len() == 0 {
        or_qwe!(None, "writes not accepted");
    }
    let send_result = if !r {
        None
    } else {
        let (send_result, wait) = SendResult::new();
        wait_write_res.push((id, wait));
        Some(send_result)
    };
    for (cid, ch) in ow.iter() {
        if let Some(pbv) = t.by_id.get(&id) {
            let req = WriteRequest {
                id,
                path: pbv.path.clone(),
                client,
                value: v.clone(),
                send_result: send_result.clone(),
            };
            write_batches
                .entry(*cid)
                .or_insert_with(|| (BATCHES.take(), ch.clone()))
                .0
                .push(req)
        }
    }
    Ok(())
}

fn check_token(
    token: Bytes,
    now: u64,
    secret: u128,
    timestamp: u64,
    permissions: u32,
    path: &Path,
) -> Result<(bool, Permissions)> {
    if token.len() < mem::size_of::<u64>() {
        bail!("error, token too short");
    }
    let expected = utils::make_sha3_token(&[
        &secret.to_be_bytes(),
        &timestamp.to_be_bytes(),
        &permissions.to_be_bytes(),
        path.as_bytes(),
    ]);
    let permissions = Permissions::from_bits(permissions)
        .ok_or_else(|| anyhow!("invalid permission bits"))?;
    let age = std::cmp::max(
        u64::saturating_sub(now, timestamp),
        u64::saturating_sub(timestamp, now),
    );
    let valid = age <= 300
        && permissions.contains(Permissions::SUBSCRIBE)
        && &*token == &expected;
    Ok((valid, permissions))
}

const HB: Duration = Duration::from_secs(5);

const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

struct ClientCtx {
    desired_auth: DesiredAuth,
    client: ClId,
    publisher: PublisherWeak,
    secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
    batch: Vec<publisher::To>,
    write_batches:
        FxHashMap<ChanId, (Pooled<Vec<WriteRequest>>, Sender<Pooled<Vec<WriteRequest>>>)>,
    deferred_subs: DeferredSubs,
    deferred_subs_batch: Vec<(Path, Permissions)>,
    wait_write_res: Vec<(Id, oneshot::Receiver<Value>)>,
    gc_on_write: Vec<ChanWrap<Pooled<Vec<WriteRequest>>>>,
    msg_sent: bool,
    tls_ctx: tls::CachedAcceptor,
}

impl ClientCtx {
    fn new(
        client: ClId,
        secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
        publisher: PublisherWeak,
        desired_auth: DesiredAuth,
        tls_ctx: tls::CachedAcceptor,
    ) -> ClientCtx {
        let mut deferred_subs: DeferredSubs =
            Batched::new(SelectAll::new(), MAX_DEFERRED);
        deferred_subs.inner_mut().push(Box::new(stream::pending()));
        ClientCtx {
            desired_auth,
            client,
            publisher,
            secrets,
            batch: Vec::new(),
            write_batches: HashMap::default(),
            deferred_subs,
            deferred_subs_batch: Vec::new(),
            wait_write_res: Vec::new(),
            gc_on_write: Vec::new(),
            msg_sent: false,
            tls_ctx,
        }
    }

    fn client_arrived(&mut self) {
        if let Some(publisher) = self.publisher.upgrade() {
            let mut pb = publisher.0.lock();
            for tx in pb.wait_any_client.drain(..) {
                let _ = tx.send(());
            }
        }
    }

    // CR estokes: Implement periodic rekeying to improve security
    async fn hello(&mut self, con: &mut TcpStream) -> Result<Channel> {
        use protocol::publisher::Hello;
        static NO: &str = "authentication mechanism not supported";
        debug!("hello_client");
        channel::write_raw(con, &2u64).await?;
        if channel::read_raw::<u64>(con).await? != 2 {
            bail!("incompatible protocol version")
        }
        let hello: Hello = channel::read_raw(con).await?;
        debug!("hello_client received {:?}", hello);
        match hello {
            Hello::Anonymous => {
                channel::write_raw(&mut con, &Hello::Anonymous).await?;
                self.client_arrived();
                Ok(Channel::new(None, con))
            }
            Hello::Local => {
                channel::write_raw(&mut con, &Hello::Local).await?;
                self.client_arrived();
                Ok(Channel::new(None, con))
            }
            Hello::Krb5 => match &self.desired_auth {
                DesiredAuth::Anonymous | DesiredAuth::Tls { .. } => bail!(NO),
                DesiredAuth::Local => {
                    channel::write_raw(&mut con, &Hello::Local).await?;
                    self.client_arrived();
                    Ok(Channel::new(None, con))
                }
                DesiredAuth::Krb5 { upn: _, spn } => {
                    let spn = spn.as_ref().map(|s| s.as_str());
                    let ctx = krb5_authentication(HELLO_TIMEOUT, spn, &mut con).await?;
                    let mut con = Channel::new(Some(ctx), con);
                    con.send_one(&Hello::Krb5).await?;
                    self.client_arrived();
                    Ok(con)
                }
            },
            Hello::Tls => match &self.desired_auth {
                DesiredAuth::Anonymous | DesiredAuth::Krb5 { .. } => bail!(NO),
                DesiredAuth::Local => {
                    channel::write_raw(&mut con, &Hello::Local).await?;
                    self.client_arrived();
                    Ok(Channel::new(None, con))
                }
                DesiredAuth::Tls {
                    name,
                    root_certificates,
                    certificate,
                    private_key,
                } => {
                    let ctx = task::block_in_place(|| {
                        self.tls_ctx.load(&root_certificates, &certificate, &private_key)
                    })?;
                    let mut tls = time::timeout(HELLO_TIMEOUT, ctx.accept(con))?;
                    let mut con = Channel::new(None, tls);
                    con.send_one(&Hello::Tls).await?;
                    self.client_arrived();
                    Ok(con)
                }
            },
            Hello::ResolverAuthenticate(id) => {
                info!("hello_client processing listener ownership check from resolver");
                let mut con = Channel::new(None, con);
                let secret = self
                    .secrets
                    .read()
                    .get(&id)
                    .copied()
                    .ok_or_else(|| anyhow!("no secret"))?;
                let challenge: AuthChallenge = con.receive().await?;
                if challenge.hash_method != HashMethod::Sha3_512 {
                    bail!("requested hash method not supported")
                }
                let reply = utils::make_sha3_token(&[
                    &challenge.challenge.to_be_bytes(),
                    &secret.to_be_bytes(),
                ]);
                con.send_one(&BoundedBytes::<4096>(reply)).await?;
                // prevent fishing for the key
                time::sleep(Duration::from_secs(1)).await;
                bail!("resolver authentication complete");
            }
        }
    }

    async fn handle_deferred_sub(
        &mut self,
        con: &mut Channel,
        s: Option<BatchItem<(Path, Permissions)>>,
    ) -> Result<()> {
        match s {
            None => (),
            Some(BatchItem::InBatch(v)) => {
                self.deferred_subs_batch.push(v);
            }
            Some(BatchItem::EndBatch) => match self.publisher.upgrade() {
                None => {
                    self.deferred_subs_batch.clear();
                }
                Some(t) => {
                    {
                        let mut pb = t.0.lock();
                        for (path, perms) in self.deferred_subs_batch.drain(..) {
                            if !pb.by_path.contains_key(path.as_ref()) {
                                let m = publisher::From::NoSuchValue(path);
                                con.queue_send(&m)?
                            } else {
                                subscribe(
                                    &mut *pb,
                                    con,
                                    self.client,
                                    path,
                                    perms,
                                    &mut self.deferred_subs,
                                )?
                            }
                        }
                    }
                    con.flush().await?
                }
            },
        }
        Ok(())
    }

    fn handle_batch_inner(&mut self, con: &mut Channel) -> Result<()> {
        use protocol::publisher::{From, To::*};
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
        let t_st = self.publisher.upgrade().ok_or_else(|| anyhow!("dead publisher"))?;
        let mut pb = t_st.0.lock();
        let secrets = self.secrets.read();
        let mut gc = false;
        for msg in self.batch.drain(..) {
            match msg {
                Subscribe { path, resolver, timestamp, permissions, token } => {
                    gc = true;
                    match self.desired_auth {
                        DesiredAuth::Anonymous => subscribe(
                            &mut *pb,
                            con,
                            self.client,
                            path,
                            Permissions::all(),
                            &mut self.deferred_subs,
                        )?,
                        DesiredAuth::Krb5 { .. }
                        | DesiredAuth::Local
                        | DesiredAuth::Tls { .. } => match secrets.get(&resolver) {
                            None => {
                                debug!("denied, no stored secret for {}", resolver);
                                con.queue_send(&From::Denied(path))?
                            }
                            Some(secret) => {
                                let (valid, permissions) = check_token(
                                    token,
                                    now,
                                    *secret,
                                    timestamp,
                                    permissions,
                                    &path,
                                )?;
                                if !valid {
                                    debug!("subscribe permission denied");
                                    con.queue_send(&From::Denied(path))?
                                } else {
                                    subscribe(
                                        &mut *pb,
                                        con,
                                        self.client,
                                        path,
                                        permissions,
                                        &mut self.deferred_subs,
                                    )?
                                }
                            }
                        },
                    }
                }
                Write(id, v, r) => write(
                    &mut *pb,
                    con,
                    self.client,
                    &mut self.gc_on_write,
                    &mut self.wait_write_res,
                    &mut self.write_batches,
                    id,
                    v,
                    r,
                )?,
                Unsubscribe(id) => {
                    gc = true;
                    unsubscribe(&mut *pb, self.client, id);
                    con.queue_send(&From::Unsubscribed(id))?;
                }
            }
        }
        if gc {
            pb.hc_subscribed.retain(|_, v| Arc::get_mut(v).is_none());
        }
        for c in self.gc_on_write.drain(..) {
            pb.on_write_chans.remove(&c);
        }
        Ok(())
    }

    async fn handle_batch(&mut self, con: &mut Channel) -> Result<()> {
        use protocol::publisher::From;
        self.handle_batch_inner(con)?;
        for (_, (batch, mut sender)) in self.write_batches.drain() {
            let _ = sender.send(batch).await;
        }
        for (id, rx) in self.wait_write_res.drain(..) {
            match rx.await {
                Ok(v) => con.queue_send(&From::WriteResult(id, v))?,
                Err(_) => con.queue_send(&From::WriteResult(id, Value::Ok))?,
            }
        }
        Ok(con.flush().await?)
    }

    async fn flush_with_timeout(
        &mut self,
        con: &mut Channel,
        timeout: Option<Duration>,
    ) -> Result<()> {
        self.msg_sent = true;
        let f = con.flush();
        match timeout {
            None => f.await?,
            Some(d) => time::timeout(d, f).await??,
        }
        Ok(())
    }

    async fn handle_updates(
        &mut self,
        con: &mut Channel,
        (timeout, mut up): (Option<Duration>, Update),
    ) -> Result<()> {
        use publisher::To;
        for m in up.updates.drain(..) {
            if let Err(e) = con.queue_send(&m) {
                if con.bytes_queued() > 0 {
                    self.flush_with_timeout(timeout, con).await?;
                    con.queue_send(&m)?;
                } else {
                    return Err(e);
                }
            }
        }
        if let Some(usubs) = &mut up.unsubscribes {
            for id in usubs.drain(..) {
                self.batch.push(To::Unsubscribe(id));
            }
        }
        if self.batch.len() > 0 {
            self.handle_batch(con).await?;
        }
        if con.bytes_queued() > 0 {
            self.flush_with_timeout(timeout, con).await?
        }
        Ok(())
    }

    async fn run(
        mut self,
        mut con: TcpStream,
        updates: Receiver<(Option<Duration>, Update)>,
    ) -> Result<()> {
        let mut updates = updates.fuse();
        let mut hb = time::interval(HB);
        // make sure the deferred subs stream never ends
        let mut con = time::timeout(HELLO_TIMEOUT, self.hello(&mut con)).await??;
        loop {
            select_biased! {
                _ = hb.tick().fuse() => {
                    if !self.msg_sent {
                        con.queue_send(&publisher::From::Heartbeat)?;
                        con.flush().await?;
                    }
                    self.msg_sent = false;
                },
                s = self.deferred_subs.next() => self.handle_deferred_sub(&mut con, s).await?,
                r = con.receive_batch(&mut self.batch).fuse() => match r {
                    Err(e) => return Err(Error::from(e)),
                    Ok(()) => self.handle_batch(&mut con).await?
                },
                u = updates.next() => match u {
                    None => break Ok(()),
                    Some(u) => self.handle_updates(&mut con, u).await?,
                },
            }
        }
    }
}

async fn accept_loop(
    t: PublisherWeak,
    serv: TcpListener,
    stop: oneshot::Receiver<()>,
    desired_auth: DesiredAuth,
) {
    let mut stop = stop.fuse();
    let tls_ctx = tls::CachedAcceptor::new();
    loop {
        select_biased! {
            _ = stop => break,
            cl = serv.accept().fuse() => match cl {
                Err(e) => info!("accept error {}", e),
                Ok((s, addr)) => {
                    debug!("accepted client {:?}", addr);
                    let clid = ClId::new();
                    let t_weak = t.clone();
                    let t = match t.upgrade() {
                        None => return,
                        Some(t) => t
                    };
                    let mut pb = t.0.lock();
                    let secrets = pb.resolver.secrets();
                    if pb.clients.len() < MAX_CLIENTS {
                        let (tx, rx) = channel(3);
                        try_cf!("nodelay", continue, s.set_nodelay(true));
                        pb.clients.insert(clid, Client {
                            msg_queue: tx,
                            subscribed: HashMap::default(),
                        });
                        let desired_auth = desired_auth.clone();
                        let tls_ctx = tls_ctx.clone();
                        task::spawn(async move {
                            let ctx = ClientCtx::new(
                                clid,
                                secrets,
                                t_weak.clone(),
                                desired_auth,
                                tls_ctx,
                            );
                            let r = ctx.run(rx, s).await;
                            info!("accept_loop client shutdown {:?}", r);
                            if let Some(t) = t_weak.upgrade() {
                                let mut pb = t.0.lock();
                                if let Some(cl) = pb.clients.remove(&clid) {
                                    for (id, _) in cl.subscribed {
                                        unsubscribe(&mut *pb, clid, id);
                                    }
                                    pb.hc_subscribed.retain(|_, v| {
                                        Arc::get_mut(v).is_none()
                                    });
                                }
                            }
                        });
                    }
                }
            },
        }
    }
}

async fn publish_loop(
    publisher: PublisherWeak,
    mut trigger_rx: UnboundedReceiver<Option<oneshot::Sender<()>>>,
) {
    while let Some(reply) = trigger_rx.next().await {
        if let Some(publisher) = publisher.upgrade() {
            let mut to_publish;
            let mut to_publish_default;
            let mut to_unpublish;
            let mut to_unpublish_default;
            let mut to_unsubscribe;
            let resolver = {
                let mut pb = publisher.0.lock();
                to_publish = mem::replace(&mut pb.to_publish, TOPUB.take());
                to_publish_default =
                    mem::replace(&mut pb.to_publish_default, TOPUB.take());
                to_unpublish = mem::replace(&mut pb.to_unpublish, TOUPUB.take());
                to_unpublish_default =
                    mem::replace(&mut pb.to_unpublish_default, TOUPUB.take());
                to_unsubscribe = mem::replace(&mut pb.to_unsubscribe, TOUSUB.take());
                pb.publish_triggered = false;
                pb.resolver.clone()
            };
            if to_publish.len() > 0 {
                if let Err(e) = resolver.publish_with_flags(to_publish.drain()).await {
                    error!("failed to publish some paths {} will retry", e);
                }
            }
            if to_publish_default.len() > 0 {
                if let Err(e) =
                    resolver.publish_default_with_flags(to_publish_default.drain()).await
                {
                    error!("failed to publish_default some paths {} will retry", e)
                }
            }
            if to_unpublish.len() > 0 {
                if let Err(e) = resolver.unpublish(to_unpublish.drain()).await {
                    error!("failed to unpublish some paths {} will retry", e)
                }
            }
            if to_unpublish_default.len() > 0 {
                if let Err(e) =
                    resolver.unpublish_default(to_unpublish_default.drain()).await
                {
                    error!("failed to unpublish default some paths {} will retry", e)
                }
            }
            if to_unsubscribe.len() > 0 {
                let mut usubs = RAWUNSUBS.take();
                for (id, subs) in to_unsubscribe.drain() {
                    for cl in subs.iter() {
                        usubs.push((*cl, id));
                    }
                }
                let mut batch = publisher.start_batch();
                batch.unsubscribes = Some(usubs);
                batch.commit(None).await;
            }
        }
        if let Some(reply) = reply {
            let _ = reply.send(());
        }
    }
}
