pub use crate::protocol::publisher::v1::{Id, Typ, Value, FromValue};
use crate::{
    auth::Permissions,
    channel::Channel,
    chars::Chars,
    config::Config,
    os::{self, Krb5Ctx, ServerCtx},
    path::Path,
    pool::{Pool, Pooled},
    protocol::publisher,
    resolver::{Auth, ResolverWrite},
    utils::{self, BatchItem, Batched, ChanId, ChanWrap, Addr},
};
use anyhow::{anyhow, Error, Result};
use bytes::Buf;
use crossbeam::queue::SegQueue;
use futures::{channel::mpsc as fmpsc, prelude::*, select_biased, stream::SelectAll};
use fxhash::FxBuildHasher;
use get_if_addrs::get_if_addrs;
use log::{debug, error, info};
use parking_lot::{Mutex, RwLock};
use rand::{self, Rng};
use std::{
    boxed::Box,
    collections::{hash_map::Entry, BTreeMap, BTreeSet, Bound, HashMap, HashSet},
    convert::From,
    default::Default,
    iter::{self, FromIterator},
    mem,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    ops::{Deref, DerefMut},
    result,
    str::FromStr,
    sync::{Arc, Weak},
    time::{Duration, SystemTime},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{channel, Receiver, Sender},
        oneshot,
    },
    task, time,
};

static MAX_CLIENTS: usize = 768;

#[derive(Debug)]
pub struct WriteRequest {
    pub id: Id,
    pub addr: SocketAddr,
    pub value: Value,
    pub send_result: Option<SendResult>,
}

lazy_static! {
    static ref BATCHES: Pool<Vec<WriteRequest>> = Pool::new(1000, 50000);
}

// The set of clients subscribed to a given value is hashconsed.
// Instead of having a seperate hash table for each published value,
// we can just keep a pointer to a set shared by other published
// values. Since in the case where we care about memory useage there
// are many more published values than clients we can save a lot of
// memory this way. Roughly the size of a hashmap, plus it's
// keys/values, replaced by 1 word.
type Subscribed = Arc<HashMap<Addr, Arc<SegQueue<ToClientMsg>>, FxBuildHasher>>;

struct ValLocked {
    current: Value,
    subscribed: Subscribed,
}

struct ValInner {
    id: Id,
    path: Path,
    publisher: PublisherWeak,
    locked: Mutex<ValLocked>,
}

impl Drop for ValInner {
    fn drop(&mut self) {
        if let Some(t) = self.publisher.upgrade() {
            let mut pb = t.0.lock();
            pb.by_path.remove(&self.path);
            pb.wait_clients.remove(&self.id);
            if let Some(chans) = pb.on_write.remove(&self.id) {
                for (_, c) in chans {
                    match pb.on_write_chans.entry(ChanWrap(c)) {
                        Entry::Vacant(_) => (),
                        Entry::Occupied(mut e) => {
                            e.get_mut().1.remove(&self.id);
                            if e.get().1.is_empty() {
                                e.remove();
                            }
                        }
                    }
                }
            }
            if !pb.to_publish.remove(&self.path) {
                pb.to_unpublish.insert(self.path.clone());
            }
            for q in self.locked.lock().subscribed.values() {
                q.push(ToClientMsg::Unpublish(self.id));
            }
        }
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

/// This represents a published value. Internally it is wrapped in an
/// Arc, so cloning it is free. When all references to a given
/// published value have been dropped it will be unpublished.  However
/// you must call flush before references to it will be removed from
/// the resolver server.
#[derive(Clone)]
pub struct Val(Arc<ValInner>);

impl Val {
    pub fn downgrade(&self) -> ValWeak {
        ValWeak(Arc::downgrade(&self.0))
    }

    /// Queue an update to the published value, it will not be sent
    /// out until you call `flush` on the publisher, however new
    /// subscribers will receive the last value passed to update when
    /// they subscribe regardless of flush. Multiple updates can be
    /// queued before flush is called, in which case on `flush`
    /// subscribers will receive all the queued values in order. If
    /// updates are queued on multiple different published values
    /// before `flush` is called, they will all be sent out as a batch
    /// and are guarenteed to arrive in the order `update` was called.
    pub fn update(&self, v: Value) {
        let mut inner = self.0.locked.lock();
        for q in inner.subscribed.values() {
            q.push(ToClientMsg::Val(self.0.id, v.clone()))
        }
        inner.current = v;
    }

    /// `update` the current value only if the new value is different
    /// from the existing one. Otherwise exactly the same as update.
    pub fn update_changed(&self, v: Value) {
        let mut inner = self.0.locked.lock();
        if v != inner.current {
            for q in inner.subscribed.values() {
                q.push(ToClientMsg::Val(self.0.id, v.clone()))
            }
            inner.current = v;
        }
    }

    /// Send `v` as an update ONLY to the specified subscriber, and do
    /// not update `current`. You can use this to implement a simple
    /// unicast side channel in parallel with the existing multicast
    /// `update` mechanism.
    ///
    /// One example use for this function is implementing a query
    /// response value, where the query is encoded in the name of the
    /// value (perhaps passed via publish_default), and the full
    /// response set is sent to each client as it subscribes.
    pub fn update_subscriber(&self, subscriber: &SocketAddr, v: Value) {
        let inner = self.0.locked.lock();
        if let Some(q) = inner.subscribed.get(subscriber) {
            q.push(ToClientMsg::Val(self.0.id, v));
        }
    }

    /// Register `tx` to receive writes. You can register multiple
    /// channels, and you can register the same channel on multiple
    /// `Val` objects. If no channels are registered to receive writes
    /// they will return an error to the subscriber.
    ///
    /// If the `send_result` struct member of `WriteRequest` is set
    /// then the client has requested that an explicit reply be made
    /// to the write. In that case the included `SendReply` object can
    /// be used to send the reply back to the write client. If the
    /// `SendReply` object is dropped without any reply being sent
    /// then `Value::Ok` will be sent. `SendReply::send` may only be
    /// called once, further calls will be silently ignored.
    ///
    /// If you no longer wish to accept writes, simply drop all
    /// registered channels.
    pub fn writes(&self, tx: fmpsc::Sender<Pooled<Vec<WriteRequest>>>) {
        if let Some(publisher) = self.0.publisher.upgrade() {
            let mut pb = publisher.0.lock();
            let e = pb
                .on_write_chans
                .entry(ChanWrap(tx.clone()))
                .or_insert_with(|| (ChanId::new(), HashSet::new()));
            e.1.insert(self.0.id);
            let id = e.0;
            let mut gc = Vec::new();
            let ow = pb.on_write.entry(self.0.id).or_insert_with(Vec::new);
            ow.retain(|(_, c)| {
                if c.is_closed() {
                    gc.push(ChanWrap(c.clone()));
                    false
                } else {
                    true
                }
            });
            ow.push((id, tx));
            for c in gc {
                pb.on_write_chans.remove(&c);
            }
        }
    }

    /// Register `tx` to receive a message when a new client
    /// subscribes to this value. If `include_existing` is true, then
    /// current subscribers will be sent to the channel immediatly,
    /// otherwise it will only receive new subscribers.
    pub fn subscribers(
        &self,
        include_existing: bool,
        tx: fmpsc::UnboundedSender<(Id, SocketAddr)>,
    ) {
        if let Some(publisher) = self.0.publisher.upgrade() {
            let mut inner = publisher.0.lock();
            inner
                .on_subscribe_chans
                .entry(self.0.id)
                .or_insert_with(Vec::new)
                .push(tx.clone());
            if include_existing {
                for a in self.0.locked.lock().subscribed.keys() {
                    let _: result::Result<_, _> = tx.unbounded_send((self.0.id, a.0));
                }
            }
        }
    }

    /// Unsubscribe the specified client.
    pub fn unsubscribe(&self, subscriber: &SocketAddr) {
        let inner = self.0.locked.lock();
        if let Some(q) = inner.subscribed.get(subscriber) {
            q.push(ToClientMsg::Unpublish(self.0.id));
        }
    }

    /// Get a copy of the current value
    pub fn current(&self) -> Value {
        self.0.locked.lock().current.clone()
    }

    /// Get the unique `Id` of this `Val`
    pub fn id(&self) -> Id {
        self.0.id
    }

    /// Get a reference to the `Path` of this published value.
    pub fn path(&self) -> &Path {
        &self.0.path
    }

    pub fn subscribed(&self) -> Vec<SocketAddr> {
        self.0.locked.lock().subscribed.keys().map(|a| a.0).collect()
    }
}

/// A weak reference to a published value.
#[derive(Clone)]
pub struct ValWeak(Weak<ValInner>);

impl ValWeak {
    pub fn upgrade(&self) -> Option<Val> {
        Weak::upgrade(&self.0).map(Val)
    }
}

/// A handle to the channel that will receive notifications about
/// subscriptions to paths in a subtree with a default publisher.
pub struct DefaultHandle {
    chan: fmpsc::UnboundedReceiver<(Path, oneshot::Sender<()>)>,
    path: Path,
    publisher: PublisherWeak,
}

impl Deref for DefaultHandle {
    type Target = fmpsc::UnboundedReceiver<(Path, oneshot::Sender<()>)>;

    fn deref(&self) -> &Self::Target {
        &self.chan
    }
}

impl DerefMut for DefaultHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.chan
    }
}

impl Drop for DefaultHandle {
    fn drop(&mut self) {
        if let Some(t) = self.publisher.upgrade() {
            let mut pb = t.0.lock();
            pb.default.remove(self.path.as_ref());
            pb.to_unpublish.insert(self.path.clone());
        }
    }
}

#[derive(Debug)]
enum ToClientMsg {
    Val(Id, Value),
    Unpublish(Id),
    Flush,
}

struct Client {
    flush_trigger: Sender<Option<Duration>>,
    msg_queue: Arc<SegQueue<ToClientMsg>>,
    subscribed: HashMap<Id, Permissions, FxBuildHasher>,
}

struct PublisherInner {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    clients: HashMap<SocketAddr, Client, FxBuildHasher>,
    hc_subscribed: HashMap<BTreeSet<Addr>, Subscribed, FxBuildHasher>,
    by_path: HashMap<Path, Id>,
    by_id: HashMap<Id, ValWeak, FxBuildHasher>,
    on_write_chans: HashMap<
        ChanWrap<Pooled<Vec<WriteRequest>>>,
        (ChanId, HashSet<Id>),
        FxBuildHasher,
    >,
    on_subscribe_chans:
        HashMap<Id, Vec<fmpsc::UnboundedSender<(Id, SocketAddr)>>, FxBuildHasher>,
    on_write: HashMap<
        Id,
        Vec<(ChanId, fmpsc::Sender<Pooled<Vec<WriteRequest>>>)>,
        FxBuildHasher,
    >,
    resolver: ResolverWrite,
    to_publish: HashSet<Path>,
    to_publish_default: HashSet<Path>,
    to_unpublish: HashSet<Path>,
    wait_clients: HashMap<Id, Vec<oneshot::Sender<()>>, FxBuildHasher>,
    wait_any_client: Vec<oneshot::Sender<()>>,
    default: BTreeMap<Path, fmpsc::UnboundedSender<(Path, oneshot::Sender<()>)>>,
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

    async fn shutdown(&mut self) {
        if self.cleanup() {
            let _ = self.resolver.clear().await;
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
/// - loopback (127.0.0.1, or ::1) is only allowed if all the resolvers are also loopback
/// - private addresses (192.168.0.0/16, 10.0.0.0/8,
///   172.16.0.0/12) are only allowed if all the resolvers are also
///   using private addresses.
///
/// As well as the above rules we will enumerate all the network
/// interface addresses present at startup time and check that the
/// specified bind address (or specification in case of Addr) matches
/// exactly 1 of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindCfg {
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
        match s.find("/") {
            None => Ok(BindCfg::Exact(s.parse()?)),
            Some(_) => {
                let mut parts = s.splitn(2, '/');
                let addr: IpAddr =
                    parts.next().ok_or_else(|| anyhow!("expected ip"))?.parse()?;
                let bits: u32 =
                    parts.next().ok_or_else(|| anyhow!("expected netmask"))?.parse()?;
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

impl BindCfg {
    fn select(&self) -> Result<IpAddr> {
        match self {
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

fn rand_port(current: u16) -> u16 {
    let mut rng = rand::thread_rng();
    current + rng.gen_range(0u16..10u16)
}

/// Publish values and centrally flush queued updates. Publisher is
/// internally wrapped in an Arc, so cloning it is virtually
/// free. When all references to to the publisher have been dropped
/// the publisher will shutdown the listener, and remove all published
/// paths from the resolver server.
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
        desired_auth: Auth,
        bind_cfg: BindCfg,
    ) -> Result<Publisher> {
        let ip = bind_cfg.select()?;
        utils::check_addr(ip, &resolver.addrs)?;
        let (addr, listener) = match bind_cfg {
            BindCfg::Exact(addr) => {
                let l = TcpListener::bind(&addr).await?;
                (l.local_addr()?, l)
            }
            BindCfg::Match { .. } => {
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
        let pb = Publisher(Arc::new(Mutex::new(PublisherInner {
            addr,
            stop: Some(stop),
            clients: HashMap::with_hasher(FxBuildHasher::default()),
            hc_subscribed: HashMap::with_hasher(FxBuildHasher::default()),
            by_path: HashMap::new(),
            by_id: HashMap::with_hasher(FxBuildHasher::default()),
            on_write_chans: HashMap::with_hasher(FxBuildHasher::default()),
            on_subscribe_chans: HashMap::with_hasher(FxBuildHasher::default()),
            on_write: HashMap::with_hasher(FxBuildHasher::default()),
            resolver,
            to_publish: HashSet::new(),
            to_publish_default: HashSet::new(),
            to_unpublish: HashSet::new(),
            wait_clients: HashMap::with_hasher(FxBuildHasher::default()),
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
    ///
    /// This function will return an error if other references to the
    /// publisher exist.
    pub async fn shutdown(self) -> Result<()> {
        match Arc::try_unwrap(self.0) {
            Err(_) => bail!("publisher is not unique"),
            Ok(mtx) => {
                let mut inner = Mutex::into_inner(mtx);
                Ok(inner.shutdown().await)
            }
        }
    }

    /// get the `SocketAddr` that publisher is bound to
    pub fn addr(&self) -> SocketAddr {
        self.0.lock().addr
    }

    /// Publish `Path` with initial value `init`. It is an error for
    /// the same publisher to publish the same path twice, however
    /// different publishers may publish a given path as many times as
    /// they like. Subscribers will then pick randomly among the
    /// advertised publishers when subscribing. See `subscriber`
    pub fn publish(&self, path: Path, init: Value) -> Result<Val> {
        let mut pb = self.0.lock();
        if !Path::is_absolute(&path) {
            bail!("can't publish to relative path")
        } else if pb.stop.is_none() {
            bail!("publisher is dead")
        } else if pb.by_path.contains_key(&path) {
            bail!("already published")
        } else {
            let subscribed = pb
                .hc_subscribed
                .entry(BTreeSet::new())
                .or_insert_with(|| {
                    Arc::new(HashMap::with_hasher(FxBuildHasher::default()))
                })
                .clone();
            let id = Id::new();
            let val = Val(Arc::new(ValInner {
                id,
                path: path.clone(),
                publisher: self.downgrade(),
                locked: Mutex::new(ValLocked { current: init, subscribed }),
            }));
            pb.by_id.insert(id, val.downgrade());
            if !pb.to_unpublish.remove(&path) {
                pb.to_publish.insert(path.clone());
            }
            pb.by_path.insert(path, id);
            Ok(val)
        }
    }

    /// Install a default publisher rooted at `base`. Once installed,
    /// any subscription request for a child of `base`, regardless if
    /// it doesn't exist in the resolver, will be routed to this
    /// publisher or one of it's peers in the case of multiple default
    /// publishers.
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
        let mut pb = self.0.lock();
        if !Path::is_absolute(base.as_ref()) {
            bail!("can't publish a relative path")
        } else if pb.stop.is_none() {
            bail!("publisher is dead")
        } else {
            if !pb.to_unpublish.remove(base.as_ref()) {
                if !pb.default.contains_key(base.as_ref()) {
                    pb.to_publish_default.insert(base.clone());
                }
            }
            let (tx, rx) = fmpsc::unbounded();
            pb.default.insert(base.clone(), tx);
            Ok(DefaultHandle { chan: rx, path: base, publisher: self.downgrade() })
        }
    }

    /// Flush initiates sending queued updates out to subscribers, and
    /// also sends all queued publish/unpublish operations to the
    /// resolver. The semantics of flush are such that any update
    /// queued before flush is called will go out when it is called,
    /// and any update queued after flush is called will only go out
    /// on the next flush.
    ///
    /// If you don't want to wait for the future you can just throw it
    /// away, `flush` triggers sending the data whether you await the
    /// future or not, unlike most futures. However if you never wait
    /// for any of the futures returned by flush there won't be any
    /// pushback, so a slow client could cause the publisher to
    /// consume arbitrary amounts of memory unless you set an
    /// aggressive timeout.
    ///
    /// If timeout is specified then any client that can't accept the
    /// update within the timeout duration will be disconnected.
    /// Otherwise flush will wait as long as necessary to flush the
    /// update to every client.
    pub async fn flush(&self, timeout: Option<Duration>) {
        let mut to_publish;
        let mut to_publish_default;
        let mut to_unpublish;
        let clients;
        let resolver = {
            let mut pb = self.0.lock();
            to_publish = mem::replace(&mut pb.to_publish, HashSet::new());
            to_publish_default = mem::replace(&mut pb.to_publish_default, HashSet::new());
            to_unpublish = mem::replace(&mut pb.to_unpublish, HashSet::new());
            clients = pb
                .clients
                .values()
                .map(|c| {
                    c.msg_queue.push(ToClientMsg::Flush);
                    c.flush_trigger.clone()
                })
                .collect::<Vec<_>>();
            pb.resolver.clone()
        };
        for client in clients {
            let _ = client.send(timeout).await;
        }
        if to_publish.len() > 0 {
            if let Err(e) = resolver.publish(to_publish.drain()).await {
                error!("failed to publish some paths {} will retry", e);
            }
        }
        if to_publish_default.len() > 0 {
            if let Err(e) = resolver.publish_default(to_publish_default.drain()).await {
                error!("failed to publish_default some paths {} will retry", e)
            }
        }
        if to_unpublish.len() > 0 {
            if let Err(e) = resolver.unpublish(to_unpublish.drain()).await {
                error!("failed to unpublish some paths {} will retry", e)
            }
        }
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

    /// Wait for at least one client to subscribe to the specified
    /// published value. Returns immediatly if there is a client, or
    /// if the published value is dead.
    pub async fn wait_client(&self, id: Id) {
        let wait = {
            let mut inner = self.0.lock();
            match inner.by_id.get(&id) {
                None => return,
                Some(ut) => match ut.upgrade() {
                    None => return,
                    Some(ut) => {
                        if ut.0.locked.lock().subscribed.len() > 0 {
                            return;
                        }
                        let (tx, rx) = oneshot::channel();
                        inner.wait_clients.entry(id).or_insert_with(Vec::new).push(tx);
                        rx
                    }
                },
            }
        };
        let _ = wait.await;
    }
}

const MAX_DEFERRED: usize = 1000000;
type DeferredSubs =
    Batched<SelectAll<Box<dyn Stream<Item = (Path, Permissions)> + Send + Sync + Unpin>>>;

fn subscribe(
    t: &mut PublisherInner,
    updates: &Arc<SegQueue<ToClientMsg>>,
    con: &mut Channel<ServerCtx>,
    addr: SocketAddr,
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
                        con.queue_send(&publisher::v1::From::NoSuchValue(path.clone()))?;
                        break;
                    }
                }
            }
        }
        Some(id) => {
            let id = *id;
            if let Some(ut) = t.by_id.get(&id).and_then(|v| v.upgrade()) {
                if let Some(cl) = t.clients.get_mut(&addr) {
                    cl.subscribed.insert(id, permissions);
                }
                let mut inner = ut.0.locked.lock();
                let subs = BTreeSet::from_iter(
                    iter::once(Addr::from(addr)).chain(inner.subscribed.keys().copied()),
                );
                match t.hc_subscribed.entry(subs) {
                    Entry::Occupied(e) => {
                        inner.subscribed = e.get().clone();
                    }
                    Entry::Vacant(e) => {
                        let mut s = HashMap::clone(&inner.subscribed);
                        s.insert(Addr::from(addr), Arc::clone(updates));
                        inner.subscribed = Arc::new(s);
                        e.insert(inner.subscribed.clone());
                    }
                }
                let m = publisher::v1::From::Subscribed(path, id, inner.current.clone());
                con.queue_send(&m)?;
                if let Some(chans) = t.on_subscribe_chans.get(&id) {
                    for chan in chans {
                        let _: result::Result<_, _> = chan.unbounded_send((id, addr));
                    }
                }
                if let Some(waiters) = t.wait_clients.remove(&id) {
                    for tx in waiters {
                        let _ = tx.send(());
                    }
                }
            }
        }
    }
    Ok(())
}

fn unsubscribe(t: &mut PublisherInner, addr: &SocketAddr, id: Id) {
    if let Some(ut) = t.by_id.get(&id).and_then(|v| v.upgrade()) {
        let addr = Addr::from(*addr);
        let mut inner = ut.0.locked.lock();
        let subs =
            BTreeSet::from_iter(inner.subscribed.keys().filter(|a| *a != &addr).copied());
        match t.hc_subscribed.entry(subs) {
            Entry::Occupied(e) => {
                inner.subscribed = e.get().clone();
            }
            Entry::Vacant(e) => {
                let mut h = HashMap::clone(&inner.subscribed);
                h.remove(&addr);
                inner.subscribed = Arc::new(h);
                e.insert(inner.subscribed.clone());
            }
        }
        if let Some(cl) = t.clients.get_mut(&addr.0) {
            cl.subscribed.remove(&id);
        }
    }
}

async fn handle_batch(
    t: &PublisherWeak,
    updates: &Arc<SegQueue<ToClientMsg>>,
    addr: &SocketAddr,
    msgs: impl Iterator<Item = publisher::v1::To>,
    con: &mut Channel<ServerCtx>,
    write_batches: &mut HashMap<
        ChanId,
        (Pooled<Vec<WriteRequest>>, fmpsc::Sender<Pooled<Vec<WriteRequest>>>),
        FxBuildHasher,
    >,
    secrets: &Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    auth: &Auth,
    now: u64,
    deferred_subs: &mut DeferredSubs,
) -> Result<()> {
    use crate::protocol::publisher::v1::{From, To::*};
    let mut wait_write_res = Vec::new();
    fn qwe(con: &mut Channel<ServerCtx>, id: Id, r: bool, m: &'static str) -> Result<()> {
        if r {
            let m = Value::Error(Chars::from(m));
            con.queue_send(&From::WriteResult(id, m))?
        }
        Ok(())
    }
    {
        let t_st = t.upgrade().ok_or_else(|| anyhow!("dead publisher"))?;
        let mut pb = t_st.0.lock();
        let secrets = secrets.read();
        let mut gc = false;
        let mut gc_on_write = Vec::new();
        for msg in msgs {
            match msg {
                Subscribe { path, resolver, timestamp, permissions, mut token } => {
                    gc = true;
                    match auth {
                        Auth::Anonymous => subscribe(
                            &mut *pb,
                            updates,
                            con,
                            *addr,
                            path,
                            Permissions::all(),
                            deferred_subs,
                        )?,
                        Auth::Krb5 { .. } => match secrets.get(&resolver) {
                            None => {
                                debug!("denied, no stored secret for {}", resolver);
                                con.queue_send(&From::Denied(path))?
                            }
                            Some(secret) => {
                                if token.len() < mem::size_of::<u64>() {
                                    bail!("error, token too short");
                                }
                                let salt = token.get_u64();
                                let expected = utils::make_sha3_token(
                                    Some(salt),
                                    &[
                                        &secret.to_be_bytes(),
                                        &timestamp.to_be_bytes(),
                                        &permissions.to_be_bytes(),
                                        path.as_bytes(),
                                    ],
                                );
                                let permissions = Permissions::from_bits(permissions)
                                    .ok_or_else(|| anyhow!("invalid permission bits"))?;
                                let age = std::cmp::max(
                                    u64::saturating_sub(now, timestamp),
                                    u64::saturating_sub(timestamp, now),
                                );
                                if age > 300
                                    || !permissions.contains(Permissions::SUBSCRIBE)
                                    || &*token != &expected[mem::size_of::<u64>()..]
                                {
                                    debug!("subscribe permission denied");
                                    con.queue_send(&From::Denied(path))?
                                } else {
                                    subscribe(
                                        &mut *pb,
                                        updates,
                                        con,
                                        *addr,
                                        path,
                                        permissions,
                                        deferred_subs,
                                    )?
                                }
                            }
                        },
                    }
                }
                Unsubscribe(id) => {
                    gc = true;
                    unsubscribe(&mut *pb, addr, id);
                    con.queue_send(&From::Unsubscribed(id))?;
                }
                Write(id, v, r) => match pb.clients.get(&addr) {
                    None => qwe(con, id, r, "cannot write to unsubscribed value")?,
                    Some(cl) => match cl.subscribed.get(&id) {
                        None => qwe(con, id, r, "cannot write to unsubscribed value")?,
                        Some(perms) => match perms.contains(Permissions::WRITE) {
                            false => qwe(con, id, r, "write permission denied")?,
                            true => match pb.on_write.get_mut(&id) {
                                None => qwe(con, id, r, "writes not accepted")?,
                                Some(ow) => {
                                    let send_result = if !r {
                                        None
                                    } else {
                                        ow.retain(|(_, c)| {
                                            if c.is_closed() {
                                                gc_on_write.push(ChanWrap(c.clone()));
                                                false
                                            } else {
                                                true
                                            }
                                        });
                                        if ow.len() == 0 {
                                            qwe(con, id, r, "writes not accepted")?;
                                            continue;
                                        }
                                        let (send_result, wait) = SendResult::new();
                                        wait_write_res.push((id, wait));
                                        Some(send_result)
                                    };
                                    for (cid, ch) in ow.iter() {
                                        let req = WriteRequest {
                                            id,
                                            addr: *addr,
                                            value: v.clone(),
                                            send_result: send_result.clone(),
                                        };
                                        write_batches
                                            .entry(*cid)
                                            .or_insert_with(|| {
                                                (BATCHES.take(), ch.clone())
                                            })
                                            .0
                                            .push(req)
                                    }
                                }
                            },
                        },
                    },
                },
            }
        }
        if gc {
            pb.hc_subscribed.retain(|_, v| Arc::get_mut(v).is_none());
        }
        for c in gc_on_write {
            pb.on_write_chans.remove(&c);
        }
    }
    for (_, (batch, mut sender)) in write_batches.drain() {
        let _ = sender.send(batch).await;
    }
    for (id, rx) in wait_write_res {
        match rx.await {
            Ok(v) => con.queue_send(&From::WriteResult(id, v))?,
            Err(_) => con.queue_send(&From::WriteResult(id, Value::Ok))?,
        }
    }
    Ok(())
}

const HB: Duration = Duration::from_secs(5);

fn client_arrived(publisher: &PublisherWeak) {
    if let Some(publisher) = publisher.upgrade() {
        let mut pb = publisher.0.lock();
        for tx in pb.wait_any_client.drain(..) {
            let _ = tx.send(());
        }
    }
}

async fn hello_client(
    publisher: &PublisherWeak,
    secrets: &Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    con: &mut Channel<ServerCtx>,
    auth: &Auth,
) -> Result<()> {
    use crate::protocol::publisher::v1::Hello::{self, *};
    debug!("hello_client");
    // negotiate protocol version
    con.send_one(&1u64).await?;
    let _ver: u64 = con.receive().await?;
    debug!("protocol version {}", _ver);
    let hello: Hello = con.receive().await?;
    debug!("hello_client received {:?}", hello);
    match hello {
        Anonymous => {
            con.send_one(&Anonymous).await?;
            client_arrived(publisher);
        }
        Token(tok) => match auth {
            Auth::Anonymous => bail!("authentication not supported"),
            Auth::Krb5 { upn, spn } => {
                let p = spn.as_ref().or(upn.as_ref()).map(|s| s.as_str());
                let ctx = os::create_server_ctx(p)?;
                let tok = ctx
                    .step(Some(&*tok))?
                    .map(|b| utils::bytes(&*b))
                    .ok_or_else(|| anyhow!("expected step to generate a token"))?;
                con.send_one(&Token(tok)).await?;
                con.set_ctx(ctx).await;
                client_arrived(publisher);
            }
        },
        ResolverAuthenticate(id, _) => {
            // slow down brute force attacks on the hashed secret
            time::sleep(Duration::from_millis(100)).await;
            info!("hello_client processing listener ownership check from resolver");
            let secret =
                secrets.read().get(&id).copied().ok_or_else(|| anyhow!("no secret"))?;
            let reply = utils::make_sha3_token(None, &[&(!secret).to_be_bytes()]);
            con.send_one(&ResolverAuthenticate(id, reply)).await?;
            bail!("resolver authentication complete");
        }
    }
    Ok(())
}

async fn client_loop(
    t: PublisherWeak,
    updates: Arc<SegQueue<ToClientMsg>>,
    secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    addr: SocketAddr,
    mut flushes: Receiver<Option<Duration>>,
    s: TcpStream,
    desired_auth: Auth,
) -> Result<()> {
    let mut con: Channel<ServerCtx> = Channel::new(s);
    let mut batch: Vec<publisher::v1::To> = Vec::new();
    let mut write_batches: HashMap<
        ChanId,
        (Pooled<Vec<WriteRequest>>, fmpsc::Sender<Pooled<Vec<WriteRequest>>>),
        FxBuildHasher,
    > = HashMap::with_hasher(FxBuildHasher::default());
    let mut hb = time::interval(HB);
    let mut msg_sent = false;
    let mut deferred_subs: DeferredSubs = Batched::new(SelectAll::new(), MAX_DEFERRED);
    let mut deferred_subs_batch: Vec<(Path, Permissions)> = Vec::new();
    // make sure the deferred subs stream never ends
    deferred_subs.inner_mut().push(Box::new(stream::pending()));
    hello_client(&t, &secrets, &mut con, &desired_auth).await?;
    loop {
        select_biased! {
            _ = hb.tick().fuse() => {
                if !msg_sent {
                    con.queue_send(&publisher::v1::From::Heartbeat)?;
                    con.flush().await?;
                }
                msg_sent = false;
            },
            s = deferred_subs.next() => match s {
                None => (),
                Some(BatchItem::InBatch(v)) => { deferred_subs_batch.push(v); }
                Some(BatchItem::EndBatch) => match t.upgrade() {
                    None => { deferred_subs_batch.clear(); }
                    Some(t) => {
                        {
                            let mut pb = t.0.lock();
                            for (path, perms) in deferred_subs_batch.drain(..) {
                                if !pb.by_path.contains_key(path.as_ref()) {
                                    let m = publisher::v1::From::NoSuchValue(path);
                                    con.queue_send(&m)?
                                } else {
                                    subscribe(
                                        &mut *pb, &updates, &mut con, addr,
                                        path, perms, &mut deferred_subs
                                    )?
                                }
                            }
                        }
                        con.flush().await?
                    }
                }
            },
            from_cl = con.receive_batch(&mut batch).fuse() => match from_cl {
                Err(e) => return Err(Error::from(e)),
                Ok(()) => {
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)?
                        .as_secs();
                    handle_batch(
                        &t, &updates, &addr, batch.drain(..), &mut con,
                        &mut write_batches, &secrets, &desired_auth, now,
                        &mut deferred_subs,
                    ).await?;
                    con.flush().await?
                }
            },
            to_cl = flushes.recv().fuse() => match to_cl {
                None => break Ok(()),
                Some(timeout) => {
                    while let Some(m) = updates.pop() {
                        msg_sent = true;
                        match m {
                            ToClientMsg::Val(id, v) => {
                                con.queue_send(&publisher::v1::From::Update(id, v))?;
                            }
                            ToClientMsg::Unpublish(id) => {
                                // handle this as if the client had requested it
                                batch.push(publisher::v1::To::Unsubscribe(id));
                            }
                            ToClientMsg::Flush => break,
                        }
                    }
                    if batch.len() > 0 {
                        let now = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)?
                            .as_secs();
                        handle_batch(
                            &t, &updates, &addr, batch.drain(..),
                            &mut con, &mut write_batches, &secrets,
                            &desired_auth, now, &mut deferred_subs,
                        ).await?;
                    }
                    let f = con.flush();
                    match timeout {
                        None => f.await?,
                        Some(d) => time::timeout(d, f).await??
                    }
                }
            },
        }
    }
}

async fn accept_loop(
    t: PublisherWeak,
    serv: TcpListener,
    stop: oneshot::Receiver<()>,
    desired_auth: Auth,
) {
    let mut stop = stop.fuse();
    loop {
        select_biased! {
            _ = stop => break,
            cl = serv.accept().fuse() => match cl {
                Err(e) => info!("accept error {}", e),
                Ok((s, addr)) => {
                    debug!("accepted client {:?}", addr);
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
                        let updates = Arc::new(SegQueue::new());
                        pb.clients.insert(addr, Client {
                            flush_trigger: tx,
                            msg_queue: updates.clone(),
                            subscribed: HashMap::with_hasher(FxBuildHasher::default()),
                        });
                        let desired_auth = desired_auth.clone();
                        task::spawn(async move {
                            let r = client_loop(
                                t_weak.clone(), updates, secrets, addr, rx, s, desired_auth
                            ).await;
                            info!("accept_loop client shutdown {:?}", r);
                            if let Some(t) = t_weak.upgrade() {
                                let mut pb = t.0.lock();
                                if let Some(cl) = pb.clients.remove(&addr) {
                                    for (id, _) in cl.subscribed {
                                        unsubscribe(&mut *pb, &addr, id);
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
