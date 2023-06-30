mod server;
pub use crate::protocol::{
    publisher::Id,
    value::{FromValue, Typ, Value},
};
pub use crate::resolver_client::DesiredAuth;
use crate::{
    config::Config,
    path::Path,
    pool::{Pool, Pooled},
    protocol::{publisher, resolver::UserInfo},
    resolver_client::ResolverWrite,
    resolver_server::auth::Permissions,
    tls,
    utils::{self, ChanId, ChanWrap},
};
use anyhow::{anyhow, Error, Result};
use futures::{
    channel::{
        mpsc::{unbounded, Sender, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    prelude::*,
    stream::FusedStream,
};
use fxhash::{FxHashMap, FxHashSet};
use if_addrs::get_if_addrs;
use log::{error, info};
use parking_lot::Mutex;
use rand::{self, Rng};
use std::{
    boxed::Box,
    collections::{hash_map::Entry, BTreeMap, BTreeSet, HashMap, HashSet},
    convert::{From, Into, TryInto},
    default::Default,
    iter, mem,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    pin::Pin,
    result,
    str::FromStr,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{net::TcpListener, task};

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

    /// If you have a public ip that will always route traffic to the machine
    /// the publisher is running on, but isn't actually visible on that machine.
    /// The publisher will put the public ip in the resolver server, but will bind
    /// to the matching private ip.
    ///
    /// `54.32.224.1@172.23.112.0/24`
    ///
    /// will bind the listener to any address in the 172.23.112.* subnet, but will
    /// tell the resolver server that it's address is 54.32.224.1.
    ///
    /// # Examples
    /// ```
    /// use netidx::publisher::BindCfg;
    /// "54.32.224.1@172.23.112.0/24".parse::<BindCfg>().unwrap();
    /// "54.32.224.1@0.0.0.0/32".parse::<BindCfg>().unwrap();
    /// ```
    Elastic { public: IpAddr, private: IpAddr, netmask: IpAddr },

    /// If you have a public ip and port that are set up to forward to
    /// a private ip and port (e.g. you have a NAT of some kind). Then
    /// you can use this directive to ensure the publisher puts the public
    /// ip and port in the resolver, but binds to the private ip and port.
    ///
    /// # Examples
    /// ```
    /// use netidx::publisher::BindCfg;
    /// "54.32.224.1:1234@172.31.224.4:5001".parse::<BindCfg>().unwrap();
    /// ```
    ElasticExact { public: SocketAddr, private: SocketAddr },

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

impl Default for BindCfg {
    fn default() -> Self {
        BindCfg::Local
    }
}

impl FromStr for BindCfg {
    type Err = Error;
    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match Self::parse_str(s) {
            Ok(t) => Ok(t),
            Err(e) => bail!("failed to parse '{}', {}", s, e),
        }
    }
}

impl BindCfg {
    fn parse_str(s: &str) -> Result<Self> {
        fn addr_and_netmask(s: &str) -> Result<(IpAddr, IpAddr)> {
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
            Ok((addr, netmask))
        }
        fn parse_elastic(s: &str) -> Result<BindCfg> {
            let mut parts = s.splitn(2, '@');
            let public = parts.next().ok_or_else(|| anyhow!("expected a public ip"))?;
            match public.parse::<SocketAddr>() {
                Ok(public) => {
                    let private = parts
                        .next()
                        .ok_or_else(|| anyhow!("expected private ip:port"))?
                        .parse::<SocketAddr>()?;
                    Ok(BindCfg::ElasticExact { public, private })
                }
                Err(_) => {
                    let public = public.parse::<IpAddr>()?;
                    let (private, netmask) = addr_and_netmask(
                        parts
                            .next()
                            .ok_or_else(|| anyhow!("expected private addr/netmask"))?,
                    )?;
                    Ok(BindCfg::Elastic { public, private, netmask })
                }
            }
        }
        if s.trim() == "local" {
            Ok(BindCfg::Local)
        } else {
            match s.find("/") {
                None => match s.find("@") {
                    None => Ok(BindCfg::Exact(s.parse()?)),
                    Some(_) => parse_elastic(s),
                },
                Some(_) => match s.find("@") {
                    None => {
                        let (addr, netmask) = addr_and_netmask(s)?;
                        Ok(BindCfg::Match { addr, netmask })
                    }
                    Some(_) => parse_elastic(s),
                },
            }
        }
    }

    fn select_local_ip(&self, addr: &IpAddr, netmask: &IpAddr) -> Result<IpAddr> {
        // this may or may not be allowed, that will be checked later
        match addr {
            IpAddr::V4(a) => {
                if a.is_unspecified() {
                    return Ok(*addr);
                }
            }
            IpAddr::V6(a) => {
                if a.is_unspecified() {
                    return Ok(*addr);
                }
            }
        }
        let selected = get_if_addrs()?
            .iter()
            .filter_map(|i| match (i.ip(), addr, netmask) {
                (IpAddr::V4(ip), IpAddr::V4(addr), IpAddr::V4(nm)) => {
                    let masked = Ipv4Addr::from(
                        u32::from_be_bytes(ip.octets()) & u32::from_be_bytes(nm.octets()),
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

    fn select(&self) -> Result<(IpAddr, IpAddr)> {
        match self {
            BindCfg::Local => {
                Ok((IpAddr::V4(Ipv4Addr::LOCALHOST), IpAddr::V4(Ipv4Addr::LOCALHOST)))
            }
            BindCfg::Exact(addr) => {
                if get_if_addrs()?.iter().any(|i| i.ip() == addr.ip()) {
                    Ok((addr.ip(), addr.ip()))
                } else {
                    bail!("no interface matches the bind address {:?}", addr);
                }
            }
            BindCfg::Elastic { public, private, netmask } => {
                let private = self.select_local_ip(private, netmask)?;
                Ok((*public, private))
            }
            BindCfg::ElasticExact { public, private } => Ok((public.ip(), private.ip())),
            BindCfg::Match { addr, netmask } => {
                let private = self.select_local_ip(addr, netmask)?;
                Ok((private, private))
            }
        }
    }
}

atomic_id!(ClId);

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
    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
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

        /// If set, then subscribers will never reuse an existing
        /// connection to the publisher when subscribing to this
        /// value.

        /// This flag should not be set lightly, as connections are
        /// not an infinite resource. However, for example, channels
        /// use this flag to ensure that multiple channels connected
        /// to the same publisher are isolated from each other with
        /// respect to blocking behavior. That is, if more than one
        /// channel is connected to the same publisher, and one of
        /// them backs up, but others do not, then if they were
        /// subscribed with the ISOLATED flag set they will operate
        /// independently, whereas if the ISOLATED flag was not set
        /// then they would all block if one of them blocks.
        ///
        /// This flag is mutually exclusive with USE_EXISTING, and if
        /// both are set then USE_EXISTING will override.
        const ISOLATED = 0x04;

        /// If the subscriber has a choice between publishers, it will
        /// choose the one "closest" to it first before trying any others.
        /// In this context "closest" means, in order of closeness
        ///
        /// - on the same host
        /// - in the same subnet
        /// - any
        ///
        /// Meaning a subsciber with this flag set will first try the
        /// publisher on the local machine, and if that fails, then it
        /// will try the publisher in the same subnet with it, and only
        /// if that fails will it try all the other publishers.
        const PREFER_LOCAL = 0x08;

        /// This is the same as PREFER_LOCAL except when mixed with
        /// USE_EXISTING. In that case it will override USE_EXISTING.
        /// If your publisher semantics rely on all clients talking
        /// to the same publisher then do not use this flag. e.g. do
        /// not use this flag for rpcs.
        const FORCE_LOCAL = 0x10;
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

#[derive(Debug)]
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
    pub fn update<T: Into<Value>>(&self, batch: &mut UpdateBatch, v: T) {
        batch.updates.push(BatchMsg::Update(None, self.0, v.into()))
    }

    /// Same as update, except the argument can be TryInto<Value>
    /// instead of Into<Value>
    pub fn try_update<T: TryInto<Value>>(
        &self,
        batch: &mut UpdateBatch,
        v: T,
    ) -> result::Result<(), T::Error> {
        Ok(batch.updates.push(BatchMsg::Update(None, self.0, v.try_into()?)))
    }

    /// update the current value only if the new value is different
    /// from the existing one. Otherwise exactly the same as update.
    pub fn update_changed<T: Into<Value>>(&self, batch: &mut UpdateBatch, v: T) {
        batch.updates.push(BatchMsg::UpdateChanged(self.0, v.into()))
    }

    /// Same as update_changed except the argument can be
    /// TryInto<Value> instead of Into<Value>.
    pub fn try_update_changed<T: TryInto<Value>>(
        &self,
        batch: &mut UpdateBatch,
        v: T,
    ) -> result::Result<(), T::Error> {
        Ok(batch.updates.push(BatchMsg::UpdateChanged(self.0, v.try_into()?)))
    }

    /// Queue sending `v` as an update ONLY to the specified
    /// subscriber, and do not update `current`.
    pub fn update_subscriber<T: Into<Value>>(
        &self,
        batch: &mut UpdateBatch,
        dst: ClId,
        v: T,
    ) {
        batch.updates.push(BatchMsg::Update(Some(dst), self.0, v.into()));
    }

    /// Same as update_subscriber except the argument can be TryInto<Value>.
    pub fn try_update_subscriber<T: TryInto<Value>>(
        &self,
        batch: &mut UpdateBatch,
        dst: ClId,
        v: T,
    ) -> result::Result<(), T::Error> {
        Ok(batch.updates.push(BatchMsg::Update(Some(dst), self.0, v.try_into()?)))
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
                let flags = if flags.is_empty() { None } else { Some(flags.bits()) };
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

#[derive(Debug)]
struct Client {
    msg_queue: MsgQ,
    subscribed: FxHashMap<Id, Permissions>,
    user: Option<UserInfo>,
}

#[derive(Debug)]
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

#[derive(Debug)]
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
    on_event_by_id_chans: FxHashMap<Id, Vec<UnboundedSender<Event>>>,
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

    pub fn check_publish(&self, path: &Path) -> Result<()> {
        if !Path::is_absolute(&path) {
            bail!("can't publish a relative path")
        }
        if self.stop.is_none() {
            bail!("publisher is dead")
        }
        if self.by_path.contains_key(path) {
            bail!("already published")
        }
        Ok(())
    }

    pub fn publish(&mut self, id: Id, flags: PublishFlags, path: Path) {
        self.by_path.insert(path.clone(), id);
        self.to_unpublish.remove(&path);
        self.to_publish.insert(
            path.clone(),
            if flags.is_empty() { None } else { Some(flags.bits()) },
        );
        self.trigger_publish();
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
        let id = match &event {
            Event::Destroyed(id) => id,
            Event::Subscribe(id, _) => id,
            Event::Unsubscribe(id, _) => id,
        };
        if let Some(chans) = self.on_event_by_id_chans.get_mut(id) {
            chans.retain(|chan| chan.unbounded_send(event).is_ok());
            if chans.is_empty() {
                self.on_event_by_id_chans.remove(id);
            }
        }
    }

    fn trigger_publish(&mut self) {
        if !self.publish_triggered {
            self.publish_triggered = true;
            let _: Result<_, _> = self.trigger_publish.unbounded_send(None);
        }
    }

    fn writes(&mut self, id: Id, tx: Sender<Pooled<Vec<WriteRequest>>>) {
        if self.by_id.contains_key(&id) {
            let e = self
                .on_write_chans
                .entry(ChanWrap(tx.clone()))
                .or_insert_with(|| (ChanId::new(), HashSet::new()));
            e.1.insert(id);
            let cid = e.0;
            let mut gc = Vec::new();
            let ow = self.on_write.entry(id).or_insert_with(Vec::new);
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
                self.on_write_chans.remove(&c);
            }
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

#[derive(Debug)]
pub struct PublisherBuilder {
    config: Option<Config>,
    desired_auth: Option<DesiredAuth>,
    bind_cfg: Option<BindCfg>,
    max_clients: usize,
    slack: usize,
}

impl PublisherBuilder {
    pub fn new(config: Config) -> Self {
        Self {
            config: Some(config),
            desired_auth: None,
            bind_cfg: None,
            max_clients: 768,
            slack: 3,
        }
    }

    pub async fn build(&mut self) -> Result<Publisher> {
        let cfg = self.config.take().unwrap();
        let desired_auth = self.desired_auth.take().unwrap_or_else(|| cfg.default_auth());
        let bind_cfg =
            self.bind_cfg.take().unwrap_or_else(|| cfg.default_bind_config.clone());
        Publisher::new(cfg, desired_auth, bind_cfg, self.max_clients, self.slack).await
    }

    /// The desired authentication mechanism you want to use. If not
    /// specified then the default specified in the config will be
    /// used
    pub fn desired_auth(&mut self, auth: DesiredAuth) -> &mut Self {
        self.desired_auth = Some(auth);
        self
    }

    /// The speficiation for the ip you want to bind to. If not
    /// specified then the config default will be used.
    pub fn bind_cfg(&mut self, bind: Option<BindCfg>) -> &mut Self {
        self.bind_cfg = bind;
        self
    }

    /// The maximum number of simultaneous subscribers. default 768.
    pub fn max_clients(&mut self, max_clients: usize) -> &mut Self {
        self.max_clients = max_clients;
        self
    }

    /// The maximum number of queued batches for a client before
    /// there is pushback. Calibrate this with your flush timeout to deal
    /// with slow consumers.
    pub fn slack(&mut self, slack: usize) -> &mut Self {
        self.slack = slack;
        self
    }
}

/// Publish values. Publisher is internally wrapped in an Arc, so
/// cloning it is virtually free. When all references to to the
/// publisher have been dropped the publisher will shutdown the
/// listener, and remove all published paths from the resolver server.
#[derive(Debug, Clone)]
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
        max_clients: usize,
        slack: usize,
    ) -> Result<Publisher> {
        let (public, private) = bind_cfg.select()?;
        utils::check_addr(public, &resolver.addrs)?;
        let (addr, listener) = match bind_cfg {
            BindCfg::Exact(addr) => {
                let l = TcpListener::bind(&addr).await?;
                (l.local_addr()?, l)
            }
            BindCfg::ElasticExact { public, private } => {
                let l = TcpListener::bind(&private).await?;
                (public, l)
            }
            BindCfg::Match { .. } | BindCfg::Local | BindCfg::Elastic { .. } => {
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
                    let addr = mkaddr(private, port)?;
                    match TcpListener::bind(&addr).await {
                        Ok(l) => break (mkaddr(public, port)?, l),
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::AddrInUse {
                                bail!(e)
                            }
                        }
                    }
                }
            }
        };
        let tls_ctx = resolver.tls.clone().map(tls::CachedAcceptor::new);
        let resolver = ResolverWrite::new(resolver, desired_auth.clone(), addr)?;
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
            on_event_by_id_chans: HashMap::default(),
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
                server::start(
                    pb_weak.clone(),
                    listener,
                    receive_stop,
                    desired_auth,
                    tls_ctx,
                    max_clients,
                    slack,
                )
                .await;
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
    ///
    /// If specified the write channel will be registered before the
    /// value is published, so there can be no race (however small)
    /// that might cause you to miss a write.
    pub fn publish_with_flags_and_writes<T>(
        &self,
        mut flags: PublishFlags,
        path: Path,
        init: T,
        tx: Option<Sender<Pooled<Vec<WriteRequest>>>>,
    ) -> Result<Val>
    where
        T: TryInto<Value>,
        <T as TryInto<Value>>::Error: std::error::Error + Send + Sync + 'static,
    {
        let init: Value = init.try_into()?;
        let id = Id::new();
        let destroy_on_idle = flags.contains(PublishFlags::DESTROY_ON_IDLE);
        flags.remove(PublishFlags::DESTROY_ON_IDLE);
        let mut pb = self.0.lock();
        pb.check_publish(&path)?;
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
        if let Some(tx) = tx {
            pb.writes(id, tx);
        }
        pb.publish(id, flags, path.clone());
        Ok(Val(id))
    }

    /// Publish `Path` with initial value `init` and flags `flags`. It
    /// is an error for the same publisher to publish the same path
    /// twice, however different publishers may publish a given path
    /// as many times as they like. Subscribers will then pick
    /// randomly among the advertised publishers when subscribing. See
    /// `subscriber`
    pub fn publish_with_flags<T>(
        &self,
        flags: PublishFlags,
        path: Path,
        init: T,
    ) -> Result<Val>
    where
        T: TryInto<Value>,
        <T as TryInto<Value>>::Error: std::error::Error + Send + Sync + 'static,
    {
        self.publish_with_flags_and_writes(flags, path, init, None)
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
        pb.check_publish(&path)?;
        pb.publish(id, flags, path.clone());
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
    pub fn publish<T>(&self, path: Path, init: T) -> Result<Val>
    where
        T: TryInto<Value>,
        <T as TryInto<Value>>::Error: std::error::Error + Send + Sync + 'static,
    {
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
        if pb.default.contains_key(&base) {
            bail!("default is already published")
        }
        if pb.stop.is_none() {
            bail!("publisher is dead")
        }
        pb.to_unpublish.remove(base.as_ref());
        pb.to_publish_default.insert(
            base.clone(),
            if flags.is_empty() { None } else { Some(flags.bits()) },
        );
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

    /// Return true if the specified client is subscribed to the
    /// specifed Id.
    pub fn is_subscribed(&self, id: &Id, client: &ClId) -> bool {
        match self.0.lock().by_id.get(&id) {
            Some(p) => p.subscribed.contains(client),
            None => false,
        }
    }

    /// Return the user information associated with the specified
    /// client id. If the authentication mechanism is Krb5 then this
    /// will be the remote user's user principal name,
    /// e.g. eric@RYU-OH.ORG on posix systems. If the auth mechanism
    /// is tls, then this will be the common name of the user's
    /// certificate. If the authentication mechanism is Local then
    /// this will be the local user name.
    ///
    /// This will always be None if the auth mechanism is Anonymous.
    pub fn user(&self, client: &ClId) -> Option<UserInfo> {
        self.0.lock().clients.get(client).and_then(|c| c.user.clone())
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
        self.0.lock().writes(id, tx)
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

    /// Register `tx` to receive a message about publisher events
    ///
    /// This does exactly the same thing as events, however only
    /// events for the specifed id will be delivered to the
    /// channel. As with events, if you don't want to receive events
    /// anymore for the channel you can just drop it.
    pub fn events_for_id(&self, id: Id, tx: UnboundedSender<Event>) {
        self.0.lock().on_event_by_id_chans.entry(id).or_insert(vec![]).push(tx);
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
