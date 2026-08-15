//! Subscribe to published values.
mod connection;
pub use crate::protocol::value::{FromValue, Typ, Value};
pub use crate::resolver_client::DesiredAuth;
use crate::{
    batch_channel::{self, BatchSender},
    config::Config,
    pack::{Pack, PackError},
    path::Path,
    protocol::{
        publisher::{From, Id, WriteId},
        resolver::{Publisher, Resolved, TargetAuth},
    },
    publisher::PublishFlags,
    resolver_client::{
        PublisherKey, PublisherTable, ResolverError, ResolverErrors, ResolverRead,
    },
    tls,
    utils::{BatchItem, Batched, ChanWrap},
};
use ahash::AHashMap;
use anyhow::{Error, Result, anyhow};
use bytes::{Buf, BufMut, Bytes};
use compact_str::format_compact;
use enumflags2::{BitFlags, bitflags};
use futures::{
    channel::{
        mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    prelude::*,
    select_biased,
    stream::FuturesUnordered,
};
use if_addrs::{IfAddr, Interface as NetworkInterface, get_if_addrs};
use log::{info, trace, warn};
use netidx_netproto::resolver::{PublisherPriority, PublisherRef, UserInfo};
use nohash::IntMap;
use parking_lot::Mutex;
use poolshark::global::{GPooled, Pool};
use poolshark::local::LPooled;
use rand::RngExt;
use smallvec::SmallVec;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::LazyLock;
use std::{
    cmp::{Eq, PartialEq, max},
    collections::{VecDeque, hash_map::Entry},
    error, fmt,
    hash::Hash,
    iter, mem,
    net::SocketAddr,
    result,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::{
    task,
    time::{self, Instant},
};
use triomphe::Arc as TArc;

static BATCHES: LazyLock<Pool<Vec<(SubId, Event)>>> =
    LazyLock::new(|| Pool::new(64, 16384));
static DECODE_BATCHES: LazyLock<Pool<Vec<From>>> = LazyLock::new(|| Pool::new(64, 16384));
static ERRORS: LazyLock<Pool<Vec<(SubId, SubscribeErrors)>>> =
    LazyLock::new(|| Pool::new(64, 16384));

/// Why a subscription failed.
///
/// A classification, not a diagnosis. Which publisher refused, which resolver
/// was unreachable, and what exactly either of them said is in the log; this
/// tells you what kind of thing went wrong so you know whether to go read it.
#[bitflags]
#[repr(u64)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeError {
    /// The subscription attempt was abandoned before it finished, e.g. the
    /// subscriber was shut down.
    Dropped,
    /// The publisher unsubscribed us while we were subscribed.
    Unpublished,
    /// The connection to the publisher died.
    ConnectionLost,
    /// We could not connect to the publisher.
    ConnectFailed,
    /// Authentication with the publisher failed.
    AuthFailed,
    /// The kerberos exchange failed. The gssapi error is in the log.
    KrbError,
    /// The tls session failed. The rustls error is in the log.
    TlsError,
    /// The publisher speaks a different protocol version.
    ProtocolMismatch,
    /// The publisher refused the subscription.
    Denied,
    /// The publisher does not publish the path.
    NoSuchValue,
    /// The publisher never answered the subscription request.
    SubscribeTimeout,
    /// The resolver refused to resolve the path.
    ResolverDenied,
    /// The path resolved, but nobody publishes it.
    NotFound,
    /// No resolver server could be reached.
    ResolverUnreachable,
    /// The resolver answered with an error.
    ResolverError,
    /// The resolver did not answer in time.
    ResolveTimeout,
    /// It failed, but we could not classify why. Always logged in full.
    Unspecified,
}

impl SubscribeError {
    fn name(&self) -> &'static str {
        match self {
            Self::Dropped => "dropped",
            Self::Unpublished => "unpublished",
            Self::ConnectionLost => "connection lost",
            Self::ConnectFailed => "connect failed",
            Self::AuthFailed => "authentication failed",
            Self::KrbError => "kerberos error",
            Self::TlsError => "tls error",
            Self::ProtocolMismatch => "protocol mismatch",
            Self::Denied => "permission denied",
            Self::NoSuchValue => "no such value",
            Self::SubscribeTimeout => "subscribe timed out",
            Self::ResolverDenied => "resolver denied",
            Self::NotFound => "not found",
            Self::ResolverUnreachable => "resolver unreachable",
            Self::ResolverError => "resolver error",
            Self::ResolveTimeout => "resolve timed out",
            Self::Unspecified => "unspecified",
        }
    }

    /// An `anyhow::Error` carrying this classification, which
    /// `SubscribeErrors::classify` can recover.
    pub(crate) fn err(self) -> Error {
        Error::from(SubscribeErrors::from(self))
    }
}

/// The set of reasons a subscription is not subscribed.
///
/// A set rather than one reason because several genuinely apply at once: a
/// retrying `Dval` accumulates distinct failures across rounds. Empty means
/// there is nothing wrong.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SubscribeErrors(pub BitFlags<SubscribeError>);

impl SubscribeErrors {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn contains(&self, e: SubscribeError) -> bool {
        self.0.contains(e)
    }

    pub fn iter(&self) -> impl Iterator<Item = SubscribeError> {
        self.0.iter()
    }

    pub fn insert(&mut self, e: impl Into<BitFlags<SubscribeError>>) {
        self.0 |= e.into();
    }

    /// Recover the classification of a failed subscription.
    ///
    /// Every error the subscriber produces carries one. An error that does not
    /// is a gap in the classification rather than an `Unspecified` subscription,
    /// so it is logged in full before being called `Unspecified`.
    fn classify(e: &Error, path: &Path) -> Self {
        match e.downcast_ref::<Self>() {
            Some(errs) => *errs,
            None => {
                warn!("unclassified subscribe error for {path}: {e:?}");
                Self::from(SubscribeError::Unspecified)
            }
        }
    }
}

// `From` in this module is the publisher's wire message
impl std::convert::From<SubscribeError> for SubscribeErrors {
    fn from(e: SubscribeError) -> Self {
        Self(e.into())
    }
}

impl fmt::Display for SubscribeErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return write!(f, "no errors");
        }
        for (i, e) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?
            }
            write!(f, "{}", e.name())?
        }
        Ok(())
    }
}

impl error::Error for SubscribeErrors {}

#[cfg(test)]
mod classification {
    use super::*;
    use bytes::BytesMut;

    /// `Event` is what the archive writes, so its encoding is not ours to
    /// change while adding an error channel beside it.
    #[test]
    fn the_event_encoding_is_unchanged() {
        for e in [Event::Unsubscribed, Event::Update(Value::I64(42))] {
            let mut buf = BytesMut::new();
            Pack::encode(&e, &mut buf).unwrap();
            assert_eq!(<Event as Pack>::decode(&mut buf).unwrap(), e);
        }
        let mut buf = BytesMut::new();
        Pack::encode(&Event::Unsubscribed, &mut buf).unwrap();
        assert_eq!(&buf[..], &[0x40]);
        // an exhausted buffer used to index past the end and panic
        let mut empty = BytesMut::new();
        assert!(<Event as Pack>::decode(&mut empty).is_err());
    }

    /// A cluster that failed several ways at once keeps all of them across
    /// the restatement. Collapsing to one would name a single fix for a
    /// situation that needs two.
    #[test]
    fn every_resolver_reason_becomes_a_subscribe_reason() {
        let path = Path::from("/foo");
        let mut errors = ResolverErrors::default();
        errors.insert(ResolverError::Unreachable);
        errors.insert(ResolverError::TlsError);
        let e = resolve_failed(&Error::from(errors), &path);
        let mut want = SubscribeErrors::default();
        want.insert(SubscribeError::ResolverUnreachable);
        want.insert(SubscribeError::TlsError);
        assert_eq!(SubscribeErrors::classify(&e, &path), want);
    }

    #[test]
    fn a_classified_error_survives_context_layers() {
        let e = SubscribeError::Denied.err().context("subscribing to /foo");
        assert_eq!(
            SubscribeErrors::classify(&e, &Path::from("/foo")),
            SubscribeErrors::from(SubscribeError::Denied)
        );
    }

    /// The rule the whole design rests on: an error nobody classified is a gap
    /// in the classification, not a subscription that failed for no reason.
    /// It is `Unspecified` *and* logged, never silently absorbed.
    #[test]
    fn an_unclassified_error_is_unspecified() {
        let e = anyhow!("something nobody thought about");
        assert_eq!(
            SubscribeErrors::classify(&e, &Path::from("/foo")),
            SubscribeErrors::from(SubscribeError::Unspecified)
        );
    }

    #[test]
    fn the_set_accumulates_and_displays_every_reason() {
        let mut e = SubscribeErrors::from(SubscribeError::ConnectionLost);
        assert!(!e.is_empty());
        e.insert(SubscribeError::NotFound);
        e.insert(SubscribeError::ConnectionLost);
        assert!(e.contains(SubscribeError::ConnectionLost));
        assert!(e.contains(SubscribeError::NotFound));
        assert_eq!(e.iter().count(), 2);
        assert_eq!(&format!("{e}"), "connection lost, not found");
        assert_eq!(&format!("{}", SubscribeErrors::default()), "no errors");
    }
}

atomic_id!(SubId);
atomic_id!(SubscriberId);
atomic_id!(ConId);

bitflags! {
    /// Flags controlling update delivery behavior.
    #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
    pub struct UpdatesFlags: u32 {
        /// if set, then an immediate update will be sent consisting
        /// of the last value received from the publisher. If you
        /// reregister the same channel the last will be sent to that
        /// channel again, even though other events will not be sent
        /// twice. If you don't want this behavior you must also set
        /// NO_SPURIOUS.
        const BEGIN_WITH_LAST      = 0x01;

        /// If set then the subscriber will stop storing the last
        /// value. The `last` method will return whatever last was
        /// before this method was called, and the passed in channel
        /// will be the only way of getting data from the
        /// subscription. This improves performance at the expense of
        /// flexibility.
        const STOP_COLLECTING_LAST = 0x02;

        /// If BEGIN_WITH_LAST is set, and you reregister the same
        /// channel, do not send the last again to that
        /// channel.
        const NO_SPURIOUS          = 0x04;
    }
}

type Updates = GPooled<Vec<(SubId, Event)>>;
pub type UpdateChan = Sender<Updates>;
type WUpdateChan = ChanWrap<Updates>;
type Streams = SmallVec<[(UpdatesFlags, WUpdateChan); 1]>;

#[derive(Debug)]
struct SubscribeValRequest {
    path: Path,
    sub_id: SubId,
    timestamp: u64,
    permissions: u32,
    token: Bytes,
    resolver: SocketAddr,
    finished: oneshot::Sender<Result<Val>>,
    con: BatchSender<ToCon>,
    deadline: Option<Instant>,
    streams: Streams,
}

#[derive(Debug)]
enum ToCon {
    Subscribe(SubscribeValRequest),
    Unsubscribe(Id),
    Stream { id: Id, tx: WUpdateChan, flags: UpdatesFlags },
    Write(Id, Value, WriteId, Option<oneshot::Sender<Value>>),
    Flush(oneshot::Sender<()>),
}

/// A subscription event
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Event {
    /// The subscription has been unsubsubscribed
    Unsubscribed,
    /// The subscription's value has updated
    Update(Value),
}

impl Pack for Event {
    fn encoded_len(&self) -> usize {
        match self {
            Event::Unsubscribed => 1,
            Event::Update(v) => Pack::encoded_len(v),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> result::Result<(), PackError> {
        match self {
            Event::Unsubscribed => Ok(buf.put_u8(0x40)),
            Event::Update(v) => Pack::encode(v, buf),
        }
    }

    fn decode(buf: &mut impl Buf) -> result::Result<Self, PackError> {
        if !buf.has_remaining() {
            return Err(PackError::BufferShort);
        }
        if buf.chunk()[0] == 0x40 {
            buf.advance(1);
            Ok(Event::Unsubscribed)
        } else {
            Ok(Event::Update(Pack::decode(buf)?))
        }
    }
}

#[derive(Debug)]
struct ValInner {
    sub_id: SubId,
    id: Id,
    conid: ConId,
    connection: BatchSender<ToCon>,
    last: TArc<Mutex<Event>>,
}

impl Drop for ValInner {
    fn drop(&mut self) {
        self.connection.send(ToCon::Unsubscribe(self.id));
    }
}

#[derive(Debug, Clone)]
pub struct ValWeak(Weak<ValInner>);

impl ValWeak {
    pub fn upgrade(&self) -> Option<Val> {
        Weak::upgrade(&self.0).map(|r| Val(r))
    }
}

/// A non-durable subscription to a value.
///
/// If all user held references to `Val` are dropped then it will be unsubscribed.
#[derive(Debug, Clone)]
pub struct Val(Arc<ValInner>);

impl Val {
    pub fn downgrade(&self) -> ValWeak {
        ValWeak(Arc::downgrade(&self.0))
    }

    /// Get the last event value.
    pub fn last(&self) -> Event {
        self.0.last.lock().clone()
    }

    /// Register a channel to receive updates to this subscription.
    ///
    /// You may register multiple different channels to receive
    /// updates from a `Val`, and you may register one channel to
    /// receive updates from multiple `Val`s.
    ///
    /// If you register multiple channels pointing to the same
    /// receiver you will not get duplicate updates. However, if you
    /// register a duplicate channel and begin_with_last is true you
    /// will get an update with the current state, even though the
    /// channel registration will be ignored.
    pub fn updates(&self, flags: UpdatesFlags, tx: UpdateChan) {
        let m = ToCon::Stream { tx: ChanWrap(tx), id: self.0.id, flags };
        self.0.connection.send(m);
    }

    /// Write a value back to the publisher. This will start going out
    /// as soon as this method returns, and you can call `flush` on
    /// the subscriber to get pushback in case of a slow publisher.
    ///
    /// The publisher will receive multiple writes in the order you
    /// call `write`.
    ///
    /// The publisher will not reply to your write, except that it may
    /// update values you are subscribed to, or trigger some other
    /// observable action.
    pub fn write(&self, v: Value) {
        self.0.connection.send(ToCon::Write(self.0.id, v, WriteId::new(), None));
    }

    /// Write a value and wait for a reply from the publisher.
    ///
    /// This does the same thing as `write` except that it requires
    /// the publisher send a reply indicating the outcome of the
    /// request. The reply can be read from the returned oneshot
    /// channel.
    ///
    /// Note that compared to `write` this function has higher
    /// overhead, avoid it in situations where high message volumes
    /// are required.
    pub fn write_with_recipt(&self, v: Value) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        self.0.connection.send(ToCon::Write(self.0.id, v, WriteId::new(), Some(tx)));
        rx
    }

    /// Get the unique id of this subscription.
    pub fn id(&self) -> SubId {
        self.0.sub_id
    }

    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.0.connection.send(ToCon::Flush(tx));
        rx.await.map_err(|_| anyhow!("subscription is dead"))
    }
}

#[derive(Debug)]
struct DvDead {
    queued_writes: Vec<(Value, Option<oneshot::Sender<Value>>)>,
    waiting: Vec<oneshot::Sender<()>>,
    tries: usize,
    next_try: Instant,
    /// everything that has gone wrong since this subscription last succeeded
    errors: SubscribeErrors,
}

#[derive(Debug)]
enum DvState {
    Subscribed(Val),
    Dead(Box<DvDead>), // the box ensures that DvState is tag + 1 word
}

#[derive(Debug)]
struct DvalInner {
    sub_id: SubId,
    sub: DvState,
    streams: Streams,
}

#[derive(Debug, Clone)]
pub struct DvalWeak(Weak<Mutex<DvalInner>>);

impl DvalWeak {
    pub fn new() -> Self {
        DvalWeak(Weak::new())
    }

    pub fn upgrade(&self) -> Option<Dval> {
        Weak::upgrade(&self.0).map(|s| Dval(s))
    }
}

/// A durable value subscription with automatic reconnection.
///
/// `Dval` behaves just like `Val`, except that if it dies a task within subscriber will
/// attempt to resubscribe. The resubscription process goes through
/// the entire resolution and connection process again, so `Dval` is
/// robust to many failures. For example,
///
/// - multiple publishers are publishing on a path and one of them dies.
///   `Dval` will transparently move to another one.
///
/// - a publisher is restarted (possibly on a different
///   machine). `Dval` will wait using linear backoff for the publisher
///   to come back, and then it will resubscribe.
///
/// - The resolver server cluster is restarted. In this case existing
///   subscriptions won't die, but new ones will fail while the
///   cluster is down. However once it is back up, and the publishers
///   have republished all their data, which they will do
///   automatically, `Dval` will resubscribe to anything it couldn't
///   find while the resolver server cluster was down.
///
/// A `Dval` uses a bit more memory than a `Val` subscription, but
/// other than that the performance is the same. It is therefore
/// recommended that you use `Dval` as the default kind of value
/// subscription.
/// If `stop_collecting_last` is true then the subscriber will
/// stop storing the last value in addition to giving it to this
/// channel. The `last` method will return null from now on, and
/// the passed in channel will be the only way of getting data
/// from the subscription. This improves performance at the
/// expense of flexibility.

///
/// If all user held references to `Dval` are dropped it will be
/// unsubscribed.
#[derive(Debug, Clone)]
pub struct Dval(Arc<Mutex<DvalInner>>);

impl Dval {
    pub fn downgrade(&self) -> DvalWeak {
        DvalWeak(Arc::downgrade(&self.0))
    }

    /// Return the number of strong references to this dval
    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.0)
    }

    /// Get the last value published by the publisher, or Unsubscribed
    /// if the subscription is currently dead.
    pub fn last(&self) -> Event {
        match &self.0.lock().sub {
            DvState::Dead(_) => Event::Unsubscribed,
            DvState::Subscribed(val) => val.last(),
        }
    }

    /// Register a channel to receive updates to this durable subscription.
    ///
    /// You may register multiple different channels to receive
    /// updates from a `Dval`, and you may register one channel to
    /// receive updates from multiple `Dval`s.
    pub fn updates(
        &self,
        flags: UpdatesFlags,
        tx: mpsc::Sender<GPooled<Vec<(SubId, Event)>>>,
    ) {
        let mut t = self.0.lock();
        let tx = ChanWrap(tx);
        if !t.streams.iter().any(|(_, s)| &tx == s) {
            t.streams.push((flags, tx.clone()));
        }
        if let DvState::Subscribed(ref sub) = t.sub {
            let m = ToCon::Stream { tx, id: sub.0.id, flags };
            sub.0.connection.send(m);
        }
    }

    /// Wait until the `Dval` is subscribed.
    ///
    /// This is not a guarantee that the `Dval` will stay subscribed for any
    /// length of time, just that at the moment this method returns
    /// the `Dval` is subscribed. If the `Dval` is subscribed when
    /// this method is called, it will return immediatly without
    /// allocating any resources.
    pub async fn wait_subscribed(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        match &mut self.0.lock().sub {
            DvState::Subscribed(_) => return Ok(()),
            DvState::Dead(d) => d.waiting.push(tx),
        }
        let _ = rx.await;
        Ok(())
    }

    /// Write a value back to the publisher.
    ///
    /// See `Val::write` for details. If we
    /// aren't currently subscribed the write will be queued and sent
    /// when we are. The return value will be `true` if the write was
    /// sent immediatly, and false if it was queued. It is still
    /// possible that a write will be dropped e.g. if the connection
    /// dies while we are writing it.
    pub fn write(&self, v: Value) -> bool {
        let mut t = self.0.lock();
        match &mut t.sub {
            DvState::Subscribed(val) => {
                val.write(v);
                true
            }
            DvState::Dead(dead) => {
                dead.queued_writes.push((v, None));
                false
            }
        }
    }

    /// Write a value and wait for a reply from the publisher.
    ///
    /// This does the same thing as `write` except that it requires
    /// the publisher send a reply indicating the outcome of the
    /// request. The reply can be read from the returned oneshot
    /// channel.
    ///
    /// Note that compared to `write` this function has higher
    /// overhead, avoid it in situations where high message volumes
    /// are required.
    ///
    /// If we are not currently subscribed then the write will be
    /// queued until we are. It is still possible that a write will be
    /// dropped e.g. if the connection dies while we are writing it.
    pub fn write_with_recipt(&self, v: Value) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        let mut t = self.0.lock();
        match &mut t.sub {
            DvState::Subscribed(sub) => {
                sub.0.connection.send(ToCon::Write(
                    sub.0.id,
                    v,
                    WriteId::new(),
                    Some(tx),
                ));
            }
            DvState::Dead(dead) => {
                dead.queued_writes.push((v, Some(tx)));
            }
        }
        rx
    }

    /// Clear the write queue.
    pub fn clear_queued_writes(&self) {
        let mut t = self.0.lock();
        if let DvState::Dead(dead) = &mut t.sub {
            dead.queued_writes.clear();
        }
    }

    /// Return the number of queued writes.
    pub fn queued_writes(&self) -> usize {
        match &mut self.0.lock().sub {
            DvState::Subscribed(_) => 0,
            DvState::Dead(dead) => dead.queued_writes.len(),
        }
    }

    /// Return the unique id of this `Dval`.
    pub fn id(&self) -> SubId {
        self.0.lock().sub_id
    }

    /// The errors seen since this subscription last succeeded, or `None` if it
    /// is currently subscribed.
    ///
    /// This is the same state [`Subscriber::errors`] reports; ask here if you
    /// only care about this one subscription.
    pub fn last_error(&self) -> Option<SubscribeErrors> {
        match &self.0.lock().sub {
            DvState::Subscribed(_) => None,
            DvState::Dead(d) => Some(d.errors),
        }
    }
}

#[derive(Debug)]
enum SubStatus {
    Subscribed(ValWeak),
    Pending(Box<SmallVec<[oneshot::Sender<Result<Val>>; 1]>>), // the box ensures SubStatus is tag + 1 word
}

const REMEBER_FAILED: Duration = Duration::from_secs(60);

fn pick(n: usize) -> usize {
    let mut rng = rand::rng();
    rng.random_range(0..n)
}

/// Restate a resolver failure as a subscribe failure.
///
/// A cluster can fail for several reasons at once, and all of them are the
/// subscriber's business: a member with a certificate we won't accept and a
/// member that is down are different problems with different fixes.
fn resolve_failed(e: &Error, path: &Path) -> Error {
    let cause = match e.downcast_ref::<ResolverErrors>() {
        None => {
            warn!("unclassified resolver error for {path}: {e:?}");
            SubscribeErrors::from(SubscribeError::Unspecified)
        }
        Some(errors) => {
            let mut cause = SubscribeErrors::default();
            for e in errors.iter() {
                cause.insert(match e {
                    ResolverError::Denied => SubscribeError::ResolverDenied,
                    ResolverError::Unreachable => SubscribeError::ResolverUnreachable,
                    ResolverError::KrbError => SubscribeError::KrbError,
                    ResolverError::TlsError => SubscribeError::TlsError,
                    ResolverError::Error
                    | ResolverError::ReferralLimit
                    | ResolverError::Unexpected => SubscribeError::ResolverError,
                })
            }
            cause
        }
    };
    Error::from(cause).context(format_compact!("resolving {path} failed: {e}"))
}

/// Give every waiter on the same path the same failure.
///
/// They can't share one `anyhow::Error` because it isn't `Clone`, but the
/// classification is `Copy` and the message is a string, so rebuilding it per
/// waiter loses nothing — which is why this no longer flattens it to text.
fn fanout(e: &Error, path: &Path, waiters: SmallVec<[oneshot::Sender<Result<Val>>; 1]>) {
    if waiters.is_empty() {
        return;
    }
    let errors = SubscribeErrors::classify(e, path);
    let msg = format_compact!("{e}");
    for w in waiters {
        let _ = w.send(Err(Error::from(errors).context(msg.clone())));
    }
}

#[derive(Debug)]
struct Connection {
    primary: Option<(ConId, BatchSender<ToCon>)>,
    isolated: IntMap<ConId, BatchSender<ToCon>>,
}

impl Connection {
    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = &'a BatchSender<ToCon>> + 'a> {
        match &self.primary {
            Some((_, c)) => Box::new(iter::once(c).chain(self.isolated.values())),
            None => Box::new(self.isolated.values()),
        }
    }

    fn remove(&mut self, id: ConId) {
        if let Some((other, _)) = &self.primary {
            if id == *other {
                self.primary = None;
            }
        }
        self.isolated.remove(&id);
    }

    fn is_empty(&self) -> bool {
        self.primary.is_none() && self.isolated.is_empty()
    }
}

#[derive(Debug)]
struct Chosen {
    addr: SocketAddr,
    target_auth: TargetAuth,
    token: Bytes,
    uifo: Option<UserInfo>,
    flags: PublishFlags,
}

fn publisher_for<'a>(
    publishers: &'a PublisherTable,
    resolved: &Resolved,
    publisher: &PublisherRef,
) -> Option<&'a Publisher> {
    publishers.get(&PublisherKey::new(resolved.resolver, publisher.id))
}

#[derive(Debug)]
struct SubscriberInner {
    id: SubscriberId,
    resolver: ResolverRead,
    connections: AHashMap<SocketAddr, Connection>,
    recently_failed: AHashMap<SocketAddr, Instant>,
    subscribed: AHashMap<Path, SubStatus>,
    durable_dead: AHashMap<Path, DvalWeak>,
    durable_pending: AHashMap<Path, DvalWeak>,
    durable_alive: AHashMap<Path, DvalWeak>,
    trigger_resub: UnboundedSender<()>,
    desired_auth: DesiredAuth,
    tls_ctx: Option<tls::CachedConnector>,
    interfaces: Vec<NetworkInterface>,
    error_chans: Vec<Sender<GPooled<Vec<(SubId, SubscribeErrors)>>>>,
    /// staged by whoever changed a durable subscription's error state, drained
    /// by the error task, which is the only place that can wait on a slow
    /// consumer without holding this lock or stalling a resubscription
    pending_errors: GPooled<Vec<(SubId, SubscribeErrors)>>,
    error_notify: Sender<()>,
}

impl SubscriberInner {
    /// Record that `id`'s error set is now `errors`. Only call this when the
    /// set has actually changed; a durable subscription restates its condition
    /// on every retry and level triggering would emit forever.
    fn record_error(&mut self, id: SubId, errors: SubscribeErrors) {
        if !self.error_chans.is_empty() {
            self.pending_errors.push((id, errors));
            // a queued notification already says "there is work"
            let _ = self.error_notify.try_send(());
        }
    }

    fn durable_id(&self, path: &Path) -> Option<SubId> {
        self.durable_dead
            .get(path)
            .or_else(|| self.durable_pending.get(path))
            .or_else(|| self.durable_alive.get(path))
            .and_then(|w| w.upgrade())
            .map(|d| d.id())
    }

    fn choose_random_addr(
        &mut self,
        publishers: &GPooled<PublisherTable>,
        resolved: &Resolved,
        flags: PublishFlags,
    ) -> Option<Chosen> {
        use rand::seq::IteratorRandom;
        trace!("publishers {:?}", publishers);
        trace!("resolved {:?}", resolved);
        let mk = |(pref, pb): (&PublisherRef, &Publisher)| Chosen {
            addr: pb.addr,
            target_auth: pb.target_auth.clone(),
            token: pref.token.clone(),
            uifo: pb.user_info.clone(),
            flags,
        };
        macro_rules! with_pred {
            ($f:expr) => {
                resolved
                    .publishers
                    .iter()
                    .filter_map(|pref| {
                        publisher_for(publishers, resolved, pref)
                            .filter($f)
                            .map(|pb| (pref, pb))
                    })
                    .choose(&mut rand::rng())
                    .map(mk)
            };
        }
        let high = with_pred!(|pb| {
            pb.priority == PublisherPriority::High
                && !self.recently_failed.contains_key(&pb.addr)
        });
        if let Some(chosen) = high {
            trace!("chosen {chosen:?}");
            return Some(chosen);
        }
        let normal = with_pred!(|pb| {
            (pb.priority == PublisherPriority::High
                || pb.priority == PublisherPriority::Normal)
                && !self.recently_failed.contains_key(&pb.addr)
        });
        if let Some(chosen) = normal {
            trace!("chosen {chosen:?}");
            return Some(chosen);
        }
        let low = with_pred!(|pb| !self.recently_failed.contains_key(&pb.addr));
        if let Some(chosen) = low {
            return Some(chosen);
        }
        let chosen = resolved
            .publishers
            .iter()
            .filter_map(|pref| {
                publisher_for(publishers, resolved, pref).map(|pb| (pref, pb))
            })
            .choose(&mut rand::rng())
            .map(mk);
        trace!("chosen {chosen:?}");
        chosen
    }

    fn choose_existing_addr(
        &mut self,
        publishers: &GPooled<PublisherTable>,
        resolved: &Resolved,
        mut flags: PublishFlags,
    ) -> Option<Chosen> {
        flags = flags & !PublishFlags::ISOLATED;
        for pref in &*resolved.publishers {
            if let Some(pb) = publisher_for(publishers, resolved, pref) {
                if self.connections.contains_key(&pb.addr) {
                    return Some(Chosen {
                        addr: pb.addr,
                        target_auth: pb.target_auth.clone(),
                        token: pref.token.clone(),
                        uifo: pb.user_info.clone(),
                        flags,
                    });
                }
            }
        }
        if flags.contains(PublishFlags::PREFER_LOCAL) {
            self.choose_local_addr(true, publishers, resolved, flags)
        } else {
            self.choose_random_addr(publishers, resolved, flags)
        }
    }

    fn choose_local_addr(
        &mut self,
        tried_existing: bool,
        publishers: &GPooled<PublisherTable>,
        resolved: &Resolved,
        flags: PublishFlags,
    ) -> Option<Chosen> {
        use std::{cmp::min, net::IpAddr};
        fn mv4(ip: Ipv4Addr, mask: Ipv4Addr) -> Ipv4Addr {
            let mut masked = [0u8; 4];
            let ip = ip.octets();
            let mask = mask.octets();
            for i in 0..4 {
                masked[i] = ip[i] & mask[i];
            }
            masked.into()
        }
        fn mv6(ip: Ipv6Addr, mask: Ipv6Addr) -> Ipv6Addr {
            let mut masked = [0u8; 16];
            let ip = ip.octets();
            let mask = mask.octets();
            for i in 0..16 {
                masked[i] = ip[i] & mask[i];
            }
            masked.into()
        }
        let mut buf = SmallVec::<[(&PublisherRef, &Publisher); 16]>::new();
        buf.extend(
            resolved
                .publishers
                .iter()
                .filter_map(|r| publisher_for(publishers, resolved, r).map(|pb| (r, pb)))
                .filter(|(_, p)| !self.recently_failed.contains_key(&p.addr)),
        );
        let mut all_far = true;
        buf.sort_by_key(|(_, pb): &(&PublisherRef, &Publisher)| {
            let ip = pb.addr.ip();
            let pri = self.interfaces.iter().fold(2, |cur, i| match &i.addr {
                IfAddr::V4(ifv4) => match ip {
                    IpAddr::V6(_) => cur,
                    IpAddr::V4(ipv4) => {
                        if ipv4 == ifv4.ip {
                            0
                        } else if mv4(ifv4.ip, ifv4.netmask) == mv4(ipv4, ifv4.netmask) {
                            min(cur, 1)
                        } else {
                            min(cur, 2)
                        }
                    }
                },
                IfAddr::V6(ifv6) => match ip {
                    IpAddr::V4(_) => cur,
                    IpAddr::V6(ipv6) => {
                        if ipv6 == ifv6.ip {
                            0
                        } else if mv6(ifv6.ip, ifv6.netmask) == mv6(ipv6, ifv6.netmask) {
                            min(cur, 1)
                        } else {
                            min(cur, 2)
                        }
                    }
                },
            });
            if pri < 2 {
                all_far = false;
            }
            pri
        });
        if all_far || buf.len() == 0 {
            if !tried_existing && flags.contains(PublishFlags::USE_EXISTING) {
                self.choose_existing_addr(publishers, resolved, flags)
            } else {
                self.choose_random_addr(publishers, resolved, flags)
            }
        } else {
            buf.first().map(|(pref, pb)| Chosen {
                addr: pb.addr,
                target_auth: pb.target_auth.clone(),
                token: pref.token.clone(),
                uifo: pb.user_info.clone(),
                flags,
            })
        }
    }

    fn choose_addr(
        &mut self,
        publishers: &GPooled<PublisherTable>,
        resolved: &Resolved,
    ) -> Option<Chosen> {
        let mut flags = PublishFlags::from_bits(resolved.flags)?;
        if flags.contains(PublishFlags::FORCE_LOCAL)
            && flags.contains(PublishFlags::PREFER_LOCAL)
        {
            flags &= !PublishFlags::PREFER_LOCAL;
        }
        if flags.contains(PublishFlags::FORCE_LOCAL) {
            self.choose_local_addr(false, publishers, resolved, flags)
        } else if flags.contains(PublishFlags::USE_EXISTING) {
            self.choose_existing_addr(publishers, resolved, flags)
        } else if flags.contains(PublishFlags::PREFER_LOCAL) {
            self.choose_local_addr(false, publishers, resolved, flags)
        } else {
            self.choose_random_addr(publishers, resolved, flags)
        }
    }

    fn gc_recently_failed(&mut self) {
        let now = Instant::now();
        self.recently_failed.retain(|_, v| (now - *v) < REMEBER_FAILED)
    }
}

#[derive(Debug, Clone)]
struct SubscriberWeak(Weak<Mutex<SubscriberInner>>);

impl SubscriberWeak {
    fn upgrade(&self) -> Option<Subscriber> {
        Weak::upgrade(&self.0).map(|s| Subscriber(s))
    }
}

/// Statistics about durable subscriptions.
#[derive(Debug, Clone, Copy)]
pub struct DurableStats {
    pub alive: usize,
    pub pending: usize,
    pub dead: usize,
}

/// Builder for configuring and creating a Subscriber.
pub struct SubscriberBuilder {
    cfg: Option<Config>,
    desired_auth: Option<DesiredAuth>,
    follow_config: bool,
}

impl SubscriberBuilder {
    pub fn new(cfg: Config) -> Self {
        Self { cfg: Some(cfg), desired_auth: None, follow_config: true }
    }

    pub fn build(&mut self) -> Result<Subscriber> {
        let mut cfg = self
            .cfg
            .take()
            .ok_or_else(|| anyhow!("config is required, did you reuse the builder?"))?;
        if !self.follow_config {
            cfg.detach();
        }
        let desired_auth = self.desired_auth.take().unwrap_or_else(|| cfg.default_auth());
        Subscriber::new(cfg, desired_auth)
    }

    pub fn desired_auth(&mut self, auth: DesiredAuth) -> &mut Self {
        self.desired_auth = Some(auth);
        self
    }

    /// Whether to follow the config file's resolver addresses as they
    /// change. Default true.
    ///
    /// A config loaded from a file keeps a pointer back to it, and the
    /// subscriber re-reads the address list so that a resolver added to or
    /// removed from the cluster reaches it without a restart. Set this false
    /// to pin the addresses you started with — which you must do if you
    /// modify `Config::addrs` yourself, since otherwise the file wins.
    /// Configs built in memory are unaffected either way.
    pub fn follow_config(&mut self, follow: bool) -> &mut Self {
        self.follow_config = follow;
        self
    }
}

/// Subscribe to published values.
#[derive(Clone, Debug)]
pub struct Subscriber(Arc<Mutex<SubscriberInner>>);

impl Subscriber {
    /// Create a new subscriber with the specified config and desired auth.
    pub fn new(resolver: Config, desired_auth: DesiredAuth) -> Result<Subscriber> {
        let (tx, rx) = mpsc::unbounded();
        let (error_notify, error_rx) = mpsc::channel(0);
        let tls_ctx = resolver.tls.clone().map(tls::CachedConnector::new);
        let resolver = ResolverRead::new(resolver, desired_auth.clone());
        let t = Subscriber(Arc::new(Mutex::new(SubscriberInner {
            id: SubscriberId::new(),
            resolver,
            desired_auth,
            connections: AHashMap::default(),
            recently_failed: AHashMap::default(),
            subscribed: AHashMap::default(),
            durable_dead: AHashMap::default(),
            durable_pending: AHashMap::default(),
            durable_alive: AHashMap::default(),
            trigger_resub: tx,
            tls_ctx,
            interfaces: get_if_addrs()?,
            error_chans: Vec::new(),
            pending_errors: ERRORS.take(),
            error_notify,
        })));
        t.start_resub_task(rx);
        t.start_error_task(error_rx);
        Ok(t)
    }

    /// Return a unique identifier for this subscriber instance.
    ///
    /// The identifier will be unique across all subscribers created in
    /// this process, but not across processes or machines.
    pub fn id(&self) -> SubscriberId {
        self.0.lock().id
    }

    /// Return stats about durable subscriptions.
    pub fn durable_stats(&self) -> DurableStats {
        let t = self.0.lock();
        DurableStats {
            alive: t.durable_alive.len(),
            pending: t.durable_pending.len(),
            dead: t.durable_dead.len(),
        }
    }

    pub fn is_subscribed_or_pending(&self, path: &Path) -> bool {
        let t = self.0.lock();
        t.subscribed.contains_key(path)
            || t.durable_dead.contains_key(path)
            || t.durable_pending.contains_key(path)
            || t.durable_alive.contains_key(path)
    }

    pub fn resolver(&self) -> ResolverRead {
        self.0.lock().resolver.clone()
    }

    /// Register a channel to receive errors about durable subscriptions.
    ///
    /// An item is sent whenever a `Dval`'s error set changes: the union of
    /// everything that has gone wrong since it last succeeded, or the empty
    /// set when it resubscribes. So a consumer watching only this channel sees
    /// both failure and recovery, and a permanently dead subscription goes
    /// quiet once it has said everything it has to say.
    ///
    /// The set is a classification. What exactly failed is in the log.
    ///
    /// Non durable subscriptions report through their `Result` instead, and
    /// never appear here. Drop the channel to stop receiving.
    pub fn errors(&self, tx: Sender<GPooled<Vec<(SubId, SubscribeErrors)>>>) {
        self.0.lock().error_chans.push(tx)
    }

    fn downgrade(&self) -> SubscriberWeak {
        SubscriberWeak(Arc::downgrade(&self.0))
    }

    fn start_resub_task(&self, incoming: UnboundedReceiver<()>) {
        async fn wait_retry(retry: Option<Instant>) {
            match retry {
                None => future::pending().await,
                Some(d) => time::sleep_until(d).await,
            }
        }
        fn update_retry(subscriber: &mut SubscriberInner, retry: &mut Option<Instant>) {
            let now = Instant::now();
            *retry = None;
            for w in subscriber.durable_dead.values() {
                if let Some(dv) = w.upgrade() {
                    let next_try = match &dv.0.lock().sub {
                        DvState::Dead(dead) => dead.next_try,
                        DvState::Subscribed(_) => unreachable!(),
                    };
                    match retry {
                        None => {
                            *retry = Some(next_try);
                        }
                        Some(retry) => {
                            if next_try < *retry {
                                *retry = next_try;
                            }
                        }
                    }
                    if next_try <= now {
                        break;
                    }
                }
            }
        }
        async fn do_resub(
            subscriber: &SubscriberWeak,
            retry: &mut Option<Instant>,
        ) -> Option<FuturesUnordered<impl Future<Output = (Path, Result<Val>)> + use<>>>
        {
            let subscriber = subscriber.upgrade()?;
            info!("doing resubscriptions");
            let now = Instant::now();
            let (batch, timeout) = {
                let mut dead = Vec::new();
                let mut batch: Vec<(Path, Streams)> = Vec::new();
                let mut subscriber = subscriber.0.lock();
                let subscriber = &mut *subscriber;
                let durable_dead = &mut subscriber.durable_dead;
                let durable_pending = &mut subscriber.durable_pending;
                let mut max_tries = 1;
                let mut total_retries = 0;
                for (p, w) in durable_dead.iter() {
                    match w.upgrade() {
                        None => {
                            dead.push(p.clone());
                        }
                        Some(s) => {
                            let mut dv = s.0.lock();
                            let (next_try, tries) = {
                                match &mut dv.sub {
                                    DvState::Dead(d) => (d.next_try, d.tries),
                                    DvState::Subscribed(_) => unreachable!(),
                                }
                            };
                            if next_try <= now {
                                let streams = dv.streams.clone();
                                drop(dv);
                                batch.push((p.clone(), streams));
                                durable_pending.insert(p.clone(), w.clone());
                                max_tries = max(max_tries, tries);
                                total_retries += 1;
                                if total_retries >= 100_000 {
                                    break;
                                }
                            }
                        }
                    }
                }
                for p in dead.iter().chain(batch.iter().map(|(p, _)| p)) {
                    durable_dead.remove(p);
                }
                let timeout = 30 + max(10, batch.len() / 10000) * max_tries;
                (batch, Duration::from_secs(timeout as u64))
            };
            if batch.len() == 0 {
                let mut subscriber = subscriber.0.lock();
                update_retry(&mut *subscriber, retry);
                None
            } else {
                update_retry(&mut *subscriber.0.lock(), retry);
                Some(subscriber.subscribe_nondurable_internal(batch, Some(timeout)).await)
            }
        }
        fn finish_resubscription_batch(
            subscriber: &SubscriberWeak,
            batch: &mut Vec<(Path, Result<Val>)>,
            retry: &mut Option<Instant>,
        ) {
            if let Some(subscriber) = subscriber.upgrade() {
                let mut subscriber = subscriber.0.lock();
                let now = Instant::now();
                for (p, r) in batch.drain(..) {
                    if let Some(ds) =
                        subscriber.durable_pending.remove(&p).and_then(|ds| ds.upgrade())
                    {
                        trace!("processing pending subscrition to {p}");
                        let dsw = ds.downgrade();
                        let mut dv = ds.0.lock();
                        let sub_id = dv.sub_id;
                        macro_rules! failed {
                            ($e:expr) => {{
                                let e: Error = $e;
                                match &mut dv.sub {
                                    DvState::Subscribed(_) => unreachable!(),
                                    DvState::Dead(d) => {
                                        d.tries += 1;
                                        let wait = Duration::from_millis(
                                            pick(d.tries) as u64 * 50,
                                        );
                                        d.next_try = now + wait;
                                        let s = wait.as_secs_f32();
                                        warn!(
                                            "resubscription error {}: {}, next try: {}s",
                                            p, e, s
                                        );
                                        let before = d.errors;
                                        d.errors
                                            .insert(SubscribeErrors::classify(&e, &p).0);
                                        if d.errors != before {
                                            let errors = d.errors;
                                            subscriber.record_error(sub_id, errors);
                                        }
                                        subscriber.durable_dead.insert(p.clone(), dsw);
                                    }
                                }
                            }};
                        }
                        match r {
                            Err(e) => failed!(e),
                            Ok(sub) if *sub.0.last.lock() == Event::Unsubscribed => {
                                failed!(SubscribeError::Unpublished.err())
                            }
                            Ok(sub) => {
                                info!("resubscription success {}", p);
                                for (f, tx) in &dv.streams {
                                    sub.0.connection.send(ToCon::Stream {
                                        tx: tx.clone(),
                                        id: sub.0.id,
                                        flags: *f
                                            | UpdatesFlags::BEGIN_WITH_LAST
                                            | UpdatesFlags::NO_SPURIOUS,
                                    });
                                }
                                if let DvState::Dead(d) = &mut dv.sub {
                                    for (v, resp) in d.queued_writes.drain(..) {
                                        sub.0.connection.send(ToCon::Write(
                                            sub.0.id,
                                            v,
                                            WriteId::new(),
                                            resp,
                                        ));
                                    }
                                    if !d.errors.is_empty() {
                                        subscriber.record_error(
                                            sub_id,
                                            SubscribeErrors::default(),
                                        );
                                    }
                                }
                                dv.sub = DvState::Subscribed(sub);
                                subscriber.durable_alive.insert(p.clone(), dsw);
                            }
                        }
                    }
                }
                update_retry(&mut *subscriber, retry);
            }
        }
        async fn next_subscription_result(
            subscriptions: &mut VecDeque<
                Batched<FuturesUnordered<impl Future<Output = (Path, Result<Val>)>>>,
            >,
        ) -> BatchItem<(Path, Result<Val>)> {
            loop {
                if subscriptions.is_empty() {
                    return future::pending().await;
                } else if subscriptions.len() == 1 {
                    match subscriptions[0].next().await {
                        Some(v) => return v,
                        None => {
                            subscriptions.pop_front();
                        }
                    }
                } else {
                    let i = subscriptions
                        .iter_mut()
                        .enumerate()
                        .map(|(i, f)| f.next().map(move |r| (r, i)));
                    let ((r, i), _, _) = future::select_all(i).await;
                    match r {
                        Some(v) => return v,
                        None => {
                            subscriptions.remove(i);
                        }
                    }
                }
            }
        }
        let subscriber = self.downgrade();
        task::spawn(async move {
            let mut incoming = Batched::new(incoming.fuse(), 1_000_000_000);
            let mut subscriptions = VecDeque::new();
            let mut subscription_batch = Vec::new();
            let mut retry: Option<Instant> = None;
            loop {
                select_biased! {
                    m = incoming.next() => match m {
                        None => break,
                        Some(BatchItem::InBatch(())) => {
                            trace!("incoming");
                            ()
                        },
                        Some(BatchItem::EndBatch) => {
                            trace!("incoming end batch");
                            if let Some(set) = do_resub(&subscriber, &mut retry).await {
                                subscriptions.push_back(Batched::new(set, 100_000));
                            }
                        }
                    },
                    m = next_subscription_result(&mut subscriptions).fuse() => match m {
                        BatchItem::InBatch((p, r)) => {
                            trace!("subscription result for {p} {r:?}");
                            subscription_batch.push((p, r))
                        },
                        BatchItem::EndBatch => {
                            trace!("end subscription batch {}", subscription_batch.len());
                            finish_resubscription_batch(
                                &subscriber,
                                &mut subscription_batch,
                                &mut retry
                            );
                            if let Some(t) = retry {
                                if Instant::now() >= t {
                                    if let Some(set) = do_resub(&subscriber, &mut retry).await {
                                        subscriptions.push_back(Batched::new(set, 100_000));
                                    }
                                }
                            }
                        }
                    },
                    _ = wait_retry(retry).fuse() => {
                        trace!("time to retry");
                        if let Some(set) = do_resub(&subscriber, &mut retry).await {
                            subscriptions.push_back(Batched::new(set, 100_000));
                        }
                    },
                }
            }
            trace!("resub loop ended");
        });
    }

    /// Deliver staged error state changes to the registered channels.
    ///
    /// Its own task because it is the one place that waits: a consumer that
    /// stops reading must not stall resubscription or a connection, so the
    /// producers only stage under the subscriber lock and this task does the
    /// sending.
    fn start_error_task(&self, mut notify: Receiver<()>) {
        let subscriber = self.downgrade();
        task::spawn(async move {
            while let Some(()) = notify.next().await {
                // whatever is staged while we are blocked goes out too
                loop {
                    let (batch, mut chans) = match subscriber.upgrade() {
                        None => return,
                        Some(s) => {
                            let mut t = s.0.lock();
                            if t.pending_errors.is_empty() {
                                break;
                            }
                            let batch =
                                mem::replace(&mut t.pending_errors, ERRORS.take());
                            if t.error_chans.is_empty() {
                                continue;
                            }
                            let chans: LPooled<Vec<_>> =
                                t.error_chans.iter().cloned().collect();
                            (batch, chans)
                        }
                    };
                    for c in chans.iter_mut() {
                        let mut b = ERRORS.take();
                        b.extend_from_slice(&batch);
                        let _ = c.send(b).await;
                    }
                    if let Some(s) = subscriber.upgrade() {
                        s.0.lock().error_chans.retain(|c| !c.is_closed());
                    }
                }
            }
            trace!("error loop ended");
        });
    }

    fn start_connection(
        &self,
        tls_ctx: Option<tls::CachedConnector>,
        uifo: Option<UserInfo>,
        addr: SocketAddr,
        target_auth: &TargetAuth,
        desired_auth: &DesiredAuth,
    ) -> (ConId, BatchSender<ToCon>) {
        let (tx, rx) = batch_channel::channel();
        let subscriber = self.downgrade();
        let desired_auth = desired_auth.clone();
        let conid = ConId::new();
        let target_auth = target_auth.clone();
        task::spawn(async move {
            let res = connection::ConnectionCtx::new(
                addr,
                subscriber.clone(),
                conid,
                tls_ctx,
                uifo,
                target_auth,
                desired_auth,
                rx,
            )
            .start()
            .await;
            if let Some(subscriber) = subscriber.upgrade() {
                if let Entry::Occupied(mut e) =
                    subscriber.0.lock().connections.entry(addr)
                {
                    let c = e.get_mut();
                    c.remove(conid);
                    if c.is_empty() {
                        e.remove();
                    }
                }
                match res {
                    Ok(()) => {
                        info!("connection to {} closed", addr)
                    }
                    Err(e) => {
                        subscriber.0.lock().recently_failed.insert(addr, Instant::now());
                        warn!("connection to {} failed {}", addr, e)
                    }
                }
            }
        });
        (conid, tx)
    }

    /// Subscribe to a batch of values.
    ///
    /// To minimize round trips and amortize locking path resolution
    /// and subscription are done in batches. Best performance will be
    /// achieved with larger batches.
    ///
    /// In case you are already subscribed to one or more of the paths
    /// or aliases of the paths in the batch, you will receive a
    /// reference to the existing subscription. However subscriber
    /// does not retain strong references to subscribed values, so if
    /// you drop all of them it will be unsubscribed.
    ///
    /// It is safe to call this function concurrently with the same or
    /// overlapping sets of paths in the batch, only one subscription
    /// attempt will be made, and the result of that one attempt will
    /// be given to each concurrent caller upon success or failure.
    ///
    /// The timeout, if specified, will apply to each subscription
    /// individually. Any subscription that does not complete
    /// successfully before the specified timeout will result in an
    /// error, but that error will not effect other subscriptions in
    /// the batch, which may complete successfully. If you need all or
    /// nothing behavior, specify None for timeout and wrap the
    /// `subscribe` future in a `tokio::time::timeout`.
    pub async fn subscribe_nondurable(
        &self,
        batch: impl Iterator<Item = Path>,
        timeout: Option<Duration>,
    ) -> FuturesUnordered<impl Future<Output = (Path, Result<Val>)>> {
        self.subscribe_nondurable_internal(batch.map(|p| (p, [])), timeout).await
    }

    /// Subscribe to a batch of values with updates channels.
    ///
    /// This is the same as subscribe_nondurable except that updates
    /// channels may be registered for each path from the very
    /// beginning of the subscription. This ensures that every update
    /// from the beginning of the subscription will appear in the
    /// specified update channels.
    pub async fn subscribe_nondurable_updates<I, CI>(
        &self,
        batch: I,
        timeout: Option<Duration>,
    ) -> FuturesUnordered<impl Future<Output = (Path, Result<Val>)>>
    where
        I: IntoIterator<Item = (Path, CI)>,
        CI: IntoIterator<Item = (UpdatesFlags, UpdateChan)>,
    {
        let batch = batch
            .into_iter()
            .map(|(p, i)| (p, i.into_iter().map(|(f, c)| (f, ChanWrap(c)))));
        self.subscribe_nondurable_internal(batch, timeout).await
    }

    async fn subscribe_nondurable_internal<I, CI>(
        &self,
        batch: I,
        timeout: Option<Duration>,
    ) -> FuturesUnordered<impl Future<Output = (Path, Result<Val>)> + use<I, CI>>
    where
        I: IntoIterator<Item = (Path, CI)>,
        CI: IntoIterator<Item = (UpdatesFlags, WUpdateChan)>,
    {
        #[derive(Debug)]
        enum St {
            Resolve(Streams),
            Subscribing(oneshot::Receiver<Result<Val>>),
            WaitingOther(oneshot::Receiver<Result<Val>>, Streams),
            Subscribed(Val, Streams),
            Error(Error),
        }
        let now = Instant::now();
        let mut pending: LPooled<AHashMap<Path, St>> = LPooled::take();
        // Init
        let r = {
            let mut t = self.0.lock();
            t.gc_recently_failed();
            for (p, chans) in batch {
                let streams: Streams = chans.into_iter().collect();
                trace!("subscribing to {} streams {}", p, streams.len());
                match t.subscribed.entry(p.clone()) {
                    Entry::Vacant(e) => {
                        e.insert(SubStatus::Pending(Box::new(SmallVec::new())));
                        pending.insert(p, St::Resolve(streams));
                    }
                    Entry::Occupied(mut e) => match e.get_mut() {
                        SubStatus::Pending(v) => {
                            let (tx, rx) = oneshot::channel();
                            v.push(tx);
                            pending.insert(p, St::WaitingOther(rx, streams));
                        }
                        SubStatus::Subscribed(r) => match r.upgrade() {
                            Some(r) => {
                                trace!("already subscribed to {}", p);
                                pending.insert(p, St::Subscribed(r, streams));
                            }
                            None => {
                                e.insert(SubStatus::Pending(Box::new(SmallVec::new())));
                                pending.insert(p, St::Resolve(streams));
                            }
                        },
                    },
                }
            }
            t.resolver.clone()
        };
        // Resolve, Connect, Subscribe
        {
            let to_resolve = pending
                .iter()
                .filter(|(_, s)| match s {
                    St::Resolve(_) => true,
                    _ => false,
                })
                .map(|(p, _)| p.clone())
                .collect::<SmallVec<[_; 100]>>();
            let r = match timeout {
                None => Ok(r.resolve(to_resolve.iter().cloned()).await),
                Some(d) => time::timeout(d, r.resolve(to_resolve.iter().cloned())).await,
            };
            match r {
                Err(_) => {
                    for p in to_resolve {
                        let e = SubscribeError::ResolveTimeout
                            .err()
                            .context(format_compact!("resolving {p} timed out"));
                        pending.insert(p, St::Error(e));
                    }
                }
                Ok(Err(e)) => {
                    for p in to_resolve {
                        let s = St::Error(resolve_failed(&e, &p));
                        pending.insert(p, s);
                    }
                }
                Ok(Ok((publishers, mut res))) => {
                    let mut t = self.0.lock();
                    let deadline = timeout.map(|t| now + t);
                    let desired_auth = t.desired_auth.clone();
                    for (p, resolved) in to_resolve.into_iter().zip(res.drain(..)) {
                        let resolved = match resolved {
                            Ok(r) => r,
                            Err(e) => {
                                pending
                                    .insert(p.clone(), St::Error(resolve_failed(&e, &p)));
                                continue;
                            }
                        };
                        if resolved.publishers.len() == 0 {
                            pending.insert(p, St::Error(SubscribeError::NotFound.err()));
                        } else if let Some(ch) = t.choose_addr(&publishers, &resolved) {
                            let tls_ctx = t.tls_ctx.clone();
                            let sub_id = t.durable_id(&p).unwrap_or_else(SubId::new);
                            let con = t.connections.entry(ch.addr).or_insert_with(|| {
                                Connection { primary: None, isolated: IntMap::default() }
                            });
                            let con = if ch.flags.contains(PublishFlags::ISOLATED) {
                                let (id, c) = self.start_connection(
                                    tls_ctx,
                                    ch.uifo,
                                    ch.addr,
                                    &ch.target_auth,
                                    &desired_auth,
                                );
                                con.isolated.insert(id, c.clone());
                                c
                            } else {
                                match &con.primary {
                                    Some((_, c)) => c.clone(),
                                    None => {
                                        let (id, c) = self.start_connection(
                                            tls_ctx,
                                            ch.uifo,
                                            ch.addr,
                                            &ch.target_auth,
                                            &desired_auth,
                                        );
                                        con.primary = Some((id, c.clone()));
                                        c
                                    }
                                }
                            };
                            let (tx, rx) = oneshot::channel();
                            let con_ = con.clone();
                            let streams = match pending.remove(&p) {
                                Some(St::Resolve(streams)) => streams,
                                _ => unreachable!(),
                            };
                            let r = con.send(ToCon::Subscribe(SubscribeValRequest {
                                path: p.clone(),
                                sub_id,
                                timestamp: resolved.timestamp,
                                permissions: resolved.permissions as u32,
                                token: ch.token,
                                resolver: resolved.resolver,
                                finished: tx,
                                con: con_,
                                deadline,
                                streams,
                            }));
                            if r {
                                pending.insert(p, St::Subscribing(rx));
                            } else {
                                let e = SubscribeError::ConnectionLost
                                    .err()
                                    .context("connection closed");
                                pending.insert(p, St::Error(e));
                            }
                        } else {
                            let e = SubscribeError::ResolverError
                                .err()
                                .context("missing publisher record");
                            pending.insert(p, St::Error(e));
                        }
                    }
                }
            }
        }
        // Wait
        async fn wait_result(sub: Subscriber, path: Path, st: St) -> (Path, Result<Val>) {
            match st {
                St::Resolve(_) => unreachable!(),
                St::Subscribed(raw, streams) => {
                    for (f, tx) in streams {
                        let m = ToCon::Stream {
                            tx,
                            flags: f | UpdatesFlags::BEGIN_WITH_LAST,
                            id: raw.0.id,
                        };
                        raw.0.connection.send(m);
                    }
                    (path, Ok(raw))
                }
                St::Error(e) => {
                    let mut t = sub.0.lock();
                    if let Some(sub) = t.subscribed.remove(path.as_ref()) {
                        match sub {
                            SubStatus::Subscribed(_) => unreachable!(),
                            SubStatus::Pending(waiters) => fanout(&e, &path, *waiters),
                        }
                    }
                    (path, Err(e))
                }
                St::WaitingOther(w, streams) => match w.await {
                    Err(_) => (path, Err(SubscribeError::Dropped.err())),
                    Ok(Err(e)) => (path, Err(e)),
                    Ok(Ok(raw)) => {
                        for (f, tx) in streams {
                            let m = ToCon::Stream { tx, flags: f, id: raw.0.id };
                            raw.0.connection.send(m);
                        }
                        (path, Ok(raw))
                    }
                },
                St::Subscribing(w) => {
                    let res = match w.await {
                        Err(_) => Err(SubscribeError::ConnectionLost
                            .err()
                            .context("connection died")),
                        Ok(Err(e)) => Err(e),
                        Ok(Ok(raw)) => Ok(raw),
                    };
                    let mut t = sub.0.lock();
                    match t.subscribed.entry(path.clone()) {
                        Entry::Vacant(_) => unreachable!(),
                        Entry::Occupied(mut e) => match res {
                            Err(err) => match e.remove() {
                                SubStatus::Subscribed(_) => unreachable!(),
                                SubStatus::Pending(waiters) => {
                                    fanout(&err, &path, *waiters);
                                    (path, Err(err))
                                }
                            },
                            Ok(raw) => {
                                let s = mem::replace(
                                    e.get_mut(),
                                    SubStatus::Subscribed(raw.downgrade()),
                                );
                                match s {
                                    SubStatus::Subscribed(_) => unreachable!(),
                                    SubStatus::Pending(waiters) => {
                                        for w in waiters.into_iter() {
                                            let _ = w.send(Ok(raw.clone()));
                                        }
                                        (path, Ok(raw))
                                    }
                                }
                            }
                        },
                    }
                }
            }
        }
        pending.drain().map(|(path, st)| wait_result(self.clone(), path, st)).collect()
    }

    /// Subscribe to a single value.
    ///
    /// This is sufficient for a small number of paths, but if you
    /// need to subscribe to a lot of values it is more efficent to
    /// use `subscribe`. The semantics of this method are the same as
    /// `subscribe` called with 1 path.
    pub async fn subscribe_nondurable_one(
        &self,
        path: Path,
        timeout: Option<Duration>,
    ) -> Result<Val> {
        self.subscribe_nondurable(iter::once(path), timeout).await.next().await.unwrap().1
    }

    /// Subscribe to a single value with updates channels.
    ///
    /// This is sufficient for a small number of paths, but if you
    /// need to subscribe to a lot of values it is more efficent to
    /// use `subscribe`. The semantics of this method are the same as
    /// `subscribe` called with 1 path.
    pub async fn subscribe_nondurable_one_updates(
        &self,
        path: Path,
        updates: impl IntoIterator<Item = (UpdatesFlags, UpdateChan)>,
        timeout: Option<Duration>,
    ) -> Result<Val> {
        let updates = updates.into_iter().map(|(f, c)| (f, ChanWrap(c)));
        self.subscribe_nondurable_internal(iter::once((path, updates)), timeout)
            .await
            .next()
            .await
            .unwrap()
            .1
    }

    fn subscribe_internal<I>(&self, path: Path, updates: I) -> Dval
    where
        I: IntoIterator<Item = (UpdatesFlags, Sender<GPooled<Vec<(SubId, Event)>>>)>,
    {
        let mut t = self.0.lock();
        if let Some(s) = t
            .durable_dead
            .get(&path)
            .or_else(|| t.durable_pending.get(&path))
            .or_else(|| t.durable_alive.get(&path))
        {
            if let Some(s) = s.upgrade() {
                for (f, c) in updates {
                    s.updates(f, c)
                }
                return s;
            }
        }
        let s = Dval(Arc::new(Mutex::new(DvalInner {
            sub_id: SubId::new(),
            sub: DvState::Dead(Box::new(DvDead {
                queued_writes: Vec::new(),
                waiting: Vec::new(),
                tries: 0,
                next_try: Instant::now(),
                errors: SubscribeErrors::default(),
            })),
            streams: SmallVec::from_iter(
                updates.into_iter().map(|(f, c)| (f, ChanWrap(c))),
            ),
        })));
        t.durable_dead.insert(path, s.downgrade());
        let _ = t.trigger_resub.unbounded_send(());
        s
    }

    /// Create a durable subscription with updates channels.
    ///
    /// Batching of durable subscriptions is automatic, if you create
    /// a lot of durable subscriptions all at once they will batch.
    ///
    /// The semantics of `durable_subscribe` are the same as
    /// subscribe_nondurable, except that certain errors are caught,
    /// and resubscriptions are attempted. see `Dval`.
    ///
    /// If the specified path is already subscribed then the specified
    /// updates channels will be registered with the dval as if
    /// dval.updates has been called.
    pub fn subscribe_updates<I>(&self, path: Path, updates: I) -> Dval
    where
        I: IntoIterator<Item = (UpdatesFlags, Sender<GPooled<Vec<(SubId, Event)>>>)>,
    {
        self.subscribe_internal(path, updates)
    }

    /// Create a durable subscription.
    ///
    /// Batching of durable subscriptions is automatic, if you create
    /// a lot of durable subscriptions all at once they will batch.
    ///
    /// The semantics of `durable_subscribe` are the same as
    /// subscribe_nondurable, except that certain errors are caught,
    /// and resubscriptions are attempted. see `Dval`.
    pub fn subscribe(&self, path: Path) -> Dval {
        self.subscribe_internal(path, [])
    }

    /// Wait for all pending operations to flush to publishers.
    ///
    /// This is primarially used to provide
    /// pushback in the case you want to do a lot of writes, and you
    /// need pushback in case a publisher is slow to process them,
    /// however it applies to durable_subscribe and unsubscribe as well.
    pub async fn flush(&self) {
        let flushes = {
            let t = self.0.lock();
            t.connections
                .values()
                .flat_map(|c| {
                    c.iter().map(|c| {
                        let (tx, rx) = oneshot::channel();
                        c.send(ToCon::Flush(tx));
                        rx
                    })
                })
                .collect::<Vec<_>>()
        };
        for flush in flushes {
            let _ = flush.await;
        }
    }
}
