use std::{
    self, io, iter, mem,
    result::Result,
    marker::PhantomData,
    convert::AsRef,
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddrV4, SocketAddr, ToSocketAddrs},
    sync::{Arc, Weak, Mutex},
    default::Default,
    cell::RefCell,
    time::Duration,
};
use async_std::{
    future, task,
    prelude::*,
    net::{TcpStream, TcpListener},
};
use fxhash::FxBuildHasher;
use rand::{self, Rng};
use futures::{
    stream,
    channel::{oneshot, mpsc::{channel, Receiver, Sender}},
    sink::SinkExt as FrsSinkExt,
};
use futures_codec::{Framed, LengthCodec};
use serde::{Serialize, de::DeserializeOwned};
use path::Path;
use resolver::{Resolver, ReadWrite};
use line_writer::LineWriter;
use failure::Error;
use crossbeam::queue::SeqQueue;
use bytes::{Bytes, BytesMut};
use utils::BytesWriter;

// TODO
// * add a handler for lazy publishing (delegated subtrees)

static MAX_CLIENTS: usize = 768;

/// This is the set of protocol messages that may be sent to the publisher
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ToPublisher {
    /// Subscribe to the specified value, if it is not available the
    /// result will be NoSuchValue
    Subscribe(Path),
    /// Unsubscribe from the specified value, this will always result
    /// in an Unsubscibed message even if you weren't ever subscribed
    /// to the value, or it doesn't exist.
    Unsubscribe(Id)
}

/// This is the set of protocol messages that may come from the publisher
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FromPublisher {
    /// The requested subscription to Path cannot be completed because
    /// it doesn't exist
    NoSuchValue(Path),
    /// You have been unsubscriped from Path. This can be the result
    /// of an Unsubscribe message, or it may be sent unsolicited, in
    /// the case the value is no longer published, or the publisher is
    /// in the process of shutting down.
    Unsubscribed(Id),
    /// The next message contains an updated value for Id. If you have
    /// pending subscriptions, then Message(new id) indicates that
    /// your subscription was succcessful, and that the next message
    /// contains the current value of the path you subscribed
    /// to. First messages are sent in the order of subscription, and
    /// no messages for any other subscribed paths will be sent
    /// between a subscribe and the first message for that
    /// subscription.
    Message(Id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(u64);

impl Id {
    fn zero() -> Self {
        Id(0)
    }

    fn take(&mut self) -> Self {
        let new = *self;
        self = Id(self.0 + 1);
        new
    }
}

thread_local! {
    static BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(512));
}

struct PublishedInner<T> {
    id: Id,
    publisher: PublisherWeak,
    updates: Arc<SeqQueue<(Id, Bytes)>>,
    typ: PhantomData<T>,
}

impl<T> Drop for PublishedInner<T> {
    fn drop(&mut self) {
        if let Some(p) = self.publisher.upgrade() {
            p.unpublish(self.id);
        }
    }
}

// Err(Dead) should contain the last published value
/// This represents a published value. Internally it is wrapped in an
/// Arc, so cloning it is essentially free. When all references to a
/// given published value have been dropped it will be unpublished.
#[derive(Clone)]
pub struct Published<T>(Arc<PublishedInner<T>>);

impl<T: Serialize> Published<T> {
    /// Queue an update to the published value, it will not be sent
    /// out until you call `flush` on the publisher. New subscribers
    /// will not see the new value until you have called
    /// `flush`. Multiple updates can be queued before flush is
    /// called, in which case after `flush` is called new subscribers
    /// will see the last value, and existing subscribers will receive
    /// all the queued values in order. If updates are queued on
    /// multiple different published values before `flush` is called,
    /// they will all be sent out as a batch in the order that update
    /// is called.
    ///
    /// The thread calling update pays the serialization cost. No
    /// locking occurs during update.
    pub fn update(&self, v: &T) -> Result<(), Error> {
        use rmp_serde::encode::write_named;
        let bytes = BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            write_named(&mut BytesWriter(&mut *buf), v).map(|()| {
                buf.take().freeze()
            })
        })?;
        self.0.updates.push((self.0.id, bytes));
        Ok(())
    }

    pub fn id(&self) -> Id { *self.id }
}

struct ToClient {
    msgs: Vec<Bytes>,
    done: oneshot::Sender<()>,
    timeout: Option<Duration>,
}

struct PublishedUntyped {
    header: Bytes,
    current: Bytes,
    subscribed: HashMap<SocketAddr, Sender<ToClient>, FxBuildHasher>,
}

struct Client {
    to_client: Sender<ToClient>,
    subscribed: HashSet<Id, FxBuildHasher>,
}

struct PublisherInner {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    clients: HashMap<SocketAddr, Client, FxBuildHasher>,
    next_id: Id,
    by_path: HashMap<Path, Id>,
    by_id: HashMap<Id, PublishedUntyped, FxBuildHasher>,
    updates: Arc<SeqQueue<(Id, Bytes)>>,
    resolver: Resolver<ReadWrite>,
    to_publish: HashSet<Path>,
    to_unpublish: HashSet<Path>,
}

impl PublisherInner {
    fn shutdown(&mut self) {
        if let Some(stop) = mem::replace(&mut self.stop, None) {
            let _ = stop.send(());
            self.clients.clear();
            self.by_id.clear();
            while let Ok(_) = self.updates.pop() {}
            let paths = mem::replace(&mut self.by_path, HashMap::new());
            let resolver = self.resolver.clone();
            task::spawn(async move {
                let paths = paths.into_iter().map(|(p, _)| p).collect();
                let _ = resolver.unpublish(paths).await;
            });
        }
    }
}

impl Drop for PublisherInner {
    fn drop(&mut self) {
        self.shutdown()
    }
}

#[derive(Clone)]
struct PublisherWeak(Weak<Mutex<PublisherInner>>);

impl PublisherWeak {
    fn upgrade(&self) -> Option<Publisher> {
        Weak::upgrade(&self.0).map(|r| Publisher(r))
    }
}

/// Control how the publisher picks a `SocketAddr` to bind to
#[derive(Clone, Copy, Debug)]
pub enum BindCfg {
    /// Bind to `127.0.0.1`, automatically pick an unused port starting
    /// at 5000 and ending at the ephemeral port range. Error if no
    /// unused port can be found.
    Local,
    /// Bind to `0.0.0.0`, automatically pick an unused port as in `Local`.
    Any,
    /// Bind to the specified address, but automatically pick an
    /// unused port as in `Local`.
    Addr(IpAddr),
    /// Bind to the specifed `SocketAddr`, error if it is in use.
    Exact(SocketAddr)
}

impl Default for BindCfg {
    fn default() -> Self {
        BindCfg::Any
    }
}

/// Publisher allows to publish values, and gives central control of
/// flushing queued updates. Publisher is internally wrapped in an
/// Arc, so cloning it is virtually free. When all references to
/// published values, and all references to publisher have been
/// dropped the publisher will shutdown the listener.
#[derive(Clone)]
pub struct Publisher(Arc<Mutex<PublisherInner>>);

impl Publisher {
    fn downgrade(&self) -> PublisherWeak {
        PublisherWeak(Arc::downgrade(&self.0))
    }

    /// Create a new publisher using the specified resolver and bind config.
    pub async fn new<T: ToSocketAddrs>(
        resolver: T,
        bind_cfg: BindCfg
    ) -> Result<Publisher, Error> {
        let (addr, listener) = match bind_cfg {
            BindCfg::Exact(addr) => (addr, TcpListener::bind(&addr).await?),
            BindCfg::Local | BindCfg::Any | BindCfg::Addr(_) => {
                let mkaddr = |ip: IpAddr, port: u16| {
                    Ok((ip, port).to_socket_addrs()?.next()
                       .ok_or_else(|| Error::from("socketaddrs bug".into()))?)
                };
                let ip = match bind_cfg {
                    BindCfg::Exact(_) => unreachable!(),
                    BindCfg::Local => IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
                    BindCfg::Any => IpAddr::from(Ipv4Addr::new(0, 0, 0, 0)),
                    BindCfg::Addr(addr) => addr,
                };
                let mut rng = rand::thread_rng();
                let mut port = 5000;
                let mut addr = mkaddr(ip, port)?;
                loop {
                    if port >= 32768 { bail!("couldn't allocate a port"); }
                    port += rng.gen_range(0u16, 10u16);
                    addr = mkaddr(ip, port)?;
                    match TcpListener::bind(&addr).await {
                        Ok(l) => break (addr, l),
                        Err(e) => if e.kind() != std::io::ErrorKind::AddrInUse {
                            bail!(e)
                        }
                    }
                }
            }
        };
        let resolver = Resolver::new_rw(resolver, addr)?;
        let (stop, receive_stop) = oneshot::channel();
        let pb = Publisher(Arc::new(Mutex::new(PublisherInner {
            addr,
            stop: Some(stop),
            clients: HashMap::with_hasher(FxBuildHasher::default()),
            next_id: Id::zero(),
            by_path: HashMap::new(),
            by_id: HashMap::with_hasher(FxBuildHasher::default()),
            updates: SeqQueue::new(),
            resolver,
            to_publish: HashSet::new(),
            to_unpublish: HashSet::new(),
        })));
        task::spawn({
            let pb_weak = pb.downgrade();
            async move {
                // CR estokes: log the error?
                accept_loop(pb_weak.clone(), listener, receive_stop).await;
                if let Some(pb) = pb_weak.upgrade() {
                    pb.0.lock().unwrap().shutdown();
                }
            }
        });
        Ok(pb)
    }

    /// get the `SocketAddr` that publisher is bound to
    pub fn addr(&self) -> SocketAddr { self.0.lock().unwrap().addr }

    /// Publish `Path` with initial value `init`. Subscribers will not
    /// be able to subscribe until you call flush and await the
    /// returned future.
    ///
    /// Multiple publishers may publish values at the same path. See
    /// `subscriber::Subscriber::subscribe` for details.
    pub fn publish<T: Serialize>(
        &self,
        path: Path,
        init: &T
    ) -> Result<Published<T>, Error> {
        let current = BUF.with(|buf| {
            let mut b = buf.borrow_mut();
            rmp_serde::encode::write_named(&mut BytesWriter(&mut *b), init)?;
            Ok(buf.take().freeze())
        })?;
        let mut pb = self.0.lock().unwrap();
        if !Path::is_absolute(path.as_ref()) {
            Error::from("can't publish to relative path");
        } else if pb.stop.is_none() {
            Error::from("publisher is dead")
        } else if pb.by_path.contains_key(&path) {
            Error::from("already published")
        } else {
            let id = pb.next_id.take();
            let header = BUF.with(|buf| {
                let mut b = buf.borrow_mut();
                let m = FromPublisher::Message(id);
                rmp_serde::encode::write_named(&mut BytesWriter(&mut *b), &m)?;
                Ok(buf.take().freeze())
            })?;
            pb.by_path.insert(path.clone(), id);
            pb.by_id.insert(id, PublishedUntyped {
                header, current,
                subscribed: HashMap::with_hasher(FxBuildHasher::default()),
            });
            if !pb.to_unpublish.remove(&path) {
                pb.to_publish.insert(path);
            }
            Ok(Published(Arc::new(PublishedInner {
                id,
                publisher: self.downgrade(),
                updates: pb.updates.clone(),
                typ: PhantomData,
            })))
        }
    }
    
    /// Send all queued updates out to subscribers, and send all
    /// queued publish/unpublish operations to the resolver. When the
    /// future returned by this function is ready all data has been
    /// flushed to the underlying OS sockets.
    ///
    /// If you don't want to wait for the future you can just throw it
    /// away, `flush` triggers sending the data whether you await the
    /// future or not.
    ///
    /// If timeout is specified then any client that can't accept the
    /// update within the timeout duration will be disconnected.
    /// Otherwise flush will wait as long as necessary to flush the
    /// update to every client.
    pub async fn flush(&self, timeout: Option<Duration>) -> Result<(), Error> {
        struct Tc {
            sender: Sender<ToClient>,
            to_client: ToClient,
        }
        use std::collections::hash_map::Entry;
        let mut to_clients: HashMap<SocketAddr, Tc, FxBuildHasher> =
            HashMap::with_hasher(FxBuildHasher::default());
        let mut flushes = Vec::new();
        let mut to_publish = Vec::new();
        let mut to_unpublish = Vec::new();
        let mut resolver = {
            let mut pb = self.0.lock().unwrap();
            to_publish.extend(pb.to_publish.drain());
            to_unpublish.extend(pb.to_unpublish.drain());
            while let Ok((id, m)) = pb.updates.pop() {
                if let Some(ut) = pb.by_id.get(&id) {
                    ut.current = m.clone();
                    for (addr, sender) in ut.subscribed.iter() {
                        match to_clients.entry(*addr) {
                            Entry::Occupied(e) => {
                                let v = e.get_mut();
                                v.to_client.msgs.push(ut.header.clone());
                                v.to_client.msgs.push(m.clone());
                            },
                            Entry::Vacant(e) => {
                                let (tx, rx) = oneshot::channel();
                                flushes.push(rx);
                                e.insert(Tc {
                                    sender: sender.clone(),
                                    to_client: ToClient {
                                        msgs: vec![ut.header.clone(); m.clone()],
                                        done: tx,
                                        timeout
                                    }
                                });
                            }
                        }
                    }
                }
            }
            pb.resolver.clone()
        };
        for (_, mut tc) in to_clients {
            let _ = tc.sender.send(tc.to_client).await;
        }
        if to_publish.len() > 0 {
            resolver.publish(to_publish).await?
        }
        if to_unpublish.len() > 0 {
            resolver.unpublish(to_unpublish).await?
        }
        for rx in flushes {
            let _ = rx.await;
        }
        Ok(())
    }

    /// Returns the number of subscribers subscribing to at least one value.
    pub fn clients(&self) -> usize { self.0.lock().unwrap().clients.len() }
}

async fn handle_client_msg(
    t: &PublisherWeak,
    addr: &SocketAddr,
    codec: &mut Framed<TcpStream, LengthCodec>,
    buf: &mut BytesMut,
    m: Bytes,
) -> Result<(), Error> {
    let msg = from_read::<&[u8], ToPublisher>(&*b)?;
    let t_st = t.upgrade().ok_or_else(|| Error::from("dead publisher"))?;
    let mut pb = t_st.0.lock().unwrap();
    let (reply, v) = match msg {
        ToPublisher::Subscribe(path) => {
            match pb.by_path.get(&path) {
                None => (FromPublisher::NoSuchValue(path), None),
                Some(id) => {
                    let cl = pb.clients.get_mut(&addr).unwrap();
                    let ut = pb.by_id.get_mut(id).unwrap();
                    cl.subscribed.insert(*id);
                    ut.subscribed.insert(addr, cl.to_client.clone());
                    (FromPublisher::Message(*id), Some(ut.current.clone()))
                }
            }
        }
        ToPublisher::Unsubscribe(id) => {
            if let Some(ut) = pb.by_id.get_mut(id) {
                ut.subscribed.remove(&addr);
                pb.clients.get_mut(&addr).unwrap().subscribed.remove(id);
            }
            (FromPublisher::Unsubscribed(*id), None)
        }
    };
    drop(pb);
    write_named(&mut BytesWriter(&mut buf), &reply)?;
    match v {
        None => Ok(codec.send(buf.take().freeze()).await?),
        Some(v) => {
            use std::iter::once;
            let b = buf.take().freeze();
            let mut s = stream::iter(once(Ok(b)).chain(once(Ok(v))));
            Ok(codec.send_all(&mut s).await?)
        }
    }
}

async fn client_loop(
    t: PublisherWeak,
    addr: SocketAddr,
    mut msgs: Receiver<ToClient>,
    s: TcpStream,
) -> Result<(), Error> {
    use rmp_serde::{encode::write_named, decode::from_read};
    enum M { FromCl(Option<Result<Bytes, io::Error>>), ToCl(Option<ToClient>) };
    let mut buf = BytesMut::with_capacity(512);
    let mut codec = Framed::new(s, LengthCodec);
    loop {
        let to_cl = msgs.next().map(|m| M::ToCl(m));
        let from_cl = codec.next().map(|m| M::FromCl(m));
        match to_cl.race(from_cl).await {
            M::FromCl(None) | M::ToCl(None) => break Ok(()),
            M::FromCl(Some(Err(e))) => return Err(Error::from(e)),
            M::FromCl(Some(Ok(b))) =>
                handle_client_msg(&t, &addr, &mut codec, &mut buf, b).await?,
            M::ToCl(Some(m)) => {
                let mut s = stream::iter(m.msgs.into_iter().map(|b| Ok(b)));
                let f = codec.send_all(&mut s);
                match m.timeout {
                    None => f.await?,
                    Some(d) => future::timeout(d, f).await?
                }
                let _ = m.done.send(());
            }
        }
    }
}

async fn accept_loop(
    t: PublisherWeak,
    serv: TcpListener,
    stop: oneshot::Receiver<()>,
) {
    enum M { Client(Result<TcpStream, io::Error>), Stop };
    let stop = stop.map(|_| M::Stop).shared();
    loop {
        let client = serv.accept().map(|r| M::Client(r));
        match stop.clone().race(client) {
            M::Stop => break,
            M::Client(Err(_)) => (), // CR estokes: log this? exit the loop?
            M::Client(Ok((s, addr))) => {
                let t_weak = t.clone();
                let t = match t.upgrade() {
                    None => return Ok(()),
                    Some(t) => t
                };
                let mut pb = t.0.write().unwrap();
                if pb.clients.len() < MAX_CLIENTS {
                    try_cont!("nodelay", cl.nodelay(true));
                    let (tx, rx) = channel(100);
                    pb.clients.insert(addr, Client {
                        to_client: tx,
                        subscribed: HashSet::with_hasher(FxBuildHasher::default()),
                    });
                    drop(pb);
                    task::spawn(async move {
                        let _ = client_loop(t_weak.clone(), addr, rx, s).await;
                        if let Some(t) = t_weak.upgrade() {
                            let mut pb = t.0.write().unwrap();
                            if let Some(cl) = pb.clients.remove(&addr) {
                                for id in cl.subscribed {
                                    if let Some(ut) = pb.by_id.get_mut(&id) {
                                        ut.subscribed.remove(&id);
                                    }
                                }
                            }
                        }
                    });
                }
            }
        }
    }
}
