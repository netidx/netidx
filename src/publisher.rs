use crate::{utils::BytesWriter, path::Path, resolver::{Resolver, ReadWrite}};
use std::{
    self, io, iter, mem,
    result::Result,
    marker::PhantomData,
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
    sync::{Arc, Weak},
    default::Default,
    cell::RefCell,
    time::Duration,
};
use async_std::{
    task, future,
    prelude::*,
    net::{TcpStream, TcpListener},
    stream::StreamExt,
};
use fxhash::FxBuildHasher;
use rand::{self, Rng};
use futures::{
    stream,
    channel::{oneshot, mpsc::{channel, Receiver, Sender}},
    sink::SinkExt as FrsSinkExt,
    future::FutureExt as FrsFutureExt,
};
use futures_codec::{Framed, LengthCodec};
use serde::Serialize;
use failure::Error;
use crossbeam::queue::SegQueue;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;

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

#[derive(
    Serialize, Deserialize, Debug, Clone,
    Copy, PartialEq, Eq, PartialOrd, Ord, Hash
)]
pub struct Id(u64);

impl Id {
    fn zero() -> Self {
        Id(0)
    }

    fn take(&mut self) -> Self {
        let new = *self;
        *self = Id(self.0 + 1);
        new
    }
}

thread_local! {
    static BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(512));
}

fn mp_encode<T: Serialize>(t: &T) -> Result<Bytes, Error> {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        rmp_serde::encode::write_named(&mut BytesWriter(&mut *b), t)?;
        Ok(b.take().freeze())
    })
}

#[derive(Clone)]
enum Update {
    Val(Bytes),
    Unpublish,
}

struct PublishedInner {
    id: Id,
    path: Path,
    publisher: PublisherWeak,
    updates: Arc<SegQueue<(Id, Update)>>,
}

impl Drop for PublishedInner {
    fn drop(&mut self) {
        self.updates.push((self.id, Update::Unpublish));
        if let Some(t) = self.publisher.upgrade() {
            let mut pb = t.0.lock();
            pb.by_path.remove(&self.path);
            if !pb.to_publish.remove(&self.path) {
                pb.to_unpublish.insert(self.path.clone());
            }
        }
    }
}

/// This represents a published value. Internally it is wrapped in an
/// Arc, so cloning it is essentially free. When all references to a
/// given published value have been dropped it will be unpublished.
/// However you must call flush before reference to it will be removed
/// from the resolver server.
#[derive(Clone)]
pub struct Published<T>(Arc<PublishedInner>, PhantomData<T>);

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
        Ok(self.0.updates.push((self.0.id, Update::Val(mp_encode(v)?))))
    }

    pub fn id(&self) -> Id { self.0.id }
    pub fn path(&self) -> &Path { &self.0.path }
}

/// Except that you send raw bytes, this is exactly the same as a
/// `Published`. You can mix `PublishedRaw` and `Published` on the
/// same publisher. The bytes you send will be received by the
/// subscriber as a discrete message, so there is no need to handle
/// multiple messages in a buffer, or partial messages, or length
/// encoding (the length of the `Bytes` is the length of the message).
#[derive(Clone)]
pub struct PublishedRaw(Arc<PublishedInner>);

impl PublishedRaw {
    pub fn update(&self, v: Bytes) {
        self.0.updates.push((self.0.id, Update::Val(v)));
    }

    pub fn id(&self) -> Id { self.0.id }
    pub fn path(&self) -> &Path { &self.0.path }
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
    updates: Arc<SegQueue<(Id, Update)>>,
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
            let mut resolver = self.resolver.clone();
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
                let mkaddr = |ip: IpAddr, port: u16| -> Result<SocketAddr, Error> {
                    Ok((ip, port).to_socket_addrs()?.next()
                       .ok_or_else(|| format_err!("socketaddrs bug"))?)
                };
                let ip = match bind_cfg {
                    BindCfg::Exact(_) => unreachable!(),
                    BindCfg::Local => IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
                    BindCfg::Any => IpAddr::from(Ipv4Addr::new(0, 0, 0, 0)),
                    BindCfg::Addr(addr) => addr,
                };
                let mut rng = rand::thread_rng();
                let mut port = 5000;
                loop {
                    if port >= 32768 { bail!("couldn't allocate a port"); }
                    port += rng.gen_range(0u16, 10u16);
                    let addr = mkaddr(ip, port)?;
                    match TcpListener::bind(&addr).await {
                        Ok(l) => break (addr, l),
                        Err(e) => if e.kind() != std::io::ErrorKind::AddrInUse {
                            bail!(e)
                        }
                    }
                }
            }
        };
        let resolver = Resolver::<ReadWrite>::new_rw(resolver, addr)?;
        let (stop, receive_stop) = oneshot::channel();
        let pb = Publisher(Arc::new(Mutex::new(PublisherInner {
            addr,
            stop: Some(stop),
            clients: HashMap::with_hasher(FxBuildHasher::default()),
            next_id: Id::zero(),
            by_path: HashMap::new(),
            by_id: HashMap::with_hasher(FxBuildHasher::default()),
            updates: Arc::new(SegQueue::new()),
            resolver,
            to_publish: HashSet::new(),
            to_unpublish: HashSet::new(),
        })));
        task::spawn({
            let pb_weak = pb.downgrade();
            async move {
                accept_loop(pb_weak.clone(), listener, receive_stop).await;
                if let Some(pb) = pb_weak.upgrade() {
                    pb.0.lock().shutdown();
                }
            }
        });
        Ok(pb)
    }

    /// get the `SocketAddr` that publisher is bound to
    pub fn addr(&self) -> SocketAddr { self.0.lock().addr }

    fn publish_internal(
        &self,
        path: Path,
        init: Bytes
    ) -> Result<Arc<PublishedInner>, Error> {
        let mut pb = self.0.lock();
        if !Path::is_absolute(&path) {
            Err(format_err!("can't publish to relative path"))
        } else if pb.stop.is_none() {
            Err(format_err!("publisher is dead"))
        } else if pb.by_path.contains_key(&path) {
            Err(format_err!("already published"))
        } else {
            let id = pb.next_id.take();
            let header = mp_encode(&FromPublisher::Message(id))?;
            pb.by_path.insert(path.clone(), id);
            pb.by_id.insert(id, PublishedUntyped {
                header,
                current: init,
                subscribed: HashMap::with_hasher(FxBuildHasher::default()),
            });
            if !pb.to_unpublish.remove(&path) {
                pb.to_publish.insert(path.clone());
            }
            Ok(Arc::new(PublishedInner {
                id, path,
                publisher: self.downgrade(),
                updates: Arc::clone(&pb.updates)
            }))
        }
    }

    /// Publish `Path` with initial value `init`. It is an error for
    /// the same publisher to publish the same path twice, however
    /// different publishers may publish a given path as many times as
    /// they like. Subscribers will then pick randomly among the
    /// advertised publishers when subscribing. See the `subscriber`
    /// module for details.
    pub fn publish<T: Serialize>(
        &self,
        path: Path,
        init: &T
    ) -> Result<Published<T>, Error> {
        let init = mp_encode(init)?;
        Ok(Published(self.publish_internal(path, init)?, PhantomData))
    }

    /// Publish `Path` with initial raw `Bytes` `init`. This is the
    /// otherwise exactly the same as publish.
    pub fn publish_raw(
        &self,
        path: Path,
        init: Bytes,
    ) -> Result<PublishedRaw, Error> {
        Ok(PublishedRaw(self.publish_internal(path, init)?))
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
        fn get_tc<'a>(
            addr: SocketAddr,
            sender: &Sender<ToClient>,
            flushes: &mut Vec<oneshot::Receiver<()>>,
            to_clients: &'a mut HashMap<SocketAddr, Tc, FxBuildHasher>,
            timeout: Option<Duration>,
        ) -> &'a mut Tc {
            to_clients.entry(addr).or_insert_with(|| {
                let (done, rx) = oneshot::channel();
                flushes.push(rx);
                Tc {
                    sender: sender.clone(),
                    to_client: ToClient {msgs: vec![], done, timeout}
                }
            })
        }
        let mut to_clients: HashMap<SocketAddr, Tc, FxBuildHasher> =
            HashMap::with_hasher(FxBuildHasher::default());
        let mut flushes = Vec::new();
        let mut to_publish = Vec::new();
        let mut to_unpublish = Vec::new();
        let mut resolver = {
            let mut pb = self.0.lock();
            to_publish.extend(pb.to_publish.drain());
            to_unpublish.extend(pb.to_unpublish.drain());
            while let Ok((id, m)) = pb.updates.pop() {
                match m {
                    Update::Val(m) => {
                        if let Some(ut) = pb.by_id.get_mut(&id) {
                            ut.current = m.clone();
                            for (addr, sender) in ut.subscribed.iter() {
                                let tc = get_tc(
                                    *addr, sender, &mut flushes, &mut to_clients, timeout
                                );
                                tc.to_client.msgs.push(ut.header.clone());
                                tc.to_client.msgs.push(m.clone());
                            }
                        }
                    }
                    Update::Unpublish => {
                        let m = mp_encode(&FromPublisher::Unsubscribed(id))?;
                        if let Some(ut) = pb.by_id.remove(&id) {
                            for (addr, sender) in ut.subscribed {
                                if let Some(cl) = pb.clients.get_mut(&addr) {
                                    cl.subscribed.remove(&id);
                                    let tc = get_tc(
                                        addr, &sender, &mut flushes, &mut to_clients,
                                        timeout
                                    );
                                    tc.to_client.msgs.push(m.clone());
                                }
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
    pub fn clients(&self) -> usize { self.0.lock().clients.len() }
}

fn handle_client_msg(
    t: &PublisherWeak,
    addr: &SocketAddr,
    buf: &mut BytesMut,
    m: Bytes,
) -> Result<(Bytes, Option<Bytes>), Error> {
    use rmp_serde::{encode::write_named, decode::from_read};
    let msg = from_read::<&[u8], ToPublisher>(&*m)?;
    let t_st = t.upgrade().ok_or_else(|| format_err!("dead publisher"))?;
    let mut pb = t_st.0.lock();
    let (reply, v) = match msg {
        ToPublisher::Subscribe(path) => {
            match pb.by_path.get(&path) {
                None => (FromPublisher::NoSuchValue(path), None),
                Some(id) => {
                    let id = *id;
                    let cl = pb.clients.get_mut(&addr).unwrap();
                    cl.subscribed.insert(id);
                    let sender = cl.to_client.clone();
                    let ut = pb.by_id.get_mut(&id).unwrap();
                    ut.subscribed.insert(*addr, sender);
                    (FromPublisher::Message(id), Some(ut.current.clone()))
                }
            }
        }
        ToPublisher::Unsubscribe(id) => {
            if let Some(ut) = pb.by_id.get_mut(&id) {
                ut.subscribed.remove(&addr);
                pb.clients.get_mut(&addr).unwrap().subscribed.remove(&id);
            }
            (FromPublisher::Unsubscribed(id), None)
        }
    };
    drop(pb);
    write_named(&mut BytesWriter(buf), &reply)?;
    Ok((buf.take().freeze(), v))
}

async fn client_loop(
    t: PublisherWeak,
    addr: SocketAddr,
    mut msgs: Receiver<ToClient>,
    s: TcpStream,
) -> Result<(), Error> {
    enum M { FromCl(Option<Result<Bytes, io::Error>>), ToCl(Option<ToClient>) };
    let mut buf = BytesMut::with_capacity(512);
    let mut codec = Framed::new(s, LengthCodec);
    loop {
        let to_cl = msgs.next().map(|m| M::ToCl(m));
        let from_cl = codec.next().map(|m| M::FromCl(m));
        match to_cl.race(from_cl).await {
            M::FromCl(None) | M::ToCl(None) => break Ok(()),
            M::FromCl(Some(Err(e))) => return Err(Error::from(e)),
            M::FromCl(Some(Ok(b))) => {
                let (reply, v) = handle_client_msg(&t, &addr, &mut buf, b)?;
                match v {
                    None => codec.send(reply).await?,
                    Some(v) => {
                        let mut s =
                            stream::iter(iter::once(Ok(reply)).chain(iter::once(Ok(v))));
                        codec.send_all(&mut s).await?
                    }
                }
            },
            M::ToCl(Some(m)) => {
                let mut s = stream::iter(m.msgs.into_iter().map(|b| Ok(b)));
                let f = codec.send_all(&mut s);
                match m.timeout {
                    None => f.await?,
                    Some(d) => future::timeout(d, f).await??
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
    enum M { Client(Result<(TcpStream, SocketAddr), io::Error>), Stop };
    let stop = stop.shared();
    loop {
        let client = serv.accept().map(|r| M::Client(r));
        match stop.clone().map(|_| M::Stop).race(client).await {
            M::Stop => break,
            M::Client(Err(_)) => (), // CR estokes: log this? exit the loop?
            M::Client(Ok((s, addr))) => {
                let t_weak = t.clone();
                let t = match t.upgrade() {
                    None => return,
                    Some(t) => t
                };
                let mut pb = t.0.lock();
                if pb.clients.len() < MAX_CLIENTS {
                    let (tx, rx) = channel(100);
                    try_cont!("nodelay", s.set_nodelay(true));
                    pb.clients.insert(addr, Client {
                        to_client: tx,
                        subscribed: HashSet::with_hasher(FxBuildHasher::default()),
                    });
                    task::spawn(async move {
                        let _ = client_loop(t_weak.clone(), addr, rx, s).await;
                        if let Some(t) = t_weak.upgrade() {
                            let mut pb = t.0.lock();
                            if let Some(cl) = pb.clients.remove(&addr) {
                                for id in cl.subscribed {
                                    if let Some(ut) = pb.by_id.get_mut(&id) {
                                        ut.subscribed.remove(&addr);
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
