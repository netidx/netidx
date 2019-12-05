use std::{
    self, io,
    result::Result,
    marker::PhantomData,
    convert::AsRef,
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddrV4, SocketAddr, ToSocketAddrs},
    sync::{Arc, Weak, RwLock, Mutex},
    default::Default,
};
use async_std::{
    prelude::*,
    net::{TcpStream, TcpListener},
}
use fxhash::FxBuildHasher;
use rand::{self, distributions::IndependentSample};
use futures::{channel::{oneshot, mpsc::{channel, Receiver, Sender}}};
use serde::{Serialize, de::DeserializeOwned};
use serde_json;
use path::Path;
use resolver::{Resolver, ReadWrite};
use line_writer::LineWriter;
use utils::Encoded;
use failure::Error;
use crossbeam::queue::SeqQueue;

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
    Unsubscribe(Path)
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
    Unsubscribed(Path),
    /// The next line contains an updated value for Path
    Message(Path)
}

/*
impl Drop for PublishedUntypedInner {
    fn drop(&mut self) {
        let mut t = self.publisher.0.write().unwrap();
        t.published.remove(&self.path);
        let r = t.resolver.clone();
        let addr = t.addr;
        drop(t);
        let path = self.path.clone();
        spawn(async_block! { await!(r.unpublish(path, addr)).map_err(|_| ()) });
        let msg =
            serde_json::to_vec(&FromPublisher::Unsubscribed(self.path.clone()))
            .expect("failed to encode unsubscribed message");
        let msg = Encoded::new(msg);
        for (_, c) in self.subscribed.iter() {
            c.write_one(msg.clone());
            c.flush_nowait();
        }
    }
}
*/

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Id(u64);

impl Id {
    fn zero() -> Self {
        Id(0)
    }

    fn succ(&self) -> Self {
        Id(self.0 + 1)
    }
}

struct PublishedInner<T> {
    id: Id,
    publisher: PublisherWeak,
    updates: Arc<SeqQueue<(Id, Arc<[u8]>)>>,
    typ: PhantomData<T>,
}

impl<T> Drop for PublisherInner<T> {
    fn drop(&mut self) {
        if let Some(p) = self.publisher.upgrade() {
            task::spawn(async move { p.unpublish(self.id) });
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
    /// locking occurs during update, just a push to a concurrent
    /// queue (assuming your platform supports the necessary atomic
    /// operations).
    pub fn update(&self, v: &T) -> Result<(), Error> {
        let id = self.0.id;
        self.0.updates.push(Arc::from(rmp_serde::encode::to_vec_named((id, v))?));
        Ok(())
    }
}

struct ToClient {
    msgs: Vec<Arc<[u8]>>,
    done: oneshot::Sender<()>
}

struct PublishedUntyped {
    header: Arc<[u8]>,
    current: Arc<[u8]>,
    subscribed: HashMap<SocketAddr, Sender<ToClient>, FxBuildHasher>,
}

struct PublisherInner {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    clients: HashMap<SocketAddr, Sender<ToClient>, FxBuildHasher>,
    next_id: Id,
    by_path: HashMap<Path, Id>,
    by_id: HashMap<Id, PublishedUntyped, FxBuildHasher>,
    updates: Arc<SeqQueue<(Id, Arc<[u8]>)>>,
    resolver: Resolver<ReadWrite>
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
                let _ = resolver.unpublish(paths).await
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
struct PublisherWeak(Weak<RwLock<PublisherInner>>);

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
pub struct Publisher(Arc<RwLock<PublisherInner>>);

impl Publisher {
    fn downgrade(&self) -> PublisherWeak {
        PublisherWeak(Arc::downgrade(&self.0))
    }

    /// Create a new publisher, and start the listener.
    pub async fn new<T: ToSocketAddrs>(
        resolver: T,
        bind_cfg: BindCfg
    ) -> Result<Publisher, Error> {
        let (addr, listener) = match bind_cfg {
            BindCfg::Exact(addr) => (addr, TcpListener::bind(&addr).await?),
            BindCfg::Local | BindCfg::Any | BindCfg::Addr(_) => {
                let range = rand::distributions::Range::new(0u16, 10u16);
                let ip = match bind_cfg {
                    BindCfg::Exact(_) => unreachable!(),
                    BindCfg::Local => IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
                    BindCfg::Any => IpAddr::from(Ipv4Addr::new(0, 0, 0, 0)),
                    BindCfg::Addr(addr) => addr,
                };
                let mut rng = rand::thread_rng();
                let mut port = 5000;
                let mut addr = (ip, port).to_socket_addrs().next().unwrap();
                loop {
                    if port >= 32768 { bail!("couldn't allocate a port"); }
                    port += range.ind_sample(&mut rng);
                    addr = (ip, port).to_socket_addrs().next().unwrap();
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
        let pb = Publisher(Arc::new(RwLock::new(PublisherInner {
            addr,
            stop: Some(stop),
            clients: HashMap::with_hasher(FxBuildHasher::default()),
            next_id: Id::zero(),
            by_path: HashMap::new(),
            by_id: HashMap::with_hasher(FxBuildHasher::default()),
            updates: SeqQueue::new(),
            resolver
        })));
        task::spawn({
            let pb_weak = pb.downgrade();
            async move {
                // CR estokes: log the error?
                accept_loop(pb_weak.clone(), listener, receive_stop).await;
                if let Some(pb) = pb_weak.upgrade() {
                    pb.write().unwrap().shutdown();
                }
            }
        });
        Ok(pb)
    }

    /// get the `SocketAddr` that publisher is bound to
    pub fn addr(&self) -> SocketAddr { self.0.read().unwrap().addr }

    /// Publish `Path` with initial value `init`. Subscribers can
    /// subscribe to the value as soon as the future returned by this
    /// function is ready.
    ///
    /// Multiple publishers may publish values at the same path. See
    /// `subscriber::Subscriber::subscribe` for details.
    #[async]
    pub fn publish<T: Serialize + 'static>(
        self, path: Path, init: T
    ) -> Result<Published<T>> {
        let header_msg = FromPublisher::Message(path.clone());
        let inner = PublishedUntypedInner {
            publisher: self.clone(),
            path: path.clone(),
            on_write: None,
            message_header: Encoded::new(serde_json::to_vec(&header_msg)?),
            current: Encoded::new(serde_json::to_vec(&init)?),
            subscribed: HashMap::new()
        };
        let ut = PublishedUntyped(Arc::new(Mutex::new(inner)));
        let (addr, resolver) = {
            let mut t = self.0.write().unwrap();
            if t.published.contains_key(&path) {
                bail!(ErrorKind::AlreadyPublished(path))
            } else {
                t.published.insert(path.clone(), ut.downgrade());
                (t.addr, t.resolver.clone())
            }
        };
        await!(resolver.publish(path, addr))?;
        Ok(Published(ut, PhantomData))
    }
    
    /// Send all queued updates out to subscribers. When the future
    /// returned by this function is ready all data has been flushed to
    /// the underlying OS sockets.
    ///
    /// If you don't want to wait for the future you can just throw it
    /// away, `flush` triggers sending the data whether you await the
    /// future or not.
    pub fn flush(self) -> impl Future<Item=(), Error=Error> {
        let flushes =
            self.0.read().unwrap().clients.iter()
            .map(|(_, c)| c.clone().flush())
            .collect::<Vec<_>>();
        async_block! {
            for flush in flushes.into_iter() { await!(flush)? }
            Ok(())
        }
    }

    /// Returns a future that will become ready when there are `n` or
    /// more subscribers subscribing to at least one value.
    #[async]
    pub fn wait_client(self, n: usize) -> Result<()> {
        let rx = {
            let mut t = self.0.write().unwrap();
            if t.clients.len() >= n { return Ok(()) }
            else {
                let (tx, rx) = oneshot::channel();
                t.wait_clients.push(tx);
                rx
            }
        };
        Ok(await!(rx)?)
    }

    /// Returns the number of subscribers subscribing to at least one value.
    pub fn clients(&self) -> usize { self.0.read().unwrap().clients.len() }
}

fn get_published(t: &PublisherWeak, path: &Path) -> Option<PublishedUntyped> {
    t.upgrade().and_then(|t| {
        let t = t.0.read().unwrap();
        t.published.get(path).and_then(|p| p.upgrade())
    })
}

#[async]
fn client_loop<S>(
    t: PublisherWeak,
    addr: SocketAddr,
    msgs: S,
    writer: LineWriter<Encoded, TcpStream>
) -> Result<()>
where S: Stream<Item=String, Error=Error> + 'static {
    enum C { Msg(String), Stop };
    let stop = writer.clone().on_shutdown().into_stream().then(|_| Ok(C::Stop));
    let msgs = msgs.map(|m| C::Msg(m));
    let mut wait_set: Option<Path> = None;
    #[async]
    for msg in msgs.select(stop) {
        match msg {
            C::Stop => break,
            C::Msg(msg) => {
                let mut ws = None;
                std::mem::swap(&mut wait_set, &mut ws);
                match ws {
                    Some(path) => {
                        if let Some(published) = get_published(&t, &path) {
                            let mut inner = published.0.lock().unwrap();
                            // CR estokes: this is asking for a deadlock
                            if let Some(ref mut f) = inner.on_write { (f)(msg) }
                        }
                    },
                    None =>
                        match serde_json::from_str(&msg)? {
                            ToPublisher::Set(path) => { wait_set = Some(path); },
                            ToPublisher::Unsubscribe(s) => {
                                if let Some(published) = get_published(&t, &s) {
                                    published.0.lock().unwrap().subscribed.remove(&addr);
                                }
                                let resp =
                                    serde_json::to_vec(&FromPublisher::Unsubscribed(s))?;
                                writer.write_one(Encoded::new(resp));
                                writer.flush_nowait();
                            },
                            ToPublisher::Subscribe(s) =>
                                match get_published(&t, &s) {
                                    None => {
                                        let v =
                                            serde_json::to_vec(
                                                &FromPublisher::NoSuchValue(s)
                                            )?;
                                        writer.write_one(Encoded::new(v));
                                        writer.flush_nowait();
                                    },
                                    Some(published) => {
                                        let mut inner = published.0.lock().unwrap();
                                        inner.subscribed.insert(addr, writer.clone());
                                        let c = inner.message_header.clone();
                                        let v = inner.current.clone();
                                        writer.write_two(c, v);
                                        writer.flush_nowait();
                                    }
                                },
                        }
                }
            }
        }
    }
    Ok(())
}

#[async]
fn start_client(
    t: PublisherWeak,
    reader: ReadHalf<TcpStream>,
    writer: LineWriter<Encoded, TcpStream>,
    addr: SocketAddr
) -> result::Result<(), ()> {
    let msgs = tokio::io::lines(BufReader::new(reader)).then(|l| Ok(l?));
    // CR estokes: Do something with this error
    let _ = await!(client_loop(t.clone(), addr, msgs, writer.clone()));
    writer.shutdown();
    if let Some(t) = t.upgrade() {
        let mut pb = t.0.write().unwrap();
        pb.clients.remove(&addr);
        let published = pb.published.clone();
        drop(pb);
        for (_, pv) in published.into_iter() {
            if let Some(pv) = pv.upgrade() {
                let mut inner = pv.0.lock().unwrap();
                inner.subscribed.remove(&addr);
            }
        }
    }
    Ok(())
}

async fn accept_loop(
    t: PublisherWeak,
    serv: TcpListener,
    stop: oneshot::Receiver<()>,
) -> Result<(), Error> {
    enum M { Client(Result<TcpStream, ), Stop };
    let accepted =
        serv.incoming().map(|x| Combined::Client(x)).map_err(|e| Error::from(e));
    let stop = stop.map(|_| Combined::Stop).map_err(|()| Error::from("ipc error"));
    #[async]
    for msg in accepted.select(stop) {
        let t =
            match t.upgrade() {
                None => return Ok(()),
                Some(t) => t
            };
        match msg {
            Combined::Stop => break,
            Combined::Client(cl) => {
                let mut pb = t.0.write().unwrap();
                if pb.clients.len() < MAX_CLIENTS {
                    match cl.peer_addr() {
                        Err(_) => (),
                        Ok(addr) => {
                            cl.set_nodelay(true).unwrap_or(());
                            let (read, write) = cl.split();
                            let writer = LineWriter::new(write);
                            pb.clients.insert(addr, writer.clone());
                            for s in pb.wait_clients.drain(0..) { let _ = s.send(()); }
                            drop(pb);
                            spawn(start_client(t.downgrade(), read, writer, addr));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
