use crate::{
    auth::{
        syskrb5::{ClientCtx, ServerCtx, SYS_KRB5},
        Krb5Ctx,
    },
    channel::Channel,
    config,
    path::Path,
    protocol::{
        publisher::{self, Id},
        resolver::ResolverId,
    },
    resolver::{Auth, ResolverWrite},
    utils::mp_encode,
};
use bytes::Bytes;
use crossbeam::queue::SegQueue;
use failure::Error;
use futures::{prelude::*, select};
use fxhash::FxBuildHasher;
use parking_lot::{Mutex, RwLock};
use rand::{self, Rng};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
    default::Default,
    marker::PhantomData,
    mem,
    net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
    result::Result,
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

// CR estokes: add a handler for lazy publishing (delegated subtrees)

static MAX_CLIENTS: usize = 768;

#[derive(Clone)]
enum Update {
    Val(Bytes),
    Unpublish,
}

struct UValInner {
    id: Id,
    path: Path,
    publisher: PublisherWeak,
    updates: Arc<SegQueue<(Id, Update)>>,
}

impl Drop for UValInner {
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
/// Arc, so cloning it is free. When all references to a given
/// published value have been dropped it will be unpublished.  However
/// you must call flush before references to it will be removed from
/// the resolver server.
#[derive(Clone)]
pub struct Val<T>(Arc<UValInner>, PhantomData<T>);

impl<T: Serialize> Val<T> {
    /// Queue an update to the published value, it will not be sent
    /// out until you call `flush` on the publisher. New subscribers
    /// will not see the new value until you have called
    /// `flush`. Multiple updates can be queued before flush is
    /// called, in which case after `flush` is called new subscribers
    /// will see the last queued value, and existing subscribers will
    /// receive all the queued values in order. If updates are queued
    /// on multiple different published values before `flush` is
    /// called, they will all be sent out as a batch in the order that
    /// update is called.
    ///
    /// The thread calling update pays the serialization cost. No
    /// locking occurs during update.
    pub fn update(&self, v: &T) -> Result<(), Error> {
        Ok(self.0.updates.push((self.0.id, Update::Val(mp_encode(v)?))))
    }

    /// Get the unique `Id` of this `Val`. This id is unique on this
    /// publisher only, no attempt is made to make it globally unique.
    pub fn id(&self) -> Id {
        self.0.id
    }

    /// Get a reference to the `Path` of this published value.
    pub fn path(&self) -> &Path {
        &self.0.path
    }
}

/// Except that you send raw bytes, this is exactly the same as a
/// `Val`. You can mix `UVal` and `Val` on the same publisher. The
/// bytes you send will be received by the subscriber as a discrete
/// message, so there is no need to handle multiple messages in a
/// buffer, or partial messages, or length encoding (the length of the
/// `Bytes` is the length of the message).
#[derive(Clone)]
pub struct UVal(Arc<UValInner>);

impl UVal {
    /// Same as `Val::update`, but untyped.
    pub fn update(&self, v: Bytes) {
        self.0.updates.push((self.0.id, Update::Val(v)));
    }

    /// Same as `Val::id`
    pub fn id(&self) -> Id {
        self.0.id
    }

    /// Same as `Val::path`
    pub fn path(&self) -> &Path {
        &self.0.path
    }
}

#[derive(Debug)]
enum ToClientMsg {
    Val(Bytes, Bytes),
    Unpublish(Bytes),
}

#[derive(Debug)]
struct ToClient {
    msgs: Vec<ToClientMsg>,
    done: oneshot::Sender<()>,
    timeout: Option<Duration>,
}

struct PublishedUVal {
    header: Bytes,
    current: Bytes,
    subscribed: HashMap<SocketAddr, Sender<ToClient>, FxBuildHasher>,
    wait_client: Vec<oneshot::Sender<()>>,
}

struct Client {
    to_client: Sender<ToClient>,
    subscribed: HashSet<Id, FxBuildHasher>,
}

struct PublisherInner {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    clients: HashMap<SocketAddr, Client, FxBuildHasher>,
    by_path: HashMap<Path, Id>,
    by_id: HashMap<Id, PublishedUVal, FxBuildHasher>,
    updates: Arc<SegQueue<(Id, Update)>>,
    resolver: ResolverWrite,
    to_publish: HashSet<Path>,
    to_unpublish: HashSet<Path>,
    wait_any_client: Vec<oneshot::Sender<()>>,
}

impl PublisherInner {
    fn cleanup(&mut self) -> bool {
        match mem::replace(&mut self.stop, None) {
            None => false,
            Some(stop) => {
                let _ = stop.send(());
                while let Ok(_) = self.updates.pop() {}
                self.clients.clear();
                self.by_id.clear();
                true
            }
        }
    }

    async fn shutdown(&mut self) {
        if self.cleanup() {
            // CR estokes: report this, or is best effort the right approach?
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
    Exact(SocketAddr),
}

impl Default for BindCfg {
    fn default() -> Self {
        BindCfg::Any
    }
}

fn rand_port(current: u16) -> u16 {
    let mut rng = rand::thread_rng();
    current + rng.gen_range(0u16, 10u16)
}

/// Publisher allows to publish values, and gives central control of
/// flushing queued updates. Publisher is internally wrapped in an
/// Arc, so cloning it is virtually free. When all references to to
/// the publisher have been dropped the publisher will shutdown the
/// listener, and remove all published paths from the resolver server.
#[derive(Clone)]
pub struct Publisher(Arc<Mutex<PublisherInner>>);

impl Publisher {
    fn downgrade(&self) -> PublisherWeak {
        PublisherWeak(Arc::downgrade(&self.0))
    }

    /// Create a new publisher using the specified resolver and bind config.
    pub async fn new(
        resolver: config::resolver::Config,
        desired_auth: Auth,
        bind_cfg: BindCfg,
    ) -> Result<Publisher, Error> {
        let (addr, listener) = match bind_cfg {
            BindCfg::Exact(addr) => (addr, TcpListener::bind(&addr).await?),
            BindCfg::Local | BindCfg::Any | BindCfg::Addr(_) => {
                let mkaddr = |ip: IpAddr, port: u16| -> Result<SocketAddr, Error> {
                    Ok((ip, port)
                        .to_socket_addrs()?
                        .next()
                        .ok_or_else(|| format_err!("socketaddrs bug"))?)
                };
                let ip = match bind_cfg {
                    BindCfg::Exact(_) => unreachable!(),
                    BindCfg::Local => IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)),
                    BindCfg::Any => IpAddr::from(Ipv4Addr::new(0, 0, 0, 0)),
                    BindCfg::Addr(addr) => addr,
                };
                let mut port = 5000;
                loop {
                    if port >= 32768 {
                        bail!("couldn't allocate a port");
                    }
                    port = rand_port(port);
                    let addr = mkaddr(ip, port)?;
                    match TcpListener::bind(&addr).await {
                        Ok(l) => break (addr, l),
                        Err(e) => {
                            if e.kind() != std::io::ErrorKind::AddrInUse {
                                bail!(e)
                            }
                        }
                    }
                }
            }
        };
        let resolver = ResolverWrite::new(resolver, desired_auth.clone(), addr)?;
        let (stop, receive_stop) = oneshot::channel();
        let pb = Publisher(Arc::new(Mutex::new(PublisherInner {
            addr,
            stop: Some(stop),
            clients: HashMap::with_hasher(FxBuildHasher::default()),
            by_path: HashMap::new(),
            by_id: HashMap::with_hasher(FxBuildHasher::default()),
            updates: Arc::new(SegQueue::new()),
            resolver,
            to_publish: HashSet::new(),
            to_unpublish: HashSet::new(),
            wait_any_client: Vec::new(),
        })));
        task::spawn({
            let pb_weak = pb.downgrade();
            async move {
                accept_loop(pb_weak.clone(), listener, receive_stop, desired_auth).await;
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
    pub async fn shutdown(self) -> Result<(), Error> {
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

    fn publish_val_internal(
        &self,
        path: Path,
        init: Bytes,
    ) -> Result<Arc<UValInner>, Error> {
        let mut pb = self.0.lock();
        if !Path::is_absolute(&path) {
            Err(format_err!("can't publish to relative path"))
        } else if pb.stop.is_none() {
            Err(format_err!("publisher is dead"))
        } else if pb.by_path.contains_key(&path) {
            Err(format_err!("already published"))
        } else {
            let id = Id::new();
            let header = mp_encode(&publisher::From::Message(id))?;
            pb.by_path.insert(path.clone(), id);
            pb.by_id.insert(
                id,
                PublishedUVal {
                    header,
                    current: init,
                    subscribed: HashMap::with_hasher(FxBuildHasher::default()),
                    wait_client: Vec::new(),
                },
            );
            if !pb.to_unpublish.remove(&path) {
                pb.to_publish.insert(path.clone());
            }
            Ok(Arc::new(UValInner {
                id,
                path,
                publisher: self.downgrade(),
                updates: Arc::clone(&pb.updates),
            }))
        }
    }

    /// Publish `Path` with initial value `init`. It is an error for
    /// the same publisher to publish the same path twice, however
    /// different publishers may publish a given path as many times as
    /// they like. Subscribers will then pick randomly among the
    /// advertised publishers when subscribing. See the `subscriber`
    /// module for details.
    pub fn publish_val<T: Serialize>(
        &self,
        path: Path,
        init: &T,
    ) -> Result<Val<T>, Error> {
        let init = mp_encode(init)?;
        Ok(Val(self.publish_val_internal(path, init)?, PhantomData))
    }

    /// Publish `Path` with initial raw `Bytes` `init`. This is the
    /// otherwise exactly the same as publish.
    pub fn publish_val_ut(&self, path: Path, init: Bytes) -> Result<UVal, Error> {
        Ok(UVal(self.publish_val_internal(path, init)?))
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
                    to_client: ToClient {
                        msgs: Vec::new(),
                        done,
                        timeout,
                    },
                }
            })
        }
        let mut to_clients: HashMap<SocketAddr, Tc, FxBuildHasher> =
            HashMap::with_hasher(FxBuildHasher::default());
        let mut flushes = Vec::<oneshot::Receiver<()>>::new();
        let mut to_publish = Vec::new();
        let mut to_unpublish = Vec::new();
        let resolver = {
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
                                    *addr,
                                    sender,
                                    &mut flushes,
                                    &mut to_clients,
                                    timeout,
                                );
                                tc.to_client
                                    .msgs
                                    .push(ToClientMsg::Val(ut.header.clone(), m.clone()));
                            }
                        }
                    }
                    Update::Unpublish => {
                        let m = mp_encode(&publisher::From::Unsubscribed(id))?;
                        if let Some(ut) = pb.by_id.remove(&id) {
                            for (addr, sender) in ut.subscribed {
                                if let Some(cl) = pb.clients.get_mut(&addr) {
                                    cl.subscribed.remove(&id);
                                    let tc = get_tc(
                                        addr,
                                        &sender,
                                        &mut flushes,
                                        &mut to_clients,
                                        timeout,
                                    );
                                    tc.to_client
                                        .msgs
                                        .push(ToClientMsg::Unpublish(m.clone()));
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
    pub fn clients(&self) -> usize {
        self.0.lock().clients.len()
    }

    /// Wait for at least one client to connect
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
            match inner.by_id.get_mut(&id) {
                None => return,
                Some(ut) => {
                    if ut.subscribed.len() > 0 {
                        return;
                    } else {
                        let (tx, rx) = oneshot::channel();
                        ut.wait_client.push(tx);
                        rx
                    }
                }
            }
        };
        let _ = wait.await;
    }
}

fn subscribe(
    t: &mut PublisherInner,
    con: &mut Channel<ServerCtx>,
    addr: SocketAddr,
    path: Path,
) -> Result<(), Error> {
    match t.by_path.get(&path) {
        None => con.queue_send(&publisher::From::NoSuchValue(path))?,
        Some(id) => {
            let id = *id;
            let cl = t.clients.get_mut(&addr).unwrap();
            cl.subscribed.insert(id);
            let sender = cl.to_client.clone();
            let ut = t.by_id.get_mut(&id).unwrap();
            ut.subscribed.insert(addr, sender);
            let mut msg = con.begin_msg();
            msg.pack(&publisher::From::Subscribed(path, id))?;
            msg.pack_ut(&ut.current);
            msg.finish()?;
            for tx in ut.wait_client.drain(..) {
                let _ = tx.send(());
            }
        }
    }
    Ok(())
}

fn handle_batch(
    t: &PublisherWeak,
    addr: &SocketAddr,
    msgs: impl Iterator<Item = publisher::To>,
    con: &mut Channel<ServerCtx>,
    ctxts: &Arc<RwLock<HashMap<ResolverId, ClientCtx, FxBuildHasher>>>,
    auth: &Auth,
    now: u64,
) -> Result<(), Error> {
    use crate::protocol::{
        publisher::{From, To::*},
        resolver::PermissionToken,
    };
    use rmp_serde::decode::{from_read_ref, Error as DErr};
    let t_st = t.upgrade().ok_or_else(|| format_err!("dead publisher"))?;
    let mut pb = t_st.0.lock();
    let ctxts = ctxts.read();
    for msg in msgs {
        match msg {
            Subscribe {
                path,
                resolver,
                token,
            } => match auth {
                Auth::Anonymous => subscribe(&mut *pb, con, *addr, path)?,
                Auth::Krb5 { .. } => match ctxts.get(&resolver) {
                    None => con.queue_send(&From::Denied(path))?,
                    Some(ctx) => match ctx.unwrap(&token) {
                        Err(_) => con.queue_send(&From::Denied(path))?,
                        Ok(b) => {
                            let tok: Result<PermissionToken, DErr> = from_read_ref(&*b);
                            match tok {
                                Err(_) => con.queue_send(&From::Denied(path))?,
                                Ok(PermissionToken(a_path, ts)) => {
                                    let age = std::cmp::max(
                                        u64::saturating_sub(now, ts),
                                        u64::saturating_sub(ts, now),
                                    );
                                    if age > 300 || a_path != &*path {
                                        con.queue_send(&From::Denied(path))?
                                    } else {
                                        subscribe(&mut *pb, con, *addr, path)?
                                    }
                                }
                            }
                        }
                    },
                },
            },
            Unsubscribe(id) => {
                if let Some(ut) = pb.by_id.get_mut(&id) {
                    ut.subscribed.remove(&addr);
                    pb.clients.get_mut(&addr).unwrap().subscribed.remove(&id);
                }
                con.queue_send(&From::Unsubscribed(id))?;
            }
        }
    }
    Ok(())
}

const HB: Duration = Duration::from_secs(5);

async fn hello_client(
    ctxts: &Arc<RwLock<HashMap<ResolverId, ClientCtx, FxBuildHasher>>>,
    con: &mut Channel<ServerCtx>,
    auth: &Auth,
) -> Result<(), Error> {
    use crate::{
        auth::Krb5,
        protocol::publisher::Hello::{self, *},
    };
    let hello: Hello = con.receive().await?;
    match hello {
        Anonymous => con.send_one(&Anonymous).await?,
        Token(tok) => match auth {
            Auth::Anonymous => bail!("authentication not supported"),
            Auth::Krb5 { principal } => {
                let p = principal.as_ref().map(|p| p.as_bytes());
                let ctx = SYS_KRB5.create_server_ctx(p)?;
                let tok = ctx
                    .step(Some(&*tok))?
                    .map(|b| Vec::from(&*b))
                    .ok_or_else(|| format_err!("expected step to generate a token"))?;
                con.set_ctx(Some(ctx));
                con.send_one(&Token(tok)).await?;
            }
        },
        ResolverAuthenticate(id, tok) => {
            let ctx = ctxts.read().get(&id).cloned();
            match ctx {
                None => bail!("no security context"),
                Some(ctx) => {
                    let n = ctx.unwrap(&*tok)?;
                    let n = u64::from_be_bytes(TryFrom::try_from(&*n)?);
                    let tok = Vec::from(&*ctx.wrap(true, &(n + 2).to_be_bytes())?);
                    con.send_one(&ResolverAuthenticate(id, tok)).await?;
                }
            }
        }
    }
    Ok(())
}

async fn client_loop(
    t: PublisherWeak,
    ctxts: Arc<RwLock<HashMap<ResolverId, ClientCtx, FxBuildHasher>>>,
    addr: SocketAddr,
    msgs: Receiver<ToClient>,
    s: TcpStream,
    desired_auth: Auth,
) -> Result<(), Error> {
    let mut con: Channel<ServerCtx> = Channel::new(s);
    let mut batch: Vec<publisher::To> = Vec::new();
    let mut msgs = msgs.fuse();
    let mut hb = time::interval(HB).fuse();
    let mut msg_sent = false;
    hello_client(&ctxts, &mut con, &desired_auth).await?;
    loop {
        select! {
            _ = hb.next() => {
                if !msg_sent {
                    con.queue_send(&publisher::From::Heartbeat)?;
                    con.flush().await?;
                }
                msg_sent = false;
            },
            from_cl = con.receive_batch(&mut batch).fuse() => match from_cl {
                Err(e) => return Err(Error::from(e)),
                Ok(()) => {
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)?
                        .as_secs();
                    handle_batch(
                        &t, &addr, batch.drain(..), &mut con,
                        &ctxts, &desired_auth, now,
                    )?;
                    con.flush().await?
                }
            },
            to_cl = msgs.next() => match to_cl {
                None => break Ok(()),
                Some(m) => {
                    msg_sent = true;
                    for msg in m.msgs {
                        match msg {
                            ToClientMsg::Val(hdr, body) => {
                                con.queue_send_ut(&[hdr, body])?;
                            }
                            ToClientMsg::Unpublish(m) => {
                                con.queue_send_ut(&[m])?;
                            }
                        }
                    }
                    let f = con.flush();
                    match m.timeout {
                        None => f.await?,
                        Some(d) => time::timeout(d, f).await??
                    }
                    let _ = m.done.send(());
                }
            }
        }
    }
}

async fn accept_loop(
    t: PublisherWeak,
    mut serv: TcpListener,
    stop: oneshot::Receiver<()>,
    desired_auth: Auth,
) {
    let mut stop = stop.fuse();
    loop {
        select! {
            _ = stop => break,
            cl = serv.accept().fuse() => match cl {
                Err(_) => (), // CR estokes: log this? exit the loop?
                Ok((s, addr)) => {
                    let t_weak = t.clone();
                    let t = match t.upgrade() {
                        None => return,
                        Some(t) => t
                    };
                    let mut pb = t.0.lock();
                    let ctxts = pb.resolver.ctxts();
                    if pb.clients.len() < MAX_CLIENTS {
                        let (tx, rx) = channel(100);
                        try_cont!("nodelay", s.set_nodelay(true));
                        pb.clients.insert(addr, Client {
                            to_client: tx,
                            subscribed: HashSet::with_hasher(FxBuildHasher::default()),
                        });
                        for tx in pb.wait_any_client.drain(..) {
                            let _ = tx.send(());
                        }
                        let desired_auth = desired_auth.clone();
                        task::spawn(async move {
                            let _ = client_loop(
                                t_weak.clone(), ctxts, addr, rx, s, desired_auth
                            ).await;
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
            },
        }
    }
}
