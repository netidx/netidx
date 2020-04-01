use crate::{
    auth::{
        sysgmapper::Mapper,
        syskrb5::{sys_krb5, ServerCtx},
        Krb5, Krb5Ctx, Krb5ServerCtx, PMap, Permissions, UserDb, UserInfo, ANONYMOUS,
    },
    channel::Channel,
    config,
    path::Path,
    protocol::{
        publisher,
        resolver::{ClientAuth, ClientHello, From, ServerAuth, ServerHello, To},
        Id,
    },
    resolver_store::Store,
};
use arc_swap::{ArcSwap, Guard};
use failure::Error;
use futures::{prelude::*, select};
use fxhash::FxBuildHasher;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::{
    collections::HashMap,
    io, mem,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
    convert::TryFrom,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task,
    time::{self, Instant},
};

type ClientInfo = Option<oneshot::Sender<()>>;

struct SecStoreInner {
    next: Id,
    read_ctxts: HashMap<Id, ServerCtx, FxBuildHasher>,
    write_ctxts: HashMap<SocketAddr, (Arc<UserInfo>, ServerCtx), FxBuildHasher>,
    userdb: UserDb<Mapper>,
}

impl SecStoreInner {
    fn get_read(&mut self, id: &Id) -> Option<ServerCtx> {
        self.read_ctxts.get(id).and_then(|ctx| match ctx.ttl() {
            Ok(ttl) if ttl.as_secs() > 0 => Some(ctx.clone()),
            _ => None,
        })
    }

    fn get_write(&mut self, id: &SocketAddr) -> Option<ServerCtx> {
        self.write_ctxts.get(id).and_then(|ctx| match ctx.ttl() {
            Ok(ttl) if ttl.as_secs() > 0 => Some(ctx.clone()),
            _ => None,
        })
    }

    fn delete(&mut self, id: &Id) {
        self.ctxts.remove(id);
    }

    fn id(&mut self) -> Id {
        self.next.take()
    }

    fn gc(&mut self) -> Result<(), Error> {
        let mut read_delete = SmallVec::<[Id; 64]>::new();
        let mut write_delete = SmallVec::<[SocketAddr; 64]>::new();
        for (id, ctx) in self.read_ctxts.iter() {
            if ctx.ttl()?.as_secs() == 0 {
                read_delete.push(*id);
            }
        }
        for (id, ctx) in self.write_ctxts.iter() {
            if ctx.ttl()?.as_secs() == 0 {
                write_delete.push(*id);
            }
        }
        for id in read_delete.into_iter() {
            self.read_ctxts.remove(&id);
        }
        for id in write_delete.into_iter() {
            self.write_ctxts.remove(&id);
        }
        Ok(())
    }

    fn ifo(&mut self, user: &str) -> Result<Arc<UserInfo>, Error> {
        self.userdb.ifo(user)
    }
}

#[derive(Clone)]
struct SecStore {
    principal: Arc<String>,
    pmap: ArcSwap<PMap>,
    store: Arc<Mutex<SecStoreInner>>,
}

impl SecStore {
    fn new(principal: String, pmap: PMap) -> Self {
        SecStore {
            principal: Arc::new(principal),
            pmap: ArcSwap::from(Arc::new(pmap)),
            store: Arc::new(Mutex::new(SecStoreInner {
                next: Id::zero(),
                read_ctxts: HashMap::with_hasher(FxBuildHasher::default()),
                write_ctxts: HashMap::with_hasher(FxBuildHasher::default()),
            })),
        }
    }

    fn pmap(&self) -> Guard<'static, Arc<PMap>> {
        self.pmap.load()
    }

    fn update_pmap(&self, pmap: PMap) {
        self.pmap.swap(Arc::new(pmap));
    }

    fn get_read(&self, id: &Id) -> Option<ServerCtx> {
        let mut inner = self.store.lock();
        inner.get_read(id)
    }

    fn get_write(&self, id: &SocketAddr) -> Option<ServerCtx> {
        let mut inner = self.store.lock();
        inner.get_write(id)
    }

    fn delete(&self, id: &Id) {
        let mut inner = self.store.lock();
        inner.delete(id);
    }

    fn create(&self, tok: &[u8]) -> Result<(ServerCtx, Vec<u8>), Error> {
        let ctx = sys_krb5.create_server_ctx(self.principal.as_bytes())?;
        let tok = ctx
            .step(Some(tok))?
            .map(|b| Vec::from(&*b))
            .ok_or_else(|| {
                format_error!("step didn't generate a mutual authentication token")
            })?;
        Ok((ctx, tok))
    }

    fn store_read(&self, ctx: ServerCtx) -> Id {
        let mut inner = self.store.lock();
        let id = inner.id();
        inner.read_ctxts.insert(id, ctx);
        id
    }

    fn store_write(&self, addr: SocketAddr, ctx: ServerCtx) {
        let mut inner = self.store.lock();
        inner.write_ctxts.insert(*addr, ctx);
    }

    fn gc(&self) -> Result<(), Error> {
        let mut inner = self.store.lock();
        Ok(inner.gc()?)
    }

    fn ifo(&self, user: &str) -> Result<Arc<UserInfo>, Error> {
        let mut inner = self.store.lock();
        Ok(inner.ifo(user)?)
    }
}

fn handle_batch(
    store: &Store<ClientInfo>,
    secstore: Option<&SecStore>,
    user: &UserInfo,
    msgs: impl Iterator<Item = resolver::To>,
    con: &mut Channel,
    wa: Option<SocketAddr>,
) -> Result<(), Error> {
    let pmap = secstore.map(|s| s.pmap());
    let allowed_for = |paths: &Vec<Path>, perm: Permissions| -> bool {
        pmap.map(|pm| pm.allowed_all(paths.iter().map(|p| &*p), perm, user))
            .unwrap_or(true)
    };
    match wa {
        None => {
            let s = store.read();
            for m in msgs {
                match m {
                    resolver::To::Heartbeat => (),
                    resolver::To::Resolve(paths) => {
                        if allowed_for(&paths, Permissions::SUBSCRIBE) {
                            let res = paths.iter().map(|p| s.resolve(p)).collect();
                            con.queue_send(&resolver::From::Resolved(res))?
                        } else {
                            con.queue_send(&resolver::From::Error("denied".into()))?
                        }
                    }
                    resolver::To::List(path) => {
                        let allowed = pmap
                            .map(|pm| pm.allowed(&*path, user, Permissions::LIST))
                            .unwrap_or(true);
                        if allowed {
                            con.queue_send(&resolver::From::List(s.list(&path)))?
                        } else {
                            con.queue_send(&resolver::From::Error("denied".into()))
                        }
                    }
                    resolver::To::Publish(_)
                    | resolver::To::Unpublish(_)
                    | resolver::To::Clear => {
                        con.queue_send(&resolver::From::Error("read only".into()))?
                    }
                }
            }
        }
        Some(write_addr) => {
            let mut s = store.write();
            for m in msgs {
                match m {
                    resolver::To::Heartbeat => (),
                    resolver::To::Resolve(_) | resolver::To::List(_) => {
                        con.queue_send(&resolver::From::Error("write only".into()))?
                    }
                    resolver::To::Publish(paths) => {
                        if !paths.iter().all(Path::is_absolute) {
                            con.queue_send(&resolver::From::Error(
                                "absolute paths required".into(),
                            ))?
                        } else {
                            if allowed_for(&paths, Permissions::PUBLISH) {
                                for path in paths {
                                    s.publish(path, write_addr);
                                }
                                con.queue_send(&resolver::From::Published)?
                            } else {
                                con.queue_send(&resolver::From::Error("denied".into()))
                            }
                        }
                    }
                    resolver::To::Unpublish(paths) => {
                        if allowed_for(&paths, Permissions::PUBLISH) {
                            for path in paths {
                                s.unpublish(path, write_addr);
                            }
                            con.queue_send(&resolver::From::Unpublished)?
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

static HELLO_TIMEOUT: Duration = Duration::from_secs(10);
static READER_TTL: Duration = Duration::from_secs(120);
static MIN_TTL: u64 = 60;
static MAX_TTL: u64 = 3600;

enum ClientState {
    AnonymousRead,
    AuthenticatedRead {
        ctx: ServerCtx,
        uifo: Arc<UserInfo>,
    },
    AnonymousWrite {
        write_addr: SocketAddr,
        ttl: Duration,
    },
    AuthenticatedWrite {
        ctx: ServerCtx,
        uifo: Arc<UserInfo>,
        write_addr: SocketAddr,
        ttl: Duration,
    },
}

impl ClientState {
    fn ttl(&self) -> Duration {
        match self {
            ClientState::AnonymousRead | ClientState::AuthenticatedRead { .. } => {
                READER_TTL
            }
            ClientState::AnonymousWrite { ttl, .. } => ttl,
            ClientState::AuthenticatedWrite { ttl, .. } => ttl,
        }
    }

    fn ifo(&self) -> Arc<UserInfo> {
        match self {
            ClientState::AnonymousRead | ClientState::AnonymousWrite { .. } => {
                ANONYMOUS.clone()
            }
            ClientState::AuthenticatedRead { uifo, .. } => uifo.clone(),
            ClientState::AuthenticatedWrite { uifo, .. } => uifo.clone(),
        }
    }
}

async fn hello_client(
    store: &Store<ClientInfo>,
    con: &mut Channel,
    secstore: Option<&SecStore>,
    tx_stop: oneshot::Sender<()>,
) -> Result<ClientState, Error> {
    async fn send(con: &mut Channel, hello: ServerHello) -> Result<(), Error> {
        Ok(time::timeout(HELLO_TIMEOUT, con.send_one(hello)).await??)
    }
    fn salt() -> u64 {
        let mut rng = rand::thread_rng();
        rng.gen_range(0, u64::max_value() - 2)
    }
    let hello: resolver::ClientHello =
        time::timeout(HELLO_TIMEOUT, con.receive()).await??;
    match hello {
        ClientHello::ReadOnly(ClientAuth::Anonymous) => ClientState::AnonymousRead,
        ClientHello::ReadOnly(ClientAuth::Reuse(None)) => {
            bail!("protocol error, expected auth token id")
        }
        ClientHello::ReadOnly(ClientAuth::Reuse(Some(id))) => match secstore {
            None => bail!("authentication not supported"),
            Some(secstore) => match secstore.get_read(&id) {
                None => bail!("id not recognized"),
                Some(ctx) => {
                    let uifo = secstore.ifo(&ctx.client()?)?;
                    send(con, ServerHello::ReadOnly(ServerAuth::Reused)).await?;
                    ClientState::AuthenticatedRead { ctx, uifo }
                }
            },
        },
        ClientHello::ReadOnly(ClientAuth::Token(tok)) => match secstore {
            None => bail!("authentication not supported"),
            Some(secstore) => {
                let (ctx, tok) = secstore.create(&tok)?;
                let uifo = secstore.ifo(&ctx.client()?)?;
                let id = secstore.store_read(ctx.clone());
                let h = ServerHello::ReadOnly(ServerAuth::Accepted(tok, Some(id)));
                match send(con, h).await {
                    Ok(()) => (),
                    Err(e) => {
                        secstore.delete(&id);
                        return Err(e);
                    }
                }
                ClientState::AuthenticatedRead { ctx, uifo }
            }
        },
        ClientHello::WriteOnly {
            ttl,
            write_addr,
            auth,
        } => {
            if ttl < MIN_TTL || ttl > MAX_TTL {
                bail!("invalid ttl")
            }
            let ttl = Duration::from_secs(ttl);
            // this is a race, but if it races it'll resolve by the next heartbeat
            let ttl_expired = !store.read().clinfo.contains(&write_addr);
            let client_state = match auth {
                ClientAuth::Anonymous => {
                    let h = ServerHello::WriteOnly {
                        ttl_expired,
                        auth: ServerAuth::Anonymous,
                    };
                    send(con, h).await?;
                    ClientState::AnonymousWrite { write_addr, ttl }
                }
                ClientAuth::Reuse(Some(_)) => {
                    bail!("protocol error: id's are not used for write only clients")
                }
                ClientAuth::Reuse(None) => match secstore {
                    None => bail!("authentication not supported"),
                    Some(secstore) => match secstore.get_write(&write_addr) {
                        None => bail!("session does not exist"),
                        Some(ctx) => {
                            let uifo = secstore.ifo(&ctx.client()?)?;
                            let h = ServerHello::WriteOnly {
                                ttl_expired,
                                auth: ServerAuth::Reused,
                            };
                            send(con, h).await?;
                            ClientState::AuthenticatedWrite {
                                ctx,
                                uifo,
                                write_addr,
                                ttl,
                            }
                        }
                    },
                },
                ClientAuth::Token(tok) => match secstore {
                    None => bail!("authentication not supported"),
                    Some(secstore) => {
                        let (ctx, tok) = secstore.create(&tok)?;
                        let uifo = secstore.ifo(&ctx.client()?)?;
                        let h = ServerHello::WriteOnly {
                            ttl_expired,
                            auth: ServerAuth::Accepted(tok, None),
                        };
                        send(con, h).await?;
                        let con = Channel::new(
                            time::timeout(HELLO_TIMEOUT, TcpStream::connect(addr))
                                .await??,
                        );
                        let salt = salt();
                        let tok = Vec::from(&*ctx.wrap(&salt.to_be_bytes())?);
                        time::timeout(
                            HELLO_TIMEOUT,
                            con.send_one(publisher::Hello::Authenticate(tok)),
                        )
                        .await??;
                        match time::timeout(HELLO_TIMEOUT, con.receive()).await?? {
                            publisher::Hello::Anonymous | publisher::Hello::Token(_) => {
                                bail!("denied")
                            }
                            publisher::Hello::Authenticate(tok) => {
                                let d = Vec::from(&*ctx.unwrap(&tok)?);
                                let dsalt = u64::from_be_bytes(TryFrom::try_from(&*d)?);
                                if dsalt != salt + 2 {
                                    bail!("denied")
                                }
                                secstore.store_write(write_addr, ctx);
                                ClientState::AuthenticatedWrite {
                                    ctx,
                                    uifo,
                                    write_addr,
                                    ttl,
                                }
                            }
                        }
                    }
                },
            };
            let mut store = store.write();
            let clinfos = store.clinfo_mut();
            match clinfos.get_mut(&write_addr) {
                None => {
                    clinfos.insert(write_addr, Some(tx_stop));
                }
                Some(cl) => {
                    if let Some(old_stop) = mem::replace(cl, Some(tx_stop)) {
                        let _ = old_stop.send(());
                    }
                }
            }
            client_state
        }
    }
}

async fn client_loop(
    store: Store<ClientInfo>,
    s: TcpStream,
    origin: SocketAddr,
    server_stop: oneshot::Receiver<()>,
    secstore: Option<SecStore>,
) -> Result<(), Error> {
    // CR estokes: require that a write client asserting write addr ip
    // is actually coming from that ip.
    s.set_nodelay(true)?;
    let mut con = Channel::new(s);
    let (tx_stop, rx_stop) = oneshot::channel();
    let state =
        hello_client(&store, &mut con, origin, secstore.as_ref(), tx_stop).await?;
    let mut con = Some(con);
    let mut server_stop = server_stop.fuse();
    let mut rx_stop = rx_stop.fuse();
    let mut batch = Vec::new();
    let mut act = false;
    let mut timeout = time::interval_at(Instant::now() + state.ttl, state.ttl).fuse();
    async fn receive_batch(
        con: &mut Option<Channel>,
        batch: &mut Vec<resolver::To>,
    ) -> Result<(), io::Error> {
        match con {
            Some(ref mut con) => con.receive_batch(batch).await,
            None => future::pending().await,
        }
    }
    loop {
        select! {
            _ = server_stop => break Ok(()),
            _ = rx_stop => break Ok(()),
            m = receive_batch(&mut con, &mut batch).fuse() => match m {
                Err(e) => {
                    batch.clear();
                    con = None;
                    // CR estokes: use proper log module
                    println!("error reading message: {}", e)
                },
                Ok(()) => {
                    act = true;
                    let c = con.as_mut().unwrap();
                    let r = handle_batch(
                        &store, secstore.as_ref(), &state.uifo,
                        batch.drain(..), c, state.write_addr
                    );
                    match r {
                        Err(_) => { con = None },
                        Ok(()) => match c.flush().await {
                            Err(_) => { con = None }, // CR estokes: Log this
                            Ok(()) => ()
                        }
                    }
                }
            },
            _ = timeout.next() => {
                if act {
                    act = false;
                } else {
                    if let Some(ref secstore) = secstore {
                        if let Some((id, _)) = state.ctx {
                            secstore.delete(&id);
                        }
                    }
                    if let Some(write_addr) = state.write_addr {
                        let mut store = store.write();
                        if let Some(ref mut cl) = store.clinfo_mut().remove(&write_addr) {
                            if let Some(stop) = mem::replace(cl, None) {
                                let _ = stop.send(());
                            }
                        }
                        store.unpublish_addr(write_addr);
                        store.gc();
                    }
                    bail!("client timed out");
                }
            },
        }
    }
}

async fn server_loop(
    cfg: config::Resolver,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<SocketAddr>,
) -> Result<SocketAddr, Error> {
    let connections = Arc::new(AtomicUsize::new(0));
    let published: Store<ClientInfo> = Store::new();
    let secstore = match cfg.auth {
        config::Auth::Anonymous => None,
        config::Auth::Krb5 { principal } => Some(SecStore::new(principal.clone())),
    };
    let mut listener = TcpListener::bind(cfg.addr).await?;
    let local_addr = listener.local_addr()?;
    let mut stop = stop.fuse();
    let mut client_stops = Vec::new();
    let _ = ready.send(local_addr);
    loop {
        select! {
            cl = listener.accept().fuse() => match cl {
                Err(_) => (),
                Ok((client, sa)) => {
                    if connections.fetch_add(1, Ordering::Relaxed) < cfg.max_connections {
                        let connections = connections.clone();
                        let published = published.clone();
                        let secstore = secstore.clone();
                        let (tx, rx) = oneshot::channel();
                        client_stops.push(tx);
                        task::spawn(async move {
                            let _ = client_loop(
                                published, client, sa, rx, secstore
                            ).await;
                            connections.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                }
            },
            _ = stop => {
                for cl in client_stops.drain(..) {
                    let _ = cl.send(());
                }
                return Ok(local_addr)
            },
        }
    }
}

#[derive(Debug)]
pub struct Server {
    stop: Option<oneshot::Sender<()>>,
    local_addr: SocketAddr,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = mem::replace(&mut self.stop, None) {
            let _ = stop.send(());
        }
    }
}

impl Server {
    pub async fn new(cfg: config::Resolver) -> Result<Server, Error> {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        let tsk = server_loop(cfg, recv_stop, send_ready);
        let local_addr = select! {
            a = task::spawn(tsk).fuse() => a??,
            a = recv_ready.fuse() => a?,
        };
        Ok(Server {
            stop: Some(send_stop),
            local_addr,
        })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }
}

#[cfg(test)]
mod test {
    use crate::{
        config,
        path::Path,
        resolver::{ReadOnly, Resolver, WriteOnly},
        resolver_server::Server,
    };
    use std::net::SocketAddr;

    async fn init_server() -> Server {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        Server::new(addr, 100, "".into())
            .await
            .expect("start server")
    }

    fn p(p: &str) -> Path {
        Path::from(p)
    }

    #[test]
    fn publish_resolve() {
        use tokio::runtime::Runtime;
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let server = init_server().await;
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let cfg = config::Resolver {
                addr: *server.local_addr(),
            };
            let mut w = Resolver::<WriteOnly>::new_w(cfg, paddr).unwrap();
            let mut r = Resolver::<ReadOnly>::new_r(cfg).unwrap();
            let paths = vec![p("/foo/bar"), p("/foo/baz"), p("/app/v0"), p("/app/v1")];
            w.publish(paths.clone()).await.unwrap();
            for addrs in r.resolve(paths.clone()).await.unwrap() {
                assert_eq!(addrs.len(), 1);
                assert_eq!(addrs[0], paddr);
            }
            assert_eq!(r.list(p("/")).await.unwrap(), vec![p("/app"), p("/foo")]);
            assert_eq!(
                r.list(p("/foo")).await.unwrap(),
                vec![p("/foo/bar"), p("/foo/baz")]
            );
            assert_eq!(
                r.list(p("/app")).await.unwrap(),
                vec![p("/app/v0"), p("/app/v1")]
            );
        });
    }
}
