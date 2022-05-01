pub(crate) mod auth;
pub mod config;
mod secctx;
mod shard_store;
mod store;
#[cfg(test)]
mod test;

use auth::{UserInfo, ANONYMOUS};
use config::Config;
use secctx::{K5SecData, LocalSecData, SecCtx};
use shard_store::Store;

use crate::{
    channel::{Channel, K5CtxWrap},
    chars::Chars,
    pack::Pack,
    pool::{Pool, Pooled},
    protocol::{
        publisher,
        resolver::{
            AuthChallenge, AuthRead, AuthWrite, ClientHello, ClientHelloWrite, FromWrite,
            HashMethod, Publisher, PublisherId, ReadyForOwnershipCheck, Secret,
            ServerHelloWrite, TargetAuth, ToRead, ToWrite,
        },
    },
    utils,
};
use anyhow::Result;
use cross_krb5::{AcceptFlags, K5ServerCtx, ServerCtx, Step};
use futures::{channel::oneshot, prelude::*, select_biased};
use fxhash::FxHashMap;
use log::{debug, info, warn};
use netidx_core::{pack::BoundedBytes, utils::make_sha3_token};
use parking_lot::Mutex;
use rand::{thread_rng, Rng};
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::Debug,
    mem,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    task,
    time::{self, Instant},
};

lazy_static! {
    static ref WRITE_BATCHES: Pool<Vec<ToWrite>> = Pool::new(5000, 100000);
    static ref READ_BATCHES: Pool<Vec<ToRead>> = Pool::new(5000, 100000);
}

atomic_id!(CId);

struct CTracker(Mutex<HashSet<CId>>);

impl CTracker {
    fn new() -> Self {
        CTracker(Mutex::new(HashSet::new()))
    }

    fn open(&self) -> CId {
        let id = CId::new();
        self.0.lock().insert(id);
        id
    }

    fn close(&self, id: CId) {
        self.0.lock().remove(&id);
    }

    fn num_open(&self) -> usize {
        self.0.lock().len()
    }
}

enum ClientInfo {
    CleaningUp(Vec<oneshot::Sender<()>>),
    Running { publisher: Arc<Publisher>, stop: oneshot::Sender<()> },
}

struct Clinfos(Mutex<FxHashMap<SocketAddr, ClientInfo>>);

impl Clinfos {
    fn new() -> Self {
        Clinfos(Mutex::new(HashMap::default()))
    }

    async fn wait_running<F, R>(&self, addr: &SocketAddr, f: F) -> R
    where
        R: 'static,
        F: FnOnce(Entry<SocketAddr, ClientInfo>) -> R,
    {
        loop {
            let rx = {
                let mut inner = self.0.lock();
                match inner.get_mut(&addr) {
                    None => break f(inner.entry(*addr)),
                    Some(ClientInfo::Running { .. }) => break f(inner.entry(*addr)),
                    Some(ClientInfo::CleaningUp(w)) => {
                        let (tx, rx) = oneshot::channel();
                        w.push(tx);
                        rx
                    }
                }
            };
            let _ = rx.await;
        }
    }

    async fn remove(
        &self,
        ctx: &Ctx,
        publisher: &Arc<Publisher>,
        uifo: &Arc<UserInfo>,
    ) -> Result<()> {
        let cleanup = self
            .wait_running(&publisher.addr, |e| match e {
                Entry::Vacant(_) => false,
                Entry::Occupied(mut e) => {
                    *e.get_mut() = ClientInfo::CleaningUp(Vec::new());
                    ctx.secctx.remove(&publisher.id);
                    true
                }
            })
            .await;
        if cleanup {
            ctx.store.handle_clear(uifo.clone(), publisher.clone()).await?;
            self.0.lock().remove(&publisher.addr);
        }
        Ok(())
    }

    async fn insert(
        &self,
        ctx: &Ctx,
        uifo: &Arc<UserInfo>,
        hello: &ClientHelloWrite,
    ) -> Result<(Arc<Publisher>, bool, oneshot::Receiver<()>)> {
        enum R {
            ClearClient(Arc<Publisher>),
            Finished(Arc<Publisher>, bool, oneshot::Receiver<()>),
        }
        loop {
            let r = self
                .wait_running(&hello.write_addr, |e| match e {
                    Entry::Vacant(e) => {
                        let publisher = Arc::new(Publisher {
                            addr: hello.write_addr,
                            resolver: ctx.id,
                            id: PublisherId::new(),
                            hash_method: HashMethod::Sha3_512,
                            target_auth: hello.auth.clone().try_into()?,
                        });
                        let (tx, rx) = oneshot::channel();
                        e.insert(ClientInfo::Running {
                            publisher: publisher.clone(),
                            stop: tx,
                        });
                        Ok(R::Finished(publisher, true, rx))
                    }
                    Entry::Occupied(mut e) => {
                        let ifo = e.get_mut();
                        match ifo {
                            ClientInfo::Running { publisher, stop } => {
                                match (&hello.auth, &publisher.target_auth) {
                                    (AuthWrite::Anonymous, TargetAuth::Anonymous) => (),
                                    (AuthWrite::Anonymous, _) => bail!("not permitted"),
                                    (AuthWrite::Reuse, _) => (),
                                    (
                                        AuthWrite::Krb5 { spn: cspn },
                                        TargetAuth::Krb5 { spn },
                                    ) if cspn == spn => (),
                                    (AuthWrite::Local, TargetAuth::Local) => (),
                                    (AuthWrite::Krb5 { .. } | AuthWrite::Local, _) => {
                                        let publisher = publisher.clone();
                                        *ifo = ClientInfo::CleaningUp(Vec::new());
                                        ctx.secctx.remove(&publisher.id);
                                        return Ok(R::ClearClient(publisher));
                                    }
                                }
                                let (tx, rx) = oneshot::channel();
                                *stop = tx;
                                Ok(R::Finished(publisher.clone(), false, rx))
                            }
                            _ => unreachable!(),
                        }
                    }
                })
                .await?;
            match r {
                R::Finished(publisher, t, rx) => break Ok((publisher, t, rx)),
                R::ClearClient(publisher) => {
                    ctx.store.handle_clear(uifo.clone(), publisher).await?;
                    self.0.lock().remove(&hello.write_addr);
                }
            }
        }
    }

    fn id(&self, addr: &SocketAddr) -> Option<PublisherId> {
        self.0.lock().get(addr).and_then(|ifo| match ifo {
            ClientInfo::CleaningUp(_) => None,
            ClientInfo::Running { publisher, .. } => Some(publisher.id),
        })
    }
}

struct Ctx {
    clinfos: Clinfos,
    ctracker: CTracker,
    secctx: SecCtx,
    cfg: Config,
    id: SocketAddr,
    listen_addr: SocketAddr,
    store: Store,
    delay_reads: Option<Instant>,
}

async fn client_loop_write(
    ctx: Arc<Ctx>,
    connection_id: CId,
    con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    rx_stop: oneshot::Receiver<()>,
    uifo: Arc<UserInfo>,
    publisher: Arc<Publisher>,
) -> Result<()> {
    let mut con = Some(con);
    let mut server_stop = server_stop.fuse();
    let mut rx_stop = rx_stop.fuse();
    let mut batch = WRITE_BATCHES.take();
    let mut act = false;
    let mut timeout =
        time::interval_at(Instant::now() + ctx.cfg.writer_ttl, ctx.cfg.writer_ttl);
    async fn receive_batch(
        con: &mut Option<Channel<ServerCtx>>,
        batch: &mut Vec<ToWrite>,
    ) -> Result<()> {
        match con {
            Some(ref mut con) => con.receive_batch(batch).await,
            None => future::pending().await,
        }
    }
    'main: loop {
        select_biased! {
            _ = server_stop => break Ok(()),
            _ = rx_stop => break Ok(()),
            _ = timeout.tick().fuse() => {
                if act {
                    act = false;
                } else {
                    drop(con);
                    ctx.ctracker.close(connection_id);
                    ctx.clinfos.remove(&ctx, &publisher, &uifo).await?;
                    bail!("write client timed out");
                }
            },
            m = receive_batch(&mut con, &mut *batch).fuse() => match m {
                Err(e) => {
                    batch.clear();
                    con = None;
                    ctx.ctracker.close(connection_id);
                    info!("write client loop error reading message: {}", e)
                },
                Ok(()) => {
                    act = true;
                    if batch.len() == 1 && batch[0] == ToWrite::Heartbeat {
                        continue 'main
                    }
                    let c = con.as_mut().unwrap();
                    while let Some((i, _)) =
                        batch.iter().enumerate().find(|(_, m)| *m == &ToWrite::Clear)
                    {
                        debug!("batch {} contains clear at pos {}", batch.len(), i);
                        let rest = batch.split_off(i + 1);
                        for m in batch.drain(..) {
                            match m {
                                ToWrite::Heartbeat => (),
                                ToWrite::Publish(_)
                                    | ToWrite::PublishDefault(_)
                                    | ToWrite::PublishWithFlags(_, _)
                                    | ToWrite::PublishDefaultWithFlags(_, _) =>
                                    c.queue_send(&FromWrite::Published)?,
                                ToWrite::Unpublish(_) =>
                                    c.queue_send(&FromWrite::Unpublished)?,
                                ToWrite::UnpublishDefault(_) =>
                                    c.queue_send(&FromWrite::Unpublished)?,
                                ToWrite::Clear => {
                                    ctx.store.handle_clear(
                                        uifo.clone(),
                                        publisher.clone()
                                    ).await?;
                                    c.queue_send(&FromWrite::Unpublished)?
                                }
                            }
                        }
                        c.flush().await?;
                        batch = Pooled::orphan(rest);
                    }
                    if let Err(e) = ctx.store.handle_batch_write(
                        Some(c),
                        uifo.clone(),
                        publisher.clone(),
                        batch.drain(..)
                    ).await {
                        warn!("handle_write_batch failed {}", e);
                        con = None;
                        ctx.ctracker.close(connection_id);
                        continue 'main;
                    }
                }
            },
        }
    }
}

const TOKEN_MAX: usize = 4096;

async fn recv<T: Pack + Debug>(
    timeout: Duration,
    con: &mut Channel<ServerCtx>,
) -> Result<T> {
    Ok(time::timeout(timeout, con.receive()).await??)
}
async fn send(
    timeout: Duration,
    con: &mut Channel<ServerCtx>,
    msg: &impl Pack,
) -> Result<()> {
    Ok(time::timeout(timeout, con.send_one(msg)).await??)
}

pub(crate) async fn krb5_authentication(
    timeout: Duration,
    spn: Option<&str>,
    con: &mut Channel<ServerCtx>,
) -> Result<ServerCtx> {
    // the GSS token shouldn't ever be bigger than 1 MB
    const L: usize = 1 * 1024 * 1024;
    let mut ctx = task::block_in_place(|| ServerCtx::new(AcceptFlags::empty(), spn))?;
    loop {
        let token: BoundedBytes<L> = recv(timeout, con).await?;
        match task::block_in_place(|| ctx.step(&*token))? {
            Step::Continue((nctx, token)) => {
                ctx = nctx;
                let token = BoundedBytes::<L>(utils::bytes(&*token));
                send(timeout, con, &token).await?;
            }
            Step::Finished((ctx, token)) => {
                if let Some(token) = token {
                    let token = BoundedBytes::<L>(utils::bytes(&*token));
                    send(timeout, con, &token).await?;
                }
                break Ok(ctx);
            }
        }
    }
}

async fn hello_client_write(
    ctx: Arc<Ctx>,
    connection_id: CId,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    hello: ClientHelloWrite,
) -> Result<()> {
    info!("hello_write starting negotiation");
    debug!("hello_write client_hello: {:?}", hello);
    async fn challenge_auth(
        cfg: &Config,
        con: &mut Channel<ServerCtx>,
        secret: u128,
    ) -> Result<()> {
        let n = thread_rng().gen::<u128>();
        let answer = make_sha3_token(&[&n.to_be_bytes(), &secret.to_be_bytes()]);
        let challenge = AuthChallenge { hash_method: HashMethod::Sha3_512, challenge: n };
        send(cfg.hello_timeout, con, &challenge).await?;
        let token: BoundedBytes<TOKEN_MAX> = recv(cfg.hello_timeout, con).await?;
        if &*token != &*answer {
            bail!("denied")
        }
        Ok(())
    }
    async fn ownership_check(
        ctx: &Ctx,
        con: &mut Channel<ServerCtx>,
        write_addr: SocketAddr,
        secret: u128,
    ) -> Result<()> {
        let timeout = ctx.cfg.hello_timeout;
        send(timeout, con, &Secret(secret)).await?;
        let _: ReadyForOwnershipCheck = time::timeout(timeout, con.receive()).await??;
        info!("hello_write connecting to {:?} for listener ownership check", write_addr);
        let mut con: Channel<ServerCtx> =
            Channel::new(time::timeout(timeout, TcpStream::connect(write_addr)).await??);
        use publisher::Hello as PHello;
        let n = thread_rng().gen::<u128>();
        let answer = utils::make_sha3_token(&[&n.to_be_bytes(), &secret.to_be_bytes()]);
        send(timeout, &mut con, &PHello::ResolverAuthenticate(ctx.id)).await?;
        let m = AuthChallenge { hash_method: HashMethod::Sha3_512, challenge: n };
        send(timeout, &mut con, &m).await?;
        let token: BoundedBytes<TOKEN_MAX> = recv(timeout, &mut con).await?;
        if &*token != &*answer {
            bail!("listener ownership check failed");
        }
        Ok(())
    }
    utils::check_addr(hello.write_addr.ip(), &[(ctx.listen_addr, ())])?;
    let (uifo, publisher, rx_stop) = match hello.auth {
        AuthWrite::Anonymous => {
            let uifo = &*ANONYMOUS;
            let (publisher, ttl_expired, rx_stop) =
                ctx.clinfos.insert(&ctx, uifo, &hello).await?;
            let h = ServerHelloWrite {
                ttl: ctx.cfg.writer_ttl.as_secs(),
                ttl_expired,
                resolver_id: ctx.id,
                auth: AuthWrite::Anonymous,
            };
            info!("hello_write accepting Anonymous authentication");
            debug!("hello_write sending hello {:?}", h);
            if let Err(e) = send(ctx.cfg.hello_timeout, &mut con, &h).await {
                ctx.clinfos.remove(&ctx, &publisher, uifo).await?;
                Err(e)?;
            }
            (ANONYMOUS.clone(), publisher, rx_stop)
        }
        AuthWrite::Local => match ctx.secctx {
            SecCtx::Anonymous | SecCtx::Krb5(_) => bail!("authentication not supported"),
            SecCtx::Local(ref a) => {
                let tok: BoundedBytes<TOKEN_MAX> =
                    recv(ctx.cfg.hello_timeout, &mut con).await?;
                let cred = a.0.authenticate(&*tok)?;
                let uifo = a.1.write().users.ifo(Some(&cred.user))?;
                info!("hello_write local auth succeeded");
                let h = ServerHelloWrite {
                    ttl: ctx.cfg.writer_ttl.as_secs(),
                    ttl_expired: true, // re auth always clears
                    resolver_id: ctx.id,
                    auth: AuthWrite::Local,
                };
                debug!("hello_write sending {:?}", h);
                send(ctx.cfg.hello_timeout, &mut con, &h).await?;
                let secret = thread_rng().gen::<u128>();
                ownership_check(&ctx, &mut con, hello.write_addr, secret).await?;
                let (publisher, _, rx_stop) =
                    ctx.clinfos.insert(&ctx, &uifo, &hello).await?;
                let d = LocalSecData { user: cred.user, secret };
                a.1.write().insert(publisher.id, d);
                (uifo, publisher, rx_stop)
            }
        },
        AuthWrite::Krb5 { .. } => match ctx.secctx {
            SecCtx::Anonymous | SecCtx::Local(_) => bail!("authentication not supported"),
            SecCtx::Krb5(ref a) => {
                info!(
                    "hello_write initiating new krb5 context for {:?}",
                    hello.write_addr
                );
                let secret = thread_rng().gen::<u128>();
                let k5ctx =
                    krb5_authentication(ctx.cfg.hello_timeout, Some(&*a.0), &mut con)
                        .await?;
                let k5ctx = K5CtxWrap::new(k5ctx);
                con.set_ctx(k5ctx.clone()).await;
                info!("hello_write all traffic now encrypted");
                let h = ServerHelloWrite {
                    ttl: ctx.cfg.writer_ttl.as_secs(),
                    ttl_expired: true, // re auth always clears
                    resolver_id: ctx.id,
                    auth: AuthWrite::Krb5 { spn: Chars::from("") },
                };
                debug!("hello_write sending {:?}", h);
                send(ctx.cfg.hello_timeout, &mut con, &h).await?;
                ownership_check(&ctx, &mut con, hello.write_addr, secret).await?;
                let client = task::block_in_place(|| k5ctx.lock().client())?;
                let uifo = a.1.write().users.ifo(Some(&client))?;
                info!("hello_write listener ownership check succeeded");
                let (publisher, _, rx_stop) =
                    ctx.clinfos.insert(&ctx, &uifo, &hello).await?;
                let d = K5SecData { ctx: k5ctx, secret };
                a.1.write().insert(publisher.id, d);
                (uifo, publisher, rx_stop)
            }
        },
        AuthWrite::Reuse => match ctx.secctx {
            SecCtx::Anonymous => bail!("authentication not supported"),
            SecCtx::Local(ref a) => {
                let wa = &hello.write_addr;
                let id = ctx.clinfos.id(wa).ok_or_else(|| anyhow!("missing"))?;
                let d = a.1.read().get(&id).ok_or_else(|| anyhow!("missing"))?.clone();
                let uifo = a.1.write().users.ifo(Some(&*d.user))?;
                challenge_auth(&ctx.cfg, &mut con, d.secret).await?;
                let (publisher, ttl_expired, rx_stop) =
                    ctx.clinfos.insert(&ctx, &uifo, &hello).await?;
                let h = ServerHelloWrite {
                    ttl: ctx.cfg.writer_ttl.as_secs(),
                    ttl_expired,
                    resolver_id: ctx.id,
                    auth: AuthWrite::Reuse,
                };
                if let Err(e) = send(ctx.cfg.hello_timeout, &mut con, &h).await {
                    ctx.clinfos.remove(&ctx, &publisher, &uifo).await?;
                    Err(e)?
                }
                (uifo, publisher, rx_stop)
            }
            SecCtx::Krb5(ref a) => {
                let wa = &hello.write_addr;
                let id = ctx.clinfos.id(wa).ok_or_else(|| anyhow!("missing"))?;
                let d = a.1.read().get(&id).ok_or_else(|| anyhow!("missing"))?.clone();
                let uifo =
                    a.1.write()
                        .users
                        .ifo(Some(&task::block_in_place(|| d.ctx.lock().client())?))?;
                con.set_ctx(d.ctx).await;
                info!("hello_write all traffic now encrypted");
                challenge_auth(&ctx.cfg, &mut con, d.secret).await?;
                let (publisher, ttl_expired, rx_stop) =
                    ctx.clinfos.insert(&ctx, &uifo, &hello).await?;
                let h = ServerHelloWrite {
                    ttl: ctx.cfg.writer_ttl.as_secs(),
                    ttl_expired,
                    resolver_id: ctx.id,
                    auth: AuthWrite::Reuse,
                };
                info!("hello_write reusing krb5 context");
                debug!("hello_write sending {:?}", h);
                if let Err(e) = send(ctx.cfg.hello_timeout, &mut con, &h).await {
                    ctx.clinfos.remove(&ctx, &publisher, &uifo).await?;
                    Err(e)?
                }
                (uifo, publisher, rx_stop)
            }
        },
    };
    Ok(client_loop_write(ctx, connection_id, con, server_stop, rx_stop, uifo, publisher)
        .await?)
}

async fn client_loop_read(
    ctx: Arc<Ctx>,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    uifo: Arc<UserInfo>,
) -> Result<()> {
    let mut batch = READ_BATCHES.take();
    let mut server_stop = server_stop.fuse();
    let mut act = false;
    let mut timeout =
        time::interval_at(Instant::now() + ctx.cfg.reader_ttl, ctx.cfg.reader_ttl);
    loop {
        select_biased! {
            _ = server_stop => break Ok(()),
            _ = timeout.tick().fuse() => {
                if act {
                    act = false;
                } else {
                    bail!("client timed out");
                }
            }
            m = con.receive_batch(&mut batch).fuse() => {
                m?;
                act = true;
                ctx.store.handle_batch_read(
                    &mut con,
                    uifo.clone(),
                    batch.drain(..)
                ).await?;
            },
        }
    }
}

async fn hello_client_read(
    ctx: Arc<Ctx>,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    hello: AuthRead,
) -> Result<()> {
    let uifo = match hello {
        AuthRead::Anonymous => {
            send(ctx.cfg.hello_timeout, &mut con, &AuthRead::Anonymous).await?;
            ANONYMOUS.clone()
        }
        AuthRead::Local => match ctx.secctx {
            SecCtx::Anonymous | SecCtx::Krb5(_) => bail!("auth mech not supported"),
            SecCtx::Local(ref a) => {
                let tok: BoundedBytes<TOKEN_MAX> =
                    recv(ctx.cfg.hello_timeout, &mut con).await?;
                let cred = a.0.authenticate(&*tok)?;
                let uifo = a.1.write().users.ifo(Some(&cred.user))?;
                send(ctx.cfg.hello_timeout, &mut con, &AuthRead::Local).await?;
                uifo
            }
        },
        AuthRead::Krb5 => match ctx.secctx {
            SecCtx::Anonymous | SecCtx::Local(_) => {
                bail!("authentication mechanism krb5 not supported")
            }
            SecCtx::Krb5(ref a) => {
                let k5ctx =
                    krb5_authentication(ctx.cfg.hello_timeout, Some(&*a.0), &mut con)
                        .await?;
                let k5ctx = K5CtxWrap::new(k5ctx);
                send(ctx.cfg.hello_timeout, &mut con, &AuthRead::Krb5).await?;
                con.set_ctx(k5ctx.clone()).await;
                a.1.write()
                    .users
                    .ifo(Some(&task::block_in_place(|| k5ctx.lock().client())?))?
            }
        },
    };
    Ok(client_loop_read(ctx, con, server_stop, uifo).await?)
}

async fn hello_client(
    ctx: Arc<Ctx>,
    connection_id: CId,
    s: TcpStream,
    server_stop: oneshot::Receiver<()>,
) -> Result<()> {
    s.set_nodelay(true)?;
    let mut con = Channel::new(s);
    time::timeout(ctx.cfg.hello_timeout, con.send_one(&1u64)).await??;
    // we will use this to select a protocol version when there is more than one
    let _version: u64 = time::timeout(ctx.cfg.hello_timeout, con.receive()).await??;
    let hello: ClientHello =
        time::timeout(ctx.cfg.hello_timeout, con.receive()).await??;
    match hello {
        ClientHello::ReadOnly(hello) => {
            if let Some(t) = ctx.delay_reads {
                if Instant::now() < t {
                    bail!("no read clients allowed yet");
                }
            }
            Ok(hello_client_read(ctx, con, server_stop, hello).await?)
        }
        ClientHello::WriteOnly(hello) => {
            Ok(hello_client_write(ctx, connection_id, con, server_stop, hello).await?)
        }
    }
}

async fn server_loop(
    cfg: Config,
    delay_reads: bool,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<SocketAddr>,
) -> Result<SocketAddr> {
    let delay_reads =
        if delay_reads { Some(Instant::now() + cfg.writer_ttl) } else { None };
    let id = cfg.addr;
    let secctx = SecCtx::new(&cfg).await?;
    let store = Store::new(
        cfg.parent.clone().map(|s| s.into()),
        cfg.children.iter().map(|(p, s)| (p.clone(), s.clone().into())).collect(),
        secctx.clone(),
        id,
    );
    let listener = TcpListener::bind(id).await?;
    let listen_addr = listener.local_addr()?;
    let ctx = Arc::new(Ctx {
        cfg,
        secctx,
        clinfos: Clinfos::new(),
        ctracker: CTracker::new(),
        id,
        delay_reads,
        listen_addr,
        store,
    });
    let mut stop = stop.fuse();
    let mut client_stops: Vec<oneshot::Sender<()>> = Vec::new();
    let max_connections = ctx.cfg.max_connections;
    let _ = ready.send(ctx.listen_addr);
    loop {
        select_biased! {
            _ = stop => {
                for cl in client_stops.drain(..) {
                    let _ = cl.send(());
                }
                return Ok(ctx.listen_addr)
            },
            cl = listener.accept().fuse() => match cl {
                Err(e) => warn!("accept failed: {}", e),
                Ok((client, _)) => {
                    let (tx, rx) = oneshot::channel();
                    client_stops.push(tx);
                    let connection_id = ctx.ctracker.open();
                    task::spawn({
                        let ctx = Arc::clone(&ctx);
                        async move {
                            let r = hello_client(
                                Arc::clone(&ctx),
                                connection_id,
                                client,
                                rx
                            ).await;
                            ctx.ctracker.close(connection_id);
                            info!("server_loop client shutting down {:?}", r);
                        }
                    });
                    while ctx.ctracker.num_open() > max_connections {
                        time::sleep(Duration::from_millis(10u64)).await;
                    }
                    debug!("I have {} writers", ctx.clinfos.0.lock().len())
                }
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
    pub async fn new(cfg: Config, delay_reads: bool) -> Result<Server> {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        let tsk = server_loop(cfg, delay_reads, recv_stop, send_ready);
        let local_addr = select_biased! {
            a = task::spawn(tsk).fuse() => a??,
            a = recv_ready.fuse() => a?,
        };
        Ok(Server { stop: Some(send_stop), local_addr })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }
}
