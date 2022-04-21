use crate::{
    auth::{UserInfo, ANONYMOUS},
    channel::Channel,
    chars::Chars,
    config,
    pack::Pack,
    pool::{Pool, Pooled},
    protocol::{
        publisher,
        resolver::{
            AuthRead, AuthWrite, ClientHello, ClientHelloWrite, CtxId, FromWrite,
            ReadyForOwnershipCheck, Secret, ServerAuthWrite, ServerHelloRead,
            ServerHelloWrite, ToRead, ToWrite,
        },
    },
    secctx::SecCtx,
    shard_resolver_store::Store,
    utils,
};
use anyhow::Result;
use bytes::{Buf, Bytes};
use cross_krb5::{AcceptFlags, K5ServerCtx, PendingServerCtx, ServerCtx, Step};
use futures::{channel::oneshot, prelude::*, select_biased};
use log::{debug, info, warn};
use netidx_core::{pack::BoundedBytes, utils::make_sha3_token};
use parking_lot::Mutex;
use rand::{thread_rng, Rng};
use std::{
    collections::{HashMap, HashSet},
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

atomic_id!(CId);

#[derive(Debug, Clone)]
struct CTracker(Arc<Mutex<HashSet<CId>>>);

impl CTracker {
    fn new() -> Self {
        CTracker(Arc::new(Mutex::new(HashSet::new())))
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
    Running(oneshot::Sender<()>),
    CleaningUp(Vec<oneshot::Sender<()>>),
}

#[derive(Clone)]
struct Clinfos(Arc<Mutex<HashMap<SocketAddr, ClientInfo>>>);

lazy_static! {
    static ref WRITE_BATCHES: Pool<Vec<ToWrite>> = Pool::new(5000, 100000);
    static ref READ_BATCHES: Pool<Vec<ToRead>> = Pool::new(5000, 100000);
}

pub(crate) async fn krb5_authentication(
    timeout: Duration,
    spn: &str,
    con: &mut Channel<ServerCtx>,
) -> Result<ServerCtx> {
    // the GSS token shouldn't ever be bigger than 1 MB
    const L: usize = 1 * 1024 * 1024;
    use pack::BoundedBytes;
    let mut ctx =
        task::block_in_place(|| ServerCtx::new(AcceptFlags::empty(), Some(spn)))?;
    loop {
        let token: BoundedBytes<L> = time::timeout(timeout, con.receive()).await??;
        match task::block_in_place(|| ctx.step(&*token))? {
            Step::Continue((nctx, token)) => {
                ctx = nctx;
                let token = BytesBounded::<L>(Bytes::copy_from_slice(&*token));
                time::timeout(timeout, con.send_one(&token)).await??;
            }
            Step::Finished((ctx, token)) => {
                if let Some(token) = token {
                    let token = BytesBounded::<L>(Bytes::copy_from_slice(&*token));
                    time::timeout(timeout, con.send_one(&token)).await??;
                }
                break Ok(ctx);
            }
        }
    }
}

async fn client_loop_write(
    cfg: Arc<config::server::Config>,
    clinfos: Clinfos,
    ctracker: CTracker,
    connection_id: CId,
    mut store: Store,
    con: Channel<ServerCtx>,
    secctx: SecCtx,
    server_stop: oneshot::Receiver<()>,
    rx_stop: oneshot::Receiver<()>,
    uifo: Arc<UserInfo>,
    write_addr: SocketAddr,
) -> Result<()> {
    let mut con = Some(con);
    let mut server_stop = server_stop.fuse();
    let mut rx_stop = rx_stop.fuse();
    let mut batch = WRITE_BATCHES.take();
    let mut act = false;
    let mut timeout = time::interval_at(Instant::now() + cfg.writer_ttl, cfg.writer_ttl);
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
                    ctracker.close(connection_id);
                    {
                        let mut inner = clinfos.0.lock();
                        match inner.remove(&write_addr) {
                            None => (),
                            Some(ClientInfo::CleaningUp(_)) => unreachable!(),
                            Some(ClientInfo::Running(stop)) => {
                                let _ = stop.send(());
                            }
                        }
                        let state = ClientInfo::CleaningUp(Vec::new());
                        inner.insert(write_addr, state);
                        match &secctx {
                            SecCtx::Krb5(a) => a.1.write().remove(&write_addr),
                            SecCtx::Local(a) => a.1.write().remove(&write_addr),
                            SecCtx::Anonymous => ()
                        }
                    }
                    store.handle_clear(uifo.clone(), write_addr).await?;
                    let mut inner = clinfos.0.lock();
                    inner.remove(&write_addr);
                    bail!("write client timed out");
                }
            },
            m = receive_batch(&mut con, &mut *batch).fuse() => match m {
                Err(e) => {
                    batch.clear();
                    con = None;
                    ctracker.close(connection_id);
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
                                    store.handle_clear(
                                        uifo.clone(),
                                        write_addr
                                    ).await?;
                                    c.queue_send(&FromWrite::Unpublished)?
                                }
                            }
                        }
                        c.flush().await?;
                        batch = Pooled::orphan(rest);
                    }
                    if let Err(e) = store.handle_batch_write(
                        Some(c),
                        uifo.clone(),
                        write_addr,
                        batch.drain(..)
                    ).await {
                        warn!("handle_write_batch failed {}", e);
                        con = None;
                        ctracker.close(connection_id);
                        continue 'main;
                    }
                }
            },
        }
    }
}

async fn hello_client_write(
    cfg: Arc<config::server::Config>,
    clinfos: Clinfos,
    ctracker: CTracker,
    connection_id: CId,
    listen_addr: SocketAddr,
    store: Store,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    secctx: SecCtx,
    resolver_id: SocketAddr,
    hello: ClientHelloWrite,
) -> Result<()> {
    info!("hello_write starting negotiation");
    debug!("hello_write client_hello: {:?}", hello);
    async fn recv<T: Pack + Debug>(
        cfg: &Arc<config::server::Config>,
        con: &mut Channel<ServerCtx>,
    ) -> Result<T> {
        Ok(time::timeout(cfg.hello_timeout, con.receive()).await??)
    }
    async fn send(
        cfg: &Arc<config::server::Config>,
        con: &mut Channel<ServerCtx>,
        msg: impl Pack,
    ) -> Result<()> {
        Ok(time::timeout(cfg.hello_timeout, con.send_one(&msg)).await??)
    }
    async fn ownership_check(
        cfg: &Arc<config::server::Config>,
        con: &mut Channel<ServerCtx>,
        write_addr: SocketAddr,
        resolver_id: SocketAddr,
        secret: u128,
    ) -> Result<()> {
        send(cfg, con, Secret(secret)).await?;
        let _: ReadyForOwnershipCheck =
            time::timeout(cfg.hello_timeout, con.receive()).await??;
        info!("hello_write connecting to {:?} for listener ownership check", write_addr);
        let mut con: Channel<ServerCtx> = Channel::new(
            time::timeout(cfg.hello_timeout, TcpStream::connect(write_addr)).await??,
        );
        use publisher::Hello as PHello;
        let n = thread_rng().gen::<u128>();
        send(cfg, &mut con, PHello::ResolverAuthenticate(resolver_id)).await?;
        send(cfg, &mut con, n).await?;
        let token: BoundedBytes<4096> = recv(&cfg, &mut con).await?;
        let answer = utils::make_sha3_token(&[&n.to_be_bytes(), &secret.to_be_bytes()]);
        if &*token != &*answer {
            bail!("listener ownership check failed");
        }
        Ok(())
    }
    utils::check_addr(hello.write_addr.ip(), &[listen_addr])?;
    let ttl_expired = loop {
        let rx = {
            let mut inner = clinfos.0.lock();
            match inner.get_mut(&hello.write_addr) {
                None => break true,
                Some(ClientInfo::Running(_)) => break false,
                Some(ClientInfo::CleaningUp(waiters)) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    rx
                }
            }
        };
        let _ = rx.await;
    };
    let uifo = match hello.auth {
        AuthWrite::Anonymous => {
            let h = ServerHelloWrite {
                ttl: cfg.writer_ttl.as_secs(),
                ttl_expired,
                resolver_id,
                auth: AuthWrite::Anonymous,
            };
            info!("hello_write accepting Anonymous authentication");
            debug!("hello_write sending hello {:?}", h);
            send(&cfg, &mut con, h).await?;
            ANONYMOUS.clone()
        }
        AuthWrite::Local => match secctx {
            SecCtx::Anonymous | SecCtx::Krb5(_) => bail!("authentication not supported"),
            SecCtx::Local(ref a) => {
                let tok: BoundedBytes<4096> = recv(&cfg, &mut con).await?;
                let cred = a.0.authenticate(&*tok)?;
                let uifo = a.1.write().users.ifo(Some(&cred.user))?;
                info!("hello_write local auth succeeded");
                let h = ServerHelloWrite {
                    ttl: cfg.writer_ttl.as_secs(),
                    ttl_expired,
                    resolver_id,
                    auth: AuthWrite::Local,
                };
                debug!("hello_write sending {:?}", h);
                send(&cfg, &mut con, h).await?;
                let secret = thread_rng().gen::<u128>();
                ownership_check(&cfg, &mut con, hello.write_addr, resolver_id, secret)
                    .await?;
                a.1.write().insert_local(hello.write_addr, cred.user, secret);
                uifo
            }
        },
        AuthWrite::Reuse => match secctx {
            SecCtx::Anonymous => bail!("authentication not supported"),
            SecCtx::Local(ref a) => {
                let (user, secret) =
                    a.1.read()
                        .get_local(&hello.write_addr)
                        .ok_or_else(|| anyhow!("session not found"))?;
                let n = thread_rng().gen::<u128>();
                send(&cfg, &mut con, n).await?;
                let token: BoundedBytes<4096> = recv(&cfg, &mut con).await?;
                let answer = make_sha3_token(&[&n.to_be_bytes(), &secret.to_be_bytes()]);
                if &*token != &*answer {
                    bail!("reuse denied")
                }
                let h = ServerHelloWrite {
                    ttl: cfg.writer_ttl.as_secs(),
                    ttl_expired,
                    resolver_id,
                    auth: AuthWrite::Reuse,
                };
                send(&cfg, &mut con, h).await?;
                a.1.write().users.ifo(Some(&*user))?
            }
            SecCtx::Krb5(ref a) => {
                let ctx =
                    a.1.read()
                        .get_k5_ctx(&hello.write_addr)
                        .ok_or_else(|| anyhow!("session not found"))?;
                let h = ServerHelloWrite {
                    ttl: cfg.writer_ttl.as_secs(),
                    ttl_expired,
                    resolver_id,
                    auth: AuthWrite::Reuse,
                };
                info!("hello_write reusing krb5 context");
                debug!("hello_write sending {:?}", h);
                send(&cfg, &mut con, h).await?;
                con.set_ctx(ctx.clone()).await;
                info!("hello_write all traffic now encrypted");
                a.1.write()
                    .users
                    .ifo(Some(&task::block_in_place(|| ctx.lock().client())?))?
            }
        },
        AuthWrite::Krb5 { spn } => match secctx {
            SecCtx::Anonymous | SecCtx::Local(_) => bail!("authentication not supported"),
            SecCtx::Krb5(ref a) => {
                info!(
                    "hello_write initiating new krb5 context for {:?}",
                    hello.write_addr
                );
                let secret = thread_rng().gen::<u128>();
                let ctx = krb5_authentication(cfg.hello_timeout, &*a.0, &mut con)?;
                let h = ServerHelloWrite {
                    ttl: cfg.writer_ttl.as_secs(),
                    ttl_expired,
                    resolver_id,
                    auth: AuthWrite::Krb5 { spn: Chars::from("") },
                };
                info!("hello_write created context for {:?}", hello.write_addr);
                debug!("hello_write sending {:?}", h);
                send(&cfg, &mut con, h).await?;
                con.set_ctx(ctx.clone()).await;
                info!("hello_write all traffic now encrypted");
                ownership_check(&cfg, &mut con, hello.write_addr, resolver_id, secret)
                    .await?;
                let client = task::block_in_place(|| ctx.lock().client())?;
                let uifo = a.1.write().users.ifo(Some(&client))?;
                let spn = spn.unwrap_or(Chars::from(client));
                info!("hello_write listener ownership check succeeded");
                a.1.write().insert_k5(hello.write_addr, spn, secret, ctx.clone());
                uifo
            }
        },
    };
    let (tx_stop, rx_stop) = oneshot::channel();
    {
        let mut inner = clinfos.0.lock();
        match inner.get_mut(&hello.write_addr) {
            None => {
                inner.insert(hello.write_addr, ClientInfo::Running(tx_stop));
            }
            Some(ClientInfo::Running(cl)) => {
                let cl = mem::replace(cl, tx_stop);
                let _ = cl.send(());
            }
            Some(ClientInfo::CleaningUp(_)) => bail!("unexpected cleaning up"),
        }
    }
    Ok(client_loop_write(
        cfg,
        clinfos,
        ctracker,
        connection_id,
        store.clone(),
        con,
        secctx,
        server_stop,
        rx_stop,
        uifo,
        hello.write_addr,
    )
    .await?)
}

async fn client_loop_read(
    cfg: Arc<config::server::Config>,
    mut store: Store,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    uifo: Arc<UserInfo>,
) -> Result<()> {
    let mut batch = READ_BATCHES.take();
    let mut server_stop = server_stop.fuse();
    let mut act = false;
    let mut timeout = time::interval_at(Instant::now() + cfg.reader_ttl, cfg.reader_ttl);
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
                store.handle_batch_read(
                    &mut con,
                    uifo.clone(),
                    batch.drain(..)
                ).await?;
            },
        }
    }
}

async fn hello_client_read(
    cfg: Arc<config::server::Config>,
    store: Store,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    secctx: SecCtx,
    hello: ClientAuthRead,
) -> Result<()> {
    async fn send(
        cfg: &Arc<config::server::Config>,
        con: &mut Channel<ServerCtx>,
        hello: ServerHelloRead,
    ) -> Result<()> {
        Ok(time::timeout(cfg.hello_timeout, con.send_one(&hello)).await??)
    }
    let uifo = match hello {
        ClientAuthRead::Anonymous => {
            send(&cfg, &mut con, ServerHelloRead::Anonymous).await?;
            ANONYMOUS.clone()
        }
        ClientAuthRead::Local(tok) => match secctx {
            SecCtx::Anonymous | SecCtx::Krb5(_) => bail!("auth mech not supported"),
            SecCtx::Local(ref a) => {
                let cred = a.0.authenticate(&*tok)?;
                let uifo = a.1.write().users.ifo(Some(&cred.user))?;
                send(&cfg, &mut con, ServerHelloRead::Local).await?;
                uifo
            }
        },
        ClientAuthRead::Reuse(_) => bail!("read session reuse deprecated"),
        ClientAuthRead::Initiate(tok) => match secctx {
            SecCtx::Anonymous | SecCtx::Local(_) => {
                bail!("authentication requested but not supported")
            }
            SecCtx::Krb5(ref a) => {
                let (ctx, tok) = a.0.authenticate(&tok)?;
                send(&cfg, &mut con, ServerHelloRead::Accepted(tok, CtxId::new()))
                    .await?;
                con.set_ctx(ctx.clone()).await;
                a.1.write()
                    .users
                    .ifo(Some(&task::block_in_place(|| ctx.lock().client())?))?
            }
        },
    };
    Ok(client_loop_read(cfg, store.clone(), con, server_stop, uifo).await?)
}

async fn hello_client(
    cfg: Arc<config::server::Config>,
    clinfos: Clinfos,
    ctracker: CTracker,
    connection_id: CId,
    delay_reads: Option<Instant>,
    listen_addr: SocketAddr,
    store: Store,
    s: TcpStream,
    server_stop: oneshot::Receiver<()>,
    secctx: SecCtx,
    id: SocketAddr,
) -> Result<()> {
    s.set_nodelay(true)?;
    let mut con = Channel::new(s);
    time::timeout(cfg.hello_timeout, con.send_one(&1u64)).await??;
    // we will use this to select a protocol version when there is more than one
    let _version: u64 = time::timeout(cfg.hello_timeout, con.receive()).await??;
    let hello: ClientHello = time::timeout(cfg.hello_timeout, con.receive()).await??;
    match hello {
        ClientHello::ReadOnly(hello) => {
            if let Some(t) = delay_reads {
                if Instant::now() < t {
                    bail!("no read clients allowed yet");
                }
            }
            Ok(hello_client_read(cfg, store.clone(), con, server_stop, secctx, hello)
                .await?)
        }
        ClientHello::WriteOnly(hello) => Ok(hello_client_write(
            cfg,
            clinfos,
            ctracker,
            connection_id,
            listen_addr,
            store.clone(),
            con,
            server_stop,
            secctx,
            id,
            hello,
        )
        .await?),
    }
}

async fn server_loop(
    cfg: config::server::Config,
    delay_reads: bool,
    id: usize,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<SocketAddr>,
) -> Result<SocketAddr> {
    let delay_reads =
        if delay_reads { Some(Instant::now() + cfg.writer_ttl) } else { None };
    let cfg = Arc::new(cfg);
    let ctracker = CTracker::new();
    let clinfos = Clinfos(Arc::new(Mutex::new(HashMap::new())));
    let id = cfg.addrs[id];
    let secctx = SecCtx::new(&cfg, &id).await?;
    let published = Store::new(
        cfg.parent.clone().map(|s| s.into()),
        cfg.children.iter().map(|(p, s)| (p.clone(), s.clone().into())).collect(),
        secctx.clone(),
        id,
    );
    let listener = TcpListener::bind(id).await?;
    let local_addr = listener.local_addr()?;
    let mut stop = stop.fuse();
    let mut client_stops: Vec<oneshot::Sender<()>> = Vec::new();
    let max_connections = cfg.max_connections;
    let _ = ready.send(local_addr);
    loop {
        select_biased! {
            _ = stop => {
                for cl in client_stops.drain(..) {
                    let _ = cl.send(());
                }
                return Ok(local_addr)
            },
            cl = listener.accept().fuse() => match cl {
                Err(e) => warn!("accept failed: {}", e),
                Ok((client, _)) => {
                    let (tx, rx) = oneshot::channel();
                    client_stops.push(tx);
                    let connection_id = ctracker.open();
                    task::spawn({
                        let clinfos = clinfos.clone();
                        let ctracker = ctracker.clone();
                        let published = published.clone();
                        let secctx = secctx.clone();
                        let cfg = cfg.clone();
                        async move {
                            let r = hello_client(
                                cfg,
                                clinfos,
                                ctracker.clone(),
                                connection_id,
                                delay_reads,
                                local_addr,
                                published,
                                client,
                                rx,
                                secctx,
                                id
                            ).await;
                            ctracker.close(connection_id);
                            info!("server_loop client shutting down {:?}", r);
                        }
                    });
                    while ctracker.num_open() > max_connections {
                        time::sleep(Duration::from_millis(10u64)).await;
                    }
                    debug!("I have {} writers", clinfos.0.lock().len())
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
    pub async fn new(
        cfg: config::server::Config,
        delay_reads: bool,
        id: usize,
    ) -> Result<Server> {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        let tsk = server_loop(cfg, delay_reads, id, recv_stop, send_ready);
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
