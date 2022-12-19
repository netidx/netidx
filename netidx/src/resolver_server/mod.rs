pub(crate) mod auth;
pub mod config;
pub(crate) mod secctx;
mod shard_store;
mod store;
#[cfg(test)]
mod test;

use crate::{
    channel::{self, Channel, K5CtxWrap},
    chars::Chars,
    pack::Pack,
    pool::{Pool, Pooled},
    protocol::{
        publisher,
        resolver::{
            AuthChallenge, AuthRead, AuthWrite, ClientHello, ClientHelloWrite, FromWrite,
            HashMethod, Publisher, PublisherId, ReadyForOwnershipCheck, Secret,
            ServerHelloWrite, ToRead, ToWrite,
        },
    },
    utils,
};
use anyhow::Result;
use auth::{UserInfo, ANONYMOUS};
use config::{Config, MemberServer};
use cross_krb5::{AcceptFlags, K5ServerCtx, ServerCtx, Step};
use futures::{channel::oneshot, prelude::*, select_biased};
use fxhash::FxHashMap;
use log::{debug, info, warn};
use netidx_core::{pack::BoundedBytes, utils::make_sha3_token};
use parking_lot::{Mutex, RwLock};
use rand::{thread_rng, Rng};
use secctx::{K5SecData, LocalSecData, SecCtx, TlsSecData};
use shard_store::Store;
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
                                let anon = publisher.target_auth.is_anonymous();
                                match &hello.auth {
                                    AuthWrite::Anonymous if anon => (),
                                    AuthWrite::Anonymous => bail!("not permitted"),
                                    AuthWrite::Reuse => (),
                                    AuthWrite::Krb5 { .. }
                                    | AuthWrite::Local
                                    | AuthWrite::Tls { .. } => {
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
    cfg: MemberServer,
    id: SocketAddr,
    listen_addr: SocketAddr,
    store: Store,
    delay_reads: Option<Instant>,
}

async fn client_loop_write(
    ctx: Arc<Ctx>,
    connection_id: CId,
    con: Channel,
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
        con: &mut Option<Channel>,
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

async fn recv<T: Pack + Debug>(timeout: Duration, con: &mut TcpStream) -> Result<T> {
    Ok(time::timeout(timeout, channel::read_raw(con)).await??)
}
async fn send(timeout: Duration, con: &mut TcpStream, msg: &impl Pack) -> Result<()> {
    Ok(time::timeout(timeout, channel::write_raw(con, msg)).await??)
}

pub(crate) async fn krb5_authentication(
    timeout: Duration,
    spn: Option<&str>,
    con: &mut TcpStream,
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

async fn challenge_auth(
    cfg: &MemberServer,
    con: &mut Channel,
    secret: u128,
) -> Result<()> {
    let n = thread_rng().gen::<u128>();
    let answer = make_sha3_token(&[&n.to_be_bytes(), &secret.to_be_bytes()]);
    let challenge = AuthChallenge { hash_method: HashMethod::Sha3_512, challenge: n };
    time::timeout(cfg.hello_timeout, con.send_one(&challenge)).await??;
    let token: BoundedBytes<TOKEN_MAX> =
        time::timeout(cfg.hello_timeout, con.receive()).await??;
    if &*token != &*answer {
        bail!("denied")
    }
    Ok(())
}

async fn ownership_check(
    ctx: &Ctx,
    con: &mut Channel,
    write_addr: SocketAddr,
) -> Result<u128> {
    let secret = thread_rng().gen::<u128>();
    let timeout = ctx.cfg.hello_timeout;
    time::timeout(timeout, con.send_one(&Secret(secret))).await??;
    let _: ReadyForOwnershipCheck = time::timeout(timeout, con.receive()).await??;
    info!("hello_write connecting to {:?} for listener ownership check", write_addr);
    let con = time::timeout(timeout, TcpStream::connect(write_addr)).await??;
    let mut con = Channel::new::<ServerCtx, TcpStream>(None, con);
    time::timeout(timeout, con.send_one(&2u64)).await??;
    if time::timeout(timeout, con.receive::<u64>()).await?? != 2 {
        bail!("incompatible protocol version")
    }
    use publisher::Hello as PHello;
    let n = thread_rng().gen::<u128>();
    let answer = utils::make_sha3_token(&[&n.to_be_bytes(), &secret.to_be_bytes()]);
    time::timeout(timeout, con.send_one(&PHello::ResolverAuthenticate(ctx.id))).await??;
    let m = AuthChallenge { hash_method: HashMethod::Sha3_512, challenge: n };
    time::timeout(timeout, con.send_one(&m)).await??;
    let token: BoundedBytes<TOKEN_MAX> = time::timeout(timeout, con.receive()).await??;
    if &*token != &*answer {
        bail!("listener ownership check failed");
    }
    Ok(secret)
}

type AuthResult = Result<(Channel, Arc<UserInfo>, Arc<Publisher>, oneshot::Receiver<()>)>;

async fn write_client_anonymous_auth(
    ctx: &Arc<Ctx>,
    mut con: TcpStream,
    hello: &ClientHelloWrite,
) -> AuthResult {
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
    Ok((
        Channel::new::<ServerCtx, TcpStream>(None, con),
        ANONYMOUS.clone(),
        publisher,
        rx_stop,
    ))
}

async fn write_client_local_auth(
    ctx: &Arc<Ctx>,
    mut con: TcpStream,
    a: &Arc<(secctx::LocalAuth, RwLock<secctx::SecCtxData<secctx::LocalSecData>>)>,
    hello: &ClientHelloWrite,
) -> AuthResult {
    let tok: BoundedBytes<TOKEN_MAX> = recv(ctx.cfg.hello_timeout, &mut con).await?;
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
    let mut con = Channel::new::<ServerCtx, TcpStream>(None, con);
    let secret = ownership_check(&ctx, &mut con, hello.write_addr).await?;
    let (publisher, _, rx_stop) = ctx.clinfos.insert(&ctx, &uifo, &hello).await?;
    let d = LocalSecData { user: cred.user, secret };
    a.1.write().insert(publisher.id, d);
    Ok((con, uifo, publisher, rx_stop))
}

async fn write_client_reuse_local(
    ctx: &Arc<Ctx>,
    con: TcpStream,
    a: &Arc<(secctx::LocalAuth, RwLock<secctx::SecCtxData<secctx::LocalSecData>>)>,
    hello: &ClientHelloWrite,
) -> AuthResult {
    let wa = &hello.write_addr;
    let id = ctx.clinfos.id(wa).ok_or_else(|| anyhow!("missing"))?;
    let d = a.1.read().get(&id).ok_or_else(|| anyhow!("missing"))?.clone();
    let uifo = a.1.write().users.ifo(Some(&*d.user))?;
    let mut con = Channel::new::<ServerCtx, TcpStream>(None, con);
    challenge_auth(&ctx.cfg, &mut con, d.secret).await?;
    let (publisher, ttl_expired, rx_stop) =
        ctx.clinfos.insert(&ctx, &uifo, &hello).await?;
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired,
        resolver_id: ctx.id,
        auth: AuthWrite::Reuse,
    };
    match time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await {
        Ok(Ok(())) => (),
        Err(e) => {
            ctx.clinfos.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
        Ok(Err(e)) => {
            ctx.clinfos.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
    }
    Ok((con, uifo, publisher, rx_stop))
}

async fn write_client_krb5_auth(
    ctx: &Arc<Ctx>,
    mut con: TcpStream,
    a: &Arc<(Chars, RwLock<secctx::SecCtxData<secctx::K5SecData>>)>,
    hello: &ClientHelloWrite,
) -> AuthResult {
    info!("hello_write initiating new krb5 context for {:?}", hello.write_addr);
    let k5ctx = krb5_authentication(ctx.cfg.hello_timeout, Some(&*a.0), &mut con).await?;
    let k5ctx = K5CtxWrap::new(k5ctx);
    let mut con = Channel::new(Some(k5ctx.clone()), con);
    info!("hello_write all traffic now encrypted");
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired: true, // re auth always clears
        resolver_id: ctx.id,
        auth: AuthWrite::Krb5 { spn: Chars::from("") },
    };
    debug!("hello_write sending {:?}", h);
    time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await??;
    let secret = ownership_check(&ctx, &mut con, hello.write_addr).await?;
    let client = task::block_in_place(|| k5ctx.lock().client())?;
    let uifo = a.1.write().users.ifo(Some(&client))?;
    info!("hello_write listener ownership check succeeded");
    let (publisher, _, rx_stop) = ctx.clinfos.insert(&ctx, &uifo, &hello).await?;
    let d = K5SecData { ctx: k5ctx, secret };
    a.1.write().insert(publisher.id, d);
    Ok((con, uifo, publisher, rx_stop))
}

async fn write_client_reuse_krb5(
    ctx: &Arc<Ctx>,
    con: TcpStream,
    a: &Arc<(Chars, RwLock<secctx::SecCtxData<secctx::K5SecData>>)>,
    hello: &ClientHelloWrite,
) -> AuthResult {
    let wa = &hello.write_addr;
    let id = ctx.clinfos.id(wa).ok_or_else(|| anyhow!("missing"))?;
    let d = a.1.read().get(&id).ok_or_else(|| anyhow!("missing"))?.clone();
    let uifo =
        a.1.write().users.ifo(Some(&task::block_in_place(|| d.ctx.lock().client())?))?;
    let mut con = Channel::new(Some(d.ctx), con);
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
    match time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await {
        Ok(Ok(())) => (),
        Err(e) => {
            ctx.clinfos.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
        Ok(Err(e)) => {
            ctx.clinfos.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
    }
    Ok((con, uifo, publisher, rx_stop))
}

fn get_tls_uifo(
    tls: &tokio_rustls::server::TlsStream<TcpStream>,
    a: &Arc<(tokio_rustls::TlsAcceptor, RwLock<secctx::SecCtxData<secctx::TlsSecData>>)>,
) -> Result<Arc<UserInfo>> {
    let (_, server_con) = tls.get_ref();
    match server_con.peer_certificates() {
        Some([cert, ..]) => {
            let anchor = webpki::TrustAnchor::try_from_cert_der(&cert.0)?;
            let user = std::str::from_utf8(anchor.subject)?;
            Ok(a.1.write().users.ifo(Some(user))?)
        }
        Some(_) | None => bail!("tls handshake should be complete by now"),
    }
}

async fn write_client_tls_auth(
    ctx: &Arc<Ctx>,
    con: TcpStream,
    a: &Arc<(tokio_rustls::TlsAcceptor, RwLock<secctx::SecCtxData<secctx::TlsSecData>>)>,
    hello: &ClientHelloWrite,
) -> AuthResult {
    let tls = a.0.accept(con).await?;
    let uifo = get_tls_uifo(&tls, a)?;
    let mut con =
        Channel::new::<ServerCtx, tokio_rustls::server::TlsStream<TcpStream>>(None, tls);
    info!("hello_write all traffic now encrypted");
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired: true,
        resolver_id: ctx.id,
        auth: AuthWrite::Tls { name: Chars::from("") },
    };
    debug!("hello_write sending {:?}", h);
    time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await??;
    let secret = ownership_check(&ctx, &mut con, hello.write_addr).await?;
    info!("hello_write listener ownership check succeeded");
    let (publisher, _, rx_stop) = ctx.clinfos.insert(&ctx, &uifo, hello).await?;
    let d = TlsSecData(secret);
    a.1.write().insert(publisher.id, d);
    Ok((con, uifo, publisher, rx_stop))
}

async fn write_client_reuse_tls(
    ctx: &Arc<Ctx>,
    con: TcpStream,
    a: &Arc<(tokio_rustls::TlsAcceptor, RwLock<secctx::SecCtxData<secctx::TlsSecData>>)>,
    hello: &ClientHelloWrite,
) -> AuthResult {
    let tls = a.0.accept(con).await?;
    let wa = &hello.write_addr;
    let id = ctx.clinfos.id(wa).ok_or_else(|| anyhow!("missing"))?;
    let d = a.1.read().get(&id).ok_or_else(|| anyhow!("missing"))?.clone();
    let uifo = get_tls_uifo(&tls, a)?;
    let mut con =
        Channel::new::<ServerCtx, tokio_rustls::server::TlsStream<TcpStream>>(None, tls);
    info!("hello_write all traffic now encrypted");
    challenge_auth(&ctx.cfg, &mut con, d.0).await?;
    let (publisher, ttl_expired, rx_stop) =
        ctx.clinfos.insert(&ctx, &uifo, &hello).await?;
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired,
        resolver_id: ctx.id,
        auth: AuthWrite::Reuse,
    };
    info!("hello_write reusing tls context");
    debug!("hello_write sending {:?}", h);
    match time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await {
        Ok(Ok(())) => (),
        Err(e) => {
            ctx.clinfos.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
        Ok(Err(e)) => {
            ctx.clinfos.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
    }
    Ok((con, uifo, publisher, rx_stop))
}

async fn hello_client_write(
    ctx: Arc<Ctx>,
    connection_id: CId,
    con: TcpStream,
    server_stop: oneshot::Receiver<()>,
    hello: ClientHelloWrite,
) -> Result<()> {
    static NO: &str = "authentication mechanism not supported";
    info!("hello_write starting negotiation");
    debug!("hello_write client_hello: {:?}", hello);
    utils::check_addr(hello.write_addr.ip(), &[(ctx.listen_addr, ())])?;
    let (con, uifo, publisher, rx_stop) = match hello.auth {
        AuthWrite::Anonymous => write_client_anonymous_auth(&ctx, con, &hello).await?,
        AuthWrite::Local => match &ctx.secctx {
            SecCtx::Local(a) => write_client_local_auth(&ctx, con, a, &hello).await?,
            SecCtx::Anonymous | SecCtx::Krb5(_) | SecCtx::Tls(_) => bail!(NO),
        },
        AuthWrite::Krb5 { .. } => match &ctx.secctx {
            SecCtx::Krb5(a) => write_client_krb5_auth(&ctx, con, a, &hello).await?,
            SecCtx::Anonymous | SecCtx::Local(_) | SecCtx::Tls(_) => bail!(NO),
        },
        AuthWrite::Tls { .. } => match &ctx.secctx {
            SecCtx::Tls(a) => write_client_tls_auth(&ctx, con, a, &hello).await?,
            SecCtx::Anonymous | SecCtx::Local(_) | SecCtx::Krb5(_) => bail!(NO),
        },
        AuthWrite::Reuse => match &ctx.secctx {
            SecCtx::Local(a) => write_client_reuse_local(&ctx, con, a, &hello).await?,
            SecCtx::Krb5(a) => write_client_reuse_krb5(&ctx, con, a, &hello).await?,
            SecCtx::Tls(a) => write_client_reuse_tls(&ctx, con, a, &hello).await?,
            SecCtx::Anonymous => bail!(NO),
        },
    };
    Ok(client_loop_write(ctx, connection_id, con, server_stop, rx_stop, uifo, publisher)
        .await?)
}

async fn client_loop_read(
    ctx: Arc<Ctx>,
    mut con: Channel,
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
    mut con: TcpStream,
    server_stop: oneshot::Receiver<()>,
    hello: AuthRead,
) -> Result<()> {
    static NO: &str = "authentication mechanism not supported";
    let (con, uifo) = match hello {
        AuthRead::Anonymous => {
            send(ctx.cfg.hello_timeout, &mut con, &AuthRead::Anonymous).await?;
            (Channel::new::<ServerCtx, TcpStream>(None, con), ANONYMOUS.clone())
        }
        AuthRead::Local => match &ctx.secctx {
            SecCtx::Local(a) => {
                let tok: BoundedBytes<TOKEN_MAX> =
                    recv(ctx.cfg.hello_timeout, &mut con).await?;
                let cred = a.0.authenticate(&*tok)?;
                let uifo = a.1.write().users.ifo(Some(&cred.user))?;
                send(ctx.cfg.hello_timeout, &mut con, &AuthRead::Local).await?;
                (Channel::new::<ServerCtx, TcpStream>(None, con), uifo)
            }
            SecCtx::Anonymous | SecCtx::Krb5(_) | SecCtx::Tls(_) => bail!(NO),
        },
        AuthRead::Krb5 => match &ctx.secctx {
            SecCtx::Krb5(a) => {
                let k5ctx =
                    krb5_authentication(ctx.cfg.hello_timeout, Some(&*a.0), &mut con)
                        .await?;
                send(ctx.cfg.hello_timeout, &mut con, &AuthRead::Krb5).await?;
                let k5ctx = K5CtxWrap::new(k5ctx);
                let con = Channel::new::<ServerCtx, TcpStream>(Some(k5ctx.clone()), con);
                let uifo =
                    a.1.write()
                        .users
                        .ifo(Some(&task::block_in_place(|| k5ctx.lock().client())?))?;
                (con, uifo)
            }
            SecCtx::Anonymous | SecCtx::Local(_) | SecCtx::Tls(_) => bail!(NO),
        },
        AuthRead::Tls => match &ctx.secctx {
            SecCtx::Tls(a) => {
                let tls = a.0.accept(con).await?;
                let uifo = get_tls_uifo(&tls, a)?;
                let mut con = Channel::new::<
                    ServerCtx,
                    tokio_rustls::server::TlsStream<TcpStream>,
                >(None, tls);
                time::timeout(ctx.cfg.hello_timeout, con.send_one(&AuthRead::Tls))
                    .await??;
                (con, uifo)
            }
            SecCtx::Anonymous | SecCtx::Local(_) | SecCtx::Krb5(_) => bail!(NO),
        },
    };
    Ok(client_loop_read(ctx, con, server_stop, uifo).await?)
}

async fn hello_client(
    ctx: Arc<Ctx>,
    connection_id: CId,
    mut s: TcpStream,
    server_stop: oneshot::Receiver<()>,
) -> Result<()> {
    s.set_nodelay(true)?;
    send(ctx.cfg.hello_timeout, &mut s, &2u64).await?;
    let version: u64 = recv(ctx.cfg.hello_timeout, &mut s).await?;
    if version != 2 {
        bail!("unsupported protocol version")
    }
    let hello: ClientHello = recv(ctx.cfg.hello_timeout, &mut s).await?;
    match hello {
        ClientHello::ReadOnly(hello) => {
            if let Some(t) = ctx.delay_reads {
                if Instant::now() < t {
                    bail!("no read clients allowed yet");
                }
            }
            Ok(hello_client_read(ctx, s, server_stop, hello).await?)
        }
        ClientHello::WriteOnly(hello) => {
            Ok(hello_client_write(ctx, connection_id, s, server_stop, hello).await?)
        }
    }
}

async fn server_loop(
    cfg: Config,
    delay_reads: bool,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<SocketAddr>,
    id: usize,
) -> Result<SocketAddr> {
    let member = cfg.member_servers[id].clone();
    let delay_reads =
        if delay_reads { Some(Instant::now() + member.writer_ttl) } else { None };
    let id = member.addr;
    let secctx = SecCtx::new(&cfg, &member).await?;
    let store = Store::new(
        cfg.parent.clone().map(|s| s.into()),
        cfg.children.iter().map(|(p, s)| (p.clone(), s.clone().into())).collect(),
        secctx.clone(),
        id,
    );
    let listener = TcpListener::bind(id).await?;
    let listen_addr = listener.local_addr()?;
    let ctx = Arc::new(Ctx {
        cfg: member,
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
    pub async fn new(cfg: Config, delay_reads: bool, id: usize) -> Result<Server> {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        let tsk = server_loop(cfg, delay_reads, recv_stop, send_ready, id);
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
