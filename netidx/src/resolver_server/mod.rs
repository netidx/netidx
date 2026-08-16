//! Resolver server for mapping paths to publishers.
use crate::path::Path;
use crate::{
    channel::{self, Channel, K5CtxWrap},
    pack::Pack,
    protocol::{
        publisher,
        resolver::{
            AuthChallenge, AuthRead, AuthWrite, ClientHello, ClientHelloWrite, FromWrite,
            HashMethod, Publisher, PublisherId, ReadyForOwnershipCheck, Secret,
            ServerHelloWrite, ToRead, ToWrite, WriteRefusal,
        },
    },
    tls, utils,
};
use ahash::AHashMap;
use anyhow::{Context, Result, anyhow};
use arcstr::{ArcStr, literal};
use auth::{ANONYMOUS, UserInfo};
use config::{Config, MemberServer, ReadGate};
use cross_krb5::{AcceptFlags, K5ServerCtx, ServerCtx, Step};
use futures::{channel::oneshot, prelude::*, select_biased};
use log::{debug, error, info, trace, warn};
use netidx_core::{pack::BoundedBytes, utils::make_sha3_token};
use nohash::IntSet;
use parking_lot::Mutex as SyncMutex;
use poolshark::global::{GPooled, Pool};
use rand::{RngExt, rng};
use secctx::{K5SecData, LocalSecData, SecCtx, TlsSecData};
use shard_store::Store;
use std::{
    collections::{BTreeSet, hash_map::Entry},
    fmt::Debug,
    mem,
    net::SocketAddr,
    ops::Deref,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, RwLock},
    task,
    time::{self, Instant},
};

pub mod auth;
pub mod config;
#[cfg(unix)]
mod id_map_watch;
pub(crate) mod secctx;
mod shard_store;
mod store;
#[cfg(test)]
mod test;

static WRITE_BATCHES: LazyLock<Pool<Vec<ToWrite>>> =
    LazyLock::new(|| Pool::new(100, 10_000));
static READ_BATCHES: LazyLock<Pool<Vec<ToRead>>> =
    LazyLock::new(|| Pool::new(100, 10_000));

atomic_id!(CId);

struct CTracker(SyncMutex<IntSet<CId>>);

impl CTracker {
    fn new() -> Self {
        CTracker(SyncMutex::new(IntSet::default()))
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

struct ClinfosInner(AHashMap<SocketAddr, ClientInfo>);

impl ClinfosInner {
    async fn wait_running<'b, 'a, F, A, R>(&'b mut self, addr: &SocketAddr, f: F) -> R
    where
        'b: 'a,
        R: 'static,
        A: Future<Output = R> + 'a,
        F: FnOnce(Entry<'a, SocketAddr, ClientInfo>) -> A,
    {
        loop {
            let rx = {
                match self.0.get_mut(&addr) {
                    None => break f(self.0.entry(*addr)).await,
                    Some(ClientInfo::Running { .. }) => {
                        let entry = self.0.entry(*addr);
                        let r = f(entry).await;
                        break r;
                    }
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
        &mut self,
        ctx: &Ctx,
        publisher: &Arc<Publisher>,
        uifo: &Arc<UserInfo>,
    ) -> Result<()> {
        let cleanup = self
            .wait_running(&publisher.addr, |e| async {
                match e {
                    Entry::Vacant(_) => false,
                    Entry::Occupied(mut e) => {
                        *e.get_mut() = ClientInfo::CleaningUp(Vec::new());
                        ctx.secctx.remove(&publisher.id).await;
                        true
                    }
                }
            })
            .await;
        if cleanup {
            ctx.store.handle_clear(uifo.clone(), publisher.clone()).await?;
            self.0.remove(&publisher.addr);
        }
        Ok(())
    }

    async fn insert(
        &mut self,
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
                .wait_running(&hello.write_addr, |e| async {
                    match e {
                        Entry::Vacant(e) => {
                            let publisher = Arc::new(Publisher {
                                addr: hello.write_addr,
                                resolver: ctx.id,
                                id: PublisherId::new(),
                                hash_method: HashMethod::Sha3_512,
                                target_auth: hello.auth.clone().try_into()?,
                                user_info: None,
                                priority: hello.priority,
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
                            macro_rules! kill_it {
                                ($publisher:expr) => {{
                                    let publisher = $publisher.clone();
                                    *ifo = ClientInfo::CleaningUp(Vec::new());
                                    ctx.secctx.remove(&publisher.id).await;
                                    return Ok(R::ClearClient(publisher));
                                }};
                            }
                            match ifo {
                                ClientInfo::Running { publisher, stop } => {
                                    let anon = publisher.target_auth.is_anonymous();
                                    match &hello.auth {
                                        AuthWrite::Anonymous if anon => {
                                            if publisher.priority != hello.priority {
                                                kill_it!(publisher)
                                            }
                                        }
                                        AuthWrite::Anonymous => bail!("not permitted"),
                                        AuthWrite::Reuse => {
                                            if publisher.priority != hello.priority {
                                                bail!("illegal priority change")
                                            }
                                        }
                                        AuthWrite::Krb5 { .. }
                                        | AuthWrite::Local
                                        | AuthWrite::Tls { .. } => kill_it!(publisher),
                                    }
                                    let (tx, rx) = oneshot::channel();
                                    *stop = tx;
                                    Ok(R::Finished(publisher.clone(), false, rx))
                                }
                                _ => unreachable!(),
                            }
                        }
                    }
                })
                .await?;
            match r {
                R::Finished(publisher, t, rx) => break Ok((publisher, t, rx)),
                R::ClearClient(publisher) => {
                    ctx.store.handle_clear(uifo.clone(), publisher).await?;
                    self.0.remove(&hello.write_addr);
                }
            }
        }
    }

    fn id(&self, addr: &SocketAddr) -> Option<PublisherId> {
        self.0.get(addr).and_then(|ifo| match ifo {
            ClientInfo::CleaningUp(_) => None,
            ClientInfo::Running { publisher, .. } => Some(publisher.id),
        })
    }
}

struct Clinfos(Mutex<ClinfosInner>);

impl Deref for Clinfos {
    type Target = Mutex<ClinfosInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clinfos {
    fn new() -> Self {
        Clinfos(Mutex::new(ClinfosInner(AHashMap::default())))
    }
}

/// The read gate as the accept path wants it: one relaxed load.
///
/// Every read connection checks this at hello, so it must not be a lock. A
/// herd of subscribers reconnecting at once would serialize on the cache
/// line, and an `RwLock` would not help — a reader still has to take the line
/// exclusively to count itself in, where a shared load leaves it shared.
struct Gate(AtomicI64);

impl Gate {
    fn new(gate: ReadGate) -> Gate {
        Gate(AtomicI64::new(gate.opens_at()))
    }

    fn is_open(&self) -> bool {
        ReadGate::open_at(self.0.load(Ordering::Relaxed))
    }

    /// Set the gate, returning whether that changed it. Relaxed is enough
    /// either way: the gate publishes nothing but itself, so a connection that
    /// reads the old one is indistinguishable from one that arrived sooner.
    fn set(&self, gate: ReadGate) -> bool {
        let opens_at = gate.opens_at();
        self.0.swap(opens_at, Ordering::Relaxed) != opens_at
    }
}

struct Ctx {
    clinfos: Clinfos,
    ctracker: CTracker,
    secctx: SecCtx,
    cfg: MemberServer,
    id: SocketAddr,
    store: Store,
    delay_reads: Option<Instant>,
    read_gate: Arc<Gate>,
}

impl Ctx {
    /// Reads are refused while the startup delay is still running, or while
    /// the config gate is shut. Writes are never gated either way: a
    /// publisher has to be able to fill a replica that subscribers are being
    /// kept away from, and a departing one has to be able to age out.
    fn reads_allowed(&self) -> bool {
        match self.delay_reads {
            Some(t) if Instant::now() < t => false,
            Some(_) | None => self.read_gate.is_open(),
        }
    }
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
    debug!("starting write loop for {:?}", connection_id);
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
            Some(con) => con.receive_batch(batch).await,
            None => {
                trace!("isn't connected, not receiving");
                future::pending().await
            }
        }
    }
    'main: loop {
        select_biased! {
            _ = server_stop => break Ok(()),
            _ = rx_stop => break Ok(()),
            _ = timeout.tick().fuse() => {
                if act {
                    trace!("checking timeout, {:?} was active", connection_id);
                    act = false;
                } else {
                    trace!("dropping inactive connection {:?} ", connection_id);
                    drop(con);
                    ctx.ctracker.close(connection_id);
                    ctx.clinfos.lock().await.remove(&ctx, &publisher, &uifo).await?;
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
                    trace!("{:?} received a batch {batch:?}", connection_id);
                    act = true;
                    if batch.len() == 1 && batch[0] == ToWrite::Heartbeat {
                        trace!("{:?} batch is just a heartbeat", connection_id);
                        continue 'main
                    }
                    let c = match con.as_mut() {
                        Some(c) => c,
                        None => unreachable!("bug, con is none and we received a batch"),
                    };
                    trace!("{:?} checking batch of len {} for clear", connection_id, batch.len());
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
                                    trace!("{:?} handling clear", connection_id);
                                    ctx.store.handle_clear(
                                        uifo.clone(),
                                        publisher.clone()
                                    ).await?;
                                    c.queue_send(&FromWrite::Unpublished)?
                                }
                            }
                        }
                        c.flush().await?;
                        batch = GPooled::orphan(rest);
                    }
                    trace!("{:?} handling write batch of size {}", connection_id, batch.len());
                    if let Err(e) = ctx.store.handle_batch_write(
                        Some(c),
                        uifo.clone(),
                        publisher.clone(),
                        mem::replace(&mut batch, WRITE_BATCHES.take())
                    ).await {
                        warn!("handle_write_batch failed {}", e);
                        con = None;
                        ctx.ctracker.close(connection_id);
                        continue 'main;
                    }
                    trace!("{:?} write success", connection_id);
                }
            },
        }
    }
}

const TOKEN_MAX: usize = 64 * 1024;

async fn recv<T: Pack + Debug>(timeout: Duration, con: &mut TcpStream) -> Result<T> {
    Ok(time::timeout(timeout, channel::read_raw::<_, _, TOKEN_MAX>(con)).await??)
}
async fn send(timeout: Duration, con: &mut TcpStream, msg: &impl Pack) -> Result<()> {
    Ok(time::timeout(timeout, channel::write_raw(con, msg)).await??)
}

pub(crate) async fn krb5_authentication(
    timeout: Duration,
    spn: Option<&str>,
    con: &mut TcpStream,
) -> Result<ServerCtx> {
    let spn = spn.map(ArcStr::from);
    let mut ctx = task::spawn_blocking(move || {
        ServerCtx::new(AcceptFlags::empty(), spn.as_ref().map(|s| s.as_str()), None)
    })
    .await??;
    loop {
        let token: BoundedBytes<TOKEN_MAX> = recv(timeout, con).await?;
        match task::spawn_blocking(move || ctx.step(&*token)).await?? {
            Step::Continue((nctx, token)) => {
                ctx = nctx;
                let token = BoundedBytes::<TOKEN_MAX>(utils::bytes(&*token));
                send(timeout, con, &token).await?;
            }
            Step::Finished((ctx, token)) => {
                if let Some(token) = token {
                    let token = BoundedBytes::<TOKEN_MAX>(utils::bytes(&*token));
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
    let n = rng().random::<u128>();
    let answer = make_sha3_token([&n.to_be_bytes()[..], &secret.to_be_bytes()[..]]);
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
    let secret = rng().random::<u128>();
    let timeout = ctx.cfg.hello_timeout;
    time::timeout(timeout, con.send_one(&Secret(secret))).await??;
    let _: ReadyForOwnershipCheck = time::timeout(timeout, con.receive()).await??;
    info!("hello_write connecting to {:?} for listener ownership check", write_addr);
    let con = time::timeout(timeout, TcpStream::connect(write_addr)).await??;
    let mut con = Channel::new::<ServerCtx, TcpStream>(None, con);
    time::timeout(timeout, con.send_one(&3u64)).await??;
    if time::timeout(timeout, con.receive::<u64>()).await?? != 3 {
        bail!("incompatible protocol version")
    }
    use publisher::Hello as PHello;
    let n = rng().random::<u128>();
    let answer =
        utils::make_sha3_token([&n.to_be_bytes()[..], &secret.to_be_bytes()[..]]);
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

/// A hello that says nothing except why we will not take this publisher.
fn refusal_hello(ctx: &Ctx, auth: AuthWrite, refused: WriteRefusal) -> ServerHelloWrite {
    info!("hello_write refusing the publisher: {refused:?}");
    ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired: true,
        resolver_id: ctx.id,
        auth,
        refused: Some(refused),
    }
}

/// Say why, rather than dropping the socket and leaving the publisher to
/// guess. It has nothing else to go on: it will retry forever either way.
///
/// Sent where the publisher is already waiting to read a `ServerHelloWrite`,
/// and before anything is registered, so refusing costs it one round trip and
/// us nothing.
async fn refuse_write_raw(
    ctx: &Ctx,
    con: &mut TcpStream,
    auth: AuthWrite,
    refused: WriteRefusal,
) -> anyhow::Error {
    let h = refusal_hello(ctx, auth, refused);
    let _ = send(ctx.cfg.hello_timeout, con, &h).await;
    anyhow!("refused the publisher: {refused:?}")
}

async fn refuse_write(
    ctx: &Ctx,
    con: &mut Channel,
    auth: AuthWrite,
    refused: WriteRefusal,
) -> anyhow::Error {
    let h = refusal_hello(ctx, auth, refused);
    let _ = time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await;
    anyhow!("refused the publisher: {refused:?}")
}

/// Turn a failure to work out who the publisher is into a refusal.
///
/// The handshake has already succeeded by the time this is asked, so dropping
/// the socket here looks to the publisher exactly like the resolver going
/// away, and it retries forever against a user database that will keep saying
/// the same thing. `refused` is the only word that distinguishes "I will not
/// have you" from "I am not here".
///
/// Logged at error rather than warn: the publisher is told to come here for
/// the reason, and a resolver run at the default filter would otherwise have
/// nothing to show it.
async fn authorized(
    ctx: &Ctx,
    con: &mut Channel,
    auth: AuthWrite,
    write_addr: SocketAddr,
    uifo: Result<Arc<UserInfo>>,
) -> Result<Arc<UserInfo>> {
    match uifo {
        Ok(uifo) => Ok(uifo),
        Err(e) => {
            error!("refusing publisher {write_addr}, could not authorize it: {e:#}");
            Err(refuse_write(ctx, con, auth, WriteRefusal::Unauthorized).await)
        }
    }
}

async fn write_client_anonymous_auth(
    ctx: &Arc<Ctx>,
    mut con: TcpStream,
    hello: &ClientHelloWrite,
    refused: Option<WriteRefusal>,
) -> AuthResult {
    if let Some(r) = refused {
        return Err(refuse_write_raw(ctx, &mut con, AuthWrite::Anonymous, r).await);
    }
    let uifo = &*ANONYMOUS;
    let (publisher, ttl_expired, rx_stop) =
        ctx.clinfos.lock().await.insert(&ctx, uifo, &hello).await?;
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired,
        resolver_id: ctx.id,
        auth: AuthWrite::Anonymous,
        refused: None,
    };
    info!("hello_write accepting Anonymous authentication");
    debug!("hello_write sending hello {:?}", h);
    if let Err(e) = send(ctx.cfg.hello_timeout, &mut con, &h).await {
        ctx.clinfos.lock().await.remove(&ctx, &publisher, uifo).await?;
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
    refused: Option<WriteRefusal>,
) -> AuthResult {
    let tok: BoundedBytes<TOKEN_MAX> = recv(ctx.cfg.hello_timeout, &mut con).await?;
    // the channel frames a lone message exactly as `write_raw` does, so
    // building it here rather than after the hello changes no bytes, and lets
    // everything below refuse through the same path
    let mut con = Channel::new::<ServerCtx, TcpStream>(None, con);
    let cred = a.0.authenticate(&*tok)?;
    let uifo = a.1.write().await.users.ifo(ctx.id, Some(&cred.user)).await;
    let uifo =
        authorized(ctx, &mut con, AuthWrite::Local, hello.write_addr, uifo).await?;
    if let Some(r) = refused {
        return Err(refuse_write(ctx, &mut con, AuthWrite::Local, r).await);
    }
    info!("hello_write local auth succeeded");
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired: true, // re auth always clears
        resolver_id: ctx.id,
        auth: AuthWrite::Local,
        refused: None,
    };
    debug!("hello_write sending {:?}", h);
    time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await??;
    let secret = ownership_check(&ctx, &mut con, hello.write_addr).await?;
    let (publisher, _, rx_stop) =
        ctx.clinfos.lock().await.insert(&ctx, &uifo, &hello).await?;
    let d = LocalSecData { user: cred.user, secret };
    a.1.write().await.insert(publisher.id, d);
    Ok((con, uifo, publisher, rx_stop))
}

async fn write_client_reuse_local(
    ctx: &Arc<Ctx>,
    con: TcpStream,
    a: &Arc<(secctx::LocalAuth, RwLock<secctx::SecCtxData<secctx::LocalSecData>>)>,
    hello: &ClientHelloWrite,
    refused: Option<WriteRefusal>,
) -> AuthResult {
    let wa = &hello.write_addr;
    let id = ctx.clinfos.lock().await.id(wa).ok_or_else(|| anyhow!("missing"))?;
    let d = a.1.read().await.get(&id).ok_or_else(|| anyhow!("missing"))?.clone();
    let uifo = a.1.write().await.users.ifo(ctx.id, Some(&*d.user)).await;
    let mut con = Channel::new::<ServerCtx, TcpStream>(None, con);
    challenge_auth(&ctx.cfg, &mut con, d.secret).await?;
    let uifo =
        authorized(ctx, &mut con, AuthWrite::Reuse, hello.write_addr, uifo).await?;
    if let Some(r) = refused {
        return Err(refuse_write(ctx, &mut con, AuthWrite::Reuse, r).await);
    }
    let (publisher, ttl_expired, rx_stop) =
        ctx.clinfos.lock().await.insert(&ctx, &uifo, &hello).await?;
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired,
        resolver_id: ctx.id,
        auth: AuthWrite::Reuse,
        refused: None,
    };
    match time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await {
        Ok(Ok(())) => (),
        Err(e) => {
            ctx.clinfos.lock().await.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
        Ok(Err(e)) => {
            ctx.clinfos.lock().await.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
    }
    Ok((con, uifo, publisher, rx_stop))
}

async fn write_client_krb5_auth(
    ctx: &Arc<Ctx>,
    mut con: TcpStream,
    a: &Arc<(ArcStr, RwLock<secctx::SecCtxData<secctx::K5SecData>>)>,
    hello: &ClientHelloWrite,
    refused: Option<WriteRefusal>,
) -> AuthResult {
    info!("hello_write initiating new krb5 context for {:?}", hello.write_addr);
    let k5ctx = krb5_authentication(ctx.cfg.hello_timeout, Some(&*a.0), &mut con).await?;
    let k5ctx = K5CtxWrap::new(k5ctx);
    let mut con = Channel::new(Some(k5ctx.clone()), con);
    info!("hello_write all traffic now encrypted");
    let auth = AuthWrite::Krb5 { spn: literal!("") };
    // above the hello rather than below it, because the hello is the last
    // thing we can attach a reason to
    let uifo = krb5_uifo(ctx.id, &k5ctx, a).await;
    let uifo = authorized(ctx, &mut con, auth.clone(), hello.write_addr, uifo).await?;
    if let Some(r) = refused {
        return Err(refuse_write(ctx, &mut con, auth, r).await);
    }
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired: true, // re auth always clears
        resolver_id: ctx.id,
        auth,
        refused: None,
    };
    debug!("hello_write sending {:?}", h);
    time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await??;
    let secret = ownership_check(&ctx, &mut con, hello.write_addr).await?;
    info!("hello_write listener ownership check succeeded");
    let (publisher, _, rx_stop) =
        ctx.clinfos.lock().await.insert(&ctx, &uifo, &hello).await?;
    let d = K5SecData { ctx: k5ctx, secret };
    a.1.write().await.insert(publisher.id, d);
    Ok((con, uifo, publisher, rx_stop))
}

/// Who the peer is according to the kerberos context, and what the resolver's
/// user database says they may do.
async fn krb5_uifo(
    id: SocketAddr,
    k5ctx: &K5CtxWrap<ServerCtx>,
    a: &Arc<(ArcStr, RwLock<secctx::SecCtxData<secctx::K5SecData>>)>,
) -> Result<Arc<UserInfo>> {
    let client = k5ctx.lock().client().context("getting the krb5 client name")?;
    a.1.write().await.users.ifo(id, Some(&client)).await.context("getting user info")
}

async fn write_client_reuse_krb5(
    ctx: &Arc<Ctx>,
    con: TcpStream,
    a: &Arc<(ArcStr, RwLock<secctx::SecCtxData<secctx::K5SecData>>)>,
    hello: &ClientHelloWrite,
    refused: Option<WriteRefusal>,
) -> AuthResult {
    let wa = &hello.write_addr;
    let id = ctx.clinfos.lock().await.id(wa).ok_or_else(|| anyhow!("missing"))?;
    let d = a.1.read().await.get(&id).ok_or_else(|| anyhow!("missing"))?.clone();
    let uifo = krb5_uifo(ctx.id, &d.ctx, a).await;
    let mut con = Channel::new(Some(d.ctx), con);
    info!("hello_write all traffic now encrypted");
    challenge_auth(&ctx.cfg, &mut con, d.secret).await?;
    let uifo =
        authorized(ctx, &mut con, AuthWrite::Reuse, hello.write_addr, uifo).await?;
    if let Some(r) = refused {
        return Err(refuse_write(ctx, &mut con, AuthWrite::Reuse, r).await);
    }
    let (publisher, ttl_expired, rx_stop) =
        ctx.clinfos.lock().await.insert(&ctx, &uifo, &hello).await?;
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired,
        resolver_id: ctx.id,
        auth: AuthWrite::Reuse,
        refused: None,
    };
    info!("hello_write reusing krb5 context");
    debug!("hello_write sending {:?}", h);
    match time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await {
        Ok(Ok(())) => (),
        Err(e) => {
            ctx.clinfos.lock().await.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
        Ok(Err(e)) => {
            ctx.clinfos.lock().await.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
    }
    Ok((con, uifo, publisher, rx_stop))
}

async fn get_tls_uifo(
    id: SocketAddr,
    tls: &tokio_rustls::server::TlsStream<TcpStream>,
    a: &Arc<(tls::CrlWatchingAcceptor, RwLock<secctx::SecCtxData<secctx::TlsSecData>>)>,
) -> Result<Arc<UserInfo>> {
    let (_, server_con) = tls.get_ref();
    match server_con.peer_certificates() {
        Some([cert, ..]) => {
            let names = tls::get_names(&*cert).context("getting tls names")?;
            Ok(a.1
                .write()
                .await
                .users
                .ifo(id, names.as_ref().map(|names| names.cn.as_str()))
                .await
                .context("getting user info")?)
        }
        Some(_) | None => bail!("tls handshake should be complete by now"),
    }
}

async fn write_client_tls_auth(
    ctx: &Arc<Ctx>,
    con: TcpStream,
    a: &Arc<(tls::CrlWatchingAcceptor, RwLock<secctx::SecCtxData<secctx::TlsSecData>>)>,
    hello: &ClientHelloWrite,
    refused: Option<WriteRefusal>,
) -> AuthResult {
    let tls = a.0.acceptor().accept(con).await?;
    // held rather than propagated: the answer needs the channel that the
    // stream is about to become before it can be sent
    let uifo = get_tls_uifo(ctx.id, &tls, a).await;
    let mut con =
        Channel::new::<ServerCtx, tokio_rustls::server::TlsStream<TcpStream>>(None, tls);
    info!("hello_write all traffic now encrypted");
    let auth = AuthWrite::Tls { name: literal!("") };
    let uifo = authorized(ctx, &mut con, auth.clone(), hello.write_addr, uifo).await?;
    if let Some(r) = refused {
        return Err(refuse_write(ctx, &mut con, auth, r).await);
    }
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired: true,
        resolver_id: ctx.id,
        auth,
        refused: None,
    };
    debug!("hello_write sending {:?}", h);
    time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await??;
    let secret = ownership_check(&ctx, &mut con, hello.write_addr).await?;
    info!("hello_write listener ownership check succeeded");
    let (publisher, _, rx_stop) =
        ctx.clinfos.lock().await.insert(&ctx, &uifo, hello).await?;
    let d = TlsSecData(secret);
    a.1.write().await.insert(publisher.id, d);
    Ok((con, uifo, publisher, rx_stop))
}

async fn write_client_reuse_tls(
    ctx: &Arc<Ctx>,
    con: TcpStream,
    a: &Arc<(tls::CrlWatchingAcceptor, RwLock<secctx::SecCtxData<secctx::TlsSecData>>)>,
    hello: &ClientHelloWrite,
    refused: Option<WriteRefusal>,
) -> AuthResult {
    let tls = a.0.acceptor().accept(con).await?;
    let wa = &hello.write_addr;
    let id = ctx.clinfos.lock().await.id(wa).ok_or_else(|| anyhow!("missing"))?;
    let d = a.1.read().await.get(&id).ok_or_else(|| anyhow!("missing"))?.clone();
    let uifo = get_tls_uifo(ctx.id, &tls, a).await;
    let mut con =
        Channel::new::<ServerCtx, tokio_rustls::server::TlsStream<TcpStream>>(None, tls);
    info!("hello_write all traffic now encrypted");
    challenge_auth(&ctx.cfg, &mut con, d.0).await?;
    let uifo =
        authorized(ctx, &mut con, AuthWrite::Reuse, hello.write_addr, uifo).await?;
    if let Some(r) = refused {
        return Err(refuse_write(ctx, &mut con, AuthWrite::Reuse, r).await);
    }
    let (publisher, ttl_expired, rx_stop) =
        ctx.clinfos.lock().await.insert(&ctx, &uifo, &hello).await?;
    let h = ServerHelloWrite {
        ttl: ctx.cfg.writer_ttl.as_secs(),
        ttl_expired,
        resolver_id: ctx.id,
        auth: AuthWrite::Reuse,
        refused: None,
    };
    info!("hello_write reusing tls context");
    debug!("hello_write sending {:?}", h);
    match time::timeout(ctx.cfg.hello_timeout, con.send_one(&h)).await {
        Ok(Ok(())) => (),
        Err(e) => {
            ctx.clinfos.lock().await.remove(&ctx, &publisher, &uifo).await?;
            Err(e)?
        }
        Ok(Err(e)) => {
            ctx.clinfos.lock().await.remove(&ctx, &publisher, &uifo).await?;
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
    // not `?`: the publisher can only be told this over a connection we
    // finish negotiating, and it is the answer it has never been able to get
    let refused =
        utils::check_addr(hello.write_addr.ip(), &[(ctx.id, ())]).err().map(|e| {
            error!("refusing publisher {}: {e}", hello.write_addr);
            WriteRefusal::from(e)
        });
    let (con, uifo, publisher, rx_stop) = match hello.auth {
        AuthWrite::Anonymous => {
            write_client_anonymous_auth(&ctx, con, &hello, refused).await?
        }
        AuthWrite::Local => match &ctx.secctx {
            SecCtx::Local(a) => {
                write_client_local_auth(&ctx, con, a, &hello, refused).await?
            }
            SecCtx::Anonymous | SecCtx::Krb5(_) | SecCtx::Tls(_) => bail!(NO),
        },
        AuthWrite::Krb5 { .. } => match &ctx.secctx {
            SecCtx::Krb5(a) => {
                write_client_krb5_auth(&ctx, con, a, &hello, refused).await?
            }
            SecCtx::Anonymous | SecCtx::Local(_) | SecCtx::Tls(_) => bail!(NO),
        },
        AuthWrite::Tls { .. } => match &ctx.secctx {
            SecCtx::Tls(a) => {
                write_client_tls_auth(&ctx, con, a, &hello, refused).await?
            }
            SecCtx::Anonymous | SecCtx::Local(_) | SecCtx::Krb5(_) => bail!(NO),
        },
        AuthWrite::Reuse => match &ctx.secctx {
            SecCtx::Local(a) => {
                write_client_reuse_local(&ctx, con, a, &hello, refused).await?
            }
            SecCtx::Krb5(a) => {
                write_client_reuse_krb5(&ctx, con, a, &hello, refused).await?
            }
            SecCtx::Tls(a) => {
                write_client_reuse_tls(&ctx, con, a, &hello, refused).await?
            }
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
                let uifo = a.1.write().await.users.ifo(ctx.id, Some(&cred.user)).await?;
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
                let client = k5ctx.lock().client()?;
                let uifo = a.1.write().await.users.ifo(ctx.id, Some(&client)).await?;
                (con, uifo)
            }
            SecCtx::Anonymous | SecCtx::Local(_) | SecCtx::Tls(_) => bail!(NO),
        },
        AuthRead::Tls => match &ctx.secctx {
            SecCtx::Tls(a) => {
                let tls =
                    a.0.acceptor()
                        .accept(con)
                        .await
                        .context("accepting tls connection")?;
                let uifo =
                    get_tls_uifo(ctx.id, &tls, a).await.context("getting tls info")?;
                let mut con = Channel::new::<
                    ServerCtx,
                    tokio_rustls::server::TlsStream<TcpStream>,
                >(None, tls);
                time::timeout(ctx.cfg.hello_timeout, con.send_one(&AuthRead::Tls))
                    .await
                    .context("saying hello")??;
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
    send(ctx.cfg.hello_timeout, &mut s, &3u64).await?;
    let version: u64 = recv(ctx.cfg.hello_timeout, &mut s).await?;
    if version != 3 {
        bail!("unsupported protocol version")
    }
    let hello: ClientHello = recv(ctx.cfg.hello_timeout, &mut s).await?;
    match hello {
        ClientHello::ReadOnly(hello) => {
            if !ctx.reads_allowed() {
                bail!("no read clients allowed yet");
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
    ready: oneshot::Sender<Ready>,
    id: usize,
    listener: Option<TcpListener>,
) -> Result<()> {
    debug!("server task start I am id: {}", id);
    let member = cfg.member_servers[id].clone();
    debug!("my member config {:?}", member);
    let delay_reads =
        if delay_reads { Some(Instant::now() + member.writer_ttl) } else { None };
    let id = member.addr;
    let listen_addr = SocketAddr::new(member.bind_addr, id.port());
    debug!("creating tcp listener on {:?}", listen_addr);
    let listener = match listener {
        None => TcpListener::bind(listen_addr).await?,
        Some(listener) => listener,
    };
    debug!("creating security context");
    let secctx = SecCtx::new(&cfg, &member).await?;
    debug!("creating resolver store");
    let store = Store::new(
        cfg.parent.clone().map(|s| s.into()),
        cfg.children.iter().map(|(p, s)| (p.clone(), s.clone().into())).collect(),
        secctx.clone(),
        id,
    );
    let read_gate = Arc::new(Gate::new(member.read_gated));
    let ctx = Arc::new(Ctx {
        cfg: member,
        secctx,
        clinfos: Clinfos::new(),
        ctracker: CTracker::new(),
        id,
        delay_reads,
        store,
        read_gate: read_gate.clone(),
    });
    let mut stop = stop.fuse();
    let mut client_stops: Vec<oneshot::Sender<()>> = Vec::new();
    // Hold a line open to the id-map daemon, if this member takes its
    // identities from one, so a group revoked by an administrator stops being
    // honoured when the daemon says so rather than when the cache happens to
    // expire. Parked in `client_stops` because that is already drained at
    // shutdown — the watch should end with the server, not outlive it.
    #[cfg(unix)]
    if let config::IdMap::Socket(path) = &ctx.cfg.id_map {
        client_stops.push(id_map_watch::spawn(
            netidx_core::utils::id_map_control_socket(std::path::Path::new(&**path)),
            ctx.secctx.clone(),
        ));
    }
    let max_connections = ctx.cfg.max_connections;
    debug!("signaling ready");
    let mut listen_addr = listener.local_addr()?;
    listen_addr.set_ip(id.ip());
    let _ = ready.send(Ready {
        local_addr: listen_addr,
        secctx: ctx.secctx.clone(),
        store: ctx.store.clone(),
        read_gate,
    });
    loop {
        select_biased! {
            _ = stop => {
                debug!("server loop stop requested");
                for cl in client_stops.drain(..) {
                    let _ = cl.send(());
                }
                return Ok(())
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
                    debug!("I have {} writers", ctx.clinfos.lock().await.0.len())
                }
            },
        }
    }
}

/// What `server_loop` hands back once the server is up.
struct Ready {
    local_addr: SocketAddr,
    secctx: SecCtx,
    store: Store,
    read_gate: Arc<Gate>,
}

/// The parts of a config that are fixed once the server is running: which
/// members exist, and where each child cluster attaches.
fn startup_shape(
    cfg: &Config,
    id: usize,
) -> (SocketAddr, Vec<SocketAddr>, BTreeSet<Path>) {
    let members = cfg.member_servers.iter().map(|m| m.addr).collect::<Vec<_>>();
    let children = cfg.children.keys().cloned().collect::<BTreeSet<_>>();
    (members[id], members, children)
}

/// Run a resolver server
pub struct Server {
    stop: Option<oneshot::Sender<()>>,
    local_addr: SocketAddr,
    secctx: SecCtx,
    store: Store,
    read_gate: Arc<Gate>,
    /// The advertised address of the member this process is running. Used to
    /// find ourselves in an edited config: matching by address rather than by
    /// index means a reordered `member_servers` can't hand us someone else's
    /// settings.
    member_addr: SocketAddr,
    /// The member addresses this server started with. `member_servers` is not
    /// applied live, so this — not whatever the file says now — is what a
    /// reloaded referral must not point back at.
    member_addrs: Vec<SocketAddr>,
    /// The child paths this server started with. Where a child attaches is
    /// structural; only its addresses can change under a running server.
    child_paths: BTreeSet<Path>,
}

/// What a reload could not apply.
///
/// The resolver takes referral *addresses* live, so a resolver added to or
/// removed from a neighbouring cluster reaches it without a restart. The shape
/// of the tree is fixed when the store is built.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NotApplied {
    /// Child paths in the file that the running server does not serve.
    pub children_added: Vec<Path>,
    /// Child paths the running server serves that the file no longer lists.
    pub children_removed: Vec<Path>,
}

impl NotApplied {
    pub fn is_empty(&self) -> bool {
        self.children_added.is_empty() && self.children_removed.is_empty()
    }
}

impl std::fmt::Debug for Server {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Server").field("local_addr", &self.local_addr).finish()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = mem::replace(&mut self.stop, None) {
            let _ = stop.send(());
        }
    }
}

impl Server {
    /// Create a new resolver server
    ///
    /// # Parameters
    /// - cfg: the config
    /// - delay_reads: don't allow reads until 2x the writer timeout, this allows
    ///   publisher to republish to this resolver server before subscribers are allowed
    ///   to use it.
    /// - id: the index of this resolver server in the config's members array
    ///
    /// When this future is resolved the server will be running. If
    /// the returned `Server` is dropped the server will stop.
    pub async fn new(cfg: Config, delay_reads: bool, id: usize) -> Result<Server> {
        let (member_addr, member_addrs, child_paths) = startup_shape(&cfg, id);
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        task::spawn(async move {
            let res =
                server_loop(cfg, delay_reads, recv_stop, send_ready, id, None).await;
            match &res {
                Ok(_) => info!("resolver server shutdown"),
                Err(e) => error!("resolver server failed {}", e),
            }
            res
        });
        let ready = match recv_ready.await {
            Err(_) => bail!("resolver server shutdown"),
            Ok(t) => t,
        };
        Ok(Server {
            stop: Some(send_stop),
            local_addr: ready.local_addr,
            secctx: ready.secctx,
            store: ready.store,
            read_gate: ready.read_gate,
            member_addr,
            member_addrs,
            child_paths,
        })
    }

    /// Start a new local only resolver server
    ///
    /// # Parameters
    /// - cfg: the config
    /// - listener: the already listening socket
    ///
    /// This function requires the following conditions,
    /// - Config must contain exactly one member server, and it's address must
    /// match the address of the listener.
    /// - The ip address of the listener must be localhost
    pub(crate) async fn new_local_only(
        cfg: Config,
        listener: TcpListener,
    ) -> Result<Server> {
        if cfg.member_servers.len() != 1 {
            bail!("expected exactly one member server")
        }
        if cfg.member_servers[0].addr != listener.local_addr()? {
            bail!("cfg addr does not match actual listen addr")
        }
        let (member_addr, member_addrs, child_paths) = startup_shape(&cfg, 0);
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        task::spawn(async move {
            let res =
                server_loop(cfg, false, recv_stop, send_ready, 0, Some(listener)).await;
            match &res {
                Ok(_) => info!("resolver server shutdown"),
                Err(e) => error!("resolver server failed {}", e),
            }
            res
        });
        let ready = match recv_ready.await {
            Err(_) => bail!("resolver server shutdown"),
            Ok(t) => t,
        };
        Ok(Server {
            stop: Some(send_stop),
            local_addr: ready.local_addr,
            secctx: ready.secctx,
            store: ready.store,
            read_gate: ready.read_gate,
            member_addr,
            member_addrs,
            child_paths,
        })
    }

    /// Get the local address this resolver server is bound to
    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    /// Replace the running PMap with one rebuilt from `new_perms`.
    ///
    /// This is the runtime hook the SIGHUP-handler in
    /// `netidx-tools/src/resolver_server.rs` calls when the operator
    /// sends `SIGHUP`. Callers pass the merged file-level perms map
    /// (use `Config::merge_perms_only` to compute it from a
    /// `file::Config`); the cluster root and children are reused
    /// from the running server's startup state. On the `Anonymous`
    /// auth variant the call is a no-op (no PMap to swap). On any
    /// other variant it acquires the relevant write lock, rebuilds
    /// the `PMap` using the existing `UserDb` (so previously-
    /// resolved entity IDs stay valid for in-flight connections),
    /// and swaps it in.
    ///
    /// On any error the running PMap is left intact and the error
    /// is returned to the caller. Callers are expected to log it
    /// at WARN level and continue — never crash the server because
    /// a perms reload failed.
    pub async fn reload_perms(
        &self,
        new_perms: &crate::resolver_server::config::PMap,
    ) -> Result<()> {
        self.secctx.reload_pmap(new_perms).await
    }

    /// Apply an edited config to the running server.
    ///
    /// Two things are taken live: the permission map, and the *addresses* of
    /// the parent and child referrals. Between them that covers the whole of
    /// "a resolver was added to or removed from a neighbouring cluster",
    /// which the administrative plane pushes into this file and which
    /// otherwise sat there until the next restart.
    ///
    /// What is not taken live is the shape of the tree — where children
    /// attach, and which members exist — because the store is built around
    /// it. Anything of that kind found in `cfg` is returned in
    /// [`NotApplied`] rather than silently ignored, so the caller can tell
    /// the operator a restart is still needed.
    ///
    /// Validation happens before anything is swapped, so a config that
    /// doesn't hold together leaves the running server exactly as it was and
    /// returns the error. Callers should log it and carry on — never crash a
    /// running resolver over a bad edit.
    pub async fn reload(&self, cfg: &config::file::Config) -> Result<NotApplied> {
        let perms = config::merge_perms_only(cfg)
            .context("merging perms (include_permissions + inline)")?;
        let (parent, children) = config::check_referrals(
            cfg.parent.clone(),
            cfg.children.clone(),
            &self.member_addrs,
        )
        .context("validating referrals")?;
        let not_applied = NotApplied {
            children_added: children
                .keys()
                .filter(|p| !self.child_paths.contains(*p))
                .cloned()
                .collect(),
            children_removed: self
                .child_paths
                .iter()
                .filter(|p| !children.contains_key(*p))
                .cloned()
                .collect(),
        };
        self.secctx.reload_pmap(&perms).await.context("swapping the live PMap")?;
        self.store
            .set_referrals(
                parent.map(|r| r.into()),
                children.into_iter().map(|(p, r)| (p, r.into())).collect(),
            )
            .await;
        // Found by address, not by index: a reordered `member_servers` must
        // not hand this process someone else's gate. If we aren't in the file
        // at all the gate is left alone — that is a `member_servers` edit,
        // and the caller is already warning about it.
        if let Some(member) =
            cfg.member_servers.iter().find(|m| m.addr == self.member_addr)
        {
            if self.read_gate.set(member.read_gated) {
                info!("read gate is now {:?}", member.read_gated);
            }
        }
        Ok(not_applied)
    }

    /// Whether this server is currently answering read clients.
    pub fn reads_allowed(&self) -> bool {
        self.read_gate.is_open()
    }
}
