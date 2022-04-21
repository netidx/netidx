use crate::{
    channel::Channel,
    channel::K5CtxWrap,
    chars::Chars,
    config::{Auth, Server},
    os::local_auth::AuthClient,
    path::Path,
    pool::{Pool, Pooled},
    protocol::resolver::{
        AuthChallenge, AuthRead, AuthWrite, ClientHello, ClientHelloWrite, FromRead,
        FromWrite, HashMethod, ReadyForOwnershipCheck, Secret, ServerHelloWrite, ToRead,
        ToWrite,
    },
    utils,
};
use anyhow::{anyhow, Error, Result};
use cross_krb5::{ClientCtx, InitiateFlags, K5Ctx, PendingClientCtx, Step};
use crossbeam::epoch::Pointable;
use futures::{
    channel::{mpsc, oneshot},
    future::select_ok,
    prelude::*,
    select_biased,
};
use fxhash::FxBuildHasher;
use log::{debug, error, info, warn};
use netidx_core::pack::BoundedBytes;
use parking_lot::RwLock;
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::{
    arch::x86_64::_MM_FLUSH_ZERO_MASK, cmp::max, collections::HashMap, fmt::Debug,
    net::SocketAddr, str::FromStr, sync::Arc, time::Duration,
};
use tokio::{
    net::TcpStream,
    task,
    time::{self, Instant, Interval},
};

const HELLO_TO: Duration = Duration::from_secs(15);
static TTL: u64 = 120;

lazy_static! {
    pub(crate) static ref TOREADPOOL: Pool<Vec<(usize, ToRead)>> = Pool::new(1000, 10000);
    static ref FROMREADPOOL: Pool<Vec<(usize, FromRead)>> = Pool::new(1000, 10000);
    pub(crate) static ref RAWFROMREADPOOL: Pool<Vec<FromRead>> = Pool::new(1000, 10000);
    pub(crate) static ref TOWRITEPOOL: Pool<Vec<(usize, ToWrite)>> =
        Pool::new(1000, 10000);
    static ref FROMWRITEPOOL: Pool<Vec<(usize, FromWrite)>> = Pool::new(1000, 10000);
    pub(crate) static ref RAWFROMWRITEPOOL: Pool<Vec<FromWrite>> = Pool::new(1000, 10000);
}

#[derive(Debug, Clone)]
pub enum DesiredAuth {
    Anonymous,
    Krb5 { upn: Option<String>, spn: Option<String> },
    Local,
}

impl FromStr for DesiredAuth {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<DesiredAuth, Self::Err> {
        match s {
            "anonymous" => Ok(DesiredAuth::Anonymous),
            "local" => Ok(DesiredAuth::Local),
            "krb5" => Ok(DesiredAuth::Krb5 { upn: None, spn: None }),
            _ => bail!("expected, anonymous, local, or krb5"),
        }
    }
}

pub(crate) async fn krb5_authentication(
    principal: Option<&str>,
    target_principal: &str,
    con: &mut Channel<ClientCtx>,
) -> Result<ClientCtx> {
    async fn send(con: &mut Channel<ClientCtx>, token: &[u8]) -> Result<()> {
        let token = BoundedBytes::<L>(utils::bytes(&*token));
        Ok(time::timeout(HELLO_TO, con.send_one(&token)).await??)
    }
    const L: usize = 1 * 1024 * 1024;
    let (mut ctx, token) = task::block_in_place(|| {
        ClientCtx::new(InitiateFlags::empty(), principal, target_principal, None)
    })?;
    send(con, &*token).await?;
    loop {
        let token: BoundedBytes<L> = time::timeout(HELLO_TO, con.receive()).await??;
        match task::block_in_place(|| ctx.step(&*token))? {
            Step::Continue((nctx, token)) => {
                ctx = nctx;
                send(con, &*token).await?
            }
            Step::Finished((ctx, token)) => {
                if let Some(token) = token {
                    send(con, &*token).await?
                }
                break Ok(ctx);
            }
        }
    }
}

// continue with timeout
macro_rules! cwt {
    ($msg:expr, $e:expr) => {
        try_cf!(
            $msg,
            continue,
            try_cf!($msg, continue, time::timeout(HELLO_TO, $e).await)
        )
    };
}

async fn connect_read(
    resolver: &Server,
    desired_auth: &DesiredAuth,
) -> Result<Channel<ClientCtx>> {
    let mut addrs = resolver.addrs.clone();
    addrs.as_mut_slice().shuffle(&mut thread_rng());
    let mut n = 0;
    loop {
        let addr = addrs[n % addrs.len()];
        let tries = n / addrs.len();
        if tries >= 3 {
            bail!("can't connect to any resolver servers");
        }
        if n % addrs.len() == 0 && tries > 0 {
            let wait = thread_rng().gen_range(1..12);
            time::sleep(Duration::from_secs(wait)).await;
        }
        n += 1;
        let con = cwt!("connect", TcpStream::connect(&addr));
        try_cf!("no delay", con.set_nodelay(true));
        let mut con = Channel::new(con);
        match (desired_auth, &resolver.auth) {
            (DesiredAuth::Anonymous, _) => {
                cwt!("hello", con.send_one(&ClientHello::ReadOnly(AuthRead::Anonymous)));
                match cwt!("reply", con.receive::<AuthRead>()) {
                    AuthRead::Anonymous => (),
                    AuthRead::Local | AuthRead::Krb5 => bail!("protocol error"),
                }
            }
            (DesiredAuth::Local | DesiredAuth::Krb5 { .. }, Auth::Local(path)) => {
                let tok = cwt!("local token", AuthClient::token(path));
                cwt!("hello", con.send_one(&ClientHello::ReadOnly(AuthRead::Local)));
                cwt!("token", con.send_one(&tok));
                match cwt!("reply", con.receive::<AuthRead>()) {
                    AuthRead::Local => (),
                    AuthRead::Krb5 | AuthRead::Anonymous => bail!("protocol error"),
                }
            }
            (DesiredAuth::Krb5 { upn, .. }, Auth::Krb5(spns)) => {
                let upn = upn.as_ref().map(|s| s.as_str());
                let target_spn = spns
                    .get(&addr)
                    .ok_or_else(|| anyhow!("no target spn for resolver {:?}", addr))?;
                cwt!("hello", con.send_one(&ClientHello::ReadOnly(AuthRead::Krb5)));
                let ctx = cwt!("k5auth", krb5_authentication(upn, target_spn, &mut con));
                match cwt!("reply", con.receive::<AuthRead>()) {
                    AuthRead::Krb5 => con.set_ctx(K5CtxWrap::new(ctx)).await,
                    AuthRead::Local | AuthRead::Anonymous => bail!("protocol error"),
                }
            }
        };
        break Ok(con);
    }
}

type ReadBatch =
    (Pooled<Vec<(usize, ToRead)>>, oneshot::Sender<Pooled<Vec<(usize, FromRead)>>>);

async fn connection_read(
    mut receiver: mpsc::UnboundedReceiver<ReadBatch>,
    resolver: Arc<Server>,
    desired_auth: DesiredAuth,
) {
    let mut con: Option<Channel<ClientCtx>> = None;
    'main: loop {
        match receiver.next().await {
            None => break,
            Some((tx_batch, reply)) => {
                let mut tries: usize = 0;
                'batch: loop {
                    if tries > 3 {
                        break;
                    }
                    if tries > 1 {
                        let wait = thread_rng().gen_range(1..12);
                        time::sleep(Duration::from_secs(wait)).await
                    }
                    tries += 1;
                    let c = match con {
                        Some(ref mut c) => c,
                        None => match connect_read(&resolver, &desired_auth).await {
                            Ok(c) => {
                                con = Some(c);
                                con.as_mut().unwrap()
                            }
                            Err(e) => {
                                con = None;
                                warn!("connect_read failed: {}", e);
                                continue;
                            }
                        },
                    };
                    let mut timeout =
                        max(HELLO_TO, Duration::from_micros(tx_batch.len() as u64 * 50));
                    for (_, m) in &*tx_batch {
                        match m {
                            ToRead::List(_) | ToRead::ListMatching(_) => {
                                timeout += HELLO_TO;
                            }
                            _ => (),
                        }
                        match c.queue_send(m) {
                            Ok(()) => (),
                            Err(e) => {
                                warn!("failed to encode {:?}", e);
                                c.clear();
                                continue 'main;
                            }
                        }
                    }
                    match c.flush_timeout(timeout).await {
                        Err(e) => {
                            warn!("read connection send error: {}", e);
                            con = None;
                        }
                        Ok(()) => {
                            let mut rx_batch = RAWFROMREADPOOL.take();
                            while rx_batch.len() < tx_batch.len() {
                                let f = c.receive_batch(&mut *rx_batch);
                                match time::timeout(timeout, f).await {
                                    Ok(Ok(())) => (),
                                    Ok(Err(e)) => {
                                        warn!("read connection failed {}", e);
                                        con = None;
                                        continue 'batch;
                                    }
                                    Err(e) => {
                                        warn!("read connection timeout: {}", e);
                                        con = None;
                                        continue 'batch;
                                    }
                                }
                            }
                            let mut result = FROMREADPOOL.take();
                            result.extend(
                                rx_batch
                                    .drain(..)
                                    .enumerate()
                                    .map(|(i, m)| (tx_batch[i].0, m)),
                            );
                            let _ = reply.send(result);
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolverRead(mpsc::UnboundedSender<ReadBatch>);

impl ResolverRead {
    pub(crate) fn new(resolver: Arc<Server>, desired_auth: DesiredAuth) -> ResolverRead {
        let (to_tx, to_rx) = mpsc::unbounded();
        task::spawn(async move {
            connection_read(to_rx, resolver, desired_auth).await;
            info!("read task shutting down")
        });
        ResolverRead(to_tx)
    }

    pub(crate) fn send(
        &mut self,
        batch: Pooled<Vec<(usize, ToRead)>>,
    ) -> oneshot::Receiver<Pooled<Vec<(usize, FromRead)>>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.unbounded_send((batch, tx));
        rx
    }
}

macro_rules! wt {
    ($e:expr) => {
        time::timeout(HELLO_TO, $e).await
    };
}

async fn connect_write(
    resolver: &Server,
    resolver_addr: SocketAddr,
    write_addr: SocketAddr,
    published: &Arc<RwLock<HashMap<Path, ToWrite>>>,
    secrets: &Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    security_context: &mut Option<K5CtxWrap<ClientCtx>>,
    desired_auth: &DesiredAuth,
    degraded: &mut bool,
) -> Result<(u64, Channel<ClientCtx>)> {
    info!("write_con connecting to resolver {:?}", resolver_addr);
    let con = wt!(TcpStream::connect(&resolver_addr))??;
    con.set_nodelay(true)?;
    let mut con = Channel::new(con);
    wt!(con.send_one(&1u64))??;
    let _version: u64 = wt!(con.receive())??;
    let sec = Duration::from_secs(1);
    let hello = |auth| {
        let h = ClientHello::WriteOnly(ClientHelloWrite { write_addr, auth });
        debug!("write_con connection established hello {:?}", h);
        h
    };
    let (r, ownership_check) = match (desired_auth, &resolver.auth) {
        (DesiredAuth::Anonymous, _) => {
            wt!(con.send_one(&hello(AuthWrite::Anonymous)))??;
            (wt!(con.receive::<ServerHelloWrite>())??, false)
        }
        (DesiredAuth::Krb5 { .. } | DesiredAuth::Local, Auth::Local(path)) => {
            let secret = secrets.read().get(&resolver_addr).map(|u| *u);
            match secret {
                None => {
                    let tok = wt!(AuthClient::token(path))??;
                    wt!(con.send_one(&hello(AuthWrite::Local)))??;
                    wt!(con.send_one(&tok))??;
                    (wt!(con.receive::<ServerHelloWrite>())??, true)
                }
                Some(secret) => {
                    wt!(con.send_one(&hello(AuthWrite::Reuse)))??;
                    let c: AuthChallenge = wt!(con.receive())??;
                    if c.hash_method != HashMethod::Sha3_512 {
                        bail!("hash method not supported")
                    }
                    let answer = utils::make_sha3_token(&[
                        &c.challenge.to_be_bytes(),
                        &secret.to_be_bytes(),
                    ]);
                    wt!(con.send_one(&answer))??;
                    (wt!(con.receive::<ServerHelloWrite>())??, false)
                }
            }
        }
        (DesiredAuth::Krb5 { upn, spn }, Auth::Krb5(spns)) => match security_context {
            Some(ctx)
                if task::block_in_place(|| ctx.lock().ttl()).unwrap_or(sec) > sec =>
            {
                wt!(con.send_one(&hello(AuthWrite::Reuse)))??;
                let r: ServerHelloWrite = wt!(con.receive())??;
                con.set_ctx(ctx.clone()).await;
                (r, false)
            }
            None | Some(_) => {
                let upn = upn.as_ref().map(|s| s.as_str());
                let spn = Chars::from(
                    spn.as_ref()
                        .map(|s| s.clone())
                        .ok_or_else(|| anyhow!("spn is required for writers"))?,
                );
                let target_spn = spns.get(&resolver_addr).ok_or_else(|| {
                    anyhow!("no target spn for resolver {:?}", resolver_addr)
                })?;
                wt!(con.send_one(&hello(AuthWrite::Krb5 { spn })))??;
                let ctx = krb5_authentication(upn, target_spn, &mut con).await?;
                let ctx = K5CtxWrap::new(ctx);
                let r: ServerHelloWrite = wt!(con.receive())??;
                con.set_ctx(ctx.clone());
                *security_context = Some(ctx);
                (r, true)
            }
        },
    };
    debug!("write_con resolver hello {:?}", r);
    if ownership_check {
        let secret: Secret = wt!(con.receive())??;
        {
            let mut secrets = secrets.write();
            secrets.insert(resolver_addr, secret.0);
            secrets.insert(r.resolver_id, secret.0);
        }
        wt!(con.send_one(&ReadyForOwnershipCheck))??;
    }
    if !r.ttl_expired && !*degraded {
        info!("connected to resolver {:?} for write", resolver_addr);
        Ok((r.ttl, con))
    } else {
        let names = published.read().values().cloned().collect::<Vec<ToWrite>>();
        let len = names.len();
        if len == 0 {
            info!("connected to resolver {:?} for write", resolver_addr);
            if *degraded {
                con.send_one(&ToWrite::Clear).await?;
                match con.receive().await? {
                    FromWrite::Unpublished => {
                        *degraded = false;
                    }
                    m => warn!("unexpected response to clear {:?}", m),
                }
            }
            Ok((r.ttl, con))
        } else {
            info!(
                "write_con ttl: {} degraded: {}, republishing: {}",
                len, r.ttl_expired, *degraded
            );
            for msg in &names {
                con.queue_send(msg)?
            }
            con.flush().await?;
            let mut success = 0;
            for msg in &names {
                match try_cf!("replublish reply", continue, con.receive().await) {
                    FromWrite::Published => {
                        success += 1;
                    }
                    r => {
                        warn!("unexpected republish reply for {:?} {:?}", msg, r)
                    }
                }
            }
            *degraded = success != len;
            info!(
                "connected to resolver {:?} for write (republished {})",
                resolver_addr,
                names.len()
            );
            Ok((r.ttl, con))
        }
    }
}

async fn connection_write(
    receiver: mpsc::Receiver<(
        Arc<Pooled<Vec<(usize, ToWrite)>>>,
        oneshot::Sender<Pooled<Vec<(usize, FromWrite)>>>,
    )>,
    resolver: Arc<Server>,
    resolver_addr: SocketAddr,
    write_addr: SocketAddr,
    published: Arc<RwLock<HashMap<Path, ToWrite>>>,
    desired_auth: DesiredAuth,
    secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
) {
    let mut receiver = receiver.fuse();
    let mut degraded = false;
    let mut con: Option<Channel<ClientCtx>> = None;
    let mut ctx: Option<K5CtxWrap<ClientCtx>> = None;
    let hb = Duration::from_secs(TTL / 2);
    let linger = Duration::from_secs(TTL / 10);
    let now = Instant::now();
    let mut act = false;
    let mut hb = time::interval_at(now + hb, hb);
    let mut dc = time::interval_at(now + linger, linger);
    fn set_ttl(ttl: u64, hb: &mut Interval, dc: &mut Interval) {
        let linger = Duration::from_secs(max(1, ttl / 10));
        let heartbeat = Duration::from_secs(max(1, ttl / 2));
        let now = Instant::now();
        *hb = time::interval_at(now + heartbeat, heartbeat);
        *dc = time::interval_at(now + linger, linger);
    }
    'main: loop {
        select_biased! {
            _ = dc.tick().fuse() => {
                if act {
                   act = false;
                } else if con.is_some() {
                    info!("write_con dropping inactive connection");
                    con = None;
                }
            },
            _ = hb.tick().fuse() => {
                if act {
                    act = false;
                } else {
                    for _ in 0..3 {
                        match con {
                            Some(ref mut c) => {
                                match c.send_one(&ToWrite::Heartbeat).await {
                                    Ok(()) => break,
                                    Err(e) => {
                                        info!("write_con heartbeat send error {}", e);
                                        con = None;
                                    }
                                }
                            }
                            None => {
                                let r = connect_write(
                                    &resolver, resolver_addr, write_addr, &published,
                                    &secrets, &mut ctx, &desired_auth, &mut degraded
                                ).await;
                                match r {
                                    Ok((ttl, c)) => {
                                        set_ttl(ttl, &mut hb, &mut dc);
                                        con = Some(c);
                                        break
                                    }
                                    Err(e) => {
                                        ctx = None;
                                        warn!(
                                            "write connection to {:?} failed {}",
                                            resolver_addr, e
                                        );
                                        let wait = thread_rng().gen_range(1..12);
                                        time::sleep(Duration::from_secs(wait)).await;
                                    }
                                }
                            },
                        }
                    }
                }
            },
            batch = receiver.next() => match batch {
                None => break,
                Some((tx_batch, reply)) => {
                    act = true;
                    let mut tries: usize = 0;
                    'batch: loop {
                        if tries > 3 {
                            degraded = true;
                            warn!("abandoning batch, replica now degraded");
                            break 'batch;
                        }
                        if tries > 0 {
                            let wait = thread_rng().gen_range(1..12);
                            time::sleep(Duration::from_secs(wait)).await;
                        }
                        tries += 1;
                        let c = match con {
                            Some(ref mut c) => c,
                            None => {
                                let r = connect_write(
                                    &resolver, resolver_addr, write_addr, &published,
                                    &secrets, &mut ctx, &desired_auth, &mut degraded
                                ).await;
                                match r {
                                    Ok((ttl, c)) => {
                                        set_ttl(ttl, &mut hb, &mut dc);
                                        con = Some(c);
                                        con.as_mut().unwrap()
                                    }
                                    Err(e) => {
                                        ctx = None;
                                        warn!(
                                            "failed to connect to resolver {:?} {}",
                                            resolver_addr, e
                                        );
                                        continue 'batch
                                    }
                                }
                            }
                        };
                        let timeout = max(
                            HELLO_TO,
                            Duration::from_micros(tx_batch.len() as u64 * 100)
                        );
                        for (_, m) in &**tx_batch {
                            try_cf!("queue send {}", continue, 'main, c.queue_send(m))
                        }
                        match c.flush_timeout(timeout).await {
                            Err(e) => {
                                info!("write_con connection send error {}", e);
                                con = None;
                            }
                            Ok(()) => {
                                let mut rx_batch = RAWFROMWRITEPOOL.take();
                                while rx_batch.len() < tx_batch.len() {
                                    let f = c.receive_batch(&mut *rx_batch);
                                    match time::timeout(timeout, f).await {
                                        Ok(Ok(())) => (),
                                        Ok(Err(e)) => {
                                            warn!("write_con connection recv error {}", e);
                                            con = None;
                                            continue 'batch;
                                        }
                                        Err(e) => {
                                            warn!("write_con timeout, waited: {}", e);
                                            con = None;
                                            continue 'batch;
                                        }
                                    }
                                }
                                for ((_, tx), rx) in tx_batch.iter().zip(rx_batch.iter()) {
                                    match tx {
                                        ToWrite::Publish(_) => match rx {
                                            FromWrite::Published => (),
                                            _ => { degraded = true; }
                                        }
                                        _ => ()
                                    }
                                }
                                let mut result = FROMWRITEPOOL.take();
                                for (i, m) in rx_batch.drain(..).enumerate() {
                                    result.push((tx_batch[i].0, m))
                                }
                                let _ = reply.send(result);
                                break 'batch
                            }
                        }
                    }
                }
            }
        }
    }
}

type WriteBatch =
    (Pooled<Vec<(usize, ToWrite)>>, oneshot::Sender<Pooled<Vec<(usize, FromWrite)>>>);

async fn write_mgr(
    mut receiver: mpsc::UnboundedReceiver<WriteBatch>,
    resolver: Arc<Server>,
    desired_auth: DesiredAuth,
    secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    write_addr: SocketAddr,
) -> Result<()> {
    let published: Arc<RwLock<HashMap<Path, ToWrite>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let mut senders = {
        let mut senders = Vec::new();
        for addr in resolver.addrs.iter() {
            let (sender, receiver) = mpsc::channel(100);
            let addr = *addr;
            let resolver = resolver.clone();
            let published = published.clone();
            let desired_auth = desired_auth.clone();
            let secrets = secrets.clone();
            senders.push(sender);
            task::spawn(async move {
                connection_write(
                    receiver,
                    resolver,
                    addr,
                    write_addr,
                    published,
                    desired_auth,
                    secrets,
                )
                .await;
                info!("write task for {:?} exited", addr);
            });
        }
        senders
    };
    while let Some((batch, reply)) = receiver.next().await {
        let tx_batch = Arc::new(batch);
        let mut waiters = Vec::new();
        {
            let mut published = published.write();
            for (_, tx) in tx_batch.iter() {
                match tx {
                    ToWrite::Publish(p)
                    | ToWrite::PublishDefault(p)
                    | ToWrite::PublishWithFlags(p, _)
                    | ToWrite::PublishDefaultWithFlags(p, _) => {
                        published.insert(p.clone(), tx.clone());
                    }
                    ToWrite::Unpublish(_)
                    | ToWrite::UnpublishDefault(_)
                    | ToWrite::Clear
                    | ToWrite::Heartbeat => (),
                }
            }
        }
        for s in senders.iter_mut() {
            let (tx, rx) = oneshot::channel();
            let _ = s.send((Arc::clone(&tx_batch), tx)).await;
            waiters.push(rx);
        }
        match select_ok(waiters).await {
            Err(e) => warn!("write_mgr: write failed on all writers {}", e),
            Ok((rx_batch, _)) => {
                let _ = reply.send(rx_batch);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct ResolverWrite(mpsc::UnboundedSender<WriteBatch>);

impl ResolverWrite {
    pub(crate) fn new(
        resolver: Arc<Server>,
        desired_auth: DesiredAuth,
        write_addr: SocketAddr,
        secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    ) -> ResolverWrite {
        let (to_tx, to_rx) = mpsc::unbounded();
        task::spawn(async move {
            let r = write_mgr(to_rx, resolver, desired_auth, secrets, write_addr).await;
            info!("write manager exited {:?}", r);
        });
        ResolverWrite(to_tx)
    }

    pub(crate) fn send(
        &mut self,
        batch: Pooled<Vec<(usize, ToWrite)>>,
    ) -> oneshot::Receiver<Pooled<Vec<(usize, FromWrite)>>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.unbounded_send((batch, tx));
        rx
    }
}
