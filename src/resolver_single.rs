use crate::{
    channel::Channel,
    chars::Chars,
    config::{self, Config},
    os::{self, ClientCtx, Krb5Ctx},
    path::Path,
    pool::{Pool, Pooled},
    protocol::resolver::v1::{
        ClientAuthRead, ClientAuthWrite, ClientHello, ClientHelloWrite, FromRead,
        FromWrite, ReadyForOwnershipCheck, Secret, ServerAuthWrite, ServerHelloRead,
        ServerHelloWrite, ToRead, ToWrite,
    },
    utils,
};
use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use futures::{future::select_ok, prelude::*, select_biased, stream::Fuse};
use fxhash::FxBuildHasher;
use log::{debug, info, warn};
use parking_lot::RwLock;
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    fmt::Debug,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    task,
    time::{self, Instant, Interval},
};

const HELLO_TO: Duration = Duration::from_secs(15);

static TTL: u64 = 120;

lazy_static! {
    pub(crate) static ref TOREADPOOL: Pool<Vec<(usize, ToRead)>> = Pool::new(1000);
    static ref FROMREADPOOL: Pool<Vec<(usize, FromRead)>> = Pool::new(1000);
    pub(crate) static ref RAWFROMREADPOOL: Pool<Vec<FromRead>> = Pool::new(1000);
    pub(crate) static ref TOWRITEPOOL: Pool<Vec<(usize, ToWrite)>> = Pool::new(1000);
    static ref FROMWRITEPOOL: Pool<Vec<(usize, FromWrite)>> = Pool::new(1000);
    pub(crate) static ref RAWFROMWRITEPOOL: Pool<Vec<FromWrite>> = Pool::new(1000);
}

#[derive(Debug, Clone)]
pub enum Auth {
    Anonymous,
    Krb5 { upn: Option<String>, spn: Option<String> },
}

fn create_ctx(upn: Option<&str>, target_spn: &str) -> Result<(ClientCtx, Bytes)> {
    let ctx = os::create_client_ctx(upn, target_spn)?;
    match ctx.step(None)? {
        None => bail!("client ctx first step produced no token"),
        Some(tok) => Ok((ctx, utils::bytes(&*tok))),
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
    resolver: &Config,
    desired_auth: &Auth,
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
            let wait = thread_rng().gen_range(1, 12);
            time::sleep(Duration::from_secs(wait)).await;
        }
        n += 1;
        let con = cwt!("connect", TcpStream::connect(&addr));
        try_cf!("no delay", con.set_nodelay(true));
        let mut con = Channel::new(con);
        cwt!("send version", con.send_one(&1u64));
        let _ver: u64 = cwt!("recv version", con.receive());
        let (auth, ctx) = match (desired_auth, &resolver.auth) {
            (Auth::Anonymous, _) => (ClientAuthRead::Anonymous, None),
            (Auth::Krb5 { .. }, config::Auth::Anonymous) => {
                bail!("authentication unavailable")
            }
            (Auth::Krb5 { upn, .. }, config::Auth::Krb5(spns)) => {
                let upn = upn.as_ref().map(|s| s.as_str());
                let target_spn = spns
                    .get(&addr)
                    .ok_or_else(|| anyhow!("no target spn for resolver {:?}", addr))?;
                let (ctx, tok) =
                    try_cf!("create ctx", continue, create_ctx(upn, target_spn));
                (ClientAuthRead::Initiate(tok), Some(ctx))
            }
        };
        cwt!("hello", con.send_one(&ClientHello::ReadOnly(auth)));
        let r: ServerHelloRead = cwt!("hello reply", con.receive());
        if let Some(ref ctx) = ctx {
            con.set_ctx(ctx.clone()).await
        }
        match (desired_auth, r) {
            (Auth::Anonymous, ServerHelloRead::Anonymous) => (),
            (Auth::Anonymous, _) => {
                info!("server requires authentication");
                continue;
            }
            (Auth::Krb5 { .. }, ServerHelloRead::Anonymous) => {
                info!("could not authenticate resolver server");
                continue;
            }
            (Auth::Krb5 { .. }, ServerHelloRead::Reused) => (),
            (Auth::Krb5 { .. }, ServerHelloRead::Accepted(tok, _)) => {
                let ctx = ctx.ok_or_else(|| anyhow!("bug accepted but no ctx"))?;
                try_cf!("resolver tok", continue, ctx.step(Some(&tok)));
            }
        };
        break Ok(con);
    }
}

type ReadBatch =
    (Pooled<Vec<(usize, ToRead)>>, oneshot::Sender<Pooled<Vec<(usize, FromRead)>>>);

async fn connection_read(
    mut receiver: mpsc::UnboundedReceiver<ReadBatch>,
    resolver: Config,
    desired_auth: Auth,
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
                        let wait = thread_rng().gen_range(1, 12);
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
                    let timeout =
                        max(HELLO_TO, Duration::from_micros(tx_batch.len() as u64 * 6));
                    for (_, m) in &*tx_batch {
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
                        Err(_) => {
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
                                        warn!("read connection timeout, waited: {}", e);
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
    pub(crate) fn new(resolver: Config, desired_auth: Auth) -> ResolverRead {
        let (to_tx, to_rx) = mpsc::unbounded_channel();
        task::spawn(async move {
            connection_read(to_rx, resolver, desired_auth).await;
            info!("read task shutting down")
        });
        ResolverRead(to_tx)
    }

    pub(crate) fn send(
        &self,
        batch: Pooled<Vec<(usize, ToRead)>>,
    ) -> oneshot::Receiver<Pooled<Vec<(usize, FromRead)>>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.send((batch, tx));
        rx
    }
}

macro_rules! wt {
    ($e:expr) => {
        time::timeout(HELLO_TO, $e).await
    };
}

async fn connect_write(
    resolver: &Config,
    resolver_addr: SocketAddr,
    write_addr: SocketAddr,
    published: &Arc<RwLock<HashSet<Path>>>,
    secrets: &Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    security_context: &mut Option<ClientCtx>,
    desired_auth: &Auth,
    degraded: &mut bool,
) -> Result<(u64, Channel<ClientCtx>)> {
    info!("write_con connecting to resolver {:?}", resolver_addr);
    let con = wt!(TcpStream::connect(&resolver_addr))??;
    con.set_nodelay(true)?;
    let mut con = Channel::new(con);
    wt!(con.send_one(&1u64))??;
    let _version: u64 = wt!(con.receive())??;
    let sec = Duration::from_secs(1);
    let (auth, ctx) = match (desired_auth, &resolver.auth) {
        (Auth::Anonymous, _) => (ClientAuthWrite::Anonymous, None),
        (Auth::Krb5 { .. }, config::Auth::Anonymous) => {
            bail!("authentication unavailable")
        }
        (Auth::Krb5 { upn, spn }, config::Auth::Krb5(spns)) => match security_context {
            Some(ctx) if ctx.ttl().unwrap_or(sec) > sec => {
                (ClientAuthWrite::Reuse, Some(ctx.clone()))
            }
            _ => {
                let upnr = upn.as_ref().map(|s| s.as_str());
                let target_spn = spns.get(&resolver_addr).ok_or_else(|| {
                    anyhow!("no target spn for resolver {:?}", resolver_addr)
                })?;
                let (ctx, token) = create_ctx(upnr, target_spn)?;
                let spn = spn.as_ref().or(upn.as_ref()).cloned().map(Chars::from);
                (ClientAuthWrite::Initiate { spn, token }, Some(ctx))
            }
        },
    };
    let h = ClientHello::WriteOnly(ClientHelloWrite { write_addr, auth });
    debug!("write_con connection established hello {:?}", h);
    wt!(con.send_one(&h))??;
    let r: ServerHelloWrite = wt!(con.receive())??;
    debug!("write_con resolver hello {:?}", r);
    match (desired_auth, r.auth) {
        (Auth::Anonymous, ServerAuthWrite::Anonymous) => {
            *security_context = None;
        }
        (Auth::Anonymous, _) => {
            bail!("server requires authentication");
        }
        (Auth::Krb5 { .. }, ServerAuthWrite::Anonymous) => {
            bail!("could not authenticate resolver server");
        }
        (Auth::Krb5 { .. }, ServerAuthWrite::Reused) => {
            let ctx = ctx.ok_or_else(|| anyhow!("bug, reused but no ctx"))?;
            con.set_ctx(ctx.clone()).await;
            info!("write_con all traffic now encrypted");
        }
        (Auth::Krb5 { .. }, ServerAuthWrite::Accepted(tok)) => {
            let ctx = ctx.ok_or_else(|| anyhow!("bug, accepted but no ctx"))?;
            info!("write_con processing resolver mutual authentication");
            ctx.step(Some(&tok))?;
            info!("write_con mutual authentication succeeded");
            con.set_ctx(ctx.clone()).await;
            info!("write_con all traffic now encrypted");
            *security_context = Some(ctx.clone());
            let secret: Secret = wt!(con.receive())??;
            {
                let mut secrets = secrets.write();
                secrets.insert(resolver_addr, secret.0);
                secrets.insert(r.resolver_id, secret.0);
            }
            wt!(con.send_one(&ReadyForOwnershipCheck))??;
        }
    }
    if !r.ttl_expired && !*degraded {
        info!("connected to resolver {:?} for write", resolver_addr);
        Ok((r.ttl, con))
    } else {
        let names: Vec<Path> = published.read().iter().cloned().collect();
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
            for p in &names {
                con.queue_send(&ToWrite::Publish(p.clone()))?
            }
            con.flush().await?;
            let mut success = 0;
            for p in &names {
                match try_cf!("replublish reply", continue, con.receive().await) {
                    FromWrite::Published => {
                        success += 1;
                    }
                    r => warn!("unexpected republish reply for {} {:?}", p, r),
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
    resolver: Config,
    resolver_addr: SocketAddr,
    write_addr: SocketAddr,
    published: Arc<RwLock<HashSet<Path>>>,
    desired_auth: Auth,
    secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
) {
    let mut degraded = false;
    let mut con: Option<Channel<ClientCtx>> = None;
    let mut ctx: Option<ClientCtx> = None;
    let hb = Duration::from_secs(TTL / 2);
    let linger = Duration::from_secs(TTL / 10);
    let now = Instant::now();
    let mut act = false;
    let mut receiver = receiver.fuse();
    let mut hb = time::interval_at(now + hb, hb).fuse();
    let mut dc = time::interval_at(now + linger, linger).fuse();
    fn set_ttl(ttl: u64, hb: &mut Fuse<Interval>, dc: &mut Fuse<Interval>) {
        let linger = Duration::from_secs(max(1, ttl / 10));
        let heartbeat = Duration::from_secs(max(1, ttl / 2));
        let now = Instant::now();
        *hb = time::interval_at(now + heartbeat, heartbeat).fuse();
        *dc = time::interval_at(now + linger, linger).fuse();
    }
    'main: loop {
        select_biased! {
            _ = dc.next() => {
                if act {
                   act = false;
                } else if con.is_some() {
                    info!("write_con dropping inactive connection");
                    con = None;
                }
            },
            _ = hb.next() => {
                println!("{}: {} ({})", write_addr, degraded, published.read().len());
                if act {
                    act = false;
                } else {
                    for i in 0..3 {
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
                                        let wait = thread_rng().gen_range(1, 12);
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
                            let wait = thread_rng().gen_range(1, 12);
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
                            Duration::from_micros(tx_batch.len() as u64 * 12)
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
                                let mut err = false;
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
                                if degraded {
                                    warn!("replica now degraded");
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
    resolver: Config,
    desired_auth: Auth,
    secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    write_addr: SocketAddr,
) -> Result<()> {
    let published: Arc<RwLock<HashSet<Path>>> = Arc::new(RwLock::new(HashSet::new()));
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
                if let ToWrite::Publish(path) = tx {
                    published.insert(path.clone());
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
        resolver: Config,
        desired_auth: Auth,
        write_addr: SocketAddr,
        secrets: Arc<RwLock<HashMap<SocketAddr, u128, FxBuildHasher>>>,
    ) -> ResolverWrite {
        let (to_tx, to_rx) = mpsc::unbounded_channel();
        task::spawn(async move {
            let r = write_mgr(to_rx, resolver, desired_auth, secrets, write_addr).await;
            info!("write manager exited {:?}", r);
        });
        ResolverWrite(to_tx)
    }

    pub(crate) fn send(
        &self,
        batch: Pooled<Vec<(usize, ToWrite)>>,
    ) -> oneshot::Receiver<Pooled<Vec<(usize, FromWrite)>>> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.send((batch, tx));
        rx
    }
}
