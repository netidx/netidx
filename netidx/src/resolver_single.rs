use crate::{
    channel::Channel,
    channel::K5CtxWrap,
    chars::Chars,
    config::{Server, ServerAuth},
    path::Path,
    pool::{Pool, Pooled},
    protocol::resolver::{
        ClientAuthRead, ClientAuthWrite, ClientHello, ClientHelloWrite, FromRead,
        FromWrite, ReadyForOwnershipCheck, Secret, ServerAuthWrite, ServerHelloRead,
        ServerHelloWrite, ToRead, ToWrite,
    },
    utils,
};
use anyhow::{anyhow, Error, Result};
use cross_krb5::{ClientCtx, InitiateFlags, K5Ctx, PendingClientCtx};
use futures::{
    channel::{mpsc, oneshot},
    future::select_ok,
    prelude::*,
    select_biased,
};
use fxhash::FxBuildHasher;
use log::{debug, error, info, warn};
use parking_lot::RwLock;
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::{
    cmp::max, collections::HashMap, fmt::Debug, net::SocketAddr, sync::Arc,
    time::Duration,
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
        cwt!("send version", con.send_one(&1u64));
        let _ver: u64 = cwt!("recv version", con.receive());
        let (auth, ctx) = match desired_auth {
            DesiredAuth::Anonymous => (ClientAuthRead::Anonymous, None),
            DesiredAuth::Local => unimplemented!(),
            DesiredAuth::Krb5 { upn, .. } => match &resolver.auth {
                ServerAuth::Anonymous | ServerAuth::Local(_) => {
                    bail!("requested auth mechanism is not available")
                }
                ServerAuth::Krb5(spns) => {
                    let upn = upn.as_ref().map(|s| s.as_str());
                    let target_spn = spns.get(&addr).ok_or_else(|| {
                        anyhow!("no target spn for resolver {:?}", addr)
                    })?;
                    let (ctx, tok) = try_cf!(
                        "create ctx",
                        continue,
                        task::block_in_place(|| ClientCtx::initiate(
                            InitiateFlags::empty(),
                            upn,
                            target_spn
                        ))
                    );
                    (ClientAuthRead::Initiate(utils::bytes(&*tok)), Some(ctx))
                }
            },
        };
        cwt!("hello", con.send_one(&ClientHello::ReadOnly(auth)));
        let r: ServerHelloRead = cwt!("hello reply", con.receive());
        match (desired_auth, r) {
            (DesiredAuth::Anonymous, ServerHelloRead::Anonymous) => (),
            (DesiredAuth::Local, _) => unimplemented!(),
            (DesiredAuth::Anonymous, _) => {
                error!("server requires authentication");
                continue;
            }
            (DesiredAuth::Krb5 { .. }, ServerHelloRead::Anonymous) => {
                error!("could not authenticate resolver server");
                continue;
            }
            (DesiredAuth::Krb5 { .. }, ServerHelloRead::Reused) => {
                error!("protocol error, we didn't ask to reuse a security context");
                continue;
            }
            (DesiredAuth::Krb5 { .. }, ServerHelloRead::Accepted(tok, _)) => {
                let ctx = ctx.ok_or_else(|| anyhow!("bug accepted but no ctx"))?;
                let ctx = try_cf!(
                    "resolver tok",
                    continue,
                    task::block_in_place(|| ctx.finish(&tok))
                );
                con.set_ctx(K5CtxWrap::new(ctx)).await
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
    enum SecState {
        Anonymous,
        Reused(K5CtxWrap<ClientCtx>),
        Pending(PendingClientCtx),
    }
    info!("write_con connecting to resolver {:?}", resolver_addr);
    let con = wt!(TcpStream::connect(&resolver_addr))??;
    con.set_nodelay(true)?;
    let mut con = Channel::new(con);
    wt!(con.send_one(&1u64))??;
    let _version: u64 = wt!(con.receive())??;
    let sec = Duration::from_secs(1);
    let (auth, ctx) = match desired_auth {
        DesiredAuth::Anonymous => (ClientAuthWrite::Anonymous, SecState::Anonymous),
        DesiredAuth::Local => unimplemented!(),
        DesiredAuth::Krb5 { upn, spn } => match &resolver.auth {
            ServerAuth::Anonymous | ServerAuth::Local(_) => {
                bail!("authentication unavailable")
            }
            ServerAuth::Krb5(spns) => match security_context {
                Some(ctx)
                    if task::block_in_place(|| ctx.lock().ttl()).unwrap_or(sec) > sec =>
                {
                    (ClientAuthWrite::Reuse, SecState::Reused(ctx.clone()))
                }
                _ => {
                    let upnr = upn.as_ref().map(|s| s.as_str());
                    let target_spn = spns.get(&resolver_addr).ok_or_else(|| {
                        anyhow!("no target spn for resolver {:?}", resolver_addr)
                    })?;
                    let (ctx, token) = task::block_in_place(|| {
                        ClientCtx::initiate(InitiateFlags::empty(), upnr, target_spn)
                    })?;
                    let token = utils::bytes(&*token);
                    let spn = spn.as_ref().or(upn.as_ref()).cloned().map(Chars::from);
                    (ClientAuthWrite::Initiate { spn, token }, SecState::Pending(ctx))
                }
            },
        },
    };
    let h = ClientHello::WriteOnly(ClientHelloWrite { write_addr, auth });
    debug!("write_con connection established hello {:?}", h);
    wt!(con.send_one(&h))??;
    let r: ServerHelloWrite = wt!(con.receive())??;
    debug!("write_con resolver hello {:?}", r);
    match (desired_auth, r.auth, ctx) {
        (DesiredAuth::Anonymous, ServerAuthWrite::Anonymous, SecState::Anonymous) => {
            *security_context = None;
        }
        (DesiredAuth::Anonymous, _, _) => {
            bail!("server requires authentication");
        }
        (DesiredAuth::Local, _, _) => unimplemented!(),
        (DesiredAuth::Krb5 { .. }, ServerAuthWrite::Anonymous, _) => {
            bail!("could not authenticate resolver server");
        }
        (DesiredAuth::Krb5 { .. }, ServerAuthWrite::Reused, SecState::Reused(ctx)) => {
            con.set_ctx(ctx).await;
            info!("write_con all traffic now encrypted");
        }
        (DesiredAuth::Krb5 { .. }, ServerAuthWrite::Reused, _) => {
            bail!("missing security context to reuse")
        }
        (DesiredAuth::Krb5 { .. }, ServerAuthWrite::Accepted(tok), SecState::Pending(ctx)) => {
            info!("write_con processing resolver mutual authentication");
            let ctx = K5CtxWrap::new(task::block_in_place(|| ctx.finish(&tok))?);
            info!("write_con mutual authentication succeeded");
            con.set_ctx(ctx.clone()).await;
            info!("write_con all traffic now encrypted");
            *security_context = Some(ctx);
            let secret: Secret = wt!(con.receive())??;
            {
                let mut secrets = secrets.write();
                secrets.insert(resolver_addr, secret.0);
                secrets.insert(r.resolver_id, secret.0);
            }
            wt!(con.send_one(&ReadyForOwnershipCheck))??;
        }
        (DesiredAuth::Krb5 { .. }, ServerAuthWrite::Accepted(_), _) => {
            bail!("missing pending security context")
        }
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
