use super::common::{
    krb5_authentication, DesiredAuth, Response, ResponseChan, FROMWRITEPOOL, HELLO_TO,
    PUBLISHERPOOL, RAWFROMWRITEPOOL,
};

use crate::{
    channel::{self, Channel, K5CtxWrap},
    chars::Chars,
    os::local_auth::AuthClient,
    path::Path,
    pool::Pooled,
    protocol::resolver::{
        Auth, AuthChallenge, AuthWrite, ClientHello, ClientHelloWrite, FromWrite,
        HashMethod, ReadyForOwnershipCheck, Referral, Secret, ServerHelloWrite, ToWrite,
    },
    tls, utils,
};
use anyhow::{anyhow, Result};
use cross_krb5::{ClientCtx, K5Ctx};
use futures::{
    channel::{mpsc, oneshot},
    future::select_ok,
    prelude::*,
    select_biased,
};
use fxhash::FxHashMap;
use log::{debug, info, warn};
use parking_lot::RwLock;
use rand::{thread_rng, Rng};
use std::{
    cmp::max, collections::HashMap, fmt::Debug, net::SocketAddr, sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpStream,
    task,
    time::{self, Instant, Interval},
};

const TTL: u64 = 120;

type Batch = (Pooled<Vec<(usize, ToWrite)>>, oneshot::Sender<Response<FromWrite>>);
type ArcBatch =
    (Arc<Pooled<Vec<(usize, ToWrite)>>>, oneshot::Sender<Response<FromWrite>>);

macro_rules! wt {
    ($e:expr) => {
        time::timeout(HELLO_TO, $e).await
    };
}

const HB: Duration = Duration::from_secs(TTL / 2);
const LINGER: Duration = Duration::from_secs(TTL / 10);

struct Connection {
    con: Option<Channel>,
    resolver_addr: SocketAddr,
    resolver_auth: Auth,
    write_addr: SocketAddr,
    published: Arc<RwLock<HashMap<Path, ToWrite>>>,
    secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
    security_context: Option<K5CtxWrap<ClientCtx>>,
    tls: Option<tls::CachedConnector>,
    desired_auth: DesiredAuth,
    degraded: bool,
    active: bool,
    heartbeat: Interval,
    disconnect: Interval,
}

impl Connection {
    fn set_ttl(&mut self, ttl: u64) {
        let linger = Duration::from_secs(max(1, ttl / 10));
        let heartbeat = Duration::from_secs(max(1, ttl / 2));
        let now = Instant::now();
        self.heartbeat = time::interval_at(now + heartbeat, heartbeat);
        self.disconnect = time::interval_at(now + linger, linger);
    }

    async fn republish(&mut self, con: &mut Channel, ttl_expired: bool) -> Result<()> {
        let names = self.published.read().values().cloned().collect::<Vec<ToWrite>>();
        let len = names.len();
        if len == 0 {
            info!("connected to resolver {:?} for write", self.resolver_addr);
            if self.degraded {
                con.send_one(&ToWrite::Clear).await?;
                match con.receive().await? {
                    FromWrite::Unpublished => {
                        self.degraded = false;
                    }
                    m => warn!("unexpected response to clear {:?}", m),
                }
            }
        } else {
            info!(
                "write_con ttl: {} degraded: {}, republishing: {}",
                len, ttl_expired, self.degraded
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
            self.degraded = success != len;
            info!(
                "connected to resolver {:?} for write (republished {})",
                self.resolver_addr,
                names.len()
            );
        }
        Ok(())
    }

    async fn connect(&mut self) -> Result<()> {
        async fn auth_challenge(con: &mut Channel, secret: u128) -> Result<()> {
            let c: AuthChallenge = con.receive().await?;
            if c.hash_method != HashMethod::Sha3_512 {
                bail!("hash method not supported")
            }
            let answer = utils::make_sha3_token(&[
                &c.challenge.to_be_bytes(),
                &secret.to_be_bytes(),
            ]);
            Ok(con.send_one(&answer).await?)
        }
        info!("write_con connecting to resolver {:?}", self.resolver_addr);
        let mut con = wt!(TcpStream::connect(&self.resolver_addr))??;
        debug!("setting no delay = true");
        con.set_nodelay(true)?;
        debug!("writing protocol version 2");
        wt!(channel::write_raw(&mut con, &2u64))??;
        debug!("reading protocol version");
        if wt!(channel::read_raw::<u64, _>(&mut con))?? != 2 {
            bail!("incompatible protocol version")
        }
        let sec = Duration::from_secs(1);
        let hello = |auth| {
            let h = ClientHello::WriteOnly(ClientHelloWrite {
                write_addr: self.write_addr,
                auth,
            });
            debug!("write_con connection established hello {:?}", h);
            h
        };
        let (mut con, r, ownership_check) =
            match (&self.desired_auth, &self.resolver_auth) {
                (DesiredAuth::Anonymous, _) => {
                    debug!("sending anymous auth hello");
                    wt!(channel::write_raw(&mut con, &hello(AuthWrite::Anonymous)))??;
                    let r = wt!(channel::read_raw::<ServerHelloWrite, _>(&mut con))??;
                    (Channel::new::<ClientCtx, TcpStream>(None, con), r, false)
                }
                (
                    DesiredAuth::Krb5 { .. }
                    | DesiredAuth::Tls { .. }
                    | DesiredAuth::Local,
                    Auth::Anonymous,
                ) => {
                    bail!("authentication not supported")
                }
                (
                    DesiredAuth::Local
                    | DesiredAuth::Krb5 { .. }
                    | DesiredAuth::Tls { .. },
                    Auth::Local { path },
                ) => {
                    debug!("local authentication selected");
                    let secret = self.secrets.read().get(&self.resolver_addr).map(|u| *u);
                    let mut con = Channel::new::<ClientCtx, TcpStream>(None, con);
                    match secret {
                        Some(secret) => {
                            debug!("reusing existing session");
                            wt!(con.send_one(&hello(AuthWrite::Reuse)))??;
                            wt!(auth_challenge(&mut con, secret))??;
                            let r = wt!(con.receive::<ServerHelloWrite>())??;
                            (con, r, false)
                        }
                        None => {
                            debug!("starting a new local auth session");
                            let tok = wt!(AuthClient::token(&*path))??;
                            wt!(con.send_one(&hello(AuthWrite::Local)))??;
                            wt!(con.send_one(&tok))??;
                            let r = wt!(con.receive::<ServerHelloWrite>())??;
                            (con, r, true)
                        }
                    }
                }
                (DesiredAuth::Local, Auth::Krb5 { .. } | Auth::Tls { .. }) => {
                    bail!("local auth not supported")
                }
                (DesiredAuth::Krb5 { .. }, Auth::Tls { .. }) => {
                    bail!("krb5 auth is not supported")
                }
                (DesiredAuth::Krb5 { upn, spn }, Auth::Krb5 { spn: target_spn }) => {
                    debug!("krb5 auth selected");
                    let secret = self.secrets.read().get(&self.resolver_addr).map(|u| *u);
                    match (&self.security_context, secret) {
                        (Some(ctx), Some(secret))
                            if task::block_in_place(|| ctx.lock().ttl())
                                .unwrap_or(sec)
                                > sec =>
                        {
                            debug!("reusing existing session");
                            wt!(channel::write_raw(&mut con, &hello(AuthWrite::Reuse)))??;
                            let mut con = Channel::new(Some(ctx.clone()), con);
                            wt!(auth_challenge(&mut con, secret))??;
                            let r: ServerHelloWrite = wt!(con.receive())??;
                            (con, r, false)
                        }
                        (None | Some(_), _) => {
                            debug!("starting a new krb5 session");
                            let upn = upn.as_ref().map(|s| s.as_str());
                            let spn =
                                Chars::from(spn.clone().ok_or_else(|| {
                                    anyhow!("spn is required for writers")
                                })?);
                            wt!(channel::write_raw(
                                &mut con,
                                &hello(AuthWrite::Krb5 { spn })
                            ))??;
                            let ctx =
                                krb5_authentication(upn, &*target_spn, &mut con).await?;
                            let ctx = K5CtxWrap::new(ctx);
                            let mut con = Channel::new(Some(ctx.clone()), con);
                            let r: ServerHelloWrite = wt!(con.receive())??;
                            self.security_context = Some(ctx);
                            (con, r, true)
                        }
                    }
                }
                (DesiredAuth::Tls { .. }, Auth::Krb5 { .. }) => {
                    bail!("tls auth not supported")
                }
                (DesiredAuth::Tls { name: pname }, Auth::Tls { name }) => {
                    debug!("tls auth selected");
                    let tls = self.tls.as_ref().ok_or_else(|| anyhow!("no tls ctx"))?;
                    let ctx = task::block_in_place(|| tls.load(name))?;
                    let secret = self.secrets.read().get(&self.resolver_addr).map(|u| *u);
                    let name = rustls::ServerName::try_from(&**name)?;
                    match secret {
                        Some(secret) => {
                            debug!("reusing existing tls session");
                            wt!(channel::write_raw(&mut con, &hello(AuthWrite::Reuse)))??;
                            let tls = ctx.connect(name, con).await?;
                            let mut con = Channel::new::<
                                ClientCtx,
                                tokio_rustls::client::TlsStream<TcpStream>,
                            >(None, tls);
                            wt!(auth_challenge(&mut con, secret))??;
                            let r: ServerHelloWrite = wt!(con.receive())??;
                            (con, r, false)
                        }
                        None => {
                            let publisher_name = Chars::from(match pname {
                                Some(pname) => pname.clone(),
                                None => tls.default_identity().name.clone(),
                            });
                            debug!("starting a new tls session for {}", publisher_name);
                            let h = hello(AuthWrite::Tls { name: publisher_name });
                            wt!(channel::write_raw(&mut con, &h))??;
                            let tls = ctx.connect(name, con).await?;
                            let mut con = Channel::new::<
                                ClientCtx,
                                tokio_rustls::client::TlsStream<TcpStream>,
                            >(None, tls);
                            let r: ServerHelloWrite = wt!(con.receive())??;
                            (con, r, true)
                        }
                    }
                }
            };
        debug!("write_con resolver hello {:?}", r);
        if ownership_check {
            let secret: Secret = wt!(con.receive())??;
            {
                let mut secrets = self.secrets.write();
                secrets.insert(self.resolver_addr, secret.0);
                secrets.insert(r.resolver_id, secret.0);
            }
            wt!(con.send_one(&ReadyForOwnershipCheck))??;
        }
        if !r.ttl_expired && !self.degraded {
            info!("connected to resolver {:?} for write", self.resolver_addr);
            self.con = Some(con);
            Ok(self.set_ttl(r.ttl))
        } else {
            self.republish(&mut con, r.ttl_expired).await?;
            self.con = Some(con);
            Ok(self.set_ttl(r.ttl))
        }
    }

    fn handle_failed_connect(&mut self, e: anyhow::Error) {
        self.security_context = None;
        self.secrets.write().remove(&self.resolver_addr);
        warn!("write connection {:?} failed {}", self.resolver_addr, e);
    }

    async fn send_heartbeat(&mut self) {
        for _ in 0..3 {
            match self.con {
                Some(ref mut c) => match c.send_one(&ToWrite::Heartbeat).await {
                    Ok(()) => break,
                    Err(e) => {
                        info!("write_con heartbeat send error {}", e);
                        self.con = None;
                    }
                },
                None => match self.connect().await {
                    Ok(()) => break,
                    Err(e) => {
                        self.handle_failed_connect(e);
                        let wait = thread_rng().gen_range(1..12);
                        time::sleep(Duration::from_secs(wait)).await;
                    }
                },
            }
        }
    }

    async fn process_batch(&mut self, (tx_batch, reply): ArcBatch) -> Result<()> {
        self.active = true;
        let mut tries: usize = 0;
        'batch: loop {
            if tries > 3 {
                self.degraded = true;
                bail!("abandoning batch");
            }
            if tries > 0 {
                let wait = thread_rng().gen_range(1..12);
                time::sleep(Duration::from_secs(wait)).await;
            }
            tries += 1;
            let c = match self.con {
                Some(ref mut c) => c,
                None => match self.connect().await {
                    Ok(()) => self.con.as_mut().unwrap(),
                    Err(e) => {
                        self.handle_failed_connect(e);
                        continue 'batch;
                    }
                },
            };
            let timeout =
                max(HELLO_TO, Duration::from_micros(tx_batch.len() as u64 * 100));
            for (_, m) in &**tx_batch {
                c.queue_send(m)?;
            }
            match c.flush_timeout(timeout).await {
                Err(e) => {
                    info!("write_con connection send error {}", e);
                    self.con = None;
                }
                Ok(()) => {
                    let mut rx_batch = RAWFROMWRITEPOOL.take();
                    while rx_batch.len() < tx_batch.len() {
                        let f = c.receive_batch(&mut *rx_batch);
                        match time::timeout(timeout, f).await {
                            Ok(Ok(())) => (),
                            Ok(Err(e)) => {
                                warn!("write_con connection recv error {}", e);
                                self.con = None;
                                continue 'batch;
                            }
                            Err(e) => {
                                warn!("write_con timeout, waited: {}", e);
                                self.con = None;
                                continue 'batch;
                            }
                        }
                    }
                    for ((_, tx), rx) in tx_batch.iter().zip(rx_batch.iter()) {
                        match tx {
                            ToWrite::Publish(_) => match rx {
                                FromWrite::Published => (),
                                _ => {
                                    self.degraded = true;
                                }
                            },
                            _ => (),
                        }
                    }
                    let mut result = FROMWRITEPOOL.take();
                    // not relevant for writes
                    let publishers = PUBLISHERPOOL.take();
                    for (i, m) in rx_batch.drain(..).enumerate() {
                        result.push((tx_batch[i].0, m))
                    }
                    let _ = reply.send((publishers, result));
                    break 'batch;
                }
            }
        }
        Ok(())
    }

    async fn start(
        receiver: mpsc::Receiver<ArcBatch>,
        resolver_addr: SocketAddr,
        resolver_auth: Auth,
        write_addr: SocketAddr,
        published: Arc<RwLock<HashMap<Path, ToWrite>>>,
        desired_auth: DesiredAuth,
        secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
        tls: Option<tls::CachedConnector>,
    ) {
        let now = Instant::now();
        let mut t = Self {
            resolver_addr,
            resolver_auth,
            write_addr,
            published,
            secrets,
            desired_auth,
            security_context: None,
            tls,
            con: None,
            degraded: false,
            active: false,
            heartbeat: time::interval_at(now + HB, HB),
            disconnect: time::interval_at(now + LINGER, LINGER),
        };
        let mut receiver = receiver.fuse();
        loop {
            select_biased! {
                _ = t.disconnect.tick().fuse() => {
                    if t.active {
                        t.active = false;
                    } else if t.con.is_some() {
                        info!("write_con dropping inactive connection");
                        t.con = None;
                    }
                },
                _ = t.heartbeat.tick().fuse() => {
                    if t.active {
                        t.active = false;
                    } else {
                        t.send_heartbeat().await;
                    }
                },
                batch = receiver.next() => match batch {
                    None => break,
                    Some(batch) => match t.process_batch(batch).await {
                        Err(e) => warn!("write batch failed {}", e),
                        Ok(()) => ()
                    }
                }
            }
        }
    }
}

async fn write_mgr(
    mut receiver: mpsc::UnboundedReceiver<Batch>,
    resolver: Arc<Referral>,
    desired_auth: DesiredAuth,
    secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
    write_addr: SocketAddr,
    tls: Option<tls::CachedConnector>,
) -> Result<()> {
    let published: Arc<RwLock<HashMap<Path, ToWrite>>> =
        Arc::new(RwLock::new(HashMap::new()));
    let mut senders = {
        let mut senders = Vec::new();
        for (addr, auth) in resolver.addrs.iter() {
            let (sender, receiver) = mpsc::channel(100);
            let addr = *addr;
            let auth = auth.clone();
            let published = published.clone();
            let desired_auth = desired_auth.clone();
            let secrets = secrets.clone();
            let tls = tls.clone();
            senders.push(sender);
            task::spawn(async move {
                Connection::start(
                    receiver,
                    addr,
                    auth,
                    write_addr,
                    published,
                    desired_auth,
                    secrets,
                    tls,
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
pub(crate) struct WriteClient(mpsc::UnboundedSender<Batch>);

impl WriteClient {
    pub(crate) fn new(
        resolver: Arc<Referral>,
        desired_auth: DesiredAuth,
        write_addr: SocketAddr,
        secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
        tls: Option<tls::CachedConnector>,
    ) -> Self {
        let (to_tx, to_rx) = mpsc::unbounded();
        task::spawn(async move {
            let r =
                write_mgr(to_rx, resolver, desired_auth, secrets, write_addr, tls).await;
            info!("write manager exited {:?}", r);
        });
        Self(to_tx)
    }

    pub(crate) fn send(
        &mut self,
        batch: Pooled<Vec<(usize, ToWrite)>>,
    ) -> ResponseChan<FromWrite> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.unbounded_send((batch, tx));
        rx
    }
}
