use super::common::{
    DesiredAuth, FROMWRITEPOOL, HELLO_TO, PUBLISHERPOOL, RAWFROMWRITEPOOL, Response,
    ResponseChan, addrs_changed, krb5_authentication,
};
use crate::{
    channel::{self, Channel, K5CtxWrap},
    os::local_auth::AuthClient,
    path::Path,
    protocol::resolver::{
        Auth, AuthChallenge, AuthWrite, ClientHello, ClientHelloWrite, FromWrite,
        HashMethod, ReadyForOwnershipCheck, Referral, Secret, ServerHelloWrite, ToWrite,
    },
    publisher::{PublishError, PublishErrors},
    tls, utils,
};
use ahash::{AHashMap, AHasher};
use anyhow::{Result, anyhow};
use arcstr::ArcStr;
use cross_krb5::{ClientCtx, K5Ctx};
use futures::{
    channel::{mpsc, oneshot},
    future::select_ok,
    prelude::*,
    select_biased,
};
use indexmap::IndexMap;
use log::{debug, info, warn};
use netidx_netproto::resolver::PublisherPriority;
use parking_lot::{Mutex, RwLock};
use poolshark::{
    global::{GPooled, Pool},
    local::LPooled,
};
use rand::{RngExt, rng};
use std::{
    cmp::max,
    fmt::Debug,
    hash::BuildHasherDefault,
    net::SocketAddr,
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{
        broadcast::{self, error::RecvError},
        watch,
    },
    task,
    time::{self, Instant, Interval},
};

const TTL: u64 = 120;

static WRITE_EVENTS: LazyLock<Pool<Vec<WriteEvent>>> =
    LazyLock::new(|| Pool::new(64, 10_000));
static OUTCOMES: LazyLock<Pool<Vec<(Path, Option<PublishError>)>>> =
    LazyLock::new(|| Pool::new(64, 10_000));

type Batch = (GPooled<Vec<(usize, ToWrite)>>, oneshot::Sender<Response<FromWrite>>);

struct ToCon {
    batch: GPooled<Vec<(usize, ToWrite)>>,
    replies: Mutex<Vec<oneshot::Sender<Response<FromWrite>>>>,
}

/// A change in what the cluster is doing with a path, or with everything this
/// publisher has published.
///
/// `path` is `None` for a condition that isn't tied to one path — a member we
/// can't reach, a heartbeat reconnect refused — which is exactly the claim
/// that every published path gained that error, made without emitting a
/// million identical items.
#[derive(Debug, Clone)]
pub(crate) struct WriteEvent {
    pub(crate) path: Option<Path>,
    pub(crate) errors: PublishErrors,
}

/// What one member of the cluster is doing, reported up to `write_mgr`, which
/// is the only place that knows the whole member set and can therefore say
/// whether anyone is still accepting.
enum ConEvent {
    /// this member answered for these paths; `None` means it accepted
    Outcome(SocketAddr, GPooled<Vec<(Path, Option<PublishError>)>>),
    /// this member is not usable
    Down(SocketAddr, PublishError),
    /// this member is connected and taking writes
    Up(SocketAddr),
}

/// Classify what a resolver said about a path it would not publish.
///
/// `FromWrite::Error` is free form text and only one value is recognised, so
/// anything else collapses to `ResolverError` — warn the text first, or the
/// only record of what actually happened is gone.
fn refusal(addr: SocketAddr, path: &Path, reply: &FromWrite) -> Option<PublishError> {
    match reply {
        FromWrite::Published | FromWrite::Referral(_) => None,
        FromWrite::Denied => Some(PublishError::Denied),
        FromWrite::Error(e) if &**e == "absolute paths required" => {
            Some(PublishError::InvalidPath)
        }
        FromWrite::Error(e) => {
            warn!("resolver {addr:?} refused {path}: {e}");
            Some(PublishError::ResolverError)
        }
        FromWrite::Unpublished => {
            warn!("resolver {addr:?} answered publish {path} with unpublished");
            Some(PublishError::ResolverError)
        }
    }
}

macro_rules! wt {
    ($loc:expr, $e:expr) => {{
        let r = time::timeout(HELLO_TO, $e).await;
        match r {
            Err(_) => {
                warn!("timeout while attempting {}", $loc);
            }
            Ok(_) => (),
        }
        r
    }};
}

const HB: Duration = Duration::from_secs(TTL / 2);
const LINGER: Duration = Duration::from_secs(TTL / 10);

/// What every member of this referral's cluster should be holding for us.
///
/// The publisher is the only authority on this, and a resolver keeps a
/// publisher's records only while the publisher keeps talking to it, so any
/// connection may have to rebuild a member's whole view from scratch.
/// `write_mgr` sees every batch before it broadcasts, so it maintains this and
/// the connections only read it — one copy for the cluster rather than one per
/// member.
type Published = Arc<RwLock<IndexMap<Path, ToWrite, BuildHasherDefault<AHasher>>>>;

struct Connection {
    con: Option<Channel>,
    resolver_addr: SocketAddr,
    resolver_auth: Auth,
    write_addr: SocketAddr,
    published: Published,
    /// A `Clear` that failed to reach *this* member, to retry on reconnect.
    pending_clear: bool,
    /// Unpublishes that failed to reach *this* member. They are already gone
    /// from `published`, so nothing else would ever remove them there.
    pending_unpublish: AHashMap<Path, ToWrite>,
    secrets: Arc<RwLock<AHashMap<SocketAddr, u128>>>,
    security_context: Option<K5CtxWrap<ClientCtx>>,
    tls: Option<tls::CachedConnector>,
    desired_auth: DesiredAuth,
    degraded: bool,
    active: bool,
    heartbeat: Interval,
    disconnect: Interval,
    priority: PublisherPriority,
    events: mpsc::UnboundedSender<ConEvent>,
    /// what this member has refused, so that only changes are reported. Empty
    /// in the healthy case, which is why reporting costs nothing there.
    refused: AHashMap<Path, PublishError>,
    /// what we last told `write_mgr` about this member. `None` is usable,
    /// which is also what it assumes about a member it has just started.
    reported_down: Option<PublishError>,
}

impl Connection {
    /// Note what this member said about a path, and report it if it differs
    /// from what we last said about it.
    ///
    /// Returns true if the member is now out of step with what the publisher
    /// believes. A definitive refusal is not: the member heard us perfectly
    /// well and said no, and replaying the publish set at it forever would
    /// change nothing. `degraded` means it may not have heard us.
    fn note(
        &mut self,
        outcomes: &mut GPooled<Vec<(Path, Option<PublishError>)>>,
        path: &Path,
        reply: &FromWrite,
    ) -> bool {
        match refusal(self.resolver_addr, path, reply) {
            Some(e) => {
                if self.refused.insert(path.clone(), e) != Some(e) {
                    outcomes.push((path.clone(), Some(e)))
                }
                !matches!(e, PublishError::Denied | PublishError::InvalidPath)
            }
            None => {
                if self.refused.remove(path).is_some() {
                    outcomes.push((path.clone(), None))
                }
                false
            }
        }
    }

    fn report(&self, outcomes: GPooled<Vec<(Path, Option<PublishError>)>>) {
        if !outcomes.is_empty() {
            let _ = self
                .events
                .unbounded_send(ConEvent::Outcome(self.resolver_addr, outcomes));
        }
    }

    fn report_up(&mut self) {
        if self.reported_down.take().is_some() {
            let _ = self.events.unbounded_send(ConEvent::Up(self.resolver_addr));
        }
    }

    fn report_down(&mut self, reason: PublishError) {
        if self.reported_down != Some(reason) {
            self.reported_down = Some(reason);
            let _ =
                self.events.unbounded_send(ConEvent::Down(self.resolver_addr, reason));
        }
    }

    fn set_ttl(&mut self, ttl: u64) {
        let linger = Duration::from_secs(max(1, ttl / 10));
        let heartbeat = Duration::from_secs(max(1, ttl / 2));
        let now = Instant::now();
        self.heartbeat = time::interval_at(now + heartbeat, heartbeat);
        self.disconnect = time::interval_at(now + linger, linger);
    }

    /// Bring this member back in line with what the publisher believes.
    ///
    /// Retries first — a pending `Clear` has to precede the republish it would
    /// otherwise wipe out — then the desired state. An unpublish replayed
    /// against a member that never had the path is a no-op, so this is safe
    /// whether the member kept our records or lost them.
    async fn republish(&mut self, con: &mut Channel, ttl_expired: bool) -> Result<()> {
        let mut nretry = 0;
        if self.pending_clear {
            con.queue_send(&ToWrite::Clear)?;
            nretry += 1;
        }
        for msg in self.pending_unpublish.values() {
            con.queue_send(msg)?;
            nretry += 1;
        }
        // the paths in send order, so each reply can be attributed. The read
        // guard can't be held across the awaits below, and the set can change
        // under us in the meantime.
        let mut sent: LPooled<Vec<Path>> = LPooled::take();
        {
            let published = self.published.read();
            for (path, msg) in published.iter() {
                con.queue_send(msg)?;
                sent.push(path.clone());
            }
        }
        let npub = sent.len();
        let len = nretry + npub;
        if len == 0 {
            info!("connected to resolver {:?} for write", self.resolver_addr);
            if self.degraded {
                con.send_one(&ToWrite::Clear).await?;
                match con.receive().await? {
                    FromWrite::Unpublished | FromWrite::Referral(_) => {
                        self.degraded = false;
                    }
                    m => warn!("unexpected response to clear {:?}", m),
                }
            }
            return Ok(());
        }
        info!(
            "write_con ttl_expired: {} degraded: {} republishing: {}",
            ttl_expired, self.degraded, len
        );
        con.flush().await?;
        let mut settled = 0;
        for _ in 0..nretry {
            match con.receive().await? {
                FromWrite::Unpublished | FromWrite::Referral(_) => settled += 1,
                r => warn!(
                    "republish unexpected response to retry {:?} from resolver {:?}",
                    r, self.resolver_addr
                ),
            }
        }
        if settled == nretry {
            self.pending_clear = false;
            self.pending_unpublish.clear();
        }
        let mut outcomes = OUTCOMES.take();
        for path in sent.drain(..) {
            let r: FromWrite = con.receive().await?;
            if !self.note(&mut outcomes, &path, &r) {
                settled += 1
            }
        }
        self.report(outcomes);
        self.degraded = settled != len;
        info!(
            "connected to resolver {:?} for write (settled {}) degraded: {}",
            self.resolver_addr, settled, self.degraded
        );
        Ok(())
    }

    async fn connect(&mut self) -> Result<()> {
        async fn auth_challenge(con: &mut Channel, secret: u128) -> Result<()> {
            let c: AuthChallenge = con.receive().await?;
            if c.hash_method != HashMethod::Sha3_512 {
                bail!("hash method not supported")
            }
            let answer = utils::make_sha3_token([
                &c.challenge.to_be_bytes()[..],
                &secret.to_be_bytes(),
            ]);
            Ok(con.send_one(&answer).await?)
        }
        info!("write_con connecting to resolver {:?}", self.resolver_addr);
        let mut con = wt!("connect", TcpStream::connect(&self.resolver_addr))??;
        debug!("setting no delay = true");
        con.set_nodelay(true)?;
        debug!("writing protocol version 3");
        wt!("write version", channel::write_raw(&mut con, &3u64))??;
        debug!("reading protocol version");
        if wt!("read version", channel::read_raw::<u64, _, 1024>(&mut con))?? != 3 {
            bail!("incompatible protocol version")
        }
        let sec = Duration::from_secs(1);
        let hello = |auth| {
            let h = ClientHello::WriteOnly(ClientHelloWrite {
                auth,
                write_addr: self.write_addr,
                priority: self.priority,
            });
            debug!("write_con connection established hello {:?}", h);
            h
        };
        let (mut con, r, ownership_check) =
            match (&self.desired_auth, &self.resolver_auth) {
                (DesiredAuth::Anonymous, _) => {
                    debug!("sending anymous auth hello");
                    wt!(
                        "write anonymous",
                        channel::write_raw(&mut con, &hello(AuthWrite::Anonymous))
                    )??;
                    let r = wt!(
                        "read anonymous",
                        channel::read_raw::<ServerHelloWrite, _, 1024>(&mut con)
                    )??;
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
                            wt!(
                                "write local reuse",
                                con.send_one(&hello(AuthWrite::Reuse))
                            )??;
                            wt!("auth challenge", auth_challenge(&mut con, secret))??;
                            let r = wt!(
                                "recv local hello",
                                con.receive::<ServerHelloWrite>()
                            )??;
                            (con, r, false)
                        }
                        None => {
                            debug!("starting a new local auth session");
                            let tok = wt!("get local token", AuthClient::token(&*path))??;
                            wt!(
                                "send local hello",
                                con.send_one(&hello(AuthWrite::Local))
                            )??;
                            wt!("send local token", con.send_one(&tok))??;
                            let r = wt!(
                                "recv local hello",
                                con.receive::<ServerHelloWrite>()
                            )??;
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
                            if ctx.lock().ttl().unwrap_or(sec) > sec =>
                        {
                            debug!("reusing existing session");
                            wt!(
                                "write krb5 reuse",
                                channel::write_raw(&mut con, &hello(AuthWrite::Reuse))
                            )??;
                            let mut con = Channel::new(Some(ctx.clone()), con);
                            wt!(
                                "krb5 auth challenge",
                                auth_challenge(&mut con, secret)
                            )??;
                            let r: ServerHelloWrite =
                                wt!("recv krb5 hello", con.receive())??;
                            (con, r, false)
                        }
                        (None | Some(_), _) => {
                            debug!("starting a new krb5 session");
                            let upn = upn.as_ref().map(|s| s.as_str());
                            let spn =
                                ArcStr::from(spn.clone().ok_or_else(|| {
                                    anyhow!("spn is required for writers")
                                })?);
                            wt!(
                                "write krb5 hello",
                                channel::write_raw(
                                    &mut con,
                                    &hello(AuthWrite::Krb5 { spn })
                                )
                            )??;
                            let ctx =
                                krb5_authentication(upn, &*target_spn, &mut con).await?;
                            let ctx = K5CtxWrap::new(ctx);
                            let mut con = Channel::new(Some(ctx.clone()), con);
                            let r: ServerHelloWrite =
                                wt!("recv krb5 hello", con.receive())??;
                            self.security_context = Some(ctx);
                            (con, r, true)
                        }
                    }
                }
                (DesiredAuth::Tls { .. }, Auth::Krb5 { .. }) => {
                    bail!("tls auth not supported")
                }
                (DesiredAuth::Tls { identity }, Auth::Tls { name }) => {
                    debug!("tls auth selected");
                    let tls = self.tls.as_ref().ok_or_else(|| anyhow!("no tls ctx"))?;
                    let ctx = task::spawn_blocking({
                        let tls = tls.clone();
                        let name = name.clone();
                        move || tls.load(&name)
                    })
                    .await??;
                    let secret = self.secrets.read().get(&self.resolver_addr).map(|u| *u);
                    let name =
                        rustls_pki_types::ServerName::try_from(&**name)?.to_owned();
                    match secret {
                        Some(secret) => {
                            debug!("reusing existing tls session");
                            wt!(
                                "write tls hello reuse",
                                channel::write_raw(&mut con, &hello(AuthWrite::Reuse))
                            )??;
                            let tls = ctx.connect(name, con).await?;
                            let mut con = Channel::new::<
                                ClientCtx,
                                tokio_rustls::client::TlsStream<TcpStream>,
                            >(None, tls);
                            wt!("tls auth challenge", auth_challenge(&mut con, secret))??;
                            let r: ServerHelloWrite =
                                wt!("recv tls hello", con.receive())??;
                            (con, r, false)
                        }
                        None => {
                            let publisher_name = ArcStr::from(match identity {
                                None => tls.default_identity().name.clone(),
                                Some(id) => match tls.get_identity(id) {
                                    None => bail!("identity not found"),
                                    Some(id) => id.name.clone(),
                                },
                            });
                            debug!("starting a new tls session for {}", publisher_name);
                            let h = hello(AuthWrite::Tls { name: publisher_name });
                            wt!("write tls hello", channel::write_raw(&mut con, &h))??;
                            let tls = ctx.connect(name, con).await?;
                            let mut con = Channel::new::<
                                ClientCtx,
                                tokio_rustls::client::TlsStream<TcpStream>,
                            >(None, tls);
                            let r: ServerHelloWrite =
                                wt!("recv tls hello", con.receive())??;
                            (con, r, true)
                        }
                    }
                }
            };
        debug!("write_con resolver hello {:?}", r);
        if ownership_check {
            let secret: Secret = wt!("recv secret", con.receive())??;
            {
                let mut secrets = self.secrets.write();
                secrets.insert(self.resolver_addr, secret.0);
                secrets.insert(r.resolver_id, secret.0);
            }
            wt!(
                "send ready for ownership check",
                con.send_one(&ReadyForOwnershipCheck)
            )??;
        }
        // before republishing, so that whatever it discovers is attributed to
        // a member write_mgr already counts as one that could have accepted
        self.report_up();
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
        // the log has the diagnosis; there is nothing more specific we could
        // call this until the resolver can tell us why it refused us
        self.report_down(PublishError::ResolverUnreachable);
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
                        let wait = rng().random_range(1..12);
                        time::sleep(Duration::from_secs(wait)).await;
                    }
                },
            }
        }
    }

    async fn process_batch(&mut self, tx: Arc<ToCon>) -> Result<()> {
        self.active = true;
        let c = match self.con {
            Some(ref mut c) => c,
            None => match self.connect().await {
                Ok(()) => self.con.as_mut().unwrap(),
                Err(e) => {
                    let err = format!("connection failed {}", e);
                    self.handle_failed_connect(e);
                    bail!(err)
                }
            },
        };
        let timeout = max(HELLO_TO, Duration::from_micros(tx.batch.len() as u64 * 100));
        for (_, m) in &*tx.batch {
            c.queue_send(m)?;
        }
        c.flush_timeout(timeout).await?;
        let mut rx_batch = RAWFROMWRITEPOOL.take();
        while rx_batch.len() < tx.batch.len() {
            time::timeout(timeout, c.receive_batch(&mut *rx_batch)).await??
        }
        let mut outcomes = OUTCOMES.take();
        for ((_, tx), rx) in tx.batch.iter().zip(rx_batch.iter()) {
            match tx {
                ToWrite::Publish(p)
                | ToWrite::PublishDefault(p)
                | ToWrite::PublishWithFlags(p, _)
                | ToWrite::PublishDefaultWithFlags(p, _) => {
                    if self.note(&mut outcomes, p, rx) {
                        self.degraded = true
                    }
                }
                ToWrite::Unpublish(p) | ToWrite::UnpublishDefault(p) => {
                    if self.refused.remove(p).is_some() {
                        outcomes.push((p.clone(), None))
                    }
                }
                ToWrite::Clear => {
                    for (p, _) in self.refused.drain() {
                        outcomes.push((p, None))
                    }
                }
                ToWrite::Heartbeat => (),
            }
        }
        self.report(outcomes);
        let mut result = FROMWRITEPOOL.take();
        // not relevant for writes
        let publishers = PUBLISHERPOOL.take();
        for (i, m) in rx_batch.drain(..).enumerate() {
            result.push((tx.batch[i].0, m))
        }
        if let Some(reply) = tx.replies.lock().pop() {
            let _ = reply.send((publishers, result));
        }
        Ok(())
    }

    async fn start(
        mut receiver: broadcast::Receiver<Arc<ToCon>>,
        resolver_addr: SocketAddr,
        resolver_auth: Auth,
        write_addr: SocketAddr,
        priority: PublisherPriority,
        desired_auth: DesiredAuth,
        secrets: Arc<RwLock<AHashMap<SocketAddr, u128>>>,
        tls: Option<tls::CachedConnector>,
        published: Published,
        events: mpsc::UnboundedSender<ConEvent>,
        stop: oneshot::Receiver<()>,
    ) {
        let mut stop = stop.fuse();
        let now = Instant::now();
        let mut t = Self {
            resolver_addr,
            resolver_auth,
            write_addr,
            priority,
            published,
            pending_clear: false,
            pending_unpublish: AHashMap::default(),
            secrets,
            desired_auth,
            security_context: None,
            tls,
            con: None,
            degraded: false,
            active: false,
            heartbeat: time::interval_at(now + HB, HB),
            disconnect: time::interval_at(now + LINGER, LINGER),
            events,
            refused: AHashMap::default(),
            reported_down: None,
        };
        // A member added to a cluster that is already publishing has to be
        // brought up to date now. Otherwise it sits empty until the next batch
        // or heartbeat, and a heartbeat is half a ttl away. When there is
        // nothing to say — the ordinary case at startup — stay lazy and don't
        // touch the resolver until the publisher does.
        if !t.published.read().is_empty() {
            t.send_heartbeat().await;
        }
        loop {
            select_biased! {
                _ = stop => {
                    // This member has left the cluster. Stop heartbeating and
                    // let the resolver expire our records; unpublishing would
                    // be pointless work against a server nobody is reading.
                    info!("write_con {:?} left the cluster", t.resolver_addr);
                    break
                },
                _ = t.disconnect.tick().fuse() => {
                    if t.active {
                        t.active = false;
                    } else if t.con.is_some() {
                        info!("write_con dropping inactive connection");
                        t.con = None;
                    }
                },
                _ = t.heartbeat.tick().fuse() => {
                    if t.active && !t.degraded {
                        t.active = false;
                    } else {
                        t.send_heartbeat().await;
                    }
                },
                batch = receiver.recv().fuse() => match batch {
                    Err(RecvError::Closed) => break,
                    Err(RecvError::Lagged(_)) => {
                        t.con = None;
                        t.degraded = true;
                    }
                    Ok(batch) => match t.process_batch(batch.clone()).await {
                        Ok(()) => (),
                        Err(e) => {
                            t.con = None;
                            t.degraded = true;
                            // Publishes need no note: they are in `published`,
                            // and being degraded means the whole of it gets
                            // replayed. Removals are already gone from there,
                            // so only this connection remembers them.
                            for (_, tx) in batch.batch.iter() {
                                match tx {
                                    ToWrite::Publish(_)
                                    | ToWrite::PublishDefault(_)
                                    | ToWrite::PublishWithFlags(_, _)
                                    | ToWrite::PublishDefaultWithFlags(_, _) => (),
                                    ToWrite::Unpublish(p) | ToWrite::UnpublishDefault(p) => {
                                        t.pending_unpublish.insert(p.clone(), tx.clone());
                                    }
                                    ToWrite::Clear => t.pending_clear = true,
                                    ToWrite::Heartbeat => (),
                                }
                            }
                            warn!("write batch failed {}", e)
                        },
                    }
                }
            }
        }
    }
}

/// One live write connection. Dropping the stop sender ends the task.
struct Live {
    addr: SocketAddr,
    auth: Auth,
    _stop: oneshot::Sender<()>,
}

/// Start a connection to every member of `referral` we don't already have one
/// for, and retire the ones that are no longer in it. A new member finds the
/// publish set already waiting for it in `published`, so nothing has to be
/// handed over.
fn reconcile(
    live: &mut LPooled<Vec<Live>>,
    referral: &Referral,
    sender: &broadcast::Sender<Arc<ToCon>>,
    published: &Published,
    desired_auth: &DesiredAuth,
    secrets: &Arc<RwLock<AHashMap<SocketAddr, u128>>>,
    write_addr: SocketAddr,
    priority: PublisherPriority,
    tls: &Option<tls::CachedConnector>,
    con_events: &mpsc::UnboundedSender<ConEvent>,
    members: &mut AHashMap<SocketAddr, PublishError>,
) {
    live.retain(|l| {
        let keep = referral.addrs.iter().any(|(a, auth)| *a == l.addr && *auth == l.auth);
        if !keep {
            members.remove(&l.addr);
        }
        keep
    });
    for (addr, auth) in referral.addrs.iter() {
        if live.iter().any(|l| l.addr == *addr && l.auth == *auth) {
            continue;
        }
        let (stop, stop_rx) = oneshot::channel();
        live.push(Live { addr: *addr, auth: auth.clone(), _stop: stop });
        let addr = *addr;
        // a member is assumed usable until it says otherwise, which is what
        // `Connection::reported_down` starts out agreeing with
        members.remove(&addr);
        let auth = auth.clone();
        let desired_auth = desired_auth.clone();
        let secrets = secrets.clone();
        let tls = tls.clone();
        let receiver = sender.subscribe();
        let published = published.clone();
        let con_events = con_events.clone();
        task::spawn(async move {
            Connection::start(
                receiver,
                addr,
                auth,
                write_addr,
                priority,
                desired_auth,
                secrets,
                tls,
                published,
                con_events,
                stop_rx,
            )
            .await;
            info!("write task for {:?} exited", addr);
        });
    }
}

/// What the cluster is collectively doing with the paths we publish.
///
/// One member accepting is enough for a path to be in netidx, so nothing here
/// can be decided by a single connection — this is the only place that knows
/// the whole member set.
struct Aggregate {
    /// members that are not usable, and why. Absent means usable.
    members: AHashMap<SocketAddr, PublishError>,
    /// paths some member refused, and which members refused them. Empty in
    /// the healthy case.
    refused: AHashMap<Path, AHashMap<SocketAddr, PublishError>>,
    /// what we last told the publisher, so that only changes are sent
    last_global: PublishErrors,
    events: Option<mpsc::UnboundedSender<GPooled<Vec<WriteEvent>>>>,
}

impl Aggregate {
    /// The condition of a path: the union of what refused it, plus
    /// `NotPublished` if nobody who could have accepted it did.
    fn state(&self, path: &Path, live: usize) -> PublishErrors {
        let mut errors = PublishErrors::default();
        let refused = match self.refused.get(path) {
            None => return errors,
            Some(r) => r,
        };
        let mut usable_refusals = 0;
        for (addr, e) in refused.iter() {
            errors.insert(*e);
            if !self.members.contains_key(addr) {
                usable_refusals += 1
            }
        }
        if usable_refusals >= live.saturating_sub(self.members.len()) {
            errors.insert(PublishError::NotPublished)
        }
        errors
    }

    /// The condition of every published path: the union of why members are
    /// unusable, plus `NotPublished` if none of them is left.
    fn global(&self, live: usize) -> PublishErrors {
        let mut errors = PublishErrors::default();
        for e in self.members.values() {
            errors.insert(*e)
        }
        if !errors.is_empty() && self.members.len() >= live {
            errors.insert(PublishError::NotPublished)
        }
        errors
    }

    fn send(&mut self, batch: GPooled<Vec<WriteEvent>>) {
        if !batch.is_empty()
            && let Some(events) = &self.events
            && events.unbounded_send(batch).is_err()
        {
            self.events = None
        }
    }

    /// The condition of every refused path, to be compared against after
    /// something that changes what refusals mean.
    fn snapshot(&self, live: usize) -> LPooled<Vec<(Path, PublishErrors)>> {
        self.refused.keys().map(|p| (p.clone(), self.state(p, live))).collect()
    }

    /// Report what changed since `before`, and the global condition if that
    /// changed too. Nothing a member does can add a path to `refused`, so
    /// `before` covers everything that could have moved.
    fn settle(&mut self, before: LPooled<Vec<(Path, PublishErrors)>>, live: usize) {
        let mut batch = WRITE_EVENTS.take();
        for (path, was) in before.iter() {
            let now = self.state(path, live);
            if now != *was {
                batch.push(WriteEvent { path: Some(path.clone()), errors: now })
            }
        }
        let global = self.global(live);
        if global != self.last_global {
            self.last_global = global;
            batch.push(WriteEvent { path: None, errors: global })
        }
        self.send(batch)
    }

    fn handle(&mut self, ev: ConEvent, live: usize) {
        match ev {
            ConEvent::Up(addr) => {
                let before = self.snapshot(live);
                self.members.remove(&addr);
                self.settle(before, live)
            }
            ConEvent::Down(addr, reason) => {
                let before = self.snapshot(live);
                self.members.insert(addr, reason);
                self.settle(before, live)
            }
            ConEvent::Outcome(addr, mut outcomes) => {
                let mut batch = WRITE_EVENTS.take();
                for (path, outcome) in outcomes.drain(..) {
                    let before = self.state(&path, live);
                    match outcome {
                        Some(e) => {
                            self.refused
                                .entry(path.clone())
                                .or_insert_with(AHashMap::default)
                                .insert(addr, e);
                        }
                        None => {
                            if let Some(r) = self.refused.get_mut(&path) {
                                r.remove(&addr);
                                if r.is_empty() {
                                    self.refused.remove(&path);
                                }
                            }
                        }
                    }
                    let after = self.state(&path, live);
                    if after != before {
                        batch.push(WriteEvent { path: Some(path), errors: after })
                    }
                }
                self.send(batch)
            }
        }
    }
}

async fn write_mgr(
    mut receiver: mpsc::UnboundedReceiver<Batch>,
    mut resolver: watch::Receiver<Arc<Referral>>,
    desired_auth: DesiredAuth,
    secrets: Arc<RwLock<AHashMap<SocketAddr, u128>>>,
    write_addr: SocketAddr,
    priority: PublisherPriority,
    tls: Option<tls::CachedConnector>,
    events: Option<mpsc::UnboundedSender<GPooled<Vec<WriteEvent>>>>,
) -> Result<()> {
    let published: Published = Arc::new(RwLock::new(IndexMap::default()));
    let (sender, _) = broadcast::channel(100);
    let (con_events, mut con_events_rx) = mpsc::unbounded();
    let mut agg = Aggregate {
        members: AHashMap::default(),
        refused: AHashMap::default(),
        last_global: PublishErrors::default(),
        events,
    };
    let mut live: LPooled<Vec<Live>> = LPooled::take();
    let reconcile_now =
        |live: &mut LPooled<Vec<Live>>, agg: &mut Aggregate, referral: &Referral| {
            reconcile(
                live,
                referral,
                &sender,
                &published,
                &desired_auth,
                &secrets,
                write_addr,
                priority,
                &tls,
                &con_events,
                &mut agg.members,
            )
        };
    let referral = resolver.borrow_and_update().clone();
    reconcile_now(&mut live, &mut agg, &referral);
    loop {
        let (batch, reply) = select_biased! {
            () = addrs_changed(&mut resolver).fuse() => {
                let referral = resolver.borrow_and_update().clone();
                let before = agg.snapshot(live.len());
                reconcile_now(&mut live, &mut agg, &referral);
                agg.settle(before, live.len());
                continue
            },
            ev = con_events_rx.next().fuse() => match ev {
                None => break,
                Some(ev) => {
                    agg.handle(ev, live.len());
                    continue
                }
            },
            batch = receiver.next().fuse() => match batch {
                None => break,
                Some(b) => b,
            },
        };
        // Record what the publisher wants before telling anyone about it. A
        // connection that misses this batch — because it is down, because it
        // fell behind the broadcast, or because it doesn't exist yet — still
        // finds it here when it connects, which is the only way it could ever
        // learn.
        {
            let mut published = published.write();
            for (_, m) in batch.iter() {
                match m {
                    ToWrite::Publish(p)
                    | ToWrite::PublishDefault(p)
                    | ToWrite::PublishWithFlags(p, _)
                    | ToWrite::PublishDefaultWithFlags(p, _) => {
                        published.insert(p.clone(), m.clone());
                    }
                    ToWrite::Unpublish(p) | ToWrite::UnpublishDefault(p) => {
                        published.swap_remove(p);
                    }
                    ToWrite::Clear => published.clear(),
                    ToWrite::Heartbeat => (),
                }
            }
        }
        let mut replies = vec![];
        let mut waiters = vec![];
        for _ in live.iter() {
            let (tx, rx) = oneshot::channel();
            replies.push(tx);
            waiters.push(rx);
        }
        let tx_batch = Arc::new(ToCon { batch, replies: Mutex::new(replies) });
        let _ = sender.send(tx_batch);
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
        resolver: watch::Receiver<Arc<Referral>>,
        desired_auth: DesiredAuth,
        write_addr: SocketAddr,
        priority: PublisherPriority,
        secrets: Arc<RwLock<AHashMap<SocketAddr, u128>>>,
        tls: Option<tls::CachedConnector>,
        events: Option<mpsc::UnboundedSender<GPooled<Vec<WriteEvent>>>>,
    ) -> Self {
        let (to_tx, to_rx) = mpsc::unbounded();
        task::spawn(async move {
            let r = write_mgr(
                to_rx,
                resolver,
                desired_auth,
                secrets,
                write_addr,
                priority,
                tls,
                events,
            )
            .await;
            info!("write manager exited {:?}", r);
        });
        Self(to_tx)
    }

    pub(crate) fn send(
        &mut self,
        batch: GPooled<Vec<(usize, ToWrite)>>,
    ) -> ResponseChan<FromWrite> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.unbounded_send((batch, tx));
        rx
    }
}
