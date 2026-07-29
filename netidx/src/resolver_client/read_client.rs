use super::common::{
    DesiredAuth, FROMREADPOOL, HELLO_TO, PUBLISHERPOOL, RAWFROMREADPOOL, Response,
    ResponseChan, addrs_changed, krb5_authentication,
};
use super::insert_publisher;
use crate::{
    channel::{self, Channel, K5CtxWrap},
    os::local_auth::AuthClient,
    protocol::resolver::{
        Auth, AuthRead, ClientHello, FromRead, Publisher, Referral, ToRead,
    },
    tls,
    utils::Either,
};
use ahash::AHashSet;
use anyhow::{Context, Error, Result};
use cross_krb5::ClientCtx;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
    select_biased,
};
use log::{info, warn};
use poolshark::{global::GPooled, local::LPooled};
use rand::{RngExt, rng, seq::SliceRandom};
use std::{cmp::max, fmt::Debug, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::watch, task, time};

/// Try a step of the hello exchange with `addr`, moving on to the next member
/// if it fails or times out.
///
/// The address is in the message because these are the failures you cannot
/// otherwise attribute: a member refusing reads (a read gate) closes the
/// connection mid-hello, and without the address the operator is left looking
/// at the members that are merely *down*, which are the ones that do get
/// named. `warn` for the same reason — a member that hangs up on you is a
/// failed connection attempt like any other.
macro_rules! cwt {
    ($step:expr, $addr:expr, $e:expr) => {{
        let step = $step;
        let addr = $addr;
        match time::timeout(HELLO_TO, $e).await {
            Err(_) => {
                warn!("resolver server {addr} timed out during {step}");
                continue;
            }
            Ok(Err(e)) => {
                warn!("resolver server {addr} failed during {step}: {e}");
                continue;
            }
            Ok(Ok(r)) => r,
        }
    }};
}

/// Connect to any one member of `resolver`, returning which one it turned out
/// to be so the caller can tell when that member leaves the cluster.
async fn connect(
    bad_addrs: &mut AHashSet<SocketAddr>,
    resolver: &Referral,
    desired_auth: &DesiredAuth,
    tls: &Option<tls::CachedConnector>,
) -> Result<(SocketAddr, Channel)> {
    let mut addrs = resolver.addrs.clone();
    addrs.as_mut_slice().shuffle(&mut rng());
    let mut n = 0;
    loop {
        let (addr, auth) = &addrs[n % addrs.len()];
        let addr = *addr;
        let tries = n / addrs.len();
        if tries >= 3 {
            bail!("can't connect to any resolver servers");
        }
        if tries == 0 && bad_addrs.contains(&addr) {
            n += 1;
            continue;
        } else {
            bad_addrs.clear()
        }
        if n % addrs.len() == 0 && tries > 0 {
            let wait = rng().random_range(1..12);
            time::sleep(Duration::from_secs(wait)).await;
        }
        n += 1;
        let mut con = match time::timeout(HELLO_TO, TcpStream::connect(&addr)).await {
            Ok(Ok(con)) => con,
            Err(_) => {
                warn!(
                    "failed to connect to resolver server {} connection timed out",
                    addr
                );
                bad_addrs.insert(addr);
                continue;
            }
            Ok(Err(e)) => {
                warn!("failed to connect to resolver server {} error: {}", addr, e);
                bad_addrs.insert(addr);
                continue;
            }
        };
        try_cf!("no delay", con.set_nodelay(true));
        cwt!("send version", addr, channel::write_raw(&mut con, &3u64));
        if cwt!("recv version", addr, channel::read_raw::<u64, _, 1024>(&mut con)) != 3 {
            continue;
        }
        let con = match (desired_auth, auth) {
            (DesiredAuth::Anonymous, _) => {
                let mut con = Channel::new::<ClientCtx, TcpStream>(None, con);
                cwt!(
                    "hello",
                    addr,
                    con.send_one(&ClientHello::ReadOnly(AuthRead::Anonymous))
                );
                match cwt!("reply", addr, con.receive::<AuthRead>()) {
                    AuthRead::Anonymous => (),
                    AuthRead::Local | AuthRead::Krb5 | AuthRead::Tls => {
                        bail!("protocol error")
                    }
                }
                con
            }
            (
                DesiredAuth::Krb5 { .. } | DesiredAuth::Local | DesiredAuth::Tls { .. },
                Auth::Anonymous,
            ) => {
                bail!("requested authentication mechanism not supported")
            }
            (
                DesiredAuth::Local | DesiredAuth::Krb5 { .. } | DesiredAuth::Tls { .. },
                Auth::Local { path },
            ) => {
                let mut con = Channel::new::<ClientCtx, TcpStream>(None, con);
                let tok = cwt!("local token", addr, AuthClient::token(&*path));
                cwt!(
                    "hello",
                    addr,
                    con.send_one(&ClientHello::ReadOnly(AuthRead::Local))
                );
                cwt!("token", addr, con.send_one(&tok));
                match cwt!("reply", addr, con.receive::<AuthRead>()) {
                    AuthRead::Local => (),
                    AuthRead::Krb5 | AuthRead::Anonymous | AuthRead::Tls => {
                        bail!("protocol error")
                    }
                }
                con
            }
            (DesiredAuth::Local, Auth::Krb5 { .. } | Auth::Tls { .. }) => {
                bail!("local auth not supported")
            }
            (DesiredAuth::Krb5 { .. }, Auth::Tls { .. }) => {
                bail!("krb5 authentication is not supported")
            }
            (DesiredAuth::Krb5 { upn, .. }, Auth::Krb5 { spn }) => {
                let upn = upn.as_ref().map(|s| s.as_str());
                let hello = ClientHello::ReadOnly(AuthRead::Krb5);
                cwt!("hello", addr, channel::write_raw(&mut con, &hello));
                let ctx = cwt!("k5auth", addr, krb5_authentication(upn, &*spn, &mut con));
                match cwt!(
                    "reply",
                    addr,
                    channel::read_raw::<AuthRead, _, 1024>(&mut con)
                ) {
                    AuthRead::Krb5 => Channel::new(Some(K5CtxWrap::new(ctx)), con),
                    AuthRead::Local | AuthRead::Anonymous | AuthRead::Tls => {
                        bail!("protocol error")
                    }
                }
            }
            (DesiredAuth::Tls { .. }, Auth::Krb5 { .. }) => {
                bail!("tls authentication is not supported")
            }
            (DesiredAuth::Tls { .. }, Auth::Tls { name }) => {
                let tls = tls.as_ref().ok_or_else(|| anyhow!("no tls cache"))?;
                // Everything from here on is specific to *this* server: its
                // identity, its name, its handshake. A failure means try the
                // next address, not give up on the cluster — and a server
                // that accepts the connection and then says nothing must not
                // hang us, which is what a read-gated member looks like.
                let ctx = try_cf!(
                    "loading tls connector",
                    continue,
                    task::spawn_blocking({
                        let tls = tls.clone();
                        let name = name.clone();
                        move || tls.load(&name)
                    })
                    .await
                    .context("joining tls connector load")
                    .and_then(|r| r)
                );
                let hello = ClientHello::ReadOnly(AuthRead::Tls);
                cwt!("hello", addr, channel::write_raw(&mut con, &hello));
                let name = try_cf!(
                    "creating rustls servername",
                    continue,
                    rustls_pki_types::ServerName::try_from(&**name)
                )
                .to_owned();
                let tls = cwt!("tls handshake", addr, ctx.connect(name, con));
                let mut con = Channel::new::<
                    ClientCtx,
                    tokio_rustls::client::TlsStream<TcpStream>,
                >(None, tls);
                match cwt!("reply", addr, con.receive::<AuthRead>()) {
                    AuthRead::Tls => con,
                    AuthRead::Local | AuthRead::Anonymous | AuthRead::Krb5 { .. } => {
                        bail!("protocol error")
                    }
                }
            }
        };
        break Ok((addr, con));
    }
}

type Batch = (GPooled<Vec<(usize, ToRead)>>, oneshot::Sender<Response<FromRead>>);

fn partition_publishers(m: FromRead) -> Either<FromRead, Publisher> {
    match m {
        FromRead::Publisher(p) => Either::Right(p),
        FromRead::Denied
        | FromRead::Error(_)
        | FromRead::GetChangeNr(_)
        | FromRead::List(_)
        | FromRead::ListMatching(_)
        | FromRead::Referral(_)
        | FromRead::Resolved(_)
        | FromRead::Table(_) => Either::Left(m),
    }
}

async fn connection(
    mut receiver: mpsc::UnboundedReceiver<Batch>,
    mut resolver: watch::Receiver<Arc<Referral>>,
    desired_auth: DesiredAuth,
    tls: Option<tls::CachedConnector>,
) {
    let mut con: Option<(SocketAddr, Channel)> = None;
    let mut bad_addrs: LPooled<AHashSet<SocketAddr>> = LPooled::take();
    'main: loop {
        let batch = select_biased! {
            () = addrs_changed(&mut resolver).fuse() => {
                // Nothing forces us off a member that is still in the cluster,
                // but staying connected to one that has left would keep
                // answering from a resolver nobody is publishing to any more.
                if let Some((addr, _)) = con.as_ref()
                    && !resolver.borrow().addrs.iter().any(|(a, _)| a == addr)
                {
                    info!("read_con {addr} left the cluster, disconnecting");
                    con = None;
                }
                continue 'main;
            },
            batch = receiver.next().fuse() => batch,
        };
        match batch {
            None => break,
            Some((tx_batch, reply)) => {
                let mut tries: usize = 0;
                'batch: loop {
                    if tries > 3 {
                        break;
                    }
                    if tries > 1 {
                        let wait = rng().random_range(1..12);
                        time::sleep(Duration::from_secs(wait)).await
                    }
                    tries += 1;
                    let c = match con {
                        Some((_, ref mut c)) => c,
                        None => {
                            let current = resolver.borrow_and_update().clone();
                            match connect(&mut *bad_addrs, &current, &desired_auth, &tls)
                                .await
                            {
                                Ok(c) => {
                                    con = Some(c);
                                    &mut con.as_mut().unwrap().1
                                }
                                Err(e) => {
                                    con = None;
                                    warn!(
                                        "connect_read failed: {}, {}",
                                        e,
                                        e.root_cause()
                                    );
                                    continue;
                                }
                            }
                        }
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
                            let mut publishers = PUBLISHERPOOL.take();
                            while rx_batch.len() < tx_batch.len() {
                                let f =
                                    c.receive_batch_fn(|m| {
                                        match partition_publishers(m) {
                                            Either::Left(m) => rx_batch.push(m),
                                            Either::Right(p) => {
                                                insert_publisher(&mut publishers, p);
                                            }
                                        }
                                    });
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
                            let _ = reply.send((publishers, result));
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ReadClient(mpsc::UnboundedSender<Batch>);

impl ReadClient {
    pub(super) fn new(
        resolver: watch::Receiver<Arc<Referral>>,
        desired_auth: DesiredAuth,
        tls: Option<tls::CachedConnector>,
    ) -> Self {
        let (to_tx, to_rx) = mpsc::unbounded();
        task::spawn(async move {
            connection(to_rx, resolver, desired_auth, tls).await;
            info!("read task shutting down")
        });
        Self(to_tx)
    }

    pub(crate) fn send(
        &mut self,
        batch: GPooled<Vec<(usize, ToRead)>>,
    ) -> ResponseChan<FromRead> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.unbounded_send((batch, tx));
        rx
    }
}
