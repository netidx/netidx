use super::common::{
    krb5_authentication, DesiredAuth, Response, ResponseChan, FROMREADPOOL, HELLO_TO,
    PUBLISHERPOOL, RAWFROMREADPOOL,
};
use crate::{
    channel::{self, Channel, K5CtxWrap},
    os::local_auth::AuthClient,
    pool::Pooled,
    protocol::resolver::{
        Auth, AuthRead, ClientHello, FromRead, Publisher, Referral, ToRead,
    },
    tls,
    utils::Either,
};
use anyhow::{Context, Error, Result};
use cross_krb5::ClientCtx;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use fxhash::FxHashSet;
use log::{info, warn};
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::{
    cmp::max, collections::HashSet, fmt::Debug, net::SocketAddr, sync::Arc,
    time::Duration,
};
use tokio::{net::TcpStream, task, time};

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

async fn connect(
    bad_addrs: &mut FxHashSet<SocketAddr>,
    resolver: &Referral,
    desired_auth: &DesiredAuth,
    tls: &Option<tls::CachedConnector>,
) -> Result<Channel> {
    let mut addrs = resolver.addrs.clone();
    addrs.as_mut_slice().shuffle(&mut thread_rng());
    let mut n = 0;
    loop {
        let (addr, auth) = &addrs[n % addrs.len()];
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
            let wait = thread_rng().gen_range(1..12);
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
                bad_addrs.insert(*addr);
                continue;
            }
            Ok(Err(e)) => {
                warn!("failed to connect to resolver server {} error: {}", addr, e);
                bad_addrs.insert(*addr);
                continue;
            }
        };
        try_cf!("no delay", con.set_nodelay(true));
        cwt!("send version", channel::write_raw(&mut con, &3u64));
        if cwt!("recv version", channel::read_raw::<u64, _>(&mut con)) != 3 {
            continue;
        }
        let con = match (desired_auth, auth) {
            (DesiredAuth::Anonymous, _) => {
                let mut con = Channel::new::<ClientCtx, TcpStream>(None, con);
                cwt!("hello", con.send_one(&ClientHello::ReadOnly(AuthRead::Anonymous)));
                match cwt!("reply", con.receive::<AuthRead>()) {
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
                let tok = cwt!("local token", AuthClient::token(&*path));
                cwt!("hello", con.send_one(&ClientHello::ReadOnly(AuthRead::Local)));
                cwt!("token", con.send_one(&tok));
                match cwt!("reply", con.receive::<AuthRead>()) {
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
                cwt!("hello", channel::write_raw(&mut con, &hello));
                let ctx = cwt!("k5auth", krb5_authentication(upn, &*spn, &mut con));
                match cwt!("reply", channel::read_raw::<AuthRead, _>(&mut con)) {
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
                let ctx = task::spawn_blocking({
                    let tls = tls.clone();
                    let name = name.clone();
                    move || tls.load(&name)
                })
                .await
                .context("loading tls connector")??;
                let hello = ClientHello::ReadOnly(AuthRead::Tls);
                cwt!("hello", channel::write_raw(&mut con, &hello));
                let name = rustls_pki_types::ServerName::try_from(&**name)
                    .context("creating rustls servername")?
                    .to_owned();
                let tls = ctx.connect(name, con).await?;
                let mut con = Channel::new::<
                    ClientCtx,
                    tokio_rustls::client::TlsStream<TcpStream>,
                >(None, tls);
                match cwt!("reply", con.receive::<AuthRead>()) {
                    AuthRead::Tls => con,
                    AuthRead::Local | AuthRead::Anonymous | AuthRead::Krb5 { .. } => {
                        bail!("protocol error")
                    }
                }
            }
        };
        break Ok(con);
    }
}

type Batch = (Pooled<Vec<(usize, ToRead)>>, oneshot::Sender<Response<FromRead>>);

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
    resolver: Arc<Referral>,
    desired_auth: DesiredAuth,
    tls: Option<tls::CachedConnector>,
) {
    let mut con: Option<Channel> = None;
    let mut bad_addrs: FxHashSet<SocketAddr> = HashSet::default();
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
                        None => {
                            match connect(&mut bad_addrs, &resolver, &desired_auth, &tls)
                                .await
                            {
                                Ok(c) => {
                                    con = Some(c);
                                    con.as_mut().unwrap()
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
                                                publishers.insert(p.id, p);
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
        resolver: Arc<Referral>,
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
        batch: Pooled<Vec<(usize, ToRead)>>,
    ) -> ResponseChan<FromRead> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.unbounded_send((batch, tx));
        rx
    }
}
