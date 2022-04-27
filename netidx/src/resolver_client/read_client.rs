use super::common::{
    krb5_authentication, DesiredAuth, Response, ResponseChan, FROMREADPOOL, HELLO_TO,
    PUBLISHERPOOL, RAWFROMREADPOOL,
};
use crate::{
    channel::Channel,
    channel::K5CtxWrap,
    os::local_auth::AuthClient,
    pool::Pooled,
    protocol::resolver::{
        Auth, AuthRead, ClientHello, FromRead, Publisher, Referral, ToRead,
    },
    utils::Either,
};
use anyhow::{Error, Result};
use cross_krb5::ClientCtx;
use futures::{
    channel::{mpsc, oneshot},
    prelude::*,
};
use log::{info, warn};
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::{cmp::max, fmt::Debug, sync::Arc, time::Duration};
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
    resolver: &Referral,
    desired_auth: &DesiredAuth,
) -> Result<Channel<ClientCtx>> {
    let mut addrs = resolver.addrs.clone();
    addrs.as_mut_slice().shuffle(&mut thread_rng());
    let mut n = 0;
    loop {
        let (addr, auth) = &addrs[n % addrs.len()];
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
        match (desired_auth, auth) {
            (DesiredAuth::Anonymous, _) => {
                cwt!("hello", con.send_one(&ClientHello::ReadOnly(AuthRead::Anonymous)));
                match cwt!("reply", con.receive::<AuthRead>()) {
                    AuthRead::Anonymous => (),
                    AuthRead::Local | AuthRead::Krb5 => bail!("protocol error"),
                }
            }
            (DesiredAuth::Local | DesiredAuth::Krb5 { .. }, Auth::Local { path }) => {
                let tok = cwt!("local token", AuthClient::token(&*path));
                cwt!("hello", con.send_one(&ClientHello::ReadOnly(AuthRead::Local)));
                cwt!("token", con.send_one(&tok));
                match cwt!("reply", con.receive::<AuthRead>()) {
                    AuthRead::Local => (),
                    AuthRead::Krb5 | AuthRead::Anonymous => bail!("protocol error"),
                }
            }
            (DesiredAuth::Krb5 { upn, .. }, Auth::Krb5 { spn }) => {
                let upn = upn.as_ref().map(|s| s.as_str());
                cwt!("hello", con.send_one(&ClientHello::ReadOnly(AuthRead::Krb5)));
                let ctx = cwt!("k5auth", krb5_authentication(upn, &*spn, &mut con));
                match cwt!("reply", con.receive::<AuthRead>()) {
                    AuthRead::Krb5 => con.set_ctx(K5CtxWrap::new(ctx)).await,
                    AuthRead::Local | AuthRead::Anonymous => bail!("protocol error"),
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
                        None => match connect(&resolver, &desired_auth).await {
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
    pub(super) fn new(resolver: Arc<Referral>, desired_auth: DesiredAuth) -> Self {
        let (to_tx, to_rx) = mpsc::unbounded();
        task::spawn(async move {
            connection(to_rx, resolver, desired_auth).await;
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
