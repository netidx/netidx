use crate::{
    utils::MPCodec,
    path::Path,
    resolver_server::{ToResolver, FromResolver, ClientHello, ServerHello}
};
use futures::{
    future::FutureExt as FRSFutureExt,
    sink::SinkExt,
    channel::{mpsc, oneshot},
};
use std::{
    result,
    net::SocketAddr,
    collections::HashSet,
    time::Duration,
    marker::PhantomData,
};
use async_std::{
    task, future,
    prelude::*,
    net::TcpStream,
};
use futures_codec::Framed;
use failure::Error;

static TTL: u64 = 600;
static LINGER: u64 = 10;

type FromCon = oneshot::Sender<FromResolver>;
type ToCon = (ToResolver, FromCon);

pub trait Readable {}
pub trait Writeable {}

pub struct ReadOnly {}

impl Readable for ReadOnly {}

pub struct ReadWrite {}

impl Readable for ReadWrite {}
impl Writeable for ReadWrite {}

type Result<T> = result::Result<T, Error>;

#[derive(Clone)]
pub struct Resolver<R> {
    sender: mpsc::UnboundedSender<ToCon>,
    kind: PhantomData<R>
}

impl<R> Resolver<R> {
    async fn send(&mut self, m: ToResolver) -> Result<FromResolver> {
        let (tx, rx) = oneshot::channel();
        self.sender.send((m, tx)).await?;
        match rx.await? {
            FromResolver::Error(s) => bail!(s),
            m => Ok(m)
        }
    }

    pub fn new_rw(resolver: SocketAddr, publisher: SocketAddr) -> Resolver<ReadWrite> {
        let (sender, receiver) = mpsc::unbounded();
        task::spawn(connection(receiver, resolver, Some(publisher)));
        Resolver { sender, kind: PhantomData }
    }

    pub fn new_ro(resolver: SocketAddr) -> Resolver<ReadOnly> {
        let (sender, receiver) = mpsc::unbounded();
        task::spawn(connection(receiver, resolver, None));
        Resolver { sender, kind: PhantomData }
    }
}
 
impl<R: Readable> Resolver<R> {
    pub async fn resolve(&mut self, paths: Vec<Path>) -> Result<Vec<Vec<SocketAddr>>> {
        match self.send(ToResolver::Resolve(paths)).await? {
            FromResolver::Resolved(ports) => Ok(ports),
            _ => bail!("unexpected response")
        }
    }

    pub async fn list(&mut self, p: Path) -> Result<Vec<Path>> {
        match self.send(ToResolver::List(p)).await? {
            FromResolver::List(v) => Ok(v),
            _ => bail!("unexpected response")
        }
    }
}

impl <R: Writeable> Resolver<R> {
    pub async fn publish(&mut self, paths: Vec<Path>) -> Result<()> {
        match self.send(ToResolver::Publish(paths)).await? {
            FromResolver::Published => Ok(()),
            _ => bail!("unexpected response")
        }
    }

    pub async fn unpublish(&mut self, paths: Vec<Path>) -> Result<()> {
        match self.send(ToResolver::Unpublish(paths)).await? {
            FromResolver::Unpublished => Ok(()),
            _ => bail!("unexpected response")
        }
    }
}

type Con = Framed<TcpStream, MPCodec<ToResolver, FromResolver>>;

macro_rules! or_continue {
    ($e:expr) => {
        match $e {
            Err(_) => continue,
            Ok(x) => x
        }
    }
}

async fn connect(
    addr: SocketAddr,
    publisher: Option<SocketAddr>,
    published: &HashSet<Path>,
) -> Con {
    let mut backoff = 0;
    loop {
        if backoff > 0 {
            task::sleep(Duration::from_secs(backoff)).await;
        }
        backoff += 1;
        let con = or_continue!(TcpStream::connect(&addr).await);
        let mut con = Framed::new(con, MPCodec::<ClientHello, ServerHello>::new());
        or_continue!(con.send(match publisher {
            None => ClientHello::ReadOnly,
            Some(write_addr) => ClientHello::ReadWrite {ttl: TTL, write_addr},
        }).await);
        match con.next().await {
            None | Some(Err(_)) => (),
            Some(Ok(ServerHello { ttl_expired })) => {
                let mut con = Framed::new(
                    con.release().0,
                    MPCodec::<ToResolver, FromResolver>::new()
                );
                if !ttl_expired {
                    break con
                } else {
                    let paths = published.iter().cloned().collect();
                    or_continue!(con.send(ToResolver::Publish(paths)).await);
                    match con.next().await {
                        Some(Ok(FromResolver::Published)) => break con,
                        _ => ()
                    }
                }
            }
        }
    }
}

async fn connection(
    mut receiver: mpsc::UnboundedReceiver<ToCon>,
    resolver: SocketAddr,
    publisher: Option<SocketAddr>
) {
    enum M { TimeToHB, TimeToDC, Msg(ToCon), Stop }
    let mut published = HashSet::new();
    let mut con: Option<Con> = None;
    let ttl = Duration::from_secs(TTL / 2);
    let linger = Duration::from_secs(LINGER);
    loop {
        let hb = future::ready(M::TimeToHB).delay(ttl);
        let dc = future::ready(M::TimeToDC).delay(linger);
        let msg = receiver.next().map(|m| match m {
            None => M::Stop,
            Some(m) => M::Msg(m)
        });
        match hb.race(dc).race(msg).await {
            M::Stop => break,
            M::TimeToDC => { con = None; }
            M::TimeToHB => {
                con = Some(connect(resolver, publisher, &published).await);
            }
            M::Msg((m, reply)) => {
                let m_r = &m;
                let r = loop {
                    let c = match con {
                        Some(ref mut c) => c,
                        None => {
                            con = Some(connect(resolver, publisher, &published).await);
                            con.as_mut().unwrap()
                        }
                    };
                    match c.send(m_r.clone()).await {
                        Err(_) => { con = None; }
                        Ok(()) => match c.next().await {
                            None | Some(Err(_)) => { con = None; }
                            Some(Ok(FromResolver::Published)) => {
                                published.extend(match m_r {
                                    ToResolver::Publish(p) => p.clone(),
                                    _ => Vec::new()
                                }.into_iter());
                                break FromResolver::Published
                            }
                            Some(Ok(r)) => break r
                        }
                    }
                };
                let _ = reply.send(r);
            }
        }
    }
}
