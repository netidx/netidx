use crate::{
    path::Path,
    channel::Channel,
    protocol::resolver::*,
};
use std::{
    result,
    net::{SocketAddr, ToSocketAddrs},
    collections::HashSet,
    time::Duration,
    marker::PhantomData,
};
use futures::{
    select,
    prelude::*
};
use tokio::{
    task,
    time::{self, Instant},
    sync::{oneshot, mpsc},
    net::TcpStream,
};
use failure::Error;

static TTL: u64 = 600;
static LINGER: u64 = 10;

type FromCon = oneshot::Sender<FromResolver>;
type ToCon = (ToResolver, FromCon);

pub trait Readable {}
pub trait Writeable {}
pub trait ReadableOrWritable {}

#[derive(Debug, Clone)]
pub struct ReadOnly {}

impl Readable for ReadOnly {}
impl ReadableOrWritable for ReadOnly {}

#[derive(Debug, Clone)]
pub struct WriteOnly {}

impl Writeable for WriteOnly {}
impl ReadableOrWritable for WriteOnly {}

type Result<T> = result::Result<T, Error>;

#[derive(Debug, Clone)]
pub struct Resolver<R> {
    sender: mpsc::UnboundedSender<ToCon>,
    kind: PhantomData<R>
}

impl<R: ReadableOrWritable> Resolver<R> {
    async fn send(&mut self, m: ToResolver) -> Result<FromResolver> {
        let (tx, rx) = oneshot::channel();
        self.sender.send((m, tx))?;
        match rx.await? {
            FromResolver::Error(s) => bail!(s),
            m => Ok(m)
        }
    }

    pub fn new_w<T>(resolver: T, publisher: SocketAddr) -> Result<Resolver<WriteOnly>>
    where T: ToSocketAddrs {
        let resolver =
            resolver.to_socket_addrs()?.next().ok_or_else(|| format_err!("no address"))?;
        let (sender, receiver) = mpsc::unbounded_channel();
        task::spawn(connection(receiver, resolver, Some(publisher)));
        Ok(Resolver { sender, kind: PhantomData })
    }

    // CR estokes: when given more than one socket address the
    // resolver should make use of all of them.
    pub fn new_r<T>(resolver: T) -> Result<Resolver<ReadOnly>>
    where T: ToSocketAddrs {
        let resolver =
            resolver.to_socket_addrs()?.next().ok_or_else(|| format_err!("no address"))?;
        let (sender, receiver) = mpsc::unbounded_channel();
        task::spawn(connection(receiver, resolver, None));
        Ok(Resolver { sender, kind: PhantomData })
    }
}
 
impl<R: Readable + ReadableOrWritable> Resolver<R> {
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

impl <R: Writeable + ReadableOrWritable> Resolver<R> {
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

async fn connect(
    addr: SocketAddr,
    publisher: Option<SocketAddr>,
    published: &HashSet<Path>,
) -> Channel {
    let mut backoff = 0;
    loop {
        if backoff > 0 {
            time::delay_for(Duration::from_secs(backoff)).await;
        }
        backoff += 1;
        let con = try_cont!("connect", TcpStream::connect(&addr).await);
        let mut con = Channel::new(con);
        // the unwrap can't fail, we can always encode the message,
        // and it's never bigger then 4 GB.
        try_cont!("hello", con.send_one(&match publisher {
            None => ClientHello::ReadOnly,
            Some(write_addr) => ClientHello::WriteOnly {ttl: TTL, write_addr},
        }).await);
        let r: ServerHello = try_cont!("hello reply", con.receive().await);
        if !r.ttl_expired {
            break con
        } else {
            let m = ToResolver::Publish(published.iter().cloned().collect());
            try_cont!("republish", con.send_one(&m).await);
            match try_cont!("replublish reply", con.receive().await) {
                FromResolver::Published => break con,
                _ => ()
            }
        }
    }
}

async fn connection(
    receiver: mpsc::UnboundedReceiver<ToCon>,
    resolver: SocketAddr,
    publisher: Option<SocketAddr>
) {
    let mut published = HashSet::new();
    let mut con: Option<Channel> = None;
    let ttl = Duration::from_secs(TTL / 2);
    let linger = Duration::from_secs(LINGER);
    let now = Instant::now();
    let mut act = false;
    let mut receiver = receiver.fuse();
    let mut hb = time::interval_at(now + ttl, ttl).fuse();
    let mut dc = time::interval_at(now + linger, linger).fuse();
    loop {
        select! {
            _ = dc.next() => {
                if act {
                   act = false; 
                } else {
                    con = None;
                }
            },
            _ = hb.next() => loop {
                if act {
                    break;
                } else {
                    match con {
                        None => {
                            con = Some(connect(resolver, publisher, &published).await);
                            break;
                        },
                        Some(ref mut c) => match c.send_one(&ToResolver::Heartbeat).await {
                            Ok(()) => break,
                            Err(_) => { con = None; }
                        }
                    }
                }
            },
            m = receiver.next() => match m {
                None => break,
                Some((m, reply)) => {
                    act = true;
                    let m_r = &m;
                    let r = loop {
                        let c = match con {
                            Some(ref mut c) => c,
                            None => {
                                con = Some(connect(resolver, publisher, &published).await);
                                con.as_mut().unwrap()
                            }
                        };
                        match c.send_one(m_r).await {
                            Err(_) => { con = None; }
                            Ok(()) => match c.receive().await {
                                Err(_) => { con = None; }
                                Ok(r) => break r,
                                Ok(FromResolver::Published) => {
                                    if let ToResolver::Publish(p) = m_r {
                                        published.extend(p.iter().cloned());
                                    }
                                    break FromResolver::Published
                                }
                            }
                        }
                    };
                    let _ = reply.send(r);
                }
            }
        }
    }
}
