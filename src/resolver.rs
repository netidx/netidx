use futures::{
    future::{FutureExt, TryFutureExt, RemoteHandle},
    channel::{mpsc, oneshot},
};
use std::{
    result, mem,
    io::BufReader,
    net::{SocketAddr, TcpStream},
    collections::{VecDeque, HashSet},
    sync::{Arc, Weak, Mutex},
    time::{Duration, Instant},
    marker::PhantomData,
};
use async_std::{ prelude::*, task, future };
use futures_codec::Framed;
use utils::MPCodec;
use path::Path;
use resolver_server::{ToResolver, FromResolver, ClientHello, ServerHello};
use error::*;

static TTL: u64 = 600;
static LINGER: u64 = 10;

type FromCon = oneshot::Sender<Result<FromResolver>>;
type ToCon = (ToResolver, FromCon);

pub trait Readable {}
pub trait Writeable {}

pub struct ReadOnly {}

impl Readable for ReadOnly {}

pub struct ReadWrite {}

impl Readable for ReadWrite {}
imple Writeable for ReadWrite {}

#[derive(Clone)]
pub struct Resolver<R> {
    sender: mpsc::Sender<ToCon>,
    kind: PhantomData<R>
};

impl<R> Resolver<R> {
    async fn send(&self, m: ToResolver) -> Result<FromResolver> {
        let (tx, rx) = oneshot::channel();
        let notify = {
            let mut t = self.0.lock().unwrap();
            t.never_sent.push_back((m, tx));
            if t.notify.is_some() { t.notify.as_ref().unwrap().clone() }
            else {
                let (tx, rx) = mpsc::channel(100);
                t.notify = Some(tx.clone());
                spawn(start_connection(self.downgrade(), rx));
                tx
            }
        };
        async {
            let _ = await!(notify.send(()))?;
            match await!(rx)? {
                FromResolver::Error(s) => bail!(ErrorKind::ResolverError(s)),
                m => Ok(m)
            }
        }
    }

    pub fn new_rw(resolver: SocketAddr, publisher: SocketAddr) -> Resolver<ReadWrite> {
        let (sender, receiver) = mpsc::channel(10);
        task::spawn(connection(receiver, resolver, Some(publisher)));
        Resolver { sender, kind: PhantomData }
    }

    pub fn new_ro(resolver: SocketAddr) -> Resolver<ReadOnly> {
        let (sender, receiver) = mpsc::channel(10);
        task::spawn(connection(receiver, resolver, None));
        Resolver { sender, kind: PhantomData }
    }
}
 
impl<R: Readable> Resolver<R> {
    pub async fn resolve(self, paths: Vec<Path>) -> Result<Vec<Vec<SocketAddr>>> {
        match self.send(ToResolver::Resolve(paths)).await? {
            FromResolver::Resolved(ports) => Ok(ports),
            _ => Err(Error::from(ErrorKind::ResolverUnexpected))
        }
    }

    pub async fn list(self, p: Path) -> Result<Vec<Path>> {
        match self.send(ToResolver::List(p)).await? {
            FromResolver::List(v) => Ok(v),
            _ => Err(Error::from(ErrorKind::ResolverUnexpected))
        }
    }
}

impl <R: Writeable> Resolver<R> {
    pub async fn publish(self, paths: Vec<Path>) -> Result<()> {
        match self.send(ToResolver::Publish(paths)).await? {
            FromResolver::Published => Ok(()),
            _ => Err(Error::from(ErrorKind::ResolverUnexpected))
        }
    }

    pub fn unpublish(self, paths: Vec<Path>) -> Result<()> {
        match self.send(ToResolver::Unpublish(paths)).await? {
            FromResolver::Unpublished => Ok(()),
            _ => Err(Error::from(ErrorKind::ResolverUnexpected))
        }
    }
}

type Con<'a> = Framed<TcpStream, MPCodec<&'a ToResolver, FromResolver>>;

macro_rules! or_continue {
    (e:expr) => {
        match $e {
            Err(_) => continue,
            Ok(x) => x
        }
    }
}

async fn connect(
    resolver: SocketAddr,
    publisher: Option<SocketAddr>,
    published: &HashSet<Path>,
) -> Con {
    let mut backoff = 0;
    loop {
        if backoff > 0 {
            task::sleep(Duration::from_secs(backoff)).await;
        }
        backoff += 1;
        let mut con = or_continue!(TcpStream::connect(&addr).await);
        let mut con = Framed::new(con, MPCodec::<ToResolver, FromResolver>::new());
        let hello = match publisher {
            None => ClientHello::ReadOnly,
            Some(write_addr) => ClientHello::ReadWrite {ttl: TTL, write_addr},
        };
        or_continue!(con.send(&hello).await);
        let hello = or_continue!(con.next().await);
        if !ttl_expired {
            break con
        } else {
            let paths = published.iter().cloned().collect();
            or_continue!(con.send(&ToResolver::Publish(paths)).await);
            match or_continue!(con.next().await) {
                FromResolver::Published => break con,
                _ => continue
            }
        }
    }
}

async fn connection(
    receiver: mpsc::Receiver<ToCon>,
    resolver: SocketAddr,
    publisher: Option<SocketAddr>
) {
    enum M {
        TimeToHB,
        TimeToDC,
        Msg(ToCon),
        Stop,
    }
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
        match hb.race(dc).race(msg) {
            M::Stop => break,
            M::TimeToDC => { con = None; }
            M::TimeToHB => {
                con = Some(connect(resolver, publisher, &published).await);
            }
            M::Msg((m, reply)) => {
                let con = match con {
                    Some(ref mut c) => c,
                    None => {
                        con = Some(connect(resolver, publisher, &published).await);
                        con.as_mut().unwrap()
                    }
                };
                let r = con.send(&m).err_into()
                    .and_then(|()| con.next().err_into())
                    .await;
                if let (ToResolver::Publish(p), Ok(FromResolver::Published)) = (m, r) {
                    published.extend(p.into_iter());
                }
                let _ = reply.send(r);
            }
        }
    }
}
