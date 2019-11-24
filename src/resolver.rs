use futures::{
    future::{FutureExt, RemoteHandle},
    channel::{mpsc, oneshot},
};
use async_std::{
    result, mem, task, future,
    io::BufReader,
    net::{SocketAddr, TcpStream},
    collections::VecDeque,
    sync::{Arc, Weak, Mutex},
    time::{Duration, Instant},
};
use futures_codec::Framed;
use futures_cbor_codec::Codec;
use path::Path;
use resolver_server::{ToResolver, FromResolver, ClientHello, ServerHello};
use resolver_store::{Store, Published};
use uuid::Uuid;
use error::*;

static TTL: u64 = 600;
static LINGER: u64 = 10;

type FromCon = oneshot::Sender<Result<FromResolver>>;
type ToCon = (ToResolver, FromCon);

#[derive(Clone)]
struct Resolver(mpsc::Sender<ToCon>);
 
impl Resolver {
    pub fn new(address: SocketAddr) -> Resolver {
        let (tx, rx) = mpsc::channel(10);
        task::spawn(connection(rx, address));
        Resolver(tx)
    }

    fn downgrade(&self) -> ResolverWeak { ResolverWeak(Arc::downgrade(&self.0)) }

    fn send(self, m: ToResolver) -> impl Future<Item=FromResolver, Error=Error> {
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
        async_block! {
            let _ = await!(notify.send(()))?;
            match await!(rx)? {
                FromResolver::Error(s) => bail!(ErrorKind::ResolverError(s)),
                m => Ok(m)
            }
        }
    }

    #[async]
    pub fn resolve(self, p: Path) -> Result<Vec<SocketAddr>> {
        match await!(self.send(ToResolver::Resolve(p)))? {
            FromResolver::Resolved(port) => Ok(port),
            _ => Err(Error::from(ErrorKind::ResolverUnexpected))
        }
    }

    #[async]
    pub fn list(self, p: Path) -> Result<Vec<Path>> {
        match await!(self.send(ToResolver::List(p)))? {
            FromResolver::List(v) => Ok(v),
            _ => Err(Error::from(ErrorKind::ResolverUnexpected))
        }
    }

    #[async]
    pub fn publish(self, path: Path, port: SocketAddr) -> Result<()> {
        match await!(self.send(ToResolver::Publish(path, port)))? {
            FromResolver::Published => Ok(()),
            _ => Err(Error::from(ErrorKind::ResolverUnexpected))
        }
    }

    #[async]
    pub fn unpublish(self, path: Path, port: SocketAddr) -> Result<()> {
        match await!(self.send(ToResolver::Unpublish(path, port)))? {
            FromResolver::Unpublished => Ok(()),
            _ => Err(Error::from(ErrorKind::ResolverUnexpected))
        }
    }
}

#[async]
fn connection_loop(
    t: ResolverWeak,
    con: TcpStream,
    notify: mpsc::Receiver<()>
) -> Result<()> {
    enum C { Notify, Wakeup(Instant), Msg(String) };
    let (rx, tx) = con.split();
    let writer : LineWriter<Vec<u8>, TcpStream> = LineWriter::new(tx);
    let msgs = tokio::io::lines(BufReader::new(rx)).map_err(|e| Error::from(e));
    let notify = notify.then(|r| match r {
        Ok(()) => Ok(C::Notify),
        Err(_) => Err(Error::from("ipc err"))
    });
    let now = Instant::now();
    let wait = Duration::from_secs(LINGER);
    let wakeup = Interval::new(now, wait).then(|r| match r {
        Ok(i) => Ok(C::Wakeup(i)),
        Err(e) => Err(Error::from(e))
    });
    match t.upgrade() {
        None => return Ok(()),
        Some(t) => {
            let mut t = t.0.lock().unwrap();
            let hello = ClientHello {ttl: TTL as i64, uuid: t.our_id};
            writer.write_one(serde_json::to_vec(&hello)?)
        }
    }
    await!(writer.clone().flush())?;
    let (hello, msgs) = await!(msgs.into_future()).map_err(|(e, _)| e)?;
    let hello = hello.ok_or_else(|| Error::from("no hello from server"))?;
    let mut resent = resend(&t, &writer, &hello)?;
    await!(writer.clone().flush())?;
    let msgs = msgs.map(|m| C::Msg(m));
    let mut last_count : usize = 0;
    let mut count : usize = 0;
    #[async]
    for msg in msgs.select(notify).select(wakeup) {
        match msg {
            C::Wakeup(_) => {
                let t = t.upgrade().ok_or_else(|| Error::from("client dropped"))?;
                let t = t.0.lock().unwrap();
                if last_count == count
                    && t.waiting_reply.len() == 0
                    && t.never_sent.len() == 0
                    && resent == 0 { break }
                last_count = count;
            },
            C::Notify => {
                {
                    count += 1;
                    let t = t.upgrade().ok_or_else(|| Error::from("client dropped"))?;
                    let mut t = t.0.lock().unwrap();
                    while let Some((m, r)) = t.never_sent.pop_front() {
                        writer.write_one(serde_json::to_vec(&m)?);
                        t.waiting_reply.push_back((m, r))
                    }
                }
                await!(writer.clone().flush())?
            },
            C::Msg(msg) => {
                let t = t.upgrade().ok_or_else(|| Error::from("client dropped"))?;
                count += 1;
                if resent > 0 { resent -= 1 }
                else {
                    let m = serde_json::from_str(&msg)?;
                    let mut t = t.0.lock().unwrap();
                    match t.waiting_reply.pop_front() {
                        None => bail!("unsolicited response"),
                        Some((sent, r)) => {
                            match m {
                                m@ FromResolver::Error(..)
                                    | m@ FromResolver::List(..)
                                    | m@ FromResolver::Resolved(..) => {
                                        let _ = r.send(m);
                                    },
                                FromResolver::Published => {
                                    match sent {
                                        ToResolver::Publish(path, addr) => { 
                                            t.published.publish(path, addr);
                                            let _ = r.send(m);
                                        },
                                        ToResolver::List(..)
                                            | ToResolver::Resolve(..)
                                            | ToResolver::Unpublish(..) =>
                                            bail!("unexpected reply")
                                    }
                                },
                                FromResolver::Unpublished => {
                                    match sent {
                                        ToResolver::Unpublish(path, addr) => {
                                            t.published.unpublish(&path, &addr);
                                            let _ = r.send(m);
                                        },
                                        ToResolver::List(..)
                                            | ToResolver::Resolve(..)
                                            | ToResolver::Publish(..) =>
                                            bail!("unexpected reply")
                                    }
                                },
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

async fn heartbeat_loop(r: ResolverWeak, rx: Receiver<()>) {
    loop {
        let activity = rx.recv().map(|o| match o {
            Some(()) => M::ClientActivity,
            None => M::Stop
        });
        match activity.race(hb).race(dc).await {
            M::Stop => break,
            M::Activity => ()
            M::TimeToHB => {
                match r.upgrade() {
                    None => break,
                    Some(r) => {
                        let _ = get_connection(&r);
                    }
                }
            }
            M::TimeToDC => {
                match r.upgrade() {
                    None => break,
                    Some(r) => {
                        let inner = r.lock().unwrap();
                        inner.con = ConnectionState::NotConnected;
                    }
                }
            }
        }
    }
}

type Con = Framed<TcpStream, Codec<FromResolver, ToResolver>>;

async fn resend(hello: ServerHello, published: &Store, con: &mut Con) -> Result<usize> {
    let mut resent = 0;
    if ttl_expired {
        let msgs = Vec::new();
        for (path, published) in published.iter() {
            match published {
                Published::Empty => (),
                Published::One(addr, n) => {
                    resent += *n;
                    for _ in 0..*n {
                        msgs.push(ToResolver::Publish(path.clone(), *addr));
                    }
                },
                Published::Many(addrs) => {
                    for (addr, n) in addrs.iter() {
                        resent += n;
                        let m = ToResolver::Publish(path.clone(), *addr);
                        for _ in 0..*n { writer.write_one(serde_json::to_vec(&m)?) }
                    }
                }
            }
        }
    }
    while let Some((m, r)) = t.never_sent.pop_front() {
        t.waiting_reply.push_back((m, r));
    }
    for (m, _) in t.waiting_reply.iter() {
        writer.write_one(serde_json::to_vec(m)?);
    }
    Ok(resent)
}

async fn connect(addr: SocketAddr, our_id: Uuid, store: &Store) -> Con {
    let mut backoff = 0;
    loop {
        if backoff > 0 {
            task::sleep(Duration::from_secs(backoff)).await;
        }
        backoff += 1;
        let mut con = {
            loop {
                match TcpStream::connect(&addr).await {
                    Ok(con) => break con,
                    Err(_) => (),
                }
            }
        };
        let mut con = Framed::new(con, Codec::new());
        match con.send(ClientHello {ttl: TTL as i64, uuid: our_id}).await {
            Err(_) => (),
            Ok(()) => match con.next() {
                Err(_) => (),
                Ok(hello_reply) => {
                    
                }
            }
        }
    }
}

async fn connection(rx: mpsc::Receiver<ToCon>, addr: SocketAddr) {
    enum M {
        TimeToHB,
        TimeToDC,
        Msg(ToCon),
        Stop,
    }
    let our_id = Uuid::new_v4();
    let published = Store::new();
    let mut pending: VecDeque<FromCon> = VecDeque::new();
    let mut con: Option<TcpStream> = None;
    let ttl = Duration::from_secs(TTL / 2);
    let linger = Duration::from_secs(LINGER);

    loop {
        let hb = future::ready(M::TimeToHB).delay(ttl);
        let dc = future::ready(M::TimeToDC).delay(linger);
        let msg = rx.next().map(|m| match m {
            None => M::Stop,
            Some(m) => M::Msg(m)
        });

        match hb.race(dc).race(msg) {
            M::TimeToHB => 
        }
    }
}
