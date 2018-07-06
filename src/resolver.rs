use futures::{prelude::*, sync::oneshot, sync::mpsc};
use tokio::{self, spawn, prelude::*, net::TcpStream};
use tokio_timer::{Interval, sleep};
use std::{
    result, io::BufReader, net::SocketAddr, mem,
    collections::VecDeque, sync::{Arc, Weak, Mutex},
    time::{Duration, Instant},
};
use path::Path;
use serde_json;
use line_writer::LineWriter;
use resolver_server::{ToResolver, FromResolver, ClientHello, ServerHello};
use resolver_store::{Store, Published};
use uuid::Uuid;
use error::*;

static TTL: u64 = 600;
static LINGER: u64 = 10;

struct ResolverInner {
    waiting_reply: VecDeque<(ToResolver, oneshot::Sender<FromResolver>)>,
    never_sent: VecDeque<(ToResolver, oneshot::Sender<FromResolver>)>,
    published: Store,
    our_id: Uuid,
    address: SocketAddr,
    notify: Option<mpsc::Sender<()>>,
    dead: Option<oneshot::Sender<()>>,
}

impl Drop for ResolverInner {
    fn drop(&mut self) {
        let mut dead = None;
        mem::swap(&mut dead, &mut self.dead);
        if let Some(dead) = dead { let _ = dead.send(()); }
    }
}

#[derive(Clone)]
pub struct Resolver(Arc<Mutex<ResolverInner>>);

#[derive(Clone)]
struct ResolverWeak(Weak<Mutex<ResolverInner>>);

impl ResolverWeak {
    fn upgrade(&self) -> Option<Resolver> { self.0.upgrade().map(|r| Resolver(r)) }
}

impl Resolver {
    pub fn new(address: SocketAddr) -> Resolver {
        let (tx, rx) = oneshot::channel();
        let t = Resolver(Arc::new(Mutex::new(ResolverInner {
            waiting_reply: VecDeque::new(),
            never_sent: VecDeque::new(),
            published: Store::new(),
            our_id: Uuid::new_v4(),
            address,
            notify: None,
            dead: Some(tx),
        })));
        spawn(heartbeat_loop(t.downgrade(), rx));
        t
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

fn resend(
    t: &ResolverWeak,
    writer: &LineWriter<Vec<u8>, TcpStream>,
    hello: &str
) -> Result<usize> {
    let ServerHello {ttl_expired} = serde_json::from_str::<ServerHello>(hello)?;
    let t = t.upgrade().ok_or_else(|| Error::from("dropped"))?;
    let mut t = t.0.lock().unwrap();
    let mut resent = 0;
    if ttl_expired {
        for (path, published) in t.published.iter() {
            match published {
                Published::Empty => (),
                Published::One(addr, n) => {
                    resent += *n;
                    let m = ToResolver::Publish(path.clone(), *addr);
                    for _ in 0..*n { writer.write_one(serde_json::to_vec(&m)?) }
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

#[async]
fn start_connection(
    t: ResolverWeak,
    mut notify: mpsc::Receiver<()>
) -> result::Result<(), ()> {
    loop {
        let con = {
            let mut backoff = 1;
            loop {
                match t.upgrade() {
                    None => return Ok(()),
                    Some(t) => {
                        let addr = t.0.lock().unwrap().address;
                        match await!(TcpStream::connect(&addr)) {
                            Ok(con) => break con,
                            Err(_) => {
                                await!(sleep(Duration::from_secs(backoff)))
                                    .map_err(|_| ())?;
                                backoff += 1
                            }
                        }
                    }
                }
            }
        };
        let _ = await!(connection_loop(t.clone(), con, notify));
        match t.upgrade() {
            None => return Ok(()),
            Some(t) => {
                let mut t = t.0.lock().unwrap();
                if t.never_sent.len() == 0 || t.waiting_reply.len() == 0 {
                    t.notify = None;
                    return Ok(())
                } else {
                    let (tx, rx) = mpsc::channel(100);
                    t.notify = Some(tx);
                    notify = rx;
                }
            }
        }
    }
}

#[async]
fn heartbeat_loop(t: ResolverWeak, dead: oneshot::Receiver<()>) -> result::Result<(), ()> {
    enum M { Heartbeat, Stop }
    let wake =
        Interval::new(Instant::now(), Duration::from_secs(TTL / 2)).then(|r| match r {
            Ok(_) => Ok(M::Heartbeat),
            Err(_) => Err(())
        });
    let stop = dead.into_stream().then(|r| match r {
        Ok(()) => Ok(M::Stop),
        Err(_) => Err(())
    });
    #[async]
    for msg in wake.select(stop) {
        match msg {
            M::Stop => break,
            M::Heartbeat => {
                let t = t.upgrade().ok_or_else(|| ())?;
                let mut ti = t.0.lock().unwrap();
                if ti.notify.is_none() {
                    let (tx, rx) = mpsc::channel(100);
                    ti.notify = Some(tx);
                    spawn(start_connection(t.downgrade(), rx));
                }
            },
        }
    }
    Ok(())
}
