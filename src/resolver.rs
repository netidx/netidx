use futures::{prelude::*, sync::oneshot, sync::mpsc};
use tokio::{self, spawn, prelude::*, net::TcpStream};
use tokio_io::io::ReadHalf;
use tokio_timer::{Interval, sleep};
use std::{
  result, io::BufReader, net::SocketAddr, mem,
  collections::VecDeque, sync::{Arc, Weak, Mutex},
  time::Duration,
};
use path::Path;
use serde_json;
use line_writer::LineWriter;
use resolver_server::{ToResolver, FromResolver, ClientHello, ServerHello};
use resolver_store::{Store, Published};
use uuid::Uuid;
use error::*;

static TTL: i64 = 600;
static LINGER: i64 = 10;

struct ResolverInner {
  queued: VecDeque<(ToResolver, oneshot::Sender<FromResolver>)>,
  published: Store,
  our_id: Uuid,
  address: SocketAddr,
  notify: Option<mpsc::Sender<()>>
}

#[derive(Clone)]
struct Resolver(Arc<Mutex<ResolverInner>>);

#[derive(Clone)]
struct ResolverWeak(Weak<Mutex<ResolverInner>>);

impl ResolverWeak {
  fn upgrade(&self) -> Option<Resolver> { self.0.upgrade().map(|r| Resolver(r)) }
}

impl Resolver {
  pub fn new(address: SocketAddr) -> Resolver {
    Resolver(Arc::new(Mutex::new(ResolverInner {
      queued: VecDeque::new(),
      published: Store::new(),
      our_id: Uuid::new_v4(),
      address,
      notify: None
    })))
  }

  fn downgrade(&self) -> ResolverWeak { ResolverWeak(self.0.downgrade()) }

  #[async]
  fn send(self, m: ToResolver) -> Result<FromResolver> {
    let (tx, rx) = oneshot::channel();
    let notify = {
      let mut t = self.0.lock().unwrap();
      t.queued.push_back((m, tx));
      if t.notify.is_some() { t.notify.as_ref().unwrap().clone() }
      else {
        let (tx, rx) = mpsc::channel(100);
        t.notify = Some(tx.clone());
        spawn(start_connection(self.downgrade(), tx));
        rx
      }
    };
    let _ = await!(notify.send(()))?;
    match await!(rx)? {
      FromResolver::Error(s) => bail!(ErrorKind::ResolverError(s)),
      m => Ok(m)
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
    match await!(self.send(ToResolver::Publish(path, port))) {
      FromResolver::Published => Ok(()),
      _ => Err(Error::from(ErrorKind::ResolverUnexpected))
    }
  }

  #[async]
  pub fn unpublish(self, path: Path, port: SocketAddr) {
    match await!(self.send(ToResolver::Unpublish(path, port))) {
      FromResolver::Unpublished => Ok(()),
      _ => Err(Error::from(ErrorKind::ResolverUnexpected))
    }
  }
}

fn resend(
  t: &ResolverWeak,
  writer: &LineWriter<TcpStream>,
  hello: &str
) -> Result<usize> {
  let hello = serde_json::from_str::<ServerHello>(hello)?;
  match hello {
    None => bail!("no hello from server"),
    Some(ServerHello {ttl_expired}) => {
      match t.upgrade() {
        None => bail!("we are dead"),
        Some(t) => {
          let mut t = t.0.lock().unwrap();
          let mut resent = 0;
          if ttl_expired {
            for (path, published) in t.store.iter() {
              match published {
                Published::Empty => (),
                Published::One(addr, n) => {
                  resent += n;
                  let m = ToResolver::Publish(path.clone(), addr);
                  for _ in 0..n { writer.write_one(serde_json::to_vec(&m)?) }
                },
                Published::Many(addrs) => {
                  for (addr, n) in addrs.iter() {
                    resent += n;
                    let m = ToResolver::Publish(path.clone(), addr);
                    for _ in 0..n { writer.write_one(serde_json::to_vec(&m)?) }
                  }
                }
              }
            }
          }
          for (m, _) in t.queued.iter() { writer.write_one(serde_json::to_vec(m)?) }
          resent
        }
      }
    }
  }
}

#[async]
pub fn client_loop(
  t: ResolverWeak,
  con: TcpStream,
  notify: mpsc::Receiver<()>
) -> Result<()> {
  enum C { Notify, Wakeup(Instant), Msg(String) };
  let (rx, tx) = con.split();
  let writer = LineWriter(tx);
  let msgs = tokio::io::lines(BufReader::new(rx)).map_err(|e| Error::from(e));
  let notify = notify.then(|r| match r {
    Ok(()) => Ok(C::Notify),
    Err(_) => Err(Error::from("ipc err"))
  });
  let now = Instant::now();
  let wait = Duration::from_secs(LINGER);
  let wakeup = Interval::new(now, wait).then(|r| match r {
    Ok(i) => C::Wakeup(i),
    Err(e) => Err(Error::from(e))
  });
  match t.upgrade() {
    None => return Ok(()),
    Some(t) => {
      let mut t = t.0.lock().unwrap();
      let hello = ClientHello {ttl: TTL, uuid: t.our_id};
      writer.write_one(serde_json::to_vec(&hello))
    }
  }
  await!(writer.clone().flush())?;
  let (hello, msgs) = await!(msgs.into_future()).map_err(|(e, _)| e)?;
  let (mut resent, mut send_upto) = resend(&t, &writer, &hello)?;
  await!(writer.clone().flush())?;
  let msgs = msgs.map(|m| C::Msg(m));
  let mut last = now;
  #[async]
  for msg in msgs {
    match msg {
      C::Wakeup(now) => if now - last > Duration::from_secs(TTL) { break },
      C::Notify => {
        
      },
      C::Msg(msg) => {
        let t = t.upgrade().ok_or_else(|| Error::from("client dropped"))?;
        let msg = serde_json::from_str(&msg)?;
        let ret = t.0.lock().unwrap().queued.pop_front();
        match ret {
          Some(ret) => ret.send(msg).map_err(|_| Error::from("ipc error"))?,
          None => bail!("got unsolicited message from resolver")
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
            match await!(TcpStream::connect(&addr)) {
              Ok(con) => break con,
              Err(_) => {
                await!(sleep(Duration::from_secs(backoff))).map_err(|_| ())?;
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
        if t.queued.len() == 0 {
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
