use futures::prelude::*;
use futures::unsync::oneshot;
use tokio;
use tokio::prelude::*;
use tokio::executor::current_thread::spawn;
use tokio::net::TcpStream;
use tokio_io::io::ReadHalf;
use std::io::BufReader;
use std::net::SocketAddr;
use std::rc::{Rc, Weak};
use std::cell::{RefCell, Cell};
use std::result;
use std::collections::VecDeque;
use path::Path;
use serde_json;
use line_writer::LineWriter;
use resolver_server::{ToResolver, FromResolver};
use error::*;

struct ResolverInner {
  writer: LineWriter<Vec<u8>, TcpStream>,
  queued: VecDeque<oneshot::Sender<FromResolver>>,
  stop: Cell<Option<oneshot::Sender<()>>>,
}

impl Drop for ResolverInner {
  fn drop(&mut self) {
    if let Some(stop) = self.stop.replace(None) {
      let _ = stop.send(());
    }
  }
}

#[derive(Clone)]
struct ResolverWeak(Weak<RefCell<ResolverInner>>);

impl ResolverWeak {
  fn upgrade(&self) -> Option<Resolver> {
    Weak::upgrade(&self.0).map(|r| Resolver(r))
  }
}

#[derive(Clone)]
pub struct Resolver(Rc<RefCell<ResolverInner>>);

impl Resolver {
  fn downgrade(&self) -> ResolverWeak {
    ResolverWeak(Rc::downgrade(&self.0))
  }

  #[async(boxed)]
  pub fn new(addr: SocketAddr) -> Result<Resolver> {
    let con = await!(TcpStream::connect(&addr))?;
    let (rx, tx) = con.split();
    let (stop_tx, stop_rx) = oneshot::channel();
    let inner = ResolverInner {
      writer: LineWriter::new(tx),
      queued: VecDeque::new(),
      stop: Cell::new(Some(stop_tx))
    };
    let t = Resolver(Rc::new(RefCell::new(inner)));
    spawn(start_client(t.downgrade(), rx, stop_rx));
    Ok(t)
  }

  #[async]
  fn send(self, m: ToResolver) -> Result<FromResolver> {
    let (tx, rx) = oneshot::channel();
    {
      let msg = serde_json::to_vec(&m)?;
      let mut t = self.0.borrow_mut();
      t.queued.push_back(tx);
      t.writer.write(msg)
    }
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
    match await!(self.send(ToResolver::Publish(path, port)))? {
      FromResolver::Published => Ok(()),
      _ => Err(Error::from(ErrorKind::ResolverUnexpected))
    }
  }

  #[async]
  pub fn unpublish(self, path: Path, port: SocketAddr) -> Result<()> {
    match await!(self.send(ToResolver::Unpublish(path, port)))? {
      FromResolver::Unpublished => Ok(()),
      _ => bail!(ErrorKind::ResolverUnexpected)
    }
  }
}

#[async]
fn client_loop(
  t: ResolverWeak,
  rx: ReadHalf<TcpStream>,
  stop: oneshot::Receiver<()>
) -> Result<()> {
  enum C { Stop, Msg(String) };
  let msgs = tokio::io::lines(BufReader::new(rx)).then(|l| match l {
    Ok(l) => Ok(C::Msg(l)),
    Err(e) => Err(Error::from(e))
  });
  let stop = stop.into_stream().then(|r| match r {
    Ok(()) => Ok(C::Stop),
    Err(_) => Err(Error::from("ipc err"))
  });
  #[async]
  for msg in msgs.select(stop) {
    match msg {
      C::Stop => break,
      C::Msg(msg) => {
        let t = t.upgrade().ok_or_else(|| Error::from("client dropped"))?;
        let msg = serde_json::from_str(&msg)?;
        let ret = t.0.borrow_mut().queued.pop_front();
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
fn start_client(
  t: ResolverWeak,
  rx: ReadHalf<TcpStream>,
  stop: oneshot::Receiver<()>
) -> result::Result<(), ()> {
  let r = await!(client_loop(t.clone(), rx, stop));
  let msg =
    FromResolver::Error(
      match r {
        Ok(()) => String::from("connection was closed"),
        Err(e) => format!("{}", e)
      });
  if let Some(t) = t.upgrade() {
    t.0.borrow().writer.shutdown();
    while let Some(c) = t.0.borrow_mut().queued.pop_front() {
      let _ = c.send(msg.clone());
    }
  }
  Ok(())
}
