use futures::{prelude::*, sync::oneshot, sync::mpsc};
use tokio::{self, spawn, prelude::*, net::TcpStream};
use tokio_io::io::ReadHalf;
use std::{
  result, io::BufReader, net::SocketAddr, mem,
  collections::VecDeque, sync::{Arc, Weak, Mutex},
};
use path::Path;
use serde_json;
use line_writer::LineWriter;
use resolver_server::{ToResolver, FromResolver};
use resolver_store::Store;
use pipe::{pipe, Reader, Writer};
use error::*;

enum Queued {
  Read((ToResolver, oneshot::Sender<FromResolver>)),
  Write(ToResolver)
}

#[derive(Clone)]
struct Resolver(Writer<Queued>)

impl Resolver {
  pub fn new(addr: SocketAddr) -> Resolver {
    let (tx, rx) = pipe();
    spawn(start_client(addr, rx));
    Ok(Resolver(tx))
  }

  #[async]
  fn send(self, m: ToResolver) -> Result<FromResolver> {
    let (tx, rx) = oneshot::channel();
    self.0.write(Queued::Read(m, tx));
    await!(self.0.clone().flush())?;
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

  pub fn publish(&self, path: Path, port: SocketAddr) {
    self.0.write(Queued::Write(ToResolver::Publish(path, port)))
  }

  pub fn unpublish(&self, path: Path, port: SocketAddr) {
    self.0.write(Queued::Write(ToResolver::Unpublish(path, port)))
  }

  pub fn flush(self) -> impl Future<Item=(), Error=Error> { self.0.clone().flush() }
}

#[async]
pub fn client_loop(
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

/*
    let con = await!(TcpStream::connect(&addr))?;
    let (rx, tx) = con.split();
    let (stop_tx, stop_rx) = oneshot::channel();
*/
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
    let mut t = t.0.lock().unwrap();
    t.writer.shutdown();
    while let Some(c) = t.queued.pop_front() { let _ = c.send(msg.clone()); }
  }
  Ok(())
}
