use std::{
  self, result, marker::PhantomData, io::BufReader, convert::AsRef,
  collections::HashMap, net::{Ipv4Addr, SocketAddrV4, SocketAddr},
  sync::{Arc, Weak, RwLock},
};
use rand::{self, distributions::IndependentSample};
use futures::{prelude::*, sync::{oneshot, mpsc::{channel, Receiver, Sender}}};
use serde::{Serialize, de::DeserializeOwned};
use serde_json;
use tokio::{self, prelude::*, spawn, net::{TcpListener, TcpStream}};
use tokio_io::io::ReadHalf;
use path::Path;
use resolver_client::Resolver;
use line_writer::LineWriter;
use utils::Encoded;
use error::*;

// TODO
// * add a handler for lazy publishing (delegated subtrees)

static MAX_CLIENTS: usize = 768;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ToPublisher {
  Subscribe(Path),
  // the payload will be on the next line after the set tag
  Set(Path),
  Unsubscribe(Path)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FromPublisher {
  NoSuchValue(Path),
  Unsubscribed(Path),
  // the payload will be on the next line after the message tag
  Message(Path)
}

struct PublishedUntypedInner {
  publisher: Publisher,
  path: Path,
  message_header: Encoded,
  current: Encoded,
  on_write: Option<Box<FnMut(String) -> () + Send + Sync>>,
  subscribed: HashMap<SocketAddr, LineWriter<Encoded, TcpStream>>,
}

impl Drop for PublishedUntypedInner {
  fn drop(&mut self) {
    let mut t = self.publisher.0.write().unwrap();
    t.published.remove(&self.path);
    let r = t.resolver.clone();
    let addr = t.addr;
    drop(t);
    let path = self.path.clone();
    async_block! { await!(r.unpublish(path, addr)) };
    let msg =
      serde_json::to_vec(&FromPublisher::Unsubscribed(self.path.clone()))
      .expect("failed to encode unsubscribed message");
    let msg = Encoded::new(msg);
    for (_, c) in self.subscribed.iter() { c.write_one(msg.clone()) }
  }
}

#[derive(Clone)]
struct PublishedUntypedWeak(Weak<RwLock<PublishedUntypedInner>>);

impl PublishedUntypedWeak {
  fn upgrade(&self) -> Option<PublishedUntyped> {
    Weak::upgrade(&self.0).map(|r| PublishedUntyped(r))
  }
}

#[derive(Clone)]
struct PublishedUntyped(Arc<RwLock<PublishedUntypedInner>>);

impl PublishedUntyped {
  fn downgrade(&self) -> PublishedUntypedWeak {
    PublishedUntypedWeak(Arc::downgrade(&self.0))
  }
}

#[derive(Clone)]
pub struct Published<T>(PublishedUntyped, PhantomData<T>);

impl<T: Serialize> Published<T> {
  pub fn update(&self, v: &T) -> Result<()> {
    let t = (self.0).0.read().unwrap();
    let c = t.message_header.clone();
    let v = Encoded::new(serde_json::to_vec(v)?);
    for (_, cl) in t.subscribed.iter() { cl.write_two(c.clone(), v.clone()) }
    Ok(())
  }

  pub fn on_write<U: DeserializeOwned + 'static>(
    &self,
    mut f: Box<FnMut(result::Result<U, serde_json::Error>) -> () + Send + Sync>
  ) {
    let f = Box::new(move |s: String| (f)(serde_json::from_str::<U>(s.as_ref())));
    (self.0).0.write().unwrap().on_write = Some(f);
  }
}

struct PublisherInner {
  dead: bool,
  addr: SocketAddr,
  stop_accept: Sender<()>,
  clients: HashMap<SocketAddr, LineWriter<Encoded, TcpStream>>,
  wait_clients: Vec<oneshot::Sender<()>>,
  published: HashMap<Path, PublishedUntypedWeak>,
  resolver: Resolver
}

impl PublisherInner {
  fn shutdown(&mut self) {
    if !self.dead {
      self.dead = true;
      let published = self.published.clone();
      let addr = self.addr;
      let r = self.resolver.clone();
      let stop = self.stop_accept.clone();
      for (_, c) in self.clients.iter() { c.shutdown() }
      async_block! {
        let _ = await!(stop.send(()));
        for (s, _) in published.into_iter() {
          let _ = await!(r.clone().unpublish(s, addr));
        }
        let r: Result<()> = Ok(());
        r
      };
    }
  }
}

impl Drop for PublisherInner {
  fn drop(&mut self) { self.shutdown() }
}

#[derive(Clone)]
struct PublisherWeak(Weak<RwLock<PublisherInner>>);

impl PublisherWeak {
  fn upgrade(&self) -> Option<Publisher> {
    Weak::upgrade(&self.0).map(|r| Publisher(r))
  }
}

#[derive(Clone, Copy, Debug)]
pub enum BindCfg {
  Local,
  Any,
  Exact(SocketAddr)
}

#[derive(Clone)]
pub struct Publisher(Arc<RwLock<PublisherInner>>);

impl Publisher {
  fn downgrade(&self) -> PublisherWeak {
    PublisherWeak(Arc::downgrade(&self.0))
  }

  pub fn new(
    resolver: Resolver,
    bind_cfg: BindCfg
  ) -> Result<Publisher> {
    let (addr, listener) =
      match bind_cfg {
        BindCfg::Exact(addr) => (addr, TcpListener::bind(&addr)?),
        BindCfg::Local | BindCfg::Any => {
          let mut rng = rand::thread_rng();
          let range = rand::distributions::Range::new(0u16, 10u16);
          let ip = match bind_cfg {
            BindCfg::Exact(_) => unreachable!(),
            BindCfg::Local => Ipv4Addr::new(127, 0, 0, 1),
            BindCfg::Any => Ipv4Addr::new(0, 0, 0, 0)
          };
          let mut port = 5000;
          let mut listener = None;
          let mut addr = SocketAddr::V4(SocketAddrV4::new(ip, port));
          while listener.is_none() && port < 32768 {
            port += range.ind_sample(&mut rng);
            addr = SocketAddr::V4(SocketAddrV4::new(ip, port));
            match TcpListener::bind(&addr) {
              Ok(l) => listener = Some(l),
              Err(e) => if e.kind() != std::io::ErrorKind::AddrInUse { bail!(e) }
            }
          }
          (addr, listener.ok_or_else(|| Error::from("could not find an open port"))?)
        }
      };
    let (send, stop) = channel(1000);
    let pb = PublisherInner {
      addr, resolver,
      dead: false,
      stop_accept: send,
      clients: HashMap::new(),
      published: HashMap::new(),
      wait_clients: Vec::new(),
    };
    let pb = Publisher(Arc::new(RwLock::new(pb)));
    spawn(start_accept(pb.downgrade(), listener, stop));
    Ok(pb)
  }

  pub fn addr(&self) -> SocketAddr { self.0.read().unwrap().addr }

  #[async]
  pub fn publish<T: Serialize + 'static>(
    self, path: Path, init: T
  ) -> Result<Published<T>> {
    let header_msg = FromPublisher::Message(path.clone());
    let inner = PublishedUntypedInner {
      publisher: self.clone(),
      path: path.clone(),
      on_write: None,
      message_header: Encoded::new(serde_json::to_vec(&header_msg)?),
      current: Encoded::new(serde_json::to_vec(&init)?),
      subscribed: HashMap::new()
    };
    let ut = PublishedUntyped(Arc::new(RwLock::new(inner)));
    let mut t = self.0.write().unwrap();
    if t.published.contains_key(&path) { bail!(ErrorKind::AlreadyPublished(path)) }
    else {
      t.published.insert(path.clone(), ut.downgrade());
      let addr = t.addr;
      let resolver = t.resolver.clone();
      drop(t);
      await!(resolver.publish(path, addr))?;
      Ok(Published(ut, PhantomData))
    }
  }
  
  #[async]
  pub fn flush(self) -> Result<()> {
    let flushes =
      self.0.read().unwrap().clients.iter()
      .map(|(_, c)| c.clone().flush())
      .collect::<Vec<_>>();
    for flush in flushes.into_iter() { await!(flush)? }
    Ok(())
  }

  #[async]
  pub fn wait_client(self) -> Result<()> {
    let mut t = self.0.write().unwrap();
    if t.clients.len() > 0 { Ok(()) }
    else {
      let (tx, rx) = oneshot::channel();
      t.wait_clients.push(tx);
      drop(t);
      Ok(await!(rx)?)
    }
  }
}

#[async]
fn client_loop<S>(
  t: PublisherWeak,
  addr: SocketAddr,
  msgs: S,
  writer: LineWriter<Encoded, TcpStream>
) -> Result<()>
where S: Stream<Item=String, Error=Error> + 'static {
  enum C { Msg(String), Stop };
  let stop = writer.clone().on_shutdown().into_stream().then(|_| Ok(C::Stop));
  let msgs = msgs.map(|m| C::Msg(m));
  let mut wait_set: Option<Path> = None;
  #[async]
  for msg in msgs.select(stop) {
    match msg {
      C::Stop => break,
      C::Msg(msg) => {
        let ws = wait_set;
        wait_set = None;
        match ws {
          Some(path) => {
            if let Some(t) = t.upgrade() {
              let pb = t.0.read().unwrap();
              if let Some(ref published) = pb.published.get(&path) {
                if let Some(published) = published.upgrade() {
                  drop(pb);
                  let mut inner = published.0.write().unwrap();
                  if let Some(ref mut on_write) = inner.on_write {
                    (on_write)(msg)
                  }
                }
              }
            }
          },
          None =>
            match serde_json::from_str(&msg)? {
              ToPublisher::Set(path) => { wait_set = Some(path); },
              ToPublisher::Unsubscribe(s) => {
                if let Some(t) = t.upgrade() {
                  let pb = t.0.read().unwrap();
                  if let Some(ref published) = pb.published.get(&s) {
                    if let Some(published) = published.upgrade() {
                      drop(pb);
                      published.0.write().unwrap().subscribed.remove(&addr);
                    }
                  }
                };
                let resp = serde_json::to_vec(&FromPublisher::Unsubscribed(s))?;
                writer.write_one(Encoded::new(resp));
              },
              ToPublisher::Subscribe(s) => {
                fn no(s: Path) -> Result<Encoded> {
                  let v = serde_json::to_vec(&FromPublisher::NoSuchValue(s))?;
                  Ok(Encoded::new(v))
                };
                let resp = match t.upgrade() {
                  None => Err(no(s)?),
                  Some(t) => {
                    let pb = t.0.read().unwrap();
                    match pb.published.get(&s) {
                      None => Err(no(s)?),
                      Some(ref published) => {
                        match published.upgrade() {
                          None => Err(no(s)?),
                          Some(published) => {
                            drop(pb);
                            let mut inner = published.0.write().unwrap();
                            inner.subscribed.insert(addr, writer.clone());
                            let c = inner.message_header.clone();
                            let v = inner.current.clone();
                            Ok((c, v))
                          }
                        }
                      }
                    }
                  }
                };
                match resp {
                  Err(resp) => writer.write_one(resp),
                  Ok((c, v)) => writer.write_two(c, v),
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
fn start_client(
  t: PublisherWeak,
  reader: ReadHalf<TcpStream>,
  writer: LineWriter<Encoded, TcpStream>,
  addr: SocketAddr
) -> result::Result<(), ()> {
  let msgs = tokio::io::lines(BufReader::new(reader)).then(|l| Ok(l?));
  // CR estokes: Do something with this error
  let _ = await!(client_loop(t.clone(), addr, msgs, writer.clone()));
  writer.shutdown();
  if let Some(t) = t.upgrade() {
    let mut pb = t.0.write().unwrap();
    pb.clients.remove(&addr);
    let published = pb.published.clone();
    drop(pb);
    for (_, pv) in published.into_iter() {
      if let Some(pv) = pv.upgrade() {
        let mut inner = pv.0.write().unwrap();
        inner.subscribed.remove(&addr);
      }
    }
  }
  Ok(())
}

#[async]
fn accept_loop(
  t: PublisherWeak,
  serv: TcpListener,
  stop: Receiver<()>
) -> Result<()> {
  enum Combined { Client(TcpStream), Stop };
  let accepted = serv.incoming().map(|x| Combined::Client(x)).map_err(|e| Error::from(e));
  let stop = stop.map(|_| Combined::Stop).map_err(|()| Error::from("ipc error"));
  #[async]
  for msg in accepted.select(stop) {
    let t =
      match t.upgrade() {
        None => return Ok(()),
        Some(t) => t
      };
    match msg {
      Combined::Stop => break,
      Combined::Client(cl) => {
        let mut pb = t.0.write().unwrap();
        if pb.clients.len() < MAX_CLIENTS {
          match cl.peer_addr() {
            Err(_) => (),
            Ok(addr) => {
              cl.set_nodelay(true).unwrap_or(());
              let (read, write) = cl.split();
              let writer = LineWriter::new(write);
              pb.clients.insert(addr, writer.clone());
              for s in pb.wait_clients.drain(0..) { let _ = s.send(()); }
              drop(pb);
              spawn(start_client(t.downgrade(), read, writer, addr));
            }
          }
        }
      }
    }
  }
  Ok(())
}

#[async]
fn start_accept(
  t: PublisherWeak,
  serv: TcpListener,
  stop: Receiver<()>
) -> result::Result<(), ()> {
  // CR estokes: Do something with this error
  let _ = await!(accept_loop(t.clone(), serv, stop));
  if let Some(t) = t.upgrade() { t.0.write().unwrap().shutdown() }
  Ok(())
}
