use std::{
  self, result, marker::PhantomData, io::BufReader, convert::AsRef,
  collections::HashMap, net::{Ipv4Addr, SocketAddrV4, SocketAddr},
  sync::{Arc, Weak, RwLock, Mutex},
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

/// This is the set of protocol messages that may be sent to the publisher
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ToPublisher {
  /// Subscribe to the specified value, if it is not available the result will be NoSuchValue
  Subscribe(Path),
  /// Write to the specified value, the payload must be the next line after this message
  Set(Path),
  /// Unsubscribe from the specified value, this will always result in an Unsubscibed message
  /// even if you weren't ever subscribed to the value, or it doesn't exist.
  Unsubscribe(Path)
}

/// This is the set of protocol messages that may come from the publisher
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum FromPublisher {
  /// The requested subscription to Path cannot be completed because it doesn't exist
  NoSuchValue(Path),
  /// You have been unsubscriped from Path. This can be the result of an Unsubscribe message,
  /// or it may be sent unsolicited, in the case the value is no longer published, or the
  /// publisher is in the process of shutting down.
  Unsubscribed(Path),
  /// The next line contains an updated value for Path
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
    spawn(async_block! { await!(r.unpublish(path, addr)).map_err(|_| ()) });
    let msg =
      serde_json::to_vec(&FromPublisher::Unsubscribed(self.path.clone()))
      .expect("failed to encode unsubscribed message");
    let msg = Encoded::new(msg);
    for (_, c) in self.subscribed.iter() {
      c.write_one(msg.clone());
      c.flush_nowait();
    }
  }
}

#[derive(Clone)]
struct PublishedUntypedWeak(Weak<Mutex<PublishedUntypedInner>>);

impl PublishedUntypedWeak {
  fn upgrade(&self) -> Option<PublishedUntyped> {
    Weak::upgrade(&self.0).map(|r| PublishedUntyped(r))
  }
}

#[derive(Clone)]
struct PublishedUntyped(Arc<Mutex<PublishedUntypedInner>>);

impl PublishedUntyped {
  fn downgrade(&self) -> PublishedUntypedWeak {
    PublishedUntypedWeak(Arc::downgrade(&self.0))
  }
}

// Err(Dead) should contain the last published value
/// This represents a published value. Internally it is wrapped in an
/// Arc, so cloning it is essentially free. When all references to a
/// given published value have been dropped it will be unpublished
/// from the resolver, subscribers will be notified that no further
/// updates will happen (the updates stream will end, get will return
/// `Err(Dead)`), and subsuquent subscriptions will fail.
#[derive(Clone)]
pub struct Published<T>(PublishedUntyped, PhantomData<T>);

impl<T: Serialize> Published<T> {
  /// Update the published value. For existing subscribers this only
  /// queues the update, it will not be sent out until you call
  /// `flush` on the publisher. This gives you control of batching
  /// multiple updates into as few syscalls as is possible, which can
  /// be important for throughput.
  ///
  /// New subscribers will see the latest value immediatly,
  /// reguardless of whether you've flushed it or not.
  pub fn update(&self, v: &T) -> Result<()> {
    let mut t = (self.0).0.lock().unwrap();
    let c = t.message_header.clone();
    let v = Encoded::new(serde_json::to_vec(v)?);
    t.current = v.clone();
    for (_, cl) in t.subscribed.iter() { cl.write_two(c.clone(), v.clone()) }
    Ok(())
  }

  /// EXPERIMENTAL Register `f` to be called when the subscriber calls
  /// `write` on a value. Sadly, you may not call `update` from this
  /// closure, since you hold the `Published` lock. This shortcoming
  /// may be addressed in the future, or this api may be completely
  /// changed or just removed.
  pub fn on_write<U: DeserializeOwned + 'static>(
    &self,
    mut f: Box<FnMut(result::Result<U, serde_json::Error>) -> () + Send + Sync>
  ) {
    let f = Box::new(move |s: String| (f)(serde_json::from_str::<U>(s.as_ref())));
    (self.0).0.lock().unwrap().on_write = Some(f);
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
      spawn(async_block! {
        let _ = await!(stop.send(()));
        for (s, _) in published.into_iter() {
          let _ = await!(r.clone().unpublish(s, addr));
        }
        Ok(())
      });
    }
  }
}

impl Drop for PublisherInner {
  fn drop(&mut self) {
    self.shutdown()
  }
}

#[derive(Clone)]
struct PublisherWeak(Weak<RwLock<PublisherInner>>);

impl PublisherWeak {
  fn upgrade(&self) -> Option<Publisher> {
    Weak::upgrade(&self.0).map(|r| Publisher(r))
  }
}

/// Control how the publisher picks a `SocketAddr` to bind to
#[derive(Clone, Copy, Debug)]
pub enum BindCfg {
  /// Bind to `127.0.0.1`, automatically pick an unused port starting
  /// at 5000 and ending at the ephemeral port range. Error if no
  /// unused port can be found.
  Local,
  /// Bind to `0.0.0.0`, automatically pick an unused port as in `Local`.
  Any,
  /// Bind to the specifed `SocketAddr`, error if it is in use.
  Exact(SocketAddr)
}

/// Publisher allows to publish values, and gives central control of
/// flushing queued updates. Publisher is internally wrapped in an
/// Arc, so cloning it is virtually free. When all references to
/// published values, and all references to publisher have been
/// dropped the publisher will shutdown the listener.
#[derive(Clone)]
pub struct Publisher(Arc<RwLock<PublisherInner>>);

impl Publisher {
  fn downgrade(&self) -> PublisherWeak {
    PublisherWeak(Arc::downgrade(&self.0))
  }

  // CR estokes: shouldn't this be async? if for no other reason than
  // to signal that the listener is ready to receive subscribers ...
  /// Create a new publisher, and start the listener.
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

  /// get the `SocketAddr` that publisher is bound to
  pub fn addr(&self) -> SocketAddr { self.0.read().unwrap().addr }

  /// Publish `Path` with initial value `init`. Subscribers can
  /// subscribe to the value as soon as the future returned by this
  /// function is ready.
  ///
  /// Multiple publishers may publish values at the same path. See
  /// `subscriber::Subscriber::subscribe` for details.
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
    let ut = PublishedUntyped(Arc::new(Mutex::new(inner)));
    let (addr, resolver) = {
      let mut t = self.0.write().unwrap();
      if t.published.contains_key(&path) { bail!(ErrorKind::AlreadyPublished(path)) }
      else {
        t.published.insert(path.clone(), ut.downgrade());
        (t.addr, t.resolver.clone())
      }
    };
    await!(resolver.publish(path, addr))?;
    Ok(Published(ut, PhantomData))
  }
  
  /// Send all queued updates out to subscribers. When the future
  /// returned by this function is ready all data has been flushed to
  /// the underlying OS sockets.
  ///
  /// If you don't want to wait for the future you can just throw it
  /// away, `flush` triggers sending the data whether you await the
  /// future or not.
  pub fn flush(self) -> impl Future<Item=(), Error=Error> {
    let flushes =
      self.0.read().unwrap().clients.iter()
      .map(|(_, c)| c.clone().flush())
      .collect::<Vec<_>>();
    async_block! {
      for flush in flushes.into_iter() { await!(flush)? }
      Ok(())
    }
  }

  /// Returns a future that will become ready when there are `n` or
  /// more subscribers subscribing to at least one value.
  #[async]
  pub fn wait_client(self, n: usize) -> Result<()> {
    let rx = {
      let mut t = self.0.write().unwrap();
      if t.clients.len() >= n { return Ok(()) }
      else {
        let (tx, rx) = oneshot::channel();
        t.wait_clients.push(tx);
        rx
      }
    };
    Ok(await!(rx)?)
  }

  /// Returns the number of subscribers subscribing to at least one value.
  pub fn clients(&self) -> usize { self.0.read().unwrap().clients.len() }
}

fn get_published(t: &PublisherWeak, path: &Path) -> Option<PublishedUntyped> {
  t.upgrade().and_then(|t| {
    let t = t.0.read().unwrap();
    t.published.get(path).and_then(|p| p.upgrade())
  })
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
        let mut ws = None;
        std::mem::swap(&mut wait_set, &mut ws);
        match ws {
          Some(path) => {
            if let Some(published) = get_published(&t, &path) {
              let mut inner = published.0.lock().unwrap();
              // CR estokes: this is asking for a deadlock
              if let Some(ref mut f) = inner.on_write { (f)(msg) }
            }
          },
          None =>
            match serde_json::from_str(&msg)? {
              ToPublisher::Set(path) => { wait_set = Some(path); },
              ToPublisher::Unsubscribe(s) => {
                if let Some(published) = get_published(&t, &s) {
                  published.0.lock().unwrap().subscribed.remove(&addr);
                }
                let resp = serde_json::to_vec(&FromPublisher::Unsubscribed(s))?;
                writer.write_one(Encoded::new(resp));
                writer.flush_nowait();
              },
              ToPublisher::Subscribe(s) =>
                match get_published(&t, &s) {
                  None => {
                    let v = serde_json::to_vec(&FromPublisher::NoSuchValue(s))?;
                    writer.write_one(Encoded::new(v));
                    writer.flush_nowait();
                  },
                  Some(published) => {
                    let mut inner = published.0.lock().unwrap();
                    inner.subscribed.insert(addr, writer.clone());
                    let c = inner.message_header.clone();
                    let v = inner.current.clone();
                    writer.write_two(c, v);
                    writer.flush_nowait();
                  }
                },
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
        let mut inner = pv.0.lock().unwrap();
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
