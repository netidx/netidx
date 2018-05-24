use std::{
  result, cell::Cell, sync::{Arc, Weak, RwLock}, io::BufReader,
  convert::AsRef, net::SocketAddr, collections::{hash_map::Entry, HashMap, VecDeque},
  marker::PhantomData
};
use rand;
use futures::{self, prelude::*, Poll, Async, sync::oneshot};
use serde::{Serialize, de::DeserializeOwned};
use serde_json;
use tokio::{self, prelude::*, spawn, net::TcpStream};
use tokio_io::io::ReadHalf;
use path::Path;
use line_writer::LineWriter;
use publisher::{FromPublisher, ToPublisher};
use resolver_client::Resolver;
use error::*;

static MAXQ: usize = 1000;

struct UntypedSubscriptionInner {
  path: Path,
  current: Arc<String>,
  subscriber: Subscriber,
  connection: Connection,
  streams: Vec<UntypedUpdatesWeak>,
  nexts: Vec<oneshot::Sender<()>>,
  dead: bool
}

impl UntypedSubscriptionInner {
  fn unsubscribe(&mut self) {
    if !self.dead {
      self.dead = true;
      self.subscriber.fail_pending(&self.path, "dead");
      for strm in self.streams.iter() {
        if let Some(strm) = strm.upgrade() {
          strm.0.write().unwrap().ready.push_back(None)
        }
      }
    }
  }
}

impl Drop for UntypedSubscriptionInner {
  fn drop(&mut self) {
    self.unsubscribe();
  }
}

#[derive(Clone)]
struct UntypedSubscriptionWeak(Weak<RwLock<UntypedSubscriptionInner>>);

impl UntypedSubscriptionWeak {
  fn upgrade(&self) -> Option<UntypedSubscription> {
    Weak::upgrade(&self.0).map(|r| UntypedSubscription(r))
  }
}

#[derive(Clone)]
struct UntypedSubscription(Arc<RwLock<UntypedSubscriptionInner>>);

impl UntypedSubscription {
  fn new(
    path: Path,
    current: Arc<String>,
    connection: Connection,
    subscriber: Subscriber
  ) -> UntypedSubscription {
    let inner = UntypedSubscriptionInner {
      path, current, connection, subscriber,
      dead: false, streams: Vec::new(), nexts: Vec::new()
    };
    UntypedSubscription(Arc::new(RwLock::new(inner)))
  }

  fn downgrade(&self) -> UntypedSubscriptionWeak {
    UntypedSubscriptionWeak(Arc::downgrade(&self.0))
  }

  #[async]
  fn next(self) -> Result<()> {
    let rx = {
      let mut t = self.0.write().unwrap();
      if t.dead { bail!(ErrorKind::SubscriptionIsDead) }
      else {
        let (tx, rx) = oneshot::channel();
        t..nexts.push(tx);
        rx
      }
    };
    Ok(await!(rx)?)
  }

  fn updates(&self) -> UntypedUpdates {
    let inner = UntypedUpdatesInner {
      notify: Cell::new(None),
      hold: Cell::new(None),
      ready: VecDeque::new()
    };
    let t = UntypedUpdates(Arc::new(RwLock::new(inner)));
    self.0.write().unwrap().streams.push(t.downgrade());
    t
  }
}

struct UntypedUpdatesInner {
  notify: Cell<Option<Box<Fn() -> ()>>>,
  hold: Cell<Option<oneshot::Sender<()>>>,
  ready: VecDeque<Option<Arc<String>>>
}

#[derive(Clone)]
struct UntypedUpdates(Arc<RwLock<UntypedUpdatesInner>>);

impl UntypedUpdates {
  fn downgrade(&self) -> UntypedUpdatesWeak {
    UntypedUpdatesWeak(Arc::downgrade(&self.0))
  }
}

#[derive(Clone)]
struct UntypedUpdatesWeak(Weak<RwLock<UntypedUpdatesInner>>);

impl UntypedUpdatesWeak {
  fn upgrade(&self) -> Option<UntypedUpdates> {
    Weak::upgrade(&self.0).map(|v| UntypedUpdates(v))
  }
}

impl Stream for UntypedUpdates {
  type Item = Arc<String>;
  type Error = Error;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    let mut t = self.0.write().unwrap();
    let res =
      match t.ready.pop_front() {
        Some(v) => Ok(Async::Ready(v)),
        None => {
          let task = futures::task::current();
          t.notify.replace(Some(Box::new(move || task.notify())));
          Ok(Async::NotReady)
        }
      };
    if t.ready.len() < MAXQ / 2 {
      if let Some(hold) = t.hold.replace(None) {
        let _ = hold.send(());
      }
    }
    res
  }
}

pub struct Subscription<T> {
  untyped: UntypedSubscription,
  phantom: PhantomData<T>
}

impl<T> ::std::fmt::Debug for Subscription<T> {
  fn fmt(&self, f: &mut ::std::fmt::Formatter) -> result::Result<(), ::std::fmt::Error> {
    let t = self.0.read().unwrap();
    let ut = t.untyped.0.read().unwrap();
    write!(f, "Subscription (path = {:?})", ut.path)
  }
}

impl<T> Clone for Subscription<T> {
  fn clone(&self) -> Self {
    Subscription { untyped: Arc::clone(&self.untyped), phantom: PhantomData }
  }
}

impl<T> Subscription<T> where T: DeserializeOwned {
  fn new(untyped: UntypedSubscription) -> Subscription<T> {
    Subscription { untyped, phantom: PhantomData }
  }

  pub fn get(&self) -> Result<T> {
    let cur = {
      let ut = self.untyped.0.read().unwrap();
      if ut.dead { bail!(ErrorKind::SubscriptionIsDead); }
      Arc::clone(&ut.current)
    };
    Ok(serde_json::from_str(cur.as_ref())?)
  }

  pub fn get_raw(&self) -> Result<Arc<String>> {
    let ut = self.untyped.0.read().unwrap();
    if ut.dead { bail!(ErrorKind::SubscriptionIsDead) }
    else { Ok(Arc::clone(&ut.current)) }
  }

  pub fn next(self) -> impl Future<Item=(), Error=Error> {
    self.untyped.clone().next()
  }

  pub fn updates(&self) -> impl Stream<Item=T, Error=Error> {
    self.untyped.updates().and_then(|v| {
      Ok(serde_json::from_str(v.as_ref())?)
    })
  }

  pub fn updates_raw(&self) -> impl Stream<Item=Arc<String>, Error=Error> {
    self.untyped.updates()
  }

  pub fn write<U: Serialize>(&self, v: &U) -> Result<()> {
    let ut = self.untyped.0.read().unwrap();
    ut.connection.write(&ToPublisher::Set(ut.path.clone()))?;
    ut.connection.write(v)
  }

  pub fn flush(&self) -> impl Future<Item = (), Error = Error> {
    self.untyped.read().unwrap().connection.flush()
  }
}

struct ConnectionInner {
  pending: HashMap<Path, oneshot::Sender<Result<UntypedSubscription>>>,
  subscriptions: HashMap<Path, UntypedSubscriptionWeak>,
  writer: LineWriter<Vec<u8>, TcpStream>,
  subscriber: Subscriber,
  addr: SocketAddr
}

impl Drop for ConnectionInner {
  fn drop(&mut self) {
    self.writer.shutdown();
    self.subscriber.untyped.write().unwrap().connections.remove(&self.addr);
  }
}

#[derive(Clone)]
struct ConnectionWeak(Weak<RefCell<ConnectionInner>>);

impl ConnectionWeak {
  fn upgrade(&self) -> Option<Connection> {
    Weak::upgrade(&self.0).map(|r| Connection(r))
  }
}

#[derive(Clone)]
struct Connection(Arc<RwLock<ConnectionInner>>);

impl Connection {
  #[async]
  fn new(
    addr: SocketAddr,
    subscriber: Subscriber
  ) -> Result<(ReadHalf<TcpStream>, Connection)> {
    let s = await!(TcpStream::connect(&addr))?;
    s.set_nodelay(true)?;
    let (rd, wr) = s.split();
    let inner = ConnectionInner {
      subscriber, addr,
      pending: HashMap::new(),
      subscriptions: HashMap::new(),
      writer: LineWriter::new(wr),
    };
    Ok((rd, Connection(Arc::new(RwLock::new(inner)))))
  }

  fn downgrade(&self) -> ConnectionWeak { ConnectionWeak(Arc::downgrade(&self.0)) }

  fn write<T: Serialize>(&self, v: &T) -> Result<()> {
    let d = serde_json::to_vec(v)?;
    Ok(self.0.read().writer.write_one(d))
  }

  fn flush(&self) -> impl Future<Item=(), Error=Error> {
    let w = self.0.read().unwrap().writer.clone();
    w.flush()
  }
}

enum ConnectionState {
  Pending(Vec<oneshot::Sender<Result<Connection>>>),
  Connected(ConnectionWeak)
}

enum SubscriptionState {
  Pending(Vec<oneshot::Sender<Result<UntypedSubscription>>>),
  Subscribed(UntypedSubscriptionWeak),
}

struct SubscriberInner {
  resolver: Resolver,
  connections: HashMap<SocketAddr, ConnectionState>,
  subscriptions: HashMap<Path, SubscriptionState>,
}

#[derive(Clone)]
struct SubscriberWeak(Weak<RwLock<SubscriberInner>>);

impl SubscriberWeak {
  fn upgrade(&self) -> Option<Subscriber> {
    Weak::upgrade(&self.0).map(|r| Subscriber(r))
  }
}

#[derive(Clone)]
pub struct Subscriber(Arc<RwLock<SubscriberInner>>);

impl Subscriber {
  pub fn new(resolver: Resolver) -> Subscriber {
    let inner = SubscriberInner {
      resolver,
      connections: HashMap::new(),
      subscriptions: HashMap::new(),
    };
    Subscriber(Arc::new(RwLock::new(inner)))
  }

  fn downgrade(&self) -> SubscriberWeak {
    SubscriberWeak(Arc::downgrade(&self.0))
  }

  fn fail_pending<E: Into<Error>>(&self, path: &Path, e: E) -> Error {
    let mut t = self.0.write().unwrap();
    match t.subscriptions.get_mut(path) {
      None | Some(SubscriptionState::Subscribed(_)) => (),
      Some(SubscriptionState::Pending(ref mut q)) =>
        for c in q.drain(0..) { let _ = c.send(Err(Error::from(e))); }
    }
    t.subscriptions.remove(&path);
    Error::from(e)
  }

  #[async]
  pub fn subscribe<T: DeserializeOwned>(
    self,
    path: Path
  ) -> Result<Subscription<T>> {
    let mut t = self.0.write().unwrap();
    match t.subscriptions.entry(path.clone()) {
      Entry::Occupied(e) => {
        match e.get_mut() {
          SubscriptionState::Subscribed(ref ut) => Ok(Subscription::new(ut.clone())),
          SubscriptionState::Pending(ref mut q) => {
            let (send, recv) = oneshot::channel();
            q.push(send);
            drop(t)
            Ok(Subscription::new(await!(recv)?))
          }
        }
      },
      Entry::Vacant(e) => {
        let resolver = t.resolver.clone();
        e.insert(SubscriptionState::Pending(Vec::new()));
        drop(t);
        let mut addrs =
          match await!(resolver.resolve(path.clone())) {
            Ok(addrs) => addrs,
            Err(e) => bail!(self.fail_pending(e))
          };
        let mut last_error = None;
        while let Some(addr) = choose_address(&mut addrs) {
          match await!(self.clone().subscribe_addr(addr, path.clone())) {
            Err(e) => last_error = Some(e),
            Ok(s) => {
              let mut t = self.0.write().unwrap();
              match t.subscriptions.get_mut() {
                None | Some(SubscriptionState::Subscribed(_)) => unreachable!("bug"),
                Some(ref mut v @ SubscriptionState::Pending(ref mut q)) => {
                  for c in q.drain(0..) { let _ = c.send(Ok(s.untyped.clone())) }
                  *v = SubscriptionState::Subscribed(s.untyped.downgrade());
                }
              }
              Ok(s)
            }
          }
        }
        let e = last_error.unwrap_or(Error::from(ErrorKind::PathNotFound(path)));
        bail!(self.fail_pending(e))
      }
    }
  }

  #[async]
  fn subscribe_addr<T: DeserializeOwned>(
    self,
    addr: SocketAddr,
    path: Path
  ) -> Result<Subscription<T>> {
    let con = loop {
      let mut t = self.0.write().unwrap();
      match t.connections.get_mut(&addr) {
        Some(ConnectionState::Connected(ref con)) =>
          match con.upgrade() {
            Some(con) => break con,
            None => t.connections.remove(&addr),
          },
        Some(ConnectionState::Pending(ref mut q)) => {
          let (recv, send) = oneshot::channel();
          q.push(send);
          drop(t);
          break await!(recv)?;
        },
        None => {
          t.connections.insert(addr, ConnectionState::Pending(vec![]));
          drop(t);
          match await!(Connection::new(addr, self.clone())) {
            Err(e) => {
              let mut t = self.0.write().unwrap();
              match t.connections.get_mut(&addr) {
                Some(ConnectionState::Connected(_)) | None => unreachable!("bug"),
                Some(ConnectionState::Pending(ref mut q)) => {
                  for s in q.drain(0..) { let _ = s.send(Err(Error::from(&e))) }
                  t.connections.remove(&addr);
                  bail!(e)
                },
              }
            },
            Ok(con) => {
              spawn(start_connection(self.downgrade(), reader, con.downgrade()));
              let mut t = self.0.write().unwrap();
              match t.connections.get_mut(&addr) {
                Some(ConnectionState::Connected(_)) | None => unreachable!("bug"),
                Some(ref mut v @ ConnectionState::Pending(ref mut q)) => {
                  for s in q.drain(0..) { let _ = s.send(Ok(con.clone())); }
                  *v = ConnectionState::Connected(con.downgrade());
                  break con;
                },
              }
            }
          }
        }
      }
    };
    let (send, recv) = oneshot::channel();
    let msg = serde_json::to_vec(&ToPublisher::Subscribe(path.clone()))?;
    let mut c = con.0.write().unwrap();
    c.pending.insert(path, send);
    c.writer.write(msg);
    drop(c);
    let ut = await!(recv)??;
    Ok(Subscription::new(ut))
  }
}

fn choose_address(addrs: &mut Vec<SocketAddr>) -> Option<SocketAddr> {
  let len = addrs.len();
  if len == 0 { None }
  else if len == 1 { addrs.pop() }
  else {
    use rand::distributions::range::Range;
    use rand::distributions::IndependentSample;
    let mut rng = rand::thread_rng();
    let r = Range::new(0usize, len);
    Some(addrs.remove(r.ind_sample(&mut rng)))
  }
}

#[async]
fn connection_loop(
  t: SubscriberWeak,
  reader: ReadHalf<TcpStream>,
  con: ConnectionWeak
) -> Result<()> {
  let mut msg_pending = None;
  let mut holds = Vec::new();
  #[async]
  for line in tokio::io::lines(BufReader::new(reader)).map_err(|e| Error::from(e)) {
    match msg_pending {
      None =>
        match serde_json::from_str::<FromPublisher>(line.as_ref())? {
          FromPublisher::Message(path) => msg_pending = Some(path),
          FromPublisher::NoSuchValue(path) => {
            let con = con.upgrade().ok_or_else(|| Error::from("connection closed"))?;
            match con.0.write().unwrap().pending.remove(&path) {
              None => bail!("unsolicited"),
              Some(chan) =>
                chan.send(Err(Error::from(ErrorKind::PathNotFound(path))))
                .map_err(|_| Error::from("ipc err"))?
            }
          },
          FromPublisher::Unsubscribed(path) => {
            let con = con.upgrade().ok_or_else(|| Error::from("connection closed"))?;
            if let Some(ref ut) = con.0.write().unwrap().subscriptions.remove(&path) {
              if let Some(ut) = ut.upgrade() { ut.0.write().unwrap().unsubscribe() }
            }
          },
        },
      Some(path) => {
        msg_pending = None;
        let con = con.upgrade().ok_or_else(|| Error::from("connection closed"))?;
        let ut = con.0.read().unwrap()subscriptions.get(&path).map(|ut| ut.clone());
        match ut {
          Some(ref ut) => {
            if let Some(ut) = ut.upgrade() {
              let v = Arc::new(line);
              let mut ut = ut.0.write().unwrap();
              ut.current = v;
              for next in ut.nexts.drain(0..) { let _ = next.send(()); }
              let mut i = 0;
              while i < ut.streams.len() {
                match ut.streams[i].upgrade() {
                  None => { ut.streams.remove(i); },
                  Some(strm) => {
                    i += 1;
                    let mut strm = strm.0.write().unwrap();
                    strm.ready.push_back(Some(Arc::clone(&ut.current)));
                    if strm.ready.len() > MAXQ {
                      let (tx, rx) = oneshot::channel();
                      strm.hold.replace(Some(tx));
                      holds.push(rx)
                    }
                    if let Some(notify) = strm.notify.replace(None) { (notify)(); }
                  }
                }
              }
            }
            for hold in holds.drain(0..) { let _ = await!(hold); }
          },
          None => {
            let ut =
              UntypedSubscription::new(
                path.clone(), Arc::new(line), con.clone(), t.clone()
              );
            let mut c = con.0.write().unwrap();
            match c.pending.remove(&path) {
              None => bail!("unsolicited"),
              Some(chan) => {
                c.subscriptions.insert(path, ut.downgrade());
                chan.send(Ok(ut)).map_err(|_| Error::from("ipc err"))?;
              }
            }
          },
        }
      }
    }
  }
  Ok(())
}

#[async(boxed)]
fn start_connection(
  t: SubscriberWeak,
  reader: ReadHalf<TcpStream>,
  con: ConnectionWeak
) -> result::Result<(), ()> {
  let _ = await!(connection_loop(t.clone(), reader, con.clone()));
  if let Some(con) = con.upgrade() {
    let c = con.0.read().unwrap();
    c.writer.shutdown();
    for (_, s) in c.subscriptions.iter() {
      if let Some(s) = s.upgrade() {
        s.0.write().unwrap().unsubscribe()
      }
    }
  }
  Ok(())
}
