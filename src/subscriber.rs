use std::{
  result, sync::{Arc, Weak, RwLock}, io::BufReader, convert::AsRef, net::SocketAddr,
  collections::{hash_map::{Entry, OccupiedEntry}, HashMap, VecDeque},
  marker::PhantomData, mem
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
use utils::{self, BatchItem};
use error::*;

static MAXQ: usize = 10;

pub struct UntypedSubscriptionInner {
  path: Path,
  current: Arc<String>,
  subscriber: Subscriber,
  connection: Connection,
  streams: Vec<UntypedUpdatesWeak>,
  nexts: Vec<oneshot::Sender<()>>,
  dead: bool
}

impl UntypedSubscriptionInner {
  pub fn unsubscribe(&mut self) {
    if !self.dead {
      self.dead = true;
      self.subscriber.fail_pending(&self.path, "dead");
      for strm in self.streams.iter() {
        if let Some(strm) = strm.upgrade() {
          let mut strm = strm.0.write().unwrap();
          strm.ready.push_back(None);
          if let Some(ref notify) = strm.notify { (notify)() }
          strm.notify = None
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
        t.nexts.push(tx);
        rx
      }
    };
    Ok(await!(rx)?)
  }

  fn updates(&self) -> UntypedUpdates {
    let inner = UntypedUpdatesInner {
      notify: None,
      hold: None,
      ready: VecDeque::new()
    };
    let t = UntypedUpdates(Arc::new(RwLock::new(inner)));
    self.0.write().unwrap().streams.push(t.downgrade());
    t
  }
}

struct UntypedUpdatesInner {
  notify: Option<Box<Fn() -> () + Send + Sync + 'static>>,
  hold: Option<oneshot::Sender<()>>,
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
          t.notify = Some(Box::new(move || task.notify()));
          Ok(Async::NotReady)
        }
      };
    if t.ready.len() < MAXQ / 2 {
      let mut hold = None;
      mem::swap(&mut t.hold, &mut hold);
      if let Some(c) = hold { let _ = c.send(()); }
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
    let ut = self.untyped.0.read().unwrap();
    write!(f, "Subscription (path = {:?})", ut.path)
  }
}

impl<T> Clone for Subscription<T> {
  fn clone(&self) -> Self {
    Subscription { untyped: self.untyped.clone(), phantom: PhantomData }
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
    self.untyped.0.read().unwrap().connection.flush()
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
    self.subscriber.0.write().unwrap().connections.remove(&self.addr);
  }
}

#[derive(Clone)]
pub struct ConnectionWeak(Weak<RwLock<ConnectionInner>>);

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
    Ok(self.0.read().unwrap().writer.write_one(d))
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
pub struct SubscriberWeak(Weak<RwLock<SubscriberInner>>);

impl SubscriberWeak {
  fn upgrade(&self) -> Option<Subscriber> {
    Weak::upgrade(&self.0).map(|r| Subscriber(r))
  }
}

#[derive(Clone)]
pub struct Subscriber(Arc<RwLock<SubscriberInner>>);

fn get_pending<'a>(
  e: &'a mut OccupiedEntry<SocketAddr, ConnectionState>
) -> &'a mut Vec<oneshot::Sender<Result<Connection>>> {
  match e.get_mut() {
    ConnectionState::Connected(_) => unreachable!("bug"),
    ConnectionState::Pending(ref mut q) => q
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

  fn fail_pending<E: Into<Error> + ToString>(&self, path: &Path, e: E) -> Error {
    let mut t = self.0.write().unwrap();
    match t.subscriptions.get_mut(path) {
      None | Some(SubscriptionState::Subscribed(_)) => (),
      Some(SubscriptionState::Pending(ref mut q)) =>
        for c in q.drain(0..) { let _ = c.send(Err(Error::from(e.to_string()))); }
    }
    t.subscriptions.remove(path);
    e.into()
  }

  #[async]
  pub fn subscribe<T: DeserializeOwned>(
    self,
    path: Path
  ) -> Result<Subscription<T>> {
    enum Action {
      Subscribed(UntypedSubscription),
      Subscribe(Resolver),
      Wait(oneshot::Receiver<Result<UntypedSubscription>>)
    }
    let action = {
      let mut t = self.0.write().unwrap();
      let resolver = t.resolver.clone();
      match t.subscriptions.entry(path.clone()) {
        Entry::Vacant(e) => {
          e.insert(SubscriptionState::Pending(Vec::new()));
          Action::Subscribe(resolver)
        },
        Entry::Occupied(mut e) => {
          let action =
            match e.get_mut() {
              SubscriptionState::Subscribed(ref ut) => {
                match ut.upgrade() {
                  Some(ut) => Action::Subscribed(ut),
                  None => Action::Subscribe(resolver),
                }
              },
              SubscriptionState::Pending(ref mut q) => {
                let (send, recv) = oneshot::channel();
                q.push(send);
                Action::Wait(recv)
              }
            };
          match action {
            action @ Action::Wait(_) | action @ Action::Subscribed(_) => action,
            action @ Action::Subscribe(_) => {
              *e.get_mut() = SubscriptionState::Pending(Vec::new());
              action
            },
          }
        },
      }
    };
    match action {
      Action::Subscribed(ut) => Ok(Subscription::new(ut)),
      Action::Wait(rx) => Ok(Subscription::new(await!(rx)??)),
      Action::Subscribe(resolver) => {
        let mut addrs =
          match await!(resolver.resolve(path.clone())) {
            Ok(addrs) => addrs,
            Err(e) => bail!(self.fail_pending(&path, e))
          };
        let mut last_error = None;
        while let Some(addr) = choose_address(&mut addrs) {
          match await!(self.clone().subscribe_addr(addr, path.clone())) {
            Err(e) => last_error = Some(e),
            Ok(s) => {
              let mut t = self.0.write().unwrap();
              match t.subscriptions.get_mut(&path) {
                None | Some(SubscriptionState::Subscribed(_)) => unreachable!("bug"),
                Some(SubscriptionState::Pending(ref mut q)) =>
                  for c in q.drain(0..) { let _ = c.send(Ok(s.untyped.clone())); },
              }
              *t.subscriptions.get_mut(&path).unwrap() =
                SubscriptionState::Subscribed(s.untyped.downgrade());
              return Ok(s)
            }
          }
        }
        let e = last_error.unwrap_or(Error::from(ErrorKind::PathNotFound(path.clone())));
        bail!(self.fail_pending(&path, e))
      }
    }
  }

  #[async]
  fn initiate_connection(self, addr: SocketAddr) -> Result<Connection> {
    match await!(Connection::new(addr, self.clone())) {
      Err(err) => {
        let mut t = self.0.write().unwrap();
        match t.connections.entry(addr) {
          Entry::Vacant(_) => unreachable!("bug"),
          Entry::Occupied(mut e) => {
            {
              let q = get_pending(&mut e);
              for s in q.drain(0..) { let _ = s.send(Err(Error::from(err.to_string()))); }
            }
            e.remove();
            bail!(err)
          }
        }
      },
      Ok((reader, con)) => {
        spawn(start_connection(self.downgrade(), reader, con.downgrade()));
        let mut t = self.0.write().unwrap();
        match t.connections.entry(addr) {
          Entry::Vacant(_) => unreachable!("bug"),
          Entry::Occupied(mut e) => {
            {
              let q = get_pending(&mut e);
              for s in q.drain(0..) { let _ = s.send(Ok(con.clone())); }
            }
            *e.get_mut() = ConnectionState::Connected(con.downgrade());
            Ok(con)
          },
        }
      }
    }
  }

  #[async]
  fn subscribe_addr<T: DeserializeOwned>(
    self,
    addr: SocketAddr,
    path: Path
  ) -> Result<Subscription<T>> {
    enum Action {
      Connected(Connection),
      Wait(oneshot::Receiver<Result<Connection>>),
      Connect
    }
    let action = {
      let mut t = self.0.write().unwrap();
      match t.connections.entry(addr) {
        Entry::Occupied(mut e) => {
          let action = 
            match e.get_mut() {
              ConnectionState::Connected(ref con) =>
                match con.upgrade() {
                  Some(con) => Action::Connected(con),
                  None => Action::Connect,
                },
              ConnectionState::Pending(ref mut q) => {
                let (send, recv) = oneshot::channel();
                q.push(send);
                Action::Wait(recv)
              },
            };
          match action {
            a @ Action::Wait(_) | a @ Action::Connected(_) => a,
            Action::Connect => { e.remove(); Action::Connect },
          }
        },
        Entry::Vacant(e) => {
          e.insert(ConnectionState::Pending(vec![]));
          Action::Connect
        },
      }
    };
    let con =
      match action {
        Action::Connected(con) => con,
        Action::Wait(rx) => await!(rx)??,
        Action::Connect => await!(self.clone().initiate_connection(addr))?
      };
    let (send, recv) = oneshot::channel();
    let msg = serde_json::to_vec(&ToPublisher::Subscribe(path.clone()))?;
    {
      let mut c = con.0.write().unwrap();
      c.pending.insert(path, send);
      c.writer.write_one(msg);
      c.writer.flush_nowait();
    }
    let ut = await!(recv)??;
    Ok(Subscription::new(ut))
  }
}

fn process_batch(
  t: &SubscriberWeak,
  con: &ConnectionWeak,
  msgs: &mut HashMap<Path, Vec<Arc<String>>>,
  novalue: &mut Vec<Path>,
  unsubscribed: &mut Vec<Path>,
  holds: &mut Vec<oneshot::Receiver<()>>,
  strms: &mut Vec<UntypedUpdates>,
  nexts: &mut Vec<oneshot::Sender<()>>
) -> Result<()> {
  let con = con.upgrade().ok_or_else(|| Error::from("connection closed"))?;
  // process msgs
  for (path, updates) in msgs.iter_mut() {
    let ut = con.0.read().unwrap().subscriptions.get(path).map(|ut| ut.clone());
    match ut {
      Some(ref ut) => {
        if let Some(ut) = ut.upgrade() {
          {
            let mut ut = ut.0.write().unwrap();
            if let Some(ref up) = updates.last() {
              ut.current = Arc::clone(up);
            }
            let mut i = 0;
            while i < ut.streams.len() {
              match ut.streams[i].upgrade() {
                None => { ut.streams.remove(i); },
                Some(s) => {
                  i += 1;
                  strms.push(s)
                }
              }
            }
            for next in ut.nexts.drain(0..) { nexts.push(next); }
          }
          for next in nexts.drain(0..) { let _ = next.send(()); }
          for strm in strms.drain(0..) {
            let mut strm = strm.0.write().unwrap();
            strm.ready.extend(updates.iter().map(|v| Some(v.clone())));
            if strm.ready.len() > MAXQ {
              let (tx, rx) = oneshot::channel();
              strm.hold = Some(tx);
              holds.push(rx)
            }
            if let Some(ref notify) = strm.notify { (notify)(); }
            strm.notify = None;
          }
          updates.clear();
        }
      },
      None => {
        if let Some(ref up) = updates.last() {
          let t = t.upgrade().ok_or_else(|| Error::from("subscriber dropped"))?;
          let ut = UntypedSubscription::new(path.clone(), Arc::clone(up), con.clone(), t);
          let mut c = con.0.write().unwrap();
          match c.pending.remove(path) {
            None => bail!("unsolicited"),
            Some(chan) => {
              c.subscriptions.insert(path.clone(), ut.downgrade());
              chan.send(Ok(ut)).map_err(|_| Error::from("ipc err"))?;
            }
          }
        }
      },
    }
  }
  // process no value control msgs
  for path in novalue.drain(0..) {
    msgs.remove(&path);
    let p = {
      let mut con = con.0.write().unwrap();
      con.pending.remove(&path)
    };
    match p {
      None => bail!("unsolicited"),
      Some(chan) =>
        chan.send(Err(Error::from(ErrorKind::PathNotFound(path))))
        .map_err(|_| Error::from("ipc err"))?
    }
  }
  // process unsubscribed control msgs
  for path in unsubscribed.drain(0..) {
    msgs.remove(&path);
    let sub = {
      let mut con = con.0.write().unwrap();
      con.subscriptions.remove(&path)
    };
    if let Some(ref ut) = sub {
      if let Some(ut) = ut.upgrade() { ut.0.write().unwrap().unsubscribe() }
    }
  }
  Ok(())
}

#[async]
pub fn connection_loop(
  t: SubscriberWeak,
  reader: ReadHalf<TcpStream>,
  con: ConnectionWeak
) -> Result<()> {
  let mut msg_pending = None;
  let mut msgs : HashMap<Path, Vec<Arc<String>>> = HashMap::new();
  let mut novalue : Vec<Path> = Vec::new();
  let mut unsubscribed: Vec<Path> = Vec::new();
  let mut holds : Vec<oneshot::Receiver<()>> = Vec::new();
  let mut strms : Vec<UntypedUpdates> = Vec::new();
  let mut nexts : Vec<oneshot::Sender<()>> = Vec::new();
  let batched = {
    let lines = tokio::io::lines(BufReader::new(reader)).map_err(|e| Error::from(e));
    utils::batched(lines, 100000)
  };
  #[async]
  for item in batched {
    match item {
      BatchItem::InBatch(line) => {
        match msg_pending {
          Some(path) => {
            msg_pending = None;
            msgs.entry(path).or_insert(Vec::new()).push(Arc::new(line))
          },
          None => {
            match serde_json::from_str::<FromPublisher>(line.as_ref())? {
              FromPublisher::Message(path) => msg_pending = Some(path),
              FromPublisher::NoSuchValue(path) => novalue.push(path),
              FromPublisher::Unsubscribed(path) => unsubscribed.push(path),
            }
          },
        }
      },
      BatchItem::EndBatch => {
        process_batch(
          &t, &con, &mut msgs, &mut novalue, &mut unsubscribed,
          &mut holds, &mut strms, &mut nexts
        )?;
        while let Some(hold) = holds.pop() {
          let _ = await!(hold);
        }
      }
    }
  }
  Ok(())
}

#[async]
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
