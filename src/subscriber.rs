use std::{
  result, cell::Cell, sync::{Arc, Weak, RwLock}, io::BufReader,
  convert::AsRef, net::SocketAddr, collections::{HashMap, VecDeque},
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
      self.subscriber.0.borrow_mut().subscriptions.remove(&self.path);
      for strm in self.streams.iter() {
        if let Some(strm) = strm.upgrade() {
          strm.0.borrow_mut().ready.push_back(None)
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
    current: Rc<String>,
    connection: Connection,
    subscriber: Subscriber
  ) -> UntypedSubscription {
    let inner = UntypedSubscriptionInner {
      path, current, connection, subscriber,
      dead: false, streams: Vec::new(), nexts: Vec::new()
    };
    UntypedSubscription(Rc::new(RefCell::new(inner)))
  }

  fn downgrade(&self) -> UntypedSubscriptionWeak {
    UntypedSubscriptionWeak(Rc::downgrade(&self.0))
  }

  #[async]
  fn next(self) -> Result<()> {
    if self.0.borrow().dead {
      Err(Error::from(ErrorKind::SubscriptionIsDead))
    } else {
      let (tx, rx) = oneshot::channel();
      self.0.borrow_mut().nexts.push(tx);
      Ok(await!(rx)?)
    }
  }

  fn updates(&self) -> UntypedUpdates {
    let inner = UntypedUpdatesInner {
      notify: Cell::new(None),
      hold: Cell::new(None),
      ready: VecDeque::new()
    };
    let t = UntypedUpdates(Rc::new(RefCell::new(inner)));
    self.0.borrow_mut().streams.push(t.downgrade());
    t
  }
}

struct UntypedUpdatesInner {
  notify: Cell<Option<Box<Fn() -> ()>>>,
  hold: Cell<Option<oneshot::Sender<()>>>,
  ready: VecDeque<Option<Rc<String>>>
}

#[derive(Clone)]
struct UntypedUpdates(Rc<RefCell<UntypedUpdatesInner>>);

impl UntypedUpdates {
  fn downgrade(&self) -> UntypedUpdatesWeak {
    UntypedUpdatesWeak(Rc::downgrade(&self.0))
  }
}

#[derive(Clone)]
struct UntypedUpdatesWeak(Weak<RefCell<UntypedUpdatesInner>>);

impl UntypedUpdatesWeak {
  fn upgrade(&self) -> Option<UntypedUpdates> {
    Weak::upgrade(&self.0).map(|v| UntypedUpdates(v))
  }
}

impl Stream for UntypedUpdates {
  type Item = Rc<String>;
  type Error = Error;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    let mut t = self.0.borrow_mut();
    if t.ready.len() < MAXQ / 2 {
      if let Some(hold) = t.hold.replace(None) {
        let _ = hold.send(());
      }
    }
    match t.ready.pop_front() {
      Some(v) => Ok(Async::Ready(v)),
      None => {
        let task = futures::task::current();
        t.notify.replace(Some(Box::new(move || task.notify())));
        Ok(Async::NotReady)
      }
    }
  }
}

struct SubscriptionInner<T> {
  untyped: UntypedSubscription,
  phantom: PhantomData<T>
}

pub struct Subscription<T>(Rc<RefCell<SubscriptionInner<T>>>);

impl<T> ::std::fmt::Debug for Subscription<T> {
  fn fmt(&self, f: &mut ::std::fmt::Formatter) -> result::Result<(), ::std::fmt::Error> {
    let t = self.0.borrow();
    let ut = t.untyped.0.borrow();
    write!(f, "Subscription (path = {:?})", ut.path)
  }
}

impl<T> Clone for Subscription<T> {
  fn clone(&self) -> Self { Subscription(Rc::clone(&self.0)) }
}

impl<T> Subscription<T> where T: DeserializeOwned {
  fn new(untyped: UntypedSubscription) -> Subscription<T> {
    let inner = SubscriptionInner {
      untyped: untyped,
      phantom: PhantomData
    };
    Subscription(Rc::new(RefCell::new(inner)))
  }

  pub fn get(&self) -> Result<T> {
    let t = self.0.borrow();
    let ut = t.untyped.0.borrow();
    if ut.dead {
      Err(Error::from(ErrorKind::SubscriptionIsDead))
    } else {
      Ok(serde_json::from_str(ut.current.as_ref())?)
    }
  }

  pub fn get_raw(&self) -> Result<Rc<String>> {
    let t = self.0.borrow();
    let ut = t.untyped.0.borrow();
    if ut.dead { bail!(ErrorKind::SubscriptionIsDead) }
    else { Ok(Rc::clone(&ut.current)) }
  }

  pub fn next(self) -> impl Future<Item=(), Error=Error> {
    self.0.borrow().untyped.clone().next()
  }

  pub fn updates(&self) -> impl Stream<Item=T, Error=Error> {
    self.0.borrow().untyped.updates().and_then(|v| {
      Ok(serde_json::from_str(v.as_ref())?)
    })
  }

  pub fn updates_raw(&self) -> impl Stream<Item=Rc<String>, Error=Error> {
    self.0.borrow().untyped.updates()
  }

  pub fn write<U: Serialize>(&self, v: &U) -> Result<()> {
    let t = self.0.borrow();
    let ut = t.untyped.0.borrow();
    ut.connection.write(&ToPublisher::Set(ut.path.clone()))?;
    ut.connection.write(v)
  }

  pub fn flush(&self) -> impl Future<Item = (), Error = Error> {
    let t = self.0.borrow();
    let ut = t.untyped.0.borrow();
    ut.connection.flush()
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
    self.subscriber.0.borrow_mut().connections.remove(&self.addr);
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
struct Connection(Rc<RefCell<ConnectionInner>>);

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
    Ok((rd, Connection(Rc::new(RefCell::new(inner)))))
  }

  fn downgrade(&self) -> ConnectionWeak { ConnectionWeak(Rc::downgrade(&self.0)) }

  fn write<T: Serialize>(&self, v: &T) -> Result<()> {
    Ok(self.0.borrow().writer.write(serde_json::to_vec(v)?))
  }

  fn flush(&self) -> impl Future<Item=(), Error=Error> {
    self.0.borrow().writer.clone().flush()
  }
}

struct SubscriberInner {
  resolver: Resolver,
  connections: HashMap<SocketAddr, ConnectionWeak>,
  subscriptions: HashMap<Path, UntypedSubscriptionWeak>,
}

#[derive(Clone)]
struct SubscriberWeak(Weak<RefCell<SubscriberInner>>);

impl SubscriberWeak {
  fn upgrade(&self) -> Option<Subscriber> {
    Weak::upgrade(&self.0).map(|r| Subscriber(r))
  }
}

#[derive(Clone)]
pub struct Subscriber(Rc<RefCell<SubscriberInner>>);

impl Subscriber {
  pub fn new(resolver: Resolver) -> Subscriber {
    let inner = SubscriberInner {
      resolver,
      connections: HashMap::new(),
      subscriptions: HashMap::new(),
    };
    Subscriber(Rc::new(RefCell::new(inner)))
  }

  fn downgrade(&self) -> SubscriberWeak {
    SubscriberWeak(Rc::downgrade(&self.0))
  }

  #[async]
  pub fn subscribe<T: DeserializeOwned>(
    self,
    path: Path
  ) -> Result<Subscription<T>> {
    let s = self.0.borrow().subscriptions.get(&path).and_then(|ref u| u.upgrade());
    match s {
      Some(ut) => Ok(Subscription::new(ut)),
      None => {
        let mut addrs = await!(self.0.borrow().resolver.clone().resolve(path.clone()))?;
        let mut last_error = None;
        while let Some(addr) = choose_address(&mut addrs) {
          let c = self.0.borrow().connections.get(&addr).map(|c| c.clone());
          match await!(do_subscribe(self.clone(), addr, c, path.clone())) {
            Err(e) => last_error = Some(e),
            Ok(s) => return Ok(s)
          }
        }
        Err(last_error.unwrap_or(Error::from(ErrorKind::PathNotFound(path))))
      }
    }
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
fn do_subscribe<T: DeserializeOwned>(
  t: Subscriber,
  addr: SocketAddr,
  con: Option<ConnectionWeak>,
  path: Path
) -> Result<Subscription<T>> {
  let con =
    match con.and_then(|c| c.upgrade()) {
      Some(con) => con,
      None => {        
        let (reader, con) = await!(Connection::new(addr, t.clone()))?;
        t.0.borrow_mut().connections.insert(addr, con.downgrade());
        spawn(start_connection(t.downgrade(), reader, con.downgrade()));
        con
      }
    };
  let msg = serde_json::to_vec(&ToPublisher::Subscribe(path.clone()))?;
  let (send, recv) = oneshot::channel();
  {
    let mut c = con.0.borrow_mut();
    c.pending.insert(path, send);
    c.writer.write(msg);
  }
  let ut = await!(recv)??;
  Ok(Subscription::new(ut))
}

#[async]
fn connection_loop(
  t: SubscriberWeak,
  reader: ReadHalf<TcpStream>,
  con: ConnectionWeak
) -> Result<()> {
  let mut msg_pending = None;
  #[async]
  for line in tokio::io::lines(BufReader::new(reader)).map_err(|e| Error::from(e)) {
    let con = con.upgrade().ok_or_else(|| Error::from("connection closed"))?;
    let t = t.upgrade().ok_or_else(|| Error::from("subscriber closed"))?;
    match msg_pending {
      None => {
        match serde_json::from_str::<FromPublisher>(line.as_ref())? {
          FromPublisher::Message(path) => msg_pending = Some(path),
          FromPublisher::NoSuchValue(path) =>
            match con.0.borrow_mut().pending.remove(&path) {
              None => bail!("unsolicited"),
              Some(chan) =>
                chan.send(Err(Error::from(ErrorKind::PathNotFound(path))))
                .map_err(|_| Error::from("ipc err"))?
            },
          FromPublisher::Unsubscribed(path) =>
            if let Some(ref ut) = con.0.borrow().subscriptions.get(&path) {
              if let Some(ut) = ut.upgrade() {
                ut.0.borrow_mut().unsubscribe()
              }
            }
        }
      },
      Some(path) => {
        msg_pending = None;
        let ut = 
          con.0.borrow_mut().subscriptions.get(&path)
          .map(|ut| ut.clone());
        match ut {
          Some(ref ut) => {
            if let Some(ut) = ut.upgrade() {
              let holds = {
                let mut ut = ut.0.borrow_mut();
                let v = Rc::new(line);
                ut.current = v;
                for next in ut.nexts.drain(0..) { let _ = next.send(()); }
                let mut holds = Vec::new();
                let mut i = 0;
                while i < ut.streams.len() {
                  match ut.streams[i].upgrade() {
                    None => { ut.streams.remove(i); },
                    Some(strm) => {
                      i += 1;
                      let mut strm = strm.0.borrow_mut();
                      strm.ready.push_back(Some(Rc::clone(&ut.current)));
                      if let Some(notify) = strm.notify.replace(None) { (notify)(); }
                      if strm.ready.len() > MAXQ {
                        let (tx, rx) = oneshot::channel();
                        strm.hold.replace(Some(tx));
                        holds.push(rx)
                      }
                    }
                  }
                }
                holds
              };
              for hold in holds.into_iter() {
                let _ = await!(hold);
              }
            }
          },
          None => {
            let mut con_m = con.0.borrow_mut();
            let chan = con_m.pending.remove(&path);
            match chan {
              None => bail!("unsolicited"),
              Some(chan) => {
                let ut =
                  UntypedSubscription::new(
                    path.clone(), Rc::new(line), con.clone(), t.clone()
                  );
                con_m.subscriptions.insert(path.clone(), ut.downgrade());
                t.0.borrow_mut().subscriptions.insert(path, ut.downgrade());
                chan.send(Ok(ut)).map_err(|_| Error::from("ipc err"))?;
              }
            }
          }
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
    let c = con.0.borrow();
    c.writer.shutdown();
    for (_, s) in c.subscriptions.iter() {
      if let Some(s) = s.upgrade() {
        s.0.borrow_mut().unsubscribe()
      }
    }
  }
  Ok(())
}
