use futures::{prelude::*, sync::oneshot};
use tokio::{self, prelude::*, spawn, net::{TcpStream, TcpListener}};
use tokio_io::io::{WriteHalf, write_all};
use std::{
  io::BufReader, net::SocketAddr, sync::{Arc, RwLock}, result,
  time::{Instant, Duration},
  collections::{
    BTreeMap, HashMap, HashSet, hash_map::Entry,
    Bound::{Included, Excluded, Unbounded},
    Bound
  }
};
use path::{self, Path};
use utils::{Batched, BatchItem, batched};
use serde_json;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ClientHello {
  ttl: i64, // seconds 1 - 3600
  uuid: String
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServerHello {
  instance_uuid: String
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ToResolver {
  Alive,
  Resolve(Path),
  Publish(Path, SocketAddr),
  Unpublish(Path, SocketAddr),
  List(Path)
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FromResolver {
  Resolved(Vec<SocketAddr>),
  List(Vec<Path>),
  Published,
  Unpublished,
  Error(String)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Published {
  Empty,
  One(SocketAddr, usize),
  Many(HashMap<SocketAddr, usize>)
}

struct Store(BTreeMap<Path, Published>);

impl Store {
  fn new() -> Self { Store(BTreeMap::new()) }

  fn publish(&mut self, path: Path, addr: SocketAddr) {
    let v = self.0.entry(path).or_insert(Published::One(addr, 1));
    match *v {
      Published::Empty => { *v = Published::One(addr, 1) },
      Published::Many(ref mut set) => *set.entry(addr).or_insert(0) += 1,
      Published::One(cur, i) =>
        if cur == addr {
          *v = Published::One(cur, i + 1)
        } else {
          let s =
            [(addr, 1), (cur, i)].iter().map(|&(a, i)| (*a, *i))
            .collect::<HashMap<_>>();
          *v = Published::Many(s)
        }
    }
  }

  fn unpublish(&mut self, path: &Path, addr: &SocketAddr) {
    let remove =
      match self.0.get_mut(path) {
        None => false,
        Some(mut v) => {
          match *v {
            Published::Empty => false,
            Published::One(a, ref mut i) => *a == *addr && { *i -= 1; *i <= 0 },
            Published::Many(ref mut set) => {
              let remove = 
                match set.get_mut(addr) {
                  None => false,
                  Some(ref mut i) => {
                    *i -= 1;
                    *i <= 0
                  }
                };
              if remove { set.remove(&addr); }
              set.is_empty()
            }
          }
        }
      };
    if remove {
      self.0.remove(path);
      // remove parents that have no further children
      let mut p : &str = path.as_ref();
      loop {
        match path::dirname(p) {
          None => break,
          Some(parent) => {
            let remove = {
              let mut r =
                self.0.range::<str, (Bound<&str>, Bound<&str>)>(
                  (Included(parent), Unbounded)
                );
              match r.next() {
                None => false, // parent doesn't exist, probably a bug
                Some((_, parent_v)) => {
                  if parent_v != &Published::Empty { break; }
                  else {
                    match r.next() {
                      None => true,
                      Some((sib, _)) => !sib.starts_with(parent)
                    }
                  }
                }
              }
            };
            if remove { self.0.remove(parent); }
            p = parent;
          }
        }
      }
    }
  }

  fn list(&self, parent: &Path) -> Vec<Path> {
    let parent : &str = parent.as_ref();
    let mut res = Vec::new();
    let paths =
      self.0.range::<str, (Bound<&str>, Bound<&str>)>(
        (Excluded(parent), Unbounded)
      );
    for (p, _) in paths {
      let d =
        match path::dirname(p) {
          None => "/",
          Some(d) => d
        };
      if parent != d { break }
      else { path::basename(p).map(|p| res.push(Path::from(p))); }
    }
  }
}

#[async]
fn send(
  w: WriteHalf<TcpStream>,
  m: FromResolver
) -> result::Result<WriteHalf<TcpStream>, ()> {
  let m = serde_json::to_vec(&m).map_err(|_| ())?;
  let w = await!(write_all(w, m)).map_err(|_| ())?.0;
  Ok(await!(write_all(w, "\n")).map_err(|_| ())?.0)
}

struct ClientInfoInner {
  ttl: Duration,
  last: Instant,
  published: Store,
  stop: Option<oneshot::Sender<()>>,
}

struct ClientInfo(Arc<Mutex<ClientInfoInner>>);

struct ContextInner {
  published: Store,
  clients: HashMap<String, ClientInfo>,
  stops: HashMap<usize, oneshot::Sender<()>>,
}

impl ContextInner {
  fn timeout_client(&mut self, client: &ClientInfoInner) {
    for (ref path, ref published) in client.published.iter() {
      match published {
        Published::Empty => (),
        Published::One(ref addr, i) =>
          for j in 0..i { self.published.unpublish(path, addr) },
        Published::Many(ref set) =>
          for (addr, i) in set.iter() {
            for j in 0..i { self.published.unpublish(path, addr) }
          }
      }
    }
  }

  fn resolve(&self, path: &Path) -> FromResolver {
    if !path.is_absolute() { FromResolver::Error("resolve relative path".into()) }
    else {
      match self.published.get(path) {
        None | Some(&Published::Empty) => FromResolver::Resolved(vec![]),
        Some(&Published::One(a, _)) => FromResolver::Resolved(vec![a]),
        Some(&Published::Many(ref a)) => {
          let s = a.iter().map(|(a, _)| *a).collect::<Vec<_>>();
          FromResolver::Resolved(s)
        }
      }
    }
  }

  fn publish(
    &mut self,
    path: Path,
    addr: SocketAddr,
    client: &mut ClientInfoInner
  ) -> FromResolver {
    if !path.is_absolute() {
      return FromResolver::Error("publish relative path".into())
    }
    self.published.publish(path.clone(), addr);
    client.published.publish(path, addr);
    FromResolver::Published
  }

  fn unpublish(&self, path: Path, addr: SocketAddr, client: &mut ClientInfoInner) {
    self.published.unpublish(&path, &addr);
    client.published.unpublish(&path, &addr)
  }

  fn list(&self, parent: Path) -> FromResolver {
    if !parent.is_absolute() {
      return FromResolver::Error("list relative path".into())
    }
    let t = self.0.read().unwrap();
    FromResolver::List(t.published.list(&parent))
  }
}

pub struct Context(Arc<RwLock<ContextInner>>);

impl Clone for Context {
  fn clone(&self) -> Self { Context(Arc::clone(&self.0)) }
}

impl Context {
  #[async]
  fn handle_client(
    self, s: TcpStream,
    server_stop: oneshot::Receiver<()>,
    instance_uuid: String
  ) -> result::Result<(), ()> {
    enum M { Stop, Line(String) }
    let (rx, mut tx) = s.split();
    let msgs =
      tokio::io::lines(BufReader::new(rx)).map_err(|_| ()).map(|l| M::Line(l))
      .select(server_stop.into_stream().map_err(|_| ()).map(|()| M::Stop));
    let (client, client_stop, msgs) =
      match await!(msgs.into_future()) {
        (None, _) => return Err(()),
        (Some(M::Stop), _) => return Ok(()),
        (Some(M::Line(l)), msgs) => {
          let h = serde_json::from_str::<ClientHello>(&l).map_err(|_| ())?;
          if h.ttl <= 0 || h.ttl > 3600 { return Err(()) }
          let mut t = self.0.write().unwrap();
          let (tx_stop, rx_stop) = oneshot::channel();
          let client = 
            match t.clients.entry(h.uuid) {
              Entry::Vacant(e) => {
                let client = ClientInfo(Arc::new(Mutex::new(ClientInfoInner {
                  ttl: Duration::from_secs(h.ttl),
                  last: Instant::now(),
                  published: Store::new(),
                  stop: Some(tx_stop),
                })));
                e.insert(client.clone());
                client
              },
              Entry::Occupied(e) => {
                let client = e.get_mut();
                let mut cl = client.0.lock().unwrap();
                cl.last = t.now;
                cl.stop = Some(tx_stop);
                client.clone()
              }
            }
          tx = await!(send(tx, ServerHello { instance_uuid }))?;
          (client, tx_stop, msgs)
        }
      };
    let msgs = msgs.select(client_stop.into_stream().map_err(|_| ()).map(|()| M::Stop));
    let msgs = batched(msgs, 10000);
    let mut batch : Vec<ToResolver> = Vec::new();
    let mut response : Vec<FromResolver> = Vec::new();
    let mut batch_needs_write_lock = false;
    #[async]
    for msg in msgs {
      match msg {
        BatchItem::InBatch(m) =>
          match m {
            M::Stop => break,
            M::Line(l) =>
              match serde_json::from_str::<ToResolver>(&l).map_err(|_| ()) {
                m@ ToResolver::Resolve(_)
                  | m@ ToResolver::List(_)
                  | m@ ToResolver::Alive => batch.push(m),
                m@ ToResolver::Publish(_) | m@ ToResolver::Unpublish(_) => {
                  batch_needs_write_lock = true;
                  batch.push(m)
                }
              }
          },
        BatchItem::EndBatch => {         
          if batch_needs_write_lock {
            let mut ci = client.0.lock().unwrap();
            ci.last = Instant::now();
            let mut t = self.0.write().unwrap();
            for m in batch.drain(0..) {
              match m {
                ToResolver::Alive => (),
                ToResolver::Resolve(ref path) => response.push(t.resolve(path)),
                ToResolver::List(ref path) => response.push(t.list(path)),
                ToResolver::Publish(path, addr) =>
                  response.push(t.publish(path, addr, &mut ci)),
                ToResolver::Unpublish(path, addr) =>
                  response.push(t.unpublish(path, addr,  &mut ci)),
              }
            }
          } else {
            {
              let mut ci = client.0.lock().unwrap();
              ci.last = Instant::now();
            }
            let t = self.0.read().unwrap();
            for m in batch.drain(0..) {
              ToResolver::Alive => (),
              ToResolver::Resolve(ref path) => response.push(t.resolve(path)),
              ToResolver::List(ref path) => response.push(t.list(path)),
              ToResolver::Publish(..) | ToResolver::Unpublish(..) =>
                unreachable!("write lock required")
            }
          }
          for m in response.drain(0..) { tx = await!(send(tx, m))?; }
          batch_needs_write_lock = false;
        }
      }
    }
    Ok(())
  }

  fn start_client(
    self, s: TcpStream,
    client: usize,
    server_stop: oneshot::Receiver<()>,
    instance_uuid: String
  ) -> result::Result<(), ()> {
    let _ = await!(self.handle_client(s, server_stop, instance_uuid));
    let mut t = self.0.write().unwrap();
    t.stops.remove(&client);
    Ok(())
  }
}

#[async]
fn accept_loop(
  addr: SocketAddr,
  stop: oneshot::Receiver<()>,
  ready: oneshot::Sender<()>,
  instance_uuid: String
) -> result::Result<(), ()> {
  let mut cid = 0;
  let t : Context =
    Context(Arc::new(RwLock::new(ContextInner {
      published: BTreeMap::new(),
      clients: HashMap::new(),
      stops: HashMap::new(),
    })));
  enum M { Stop, Client(TcpStream) }
  let msgs =
    TcpListener::bind(&addr).map_err(|_| ())?
    .incoming().map_err(|_| ()).map(|c| M::Client(c))
    .select(stop.into_stream().map_err(|_| ()).map(|()| M::Stop));
  ready.send(()).map_err(|_| ())?;
  #[async]
  for msg in msgs {
    match msg {
      M::Client(client) => {
        let (tx_stop, rx_stop) = oneshot::channel();
        let mut ctx = t.0.write().unwrap();
        ctx.stops.insert(cid, tx_stop);
        spawn(t.clone().start_client(client, cid, rx_stop, instance_uuid.clone()));
        cid += 1
      },
      M::Stop => {
        let mut ctx = t.0.write().unwrap();
        for s in ctx.stops.drain(0..) { let _ = s.send(()); }
        break;
      },
    }
  }
  Ok(())
}

pub struct Server(Option<oneshot::Sender<()>>);

impl Drop for Server {
  fn drop(&mut self) {
    let mut stop = None;
    ::std::mem::swap(&mut stop, &mut self.0);
    if let Some(stop) = stop { let _ = stop.send(()); }
  }
}

use error::*;

impl Server {
  #[async]
  pub fn new(addr: SocketAddr) -> Result<Server> {
    let (send_stop, recv_stop) = oneshot::channel();
    let (send_ready, recv_ready) = oneshot::channel();
    spawn(accept_loop(addr, recv_stop, send_ready));
    await!(recv_ready).map_err(|_| Error::from("ipc error"))?;
    Ok(Server(Some(send_stop)))
  }
}
