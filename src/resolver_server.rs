use futures::{prelude::*, sync::oneshot};
use tokio::{self, prelude::*, spawn, net::{TcpStream, TcpListener}};
use tokio_io::io::{WriteHalf, write_all};
use std::{
  io::BufReader, net::SocketAddr, sync::{Arc, RwLock}, result,
  collections::{
    BTreeMap, HashMap, HashSet, hash_map::Entry,
    Bound::{Included, Excluded, Unbounded},
    Bound
  }
};
use path::{self, Path};
use serde_json;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ToResolver {
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
  One(SocketAddr),
  Many(HashSet<SocketAddr>)
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

struct ContextInner {
  published: BTreeMap<Path, Published>,
  addr_to_paths: HashMap<SocketAddr, HashSet<Path>>,
  addr_to_client: HashMap<SocketAddr, SocketAddr>,
  client_to_addrs: HashMap<SocketAddr, HashSet<SocketAddr>>,
}

impl ContextInner {
  fn handle_unpublish(
    &mut self,
    path: Path,
    addr: SocketAddr,
    client: SocketAddr
  ) {
    match self.addr_to_paths.entry(addr) {
      Entry::Vacant(_) => (),
      Entry::Occupied(mut e) => {
        let empty = {
          let mut set = e.get_mut();
          set.remove(&path);
          set.is_empty()
        };
        if empty {
          e.remove_entry();
          self.addr_to_client.remove(&addr);
          match self.client_to_addrs.entry(client) {
            Entry::Vacant(_) => (),
            Entry::Occupied(mut e) => {
              let empty = {
                let mut set = e.get_mut();
                set.remove(&addr);
                set.is_empty()
              };
              if empty { e.remove_entry(); }
            }
          }
        }
      }
    }
    let remove =
      match self.published.get_mut(&path) {
        None => false,
        Some(mut v) => {
          match *v {
            Published::Empty => false,
            Published::One(a) => a == addr,
            Published::Many(ref mut set) => {
              set.remove(&addr);
              set.is_empty()
            }
          }
        }
      };
    if remove {
      self.published.remove(&path);
      // remove parents that have no further children
      let mut p : &str = path.as_ref();
      loop {
        match path::dirname(p) {
          None => break,
          Some(parent) => {
            let remove = {
              let mut r =
                self.published.range::<str, (Bound<&str>, Bound<&str>)>(
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
            if remove { self.published.remove(parent); }
            p = parent;
          }
        }
      }
    }
  }
}

struct Context(Arc<RwLock<ContextInner>>);

impl Clone for Context {
  fn clone(&self) -> Self { Context(Arc::clone(&self.0)) }
}

impl Context {
  fn handle_resolve(&self, path: Path) -> FromResolver {
    if !path.is_absolute() { FromResolver::Error("resolve relative path".into()) }
    else {
      match self.0.read().unwrap().published.get(&path) {
        None | Some(&Published::Empty) => FromResolver::Resolved(vec![]),
        Some(&Published::One(a)) => FromResolver::Resolved(vec![a]),
        Some(&Published::Many(ref a)) => {
          let s = a.iter().map(|a| *a).collect::<Vec<_>>();
          FromResolver::Resolved(s)
        }
      }
    }
  }

  fn handle_publish(
    &self,
    path: Path,
    addr: SocketAddr,
    client: SocketAddr
  ) -> FromResolver {
    if !path.is_absolute() {
      return FromResolver::Error("publish relative path".into())
    }
    let mut t = self.0.write().unwrap();
    if *(t.addr_to_client.entry(addr).or_insert(client)) != client {
      let m = "address is already published by another client".into();
      return FromResolver::Error(m)
    }
    t.client_to_addrs.entry(client).or_insert_with(HashSet::new).insert(addr);
    {
      let v = t.published.entry(path.clone()).or_insert(Published::One(addr));
      match *v {
        Published::Empty => { *v = Published::One(addr) },
        Published::Many(ref mut set) => { set.insert(addr); },
        Published::One(cur) =>
          if cur != addr {
            let s = [addr, cur].iter().map(|a| *a).collect::<HashSet<_>>();
            *v = Published::Many(s)
          }
      }
    }
    {
      let mut p: &str = path.as_ref();
      while let Some(sp) = path::dirname(p) {
        t.published.entry(Path::from(sp)).or_insert(Published::Empty);
        p = sp
      }
    }
    t.addr_to_paths.entry(addr).or_insert_with(HashSet::new).insert(path);
    FromResolver::Published
  }

  fn handle_unpublish(&self, path: Path, addr: SocketAddr, client: SocketAddr) {
    let mut t = self.0.write().unwrap();
    t.handle_unpublish(path, addr, client)
  }

  fn handle_list(&self, parent: Path) -> FromResolver {
    if !parent.is_absolute() {
      return FromResolver::Error("list relative path".into())
    }
    let t = self.0.read().unwrap();
    let parent : &str = parent.as_ref();
    let mut res = Vec::new();
    let paths =
      t.published.range::<str, (Bound<&str>, Bound<&str>)>(
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
    FromResolver::List(res)
  }

  fn handle_shutdown(self, client: SocketAddr) {
    let mut t = self.0.write().unwrap();
    let mut paths = Vec::new();
    if let Some(addrs) = t.client_to_addrs.remove(&client) {
      for addr in addrs.into_iter() {
        match t.addr_to_paths.remove(&addr) {
          Some(v) => paths.push((addr, v)),
          None => (),
        }
      }
    }
    for (addr, paths) in paths.into_iter() {
      for path in paths.into_iter() {
        t.handle_unpublish(path, addr, client)
      }
    }
  }

  #[async]
  fn handle_client(
    self, s: TcpStream, client: SocketAddr, stop: oneshot::Receiver<()>
  ) -> result::Result<(), ()> {
    enum M { Stop, Line(String) }
    let (rx, mut tx) = s.split();
    let msgs =
      tokio::io::lines(BufReader::new(rx)).map_err(|_| ()).map(|l| M::Line(l))
      .select(stop.into_stream().map_err(|_| ()).map(|()| M::Stop));
    #[async]
    for msg in msgs {
      match msg {
        M::Stop => break,
        M::Line(l) => {
          match serde_json::from_str::<ToResolver>(&l).map_err(|_| ())? {
            ToResolver::Resolve(path) =>
              tx = await!(send(tx, self.handle_resolve(path)))?,
            ToResolver::List(parent) =>
              tx = await!(send(tx, self.handle_list(parent)))?,
            ToResolver::Publish(path, addr) =>
              tx = await!(send(tx, self.handle_publish(path, addr, client)))?,
            ToResolver::Unpublish(path, addr) => {
              self.handle_unpublish(path, addr, client);
              tx = await!(send(tx, FromResolver::Unpublished))?
            },
          }
        }
      }
    }
    Ok(())
  }

  #[async]
  fn start_client(
    self, s: TcpStream, stop: oneshot::Receiver<()>
  ) -> result::Result<(), ()> {
    match s.peer_addr() {
      Err(_) => Err(()),
      Ok(client) => {
        let _ = await!(self.clone().handle_client(s, client, stop));
        self.handle_shutdown(client);
        Ok(())
      }
    }
  }
}

#[async]
fn accept_loop(
  addr: SocketAddr,
  stop: oneshot::Receiver<()>,
  ready: oneshot::Sender<()>
) -> result::Result<(), ()> {
  let t : Context =
    Context(Arc::new(RwLock::new(ContextInner {
      published: BTreeMap::new(),
      addr_to_paths: HashMap::new(),
      addr_to_client: HashMap::new(),
      client_to_addrs: HashMap::new(),
    })));
  enum M { Stop, Client(TcpStream) }
  let mut stops: Vec<oneshot::Sender<()>> = Vec::new();
  let msgs =
    TcpListener::bind(&addr).map_err(|_| ())?
    .incoming().map_err(|_| ()).map(|c| M::Client(c))
    .select(stop.into_stream().map_err(|_| ()).map(|()| M::Stop));
  ready.send(()).map_err(|_| ())?;
  #[async]
  for msg in msgs {
    match msg {
      M::Stop => {
        for s in stops.drain(0..) { s.send(())? }
        break;
      },
      M::Client(client) => {
        let (send_stop, recv_stop) = oneshot::channel();
        stops.push(send_stop);
        spawn(t.clone().start_client(client, recv_stop));
      }
    }
  }
  Ok(())
}

pub struct Resolver(Option<oneshot::Sender<()>>);

impl Drop for Resolver {
  fn drop(&mut self) {
    let mut stop = None;
    ::std::mem::swap(&mut stop, &mut self.0);
    if let Some(stop) = stop { let _ = stop.send(()); }
  }
}

use error::*;

impl Resolver {
  #[async]
  pub fn new(addr: SocketAddr) -> Result<Resolver> {
    let (send_stop, recv_stop) = oneshot::channel();
    let (send_ready, recv_ready) = oneshot::channel();
    spawn(accept_loop(addr, recv_stop, send_ready));
    await!(recv_ready).map_err(|_| Error::from("ipc error"))?;
    Ok(Resolver(Some(send_stop)))
  }
}
