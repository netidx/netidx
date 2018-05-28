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

struct ContextInner {
  published: BTreeMap<Path, Published>,
  addr_to_paths: HashMap<SocketAddr, HashSet<Path>>,
  addr_to_client: HashMap<SocketAddr, SocketAddr>,
  client_to_addrs: HashMap<SocketAddr, HashSet<SocketAddr>>
}

type Context = Arc<RwLock<ContextInner>>;

#[async]
fn send(
  w: WriteHalf<TcpStream>,
  m: FromResolver
) -> result::Result<WriteHalf<TcpStream>, ()> {
  let m = serde_json::to_vec(&m).map_err(|_| ())?;
  let w = await!(write_all(w, m)).map_err(|_| ())?.0;
  Ok(await!(write_all(w, "\n")).map_err(|_| ())?.0)
}

fn handle_resolve(t: &Context, path: Path) -> FromResolver {
  if !path.is_absolute() { FromResolver::Error("resolve relative path".into()) }
  else {
    match t.read().unwrap().published.get(&path) {
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
  t: &Context,
  path: Path,
  addr: SocketAddr,
  client: SocketAddr
) -> FromResolver {
  if !path.is_absolute() {
    return FromResolver::Error("publish relative path".into())
  }
  let mut t = t.write().unwrap();
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

fn handle_unpublish_nolock(
  t: &mut ContextInner,
  path: Path,
  addr: SocketAddr,
  client: SocketAddr
) {
  match t.addr_to_paths.entry(addr) {
    Entry::Vacant(_) => (),
    Entry::Occupied(mut e) => {
      let empty = {
        let mut set = e.get_mut();
        set.remove(&path);
        set.is_empty()
      };
      if empty {
        e.remove_entry();
        t.addr_to_client.remove(&addr);
        match t.client_to_addrs.entry(client) {
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
    match t.published.get_mut(&path) {
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
    t.published.remove(&path);
    // remove parents that have no further children
    let mut p : &str = path.as_ref();
    loop {
      match path::dirname(p) {
        None => break,
        Some(parent) => {
          let remove = {
            let mut r =
              t.published.range::<str, (Bound<&str>, Bound<&str>)>(
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
          if remove { t.published.remove(parent); }
          p = parent;
        }
      }
    }
  }
}

fn handle_unpublish(t: &Context, path: Path, addr: SocketAddr, client: SocketAddr) {
  let mut t = t.write().unwrap();
  handle_unpublish_nolock(&mut t, path, addr, client)
}

fn handle_list(t: &Context, parent: Path) -> FromResolver {
  if !parent.is_absolute() {
    return FromResolver::Error("list relative path".into())
  }
  let t = t.read().unwrap();
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

fn handle_shutdown(t: Context, client: SocketAddr) {
  let mut t = t.write().unwrap();
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
      handle_unpublish_nolock(&mut t, path, addr, client)
    }
  }
}

#[async]
fn handle_client(
  t: Context, s: TcpStream, client: SocketAddr, stop: oneshot::Receiver<()>
) -> Result<(), ()> {
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
          ToResolver::Resolve(path) => tx = await!(send(tx, handle_resolve(&t, path)))?,
          ToResolver::List(parent) => tx = await!(send(tx, handle_list(&t, parent)))?,
          ToResolver::Publish(path, addr) =>
            tx = await!(send(tx, handle_publish(&t, path, addr, client)))?,
          ToResolver::Unpublish(path, addr) => {
            handle_unpublish(&t, path, addr, client);
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
  t: Context, s: TcpStream, stop: oneshot::Receiver<()>
) -> result::Result<(), ()> {
  match s.peer_addr() {
    Err(_) => Err(()),
    Ok(client) => {
      let _ = await!(handle_client(t.clone(), s, client, stop));
      handle_shutdown(t, client);
      Ok(())
    }
  }
}

#[async]
fn accept_loop(
  addr: SocketAddr,
  stop: oneshot::Receiver<()>
) -> result::Result<(), ()> {
  let t : Context =
    Arc::new(RwLock::new(ContextInner {
      published: BTreeMap::new(),
      addr_to_paths: HashMap::new(),
      addr_to_client: HashMap::new(),
      client_to_addrs: HashMap::new()
    }));
  enum M { Stop, Client(TcpStream) }
  let mut stops: Vec<oneshot::Sender<()>> = Vec::new();
  let msgs =
    TcpListener::bind(&addr).map_err(|_| ())?
    .incoming().map_err(|_| ()).map(|c| M::Client(c))
    .select(stop.into_stream().map_err(|_| ()).map(|()| M::Stop));
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
        spawn(start_client(t.clone(), client, recv_stop));
      }
    }
  }
  Ok(())
}

pub struct Resolver(oneshot::Sender<()>);

impl Drop for Resolver {
  fn drop(&mut self) {
    // this is kinda silly, but we must own the channel to use it
    let (mut stop, _) = oneshot::channel();
    ::std::mem::swap(&mut stop, &mut self.0);
    let _ = stop.send(());
  }
}

pub fn run(addr: SocketAddr) -> Resolver {
  let (send_stop, recv_stop) = oneshot::channel();
  spawn(accept_loop(addr, recv_stop));
  Resolver(send_stop)
}
