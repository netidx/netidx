use futures::{prelude::*, sync::oneshot, sync::mpsc};
use tokio::{self, prelude::*, spawn, net::{TcpStream, TcpListener}};
use tokio_io::io::{WriteHalf, write_all};
use tokio_timer::Delay;
use std::{
    io::BufReader, net::SocketAddr, sync::{Arc, RwLock, Mutex}, result,
    time::{Instant, Duration},
    collections::{HashMap, BTreeSet}
};
use path::Path;
use utils::{BatchItem, batched, GenFuture};
use serde::Serialize;
use serde_json;
use resolver_store::{Action, Store};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ClientHello {
    ReadOnly,
    ReadWrite { ttl: i64, write_addr: SocketAddr }
}
 

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServerHello { pub ttl_expired: bool }

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ToResolver {
    Resolve(Path),
    List(Path),
    Publish(Vec<Path>),
    Unpublish(Vec<Path>),
    Clear
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FromResolver {
    Resolved(Vec<SocketAddr>),
    List(Vec<Path>),
    Published,
    Unpublished,
    Error(String)
}

#[async]
fn send<T: Serialize + 'static>(
    w: WriteHalf<TcpStream>, m: T
) -> result::Result<WriteHalf<TcpStream>, ()> {
    let m = serde_json::to_vec(&m).map_err(|_| ())?;
    let w = await!(write_all(w, m)).map_err(|_| ())?.0;
    Ok(await!(write_all(w, "\n")).map_err(|_| ())?.0)
}

struct ClientInfoInner {
    addr: SocketAddr,
    ttl: Duration,
    published: BTreeSet<Path>,
    stop: Option<oneshot::Sender<()>>,
    timeout: mpsc::UnboundedSender<Duration>
}

#[derive(Clone)]
struct ClientInfo(Arc<Mutex<ClientInfoInner>>);

impl ClientInfo {
    fn new(
        ctx: Context,
        addr: SocketAddr,
        ttl: u64,
        stop: oneshot::Sender<()>
    ) -> Self {
        let ttl = Duration::from_secs(ttl);
        let (tx, rx) = mpsc::unbounded();
        let inner = ClientInfoInner {
            addr, ttl,
            published: BTreeSet::new(),
            stop: Some(stop),
            timeout: tx
        };
        let cl = ClientInfo(Arc::new(Mutex::new(inner)));
        spawn(start_timeout_loop(ctx, cl.clone(), ttl, rx));
        cl
    }
}

struct Stops {
    stops: HashMap<usize, oneshot::Sender<()>>,
    stop_id: usize,
    stopped: bool
}

impl Stops {
    fn new() -> Self {
        Stops {
            stops: HashMap::new(),
            stop_id: 0,
            stopped: false
        }
    }

    fn make(&mut self) -> (oneshot::Receiver<()>, usize) {
        let (tx, rx) = oneshot::channel();
        let id = self.stop_id;
        self.stops.insert(id, tx);
        self.stop_id += 1;
        if self.stopped { let _ = tx.send(()); }
        (rx, id)
    }

    fn remove(&mut self, id: &usize) { self.stops.remove(id); }

    fn stop(&mut self) {
        self.stopped = true;
        for (_, s) in self.stops.drain() { let _ = s.send(()); }
    }
}

#[derive(Clone)]
struct Context {
    published: Store,
    clients: Arc<RwLock<HashMap<SocketAddr, ClientInfo>>>,
    stops: Arc<Mutex<Stops>>,
    connections: Arc<Mutex<usize>>,
}

impl Context {
    fn new() -> Self {
        Context {
            published: Store::new(),
            clients: Arc::new(RwLock::new(HashMap::new())),
            stops: Arc::new(Mutex::new()),
            connections: Arc::new(Mutex::new(0))
        }
    }

    fn timeout_client(&self, client: &mut ClientInfoInner) {
        let mut stop = None;
        ::std::mem::swap(&mut client.stop, &mut stop);
        if let Some(stop) = stop { let _ = stop.send(()); }
        if client.published.len() > 0 {
            self.published.change(client.published.iter().map(|p| {
                (p, (Action::Unpublish, client.addr))
            }))
        }
    }

    fn publish(
        &self, mut paths: Vec<Path>, client: &mut ClientInfoInner
    ) -> FromResolver {
        paths.sort_unstable();
        if !paths.iter().all(|p| p.is_absolute()) {
            return FromResolver::Error("publish relative path".into())
        }
        let paths = self.published.change(paths.into_iter().map(|p| {
            (p, (Action::Publish, client.addr))
        }));
        for p in &paths { client.insert(p.clone()); }
        FromResolver::Published
    }

    fn unpublish(
        &self, mut paths: Vec<Path>, client: &mut ClientInfoInner
    ) -> FromResolver {
        paths.sort_unstable();
        let paths = self.published.unpublish(paths.into_iter().map(|p| {
            (p, (Action::Unpublish, client.addr))
        }));
        for p in &paths { client.published.remove(p); }
        FromResolver::Unpublished
    }
}

fn timeout_loop(
    ctx: Context,
    client: ClientInfo,
    init: Duration,
    mut msgs: mpsc::UnboundedReceiver<Duration>,
    mut server_stop: oneshot::Receiver<()>
) -> impl Future<Item=(), Error=()> {
    // this is a manual generator so we can reset the timer
    let mut timeout = Delay::new(Instant::now() + init);
    let mut generator = || {
        match server_stop.poll() {
            Err(_) => return Err(()),
            Ok(Async::Ready(())) => return Ok(()),
            Ok(Async::NotReady) =>
                match msgs.poll() {
                    Err(_) => return Err(()),
                    Ok(Async::Ready(None)) => return Ok(()),
                    Ok(Async::Ready(Some(ttl))) => timeout.reset(Instant::now() + ttl),
                    Ok(Async::NotReady) =>
                        match timeout.poll() {
                            Err(_) => return Err(()),
                            Ok(Async::NotReady) => yield Async::NotReady,
                            Ok(Async::Ready(_)) => {
                                let mut cl = client.0.lock().unwrap();
                                let mut clients = ctx.clients.write().unwrap();
                                ctx.timeout_client(&mut cl);
                                clients.remove(&cl.addr);
                                return Ok(())
                            }
                        }
                }
        }
        Ok(())
    };
    GenFuture(generator)
}

#[async]
fn start_timeout_loop(
    ctx: Context,
    client: ClientInfo,
    init_ttl: Duration,
    msgs: mpsc::UnboundedReceiver<Duration>
) -> result::Result<(), ()> {
    let (server_stop, cid) = ctx.stops.lock().unwrap().make();
    let _ = await!(timeout_loop(
        ctx.clone(), client.clone(), init_ttl, msgs, server_stop
    ));
    ctx.stops.lock().unwrap().remove(&cid);
    Ok(())
}

#[async]
fn client_loop(
    ctx: Context, s: TcpStream, server_stop: oneshot::Receiver<()>
) -> result::Result<(), ()> {
    enum M { Stop, Line(String) }
    let addr = s.peer_addr().map_err(|_| ())?;
    s.set_nodelay(true).map_err(|_| ())?;
    let (rx, mut tx) = s.split();
    let msgs =
        tokio::io::lines(BufReader::new(rx)).map_err(|_| ()).map(|l| M::Line(l))
        .select(server_stop.into_stream().map_err(|_| ()).map(|()| M::Stop));
    let (hello, msgs) =
        match await!(msgs.into_future()) {
            Err(..) => return Err(()),
            Ok((None, _)) => return Err(()),
            Ok((Some(M::Stop), _)) => return Ok(()),
            Ok((Some(M::Line(l)), msgs)) =>
                (serde_json::from_str::<ClientHello>(&l).map_err(|_| ())?, msgs)
        };
    let (client, client_stop, mut client_added) = {
        let (tx_stop, rx_stop) = oneshot::channel();
        let (client, added, ttl_expired) =
            match hello {
                ClientHello::ReadOnly =>
                    (ClientInfo::new(addr, 120, tx_stop), false, false),
                ClientHello::ReadWrite {ttl, write_addr} => {
                    if ttl <= 0 || ttl > 3600 { return Err(()) }
                    match ctx.clients.read().unwrap().get(&write_addr) {
                        None => {
                            let c = ClientInfo::new(
                                ctx.clone(), write_addr, ttl as u64, tx_stop
                            );
                            (c, false, true)
                        },
                        Some(client) => {
                            let mut cl = client.0.lock().unwrap();
                            cl.ttl = ttl;
                            cl.timeout.unbounded_send(ttl).map_err(|_| ())?;
                            cl.stop = Some(tx_stop);
                            (client.clone(), true, false)
                        }
                    }
                }
            };
        tx = await!(send::<ServerHello>(tx, ServerHello { ttl_expired }))?;
        (client, rx_stop, added)
    };
    let maybe_add_client = || {
        if !client_added {
            *client_added = true;
            match hello {
                ClientHello::ReadOnly => Err(()),
                ClientHello::ReadWrite {write_addr, ..} => {
                    let mut c = ctx.clients.write().lock().unwrap();
                    c.insert(write_addr, client.clone());
                    Ok(())
                }
            }
        }
    };
    let msgs = msgs.select(client_stop.into_stream().map_err(|_| ()).map(|_| M::Stop));
    let msgs = batched(msgs, 1000);
    let mut batch : Vec<ToResolver> = Vec::new();
    let mut response : Vec<FromResolver> = Vec::new();
    #[async]
    for msg in msgs {
        match msg {
            BatchItem::InBatch(m) =>
                match m {
                    M::Stop => break,
                    M::Line(l) =>
                        batch.push(
                            serde_json::from_str::<ToResolver>(&l).map_err(|_| ())?
                        )
                },
            BatchItem::EndBatch => {
                let mut ci = client.0.lock().unwrap();
                let mut store = ctx.store.read();
                ci.timeout.unbounded_send(ci.ttl).map_err(|_| ())?;
                for m in batch.drain(0..) {
                    match m {
                        ToResolver::Publish(paths) => {
                            maybe_add_client()?;
                            response.push(ctx.publish(paths, ci));
                            store = ctx.store.read();
                        },
                        ToResolver::Unpublish(paths) => {
                            maybe_add_client()?;
                            response.push(ctx.unpublish(paths, ci));
                            store = ctx.store.read();
                        },
                        ToResolver::Resolve(path) => {
                            let r = resolver_store::resolve(&store, &path);
                            response.push(FromResolver::Resolved(r))
                        },
                        ToResolver::List(path) => {
                            let r = resolver_store::list(&store, &path);
                            response.push(FromResolver::List(r))
                        }
                    }
                }
                while let Some(m) = response.pop() {
                    tx = await!(send(tx, m)).map_err(|_| ())?;
                }
            }
        }
    }
    Ok(())
}

#[async]
fn start_client_loop(ctx: Context, s: TcpStream) -> result::Result<(), ()> {
    let (server_stop, client) = ctx.stops.lock().unwrap().make();
    let _ = await!(client_loop(ctx.clone(), s, server_stop));
    *ctx.connections.lock().unwrap() -= 1;
    ctx.stops.lock().unwrap().remove(&client);
    Ok(())
}

#[async]
fn accept_loop(
    addr: SocketAddr,
    max_connections: usize,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<()>,
) -> result::Result<(), ()> {
    let t = Context::new();
    enum M { Stop, Client(TcpStream) }
    let msgs =
        TcpListener::bind(&addr).map_err(|_| ())?
        .incoming().map_err(|_| ()).map(|c| M::Client(c))
        .select(stop.into_stream().map_err(|_| ()).map(|()| M::Stop));
    let _ = ready.send(());
    #[async]
    for msg in msgs {
        match msg {
            M::Client(client) => {
                let mut connections = t.connections.lock().unwrap();
                *connections += 1;
                if *connections < max_connections {
                    spawn(start_client_loop(t.clone(), client));
                }
            },
            M::Stop => break,
        }
    }
    t.stops.lock().unwrap().stop();
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
    pub fn new(addr: SocketAddr, max_connections: usize) -> Result<Server> {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        spawn(accept_loop(addr, max_connections, recv_stop, send_ready));
        await!(recv_ready).map_err(|_| Error::from("ipc error"))?;
        Ok(Server(Some(send_stop)))
    }
}
