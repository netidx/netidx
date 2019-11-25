use futures::{sync::oneshot, sync::mpsc, future::FutureExt as FRSFutureExt};
use std::{
    result, mem,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock, Mutex}
    time::{Instant, Duration},
    io::BufReader,
    net::SocketAddr,
};
use async_std::{prelude::*, task, future};
use futures_codec::Framed;
use futures_cbor_codec::Codec;
use path::Path;
use utils::MPCodec;
use serde::Serialize;
use resolver_store::{Action, Store};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ClientHello {
    ReadOnly,
    ReadWrite { ttl: u64, write_addr: SocketAddr }
}
 

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ServerHello { pub ttl_expired: bool }

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ToResolver {
    Resolve(Vec<Path>),
    List(Path),
    Publish(Vec<Path>),
    Unpublish(Vec<Path>),
    Clear
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FromResolver {
    Resolved(Vec<Vec<SocketAddr>>),
    List(Vec<Path>),
    Published,
    Unpublished,
    Error(String)
}

/*
#[async]
fn send<T: Serialize + 'static>(
    w: WriteHalf<TcpStream>, m: T
) -> result::Result<WriteHalf<TcpStream>, ()> {
    let m = serde_json::to_vec(&m).map_err(|_| ())?;
    let w = await!(write_all(w, m)).map_err(|_| ())?.0;
    Ok(await!(write_all(w, "\n")).map_err(|_| ())?.0)
}
*/

struct ClientInfoInner {
    addr: SocketAddr,
    ttl: Duration,
    published: HashSet<Path>,
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
            published: HashSet::new(),
            stop: Some(stop),
            timeout: tx
        };
        let cl = ClientInfo(Arc::new(Mutex::new(inner)));
        task::spawn(timeout_loop(ctx, cl.clone(), ttl, rx));
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
        if let Some(s) = mem::replace(&mut client.stop, None) { let _ = s.send(()); }
        if client.published.len() > 0 {
            self.published.change(client.published.iter().map(|p| {
                (p, (Action::Unpublish, client.addr))
            }))
        }
    }

    fn publish(
        &self, mut paths: Vec<Path>, client: &mut ClientInfoInner
    ) -> FromResolver {
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
        let paths = self.published.unpublish(paths.into_iter().map(|p| {
            (p, (Action::Unpublish, client.addr))
        }));
        for p in &paths { client.published.remove(p); }
        FromResolver::Unpublished
    }
}

async fn timeout_loop(
    ctx: Context,
    client: ClientInfo,
    init: Duration,
    msgs: mpsc::UnboundedReceiver<Duration>,
) {
    enum M {
        Stop,
        Timeout,
        Activity(Duration),
    }
    let mut timeout = init;
    let (server_stop, cid) = ctx.stops.lock().unwrap().make();
    let stop = server_stop.map(|_| M::Stop).shared();
    loop {
        let timeout = future::ready(M::Timeout).delay(timeout);
        let activity = msgs.next().map(|v| match v {
            Err(_) => M::Stop,
            Ok(v) => M::Activity(v),
        });
        match timeout.race(stop.clone()).race(activity).await {
            M::Stop => break,
            M::Activity(ttl) => { timeout = ttl; },
            M::Timeout => {
                let mut cl = client.0.lock().unwrap();
                let mut clients = ctx.clients.write().unwrap();
                ctx.timeout_client(&mut cl);
                clients.remove(&cl.addr);
                return Ok(())
            }
        }
    }
    ctx.stops.lock().unwrap().remove(&cid);
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
                            let mut old_stop = None;
                            swap(&mut cl.stop, old_stop);
                            if let Some(old_stop) = old_stop {
                                let _ = old_stop.send(());
                            }
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
    let msgs = batched(msgs, 100000);
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
