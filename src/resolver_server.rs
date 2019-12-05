use crate::{
    utils::MPCodec,
    path::Path,
    resolver_store::Store,
};
use futures::{
    channel::oneshot,
    sink::SinkExt,
    future::{FutureExt as FRSFutureExt}
};
use std::{
    result, mem,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::Duration,
    net::SocketAddr,
};
use async_std::{
    prelude::*,
    task,
    future,
    stream::StreamExt,
    net::{TcpStream, TcpListener},
};
use futures_codec::Framed;
use serde::Serialize;
use failure::Error;

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

type ClientInfo = Option<oneshot::Sender<()>>;

fn handle_msg(
    store: &Store<ClientInfo>,
    m: ToResolver,
    wa: Option<SocketAddr>
) -> FromResolver {
    match m {
        ToResolver::Publish(paths) => match wa {
            None => FromResolver::Error("read only".into()),
            Some(write_addr) => {
                if !paths.iter().all(Path::is_absolute) {
                    FromResolver::Error("absolute paths required".into())
                } else {
                    let mut store = store.write().unwrap();
                    for path in paths {
                        store.publish(path, write_addr);
                    }
                    FromResolver::Published
                }
            }
        },
        ToResolver::Unpublish(paths) => match wa {
            None => FromResolver::Error("read only".into()),
            Some(write_addr) => {
                let mut store = store.write().unwrap();
                for path in paths {
                    store.unpublish(path, write_addr);
                }
                FromResolver::Unpublished
            }
        },
        ToResolver::Clear => match wa {
            None => FromResolver::Error("read only".into()),
            Some(write_addr) => {
                let mut store = store.write().unwrap();
                store.unpublish_addr(write_addr);
                store.gc();
                FromResolver::Unpublished
            }
        }
        ToResolver::Resolve(paths) => {
            use rayon::prelude::*;
            FromResolver::Resolved(
                paths.par_iter().map_init(|| store.read().unwrap(), |s, p| s.resolve(p))
                    .collect()
            )
        },
        ToResolver::List(path) => {
            FromResolver::List(store.read().unwrap().list(&path))
        }
    }
}

async fn client_loop(
    store: Store<ClientInfo>,
    s: TcpStream,
    server_stop: impl Future<Output = result::Result<(), oneshot::Canceled>>,
) {
    enum M { Stop, Timeout, Msg(Option<result::Result<ToResolver, Error>>) }
    try_ret!("can't set nodelay", s.set_nodelay(true));
    let mut codec = Framed::new(s, MPCodec::<ServerHello, ClientHello>::new());
    let hello = match codec.next().await {
        None => ret!("no hello from client"),
        Some(h) => try_ret!("error reading hello", h)
    };
    let (tx_stop, rx_stop) = oneshot::channel();
    let (ttl, ttl_expired, write_addr) = match hello {
        ClientHello::ReadOnly => (Duration::from_secs(120), false, None),
        ClientHello::ReadWrite {ttl, write_addr} => {
            if ttl <= 0 || ttl > 3600 { ret!("invalid ttl") }
            let mut store = store.write().unwrap();
            let clinfos = store.clinfo_mut();
            let ttl = Duration::from_secs(ttl);
            match clinfos.get_mut(&write_addr) {
                None => {
                    clinfos.insert(write_addr, Some(tx_stop));
                    (ttl, true, Some(write_addr))
                },
                Some(cl) => {
                    if let Some(old_stop) = mem::replace(cl, Some(tx_stop)) {
                        let _ = old_stop.send(());
                    }
                    (ttl, false, Some(write_addr))
                }
            }
        }
    };
    try_ret!("couldn't send hello", codec.send(ServerHello { ttl_expired }).await);
    let mut codec = Some(Framed::new(
        codec.release().0,
        MPCodec::<FromResolver, ToResolver>::new()
    ));
    let server_stop = server_stop.shared();
    let rx_stop = rx_stop.shared();
    loop {
        let msg = match codec {
            None => future::pending::<M>().left_future(),
            Some(ref mut codec) => codec.next().map(|m| M::Msg(m)).right_future()
        };
        let timeout = future::ready(M::Timeout).delay(ttl);
        let stop =
            server_stop.clone().map(|_| M::Stop)
            .race(rx_stop.clone().map(|_| M::Stop));
        match msg.race(stop).race(timeout).await {
            M::Stop => break,
            M::Msg(None) => { codec = None; }
            M::Msg(Some(Err(e))) => {
                codec = None;
                // CR estokes: use proper log module
                println!("error reading message: {}", e)
            },
            M::Msg(Some(Ok(m))) => {
                match codec {
                    None => (),
                    Some(ref mut c) =>
                        match c.send(handle_msg(&store, m, write_addr)).await {
                            Err(_) => { codec = None }, // CR estokes: Log this
                            Ok(()) => ()
                        }
                }
            }
            M::Timeout => {
                if let Some(write_addr) = write_addr {
                    let mut store = store.write().unwrap();
                    if let Some(ref mut cl) = store.clinfo_mut().remove(&write_addr) {
                        if let Some(stop) = mem::replace(cl, None) {
                            let _ = stop.send(());
                        }
                    }
                    store.unpublish_addr(write_addr);
                    store.gc();
                }
                ret!("client timed out");
            }
        }
    }
}

async fn server_loop(
    addr: SocketAddr,
    max_connections: usize,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<()>,
) {
    enum M { Stop, Drop, Client(TcpStream) }
    let connections = Arc::new(AtomicUsize::new(0));
    let published: Store<ClientInfo> = Store::new();
    let listener = try_ret!("TcpListener::bind", TcpListener::bind(addr).await);
    let stop = stop.shared();
    let _ = ready.send(());
    loop {
        let client = listener.accept().map(|c| match c {
            Ok((c, _)) => M::Client(c),
            Err(_) => M::Drop, // CR estokes: maybe log this?
        });
        let should_stop = stop.clone().map(|_| M::Stop);
        match should_stop.race(client).await {
            M::Stop => return,
            M::Drop => (),
            M::Client(client) => {
                if connections.fetch_add(1, Ordering::Relaxed) < max_connections {
                    let connections = connections.clone();
                    let published = published.clone();
                    let stop = stop.clone();
                    task::spawn(async move {
                        // CR estokes: log any errors
                        task::spawn(client_loop(published, client, stop)).await;
                        connections.fetch_sub(1, Ordering::Relaxed);
                    });
                }
            },
        }
    }
}

pub struct Server(Option<oneshot::Sender<()>>);

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = mem::replace(&mut self.0, None) {
            let _ = stop.send(());
        }
    }
}

impl Server {
    pub async fn new(addr: SocketAddr, max_connections: usize) -> Server {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        task::spawn(server_loop(addr, max_connections, recv_stop, send_ready))
            .race(recv_ready.map(|_| ())).await;
        Server(Some(send_stop))
    }
}
