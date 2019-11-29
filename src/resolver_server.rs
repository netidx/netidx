use crate::{
    utils::MPCodec,
    error::*,
    path::Path,
    resolver_store::Store,
};
use futures::{
    channel::{oneshot, mpsc},
    future::{FutureExt as FRSFutureExt, pending}
};
use std::{
    result, mem,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock, Mutex, atomic::{AtomicUsize, Ordering}},
    time::{Instant, Duration},
    io::BufReader,
    net::SocketAddr,
};
use async_std::{
    prelude::*,
    task,
    future,
    net::{TcpStream, TcpListener},
};
use futures_codec::Framed;
use serde::Serialize;

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

fn handle_msg(store: &Store<ClientInfo>, m: ToResolver, wa: Option<SocketAddr>) -> FromResolver {
    match m {
        ToResolver::Publish(paths) => match wa {
            None => FromResolver::Error("read only client".into()),
            Some(write_addr) => {
                if !paths.iter().all(|p| Path::is_absolute()) {
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
            None => FromResolver::Error("read only client".into()),
            Some(write_addr) => {
                let mut store = store.write().unwrap();
                for path in paths {
                    store.unpublish(path, write_addr);
                }
                FromResolver::Unpublished
            }
        },
        ToResolver::Resolve(paths) => {
            let store = store.read().unwrap();
            FromResolver::Resolved(
                paths.iter().map(|p| store.resolve(p)).collect()
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
    server_stop: impl Future<Output = ()>,
) -> Result<()> {
    enum M { Stop, Timeout, Msg(Result<ToResolver>) }
    s.set_nodelay(true)?;
    let mut codec = Framed::new(s, MPCodec::<'_, ToResolver, FromResolver>::new());
    let hello = codec.next().await?;
    let (tx_stop, rx_stop) = oneshot::channel();
    let (ttl, ttl_expired, write_addr) = match hello {
        ClientHello::ReadOnly => (Duration::from_secs(120), false, None),
        ClientHello::ReadWrite {ttl, write_addr} => {
            if ttl <= 0 || ttl > 3600 { bail!("invalid ttl") }
            let mut store = store.write().unwrap();
            let clinfos = store.clinfos_mut();
            match clinfos.get_mut(&write_addr) {
                None => {
                    clinfos.insert(write_addr, Some(tx_stop));
                    (Duration::from_secs(ttl), true, Some(write_addr))
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
    codec.send(&ServerHello { ttl_expired }).await?;
    let mut codec = Some(codec);
    let stop = server_stop.race(rx_stop).map(|m| M::Stop).shared();
    loop {
        let msg = match codec {
            None => future::pending::<M>(),
            Some(ref mut coded) => codec.next().err_into().map(|m| M::Msg(m))
        };
        let timeout = future::ready(M::Timeout).delay(ttl);
        match msg.race(stop.clone()).race(timeout).await {
            M::Stop => break Ok(()),
            M::Msg(Err(_)) => { codec = None; }
            M::Timeout => {
                if let Some(write_addr) = write_addr {
                    let mut store = store.write().unwrap();
                    if let Some(cl) = store.clinfos_mut().remove(&write_addr) {
                        if let Some(stop) = mem::replace(cl, None) {
                            let _ = stop.send(());
                        }
                    }
                    store.unpublish_addr(write_addr);
                }
                bail!("client timed out");
            }
            M::Msg(Ok(m)) => {
                match codec.unwrap().send(handle_msg(&store, m, write_addr)).await {
                    Err(_) => { codec = None },
                    Ok(()) => ()
                }
            }
        }
    }
}

async fn server_loop(
    addr: SocketAddr,
    max_connections: usize,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<Result<()>>,
) -> Result<()> {
    enum M { Stop, Client(Result<TcpStream>) }
    let connections = Arc::new(AtomicUsize::new(0));
    let published = Store::new();
    let listener = TcpListener::bind(&addr).await?;
    let stop = stop.map(|()| M::Stop).shared();
    ready.send(Ok(()))?;
    loop {
        let client = listener.accept().err_into().map(|c| M::Client(c));
        match stop.clone().race(client).await {
            M::Stop => break,
            M::Client(Err(_)) => (), // CR estokes: log this error
            M::Client(Ok(client)) => {
                if connections.fetch_add(1, Ordering::Relaxed) < max_connections {
                    let published = published.clone();
                    let stop = stop.clone();
                    task::spawn(async move {
                        // CR estokes: log any errors
                        let _ = task::spawn(client_loop(published, client, stop)).await;
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
    pub async fn new(addr: SocketAddr, max_connections: usize) -> Result<Server> {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        task::spawn(server_loop(addr, max_connections, recv_stop, send_ready))
            .race(recv_ready).await?;
        Ok(Server(Some(send_stop)))
    }
}
