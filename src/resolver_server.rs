use crate::utils::MPCodec;
use futures::{
    sync::oneshot,
    sync::mpsc,
    future::{FutureExt as FRSFutureExt, pending}
};
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
use path::Path;
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

type ClientInfo = Option<oneshot::Sender<()>>;

async fn client_loop(
    store: Store<ClientInfo>,
    s: TcpStream,
    server_stop: oneshot::Receiver<()>
) -> Result<()> {
    enum M { Stop, Timeout, Msg(Result<ToResolver>) }
    s.set_nodelay(true).await?;
    let mut codec = Framed::new(s, MPCodec::new<'_, ToResolver, FromResolver>());
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
                let response = match m {
                    ToResolver::Publish(paths) => match write_addr {
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
                    ToResolver::Unpublish(paths) => match write_addr {
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
                };
                match codec.unwrap().send(response).await {
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
    ready: oneshot::Sender<()>,
) {
    enum M { Stop, Client(TcpStream) }
    let mut connections = 0;
    let mut stops = Stops::new();
    let published = Store::new();
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
