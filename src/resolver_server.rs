use crate::{
    utils::{MPCodec, Batched, BatchItem},
    path::Path,
    resolver_store::Store,
};
use futures::{
    channel::oneshot,
    sink::SinkExt,
    future::{FutureExt as FRSFutureExt},
    stream,
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
    WriteOnly { ttl: u64, write_addr: SocketAddr }
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

fn handle_batch(
    store: &Store<ClientInfo>,
    msgs: impl Iterator<Item = ToResolver>,
    results: &mut Vec<FromResolver>,
    wa: Option<SocketAddr>
) {
    match wa {
        None => {
            let s = store.read();
            results.extend(msgs.map(move |m| match m {
                ToResolver::Resolve(paths) => {
                    FromResolver::Resolved(paths.iter().map(|p| s.resolve(p)).collect())
                },
                ToResolver::List(path) => {
                    FromResolver::List(s.list(&path))
                }
                ToResolver::Publish(_) | ToResolver::Unpublish(_) | ToResolver::Clear =>
                    FromResolver::Error("read only".into()),
            }))
        }
        Some(write_addr) => {
            let mut s = store.write();
            results.extend(msgs.map(move |m| match m {
                ToResolver::Resolve(_) | ToResolver::List(_) =>
                    FromResolver::Error("write only".into()),
                ToResolver::Publish(paths) => {
                    if !paths.iter().all(Path::is_absolute) {
                        FromResolver::Error("absolute paths required".into())
                    } else {
                        for path in paths {
                            s.publish(path, write_addr);
                        }
                        FromResolver::Published
                    }
                }
                ToResolver::Unpublish(paths) => {
                    for path in paths {
                        s.unpublish(path, write_addr);
                    }
                    FromResolver::Unpublished
                }
                ToResolver::Clear => {
                    s.unpublish_addr(write_addr);
                    s.gc();
                    FromResolver::Unpublished
                }
            }))
        }
    }
}

async fn client_loop(
    store: Store<ClientInfo>,
    s: TcpStream,
    server_stop: impl Future<Output = result::Result<(), oneshot::Canceled>>,
) {
    #[derive(Debug)]
    enum M {
        Stop,
        Timeout,
        Msg(Option<BatchItem<result::Result<ToResolver, Error>>>)
    }
    try_ret!("can't set nodelay", s.set_nodelay(true));
    let mut codec = Framed::new(s, MPCodec::<ServerHello, ClientHello>::new());
    let hello = match codec.next().await {
        None => ret!("no hello from client"),
        Some(h) => try_ret!("error reading hello", h)
    };
    let (tx_stop, rx_stop) = oneshot::channel();
    let (ttl, ttl_expired, write_addr) = match hello {
        ClientHello::ReadOnly => (Duration::from_secs(120), false, None),
        ClientHello::WriteOnly {ttl, write_addr} => {
            if ttl <= 0 || ttl > 3600 { ret!("invalid ttl") }
            let mut store = store.write();
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
    let mut codec = {
        let c = Framed::new(
            codec.release().0,
            MPCodec::<FromResolver, ToResolver>::new()
        );
        Some(Batched::new(c, 100_000))
    };
    let server_stop = server_stop.shared();
    let rx_stop = rx_stop.shared();
    let mut batch = Vec::new();
    let mut results = Vec::new();
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
            M::Msg(Some(BatchItem::InBatch(Err(e)))) => {
                codec = None;
                // CR estokes: use proper log module
                println!("error reading message: {}", e)
            },
            M::Msg(Some(BatchItem::InBatch(Ok(m)))) => { batch.push(m); }
            M::Msg(Some(BatchItem::EndBatch)) => match codec {
                None => { batch.clear(); }
                Some(ref mut c) => {
                    handle_batch(&store, batch.drain(..), &mut results, write_addr);
                    let mut s = stream::iter(results.drain(..).map(|m| Ok(m)));
                    match c.send_all(&mut s).await {
                        Err(_) => { codec = None }, // CR estokes: Log this
                        Ok(()) => ()
                    }
                }
            }
            M::Timeout => {
                if let Some(write_addr) = write_addr {
                    let mut store = store.write();
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
    ready: oneshot::Sender<SocketAddr>,
) -> Result<SocketAddr, Error> {
    enum M { Stop, Drop, Client(TcpStream) }
    let connections = Arc::new(AtomicUsize::new(0));
    let published: Store<ClientInfo> = Store::new();
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let stop = stop.shared();
    let _ = ready.send(local_addr);
    loop {
        let client = listener.accept().map(|c| match c {
            Ok((c, _)) => M::Client(c),
            Err(_) => M::Drop, // CR estokes: maybe log this?
        });
        let should_stop = stop.clone().map(|_| M::Stop);
        match should_stop.race(client).await {
            M::Stop => return Ok(local_addr),
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

#[derive(Debug)]
pub struct Server {
    stop: Option<oneshot::Sender<()>>,
    local_addr: SocketAddr,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = mem::replace(&mut self.stop, None) {
            let _ = stop.send(());
        }
    }
}

impl Server {
    pub async fn new(addr: SocketAddr, max_connections: usize) -> Result<Server, Error> {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        let local_addr =
            task::spawn(server_loop(addr, max_connections, recv_stop, send_ready))
            .race(recv_ready.map(|r| r.map_err(|e| Error::from(e))))
            .await?;
        Ok(Server {
            stop: Some(send_stop),
            local_addr
        })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::SocketAddr;
    use crate::resolver::{WriteOnly, ReadOnly, Resolver};

    async fn init_server() -> Server {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        Server::new(addr, 100).await.expect("start server")
    }

    fn p(p: &str) -> Path {
        Path::from(p)
    }

    #[test]
    fn publish_resolve() {
        use async_std::task;
        task::block_on(async {
            let server = init_server().await;
            let paddr: SocketAddr = "127.0.0.1:1".parse().unwrap();
            let mut w = Resolver::<WriteOnly>::new_w(server.local_addr(), paddr).unwrap();
            let mut r = Resolver::<ReadOnly>::new_r(server.local_addr()).unwrap();
            let paths = vec![
                p("/foo/bar"),
                p("/foo/baz"),
                p("/app/v0"),
                p("/app/v1"),
            ];
            w.publish(paths.clone()).await.unwrap();
            for addrs in r.resolve(paths.clone()).await.unwrap() {
                assert_eq!(addrs.len(), 1);
                assert_eq!(addrs[0], paddr);
            }
            assert_eq!(
                r.list(p("/")).await.unwrap(),
                vec![p("/app"), p("/foo")]
            );
            assert_eq!(
                r.list(p("/foo")).await.unwrap(),
                vec![p("/foo/bar"), p("/foo/baz")]
            );
            assert_eq!(
                r.list(p("/app")).await.unwrap(),
                vec![p("/app/v0"), p("/app/v1")]
            );
        });
    }
}
