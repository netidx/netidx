use crate::{
    channel::Channel,
    path::Path,
    resolver_store::Store,
};
use tokio::{
    task, time::{self, Instant}, sync::oneshot,
    net::{TcpStream, TcpListener}
};
use futures::{
    select,
    prelude::*,
};
use std::{
    mem, io,
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::Duration,
    net::SocketAddr,
};
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
    Clear,
    Heartbeat
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
    con: &mut Channel,
    wa: Option<SocketAddr>
) -> Result<(), Error> {
    match wa {
        None => {
            let s = store.read();
            for m in msgs {
                match m {
                    ToResolver::Heartbeat => (),
                    ToResolver::Resolve(paths) => {
                        let res = paths.iter().map(|p| s.resolve(p)).collect();
                        con.queue_send(&FromResolver::Resolved(res))?
                    },
                    ToResolver::List(path) => {
                        con.queue_send(&FromResolver::List(s.list(&path)))?
                    }
                    ToResolver::Publish(_)
                        | ToResolver::Unpublish(_)
                        | ToResolver::Clear =>
                        con.queue_send(&FromResolver::Error("read only".into()))?,
                }
            }
        }
        Some(write_addr) => {
            let mut s = store.write();
            for m in msgs {
                match m {
                    ToResolver::Heartbeat => (),
                    ToResolver::Resolve(_) | ToResolver::List(_) =>
                        con.queue_send(&FromResolver::Error("write only".into()))?,
                    ToResolver::Publish(paths) => {
                        if !paths.iter().all(Path::is_absolute) {
                            con.queue_send(
                                &FromResolver::Error("absolute paths required".into())
                            )?
                        } else {
                            for path in paths {
                                s.publish(path, write_addr);
                            }
                            con.queue_send(&FromResolver::Published)?
                        }
                    }
                    ToResolver::Unpublish(paths) => {
                        for path in paths {
                            s.unpublish(path, write_addr);
                        }
                        con.queue_send(&FromResolver::Unpublished)?
                    }
                    ToResolver::Clear => {
                        s.unpublish_addr(write_addr);
                        s.gc();
                        con.queue_send(&FromResolver::Unpublished)?
                    }
                }
            }
        }
    }
    Ok(())
}

static HELLO_TIMEOUT: Duration = Duration::from_secs(10);
static READER_TTL: Duration = Duration::from_secs(120);
static MAX_TTL: u64 = 3600;

async fn client_loop(
    store: Store<ClientInfo>,
    s: TcpStream,
    server_stop: oneshot::Receiver<()>,
) -> Result<(), Error> {
    s.set_nodelay(true)?;
    let mut con = Channel::new(s);
    let (tx_stop, rx_stop) = oneshot::channel();
    let hello: ClientHello = time::timeout(HELLO_TIMEOUT, con.receive()).await??;
    let (ttl, ttl_expired, write_addr) = match hello {
        ClientHello::ReadOnly => (READER_TTL, false, None),
        ClientHello::WriteOnly {ttl, write_addr} => {
            if ttl <= 0 || ttl > MAX_TTL { bail!("invalid ttl") }
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
    time::timeout(HELLO_TIMEOUT, con.send_one(&ServerHello { ttl_expired })).await??;
    let mut con = Some(con);
    let mut server_stop = server_stop.fuse();
    let mut rx_stop = rx_stop.fuse();
    let mut batch = Vec::new();
    let mut act = false;
    let mut timeout = time::interval_at(Instant::now() + ttl, ttl).fuse();
    async fn receive_batch(
        con: &mut Option<Channel>,
        batch: &mut Vec<ToResolver>
    ) -> Result<(), io::Error> {
        match con {
            Some(ref mut con) => con.receive_batch(batch).await,
            None => future::pending().await
        }
    }
    loop {
        select! {
            _ = server_stop => break Ok(()),
            _ = rx_stop => break Ok(()),
            m = receive_batch(&mut con, &mut batch).fuse() => match m {
                Err(e) => {
                    batch.clear();
                    con = None;
                    // CR estokes: use proper log module
                    println!("error reading message: {}", e)
                },
                Ok(()) => {
                    act = true;
                    let c = con.as_mut().unwrap();
                    match handle_batch(&store, batch.drain(..), c, write_addr) {
                        Err(_) => { con = None },
                        Ok(()) => match c.flush().await {
                            Err(_) => { con = None }, // CR estokes: Log this
                            Ok(()) => ()
                        }
                    }
                }
            },
            _ = timeout.next() => {
                if act {
                    act = false;
                } else {
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
                    bail!("client timed out");
                }
            },
        }
    }
}

async fn server_loop(
    addr: SocketAddr,
    max_connections: usize,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<SocketAddr>,
) -> Result<SocketAddr, Error> {
    let connections = Arc::new(AtomicUsize::new(0));
    let published: Store<ClientInfo> = Store::new();
    let mut listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let mut stop = stop.fuse();
    let mut client_stops = Vec::new();
    let _ = ready.send(local_addr);
    loop {
        select! {
            cl = listener.accept().fuse() => match cl {
                Err(_) => (),
                Ok((client, _)) => {
                    if connections.fetch_add(1, Ordering::Relaxed) < max_connections {
                        let connections = connections.clone();
                        let published = published.clone();
                        let (tx, rx) = oneshot::channel();
                        client_stops.push(tx);
                        task::spawn(async move {
                            let _ = client_loop(published, client, rx).await;
                            connections.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                }
            },
            _ = stop => {
                for cl in client_stops.drain(..) {
                    let _ = cl.send(());
                }
                return Ok(local_addr)
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
        let tsk = server_loop(addr, max_connections, recv_stop, send_ready);
        let local_addr = select! {
            a = task::spawn(tsk).fuse() => a??,
            a = recv_ready.fuse() => a?,
        };
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
    use std::net::SocketAddr;
    use crate::{
        path::Path,
        resolver_server::Server,
        resolver::{WriteOnly, ReadOnly, Resolver},
    };

    async fn init_server() -> Server {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        Server::new(addr, 100).await.expect("start server")
    }

    fn p(p: &str) -> Path {
        Path::from(p)
    }

    #[test]
    fn publish_resolve() {
        use tokio::runtime::Runtime;
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
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
