use crate::{
    auth::{
        syskrb5::ServerCtx, Krb5Ctx, Krb5ServerCtx, Permissions, UserInfo, ANONYMOUS,
    },
    channel::Channel,
    config,
    path::Path,
    protocol::{
        publisher,
        resolver::{
            ClientAuthRead, ClientAuthWrite, ClientHello, ClientHelloWrite, FromRead,
            FromWrite, Resolved, ResolverId, ServerAuthWrite, ServerHelloRead,
            ServerHelloWrite, ToRead, ToWrite,
        },
    },
    resolver_store::Store,
    secstore::SecStore,
};
use failure::Error;
use futures::{prelude::*, select};
use fxhash::FxBuildHasher;
use rand::Rng;
use std::{
    collections::HashMap,
    convert::TryFrom,
    mem,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task,
    time::{self, Instant},
};

type ClientInfo = Option<oneshot::Sender<()>>;

static HELLO_TIMEOUT: Duration = Duration::from_secs(10);
static READER_TTL: Duration = Duration::from_secs(60);
static WRITER_TTL: Duration = Duration::from_secs(120);

fn allowed_for(
    secstore: Option<&SecStore>,
    uifo: &Arc<UserInfo>,
    paths: &Vec<Path>,
    perm: Permissions,
) -> bool {
    secstore
        .map(|s| {
            let paths = paths.iter().map(|p| p.as_ref());
            s.pmap().allowed_forall(paths, perm, uifo)
        })
        .unwrap_or(true)
}

fn handle_batch_write(
    store: &Store<ClientInfo>,
    con: &mut Channel<ServerCtx>,
    secstore: Option<&SecStore>,
    uifo: &Arc<UserInfo>,
    write_addr: SocketAddr,
    msgs: impl Iterator<Item = ToWrite>,
) -> Result<(), Error> {
    let mut s = store.write();
    for m in msgs {
        match m {
            ToWrite::Heartbeat => (),
            ToWrite::Publish(paths) => {
                if !paths.iter().all(Path::is_absolute) {
                    con.queue_send(&FromWrite::Error("absolute paths required".into()))?
                } else {
                    if allowed_for(secstore, uifo, &paths, Permissions::PUBLISH) {
                        for path in paths {
                            s.publish(path, write_addr);
                        }
                        con.queue_send(&FromWrite::Published)?
                    } else {
                        con.queue_send(&FromWrite::Error("denied".into()))?
                    }
                }
            }
            ToWrite::Unpublish(paths) => {
                for path in paths {
                    s.unpublish(path, write_addr);
                }
                con.queue_send(&FromWrite::Unpublished)?
            }
            ToWrite::Clear => {
                s.unpublish_addr(write_addr);
                s.gc();
                con.queue_send(&FromWrite::Unpublished)?
            }
        }
    }
    Ok(())
}

async fn client_loop_write(
    store: Store<ClientInfo>,
    con: Channel<ServerCtx>,
    secstore: Option<SecStore>,
    server_stop: oneshot::Receiver<()>,
    rx_stop: oneshot::Receiver<()>,
    uifo: Arc<UserInfo>,
    write_addr: SocketAddr,
) -> Result<(), Error> {
    let mut con = Some(con);
    let mut server_stop = server_stop.fuse();
    let mut rx_stop = rx_stop.fuse();
    let mut batch = Vec::new();
    let mut act = false;
    let mut timeout = time::interval_at(Instant::now() + WRITER_TTL, WRITER_TTL).fuse();
    async fn receive_batch(
        con: &mut Option<Channel<ServerCtx>>,
        batch: &mut Vec<ToWrite>,
    ) -> Result<(), Error> {
        match con {
            Some(ref mut con) => con.receive_batch(batch).await,
            None => future::pending().await,
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
                    let r = handle_batch_write(
                        &store,
                        c,
                        secstore.as_ref(),
                        &uifo,
                        write_addr,
                        batch.drain(..)
                    );
                    match r {
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
                    let mut store = store.write();
                    if let Some(ref mut cl) = store.clinfo_mut().remove(&write_addr) {
                        if let Some(stop) = mem::replace(cl, None) {
                            let _ = stop.send(());
                        }
                    }
                    store.unpublish_addr(write_addr);
                    store.gc();
                    bail!("client timed out");
                }
            },
        }
    }
}

async fn hello_client_write(
    store: Store<ClientInfo>,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    secstore: Option<SecStore>,
    id: ResolverId,
    hello: ClientHelloWrite,
) -> Result<(), Error> {
    async fn send(
        con: &mut Channel<ServerCtx>,
        hello: ServerHelloWrite,
    ) -> Result<(), Error> {
        Ok(time::timeout(HELLO_TIMEOUT, con.send_one(&hello)).await??)
    }
    fn salt() -> u64 {
        let mut rng = rand::thread_rng();
        rng.gen_range(0, u64::max_value() - 2)
    }
    let ttl_expired = !store.read().clinfo().contains_key(&hello.write_addr);
    let uifo = match hello.auth {
        ClientAuthWrite::Anonymous => {
            let h = ServerHelloWrite {
                ttl_expired,
                id,
                auth: ServerAuthWrite::Anonymous,
            };
            send(&mut con, h).await?;
            ANONYMOUS.clone()
        }
        ClientAuthWrite::Reuse => match secstore {
            None => bail!("authentication not supported"),
            Some(ref secstore) => match secstore.get_write(&hello.write_addr) {
                None => bail!("session not found"),
                Some(ctx) => {
                    let h = ServerHelloWrite {
                        ttl_expired,
                        id,
                        auth: ServerAuthWrite::Reused,
                    };
                    con.set_ctx(Some(ctx.clone()));
                    send(&mut con, h).await?;
                    secstore.ifo(Some(&ctx.client()?))?
                }
            },
        },
        ClientAuthWrite::Token(tok) => match secstore {
            None => bail!("authentication not supported"),
            Some(ref secstore) => {
                let (ctx, tok) = secstore.create(&tok)?;
                let h = ServerHelloWrite {
                    ttl_expired,
                    id,
                    auth: ServerAuthWrite::Accepted(tok),
                };
                con.set_ctx(Some(ctx.clone()));
                send(&mut con, h).await?;
                let mut con: Channel<ServerCtx> = Channel::new(
                    time::timeout(HELLO_TIMEOUT, TcpStream::connect(hello.write_addr))
                        .await??,
                );
                let salt = salt();
                let tok = Vec::from(&*ctx.wrap(true, &salt.to_be_bytes())?);
                let m = publisher::Hello::ResolverAuthenticate(id, tok);
                time::timeout(HELLO_TIMEOUT, con.send_one(&m)).await??;
                match time::timeout(HELLO_TIMEOUT, con.receive()).await?? {
                    publisher::Hello::Anonymous | publisher::Hello::Token(_) => {
                        bail!("denied")
                    }
                    publisher::Hello::ResolverAuthenticate(_, tok) => {
                        let d = Vec::from(&*ctx.unwrap(&tok)?);
                        let dsalt = u64::from_be_bytes(TryFrom::try_from(&*d)?);
                        if dsalt != salt + 2 {
                            bail!("denied")
                        }
                        secstore.store_write(hello.write_addr, ctx.clone());
                        secstore.ifo(Some(&ctx.client()?))?
                    }
                }
            }
        },
    };
    let (tx_stop, rx_stop) = oneshot::channel();
    {
        let mut store = store.write();
        let clinfos = store.clinfo_mut();
        match clinfos.get_mut(&hello.write_addr) {
            None => {
                clinfos.insert(hello.write_addr, Some(tx_stop));
            }
            Some(cl) => {
                if let Some(old_stop) = mem::replace(cl, Some(tx_stop)) {
                    let _ = old_stop.send(());
                }
            }
        }
    }
    Ok(client_loop_write(
        store,
        con,
        secstore,
        server_stop,
        rx_stop,
        uifo,
        hello.write_addr,
    )
    .await?)
}

fn handle_batch_read(
    store: &Store<ClientInfo>,
    con: &mut Channel<ServerCtx>,
    secstore: Option<&SecStore>,
    uifo: &Arc<UserInfo>,
    id: ResolverId,
    msgs: impl Iterator<Item = ToRead>,
) -> Result<(), Error> {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs();
    let s = store.read();
    let sec = secstore.map(|s| s.store.read());
    for m in msgs {
        match m {
            ToRead::Resolve(paths) => {
                if allowed_for(secstore, uifo, &paths, Permissions::SUBSCRIBE) {
                    con.queue_send(&match sec {
                        None => FromRead::Resolved(Resolved {
                            krb5_principals: HashMap::with_hasher(
                                FxBuildHasher::default(),
                            ),
                            resolver: id,
                            addrs: paths.iter().map(|p| s.resolve(p)).collect::<Vec<_>>(),
                        }),
                        Some(ref sec) => {
                            let mut krb5_principals =
                                HashMap::with_hasher(FxBuildHasher::default());
                            let addrs = paths
                                .iter()
                                .map(|p| {
                                    Ok(s.resolve_and_sign(
                                        &**sec,
                                        &mut krb5_principals,
                                        now,
                                        p,
                                    )?)
                                })
                                .collect::<Result<Vec<_>, Error>>()?;
                            FromRead::Resolved(Resolved {
                                krb5_principals,
                                resolver: id,
                                addrs,
                            })
                        }
                    })?
                } else {
                    con.queue_send(&FromRead::Error("denied".into()))?
                }
            }
            ToRead::List(path) => {
                let allowed = secstore
                    .map(|s| s.pmap().allowed(&*path, Permissions::LIST, uifo))
                    .unwrap_or(true);
                if allowed {
                    con.queue_send(&FromRead::List(s.list(&path)))?
                } else {
                    con.queue_send(&FromRead::Error("denied".into()))?
                }
            }
        }
    }
    Ok(())
}

async fn client_loop_read(
    store: Store<ClientInfo>,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    secstore: Option<SecStore>,
    id: ResolverId,
    uifo: Arc<UserInfo>,
) -> Result<(), Error> {
    let mut batch: Vec<ToRead> = Vec::new();
    let mut server_stop = server_stop.fuse();
    let mut act = false;
    let mut timeout = time::interval_at(Instant::now() + READER_TTL, READER_TTL).fuse();
    loop {
        select! {
            _ = server_stop => break Ok(()),
            m = con.receive_batch(&mut batch).fuse() => {
                m?;
                act = true;
                handle_batch_read(
                    &store,
                    &mut con,
                    secstore.as_ref(),
                    &uifo,
                    id,
                    batch.drain(0..)
                )?;
                con.flush().await?;
            },
            _ = timeout.next() => {
                if act {
                    act = false;
                } else {
                    bail!("client timed out");
                }
            }
        }
    }
}

async fn hello_client_read(
    store: Store<ClientInfo>,
    mut con: Channel<ServerCtx>,
    server_stop: oneshot::Receiver<()>,
    secstore: Option<SecStore>,
    id: ResolverId,
    hello: ClientAuthRead,
) -> Result<(), Error> {
    async fn send(
        con: &mut Channel<ServerCtx>,
        hello: ServerHelloRead,
    ) -> Result<(), Error> {
        Ok(time::timeout(HELLO_TIMEOUT, con.send_one(&hello)).await??)
    }
    let uifo = match hello {
        ClientAuthRead::Anonymous => {
            send(&mut con, ServerHelloRead::Anonymous).await?;
            ANONYMOUS.clone()
        }
        ClientAuthRead::Reuse(id) => match secstore {
            None => bail!("authentication requested but not supported"),
            Some(ref secstore) => match secstore.get_read(&id) {
                None => bail!("ctx id not found"),
                Some(ctx) => {
                    con.set_ctx(Some(ctx.clone()));
                    send(&mut con, ServerHelloRead::Reused).await?;
                    secstore.ifo(Some(&ctx.client()?))?
                }
            },
        },
        ClientAuthRead::Token(tok) => match secstore {
            None => bail!("authentication requested but not supported"),
            Some(ref secstore) => {
                let (ctx, tok) = secstore.create(&tok)?;
                let id = secstore.store_read(ctx.clone());
                con.set_ctx(Some(ctx.clone()));
                send(&mut con, ServerHelloRead::Accepted(tok, id)).await?;
                secstore.ifo(Some(&ctx.client()?))?
            }
        },
    };
    Ok(client_loop_read(store, con, server_stop, secstore, id, uifo).await?)
}

async fn hello_client(
    store: Store<ClientInfo>,
    s: TcpStream,
    server_stop: oneshot::Receiver<()>,
    secstore: Option<SecStore>,
    id: ResolverId,
) -> Result<(), Error> {
    s.set_nodelay(true)?;
    let mut con = Channel::new(s);
    let hello: ClientHello = time::timeout(HELLO_TIMEOUT, con.receive()).await??;
    match hello {
        ClientHello::ReadOnly(hello) => {
            Ok(hello_client_read(store, con, server_stop, secstore, id, hello).await?)
        }
        ClientHello::WriteOnly(hello) => {
            Ok(hello_client_write(store, con, server_stop, secstore, id, hello).await?)
        }
    }
}

async fn server_loop(
    cfg: config::resolver_server::Config,
    stop: oneshot::Receiver<()>,
    ready: oneshot::Sender<SocketAddr>,
) -> Result<SocketAddr, Error> {
    let connections = Arc::new(AtomicUsize::new(0));
    let published: Store<ClientInfo> = Store::new();
    let secstore = match &cfg.auth {
        config::resolver_server::Auth::Anonymous => None,
        config::resolver_server::Auth::Krb5 {
            principal,
            permissions,
        } => Some(SecStore::new(principal.clone(), permissions.clone())?),
    };
    let mut listener = TcpListener::bind(cfg.addr).await?;
    let local_addr = listener.local_addr()?;
    let mut stop = stop.fuse();
    let mut client_stops = Vec::new();
    let max_connections = cfg.max_connections;
    let id = cfg.id;
    let _ = ready.send(local_addr);
    loop {
        select! {
            cl = listener.accept().fuse() => match cl {
                Err(_) => (),
                Ok((client, _)) => {
                    if connections.fetch_add(1, Ordering::Relaxed) < max_connections {
                        let connections = connections.clone();
                        let published = published.clone();
                        let secstore = secstore.clone();
                        let (tx, rx) = oneshot::channel();
                        client_stops.push(tx);
                        task::spawn(async move {
                            let _ = hello_client(
                                published, client, rx, secstore, id
                            ).await;
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
    pub async fn new(cfg: config::resolver_server::Config) -> Result<Server, Error> {
        let (send_stop, recv_stop) = oneshot::channel();
        let (send_ready, recv_ready) = oneshot::channel();
        let tsk = server_loop(cfg, recv_stop, send_ready);
        let local_addr = select! {
            a = task::spawn(tsk).fuse() => a??,
            a = recv_ready.fuse() => a?,
        };
        Ok(Server {
            stop: Some(send_stop),
            local_addr,
        })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }
}

#[cfg(test)]
mod test {
    use crate::{
        config,
        path::Path,
        resolver::{ReadOnly, Resolver, WriteOnly},
        resolver_server::Server,
    };
    use std::net::SocketAddr;

    async fn init_server() -> Server {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        Server::new(addr, 100, "".into())
            .await
            .expect("start server")
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
            let cfg = config::Resolver {
                addr: *server.local_addr(),
            };
            let mut w = Resolver::<WriteOnly>::new_w(cfg, paddr).unwrap();
            let mut r = Resolver::<ReadOnly>::new_r(cfg).unwrap();
            let paths = vec![p("/foo/bar"), p("/foo/baz"), p("/app/v0"), p("/app/v1")];
            w.publish(paths.clone()).await.unwrap();
            for addrs in r.resolve(paths.clone()).await.unwrap() {
                assert_eq!(addrs.len(), 1);
                assert_eq!(addrs[0], paddr);
            }
            assert_eq!(r.list(p("/")).await.unwrap(), vec![p("/app"), p("/foo")]);
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
