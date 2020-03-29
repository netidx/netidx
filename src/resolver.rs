use crate::{
    auth::{
        syskrb5::{sys_krb5, ClientCtx},
        Krb5, Krb5Ctx,
    },
    channel::Channel,
    config,
    path::Path,
    protocol::{Id, resolver},
};
use failure::Error;
use futures::{prelude::*, select};
use std::{
    collections::HashSet, marker::PhantomData, net::SocketAddr, result, time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    task,
    time::{self, Instant},
};

static TTL: u64 = 600;
static LINGER: u64 = 10;

type FromCon = oneshot::Sender<resolver::From>;
type ToCon = (resolver::To, FromCon);

pub trait Readable {}
pub trait Writeable {}
pub trait ReadableOrWritable {}

#[derive(Debug, Clone)]
pub struct ReadOnly {}

impl Readable for ReadOnly {}
impl ReadableOrWritable for ReadOnly {}

#[derive(Debug, Clone)]
pub struct WriteOnly {}

impl Writeable for WriteOnly {}
impl ReadableOrWritable for WriteOnly {}

type Result<T> = result::Result<T, Error>;

#[derive(Debug, Clone)]
pub enum Auth {
    Anonymous,
    Krb5 {principal: Option<String>},
}

#[derive(Debug, Clone)]
pub struct Resolver<R> {
    sender: mpsc::UnboundedSender<ToCon>,
    kind: PhantomData<R>,
}

impl<R: ReadableOrWritable> Resolver<R> {
    async fn send(&mut self, m: resolver::To) -> Result<resolver::From> {
        let (tx, rx) = oneshot::channel();
        self.sender.send((m, tx))?;
        match rx.await? {
            resolver::From::Error(s) => bail!(s),
            m => Ok(m),
        }
    }

    pub fn new_w(
        resolver: config::Resolver,
        publisher: SocketAddr,
        desired_auth: Auth,
    ) -> Result<Resolver<WriteOnly>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        task::spawn(connection(receiver, resolver, Some(publisher), desired_auth));
        Ok(Resolver {
            sender,
            kind: PhantomData,
        })
    }

    // CR estokes: when given more than one socket address the
    // resolver should make use of all of them.
    pub fn new_r(
        resolver: config::Resolver,
        desired_auth: Auth
    ) -> Result<Resolver<ReadOnly>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        task::spawn(connection(receiver, resolver, None, desired_auth));
        Ok(Resolver {
            sender,
            kind: PhantomData,
        })
    }
}

impl<R: Readable + ReadableOrWritable> Resolver<R> {
    pub async fn resolve(&mut self, paths: Vec<Path>) -> Result<Vec<Vec<SocketAddr>>> {
        match self.send(resolver::To::Resolve(paths)).await? {
            resolver::From::Resolved(ports) => Ok(ports),
            _ => bail!("unexpected response"),
        }
    }

    pub async fn list(&mut self, p: Path) -> Result<Vec<Path>> {
        match self.send(resolver::To::List(p)).await? {
            resolver::From::List(v) => Ok(v),
            _ => bail!("unexpected response"),
        }
    }
}

impl<R: Writeable + ReadableOrWritable> Resolver<R> {
    pub async fn publish(&mut self, paths: Vec<Path>) -> Result<()> {
        match self.send(resolver::To::Publish(paths)).await? {
            resolver::From::Published => Ok(()),
            _ => bail!("unexpected response"),
        }
    }

    pub async fn unpublish(&mut self, paths: Vec<Path>) -> Result<()> {
        match self.send(resolver::To::Unpublish(paths)).await? {
            resolver::From::Unpublished => Ok(()),
            _ => bail!("unexpected response"),
        }
    }

    pub async fn clear(&mut self) -> Result<()> {
        match self.send(resolver::To::Clear).await? {
            resolver::From::Unpublished => Ok(()),
            _ => bail!("unexpected response"),
        }
    }
}

fn create_ctx(
    principal: Option<&[u8]>,
    target_principal: &[u8]
) -> Result<(ClientCtx, Vec<u8>), Error> {
    let ctx = sys_krb5.create_client_ctx(principal, target_principal)?;
    match ctx.step(None)? {
        None => bail!("client ctx first step produced no token"),
        Some(tok) => Ok((ctx, Vec::from(&*tok)))
    }
}

async fn connect(
    resolver: config::Resolver,
    publisher: Option<SocketAddr>,
    published: &HashSet<Path>,
    ctx: &mut Option<(Id, ClientCtx)>,
    desired_auth: &Auth,
) -> Channel {
    let mut backoff = 0;
    loop {
        if backoff > 0 {
            time::delay_for(Duration::from_secs(backoff)).await;
        }
        backoff += 1;
        let con = try_cont!("connect", TcpStream::connect(&resolver.addr).await);
        let mut con = Channel::new(con);
        let (auth, new_ctx) = match desired_auth {
            Auth::Anonymous => (resolver::ClientAuth::Anonymous, None),
            Auth::Krb5 {ref principal} => match ctx {
                Some((id, ctx)) => (resolver::ClientAuth::Reuse(id), None),
                None => {
                    let (ctx, tok) = try_cont!("create ctx", task::block_in_place(|| {
                        create_ctx(
                            principal.map(|s| s.as_bytes()),
                            resolver.principal.as_bytes()
                        )
                    }));
                    (resolver::ClientAuth::Token(tok), Some(ctx))
                }
            }
        };
        // the unwrap can't fail, we can always encode the message,
        // and it's never bigger then 4 GB.
        try_cont!(
            "hello",
            con.send_one(&match publisher {
                None => resolver::ClientHello::ReadOnly(auth),
                Some(write_addr) => resolver::ClientHello::WriteOnly {
                    ttl: TTL,
                    write_addr,
                    auth,
                },
            })
            .await
        );
        let r: resolver::ServerHello = try_cont!("hello reply", con.receive().await);
        // CR estokes: replace this with proper logging
        match (desired_auth, r.auth) {
            (Auth::Anonymous, resolver::ServerAuth::Anonymous) => (),
            (Auth::Anonymous, _) => {
                println!("server requires authentication");
                continue;
            }
            (Auth::Krb5 {..}, resolver::ServerAuth::Anonymous) => {
                println!("could not authenticate resolver server");
                continue;
            }
            (Auth::Krb5 {..}, resolver::ServerAuth::Reused) => match new_ctx {
                None => (), // context reused
                Some(_) => {
                    println!("server wants to reuse a ctx, but we created a new one");
                    continue;
                }
            }
            (Auth::Krb5 {..}, resolver::ServerAuth::Accepted(tok, id)) => match new_ctx {
                None => {
                    println!("we asked to reuse a ctx but the server created a new one");
                    continue;
                }
                Some(new_ctx) => {
                    try_cont!(
                        "authenticate resolver",
                        task::block_in_place(|| new_ctx.step(&tok))
                    );
                    ctx = Some((id, new_ctx));
                }
            }
        }
        if !r.ttl_expired {
            break con;
        } else {
            let m = resolver::To::Publish(published.iter().cloned().collect());
            try_cont!("republish", con.send_one(&m).await);
            match try_cont!("replublish reply", con.receive().await) {
                resolver::From::Published => break con,
                _ => (),
            }
        }
    }
}

async fn connection(
    receiver: mpsc::UnboundedReceiver<ToCon>,
    resolver: config::Resolver,
    publisher: Option<SocketAddr>,
    desired_auth: Auth,
) {
    let mut published = HashSet::new();
    let mut con: Option<Channel> = None;
    let mut ctx: Option<(Id, ClientCtx)> = None;
    let ttl = Duration::from_secs(TTL / 2);
    let linger = Duration::from_secs(LINGER);
    let now = Instant::now();
    let mut act = false;
    let mut receiver = receiver.fuse();
    let mut hb = time::interval_at(now + ttl, ttl).fuse();
    let mut dc = time::interval_at(now + linger, linger).fuse();
    loop {
        select! {
            _ = dc.next() => {
                if act {
                   act = false;
                } else {
                    con = None;
                }
            },
            _ = hb.next() => loop {
                if act {
                    break;
                } else {
                    match con {
                        None => {
                            con = Some(connect(
                                resolver, publisher, &published, &mut ctx, &auth
                            ).await);
                            break;
                        },
                        Some(ref mut c) =>
                            match c.send_one(&resolver::To::Heartbeat).await {
                                Ok(()) => break,
                                Err(_) => { con = None; }
                            }
                    }
                }
            },
            m = receiver.next() => match m {
                None => break,
                Some((m, reply)) => {
                    act = true;
                    let m_r = &m;
                    let r = loop {
                        let c = match con {
                            Some(ref mut c) => c,
                            None => {
                                con = Some(connect(
                                    resolver, publisher, &published, &mut ctx, &auth
                                ).await);
                                con.as_mut().unwrap()
                            }
                        };
                        match c.send_one(m_r).await {
                            Err(_) => { con = None; }
                            Ok(()) => match c.receive().await {
                                Err(_) => { con = None; }
                                Ok(r) => break r,
                                Ok(resolver::From::Published) => {
                                    if let resolver::To::Publish(p) = m_r {
                                        published.extend(p.iter().cloned());
                                    }
                                    break resolver::From::Published
                                }
                            }
                        }
                    };
                    let _ = reply.send(r);
                }
            }
        }
    }
}
