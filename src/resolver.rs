use crate::{
    auth::{
        syskrb5::{ClientCtx, SYS_KRB5},
        Krb5, Krb5Ctx,
    },
    channel::Channel,
    config::{self, resolver::Auth as CAuth},
    path::Path,
    protocol::resolver::{
        self, ClientAuthRead, ClientHello, CtxId, FromRead, FromWrite, Resolved,
        ServerAuthWrite, ServerHelloRead, ServerHelloWrite, ToRead, ToWrite,
    },
};
use failure::Error;
use futures::{future::select_ok, prelude::*, select};
use fxhash::FxBuildHasher;
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    result,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    task,
    time::{self, Instant},
};

static TTL: u64 = 120;
static LINGER: u64 = 10;

type Result<T> = result::Result<T, Error>;

#[derive(Debug, Clone)]
pub enum Auth {
    Anonymous,
    Krb5 { principal: Option<String> },
}

type FromCon<F> = oneshot::Sender<F>;
type ToCon<T, F> = (T, FromCon<F>);

async fn send<T: Send + Sync + 'static, F: Send + Sync + 'static>(
    sender: &mpsc::UnboundedSender<ToCon<T, F>>,
    m: T,
) -> Result<F> {
    let (tx, rx) = oneshot::channel();
    sender.send((m, tx))?;
    Ok(rx.await?)
}

fn create_ctx(
    principal: Option<&[u8]>,
    target_principal: &[u8],
) -> Result<(ClientCtx, Vec<u8>)> {
    let ctx = SYS_KRB5.create_client_ctx(principal, target_principal)?;
    match ctx.step(None)? {
        None => bail!("client ctx first step produced no token"),
        Some(tok) => Ok((ctx, Vec::from(&*tok))),
    }
}

fn choose_read_addr(resolver: &config::resolver::Config) -> SocketAddr {
    use rand::{thread_rng, Rng};
    resolver.servers[thread_rng().gen_range(0, resolver.servers.len())]
}

async fn connect_read(
    resolver: &config::resolver::Config,
    sc: &mut Option<(CtxId, ClientCtx)>,
    desired_auth: &Auth,
) -> Result<Channel<ClientCtx>> {
    let mut backoff = 0;
    loop {
        if backoff > 0 {
            time::delay_for(Duration::from_secs(backoff)).await;
        }
        backoff += 1;
        let addr = choose_read_addr(resolver);
        let con = try_cont!("connect", TcpStream::connect(&addr).await);
        let mut con = Channel::new(con);
        let (auth, ctx) = match (desired_auth, &resolver.auth) {
            (Auth::Anonymous, _) => (ClientAuthRead::Anonymous, None),
            (Auth::Krb5 { .. }, CAuth::Anonymous) => bail!("authentication unavailable"),
            (Auth::Krb5 { principal }, CAuth::Krb5 { principal: t }) => match sc {
                Some((id, ctx)) => (ClientAuthRead::Reuse(*id), Some(ctx.clone())),
                None => {
                    let p = principal.as_ref().map(|s| s.as_bytes());
                    let (ctx, tok) = try_cont!("create ctx", create_ctx(p, t.as_bytes()));
                    (ClientAuthRead::Token(tok), Some(ctx))
                }
            },
        };
        try_cont!("hello", con.send_one(&ClientHello::ReadOnly(auth)).await);
        con.set_ctx(ctx.clone());
        let r: ServerHelloRead = try_cont!("hello reply", con.receive().await);
        // CR estokes: replace this with proper logging
        match (desired_auth, r) {
            (Auth::Anonymous, ServerHelloRead::Anonymous) => (),
            (Auth::Anonymous, _) => {
                println!("server requires authentication");
                continue;
            }
            (Auth::Krb5 { .. }, ServerHelloRead::Anonymous) => {
                println!("could not authenticate resolver server");
                continue;
            }
            (Auth::Krb5 { .. }, ServerHelloRead::Reused) => (),
            (Auth::Krb5 { .. }, ServerHelloRead::Accepted(tok, id)) => {
                let ctx = ctx.unwrap();
                try_cont!("resolver tok", ctx.step(Some(&tok)));
                *sc = Some((id, ctx));
            }
        }
        break Ok(con);
    }
}

async fn connection_read(
    mut receiver: mpsc::UnboundedReceiver<ToCon<ToRead, FromRead>>,
    resolver: config::resolver::Config,
    publisher: Option<SocketAddr>,
    desired_auth: Auth,
) -> Result<()> {
    let mut ctx: Option<(CtxId, ClientCtx)> = None;
    let mut con: Option<Channel<ClientCtx>> = None;
    while let Some((m, reply)) = receiver.next()? {
        let m_r = &m;
        let r = loop {
            let c = match con {
                Some(ref mut c) => c,
                None => {
                    con = Some(connect_read(&resolver, &mut ctx, &desired_auth).await?);
                    con.as_mut().unwrap()
                }
            };
            match c.send_one(m_r).await {
                Err(_) => {
                    con = None;
                }
                Ok(()) => match c.receive().await {
                    Err(_) => {
                        con = None;
                    }
                    Ok(r) => break r,
                },
            }
        };
        let _ = reply.send(r);
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ResolverRead(mpsc::UnboundedSender<ToCon<ToRead, FromRead>>);

impl ResolverRead {
    pub fn new(
        resolver: config::resolver::Config,
        desired_auth: Auth,
    ) -> Result<ResolverRead> {
        let (sender, receiver) = mpsc::unbounded_channel();
        task::spawn(async move {
            let _ = connection_read(receiver, resolver, desired_auth);
        });
        Ok(ResolverRead(sender))
    }

    pub async fn resolve(&mut self, paths: Vec<Path>) -> Result<Resolved> {
        match send(&self.0, resolver::ToRead::Resolve(paths)).await? {
            resolver::FromRead::Resolved(r) => Ok(r),
            _ => bail!("unexpected response"),
        }
    }

    pub async fn list(&mut self, p: Path) -> Result<Vec<Path>> {
        match send(&self.0, resolver::ToRead::List(p)).await? {
            resolver::FromRead::List(v) => Ok(v),
            _ => bail!("unexpected response"),
        }
    }
}

async fn connect_write(
    resolver: &config::resolver::Config,
    resolver_addr: SocketAddr,
    write_addr: SocketAddr,
    published: &Arc<RwLock<HashSet<Path>>>,
    ctxts: &Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
    desired_auth: &Auth,
) -> Result<Channel<ClientCtx>> {
    let mut backoff = 0;
    loop {
        if backoff > 0 {
            time::delay_for(Duration::from_secs(backoff)).await;
        }
        backoff += 1;
        let con = try_cont!("connect", TcpStream::connect(&resolver_addr).await);
        let mut con = Channel::new(con);
        let (auth, ctx) = match (desired_auth, &resolver.auth) {
            (Auth::Anonymous, _) => (ClientAuthRead::Anonymous, None),
            (Auth::Krb5 { .. }, CAuth::Anonymous) => bail!("authentication unavailable"),
            (Auth::Krb5 { principal }, CAuth::Krb5 { principal: t }) => {
                match ctxts.read().get(&resolver_addr) {
                    Some(ctx) => (ClientAuthRead::Reuse, Some(ctx.clone())),
                    None => {
                        let p = principal.as_ref().map(|s| s.as_bytes());
                        let (ctx, tok) =
                            try_cont!("create ctx", create_ctx(p, t.as_bytes()));
                        (ClientAuthRead::Token(tok), Some(ctx))
                    }
                }
            }
        };
        let h = ClientHello::WriteOnly {
            write_addr,
            resolver_addr,
            auth,
        };
        try_cont!("hello", con.send_one(&h).await);
        con.set_ctx(ctx.clone());
        let r: ServerHelloWrite = try_cont!("hello reply", con.receive().await);
        // CR estokes: replace this with proper logging
        match (desired_auth, r.auth) {
            (Auth::Anonymous, ServerAuthWrite::Anonymous) => (),
            (Auth::Anonymous, _) => {
                println!("server requires authentication");
                continue;
            }
            (Auth::Krb5 { .. }, ServerAuthWrite::Anonymous) => {
                println!("could not authenticate resolver server");
                continue;
            }
            (Auth::Krb5 { .. }, ServerAuthWrite::Reused) => (),
            (Auth::Krb5 { .. }, ServerAuthWrite::Accepted(tok)) => {
                let ctx = ctx.unwrap();
                try_cont!("resolver tok", ctx.step(Some(&tok)));
                ctxts.write().insert(resolver_addr, ctx.clone());
            }
        }
        if !r.ttl_expired {
            break Ok(con);
        } else {
            let m = ToWrite::Publish(published.read().iter().cloned().collect());
            try_cont!("republish", con.send_one(&m).await);
            match try_cont!("replublish reply", con.receive().await) {
                FromWrite::Published => break Ok(con),
                _ => (),
            }
        }
    }
}

async fn connection_write(
    receiver: mpsc::UnboundedReceiver<ToCon<Arc<ToWrite>, FromWrite>>,
    resolver: config::resolver::Config,
    resolver_addr: SocketAddr,
    write_addr: SocketAddr,
    published: Arc<RwLock<HashSet<Path>>>,
    desired_auth: Auth,
    ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
) -> Result<()> {
    let mut con: Option<Channel<ClientCtx>> = None;
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
                            con = Some(connect_write(
                                &resolver, resolver_addr, write_addr, &published,
                                &ctxts, &desired_auth
                            ).await?);
                            break;
                        },
                        Some(ref mut c) => match c.send_one(&ToWrite::Heartbeat).await {
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
                                con = Some(connect_write(
                                    &resolver, resolver_addr, write_addr, &published,
                                    &ctxts, &desired_auth
                                ).await?);
                                con.as_mut().unwrap()
                            }
                        };
                        match c.send_one(m_r).await {
                            Err(_) => { con = None; }
                            Ok(()) => match c.receive().await {
                                Err(_) => { con = None; }
                                Ok(r) => break r,
                            }
                        }
                    };
                    let _ = reply.send(r);
                }
            }
        }
    }
    Ok(())
}

async fn write_mgr(
    receiver: mpsc::UnboundedReceiver<ToCon>,
    resolver: config::resolver::Config,
    desired_auth: Auth,
    ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
    write_addr: SocketAddr,
) -> Result<()> {
    let published: Arc<RwLock<HashSet<Path>>> = Arc::new(RwLock::new(HashSet::new()));
    let senders = {
        let mut senders = Vec::new();
        for addr in &resolver.servers {
            let (sender, receiver) = mpsc::unbounded_channel();
            let resolver = resolver.clone();
            let published = published.clone();
            let desired_auth = desired_auth.clone();
            let ctxts = ctxts.clone();
            senders.push(sender);
            task::spawn(async move {
                let _ = connection_write(
                    receiver,
                    resolver,
                    *addr,
                    write_addr,
                    published,
                    desired_auth.clone(),
                    ctxts.clone(),
                )
                .await;
                // CR estokes: handle failure
            });
        }
        senders
    };
    while let Some((m, reply)) = receiver.next() {
        let m = Arc::new(m);
        let r = select_ok(senders.iter().map(|s| {
            let (tx, rx) = oneshot::channel();
            let _ = s.send((m.clone(), tx));
            rx
        }))
        .await?;
        if let ToWrite::Publish(paths) = &*m {
            if let FromWrite::Published = &r {
                for p in paths {
                    published.write().insert(p.clone());
                }
            }
        }
        let _ = reply.send(r);
    }
}

#[derive(Debug, Clone)]
pub struct ResolverWrite {
    sender: mpsc::UnboundedSender<ToCon<ToWrite, FromWrite>>,
    ctxts: Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>>,
}

impl ResolverWrite {
    pub fn new(
        resolver: config::resolver::Config,
        desired_auth: Auth,
        write_addr: SocketAddr,
    ) -> Result<ResolverWrite> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let ctxts = Arc::new(RwLock::new(HashMap::with_hasher(FxBuildHasher::default())));
        let ctxts_c = ctxts.clone();
        task::spawn(async move {
            let _ =
                write_mgr(receiver, resolver, desired_auth, ctxts_c, write_addr).await;
        });
        Ok(ResolverWrite { sender, ctxts })
    }

    pub async fn publish(&self, paths: Vec<Path>) -> Result<()> {
        match send(&self.sender, ToWrite::Publish(paths)).await? {
            FromWrite::Published => Ok(()),
            _ => bail!("unexpected response"),
        }
    }

    pub async fn unpublish(&self, paths: Vec<Path>) -> Result<()> {
        match send(&self.sender, ToWrite::Unpublish(paths)).await? {
            FromWrite::Unpublished => Ok(()),
            _ => bail!("unexpected response"),
        }
    }

    pub async fn clear(&self) -> Result<()> {
        match send(&self.sender, ToWrite::Clear).await? {
            FromWrite::Unpublished => Ok(()),
            _ => bail!("unexpected response"),
        }
    }

    pub(crate) fn ctxts(
        &self,
    ) -> Arc<RwLock<HashMap<SocketAddr, ClientCtx, FxBuildHasher>>> {
        self.ctxts.clone()
    }
}
