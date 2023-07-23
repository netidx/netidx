use super::{
    ClId, Client, Event, PublisherInner, PublisherWeak, SendResult, Update, WriteRequest,
    BATCHES,
};
use crate::{
    channel::{self, Channel, K5CtxWrap, ReadChannel, WriteChannel},
    chars::Chars,
    pack::BoundedBytes,
    path::Path,
    pool::Pooled,
    protocol::{
        self,
        publisher::{self, Id},
        value::Value,
    },
    resolver_client::DesiredAuth,
    resolver_server::{auth::Permissions, krb5_authentication},
    tls,
    utils::{self, BatchItem, Batched, ChanId, ChanWrap},
};
use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use cross_krb5::ServerCtx;
use futures::{
    channel::{
        mpsc::{channel, Receiver, Sender},
        oneshot,
    },
    prelude::*,
    select_biased,
    stream::{FuturesUnordered, SelectAll},
};
use fxhash::FxHashMap;
use log::{debug, info};
use parking_lot::RwLock;
use protocol::resolver::{AuthChallenge, HashMethod, UserInfo};
use std::{
    boxed::Box,
    collections::{hash_map::Entry, BTreeSet, Bound, HashMap, HashSet},
    convert::From,
    default::Default,
    iter::{self, FromIterator},
    mem,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    net::{TcpListener, TcpStream},
    task, time,
};

const MAX_DEFERRED: usize = 1000000;
type DeferredSubs =
    Batched<SelectAll<Box<dyn Stream<Item = (Path, Permissions)> + Send + Sync + Unpin>>>;

fn subscribe(
    t: &mut PublisherInner,
    con: &mut WriteChannel,
    client: ClId,
    path: Path,
    permissions: Permissions,
    deferred_subs: &mut DeferredSubs,
) -> Result<()> {
    match t.by_path.get(&path) {
        None => {
            let mut r = t.default.range_mut::<str, (Bound<&str>, Bound<&str>)>((
                Bound::Unbounded,
                Bound::Included(path.as_ref()),
            ));
            loop {
                match r.next_back() {
                    Some((base, chan))
                        if path.starts_with(base.as_ref())
                            && deferred_subs.inner().len() < MAX_DEFERRED =>
                    {
                        let (tx, rx) = oneshot::channel();
                        if let Ok(()) = chan.unbounded_send((path.clone(), tx)) {
                            let path = path.clone();
                            let s = rx.map(move |_| (path, permissions));
                            deferred_subs.inner_mut().push(Box::new(s.into_stream()));
                            break;
                        }
                    }
                    _ => {
                        con.queue_send(&publisher::From::NoSuchValue(path.clone()))?;
                        break;
                    }
                }
            }
        }
        Some(id) => {
            let id = *id;
            if let Some(ut) = t.by_id.get_mut(&id) {
                if let Some(eauth) = &t.extended_auth {
                    let mut res = false;
                    if let Some(cl) = t.clients.get(&client) {
                        res = eauth.0(client, id, cl.user.as_ref());
                    }
                    if !res {
                        con.queue_send(&publisher::From::Denied(path))?;
                        return Ok(());
                    }
                }
                if let Some(cl) = t.clients.get_mut(&client) {
                    cl.subscribed.insert(id, permissions);
                }
                let subs = BTreeSet::from_iter(
                    iter::once(client).chain(ut.subscribed.iter().copied()),
                );
                match t.hc_subscribed.entry(subs) {
                    Entry::Occupied(e) => {
                        ut.subscribed = Arc::clone(e.get());
                    }
                    Entry::Vacant(e) => {
                        let mut s = HashSet::clone(&ut.subscribed);
                        s.insert(client);
                        ut.subscribed = Arc::new(s);
                        e.insert(Arc::clone(&ut.subscribed));
                    }
                }
                let m = publisher::From::Subscribed(path, id, ut.current.clone());
                con.queue_send(&m)?;
                if let Some(waiters) = t.wait_clients.remove(&id) {
                    for tx in waiters {
                        let _ = tx.send(());
                    }
                }
                t.send_event(Event::Subscribe(id, client));
            }
        }
    }
    Ok(())
}

fn unsubscribe(t: &mut PublisherInner, client: ClId, id: Id) {
    if let Some(ut) = t.by_id.get_mut(&id) {
	let current_subs = BTreeSet::from_iter(ut.subscribed.iter().copied());
        let new_subs =
            BTreeSet::from_iter(ut.subscribed.iter().filter(|a| *a != &client).copied());
        match t.hc_subscribed.entry(new_subs) {
            Entry::Occupied(e) => {
                ut.subscribed = e.get().clone();
            }
            Entry::Vacant(e) => {
                let mut h = HashSet::clone(&ut.subscribed);
                h.remove(&client);
                ut.subscribed = Arc::new(h);
                e.insert(Arc::clone(&ut.subscribed));
            }
        }
	if let Entry::Occupied(e) = t.hc_subscribed.entry(current_subs) {
	    if Arc::strong_count(e.get()) == 1 {
		e.remove();
	    }
	}
        let nsubs = ut.subscribed.len();
        if let Some(cl) = t.clients.get_mut(&client) {
            cl.subscribed.remove(&id);
        }
        t.send_event(Event::Unsubscribe(id, client));
        if nsubs == 0 && t.destroy_on_idle.remove(&id) {
            t.destroy_val(id)
        }
    }
}

fn write(
    t: &mut PublisherInner,
    con: &mut WriteChannel,
    client: ClId,
    gc_on_write: &mut Vec<ChanWrap<Pooled<Vec<WriteRequest>>>>,
    wait_write_res: &mut Vec<(Id, oneshot::Receiver<Value>)>,
    write_batches: &mut FxHashMap<
        ChanId,
        (Pooled<Vec<WriteRequest>>, Sender<Pooled<Vec<WriteRequest>>>),
    >,
    id: Id,
    v: Value,
    r: bool,
) -> Result<()> {
    macro_rules! or_qwe {
        ($v:expr, $m:expr) => {
            match $v {
                Some(v) => v,
                None => {
                    if r {
                        let m = Value::Error(Chars::from($m));
                        con.queue_send(&From::WriteResult(id, m))?
                    }
                    return Ok(());
                }
            }
        };
    }
    use protocol::publisher::From;
    let cl = or_qwe!(t.clients.get(&client), "cannot write to unsubscribed value");
    let perms = or_qwe!(cl.subscribed.get(&id), "cannot write to unsubscribed value");
    if !perms.contains(Permissions::WRITE) {
        or_qwe!(None, "write permission denied")
    }
    let ow = or_qwe!(t.on_write.get_mut(&id), "writes not accepted");
    ow.retain(|(_, c)| {
        if c.is_closed() {
            gc_on_write.push(ChanWrap(c.clone()));
            false
        } else {
            true
        }
    });
    if ow.len() == 0 {
        or_qwe!(None, "writes not accepted");
    }
    let send_result = if !r {
        None
    } else {
        let (send_result, wait) = SendResult::new();
        wait_write_res.push((id, wait));
        Some(send_result)
    };
    for (cid, ch) in ow.iter() {
        if let Some(pbv) = t.by_id.get(&id) {
            let req = WriteRequest {
                id,
                path: pbv.path.clone(),
                client,
                value: v.clone(),
                send_result: send_result.clone(),
            };
            write_batches
                .entry(*cid)
                .or_insert_with(|| (BATCHES.take(), ch.clone()))
                .0
                .push(req)
        }
    }
    Ok(())
}

fn check_token(
    token: Bytes,
    now: u64,
    secret: u128,
    timestamp: u64,
    permissions: u32,
    path: &Path,
) -> Result<(bool, Permissions)> {
    if token.len() < mem::size_of::<u64>() {
        bail!("error, token too short");
    }
    let expected = utils::make_sha3_token([
        &secret.to_be_bytes(),
        &timestamp.to_be_bytes()[..],
        &permissions.to_be_bytes(),
        path.as_bytes(),
    ]);
    let permissions = Permissions::from_bits(permissions)
        .ok_or_else(|| anyhow!("invalid permission bits"))?;
    let age = std::cmp::max(
        u64::saturating_sub(now, timestamp),
        u64::saturating_sub(timestamp, now),
    );
    let valid = age <= 300
        && permissions.contains(Permissions::SUBSCRIBE)
        && &*token == &expected;
    Ok((valid, permissions))
}

const HB: Duration = Duration::from_secs(5);

const HELLO_TIMEOUT: Duration = Duration::from_secs(10);

enum BlockedWrite {
    Wrote,
    Reply(publisher::From),
}

type BlockedWriteFut =
    Pin<Box<dyn Future<Output = BlockedWrite> + Send + Sync + 'static>>;

struct ClientCtx {
    desired_auth: DesiredAuth,
    client: ClId,
    publisher: PublisherWeak,
    secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
    batch: Vec<publisher::To>,
    write_batches:
        FxHashMap<ChanId, (Pooled<Vec<WriteRequest>>, Sender<Pooled<Vec<WriteRequest>>>)>,
    blocked_writes: FuturesUnordered<BlockedWriteFut>,
    flushing_updates: bool,
    flush_timeout: Option<Duration>,
    deferred_subs: DeferredSubs,
    deferred_subs_batch: Vec<(Path, Permissions)>,
    wait_write_res: Vec<(Id, oneshot::Receiver<Value>)>,
    gc_on_write: Vec<ChanWrap<Pooled<Vec<WriteRequest>>>>,
    msg_sent: bool,
    tls_ctx: Option<tls::CachedAcceptor>,
}

impl ClientCtx {
    fn new(
        client: ClId,
        secrets: Arc<RwLock<FxHashMap<SocketAddr, u128>>>,
        publisher: PublisherWeak,
        desired_auth: DesiredAuth,
        tls_ctx: Option<tls::CachedAcceptor>,
    ) -> ClientCtx {
        let mut deferred_subs: DeferredSubs =
            Batched::new(SelectAll::new(), MAX_DEFERRED);
        deferred_subs.inner_mut().push(Box::new(stream::pending()));
        ClientCtx {
            desired_auth,
            client,
            publisher,
            secrets,
            batch: Vec::new(),
            write_batches: HashMap::default(),
            blocked_writes: FuturesUnordered::new(),
            flushing_updates: false,
            flush_timeout: None,
            deferred_subs,
            deferred_subs_batch: Vec::new(),
            wait_write_res: Vec::new(),
            gc_on_write: Vec::new(),
            msg_sent: false,
            tls_ctx,
        }
    }

    fn client_arrived(&mut self) {
        if let Some(publisher) = self.publisher.upgrade() {
            let mut pb = publisher.0.lock();
            for tx in pb.wait_any_client.drain(..) {
                let _ = tx.send(());
            }
        }
    }

    fn set_user(&mut self, ifo: Option<UserInfo>) {
        if let Some(ifo) = ifo {
            if let Some(secret) = self.secrets.read().get(&ifo.resolver).copied() {
                let expected = utils::make_sha3_token(
                    iter::once(&secret.to_be_bytes()[..])
                        .chain(iter::once(ifo.name.as_bytes()))
                        .chain(iter::once(ifo.primary_group.as_bytes()))
                        .chain(ifo.groups.iter().map(|s| s.as_bytes())),
                );
                if ifo.token == expected {
                    if let Some(pb) = self.publisher.upgrade() {
                        let mut t = pb.0.lock();
                        if let Some(ci) = t.clients.get_mut(&self.client) {
                            ci.user = Some(ifo);
                        }
                    }
                }
            }
        }
    }

    // CR estokes: Implement periodic rekeying to improve security
    async fn hello(&mut self, mut con: TcpStream) -> Result<Channel> {
        use protocol::publisher::Hello;
        static NO: &str = "authentication mechanism not supported";
        debug!("hello_client");
        channel::write_raw(&mut con, &3u64).await?;
        if channel::read_raw::<u64, _>(&mut con).await? != 3 {
            bail!("incompatible protocol version")
        }
        let hello: Hello = channel::read_raw(&mut con).await?;
        debug!("hello_client received {:?}", hello);
        match hello {
            Hello::Anonymous => {
                channel::write_raw(&mut con, &Hello::Anonymous).await?;
                self.client_arrived();
                Ok(Channel::new::<ServerCtx, TcpStream>(None, con))
            }
            Hello::Local(uifo) => {
                channel::write_raw(&mut con, &Hello::Local(None)).await?;
                self.set_user(uifo);
                self.client_arrived();
                Ok(Channel::new::<ServerCtx, TcpStream>(None, con))
            }
            Hello::Krb5(uifo) => match &self.desired_auth {
                DesiredAuth::Anonymous | DesiredAuth::Tls { .. } => bail!(NO),
                DesiredAuth::Local => {
                    channel::write_raw(&mut con, &Hello::Local(None)).await?;
                    self.set_user(uifo);
                    self.client_arrived();
                    Ok(Channel::new::<ServerCtx, TcpStream>(None, con))
                }
                DesiredAuth::Krb5 { upn: _, spn } => {
                    let spn = spn.as_ref().map(|s| s.as_str());
                    let ctx = krb5_authentication(HELLO_TIMEOUT, spn, &mut con).await?;
                    self.set_user(uifo);
                    let mut con = Channel::new(Some(K5CtxWrap::new(ctx)), con);
                    con.send_one(&Hello::Krb5(None)).await?;
                    self.client_arrived();
                    Ok(con)
                }
            },
            Hello::Tls(uifo) => match &self.desired_auth {
                DesiredAuth::Anonymous | DesiredAuth::Krb5 { .. } => bail!(NO),
                DesiredAuth::Local => {
                    channel::write_raw(&mut con, &Hello::Local(None)).await?;
                    self.set_user(uifo);
                    self.client_arrived();
                    Ok(Channel::new::<ServerCtx, TcpStream>(None, con))
                }
                DesiredAuth::Tls { identity } => {
                    let tls =
                        self.tls_ctx.as_ref().ok_or_else(|| anyhow!("no tls ctx"))?;
                    let ctx = task::spawn_blocking({
                        let identity = identity.clone();
                        let tls = tls.clone();
                        move || tls.load(identity.as_ref().map(|s| s.as_str()))
                    })
                    .await??;
                    let tls = time::timeout(HELLO_TIMEOUT, ctx.accept(con)).await??;
                    self.set_user(uifo);
                    let mut con = Channel::new::<
                        ServerCtx,
                        tokio_rustls::server::TlsStream<TcpStream>,
                    >(None, tls);
                    con.send_one(&Hello::Tls(None)).await?;
                    self.client_arrived();
                    Ok(con)
                }
            },
            Hello::ResolverAuthenticate(id) => {
                info!("hello_client processing listener ownership check from resolver");
                let mut con = Channel::new::<ServerCtx, TcpStream>(None, con);
                let secret = self
                    .secrets
                    .read()
                    .get(&id)
                    .copied()
                    .ok_or_else(|| anyhow!("no secret"))?;
                let challenge: AuthChallenge = con.receive().await?;
                if challenge.hash_method != HashMethod::Sha3_512 {
                    bail!("requested hash method not supported")
                }
                let reply = utils::make_sha3_token([
                    &challenge.challenge.to_be_bytes()[..],
                    &secret.to_be_bytes()[..],
                ]);
                con.send_one(&BoundedBytes::<4096>(reply)).await?;
                // prevent fishing for the key
                time::sleep(Duration::from_secs(1)).await;
                bail!("resolver authentication complete");
            }
        }
    }

    fn handle_deferred_sub(
        &mut self,
        con: &mut WriteChannel,
        s: Option<BatchItem<(Path, Permissions)>>,
    ) -> Result<()> {
        match s {
            None => (),
            Some(BatchItem::InBatch(v)) => {
                self.deferred_subs_batch.push(v);
            }
            Some(BatchItem::EndBatch) => match self.publisher.upgrade() {
                None => {
                    self.deferred_subs_batch.clear();
                }
                Some(t) => {
                    let mut pb = t.0.lock();
                    for (path, perms) in self.deferred_subs_batch.drain(..) {
                        if !pb.by_path.contains_key(path.as_ref()) {
                            let m = publisher::From::NoSuchValue(path);
                            con.queue_send(&m)?
                        } else {
                            subscribe(
                                &mut *pb,
                                con,
                                self.client,
                                path,
                                perms,
                                &mut self.deferred_subs,
                            )?
                        }
                    }
                }
            },
        }
        Ok(())
    }

    fn handle_batch_inner(&mut self, con: &mut WriteChannel) -> Result<()> {
        use protocol::publisher::{From, To::*};
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_secs();
        let t_st = self.publisher.upgrade().ok_or_else(|| anyhow!("dead publisher"))?;
        let mut pb = t_st.0.lock();
        let secrets = self.secrets.read();
        let mut gc = false;
        for msg in self.batch.drain(..) {
            match msg {
                Subscribe { path, resolver, timestamp, permissions, token } => {
                    gc = true;
                    match self.desired_auth {
                        DesiredAuth::Anonymous => subscribe(
                            &mut *pb,
                            con,
                            self.client,
                            path,
                            Permissions::all(),
                            &mut self.deferred_subs,
                        )?,
                        DesiredAuth::Krb5 { .. }
                        | DesiredAuth::Local
                        | DesiredAuth::Tls { .. } => match secrets.get(&resolver) {
                            None => {
                                debug!("denied, no stored secret for {}", resolver);
                                con.queue_send(&From::Denied(path))?
                            }
                            Some(secret) => {
                                let (valid, permissions) = check_token(
                                    token,
                                    now,
                                    *secret,
                                    timestamp,
                                    permissions,
                                    &path,
                                )?;
                                if !valid {
                                    debug!("subscribe permission denied");
                                    con.queue_send(&From::Denied(path))?
                                } else {
                                    subscribe(
                                        &mut *pb,
                                        con,
                                        self.client,
                                        path,
                                        permissions,
                                        &mut self.deferred_subs,
                                    )?
                                }
                            }
                        },
                    }
                }
                Write(id, r, v) => write(
                    &mut *pb,
                    con,
                    self.client,
                    &mut self.gc_on_write,
                    &mut self.wait_write_res,
                    &mut self.write_batches,
                    id,
                    v,
                    r,
                )?,
                Unsubscribe(id) => {
                    gc = true;
                    unsubscribe(&mut *pb, self.client, id);
                    con.queue_send(&From::Unsubscribed(id))?;
                }
            }
        }
        if gc {
            pb.hc_subscribed.retain(|_, v| Arc::get_mut(v).is_none());
        }
        for c in self.gc_on_write.drain(..) {
            pb.on_write_chans.remove(&c);
        }
        Ok(())
    }

    fn handle_batch(&mut self, con: &mut WriteChannel) -> Result<()> {
        use protocol::publisher::From;
        self.handle_batch_inner(con)?;
        if self.write_batches.len() > 0 || self.wait_write_res.len() > 0 {
            self.blocked_writes.extend(self.write_batches.drain().map(
                |(_, (batch, mut sender))| {
                    Box::pin(async move {
                        let _ = sender.send(batch).await;
                        BlockedWrite::Wrote
                    }) as BlockedWriteFut
                },
            ));
            self.blocked_writes.extend(self.wait_write_res.drain(..).map(|(id, rx)| {
                Box::pin(async move {
                    BlockedWrite::Reply(match rx.await {
                        Ok(v) => From::WriteResult(id, v),
                        Err(_) => From::WriteResult(id, Value::Ok),
                    })
                }) as BlockedWriteFut
            }));
        }
        Ok(())
    }

    fn handle_updates(
        &mut self,
        con: &mut WriteChannel,
        (timeout, mut up): (Option<Duration>, Update),
    ) -> Result<()> {
        use publisher::To;
        for m in up.updates.drain(..) {
            con.queue_send(&m)?
        }
        if let Some(usubs) = &mut up.unsubscribes {
            for id in usubs.drain(..) {
                self.batch.push(To::Unsubscribe(id));
            }
        }
        if self.batch.len() > 0 {
            self.handle_batch(con)?;
        }
        if con.bytes_queued() > 0 {
            self.flushing_updates = true;
            self.flush_timeout = timeout;
            self.msg_sent = true;
        }
        Ok(())
    }

    async fn run(
        mut self,
        con: TcpStream,
        mut updates: Receiver<(Option<Duration>, Update)>,
    ) -> Result<()> {
        async fn flush(c: &mut WriteChannel, timeout: Option<Duration>) -> Result<()> {
            if c.bytes_queued() > 0 {
                if let Some(timeout) = timeout {
                    c.flush_timeout(timeout).await
                } else {
                    c.flush().await
                }
            } else {
                future::pending().await
            }
        }
        async fn read_updates(
            flushing: bool,
            c: &mut Receiver<(Option<Duration>, Update)>,
        ) -> Option<(Option<Duration>, Update)> {
            if flushing {
                future::pending().await
            } else {
                c.next().await
            }
        }
        async fn read_from_subscriber(
            con: &mut ReadChannel,
            batch: &mut Vec<publisher::To>,
            blocked: &mut FuturesUnordered<BlockedWriteFut>,
        ) -> Result<Option<publisher::From>> {
            loop {
                if blocked.len() == 0 {
                    con.receive_batch(batch).await?;
                    break Ok(None);
                } else {
                    if let Some(w) = blocked.next().await {
                        match w {
                            BlockedWrite::Wrote => (),
                            BlockedWrite::Reply(m) => break Ok(Some(m)),
                        }
                    }
                }
            }
        }
        let mut hb = time::interval(HB);
        let (mut read_con, mut write_con) =
            time::timeout(HELLO_TIMEOUT, self.hello(con)).await??.split();
        loop {
            select_biased! {
                r = flush(&mut write_con, self.flush_timeout).fuse() => {
                    r?;
                    self.flushing_updates = false;
                    self.flush_timeout = None;
                },
                _ = hb.tick().fuse() => {
                    if !self.msg_sent {
                        write_con.queue_send(&publisher::From::Heartbeat)?;
                    }
                    self.msg_sent = false;
                },
                s = self.deferred_subs.next() =>
                    self.handle_deferred_sub(&mut write_con, s)?,
                r = read_from_subscriber(
                    &mut read_con,
                    &mut self.batch,
                    &mut self.blocked_writes
                ).fuse() => match r {
                    Err(e) => return Err(Error::from(e)),
                    Ok(None) => self.handle_batch(&mut write_con)?,
                    Ok(Some(m)) => {
                        write_con.queue_send(&m)?;
                        self.msg_sent = true;
                    },
                },
                u = read_updates(self.flushing_updates, &mut updates).fuse() => {
                    match u {
                        None => break Ok(()),
                        Some(u) => self.handle_updates(&mut write_con, u)?,
                    }
                },
            }
        }
    }
}

pub(super) async fn start(
    t: PublisherWeak,
    serv: TcpListener,
    stop: oneshot::Receiver<()>,
    desired_auth: DesiredAuth,
    tls_ctx: Option<tls::CachedAcceptor>,
    max_clients: usize,
    slack: usize,
) {
    let mut stop = stop.fuse();
    loop {
        select_biased! {
            _ = stop => break,
            cl = serv.accept().fuse() => match cl {
                Err(e) => info!("accept error {}", e), // CR estokes: Handle this
                Ok((s, addr)) => {
                    debug!("accepted client {:?}", addr);
                    let clid = ClId::new();
                    let t_weak = t.clone();
                    let t = match t.upgrade() {
                        None => return,
                        Some(t) => t
                    };
                    let mut pb = t.0.lock();
                    let secrets = pb.resolver.secrets();
                    let (tx, rx) = channel(slack);
                    try_cf!("nodelay", continue, s.set_nodelay(true));
                    if pb.clients.len() < max_clients {
                        pb.clients.insert(clid, Client {
                            msg_queue: tx,
                            subscribed: HashMap::default(),
                            user: None,
                        });
                        let desired_auth = desired_auth.clone();
                        let tls_ctx = tls_ctx.clone();
                        task::spawn(async move {
                            let ctx = ClientCtx::new(
                                clid,
                                secrets,
                                t_weak.clone(),
                                desired_auth,
                                tls_ctx,
                            );
                            let r = ctx.run(s, rx).await;
                            info!("accept_loop client shutdown {:?}", r);
                            if let Some(t) = t_weak.upgrade() {
                                let mut pb = t.0.lock();
                                if let Some(cl) = pb.clients.remove(&clid) {
                                    for (id, _) in cl.subscribed {
                                        unsubscribe(&mut *pb, clid, id);
                                    }
                                    pb.hc_subscribed.retain(|_, v| {
                                        Arc::get_mut(v).is_none()
                                    });
                                }
                            }
                        });
                    }
                }
            },
        }
    }
}
