use super::{
    ConId, DvDead, DvState, Event, NoSuchValue, PermissionDenied, SubId, SubStatus,
    SubscribeValRequest, Subscriber, SubscriberInner, SubscriberWeak, ToCon,
    UpdatesFlags, Val, ValInner, ValWeak, WUpdateChan, BATCHES, DECODE_BATCHES,
};
pub use crate::protocol::value::{FromValue, Value};
pub use crate::resolver_client::DesiredAuth;
use crate::{
    batch_channel::BatchReceiver,
    channel::{self, Channel, K5CtxWrap, ReadChannel, WriteChannel},
    path::Path,
    protocol::{
        self,
        publisher::{From, Id, To, WriteId},
        resolver::TargetAuth,
    },
    resolver_client::common::krb5_authentication,
    tls,
    utils::{ChanId, ChanWrap},
};
use anyhow::{anyhow, Error, Result};
use cross_krb5::ClientCtx;
use futures::{
    channel::{
        mpsc::{self, Receiver},
        oneshot,
    },
    prelude::*,
    select_biased,
    stream::FuturesUnordered,
};
use fxhash::{FxHashMap, FxHashSet};
use log::{info, trace};
use parking_lot::Mutex;
use poolshark::global::GPooled;
use protocol::resolver::UserInfo;
use smallvec::SmallVec;
use std::hint::unreachable_unchecked;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    mem,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpStream,
    task,
    time::{self, Instant},
};
use triomphe::Arc as TArc;

#[derive(Debug)]
struct Sub {
    path: Path,
    sub_id: SubId,
    streams: SmallVec<[(ChanId, ChanWrap<GPooled<Vec<(SubId, Event)>>>); 1]>,
    last: Option<TArc<Mutex<Event>>>,
    val: ValWeak,
}

type ByChan = FxHashMap<
    ChanId,
    (ChanWrap<GPooled<Vec<(SubId, Event)>>>, GPooled<Vec<(SubId, Event)>>),
>;

fn unsubscribe(
    subscriber: &mut SubscriberInner,
    by_chan: &mut ByChan,
    sub: Sub,
    id: Id,
    conid: ConId,
) {
    for (chan_id, c) in sub.streams.iter() {
        by_chan
            .entry(*chan_id)
            .or_insert_with(|| (c.clone(), BATCHES.take()))
            .1
            .push((sub.sub_id, Event::Unsubscribed))
    }
    if let Some(last) = &sub.last {
        *last.lock() = Event::Unsubscribed;
    }
    if let Some(dsw) = subscriber
        .durable_alive
        .remove(&sub.path)
        .or_else(|| subscriber.durable_pending.remove(&sub.path))
    {
        trace!("unsubscribing {}", sub.path);
        if let Some(ds) = dsw.upgrade() {
            let mut inner = ds.0.lock();
            inner.sub = DvState::Dead(Box::new(DvDead {
                queued_writes: Vec::new(),
                waiting: Vec::new(),
                tries: 0,
                next_try: Instant::now(),
            }));
            subscriber.durable_dead.insert(sub.path.clone(), dsw);
            let _ = subscriber.trigger_resub.unbounded_send(());
        }
    }
    match subscriber.subscribed.entry(sub.path) {
        Entry::Vacant(_) => (),
        Entry::Occupied(e) => match e.get() {
            SubStatus::Pending(_) => (),
            SubStatus::Subscribed(s) => match s.upgrade() {
                None => {
                    e.remove();
                }
                Some(s) => {
                    if s.0.id == id && s.0.conid == conid {
                        e.remove();
                    }
                }
            },
        },
    }
}

async fn hello_publisher(
    mut con: TcpStream,
    tls_ctx: Option<tls::CachedConnector>,
    uifo: Option<UserInfo>,
    desired_auth: &DesiredAuth,
    target_auth: &TargetAuth,
) -> Result<Channel> {
    use protocol::publisher::Hello;
    channel::write_raw(&mut con, &3u64).await?;
    if channel::read_raw::<u64, _, 1024>(&mut con).await? != 3 {
        bail!("incompatible protocol version")
    }
    match (desired_auth, target_auth) {
        (DesiredAuth::Anonymous, TargetAuth::Anonymous) => {
            channel::write_raw(&mut con, &Hello::Anonymous).await?;
            match channel::read_raw::<_, _, 8124>(&mut con).await? {
                Hello::Anonymous => (),
                _ => bail!("unexpected response from publisher"),
            }
            Ok(Channel::new::<ClientCtx, TcpStream>(None, con))
        }
        (
            DesiredAuth::Anonymous,
            TargetAuth::Local { .. } | TargetAuth::Krb5 { .. } | TargetAuth::Tls { .. },
        ) => {
            bail!("anonymous access not allowed")
        }
        (
            DesiredAuth::Local | DesiredAuth::Krb5 { .. } | DesiredAuth::Tls { .. },
            TargetAuth::Anonymous,
        ) => {
            bail!("authentication not supported")
        }
        (
            DesiredAuth::Local | DesiredAuth::Krb5 { .. } | DesiredAuth::Tls { .. },
            TargetAuth::Local,
        ) => {
            channel::write_raw(&mut con, &Hello::Local(uifo)).await?;
            match channel::read_raw::<_, _, 8124>(&mut con).await? {
                Hello::Local(_) => (),
                _ => bail!("unexpected response from publisher"),
            }
            Ok(Channel::new::<ClientCtx, TcpStream>(None, con))
        }
        (DesiredAuth::Local, TargetAuth::Krb5 { .. } | TargetAuth::Tls { .. }) => {
            bail!("local auth not supported")
        }
        (DesiredAuth::Krb5 { upn, .. }, TargetAuth::Krb5 { spn }) => {
            let upn = upn.as_ref().map(|p| p.as_str());
            channel::write_raw(&mut con, &Hello::Krb5(uifo)).await?;
            let ctx = krb5_authentication(upn, spn, &mut con).await?;
            let mut con = Channel::new(Some(K5CtxWrap::new(ctx)), con);
            match con.receive::<Hello>().await? {
                Hello::Krb5(_) => (),
                _ => bail!("protocol error"),
            }
            Ok(con)
        }
        (DesiredAuth::Krb5 { .. }, TargetAuth::Tls { .. }) => {
            bail!("desired authentication mechanism not supported")
        }
        (DesiredAuth::Tls { .. }, TargetAuth::Tls { name }) => {
            let tls = tls_ctx.clone().ok_or_else(|| anyhow!("no tls ctx"))?;
            let ctx = task::spawn_blocking({
                let name = name.clone();
                move || tls.load(&name)
            })
            .await??;
            let name = rustls_pki_types::ServerName::try_from(&**name)?.to_owned();
            channel::write_raw(&mut con, &Hello::Tls(uifo)).await?;
            let tls = ctx.connect(name, con).await?;
            let mut con = Channel::new::<
                ClientCtx,
                tokio_rustls::client::TlsStream<TcpStream>,
            >(None, tls);
            match con.receive::<Hello>().await? {
                Hello::Tls(_) => (),
                _ => bail!("protocol error"),
            }
            Ok(con)
        }
        (DesiredAuth::Tls { .. }, TargetAuth::Krb5 { .. }) => {
            bail!("desired authentication mechanism not supported")
        }
    }
}

const PERIOD: Duration = Duration::from_secs(100);

fn decode_task(
    mut con: ReadChannel,
    stop: oneshot::Receiver<()>,
) -> Receiver<Result<(GPooled<Vec<From>>, bool)>> {
    let (mut send, recv) = mpsc::channel(3);
    let mut stop = stop.fuse();
    task::spawn(async move {
        let mut buf = DECODE_BATCHES.take();
        let r: Result<(), anyhow::Error> = loop {
            let mut only_updates = true;
            select_biased! {
                _ = stop => { break Ok(()); },
                r = con.receive_batch_fn(|up| {
                    match up {
                        From::Update(_, _) => (),
                        _ => { only_updates = false }
                    }
                    buf.push(up);
                }).fuse() => match r {
                    Err(e) => {
                        buf.clear();
                        try_cf!(send.send(Err(e)).await)
                    }
                    Ok(()) => {
                        let batch = mem::replace(&mut buf, DECODE_BATCHES.take());
                        try_cf!(send.send(Ok((batch, only_updates))).await)
                    }
                }
            }
        };
        info!("decode task shutting down {:?}", r);
    });
    recv
}

type BlockedChannelFut = Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>;

pub(super) struct ConnectionCtx {
    addr: SocketAddr,
    subscriber: SubscriberWeak,
    target_auth: TargetAuth,
    desired_auth: DesiredAuth,
    conid: ConId,
    tls_ctx: Option<tls::CachedConnector>,
    uifo: Option<UserInfo>,
    from_sub: BatchReceiver<ToCon>,
    pending: HashMap<Path, SubscribeValRequest>,
    subscriptions: FxHashMap<Id, Sub>,
    msg_recvd: bool,
    pending_flushes: Vec<oneshot::Sender<()>>,
    pending_writes: FxHashMap<Id, FxHashMap<WriteId, oneshot::Sender<Value>>>,
    by_receiver: FxHashMap<ChanWrap<GPooled<Vec<(SubId, Event)>>>, ChanId>,
    by_chan: ByChan,
    gc_chan: FxHashSet<ChanId>,
    blocked_channels: FuturesUnordered<BlockedChannelFut>,
    timed_out: Vec<Path>,
}

impl ConnectionCtx {
    pub(super) fn new(
        addr: SocketAddr,
        subscriber: SubscriberWeak,
        conid: ConId,
        tls_ctx: Option<tls::CachedConnector>,
        uifo: Option<UserInfo>,
        target_auth: TargetAuth,
        desired_auth: DesiredAuth,
        from_sub: BatchReceiver<ToCon>,
    ) -> Self {
        Self {
            addr,
            subscriber,
            target_auth,
            desired_auth,
            conid,
            tls_ctx,
            uifo,
            from_sub,
            pending: HashMap::default(),
            subscriptions: HashMap::default(),
            msg_recvd: false,
            pending_flushes: Vec::new(),
            pending_writes: HashMap::default(),
            by_receiver: HashMap::default(),
            by_chan: HashMap::default(),
            gc_chan: HashSet::default(),
            blocked_channels: FuturesUnordered::<BlockedChannelFut>::new(),
            timed_out: Vec::new(),
        }
    }

    fn handle_heartbeat(&mut self, now: Instant) -> Result<()> {
        if !self.msg_recvd {
            bail!("hung publisher");
        } else {
            self.msg_recvd = false;
        }
        for (path, req) in self.pending.iter() {
            if let Some(deadline) = req.deadline {
                if deadline < now {
                    self.timed_out.push(path.clone());
                }
            }
        }
        for path in self.timed_out.drain(..) {
            if let Some(req) = self.pending.remove(&path) {
                let _ = req.finished.send(Err(anyhow!("timed out")));
            }
        }
        Ok(())
    }

    fn handle_connect_stream(
        &mut self,
        stream_batch: &mut Vec<From>,
        id: Id,
        tx: WUpdateChan,
        flags: UpdatesFlags,
    ) -> Result<()> {
        if let Some(sub) = self.subscriptions.get_mut(&id) {
            let mut already_have = false;
            for (id, c) in sub.streams.iter() {
                if &tx == c {
                    trace!("ignore already registered stream");
                    already_have = true;
                }
                if c.0.is_closed() {
                    trace!("scheduling closed stream for gc");
                    self.by_receiver.remove(&c);
                    self.gc_chan.insert(*id);
                }
            }
            if flags.contains(UpdatesFlags::BEGIN_WITH_LAST)
                && !(already_have && flags.contains(UpdatesFlags::NO_SPURIOUS))
            {
                if let Some(last) = &sub.last {
                    if let Event::Update(v) = last.lock().clone() {
                        stream_batch.push(From::Update(id, v.clone()));
                    }
                }
            }
            if flags.contains(UpdatesFlags::STOP_COLLECTING_LAST) {
                trace!("no longer collecting last");
                sub.last = None;
            }
            if !already_have {
                trace!("adding new channel to streams");
                let id = self.by_receiver.entry(tx.clone()).or_insert_with(ChanId::new);
                sub.streams.push((*id, tx));
            }
        }
        Ok(())
    }

    fn handle_from_sub(
        &mut self,
        write_con: &mut WriteChannel,
        mut batch: GPooled<Vec<ToCon>>,
    ) -> Result<()> {
        let mut stream_batch = DECODE_BATCHES.take();
        for msg in batch.drain(..) {
            match msg {
                ToCon::Subscribe(req) => {
                    let path = req.path.clone();
                    let resolver = req.resolver;
                    let token = req.token.clone();
                    let permissions = req.permissions;
                    let timestamp = req.timestamp;
                    self.pending.insert(path.clone(), req);
                    let m =
                        To::Subscribe { path, resolver, timestamp, permissions, token };
                    trace!("subscribe {m:?}");
                    write_con.queue_send(&m)?
                }
                ToCon::Unsubscribe(id) => {
                    info!("unsubscribe {:?}", id);
                    write_con.queue_send(&To::Unsubscribe(id))?
                }
                ToCon::Stream { id, tx, flags } => {
                    self.handle_connect_stream(&mut stream_batch, id, tx, flags)?
                }
                ToCon::Write(id, v, wid, tx) => {
                    write_con.queue_send(&To::Write(id, tx.is_some(), v, wid))?;
                    if let Some(tx) = tx {
                        self.pending_writes
                            .entry(id)
                            .or_insert_with(HashMap::default)
                            .insert(wid, tx);
                    }
                }
                ToCon::Flush(tx) => self.pending_flushes.push(tx),
            }
        }
        if stream_batch.len() > 0 {
            self.process_updates_batch(stream_batch)
        }
        Ok(())
    }

    fn process_batch(
        &mut self,
        mut batch: GPooled<Vec<From>>,
        con: &mut WriteChannel,
        subscriber: &Subscriber,
    ) -> Result<()> {
        let mut stream_batch = DECODE_BATCHES.take();
        for m in batch.drain(..) {
            trace!("processing from publisher {m:?}");
            match m {
                From::Update(i, m) => match self.subscriptions.get(&i) {
                    Some(sub) => {
                        for (chan_id, c) in sub.streams.iter() {
                            self.by_chan
                                .entry(*chan_id)
                                .or_insert_with(|| (c.clone(), BATCHES.take()))
                                .1
                                .push((sub.sub_id, Event::Update(m.clone())));
                        }
                        if let Some(last) = &sub.last {
                            *last.lock() = Event::Update(m);
                        }
                    }
                    None => con.queue_send(&To::Unsubscribe(i))?,
                },
                From::Heartbeat => (),
                From::WriteResult(id, v, wid) => {
                    if let Entry::Occupied(mut e) = self.pending_writes.entry(id) {
                        let tbl = e.get_mut();
                        if let Some(tx) = tbl.remove(&wid) {
                            let _ = tx.send(v);
                        }
                        if tbl.is_empty() {
                            e.remove();
                        }
                    }
                }
                From::NoSuchValue(path) => {
                    if let Some(r) = self.pending.remove(&path) {
                        let _ = r.finished.send(Err(Error::from(NoSuchValue)));
                    }
                }
                From::Denied(path) => {
                    if let Some(r) = self.pending.remove(&path) {
                        let _ = r.finished.send(Err(Error::from(PermissionDenied)));
                    }
                }
                From::Unsubscribed(id) => {
                    if let Some(s) = self.subscriptions.remove(&id) {
                        let mut t = subscriber.0.lock();
                        unsubscribe(&mut *t, &mut self.by_chan, s, id, self.conid);
                    }
                }
                From::Subscribed(p, id, m) => {
                    match self.pending.remove(&p) {
                        None => {
                            trace!("subscribed for id with no subscription");
                            con.queue_send(&To::Unsubscribe(id))?
                        }
                        Some(req) => match self.subscriptions.get_mut(&id) {
                            Some(sub) => match sub.val.upgrade() {
                                // we're subscribed to an alias
                                Some(val) => {
                                    trace!("subscribe to alias success");
                                    // we ignore last in this case because we already have it
                                    for (f, c) in req.streams {
                                        self.handle_connect_stream(
                                            &mut stream_batch,
                                            id,
                                            c,
                                            f | UpdatesFlags::BEGIN_WITH_LAST,
                                        )?
                                    }
                                    let _ = req.finished.send(Ok(val));
                                }
                                None => {
                                    trace!("alias pair dropped while subscribing");
                                    let _ = req.finished.send(Err(anyhow!(
                                        "subscribe alias while unsubscribing"
                                    )));
                                }
                            },
                            None => {
                                trace!("subscribe success");
                                let last = TArc::new(Mutex::new(Event::Update(m)));
                                let s = Val(Arc::new(ValInner {
                                    sub_id: req.sub_id,
                                    id,
                                    conid: self.conid,
                                    connection: req.con,
                                    last: last.clone(),
                                }));
                                match req.finished.send(Ok(s.clone())) {
                                    Err(e) => {
                                        trace!("could not deliver finished subscription {:?}", e);
                                        con.queue_send(&To::Unsubscribe(id))?
                                    }
                                    Ok(()) => {
                                        trace!("storing finished subscripiton");
                                        self.subscriptions.insert(
                                            id,
                                            Sub {
                                                path: req.path,
                                                sub_id: req.sub_id,
                                                last: Some(last),
                                                streams: SmallVec::new(),
                                                val: s.downgrade(),
                                            },
                                        );
                                    }
                                }
                                trace!("connecting {} streams", req.streams.len());
                                for (f, c) in req.streams {
                                    self.handle_connect_stream(
                                        &mut stream_batch,
                                        id,
                                        c,
                                        f | UpdatesFlags::BEGIN_WITH_LAST,
                                    )?
                                }
                            }
                        },
                    }
                }
            }
        }
        if stream_batch.len() > 0 {
            self.process_updates_batch(stream_batch)
        } else {
            self.send_updates();
        }
        Ok(())
    }

    // This is the fast path for the common case where the batch contains
    // only updates. As of 2020-04-30, sending to an mpsc channel is
    // pretty slow, about 250ns, so we go to great lengths to avoid it.
    fn process_updates_batch(&mut self, mut batch: GPooled<Vec<From>>) {
        for m in batch.drain(..) {
            match m {
                From::Update(i, m) => {
                    if let Some(sub) = self.subscriptions.get(&i) {
                        for (chan_id, c) in sub.streams.iter() {
                            self.by_chan
                                .entry(*chan_id)
                                .or_insert_with(|| (c.clone(), BATCHES.take()))
                                .1
                                .push((sub.sub_id, Event::Update(m.clone())))
                        }
                        if let Some(last) = &sub.last {
                            *last.lock() = Event::Update(m);
                        }
                    }
                }
                _ => unsafe { unreachable_unchecked() },
            }
        }
        self.send_updates()
    }

    fn send_updates(&mut self) {
        for (id, (c, batch)) in self.by_chan.iter_mut() {
            if batch.len() == 0 {
                continue;
            }
            let batch = mem::replace(batch, BATCHES.take());
            if let Err(e) = c.0.try_send(batch) {
                if e.is_full() {
                    let batch = e.into_inner();
                    let mut c = c.clone();
                    self.blocked_channels.push(Box::pin(async move {
                        let _ = c.0.send(batch).await;
                    }))
                } else if e.is_disconnected() {
                    self.by_receiver.remove(c);
                    self.gc_chan.insert(*id);
                }
            }
        }
        for id in self.gc_chan.drain() {
            self.by_chan.remove(&id);
        }
    }

    // return true if we should keep running, false if we are idle
    fn maybe_disconnect_idle(&mut self) -> bool {
        match self.subscriber.upgrade() {
            None => false,
            Some(subscriber) => {
                if self.subscriptions.is_empty()
                    && self.pending.is_empty()
                    && self.blocked_channels.is_empty()
                {
                    let mut inner = subscriber.0.lock();
                    if self.from_sub.len() == 0 {
                        // we do this here the make sure we
                        // hold the lock and there can be no
                        // subscriptions while we clean up.
                        if let Entry::Occupied(mut e) = inner.connections.entry(self.addr)
                        {
                            let c = e.get_mut();
                            c.remove(self.conid);
                            if c.is_empty() {
                                e.remove();
                            }
                        }
                        return false;
                    }
                }
                true
            }
        }
    }

    fn handle_updates(
        &mut self,
        write_con: &mut WriteChannel,
        batch: GPooled<Vec<From>>,
    ) -> Result<bool> {
        if let Some(subscriber) = self.subscriber.upgrade() {
            self.msg_recvd = true;
            self.process_batch(batch, write_con, &subscriber)?;
        }
        Ok(self.maybe_disconnect_idle())
    }

    async fn run(
        &mut self,
        mut batches: Receiver<Result<(GPooled<Vec<From>>, bool)>>,
        write_con: &mut WriteChannel,
    ) -> Result<()> {
        async fn read_batch(
            batches: &mut Receiver<Result<(GPooled<Vec<From>>, bool)>>,
            blocked: &mut FuturesUnordered<BlockedChannelFut>,
        ) -> Option<Result<(GPooled<Vec<From>>, bool)>> {
            loop {
                if blocked.len() > 0 {
                    let _: Option<_> = blocked.next().await;
                } else {
                    break batches.next().await;
                }
            }
        }
        async fn flush(
            con: &mut WriteChannel,
            pending: &mut Vec<oneshot::Sender<()>>,
        ) -> Result<()> {
            let mut flushed = || {
                for s in pending.drain(..) {
                    let _ = s.send(());
                }
            };
            if con.bytes_queued() == 0 {
                flushed();
                future::pending().await
            } else {
                con.flush().await?;
                flushed();
                Ok(())
            }
        }
        let mut periodic = time::interval_at(Instant::now() + PERIOD, PERIOD);
        loop {
            select_biased! {
                // this has to come first because batch_channel isn't cancel safe
                batch = self.from_sub.recv().fuse() => match batch {
                    Some(batch) => self.handle_from_sub(write_con, batch)?,
                    None => break Ok(()),
                },
                r = flush(write_con, &mut self.pending_flushes).fuse() => r?,
                now = periodic.tick().fuse() => {
                    self.handle_heartbeat(now)?;
                    if !self.maybe_disconnect_idle() {
                        break Ok(())
                    }
                },
                r = read_batch(
                    &mut batches,
                    &mut self.blocked_channels
                ).fuse() => match r {
                    Some(Ok((batch, true))) => {
                        self.msg_recvd = true;
                        self.process_updates_batch(batch);
                    },
                    Some(Ok((batch, false))) => {
                        if !self.handle_updates(write_con, batch)? {
                            break Ok(())
                        }
                    },
                    Some(Err(e)) => break Err(Error::from(e)),
                    None => break Err(anyhow!("EOF")),
                }
            }
        }
    }

    pub(super) async fn start(mut self) -> Result<()> {
        let soc = time::timeout(PERIOD, TcpStream::connect(self.addr)).await??;
        soc.set_nodelay(true)?;
        const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
        let con = time::timeout(
            HELLO_TIMEOUT,
            hello_publisher(
                soc,
                self.tls_ctx.clone(),
                self.uifo.take(),
                &self.desired_auth,
                &self.target_auth,
            ),
        )
        .await??;
        let (read_con, mut write_con) = con.split();
        let (tx_stop, rx_stop) = oneshot::channel();
        let res = self.run(decode_task(read_con, rx_stop), &mut write_con).await;
        let _ = tx_stop.send(());
        if let Some(subscriber) = self.subscriber.upgrade() {
            let mut batch = DECODE_BATCHES.take();
            batch.extend(self.subscriptions.keys().map(|id| From::Unsubscribed(*id)));
            self.process_batch(batch, &mut write_con, &subscriber)?;
            for (_, req) in self.pending {
                let _ = req.finished.send(Err(anyhow!("connection died")));
            }
        }
        // info!("connection shutting down {res:?}");
        res
    }
}
