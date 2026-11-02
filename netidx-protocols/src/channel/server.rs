use super::HANDSHAKE_TIMEOUT;
use anyhow::{Result, anyhow};
use futures::{channel::mpsc, prelude::*, stream::FuturesUnordered};
use log::warn;
use netidx::{
    path::Path,
    protocol::resolver::UserInfo,
    publisher::{
        ClId, Event, PublishFlags, Publisher, SendResult, UpdateBatch, Val, Value,
        WriteRequest,
    },
};
use poolshark::global::GPooled;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, oneshot},
    task::{self, JoinHandle},
    time,
};

/// The most handshakes a listener runs at once. A "connect" beyond them
/// waits in the listener's write channel.
const MAX_HANDSHAKES: usize = 64;

/// Generate a random session name ${base}/uuid
pub fn session(base: &Path) -> Path {
    use uuid::{Uuid, fmt::Simple};
    let id = Uuid::new_v4();
    let mut buf = [0u8; Simple::LENGTH];
    base.append(Simple::from_uuid(id).encode_lower(&mut buf))
}

pub struct Batch {
    anchor: Arc<Val>,
    client: ClId,
    queued: UpdateBatch,
}

impl Batch {
    /// queue a value in the batch
    pub fn queue(&mut self, v: Value) {
        self.anchor.update_subscriber(&mut self.queued, self.client, v);
    }

    /// returns the number of values queued in this batch
    pub fn len(&self) -> usize {
        self.queued.len()
    }
}

struct Receiver {
    writes: mpsc::Receiver<GPooled<Vec<WriteRequest>>>,
    /// The anchor's subscribe and unsubscribe events. A receive waits on
    /// them too, so it wakes when the client goes away.
    events: mpsc::UnboundedReceiver<Event>,
    queued: VecDeque<Value>,
}

impl Receiver {
    fn fill_from_channel(
        &mut self,
        dead: &AtomicBool,
        client: ClId,
        r: Option<GPooled<Vec<WriteRequest>>>,
    ) -> Result<()> {
        match r {
            Some(mut batch) => self.queued.extend(batch.drain(..).filter_map(|req| {
                if req.client == client { Some(req.value) } else { None }
            })),
            None => {
                dead.store(true, Ordering::Relaxed);
                bail!("connection is dead")
            }
        }
        Ok(())
    }

    fn try_fill_queue(&mut self, dead: &AtomicBool, client: ClId) -> Result<()> {
        for _ in 0..10 {
            match self.writes.try_recv() {
                Err(_) => break,
                Ok(r) => {
                    if let Err(e) = self.fill_from_channel(dead, client, Some(r)) {
                        if self.queued.len() == 0 {
                            return Err(e);
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// This is a single pending connection. You must call wait_connected
/// to finish the handshake.
struct SingletonInner {
    publisher: Publisher,
    anchor: Arc<Val>,
    timeout: Option<Duration>,
    subscribe_within: Option<Duration>,
    writes: mpsc::Receiver<GPooled<Vec<WriteRequest>>>,
    events: mpsc::UnboundedReceiver<Event>,
}

impl SingletonInner {
    pub async fn wait_connected(self) -> Result<Connection> {
        let subscribed = async {
            loop {
                self.publisher.wait_client(self.anchor.id()).await;
                let subs = self.publisher.subscribed(&self.anchor.id());
                if subs.len() > 0 {
                    break subs;
                }
            }
        };
        let mut subscribed = match self.subscribe_within {
            None => subscribed.await,
            Some(to) => time::timeout(to, subscribed)
                .await
                .map_err(|_| anyhow!("the client did not subscribe in time"))?,
        };
        let con = Connection {
            publisher: self.publisher,
            anchor: self.anchor,
            client: subscribed.pop().unwrap(),
            dead: AtomicBool::new(false),
            timeout: self.timeout,
            receiver: Mutex::new(Receiver {
                writes: self.writes,
                events: self.events,
                queued: VecDeque::new(),
            }),
        };
        let to = HANDSHAKE_TIMEOUT;
        match time::timeout(to, con.recv_one()).await?? {
            Value::String(s) if &*s == "ready" => {
                con.send_one(Value::from("ready")).await?
            }
            v => bail!("unexpected handshake, expected String(\"ready\") got {}", v),
        }
        match time::timeout(to, con.recv_one()).await?? {
            Value::String(s) if &*s == "go" => return Ok(con),
            v => bail!("unexpected handshake, expected String(\"go\") got {}", v),
        }
    }
}

pub struct Singleton(task::JoinHandle<Result<Connection>>);

impl Drop for Singleton {
    fn drop(&mut self) {
        self.0.abort()
    }
}

impl Singleton {
    /// Wait for the client to connect. This method is cancel safe, it
    /// is safe to use in select.
    pub async fn wait_connected(&mut self) -> Result<Connection> {
        (&mut self.0).await?
    }
}

/// Create a new single connection at path. One client can connect to
/// this connection, further connection attempts will be ignored. This
/// is useful for example for returning a channel from an rpc call, in
/// that case there is no need for a listener.
pub async fn singleton_with_flags(
    publisher: &Publisher,
    flags: PublishFlags,
    timeout: Option<Duration>,
    path: Path,
) -> Result<Singleton> {
    start_singleton(publisher, flags, timeout, path, None).await
}

/// `singleton_with_flags`, with a deadline for the client's subscribe when
/// `subscribe_within` is given.
async fn start_singleton(
    publisher: &Publisher,
    flags: PublishFlags,
    timeout: Option<Duration>,
    path: Path,
    subscribe_within: Option<Duration>,
) -> Result<Singleton> {
    let publisher = publisher.clone();
    let (tx_res, rx_res) = oneshot::channel();
    let jh = task::spawn(async move {
        let (tx, rx) = mpsc::channel(5);
        let res = publisher.publish_with_flags_and_writes(
            flags | PublishFlags::ISOLATED,
            path.clone(),
            Value::from("connection"),
            Some(tx),
        );
        publisher.flushed().await;
        let (tx_events, events) = mpsc::unbounded();
        let val = match res {
            Ok(val) => {
                publisher.events_for_id(val.id(), tx_events);
                let _ = tx_res.send(Ok(()));
                val
            }
            Err(e) => {
                let _ = tx_res.send(Err(e));
                bail!("failed to publish")
            }
        };
        let inner = SingletonInner {
            publisher: publisher.clone(),
            timeout,
            subscribe_within,
            anchor: Arc::new(val),
            writes: rx,
            events,
        };
        Ok(inner.wait_connected().await?)
    });
    rx_res.await??;
    Ok(Singleton(jh))
}

pub async fn singleton(
    publisher: &Publisher,
    timeout: Option<Duration>,
    path: Path,
) -> Result<Singleton> {
    singleton_with_flags(publisher, PublishFlags::empty(), timeout, path).await
}

/// A bidirectional channel between two endpoints.
pub struct Connection {
    publisher: Publisher,
    anchor: Arc<Val>,
    client: ClId,
    dead: AtomicBool,
    timeout: Option<Duration>,
    receiver: Mutex<Receiver>,
}

impl Connection {
    /// Start a new batch of messages, return the batch. You may fill
    /// it with the queue method.
    pub fn start_batch(&self) -> Batch {
        Batch {
            anchor: self.anchor.clone(),
            client: self.client,
            queued: self.publisher.start_batch(),
        }
    }

    /// Return true of the channel has been disconnected. A
    /// disconnected channel is permanently dead.
    pub fn is_dead(&self) -> bool {
        if !self.publisher.is_subscribed(&self.anchor.id(), &self.client) {
            self.dead.store(true, Ordering::Relaxed);
        }
        self.dead.load(Ordering::Relaxed)
    }

    /// Send a batch of message to the other side
    pub async fn send(&self, batch: Batch) -> Result<()> {
        if self.is_dead() {
            bail!("connection is dead")
        }
        Ok(batch.queued.commit(self.timeout).await)
    }

    /// Send just one message to the other side. This is less
    /// efficient than send.
    pub async fn send_one(&self, v: Value) -> Result<()> {
        if self.is_dead() {
            bail!("connection is dead")
        }
        let mut batch = self.publisher.start_batch();
        self.anchor.update_subscriber(&mut batch, self.client, v);
        Ok(batch.commit(self.timeout).await)
    }

    fn check_dead(&self) -> Result<()> {
        if self.is_dead() {
            bail!("connection is dead")
        }
        Ok(())
    }

    /// Queue what the client has sent, waiting until something comes. Fails
    /// only when nothing is left to read and the client is gone.
    async fn fill(&self, recv: &mut Receiver) -> Result<()> {
        loop {
            recv.try_fill_queue(&self.dead, self.client)?;
            if recv.queued.len() > 0 {
                break Ok(());
            }
            self.check_dead()?;
            tokio::select! {
                biased;
                r = recv.writes.next() => recv.fill_from_channel(&self.dead, self.client, r)?,
                e = recv.events.next() => if e.is_none() {
                    self.dead.store(true, Ordering::Relaxed)
                },
            }
        }
    }

    /// Wait for one message from the other side, and return it when
    /// it arrives.
    pub async fn recv_one(&self) -> Result<Value> {
        let mut recv = self.receiver.lock().await;
        loop {
            match recv.queued.pop_front() {
                Some(v) => break Ok(v),
                None => self.fill(&mut recv).await?,
            }
        }
    }

    /// Return a message if one is available now, but don't wait for
    /// one to arrive. Will only block if another receive is in
    /// progress.
    pub async fn try_recv_one(&self) -> Result<Option<Value>> {
        let mut recv = self.receiver.lock().await;
        if recv.queued.len() == 0 {
            recv.try_fill_queue(&self.dead, self.client)?
        }
        Ok(recv.queued.pop_front())
    }

    /// Receive all available messages from the other side and place
    /// them in the specified data structure. If no messages are
    /// available right now, then wait until at least 1 message
    /// arrives.
    pub async fn recv(&self, dst: &mut impl Extend<Value>) -> Result<()> {
        let mut recv = self.receiver.lock().await;
        self.fill(&mut recv).await?;
        Ok(dst.extend(recv.queued.drain(..)))
    }

    /// Receive all available messages, if any, but don't wait for
    /// messages to arrive. This will only block if another receive is
    /// in progress. Returns true if any messages were received.
    pub async fn try_recv(&self, dst: &mut impl Extend<Value>) -> Result<bool> {
        let mut recv = self.receiver.lock().await;
        recv.try_fill_queue(&self.dead, self.client)?;
        if recv.queued.len() > 0 {
            dst.extend(recv.queued.drain(..));
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Return the user connected to this channel, if known
    pub fn user(&self) -> Option<UserInfo> {
        self.publisher.user(&self.client)
    }
}

/// A listener can accept connections from muliple clients and produce
/// a channel to talk to each one.
pub struct ListenerInner {
    publisher: Publisher,
    _listener: Val,
    waiting: mpsc::Receiver<GPooled<Vec<WriteRequest>>>,
    queued: GPooled<Vec<WriteRequest>>,
    base: Path,
    timeout: Option<Duration>,
    flags: PublishFlags,
}

impl ListenerInner {
    /// just like new, but with publish flags
    async fn new_with_flags(
        publisher: Publisher,
        flags: PublishFlags,
        timeout: Option<Duration>,
        path: Path,
    ) -> Result<Self> {
        let (tx_waiting, rx_waiting) = mpsc::channel(50);
        let listener = publisher.publish_with_flags_and_writes(
            flags,
            path.clone(),
            Value::from("channel"),
            Some(tx_waiting),
        )?;
        publisher.flushed().await;
        Ok(Self {
            publisher,
            _listener: listener,
            waiting: rx_waiting,
            queued: GPooled::orphan(Vec::new()),
            base: path,
            timeout,
            flags,
        })
    }

    /// Wait for the next "connect" that asked for a reply. None when the
    /// listener is closed. Cancel safe.
    async fn next_connect(&mut self) -> Option<SendResult> {
        loop {
            match self.queued.pop() {
                Some(req) => match (&req.value, req.send_result) {
                    (Value::String(c), Some(reply)) if &**c == "connect" => {
                        return Some(reply);
                    }
                    _ => (),
                },
                None => self.queued = self.waiting.next().await?,
            }
        }
    }

    /// Hand out connections until the listener is closed or nobody takes
    /// them. Every client gets its own handshake, and one that fails is
    /// logged and dropped.
    async fn run(mut self, mut tx: mpsc::Sender<Connection>) {
        let mut handshakes = FuturesUnordered::new();
        // XCR claude for estokes: [bug/high] One client can stop a
        // listener. This task runs each handshake to the end before
        // it takes the next "connect", and SingletonInner::wait_connected
        // waits for the client to subscribe with no timeout, so a
        // client that writes "connect" and dies before subscribing
        // (a killed process, an address change) stalls the listener
        // for good, with nothing logged. A handshake that does fail
        // comes out of Listener::accept as an Err, the same as
        // "listener is closed" and "accept task died", which repeat
        // on every call, so a caller can't tell whether to keep
        // accepting; rustc-dist stopped taking jobs on the first one.
        // Repro: rustc-dist's e2e test
        // a_client_that_fails_to_connect_holds_up_no_other timed out
        // against this Listener. rustc-dist now runs its own accept
        // loop over singleton_with_flags (rustc-dist src/sandbox.rs
        // `listen`). A fix here: wait for each singleton in a task of
        // its own, with a timeout, send only finished connections, and
        // make accept fail only when the listener is gone.
        // (channel-listener-01)
        // 2026-11-02 claude: Eric agreed the design (2026-11-02).
        // ListenerInner::run now takes connects while up to MAX_HANDSHAKES
        // handshakes run, each in its own singleton task; the session is
        // published before the client is told its path, the client has
        // HANDSHAKE_TIMEOUT (15 s, shared with the client side) to subscribe
        // and each later step its own 15 s; a failed handshake is logged and
        // dropped; Listener::accept fails only once the listener is closed, and
        // then every time. Tests that fail if undone:
        // a_client_that_never_comes_to_its_session_holds_up_no_other and
        // a_failed_handshake_is_not_an_accept_error (channel/test.rs).
        loop {
            let room = handshakes.len() < MAX_HANDSHAKES;
            tokio::select! {
                biased;
                Some(r) = handshakes.next() => match r {
                    Ok(con) => if tx.send(con).await.is_err() {
                        break;
                    },
                    Err(e) => warn!("{}: a client's handshake failed: {e:?}", self.base),
                },
                reply = self.next_connect(), if room => match reply {
                    None => break,
                    Some(reply) => handshakes.push(handshake(
                        self.publisher.clone(),
                        self.flags,
                        self.timeout,
                        session(&self.base),
                        reply,
                    )),
                },
            }
        }
    }
}

/// Publish `session` for one client, tell the client where it is, and wait
/// for the client to connect there.
async fn handshake(
    publisher: Publisher,
    flags: PublishFlags,
    timeout: Option<Duration>,
    session: Path,
    reply: SendResult,
) -> Result<Connection> {
    let mut singleton = start_singleton(
        &publisher,
        flags,
        timeout,
        session.clone(),
        Some(HANDSHAKE_TIMEOUT),
    )
    .await?;
    reply.send(Value::from(session));
    singleton.wait_connected().await
}

pub struct Listener {
    rx: mpsc::Receiver<Connection>,
    jh: JoinHandle<()>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.jh.abort()
    }
}

impl Listener {
    /// just like new, but with publish flags
    pub async fn new_with_flags(
        publisher: &Publisher,
        flags: PublishFlags,
        timeout: Option<Duration>,
        path: Path,
    ) -> Result<Listener> {
        let publisher = publisher.clone();
        let (tx, rx) = mpsc::channel(3);
        let (tx_res, rx_res) = oneshot::channel();
        let jh = task::spawn(async move {
            match ListenerInner::new_with_flags(publisher, flags, timeout, path).await {
                Err(e) => {
                    let _ = tx_res.send(Err(e));
                }
                Ok(inner) => {
                    let _ = tx_res.send(Ok(()));
                    inner.run(tx).await
                }
            }
        });
        rx_res.await??;
        Ok(Self { rx, jh })
    }

    /// Create a new listener at the specified path. The actual
    /// connections will be randomly generated uuids under the
    /// specified path.
    pub async fn new(
        publisher: &Publisher,
        timeout: Option<Duration>,
        path: Path,
    ) -> Result<Listener> {
        Self::new_with_flags(publisher, PublishFlags::empty(), timeout, path).await
    }

    /// Wait for a client to connect, and return a singleton
    /// connection to the new client. This method is cancel safe, it
    /// can be safely used in a select! without risk of losing an
    /// incoming connection. A client whose handshake fails is logged
    /// and never returned. An error means the listener is closed, and
    /// every later call fails too.
    pub async fn accept(&mut self) -> Result<Connection> {
        match self.rx.next().await {
            Some(con) => Ok(con),
            None => bail!("listener is closed"),
        }
    }
}
