use anyhow::Result;
use futures::{channel::mpsc, prelude::*};
use netidx::{
    path::Path,
    protocol::resolver::UserInfo,
    publisher::{ClId, PublishFlags, Publisher, UpdateBatch, Val, Value, WriteRequest},
};
use poolshark::global::GPooled;
use std::{
    collections::VecDeque,
    result,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{oneshot, Mutex},
    task::{self, JoinHandle},
    time,
};

/// Generate a random session name ${base}/uuid
pub fn session(base: &Path) -> Path {
    use uuid::{fmt::Simple, Uuid};
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
                if req.client == client {
                    Some(req.value)
                } else {
                    None
                }
            })),
            None => {
                dead.store(true, Ordering::Relaxed);
                bail!("connection is dead")
            }
        }
        Ok(())
    }

    async fn fill_queue(&mut self, dead: &AtomicBool, client: ClId) -> Result<()> {
        self.try_fill_queue(dead, client)?;
        while self.queued.len() == 0 {
            let r = self.writes.next().await;
            self.fill_from_channel(dead, client, r)?
        }
        Ok(())
    }

    fn try_fill_queue(&mut self, dead: &AtomicBool, client: ClId) -> Result<()> {
        for _ in 0..10 {
            match self.writes.try_next() {
                Err(_) => break,
                Ok(r) => {
                    if let Err(e) = self.fill_from_channel(dead, client, r) {
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
    writes: mpsc::Receiver<GPooled<Vec<WriteRequest>>>,
}

impl SingletonInner {
    pub async fn wait_connected(self) -> Result<Connection> {
        let mut subscribed = loop {
            self.publisher.wait_client(self.anchor.id()).await;
            let subs = self.publisher.subscribed(&self.anchor.id());
            if subs.len() > 0 {
                break subs;
            }
        };
        let con = Connection {
            publisher: self.publisher,
            anchor: self.anchor,
            client: subscribed.pop().unwrap(),
            dead: AtomicBool::new(false),
            timeout: self.timeout,
            receiver: Mutex::new(Receiver {
                writes: self.writes,
                queued: VecDeque::new(),
            }),
        };
        let to = Duration::from_secs(15);
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
        let val = match res {
            Ok(val) => {
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
            anchor: Arc::new(val),
            writes: rx,
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

    /// Wait for one message from the other side, and return it when
    /// it arrives.
    pub async fn recv_one(&self) -> Result<Value> {
        let mut recv = self.receiver.lock().await;
        loop {
            match recv.queued.pop_front() {
                Some(v) => break Ok(v),
                None => {
                    self.check_dead()?;
                    recv.fill_queue(&self.dead, self.client).await?
                }
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
        recv.try_fill_queue(&self.dead, self.client)?;
        loop {
            if recv.queued.len() > 0 {
                break Ok(dst.extend(recv.queued.drain(..)));
            } else {
                self.check_dead()?;
                recv.fill_queue(&self.dead, self.client).await?
            }
        }
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

    /// Wait for a client to connect, and return a singleton
    /// connection to the new client.
    async fn accept(&mut self) -> Result<Singleton> {
        let send_result = loop {
            if let Some(req) = self.queued.pop() {
                match &req.value {
                    Value::String(c) if &**c == "connect" => {
                        if let Some(send_result) = req.send_result {
                            break send_result;
                        }
                    }
                    _ => (),
                }
            } else {
                if let Some(batch) = self.waiting.next().await {
                    self.queued = batch;
                } else {
                    bail!("listener is closed")
                }
            }
        };
        let session = session(&self.base);
        send_result.send(Value::from(session.clone()));
        Ok(singleton_with_flags(&self.publisher, self.flags, self.timeout, session)
            .await?)
    }
}

pub struct Listener {
    rx: mpsc::Receiver<Result<Connection>>,
    jh: JoinHandle<result::Result<(), mpsc::SendError>>,
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
        let (mut tx, rx) = mpsc::channel(3);
        let (tx_res, rx_res) = oneshot::channel();
        let jh = task::spawn(async move {
            match ListenerInner::new_with_flags(publisher, flags, timeout, path).await {
                Err(e) => {
                    let _ = tx_res.send(Err(e));
                }
                Ok(mut inner) => {
                    let _ = tx_res.send(Ok(()));
                    loop {
                        match inner.accept().await {
                            Ok(mut s) => match s.wait_connected().await {
                                Err(e) => tx.send(Err(e)).await?,
                                Ok(con) => tx.send(Ok(con)).await?,
                            },
                            Err(e) => tx.send(Err(e)).await?,
                        }
                    }
                }
            }
            Ok::<_, mpsc::SendError>(())
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
    /// incoming connection.
    pub async fn accept(&mut self) -> Result<Connection> {
        match self.rx.next().await {
            Some(r) => r,
            None => bail!("accept task died"),
        }
    }
}
