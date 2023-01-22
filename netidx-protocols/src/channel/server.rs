use anyhow::Result;
use arcstr::ArcStr;
use futures::{channel::mpsc, prelude::*};
use netidx::{
    path::Path,
    pool::Pooled,
    publisher::{ClId, PublishFlags, Publisher, UpdateBatch, Val, Value, WriteRequest},
};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::Mutex, time};

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
}

struct Receiver {
    writes: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
    queued: VecDeque<Value>,
}

impl Receiver {
    async fn fill_queue(&mut self, dead: &AtomicBool, client: ClId) -> Result<()> {
        while self.queued.len() == 0 {
            match self.writes.next().await {
                Some(mut batch) => {
                    self.queued.extend(batch.drain(..).filter_map(|req| {
                        if req.client == client {
                            Some(req.value)
                        } else {
                            None
                        }
                    }))
                }
                None => {
                    dead.store(true, Ordering::Relaxed);
                    bail!("connection is dead")
                }
            }
        }
        Ok(())
    }
}

/// This is a single pending connection. You must call wait_connected
/// to finish the handshake.
pub struct Singleton {
    publisher: Publisher,
    anchor: Arc<Val>,
    timeout: Option<Duration>,
    writes: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
}

/// Create a new single connection at path. One client can connect to
/// this connection, further connection attempts will be ignored. This
/// is useful for example for returning a channel from an rpc call, in
/// that case there is no need for a listener.
pub async fn singleton(
    publisher: &Publisher,
    timeout: Option<Duration>,
    path: Path,
) -> Result<Singleton> {
    let val = publisher.publish_with_flags(
        PublishFlags::ISOLATED,
        path.clone(),
        Value::from("connection"),
    )?;
    let (tx, rx) = mpsc::channel(5);
    publisher.writes(val.id(), tx);
    publisher.flushed().await;
    Ok(Singleton {
        publisher: publisher.clone(),
        timeout,
        anchor: Arc::new(val),
        writes: rx,
    })
}

impl Singleton {
    /// Wait for the client to connect and return the connection when
    /// then have done so.
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
        for _ in 1..3 {
            let to = Duration::from_secs(3);
            match time::timeout(to, con.recv_one()).await?? {
                Value::String(s) if &*s == "ready" => {
                    con.send_one(Value::from("ready")).await?
                }
                Value::String(s) if &*s == "go" => return Ok(con),
                _ => (),
            }
        }
        bail!("protocol negotiation failed")
    }
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

    /// Wait for one message from the other side, and return it when
    /// it arrives.
    pub async fn recv_one(&self) -> Result<Value> {
        let mut recv = self.receiver.lock().await;
        loop {
            match recv.queued.pop_front() {
                Some(v) => break Ok(v),
                None => {
                    if self.is_dead() {
                        bail!("connection is dead")
                    }
                    recv.fill_queue(&self.dead, self.client).await?
                }
            }
        }
    }

    /// Receive a batch of messages from the other side and place them
    /// in the specified data structure.
    pub async fn recv(&self, dst: &mut impl Extend<Value>) -> Result<()> {
        let mut recv = self.receiver.lock().await;
        loop {
            if recv.queued.len() > 0 {
                break Ok(dst.extend(recv.queued.drain(..)));
            } else {
                if self.is_dead() {
                    bail!("connection is dead")
                }
                recv.fill_queue(&self.dead, self.client).await?
            }
        }
    }

    /// Return the user connected to this channel, if known
    pub fn user(&self) -> Option<ArcStr> {
        self.publisher.user(&self.client)
    }
}

/// A listener can accept connections from muliple clients and produce
/// a channel to talk to each one.
pub struct Listener {
    publisher: Publisher,
    _listener: Val,
    waiting: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
    queued: Pooled<Vec<WriteRequest>>,
    base: Path,
    timeout: Option<Duration>,
}

impl Listener {
    /// Create a new listener at the specified path. The actual
    /// connections will be randomly generated uuids under the
    /// specified path.
    pub async fn new(
        publisher: &Publisher,
        timeout: Option<Duration>,
        path: Path,
    ) -> Result<Listener> {
        let publisher = publisher.clone();
        let listener = publisher.publish(path.clone(), Value::from("channel"))?;
        let (tx_waiting, rx_waiting) = mpsc::channel(50);
        publisher.writes(listener.id(), tx_waiting);
        publisher.flushed().await;
        Ok(Self {
            publisher,
            _listener: listener,
            waiting: rx_waiting,
            queued: Pooled::orphan(Vec::new()),
            base: path,
            timeout,
        })
    }

    /// Wait for a client to connect, and return a singleton
    /// connection to the new client.
    pub async fn accept(&mut self) -> Result<Singleton> {
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
        Ok(singleton(&self.publisher, self.timeout, session).await?)
    }
}
