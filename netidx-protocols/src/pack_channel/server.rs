use crate::channel::server;
use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use netidx::{
    pack::Pack,
    path::Path,
    protocol::resolver::UserInfo,
    publisher::{PublishFlags, Publisher, Value},
};
use parking_lot::Mutex;
use std::{mem, time::Duration};
use tokio::sync::Mutex as AsyncMutex;

pub struct Batch {
    data: BytesMut,
}

impl Batch {
    /// Queue a packable thing in the batch.
    pub fn queue<S: Pack>(&mut self, t: &S) -> Result<()> {
        Ok(Pack::encode(t, &mut self.data)?)
    }

    /// Returns the number of bytes queued in this batch
    pub fn len(&self) -> usize {
	self.data.len()
    }
}

/// A bidirectional channel between two end points.
pub struct Connection {
    inner: server::Connection,
    buf: Mutex<BytesMut>,
    queue: AsyncMutex<Bytes>,
}

impl Connection {
    /// Return true if the connection is dead.
    pub fn is_dead(&self) -> bool {
        self.inner.is_dead()
    }

    /// Start a new batch of messages to be sent to the other side
    pub fn start_batch(&self) -> Batch {
        Batch { data: mem::replace(&mut *self.buf.lock(), BytesMut::new()) }
    }

    /// Send a batch of messages to the other side.
    pub async fn send(&self, mut batch: Batch) -> Result<()> {
        let v = Value::Bytes(batch.data.split().freeze());
        self.inner.send_one(v).await?;
        *self.buf.lock() = batch.data;
        Ok(())
    }

    /// Send one message to the other side
    pub async fn send_one<S: Pack>(&self, t: &S) -> Result<()> {
        let mut b = self.start_batch();
        b.queue(t)?;
        self.send(b).await
    }

    async fn try_fill_queue(&self, queue: &mut Bytes) -> Result<()> {
        if !queue.has_remaining() {
            match self.inner.try_recv_one().await? {
                Some(Value::Bytes(buf)) => *queue = buf,
                Some(_) => bail!("unexpected response"),
                None => (),
            }
        }
        Ok(())
    }

    async fn fill_queue(&self, queue: &mut Bytes) -> Result<()> {
        if !queue.has_remaining() {
            match self.inner.recv_one().await? {
                Value::Bytes(buf) => *queue = buf,
                _ => bail!("unexpected response"),
            }
        }
        Ok(())
    }

    /// Receive a batch of messages from the other side. Recv will
    /// repeatedly call the specified closure with new messages until
    /// either,
    ///
    /// - the closure returns false
    /// - there are no more messages
    ///
    pub async fn recv<R: Pack + 'static, F: FnMut(R) -> bool>(
        &self,
        mut f: F,
    ) -> Result<()> {
        let mut queue = self.queue.lock().await;
        self.fill_queue(&mut *queue).await?;
        loop {
            if queue.has_remaining() {
                if !f(Pack::decode(&mut *queue)?) {
                    break;
                }
            } else {
                self.try_fill_queue(&mut *queue).await?;
                if !queue.has_remaining() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Receive all available messages, but do not wait for at least 1
    /// message to arrive. If no messages are available return
    /// immediatly. This will only block if a concurrent receive is in
    /// progress.
    pub async fn try_recv<R: Pack + 'static, F: FnMut(R) -> bool>(
        &self,
        mut f: F,
    ) -> Result<()> {
        let mut queue = self.queue.lock().await;
        self.try_fill_queue(&mut *queue).await?;
        loop {
            if queue.has_remaining() {
                if !f(Pack::decode(&mut *queue)?) {
                    break;
                }
            } else {
                self.try_fill_queue(&mut *queue).await?;
                if !queue.has_remaining() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Wait for one message from the other side, and return it when
    /// it is available.
    pub async fn recv_one<R: Pack + 'static>(&self) -> Result<R> {
        let mut queue = self.queue.lock().await;
        self.fill_queue(&mut *queue).await?;
        Ok(<R as Pack>::decode(&mut *queue)?)
    }

    /// Receive a message if one is available, otherwise return
    /// Ok(None).
    pub async fn try_recv_one<R: Pack + 'static>(&self) -> Result<Option<R>> {
        let mut queue = self.queue.lock().await;
        self.try_fill_queue(&mut *queue).await?;
        if queue.remaining() > 0 {
            Ok(Some(Pack::decode(&mut *queue)?))
        } else {
            Ok(None)
        }
    }

    /// Return the user connected to this channel, if known
    pub fn user(&self) -> Option<UserInfo> {
        self.inner.user()
    }
}

/// A single pending connection. You must call wait_connected to
/// finish the handshake.
pub struct Singleton(server::Singleton);

impl Singleton {
    /// Wait for the client to connect and complete the handshake
    pub async fn wait_connected(&mut self) -> Result<Connection> {
        let inner = self.0.wait_connected().await?;
        Ok(Connection {
            inner,
            buf: Mutex::new(BytesMut::new()),
            queue: AsyncMutex::new(Bytes::new()),
        })
    }
}

/// Create a single waiting connection at the specified path. This
/// will allow one client to connect and form a bidirectional channel.
pub async fn singleton(
    publisher: &Publisher,
    timeout: Option<Duration>,
    path: Path,
) -> Result<Singleton> {
    Ok(Singleton(server::singleton(publisher, timeout, path).await?))
}

pub async fn singleton_with_flags(
    publisher: &Publisher,
    flags: PublishFlags,
    timeout: Option<Duration>,
    path: Path,
) -> Result<Singleton> {
    Ok(Singleton(server::singleton_with_flags(publisher, flags, timeout, path).await?))
}

/// A listener can accept connections from muliple clients and produce
/// a channel to talk to each one.
pub struct Listener(server::Listener);

impl Listener {
    pub async fn new(
        publisher: &Publisher,
        timeout: Option<Duration>,
        path: Path,
    ) -> Result<Self> {
        let inner = server::Listener::new(publisher, timeout, path).await?;
        Ok(Self(inner))
    }

    pub async fn new_with_flags(
        publisher: &Publisher,
        flags: PublishFlags,
        timeout: Option<Duration>,
        path: Path,
    ) -> Result<Self> {
        let inner =
            server::Listener::new_with_flags(publisher, flags, timeout, path).await?;
        Ok(Self(inner))
    }

    /// Wait for a client to connect, and return a singleton
    /// connection to the new client.
    pub async fn accept(&mut self) -> Result<Connection> {
        let inner = self.0.accept().await?;
        Ok(Connection {
            inner,
            buf: Mutex::new(BytesMut::new()),
            queue: AsyncMutex::new(Bytes::new()),
        })
    }
}
