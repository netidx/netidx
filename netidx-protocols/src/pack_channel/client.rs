use crate::channel::client;
use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use netidx::{
    pack::Pack,
    path::Path,
    subscriber::{Subscriber, Value},
};
use parking_lot::Mutex;
use std::mem;
use tokio::sync::Mutex as AsyncMutex;

pub struct Batch {
    data: BytesMut,
}

impl Batch {
    /// Queue a packable thing in the batch.
    pub fn queue<S: Pack>(&mut self, t: &S) -> Result<()> {
        Ok(Pack::encode(t, &mut self.data)?)
    }

    /// returns the number of bytes queued in this batch
    pub fn len(&self) -> usize {
	self.data.len()
    }
}

pub struct Connection {
    inner: client::Connection,
    buf: Mutex<BytesMut>,
    queue: AsyncMutex<Bytes>,
}

impl Connection {
    /// Connect to the channel at the the specified path. The channel
    /// may either be a listener, or a singleton.
    pub async fn connect(subscriber: &Subscriber, path: Path) -> Result<Connection> {
        let inner = client::Connection::connect(subscriber, path).await?;
        Ok(Connection {
            inner,
            buf: Mutex::new(BytesMut::new()),
            queue: AsyncMutex::new(Bytes::new()),
        })
    }

    /// return true if the connection is dead
    pub fn is_dead(&self) -> bool {
        self.inner.is_dead()
    }

    /// start a new batch of messages
    pub fn start_batch(&self) -> Batch {
        Batch { data: mem::replace(&mut *self.buf.lock(), BytesMut::new()) }
    }

    /// send a batch of messages
    pub fn send(&self, mut batch: Batch) -> Result<()> {
        let v = Value::from(batch.data.split().freeze());
        self.inner.send(v)?;
        Ok(*self.buf.lock() = batch.data)
    }

    /// send just one message
    pub fn send_one<S: Pack>(&self, t: &S) -> Result<()> {
        let mut b = self.start_batch();
        b.queue(t)?;
        self.send(b)
    }

    /// return true if messages have been sent but not flushed. Flush
    /// is only required for pushback.
    pub fn dirty(&self) -> bool {
        self.inner.dirty()
    }

    /// Wait for sent messages to flush to the OS
    pub async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }

    async fn try_fill_queue(&self, queue: &mut Bytes) -> Result<()> {
        if !queue.has_remaining() {
            match self.inner.try_recv_one().await? {
                Some(Value::Bytes(buf)) => *queue = (*buf).clone(),
                Some(v) => bail!("unexpected response {}", v),
                None => (),
            }
        }
        Ok(())
    }

    async fn fill_queue(&self, queue: &mut Bytes) -> Result<()> {
        if !queue.has_remaining() {
            match self.inner.recv_one().await? {
                Value::Bytes(buf) => *queue = (*buf).clone(),
                v => bail!("unexpected response {}", v),
            }
        }
        Ok(())
    }

    /// Receive all avaliable messages, waiting for at least one
    /// message to arrive before returning. Each message will be
    /// passed to f, if f returns false, then processing the batch
    /// will be halted and any remaining messages will stay in the
    /// channel.
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

    /// Receive one message, waiting for at least one message to
    /// arrive if none are queued.
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
}
