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
    pub fn queue<S: Pack>(&mut self, t: &S) -> Result<()> {
        Ok(Pack::encode(t, &mut self.data)?)
    }
}

pub struct Connection {
    inner: client::Connection,
    buf: Mutex<BytesMut>,
    queue: AsyncMutex<Bytes>,
}

impl Connection {
    pub async fn connect(subscriber: &Subscriber, path: Path) -> Result<Connection> {
        let inner = client::Connection::connect(subscriber, path).await?;
        Ok(Connection {
            inner,
            buf: Mutex::new(BytesMut::new()),
            queue: AsyncMutex::new(Bytes::new()),
        })
    }

    pub fn is_dead(&self) -> bool {
        self.inner.is_dead()
    }

    pub fn start_batch(&self) -> Batch {
        Batch { data: mem::replace(&mut *self.buf.lock(), BytesMut::new()) }
    }

    pub fn send(&self, mut batch: Batch) -> Result<()> {
        let v = Value::Bytes(batch.data.split().freeze());
        self.inner.send(v)?;
        Ok(*self.buf.lock() = batch.data)
    }

    pub fn send_one<S: Pack>(&self, t: &S) -> Result<()> {
        let mut b = self.start_batch();
        b.queue(t)?;
        self.send(b)
    }

    pub fn dirty(&self) -> bool {
        self.inner.dirty()
    }

    pub async fn flush(&self) -> Result<()> {
        self.inner.flush().await
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

    pub async fn recv<R: Pack + 'static, F: FnMut(R) -> bool>(
        &self,
        mut f: F,
    ) -> Result<()> {
        let mut queue = self.queue.lock().await;
        self.fill_queue(&mut *queue).await?;
        while queue.has_remaining() && f(<R as Pack>::decode(&mut *queue)?) {}
        Ok(())
    }

    pub async fn recv_one<R: Pack + 'static>(&self) -> Result<R> {
        let mut queue = self.queue.lock().await;
        self.fill_queue(&mut *queue).await?;
        Ok(<R as Pack>::decode(&mut *queue)?)
    }
}
