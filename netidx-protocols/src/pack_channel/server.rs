use crate::channel::server;
use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use netidx::{
    pack::Pack,
    path::Path,
    publisher::{Publisher, Value},
};
use parking_lot::Mutex;
use std::{mem, time::Duration};
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
    inner: server::Connection,
    buf: Mutex<BytesMut>,
    queue: AsyncMutex<Bytes>,
}

impl Connection {
    pub fn is_dead(&self) -> bool {
        self.inner.is_dead()
    }

    pub fn start_batch(&self) -> Batch {
        Batch { data: mem::replace(&mut *self.buf.lock(), BytesMut::new()) }
    }

    pub async fn send(&self, mut batch: Batch) -> Result<()> {
        let v = Value::Bytes(batch.data.split().freeze());
        self.inner.send_one(v).await?;
        *self.buf.lock() = batch.data;
        Ok(())
    }

    pub async fn send_one<S: Pack>(&self, t: &S) -> Result<()> {
        let mut b = self.start_batch();
        b.queue(t)?;
        self.send(b).await
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

pub struct Singleton(server::Singleton);

impl Singleton {
    pub async fn wait_connected(self) -> Result<Connection> {
        let inner = self.0.wait_connected().await?;
        Ok(Connection {
            inner,
            buf: Mutex::new(BytesMut::new()),
            queue: AsyncMutex::new(Bytes::new()),
        })
    }
}

pub async fn singleton(
    publisher: &Publisher,
    timeout: Option<Duration>,
    path: Path,
) -> Result<Singleton> {
    Ok(Singleton(server::singleton(publisher, timeout, path).await?))
}

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

    pub async fn accept(&mut self) -> Result<Singleton> {
        Ok(Singleton(self.0.accept().await?))
    }
}
