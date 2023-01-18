// not Packled channel :P

pub mod server {
    use crate::channel::server;
    use anyhow::Result;
    use bytes::{Buf, Bytes, BytesMut};
    use futures::prelude::*;
    use netidx::{
        pack::Pack,
        path::Path,
        publisher::{Publisher, Value},
    };
    use parking_lot::Mutex;
    use std::{marker::PhantomData, mem, time::Duration};
    use tokio::sync::Mutex as AsyncMutex;

    pub struct Batch<T: Pack + 'static> {
        data: BytesMut,
        phantom: PhantomData<T>,
    }

    impl<T: Pack + 'static> Batch<T> {
        pub fn queue(&mut self, t: &T) -> Result<()> {
            Ok(Pack::encode(t, &mut self.data)?)
        }
    }

    pub struct Connection<T: Pack + 'static> {
        inner: server::Connection,
        phantom: PhantomData<T>,
        buf: Mutex<BytesMut>,
        queue: AsyncMutex<Bytes>,
    }

    impl<T: Pack + 'static> Connection<T> {
        pub fn is_dead(&self) -> bool {
            self.inner.is_dead()
        }

        pub fn start_batch(&self) -> Batch<T> {
            Batch {
                data: mem::replace(&mut *self.buf.lock(), BytesMut::new()),
                phantom: PhantomData,
            }
        }

        pub async fn send(&self, mut batch: Batch<T>) -> Result<()> {
            let v = Value::Bytes(batch.data.split().freeze());
            self.inner.send_one(v).await?;
            *self.buf.lock() = batch.data;
            Ok(())
        }

        pub async fn send_one(&self, t: &T) -> Result<()> {
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

        pub async fn recv<F: FnMut(T)>(&self, mut f: F) -> Result<()> {
            let mut queue = self.queue.lock().await;
            self.fill_queue(&mut *queue).await?;
            while queue.has_remaining() {
                f(<T as Pack>::decode(&mut *queue)?);
            }
            Ok(())
        }

        pub async fn recv_one(&self) -> Result<T> {
            let mut queue = self.queue.lock().await;
            self.fill_queue(&mut *queue).await?;
            Ok(<T as Pack>::decode(&mut *queue)?)
        }
    }

    pub struct Listener<T: Pack + 'static> {
        inner: server::Listener,
        phantom: PhantomData<T>,
    }

    impl<T: Pack + 'static> Listener<T> {
        pub async fn new(
            publisher: &Publisher,
            queue_depth: usize,
            timeout: Option<Duration>,
            path: Path,
        ) -> Result<Self> {
            let inner =
                server::Listener::new(publisher, queue_depth, timeout, path).await?;
            Ok(Self { inner, phantom: PhantomData })
        }

        pub async fn accept(
            &mut self,
        ) -> Result<impl Future<Output = Result<Connection<T>>>> {
            let inner = self.inner.accept().await?;
            Ok(inner.map(|c| {
                let inner = c?;
                Ok(Connection {
                    inner,
                    phantom: PhantomData,
                    buf: Mutex::new(BytesMut::new()),
                    queue: AsyncMutex::new(Bytes::new()),
                })
            }))
        }
    }
}

pub mod client {
    use crate::channel::client;
    use anyhow::Result;
    use bytes::{Buf, Bytes, BytesMut};
    use netidx::{
        pack::Pack,
        path::Path,
        subscriber::{Subscriber, Value},
    };
    use parking_lot::Mutex;
    use std::{marker::PhantomData, mem, time::Duration};
    use tokio::sync::Mutex as AsyncMutex;

    pub struct Batch<T: Pack + 'static> {
        data: BytesMut,
        phantom: PhantomData<T>,
    }

    impl<T: Pack + 'static> Batch<T> {
        pub fn queue(&mut self, t: &T) -> Result<()> {
            Ok(Pack::encode(t, &mut self.data)?)
        }
    }

    pub struct Connection<T: Pack + 'static> {
        inner: client::Connection,
        phantom: PhantomData<T>,
        buf: Mutex<BytesMut>,
        queue: AsyncMutex<Bytes>,
    }

    impl<T: Pack + 'static> Connection<T> {
        pub async fn connect(
            subscriber: &Subscriber,
            queue_depth: usize,
            timeout: Option<Duration>,
            path: Path,
        ) -> Result<Connection<T>> {
            let inner =
                client::Connection::connect(subscriber, queue_depth, timeout, path)
                    .await?;
            Ok(Connection {
                inner,
                phantom: PhantomData,
                buf: Mutex::new(BytesMut::new()),
                queue: AsyncMutex::new(Bytes::new()),
            })
        }

        pub fn is_dead(&self) -> bool {
            self.inner.is_dead()
        }

        pub fn start_batch(&self) -> Batch<T> {
            Batch {
                data: mem::replace(&mut *self.buf.lock(), BytesMut::new()),
                phantom: PhantomData,
            }
        }

        pub fn send(&self, mut batch: Batch<T>) -> Result<()> {
            let v = Value::Bytes(batch.data.split().freeze());
            self.inner.send_one(v)?;
            Ok(*self.buf.lock() = batch.data)
        }

        pub fn send_one(&self, t: &T) -> Result<()> {
            let mut b = self.start_batch();
            b.queue(t)?;
            self.send(b)
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

        pub async fn recv<F: FnMut(T)>(&self, mut f: F) -> Result<()> {
            let mut queue = self.queue.lock().await;
            self.fill_queue(&mut *queue).await?;
            while queue.has_remaining() {
                f(<T as Pack>::decode(&mut *queue)?)
            }
            Ok(())
        }

        pub async fn recv_one(&self) -> Result<T> {
            let mut queue = self.queue.lock().await;
            self.fill_queue(&mut *queue).await?;
            Ok(<T as Pack>::decode(&mut *queue)?)
        }
    }
}
