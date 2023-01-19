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

    pub struct Listener(server::Listener);

    impl Listener {
        pub async fn new(
            publisher: &Publisher,
            queue_depth: usize,
            timeout: Option<Duration>,
            path: Path,
        ) -> Result<Self> {
            let inner =
                server::Listener::new(publisher, queue_depth, timeout, path).await?;
            Ok(Self(inner))
        }

        pub async fn accept(
            &mut self,
        ) -> Result<impl Future<Output = Result<Connection>>> {
            let inner = self.0.accept().await?;
            Ok(inner.map(|c| {
                let inner = c?;
                Ok(Connection {
                    inner,
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
        pub async fn connect(
            subscriber: &Subscriber,
            queue_depth: usize,
            path: Path,
        ) -> Result<Connection> {
            let inner =
                client::Connection::connect(subscriber, queue_depth, path).await?;
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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::channel::test::Ctx;
    use tokio::{runtime::Runtime, task};

    #[test]
    fn pack_ping_pong() {
        Runtime::new().unwrap().block_on(async move {
            let ctx = Ctx::new().await;
            let mut listener =
                server::Listener::new(&ctx.publisher, 50, None, ctx.base.clone())
                    .await
                    .unwrap();
            task::spawn(async move {
                let con = client::Connection::connect(&ctx.subscriber, 50, ctx.base)
                    .await
                    .unwrap();
                for i in 0..100u64 {
                    con.send_one(&i).unwrap();
                    let j: u64 = con.recv_one().await.unwrap();
                    assert_eq!(j, i)
                }
            });
            let con = listener.accept().await.unwrap().await.unwrap();
            for _ in 0..100 {
                let i: u64 = con.recv_one().await.unwrap();
                con.send_one(&i).await.unwrap();
            }
        })
    }

    #[test]
    fn pack_batch_ping_pong() {
        Runtime::new().unwrap().block_on(async move {
            let ctx = Ctx::new().await;
            let mut listener =
                server::Listener::new(&ctx.publisher, 50, None, ctx.base.clone())
                    .await
                    .unwrap();
            task::spawn(async move {
                let con = client::Connection::connect(&ctx.subscriber, 50, ctx.base)
                    .await
                    .unwrap();
                for _ in 0..100 {
                    let mut b = con.start_batch();
                    for i in 0..100u64 {
                        b.queue(&i).unwrap()
                    }
                    con.send(b).unwrap();
                    let mut v: Vec<u64> = Vec::new();
                    con.recv(|i| {
                        v.push(i);
                        true
                    })
                    .await
                    .unwrap();
                    let mut i = 0;
                    for j in v {
                        assert_eq!(j, i);
                        i += 1;
                    }
                    assert_eq!(i, 100)
                }
            });
            let con = listener.accept().await.unwrap().await.unwrap();
            for _ in 0..100 {
                let mut v: Vec<u64> = Vec::new();
                con.recv(|i| {
                    v.push(i);
                    true
                })
                .await
                .unwrap();
                let mut b = con.start_batch();
                for i in v {
                    b.queue(&i).unwrap();
                }
                con.send(b).await.unwrap();
            }
        })
    }
}
