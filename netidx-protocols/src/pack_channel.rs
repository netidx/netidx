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

#[cfg(test)]
mod test {
    use super::*;
    use netidx::{
        config::Config as ClientConfig,
        path::Path,
        publisher::Publisher,
        resolver_client::DesiredAuth,
        resolver_server::{config::Config as ServerConfig, Server},
        subscriber::Subscriber,
    };
    use tokio::{runtime::Runtime, task};

    #[test]
    fn pack_ping_pong() {
        Runtime::new().unwrap().block_on(async move {
            let cfg = ServerConfig::load("../cfg/simple-server.json")
                .expect("load simple server config");
            let server =
                Server::new(cfg.clone(), false, 0).await.expect("start resolver server");
            let mut cfg = ClientConfig::load("../cfg/simple-client.json")
                .expect("load simple client config");
            cfg.addrs[0].0 = *server.local_addr();
            let publisher = Publisher::new(
                cfg.clone(),
                DesiredAuth::Anonymous,
                "127.0.0.1/32".parse().unwrap(),
            )
            .await
            .unwrap();
            let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous).unwrap();
            let base = Path::from("/channel");
            let mut listener =
                server::Listener::<u64>::new(&publisher, 50, None, base.clone())
                    .await
                    .unwrap();
            task::spawn(async move {
                let con = client::Connection::<u64>::connect(&subscriber, 50, None, base)
                    .await
                    .unwrap();
                for i in 0..100u64 {
                    con.send_one(&i).unwrap();
                    let j = con.recv_one().await.unwrap();
                    assert_eq!(j, i)
                }
            });
            let con = listener.accept().await.unwrap().await.unwrap();
            for _ in 0..100 {
                let i = con.recv_one().await.unwrap();
                con.send_one(&i).await.unwrap();
            }
        })
    }

    #[test]
    fn pack_batch_ping_pong() {
        Runtime::new().unwrap().block_on(async move {
            let cfg = ServerConfig::load("../cfg/simple-server.json")
                .expect("load simple server config");
            let server =
                Server::new(cfg.clone(), false, 0).await.expect("start resolver server");
            let mut cfg = ClientConfig::load("../cfg/simple-client.json")
                .expect("load simple client config");
            cfg.addrs[0].0 = *server.local_addr();
            let publisher = Publisher::new(
                cfg.clone(),
                DesiredAuth::Anonymous,
                "127.0.0.1/32".parse().unwrap(),
            )
            .await
            .unwrap();
            let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous).unwrap();
            let base = Path::from("/channel");
            let mut listener =
                server::Listener::<u64>::new(&publisher, 50, None, base.clone())
                    .await
                    .unwrap();
            task::spawn(async move {
                let con = client::Connection::<u64>::connect(&subscriber, 50, None, base)
                    .await
                    .unwrap();
                for _ in 0..100 {
                    let mut b = con.start_batch();
                    for i in 0..100u64 {
                        b.queue(&i).unwrap()
                    }
                    con.send(b).unwrap();
                    let mut v = Vec::new();
                    con.recv(|i| v.push(i)).await.unwrap();
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
                let mut v = Vec::new();
                con.recv(|i| v.push(i)).await.unwrap();
                let mut b = con.start_batch();
                for i in v {
                    b.queue(&i).unwrap();
                }
                con.send(b).await.unwrap();
            }
        })
    }
}
