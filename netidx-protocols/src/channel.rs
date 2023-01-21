pub mod server {
    use anyhow::Result;
    use futures::{channel::mpsc, prelude::*};
    use netidx::{
        path::Path,
        pool::Pooled,
        publisher::{
            ClId, PublishFlags, Publisher, UpdateBatch, Val, Value, WriteRequest,
        },
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

    fn session(base: &Path) -> Path {
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

    pub struct Singleton {
        publisher: Publisher,
        anchor: Arc<Val>,
        timeout: Option<Duration>,
        writes: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
    }

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

    pub struct Connection {
        publisher: Publisher,
        anchor: Arc<Val>,
        client: ClId,
        dead: AtomicBool,
        timeout: Option<Duration>,
        receiver: Mutex<Receiver>,
    }

    impl Connection {
        pub fn start_batch(&self) -> Batch {
            Batch {
                anchor: self.anchor.clone(),
                client: self.client,
                queued: self.publisher.start_batch(),
            }
        }

        pub fn is_dead(&self) -> bool {
            if !self.publisher.is_subscribed(&self.anchor.id(), &self.client) {
                self.dead.store(true, Ordering::Relaxed);
            }
            self.dead.load(Ordering::Relaxed)
        }

        pub async fn send(&self, batch: Batch) -> Result<()> {
            if self.is_dead() {
                bail!("connection is dead")
            }
            Ok(batch.queued.commit(self.timeout).await)
        }

        pub async fn send_one(&self, v: Value) -> Result<()> {
            if self.is_dead() {
                bail!("connection is dead")
            }
            let mut batch = self.publisher.start_batch();
            self.anchor.update_subscriber(&mut batch, self.client, v);
            Ok(batch.commit(self.timeout).await)
        }

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
    }

    pub struct Listener {
        publisher: Publisher,
        _listener: Val,
        waiting: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
        queued: Pooled<Vec<WriteRequest>>,
        base: Path,
        timeout: Option<Duration>,
    }

    impl Listener {
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
}

pub mod client {
    use anyhow::{anyhow, Result};
    use futures::{channel::mpsc, prelude::*};
    use netidx::{
        path::Path,
        pool::{Pool, Pooled},
        subscriber::{Event, SubId, Subscriber, UpdatesFlags, Val, Value},
    };
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };
    use tokio::{sync::Mutex, time};

    lazy_static! {
        static ref BATCHES: Pool<Vec<Value>> = Pool::new(1000, 100_000);
    }

    struct Receiver {
        updates: mpsc::Receiver<Pooled<Vec<(SubId, Event)>>>,
        queued: VecDeque<Value>,
    }

    impl Receiver {
        async fn fill_queue(&mut self, dead: &AtomicBool) -> Result<()> {
            if self.queued.len() == 0 {
                match self.updates.next().await {
                    None => {
                        dead.store(true, Ordering::Relaxed);
                        bail!("connection is dead")
                    }
                    Some(mut batch) => {
                        for (_, ev) in batch.drain(..) {
                            match ev {
                                Event::Update(v) => self.queued.push_back(v),
                                Event::Unsubscribed => {
                                    dead.store(true, Ordering::Relaxed)
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        }
    }

    pub struct Connection {
        _subscriber: Subscriber,
        con: Val,
        receiver: Mutex<Receiver>,
        dead: AtomicBool,
        dirty: AtomicBool,
    }

    impl Connection {
        async fn connect_singleton(subscriber: &Subscriber, path: Path) -> Result<Self> {
            let to = Duration::from_secs(3);
            let (tx, rx) = mpsc::channel(5);
            let mut n = 0;
            let con = loop {
                if n > 7 {
                    break subscriber.subscribe_one(path.clone(), Some(to)).await?;
                } else {
                    match subscriber.subscribe_one(path.clone(), Some(to)).await {
                        Ok(con) => break con,
                        Err(_) => {
                            n += 1;
                            time::sleep(Duration::from_millis(250)).await;
                            continue;
                        }
                    }
                }
            };
            con.updates(UpdatesFlags::empty(), tx);
            let con = Connection {
                _subscriber: subscriber.clone(),
                con,
                dead: AtomicBool::new(false),
                dirty: AtomicBool::new(false),
                receiver: Mutex::new(Receiver { updates: rx, queued: VecDeque::new() }),
            };
            let to = Duration::from_millis(100);
            loop {
                con.send(Value::from("ready"))?;
                match time::timeout(to, con.recv_one()).await {
                    Err(_) => (),
                    Ok(Err(e)) => return Err(e),
                    Ok(Ok(Value::String(s))) if &*s == "ready" => {
                        con.send(Value::from("go"))?;
                        break;
                    }
                    _ => (),
                }
            }
            Ok(con)
        }

        pub async fn connect(subscriber: &Subscriber, path: Path) -> Result<Self> {
            let to = Duration::from_secs(3);
            let acceptor = subscriber.durable_subscribe(path.clone());
            time::timeout(to, acceptor.wait_subscribed()).await??;
            match acceptor.last() {
                Event::Unsubscribed => bail!("connect failed"),
                Event::Update(Value::String(s)) if &*s == "connection" => {
                    Self::connect_singleton(subscriber, path).await
                }
                Event::Update(Value::String(s)) if &*s == "channel" => {
                    let f = acceptor.write_with_recipt(Value::from("connect"));
                    match time::timeout(to, f).await? {
                        Err(_) => bail!("connect failed"),
                        Ok(v @ Value::String(_)) => {
                            let path = v.cast_to::<Path>()?;
                            Self::connect_singleton(subscriber, path).await
                        }
                        Ok(_) => bail!("unexpected response from publisher"),
                    }
                }
                Event::Update(_) => bail!("not a channel or connection"),
            }
        }

        pub fn is_dead(&self) -> bool {
            self.dead.load(Ordering::Relaxed)
        }

        fn check_dead(&self) -> Result<()> {
            Ok(if self.is_dead() {
                bail!("connection is dead")
            })
        }

        pub fn send(&self, v: Value) -> Result<()> {
            self.check_dead()?;
            self.dirty.store(true, Ordering::Relaxed);
            Ok(self.con.write(v))
        }

        pub fn dirty(&self) -> bool {
            self.dirty.load(Ordering::Relaxed)
        }

        pub async fn flush(&self) -> Result<()> {
            self.check_dead()?;
            let r = self.con.flush().await.map_err(|_| {
                self.dead.store(true, Ordering::Relaxed);
                anyhow!("connection is dead")
            });
            self.dirty.store(false, Ordering::Relaxed);
            r
        }

        pub async fn recv_one(&self) -> Result<Value> {
            let mut recv = self.receiver.lock().await;
            loop {
                match recv.queued.pop_front() {
                    Some(v) => break Ok(v),
                    None => {
                        self.check_dead()?;
                        recv.fill_queue(&self.dead).await?
                    }
                }
            }
        }

        pub async fn recv(&self, dst: &mut impl Extend<Value>) -> Result<()> {
            let mut recv = self.receiver.lock().await;
            loop {
                if recv.queued.len() > 0 {
                    break Ok(dst.extend(recv.queued.drain(..)));
                } else {
                    self.check_dead()?;
                    recv.fill_queue(&self.dead).await?
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use netidx::{
        config::Config as ClientConfig,
        path::Path,
        publisher::Publisher,
        resolver_client::DesiredAuth,
        resolver_server::{config::Config as ServerConfig, Server},
        subscriber::{Subscriber, Value},
    };
    use std::{sync::Arc, time::Duration};
    use tokio::{runtime::Runtime, sync::oneshot, task, time};

    pub(crate) struct Ctx {
        pub(crate) _server: Server,
        pub(crate) publisher: Publisher,
        pub(crate) subscriber: Subscriber,
        pub(crate) base: Path,
    }

    impl Ctx {
        pub(crate) async fn new() -> Self {
            let cfg = ServerConfig::load("../cfg/simple-server.json")
                .expect("load simple server config");
            let _server =
                Server::new(cfg.clone(), false, 0).await.expect("start resolver server");
            let mut cfg = ClientConfig::load("../cfg/simple-client.json")
                .expect("load simple client config");
            cfg.addrs[0].0 = *_server.local_addr();
            let publisher = Publisher::new(
                cfg.clone(),
                DesiredAuth::Anonymous,
                "127.0.0.1/32".parse().unwrap(),
                768,
            )
            .await
            .unwrap();
            let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous).unwrap();
            let base = Path::from("/channel");
            Self { _server, publisher, subscriber, base }
        }
    }

    #[test]
    fn ping_pong() {
        Runtime::new().unwrap().block_on(async move {
            let ctx = Ctx::new().await;
            let mut listener =
                server::Listener::new(&ctx.publisher, None, ctx.base.clone())
                    .await
                    .unwrap();
            task::spawn(async move {
                let con =
                    client::Connection::connect(&ctx.subscriber, ctx.base).await.unwrap();
                for i in 0..100 {
                    con.send(Value::U64(i as u64)).unwrap();
                    match con.recv_one().await.unwrap() {
                        Value::U64(j) => assert_eq!(j, i),
                        _ => panic!("expected u64"),
                    }
                }
            });
            let con = listener.accept().await.unwrap().wait_connected().await.unwrap();
            for _ in 0..100 {
                match con.recv_one().await.unwrap() {
                    Value::U64(i) => con.send_one(Value::U64(i)).await.unwrap(),
                    _ => panic!("expected u64"),
                }
            }
        })
    }

    #[test]
    fn singleton() {
        Runtime::new().unwrap().block_on(async move {
            let ctx = Ctx::new().await;
            let path = ctx.base.clone();
            let acceptor =
                server::singleton(&ctx.publisher, None, ctx.base).await.unwrap();
            task::spawn(async move {
                let con =
                    client::Connection::connect(&ctx.subscriber, path).await.unwrap();
                for i in 0..100 {
                    con.send(Value::U64(i as u64)).unwrap();
                    match con.recv_one().await.unwrap() {
                        Value::U64(j) => assert_eq!(j, i),
                        _ => panic!("expected u64"),
                    }
                }
            });
            let con = acceptor.wait_connected().await.unwrap();
            for _ in 0..100 {
                match con.recv_one().await.unwrap() {
                    Value::U64(i) => con.send_one(Value::U64(i)).await.unwrap(),
                    _ => panic!("expected u64"),
                }
            }
        })
    }

    #[test]
    fn hang() {
        Runtime::new().unwrap().block_on(async move {
            let ctx = Arc::new(Ctx::new().await);
            let mut listener =
                server::Listener::new(&ctx.publisher, None, ctx.base.clone())
                    .await
                    .unwrap();
            let ctx_ = ctx.clone();
            task::spawn(async move {
                let con =
                    client::Connection::connect(&ctx_.subscriber, ctx_.base.clone())
                        .await
                        .unwrap();
                for i in 0..100 {
                    con.send(Value::U64(i as u64)).unwrap();
                }
                con.flush().await.unwrap();
                for i in 0..100 {
                    match con.recv_one().await.unwrap() {
                        Value::U64(j) => assert_eq!(j, i),
                        _ => panic!("expected u64"),
                    }
                }
            });
            let ctx_ = ctx.clone();
            task::spawn(async move {
                let con =
                    client::Connection::connect(&ctx_.subscriber, ctx_.base.clone())
                        .await
                        .unwrap();
                for i in 0..100 {
                    con.send(Value::U64(i as u64)).unwrap();
                }
                con.flush().await.unwrap();
                for i in 0..100 {
                    match con.recv_one().await.unwrap() {
                        Value::U64(j) => assert_eq!(j, i),
                        _ => panic!("expected u64"),
                    }
                }
            });
            let con = listener.accept().await.unwrap().wait_connected().await.unwrap();
            let (tx, rx) = oneshot::channel();
            task::spawn(async move {
                for _ in 0..100 {
                    match con.recv_one().await.unwrap() {
                        Value::U64(i) => con.send_one(Value::U64(i)).await.unwrap(),
                        _ => panic!("expected u64"),
                    }
                }
                let _ = tx.send(());
            });
            // hang the second connection
            let _con = listener.accept().await.unwrap().wait_connected().await.unwrap();
            time::timeout(Duration::from_secs(10), rx).await.unwrap().unwrap()
        })
    }
}
