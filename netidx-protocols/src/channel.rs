pub mod server {
    use anyhow::Result;
    use futures::{channel::mpsc, prelude::*};
    use netidx::{
        path::Path,
        pool::Pooled,
        publisher::{ClId, Publisher, UpdateBatch, Val, Value, WriteRequest},
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
        queue_depth: usize,
        timeout: Option<Duration>,
    }

    impl Listener {
        pub async fn new(
            publisher: &Publisher,
            queue_depth: usize,
            timeout: Option<Duration>,
            path: Path,
        ) -> Result<Listener> {
            let publisher = publisher.clone();
            let listener = publisher.publish(path.clone(), Value::from("channel"))?;
            let (tx_waiting, rx_waiting) = mpsc::channel(queue_depth);
            publisher.writes(listener.id(), tx_waiting);
            publisher.flushed().await;
            Ok(Self {
                publisher,
                _listener: listener,
                waiting: rx_waiting,
                queued: Pooled::orphan(Vec::new()),
                base: path,
                queue_depth,
                timeout,
            })
        }

        pub async fn accept(
            &mut self,
        ) -> Result<impl Future<Output = Result<Connection>>> {
            let req = loop {
                if let Some(req) = self.queued.pop() {
                    match &req.value {
                        Value::String(c) if &**c == "connect" => {
                            if req.send_result.is_some() {
                                break req;
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
            let queue_depth = self.queue_depth;
            let val =
                self.publisher.publish(session.clone(), Value::from("connection"))?;
            let publisher = self.publisher.clone();
            let timeout = self.timeout;
            let (tx, rx) = mpsc::channel(queue_depth);
            publisher.writes(val.id(), tx);
            Ok(async move {
                let con = Connection {
                    publisher,
                    anchor: Arc::new(val),
                    client: req.client,
                    dead: AtomicBool::new(false),
                    timeout,
                    receiver: Mutex::new(Receiver {
                        writes: rx,
                        queued: VecDeque::new(),
                    }),
                };
                req.send_result.unwrap().send(Value::from(session));
                loop {
                    let to = Duration::from_secs(3);
                    if con.publisher.is_subscribed(&con.anchor.id(), &con.client) {
                        match time::timeout(to, con.recv_one()).await?? {
                            Value::String(s) if &*s == "ready" => {
                                con.send_one(Value::from("ready")).await?
                            }
                            Value::String(s) if &*s == "go" => break Ok(con),
                            _ => (),
                        }
                    } else {
                        let f = con.publisher.wait_client(con.anchor.id());
                        time::timeout(to, f).await?
                    }
                }
            })
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

    pub struct Batch {
        updates: Pooled<Vec<Value>>,
    }

    impl Batch {
        pub fn queue(&mut self, v: Value) {
            self.updates.push(v);
        }
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
    }

    impl Connection {
        pub async fn connect(
            subscriber: &Subscriber,
            queue_depth: usize,
            timeout: Option<Duration>,
            path: Path,
        ) -> Result<Connection> {
            let acceptor = subscriber.subscribe_one(path.clone(), timeout).await?;
            match acceptor.write_with_recipt(Value::from("connect")).await {
                Err(_) => bail!("connect failed"),
                Ok(v @ Value::String(_)) => {
                    let path = v.cast_to::<Path>()?;
                    let (tx, rx) = mpsc::channel(queue_depth);
                    let mut n = 0;
                    let con = loop {
                        if n >= 7 {
                            break subscriber
                                .subscribe_one(path.clone(), timeout)
                                .await?;
                        } else {
                            match subscriber.subscribe_one(path.clone(), timeout).await {
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
                        receiver: Mutex::new(Receiver {
                            updates: rx,
                            queued: VecDeque::new(),
                        }),
                    };
                    let to = Duration::from_millis(100);
                    loop {
                        con.send_one(Value::from("ready"))?;
                        match time::timeout(to, con.recv_one()).await {
                            Err(_) => (),
                            Ok(Err(e)) => return Err(e),
                            Ok(Ok(Value::String(s))) if &*s == "ready" => {
                                con.send_one(Value::from("go"))?;
                                break;
                            }
                            _ => (),
                        }
                    }
                    Ok(con)
                }
                Ok(_) => bail!("unexpected response from publisher"),
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

        pub fn start_batch(&self) -> Batch {
            Batch { updates: BATCHES.take() }
        }

        pub fn send(&self, mut batch: Batch) -> Result<()> {
            self.check_dead()?;
            for v in batch.updates.drain(..) {
                self.con.write(v);
            }
            Ok(())
        }

        pub fn send_one(&self, v: Value) -> Result<()> {
            self.check_dead()?;
            Ok(self.con.write(v))
        }

        pub async fn flush(&self) -> Result<()> {
            self.check_dead()?;
            self.con.flush().await.map_err(|_| {
                self.dead.store(true, Ordering::Relaxed);
                anyhow!("connection is dead")
            })
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
    use tokio::{runtime::Runtime, task};

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
                server::Listener::new(&ctx.publisher, 50, None, ctx.base.clone())
                    .await
                    .unwrap();
            task::spawn(async move {
                let con =
                    client::Connection::connect(&ctx.subscriber, 50, None, ctx.base)
                        .await
                        .unwrap();
                for i in 0..100 {
                    con.send_one(Value::U64(i as u64)).unwrap();
                    match con.recv_one().await.unwrap() {
                        Value::U64(j) => assert_eq!(j, i),
                        _ => panic!("expected u64"),
                    }
                }
            });
            let con = listener.accept().await.unwrap().await.unwrap();
            for _ in 0..100 {
                match con.recv_one().await.unwrap() {
                    Value::U64(i) => con.send_one(Value::U64(i)).await.unwrap(),
                    _ => panic!("expected u64"),
                }
            }
        })
    }

    #[test]
    fn batch_ping_pong() {
        Runtime::new().unwrap().block_on(async move {
            let ctx = Ctx::new().await;
            let mut listener =
                server::Listener::new(&ctx.publisher, 50, None, ctx.base.clone())
                    .await
                    .unwrap();
            task::spawn(async move {
                let con = client::Connection::connect(
                    &ctx.subscriber,
                    50,
                    None,
                    ctx.base,
                )
                .await
                .unwrap();
                for _ in 0..100 {
                    let mut b = con.start_batch();
                    for i in 0..100u64 {
                        b.queue(Value::U64(i))
                    }
                    con.send(b).unwrap();
                    let mut v = Vec::new();
                    con.recv(&mut v).await.unwrap();
                    let mut i = 0;
                    for j in v {
                        assert_eq!(j, Value::U64(i));
                        i += 1;
                    }
                    assert_eq!(i, 100)
                }
            });
            let con = listener.accept().await.unwrap().await.unwrap();
            for _ in 0..100 {
                let mut v = Vec::new();
                con.recv(&mut v).await.unwrap();
                let mut b = con.start_batch();
                for i in v {
                    b.queue(i);
                }
                con.send(b).await.unwrap();
            }
        })
    }
}
