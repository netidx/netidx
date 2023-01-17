pub mod server {
    use anyhow::Result;
    use futures::{channel::mpsc, prelude::*};
    use netidx::{
        path::Path,
        pool::Pooled,
        publisher::{ClId, Publisher, UpdateBatch, Val, Value, WriteRequest},
    };
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::sync::Mutex;

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
        pub fn push(&mut self, v: Value) {
            self.anchor.update_subscriber(&mut self.queued, self.client, v);
        }
    }

    struct Receiver {
        writes: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
        queued: Pooled<Vec<WriteRequest>>,
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

        pub async fn send_batch(&self, batch: Batch) -> Result<()> {
            if self.is_dead() {
                bail!("connection is dead")
            }
            Ok(batch.queued.commit(self.timeout).await)
        }

        pub async fn send(&self, v: Value) -> Result<()> {
            if self.is_dead() {
                bail!("connection is dead")
            }
            let mut batch = self.publisher.start_batch();
            self.anchor.update_subscriber(&mut batch, self.client, v);
            Ok(batch.commit(self.timeout).await)
        }

        pub async fn recv(&self) -> Result<Value> {
            let mut recv = self.receiver.lock().await;
            loop {
                match recv.queued.pop() {
                    Some(req) => {
                        if req.client == self.client {
                            break Ok(req.value);
                        }
                    }
                    None => {
                        if self.is_dead() {
                            bail!("connection is dead")
                        }
                        match recv.writes.next().await {
                            Some(batch) => recv.queued = batch,
                            None => {
                                self.dead.store(true, Ordering::Relaxed);
                                bail!("connection is dead")
                            }
                        }
                    }
                }
            }
        }

        pub async fn recv_batch(&self, dst: &mut impl Extend<Value>) -> Result<()> {
            let mut recv = self.receiver.lock().await;
            let mut n = 0;
            loop {
                if recv.queued.len() > 0 {
                    dst.extend(recv.queued.drain(..).filter_map(|req| {
                        if req.client == self.client {
                            n += 1;
                            Some(req.value)
                        } else {
                            None
                        }
                    }))
                } else {
                    if self.is_dead() {
                        bail!("connection is dead")
                    }
                    match recv.writes.next().await {
                        Some(batch) => recv.queued = batch,
                        None => {
                            self.dead.store(true, Ordering::Relaxed);
                            bail!("connection is dead")
                        }
                    }
                }
                if n > 0 {
                    break Ok(());
                }
            }
        }
    }

    pub struct Listener {
        publisher: Publisher,
        listener: Val,
        waiting: mpsc::Receiver<Pooled<Vec<WriteRequest>>>,
        queued: Pooled<Vec<WriteRequest>>,
        base: Path,
        queue_depth: usize,
        timeout: Option<Duration>,
    }

    impl Listener {
        pub fn new(
            publisher: &Publisher,
            queue_depth: usize,
            timeout: Option<Duration>,
            path: Path,
        ) -> Result<Listener> {
            let publisher = publisher.clone();
            let listener = publisher.publish(path.clone(), Value::from("channel"))?;
            let (tx_waiting, rx_waiting) = mpsc::channel(queue_depth);
            publisher.writes(listener.id(), tx_waiting);
            Ok(Self {
                publisher,
                listener,
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
                        queued: Pooled::orphan(Vec::new()),
                    }),
                };
                req.send_result.unwrap().send(Value::from(session));
                loop {
                    if con.publisher.is_subscribed(&con.anchor.id(), &con.client) {
                        match con.recv().await? {
                            Value::String(s) if &*s == "ready" => break Ok(con),
                            _ => (),
                        }
                    } else {
                        con.publisher.wait_any_new_client().await
                    }
                }
            })
        }
    }
}

pub mod client {
    use anyhow::{Result, anyhow};
    use futures::{channel::mpsc, prelude::*};
    use netidx::{
        path::Path,
        pool::{Pool, Pooled},
        subscriber::{Event, SubId, Subscriber, UpdatesFlags, Val, Value},
    };
    use std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::sync::Mutex;

    lazy_static! {
        static ref BATCHES: Pool<Vec<Value>> = Pool::new(1000, 100_000);
    }

    pub struct Batch {
        updates: Pooled<Vec<Value>>,
    }

    impl Batch {
        pub fn push(&mut self, v: Value) {
            self.updates.push(v);
        }
    }

    struct Receiver {
        updates: mpsc::Receiver<Pooled<Vec<(SubId, Event)>>>,
        queued: Pooled<Vec<(SubId, Event)>>,
    }

    pub struct Connection {
        subscriber: Subscriber,
        con: Val,
        receiver: Mutex<Receiver>,
        dead: AtomicBool,
    }

    impl Connection {
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

        pub fn send_batch(&self, mut batch: Batch) -> Result<()> {
            self.check_dead()?;
            for v in batch.updates.drain(..) {
                self.con.write(v);
            }
            Ok(())
        }

        pub fn send(&self, v: Value) -> Result<()> {
            self.check_dead()?;
            Ok(self.con.write(v))
        }

        pub async fn flush(&self) -> Result<()> {
            self.check_dead()?;
            self.con.flush().await.map_err(|e| {
                self.dead.store(true, Ordering::Relaxed);
                anyhow!("connection is dead")
            })
        }

        pub async fn recv(&self) -> Result<Value> {
            self.check_dead()?;
            let mut recv = self.receiver.lock().await;
            loop {
                
            }
        }
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
                    let con = subscriber.subscribe_one(path, timeout).await?;
                    con.updates(UpdatesFlags::empty(), tx);
                    con.write(Value::from("ready"));
                    Ok(Connection {
                        con,
                        dead: AtomicBool::new(false),
                        receiver: Mutex::new(Receiver {
                            updates: rx,
                            queued: Pooled::orphan(Vec::new()),
                        }),
                    })
                }
                Ok(_) => bail!("unexpected response from publisher"),
            }
        }
    }
}
