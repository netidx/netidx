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
                            Event::Unsubscribed => dead.store(true, Ordering::Relaxed),
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
