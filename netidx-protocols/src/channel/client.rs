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
    fn fill_from_channel(
        &mut self,
        dead: &AtomicBool,
        r: Option<Pooled<Vec<(SubId, Event)>>>,
    ) -> Result<()> {
        match r {
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
        Ok(())
    }

    async fn fill_queue(&mut self, dead: &AtomicBool) -> Result<()> {
        self.try_fill_queue(dead)?;
        if self.queued.len() == 0 {
            if dead.load(Ordering::Relaxed) {
                bail!("connection is dead")
            }
            let r = self.updates.next().await;
            self.fill_from_channel(dead, r)?
        }
        Ok(())
    }

    fn try_fill_queue(&mut self, dead: &AtomicBool) -> Result<()> {
        for _ in 0..10 {
            match self.updates.try_next() {
                Err(_) => break,
                Ok(r) => {
                    if let Err(e) = self.fill_from_channel(dead, r) {
                        if self.queued.len() == 0 {
                            return Err(e);
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// A connection is a bidirectional channel between two endpoints.
pub struct Connection {
    _subscriber: Subscriber,
    con: Val,
    receiver: Mutex<Receiver>,
    dead: AtomicBool,
    dirty: AtomicBool,
}

impl Connection {
    async fn connect_singleton(subscriber: &Subscriber, path: Path) -> Result<Self> {
        let to = Duration::from_secs(15);
        let (tx, rx) = mpsc::channel(5);
        let mut n = 0;
        let con = loop {
            if n > 3 {
                break subscriber
                    .subscribe_nondurable_one(path.clone(), Some(to))
                    .await?;
            } else {
                match subscriber.subscribe_nondurable_one(path.clone(), Some(to)).await {
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
        con.send(Value::from("ready"))?;
        match time::timeout(Duration::from_secs(15), con.recv_one()).await {
            Err(_) => bail!("timeout waiting for channel handshake"),
            Ok(Err(e)) => return Err(e),
            Ok(Ok(Value::String(s))) if &*s == "ready" => con.send(Value::from("go"))?,
            Ok(Ok(v)) => bail!(
                "unexpected channel handshake, expected Value::String(\"ready\") got {}",
                v
            ),
        }
        Ok(con)
    }

    /// Connect to the endpoint at the specified path. The endpoint
    /// may be either a listener or a singleton.
    pub async fn connect(subscriber: &Subscriber, path: Path) -> Result<Self> {
        let to = Duration::from_secs(15);
        let acceptor = subscriber.subscribe(path.clone());
        time::timeout(to, acceptor.wait_subscribed()).await??;
        match dbg!(acceptor.last()) {
            Event::Unsubscribed => bail!("connect failed, unsubscribed after connect"),
            Event::Update(Value::String(s)) if &*s == "connection" => {
                Self::connect_singleton(subscriber, path).await
            }
            Event::Update(Value::String(s)) if &*s == "channel" => {
                let f = acceptor.write_with_recipt(Value::from("connect"));
                match time::timeout(to, f).await? {
                    Err(_) => bail!("connect failed, timed out"),
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

    /// Return true if the connection is dead.
    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Relaxed)
    }

    fn check_dead(&self) -> Result<()> {
        Ok(if self.is_dead() {
            bail!("connection is dead")
        })
    }

    /// Send the specified value to the other side. Note, the value
    /// will start it's journey immediatly, there is no need to call
    /// flush unless you want pushback.
    pub fn send(&self, v: Value) -> Result<()> {
        self.check_dead()?;
        self.dirty.store(true, Ordering::Relaxed);
        Ok(self.con.write(v))
    }

    /// True if you have sent values, but have not called flush.
    pub fn dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    /// Wait for previously sent values to be flushed out to os
    /// buffers.
    pub async fn flush(&self) -> Result<()> {
        self.check_dead()?;
        let r = self.con.flush().await.map_err(|_| {
            self.dead.store(true, Ordering::Relaxed);
            anyhow!("connection is dead")
        });
        self.dirty.store(false, Ordering::Relaxed);
        r
    }

    /// Wait for a value to arrive from the other side and return it
    /// when it does.
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

    /// Return a value if one is available now, otherwise don't wait
    /// and return None. This will only block if another receive is in
    /// progress.
    pub async fn try_recv_one(&self) -> Result<Option<Value>> {
        let mut recv = self.receiver.lock().await;
        if recv.queued.len() == 0 {
            recv.try_fill_queue(&self.dead)?
        }
        Ok(recv.queued.pop_front())
    }

    /// Wait for a batch of values to arrive from the other side and
    /// put them in the specified data structure.
    pub async fn recv(&self, dst: &mut impl Extend<Value>) -> Result<()> {
        let mut recv = self.receiver.lock().await;
        recv.try_fill_queue(&self.dead)?;
        loop {
            if recv.queued.len() > 0 {
                break Ok(dst.extend(recv.queued.drain(..)));
            } else {
                self.check_dead()?;
                recv.fill_queue(&self.dead).await?
            }
        }
    }

    /// Receive all messages that are available now, but don't wait
    /// for more. This will only block when another recv is in
    /// progress concurrently.
    pub async fn try_recv(&self, dst: &mut impl Extend<Value>) -> Result<bool> {
        let mut recv = self.receiver.lock().await;
        recv.try_fill_queue(&self.dead)?;
        if recv.queued.len() > 0 {
            dst.extend(recv.queued.drain(..));
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
