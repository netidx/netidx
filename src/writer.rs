use std::{
    result,
    iter::IntoIterator,
    marker::PhantomData,
    sync::{Arc, Weak, Mutex},
    convert::AsRef
};
use futures::{
    io::WriteHalf,
    channel::{mpsc::{channel, Receiver, Sender}, oneshot}
};
use async_std::prelude::*
use failure::Error;
use crossbeam::queue::SeqQueue;

enum Msg<T> {
    Stop,
    Flush,
    Write(T)
}

struct WriterInner<T, K> {
    sender: Sender<Vec<Msg<T>>>,
    queued: Vec<Msg<T>>,
    flushes: Vec<oneshot::Sender<()>>,
    on_shutdown: Vec<oneshot::Sender<()>>,
    dead: bool,
    kind: PhantomData<K>
}

struct WriterWeak<T, K>(Weak<Mutex<WriterInner<T, K>>>);

impl<T,K> Clone for LineWriterWeak<T,K> {
    fn clone(&self) -> Self { WriterWeak(Weak::clone(&self.0)) }
}

impl<T,K> WriterWeak<T,K> {
    fn upgrade(&self) -> Option<Writer<T,K>> {
        Weak::upgrade(&self.0).map(|r| Writer(r))
    }
}

pub struct Writer<T, K>(Arc<Mutex<WriterInner<T, K>>>);

impl<T, K> Clone for Writer<T, K> {
    fn clone(&self) -> Self { Writer(Arc::clone(&self.0)) }
}

impl<T: AsRef<[u8]> + Send + Sync + 'static,
     K: AsyncWrite + Write + Send + Sync + 'static> Writer<T, K> {
    pub fn new(chan: WriteHalf<K>) -> Self {
        let (sender, receiver) = channel(10);
        let inner = WriterInner {
            sender,
            queued: Vec::new(),
            flushes: Vec::new(),
            on_shutdown: Vec::new(),
            dead: false,
            kind: PhantomData
        };
        let t = Writer(Arc::new(Mutex::new(inner)));
        spawn(start_inner_loop(t.downgrade(), receiver, chan));
        t
    }

    fn downgrade(&self) -> WriterWeak<T,K> {
        WriterWeak(Arc::downgrade(&self.0))
    }

    #[async]
    fn send_batch(self) -> result::Result<(), ()> {
        let (msgs, mut sender, dead) = {
            let mut t = self.0.lock().unwrap();
            if t.queued.len() == 0 {
                return Ok(());
            } else {
                (t.queued.split_off(0), t.sender.clone(), t.dead)
            }
        };
        if dead { Err(()) }
        else {
            match await!(sender.send(msgs)) {
                Ok(_) => Ok(()),
                Err(_) => {
                    let mut t = self.0.lock().unwrap();
                    for f in t.flushes.drain(0..) {
                        let _ = f.send(());
                    }
                    Err(())
                }
            }
        }
    }

    pub fn shutdown(&self) {
        {
            let mut t = self.0.lock().unwrap();
            t.queued.push(Msg::Stop);
        }
        spawn(self.clone().send_batch());
    }

    pub fn write_one(&self, msg: T) {
        let mut t = self.0.lock().unwrap();
        t.queued.push(Msg::Write(msg));
    }

    pub fn write_two(&self, msg0: T, msg1: T) {
        let mut t = self.0.lock().unwrap();
        t.queued.push(Msg::Write(msg0));
        t.queued.push(Msg::Write(msg1));
    }

    pub fn write_n<Q: IntoIterator<Item=T>>(&self, msgs: Q) {
        let mut t = self.0.lock().unwrap();
        for msg in msgs.into_iter() { t.queued.push(Msg::Write(msg)) };
    }

    pub fn flush_nowait(&self) {
        spawn(self.clone().send_batch());
    }

    // when the future returned by flush is ready, all messages
    // sent before flush was called have been handed to the OS,
    // or the connection has failed
    pub fn flush(self) -> impl Future<Item=(), Error=Error> {
        let (tx, rx) = oneshot::channel();
        {
            let mut t = self.0.lock().unwrap();
            t.flushes.push(tx);
            t.queued.push(Msg::Flush);
        }
        spawn(self.clone().send_batch());
        async_block! {
            await!(rx)?;
            Ok(())
        }
    }

    #[async]
    pub fn on_shutdown(self) -> Result<()> {
        let wait = {
            let mut t = self.0.lock().unwrap();
            if t.dead { None } else {
                let (tx, rx) = oneshot::channel();
                t.on_shutdown.push(tx);
                Some(rx)
            }
        };
        match wait {
            None => Ok(()),
            Some(rx) => Ok(await!(rx)?),
        }
    }
}

#[async]
fn inner_loop<T, K>(
    t: WriterWeak<T, K>,
    receiver: Receiver<Vec<Msg<T>>>,
    mut chan: WriteHalf<K>
) -> Result<()>
where T: AsRef<[u8]> + Send + Sync + 'static,
      K: AsyncWrite + Write + Send + Sync + 'static {
    let mut buf : Vec<u8> = Vec::new();
    #[async]
    'outer: for batch in receiver.map_err(|()| Error::from("stream error")) {
        buf.clear();
        for msg in batch.into_iter() {
            match msg {
                Msg::Stop => {
                    if buf.len() > 0 {
                        let r = await!(write_all(chan, buf))?;
                        chan = r.0;
                    }
                    break 'outer;
                },
                Msg::Write(v) => {
                    buf.extend_from_slice(v.as_ref());
                    if v.as_ref()[v.as_ref().len() - 1] != 10u8 { buf.push('\n' as u8) }
                },
                Msg::Flush => {
                    if buf.len() > 0 {
                        let r = await!(write_all(chan, buf))?;
                        chan = r.0;
                        buf = r.1;
                        buf.clear();
                    }
                    chan = await!(tokio::io::flush(chan))?;
                    if let Some(t) = t.upgrade() {
                        let mut t = t.0.lock().unwrap();
                        if t.flushes.len() > 0 {
                            let _ = t.flushes.remove(0).send(());
                        }
                    }
                }
            }
        }
        if buf.len() > 0 {
            let r = await!(write_all(chan, buf))?;
            chan = r.0;
            buf = r.1;
        }
    }
    await!(tokio::io::shutdown(chan))?;
    Ok(())
}

#[async]
fn start_inner_loop<T, K>(
    t: WriterWeak<T, K>,
    receiver: Receiver<Vec<Msg<T>>>,
    chan: WriteHalf<K>
) -> result::Result<(), ()>
where T: AsRef<[u8]> + Send + Sync + 'static,
      K: AsyncWrite + Write + Send + Sync + 'static {
    let _ = await!(inner_loop(t.clone(), receiver, chan));
    if let Some(t) = t.upgrade() {
        let mut t = t.0.lock().unwrap();
        t.dead = true;
        for f in t.flushes.drain(0..) {
            let _ = f.send(());
        }
        for s in t.on_shutdown.drain(0..) {
            let _ = s.send(());
        }
    }
    Ok(())
}
