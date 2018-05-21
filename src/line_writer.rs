use std::result;
use std::marker::PhantomData;
use std::cell::RefCell;
use std::rc::{Rc, Weak};
use std::convert::AsRef;
use futures::prelude::*;
use futures::unsync::mpsc::{channel, Receiver, Sender};
use futures::unsync::oneshot;
use tokio;
use tokio::executor::current_thread::spawn;
use tokio::io::{AsyncWrite, Write};
use tokio_io::io::{WriteHalf, write_all};
use error::*;

enum Msg<T> {
  Stop,
  Flush,
  Write(T)
}

struct LineWriterInner<T, K> {
  sender: Sender<Vec<Msg<T>>>,
  running: bool,
  queued: Vec<Msg<T>>,
  flushes: Vec<oneshot::Sender<()>>,
  on_shutdown: Vec<oneshot::Sender<()>>,
  dead: bool,
  kind: PhantomData<K>
}

struct LineWriterWeak<T, K>(Weak<RefCell<LineWriterInner<T, K>>>);

impl<T,K> Clone for LineWriterWeak<T,K> {
  fn clone(&self) -> Self { LineWriterWeak(Weak::clone(&self.0)) }
}

impl<T,K> LineWriterWeak<T,K> {
  fn upgrade(&self) -> Option<LineWriter<T,K>> {
    Weak::upgrade(&self.0).map(|r| LineWriter(r))
  }
}

pub struct LineWriter<T, K>(Rc<RefCell<LineWriterInner<T, K>>>);

impl<T, K> Clone for LineWriter<T, K> {
  fn clone(&self) -> Self { LineWriter(Rc::clone(&self.0)) }
}

impl<T: AsRef<[u8]> + 'static, K: AsyncWrite + Write + 'static> LineWriter<T, K> {
  pub fn new(chan: WriteHalf<K>) -> Self {
    let (sender, receiver) = channel(10);
    let inner = LineWriterInner {
      sender,
      running: false,
      queued: Vec::new(),
      flushes: Vec::new(),
      on_shutdown: Vec::new(),
      dead: false,
      kind: PhantomData
    };
    let t = LineWriter(Rc::new(RefCell::new(inner)));
    spawn(start_inner_loop(t.downgrade(), receiver, chan));
    t
  }

  fn downgrade(&self) -> LineWriterWeak<T,K> {
    LineWriterWeak(Rc::downgrade(&self.0))
  }

  #[async]
  fn send_loop(self) -> result::Result<(), ()> {
    loop {          
      let (msgs, mut sender, dead) = {
        let mut t = self.0.borrow_mut();
        if t.queued.len() == 0 { break; }
        else { (t.queued.split_off(0), t.sender.clone(), t.dead) }
      };
      if !dead { await!(sender.send(msgs)).map_err(|_| ())?; }
      else {
        for f in self.0.borrow_mut().flushes.drain(0..) {
          let _ = f.send(());
        }
      }
    }
    self.0.borrow_mut().running = false;
    Ok(())
  }

  // send ensures messages are sent in order, but allows the sender
  // not to wait for each message to go out. To wait, use a flush.
  fn send(&self, msg: Msg<T>) {
    let mut t = self.0.borrow_mut();
    t.queued.push(msg);
    if !t.running {
      t.running = true;
      spawn(self.clone().send_loop())
    }
  }

  pub fn shutdown(&self) { self.send(Msg::Stop) }

  pub fn write(&self, data: T) { self.send(Msg::Write(data)) }

  // when the future returned by flush is ready, all messages
  // sent before flush was called have been handed to the OS,
  // or the connection has failed
  #[async]
  pub fn flush(self) -> Result<()> {
    let (tx, rx) = oneshot::channel();
    self.0.borrow_mut().flushes.push(tx);
    self.send(Msg::Flush);
    await!(rx)?;
    Ok(())
  }

  #[async]
  pub fn on_shutdown(self) -> Result<()> {
    if self.0.borrow().dead { Ok(()) }
    else {
      let (tx, rx) = oneshot::channel();
      self.0.borrow_mut().on_shutdown.push(tx);
      Ok(await!(rx)?)
    }
  }
}

#[async]
fn inner_loop<T, K>(
  t: LineWriterWeak<T, K>,
  receiver: Receiver<Vec<Msg<T>>>,
  mut chan: WriteHalf<K>
) -> Result<()>
where T: AsRef<[u8]> + 'static, K: AsyncWrite + Write + 'static {
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
            let mut t = t.0.borrow_mut();
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
  t: LineWriterWeak<T, K>,
  receiver: Receiver<Vec<Msg<T>>>,
  chan: WriteHalf<K>
) -> result::Result<(), ()>
where T: AsRef<[u8]> + 'static, K: AsyncWrite + Write + 'static {
  let _ = await!(inner_loop(t.clone(), receiver, chan));
  if let Some(t) = t.upgrade() {
    let mut t = t.0.borrow_mut();
    t.dead = true;
    for f in t.flushes.split_off(0).into_iter() {
      let _ = f.send(());
    }
    for s in t.on_shutdown.drain(0..) {
      let _ = s.send(());
    }
  }
  Ok(())
}
