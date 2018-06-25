//! A pipe is a batch oriented, heap allocated, syncronized, inter
//! thread queue. It is a multi producer multi consumer structure with
//! a read end and a write end, each end may be cheaply cloned, sent
//! between threads, and used by multiple threads simultaneously.

use futures::prelude::*;
use futures::sync::oneshot;
use std::{sync::{Arc, Mutex, MutexGuard}, collections::VecDeque, mem, iter::Extend};
use error::*;

struct PipeInner<T> {
  waiters: Vec<oneshot::Sender<()>>,
  flushes: Vec<oneshot::Sender<()>>,
  buffer: VecDeque<T>,
  closed: bool
}

#[derive(Clone)]
struct Pipe<T>(Arc<Mutex<PipeInner<T>>>);

struct Sentinal<T>(Pipe<T>);

impl<T> Drop for Sentinal<T> {
  fn drop(&mut self) {
    let mut t = (self.0).0.lock().unwrap();
    t.closed = true;
    for s in t.waiters.drain(0..) { let _ = s.send(()); }
  }
}

/// The write end of the pipe.
#[derive(Clone)]
pub struct Writer<T>(Pipe<T>, Arc<Sentinal<T>>);

impl<T: 'static> Writer<T> {
  /// Add an element to the current batch. Nothing will happen until
  /// flush is called.
  pub fn write(&self, v: T) {
    let mut t = (self.0).0.lock().unwrap();
    t.buffer.push_back(v);
  }

  /// Add many elements to the current batch. Nothing will happen
  /// until flush is called.
  pub fn write_many<V: IntoIterator<Item=T>>(&self, batch: V) {
    let mut t = (self.0).0.lock().unwrap();
    t.buffer.extend(batch);
  }

  /// Flush the currently queued batch to the reader. The future
  /// returned by flush will be ready when the entire batch has been
  /// removed by one or more readers. If elements are queued, and the
  /// writer is dropped before flush is called, the elements will not
  /// be processed.
  #[async]
  pub fn flush(self) -> Result<()> {
    let wait = {
      let mut t = (self.0).0.lock().unwrap();
      if t.closed { bail!("pipe is closed") }
      if t.buffer.len() == 0 { None }
      else {
        let (tx, rx) = oneshot::channel();
        t.flushes.push(tx);
        for s in t.waiters.drain(0..) { let _ = s.send(()); }
        Some(rx)
      }
    };
    match wait {
      Some(wait) => Ok(await!(wait)?),
      None => Ok(())
    }
  }

  /// Return the length of the current batch
  pub fn len(&self) {
    let t = (self.0).0.lock().unwrap();
    t.buffer.len()
  }
}

/// The read end of the pipe
#[derive(Clone)]
pub struct Reader<T>(Pipe<T>, Arc<Sentinal<T>>);

impl<T: 'static> Reader<T> {
  /// Wait until a batch is ready, read and return one element from it.
  #[async]
  pub fn read_one(self) -> Result<T> {
    Ok(await!(self.process(|buf| buf.pop_front().unwrap()))?)
  }

  /// Wait until a batch is ready, read and return it.
  #[async]
  pub fn read(self) -> Result<VecDeque<T>> {
    Ok(await!(self.process(|buf| {
      let mut new = VecDeque::new();
      mem::swap(&mut new, buf);
      new
    }))?)
  }

  /// Wait until a batch is ready, read it into the passed in container.
  #[async]
  pub fn read_into<C: Extend<T> + 'static>(self, mut container: C) -> Result<C> {
    Ok(await!(self.process(move |buf| {
      container.extend(buf.drain(0..));
      container
    }))?)
  }

  /// Wait for a batch to be ready, and then process it with f. F is
  /// responsible for removing elements from the batch as they are
  /// processed. It will not be finished until all elements have been
  /// removed. you don't have to remove all elements from a batch in
  /// one call, you can call this function again as many times as
  /// needed until the batch is processed.
  #[async]
  pub fn process<V, F>(self, f: F) -> Result<V>
    where V: 'static, F: FnMut(&mut VecDeque<T>) -> V + 'static
  {
    loop {
      let wait = {
        let mut t = (self.0).0.lock().unwrap();        
        if t.flushes.len() == 0 {
          if t.closed { bail!("pipe is closed") }
          let (tx, rx) = oneshot::channel();
          t.waiters.push(tx);
          rx
        } else {
          let r = f(&mut t.buffer);
          if t.buffer.len() == 0 {
            for s in t.flushes.drain(0..) { let _ = s.send(()); }
          }
          return Ok(r)
        }
      };
      await!(wait)?
    }
  }

  /// Access the current batch with f. If the batch is empty `with`
  /// returns `None` and does not call f. Otherwise `with` returns
  /// `Some(f(batch))`.
  pub fn with<V, F>(&self, f: F) -> Option<V>
    where V: 'static, F: FnMut(&mut VecDeque<T>) -> V + 'static
  {
    let mut t = (self.0).0.lock().unwrap();        
    if t.flushes.len() == 0 { None }
    else {
      let r = f(&mut t.buffer);
      if t.buffer.len() == 0 {
        for s in t.flushes.drain(0..) { let _ = s.send(()); }
      }
      Some(r)
    }
  }
}

/// create a new pipe
pub fn pipe<T>() -> (Writer<T>, Reader<T>) {
  let pipe = Pipe(Arc::new(Mutex::new(Pipe {
    waiters: Vec::new(),
    flushes: Vec::new(),
    buffer: VecDeque::new(),
    closed: false
  })));
  let sentinal = Arc::new(Sentinal(pipe.clone()));
  (Writer(pipe.clone(), sentinal.clone()), Reader(pipe, sentinal))
}
