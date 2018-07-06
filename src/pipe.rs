use futures::{prelude::*, stream::unfold};
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

#[derive(Clone)]
pub struct Writer<T>(Pipe<T>, Arc<Sentinal<T>>);

impl<T: 'static> Writer<T> {
    pub fn write(&self, v: T) {
        let mut t = (self.0).0.lock().unwrap();
        t.buffer.push_back(v);
        for s in t.waiters.drain(0..) { let _ = s.send(()); }
    }

    pub fn write_many<V: IntoIterator<Item=T>>(&self, batch: V) {
        let mut t = (self.0).0.lock().unwrap();
        t.buffer.extend(batch);
        for s in t.waiters.drain(0..) { let _ = s.send(()); }
    }

    /// wait for all the previous writes to be processed
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

    /// Return the number of items in the pipe now
    pub fn len(&self) {
        let t = (self.0).0.lock().unwrap();
        t.buffer.len()
    }
}

#[derive(Clone)]
pub struct Reader<T>(Pipe<T>, Arc<Sentinal<T>>);

impl<T: 'static> Reader<T> {
    #[async]
    pub fn read(self) -> Result<T> {
        Ok(await!(self.process(|buf| buf.pop_front().unwrap()))?)
    }

    /// Wait until a batch is ready, read it into the passed in container.
    #[async]
    pub fn read_into<C: Extend<T> + 'static>(self, mut container: C) -> Result<C> {
        Ok(await!(self.process(move |buf| {
            container.extend(buf.drain(0..));
            container
        }))?)
    }

    #[async]
    fn process<V, F>(self, f: F) -> Result<V>
    where V: 'static, F: FnMut(&mut VecDeque<T>) -> V
    {
        loop {
            let wait = {
                let mut t = (self.0).0.lock().unwrap();        
                if t.buffer.len() == 0 {
                    if t.closed { bail!("pipe is closed") }
                    let (tx, rx) = oneshot::channel();
                    t.waiters.push(tx);
                    rx
                } else {
                    let r = f(&mut t.buffer);
                    if t.buffer.len() == 0 {
                        for s in t.flushes.drain(0..) { let _ = s.send(()); }
                    }
                    return r
                }
            };
            await!(wait)?
        }
    }

    /// return a future that is ready when a new batch is ready.
    #[async]
    pub fn ready(self) -> Result<()> {
        loop {
            let wait = {
                let mut t = (self.0).0.lock().unwrap();        
                if t.flushes.len() > 0 { return Ok(()) }
                else {
                    if t.closed { bail!("pipe is closed") }
                    let (tx, rx) = oneshot::channel();
                    t.waiters.push(tx);
                    rx
                }
            };
            await!(wait)?
        }
    }

    /// return a stream that emits an item whenever a batch is ready
    pub fn all_ready(self) -> impl Stream<Item=(), Error=Error> {
        unfold((), move |f| Some(self.clone.ready(), ())).map(|_| ())
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
