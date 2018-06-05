use futures::prelude::*;
use std::{sync::Arc, mem};

#[derive(Debug, Clone)]
pub struct Encoded(Arc<Vec<u8>>);

impl Encoded {
  pub fn new(data: Vec<u8>) -> Self { Encoded(Arc::new(data)) }
}

impl AsRef<[u8]> for Encoded {
  fn as_ref(&self) -> &[u8] { self.0.as_ref().as_ref() }
}

/*
pub struct Batched<S: Stream> {
  stream: S,
  error: Option<<S as Stream>::Error>,
  ended: bool,
  max: usize
}

pub fn batched<S: Stream>(stream: S, max: usize) -> Batched<S> {
  Batched {
    stream, max,
    error: None,
    ended: false
  }
}

impl<S: Stream> Stream for Batched<S> {
  type Item = Vec<<S as Stream>::Item>;
  type Error = <S as Stream>::Error;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    if self.ended { return Ok(Async::Ready(None)) }
    let mut e = None;
    mem::swap(&mut self.error, &mut e);
    if let Some(e) = e { return Err(e) }
    let mut ready = vec![];
    loop {
      if ready.len() >= self.max { return Ok(Async::Ready(Some(ready))) }
      match self.stream.poll() {
        Ok(Async::Ready(Some(v))) => ready.push(v),
        Ok(Async::Ready(None)) => {
          if ready.len() == 0 { return Ok(Async::Ready(None)) }
          else {
            self.ended = true;
            return Ok(Async::Ready(Some(ready)))
          }
        },
        Ok(Async::NotReady) => {
          if ready.len() == 0 { return Ok(Async::NotReady) }
          else { return Ok(Async::Ready(Some(ready))) }
        },
        Err(e) => {
          if ready.len() == 0 { return Err(e) }
          else {
            let mut err = Some(e);
            mem::swap(&mut self.error, &mut err);
            return Ok(Async::Ready(Some(ready)))
          }
        },
      }
    }
  }
}
*/

pub fn batched<S: Stream>(stream: S, max: usize) -> Batched<S> {
  Batched {
    stream, max,
    ended: false,
    blocked: false,
    error: None,
    current: 0
  }
}

pub enum BatchItem<T> {
  InBatch(T),
  EndBatch
}

pub struct Batched<S: Stream> {
  stream: S,
  ended: bool,
  blocked: bool,
  error: Option<<S as Stream>::Error>,
  max: usize,
  current: usize,
}

impl<S: Stream> Stream for Batched<S> {
  type Item = BatchItem<<S as Stream>::Item>;
  type Error = <S as Stream>::Error;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    if self.ended { return Ok(Async::Ready(None)) }
    let mut err = None;
    mem::swap(&mut err, &mut self.error);
    if let Some(e) = err { return Err(e) }
    if self.current >= self.max {
      self.current = 0;
      return Ok(Async::Ready(Some(BatchItem::EndBatch)))
    }
    match self.stream.poll() {
      Ok(Async::Ready(Some(v))) => {
        self.blocked = false;
        self.current += 1;
        Ok(Async::Ready(Some(BatchItem::InBatch(v))))
      },
      Ok(Async::Ready(None)) => {
        self.blocked = false;
        self.ended = true;
        Ok(Async::Ready(Some(BatchItem::EndBatch)))
      },
      Ok(Async::NotReady) => {
        if self.blocked { Ok(Async::NotReady) }
        else {
          self.blocked = true;
          Ok(Async::Ready(Some(BatchItem::EndBatch)))
        }
      },
      Err(e) => {
        self.blocked = false;
        self.error = Some(e);
        Ok(Async::Ready(Some(BatchItem::EndBatch)))
      }
    }
  }
}
