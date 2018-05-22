use futures::prelude::*;
use std::{sync::Arc, cell::Cell};

#[derive(Debug, Clone)]
pub struct Encoded(Arc<Vec<u8>>);

impl Encoded {
  pub fn new(data: Vec<u8>) -> Self { Encoded(Arc::new(data)) }
}

impl AsRef<[u8]> for Encoded {
  fn as_ref(&self) -> &[u8] { self.0.as_ref().as_ref() }
}

pub struct Batched<S: Stream> {
  stream: S,
  error: Cell<Option<<S as Stream>::Error>>,
  ended: bool,
  max: usize
}

pub fn batched<S: Stream>(stream: S, max: usize) -> Batched<S> {
  Batched {
    stream, max,
    error: Cell::new(None),
    ended: false
  }
}

impl<S: Stream> Stream for Batched<S> {
  type Item = Vec<<S as Stream>::Item>;
  type Error = <S as Stream>::Error;

  fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
    if self.ended { return Ok(Async::Ready(None)) }
    if let Some(e) = self.error.replace(None) { return Err(e) }
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
            self.error.replace(Some(e));
            return Ok(Async::Ready(Some(ready)))
          }
        },
      }
    }
  }
}
