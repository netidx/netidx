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

pub(crate) fn batched<S: Stream>(stream: S, max: usize) -> Batched<S> {
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

pub(crate) struct Batched<S: Stream> {
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
                // this may not be ok, as it can result in calling poll again
                // after it has returned not ready, but before the wakeup has
                // been called. It seems to work, and I'm not sure a better
                // way to do it.
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
