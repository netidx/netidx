use bytes::{Bytes, BytesMut};
use std::{
    io::{self, Write},
    result::Result,
    cell::RefCell,
};
use async_std::prelude::*;
use futures::{task::{Poll, Context}, sink::Sink};
use std::pin::Pin;
use serde::Serialize;

#[macro_export]
macro_rules! try_cont {
    ($m:expr, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                eprintln!("{}: {}", $m, e);
                continue
            }
        }
    }
}

#[macro_export]
macro_rules! try_ret {
    ($m:expr, $e:expr) => {
        match $e {
            Ok(r) => r,
            Err(e) => {
                // CR estokes: use a better method
                eprintln!("{}: {}", $m, e);
                return
            }
        }
    }
}

#[macro_export]
macro_rules! or_ret {
    ($e:expr) => {
        match $e {
            Some(r) => r,
            None => return
        }
    }
}

#[macro_export]
macro_rules! ret {
    ($m:expr) => { {
        eprintln!("{}", $m);
        return
    } }
}

pub fn is_sep(esc: &mut bool, c: char, escape: char, sep: char) -> bool {
    if c == sep { !esc }
    else {
        *esc = c == escape && !*esc;
        false
    }
}

pub fn splitn_escaped(
    s: &str,
    n: usize,
    escape: char,
    sep: char
) {
    s.splitn({
        let mut esc = false;
        move |c| is_sep(&mut esc, c, escape, sep)
    })
}

pub fn split_escaped(
    s: &str,
    escape: char,
    sep: char
) -> impl Iterator<Item=&str> {
    s.split({
        let mut esc = false;
        move |c| is_sep(&mut esc, c, escape, sep)
    })
}

pub(crate) struct BytesWriter<'a>(pub(crate) &'a mut BytesMut);

impl Write for BytesWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
	self.0.extend(buf);
	Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
	Ok(())
    }
}

thread_local! {
    static BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(512));
}

pub fn mp_encode<T: Serialize>(t: &T) -> Result<Bytes, failure::Error> {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        rmp_serde::encode::write_named(&mut BytesWriter(&mut *b), t)?;
        Ok(b.split().freeze())
    })
}

pub fn rmpv_encode(t: &rmpv::Value) -> Result<Bytes, failure::Error> {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        rmpv::encode::write_value(&mut BytesWriter(&mut *b), t)?;
        Ok(b.split().freeze())
    })
}

pub fn str_encode(t: &str) -> Bytes {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        b.extend_from_slice(t.as_bytes());
        b.split().freeze()
    })
}

#[derive(Debug, Clone)]
pub enum BatchItem<T> {
    InBatch(T),
    EndBatch
}

#[must_use = "streams do nothing unless polled"]
pub struct Batched<S: Stream> {
    stream: S,
    ended: bool,
    max: usize,
    current: usize,
}

impl<S: Stream> Batched<S> {
    // this is safe because,
    // - Batched doesn't implement Drop
    // - Batched doesn't implement Unpin
    // - Batched isn't #[repr(packed)]
    unsafe_pinned!(stream: S);

    // these are safe because both types are copy
    unsafe_unpinned!(ended: bool);
    unsafe_unpinned!(current: usize);

    pub fn new(stream: S, max: usize) -> Batched<S> {
        Batched {
            stream, max,
            ended: false,
            current: 0
        }
    }
}

impl<S: Stream> Stream for Batched<S> {
    type Item = BatchItem<<S as Stream>::Item>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.ended {
            Poll::Ready(None)
        } else if self.current >= self.max {
            *self.current() = 0;
            Poll::Ready(Some(BatchItem::EndBatch))
        } else {
            match self.as_mut().stream().poll_next(cx) {
                Poll::Ready(Some(v)) => {
                    *self.as_mut().current() += 1;
                    Poll::Ready(Some(BatchItem::InBatch(v)))
                },
                Poll::Ready(None) => {
                    *self.as_mut().ended() = true;
                    if self.current == 0 {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(BatchItem::EndBatch))
                    }
                },
                Poll::Pending => {
                    if self.current == 0 {
                        Poll::Pending
                    } else {
                        Poll::Ready(Some(BatchItem::EndBatch))
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

impl<Item, S: Stream + Sink<Item>> Sink<Item> for Batched<S> {
    type Error = <S as Sink<Item>>::Error;

    fn poll_ready(
        self: Pin<&mut Self>, 
        cx: &mut Context
    ) -> Poll<Result<(), Self::Error>> {
        self.stream().poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        self.stream().start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>, 
        cx: &mut Context
    ) -> Poll<Result<(), Self::Error>> {
        self.stream().poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>, 
        cx: &mut Context
    ) -> Poll<Result<(), Self::Error>> {
        self.stream().poll_close(cx)
    }
}
