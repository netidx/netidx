use bytes::{BytesMut, BigEndian, ByteOrder};
use futures_codec::{Encoder, Decoder};
use std::{
    mem,
    convert::TryInto,
    marker::PhantomData,
    io::{self, Write},
    result::Result,
};
use serde::{Serialize, de::DeserializeOwned};
use failure::Error;

#[macro_export]
macro_rules! try_cont {
    ($m:expr, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                println!("{}: {}", $m, e);
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
                println!("{}: {}", $m, e);
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
        println!("{}", $m);
        return
    } }
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

static U64S: usize = mem::size_of::<u64>();

pub struct MPCodec<T, F>(PhantomData<T>, PhantomData<F>);

impl<T, F> MPCodec<T, F> {
    pub fn new() -> MPCodec<T, F> {
        MPCodec(PhantomData, PhantomData)
    }
}

impl<T: Serialize, F: DeserializeOwned> Encoder for MPCodec<T, F> {
    type Item = T;
    type Error = Error;

    fn encode(&mut self, src: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if dst.capacity() < U64S {
            dst.reserve(U64S * 20);
        }
        let mut header = dst.split_to(U64S);
        let res = rmp_serde::encode::write_named(&mut BytesWriter(dst), &src);
        BigEndian::write_u64(&mut header, dst.len() as u64);
        Ok(res?)
    }
}

impl<T: Serialize, F: DeserializeOwned> Decoder for MPCodec<T, F> {
    type Item = F;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < U64S {
            Ok(None)
        } else {
            let len: usize =
                BigEndian::read_u64(src).try_into()?;
            if src.len() - U64S < len {
                Ok(None)
            } else {
                src.advance(U64S);
                let res = rmp_serde::decode::from_read(src.as_ref());
                src.advance(len);
                Ok(Some(res?))
            }
        }
    }
}

use async_std::prelude::*;
use futures::{task::{Poll, Context}, sink::Sink};
use std::pin::Pin;

pub enum BatchItem<T> {
    InBatch(T),
    EndBatch
}

#[must_use = "streams do nothing unless polled"]
pub(crate) struct Batched<S: Stream> {
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

    pub(crate) fn new(stream: S, max: usize) -> Batched<S> {
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
