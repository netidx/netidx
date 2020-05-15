use crate::pack::{Pack, PackError};
use anyhow::{self, Result};
use bytes::{Buf, Bytes, BytesMut};
use futures::{
    prelude::*,
    sink::Sink,
    stream::FusedStream,
    task::{Context, Poll},
    channel::mpsc,
};
use std::{
    cell::RefCell,
    cmp::min,
    collections::VecDeque,
    hash::Hash,
    io::{self, IoSlice, Write},
    net::{IpAddr, SocketAddr},
    ops::{Deref, DerefMut},
    pin::Pin,
    str,
    any::{Any, TypeId}
};

macro_rules! bail {
    ($($msg:expr), +) => {
        return Err(anyhow::anyhow!($($msg), +))
    }
}

#[macro_export]
macro_rules! try_cf {
    ($msg:expr, continue, $lbl:tt, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                log::info!($msg, e);
                continue $lbl;
            }
        }
    };
    ($msg:expr, break, $lbl:tt, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                log::info!($msg, e);
                break $lbl Err(Error::from(e));
            }
        }
    };
    ($msg:expr, continue, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                log::info!("{}: {}", $msg, e);
                continue;
            }
        }
    };
    ($msg:expr, break, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                log::info!("{}: {}", $msg, e);
                break Err(Error::from(e));
            }
        }
    };
    (continue, $lbl:tt, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                continue $lbl;
            }
        }
    };
    (break, $lbl:tt, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                break $lbl Err(Error::from(e));
            }
        }
    };
    ($msg:expr, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                log::info!("{}: {}", $msg, e);
                break Err(Error::from(e));
            }
        }
    };
    (continue, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                continue;
            }
        }
    };
    (break, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                break Err(Error::from(e));
            }
        }
    };
    ($e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                break Err(Error::from(e));
            }
        }
    };
}

pub fn check_addr(ip: IpAddr, resolvers: &[SocketAddr]) -> Result<()> {
    match ip {
        IpAddr::V4(ip) if ip.is_link_local() => {
            bail!("addr is a link local address");
        }
        IpAddr::V4(ip) if ip.is_broadcast() => {
            bail!("addr is a broadcast address");
        }
        IpAddr::V4(ip) if ip.is_private() => {
            let ok = resolvers.iter().all(|a| match a.ip() {
                IpAddr::V4(ip) if ip.is_private() => true,
                IpAddr::V6(_) => true,
                _ => false,
            });
            if !ok {
                bail!("addr is a private address, and the resolver is not")
            }
        }
        _ => (),
    }
    if ip.is_unspecified() {
        bail!("addr is an unspecified address");
    }
    if ip.is_multicast() {
        bail!("addr is a multicast address");
    }
    if ip.is_loopback() && !resolvers.iter().all(|a| a.ip().is_loopback()) {
        bail!("addr is a loopback address and the resolver is not");
    }
    Ok(())
}

pub fn is_sep(esc: &mut bool, c: char, escape: char, sep: char) -> bool {
    if c == sep {
        !*esc
    } else {
        *esc = c == escape && !*esc;
        false
    }
}

pub fn splitn_escaped(
    s: &str,
    n: usize,
    escape: char,
    sep: char,
) -> impl Iterator<Item = &str> {
    s.splitn(n, {
        let mut esc = false;
        move |c| is_sep(&mut esc, c, escape, sep)
    })
}

pub fn split_escaped(s: &str, escape: char, sep: char) -> impl Iterator<Item = &str> {
    s.split({
        let mut esc = false;
        move |c| is_sep(&mut esc, c, escape, sep)
    })
}

thread_local! {
    static BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(512));
}

pub fn pack<T: Pack>(t: &T) -> Result<BytesMut, PackError> {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        t.encode(&mut *b)?;
        Ok(b.split())
    })
}

pub fn bytesmut(t: &[u8]) -> BytesMut {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        b.extend_from_slice(t);
        b.split()
    })
}

pub fn bytes(t: &[u8]) -> Bytes {
    bytesmut(t).freeze()
}

// CR estokes: unused?
pub struct BytesWriter<'a>(pub &'a mut BytesMut);

impl Write for BytesWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// CR estokes: unused?
pub struct BytesDeque(VecDeque<Bytes>);

impl BytesDeque {
    pub fn new() -> Self {
        BytesDeque(VecDeque::new())
    }

    pub fn with_capacity(c: usize) -> Self {
        BytesDeque(VecDeque::with_capacity(c))
    }
}

impl Deref for BytesDeque {
    type Target = VecDeque<Bytes>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BytesDeque {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Buf for BytesDeque {
    fn remaining(&self) -> usize {
        self.0.iter().fold(0, |s, b| s + b.remaining())
    }

    fn bytes(&self) -> &[u8] {
        if self.0.len() == 0 {
            &[]
        } else {
            self.0[0].bytes()
        }
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 && self.0.len() > 0 {
            let b = &mut self.0[0];
            let n = min(cnt, b.remaining());
            b.advance(n);
            cnt -= n;
            if b.remaining() == 0 {
                self.0.pop_front();
            }
        }
        if cnt > 0 {
            panic!("Buf::advance called with cnt - remaining = {} ", cnt);
        }
    }

    fn bytes_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let mut i = 0;
        while i < self.0.len() && i < dst.len() {
            dst[i] = IoSlice::new(&self.0[i].bytes());
            i += 1;
        }
        i
    }
}

#[derive(Clone)]
pub(crate) struct ChanWrap<T>(pub(crate) mpsc::Sender<T>);

impl<T> PartialEq for ChanWrap<T> {
    fn eq(&self, other: &ChanWrap<T>) -> bool {
        self.0.same_receiver(&other.0)
    }
}

impl<T> Eq for ChanWrap<T> {}

impl<T> Hash for ChanWrap<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash_receiver(state)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChanId(u64);

impl ChanId {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        ChanId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone)]
pub enum BatchItem<T> {
    InBatch(T),
    EndBatch,
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
        Batched { stream, max, ended: false, current: 0 }
    }

    pub fn inner(&self) -> &S {
        &self.stream
    }

    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    pub fn into_inner(self) -> S {
        self.stream
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
                }
                Poll::Ready(None) => {
                    *self.as_mut().ended() = true;
                    if self.current == 0 {
                        Poll::Ready(None)
                    } else {
                        *self.current() = 0;
                        Poll::Ready(Some(BatchItem::EndBatch))
                    }
                }
                Poll::Pending => {
                    if self.current == 0 {
                        Poll::Pending
                    } else {
                        *self.current() = 0;
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

impl<S: Stream> FusedStream for Batched<S> {
    fn is_terminated(&self) -> bool {
        self.ended
    }
}

impl<Item, S: Stream + Sink<Item>> Sink<Item> for Batched<S> {
    type Error = <S as Sink<Item>>::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<(), Self::Error>> {
        self.stream().poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        self.stream().start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<(), Self::Error>> {
        self.stream().poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Result<(), Self::Error>> {
        self.stream().poll_close(cx)
    }
}
