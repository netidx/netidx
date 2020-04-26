use crate::{
    model::{Error as PackError, Pack, Result as PackResult},
    protocol::shared,
};
use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use futures::{
    prelude::*,
    sink::Sink,
    stream::FusedStream,
    task::{Context, Poll},
};
use std::pin::Pin;
use std::{
    cmp::min,
    collections::VecDeque,
    fmt,
    io::{self, IoSlice, Write},
    net,
    ops::{Deref, DerefMut},
    str,
};

macro_rules! try_cf {
    ($msg:expr, $id:ident, $lbl:tt, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                log::info!($msg, e);
                $id $lbl Err(Error::from(e));
            }
        }
    };
    ($msg:expr, $id:ident, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                log::info!($msg, e);
                $id Err(Error::from(e));
            }
        }
    };
    ($id:ident, $lbl:tt, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                $id $lbl Err(Error::from(e));
            }
        }
    };
    ($msg:expr, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                log::info!($msg, e);
                break Err(Error::from(e));
            }
        }
    };
    ($id:ident, $e:expr) => {
        match $e {
            Ok(x) => x,
            Err(e) => {
                $id Err(Error::from(e));
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

pub fn saddr_from_protobuf(p: shared::SocketAddr) -> Option<net::SocketAddr> {
    p.addr.map(|addr| match addr {
        shared::SocketAddr_oneof_addr::V4(addr) => {
            let octets = addr.octets.to_be_bytes();
            let ip = net::Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3]);
            net::SocketAddr::V4(net::SocketAddrV4::new(ip, addr.port as u16))
        }
        shared::SocketAddr_oneof_addr::V6(addr) => {
            let ip = {
                let a = addr.octets_a.to_be_bytes();
                let b = addr.octets_b.to_be_bytes();
                net::Ipv6Addr::from([
                    a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], b[0], b[1], b[2],
                    b[3], b[4], b[5], b[6], b[7],
                ])
            };
            let a = net::SocketAddrV6::new(
                ip,
                addr.port as u16,
                addr.flowinfo,
                addr.scope_id,
            );
            net::SocketAddr::V6(a)
        }
    })
}

pub fn saddr_to_protobuf(a: net::SocketAddr) -> shared::SocketAddr {
    match a {
        net::SocketAddr::V4(v4) => shared::SocketAddr {
            addr: Some(shared::SocketAddr_oneof_addr::V4(
                shared::SocketAddr_SocketAddrV4 {
                    octets: u32::from_be_bytes(v4.ip().octets()),
                    port: v4.port() as u32,
                    ..shared::SocketAddr_SocketAddrV4::default()
                },
            )),
            ..shared::SocketAddr::default()
        },
        net::SocketAddr::V6(v6) => {
            let oc = &v6.ip().octets();
            let addr =
                shared::SocketAddr_oneof_addr::V6(shared::SocketAddr_SocketAddrV6 {
                    octets_a: u64::from_be_bytes([
                        oc[0], oc[1], oc[2], oc[3], oc[4], oc[5], oc[6], oc[7],
                    ]),
                    octets_b: u64::from_be_bytes([
                        oc[8], oc[9], oc[10], oc[11], oc[12], oc[13], oc[14], oc[15],
                    ]),
                    port: v6.port() as u32,
                    flowinfo: v6.flowinfo(),
                    scope_id: v6.scope_id(),
                    ..shared::SocketAddr_SocketAddrV6::default()
                });
            shared::SocketAddr {
                addr: Some(addr),
                ..shared::SocketAddr::default()
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Chars(Bytes);

impl Chars {
    pub fn new() -> Chars {
        Chars(Bytes::new())
    }

    pub fn from_bytes(bytes: Bytes) -> Result<Chars, str::Utf8Error> {
        str::from_utf8(&bytes)?;
        Ok(Chars(bytes))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Pack for Chars {
    fn len(&self) -> usize {
        Pack::len(self)
    }

    fn encode(&self, buf: &mut BytesMut) -> PackResult<()> {
        Pack::encode(&self.0, buf)
    }

    fn decode(buf: &mut BytesMut) -> PackResult<Self> {
        match Chars::from_bytes(<Bytes as Pack>::decode(buf)?) {
            Ok(c) => Ok(c),
            Err(_) => Err(PackError::InvalidFormat),
        }
    }
}

impl<'a> From<&'a str> for Chars {
    fn from(src: &'a str) -> Chars {
        Chars(Bytes::copy_from_slice(src.as_bytes()))
    }
}

impl From<String> for Chars {
    fn from(src: String) -> Chars {
        Chars(Bytes::from(src))
    }
}

impl Deref for Chars {
    type Target = str;

    fn deref(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.0) }
    }
}

impl fmt::Display for Chars {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

impl fmt::Debug for Chars {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

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
        Batched {
            stream,
            max,
            ended: false,
            current: 0,
        }
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
