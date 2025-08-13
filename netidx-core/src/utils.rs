use crate::{
    pack::{Pack, PackError},
    pool::{Pool, Poolable, Pooled},
};
use anyhow::{self, Result};
use bytes::{Bytes, BytesMut};
use digest::Digest;
use futures::{
    channel::mpsc,
    prelude::*,
    sink::Sink,
    stream::FusedStream,
    task::{Context, Poll},
};
use fxhash::FxHashMap;
use sha3::Sha3_512;
use std::{
    any::{Any, TypeId},
    borrow::Borrow,
    borrow::Cow,
    cell::RefCell,
    cmp::{Ord, Ordering, PartialOrd},
    collections::HashMap,
    hash::Hash,
    iter::{IntoIterator, Iterator},
    net::{IpAddr, SocketAddr},
    pin::Pin,
    str,
};

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

#[macro_export]
macro_rules! atomic_id {
    ($name:ident) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        pub struct $name(u64);

        impl $name {
            pub fn new() -> Self {
                use std::sync::atomic::{AtomicU64, Ordering};
                static NEXT: AtomicU64 = AtomicU64::new(0);
                $name(NEXT.fetch_add(1, Ordering::Relaxed))
            }

            pub fn inner(&self) -> u64 {
                self.0
            }

            #[cfg(test)]
            #[allow(dead_code)]
            pub fn mk(i: u64) -> Self {
                $name(i)
            }
        }

        impl netidx_core::pack::Pack for $name {
            fn encoded_len(&self) -> usize {
                netidx_core::pack::varint_len(self.0)
            }

            fn encode(
                &self,
                buf: &mut impl bytes::BufMut,
            ) -> std::result::Result<(), netidx_core::pack::PackError> {
                Ok(netidx_core::pack::encode_varint(self.0, buf))
            }

            fn decode(
                buf: &mut impl bytes::Buf,
            ) -> std::result::Result<Self, netidx_core::pack::PackError> {
                Ok(Self(netidx_core::pack::decode_varint(buf)?))
            }
        }
    };
}

pub fn check_addr<A>(ip: IpAddr, resolvers: &[(SocketAddr, A)]) -> Result<()> {
    match ip {
        IpAddr::V4(ip) if ip.is_link_local() => {
            bail!("addr is a link local address");
        }
        IpAddr::V4(ip) if ip.is_broadcast() => {
            bail!("addr is a broadcast address");
        }
        IpAddr::V4(ip) if ip.is_private() => {
            let ok = resolvers.iter().all(|(a, _)| match a.ip() {
                IpAddr::V4(ip) if ip.is_private() || ip.is_loopback() => true,
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
    if ip.is_loopback() && !resolvers.iter().all(|(a, _)| a.ip().is_loopback()) {
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

/// escape the specified string using the specified escape character
/// and a slice of special characters that need escaping. Place the
/// results into the specified buffer
pub fn escape_to<T>(s: &T, buf: &mut String, esc: char, spec: &[char])
where
    T: AsRef<str> + ?Sized,
{
    for c in s.as_ref().chars() {
        if spec.contains(&c) {
            buf.push(esc);
            buf.push(c);
        } else if c == esc {
            buf.push(esc);
            buf.push(c);
        } else {
            buf.push(c);
        }
    }
}

/// escape the specified string using the specified escape character
/// and a slice of special characters that need escaping.
pub fn escape<'a, 'b, T>(s: &'a T, esc: char, spec: &'b [char]) -> Cow<'a, str>
where
    T: AsRef<str> + ?Sized,
    'a: 'b,
{
    let s = s.as_ref();
    if s.find(|c: char| spec.contains(&c) || c == esc).is_none() {
        Cow::Borrowed(s.as_ref())
    } else {
        let mut out = String::with_capacity(s.len());
        escape_to(s, &mut out, esc, spec);
        Cow::Owned(out)
    }
}

/// unescape the specified string using the specified escape
/// character. Place the result in the specified buffer.
pub fn unescape_to<T>(s: &T, buf: &mut String, esc: char)
where
    T: AsRef<str> + ?Sized,
{
    let mut escaped = false;
    buf.extend(s.as_ref().chars().filter_map(|c| {
        if c == esc && !escaped {
            escaped = true;
            None
        } else {
            escaped = false;
            Some(c)
        }
    }))
}

/// unescape the specified string using the specified escape character. Place
/// the result in the specified buffer. Translate escaped characters in the
/// tr mapping.
pub fn unescape_tr_to<T>(s: &T, buf: &mut String, esc: char, tr: &[(char, char)])
where
    T: AsRef<str> + ?Sized,
{
    let mut escaped = false;
    buf.extend(s.as_ref().chars().filter_map(|c| {
        if c == esc && !escaped {
            escaped = true;
            None
        } else if escaped {
            escaped = false;
            match tr.iter().find_map(|(k, v)| if c == *k { Some(*v) } else { None }) {
                None => Some(c),
                Some(c) => Some(c),
            }
        } else {
            Some(c)
        }
    }))
}

/// unescape the specified string using the specified escape character
pub fn unescape<T>(s: &T, esc: char) -> Cow<str>
where
    T: AsRef<str> + ?Sized,
{
    let s = s.as_ref();
    if !s.contains(esc) {
        Cow::Borrowed(s.as_ref())
    } else {
        let mut res = String::with_capacity(s.len());
        unescape_to(s, &mut res, esc);
        Cow::Owned(res)
    }
}

/// unescape the specified string using the specified escape character,
/// translate escaped characters using the tr map
pub fn unescape_tr<'a, T>(s: &'a T, esc: char, tr: &[(char, char)]) -> Cow<'a, str>
where
    T: AsRef<str> + ?Sized,
{
    let s = s.as_ref();
    if !s.contains(esc) {
        Cow::Borrowed(s.as_ref())
    } else {
        let mut res = String::with_capacity(s.len());
        unescape_tr_to(s, &mut res, esc, tr);
        Cow::Owned(res)
    }
}

pub fn is_escaped(s: &str, esc: char, i: usize) -> bool {
    let b = s.as_bytes();
    !s.is_char_boundary(i) || {
        let mut res = false;
        for j in (0..i).rev() {
            if s.is_char_boundary(j) && b[j] == (esc as u8) {
                res = !res;
            } else {
                break;
            }
        }
        res
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

pub fn rsplit_escaped(s: &str, escape: char, sep: char) -> impl Iterator<Item = &str> {
    s.rsplit({
        let mut esc = false;
        move |c| is_sep(&mut esc, c, escape, sep)
    })
}

thread_local! {
    static BUF: RefCell<BytesMut> = RefCell::new(BytesMut::with_capacity(512));
}

pub fn make_sha3_token<'a>(data: impl IntoIterator<Item = &'a [u8]> + 'a) -> Bytes {
    let mut hash = Sha3_512::new();
    for v in data.into_iter() {
        hash.update(v);
    }
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        b.extend(hash.finalize().into_iter());
        b.split().freeze()
    })
}

/// pack T and return a bytesmut from the global thread local buffer
pub fn pack<T: Pack>(t: &T) -> Result<BytesMut, PackError> {
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        t.encode(&mut *b)?;
        Ok(b.split())
    })
}

thread_local! {
    static POOLS: RefCell<FxHashMap<TypeId, Box<dyn Any>>> =
        RefCell::new(HashMap::default());
}

/// Take a poolable type T from the generic thread local pool set.
/// Note it is much more efficient to construct your own pools.
/// size and max are the pool parameters used if the pool doesn't
/// already exist.
pub fn take_t<T: Any + Poolable + Send + 'static>(size: usize, max: usize) -> Pooled<T> {
    POOLS.with(|pools| {
        let mut pools = pools.borrow_mut();
        let pool: &mut Pool<T> = pools
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(Pool::<T>::new(size, max)))
            .downcast_mut()
            .unwrap();
        pool.take()
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

#[derive(Clone, Debug)]
pub struct ChanWrap<T>(pub mpsc::Sender<T>);

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

// a socketaddr wrapper that implements Ord so we can put clients in a
// set.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct Addr(pub SocketAddr);

impl From<SocketAddr> for Addr {
    fn from(addr: SocketAddr) -> Self {
        Addr(addr)
    }
}

impl Borrow<SocketAddr> for Addr {
    fn borrow(&self) -> &SocketAddr {
        &self.0
    }
}

impl PartialOrd for Addr {
    fn partial_cmp(&self, other: &Addr) -> Option<Ordering> {
        match (self.0, other.0) {
            (SocketAddr::V4(v0), SocketAddr::V4(v1)) => {
                match v0.ip().octets().partial_cmp(&v1.ip().octets()) {
                    None => None,
                    Some(Ordering::Equal) => v0.port().partial_cmp(&v1.port()),
                    Some(o) => Some(o),
                }
            }
            (SocketAddr::V6(v0), SocketAddr::V6(v1)) => {
                match v0.ip().octets().partial_cmp(&v1.ip().octets()) {
                    None => None,
                    Some(Ordering::Equal) => v0.port().partial_cmp(&v1.port()),
                    Some(o) => Some(o),
                }
            }
            (SocketAddr::V4(_), SocketAddr::V6(_)) => Some(Ordering::Less),
            (SocketAddr::V6(_), SocketAddr::V4(_)) => Some(Ordering::Greater),
        }
    }
}

impl Ord for Addr {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum Either<T, U> {
    Left(T),
    Right(U),
}

impl<T, U> Either<T, U> {
    pub fn is_left(&self) -> bool {
        match self {
            Self::Left(_) => true,
            Self::Right(_) => false,
        }
    }

    pub fn is_right(&self) -> bool {
        match self {
            Self::Left(_) => false,
            Self::Right(_) => true,
        }
    }

    pub fn left(self) -> Option<T> {
        match self {
            Either::Left(t) => Some(t),
            Either::Right(_) => None,
        }
    }

    pub fn right(self) -> Option<U> {
        match self {
            Either::Right(t) => Some(t),
            Either::Left(_) => None,
        }
    }
}

impl<I, T: Iterator<Item = I>, U: Iterator<Item = I>> Iterator for Either<T, U> {
    type Item = I;
    fn next(&mut self) -> Option<I> {
        match self {
            Either::Left(t) => t.next(),
            Either::Right(t) => t.next(),
        }
    }
}
