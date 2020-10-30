use crate::pack::{Pack, PackError};
use anyhow::{self, Result};
use bytes::{BufMut, Bytes, BytesMut};
use digest::Digest;
use futures::{
    channel::mpsc,
    prelude::*,
    sink::Sink,
    stream::FusedStream,
    task::{Context, Poll},
};
use rand::Rng;
use sha3::Sha3_512;
use std::{
    cell::RefCell,
    hash::Hash,
    iter::{IntoIterator, Iterator},
    net::{IpAddr, SocketAddr},
    pin::Pin,
    borrow::Borrow,
    cmp::{Ord, Ordering, PartialOrd},
    str,
    borrow::Cow,
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
    ($name:ident) => (
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            pub fn new() -> Self {
                use std::sync::atomic::{AtomicU64, Ordering};
                static NEXT: AtomicU64 = AtomicU64::new(0);
                $name(NEXT.fetch_add(1, Ordering::Relaxed))
            }

            #[cfg(test)]
            #[allow(dead_code)]
            pub(crate) fn mk(i: u64) -> Self {
                $name(i)
            }
        }
    )
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

pub fn is_escaped(s: &str, sep: char, esc: char, i: usize) -> bool {
    let b = s.as_bytes();
    !s.is_char_boundary(i)
        || ((b[i] == (sep as u8) || b[i] == (esc as u8)) && {
            let mut res = false;
            for j in (0..i).rev() {
                if s.is_char_boundary(j) && b[j] == (esc as u8) {
                    res = !res;
                } else {
                    break;
                }
            }
            res
        })
}

pub fn escape<T: AsRef<str> + ?Sized>(s: &T, esc: char, sep: char) -> Cow<str> {
    let s = s.as_ref();
    if s.find(|c: char| c == sep || c == esc).is_none() {
        Cow::Borrowed(s.as_ref())
    } else {
        let mut out = String::with_capacity(s.len());
        for c in s.chars() {
            if c == sep {
                out.push(esc);
                out.push(c);
            } else if c == esc {
                out.push(esc);
                out.push(c);
            } else {
                out.push(c);
            }
        }
        Cow::Owned(out)
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

pub(crate) fn make_sha3_token(salt: Option<u64>, secret: &[&[u8]]) -> Bytes {
    let salt = salt.unwrap_or_else(|| rand::thread_rng().gen::<u64>());
    let mut hash = Sha3_512::new();
    hash.update(&salt.to_be_bytes());
    for v in secret {
        hash.update(v);
    }
    BUF.with(|buf| {
        let mut b = buf.borrow_mut();
        b.put_u64(salt);
        b.extend(hash.finalize().into_iter());
        b.split().freeze()
    })
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
