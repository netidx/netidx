use crate::pool::{Pool, Poolable, Pooled};
use bytes::{Buf, BufMut, Bytes};
use chrono::{naive::NaiveDateTime, prelude::*};
use fxhash::FxBuildHasher;
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    cmp::Eq,
    collections::HashMap,
    default::Default,
    error, fmt,
    hash::{BuildHasher, Hash},
    mem, net,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
    str,
};
use arcstr::ArcStr;

#[derive(Debug, Clone, Copy)]
pub enum PackError {
    UnknownTag,
    TooBig,
    InvalidFormat,
}

impl fmt::Display for PackError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl error::Error for PackError {}

pub trait Pack {
    fn const_encoded_len() -> Option<usize> {
        None
    }
    fn encoded_len(&self) -> usize;
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError>;
    fn decode(buf: &mut impl Buf) -> Result<Self, PackError>
    where
        Self: std::marker::Sized;

    fn decode_into(&mut self, buf: &mut impl Buf) -> Result<(), PackError>
    where
        Self: std::marker::Sized,
    {
        Ok(*self = <Self as Pack>::decode(buf)?)
    }
}

thread_local! {
    static POOLS: RefCell<HashMap<TypeId, Box<dyn Any>, FxBuildHasher>> =
        RefCell::new(HashMap::default());
}

impl<T: Pack + Any + Send + Sync + Poolable> Pack for Pooled<T> {
    fn encoded_len(&self) -> usize {
        <T as Pack>::encoded_len(&**self)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        <T as Pack>::encode(&**self, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let mut t = POOLS.with(|pools| {
            let mut pools = pools.borrow_mut();
            let pool: &mut Pool<T> = pools
                .entry(TypeId::of::<T>())
                .or_insert_with(|| Box::new(Pool::<T>::new(10000, 10000)))
                .downcast_mut()
                .unwrap();
            pool.take()
        });
        <T as Pack>::decode_into(&mut *t, buf)?;
        Ok(t)
    }

    fn decode_into(&mut self, buf: &mut impl Buf) -> Result<(), PackError> {
        <T as Pack>::decode_into(&mut **self, buf)
    }
}

impl Pack for net::SocketAddr {
    fn encoded_len(&self) -> usize {
        match self {
            net::SocketAddr::V4(_) => 7,
            net::SocketAddr::V6(_) => 27,
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        match self {
            net::SocketAddr::V4(v4) => {
                buf.put_u8(0);
                buf.put_u32(u32::from_be_bytes(v4.ip().octets()));
                buf.put_u16(v4.port());
            }
            net::SocketAddr::V6(v6) => {
                buf.put_u8(1);
                for s in &v6.ip().segments() {
                    buf.put_u16(*s);
                }
                buf.put_u16(v6.port());
                buf.put_u32(v6.flowinfo());
                buf.put_u32(v6.scope_id());
            }
        }
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        match buf.get_u8() {
            0 => {
                let ip = net::Ipv4Addr::from(u32::to_be_bytes(buf.get_u32()));
                let port = buf.get_u16();
                Ok(net::SocketAddr::V4(net::SocketAddrV4::new(ip, port)))
            }
            1 => {
                let mut segments = [0u16; 8];
                for i in 0..8 {
                    segments[i] = buf.get_u16();
                }
                let port = buf.get_u16();
                let flowinfo = buf.get_u32();
                let scope_id = buf.get_u32();
                let ip = net::Ipv6Addr::from(segments);
                let v6 = net::SocketAddrV6::new(ip, port, flowinfo, scope_id);
                Ok(net::SocketAddr::V6(v6))
            }
            _ => return Err(PackError::UnknownTag),
        }
    }
}

impl Pack for Bytes {
    fn encoded_len(&self) -> usize {
        let len = Bytes::len(self);
        varint_len(len as u64) + len
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        encode_varint(Bytes::len(self) as u64, buf);
        Ok(buf.put_slice(&*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let len = decode_varint(buf)?;
        if len as usize > buf.remaining() {
            Err(PackError::TooBig)
        } else {
            Ok(buf.copy_to_bytes(len as usize))
        }
    }
}

impl Pack for String {
    fn encoded_len(&self) -> usize {
        let len = String::len(self);
        varint_len(len as u64) + len
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        encode_varint(String::len(self) as u64, buf);
        Ok(buf.put_slice(self.as_bytes()))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let len = decode_varint(buf)? as usize;
        if len > buf.remaining() {
            Err(PackError::TooBig)
        } else {
            let mut v = vec![0; len];
            buf.copy_to_slice(&mut v);
            match String::from_utf8(v) {
                Ok(s) => Ok(s),
                Err(_) => Err(PackError::InvalidFormat),
            }
        }
    }
}

impl Pack for Arc<str> {
    fn encoded_len(&self) -> usize {
        let s: &str = &*self;
        let len = s.len();
        varint_len(len as u64) + len
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        let s: &str = &*self;
        encode_varint(s.len() as u64, buf);
        Ok(buf.put_slice(self.as_bytes()))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let len = decode_varint(buf)? as usize;
        if len > buf.remaining() {
            Err(PackError::TooBig)
        } else {
            let mut v = vec![0; len];
            buf.copy_to_slice(&mut v);
            match String::from_utf8(v) {
                Ok(s) => Ok(Arc::from(s)),
                Err(_) => Err(PackError::InvalidFormat),
            }
        }
    }
}

impl Pack for ArcStr {
    fn encoded_len(&self) -> usize {
        let s: &str = &*self;
        let len = s.len();
        varint_len(len as u64) + len
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        let s: &str = &*self;
        encode_varint(s.len() as u64, buf);
        Ok(buf.put_slice(self.as_bytes()))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let len = decode_varint(buf)? as usize;
        if len > buf.remaining() {
            Err(PackError::TooBig)
        } else {
            let res = match str::from_utf8(&buf.chunk()[0..len]) {
                Ok(s) => Ok(ArcStr::from(s)),
                Err(_) => Err(PackError::InvalidFormat)
            };
            buf.advance(len);
            res
        }
    }
}
    
pub fn varint_len(value: u64) -> usize {
    ((((value | 1).leading_zeros() ^ 63) * 9 + 73) / 64) as usize
}

pub fn encode_varint(mut value: u64, buf: &mut impl BufMut) {
    loop {
        if value < 0x80 {
            buf.put_u8(value as u8);
            break;
        } else {
            buf.put_u8(((value & 0x7F) | 0x80) as u8);
            value >>= 7;
        }
    }
}

pub fn i32_zz(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

pub fn i32_uzz(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (((n as i32) << 31) >> 31)
}

pub fn i64_zz(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

pub fn i64_uzz(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (((n as i64) << 63) >> 63)
}

pub fn decode_varint(buf: &mut impl Buf) -> Result<u64, PackError> {
    let mut value = 0;
    let mut i = 0;
    while i < 10 {
        let byte = buf.get_u8();
        value |= u64::from(byte & 0x7F) << (i * 7);
        if byte <= 0x7F {
            return Ok(value);
        }
        i += 1;
    }
    Err(PackError::InvalidFormat)
}

impl Pack for u128 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<u128>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<u128>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_u128(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(buf.get_u128())
    }
}

impl Pack for u64 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<u64>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<u64>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_u64(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(buf.get_u64())
    }
}

#[derive(Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Default, Debug)]
pub struct Z64(pub u64);

impl Deref for Z64 {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Z64 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Pack for Z64 {
    fn encoded_len(&self) -> usize {
        varint_len(**self)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(encode_varint(**self, buf))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(Z64(decode_varint(buf)?))
    }
}

impl Pack for u32 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<u32>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<u32>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_u32(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(buf.get_u32())
    }
}

impl Pack for u16 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<u16>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<u16>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_u16(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(buf.get_u16())
    }
}

impl<T: Pack> Pack for Vec<T> {
    fn encoded_len(&self) -> usize {
        self.iter().fold(varint_len(Vec::len(self) as u64), |len, t| {
            len + <T as Pack>::encoded_len(t)
        })
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        encode_varint(Vec::len(self) as u64, buf);
        for t in self {
            <T as Pack>::encode(t, buf)?
        }
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let elts = decode_varint(buf)? as usize;
        let mut data = Vec::with_capacity(elts);
        for _ in 0..elts {
            data.push(<T as Pack>::decode(buf)?);
        }
        Ok(data)
    }

    fn decode_into(&mut self, buf: &mut impl Buf) -> Result<(), PackError> {
        let elts = decode_varint(buf)? as usize;
        for _ in 0..elts {
            self.push(<T as Pack>::decode(buf)?);
        }
        Ok(())
    }
}

impl<K, V, R> Pack for HashMap<K, V, R>
where
    K: Pack + Hash + Eq,
    V: Pack + Hash + Eq,
    R: Default + BuildHasher,
{
    fn encoded_len(&self) -> usize {
        self.iter().fold(varint_len(self.len() as u64), |len, (k, v)| {
            len + <K as Pack>::encoded_len(k) + <V as Pack>::encoded_len(v)
        })
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        encode_varint(self.len() as u64, buf);
        for (k, v) in self {
            <K as Pack>::encode(k, buf)?;
            <V as Pack>::encode(v, buf)?;
        }
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let elts = decode_varint(buf)? as usize;
        let mut data = HashMap::with_capacity_and_hasher(elts, R::default());
        for _ in 0..elts {
            let k = <K as Pack>::decode(buf)?;
            let v = <V as Pack>::decode(buf)?;
            data.insert(k, v);
        }
        Ok(data)
    }

    fn decode_into(&mut self, buf: &mut impl Buf) -> Result<(), PackError> {
        let elts = decode_varint(buf)? as usize;
        for _ in 0..elts {
            let k = <K as Pack>::decode(buf)?;
            let v = <V as Pack>::decode(buf)?;
            self.insert(k, v);
        }
        Ok(())
    }
}

impl<T: Pack> Pack for Option<T> {
    fn encoded_len(&self) -> usize {
        1 + match self {
            None => 0,
            Some(v) => <T as Pack>::encoded_len(v),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        match self {
            None => Ok(buf.put_u8(0)),
            Some(v) => {
                buf.put_u8(1);
                <T as Pack>::encode(v, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        match buf.get_u8() {
            0 => Ok(None),
            1 => Ok(Some(<T as Pack>::decode(buf)?)),
            _ => return Err(PackError::UnknownTag),
        }
    }
}

impl<T: Pack, U: Pack> Pack for (T, U) {
    fn encoded_len(&self) -> usize {
        <T as Pack>::encoded_len(&self.0) + <U as Pack>::encoded_len(&self.1)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        <T as Pack>::encode(&self.0, buf)?;
        Ok(<U as Pack>::encode(&self.1, buf)?)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let fst = <T as Pack>::decode(buf)?;
        let snd = <U as Pack>::decode(buf)?;
        Ok((fst, snd))
    }
}

impl Pack for bool {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<bool>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<bool>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_u8(if *self { 1 } else { 0 }))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        match buf.get_u8() {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PackError::UnknownTag),
        }
    }
}

impl Pack for DateTime<Utc> {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<i64>() + mem::size_of::<u32>())
    }

    fn encoded_len(&self) -> usize {
        <DateTime<Utc> as Pack>::const_encoded_len().unwrap()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        buf.put_i64(self.timestamp());
        Ok(buf.put_u32(self.timestamp_subsec_nanos()))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let ts = buf.get_i64();
        let ns = buf.get_u32();
        let ndt = NaiveDateTime::from_timestamp_opt(ts, ns)
            .ok_or_else(|| PackError::InvalidFormat)?;
        Ok(DateTime::from_utc(ndt, Utc))
    }
}

impl Pack for Duration {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<i64>() + mem::size_of::<u32>())
    }

    fn encoded_len(&self) -> usize {
        <Duration as Pack>::const_encoded_len().unwrap()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        buf.put_u64(self.as_secs());
        Ok(buf.put_u32(self.subsec_nanos()))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let secs = buf.get_u64();
        let ns = buf.get_u32();
        Ok(Duration::new(secs, ns))
    }
}
