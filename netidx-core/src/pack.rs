use crate::{
    pool::{Poolable, Pooled},
    utils::take_t,
};
use arcstr::ArcStr;
use bytes::{buf, Buf, BufMut, Bytes, BytesMut};
use chrono::{naive::NaiveDateTime, prelude::*};
use indexmap::{IndexMap, IndexSet};
use rust_decimal::Decimal;
use std::{
    any::Any,
    boxed::Box,
    cell::RefCell,
    cmp::Eq,
    collections::{HashMap, HashSet, VecDeque},
    default::Default,
    error, fmt,
    hash::{BuildHasher, Hash},
    mem, net,
    ops::{Deref, DerefMut},
    result, str,
    sync::Arc,
    time::Duration,
};

#[derive(Debug, Clone, Copy)]
pub enum PackError {
    UnknownTag,
    TooBig,
    InvalidFormat,
    BufferShort,
    Application(u64),
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

impl<T: Pack + Any + Send + Sync + Poolable> Pack for Pooled<T> {
    fn encoded_len(&self) -> usize {
        <T as Pack>::encoded_len(&**self)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        <T as Pack>::encode(&**self, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let mut t = take_t::<T>(10000, 10000);
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
        match <u8 as Pack>::decode(buf)? {
            0 => {
                let ip =
                    net::Ipv4Addr::from(u32::to_be_bytes(<u32 as Pack>::decode(buf)?));
                let port = <u16 as Pack>::decode(buf)?;
                Ok(net::SocketAddr::V4(net::SocketAddrV4::new(ip, port)))
            }
            1 => {
                let mut segments = [0u16; 8];
                for i in 0..8 {
                    segments[i] = <u16 as Pack>::decode(buf)?;
                }
                let port = <u16 as Pack>::decode(buf)?;
                let flowinfo = <u32 as Pack>::decode(buf)?;
                let scope_id = <u32 as Pack>::decode(buf)?;
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

#[derive(Debug)]
pub struct BoundedBytes<const L: usize>(pub Bytes);

impl<const L: usize> Deref for BoundedBytes<L> {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const L: usize> DerefMut for BoundedBytes<L> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<const L: usize> Pack for BoundedBytes<L> {
    fn encoded_len(&self) -> usize {
        let len = Bytes::len(&self.0);
        varint_len(len as u64) + len
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        if Bytes::len(self) > L {
            Err(PackError::TooBig)
        } else {
            <Bytes as Pack>::encode(self, buf)
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let len = decode_varint(buf)? as usize;
        if len > L || len > buf.remaining() {
            Err(PackError::TooBig)
        } else {
            Ok(BoundedBytes(buf.copy_to_bytes(len)))
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
            return Err(PackError::TooBig);
        }
        let mut v = vec![0; len];
        buf.copy_to_slice(&mut v);
        match String::from_utf8(v) {
            Ok(s) => Ok(s),
            Err(_) => Err(PackError::InvalidFormat),
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
            return Err(PackError::TooBig);
        }
        let mut v = vec![0; len];
        buf.copy_to_slice(&mut v);
        match String::from_utf8(v) {
            Ok(s) => Ok(Arc::from(s)),
            Err(_) => Err(PackError::InvalidFormat),
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
                Err(_) => Err(PackError::InvalidFormat),
            };
            buf.advance(len);
            res
        }
    }
}

pub fn varint_len(value: u64) -> usize {
    ((((value | 1).leading_zeros() ^ 63) * 9 + 73) >> 6) as usize
}

pub fn encode_varint(mut value: u64, buf: &mut impl BufMut) {
    for _ in 0..10 {
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
    let bytes = buf.chunk();
    let mut value = 0;
    for i in 0..10 {
	if i >= bytes.len() {
	    return Err(PackError::BufferShort);
	}
        let byte = bytes[i];
        value |= u64::from(byte & 0x7F) << (i * 7);
        if byte <= 0x7F {
	    buf.advance(i + 1);
            return Ok(value);
        }
    }
    buf.advance(10);
    Err(PackError::InvalidFormat)
}

pub fn len_wrapped_len(len: usize) -> usize {
    let vlen = varint_len((len + varint_len(len as u64)) as u64);
    len + vlen
}

pub fn len_wrapped_encode<B, T, F>(buf: &mut B, t: &T, f: F) -> Result<(), PackError>
where
    B: BufMut,
    T: Pack,
    F: FnOnce(&mut B) -> Result<(), PackError>,
{
    encode_varint(t.encoded_len() as u64, buf);
    f(buf)
}

pub fn len_wrapped_decode<B, T, F>(buf: &mut B, f: F) -> Result<T, PackError>
where
    B: Buf,
    T: Pack + 'static,
    F: FnOnce(&mut buf::Take<&mut B>) -> Result<T, PackError>,
{
    let len = decode_varint(buf)?;
    if len < 1 {
        return Err(PackError::BufferShort);
    }
    let mut limited = buf.take((len - varint_len(len) as u64) as usize);
    let r = f(&mut limited);
    if limited.has_remaining() {
        limited.advance(limited.remaining());
    }
    r
}

impl Pack for f32 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<f32>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<f32>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_f32(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < mem::size_of::<f32>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_f32())
        }
    }
}

impl Pack for f64 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<f64>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<f64>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_f64(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < mem::size_of::<f64>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_f64())
        }
    }
}

impl Pack for Decimal {
    fn const_encoded_len() -> Option<usize> {
        Some(16)
    }

    fn encoded_len(&self) -> usize {
        Self::const_encoded_len().unwrap()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_slice(&self.serialize()[..]))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < Self::const_encoded_len().unwrap() {
            Err(PackError::BufferShort)
        } else {
            let mut b = [0u8; 16];
            buf.copy_to_slice(&mut b);
            Ok(Decimal::deserialize(b))
        }
    }
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
        if buf.remaining() < mem::size_of::<u128>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_u128())
        }
    }
}

impl Pack for i128 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<i128>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<i128>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_i128(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < mem::size_of::<i128>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_i128())
        }
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
        if buf.remaining() < mem::size_of::<u64>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_u64())
        }
    }
}

impl Pack for i64 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<i64>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<i64>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_i64(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < mem::size_of::<i64>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_i64())
        }
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
        if buf.remaining() < mem::size_of::<u32>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_u32())
        }
    }
}

impl Pack for i32 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<i32>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<i32>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_i32(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < mem::size_of::<i32>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_i32())
        }
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
        if buf.remaining() < mem::size_of::<u16>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_u16())
        }
    }
}

impl Pack for i16 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<i16>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<i16>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_i16(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < mem::size_of::<i16>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_i16())
        }
    }
}

impl Pack for u8 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<u8>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<u8>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_u8(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < mem::size_of::<u8>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_u8())
        }
    }
}

impl Pack for i8 {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<i8>())
    }

    fn encoded_len(&self) -> usize {
        mem::size_of::<i8>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(buf.put_i8(*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        if buf.remaining() < mem::size_of::<i8>() {
            Err(PackError::BufferShort)
        } else {
            Ok(buf.get_i8())
        }
    }
}

const MAX_VEC: usize = 2 * 1024 * 1024 * 1024;

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
        if elts > MAX_VEC {
            return Err(PackError::TooBig);
        }
        let pre = if elts * mem::size_of::<T>() > MAX_VEC {
            MAX_VEC / mem::size_of::<T>()
        } else {
            elts
        };
        let mut data = Vec::with_capacity(pre);
        for _ in 0..elts {
            data.push(<T as Pack>::decode(buf)?);
        }
        Ok(data)
    }

    fn decode_into(&mut self, buf: &mut impl Buf) -> Result<(), PackError> {
        let elts = decode_varint(buf)? as usize;
        if elts > MAX_VEC {
            return Err(PackError::TooBig);
        }
        let pre = if elts * mem::size_of::<T>() > MAX_VEC {
            MAX_VEC / mem::size_of::<T>()
        } else {
            elts
        };
        if pre > self.capacity() {
            self.reserve(pre - self.capacity());
        }
        for _ in 0..elts {
            self.push(<T as Pack>::decode(buf)?);
        }
        Ok(())
    }
}

impl<T: Pack> Pack for VecDeque<T> {
    fn encoded_len(&self) -> usize {
        self.iter().fold(varint_len(VecDeque::len(self) as u64), |len, t| {
            len + <T as Pack>::encoded_len(t)
        })
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        encode_varint(VecDeque::len(self) as u64, buf);
        for t in self {
            <T as Pack>::encode(t, buf)?
        }
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let elts = decode_varint(buf)? as usize;
        if elts > MAX_VEC {
            return Err(PackError::TooBig);
        }
        let pre = if elts * mem::size_of::<T>() > MAX_VEC {
            MAX_VEC / mem::size_of::<T>()
        } else {
            elts
        };
        let mut data = VecDeque::with_capacity(pre);
        for _ in 0..elts {
            data.push_back(<T as Pack>::decode(buf)?);
        }
        Ok(data)
    }

    fn decode_into(&mut self, buf: &mut impl Buf) -> Result<(), PackError> {
        let elts = decode_varint(buf)? as usize;
        if elts > MAX_VEC {
            return Err(PackError::TooBig);
        }
        let pre = if elts * mem::size_of::<T>() > MAX_VEC {
            MAX_VEC / mem::size_of::<T>()
        } else {
            elts
        };
        if pre > self.capacity() {
            self.reserve(pre - self.capacity());
        }
        for _ in 0..elts {
            self.push_back(<T as Pack>::decode(buf)?);
        }
        Ok(())
    }
}

macro_rules! impl_hashmap {
    ($ty:ident) => {
	impl<K, V, R> Pack for $ty<K, V, R>
	where
	    K: Pack + Hash + Eq,
	    V: Pack,
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
		if elts > MAX_VEC {
		    return Err(PackError::TooBig);
		}
		let pre = if elts * (mem::size_of::<K>() + mem::size_of::<V>()) > MAX_VEC {
		    MAX_VEC / (mem::size_of::<K>() + mem::size_of::<V>())
		} else {
		    elts
		};
		let mut data = $ty::with_capacity_and_hasher(pre, R::default());
		for _ in 0..elts {
		    let k = <K as Pack>::decode(buf)?;
		    let v = <V as Pack>::decode(buf)?;
		    data.insert(k, v);
		}
		Ok(data)
	    }

	    fn decode_into(&mut self, buf: &mut impl Buf) -> Result<(), PackError> {
		let elts = decode_varint(buf)? as usize;
		if elts > MAX_VEC {
		    return Err(PackError::TooBig);
		}
		let pre = if elts * (mem::size_of::<K>() + mem::size_of::<V>()) > MAX_VEC {
		    MAX_VEC / (mem::size_of::<K>() + mem::size_of::<V>())
		} else {
		    elts
		};
		if pre > self.capacity() {
		    self.reserve(pre - self.capacity());
		}
		for _ in 0..elts {
		    let k = <K as Pack>::decode(buf)?;
		    let v = <V as Pack>::decode(buf)?;
		    self.insert(k, v);
		}
		Ok(())
	    }
	}
    }
}

impl_hashmap!(HashMap);
impl_hashmap!(IndexMap);

macro_rules! impl_hashset {
    ($ty:ident) => {
	impl<K, R> Pack for $ty<K, R>
	where
	    K: Pack + Hash + Eq,
	    R: Default + BuildHasher,
	{
	    fn encoded_len(&self) -> usize {
		self.iter().fold(varint_len(self.len() as u64), |len, k| {
		    len + <K as Pack>::encoded_len(k)
		})
	    }

	    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
		encode_varint(self.len() as u64, buf);
		for k in self {
		    <K as Pack>::encode(k, buf)?;
		}
		Ok(())
	    }

	    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
		let elts = decode_varint(buf)? as usize;
		if elts > MAX_VEC {
		    return Err(PackError::TooBig);
		}
		let pre = if elts * mem::size_of::<K>() > MAX_VEC {
		    MAX_VEC / mem::size_of::<K>()
		} else {
		    elts
		};
		let mut data = $ty::with_capacity_and_hasher(pre, R::default());
		for _ in 0..elts {
		    data.insert(<K as Pack>::decode(buf)?);
		}
		Ok(data)
	    }

	    fn decode_into(&mut self, buf: &mut impl Buf) -> Result<(), PackError> {
		let elts = decode_varint(buf)? as usize;
		if elts > MAX_VEC {
		    return Err(PackError::TooBig);
		}
		let pre = if elts * mem::size_of::<K>() > MAX_VEC {
		    MAX_VEC / mem::size_of::<K>()
		} else {
		    elts
		};
		if pre > self.capacity() {
		    self.reserve(pre - self.capacity());
		}
		for _ in 0..elts {
		    self.insert(<K as Pack>::decode(buf)?);
		}
		Ok(())
	    }
	}
    }
}

impl_hashset!(HashSet);
impl_hashset!(IndexSet);

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
        match <u8 as Pack>::decode(buf)? {
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

impl<T: Pack, U: Pack, V: Pack> Pack for (T, U, V) {
    fn encoded_len(&self) -> usize {
        <T as Pack>::encoded_len(&self.0)
            + <U as Pack>::encoded_len(&self.1)
            + <V as Pack>::encoded_len(&self.2)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        <T as Pack>::encode(&self.0, buf)?;
        <U as Pack>::encode(&self.1, buf)?;
        Ok(<V as Pack>::encode(&self.2, buf)?)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let fst = <T as Pack>::decode(buf)?;
        let snd = <U as Pack>::decode(buf)?;
        let trd = <V as Pack>::decode(buf)?;
        Ok((fst, snd, trd))
    }
}

impl<T: Pack, U: Pack, V: Pack, W: Pack> Pack for (T, U, V, W) {
    fn encoded_len(&self) -> usize {
        <T as Pack>::encoded_len(&self.0)
            + <U as Pack>::encoded_len(&self.1)
            + <V as Pack>::encoded_len(&self.2)
            + <W as Pack>::encoded_len(&self.3)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        <T as Pack>::encode(&self.0, buf)?;
        <U as Pack>::encode(&self.1, buf)?;
        <V as Pack>::encode(&self.2, buf)?;
        Ok(<W as Pack>::encode(&self.3, buf)?)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let fst = <T as Pack>::decode(buf)?;
        let snd = <U as Pack>::decode(buf)?;
        let trd = <V as Pack>::decode(buf)?;
        let fth = <W as Pack>::decode(buf)?;
        Ok((fst, snd, trd, fth))
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
        match <u8 as Pack>::decode(buf)? {
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
        let ts = Pack::decode(buf)?;
        let ns = Pack::decode(buf)?;
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
        let secs = Pack::decode(buf)?;
        let ns = Pack::decode(buf)?;
        Ok(Duration::new(secs, ns))
    }
}

impl Pack for chrono::Duration {
    fn encoded_len(&self) -> usize {
        mem::size_of::<i64>()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        match self.num_nanoseconds() {
            Some(i) => Pack::encode(&i, buf),
            None => Err(PackError::InvalidFormat),
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(chrono::Duration::nanoseconds(Pack::decode(buf)?))
    }
}

impl Pack for () {
    fn const_encoded_len() -> Option<usize> {
        Some(0)
    }

    fn encoded_len(&self) -> usize {
        <() as Pack>::const_encoded_len().unwrap()
    }

    fn encode(&self, _buf: &mut impl BufMut) -> Result<(), PackError> {
        Ok(())
    }

    fn decode(_buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(())
    }
}

impl Pack for uuid::Uuid {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<u128>())
    }

    fn encoded_len(&self) -> usize {
        Self::const_encoded_len().unwrap()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Pack::encode(&self.as_u128(), buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(uuid::Uuid::from_u128(Pack::decode(buf)?))
    }
}

impl Pack for usize {
    fn const_encoded_len() -> Option<usize> {
        Some(mem::size_of::<u64>())
    }

    fn encoded_len(&self) -> usize {
        Self::const_encoded_len().unwrap()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        <u64 as Pack>::encode(&(*self as u64), buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(<u64 as Pack>::decode(buf)? as usize)
    }
}

impl<T: Pack, U: Pack> Pack for result::Result<T, U> {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Ok(t) => Pack::encoded_len(t),
            Err(u) => Pack::encoded_len(u),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        match self {
            Ok(t) => {
                <u8 as Pack>::encode(&0, buf)?;
                Pack::encode(t, buf)
            }
            Err(u) => {
                <u8 as Pack>::encode(&1, buf)?;
                Pack::encode(u, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(Ok(Pack::decode(buf)?)),
            1 => Ok(Err(Pack::decode(buf)?)),
            _ => Err(PackError::UnknownTag),
        }
    }
}

impl<T: Pack> Pack for Arc<T> {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&**self)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Pack::encode(&**self, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(Arc::new(Pack::decode(buf)?))
    }
}

thread_local! {
    static ERR: RefCell<BytesMut> = RefCell::new(BytesMut::new());
}

fn write_anyhow(s: &mut BytesMut, e: &anyhow::Error) {
    use std::fmt::Write;
    s.clear();
    let _ = write!(s, "{}", e);
}

// this won't round trip to exactly the same object
impl Pack for anyhow::Error {
    fn encoded_len(&self) -> usize {
        ERR.with(|s| {
            let mut s = s.borrow_mut();
            write_anyhow(&mut *s, self);
            let len = s.len();
            varint_len(len as u64) + len
        })
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        ERR.with(|s| {
            let mut s = s.borrow_mut();
            write_anyhow(&mut *s, self);
            let len = s.len();
            encode_varint(len as u64, buf);
            Ok(buf.put_slice(&*s))
        })
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let len = decode_varint(buf)? as usize;
        if len > buf.remaining() {
            Err(PackError::BufferShort)
        } else {
            let s = match crate::chars::Chars::from_bytes(buf.copy_to_bytes(len)) {
                Ok(s) => s,
                Err(_) => return Err(PackError::InvalidFormat),
            };
            Ok(anyhow::anyhow!(s))
        }
    }
}

impl<T: Pack> Pack for Box<T> {
    fn encoded_len(&self) -> usize {
        Pack::encoded_len(&**self)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Pack::encode(&**self, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(Box::new(Pack::decode(buf)?))
    }
}

use smallvec::{Array, SmallVec};

impl<A> Pack for SmallVec<A>
where
    A: Array,
    <A as Array>::Item: Pack,
{
    fn encoded_len(&self) -> usize {
        self.iter().fold(varint_len(SmallVec::len(self) as u64), |len, t| {
            len + Pack::encoded_len(t)
        })
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        encode_varint(SmallVec::len(self) as u64, buf);
        for t in self {
            Pack::encode(t, buf)?
        }
        Ok(())
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let elts = decode_varint(buf)? as usize;
        let mut data = SmallVec::new();
        for _ in 0..elts {
            data.push(Pack::decode(buf)?);
        }
        Ok(data)
    }
}

use enumflags2::{BitFlag, BitFlags, _internal::RawBitFlags};

impl<T> Pack for BitFlags<T>
where
    T: BitFlag,
    <T as RawBitFlags>::Numeric: Pack,
{
    fn encoded_len(&self) -> usize {
        <<T as RawBitFlags>::Numeric as Pack>::encoded_len(&self.bits())
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        <<T as RawBitFlags>::Numeric as Pack>::encode(&self.bits(), buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let bits = <<T as RawBitFlags>::Numeric as Pack>::decode(buf)?;
        Self::from_bits(bits).map_err(|_| PackError::InvalidFormat)
    }
}

use std::ops::Bound;

impl<T: Pack> Pack for Bound<T> {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Bound::Excluded(t) | Bound::Included(t) => Pack::encoded_len(t),
            Bound::Unbounded => 0,
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        match self {
            Bound::Unbounded => <u8 as Pack>::encode(&0, buf),
            Bound::Excluded(t) => {
                <u8 as Pack>::encode(&1, buf)?;
                Pack::encode(t, buf)
            }
            Bound::Included(t) => {
                <u8 as Pack>::encode(&2, buf)?;
                Pack::encode(t, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(Bound::Unbounded),
            1 => Ok(Bound::Excluded(Pack::decode(buf)?)),
            2 => Ok(Bound::Included(Pack::decode(buf)?)),
            _ => Err(PackError::UnknownTag),
        }
    }
}
