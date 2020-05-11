use bytes::{Bytes, BytesMut, Buf, BufMut};
use std::{
    cmp::Eq,
    collections::HashMap,
    error, fmt,
    hash::{BuildHasher, Hash},
    mem, net,
};

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
    fn len(&self) -> usize;
    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError>;
    fn decode(buf: &mut BytesMut) -> Result<Self, PackError>
    where
        Self: std::marker::Sized;
}

impl Pack for net::SocketAddr {
    fn len(&self) -> usize {
        match self {
            net::SocketAddr::V4(_) => 7,
            net::SocketAddr::V6(_) => 27,
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError> {
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

    fn decode(buf: &mut BytesMut) -> Result<Self, PackError> {
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
    fn len(&self) -> usize {
        Bytes::len(self) + mem::size_of::<u32>()
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError> {
        buf.put_u32(Bytes::len(self) as u32);
        Ok(buf.extend_from_slice(&*self))
    }

    fn decode(buf: &mut BytesMut) -> Result<Self, PackError> {
        let len = buf.get_u32();
        Ok(buf.split_to(len as usize).freeze())
    }
}

pub(crate) fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

pub(crate) fn encode_varint(mut value: u64, buf: &mut BytesMut) {
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

pub(crate) fn decode_varint(buf: &mut BytesMut) -> Result<u64, PackError> {
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

impl Pack for u64 {
    fn len(&self) -> usize {
        mem::size_of::<u64>()
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError> {
        Ok(buf.put_u64(*self))
    }

    fn decode(buf: &mut BytesMut) -> Result<Self, PackError> {
        Ok(buf.get_u64())
    }
}

impl Pack for u32 {
    fn len(&self) -> usize {
        mem::size_of::<u32>()
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError> {
        Ok(buf.put_u32(*self))
    }

    fn decode(buf: &mut BytesMut) -> Result<Self, PackError> {
        Ok(buf.get_u32())
    }
}

impl<T: Pack> Pack for Vec<T> {
    fn len(&self) -> usize {
        self.iter().fold(mem::size_of::<u32>(), |len, t| len + <T as Pack>::len(t))
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError> {
        buf.put_u32(Vec::len(self) as u32);
        for t in self {
            <T as Pack>::encode(t, buf)?
        }
        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> Result<Self, PackError> {
        let elts = buf.get_u32() as usize;
        let mut data = Vec::with_capacity(elts);
        for _ in 0..elts {
            data.push(<T as Pack>::decode(buf)?);
        }
        Ok(data)
    }
}

impl<K, V, R> Pack for HashMap<K, V, R>
where
    K: Pack + Hash + Eq,
    V: Pack + Hash + Eq,
    R: Default + BuildHasher,
{
    fn len(&self) -> usize {
        self.iter().fold(mem::size_of::<u32>(), |len, (k, v)| {
            len + <K as Pack>::len(k) + <V as Pack>::len(v)
        })
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError> {
        buf.put_u32(HashMap::len(self) as u32);
        for (k, v) in self {
            <K as Pack>::encode(k, buf)?;
            <V as Pack>::encode(v, buf)?;
        }
        Ok(())
    }

    fn decode(buf: &mut BytesMut) -> Result<Self, PackError> {
        let elts = buf.get_u32() as usize;
        let mut data = HashMap::with_capacity_and_hasher(elts, R::default());
        for _ in 0..elts {
            let k = <K as Pack>::decode(buf)?;
            let v = <V as Pack>::decode(buf)?;
            data.insert(k, v);
        }
        Ok(data)
    }
}

impl<T: Pack> Pack for Option<T> {
    fn len(&self) -> usize {
        1 + match self {
            None => 0,
            Some(v) => <T as Pack>::len(v),
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError> {
        match self {
            None => Ok(buf.put_u8(0)),
            Some(v) => {
                buf.put_u8(1);
                <T as Pack>::encode(v, buf)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self, PackError> {
        match buf.get_u8() {
            0 => Ok(None),
            1 => Ok(Some(<T as Pack>::decode(buf)?)),
            _ => return Err(PackError::UnknownTag),
        }
    }
}

impl<T: Pack, U: Pack> Pack for (T, U) {
    fn len(&self) -> usize {
        <T as Pack>::len(&self.0) + <U as Pack>::len(&self.1)
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError> {
        <T as Pack>::encode(&self.0, buf)?;
        Ok(<U as Pack>::encode(&self.1, buf)?)
    }

    fn decode(buf: &mut BytesMut) -> Result<Self, PackError> {
        let fst = <T as Pack>::decode(buf)?;
        let snd = <U as Pack>::decode(buf)?;
        Ok((fst, snd))
    }
}

impl Pack for bool {
    fn len(&self) -> usize {
        1
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError> {
        Ok(buf.put_u8(if *self { 1 } else { 0 }))
    }

    fn decode(buf: &mut BytesMut) -> Result<Self, PackError> {
        match buf.get_u8() {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PackError::UnknownTag),
        }
    }
}
