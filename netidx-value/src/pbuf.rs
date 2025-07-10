/// Why? Because Bytes is 5 words. This module export PBytes, which
/// reduces Bytes from 5 words to 1 words by wrapping it in a pooled
/// arc. This allows us to reduce the size of Value from 5 words to 3
/// words, while only paying the cost of a double indirection when
/// accessing a zero copy Bytes.
use bytes::{Buf, BufMut, Bytes};
use netidx_core::{
    pack::{decode_varint, encode_varint, varint_len, Pack, PackError},
    pool::{pooled::PArc, RawPool},
};
use serde::{de::Visitor, Deserialize, Serialize};
use std::{borrow::Borrow, mem, ops::Deref, sync::LazyLock};

static POOL: LazyLock<RawPool<PArc<Bytes>>> = LazyLock::new(|| RawPool::new(8124, 64));

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PBytes(PArc<Bytes>);

impl Deref for PBytes {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl Borrow<Bytes> for PBytes {
    fn borrow(&self) -> &Bytes {
        &*self
    }
}

impl Borrow<[u8]> for PBytes {
    fn borrow(&self) -> &[u8] {
        &**self
    }
}

impl AsRef<[u8]> for PBytes {
    fn as_ref(&self) -> &[u8] {
        &**self
    }
}

impl PBytes {
    pub fn new(b: Bytes) -> Self {
        Self(PArc::new(&POOL, b))
    }
}

impl From<Bytes> for PBytes {
    fn from(value: Bytes) -> Self {
        Self::new(value)
    }
}

impl Into<Bytes> for PBytes {
    fn into(mut self) -> Bytes {
        match PArc::get_mut(&mut self.0) {
            Some(b) => mem::take(b),
            None => (*self.0).clone(),
        }
    }
}

impl Serialize for PBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes: &Bytes = &*self.0;
        bytes.serialize(serializer)
    }
}

struct PBytesVisitor;

impl<'de> Visitor<'de> for PBytesVisitor {
    type Value = PBytes;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "expecting a bytes")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(PBytes::new(Bytes::copy_from_slice(v)))
    }
}

impl<'de> Deserialize<'de> for PBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(PBytesVisitor)
    }
}

impl Pack for PBytes {
    fn encoded_len(&self) -> usize {
        let len = self.len();
        varint_len(len as u64) + len
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        encode_varint(self.len() as u64, buf);
        Ok(buf.put_slice(&*self))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let len = decode_varint(buf)?;
        if len as usize > buf.remaining() {
            Err(PackError::TooBig)
        } else {
            Ok(Self::from(buf.copy_to_bytes(len as usize)))
        }
    }
}
