/// Why? Because Bytes is 5 words. This module export PBytes, which
/// reduces Bytes from 5 words to 2 words by wrapping it in a pooled
/// arc. This allows us to reduce the size of Value from 5 words to 3
/// words, while only paying the cost of a double indirection when
/// accessing a zero copy Bytes.

use bytes::Bytes;
use netidx_core::pool::{Pool, Poolable, Pooled};
use serde::{de::Visitor, Deserialize, Serialize};
use std::{mem, ops::Deref, sync::LazyLock};
use triomphe::Arc;

static POOL: LazyLock<Pool<ArcBytes>> = LazyLock::new(|| Pool::new(8124, 64));

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct ArcBytes(Arc<Bytes>);

impl Poolable for ArcBytes {
    fn capacity(&self) -> usize {
        1
    }

    fn empty() -> Self {
        Self(Arc::new(Bytes::new()))
    }

    fn really_dropped(&self) -> bool {
        Arc::is_unique(&self.0)
    }

    fn reset(&mut self) {
        // drop the contained bytes and replace it with an empty one
        let _: Bytes = mem::take(Arc::make_mut(&mut self.0));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PBytes(Pooled<ArcBytes>);

impl Deref for PBytes {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &*(self.0).0
    }
}

impl PBytes {
    pub fn new(b: Bytes) -> Self {
        let mut t = POOL.take();
        *Arc::make_mut(&mut t.0) = b;
        Self(t)
    }
}

impl From<Bytes> for PBytes {
    fn from(value: Bytes) -> Self {
        Self::new(value)
    }
}

impl Into<Bytes> for PBytes {
    fn into(mut self) -> Bytes {
        mem::take(Arc::make_mut(&mut (self.0).0))
    }
}

impl Serialize for PBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes: &Bytes = &*(self.0).0;
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
