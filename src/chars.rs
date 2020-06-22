use crate::pack::{Pack, PackError};
use bytes::{Bytes, BytesMut};
use std::{
    cmp::{Eq, Ord, Ordering, PartialEq, PartialOrd},
    convert::AsRef,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    str,
};

/// This is a thin wrapper around a Bytes that guarantees that it's contents are
/// well formed unicode.
#[derive(Clone)]
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

impl PartialEq for Chars {
    fn eq(&self, other: &Chars) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for Chars {}

impl PartialOrd for Chars {
    fn partial_cmp(&self, other: &Chars) -> Option<Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}

impl Ord for Chars {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl Hash for Chars {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}

impl AsRef<str> for Chars {
    fn as_ref(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&self.0) }
    }
}

impl Pack for Chars {
    fn len(&self) -> usize {
        Pack::len(&self.0)
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<(), PackError> {
        Pack::encode(&self.0, buf)
    }

    fn decode(buf: &mut BytesMut) -> Result<Self, PackError> {
        match Chars::from_bytes(<Bytes as Pack>::decode(buf)?) {
            Ok(c) => Ok(c),
            Err(_) => Err(PackError::InvalidFormat),
        }
    }

    fn decode_into(&mut self, buf: &mut BytesMut) -> Result<(), PackError> {
        Ok(*self = <Chars as Pack>::decode(buf)?)
    }
}

impl From<&'static str> for Chars {
    fn from(src: &'static str) -> Chars {
        Chars(Bytes::from(src.as_bytes()))
    }
}

impl From<String> for Chars {
    fn from(src: String) -> Chars {
        Chars(Bytes::from(src))
    }
}

impl Into<String> for &Chars {
    fn into(self) -> String {
        self.as_ref().into()
    }
}

impl Into<String> for Chars {
    fn into(self) -> String {
        self.as_ref().into()
    }
}

impl Deref for Chars {
    type Target = str;

    fn deref(&self) -> &str {
        unsafe { str::from_utf8_unchecked(&*self.0) }
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
