use crate::{
    chars::Chars,
    path::Path,
    protocol::resolver::ResolverId,
    utils::{Pack, PackError},
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{mem, result};

type Result<T> = result::Result<T, PackError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(u64);

impl Id {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Id(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    #[cfg(test)]
    pub(crate) fn mk(i: u64) -> Self {
        Id(i)
    }
}

impl Pack for Id {
    fn len(&self) -> usize {
        <u64 as Pack>::len(&self.0)
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        <u64 as Pack>::encode(&self.0, buf)
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        Ok(Id(<u64 as Pack>::decode(buf)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hello {
    /// No authentication will be provided. The publisher may drop
    /// the connection at this point, if it chooses to allow this
    /// then it will return Anonymous.
    Anonymous,
    /// An authentication token, if the token is valid then the
    /// publisher will send a token back to authenticate itself to
    /// the subscriber.
    Token(Bytes),
    /// In order to prevent denial of service, spoofing, etc,
    /// authenticated publishers must prove that they are actually
    /// listening on the socket they claim to be listening on. To
    /// facilitate this, after a new security context has been
    /// created the resolver server will encrypt a random number
    /// with it, connect to the write address specified by the
    /// publisher, and send the encrypted token. The publisher
    /// must decrypt the token using it's end of the security
    /// context, add 1 to the number, encrypt it again and send it
    /// back. If that round trip succeeds then the new security
    /// context will replace any old one, if it fails the new
    /// context will be thrown away and the old one will continue
    /// to be associated with the write address.
    ResolverAuthenticate(ResolverId, Bytes),
}

impl Pack for Hello {
    fn len(&self) -> usize {
        1 + match self {
            Hello::Anonymous => 0,
            Hello::Token(tok) => <Bytes as Pack>::len(tok),
            Hello::ResolverAuthenticate(id, tok) => {
                ResolverId::len(id) + <Bytes as Pack>::len(tok)
            }
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            Hello::Anonymous => Ok(buf.put_u8(0)),
            Hello::Token(tok) => {
                buf.put_u8(1);
                Ok(<Bytes as Pack>::encode(tok, buf)?)
            }
            Hello::ResolverAuthenticate(id, tok) => {
                buf.put_u8(2);
                ResolverId::encode(id, buf)?;
                Ok(<Bytes as Pack>::encode(tok, buf)?)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(Hello::Anonymous),
            1 => Ok(Hello::Token(<Bytes as Pack>::decode(buf)?)),
            2 => {
                let id = ResolverId::decode(buf)?;
                let tok = <Bytes as Pack>::decode(buf)?;
                Ok(Hello::ResolverAuthenticate(id, tok))
            }
            _ => Err(PackError::UnknownTag),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum To {
    /// Subscribe to the specified value, if it is not available
    /// the result will be NoSuchValue. The optional security
    /// token is a proof from the resolver server that this
    /// subscription is permitted. In the case of an anonymous
    /// connection this proof will be empty.
    Subscribe { path: Path, resolver: ResolverId, token: Bytes },
    /// Unsubscribe from the specified value, this will always result
    /// in an Unsubscibed message even if you weren't ever subscribed
    /// to the value, or it doesn't exist.
    Unsubscribe(Id),
}

impl Pack for To {
    fn len(&self) -> usize {
        1 + match self {
            To::Subscribe { path, resolver, token } => {
                <Path as Pack>::len(path)
                    + ResolverId::len(resolver)
                    + <Bytes as Pack>::len(token)
            }
            To::Unsubscribe(id) => Id::len(id),
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> anyhow::Result<(), PackError> {
        match self {
            To::Subscribe { path, resolver, token } => {
                buf.put_u8(0);
                <Path as Pack>::encode(path, buf)?;
                ResolverId::encode(resolver, buf)?;
                Ok(<Bytes as Pack>::encode(token, buf)?)
            }
            To::Unsubscribe(id) => {
                buf.put_u8(1);
                Ok(Id::encode(id, buf)?)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> anyhow::Result<Self, PackError> {
        match buf.get_u8() {
            0 => {
                let path = <Path as Pack>::decode(buf)?;
                let resolver = ResolverId::decode(buf)?;
                let token = <Bytes as Pack>::decode(buf)?;
                Ok(To::Subscribe { path, resolver, token })
            }
            1 => Ok(To::Unsubscribe(Id::decode(buf)?)),
            _ => Err(PackError::UnknownTag),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    F32(f32),
    F64(f64),
    String(Chars),
    Bytes(Bytes),
}

impl Pack for Value {
    fn len(&self) -> usize {
        1 + match self {
            Value::U32(_) => mem::size_of::<u32>(),
            Value::I32(_) => mem::size_of::<i32>(),
            Value::U64(_) => mem::size_of::<u64>(),
            Value::I64(_) => mem::size_of::<i64>(),
            Value::F32(_) => mem::size_of::<f32>(),
            Value::F64(_) => mem::size_of::<f64>(),
            Value::String(c) => <Chars as Pack>::len(c),
            Value::Bytes(b) => <Bytes as Pack>::len(b),
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            Value::U32(i) => {
                buf.put_u8(0);
                Ok(buf.put_u32(*i))
            }
            Value::I32(i) => {
                buf.put_u8(1);
                Ok(buf.put_i32(*i))
            }
            Value::U64(i) => {
                buf.put_u8(2);
                Ok(buf.put_u64(*i))
            }
            Value::I64(i) => {
                buf.put_u8(3);
                Ok(buf.put_i64(*i))
            }
            Value::F32(i) => {
                buf.put_u8(4);
                Ok(buf.put_f32(*i))
            }
            Value::F64(i) => {
                buf.put_u8(5);
                Ok(buf.put_f64(*i))
            }
            Value::String(s) => {
                buf.put_u8(6);
                Ok(<Chars as Pack>::encode(s, buf)?)
            }
            Value::Bytes(b) => {
                buf.put_u8(7);
                Ok(<Bytes as Pack>::encode(b, buf)?)
            }
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(Value::U32(buf.get_u32())),
            1 => Ok(Value::I32(buf.get_i32())),
            2 => Ok(Value::U64(buf.get_u64())),
            3 => Ok(Value::I64(buf.get_i64())),
            4 => Ok(Value::F32(buf.get_f32())),
            5 => Ok(Value::F64(buf.get_f64())),
            6 => Ok(Value::String(<Chars as Pack>::decode(buf)?)),
            7 => Ok(Value::Bytes(<Bytes as Pack>::decode(buf)?)),
            _ => Err(PackError::UnknownTag),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum From {
    /// The requested subscription to Path cannot be completed because
    /// it doesn't exist
    NoSuchValue(Path),
    /// Permission to subscribe to the specified path is denied.
    Denied(Path),
    /// You have been unsubscriped from Path. This can be the result
    /// of an Unsubscribe message, or it may be sent unsolicited, in
    /// the case the value is no longer published, or the publisher is
    /// in the process of shutting down.
    Unsubscribed(Id),
    /// You are now subscribed to Path with subscription id `Id`, and
    /// The next message contains the first value for Id. All further
    /// communications about this subscription will only refer to the
    /// Id.
    Subscribed(Path, Id, Value),
    /// A value update to Id
    Update(Id, Value),
    /// Indicates that the publisher is idle, but still
    /// functioning correctly.
    Heartbeat,
}

impl Pack for From {
    fn len(&self) -> usize {
        1 + match self {
            From::NoSuchValue(p) => <Path as Pack>::len(p),
            From::Denied(p) => <Path as Pack>::len(p),
            From::Unsubscribed(id) => Id::len(id),
            From::Subscribed(p, id, v) => {
                <Path as Pack>::len(p) + Id::len(id) + Value::len(v)
            }
            From::Update(id, v) => Id::len(id) + Value::len(v),
            From::Heartbeat => 0,
        }
    }

    fn encode(&self, buf: &mut BytesMut) -> Result<()> {
        match self {
            From::NoSuchValue(p) => {
                buf.put_u8(0);
                Ok(<Path as Pack>::encode(p, buf)?)
            }
            From::Denied(p) => {
                buf.put_u8(1);
                Ok(<Path as Pack>::encode(p, buf)?)
            }
            From::Unsubscribed(id) => {
                buf.put_u8(2);
                Ok(Id::encode(id, buf)?)
            }
            From::Subscribed(p, id, v) => {
                buf.put_u8(3);
                <Path as Pack>::encode(p, buf)?;
                Id::encode(id, buf)?;
                Ok(Value::encode(v, buf)?)
            }
            From::Update(id, v) => {
                buf.put_u8(4);
                Id::encode(id, buf)?;
                Ok(Value::encode(v, buf)?)
            }
            From::Heartbeat => Ok(buf.put_u8(5)),
        }
    }

    fn decode(buf: &mut BytesMut) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(From::NoSuchValue(<Path as Pack>::decode(buf)?)),
            1 => Ok(From::Denied(<Path as Pack>::decode(buf)?)),
            2 => Ok(From::Unsubscribed(Id::decode(buf)?)),
            3 => {
                let path = <Path as Pack>::decode(buf)?;
                let id = Id::decode(buf)?;
                let v = Value::decode(buf)?;
                Ok(From::Subscribed(path, id, v))
            }
            4 => {
                let id = Id::decode(buf)?;
                let value = Value::decode(buf)?;
                Ok(From::Update(id, value))
            }
            5 => Ok(From::Heartbeat),
            _ => Err(PackError::UnknownTag),
        }
    }
}
