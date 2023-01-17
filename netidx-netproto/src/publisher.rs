use crate::value::Value;
use bytes::{Buf, BufMut, Bytes};
use netidx_core::{
    pack::{
        self, len_wrapped_decode, len_wrapped_encode, len_wrapped_len, Pack, PackError,
    },
    path::Path,
};
use std::{net::SocketAddr, result};

type Result<T> = result::Result<T, PackError>;

atomic_id!(Id);

impl Pack for Id {
    fn encoded_len(&self) -> usize {
        pack::varint_len(self.0)
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        Ok(pack::encode_varint(self.0, buf))
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        Ok(Id(pack::decode_varint(buf)?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hello {
    /// No authentication will be provided. The publisher may drop
    /// the connection at this point, if it chooses to allow this
    /// then it will return Anonymous.
    Anonymous,
    /// Authenticate using kerberos 5, following the hello, the
    /// subscriber and publisher will exchange tokens to complete the
    /// authentication.
    Krb5,
    /// Authenticate using a local unix socket, only valid for
    /// publishers on the same machine as the subscriber.
    Local,
    /// Authenticate using transport layer security. In this case both
    /// the server AND the client must have certificates that are
    /// signed by a CA they mutually trust.
    Tls,
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
    ResolverAuthenticate(SocketAddr),
}

impl Pack for Hello {
    fn encoded_len(&self) -> usize {
        len_wrapped_len(
            1 + match self {
                Hello::Anonymous | Hello::Krb5 | Hello::Local | Hello::Tls => 0,
                Hello::ResolverAuthenticate(addr) => Pack::encoded_len(addr),
            },
        )
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        len_wrapped_encode(buf, self, |buf| match self {
            Hello::Anonymous => Ok(buf.put_u8(0)),
            Hello::Krb5 => Ok(buf.put_u8(1)),
            Hello::Local => Ok(buf.put_u8(2)),
            Hello::ResolverAuthenticate(id) => {
                buf.put_u8(3);
                Pack::encode(id, buf)
            }
            Hello::Tls => Ok(buf.put_u8(4)),
        })
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        len_wrapped_decode(buf, |buf| match <u8 as Pack>::decode(buf)? {
            0 => Ok(Hello::Anonymous),
            1 => Ok(Hello::Krb5),
            2 => Ok(Hello::Local),
            3 => Ok(Hello::ResolverAuthenticate(Pack::decode(buf)?)),
            4 => Ok(Hello::Tls),
            _ => Err(PackError::UnknownTag),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum To {
    /// Subscribe to the specified value, if it is not available
    /// the result will be NoSuchValue. The optional security
    /// token is a proof from the resolver server that this
    /// subscription is permitted. In the case of an anonymous
    /// connection this proof will be empty.
    Subscribe {
        path: Path,
        resolver: SocketAddr,
        timestamp: u64,
        permissions: u32,
        token: Bytes,
    },
    /// Unsubscribe from the specified value, this will always result
    /// in an Unsubscribed message even if you weren't ever subscribed
    /// to the value, or it doesn't exist.
    Unsubscribe(Id),
    /// Send a write to the specified value.
    Write(Id, Value, bool),
}

impl Pack for To {
    fn encoded_len(&self) -> usize {
        len_wrapped_len(
            1 + match self {
                To::Subscribe { path, resolver, timestamp, permissions, token } => {
                    Pack::encoded_len(path)
                        + Pack::encoded_len(resolver)
                        + Pack::encoded_len(timestamp)
                        + Pack::encoded_len(permissions)
                        + Pack::encoded_len(token)
                }
                To::Unsubscribe(id) => Pack::encoded_len(id),
                To::Write(id, v, reply) => {
                    Pack::encoded_len(id)
                        + Pack::encoded_len(v)
                        + Pack::encoded_len(reply)
                }
            },
        )
    }

    fn encode(&self, buf: &mut impl BufMut) -> anyhow::Result<(), PackError> {
        len_wrapped_encode(buf, self, |buf| match self {
            To::Subscribe { path, resolver, timestamp, permissions, token } => {
                buf.put_u8(0);
                Pack::encode(path, buf)?;
                Pack::encode(resolver, buf)?;
                Pack::encode(timestamp, buf)?;
                Pack::encode(permissions, buf)?;
                Pack::encode(token, buf)
            }
            To::Unsubscribe(id) => {
                buf.put_u8(1);
                Pack::encode(id, buf)
            }
            To::Write(id, v, reply) => {
                buf.put_u8(2);
                Pack::encode(id, buf)?;
                Pack::encode(v, buf)?;
                Pack::encode(reply, buf)
            }
        })
    }

    fn decode(buf: &mut impl Buf) -> anyhow::Result<Self, PackError> {
        len_wrapped_decode(buf, |buf| match <u8 as Pack>::decode(buf)? {
            0 => {
                let path = Pack::decode(buf)?;
                let resolver = Pack::decode(buf)?;
                let timestamp = Pack::decode(buf)?;
                let permissions = Pack::decode(buf)?;
                let token = Pack::decode(buf)?;
                Ok(To::Subscribe { path, resolver, timestamp, permissions, token })
            }
            1 => Ok(To::Unsubscribe(Pack::decode(buf)?)),
            2 => {
                let id = Pack::decode(buf)?;
                let v = Pack::decode(buf)?;
                let reply = Pack::decode(buf)?;
                Ok(To::Write(id, v, reply))
            }
            _ => Err(PackError::UnknownTag),
        })
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
    /// Indicates the result of a write request
    WriteResult(Id, Value),
}

impl Pack for From {
    fn encoded_len(&self) -> usize {
        len_wrapped_len(
            1 + match self {
                From::NoSuchValue(p) => Pack::encoded_len(p),
                From::Denied(p) => Pack::encoded_len(p),
                From::Unsubscribed(id) => Pack::encoded_len(id),
                From::Subscribed(p, id, v) => {
                    Pack::encoded_len(p) + Pack::encoded_len(id) + Pack::encoded_len(v)
                }
                From::Update(id, v) => Pack::encoded_len(id) + Pack::encoded_len(v),
                From::Heartbeat => 0,
                From::WriteResult(id, v) => Pack::encoded_len(id) + Pack::encoded_len(v),
            },
        )
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        len_wrapped_encode(buf, self, |buf| match self {
            From::NoSuchValue(p) => {
                buf.put_u8(0);
                Pack::encode(p, buf)
            }
            From::Denied(p) => {
                buf.put_u8(1);
                Pack::encode(p, buf)
            }
            From::Unsubscribed(id) => {
                buf.put_u8(2);
                Pack::encode(id, buf)
            }
            From::Subscribed(p, id, v) => {
                buf.put_u8(3);
                Pack::encode(p, buf)?;
                Pack::encode(id, buf)?;
                Pack::encode(v, buf)
            }
            From::Update(id, v) => {
                buf.put_u8(4);
                Pack::encode(id, buf)?;
                Pack::encode(v, buf)
            }
            From::Heartbeat => Ok(buf.put_u8(5)),
            From::WriteResult(id, v) => {
                buf.put_u8(6);
                Pack::encode(id, buf)?;
                Pack::encode(v, buf)
            }
        })
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        len_wrapped_decode(buf, |buf| match <u8 as Pack>::decode(buf)? {
            0 => Ok(From::NoSuchValue(Pack::decode(buf)?)),
            1 => Ok(From::Denied(Pack::decode(buf)?)),
            2 => Ok(From::Unsubscribed(Pack::decode(buf)?)),
            3 => {
                let path = Pack::decode(buf)?;
                let id = Pack::decode(buf)?;
                let v = Pack::decode(buf)?;
                Ok(From::Subscribed(path, id, v))
            }
            4 => {
                let id = Pack::decode(buf)?;
                let value = Pack::decode(buf)?;
                Ok(From::Update(id, value))
            }
            5 => Ok(From::Heartbeat),
            6 => {
                let id = Pack::decode(buf)?;
                let value = Pack::decode(buf)?;
                Ok(From::WriteResult(id, value))
            }
            _ => Err(PackError::UnknownTag),
        })
    }
}
