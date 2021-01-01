use crate::{
    chars::Chars,
    pack::{self, Pack, PackError},
    path::Path,
};
use bytes::{Buf, BufMut, Bytes};
use chrono::{naive::NaiveDateTime, prelude::*};
use std::{
    convert, error, fmt, mem,
    net::SocketAddr,
    num::FpCategory,
    ops::{Add, Div, Mul, Not, Sub},
    result,
    str::FromStr,
    time::Duration,
};

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
    ResolverAuthenticate(SocketAddr, Bytes),
}

impl Pack for Hello {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Hello::Anonymous => 0,
            Hello::Token(tok) => <Bytes as Pack>::encoded_len(tok),
            Hello::ResolverAuthenticate(addr, tok) => {
                <SocketAddr as Pack>::encoded_len(addr)
                    + <Bytes as Pack>::encoded_len(tok)
            }
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            Hello::Anonymous => Ok(buf.put_u8(0)),
            Hello::Token(tok) => {
                buf.put_u8(1);
                <Bytes as Pack>::encode(tok, buf)
            }
            Hello::ResolverAuthenticate(id, tok) => {
                buf.put_u8(2);
                <SocketAddr as Pack>::encode(id, buf)?;
                <Bytes as Pack>::encode(tok, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(Hello::Anonymous),
            1 => Ok(Hello::Token(<Bytes as Pack>::decode(buf)?)),
            2 => {
                let addr = <SocketAddr as Pack>::decode(buf)?;
                let tok = <Bytes as Pack>::decode(buf)?;
                Ok(Hello::ResolverAuthenticate(addr, tok))
            }
            _ => Err(PackError::UnknownTag),
        }
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
    /// in an Unsubscibed message even if you weren't ever subscribed
    /// to the value, or it doesn't exist.
    Unsubscribe(Id),
    /// Send a write to the specified value.
    Write(Id, Value, bool),
}

impl Pack for To {
    fn encoded_len(&self) -> usize {
        1 + match self {
            To::Subscribe { path, resolver, timestamp, permissions, token } => {
                <Path as Pack>::encoded_len(path)
                    + <SocketAddr as Pack>::encoded_len(resolver)
                    + <u64 as Pack>::encoded_len(timestamp)
                    + <u32 as Pack>::encoded_len(permissions)
                    + <Bytes as Pack>::encoded_len(token)
            }
            To::Unsubscribe(id) => Id::encoded_len(id),
            To::Write(id, v, reply) => {
                Id::encoded_len(id)
                    + Value::encoded_len(v)
                    + <bool as Pack>::encoded_len(reply)
            }
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> anyhow::Result<(), PackError> {
        match self {
            To::Subscribe { path, resolver, timestamp, permissions, token } => {
                buf.put_u8(0);
                <Path as Pack>::encode(path, buf)?;
                <SocketAddr as Pack>::encode(resolver, buf)?;
                <u64 as Pack>::encode(timestamp, buf)?;
                <u32 as Pack>::encode(permissions, buf)?;
                <Bytes as Pack>::encode(token, buf)
            }
            To::Unsubscribe(id) => {
                buf.put_u8(1);
                Id::encode(id, buf)
            }
            To::Write(id, v, reply) => {
                buf.put_u8(2);
                Id::encode(id, buf)?;
                Value::encode(v, buf)?;
                <bool as Pack>::encode(reply, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> anyhow::Result<Self, PackError> {
        match buf.get_u8() {
            0 => {
                let path = <Path as Pack>::decode(buf)?;
                let resolver = <SocketAddr as Pack>::decode(buf)?;
                let timestamp = <u64 as Pack>::decode(buf)?;
                let permissions = <u32 as Pack>::decode(buf)?;
                let token = <Bytes as Pack>::decode(buf)?;
                Ok(To::Subscribe { path, resolver, timestamp, permissions, token })
            }
            1 => Ok(To::Unsubscribe(Id::decode(buf)?)),
            2 => {
                let id = Id::decode(buf)?;
                let v = Value::decode(buf)?;
                let reply = <bool as Pack>::decode(buf)?;
                Ok(To::Write(id, v, reply))
            }
            _ => Err(PackError::UnknownTag),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Typ {
    U32,
    V32,
    I32,
    Z32,
    U64,
    V64,
    I64,
    Z64,
    F32,
    F64,
    DateTime,
    Duration,
    Bool,
    String,
    Bytes,
    Result,
}

impl Typ {
    pub fn name(&self) -> &'static str {
        match self {
            Typ::U32 => "u32",
            Typ::V32 => "v32",
            Typ::I32 => "i32",
            Typ::Z32 => "z32",
            Typ::U64 => "u64",
            Typ::I64 => "i64",
            Typ::V64 => "v64",
            Typ::Z64 => "z64",
            Typ::F32 => "f32",
            Typ::F64 => "f64",
            Typ::DateTime => "datetime",
            Typ::Duration => "duration",
            Typ::Bool => "bool",
            Typ::String => "string",
            Typ::Bytes => "bytes",
            Typ::Result => "result",
        }
    }

    pub fn get(v: &Value) -> Option<Self> {
        match v {
            Value::U32(_) => Some(Typ::U32),
            Value::V32(_) => Some(Typ::V32),
            Value::I32(_) => Some(Typ::I32),
            Value::Z32(_) => Some(Typ::Z32),
            Value::U64(_) => Some(Typ::U64),
            Value::V64(_) => Some(Typ::V64),
            Value::I64(_) => Some(Typ::I64),
            Value::Z64(_) => Some(Typ::Z64),
            Value::F32(_) => Some(Typ::F32),
            Value::F64(_) => Some(Typ::F64),
            Value::DateTime(_) => Some(Typ::DateTime),
            Value::Duration(_) => Some(Typ::Duration),
            Value::String(_) => Some(Typ::String),
            Value::Bytes(_) => Some(Typ::Bytes),
            Value::True | Value::False => Some(Typ::Bool),
            Value::Null => None,
            Value::Ok | Value::Error(_) => Some(Typ::Result),
        }
    }

    pub fn parse(&self, s: &str) -> anyhow::Result<Value> {
        Ok(match s {
            "null" => Value::Null,
            s => match self {
                Typ::U32 => Value::U32(s.parse::<u32>()?),
                Typ::V32 => Value::V32(s.parse::<u32>()?),
                Typ::I32 => Value::I32(s.parse::<i32>()?),
                Typ::Z32 => Value::Z32(s.parse::<i32>()?),
                Typ::U64 => Value::U64(s.parse::<u64>()?),
                Typ::V64 => Value::V64(s.parse::<u64>()?),
                Typ::I64 => Value::I64(s.parse::<i64>()?),
                Typ::Z64 => Value::Z64(s.parse::<i64>()?),
                Typ::F32 => Value::F32(s.parse::<f32>()?),
                Typ::F64 => Value::F64(s.parse::<f64>()?),
                Typ::DateTime => match DateTime::parse_from_rfc3339(s) {
                    Err(_) => Value::DateTime(DateTime::<Utc>::from(
                        DateTime::parse_from_rfc2822(s)?,
                    )),
                    Ok(dt) => Value::DateTime(DateTime::<Utc>::from(dt)),
                },
                Typ::Duration => {
                    let s = s.trim();
                    let last =
                        s.chars().next_back().ok_or_else(|| anyhow!("too short"))?;
                    let n = if last.is_ascii_digit() {
                        s.parse::<f64>()?
                    } else {
                        s.strip_suffix(|c: char| !c.is_ascii_digit())
                            .ok_or_else(|| anyhow!("duration strip suffix"))
                            .and_then(|s| s.parse::<f64>().map_err(anyhow::Error::from))?
                    };
                    let n = if last == 's' {
                        n
                    } else {
                        bail!("invalid duration suffix {}", last)
                    };
                    Value::F64(n)
                        .cast(Typ::Duration)
                        .ok_or_else(|| anyhow!("failed to cast float to duration"))?
                }
                Typ::Bool => match s.parse::<bool>()? {
                    true => Value::True,
                    false => Value::False,
                },
                Typ::String => Value::String(Chars::from(String::from(s))),
                Typ::Bytes => Value::Bytes(Bytes::from(base64::decode(s)?)),
                Typ::Result => {
                    if s == "ok" {
                        Value::Ok
                    } else if s == "error" {
                        Value::Error(Chars::from(""))
                    } else if s.starts_with("error:") {
                        Value::Error(Chars::from(String::from(s[6..].trim())))
                    } else {
                        bail!("invalid error type, must start with 'ok' or 'error:'")
                    }
                }
            },
        })
    }
}

impl FromStr for Typ {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "u32" => Ok(Typ::U32),
            "v32" => Ok(Typ::V32),
            "i32" => Ok(Typ::I32),
            "z32" => Ok(Typ::Z32),
            "u64" => Ok(Typ::U64),
            "v64" => Ok(Typ::V64),
            "i64" => Ok(Typ::I64),
            "z64" => Ok(Typ::Z64),
            "f32" => Ok(Typ::F32),
            "f64" => Ok(Typ::F64),
            "datetime" => Ok(Typ::DateTime),
            "duration" => Ok(Typ::Duration),
            "bool" => Ok(Typ::Bool),
            "string" => Ok(Typ::String),
            "bytes" => Ok(Typ::Bytes),
            "result" => Ok(Typ::Result),
            s => Err(anyhow!(
                "invalid type, {}, valid types: u32, i32, u64, i64, f32, f64, bool, string, bytes, result", s))
        }
    }
}

impl fmt::Display for Typ {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

// This enum is limited to 0x3F cases, because the high 2 bits of the
// tag are reserved for zero cost wrapper types.
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Value {
    /// full 4 byte u32
    U32(u32),
    /// LEB128 varint, 1 - 5 bytes depending on value
    V32(u32),
    /// full 4 byte i32
    I32(i32),
    /// LEB128 varint zigzag encoded, 1 - 5 bytes depending on abs(value)
    Z32(i32),
    /// full 8 byte u64
    U64(u64),
    /// LEB128 varint, 1 - 10 bytes depending on value
    V64(u64),
    /// full 8 byte i64
    I64(i64),
    /// LEB128 varint zigzag encoded, 1 - 10 bytes depending on abs(value)
    Z64(i64),
    /// 4 byte ieee754 single precision float
    F32(f32),
    /// 8 byte ieee754 double precision float
    F64(f64),
    /// UTC timestamp
    DateTime(DateTime<Utc>),
    /// Duration
    Duration(Duration),
    /// unicode string, zero copy decode
    String(Chars),
    /// byte array, zero copy decode
    Bytes(Bytes),
    /// boolean true
    True,
    /// boolean false
    False,
    /// Empty value
    Null,
    /// An explicit ok
    Ok,
    /// An explicit error
    Error(Chars),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::U32(v) | Value::V32(v) => write!(f, "{}", v),
            Value::I32(v) | Value::Z32(v) => write!(f, "{}", v),
            Value::U64(v) | Value::V64(v) => write!(f, "{}", v),
            Value::I64(v) | Value::Z64(v) => write!(f, "{}", v),
            Value::F32(v) => write!(f, "{}", v),
            Value::F64(v) => write!(f, "{}", v),
            Value::DateTime(d) => write!(f, "{}", d),
            Value::Duration(d) => write!(f, "{}s", d.as_secs_f64()),
            Value::String(v) => write!(f, "{}", &*v),
            Value::Bytes(_) => write!(f, "<binary>"),
            Value::True => write!(f, "True"),
            Value::False => write!(f, "False"),
            Value::Null => write!(f, "Null"),
            Value::Ok => write!(f, "Ok"),
            Value::Error(v) => write!(f, "Error {}", v),
        }
    }
}

impl Add for Value {
    type Output = Value;

    fn add(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Value::U32(l), Value::U32(r)) => Value::U32(l + r),
            (Value::U32(l), Value::V32(r)) => Value::U32(l + r),
            (Value::V32(l), Value::V32(r)) => Value::V32(l + r),
            (Value::V32(l), Value::U32(r)) => Value::U32(l + r),
            (Value::I32(l), Value::I32(r)) => Value::I32(l + r),
            (Value::I32(l), Value::Z32(r)) => Value::I32(l + r),
            (Value::Z32(l), Value::Z32(r)) => Value::Z32(l + r),
            (Value::Z32(l), Value::I32(r)) => Value::I32(l + r),
            (Value::U64(l), Value::U64(r)) => Value::U64(l + r),
            (Value::U64(l), Value::V64(r)) => Value::U64(l + r),
            (Value::V64(l), Value::V64(r)) => Value::V64(l + r),
            (Value::I64(l), Value::I64(r)) => Value::I64(l + r),
            (Value::I64(l), Value::Z64(r)) => Value::I64(l + r),
            (Value::Z64(l), Value::Z64(r)) => Value::Z64(l + r),
            (Value::Z64(l), Value::I64(r)) => Value::I64(l + r),
            (Value::F32(l), Value::F32(r)) => Value::F32(l + r),
            (Value::F64(l), Value::F64(r)) => Value::F64(l + r),
            (Value::DateTime(dt), Value::Duration(d)) => {
                match chrono::Duration::from_std(d) {
                    Ok(d) => Value::DateTime(dt + d),
                    Err(e) => Value::Error(Chars::from(format!("{}", e))),
                }
            }
            (Value::Duration(d0), Value::Duration(d1)) => Value::Duration(d0 + d1),
            (l, r) => Value::Error(Chars::from(format!("can't add {:?} and {:?}", l, r))),
        }
    }
}

impl Sub for Value {
    type Output = Value;

    fn sub(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Value::U32(l), Value::U32(r)) if l >= r => Value::U32(l - r),
            (Value::U32(l), Value::V32(r)) if l >= r => Value::U32(l - r),
            (Value::V32(l), Value::V32(r)) if l >= r => Value::V32(l - r),
            (Value::V32(l), Value::U32(r)) if l >= r => Value::U32(l - r),
            (Value::I32(l), Value::I32(r)) => Value::I32(l - r),
            (Value::I32(l), Value::Z32(r)) => Value::I32(l - r),
            (Value::Z32(l), Value::Z32(r)) => Value::Z32(l - r),
            (Value::Z32(l), Value::I32(r)) => Value::I32(l - r),
            (Value::U64(l), Value::U64(r)) if l >= r => Value::U64(l - r),
            (Value::U64(l), Value::V64(r)) if l >= r => Value::U64(l - r),
            (Value::V64(l), Value::V64(r)) if l >= r => Value::V64(l - r),
            (Value::I64(l), Value::I64(r)) => Value::I64(l - r),
            (Value::I64(l), Value::Z64(r)) => Value::I64(l - r),
            (Value::Z64(l), Value::Z64(r)) => Value::Z64(l - r),
            (Value::Z64(l), Value::I64(r)) => Value::I64(l - r),
            (Value::F32(l), Value::F32(r)) => Value::F32(l - r),
            (Value::F64(l), Value::F64(r)) => Value::F64(l - r),
            (Value::DateTime(dt), Value::Duration(d)) => {
                match chrono::Duration::from_std(d) {
                    Ok(d) => Value::DateTime(dt - d),
                    Err(e) => Value::Error(Chars::from(format!("{}", e))),
                }
            }
            (Value::Duration(d0), Value::Duration(d1)) => Value::Duration(d0 - d1),
            (l, r) => Value::Error(Chars::from(format!("can't sub {:?} and {:?}", l, r))),
        }
    }
}

impl Mul for Value {
    type Output = Value;

    fn mul(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Value::U32(l), Value::U32(r)) => Value::U32(l * r),
            (Value::U32(l), Value::V32(r)) => Value::U32(l * r),
            (Value::V32(l), Value::V32(r)) => Value::V32(l * r),
            (Value::V32(l), Value::U32(r)) => Value::U32(l * r),
            (Value::I32(l), Value::I32(r)) => Value::I32(l * r),
            (Value::I32(l), Value::Z32(r)) => Value::I32(l * r),
            (Value::Z32(l), Value::Z32(r)) => Value::Z32(l * r),
            (Value::Z32(l), Value::I32(r)) => Value::I32(l * r),
            (Value::U64(l), Value::U64(r)) => Value::U64(l * r),
            (Value::U64(l), Value::V64(r)) => Value::U64(l * r),
            (Value::V64(l), Value::V64(r)) => Value::V64(l * r),
            (Value::I64(l), Value::I64(r)) => Value::I64(l * r),
            (Value::I64(l), Value::Z64(r)) => Value::I64(l * r),
            (Value::Z64(l), Value::Z64(r)) => Value::Z64(l * r),
            (Value::Z64(l), Value::I64(r)) => Value::I64(l * r),
            (Value::F32(l), Value::F32(r)) => Value::F32(l * r),
            (Value::F64(l), Value::F64(r)) => Value::F64(l * r),
            (Value::Duration(d), Value::U32(s)) => Value::Duration(d * s),
            (Value::U32(s), Value::Duration(d)) => Value::Duration(d * s),
            (Value::Duration(d), Value::V32(s)) => Value::Duration(d * s),
            (Value::V32(s), Value::Duration(d)) => Value::Duration(d * s),
            (Value::Duration(d), Value::F32(s)) => Value::Duration(d.mul_f32(s)),
            (Value::F32(s), Value::Duration(d)) => Value::Duration(d.mul_f32(s)),
            (Value::Duration(d), Value::F64(s)) => Value::Duration(d.mul_f64(s)),
            (Value::F64(s), Value::Duration(d)) => Value::Duration(d.mul_f64(s)),
            (l, r) => {
                Value::Error(Chars::from(format!("can't multiply {:?} and {:?}", l, r)))
            }
        }
    }
}

impl Div for Value {
    type Output = Value;

    fn div(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Value::U32(l), Value::U32(r)) if r > 0 => Value::U32(l / r),
            (Value::U32(l), Value::V32(r)) if r > 0 => Value::U32(l / r),
            (Value::V32(l), Value::V32(r)) if r > 0 => Value::V32(l / r),
            (Value::V32(l), Value::U32(r)) if r > 0 => Value::U32(l / r),
            (Value::I32(l), Value::I32(r)) if r > 0 => Value::I32(l / r),
            (Value::I32(l), Value::Z32(r)) if r > 0 => Value::I32(l / r),
            (Value::Z32(l), Value::Z32(r)) if r > 0 => Value::Z32(l / r),
            (Value::Z32(l), Value::I32(r)) if r > 0 => Value::I32(l / r),
            (Value::U64(l), Value::U64(r)) if r > 0 => Value::U64(l / r),
            (Value::U64(l), Value::V64(r)) if r > 0 => Value::U64(l / r),
            (Value::V64(l), Value::V64(r)) if r > 0 => Value::V64(l / r),
            (Value::I64(l), Value::I64(r)) if r > 0 => Value::I64(l / r),
            (Value::I64(l), Value::Z64(r)) if r > 0 => Value::I64(l / r),
            (Value::Z64(l), Value::Z64(r)) if r > 0 => Value::Z64(l / r),
            (Value::Z64(l), Value::I64(r)) if r > 0 => Value::I64(l / r),
            (Value::F32(l), Value::F32(r)) => Value::F32(l / r),
            (Value::F64(l), Value::F64(r)) => Value::F64(l / r),
            (Value::Duration(d), Value::U32(s)) => Value::Duration(d / s),
            (Value::Duration(d), Value::V32(s)) => Value::Duration(d / s),
            (Value::Duration(d), Value::F32(s)) => Value::Duration(d.div_f32(s)),
            (Value::Duration(d), Value::F64(s)) => Value::Duration(d.div_f64(s)),
            (l, r) => {
                Value::Error(Chars::from(format!("can't divide {:?} by {:?}", l, r)))
            }
        }
    }
}

impl Not for Value {
    type Output = Value;

    fn not(self) -> Self {
        match self {
            Value::U32(v) => {
                Value::Error(Chars::from(format!("can't apply not to U32({})", v)))
            }
            Value::V32(v) => {
                Value::Error(Chars::from(format!("can't apply not to V32({})", v)))
            }
            Value::I32(v) => {
                Value::Error(Chars::from(format!("can't apply not to I32({})", v)))
            }
            Value::Z32(v) => {
                Value::Error(Chars::from(format!("can't apply not to Z32({})", v)))
            }
            Value::U64(v) => {
                Value::Error(Chars::from(format!("can't apply not to U64({})", v)))
            }
            Value::V64(v) => {
                Value::Error(Chars::from(format!("can't apply not to V64({})", v)))
            }
            Value::I64(v) => {
                Value::Error(Chars::from(format!("can't apply not to I64({})", v)))
            }
            Value::Z64(v) => {
                Value::Error(Chars::from(format!("can't apply not to Z64({})", v)))
            }
            Value::F32(v) => {
                Value::Error(Chars::from(format!("can't apply not to F32({})", v)))
            }
            Value::F64(v) => {
                Value::Error(Chars::from(format!("can't apply not to F64({})", v)))
            }
            Value::DateTime(v) => {
                Value::Error(Chars::from(format!("can't apply not to DateTime({})", v)))
            }
            Value::Duration(v) => Value::Error(Chars::from(format!(
                "can't apply not to Duration({}s)",
                v.as_secs_f64()
            ))),
            Value::String(v) => {
                Value::Error(Chars::from(format!("can't apply not to String({})", v)))
            }
            Value::Bytes(_) => {
                Value::Error(Chars::from(format!("can't apply not to Bytes")))
            }
            Value::True => Value::False,
            Value::False => Value::True,
            Value::Null => Value::Null,
            Value::Ok => Value::Error(Chars::from(format!("can't apply not to Ok"))),
            Value::Error(v) => {
                Value::Error(Chars::from(format!("can't apply not to Error({})", v)))
            }
        }
    }
}

impl Pack for Value {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Value::U32(_) => mem::size_of::<u32>(),
            Value::V32(v) => pack::varint_len(*v as u64),
            Value::I32(_) => mem::size_of::<i32>(),
            Value::Z32(v) => pack::varint_len(pack::i32_zz(*v) as u64),
            Value::U64(_) => mem::size_of::<u64>(),
            Value::V64(v) => pack::varint_len(*v),
            Value::I64(_) => mem::size_of::<i64>(),
            Value::Z64(v) => pack::varint_len(pack::i64_zz(*v) as u64),
            Value::F32(_) => mem::size_of::<f32>(),
            Value::F64(_) => mem::size_of::<f64>(),
            Value::DateTime(_) => 12,
            Value::Duration(_) => 12,
            Value::String(c) => <Chars as Pack>::encoded_len(c),
            Value::Bytes(b) => <Bytes as Pack>::encoded_len(b),
            Value::True | Value::False | Value::Null => 0,
            Value::Ok => 0,
            Value::Error(c) => <Chars as Pack>::encoded_len(c),
        }
    }

    // the high two bits of the tag are reserved for wrapper types,
    // max tag is therefore 0x3F
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            Value::U32(i) => {
                buf.put_u8(0);
                Ok(buf.put_u32(*i))
            }
            Value::V32(i) => {
                buf.put_u8(1);
                Ok(pack::encode_varint(*i as u64, buf))
            }
            Value::I32(i) => {
                buf.put_u8(2);
                Ok(buf.put_i32(*i))
            }
            Value::Z32(i) => {
                buf.put_u8(3);
                Ok(pack::encode_varint(pack::i32_zz(*i) as u64, buf))
            }
            Value::U64(i) => {
                buf.put_u8(4);
                Ok(buf.put_u64(*i))
            }
            Value::V64(i) => {
                buf.put_u8(5);
                Ok(pack::encode_varint(*i, buf))
            }
            Value::I64(i) => {
                buf.put_u8(6);
                Ok(buf.put_i64(*i))
            }
            Value::Z64(i) => {
                buf.put_u8(7);
                Ok(pack::encode_varint(pack::i64_zz(*i), buf))
            }
            Value::F32(i) => {
                buf.put_u8(8);
                Ok(buf.put_f32(*i))
            }
            Value::F64(i) => {
                buf.put_u8(9);
                Ok(buf.put_f64(*i))
            }
            Value::DateTime(dt) => {
                buf.put_u8(10);
                Ok(<DateTime<Utc> as Pack>::encode(dt, buf)?)
            }
            Value::Duration(d) => {
                buf.put_u8(11);
                Ok(<Duration as Pack>::encode(d, buf)?)
            }
            Value::String(s) => {
                buf.put_u8(12);
                <Chars as Pack>::encode(s, buf)
            }
            Value::Bytes(b) => {
                buf.put_u8(13);
                <Bytes as Pack>::encode(b, buf)
            }
            Value::True => Ok(buf.put_u8(14)),
            Value::False => Ok(buf.put_u8(15)),
            Value::Null => Ok(buf.put_u8(16)),
            Value::Ok => Ok(buf.put_u8(17)),
            Value::Error(e) => {
                buf.put_u8(18);
                <Chars as Pack>::encode(e, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
        match buf.get_u8() {
            0 => Ok(Value::U32(buf.get_u32())),
            1 => Ok(Value::V32(pack::decode_varint(buf)? as u32)),
            2 => Ok(Value::I32(buf.get_i32())),
            3 => Ok(Value::Z32(pack::i32_uzz(pack::decode_varint(buf)? as u32))),
            4 => Ok(Value::U64(buf.get_u64())),
            5 => Ok(Value::V64(pack::decode_varint(buf)?)),
            6 => Ok(Value::I64(buf.get_i64())),
            7 => Ok(Value::Z64(pack::i64_uzz(pack::decode_varint(buf)?))),
            8 => Ok(Value::F32(buf.get_f32())),
            9 => Ok(Value::F64(buf.get_f64())),
            10 => Ok(Value::DateTime(<DateTime<Utc> as Pack>::decode(buf)?)),
            11 => Ok(Value::Duration(<Duration as Pack>::decode(buf)?)),
            12 => Ok(Value::String(<Chars as Pack>::decode(buf)?)),
            13 => Ok(Value::Bytes(<Bytes as Pack>::decode(buf)?)),
            14 => Ok(Value::True),
            15 => Ok(Value::False),
            16 => Ok(Value::Null),
            17 => Ok(Value::Ok),
            18 => Ok(Value::Error(<Chars as Pack>::decode(buf)?)),
            _ => Err(PackError::UnknownTag),
        }
    }
}

pub trait FromValue {
    type Error;

    fn from_value(v: Value) -> result::Result<Self, Self::Error>
    where
        Self: Sized;
}

impl Value {
    /// Whatever value is attempt to turn it into the type specified
    pub fn cast(self, typ: Typ) -> Option<Value> {
        match typ {
            Typ::U32 => match self {
                Value::U32(v) => Some(Value::U32(v)),
                Value::V32(v) => Some(Value::U32(v)),
                Value::I32(v) => Some(Value::U32(v as u32)),
                Value::Z32(v) => Some(Value::U32(v as u32)),
                Value::U64(v) => Some(Value::U32(v as u32)),
                Value::V64(v) => Some(Value::U32(v as u32)),
                Value::I64(v) => Some(Value::U32(v as u32)),
                Value::Z64(v) => Some(Value::U32(v as u32)),
                Value::F32(v) => Some(Value::U32(v as u32)),
                Value::F64(v) => Some(Value::U32(v as u32)),
                Value::DateTime(_) => None,
                Value::Duration(d) => Some(Value::U32(d.as_secs() as u32)),
                Value::String(s) => match s.parse::<u32>() {
                    Err(_) => None,
                    Ok(v) => Some(Value::U32(v)),
                },
                Value::Bytes(_) => None,
                Value::True => Some(Value::U32(1)),
                Value::False => Some(Value::U32(0)),
                Value::Null => None,
                Value::Ok => None,
                Value::Error(_) => None,
            },
            Typ::V32 => match self {
                Value::U32(v) => Some(Value::V32(v)),
                Value::V32(v) => Some(Value::V32(v)),
                Value::I32(v) => Some(Value::V32(v as u32)),
                Value::Z32(v) => Some(Value::V32(v as u32)),
                Value::U64(v) => Some(Value::V32(v as u32)),
                Value::V64(v) => Some(Value::V32(v as u32)),
                Value::I64(v) => Some(Value::V32(v as u32)),
                Value::Z64(v) => Some(Value::V32(v as u32)),
                Value::F32(v) => Some(Value::V32(v as u32)),
                Value::F64(v) => Some(Value::V32(v as u32)),
                Value::DateTime(_) => None,
                Value::Duration(d) => Some(Value::V32(d.as_secs() as u32)),
                Value::String(s) => match s.parse::<u32>() {
                    Err(_) => None,
                    Ok(v) => Some(Value::V32(v)),
                },
                Value::Bytes(_) => None,
                Value::True => Some(Value::V32(1)),
                Value::False => Some(Value::V32(0)),
                Value::Null => None,
                Value::Ok => None,
                Value::Error(_) => None,
            },
            Typ::I32 => match self {
                Value::U32(v) => Some(Value::I32(v as i32)),
                Value::V32(v) => Some(Value::I32(v as i32)),
                Value::I32(v) => Some(Value::I32(v)),
                Value::Z32(v) => Some(Value::I32(v)),
                Value::U64(v) => Some(Value::I32(v as i32)),
                Value::V64(v) => Some(Value::I32(v as i32)),
                Value::I64(v) => Some(Value::I32(v as i32)),
                Value::Z64(v) => Some(Value::I32(v as i32)),
                Value::F32(v) => Some(Value::I32(v as i32)),
                Value::F64(v) => Some(Value::I32(v as i32)),
                Value::DateTime(v) => Some(Value::I32(v.timestamp() as i32)),
                Value::Duration(v) => Some(Value::I32(v.as_secs() as i32)),
                Value::String(s) => match s.parse::<i32>() {
                    Err(_) => None,
                    Ok(v) => Some(Value::I32(v)),
                },
                Value::Bytes(_) => None,
                Value::True => Some(Value::I32(1)),
                Value::False => Some(Value::I32(0)),
                Value::Null => None,
                Value::Ok => None,
                Value::Error(_) => None,
            },
            Typ::Z32 => match self {
                Value::U32(v) => Some(Value::Z32(v as i32)),
                Value::V32(v) => Some(Value::Z32(v as i32)),
                Value::I32(v) => Some(Value::Z32(v)),
                Value::Z32(v) => Some(Value::Z32(v)),
                Value::U64(v) => Some(Value::Z32(v as i32)),
                Value::V64(v) => Some(Value::Z32(v as i32)),
                Value::I64(v) => Some(Value::Z32(v as i32)),
                Value::Z64(v) => Some(Value::Z32(v as i32)),
                Value::F32(v) => Some(Value::Z32(v as i32)),
                Value::F64(v) => Some(Value::Z32(v as i32)),
                Value::DateTime(v) => Some(Value::Z32(v.timestamp() as i32)),
                Value::Duration(v) => Some(Value::Z32(v.as_secs() as i32)),
                Value::String(s) => match s.parse::<i32>() {
                    Err(_) => None,
                    Ok(v) => Some(Value::Z32(v)),
                },
                Value::Bytes(_) => None,
                Value::True => Some(Value::Z32(1)),
                Value::False => Some(Value::Z32(0)),
                Value::Null => None,
                Value::Ok => None,
                Value::Error(_) => None,
            },
            Typ::U64 => match self {
                Value::U32(v) => Some(Value::U64(v as u64)),
                Value::V32(v) => Some(Value::U64(v as u64)),
                Value::I32(v) => Some(Value::U64(v as u64)),
                Value::Z32(v) => Some(Value::U64(v as u64)),
                Value::U64(v) => Some(Value::U64(v)),
                Value::V64(v) => Some(Value::U64(v)),
                Value::I64(v) => Some(Value::U64(v as u64)),
                Value::Z64(v) => Some(Value::U64(v as u64)),
                Value::F32(v) => Some(Value::U64(v as u64)),
                Value::F64(v) => Some(Value::U64(v as u64)),
                Value::DateTime(_) => None,
                Value::Duration(d) => Some(Value::U64(d.as_secs())),
                Value::String(s) => match s.parse::<u64>() {
                    Err(_) => None,
                    Ok(v) => Some(Value::U64(v)),
                },
                Value::Bytes(_) => None,
                Value::True => Some(Value::U64(1)),
                Value::False => Some(Value::U64(0)),
                Value::Null => None,
                Value::Ok => None,
                Value::Error(_) => None,
            },
            Typ::V64 => match self {
                Value::U32(v) => Some(Value::V64(v as u64)),
                Value::V32(v) => Some(Value::V64(v as u64)),
                Value::I32(v) => Some(Value::V64(v as u64)),
                Value::Z32(v) => Some(Value::V64(v as u64)),
                Value::U64(v) => Some(Value::V64(v)),
                Value::V64(v) => Some(Value::V64(v)),
                Value::I64(v) => Some(Value::V64(v as u64)),
                Value::Z64(v) => Some(Value::V64(v as u64)),
                Value::F32(v) => Some(Value::V64(v as u64)),
                Value::F64(v) => Some(Value::V64(v as u64)),
                Value::DateTime(_) => None,
                Value::Duration(d) => Some(Value::V64(d.as_secs())),
                Value::String(s) => match s.parse::<u64>() {
                    Err(_) => None,
                    Ok(v) => Some(Value::V64(v)),
                },
                Value::Bytes(_) => None,
                Value::True => Some(Value::V64(1)),
                Value::False => Some(Value::V64(0)),
                Value::Null => None,
                Value::Ok => None,
                Value::Error(_) => None,
            },
            Typ::I64 => match self {
                Value::U32(v) => Some(Value::I64(v as i64)),
                Value::V32(v) => Some(Value::I64(v as i64)),
                Value::I32(v) => Some(Value::I64(v as i64)),
                Value::Z32(v) => Some(Value::I64(v as i64)),
                Value::U64(v) => Some(Value::I64(v as i64)),
                Value::V64(v) => Some(Value::I64(v as i64)),
                Value::I64(v) => Some(Value::I64(v)),
                Value::Z64(v) => Some(Value::I64(v)),
                Value::F32(v) => Some(Value::I64(v as i64)),
                Value::F64(v) => Some(Value::I64(v as i64)),
                Value::DateTime(v) => Some(Value::I64(v.timestamp())),
                Value::Duration(v) => Some(Value::I64(v.as_secs() as i64)),
                Value::String(s) => match s.parse::<i64>() {
                    Err(_) => None,
                    Ok(v) => Some(Value::I64(v)),
                },
                Value::Bytes(_) => None,
                Value::True => Some(Value::I64(1)),
                Value::False => Some(Value::I64(0)),
                Value::Null => None,
                Value::Ok => None,
                Value::Error(_) => None,
            },
            Typ::Z64 => match self {
                Value::U32(v) => Some(Value::Z64(v as i64)),
                Value::V32(v) => Some(Value::Z64(v as i64)),
                Value::I32(v) => Some(Value::Z64(v as i64)),
                Value::Z32(v) => Some(Value::Z64(v as i64)),
                Value::U64(v) => Some(Value::Z64(v as i64)),
                Value::V64(v) => Some(Value::Z64(v as i64)),
                Value::I64(v) => Some(Value::Z64(v)),
                Value::Z64(v) => Some(Value::Z64(v)),
                Value::F32(v) => Some(Value::Z64(v as i64)),
                Value::F64(v) => Some(Value::Z64(v as i64)),
                Value::DateTime(v) => Some(Value::Z64(v.timestamp())),
                Value::Duration(v) => Some(Value::Z64(v.as_secs() as i64)),
                Value::String(s) => match s.parse::<i64>() {
                    Err(_) => None,
                    Ok(v) => Some(Value::Z64(v)),
                },
                Value::Bytes(_) => None,
                Value::True => Some(Value::Z64(1)),
                Value::False => Some(Value::Z64(0)),
                Value::Null => None,
                Value::Ok => None,
                Value::Error(_) => None,
            },
            Typ::F32 => match self {
                Value::U32(v) => Some(Value::F32(v as f32)),
                Value::V32(v) => Some(Value::F32(v as f32)),
                Value::I32(v) => Some(Value::F32(v as f32)),
                Value::Z32(v) => Some(Value::F32(v as f32)),
                Value::U64(v) => Some(Value::F32(v as f32)),
                Value::V64(v) => Some(Value::F32(v as f32)),
                Value::I64(v) => Some(Value::F32(v as f32)),
                Value::Z64(v) => Some(Value::F32(v as f32)),
                Value::F32(v) => Some(Value::F32(v)),
                Value::F64(v) => Some(Value::F32(v as f32)),
                Value::DateTime(v) => Some(Value::F32(v.timestamp() as f32)),
                Value::Duration(v) => Some(Value::F32(v.as_secs() as f32)),
                Value::String(s) => match s.parse::<f32>() {
                    Err(_) => None,
                    Ok(v) => Some(Value::F32(v)),
                },
                Value::Bytes(_) => None,
                Value::True => Some(Value::F32(1.)),
                Value::False => Some(Value::F32(0.)),
                Value::Null => None,
                Value::Ok => None,
                Value::Error(_) => None,
            },
            Typ::F64 => match self {
                Value::U32(v) => Some(Value::F64(v as f64)),
                Value::V32(v) => Some(Value::F64(v as f64)),
                Value::I32(v) => Some(Value::F64(v as f64)),
                Value::Z32(v) => Some(Value::F64(v as f64)),
                Value::U64(v) => Some(Value::F64(v as f64)),
                Value::V64(v) => Some(Value::F64(v as f64)),
                Value::I64(v) => Some(Value::F64(v as f64)),
                Value::Z64(v) => Some(Value::F64(v as f64)),
                Value::F32(v) => Some(Value::F64(v as f64)),
                Value::F64(v) => Some(Value::F64(v)),
                Value::DateTime(v) => Some(Value::F64(v.timestamp() as f64)),
                Value::Duration(v) => Some(Value::F64(v.as_secs() as f64)),
                Value::String(s) => match s.parse::<f64>() {
                    Err(_) => None,
                    Ok(v) => Some(Value::F64(v)),
                },
                Value::Bytes(_) => None,
                Value::True => Some(Value::F64(1.)),
                Value::False => Some(Value::F64(0.)),
                Value::Null => None,
                Value::Ok => None,
                Value::Error(_) => None,
            },
            Typ::Bool => match self {
                Value::U32(v) => Some(if v > 0 { Value::True } else { Value::False }),
                Value::V32(v) => Some(if v > 0 { Value::True } else { Value::False }),
                Value::I32(v) => Some(if v > 0 { Value::True } else { Value::False }),
                Value::Z32(v) => Some(if v > 0 { Value::True } else { Value::False }),
                Value::U64(v) => Some(if v > 0 { Value::True } else { Value::False }),
                Value::V64(v) => Some(if v > 0 { Value::True } else { Value::False }),
                Value::I64(v) => Some(if v > 0 { Value::True } else { Value::False }),
                Value::Z64(v) => Some(if v > 0 { Value::True } else { Value::False }),
                Value::F32(v) => Some(if v > 0. { Value::True } else { Value::False }),
                Value::F64(v) => Some(if v > 0. { Value::True } else { Value::False }),
                Value::DateTime(_) => None,
                Value::Duration(_) => None,
                Value::String(s) => {
                    Some(if s.len() > 0 { Value::True } else { Value::False })
                }
                Value::Bytes(_) => None,
                Value::True => Some(Value::True),
                Value::False => Some(Value::False),
                Value::Null => Some(Value::False),
                Value::Ok => Some(Value::True),
                Value::Error(_) => Some(Value::False),
            },
            Typ::String => match self {
                Value::U32(v) => Some(Value::String(Chars::from(v.to_string()))),
                Value::V32(v) => Some(Value::String(Chars::from(v.to_string()))),
                Value::I32(v) => Some(Value::String(Chars::from(v.to_string()))),
                Value::Z32(v) => Some(Value::String(Chars::from(v.to_string()))),
                Value::U64(v) => Some(Value::String(Chars::from(v.to_string()))),
                Value::V64(v) => Some(Value::String(Chars::from(v.to_string()))),
                Value::I64(v) => Some(Value::String(Chars::from(v.to_string()))),
                Value::Z64(v) => Some(Value::String(Chars::from(v.to_string()))),
                Value::F32(v) => Some(Value::String(Chars::from(v.to_string()))),
                Value::F64(v) => Some(Value::String(Chars::from(v.to_string()))),
                Value::DateTime(d) => Some(Value::String(Chars::from(format!("{}", d)))),
                Value::Duration(d) => {
                    Some(Value::String(Chars::from(format!("{}s", d.as_secs_f64()))))
                }
                Value::String(s) => Some(Value::String(s)),
                Value::Bytes(_) => None,
                Value::True => Some(Value::String(Chars::from("true"))),
                Value::False => Some(Value::String(Chars::from("false"))),
                Value::Null => Some(Value::String(Chars::from("null"))),
                Value::Ok => Some(Value::String(Chars::from("ok"))),
                Value::Error(s) => Some(Value::String(s)),
            },
            Typ::Bytes => None,
            Typ::Result => match self {
                Value::U32(_) => Some(Value::Ok),
                Value::V32(_) => Some(Value::Ok),
                Value::I32(_) => Some(Value::Ok),
                Value::Z32(_) => Some(Value::Ok),
                Value::U64(_) => Some(Value::Ok),
                Value::V64(_) => Some(Value::Ok),
                Value::I64(_) => Some(Value::Ok),
                Value::Z64(_) => Some(Value::Ok),
                Value::F32(_) => Some(Value::Ok),
                Value::F64(_) => Some(Value::Ok),
                Value::DateTime(_) => Some(Value::Ok),
                Value::Duration(_) => Some(Value::Ok),
                Value::String(_) => Some(Value::Ok),
                Value::Bytes(_) => None,
                Value::True => Some(Value::Ok),
                Value::False => Some(Value::Ok),
                Value::Null => Some(Value::Ok),
                Value::Ok => Some(Value::Ok),
                Value::Error(s) => Some(Value::Error(s)),
            },
            Typ::DateTime => match self {
                Value::U32(v) | Value::V32(v) => {
                    Some(Value::DateTime(DateTime::from_utc(
                        NaiveDateTime::from_timestamp_opt(v as i64, 0)?,
                        Utc,
                    )))
                }
                Value::I32(v) | Value::Z32(v) => {
                    Some(Value::DateTime(DateTime::from_utc(
                        NaiveDateTime::from_timestamp_opt(v as i64, 0)?,
                        Utc,
                    )))
                }
                Value::U64(v) | Value::V64(v) => {
                    Some(Value::DateTime(DateTime::from_utc(
                        NaiveDateTime::from_timestamp_opt(v as i64, 0)?,
                        Utc,
                    )))
                }
                Value::I64(v) | Value::Z64(v) => Some(Value::DateTime(
                    DateTime::from_utc(NaiveDateTime::from_timestamp_opt(v, 0)?, Utc),
                )),
                Value::F32(v) => match v.classify() {
                    FpCategory::Nan | FpCategory::Infinite => None,
                    FpCategory::Normal | FpCategory::Subnormal | FpCategory::Zero => {
                        Some(Value::DateTime(DateTime::from_utc(
                            NaiveDateTime::from_timestamp_opt(
                                v.trunc() as i64,
                                v.fract().abs() as u32,
                            )?,
                            Utc,
                        )))
                    }
                },
                Value::F64(v) => match v.classify() {
                    FpCategory::Nan | FpCategory::Infinite => None,
                    FpCategory::Normal | FpCategory::Subnormal | FpCategory::Zero => {
                        Some(Value::DateTime(DateTime::from_utc(
                            NaiveDateTime::from_timestamp_opt(
                                v.trunc() as i64,
                                v.fract().abs() as u32,
                            )?,
                            Utc,
                        )))
                    }
                },
                v @ Value::DateTime(_) => Some(v),
                Value::Duration(d) => Some(Value::DateTime(DateTime::from_utc(
                    NaiveDateTime::from_timestamp_opt(
                        d.as_secs() as i64,
                        d.subsec_nanos(),
                    )?,
                    Utc,
                ))),

                Value::String(c) => match Typ::DateTime.parse(&*c) {
                    Err(_) => None,
                    Ok(dt) => Some(dt),
                },
                Value::Bytes(_)
                | Value::True
                | Value::False
                | Value::Null
                | Value::Ok
                | Value::Error(_) => None,
            },
            Typ::Duration => match self {
                Value::U32(v) | Value::V32(v) => {
                    Some(Value::Duration(Duration::from_secs(v as u64)))
                }
                Value::I32(v) | Value::Z32(v) => {
                    Some(Value::Duration(Duration::from_secs(i32::abs(v) as u64)))
                }
                Value::U64(v) | Value::V64(v) => {
                    Some(Value::Duration(Duration::from_secs(v)))
                }
                Value::I64(v) | Value::Z64(v) => {
                    Some(Value::Duration(Duration::from_secs(i64::abs(v) as u64)))
                }
                Value::F32(v) => match v.classify() {
                    FpCategory::Nan | FpCategory::Infinite => None,
                    FpCategory::Normal | FpCategory::Subnormal | FpCategory::Zero => {
                        if v < 0. || v > u64::MAX as f32 {
                            None
                        } else {
                            Some(Value::Duration(Duration::from_secs_f32(v)))
                        }
                    }
                },
                Value::F64(v) => match v.classify() {
                    FpCategory::Nan | FpCategory::Infinite => None,
                    FpCategory::Normal | FpCategory::Subnormal | FpCategory::Zero => {
                        if v < 0. || v > u64::MAX as f64 {
                            None
                        } else {
                            Some(Value::Duration(Duration::from_secs_f64(v)))
                        }
                    }
                },
                Value::DateTime(d) => {
                    let dur = d.timestamp() as f64;
                    let dur = dur + (d.timestamp_nanos() / 1_000_000_000) as f64;
                    Some(Value::Duration(Duration::from_secs_f64(dur)))
                }
                v @ Value::Duration(_) => Some(v),
                Value::String(c) => {
                    match c.trim_matches(|c: char| !c.is_ascii_digit()).parse::<f64>() {
                        Err(_) => None,
                        Ok(v) => Value::F64(v).cast(Typ::Duration),
                    }
                }
                Value::Bytes(_)
                | Value::True
                | Value::False
                | Value::Null
                | Value::Ok
                | Value::Error(_) => None,
            },
        }
    }

    /// cast value directly to any type implementing `FromValue`
    pub fn cast_to<T: FromValue + Sized>(self) -> result::Result<T, T::Error> {
        <T as FromValue>::from_value(self)
    }

    /// return true if the value is some kind of number, otherwise
    /// false.
    pub fn is_number(&self) -> bool {
        match self {
            Value::U32(_)
            | Value::V32(_)
            | Value::I32(_)
            | Value::Z32(_)
            | Value::U64(_)
            | Value::V64(_)
            | Value::I64(_)
            | Value::Z64(_)
            | Value::F32(_)
            | Value::F64(_) => true,
            Value::DateTime(_)
            | Value::Duration(_)
            | Value::String(_)
            | Value::Bytes(_)
            | Value::True
            | Value::False
            | Value::Null
            | Value::Ok
            | Value::Error(_) => false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CantCast;

impl fmt::Display for CantCast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "could not cast to the requested type")
    }
}

impl error::Error for CantCast {}

impl<T: Into<Value> + Copy> convert::From<&T> for Value {
    fn from(v: &T) -> Value {
        (*v).into()
    }
}

impl FromValue for u8 {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        let v = v.cast_to::<u32>()?;
        if v <= u8::MAX as u32 {
            Ok(v as u8)
        } else {
            Err(CantCast)
        }
    }
}

impl convert::From<u8> for Value {
    fn from(v: u8) -> Value {
        Value::U32(v as u32)
    }
}

impl FromValue for i8 {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        let v = v.cast_to::<i32>()?;
        if v <= i8::MAX as i32 && v >= i8::MIN as i32 {
            Ok(v as i8)
        } else {
            Err(CantCast)
        }
    }
}

impl convert::From<i8> for Value {
    fn from(v: i8) -> Value {
        Value::I32(v as i32)
    }
}

impl FromValue for u16 {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        let v = v.cast_to::<u32>()?;
        if v <= u16::MAX as u32 {
            Ok(v as u16)
        } else {
            Err(CantCast)
        }
    }
}

impl convert::From<u16> for Value {
    fn from(v: u16) -> Value {
        Value::U32(v as u32)
    }
}

impl FromValue for i16 {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        let v = v.cast_to::<i32>()?;
        if v <= i16::MAX as i32 && v >= i16::MIN as i32 {
            Ok(v as i16)
        } else {
            Err(CantCast)
        }
    }
}

impl convert::From<i16> for Value {
    fn from(v: i16) -> Value {
        Value::I32(v as i32)
    }
}

impl FromValue for u32 {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::U32).ok_or(CantCast).and_then(|v| match v {
            Value::U32(v) => Ok(v),
            _ => Err(CantCast),
        })
    }
}

impl convert::From<u32> for Value {
    fn from(v: u32) -> Value {
        Value::U32(v)
    }
}

impl FromValue for i32 {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::I32).ok_or(CantCast).and_then(|v| match v {
            Value::I32(v) => Ok(v),
            _ => Err(CantCast),
        })
    }
}

impl convert::From<i32> for Value {
    fn from(v: i32) -> Value {
        Value::I32(v)
    }
}

impl FromValue for u64 {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::U64).ok_or(CantCast).and_then(|v| match v {
            Value::U64(v) => Ok(v),
            _ => Err(CantCast),
        })
    }
}

impl convert::From<u64> for Value {
    fn from(v: u64) -> Value {
        Value::U64(v)
    }
}

impl FromValue for i64 {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::I64).ok_or(CantCast).and_then(|v| match v {
            Value::I64(v) => Ok(v),
            _ => Err(CantCast),
        })
    }
}

impl convert::From<i64> for Value {
    fn from(v: i64) -> Value {
        Value::I64(v)
    }
}

impl FromValue for f32 {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::F32).ok_or(CantCast).and_then(|v| match v {
            Value::F32(v) => Ok(v),
            _ => Err(CantCast),
        })
    }
}

impl convert::From<f32> for Value {
    fn from(v: f32) -> Value {
        Value::F32(v)
    }
}

impl FromValue for f64 {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::F64).ok_or(CantCast).and_then(|v| match v {
            Value::F64(v) => Ok(v),
            _ => Err(CantCast),
        })
    }
}

impl convert::From<f64> for Value {
    fn from(v: f64) -> Value {
        Value::F64(v)
    }
}

impl FromValue for Chars {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::String).ok_or(CantCast).and_then(|v| match v {
            Value::String(v) => Ok(v),
            _ => Err(CantCast),
        })
    }
}

impl convert::From<Chars> for Value {
    fn from(v: Chars) -> Value {
        Value::String(v)
    }
}

impl FromValue for String {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast_to::<Chars>().map(|c| c.into())
    }
}

impl convert::From<String> for Value {
    fn from(v: String) -> Value {
        Value::String(Chars::from(v))
    }
}

impl convert::From<&'static str> for Value {
    fn from(v: &'static str) -> Value {
        Value::String(Chars::from(v))
    }
}

impl FromValue for DateTime<Utc> {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::DateTime).ok_or(CantCast).and_then(|v| match v {
            Value::DateTime(d) => Ok(d),
            _ => Err(CantCast),
        })
    }
}

impl convert::From<DateTime<Utc>> for Value {
    fn from(v: DateTime<Utc>) -> Value {
        Value::DateTime(v)
    }
}

impl FromValue for Duration {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::Duration).ok_or(CantCast).and_then(|v| match v {
            Value::Duration(d) => Ok(d),
            _ => Err(CantCast),
        })
    }
}

impl convert::From<Duration> for Value {
    fn from(v: Duration) -> Value {
        Value::Duration(v)
    }
}

impl FromValue for bool {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::Bool).ok_or(CantCast).and_then(|v| match v {
            Value::True => Ok(true),
            Value::False => Ok(false),
            _ => Err(CantCast),
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
        1 + match self {
            From::NoSuchValue(p) => <Path as Pack>::encoded_len(p),
            From::Denied(p) => <Path as Pack>::encoded_len(p),
            From::Unsubscribed(id) => Id::encoded_len(id),
            From::Subscribed(p, id, v) => {
                <Path as Pack>::encoded_len(p)
                    + Id::encoded_len(id)
                    + Value::encoded_len(v)
            }
            From::Update(id, v) => Id::encoded_len(id) + Value::encoded_len(v),
            From::Heartbeat => 0,
            From::WriteResult(id, v) => Id::encoded_len(id) + Value::encoded_len(v),
        }
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        match self {
            From::NoSuchValue(p) => {
                buf.put_u8(0);
                <Path as Pack>::encode(p, buf)
            }
            From::Denied(p) => {
                buf.put_u8(1);
                <Path as Pack>::encode(p, buf)
            }
            From::Unsubscribed(id) => {
                buf.put_u8(2);
                Id::encode(id, buf)
            }
            From::Subscribed(p, id, v) => {
                buf.put_u8(3);
                <Path as Pack>::encode(p, buf)?;
                Id::encode(id, buf)?;
                Value::encode(v, buf)
            }
            From::Update(id, v) => {
                buf.put_u8(4);
                Id::encode(id, buf)?;
                Value::encode(v, buf)
            }
            From::Heartbeat => Ok(buf.put_u8(5)),
            From::WriteResult(id, v) => {
                buf.put_u8(6);
                Id::encode(id, buf)?;
                Value::encode(v, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
            6 => {
                let id = Id::decode(buf)?;
                let value = Value::decode(buf)?;
                Ok(From::WriteResult(id, value))
            }
            _ => Err(PackError::UnknownTag),
        }
    }
}
