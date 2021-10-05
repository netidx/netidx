use bytes::{Buf, BufMut, Bytes};
use chrono::{naive::NaiveDateTime, prelude::*};
use netidx_core::{
    chars::Chars,
    pack::{self, Pack, PackError},
    utils,
};
use std::{
    convert, error, fmt, iter, mem,
    num::Wrapping,
    ops::{Add, Div, Mul, Not, Sub},
    panic::{catch_unwind, AssertUnwindSafe},
    result,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::value_parser;

type Result<T> = result::Result<T, PackError>;

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
    Array,
    Null,
}

static TYPES: [Typ; 18] = [
    Typ::U32,
    Typ::V32,
    Typ::I32,
    Typ::Z32,
    Typ::U64,
    Typ::V64,
    Typ::I64,
    Typ::Z64,
    Typ::F32,
    Typ::F64,
    Typ::DateTime,
    Typ::Duration,
    Typ::Bool,
    Typ::String,
    Typ::Bytes,
    Typ::Result,
    Typ::Array,
    Typ::Null,
];

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
            Typ::Array => "array",
            Typ::Null => "null",
        }
    }

    pub fn get(v: &Value) -> Self {
        match v {
            Value::U32(_) => Typ::U32,
            Value::V32(_) => Typ::V32,
            Value::I32(_) => Typ::I32,
            Value::Z32(_) => Typ::Z32,
            Value::U64(_) => Typ::U64,
            Value::V64(_) => Typ::V64,
            Value::I64(_) => Typ::I64,
            Value::Z64(_) => Typ::Z64,
            Value::F32(_) => Typ::F32,
            Value::F64(_) => Typ::F64,
            Value::DateTime(_) => Typ::DateTime,
            Value::Duration(_) => Typ::Duration,
            Value::String(_) => Typ::String,
            Value::Bytes(_) => Typ::Bytes,
            Value::True | Value::False => Typ::Bool,
            Value::Null => Typ::Null,
            Value::Ok | Value::Error(_) => Typ::Result,
            Value::Array(_) => Typ::Array,
        }
    }

    pub fn all() -> &'static [Self] {
        &TYPES
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
            "array" => Ok(Typ::Array),
            "null" => Ok(Typ::Null),
            s => Err(anyhow!(
                "invalid type, {}, valid types: u32, i32, u64, i64, f32, f64, bool, string, bytes, result, array, null", s))
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
    /// An array of values
    Array(Arc<[Value]>),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_ext(f, &[], true)
    }
}

impl FromStr for Value {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        value_parser::parse_value(s)
    }
}

macro_rules! apply_op {
    ($self:expr, $rhs:expr, $op:tt, $($($pat:pat)|+ => $blk:block),+) => {
        match ($self, $rhs) {
            (Value::U32(l) | Value::V32(l), Value::U32(r) | Value::V32(r)) => {
                Value::U32((Wrapping(l) $op Wrapping(r)).0)
            }
            (Value::I32(l) | Value::Z32(l), Value::I32(r) | Value::Z32(r)) => {
                Value::I32((Wrapping(l) $op Wrapping(r)).0)
            }
            (Value::U64(l) | Value::V64(l), Value::U64(r) | Value::V64(r)) => {
                Value::U64((Wrapping(l) $op Wrapping(r)).0)
            }
            (Value::I64(l) | Value::Z64(l), Value::I64(r) | Value::Z64(r)) => {
                Value::I64((Wrapping(l) $op Wrapping(r)).0)
            }
            (Value::F32(l), Value::F32(r)) => Value::F32(l $op r),
            (Value::F64(l), Value::F64(r)) => Value::F64(l $op r),
            (Value::U32(l) | Value::V32(l), Value::U64(r) | Value::V64(r)) => {
                Value::U64((Wrapping(l as u64) $op Wrapping(r)).0)
            }
            (Value::U64(l) | Value::V64(l), Value::U32(r) | Value::V32(r)) => {
                Value::U64((Wrapping(l) $op Wrapping(r as u64)).0)
            }
            (Value::I32(l) | Value::Z32(l), Value::I64(r) | Value::Z64(r)) => {
                Value::I64((Wrapping(l as i64) $op Wrapping(r)).0)
            }
            (Value::I32(l) | Value::Z32(l), Value::U32(r) | Value::V32(r)) => {
                Value::I64((Wrapping(l as i64) $op Wrapping(r as i64)).0)
            }
            (Value::U32(l) | Value::V32(l), Value::I32(r) | Value::Z32(r)) => {
                Value::I64((Wrapping(l as i64) $op Wrapping(r as i64)).0)
            }
            (Value::I64(l) | Value::Z64(l), Value::I32(r) | Value::Z32(r)) => {
                Value::I64((Wrapping(l) $op Wrapping(r as i64)).0)
            }
            (Value::I64(l) | Value::Z64(l), Value::U32(r) | Value::V32(r)) => {
                Value::I64((Wrapping(l) $op Wrapping(r as i64)).0)
            }
            (Value::U32(l) | Value::V32(l), Value::I64(r) | Value::Z64(r)) => {
                Value::I64((Wrapping(l as i64) $op Wrapping(r)).0)
            }
            (Value::I64(l) | Value::Z64(l), Value::U64(r) | Value::V64(r)) => {
                Value::I64((Wrapping(l) $op Wrapping(r as i64)).0)
            }
            (Value::U64(l) | Value::V64(l), Value::I64(r) | Value::Z64(r)) => {
                Value::I64((Wrapping(l as i64) $op Wrapping(r)).0)
            }
            (Value::U64(l) | Value::V64(l), Value::I32(r) | Value::Z32(r)) => {
                Value::I64((Wrapping(l as i64) $op Wrapping(r as i64)).0)
            }
            (Value::I32(l) | Value::Z32(l), Value::U64(r) | Value::V64(r)) => {
                Value::I64((Wrapping(l as i64) $op Wrapping(r as i64)).0)
            }
            (Value::F32(l), Value::U32(r) | Value::V32(r)) => Value::F32(l $op r as f32),
            (Value::U32(l) | Value::V32(l), Value::F32(r)) => Value::F32(l as f32 $op r),
            (Value::F32(l), Value::U64(r) | Value::V64(r)) => Value::F32(l $op r as f32),
            (Value::U64(l) | Value::V64(l), Value::F32(r)) => Value::F32(l as f32 $op r),
            (Value::F32(l), Value::I32(r) | Value::Z32(r)) => Value::F32(l $op r  as f32),
            (Value::I32(l) | Value::Z32(l), Value::F32(r)) => Value::F32(l as f32 $op r),
            (Value::F32(l), Value::I64(r) | Value::Z64(r)) => Value::F32(l $op r as f32),
            (Value::I64(l) | Value::Z64(l), Value::F32(r)) => Value::F32(l as f32 $op r),
            (Value::F32(l), Value::F64(r)) => Value::F64(l as f64 $op r),
            (Value::F64(l), Value::U32(r) | Value::V32(r)) => Value::F64(l $op r as f64),
            (Value::U32(l) | Value::V32(l), Value::F64(r)) => Value::F64(l as f64 $op r),
            (Value::F64(l), Value::U64(r) | Value::V64(r)) => Value::F64(l $op r as f64),
            (Value::U64(l) | Value::V64(l), Value::F64(r)) => Value::F64(l as f64 $op r),
            (Value::F64(l), Value::I32(r) | Value::Z32(r)) => Value::F64(l $op r as f64),
            (Value::I32(l) | Value::Z32(l), Value::F64(r)) => Value::F64(l as f64 $op r),
            (Value::F64(l), Value::I64(r) | Value::Z64(r)) => Value::F64(l $op r as f64),
            (Value::I64(l) | Value::Z64(l), Value::F64(r)) => Value::F64(l as f64 $op r),
            (Value::F64(l), Value::F32(r)) => Value::F64(l $op r as f64),
            (Value::String(s), n) => match s.parse::<Value>() {
                Err(e) => Value::Error(Chars::from(format!("{}", e))),
                Ok(s) => s $op n,
            }
            (n, Value::String(s)) => match s.parse::<Value>() {
                Err(e) => Value::Error(Chars::from(format!("{}", e))),
                Ok(s) => n $op s,
            },
            (Value::Array(e0), Value::Array(e1)) => {
                let (e0, e1) = if e0.len() < e1.len() { (e0, e1) } else { (e1, e0) };
                let iter = e0
                    .iter()
                    .cloned()
                    .chain(iter::repeat(Value::F64(0.)))
                    .zip(e1.iter().cloned());
                Value::Array(iter.map(|(v0, v1)| v0 $op v1).collect())
            }
            (l @ Value::Array(_), n) => {
                match n.cast(Typ::Array) {
                    None => Value::Error(Chars::from("can't add to array")),
                    Some(r) => l $op r,
                }
            }
            (n, r @ Value::Array(_)) => {
                match n.cast(Typ::Array) {
                    None => Value::Error(Chars::from("can't add to array")),
                    Some(l) => l $op r,
                }
            }
            (Value::Bytes(_), _) | (_, Value::Bytes(_)) => {
                Value::Error(Chars::from("can't add bytes"))
            }
            (Value::Null, _) | (_, Value::Null) => {
                Value::Error(Chars::from("can't add null"))
            }
            (Value::Ok, _)
            | (_, Value::Ok)
            | (Value::Error(_), _)
            | (_, Value::Error(_)) => Value::Error(Chars::from("can't add result types")),
            (Value::True, n) => Value::U32(1) $op n,
            (n, Value::True) => n $op Value::U32(1),
            (Value::False, n) => Value::U32(0) $op n,
            (n, Value::False) => n $op Value::U32(0),
            $($($pat)|+ => $blk),+
        }
    }
}

impl Add for Value {
    type Output = Value;

    fn add(self, rhs: Self) -> Self {
        apply_op!(
            self, rhs, +,
            (Value::DateTime(dt), Value::Duration(d))
                | (Value::Duration(d), Value::DateTime(dt)) => {
                    match chrono::Duration::from_std(d) {
                        Ok(d) => Value::DateTime(dt + d),
                        Err(e) => Value::Error(Chars::from(format!("{}", e))),
                    }
                },
            (Value::Duration(d0), Value::Duration(d1)) => { Value::Duration(d0 + d1) },
            (Value::Duration(_), _)
                | (_, Value::Duration(_))
                | (_, Value::DateTime(_))
                | (Value::DateTime(_), _) => {
                    Value::Error(Chars::from("can't add to datetime/duration"))
                }
        )
    }
}

impl Sub for Value {
    type Output = Value;

    fn sub(self, rhs: Self) -> Self {
        apply_op!(
            self, rhs, -,
            (Value::DateTime(dt), Value::Duration(d))
                | (Value::Duration(d), Value::DateTime(dt)) => {
                    match chrono::Duration::from_std(d) {
                        Ok(d) => Value::DateTime(dt - d),
                        Err(e) => Value::Error(Chars::from(format!("{}", e))),
                    }
                },
            (Value::Duration(d0), Value::Duration(d1)) => { Value::Duration(d0 - d1) },
            (Value::Duration(_), _)
                | (_, Value::Duration(_))
                | (_, Value::DateTime(_))
                | (Value::DateTime(_), _) => {
                    Value::Error(Chars::from("can't add to datetime/duration"))
                }
        )
    }
}

impl Mul for Value {
    type Output = Value;

    fn mul(self, rhs: Self) -> Self {
        apply_op!(
            self, rhs, *,
            (Value::Duration(_), _)
                | (_, Value::Duration(_))
                | (_, Value::DateTime(_))
                | (Value::DateTime(_), _) => {
                    Value::Error(Chars::from("can't add to datetime/duration"))
                }
        )
    }
}

impl Div for Value {
    type Output = Value;

    fn div(self, rhs: Self) -> Self {
        let res = catch_unwind(AssertUnwindSafe(|| {
            apply_op!(
                self, rhs, *,
                (Value::Duration(d), Value::U32(s)) => { Value::Duration(d / s) },
                (Value::Duration(d), Value::V32(s)) => { Value::Duration(d / s) },
                (Value::Duration(d), Value::F32(s)) => { Value::Duration(d.div_f32(s)) },
                (Value::Duration(d), Value::F64(s)) => { Value::Duration(d.div_f64(s)) },
                (Value::Duration(_), _)
                    | (_, Value::Duration(_))
                    | (_, Value::DateTime(_))
                    | (Value::DateTime(_), _) => {
                        Value::Error(Chars::from("can't add to datetime/duration"))
                    }
            )
        }));
        match res {
            Ok(r) => r,
            Err(_) => Value::Error(Chars::from("can't divide by zero"))
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
            Value::Array(elts) => {
                Value::Array(elts.iter().cloned().map(|v| !v).collect())
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
            Value::Array(elts) => {
                pack::varint_len(elts.len() as u64)
                    + elts.iter().fold(0, |sum, v| sum + Pack::encoded_len(v))
            }
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
            Value::Array(elts) => {
                buf.put_u8(19);
                pack::encode_varint(elts.len() as u64, buf);
                for elt in &**elts {
                    <Value as Pack>::encode(elt, buf)?
                }
                Ok(())
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
            19 => {
                let len = pack::decode_varint(buf)? as usize;
                let mut elts = Vec::with_capacity(len);
                while elts.len() < len {
                    elts.push(<Value as Pack>::decode(buf)?);
                }
                Ok(Value::Array(Arc::from(elts)))
            }
            _ => Err(PackError::UnknownTag),
        }
    }
}

pub trait FromValue {
    type Error: fmt::Debug;

    /// attempt to cast v to the type of self using any reasonable means
    fn from_value(v: Value) -> result::Result<Self, Self::Error>
    where
        Self: Sized;

    /// extract the type of self from v if the type of v is equivelent
    /// to the type of self, otherwise return None.
    fn get(v: Value) -> Option<Self>
    where
        Self: Sized;
}

impl Value {
    pub fn fmt_ext(
        &self,
        f: &mut fmt::Formatter<'_>,
        esc: &[char],
        types: bool,
    ) -> fmt::Result {
        match self {
            Value::U32(v) => {
                if types {
                    write!(f, "u32:{}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
            Value::V32(v) => {
                if types {
                    write!(f, "v32:{}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
            Value::I32(v) => {
                if types {
                    write!(f, "i32:{}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
            Value::Z32(v) => {
                if types {
                    write!(f, "z32:{}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
            Value::U64(v) => {
                if types {
                    write!(f, "u64:{}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
            Value::V64(v) => {
                if types {
                    write!(f, "v64:{}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
            Value::I64(v) => {
                if types {
                    write!(f, "i64:{}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
            Value::Z64(v) => {
                if types {
                    write!(f, "z64:{}", v)
                } else {
                    write!(f, "{}", v)
                }
            }
            Value::F32(v) => {
                let pfx = if types { "f32:" } else { "" };
                if v.fract() == 0. {
                    write!(f, "{}{}.", pfx, v)
                } else {
                    write!(f, "{}{}", pfx, v)
                }
            }
            Value::F64(v) => {
                let pfx = if types { "f64:" } else { "" };
                if v.fract() == 0. {
                    write!(f, "{}{}.", pfx, v)
                } else {
                    write!(f, "{}{}", pfx, v)
                }
            }
            Value::DateTime(v) => {
                if types {
                    write!(f, r#"datetime:"{}""#, v)
                } else {
                    write!(f, r#""{}""#, v)
                }
            }
            Value::Duration(v) => {
                let pfx = if types { "duration:" } else { "" };
                let v = v.as_secs_f64();
                if v.fract() == 0. {
                    write!(f, r#"{}{}.s"#, pfx, v)
                } else {
                    write!(f, r#"{}{}s"#, pfx, v)
                }
            }
            Value::String(s) => {
                write!(f, r#""{}""#, utils::escape(&*s, '\\', esc))
            }
            Value::Bytes(b) => {
                let pfx = if types { "bytes:" } else { "" };
                write!(f, "{}{}", pfx, base64::encode(&*b))
            }
            Value::True => write!(f, "true"),
            Value::False => write!(f, "false"),
            Value::Null => write!(f, "null"),
            Value::Ok => write!(f, "ok"),
            Value::Error(v) => {
                write!(f, r#"error:"{}""#, utils::escape(&*v, '\\', esc))
            }
            Value::Array(elts) => {
                write!(f, "[")?;
                for (i, v) in elts.iter().enumerate() {
                    if i < elts.len() - 1 {
                        v.fmt_ext(f, esc, types)?;
                        write!(f, ", ")?
                    } else {
                        v.fmt_ext(f, esc, types)?
                    }
                }
                write!(f, "]")
            }
        }
    }

    /// Whatever value is attempt to turn it into the type specified
    pub fn cast(self, typ: Typ) -> Option<Value> {
        macro_rules! cast_number {
            ($v:expr, $typ:expr) => {
                match typ {
                    Typ::U32 => Some(Value::U32($v as u32)),
                    Typ::V32 => Some(Value::V32($v as u32)),
                    Typ::I32 => Some(Value::I32($v as i32)),
                    Typ::Z32 => Some(Value::Z32($v as i32)),
                    Typ::U64 => Some(Value::U64($v as u64)),
                    Typ::V64 => Some(Value::V64($v as u64)),
                    Typ::I64 => Some(Value::I64($v as i64)),
                    Typ::Z64 => Some(Value::Z64($v as i64)),
                    Typ::F32 => Some(Value::F32($v as f32)),
                    Typ::F64 => Some(Value::F64($v as f64)),
                    Typ::DateTime => Some(Value::DateTime(DateTime::from_utc(
                        NaiveDateTime::from_timestamp_opt($v as i64, 0)?,
                        Utc,
                    ))),
                    Typ::Duration => {
                        Some(Value::Duration(Duration::from_secs($v as u64)))
                    }
                    Typ::Bool => {
                        Some(if $v as i64 > 0 { Value::True } else { Value::False })
                    }
                    Typ::String => Some(Value::String(Chars::from(format!("{}", self)))),
                    Typ::Bytes => None,
                    Typ::Result => Some(Value::Ok),
                    Typ::Array => {
                        Some(Value::Array(Arc::from(Vec::from([self.clone()]))))
                    }
                    Typ::Null => Some(Value::Null),
                }
            };
        }
        match self {
            Value::String(s) if typ != Typ::String => {
                s.parse::<Value>().ok().and_then(|v| v.cast(typ))
            }
            v @ Value::String(_) => Some(v),
            v if typ == Typ::String => Some(Value::String(Chars::from(format!("{}", v)))),
            Value::Array(elts) if typ != Typ::Array => {
                elts.first().and_then(|v| v.clone().cast(typ))
            }
            v @ Value::Array(_) => Some(v),
            Value::U32(v) | Value::V32(v) => cast_number!(v, typ),
            Value::I32(v) | Value::Z32(v) => cast_number!(v, typ),
            Value::U64(v) | Value::V64(v) => cast_number!(v, typ),
            Value::I64(v) | Value::Z64(v) => cast_number!(v, typ),
            Value::F32(v) => cast_number!(v, typ),
            Value::F64(v) => cast_number!(v, typ),
            Value::DateTime(v) => match typ {
                Typ::U32 | Typ::V32 => {
                    let ts = v.timestamp();
                    if ts < 0 && ts > u32::MAX as i64 {
                        None
                    } else {
                        if typ == Typ::U32 {
                            Some(Value::U32(ts as u32))
                        } else {
                            Some(Value::V32(ts as u32))
                        }
                    }
                }
                Typ::I32 | Typ::Z32 => {
                    let ts = v.timestamp();
                    if ts < i32::MIN as i64 || ts > i32::MAX as i64 {
                        None
                    } else {
                        if typ == Typ::I32 {
                            Some(Value::I32(ts as i32))
                        } else {
                            Some(Value::Z32(ts as i32))
                        }
                    }
                }
                Typ::U64 | Typ::V64 => {
                    let ts = v.timestamp();
                    if ts < 0 {
                        None
                    } else {
                        if typ == Typ::U64 {
                            Some(Value::U64(ts as u64))
                        } else {
                            Some(Value::V64(ts as u64))
                        }
                    }
                }
                Typ::I64 => Some(Value::I64(v.timestamp())),
                Typ::Z64 => Some(Value::Z64(v.timestamp())),
                Typ::F32 | Typ::F64 => {
                    let dur = v.timestamp() as f64;
                    let dur = dur + (v.timestamp_nanos() / 1_000_000_000) as f64;
                    if typ == Typ::F32 {
                        Some(Value::F32(dur as f32))
                    } else {
                        Some(Value::F64(dur))
                    }
                }
                Typ::DateTime => Some(Value::DateTime(v)),
                Typ::Duration => None,
                Typ::Bool => None,
                Typ::Bytes => None,
                Typ::Result => Some(Value::Ok),
                Typ::Array => Some(Value::Array(Arc::from(Vec::from([self])))),
                Typ::Null => Some(Value::Null),
                Typ::String => unreachable!(),
            },
            Value::Duration(d) => match typ {
                Typ::U32 => Some(Value::U32(d.as_secs() as u32)),
                Typ::V32 => Some(Value::V32(d.as_secs() as u32)),
                Typ::I32 => Some(Value::I32(d.as_secs() as i32)),
                Typ::Z32 => Some(Value::Z32(d.as_secs() as i32)),
                Typ::U64 => Some(Value::U64(d.as_secs() as u64)),
                Typ::V64 => Some(Value::V64(d.as_secs() as u64)),
                Typ::I64 => Some(Value::I64(d.as_secs() as i64)),
                Typ::Z64 => Some(Value::Z64(d.as_secs() as i64)),
                Typ::F32 => Some(Value::F32(d.as_secs_f32())),
                Typ::F64 => Some(Value::F64(d.as_secs_f64())),
                Typ::DateTime => None,
                Typ::Duration => Some(Value::Duration(d)),
                Typ::Bool => None,
                Typ::Bytes => None,
                Typ::Result => Some(Value::Ok),
                Typ::Array => Some(Value::Array(Arc::from(Vec::from([self])))),
                Typ::Null => Some(Value::Null),
                Typ::String => unreachable!(),
            },
            Value::True | Value::False => {
                let b = self == Value::True;
                match typ {
                    Typ::U32 => Some(Value::U32(b as u32)),
                    Typ::V32 => Some(Value::V32(b as u32)),
                    Typ::I32 => Some(Value::I32(b as i32)),
                    Typ::Z32 => Some(Value::Z32(b as i32)),
                    Typ::U64 => Some(Value::U64(b as u64)),
                    Typ::V64 => Some(Value::V64(b as u64)),
                    Typ::I64 => Some(Value::I64(b as i64)),
                    Typ::Z64 => Some(Value::Z64(b as i64)),
                    Typ::F32 => Some(Value::F32(b as u32 as f32)),
                    Typ::F64 => Some(Value::F64(b as u64 as f64)),
                    Typ::DateTime => None,
                    Typ::Duration => None,
                    Typ::Bool => Some(self),
                    Typ::Bytes => None,
                    Typ::Result => Some(Value::Ok),
                    Typ::Array => Some(Value::Array(Arc::from(Vec::from([self])))),
                    Typ::Null => Some(Value::Null),
                    Typ::String => unreachable!(),
                }
            }
            Value::Bytes(_) if typ == Typ::Bytes => Some(self),
            Value::Bytes(_) => None,
            Value::Ok => Value::True.cast(typ),
            Value::Error(_) => Value::False.cast(typ),
            Value::Null if typ == Typ::Null => Some(self),
            Value::Null => None,
        }
    }

    /// cast value directly to any type implementing `FromValue`
    pub fn cast_to<T: FromValue + Sized>(self) -> result::Result<T, T::Error> {
        <T as FromValue>::from_value(self)
    }

    pub fn get_as<T: FromValue + Sized>(self) -> Option<T> {
        <T as FromValue>::get(self)
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
            | Value::Error(_)
            | Value::Array(_) => false,
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Some(v as u8),
            Value::U64(v) | Value::V64(v) => Some(v as u8),
            Value::I32(v) | Value::Z32(v) => Some(v as u8),
            Value::I64(v) | Value::Z64(v) => Some(v as u8),
            _ => None,
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Some(v as i8),
            Value::U64(v) | Value::V64(v) => Some(v as i8),
            Value::I32(v) | Value::Z32(v) => Some(v as i8),
            Value::I64(v) | Value::Z64(v) => Some(v as i8),
            _ => None,
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Some(v as u16),
            Value::U64(v) | Value::V64(v) => Some(v as u16),
            Value::I32(v) | Value::Z32(v) => Some(v as u16),
            Value::I64(v) | Value::Z64(v) => Some(v as u16),
            _ => None,
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Some(v as i16),
            Value::U64(v) | Value::V64(v) => Some(v as i16),
            Value::I32(v) | Value::Z32(v) => Some(v as i16),
            Value::I64(v) | Value::Z64(v) => Some(v as i16),
            _ => None,
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Some(v as u32),
            Value::U64(v) | Value::V64(v) => Some(v as u32),
            Value::I32(v) | Value::Z32(v) => Some(v as u32),
            Value::I64(v) | Value::Z64(v) => Some(v as u32),
            _ => None,
        }
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Some(v as i32),
            Value::U64(v) | Value::V64(v) => Some(v as i32),
            Value::I32(v) | Value::Z32(v) => Some(v as i32),
            Value::I64(v) | Value::Z64(v) => Some(v as i32),
            _ => None,
        }
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Some(v as u64),
            Value::U64(v) | Value::V64(v) => Some(v as u64),
            Value::I32(v) | Value::Z32(v) => Some(v as u64),
            Value::I64(v) | Value::Z64(v) => Some(v as u64),
            _ => None,
        }
    }
}

impl convert::From<u64> for Value {
    fn from(v: u64) -> Value {
        Value::U64(v)
    }
}

impl convert::From<usize> for Value {
    fn from(v: usize) -> Value {
        Value::U64(v as u64)
    }
}

impl FromValue for usize {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::U64).ok_or(CantCast).and_then(|v| match v {
            Value::U64(v) => Ok(v as usize),
            _ => Err(CantCast),
        })
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Some(v as usize),
            Value::U64(v) | Value::V64(v) => Some(v as usize),
            Value::I32(v) | Value::Z32(v) => Some(v as usize),
            Value::I64(v) | Value::Z64(v) => Some(v as usize),
            _ => None,
        }
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Some(v as i64),
            Value::U64(v) | Value::V64(v) => Some(v as i64),
            Value::I32(v) | Value::Z32(v) => Some(v as i64),
            Value::I64(v) | Value::Z64(v) => Some(v as i64),
            _ => None,
        }
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::F32(v) => Some(v as f32),
            Value::F64(v) => Some(v as f32),
            _ => None,
        }
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::F32(v) => Some(v as f64),
            Value::F64(v) => Some(v as f64),
            _ => None,
        }
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::String(c) => Some(c),
            _ => None,
        }
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::String(c) => Some(c.into()),
            _ => None,
        }
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::DateTime(d) => Some(d),
            _ => None,
        }
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Duration(d) => Some(d),
            _ => None,
        }
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

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::True => Some(true),
            Value::False => Some(false),
            _ => None,
        }
    }
}

impl convert::From<bool> for Value {
    fn from(v: bool) -> Value {
        if v {
            Value::True
        } else {
            Value::False
        }
    }
}

impl FromValue for Arc<[Value]> {
    type Error = CantCast;

    fn from_value(v: Value) -> result::Result<Self, Self::Error> {
        v.cast(Typ::Array).ok_or(CantCast).and_then(|v| match v {
            Value::Array(elts) => Ok(elts),
            _ => Err(CantCast)
        })
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) => Some(elts),
            _ => None
        }
    }
}

impl convert::From<Arc<[Value]>> for Value {
    fn from(v: Arc<[Value]>) -> Value {
        Value::Array(v)
    }
}
