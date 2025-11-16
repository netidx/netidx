use anyhow::Result;
use arcstr::ArcStr;
use bytes::{Buf, BufMut};
use chrono::prelude::*;
use compact_str::{format_compact, CompactString};
use immutable_chunkmap::map;
use netidx_core::{
    pack::{self, Pack, PackError},
    utils,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::{hint::unreachable_unchecked, iter, ptr, result, str::FromStr, time::Duration};
use triomphe::Arc;

pub mod abstract_type;
pub mod array;
mod convert;
mod op;
pub mod parser;
pub mod pbuf;
mod print;
#[cfg(test)]
mod test;
mod typ;

pub use array::ValArray;
pub use convert::FromValue;
pub use pbuf::PBytes;
pub use print::{printf, NakedValue};
pub use typ::Typ;

use crate::abstract_type::Abstract;

#[macro_export]
macro_rules! valarray {
    ($proto:expr; $size:literal) => {{
        let proto: Value = $proto.into();
        Value::Array(std::array::from_fn::<_, $size, _>(|_| proto.clone()))
    }};
    ($($e:expr),+) => {
        Value::Array([$($e.into()),+].into())
    }
}

fn _test_valarray() {
    let v: Value = valarray![1, 2, 5.3, 10];
    let _: Value = valarray![valarray!["elts", v], valarray!["foo", ["bar"]]];
}

pub type Map = map::Map<Value, Value, 32>;

const COPY_MAX: u32 = 0x0000_8000;

// this type is divided into two subtypes, the copy part and the clone
// part. If the tag word is <= COPY_MAX then the type is copy,
// otherwise it must be cloned. Additionally, the tag word values are
// THE SAME as the values of the cases of Typ. Getting the Typ of a
// value is therefore a simply copy of the discriminant of Value.
//
// It is essential that when adding variants you update COPY_MAX and
// Typ correctly. If adding a non copy type, you will also need to
// update the implementation of clone to match and delegate the clone
// operation
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
#[repr(u32)]
pub enum Value {
    /// unsigned byte
    U8(u8) = 0x0000_0001,
    /// signed byte
    I8(i8) = 0x0000_0002,
    /// u16
    U16(u16) = 0x0000_0004,
    /// i16
    I16(i16) = 0x0000_0008,
    /// full 4 byte u32
    U32(u32) = 0x0000_0010,
    /// LEB128 varint, 1 - 5 bytes depending on value
    V32(u32) = 0x0000_0020,
    /// full 4 byte i32
    I32(i32) = 0x0000_0040,
    /// LEB128 varint zigzag encoded, 1 - 5 bytes depending on abs(value)
    Z32(i32) = 0x0000_0080,
    /// full 8 byte u64
    U64(u64) = 0x0000_0100,
    /// LEB128 varint, 1 - 10 bytes depending on value
    V64(u64) = 0x0000_0200,
    /// full 8 byte i64
    I64(i64) = 0x0000_0400,
    /// LEB128 varint zigzag encoded, 1 - 10 bytes depending on abs(value)
    Z64(i64) = 0x0000_0800,
    /// 4 byte ieee754 single precision float
    F32(f32) = 0x0000_1000,
    /// 8 byte ieee754 double precision float
    F64(f64) = 0x0000_2000,
    /// boolean true
    Bool(bool) = 0x0000_4000,
    /// Empty value
    Null = 0x0000_8000,
    /// unicode string
    String(ArcStr) = 0x8000_0000,
    /// byte array
    Bytes(PBytes) = 0x4000_0000,
    /// An explicit error
    Error(Arc<Value>) = 0x2000_0000,
    /// An array of values
    Array(ValArray) = 0x1000_0000,
    /// A Map of values
    Map(Map) = 0x0800_0000,
    /// fixed point decimal type
    Decimal(Arc<Decimal>) = 0x0400_0000,
    /// UTC timestamp
    DateTime(Arc<DateTime<Utc>>) = 0x0200_0000,
    /// Duration
    Duration(Arc<Duration>) = 0x0100_0000,
    /// Abstract
    Abstract(Abstract) = 0x0080_0000,
}

// This will fail to compile if any variant that is supposed to be
// Copy changes to not Copy. It is never intended to be called, and it
// will panic if it ever is.
fn _assert_variants_are_copy(v: &Value) -> Value {
    let i = match v {
        // copy types
        Value::U8(i) => Value::U8(*i),
        Value::I8(i) => Value::I8(*i),
        Value::U16(i) => Value::U16(*i),
        Value::I16(i) => Value::I16(*i),
        Value::U32(i) | Value::V32(i) => Value::U32(*i),
        Value::I32(i) | Value::Z32(i) => Value::I32(*i),
        Value::U64(i) | Value::V64(i) => Value::U64(*i),
        Value::I64(i) | Value::Z64(i) => Value::I64(*i),
        Value::F32(i) => Value::F32(*i),
        Value::F64(i) => Value::F64(*i),
        Value::Bool(b) => Value::Bool(*b),
        Value::Null => Value::Null,

        // not copy types
        Value::String(i) => Value::String(i.clone()),
        Value::Bytes(i) => Value::Bytes(i.clone()),
        Value::Error(i) => Value::Error(i.clone()),
        Value::Array(i) => Value::Array(i.clone()),
        Value::Map(i) => Value::Map(i.clone()),
        Value::Decimal(i) => Value::Decimal(i.clone()),
        Value::DateTime(i) => Value::DateTime(i.clone()),
        Value::Duration(i) => Value::Duration(i.clone()),
        Value::Abstract(i) => Value::Abstract(i.clone()),
    };
    panic!("{i}")
}

impl Clone for Value {
    fn clone(&self) -> Self {
        if self.is_copy() {
            unsafe { ptr::read(self) }
        } else {
            match self {
                Self::String(c) => Self::String(c.clone()),
                Self::Bytes(b) => Self::Bytes(b.clone()),
                Self::Error(e) => Self::Error(e.clone()),
                Self::Array(a) => Self::Array(a.clone()),
                Self::Map(m) => Self::Map(m.clone()),
                Self::Decimal(d) => Self::Decimal(d.clone()),
                Self::DateTime(d) => Self::DateTime(d.clone()),
                Self::Duration(d) => Self::Duration(d.clone()),
                Self::Abstract(v) => Self::Abstract(v.clone()),
                Self::U8(_)
                | Self::I8(_)
                | Self::U16(_)
                | Self::I16(_)
                | Self::U32(_)
                | Self::V32(_)
                | Self::I32(_)
                | Self::Z32(_)
                | Self::U64(_)
                | Self::V64(_)
                | Self::I64(_)
                | Self::Z64(_)
                | Self::F32(_)
                | Self::F64(_)
                | Self::Bool(_)
                | Self::Null => unsafe { unreachable_unchecked() },
            }
        }
    }

    fn clone_from(&mut self, source: &Self) {
        if self.is_copy() {
            unsafe { ptr::copy_nonoverlapping(source, self, 1) };
        } else {
            match source {
                Self::String(c) => {
                    *self = Self::String(c.clone());
                }
                Self::Bytes(b) => {
                    *self = Self::Bytes(b.clone());
                }
                Self::Error(e) => {
                    *self = Self::Error(e.clone());
                }
                Self::Array(a) => {
                    *self = Self::Array(a.clone());
                }
                Self::Map(m) => {
                    *self = Self::Map(m.clone());
                }
                Value::Decimal(d) => {
                    *self = Self::Decimal(d.clone());
                }
                Value::DateTime(d) => {
                    *self = Self::DateTime(d.clone());
                }
                Value::Duration(d) => {
                    *self = Self::Duration(d.clone());
                }
                Value::Abstract(v) => {
                    *self = Self::Abstract(v.clone());
                }
                Value::I8(_)
                | Value::U8(_)
                | Value::U16(_)
                | Value::I16(_)
                | Value::U32(_)
                | Value::V32(_)
                | Value::I32(_)
                | Value::Z32(_)
                | Value::U64(_)
                | Value::V64(_)
                | Value::I64(_)
                | Value::Z64(_)
                | Value::F32(_)
                | Value::F64(_)
                | Value::Bool(_)
                | Value::Null => unsafe { unreachable_unchecked() },
            }
        }
    }
}

impl FromStr for Value {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        parser::parse_value(s)
    }
}

impl Pack for Value {
    fn encoded_len(&self) -> usize {
        1 + match self {
            Value::U8(v) => Pack::encoded_len(v),
            Value::I8(v) => Pack::encoded_len(v),
            Value::U16(v) => Pack::encoded_len(v),
            Value::I16(v) => Pack::encoded_len(v),
            Value::U32(v) => Pack::encoded_len(v),
            Value::V32(v) => pack::varint_len(*v as u64),
            Value::I32(v) => Pack::encoded_len(v),
            Value::Z32(v) => pack::varint_len(pack::i32_zz(*v) as u64),
            Value::U64(v) => Pack::encoded_len(v),
            Value::V64(v) => pack::varint_len(*v),
            Value::I64(v) => Pack::encoded_len(v),
            Value::Z64(v) => pack::varint_len(pack::i64_zz(*v) as u64),
            Value::F32(v) => Pack::encoded_len(v),
            Value::F64(v) => Pack::encoded_len(v),
            Value::DateTime(d) => Pack::encoded_len(d),
            Value::Duration(d) => Pack::encoded_len(d),
            Value::String(c) => Pack::encoded_len(c),
            Value::Bytes(b) => Pack::encoded_len(b),
            Value::Bool(_) | Value::Null => 0,
            Value::Error(c) => match &**c {
                Value::String(s) => Pack::encoded_len(s),
                v => Pack::encoded_len(v),
            },
            Value::Array(elts) => Pack::encoded_len(elts),
            Value::Decimal(d) => Pack::encoded_len(d),
            Value::Map(m) => Pack::encoded_len(m),
            Value::Abstract(v) => Pack::encoded_len(v),
        }
    }

    // the high two bits of the tag are reserved for wrapper types,
    // max tag is therefore 0x3F
    fn encode(&self, buf: &mut impl BufMut) -> result::Result<(), PackError> {
        match self {
            Value::U32(i) => {
                buf.put_u8(0);
                Pack::encode(i, buf)
            }
            Value::V32(i) => {
                buf.put_u8(1);
                Ok(pack::encode_varint(*i as u64, buf))
            }
            Value::I32(i) => {
                buf.put_u8(2);
                Pack::encode(i, buf)
            }
            Value::Z32(i) => {
                buf.put_u8(3);
                Ok(pack::encode_varint(pack::i32_zz(*i) as u64, buf))
            }
            Value::U64(i) => {
                buf.put_u8(4);
                Pack::encode(i, buf)
            }
            Value::V64(i) => {
                buf.put_u8(5);
                Ok(pack::encode_varint(*i, buf))
            }
            Value::I64(i) => {
                buf.put_u8(6);
                Pack::encode(i, buf)
            }
            Value::Z64(i) => {
                buf.put_u8(7);
                Ok(pack::encode_varint(pack::i64_zz(*i), buf))
            }
            Value::F32(i) => {
                buf.put_u8(8);
                Pack::encode(i, buf)
            }
            Value::F64(i) => {
                buf.put_u8(9);
                Pack::encode(i, buf)
            }
            Value::DateTime(dt) => {
                buf.put_u8(10);
                Pack::encode(dt, buf)
            }
            Value::Duration(d) => {
                buf.put_u8(11);
                Pack::encode(d, buf)
            }
            Value::String(s) => {
                buf.put_u8(12);
                Pack::encode(s, buf)
            }
            Value::Bytes(b) => {
                buf.put_u8(13);
                Pack::encode(b, buf)
            }
            Value::Bool(true) => Ok(buf.put_u8(14)),
            Value::Bool(false) => Ok(buf.put_u8(15)),
            Value::Null => Ok(buf.put_u8(16)),
            //          OK is deprecated, but we reserve 17 for backwards compatibility
            //          Value::Ok => Ok(buf.put_u8(17))
            // string error is encoded as 18 for backwards compatibility
            Value::Array(elts) => {
                buf.put_u8(19);
                Pack::encode(elts, buf)
            }
            Value::Decimal(d) => {
                buf.put_u8(20);
                Pack::encode(d, buf)
            }
            Value::Map(m) => {
                buf.put_u8(21);
                Pack::encode(m, buf)
            }
            Value::Error(e) => match &**e {
                Value::String(s) => {
                    buf.put_u8(18);
                    Pack::encode(s, buf)
                }
                v => {
                    buf.put_u8(22);
                    Pack::encode(v, buf)
                }
            },
            Value::U8(i) => {
                buf.put_u8(23);
                Pack::encode(i, buf)
            }
            Value::I8(i) => {
                buf.put_u8(24);
                Pack::encode(i, buf)
            }
            Value::U16(i) => {
                buf.put_u8(25);
                Pack::encode(i, buf)
            }
            Value::I16(i) => {
                buf.put_u8(26);
                Pack::encode(i, buf)
            }
            Value::Abstract(v) => {
                buf.put_u8(27);
                Pack::encode(v, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> result::Result<Self, PackError> {
        match <u8 as Pack>::decode(buf)? {
            0 => Ok(Value::U32(Pack::decode(buf)?)),
            1 => Ok(Value::V32(pack::decode_varint(buf)? as u32)),
            2 => Ok(Value::I32(Pack::decode(buf)?)),
            3 => Ok(Value::Z32(pack::i32_uzz(pack::decode_varint(buf)? as u32))),
            4 => Ok(Value::U64(Pack::decode(buf)?)),
            5 => Ok(Value::V64(pack::decode_varint(buf)?)),
            6 => Ok(Value::I64(Pack::decode(buf)?)),
            7 => Ok(Value::Z64(pack::i64_uzz(pack::decode_varint(buf)?))),
            8 => Ok(Value::F32(Pack::decode(buf)?)),
            9 => Ok(Value::F64(Pack::decode(buf)?)),
            10 => Ok(Value::DateTime(Pack::decode(buf)?)),
            11 => Ok(Value::Duration(Pack::decode(buf)?)),
            12 => Ok(Value::String(Pack::decode(buf)?)),
            13 => Ok(Value::Bytes(Pack::decode(buf)?)),
            14 => Ok(Value::Bool(true)),
            15 => Ok(Value::Bool(false)),
            16 => Ok(Value::Null),
            17 => Ok(Value::Null), // 17 used to be Ok now translated to Null
            18 => {
                // backwards compatible with previous encodings of error when it
                // was only a string
                Ok(Value::Error(Arc::new(Value::String(<ArcStr as Pack>::decode(buf)?))))
            }
            19 => Ok(Value::Array(Pack::decode(buf)?)),
            20 => Ok(Value::Decimal(Pack::decode(buf)?)),
            21 => Ok(Value::Map(Pack::decode(buf)?)),
            22 => Ok(Value::Error(Arc::new(Pack::decode(buf)?))),
            23 => Ok(Value::U8(Pack::decode(buf)?)),
            24 => Ok(Value::I8(Pack::decode(buf)?)),
            25 => Ok(Value::U16(Pack::decode(buf)?)),
            26 => Ok(Value::I16(Pack::decode(buf)?)),
            27 => Ok(Value::Abstract(Pack::decode(buf)?)),
            _ => Err(PackError::UnknownTag),
        }
    }
}

impl Value {
    pub fn approx_eq(&self, v: &Self) -> bool {
        use std::num::FpCategory::*;
        match (self, v) {
            (Value::U32(l) | Value::V32(l), Value::U32(r) | Value::V32(r)) => l == r,
            (Value::I32(l) | Value::Z32(l), Value::I32(r) | Value::Z32(r)) => l == r,
            (Value::U64(l) | Value::V64(l), Value::U64(r) | Value::V64(r)) => l == r,
            (Value::I64(l) | Value::Z64(l), Value::I64(r) | Value::Z64(r)) => l == r,
            (Value::F32(l), Value::F32(r)) => match (l.classify(), r.classify()) {
                (Nan, Nan) => true,
                (Zero, Zero) => true,
                (_, _) => (l - r).abs() <= f32::EPSILON,
            },
            (Value::F64(l), Value::F64(r)) => match (l.classify(), r.classify()) {
                (Nan, Nan) => true,
                (Zero, Zero) => true,
                (_, _) => (l - r).abs() <= f64::EPSILON,
            },
            (Value::Decimal(l), Value::Decimal(r)) => l == r,
            (Value::DateTime(l), Value::DateTime(r)) => l == r,
            (Value::Duration(l), Value::Duration(r)) => {
                (l.as_secs_f64() - r.as_secs_f64()).abs() <= f64::EPSILON
            }
            (Value::String(l), Value::String(r)) => l == r,
            (Value::Bytes(l), Value::Bytes(r)) => l == r,
            (Value::Bool(l), Value::Bool(r)) => l == r,
            (Value::Null, Value::Null) => true,
            (Value::Error(l), Value::Error(r)) => l.approx_eq(r),
            (Value::Array(l), Value::Array(r)) => {
                l.len() == r.len()
                    && l.iter().zip(r.iter()).all(|(v0, v1)| v0.approx_eq(v1))
            }
            (Value::Map(l), Value::Map(r)) => {
                l.len() == r.len()
                    && l.into_iter()
                        .zip(r.into_iter())
                        .all(|((k0, v0), (k1, v1))| k0.approx_eq(k1) && v0.approx_eq(v1))
            }
            (Value::Array(_), _) | (_, Value::Array(_)) => false,
            (l, r) if l.number() || r.number() => {
                match (l.clone().cast_to::<f64>(), r.clone().cast_to::<f64>()) {
                    (Ok(l), Ok(r)) => match (l.classify(), r.classify()) {
                        (Nan, Nan) => true,
                        (Zero, Zero) => true,
                        (_, _) => (l - r).abs() <= f64::EPSILON,
                    },
                    (_, _) => false,
                }
            }
            (_, _) => false,
        }
    }

    pub fn discriminant(&self) -> u32 {
        unsafe { *<*const _>::from(self).cast::<u32>() }
    }

    pub fn is_copy(&self) -> bool {
        self.discriminant() <= COPY_MAX
    }

    /// Whatever value is attempt to turn it into the type specified
    pub fn cast(self, typ: Typ) -> Option<Value> {
        macro_rules! cast_number {
            ($v:expr, $typ:expr) => {
                match typ {
                    Typ::U8 => Some(Value::U8($v as u8)),
                    Typ::I8 => Some(Value::I8($v as i8)),
                    Typ::U16 => Some(Value::U16($v as u16)),
                    Typ::I16 => Some(Value::I16($v as i16)),
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
                    Typ::Decimal => match Decimal::try_from($v) {
                        Ok(d) => Some(Value::Decimal(Arc::new(d))),
                        Err(_) => None,
                    },
                    Typ::DateTime => Some(Value::DateTime(Arc::new(
                        DateTime::from_timestamp($v as i64, 0)?,
                    ))),
                    Typ::Duration => {
                        Some(Value::Duration(Arc::new(Duration::from_secs($v as u64))))
                    }
                    Typ::Bool => Some(if $v as i64 > 0 {
                        Value::Bool(true)
                    } else {
                        Value::Bool(false)
                    }),
                    Typ::String => {
                        Some(Value::String(format_compact!("{}", self).as_str().into()))
                    }
                    Typ::Array => Some(Value::Array([self.clone()].into())),
                    Typ::Null => Some(Value::Null),
                    Typ::Bytes | Typ::Error | Typ::Map | Typ::Abstract => None,
                }
            };
        }
        match self {
            Value::String(s) => match typ {
                Typ::String => Some(Value::String(s)),
                Typ::Error => Some(Value::Error(Arc::new(Value::String(s)))),
                Typ::Array => Some(Value::Array([Value::String(s)].into())),
                _ => s.parse::<Value>().ok().and_then(|v| v.cast(typ)),
            },
            v if typ == Typ::String => {
                Some(Value::String(format_compact!("{}", v).as_str().into()))
            }
            Value::Map(m) => match typ {
                Typ::Map => Some(Value::Map(m)),
                Typ::Array => Some(Value::Array(ValArray::from_iter(m.into_iter().map(
                    |(k, v)| {
                        Value::Array(ValArray::from_iter_exact(
                            [k.clone(), v.clone()].into_iter(),
                        ))
                    },
                )))),
                _ => None,
            },
            Value::Array(elts) => match typ {
                Typ::Array => Some(Value::Array(elts)),
                Typ::Map => {
                    match Value::Array(elts).cast_to::<SmallVec<[(Value, Value); 8]>>() {
                        Err(_) => None,
                        Ok(vals) => Some(Value::Map(Map::from_iter(vals))),
                    }
                }
                typ => elts.first().and_then(|v| v.clone().cast(typ)),
            },
            Value::U8(v) => cast_number!(v, typ),
            Value::I8(v) => cast_number!(v, typ),
            Value::U16(v) => cast_number!(v, typ),
            Value::I16(v) => cast_number!(v, typ),
            Value::U32(v) | Value::V32(v) => cast_number!(v, typ),
            Value::I32(v) | Value::Z32(v) => cast_number!(v, typ),
            Value::U64(v) | Value::V64(v) => cast_number!(v, typ),
            Value::I64(v) | Value::Z64(v) => cast_number!(v, typ),
            Value::F32(v) => cast_number!(v, typ),
            Value::F64(v) => cast_number!(v, typ),
            Value::Decimal(v) => match typ {
                Typ::Decimal => Some(Value::Decimal(v)),
                Typ::U8 => (*v).try_into().ok().map(Value::U8),
                Typ::I8 => (*v).try_into().ok().map(Value::I8),
                Typ::U16 => (*v).try_into().ok().map(Value::U16),
                Typ::I16 => (*v).try_into().ok().map(Value::I16),
                Typ::U32 => (*v).try_into().ok().map(Value::U32),
                Typ::V32 => (*v).try_into().ok().map(Value::V32),
                Typ::I32 => (*v).try_into().ok().map(Value::I32),
                Typ::Z32 => (*v).try_into().ok().map(Value::Z32),
                Typ::U64 => (*v).try_into().ok().map(Value::U64),
                Typ::V64 => (*v).try_into().ok().map(Value::V64),
                Typ::I64 => (*v).try_into().ok().map(Value::I64),
                Typ::Z64 => (*v).try_into().ok().map(Value::Z64),
                Typ::F32 => (*v).try_into().ok().map(Value::F32),
                Typ::F64 => (*v).try_into().ok().map(Value::F64),
                Typ::String => {
                    Some(Value::String(format_compact!("{}", v).as_str().into()))
                }
                Typ::Bool
                | Typ::Array
                | Typ::Map
                | Typ::Abstract
                | Typ::Bytes
                | Typ::DateTime
                | Typ::Duration
                | Typ::Null
                | Typ::Error => None,
            },
            Value::DateTime(ref v) => match typ {
                Typ::U8 | Typ::I8 | Typ::U16 | Typ::I16 => None,
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
                    let dur = dur + (v.timestamp_nanos_opt()? / 1_000_000_000) as f64;
                    if typ == Typ::F32 {
                        Some(Value::F32(dur as f32))
                    } else {
                        Some(Value::F64(dur))
                    }
                }
                Typ::DateTime => Some(Value::DateTime(v.clone())),
                Typ::Array => Some(Value::Array([self].into())),
                Typ::Null => Some(Value::Null),
                Typ::String => unreachable!(),
                Typ::Decimal
                | Typ::Duration
                | Typ::Bool
                | Typ::Bytes
                | Typ::Error
                | Typ::Map
                | Typ::Abstract => None,
            },
            Value::Duration(ref d) => match typ {
                Typ::U8 | Typ::I8 | Typ::U16 | Typ::I16 => None,
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
                Typ::Array => Some(Value::Array([self].into())),
                Typ::Duration => Some(Value::Duration(d.clone())),
                Typ::Null => Some(Value::Null),
                Typ::String => unreachable!(),
                Typ::Decimal
                | Typ::DateTime
                | Typ::Bool
                | Typ::Bytes
                | Typ::Error
                | Typ::Map
                | Typ::Abstract => None,
            },
            Value::Bool(b) => match typ {
                Typ::U8 => Some(Value::U8(b as u8)),
                Typ::I8 => Some(Value::I8(b as i8)),
                Typ::U16 => Some(Value::U16(b as u16)),
                Typ::I16 => Some(Value::I16(b as i16)),
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
                Typ::Bool => Some(self),
                Typ::Array => Some(Value::Array([self].into())),
                Typ::Null => Some(Value::Null),
                Typ::String => unreachable!(),
                Typ::Decimal
                | Typ::DateTime
                | Typ::Duration
                | Typ::Bytes
                | Typ::Error
                | Typ::Map
                | Typ::Abstract => None,
            },
            Value::Bytes(_) if typ == Typ::Bytes => Some(self),
            Value::Bytes(_) => None,
            Value::Error(_) => Value::Bool(false).cast(typ),
            Value::Null if typ == Typ::Null => Some(self),
            Value::Null => None,
            Value::Abstract(_) if typ == Typ::Abstract => Some(self),
            Value::Abstract(_) => None,
        }
    }

    /// cast value directly to any type implementing `FromValue`
    pub fn cast_to<T: FromValue + Sized>(self) -> Result<T> {
        <T as FromValue>::from_value(self)
    }

    pub fn get_as<T: FromValue + Sized>(self) -> Option<T> {
        <T as FromValue>::get(self)
    }

    pub fn err<T: std::error::Error>(e: T) -> Value {
        use std::fmt::Write;
        let mut tmp = CompactString::new("");
        write!(tmp, "{e}").unwrap();
        Value::Error(Arc::new(Value::String(tmp.as_str().into())))
    }

    pub fn error<S: Into<ArcStr>>(e: S) -> Value {
        Value::Error(Arc::new(Value::String(e.into())))
    }

    /// return true if the value is some kind of number, otherwise
    /// false.
    pub fn number(&self) -> bool {
        match self {
            Value::U8(_)
            | Value::I8(_)
            | Value::U16(_)
            | Value::I16(_)
            | Value::U32(_)
            | Value::V32(_)
            | Value::I32(_)
            | Value::Z32(_)
            | Value::U64(_)
            | Value::V64(_)
            | Value::I64(_)
            | Value::Z64(_)
            | Value::F32(_)
            | Value::F64(_)
            | Value::Decimal(_) => true,
            Value::DateTime(_)
            | Value::Duration(_)
            | Value::String(_)
            | Value::Bytes(_)
            | Value::Bool(_)
            | Value::Null
            | Value::Error(_)
            | Value::Array(_)
            | Value::Map(_)
            | Value::Abstract(_) => false,
        }
    }

    /// return an iterator that will perform a depth first traversal
    /// of the specified value. All array elements will be flattened
    /// into non array values.
    pub fn flatten(self) -> impl Iterator<Item = Value> {
        use utils::Either;
        match self {
            Value::Array(elts) => {
                let mut stack: SmallVec<[(ValArray, usize); 8]> = SmallVec::new();
                stack.push((elts, 0));
                Either::Left(iter::from_fn(move || loop {
                    match stack.last_mut() {
                        None => break None,
                        Some((elts, pos)) => {
                            if *pos >= elts.len() {
                                stack.pop();
                            } else {
                                match &elts[*pos] {
                                    Value::Array(elts) => {
                                        *pos += 1;
                                        let elts = elts.clone();
                                        stack.push((elts, 0));
                                    }
                                    val => {
                                        *pos += 1;
                                        break Some(val.clone());
                                    }
                                }
                            }
                        }
                    }
                }))
            }
            val => Either::Right(iter::once(val)),
        }
    }
}
