use crate::{pbuf::PBytes, valarray::ValArray, value_parser};
use anyhow::{bail, Result as Res};
use arcstr::{literal, ArcStr};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bytes::{Buf, BufMut, Bytes};
use chrono::prelude::*;
use compact_str::{format_compact, CompactString};
use enumflags2::bitflags;
use fxhash::FxHashMap;
use indexmap::{IndexMap, IndexSet};
use netidx_core::{
    pack::{self, Pack, PackError},
    path::Path,
    pool::{Pool, Pooled},
    utils,
};
use rust_decimal::Decimal;
use smallvec::SmallVec;
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    cmp::{Ordering, PartialEq, PartialOrd},
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    convert, fmt,
    hash::{BuildHasher, Hash},
    hint::unreachable_unchecked,
    iter, mem,
    num::Wrapping,
    ops::{Add, Div, Mul, Not, Sub},
    panic::{catch_unwind, AssertUnwindSafe},
    ptr, result,
    str::FromStr,
    time::Duration,
};

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

type Result<T> = result::Result<T, PackError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u32)]
#[bitflags]
pub enum Typ {
    U32 = 0x0000_0001,
    V32 = 0x0000_0002,
    I32 = 0x0000_0004,
    Z32 = 0x0000_0008,
    U64 = 0x0000_0010,
    V64 = 0x0000_0020,
    I64 = 0x0000_0040,
    Z64 = 0x0000_0080,
    F32 = 0x0000_0100,
    F64 = 0x0000_0200,
    Decimal = 0x0000_0400,
    DateTime = 0x0000_0800,
    Duration = 0x0000_1000,
    Bool = 0x0000_2000,
    Null = 0x0000_4000,
    String = 0x8000_0000,
    Bytes = 0x4000_0000,
    Error = 0x2000_0000,
    Array = 0x1000_0000,
}

impl Typ {
    pub fn parse(&self, s: &str) -> anyhow::Result<Value> {
        match self {
            Typ::U32 => Ok(Value::U32(s.parse::<u32>()?)),
            Typ::V32 => Ok(Value::V32(s.parse::<u32>()?)),
            Typ::I32 => Ok(Value::I32(s.parse::<i32>()?)),
            Typ::Z32 => Ok(Value::Z32(s.parse::<i32>()?)),
            Typ::U64 => Ok(Value::U64(s.parse::<u64>()?)),
            Typ::V64 => Ok(Value::V64(s.parse::<u64>()?)),
            Typ::I64 => Ok(Value::I64(s.parse::<i64>()?)),
            Typ::Z64 => Ok(Value::Z64(s.parse::<i64>()?)),
            Typ::F32 => Ok(Value::F32(s.parse::<f32>()?)),
            Typ::F64 => Ok(Value::F64(s.parse::<f64>()?)),
            Typ::Decimal => Ok(Value::Decimal(s.parse::<Decimal>()?)),
            Typ::DateTime => Ok(Value::DateTime(DateTime::from_str(s)?)),
            Typ::Duration => {
                let mut tmp = String::from("duration:");
                tmp.push_str(s);
                Ok(tmp.parse::<Value>()?)
            }
            Typ::Bool => Ok(Value::Bool(s.parse::<bool>()?)),
            Typ::String => Ok(Value::String(ArcStr::from(s))),
            Typ::Bytes => {
                let mut tmp = String::from("bytes:");
                tmp.push_str(s);
                Ok(tmp.parse::<Value>()?)
            }
            Typ::Error => Ok(s.parse::<Value>()?),
            Typ::Array => Ok(s.parse::<Value>()?),
            Typ::Null => {
                if s.trim() == "null" {
                    Ok(Value::Null)
                } else {
                    bail!("expected null")
                }
            }
        }
    }

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
            Typ::Decimal => "decimal",
            Typ::DateTime => "datetime",
            Typ::Duration => "duration",
            Typ::Bool => "bool",
            Typ::String => "string",
            Typ::Bytes => "bytes",
            Typ::Error => "error",
            Typ::Array => "array",
            Typ::Null => "null",
        }
    }

    pub fn get(v: &Value) -> Self {
        // safe because we are repr(u32) and because the tags are the
        // same between Typ and Value
        unsafe { mem::transmute::<u32, Typ>(v.discriminant()) }
    }

    pub fn any() -> BitFlags<Typ> {
        BitFlags::all()
    }

    pub fn number() -> BitFlags<Typ> {
        Typ::U32
            | Typ::V32
            | Typ::I32
            | Typ::Z32
            | Typ::U64
            | Typ::V64
            | Typ::I64
            | Typ::Z64
            | Typ::F32
            | Typ::F64
            | Typ::Decimal
    }

    pub fn is_number(&self) -> bool {
        Self::number().contains(*self)
    }

    pub fn integer() -> BitFlags<Typ> {
        Typ::U32
            | Typ::V32
            | Typ::I32
            | Typ::Z32
            | Typ::U64
            | Typ::V64
            | Typ::I64
            | Typ::Z64
    }

    pub fn is_integer(&self) -> bool {
        Self::integer().contains(*self)
    }

    pub fn signed_integer() -> BitFlags<Typ> {
        Typ::I32 | Typ::Z32 | Typ::I64 | Typ::Z64
    }

    pub fn is_signed_integer(&self) -> bool {
        Self::signed_integer().contains(*self)
    }

    pub fn unsigned_integer() -> BitFlags<Typ> {
        Typ::U32 | Typ::V32 | Typ::U64 | Typ::V64
    }

    pub fn is_unsigned_integer(&self) -> bool {
        Self::unsigned_integer().contains(*self)
    }

    pub fn real() -> BitFlags<Typ> {
        Typ::F32 | Typ::F64 | Typ::Decimal
    }

    pub fn is_real(&self) -> bool {
        Self::real().contains(*self)
    }
}

impl FromStr for Typ {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        match s {
            "U32" | "u32" => Ok(Typ::U32),
            "V32" | "v32" => Ok(Typ::V32),
            "I32" | "i32" => Ok(Typ::I32),
            "Z32" | "z32" => Ok(Typ::Z32),
            "U64" | "u64" => Ok(Typ::U64),
            "V64" | "v64" => Ok(Typ::V64),
            "I64" | "i64" => Ok(Typ::I64),
            "Z64" | "z64" => Ok(Typ::Z64),
            "F32" | "f32" => Ok(Typ::F32),
            "F64" | "f64" => Ok(Typ::F64),
            "Decimal" | "decimal" => Ok(Typ::Decimal),
            "DateTime" | "datetime" => Ok(Typ::DateTime),
            "Duration" | "duration" => Ok(Typ::Duration),
            "Bool" | "bool" => Ok(Typ::Bool),
            "String" | "string" => Ok(Typ::String),
            "Bytes" | "bytes" => Ok(Typ::Bytes),
            "Error" | "error" => Ok(Typ::Error),
            "Array" | "array" => Ok(Typ::Array),
            "Null" | "null" => Ok(Typ::Null),
            s => Err(anyhow!(
                "invalid type, {}, valid types: u32, i32, u64, i64, f32, f64, bool, string, bytes, error, array, null", s))
        }
    }
}

impl fmt::Display for Typ {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

const COPY_MAX: u32 = 0x0000_4000;

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
    /// full 4 byte u32
    U32(u32) = 0x0000_0001,
    /// LEB128 varint, 1 - 5 bytes depending on value
    V32(u32) = 0x0000_0002,
    /// full 4 byte i32
    I32(i32) = 0x0000_0004,
    /// LEB128 varint zigzag encoded, 1 - 5 bytes depending on abs(value)
    Z32(i32) = 0x0000_0008,
    /// full 8 byte u64
    U64(u64) = 0x0000_0010,
    /// LEB128 varint, 1 - 10 bytes depending on value
    V64(u64) = 0x0000_0020,
    /// full 8 byte i64
    I64(i64) = 0x0000_0040,
    /// LEB128 varint zigzag encoded, 1 - 10 bytes depending on abs(value)
    Z64(i64) = 0x0000_0080,
    /// 4 byte ieee754 single precision float
    F32(f32) = 0x0000_0100,
    /// 8 byte ieee754 double precision float
    F64(f64) = 0x0000_0200,
    /// fixed point decimal type
    Decimal(Decimal) = 0x0000_0400,
    /// UTC timestamp
    DateTime(DateTime<Utc>) = 0x0000_0800,
    /// Duration
    Duration(Duration) = 0x0000_1000,
    /// boolean true
    Bool(bool) = 0x0000_2000,
    /// Empty value
    Null = 0x0000_4000,
    /// unicode string
    String(ArcStr) = 0x8000_0000,
    /// byte array
    Bytes(PBytes) = 0x4000_0000,
    /// An explicit error
    Error(ArcStr) = 0x2000_0000,
    /// An array of values
    Array(ValArray) = 0x1000_0000,
}

// This will fail to compile if any variant that is supposed to be
// Copy changes to not Copy. It is never intended to be called, and it
// will panic if it ever is.
fn _assert_variants_are_copy(v: &Value) -> Value {
    let i = match v {
        // copy types
        Value::U32(i) | Value::V32(i) => Value::U32(*i),
        Value::I32(i) | Value::Z32(i) => Value::I32(*i),
        Value::U64(i) | Value::V64(i) => Value::U64(*i),
        Value::I64(i) | Value::Z64(i) => Value::I64(*i),
        Value::F32(i) => Value::F32(*i),
        Value::F64(i) => Value::F64(*i),
        Value::Decimal(i) => Value::Decimal(*i),
        Value::DateTime(i) => Value::DateTime(*i),
        Value::Duration(i) => Value::Duration(*i),
        Value::Bool(b) => Value::Bool(*b),
        Value::Null => Value::Null,

        // not copy types
        Value::String(i) => Value::String(i.clone()),
        Value::Bytes(i) => Value::Bytes(i.clone()),
        Value::Error(i) => Value::Error(i.clone()),
        Value::Array(i) => Value::Array(i.clone()),
    };
    panic!("{i}")
}

// CR estokes: evaluate the performance implications of this new repr
// which enables this optimized clone implementation
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
                Value::U32(_)
                | Value::V32(_)
                | Value::I32(_)
                | Value::Z32(_)
                | Value::U64(_)
                | Value::V64(_)
                | Value::I64(_)
                | Value::Z64(_)
                | Value::F32(_)
                | Value::F64(_)
                | Value::Decimal(_)
                | Value::DateTime(_)
                | Value::Duration(_)
                | Value::Bool(_)
                | Value::Null => unsafe { unreachable_unchecked() },
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
                Value::U32(_)
                | Value::V32(_)
                | Value::I32(_)
                | Value::Z32(_)
                | Value::U64(_)
                | Value::V64(_)
                | Value::I64(_)
                | Value::Z64(_)
                | Value::F32(_)
                | Value::F64(_)
                | Value::Decimal(_)
                | Value::DateTime(_)
                | Value::Duration(_)
                | Value::Bool(_)
                | Value::Null => unsafe { unreachable_unchecked() },
            }
        }
    }
}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use std::num::FpCategory::*;
        match self {
            Value::U32(v) => {
                0u8.hash(state);
                v.hash(state)
            }
            Value::V32(v) => {
                1u8.hash(state);
                v.hash(state)
            }
            Value::I32(v) => {
                2u8.hash(state);
                v.hash(state)
            }
            Value::Z32(v) => {
                3u8.hash(state);
                v.hash(state)
            }
            Value::U64(v) => {
                4u8.hash(state);
                v.hash(state)
            }
            Value::V64(v) => {
                5u8.hash(state);
                v.hash(state)
            }
            Value::I64(v) => {
                6u8.hash(state);
                v.hash(state)
            }
            Value::Z64(v) => {
                7u8.hash(state);
                v.hash(state)
            }
            Value::F32(v) => {
                8u8.hash(state);
                let bits = v.to_bits();
                match v.classify() {
                    Nan => ((bits & 0xFF00_0000) | 0x1).hash(state), // normalize NaN
                    _ => bits.hash(state),
                }
            }
            Value::F64(v) => {
                9u8.hash(state);
                let bits = v.to_bits();
                match v.classify() {
                    Nan => ((bits & 0xFFE0_0000_0000_0000) | 0x1).hash(state), // normalize NaN
                    _ => bits.hash(state),
                }
            }
            Value::DateTime(d) => {
                10u8.hash(state);
                d.hash(state)
            }
            Value::Duration(d) => {
                11u8.hash(state);
                d.hash(state)
            }
            Value::String(c) => {
                12u8.hash(state);
                c.hash(state)
            }
            Value::Bytes(b) => {
                13u8.hash(state);
                b.hash(state)
            }
            Value::Bool(true) => 14u8.hash(state),
            Value::Bool(false) => 15u8.hash(state),
            Value::Null => 16u8.hash(state),
            Value::Error(c) => {
                18u8.hash(state);
                c.hash(state)
            }
            Value::Array(a) => {
                19u8.hash(state);
                for v in a.iter() {
                    v.hash(state)
                }
            }
            Value::Decimal(d) => {
                20u8.hash(state);
                d.hash(state);
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, rhs: &Value) -> bool {
        use std::num::FpCategory::*;
        match (self, rhs) {
            (Value::U32(l) | Value::V32(l), Value::U32(r) | Value::V32(r)) => l == r,
            (Value::I32(l) | Value::Z32(l), Value::I32(r) | Value::Z32(r)) => l == r,
            (Value::U64(l) | Value::V64(l), Value::U64(r) | Value::V64(r)) => l == r,
            (Value::I64(l) | Value::Z64(l), Value::I64(r) | Value::Z64(r)) => l == r,
            (Value::F32(l), Value::F32(r)) => match (l.classify(), r.classify()) {
                (Nan, Nan) => true,
                (Zero, Zero) => true,
                (_, _) => l == r,
            },
            (Value::F64(l), Value::F64(r)) => match (l.classify(), r.classify()) {
                (Nan, Nan) => true,
                (Zero, Zero) => true,
                (_, _) => l == r,
            },
            (Value::Decimal(l), Value::Decimal(r)) => l == r,
            (Value::DateTime(l), Value::DateTime(r)) => l == r,
            (Value::Duration(l), Value::Duration(r)) => l == r,
            (Value::String(l), Value::String(r)) => l == r,
            (Value::Bytes(l), Value::Bytes(r)) => l == r,
            (Value::Bool(l), Value::Bool(r)) => l == r,
            (Value::Null, Value::Null) => true,
            (Value::Error(l), Value::Error(r)) => l == r,
            (Value::Array(l), Value::Array(r)) => l == r,
            (Value::Array(_), _) | (_, Value::Array(_)) => false,
            (l, r) if l.number() || r.number() => {
                match (l.clone().cast_to::<f64>(), r.clone().cast_to::<f64>()) {
                    (Ok(l), Ok(r)) => match (l.classify(), r.classify()) {
                        (Nan, Nan) => true,
                        (Zero, Zero) => true,
                        (_, _) => l == r,
                    },
                    (_, _) => false,
                }
            }
            (_, _) => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use std::num::FpCategory::*;
        match (self, other) {
            (Value::U32(l) | Value::V32(l), Value::U32(r) | Value::V32(r)) => {
                l.partial_cmp(r)
            }
            (Value::I32(l) | Value::Z32(l), Value::I32(r) | Value::Z32(r)) => {
                l.partial_cmp(r)
            }
            (Value::U64(l) | Value::V64(l), Value::U64(r) | Value::V64(r)) => {
                l.partial_cmp(r)
            }
            (Value::I64(l) | Value::Z64(l), Value::I64(r) | Value::Z64(r)) => {
                l.partial_cmp(r)
            }
            (Value::F32(l), Value::F32(r)) => match (l.classify(), r.classify()) {
                (Nan, Nan) => Some(Ordering::Equal),
                (Nan, _) => Some(Ordering::Less),
                (_, Nan) => Some(Ordering::Greater),
                (_, _) => l.partial_cmp(r),
            },
            (Value::F64(l), Value::F64(r)) => match (l.classify(), r.classify()) {
                (Nan, Nan) => Some(Ordering::Equal),
                (Nan, _) => Some(Ordering::Less),
                (_, Nan) => Some(Ordering::Greater),
                (_, _) => l.partial_cmp(r),
            },
            (Value::Decimal(l), Value::Decimal(r)) => l.partial_cmp(r),
            (Value::DateTime(l), Value::DateTime(r)) => l.partial_cmp(r),
            (Value::Duration(l), Value::Duration(r)) => l.partial_cmp(r),
            (Value::String(l), Value::String(r)) => l.partial_cmp(r),
            (Value::Bytes(l), Value::Bytes(r)) => l.partial_cmp(r),
            (Value::Bool(l), Value::Bool(r)) => l.partial_cmp(r),
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Error(l), Value::Error(r)) => l.partial_cmp(r),
            (Value::Error(_), _) => Some(Ordering::Less),
            (_, Value::Error(_)) => Some(Ordering::Greater),
            (Value::Array(l), Value::Array(r)) => l.partial_cmp(r),
            (Value::Array(_), _) => Some(Ordering::Less),
            (_, Value::Array(_)) => Some(Ordering::Greater),
            (l, r) if l.number() || r.number() => {
                match (l.clone().cast_to::<f64>(), r.clone().cast_to::<f64>()) {
                    (Ok(l), Ok(r)) => match (l.classify(), r.classify()) {
                        (Nan, Nan) => Some(Ordering::Equal),
                        (Nan, _) => Some(Ordering::Less),
                        (_, Nan) => Some(Ordering::Greater),
                        (_, _) => l.partial_cmp(&r),
                    },
                    (_, _) => {
                        format_compact!("{}", l).partial_cmp(&format_compact!("{}", r))
                    }
                }
            }
            (l, r) => format_compact!("{}", l).partial_cmp(&format_compact!("{}", r)),
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_ext(f, &value_parser::VAL_ESC, true)
    }
}

impl FromStr for Value {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        value_parser::parse_value(s)
    }
}

macro_rules! apply_op {
    ($self:expr, $rhs:expr, $id:expr, $op:tt, $($pat:pat => $blk:block),+) => {
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
            (Value::Decimal(l), Value::Decimal(r)) => Value::Decimal(l $op r),
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
            (Value::Decimal(l), Value::U32(r) | Value::V32(r)) =>
                Value::Decimal(l $op Decimal::from(r)),
            (Value::U32(l) | Value::V32(l), Value::Decimal(r)) =>
                Value::Decimal(Decimal::from(l) $op r),
            (Value::Decimal(l), Value::U64(r) | Value::V64(r)) =>
                Value::Decimal(l $op Decimal::from(r)),
            (Value::U64(l) | Value::V64(l), Value::Decimal(r)) =>
                Value::Decimal(Decimal::from(l) $op r),
            (Value::Decimal(l), Value::I32(r) | Value::Z32(r)) =>
                Value::Decimal(l $op Decimal::from(r)),
            (Value::I32(l) | Value::Z32(l), Value::Decimal(r)) =>
                Value::Decimal(Decimal::from(l) $op r),
            (Value::Decimal(l), Value::I64(r) | Value::Z64(r)) =>
                Value::Decimal(l $op Decimal::from(r)),
            (Value::I64(l) | Value::Z64(l), Value::Decimal(r)) =>
                Value::Decimal(Decimal::from(l) $op r),
            (Value::Decimal(l), Value::F32(r)) => match Decimal::try_from(r) {
                Ok(r) => Value::Decimal(l $op r),
                Err(_) => {
                    let e = format_compact!("can't parse {} as a decimal", r);
                    Value::Error(e.as_str().into())
                },
            },
            (Value::F32(l), Value::Decimal(r)) => match Decimal::try_from(l) {
                Ok(l) => Value::Decimal(l $op r),
                Err(_) => {
                    let e = format_compact!("can't parse {} as a decimal", l);
                    Value::Error(e.as_str().into())
                },
            },
            (Value::Decimal(l), Value::F64(r)) => match Decimal::try_from(r) {
                Ok(r) => Value::Decimal(l $op r),
                Err(_) => {
                    let e = format_compact!("can't parse {} as a decimal", r);
                    Value::Error(e.as_str().into())
                },
            },
            (Value::F64(l), Value::Decimal(r)) => match Decimal::try_from(l) {
                Ok(l) => Value::Decimal(l $op r),
                Err(_) => {
                    let e = format_compact!("can't parse {} as a decimal", l);
                    Value::Error(e.as_str().into())
                },
            },
            (Value::String(s), n) => match s.parse::<Value>() {
                Err(e) => Value::Error(format_compact!("{}", e).as_str().into()),
                Ok(s) => s $op n,
            }
            (n, Value::String(s)) => match s.parse::<Value>() {
                Err(e) => Value::Error(format_compact!("{}", e).as_str().into()),
                Ok(s) => n $op s,
            },
            (Value::Array(e0), Value::Array(e1)) => {
                let (e0, e1) = if e0.len() < e1.len() { (e0, e1) } else { (e1, e0) };
                let iter = e0
                    .iter()
                    .cloned()
                    .chain(iter::repeat(Value::F64($id)))
                    .zip(e1.iter().cloned());
                Value::Array(iter.map(|(v0, v1)| v0 $op v1).collect())
            }
            (l @ Value::Array(_), n) => {
                match n.cast(Typ::Array) {
                    None => Value::Error(literal!("can't add to array")),
                    Some(r) => l $op r,
                }
            }
            (n, r @ Value::Array(_)) => {
                match n.cast(Typ::Array) {
                    None => Value::Error(literal!("can't add to array")),
                    Some(l) => l $op r,
                }
            }
            (Value::Bytes(_), _) | (_, Value::Bytes(_)) => {
                Value::Error(literal!("can't add bytes"))
            }
            (Value::Null, _) | (_, Value::Null) => {
                Value::Error(literal!("can't add null"))
            }
            | (Value::Error(_), _)
            | (_, Value::Error(_)) => Value::Error(literal!("can't add error types")),
            (Value::Bool(true), n) => Value::U32(1) $op n,
            (n, Value::Bool(true)) => n $op Value::U32(1),
            (Value::Bool(false), n) => Value::U32(0) $op n,
            (n, Value::Bool(false)) => n $op Value::U32(0),
            $($pat => $blk),+
        }
    }
}

impl Add for Value {
    type Output = Value;

    fn add(self, rhs: Self) -> Self {
        apply_op!(
            self, rhs, 0., +,
            (Value::DateTime(dt), Value::Duration(d))
                | (Value::Duration(d), Value::DateTime(dt)) => {
                    match chrono::Duration::from_std(d) {
                        Ok(d) => Value::DateTime(dt + d),
                        Err(e) => Value::Error(format_compact!("{}", e).as_str().into()),
                    }
                },
            (Value::Duration(d0), Value::Duration(d1)) => { Value::Duration(d0 + d1) },
            (Value::Duration(_), _)
                | (_, Value::Duration(_))
                | (_, Value::DateTime(_))
                | (Value::DateTime(_), _) => {
                    Value::Error(literal!("can't add to datetime/duration"))
                }
        )
    }
}

impl Sub for Value {
    type Output = Value;

    fn sub(self, rhs: Self) -> Self {
        apply_op!(
            self, rhs, 0., -,
            (Value::DateTime(dt), Value::Duration(d))
                | (Value::Duration(d), Value::DateTime(dt)) => {
                    match chrono::Duration::from_std(d) {
                        Ok(d) => Value::DateTime(dt - d),
                        Err(e) => Value::Error(format_compact!("{}", e).as_str().into()),
                    }
                },
            (Value::Duration(d0), Value::Duration(d1)) => { Value::Duration(d0 - d1) },
            (Value::Duration(_), _)
                | (_, Value::Duration(_))
                | (_, Value::DateTime(_))
                | (Value::DateTime(_), _) => {
                    Value::Error(literal!("can't add to datetime/duration"))
                }
        )
    }
}

impl Mul for Value {
    type Output = Value;

    fn mul(self, rhs: Self) -> Self {
        apply_op!(
            self, rhs, 1., *,
            (Value::Duration(_), _)
                | (_, Value::Duration(_))
                | (_, Value::DateTime(_))
                | (Value::DateTime(_), _) => {
                    Value::Error(literal!("can't add to datetime/duration"))
                }
        )
    }
}

impl Div for Value {
    type Output = Value;

    fn div(self, rhs: Self) -> Self {
        let res = catch_unwind(AssertUnwindSafe(|| {
            apply_op!(
                self, rhs, 1., /,
                (Value::Duration(d), Value::U32(s)) => { Value::Duration(d / s) },
                (Value::Duration(d), Value::V32(s)) => { Value::Duration(d / s) },
                (Value::Duration(d), Value::F32(s)) => { Value::Duration(d.div_f32(s)) },
                (Value::Duration(d), Value::F64(s)) => { Value::Duration(d.div_f64(s)) },
                (Value::Duration(_), _)
                    | (_, Value::Duration(_))
                    | (_, Value::DateTime(_))
                    | (Value::DateTime(_), _) => {
                        Value::Error(literal!("can't add to datetime/duration"))
                    }
            )
        }));
        match res {
            Ok(r) => r,
            Err(_) => Value::Error(literal!("can't divide by zero")),
        }
    }
}

impl Not for Value {
    type Output = Value;

    fn not(self) -> Self {
        match self {
            Value::Bool(v) => Value::Bool(!v),
            Value::Null => Value::Null,
            Value::U32(v) => Value::U32(!v),
            Value::V32(v) => Value::V32(!v),
            Value::I32(v) => Value::I32(!v),
            Value::Z32(v) => Value::Z32(!v),
            Value::U64(v) => Value::U64(!v),
            Value::V64(v) => Value::V64(!v),
            Value::I64(v) => Value::I64(!v),
            Value::Z64(v) => Value::Z64(!v),
            Value::F32(_) => Value::Error(literal!("can't apply not to F32")),
            Value::F64(_) => Value::Error(literal!("can't apply not to F64")),
            Value::Decimal(_) => Value::Error(literal!("can't apply not to Decimal")),
            Value::DateTime(_) => Value::Error(literal!("can't apply not to DateTime")),
            Value::Duration(_) => Value::Error(literal!("can't apply not to Duration")),
            Value::String(_) => Value::Error(literal!("can't apply not to String")),
            Value::Bytes(_) => Value::Error(literal!("can't apply not to Bytes")),
            Value::Error(_) => Value::Error(literal!("can't apply not to Error")),
            Value::Array(elts) => {
                Value::Array(ValArray::from_iter_exact(elts.iter().cloned().map(|v| !v)))
            }
        }
    }
}

impl Pack for Value {
    fn encoded_len(&self) -> usize {
        1 + match self {
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
            Value::Error(c) => Pack::encoded_len(c),
            Value::Array(elts) => Pack::encoded_len(elts),
            Value::Decimal(d) => Pack::encoded_len(d),
        }
    }

    // the high two bits of the tag are reserved for wrapper types,
    // max tag is therefore 0x3F
    fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
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
            Value::Error(e) => {
                buf.put_u8(18);
                Pack::encode(e, buf)
            }
            Value::Array(elts) => {
                buf.put_u8(19);
                Pack::encode(elts, buf)
            }
            Value::Decimal(d) => {
                buf.put_u8(20);
                Pack::encode(d, buf)
            }
        }
    }

    fn decode(buf: &mut impl Buf) -> Result<Self> {
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
            17 => Ok(Value::Null), // 17 used to be Ok
            18 => Ok(Value::Error(Pack::decode(buf)?)),
            19 => Ok(Value::Array(Pack::decode(buf)?)),
            20 => Ok(Value::Decimal(Pack::decode(buf)?)),
            _ => Err(PackError::UnknownTag),
        }
    }
}

pub trait FromValue {
    /// attempt to cast v to the type of self using any reasonable means
    fn from_value(v: Value) -> Res<Self>
    where
        Self: Sized;

    /// extract the type of self from v if the type of v is equivelent
    /// to the type of self, otherwise return None.
    fn get(v: Value) -> Option<Self>
    where
        Self: Sized,
    {
        FromValue::from_value(v).ok()
    }
}

impl Value {
    pub fn discriminant(&self) -> u32 {
        unsafe { *<*const _>::from(self).cast::<u32>() }
    }

    pub fn is_copy(&self) -> bool {
        self.discriminant() <= COPY_MAX
    }

    pub fn to_string_naked(&self) -> String {
        struct WVal<'a>(&'a Value);
        impl<'a> fmt::Display for WVal<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                self.0.fmt_naked(f)
            }
        }
        format!("{}", WVal(self))
    }

    pub fn fmt_naked(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::U32(v) | Value::V32(v) => write!(f, "{}", v),
            Value::I32(v) | Value::Z32(v) => write!(f, "{}", v),
            Value::U64(v) | Value::V64(v) => write!(f, "{}", v),
            Value::I64(v) | Value::Z64(v) => write!(f, "{}", v),
            Value::F32(v) => write!(f, "{}", v),
            Value::F64(v) => write!(f, "{}", v),
            Value::Decimal(v) => write!(f, "{}", v),
            Value::DateTime(v) => write!(f, "{}", v),
            Value::Duration(v) => {
                let v = v.as_secs_f64();
                if v.fract() == 0. {
                    write!(f, "{}.s", v)
                } else {
                    write!(f, "{}s", v)
                }
            }
            Value::String(s) => write!(f, "{}", s),
            Value::Bytes(b) => write!(f, "{}", BASE64.encode(b)),
            Value::Bool(true) => write!(f, "true"),
            Value::Bool(false) => write!(f, "false"),
            Value::Null => write!(f, "null"),
            v @ Value::Error(_) => write!(f, "{}", v),
            v @ Value::Array(_) => write!(f, "{}", v),
        }
    }

    pub fn fmt_notyp(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_ext(f, &value_parser::VAL_ESC, false)
    }

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
            Value::Decimal(v) => {
                if types {
                    write!(f, "decimal:{}", v)
                } else {
                    write!(f, "{}", v)
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
                write!(f, "{}{}", pfx, BASE64.encode(&*b))
            }
            Value::Bool(true) => write!(f, "true"),
            Value::Bool(false) => write!(f, "false"),
            Value::Null => write!(f, "null"),
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
                    Typ::Decimal => match Decimal::try_from($v) {
                        Ok(d) => Some(Value::Decimal(d)),
                        Err(_) => None,
                    },
                    Typ::DateTime => {
                        Some(Value::DateTime(DateTime::from_timestamp($v as i64, 0)?))
                    }
                    Typ::Duration => {
                        Some(Value::Duration(Duration::from_secs($v as u64)))
                    }
                    Typ::Bool => Some(if $v as i64 > 0 {
                        Value::Bool(true)
                    } else {
                        Value::Bool(false)
                    }),
                    Typ::String => {
                        Some(Value::String(format_compact!("{}", self).as_str().into()))
                    }
                    Typ::Bytes => None,
                    Typ::Error => None,
                    Typ::Array => Some(Value::Array([self.clone()].into())),
                    Typ::Null => Some(Value::Null),
                }
            };
        }
        match self {
            Value::String(s) => match typ {
                Typ::String => Some(Value::String(s)),
                Typ::Error => Some(Value::Error(s)),
                _ => s.parse::<Value>().ok().and_then(|v| v.cast(typ)),
            },
            v if typ == Typ::String => {
                Some(Value::String(format_compact!("{}", v).as_str().into()))
            }
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
            Value::Decimal(v) => match typ {
                Typ::Decimal => Some(Value::Decimal(v)),
                Typ::U32 => v.try_into().ok().map(Value::U32),
                Typ::V32 => v.try_into().ok().map(Value::V32),
                Typ::I32 => v.try_into().ok().map(Value::I32),
                Typ::Z32 => v.try_into().ok().map(Value::Z32),
                Typ::U64 => v.try_into().ok().map(Value::U64),
                Typ::V64 => v.try_into().ok().map(Value::V64),
                Typ::I64 => v.try_into().ok().map(Value::I64),
                Typ::Z64 => v.try_into().ok().map(Value::Z64),
                Typ::F32 => v.try_into().ok().map(Value::F32),
                Typ::F64 => v.try_into().ok().map(Value::F64),
                Typ::String => {
                    Some(Value::String(format_compact!("{}", v).as_str().into()))
                }
                Typ::Bool
                | Typ::Array
                | Typ::Bytes
                | Typ::DateTime
                | Typ::Duration
                | Typ::Null
                | Typ::Error => None,
            },
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
                    let dur = dur
                        + (v.timestamp_nanos_opt()
                            .expect("cannot represent as timestamp with ns precision")
                            / 1_000_000_000) as f64;
                    if typ == Typ::F32 {
                        Some(Value::F32(dur as f32))
                    } else {
                        Some(Value::F64(dur))
                    }
                }
                Typ::DateTime => Some(Value::DateTime(v)),
                Typ::Decimal => None,
                Typ::Duration => None,
                Typ::Bool => None,
                Typ::Bytes => None,
                Typ::Error => None,
                Typ::Array => Some(Value::Array([self].into())),
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
                Typ::Decimal => None,
                Typ::DateTime => None,
                Typ::Duration => Some(Value::Duration(d)),
                Typ::Bool => None,
                Typ::Bytes => None,
                Typ::Error => None,
                Typ::Array => Some(Value::Array([self].into())),
                Typ::Null => Some(Value::Null),
                Typ::String => unreachable!(),
            },
            Value::Bool(b) => match typ {
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
                Typ::Decimal => None,
                Typ::DateTime => None,
                Typ::Duration => None,
                Typ::Bool => Some(self),
                Typ::Bytes => None,
                Typ::Error => None,
                Typ::Array => Some(Value::Array([self].into())),
                Typ::Null => Some(Value::Null),
                Typ::String => unreachable!(),
            },
            Value::Bytes(_) if typ == Typ::Bytes => Some(self),
            Value::Bytes(_) => None,
            Value::Error(_) => Value::Bool(false).cast(typ),
            Value::Null if typ == Typ::Null => Some(self),
            Value::Null => None,
        }
    }

    /// cast value directly to any type implementing `FromValue`
    pub fn cast_to<T: FromValue + Sized>(self) -> Res<T> {
        <T as FromValue>::from_value(self)
    }

    pub fn get_as<T: FromValue + Sized>(self) -> Option<T> {
        <T as FromValue>::get(self)
    }

    pub fn err<T: std::error::Error>(e: T) -> Value {
        use std::fmt::Write;
        let mut tmp = CompactString::new("");
        write!(tmp, "{e}").unwrap();
        Value::Error(tmp.as_str().into())
    }

    /// return true if the value is some kind of number, otherwise
    /// false.
    pub fn number(&self) -> bool {
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
            | Value::F64(_)
            | Value::Decimal(_) => true,
            Value::DateTime(_)
            | Value::Duration(_)
            | Value::String(_)
            | Value::Bytes(_)
            | Value::Bool(_)
            | Value::Null
            | Value::Error(_)
            | Value::Array(_) => false,
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

impl FromValue for Value {
    fn from_value(v: Value) -> Res<Self> {
        Ok(v)
    }

    fn get(v: Value) -> Option<Self> {
        Some(v)
    }
}

impl<T: Into<Value> + Copy> convert::From<&T> for Value {
    fn from(v: &T) -> Value {
        (*v).into()
    }
}

impl FromValue for u8 {
    fn from_value(v: Value) -> Res<Self> {
        let v = v.cast_to::<u32>()?;
        if v <= u8::MAX as u32 {
            Ok(v as u8)
        } else {
            bail!("can't cast")
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
    fn from_value(v: Value) -> Res<Self> {
        let v = v.cast_to::<i32>()?;
        if v <= i8::MAX as i32 && v >= i8::MIN as i32 {
            Ok(v as i8)
        } else {
            bail!("can't cast")
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
    fn from_value(v: Value) -> Res<Self> {
        let v = v.cast_to::<u32>()?;
        if v <= u16::MAX as u32 {
            Ok(v as u16)
        } else {
            bail!("can't cast")
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
    fn from_value(v: Value) -> Res<Self> {
        let v = v.cast_to::<i32>()?;
        if v <= i16::MAX as i32 && v >= i16::MIN as i32 {
            Ok(v as i16)
        } else {
            bail!("can't cast")
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
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Ok(v),
            Value::U64(v) | Value::V64(v) => Ok(v as u32),
            Value::I32(v) | Value::Z32(v) => Ok(v as u32),
            Value::I64(v) | Value::Z64(v) => Ok(v as u32),
            v => {
                v.cast(Typ::U32).ok_or_else(|| anyhow!("can't cast")).and_then(
                    |v| match v {
                        Value::U32(v) => Ok(v),
                        _ => bail!("can't cast"),
                    },
                )
            }
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U32(v) | Value::V32(v) => Some(v),
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
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::I32(v) | Value::Z32(v) => Ok(v),
            Value::U32(v) | Value::V32(v) => Ok(v as i32),
            Value::U64(v) | Value::V64(v) => Ok(v as i32),
            Value::I64(v) | Value::Z64(v) => Ok(v as i32),
            v => {
                v.cast(Typ::I32).ok_or_else(|| anyhow!("can't cast")).and_then(
                    |v| match v {
                        Value::I32(v) => Ok(v),
                        _ => bail!("can't cast"),
                    },
                )
            }
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::I32(v) | Value::Z32(v) => Some(v),
            Value::U32(v) | Value::V32(v) => Some(v as i32),
            Value::U64(v) | Value::V64(v) => Some(v as i32),
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
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::U64(v) | Value::V64(v) => Ok(v),
            Value::U32(v) | Value::V32(v) => Ok(v as u64),
            Value::I32(v) | Value::Z32(v) => Ok(v as u64),
            Value::I64(v) | Value::Z64(v) => Ok(v as u64),
            v => {
                v.cast(Typ::U64).ok_or_else(|| anyhow!("can't cast")).and_then(
                    |v| match v {
                        Value::U64(v) => Ok(v),
                        _ => bail!("can't cast"),
                    },
                )
            }
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U64(v) | Value::V64(v) => Some(v),
            Value::U32(v) | Value::V32(v) => Some(v as u64),
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
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::U64(v) | Value::V64(v) => Ok(v as usize),
            Value::U32(v) | Value::V32(v) => Ok(v as usize),
            Value::I32(v) | Value::Z32(v) => Ok(v as usize),
            Value::I64(v) | Value::Z64(v) => Ok(v as usize),
            v => {
                v.cast(Typ::U64).ok_or_else(|| anyhow!("can't cast")).and_then(
                    |v| match v {
                        Value::U64(v) => Ok(v as usize),
                        _ => bail!("can't cast"),
                    },
                )
            }
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::U64(v) | Value::V64(v) => Some(v as usize),
            Value::U32(v) | Value::V32(v) => Some(v as usize),
            Value::I32(v) | Value::Z32(v) => Some(v as usize),
            Value::I64(v) | Value::Z64(v) => Some(v as usize),
            _ => None,
        }
    }
}

impl FromValue for i64 {
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::I64(v) | Value::Z64(v) => Ok(v),
            Value::U32(v) | Value::V32(v) => Ok(v as i64),
            Value::U64(v) | Value::V64(v) => Ok(v as i64),
            Value::I32(v) | Value::Z32(v) => Ok(v as i64),
            v => {
                v.cast(Typ::I64).ok_or_else(|| anyhow!("can't cast")).and_then(
                    |v| match v {
                        Value::I64(v) => Ok(v),
                        _ => bail!("can't cast"),
                    },
                )
            }
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::I64(v) | Value::Z64(v) => Some(v),
            Value::U32(v) | Value::V32(v) => Some(v as i64),
            Value::U64(v) | Value::V64(v) => Some(v as i64),
            Value::I32(v) | Value::Z32(v) => Some(v as i64),
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
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::F32(v) => Ok(v),
            Value::F64(v) => Ok(v as f32),
            v => {
                v.cast(Typ::F32).ok_or_else(|| anyhow!("can't cast")).and_then(
                    |v| match v {
                        Value::F32(v) => Ok(v),
                        _ => bail!("can't cast"),
                    },
                )
            }
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::F32(v) => Some(v),
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
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::F64(v) => Ok(v),
            Value::F32(v) => Ok(v as f64),
            v => {
                v.cast(Typ::F64).ok_or_else(|| anyhow!("can't cast")).and_then(
                    |v| match v {
                        Value::F64(v) => Ok(v),
                        _ => bail!("can't cast"),
                    },
                )
            }
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::F64(v) => Some(v),
            Value::F32(v) => Some(v as f64),
            _ => None,
        }
    }
}

impl convert::From<f64> for Value {
    fn from(v: f64) -> Value {
        Value::F64(v)
    }
}

impl FromValue for Decimal {
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::Decimal(v) => Ok(v),
            v => {
                v.cast(Typ::Decimal).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                    match v {
                        Value::Decimal(v) => Ok(v),
                        _ => bail!("can't cast"),
                    }
                })
            }
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Decimal(v) => Some(v),
            _ => None,
        }
    }
}

impl convert::From<Decimal> for Value {
    fn from(value: Decimal) -> Self {
        Value::Decimal(value)
    }
}

impl FromValue for Bytes {
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::Bytes(b) => Ok(b.into()),
            v => v.cast(Typ::Bytes).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Bytes(b) => Ok(b.into()),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Bytes(b) => Some(b.into()),
            _ => None,
        }
    }
}

impl convert::From<Bytes> for Value {
    fn from(v: Bytes) -> Value {
        Value::Bytes(v.into())
    }
}

impl FromValue for Path {
    fn from_value(v: Value) -> Res<Self> {
        v.cast_to::<ArcStr>().map(Path::from)
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::String(c) => Some(Path::from(c)),
            _ => None,
        }
    }
}

impl convert::From<Path> for Value {
    fn from(v: Path) -> Value {
        Value::String(v.into())
    }
}

impl FromValue for String {
    fn from_value(v: Value) -> Res<Self> {
        v.cast_to::<ArcStr>().map(|c| String::from(c.as_str()))
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::String(c) => Some(String::from(c.as_str())),
            _ => None,
        }
    }
}

impl convert::From<String> for Value {
    fn from(v: String) -> Value {
        Value::String(v.into())
    }
}

impl convert::From<&'static str> for Value {
    fn from(v: &'static str) -> Value {
        Value::String(v.into())
    }
}

impl FromValue for ArcStr {
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::String(s) => Ok(s),
            v => v.cast(Typ::String).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::String(s) => Ok(s),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::String(c) => Some(c),
            _ => None,
        }
    }
}

impl convert::From<ArcStr> for Value {
    fn from(v: ArcStr) -> Value {
        Value::String(v)
    }
}

impl FromValue for CompactString {
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::String(s) => Ok(CompactString::from(s.as_str())),
            v => v.cast(Typ::String).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::String(s) => Ok(CompactString::from(s.as_str())),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::String(c) => Some(CompactString::from(c.as_str())),
            _ => None,
        }
    }
}

impl convert::From<CompactString> for Value {
    fn from(v: CompactString) -> Value {
        Value::String(ArcStr::from(v.as_str()))
    }
}

impl FromValue for DateTime<Utc> {
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::DateTime(d) => Ok(d),
            v => {
                v.cast(Typ::DateTime).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                    match v {
                        Value::DateTime(d) => Ok(d),
                        _ => bail!("can't cast"),
                    }
                })
            }
        }
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
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::Duration(d) => Ok(d),
            v => {
                v.cast(Typ::Duration).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                    match v {
                        Value::Duration(d) => Ok(d),
                        _ => bail!("can't cast"),
                    }
                })
            }
        }
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
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::Bool(b) => Ok(b),
            v => v.cast(Typ::Bool).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Bool(b) => Ok(b),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Bool(b) => Some(b),
            _ => None,
        }
    }
}

impl convert::From<bool> for Value {
    fn from(v: bool) -> Value {
        Value::Bool(v)
    }
}

impl FromValue for ValArray {
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::Array(a) => Ok(a),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(elts) => Ok(elts),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) => Some(elts),
            _ => None,
        }
    }
}

impl convert::From<ValArray> for Value {
    fn from(v: ValArray) -> Value {
        Value::Array(v)
    }
}

impl<T: FromValue> FromValue for Vec<T> {
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($elts:expr) => {
                $elts
                    .iter()
                    .map(|v| <T as FromValue>::from_value(v.clone()))
                    .collect::<Res<Vec<_>>>()
            };
        }
        match v {
            Value::Array(a) => Ok(convert!(a)?),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) => Ok(convert!(a)?),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) => {
                elts.iter().map(|v| FromValue::get(v.clone())).collect::<Option<Vec<_>>>()
            }
            _ => None,
        }
    }
}

impl<T: convert::Into<Value>> convert::From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Value {
        Value::Array(ValArray::from_iter_exact(v.into_iter().map(|e| e.into())))
    }
}

impl<T: convert::Into<Value> + Clone + Send + Sync> convert::From<Pooled<Vec<T>>>
    for Value
{
    fn from(mut v: Pooled<Vec<T>>) -> Value {
        Value::Array(ValArray::from_iter_exact(v.drain(..).map(|e| e.into())))
    }
}

impl<const S: usize, T: FromValue> FromValue for [T; S] {
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($elts:expr) => {{
                let a = $elts
                    .iter()
                    .map(|v| <T as FromValue>::from_value(v.clone()))
                    .collect::<Res<SmallVec<[T; S]>>>()?;
                Ok(a.into_inner().map_err(|_| anyhow!("size mismatch"))?)
            }};
        }
        match v {
            Value::Array(a) if a.len() == S => convert!(a),
            Value::Array(_) => bail!("size mismatch"),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) if a.len() == S => convert!(a),
                    Value::Array(_) => bail!("size mismatch"),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(a) if a.len() == S => {
                let a = a
                    .iter()
                    .map(|v| <T as FromValue>::get(v.clone()))
                    .collect::<Option<SmallVec<[T; S]>>>()?;
                a.into_inner().ok()
            }
            _ => None,
        }
    }
}

impl<const S: usize, T: convert::Into<Value>> convert::From<[T; S]> for Value {
    fn from(v: [T; S]) -> Self {
        Value::Array(ValArray::from_iter_exact(v.into_iter().map(|e| e.into())))
    }
}

impl<const S: usize, T: FromValue> FromValue for SmallVec<[T; S]> {
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($elts:expr) => {
                $elts
                    .iter()
                    .map(|v| <T as FromValue>::from_value(v.clone()))
                    .collect::<Res<SmallVec<_>>>()
            };
        }
        match v {
            Value::Array(a) => Ok(convert!(a)?),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) => Ok(convert!(a)?),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) => elts
                .iter()
                .map(|v| FromValue::get(v.clone()))
                .collect::<Option<SmallVec<_>>>(),
            _ => None,
        }
    }
}

impl<const S: usize, T: convert::Into<Value>> convert::From<SmallVec<[T; S]>> for Value {
    fn from(v: SmallVec<[T; S]>) -> Self {
        Value::Array(ValArray::from_iter_exact(v.into_iter().map(|e| e.into())))
    }
}

impl<T: FromValue, U: FromValue> FromValue for (T, U) {
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($elts:expr) => {{
                let v0 = $elts[0].clone().cast_to::<T>()?;
                let v1 = $elts[1].clone().cast_to::<U>()?;
                Ok((v0, v1))
            }};
        }
        match v {
            Value::Array(a) if a.len() == 2 => convert!(a),
            Value::Array(_) => bail!("not a tuple"),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) if a.len() == 2 => convert!(a),
                    Value::Array(_) => bail!("not a tuple"),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) if elts.len() == 2 => {
                let v0 = elts[0].clone().get_as::<T>()?;
                let v1 = elts[1].clone().get_as::<U>()?;
                Some((v0, v1))
            }
            _ => None,
        }
    }
}

impl<T: convert::Into<Value>, U: convert::Into<Value>> convert::From<(T, U)> for Value {
    fn from((t, u): (T, U)) -> Value {
        let v0 = t.into();
        let v1 = u.into();
        Value::Array([v0, v1].into())
    }
}

impl<T: FromValue, U: FromValue, V: FromValue> FromValue for (T, U, V) {
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($elts:expr) => {{
                let v0 = $elts[0].clone().cast_to::<T>()?;
                let v1 = $elts[1].clone().cast_to::<U>()?;
                let v2 = $elts[2].clone().cast_to::<V>()?;
                Ok((v0, v1, v2))
            }};
        }
        match v {
            Value::Array(a) if a.len() == 3 => convert!(a),
            Value::Array(_) => bail!("not a triple"),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) if a.len() == 3 => convert!(a),
                    Value::Array(_) => bail!("not a triple"),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) if elts.len() == 3 => {
                let v0 = elts[0].clone().get_as::<T>()?;
                let v1 = elts[1].clone().get_as::<U>()?;
                let v2 = elts[2].clone().get_as::<V>()?;
                Some((v0, v1, v2))
            }
            _ => None,
        }
    }
}

impl<T: convert::Into<Value>, U: convert::Into<Value>, V: convert::Into<Value>>
    convert::From<(T, U, V)> for Value
{
    fn from((t, u, v): (T, U, V)) -> Value {
        let v0 = t.into();
        let v1 = u.into();
        let v2 = v.into();
        Value::Array([v0, v1, v2].into())
    }
}

impl<K: FromValue + Eq + Hash, V: FromValue, S: BuildHasher + Default> FromValue
    for HashMap<K, V, S>
{
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($a:expr) => {
                $a.iter().map(|v| v.clone().cast_to::<(K, V)>()).collect()
            };
        }
        match v {
            Value::Array(a) => convert!(a),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) => convert!(a),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) => {
                elts.iter().map(|v| v.clone().get_as::<(K, V)>()).collect()
            }
            _ => None,
        }
    }
}

impl<K: convert::Into<Value>, V: convert::Into<Value>, S: BuildHasher + Default>
    convert::From<HashMap<K, V, S>> for Value
{
    fn from(h: HashMap<K, V, S>) -> Value {
        Value::Array(ValArray::from_iter_exact(h.into_iter().map(|v| v.into())))
    }
}

impl<K: FromValue + Ord, V: FromValue> FromValue for BTreeMap<K, V> {
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($a:expr) => {
                $a.iter().map(|v| v.clone().cast_to::<(K, V)>()).collect()
            };
        }
        match v {
            Value::Array(a) => convert!(a),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) => convert!(a),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) => {
                elts.iter().map(|v| v.clone().get_as::<(K, V)>()).collect()
            }
            _ => None,
        }
    }
}

impl<K: convert::Into<Value>, V: convert::Into<Value>> convert::From<BTreeMap<K, V>>
    for Value
{
    fn from(v: BTreeMap<K, V>) -> Self {
        Value::Array(ValArray::from_iter_exact(v.into_iter().map(|v| v.into())))
    }
}

impl<K: FromValue + Eq + Hash, S: BuildHasher + Default> FromValue for HashSet<K, S> {
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($a:expr) => {
                $a.iter().map(|v| v.clone().cast_to::<K>()).collect()
            };
        }
        match v {
            Value::Array(a) => convert!(a),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) => convert!(a),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) => elts.iter().map(|v| v.clone().get_as::<K>()).collect(),
            _ => None,
        }
    }
}

impl<K: convert::Into<Value>, S: BuildHasher + Default> convert::From<HashSet<K, S>>
    for Value
{
    fn from(h: HashSet<K, S>) -> Value {
        Value::Array(ValArray::from_iter_exact(h.into_iter().map(|v| v.into())))
    }
}

impl<K: FromValue + Ord> FromValue for BTreeSet<K> {
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($a:expr) => {
                $a.iter().map(|v| v.clone().cast_to::<K>()).collect()
            };
        }
        match v {
            Value::Array(a) => convert!(a),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) => convert!(a),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) => elts.iter().map(|v| v.clone().get_as::<K>()).collect(),
            _ => None,
        }
    }
}

impl<K: convert::Into<Value>> convert::From<BTreeSet<K>> for Value {
    fn from(s: BTreeSet<K>) -> Self {
        Value::Array(ValArray::from_iter_exact(s.into_iter().map(|v| v.into())))
    }
}

impl<K: FromValue + Eq + Hash, V: FromValue, S: BuildHasher + Default> FromValue
    for IndexMap<K, V, S>
{
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($a:expr) => {
                $a.iter().map(|v| v.clone().cast_to::<(K, V)>()).collect()
            };
        }
        match v {
            Value::Array(a) => convert!(a),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) => convert!(a),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) => {
                elts.iter().map(|v| v.clone().get_as::<(K, V)>()).collect()
            }
            _ => None,
        }
    }
}

impl<K: convert::Into<Value>, V: convert::Into<Value>, S: BuildHasher + Default>
    convert::From<IndexMap<K, V, S>> for Value
{
    fn from(h: IndexMap<K, V, S>) -> Value {
        Value::Array(ValArray::from_iter_exact(h.into_iter().map(|v| v.into())))
    }
}

impl<K: FromValue + Eq + Hash, S: BuildHasher + Default> FromValue for IndexSet<K, S> {
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($a:expr) => {
                $a.iter()
                    .map(|v| v.clone().cast_to::<K>())
                    .collect::<Res<IndexSet<K, S>>>()
            };
        }
        match v {
            Value::Array(a) => convert!(a),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) => convert!(a),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Array(elts) => elts.iter().map(|v| v.clone().get_as::<K>()).collect(),
            _ => None,
        }
    }
}

impl<K: convert::Into<Value>, S: BuildHasher + Default> convert::From<IndexSet<K, S>>
    for Value
{
    fn from(h: IndexSet<K, S>) -> Value {
        Value::Array(ValArray::from_iter_exact(h.into_iter().map(|v| v.into())))
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::Null => Ok(None),
            v => v.cast_to::<T>().map(|v| Some(v)),
        }
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::Null => Some(None),
            v => v.get_as::<T>().map(|v| Some(v)),
        }
    }
}

impl<T: convert::Into<Value>> convert::From<Option<T>> for Value {
    fn from(o: Option<T>) -> Value {
        o.map(|v| v.into()).unwrap_or(Value::Null)
    }
}

use enumflags2::{BitFlag, BitFlags, _internal::RawBitFlags};
impl<T> FromValue for BitFlags<T>
where
    T: BitFlag,
    <T as RawBitFlags>::Numeric: FromValue,
{
    fn from_value(v: Value) -> Res<Self> {
        let bits = v.cast_to::<<T as RawBitFlags>::Numeric>()?;
        BitFlags::from_bits(bits).map_err(|_| anyhow!("invalid bits"))
    }

    fn get(v: Value) -> Option<Self> {
        let bits = v.get_as::<<T as RawBitFlags>::Numeric>()?;
        BitFlags::from_bits(bits).ok()
    }
}

impl<T> convert::From<BitFlags<T>> for Value
where
    T: BitFlag,
    <T as RawBitFlags>::Numeric: Into<Value>,
{
    fn from(v: BitFlags<T>) -> Self {
        v.bits().into()
    }
}

impl FromValue for uuid::Uuid {
    fn from_value(v: Value) -> Res<Self> {
        match v {
            Value::String(v) => Ok(v.parse::<uuid::Uuid>()?),
            _ => bail!("can't cast"),
        }
    }

    fn get(v: Value) -> Option<Self> {
        <uuid::Uuid as FromValue>::from_value(v).ok()
    }
}

impl convert::From<uuid::Uuid> for Value {
    fn from(id: uuid::Uuid) -> Self {
        Value::from(id.to_string())
    }
}

thread_local! {
    static POOLS: RefCell<FxHashMap<TypeId, Box<dyn Any>>> =
        RefCell::new(HashMap::default());
}

impl<T: FromValue + Send + Sync> FromValue for Pooled<Vec<T>> {
    fn from_value(v: Value) -> Res<Self> {
        macro_rules! convert {
            ($a:expr) => {{
                let mut t = POOLS.with(|pools| {
                    let mut pools = pools.borrow_mut();
                    let pool: &mut Pool<Vec<T>> = pools
                        .entry(TypeId::of::<Vec<T>>())
                        .or_insert_with(|| Box::new(Pool::<Vec<T>>::new(10000, 10000)))
                        .downcast_mut()
                        .unwrap();
                    pool.take()
                });
                for elt in $a.iter() {
                    t.push(elt.clone().cast_to::<T>()?)
                }
                Ok(t)
            }};
        }
        match v {
            Value::Array(a) => convert!(a),
            v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                match v {
                    Value::Array(a) => convert!(a),
                    _ => bail!("can't cast"),
                }
            }),
        }
    }

    fn get(v: Value) -> Option<Self> {
        <Pooled<Vec<T>> as FromValue>::from_value(v).ok()
    }
}
