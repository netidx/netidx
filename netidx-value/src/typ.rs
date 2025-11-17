use crate::Value;
use anyhow::{anyhow, bail};
use arcstr::ArcStr;
use chrono::prelude::*;
use enumflags2::{bitflags, BitFlags};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{PartialEq, PartialOrd},
    fmt, mem, result,
    str::FromStr,
};
use triomphe::Arc;

/// The type of a Value.
///
/// Each Typ corresponds directly to the tag of a Value; the bits are
/// the same, and they can be used interchangeably.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u64)]
#[bitflags]
pub enum Typ {
    U8 = 0x0000_0001,
    I8 = 0x0000_0002,
    U16 = 0x0000_0004,
    I16 = 0x0000_0008,
    U32 = 0x0000_0010,
    V32 = 0x0000_0020,
    I32 = 0x0000_0040,
    Z32 = 0x0000_0080,
    U64 = 0x0000_0100,
    V64 = 0x0000_0200,
    I64 = 0x0000_0400,
    Z64 = 0x0000_0800,
    F32 = 0x0000_1000,
    F64 = 0x0000_2000,
    Bool = 0x0000_4000,
    Null = 0x0000_8000,
    String = 0x8000_0000,
    Bytes = 0x4000_0000,
    Error = 0x2000_0000,
    Array = 0x1000_0000,
    Map = 0x0800_0000,
    Decimal = 0x0400_0000,
    DateTime = 0x0200_0000,
    Duration = 0x0100_0000,
    Abstract = 0x0080_0000,
}

impl Typ {
    pub fn parse(&self, s: &str) -> anyhow::Result<Value> {
        match self {
            Typ::U8 => Ok(Value::U8(s.parse::<u8>()?)),
            Typ::I8 => Ok(Value::I8(s.parse::<i8>()?)),
            Typ::U16 => Ok(Value::U16(s.parse::<u16>()?)),
            Typ::I16 => Ok(Value::I16(s.parse::<i16>()?)),
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
            Typ::Decimal => Ok(Value::Decimal(Arc::new(s.parse::<Decimal>()?))),
            Typ::DateTime => Ok(Value::DateTime(Arc::new(DateTime::from_str(s)?))),
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
            Typ::Map => Ok(s.parse::<Value>()?),
            Typ::Null => {
                if s.trim() == "null" {
                    Ok(Value::Null)
                } else {
                    bail!("expected null")
                }
            }
            Typ::Abstract => {
                let mut tmp = String::from("abstract:");
                tmp.push_str(s);
                Ok(tmp.parse::<Value>()?)
            }
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Typ::U8 => "u8",
            Typ::I8 => "i8",
            Typ::U16 => "u16",
            Typ::I16 => "i16",
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
            Typ::Map => "map",
            Typ::Null => "null",
            Typ::Abstract => "abstract",
        }
    }

    pub fn get(v: &Value) -> Self {
        // safe because we are repr(u64) and because the tags are the
        // same between Typ and Value
        unsafe { mem::transmute::<u64, Typ>(v.discriminant()) }
    }

    pub fn any() -> BitFlags<Typ> {
        BitFlags::all()
    }

    pub fn number() -> BitFlags<Typ> {
        Typ::U8
            | Typ::I8
            | Typ::U16
            | Typ::I16
            | Typ::U32
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
        Typ::U8
            | Typ::I8
            | Typ::U16
            | Typ::I16
            | Typ::U32
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
        Typ::I8 | Typ::I16 | Typ::I32 | Typ::Z32 | Typ::I64 | Typ::Z64
    }

    pub fn is_signed_integer(&self) -> bool {
        Self::signed_integer().contains(*self)
    }

    pub fn unsigned_integer() -> BitFlags<Typ> {
        Typ::U8 | Typ::U16 | Typ::U32 | Typ::V32 | Typ::U64 | Typ::V64
    }

    pub fn is_unsigned_integer(&self) -> bool {
        Self::unsigned_integer().contains(*self)
    }

    pub fn float() -> BitFlags<Typ> {
        Typ::F32 | Typ::F64
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
            "U8" | "u8" => Ok(Typ::U8),
            "I8" | "i8" => Ok(Typ::I8),
            "U16" | "u16" => Ok(Typ::U16),
            "I16" | "i16" => Ok(Typ::I16),
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
            "Map" | "map" => Ok(Typ::Map),
            "Null" | "null" => Ok(Typ::Null),
            "Abstract" | "abstract" => Ok(Typ::Abstract),
            s => Err(anyhow!(
                "invalid type, {}, valid types: u32, i32, u64, i64, f32, f64, bool, string, bytes, error, array, map, null", s))
        }
    }
}

impl fmt::Display for Typ {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
