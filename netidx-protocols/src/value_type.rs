use anyhow::Result;
use netidx::{publisher::Value, chars::Chars};
use std::str::FromStr;
use bytes::Bytes;

#[derive(Debug, Clone, Copy)]
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
    Bool,
    String,
    Bytes,
    Result,
}

pub static TYPES: [Typ; 14] = [
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
    Typ::Bool,
    Typ::String,
    Typ::Bytes,
    Typ::Result,
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
            Value::String(_) => Some(Typ::String),
            Value::Bytes(_) => Some(Typ::Bytes),
            Value::True | Value::False => Some(Typ::Bool),
            Value::Null => None,
            Value::Ok | Value::Error(_) => Some(Typ::Result),
        }
    }

    pub fn parse(&self, s: &str) -> Result<Value> {
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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
            "bool" => Ok(Typ::Bool),
            "string" => Ok(Typ::String),
            "bytes" => Ok(Typ::Bytes),
            "result" => Ok(Typ::Result),
            s => Err(anyhow!(
                "invalid type, {}, valid types: u32, i32, u64, i64, f32, f64, bool, string, bytes, result", s))
        }
    }
}
