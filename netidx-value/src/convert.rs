use crate::{Typ, ValArray, Value};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use bytes::Bytes;
use chrono::prelude::*;
use compact_str::CompactString;
use fxhash::FxHashMap;
use indexmap::{IndexMap, IndexSet};
use netidx_core::{
    path::Path,
    pool::{Pool, Pooled},
};
use rust_decimal::Decimal;
use smallvec::SmallVec;
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    convert::{From, Into},
    hash::{BuildHasher, Hash},
    time::Duration,
};

pub trait FromValue {
    /// attempt to cast v to the type of self using any reasonable means
    fn from_value(v: Value) -> Result<Self>
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

impl FromValue for Value {
    fn from_value(v: Value) -> Result<Self> {
        Ok(v)
    }

    fn get(v: Value) -> Option<Self> {
        Some(v)
    }
}

impl<T: Into<Value> + Copy> From<&T> for Value {
    fn from(v: &T) -> Value {
        (*v).into()
    }
}

impl FromValue for u8 {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<u8> for Value {
    fn from(v: u8) -> Value {
        Value::U32(v as u32)
    }
}

impl FromValue for i8 {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<i8> for Value {
    fn from(v: i8) -> Value {
        Value::I32(v as i32)
    }
}

impl FromValue for u16 {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<u16> for Value {
    fn from(v: u16) -> Value {
        Value::U32(v as u32)
    }
}

impl FromValue for i16 {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<i16> for Value {
    fn from(v: i16) -> Value {
        Value::I32(v as i32)
    }
}

impl FromValue for u32 {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<u32> for Value {
    fn from(v: u32) -> Value {
        Value::U32(v)
    }
}

impl FromValue for i32 {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<i32> for Value {
    fn from(v: i32) -> Value {
        Value::I32(v)
    }
}

impl FromValue for u64 {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<u64> for Value {
    fn from(v: u64) -> Value {
        Value::U64(v)
    }
}

impl From<usize> for Value {
    fn from(v: usize) -> Value {
        Value::U64(v as u64)
    }
}

impl FromValue for usize {
    fn from_value(v: Value) -> Result<Self> {
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
    fn from_value(v: Value) -> Result<Self> {
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

impl From<i64> for Value {
    fn from(v: i64) -> Value {
        Value::I64(v)
    }
}

impl FromValue for f32 {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<f32> for Value {
    fn from(v: f32) -> Value {
        Value::F32(v)
    }
}

impl FromValue for f64 {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<f64> for Value {
    fn from(v: f64) -> Value {
        Value::F64(v)
    }
}

impl FromValue for Decimal {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<Decimal> for Value {
    fn from(value: Decimal) -> Self {
        Value::Decimal(value)
    }
}

impl FromValue for Bytes {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<Bytes> for Value {
    fn from(v: Bytes) -> Value {
        Value::Bytes(v.into())
    }
}

impl FromValue for Path {
    fn from_value(v: Value) -> Result<Self> {
        v.cast_to::<ArcStr>().map(Path::from)
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::String(c) => Some(Path::from(c)),
            _ => None,
        }
    }
}

impl From<Path> for Value {
    fn from(v: Path) -> Value {
        Value::String(v.into())
    }
}

impl FromValue for String {
    fn from_value(v: Value) -> Result<Self> {
        v.cast_to::<ArcStr>().map(|c| String::from(c.as_str()))
    }

    fn get(v: Value) -> Option<Self> {
        match v {
            Value::String(c) => Some(String::from(c.as_str())),
            _ => None,
        }
    }
}

impl From<String> for Value {
    fn from(v: String) -> Value {
        Value::String(v.into())
    }
}

impl From<&'static str> for Value {
    fn from(v: &'static str) -> Value {
        Value::String(v.into())
    }
}

impl FromValue for ArcStr {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<ArcStr> for Value {
    fn from(v: ArcStr) -> Value {
        Value::String(v)
    }
}

impl FromValue for CompactString {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<CompactString> for Value {
    fn from(v: CompactString) -> Value {
        Value::String(ArcStr::from(v.as_str()))
    }
}

impl FromValue for DateTime<Utc> {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<DateTime<Utc>> for Value {
    fn from(v: DateTime<Utc>) -> Value {
        Value::DateTime(v)
    }
}

impl FromValue for Duration {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<Duration> for Value {
    fn from(v: Duration) -> Value {
        Value::Duration(v)
    }
}

impl FromValue for bool {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<bool> for Value {
    fn from(v: bool) -> Value {
        Value::Bool(v)
    }
}

impl FromValue for ValArray {
    fn from_value(v: Value) -> Result<Self> {
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

impl From<ValArray> for Value {
    fn from(v: ValArray) -> Value {
        Value::Array(v)
    }
}

impl<T: FromValue> FromValue for Vec<T> {
    fn from_value(v: Value) -> Result<Self> {
        macro_rules! convert {
            ($elts:expr) => {
                $elts
                    .iter()
                    .map(|v| <T as FromValue>::from_value(v.clone()))
                    .collect::<Result<Vec<_>>>()
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

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Value {
        Value::Array(ValArray::from_iter_exact(v.into_iter().map(|e| e.into())))
    }
}

impl<T: Into<Value> + Clone + Send + Sync> From<Pooled<Vec<T>>> for Value {
    fn from(mut v: Pooled<Vec<T>>) -> Value {
        Value::Array(ValArray::from_iter_exact(v.drain(..).map(|e| e.into())))
    }
}

impl<const S: usize, T: FromValue> FromValue for [T; S] {
    fn from_value(v: Value) -> Result<Self> {
        macro_rules! convert {
            ($elts:expr) => {{
                let a = $elts
                    .iter()
                    .map(|v| <T as FromValue>::from_value(v.clone()))
                    .collect::<Result<SmallVec<[T; S]>>>()?;
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

impl<const S: usize, T: Into<Value>> From<[T; S]> for Value {
    fn from(v: [T; S]) -> Self {
        Value::Array(ValArray::from_iter_exact(v.into_iter().map(|e| e.into())))
    }
}

impl<const S: usize, T: FromValue> FromValue for SmallVec<[T; S]> {
    fn from_value(v: Value) -> Result<Self> {
        macro_rules! convert {
            ($elts:expr) => {
                $elts
                    .iter()
                    .map(|v| <T as FromValue>::from_value(v.clone()))
                    .collect::<Result<SmallVec<_>>>()
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

impl<const S: usize, T: Into<Value>> From<SmallVec<[T; S]>> for Value {
    fn from(v: SmallVec<[T; S]>) -> Self {
        Value::Array(ValArray::from_iter_exact(v.into_iter().map(|e| e.into())))
    }
}

macro_rules! tuple {
    ($len:literal, $(($i:literal, $t:ident, $v:ident)),+) =>{
        impl<$($t: FromValue),+> FromValue for ($($t),+) {
            fn from_value(v: Value) -> Result<Self> {
                match v {
                    Value::Array(a) if a.len() == $len => {
                        Ok(($(a[$i].clone().cast_to::<$t>()?),+))
                    },
                    Value::Array(_) => bail!("not a tuple of length {}", $len),
                    v => v.cast(Typ::Array).ok_or_else(|| anyhow!("can't cast")).and_then(|v| {
                        match v {
                            Value::Array(a) if a.len() == $len => {
                                Ok(($(a[$i].clone().cast_to::<$t>()?),+))
                            },
                            Value::Array(_) => bail!("not a tuple of length {}", $len),
                            _ => bail!("can't cast"),
                        }
                    }),
                }
            }
        }

        impl<$($t: Into<Value>),+> From<($($t),+)> for Value {
            fn from(($($v),+): ($($t),+)) -> Value {
                Value::Array([$($v.into()),+].into())
            }
        }
    }
}

tuple!(2, (0, T, t), (1, U, u));
tuple!(3, (0, T, t), (1, U, u), (2, V, v));
tuple!(4, (0, T, t), (1, U, u), (2, V, v), (3, W, w));
tuple!(5, (0, T, t), (1, U, u), (2, V, v), (3, W, w), (4, X, x));
tuple!(6, (0, T, t), (1, U, u), (2, V, v), (3, W, w), (4, X, x), (5, Y, y));
tuple!(7, (0, T, t), (1, U, u), (2, V, v), (3, W, w), (4, X, x), (5, Y, y), (6, Z, z));
tuple!(
    8,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a)
);
tuple!(
    9,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b)
);
tuple!(
    10,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c)
);
tuple!(
    11,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c),
    (10, D, d)
);
tuple!(
    12,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c),
    (10, D, d),
    (11, E, e)
);
tuple!(
    13,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c),
    (10, D, d),
    (11, E, e),
    (12, F, f)
);
tuple!(
    14,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c),
    (10, D, d),
    (11, E, e),
    (12, F, f),
    (13, G, g)
);
tuple!(
    15,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c),
    (10, D, d),
    (11, E, e),
    (12, F, f),
    (13, G, g),
    (14, H, h)
);
tuple!(
    16,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c),
    (10, D, d),
    (11, E, e),
    (12, F, f),
    (13, G, g),
    (14, H, h),
    (15, I, i)
);
tuple!(
    17,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c),
    (10, D, d),
    (11, E, e),
    (12, F, f),
    (13, G, g),
    (14, H, h),
    (15, I, i),
    (16, J, j)
);
tuple!(
    18,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c),
    (10, D, d),
    (11, E, e),
    (12, F, f),
    (13, G, g),
    (14, H, h),
    (15, I, i),
    (16, J, j),
    (17, K, k)
);
tuple!(
    19,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c),
    (10, D, d),
    (11, E, e),
    (12, F, f),
    (13, G, g),
    (14, H, h),
    (15, I, i),
    (16, J, j),
    (17, K, k),
    (18, L, l)
);
tuple!(
    20,
    (0, T, t),
    (1, U, u),
    (2, V, v),
    (3, W, w),
    (4, X, x),
    (5, Y, y),
    (6, Z, z),
    (7, A, a),
    (8, B, b),
    (9, C, c),
    (10, D, d),
    (11, E, e),
    (12, F, f),
    (13, G, g),
    (14, H, h),
    (15, I, i),
    (16, J, j),
    (17, K, k),
    (18, L, l),
    (19, M, m)
);

impl<K: FromValue + Eq + Hash, V: FromValue, S: BuildHasher + Default> FromValue
    for HashMap<K, V, S>
{
    fn from_value(v: Value) -> Result<Self> {
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

impl<K: Into<Value>, V: Into<Value>, S: BuildHasher + Default> From<HashMap<K, V, S>>
    for Value
{
    fn from(h: HashMap<K, V, S>) -> Value {
        Value::Array(ValArray::from_iter_exact(h.into_iter().map(|v| v.into())))
    }
}

impl<K: FromValue + Ord, V: FromValue> FromValue for BTreeMap<K, V> {
    fn from_value(v: Value) -> Result<Self> {
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

impl<K: Into<Value>, V: Into<Value>> From<BTreeMap<K, V>> for Value {
    fn from(v: BTreeMap<K, V>) -> Self {
        Value::Array(ValArray::from_iter_exact(v.into_iter().map(|v| v.into())))
    }
}

impl<K: FromValue + Eq + Hash, S: BuildHasher + Default> FromValue for HashSet<K, S> {
    fn from_value(v: Value) -> Result<Self> {
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

impl<K: Into<Value>, S: BuildHasher + Default> From<HashSet<K, S>> for Value {
    fn from(h: HashSet<K, S>) -> Value {
        Value::Array(ValArray::from_iter_exact(h.into_iter().map(|v| v.into())))
    }
}

impl<K: FromValue + Ord> FromValue for BTreeSet<K> {
    fn from_value(v: Value) -> Result<Self> {
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

impl<K: Into<Value>> From<BTreeSet<K>> for Value {
    fn from(s: BTreeSet<K>) -> Self {
        Value::Array(ValArray::from_iter_exact(s.into_iter().map(|v| v.into())))
    }
}

impl<K: FromValue + Eq + Hash, V: FromValue, S: BuildHasher + Default> FromValue
    for IndexMap<K, V, S>
{
    fn from_value(v: Value) -> Result<Self> {
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

impl<K: Into<Value>, V: Into<Value>, S: BuildHasher + Default> From<IndexMap<K, V, S>>
    for Value
{
    fn from(h: IndexMap<K, V, S>) -> Value {
        Value::Array(ValArray::from_iter_exact(h.into_iter().map(|v| v.into())))
    }
}

impl<K: FromValue + Eq + Hash, S: BuildHasher + Default> FromValue for IndexSet<K, S> {
    fn from_value(v: Value) -> Result<Self> {
        macro_rules! convert {
            ($a:expr) => {
                $a.iter()
                    .map(|v| v.clone().cast_to::<K>())
                    .collect::<Result<IndexSet<K, S>>>()
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

impl<K: Into<Value>, S: BuildHasher + Default> From<IndexSet<K, S>> for Value {
    fn from(h: IndexSet<K, S>) -> Value {
        Value::Array(ValArray::from_iter_exact(h.into_iter().map(|v| v.into())))
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(v: Value) -> Result<Self> {
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

impl<T: Into<Value>> From<Option<T>> for Value {
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
    fn from_value(v: Value) -> Result<Self> {
        let bits = v.cast_to::<<T as RawBitFlags>::Numeric>()?;
        BitFlags::from_bits(bits).map_err(|_| anyhow!("invalid bits"))
    }

    fn get(v: Value) -> Option<Self> {
        let bits = v.get_as::<<T as RawBitFlags>::Numeric>()?;
        BitFlags::from_bits(bits).ok()
    }
}

impl<T> From<BitFlags<T>> for Value
where
    T: BitFlag,
    <T as RawBitFlags>::Numeric: Into<Value>,
{
    fn from(v: BitFlags<T>) -> Self {
        v.bits().into()
    }
}

impl FromValue for uuid::Uuid {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::String(v) => Ok(v.parse::<uuid::Uuid>()?),
            _ => bail!("can't cast"),
        }
    }

    fn get(v: Value) -> Option<Self> {
        <uuid::Uuid as FromValue>::from_value(v).ok()
    }
}

impl From<uuid::Uuid> for Value {
    fn from(id: uuid::Uuid) -> Self {
        Value::from(id.to_string())
    }
}

thread_local! {
    static POOLS: RefCell<FxHashMap<TypeId, Box<dyn Any>>> =
        RefCell::new(HashMap::default());
}

impl<T: FromValue + Send + Sync> FromValue for Pooled<Vec<T>> {
    fn from_value(v: Value) -> Result<Self> {
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
