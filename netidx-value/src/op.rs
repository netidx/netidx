use crate::{Typ, ValArray, Value};
use arcstr::literal;
use compact_str::format_compact;
use rust_decimal::Decimal;
use std::{
    cmp::{Ordering, PartialEq, PartialOrd},
    hash::Hash,
    iter,
    num::Wrapping,
    ops::{Add, Div, Mul, Not, Rem, Sub},
    panic::{catch_unwind, AssertUnwindSafe},
};
use triomphe::Arc;

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
            Value::Error(c) => match &**c {
                Value::String(e) => {
                    18u8.hash(state);
                    e.hash(state)
                }
                v => {
                    21u8.hash(state);
                    v.hash(state)
                }
            },
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
            Value::Map(m) => {
                21u8.hash(state);
                m.hash(state);
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, rhs: &Value) -> bool {
        use std::num::FpCategory::*;
        Typ::get(self) == Typ::get(rhs)
            && match (self, rhs) {
                (Value::U32(l), Value::U32(r)) => l == r,
                (Value::V32(l), Value::V32(r)) => l == r,
                (Value::I32(l), Value::I32(r)) => l == r,
                (Value::Z32(l), Value::Z32(r)) => l == r,
                (Value::U64(l), Value::U64(r)) => l == r,
                (Value::V64(l), Value::V64(r)) => l == r,
                (Value::I64(l), Value::I64(r)) => l == r,
                (Value::Z64(l), Value::Z64(r)) => l == r,
                (Value::F32(l), Value::F32(r)) => match (l.classify(), r.classify()) {
                    (Nan, Nan) => true,
                    (_, _) => l == r,
                },
                (Value::F64(l), Value::F64(r)) => match (l.classify(), r.classify()) {
                    (Nan, Nan) => true,
                    (_, _) => l == r,
                },
                (Value::Decimal(l), Value::Decimal(r)) => l == r,
                (Value::DateTime(l), Value::DateTime(r)) => l == r,
                (Value::Duration(l), Value::Duration(r)) => l == r,
                (Value::Bool(l), Value::Bool(r)) => l == r,
                (Value::Null, Value::Null) => true,
                (Value::String(l), Value::String(r)) => l == r,
                (Value::Bytes(l), Value::Bytes(r)) => l == r,
                (Value::Error(l), Value::Error(r)) => l == r,
                (Value::Array(l), Value::Array(r)) => l == r,
                (Value::Map(l), Value::Map(r)) => l == r,
                (_, _) => false,
            }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use std::num::FpCategory::*;
        match Typ::get(self).cmp(&Typ::get(other)) {
            Ordering::Greater => Some(Ordering::Greater),
            Ordering::Less => Some(Ordering::Less),
            Ordering::Equal => match (self, other) {
                (Value::U32(l), Value::U32(r)) => l.partial_cmp(r),
                (Value::V32(l), Value::V32(r)) => l.partial_cmp(r),
                (Value::I32(l), Value::I32(r)) => l.partial_cmp(r),
                (Value::Z32(l), Value::Z32(r)) => l.partial_cmp(r),
                (Value::U64(l), Value::U64(r)) => l.partial_cmp(r),
                (Value::V64(l), Value::V64(r)) => l.partial_cmp(r),
                (Value::I64(l), Value::I64(r)) => l.partial_cmp(r),
                (Value::Z64(l), Value::Z64(r)) => l.partial_cmp(r),
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
                (Value::Bool(l), Value::Bool(r)) => l.partial_cmp(r),
                (Value::Null, Value::Null) => Some(Ordering::Equal),
                (Value::String(l), Value::String(r)) => l.partial_cmp(r),
                (Value::Bytes(l), Value::Bytes(r)) => l.partial_cmp(r),
                (Value::Error(l), Value::Error(r)) => l.partial_cmp(r),
                (Value::Array(l), Value::Array(r)) => l.partial_cmp(r),
                (Value::Map(l), Value::Map(r)) => l.partial_cmp(r),
                (_, _) => unreachable!(),
            },
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
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
            (Value::Decimal(l), Value::Decimal(r)) => Value::Decimal(Arc::new((*l) $op (*r))),
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
                Value::Decimal(Arc::new((*l) $op Decimal::from(r))),
            (Value::U32(l) | Value::V32(l), Value::Decimal(r)) =>
                Value::Decimal(Arc::new(Decimal::from(l) $op (*r))),
            (Value::Decimal(l), Value::U64(r) | Value::V64(r)) =>
                Value::Decimal(Arc::new((*l) $op Decimal::from(r))),
            (Value::U64(l) | Value::V64(l), Value::Decimal(r)) =>
                Value::Decimal(Arc::new(Decimal::from(l) $op (*r))),
            (Value::Decimal(l), Value::I32(r) | Value::Z32(r)) =>
                Value::Decimal(Arc::new((*l) $op Decimal::from(r))),
            (Value::I32(l) | Value::Z32(l), Value::Decimal(r)) =>
                Value::Decimal(Arc::new(Decimal::from(l) $op (*r))),
            (Value::Decimal(l), Value::I64(r) | Value::Z64(r)) =>
                Value::Decimal(Arc::new((*l) $op Decimal::from(r))),
            (Value::I64(l) | Value::Z64(l), Value::Decimal(r)) =>
                Value::Decimal(Arc::new(Decimal::from(l) $op (*r))),
            (Value::Decimal(l), Value::F32(r)) => match Decimal::try_from(r) {
                Ok(r) => Value::Decimal(Arc::new((*l) $op r)),
                Err(_) => {
                    let e = format_compact!("can't parse {} as a decimal", r);
                    Value::error(e.as_str())
                },
            },
            (Value::F32(l), Value::Decimal(r)) => match Decimal::try_from(l) {
                Ok(l) => Value::Decimal(Arc::new(l $op (*r))),
                Err(_) => {
                    let e = format_compact!("can't parse {} as a decimal", l);
                    Value::error(e.as_str())
                },
            },
            (Value::Decimal(l), Value::F64(r)) => match Decimal::try_from(r) {
                Ok(r) => Value::Decimal(Arc::new((*l) $op r)),
                Err(_) => {
                    let e = format_compact!("can't parse {} as a decimal", r);
                    Value::error(e.as_str())
                },
            },
            (Value::F64(l), Value::Decimal(r)) => match Decimal::try_from(l) {
                Ok(l) => Value::Decimal(Arc::new(l $op (*r))),
                Err(_) => {
                    let e = format_compact!("can't parse {} as a decimal", l);
                    Value::error(e.as_str())
                },
            },
            (Value::String(s), n) => match s.parse::<Value>() {
                Err(e) => Value::error(format_compact!("{}", e).as_str()),
                Ok(s) => s $op n,
            }
            (n, Value::String(s)) => match s.parse::<Value>() {
                Err(e) => Value::error(format_compact!("{}", e).as_str()),
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
                    None => Value::error(literal!("can't add to array")),
                    Some(r) => l $op r,
                }
            }
            (n, r @ Value::Array(_)) => {
                match n.cast(Typ::Array) {
                    None => Value::error(literal!("can't add to array")),
                    Some(l) => l $op r,
                }
            }
            (Value::Map(_), _) | (_, Value::Map(_)) => Value::error(literal!("can't apply to Map")),
            (Value::Bytes(_), _) | (_, Value::Bytes(_)) => {
                Value::error(literal!("can't add bytes"))
            }
            (Value::Null, _) | (_, Value::Null) => {
                Value::error(literal!("can't add null"))
            }
            | (Value::Error(_), _)
            | (_, Value::Error(_)) => Value::error(literal!("can't add error types")),
            (Value::Bool(true), n) => Value::U32(1) $op n,
            (n, Value::Bool(true)) => n $op Value::U32(1),
            (Value::Bool(false), n) => Value::U32(0) $op n,
            (n, Value::Bool(false)) => n $op Value::U32(0),
            $($pat => $blk),+
        }
    }
}

macro_rules! handle_arith_result {
    ($res:ident, $fallback:literal) => {
        match $res {
            Ok(r) => r,
            Err(e) => match e.downcast_ref::<String>() {
                Some(s) => Value::error(s),
                None => match e.downcast_ref::<&str>() {
                    Some(s) => Value::error(*s),
                    None => Value::error(literal!($fallback)),
                },
            },
        }
    };
}

impl Add for Value {
    type Output = Value;

    fn add(self, rhs: Self) -> Self {
        let res = catch_unwind(AssertUnwindSafe(|| {
            apply_op!(
                self, rhs, 0., +,
                (Value::DateTime(dt), Value::Duration(d))
                    | (Value::Duration(d), Value::DateTime(dt)) => {
                        match chrono::Duration::from_std(*d) {
                            Ok(d) => Value::DateTime(Arc::new((*dt) + d)),
                            Err(e) => Value::error(format_compact!("{}", e).as_str()),
                        }
                    },
                (Value::Duration(d0), Value::Duration(d1)) => { Value::Duration(Arc::new((*d0) + (*d1))) },
                (Value::Duration(_), _)
                    | (_, Value::Duration(_))
                    | (_, Value::DateTime(_))
                    | (Value::DateTime(_), _) => {
                        Value::error(literal!("can't add to datetime/duration"))
                    }
            )
        }));
        handle_arith_result!(res, "panic while executing add")
    }
}

impl Sub for Value {
    type Output = Value;

    fn sub(self, rhs: Self) -> Self {
        let res = catch_unwind(AssertUnwindSafe(|| {
            apply_op!(
                self, rhs, 0., -,
                (Value::DateTime(dt), Value::Duration(d))
                    | (Value::Duration(d), Value::DateTime(dt)) => {
                        match chrono::Duration::from_std(*d) {
                            Ok(d) => Value::DateTime(Arc::new((*dt) - d)),
                            Err(e) => Value::error(format_compact!("{}", e).as_str()),
                        }
                    },
                (Value::Duration(d0), Value::Duration(d1)) => { Value::Duration(Arc::new((*d0) - (*d1))) },
                (Value::Duration(_), _)
                    | (_, Value::Duration(_))
                    | (_, Value::DateTime(_))
                    | (Value::DateTime(_), _) => {
                        Value::error(literal!("can't sub datetime/duration"))
                    }
            )
        }));
        handle_arith_result!(res, "panic while executing sub")
    }
}

impl Mul for Value {
    type Output = Value;

    fn mul(self, rhs: Self) -> Self {
        let res = catch_unwind(AssertUnwindSafe(|| {
            apply_op!(
                self, rhs, 1., *,
                (Value::Duration(d), Value::U32(n) | Value::V32(n))
                | (Value::U32(n) | Value::V32(n), Value::Duration(d)) => { Value::Duration(Arc::new((*d) * n)) },
                (Value::Duration(d), Value::I32(n) | Value::Z32(n))
                | (Value::I32(n) | Value::Z32(n), Value::Duration(d)) => {
                    if n < 0 { panic!("can't multiply a duration by a negative number") }
                    Value::Duration(Arc::new((*d) * n as u32))
                },
                (Value::Duration(d), Value::U64(n) | Value::V64(n))
                | (Value::U64(n) | Value::V64(n), Value::Duration(d)) => {
                    Value::Duration(Arc::new((*d) * n as u32))
                },
                (Value::Duration(d), Value::I64(n) | Value::Z64(n))
                | (Value::I64(n) | Value::Z64(n), Value::Duration(d)) => {
                    if n < 0 { panic!("can't multiply a duration by a negative number") }
                    Value::Duration(Arc::new((*d) * n as u32))
                },
                (Value::Duration(d), Value::F32(s)) | (Value::F32(s), Value::Duration(d)) => {
                    Value::Duration(Arc::new(d.mul_f32(s)))
                },
                (Value::Duration(d), Value::F64(s)) | (Value::F64(s), Value::Duration(d)) => {
                    Value::Duration(Arc::new(d.mul_f64(s)))
                },
                    | (Value::Duration(_), _)
                    | (_, Value::Duration(_))
                    | (_, Value::DateTime(_))
                    | (Value::DateTime(_), _) => {
                        Value::error(literal!("can't mul datetime/duration"))
                    }
            )
        }));
        handle_arith_result!(res, "panic while executing mul")
    }
}

impl Div for Value {
    type Output = Value;

    fn div(self, rhs: Self) -> Self {
        let res = catch_unwind(AssertUnwindSafe(|| {
            apply_op!(
                self, rhs, 1., /,
                (Value::Duration(d), Value::U32(s) | Value::V32(s)) => { Value::Duration(Arc::new((*d) / s)) },
                (Value::Duration(d), Value::I32(s) | Value::Z32(s)) => {
                    if s < 0 { panic!("can't divide duration by a negative number") }
                    Value::Duration(Arc::new((*d) / s as u32))
                },
                (Value::Duration(d), Value::U64(s) | Value::V64(s)) => {
                    Value::Duration(Arc::new((*d) / s as u32))
                },
                (Value::Duration(d), Value::I64(s) | Value::Z64(s)) => {
                    if s < 0 { panic!("can't divide duration by a negative number") }
                    Value::Duration(Arc::new((*d) / s as u32))
                },
                (Value::Duration(d), Value::F32(s)) => { Value::Duration(Arc::new(d.div_f32(s))) },
                (Value::Duration(d), Value::F64(s)) => { Value::Duration(Arc::new(d.div_f64(s))) },
                (Value::Duration(_), _)
                    | (_, Value::Duration(_))
                    | (_, Value::DateTime(_))
                    | (Value::DateTime(_), _) => {
                        Value::error(literal!("can't div datetime/duration"))
                    }
            )
        }));
        handle_arith_result!(res, "panic while executing div")
    }
}

impl Rem for Value {
    type Output = Value;

    fn rem(self, rhs: Self) -> Self::Output {
        let res = catch_unwind(AssertUnwindSafe(|| {
            apply_op!(
                self, rhs, 1., %,
                (Value::Duration(_), _)
                    | (_, Value::Duration(_))
                    | (_, Value::DateTime(_))
                    | (Value::DateTime(_), _) => {
                        Value::error(literal!("can't mod datetime/duration"))
                    }
            )
        }));
        handle_arith_result!(res, "panic while executing mod")
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
            Value::F32(_) => Value::error(literal!("can't apply not to F32")),
            Value::F64(_) => Value::error(literal!("can't apply not to F64")),
            Value::Decimal(_) => Value::error(literal!("can't apply not to Decimal")),
            Value::DateTime(_) => Value::error(literal!("can't apply not to DateTime")),
            Value::Duration(_) => Value::error(literal!("can't apply not to Duration")),
            Value::String(_) => Value::error(literal!("can't apply not to String")),
            Value::Bytes(_) => Value::error(literal!("can't apply not to Bytes")),
            Value::Error(_) => Value::error(literal!("can't apply not to Error")),
            Value::Map(_) => Value::error(literal!("can't apply not to Map")),
            Value::Array(elts) => {
                Value::Array(ValArray::from_iter_exact(elts.iter().cloned().map(|v| !v)))
            }
        }
    }
}
