use super::super::publisher::Typ;
use super::Source;
use anyhow::Result;
use indexmap::IndexMap;
use netidx::{
    chars::Chars,
    subscriber::{SubId, Value},
};
use std::{
    cell::{Cell, RefCell},
    cmp::{PartialEq, PartialOrd},
    rc::Rc,
    sync::Arc,
};

fn eval_any(from: &[Source]) -> Option<Value> {
    from.into_iter().find_map(|s| s.current())
}

fn eval_all(from: &[Source]) -> Option<Value> {
    match from {
        [] => None,
        [hd, tl @ ..] => match hd.current() {
            None => None,
            v @ Some(_) => {
                if tl.into_iter().all(|s| s.current() == v) {
                    v
                } else {
                    None
                }
            }
        },
    }
}

fn add_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l + r),
    }
}

fn eval_sum(from: &[Source]) -> Option<Value> {
    from.into_iter().fold(None, |res, s| match res {
        res @ Some(Value::Error(_)) => res,
        res => add_vals(res, s.current()),
    })
}

fn prod_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l * r),
    }
}

fn eval_product(from: &[Source]) -> Option<Value> {
    from.into_iter().fold(None, |res, s| match res {
        res @ Some(Value::Error(_)) => res,
        res => prod_vals(res, s.current()),
    })
}

fn div_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l / r),
    }
}

fn eval_divide(from: &[Source]) -> Option<Value> {
    from.into_iter().fold(None, |res, s| match res {
        res @ Some(Value::Error(_)) => res,
        res => div_vals(res, s.current()),
    })
}

fn cast_val(typ: Typ, v: Value) -> Option<Value> {
    match typ {
        Typ::U32 => match v {
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
        Typ::V32 => match v {
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
        Typ::I32 => match v {
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
        Typ::Z32 => match v {
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
        Typ::U64 => match v {
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
        Typ::V64 => match v {
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
        Typ::I64 => match v {
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
        Typ::Z64 => match v {
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
        Typ::F32 => match v {
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
        Typ::F64 => match v {
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
        Typ::Bool => match v {
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
        Typ::String => match v {
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
            Value::String(s) => Some(Value::String(s)),
            Value::Bytes(_) => None,
            Value::True => Some(Value::String(Chars::from("true"))),
            Value::False => Some(Value::String(Chars::from("false"))),
            Value::Null => Some(Value::String(Chars::from("null"))),
            Value::Ok => Some(Value::String(Chars::from("ok"))),
            Value::Error(s) => Some(Value::String(s)),
        },
        Typ::Bytes => None,
        Typ::Result => match v {
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
            Value::String(_) => Some(Value::Ok),
            Value::Bytes(_) => None,
            Value::True => Some(Value::Ok),
            Value::False => Some(Value::Ok),
            Value::Null => Some(Value::Ok),
            Value::Ok => Some(Value::Ok),
            Value::Error(s) => Some(Value::Error(s)),
        },
    }
}

#[derive(Debug, Clone)]
struct Mean {
    total: Cell<f64>,
    samples: Cell<usize>,
}

impl Mean {
    fn new() -> Self {
        Mean { total: Cell::new(0.), samples: Cell::new(0) }
    }

    fn eval(&self, from: &[Source]) -> Option<Value> {
        for s in from.into_iter() {
            if let Some(v) = s.current() {
                if let Some(Value::F64(v)) = cast_val(Typ::F64, v) {
                    self.total.set(self.total.get() + v);
                    self.samples.set(self.samples.get() + 1);
                }
            }
        }
        if self.samples.get() > 0 {
            Some(Value::F64(self.total.get() / (self.samples.get() as f64)))
        } else {
            None
        }
    }
}

fn eval_min(from: &[Source]) -> Option<Value> {
    from.into_iter().filter_map(|s| s.current()).fold(None, |res, v| match res {
        None => Some(v),
        Some(v0) => {
            if v < v0 {
                Some(v)
            } else {
                Some(v0)
            }
        }
    })
}

fn eval_max(from: &[Source]) -> Option<Value> {
    from.into_iter().filter_map(|s| s.current()).fold(None, |res, v| match res {
        None => Some(v),
        Some(v0) => {
            if v > v0 {
                Some(v)
            } else {
                Some(v0)
            }
        }
    })
}

fn eval_and(from: &[Source]) -> Option<Value> {
    let res = from.into_iter().all(|s| match s.current() {
        Some(Value::True) => true,
        _ => false,
    });
    if res {
        Some(Value::True)
    } else {
        Some(Value::False)
    }
}

fn eval_or(from: &[Source]) -> Option<Value> {
    let res = from.into_iter().any(|s| match s.current() {
        Some(Value::True) => true,
        _ => false,
    });
    if res {
        Some(Value::True)
    } else {
        Some(Value::False)
    }
}

fn eval_not(from: &[Source]) -> Option<Value> {
    match from {
        [s] => s.current().map(|v| !v),
        _ => Some(Value::Error(Chars::from("not expected 1 argument"))),
    }
}

fn eval_op<T: PartialEq + PartialOrd>(op: &str, v0: T, v1: T) -> Value {
    match op {
        "eq" => {
            if v0 == v1 {
                Value::True
            } else {
                Value::False
            }
        }
        "lt" => {
            if v0 < v1 {
                Value::True
            } else {
                Value::False
            }
        }
        "gt" => {
            if v0 > v1 {
                Value::True
            } else {
                Value::False
            }
        }
        "lte" => {
            if v0 <= v1 {
                Value::True
            } else {
                Value::False
            }
        }
        "gte" => {
            if v0 >= v1 {
                Value::True
            } else {
                Value::False
            }
        }
        op => Value::Error(Chars::from(format!(
            "invalid op {}, expected eq, lt, gt, lte, or gte",
            op
        ))),
    }
}

fn eval_cmp(from: &[Source]) -> Option<Value> {
    match from {
        [op, v0, v1] => match op.current() {
            None => None,
            Some(Value::String(op)) => match (v0.current(), v1.current()) {
                (None, None) => Some(Value::False),
                (_, None) => Some(Value::False),
                (None, _) => Some(Value::False),
                (Some(v0), Some(v1)) => match (v0, v1) {
                    (Value::U32(v0), Value::U32(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::U32(v0), Value::V32(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::V32(v0), Value::V32(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::V32(v0), Value::U32(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::I32(v0), Value::I32(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::I32(v0), Value::Z32(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::Z32(v0), Value::Z32(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::Z32(v0), Value::I32(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::U64(v0), Value::U64(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::U64(v0), Value::V64(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::V64(v0), Value::V64(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::V64(v0), Value::U64(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::I64(v0), Value::I64(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::I64(v0), Value::Z64(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::Z64(v0), Value::Z64(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::Z64(v0), Value::I64(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::F32(v0), Value::F32(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::F64(v0), Value::F64(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::String(v0), Value::String(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::Bytes(v0), Value::Bytes(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::True, Value::True) => Some(eval_op(&*op, true, true)),
                    (Value::True, Value::False) => Some(eval_op(&*op, true, false)),
                    (Value::False, Value::True) => Some(eval_op(&*op, false, true)),
                    (Value::False, Value::False) => Some(eval_op(&*op, false, false)),
                    (Value::Ok, Value::Ok) => Some(eval_op(&*op, true, true)),
                    (Value::Error(v0), Value::Error(v1)) => Some(eval_op(&*op, v0, v1)),
                    (v0, v1) => Some(Value::Error(Chars::from(format!(
                        "can't compare incompatible types {:?} and {:?}",
                        v0, v1
                    )))),
                },
            },
        },
    }
}

fn eval_if(from: &[Source]) -> Option<Value> {
    match from {
        [cond, b1, b2] => match cond.current() {
            None => None,
            Some(Value::True) => b1.current(),
            Some(Value::False) => b2.current(),
            _ => Some(Value::Error(Chars::from("if: expected boolean condition"))),
        },
        _ => Some(Value::Error(Chars::from("if: expected 3 arguments"))),
    }
}

fn with_typ_prefix(
    from: &[Source],
    name: &'static str,
    f: impl Fn(Typ, &Source) -> Option<Value>,
) -> Option<Value> {
    match from {
        [typ, src] => match typ.current() {
            None => None,
            Some(Value::String(s)) => match s.parse::<Typ>() {
                Err(e) => Some(Value::Error(Chars::from(format!(
                    "{}: invalid type {}, {}",
                    name, s, e
                )))),
                Ok(typ) => f(typ, src),
            },
            _ => Some(Value::Error(Chars::from(format!(
                "{} expected typ as string",
                name
            )))),
        },
        _ => Some(Value::Error(Chars::from(format!("{} expected 2 arguments", name)))),
    }
}

fn eval_filter(from: &[Source]) -> Option<Value> {
    with_typ_prefix(from, "filter(typ, src)", |typ, src| match (typ, src.current()) {
        (_, None) => None,
        (Typ::U32, v @ Some(Value::U32(_))) => v,
        (Typ::V32, v @ Some(Value::V32(_))) => v,
        (Typ::I32, v @ Some(Value::I32(_))) => v,
        (Typ::Z32, v @ Some(Value::Z32(_))) => v,
        (Typ::U64, v @ Some(Value::U64(_))) => v,
        (Typ::V64, v @ Some(Value::V64(_))) => v,
        (Typ::I64, v @ Some(Value::I64(_))) => v,
        (Typ::Z64, v @ Some(Value::Z64(_))) => v,
        (Typ::F32, v @ Some(Value::F32(_))) => v,
        (Typ::F64, v @ Some(Value::F64(_))) => v,
        (Typ::Bool, v @ Some(Value::True)) => v,
        (Typ::Bool, v @ Some(Value::False)) => v,
        (Typ::String, v @ Some(Value::String(_))) => v,
        (Typ::Bytes, v @ Some(Value::Bytes(_))) => v,
        (Typ::Result, v @ Some(Value::Ok)) => v,
        (Typ::Result, v @ Some(Value::Error(_))) => v,
        (_, _) => None,
    })
}

fn eval_cast(from: &[Source]) -> Option<Value> {
    with_typ_prefix(from, "cast(typ, src)", |typ, src| match src.current() {
        None => None,
        Some(v) => cast_val(typ, v),
    })
}

fn eval_isa(from: &[Source]) -> Option<Value> {
    with_typ_prefix(from, "isa(typ, src)", |typ, src| match (typ, src.current()) {
        (_, None) => None,
        (Typ::U32, Some(Value::U32(_))) => Some(Value::True),
        (Typ::V32, Some(Value::V32(_))) => Some(Value::True),
        (Typ::I32, Some(Value::I32(_))) => Some(Value::True),
        (Typ::Z32, Some(Value::Z32(_))) => Some(Value::True),
        (Typ::U64, Some(Value::U64(_))) => Some(Value::True),
        (Typ::V64, Some(Value::V64(_))) => Some(Value::True),
        (Typ::I64, Some(Value::I64(_))) => Some(Value::True),
        (Typ::Z64, Some(Value::Z64(_))) => Some(Value::True),
        (Typ::F32, Some(Value::F32(_))) => Some(Value::True),
        (Typ::F64, Some(Value::F64(_))) => Some(Value::True),
        (Typ::Bool, Some(Value::True)) => Some(Value::True),
        (Typ::Bool, Some(Value::False)) => Some(Value::True),
        (Typ::String, Some(Value::String(_))) => Some(Value::True),
        (Typ::Bytes, Some(Value::Bytes(_))) => Some(Value::True),
        (Typ::Result, Some(Value::Ok)) => Some(Value::True),
        (Typ::Result, Some(Value::Error(_))) => Some(Value::True),
        (_, Some(_)) => Some(Value::False),
    })
}

#[derive(Debug, Clone)]
pub(super) enum Formula {
    Any,
    All,
    Sum,
    Product,
    Divide,
    Mean(Mean),
    Min,
    Max,
    And,
    Or,
    Not,
    Cmp,
    If,
    Filter,
    Cast,
    IsA,
}

impl Formula {
    pub(super) fn new(name: &str) -> Result<Formula> {
        match name {
            "any" => Ok(Formula::Any),
            "all" => Ok(Formula::All),
            "sum" => Ok(Formula::Sum),
            "product" => Ok(Formula::Product),
            "divide" => Ok(Formula::Divide),
            "mean" => Ok(Formula::Mean(Mean::new())),
            "min" => Ok(Formula::Min),
            "max" => Ok(Formula::Max),
            "and" => Ok(Formula::And),
            "or" => Ok(Formula::Or),
            "not" => Ok(Formula::Not),
            "if" => Ok(Formula::If),
            "filter" => Ok(Formula::Filter),
            "cast" => Ok(Formula::Cast),
            "isa" => Ok(Formula::IsA),
            name => Err(anyhow!("no such function {}", name)),
        }
    }

    pub(super) fn current(&self, from: &[Source]) -> Option<Value> {
        match self {
            Formula::Any => eval_any(from),
            Formula::All => eval_all(from),
            Formula::Sum => eval_sum(from),
            Formula::Product => eval_product(from),
            Formula::Divide => eval_divide(from),
            Formula::Mean(m) => m.eval(from),
            Formula::Min => eval_min(from),
            Formula::Max => eval_max(from),
            Formula::And => eval_and(from),
            Formula::Or => eval_or(from),
            Formula::Not => eval_not(from),
            Formula::Cmp => eval_cmp(from),
            Formula::If => eval_if(from),
            Formula::Filter => eval_filter(from),
            Formula::Cast => eval_cast(from),
            Formula::IsA => eval_isa(from),
        }
    }

    pub(super) fn update(
        &self,
        from: &[Source],
        changed: &Arc<IndexMap<SubId, Value>>,
    ) -> Option<Value> {
        if !from.into_iter().filter_map(|s| s.update(changed)).last().is_some() {
            None
        } else {
            self.current(from)
        }
    }

    pub(super) fn update_var(
        &self,
        from: &[Source],
        name: &str,
        value: &Value,
    ) -> Option<Value> {
        if !from.into_iter().filter_map(|s| s.update_var(name, value)).last().is_some() {
            None
        } else {
            self.current(from)
        }
    }
}
