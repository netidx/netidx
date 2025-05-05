use super::{NoRefs, Type};
use netidx::publisher::Value;
use netidx_netproto::value::NakedValue;
use std::fmt;

/// A value with it's type, used for formatting
#[derive(Debug, Clone)]
pub struct TVal<'a>(pub &'a Type<NoRefs>, pub &'a Value);

impl<'a> fmt::Display for TVal<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.0.is_a(self.1) {
            return write!(
                f,
                "error, type {} does not match value {}",
                self.0,
                NakedValue(self.1)
            );
        }
        match (self.0, self.1) {
            (Type::Primitive(_) | Type::Bottom(_) | Type::Ref(_) | Type::Fn(_), v) => {
                write!(f, "{}", NakedValue(v))
            }
            (Type::Array(et), Value::Array(a)) => {
                write!(f, "[")?;
                for (i, v) in a.iter().enumerate() {
                    write!(f, "{}", Self(et, v))?;
                    if i < a.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, "]")
            }
            (Type::Array(_), v) => write!(f, "{}", NakedValue(v)),
            (Type::Struct(flds), Value::Array(a)) => {
                write!(f, "{{")?;
                for (i, ((n, et), v)) in flds.iter().zip(a.iter()).enumerate() {
                    write!(f, "{n}: ")?;
                    match v {
                        Value::Array(a) if a.len() == 2 => {
                            write!(f, "{}", Self(et, &a[1]))?
                        }
                        _ => write!(f, "err")?,
                    }
                    if i < flds.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, "}}")
            }
            (Type::Struct(_), v) => write!(f, "{}", NakedValue(v)),
            (Type::Tuple(flds), Value::Array(a)) => {
                write!(f, "(")?;
                for (i, (t, v)) in flds.iter().zip(a.iter()).enumerate() {
                    write!(f, "{}", Self(t, v))?;
                    if i < flds.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, ")")
            }
            (Type::Tuple(_), v) => write!(f, "{}", NakedValue(v)),
            (Type::TVar(tv), v) => match &*tv.read().typ.read() {
                None => write!(f, "{}", NakedValue(v)),
                Some(t) => write!(f, "{}", TVal(t, v)),
            },
            (Type::Variant(n, flds), Value::Array(a)) if a.len() >= 2 => {
                write!(f, "`{n}(")?;
                for (i, (t, v)) in flds.iter().zip(a[1..].iter()).enumerate() {
                    write!(f, "{}", Self(t, v))?;
                    if i < flds.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, ")")
            }
            (Type::Variant(_, _), Value::String(s)) => write!(f, "`{s}"),
            (Type::Variant(_, _), v) => write!(f, "{}", NakedValue(v)),
            (Type::Set(ts), v) => match ts.iter().find(|t| t.is_a(v)) {
                None => write!(f, "{}", NakedValue(v)),
                Some(t) => write!(f, "{}", Self(t, v)),
            },
        }
    }
}
