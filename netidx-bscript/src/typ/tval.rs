use super::{PrintFlag, Type};
use crate::{env::Env, typ::format_with_flags, Ctx, UserEvent};
use fxhash::FxHashSet;
use netidx::publisher::Value;
use netidx_value::NakedValue;
use std::{cell::RefCell, collections::HashSet, fmt};

/// A value with it's type, used for formatting
pub struct TVal<'a, C: Ctx, E: UserEvent> {
    pub env: &'a Env<C, E>,
    pub typ: &'a Type,
    pub v: &'a Value,
}

impl<'a, C: Ctx, E: UserEvent> TVal<'a, C, E> {
    fn fmt_int(
        &self,
        f: &mut fmt::Formatter<'_>,
        hist: &mut FxHashSet<(usize, usize)>,
    ) -> fmt::Result {
        if !self.typ.is_a(&self.env, &self.v) {
            return format_with_flags(PrintFlag::DerefTVars.into(), || {
                write!(
                    f,
                    "error, type {} does not match value {}",
                    self.typ,
                    NakedValue(self.v)
                )
            });
        }
        match (&self.typ, &self.v) {
            (Type::Primitive(_) | Type::Bottom | Type::Any | Type::Fn(_), v) => {
                write!(f, "{}", NakedValue(v))
            }
            (Type::Ref { .. }, v) => match self.typ.lookup_ref(&self.env) {
                Err(e) => write!(f, "error, {e:?}"),
                Ok(typ) => {
                    let typ_addr = (typ as *const Type).addr();
                    let v_addr = (self.v as *const Value).addr();
                    if !hist.contains(&(typ_addr, v_addr)) {
                        hist.insert((typ_addr, v_addr));
                        Self { typ, env: self.env, v }.fmt_int(f, hist)?
                    }
                    Ok(())
                }
            },
            (Type::Array(et), Value::Array(a)) => {
                write!(f, "[")?;
                for (i, v) in a.iter().enumerate() {
                    Self { typ: et, env: self.env, v }.fmt_int(f, hist)?;
                    if i < a.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, "]")
            }
            (Type::Array(_), v) => write!(f, "{}", NakedValue(v)),
            (Type::ByRef(_), v) => write!(f, "{}", NakedValue(v)),
            (Type::Struct(flds), Value::Array(a)) => {
                write!(f, "{{")?;
                for (i, ((n, et), v)) in flds.iter().zip(a.iter()).enumerate() {
                    write!(f, "{n}: ")?;
                    match v {
                        Value::Array(a) if a.len() == 2 => {
                            Self { typ: et, env: self.env, v: &a[1] }.fmt_int(f, hist)?
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
                    Self { typ: t, env: self.env, v }.fmt_int(f, hist)?;
                    if i < flds.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, ")")
            }
            (Type::Tuple(_), v) => write!(f, "{}", NakedValue(v)),
            (Type::TVar(tv), v) => match &*tv.read().typ.read() {
                None => write!(f, "{}", NakedValue(v)),
                Some(typ) => TVal { env: self.env, typ, v }.fmt_int(f, hist),
            },
            (Type::Variant(n, flds), Value::Array(a)) if a.len() >= 2 => {
                write!(f, "`{n}(")?;
                for (i, (t, v)) in flds.iter().zip(a[1..].iter()).enumerate() {
                    Self { typ: t, env: self.env, v }.fmt_int(f, hist)?;
                    if i < flds.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, ")")
            }
            (Type::Variant(_, _), Value::String(s)) => write!(f, "`{s}"),
            (Type::Variant(_, _), v) => write!(f, "{}", NakedValue(v)),
            (Type::Set(ts), v) => match ts.iter().find(|t| t.is_a(&self.env, v)) {
                None => write!(f, "{}", NakedValue(v)),
                Some(t) => Self { typ: t, env: self.env, v }.fmt_int(f, hist),
            },
        }
    }
}

impl<'a, C: Ctx, E: UserEvent> fmt::Display for TVal<'a, C, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        thread_local! {
            static HIST: RefCell<FxHashSet<(usize, usize)>> = RefCell::new(HashSet::default());
        }
        HIST.with_borrow_mut(|hist| {
            hist.clear();
            self.fmt_int(f, hist)
        })
    }
}
