use crate::{parser, Value};
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use netidx_core::utils;
use smallvec::smallvec;
use std::{
    fmt::{self, Write},
    ops::Deref,
};

/// A value reference that formats without type tags
pub struct NakedValue<'a>(pub &'a Value);

impl<'a> Deref for NakedValue<'a> {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a> fmt::Display for NakedValue<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt_naked(f)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_ext(f, &parser::VAL_ESC, true)
    }
}

pub fn printf(f: &mut impl Write, fmt: &str, args: &[Value]) -> Result<usize> {
    use compact_str::{format_compact, CompactString};
    use fish_printf::{printf_c_locale, Arg, ToArg};
    use rust_decimal::prelude::ToPrimitive;
    use smallvec::SmallVec;
    enum T<'a> {
        Arg(Arg<'a>),
        Index(usize),
    }
    let mut strings: SmallVec<[CompactString; 4]> = smallvec![];
    let mut fish_args: SmallVec<[T; 8]> = smallvec![];
    for v in args {
        fish_args.push(match v {
            Value::U32(v) | Value::V32(v) => T::Arg(v.to_arg()),
            Value::I32(v) | Value::Z32(v) => T::Arg(v.to_arg()),
            Value::U64(v) | Value::V64(v) => T::Arg(v.to_arg()),
            Value::I64(v) | Value::Z64(v) => T::Arg(v.to_arg()),
            Value::F32(v) => T::Arg(v.to_arg()),
            Value::F64(v) => T::Arg(v.to_arg()),
            Value::Decimal(v) => match v.to_f64() {
                Some(f) => T::Arg(f.to_arg()),
                None => {
                    strings.push(format_compact!("{v}"));
                    T::Index(strings.len() - 1)
                }
            },
            Value::DateTime(v) => {
                strings.push(format_compact!("{v}"));
                T::Index(strings.len() - 1)
            }
            Value::Duration(v) => {
                strings.push(format_compact!("{v:?}"));
                T::Index(strings.len() - 1)
            }
            Value::String(s) => T::Arg(s.to_arg()),
            Value::Bytes(b) => {
                strings.push(format_compact!("{}", BASE64.encode(b)));
                T::Index(strings.len() - 1)
            }
            Value::Bool(true) => T::Arg("true".to_arg()),
            Value::Bool(false) => T::Arg("false".to_arg()),
            Value::Null => T::Arg("null".to_arg()),
            v @ Value::Error(_) => {
                strings.push(format_compact!("{v}"));
                T::Index(strings.len() - 1)
            }
            v @ Value::Array(_) => {
                strings.push(format_compact!("{v}"));
                T::Index(strings.len() - 1)
            }
        })
    }
    let mut fish_args: SmallVec<[Arg; 8]> = fish_args
        .into_iter()
        .map(|t| match t {
            T::Arg(a) => a,
            T::Index(i) => strings[i].to_arg(),
        })
        .collect();
    printf_c_locale(f, fmt, &mut fish_args).map_err(|e| anyhow!(format!("{e:?}")))
}

impl Value {
    pub fn to_string_naked(&self) -> String {
        format!("{}", NakedValue(self))
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
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Bytes(b) => write!(f, "{}", BASE64.encode(b)),
            Value::Bool(true) => write!(f, "true"),
            Value::Bool(false) => write!(f, "false"),
            Value::Null => write!(f, "null"),
            v @ Value::Error(_) => write!(f, "{}", v),
            v @ Value::Array(_) => write!(f, "{}", v),
        }
    }

    pub fn fmt_notyp(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_ext(f, &parser::VAL_ESC, false)
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
}
