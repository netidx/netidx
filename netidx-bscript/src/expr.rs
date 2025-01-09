use crate::parser;
use netidx::{chars::Chars, subscriber::Value, utils};
use regex::Regex;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    cmp::{Ordering, PartialEq, PartialOrd},
    fmt::{self, Write},
    result,
    str::FromStr,
};
use triomphe::Arc;

lazy_static! {
    pub static ref VNAME: Regex = Regex::new("^[a-z][a-z0-9_]*$").unwrap();
}

atomic_id!(ExprId);

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum ExprKind {
    Constant(Value),
    Bind { name: Chars, value: Arc<Expr> },
    Ref { name: Chars },
    Apply { args: Arc<[Expr]>, function: Chars },
}

impl ExprKind {
    pub fn to_expr(self) -> Expr {
        Expr { id: ExprId::new(), kind: self }
    }

    pub fn to_string_pretty(&self, col_limit: usize) -> String {
        let mut buf = String::new();
        self.pretty_print(0, col_limit, &mut buf).unwrap();
        buf
    }

    fn pretty_print(&self, indent: usize, limit: usize, buf: &mut String) -> fmt::Result {
        fn push_indent(indent: usize, buf: &mut String) {
            buf.extend((0..indent).into_iter().map(|_| ' '));
        }
        match self {
            ExprKind::Constant(_) => {
                push_indent(indent, buf);
                write!(buf, "{}", self)
            }
            ExprKind::Bind { name, value } => {
                let mut tmp = String::new();
                push_indent(indent, &mut tmp);
                write!(tmp, "{}", self)?;
                if tmp.len() < limit {
                    buf.push_str(&*tmp);
                    Ok(())
                } else {
                    writeln!(buf, "let {name} =")?;
                    value.kind.pretty_print(indent + 2, limit, buf)
                }
            }
            ExprKind::Ref { name: _ } => {
                push_indent(indent, buf);
                write!(buf, "{}", self)
            }
            ExprKind::Apply { function, args } => {
                let mut tmp = String::new();
                push_indent(indent, &mut tmp);
                write!(tmp, "{}", self)?;
                if tmp.len() < limit {
                    buf.push_str(&*tmp);
                    Ok(())
                } else {
                    if &**function == "string_concat" {
                        buf.push_str(&*tmp);
                        Ok(())
                    } else if &**function == "do" {
                        push_indent(indent, buf);
                        writeln!(buf, "{}", "{")?;
                        for i in 0..args.len() {
                            args[i].kind.pretty_print(indent + 2, limit, buf)?;
                            if i < args.len() - 1 {
                                writeln!(buf, ";")?
                            } else {
                                writeln!(buf, "")?
                            }
                        }
                        push_indent(indent, buf);
                        writeln!(buf, "{}", "}")
                    } else if &**function == "array" {
                        push_indent(indent, buf);
                        writeln!(buf, "{}", "[")?;
                        for i in 0..args.len() {
                            args[i].kind.pretty_print(indent + 2, limit, buf)?;
                            if i < args.len() - 1 {
                                writeln!(buf, ",")?
                            } else {
                                writeln!(buf, "")?
                            }
                        }
                        push_indent(indent, buf);
                        writeln!(buf, "{}", "]")
                    } else {
                        push_indent(indent, buf);
                        writeln!(buf, "{}(", function)?;
                        for i in 0..args.len() {
                            args[i].kind.pretty_print(indent + 2, limit, buf)?;
                            if i < args.len() - 1 {
                                writeln!(buf, ", ")?;
                            } else {
                                writeln!(buf, "")?;
                            }
                        }
                        push_indent(indent, buf);
                        writeln!(buf, ")")
                    }
                }
            }
        }
    }
}

impl fmt::Display for ExprKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExprKind::Constant(v) => v.fmt_ext(f, &parser::BSCRIPT_ESC, true),
            ExprKind::Bind { name, value } => {
                write!(f, "let {name} = {value}")
            }
            ExprKind::Ref { name } => {
                write!(f, "{name}")
            }
            ExprKind::Apply { args, function } => {
                if &**function == "string_concat" && args.len() > 0 {
                    // interpolation
                    write!(f, "\"")?;
                    for s in args.iter() {
                        match &s.kind {
                            ExprKind::Constant(Value::String(s)) if s.len() > 0 => {
                                write!(
                                    f,
                                    "{}",
                                    utils::escape(&*s, '\\', &parser::BSCRIPT_ESC)
                                )?;
                            }
                            s => {
                                write!(f, "[{}]", s)?;
                            }
                        }
                    }
                    write!(f, "\"")
                } else if &**function == "do" {
                    // do block
                    write!(f, "{}", '{')?;
                    for i in 0..args.len() {
                        if i < args.len() - 1 {
                            write!(f, "{};", &args[i])?
                        } else {
                            write!(f, "{}", &args[i])?
                        }
                    }
                    write!(f, "{}", '}')
                } else if &**function == "array" {
                    write!(f, "{}", '[')?;
                    for i in 0..args.len() {
                        if i < args.len() - 1 {
                            write!(f, "{},", &args[i])?
                        } else {
                            write!(f, "{}", &args[i])?
                        }
                    }
                    write!(f, "{}", ']')
                } else {
                    // it's a normal function
                    write!(f, "{}(", function)?;
                    for i in 0..args.len() {
                        write!(f, "{}", &args[i])?;
                        if i < args.len() - 1 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, ")")
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Expr {
    pub id: ExprId,
    pub kind: ExprKind,
}

impl PartialOrd for Expr {
    fn partial_cmp(&self, rhs: &Expr) -> Option<Ordering> {
        self.kind.partial_cmp(&rhs.kind)
    }
}

impl PartialEq for Expr {
    fn eq(&self, rhs: &Expr) -> bool {
        self.kind.eq(&rhs.kind)
    }
}

impl Eq for Expr {}

impl Serialize for Expr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl Default for Expr {
    fn default() -> Self {
        ExprKind::Constant(Value::Null).to_expr()
    }
}

#[derive(Clone, Copy)]
struct ExprVisitor;

impl<'de> Visitor<'de> for ExprVisitor {
    type Value = Expr;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "expected expression")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Expr::from_str(s).map_err(de::Error::custom)
    }

    fn visit_borrowed_str<E>(self, s: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Expr::from_str(s).map_err(de::Error::custom)
    }

    fn visit_string<E>(self, s: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Expr::from_str(&s).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Expr {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        de.deserialize_str(ExprVisitor)
    }
}

impl Expr {
    pub fn new(kind: ExprKind) -> Self {
        Expr { id: ExprId::new(), kind }
    }

    /* CR estokes: reevaluate this
    pub fn is_fn(&self) -> bool {
        match &self.kind {
            ExprKind::Constant(Value::String(c)) => VNAME.is_match(&*c),
            ExprKind::Constant(_)
            | ExprKind::Bind { .. }
            | ExprKind::Ref { .. }
            | ExprKind::Apply { .. } => false,
        }
    }
    */

    pub fn to_string_pretty(&self, col_limit: usize) -> String {
        self.kind.to_string_pretty(col_limit)
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl FromStr for Expr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        parser::parse_expr(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::prelude::*;
    use netidx_core::chars::Chars;
    use proptest::{collection, prelude::*};
    use std::time::Duration;

    fn datetime() -> impl Strategy<Value = DateTime<Utc>> {
        (
            DateTime::<Utc>::MIN_UTC.timestamp()..DateTime::<Utc>::MAX_UTC.timestamp(),
            0..1_000_000_000u32,
        )
            .prop_map(|(s, ns)| Utc.timestamp_opt(s, ns).unwrap())
    }

    fn duration() -> impl Strategy<Value = Duration> {
        (any::<u64>(), 0..1_000_000_000u32).prop_map(|(s, ns)| Duration::new(s, ns))
    }

    fn bytes() -> impl Strategy<Value = Bytes> {
        any::<Vec<u8>>().prop_map(Bytes::from)
    }

    fn chars() -> impl Strategy<Value = Chars> {
        any::<String>().prop_map(Chars::from)
    }

    fn value() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<u32>().prop_map(Value::U32),
            any::<u32>().prop_map(Value::V32),
            any::<i32>().prop_map(Value::I32),
            any::<i32>().prop_map(Value::Z32),
            any::<u64>().prop_map(Value::U64),
            any::<u64>().prop_map(Value::V64),
            any::<i64>().prop_map(Value::I64),
            any::<i64>().prop_map(Value::Z64),
            any::<f32>().prop_map(Value::F32),
            any::<f64>().prop_map(Value::F64),
            datetime().prop_map(Value::DateTime),
            duration().prop_map(Value::Duration),
            chars().prop_map(Value::String),
            bytes().prop_map(Value::Bytes),
            Just(Value::True),
            Just(Value::False),
            Just(Value::Null),
            Just(Value::Ok),
            chars().prop_map(Value::Error),
        ]
    }

    prop_compose! {
        fn random_fname()(s in "[a-z][a-z0-9_]*".prop_filter("Filter reserved words", |s| {
            s != "ok"
                && s != "true"
                && s != "false"
                && s != "null"
                && s != "load"
                && s != "store"
                && s != "load_var"
                && s != "store_var"
        })) -> Chars {
            Chars::from(s)
        }
    }

    fn valid_fname() -> impl Strategy<Value = Chars> {
        prop_oneof![
            Just(Chars::from("any")),
            Just(Chars::from("array")),
            Just(Chars::from("all")),
            Just(Chars::from("sum")),
            Just(Chars::from("product")),
            Just(Chars::from("divide")),
            Just(Chars::from("mean")),
            Just(Chars::from("min")),
            Just(Chars::from("max")),
            Just(Chars::from("and")),
            Just(Chars::from("or")),
            Just(Chars::from("not")),
            Just(Chars::from("cmp")),
            Just(Chars::from("if")),
            Just(Chars::from("filter")),
            Just(Chars::from("cast")),
            Just(Chars::from("isa")),
            Just(Chars::from("eval")),
            Just(Chars::from("count")),
            Just(Chars::from("sample")),
            Just(Chars::from("string_join")),
            Just(Chars::from("string_concat")),
            Just(Chars::from("navigate")),
            Just(Chars::from("confirm")),
            Just(Chars::from("load")),
            Just(Chars::from("get")),
            Just(Chars::from("store")),
            Just(Chars::from("set")),
            Just(Chars::from("let")),
        ]
    }

    fn fname() -> impl Strategy<Value = Chars> {
        prop_oneof![random_fname(), valid_fname(),]
    }

    fn expr() -> impl Strategy<Value = Expr> {
        let leaf = value().prop_map(|v| ExprKind::Constant(v).to_expr());
        leaf.prop_recursive(100, 1000000, 10, |inner| {
            prop_oneof![(collection::vec(inner, (0, 10)), fname()).prop_map(|(s, f)| {
                ExprKind::Apply { function: f, args: Arc::from(s) }.to_expr()
            })]
        })
    }

    fn acc_strings(args: &[Expr]) -> Arc<[Expr]> {
        let mut v: Vec<Expr> = Vec::new();
        for s in args {
            let s = s.clone();
            match s.kind {
                ExprKind::Constant(Value::String(ref c1)) => match v.last_mut() {
                    None => v.push(s),
                    Some(e0) => match &mut e0.kind {
                        ExprKind::Constant(Value::String(c0))
                            if c1.len() > 0 && c0.len() > 0 =>
                        {
                            let mut st = String::new();
                            st.push_str(&*c0);
                            st.push_str(&*c1);
                            *c0 = Chars::from(st);
                        }
                        _ => v.push(s),
                    },
                },
                _ => v.push(s),
            }
        }
        Arc::from(v)
    }

    fn check(s0: &Expr, s1: &Expr) -> bool {
        match (&s0.kind, &s1.kind) {
            (ExprKind::Constant(v0), ExprKind::Constant(v1)) => match (v0, v1) {
                (Value::Duration(d0), Value::Duration(d1)) => {
                    let f0 = d0.as_secs_f64();
                    let f1 = d1.as_secs_f64();
                    f0 == f1 || (f0 != 0. && f1 != 0. && ((f0 - f1).abs() / f0) < 1e-8)
                }
                (Value::F32(v0), Value::F32(v1)) => v0 == v1 || (v0 - v1).abs() < 1e-7,
                (Value::F64(v0), Value::F64(v1)) => v0 == v1 || (v0 - v1).abs() < 1e-8,
                (v0, v1) => dbg!(dbg!(v0) == dbg!(v1)),
            },
            (
                ExprKind::Apply { args: srs0, function: fn0 },
                ExprKind::Constant(Value::String(c1)),
            ) if &**fn0 == "string_concat" => match &acc_strings(srs0)[..] {
                [Expr { kind: ExprKind::Constant(Value::String(c0)), .. }] => c0 == c1,
                _ => false,
            },
            (
                ExprKind::Apply { args: srs0, function: fn0 },
                ExprKind::Apply { args: srs1, function: fn1 },
            ) if &*fn0 == fn1 && &**fn0 == "string_concat" => {
                let srs0 = acc_strings(srs0);
                srs0.iter().zip(srs1.iter()).fold(true, |r, (s0, s1)| r && check(s0, s1))
            }
            (
                ExprKind::Apply { args: srs0, function: f0 },
                ExprKind::Apply { args: srs1, function: f1 },
            ) if f0 == f1 && srs0.len() == srs1.len() => {
                srs0.iter().zip(srs1.iter()).fold(true, |r, (s0, s1)| r && check(s0, s1))
            }
            (_, _) => false,
        }
    }

    proptest! {
        #[test]
        fn expr_round_trip(s in expr()) {
            assert!(check(dbg!(&s), &dbg!(dbg!(s.to_string()).parse::<Expr>().unwrap())))
        }

        #[test]
        fn expr_pp_round_trip(s in expr()) {
            assert!(check(dbg!(&s), &dbg!(dbg!(s.to_string_pretty(80)).parse::<Expr>().unwrap())))
        }
    }
}
