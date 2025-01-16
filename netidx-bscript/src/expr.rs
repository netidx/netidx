use crate::parser;
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::Value,
    utils::{self, Either},
};
use regex::Regex;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    borrow::Borrow,
    cmp::{Ordering, PartialEq, PartialOrd},
    fmt::{self, Display, Write},
    result,
    str::FromStr,
};
use triomphe::Arc;

lazy_static! {
    pub static ref VNAME: Regex = Regex::new("^[a-z][a-z0-9_]*$").unwrap();
}

atomic_id!(ExprId);

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct ModPath(pub Path);

impl Display for ModPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = Path::levels(&self.0);
        for (i, part) in Path::parts(&self.0).enumerate() {
            write!(f, "{part}")?;
            if i < len - 1 {
                write!(f, "::")?
            }
        }
        Ok(())
    }
}

impl<A> FromIterator<A> for ModPath
where
    A: Borrow<str>,
{
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        ModPath(Path::from_iter(iter))
    }
}

impl<I, A> From<I> for ModPath
where
    A: Borrow<str>,
    I: IntoIterator<Item = A>,
{
    fn from(value: I) -> Self {
        ModPath::from_iter(value)
    }
}

impl PartialEq<[&str]> for ModPath {
    fn eq(&self, other: &[&str]) -> bool {
        Path::levels(&self.0) == other.len()
            && Path::parts(&self.0).zip(other.iter()).all(|(s0, s1)| s0 == *s1)
    }
}

impl<const L: usize> PartialEq<[&str; L]> for ModPath {
    fn eq(&self, other: &[&str; L]) -> bool {
        Path::levels(&self.0) == L
            && Path::parts(&self.0).zip(other.iter()).all(|(s0, s1)| s0 == *s1)
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum ExprKind {
    Constant(Value),
    Module { name: Chars, export: bool, value: Option<Arc<[Expr]>> },
    Do { exprs: Arc<[Expr]> },
    Use { name: ModPath },
    Bind { name: Chars, export: bool, value: Arc<Expr> },
    Ref { name: ModPath },
    Connect { name: ModPath, value: Arc<Expr> },
    Lambda { args: Arc<[Chars]>, vargs: bool, body: Arc<Either<Expr, Chars>> },
    Apply { args: Arc<[Expr]>, function: ModPath },
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
        fn pretty_print_exprs(
            indent: usize,
            limit: usize,
            buf: &mut String,
            exprs: &[Expr],
            open: &str,
            close: &str,
            sep: &str,
        ) -> fmt::Result {
            writeln!(buf, "{}", open)?;
            for i in 0..exprs.len() {
                exprs[i].kind.pretty_print(indent + 2, limit, buf)?;
                if i < exprs.len() - 1 {
                    writeln!(buf, "{}", sep)?
                } else {
                    writeln!(buf, "")?
                }
            }
            push_indent(indent, buf);
            writeln!(buf, "{}", close)
        }
        macro_rules! try_single_line {
            ($trunc:ident) => {{
                let len = buf.len();
                push_indent(indent, buf);
                write!(buf, "{}", self)?;
                if buf.len() - len <= limit {
                    return Ok(());
                } else {
                    if $trunc {
                        buf.truncate(len + indent)
                    }
                    len + indent
                }
            }};
        }
        let exp = |export| if export { "pub " } else { "" };
        match self {
            ExprKind::Constant(_)
            | ExprKind::Use { name: _ }
            | ExprKind::Ref { name: _ }
            | ExprKind::Module { name: _, export: _, value: None } => {
                push_indent(indent, buf);
                write!(buf, "{self}")
            }
            ExprKind::Bind { export, name, value } => {
                try_single_line!(true);
                writeln!(buf, "{}let {name} =", exp(*export))?;
                value.kind.pretty_print(indent + 2, limit, buf)?;
                write!(buf, ";")
            }
            ExprKind::Module { name, export, value: Some(exprs) } => {
                try_single_line!(true);
                write!(buf, "{}mod {name} ", exp(*export))?;
                pretty_print_exprs(indent, limit, buf, exprs, "{", "}", "")
            }
            ExprKind::Do { exprs } => {
                try_single_line!(true);
                pretty_print_exprs(indent, limit, buf, exprs, "{", "}", "")
            }
            ExprKind::Connect { name, value } => {
                try_single_line!(true);
                writeln!(buf, "{name} <- ")?;
                value.kind.pretty_print(indent + 2, limit, buf)?;
                write!(buf, ";")
            }
            ExprKind::Apply { function, args } => {
                let len = try_single_line!(false);
                if function == &["str", "concat"] {
                    Ok(())
                } else if function == &["array"] {
                    buf.truncate(len);
                    pretty_print_exprs(indent, limit, buf, args, "[", "]", ",")
                } else {
                    buf.truncate(len);
                    write!(buf, "{function}")?;
                    pretty_print_exprs(indent, limit, buf, args, "(", ")", ",")
                }
            }
            ExprKind::Lambda { args, vargs, body } => {
                try_single_line!(true);
                write!(buf, "|")?;
                for i in 0..args.len() {
                    write!(buf, "{}", &args[i])?;
                    if *vargs || i < args.len() - 1 {
                        write!(buf, ", ")?
                    }
                }
                if *vargs {
                    write!(buf, "@args")?;
                }
                write!(buf, "| ")?;
                match &body {
                    Either::Right(builtin) => {
                        write!(buf, "'{builtin}")
                    }
                    Either::Left(body) => match &body.kind {
                        ExprKind::Apply { args, function } if function == &["do"] => {
                            pretty_print_exprs(indent, limit, buf, args, "{", "}", "")
                        }
                        _ => {
                            writeln!(buf, "")?;
                            body.kind.pretty_print(indent + 2, limit, buf)
                        }
                    },
                }
            }
        }
    }
}

impl fmt::Display for ExprKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn print_exprs(
            f: &mut fmt::Formatter,
            exprs: &[Expr],
            open: &str,
            close: &str,
            sep: &str,
        ) -> fmt::Result {
            write!(f, "{open}")?;
            for i in 0..exprs.len() {
                write!(f, "{}", &exprs[i])?;
                if i < exprs.len() - 1 {
                    write!(f, "{sep}")?
                }
            }
            write!(f, "{close}")
        }
        let exp = |export| if export { "pub " } else { "" };
        match self {
            ExprKind::Constant(v) => v.fmt_ext(f, &parser::BSCRIPT_ESC, true),
            ExprKind::Bind { export, name, value } => {
                write!(f, "{}let {name} = {value};", exp(*export))
            }
            ExprKind::Connect { name, value } => {
                write!(f, "{name} <- {value};")
            }
            ExprKind::Use { name } => {
                write!(f, "use {name};")
            }
            ExprKind::Ref { name } => {
                write!(f, "{name}")
            }
            ExprKind::Module { name, export, value } => {
                write!(f, "{}mod {name}", exp(*export))?;
                match value {
                    Some(exprs) => print_exprs(f, &**exprs, "{", "}", ""),
                    None => write!(f, ";"),
                }
            }
            ExprKind::Do { exprs } => print_exprs(f, &**exprs, "{", "}", ""),
            ExprKind::Lambda { args, vargs, body } => {
                write!(f, "|")?;
                for i in 0..args.len() {
                    write!(f, "{}", args[i])?;
                    if !vargs || i < args.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                if !vargs {
                    write!(f, "@args")?;
                }
                write!(f, "| ")?;
                match body {
                    Either::Left(body) => write!(f, "{body}"),
                    Either::Right(builtin) => write!(f, "'{builtin}"),
                }
            }
            ExprKind::Apply { args, function } => {
                if function == &["str", "concat"] && args.len() > 0 {
                    // interpolation
                    write!(f, "\"")?;
                    for s in args.iter() {
                        match &s.kind {
                            ExprKind::Constant(Value::String(s)) if s.len() > 0 => {
                                let es = utils::escape(&*s, '\\', &parser::BSCRIPT_ESC);
                                write!(f, "{es}",)?;
                            }
                            s => {
                                write!(f, "[{s}]")?;
                            }
                        }
                    }
                    write!(f, "\"")
                } else if function == &["array"] {
                    print_exprs(f, &**args, "[", "]", ",")
                } else {
                    write!(f, "{function}")?;
                    print_exprs(f, &**args, "(", ")", ",")
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
        })) -> ModPath {
            ModPath::from_iter([s])
        }
    }

    fn valid_fname() -> impl Strategy<Value = ModPath> {
        prop_oneof![
            Just(ModPath::from_iter(["any"])),
            Just(ModPath::from_iter(["array"])),
            Just(ModPath::from_iter(["all"])),
            Just(ModPath::from_iter(["sum"])),
            Just(ModPath::from_iter(["product"])),
            Just(ModPath::from_iter(["divide"])),
            Just(ModPath::from_iter(["mean"])),
            Just(ModPath::from_iter(["min"])),
            Just(ModPath::from_iter(["max"])),
            Just(ModPath::from_iter(["and"])),
            Just(ModPath::from_iter(["or"])),
            Just(ModPath::from_iter(["not"])),
            Just(ModPath::from_iter(["cmp"])),
            Just(ModPath::from_iter(["if"])),
            Just(ModPath::from_iter(["filter"])),
            Just(ModPath::from_iter(["cast"])),
            Just(ModPath::from_iter(["isa"])),
            Just(ModPath::from_iter(["eval"])),
            Just(ModPath::from_iter(["count"])),
            Just(ModPath::from_iter(["sample"])),
            Just(ModPath::from_iter(["str", "join"])),
            Just(ModPath::from_iter(["str", "concat"])),
            Just(ModPath::from_iter(["navigate"])),
            Just(ModPath::from_iter(["confirm"])),
            Just(ModPath::from_iter(["load"])),
            Just(ModPath::from_iter(["get"])),
            Just(ModPath::from_iter(["store"])),
            Just(ModPath::from_iter(["set"])),
            Just(ModPath::from_iter(["let"])),
        ]
    }

    fn fname() -> impl Strategy<Value = ModPath> {
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
            ) if fn0 == &["str", "concat"] => match &acc_strings(srs0)[..] {
                [Expr { kind: ExprKind::Constant(Value::String(c0)), .. }] => c0 == c1,
                _ => false,
            },
            (
                ExprKind::Apply { args: srs0, function: fn0 },
                ExprKind::Apply { args: srs1, function: fn1 },
            ) if fn0 == fn1 && fn0 == &["str", "concat"] => {
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
