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
    ops::Deref,
    result,
    str::FromStr,
};
use triomphe::Arc;

#[cfg(test)]
mod test;

lazy_static! {
    pub static ref VNAME: Regex = Regex::new("^[a-z][a-z0-9_]*$").unwrap();
}

atomic_id!(ExprId);

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct ModPath(pub Path);

impl Borrow<str> for ModPath {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}

impl Deref for ModPath {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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
    Lambda { args: Arc<[Chars]>, vargs: bool, body: Either<Arc<Expr>, Chars> },
    Apply { args: Arc<[Expr]>, function: ModPath },
    Select { arms: Arc<[(Expr, Expr)]> },
    Eq { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Ne { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Lt { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Gt { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Lte { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Gte { lhs: Arc<Expr>, rhs: Arc<Expr> },
    And { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Or { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Not { expr: Arc<Expr> },
    Add { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Sub { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Mul { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Div { lhs: Arc<Expr>, rhs: Arc<Expr> },
}

impl ExprKind {
    pub fn to_expr(self) -> Expr {
        Expr { id: ExprId::new(), kind: self }
    }

    pub fn to_string_pretty(&self, col_limit: usize) -> String {
        let mut buf = String::new();
        self.pretty_print(0, col_limit, true, &mut buf).unwrap();
        buf
    }

    fn pretty_print(
        &self,
        indent: usize,
        limit: usize,
        newline: bool,
        buf: &mut String,
    ) -> fmt::Result {
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
                exprs[i].kind.pretty_print(indent + 2, limit, true, buf)?;
                if i < exprs.len() - 1 {
                    buf.pop(); // pop the newline
                    writeln!(buf, "{}", sep)?
                }
            }
            push_indent(indent, buf);
            writeln!(buf, "{}", close)
        }
        macro_rules! try_single_line {
            ($trunc:ident) => {{
                let len = buf.len();
                if newline {
                    push_indent(indent, buf);
                }
                writeln!(buf, "{}", self)?;
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
        macro_rules! binop {
            ($sep:literal, $lhs:expr, $rhs:expr) => {{
                try_single_line!(true);
                write!(buf, "(")?;
                writeln!(buf, "{} {}", $lhs, $sep)?;
                $rhs.kind.pretty_print(indent, limit, true, buf)?;
                write!(buf, ")")
            }};
        }
        let exp = |export| if export { "pub " } else { "" };
        match self {
            ExprKind::Constant(_)
            | ExprKind::Use { name: _ }
            | ExprKind::Ref { name: _ }
            | ExprKind::Module { name: _, export: _, value: None } => {
                if newline {
                    push_indent(indent, buf);
                }
                writeln!(buf, "{self}")
            }
            ExprKind::Bind { export, name, value } => {
                try_single_line!(true);
                writeln!(buf, "{}let {name} = ", exp(*export))?;
                value.kind.pretty_print(indent + 2, limit, false, buf)
            }
            ExprKind::Module { name, export, value: Some(exprs) } => {
                try_single_line!(true);
                write!(buf, "{}mod {name} ", exp(*export))?;
                pretty_print_exprs(indent, limit, buf, exprs, "{", "}", "")
            }
            ExprKind::Do { exprs } => {
                try_single_line!(true);
                pretty_print_exprs(indent, limit, buf, exprs, "{", "}", ";")
            }
            ExprKind::Connect { name, value } => {
                try_single_line!(true);
                writeln!(buf, "{name} <- ")?;
                value.kind.pretty_print(indent + 2, limit, false, buf)
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
                match body {
                    Either::Right(builtin) => {
                        writeln!(buf, "'{builtin}")
                    }
                    Either::Left(body) => match &body.kind {
                        ExprKind::Do { exprs } => {
                            pretty_print_exprs(indent, limit, buf, exprs, "{", "}", ";")
                        }
                        _ => body.kind.pretty_print(indent, limit, false, buf),
                    },
                }
            }
            ExprKind::Select { arms } => {
                try_single_line!(true);
                writeln!(buf, "select {{")?;
                for (i, (pred, expr)) in arms.iter().enumerate() {
                    write!(buf, "{pred} => ")?;
                    if let ExprKind::Do { exprs } = &expr.kind {
                        let term = if i < arms.len() - 1 { "}," } else { "}" };
                        pretty_print_exprs(indent, limit, buf, exprs, "{", term, ";")?
                    } else if i < arms.len() - 1 {
                        expr.kind.pretty_print(indent, limit, false, buf)?;
                        buf.pop();
                        writeln!(buf, ",")?
                    } else {
                        expr.kind.pretty_print(indent, limit, false, buf)?;
                    }
                }
                push_indent(indent, buf);
                writeln!(buf, "}}")
            }
            ExprKind::Eq { lhs, rhs } => binop!("=", lhs, rhs),
            ExprKind::Ne { lhs, rhs } => binop!("!=", lhs, rhs),
            ExprKind::Lt { lhs, rhs } => binop!("<", lhs, rhs),
            ExprKind::Gt { lhs, rhs } => binop!(">", lhs, rhs),
            ExprKind::Lte { lhs, rhs } => binop!("<=", lhs, rhs),
            ExprKind::Gte { lhs, rhs } => binop!(">=", lhs, rhs),
            ExprKind::And { lhs, rhs } => binop!("&&", lhs, rhs),
            ExprKind::Or { lhs, rhs } => binop!("||", lhs, rhs),
            ExprKind::Add { lhs, rhs } => binop!("+", lhs, rhs),
            ExprKind::Sub { lhs, rhs } => binop!("-", lhs, rhs),
            ExprKind::Mul { lhs, rhs } => binop!("*", lhs, rhs),
            ExprKind::Div { lhs, rhs } => binop!("/", lhs, rhs),
            ExprKind::Not { expr } => {
                try_single_line!(true);
                match &expr.kind {
                    ExprKind::Do { exprs } => {
                        pretty_print_exprs(indent, limit, buf, exprs, "!{", "}", ";")
                    }
                    _ => {
                        writeln!(buf, "!(")?;
                        expr.kind.pretty_print(indent + 2, limit, true, buf)?;
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
        fn write_binop(
            f: &mut fmt::Formatter,
            op: &str,
            lhs: &Expr,
            rhs: &Expr,
        ) -> fmt::Result {
            write!(f, "(")?;
            write!(f, "{lhs} {op} {rhs}")?;
            write!(f, ")")
        }
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
                write!(f, "{}let {name} = {value}", exp(*export))
            }
            ExprKind::Connect { name, value } => {
                write!(f, "{name} <- {value}")
            }
            ExprKind::Use { name } => {
                write!(f, "use {name}")
            }
            ExprKind::Ref { name } => {
                write!(f, "{name}")
            }
            ExprKind::Module { name, export, value } => {
                write!(f, "{}mod {name}", exp(*export))?;
                match value {
                    None => write!(f, ";"),
                    Some(exprs) => print_exprs(f, &**exprs, "{", "}", " "),
                }
            }
            ExprKind::Do { exprs } => print_exprs(f, &**exprs, "{", "}", "; "),
            ExprKind::Lambda { args, vargs, body } => {
                write!(f, "|")?;
                for i in 0..args.len() {
                    write!(f, "{}", args[i])?;
                    if *vargs || i < args.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                if *vargs {
                    write!(f, "@args")?;
                }
                write!(f, "| ")?;
                match body {
                    Either::Right(builtin) => write!(f, "'{builtin}"),
                    Either::Left(body) => write!(f, "{body}"),
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
                    print_exprs(f, &**args, "[", "]", ", ")
                } else {
                    write!(f, "{function}")?;
                    print_exprs(f, &**args, "(", ")", ", ")
                }
            }
            ExprKind::Select { arms } => {
                write!(f, "select {{")?;
                for (i, (lhs, rhs)) in arms.iter().enumerate() {
                    write!(f, "{lhs} => {rhs}")?;
                    if i < arms.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, "}}")
            }
            ExprKind::Eq { lhs, rhs } => write_binop(f, "=", lhs, rhs),
            ExprKind::Ne { lhs, rhs } => write_binop(f, "!=", lhs, rhs),
            ExprKind::Gt { lhs, rhs } => write_binop(f, ">", lhs, rhs),
            ExprKind::Lt { lhs, rhs } => write_binop(f, "<", lhs, rhs),
            ExprKind::Gte { lhs, rhs } => write_binop(f, ">=", lhs, rhs),
            ExprKind::Lte { lhs, rhs } => write_binop(f, "<=", lhs, rhs),
            ExprKind::And { lhs, rhs } => write_binop(f, "&&", lhs, rhs),
            ExprKind::Or { lhs, rhs } => write_binop(f, "||", lhs, rhs),
            ExprKind::Add { lhs, rhs } => write_binop(f, "+", lhs, rhs),
            ExprKind::Sub { lhs, rhs } => write_binop(f, "-", lhs, rhs),
            ExprKind::Mul { lhs, rhs } => write_binop(f, "*", lhs, rhs),
            ExprKind::Div { lhs, rhs } => write_binop(f, "/", lhs, rhs),
            ExprKind::Not { expr } => {
                write!(f, "(!{expr})")
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
