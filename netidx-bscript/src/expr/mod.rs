use crate::typ::{Refs, TVar, Type};
use arcstr::ArcStr;
use compact_str::{format_compact, CompactString};
use netidx::{
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
    sync::LazyLock,
};
use triomphe::Arc;

pub mod parser;
#[cfg(test)]
mod test;

pub static VNAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("^[a-z][a-z0-9_]*$").unwrap());

atomic_id!(ExprId);

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct ModPath(pub Path);

impl ModPath {
    pub fn root() -> ModPath {
        ModPath(Path::root())
    }
}

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

#[derive(Debug, Clone, PartialOrd, Ord)]
pub enum ValPat {
    Ignore,
    Literal(Value),
    Bind(ArcStr),
}

impl fmt::Display for ValPat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValPat::Ignore => write!(f, "_"),
            ValPat::Literal(v) => write!(f, "{v}"),
            ValPat::Bind(name) => write!(f, "{name}"),
        }
    }
}

#[cfg(not(test))]
impl PartialEq for ValPat {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Ignore, Self::Ignore) => true,
            (Self::Bind(t0), Self::Bind(t1)) => t0 == t1,
            (Self::Literal(v0), Self::Literal(v1)) => v0 == v1,
            (_, _) => false,
        }
    }
}

#[cfg(test)]
impl PartialEq for ValPat {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Ignore, Self::Ignore) => true,
            (Self::Bind(t0), Self::Bind(t1)) => t0 == t1,
            (Self::Literal(v0), Self::Literal(v1)) => v0.approx_eq(v1),
            (_, _) => false,
        }
    }
}

impl Eq for ValPat {}

impl ValPat {
    pub fn name(&self) -> Option<&ArcStr> {
        match self {
            ValPat::Ignore | ValPat::Literal(_) => None,
            ValPat::Bind(s) => Some(s),
        }
    }

    pub fn lit(&self) -> bool {
        match self {
            ValPat::Ignore | ValPat::Bind(_) => false,
            ValPat::Literal(_) => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StructurePattern {
    BindAll { name: ValPat },
    Slice { all: Option<ArcStr>, binds: Arc<[ValPat]> },
    SlicePrefix { all: Option<ArcStr>, prefix: Arc<[ValPat]>, tail: Option<ArcStr> },
    SliceSuffix { all: Option<ArcStr>, head: Option<ArcStr>, suffix: Arc<[ValPat]> },
    Tuple { all: Option<ArcStr>, binds: Arc<[ValPat]> },
    Struct { exhaustive: bool, all: Option<ArcStr>, binds: Arc<[(ArcStr, ValPat)]> },
}

impl fmt::Display for StructurePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        macro_rules! with_sep {
            ($binds:expr) => {
                for (i, b) in $binds.iter().enumerate() {
                    write!(f, "{b}")?;
                    if i < $binds.len() - 1 {
                        write!(f, ", ")?
                    }
                }
            };
        }
        match self {
            StructurePattern::BindAll { name } => write!(f, "{name}"),
            StructurePattern::Slice { all, binds } => {
                if let Some(all) = all {
                    write!(f, "{all}@ ")?
                }
                write!(f, "[")?;
                with_sep!(binds);
                write!(f, "]")
            }
            StructurePattern::SlicePrefix { all, prefix, tail } => {
                if let Some(all) = all {
                    write!(f, "{all}@ ")?
                }
                write!(f, "[")?;
                for b in prefix.iter() {
                    write!(f, "{b}, ")?
                }
                match tail {
                    None => write!(f, "..]"),
                    Some(name) => write!(f, "{name}..]"),
                }
            }
            StructurePattern::SliceSuffix { all, head, suffix } => {
                if let Some(all) = all {
                    write!(f, "{all}@ ")?
                }
                write!(f, "[")?;
                match head {
                    None => write!(f, ".., ")?,
                    Some(name) => write!(f, "{name}.., ")?,
                }
                with_sep!(suffix);
                write!(f, "]")
            }
            StructurePattern::Tuple { all, binds } => {
                if let Some(all) = all {
                    write!(f, "{all}@ ")?
                }
                write!(f, "(")?;
                with_sep!(binds);
                write!(f, ")")
            }
            StructurePattern::Struct { exhaustive, all, binds } => {
                if let Some(all) = all {
                    write!(f, "{all}@ ")?
                }
                write!(f, "{{")?;
                for (i, (name, pat)) in binds.iter().enumerate() {
                    write!(f, "{name}: {pat}")?;
                    if !exhaustive || i < binds.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                if *exhaustive {
                    write!(f, "..")?
                }
                write!(f, "}}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Pattern {
    pub type_predicate: Type<Refs>,
    pub structure_predicate: StructurePattern,
    pub guard: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Arg {
    pub labeled: Option<Option<Expr>>,
    pub name: ArcStr,
    pub constraint: Option<Type<Refs>>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ExprKind {
    Constant(Value),
    Module {
        name: ArcStr,
        export: bool,
        value: Option<Arc<[Expr]>>,
    },
    Do {
        exprs: Arc<[Expr]>,
    },
    Use {
        name: ModPath,
    },
    Bind {
        name: Option<ArcStr>,
        typ: Option<Type<Refs>>,
        export: bool,
        value: Arc<Expr>,
    },
    BindTuple {
        names: Arc<[Option<ArcStr>]>,
        typ: Option<Type<Refs>>,
        export: bool,
        value: Arc<Expr>,
    },
    BindStruct {
        names: Arc<[(ArcStr, Option<ArcStr>)]>,
        typ: Option<Type<Refs>>,
        exhaustive: bool,
        export: bool,
        value: Arc<Expr>,
    },
    Ref {
        name: ModPath,
    },
    StructRef {
        name: ModPath,
        field: ArcStr,
    },
    TupleRef {
        name: ModPath,
        field: usize,
    },
    StructWith {
        name: ModPath,
        replace: Arc<[(ArcStr, Expr)]>,
    },
    Connect {
        name: ModPath,
        value: Arc<Expr>,
    },
    Lambda {
        args: Arc<[Arg]>,
        vargs: Option<Option<Type<Refs>>>,
        rtype: Option<Type<Refs>>,
        constraints: Arc<[(TVar<Refs>, Type<Refs>)]>,
        body: Either<Arc<Expr>, ArcStr>,
    },
    TypeDef {
        name: ArcStr,
        typ: Type<Refs>,
    },
    TypeCast {
        expr: Arc<Expr>,
        typ: Type<Refs>,
    },
    Apply {
        args: Arc<[(Option<ArcStr>, Expr)]>,
        function: ModPath,
    },
    Array {
        args: Arc<[Expr]>,
    },
    Tuple {
        args: Arc<[Expr]>,
    },
    Struct {
        args: Arc<[(ArcStr, Expr)]>,
    },
    Select {
        arg: Arc<Expr>,
        arms: Arc<[(Pattern, Expr)]>,
    },
    Qop(Arc<Expr>),
    Eq {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    Ne {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    Lt {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    Gt {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    Lte {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    Gte {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    And {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    Or {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    Not {
        expr: Arc<Expr>,
    },
    Add {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    Sub {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    Mul {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
    Div {
        lhs: Arc<Expr>,
        rhs: Arc<Expr>,
    },
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
        macro_rules! kill_newline {
            ($buf:expr) => {
                if let Some('\n') = $buf.chars().next_back() {
                    $buf.pop();
                }
            };
        }
        macro_rules! try_single_line {
            ($trunc:ident) => {{
                let len = buf.len();
                let (start, indent) = if newline {
                    push_indent(indent, buf);
                    (len, indent)
                } else {
                    (buf.rfind('\n').unwrap_or(0), 0)
                };
                writeln!(buf, "{}", self)?;
                if buf.len() - start <= limit {
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
        let mut tbuf = CompactString::new("");
        macro_rules! typ {
            ($typ:expr) => {{
                match $typ {
                    None => "",
                    Some(typ) => {
                        tbuf.clear();
                        write!(tbuf, ": {typ}")?;
                        tbuf.as_str()
                    }
                }
            }};
        }
        fn push_indent(indent: usize, buf: &mut String) {
            buf.extend((0..indent).into_iter().map(|_| ' '));
        }
        fn pretty_print_exprs_int<'a, A, F: Fn(&'a A) -> &'a Expr>(
            indent: usize,
            limit: usize,
            buf: &mut String,
            exprs: &'a [A],
            open: &str,
            close: &str,
            sep: &str,
            f: F,
        ) -> fmt::Result {
            writeln!(buf, "{}", open)?;
            for i in 0..exprs.len() {
                f(&exprs[i]).kind.pretty_print(indent + 2, limit, true, buf)?;
                if i < exprs.len() - 1 {
                    kill_newline!(buf);
                    writeln!(buf, "{}", sep)?
                }
            }
            push_indent(indent, buf);
            writeln!(buf, "{}", close)
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
            pretty_print_exprs_int(indent, limit, buf, exprs, open, close, sep, |a| a)
        }
        let exp = |export| if export { "pub " } else { "" };
        match self {
            ExprKind::Constant(_)
            | ExprKind::Use { .. }
            | ExprKind::Ref { .. }
            | ExprKind::StructRef { .. }
            | ExprKind::TupleRef { .. }
            | ExprKind::TypeDef { .. }
            | ExprKind::Module { name: _, export: _, value: None } => {
                if newline {
                    push_indent(indent, buf);
                }
                writeln!(buf, "{self}")
            }
            ExprKind::Bind { name, typ, export, value } => {
                try_single_line!(true);
                match name {
                    None => writeln!(buf, "{}let _{} = ", exp(*export), typ!(typ))?,
                    Some(name) => {
                        writeln!(buf, "{}let {name}{} = ", exp(*export), typ!(typ))?
                    }
                }
                value.kind.pretty_print(indent + 2, limit, false, buf)
            }
            ExprKind::BindTuple { export, names, typ, value } => {
                try_single_line!(true);
                write!(buf, "{}let (", exp(*export))?;
                for (i, n) in names.iter().enumerate() {
                    match n {
                        None => write!(buf, "_")?,
                        Some(n) => write!(buf, "{n}")?,
                    }
                    if i < names.len() - 1 {
                        write!(buf, ", ")?
                    }
                }
                writeln!(buf, "){} = ", typ!(typ))?;
                value.kind.pretty_print(indent + 2, limit, false, buf)
            }
            ExprKind::BindStruct { names, typ, export, value } => {
                try_single_line!(true);
                write!(buf, "{}let {{", exp(*export))?;
                for (i, (name, n)) in names.iter().enumerate() {
                    match n {
                        None => write!(buf, "{name}: _")?,
                        Some(n) if n == name => write!(buf, "{n}")?,
                        Some(n) => write!(buf, "{name}: {n}")?,
                    }
                    if i < names.len() - 1 {
                        write!(buf, ", ")?
                    }
                }
                writeln!(buf, "}}{} = ", typ!(typ))?;
                value.kind.pretty_print(indent + 2, limit, false, buf)
            }
            ExprKind::StructWith { name, replace } => {
                try_single_line!(true);
                writeln!(buf, "{{ {name} with")?;
                let indent = indent + 2;
                for (i, (name, e)) in replace.iter().enumerate() {
                    push_indent(indent, buf);
                    write!(buf, "{name}: ")?;
                    e.kind.pretty_print(indent + 2, limit, false, buf)?;
                    if i < replace.len() - 1 {
                        kill_newline!(buf);
                        writeln!(buf, ",")?
                    }
                }
                writeln!(buf, "}}")
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
            ExprKind::TypeCast { expr, typ } => {
                try_single_line!(true);
                writeln!(buf, "cast<{typ}>(")?;
                expr.kind.pretty_print(indent + 2, limit, true, buf)?;
                writeln!(buf, ")")
            }
            ExprKind::Array { args } => {
                try_single_line!(true);
                pretty_print_exprs(indent, limit, buf, args, "[", "]", ",")
            }
            ExprKind::Tuple { args } => {
                try_single_line!(true);
                pretty_print_exprs(indent, limit, buf, args, "(", ")", ",")
            }
            ExprKind::Struct { args } => {
                try_single_line!(true);
                writeln!(buf, "{{")?;
                for (i, (n, e)) in args.iter().enumerate() {
                    push_indent(indent + 2, buf);
                    write!(buf, "{n}: ")?;
                    e.kind.pretty_print(indent + 2, limit, false, buf)?;
                    if i < args.len() - 1 {
                        kill_newline!(buf);
                        writeln!(buf, ", ")?
                    }
                }
                push_indent(indent, buf);
                writeln!(buf, "}}")
            }
            ExprKind::Qop(e) => {
                try_single_line!(true);
                e.kind.pretty_print(indent, limit, true, buf)?;
                kill_newline!(buf);
                writeln!(buf, "?")
            }
            ExprKind::Apply { function, args: _ }
                if function == &["str", "concat"]
                    || function == &["op", "index"]
                    || function == &["op", "slice"] =>
            {
                try_single_line!(false);
                Ok(())
            }
            ExprKind::Apply { function, args } => {
                try_single_line!(true);
                write!(buf, "{function}")?;
                writeln!(buf, "(")?;
                for i in 0..args.len() {
                    match &args[i].0 {
                        None => {
                            args[i].1.kind.pretty_print(indent + 2, limit, true, buf)?
                        }
                        Some(name) => match &args[i].1.kind {
                            ExprKind::Ref { name: n }
                                if Path::dirname(&n.0).is_none()
                                    && Path::basename(&n.0) == Some(name.as_str()) =>
                            {
                                writeln!(buf, "#{name}")?
                            }
                            _ => {
                                write!(buf, "#{name}: ")?;
                                args[i].1.kind.pretty_print(
                                    indent + 2,
                                    limit,
                                    false,
                                    buf,
                                )?
                            }
                        },
                    }
                    if i < args.len() - 1 {
                        kill_newline!(buf);
                        writeln!(buf, ",")?
                    }
                }
                writeln!(buf, ")")
            }
            ExprKind::Lambda { args, vargs, rtype, constraints, body } => {
                try_single_line!(true);
                for (i, (tvar, typ)) in constraints.iter().enumerate() {
                    write!(buf, "{tvar}: {typ}")?;
                    if i < constraints.len() - 1 {
                        write!(buf, ", ")?;
                    }
                }
                write!(buf, "|")?;
                for (i, a) in args.iter().enumerate() {
                    match &a.labeled {
                        None => {
                            write!(buf, "{}", a.name)?;
                            buf.push_str(typ!(&a.constraint));
                        }
                        Some(def) => {
                            write!(buf, "#{}", a.name)?;
                            buf.push_str(typ!(&a.constraint));
                            if let Some(def) = def {
                                write!(buf, " = {def}")?;
                            }
                        }
                    }
                    if vargs.is_some() || i < args.len() - 1 {
                        write!(buf, ", ")?
                    }
                }
                if let Some(typ) = vargs {
                    write!(buf, "@args{}", typ!(typ))?;
                }
                write!(buf, "| ")?;
                if let Some(t) = rtype {
                    write!(buf, "-> {t} ")?
                }
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
            ExprKind::Eq { lhs, rhs } => binop!("==", lhs, rhs),
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
            ExprKind::Select { arg, arms } => {
                try_single_line!(true);
                write!(buf, "select ")?;
                arg.kind.pretty_print(indent, limit, false, buf)?;
                kill_newline!(buf);
                writeln!(buf, " {{")?;
                for (i, (pat, expr)) in arms.iter().enumerate() {
                    write!(
                        buf,
                        "{} as {} ",
                        pat.type_predicate, pat.structure_predicate
                    )?;
                    if let Some(guard) = &pat.guard {
                        write!(buf, "if ")?;
                        guard.kind.pretty_print(indent + 2, limit, false, buf)?;
                        kill_newline!(buf);
                        write!(buf, " ")?;
                    }
                    write!(buf, "=> ")?;
                    if let ExprKind::Do { exprs } = &expr.kind {
                        let term = if i < arms.len() - 1 { "}," } else { "}" };
                        pretty_print_exprs(
                            indent + 2,
                            limit,
                            buf,
                            exprs,
                            "{",
                            term,
                            ";",
                        )?;
                    } else if i < arms.len() - 1 {
                        expr.kind.pretty_print(indent + 2, limit, false, buf)?;
                        kill_newline!(buf);
                        writeln!(buf, ",")?
                    } else {
                        expr.kind.pretty_print(indent, limit, false, buf)?;
                    }
                }
                push_indent(indent, buf);
                writeln!(buf, "}}")
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
        let mut tbuf = CompactString::new("");
        macro_rules! typ {
            ($typ:expr) => {{
                match $typ {
                    None => "",
                    Some(typ) => {
                        tbuf.clear();
                        write!(tbuf, ": {typ}")?;
                        tbuf.as_str()
                    }
                }
            }};
        }
        let exp = |export| if export { "pub " } else { "" };
        match self {
            ExprKind::Constant(v) => v.fmt_ext(f, &parser::BSCRIPT_ESC, true),
            ExprKind::Bind { name: Some(name), typ, export, value } => {
                write!(f, "{}let {name}{} = {value}", exp(*export), typ!(typ))
            }
            ExprKind::Bind { name: None, typ, export, value } => {
                write!(f, "{}let _{} = {value}", exp(*export), typ!(typ))
            }
            ExprKind::BindTuple { export, names, typ, value } => {
                write!(f, "{}let (", exp(*export))?;
                for (i, n) in names.iter().enumerate() {
                    match n {
                        None => write!(f, "_")?,
                        Some(n) => write!(f, "{n}")?,
                    }
                    if i < names.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, "){} = {value}", typ!(typ))
            }
            ExprKind::BindStruct { export, names, typ, value } => {
                write!(f, "{}let {{", exp(*export))?;
                for (i, (name, n)) in names.iter().enumerate() {
                    match n {
                        None => write!(f, "_")?,
                        Some(n) if n == name => write!(f, "{n}")?,
                        Some(n) => write!(f, "{name}: {n}")?,
                    }
                    if i < names.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, "}}{} = {value}", typ!(typ))
            }
            ExprKind::StructWith { name, replace } => {
                write!(f, "{{ {name} with ")?;
                for (i, (name, e)) in replace.iter().enumerate() {
                    write!(f, "{name}: {e}")?;
                    if i < replace.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, " }}")
            }
            ExprKind::Connect { name, value } => write!(f, "{name} <- {value}"),
            ExprKind::Use { name } => {
                write!(f, "use {name}")
            }
            ExprKind::Ref { name } => {
                write!(f, "{name}")
            }
            ExprKind::StructRef { name, field } => {
                write!(f, "{name}.{field}")
            }
            ExprKind::TupleRef { name, field } => {
                write!(f, "{name}.{field}")
            }
            ExprKind::Module { name, export, value } => {
                write!(f, "{}mod {name}", exp(*export))?;
                match value {
                    None => write!(f, ";"),
                    Some(exprs) => print_exprs(f, &**exprs, "{", "}", " "),
                }
            }
            ExprKind::TypeCast { expr, typ } => write!(f, "cast<{typ}>({expr})"),
            ExprKind::TypeDef { name, typ } => write!(f, "type {name} = {typ}"),
            ExprKind::Do { exprs } => print_exprs(f, &**exprs, "{", "}", "; "),
            ExprKind::Lambda { args, vargs, rtype, constraints, body } => {
                for (i, (tvar, typ)) in constraints.iter().enumerate() {
                    write!(f, "{tvar}: {typ}")?;
                    if i < constraints.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "|")?;
                for (i, a) in args.iter().enumerate() {
                    match &a.labeled {
                        None => {
                            write!(f, "{}", a.name)?;
                            write!(f, "{}", typ!(&a.constraint))?;
                        }
                        Some(def) => {
                            write!(f, "#{}", a.name)?;
                            write!(f, "{}", typ!(&a.constraint))?;
                            if let Some(def) = def {
                                write!(f, " = {def}")?;
                            }
                        }
                    }
                    if vargs.is_some() || i < args.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                if let Some(typ) = vargs {
                    write!(f, "@args{}", typ!(typ))?;
                }
                write!(f, "| ")?;
                if let Some(t) = rtype {
                    write!(f, "-> {t} ")?
                }
                match body {
                    Either::Right(builtin) => write!(f, "'{builtin}"),
                    Either::Left(body) => write!(f, "{body}"),
                }
            }
            ExprKind::Array { args } => print_exprs(f, args, "[", "]", ", "),
            ExprKind::Tuple { args } => print_exprs(f, args, "(", ")", ", "),
            ExprKind::Struct { args } => {
                write!(f, "{{ ")?;
                for (i, (n, e)) in args.iter().enumerate() {
                    write!(f, "{n}: {e}")?;
                    if i < args.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, " }}")
            }
            ExprKind::Apply { args, function }
                if function == &["str", "concat"] && args.len() > 0 =>
            {
                write!(f, "\"")?;
                for s in args.iter() {
                    match &s.1.kind {
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
            }
            ExprKind::Apply { args, function }
                if function == &["op", "index"] && args.len() == 2 =>
            {
                write!(f, "{}[{}]", &args[0].1, &args[1].1)
            }
            ExprKind::Apply { args, function }
                if function == &["op", "slice"] && args.len() == 3 =>
            {
                let s = match &args[1].1.kind {
                    ExprKind::Constant(Value::Null) => "",
                    e => &format_compact!("{e}"),
                };
                let e = match &args[2].1.kind {
                    ExprKind::Constant(Value::Null) => "",
                    e => &format_compact!("{e}"),
                };
                write!(f, "{}[{}..{}]", &args[0].1, s, e)
            }
            ExprKind::Qop(e) => write!(f, "{}?", e),
            ExprKind::Apply { args, function } => {
                write!(f, "{function}")?;
                write!(f, "(")?;
                for i in 0..args.len() {
                    match &args[i].0 {
                        None => write!(f, "{}", &args[i].1)?,
                        Some(name) => match &args[i].1.kind {
                            ExprKind::Ref { name: n }
                                if Path::dirname(&n.0).is_none()
                                    && Path::basename(&n.0) == Some(name.as_str()) =>
                            {
                                write!(f, "#{name}")?
                            }
                            _ => write!(f, "#{name}: {}", &args[i].1)?,
                        },
                    }
                    if i < args.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, ")")
            }
            ExprKind::Select { arg, arms } => {
                write!(f, "select {arg} {{")?;
                for (i, (pat, rhs)) in arms.iter().enumerate() {
                    write!(f, "{} as {} ", pat.type_predicate, pat.structure_predicate)?;
                    if let Some(guard) = &pat.guard {
                        write!(f, "if {guard} ")?;
                    }
                    write!(f, "=> {rhs}")?;
                    if i < arms.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, "}}")
            }
            ExprKind::Eq { lhs, rhs } => write_binop(f, "==", lhs, rhs),
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
        parser::parse(s)
    }
}
