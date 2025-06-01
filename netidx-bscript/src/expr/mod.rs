use crate::typ::{TVar, Type};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use combine::{stream::position::SourcePosition, EasyParser};
use compact_str::{format_compact, CompactString};
use netidx::{
    path::Path,
    publisher::Typ,
    subscriber::{Event, Subscriber, Value},
    utils::{self, Either},
};
use regex::Regex;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use smallvec::{smallvec, SmallVec};
use std::{
    borrow::Borrow,
    cmp::{Ordering, PartialEq, PartialOrd},
    fmt::{self, Display, Write},
    future::Future,
    ops::Deref,
    path::PathBuf,
    pin::Pin,
    result,
    str::FromStr,
    sync::LazyLock,
    time::Duration,
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

impl FromStr for ModPath {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        parser::modpath()
            .easy_parse(combine::stream::position::Stream::new(s))
            .map(|(r, _)| r)
            .map_err(|e| anyhow::anyhow!(format!("{e:?}")))
    }
}

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

#[derive(Debug, Clone)]
pub enum ModuleResolver {
    Files(PathBuf),
    Netidx { subscriber: Subscriber, base: Path, timeout: Option<Duration> },
}

impl ModuleResolver {
    /// Parse a comma separated list of module resolvers. Netidx
    /// resolvers are of the form, netidx:/path/in/netidx, and
    /// filesystem resolvers are of the form file:/path/in/fs
    ///
    /// This format is intended to be used in an environment variable,
    /// for example.
    pub fn parse_env(
        subscriber: Subscriber,
        timeout: Option<Duration>,
        s: &str,
    ) -> Result<Vec<ModuleResolver>> {
        let mut res = vec![];
        for l in utils::split_escaped(s, '\\', ',') {
            let l = l.trim();
            if let Some(s) = l.strip_prefix("netidx:") {
                let base = Path::from_str(s);
                let r = Self::Netidx { subscriber: subscriber.clone(), timeout, base };
                res.push(r);
            } else if let Some(s) = l.strip_prefix("file:") {
                let base = PathBuf::from_str(s)?;
                let r = Self::Files(base);
                res.push(r);
            } else {
                bail!("expected netidx: or file:")
            }
        }
        Ok(res)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StructurePattern {
    Ignore,
    Literal(Value),
    Bind(ArcStr),
    Slice {
        all: Option<ArcStr>,
        binds: Arc<[StructurePattern]>,
    },
    SlicePrefix {
        all: Option<ArcStr>,
        prefix: Arc<[StructurePattern]>,
        tail: Option<ArcStr>,
    },
    SliceSuffix {
        all: Option<ArcStr>,
        head: Option<ArcStr>,
        suffix: Arc<[StructurePattern]>,
    },
    Tuple {
        all: Option<ArcStr>,
        binds: Arc<[StructurePattern]>,
    },
    Variant {
        all: Option<ArcStr>,
        tag: ArcStr,
        binds: Arc<[StructurePattern]>,
    },
    Struct {
        exhaustive: bool,
        all: Option<ArcStr>,
        binds: Arc<[(ArcStr, StructurePattern)]>,
    },
}

impl StructurePattern {
    pub fn single_bind(&self) -> Option<&ArcStr> {
        match self {
            Self::Bind(s) => Some(s),
            Self::Ignore
            | Self::Literal(_)
            | Self::Slice { .. }
            | Self::SlicePrefix { .. }
            | Self::SliceSuffix { .. }
            | Self::Tuple { .. }
            | Self::Struct { .. }
            | Self::Variant { .. } => None,
        }
    }

    pub fn with_names<'a>(&'a self, f: &mut impl FnMut(&'a ArcStr)) {
        match self {
            Self::Bind(n) => f(n),
            Self::Ignore | Self::Literal(_) => (),
            Self::Slice { all, binds } => {
                if let Some(n) = all {
                    f(n)
                }
                for t in binds.iter() {
                    t.with_names(f)
                }
            }
            Self::SlicePrefix { all, prefix, tail } => {
                if let Some(n) = all {
                    f(n)
                }
                if let Some(n) = tail {
                    f(n)
                }
                for t in prefix.iter() {
                    t.with_names(f)
                }
            }
            Self::SliceSuffix { all, head, suffix } => {
                if let Some(n) = all {
                    f(n)
                }
                if let Some(n) = head {
                    f(n)
                }
                for t in suffix.iter() {
                    t.with_names(f)
                }
            }
            Self::Tuple { all, binds } => {
                if let Some(n) = all {
                    f(n)
                }
                for t in binds.iter() {
                    t.with_names(f)
                }
            }
            Self::Variant { all, tag: _, binds } => {
                if let Some(n) = all {
                    f(n)
                }
                for t in binds.iter() {
                    t.with_names(f)
                }
            }
            Self::Struct { exhaustive: _, all, binds } => {
                if let Some(n) = all {
                    f(n)
                }
                for (_, t) in binds.iter() {
                    t.with_names(f)
                }
            }
        }
    }

    pub fn binds_uniq(&self) -> bool {
        let mut names: SmallVec<[&ArcStr; 16]> = smallvec![];
        self.with_names(&mut |s| names.push(s));
        names.sort();
        let len = names.len();
        names.dedup();
        names.len() == len
    }

    pub fn infer_type_predicate(&self) -> Type {
        match self {
            Self::Bind(_) | Self::Ignore => Type::empty_tvar(),
            Self::Literal(v) => Type::Primitive(Typ::get(v).into()),
            Self::Tuple { all: _, binds } => {
                let a = binds.iter().map(|p| p.infer_type_predicate());
                Type::Tuple(Arc::from_iter(a))
            }
            Self::Variant { all: _, tag, binds } => {
                let a = binds.iter().map(|p| p.infer_type_predicate());
                Type::Variant(tag.clone(), Arc::from_iter(a))
            }
            Self::Slice { all: _, binds }
            | Self::SlicePrefix { all: _, prefix: binds, tail: _ }
            | Self::SliceSuffix { all: _, head: _, suffix: binds } => {
                let t = binds
                    .iter()
                    .fold(Type::Bottom, |t, p| t.union(&p.infer_type_predicate()));
                Type::Array(Arc::new(t))
            }
            Self::Struct { all: _, exhaustive: _, binds } => {
                let mut typs = binds
                    .iter()
                    .map(|(n, p)| (n.clone(), p.infer_type_predicate()))
                    .collect::<SmallVec<[(ArcStr, Type); 8]>>();
                typs.sort_by_key(|(n, _)| n.clone());
                Type::Struct(Arc::from_iter(typs.into_iter()))
            }
        }
    }
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
            StructurePattern::Ignore => write!(f, "_"),
            StructurePattern::Literal(v) => write!(f, "{v}"),
            StructurePattern::Bind(n) => write!(f, "{n}"),
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
            StructurePattern::Variant { all, tag, binds } if binds.len() == 0 => {
                if let Some(all) = all {
                    write!(f, "{all}@")?
                }
                write!(f, "`{tag}")
            }
            StructurePattern::Variant { all, tag, binds } => {
                if let Some(all) = all {
                    write!(f, "{all}@")?
                }
                write!(f, "`{tag}(")?;
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
                if !exhaustive {
                    write!(f, "..")?
                }
                write!(f, "}}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Pattern {
    pub type_predicate: Option<Type>,
    pub structure_predicate: StructurePattern,
    pub guard: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Arg {
    pub labeled: Option<Option<Expr>>,
    pub pattern: StructurePattern,
    pub constraint: Option<Type>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ModuleKind {
    Inline(Arc<[Expr]>),
    Resolved(Origin),
    Unresolved,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Bind {
    pub doc: Option<ArcStr>,
    pub pattern: StructurePattern,
    pub typ: Option<Type>,
    pub export: bool,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Lambda {
    pub args: Arc<[Arg]>,
    pub vargs: Option<Option<Type>>,
    pub rtype: Option<Type>,
    pub constraints: Arc<[(TVar, Type)]>,
    pub body: Either<Expr, ArcStr>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ExprKind {
    Constant(Value),
    Module { name: ArcStr, export: bool, value: ModuleKind },
    Do { exprs: Arc<[Expr]> },
    Use { name: ModPath },
    Bind(Arc<Bind>),
    Ref { name: ModPath },
    Connect { name: ModPath, value: Arc<Expr> },
    StringInterpolate { args: Arc<[Expr]> },
    StructRef { source: Arc<Expr>, field: ArcStr },
    TupleRef { source: Arc<Expr>, field: usize },
    ArrayRef { source: Arc<Expr>, i: Arc<Expr> },
    ArraySlice { source: Arc<Expr>, start: Option<Arc<Expr>>, end: Option<Arc<Expr>> },
    StructWith { source: Arc<Expr>, replace: Arc<[(ArcStr, Expr)]> },
    Lambda(Arc<Lambda>),
    TypeDef { name: ArcStr, typ: Type },
    TypeCast { expr: Arc<Expr>, typ: Type },
    Apply { args: Arc<[(Option<ArcStr>, Expr)]>, function: Arc<Expr> },
    Any { args: Arc<[Expr]> },
    Array { args: Arc<[Expr]> },
    Tuple { args: Arc<[Expr]> },
    Variant { tag: ArcStr, args: Arc<[Expr]> },
    Struct { args: Arc<[(ArcStr, Expr)]> },
    Select { arg: Arc<Expr>, arms: Arc<[(Pattern, Expr)]> },
    Qop(Arc<Expr>),
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
    pub fn to_expr(self, pos: SourcePosition) -> Expr {
        Expr { id: ExprId::new(), pos, kind: self }
    }

    /// does not provide any position information or comment
    pub fn to_expr_nopos(self) -> Expr {
        Expr { id: ExprId::new(), pos: Default::default(), kind: self }
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
            | ExprKind::ArrayRef { .. }
            | ExprKind::ArraySlice { .. }
            | ExprKind::StringInterpolate { .. }
            | ExprKind::Module {
                name: _,
                export: _,
                value: ModuleKind::Unresolved | ModuleKind::Resolved(_),
            } => {
                if newline {
                    push_indent(indent, buf);
                }
                writeln!(buf, "{self}")
            }
            ExprKind::Bind(b) => {
                let Bind { doc, pattern, typ, export, value } = &**b;
                try_single_line!(true);
                if let Some(doc) = doc {
                    if doc == "" {
                        writeln!(buf, "///")?;
                    } else {
                        for line in doc.lines() {
                            writeln!(buf, "///{line}")?;
                        }
                    }
                }
                writeln!(buf, "{}let {pattern}{} = ", exp(*export), typ!(typ))?;
                value.kind.pretty_print(indent + 2, limit, false, buf)
            }
            ExprKind::StructWith { source, replace } => {
                try_single_line!(true);
                match &source.kind {
                    ExprKind::Ref { .. }
                    | ExprKind::Do { .. }
                    | ExprKind::Apply { .. } => writeln!(buf, "{{ {source} with")?,
                    _ => writeln!(buf, "{{ ({source}) with")?,
                }
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
            ExprKind::Module { name, export, value: ModuleKind::Inline(exprs) } => {
                try_single_line!(true);
                write!(buf, "{}mod {name} ", exp(*export))?;
                pretty_print_exprs(indent, limit, buf, exprs, "{", "}", ";")
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
            ExprKind::Any { args } => {
                try_single_line!(true);
                write!(buf, "any")?;
                pretty_print_exprs(indent, limit, buf, args, "(", ")", ",")
            }
            ExprKind::Tuple { args } => {
                try_single_line!(true);
                pretty_print_exprs(indent, limit, buf, args, "(", ")", ",")
            }
            ExprKind::Variant { tag: _, args } if args.len() == 0 => {
                if newline {
                    push_indent(indent, buf)
                }
                write!(buf, "{self}")
            }
            ExprKind::Variant { tag, args } => {
                try_single_line!(true);
                write!(buf, "`{tag}")?;
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
            ExprKind::Apply { function, args } => {
                try_single_line!(true);
                match &function.kind {
                    ExprKind::Ref { .. } | ExprKind::Do { .. } => {
                        function.kind.pretty_print(indent, limit, true, buf)?
                    }
                    e => {
                        write!(buf, "(")?;
                        e.pretty_print(indent, limit, true, buf)?;
                        kill_newline!(buf);
                        write!(buf, ")")?;
                    }
                }
                kill_newline!(buf);
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
            ExprKind::Lambda(l) => {
                let Lambda { args, vargs, rtype, constraints, body } = &**l;
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
                            write!(buf, "{}", a.pattern)?;
                            buf.push_str(typ!(&a.constraint));
                        }
                        Some(def) => {
                            write!(buf, "#{}", a.pattern)?;
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
                    if let Some(tp) = &pat.type_predicate {
                        write!(buf, "{tp} as ")?;
                    }
                    write!(buf, "{} ", pat.structure_predicate)?;
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
            ExprKind::Bind(b) => {
                let Bind { doc, pattern, typ, export, value } = &**b;
                if let Some(doc) = doc {
                    if doc == "" {
                        writeln!(f, "///")?
                    } else {
                        for line in doc.lines() {
                            writeln!(f, "///{line}")?
                        }
                    }
                }
                write!(f, "{}let {pattern}{} = {value}", exp(*export), typ!(typ))
            }
            ExprKind::StructWith { source, replace } => {
                match &source.kind {
                    ExprKind::Ref { .. }
                    | ExprKind::Do { .. }
                    | ExprKind::Apply { .. } => write!(f, "{{ {source} with ")?,
                    _ => write!(f, "{{ ({source}) with ")?,
                }
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
            ExprKind::StructRef { source, field } => match &source.kind {
                ExprKind::Do { .. } | ExprKind::Ref { .. } | ExprKind::Apply { .. } => {
                    write!(f, "{source}.{field}")
                }
                source => write!(f, "({source}).{field}"),
            },
            ExprKind::TupleRef { source, field } => match &source.kind {
                ExprKind::Do { .. } | ExprKind::Ref { .. } | ExprKind::Apply { .. } => {
                    write!(f, "{source}.{field}")
                }
                source => write!(f, "({source}).{field}"),
            },
            ExprKind::Module { name, export, value } => {
                write!(f, "{}mod {name}", exp(*export))?;
                match value {
                    ModuleKind::Resolved(_) | ModuleKind::Unresolved => write!(f, ";"),
                    ModuleKind::Inline(exprs) => print_exprs(f, &**exprs, "{", "}", "; "),
                }
            }
            ExprKind::TypeCast { expr, typ } => write!(f, "cast<{typ}>({expr})"),
            ExprKind::TypeDef { name, typ } => write!(f, "type {name} = {typ}"),
            ExprKind::Do { exprs } => print_exprs(f, &**exprs, "{", "}", "; "),
            ExprKind::Lambda(l) => {
                let Lambda { args, vargs, rtype, constraints, body } = &**l;
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
                            write!(f, "{}", a.pattern)?;
                            write!(f, "{}", typ!(&a.constraint))?;
                        }
                        Some(def) => {
                            write!(f, "#{}", a.pattern)?;
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
            ExprKind::Any { args } => {
                write!(f, "any")?;
                print_exprs(f, args, "(", ")", ", ")
            }
            ExprKind::Tuple { args } => print_exprs(f, args, "(", ")", ", "),
            ExprKind::Variant { tag, args } if args.len() == 0 => {
                write!(f, "`{tag}")
            }
            ExprKind::Variant { tag, args } => {
                write!(f, "`{tag}")?;
                print_exprs(f, args, "(", ")", ", ")
            }
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
            ExprKind::Qop(e) => write!(f, "{}?", e),
            ExprKind::StringInterpolate { args } => {
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
            }
            ExprKind::ArrayRef { source, i } => match &source.kind {
                ExprKind::Ref { .. } | ExprKind::Do { .. } | ExprKind::Apply { .. } => {
                    write!(f, "{}[{}]", source, i)
                }
                _ => write!(f, "({})[{}]", &source, &i),
            },
            ExprKind::ArraySlice { source, start, end } => {
                let s = match start.as_ref() {
                    None => "",
                    Some(e) => &format_compact!("{e}"),
                };
                let e = match &end.as_ref() {
                    None => "",
                    Some(e) => &format_compact!("{e}"),
                };
                match &source.kind {
                    ExprKind::Ref { .. }
                    | ExprKind::Do { .. }
                    | ExprKind::Apply { .. } => {
                        write!(f, "{}[{}..{}]", source, s, e)
                    }
                    _ => write!(f, "({})[{}..{}]", source, s, e),
                }
            }
            ExprKind::Apply { args, function } => {
                match &function.kind {
                    ExprKind::Ref { name: _ } | ExprKind::Do { exprs: _ } => {
                        write!(f, "{function}")?
                    }
                    function => write!(f, "({function})")?,
                }
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
                    if let Some(tp) = &pat.type_predicate {
                        write!(f, "{tp} as ")?;
                    }
                    write!(f, "{} ", pat.structure_predicate)?;
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
    pub pos: SourcePosition,
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
        ExprKind::Constant(Value::Null).to_expr(Default::default())
    }
}

impl FromStr for Expr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        parser::parse_one(s)
    }
}

// hallowed are the ori
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Origin {
    pub name: Option<ArcStr>,
    pub source: ArcStr,
    pub exprs: Arc<[Expr]>,
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.name {
            None => write!(f, "in expr {}", self.source),
            Some(n) => {
                if n.ends_with(".bs") {
                    write!(f, "in file {n}")
                } else {
                    write!(f, "in module {n}")
                }
            }
        }
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
    pub fn new(kind: ExprKind, pos: SourcePosition) -> Self {
        Expr { id: ExprId::new(), pos, kind }
    }

    pub fn to_string_pretty(&self, col_limit: usize) -> String {
        self.kind.to_string_pretty(col_limit)
    }

    /// fold over self and all of self's sub expressions
    pub fn fold<T, F: FnMut(T, &Self) -> T>(&self, init: T, f: &mut F) -> T {
        let init = f(init, self);
        match &self.kind {
            ExprKind::Constant(_)
            | ExprKind::Use { .. }
            | ExprKind::Ref { .. }
            | ExprKind::StructRef { .. }
            | ExprKind::TupleRef { .. }
            | ExprKind::TypeDef { .. } => init,
            ExprKind::Module { value: ModuleKind::Inline(e), .. } => {
                e.iter().fold(init, |init, e| e.fold(init, f))
            }
            ExprKind::Module { value: ModuleKind::Resolved(o), .. } => {
                o.exprs.iter().fold(init, |init, e| e.fold(init, f))
            }
            ExprKind::Module { value: ModuleKind::Unresolved, .. } => init,
            ExprKind::Do { exprs } => exprs.iter().fold(init, |init, e| e.fold(init, f)),
            ExprKind::Bind(b) => b.value.fold(init, f),
            ExprKind::StructWith { replace, .. } => {
                replace.iter().fold(init, |init, (_, e)| e.fold(init, f))
            }
            ExprKind::Connect { value, .. } => value.fold(init, f),
            ExprKind::Lambda(l) => match &l.body {
                Either::Left(e) => e.fold(init, f),
                Either::Right(_) => init,
            },
            ExprKind::TypeCast { expr, .. } => expr.fold(init, f),
            ExprKind::Apply { args, function: _ } => {
                args.iter().fold(init, |init, (_, e)| e.fold(init, f))
            }
            ExprKind::Any { args }
            | ExprKind::Array { args }
            | ExprKind::Tuple { args }
            | ExprKind::Variant { args, .. }
            | ExprKind::StringInterpolate { args } => {
                args.iter().fold(init, |init, e| e.fold(init, f))
            }
            ExprKind::ArrayRef { source, i } => {
                let init = source.fold(init, f);
                i.fold(init, f)
            }
            ExprKind::ArraySlice { source, start, end } => {
                let init = source.fold(init, f);
                let init = match start {
                    None => init,
                    Some(e) => e.fold(init, f),
                };
                match end {
                    None => init,
                    Some(e) => e.fold(init, f),
                }
            }
            ExprKind::Struct { args } => {
                args.iter().fold(init, |init, (_, e)| e.fold(init, f))
            }
            ExprKind::Select { arg, arms } => {
                let init = arg.fold(init, f);
                arms.iter().fold(init, |init, (p, e)| {
                    let init = match p.guard.as_ref() {
                        None => init,
                        Some(g) => g.fold(init, f),
                    };
                    e.fold(init, f)
                })
            }
            ExprKind::Qop(e) | ExprKind::Not { expr: e } => e.fold(init, f),
            ExprKind::Add { lhs, rhs }
            | ExprKind::Sub { lhs, rhs }
            | ExprKind::Mul { lhs, rhs }
            | ExprKind::Div { lhs, rhs }
            | ExprKind::And { lhs, rhs }
            | ExprKind::Or { lhs, rhs }
            | ExprKind::Eq { lhs, rhs }
            | ExprKind::Ne { lhs, rhs }
            | ExprKind::Gt { lhs, rhs }
            | ExprKind::Lt { lhs, rhs }
            | ExprKind::Gte { lhs, rhs }
            | ExprKind::Lte { lhs, rhs } => {
                let init = lhs.fold(init, f);
                rhs.fold(init, f)
            }
        }
    }

    pub fn has_unresolved_modules(&self) -> bool {
        self.fold(false, &mut |acc, e| {
            acc || match &e.kind {
                ExprKind::Module { value: ModuleKind::Unresolved, .. } => true,
                _ => false,
            }
        })
    }

    /// Resolve external modules referenced in the expression using
    /// the resolvers list. Each resolver will be tried in order,
    /// until one succeeds. If no resolver succeeds then an error will
    /// be returned.
    pub async fn resolve_modules<'a>(
        &'a self,
        scope: &'a ModPath,
        resolvers: &'a [ModuleResolver],
    ) -> Result<Expr> {
        if self.has_unresolved_modules() {
            self.resolve_modules_inner(scope, resolvers).await
        } else {
            Ok(self.clone())
        }
    }

    fn resolve_modules_inner<'a>(
        &'a self,
        scope: &'a ModPath,
        resolvers: &'a [ModuleResolver],
    ) -> Pin<Box<dyn Future<Output = Result<Expr>> + Send + Sync + 'a>> {
        macro_rules! subexprs {
            ($args:expr) => {{
                let mut tmp = vec![];
                for e in $args.iter() {
                    tmp.push(e.resolve_modules(scope, resolvers).await?);
                }
                tmp
            }};
        }
        macro_rules! subtuples {
            ($args:expr) => {{
                let mut tmp = vec![];
                for (k, e) in $args.iter() {
                    let e = e.resolve_modules(scope, resolvers).await?;
                    tmp.push((k.clone(), e));
                }
                tmp
            }};
        }
        macro_rules! only_args {
            ($kind:ident, $args:expr) => {
                Box::pin(async move {
                    let args = Arc::from(subexprs!($args));
                    Ok(Expr {
                        id: self.id,
                        pos: self.pos,
                        kind: ExprKind::$kind { args },
                    })
                })
            };
        }
        macro_rules! bin_op {
            ($kind:ident, $lhs:expr, $rhs:expr) => {
                Box::pin(async move {
                    let lhs = Arc::from($lhs.resolve_modules(scope, resolvers).await?);
                    let rhs = Arc::from($rhs.resolve_modules(scope, resolvers).await?);
                    Ok(Expr {
                        id: self.id,
                        pos: self.pos,
                        kind: ExprKind::$kind { lhs, rhs },
                    })
                })
            };
        }
        if !self.has_unresolved_modules() {
            return Box::pin(async { Ok(self.clone()) });
        }
        match self.kind.clone() {
            ExprKind::Module { value: ModuleKind::Unresolved, export, name } => {
                Box::pin(async move {
                    let full_name = scope.append(&name);
                    let full_name = full_name.trim_start_matches(Path::SEP);
                    let mut errors = vec![];
                    for r in resolvers {
                        let (filename, s) = match r {
                            ModuleResolver::Files(base) => {
                                let full_path = base.join(full_name).with_extension("bs");
                                match tokio::fs::read_to_string(&full_path).await {
                                    Ok(s) => (
                                        ArcStr::from(full_path.to_string_lossy()),
                                        ArcStr::from(s),
                                    ),
                                    Err(e) => {
                                        errors.push(anyhow::Error::from(e));
                                        continue;
                                    }
                                }
                            }
                            ModuleResolver::Netidx { subscriber, base, timeout } => {
                                let full_path = base.append(full_name);
                                let name: ArcStr = full_path.clone().into();
                                let sub = subscriber
                                    .subscribe_nondurable_one(full_path, *timeout)
                                    .await;
                                match sub {
                                    Err(e) => {
                                        errors.push(e);
                                        continue;
                                    }
                                    Ok(v) => match v.last() {
                                        Event::Update(Value::String(s)) => (name, s),
                                        Event::Unsubscribed | Event::Update(_) => {
                                            errors.push(anyhow!("expected string"));
                                            continue;
                                        }
                                    },
                                }
                            }
                        };
                        let value =
                            ModuleKind::Resolved(parser::parse(Some(filename), s)?);
                        return Ok(Expr {
                            id: self.id,
                            pos: self.pos,
                            kind: ExprKind::Module { name, export, value },
                        });
                    }
                    bail!("module {name} could not be found {errors:?}")
                })
            }
            ExprKind::Constant(_)
            | ExprKind::Use { .. }
            | ExprKind::Ref { .. }
            | ExprKind::StructRef { .. }
            | ExprKind::TupleRef { .. }
            | ExprKind::TypeDef { .. } => Box::pin(async move { Ok(self.clone()) }),
            ExprKind::Module { value: ModuleKind::Inline(exprs), export, name } => {
                Box::pin(async move {
                    let scope = ModPath(scope.append(&name));
                    let mut tmp = vec![];
                    for e in exprs.iter() {
                        tmp.push(e.resolve_modules(&scope, resolvers).await?);
                    }
                    Ok(Expr {
                        id: self.id,
                        pos: self.pos,
                        kind: ExprKind::Module {
                            value: ModuleKind::Inline(Arc::from(tmp)),
                            name,
                            export,
                        },
                    })
                })
            }
            ExprKind::Module { value: ModuleKind::Resolved(o), export, name } => {
                Box::pin(async move {
                    let scope = ModPath(scope.append(&name));
                    let mut tmp = vec![];
                    for e in o.exprs.iter() {
                        tmp.push(e.resolve_modules(&scope, resolvers).await?);
                    }
                    Ok(Expr {
                        id: self.id,
                        pos: self.pos,
                        kind: ExprKind::Module {
                            value: ModuleKind::Resolved(Origin {
                                exprs: Arc::from(tmp),
                                ..o.clone()
                            }),
                            name,
                            export,
                        },
                    })
                })
            }
            ExprKind::Do { exprs } => Box::pin(async move {
                let exprs = Arc::from(subexprs!(exprs));
                Ok(Expr { id: self.id, pos: self.pos, kind: ExprKind::Do { exprs } })
            }),
            ExprKind::Bind(b) => Box::pin(async move {
                let Bind { doc, pattern, typ, export, value } = &*b;
                let value = value.resolve_modules(scope, resolvers).await?;
                Ok(Expr {
                    id: self.id,
                    pos: self.pos,
                    kind: ExprKind::Bind(Arc::new(Bind {
                        doc: doc.clone(),
                        pattern: pattern.clone(),
                        typ: typ.clone(),
                        export: *export,
                        value,
                    })),
                })
            }),
            ExprKind::StructWith { source, replace } => Box::pin(async move {
                Ok(Expr {
                    id: self.id,
                    pos: self.pos,
                    kind: ExprKind::StructWith {
                        source: Arc::new(source.resolve_modules(scope, resolvers).await?),
                        replace: Arc::from(subtuples!(replace)),
                    },
                })
            }),
            ExprKind::Connect { name, value } => Box::pin(async move {
                let value = value.resolve_modules(scope, resolvers).await?;
                Ok(Expr {
                    id: self.id,
                    pos: self.pos,
                    kind: ExprKind::Connect { name, value: Arc::new(value) },
                })
            }),
            ExprKind::Lambda(l) => Box::pin(async move {
                let Lambda { args, vargs, rtype, constraints, body } = &*l;
                let body = match body {
                    Either::Right(s) => Either::Right(s.clone()),
                    Either::Left(e) => {
                        Either::Left(e.resolve_modules(scope, resolvers).await?)
                    }
                };
                let l = Lambda {
                    args: args.clone(),
                    vargs: vargs.clone(),
                    rtype: rtype.clone(),
                    constraints: constraints.clone(),
                    body,
                };
                let kind = ExprKind::Lambda(Arc::new(l));
                Ok(Expr { id: self.id, pos: self.pos, kind })
            }),
            ExprKind::TypeCast { expr, typ } => Box::pin(async move {
                let expr = expr.resolve_modules(scope, resolvers).await?;
                Ok(Expr {
                    id: self.id,
                    pos: self.pos,
                    kind: ExprKind::TypeCast { expr: Arc::new(expr), typ },
                })
            }),
            ExprKind::Apply { args, function } => Box::pin(async move {
                Ok(Expr {
                    id: self.id,
                    pos: self.pos,
                    kind: ExprKind::Apply { args: Arc::from(subtuples!(args)), function },
                })
            }),
            ExprKind::Any { args } => only_args!(Any, args),
            ExprKind::Array { args } => only_args!(Array, args),
            ExprKind::Tuple { args } => only_args!(Tuple, args),
            ExprKind::StringInterpolate { args } => only_args!(StringInterpolate, args),
            ExprKind::Struct { args } => Box::pin(async move {
                let args = Arc::from(subtuples!(args));
                Ok(Expr { id: self.id, pos: self.pos, kind: ExprKind::Struct { args } })
            }),
            ExprKind::ArrayRef { source, i } => Box::pin(async move {
                let source = Arc::new(source.resolve_modules(scope, resolvers).await?);
                let i = Arc::new(i.resolve_modules(scope, resolvers).await?);
                Ok(Expr {
                    id: self.id,
                    pos: self.pos,
                    kind: ExprKind::ArrayRef { source, i },
                })
            }),
            ExprKind::ArraySlice { source, start, end } => Box::pin(async move {
                let source = Arc::new(source.resolve_modules(scope, resolvers).await?);
                let start = match start {
                    None => None,
                    Some(e) => Some(Arc::new(e.resolve_modules(scope, resolvers).await?)),
                };
                let end = match end {
                    None => None,
                    Some(e) => Some(Arc::new(e.resolve_modules(scope, resolvers).await?)),
                };
                Ok(Expr {
                    id: self.id,
                    pos: self.pos,
                    kind: ExprKind::ArraySlice { source, start, end },
                })
            }),
            ExprKind::Variant { tag, args } => Box::pin(async move {
                let args = Arc::from(subexprs!(args));
                Ok(Expr {
                    id: self.id,
                    pos: self.pos,
                    kind: ExprKind::Variant { tag, args },
                })
            }),
            ExprKind::Select { arg, arms } => Box::pin(async move {
                let arg = Arc::new(arg.resolve_modules(scope, resolvers).await?);
                let mut tmp = vec![];
                for (p, e) in arms.iter() {
                    let p = match &p.guard {
                        None => p.clone(),
                        Some(e) => {
                            let e = e.resolve_modules(scope, resolvers).await?;
                            Pattern {
                                guard: Some(e),
                                type_predicate: p.type_predicate.clone(),
                                structure_predicate: p.structure_predicate.clone(),
                            }
                        }
                    };
                    let e = e.resolve_modules(scope, resolvers).await?;
                    tmp.push((p, e));
                }
                Ok(Expr {
                    id: self.id,
                    pos: self.pos,
                    kind: ExprKind::Select { arg, arms: Arc::from(tmp) },
                })
            }),
            ExprKind::Qop(e) => Box::pin(async move {
                let e = e.resolve_modules(scope, resolvers).await?;
                Ok(Expr { id: self.id, pos: self.pos, kind: ExprKind::Qop(Arc::new(e)) })
            }),
            ExprKind::Not { expr: e } => Box::pin(async move {
                let e = e.resolve_modules(scope, resolvers).await?;
                Ok(Expr {
                    id: self.id,
                    pos: self.pos,
                    kind: ExprKind::Not { expr: Arc::new(e) },
                })
            }),
            ExprKind::Add { lhs, rhs } => bin_op!(Add, lhs, rhs),
            ExprKind::Sub { lhs, rhs } => bin_op!(Sub, lhs, rhs),
            ExprKind::Mul { lhs, rhs } => bin_op!(Mul, lhs, rhs),
            ExprKind::Div { lhs, rhs } => bin_op!(Div, lhs, rhs),
            ExprKind::And { lhs, rhs } => bin_op!(And, lhs, rhs),
            ExprKind::Or { lhs, rhs } => bin_op!(Or, lhs, rhs),
            ExprKind::Eq { lhs, rhs } => bin_op!(Eq, lhs, rhs),
            ExprKind::Ne { lhs, rhs } => bin_op!(Ne, lhs, rhs),
            ExprKind::Gt { lhs, rhs } => bin_op!(Gt, lhs, rhs),
            ExprKind::Lt { lhs, rhs } => bin_op!(Lt, lhs, rhs),
            ExprKind::Gte { lhs, rhs } => bin_op!(Gte, lhs, rhs),
            ExprKind::Lte { lhs, rhs } => bin_op!(Lte, lhs, rhs),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.kind)
    }
}
