use crate::typ::{NoRefs, Refs, TVar, Type, TypeMark};
use arcstr::ArcStr;
use compact_str::{format_compact, CompactString};
use netidx::{
    path::Path,
    publisher::Typ,
    subscriber::Value,
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
    marker::PhantomData,
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

    pub fn infer_type_predicate(&self) -> Type<NoRefs> {
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
                let t = binds.iter().fold(Type::Bottom(PhantomData), |t, p| {
                    t.union(&p.infer_type_predicate())
                });
                Type::Array(Arc::new(t))
            }
            Self::Struct { all: _, exhaustive: _, binds } => {
                let mut typs = binds
                    .iter()
                    .map(|(n, p)| (n.clone(), p.infer_type_predicate()))
                    .collect::<SmallVec<[(ArcStr, Type<NoRefs>); 8]>>();
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
    pub type_predicate: Option<Type<Refs>>,
    pub structure_predicate: StructurePattern,
    pub guard: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Arg<T: TypeMark> {
    pub labeled: Option<Option<Expr>>,
    pub pattern: StructurePattern,
    pub constraint: Option<Type<T>>,
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
        pattern: StructurePattern,
        typ: Option<Type<Refs>>,
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
        args: Arc<[Arg<Refs>]>,
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
    Any {
        args: Arc<[Expr]>,
    },
    Array {
        args: Arc<[Expr]>,
    },
    Tuple {
        args: Arc<[Expr]>,
    },
    Variant {
        tag: ArcStr,
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
            ExprKind::Bind { pattern, typ, export, value } => {
                try_single_line!(true);
                writeln!(buf, "{}let {pattern}{} = ", exp(*export), typ!(typ))?;
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
            ExprKind::Bind { pattern, typ, export, value } => {
                write!(f, "{}let {pattern}{} = {value}", exp(*export), typ!(typ))
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
