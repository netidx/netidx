use arcstr::ArcStr;
use compact_str::CompactString;
use enumflags2::BitFlags;
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
    ops::Deref,
    result,
    str::FromStr,
};
use triomphe::Arc;

pub mod parser;
#[cfg(test)]
mod test;

lazy_static! {
    pub static ref VNAME: Regex = Regex::new("^[a-z][a-z0-9_]*$").unwrap();
}

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

#[derive(Debug, Clone, PartialOrd, PartialEq)]
pub enum Pattern {
    Underscore,
    Typ { tag: BitFlags<Typ>, bind: ArcStr, guard: Option<Expr> },
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub enum Type {
    Bottom,
    Primitive(BitFlags<Typ>),
    Ref(ModPath),
    Fn(Arc<FnType>),
    Set(Arc<[Type]>),
}

impl Type {
    pub fn is_bot(&self) -> bool {
        match self {
            Type::Bottom => true,
            Type::Primitive(_) | Type::Ref(_) | Type::Fn(_) | Type::Set(_) => false,
        }
    }

    pub fn contains(&self, t: &Type) -> bool {
        match (self, t) {
            (Self::Ref(_), _) | (_, Self::Ref(_)) => false, // refs must be eliminated before unification
            (Self::Bottom, _) | (_, Self::Bottom) => true,
            (Self::Primitive(p0), Self::Primitive(p1)) => p0.contains(*p1),
            (Self::Set(s), t) => s.iter().any(|s| s.contains(t)),
            (s, Self::Set(t)) => t.iter().all(|t| s.contains(t)),
            (Self::Fn(f0), Self::Fn(f1)) => f0.contains(f1),
            (Self::Fn(_), _) | (_, Self::Fn(_)) => false,
        }
    }

    fn merge_sets(s0: &[Type], s1: &[Type]) -> Type {
        let mut res: SmallVec<[Type; 20]> = smallvec![];
        for t in s0.iter().chain(s1.iter()) {
            let mut merged = false;
            for i in 0..res.len() {
                if let Some(t) = t.merge(&res[i]) {
                    res[i] = t;
                    merged = true;
                    break;
                }
            }
            if !merged {
                res.push(t.clone());
            }
        }
        Type::Set(Arc::from_iter(res))
    }

    fn merge_into_set(s: &[Type], t: &Type) -> Type {
        let mut res: SmallVec<[Type; 20]> = smallvec![];
        res.extend(s.iter().map(|t| t.clone()));
        let mut merged = false;
        for i in 0..res.len() {
            if let Some(t) = t.merge(&res[i]) {
                merged = true;
                res[i] = t;
                break;
            }
        }
        if !merged {
            res.push(t.clone());
        }
        Type::Set(Arc::from_iter(res))
    }

    fn merge(&self, t: &Type) -> Option<Type> {
        match (self, t) {
            (Type::Bottom, t) | (t, Type::Bottom) => Some(t.clone()),
            (Type::Primitive(s0), Type::Primitive(s1)) => {
                let mut s = *s0;
                s.insert(*s1);
                Some(Type::Primitive(s))
            }
            (Type::Ref(_), _) | (_, Type::Ref(_)) => None,
            (Type::Fn(f0), Type::Fn(f1)) => {
                if f0 == f1 {
                    Some(Type::Fn(f0.clone()))
                } else {
                    None
                }
            }
            (_, Type::Fn(_)) | (Type::Fn(_), _) => None,
            (Type::Set(s0), Type::Set(s1)) => Some(Self::merge_sets(s0, s1)),
            (Type::Set(s), t) | (t, Type::Set(s)) => Some(Self::merge_into_set(s, t)),
        }
    }

    pub fn union(&self, t: &Type) -> Type {
        match (self, t) {
            (Type::Bottom, t) | (t, Type::Bottom) => t.clone(),
            (Type::Primitive(s0), Type::Primitive(s1)) => {
                let mut s = *s0;
                s.insert(*s1);
                Type::Primitive(s)
            }
            (Type::Set(s0), Type::Set(s1)) => Self::merge_sets(s0, s1),
            (Type::Set(s), t) | (t, Type::Set(s)) => Self::merge_into_set(s, t),
            (r @ Type::Ref(_), t) | (t, r @ Type::Ref(_)) => {
                Type::Set(Arc::from_iter([r.clone(), t.clone()]))
            }
            (Type::Fn(f0), Type::Fn(f1)) => {
                if f0 == f1 {
                    Type::Fn(f0.clone())
                } else {
                    Type::Set(Arc::from_iter([
                        Type::Fn(f0.clone()),
                        Type::Fn(f1.clone()),
                    ]))
                }
            }
            (f @ Type::Fn(_), t) | (t, f @ Type::Fn(_)) => {
                Type::Set(Arc::from_iter([f.clone(), t.clone()]))
            }
        }
    }

    pub fn any() -> Self {
        Self::Primitive(Typ::any())
    }

    pub fn boolean() -> Self {
        Self::Primitive(Typ::Bool.into())
    }

    pub fn number() -> Self {
        Self::Primitive(Typ::number())
    }
}

impl Default for Type {
    fn default() -> Self {
        Self::Primitive(Typ::any())
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bottom => write!(f, "_"),
            Self::Ref(t) => write!(f, "{t}"),
            Self::Fn(t) => write!(f, "{t}"),
            Self::Set(s) => {
                write!(f, "[")?;
                for (i, t) in s.iter().enumerate() {
                    write!(f, "{t}")?;
                    if i < s.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, "]")
            }
            Self::Primitive(s) => {
                if s.len() == 0 {
                    write!(f, "[]")
                } else if s.len() == 1 {
                    write!(f, "{}", s.iter().next().unwrap())
                } else {
                    write!(f, "[")?;
                    for (i, t) in s.iter().enumerate() {
                        write!(f, "{t}")?;
                        if i < s.len() - 1 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, "]")
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct FnType {
    pub args: Arc<[Type]>,
    pub vargs: Type,
    pub rtype: Type,
}

impl FnType {
    pub fn contains(&self, t: &FnType) -> bool {
        self.args.len() == t.args.len()
            && t.args.iter().zip(self.args.iter()).all(|(t, s)| t.contains(s))
            && t.vargs.contains(&self.vargs)
            && self.rtype.contains(&t.rtype)
    }
}

impl fmt::Display for FnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fn(")?;
        for (i, t) in self.args.iter().enumerate() {
            write!(f, "{t}")?;
            if i < self.args.len() - 1 {
                write!(f, ", ")?;
            }
        }
        match &self.vargs {
            Type::Bottom => (),
            t => write!(f, "@args: {t}")?,
        }
        write!(f, ") -> {}", self.rtype)
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq)]
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
        name: ArcStr,
        typ: Option<Type>,
        export: bool,
        value: Arc<Expr>,
    },
    Ref {
        name: ModPath,
    },
    Connect {
        name: ModPath,
        value: Arc<Expr>,
    },
    Lambda {
        args: Arc<[(ArcStr, Option<Type>)]>,
        vargs: Option<Option<Type>>,
        rtype: Option<Type>,
        body: Either<Arc<Expr>, ArcStr>,
    },
    TypeDef {
        name: ArcStr,
        typ: Type,
    },
    TypeCast {
        expr: Arc<Expr>,
        typ: Typ,
    },
    Apply {
        args: Arc<[Expr]>,
        function: ModPath,
    },
    Select {
        arg: Arc<Expr>,
        arms: Arc<[(Pattern, Expr)]>,
    },
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
                    kill_newline!(buf);
                    writeln!(buf, "{}", sep)?
                }
            }
            push_indent(indent, buf);
            writeln!(buf, "{}", close)
        }
        let exp = |export| if export { "pub " } else { "" };
        match self {
            ExprKind::Constant(_)
            | ExprKind::Use { name: _ }
            | ExprKind::Ref { name: _ }
            | ExprKind::TypeDef { name: _, typ: _ }
            | ExprKind::Module { name: _, export: _, value: None } => {
                if newline {
                    push_indent(indent, buf);
                }
                writeln!(buf, "{self}")
            }
            ExprKind::Bind { export, name, typ, value } => {
                try_single_line!(true);
                writeln!(buf, "{}let {name}{} = ", exp(*export), typ!(typ))?;
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
            ExprKind::TypeCast { expr, typ } => {
                try_single_line!(true);
                writeln!(buf, "cast<{typ}>(")?;
                expr.kind.pretty_print(indent + 2, limit, true, buf)?;
                writeln!(buf, ")")
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
            ExprKind::Lambda { args, vargs, rtype, body } => {
                try_single_line!(true);
                write!(buf, "|")?;
                for (i, (a, typ)) in args.iter().enumerate() {
                    write!(buf, "{a}{}", typ!(typ))?;
                    if vargs.is_some() || i < args.len() - 1 {
                        write!(buf, ", ")?
                    }
                }
                if let Some(typ) = vargs {
                    write!(buf, "@args{}", typ!(typ))?;
                }
                write!(buf, "| ")?;
                if let Some(t) = rtype {
                    write!(buf, "-> {t}")?
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
                    match pat {
                        Pattern::Underscore => write!(buf, "_ ")?,
                        Pattern::Typ { tag, bind, guard } if tag.len() == 0 => {
                            write!(buf, "{bind}")?;
                            if let Some(guard) = guard {
                                write!(buf, " if ")?;
                                guard.kind.pretty_print(indent + 2, limit, false, buf)?;
                                write!(buf, " ")?;
                            }
                        }
                        Pattern::Typ { tag, bind, guard } => {
                            let len = tag.len();
                            for (i, tag) in tag.iter().enumerate() {
                                if i == 0 {
                                    write!(buf, "{tag:?}({bind})")?;
                                    if len > 1 {
                                        writeln!(buf, "")?;
                                    }
                                } else {
                                    if i < len - 1 {
                                        writeln!(buf, "| {tag:?}({bind})")?;
                                    } else {
                                        write!(buf, "| {tag:?}({bind})")?;
                                    }
                                }
                            }
                            if let Some(guard) = guard {
                                write!(buf, " if ")?;
                                guard.kind.pretty_print(indent + 2, limit, false, buf)?;
                                write!(buf, " ")?;
                            }
                        }
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
            ExprKind::Bind { export, name, typ, value } => {
                write!(f, "{}let {name}{} = {value}", exp(*export), typ!(typ))
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
            ExprKind::TypeCast { expr, typ } => write!(f, "cast<{typ}>({expr})"),
            ExprKind::TypeDef { name, typ } => write!(f, "type {name} = {typ}"),
            ExprKind::Do { exprs } => print_exprs(f, &**exprs, "{", "}", "; "),
            ExprKind::Lambda { args, vargs, rtype, body } => {
                write!(f, "|")?;
                for (i, (a, typ)) in args.iter().enumerate() {
                    write!(f, "{a}{}", typ!(typ))?;
                    if vargs.is_some() || i < args.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                if let Some(typ) = vargs {
                    write!(f, "@args{}", typ!(typ))?;
                }
                write!(f, "| ")?;
                if let Some(t) = rtype {
                    write!(f, "-> {t}")?
                }
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
            ExprKind::Select { arg, arms } => {
                write!(f, "select {arg} {{")?;
                for (i, (pat, rhs)) in arms.iter().enumerate() {
                    match pat {
                        Pattern::Underscore => write!(f, "_ => {rhs}")?,
                        Pattern::Typ { tag, bind, guard } if tag.len() == 0 => {
                            write!(f, "{bind}")?;
                            if let Some(guard) = guard {
                                write!(f, " if {guard}")?;
                            }
                            write!(f, " => {rhs}")?
                        }
                        Pattern::Typ { tag, bind, guard } => {
                            for (i, t) in tag.iter().enumerate() {
                                write!(f, "{t:?}({bind})")?;
                                if i < tag.len() - 1 {
                                    write!(f, " | ")?
                                }
                            }
                            if let Some(guard) = guard {
                                write!(f, " if {guard}")?
                            }
                            write!(f, " => {rhs}")?
                        }
                    }
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
