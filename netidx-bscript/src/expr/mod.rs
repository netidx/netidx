use crate::typ::{TVar, Type};
use anyhow::Result;
use arcstr::ArcStr;
use combine::stream::position::SourcePosition;
pub use modpath::ModPath;
use netidx::{subscriber::Value, utils::Either};
pub use pattern::{Pattern, StructurePattern};
use regex::Regex;
pub use resolver::ModuleResolver;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    cell::RefCell,
    cmp::{Ordering, PartialEq, PartialOrd},
    fmt, result,
    str::FromStr,
    sync::LazyLock,
};
use triomphe::Arc;

mod modpath;
pub mod parser;
mod pattern;
mod print;
mod resolver;
#[cfg(test)]
mod test;

pub static VNAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("^[a-z][a-z0-9_]*$").unwrap());

atomic_id!(ExprId);

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
    Connect { name: ModPath, value: Arc<Expr>, deref: bool },
    StringInterpolate { args: Arc<[Expr]> },
    StructRef { source: Arc<Expr>, field: ArcStr },
    TupleRef { source: Arc<Expr>, field: usize },
    ArrayRef { source: Arc<Expr>, i: Arc<Expr> },
    ArraySlice { source: Arc<Expr>, start: Option<Arc<Expr>>, end: Option<Arc<Expr>> },
    StructWith { source: Arc<Expr>, replace: Arc<[(ArcStr, Expr)]> },
    Lambda(Arc<Lambda>),
    TypeDef { name: ArcStr, params: Arc<[(TVar, Option<Type>)]>, typ: Type },
    TypeCast { expr: Arc<Expr>, typ: Type },
    Apply { args: Arc<[(Option<ArcStr>, Expr)]>, function: Arc<Expr> },
    Any { args: Arc<[Expr]> },
    Array { args: Arc<[Expr]> },
    Tuple { args: Arc<[Expr]> },
    Variant { tag: ArcStr, args: Arc<[Expr]> },
    Struct { args: Arc<[(ArcStr, Expr)]> },
    Select { arg: Arc<Expr>, arms: Arc<[(Pattern, Expr)]> },
    Qop(Arc<Expr>),
    ByRef(Arc<Expr>),
    Deref(Arc<Expr>),
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
    Mod { lhs: Arc<Expr>, rhs: Arc<Expr> },
    Sample { lhs: Arc<Expr>, rhs: Arc<Expr> },
}

impl ExprKind {
    pub fn to_expr(self, pos: SourcePosition) -> Expr {
        Expr { id: ExprId::new(), pos, kind: self }
    }

    /// does not provide any position information or comment
    pub fn to_expr_nopos(self) -> Expr {
        Expr { id: ExprId::new(), pos: Default::default(), kind: self }
    }
}

#[derive(Debug, Clone)]
pub struct Expr {
    pub id: ExprId,
    pub pos: SourcePosition,
    pub kind: ExprKind,
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.kind)
    }
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
            ExprKind::Qop(e)
            | ExprKind::ByRef(e)
            | ExprKind::Deref(e)
            | ExprKind::Not { expr: e } => e.fold(init, f),
            ExprKind::Add { lhs, rhs }
            | ExprKind::Sub { lhs, rhs }
            | ExprKind::Mul { lhs, rhs }
            | ExprKind::Div { lhs, rhs }
            | ExprKind::Mod { lhs, rhs }
            | ExprKind::And { lhs, rhs }
            | ExprKind::Or { lhs, rhs }
            | ExprKind::Eq { lhs, rhs }
            | ExprKind::Ne { lhs, rhs }
            | ExprKind::Gt { lhs, rhs }
            | ExprKind::Lt { lhs, rhs }
            | ExprKind::Gte { lhs, rhs }
            | ExprKind::Lte { lhs, rhs }
            | ExprKind::Sample { lhs, rhs } => {
                let init = lhs.fold(init, f);
                rhs.fold(init, f)
            }
        }
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

pub struct ErrorContext(pub Expr);

impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::fmt::Write;
        const MAX: usize = 38;
        thread_local! {
            static BUF: RefCell<String> = RefCell::new(String::new());
        }
        BUF.with_borrow_mut(|buf| {
            buf.clear();
            write!(buf, "{}", self.0).unwrap();
            if buf.len() <= MAX {
                write!(f, "at: {}, in: {buf}", self.0.pos)
            } else {
                let mut end = MAX;
                while !buf.is_char_boundary(end) {
                    end += 1
                }
                let buf = &buf[0..end];
                write!(f, "at: {}, in: {buf}..", self.0.pos)
            }
        })
    }
}
