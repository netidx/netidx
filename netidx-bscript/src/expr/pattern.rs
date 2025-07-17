use std::fmt;

use anyhow::Result;
use arcstr::ArcStr;
use netidx_value::{Typ, Value};
use smallvec::{smallvec, SmallVec};
use triomphe::Arc;

use crate::{env::Env, typ::Type, Ctx, UserEvent};

use super::Expr;

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

    pub fn infer_type_predicate<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
    ) -> Result<Type> {
        match self {
            Self::Bind(_) | Self::Ignore => Ok(Type::empty_tvar()),
            Self::Literal(v) => Ok(Type::Primitive(Typ::get(v).into())),
            Self::Tuple { all: _, binds } => {
                let a = binds
                    .iter()
                    .map(|p| p.infer_type_predicate(env))
                    .collect::<Result<SmallVec<[_; 8]>>>()?;
                Ok(Type::Tuple(Arc::from_iter(a)))
            }
            Self::Variant { all: _, tag, binds } => {
                let a = binds
                    .iter()
                    .map(|p| p.infer_type_predicate(env))
                    .collect::<Result<SmallVec<[_; 8]>>>()?;
                Ok(Type::Variant(tag.clone(), Arc::from_iter(a)))
            }
            Self::Slice { all: _, binds }
            | Self::SlicePrefix { all: _, prefix: binds, tail: _ }
            | Self::SliceSuffix { all: _, head: _, suffix: binds } => {
                let t =
                    binds.iter().fold(Ok::<_, anyhow::Error>(Type::Bottom), |t, p| {
                        Ok(t?.union(env, &p.infer_type_predicate(env)?)?)
                    })?;
                Ok(Type::Array(Arc::new(t)))
            }
            Self::Struct { all: _, exhaustive: _, binds } => {
                let mut typs = binds
                    .iter()
                    .map(|(n, p)| Ok((n.clone(), p.infer_type_predicate(env)?)))
                    .collect::<Result<SmallVec<[(ArcStr, Type); 8]>>>()?;
                typs.sort_by_key(|(n, _)| n.clone());
                Ok(Type::Struct(Arc::from_iter(typs.into_iter())))
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
