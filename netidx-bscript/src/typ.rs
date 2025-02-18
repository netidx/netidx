use crate::expr::ModPath;
use anyhow::{bail, Result};
use arcstr::ArcStr;
use enumflags2::BitFlags;
use netidx::{publisher::Typ, utils::Either};
use parking_lot::Mutex;
use smallvec::{smallvec, SmallVec};
use std::{
    cmp::{Eq, PartialEq},
    fmt, iter,
    marker::PhantomData,
};
use triomphe::Arc;

pub trait TypeMark {}

#[derive(Clone, Copy)]
pub struct Refs;

impl TypeMark for Refs {}

#[derive(Clone, Copy)]
pub struct NoRefs;

impl TypeMark for NoRefs {}

#[derive(Debug, Clone)]
pub enum Type<T: TypeMark> {
    Bottom(PhantomData<T>),
    Primitive(BitFlags<Typ>),
    Ref(ModPath),
    Fn(Arc<FnType<T>>),
    Set(Arc<[Type<T>]>),
    TVar(ArcStr, Arc<Mutex<Option<Type<T>>>>),
}

impl<T: TypeMark> PartialEq for Type<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bottom(_), Self::Bottom(_)) => true,
            (Self::Primitive(p0), Self::Primitive(p1)) => p0 == p1,
            (Self::Ref(r0), Self::Ref(r1)) => r0 == r1,
            (Self::Fn(f0), Self::Fn(f1)) => f0 == f1,
            (Self::Set(s0), Self::Set(s1)) => {
                s0.len() == s1.len() && s0.iter().zip(s1.iter()).all(|(t0, t1)| t0 == t1)
            }
            (Self::TVar(_, t0), Self::TVar(_, t1)) => {
                t0.as_ptr().addr() == t1.as_ptr().addr()
            }
            (Type::Bottom(_), _)
            | (_, Type::Bottom(_))
            | (Type::Primitive(_), _)
            | (_, Type::Primitive(_))
            | (Type::Ref(_), _)
            | (_, Type::Ref(_))
            | (Type::Fn(_), _)
            | (_, Type::Fn(_))
            | (Type::Set(_), _)
            | (_, Type::Set(_)) => false,
        }
    }
}

impl<T: TypeMark> Eq for Type<T> {}

impl Type<NoRefs> {
    fn iter_prims(&self) -> impl Iterator<Item = Self> {
        match self {
            Self::Primitive(p) => {
                Either::Left(p.iter().map(|t| Type::Primitive(t.into())))
            }
            t => Either::Right(iter::once(t.clone())),
        }
    }

    pub fn contains(&self, t: &Self) -> Result<bool> {
        match (self, t) {
            (Self::Bottom(_), _) | (_, Self::Bottom(_)) => Ok(true),
            (Self::Primitive(p0), Self::Primitive(p1)) => Ok(p0.contains(*p1)),
            (Self::TVar(_, t0), Self::TVar(_, t1)) => {
                match (&mut *t0.lock(), &mut *t1.lock()) {
                    (None, None) => bail!("type must be known"),
                    (Some(t0), Some(t1)) => t0.contains(t1),
                    (t0 @ None, Some(t1)) | (Some(t1), t0 @ None) => {
                        *t0 = Some(t1.clone());
                        Ok(true)
                    }
                }
            }
            (Self::TVar(_, t0), t1) => match &mut *t0.lock() {
                Some(t0) => t0.contains(t1),
                t0 @ None => {
                    *t0 = Some(t1.clone());
                    Ok(true)
                }
            },
            (t0, Self::TVar(_, t1)) => match &mut *t1.lock() {
                Some(t1) => t0.contains(t1),
                t1 @ None => {
                    *t1 = Some(t0.clone());
                    Ok(true)
                }
            },
            (Self::Set(_), Self::Set(s1)) => {
                let mut ok = true;
                for t in s1.iter() {
                    ok &= self.contains(t)?
                }
                Ok(ok)
            }
            (Self::Set(s), t) => {
                let mut ok = true;
                for t1 in t.iter_prims() {
                    let mut c = false;
                    for t0 in s.iter() {
                        c |= t0.contains(&t1)?;
                    }
                    ok &= c
                }
                Ok(ok)
            }
            (s, Self::Set(t)) => Ok(t
                .iter()
                .map(|t| s.contains(t))
                .collect::<Result<SmallVec<[bool; 64]>>>()?
                .into_iter()
                .all(|b| b)),
            (Self::Fn(f0), Self::Fn(f1)) => f0.contains(f1),
            (Self::Fn(_), _) | (_, Self::Fn(_)) => Ok(false),
            (Self::Ref(_), _) | (_, Self::Ref(_)) => unreachable!(),
        }
    }

    pub fn union(&self, t: &Self) -> Self {
        match (self, t) {
            (Type::Bottom(_), t) | (t, Type::Bottom(_)) => t.clone(),
            (Type::Primitive(s0), Type::Primitive(s1)) => {
                let mut s = *s0;
                s.insert(*s1);
                Type::Primitive(s)
            }
            (Type::Set(s0), Type::Set(s1)) => Self::merge_sets(s0, s1),
            (Type::Set(s), t) | (t, Type::Set(s)) => Self::merge_into_set(s, t),
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
            (t0 @ Type::TVar(_, _), t1 @ Type::TVar(_, _)) => {
                if t0 == t1 {
                    t0.clone()
                } else {
                    Type::Set(Arc::from_iter([t0.clone(), t1.clone()]))
                }
            }
            (t0 @ Type::TVar(_, _), t1) | (t1, t0 @ Type::TVar(_, _)) => {
                Type::Set(Arc::from_iter([t0.clone(), t1.clone()]))
            }
            (Type::Ref(_), _) | (_, Type::Ref(_)) => unreachable!(),
        }
    }
}

impl<T: TypeMark + Clone> Type<T> {
    pub fn is_bot(&self) -> bool {
        match self {
            Type::Bottom(_) => true,
            Type::TVar(_, _)
            | Type::Primitive(_)
            | Type::Ref(_)
            | Type::Fn(_)
            | Type::Set(_) => false,
        }
    }

    pub(crate) fn flatten_set(set: impl IntoIterator<Item = Self>) -> Self {
        let init: Box<dyn Iterator<Item = Self>> = Box::new(set.into_iter());
        let mut iters: SmallVec<[Box<dyn Iterator<Item = Self>>; 16]> = smallvec![init];
        let mut acc: SmallVec<[Self; 16]> = smallvec![];
        loop {
            match iters.last_mut() {
                None => break,
                Some(iter) => match iter.next() {
                    None => {
                        iters.pop();
                    }
                    Some(Type::Set(s)) => {
                        let v: SmallVec<[Self; 16]> =
                            s.iter().map(|t| t.clone()).collect();
                        iters.push(Box::new(v.into_iter()))
                    }
                    Some(t) => {
                        let mut merged = false;
                        for i in 0..acc.len() {
                            if let Some(t) = t.merge(&acc[i]) {
                                acc[i] = t;
                                merged = true;
                                break;
                            }
                        }
                        if !merged {
                            acc.push(t);
                        }
                    }
                },
            }
        }
        match &*acc {
            [] => Type::Primitive(BitFlags::empty()),
            [t] => t.clone(),
            _ => Type::Set(Arc::from_iter(acc)),
        }
    }

    fn merge_sets(s0: &[Self], s1: &[Self]) -> Self {
        let mut res: SmallVec<[Self; 20]> = smallvec![];
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

    fn merge_into_set(s: &[Self], t: &Self) -> Self {
        let mut res: SmallVec<[Self; 20]> = smallvec![];
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

    fn merge(&self, t: &Self) -> Option<Self> {
        match (self, t) {
            (Type::Bottom(_), t) | (t, Type::Bottom(_)) => Some(t.clone()),
            (Type::Primitive(s0), Type::Primitive(s1)) => {
                let mut s = *s0;
                s.insert(*s1);
                Some(Type::Primitive(s))
            }
            (Type::Fn(_), Type::Fn(_)) => None,
            (Type::Set(s0), Type::Set(s1)) => {
                if s0.is_empty() {
                    Some(Type::Set(s1.clone()))
                } else if s1.is_empty() {
                    Some(Type::Set(s0.clone()))
                } else {
                    Some(Self::merge_sets(s0, s1))
                }
            }
            (Type::Set(s), t) | (t, Type::Set(s)) => Some(Self::merge_into_set(s, t)),
            (Type::Ref(_), _) | (_, Type::Ref(_)) => None,
            (_, Type::TVar(_, _))
            | (Type::TVar(_, _), _)
            | (_, Type::Fn(_))
            | (Type::Fn(_), _) => None,
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

impl<T: TypeMark> fmt::Display for Type<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bottom(_) => write!(f, "_"),
            Self::Ref(t) => write!(f, "{t}"),
            Self::TVar(name, _) => write!(f, "'{name}"),
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

#[derive(Debug, Clone)]
pub struct FnArgType<T: TypeMark> {
    pub label: Option<(ArcStr, bool)>,
    pub typ: Type<T>,
}

impl<T: TypeMark> PartialEq for FnArgType<T> {
    fn eq(&self, other: &Self) -> bool {
        self.label == other.label && self.typ == other.typ
    }
}

impl<T: TypeMark> Eq for FnArgType<T> {}

#[derive(Debug, Clone)]
pub struct FnType<T: TypeMark> {
    pub args: Arc<[FnArgType<T>]>,
    pub vargs: Option<Type<T>>,
    pub rtype: Type<T>,
}

impl<T: TypeMark> PartialEq for FnType<T> {
    fn eq(&self, other: &Self) -> bool {
        self.args.len() == other.args.len()
            && self.args.iter().zip(other.args.iter()).all(|(a0, a1)| a0 == a1)
            && self.vargs == other.vargs
            && self.rtype == other.rtype
    }
}

impl<T: TypeMark> Eq for FnType<T> {}

impl FnType<NoRefs> {
    pub fn contains(&self, t: &Self) -> Result<bool> {
        let mut sul = 0;
        let mut tul = 0;
        for (i, a) in self.args.iter().enumerate() {
            match &a.label {
                None => {
                    sul = i;
                    break;
                }
                Some((l, _)) => {
                    match t
                        .args
                        .iter()
                        .find(|a| a.label.as_ref().map(|a| &a.0) == Some(l))
                    {
                        None => return Ok(false),
                        Some(o) => {
                            if !o.typ.contains(&a.typ)? {
                                return Ok(false);
                            }
                        }
                    }
                }
            }
        }
        for (i, a) in t.args.iter().enumerate() {
            match &a.label {
                None => {
                    tul = i;
                    break;
                }
                Some((l, opt)) => match self
                    .args
                    .iter()
                    .find(|a| a.label.as_ref().map(|a| &a.0) == Some(l))
                {
                    Some(_) => (),
                    None => {
                        if !opt {
                            return Ok(false);
                        }
                    }
                },
            }
        }
        let slen = self.args.len() - sul;
        let tlen = t.args.len() - tul;
        Ok(slen == tlen
            && t.args[tul..]
                .iter()
                .zip(self.args[sul..].iter())
                .map(|(t, s)| t.typ.contains(&s.typ))
                .collect::<Result<SmallVec<[bool; 64]>>>()?
                .into_iter()
                .all(|b| b)
            && match (&t.vargs, &self.vargs) {
                (Some(tv), Some(sv)) => tv.contains(sv)?,
                (None, None) => true,
                (_, _) => false,
            }
            && self.rtype.contains(&t.rtype)?)
    }
}

impl<T: TypeMark> fmt::Display for FnType<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fn(")?;
        for (i, a) in self.args.iter().enumerate() {
            match &a.label {
                Some((l, true)) => write!(f, "?#{l}: ")?,
                Some((l, false)) => write!(f, "#{l}: ")?,
                None => (),
            }
            write!(f, "{}", a.typ)?;
            if i < self.args.len() - 1 || self.vargs.is_some() {
                write!(f, ", ")?;
            }
        }
        if let Some(vargs) = &self.vargs {
            write!(f, "@args: {}", vargs)?;
        }
        write!(f, ") -> {}", self.rtype)
    }
}
