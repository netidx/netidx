use crate::{env::Env, expr::ModPath, Ctx};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use compact_str::format_compact;
use enumflags2::BitFlags;
use fxhash::FxHashMap;
use netidx::{publisher::Typ, utils::Either};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use smallvec::{smallvec, SmallVec};
use std::{
    cell::Cell,
    cmp::{Eq, PartialEq},
    collections::hash_map::Entry,
    fmt::{self, Debug},
    hash::Hash,
    iter,
    marker::PhantomData,
    ops::Deref,
};
use triomphe::Arc;

atomic_id!(TVarId);

pub trait TypeMark: Clone + Copy + PartialOrd + Ord + PartialEq + Eq + Hash {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Refs;

impl TypeMark for Refs {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NoRefs;

impl TypeMark for NoRefs {}

#[derive(Debug)]
pub struct TVarInner<T: TypeMark> {
    pub name: ArcStr,
    typ: RwLock<Arc<RwLock<Option<Type<T>>>>>,
}

#[derive(Debug, Clone)]
pub struct TVar<T: TypeMark>(Arc<TVarInner<T>>);

thread_local! {
    static TVAR_DEREF: Cell<bool> = Cell::new(false);
}

/// For the duration of the closure F change the way type
/// variables are formatted (on this thread only) such that the
/// inferred type is also shown.
pub fn format_with_deref<R, F: FnOnce() -> R>(f: F) -> R {
    let prev = TVAR_DEREF.replace(true);
    let res = f();
    TVAR_DEREF.set(prev);
    res
}

impl<T: TypeMark> fmt::Display for TVar<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !TVAR_DEREF.get() {
            write!(f, "'{}", self.name)
        } else {
            write!(f, "'{}: ", self.name)?;
            match &*self.read().read() {
                Some(t) => write!(f, "{t}"),
                None => write!(f, "undefined"),
            }
        }
    }
}

impl<T: TypeMark> Default for TVar<T> {
    fn default() -> Self {
        Self::empty_named(ArcStr::from(
            format_compact!("default{}", TVarId::new().0).as_str(),
        ))
    }
}

impl<T: TypeMark> TVar<T> {
    pub fn empty_named(name: ArcStr) -> Self {
        Self(Arc::new(TVarInner { name, typ: RwLock::new(Arc::new(RwLock::new(None))) }))
    }

    pub fn named(name: ArcStr, typ: Type<T>) -> Self {
        Self(Arc::new(TVarInner {
            name,
            typ: RwLock::new(Arc::new(RwLock::new(Some(typ)))),
        }))
    }

    pub fn read(&self) -> RwLockReadGuard<Arc<RwLock<Option<Type<T>>>>> {
        self.typ.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<Arc<RwLock<Option<Type<T>>>>> {
        self.typ.write()
    }

    pub fn resolve_typrefs<'a, C: Ctx + 'static, E: Debug + Clone + 'static>(
        &self,
        scope: &ModPath,
        env: &Env<C, E>,
    ) -> Result<TVar<NoRefs>> {
        match Type::TVar(self.clone()).resolve_typrefs(scope, env)? {
            Type::TVar(tv) => Ok(tv),
            _ => bail!("unexpected result from resolve_typerefs"),
        }
    }
}

impl<T: TypeMark> Deref for TVar<T> {
    type Target = TVarInner<T>;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl<T: TypeMark> PartialEq for TVar<T> {
    fn eq(&self, other: &Self) -> bool {
        let t0 = self.read();
        let t1 = other.read();
        t0.as_ptr().addr() == t1.as_ptr().addr() || {
            let t0 = t0.read();
            let t1 = t1.read();
            *t0 == *t1
        }
    }
}

impl<T: TypeMark> Eq for TVar<T> {}

impl<T: TypeMark> PartialOrd for TVar<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let t0 = self.read();
        let t1 = other.read();
        if t0.as_ptr().addr() == t1.as_ptr().addr() {
            Some(std::cmp::Ordering::Equal)
        } else {
            let t0 = t0.read();
            let t1 = t1.read();
            t0.partial_cmp(&*t1)
        }
    }
}

impl<T: TypeMark> Ord for TVar<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let t0 = self.read();
        let t1 = other.read();
        if t0.as_ptr().addr() == t1.as_ptr().addr() {
            std::cmp::Ordering::Equal
        } else {
            let t0 = t0.read();
            let t1 = t1.read();
            t0.cmp(&*t1)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Type<T: TypeMark> {
    Bottom(PhantomData<T>),
    Primitive(BitFlags<Typ>),
    Ref(ModPath),
    Fn(Arc<FnType<T>>),
    Set(Arc<[Type<T>]>),
    TVar(TVar<T>),
    Array(Arc<Type<T>>),
}

impl Type<NoRefs> {
    pub fn empty_tvar() -> Self {
        Type::TVar(TVar::default())
    }

    fn iter_prims(&self) -> impl Iterator<Item = Self> {
        match self {
            Self::Primitive(p) => {
                Either::Left(p.iter().map(|t| Type::Primitive(t.into())))
            }
            t => Either::Right(iter::once(t.clone())),
        }
    }

    pub fn is_defined(&self) -> bool {
        match self {
            Self::Bottom(_)
            | Self::Primitive(_)
            | Self::Fn(_)
            | Self::Set(_)
            | Self::Array(_) => true,
            Self::TVar(tv) => tv.read().read().is_some(),
            Self::Ref(_) => unreachable!(),
        }
    }

    pub fn with_deref<R, F: FnOnce(Option<&Self>) -> R>(&self, f: F) -> R {
        match self {
            Self::Bottom(_)
            | Self::Primitive(_)
            | Self::Fn(_)
            | Self::Set(_)
            | Self::Array(_) => f(Some(self)),
            Self::TVar(tv) => f(tv.read().read().as_ref()),
            Self::Ref(_) => unreachable!(),
        }
    }

    pub fn check_contains(&self, t: &Self) -> Result<()> {
        if self.contains(t) {
            Ok(())
        } else {
            format_with_deref(|| bail!("type mismatch {self} does not contain {t}"))
        }
    }

    pub fn contains(&self, t: &Self) -> bool {
        match (self, t) {
            (Self::Bottom(_), _) | (_, Self::Bottom(_)) => true,
            (Self::Primitive(p0), Self::Primitive(p1)) => p0.contains(*p1),
            (Self::Array(t0), Self::Array(t1)) => t0.contains(t1),
            (Self::Array(_), Self::Primitive(_)) => false,
            (Self::Primitive(_), Self::Array(_)) => {
                self.contains(&Type::Primitive(Typ::Array.into()))
            }
            (Self::TVar(t0), Self::TVar(t1)) => {
                let alias = {
                    let t0 = t0.read();
                    let t1 = t1.read();
                    if t0.as_ptr().addr() == t1.as_ptr().addr() {
                        return true;
                    }
                    let t0i = t0.read();
                    let t1i = t1.read();
                    match (&*t0i, &*t1i) {
                        (Some(t0), Some(t1)) => return t0.contains(&*t1),
                        (None, None) | (Some(_), None) => Either::Right(()),
                        (None, Some(_)) => Either::Left(()),
                    }
                };
                match alias {
                    Either::Right(()) => {
                        *t1.write() = Arc::clone(&*t0.read());
                    }
                    Either::Left(()) => {
                        *t0.write() = Arc::clone(&*t1.read());
                    }
                }
                true
            }
            (Self::TVar(t0), t1) => {
                if let Some(t0) = t0.read().read().as_ref() {
                    return t0.contains(t1);
                }
                *t0.read().write() = Some(t1.clone());
                true
            }
            (t0, Self::TVar(t1)) => {
                if let Some(t1) = t1.read().read().as_ref() {
                    return t0.contains(t1);
                }
                *t1.read().write() = Some(t0.clone());
                true
            }
            (t0, Self::Set(s)) => {
                let mut ok = true;
                for t1 in s.iter() {
                    ok &= t0.contains(t1);
                }
                ok
            }
            (Self::Set(s), t) => {
                let mut ok = true;
                for t1 in t.iter_prims() {
                    let mut c = false;
                    for t0 in s.iter() {
                        c |= t0.contains(&t1);
                    }
                    ok &= c
                }
                ok
            }
            (Self::Fn(f0), Self::Fn(f1)) => f0.contains(f1),
            (Self::Fn(_), _) | (_, Self::Fn(_)) => false,
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
            (Type::Primitive(p), Type::Array(t))
            | (Type::Array(t), Type::Primitive(p)) => {
                if p.contains(Typ::Array) {
                    Type::Primitive(*p)
                } else {
                    Type::Set(Arc::from_iter([
                        Type::Primitive(*p),
                        Type::Array(t.clone()),
                    ]))
                }
            }
            (Type::Array(t0), Type::Array(t1)) => {
                if t0 == t1 {
                    Type::Array(t0.clone())
                } else {
                    Type::Set(Arc::from_iter([self.clone(), t.clone()]))
                }
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
            (t0 @ Type::TVar(_), t1 @ Type::TVar(_)) => {
                if t0 == t1 {
                    t0.clone()
                } else {
                    Type::Set(Arc::from_iter([t0.clone(), t1.clone()]))
                }
            }
            (t0 @ Type::TVar(_), t1) | (t1, t0 @ Type::TVar(_)) => {
                Type::Set(Arc::from_iter([t0.clone(), t1.clone()]))
            }
            (Type::Ref(_), _) | (_, Type::Ref(_)) => unreachable!(),
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

    /// alias unbound type variables with the same name to each other
    pub fn alias_unbound(&self, known: &mut FxHashMap<ArcStr, TVar<NoRefs>>) {
        match self {
            Type::Bottom(_) | Type::Primitive(_) => (),
            Type::Ref(_) => unreachable!(),
            Type::TVar(tv) => match known.entry(tv.name.clone()) {
                Entry::Vacant(e) => {
                    e.insert(tv.clone());
                    ()
                }
                Entry::Occupied(e) => {
                    match (&*e.get().read().read(), &*tv.read().read()) {
                        (None, None) | (Some(_), None) => (),
                        (None, Some(_)) | (Some(_), Some(_)) => return (),
                    }
                    *tv.write() = Arc::clone(&*e.get().read())
                }
            },
            Type::Fn(fntyp) => {
                let FnType { args, vargs, rtype, constraints } = &**fntyp;
                for arg in args.iter() {
                    arg.typ.alias_unbound(known)
                }
                if let Some(vargs) = vargs {
                    vargs.alias_unbound(known)
                }
                rtype.alias_unbound(known);
                for (tv, tc) in constraints.iter() {
                    Type::TVar(tv.clone()).alias_unbound(known);
                    tc.alias_unbound(known);
                }
            }
            Type::Set(s) => {
                for typ in s.iter() {
                    typ.alias_unbound(known)
                }
            }
        }
    }

    /// return a copy of self with all type variables unbound and
    /// unaliased. self will not be modified
    pub fn reset_tvars(&self) -> Type<NoRefs> {
        match self {
            Type::Bottom(_) => Type::Bottom(PhantomData),
            Type::Primitive(p) => Type::Primitive(*p),
            Type::Ref(_) => unreachable!(),
            Type::TVar(tv) => Type::TVar(TVar::empty_named(tv.name.clone())),
            Type::Set(s) => Type::Set(Arc::from_iter(s.iter().map(|t| t.reset_tvars()))),
            Type::Fn(fntyp) => {
                let FnType { args, vargs, rtype, constraints } = &**fntyp;
                let args = Arc::from_iter(args.iter().map(|a| FnArgType {
                    label: a.label.clone(),
                    typ: a.typ.reset_tvars(),
                }));
                let vargs = vargs.as_ref().map(|t| t.reset_tvars());
                let rtype = rtype.reset_tvars();
                let constraints = Arc::from_iter(constraints.iter().map(|(tv, tc)| {
                    (TVar::empty_named(tv.name.clone()), tc.reset_tvars())
                }));
                Type::Fn(Arc::new(FnType { args, vargs, rtype, constraints }))
            }
        }
    }
}

impl<T: TypeMark + Clone> Type<T> {
    pub fn is_bot(&self) -> bool {
        match self {
            Type::Bottom(_) => true,
            Type::TVar(_)
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
            (_, Type::TVar(_))
            | (Type::TVar(_), _)
            | (_, Type::Fn(_))
            | (Type::Fn(_), _) => None,
        }
    }

    pub fn resolve_typrefs<'a, C: Ctx + 'static, E: Debug + Clone + 'static>(
        &self,
        scope: &ModPath,
        env: &Env<C, E>,
    ) -> Result<Type<NoRefs>> {
        match self {
            Type::Bottom(_) => Ok(Type::Bottom(PhantomData)),
            Type::Primitive(s) => Ok(Type::Primitive(*s)),
            Type::TVar(tv) => match &*tv.read().read() {
                None => Ok(Type::TVar(TVar::empty_named(tv.name.clone()))),
                Some(typ) => {
                    let typ = typ.resolve_typrefs(scope, env)?;
                    Ok(Type::TVar(TVar::named(tv.name.clone(), typ)))
                }
            },
            Type::Ref(name) => env
                .find_visible(scope, name, |scope, name| {
                    env.typedefs
                        .get(scope)
                        .and_then(|defs| defs.get(name).map(|typ| typ.clone()))
                })
                .ok_or_else(|| anyhow!("undefined type {name} in scope {scope}")),
            Type::Set(ts) => {
                let mut res: SmallVec<[Type<NoRefs>; 20]> = smallvec![];
                for t in ts.iter() {
                    res.push(t.resolve_typrefs(scope, env)?)
                }
                Ok(Type::flatten_set(res))
            }
            Type::Fn(f) => {
                let vargs = f
                    .vargs
                    .as_ref()
                    .map(|t| t.resolve_typrefs(scope, env))
                    .transpose()?;
                let rtype = f.rtype.resolve_typrefs(scope, env)?;
                let mut res: SmallVec<[FnArgType<NoRefs>; 20]> = smallvec![];
                for a in f.args.iter() {
                    let typ = a.typ.resolve_typrefs(scope, env)?;
                    let a = FnArgType { label: a.label.clone(), typ };
                    res.push(a);
                }
                let mut cres: SmallVec<[(TVar<NoRefs>, Type<NoRefs>); 4]> = smallvec![];
                for (tv, tc) in f.constraints.iter() {
                    let tv = tv.resolve_typrefs(scope, env)?;
                    let tc = tc.resolve_typrefs(scope, env)?;
                    cres.push((tv, tc));
                }
                Ok(Type::Fn(Arc::new(FnType {
                    args: Arc::from_iter(res),
                    rtype,
                    constraints: Arc::from_iter(cres),
                    vargs,
                })))
            }
        }
    }
}

impl<T: TypeMark> fmt::Display for Type<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bottom(_) => write!(f, "_"),
            Self::Ref(t) => write!(f, "{t}"),
            Self::TVar(tv) => write!(f, "{tv}"),
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FnArgType<T: TypeMark> {
    pub label: Option<(ArcStr, bool)>,
    pub typ: Type<T>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FnType<T: TypeMark> {
    pub args: Arc<[FnArgType<T>]>,
    pub vargs: Option<Type<T>>,
    pub rtype: Type<T>,
    pub constraints: Arc<[(TVar<T>, Type<T>)]>,
}

impl<T: TypeMark + Clone> FnType<T> {
    pub fn resolve_typerefs<'a, C: Ctx + 'static, E: Debug + Clone + 'static>(
        &self,
        scope: &ModPath,
        env: &Env<C, E>,
    ) -> Result<FnType<NoRefs>> {
        let typ = Type::Fn(Arc::new(self.clone()));
        match typ.resolve_typrefs(scope, env)? {
            Type::Fn(f) => Ok((*f).clone()),
            _ => bail!("unexpected fn resolution"),
        }
    }
}

impl FnType<NoRefs> {
    pub fn contains(&self, t: &Self) -> bool {
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
                        None => return false,
                        Some(o) => {
                            if !o.typ.contains(&a.typ) {
                                return false;
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
                            return false;
                        }
                    }
                },
            }
        }
        let slen = self.args.len() - sul;
        let tlen = t.args.len() - tul;
        slen == tlen
            && t.args[tul..]
                .iter()
                .zip(self.args[sul..].iter())
                .all(|(t, s)| t.typ.contains(&s.typ))
            && match (&t.vargs, &self.vargs) {
                (Some(tv), Some(sv)) => tv.contains(sv),
                (None, None) => true,
                (_, _) => false,
            }
            && self.rtype.contains(&t.rtype)
            && self.constraints.len() == t.constraints.len()
            && self
                .constraints
                .iter()
                .zip(t.constraints.iter())
                .all(|((_, tc0), (_, tc1))| tc0.contains(tc1))
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
