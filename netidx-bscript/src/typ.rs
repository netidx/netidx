use crate::{env::Env, expr::ModPath, Ctx, UserEvent};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use compact_str::format_compact;
use enumflags2::BitFlags;
use fxhash::FxHashMap;
use netidx::{
    publisher::{Typ, Value},
    utils::Either,
};
use netidx_netproto::valarray::ValArray;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use smallvec::{smallvec, SmallVec};
use std::{
    cell::Cell,
    cmp::{Eq, Ordering, PartialEq},
    collections::hash_map::Entry,
    fmt::{self, Debug},
    hash::Hash,
    iter,
    marker::PhantomData,
    mem,
    ops::Deref,
};
use triomphe::Arc;

atomic_id!(TVarId);

pub trait TypeMark:
    Debug + Clone + Copy + PartialOrd + Ord + PartialEq + Eq + Hash + 'static
{
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Refs;

impl TypeMark for Refs {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NoRefs;

impl TypeMark for NoRefs {}

#[derive(Debug, Clone)]
pub enum TVarType<T: TypeMark> {
    Unbound,
    Bound(Type<T>),
    Permanent(Type<T>),
}

impl<T: TypeMark> PartialEq for TVarType<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Unbound, Self::Unbound) => true,
            (
                Self::Bound(t0) | Self::Permanent(t0),
                Self::Bound(t1) | Self::Permanent(t1),
            ) => t0 == t1,
            (Self::Unbound, Self::Bound(_) | Self::Permanent(_))
            | (Self::Bound(_) | Self::Permanent(_), Self::Unbound) => false,
        }
    }
}

impl<T: TypeMark> Eq for TVarType<T> {}

impl<T: TypeMark> PartialOrd for TVarType<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Unbound, Self::Unbound) => Some(Ordering::Equal),
            (
                Self::Bound(t0) | Self::Permanent(t0),
                Self::Bound(t1) | Self::Permanent(t1),
            ) => t0.partial_cmp(t1),
            (Self::Unbound, Self::Bound(_) | Self::Permanent(_)) => Some(Ordering::Less),
            (Self::Bound(_) | Self::Permanent(_), Self::Unbound) => {
                Some(Ordering::Greater)
            }
        }
    }
}

impl<T: TypeMark> Ord for TVarType<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<T: TypeMark> TVarType<T> {
    fn is_bound(&self) -> bool {
        match self {
            Self::Unbound => false,
            Self::Bound(_) | Self::Permanent(_) => true,
        }
    }

    fn is_unbound(&self) -> bool {
        !self.is_bound()
    }

    fn bound(&self) -> Option<&Type<T>> {
        match self {
            Self::Unbound => None,
            Self::Bound(t) | Self::Permanent(t) => Some(t),
        }
    }
}

impl TVarType<NoRefs> {
    pub fn unbind(&mut self) {
        match self {
            Self::Unbound => (),
            Self::Bound(t) => {
                t.unbind_tvars();
                *self = Self::Unbound
            }
            Self::Permanent(t) => t.unbind_tvars(),
        }
    }

    /// make the current bound type permanent
    pub fn freeze(&mut self) {
        match self {
            Self::Unbound | Self::Permanent(_) => (),
            Self::Bound(t) => {
                *self = Self::Permanent(mem::replace(t, Type::Bottom(PhantomData)));
            }
        }
    }
}

#[derive(Debug)]
pub struct TVarInnerInner<T: TypeMark> {
    id: TVarId,
    typ: Arc<RwLock<TVarType<T>>>,
}

#[derive(Debug)]
pub struct TVarInner<T: TypeMark> {
    pub name: ArcStr,
    typ: RwLock<TVarInnerInner<T>>,
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
            match &*self.read().typ.read() {
                TVarType::Bound(t) => write!(f, "{t}"),
                TVarType::Permanent(t) => write!(f, "!{t}"),
                TVarType::Unbound => write!(f, "unbound"),
            }
        }
    }
}

impl<T: TypeMark> Default for TVar<T> {
    fn default() -> Self {
        Self::empty_named(ArcStr::from(format_compact!("_{}", TVarId::new().0).as_str()))
    }
}

impl TVar<Refs> {
    pub fn resolve_typerefs<'a, C: Ctx, E: UserEvent>(
        &self,
        scope: &ModPath,
        env: &Env<C, E>,
    ) -> Result<TVar<NoRefs>> {
        match Type::TVar(self.clone()).resolve_typerefs(scope, env)? {
            Type::TVar(tv) => Ok(tv),
            _ => bail!("unexpected result from resolve_typerefs"),
        }
    }
}

impl<T: TypeMark> TVar<T> {
    pub fn empty_named(name: ArcStr) -> Self {
        Self(Arc::new(TVarInner {
            name,
            typ: RwLock::new(TVarInnerInner {
                id: TVarId::new(),
                typ: Arc::new(RwLock::new(TVarType::Unbound)),
            }),
        }))
    }

    pub fn named(name: ArcStr, typ: Type<T>) -> Self {
        Self(Arc::new(TVarInner {
            name,
            typ: RwLock::new(TVarInnerInner {
                id: TVarId::new(),
                typ: Arc::new(RwLock::new(TVarType::Bound(typ))),
            }),
        }))
    }

    pub fn read(&self) -> RwLockReadGuard<TVarInnerInner<T>> {
        self.typ.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<TVarInnerInner<T>> {
        self.typ.write()
    }

    /// make self an alias for other
    pub fn alias(&self, other: &Self) {
        let mut s = self.write();
        let o = other.read();
        s.id = o.id;
        s.typ = Arc::clone(&o.typ);
    }

    pub fn normalize(&self) -> Self {
        match &mut *self.read().typ.write() {
            TVarType::Unbound => (),
            TVarType::Bound(t) | TVarType::Permanent(t) => {
                *t = t.normalize();
            }
        }
        self.clone()
    }
}

impl TVar<NoRefs> {
    pub fn unbind(&self) {
        self.read().typ.write().unbind()
    }

    pub fn freeze(&self) {
        self.read().typ.write().freeze()
    }
}

fn would_cycle_inner(addr: usize, t: &Type<NoRefs>) -> bool {
    match t {
        Type::Primitive(_) | Type::Bottom(_) | Type::Ref(_) => false,
        Type::TVar(t) => {
            Arc::as_ptr(&t.read().typ).addr() == addr
                || match &*t.read().typ.read() {
                    TVarType::Unbound => false,
                    TVarType::Bound(t) | TVarType::Permanent(t) => {
                        would_cycle_inner(addr, t)
                    }
                }
        }
        Type::Array(a) => would_cycle_inner(addr, &**a),
        Type::Tuple(ts) => ts.iter().any(|t| would_cycle_inner(addr, t)),
        Type::Variant(_, ts) => ts.iter().any(|t| would_cycle_inner(addr, t)),
        Type::Struct(ts) => ts.iter().any(|(_, t)| would_cycle_inner(addr, t)),
        Type::Set(s) => s.iter().any(|t| would_cycle_inner(addr, t)),
        Type::Fn(f) => {
            let FnType { args, vargs, rtype, constraints } = &**f;
            args.iter().any(|t| would_cycle_inner(addr, &t.typ))
                || match vargs {
                    None => false,
                    Some(t) => would_cycle_inner(addr, t),
                }
                || would_cycle_inner(addr, &rtype)
                || constraints.iter().any(|a| {
                    Arc::as_ptr(&a.0.read().typ).addr() == addr
                        || would_cycle_inner(addr, &a.1)
                })
        }
    }
}

impl TVar<NoRefs> {
    fn would_cycle(&self, t: &Type<NoRefs>) -> bool {
        let addr = Arc::as_ptr(&self.read().typ).addr();
        would_cycle_inner(addr, t)
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
        t0.typ.as_ptr().addr() == t1.typ.as_ptr().addr() || {
            let t0 = t0.typ.read();
            let t1 = t1.typ.read();
            *t0 == *t1
        }
    }
}

impl<T: TypeMark> Eq for TVar<T> {}

impl<T: TypeMark> PartialOrd for TVar<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let t0 = self.read();
        let t1 = other.read();
        if t0.typ.as_ptr().addr() == t1.typ.as_ptr().addr() {
            Some(std::cmp::Ordering::Equal)
        } else {
            let t0 = t0.typ.read();
            let t1 = t1.typ.read();
            t0.partial_cmp(&*t1)
        }
    }
}

impl<T: TypeMark> Ord for TVar<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let t0 = self.read();
        let t1 = other.read();
        if t0.typ.as_ptr().addr() == t1.typ.as_ptr().addr() {
            std::cmp::Ordering::Equal
        } else {
            let t0 = t0.typ.read();
            let t1 = t1.typ.read();
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
    Tuple(Arc<[Type<T>]>),
    Struct(Arc<[(ArcStr, Type<T>)]>),
    Variant(ArcStr, Arc<[Type<T>]>),
}

impl<T: TypeMark> Default for Type<T> {
    fn default() -> Self {
        Self::Bottom(PhantomData)
    }
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
            | Self::Array(_)
            | Self::Tuple(_)
            | Self::Struct(_)
            | Self::Variant(_, _) => true,
            Self::TVar(tv) => tv.read().typ.read().is_bound(),
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
            (Self::TVar(t0), Self::Bottom(_)) => {
                if let Some(_) = t0.read().typ.read().bound() {
                    return true;
                }
                *t0.read().typ.write() = TVarType::Bound(Self::Bottom(PhantomData));
                true
            }
            (Self::Bottom(_), _) | (_, Self::Bottom(_)) => true,
            (Self::Primitive(p0), Self::Primitive(p1)) => p0.contains(*p1),
            (
                Self::Primitive(p),
                Self::Array(_) | Self::Tuple(_) | Self::Struct(_) | Self::Variant(_, _),
            ) => p.contains(Typ::Array),
            (Self::Array(t0), Self::Array(t1)) => t0.contains(t1),
            (Self::Array(t0), Self::Primitive(p)) if *p == BitFlags::from(Typ::Array) => {
                t0.contains(&Type::Primitive(BitFlags::all()))
            }
            (Self::Tuple(t0), Self::Tuple(t1)) => {
                t0.len() == t1.len()
                    && t0.iter().zip(t1.iter()).all(|(t0, t1)| t0.contains(t1))
            }
            (Self::Struct(t0), Self::Struct(t1)) => {
                t0.len() == t1.len() && {
                    // struct types are always sorted by field name
                    t0.iter()
                        .zip(t1.iter())
                        .all(|((n0, t0), (n1, t1))| n0 == n1 && t0.contains(t1))
                }
            }
            (Self::Variant(tg0, t0), Self::Variant(tg1, t1)) => {
                tg0 == tg1
                    && t0.len() == t1.len()
                    && t0.iter().zip(t1.iter()).all(|(t0, t1)| t0.contains(t1))
            }
            (Self::Tuple(_), Self::Array(_))
            | (Self::Tuple(_), Self::Primitive(_))
            | (Self::Tuple(_), Self::Struct(_))
            | (Self::Tuple(_), Self::Variant(_, _))
            | (Self::Array(_), Self::Primitive(_))
            | (Self::Array(_), Self::Tuple(_))
            | (Self::Array(_), Self::Struct(_))
            | (Self::Array(_), Self::Variant(_, _))
            | (Self::Struct(_), Self::Primitive(_))
            | (Self::Struct(_), Self::Array(_))
            | (Self::Struct(_), Self::Tuple(_))
            | (Self::Struct(_), Self::Variant(_, _))
            | (Self::Variant(_, _), Self::Array(_))
            | (Self::Variant(_, _), Self::Struct(_))
            | (Self::Variant(_, _), Self::Primitive(_))
            | (Self::Variant(_, _), Self::Tuple(_)) => false,
            (Self::TVar(t0), tt1 @ Self::TVar(t1)) => {
                let alias = {
                    let t0 = t0.read();
                    let t1 = t1.read();
                    let addr = Arc::as_ptr(&t0.typ).addr();
                    if addr == Arc::as_ptr(&t1.typ).addr() {
                        return true;
                    }
                    let t0i = t0.typ.read();
                    let t1i = t1.typ.read();
                    match (t0i.bound(), t1i.bound()) {
                        (Some(t0), Some(t1)) => return t0.contains(&*t1),
                        (None, None) | (Some(_), None) => {
                            if would_cycle_inner(addr, tt1) {
                                return false;
                            }
                            Either::Right(())
                        }
                        (None, Some(_)) => {
                            if would_cycle_inner(addr, tt1) {
                                return false;
                            }
                            Either::Left(())
                        }
                    }
                };
                match alias {
                    Either::Right(()) => t1.alias(t0),
                    Either::Left(()) => t0.alias(t1),
                }
                true
            }
            (Self::TVar(t0), t1) if !t0.would_cycle(t1) => {
                if let Some(t0) = t0.read().typ.read().bound() {
                    return t0.contains(t1);
                }
                *t0.read().typ.write() = TVarType::Bound(t1.clone());
                true
            }
            (t0, Self::TVar(t1)) if !t1.would_cycle(t0) => {
                if let Some(t1) = t1.read().typ.read().bound() {
                    return t0.contains(t1);
                }
                *t1.read().typ.write() = TVarType::Bound(t0.clone());
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
                s.iter().fold(false, |acc, t0| acc || t0.contains(t))
                    || t.iter_prims().fold(true, |acc, t1| {
                        acc && s.iter().fold(false, |acc, t0| acc || t0.contains(&t1))
                    })
            }
            (Self::Fn(f0), Self::Fn(f1)) => f0.as_ptr() == f1.as_ptr() || f0.contains(f1),
            (_, Self::TVar(_))
            | (Self::TVar(_), _)
            | (Self::Fn(_), _)
            | (_, Self::Fn(_)) => false,
            (Self::Ref(_), _) | (_, Self::Ref(_)) => unreachable!(),
        }
    }

    fn union_int(&self, t: &Self) -> Self {
        match (self, t) {
            (Type::Bottom(_), t) | (t, Type::Bottom(_)) => t.clone(),
            (Type::Primitive(p), t) | (t, Type::Primitive(p)) if p.is_empty() => {
                t.clone()
            }
            (Type::Primitive(s0), Type::Primitive(s1)) => {
                let mut s = *s0;
                s.insert(*s1);
                Type::Primitive(s)
            }
            (
                Type::Primitive(p),
                Type::Array(_) | Type::Struct(_) | Type::Tuple(_) | Type::Variant(_, _),
            )
            | (
                Type::Array(_) | Type::Struct(_) | Type::Tuple(_) | Type::Variant(_, _),
                Type::Primitive(p),
            ) if p.contains(Typ::Array) => Type::Primitive(*p),
            (Type::Primitive(p), Type::Array(t))
            | (Type::Array(t), Type::Primitive(p)) => {
                Type::Set(Arc::from_iter([Type::Primitive(*p), Type::Array(t.clone())]))
            }
            (Type::Array(t0), Type::Array(t1)) => {
                if t0 == t1 {
                    Type::Array(t0.clone())
                } else {
                    Type::Set(Arc::from_iter([self.clone(), t.clone()]))
                }
            }
            (Type::Set(s0), Type::Set(s1)) => {
                Type::Set(Arc::from_iter(s0.iter().cloned().chain(s1.iter().cloned())))
            }
            (Type::Set(s), t) | (t, Type::Set(s)) => {
                Type::Set(Arc::from_iter(s.iter().cloned().chain(iter::once(t.clone()))))
            }
            (Type::Struct(t0), Type::Struct(t1)) => {
                if t0.len() == t1.len() && t0 == t1 {
                    self.clone()
                } else {
                    Type::Set(Arc::from_iter([self.clone(), t.clone()]))
                }
            }
            (Type::Struct(_), t) | (t, Type::Struct(_)) => {
                Type::Set(Arc::from_iter([self.clone(), t.clone()]))
            }
            (Type::Tuple(t0), Type::Tuple(t1)) => {
                if t0 == t1 {
                    self.clone()
                } else {
                    Type::Set(Arc::from_iter([self.clone(), t.clone()]))
                }
            }
            (Type::Tuple(_), t) | (t, Type::Tuple(_)) => {
                Type::Set(Arc::from_iter([self.clone(), t.clone()]))
            }
            (Type::Variant(tg0, t0), Type::Variant(tg1, t1)) => {
                if tg0 == tg1 && t0.len() == t1.len() {
                    let typs = t0.iter().zip(t1.iter()).map(|(t0, t1)| t0.union_int(t1));
                    Type::Variant(tg0.clone(), Arc::from_iter(typs))
                } else {
                    Type::Set(Arc::from_iter([self.clone(), t.clone()]))
                }
            }
            (Type::Variant(_, _), t) | (t, Type::Variant(_, _)) => {
                Type::Set(Arc::from_iter([self.clone(), t.clone()]))
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

    pub fn union(&self, t: &Self) -> Self {
        self.union_int(t).normalize()
    }

    fn diff_int(&self, t: &Self) -> Self {
        match (self, t) {
            (Type::Bottom(_), t) | (t, Type::Bottom(_)) => t.clone(),
            (Type::Primitive(s0), Type::Primitive(s1)) => {
                let mut s = *s0;
                s.remove(*s1);
                Type::Primitive(s)
            }
            (
                Type::Primitive(p),
                Type::Array(_) | Type::Struct(_) | Type::Tuple(_) | Type::Variant(_, _),
            ) => {
                // CR estokes: is this correct? It's a bit odd.
                let mut s = *p;
                s.remove(Typ::Array);
                Type::Primitive(s)
            }
            (
                Type::Array(_) | Type::Struct(_) | Type::Tuple(_) | Type::Variant(_, _),
                Type::Primitive(p),
            ) => {
                if p.contains(Typ::Array) {
                    Type::Primitive(BitFlags::empty())
                } else {
                    self.clone()
                }
            }
            (Type::Array(t0), Type::Array(t1)) => Type::Array(Arc::new(t0.diff_int(t1))),
            (Type::Set(s0), Type::Set(s1)) => {
                let mut s: SmallVec<[Type<NoRefs>; 4]> = smallvec![];
                for i in 0..s0.len() {
                    s.push(s0[i].clone());
                    for j in 0..s1.len() {
                        s[i] = s[i].diff_int(&s1[j])
                    }
                }
                Self::flatten_set(s.into_iter())
            }
            (Type::Set(s), t) => Self::flatten_set(s.iter().map(|s| s.diff_int(t))),
            (t, Type::Set(s)) => {
                let mut t = t.clone();
                for st in s.iter() {
                    t = t.diff_int(st);
                }
                t
            }
            (Type::Tuple(t0), Type::Tuple(t1)) => {
                if t0 == t1 {
                    Type::Primitive(BitFlags::empty())
                } else {
                    self.clone()
                }
            }
            (Type::Tuple(_), _) | (_, Type::Tuple(_)) => self.clone(),
            (Type::Struct(t0), Type::Struct(t1)) => {
                if t0.len() == t1.len() && t0 == t1 {
                    Type::Primitive(BitFlags::empty())
                } else {
                    self.clone()
                }
            }
            (Type::Struct(_), _) | (_, Type::Struct(_)) => self.clone(),
            (Type::Variant(tg0, t0), Type::Variant(tg1, t1)) => {
                if tg0 == tg1 && t0.len() == t1.len() && t0 == t1 {
                    Type::Primitive(BitFlags::empty())
                } else {
                    self.clone()
                }
            }
            (Type::Variant(_, _), _) | (_, Type::Variant(_, _)) => self.clone(),
            (Type::Fn(f0), Type::Fn(f1)) => {
                if f0 == f1 {
                    Type::Primitive(BitFlags::empty())
                } else {
                    Type::Fn(f0.clone())
                }
            }
            (f @ Type::Fn(_), _) => f.clone(),
            (t, Type::Fn(_)) => t.clone(),
            (Type::TVar(tv0), Type::TVar(tv1)) => {
                if tv0.read().typ.as_ptr() == tv1.read().typ.as_ptr() {
                    return Type::Primitive(BitFlags::empty());
                }
                match (tv0.read().typ.read().bound(), tv1.read().typ.read().bound()) {
                    (None, _) | (_, None) => Type::TVar(tv0.clone()),
                    (Some(t0), Some(t1)) => t0.diff_int(t1),
                }
            }
            (Type::TVar(tv), t1) => match tv.read().typ.read().bound() {
                None => Type::TVar(tv.clone()),
                Some(t0) => t0.diff_int(t1),
            },
            (t0, Type::TVar(tv)) => match tv.read().typ.read().bound() {
                None => t0.clone(),
                Some(t1) => t0.diff_int(t1),
            },
            (Type::Ref(_), _) | (_, Type::Ref(_)) => unreachable!(),
        }
    }

    pub fn diff(&self, t: &Self) -> Self {
        self.diff_int(t).normalize()
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

    pub fn int() -> Self {
        Self::Primitive(Typ::integer())
    }

    pub fn uint() -> Self {
        Self::Primitive(Typ::unsigned_integer())
    }

    /// alias type variables with the same name to each other
    pub fn alias_tvars(&self, known: &mut FxHashMap<ArcStr, TVar<NoRefs>>) {
        match self {
            Type::Bottom(_) | Type::Primitive(_) => (),
            Type::Ref(_) => unreachable!(),
            Type::Array(t) => t.alias_tvars(known),
            Type::Tuple(ts) => {
                for t in ts.iter() {
                    t.alias_tvars(known)
                }
            }
            Type::Struct(ts) => {
                for (_, t) in ts.iter() {
                    t.alias_tvars(known)
                }
            }
            Type::Variant(_, ts) => {
                for t in ts.iter() {
                    t.alias_tvars(known)
                }
            }
            Type::TVar(tv) => match known.entry(tv.name.clone()) {
                Entry::Occupied(e) => tv.alias(e.get()),
                Entry::Vacant(e) => {
                    e.insert(tv.clone());
                    ()
                }
            },
            Type::Fn(ft) => ft.alias_tvars(known),
            Type::Set(s) => {
                for typ in s.iter() {
                    typ.alias_tvars(known)
                }
            }
        }
    }

    pub fn has_unbound(&self) -> bool {
        match self {
            Type::Bottom(_) | Type::Primitive(_) => false,
            Type::Ref(_) => unreachable!(),
            Type::Array(t0) => t0.has_unbound(),
            Type::Tuple(ts) => ts.iter().any(|t| t.has_unbound()),
            Type::Struct(ts) => ts.iter().any(|(_, t)| t.has_unbound()),
            Type::Variant(_, ts) => ts.iter().any(|t| t.has_unbound()),
            Type::TVar(tv) => tv.read().typ.read().is_bound(),
            Type::Set(s) => s.iter().any(|t| t.has_unbound()),
            Type::Fn(ft) => ft.has_unbound(),
        }
    }

    /// return a copy of self with all type variables unbound and
    /// unaliased. self will not be modified
    pub fn reset_tvars(&self) -> Type<NoRefs> {
        match self {
            Type::Bottom(_) => Type::Bottom(PhantomData),
            Type::Primitive(p) => Type::Primitive(*p),
            Type::Ref(_) => unreachable!(),
            Type::Array(t0) => Type::Array(Arc::new(t0.reset_tvars())),
            Type::Tuple(ts) => {
                Type::Tuple(Arc::from_iter(ts.iter().map(|t| t.reset_tvars())))
            }
            Type::Struct(ts) => Type::Struct(Arc::from_iter(
                ts.iter().map(|(n, t)| (n.clone(), t.reset_tvars())),
            )),
            Type::Variant(tag, ts) => Type::Variant(
                tag.clone(),
                Arc::from_iter(ts.iter().map(|t| t.reset_tvars())),
            ),
            Type::TVar(tv) => Type::TVar(TVar::empty_named(tv.name.clone())),
            Type::Set(s) => Type::Set(Arc::from_iter(s.iter().map(|t| t.reset_tvars()))),
            Type::Fn(fntyp) => Type::Fn(Arc::new(fntyp.reset_tvars())),
        }
    }

    /// Unbind any bound tvars, but do not unalias them.
    fn unbind_tvars(&self) {
        match self {
            Type::Bottom(_) | Type::Primitive(_) => (),
            Type::Array(t0) => t0.unbind_tvars(),
            Type::Tuple(ts) | Type::Variant(_, ts) | Type::Set(ts) => {
                for t in ts.iter() {
                    t.unbind_tvars()
                }
            }
            Type::Struct(ts) => {
                for (_, t) in ts.iter() {
                    t.unbind_tvars()
                }
            }
            Type::TVar(tv) => tv.unbind(),
            Type::Fn(fntyp) => fntyp.unbind_tvars(),
            Type::Ref(_) => unreachable!(),
        }
    }

    /// Freeze any bound tvars, such that unbind_tvars will no longer
    /// erase their type.
    fn freeze(&self) {
        match self {
            Type::Bottom(_) | Type::Primitive(_) => (),
            Type::Array(t0) => t0.freeze(),
            Type::Tuple(ts) | Type::Variant(_, ts) | Type::Set(ts) => {
                for t in ts.iter() {
                    t.freeze()
                }
            }
            Type::Struct(ts) => {
                for (_, t) in ts.iter() {
                    t.freeze()
                }
            }
            Type::TVar(tv) => tv.freeze(),
            Type::Fn(fntyp) => fntyp.freeze(),
            Type::Ref(_) => unreachable!(),
        }
    }

    fn first_prim(&self) -> Option<Typ> {
        match self {
            Type::Primitive(p) => p.iter().next(),
            Type::Bottom(_) => None,
            Type::Ref(_) => unreachable!(),
            Type::Fn(_) => None,
            Type::Set(s) => s.iter().find_map(|t| t.first_prim()),
            Type::TVar(tv) => tv.read().typ.read().bound().and_then(|t| t.first_prim()),
            // array, tuple, and struct casting are handled directly
            Type::Array(_) | Type::Tuple(_) | Type::Struct(_) | Type::Variant(_, _) => {
                None
            }
        }
    }

    pub fn check_cast(&self) -> Result<()> {
        match self {
            Type::Primitive(_) => Ok(()),
            Type::Fn(_) => bail!("can't cast a value to a function"),
            Type::Ref(_) => unreachable!(),
            Type::Bottom(_) => bail!("can't cast a value to bottom"),
            Type::Set(s) => Ok(for t in s.iter() {
                t.check_cast()?
            }),
            Type::TVar(tv) => match tv.read().typ.read().bound() {
                Some(t) => t.check_cast(),
                None => bail!("can't cast a value to a free type variable"),
            },
            Type::Array(et) => et.check_cast(),
            Type::Tuple(ts) => Ok(for t in ts.iter() {
                t.check_cast()?
            }),
            Type::Struct(ts) => Ok(for (_, t) in ts.iter() {
                t.check_cast()?
            }),
            Type::Variant(_, ts) => Ok(for t in ts.iter() {
                t.check_cast()?
            }),
        }
    }

    fn check_array(&self, a: &ValArray) -> bool {
        a.iter().all(|elt| match elt {
            Value::Array(elts) => match self {
                Type::Array(et) => et.check_array(elts),
                _ => false,
            },
            v => self.contains(&Type::Primitive(Typ::get(v).into())),
        })
    }

    fn cast_value_int(&self, v: Value) -> Result<Value> {
        if self.contains(&Type::Primitive(Typ::get(&v).into())) {
            return Ok(v);
        }
        match self {
            Type::Array(et) => match v {
                Value::Array(elts) => {
                    if et.check_array(&elts) {
                        return Ok(Value::Array(elts));
                    }
                    let va = elts
                        .iter()
                        .map(|el| et.cast_value_int(el.clone()))
                        .collect::<Result<SmallVec<[Value; 8]>>>()?;
                    Ok(Value::Array(ValArray::from_iter_exact(va.into_iter())))
                }
                v => Ok(Value::Array([et.cast_value_int(v)?].into())),
            },
            Type::Tuple(ts) => match v {
                Value::Array(elts) => {
                    if elts.len() != ts.len() {
                        bail!("tuple size mismatch {self} with {}", Value::Array(elts))
                    }
                    let ok = ts
                        .iter()
                        .zip(elts.iter())
                        .all(|(t, v)| t.contains(&Type::Primitive(Typ::get(v).into())));
                    if ok {
                        return Ok(Value::Array(elts));
                    }
                    let a = ts
                        .iter()
                        .zip(elts.iter())
                        .map(|(t, el)| t.cast_value_int(el.clone()))
                        .collect::<Result<SmallVec<[Value; 8]>>>()?;
                    Ok(Value::Array(ValArray::from_iter_exact(a.into_iter())))
                }
                v => bail!("can't cast {v} to {self}"),
            },
            Type::Struct(ts) => match v {
                Value::Array(elts) => {
                    if elts.len() != ts.len() {
                        bail!("struct size mismatch {self} with {}", Value::Array(elts))
                    }
                    let is_pairs = elts.iter().all(|v| match v {
                        Value::Array(a) if a.len() == 2 => match &a[0] {
                            Value::String(_) => true,
                            _ => false,
                        },
                        _ => false,
                    });
                    if !is_pairs {
                        bail!("expected array of pairs, got {}", Value::Array(elts))
                    }
                    let mut elts_s: SmallVec<[&Value; 16]> = elts.iter().collect();
                    elts_s.sort_by_key(|v| match v {
                        Value::Array(a) => match &a[0] {
                            Value::String(s) => s,
                            _ => unreachable!(),
                        },
                        _ => unreachable!(),
                    });
                    let (keys_ok, ok) = ts.iter().zip(elts_s.iter()).fold(
                        (true, true),
                        |(kok, ok), ((fname, t), v)| {
                            let (name, v) = match v {
                                Value::Array(a) => match (&a[0], &a[1]) {
                                    (Value::String(n), v) => (n, v),
                                    _ => unreachable!(),
                                },
                                _ => unreachable!(),
                            };
                            (
                                kok && name == fname,
                                ok && kok
                                    && t.contains(&Type::Primitive(Typ::get(v).into())),
                            )
                        },
                    );
                    if ok {
                        drop(elts_s);
                        return Ok(Value::Array(elts));
                    } else if keys_ok {
                        let elts = ts
                            .iter()
                            .zip(elts_s.iter())
                            .map(|((n, t), v)| match v {
                                Value::Array(a) => {
                                    let a = [
                                        Value::String(n.clone()),
                                        t.cast_value_int(a[1].clone())?,
                                    ];
                                    Ok(Value::Array(ValArray::from_iter_exact(
                                        a.into_iter(),
                                    )))
                                }
                                _ => unreachable!(),
                            })
                            .collect::<Result<SmallVec<[Value; 8]>>>()?;
                        Ok(Value::Array(ValArray::from_iter_exact(elts.into_iter())))
                    } else {
                        drop(elts_s);
                        bail!("struct fields mismatch {self}, {}", Value::Array(elts))
                    }
                }
                v => bail!("can't cast {v} to {self}"),
            },
            Type::Variant(tag, ts) if ts.len() == 0 => match &v {
                Value::String(s) if s == tag => Ok(v),
                _ => bail!("variant tag mismatch expected {tag} got {v}"),
            },
            Type::Variant(tag, ts) => match &v {
                Value::Array(elts) => {
                    if ts.len() + 1 == elts.len() {
                        match &elts[0] {
                            Value::String(s) if s == tag => (),
                            v => bail!("variant tag mismatch expected {tag} got {v}"),
                        }
                        let ok = iter::once(&Type::Primitive(Typ::String.into()))
                            .chain(ts.iter())
                            .zip(elts.iter())
                            .fold(true, |ok, (t, v)| {
                                ok && t.contains(&Type::Primitive(Typ::get(v).into()))
                            });
                        if ok {
                            Ok(v)
                        } else {
                            let a = iter::once(&Type::Primitive(Typ::String.into()))
                                .chain(ts.iter())
                                .zip(elts.iter())
                                .map(|(t, v)| t.cast_value_int(v.clone()))
                                .collect::<Result<SmallVec<[Value; 8]>>>()?;
                            Ok(Value::Array(ValArray::from_iter_exact(a.into_iter())))
                        }
                    } else if ts.len() == elts.len() {
                        let mut a = ts
                            .iter()
                            .zip(elts.iter())
                            .map(|(t, v)| t.cast_value_int(v.clone()))
                            .collect::<Result<SmallVec<[Value; 8]>>>()?;
                        a.insert(0, Value::String(tag.clone()));
                        Ok(Value::Array(ValArray::from_iter_exact(a.into_iter())))
                    } else {
                        bail!("variant length mismatch")
                    }
                }
                v => bail!("can't cast {v} to {self}"),
            },
            t => match t.first_prim() {
                None => bail!("empty or non primitive cast"),
                Some(t) => Ok(v
                    .clone()
                    .cast(t)
                    .ok_or_else(|| anyhow!("can't cast {v} to {t}"))?),
            },
        }
    }

    pub fn cast_value(&self, v: Value) -> Value {
        match self.cast_value_int(v) {
            Ok(v) => v,
            Err(e) => Value::Error(e.to_string().into()),
        }
    }
}

impl<T: TypeMark> Type<T> {
    pub fn is_bot(&self) -> bool {
        match self {
            Type::Bottom(_) => true,
            Type::TVar(_)
            | Type::Primitive(_)
            | Type::Ref(_)
            | Type::Fn(_)
            | Type::Array(_)
            | Type::Tuple(_)
            | Type::Struct(_)
            | Type::Variant(_, _)
            | Type::Set(_) => false,
        }
    }

    pub fn with_deref<R, F: FnOnce(Option<&Self>) -> R>(&self, f: F) -> R {
        match self {
            Self::Bottom(_)
            | Self::Primitive(_)
            | Self::Fn(_)
            | Self::Set(_)
            | Self::Array(_)
            | Self::Tuple(_)
            | Self::Struct(_)
            | Self::Variant(_, _)
            | Self::Ref(_) => f(Some(self)),
            Self::TVar(tv) => f(tv.read().typ.read().bound()),
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
        acc.sort();
        match &*acc {
            [] => Type::Primitive(BitFlags::empty()),
            [t] => t.clone(),
            _ => Type::Set(Arc::from_iter(acc)),
        }
    }

    pub(crate) fn normalize(&self) -> Self {
        match self {
            Type::Ref(_) | Type::Bottom(_) | Type::Primitive(_) => self.clone(),
            Type::TVar(tv) => Type::TVar(tv.normalize()),
            Type::Set(s) => Self::flatten_set(s.iter().map(|t| t.normalize())),
            Type::Array(t) => Type::Array(Arc::new(t.normalize())),
            Type::Tuple(t) => {
                Type::Tuple(Arc::from_iter(t.iter().map(|t| t.normalize())))
            }
            Type::Struct(t) => Type::Struct(Arc::from_iter(
                t.iter().map(|(n, t)| (n.clone(), t.normalize())),
            )),
            Type::Variant(tag, t) => Type::Variant(
                tag.clone(),
                Arc::from_iter(t.iter().map(|t| t.normalize())),
            ),
            Type::Fn(ft) => Type::Fn(Arc::new(ft.normalize())),
        }
    }

    fn merge(&self, t: &Self) -> Option<Self> {
        match (self, t) {
            (Type::Ref(r0), Type::Ref(r1)) => {
                if r0 == r1 {
                    Some(Type::Ref(r0.clone()))
                } else {
                    None
                }
            }
            (Type::Ref(_), _) | (_, Type::Ref(_)) => None,
            (Type::Bottom(_), t) | (t, Type::Bottom(_)) => Some(t.clone()),
            (Type::Primitive(p), t) | (t, Type::Primitive(p)) if p.is_empty() => {
                Some(t.clone())
            }
            (Type::Primitive(s0), Type::Primitive(s1)) => {
                let mut s = *s0;
                s.insert(*s1);
                Some(Type::Primitive(s))
            }
            (Type::Fn(f0), Type::Fn(f1)) => {
                if f0 == f1 {
                    Some(Type::Fn(f0.clone()))
                } else {
                    None
                }
            }
            (Type::Array(t0), Type::Array(t1)) => {
                let t0f = match &**t0 {
                    Type::Set(et) => Self::flatten_set(et.iter().cloned()),
                    t => t.clone(),
                };
                let t1f = match &**t1 {
                    Type::Set(et) => Self::flatten_set(et.iter().cloned()),
                    t => t.clone(),
                };
                if t0f == t1f {
                    Some(Type::Array(t0.clone()))
                } else {
                    None
                }
            }
            (Type::Array(_), _) | (_, Type::Array(_)) => None,
            (Type::Set(s0), Type::Set(s1)) => {
                Some(Self::flatten_set(s0.iter().cloned().chain(s1.iter().cloned())))
            }
            (Type::Set(s), Type::Primitive(p)) | (Type::Primitive(p), Type::Set(s))
                if p.is_empty() =>
            {
                Some(Type::Set(s.clone()))
            }
            (Type::Set(s), t) | (t, Type::Set(s)) => {
                Some(Self::flatten_set(s.iter().cloned().chain(iter::once(t.clone()))))
            }
            (Type::Tuple(t0), Type::Tuple(t1)) => {
                if t0.len() == t1.len() {
                    let t = t0
                        .iter()
                        .zip(t1.iter())
                        .map(|(t0, t1)| t0.merge(t1))
                        .collect::<Option<SmallVec<[Type<T>; 8]>>>()?;
                    Some(Type::Tuple(Arc::from_iter(t)))
                } else {
                    None
                }
            }
            (Type::Variant(tag0, t0), Type::Variant(tag1, t1)) => {
                if tag0 == tag1 && t0.len() == t1.len() {
                    let t = t0
                        .iter()
                        .zip(t1.iter())
                        .map(|(t0, t1)| t0.merge(t1))
                        .collect::<Option<SmallVec<[Type<T>; 8]>>>()?;
                    Some(Type::Variant(tag0.clone(), Arc::from_iter(t)))
                } else {
                    None
                }
            }
            (Type::Struct(t0), Type::Struct(t1)) => {
                if t0.len() == t1.len() {
                    let t = t0
                        .iter()
                        .zip(t1.iter())
                        .map(|((n0, t0), (n1, t1))| {
                            if n0 != n1 {
                                None
                            } else {
                                t0.merge(t1).map(|t| (n0.clone(), t))
                            }
                        })
                        .collect::<Option<SmallVec<[(ArcStr, Type<T>); 8]>>>()?;
                    Some(Type::Struct(Arc::from_iter(t)))
                } else {
                    None
                }
            }
            (Type::TVar(tv0), Type::TVar(tv1)) if tv0.name == tv1.name && tv0 == tv1 => {
                Some(Type::TVar(tv0.clone()))
            }
            (Type::TVar(tv), t) => {
                tv.read().typ.read().bound().and_then(|tv| tv.merge(t))
            }
            (t, Type::TVar(tv)) => {
                tv.read().typ.read().bound().and_then(|tv| t.merge(tv))
            }
            (Type::Tuple(_), _)
            | (_, Type::Tuple(_))
            | (Type::Struct(_), _)
            | (_, Type::Struct(_))
            | (Type::Variant(_, _), _)
            | (_, Type::Variant(_, _))
            | (_, Type::Fn(_))
            | (Type::Fn(_), _) => None,
        }
    }
}

impl Type<Refs> {
    pub fn resolve_typerefs<'a, C: Ctx, E: UserEvent>(
        &self,
        scope: &ModPath,
        env: &Env<C, E>,
    ) -> Result<Type<NoRefs>> {
        match self {
            Type::Bottom(_) => Ok(Type::Bottom(PhantomData)),
            Type::Primitive(s) => Ok(Type::Primitive(*s)),
            Type::Array(t0) => {
                Ok(Type::Array(Arc::new(t0.resolve_typerefs(scope, env)?)))
            }
            Type::Tuple(ts) => {
                let i = ts
                    .iter()
                    .map(|t| t.resolve_typerefs(scope, env))
                    .collect::<Result<SmallVec<[Type<NoRefs>; 8]>>>()?
                    .into_iter();
                Ok(Type::Tuple(Arc::from_iter(i)))
            }
            Type::Variant(tag, ts) => {
                let i = ts
                    .iter()
                    .map(|t| t.resolve_typerefs(scope, env))
                    .collect::<Result<SmallVec<[Type<NoRefs>; 8]>>>()?
                    .into_iter();
                Ok(Type::Variant(tag.clone(), Arc::from_iter(i)))
            }
            Type::Struct(ts) => {
                let i = ts
                    .iter()
                    .map(|(n, t)| Ok((n.clone(), t.resolve_typerefs(scope, env)?)))
                    .collect::<Result<SmallVec<[(ArcStr, Type<NoRefs>); 8]>>>()?
                    .into_iter();
                Ok(Type::Struct(Arc::from_iter(i)))
            }
            Type::TVar(tv) => match tv.read().typ.read().bound() {
                None => Ok(Type::TVar(TVar::empty_named(tv.name.clone()))),
                Some(typ) => {
                    let typ = typ.resolve_typerefs(scope, env)?;
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
                let mut res: SmallVec<[Type<NoRefs>; 8]> = smallvec![];
                for t in ts.iter() {
                    res.push(t.resolve_typerefs(scope, env)?)
                }
                Ok(Type::flatten_set(res))
            }
            Type::Fn(f) => {
                let vargs = f
                    .vargs
                    .as_ref()
                    .map(|t| t.resolve_typerefs(scope, env))
                    .transpose()?;
                let rtype = f.rtype.resolve_typerefs(scope, env)?;
                let mut res: SmallVec<[FnArgType<NoRefs>; 8]> = smallvec![];
                for a in f.args.iter() {
                    let typ = a.typ.resolve_typerefs(scope, env)?;
                    let a = FnArgType { label: a.label.clone(), typ };
                    res.push(a);
                }
                let mut cres: SmallVec<[(TVar<NoRefs>, Type<NoRefs>); 4]> = smallvec![];
                for (tv, tc) in f.constraints.iter() {
                    let tv = tv.resolve_typerefs(scope, env)?;
                    let tc = tc.resolve_typerefs(scope, env)?;
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
            Self::Array(t) => write!(f, "Array<{t}>"),
            Self::Tuple(ts) => {
                write!(f, "(")?;
                for (i, t) in ts.iter().enumerate() {
                    write!(f, "{t}")?;
                    if i < ts.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
            Self::Variant(tag, ts) if ts.len() == 0 => {
                write!(f, "`{tag}")
            }
            Self::Variant(tag, ts) => {
                write!(f, "`{tag}(")?;
                for (i, t) in ts.iter().enumerate() {
                    write!(f, "{t}")?;
                    if i < ts.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, ")")
            }
            Self::Struct(ts) => {
                write!(f, "{{")?;
                for (i, (n, t)) in ts.iter().enumerate() {
                    write!(f, "{n}: {t}")?;
                    if i < ts.len() - 1 {
                        write!(f, ", ")?
                    }
                }
                write!(f, "}}")
            }
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

impl<T: TypeMark> Default for FnType<T> {
    fn default() -> Self {
        Self {
            args: Arc::from_iter([]),
            vargs: None,
            rtype: Default::default(),
            constraints: Arc::from_iter([]),
        }
    }
}

impl FnType<Refs> {
    pub fn resolve_typerefs<'a, C: Ctx, E: UserEvent>(
        &self,
        scope: &ModPath,
        env: &Env<C, E>,
    ) -> Result<FnType<NoRefs>> {
        let typ = Type::Fn(Arc::new(self.clone()));
        match typ.resolve_typerefs(scope, env)? {
            Type::Fn(f) => Ok((*f).clone()),
            _ => bail!("unexpected fn resolution"),
        }
    }
}

impl<T: TypeMark> FnType<T> {
    fn normalize(&self) -> Self {
        let Self { args, vargs, rtype, constraints } = self;
        let args = Arc::from_iter(
            args.iter()
                .map(|a| FnArgType { label: a.label.clone(), typ: a.typ.normalize() }),
        );
        let vargs = vargs.as_ref().map(|t| t.normalize());
        let rtype = rtype.normalize();
        let constraints =
            Arc::from_iter(constraints.iter().map(|(tv, t)| (tv.clone(), t.normalize())));
        FnType { args, vargs, rtype, constraints }
    }
}

impl FnType<NoRefs> {
    pub fn unbind_tvars(&self) {
        let FnType { args, vargs, rtype, constraints } = self;
        for arg in args.iter() {
            arg.typ.unbind_tvars()
        }
        if let Some(t) = vargs {
            t.unbind_tvars()
        }
        rtype.unbind_tvars();
        for (tv, tc) in constraints.iter() {
            tv.unbind();
            tc.unbind_tvars()
        }
    }

    pub fn freeze(&self) {
        let FnType { args, vargs, rtype, constraints } = self;
        for arg in args.iter() {
            arg.typ.freeze()
        }
        if let Some(t) = vargs {
            t.freeze()
        }
        rtype.freeze();
        for (tv, tc) in constraints.iter() {
            tv.freeze();
            tc.freeze()
        }
    }

    pub fn reset_tvars(&self) -> Self {
        let FnType { args, vargs, rtype, constraints } = self;
        let args = Arc::from_iter(
            args.iter()
                .map(|a| FnArgType { label: a.label.clone(), typ: a.typ.reset_tvars() }),
        );
        let vargs = vargs.as_ref().map(|t| t.reset_tvars());
        let rtype = rtype.reset_tvars();
        let constraints = Arc::from_iter(
            constraints
                .iter()
                .map(|(tv, tc)| (TVar::empty_named(tv.name.clone()), tc.reset_tvars())),
        );
        FnType { args, vargs, rtype, constraints }
    }

    pub fn has_unbound(&self) -> bool {
        let FnType { args, vargs, rtype, constraints } = self;
        args.iter().any(|a| a.typ.has_unbound())
            || vargs.as_ref().map(|t| t.has_unbound()).unwrap_or(false)
            || rtype.has_unbound()
            || constraints
                .iter()
                .any(|(tv, tc)| tv.read().typ.read().is_unbound() || tc.has_unbound())
    }

    pub fn alias_tvars(&self, known: &mut FxHashMap<ArcStr, TVar<NoRefs>>) {
        let FnType { args, vargs, rtype, constraints } = self;
        for arg in args.iter() {
            arg.typ.alias_tvars(known)
        }
        if let Some(vargs) = vargs {
            vargs.alias_tvars(known)
        }
        rtype.alias_tvars(known);
        for (tv, tc) in constraints.iter() {
            Type::TVar(tv.clone()).alias_tvars(known);
            tc.alias_tvars(known);
        }
    }

    pub fn contains(&self, t: &Self) -> bool {
        let mut sul = 0;
        let mut tul = 0;
        for (i, a) in self.args.iter().enumerate() {
            sul = i;
            match &a.label {
                None => {
                    break;
                }
                Some((l, _)) => match t
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
                },
            }
        }
        for (i, a) in t.args.iter().enumerate() {
            tul = i;
            match &a.label {
                None => {
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

    pub fn check_contains(&self, other: &Self) -> Result<()> {
        if !self.contains(other) {
            bail!("Fn type mismatch {self} does not contain {other}")
        }
        Ok(())
    }

    /// Return true if function signatures match. This is contains,
    /// but does not allow labeled argument subtyping.
    pub fn sigmatch(&self, other: &Self) -> bool {
        let Self { args: args0, vargs: vargs0, rtype: rtype0, constraints: constraints0 } =
            self;
        let Self { args: args1, vargs: vargs1, rtype: rtype1, constraints: constraints1 } =
            other;
        args0.len() == args1.len()
            && args0
                .iter()
                .zip(args1.iter())
                .all(|(a0, a1)| a0.label == a1.label && a0.typ.contains(&a1.typ))
            && match (vargs0, vargs1) {
                (None, None) => true,
                (None, _) | (_, None) => false,
                (Some(t0), Some(t1)) => t0.contains(t1),
            }
            && rtype0.contains(rtype1)
            && constraints0.len() == constraints1.len()
            && constraints0
                .iter()
                .zip(constraints1.iter())
                .all(|((tv0, t0), (tv1, t1))| tv0.name == tv1.name && t0.contains(t1))
    }

    pub fn check_sigmatch(&self, other: &Self) -> Result<()> {
        if !self.sigmatch(other) {
            bail!("Fn signatures do not match {self} does not match {other}")
        }
        Ok(())
    }

    pub fn map_argpos(
        &self,
        other: &Self,
    ) -> FxHashMap<ArcStr, (Option<usize>, Option<usize>)> {
        let mut tbl: FxHashMap<ArcStr, (Option<usize>, Option<usize>)> =
            FxHashMap::default();
        for (i, a) in self.args.iter().enumerate() {
            match &a.label {
                None => break,
                Some((n, _)) => tbl.entry(n.clone()).or_default().0 = Some(i),
            }
        }
        for (i, a) in other.args.iter().enumerate() {
            match &a.label {
                None => break,
                Some((n, _)) => tbl.entry(n.clone()).or_default().1 = Some(i),
            }
        }
        tbl
    }
}

impl<T: TypeMark> fmt::Display for FnType<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.constraints.len() == 0 {
            write!(f, "fn(")?;
        } else {
            write!(f, "fn<")?;
            for (i, (tv, t)) in self.constraints.iter().enumerate() {
                write!(f, "{tv}: {t}")?;
                if i < self.constraints.len() - 1 {
                    write!(f, ", ")?;
                }
            }
            write!(f, ">(")?;
        }
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
