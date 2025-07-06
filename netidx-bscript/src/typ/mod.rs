use crate::{env::Env, expr::ModPath, Ctx, UserEvent};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use enumflags2::{bitflags, BitFlags};
use fxhash::{FxHashMap, FxHashSet};
use netidx::{
    publisher::{Typ, Value},
    utils::Either,
};
use netidx_netproto::valarray::ValArray;
use parking_lot::RwLock;
use smallvec::{smallvec, SmallVec};
use std::{
    cell::{Cell, RefCell},
    cmp::{Eq, PartialEq},
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt::{self, Debug},
    iter,
};
use triomphe::Arc;

mod fntyp;
mod tval;
mod tvar;

pub use fntyp::{FnArgType, FnType};
pub use tval::TVal;
use tvar::would_cycle_inner;
pub use tvar::TVar;

struct AndAc(bool);

impl FromIterator<bool> for AndAc {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        AndAc(iter.into_iter().all(|b| b))
    }
}

#[derive(Debug, Clone, Copy)]
#[bitflags]
#[repr(u64)]
pub enum PrintFlag {
    /// Dereference type variables and print both the tvar name and the bound
    /// type or "unbound".
    DerefTVars,
    /// Replace common primitives with shorter type names as defined
    /// in core. e.g. Any, instead of the set of every primitive type.
    ReplacePrims,
}

thread_local! {
    static PRINT_FLAGS: Cell<BitFlags<PrintFlag>> = Cell::new(PrintFlag::ReplacePrims.into());
}

/// For the duration of the closure F change the way type variables
/// are formatted (on this thread only) according to the specified
/// flags.
pub fn format_with_flags<R, F: FnOnce() -> R>(flags: BitFlags<PrintFlag>, f: F) -> R {
    let prev = PRINT_FLAGS.replace(flags);
    let res = f();
    PRINT_FLAGS.set(prev);
    res
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Type {
    Bottom,
    Any,
    Primitive(BitFlags<Typ>),
    Ref { scope: ModPath, name: ModPath, params: Arc<[Type]> },
    Fn(Arc<FnType>),
    Set(Arc<[Type]>),
    TVar(TVar),
    Array(Arc<Type>),
    ByRef(Arc<Type>),
    Tuple(Arc<[Type]>),
    Struct(Arc<[(ArcStr, Type)]>),
    Variant(ArcStr, Arc<[Type]>),
}

impl Default for Type {
    fn default() -> Self {
        Self::Bottom
    }
}

impl Type {
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
            Self::Bottom
            | Self::Any
            | Self::Primitive(_)
            | Self::Fn(_)
            | Self::Set(_)
            | Self::Array(_)
            | Self::ByRef(_)
            | Self::Tuple(_)
            | Self::Struct(_)
            | Self::Variant(_, _) => true,
            Self::TVar(tv) => tv.read().typ.read().is_some(),
            Self::Ref { .. } => true,
        }
    }

    pub fn lookup_ref<'a, C: Ctx, E: UserEvent>(
        &'a self,
        env: &'a Env<C, E>,
    ) -> Result<&'a Type> {
        match self {
            Self::Ref { scope, name, params } => {
                let def = env
                    .lookup_typedef(scope, name)
                    .ok_or_else(|| anyhow!("undefined type {name} in {scope}"))?;
                if def.params.len() != params.len() {
                    bail!("{} expects {} type parameters", name, def.params.len());
                }
                def.typ.unbind_tvars();
                for ((tv, ct), arg) in def.params.iter().zip(params.iter()) {
                    if let Some(ct) = ct {
                        ct.check_contains(env, arg)?;
                    }
                    if !tv.would_cycle(arg) {
                        *tv.read().typ.write() = Some(arg.clone());
                    }
                }
                Ok(&def.typ)
            }
            t => Ok(t),
        }
    }

    pub fn check_contains<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        t: &Self,
    ) -> Result<()> {
        if self.contains(env, t)? {
            Ok(())
        } else {
            format_with_flags(PrintFlag::DerefTVars | PrintFlag::ReplacePrims, || {
                bail!("type mismatch {self} does not contain {t}")
            })
        }
    }

    fn contains_int<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        hist: &mut FxHashMap<(usize, usize), bool>,
        t: &Self,
    ) -> Result<bool> {
        match (self, t) {
            (
                Self::Ref { scope: s0, name: n0, .. },
                Self::Ref { scope: s1, name: n1, .. },
            ) if s0 == s1 && n0 == n1 => Ok(true),
            (t0 @ Self::Ref { .. }, t1) | (t0, t1 @ Self::Ref { .. }) => {
                let t0 = t0.lookup_ref(env)?;
                let t1 = t1.lookup_ref(env)?;
                let t0_addr = (t0 as *const Type).addr();
                let t1_addr = (t1 as *const Type).addr();
                match hist.get(&(t0_addr, t1_addr)) {
                    Some(r) => Ok(*r),
                    None => {
                        hist.insert((t0_addr, t1_addr), true);
                        match t0.contains_int(env, hist, t1) {
                            Ok(r) => {
                                hist.insert((t0_addr, t1_addr), r);
                                Ok(r)
                            }
                            Err(e) => {
                                hist.remove(&(t0_addr, t1_addr));
                                Err(e)
                            }
                        }
                    }
                }
            }
            (Self::TVar(t0), Self::Bottom) => {
                if let Some(_) = &*t0.read().typ.read() {
                    return Ok(true);
                }
                *t0.read().typ.write() = Some(Self::Bottom);
                Ok(true)
            }
            (Self::TVar(t0), Self::Any) => {
                if let Some(t0) = &*t0.read().typ.read() {
                    return t0.contains_int(env, hist, t);
                }
                *t0.read().typ.write() = Some(Self::Any);
                Ok(true)
            }
            (Self::Any, _) => Ok(true),
            (_, Self::Any) => Ok(false),
            (Self::Bottom, _) | (_, Self::Bottom) => Ok(true),
            (Self::Primitive(p0), Self::Primitive(p1)) => Ok(p0.contains(*p1)),
            (
                Self::Primitive(p),
                Self::Array(_) | Self::Tuple(_) | Self::Struct(_) | Self::Variant(_, _),
            ) => Ok(p.contains(Typ::Array)),
            (Self::Array(t0), Self::Array(t1)) => t0.contains_int(env, hist, t1),
            (Self::Array(t0), Self::Primitive(p)) if *p == BitFlags::from(Typ::Array) => {
                t0.contains_int(env, hist, &Type::Primitive(BitFlags::all()))
            }
            (Self::Tuple(t0), Self::Tuple(t1)) => Ok(t0.len() == t1.len()
                && t0
                    .iter()
                    .zip(t1.iter())
                    .map(|(t0, t1)| t0.contains_int(env, hist, t1))
                    .collect::<Result<AndAc>>()?
                    .0),
            (Self::Struct(t0), Self::Struct(t1)) => {
                Ok(t0.len() == t1.len() && {
                    // struct types are always sorted by field name
                    t0.iter()
                        .zip(t1.iter())
                        .map(|((n0, t0), (n1, t1))| {
                            Ok(n0 == n1 && t0.contains_int(env, hist, t1)?)
                        })
                        .collect::<Result<AndAc>>()?
                        .0
                })
            }
            (Self::Variant(tg0, t0), Self::Variant(tg1, t1)) => Ok(tg0 == tg1
                && t0.len() == t1.len()
                && t0
                    .iter()
                    .zip(t1.iter())
                    .map(|(t0, t1)| t0.contains_int(env, hist, t1))
                    .collect::<Result<AndAc>>()?
                    .0),
            (Self::ByRef(t0), Self::ByRef(t1)) => t0.contains_int(env, hist, t1),
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
            | (Self::Variant(_, _), Self::Tuple(_)) => Ok(false),
            (Self::TVar(t0), tt1 @ Self::TVar(t1)) => {
                #[derive(Debug)]
                enum Act {
                    RightCopy,
                    LeftAlias,
                    LeftCopy,
                }
                let act = {
                    let t0 = t0.read();
                    let t1 = t1.read();
                    let addr = Arc::as_ptr(&t0.typ).addr();
                    if addr == Arc::as_ptr(&t1.typ).addr() {
                        return Ok(true);
                    }
                    let t0i = t0.typ.read();
                    let t1i = t1.typ.read();
                    match (&*t0i, &*t1i) {
                        (Some(t0), Some(t1)) => return t0.contains_int(env, hist, &*t1),
                        (None, None) => {
                            if would_cycle_inner(addr, tt1) {
                                return Ok(true);
                            }
                            Act::LeftAlias
                        }
                        (Some(_), None) => {
                            if would_cycle_inner(addr, tt1) {
                                return Ok(true);
                            }
                            Act::RightCopy
                        }
                        (None, Some(_)) => {
                            if would_cycle_inner(addr, tt1) {
                                return Ok(true);
                            }
                            Act::LeftCopy
                        }
                    }
                };
                match act {
                    Act::RightCopy => t1.copy(t0),
                    Act::LeftAlias => t0.alias(t1),
                    Act::LeftCopy => t0.copy(t1),
                }
                Ok(true)
            }
            (Self::TVar(t0), t1) if !t0.would_cycle(t1) => {
                if let Some(t0) = &*t0.read().typ.read() {
                    return t0.contains_int(env, hist, t1);
                }
                *t0.read().typ.write() = Some(t1.clone());
                Ok(true)
            }
            (t0, Self::TVar(t1)) if !t1.would_cycle(t0) => {
                if let Some(t1) = &*t1.read().typ.read() {
                    return t0.contains_int(env, hist, t1);
                }
                *t1.read().typ.write() = Some(t0.clone());
                Ok(true)
            }
            (t0, Self::Set(s)) => Ok(s
                .iter()
                .map(|t1| t0.contains_int(env, hist, t1))
                .collect::<Result<AndAc>>()?
                .0),
            (Self::Set(s), t) => Ok(s
                .iter()
                .fold(Ok::<_, anyhow::Error>(false), |acc, t0| {
                    Ok(acc? || t0.contains_int(env, hist, t)?)
                })?
                || t.iter_prims().fold(Ok::<_, anyhow::Error>(true), |acc, t1| {
                    Ok(acc?
                        && s.iter().fold(Ok::<_, anyhow::Error>(false), |acc, t0| {
                            Ok(acc? || t0.contains_int(env, hist, &t1)?)
                        })?)
                })?),
            (Self::Fn(f0), Self::Fn(f1)) => {
                Ok(f0.as_ptr() == f1.as_ptr() || f0.contains_int(env, hist, f1)?)
            }
            (_, Self::TVar(_))
            | (Self::TVar(_), _)
            | (Self::Fn(_), _)
            | (Self::ByRef(_), _)
            | (_, Self::ByRef(_))
            | (_, Self::Fn(_)) => Ok(false),
        }
    }

    pub fn contains<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        t: &Self,
    ) -> Result<bool> {
        thread_local! {
            static HIST: RefCell<FxHashMap<(usize, usize), bool>> = RefCell::new(HashMap::default());
        }
        HIST.with_borrow_mut(|hist| {
            hist.clear();
            self.contains_int(env, hist, t)
        })
    }

    pub(crate) fn init_wildcard_match<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        t: &Self,
    ) -> Result<()> {
        match (self, t) {
            (Type::TVar(tt0), Type::TVar(tt1)) => {
                let alias = {
                    let t0 = tt0.read();
                    let t1 = tt1.read();
                    let addr = Arc::as_ptr(&t1.typ).addr();
                    match (&*t0.typ.read(), &*t1.typ.read()) {
                        (Some(_), _) | (_, Some(_)) => false,
                        (None, None) => !would_cycle_inner(addr, self),
                    }
                };
                if alias {
                    tt1.alias(&tt0);
                } else {
                    self.contains(env, t)?;
                }
            }
            (_, _) => {
                self.contains(env, t)?;
            }
        }
        Ok(())
    }

    fn could_match_int<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        hist: &mut FxHashMap<(usize, usize), bool>,
        t: &Self,
    ) -> Result<bool> {
        match (self, t) {
            (
                Self::Ref { scope: s0, name: n0, .. },
                Self::Ref { scope: s1, name: n1, .. },
            ) if s0 == s1 && n0 == n1 => Ok(true),
            (t0 @ Self::Ref { .. }, t1) | (t0, t1 @ Self::Ref { .. }) => {
                let t0 = t0.lookup_ref(env)?;
                let t1 = t1.lookup_ref(env)?;
                let t0_addr = (t0 as *const Type).addr();
                let t1_addr = (t1 as *const Type).addr();
                match hist.get(&(t0_addr, t1_addr)) {
                    Some(r) => Ok(*r),
                    None => {
                        hist.insert((t0_addr, t1_addr), true);
                        match t0.could_match_int(env, hist, t1) {
                            Ok(r) => {
                                hist.insert((t0_addr, t1_addr), r);
                                Ok(r)
                            }
                            Err(e) => {
                                hist.remove(&(t0_addr, t1_addr));
                                Err(e)
                            }
                        }
                    }
                }
            }
            (t0, Self::Primitive(s)) => {
                for t1 in s.iter() {
                    if t0.contains_int(env, hist, &Type::Primitive(t1.into()))? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            (t0, Self::Set(ts)) => {
                for t1 in ts.iter() {
                    if t0.contains_int(env, hist, t1)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            (Type::TVar(t0), t1) => match &*t0.read().typ.read() {
                Some(t0) => t0.could_match_int(env, hist, t1),
                None => Ok(false),
            },
            (t0, Type::TVar(t1)) => match &*t1.read().typ.read() {
                Some(t1) => t0.could_match_int(env, hist, t1),
                None => Ok(false),
            },
            (t0, t1) => t0.contains_int(env, hist, t1),
        }
    }

    pub fn could_match<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        t: &Self,
    ) -> Result<bool> {
        thread_local! {
            static HIST: RefCell<FxHashMap<(usize, usize), bool>> = RefCell::new(HashMap::default());
        }
        HIST.with_borrow_mut(|hist| {
            hist.clear();
            self.could_match_int(env, hist, t)
        })
    }

    fn union_int(&self, t: &Self) -> Self {
        match (self, t) {
            (Type::Bottom, t) | (t, Type::Bottom) => t.clone(),
            (Type::Any, _) | (_, Type::Any) => Type::Any,
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
            (t @ Type::Array(t0), u @ Type::Array(t1)) => {
                if t0 == t1 {
                    Type::Array(t0.clone())
                } else {
                    Type::Set(Arc::from_iter([u.clone(), t.clone()]))
                }
            }
            (t @ Type::ByRef(t0), u @ Type::ByRef(t1)) => {
                if t0 == t1 {
                    Type::ByRef(t0.clone())
                } else {
                    Type::Set(Arc::from_iter([u.clone(), t.clone()]))
                }
            }
            (Type::Set(s0), Type::Set(s1)) => {
                Type::Set(Arc::from_iter(s0.iter().cloned().chain(s1.iter().cloned())))
            }
            (Type::Set(s), t) | (t, Type::Set(s)) => {
                Type::Set(Arc::from_iter(s.iter().cloned().chain(iter::once(t.clone()))))
            }
            (u @ Type::Struct(t0), t @ Type::Struct(t1)) => {
                if t0.len() == t1.len() && t0 == t1 {
                    u.clone()
                } else {
                    Type::Set(Arc::from_iter([u.clone(), t.clone()]))
                }
            }
            (u @ Type::Struct(_), t) | (t, u @ Type::Struct(_)) => {
                Type::Set(Arc::from_iter([u.clone(), t.clone()]))
            }
            (u @ Type::Tuple(t0), t @ Type::Tuple(t1)) => {
                if t0 == t1 {
                    u.clone()
                } else {
                    Type::Set(Arc::from_iter([u.clone(), t.clone()]))
                }
            }
            (u @ Type::Tuple(_), t) | (t, u @ Type::Tuple(_)) => {
                Type::Set(Arc::from_iter([u.clone(), t.clone()]))
            }
            (u @ Type::Variant(tg0, t0), t @ Type::Variant(tg1, t1)) => {
                if tg0 == tg1 && t0.len() == t1.len() {
                    let typs = t0.iter().zip(t1.iter()).map(|(t0, t1)| t0.union_int(t1));
                    Type::Variant(tg0.clone(), Arc::from_iter(typs))
                } else {
                    Type::Set(Arc::from_iter([u.clone(), t.clone()]))
                }
            }
            (u @ Type::Variant(_, _), t) | (t, u @ Type::Variant(_, _)) => {
                Type::Set(Arc::from_iter([u.clone(), t.clone()]))
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
            (t @ Type::ByRef(_), u) | (u, t @ Type::ByRef(_)) => {
                Type::Set(Arc::from_iter([t.clone(), u.clone()]))
            }
            (tr @ Type::Ref { .. }, t) | (t, tr @ Type::Ref { .. }) => {
                Type::Set(Arc::from_iter([tr.clone(), t.clone()]))
            }
        }
    }

    pub fn union(&self, t: &Self) -> Self {
        self.union_int(t).normalize()
    }

    fn diff_int<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        hist: &mut FxHashMap<(usize, usize), Type>,
        t: &Self,
    ) -> Result<Self> {
        match (self, t) {
            (Type::Any, _) => Ok(Type::Any),
            (_, Type::Any) => Ok(Type::Primitive(BitFlags::empty())),
            (
                Type::Ref { scope: s0, name: n0, .. },
                Type::Ref { scope: s1, name: n1, .. },
            ) if s0 == s1 && n0 == n1 => Ok(Type::Primitive(BitFlags::empty())),
            (t0 @ Type::Ref { .. }, t1) | (t0, t1 @ Type::Ref { .. }) => {
                let t0 = t0.lookup_ref(env)?;
                let t1 = t1.lookup_ref(env)?;
                let t0_addr = (t0 as *const Type).addr();
                let t1_addr = (t1 as *const Type).addr();
                match hist.get(&(t0_addr, t1_addr)) {
                    Some(r) => Ok(r.clone()),
                    None => {
                        let r = Type::Primitive(BitFlags::empty());
                        hist.insert((t0_addr, t1_addr), r);
                        match t0.diff_int(env, hist, &t1) {
                            Ok(r) => {
                                hist.insert((t0_addr, t1_addr), r.clone());
                                Ok(r)
                            }
                            Err(e) => {
                                hist.remove(&(t0_addr, t1_addr));
                                Err(e)
                            }
                        }
                    }
                }
            }
            (Type::Bottom, t) | (t, Type::Bottom) => Ok(t.clone()),
            (Type::Primitive(s0), Type::Primitive(s1)) => {
                let mut s = *s0;
                s.remove(*s1);
                Ok(Type::Primitive(s))
            }
            (
                Type::Primitive(p),
                Type::Array(_) | Type::Struct(_) | Type::Tuple(_) | Type::Variant(_, _),
            ) => {
                // CR estokes: is this correct? It's a bit odd.
                let mut s = *p;
                s.remove(Typ::Array);
                Ok(Type::Primitive(s))
            }
            (
                Type::Array(_) | Type::Struct(_) | Type::Tuple(_) | Type::Variant(_, _),
                Type::Primitive(p),
            ) => {
                if p.contains(Typ::Array) {
                    Ok(Type::Primitive(BitFlags::empty()))
                } else {
                    Ok(self.clone())
                }
            }
            (Type::Array(t0), Type::Array(t1)) => {
                Ok(Type::Array(Arc::new(t0.diff_int(env, hist, t1)?)))
            }
            (Type::ByRef(t0), Type::ByRef(t1)) => {
                Ok(Type::ByRef(Arc::new(t0.diff_int(env, hist, t1)?)))
            }
            (Type::Set(s0), Type::Set(s1)) => {
                let mut s: SmallVec<[Type; 4]> = smallvec![];
                for i in 0..s0.len() {
                    s.push(s0[i].clone());
                    for j in 0..s1.len() {
                        s[i] = s[i].diff_int(env, hist, &s1[j])?
                    }
                }
                Ok(Self::flatten_set(s.into_iter()))
            }
            (Type::Set(s), t) => Ok(Self::flatten_set(
                s.iter()
                    .map(|s| s.diff_int(env, hist, t))
                    .collect::<Result<SmallVec<[_; 8]>>>()?,
            )),
            (t, Type::Set(s)) => {
                let mut t = t.clone();
                for st in s.iter() {
                    t = t.diff_int(env, hist, st)?;
                }
                Ok(t)
            }
            (Type::Tuple(t0), Type::Tuple(t1)) => {
                if t0 == t1 {
                    Ok(Type::Primitive(BitFlags::empty()))
                } else {
                    Ok(self.clone())
                }
            }
            (Type::Tuple(_), _) | (_, Type::Tuple(_)) => Ok(self.clone()),
            (Type::Struct(t0), Type::Struct(t1)) => {
                if t0.len() == t1.len() && t0 == t1 {
                    Ok(Type::Primitive(BitFlags::empty()))
                } else {
                    Ok(self.clone())
                }
            }
            (Type::Struct(_), _) | (_, Type::Struct(_)) => Ok(self.clone()),
            (Type::ByRef(_), _) | (_, Type::ByRef(_)) => Ok(self.clone()),
            (Type::Variant(tg0, t0), Type::Variant(tg1, t1)) => {
                if tg0 == tg1 && t0.len() == t1.len() && t0 == t1 {
                    Ok(Type::Primitive(BitFlags::empty()))
                } else {
                    Ok(self.clone())
                }
            }
            (Type::Variant(_, _), _) | (_, Type::Variant(_, _)) => Ok(self.clone()),
            (Type::Fn(f0), Type::Fn(f1)) => {
                if f0 == f1 {
                    Ok(Type::Primitive(BitFlags::empty()))
                } else {
                    Ok(Type::Fn(f0.clone()))
                }
            }
            (f @ Type::Fn(_), _) => Ok(f.clone()),
            (t, Type::Fn(_)) => Ok(t.clone()),
            (Type::TVar(tv0), Type::TVar(tv1)) => {
                if tv0.read().typ.as_ptr() == tv1.read().typ.as_ptr() {
                    return Ok(Type::Primitive(BitFlags::empty()));
                }
                Ok(match (&*tv0.read().typ.read(), &*tv1.read().typ.read()) {
                    (None, _) | (_, None) => Type::TVar(tv0.clone()),
                    (Some(t0), Some(t1)) => t0.diff_int(env, hist, t1)?,
                })
            }
            (Type::TVar(tv), t1) => match &*tv.read().typ.read() {
                None => Ok(Type::TVar(tv.clone())),
                Some(t0) => t0.diff_int(env, hist, t1),
            },
            (t0, Type::TVar(tv)) => match &*tv.read().typ.read() {
                None => Ok(t0.clone()),
                Some(t1) => t0.diff_int(env, hist, t1),
            },
        }
    }

    pub fn diff<C: Ctx, E: UserEvent>(&self, env: &Env<C, E>, t: &Self) -> Result<Self> {
        thread_local! {
            static HIST: RefCell<FxHashMap<(usize, usize), Type>> = RefCell::new(HashMap::default());
        }
        HIST.with_borrow_mut(|hist| {
            hist.clear();
            Ok(self.diff_int(env, hist, t)?.normalize())
        })
    }

    pub fn any() -> Self {
        Self::Any
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
    pub fn alias_tvars(&self, known: &mut FxHashMap<ArcStr, TVar>) {
        match self {
            Type::Bottom | Type::Any | Type::Primitive(_) => (),
            Type::Ref { params, .. } => {
                for t in params.iter() {
                    t.alias_tvars(known);
                }
            }
            Type::Array(t) => t.alias_tvars(known),
            Type::ByRef(t) => t.alias_tvars(known),
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

    pub fn collect_tvars(&self, known: &mut FxHashMap<ArcStr, TVar>) {
        match self {
            Type::Bottom | Type::Any | Type::Primitive(_) => (),
            Type::Ref { params, .. } => {
                for t in params.iter() {
                    t.collect_tvars(known);
                }
            }
            Type::Array(t) => t.collect_tvars(known),
            Type::ByRef(t) => t.collect_tvars(known),
            Type::Tuple(ts) => {
                for t in ts.iter() {
                    t.collect_tvars(known)
                }
            }
            Type::Struct(ts) => {
                for (_, t) in ts.iter() {
                    t.collect_tvars(known)
                }
            }
            Type::Variant(_, ts) => {
                for t in ts.iter() {
                    t.collect_tvars(known)
                }
            }
            Type::TVar(tv) => match known.entry(tv.name.clone()) {
                Entry::Occupied(_) => (),
                Entry::Vacant(e) => {
                    e.insert(tv.clone());
                    ()
                }
            },
            Type::Fn(ft) => ft.collect_tvars(known),
            Type::Set(s) => {
                for typ in s.iter() {
                    typ.collect_tvars(known)
                }
            }
        }
    }

    pub fn check_tvars_declared(&self, declared: &FxHashSet<ArcStr>) -> Result<()> {
        match self {
            Type::Bottom | Type::Any | Type::Primitive(_) => Ok(()),
            Type::Ref { params, .. } => {
                params.iter().try_for_each(|t| t.check_tvars_declared(declared))
            }
            Type::Array(t) => t.check_tvars_declared(declared),
            Type::ByRef(t) => t.check_tvars_declared(declared),
            Type::Tuple(ts) => {
                ts.iter().try_for_each(|t| t.check_tvars_declared(declared))
            }
            Type::Struct(ts) => {
                ts.iter().try_for_each(|(_, t)| t.check_tvars_declared(declared))
            }
            Type::Variant(_, ts) => {
                ts.iter().try_for_each(|t| t.check_tvars_declared(declared))
            }
            Type::TVar(tv) => {
                if !declared.contains(&tv.name) {
                    bail!("undeclared type variable '{}'", tv.name)
                } else {
                    Ok(())
                }
            }
            Type::Set(s) => s.iter().try_for_each(|t| t.check_tvars_declared(declared)),
            Type::Fn(_) => Ok(()),
        }
    }

    pub fn has_unbound(&self) -> bool {
        match self {
            Type::Bottom | Type::Any | Type::Primitive(_) => false,
            Type::Ref { .. } => false,
            Type::Array(t0) => t0.has_unbound(),
            Type::ByRef(t0) => t0.has_unbound(),
            Type::Tuple(ts) => ts.iter().any(|t| t.has_unbound()),
            Type::Struct(ts) => ts.iter().any(|(_, t)| t.has_unbound()),
            Type::Variant(_, ts) => ts.iter().any(|t| t.has_unbound()),
            Type::TVar(tv) => tv.read().typ.read().is_some(),
            Type::Set(s) => s.iter().any(|t| t.has_unbound()),
            Type::Fn(ft) => ft.has_unbound(),
        }
    }

    /// bind all unbound type variables to the specified type
    pub fn bind_as(&self, t: &Self) {
        match self {
            Type::Bottom | Type::Any | Type::Primitive(_) => (),
            Type::Ref { .. } => (),
            Type::Array(t0) => t0.bind_as(t),
            Type::ByRef(t0) => t0.bind_as(t),
            Type::Tuple(ts) => {
                for elt in ts.iter() {
                    elt.bind_as(t)
                }
            }
            Type::Struct(ts) => {
                for (_, elt) in ts.iter() {
                    elt.bind_as(t)
                }
            }
            Type::Variant(_, ts) => {
                for elt in ts.iter() {
                    elt.bind_as(t)
                }
            }
            Type::TVar(tv) => {
                let tv = tv.read();
                let mut tv = tv.typ.write();
                if tv.is_none() {
                    *tv = Some(t.clone());
                }
            }
            Type::Set(s) => {
                for elt in s.iter() {
                    elt.bind_as(t)
                }
            }
            Type::Fn(ft) => ft.bind_as(t),
        }
    }

    /// return a copy of self with all type variables unbound and
    /// unaliased. self will not be modified
    pub fn reset_tvars(&self) -> Type {
        match self {
            Type::Bottom => Type::Bottom,
            Type::Any => Type::Any,
            Type::Primitive(p) => Type::Primitive(*p),
            Type::Ref { scope, name, params } => Type::Ref {
                scope: scope.clone(),
                name: name.clone(),
                params: Arc::from_iter(params.iter().map(|t| t.reset_tvars())),
            },
            Type::Array(t0) => Type::Array(Arc::new(t0.reset_tvars())),
            Type::ByRef(t0) => Type::ByRef(Arc::new(t0.reset_tvars())),
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

    /// return a copy of self with every TVar named in known replaced
    /// with the corresponding type
    pub fn replace_tvars(&self, known: &FxHashMap<ArcStr, Self>) -> Type {
        match self {
            Type::TVar(tv) => match known.get(&tv.name) {
                Some(t) => t.clone(),
                None => Type::TVar(tv.clone()),
            },
            Type::Bottom => Type::Bottom,
            Type::Any => Type::Any,
            Type::Primitive(p) => Type::Primitive(*p),
            Type::Ref { scope, name, params } => Type::Ref {
                scope: scope.clone(),
                name: name.clone(),
                params: Arc::from_iter(params.iter().map(|t| t.replace_tvars(known))),
            },
            Type::Array(t0) => Type::Array(Arc::new(t0.replace_tvars(known))),
            Type::ByRef(t0) => Type::ByRef(Arc::new(t0.replace_tvars(known))),
            Type::Tuple(ts) => {
                Type::Tuple(Arc::from_iter(ts.iter().map(|t| t.replace_tvars(known))))
            }
            Type::Struct(ts) => Type::Struct(Arc::from_iter(
                ts.iter().map(|(n, t)| (n.clone(), t.replace_tvars(known))),
            )),
            Type::Variant(tag, ts) => Type::Variant(
                tag.clone(),
                Arc::from_iter(ts.iter().map(|t| t.replace_tvars(known))),
            ),
            Type::Set(s) => {
                Type::Set(Arc::from_iter(s.iter().map(|t| t.replace_tvars(known))))
            }
            Type::Fn(fntyp) => Type::Fn(Arc::new(fntyp.replace_tvars(known))),
        }
    }

    /// Unbind any bound tvars, but do not unalias them.
    fn unbind_tvars(&self) {
        match self {
            Type::Bottom | Type::Any | Type::Primitive(_) | Type::Ref { .. } => (),
            Type::Array(t0) => t0.unbind_tvars(),
            Type::ByRef(t0) => t0.unbind_tvars(),
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
        }
    }

    fn first_prim_int<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        hist: &mut FxHashSet<usize>,
    ) -> Option<Typ> {
        match self {
            Type::Primitive(p) => p.iter().next(),
            Type::Set(s) => s.iter().find_map(|t| t.first_prim_int(env, hist)),
            Type::TVar(tv) => {
                tv.read().typ.read().as_ref().and_then(|t| t.first_prim_int(env, hist))
            }
            // array, tuple, and struct casting are handled directly
            Type::Bottom
            | Type::Any
            | Type::Fn(_)
            | Type::Array(_)
            | Type::Tuple(_)
            | Type::Struct(_)
            | Type::Variant(_, _)
            | Type::ByRef(_) => None,
            Type::Ref { .. } => {
                let t = self.lookup_ref(env).ok()?;
                let t_addr = (t as *const Type).addr();
                if hist.contains(&t_addr) {
                    None
                } else {
                    hist.insert(t_addr);
                    t.first_prim_int(env, hist)
                }
            }
        }
    }

    fn first_prim<C: Ctx, E: UserEvent>(&self, env: &Env<C, E>) -> Option<Typ> {
        thread_local! {
            static HIST: RefCell<FxHashSet<usize>> = RefCell::new(HashSet::default());
        }
        HIST.with_borrow_mut(|hist| {
            hist.clear();
            self.first_prim_int(env, hist)
        })
    }

    fn check_cast_int<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        hist: &mut FxHashSet<usize>,
    ) -> Result<()> {
        match self {
            Type::Primitive(_) | Type::Any => Ok(()),
            Type::Fn(_) => bail!("can't cast a value to a function"),
            Type::Bottom => bail!("can't cast a value to bottom"),
            Type::Set(s) => Ok(for t in s.iter() {
                t.check_cast_int(env, hist)?
            }),
            Type::TVar(tv) => match &*tv.read().typ.read() {
                Some(t) => t.check_cast_int(env, hist),
                None => bail!("can't cast a value to a free type variable"),
            },
            Type::Array(et) => et.check_cast_int(env, hist),
            Type::ByRef(_) => bail!("can't cast a reference"),
            Type::Tuple(ts) => Ok(for t in ts.iter() {
                t.check_cast_int(env, hist)?
            }),
            Type::Struct(ts) => Ok(for (_, t) in ts.iter() {
                t.check_cast_int(env, hist)?
            }),
            Type::Variant(_, ts) => Ok(for t in ts.iter() {
                t.check_cast_int(env, hist)?
            }),
            Type::Ref { .. } => {
                let t = self.lookup_ref(env)?;
                let t_addr = (t as *const Type).addr();
                if hist.contains(&t_addr) {
                    Ok(())
                } else {
                    hist.insert(t_addr);
                    t.check_cast_int(env, hist)
                }
            }
        }
    }

    pub fn check_cast<C: Ctx, E: UserEvent>(&self, env: &Env<C, E>) -> Result<()> {
        thread_local! {
            static HIST: RefCell<FxHashSet<usize>> = RefCell::new(FxHashSet::default());
        }
        HIST.with_borrow_mut(|hist| {
            hist.clear();
            self.check_cast_int(env, hist)
        })
    }

    fn cast_value_int<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        hist: &mut FxHashSet<usize>,
        v: Value,
    ) -> Result<Value> {
        if self.is_a_int(env, hist, &v) {
            return Ok(v);
        }
        match self {
            Type::Array(et) => match v {
                Value::Array(elts) => {
                    let va = elts
                        .iter()
                        .map(|el| et.cast_value_int(env, hist, el.clone()))
                        .collect::<Result<SmallVec<[Value; 8]>>>()?;
                    Ok(Value::Array(ValArray::from_iter_exact(va.into_iter())))
                }
                v => Ok(Value::Array([et.cast_value_int(env, hist, v)?].into())),
            },
            Type::Tuple(ts) => match v {
                Value::Array(elts) => {
                    if elts.len() != ts.len() {
                        bail!("tuple size mismatch {self} with {}", Value::Array(elts))
                    }
                    let a = ts
                        .iter()
                        .zip(elts.iter())
                        .map(|(t, el)| t.cast_value_int(env, hist, el.clone()))
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
                        Ok((true, true)),
                        |acc: Result<_>, ((fname, t), v)| {
                            let (kok, ok) = acc?;
                            let (name, v) = match v {
                                Value::Array(a) => match (&a[0], &a[1]) {
                                    (Value::String(n), v) => (n, v),
                                    _ => unreachable!(),
                                },
                                _ => unreachable!(),
                            };
                            Ok((
                                kok && name == fname,
                                ok && kok
                                    && t.contains(
                                        env,
                                        &Type::Primitive(Typ::get(v).into()),
                                    )?,
                            ))
                        },
                    )?;
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
                                        t.cast_value_int(env, hist, a[1].clone())?,
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
                        let a = iter::once(&Type::Primitive(Typ::String.into()))
                            .chain(ts.iter())
                            .zip(elts.iter())
                            .map(|(t, v)| t.cast_value_int(env, hist, v.clone()))
                            .collect::<Result<SmallVec<[Value; 8]>>>()?;
                        Ok(Value::Array(ValArray::from_iter_exact(a.into_iter())))
                    } else if ts.len() == elts.len() {
                        let mut a = ts
                            .iter()
                            .zip(elts.iter())
                            .map(|(t, v)| t.cast_value_int(env, hist, v.clone()))
                            .collect::<Result<SmallVec<[Value; 8]>>>()?;
                        a.insert(0, Value::String(tag.clone()));
                        Ok(Value::Array(ValArray::from_iter_exact(a.into_iter())))
                    } else {
                        bail!("variant length mismatch")
                    }
                }
                v => bail!("can't cast {v} to {self}"),
            },
            Type::Ref { .. } => self.lookup_ref(env)?.cast_value_int(env, hist, v),
            t => match t.first_prim(env) {
                None => bail!("empty or non primitive cast"),
                Some(t) => Ok(v
                    .clone()
                    .cast(t)
                    .ok_or_else(|| anyhow!("can't cast {v} to {t}"))?),
            },
        }
    }

    pub fn cast_value<C: Ctx, E: UserEvent>(&self, env: &Env<C, E>, v: Value) -> Value {
        thread_local! {
            static HIST: RefCell<FxHashSet<usize>> = RefCell::new(HashSet::default());
        }
        HIST.with_borrow_mut(|hist| {
            hist.clear();
            match self.cast_value_int(env, hist, v) {
                Ok(v) => v,
                Err(e) => Value::Error(e.to_string().into()),
            }
        })
    }

    fn is_a_int<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        hist: &mut FxHashSet<usize>,
        v: &Value,
    ) -> bool {
        match self {
            Type::Ref { .. } => match self.lookup_ref(env) {
                Err(_) => false,
                Ok(t) => {
                    let t_addr = (t as *const Type).addr();
                    !hist.contains(&t_addr) && {
                        hist.insert(t_addr);
                        t.is_a_int(env, hist, v)
                    }
                }
            },
            Type::Primitive(t) => t.contains(Typ::get(&v)),
            Type::Any => true,
            Type::Array(et) => match v {
                Value::Array(a) => a.iter().all(|v| et.is_a_int(env, hist, v)),
                _ => false,
            },
            Type::ByRef(_) => matches!(v, Value::U64(_) | Value::V64(_)),
            Type::Tuple(ts) => match v {
                Value::Array(elts) => {
                    elts.len() == ts.len()
                        && ts
                            .iter()
                            .zip(elts.iter())
                            .all(|(t, v)| t.is_a_int(env, hist, v))
                }
                _ => false,
            },
            Type::Struct(ts) => match v {
                Value::Array(elts) => {
                    elts.len() == ts.len()
                        && ts.iter().zip(elts.iter()).all(|((n, t), v)| match v {
                            Value::Array(a) if a.len() == 2 => match &a[..] {
                                [Value::String(key), v] => {
                                    n == key && t.is_a_int(env, hist, v)
                                }
                                _ => false,
                            },
                            _ => false,
                        })
                }
                _ => false,
            },
            Type::Variant(tag, ts) if ts.len() == 0 => match &v {
                Value::String(s) => s == tag,
                _ => false,
            },
            Type::Variant(tag, ts) => match &v {
                Value::Array(elts) => {
                    ts.len() + 1 == elts.len()
                        && match &elts[0] {
                            Value::String(s) => s == tag,
                            _ => false,
                        }
                        && ts
                            .iter()
                            .zip(elts[1..].iter())
                            .all(|(t, v)| t.is_a_int(env, hist, v))
                }
                _ => false,
            },
            Type::TVar(tv) => match &*tv.read().typ.read() {
                None => true,
                Some(t) => t.is_a_int(env, hist, v),
            },
            Type::Fn(_) => match v {
                Value::U64(_) => true,
                _ => false,
            },
            Type::Bottom => true,
            Type::Set(ts) => ts.iter().any(|t| t.is_a_int(env, hist, v)),
        }
    }

    /// return true if v is structurally compatible with the type
    pub fn is_a<C: Ctx, E: UserEvent>(&self, env: &Env<C, E>, v: &Value) -> bool {
        thread_local! {
            static HIST: RefCell<FxHashSet<usize>> = RefCell::new(HashSet::default());
        }
        HIST.with_borrow_mut(|hist| {
            hist.clear();
            self.is_a_int(env, hist, v)
        })
    }

    pub fn is_bot(&self) -> bool {
        match self {
            Type::Bottom => true,
            Type::Any
            | Type::TVar(_)
            | Type::Primitive(_)
            | Type::Ref { .. }
            | Type::Fn(_)
            | Type::Array(_)
            | Type::ByRef(_)
            | Type::Tuple(_)
            | Type::Struct(_)
            | Type::Variant(_, _)
            | Type::Set(_) => false,
        }
    }

    pub fn with_deref<R, F: FnOnce(Option<&Self>) -> R>(&self, f: F) -> R {
        match self {
            Self::Bottom
            | Self::Any
            | Self::Primitive(_)
            | Self::Fn(_)
            | Self::Set(_)
            | Self::Array(_)
            | Self::ByRef(_)
            | Self::Tuple(_)
            | Self::Struct(_)
            | Self::Variant(_, _)
            | Self::Ref { .. } => f(Some(self)),
            Self::TVar(tv) => f(tv.read().typ.read().as_ref()),
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
                    Some(Type::Any) => return Type::Any,
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
            Type::Bottom | Type::Any | Type::Primitive(_) => self.clone(),
            Type::Ref { scope, name, params } => {
                let params = Arc::from_iter(params.iter().map(|t| t.normalize()));
                Type::Ref { scope: scope.clone(), name: name.clone(), params }
            }
            Type::TVar(tv) => Type::TVar(tv.normalize()),
            Type::Set(s) => Self::flatten_set(s.iter().map(|t| t.normalize())),
            Type::Array(t) => Type::Array(Arc::new(t.normalize())),
            Type::ByRef(t) => Type::ByRef(Arc::new(t.normalize())),
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
            (
                Type::Ref { scope: s0, name: r0, params: a0 },
                Type::Ref { scope: s1, name: r1, params: a1 },
            ) => {
                if s0 == s1 && r0 == r1 && a0 == a1 {
                    Some(Type::Ref {
                        scope: s0.clone(),
                        name: r0.clone(),
                        params: a0.clone(),
                    })
                } else {
                    None
                }
            }
            (Type::Ref { .. }, _) | (_, Type::Ref { .. }) => None,
            (Type::Bottom, t) | (t, Type::Bottom) => Some(t.clone()),
            (Type::Any, _) | (_, Type::Any) => Some(Type::Any),
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
            (Type::ByRef(t0), Type::ByRef(t1)) => {
                t0.merge(t1).map(|t| Type::ByRef(Arc::new(t)))
            }
            (Type::ByRef(_), _) | (_, Type::ByRef(_)) => None,
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
                        .collect::<Option<SmallVec<[Type; 8]>>>()?;
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
                        .collect::<Option<SmallVec<[Type; 8]>>>()?;
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
                        .collect::<Option<SmallVec<[(ArcStr, Type); 8]>>>()?;
                    Some(Type::Struct(Arc::from_iter(t)))
                } else {
                    None
                }
            }
            (Type::TVar(tv0), Type::TVar(tv1)) if tv0.name == tv1.name && tv0 == tv1 => {
                Some(Type::TVar(tv0.clone()))
            }
            (Type::TVar(tv), t) => {
                tv.read().typ.read().as_ref().and_then(|tv| tv.merge(t))
            }
            (t, Type::TVar(tv)) => {
                tv.read().typ.read().as_ref().and_then(|tv| t.merge(tv))
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

    pub fn scope_refs(&self, scope: &ModPath) -> Type {
        match self {
            Type::Bottom => Type::Bottom,
            Type::Any => Type::Any,
            Type::Primitive(s) => Type::Primitive(*s),
            Type::Array(t0) => Type::Array(Arc::new(t0.scope_refs(scope))),
            Type::ByRef(t) => Type::ByRef(Arc::new(t.scope_refs(scope))),
            Type::Tuple(ts) => {
                let i = ts.iter().map(|t| t.scope_refs(scope));
                Type::Tuple(Arc::from_iter(i))
            }
            Type::Variant(tag, ts) => {
                let i = ts.iter().map(|t| t.scope_refs(scope));
                Type::Variant(tag.clone(), Arc::from_iter(i))
            }
            Type::Struct(ts) => {
                let i = ts.iter().map(|(n, t)| (n.clone(), t.scope_refs(scope)));
                Type::Struct(Arc::from_iter(i))
            }
            Type::TVar(tv) => match tv.read().typ.read().as_ref() {
                None => Type::TVar(TVar::empty_named(tv.name.clone())),
                Some(typ) => {
                    let typ = typ.scope_refs(scope);
                    Type::TVar(TVar::named(tv.name.clone(), typ))
                }
            },
            Type::Ref { scope: _, name, params } => {
                let params = Arc::from_iter(params.iter().map(|t| t.scope_refs(scope)));
                Type::Ref { scope: scope.clone(), name: name.clone(), params }
            }
            Type::Set(ts) => {
                Type::Set(Arc::from_iter(ts.iter().map(|t| t.scope_refs(scope))))
            }
            Type::Fn(f) => {
                let vargs = f.vargs.as_ref().map(|t| t.scope_refs(scope));
                let rtype = f.rtype.scope_refs(scope);
                let args = Arc::from_iter(f.args.iter().map(|a| FnArgType {
                    label: a.label.clone(),
                    typ: a.typ.scope_refs(scope),
                }));
                let mut cres: SmallVec<[(TVar, Type); 4]> = smallvec![];
                for (tv, tc) in f.constraints.read().iter() {
                    let tv = tv.scope_refs(scope);
                    let tc = tc.scope_refs(scope);
                    cres.push((tv, tc));
                }
                Type::Fn(Arc::new(FnType {
                    args,
                    rtype,
                    constraints: Arc::new(RwLock::new(cres.into_iter().collect())),
                    vargs,
                }))
            }
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bottom => write!(f, "_"),
            Self::Any => write!(f, "Any"),
            Self::Ref { scope: _, name, params } => {
                write!(f, "{name}")?;
                if !params.is_empty() {
                    write!(f, "<")?;
                    for (i, t) in params.iter().enumerate() {
                        write!(f, "{t}")?;
                        if i < params.len() - 1 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, ">")?;
                }
                Ok(())
            }
            Self::TVar(tv) => write!(f, "{tv}"),
            Self::Fn(t) => write!(f, "{t}"),
            Self::Array(t) => write!(f, "Array<{t}>"),
            Self::ByRef(t) => write!(f, "&{t}"),
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
                let replace = PRINT_FLAGS.get().contains(PrintFlag::ReplacePrims);
                if replace && *s == Typ::number() {
                    write!(f, "Number")
                } else if replace && *s == Typ::float() {
                    write!(f, "Float")
                } else if replace && *s == Typ::real() {
                    write!(f, "Real")
                } else if replace && *s == Typ::integer() {
                    write!(f, "Int")
                } else if replace && *s == Typ::unsigned_integer() {
                    write!(f, "Uint")
                } else if replace && *s == Typ::signed_integer() {
                    write!(f, "Sint")
                } else if s.len() == 0 {
                    write!(f, "[]")
                } else if s.len() == 1 {
                    write!(f, "{}", s.iter().next().unwrap())
                } else {
                    let mut s = *s;
                    macro_rules! builtin {
                        ($set:expr, $name:literal) => {
                            if replace && s.contains($set) {
                                s.remove($set);
                                write!(f, $name)?;
                                if !s.is_empty() {
                                    write!(f, ", ")?
                                }
                            }
                        };
                    }
                    write!(f, "[")?;
                    builtin!(Typ::number(), "Number");
                    builtin!(Typ::real(), "Real");
                    builtin!(Typ::float(), "Float");
                    builtin!(Typ::integer(), "Int");
                    builtin!(Typ::unsigned_integer(), "Uint");
                    builtin!(Typ::signed_integer(), "Sint");
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
