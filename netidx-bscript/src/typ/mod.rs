use crate::{env::Env, expr::ModPath, Ctx, UserEvent};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use enumflags2::{bitflags, BitFlags};
use fxhash::FxHashMap;
use netidx::{
    publisher::{Typ, Value},
    utils::Either,
};
use netidx_netproto::valarray::ValArray;
use parking_lot::RwLock;
use smallvec::{smallvec, SmallVec};
use std::{
    cell::Cell,
    cmp::{Eq, PartialEq},
    collections::hash_map::Entry,
    fmt::{self, Debug},
    hash::Hash,
    iter,
    marker::PhantomData,
};
use triomphe::Arc;

mod fntyp;
mod tval;
mod tvar;
pub use fntyp::{FnArgType, FnType};
pub use tval::TVal;
use tvar::would_cycle_inner;
pub use tvar::TVar;

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

#[derive(Debug, Clone, Copy)]
#[bitflags]
#[repr(u64)]
pub enum PrintFlag {
    /// Dereference type variables and print both the tvar name and
    /// the bound type or "unbound".
    DerefTVars,
    /// Replace common primitive with shorter type names as defined in
    /// core. e.g. Any, instead of the set of every primitive type.
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
            Self::TVar(tv) => tv.read().typ.read().is_some(),
            Self::Ref(_) => unreachable!(),
        }
    }

    pub fn check_contains(&self, t: &Self) -> Result<()> {
        if self.contains(t) {
            Ok(())
        } else {
            format_with_flags(PrintFlag::DerefTVars | PrintFlag::ReplacePrims, || {
                bail!("type mismatch {self} does not contain {t}")
            })
        }
    }

    pub fn contains(&self, t: &Self) -> bool {
        match (self, t) {
            (Self::TVar(t0), Self::Bottom(_)) => {
                if let Some(_) = &*t0.read().typ.read() {
                    return true;
                }
                *t0.read().typ.write() = Some(Self::Bottom(PhantomData));
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
                        return true;
                    }
                    let t0i = t0.typ.read();
                    let t1i = t1.typ.read();
                    match (&*t0i, &*t1i) {
                        (Some(t0), Some(t1)) => return t0.contains(&*t1),
                        (None, None) => {
                            if would_cycle_inner(addr, tt1) {
                                return true;
                            }
                            Act::LeftAlias
                        }
                        (Some(_), None) => {
                            if would_cycle_inner(addr, tt1) {
                                return true;
                            }
                            Act::RightCopy
                        }
                        (None, Some(_)) => {
                            if would_cycle_inner(addr, tt1) {
                                return true;
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
                true
            }
            (Self::TVar(t0), t1) if !t0.would_cycle(t1) => {
                if let Some(t0) = &*t0.read().typ.read() {
                    return t0.contains(t1);
                }
                *t0.read().typ.write() = Some(t1.clone());
                true
            }
            (t0, Self::TVar(t1)) if !t1.would_cycle(t0) => {
                if let Some(t1) = &*t1.read().typ.read() {
                    return t0.contains(t1);
                }
                *t1.read().typ.write() = Some(t0.clone());
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
            (t @ Type::Array(t0), u @ Type::Array(t1)) => {
                if t0 == t1 {
                    Type::Array(t0.clone())
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
                match (&*tv0.read().typ.read(), &*tv1.read().typ.read()) {
                    (None, _) | (_, None) => Type::TVar(tv0.clone()),
                    (Some(t0), Some(t1)) => t0.diff_int(t1),
                }
            }
            (Type::TVar(tv), t1) => match &*tv.read().typ.read() {
                None => Type::TVar(tv.clone()),
                Some(t0) => t0.diff_int(t1),
            },
            (t0, Type::TVar(tv)) => match &*tv.read().typ.read() {
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

    pub fn collect_tvars(&self, known: &mut FxHashMap<ArcStr, TVar<NoRefs>>) {
        match self {
            Type::Bottom(_) | Type::Primitive(_) => (),
            Type::Ref(_) => unreachable!(),
            Type::Array(t) => t.collect_tvars(known),
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

    pub fn has_unbound(&self) -> bool {
        match self {
            Type::Bottom(_) | Type::Primitive(_) => false,
            Type::Ref(_) => unreachable!(),
            Type::Array(t0) => t0.has_unbound(),
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
            Type::Bottom(_) | Type::Primitive(_) => (),
            Type::Ref(_) => unreachable!(),
            Type::Array(t0) => t0.bind_as(t),
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

    /// return a copy of self with every TVar named in known replaced
    /// with the corresponding type
    pub fn replace_tvars(&self, known: &FxHashMap<ArcStr, Self>) -> Type<NoRefs> {
        match self {
            Type::TVar(tv) => match known.get(&tv.name) {
                Some(t) => t.clone(),
                None => Type::TVar(tv.clone()),
            },
            Type::Bottom(_) => Type::Bottom(PhantomData),
            Type::Primitive(p) => Type::Primitive(*p),
            Type::Ref(_) => unreachable!(),
            Type::Array(t0) => Type::Array(Arc::new(t0.replace_tvars(known))),
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

    fn first_prim(&self) -> Option<Typ> {
        match self {
            Type::Primitive(p) => p.iter().next(),
            Type::Bottom(_) => None,
            Type::Ref(_) => unreachable!(),
            Type::Fn(_) => None,
            Type::Set(s) => s.iter().find_map(|t| t.first_prim()),
            Type::TVar(tv) => tv.read().typ.read().as_ref().and_then(|t| t.first_prim()),
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
            Type::TVar(tv) => match &*tv.read().typ.read() {
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

    /// return true if v is structurally compatible with the type
    pub fn is_a(&self, v: &Value) -> bool {
        match self {
            Type::Primitive(t) => t.contains(Typ::get(&v)),
            Type::Array(et) => match v {
                Value::Array(a) => a.iter().all(|v| et.is_a(v)),
                _ => false,
            },
            Type::Tuple(ts) => match v {
                Value::Array(elts) => {
                    elts.len() == ts.len()
                        && ts.iter().zip(elts.iter()).all(|(t, v)| t.is_a(v))
                }
                _ => false,
            },
            Type::Struct(ts) => match v {
                Value::Array(elts) => {
                    elts.len() == ts.len()
                        && ts.iter().zip(elts.iter()).all(|((n, t), v)| match v {
                            Value::Array(a) if a.len() == 2 => match &a[..] {
                                [Value::String(key), v] => n == key && t.is_a(v),
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
                        && ts.iter().zip(elts[1..].iter()).all(|(t, v)| t.is_a(v))
                }
                _ => false,
            },
            Type::TVar(tv) => match &*tv.read().typ.read() {
                None => true,
                Some(t) => t.is_a(v),
            },
            Type::Fn(_) => match v {
                Value::U64(_) => true,
                _ => false,
            },
            Type::Bottom(_) | Type::Ref(_) => true,
            Type::Set(ts) => ts.iter().any(|t| t.is_a(v)),
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
            Type::TVar(tv) => match tv.read().typ.read().as_ref() {
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
                for (tv, tc) in f.constraints.read().iter() {
                    let tv = tv.resolve_typerefs(scope, env)?;
                    let tc = tc.resolve_typerefs(scope, env)?;
                    cres.push((tv, tc));
                }
                Ok(Type::Fn(Arc::new(FnType {
                    args: Arc::from_iter(res),
                    rtype,
                    constraints: Arc::new(RwLock::new(cres.into_iter().collect())),
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
                let replace = PRINT_FLAGS.get().contains(PrintFlag::ReplacePrims);
                if replace && *s == Typ::any() {
                    write!(f, "Any")
                } else if replace && *s == Typ::number() {
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
