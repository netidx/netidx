use crate::{
    env::Env,
    expr::ModPath,
    typ::{FnType, NoRefs, PrintFlag, Refs, Type, TypeMark, PRINT_FLAGS},
    Ctx, UserEvent,
};
use anyhow::{bail, Result};
use arcstr::ArcStr;
use compact_str::format_compact;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{
    cmp::{Eq, PartialEq},
    fmt::{self, Debug},
    hash::Hash,
    ops::Deref,
};
use triomphe::Arc;

atomic_id!(TVarId);

#[derive(Debug)]
pub struct TVarInnerInner<T: TypeMark> {
    pub(super) id: TVarId,
    pub(super) typ: Arc<RwLock<Option<Type<T>>>>,
}

#[derive(Debug)]
pub struct TVarInner<T: TypeMark> {
    pub name: ArcStr,
    pub(super) typ: RwLock<TVarInnerInner<T>>,
}

#[derive(Debug, Clone)]
pub struct TVar<T: TypeMark>(Arc<TVarInner<T>>);

impl<T: TypeMark> fmt::Display for TVar<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !PRINT_FLAGS.get().contains(PrintFlag::DerefTVars) {
            write!(f, "'{}", self.name)
        } else {
            write!(f, "'{}: ", self.name)?;
            match &*self.read().typ.read() {
                Some(t) => write!(f, "{t}"),
                None => write!(f, "unbound"),
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
                typ: Arc::new(RwLock::new(None)),
            }),
        }))
    }

    pub fn named(name: ArcStr, typ: Type<T>) -> Self {
        Self(Arc::new(TVarInner {
            name,
            typ: RwLock::new(TVarInnerInner {
                id: TVarId::new(),
                typ: Arc::new(RwLock::new(Some(typ))),
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

    /// copy self from other
    pub fn copy(&self, other: &Self) {
        let s = self.read();
        let o = other.read();
        *s.typ.write() = o.typ.read().clone();
    }

    pub fn normalize(&self) -> Self {
        match &mut *self.read().typ.write() {
            None => (),
            Some(t) => {
                *t = t.normalize();
            }
        }
        self.clone()
    }
}

impl TVar<NoRefs> {
    pub fn unbind(&self) {
        *self.read().typ.write() = None
    }
}

pub(super) fn would_cycle_inner(addr: usize, t: &Type<NoRefs>) -> bool {
    match t {
        Type::Primitive(_) | Type::Bottom(_) | Type::Ref(_) => false,
        Type::TVar(t) => {
            Arc::as_ptr(&t.read().typ).addr() == addr
                || match &*t.read().typ.read() {
                    None => false,
                    Some(t) => would_cycle_inner(addr, t),
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
                || constraints.read().iter().any(|a| {
                    Arc::as_ptr(&a.0.read().typ).addr() == addr
                        || would_cycle_inner(addr, &a.1)
                })
        }
    }
}

impl TVar<NoRefs> {
    pub(super) fn would_cycle(&self, t: &Type<NoRefs>) -> bool {
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
