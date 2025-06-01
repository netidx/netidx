use crate::{
    expr::ModPath,
    typ::{FnType, PrintFlag, Type, PRINT_FLAGS},
};
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

pub(super) fn would_cycle_inner(addr: usize, t: &Type) -> bool {
    match t {
        Type::Primitive(_) | Type::Bottom | Type::Ref { .. } => false,
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

#[derive(Debug)]
pub struct TVarInnerInner {
    pub(super) id: TVarId,
    pub(super) typ: Arc<RwLock<Option<Type>>>,
}

#[derive(Debug)]
pub struct TVarInner {
    pub name: ArcStr,
    pub(super) typ: RwLock<TVarInnerInner>,
}

#[derive(Debug, Clone)]
pub struct TVar(Arc<TVarInner>);

impl fmt::Display for TVar {
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

impl Default for TVar {
    fn default() -> Self {
        Self::empty_named(ArcStr::from(format_compact!("_{}", TVarId::new().0).as_str()))
    }
}

impl Deref for TVar {
    type Target = TVarInner;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl PartialEq for TVar {
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

impl Eq for TVar {}

impl PartialOrd for TVar {
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

impl Ord for TVar {
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

impl TVar {
    pub fn scope_refs(&self, scope: &ModPath) -> Self {
        match Type::TVar(self.clone()).scope_refs(scope) {
            Type::TVar(tv) => tv,
            _ => unreachable!(),
        }
    }

    pub fn empty_named(name: ArcStr) -> Self {
        Self(Arc::new(TVarInner {
            name,
            typ: RwLock::new(TVarInnerInner {
                id: TVarId::new(),
                typ: Arc::new(RwLock::new(None)),
            }),
        }))
    }

    pub fn named(name: ArcStr, typ: Type) -> Self {
        Self(Arc::new(TVarInner {
            name,
            typ: RwLock::new(TVarInnerInner {
                id: TVarId::new(),
                typ: Arc::new(RwLock::new(Some(typ))),
            }),
        }))
    }

    pub fn read(&self) -> RwLockReadGuard<TVarInnerInner> {
        self.typ.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<TVarInnerInner> {
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

    pub fn unbind(&self) {
        *self.read().typ.write() = None
    }

    pub(super) fn would_cycle(&self, t: &Type) -> bool {
        let addr = Arc::as_ptr(&self.read().typ).addr();
        would_cycle_inner(addr, t)
    }
}
