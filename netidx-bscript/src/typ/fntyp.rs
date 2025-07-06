use crate::{
    env::Env,
    expr::ModPath,
    typ::{TVar, Type},
    Ctx, UserEvent,
};
use anyhow::{bail, Result};
use arcstr::ArcStr;
use fxhash::FxHashMap;
use parking_lot::RwLock;
use std::{
    cell::RefCell,
    cmp::{Eq, Ordering, PartialEq},
    collections::HashMap,
    fmt::{self, Debug},
};
use triomphe::Arc;

use super::AndAc;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FnArgType {
    pub label: Option<(ArcStr, bool)>,
    pub typ: Type,
}

#[derive(Debug, Clone)]
pub struct FnType {
    pub args: Arc<[FnArgType]>,
    pub vargs: Option<Type>,
    pub rtype: Type,
    pub constraints: Arc<RwLock<Vec<(TVar, Type)>>>,
}

impl PartialEq for FnType {
    fn eq(&self, other: &Self) -> bool {
        let Self { args: args0, vargs: vargs0, rtype: rtype0, constraints: constraints0 } =
            self;
        let Self { args: args1, vargs: vargs1, rtype: rtype1, constraints: constraints1 } =
            other;
        args0 == args1
            && vargs0 == vargs1
            && rtype0 == rtype1
            && &*constraints0.read() == &*constraints1.read()
    }
}

impl Eq for FnType {}

impl PartialOrd for FnType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use std::cmp::Ordering;
        let Self { args: args0, vargs: vargs0, rtype: rtype0, constraints: constraints0 } =
            self;
        let Self { args: args1, vargs: vargs1, rtype: rtype1, constraints: constraints1 } =
            other;
        match args0.partial_cmp(&args1) {
            Some(Ordering::Equal) => match vargs0.partial_cmp(vargs1) {
                Some(Ordering::Equal) => match rtype0.partial_cmp(rtype1) {
                    Some(Ordering::Equal) => {
                        constraints0.read().partial_cmp(&*constraints1.read())
                    }
                    r => r,
                },
                r => r,
            },
            r => r,
        }
    }
}

impl Ord for FnType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Default for FnType {
    fn default() -> Self {
        Self {
            args: Arc::from_iter([]),
            vargs: None,
            rtype: Default::default(),
            constraints: Arc::new(RwLock::new(vec![])),
        }
    }
}

impl FnType {
    pub fn scope_refs(&self, scope: &ModPath) -> FnType {
        let typ = Type::Fn(Arc::new(self.clone()));
        match typ.scope_refs(scope) {
            Type::Fn(f) => (*f).clone(),
            _ => unreachable!(),
        }
    }

    pub(super) fn normalize(&self) -> Self {
        let Self { args, vargs, rtype, constraints } = self;
        let args = Arc::from_iter(
            args.iter()
                .map(|a| FnArgType { label: a.label.clone(), typ: a.typ.normalize() }),
        );
        let vargs = vargs.as_ref().map(|t| t.normalize());
        let rtype = rtype.normalize();
        let constraints = Arc::new(RwLock::new(
            constraints
                .read()
                .iter()
                .map(|(tv, t)| (tv.clone(), t.normalize()))
                .collect(),
        ));
        FnType { args, vargs, rtype, constraints }
    }

    pub fn unbind_tvars(&self) {
        let FnType { args, vargs, rtype, constraints } = self;
        for arg in args.iter() {
            arg.typ.unbind_tvars()
        }
        if let Some(t) = vargs {
            t.unbind_tvars()
        }
        rtype.unbind_tvars();
        for (tv, tc) in constraints.read().iter() {
            tv.unbind();
            tc.unbind_tvars()
        }
    }

    pub fn constrain_known(&self) {
        thread_local! {
            static KNOWN: RefCell<FxHashMap<ArcStr, TVar>> = RefCell::new(HashMap::default());
        }
        KNOWN.with_borrow_mut(|known| {
            known.clear();
            self.collect_tvars(known);
            let mut constraints = self.constraints.write();
            for (name, tv) in known.drain() {
                if let Some(t) = tv.read().typ.read().as_ref() {
                    if !constraints.iter().any(|(tv, _)| tv.name == name) {
                        t.bind_as(&Type::Any);
                        constraints.push((tv.clone(), t.normalize()));
                    }
                }
            }
        });
    }

    pub fn reset_tvars(&self) -> Self {
        let FnType { args, vargs, rtype, constraints } = self;
        let args = Arc::from_iter(
            args.iter()
                .map(|a| FnArgType { label: a.label.clone(), typ: a.typ.reset_tvars() }),
        );
        let vargs = vargs.as_ref().map(|t| t.reset_tvars());
        let rtype = rtype.reset_tvars();
        let constraints = Arc::new(RwLock::new(
            constraints
                .read()
                .iter()
                .map(|(tv, tc)| (TVar::empty_named(tv.name.clone()), tc.reset_tvars()))
                .collect(),
        ));
        FnType { args, vargs, rtype, constraints }
    }

    pub fn replace_tvars(&self, known: &FxHashMap<ArcStr, Type>) -> Self {
        let FnType { args, vargs, rtype, constraints } = self;
        let args = Arc::from_iter(args.iter().map(|a| FnArgType {
            label: a.label.clone(),
            typ: a.typ.replace_tvars(known),
        }));
        let vargs = vargs.as_ref().map(|t| t.replace_tvars(known));
        let rtype = rtype.replace_tvars(known);
        let constraints = constraints.clone();
        FnType { args, vargs, rtype, constraints }
    }

    /// replace automatically constrained type variables with their
    /// constraint type. This is only useful for making nicer display
    /// types in IDEs and shells.
    pub fn replace_auto_constrained(&self) -> Self {
        thread_local! {
            static KNOWN: RefCell<FxHashMap<ArcStr, Type>> = RefCell::new(HashMap::default());
        }
        KNOWN.with_borrow_mut(|known| {
            known.clear();
            let Self { args, vargs, rtype, constraints } = self;
            let constraints: Vec<(TVar, Type)> = constraints
                .read()
                .iter()
                .filter_map(|(tv, ct)| {
                    if tv.name.starts_with("_") {
                        known.insert(tv.name.clone(), ct.clone());
                        None
                    } else {
                        Some((tv.clone(), ct.clone()))
                    }
                })
                .collect();
            let constraints = Arc::new(RwLock::new(constraints));
            let args = Arc::from_iter(args.iter().map(|FnArgType { label, typ }| {
                FnArgType { label: label.clone(), typ: typ.replace_tvars(&known) }
            }));
            let vargs = vargs.as_ref().map(|t| t.replace_tvars(&known));
            let rtype = rtype.replace_tvars(&known);
            Self { args, vargs, rtype, constraints }
        })
    }

    pub fn has_unbound(&self) -> bool {
        let FnType { args, vargs, rtype, constraints } = self;
        args.iter().any(|a| a.typ.has_unbound())
            || vargs.as_ref().map(|t| t.has_unbound()).unwrap_or(false)
            || rtype.has_unbound()
            || constraints
                .read()
                .iter()
                .any(|(tv, tc)| tv.read().typ.read().is_none() || tc.has_unbound())
    }

    pub fn bind_as(&self, t: &Type) {
        let FnType { args, vargs, rtype, constraints } = self;
        for a in args.iter() {
            a.typ.bind_as(t)
        }
        if let Some(va) = vargs.as_ref() {
            va.bind_as(t)
        }
        rtype.bind_as(t);
        for (tv, tc) in constraints.read().iter() {
            let tv = tv.read();
            let mut tv = tv.typ.write();
            if tv.is_none() {
                *tv = Some(t.clone())
            }
            tc.bind_as(t)
        }
    }

    pub fn alias_tvars(&self, known: &mut FxHashMap<ArcStr, TVar>) {
        let FnType { args, vargs, rtype, constraints } = self;
        for arg in args.iter() {
            arg.typ.alias_tvars(known)
        }
        if let Some(vargs) = vargs {
            vargs.alias_tvars(known)
        }
        rtype.alias_tvars(known);
        for (tv, tc) in constraints.read().iter() {
            Type::TVar(tv.clone()).alias_tvars(known);
            tc.alias_tvars(known);
        }
    }

    pub fn collect_tvars(&self, known: &mut FxHashMap<ArcStr, TVar>) {
        let FnType { args, vargs, rtype, constraints } = self;
        for arg in args.iter() {
            arg.typ.collect_tvars(known)
        }
        if let Some(vargs) = vargs {
            vargs.collect_tvars(known)
        }
        rtype.collect_tvars(known);
        for (tv, tc) in constraints.read().iter() {
            Type::TVar(tv.clone()).collect_tvars(known);
            tc.collect_tvars(known);
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
        HIST.with_borrow_mut(|hist| self.contains_int(env, hist, t))
    }

    pub(super) fn contains_int<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        hist: &mut FxHashMap<(usize, usize), bool>,
        t: &Self,
    ) -> Result<bool> {
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
                    None => return Ok(false),
                    Some(o) => {
                        if !o.typ.contains_int(env, hist, &a.typ)? {
                            return Ok(false);
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
                            return Ok(false);
                        }
                    }
                },
            }
        }
        let slen = self.args.len() - sul;
        let tlen = t.args.len() - tul;
        Ok(slen == tlen
            && self
                .constraints
                .read()
                .iter()
                .map(|(tv, tc)| tc.contains_int(env, hist, &Type::TVar(tv.clone())))
                .collect::<Result<AndAc>>()?
                .0
            && t.constraints
                .read()
                .iter()
                .map(|(tv, tc)| tc.contains_int(env, hist, &Type::TVar(tv.clone())))
                .collect::<Result<AndAc>>()?
                .0
            && t.args[tul..]
                .iter()
                .zip(self.args[sul..].iter())
                .map(|(t, s)| t.typ.contains_int(env, hist, &s.typ))
                .collect::<Result<AndAc>>()?
                .0
            && match (&t.vargs, &self.vargs) {
                (Some(tv), Some(sv)) => tv.contains_int(env, hist, sv)?,
                (None, None) => true,
                (_, _) => false,
            }
            && self.rtype.contains_int(env, hist, &t.rtype)?)
    }

    pub fn check_contains<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        other: &Self,
    ) -> Result<()> {
        if !self.contains(env, other)? {
            bail!("Fn type mismatch {self} does not contain {other}")
        }
        Ok(())
    }

    /// Return true if function signatures match. This is contains,
    /// but does not allow labeled argument subtyping.
    pub fn sigmatch<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        other: &Self,
    ) -> Result<bool> {
        let Self { args: args0, vargs: vargs0, rtype: rtype0, constraints: constraints0 } =
            self;
        let Self { args: args1, vargs: vargs1, rtype: rtype1, constraints: constraints1 } =
            other;
        Ok(args0.len() == args1.len()
            && args0
                .iter()
                .zip(args1.iter())
                .map(
                    |(a0, a1)| Ok(a0.label == a1.label && a0.typ.contains(env, &a1.typ)?),
                )
                .collect::<Result<AndAc>>()?
                .0
            && match (vargs0, vargs1) {
                (None, None) => true,
                (None, _) | (_, None) => false,
                (Some(t0), Some(t1)) => t0.contains(env, t1)?,
            }
            && rtype0.contains(env, rtype1)?
            && constraints0
                .read()
                .iter()
                .map(|(tv, tc)| tc.contains(env, &Type::TVar(tv.clone())))
                .collect::<Result<AndAc>>()?
                .0
            && constraints1
                .read()
                .iter()
                .map(|(tv, tc)| tc.contains(env, &Type::TVar(tv.clone())))
                .collect::<Result<AndAc>>()?
                .0)
    }

    pub fn check_sigmatch<C: Ctx, E: UserEvent>(
        &self,
        env: &Env<C, E>,
        other: &Self,
    ) -> Result<()> {
        if !self.sigmatch(env, other)? {
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

impl fmt::Display for FnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let constraints = self.constraints.read();
        if constraints.len() == 0 {
            write!(f, "fn(")?;
        } else {
            write!(f, "fn<")?;
            for (i, (tv, t)) in constraints.iter().enumerate() {
                write!(f, "{tv}: {t}")?;
                if i < constraints.len() - 1 {
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
