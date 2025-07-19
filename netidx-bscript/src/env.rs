use crate::{
    expr::{Arg, ModPath},
    typ::{FnType, TVar, Type},
    BindId, Ctx, InitFn, LambdaId, UserEvent,
};
use anyhow::{bail, Result};
use arcstr::ArcStr;
use compact_str::CompactString;
use fxhash::{FxHashMap, FxHashSet};
use immutable_chunkmap::{map::MapS as Map, set::SetS as Set};
use netidx::path::Path;
use std::{cell::RefCell, fmt, iter, ops::Bound, sync::Weak};
use triomphe::Arc;

pub struct LambdaDef<C: Ctx, E: UserEvent> {
    pub id: LambdaId,
    pub env: Env<C, E>,
    pub scope: ModPath,
    pub argspec: Arc<[Arg]>,
    pub typ: Arc<FnType>,
    pub init: InitFn<C, E>,
}

impl<C: Ctx, E: UserEvent> fmt::Debug for LambdaDef<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LambdaDef({:?})", self.id)
    }
}

pub struct Bind {
    pub id: BindId,
    pub export: bool,
    pub typ: Type,
    pub doc: Option<ArcStr>,
    pub scope: ModPath,
    pub name: CompactString,
}

impl fmt::Debug for Bind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bind {{ id: {:?}, export: {} }}", self.id, self.export,)
    }
}

impl Clone for Bind {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            scope: self.scope.clone(),
            name: self.name.clone(),
            doc: self.doc.clone(),
            export: self.export,
            typ: self.typ.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TypeDef {
    pub params: Arc<[(TVar, Option<Type>)]>,
    pub typ: Type,
}

#[derive(Debug)]
pub struct Env<C: Ctx, E: UserEvent> {
    pub by_id: Map<BindId, Bind>,
    pub lambdas: Map<LambdaId, Weak<LambdaDef<C, E>>>,
    pub byref_chain: Map<BindId, BindId>,
    pub binds: Map<ModPath, Map<CompactString, BindId>>,
    pub used: Map<ModPath, Arc<Vec<ModPath>>>,
    pub modules: Set<ModPath>,
    pub typedefs: Map<ModPath, Map<CompactString, TypeDef>>,
}

impl<C: Ctx, E: UserEvent> Clone for Env<C, E> {
    fn clone(&self) -> Self {
        Self {
            by_id: self.by_id.clone(),
            binds: self.binds.clone(),
            byref_chain: self.byref_chain.clone(),
            used: self.used.clone(),
            modules: self.modules.clone(),
            typedefs: self.typedefs.clone(),
            lambdas: self.lambdas.clone(),
        }
    }
}

impl<C: Ctx, E: UserEvent> Env<C, E> {
    pub(super) fn new() -> Self {
        Self {
            by_id: Map::new(),
            binds: Map::new(),
            byref_chain: Map::new(),
            used: Map::new(),
            modules: Set::new(),
            typedefs: Map::new(),
            lambdas: Map::new(),
        }
    }

    pub(super) fn clear(&mut self) {
        let Self { by_id, binds, byref_chain, used, modules, typedefs, lambdas } = self;
        *by_id = Map::new();
        *binds = Map::new();
        *byref_chain = Map::new();
        *used = Map::new();
        *modules = Set::new();
        *typedefs = Map::new();
        *lambdas = Map::new();
    }

    // restore the lexical environment to the state it was in at the
    // snapshot `other`, but leave the bind and type environment
    // alone.
    pub(super) fn restore_lexical_env(&self, other: Self) -> Self {
        Self {
            binds: other.binds,
            used: other.used,
            modules: other.modules,
            typedefs: other.typedefs,
            by_id: self.by_id.clone(),
            lambdas: self.lambdas.clone(),
            byref_chain: self.byref_chain.clone(),
        }
    }

    pub fn find_visible<R, F: FnMut(&str, &str) -> Option<R>>(
        &self,
        scope: &ModPath,
        name: &ModPath,
        mut f: F,
    ) -> Option<R> {
        let mut buf = CompactString::from("");
        let name_scope = Path::dirname(&**name);
        let name = Path::basename(&**name).unwrap_or("");
        for scope in Path::dirnames(&**scope).rev() {
            let used = self.used.get(scope);
            let used = iter::once(scope)
                .chain(used.iter().flat_map(|s| s.iter().map(|p| &***p)));
            for scope in used {
                let scope = name_scope
                    .map(|ns| {
                        buf.clear();
                        buf.push_str(scope);
                        if let Some(Path::SEP) = buf.chars().next_back() {
                            buf.pop();
                        }
                        buf.push_str(ns);
                        buf.as_str()
                    })
                    .unwrap_or(scope);
                if let Some(res) = f(scope, name) {
                    return Some(res);
                }
            }
        }
        None
    }

    pub fn lookup_bind(
        &self,
        scope: &ModPath,
        name: &ModPath,
    ) -> Option<(&ModPath, &Bind)> {
        self.find_visible(scope, name, |scope, name| {
            self.binds.get_full(scope).and_then(|(scope, vars)| {
                vars.get(name)
                    .and_then(|bid| self.by_id.get(bid).map(|bind| (scope, bind)))
            })
        })
    }

    pub fn lookup_typedef(&self, scope: &ModPath, name: &ModPath) -> Option<&TypeDef> {
        self.find_visible(scope, name, |scope, name| {
            self.typedefs.get(scope).and_then(|m| m.get(name))
        })
    }

    /// lookup binds in scope that match the specified partial
    /// name. This is intended to be used for IDEs and interactive
    /// shells, and is not used by the compiler.
    pub fn lookup_matching(
        &self,
        scope: &ModPath,
        part: &ModPath,
    ) -> Vec<(CompactString, BindId)> {
        let mut res = vec![];
        self.find_visible(scope, part, |scope, part| {
            if let Some(vars) = self.binds.get(scope) {
                let r = vars.range::<str, _>((Bound::Included(part), Bound::Unbounded));
                for (name, bind) in r {
                    if name.starts_with(part) {
                        res.push((name.clone(), *bind));
                    }
                }
            }
            None::<()>
        });
        res
    }

    /// lookup modules in scope that match the specified partial
    /// name. This is intended to be used for IDEs and interactive
    /// shells, and is not used by the compiler.
    pub fn lookup_matching_modules(
        &self,
        scope: &ModPath,
        part: &ModPath,
    ) -> Vec<ModPath> {
        let mut res = vec![];
        self.find_visible(scope, part, |scope, part| {
            let p = ModPath(Path::from(ArcStr::from(scope)).append(part));
            for m in self.modules.range((Bound::Included(p.clone()), Bound::Unbounded)) {
                if m.0.starts_with(&*p.0) {
                    if let Some(m) = m.strip_prefix(scope) {
                        if !m.trim().is_empty() {
                            res.push(ModPath(Path::from(ArcStr::from(m))));
                        }
                    }
                }
            }
            None::<()>
        });
        res
    }

    pub fn canonical_modpath(&self, scope: &ModPath, name: &ModPath) -> Option<ModPath> {
        self.find_visible(scope, name, |scope, name| {
            let p = ModPath(Path::from(ArcStr::from(scope)).append(name));
            if self.modules.contains(&p) {
                Some(p)
            } else {
                None
            }
        })
    }

    pub fn deftype(
        &mut self,
        scope: &ModPath,
        name: &str,
        params: Arc<[(TVar, Option<Type>)]>,
        typ: Type,
    ) -> Result<()> {
        let defs = self.typedefs.get_or_default_cow(scope.clone());
        if defs.get(name).is_some() {
            bail!("{name} is already defined in scope {scope}")
        } else {
            thread_local! {
                static KNOWN: RefCell<FxHashMap<ArcStr, TVar>> = RefCell::new(FxHashMap::default());
                static DECLARED: RefCell<FxHashSet<ArcStr>> = RefCell::new(FxHashSet::default());
            }
            KNOWN.with_borrow_mut(|known| {
                known.clear();
                for (tv, tc) in params.iter() {
                    Type::TVar(tv.clone()).alias_tvars(known);
                    if let Some(tc) = tc {
                        tc.alias_tvars(known);
                    }
                }
                typ.alias_tvars(known);
            });
            DECLARED.with_borrow_mut(|declared| {
                declared.clear();
                for (tv, _) in params.iter() {
                    if !declared.insert(tv.name.clone()) {
                        bail!("duplicate type variable {tv} in definition of {name}");
                    }
                }
                typ.check_tvars_declared(declared)?;
                for (_, t) in params.iter() {
                    if let Some(t) = t {
                        t.check_tvars_declared(declared)?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            })?;
            KNOWN.with_borrow(|known| {
                DECLARED.with_borrow(|declared| {
                    for dec in declared {
                        if !known.contains_key(dec) {
                            bail!("unused type parameter {dec} in definition of {name}")
                        }
                    }
                    Ok(())
                })
            })?;
            defs.insert_cow(name.into(), TypeDef { params, typ });
            Ok(())
        }
    }

    pub fn undeftype(&mut self, scope: &ModPath, name: &str) {
        if let Some(defs) = self.typedefs.get_mut_cow(scope) {
            defs.remove_cow(&CompactString::from(name));
            if defs.len() == 0 {
                self.typedefs.remove_cow(scope);
            }
        }
    }

    // create a new binding. If an existing bind exists in the same
    // scope shadow it.
    pub fn bind_variable(&mut self, scope: &ModPath, name: &str, typ: Type) -> &mut Bind {
        let binds = self.binds.get_or_default_cow(scope.clone());
        let mut existing = true;
        let id = binds.get_or_insert_cow(CompactString::from(name), || {
            existing = false;
            BindId::new()
        });
        if existing {
            *id = BindId::new();
        }
        self.by_id.get_or_insert_cow(*id, || Bind {
            export: true,
            id: *id,
            scope: scope.clone(),
            doc: None,
            name: CompactString::from(name),
            typ,
        })
    }

    pub fn unbind_variable(&mut self, id: BindId) {
        if let Some(b) = self.by_id.remove_cow(&id) {
            if let Some(binds) = self.binds.get_mut_cow(&b.scope) {
                binds.remove_cow(&b.name);
                if binds.len() == 0 {
                    self.binds.remove_cow(&b.scope);
                }
            }
        }
    }
}
