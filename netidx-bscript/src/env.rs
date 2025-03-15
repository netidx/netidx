use crate::{
    expr::{Arg, ModPath},
    typ::{NoRefs, Type},
    BindId, Ctx, InitFn, UserEvent,
};
use anyhow::{bail, Result};
use compact_str::CompactString;
use immutable_chunkmap::{map::MapS as Map, set::SetS as Set};
use netidx::path::Path;
use std::{fmt, iter};
use triomphe::Arc;

pub struct LambdaBind<C: Ctx, E: UserEvent> {
    pub env: Env<C, E>,
    pub scope: ModPath,
    pub argspec: Arc<[Arg]>,
    pub init: InitFn<C, E>,
}

pub struct Bind<C: Ctx, E: UserEvent> {
    pub id: BindId,
    pub export: bool,
    pub typ: Type<NoRefs>,
    pub fun: Option<Arc<LambdaBind<C, E>>>,
    scope: ModPath,
    name: CompactString,
}

impl<C: Ctx, E: UserEvent> fmt::Debug for Bind<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Bind {{ id: {:?}, export: {}, fun: {} }}",
            self.id,
            self.export,
            self.fun.is_some()
        )
    }
}

impl<C: Ctx, E: UserEvent> Clone for Bind<C, E> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            scope: self.scope.clone(),
            name: self.name.clone(),
            export: self.export,
            typ: self.typ.clone(),
            fun: self.fun.as_ref().map(Arc::clone),
        }
    }
}

pub struct Env<C: Ctx, E: UserEvent> {
    pub by_id: Map<BindId, Bind<C, E>>,
    pub binds: Map<ModPath, Map<CompactString, BindId>>,
    pub used: Map<ModPath, Arc<Vec<ModPath>>>,
    pub modules: Set<ModPath>,
    pub typedefs: Map<ModPath, Map<CompactString, Type<NoRefs>>>,
}

impl<C: Ctx, E: UserEvent> Clone for Env<C, E> {
    fn clone(&self) -> Self {
        Self {
            by_id: self.by_id.clone(),
            binds: self.binds.clone(),
            used: self.used.clone(),
            modules: self.modules.clone(),
            typedefs: self.typedefs.clone(),
        }
    }
}

impl<C: Ctx, E: UserEvent> Env<C, E> {
    pub(super) fn new() -> Self {
        Self {
            by_id: Map::new(),
            binds: Map::new(),
            used: Map::new(),
            modules: Set::new(),
            typedefs: Map::new(),
        }
    }

    pub(super) fn clear(&mut self) {
        let Self { by_id, binds, used, modules, typedefs } = self;
        *by_id = Map::new();
        *binds = Map::new();
        *used = Map::new();
        *modules = Set::new();
        *typedefs = Map::new();
    }

    // restore the lexical environment to the state it was in at the
    // snapshot `other`, but leave the bind and type environment
    // alone.
    pub(super) fn restore_lexical_env(&self, other: &Self) -> Self {
        Self {
            binds: other.binds.clone(),
            used: other.used.clone(),
            modules: other.modules.clone(),
            typedefs: other.typedefs.clone(),
            by_id: self.by_id.clone(),
        }
    }

    // merge two lexical environments, with the `orig` environment
    // taking prescidence in case of conflicts. The type and binding
    // environment is not altered
    pub(super) fn merge_lexical(&self, orig: &Self) -> Self {
        let Self { by_id: _, binds, used, modules, typedefs } = self;
        let binds = binds.update_many(
            orig.binds.into_iter().map(|(s, m)| (s.clone(), m)),
            |k, v, kv| match kv {
                None => Some((k, v.clone())),
                Some((_, m)) => {
                    let v = m.update_many(
                        v.into_iter().map(|(k, v)| (k.clone(), *v)),
                        |k, v, _| Some((k, v)),
                    );
                    Some((k, v))
                }
            },
        );
        let used = used.update_many(
            orig.used.into_iter().map(|(k, v)| (k.clone(), v.clone())),
            |k, v, _| Some((k, v)),
        );
        let modules = modules.union(&orig.modules);
        let typedefs = typedefs.update_many(
            orig.typedefs.into_iter().map(|(k, v)| (k.clone(), v)),
            |k, v, kv| match kv {
                None => Some((k, v.clone())),
                Some((_, m)) => {
                    let v = m.update_many(
                        v.into_iter().map(|(k, v)| (k.clone(), v.clone())),
                        |k, v, _| Some((k, v)),
                    );
                    Some((k, v))
                }
            },
        );
        Self { binds, used, modules, typedefs, by_id: self.by_id.clone() }
    }

    pub fn find_visible<R, F: FnMut(&str, &str) -> Option<R>>(
        &self,
        scope: &ModPath,
        name: &ModPath,
        mut f: F,
    ) -> Option<R> {
        let mut buf = CompactString::from("");
        let name_scope = Path::dirname(&**name);
        let name = Path::basename(&**name)?;
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

    pub(super) fn lookup_bind(
        &self,
        scope: &ModPath,
        name: &ModPath,
    ) -> Option<(&ModPath, &Bind<C, E>)> {
        self.find_visible(scope, name, |scope, name| {
            self.binds.get_full(scope).and_then(|(scope, vars)| {
                vars.get(name)
                    .and_then(|bid| self.by_id.get(bid).map(|bind| (scope, bind)))
            })
        })
    }

    pub fn deftype(
        &mut self,
        scope: &ModPath,
        name: &str,
        typ: Type<NoRefs>,
    ) -> Result<()> {
        let defs = self.typedefs.get_or_default_cow(scope.clone());
        if defs.get(name).is_some() {
            bail!("{name} is already defined in scope {scope}")
        } else {
            defs.insert_cow(name.into(), typ);
            Ok(())
        }
    }

    // create a new binding. If an existing bind exists in the same
    // scope shadow it.
    pub fn bind_variable(
        &mut self,
        scope: &ModPath,
        name: &str,
        typ: Type<NoRefs>,
    ) -> &mut Bind<C, E> {
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
            name: CompactString::from(name),
            typ,
            fun: None,
        })
    }
}
