use crate::{
    expr::{FnType, ModPath, Type},
    vm::{BindId, Ctx, InitFn, TypeId},
};
use anyhow::{bail, Result};
use compact_str::CompactString;
use immutable_chunkmap::{map::MapS as Map, set::SetS as Set};
use netidx::{path::Path, publisher::Typ};
use smallvec::{smallvec, SmallVec};
use std::{
    borrow::Cow,
    fmt::{self, Debug},
    iter,
    sync::{self, LazyLock},
};
use triomphe::Arc;

pub(super) struct Bind<C: Ctx + 'static, E: Debug + Clone + 'static> {
    pub id: BindId,
    pub export: bool,
    pub typ: TypeId,
    pub fun: Option<InitFn<C, E>>,
    scope: ModPath,
    name: CompactString,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> fmt::Debug for Bind<C, E> {
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

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Clone for Bind<C, E> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            scope: self.scope.clone(),
            name: self.name.clone(),
            export: self.export,
            typ: self.typ.clone(),
            fun: self.fun.as_ref().map(|fun| sync::Arc::clone(fun)),
        }
    }
}

pub(super) struct Env<C: Ctx + 'static, E: Debug + Clone + 'static> {
    by_id: Map<BindId, Bind<C, E>>,
    binds: Map<ModPath, Map<CompactString, BindId>>,
    used: Map<ModPath, Arc<Vec<ModPath>>>,
    modules: Set<ModPath>,
    typedefs: Map<ModPath, Map<CompactString, Type>>,
    typevars: Map<TypeId, Type>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Clone for Env<C, E> {
    fn clone(&self) -> Self {
        Self {
            by_id: self.by_id.clone(),
            binds: self.binds.clone(),
            used: self.used.clone(),
            modules: self.modules.clone(),
            typedefs: self.typedefs.clone(),
            typevars: self.typevars.clone(),
        }
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Env<C, E> {
    pub(super) fn new() -> Self {
        Self {
            by_id: Map::new(),
            binds: Map::new(),
            used: Map::new(),
            modules: Set::new(),
            typedefs: Map::new(),
            typevars: Map::new(),
        }
    }

    pub(super) fn clear(&mut self) {
        let Self { by_id, binds, used, modules, typedefs, typevars } = self;
        *by_id = Map::new();
        *binds = Map::new();
        *used = Map::new();
        *modules = Set::new();
        *typedefs = Map::new();
        *typevars = Map::new();
    }

    // this is NOT a general merge operation for environments
    pub(super) fn merge_lambda_env(&self, other: &Self) -> Self {
        let Self { by_id, binds, used, modules, typedefs, typevars } = self;
        let by_id = by_id
            .update_many(other.by_id.into_iter().map(|(id, b)| (*id, b)), |k, v, _| {
                Some((k, v.clone()))
            });
        let binds = binds.update_many(
            other.binds.into_iter().map(|(s, m)| (s.clone(), m)),
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
            other.used.into_iter().map(|(k, v)| (k.clone(), v.clone())),
            |k, v, _| Some((k, v)),
        );
        let modules = modules.union(&other.modules);
        let typedefs = typedefs.update_many(
            other.typedefs.into_iter().map(|(k, v)| (k.clone(), v)),
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
        let typevars = typevars.update_many(
            other.typevars.into_iter().map(|(k, v)| (*k, v.clone())),
            |k, v, _| Some((k, v)),
        );
        Self { by_id, binds, used, modules, typedefs, typevars }
    }

    fn find_visible<R, F: FnMut(&str, &str) -> Option<R>>(
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

    // alias orig_id -> new_id. orig_id will be removed, and it's in
    // scope entry will point to new_id
    pub(super) fn alias(&mut self, orig_id: BindId, new_id: BindId) {
        if let Some(bind) = self.by_id.remove_cow(&orig_id) {
            if let Some(binds) = self.binds.get_mut_cow(&bind.scope) {
                if let Some(id) = binds.get_mut_cow(bind.name.as_str()) {
                    *id = new_id
                }
            }
        }
    }

    // create a new binding. If an existing bind exists in the same
    // scope shadow it.
    pub(super) fn bind_variable(
        &mut self,
        scope: &ModPath,
        name: &str,
        typ: TypeId,
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

    pub(super) fn bottom(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        if self.typevars.get(&ID).is_none() {
            self.typevars.insert_cow(*ID, Type::Bottom);
        }
        *ID
    }

    pub(super) fn boolean(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        if self.typevars.get(&ID).is_none() {
            self.typevars.insert_cow(*ID, Type::Primitive(Typ::Bool.into()));
        }
        *ID
    }

    pub(super) fn number(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        if self.typevars.get(&ID).is_none() {
            self.typevars.insert_cow(*ID, Type::Primitive(Typ::number()));
        }
        *ID
    }

    pub(super) fn any(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        if self.typevars.get(&ID).is_none() {
            self.typevars.insert_cow(*ID, Type::Primitive(Typ::any()));
        }
        *ID
    }

    pub(super) fn add_typ(&mut self, typ: Type) -> TypeId {
        let id = TypeId::new();
        self.typevars.insert_cow(id, typ);
        id
    }

    pub(super) fn resolve_typrefs<'a>(
        &self,
        scope: &ModPath,
        typ: &'a Type,
    ) -> Result<Cow<'a, Type>> {
        match typ {
            Type::Bottom => Ok(Cow::Borrowed(typ)),
            Type::Primitive(_) => Ok(Cow::Borrowed(typ)),
            Type::Ref(name) => {
                let scope = match Path::dirname(&**name) {
                    None => scope,
                    Some(dn) => &*scope.append(dn),
                };
                let name = Path::basename(&**name).unwrap_or("");
                match self.typedefs.get(scope) {
                    None => bail!("undefined type {name} in scope {scope}"),
                    Some(defs) => match defs.get(name) {
                        None => bail!("undefined type {name} in scope {scope}"),
                        Some(typ) => Ok(Cow::Owned(typ.clone())),
                    },
                }
            }
            Type::Set(ts) => {
                let mut res: SmallVec<[Cow<Type>; 20]> = smallvec![];
                for t in ts.iter() {
                    res.push(self.resolve_typrefs(scope, t)?)
                }
                let borrowed = res.iter().all(|t| match t {
                    Cow::Borrowed(_) => true,
                    Cow::Owned(_) => false,
                });
                if borrowed {
                    Ok(Cow::Borrowed(typ))
                } else {
                    let iter = res.into_iter().map(|t| t.into_owned());
                    Ok(Cow::Owned(Type::Set(Arc::from_iter(iter))))
                }
            }
            Type::Fn(f) => {
                let vargs = self.resolve_typrefs(scope, &f.vargs)?;
                let rtype = self.resolve_typrefs(scope, &f.rtype)?;
                let mut res: SmallVec<[Cow<Type>; 20]> = smallvec![];
                for t in f.args.iter() {
                    res.push(self.resolve_typrefs(scope, t)?);
                }
                let borrowed =
                    res.iter().chain(iter::once(&vargs)).chain(iter::once(&rtype)).all(
                        |t| match t {
                            Cow::Borrowed(_) => true,
                            Cow::Owned(_) => false,
                        },
                    );
                if borrowed {
                    Ok(Cow::Borrowed(typ))
                } else {
                    Ok(Cow::Owned(Type::Fn(Arc::new(FnType {
                        args: Arc::from_iter(res.into_iter().map(|t| t.into_owned())),
                        rtype: rtype.into_owned(),
                        vargs: vargs.into_owned(),
                    }))))
                }
            }
        }
    }
}
