use crate::{
    expr::{FnType, ModPath, Type},
    vm::{BindId, Ctx, InitFnTyped, TypeId},
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
    sync::{Arc as SArc, LazyLock},
};
use triomphe::Arc;

pub struct Bind<C: Ctx + 'static, E: Debug + Clone + 'static> {
    pub id: BindId,
    pub export: bool,
    pub typ: TypeId,
    pub fun: Option<InitFnTyped<C, E>>,
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
            fun: self.fun.as_ref().map(|f| SArc::clone(f)),
        }
    }
}

#[derive(Clone)]
enum TypeOrAlias {
    Alias(TypeId),
    Type(Type),
}

pub struct Env<C: Ctx + 'static, E: Debug + Clone + 'static> {
    by_id: Map<BindId, Bind<C, E>>,
    binds: Map<ModPath, Map<CompactString, BindId>>,
    pub used: Map<ModPath, Arc<Vec<ModPath>>>,
    pub modules: Set<ModPath>,
    typedefs: Map<ModPath, Map<CompactString, Type>>,
    typevars: Map<TypeId, TypeOrAlias>,
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
            typevars: self.typevars.clone(),
        }
    }

    // merge two lexical environments, with the `orig` environment
    // taking prescidence in case of conflicts. The type and binding
    // environment is not altered
    pub(super) fn merge_lexical(&self, orig: &Self) -> Self {
        let Self { by_id: _, binds, used, modules, typedefs, typevars: _ } = self;
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
        Self {
            binds,
            used,
            modules,
            typedefs,
            by_id: self.by_id.clone(),
            typevars: self.typevars.clone(),
        }
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

    pub(super) fn lookup_bind_by_id(&self, id: &BindId) -> Option<&Bind<C, E>> {
        self.by_id.get(id)
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
    pub fn bind_variable(
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

    pub(super) fn deftype(
        &mut self,
        scope: &ModPath,
        name: &str,
        typ: Type,
    ) -> Result<()> {
        let defs = self.typedefs.get_or_default_cow(scope.clone());
        if defs.get(name).is_some() {
            bail!("{name} is already defined in scope {scope}")
        } else {
            defs.insert_cow(name.into(), typ);
            Ok(())
        }
    }

    pub fn bottom(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        if self.typevars.get(&ID).is_none() {
            self.typevars.insert_cow(*ID, TypeOrAlias::Type(Type::Bottom));
        }
        *ID
    }

    pub fn boolean(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        if self.typevars.get(&ID).is_none() {
            let t = Type::Primitive(Typ::Bool.into());
            self.typevars.insert_cow(*ID, TypeOrAlias::Type(t));
        }
        *ID
    }

    pub fn number(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        if self.typevars.get(&ID).is_none() {
            let t = Type::Primitive(Typ::number());
            self.typevars.insert_cow(*ID, TypeOrAlias::Type(t));
        }
        *ID
    }

    pub fn any(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        if self.typevars.get(&ID).is_none() {
            let t = Type::Primitive(Typ::any());
            self.typevars.insert_cow(*ID, TypeOrAlias::Type(t));
        }
        *ID
    }

    pub fn add_typ(&mut self, typ: Type) -> TypeId {
        let id = TypeId::new();
        self.typevars.insert_cow(id, TypeOrAlias::Type(typ));
        id
    }

    pub fn define_typevar(&mut self, id: TypeId, typ: Type) -> Result<()> {
        match self.typevars.get(&id) {
            None => {
                self.typevars.insert_cow(id, TypeOrAlias::Type(typ));
                Ok(())
            }
            Some(TypeOrAlias::Alias(id)) => self.define_typevar(*id, typ),
            Some(TypeOrAlias::Type(cur_typ)) if cur_typ.contains(&typ) => Ok(()),
            Some(TypeOrAlias::Type(cur_typ)) => {
                bail!("type mismatch {cur_typ} does not contain {typ}")
            }
        }
    }

    pub fn alias_typevar(&mut self, from: TypeId, to: TypeId) -> Result<()> {
        match self.typevars.get(&from) {
            None => {
                self.typevars.insert_cow(from, TypeOrAlias::Alias(to));
                Ok(())
            }
            Some(TypeOrAlias::Alias(tgt)) if &to == tgt => Ok(()),
            Some(_) => {
                bail!("can't alias {from:?} to {to:?}, {from:?} is already defined")
            }
        }
    }

    pub(super) fn check_typevar_contains(
        &mut self,
        t0: TypeId,
        t1: TypeId,
    ) -> Result<()> {
        match (self.typevars.get(&t0), self.typevars.get(&t1)) {
            (Some(TypeOrAlias::Alias(t0)), Some(TypeOrAlias::Alias(t1))) => {
                self.check_typevar_contains(*t0, *t1)
            }
            (Some(TypeOrAlias::Alias(t0)), _) => self.check_typevar_contains(*t0, t1),
            (_, Some(TypeOrAlias::Alias(t1))) => self.check_typevar_contains(t0, *t1),
            (Some(TypeOrAlias::Type(typ0)), Some(TypeOrAlias::Type(typ1)))
                if typ0.contains(typ1) =>
            {
                Ok(())
            }
            (Some(TypeOrAlias::Type(typ0)), Some(TypeOrAlias::Type(typ1))) => {
                bail!("type mismatch {typ0} does not contain {typ1}")
            }
            (Some(TypeOrAlias::Type(t0)), None) => {
                let typ = t0.clone();
                self.define_typevar(t1, typ)
            }
            (None, Some(TypeOrAlias::Type(t1))) => {
                let typ = t1.clone();
                self.define_typevar(t0, typ)
            }
            (None, None) => bail!("type must be known"),
        }
    }

    pub(super) fn get_typevar(&self, id: &TypeId) -> Result<&Type> {
        match self.typevars.get(id) {
            None => bail!("type must be know"),
            Some(TypeOrAlias::Alias(id)) => self.get_typevar(id),
            Some(TypeOrAlias::Type(t)) => Ok(t),
        }
    }

    pub(super) fn resolve_fn_typerefs<'a>(
        &self,
        scope: &ModPath,
        typ: &'a FnType,
    ) -> Result<FnType> {
        let typ = Type::Fn(Arc::new(typ.clone()));
        let typ = self.resolve_typrefs(scope, &typ)?;
        match &*typ {
            Type::Fn(f) => Ok((**f).clone()),
            _ => bail!("unexpected fn resolution"),
        }
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
