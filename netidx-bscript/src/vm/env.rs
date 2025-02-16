use crate::{
    expr::{Arg, FnArgType, FnType, ModPath, Type},
    vm::{BindId, Ctx, InitFnTyped, TypeId},
};
use anyhow::{anyhow, bail, Result};
use compact_str::CompactString;
use immutable_chunkmap::{map::MapS as Map, set::SetS as Set};
use netidx::{path::Path, publisher::Typ};
use smallvec::{smallvec, SmallVec};
use std::{
    fmt::{self, Debug},
    iter,
    sync::LazyLock,
};
use triomphe::Arc;

pub struct LambdaBind<C: Ctx + 'static, E: Debug + Clone + 'static> {
    pub env: Env<C, E>,
    pub scope: ModPath,
    pub argspec: Arc<[Arg]>,
    pub init: InitFnTyped<C, E>,
}

pub struct Bind<C: Ctx + 'static, E: Debug + Clone + 'static> {
    pub id: BindId,
    pub export: bool,
    pub typ: TypeId,
    pub fun: Option<Arc<LambdaBind<C, E>>>,
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
            fun: self.fun.as_ref().map(Arc::clone),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TypeOrAlias {
    Alias(TypeId),
    Type(Type),
}

pub struct Env<C: Ctx + 'static, E: Debug + Clone + 'static> {
    by_id: Map<BindId, Bind<C, E>>,
    binds: Map<ModPath, Map<CompactString, BindId>>,
    pub used: Map<ModPath, Arc<Vec<ModPath>>>,
    pub modules: Set<ModPath>,
    typedefs: Map<ModPath, Map<CompactString, Type>>,
    pub typevars: Map<TypeId, TypeOrAlias>,
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
        let id = *ID;
        if self.typevars.get(&id).is_none() {
            self.typevars.insert_cow(id, TypeOrAlias::Type(Type::Bottom));
        }
        id
    }

    pub fn boolean(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        let id = *ID;
        if self.typevars.get(&id).is_none() {
            let t = Type::Primitive(Typ::Bool.into());
            self.typevars.insert_cow(id, TypeOrAlias::Type(t));
        }
        id
    }

    pub fn number(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        let id = *ID;
        if self.typevars.get(&id).is_none() {
            let t = Type::Primitive(Typ::number());
            self.typevars.insert_cow(id, TypeOrAlias::Type(t));
        }
        id
    }

    pub fn any(&mut self) -> TypeId {
        const ID: LazyLock<TypeId> = LazyLock::new(|| TypeId::new());
        let id = *ID;
        if self.typevars.get(&id).is_none() {
            let t = Type::Primitive(Typ::any());
            self.typevars.insert_cow(id, TypeOrAlias::Type(t));
        }
        id
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
        if from == to {
            return Ok(());
        }
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
        undef_ok: bool,
        t0: TypeId,
        t1: TypeId,
    ) -> Result<()> {
        match (self.typevars.get(&t0), self.typevars.get(&t1)) {
            (Some(TypeOrAlias::Alias(t0)), Some(TypeOrAlias::Alias(t1))) => {
                self.check_typevar_contains(undef_ok, *t0, *t1)
            }
            (Some(TypeOrAlias::Alias(t0)), _) => {
                self.check_typevar_contains(undef_ok, *t0, t1)
            }
            (_, Some(TypeOrAlias::Alias(t1))) => {
                self.check_typevar_contains(undef_ok, t0, *t1)
            }
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
            (None, None) if undef_ok => Ok(()),
            (None, None) => {
                bail!("type must be known")
            }
        }
    }

    pub(super) fn get_typevar(&self, id: &TypeId) -> Option<&Type> {
        match self.typevars.get(id) {
            None => None,
            Some(TypeOrAlias::Alias(id)) => self.get_typevar(id),
            Some(TypeOrAlias::Type(t)) => Some(t),
        }
    }

    pub(super) fn resolve_fn_typerefs<'a>(
        &self,
        scope: &ModPath,
        typ: &'a FnType,
    ) -> Result<FnType> {
        let typ = Type::Fn(Arc::new(typ.clone()));
        match self.resolve_typrefs(scope, &typ)? {
            Type::Fn(f) => Ok((*f).clone()),
            _ => bail!("unexpected fn resolution"),
        }
    }

    pub(super) fn resolve_typrefs<'a>(
        &self,
        scope: &ModPath,
        typ: &'a Type,
    ) -> Result<Type> {
        match typ {
            Type::Bottom => Ok(Type::Bottom),
            Type::Primitive(s) => Ok(Type::Primitive(*s)),
            Type::Ref(name) => self
                .find_visible(scope, name, |scope, name| {
                    self.typedefs
                        .get(scope)
                        .and_then(|defs| defs.get(name).map(|typ| typ.clone()))
                })
                .ok_or_else(|| anyhow!("undefined type {name} in scope {scope}")),
            Type::Set(ts) => {
                let mut res: SmallVec<[Type; 20]> = smallvec![];
                for t in ts.iter() {
                    res.push(self.resolve_typrefs(scope, t)?)
                }
                Ok(Type::Set(Arc::from_iter(res)))
            }
            Type::Fn(f) => {
                let vargs = f
                    .vargs
                    .as_ref()
                    .map(|t| self.resolve_typrefs(scope, t))
                    .transpose()?;
                let rtype = self.resolve_typrefs(scope, &f.rtype)?;
                let mut res: SmallVec<[FnArgType; 20]> = smallvec![];
                for a in f.args.iter() {
                    let typ = self.resolve_typrefs(scope, &a.typ)?;
                    let a = FnArgType { label: a.label.clone(), typ };
                    res.push(a);
                }
                Ok(Type::Fn(Arc::new(FnType { args: Arc::from_iter(res), rtype, vargs })))
            }
        }
    }
}
