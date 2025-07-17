use crate::{
    env::Env,
    expr::{ExprId, ModPath, Pattern, StructurePattern},
    node::{compiler, Cached},
    typ::Type,
    BindId, Ctx, Event, ExecCtx, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use netidx::{publisher::Typ, subscriber::Value};
use smallvec::SmallVec;
use std::fmt::Debug;
use triomphe::Arc;

#[derive(Debug)]
pub enum StructPatternNode {
    Ignore,
    Literal(Value),
    Bind(BindId),
    Slice {
        tuple: bool,
        all: Option<BindId>,
        binds: Box<[StructPatternNode]>,
    },
    SlicePrefix {
        all: Option<BindId>,
        prefix: Box<[StructPatternNode]>,
        tail: Option<BindId>,
    },
    SliceSuffix {
        all: Option<BindId>,
        head: Option<BindId>,
        suffix: Box<[StructPatternNode]>,
    },
    Struct {
        all: Option<BindId>,
        binds: Box<[(ArcStr, usize, StructPatternNode)]>,
    },
    Variant {
        tag: ArcStr,
        all: Option<BindId>,
        binds: Box<[StructPatternNode]>,
    },
}

impl StructPatternNode {
    pub fn compile<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        type_predicate: &Type,
        spec: &StructurePattern,
        scope: &ModPath,
    ) -> Result<Self> {
        if !spec.binds_uniq() {
            bail!("bound variables must have unique names")
        }
        Self::compile_int(ctx, type_predicate, spec, scope)
    }

    fn compile_int<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        type_predicate: &Type,
        spec: &StructurePattern,
        scope: &ModPath,
    ) -> Result<Self> {
        macro_rules! with_pref_suf {
            ($all:expr, $single:expr, $multi:expr) => {{
                type_predicate.check_contains(
                    &ctx.env,
                    &Type::Array(Arc::new(Type::empty_tvar())),
                )?;
                match &type_predicate.with_deref(|t| t.cloned()) {
                    Some(Type::Array(et)) => {
                        let all = $all.as_ref().map(|n| {
                            ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                        });
                        let single = $single.as_ref().map(|n| {
                            ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                        });
                        let multi = $multi
                            .iter()
                            .map(|n| Self::compile_int(ctx, et, n, scope))
                            .collect::<Result<Box<[Self]>>>()?;
                        (all, single, multi)
                    }
                    _ => bail!("slice patterns can't match {type_predicate}"),
                }
            }};
        }
        let type_predicate = match type_predicate {
            Type::Ref { .. } => &type_predicate.lookup_ref(&ctx.env)?.clone(),
            t => t,
        };
        let t = match &spec {
            StructurePattern::Ignore => Self::Ignore,
            StructurePattern::Literal(v) => {
                type_predicate
                    .check_contains(&ctx.env, &Type::Primitive(Typ::get(v).into()))?;
                Self::Literal(v.clone())
            }
            StructurePattern::Bind(name) => {
                let id = ctx.env.bind_variable(scope, name, type_predicate.clone()).id;
                Self::Bind(id)
            }
            StructurePattern::SlicePrefix { all, prefix, tail } => {
                let (all, tail, prefix) = with_pref_suf!(all, tail, prefix);
                Self::SlicePrefix { all, prefix, tail }
            }
            StructurePattern::SliceSuffix { all, head, suffix } => {
                let (all, head, suffix) = with_pref_suf!(all, head, suffix);
                Self::SliceSuffix { all, head, suffix }
            }
            StructurePattern::Slice { all, binds } => {
                type_predicate.check_contains(
                    &ctx.env,
                    &Type::Array(Arc::new(Type::empty_tvar())),
                )?;
                match &type_predicate.with_deref(|t| t.cloned()) {
                    Some(Type::Array(et)) => {
                        let all = all.as_ref().map(|n| {
                            ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                        });
                        let binds = binds
                            .iter()
                            .map(|b| Self::compile_int(ctx, et, b, scope))
                            .collect::<Result<Box<[Self]>>>()?;
                        Self::Slice { tuple: false, all, binds }
                    }
                    _ => bail!("slice patterns can't match {type_predicate}"),
                }
            }
            StructurePattern::Tuple { all, binds } => {
                type_predicate.check_contains(
                    &ctx.env,
                    &Type::Tuple(Arc::from_iter(
                        binds.iter().map(|_| Type::empty_tvar()),
                    )),
                )?;
                match &type_predicate.with_deref(|t| t.cloned()) {
                    Some(Type::Tuple(elts)) => {
                        if binds.len() != elts.len() {
                            bail!("expected a tuple of length {}", elts.len())
                        }
                        let all = all.as_ref().map(|n| {
                            ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                        });
                        let binds = elts
                            .iter()
                            .zip(binds.iter())
                            .map(|(t, b)| Self::compile_int(ctx, t, b, scope))
                            .collect::<Result<Box<[Self]>>>()?;
                        Self::Slice { tuple: true, all, binds }
                    }
                    _ => bail!("tuple patterns can't match {type_predicate}"),
                }
            }
            StructurePattern::Variant { all, tag, binds } => {
                type_predicate.check_contains(
                    &ctx.env,
                    &Type::Variant(
                        tag.clone(),
                        Arc::from_iter(binds.iter().map(|_| Type::empty_tvar())),
                    ),
                )?;
                match &type_predicate.with_deref(|t| t.cloned()) {
                    Some(Type::Variant(ttag, elts)) => {
                        if ttag != tag {
                            bail!(
                                "pattern cannot match type, tag mismatch {ttag} vs {tag}"
                            )
                        }
                        if binds.len() != elts.len() {
                            bail!("expected a variant with {} args", elts.len())
                        }
                        let all = all.as_ref().map(|n| {
                            ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                        });
                        let binds = elts
                            .iter()
                            .zip(binds.iter())
                            .map(|(t, b)| Self::compile_int(ctx, t, b, scope))
                            .collect::<Result<Box<[Self]>>>()?;
                        Self::Variant { tag: tag.clone(), all, binds }
                    }
                    _ => bail!("variant patterns can't match {type_predicate}"),
                }
            }
            StructurePattern::Struct { exhaustive, all, binds } => {
                struct Ifo {
                    name: ArcStr,
                    index: usize,
                    pattern: StructurePattern,
                    typ: Type,
                }
                match &type_predicate {
                    Type::Struct(_) => (),
                    _ if *exhaustive => type_predicate.check_contains(
                        &ctx.env,
                        &Type::Struct(Arc::from_iter(
                            binds
                                .iter()
                                .map(|(name, _)| (name.clone(), Type::empty_tvar())),
                        )),
                    )?,
                    _ => bail!("non exhaustive struct matches require type annotations"),
                }
                match &type_predicate.with_deref(|t| t.cloned()) {
                    Some(Type::Struct(elts)) => {
                        let binds = binds
                            .iter()
                            .map(|(field, pat)| {
                                let r = elts.iter().enumerate().find_map(
                                    |(i, (name, typ))| {
                                        if field == name {
                                            Some(Ifo {
                                                name: name.clone(),
                                                index: i,
                                                pattern: pat.clone(),
                                                typ: typ.clone(),
                                            })
                                        } else {
                                            None
                                        }
                                    },
                                );
                                r.ok_or_else(|| anyhow!("no such struct field {field}"))
                            })
                            .collect::<Result<SmallVec<[Ifo; 8]>>>()?;
                        if *exhaustive && binds.len() < elts.len() {
                            bail!("missing bindings for struct fields")
                        }
                        let all = all.as_ref().map(|n| {
                            ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                        });
                        let binds = binds
                            .into_iter()
                            .map(|ifo| {
                                Ok((
                                    ifo.name,
                                    ifo.index,
                                    Self::compile_int(
                                        ctx,
                                        &ifo.typ,
                                        &ifo.pattern,
                                        scope,
                                    )?,
                                ))
                            })
                            .collect::<Result<Box<[(ArcStr, usize, Self)]>>>()?;
                        Self::Struct { all, binds }
                    }
                    _ => bail!("struct patterns can't match {type_predicate}"),
                }
            }
        };
        Ok(t)
    }

    pub fn ids<'a>(&'a self, f: &mut (dyn FnMut(BindId) + 'a)) {
        match &self {
            Self::Ignore | Self::Literal(_) => (),
            Self::Bind(id) => f(*id),
            Self::Slice { tuple: _, all, binds } => {
                if let Some(id) = all {
                    f(*id);
                }
                for n in binds.iter() {
                    n.ids(f)
                }
            }
            Self::Variant { tag: _, all, binds } => {
                if let Some(id) = all {
                    f(*id)
                }
                for n in binds.iter() {
                    n.ids(f)
                }
            }
            Self::SlicePrefix { all, prefix, tail } => {
                if let Some(id) = all {
                    f(*id)
                }
                for n in prefix.iter() {
                    n.ids(f)
                }
                if let Some(id) = tail {
                    f(*id)
                }
            }
            Self::SliceSuffix { all, head, suffix } => {
                if let Some(id) = all {
                    f(*id)
                }
                if let Some(id) = head {
                    f(*id)
                }
                for n in suffix.iter() {
                    n.ids(f)
                }
            }
            Self::Struct { all, binds } => {
                if let Some(id) = all {
                    f(*id)
                }
                for (_, _, n) in binds.iter() {
                    n.ids(f)
                }
            }
        }
    }

    pub fn bind<F: FnMut(BindId, Value)>(&self, v: &Value, f: &mut F) {
        match &self {
            Self::Ignore | Self::Literal(_) => (),
            Self::Bind(id) => f(*id, v.clone()),
            Self::Slice { tuple: _, all, binds } => match v {
                Value::Array(a) if a.len() == binds.len() => {
                    if let Some(id) = all {
                        f(*id, v.clone());
                    }
                    for (j, n) in binds.iter().enumerate() {
                        n.bind(&a[j], f)
                    }
                }
                _ => (),
            },
            Self::Variant { tag: _, all, binds } => {
                if let Some(id) = all {
                    f(*id, v.clone())
                }
                match v {
                    Value::Array(a) if a.len() == binds.len() + 1 => {
                        for (j, n) in binds.iter().enumerate() {
                            n.bind(&a[j + 1], f)
                        }
                    }
                    _ => (),
                }
            }
            Self::SlicePrefix { all, prefix, tail } => match v {
                Value::Array(a) if a.len() >= prefix.len() => {
                    if let Some(id) = all {
                        f(*id, v.clone())
                    }
                    for (j, n) in prefix.iter().enumerate() {
                        n.bind(&a[j], f)
                    }
                    if let Some(id) = tail {
                        let ss = a.subslice(prefix.len()..).unwrap();
                        f(*id, Value::Array(ss))
                    }
                }
                _ => (),
            },
            Self::SliceSuffix { all, head, suffix } => match v {
                Value::Array(a) if a.len() >= suffix.len() => {
                    if let Some(id) = all {
                        f(*id, v.clone())
                    }
                    if let Some(id) = head {
                        let ss = a.subslice(..suffix.len()).unwrap();
                        f(*id, Value::Array(ss))
                    }
                    let tail = a.subslice(suffix.len()..).unwrap();
                    for (j, n) in suffix.iter().enumerate() {
                        n.bind(&tail[j], f)
                    }
                }
                _ => (),
            },
            Self::Struct { all, binds } => match v {
                Value::Array(a) if a.len() >= binds.len() => {
                    if let Some(id) = all {
                        f(*id, v.clone())
                    }
                    for (_, i, n) in binds.iter() {
                        if let Some(v) = a.get(*i) {
                            match v {
                                Value::Array(a) if a.len() == 2 => n.bind(&a[1], f),
                                _ => (),
                            }
                        }
                    }
                }
                _ => (),
            },
        }
    }

    pub fn unbind<F: FnMut(BindId)>(&self, f: &mut F) {
        match &self {
            Self::Ignore | Self::Literal(_) => (),
            Self::Bind(id) => f(*id),
            Self::Slice { tuple: _, all, binds }
            | Self::Variant { tag: _, all, binds } => {
                if let Some(id) = all {
                    f(*id)
                }
                for n in binds.iter() {
                    n.unbind(f)
                }
            }
            Self::SlicePrefix { all, prefix, tail } => {
                if let Some(id) = all {
                    f(*id)
                }
                if let Some(id) = tail {
                    f(*id)
                }
                for n in prefix.iter() {
                    n.unbind(f)
                }
            }
            Self::SliceSuffix { all, head, suffix } => {
                if let Some(id) = all {
                    f(*id)
                }
                if let Some(id) = head {
                    f(*id)
                }
                for n in suffix.iter() {
                    n.unbind(f)
                }
            }
            Self::Struct { all, binds } => {
                if let Some(id) = all {
                    f(*id)
                }
                for (_, _, n) in binds.iter() {
                    n.unbind(f)
                }
            }
        }
    }

    pub fn is_match(&self, v: &Value) -> bool {
        match &self {
            Self::Ignore | Self::Bind(_) => true,
            Self::Literal(o) => v == o,
            Self::Slice { tuple: _, all: _, binds } => match v {
                Value::Array(a) => {
                    a.len() == binds.len()
                        && binds.iter().zip(a.iter()).all(|(b, v)| b.is_match(v))
                }
                _ => false,
            },
            Self::Variant { tag, all: _, binds } if binds.len() == 0 => match v {
                Value::String(s) => tag == s,
                _ => false,
            },
            Self::Variant { tag, all: _, binds } => match v {
                Value::Array(a) => {
                    a.len() == binds.len() + 1
                        && match &a[0] {
                            Value::String(s) => s == tag,
                            _ => false,
                        }
                        && binds.iter().zip(a[1..].iter()).all(|(b, v)| b.is_match(v))
                }
                _ => false,
            },
            Self::SlicePrefix { all: _, prefix, tail: _ } => match v {
                Value::Array(a) => {
                    a.len() >= prefix.len()
                        && prefix.iter().zip(a.iter()).all(|(b, v)| b.is_match(v))
                }
                _ => false,
            },
            Self::SliceSuffix { all: _, head: _, suffix } => match v {
                Value::Array(a) => {
                    a.len() >= suffix.len()
                        && suffix
                            .iter()
                            .zip(a.iter().skip(a.len() - suffix.len()))
                            .all(|(b, v)| b.is_match(v))
                }
                _ => false,
            },
            Self::Struct { all: _, binds } => match v {
                Value::Array(a) => {
                    a.len() >= binds.len()
                        && binds.iter().all(|(_, i, p)| match a.get(*i) {
                            Some(Value::Array(a)) if a.len() == 2 => p.is_match(&a[1]),
                            _ => false,
                        })
                }
                _ => false,
            },
        }
    }

    pub fn is_refutable(&self) -> bool {
        match &self {
            Self::Bind(_) | Self::Ignore => false,
            Self::Literal(_) => true,
            Self::Slice { tuple: true, all: _, binds } => {
                binds.iter().any(|p| p.is_refutable())
            }
            Self::Struct { all: _, binds } => {
                binds.iter().any(|(_, _, p)| p.is_refutable())
            }
            Self::Variant { .. }
            | Self::Slice { tuple: false, .. }
            | Self::SlicePrefix { .. }
            | Self::SliceSuffix { .. } => true,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PatternNode<C: Ctx, E: UserEvent> {
    pub(super) type_predicate: Type,
    pub(super) structure_predicate: StructPatternNode,
    pub(super) guard: Option<Cached<C, E>>,
}

impl<C: Ctx, E: UserEvent> PatternNode<C, E> {
    pub(super) fn compile(
        ctx: &mut ExecCtx<C, E>,
        arg_type: &Type,
        spec: &Pattern,
        scope: &ModPath,
        top_id: ExprId,
    ) -> Result<Self> {
        let type_predicate = match &spec.type_predicate {
            Some(t) => t.scope_refs(scope).lookup_ref(&ctx.env)?.clone(),
            None => {
                let typ = spec.structure_predicate.infer_type_predicate(&ctx.env)?;
                match &spec.structure_predicate {
                    StructurePattern::Bind(_) | StructurePattern::Ignore => {
                        arg_type.init_wildcard_match(&ctx.env, &typ)?
                    }
                    _ => {
                        typ.could_match(&ctx.env, &arg_type)?;
                    }
                }
                typ
            }
        };
        match &type_predicate {
            Type::Fn(_) => bail!("can't match on Fn type"),
            Type::Bottom
            | Type::Any
            | Type::Primitive(_)
            | Type::Set(_)
            | Type::TVar(_)
            | Type::Array(_)
            | Type::ByRef(_)
            | Type::Tuple(_)
            | Type::Variant(_, _)
            | Type::Struct(_)
            | Type::Ref { .. } => (),
        }
        let structure_predicate = StructPatternNode::compile(
            ctx,
            &type_predicate,
            &spec.structure_predicate,
            scope,
        )?;
        let guard = spec
            .guard
            .as_ref()
            .map(|g| compiler::compile(ctx, g.clone(), &scope, top_id))
            .transpose()?
            .map(Cached::new);
        Ok(PatternNode { type_predicate, structure_predicate, guard })
    }

    pub(super) fn bind_event(&self, event: &mut Event<E>, v: &Value) {
        self.structure_predicate.bind(v, &mut |id, v| {
            event.variables.insert(id, v);
        })
    }

    pub(super) fn unbind_event(&self, event: &mut Event<E>) {
        self.structure_predicate.unbind(&mut |id| {
            event.variables.remove(&id);
        })
    }

    pub(super) fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        event: &mut Event<E>,
    ) -> bool {
        match &mut self.guard {
            None => false,
            Some(g) => g.update(ctx, event),
        }
    }

    pub(super) fn is_match(&self, env: &Env<C, E>, v: &Value) -> bool {
        self.type_predicate.is_a(env, v)
            && self.structure_predicate.is_match(v)
            && match &self.guard {
                None => true,
                Some(g) => g
                    .cached
                    .as_ref()
                    .and_then(|v| v.clone().get_as::<bool>())
                    .unwrap_or(false),
            }
    }
}
