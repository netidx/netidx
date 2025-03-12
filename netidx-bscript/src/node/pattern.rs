use crate::{
    expr::{ExprId, ModPath, Pattern, StructurePattern, ValPat},
    node::{compiler, Cached},
    typ::{NoRefs, Type},
    BindId, Ctx, Event, ExecCtx, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use netidx::{publisher::Typ, subscriber::Value};
use smallvec::SmallVec;
use std::fmt::Debug;

#[derive(Debug)]
pub enum ValPNode {
    Ignore,
    Literal(Value),
    Bind(BindId),
}

impl ValPNode {
    fn compile<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        type_predicate: &Type<NoRefs>,
        spec: &ValPat,
        scope: &ModPath,
    ) -> Result<Self> {
        match spec {
            ValPat::Ignore => Ok(Self::Ignore),
            ValPat::Literal(v) => {
                let t = Type::Primitive(Typ::get(v).into());
                type_predicate.check_contains(&t)?;
                Ok(Self::Literal(v.clone()))
            }
            ValPat::Bind(name) => {
                let id = ctx.env.bind_variable(scope, name, type_predicate.clone()).id;
                Ok(Self::Bind(id))
            }
        }
    }

    fn id(&self) -> Option<BindId> {
        match self {
            Self::Ignore | Self::Literal(_) => None,
            Self::Bind(id) => Some(*id),
        }
    }

    fn is_match(&self, v1: &Value) -> bool {
        match self {
            ValPNode::Ignore | ValPNode::Bind(_) => true,
            ValPNode::Literal(v0) => v0 == v1,
        }
    }
}

#[derive(Debug)]
pub enum StructPatternNode {
    BindAll {
        name: ValPNode,
    },
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
        binds: Box<[(usize, StructPatternNode)]>,
    },
}

impl StructPatternNode {
    pub fn compile<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        type_predicate: &Type<NoRefs>,
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
        type_predicate: &Type<NoRefs>,
        spec: &StructurePattern,
        scope: &ModPath,
    ) -> Result<Self> {
        macro_rules! with_pref_suf {
            ($all:expr, $single:expr, $multi:expr) => {
                match &type_predicate {
                    Type::Array(et) => {
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
                    t => bail!("slice patterns can't match {t}"),
                }
            };
        }
        let t = match &spec {
            StructurePattern::BindAll { name } => StructPatternNode::BindAll {
                name: ValPNode::compile(ctx, type_predicate, &name, scope)?,
            },
            StructurePattern::SlicePrefix { all, prefix, tail } => {
                let (all, tail, prefix) = with_pref_suf!(all, tail, prefix);
                StructPatternNode::SlicePrefix { all, prefix, tail }
            }
            StructurePattern::SliceSuffix { all, head, suffix } => {
                let (all, head, suffix) = with_pref_suf!(all, head, suffix);
                StructPatternNode::SliceSuffix { all, head, suffix }
            }
            StructurePattern::Slice { all, binds } => match &type_predicate {
                Type::Array(et) => {
                    let all = all.as_ref().map(|n| {
                        ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                    });
                    let binds = binds
                        .iter()
                        .map(|b| Self::compile_int(ctx, et, b, scope))
                        .collect::<Result<Box<[Self]>>>()?;
                    StructPatternNode::Slice { tuple: false, all, binds }
                }
                t => bail!("slice patterns can't match {t}"),
            },
            StructurePattern::Tuple { all, binds } => match &type_predicate {
                Type::Tuple(elts) => {
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
                    StructPatternNode::Slice { tuple: true, all, binds }
                }
                t => bail!("tuple patterns can't match {t}"),
            },
            StructurePattern::Struct { exhaustive, all, binds } => {
                struct Ifo {
                    index: usize,
                    pattern: StructurePattern,
                    typ: Type<NoRefs>,
                }
                match &type_predicate {
                    Type::Struct(elts) => {
                        let binds = binds
                            .iter()
                            .map(|(field, pat)| {
                                let r = elts.iter().enumerate().find_map(
                                    |(i, (name, typ))| {
                                        if field == name {
                                            Some(Ifo {
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
                            .iter()
                            .map(|ifo| {
                                Ok((
                                    ifo.index,
                                    Self::compile_int(
                                        ctx,
                                        &ifo.typ,
                                        &ifo.pattern,
                                        scope,
                                    )?,
                                ))
                            })
                            .collect::<Result<Box<[(usize, Self)]>>>()?;
                        StructPatternNode::Struct { all, binds }
                    }
                    t => bail!("struct patterns can't match {t}"),
                }
            }
        };
        Ok(t)
    }

    pub fn bind<F: FnMut(BindId, Value)>(&self, v: &Value, mut f: F) {
        match &self {
            StructPatternNode::BindAll { name } => {
                if let Some(id) = name.id() {
                    f(id, v.clone())
                }
            }
            StructPatternNode::Slice { tuple: _, all, binds } => match v {
                Value::Array(a) if a.len() == binds.len() => {
                    if let Some(id) = all {
                        f(*id, v.clone());
                    }
                    for (j, n) in binds.iter().enumerate() {
                        n.bind(&a[j], &mut f)
                    }
                }
                _ => (),
            },
            StructPatternNode::SlicePrefix { all, prefix, tail } => match v {
                Value::Array(a) if a.len() >= prefix.len() => {
                    if let Some(id) = all {
                        f(*id, v.clone())
                    }
                    for (j, n) in prefix.iter().enumerate() {
                        n.bind(&a[j], &mut f)
                    }
                    if let Some(id) = tail {
                        let ss = a.subslice(prefix.len()..).unwrap();
                        f(*id, Value::Array(ss))
                    }
                }
                _ => (),
            },
            StructPatternNode::SliceSuffix { all, head, suffix } => match v {
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
                        n.bind(&tail[j], &mut f)
                    }
                }
                _ => (),
            },
            StructPatternNode::Struct { all, binds } => match v {
                Value::Array(a) if a.len() >= binds.len() => {
                    if let Some(id) = all {
                        f(*id, v.clone())
                    }
                    for (i, n) in binds.iter() {
                        if let Some(v) = a.get(*i) {
                            match v {
                                Value::Array(a) if a.len() == 2 => n.bind(&a[1], &mut f),
                                _ => (),
                            }
                        }
                    }
                }
                _ => (),
            },
        }
    }

    pub fn unbind<F: FnMut(BindId)>(&self, mut f: F) {
        match &self {
            StructPatternNode::BindAll { name } => {
                if let Some(id) = name.id() {
                    f(id)
                }
            }
            StructPatternNode::Slice { tuple: _, all, binds } => {
                if let Some(id) = all {
                    f(*id)
                }
                for n in binds.iter() {
                    n.unbind(&mut f)
                }
            }
            StructPatternNode::SlicePrefix { all, prefix, tail } => {
                if let Some(id) = all {
                    f(*id)
                }
                if let Some(id) = tail {
                    f(*id)
                }
                for n in prefix.iter() {
                    n.unbind(&mut f)
                }
            }
            StructPatternNode::SliceSuffix { all, head, suffix } => {
                if let Some(id) = all {
                    f(*id)
                }
                if let Some(id) = head {
                    f(*id)
                }
                for n in suffix.iter() {
                    n.unbind(&mut f)
                }
            }
            StructPatternNode::Struct { all, binds } => {
                if let Some(id) = all {
                    f(*id)
                }
                for (_, n) in binds.iter() {
                    n.unbind(&mut f)
                }
            }
        }
    }

    pub fn is_match(&self, v: &Value) -> bool {
        match &self {
            StructPatternNode::BindAll { name } => name.is_match(v),
            StructPatternNode::Slice { tuple: _, all: _, binds } => match v {
                Value::Array(a) => {
                    a.len() == binds.len()
                        && binds.iter().zip(a.iter()).all(|(b, v)| b.is_match(v))
                }
                _ => false,
            },
            StructPatternNode::SlicePrefix { all: _, prefix, tail: _ } => match v {
                Value::Array(a) => {
                    a.len() >= prefix.len()
                        && prefix.iter().zip(a.iter()).all(|(b, v)| b.is_match(v))
                }
                _ => false,
            },
            StructPatternNode::SliceSuffix { all: _, head: _, suffix } => match v {
                Value::Array(a) => {
                    a.len() >= suffix.len()
                        && suffix
                            .iter()
                            .zip(a.iter().skip(a.len() - suffix.len()))
                            .all(|(b, v)| b.is_match(v))
                }
                _ => false,
            },
            StructPatternNode::Struct { all: _, binds } => match v {
                Value::Array(a) => {
                    a.len() >= binds.len()
                        && binds.iter().all(|(i, p)| match a.get(*i) {
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
            Self::BindAll { name: ValPNode::Bind(_) | ValPNode::Ignore } => false,
            Self::BindAll { name: ValPNode::Literal(_) } => true,
            Self::Slice { tuple: true, all: _, binds } => {
                binds.iter().any(|p| p.is_refutable())
            }
            Self::Struct { all: _, binds } => binds.iter().any(|(_, p)| p.is_refutable()),
            Self::Slice { tuple: false, .. }
            | Self::SlicePrefix { .. }
            | Self::SliceSuffix { .. } => true,
        }
    }

    pub fn lambda_ok(&self) -> Option<BindId> {
        match self {
            Self::BindAll { name: ValPNode::Bind(id) } => Some(*id),
            Self::BindAll { name: ValPNode::Literal(_) | ValPNode::Ignore }
            | Self::Slice { .. }
            | Self::SlicePrefix { .. }
            | Self::SliceSuffix { .. }
            | Self::Struct { .. } => None,
        }
    }
}

pub struct PatternNode<C: Ctx, E: UserEvent> {
    pub type_predicate: Type<NoRefs>,
    pub structure_predicate: StructPatternNode,
    pub guard: Option<Cached<C, E>>,
}

impl<C: Ctx, E: UserEvent> PatternNode<C, E> {
    pub(super) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: &Pattern,
        scope: &ModPath,
        top_id: ExprId,
    ) -> Result<Self> {
        let type_predicate = match &spec.type_predicate {
            Some(t) => t.resolve_typrefs(scope, &ctx.env)?,
            None => spec.structure_predicate.infer_type_predicate(),
        };
        match &type_predicate {
            Type::Fn(_) => bail!("can't match on Fn type"),
            Type::Bottom(_)
            | Type::Primitive(_)
            | Type::Set(_)
            | Type::TVar(_)
            | Type::Array(_)
            | Type::Tuple(_)
            | Type::Struct(_) => (),
            Type::Ref(_) => unreachable!(),
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
            .map(|g| Cached::new(compiler::compile(ctx, g.clone(), &scope, top_id)));
        Ok(PatternNode { type_predicate, structure_predicate, guard })
    }

    pub(super) fn bind_event(&self, event: &mut Event<E>, v: &Value) {
        self.structure_predicate.bind(v, |id, v| {
            event.variables.insert(id, v);
        })
    }

    pub(super) fn unbind_event(&self, event: &mut Event<E>) {
        self.structure_predicate.unbind(|id| {
            event.variables.remove(&id);
        })
    }

    pub(super) fn extract_err(&self) -> Option<ArcStr> {
        self.guard.as_ref().and_then(|n| n.node.extract_err())
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

    pub(super) fn is_match(&self, typ: Typ, v: &Value) -> bool {
        let tmatch = match (&self.type_predicate, typ) {
            (Type::Array(_), Typ::Array)
            | (Type::Tuple(_), Typ::Array)
            | (Type::Struct(_), Typ::Array) => true,
            _ => self.type_predicate.contains(&Type::Primitive(typ.into())),
        };
        tmatch
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
