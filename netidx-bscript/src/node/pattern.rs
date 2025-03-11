use crate::{
    expr::{ExprId, ModPath, Pattern, StructurePattern, ValPat},
    node::{compiler, Cached},
    typ::{NoRefs, Type},
    BindId, Ctx, Event, ExecCtx, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use fxhash::FxHashSet;
use netidx::{publisher::Typ, subscriber::Value};
use smallvec::SmallVec;
use std::{fmt::Debug, hash::Hash, iter};

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
    BindAll { name: ValPNode },
    Slice { all: Option<BindId>, binds: Box<[ValPNode]> },
    SlicePrefix { all: Option<BindId>, prefix: Box<[ValPNode]>, tail: Option<BindId> },
    SliceSuffix { all: Option<BindId>, head: Option<BindId>, suffix: Box<[ValPNode]> },
    Struct { all: Option<BindId>, binds: Box<[(usize, ValPNode)]> },
}

impl StructPatternNode {
    fn compile<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        type_predicate: &Type<NoRefs>,
        spec: &StructurePattern,
        scope: &ModPath,
    ) -> Result<Self> {
        macro_rules! with_pref_suf {
            ($all:expr, $single:expr, $multi:expr) => {
                match &type_predicate {
                    Type::Array(et) => {
                        let names = $multi
                            .iter()
                            .map(|n| n.name())
                            .chain(iter::once($single.as_ref()))
                            .chain(iter::once($all.as_ref()))
                            .filter_map(|x| x);
                        if !uniq(names) {
                            bail!("bound variables must have unique names")
                        }
                        let all = $all.as_ref().map(|n| {
                            ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                        });
                        let single = $single.as_ref().map(|n| {
                            ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                        });
                        let multi = $multi
                            .iter()
                            .map(|n| ValPNode::compile(ctx, et, n, scope))
                            .collect::<Result<Box<[ValPNode]>>>()?;
                        (all, single, multi)
                    }
                    t => bail!("slice patterns can't match {t}"),
                }
            };
        }
        macro_rules! check_names {
            ($binds:expr, $all:expr, $map:expr) => {{
                let names = $binds
                    .iter()
                    .map($map)
                    .chain(iter::once($all.as_ref()))
                    .filter_map(|x| x);
                if !uniq(names) {
                    bail!("bound variables must have unique names")
                }
            }};
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
                    check_names!(binds, all, |x| x.name());
                    let all = all.as_ref().map(|n| {
                        ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                    });
                    let binds = binds
                        .iter()
                        .map(|b| ValPNode::compile(ctx, et, b, scope))
                        .collect::<Result<Box<[ValPNode]>>>()?;
                    StructPatternNode::Slice { all, binds }
                }
                t => bail!("slice patterns can't match {t}"),
            },
            StructurePattern::Tuple { all, binds } => match &type_predicate {
                Type::Tuple(elts) => {
                    check_names!(binds, all, |x| x.name());
                    if binds.len() != elts.len() {
                        bail!("expected a tuple of length {}", elts.len())
                    }
                    let all = all.as_ref().map(|n| {
                        ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                    });
                    let binds = elts
                        .iter()
                        .zip(binds.iter())
                        .map(|(t, b)| ValPNode::compile(ctx, t, b, scope))
                        .collect::<Result<Box<[ValPNode]>>>()?;
                    StructPatternNode::Slice { all, binds }
                }
                t => bail!("tuple patterns can't match {t}"),
            },
            StructurePattern::Struct { exhaustive, all, binds } => {
                match &type_predicate {
                    Type::Struct(elts) => {
                        let binds = binds
                            .iter()
                            .map(|(field, pat)| {
                                let r = elts.iter().enumerate().find_map(
                                    |(i, (name, typ))| {
                                        if field == name {
                                            Some((i, pat.clone(), typ.clone()))
                                        } else {
                                            None
                                        }
                                    },
                                );
                                r.ok_or_else(|| anyhow!("no such struct field {field}"))
                            })
                            .collect::<Result<SmallVec<[(usize, ValPat, Type<NoRefs>); 8]>>>()?;
                        check_names!(binds, all, |(_, p, _)| p.name());
                        if *exhaustive && binds.len() < elts.len() {
                            bail!("missing bindings for struct fields")
                        }
                        let all = all.as_ref().map(|n| {
                            ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                        });
                        let binds = binds
                            .iter()
                            .map(|(i, p, t)| {
                                Ok((*i, ValPNode::compile(ctx, t, p, scope)?))
                            })
                            .collect::<Result<Box<[(usize, ValPNode)]>>>()?;
                        StructPatternNode::Struct { all, binds }
                    }
                    t => bail!("struct patterns can't match {t}"),
                }
            }
        };
        Ok(t)
    }
}

pub struct PatternNode<C: Ctx, E: UserEvent> {
    pub type_predicate: Type<NoRefs>,
    pub structure_predicate: StructPatternNode,
    pub guard: Option<Cached<C, E>>,
}

fn uniq<'a, T: Hash + Eq + 'a, I: IntoIterator<Item = &'a T> + 'a>(iter: I) -> bool {
    let mut set = FxHashSet::default();
    let mut uniq = true;
    for e in iter {
        uniq &= set.insert(e);
        if !uniq {
            break;
        }
    }
    uniq
}

impl<C: Ctx, E: UserEvent> PatternNode<C, E> {
    pub(super) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: &Pattern,
        scope: &ModPath,
        top_id: ExprId,
    ) -> Result<Self> {
        let type_predicate = spec.type_predicate.resolve_typrefs(scope, &ctx.env)?;
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

    pub(super) fn bind_event(&self, event: &mut Event<E>, v: &Value) {
        match &self.structure_predicate {
            StructPatternNode::BindAll { name } => {
                if let Some(id) = name.id() {
                    event.variables.insert(id, v.clone());
                }
            }
            StructPatternNode::Slice { all, binds } => match v {
                Value::Array(a) if a.len() == binds.len() => {
                    if let Some(id) = all {
                        event.variables.insert(*id, v.clone());
                    }
                    for (j, n) in binds.iter().enumerate() {
                        if let Some(id) = n.id() {
                            event.variables.insert(id, a[j].clone());
                        }
                    }
                }
                _ => (),
            },
            StructPatternNode::SlicePrefix { all, prefix, tail } => match v {
                Value::Array(a) if a.len() >= prefix.len() => {
                    if let Some(id) = all {
                        event.variables.insert(*id, v.clone());
                    }
                    for (j, n) in prefix.iter().enumerate() {
                        if let Some(id) = n.id() {
                            event.variables.insert(id, a[j].clone());
                        }
                    }
                    if let Some(id) = tail {
                        let ss = a.subslice(prefix.len()..).unwrap();
                        event.variables.insert(*id, Value::Array(ss));
                    }
                }
                _ => (),
            },
            StructPatternNode::SliceSuffix { all, head, suffix } => match v {
                Value::Array(a) if a.len() >= suffix.len() => {
                    if let Some(id) = all {
                        event.variables.insert(*id, v.clone());
                    }
                    if let Some(id) = head {
                        let ss = a.subslice(..suffix.len()).unwrap();
                        event.variables.insert(*id, Value::Array(ss));
                    }
                    let tail = a.subslice(suffix.len()..).unwrap();
                    for (j, n) in suffix.iter().enumerate() {
                        if let Some(id) = n.id() {
                            event.variables.insert(id, tail[j].clone());
                        }
                    }
                }
                _ => (),
            },
            StructPatternNode::Struct { all, binds } => match v {
                Value::Array(a) if a.len() >= binds.len() => {
                    if let Some(id) = all {
                        event.variables.insert(*id, v.clone());
                    }
                    for (i, id) in binds.iter() {
                        if let Some(id) = id.id() {
                            if let Some(v) = a.get(*i) {
                                match v {
                                    Value::Array(a) if a.len() == 2 => {
                                        event.variables.insert(id, a[1].clone());
                                    }
                                    _ => (),
                                }
                            }
                        }
                    }
                }
                _ => (),
            },
        }
    }

    pub(super) fn unbind_event(&self, event: &mut Event<E>) {
        match &self.structure_predicate {
            StructPatternNode::BindAll { name } => {
                if let Some(id) = name.id() {
                    event.variables.remove(&id);
                }
            }
            StructPatternNode::Slice { all, binds } => {
                if let Some(id) = all {
                    event.variables.remove(id);
                }
                for id in binds.iter().filter_map(|b| b.id()) {
                    event.variables.remove(&id);
                }
            }
            StructPatternNode::SlicePrefix { all, prefix, tail } => {
                if let Some(id) = all {
                    event.variables.remove(id);
                }
                if let Some(id) = tail {
                    event.variables.remove(id);
                }
                for id in prefix.iter().filter_map(|n| n.id()) {
                    event.variables.remove(&id);
                }
            }
            StructPatternNode::SliceSuffix { all, head, suffix } => {
                if let Some(id) = all {
                    event.variables.remove(id);
                }
                if let Some(id) = head {
                    event.variables.remove(id);
                }
                for id in suffix.iter().filter_map(|p| p.id()) {
                    event.variables.remove(&id);
                }
            }
            StructPatternNode::Struct { all, binds } => {
                if let Some(id) = all {
                    event.variables.remove(id);
                }
                for id in binds.iter().filter_map(|(_, p)| p.id()) {
                    event.variables.remove(&id);
                }
            }
        }
    }

    pub(super) fn is_match(&self, typ: Typ, v: &Value) -> bool {
        let tmatch = match (&self.type_predicate, typ) {
            (Type::Array(_), Typ::Array)
            | (Type::Tuple(_), Typ::Array)
            | (Type::Struct(_), Typ::Array) => true,
            _ => self.type_predicate.contains(&Type::Primitive(typ.into())),
        };
        tmatch && {
            let smatch = match &self.structure_predicate {
                StructPatternNode::BindAll { name } => name.is_match(v),
                StructPatternNode::Slice { all: _, binds } => match v {
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
                                Some(v) => p.is_match(v),
                                None => false,
                            })
                    }
                    _ => false,
                },
            };
            smatch
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
}
