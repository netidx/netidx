use crate::{
    expr::{ExprId, ModPath, Pattern, StructurePattern},
    node::{compiler, Cached},
    typ::{NoRefs, Type},
    BindId, Ctx, Event, ExecCtx, VAR_BATCH,
};
use anyhow::{bail, Result};
use arcstr::ArcStr;
use fxhash::FxHashSet;
use netidx::{publisher::Typ, subscriber::Value};
use std::{fmt::Debug, hash::Hash, iter};

pub enum StructPatternNode {
    BindAll { name: Option<BindId> },
    Slice { binds: Box<[Option<BindId>]> },
    SlicePrefix { prefix: Box<[Option<BindId>]>, tail: Option<BindId> },
    SliceSuffix { head: Option<BindId>, suffix: Box<[Option<BindId>]> },
}

pub struct PatternNode<C: Ctx + 'static, E: Debug + Clone + 'static> {
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

impl<C: Ctx + 'static, E: Debug + Clone + 'static> PatternNode<C, E> {
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
            | Type::Tuple(_) => (),
            Type::Ref(_) => unreachable!(),
        }
        macro_rules! with_pref_suf {
            ($single:expr, $multi:expr) => {
                match &type_predicate {
                    Type::Array(et) => {
                        let names = $multi
                            .iter()
                            .chain(iter::once($single))
                            .filter_map(|x| x.as_ref());
                        if !uniq(names) {
                            bail!("bound variables must have unique names")
                        }
                        let single = $single.as_ref().map(|n| {
                            ctx.env.bind_variable(scope, n, type_predicate.clone()).id
                        });
                        let multi = $multi.iter().map(|n| {
                            n.as_ref().map(|n| {
                                ctx.env.bind_variable(scope, n, (**et).clone()).id
                            })
                        });
                        let multi = Box::from_iter(multi);
                        (single, multi)
                    }
                    t => bail!("slice patterns can't match {t}"),
                }
            };
        }
        let structure_predicate = match &spec.structure_predicate {
            StructurePattern::BindAll { name: Some(name) } => {
                let id = ctx.env.bind_variable(scope, name, type_predicate.clone()).id;
                StructPatternNode::BindAll { name: Some(id) }
            }
            StructurePattern::BindAll { name: None } => {
                StructPatternNode::BindAll { name: None }
            }
            StructurePattern::Slice { binds } => match &type_predicate {
                Type::Array(et) => {
                    if !uniq(binds.iter().filter_map(|x| x.as_ref())) {
                        bail!("bound variables must have unique names")
                    }
                    let ids = binds.iter().map(|name| {
                        name.as_ref()
                            .map(|n| ctx.env.bind_variable(scope, n, (**et).clone()).id)
                    });
                    StructPatternNode::Slice { binds: Box::from_iter(ids) }
                }
                t => bail!("slice patterns can't match {t}"),
            },
            StructurePattern::SlicePrefix { prefix, tail } => {
                let (tail, prefix) = with_pref_suf!(tail, prefix);
                StructPatternNode::SlicePrefix { prefix, tail }
            }
            StructurePattern::SliceSuffix { head, suffix } => {
                let (head, suffix) = with_pref_suf!(head, suffix);
                StructPatternNode::SliceSuffix { head, suffix }
            }
        };
        let guard = spec
            .guard
            .as_ref()
            .map(|g| Cached::new(compiler::compile(ctx, g.clone(), &scope, top_id)));
        Ok(PatternNode { type_predicate, structure_predicate, guard })
    }

    pub(super) fn extract_err(&self) -> Option<ArcStr> {
        self.guard.as_ref().and_then(|n| n.node.extract_err())
    }

    pub(super) fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> bool {
        match &mut self.guard {
            None => false,
            Some(g) => g.update(ctx, event),
        }
    }

    pub(super) fn bind_event(&self, v: &Value) -> Option<Event<E>> {
        match &self.structure_predicate {
            StructPatternNode::BindAll { name: None } => None,
            StructPatternNode::BindAll { name: Some(id) } => {
                Some(Event::Variable(*id, v.clone()))
            }
            StructPatternNode::Slice { binds } => match v {
                Value::Array(a) if a.len() == binds.len() => {
                    let mut vars = VAR_BATCH.take();
                    for (j, id) in binds.iter().enumerate() {
                        if let Some(id) = id {
                            vars.push((*id, a[j].clone()))
                        }
                    }
                    Some(Event::VarBatch(vars))
                }
                _ => None,
            },
            StructPatternNode::SlicePrefix { prefix, tail } => match v {
                Value::Array(a) if a.len() >= prefix.len() => {
                    let mut vars = VAR_BATCH.take();
                    for (j, id) in prefix.iter().enumerate() {
                        if let Some(id) = id {
                            vars.push((*id, a[j].clone()))
                        }
                    }
                    if let Some(id) = tail {
                        let ss = a.subslice(prefix.len()..).unwrap();
                        vars.push((*id, Value::Array(ss)))
                    }
                    Some(Event::VarBatch(vars))
                }
                _ => None,
            },
            StructPatternNode::SliceSuffix { head, suffix } => match v {
                Value::Array(a) if a.len() >= suffix.len() => {
                    let mut vars = VAR_BATCH.take();
                    if let Some(id) = head {
                        let ss = a.subslice(..suffix.len()).unwrap();
                        vars.push((*id, Value::Array(ss)))
                    }
                    let tail = a.subslice(suffix.len()..).unwrap();
                    for (j, id) in suffix.iter().enumerate() {
                        if let Some(id) = id {
                            vars.push((*id, tail[j].clone()))
                        }
                    }
                    Some(Event::VarBatch(vars))
                }
                _ => None,
            },
        }
    }

    pub(super) fn is_match(&self, typ: Typ, v: &Value) -> bool {
        let tmatch = match (&self.type_predicate, typ) {
            (Type::Array(_), Typ::Array) => true,
            _ => self.type_predicate.contains(&Type::Primitive(typ.into())),
        };
        tmatch && {
            let smatch = match &self.structure_predicate {
                StructPatternNode::BindAll { name: _ } => true,
                StructPatternNode::Slice { binds } => match v {
                    Value::Array(a) => a.len() == binds.len(),
                    _ => false,
                },
                StructPatternNode::SlicePrefix { prefix, tail: _ } => match v {
                    Value::Array(a) => a.len() >= prefix.len(),
                    _ => false,
                },
                StructPatternNode::SliceSuffix { head: _, suffix } => match v {
                    Value::Array(a) => a.len() >= suffix.len(),
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
