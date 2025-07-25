use super::{compiler::compile, pattern::StructPatternNode, Cached};
use crate::{
    expr::{Expr, ExprId, ModPath, Pattern},
    node::pattern::PatternNode,
    typ::Type,
    BindId, Ctx, Event, ExecCtx, Node, Refs, Update, UserEvent, REFS,
};
use anyhow::{anyhow, bail, Context, Result};
use combine::stream::position::SourcePosition;
use compact_str::format_compact;
use enumflags2::BitFlags;
use netidx::subscriber::Value;
use netidx_value::Typ;
use smallvec::{smallvec, SmallVec};
use std::collections::hash_map::Entry;

atomic_id!(SelectId);

#[derive(Debug)]
pub(crate) struct Select<C: Ctx, E: UserEvent> {
    selected: Option<usize>,
    arg: Cached<C, E>,
    arms: SmallVec<[(PatternNode<C, E>, Cached<C, E>); 8]>,
    typ: Type,
    spec: Expr,
}

impl<C: Ctx, E: UserEvent> Select<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        arg: &Expr,
        arms: &[(Pattern, Expr)],
        pos: &SourcePosition,
    ) -> Result<Node<C, E>> {
        let arg = Cached::new(compile(ctx, arg.clone(), scope, top_id)?);
        let mut atype = arg.node.typ().clone();
        let arms = arms
            .iter()
            .map(|(pat, spec)| {
                let scope =
                    ModPath(scope.append(&format_compact!("sel{}", SelectId::new().0)));
                let pat = PatternNode::compile(ctx, &atype, pat, &scope, top_id)
                    .with_context(|| format!("in select at {pos}"))?;
                if !pat.guard.is_some() && !pat.structure_predicate.is_refutable() {
                    atype = atype.diff(&ctx.env, &pat.type_predicate)?;
                }
                let n = Cached::new(compile(ctx, spec.clone(), &scope, top_id)?);
                Ok((pat, n))
            })
            .collect::<Result<SmallVec<_>>>()
            .with_context(|| format!("in select at {pos}"))?;
        let typ = Type::empty_tvar();
        Ok(Box::new(Self { spec, typ, arg, arms, selected: None }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Select<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        let Self { selected, arg, arms, typ: _, spec: _ } = self;
        let mut pat_up = false;
        let arg_up = arg.update(ctx, event);
        macro_rules! bind {
            ($i:expr) => {{
                if let Some(arg) = arg.cached.as_ref() {
                    arms[$i].0.bind_event(ctx, event, arg);
                }
            }};
        }
        for (pat, _) in arms.iter_mut() {
            if arg_up && pat.guard.is_some() {
                if let Some(arg) = arg.cached.as_ref() {
                    pat.bind_event(ctx, event, arg);
                }
            }
            pat_up |= pat.update(ctx, event);
            if arg_up && pat.guard.is_some() {
                pat.unbind_event(event);
            }
        }
        if !arg_up && !pat_up {
            self.selected.and_then(|i| {
                if arms[i].1.update(ctx, event) {
                    arms[i].1.cached.clone()
                } else {
                    None
                }
            })
        } else {
            let sel = match arg.cached.as_ref() {
                None => None,
                Some(v) => arms.iter().enumerate().find_map(|(i, (pat, _))| {
                    if pat.is_match(&ctx.env, v) {
                        Some(i)
                    } else {
                        None
                    }
                }),
            };
            match (sel, *selected) {
                (Some(i), Some(j)) if i == j => {
                    if arg_up {
                        bind!(i);
                    }
                    if arms[i].1.update(ctx, event) || arg_up {
                        arms[i].1.cached.clone()
                    } else {
                        None
                    }
                }
                (Some(i), Some(_) | None) => {
                    let mut set: SmallVec<[BindId; 32]> = smallvec![];
                    if let Some(j) = *selected {
                        arms[j].1.node.sleep(ctx);
                    }
                    *selected = Some(i);
                    bind!(i);
                    REFS.with_borrow_mut(|refs| {
                        refs.clear();
                        arms[i].1.node.refs(refs);
                        refs.with_external_refs(|id| {
                            if let Entry::Vacant(e) = event.variables.entry(id)
                                && let Some(v) = ctx.cached.get(&id)
                            {
                                e.insert(v.clone());
                                set.push(id);
                            }
                        });
                    });
                    let init = event.init;
                    event.init = true;
                    arms[i].1.update(ctx, event);
                    event.init = init;
                    for id in set {
                        event.variables.remove(&id);
                    }
                    arms[i].1.cached.clone()
                }
                (None, Some(j)) => {
                    arms[j].1.node.sleep(ctx);
                    *selected = None;
                    None
                }
                (None, None) => None,
            }
        }
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        let Self { selected: _, arg, arms, typ: _, spec: _ } = self;
        arg.node.delete(ctx);
        for (pat, arg) in arms {
            arg.node.delete(ctx);
            pat.delete(ctx);
        }
    }

    fn sleep(&mut self, ctx: &mut ExecCtx<C, E>) {
        let Self { selected: _, arg, arms, typ: _, spec: _ } = self;
        arg.sleep(ctx);
        for (pat, arg) in arms {
            arg.sleep(ctx);
            if let Some(n) = &mut pat.guard {
                n.sleep(ctx)
            }
        }
    }

    fn refs(&self, refs: &mut Refs) {
        let Self { selected: _, arg, arms, typ: _, spec: _ } = self;
        arg.node.refs(refs);
        for (pat, arg) in arms {
            arg.node.refs(refs);
            pat.structure_predicate.ids(&mut |id| {
                refs.bound.insert(id);
            });
            if let Some(n) = &pat.guard {
                n.node.refs(refs);
            }
        }
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        self.arg.node.typecheck(ctx)?;
        let mut rtype = Type::Primitive(BitFlags::empty());
        let mut mtype = Type::Primitive(BitFlags::empty());
        let mut itype = Type::Primitive(BitFlags::empty());
        let mut saw_true = false; // oooooh it's such a hack
        let mut saw_false = false; // round two round two
        for (pat, n) in self.arms.iter_mut() {
            match &mut pat.guard {
                Some(guard) => guard.node.typecheck(ctx)?,
                None => {
                    if !pat.structure_predicate.is_refutable() {
                        mtype = mtype.union(&ctx.env, &pat.type_predicate)?
                    } else if let StructPatternNode::Literal(Value::Bool(b)) =
                        &pat.structure_predicate
                    {
                        saw_true |= b;
                        saw_false |= !b;
                        if saw_true && saw_false {
                            mtype = mtype
                                .union(&ctx.env, &Type::Primitive(Typ::Bool.into()))?;
                        }
                    }
                }
            }
            itype = itype.union(&ctx.env, &pat.type_predicate)?;
            n.node.typecheck(ctx)?;
            rtype = rtype.union(&ctx.env, n.node.typ())?;
        }
        itype
            .check_contains(&ctx.env, &self.arg.node.typ())
            .map_err(|e| anyhow!("missing match cases {e}"))?;
        mtype
            .check_contains(&ctx.env, &self.arg.node.typ())
            .map_err(|e| anyhow!("missing match cases {e}"))?;
        let mut atype = self.arg.node.typ().clone().normalize();
        for (pat, _) in self.arms.iter() {
            if !&pat.type_predicate.could_match(&ctx.env, &atype)? {
                bail!(
                    "pattern {} will never match {}, unused match cases",
                    pat.type_predicate,
                    atype
                )
            }
            if !pat.structure_predicate.is_refutable() && pat.guard.is_none() {
                atype = atype.diff(&ctx.env, &pat.type_predicate)?;
            }
        }
        self.typ = rtype;
        Ok(())
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }
}
