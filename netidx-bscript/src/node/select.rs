use super::{compiler::compile, pattern::StructPatternNode, Cached};
use crate::{
    expr::{Expr, ExprId, ModPath, Pattern},
    node::pattern::PatternNode,
    typ::Type,
    BindId, Ctx, Event, ExecCtx, Node, Update, UserEvent,
};
use anyhow::{anyhow, bail, Context, Result};
use combine::stream::position::SourcePosition;
use compact_str::format_compact;
use enumflags2::BitFlags;
use netidx::subscriber::Value;
use netidx_value::Typ;
use smallvec::{smallvec, SmallVec};

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
        let mut val_up: SmallVec<[bool; 64]> = smallvec![];
        let arg_up = arg.update(ctx, event);
        macro_rules! bind {
            ($i:expr) => {{
                if let Some(arg) = arg.cached.as_ref() {
                    arms[$i].0.bind_event(event, arg);
                }
            }};
        }
        macro_rules! update {
            () => {
                for (_, val) in arms.iter_mut() {
                    val_up.push(val.update(ctx, event));
                }
            };
        }
        macro_rules! val {
            ($i:expr) => {{
                if val_up[$i] {
                    arms[$i].1.cached.clone()
                } else {
                    None
                }
            }};
        }
        let mut pat_up = false;
        for (pat, _) in arms.iter_mut() {
            if arg_up && pat.guard.is_some() {
                if let Some(arg) = arg.cached.as_ref() {
                    pat.bind_event(event, arg);
                }
            }
            pat_up |= pat.update(ctx, event);
            if arg_up && pat.guard.is_some() {
                pat.unbind_event(event);
            }
        }
        if !arg_up && !pat_up {
            update!();
            selected.and_then(|i| val!(i))
        } else {
            let sel = match arg.cached.as_ref() {
                None => None,
                Some(v) => arms.iter().enumerate().find_map(|(i, (pat, _))| {
                    let res = pat.is_match(&ctx.env, v);
                    if res {
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
                    update!();
                    if arg_up {
                        val_up[i] = true;
                    }
                    val!(i)
                }
                (Some(i), Some(_) | None) => {
                    bind!(i);
                    update!();
                    *selected = Some(i);
                    val_up[i] = true;
                    val!(i)
                }
                (None, Some(_)) => {
                    update!();
                    *selected = None;
                    None
                }
                (None, None) => {
                    update!();
                    None
                }
            }
        }
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        let mut ids: SmallVec<[BindId; 8]> = smallvec![];
        let Self { selected: _, arg, arms, typ: _, spec: _ } = self;
        arg.node.delete(ctx);
        for (pat, arg) in arms {
            arg.node.delete(ctx);
            pat.structure_predicate.ids(&mut |id| ids.push(id));
            if let Some(n) = &mut pat.guard {
                n.node.delete(ctx);
            }
            for id in ids.drain(..) {
                ctx.env.unbind_variable(id);
            }
        }
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        let Self { selected: _, arg, arms, typ: _, spec: _ } = self;
        arg.node.refs(f);
        for (pat, arg) in arms {
            arg.node.refs(f);
            pat.structure_predicate.ids(f);
            if let Some(n) = &pat.guard {
                n.node.refs(f);
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
                        mtype = mtype.union(&pat.type_predicate)
                    } else if let StructPatternNode::Literal(Value::Bool(b)) =
                        &pat.structure_predicate
                    {
                        saw_true |= b;
                        saw_false |= !b;
                        if saw_true && saw_false {
                            mtype = mtype.union(&Type::Primitive(Typ::Bool.into()));
                        }
                    }
                }
            }
            itype = itype.union(&pat.type_predicate);
            n.node.typecheck(ctx)?;
            rtype = rtype.union(&n.node.typ());
        }
        itype
            .check_contains(&ctx.env, &self.arg.node.typ())
            .map_err(|e| anyhow!("missing match cases {e}"))?;
        mtype
            .check_contains(&ctx.env, &self.arg.node.typ())
            .map_err(|e| anyhow!("missing match cases {e}"))?;
        let mut atype = self.arg.node.typ().clone().normalize();
        for (pat, _) in self.arms.iter() {
            if !pat.type_predicate.could_match(&ctx.env, &atype)? {
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
