use crate::{
    expr::ExprKind,
    node::{Node, NodeKind},
    typ::{FnArgType, Type},
    Ctx, ExecCtx, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use enumflags2::BitFlags;
use netidx::publisher::Typ;
use smallvec::SmallVec;
use std::marker::PhantomData;
use triomphe::Arc;

impl<C: Ctx, E: UserEvent> Node<C, E> {
    pub fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        macro_rules! wrap {
            ($e:expr) => {
                wrap!(self.spec, $e)
            };
            ($n:expr, $e:expr) => {
                match $e {
                    Ok(x) => Ok(x),
                    Err(e) => Err(anyhow!("in expr: {}, type error: {e}", $n)),
                }
            };
        }
        match &mut self.kind {
            NodeKind::Add { lhs, rhs }
            | NodeKind::Sub { lhs, rhs }
            | NodeKind::Mul { lhs, rhs }
            | NodeKind::Div { lhs, rhs } => {
                wrap!(lhs.node.typecheck(ctx))?;
                wrap!(rhs.node.typecheck(ctx))?;
                let typ = Type::Primitive(Typ::number());
                wrap!(typ.check_contains(&lhs.node.typ))?;
                wrap!(typ.check_contains(&rhs.node.typ))?;
                wrap!(self.typ.check_contains(&lhs.node.typ.union(&rhs.node.typ)))?;
                Ok(())
            }
            NodeKind::And { lhs, rhs } | NodeKind::Or { lhs, rhs } => {
                wrap!(lhs.node.typecheck(ctx))?;
                wrap!(rhs.node.typecheck(ctx))?;
                let typ = Type::Primitive(Typ::Bool.into());
                wrap!(typ.check_contains(&lhs.node.typ))?;
                wrap!(typ.check_contains(&rhs.node.typ))?;
                wrap!(self.typ.check_contains(&Type::boolean()))?;
                Ok(())
            }
            NodeKind::Not { node } => {
                wrap!(node.typecheck(ctx))?;
                let typ = Type::Primitive(Typ::Bool.into());
                wrap!(typ.check_contains(&node.typ))?;
                wrap!(self.typ.check_contains(&Type::boolean()))?;
                Ok(())
            }
            NodeKind::Eq { lhs, rhs }
            | NodeKind::Ne { lhs, rhs }
            | NodeKind::Lt { lhs, rhs }
            | NodeKind::Gt { lhs, rhs }
            | NodeKind::Lte { lhs, rhs }
            | NodeKind::Gte { lhs, rhs } => {
                wrap!(lhs.node.typecheck(ctx))?;
                wrap!(rhs.node.typecheck(ctx))?;
                wrap!(self.typ.check_contains(&Type::boolean()))?;
                Ok(())
            }
            NodeKind::TypeCast { target: _, n } => Ok(wrap!(n.typecheck(ctx))?),
            NodeKind::Do(nodes) => {
                for n in nodes {
                    wrap!(n.typecheck(ctx))?;
                }
                Ok(())
            }
            NodeKind::Bind { pattern: _, node } => {
                wrap!(node.typecheck(ctx))?;
                wrap!(self.typ.check_contains(&node.typ))?;
                Ok(())
            }
            NodeKind::Qop(id, n) => {
                wrap!(n.typecheck(ctx))?;
                let bind =
                    ctx.env.by_id.get(id).ok_or_else(|| anyhow!("BUG: missing bind"))?;
                let err = Type::Primitive(Typ::Error.into());
                wrap!(bind.typ.check_contains(&err))?;
                wrap!(err.check_contains(&bind.typ))?;
                if !n.typ.contains(&err) {
                    bail!("cannot use the ? operator on a non error type")
                }
                let rtyp = n.typ.diff(&err);
                wrap!(self.typ.check_contains(&rtyp))?;
                Ok(())
            }
            NodeKind::Connect(id, node) => {
                wrap!(node.typecheck(ctx))?;
                let bind = match ctx.env.by_id.get(&id) {
                    None => bail!("BUG missing bind {id:?}"),
                    Some(bind) => bind,
                };
                wrap!(bind.typ.check_contains(&node.typ))
            }
            NodeKind::Array { args } => {
                for n in args.iter_mut() {
                    wrap!(n.node, n.node.typecheck(ctx))?
                }
                let rtype = Type::Primitive(BitFlags::empty());
                let rtype = args.iter().fold(rtype, |rtype, n| n.node.typ.union(&rtype));
                let rtype = Type::Array(Arc::new(rtype));
                Ok(self.typ.check_contains(&rtype)?)
            }
            NodeKind::Any { args } => {
                for n in args.iter_mut() {
                    wrap!(n, n.typecheck(ctx))?
                }
                let rtyp = Type::Primitive(BitFlags::empty());
                let rtyp = args.iter().fold(rtyp, |rtype, n| n.typ.union(&rtype));
                Ok(self.typ.check_contains(&rtyp)?)
            }
            NodeKind::Tuple { args } => {
                for n in args.iter_mut() {
                    wrap!(n.node, n.node.typecheck(ctx))?
                }
                match &self.typ {
                    Type::Tuple(typs) => {
                        if args.len() != typs.len() {
                            bail!("tuple arity mismatch {} vs {}", args.len(), typs.len())
                        }
                        for (t, n) in typs.iter().zip(args.iter()) {
                            t.check_contains(&n.node.typ)?
                        }
                    }
                    _ => bail!("BUG: unexpected tuple rtype"),
                }
                Ok(())
            }
            NodeKind::Variant { tag, args } => {
                for n in args.iter_mut() {
                    wrap!(n.node, n.node.typecheck(ctx))?
                }
                match &self.typ {
                    Type::Variant(ttag, typs) => {
                        if ttag != tag {
                            bail!("expected {ttag} not {tag}")
                        }
                        if args.len() != typs.len() {
                            bail!("arity mismatch {} vs {}", args.len(), typs.len())
                        }
                        for (t, n) in typs.iter().zip(args.iter()) {
                            t.check_contains(&n.node.typ)?
                        }
                    }
                    _ => bail!("BUG: unexpected variant rtype"),
                }
                Ok(())
            }
            NodeKind::Struct { names: _, args } => {
                for n in args.iter_mut() {
                    wrap!(n.node, n.node.typecheck(ctx))?
                }
                match &self.typ {
                    Type::Struct(typs) => {
                        if args.len() != typs.len() {
                            bail!(
                                "struct length mismatch {} fields expected vs {}",
                                typs.len(),
                                args.len()
                            )
                        }
                        for ((_, t), n) in typs.iter().zip(args.iter()) {
                            t.check_contains(&n.node.typ)?
                        }
                    }
                    _ => bail!("BUG: expected a struct rtype"),
                }
                Ok(())
            }
            NodeKind::TupleRef(id, i) => {
                let bind =
                    ctx.env.by_id.get(id).ok_or_else(|| anyhow!("BUG: missing bind"))?;
                let etyp = bind.typ.with_deref(|typ| match typ {
                    Some(Type::Tuple(flds)) if flds.len() > *i => Ok(flds[*i].clone()),
                    None => bail!("type must be known, annotations needed"),
                    _ => bail!("expected tuple with at least {i} elements"),
                });
                let etyp = wrap!(etyp)?;
                wrap!(self.typ.check_contains(&etyp))
            }
            NodeKind::StructRef(id, i) => {
                let bind =
                    ctx.env.by_id.get(id).ok_or_else(|| anyhow!("BUG: missing bind"))?;
                let field = match &self.spec.kind {
                    ExprKind::StructRef { name: _, field } => field.clone(),
                    _ => bail!("BUG: miscompiled struct ref"),
                };
                let etyp = bind.typ.with_deref(|typ| match typ {
                    Some(Type::Struct(flds)) => {
                        let typ = flds.iter().enumerate().find_map(|(i, (n, t))| {
                            if &field == n {
                                Some((i, t.clone()))
                            } else {
                                None
                            }
                        });
                        match typ {
                            Some((i, t)) => Ok((i, t)),
                            None => bail!("in struct, unknown field {field}"),
                        }
                    }
                    None => bail!("type must be known, annotations needed"),
                    _ => bail!("expected struct"),
                });
                let (idx, typ) = wrap!(etyp)?;
                *i = idx;
                wrap!(self.typ.check_contains(&typ))
            }
            NodeKind::StructWith { name, current: _, replace } => {
                let bind = ctx
                    .env
                    .by_id
                    .get(name)
                    .ok_or_else(|| anyhow!("BUG: missing bind"))?;
                let fields = match &self.spec.kind {
                    ExprKind::StructWith { name: _, replace } => replace
                        .iter()
                        .map(|(n, _)| n.clone())
                        .collect::<SmallVec<[ArcStr; 8]>>(),
                    _ => bail!("BUG: miscompiled structwith"),
                };
                wrap!(bind.typ.with_deref(|typ| match typ {
                    Some(Type::Struct(flds)) => {
                        for ((i, c), n) in replace.iter_mut().zip(fields.iter()) {
                            let r =
                                flds.iter().enumerate().find_map(|(i, (field, typ))| {
                                    if field == n {
                                        Some((i, typ))
                                    } else {
                                        None
                                    }
                                });
                            match r {
                                None => bail!("struct has no field named {n}"),
                                Some((j, typ)) => {
                                    typ.check_contains(&c.node.typ)?;
                                    *i = j;
                                }
                            }
                        }
                        Ok(())
                    }
                    None => bail!("type must be known, annotations needed"),
                    _ => bail!("expected a struct"),
                }))?;
                wrap!(self.typ.check_contains(&bind.typ))
            }
            NodeKind::Apply { args, function } => {
                for n in args.iter_mut() {
                    wrap!(n, n.typecheck(ctx))?
                }
                wrap!(function.typecheck(ctx, args))?;
                Ok(())
            }
            NodeKind::ApplyLate(late) => {
                for n in late.args.iter_mut() {
                    wrap!(n, n.typecheck(ctx))?
                }
                late.ftype.unbind_tvars();
                for (arg, FnArgType { typ, .. }) in
                    late.args.iter_mut().zip(late.ftype.args.iter())
                {
                    wrap!(arg, arg.typecheck(ctx))?;
                    wrap!(arg, typ.check_contains(&arg.typ))?;
                }
                Ok(())
            }
            NodeKind::Select { selected: _, arg, arms } => {
                wrap!(arg.node.typecheck(ctx))?;
                let mut rtype = Type::Bottom(PhantomData);
                let mut mtype = Type::Bottom(PhantomData);
                let mut itype = Type::Bottom(PhantomData);
                for (pat, n) in arms.iter_mut() {
                    match &mut pat.guard {
                        Some(guard) => wrap!(guard.node.typecheck(ctx))?,
                        None => mtype = mtype.union(&pat.type_predicate),
                    }
                    itype = itype.union(&pat.type_predicate);
                    wrap!(n.node.typecheck(ctx))?;
                    rtype = rtype.union(&n.node.typ);
                }
                wrap!(itype
                    .check_contains(&arg.node.typ)
                    .map_err(|e| anyhow!("missing match cases {e}")))?;
                wrap!(mtype
                    .check_contains(&arg.node.typ)
                    .map_err(|e| anyhow!("missing match cases {e}")))?;
                arg.node.typ = arg.node.typ.normalize();
                let mut atype = arg.node.typ.clone();
                for (pat, _) in arms.iter() {
                    let can_match = atype.contains(&pat.type_predicate)
                        || pat.type_predicate.contains(&atype);
                    if !can_match {
                        bail!(
                            "pattern {} will never match {}, unused match cases",
                            pat.type_predicate,
                            atype
                        )
                    }
                    if !pat.structure_predicate.is_refutable() && pat.guard.is_none() {
                        atype = atype.diff(&pat.type_predicate);
                    }
                }
                self.typ.check_contains(&rtype)
            }
            NodeKind::Constant(_)
            | NodeKind::Use
            | NodeKind::TypeDef
            | NodeKind::Module(_)
            | NodeKind::Ref(_)
            | NodeKind::Error { .. }
            | NodeKind::Lambda(_) => Ok(()),
        }
    }
}
