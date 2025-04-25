use crate::{
    expr::{ExprId, ExprKind},
    node::{genn, Node, NodeKind},
    typ::{FnArgType, Type},
    Ctx, ExecCtx, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use enumflags2::BitFlags;
use netidx::publisher::Typ;
use smallvec::SmallVec;
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
                wrap!(lhs.node.typ.check_contains(&rhs.node.typ))?;
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
            NodeKind::ArraySlice(n) => {
                wrap!(n.source.node, n.source.node.typecheck(ctx))?;
                let it = Type::Primitive(Typ::unsigned_integer());
                let at = Type::Array(Arc::new(Type::empty_tvar()));
                wrap!(n.source.node, at.check_contains(&n.source.node.typ))?;
                if let Some(start) = n.start.as_mut() {
                    wrap!(start.node, start.node.typecheck(ctx))?;
                    wrap!(start.node, it.check_contains(&start.node.typ))?;
                }
                if let Some(end) = n.end.as_mut() {
                    wrap!(end.node, end.node.typecheck(ctx))?;
                    wrap!(end.node, it.check_contains(&end.node.typ))?;
                }
                wrap!(self.typ.check_contains(&at))
            }
            NodeKind::ArrayRef(n) => {
                wrap!(n.source.node, n.source.node.typecheck(ctx))?;
                wrap!(n.i.node, n.i.node.typecheck(ctx))?;
                let et = Type::empty_tvar();
                let at = Type::Array(Arc::new(et.clone()));
                wrap!(at.check_contains(&n.source.node.typ))?;
                wrap!(Type::Primitive(Typ::integer()).check_contains(&n.i.node.typ))?;
                Ok(wrap!(self.typ.check_contains(&et))?)
            }
            NodeKind::StringInterpolate { args } => {
                for a in args.iter_mut() {
                    wrap!(a.node, a.node.typecheck(ctx))?
                }
                Ok(())
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
            NodeKind::TupleRef { source, field: i, top_id: _ } => {
                wrap!(source.typecheck(ctx))?;
                let etyp = source.typ.with_deref(|typ| match typ {
                    Some(Type::Tuple(flds)) if flds.len() > *i => Ok(flds[*i].clone()),
                    None => bail!("type must be known, annotations needed"),
                    _ => bail!("expected tuple with at least {i} elements"),
                });
                let etyp = wrap!(etyp)?;
                wrap!(self.typ.check_contains(&etyp))
            }
            NodeKind::StructRef { source, field: i, top_id: _ } => {
                wrap!(source.typecheck(ctx))?;
                let field = match &self.spec.kind {
                    ExprKind::StructRef { source: _, field } => field.clone(),
                    _ => bail!("BUG: miscompiled struct ref"),
                };
                let etyp = source.typ.with_deref(|typ| match typ {
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
            NodeKind::StructWith { source, current: _, replace } => {
                wrap!(source.typecheck(ctx))?;
                let fields = match &self.spec.kind {
                    ExprKind::StructWith { source: _, replace } => replace
                        .iter()
                        .map(|(n, _)| n.clone())
                        .collect::<SmallVec<[ArcStr; 8]>>(),
                    _ => bail!("BUG: miscompiled structwith"),
                };
                wrap!(source.typ.with_deref(|typ| match typ {
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
                wrap!(self.typ.check_contains(&source.typ))
            }
            NodeKind::Select(sn) => {
                let rtype = wrap!(sn.typecheck(ctx))?;
                wrap!(self.typ.check_contains(&rtype))
            }
            NodeKind::Apply(site) => {
                for n in site.args.iter_mut() {
                    wrap!(n, n.typecheck(ctx))?
                }
                site.ftype.unbind_tvars();
                for (arg, FnArgType { typ, .. }) in
                    site.args.iter_mut().zip(site.ftype.args.iter())
                {
                    wrap!(arg, arg.typecheck(ctx))?;
                    wrap!(arg, typ.check_contains(&arg.typ))?;
                }
                wrap!(site.ftype.rtype.check_contains(&self.typ))?;
                for (tv, tc) in site.ftype.constraints.read().iter() {
                    tc.check_contains(&Type::TVar(tv.clone()))?
                }
                Ok(())
            }
            NodeKind::Lambda(lds) => {
                let mut faux_args = Box::from_iter(lds.typ.args.iter().map(|a| {
                    let mut n: Node<C, E> = genn::nop();
                    n.typ = a.typ.clone();
                    n
                }));
                let mut f = wrap!((lds.init)(ctx, &faux_args, ExprId::new()))?;
                let res = wrap!(f.typecheck(ctx, &mut faux_args));
                f.typ().constrain_known();
                f.delete(ctx);
                res
            }
            NodeKind::Module(children) => {
                for n in children.iter_mut() {
                    wrap!(n, n.typecheck(ctx))?
                }
                Ok(())
            }
            NodeKind::Constant(_)
            | NodeKind::Use { .. }
            | NodeKind::TypeDef { .. }
            | NodeKind::Ref { .. }
            | NodeKind::Error { .. }
            | NodeKind::Nop => Ok(()),
        }
    }
}
