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
        match &mut self.kind {
            NodeKind::Add { lhs, rhs }
            | NodeKind::Sub { lhs, rhs }
            | NodeKind::Mul { lhs, rhs }
            | NodeKind::Div { lhs, rhs } => {
                wrap!(lhs.node.typecheck(ctx))?;
                wrap!(rhs.node.typecheck(ctx))?;
                let typ = Type::Primitive(Typ::number());
                wrap!(typ.check_contains(&ctx.env, &lhs.node.typ))?;
                wrap!(typ.check_contains(&ctx.env, &rhs.node.typ))?;
                wrap!(self
                    .typ
                    .check_contains(&ctx.env, &lhs.node.typ.union(&rhs.node.typ)))?;
                Ok(())
            }
            NodeKind::And { lhs, rhs } | NodeKind::Or { lhs, rhs } => {
                wrap!(lhs.node.typecheck(ctx))?;
                wrap!(rhs.node.typecheck(ctx))?;
                let typ = Type::Primitive(Typ::Bool.into());
                wrap!(typ.check_contains(&ctx.env, &lhs.node.typ))?;
                wrap!(typ.check_contains(&ctx.env, &rhs.node.typ))?;
                wrap!(self.typ.check_contains(&ctx.env, &Type::boolean()))?;
                Ok(())
            }
            NodeKind::Not { node } => {
                wrap!(node.typecheck(ctx))?;
                let typ = Type::Primitive(Typ::Bool.into());
                wrap!(typ.check_contains(&ctx.env, &node.typ))?;
                wrap!(self.typ.check_contains(&ctx.env, &Type::boolean()))?;
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
                wrap!(lhs.node.typ.check_contains(&ctx.env, &rhs.node.typ))?;
                wrap!(self.typ.check_contains(&ctx.env, &Type::boolean()))?;
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
                wrap!(self.typ.check_contains(&ctx.env, &node.typ))?;
                Ok(())
            }
            NodeKind::Qop(id, n) => {
                wrap!(n.typecheck(ctx))?;
                let bind =
                    ctx.env.by_id.get(id).ok_or_else(|| anyhow!("BUG: missing bind"))?;
                let err = Type::Primitive(Typ::Error.into());
                wrap!(bind.typ.check_contains(&ctx.env, &err))?;
                wrap!(err.check_contains(&ctx.env, &bind.typ))?;
                if !n.typ.contains(&ctx.env, &err)? {
                    bail!("cannot use the ? operator on a non error type")
                }
                let rtyp = n.typ.diff(&ctx.env, &err)?;
                wrap!(self.typ.check_contains(&ctx.env, &rtyp))?;
                Ok(())
            }
            NodeKind::Connect(id, node) => {
                wrap!(node.typecheck(ctx))?;
                let bind = match ctx.env.by_id.get(&id) {
                    None => bail!("BUG missing bind {id:?}"),
                    Some(bind) => bind,
                };
                wrap!(bind.typ.check_contains(&ctx.env, &node.typ))
            }
            NodeKind::Array { args } => {
                for n in args.iter_mut() {
                    wrap!(n.node, n.node.typecheck(ctx))?
                }
                let rtype = Type::Bottom;
                let rtype = args.iter().fold(rtype, |rtype, n| n.node.typ.union(&rtype));
                let rtype = Type::Array(Arc::new(rtype));
                Ok(self.typ.check_contains(&ctx.env, &rtype)?)
            }
            NodeKind::ArraySlice(n) => {
                todo!()
            }
            NodeKind::ArrayRef(n) => {
                todo!()
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
                Ok(self.typ.check_contains(&ctx.env, &rtyp)?)
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
                            t.check_contains(&ctx.env, &n.node.typ)?
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
                            t.check_contains(&ctx.env, &n.node.typ)?
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
                            t.check_contains(&ctx.env, &n.node.typ)?
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
                wrap!(self.typ.check_contains(&ctx.env, &etyp))
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
                wrap!(self.typ.check_contains(&ctx.env, &typ))
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
                                    typ.check_contains(&ctx.env, &c.node.typ)?;
                                    *i = j;
                                }
                            }
                        }
                        Ok(())
                    }
                    None => bail!("type must be known, annotations needed"),
                    _ => bail!("expected a struct"),
                }))?;
                wrap!(self.typ.check_contains(&ctx.env, &source.typ))
            }
            NodeKind::Select(sn) => {
                let rtype = wrap!(sn.typecheck(ctx))?;
                wrap!(self.typ.check_contains(&ctx.env, &rtype))
            }
            NodeKind::Apply(site) => {
                todo!()
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
                f.typ().unbind_tvars();
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
            | NodeKind::Nop => Ok(()),
        }
    }
}
