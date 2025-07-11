use super::{compiler::compile, Cached};
use crate::{
    expr::{Expr, ExprId, ExprKind, ModPath},
    typ::Type,
    update_args, wrap, BindId, Ctx, Event, ExecCtx, Node, Update, UserEvent,
};
use anyhow::{bail, Result};
use arcstr::ArcStr;
use netidx_value::{ValArray, Value};
use smallvec::{smallvec, SmallVec};
use std::iter;
use triomphe::Arc;

#[derive(Debug)]
pub(crate) struct Struct<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    names: Box<[ArcStr]>,
    n: Box<[Cached<C, E>]>,
}

impl<C: Ctx, E: UserEvent> Struct<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        args: &[(ArcStr, Expr)],
    ) -> Result<Node<C, E>> {
        let mut names: SmallVec<[ArcStr; 8]> = smallvec![];
        let n = args.iter().map(|(n, s)| {
            names.push(n.clone());
            s
        });
        let n = n
            .map(|e| Ok(Cached::new(compile(ctx, e.clone(), scope, top_id)?)))
            .collect::<Result<Box<[_]>>>()?;
        let names = Box::from_iter(names);
        let typs =
            names.iter().zip(n.iter()).map(|(n, a)| (n.clone(), a.node.typ().clone()));
        let typ = Type::Struct(Arc::from_iter(typs));
        Ok(Box::new(Self { spec, typ, names, n }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Struct<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        if self.n.is_empty() && event.init {
            return Some(Value::Array(ValArray::from([])));
        }
        let (updated, determined) = update_args!(self.n, ctx, event);
        if updated && determined {
            let iter = self.names.iter().zip(self.n.iter()).map(|(name, n)| {
                let name = Value::String(name.clone());
                let v = n.cached.clone().unwrap();
                Value::Array(ValArray::from_iter_exact([name, v].into_iter()))
            });
            Some(Value::Array(ValArray::from_iter_exact(iter)))
        } else {
            None
        }
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.iter_mut().for_each(|n| n.node.delete(ctx))
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.n.iter().for_each(|n| n.node.refs(f))
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        for n in self.n.iter_mut() {
            wrap!(n.node, n.node.typecheck(ctx))?
        }
        match &self.typ {
            Type::Struct(typs) => {
                if self.n.len() != typs.len() {
                    bail!(
                        "struct length mismatch {} fields expected vs {}",
                        typs.len(),
                        self.n.len()
                    )
                }
                for ((_, t), n) in typs.iter().zip(self.n.iter()) {
                    t.check_contains(&ctx.env, &n.node.typ())?
                }
            }
            _ => bail!("BUG: expected a struct rtype"),
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct StructWith<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    source: Node<C, E>,
    current: Option<ValArray>,
    replace: Box<[(usize, Cached<C, E>)]>,
}

impl<C: Ctx, E: UserEvent> StructWith<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        source: &Expr,
        replace: &[(ArcStr, Expr)],
    ) -> Result<Node<C, E>> {
        let source = compile(ctx, source.clone(), scope, top_id)?;
        let replace = replace
            .iter()
            .map(|(_, e)| Ok((0, Cached::new(compile(ctx, e.clone(), scope, top_id)?))))
            .collect::<Result<Box<[_]>>>()?;
        let typ = source.typ().clone();
        Ok(Box::new(Self { spec, typ, source, current: None, replace }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for StructWith<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        let mut updated = self
            .source
            .update(ctx, event)
            .map(|v| match v {
                Value::Array(a) => {
                    self.current = Some(a.clone());
                    true
                }
                _ => false,
            })
            .unwrap_or(false);
        let mut determined = self.current.is_some();
        for (_, n) in self.replace.iter_mut() {
            updated |= n.update(ctx, event);
            determined &= n.cached.is_some();
        }
        if updated && determined {
            let mut si = 0;
            let iter =
                self.current.as_ref().unwrap().iter().enumerate().map(|(i, v)| match v {
                    Value::Array(v) if v.len() == 2 => {
                        if si < self.replace.len() && i == self.replace[si].0 {
                            let r = self.replace[si].1.cached.clone().unwrap();
                            si += 1;
                            Value::Array(ValArray::from_iter_exact(
                                [v[0].clone(), r].into_iter(),
                            ))
                        } else {
                            Value::Array(v.clone())
                        }
                    }
                    _ => v.clone(),
                });
            Some(Value::Array(ValArray::from_iter_exact(iter)))
        } else {
            None
        }
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.source.delete(ctx);
        self.replace.iter_mut().for_each(|(_, n)| n.node.delete(ctx))
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.source.refs(f);
        self.replace.iter().for_each(|(_, n)| n.node.refs(f))
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.source, self.source.typecheck(ctx))?;
        let fields = match &self.spec.kind {
            ExprKind::StructWith { source: _, replace } => {
                replace.iter().map(|(n, _)| n.clone()).collect::<SmallVec<[ArcStr; 8]>>()
            }
            _ => bail!("BUG: miscompiled structwith"),
        };
        wrap!(
            self,
            self.source.typ().with_deref(|typ| match typ {
                Some(Type::Struct(flds)) => {
                    for ((i, c), n) in self.replace.iter_mut().zip(fields.iter()) {
                        let r = flds.iter().enumerate().find_map(|(i, (field, typ))| {
                            if field == n {
                                Some((i, typ))
                            } else {
                                None
                            }
                        });
                        match r {
                            None => bail!("struct has no field named {n}"),
                            Some((j, typ)) => {
                                typ.check_contains(&ctx.env, &c.node.typ())?;
                                *i = j;
                            }
                        }
                    }
                    Ok(())
                }
                None => bail!("type must be known, annotations needed"),
                _ => bail!("expected a struct"),
            })
        )?;
        wrap!(self, self.typ.check_contains(&ctx.env, self.source.typ()))
    }
}

#[derive(Debug)]
pub(crate) struct StructRef<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    source: Node<C, E>,
    field: usize,
}

impl<C: Ctx, E: UserEvent> StructRef<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        source: &Expr,
        field: &ArcStr,
    ) -> Result<Node<C, E>> {
        let source = compile(ctx, source.clone(), scope, top_id)?;
        let (typ, field) = match &source.typ() {
            Type::Struct(flds) => flds
                .iter()
                .enumerate()
                .find_map(
                    |(i, (n, t))| {
                        if field == n {
                            Some((t.clone(), i))
                        } else {
                            None
                        }
                    },
                )
                .unwrap_or_else(|| (Type::empty_tvar(), 0)),
            _ => (Type::empty_tvar(), 0),
        };
        // typcheck will resolve the field index if we didn't find it already
        Ok(Box::new(Self { spec, typ, source, field }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for StructRef<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        match self.source.update(ctx, event) {
            Some(Value::Array(a)) => a.get(self.field).and_then(|v| match v {
                Value::Array(a) if a.len() == 2 => Some(a[1].clone()),
                _ => None,
            }),
            Some(_) | None => None,
        }
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.source.refs(f)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.source.delete(ctx)
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.source, self.source.typecheck(ctx))?;
        let field = match &self.spec.kind {
            ExprKind::StructRef { source: _, field } => field.clone(),
            _ => bail!("BUG: miscompiled struct ref"),
        };
        let etyp = self.source.typ().with_deref(|typ| match typ {
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
        let (idx, typ) = wrap!(self, etyp)?;
        self.field = idx;
        wrap!(self, self.typ.check_contains(&ctx.env, &typ))
    }
}

#[derive(Debug)]
pub(crate) struct Tuple<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    n: Box<[Cached<C, E>]>,
}

impl<C: Ctx, E: UserEvent> Tuple<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        args: &[Expr],
    ) -> Result<Node<C, E>> {
        let n = args
            .iter()
            .map(|e| Ok(Cached::new(compile(ctx, e.clone(), scope, top_id)?)))
            .collect::<Result<Box<[_]>>>()?;
        let typ = Type::Tuple(Arc::from_iter(n.iter().map(|n| n.node.typ().clone())));
        Ok(Box::new(Self { spec, typ, n }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Tuple<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        if self.n.is_empty() && event.init {
            return Some(Value::Array(ValArray::from([])));
        }
        let (updated, determined) = update_args!(self.n, ctx, event);
        if updated && determined {
            let iter = self.n.iter().map(|n| n.cached.clone().unwrap());
            Some(Value::Array(ValArray::from_iter_exact(iter)))
        } else {
            None
        }
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.iter_mut().for_each(|n| n.node.delete(ctx))
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.n.iter().for_each(|n| n.node.refs(f))
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        for n in self.n.iter_mut() {
            wrap!(n.node, n.node.typecheck(ctx))?
        }
        match &self.typ {
            Type::Tuple(typs) => {
                if self.n.len() != typs.len() {
                    bail!("tuple arity mismatch {} vs {}", self.n.len(), typs.len())
                }
                for (t, n) in typs.iter().zip(self.n.iter()) {
                    t.check_contains(&ctx.env, &n.node.typ())?
                }
            }
            _ => bail!("BUG: unexpected tuple rtype"),
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Variant<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    tag: ArcStr,
    n: Box<[Cached<C, E>]>,
}

impl<C: Ctx, E: UserEvent> Variant<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        tag: &ArcStr,
        args: &[Expr],
    ) -> Result<Node<C, E>> {
        let n = args
            .iter()
            .map(|e| Ok(Cached::new(compile(ctx, e.clone(), scope, top_id)?)))
            .collect::<Result<Box<[_]>>>()?;
        let typs = Arc::from_iter(n.iter().map(|n| n.node.typ().clone()));
        let typ = Type::Variant(tag.clone(), typs);
        let tag = tag.clone();
        Ok(Box::new(Self { spec, typ, tag, n }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Variant<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        if self.n.len() == 0 {
            if event.init {
                Some(Value::String(self.tag.clone()))
            } else {
                None
            }
        } else {
            let (updated, determined) = update_args!(self.n, ctx, event);
            if updated && determined {
                let a = iter::once(Value::String(self.tag.clone()))
                    .chain(self.n.iter().map(|n| n.cached.clone().unwrap()));
                Some(Value::Array(ValArray::from_iter(a)))
            } else {
                None
            }
        }
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.iter_mut().for_each(|n| n.node.delete(ctx))
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.n.iter().for_each(|n| n.node.refs(f))
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        for n in self.n.iter_mut() {
            wrap!(n.node, n.node.typecheck(ctx))?
        }
        match &self.typ {
            Type::Variant(ttag, typs) => {
                if ttag != &self.tag {
                    bail!("expected {ttag} not {}", self.tag)
                }
                if self.n.len() != typs.len() {
                    bail!("arity mismatch {} vs {}", self.n.len(), typs.len())
                }
                for (t, n) in typs.iter().zip(self.n.iter()) {
                    wrap!(n.node, t.check_contains(&ctx.env, &n.node.typ()))?
                }
            }
            _ => bail!("BUG: unexpected variant rtype"),
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct TupleRef<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    source: Node<C, E>,
    field: usize,
}

impl<C: Ctx, E: UserEvent> TupleRef<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        source: &Expr,
        field: &usize,
    ) -> Result<Node<C, E>> {
        let source = compile(ctx, source.clone(), scope, top_id)?;
        let field = *field;
        let typ = match &source.typ() {
            Type::Tuple(ts) => {
                ts.get(field).map(|t| t.clone()).unwrap_or_else(Type::empty_tvar)
            }
            _ => Type::empty_tvar(),
        };
        Ok(Box::new(Self { spec, typ, source, field }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for TupleRef<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        self.source.update(ctx, event).and_then(|v| match v {
            Value::Array(a) => a.get(self.field).map(|v| v.clone()),
            _ => None,
        })
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.source.refs(f)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.source.delete(ctx)
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.source, self.source.typecheck(ctx))?;
        let etyp = self.source.typ().with_deref(|typ| match typ {
            Some(Type::Tuple(flds)) if flds.len() > self.field => {
                Ok(flds[self.field].clone())
            }
            None => bail!("type must be known, annotations needed"),
            _ => bail!("expected tuple with at least {} elements", self.field),
        });
        let etyp = wrap!(self, etyp)?;
        wrap!(self, self.typ.check_contains(&ctx.env, &etyp))
    }
}
