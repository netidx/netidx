use super::{compiler::compile, Cached};
use crate::{
    err,
    expr::{Expr, ExprId, ModPath},
    typ::Type,
    update_args, wrap, Ctx, Event, ExecCtx, Node, Refs, Update, UserEvent,
};
use anyhow::Result;
use arcstr::literal;
use netidx_value::{Typ, ValArray, Value};
use triomphe::Arc;

#[derive(Debug)]
pub(crate) struct ArrayRef<C: Ctx, E: UserEvent> {
    source: Cached<C, E>,
    i: Cached<C, E>,
    spec: Expr,
    typ: Type,
}

impl<C: Ctx, E: UserEvent> ArrayRef<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        source: &Expr,
        i: &Expr,
    ) -> Result<Node<C, E>> {
        let source = Cached::new(compile(ctx, source.clone(), scope, top_id)?);
        let i = Cached::new(compile(ctx, i.clone(), scope, top_id)?);
        let ert = Type::Primitive(Typ::Error.into());
        let typ = match &source.node.typ() {
            Type::Array(et) => Type::Set(Arc::from_iter([(**et).clone(), ert.clone()])),
            _ => Type::Set(Arc::from_iter([Type::empty_tvar(), ert.clone()])),
        };
        Ok(Box::new(Self { source, i, spec, typ }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for ArrayRef<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        let up = self.source.update(ctx, event);
        let up = self.i.update(ctx, event) || up;
        if !up {
            return None;
        }
        let i = match &self.i.cached {
            Some(Value::I64(i)) => *i,
            Some(v) => match v.clone().cast_to::<i64>() {
                Ok(i) => i,
                Err(_) => return err!("op::index(array, index): expected an integer"),
            },
            None => return None,
        };
        match &self.source.cached {
            Some(Value::Array(elts)) if i >= 0 => {
                let i = i as usize;
                if i < elts.len() {
                    Some(elts[i].clone())
                } else {
                    err!("array index out of bounds")
                }
            }
            Some(Value::Array(elts)) if i < 0 => {
                let len = elts.len();
                let i = len as i64 + i;
                if i > 0 {
                    Some(elts[i as usize].clone())
                } else {
                    err!("array index out of bounds")
                }
            }
            None => None,
            _ => err!("op::index(array, index): expected an array"),
        }
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.source.node, self.source.node.typecheck(ctx))?;
        wrap!(self.i.node, self.i.node.typecheck(ctx))?;
        let at = Type::Array(Arc::new(self.typ.clone()));
        wrap!(self, at.check_contains(&ctx.env, self.source.node.typ()))?;
        let int = Type::Primitive(Typ::integer());
        wrap!(self.i.node, int.check_contains(&ctx.env, self.i.node.typ()))
    }

    fn refs(&self, refs: &mut Refs) {
        self.source.node.refs(refs);
        self.i.node.refs(refs);
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.source.node.delete(ctx);
        self.i.node.delete(ctx);
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn sleep(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.source.sleep(ctx);
        self.i.sleep(ctx);
    }
}

#[derive(Debug)]
pub(crate) struct ArraySlice<C: Ctx, E: UserEvent> {
    source: Cached<C, E>,
    start: Option<Cached<C, E>>,
    end: Option<Cached<C, E>>,
    spec: Expr,
    typ: Type,
}

impl<C: Ctx, E: UserEvent> ArraySlice<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        source: &Expr,
        start: &Option<Arc<Expr>>,
        end: &Option<Arc<Expr>>,
    ) -> Result<Node<C, E>> {
        let source = Cached::new(compile(ctx, source.clone(), scope, top_id)?);
        let start = start
            .as_ref()
            .map(|e| compile(ctx, (**e).clone(), scope, top_id).map(Cached::new))
            .transpose()?;
        let end = end
            .as_ref()
            .map(|e| compile(ctx, (**e).clone(), scope, top_id).map(Cached::new))
            .transpose()?;
        let typ = Type::Set(Arc::from_iter([
            source.node.typ().clone(),
            Type::Primitive(Typ::Error.into()),
        ]));
        Ok(Box::new(Self { spec, typ, source, start, end }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for ArraySlice<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        macro_rules! number {
            ($e:expr) => {
                match $e.clone().cast_to::<usize>() {
                    Ok(i) => i,
                    Err(_) => return err!("expected a non negative number"),
                }
            };
        }
        macro_rules! bound {
            ($bound:expr) => {{
                match $bound.cached.as_ref() {
                    None => return None,
                    Some(Value::U64(i) | Value::V64(i)) => Some(*i as usize),
                    Some(v) => Some(number!(v)),
                }
            }};
        }
        let up = self.source.update(ctx, event);
        let up = self.start.as_mut().map(|c| c.update(ctx, event)).unwrap_or(false) || up;
        let up = self.end.as_mut().map(|c| c.update(ctx, event)).unwrap_or(false) || up;
        if !up {
            return None;
        }
        let (start, end) = match (&self.start, &self.end) {
            (None, None) => (None, None),
            (Some(c), None) => (bound!(c), None),
            (None, Some(c)) => (None, bound!(c)),
            (Some(c0), Some(c1)) => (bound!(c0), bound!(c1)),
        };
        match &self.source.cached {
            Some(Value::Array(elts)) => match (start, end) {
                (None, None) => Some(Value::Array(elts.clone())),
                (Some(i), Some(j)) => match elts.subslice(i..j) {
                    Ok(a) => Some(Value::Array(a)),
                    Err(e) => Some(Value::Error(e.to_string().into())),
                },
                (Some(i), None) => match elts.subslice(i..) {
                    Ok(a) => Some(Value::Array(a)),
                    Err(e) => Some(Value::Error(e.to_string().into())),
                },
                (None, Some(i)) => match elts.subslice(..i) {
                    Ok(a) => Some(Value::Array(a)),
                    Err(e) => Some(Value::Error(e.to_string().into())),
                },
            },
            Some(_) => err!("expected array"),
            None => None,
        }
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.source.node, self.source.node.typecheck(ctx))?;
        let it = Type::Primitive(Typ::integer());
        wrap!(
            self.source.node,
            self.typ.check_contains(&ctx.env, &self.source.node.typ())
        )?;
        if let Some(start) = self.start.as_mut() {
            wrap!(start.node, start.node.typecheck(ctx))?;
            wrap!(start.node, it.check_contains(&ctx.env, &start.node.typ()))?;
        }
        if let Some(end) = self.end.as_mut() {
            wrap!(end.node, end.node.typecheck(ctx))?;
            wrap!(end.node, it.check_contains(&ctx.env, &end.node.typ()))?;
        }
        Ok(())
    }

    fn refs(&self, refs: &mut Refs) {
        self.source.node.refs(refs);
        if let Some(start) = &self.start {
            start.node.refs(refs)
        }
        if let Some(end) = &self.end {
            end.node.refs(refs)
        }
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.source.node.delete(ctx);
        if let Some(start) = &mut self.start {
            start.node.delete(ctx);
        }
        if let Some(end) = &mut self.end {
            end.node.delete(ctx);
        }
    }

    fn sleep(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.source.sleep(ctx);
        if let Some(start) = &mut self.start {
            start.sleep(ctx);
        }
        if let Some(end) = &mut self.end {
            end.sleep(ctx);
        }
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }
}

#[derive(Debug)]
pub(crate) struct Array<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    n: Box<[Cached<C, E>]>,
}

impl<C: Ctx, E: UserEvent> Array<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        args: &Arc<[Expr]>,
    ) -> Result<Node<C, E>> {
        let n = args
            .iter()
            .map(|e| Ok(Cached::new(compile(ctx, e.clone(), scope, top_id)?)))
            .collect::<Result<_>>()?;
        let typ = Type::Array(Arc::new(Type::empty_tvar()));
        Ok(Box::new(Self { spec, typ, n }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Array<C, E> {
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

    fn sleep(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.iter_mut().for_each(|n| n.sleep(ctx))
    }

    fn refs(&self, refs: &mut Refs) {
        self.n.iter().for_each(|n| n.node.refs(refs))
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        for n in &mut self.n {
            wrap!(n.node, n.node.typecheck(ctx))?
        }
        let rtype = Type::Bottom;
        let rtype = wrap!(
            self,
            self.n
                .iter()
                .fold(Ok(rtype), |rtype, n| n.node.typ().union(&ctx.env, &rtype?))
        )?;
        let rtype = Type::Array(Arc::new(rtype));
        Ok(self.typ.check_contains(&ctx.env, &rtype)?)
    }
}
