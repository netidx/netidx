use super::{compiler::compile, Cached};
use crate::{
    expr::{Expr, ExprId, ModPath},
    typ::Type,
    wrap, Ctx, Event, ExecCtx, Node, Refs, Update, UserEvent,
};
use anyhow::Result;
use netidx_value::{Typ, Value};

macro_rules! compare_op {
    ($name:ident, $op:tt) => {
        #[derive(Debug)]
        pub(crate) struct $name<C: Ctx, E: UserEvent> {
            spec: Expr,
            typ: Type,
            lhs: Cached<C, E>,
            rhs: Cached<C, E>,
        }

        impl<C: Ctx, E: UserEvent> $name<C, E> {
            pub(crate) fn compile(
                ctx: &mut ExecCtx<C, E>,
                spec: Expr,
                scope: &ModPath,
                top_id: ExprId,
                lhs: &Expr,
                rhs: &Expr
            ) -> Result<Node<C, E>> {
                let lhs = Cached::new(compile(ctx, lhs.clone(), scope, top_id)?);
                let rhs = Cached::new(compile(ctx, rhs.clone(), scope, top_id)?);
                let typ = Type::Primitive(Typ::Bool.into());
                Ok(Box::new(Self { spec, typ, lhs, rhs }))
            }
        }

        impl<C: Ctx, E: UserEvent> Update<C, E> for $name<C, E> {
            fn update(
                &mut self,
                ctx: &mut ExecCtx<C, E>,
                event: &mut Event<E>,
            ) -> Option<Value> {
                let lhs_up = self.lhs.update(ctx, event);
                let rhs_up = self.rhs.update(ctx, event);
                if lhs_up || rhs_up {
                    return self.lhs.cached.as_ref().and_then(|lhs| {
                        self.rhs.cached.as_ref().map(|rhs| (lhs $op rhs).into())
                    })
                }
                None
            }

            fn spec(&self) -> &Expr {
                &self.spec
            }

            fn typ(&self) -> &Type {
                &self.typ
            }

            fn refs(&self, refs: &mut Refs) {
                self.lhs.node.refs(refs);
                self.rhs.node.refs(refs);
            }

            fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
                self.lhs.node.delete(ctx);
                self.rhs.node.delete(ctx)
            }

            fn sleep(&mut self, ctx: &mut ExecCtx<C, E>) {
                self.lhs.node.sleep(ctx);
                self.rhs.node.sleep(ctx)
            }

            fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
                wrap!(self.lhs.node, self.lhs.node.typecheck(ctx))?;
                wrap!(self.rhs.node, self.rhs.node.typecheck(ctx))?;
                wrap!(
                    self,
                    self.lhs.node.typ().check_contains(&ctx.env, &self.rhs.node.typ())
                )?;
                wrap!(self, self.typ.check_contains(&ctx.env, &Type::boolean()))
            }
        }
    };
}

compare_op!(Eq, ==);
compare_op!(Ne, !=);
compare_op!(Lt, <);
compare_op!(Gt, >);
compare_op!(Lte, <=);
compare_op!(Gte, >=);

macro_rules! bool_op {
    ($name:ident, $op:tt) => {
        #[derive(Debug)]
        pub(crate) struct $name<C: Ctx, E: UserEvent> {
            spec: Expr,
            typ: Type,
            lhs: Cached<C, E>,
            rhs: Cached<C, E>,
        }

        impl<C: Ctx, E: UserEvent> $name<C, E> {
            pub(crate) fn compile(
                ctx: &mut ExecCtx<C, E>,
                spec: Expr,
                scope: &ModPath,
                top_id: ExprId,
                lhs: &Expr,
                rhs: &Expr
            ) -> Result<Node<C, E>> {
                let lhs = Cached::new(compile(ctx, lhs.clone(), scope, top_id)?);
                let rhs = Cached::new(compile(ctx, rhs.clone(), scope, top_id)?);
                let typ = Type::Primitive(Typ::Bool.into());
                Ok(Box::new(Self { spec, typ, lhs, rhs }))
            }
        }

        impl<C: Ctx, E: UserEvent> Update<C, E> for $name<C, E> {
            fn update(
                &mut self,
                ctx: &mut ExecCtx<C, E>,
                event: &mut Event<E>,
            ) -> Option<Value> {
                let lhs_up = self.lhs.update(ctx, event);
                let rhs_up = self.rhs.update(ctx, event);
                if lhs_up || rhs_up {
                    return match (self.lhs.cached.as_ref(), self.rhs.cached.as_ref()) {
                        (Some(Value::Bool(b0)), Some(Value::Bool(b1))) => Some(Value::Bool(*b0 $op *b1)),
                        (_, _) => None
                    }
                }
                None
            }

            fn spec(&self) -> &Expr {
                &self.spec
            }

            fn typ(&self) -> &Type {
                &self.typ
            }

            fn refs(&self, refs: &mut Refs) {
                self.lhs.node.refs(refs);
                self.rhs.node.refs(refs);
            }

            fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
                self.lhs.node.delete(ctx);
                self.rhs.node.delete(ctx)
            }

            fn sleep(&mut self, ctx: &mut ExecCtx<C, E>) {
                self.lhs.sleep(ctx);
                self.rhs.sleep(ctx)
            }

            fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
                wrap!(self.lhs.node, self.lhs.node.typecheck(ctx))?;
                wrap!(self.rhs.node, self.rhs.node.typecheck(ctx))?;
                let bt = Type::Primitive(Typ::Bool.into());
                wrap!(self.lhs.node, bt.check_contains(&ctx.env, self.lhs.node.typ()))?;
                wrap!(self.rhs.node, bt.check_contains(&ctx.env, self.rhs.node.typ()))?;
                wrap!(self, self.typ.check_contains(&ctx.env, &Type::boolean()))
            }
        }
    };
}

bool_op!(And, &&);
bool_op!(Or, ||);

#[derive(Debug)]
pub(crate) struct Not<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    n: Node<C, E>,
}

impl<C: Ctx, E: UserEvent> Not<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        n: &Expr,
    ) -> Result<Node<C, E>> {
        let n = compile(ctx, n.clone(), scope, top_id)?;
        let typ = Type::Primitive(Typ::Bool.into());
        Ok(Box::new(Self { spec, typ, n }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Not<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        self.n.update(ctx, event).and_then(|v| match v {
            Value::Bool(b) => Some(Value::Bool(!b)),
            _ => None,
        })
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn refs(&self, refs: &mut Refs) {
        self.n.refs(refs);
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.delete(ctx);
    }

    fn sleep(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.sleep(ctx);
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.n, self.n.typecheck(ctx))?;
        let bt = Type::Primitive(Typ::Bool.into());
        wrap!(self.n, bt.check_contains(&ctx.env, self.n.typ()))?;
        wrap!(self, self.typ.check_contains(&ctx.env, &Type::boolean()))
    }
}

macro_rules! arith_op {
    ($name:ident, $op:tt) => {
        #[derive(Debug)]
        pub(crate) struct $name<C: Ctx, E: UserEvent> {
            spec: Expr,
            typ: Type,
            lhs: Cached<C, E>,
            rhs: Cached<C, E>
        }

        impl<C: Ctx, E: UserEvent> $name<C, E> {
            pub(crate) fn compile(
                ctx: &mut ExecCtx<C, E>,
                spec: Expr,
                scope: &ModPath,
                top_id: ExprId,
                lhs: &Expr,
                rhs: &Expr
            ) -> Result<Node<C, E>> {
                let lhs = Cached::new(compile(ctx, lhs.clone(), scope, top_id)?);
                let rhs = Cached::new(compile(ctx, rhs.clone(), scope, top_id)?);
                let typ = Type::empty_tvar();
                Ok(Box::new(Self { spec, typ, lhs, rhs }))
            }
        }

        impl<C: Ctx, E: UserEvent> Update<C, E> for $name<C, E> {
            fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
                let lhs_up = self.lhs.update(ctx, event);
                let rhs_up = self.rhs.update(ctx, event);
                if lhs_up || rhs_up {
                    return self.lhs.cached.as_ref().and_then(|lhs| {
                        self.rhs.cached.as_ref().map(|rhs| (lhs.clone() $op rhs.clone()).into())
                    })
                }
                None
            }

            fn spec(&self) -> &Expr {
                &self.spec
            }

            fn typ(&self) -> &Type {
                &self.typ
            }

            fn refs(&self, refs: &mut Refs) {
                self.lhs.node.refs(refs);
                self.rhs.node.refs(refs);
            }

            fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
                self.lhs.node.delete(ctx);
                self.rhs.node.delete(ctx);
            }

            fn sleep(&mut self, ctx: &mut ExecCtx<C, E>) {
                self.lhs.sleep(ctx);
                self.rhs.sleep(ctx);
            }

            fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
                wrap!(self.lhs.node, self.lhs.node.typecheck(ctx))?;
                wrap!(self.rhs.node, self.rhs.node.typecheck(ctx))?;
                let typ = Type::Primitive(Typ::number());
                let lhs = self.lhs.node.typ();
                let rhs = self.rhs.node.typ();
                match (lhs.with_deref(|t| t.cloned()), rhs.with_deref(|t| t.cloned())) {
                    (None, None) | (Some(_), Some(_)) => (),
                    (Some(t), None) => { let _ = rhs.contains(&ctx.env, &t); }
                    (None, Some(t)) => { let _ = lhs.contains(&ctx.env, &t); },
                }
                wrap!(self.lhs.node, typ.check_contains(&ctx.env, lhs))?;
                wrap!(self.rhs.node, typ.check_contains(&ctx.env, rhs))?;
                let ut = wrap!(self, lhs.union(&ctx.env, rhs))?;
                wrap!(self, self.typ.check_contains(&ctx.env, &ut))
            }
        }
    }
}

arith_op!(Add, +);
arith_op!(Sub, -);
arith_op!(Mul, *);
arith_op!(Div, /);
arith_op!(Mod, %);
