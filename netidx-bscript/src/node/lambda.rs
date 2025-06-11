use super::{compiler::compile, Nop};
use crate::{
    env::LambdaDef,
    expr::{self, Arg, Expr, ExprId, ExprKind, ModPath},
    pattern::StructPatternNode,
    typ::{FnArgType, FnType, TVar, Type},
    wrap, Apply, BindId, Ctx, Event, ExecCtx, InitFn, LambdaId, Node, Update, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use compact_str::format_compact;
use fxhash::FxHashMap;
use netidx::{subscriber::Value, utils::Either};
use parking_lot::RwLock;
use smallvec::{smallvec, SmallVec};
use std::{cell::RefCell, collections::HashMap, mem, sync::Arc as SArc};
use triomphe::Arc;

#[derive(Debug)]
struct BScriptLambda<C: Ctx, E: UserEvent> {
    args: Box<[StructPatternNode]>,
    body: Node<C, E>,
    typ: Arc<FnType>,
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for BScriptLambda<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        for (arg, pat) in from.iter_mut().zip(&self.args) {
            if let Some(v) = arg.update(ctx, event) {
                pat.bind(&v, &mut |id, v| {
                    event.variables.insert(id, v);
                })
            }
        }
        self.body.update(ctx, event)
    }

    fn typecheck(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        args: &mut [Node<C, E>],
    ) -> Result<()> {
        macro_rules! wrap {
            ($n:expr, $e:expr) => {
                match $e {
                    Ok(()) => Ok(()),
                    Err(e) => Err(anyhow!("in expr: {}, type error: {e}", $n.spec())),
                }
            };
        }
        self.typ.unbind_tvars();
        for (arg, FnArgType { typ, .. }) in args.iter_mut().zip(self.typ.args.iter()) {
            wrap!(arg, arg.typecheck(ctx))?;
            wrap!(arg, typ.check_contains(&ctx.env, &arg.typ()))?;
        }
        wrap!(self.body, self.body.typecheck(ctx))?;
        wrap!(self.body, self.typ.rtype.check_contains(&ctx.env, &self.body.typ()))?;
        for (tv, tc) in self.typ.constraints.read().iter() {
            tc.check_contains(&ctx.env, &Type::TVar(tv.clone()))?
        }
        Ok(())
    }

    fn typ(&self) -> Arc<FnType> {
        Arc::clone(&self.typ)
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.body.refs(f)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.body.delete(ctx)
    }
}

impl<C: Ctx, E: UserEvent> BScriptLambda<C, E> {
    pub(super) fn new(
        ctx: &mut ExecCtx<C, E>,
        typ: Arc<FnType>,
        argspec: Arc<[Arg]>,
        args: &[Node<C, E>],
        scope: &ModPath,
        tid: ExprId,
        body: Expr,
    ) -> Result<Self> {
        if args.len() != argspec.len() {
            bail!("arity mismatch, expected {} arguments", argspec.len())
        }
        let mut argpats = vec![];
        for (a, atyp) in argspec.iter().zip(typ.args.iter()) {
            let pattern = StructPatternNode::compile(ctx, &atyp.typ, &a.pattern, &scope)?;
            if pattern.is_refutable() {
                bail!(
                    "refutable patterns are not allowed in lambda arguments {}",
                    a.pattern
                )
            }
            argpats.push(pattern);
        }
        let body = compile(ctx, body, &scope, tid)?;
        Ok(Self { args: Box::from(argpats), typ, body })
    }
}

#[derive(Debug)]
struct BuiltInLambda<C: Ctx, E: UserEvent> {
    typ: Arc<FnType>,
    apply: Box<dyn Apply<C, E> + Send + Sync + 'static>,
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for BuiltInLambda<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        self.apply.update(ctx, from, event)
    }

    fn typecheck(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        args: &mut [Node<C, E>],
    ) -> Result<()> {
        macro_rules! wrap {
            ($n:expr, $e:expr) => {
                match $e {
                    Ok(()) => Ok(()),
                    Err(e) => Err(anyhow!("in expr: {}, type error: {e}", $n.spec())),
                }
            };
        }
        if args.len() < self.typ.args.len()
            || (args.len() > self.typ.args.len() && self.typ.vargs.is_none())
        {
            let vargs = if self.typ.vargs.is_some() { "at least " } else { "" };
            bail!(
                "expected {}{} arguments got {}",
                self.typ.args.len(),
                vargs,
                args.len()
            )
        }
        for i in 0..args.len() {
            wrap!(args[i], args[i].typecheck(ctx))?;
            let atyp = if i < self.typ.args.len() {
                &self.typ.args[i].typ
            } else {
                self.typ.vargs.as_ref().unwrap()
            };
            wrap!(args[i], atyp.check_contains(&ctx.env, &args[i].typ()))?
        }
        for (tv, tc) in self.typ.constraints.read().iter() {
            tc.check_contains(&ctx.env, &Type::TVar(tv.clone()))?
        }
        self.apply.typecheck(ctx, args)?;
        self.typ.unbind_tvars();
        Ok(())
    }

    fn typ(&self) -> Arc<FnType> {
        Arc::clone(&self.typ)
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.apply.refs(f)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.apply.delete(ctx)
    }
}

#[derive(Debug)]
pub(crate) struct Lambda<C: Ctx, E: UserEvent> {
    spec: Expr,
    def: SArc<LambdaDef<C, E>>,
    typ: Type,
}

impl<C: Ctx, E: UserEvent> Lambda<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        l: &expr::Lambda,
    ) -> Result<Node<C, E>> {
        let mut s: SmallVec<[&ArcStr; 16]> = smallvec![];
        for a in l.args.iter() {
            a.pattern.with_names(&mut |n| s.push(n));
        }
        let len = s.len();
        s.sort();
        s.dedup();
        if len != s.len() {
            bail!("arguments must have unique names");
        }
        let id = LambdaId::new();
        let scope = ModPath(scope.0.append(&format_compact!("fn{}", id.0)));
        let _scope = scope.clone();
        let env = ctx.env.clone();
        let _env = ctx.env.clone();
        let vargs = match l.vargs.as_ref() {
            None => None,
            Some(None) => Some(None),
            Some(Some(typ)) => Some(Some(typ.scope_refs(&scope))),
        };
        let rtype = l.rtype.as_ref().map(|t| t.scope_refs(&scope));
        let argspec = l
            .args
            .iter()
            .map(|a| match &a.constraint {
                None => Arg {
                    labeled: a.labeled.clone(),
                    pattern: a.pattern.clone(),
                    constraint: None,
                },
                Some(typ) => Arg {
                    labeled: a.labeled.clone(),
                    pattern: a.pattern.clone(),
                    constraint: Some(typ.scope_refs(&scope)),
                },
            })
            .collect::<SmallVec<[_; 16]>>();
        let argspec = Arc::from_iter(argspec);
        let constraints = l
            .constraints
            .iter()
            .map(|(tv, tc)| {
                let tv = tv.scope_refs(&scope);
                let tc = tc.scope_refs(&scope);
                Ok((tv, tc))
            })
            .collect::<Result<SmallVec<[_; 4]>>>()?;
        let constraints = Arc::new(RwLock::new(constraints.into_iter().collect()));
        let typ = match &l.body {
            Either::Left(_) => {
                let args = Arc::from_iter(argspec.iter().map(|a| FnArgType {
                    label: a.labeled.as_ref().and_then(|dv| {
                        a.pattern.single_bind().map(|n| (n.clone(), dv.is_some()))
                    }),
                    typ: match a.constraint.as_ref() {
                        Some(t) => t.clone(),
                        None => Type::empty_tvar(),
                    },
                }));
                let vargs = match vargs {
                    Some(Some(t)) => Some(t.clone()),
                    Some(None) => Some(Type::empty_tvar()),
                    None => None,
                };
                let rtype = rtype.clone().unwrap_or_else(|| Type::empty_tvar());
                Arc::new(FnType { constraints, args, vargs, rtype })
            }
            Either::Right(builtin) => match ctx.builtins.get(builtin.as_str()) {
                None => bail!("unknown builtin function {builtin}"),
                Some((styp, _)) => Arc::new(styp.clone().scope_refs(&_scope)),
            },
        };
        thread_local! {
            static KNOWN: RefCell<FxHashMap<ArcStr, TVar>> = RefCell::new(HashMap::default());
        }
        KNOWN.with_borrow_mut(|known| {
            known.clear();
            typ.alias_tvars(known);
        });
        let _typ = typ.clone();
        let _argspec = argspec.clone();
        let body = l.body.clone();
        let init: InitFn<C, E> = SArc::new(move |ctx, args, tid| {
            // restore the lexical environment to the state it was in
            // when the closure was created
            let snap = ctx.env.restore_lexical_env(&_env);
            let orig_env = mem::replace(&mut ctx.env, snap);
            let res = match body.clone() {
                Either::Left(body) => {
                    let apply = BScriptLambda::new(
                        ctx,
                        _typ.clone(),
                        _argspec.clone(),
                        args,
                        &_scope,
                        tid,
                        body.clone(),
                    );
                    apply.map(|a| {
                        let f: Box<dyn Apply<C, E>> = Box::new(a);
                        f
                    })
                }
                Either::Right(builtin) => match ctx.builtins.get(&*builtin) {
                    None => bail!("unknown builtin function {builtin}"),
                    Some((_, init)) => {
                        let init = SArc::clone(init);
                        init(ctx, &_typ, &_scope, args, tid).map(|apply| {
                            let f: Box<dyn Apply<C, E>> =
                                Box::new(BuiltInLambda { typ: _typ.clone(), apply });
                            f
                        })
                    }
                },
            };
            ctx.env = ctx.env.merge_lexical(&orig_env);
            res
        });
        let def =
            SArc::new(LambdaDef { id, typ: typ.clone(), env, argspec, init, scope });
        ctx.env.lambdas.insert_cow(id, SArc::downgrade(&def));
        Ok(Box::new(Self { spec, def, typ: Type::Fn(typ) }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Lambda<C, E> {
    fn update(
        &mut self,
        _ctx: &mut ExecCtx<C, E>,
        event: &mut Event<E>,
    ) -> Option<Value> {
        if event.init {
            Some(Value::U64(self.def.id.0))
        } else {
            None
        }
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn refs<'a>(&'a self, _f: &'a mut (dyn FnMut(BindId) + 'a)) {}

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.env.lambdas.remove_cow(&self.def.id);
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        let mut faux_args = Box::from_iter(self.def.typ.args.iter().map(|a| {
            let n: Node<C, E> = Box::new(Nop {
                spec: ExprKind::Constant(Value::Bool(false)).to_expr(Default::default()),
                typ: a.typ.clone(),
            });
            n
        }));
        let mut f = wrap!(self, (self.def.init)(ctx, &faux_args, ExprId::new()))?;
        let res = wrap!(self, f.typecheck(ctx, &mut faux_args));
        f.typ().constrain_known();
        f.typ().unbind_tvars();
        f.delete(ctx);
        res
    }
}
