use super::genn;
use crate::{
    env::LambdaDef,
    expr::{Arg, Expr, ExprId, ModPath},
    node::{compiler, pattern::StructPatternNode, Node, NodeKind},
    typ::{FnArgType, FnType, NoRefs, Refs, TVar, Type},
    Apply, BindId, Ctx, Event, ExecCtx, InitFn, LambdaId, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use compact_str::format_compact;
use fxhash::FxHashMap;
use netidx::{subscriber::Value, utils::Either};
use parking_lot::RwLock;
use smallvec::{smallvec, SmallVec};
use std::{
    cell::RefCell, collections::HashMap, marker::PhantomData, mem, sync::Arc as SArc,
};
use triomphe::Arc;

pub(super) struct LambdaCallSite<C: Ctx, E: UserEvent> {
    args: Box<[StructPatternNode]>,
    body: Node<C, E>,
    typ: Arc<FnType<NoRefs>>,
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for LambdaCallSite<C, E> {
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
                    Err(e) => Err(anyhow!("in expr: {}, type error: {e}", $n.spec)),
                }
            };
        }
        self.typ.unbind_tvars();
        for (arg, FnArgType { typ, .. }) in args.iter_mut().zip(self.typ.args.iter()) {
            wrap!(arg, arg.typecheck(ctx))?;
            wrap!(arg, typ.check_contains(&arg.typ))?;
        }
        wrap!(self.body, self.body.typecheck(ctx))?;
        wrap!(self.body, self.typ.rtype.check_contains(&self.body.typ))?;
        for (tv, tc) in self.typ.constraints.read().iter() {
            tc.check_contains(&Type::TVar(tv.clone()))?
        }
        Ok(())
    }

    fn typ(&self) -> Arc<FnType<NoRefs>> {
        Arc::clone(&self.typ)
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.body.refs(f)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        mem::replace(&mut self.body, genn::nop()).delete(ctx)
    }
}

impl<C: Ctx, E: UserEvent> LambdaCallSite<C, E> {
    pub(super) fn new(
        ctx: &mut ExecCtx<C, E>,
        typ: Arc<FnType<NoRefs>>,
        argspec: Arc<[Arg<NoRefs>]>,
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
        let body = compiler::compile(ctx, body, &scope, tid);
        match body.extract_err() {
            None => Ok(Self { args: Box::from(argpats), typ, body }),
            Some(e) => bail!("{e}"),
        }
    }
}

pub(super) struct BuiltInCallSite<C: Ctx, E: UserEvent> {
    typ: Arc<FnType<NoRefs>>,
    apply: Box<dyn Apply<C, E> + Send + Sync + 'static>,
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for BuiltInCallSite<C, E> {
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
                    Err(e) => Err(anyhow!("in expr: {}, type error: {e}", $n.spec)),
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
            wrap!(args[i], atyp.check_contains(&args[i].typ))?
        }
        for (tv, tc) in self.typ.constraints.read().iter() {
            tc.check_contains(&Type::TVar(tv.clone()))?
        }
        self.apply.typecheck(ctx, args)?;
        self.typ.unbind_tvars();
        Ok(())
    }

    fn typ(&self) -> Arc<FnType<NoRefs>> {
        Arc::clone(&self.typ)
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.apply.refs(f)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.apply.delete(ctx)
    }
}

pub(super) fn compile<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    spec: Expr,
    argspec: Arc<[Arg<Refs>]>,
    vargs: Option<Option<Type<Refs>>>,
    rtype: Option<Type<Refs>>,
    constraints: Arc<[(TVar<Refs>, Type<Refs>)]>,
    scope: &ModPath,
    body: Either<Arc<Expr>, ArcStr>,
) -> Node<C, E> {
    macro_rules! error {
        ($msg:expr, $($arg:expr),*) => {{
            let e = ArcStr::from(format_compact!($msg, $($arg),*).as_str());
            let kind =
                NodeKind::Error { error: Some(e), children: Box::from_iter([]) };
            return Node {
                spec: Box::new(spec),
                typ: Type::Bottom(PhantomData),
                kind,
            };
        }};
    }
    {
        let mut s: SmallVec<[&ArcStr; 16]> = smallvec![];
        for a in argspec.iter() {
            a.pattern.with_names(&mut |n| s.push(n));
        }
        let len = s.len();
        s.sort();
        s.dedup();
        if len != s.len() {
            error!("arguments must have unique names",);
        }
    }
    let id = LambdaId::new();
    let scope = ModPath(scope.0.append(&format_compact!("fn{}", id.0)));
    let _scope = scope.clone();
    let env = ctx.env.clone();
    let _env = ctx.env.clone();
    let vargs = match vargs {
        None => None,
        Some(None) => Some(None),
        Some(Some(typ)) => match typ.resolve_typerefs(&scope, &ctx.env) {
            Ok(typ) => Some(Some(typ)),
            Err(e) => error!("{e}",),
        },
    };
    let rtype = match rtype {
        None => None,
        Some(typ) => match typ.resolve_typerefs(&scope, &ctx.env) {
            Ok(typ) => Some(typ),
            Err(e) => error!("{e}",),
        },
    };
    let argspec = argspec
        .iter()
        .map(|a| match &a.constraint {
            None => Ok(Arg {
                labeled: a.labeled.clone(),
                pattern: a.pattern.clone(),
                constraint: None,
            }),
            Some(typ) => {
                let typ = typ.resolve_typerefs(&scope, &ctx.env)?;
                Ok(Arg {
                    labeled: a.labeled.clone(),
                    pattern: a.pattern.clone(),
                    constraint: Some(typ),
                })
            }
        })
        .collect::<Result<SmallVec<[_; 16]>>>();
    let argspec: Arc<[Arg<NoRefs>]> = match argspec {
        Ok(a) => Arc::from_iter(a),
        Err(e) => error!("{e}",),
    };
    let constraints = constraints
        .iter()
        .map(|(tv, tc)| {
            let tv = tv.resolve_typerefs(&scope, &env)?;
            let tc = tc.resolve_typerefs(&scope, &env)?;
            Ok((tv, tc))
        })
        .collect::<Result<SmallVec<[_; 4]>>>();
    let constraints = match constraints {
        Ok(c) => Arc::new(RwLock::new(c.into_iter().collect())),
        Err(e) => error!("{e}",),
    };
    let typ = match &body {
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
            Some((styp, _)) => match styp.clone().resolve_typerefs(&_scope, &ctx.env) {
                Ok(ft) => Arc::new(ft),
                Err(e) => error!("{e:?}",),
            },
            None => error!("unknown builtin function {builtin}",),
        },
    };
    thread_local! {
        static KNOWN: RefCell<FxHashMap<ArcStr, TVar<NoRefs>>> = RefCell::new(HashMap::default());
    }
    KNOWN.with_borrow_mut(|known| {
        known.clear();
        typ.alias_tvars(known);
    });
    let _typ = typ.clone();
    let _argspec = argspec.clone();
    let init: InitFn<C, E> = SArc::new(move |ctx, args, tid| {
        // restore the lexical environment to the state it was in
        // when the closure was created
        let snap = ctx.env.restore_lexical_env(&_env);
        let orig_env = mem::replace(&mut ctx.env, snap);
        let res = match body.clone() {
            Either::Left(body) => {
                let apply = LambdaCallSite::new(
                    ctx,
                    _typ.clone(),
                    _argspec.clone(),
                    args,
                    &_scope,
                    tid,
                    (*body).clone(),
                );
                apply.map(|a| {
                    let f: Box<dyn Apply<C, E> + Send + Sync + 'static> = Box::new(a);
                    f
                })
            }
            Either::Right(builtin) => match ctx.builtins.get(&*builtin) {
                None => bail!("unknown builtin function {builtin}"),
                Some((_, init)) => {
                    let init = SArc::clone(init);
                    init(ctx, &_typ, &_scope, args, tid).map(|apply| {
                        let f: Box<dyn Apply<C, E> + Send + Sync + 'static> =
                            Box::new(BuiltInCallSite { typ: _typ.clone(), apply });
                        f
                    })
                }
            },
        };
        ctx.env = ctx.env.merge_lexical(&orig_env);
        res
    });
    let l = SArc::new(LambdaDef { id, typ: typ.clone(), env, argspec, init, scope });
    ctx.env.lambdas.insert_cow(id, SArc::downgrade(&l));
    Node { spec: Box::new(spec), typ: Type::Fn(typ), kind: NodeKind::Lambda(l) }
}
