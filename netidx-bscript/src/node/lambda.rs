use crate::{
    env::LambdaBind,
    expr::{Arg, Expr, ExprId, ModPath},
    node::{compiler, pattern::StructPatternNode, Node, NodeKind},
    typ::{FnType, NoRefs, Refs, TVar, Type},
    Apply, Ctx, Event, ExecCtx, InitFn, LambdaTVars, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use compact_str::format_compact;
use netidx::{subscriber::Value, utils::Either};
use smallvec::{smallvec, SmallVec};
use std::{fmt::Debug, hash::Hash, marker::PhantomData, mem, sync::Arc as SArc};
use triomphe::Arc;

atomic_id!(LambdaId);

pub(super) struct Lambda<C: Ctx, E: UserEvent> {
    args: Box<[StructPatternNode]>,
    spec: LambdaTVars,
    body: Node<C, E>,
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Lambda<C, E> {
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
        let spec = &mut self.spec;
        for (arg, (_, typ)) in args.iter_mut().zip(spec.argspec.iter()) {
            wrap!(arg, arg.typecheck(ctx))?;
            wrap!(arg, typ.check_contains(&arg.typ))?;
        }
        wrap!(self.body, self.body.typecheck(ctx))?;
        wrap!(self.body, spec.rtype.check_contains(&self.body.typ))?;
        if !spec.rtype.is_defined() {
            spec.rtype = self.body.typ.clone();
        }
        for (tv, tc) in spec.constraints.iter() {
            tc.check_contains(&Type::TVar(tv.clone()))?
        }
        Ok(())
    }

    fn rtype(&self) -> &Type<NoRefs> {
        &self.spec.rtype
    }
}

impl<C: Ctx, E: UserEvent> Lambda<C, E> {
    pub(super) fn new(
        ctx: &mut ExecCtx<C, E>,
        spec: LambdaTVars,
        args: &[Node<C, E>],
        scope: &ModPath,
        tid: ExprId,
        body: Expr,
    ) -> Result<Self> {
        if args.len() != spec.argspec.len() {
            bail!("arity mismatch, expected {} arguments", spec.argspec.len())
        }
        let id = LambdaId::new();
        let mut argids = vec![];
        let scope = ModPath(scope.0.append(&format_compact!("fn{}", id.0)));
        for ((a, typ), node) in spec.argspec.iter().zip(args.iter()) {
            let pattern = StructPatternNode::compile(ctx, &typ, &a.pattern, &scope)?;
            if pattern.is_refutable() {
                bail!(
                    "refutable patterns are not allowed in lambda arguments {}",
                    a.pattern
                )
            }
            if let Some(l) = node.find_lambda() {
                if let Some(id) = pattern.lambda_ok() {
                    ctx.env.by_id[&id].fun = Some(l)
                } else {
                    bail!("cannot pass a lambda to this argument pattern")
                }
            }
            argids.push(pattern);
        }
        let body = compiler::compile(ctx, body, &scope, tid);
        match body.extract_err() {
            None => Ok(Self { args: Box::from(argids), spec, body }),
            Some(e) => bail!("{e}"),
        }
    }
}

pub(super) struct BuiltIn<C: Ctx, E: UserEvent> {
    name: &'static str,
    typ: FnType<NoRefs>,
    spec: LambdaTVars,
    apply: Box<dyn Apply<C, E> + Send + Sync + 'static>,
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for BuiltIn<C, E> {
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
        let spec = &self.spec;
        if spec.argspec.len() != self.typ.args.len()
            || spec.vargs.is_none() && self.typ.vargs.is_some()
        {
            bail!("in builtin {} arity mismatch in builtin specification", self.name)
        }
        if args.len() < self.typ.args.len()
            || (args.len() > self.typ.args.len() && self.typ.vargs.is_none())
        {
            let vargs = if self.typ.vargs.is_some() { "at least " } else { "" };
            bail!("expected {}{} arguments got {}", spec.argspec.len(), vargs, args.len())
        }
        for (i, ((_, typ), a)) in
            spec.argspec.iter().zip(self.typ.args.iter()).enumerate()
        {
            wrap!(args[i], a.typ.check_contains(typ))?
        }
        if let Some(vtyp) = &spec.vargs {
            if let Some(atyp) = &self.typ.vargs {
                atyp.check_contains(&vtyp)?;
            }
        }
        self.typ.rtype.check_contains(&spec.rtype)?;
        for i in 0..args.len() {
            wrap!(args[i], args[i].typecheck(ctx))?;
            let atyp = if i < spec.argspec.len() {
                &spec.argspec[i].1
            } else {
                spec.vargs.as_ref().unwrap()
            };
            wrap!(args[i], atyp.check_contains(&args[i].typ))?
        }
        for (tv, tc) in spec.constraints.iter() {
            tc.check_contains(&Type::TVar(tv.clone()))?
        }
        self.apply.typecheck(ctx, args)?;
        Ok(())
    }

    fn rtype(&self) -> &Type<NoRefs> {
        &self.spec.rtype
    }
}

impl<C: Ctx, E: UserEvent> Node<C, E> {
    pub(super) fn find_lambda(&self) -> Option<Arc<LambdaBind<C, E>>> {
        match &self.kind {
            NodeKind::Lambda(l) => Some(l.clone()),
            NodeKind::Do(children) => children.last().and_then(|t| t.find_lambda()),
            NodeKind::Constant(_)
            | NodeKind::Any { .. }
            | NodeKind::Use
            | NodeKind::Bind { .. }
            | NodeKind::Ref(_)
            | NodeKind::StructRef(_, _)
            | NodeKind::TupleRef(_, _)
            | NodeKind::Connect(_, _)
            | NodeKind::Array { .. }
            | NodeKind::Tuple { .. }
            | NodeKind::Struct { .. }
            | NodeKind::StructWith { .. }
            | NodeKind::Apply { .. }
            | NodeKind::Error { .. }
            | NodeKind::Qop(_, _)
            | NodeKind::Module(_)
            | NodeKind::Eq { .. }
            | NodeKind::Ne { .. }
            | NodeKind::Lt { .. }
            | NodeKind::Gt { .. }
            | NodeKind::Gte { .. }
            | NodeKind::Lte { .. }
            | NodeKind::And { .. }
            | NodeKind::Or { .. }
            | NodeKind::Not { .. }
            | NodeKind::Add { .. }
            | NodeKind::Sub { .. }
            | NodeKind::Mul { .. }
            | NodeKind::Div { .. }
            | NodeKind::TypeCast { .. }
            | NodeKind::TypeDef
            | NodeKind::Select { .. } => None,
        }
    }
}

pub(super) fn compile<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    spec: Expr,
    argspec: Arc<[Arg]>,
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
    let scope = scope.clone();
    let _scope = scope.clone();
    let env = ctx.env.clone();
    let _env = ctx.env.clone();
    let vargs = match vargs {
        None => None,
        Some(None) => Some(None),
        Some(Some(typ)) => match typ.resolve_typrefs(&scope, &ctx.env) {
            Ok(typ) => Some(Some(typ)),
            Err(e) => error!("{e}",),
        },
    };
    let rtype = match rtype {
        None => None,
        Some(typ) => match typ.resolve_typrefs(&scope, &ctx.env) {
            Ok(typ) => Some(typ),
            Err(e) => error!("{e}",),
        },
    };
    let targspec = argspec
        .iter()
        .map(|a| match &a.constraint {
            None => Ok((a.clone(), None)),
            Some(typ) => {
                let typ = typ.resolve_typrefs(&scope, &ctx.env)?;
                Ok((a.clone(), Some(typ)))
            }
        })
        .collect::<Result<SmallVec<[_; 16]>>>();
    let targspec = match targspec {
        Ok(a) => a,
        Err(e) => error!("{e}",),
    };
    let constraints = constraints
        .iter()
        .map(|(tv, tc)| {
            let tv = tv.resolve_typrefs(&scope, &env)?;
            let tc = tc.resolve_typrefs(&scope, &env)?;
            Ok((tv, tc))
        })
        .collect::<Result<SmallVec<[_; 4]>>>();
    let constraints = match constraints {
        Ok(c) => c,
        Err(e) => error!("{e}",),
    };
    let init: InitFn<C, E> = SArc::new(move |ctx, args, tid| {
        // restore the lexical environment to the state it was in
        // when the closure was created
        let snap = ctx.env.restore_lexical_env(&_env);
        let orig_env = mem::replace(&mut ctx.env, snap);
        let argspec = Arc::from_iter(targspec.iter().map(|(name, typ)| match typ {
            None => (name.clone(), Type::empty_tvar()),
            Some(typ) => (name.clone(), typ.reset_tvars()),
        }));
        let vargs = match vargs.as_ref() {
            Some(Some(typ)) => Some(typ.reset_tvars()),
            Some(None) => Some(Type::empty_tvar()),
            None => None,
        };
        let rtype = match rtype.as_ref() {
            None => Type::empty_tvar(),
            Some(typ) => typ.reset_tvars(),
        };
        let constraints = Arc::from_iter(constraints.iter().map(|(tv, typ)| {
            (TVar::<NoRefs>::empty_named(tv.name.clone()), typ.reset_tvars())
        }));
        let spec = LambdaTVars { argspec, vargs, rtype, constraints };
        spec.setup_aliases();
        let res = match body.clone() {
            Either::Right(builtin) => match ctx.builtins.get_key_value(&*builtin) {
                None => bail!("unknown builtin function {builtin}"),
                Some((name, (typ, init))) => {
                    let name = *name;
                    let init = SArc::clone(init);
                    typ.resolve_typerefs(&_scope, &ctx.env).and_then(|typ| {
                        init(ctx, &typ, &_scope, args, tid).map(|apply| {
                            let f: Box<dyn Apply<C, E> + Send + Sync + 'static> =
                                Box::new(BuiltIn { name, typ, spec, apply });
                            f
                        })
                    })
                }
            },
            Either::Left(body) => {
                let apply = Lambda::new(ctx, spec, args, &_scope, tid, (*body).clone());
                apply.map(|a| {
                    let f: Box<dyn Apply<C, E> + Send + Sync + 'static> = Box::new(a);
                    f
                })
            }
        };
        ctx.env = ctx.env.merge_lexical(&orig_env);
        res
    });
    let kind = NodeKind::Lambda(Arc::new(LambdaBind { env, argspec, init, scope }));
    Node { spec: Box::new(spec), typ: Type::Bottom(PhantomData), kind }
}
