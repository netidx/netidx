use crate::{
    env::{Bind, LambdaBind},
    expr::{Arg, Expr, ExprId, ExprKind, ModPath, Pattern},
    typ::{FnType, NoRefs, Refs, TVar, Type},
    Apply, ApplyTyped, BindId, Ctx, Event, ExecCtx, InitFnTyped, LambdaTVars,
};
use anyhow::{anyhow, bail, Result};
use arcstr::{literal, ArcStr};
use compact_str::{format_compact, CompactString};
use fxhash::FxHashMap;
use immutable_chunkmap::set::SetS as Set;
use netidx::{publisher::Typ, subscriber::Value, utils::Either};
use smallvec::{smallvec, SmallVec};
use std::{
    fmt::{self, Debug},
    marker::PhantomData,
    mem,
    sync::Arc as SArc,
};
use triomphe::Arc;

atomic_id!(LambdaId);
atomic_id!(SelectId);

struct Lambda<C: Ctx + 'static, E: Debug + Clone + 'static> {
    eid: ExprId,
    argids: Vec<BindId>,
    spec: LambdaTVars,
    body: Node<C, E>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Apply<C, E> for Lambda<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        for (arg, id) in from.iter_mut().zip(&self.argids) {
            match arg {
                Node { kind: NodeKind::Ref(_), .. } => (), // the bind is forwarded into the body
                arg => {
                    if let Some(v) = arg.update(ctx, event) {
                        if ctx.dbg_ctx.trace {
                            ctx.dbg_ctx.add_event(
                                self.eid,
                                Some(event.clone()),
                                v.clone(),
                            )
                        }
                        ctx.user.set_var(*id, v)
                    }
                }
            }
        }
        self.body.update(ctx, event)
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> ApplyTyped<C, E> for Lambda<C, E> {
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

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Lambda<C, E> {
    fn new(
        ctx: &mut ExecCtx<C, E>,
        spec: LambdaTVars,
        args: &[Node<C, E>],
        scope: &ModPath,
        eid: ExprId,
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
            let bind = ctx.env.bind_variable(&scope, &*a.name, typ.clone());
            match &node.kind {
                NodeKind::Ref(id) => {
                    argids.push(*id);
                    let old_id = bind.id;
                    ctx.env.alias(old_id, *id)
                }
                _ => {
                    argids.push(bind.id);
                    bind.fun = node.find_lambda();
                }
            }
        }
        let body = Node::compile_int(ctx, body, &scope, tid);
        match body.extract_err() {
            None => Ok(Self { argids, spec, eid, body }),
            Some(e) => bail!("{e}"),
        }
    }
}

struct BuiltIn<C: Ctx + 'static, E: Debug + Clone + 'static> {
    name: &'static str,
    typ: FnType<NoRefs>,
    spec: LambdaTVars,
    apply: Box<dyn Apply<C, E> + Send + Sync + 'static>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Apply<C, E> for BuiltIn<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        self.apply.update(ctx, from, event)
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> ApplyTyped<C, E> for BuiltIn<C, E> {
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
        Ok(())
    }

    fn rtype(&self) -> &Type<NoRefs> {
        &self.spec.rtype
    }
}

pub struct Cached<C: Ctx + 'static, E: Debug + Clone + 'static> {
    pub cached: Option<Value>,
    pub node: Node<C, E>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Cached<C, E> {
    pub fn new(node: Node<C, E>) -> Self {
        Self { cached: None, node }
    }

    /// update the node, return whether the node updated. If it did,
    /// the updated value will be stored in the cached field, if not,
    /// the previous value will remain there.
    pub fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> bool {
        match self.node.update(ctx, event) {
            None => false,
            Some(v) => {
                self.cached = Some(v);
                true
            }
        }
    }

    /// update the node, return true if the node updated AND the new
    /// value is different from the old value. The cached field will
    /// only be updated if the value changed.
    pub fn update_changed(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> bool {
        match self.node.update(ctx, event) {
            v @ Some(_) if v == self.cached => {
                self.cached = v;
                true
            }
            Some(_) | None => false,
        }
    }
}

pub struct PatternNode<C: Ctx + 'static, E: Debug + Clone + 'static> {
    predicate: Type<NoRefs>,
    bind: BindId,
    guard: Option<Cached<C, E>>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> PatternNode<C, E> {
    fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: &Pattern,
        scope: &ModPath,
        top_id: ExprId,
    ) -> Result<Self> {
        let predicate = spec.predicate.resolve_typrefs(scope, &ctx.env)?;
        match &predicate {
            Type::Fn(_) => bail!("can't match on Fn type"),
            Type::Bottom(_) | Type::Primitive(_) | Type::Set(_) | Type::TVar(_) => (),
            Type::Ref(_) => unreachable!(),
        }
        let bind = ctx.env.bind_variable(scope, &*spec.bind, predicate.clone());
        let bind = bind.id;
        let guard = spec
            .guard
            .as_ref()
            .map(|g| Cached::new(Node::compile_int(ctx, g.clone(), &scope, top_id)));
        Ok(PatternNode { predicate, bind, guard })
    }

    fn extract_err(&self) -> Option<ArcStr> {
        self.guard.as_ref().and_then(|n| n.node.extract_err())
    }

    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> bool {
        match &mut self.guard {
            None => false,
            Some(g) => g.update(ctx, event),
        }
    }

    fn is_match(&self, typ: &Type<NoRefs>) -> bool {
        self.predicate.contains(&typ)
            && match &self.guard {
                None => true,
                Some(g) => g
                    .cached
                    .as_ref()
                    .and_then(|v| v.clone().get_as::<bool>())
                    .unwrap_or(false),
            }
    }
}

pub enum NodeKind<C: Ctx + 'static, E: Debug + Clone + 'static> {
    Use,
    TypeDef,
    Constant(Value),
    Module(Box<[Node<C, E>]>),
    Do(Box<[Node<C, E>]>),
    Bind(BindId, Box<Node<C, E>>),
    Ref(BindId),
    Connect(BindId, Box<Node<C, E>>),
    Lambda(Arc<LambdaBind<C, E>>),
    TypeCast {
        target: Typ,
        n: Box<Node<C, E>>,
    },
    Apply {
        args: Box<[Node<C, E>]>,
        function: Box<dyn ApplyTyped<C, E> + Send + Sync>,
    },
    Select {
        selected: Option<usize>,
        arg: Box<Cached<C, E>>,
        arms: Box<[(PatternNode<C, E>, Cached<C, E>)]>,
    },
    Eq {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Ne {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Lt {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Gt {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Lte {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Gte {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    And {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Or {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Not {
        node: Box<Node<C, E>>,
    },
    Add {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Sub {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Mul {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Div {
        lhs: Box<Cached<C, E>>,
        rhs: Box<Cached<C, E>>,
    },
    Error {
        error: Option<ArcStr>,
        children: Box<[Node<C, E>]>,
    },
}

pub struct Node<C: Ctx + 'static, E: Debug + Clone + 'static> {
    pub spec: Box<Expr>,
    pub typ: Type<NoRefs>,
    pub kind: NodeKind<C, E>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> fmt::Display for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.spec)
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Node<C, E> {
    fn find_lambda(&self) -> Option<Arc<LambdaBind<C, E>>> {
        match &self.kind {
            NodeKind::Constant(_)
            | NodeKind::Use
            | NodeKind::Bind(_, _)
            | NodeKind::Ref(_)
            | NodeKind::Connect(_, _)
            | NodeKind::Apply { .. }
            | NodeKind::Error { .. }
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
            NodeKind::Lambda(l) => Some(l.clone()),
            NodeKind::Do(children) => children.last().and_then(|t| t.find_lambda()),
        }
    }

    pub fn is_err(&self) -> bool {
        match &self.kind {
            NodeKind::Error { .. } => true,
            NodeKind::Constant(_)
            | NodeKind::Lambda { .. }
            | NodeKind::Do(_)
            | NodeKind::Use
            | NodeKind::Bind(_, _)
            | NodeKind::Ref(_)
            | NodeKind::Connect(_, _)
            | NodeKind::Apply { .. }
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
            | NodeKind::Select { .. } => false,
        }
    }

    /// extracts the first error
    pub fn extract_err(&self) -> Option<ArcStr> {
        match &self.kind {
            NodeKind::Error { error, children, .. } => {
                let mut s = CompactString::new("");
                if let Some(e) = error {
                    s.push_str(e);
                    s.push_str(", ");
                }
                for node in children {
                    if let Some(e) = node.extract_err() {
                        s.push_str(e.as_str());
                        s.push_str(", ");
                    }
                }
                if s.len() > 0 {
                    Some(ArcStr::from(s.as_str()))
                } else {
                    None
                }
            }
            NodeKind::Constant(_)
            | NodeKind::Lambda { .. }
            | NodeKind::Do(_)
            | NodeKind::Use
            | NodeKind::Bind(_, _)
            | NodeKind::Ref(_)
            | NodeKind::Connect(_, _)
            | NodeKind::Apply { .. }
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

    fn compile_lambda(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        argspec: Arc<[Arg]>,
        vargs: Option<Option<Type<Refs>>>,
        rtype: Option<Type<Refs>>,
        constraints: Arc<[(TVar<Refs>, Type<Refs>)]>,
        scope: &ModPath,
        body: Either<Arc<Expr>, ArcStr>,
        eid: ExprId,
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
        if argspec.len()
            != argspec.iter().map(|a| a.name.as_str()).collect::<Set<_>>().len()
        {
            error!("arguments must have unique names",);
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
        let init: InitFnTyped<C, E> = SArc::new(move |ctx, args, tid| {
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
                            init(ctx, &_scope, args, tid).map(|apply| {
                                let f: Box<dyn ApplyTyped<C, E> + Send + Sync + 'static> =
                                    Box::new(BuiltIn { name, typ, spec, apply });
                                f
                            })
                        })
                    }
                },
                Either::Left(body) => {
                    let apply =
                        Lambda::new(ctx, spec, args, &_scope, eid, tid, (*body).clone());
                    apply.map(|a| {
                        let f: Box<dyn ApplyTyped<C, E> + Send + Sync + 'static> =
                            Box::new(a);
                        f
                    })
                }
            };
            ctx.env = ctx.env.merge_lexical(&orig_env);
            res
        });
        let kind = NodeKind::Lambda(Arc::new(LambdaBind { env, argspec, init, scope }));
        Node { spec: Box::new(spec), typ: Type::empty_tvar(), kind }
    }

    fn compile_apply_args(
        ctx: &mut ExecCtx<C, E>,
        scope: &ModPath,
        top_id: ExprId,
        args: Arc<[(Option<ArcStr>, Expr)]>,
        lb: &LambdaBind<C, E>,
    ) -> Result<Box<[Node<C, E>]>> {
        let mut nodes: SmallVec<[Node<C, E>; 16]> = smallvec![];
        let mut named = FxHashMap::default();
        for (name, e) in args.iter() {
            if let Some(name) = name {
                if named.contains_key(name) {
                    bail!("duplicate labeled argument {name}")
                }
                named.insert(name.clone(), e.clone());
            }
        }
        for a in lb.argspec.iter() {
            match &a.labeled {
                None => break,
                Some(def) => match named.remove(&a.name) {
                    Some(e) => {
                        let n = Node::compile_int(ctx, e, scope, top_id);
                        if let Some(e) = n.extract_err() {
                            bail!(e);
                        }
                        nodes.push(n)
                    }
                    None => match def {
                        None => bail!("missing required argument {}", a.name),
                        Some(e) => {
                            let orig_env = ctx.env.restore_lexical_env(&lb.env);
                            let n = Node::compile_int(ctx, e.clone(), &lb.scope, top_id);
                            ctx.env = ctx.env.merge_lexical(&orig_env);
                            if let Some(e) = n.extract_err() {
                                bail!(e)
                            }
                            nodes.push(n);
                        }
                    },
                },
            }
        }
        if named.len() != 0 {
            let s = named.keys().fold(CompactString::new(""), |mut s, n| {
                if s != "" {
                    s.push_str(", ");
                }
                s.push_str(n);
                s
            });
            bail!("unknown labeled arguments passed, {s}")
        }
        for (name, e) in args.iter() {
            if name.is_none() {
                let n = Node::compile_int(ctx, e.clone(), scope, top_id);
                if let Some(e) = n.extract_err() {
                    bail!(e)
                }
                nodes.push(n);
            }
        }
        if nodes.len() < lb.argspec.len() {
            bail!("missing required argument {}", lb.argspec[nodes.len()].name)
        }
        Ok(Box::from_iter(nodes))
    }

    fn compile_apply(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        args: Arc<[(Option<ArcStr>, Expr)]>,
        f: ModPath,
    ) -> Node<C, E> {
        macro_rules! error {
            ("", $children:expr) => {{
                let kind = NodeKind::Error { error: None, children: Box::from_iter($children) };
                Node { spec: Box::new(spec), kind, typ: ctx.env.bottom() }
            }};
            ($fmt:expr, $children:expr, $($arg:expr),*) => {{
                let e = ArcStr::from(format_compact!($fmt, $($arg),*).as_str());
                let kind = NodeKind::Error { error: Some(e), children: Box::from_iter($children) };
                Node { spec: Box::new(spec), kind, typ: Type::Bottom(PhantomData) }
            }};
            ($fmt:expr) => { error!($fmt, [],) };
            ($fmt:expr, $children:expr) => { error!($fmt, $children,) };
        }
        match ctx.env.lookup_bind(scope, &f) {
            None => error!("{f} is undefined"),
            Some((_, Bind { fun: None, .. })) => {
                error!("{f} is not a function")
            }
            Some((_, Bind { fun: Some(lb), id, .. })) => {
                let varid = *id;
                let lb = lb.clone();
                let args = match Node::compile_apply_args(ctx, scope, top_id, args, &lb) {
                    Err(e) => return error!("{e}"),
                    Ok(a) => a,
                };
                match (lb.init)(ctx, &args, top_id) {
                    Err(e) => error!("error in function {f} {e:?}"),
                    Ok(function) => {
                        ctx.user.ref_var(varid, top_id);
                        let typ = function.rtype().clone();
                        let kind = NodeKind::Apply { args, function };
                        Node { spec: Box::new(spec), typ, kind }
                    }
                }
            }
        }
    }

    fn compile_int(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
    ) -> Self {
        macro_rules! subexprs {
            ($scope:expr, $exprs:expr) => {
                $exprs.iter().fold((false, vec![]), |(e, mut nodes), spec| {
                    let n = Node::compile_int(ctx, spec.clone(), &$scope, top_id);
                    let e = e || n.is_err();
                    nodes.push(n);
                    (e, nodes)
                })
            };
        }
        macro_rules! error {
            ("", $children:expr) => {{
                let kind = NodeKind::Error { error: None, children: Box::from_iter($children) };
                Node { spec: Box::new(spec), kind, typ: Type::Bottom(PhantomData) }
            }};
            ($fmt:expr, $children:expr, $($arg:expr),*) => {{
                let e = ArcStr::from(format_compact!($fmt, $($arg),*).as_str());
                let kind = NodeKind::Error { error: Some(e), children: Box::from_iter($children) };
                Node { spec: Box::new(spec), kind, typ: Type::Bottom(PhantomData) }
            }};
            ($fmt:expr) => { error!($fmt, [],) };
            ($fmt:expr, $children:expr) => { error!($fmt, $children,) };
        }
        macro_rules! binary_op {
            ($op:ident, $lhs:expr, $rhs:expr) => {{
                let lhs = Node::compile_int(ctx, (**$lhs).clone(), scope, top_id);
                let rhs = Node::compile_int(ctx, (**$rhs).clone(), scope, top_id);
                if lhs.is_err() || rhs.is_err() {
                    return error!("", [lhs, rhs]);
                }
                let lhs = Box::new(Cached::new(lhs));
                let rhs = Box::new(Cached::new(rhs));
                Node {
                    spec: Box::new(spec),
                    typ: Type::empty_tvar(),
                    kind: NodeKind::$op { lhs, rhs },
                }
            }};
        }
        match &spec {
            Expr { kind: ExprKind::Constant(v), id: _ } => {
                let typ = Type::Primitive(Typ::get(&v).into());
                Node { kind: NodeKind::Constant(v.clone()), spec: Box::new(spec), typ }
            }
            Expr { kind: ExprKind::Do { exprs }, id } => {
                let scope = ModPath(scope.append(&format_compact!("do{}", id.inner())));
                let (error, exp) = subexprs!(scope, exprs);
                if error {
                    error!("", exp)
                } else {
                    let typ = exp
                        .last()
                        .map(|n| n.typ.clone())
                        .unwrap_or_else(|| Type::Bottom(PhantomData));
                    Node { kind: NodeKind::Do(Box::from(exp)), spec: Box::new(spec), typ }
                }
            }
            Expr { kind: ExprKind::Module { name, export: _, value }, id: _ } => {
                let scope = ModPath(scope.append(&name));
                match value {
                    None => error!("module loading is not implemented"),
                    Some(exprs) => {
                        let (error, children) = subexprs!(scope, exprs);
                        if error {
                            error!("", children)
                        } else {
                            ctx.env.modules.insert_cow(scope.clone());
                            let typ = Type::Bottom(PhantomData);
                            let kind = NodeKind::Module(Box::from(children));
                            Node { spec: Box::new(spec), typ, kind }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Use { name }, id: _ } => {
                if !ctx.env.modules.contains(name) {
                    error!("no such module {name}")
                } else {
                    let used = ctx.env.used.get_or_default_cow(scope.clone());
                    Arc::make_mut(used).push(name.clone());
                    let kind = NodeKind::Use;
                    Node { spec: Box::new(spec), typ: Type::Bottom(PhantomData), kind }
                }
            }
            Expr { kind: ExprKind::Connect { name, value }, id: _ } => {
                match ctx.env.lookup_bind(scope, name) {
                    None => error!("{name} is undefined"),
                    Some((_, Bind { fun: Some(_), .. })) => {
                        error!("{name} is a function")
                    }
                    Some((_, Bind { id, fun: None, .. })) => {
                        let id = *id;
                        let node =
                            Node::compile_int(ctx, (**value).clone(), scope, top_id);
                        if node.is_err() {
                            error!("", vec![node])
                        } else {
                            let kind = NodeKind::Connect(id, Box::new(node));
                            let typ = Type::Bottom(PhantomData);
                            Node { spec: Box::new(spec), typ, kind }
                        }
                    }
                }
            }
            Expr {
                kind: ExprKind::Lambda { args, vargs, rtype, constraints, body },
                id,
            } => {
                let (args, vargs, rtype, constraints, body, id) = (
                    args.clone(),
                    vargs.clone(),
                    rtype.clone(),
                    constraints.clone(),
                    (*body).clone(),
                    *id,
                );
                Node::compile_lambda(
                    ctx,
                    spec,
                    args,
                    vargs,
                    rtype,
                    constraints,
                    scope,
                    body,
                    id,
                )
            }
            Expr { kind: ExprKind::Apply { args, function: f }, id: _ } => {
                let (args, f) = (args.clone(), f.clone());
                Node::compile_apply(ctx, spec, scope, top_id, args, f)
            }
            Expr { kind: ExprKind::Bind { export: _, name, typ, value }, id: _ } => {
                let node = Node::compile_int(ctx, (**value).clone(), &scope, top_id);
                let typ = match typ {
                    None => node.typ.clone(),
                    Some(typ) => match typ.resolve_typrefs(scope, &ctx.env) {
                        Ok(typ) => typ.clone(),
                        Err(e) => return error!("{e}", vec![node]),
                    },
                };
                let bind = ctx.env.bind_variable(scope, &**name, typ.clone());
                bind.fun = node.find_lambda();
                if node.is_err() {
                    error!("", vec![node])
                } else {
                    let kind = NodeKind::Bind(bind.id, Box::new(node));
                    Node { spec: Box::new(spec), typ, kind }
                }
            }
            Expr { kind: ExprKind::Ref { name }, id: _ } => {
                match ctx.env.lookup_bind(scope, name) {
                    None => error!("{name} not defined"),
                    Some((_, bind)) => {
                        ctx.user.ref_var(bind.id, top_id);
                        let typ = bind.typ.clone();
                        let spec = Box::new(spec);
                        match &bind.fun {
                            None => Node { spec, typ, kind: NodeKind::Ref(bind.id) },
                            Some(i) => {
                                Node { spec, typ, kind: NodeKind::Lambda(i.clone()) }
                            }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Select { arg, arms }, id: _ } => {
                let arg = Node::compile_int(ctx, (**arg).clone(), scope, top_id);
                if let Some(e) = arg.extract_err() {
                    return error!("{e}");
                }
                let arg = Box::new(Cached::new(arg));
                let (error, arms) =
                    arms.iter().fold((false, vec![]), |(e, mut nodes), (pat, spec)| {
                        let scope = ModPath(
                            scope.append(&format_compact!("sel{}", SelectId::new().0)),
                        );
                        let pat = PatternNode::compile(ctx, pat, &scope, top_id);
                        let n = Node::compile_int(ctx, spec.clone(), &scope, top_id);
                        let e = e
                            || pat.is_err()
                            || pat.as_ref().unwrap().extract_err().is_some()
                            || n.is_err();
                        nodes.push((pat, Cached::new(n)));
                        (e, nodes)
                    });
                use std::fmt::Write;
                let mut err = CompactString::new("");
                if error {
                    let mut v = vec![];
                    for (pat, n) in arms {
                        match pat {
                            Err(e) => write!(err, "{e}, ").unwrap(),
                            Ok(p) => {
                                if let Some(e) = p.extract_err() {
                                    write!(err, "{e}, ").unwrap();
                                }
                                if let Some(g) = p.guard {
                                    v.push(g.node);
                                }
                            }
                        }
                        v.push(n.node)
                    }
                    return error!("{err}", v);
                }
                let arms = Box::from_iter(arms.into_iter().map(|(p, n)| (p.unwrap(), n)));
                let kind = NodeKind::Select { selected: None, arg, arms };
                Node { spec: Box::new(spec), typ: Type::empty_tvar(), kind }
            }
            Expr { kind: ExprKind::TypeCast { expr, typ }, id: _ } => {
                let n = Node::compile_int(ctx, (**expr).clone(), scope, top_id);
                if n.is_err() {
                    return error!("", vec![n]);
                }
                let rtyp = Type::Primitive(*typ | Typ::Error);
                let kind = NodeKind::TypeCast { target: *typ, n: Box::new(n) };
                Node { spec: Box::new(spec), typ: rtyp, kind }
            }
            Expr { kind: ExprKind::TypeDef { name, typ }, id: _ } => {
                match typ.resolve_typrefs(scope, &ctx.env) {
                    Err(e) => error!("{e}"),
                    Ok(typ) => match ctx.env.deftype(scope, name, typ) {
                        Err(e) => error!("{e}"),
                        Ok(()) => {
                            let spec = Box::new(spec);
                            let typ = Type::Bottom(PhantomData);
                            Node { spec, typ, kind: NodeKind::TypeDef }
                        }
                    },
                }
            }
            Expr { kind: ExprKind::Not { expr }, id: _ } => {
                let node = Node::compile_int(ctx, (**expr).clone(), scope, top_id);
                if node.is_err() {
                    return error!("", vec![node]);
                }
                let node = Box::new(node);
                let spec = Box::new(spec);
                let typ = Type::Primitive(Typ::Bool.into());
                Node { spec, typ, kind: NodeKind::Not { node } }
            }
            Expr { kind: ExprKind::Eq { lhs, rhs }, id: _ } => binary_op!(Eq, lhs, rhs),
            Expr { kind: ExprKind::Ne { lhs, rhs }, id: _ } => binary_op!(Ne, lhs, rhs),
            Expr { kind: ExprKind::Lt { lhs, rhs }, id: _ } => binary_op!(Lt, lhs, rhs),
            Expr { kind: ExprKind::Gt { lhs, rhs }, id: _ } => binary_op!(Gt, lhs, rhs),
            Expr { kind: ExprKind::Lte { lhs, rhs }, id: _ } => binary_op!(Lte, lhs, rhs),
            Expr { kind: ExprKind::Gte { lhs, rhs }, id: _ } => binary_op!(Gte, lhs, rhs),
            Expr { kind: ExprKind::And { lhs, rhs }, id: _ } => binary_op!(And, lhs, rhs),
            Expr { kind: ExprKind::Or { lhs, rhs }, id: _ } => binary_op!(Or, lhs, rhs),
            Expr { kind: ExprKind::Add { lhs, rhs }, id: _ } => binary_op!(Add, lhs, rhs),
            Expr { kind: ExprKind::Sub { lhs, rhs }, id: _ } => binary_op!(Sub, lhs, rhs),
            Expr { kind: ExprKind::Mul { lhs, rhs }, id: _ } => binary_op!(Mul, lhs, rhs),
            Expr { kind: ExprKind::Div { lhs, rhs }, id: _ } => binary_op!(Div, lhs, rhs),
        }
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
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
            NodeKind::Bind(_, node) => {
                wrap!(node.typecheck(ctx))?;
                wrap!(self.typ.check_contains(&node.typ))?;
                Ok(())
            }
            NodeKind::Connect(_, node) => Ok(wrap!(node.typecheck(ctx))?),
            NodeKind::Apply { args, function } => {
                for n in args.iter_mut() {
                    wrap!(n, n.typecheck(ctx))?
                }
                wrap!(function.typecheck(ctx, args))?;
                Ok(())
            }
            NodeKind::Select { selected: _, arg, arms } => {
                wrap!(arg.node.typecheck(ctx))?;
                let mut rtype = Type::Bottom(PhantomData);
                let mut mtype = Type::Bottom(PhantomData);
                let mut mcases = Type::Bottom(PhantomData);
                for (pat, n) in arms {
                    mcases = mcases.union(&pat.predicate);
                    match &mut pat.guard {
                        Some(guard) => wrap!(guard.node.typecheck(ctx))?,
                        None => mtype = mtype.union(&pat.predicate),
                    }
                    wrap!(n.node.typecheck(ctx))?;
                    rtype = rtype.union(&n.node.typ);
                }
                wrap!(arg
                    .node
                    .typ
                    .check_contains(&mcases)
                    .map_err(|e| anyhow!("pattern will never match {e}")))?;
                wrap!(mtype
                    .check_contains(&arg.node.typ)
                    .map_err(|e| anyhow!("missing match cases {e}")))?;
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

    pub fn compile(ctx: &mut ExecCtx<C, E>, scope: &ModPath, spec: Expr) -> Self {
        let top_id = spec.id;
        let env = ctx.env.clone();
        let mut node = Self::compile_int(ctx, spec, scope, top_id);
        let node = match node.typecheck(ctx) {
            Ok(()) => node,
            Err(e) => Node {
                spec: node.spec.clone(),
                typ: Type::Bottom(PhantomData),
                kind: NodeKind::Error {
                    error: Some(format_compact!("{e}").as_str().into()),
                    children: Box::from_iter([node]),
                },
            },
        };
        if node.is_err() {
            ctx.env = env;
        }
        node
    }

    fn update_select(
        ctx: &mut ExecCtx<C, E>,
        selected: &mut Option<usize>,
        arg: &mut Cached<C, E>,
        arms: &mut [(PatternNode<C, E>, Cached<C, E>)],
        event: &Event<E>,
    ) -> Option<Value> {
        let mut val_up: SmallVec<[bool; 64]> = smallvec![];
        let arg_up = arg.update(ctx, event);
        macro_rules! set_arg {
            ($i:expr) => {{
                let id = arms[$i].0.bind;
                if let Some(arg) = arg.cached.as_ref() {
                    val_up[$i] |=
                        arms[$i].1.update(ctx, &Event::Variable(id, arg.clone()));
                }
            }};
        }
        macro_rules! val {
            ($i:expr) => {{
                if arg_up {
                    set_arg!($i)
                }
                if val_up[$i] {
                    arms[$i].1.cached.clone()
                } else {
                    None
                }
            }};
        }
        let mut pat_up = false;
        for (pat, val) in arms.iter_mut() {
            pat_up |= pat.update(ctx, event);
            if arg_up && pat.guard.is_some() {
                if let Some(arg) = arg.cached.as_ref() {
                    pat_up |= pat.update(ctx, &Event::Variable(pat.bind, arg.clone()));
                }
            }
            val_up.push(val.update(ctx, event));
        }
        if !arg_up && !pat_up {
            selected.and_then(|i| val!(i))
        } else {
            let typ = arg.cached.as_ref().map(|v| Type::Primitive(Typ::get(v).into()));
            let sel = arms.iter().enumerate().find_map(|(i, (pat, _))| {
                typ.as_ref()
                    .and_then(|typ| if pat.is_match(typ) { Some(i) } else { None })
            });
            match (sel, *selected) {
                (Some(i), Some(j)) if i == j => val!(i),
                (Some(i), Some(_) | None) => {
                    set_arg!(i);
                    *selected = Some(i);
                    arms[i].1.cached.clone()
                }
                (None, Some(_)) => {
                    *selected = None;
                    None
                }
                (None, None) => None,
            }
        }
    }

    pub fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> Option<Value> {
        macro_rules! binary_op {
            ($op:tt, $lhs:expr, $rhs:expr) => {{
                let lhs_up = $lhs.update(ctx, event);
                let rhs_up = $rhs.update(ctx, event);
                if lhs_up || rhs_up {
                    return $lhs.cached.as_ref().and_then(|lhs| {
                        $rhs.cached.as_ref().map(|rhs| (lhs $op rhs).into())
                    })
                }
                None
            }}
        }
        macro_rules! binary_op_clone {
            ($op:tt, $lhs:expr, $rhs:expr) => {{
                let lhs_up = $lhs.update(ctx, event);
                let rhs_up = $rhs.update(ctx, event);
                if lhs_up || rhs_up {
                    return $lhs.cached.as_ref().and_then(|lhs| {
                        $rhs.cached.as_ref().map(|rhs| (lhs.clone() $op rhs.clone()).into())
                    })
                }
                None
            }}
        }
        macro_rules! cast_bool {
            ($v:expr) => {
                match $v.cached.as_ref().map(|v| v.clone().get_as::<bool>()) {
                    None => return None,
                    Some(None) => return Some(Value::Error(literal!("expected bool"))),
                    Some(Some(lhs)) => lhs,
                }
            };
        }
        macro_rules! binary_boolean_op {
            ($op:tt, $lhs:expr, $rhs:expr) => {{
                let lhs_up = $lhs.update(ctx, event);
                let rhs_up = $rhs.update(ctx, event);
                if lhs_up || rhs_up {
                    let lhs = cast_bool!($lhs);
                    let rhs = cast_bool!($rhs);
                    Some((lhs $op rhs).into())
                } else {
                    None
                }
            }}
        }
        let eid = self.spec.id;
        let res = match &mut self.kind {
            NodeKind::Error { .. } => None,
            NodeKind::Constant(v) => match event {
                Event::Init => Some(v.clone()),
                Event::Netidx(_, _) | Event::User(_) | Event::Variable(_, _) => None,
            },
            NodeKind::Apply { args, function } => function.update(ctx, args, event),
            NodeKind::Connect(id, rhs) | NodeKind::Bind(id, rhs) => {
                if let Some(v) = rhs.update(ctx, event) {
                    if ctx.dbg_ctx.trace {
                        ctx.dbg_ctx.add_event(eid, Some(event.clone()), v.clone())
                    }
                    ctx.user.set_var(*id, v)
                }
                None
            }
            NodeKind::Ref(bid) => match event {
                Event::Variable(id, v) if bid == id => Some(v.clone()),
                Event::Init
                | Event::Netidx(_, _)
                | Event::User(_)
                | Event::Variable { .. } => None,
            },
            NodeKind::Module(children) => {
                for n in children {
                    n.update(ctx, event);
                }
                None
            }
            NodeKind::Do(children) => {
                children.into_iter().fold(None, |_, n| n.update(ctx, event))
            }
            NodeKind::TypeCast { target, n } => n.update(ctx, event).map(|v| {
                v.clone().cast(*target).unwrap_or_else(|| {
                    Value::Error(
                        format_compact!("can't cast {v} to {target}").as_str().into(),
                    )
                })
            }),
            NodeKind::Not { node } => node.update(ctx, event).map(|v| !v),
            NodeKind::Eq { lhs, rhs } => binary_op!(==, lhs, rhs),
            NodeKind::Ne { lhs, rhs } => binary_op!(!=, lhs, rhs),
            NodeKind::Lt { lhs, rhs } => binary_op!(<, lhs, rhs),
            NodeKind::Gt { lhs, rhs } => binary_op!(>, lhs, rhs),
            NodeKind::Lte { lhs, rhs } => binary_op!(<=, lhs, rhs),
            NodeKind::Gte { lhs, rhs } => binary_op!(>=, lhs, rhs),
            NodeKind::And { lhs, rhs } => binary_boolean_op!(&&, lhs, rhs),
            NodeKind::Or { lhs, rhs } => binary_boolean_op!(||, lhs, rhs),
            NodeKind::Add { lhs, rhs } => binary_op_clone!(+, lhs, rhs),
            NodeKind::Sub { lhs, rhs } => binary_op_clone!(-, lhs, rhs),
            NodeKind::Mul { lhs, rhs } => binary_op_clone!(*, lhs, rhs),
            NodeKind::Div { lhs, rhs } => binary_op_clone!(/, lhs, rhs),
            NodeKind::Select { selected, arg, arms } => {
                Node::update_select(ctx, selected, arg, arms, event)
            }
            NodeKind::Use | NodeKind::Lambda(_) | NodeKind::TypeDef => None,
        };
        if ctx.dbg_ctx.trace {
            if let Some(v) = &res {
                ctx.dbg_ctx.add_event(eid, Some(event.clone()), v.clone())
            }
        }
        res
    }
}
