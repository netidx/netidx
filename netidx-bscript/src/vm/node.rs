use crate::{
    expr::{Expr, ExprId, ExprKind, FnType, ModPath, Pattern, Type},
    vm::{
        env::Bind, Apply, ApplyTyped, BindId, Ctx, Event, ExecCtx, InitFnTyped,
        LambdaTVars, TypeId,
    },
};
use anyhow::{anyhow, bail, Result};
use arcstr::{literal, ArcStr};
use compact_str::{format_compact, CompactString};
use enumflags2::BitFlags;
use immutable_chunkmap::set::SetS as Set;
use netidx::{publisher::Typ, subscriber::Value, utils::Either};
use smallvec::{smallvec, SmallVec};
use std::{
    fmt::{self, Debug},
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
    ) -> Result<FnType> {
        let spec = &self.spec;
        for (arg, (_, typ)) in args.iter_mut().zip(spec.argspec.iter()) {
            arg.typecheck(ctx)?;
            ctx.env.check_typevar_contains(*typ, arg.typ)?;
        }
        self.body.typecheck(ctx)?;
        ctx.env.check_typevar_contains(spec.rtype, self.body.typ)?;
        Ok(FnType {
            args: Arc::from_iter(
                spec.argspec
                    .iter()
                    .map(|(_, typ)| Ok(ctx.env.get_typevar(typ)?.clone()))
                    .collect::<Result<SmallVec<[_; 16]>>>()?,
            ),
            vargs: Type::Bottom,
            rtype: ctx.env.get_typevar(&self.body.typ)?.clone(),
        })
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
        let scope = ModPath(scope.0.append(&format_compact!("{id:?}")));
        for ((name, typ), node) in spec.argspec.iter().zip(args.iter()) {
            let bind = ctx.env.bind_variable(&scope, &**name, *typ);
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
    typ: FnType,
    scope: ModPath,
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
    ) -> Result<FnType> {
        let spec = &self.spec;
        if spec.argspec.len() != self.typ.args.len()
            || spec.vargs.is_none() && self.typ.vargs != Type::Bottom
        {
            bail!("arity mismatch in builtin specification")
        }
        if args.len() < self.typ.args.len()
            || (args.len() > self.typ.args.len() && self.typ.vargs == Type::Bottom)
        {
            let vargs = if self.typ.vargs != Type::Bottom { "at least " } else { "" };
            bail!("expected {}{} arguments got {}", spec.argspec.len(), vargs, args.len())
        }
        for ((_, id), typ) in spec.argspec.iter().zip(self.typ.args.iter()) {
            ctx.env.define_typevar(*id, typ.clone())?
        }
        if let Some(id) = spec.vargs {
            ctx.env.define_typevar(id, self.typ.vargs.clone())?;
        }
        ctx.env.define_typevar(spec.rtype, self.typ.rtype.clone())?;
        for i in 0..args.len() {
            args[i].typecheck(ctx)?;
            let atyp = if i < spec.argspec.len() {
                spec.argspec[i].1
            } else {
                spec.vargs.unwrap()
            };
            ctx.env.check_typevar_contains(atyp, args[i].typ)?
        }
        Ok(self.typ.clone())
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

    pub fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> bool {
        match self.node.update(ctx, event) {
            None => false,
            Some(v) => {
                self.cached = Some(v);
                true
            }
        }
    }
}

pub enum PatternNode<C: Ctx + 'static, E: Debug + Clone + 'static> {
    Underscore,
    Typ { tag: BitFlags<Typ>, bind: BindId, guard: Option<Cached<C, E>> },
    Error(ArcStr),
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> PatternNode<C, E> {
    fn extract_err(&self) -> Option<&ArcStr> {
        match self {
            PatternNode::Underscore
            | PatternNode::Typ { tag: _, bind: _, guard: None } => None,
            PatternNode::Typ { tag: _, bind: _, guard: Some(c) } => c.node.extract_err(),
            PatternNode::Error(e) => Some(e),
        }
    }

    fn extract_guard(self) -> Option<Node<C, E>> {
        match self {
            PatternNode::Typ { tag: _, bind: _, guard: Some(g) } => Some(g.node),
            PatternNode::Underscore
            | PatternNode::Error(_)
            | PatternNode::Typ { tag: _, bind: _, guard: None } => None,
        }
    }

    fn match_type(&self) -> Type {
        match self {
            PatternNode::Typ { tag, bind: _, guard: _ } => Type::Primitive(*tag),
            PatternNode::Underscore => Type::any(),
            PatternNode::Error(_) => Type::Bottom,
        }
    }

    fn bind_type(&self) -> Type {
        match self {
            PatternNode::Typ { tag, bind: _, guard: None } => Type::Primitive(*tag),
            PatternNode::Underscore => Type::any(),
            PatternNode::Typ { tag: _, bind: _, guard: Some(_) }
            | PatternNode::Error(_) => Type::Bottom,
        }
    }

    fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: &Pattern,
        scope: &ModPath,
        top_id: ExprId,
    ) -> Self {
        match spec {
            Pattern::Underscore => PatternNode::Underscore,
            Pattern::Typ { tag, bind, guard } => {
                let tag = *tag;
                let bind = ctx.env.bind_variable(scope, &**bind, TypeId::new());
                let id = bind.id;
                let guard = guard.as_ref().map(|g| {
                    Cached::new(Node::compile_int(ctx, g.clone(), &scope, top_id))
                });
                PatternNode::Typ { tag, bind: id, guard }
            }
        }
    }

    fn bind(&self) -> Option<BindId> {
        match self {
            PatternNode::Underscore | PatternNode::Error(_) => None,
            PatternNode::Typ { tag: _, bind, guard: _ } => Some(*bind),
        }
    }

    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> bool {
        match self {
            PatternNode::Underscore
            | PatternNode::Typ { tag: _, bind: _, guard: None }
            | PatternNode::Error(_) => false,
            PatternNode::Typ { tag: _, bind: _, guard: Some(g) } => g.update(ctx, event),
        }
    }

    fn is_match(&self, arg: Option<&Value>) -> (bool, Option<BindId>) {
        match (arg, self) {
            (_, PatternNode::Underscore) => (true, None),
            (Some(arg), PatternNode::Typ { tag, bind, guard: None }) => {
                let typ = BitFlags::from_iter([Typ::get(arg)]);
                (tag.len() == 0 || tag.contains(typ), Some(*bind))
            }
            (Some(arg), PatternNode::Typ { tag, bind, guard: Some(g) }) => {
                let typ = BitFlags::from_iter([Typ::get(arg)]);
                let is_match = (tag.len() == 0 || tag.contains(typ))
                    && g.cached
                        .as_ref()
                        .and_then(|v| v.clone().get_as::<bool>())
                        .unwrap_or(false);
                (is_match, Some(*bind))
            }
            (Some(_), PatternNode::Error(_)) | (None, _) => (false, None),
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
    Lambda(InitFnTyped<C, E>),
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
    pub typ: TypeId,
    pub kind: NodeKind<C, E>,
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> fmt::Display for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.spec)
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Node<C, E> {
    fn find_lambda(&self) -> Option<InitFnTyped<C, E>> {
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
            NodeKind::Lambda(b) => Some(SArc::clone(b)),
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
    pub fn extract_err(&self) -> Option<&ArcStr> {
        match &self.kind {
            NodeKind::Error { error: Some(e), .. } => Some(e),
            NodeKind::Error { children, .. } => {
                for node in children {
                    if let Some(e) = node.extract_err() {
                        return Some(e);
                    }
                }
                None
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
        argspec: Arc<[(ArcStr, Option<Type>)]>,
        vargs: Option<Option<Type>>,
        rtype: Option<Type>,
        scope: &ModPath,
        body: Either<Arc<Expr>, ArcStr>,
        eid: ExprId,
    ) -> Node<C, E> {
        if argspec.len() != argspec.iter().collect::<Set<_>>().len() {
            let e = literal!("arguments must have unique names");
            let kind = NodeKind::Error { error: Some(e), children: Box::from_iter([]) };
            return Node { spec: Box::new(spec), typ: ctx.env.bottom(), kind };
        }
        let scope = scope.clone();
        let env = ctx.env.clone();
        let argspec = argspec
            .iter()
            .map(|(name, typ)| match typ {
                None => Ok((name.clone(), None)),
                Some(typ) => {
                    let typ = ctx.env.resolve_typrefs(&scope, typ)?;
                    Ok((name.clone(), Some(typ.into_owned())))
                }
            })
            .collect::<Result<SmallVec<[_; 16]>>>();
        let argspec = match argspec {
            Ok(a) => a,
            Err(e) => {
                let error = Some(format_compact!("{e}").as_str().into());
                let kind = NodeKind::Error { error, children: Box::from_iter([]) };
                return Node { spec: Box::new(spec), typ: ctx.env.bottom(), kind };
            }
        };
        let f: InitFnTyped<C, E> = SArc::new(move |ctx, args, tid| {
            let argspec = Arc::from_iter(argspec.iter().map(|(name, typ)| match typ {
                None => (name.clone(), TypeId::new()),
                Some(typ) => (name.clone(), ctx.env.add_typ(typ.clone())),
            }));
            let vargs = match vargs.as_ref() {
                Some(Some(typ)) => Some(ctx.env.add_typ(typ.clone())),
                Some(None) => Some(TypeId::new()),
                None => None,
            };
            let rtype = rtype
                .as_ref()
                .map(|t| ctx.env.add_typ(t.clone()))
                .unwrap_or_else(|| TypeId::new());
            let spec = LambdaTVars { argspec, vargs, rtype };
            match body.clone() {
                Either::Right(builtin) => match ctx.builtins.get(&*builtin) {
                    None => bail!("unknown builtin function {builtin}"),
                    Some((typ, init)) => {
                        let init = SArc::clone(init);
                        let old_env = mem::replace(&mut ctx.env, env.clone());
                        let typ = ctx.env.resolve_fn_typerefs(&scope, &typ);
                        ctx.env = old_env;
                        Ok(Box::new(BuiltIn {
                            typ: typ?,
                            scope: scope.clone(),
                            spec,
                            apply: init(ctx, args, tid)?,
                        }))
                    }
                },
                Either::Left(body) => {
                    // compile with the original env not the call site env
                    let new_env = mem::replace(&mut ctx.env, env.clone());
                    let apply =
                        Lambda::new(ctx, spec, args, &scope, eid, tid, (*body).clone());
                    ctx.env = ctx.env.merge_lambda_env(&new_env);
                    Ok(Box::new(apply?))
                }
            }
        });
        Node { spec: Box::new(spec), typ: TypeId::new(), kind: NodeKind::Lambda(f) }
    }

    pub fn compile_int(
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
                Node { spec: Box::new(spec), kind, typ: ctx.env.bottom() }
            }};
            ($fmt:expr, $children:expr, $($arg:expr),*) => {{
                let e = ArcStr::from(format_compact!($fmt, $($arg),*).as_str());
                let kind = NodeKind::Error { error: Some(e), children: Box::from_iter($children) };
                Node { spec: Box::new(spec), kind, typ: ctx.env.bottom() }
            }};
            ($fmt:expr) => { error!($fmt, [],) };
            ($fmt:expr, $children:expr) => { error!($fmt, $children,) };
        }
        macro_rules! binary_op {
            ($op:ident, $lhs:expr, $rhs:expr, $rt:expr) => {{
                let lhs = Node::compile_int(ctx, (**$lhs).clone(), scope, top_id);
                let rhs = Node::compile_int(ctx, (**$rhs).clone(), scope, top_id);
                if lhs.is_err() || rhs.is_err() {
                    return error!("", [lhs, rhs]);
                }
                let lhs = Box::new(Cached::new(lhs));
                let rhs = Box::new(Cached::new(rhs));
                Node { spec: Box::new(spec), typ: $rt, kind: NodeKind::$op { lhs, rhs } }
            }};
        }
        match &spec {
            Expr { kind: ExprKind::Constant(v), id: _ } => {
                let typ = ctx.env.add_typ(Type::Primitive(Typ::get(&v).into()));
                Node { kind: NodeKind::Constant(v.clone()), spec: Box::new(spec), typ }
            }
            Expr { kind: ExprKind::Do { exprs }, id } => {
                let scope = ModPath(scope.append(&format_compact!("do{}", id.inner())));
                let (error, exp) = subexprs!(scope, exprs);
                if error {
                    error!("", exp)
                } else {
                    let typ =
                        exp.last().map(|n| n.typ).unwrap_or_else(|| ctx.env.bottom());
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
                            let typ = ctx.env.bottom();
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
                    Node { spec: Box::new(spec), typ: ctx.env.bottom(), kind }
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
                            Node { spec: Box::new(spec), typ: ctx.env.bottom(), kind }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Lambda { args, vargs, rtype, body }, id } => {
                let (args, vargs, rtype, body, id) =
                    (args.clone(), vargs.clone(), rtype.clone(), (*body).clone(), *id);
                Node::compile_lambda(ctx, spec, args, vargs, rtype, scope, body, id)
            }
            Expr { kind: ExprKind::Apply { args, function: f }, id: _ } => {
                let (error, args) = subexprs!(scope, args);
                match ctx.env.lookup_bind(scope, f) {
                    None => error!("{f} is undefined"),
                    Some((_, Bind { fun: None, .. })) => {
                        error!("{f} is not a function")
                    }
                    Some((_, Bind { fun: Some(i), typ, id, .. })) => {
                        let varid = *id;
                        let typ = *typ;
                        let i = SArc::clone(i);
                        if error {
                            return error!("", args);
                        }
                        match i(ctx, &args, top_id) {
                            Err(e) => error!("error in function {f} {e:?}"),
                            Ok(function) => {
                                ctx.user.ref_var(varid, top_id);
                                let kind =
                                    NodeKind::Apply { args: Box::from(args), function };
                                Node { spec: Box::new(spec), typ, kind }
                            }
                        }
                    }
                }
            }
            Expr { kind: ExprKind::Bind { export: _, name, typ, value }, id: _ } => {
                let node = Node::compile_int(ctx, (**value).clone(), &scope, top_id);
                let typ = match typ {
                    None => node.typ,
                    Some(typ) => match ctx.env.resolve_typrefs(scope, typ) {
                        Ok(t) => ctx.env.add_typ(t.into_owned()),
                        Err(e) => return error!("{e}", vec![node]),
                    },
                };
                let bind = ctx.env.bind_variable(scope, &**name, typ);
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
                        let typ = bind.typ;
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
                        let e = e || pat.extract_err().is_some() || n.is_err();
                        nodes.push((pat, Cached::new(n)));
                        (e, nodes)
                    });
                let mut err = CompactString::new("");
                if error {
                    let mut v = vec![];
                    for (pat, n) in arms {
                        if let Some(e) = pat.extract_err() {
                            err.push_str(e.as_str());
                            err.push_str(", ");
                        }
                        if let Some(g) = pat.extract_guard() {
                            v.push(g);
                        }
                        v.push(n.node)
                    }
                    return error!("{err}", v);
                }
                let arms = Box::from(arms);
                let kind = NodeKind::Select { selected: None, arg, arms };
                Node { spec: Box::new(spec), typ: TypeId::new(), kind }
            }
            Expr { kind: ExprKind::TypeCast { expr, typ }, id: _ } => {
                let n = Node::compile_int(ctx, (**expr).clone(), scope, top_id);
                if n.is_err() {
                    return error!("", vec![n]);
                }
                let tid = ctx.env.add_typ(Type::Primitive(*typ | Typ::Result));
                let kind = NodeKind::TypeCast { target: *typ, n: Box::new(n) };
                Node { spec: Box::new(spec), typ: tid, kind }
            }
            Expr { kind: ExprKind::TypeDef { name, typ }, id: _ } => {
                match ctx.env.deftype(scope, name, typ.clone()) {
                    Err(e) => error!("{e}"),
                    Ok(()) => {
                        let spec = Box::new(spec);
                        Node { spec, typ: ctx.env.bottom(), kind: NodeKind::TypeDef }
                    }
                }
            }
            Expr { kind: ExprKind::Not { expr }, id: _ } => {
                let node = Node::compile_int(ctx, (**expr).clone(), scope, top_id);
                if node.is_err() {
                    return error!("", vec![node]);
                }
                let node = Box::new(node);
                let spec = Box::new(spec);
                Node { spec, typ: ctx.env.boolean(), kind: NodeKind::Not { node } }
            }
            Expr { kind: ExprKind::Eq { lhs, rhs }, id: _ } => {
                binary_op!(Eq, lhs, rhs, ctx.env.boolean())
            }
            Expr { kind: ExprKind::Ne { lhs, rhs }, id: _ } => {
                binary_op!(Ne, lhs, rhs, ctx.env.boolean())
            }
            Expr { kind: ExprKind::Lt { lhs, rhs }, id: _ } => {
                binary_op!(Lt, lhs, rhs, ctx.env.boolean())
            }
            Expr { kind: ExprKind::Gt { lhs, rhs }, id: _ } => {
                binary_op!(Gt, lhs, rhs, ctx.env.boolean())
            }
            Expr { kind: ExprKind::Lte { lhs, rhs }, id: _ } => {
                binary_op!(Lte, lhs, rhs, ctx.env.boolean())
            }
            Expr { kind: ExprKind::Gte { lhs, rhs }, id: _ } => {
                binary_op!(Gte, lhs, rhs, ctx.env.boolean())
            }
            Expr { kind: ExprKind::And { lhs, rhs }, id: _ } => {
                binary_op!(And, lhs, rhs, ctx.env.boolean())
            }
            Expr { kind: ExprKind::Or { lhs, rhs }, id: _ } => {
                binary_op!(Or, lhs, rhs, ctx.env.boolean())
            }
            Expr { kind: ExprKind::Add { lhs, rhs }, id: _ } => {
                binary_op!(Add, lhs, rhs, ctx.env.number())
            }
            Expr { kind: ExprKind::Sub { lhs, rhs }, id: _ } => {
                binary_op!(Sub, lhs, rhs, ctx.env.number())
            }
            Expr { kind: ExprKind::Mul { lhs, rhs }, id: _ } => {
                binary_op!(Mul, lhs, rhs, ctx.env.number())
            }
            Expr { kind: ExprKind::Div { lhs, rhs }, id: _ } => {
                binary_op!(Div, lhs, rhs, ctx.env.number())
            }
        }
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        match &mut self.kind {
            NodeKind::Add { lhs, rhs }
            | NodeKind::Sub { lhs, rhs }
            | NodeKind::Mul { lhs, rhs }
            | NodeKind::Div { lhs, rhs } => {
                lhs.node.typecheck(ctx)?;
                rhs.node.typecheck(ctx)?;
                let id = ctx.env.number();
                ctx.env.check_typevar_contains(id, lhs.node.typ)?;
                ctx.env.check_typevar_contains(id, rhs.node.typ)?;
                Ok(())
            }
            NodeKind::And { lhs, rhs } | NodeKind::Or { lhs, rhs } => {
                lhs.node.typecheck(ctx)?;
                rhs.node.typecheck(ctx)?;
                let id = ctx.env.boolean();
                ctx.env.check_typevar_contains(id, lhs.node.typ)?;
                ctx.env.check_typevar_contains(id, rhs.node.typ)?;
                Ok(())
            }
            NodeKind::Not { node } => {
                node.typecheck(ctx)?;
                let id = ctx.env.boolean();
                ctx.env.check_typevar_contains(id, node.typ)?;
                Ok(())
            }
            NodeKind::Eq { lhs, rhs }
            | NodeKind::Ne { lhs, rhs }
            | NodeKind::Lt { lhs, rhs }
            | NodeKind::Gt { lhs, rhs }
            | NodeKind::Lte { lhs, rhs }
            | NodeKind::Gte { lhs, rhs } => {
                lhs.node.typecheck(ctx)?;
                rhs.node.typecheck(ctx)?;
                Ok(())
            }
            NodeKind::TypeCast { target: _, n } => Ok(n.typecheck(ctx)?),
            NodeKind::Do(nodes) => {
                for n in nodes {
                    n.typecheck(ctx)?;
                }
                Ok(())
            }
            NodeKind::Bind(_, node) => {
                node.typecheck(ctx)?;
                ctx.env.check_typevar_contains(self.typ, node.typ)?;
                Ok(())
            }
            NodeKind::Connect(_, node) => Ok(node.typecheck(ctx)?),
            NodeKind::Apply { args, function } => {
                for n in args.iter_mut() {
                    n.typecheck(ctx)?
                }
                let ftyp = function.typecheck(ctx, args)?;
                ctx.env.define_typevar(self.typ, Type::Fn(Arc::new(ftyp)))?;
                Ok(())
            }
            NodeKind::Select { selected: _, arg, arms } => {
                arg.node.typecheck(ctx)?;
                let mut rtype = Type::Bottom;
                let mut mtype = Type::Bottom;
                let mut mcases = Type::Bottom;
                for (pat, n) in arms {
                    match pat {
                        PatternNode::Error(_) => bail!("pattern is error"),
                        PatternNode::Underscore => mtype = mtype.union(&Type::any()),
                        PatternNode::Typ { tag, bind, guard, .. } => {
                            let bind = ctx
                                .env
                                .lookup_bind_by_id(bind)
                                .ok_or_else(|| anyhow!("missing bind"))?;
                            let ptyp = Type::Primitive(*tag);
                            if tag.len() > 0 {
                                ctx.env.define_typevar(bind.typ, ptyp.clone())?;
                            } else {
                                ctx.env.alias_typevar(bind.typ, arg.node.typ)?;
                            }
                            if tag.len() > 0 {
                                mcases = mcases.union(&ptyp);
                            }
                            match guard {
                                Some(guard) => guard.node.typecheck(ctx)?,
                                None if tag.len() == 0 => {
                                    mtype = mtype.union(&Type::any())
                                }
                                None => mtype = mtype.union(&ptyp),
                            }
                        }
                    }
                    n.node.typecheck(ctx)?;
                    let t = ctx.env.get_typevar(&n.node.typ)?;
                    rtype = rtype.union(t);
                }
                let mtypeid = ctx.env.add_typ(mtype.clone());
                let mcasesid = ctx.env.add_typ(mcases.clone());
                ctx.env
                    .check_typevar_contains(arg.node.typ, mcasesid)
                    .map_err(|e| anyhow!("pattern will never match {e}"))?;
                ctx.env
                    .check_typevar_contains(mtypeid, arg.node.typ)
                    .map_err(|e| anyhow!("missing match cases {e}"))?;
                ctx.env.define_typevar(self.typ, rtype)
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
        let node = Self::compile_int(ctx, spec, scope, top_id);
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
        let mut val_up: SmallVec<[bool; 16]> = smallvec![];
        let arg_up = arg.update(ctx, event);
        macro_rules! set_arg {
            ($i:expr) => {
                if let Some(id) = arms[$i].0.bind() {
                    if let Some(arg) = arg.cached.as_ref() {
                        val_up[$i] |=
                            arms[$i].1.update(ctx, &Event::Variable(id, arg.clone()));
                    }
                }
            };
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
            if arg_up {
                if let Some(id) = pat.bind() {
                    if let Some(arg) = arg.cached.as_ref() {
                        pat_up |= pat.update(ctx, &Event::Variable(id, arg.clone()));
                    }
                }
            }
            val_up.push(val.update(ctx, event));
        }
        if !pat_up {
            selected.and_then(|i| val!(i))
        } else {
            let sel = arms.iter().enumerate().find_map(|(i, (pat, _))| {
                match pat.is_match(arg.cached.as_ref()) {
                    (true, id) => Some((i, id)),
                    _ => None,
                }
            });
            match (sel, *selected) {
                (Some((i, _)), Some(j)) if i == j => val!(i),
                (Some((i, _)), Some(_) | None) => {
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
