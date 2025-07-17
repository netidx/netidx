use crate::{
    env, err,
    expr::{self, Expr, ExprId, ExprKind, ModPath},
    typ::{self, TVal, TVar, Type},
    wrap, BindId, Ctx, Event, ExecCtx, Node, Update, UserEvent,
};
use anyhow::{anyhow, bail, Context, Result};
use arcstr::{literal, ArcStr};
use combine::stream::position::SourcePosition;
use compiler::compile;
use enumflags2::BitFlags;
use netidx_value::{Typ, Value};
use pattern::StructPatternNode;
use std::{cell::RefCell, sync::LazyLock};
use triomphe::Arc;

pub(crate) mod array;
pub(crate) mod callsite;
pub(crate) mod compiler;
pub(crate) mod data;
pub mod genn;
pub(crate) mod lambda;
pub(crate) mod op;
pub(crate) mod pattern;
pub(crate) mod select;

#[macro_export]
macro_rules! wrap {
    ($n:expr, $e:expr) => {
        match $e {
            Ok(x) => Ok(x),
            e => {
                anyhow::Context::context(e, $crate::expr::ErrorContext($n.spec().clone()))
            }
        }
    };
}

#[macro_export]
macro_rules! update_args {
    ($args:expr, $ctx:expr, $event:expr) => {{
        let mut updated = false;
        let mut determined = true;
        for n in $args.iter_mut() {
            updated |= n.update($ctx, $event);
            determined &= n.cached.is_some();
        }
        (updated, determined)
    }};
}

static NOP: LazyLock<Arc<Expr>> = LazyLock::new(|| {
    Arc::new(
        ExprKind::Constant(Value::String(literal!("nop"))).to_expr(Default::default()),
    )
});

#[derive(Debug)]
pub(crate) struct Nop {
    pub typ: Type,
}

impl Nop {
    pub(crate) fn new<C: Ctx, E: UserEvent>(typ: Type) -> Node<C, E> {
        Box::new(Nop { typ })
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Nop {
    fn update(
        &mut self,
        _ctx: &mut ExecCtx<C, E>,
        _event: &mut Event<E>,
    ) -> Option<Value> {
        None
    }

    fn delete(&mut self, _ctx: &mut ExecCtx<C, E>) {}

    fn typecheck(&mut self, _ctx: &mut ExecCtx<C, E>) -> Result<()> {
        Ok(())
    }

    fn spec(&self) -> &Expr {
        &NOP
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn refs<'a>(&'a self, _f: &'a mut (dyn FnMut(BindId) + 'a)) {}
}

#[derive(Debug)]
struct Cached<C: Ctx, E: UserEvent> {
    cached: Option<Value>,
    node: Node<C, E>,
}

impl<C: Ctx, E: UserEvent> Cached<C, E> {
    fn new(node: Node<C, E>) -> Self {
        Self { cached: None, node }
    }

    /// update the node, return whether the node updated. If it did,
    /// the updated value will be stored in the cached field, if not,
    /// the previous value will remain there.
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> bool {
        match self.node.update(ctx, event) {
            None => false,
            Some(v) => {
                self.cached = Some(v);
                true
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct Use {
    spec: Expr,
    scope: ModPath,
    name: ModPath,
}

impl Use {
    pub(crate) fn compile<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        name: &ModPath,
        pos: &SourcePosition,
    ) -> Result<Node<C, E>> {
        match ctx.env.canonical_modpath(scope, name) {
            None => bail!("at {pos} no such module {name}"),
            Some(_) => {
                let used = ctx.env.used.get_or_default_cow(scope.clone());
                Arc::make_mut(used).push(name.clone());
                Ok(Box::new(Self { spec, scope: scope.clone(), name: name.clone() }))
            }
        }
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Use {
    fn update(
        &mut self,
        _ctx: &mut ExecCtx<C, E>,
        _event: &mut Event<E>,
    ) -> Option<Value> {
        None
    }

    fn typecheck(&mut self, _ctx: &mut ExecCtx<C, E>) -> Result<()> {
        Ok(())
    }

    fn refs<'a>(&'a self, _f: &'a mut (dyn FnMut(BindId) + 'a)) {}

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        if let Some(used) = ctx.env.used.get_mut_cow(&self.scope) {
            Arc::make_mut(used).retain(|n| n != &self.name);
            if used.is_empty() {
                ctx.env.used.remove_cow(&self.scope);
            }
        }
    }

    fn typ(&self) -> &Type {
        &Type::Bottom
    }
}

#[derive(Debug)]
pub(crate) struct TypeDef {
    spec: Expr,
    scope: ModPath,
    name: ArcStr,
}

impl TypeDef {
    pub(crate) fn compile<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        name: &ArcStr,
        params: &Arc<[(TVar, Option<Type>)]>,
        typ: &Type,
        pos: &SourcePosition,
    ) -> Result<Node<C, E>> {
        let typ = typ.scope_refs(scope);
        ctx.env
            .deftype(scope, name, params.clone(), typ)
            .with_context(|| format!("in typedef at {pos}"))?;
        let name = name.clone();
        let scope = scope.clone();
        Ok(Box::new(Self { spec, scope, name }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for TypeDef {
    fn update(
        &mut self,
        _ctx: &mut ExecCtx<C, E>,
        _event: &mut Event<E>,
    ) -> Option<Value> {
        None
    }

    fn typecheck(&mut self, _ctx: &mut ExecCtx<C, E>) -> Result<()> {
        Ok(())
    }

    fn refs<'a>(&'a self, _f: &'a mut (dyn FnMut(BindId) + 'a)) {}

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.env.undeftype(&self.scope, &self.name)
    }

    fn typ(&self) -> &Type {
        &Type::Bottom
    }
}

#[derive(Debug)]
pub(crate) struct Constant {
    spec: Arc<Expr>,
    value: Value,
    typ: Type,
}

impl Constant {
    pub(crate) fn compile<C: Ctx, E: UserEvent>(
        spec: Expr,
        value: &Value,
    ) -> Result<Node<C, E>> {
        let spec = Arc::new(spec);
        let value = value.clone();
        let typ = Type::Primitive(Typ::get(&value).into());
        Ok(Box::new(Self { spec, value, typ }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Constant {
    fn update(
        &mut self,
        _ctx: &mut ExecCtx<C, E>,
        event: &mut Event<E>,
    ) -> Option<Value> {
        if event.init {
            Some(self.value.clone())
        } else {
            None
        }
    }

    fn delete(&mut self, _ctx: &mut ExecCtx<C, E>) {}

    fn refs<'a>(&'a self, _f: &'a mut (dyn FnMut(BindId) + 'a)) {}

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn typecheck(&mut self, _ctx: &mut ExecCtx<C, E>) -> Result<()> {
        Ok(())
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }
}

// used for both mod and do
#[derive(Debug)]
pub(crate) struct Block<C: Ctx, E: UserEvent> {
    spec: Expr,
    children: Box<[Node<C, E>]>,
}

impl<C: Ctx, E: UserEvent> Block<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        exprs: &Arc<[Expr]>,
    ) -> Result<Node<C, E>> {
        let children = exprs
            .iter()
            .map(|e| compile(ctx, e.clone(), scope, top_id))
            .collect::<Result<Box<[Node<C, E>]>>>()?;
        Ok(Box::new(Self { spec, children }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Block<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        self.children.iter_mut().fold(None, |_, n| n.update(ctx, event))
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        for n in &mut self.children {
            n.delete(ctx)
        }
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        for n in &self.children {
            n.refs(f)
        }
    }

    fn typ(&self) -> &Type {
        &self.children.last().map(|n| n.typ()).unwrap_or(&Type::Bottom)
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        for n in &mut self.children {
            wrap!(n, n.typecheck(ctx))?
        }
        Ok(())
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }
}

#[derive(Debug)]
pub(crate) struct Bind<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    pattern: StructPatternNode,
    node: Node<C, E>,
}

impl<C: Ctx, E: UserEvent> Bind<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        b: &expr::Bind,
        pos: &SourcePosition,
    ) -> Result<Node<C, E>> {
        let expr::Bind { doc, pattern, typ, export: _, value } = b;
        let node = compile(ctx, value.clone(), &scope, top_id)?;
        let typ = match typ {
            Some(typ) => typ.scope_refs(scope),
            None => {
                let typ = node.typ().clone();
                let ptyp = pattern.infer_type_predicate(&ctx.env)?;
                if !ptyp.contains(&ctx.env, &typ)? {
                    typ::format_with_flags(typ::PrintFlag::DerefTVars.into(), || {
                        bail!("at {pos} match error {typ} can't be matched by {ptyp}")
                    })?
                }
                typ
            }
        };
        let pattern = StructPatternNode::compile(ctx, &typ, pattern, scope)
            .with_context(|| format!("at {pos}"))?;
        if pattern.is_refutable() {
            bail!("at {pos} refutable patterns are not allowed in let");
        }
        if let Some(doc) = doc {
            pattern.ids(&mut |id| {
                if let Some(b) = ctx.env.by_id.get_mut_cow(&id) {
                    b.doc = Some(doc.clone());
                }
            });
        }
        Ok(Box::new(Self { spec, typ, pattern, node }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Bind<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        if let Some(v) = self.node.update(ctx, event) {
            self.pattern.bind(&v, &mut |id, v| ctx.set_var(id, v))
        }
        None
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.pattern.ids(f);
        self.node.refs(f);
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.node.delete(ctx);
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.node, self.node.typecheck(ctx))?;
        wrap!(self.node, self.typ.check_contains(&ctx.env, &self.node.typ()))?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Ref {
    spec: Arc<Expr>,
    typ: Type,
    id: BindId,
    top_id: ExprId,
}

impl Ref {
    pub(crate) fn compile<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        name: &ModPath,
        pos: &SourcePosition,
    ) -> Result<Node<C, E>> {
        match ctx.env.lookup_bind(scope, name) {
            None => bail!("at {pos} {name} not defined"),
            Some((_, bind)) => {
                ctx.user.ref_var(bind.id, top_id);
                let typ = bind.typ.clone();
                let spec = Arc::new(spec);
                Ok(Box::new(Self { spec, typ, id: bind.id, top_id }))
            }
        }
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Ref {
    fn update(
        &mut self,
        _ctx: &mut ExecCtx<C, E>,
        event: &mut Event<E>,
    ) -> Option<Value> {
        event.variables.get(&self.id).map(|v| v.clone())
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        f(self.id)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.id, self.top_id)
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn typecheck(&mut self, _ctx: &mut ExecCtx<C, E>) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct StringInterpolate<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    args: Box<[Cached<C, E>]>,
}

impl<C: Ctx, E: UserEvent> StringInterpolate<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        args: &[Expr],
    ) -> Result<Node<C, E>> {
        let args = args
            .iter()
            .map(|e| Ok(Cached::new(compile(ctx, e.clone(), scope, top_id)?)))
            .collect::<Result<_>>()?;
        let typ = Type::Primitive(Typ::String.into());
        Ok(Box::new(Self { spec, typ, args }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for StringInterpolate<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        use std::fmt::Write;
        thread_local! {
            static BUF: RefCell<String> = RefCell::new(String::new());
        }
        let (updated, determined) = update_args!(self.args, ctx, event);
        if updated && determined {
            BUF.with_borrow_mut(|buf| {
                buf.clear();
                for c in &self.args {
                    match c.cached.as_ref().unwrap() {
                        Value::String(s) => write!(buf, "{s}"),
                        v => write!(
                            buf,
                            "{}",
                            TVal { env: &ctx.env, typ: c.node.typ(), v }
                        ),
                    }
                    .unwrap()
                }
                Some(Value::String(buf.as_str().into()))
            })
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

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        for a in &self.args {
            a.node.refs(f)
        }
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        for n in &mut self.args {
            n.node.delete(ctx)
        }
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        for a in &mut self.args {
            wrap!(a.node, a.node.typecheck(ctx))?
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Connect<C: Ctx, E: UserEvent> {
    spec: Expr,
    node: Node<C, E>,
    id: BindId,
}

impl<C: Ctx, E: UserEvent> Connect<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        name: &ModPath,
        value: &Expr,
        pos: &SourcePosition,
    ) -> Result<Node<C, E>> {
        let id = match ctx.env.lookup_bind(scope, name) {
            None => bail!("at {pos} {name} is undefined"),
            Some((_, env::Bind { id, .. })) => *id,
        };
        let node = compile(ctx, value.clone(), scope, top_id)?;
        Ok(Box::new(Self { spec, node, id }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Connect<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        if let Some(v) = self.node.update(ctx, event) {
            ctx.set_var(self.id, v)
        }
        None
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &Type::Bottom
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.node.refs(f)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.node.delete(ctx)
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.node, self.node.typecheck(ctx))?;
        let bind = match ctx.env.by_id.get(&self.id) {
            None => bail!("BUG missing bind {:?}", self.id),
            Some(bind) => bind,
        };
        wrap!(self, bind.typ.check_contains(&ctx.env, self.node.typ()))
    }
}

#[derive(Debug)]
pub(crate) struct ConnectDeref<C: Ctx, E: UserEvent> {
    spec: Expr,
    rhs: Cached<C, E>,
    src_id: BindId,
    target_id: Option<BindId>,
    top_id: ExprId,
}

impl<C: Ctx, E: UserEvent> ConnectDeref<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        name: &ModPath,
        value: &Expr,
        pos: &SourcePosition,
    ) -> Result<Node<C, E>> {
        let src_id = match ctx.env.lookup_bind(scope, name) {
            None => bail!("at {pos} {name} is undefined"),
            Some((_, env::Bind { id, .. })) => *id,
        };
        ctx.user.ref_var(src_id, top_id);
        let rhs = Cached::new(compile(ctx, value.clone(), scope, top_id)?);
        Ok(Box::new(Self { spec, rhs, src_id, target_id: None, top_id }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for ConnectDeref<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        let mut up = self.rhs.update(ctx, event);
        if let Some(Value::U64(id)) = event.variables.get(&self.src_id) {
            if let Some(target_id) = ctx.env.byref_chain.get(&BindId::from(*id)) {
                self.target_id = Some(*target_id);
                up = true;
            }
        }
        if up {
            if let Some(v) = &self.rhs.cached {
                if let Some(id) = self.target_id {
                    ctx.set_var(id, v.clone())
                }
            }
        }
        None
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &Type::Bottom
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        f(self.src_id);
        self.rhs.node.refs(f)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.src_id, self.top_id);
        self.rhs.node.delete(ctx)
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.rhs.node, self.rhs.node.typecheck(ctx))?;
        let bind = match ctx.env.by_id.get(&self.src_id) {
            None => bail!("BUG missing bind {:?}", self.src_id),
            Some(bind) => bind,
        };
        let typ = Type::ByRef(Arc::new(self.rhs.node.typ().clone()));
        wrap!(self, bind.typ.check_contains(&ctx.env, &typ))
    }
}

#[derive(Debug)]
pub(crate) struct ByRef<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    child: Node<C, E>,
    id: BindId,
}

impl<C: Ctx, E: UserEvent> ByRef<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        expr: &Expr,
    ) -> Result<Node<C, E>> {
        let child = compile(ctx, expr.clone(), scope, top_id)?;
        let id = BindId::new();
        if let Some(c) = (&*child as &dyn std::any::Any).downcast_ref::<Ref>() {
            ctx.env.byref_chain.insert_cow(id, c.id);
        }
        let typ = Type::ByRef(Arc::new(child.typ().clone()));
        Ok(Box::new(Self { spec, typ, child, id }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for ByRef<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        if let Some(v) = self.child.update(ctx, event) {
            ctx.set_var(self.id, v);
        }
        if event.init {
            Some(Value::U64(self.id.inner()))
        } else {
            None
        }
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.env.byref_chain.remove_cow(&self.id);
        self.child.delete(ctx)
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.child.refs(f)
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.child, self.child.typecheck(ctx))?;
        let t = Type::ByRef(Arc::new(self.child.typ().clone()));
        wrap!(self, self.typ.check_contains(&ctx.env, &t))
    }
}

#[derive(Debug)]
pub(crate) struct Deref<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    child: Node<C, E>,
    id: Option<BindId>,
    top_id: ExprId,
}

impl<C: Ctx, E: UserEvent> Deref<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        expr: &Expr,
    ) -> Result<Node<C, E>> {
        let child = compile(ctx, expr.clone(), scope, top_id)?;
        let typ = Type::empty_tvar();
        Ok(Box::new(Self { spec, typ, child, id: None, top_id }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Deref<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        if let Some(v) = self.child.update(ctx, event) {
            match v {
                Value::U64(i) | Value::V64(i) => {
                    let new_id = BindId::from(i);
                    if self.id != Some(new_id) {
                        if let Some(old) = self.id {
                            ctx.user.unref_var(old, self.top_id);
                        }
                        ctx.user.ref_var(new_id, self.top_id);
                        self.id = Some(new_id);
                    }
                }
                _ => return err!("expected u64 bind id"),
            }
        }
        self.id.and_then(|id| event.variables.get(&id).cloned())
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        if let Some(id) = self.id.take() {
            ctx.user.unref_var(id, self.top_id);
        }
        self.child.delete(ctx);
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.child.refs(f);
        if let Some(id) = self.id {
            f(id);
        }
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.child, self.child.typecheck(ctx))?;
        let typ = match self.child.typ() {
            Type::ByRef(t) => (**t).clone(),
            _ => bail!("expected reference"),
        };
        wrap!(self, self.typ.check_contains(&ctx.env, &typ))?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Qop<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    id: BindId,
    n: Node<C, E>,
}

impl<C: Ctx, E: UserEvent> Qop<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        e: &Expr,
        pos: &SourcePosition,
    ) -> Result<Node<C, E>> {
        let n = compile(ctx, e.clone(), scope, top_id)?;
        match ctx.env.lookup_bind(scope, &ModPath::from(["errors"])) {
            None => bail!("at {pos} BUG: errors is undefined"),
            Some((_, bind)) => {
                let typ = Type::empty_tvar();
                Ok(Box::new(Self { spec, typ, id: bind.id, n }))
            }
        }
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Qop<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        match self.n.update(ctx, event) {
            None => None,
            Some(e @ Value::Error(_)) => {
                ctx.set_var(self.id, e);
                None
            }
            Some(v) => Some(v),
        }
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.n.refs(f)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.delete(ctx)
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.n, self.n.typecheck(ctx))?;
        let bind =
            ctx.env.by_id.get(&self.id).ok_or_else(|| anyhow!("BUG: missing bind"))?;
        let err = Type::Primitive(Typ::Error.into());
        wrap!(self, bind.typ.check_contains(&ctx.env, &err))?;
        wrap!(self, err.check_contains(&ctx.env, &bind.typ))?;
        if !self.n.typ().contains(&ctx.env, &err)? {
            bail!("cannot use the ? operator on a non error type")
        }
        let rtyp = self.n.typ().diff(&ctx.env, &err)?;
        wrap!(self, self.typ.check_contains(&ctx.env, &rtyp))?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct TypeCast<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    target: Type,
    n: Node<C, E>,
}

impl<C: Ctx, E: UserEvent> TypeCast<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        expr: &Expr,
        typ: &Type,
        pos: &SourcePosition,
    ) -> Result<Node<C, E>> {
        let n = compile(ctx, expr.clone(), scope, top_id)?;
        let target = typ.scope_refs(scope);
        if let Err(e) = target.check_cast(&ctx.env) {
            bail!("in cast at {pos} {e}");
        }
        let typ = target.union(&ctx.env, &Type::Primitive(Typ::Error.into()))?;
        Ok(Box::new(Self { spec, typ, target, n }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for TypeCast<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        self.n.update(ctx, event).map(|v| self.target.cast_value(&ctx.env, v))
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.delete(ctx)
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.n.refs(f)
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        Ok(wrap!(self.n, self.n.typecheck(ctx))?)
    }
}

#[derive(Debug)]
pub(crate) struct Any<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    n: Box<[Node<C, E>]>,
}

impl<C: Ctx, E: UserEvent> Any<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        args: &[Expr],
    ) -> Result<Node<C, E>> {
        let n = args
            .iter()
            .map(|e| compile(ctx, e.clone(), scope, top_id))
            .collect::<Result<Box<[_]>>>()?;
        let typ =
            Type::Set(Arc::from_iter(n.iter().map(|n| n.typ().clone()))).normalize();
        Ok(Box::new(Self { spec, typ, n }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Any<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        self.n
            .iter_mut()
            .filter_map(|s| s.update(ctx, event))
            .fold(None, |r, v| r.or(Some(v)))
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.iter_mut().for_each(|n| n.delete(ctx))
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.n.iter().for_each(|n| n.refs(f))
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        for n in self.n.iter_mut() {
            wrap!(n, n.typecheck(ctx))?
        }
        let rtyp = Type::Primitive(BitFlags::empty());
        let rtyp = wrap!(
            self,
            self.n.iter().fold(Ok(rtyp), |rtype, n| n.typ().union(&ctx.env, &rtype?))
        )?;
        Ok(self.typ.check_contains(&ctx.env, &rtyp)?)
    }
}

#[derive(Debug)]
struct Sample<C: Ctx, E: UserEvent> {
    spec: Expr,
    triggered: usize,
    typ: Type,
    id: BindId,
    top_id: ExprId,
    trigger: Node<C, E>,
    arg: Cached<C, E>,
}

impl<C: Ctx, E: UserEvent> Sample<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        lhs: &Arc<Expr>,
        rhs: &Arc<Expr>,
    ) -> Result<Node<C, E>> {
        let id = BindId::new();
        ctx.user.ref_var(id, top_id);
        let trigger = compile(ctx, (**lhs).clone(), scope, top_id)?;
        let arg = Cached::new(compile(ctx, (**rhs).clone(), scope, top_id)?);
        let typ = arg.node.typ().clone();
        Ok(Box::new(Self { triggered: 0, id, top_id, spec, typ, trigger, arg }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Sample<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        if let Some(_) = self.trigger.update(ctx, event) {
            self.triggered += 1;
        }
        self.arg.update(ctx, event);
        let var = event.variables.get(&self.id).cloned();
        let res = if self.triggered > 0 && self.arg.cached.is_some() && var.is_none() {
            self.triggered -= 1;
            self.arg.cached.clone()
        } else {
            var
        };
        if self.arg.cached.is_some() {
            while self.triggered > 0 {
                self.triggered -= 1;
                ctx.user.set_var(self.id, self.arg.cached.clone().unwrap());
            }
        }
        res
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.id, self.top_id);
        self.arg.node.delete(ctx);
        self.trigger.delete(ctx);
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        f(self.id);
        self.arg.node.refs(f);
        self.trigger.refs(f);
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.trigger, self.trigger.typecheck(ctx))?;
        wrap!(self.arg.node, self.arg.node.typecheck(ctx))
    }
}

/*
#[derive(Debug)]
struct OrNever<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    arg: Node<C, E>,
}

impl<C: Ctx, E: UserEvent> OrNever<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        arg: &Expr,
    ) -> Result<Node<C, E>> {
        let arg = compile(ctx, arg.clone(), scope, top_id)?;
        let typ = arg.typ().diff(&ctx.env, &Type::Primitive(Typ::Null.into()))?;
        Ok(Box::new(Self { spec, typ, arg }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for OrNever<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        self.arg.update(ctx, event).and_then(|v| match v {
            Value::Null => None,
            v => Some(v),
        })
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.arg.delete(ctx)
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.arg.refs(f)
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.arg, self.arg.typecheck(ctx))?;
        let rtyp = self.arg.typ().diff(&ctx.env, &Type::Primitive(Typ::Null.into()))?;
        Ok(self.typ.check_contains(&ctx.env, &rtyp)?)
    }
}

#[derive(Debug)]
struct MapNull<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    to: Cached<C, E>,
    arg: Cached<C, E>,
}

impl<C: Ctx, E: UserEvent> MapNull<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        to: &Expr,
        arg: &Expr,
    ) -> Result<Node<C, E>> {
        let to = Cached::new(compile(ctx, to.clone(), scope, top_id)?);
        let arg = Cached::new(compile(ctx, arg.clone(), scope, top_id)?);
        let typ = arg.node.typ().diff(&ctx.env, &Type::Primitive(Typ::Null.into()))?;
        Ok(Box::new(Self { spec, typ, to, arg }))
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for MapNull<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        let up = [self.to.update(ctx, event), self.arg.update(ctx, event)];
        match (up, &self.to.cached, &self.arg.cached) {
            ([false, false], _, _) => None,
            ([_, _], Some(v), Some(Value::Null)) => Some(v.clone()),
            ([_, true], _, Some(v)) => Some(v.clone()),
            ([_, _], _, _) => None,
        }
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.to.node.delete(ctx);
        self.arg.node.delete(ctx)
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.to.node.refs(f);
        self.arg.node.refs(f)
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.to.node, self.to.node.typecheck(ctx))?;
        wrap!(self.arg.node, self.arg.node.typecheck(ctx))?;
        let rtyp =
            self.arg.node.typ().diff(&ctx.env, &Type::Primitive(Typ::Null.into()))?;
        wrap!(self.to.node, rtyp.check_contains(&ctx.env, self.to.node.typ()))?;
        Ok(self.typ.check_contains(&ctx.env, &rtyp)?)
    }
}
*/
