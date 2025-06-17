use crate::{
    env, err,
    expr::{self, Expr, ExprId, ExprKind, ModPath},
    typ::{self, TVar, Type},
    BindId, Ctx, Event, ExecCtx, Node, Update, UserEvent,
};
use anyhow::{anyhow, bail, Context, Result};
use arcstr::{literal, ArcStr};
use combine::stream::position::SourcePosition;
use compiler::compile;
use enumflags2::BitFlags;
use netidx::{publisher::Typ, subscriber::Value};
use netidx_netproto::valarray::ValArray;
use pattern::StructPatternNode;
use smallvec::{smallvec, SmallVec};
use std::{cell::RefCell, iter, sync::LazyLock};
use triomphe::Arc;

pub(crate) mod callsite;
pub(crate) mod compiler;
pub mod genn;
pub(crate) mod lambda;
pub(crate) mod pattern;
pub(crate) mod select;

#[macro_export]
macro_rules! wrap {
    ($n:expr, $e:expr) => {
        match $e {
            Ok(x) => Ok(x),
            Err(e) => Err(anyhow!("in expr: {}, type error: {e}", $n.spec())),
        }
    };
}

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

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.source.node.refs(f);
        self.i.node.refs(f);
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
        let it = Type::Primitive(Typ::unsigned_integer());
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

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.source.node.refs(f);
        if let Some(start) = &self.start {
            start.node.refs(f)
        }
        if let Some(end) = &self.end {
            end.node.refs(f)
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

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn spec(&self) -> &Expr {
        &self.spec
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
    spec: Expr,
    value: Value,
    typ: Type,
}

impl Constant {
    pub(crate) fn compile<C: Ctx, E: UserEvent>(
        spec: Expr,
        value: &Value,
    ) -> Result<Node<C, E>> {
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
                let ptyp = pattern.infer_type_predicate();
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
        thread_local! {
            static BUF: RefCell<String> = RefCell::new(String::new());
        }
        let (updated, determined) = update_args!(self.args, ctx, event);
        if updated && determined {
            BUF.with_borrow_mut(|buf| {
                buf.clear();
                for c in &self.args {
                    match c.cached.as_ref().unwrap() {
                        Value::String(c) => buf.push_str(c.as_ref()),
                        v => match v.clone().cast_to::<ArcStr>().ok() {
                            Some(c) => buf.push_str(c.as_ref()),
                            None => {
                                let m = literal!("args must be strings");
                                return Some(Value::Error(m));
                            }
                        },
                    }
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
                    let new_id = BindId::from_u64(i);
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
        let typ = target.union(&Type::Primitive(Typ::Error.into()));
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
        let rtyp = self.n.iter().fold(rtyp, |rtype, n| n.typ().union(&rtype));
        Ok(self.typ.check_contains(&ctx.env, &rtyp)?)
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

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.n.iter().for_each(|n| n.node.refs(f))
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        for n in &mut self.n {
            wrap!(n.node, n.node.typecheck(ctx))?
        }
        let rtype = Type::Bottom;
        let rtype = self.n.iter().fold(rtype, |rtype, n| n.node.typ().union(&rtype));
        let rtype = Type::Array(Arc::new(rtype));
        Ok(self.typ.check_contains(&ctx.env, &rtype)?)
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

            fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
                self.lhs.node.refs(f);
                self.rhs.node.refs(f);
            }

            fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
                self.lhs.node.delete(ctx);
                self.rhs.node.delete(ctx)
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

            fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
                self.lhs.node.refs(f);
                self.rhs.node.refs(f);
            }

            fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
                self.lhs.node.delete(ctx);
                self.rhs.node.delete(ctx)
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

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        self.n.refs(f);
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.delete(ctx);
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

            fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
                self.lhs.node.refs(f);
                self.rhs.node.refs(f);
            }

            fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
                self.lhs.node.delete(ctx);
                self.rhs.node.delete(ctx);
            }

            fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
                wrap!(self.lhs.node, self.lhs.node.typecheck(ctx))?;
                wrap!(self.rhs.node, self.rhs.node.typecheck(ctx))?;
                let typ = Type::Primitive(Typ::number());
                let lhs = self.lhs.node.typ();
                let rhs = self.rhs.node.typ();
                wrap!(self.lhs.node, typ.check_contains(&ctx.env, lhs))?;
                wrap!(self.rhs.node, typ.check_contains(&ctx.env, rhs))?;
                wrap!(self,self.typ.check_contains(&ctx.env, &lhs.union(rhs)))
            }
        }
    }
}

arith_op!(Add, +);
arith_op!(Sub, -);
arith_op!(Mul, *);
arith_op!(Div, /);
