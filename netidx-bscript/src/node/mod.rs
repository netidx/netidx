use crate::{
    compile,
    env::{self, LambdaDef},
    err,
    expr::{self, Expr, ExprId, ExprKind, ModPath},
    pattern::{PatternNode, StructPatternNode},
    typ::Type,
    Apply, BindId, Ctx, Event, ExecCtx, Node, Update, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::{literal, ArcStr};
use combine::stream::position::SourcePosition;
use enumflags2::BitFlags;
use netidx::{publisher::Typ, subscriber::Value};
use netidx_netproto::valarray::ValArray;
use smallvec::{smallvec, SmallVec};
use std::{cell::RefCell, iter, sync::Arc};
use triomphe::Arc as TArc;

pub(crate) mod callsite;
pub(crate) mod lambda;

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

#[derive(Debug)]
pub(crate) struct Nop {
    pub spec: Expr,
    pub typ: Type,
}

impl Nop {
    pub(crate) fn new<C: Ctx, E: UserEvent>(typ: Type) -> Node<C, E> {
        Box::new(Nop {
            spec: ExprKind::Constant(Value::String(literal!("nop")))
                .to_expr(Default::default()),
            typ,
        })
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
        &self.spec
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

    /// update the node, return true if the node updated AND the new
    /// value is different from the old value. The cached field will
    /// only be updated if the value changed.
    fn update_changed(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> bool {
        match self.node.update(ctx, event) {
            v @ Some(_) if v != self.cached => {
                self.cached = v;
                true
            }
            Some(_) | None => false,
        }
    }
}

#[derive(Debug)]
pub struct SelectNode<C: Ctx, E: UserEvent> {
    selected: Option<usize>,
    arg: Cached<C, E>,
    arms: SmallVec<[(PatternNode<C, E>, Cached<C, E>); 8]>,
    typ: Type,
    spec: Expr,
}

impl<C: Ctx, E: UserEvent> Update<C, E> for SelectNode<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        let SelectNode { selected, arg, arms, typ: _, spec: _ } = self;
        let mut val_up: SmallVec<[bool; 64]> = smallvec![];
        let arg_up = arg.update(ctx, event);
        macro_rules! bind {
            ($i:expr) => {{
                if let Some(arg) = arg.cached.as_ref() {
                    arms[$i].0.bind_event(event, arg);
                }
            }};
        }
        macro_rules! update {
            () => {
                for (_, val) in arms.iter_mut() {
                    val_up.push(val.update(ctx, event));
                }
            };
        }
        macro_rules! val {
            ($i:expr) => {{
                if val_up[$i] {
                    arms[$i].1.cached.clone()
                } else {
                    None
                }
            }};
        }
        let mut pat_up = false;
        for (pat, _) in arms.iter_mut() {
            if arg_up && pat.guard.is_some() {
                if let Some(arg) = arg.cached.as_ref() {
                    pat.bind_event(event, arg);
                }
            }
            pat_up |= pat.update(ctx, event);
            if arg_up && pat.guard.is_some() {
                pat.unbind_event(event);
            }
        }
        if !arg_up && !pat_up {
            update!();
            selected.and_then(|i| val!(i))
        } else {
            let sel = match arg.cached.as_ref() {
                None => None,
                Some(v) => {
                    let typ = Typ::get(v);
                    arms.iter().enumerate().find_map(|(i, (pat, _))| {
                        if pat.is_match(&ctx.env, typ, v) {
                            Some(i)
                        } else {
                            None
                        }
                    })
                }
            };
            match (sel, *selected) {
                (Some(i), Some(j)) if i == j => {
                    if arg_up {
                        bind!(i);
                    }
                    update!();
                    val!(i)
                }
                (Some(i), Some(_) | None) => {
                    bind!(i);
                    update!();
                    *selected = Some(i);
                    val_up[i] = true;
                    val!(i)
                }
                (None, Some(_)) => {
                    update!();
                    *selected = None;
                    None
                }
                (None, None) => {
                    update!();
                    None
                }
            }
        }
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        let mut ids: SmallVec<[BindId; 8]> = smallvec![];
        let Self { selected: _, arg, arms, typ: _, spec: _ } = self;
        arg.node.delete(ctx);
        for (pat, arg) in arms {
            arg.node.delete(ctx);
            pat.structure_predicate.ids(&mut |id| ids.push(id));
            if let Some(n) = &mut pat.guard {
                n.node.delete(ctx);
            }
            for id in ids.drain(..) {
                ctx.env.unbind_variable(id);
            }
        }
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        let Self { selected: _, arg, arms, typ: _, spec: _ } = self;
        arg.node.refs(f);
        for (pat, arg) in arms {
            arg.node.refs(f);
            pat.structure_predicate.ids(f);
            if let Some(n) = &pat.guard {
                n.node.refs(f);
            }
        }
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        self.arg.node.typecheck(ctx)?;
        let mut rtype = Type::Bottom;
        let mut mtype = Type::Bottom;
        let mut itype = Type::Bottom;
        for (pat, n) in self.arms.iter_mut() {
            match &mut pat.guard {
                Some(guard) => guard.node.typecheck(ctx)?,
                None => mtype = mtype.union(&pat.type_predicate),
            }
            itype = itype.union(&pat.type_predicate);
            n.node.typecheck(ctx)?;
            rtype = rtype.union(&n.node.typ());
        }
        itype
            .check_contains(&ctx.env, &self.arg.node.typ())
            .map_err(|e| anyhow!("missing match cases {e}"))?;
        mtype
            .check_contains(&ctx.env, &self.arg.node.typ())
            .map_err(|e| anyhow!("missing match cases {e}"))?;
        let mut atype = self.arg.node.typ().clone().normalize();
        for (pat, _) in self.arms.iter() {
            let can_match = atype.contains(&ctx.env, &pat.type_predicate)?
                || pat.type_predicate.contains(&ctx.env, &atype)?;
            if !can_match {
                bail!(
                    "pattern {} will never match {}, unused match cases",
                    pat.type_predicate,
                    atype
                )
            }
            if !pat.structure_predicate.is_refutable() && pat.guard.is_none() {
                atype = atype.diff(&ctx.env, &pat.type_predicate)?;
            }
        }
        self.typ = rtype;
        Ok(())
    }

    fn typ(&self) -> &Type {
        &self.typ
    }

    fn spec(&self) -> &Expr {
        &self.spec
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
        let typ = match &source.typ() {
            Type::Array(et) => Type::Set(TArc::from_iter([(**et).clone(), ert.clone()])),
            _ => Type::Set(TArc::from_iter([Type::empty_tvar(), ert.clone()])),
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
        let at = Type::Array(TArc::new(self.typ.clone()));
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
        start: &Option<TArc<Expr>>,
        end: &Option<TArc<Expr>>,
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
        let typ = Type::Set(TArc::from_iter([
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
                TArc::make_mut(used).push(name.clone());
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
            TArc::make_mut(used).retain(|n| n != &self.name);
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
struct TypeDef {
    spec: Expr,
    scope: ModPath,
    name: ModPath,
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
    children: SmallVec<[Node<C, E>; 8]>,
}

impl<C: Ctx, E: UserEvent> Block<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        exprs: &TArc<[Expr]>,
    ) -> Result<Node<C, E>> {
        let children = exprs
            .iter()
            .map(|e| compile(ctx, e.clone(), scope, top_id))
            .collect::<Result<SmallVec<[Node<C, E>; 8]>>>()?;
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
struct Bind<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    pattern: StructPatternNode,
    node: Node<C, E>,
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
struct Ref {
    spec: Expr,
    typ: Type,
    id: BindId,
    top_id: ExprId,
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
struct StructRef<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    source: Node<C, E>,
    field: usize,
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
struct TupleRef<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    source: Node<C, E>,
    field: usize,
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
    args: SmallVec<[Cached<C, E>; 8]>,
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
struct Qop<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    id: BindId,
    n: Node<C, E>,
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
struct TypeCast<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    target: Type,
    n: Node<C, E>,
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
struct Any<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    n: SmallVec<[Node<C, E>; 8]>,
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
    n: SmallVec<[Cached<C, E>; 8]>,
}

impl<C: Ctx, E: UserEvent> Array<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        args: &TArc<[Expr]>,
    ) -> Result<Node<C, E>> {
        let n = args
            .iter()
            .map(|e| Ok(Cached::new(compile(ctx, e.clone(), scope, top_id)?)))
            .collect::<Result<_>>()?;
        let typ = Type::Array(TArc::new(Type::empty_tvar()));
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
        let rtype = Type::Array(TArc::new(rtype));
        Ok(self.typ.check_contains(&ctx.env, &rtype)?)
    }
}

#[derive(Debug)]
pub(crate) struct Tuple<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    n: SmallVec<[Cached<C, E>; 8]>,
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
            .collect::<Result<SmallVec<[_; 8]>>>()?;
        let typ = Type::Tuple(TArc::from_iter(n.iter().map(|n| n.node.typ().clone())));
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
    n: SmallVec<[Cached<C, E>; 8]>,
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
            .collect::<Result<SmallVec<[_; 8]>>>()?;
        let typs = TArc::from_iter(n.iter().map(|n| n.node.typ().clone()));
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
    names: SmallVec<[ArcStr; 8]>,
    n: SmallVec<[Cached<C, E>; 8]>,
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
            .collect::<Result<SmallVec<[_; 8]>>>()?;
        let typs =
            names.iter().zip(n.iter()).map(|(n, a)| (n.clone(), a.node.typ().clone()));
        let typ = Type::Struct(TArc::from_iter(typs));
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
struct StructWith<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    source: Node<C, E>,
    current: Option<ValArray>,
    replace: SmallVec<[(usize, Cached<C, E>); 8]>,
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
        struct $name<C: Ctx, E: UserEvent> {
            spec: Expr,
            typ: Type,
            lhs: Cached<C, E>,
            rhs: Cached<C, E>,
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
        struct $name<C: Ctx, E: UserEvent> {
            spec: Expr,
            typ: Type,
            lhs: Cached<C, E>,
            rhs: Cached<C, E>,
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
struct Not<C: Ctx, E: UserEvent> {
    spec: Expr,
    typ: Type,
    n: Cached<C, E>,
}

impl<C: Ctx, E: UserEvent> Update<C, E> for Not<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        if self.n.update(ctx, event) {
            self.n.cached.as_ref().and_then(|v| match v {
                Value::Bool(b) => Some(Value::Bool(!*b)),
                _ => None,
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
        self.n.node.refs(f);
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.n.node.delete(ctx);
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        wrap!(self.n.node, self.n.node.typecheck(ctx))?;
        let bt = Type::Primitive(Typ::Bool.into());
        wrap!(self.n.node, bt.check_contains(&ctx.env, self.n.node.typ()))?;
        wrap!(self, self.typ.check_contains(&ctx.env, &Type::boolean()))
    }
}

macro_rules! arith_op {
    ($name:ident, $op:tt) => {
        #[derive(Debug)]
        struct $name<C: Ctx, E: UserEvent> {
            spec: Expr,
            typ: Type,
            lhs: Cached<C, E>,
            rhs: Cached<C, E>
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

/*
#[derive(Debug)]
pub enum NodeKind<C: Ctx, E: UserEvent> {
    Nop,
    Use {
        scope: ModPath,
        name: ModPath,
    },
    TypeDef {
        scope: ModPath,
        name: ArcStr,
    },
    Constant(Value),
    Module(Box<[Node<C, E>]>),
    Do(Box<[Node<C, E>]>),
    Bind {
        pattern: Box<StructPatternNode>,
        node: Box<Node<C, E>>,
    },
    Ref {
        id: BindId,
        top_id: ExprId,
    },
    StructRef {
        source: Box<Node<C, E>>,
        field: usize,
        top_id: ExprId,
    },
    TupleRef {
        source: Box<Node<C, E>>,
        field: usize,
        top_id: ExprId,
    },
    ArrayRef(Box<ArrayRefNode<C, E>>),
    ArraySlice(Box<ArraySliceNode<C, E>>),
    StringInterpolate {
        args: Box<[Cached<C, E>]>,
    },
    Connect(BindId, Box<Node<C, E>>),
    Lambda(Arc<LambdaDef<C, E>>),
    Qop(BindId, Box<Node<C, E>>),
    TypeCast {
        target: Type,
        n: Box<Node<C, E>>,
    },
    Any {
        args: Box<[Node<C, E>]>,
    },
    Array {
        args: Box<[Cached<C, E>]>,
    },
    Tuple {
        args: Box<[Cached<C, E>]>,
    },
    Variant {
        tag: ArcStr,
        args: Box<[Cached<C, E>]>,
    },
    Struct {
        names: Box<[ArcStr]>,
        args: Box<[Cached<C, E>]>,
    },
    StructWith {
        source: Box<Node<C, E>>,
        current: Option<ValArray>,
        replace: Box<[(usize, Cached<C, E>)]>,
    },
    Apply(Box<CallSite<C, E>>),
    Select(Box<SelectNode<C, E>>),
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
}

/*
pub struct Node<C: Ctx, E: UserEvent> {
    pub spec: Box<Expr>,
    pub typ: Type,
    pub kind: NodeKind<C, E>,
}
*/

impl<C: Ctx, E: UserEvent> fmt::Debug for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.kind)
    }
}

impl<C: Ctx, E: UserEvent> Default for Node<C, E> {
    fn default() -> Self {
        genn::nop()
    }
}

impl<C: Ctx, E: UserEvent> fmt::Display for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.spec)
    }
}

impl<C: Ctx, E: UserEvent> Node<C, E> {
    pub fn compile(ctx: &mut ExecCtx<C, E>, scope: &ModPath, spec: Expr) -> Result<Self> {
        let top_id = spec.id;
        let env = ctx.env.clone();
        let mut node = match compiler::compile(ctx, spec, scope, top_id) {
            Ok(n) => n,
            Err(e) => {
                ctx.env = env;
                return Err(e);
            }
        };
        if let Err(e) = node.typecheck(ctx) {
            ctx.env = env;
            return Err(e);
        }
        Ok(node)
    }

    pub fn delete(self, ctx: &mut ExecCtx<C, E>) {
        let mut ids: SmallVec<[BindId; 8]> = smallvec![];
        match self.kind {
            NodeKind::Constant(_) | NodeKind::Nop => (),
            NodeKind::Ref { id, top_id } => ctx.user.unref_var(id, top_id),
            NodeKind::StructRef { mut source, field: _, top_id: _ }
            | NodeKind::TupleRef { mut source, field: _, top_id: _ } => {
                mem::take(&mut *source).delete(ctx)
            }
            NodeKind::ArrayRef(mut n) => mem::take(&mut *n).delete(ctx),
            NodeKind::ArraySlice(mut n) => mem::take(&mut *n).delete(ctx),
            NodeKind::Add { mut lhs, mut rhs }
            | NodeKind::Sub { mut lhs, mut rhs }
            | NodeKind::Mul { mut lhs, mut rhs }
            | NodeKind::Div { mut lhs, mut rhs }
            | NodeKind::Eq { mut lhs, mut rhs }
            | NodeKind::Ne { mut lhs, mut rhs }
            | NodeKind::Lte { mut lhs, mut rhs }
            | NodeKind::Lt { mut lhs, mut rhs }
            | NodeKind::Gt { mut lhs, mut rhs }
            | NodeKind::Gte { mut lhs, mut rhs }
            | NodeKind::And { mut lhs, mut rhs }
            | NodeKind::Or { mut lhs, mut rhs } => {
                mem::take(&mut lhs.node).delete(ctx);
                mem::take(&mut rhs.node).delete(ctx);
            }
            NodeKind::Use { scope, name } => {
                todo!()
            }
            NodeKind::TypeDef { scope, name } => todo!(),
            NodeKind::Module(nodes)
            | NodeKind::Do(nodes)
            | NodeKind::Any { args: nodes } => {
                for n in nodes {
                    n.delete(ctx)
                }
            }
            NodeKind::StringInterpolate { args } => {
                for n in args {
                    n.node.delete(ctx)
                }
            }
            NodeKind::Connect(_, mut n)
            | NodeKind::TypeCast { target: _, mut n }
            | NodeKind::Qop(_, mut n)
            | NodeKind::Not { node: mut n } => mem::take(&mut *n).delete(ctx),
            NodeKind::Variant { tag: _, args }
            | NodeKind::Array { args }
            | NodeKind::Tuple { args }
            | NodeKind::Struct { names: _, args } => {
                for n in args {
                    n.node.delete(ctx)
                }
            }
            NodeKind::StructWith { mut source, current: _, replace } => {
                mem::take(&mut *source).delete(ctx);
                for (_, n) in replace {
                    n.node.delete(ctx)
                }
            }
            NodeKind::Bind { pattern, node } => {
                pattern.ids(&mut |id| ids.push(id));
                node.delete(ctx);
                for id in ids.drain(..) {
                    ctx.env.unbind_variable(id)
                }
            }
            NodeKind::Select(sn) => sn.delete(ctx),
            NodeKind::Lambda(lb) => {
                ctx.env.lambdas.remove_cow(&lb.id);
            }
            NodeKind::Apply(site) => site.delete(ctx),
        }
    }

    /// call f with the id of every variable referenced by self
    pub fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        match &self.kind {
            NodeKind::Constant(_)
            | NodeKind::Nop
            | NodeKind::Use { .. }
            | NodeKind::TypeDef { .. }
            | NodeKind::Lambda(_) => (),
            NodeKind::Ref { id, top_id: _ } => f(*id),
            NodeKind::StructRef { source, field: _, top_id: _ }
            | NodeKind::TupleRef { source, field: _, top_id: _ } => {
                source.refs(f);
            }
            NodeKind::ArrayRef(n) => n.refs(f),
            NodeKind::ArraySlice(n) => n.refs(f),
            NodeKind::StringInterpolate { args } => {
                for a in args {
                    a.node.refs(f)
                }
            }
            NodeKind::Add { lhs, rhs }
            | NodeKind::Sub { lhs, rhs }
            | NodeKind::Mul { lhs, rhs }
            | NodeKind::Div { lhs, rhs }
            | NodeKind::Eq { lhs, rhs }
            | NodeKind::Ne { lhs, rhs }
            | NodeKind::Lte { lhs, rhs }
            | NodeKind::Lt { lhs, rhs }
            | NodeKind::Gt { lhs, rhs }
            | NodeKind::Gte { lhs, rhs }
            | NodeKind::And { lhs, rhs }
            | NodeKind::Or { lhs, rhs } => {
                lhs.node.refs(f);
                rhs.node.refs(f);
            }
            NodeKind::Module(nodes)
            | NodeKind::Do(nodes)
            | NodeKind::Any { args: nodes } => {
                for n in nodes {
                    n.refs(f)
                }
            }
            NodeKind::Connect(_, n)
            | NodeKind::TypeCast { target: _, n }
            | NodeKind::Qop(_, n)
            | NodeKind::Not { node: n } => n.refs(f),
            NodeKind::Variant { tag: _, args }
            | NodeKind::Array { args }
            | NodeKind::Tuple { args }
            | NodeKind::Struct { names: _, args } => {
                for n in args {
                    n.node.refs(f)
                }
            }
            NodeKind::StructWith { source, current: _, replace } => {
                source.refs(f);
                for (_, n) in replace {
                    n.node.refs(f)
                }
            }
            NodeKind::Bind { pattern, node } => {
                pattern.ids(f);
                node.refs(f);
            }
            NodeKind::Select(sn) => sn.refs(f),
            NodeKind::Apply(site) => {
                let CallSite { ftype: _, fnode, args, arg_spec: _, function, top_id: _ } =
                    &**site;
                if let Some((_, fun)) = function {
                    fun.refs(f)
                }
                fnode.refs(f);
                for n in args {
                    n.refs(f)
                }
            }
        }
    }

    pub fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        event: &mut Event<E>,
    ) -> Option<Value> {
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
        macro_rules! update_args {
            ($args:expr) => {{
                let mut updated = false;
                let mut determined = true;
                for n in $args.iter_mut() {
                    updated |= n.update(ctx, event);
                    determined &= n.cached.is_some();
                }
                (updated, determined)
            }};
        }
        match &mut self.kind {
            NodeKind::Constant(v) => {
                todo!()
            }
            NodeKind::StringInterpolate { args } => {
                thread_local! {
                    static BUF: RefCell<String> = RefCell::new(String::new());
                }
                let (updated, determined) = update_args!(args);
                if updated && determined {
                    BUF.with_borrow_mut(|buf| {
                        buf.clear();
                        for c in args {
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
            NodeKind::ArrayRef(n) => n.update(ctx, event),
            NodeKind::ArraySlice(n) => n.update(ctx, event),
            NodeKind::Array { args } | NodeKind::Tuple { args } => {
                if args.is_empty() && event.init {
                    return Some(Value::Array(ValArray::from([])));
                }
                let (updated, determined) = update_args!(args);
                if updated && determined {
                    let iter = args.iter().map(|n| n.cached.clone().unwrap());
                    Some(Value::Array(ValArray::from_iter_exact(iter)))
                } else {
                    None
                }
            }
            NodeKind::Variant { tag, args } if args.len() == 0 => {
                if event.init {
                    Some(Value::String(tag.clone()))
                } else {
                    None
                }
            }
            NodeKind::Variant { tag, args } => {
                let (updated, determined) = update_args!(args);
                if updated && determined {
                    let a = iter::once(Value::String(tag.clone()))
                        .chain(args.iter().map(|n| n.cached.clone().unwrap()))
                        .collect::<SmallVec<[_; 8]>>();
                    Some(Value::Array(ValArray::from_iter_exact(a.into_iter())))
                } else {
                    None
                }
            }
            NodeKind::Any { args } => args
                .iter_mut()
                .filter_map(|s| s.update(ctx, event))
                .fold(None, |r, v| r.or(Some(v))),
            NodeKind::Struct { names, args } => {
                if args.is_empty() && event.init {
                    return Some(Value::Array(ValArray::from([])));
                }
                let mut updated = false;
                let mut determined = true;
                for n in args.iter_mut() {
                    updated |= n.update(ctx, event);
                    determined &= n.cached.is_some();
                }
                if updated && determined {
                    let iter = names.iter().zip(args.iter()).map(|(name, n)| {
                        let name = Value::String(name.clone());
                        let v = n.cached.clone().unwrap();
                        Value::Array(ValArray::from_iter_exact([name, v].into_iter()))
                    });
                    Some(Value::Array(ValArray::from_iter_exact(iter)))
                } else {
                    None
                }
            }
            NodeKind::StructWith { source, current, replace } => {
                let mut updated = source
                    .update(ctx, event)
                    .map(|v| match v {
                        Value::Array(a) => {
                            *current = Some(a.clone());
                            true
                        }
                        _ => false,
                    })
                    .unwrap_or(false);
                let mut determined = current.is_some();
                for (_, n) in replace.iter_mut() {
                    updated |= n.update(ctx, event);
                    determined &= n.cached.is_some();
                }
                if updated && determined {
                    let mut si = 0;
                    let iter = current.as_ref().unwrap().iter().enumerate().map(
                        |(i, v)| match v {
                            Value::Array(v) if v.len() == 2 => {
                                if si < replace.len() && i == replace[si].0 {
                                    let r = replace[si].1.cached.clone().unwrap();
                                    si += 1;
                                    Value::Array(ValArray::from_iter_exact(
                                        [v[0].clone(), r].into_iter(),
                                    ))
                                } else {
                                    Value::Array(v.clone())
                                }
                            }
                            _ => v.clone(),
                        },
                    );
                    Some(Value::Array(ValArray::from_iter_exact(iter)))
                } else {
                    None
                }
            }
            NodeKind::Apply(site) => site.update(ctx, event),
            NodeKind::Bind { pattern, node } => {
                if let Some(v) = node.update(ctx, event) {
                    pattern.bind(&v, &mut |id, v| ctx.set_var(id, v))
                }
                None
            }
            NodeKind::Connect(id, rhs) => {
                if let Some(v) = rhs.update(ctx, event) {
                    ctx.set_var(*id, v)
                }
                None
            }
            NodeKind::Ref { id: bid, .. } => event.variables.get(bid).map(|v| v.clone()),
            NodeKind::TupleRef { source, field: i, .. } => {
                source.update(ctx, event).and_then(|v| match v {
                    Value::Array(a) => a.get(*i).map(|v| v.clone()),
                    _ => None,
                })
            }
            NodeKind::StructRef { source, field: i, .. } => {
                match source.update(ctx, event) {
                    Some(Value::Array(a)) => a.get(*i).and_then(|v| match v {
                        Value::Array(a) if a.len() == 2 => Some(a[1].clone()),
                        _ => None,
                    }),
                    Some(_) | None => None,
                }
            }
            NodeKind::Qop(id, n) => match n.update(ctx, event) {
                None => None,
                Some(e @ Value::Error(_)) => {
                    ctx.set_var(*id, e);
                    None
                }
                Some(v) => Some(v),
            },
            NodeKind::Module(children) | NodeKind::Do(children) => {
                children.into_iter().fold(None, |_, n| n.update(ctx, event))
            }
            NodeKind::TypeCast { target, n } => {
                n.update(ctx, event).map(|v| target.cast_value(&ctx.env, v))
            }
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
            NodeKind::Select(sn) => sn.update(ctx, event),
            NodeKind::Lambda(lb) if event.init => Some(Value::U64(lb.id.0)),
            NodeKind::Use { .. }
            | NodeKind::Lambda(_)
            | NodeKind::TypeDef { .. }
            | NodeKind::Nop => None,
        }
    }
}

/// helpers for dynamically generating code in built-in functions. Not used by the compiler
pub mod genn {
    use super::*;

    /// return a no op node
    pub fn nop<C: Ctx, E: UserEvent>() -> Node<C, E> {
        Node {
            spec: Box::new(
                ExprKind::Constant(Value::String(literal!("nop")))
                    .to_expr(Default::default()),
            ),
            typ: Type::Bottom,
            kind: NodeKind::Nop,
        }
    }

    /// bind a variable and return a node referencing it
    pub fn bind<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        scope: &ModPath,
        name: &str,
        typ: Type,
        top_id: ExprId,
    ) -> (BindId, Node<C, E>) {
        let id = ctx.env.bind_variable(scope, name, typ.clone()).id;
        ctx.user.ref_var(id, top_id);
        let spec = Box::new(
            ExprKind::Ref { name: ModPath(scope.0.append(name)) }
                .to_expr(Default::default()),
        );
        let kind = NodeKind::Ref { id, top_id };
        (id, Node { spec, kind, typ })
    }

    /// generate a reference to a bind id
    pub fn reference<C: Ctx, E: UserEvent>(
        ctx: &mut ExecCtx<C, E>,
        id: BindId,
        typ: Type,
        top_id: ExprId,
    ) -> Node<C, E> {
        ctx.user.ref_var(id, top_id);
        let spec = Box::new(
            ExprKind::Ref { name: ModPath::from(["x"]) }.to_expr(Default::default()),
        );
        let kind = NodeKind::Ref { id, top_id };
        Node { spec, kind, typ }
    }

    /// generate and return an apply node for the given lambda
    pub fn apply<C: Ctx, E: UserEvent>(
        fnode: Node<C, E>,
        args: Vec<Node<C, E>>,
        typ: TArc<FnType>,
        top_id: ExprId,
    ) -> Node<C, E> {
        let spec = ExprKind::Apply {
            args: TArc::from_iter(args.iter().map(|n| (None, (*n.spec).clone()))),
            function: TArc::new((*fnode.spec).clone()),
        }
        .to_expr(Default::default());
        let site = Box::new(CallSite {
            ftype: typ.clone(),
            args,
            arg_spec: HashMap::default(),
            fnode,
            function: None,
            top_id,
        });
        let typ = typ.rtype.clone();
        Node { spec: Box::new(spec), typ, kind: NodeKind::Apply(site) }
    }
}
*/
