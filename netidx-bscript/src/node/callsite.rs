use super::{compiler::compile, genn, Nop};
use crate::{
    env::LambdaDef,
    expr::{Expr, ExprId, ModPath},
    typ::{FnArgType, FnType, Type},
    wrap, Apply, BindId, Ctx, Event, ExecCtx, LambdaId, Node, Update, UserEvent,
};
use anyhow::{bail, Context, Result};
use arcstr::ArcStr;
use combine::stream::position::SourcePosition;
use compact_str::{format_compact, CompactString};
use fxhash::FxHashMap;
use netidx::subscriber::Value;
use std::{collections::hash_map::Entry, mem, sync::Arc};
use triomphe::Arc as TArc;

fn check_named_args(
    named: &mut FxHashMap<ArcStr, Expr>,
    args: &[(Option<ArcStr>, Expr)],
) -> Result<()> {
    for (name, e) in args.iter() {
        if let Some(name) = name {
            match named.entry(name.clone()) {
                Entry::Occupied(e) => bail!("duplicate labeled argument {}", e.key()),
                Entry::Vacant(en) => en.insert(e.clone()),
            };
        }
    }
    Ok(())
}

fn check_extra_named(named: &FxHashMap<ArcStr, Expr>) -> Result<()> {
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
    Ok(())
}

fn compile_apply_args<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    scope: &ModPath,
    top_id: ExprId,
    typ: &FnType,
    args: &TArc<[(Option<ArcStr>, Expr)]>,
) -> Result<(Vec<Node<C, E>>, FxHashMap<ArcStr, bool>)> {
    let mut named = FxHashMap::default();
    let mut nodes: Vec<Node<C, E>> = vec![];
    let mut arg_spec: FxHashMap<ArcStr, bool> = FxHashMap::default();
    check_named_args(&mut named, args)?;
    for a in typ.args.iter() {
        match &a.label {
            None => break,
            Some((n, optional)) => match named.remove(n) {
                Some(e) => {
                    nodes.push(compile(ctx, e, scope, top_id)?);
                    arg_spec.insert(n.clone(), false);
                }
                None if !optional => bail!("missing required argument {n}"),
                None => {
                    nodes.push(Nop::new(a.typ.clone()));
                    arg_spec.insert(n.clone(), true);
                }
            },
        }
    }
    check_extra_named(&named)?;
    for (name, e) in args.iter() {
        if name.is_none() {
            nodes.push(compile(ctx, e.clone(), scope, top_id)?);
        }
    }
    if nodes.len() < typ.args.len() {
        bail!("missing required argument")
    }
    Ok((nodes, arg_spec))
}

#[derive(Debug)]
pub(crate) struct CallSite<C: Ctx, E: UserEvent> {
    pub(super) spec: TArc<Expr>,
    pub(super) ftype: TArc<FnType>,
    pub(super) fnode: Node<C, E>,
    pub(super) actual_args: Vec<Node<C, E>>,
    pub(super) queued: Vec<(BindId, Value)>,
    pub(super) ref_args: Vec<Node<C, E>>,
    pub(super) ref_ids: Vec<BindId>,
    pub(super) arg_spec: FxHashMap<ArcStr, bool>, // true if arg is using the default value
    pub(super) function: Option<(LambdaId, Box<dyn Apply<C, E>>)>,
    pub(super) top_id: ExprId,
}

impl<C: Ctx, E: UserEvent> CallSite<C, E> {
    pub(crate) fn compile(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: &ModPath,
        top_id: ExprId,
        args: &TArc<[(Option<ArcStr>, Expr)]>,
        f: &TArc<Expr>,
        pos: &SourcePosition,
    ) -> Result<Node<C, E>> {
        let fnode = compile(ctx, (**f).clone(), scope, top_id)?;
        let ftype = match &fnode.typ() {
            Type::Fn(ftype) => ftype.clone(),
            typ => bail!("at {pos} {f} has {typ}, expected a function"),
        };
        let (actual_args, arg_spec) =
            compile_apply_args(ctx, scope, top_id, &ftype, &args)
                .with_context(|| format!("in apply at {pos}"))?;
        let spec = TArc::new(spec);
        let (ref_ids, ref_args) = actual_args
            .iter()
            .map(|n| {
                let id = BindId::new();
                let r = genn::reference(ctx, id, n.typ().clone(), top_id);
                (id, r)
            })
            .unzip();
        let site = Self {
            spec,
            ftype,
            actual_args,
            queued: vec![],
            ref_args,
            ref_ids,
            arg_spec,
            fnode,
            function: None,
            top_id,
        };
        Ok(Box::new(site))
    }

    fn bind(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        f: Arc<LambdaDef<C, E>>,
        event: &mut Event<E>,
        set: &mut Vec<BindId>,
    ) -> Result<()> {
        macro_rules! compile_default {
            ($i:expr, $f:expr) => {{
                match &$f.argspec[$i].labeled {
                    None | Some(None) => bail!("expected default value"),
                    Some(Some(expr)) => ctx.with_restored($f.env.clone(), |ctx| {
                        let n = compile(ctx, expr.clone(), &$f.scope, self.top_id)?;
                        n.refs(&mut |id| {
                            if let Some(v) = ctx.cached.get(&id) {
                                if let Entry::Vacant(e) = event.variables.entry(id) {
                                    e.insert(v.clone());
                                    set.push(id);
                                }
                            }
                        });
                        Ok::<_, anyhow::Error>(n)
                    })?,
                }
            }};
        }
        for (name, map) in self.ftype.map_argpos(&f.typ) {
            let is_default = *self.arg_spec.get(&name).unwrap_or(&false);
            match map {
                (Some(si), Some(oi)) if si == oi => {
                    if is_default {
                        self.actual_args[si] = compile_default!(si, f);
                    }
                }
                (Some(si), Some(oi)) if si < oi => {
                    let mut i = si;
                    while i < oi {
                        self.actual_args.swap(i, i + 1);
                        self.ref_args.swap(i, i + 1);
                        self.ref_ids.swap(i, i + 1);
                        i += 1;
                    }
                    if is_default {
                        self.actual_args[i] = compile_default!(si, f);
                    }
                }
                (Some(si), Some(oi)) if oi < si => {
                    let mut i = si;
                    while i > oi {
                        self.actual_args.swap(i, i - 1);
                        self.ref_args.swap(i, i - 1);
                        self.ref_ids.swap(i, i - 1);
                        i -= 1
                    }
                    if is_default {
                        self.actual_args[i] = compile_default!(i, f);
                    }
                }
                (Some(_), Some(_)) => unreachable!(),
                (Some(i), None) => {
                    self.actual_args.remove(i);
                    self.ref_args.remove(i);
                    ctx.user.unref_var(self.ref_ids.remove(i), self.top_id);
                }
                (None, Some(i)) => {
                    self.actual_args.insert(i, compile_default!(i, f));
                    let typ = self.actual_args[i].typ().clone();
                    let id = BindId::new();
                    self.ref_args.insert(i, genn::reference(ctx, id, typ, self.top_id));
                    self.ref_ids.insert(i, id);
                }
                (None, None) => bail!("unexpected args"),
            }
        }
        let mut rf = (f.init)(ctx, &self.ref_args, self.top_id)?;
        // some nodes, such as structwith, depend on the typecheck pass to
        // resolve things like field indexes. This should always succeed.
        rf.typecheck(ctx, &mut self.ref_args)?;
        self.function = Some((f.id, rf));
        Ok(())
    }
}

impl<C: Ctx, E: UserEvent> Update<C, E> for CallSite<C, E> {
    fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &mut Event<E>) -> Option<Value> {
        macro_rules! error {
            ($m:literal) => {{
                let m = format_compact!($m);
                return Some(Value::Error(m.as_str().into()));
            }};
        }
        macro_rules! set_or_queue {
            ($id:expr, $v:expr) => {
                match event.variables.entry($id) {
                    Entry::Vacant(e) => {
                        e.insert($v);
                    }
                    Entry::Occupied(_) => ctx.user.set_var($id, $v),
                }
            };
        }
        macro_rules! update_args {
            ($defined:expr) => {
                for (i, n) in self.actual_args.iter_mut().enumerate() {
                    if let Some(v) = n.update(ctx, event) {
                        let id = self.ref_ids[i];
                        if $defined {
                            set_or_queue!(id, v)
                        } else {
                            self.queued.push((id, v));
                        }
                    }
                }
            };
        }
        let mut set = vec![];
        let bound = match (&self.function, self.fnode.update(ctx, event)) {
            (_, None) => false,
            (Some((cid, _)), Some(Value::U64(id))) if cid.0 == id => false,
            (_, Some(Value::U64(id))) => match ctx.env.lambdas.get(&LambdaId(id)) {
                None => error!("no such function {id:?}"),
                Some(lb) => match lb.upgrade() {
                    None => error!("function {id:?} is no longer callable"),
                    Some(lb) => {
                        if let Err(e) = self.bind(ctx, lb, event, &mut set) {
                            error!("failed to bind to lambda {e}")
                        }
                        true
                    }
                },
            },
            (_, Some(v)) => error!("invalid function {v}"),
        };
        match &mut self.function {
            None => {
                update_args!(false);
                None
            }
            Some((_, f)) if !bound => {
                update_args!(true);
                f.update(ctx, &mut self.ref_args, event)
            }
            Some((_, f)) => {
                let init = mem::replace(&mut event.init, true);
                f.refs(&mut |id: BindId| {
                    if let Entry::Vacant(e) = event.variables.entry(id) {
                        if let Some(v) = ctx.cached.get(&id) {
                            e.insert(v.clone());
                            set.push(id);
                        }
                    }
                });
                update_args!(true);
                for (id, v) in mem::take(&mut self.queued) {
                    set_or_queue!(id, v)
                }
                let res = f.update(ctx, &mut self.ref_args, event);
                event.init = init;
                for id in set {
                    event.variables.remove(&id);
                }
                res
            }
        }
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        let Self {
            spec: _,
            ftype: _,
            fnode,
            actual_args,
            ref_args,
            ref_ids: _,
            arg_spec: _,
            queued: _,
            function,
            top_id: _,
        } = self;
        if let Some((_, f)) = function {
            f.delete(ctx)
        }
        fnode.delete(ctx);
        for n in actual_args {
            n.delete(ctx)
        }
        for n in ref_args {
            n.delete(ctx)
        }
    }

    fn typ(&self) -> &Type {
        &self.ftype.rtype
    }

    fn spec(&self) -> &Expr {
        &self.spec
    }

    fn typecheck(&mut self, ctx: &mut ExecCtx<C, E>) -> Result<()> {
        for n in self.actual_args.iter_mut() {
            wrap!(n, n.typecheck(ctx))?
        }
        self.ftype.unbind_tvars();
        for (arg, FnArgType { typ, .. }) in
            self.actual_args.iter().zip(self.ftype.args.iter())
        {
            wrap!(arg, typ.check_contains(&ctx.env, &arg.typ()))?;
        }
        for (tv, tc) in self.ftype.constraints.read().iter() {
            wrap!(self, tc.check_contains(&ctx.env, &Type::TVar(tv.clone())))?
        }
        Ok(())
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        let Self {
            spec: _,
            ftype: _,
            fnode,
            queued: _,
            actual_args,
            ref_args,
            ref_ids: _,
            arg_spec: _,
            function,
            top_id: _,
        } = self;
        if let Some((_, fun)) = function {
            fun.refs(f)
        }
        fnode.refs(f);
        for n in actual_args {
            n.refs(f)
        }
        for n in ref_args {
            n.refs(f)
        }
    }
}
