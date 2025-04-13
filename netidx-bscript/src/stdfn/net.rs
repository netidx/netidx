use crate::{
    arity1, arity2, deftype, errf,
    expr::{Expr, ExprId, ExprKind},
    node::{Node, NodeKind},
    stdfn::CachedVals,
    typ::{FnArgType, FnType, NoRefs, Type},
    Apply, BindId, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use fxhash::FxHashSet;
use netidx::{
    path::Path,
    publisher::{Typ, Val},
    subscriber::{self, Dval, UpdatesFlags, Value},
};
use netidx_core::utils::Either;
use std::{collections::HashSet, sync::Arc};

fn as_path(v: Value) -> Option<Path> {
    match v.cast_to::<String>() {
        Err(_) => None,
        Ok(p) => {
            if Path::is_absolute(&p) {
                Some(Path::from(p))
            } else {
                None
            }
        }
    }
}

struct Write {
    args: CachedVals,
    top_id: ExprId,
    dv: Either<(Path, Dval), Vec<Value>>,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Write {
    const NAME: &str = "write";
    deftype!("fn(string, Any) -> _");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, from, top_id| {
            Ok(Box::new(Write {
                args: CachedVals::new(from),
                dv: Either::Right(vec![]),
                top_id,
            }))
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Write {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        fn set(dv: &mut Either<(Path, Dval), Vec<Value>>, val: &Value) {
            match dv {
                Either::Right(q) => q.push(val.clone()),
                Either::Left((_, dv)) => {
                    dv.write(val.clone());
                }
            }
        }
        let up = self.args.update_diff(ctx, from, event);
        let ((path, value), (path_up, value_up)) = arity2!(self.args.0, up);
        match ((path, value), (path_up, value_up)) {
            ((_, _), (false, false)) => (),
            ((_, Some(val)), (false, true)) => set(&mut self.dv, val),
            ((_, None), (false, true)) => (),
            ((None, Some(val)), (true, true)) => set(&mut self.dv, val),
            ((Some(path), Some(val)), (true, true)) if self.same_path(path) => {
                set(&mut self.dv, val)
            }
            ((Some(path), _), (true, false)) if self.same_path(path) => (),
            ((None, _), (true, false)) => (),
            ((None, None), (_, _)) => (),
            ((Some(path), val), (true, _)) => match as_path(path.clone()) {
                None => {
                    if let Either::Left(_) = &self.dv {
                        self.dv = Either::Right(vec![]);
                    }
                    return errf!("write(path, val): invalid path {path:?}");
                }
                Some(path) => {
                    let dv = ctx.user.subscribe(
                        UpdatesFlags::empty(),
                        path.clone(),
                        self.top_id,
                    );
                    match &mut self.dv {
                        Either::Left(_) => (),
                        Either::Right(q) => {
                            for v in q.drain(..) {
                                dv.write(v);
                            }
                        }
                    }
                    self.dv = Either::Left((path, dv));
                    if let Some(val) = val {
                        set(&mut self.dv, val)
                    }
                }
            },
        }
        None
    }
}

impl Write {
    fn same_path(&self, new_path: &Value) -> bool {
        match (new_path, &self.dv) {
            (Value::String(p0), Either::Left((p1, _))) => &**p0 == &**p1,
            _ => false,
        }
    }
}

struct Subscribe {
    args: CachedVals,
    cur: Option<(Path, Dval)>,
    top_id: ExprId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Subscribe {
    const NAME: &str = "subscribe";
    deftype!("fn(string) -> Any");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, from, top_id| {
            Ok(Box::new(Subscribe { args: CachedVals::new(from), cur: None, top_id }))
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Subscribe {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        let up = self.args.update_diff(ctx, from, event);
        let (path, path_up) = arity1!(self.args.0, up);
        match (path, path_up) {
            (Some(_), false) | (None, false) => (),
            (None, true) => {
                if let Some((path, dv)) = self.cur.take() {
                    ctx.user.unsubscribe(path, dv, self.top_id)
                }
                return None;
            }
            (Some(Value::String(path)), true)
                if self.cur.as_ref().map(|(p, _)| &**p) != Some(&*path) =>
            {
                if let Some((path, dv)) = self.cur.take() {
                    ctx.user.unsubscribe(path, dv, self.top_id)
                }
                let path = Path::from(path);
                if !Path::is_absolute(&path) {
                    return errf!("expected absolute path");
                }
                let dval = ctx.user.subscribe(
                    UpdatesFlags::BEGIN_WITH_LAST,
                    path.clone(),
                    self.top_id,
                );
                self.cur = Some((path, dval));
            }
            (Some(v), true) => return errf!("invalid path {v}, expected string"),
        }
        self.cur.as_ref().and_then(|(_, dv)| {
            event.netidx.get(&dv.id()).map(|e| match e {
                subscriber::Event::Unsubscribed => Value::Error(literal!("unsubscribed")),
                subscriber::Event::Update(v) => v.clone(),
            })
        })
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        if let Some((path, dv)) = self.cur.take() {
            ctx.user.unsubscribe(path, dv, self.top_id)
        }
    }
}

struct RpcCall {
    args: CachedVals,
    top_id: ExprId,
    pending: FxHashSet<BindId>,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for RpcCall {
    const NAME: &str = "call";
    deftype!("fn(string, Array<Array<Any>>) -> Any");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, from, top_id| {
            Ok(Box::new(RpcCall {
                args: CachedVals::new(from),
                top_id,
                pending: HashSet::default(),
            }))
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for RpcCall {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        fn parse_args(
            path: &Value,
            args: &Value,
        ) -> Result<(Path, Vec<(ArcStr, Value)>)> {
            let path = as_path(path.clone()).ok_or_else(|| anyhow!("invalid path"))?;
            let args = match args {
                Value::Array(args) => args
                    .iter()
                    .map(|v| match v {
                        Value::Array(p) => match &**p {
                            [Value::String(name), value] => {
                                Ok((name.clone(), value.clone()))
                            }
                            _ => Err(anyhow!("rpc args expected [name, value] pair")),
                        },
                        _ => Err(anyhow!("rpc args expected [name, value] pair")),
                    })
                    .collect::<Result<Vec<_>>>()?,
                _ => bail!("rpc args expected to be an array"),
            };
            Ok((path, args))
        }
        let up = self.args.update_diff(ctx, from, event);
        let ((path, args), (path_up, args_up)) = arity2!(self.args.0, up);
        match ((path, args), (path_up, args_up)) {
            ((Some(path), Some(args)), (_, true))
            | ((Some(path), Some(args)), (true, _)) => match parse_args(path, args) {
                Err(e) => return errf!("{e}"),
                Ok((path, args)) => {
                    let id = BindId::new();
                    self.pending.insert(id);
                    ctx.user.call_rpc(path, args, self.top_id, id);
                }
            },
            ((None, _), (_, _)) | ((_, None), (_, _)) | ((_, _), (false, false)) => (),
        }
        let mut res = None;
        self.pending.retain(|id| match event.variables.get(id) {
            None => true,
            Some(v) => match res {
                None => {
                    res = Some(v.clone());
                    false
                }
                Some(_) => {
                    // multiple calls resolved simultaneously, defer until the next cycle
                    ctx.user.set_var(*id, v.clone());
                    true
                }
            },
        });
        res
    }
}

macro_rules! list {
    ($name:ident, $builtin:literal, $method:ident, $typ:literal) => {
        struct $name {
            args: CachedVals,
            current: Option<Path>,
            id: BindId,
            top_id: ExprId,
        }

        impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for $name {
            const NAME: &str = $builtin;
            deftype!($typ);

            fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
                Arc::new(|ctx, _, _, from, top_id| {
                    let id = BindId::new();
                    ctx.user.ref_var(id, top_id);
                    Ok(Box::new($name {
                        args: CachedVals::new(from),
                        current: None,
                        id,
                        top_id,
                    }))
                })
            }
        }

        impl<C: Ctx, E: UserEvent> Apply<C, E> for $name {
            fn update(
                &mut self,
                ctx: &mut ExecCtx<C, E>,
                from: &mut [Node<C, E>],
                event: &mut Event<E>,
            ) -> Option<Value> {
                let up = self.args.update_diff(ctx, from, event);
                let ((_, path), (trigger_up, path_up)) = arity2!(self.args.0, up);
                match (path, path_up, trigger_up) {
                    (Some(Value::String(path)), true, _)
                        if self
                            .current
                            .as_ref()
                            .map(|p| &**p != &**path)
                            .unwrap_or(true) =>
                    {
                        let path = Path::from(path);
                        self.current = Some(path.clone());
                        ctx.user.$method(self.id, path);
                    }
                    (Some(Value::String(path)), _, true) => {
                        ctx.user.$method(self.id, Path::from(path));
                    }
                    _ => (),
                }
                event.variables.get(&self.id).and_then(|v| match v {
                    Value::Null => None,
                    v => Some(v.clone()),
                })
            }

            fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
                ctx.user.unref_var(self.id, self.top_id);
                ctx.user.stop_list(self.id);
            }
        }
    };
}

list!(List, "list", list, "fn(?#update:Any, string) -> Array<string>");
list!(ListTable, "list_table", list_table, "fn(?#update:Any, string) -> Table");

struct Publish<C: Ctx, E: UserEvent> {
    args: CachedVals,
    current: Option<(Path, Val)>,
    top_id: ExprId,
    typ: FnType<NoRefs>,
    x: BindId,
    on_write: Option<Box<dyn Apply<C, E> + Send + Sync>>,
    from: [Node<C, E>; 1],
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Publish<C, E> {
    const NAME: &str = "publish";
    deftype!("fn(?#on_write:[null, fn(Any) -> Any], string, Any) -> error");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, typ, _, from, top_id| {
            let x = BindId::new();
            let mut args = [Node {
                spec: Box::new(ExprKind::Ref { name: ["x"].into() }.to_expr()),
                typ: from[0].typ.clone(),
                kind: NodeKind::Ref { id: x, top_id },
            }];
            match from {
                [Node { spec: _, typ: _, kind: NodeKind::Lambda(lb) }, _, _] => {
                    Ok(Box::new(Publish {
                        args: CachedVals::new(from),
                        current: None,
                        typ: typ.clone(),
                        top_id,
                        x,
                        on_write: Some((lb.init)(ctx, &mut args, top_id)?),
                        from: args,
                    }))
                }
                [Node { spec: _, typ: _, kind: NodeKind::Constant(Value::Null) }, _, _] => {
                    Ok(Box::new(Publish {
                        args: CachedVals::new(from),
                        current: None,
                        typ: typ.clone(),
                        top_id,
                        x: BindId::new(),
                        on_write: None,
                        from: args,
                    }))
                }
                _ => bail!("expected function"),
            }
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Publish<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        let up = self.args.update_diff(ctx, from, event);
        match (&up[1..], &self.args.0[1..]) {
            ([true, _], [Some(Value::String(path)), Some(v)])
                if self.current.as_ref().map(|(p, _)| &**p != path).unwrap_or(true) =>
            {
                if let Some((_, id)) = self.current.take() {
                    ctx.user.unpublish(id, self.top_id);
                }
                let path = Path::from(path.clone());
                match ctx.user.publish(path.clone(), v.clone(), self.top_id) {
                    Err(e) => return Some(Value::Error(format!("{e:?}").into())),
                    Ok(id) => {
                        self.current = Some((path, id));
                    }
                }
            }
            ([_, true], [_, Some(v)]) => {
                if let Some((_, val)) = &self.current {
                    ctx.user.update(val, v.clone())
                }
            }
            _ => (),
        }
        let mut reply = None;
        if let Some((_, val)) = &self.current {
            if let Some(req) = event.writes.remove(&val.id()) {
                event.variables.insert(self.x, req.value);
                reply = req.send_result;
            }
        }
        if let Some(on_write) = &mut self.on_write {
            if let Some(v) = on_write.update(ctx, &mut self.from, event) {
                if let Some(reply) = reply {
                    reply.send(v)
                }
            }
        }
        None
    }

    fn typecheck(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
    ) -> anyhow::Result<()> {
        for n in from.iter_mut() {
            n.typecheck(ctx)?;
        }
        Type::Primitive(Typ::String.into()).check_contains(&from[1].typ)?;
        Type::Primitive(Typ::any()).check_contains(&from[2].typ)?;
        match &mut self.on_write {
            None => Type::Primitive(Typ::Null.into()).check_contains(&from[0].typ)?,
            Some(app) => match self.typ.args.get(0) {
                Some(FnArgType { label: _, typ: Type::Set(s) }) if s.len() == 2 => {
                    match &s[1] {
                        Type::Fn(ft) => {
                            ft.check_contains(&app.typ())?;
                            app.typecheck(ctx, &mut self.from)?
                        }
                        _ => bail!("expected function as 1st arg"),
                    }
                }
                _ => bail!("expected function as 1st arg"),
            },
        }
        Ok(())
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        if let Some((_, val)) = self.current.take() {
            ctx.user.unpublish(val, self.top_id);
        }
        if let Some(mut app) = self.on_write.take() {
            app.delete(ctx);
        }
    }
}

const MOD: &str = r#"
pub mod net {
    type Table = { rows: Array<string>, columns: Array<(string, v64)> }

    pub let write = |path, value| 'write
    pub let subscribe = |path| 'subscribe
    pub let call = |path, args| 'call
    pub let list = |#update = time::timer(1, true), path| 'list
    pub let list_table = |#update = time::timer(1, true), path| 'list_table
    pub let publish = |#on_write = null, path, v| 'publish
}
"#;

pub fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<Write>().unwrap();
    ctx.register_builtin::<Subscribe>().unwrap();
    ctx.register_builtin::<RpcCall>().unwrap();
    ctx.register_builtin::<List>().unwrap();
    ctx.register_builtin::<ListTable>().unwrap();
    ctx.register_builtin::<Publish<C, E>>().unwrap();
    MOD.parse().unwrap()
}
