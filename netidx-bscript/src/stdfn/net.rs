use crate::{
    arity1, arity2, deftype, errf,
    expr::{Expr, ExprId, ModPath},
    node::{genn, Node},
    stdfn::CachedVals,
    typ::{FnType, NoRefs, Type},
    Apply, BindId, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, LambdaId, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use netidx::{
    path::Path,
    publisher::{Typ, Val},
    subscriber::{self, Dval, UpdatesFlags, Value},
};
use netidx_core::utils::Either;
use netidx_netproto::valarray::ValArray;
use netidx_protocols::rpc::server::{self, ArgSpec};
use smallvec::{smallvec, SmallVec};
use std::{collections::VecDeque, mem, sync::Arc};
use triomphe::Arc as TArc;

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
    id: BindId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for RpcCall {
    const NAME: &str = "call";
    deftype!("fn(string, Array<(string, Any)>) -> Any");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, _, _, from, top_id| {
            let id = BindId::new();
            ctx.user.ref_var(id, top_id);
            Ok(Box::new(RpcCall { args: CachedVals::new(from), top_id, id }))
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
                Ok((path, args)) => ctx.user.call_rpc(path, args, self.id),
            },
            ((None, _), (_, _)) | ((_, None), (_, _)) | ((_, _), (false, false)) => (),
        }
        event.variables.get(&self.id).cloned()
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.id, self.top_id)
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
    mftyp: TArc<FnType<NoRefs>>,
    x: BindId,
    pid: BindId,
    on_write: Node<C, E>,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Publish<C, E> {
    const NAME: &str = "publish";
    deftype!("fn(?#on_write:fn(Any) -> _, string, Any) -> error");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, typ, scope, from, top_id| match from {
            [_, _, _] => {
                let scope = ModPath(
                    scope.append(format_compact!("fn{}", LambdaId::new().0).as_str()),
                );
                let pid = BindId::new();
                let mftyp = match &typ.args[0].typ {
                    Type::Fn(ft) => ft.clone(),
                    t => bail!("expected function not {t}"),
                };
                let (x, xn) =
                    genn::bind(ctx, &scope, "x", Type::Primitive(Typ::any()), top_id);
                let fnode = genn::reference(ctx, pid, Type::Fn(mftyp.clone()), top_id);
                let on_write = genn::apply(fnode, vec![xn], mftyp.clone(), top_id);
                Ok(Box::new(Publish {
                    args: CachedVals::new(from),
                    current: None,
                    mftyp,
                    top_id,
                    pid,
                    x,
                    on_write,
                }))
            }
            _ => bail!("expected three arguments"),
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
        if up[0] {
            if let Some(v) = self.args.0[0].clone() {
                event.variables.insert(self.pid, v);
            }
        }
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
        if let Some(v) = self.on_write.update(ctx, event) {
            if let Some(reply) = reply {
                reply.send(v)
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
        Type::Fn(self.mftyp.clone()).check_contains(&from[0].typ)?;
        Type::Primitive(Typ::String.into()).check_contains(&from[1].typ)?;
        Type::Primitive(Typ::any()).check_contains(&from[2].typ)?;
        self.on_write.typecheck(ctx)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        if let Some((_, val)) = self.current.take() {
            ctx.user.unpublish(val, self.top_id);
        }
        mem::take(&mut self.on_write).delete(ctx)
    }
}

struct PublishRpc<C: Ctx, E: UserEvent> {
    args: CachedVals,
    id: BindId,
    top_id: ExprId,
    f: Node<C, E>,
    pid: BindId,
    x: BindId,
    typ: TArc<FnType<NoRefs>>,
    queue: VecDeque<server::RpcCall>,
    argbuf: SmallVec<[(ArcStr, Value); 6]>,
    ready: bool,
    current: Option<Path>,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for PublishRpc<C, E> {
    const NAME: &str = "publish_rpc";
    deftype!(
        "fn(#path:string, #doc:string, #spec:Array<ArgSpec>, #f:fn(Array<(string, Any)>) -> Any) -> _"
    );

    fn init(_ctx: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, typ, scope, from, top_id| match from {
            [_, _, _, _] => {
                let scope = ModPath(
                    scope.append(format_compact!("fn{}", LambdaId::new().0).as_str()),
                );
                let id = BindId::new();
                ctx.user.ref_var(id, top_id);
                let pid = BindId::new();
                let mftyp = match &typ.args[3].typ {
                    Type::Fn(ft) => ft.clone(),
                    t => bail!("expected a function not {t}"),
                };
                let (x, xn) =
                    genn::bind(ctx, &scope, "x", mftyp.args[0].typ.clone(), top_id);
                let fnode = genn::reference(ctx, pid, Type::Fn(mftyp.clone()), top_id);
                let f = genn::apply(fnode, vec![xn], mftyp, top_id);
                Ok(Box::new(PublishRpc {
                    queue: VecDeque::new(),
                    args: CachedVals::new(from),
                    x,
                    id,
                    top_id,
                    f,
                    pid,
                    typ: TArc::new(typ.clone()),
                    argbuf: smallvec![],
                    ready: true,
                    current: None,
                }))
            }
            _ => bail!("expected two arguments"),
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for PublishRpc<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        macro_rules! snd {
            ($pair:expr) => {
                match $pair {
                    Value::Array(p) => p[1].clone(),
                    _ => unreachable!(),
                }
            };
        }
        let changed = self.args.update_diff(ctx, from, event);
        if changed[3] {
            if let Some(v) = self.args.0[3].clone() {
                event.variables.insert(self.pid, v);
            }
        }
        if changed[0] || changed[1] || changed[2] {
            if let Some(path) = self.current.take() {
                ctx.user.unpublish_rpc(path);
            }
            if let (Some(Value::String(path)), Some(doc), Some(Value::Array(spec))) =
                (&self.args.0[0], &self.args.0[1], &self.args.0[2])
            {
                let path = Path::from(path);
                let spec = spec
                    .iter()
                    .map(|r| match r {
                        Value::Array(r) => {
                            let default_value = snd!(&r[0]);
                            let doc = snd!(&r[1]);
                            let name = snd!(&r[2]).get_as::<ArcStr>().unwrap();
                            ArgSpec { name, doc, default_value }
                        }
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();
                if let Err(e) =
                    ctx.user.publish_rpc(path.clone(), doc.clone(), spec, self.id)
                {
                    let e = Value::Error(format_compact!("{e:?}").as_str().into());
                    return Some(e);
                }
                self.current = Some(path);
            }
        }
        macro_rules! set {
            ($c:expr) => {{
                self.ready = false;
                self.argbuf.extend($c.args.iter().map(|(n, v)| (n.clone(), v.clone())));
                self.argbuf.sort_by_key(|(n, _)| n.clone());
                let args =
                    ValArray::from_iter_exact(self.argbuf.drain(..).map(|(n, v)| {
                        Value::Array(ValArray::from([Value::String(n), v]))
                    }));
                ctx.cached.insert(self.x, Value::Array(args.clone()));
                event.variables.insert(self.x, Value::Array(args));
            }};
        }
        if let Some(c) = event.rpc_calls.remove(&self.id) {
            self.queue.push_back(c);
        }
        if self.ready && self.queue.len() > 0 {
            if let Some(c) = self.queue.front() {
                set!(c)
            }
        }
        loop {
            match self.f.update(ctx, event) {
                None => break None,
                Some(v) => {
                    self.ready = true;
                    if let Some(mut call) = self.queue.pop_front() {
                        call.reply.send(v);
                    }
                    match self.queue.front() {
                        Some(c) => set!(c),
                        None => break None,
                    }
                }
            }
        }
    }

    fn typecheck(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
    ) -> Result<()> {
        for n in from.iter_mut() {
            n.typecheck(ctx)?;
        }
        self.typ.args[0].typ.check_contains(&from[0].typ)?;
        self.typ.args[1].typ.check_contains(&from[1].typ)?;
        self.typ.args[2].typ.check_contains(&from[2].typ)?;
        self.typ.args[3].typ.check_contains(&from[3].typ)?;
        self.f.typecheck(ctx)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.id, self.top_id);
        if let Some(path) = self.current.take() {
            ctx.user.unpublish_rpc(path);
        }
        ctx.cached.remove(&self.x);
        mem::take(&mut self.f).delete(ctx);
    }
}

const MOD: &str = r#"
pub mod net {
    type Table = { rows: Array<string>, columns: Array<(string, v64)> };
    type ArgSpec = { name: string, doc: string, default: Any };

    pub let write = |path, value| 'write;
    pub let subscribe = |path| 'subscribe;
    pub let call = |path, args| 'call;
    pub let rpc = |#path, #doc, #spec, #f| 'publish_rpc;
    pub let list = |#update = time::timer(1, true), path| 'list;
    pub let list_table = |#update = time::timer(1, true), path| 'list_table;
    pub let publish = |#on_write = |v| never(v), path, v| 'publish
}
"#;

pub fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<Write>().unwrap();
    ctx.register_builtin::<Subscribe>().unwrap();
    ctx.register_builtin::<RpcCall>().unwrap();
    ctx.register_builtin::<List>().unwrap();
    ctx.register_builtin::<ListTable>().unwrap();
    ctx.register_builtin::<Publish<C, E>>().unwrap();
    ctx.register_builtin::<PublishRpc<C, E>>().unwrap();
    MOD.parse().unwrap()
}
