use crate::{
    arity1, arity2, deftype, errf,
    expr::{Expr, ExprId},
    node::Node,
    stdfn::CachedVals,
    Apply, BindId, BuiltIn, Ctx, Event, ExecCtx, InitFn, UserEvent,
};
use anyhow::{anyhow, bail, Result};
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use fxhash::FxHashSet;
use netidx::{
    path::Path,
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

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, from, top_id| {
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
                    let dv = ctx.user.durable_subscribe(
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

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, from, top_id| {
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
            (Some(path), true) => {
                if let Some((path, dv)) = self.cur.take() {
                    ctx.user.unsubscribe(path, dv, self.top_id)
                }
                match as_path(path.clone()) {
                    None => {
                        return errf!("subscribe(path): invalid absolute path {path:?}")
                    }
                    Some(path) => {
                        self.cur = Some((
                            path.clone(),
                            ctx.user.durable_subscribe(
                                UpdatesFlags::BEGIN_WITH_LAST,
                                path,
                                self.top_id,
                            ),
                        ));
                    }
                }
            }
        }
        self.cur.as_ref().and_then(|(_, dv)| {
            event.netidx.get(&dv.id()).map(|e| match e {
                subscriber::Event::Unsubscribed => Value::Error(literal!("unsubscribed")),
                subscriber::Event::Update(v) => v.clone(),
            })
        })
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

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, from, top_id| {
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

const MOD: &str = r#"
pub mod net {
    pub let write = |path, value| 'write
    pub let subscribe = |path| 'subscribe
    pub let call = |path, args| 'call
}
"#;

pub fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<Write>().unwrap();
    ctx.register_builtin::<Subscribe>().unwrap();
    ctx.register_builtin::<RpcCall>().unwrap();
    MOD.parse().unwrap()
}
