#[cfg(test)]
use crate::run;
use crate::{
    deftype,
    expr::ExprKind,
    node::{Node, NodeKind},
    stdfn::{CachedArgs, CachedVals, EvalCached},
    typ::{FnArgType, FnType, NoRefs, Type},
    Apply, BindId, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, UserEvent,
};
use anyhow::bail;
#[cfg(test)]
use anyhow::Result;
use fxhash::FxHashMap;
use netidx::{publisher::Typ, subscriber::Value};
use netidx_netproto::valarray::ValArray;
use smallvec::{smallvec, SmallVec};
use std::{mem, sync::Arc};

macro_rules! mapfn {
    ($name:ident, $bname:literal, $typ:literal, $buf:literal) => {
        pub(super) struct $name<C: Ctx, E: UserEvent> {
            pred: Box<dyn Apply<C, E> + Send + Sync>,
            x: BindId,
            typ: FnType<NoRefs>,
            from: [Node<C, E>; 1],
            buf: SmallVec<[Value; $buf]>,
        }

        impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for $name<C, E> {
            const NAME: &str = $bname;
            deftype!($typ);

            fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
                Arc::new(|ctx, typ, scope, from, top_id| match from {
                    [_, Node { spec: _, typ: _, kind: NodeKind::Lambda(lb) }] => {
                        let x = ctx.env.bind_variable(scope, "x", from[0].typ.clone()).id;
                        ctx.user.ref_var(x, top_id);
                        let mut from = [Node {
                            spec: Box::new(
                                ExprKind::Ref { name: ["x"].into() }.to_expr(),
                            ),
                            typ: Type::empty_tvar(),
                            kind: NodeKind::Ref(x),
                        }];
                        let pred = (lb.init)(ctx, &mut from, top_id)?;
                        Ok(Box::new(Self {
                            pred,
                            x,
                            from,
                            typ: typ.clone(),
                            buf: smallvec![],
                        }))
                    }
                    _ => bail!("expected a function"),
                })
            }
        }

        impl<C: Ctx, E: UserEvent> Apply<C, E> for $name<C, E> {
            fn update(
                &mut self,
                ctx: &mut ExecCtx<C, E>,
                from: &mut [Node<C, E>],
                event: &mut Event<E>,
            ) -> Option<Value> {
                match from[0].update(ctx, event) {
                    Some(Value::Array(a)) if a.len() == 0 => {
                        self.predicate(ctx, false, event);
                        self.finish()
                    }
                    Some(Value::Array(a)) => {
                        let mut variables = FxHashMap::default();
                        for (i, v) in a.iter().enumerate() {
                            event.variables.insert(self.x, v.clone());
                            self.predicate(ctx, true, event);
                            if i == 0 {
                                variables = mem::take(&mut event.variables);
                            }
                        }
                        event.variables = variables;
                        self.finish()
                    }
                    Some(_) | None => {
                        self.predicate(ctx, false, event);
                        None
                    }
                }
            }

            fn typecheck(
                &mut self,
                ctx: &mut ExecCtx<C, E>,
                from: &mut [Node<C, E>],
            ) -> anyhow::Result<()> {
                for n in from.iter_mut() {
                    n.typecheck(ctx)?;
                }
                let et = self.from[0].typ.clone();
                Type::Array(triomphe::Arc::new(et)).check_contains(&from[0].typ)?;
                for n in self.from.iter_mut() {
                    n.typecheck(ctx)?;
                }
                self.pred.typecheck(ctx, &mut self.from[..])?;
                match self.typ.args.get(1) {
                    Some(FnArgType { label: _, typ: Type::Fn(ft) }) => {
                        ft.check_contains(&self.pred.typ())?
                    }
                    _ => bail!("expected function as 2nd arg"),
                }
                Ok(())
            }
        }
    };
}

mapfn!(Filter, "array_filter", "fn(Array<'a>, fn('a) -> bool) -> Array<'a>", 32);

impl<C: Ctx, E: UserEvent> Filter<C, E> {
    fn predicate(&mut self, ctx: &mut ExecCtx<C, E>, set: bool, event: &mut Event<E>) {
        match self.pred.update(ctx, &mut self.from, event) {
            Some(Value::Bool(true)) if set => {
                self.buf.push(event.variables[&self.x].clone())
            }
            _ => (),
        }
    }

    fn finish(&mut self) -> Option<Value> {
        let a = ValArray::from_iter(self.buf.drain(..));
        Some(Value::Array(a))
    }
}

#[cfg(test)]
const ARRAY_FILTER: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8];
  array::filter(a, |x| x > 3)
}
"#;

#[cfg(test)]
run!(array_filter, ARRAY_FILTER, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(4), Value::I64(5), Value::I64(6), Value::I64(7), Value::I64(8)] => {
                true
            }
            _ => false,
        },
        _ => false,
    }
});

mapfn!(Map, "array_map", "fn(Array<'a>, fn('a) -> 'b) -> Array<'b>", 32);

impl<C: Ctx, E: UserEvent> Map<C, E> {
    fn predicate(&mut self, ctx: &mut ExecCtx<C, E>, set: bool, event: &mut Event<E>) {
        match self.pred.update(ctx, &mut self.from, event) {
            Some(v) if set => self.buf.push(v),
            _ => (),
        }
    }

    fn finish(&mut self) -> Option<Value> {
        let a = ValArray::from_iter(self.buf.drain(..));
        Some(Value::Array(a))
    }
}

#[cfg(test)]
const ARRAY_MAP: &str = r#"
{
  let a = [1, 2, 3, 4];
  array::map(a, |x| x > 3)
}
"#;

#[cfg(test)]
run!(array_map, ARRAY_MAP, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::Bool(false), Value::Bool(false), Value::Bool(false), Value::Bool(true)] => {
                true
            }
            _ => false,
        },
        _ => false,
    }
});

mapfn!(FlatMap, "array_flat_map", "fn(Array<'a>, fn('a) -> Array<'b>) -> Array<'b>", 32);

impl<C: Ctx, E: UserEvent> FlatMap<C, E> {
    fn predicate(&mut self, ctx: &mut ExecCtx<C, E>, set: bool, event: &mut Event<E>) {
        match self.pred.update(ctx, &mut self.from, event) {
            Some(Value::Array(a)) if set => self.buf.extend(a.iter().map(|v| v.clone())),
            Some(v) if set => self.buf.push(v),
            _ => (),
        }
    }

    fn finish(&mut self) -> Option<Value> {
        let a = ValArray::from_iter(self.buf.drain(..));
        Some(Value::Array(a))
    }
}

#[cfg(test)]
const ARRAY_FLAT_MAP: &str = r#"
{
  let a = [1, 2];
  array::flat_map(a, |x| [x, x + 1])
}
"#;

#[cfg(test)]
run!(array_flat_map, ARRAY_FLAT_MAP, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(2), Value::I64(2), Value::I64(3)] => true,
            _ => false,
        },
        _ => false,
    }
});

mapfn!(
    FilterMap,
    "array_filter_map",
    "fn(Array<'a>, fn('a) -> ['b, null]) -> Array<'b>",
    32
);

impl<C: Ctx, E: UserEvent> FilterMap<C, E> {
    fn predicate(&mut self, ctx: &mut ExecCtx<C, E>, set: bool, event: &mut Event<E>) {
        match self.pred.update(ctx, &mut self.from, event) {
            Some(Value::Null) => (),
            Some(v) if set => self.buf.push(v),
            _ => (),
        }
    }

    fn finish(&mut self) -> Option<Value> {
        let a = ValArray::from_iter(self.buf.drain(..));
        Some(Value::Array(a))
    }
}

#[cfg(test)]
const ARRAY_FILTER_MAP: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8];
  array::filter_map(a, |x| select x > 5 {
    true => x + 1,
    false => sample(x, null)
  })
}
"#;

#[cfg(test)]
run!(array_filter_map, ARRAY_FILTER_MAP, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(7), Value::I64(8), Value::I64(9)] => true,
            _ => false,
        },
        _ => false,
    }
});

mapfn!(Find, "array_find", "fn(Array<'a>, fn('a) -> bool) -> ['a, null]", 1);

impl<C: Ctx, E: UserEvent> Find<C, E> {
    fn predicate(&mut self, ctx: &mut ExecCtx<C, E>, set: bool, event: &mut Event<E>) {
        match self.pred.update(ctx, &mut self.from, event) {
            Some(Value::Bool(true)) if set && self.buf.is_empty() => {
                self.buf.push(event.variables[&self.x].clone())
            }
            _ => (),
        }
    }

    fn finish(&mut self) -> Option<Value> {
        Some(self.buf.pop().unwrap_or(Value::Null))
    }
}

#[cfg(test)]
const ARRAY_FIND: &str = r#"
{
  type T = (string, i64);
  let a: Array<T> = [("foo", 1), ("bar", 2), ("baz", 3)];
  array::find(a, |(k, _): T| k == "bar")
}
"#;

#[cfg(test)]
run!(array_find, ARRAY_FIND, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s), Value::I64(2)] => &**s == "bar",
            _ => false,
        },
        _ => false,
    }
});

mapfn!(FindMap, "array_find_map", "fn(Array<'a>, fn('a) -> ['b, null]) -> ['b, null]", 1);

impl<C: Ctx, E: UserEvent> FindMap<C, E> {
    fn predicate(&mut self, ctx: &mut ExecCtx<C, E>, set: bool, event: &mut Event<E>) {
        match self.pred.update(ctx, &mut self.from, event) {
            Some(Value::Null) => (),
            Some(v) if set && self.buf.is_empty() => self.buf.push(v),
            _ => (),
        }
    }

    fn finish(&mut self) -> Option<Value> {
        Some(self.buf.pop().unwrap_or(Value::Null))
    }
}

#[cfg(test)]
const ARRAY_FIND_MAP: &str = r#"
{
  type T = (string, i64);
  let a: Array<T> = [("foo", 1), ("bar", 2), ("baz", 3)];
  array::find_map(a, |(k, v): T| select k == "bar" {
    true => v,
    false => sample(v, null)
  })
}
"#;

#[cfg(test)]
run!(array_find_map, ARRAY_FIND_MAP, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(2)) => true,
        _ => false,
    }
});

pub(super) struct Fold<C: Ctx, E: UserEvent> {
    pred: Box<dyn Apply<C, E> + Send + Sync>,
    x: BindId,
    y: BindId,
    typ: FnType<NoRefs>,
    from: [Node<C, E>; 2],
    cached: Option<ValArray>,
    init: Option<Value>,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Fold<C, E> {
    const NAME: &str = "array_fold";
    deftype!("fn(Array<'a>, 'b, fn('b, 'a) -> 'b) -> 'b");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, typ, scope, from, top_id| match from {
            [_, _, Node { spec: _, typ: _, kind: NodeKind::Lambda(lb) }] => {
                let x = ctx.env.bind_variable(scope, "x", from[0].typ.clone()).id;
                let y = ctx.env.bind_variable(scope, "y", from[1].typ.clone()).id;
                ctx.user.ref_var(x, top_id);
                ctx.user.ref_var(y, top_id);
                let mut from = [
                    Node {
                        spec: Box::new(ExprKind::Ref { name: ["x"].into() }.to_expr()),
                        typ: Type::empty_tvar(),
                        kind: NodeKind::Ref(x),
                    },
                    Node {
                        spec: Box::new(ExprKind::Ref { name: ["y"].into() }.to_expr()),
                        typ: Type::empty_tvar(),
                        kind: NodeKind::Ref(y),
                    },
                ];
                let pred = (lb.init)(ctx, &mut from, top_id)?;
                Ok(Box::new(Self {
                    pred,
                    x,
                    y,
                    from,
                    typ: typ.clone(),
                    cached: None,
                    init: None,
                }))
            }
            _ => bail!("expected a function"),
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Fold<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        if let Some(Value::Array(a)) = from[0].update(ctx, event) {
            self.cached = Some(a);
        }
        if let Some(init) = from[1].update(ctx, event) {
            self.init = Some(init)
        }
        match (&self.cached, &self.init) {
            (Some(a), Some(init)) if a.len() == 0 => {
                let _ = self.pred.update(ctx, &mut self.from, event);
                Some(init.clone())
            }
            (Some(a), Some(init)) => {
                let mut acc = init.clone();
                let mut variables = FxHashMap::default();
                for (i, v) in a.iter().enumerate() {
                    event.variables.insert(self.x, acc.clone());
                    event.variables.insert(self.y, v.clone());
                    if let Some(v) = self.pred.update(ctx, &mut self.from, event) {
                        acc = v;
                    }
                    if i == 0 {
                        variables = mem::take(&mut event.variables);
                    }
                }
                event.variables = variables;
                Some(acc)
            }
            (None, _) | (_, None) => {
                let _ = self.pred.update(ctx, &mut self.from, event);
                None
            }
        }
    }

    fn typecheck(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
    ) -> anyhow::Result<()> {
        for n in from.iter_mut() {
            n.typecheck(ctx)?;
        }
        let et = self.from[1].typ.clone();
        Type::Array(triomphe::Arc::new(et)).check_contains(&from[0].typ)?;
        self.from[0].typ.check_contains(&from[1].typ)?;
        for n in self.from.iter_mut() {
            n.typecheck(ctx)?;
        }
        self.pred.typecheck(ctx, &mut self.from[..])?;
        match self.typ.args.get(2) {
            Some(FnArgType { label: _, typ: Type::Fn(ft) }) => {
                ft.check_contains(&self.pred.typ())?
            }
            _ => bail!("expected function as 3rd arg"),
        }
        Ok(())
    }
}

#[cfg(test)]
const ARRAY_FOLD: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
  array::fold(a, 0, |acc, x| x + acc)
}
"#;

#[cfg(test)]
run!(array_fold, ARRAY_FOLD, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(55)) => true,
        _ => false,
    }
});

#[derive(Default)]
pub(super) struct ConcatEv(SmallVec<[Value; 32]>);

impl EvalCached for ConcatEv {
    const NAME: &str = "array_concat";
    deftype!("fn(Array<'a>, @args: Array<'a>) -> Array<'a>");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        let mut present = true;
        for v in from.0.iter() {
            match v {
                Some(Value::Array(a)) => {
                    for v in a.iter() {
                        self.0.push(v.clone())
                    }
                }
                Some(v) => self.0.push(v.clone()),
                None => present = false,
            }
        }
        if present {
            let a = ValArray::from_iter_exact(self.0.drain(..));
            Some(Value::Array(a))
        } else {
            None
        }
    }
}

pub(super) type Concat = CachedArgs<ConcatEv>;

#[cfg(test)]
const ARRAY_CONCAT: &str = r#"
{
  array::concat([1, 2, 3], [4, 5], [6])
}
"#;

#[cfg(test)]
run!(array_concat, ARRAY_CONCAT, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(2), Value::I64(3), Value::I64(4), Value::I64(5), Value::I64(6)] => {
                true
            }
            _ => false,
        },
        _ => false,
    }
});

#[derive(Default)]
pub(super) struct LenEv;

impl EvalCached for LenEv {
    const NAME: &str = "array_len";
    deftype!("fn(Array<'a>) -> u64");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::Array(a)) => Some(Value::U64(a.len() as u64)),
            Some(_) | None => None,
        }
    }
}

pub(super) type Len = CachedArgs<LenEv>;

#[cfg(test)]
const ARRAY_LEN: &str = r#"
{
  use core::array;
  len(concat([1, 2, 3], [4, 5], [6]))
}
"#;

#[cfg(test)]
run!(array_len, ARRAY_LEN, |v: Result<&Value>| {
    match v {
        Ok(Value::U64(6)) => true,
        _ => false,
    }
});

#[derive(Default)]
pub(super) struct FlattenEv(SmallVec<[Value; 32]>);

impl EvalCached for FlattenEv {
    const NAME: &str = "array_flatten";
    deftype!("fn(Array<Array<'a>>) -> Array<'a>");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::Array(a)) => {
                for v in a.iter() {
                    match v {
                        Value::Array(a) => self.0.extend(a.iter().map(|v| v.clone())),
                        v => self.0.push(v.clone()),
                    }
                }
                let a = ValArray::from_iter_exact(self.0.drain(..));
                Some(Value::Array(a))
            }
            Some(_) | None => None,
        }
    }
}

pub(super) type Flatten = CachedArgs<FlattenEv>;

#[cfg(test)]
const ARRAY_FLATTEN: &str = r#"
{
  array::flatten([[1, 2, 3], [4, 5], [6]])
}
"#;

#[cfg(test)]
run!(array_flatten, ARRAY_FLATTEN, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(2), Value::I64(3), Value::I64(4), Value::I64(5), Value::I64(6)] => {
                true
            }
            _ => false,
        },
        _ => false,
    }
});

pub(super) struct Group<C: Ctx, E: UserEvent> {
    buf: SmallVec<[Value; 16]>,
    pred: Box<dyn Apply<C, E> + Send + Sync>,
    typ: FnType<NoRefs>,
    n: BindId,
    x: BindId,
    from: [Node<C, E>; 2],
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Group<C, E> {
    const NAME: &str = "group";
    deftype!("fn('a, fn(u64, 'a) -> bool) -> Array<'a>");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, typ, scope, from, top_id| match from {
            [_, Node { spec: _, typ: _, kind: NodeKind::Lambda(lb) }] => {
                let n_typ = Type::Primitive(Typ::U64.into());
                let n = ctx.env.bind_variable(scope, "n", n_typ.clone()).id;
                let x = ctx.env.bind_variable(scope, "x", from[0].typ.clone()).id;
                ctx.user.ref_var(n, top_id);
                ctx.user.ref_var(x, top_id);
                let mut from = [
                    Node {
                        spec: Box::new(ExprKind::Ref { name: ["n"].into() }.to_expr()),
                        typ: n_typ,
                        kind: NodeKind::Ref(n),
                    },
                    Node {
                        spec: Box::new(ExprKind::Ref { name: ["x"].into() }.to_expr()),
                        typ: from[0].typ.clone(),
                        kind: NodeKind::Ref(x),
                    },
                ];
                let pred = (lb.init)(ctx, &mut from, top_id)?;
                Ok(Box::new(Self {
                    buf: smallvec![],
                    typ: typ.clone(),
                    pred,
                    n,
                    x,
                    from,
                }))
            }
            _ => bail!("expected a function"),
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Group<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        if let Some(val) = from[0].update(ctx, event) {
            self.buf.push(val.clone());
            event.variables.insert(self.n, self.buf.len().into());
            event.variables.insert(self.x, val);
        }
        match self.pred.update(ctx, &mut self.from, event) {
            Some(Value::Bool(true)) => {
                Some(Value::Array(ValArray::from_iter_exact(self.buf.drain(..))))
            }
            _ => None,
        }
    }

    fn typecheck(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
    ) -> anyhow::Result<()> {
        for n in from.iter_mut() {
            n.typecheck(ctx)?
        }
        self.from[0].typ.check_contains(&Type::Primitive(Typ::U64.into()))?;
        self.from[1].typ.check_contains(&from[0].typ)?;
        for n in self.from.iter_mut() {
            n.typecheck(ctx)?
        }
        self.pred.typecheck(ctx, &mut self.from[..])?;
        match self.typ.args.get(1) {
            Some(FnArgType { label: _, typ: Type::Fn(ft) }) => {
                ft.check_contains(&self.pred.typ())?
            }
            _ => bail!("expected function as 2nd arg"),
        }
        Ok(())
    }
}

#[cfg(test)]
const GROUP: &str = r#"
{
   let x = 1;
   let y = x + 1;
   let z = y + 1;
   array::group(any(x, y, z), |_, v| v == 3)
}
"#;

#[cfg(test)]
run!(group, GROUP, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(1), Value::I64(2), Value::I64(3)] => true,
            _ => false,
        },
        _ => false,
    }
});

pub(super) struct Iter(BindId);

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Iter {
    const NAME: &str = "iter";
    deftype!("fn(Array<'a>) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(Iter(BindId::new()))))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Iter {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        if let Some(Value::Array(a)) = from[0].update(ctx, event) {
            for v in a.iter() {
                ctx.user.set_var(self.0, v.clone());
            }
        }
        event.variables.get(&self.0).map(|v| v.clone())
    }
}

#[cfg(test)]
const ITER: &str = r#"
{
   filter(array::iter([1, 2, 3, 4]), |x| x == 4)
}
"#;

#[cfg(test)]
run!(iter, ITER, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(4)) => true,
        _ => false,
    }
});
