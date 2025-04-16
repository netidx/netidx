#[cfg(test)]
use crate::run;
use crate::{
    deftype,
    expr::{ExprId, ExprKind, ModPath},
    node::{gen, Node, NodeKind},
    stdfn::{CachedArgs, CachedVals, EvalCached},
    typ::{FnArgType, FnType, NoRefs, Refs, Type},
    Apply, BindId, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, UserEvent,
};
#[cfg(test)]
use anyhow::Result;
use anyhow::{anyhow, bail};
use fxhash::FxHashMap;
use netidx::{publisher::Typ, subscriber::Value};
use netidx_netproto::valarray::ValArray;
use smallvec::{smallvec, SmallVec};
use std::{
    marker::PhantomData,
    mem,
    sync::{Arc, LazyLock},
};
use triomphe::Arc as TArc;

pub trait MapFn<C: Ctx, E: UserEvent>: Default + Send + Sync + 'static {
    const NAME: &str;
    const TYP: LazyLock<FnType<Refs>>;

    fn predicate(&mut self, out: &Value) -> Option<Value>;
    fn finish(&mut self, buf: &mut SmallVec<[Slot<C, E>; 32]>) -> Option<Value>;
}

struct Slot<C: Ctx, E: UserEvent> {
    id: BindId,
    pred: Node<C, E>,
    out: Option<Value>,
    buf: Option<Value>,
}

pub(super) struct MapQ<C: Ctx, E: UserEvent, T: MapFn<C, E>> {
    scope: ModPath,
    predid: BindId,
    top_id: ExprId,
    ftyp: TArc<FnType<NoRefs>>,
    etyp: Type<NoRefs>,
    mftyp: TArc<FnType<NoRefs>>,
    slots: SmallVec<[Slot<C, E>; 32]>,
    cur: ValArray,
    t: T,
}

impl<C: Ctx, E: UserEvent, T: MapFn<C, E>> BuiltIn<C, E> for MapQ<C, E, T> {
    const NAME: &str = T::NAME;
    const TYP: LazyLock<FnType<Refs>> = T::TYP;

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_ctx, typ, scope, from, top_id| match from {
            [_, _] => Ok(Box::new(Self {
                scope: scope.clone(),
                predid: BindId::new(),
                top_id,
                ftyp: TArc::new(typ.clone()),
                etyp: Type::Bottom(PhantomData),
                mftyp: match &typ.args[1].typ {
                    Type::Fn(ft) => ft.clone(),
                    t => bail!("expected a function not {t}"),
                },
                slots: smallvec![],
                cur: ValArray::from([]),
                t: T::default(),
            })),
            _ => bail!("expected two arguments"),
        })
    }
}

impl<C: Ctx, E: UserEvent, T: MapFn<C, E>> Apply<C, E> for MapQ<C, E, T> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        let slen = self.slots.len();
        if let Some(v) = from[1].update(ctx, event) {
            event.variables.insert(self.predid, v);
        }
        let up = match from[0].update(ctx, event) {
            Some(Value::Array(a)) if a.len() == self.slots.len() => Some(a),
            Some(Value::Array(a)) if a.len() < self.slots.len() => {
                while self.slots.len() > a.len() {
                    if let Some(s) = self.slots.pop() {
                        s.pred.delete(ctx);
                        ctx.env.unbind_variable(s.id);
                    }
                }
                Some(a)
            }
            Some(Value::Array(a)) => {
                while self.slots.len() < a.len() {
                    let (id, node) =
                        gen::bind(ctx, &self.scope, "x", self.etyp.clone(), self.top_id);
                    let fargs = vec![node];
                    let fnode = gen::reference(
                        ctx,
                        self.predid,
                        Type::Fn(self.mftyp.clone()),
                        self.top_id,
                    );
                    let pred = gen::apply(fnode, fargs, self.mftyp.clone(), self.top_id);
                    self.slots.push(Slot { id, pred, out: None, buf: None });
                }
                Some(a)
            }
            Some(_) | None => None,
        };
        if let Some(a) = up {
            for (s, v) in self.slots.iter().zip(a.iter()) {
                event.variables.insert(s.id, v.clone());
            }
            self.cur = a.clone();
            if a.len() == 0 {
                return Some(Value::Array(a));
            }
        }
        let init = event.init;
        let mut up = false;
        for (i, s) in self.slots.iter_mut().enumerate() {
            if i == slen {
                // new nodes were added starting here
                event.init = true;
                if !event.variables.contains_key(&self.predid) {
                    if let Some(v) = ctx.cached.get(&self.predid) {
                        event.variables.insert(self.predid, v.clone());
                    }
                }
            }
            if let Some(v) = s.pred.update(ctx, event) {
                s.buf = self.t.predicate(&v);
                s.out = Some(v);
                up = true;
            }
        }
        event.init = init;
        if up && self.slots.iter().all(|s| s.out.is_some()) {
            let res = self.t.finish(&mut self.slots);
            for s in self.slots.iter_mut() {
                s.out = None;
            }
            res
        } else {
            None
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
        let at = self
            .ftyp
            .args
            .get(0)
            .map(|a| a.typ.clone())
            .ok_or_else(|| anyhow!("expected 2 arguments"))?;
        at.check_contains(&from[0].typ)?;
        Type::Fn(self.mftyp.clone()).check_contains(&from[1].typ)?;
        let et = match at {
            Type::Array(et) => (*et).clone(),
            _ => bail!("expected array got {at}"),
        };
        self.etyp = et;
        let (_, node) = gen::bind(ctx, &self.scope, "x", self.etyp.clone(), self.top_id);
        let fargs = vec![node];
        let ft = self.mftyp.clone();
        let fnode = gen::reference(ctx, self.predid, Type::Fn(ft.clone()), self.top_id);
        let mut node = gen::apply(fnode, fargs, ft, self.top_id);
        let r = node.typecheck(ctx);
        node.delete(ctx);
        r?;
        Ok(())
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        for s in &self.slots {
            s.pred.refs(f)
        }
    }
}

#[cfg(test)]
const ARRAY_MAPQ: &str = r#"
{
  let a = [1, 2, 3, 4];
  array::mapq(a, |x| x > 3)
}
"#;

#[cfg(test)]
run!(array_mapq, ARRAY_MAPQ, |v: Result<&Value>| {
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

macro_rules! mapfn {
    ($name:ident, $bname:literal, $typ:literal, $buf:literal) => {
        pub(super) struct $name<C: Ctx, E: UserEvent> {
            pred: Node<C, E>,
            pid: BindId,
            x: BindId,
            typ: FnType<NoRefs>,
            etyp: Type<NoRefs>,
            buf: SmallVec<[Value; $buf]>,
        }

        impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for $name<C, E> {
            const NAME: &str = $bname;
            deftype!($typ);

            fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
                Arc::new(|ctx, typ, scope, from, top_id| match from {
                    [_, _] => {
                        let etyp = Type::empty_tvar();
                        let (x, n) = gen::bind(ctx, scope, "x", etyp.clone(), top_id);
                        let fargs = vec![n];
                        let ft = match typ.args.get(1) {
                            Some(FnArgType { label: _, typ: Type::Fn(ft) }) => ft.clone(),
                            _ => bail!("expected function"),
                        };
                        let pid = BindId::new();
                        let fnode =
                            gen::reference(ctx, pid, Type::Fn(ft.clone()), top_id);
                        let pred = gen::apply(fnode, fargs, ft.clone(), top_id);
                        Ok(Box::new(Self {
                            pred,
                            pid,
                            x,
                            typ: typ.clone(),
                            etyp,
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
                if let Some(v) = from[1].update(ctx, event) {
                    event.variables.insert(self.pid, v);
                }
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
                let et = self.etyp.clone();
                Type::Array(TArc::new(et)).check_contains(&from[0].typ)?;
                self.pred.typecheck(ctx)?;
                Ok(())
            }
        }
    };
}

mapfn!(Filter, "array_filter", "fn(Array<'a>, fn('a) -> bool) -> Array<'a>", 32);

impl<C: Ctx, E: UserEvent> Filter<C, E> {
    fn predicate(&mut self, ctx: &mut ExecCtx<C, E>, set: bool, event: &mut Event<E>) {
        match self.pred.update(ctx, event) {
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
                        kind: NodeKind::Ref { id: x, top_id },
                    },
                    Node {
                        spec: Box::new(ExprKind::Ref { name: ["y"].into() }.to_expr()),
                        typ: Type::empty_tvar(),
                        kind: NodeKind::Ref { id: y, top_id },
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
                        kind: NodeKind::Ref { id: n, top_id },
                    },
                    Node {
                        spec: Box::new(ExprKind::Ref { name: ["x"].into() }.to_expr()),
                        typ: from[0].typ.clone(),
                        kind: NodeKind::Ref { id: x, top_id },
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
