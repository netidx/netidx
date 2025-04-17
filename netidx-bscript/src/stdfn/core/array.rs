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
use netidx::{publisher::Typ, subscriber::Value, utils::Either};
use netidx_netproto::valarray::ValArray;
use smallvec::{smallvec, SmallVec};
use std::{
    iter, mem,
    sync::{Arc, LazyLock},
};
use triomphe::Arc as TArc;

pub trait MapFn<C: Ctx, E: UserEvent>: Default + Send + Sync + 'static {
    const NAME: &str;
    const TYP: LazyLock<FnType<Refs>>;

    /// finish will be called when every lambda instance has produced
    /// a value for the updated array. Out contains the output of the
    /// predicate lambda for each index i, and a is the array. out and
    /// a are guaranteed to have the same length. out[i].cur is
    /// guaranteed to be Some.
    fn finish(&mut self, slots: &[Slot<C, E>], a: &ValArray) -> Option<Value>;
}

struct Slot<C: Ctx, E: UserEvent> {
    id: BindId,
    pred: Node<C, E>,
    cur: Option<Value>,
}

pub struct MapQ<C: Ctx, E: UserEvent, T: MapFn<C, E>> {
    scope: ModPath,
    predid: BindId,
    top_id: ExprId,
    ftyp: TArc<FnType<NoRefs>>,
    mftyp: TArc<FnType<NoRefs>>,
    etyp: Type<NoRefs>,
    slots: Vec<Slot<C, E>>,
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

                etyp: match &typ.args[1].typ {
                    Type::Array(et) => (**et).clone(),
                    t => bail!("expected array not {t}"),
                },
                mftyp: match &typ.args[1].typ {
                    Type::Fn(ft) => ft.clone(),
                    t => bail!("expected a function not {t}"),
                },
                slots: vec![],
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
                    self.slots.push(Slot { id, pred, cur: None });
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
                s.cur = Some(v);
                up = true;
            }
        }
        event.init = init;
        if up && self.slots.iter().all(|s| s.cur.is_some()) {
            self.t.finish(&mut &self.slots, &self.cur)
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
        self.ftyp
            .args
            .get(0)
            .map(|a| a.typ.clone())
            .ok_or_else(|| anyhow!("expected 2 arguments"))?
            .check_contains(&from[0].typ)?;
        Type::Fn(self.mftyp.clone()).check_contains(&from[1].typ)?;
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

#[derive(Default)]
pub(super) struct MapImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for MapImpl {
    const NAME: &str = "array_map";
    deftype!("fn(Array<'a>, fn('a) -> 'b) -> Array<'b>");

    fn finish(&mut self, slots: &[Slot<C, E>], _: &ValArray) -> Option<Value> {
        Some(Value::Array(ValArray::from_iter_exact(
            slots.iter().map(|s| s.cur.clone().unwrap()),
        )))
    }
}

pub(super) type Map<C: Ctx, E: UserEvent> = MapQ<C, E, MapImpl>;

#[derive(Default)]
pub(super) struct FilterImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for FilterImpl {
    const NAME: &str = "array_filter";
    deftype!("fn(Array<'a>, fn('a) -> bool) -> Array<'a>");

    fn finish(&mut self, slots: &[Slot<C, E>], a: &ValArray) -> Option<Value> {
        Some(Value::Array(ValArray::from_iter(slots.iter().zip(a.iter()).filter_map(
            |(p, v)| match p.cur {
                Some(Value::Bool(true)) => Some(v.clone()),
                _ => None,
            },
        ))))
    }
}

pub(super) type Filter<C, E> = MapQ<C, E, FilterImpl>;

#[derive(Default)]
pub(super) struct FlatMapImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for FlatMapImpl {
    const NAME: &str = "array_flat_map";
    deftype!("fn(Array<'a>, fn('a) -> ['b, Array<'b>]) -> Array<'b>");

    fn finish(&mut self, slots: &[Slot<C, E>], _: &ValArray) -> Option<Value> {
        Some(Value::Array(ValArray::from_iter(slots.iter().flat_map(|s| {
            match s.cur.as_ref().unwrap() {
                Value::Array(a) => Either::Left(a.clone().into_iter()),
                v => Either::Right(iter::once(v.clone())),
            }
        }))))
    }
}

pub(super) type FlatMap<C: Ctx, E: UserEvent> = MapQ<C, E, FlatMapImpl>;

#[derive(Default)]
pub(super) struct FilterMapImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for FilterMapImpl {
    const NAME: &str = "array_filter_map";
    deftype!("fn(Array<'a>, fn('a) -> ['b, null]) -> Array<'b>");

    fn finish(&mut self, slots: &[Slot<C, E>], _: &ValArray) -> Option<Value> {
        Some(Value::Array(ValArray::from_iter(slots.iter().filter_map(|s| {
            match s.cur.as_ref().unwrap() {
                Value::Null => None,
                v => Some(v.clone()),
            }
        }))))
    }
}

pub(super) type FilterMap<C: Ctx, E: UserEvent> = MapQ<C, E, FilterMapImpl>;

#[derive(Default)]
pub(super) struct FindImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for FindImpl {
    const NAME: &str = "array_find";
    deftype!("fn(Array<'a>, fn('a) -> bool) -> ['a, null]");

    fn finish(&mut self, slots: &[Slot<C, E>], a: &ValArray) -> Option<Value> {
        let r = slots
            .iter()
            .enumerate()
            .find(|(_, s)| match s.cur.as_ref() {
                Some(Value::Bool(true)) => true,
                _ => false,
            })
            .map(|(i, _)| a[i].clone())
            .unwrap_or(Value::Null);
        Some(r)
    }
}

pub(super) type Find<C: Ctx, E: UserEvent> = MapQ<C, E, FindImpl>;

#[derive(Default)]
pub(super) struct FindMapImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for FindMapImpl {
    const NAME: &str = "array_find_map";
    deftype!("fn(Array<'a>, fn('a) -> ['b, null]) -> ['b, null]");

    fn finish(&mut self, slots: &[Slot<C, E>], _: &ValArray) -> Option<Value> {
        let r = slots
            .iter()
            .find_map(|s| match s.cur.as_ref().unwrap() {
                Value::Null => None,
                v => Some(v.clone()),
            })
            .unwrap_or(Value::Null);
        Some(r)
    }
}

pub(super) type FindMap<C: Ctx, E: UserEvent> = MapQ<C, E, FindMapImpl>;

pub(super) struct Fold<C: Ctx, E: UserEvent> {
    scope: ModPath,
    top_id: ExprId,
    fid: BindId,
    initid: BindId,
    binds: Vec<BindId>,
    head: Option<Node<C, E>>,
    ftyp: TArc<FnType<NoRefs>>,
    mftype: TArc<FnType<NoRefs>>,
    etyp: Type<NoRefs>,
    ityp: Type<NoRefs>,
    cur: ValArray,
    init: None,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Fold<C, E> {
    const NAME: &str = "array_fold";
    deftype!("fn(Array<'a>, 'b, fn('b, 'a) -> 'b) -> 'b");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_ctx, typ, scope, from, top_id| match from {
            [_, _, _] => Ok(Box::new(Self {
                scope: scope.clone(),
                top_id,
                binds: vec![],
                head: None,
                fid: BindId::new(),
                initid: BindId::new(),
                ftyp: TArc::new(typ.clone()),
                etyp: match &typ.args[0].typ {
                    Type::Array(et) => (**et).clone(),
                    t => bail!("expected array not {t}"),
                },
                ityp: typ.args[1].typ.clone(),
                mftype: match &typ.args[2].typ {
                    Type::Fn(ft) => ft.clone(),
                    t => bail!("expected a function not {t}"),
                },
                cur: ValArray::from([]),
                init: None,
            })),
            _ => bail!("expected three arguments"),
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
        match from[0].update(ctx, event) {
            None => (),
            Some(Value::Array(a)) if a.len() == self.binds.len() => {
                for (id, v) in self.binds.iter().zip(a.iter()) {
                    event.variables.insert(*id, v.clone());
                }
            }
            Some(Value::Array(a)) => {
                if let Some(n) = self.head.take() {
                    n.delete(ctx)
                }
                while self.binds.len() < a.len() {
                    self.binds.push(BindId::new());
                }
                while a.len() < self.binds.len() {
                    self.binds.pop();
                }
                let mut n =
                    gen::reference(ctx, self.initid, self.ityp.clone(), self.top_id);
                for i in 0..self.binds.len() {
                    event.variables.insert(self.binds[i], a[i].clone());
                    let x = gen::reference(
                        ctx,
                        self.binds[i],
                        self.etyp.clone(),
                        self.top_id,
                    );
                    let fnode = gen::reference(
                        ctx,
                        self.fid,
                        Type::Fn(self.mftype.clone()),
                        self.top_id,
                    );
                    n = gen::apply(fnode, vec![n, x], self.mftype.clone(), self.top_id);
                }
                self.head = Some(n);
            }
            _ => (),
        }
        if let Some(v) = from[1].update(ctx, event) {
            event.variables.insert(self.initid, v.clone());
            self.init = Some(v);
        }
        if let Some(v) = from[2].update(ctx, event) {
            event.variables.insert(self.fid, v);
        }
        self.head.as_mut().and_then(|n| n.update(ctx, event))
    }

    fn typecheck(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
    ) -> anyhow::Result<()> {
        for n in from.iter_mut() {
            n.typecheck(ctx)?;
        }
        Type::Array(triomphe::Arc::new(self.etyp.clone()))
            .check_contains(&from[0].typ)?;
        self.ityp.check_contains(&from[1].typ)?;
        Type::Fn(self.mftype.clone()).check_contains(&from[2].typ)?;
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
