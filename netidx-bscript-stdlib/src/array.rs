use crate::{deftype, CachedArgs, CachedVals, EvalCached};
use anyhow::{anyhow, bail, Result};
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use netidx::{publisher::Typ, subscriber::Value, utils::Either};
use netidx_bscript::{
    expr::{ExprId, ModPath},
    node::genn,
    typ::{FnType, Type},
    Apply, BindId, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, LambdaId, Node,
    UserEvent,
};
use netidx_value::ValArray;
use smallvec::{smallvec, SmallVec};
use std::{
    collections::{hash_map::Entry, VecDeque},
    fmt::Debug,
    iter,
    sync::{Arc, LazyLock},
};
use triomphe::Arc as TArc;

pub trait MapFn<C: Ctx, E: UserEvent>: Debug + Default + Send + Sync + 'static {
    const NAME: &str;
    const TYP: LazyLock<FnType>;

    /// finish will be called when every lambda instance has produced
    /// a value for the updated array. Out contains the output of the
    /// predicate lambda for each index i, and a is the array. out and
    /// a are guaranteed to have the same length. out[i].cur is
    /// guaranteed to be Some.
    fn finish(&mut self, slots: &[Slot<C, E>], a: &ValArray) -> Option<Value>;
}

#[derive(Debug)]
pub struct Slot<C: Ctx, E: UserEvent> {
    id: BindId,
    pred: Node<C, E>,
    pub cur: Option<Value>,
}

#[derive(Debug)]
pub struct MapQ<C: Ctx, E: UserEvent, T: MapFn<C, E>> {
    scope: ModPath,
    predid: BindId,
    top_id: ExprId,
    ftyp: TArc<FnType>,
    mftyp: TArc<FnType>,
    etyp: Type,
    slots: Vec<Slot<C, E>>,
    cur: ValArray,
    t: T,
}

impl<C: Ctx, E: UserEvent, T: MapFn<C, E>> BuiltIn<C, E> for MapQ<C, E, T> {
    const NAME: &str = T::NAME;
    const TYP: LazyLock<FnType> = T::TYP;

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_ctx, typ, scope, from, top_id| match from {
            [_, _] => {
                Ok(Box::new(Self {
                    scope: ModPath(scope.0.append(
                        format_compact!("fn{}", LambdaId::new().inner()).as_str(),
                    )),
                    predid: BindId::new(),
                    top_id,
                    ftyp: TArc::new(typ.clone()),

                    etyp: match &typ.args[0].typ {
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
                }))
            }
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
            ctx.cached.insert(self.predid, v.clone());
            event.variables.insert(self.predid, v);
        }
        let (up, resized) = match from[0].update(ctx, event) {
            Some(Value::Array(a)) if a.len() == slen => (Some(a), false),
            Some(Value::Array(a)) if a.len() < slen => {
                while self.slots.len() > a.len() {
                    if let Some(mut s) = self.slots.pop() {
                        s.pred.delete(ctx);
                        ctx.cached.remove(&s.id);
                        ctx.env.unbind_variable(s.id);
                    }
                }
                (Some(a), true)
            }
            Some(Value::Array(a)) => {
                while self.slots.len() < a.len() {
                    let (id, node) =
                        genn::bind(ctx, &self.scope, "x", self.etyp.clone(), self.top_id);
                    let fargs = vec![node];
                    let fnode = genn::reference(
                        ctx,
                        self.predid,
                        Type::Fn(self.mftyp.clone()),
                        self.top_id,
                    );
                    let pred = genn::apply(fnode, fargs, self.mftyp.clone(), self.top_id);
                    self.slots.push(Slot { id, pred, cur: None });
                }
                (Some(a), true)
            }
            Some(_) | None => (None, false),
        };
        if let Some(a) = up {
            for (s, v) in self.slots.iter().zip(a.iter()) {
                ctx.cached.insert(s.id, v.clone());
                event.variables.insert(s.id, v.clone());
            }
            self.cur = a.clone();
            if a.len() == 0 {
                return Some(Value::Array(a));
            }
        }
        let init = event.init;
        let mut up = resized;
        for (i, s) in self.slots.iter_mut().enumerate() {
            if i == slen {
                // new nodes were added starting here
                event.init = true;
                if let Entry::Vacant(e) = event.variables.entry(self.predid)
                    && let Some(v) = ctx.cached.get(&self.predid)
                {
                    e.insert(v.clone());
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
            .check_contains(&ctx.env, &from[0].typ())?;
        Type::Fn(self.mftyp.clone()).check_contains(&ctx.env, &from[1].typ())?;
        let (_, node) = genn::bind(ctx, &self.scope, "x", self.etyp.clone(), self.top_id);
        let fargs = vec![node];
        let ft = self.mftyp.clone();
        let fnode = genn::reference(ctx, self.predid, Type::Fn(ft.clone()), self.top_id);
        let mut node = genn::apply(fnode, fargs, ft, self.top_id);
        let r = node.typecheck(ctx);
        node.delete(ctx);
        r
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        for s in &self.slots {
            s.pred.refs(f)
        }
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.cached.remove(&self.predid);
        for sl in &mut self.slots {
            ctx.cached.remove(&sl.id);
            sl.pred.delete(ctx);
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct MapImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for MapImpl {
    const NAME: &str = "array_map";
    deftype!("core::array", "fn(Array<'a>, fn('a) -> 'b) -> Array<'b>");

    fn finish(&mut self, slots: &[Slot<C, E>], _: &ValArray) -> Option<Value> {
        Some(Value::Array(ValArray::from_iter_exact(
            slots.iter().map(|s| s.cur.clone().unwrap()),
        )))
    }
}

pub(super) type Map<C, E> = MapQ<C, E, MapImpl>;

#[derive(Debug, Default)]
pub(super) struct FilterImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for FilterImpl {
    const NAME: &str = "array_filter";
    deftype!("core::array", "fn(Array<'a>, fn('a) -> bool) -> Array<'a>");

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

#[derive(Debug, Default)]
pub(super) struct FlatMapImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for FlatMapImpl {
    const NAME: &str = "array_flat_map";
    deftype!("core::array", "fn(Array<'a>, fn('a) -> ['b, Array<'b>]) -> Array<'b>");

    fn finish(&mut self, slots: &[Slot<C, E>], _: &ValArray) -> Option<Value> {
        Some(Value::Array(ValArray::from_iter(slots.iter().flat_map(|s| {
            match s.cur.as_ref().unwrap() {
                Value::Array(a) => Either::Left(a.clone().into_iter()),
                v => Either::Right(iter::once(v.clone())),
            }
        }))))
    }
}

pub(super) type FlatMap<C, E> = MapQ<C, E, FlatMapImpl>;

#[derive(Debug, Default)]
pub(super) struct FilterMapImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for FilterMapImpl {
    const NAME: &str = "array_filter_map";
    deftype!("core::array", "fn(Array<'a>, fn('a) -> ['b, null]) -> Array<'b>");

    fn finish(&mut self, slots: &[Slot<C, E>], _: &ValArray) -> Option<Value> {
        Some(Value::Array(ValArray::from_iter(slots.iter().filter_map(|s| {
            match s.cur.as_ref().unwrap() {
                Value::Null => None,
                v => Some(v.clone()),
            }
        }))))
    }
}

pub(super) type FilterMap<C, E> = MapQ<C, E, FilterMapImpl>;

#[derive(Debug, Default)]
pub(super) struct FindImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for FindImpl {
    const NAME: &str = "array_find";
    deftype!("core::array", "fn(Array<'a>, fn('a) -> bool) -> ['a, null]");

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

pub(super) type Find<C, E> = MapQ<C, E, FindImpl>;

#[derive(Debug, Default)]
pub(super) struct FindMapImpl;

impl<C: Ctx, E: UserEvent> MapFn<C, E> for FindMapImpl {
    const NAME: &str = "array_find_map";
    deftype!("core::array", "fn(Array<'a>, fn('a) -> ['b, null]) -> ['b, null]");

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

pub(super) type FindMap<C, E> = MapQ<C, E, FindMapImpl>;

#[derive(Debug)]
pub(super) struct Fold<C: Ctx, E: UserEvent> {
    top_id: ExprId,
    fid: BindId,
    binds: Vec<BindId>,
    nodes: Vec<Node<C, E>>,
    inits: Vec<Option<Value>>,
    initids: Vec<BindId>,
    initid: BindId,
    mftype: TArc<FnType>,
    etyp: Type,
    ityp: Type,
    init: Option<Value>,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Fold<C, E> {
    const NAME: &str = "array_fold";
    deftype!("core::array", "fn(Array<'a>, 'b, fn('b, 'a) -> 'b) -> 'b");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_ctx, typ, _, from, top_id| match from {
            [_, _, _] => Ok(Box::new(Self {
                top_id,
                binds: vec![],
                nodes: vec![],
                inits: vec![],
                initids: vec![],
                initid: BindId::new(),
                fid: BindId::new(),
                etyp: match &typ.args[0].typ {
                    Type::Array(et) => (**et).clone(),
                    t => bail!("expected array not {t}"),
                },
                ityp: typ.args[1].typ.clone(),
                mftype: match &typ.args[2].typ {
                    Type::Fn(ft) => ft.clone(),
                    t => bail!("expected a function not {t}"),
                },
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
        let init = match from[0].update(ctx, event) {
            None => self.nodes.len(),
            Some(Value::Array(a)) if a.len() == self.binds.len() => {
                for (id, v) in self.binds.iter().zip(a.iter()) {
                    ctx.cached.insert(*id, v.clone());
                    event.variables.insert(*id, v.clone());
                }
                self.nodes.len()
            }
            Some(Value::Array(a)) => {
                while self.binds.len() < a.len() {
                    self.binds.push(BindId::new());
                    self.inits.push(None);
                    self.initids.push(BindId::new());
                }
                while a.len() < self.binds.len() {
                    if let Some(id) = self.binds.pop() {
                        ctx.cached.remove(&id);
                    }
                    if let Some(id) = self.initids.pop() {
                        ctx.cached.remove(&id);
                    }
                    self.inits.pop();
                    if let Some(mut n) = self.nodes.pop() {
                        n.delete(ctx);
                    }
                }
                let init = self.nodes.len();
                for i in 0..self.binds.len() {
                    ctx.cached.insert(self.binds[i], a[i].clone());
                    event.variables.insert(self.binds[i], a[i].clone());
                    if i >= self.nodes.len() {
                        let n = genn::reference(
                            ctx,
                            if i == 0 { self.initid } else { self.initids[i - 1] },
                            self.ityp.clone(),
                            self.top_id,
                        );
                        let x = genn::reference(
                            ctx,
                            self.binds[i],
                            self.etyp.clone(),
                            self.top_id,
                        );
                        let fnode = genn::reference(
                            ctx,
                            self.fid,
                            Type::Fn(self.mftype.clone()),
                            self.top_id,
                        );
                        let node = genn::apply(
                            fnode,
                            vec![n, x],
                            self.mftype.clone(),
                            self.top_id,
                        );
                        self.nodes.push(node);
                    }
                }
                init
            }
            _ => self.nodes.len(),
        };
        if let Some(v) = from[1].update(ctx, event) {
            ctx.cached.insert(self.initid, v.clone());
            event.variables.insert(self.initid, v.clone());
            self.init = Some(v);
        }
        if let Some(v) = from[2].update(ctx, event) {
            ctx.cached.insert(self.fid, v.clone());
            event.variables.insert(self.fid, v);
        }
        let old_init = event.init;
        for i in 0..self.nodes.len() {
            if i == init {
                event.init = true;
                if let Some(v) = ctx.cached.get(&self.fid)
                    && let Entry::Vacant(e) = event.variables.entry(self.fid)
                {
                    e.insert(v.clone());
                }
                if i == 0 {
                    if let Some(v) = self.init.as_ref()
                        && let Entry::Vacant(e) = event.variables.entry(self.initid)
                    {
                        e.insert(v.clone());
                    }
                } else {
                    if let Some(v) = self.inits[i - 1].clone() {
                        event.variables.insert(self.initids[i - 1], v);
                    }
                }
            }
            match self.nodes[i].update(ctx, event) {
                Some(v) => {
                    ctx.cached.insert(self.initids[i], v.clone());
                    event.variables.insert(self.initids[i], v.clone());
                    self.inits[i] = Some(v);
                }
                None => {
                    ctx.cached.remove(&self.initids[i]);
                    event.variables.remove(&self.initids[i]);
                    self.inits[i] = None;
                }
            }
        }
        event.init = old_init;
        self.inits.last().and_then(|v| v.clone())
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
            .check_contains(&ctx.env, &from[0].typ())?;
        self.ityp.check_contains(&ctx.env, &from[1].typ())?;
        Type::Fn(self.mftype.clone()).check_contains(&ctx.env, &from[2].typ())?;
        let mut n = genn::reference(ctx, self.initid, self.ityp.clone(), self.top_id);
        let x = genn::reference(ctx, BindId::new(), self.etyp.clone(), self.top_id);
        let fnode =
            genn::reference(ctx, self.fid, Type::Fn(self.mftype.clone()), self.top_id);
        n = genn::apply(fnode, vec![n, x], self.mftype.clone(), self.top_id);
        let r = n.typecheck(ctx);
        n.delete(ctx);
        r
    }

    fn refs<'a>(&'a self, f: &'a mut (dyn FnMut(BindId) + 'a)) {
        for n in &self.nodes {
            n.refs(f)
        }
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        let i =
            iter::once(&self.initid).chain(self.binds.iter()).chain(self.initids.iter());
        for id in i {
            ctx.cached.remove(id);
        }
        for n in &mut self.nodes {
            n.delete(ctx);
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct ConcatEv(SmallVec<[Value; 32]>);

impl EvalCached for ConcatEv {
    const NAME: &str = "array_concat";
    deftype!("core::array", "fn(Array<'a>, @args: Array<'a>) -> Array<'a>");

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
            self.0.clear();
            None
        }
    }
}

pub(super) type Concat = CachedArgs<ConcatEv>;

#[derive(Debug, Default)]
pub(super) struct PushBackEv(SmallVec<[Value; 32]>);

impl EvalCached for PushBackEv {
    const NAME: &str = "array_push_back";
    deftype!("core::array", "fn(Array<'a>, @args: 'a) -> Array<'a>");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        let mut present = true;
        match &from.0[..] {
            [Some(Value::Array(a)), tl @ ..] => {
                self.0.extend(a.iter().map(|v| v.clone()));
                for v in tl {
                    match v {
                        Some(v) => self.0.push(v.clone()),
                        None => present = false,
                    }
                }
            }
            [] | [None, ..] | [Some(_), ..] => present = false,
        }
        if present {
            let a = ValArray::from_iter_exact(self.0.drain(..));
            Some(Value::Array(a))
        } else {
            self.0.clear();
            None
        }
    }
}

pub(super) type PushBack = CachedArgs<PushBackEv>;

#[derive(Debug, Default)]
pub(super) struct PushFrontEv(SmallVec<[Value; 32]>);

impl EvalCached for PushFrontEv {
    const NAME: &str = "array_push_front";
    deftype!("core::array", "fn(Array<'a>, @args: 'a) -> Array<'a>");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        let mut present = true;
        match &from.0[..] {
            [Some(Value::Array(a)), tl @ ..] => {
                for v in tl {
                    match v {
                        Some(v) => self.0.push(v.clone()),
                        None => present = false,
                    }
                }
                self.0.extend(a.iter().map(|v| v.clone()));
            }
            [] | [None, ..] | [Some(_), ..] => present = false,
        }
        if present {
            let a = ValArray::from_iter_exact(self.0.drain(..));
            Some(Value::Array(a))
        } else {
            self.0.clear();
            None
        }
    }
}

pub(super) type PushFront = CachedArgs<PushFrontEv>;

#[derive(Debug, Default)]
pub(super) struct WindowEv(SmallVec<[Value; 32]>);

impl EvalCached for WindowEv {
    const NAME: &str = "array_window";
    deftype!("core::array", "fn(#n:i64, Array<'a>, @args: 'a) -> Array<'a>");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        let mut present = true;
        match &from.0[..] {
            [Some(Value::I64(window)), Some(Value::Array(a)), tl @ ..] => {
                let window = *window as usize;
                let total = a.len() + tl.len();
                if total <= window {
                    self.0.extend(a.iter().cloned());
                    for v in tl {
                        match v {
                            Some(v) => self.0.push(v.clone()),
                            None => present = false,
                        }
                    }
                } else if a.len() >= (total - window) {
                    self.0.extend(a[(total - window)..].iter().cloned());
                    for v in tl {
                        match v {
                            Some(v) => self.0.push(v.clone()),
                            None => present = false,
                        }
                    }
                } else {
                    for v in &tl[tl.len() - window..] {
                        match v {
                            Some(v) => self.0.push(v.clone()),
                            None => present = false,
                        }
                    }
                }
            }
            [] | [_] | [_, None, ..] | [None, _, ..] | [Some(_), Some(_), ..] => {
                present = false
            }
        }
        if present {
            let a = ValArray::from_iter_exact(self.0.drain(..));
            Some(Value::Array(a))
        } else {
            self.0.clear();
            None
        }
    }
}

pub(super) type Window = CachedArgs<WindowEv>;

#[derive(Debug, Default)]
pub(super) struct LenEv;

impl EvalCached for LenEv {
    const NAME: &str = "array_len";
    deftype!("core::array", "fn(Array<'a>) -> i64");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::Array(a)) => Some(Value::I64(a.len() as i64)),
            Some(_) | None => None,
        }
    }
}

pub(super) type Len = CachedArgs<LenEv>;

#[derive(Debug, Default)]
pub(super) struct FlattenEv(SmallVec<[Value; 32]>);

impl EvalCached for FlattenEv {
    const NAME: &str = "array_flatten";
    deftype!("core::array", "fn(Array<Array<'a>>) -> Array<'a>");

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

#[derive(Debug, Default)]
pub(super) struct SortEv(SmallVec<[Value; 32]>);

impl EvalCached for SortEv {
    const NAME: &str = "array_sort";
    deftype!("core::array", "fn(Array<'a>) -> Array<'a>");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        if let Some(Value::Array(a)) = &from.0[0] {
            self.0.extend(a.iter().cloned());
            self.0.sort();
            return Some(Value::Array(ValArray::from_iter_exact(self.0.drain(..))));
        }
        None
    }
}

pub(super) type Sort = CachedArgs<SortEv>;

#[derive(Debug, Default)]
pub(super) struct EnumerateEv;

impl EvalCached for EnumerateEv {
    const NAME: &str = "array_enumerate";
    deftype!("core::array", "fn(Array<'a>) -> Array<(i64, 'a)>");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        if let Some(Value::Array(a)) = &from.0[0] {
            let a = ValArray::from_iter_exact(
                a.iter().enumerate().map(|(i, v)| (i, v.clone()).into()),
            );
            return Some(Value::Array(a));
        }
        None
    }
}

pub(super) type Enumerate = CachedArgs<EnumerateEv>;

#[derive(Debug, Default)]
pub(super) struct ZipEv;

impl EvalCached for ZipEv {
    const NAME: &str = "array_zip";
    deftype!("core::array", "fn(Array<'a>, Array<'b>) -> Array<('a, 'b)>");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[..] {
            [Some(Value::Array(a0)), Some(Value::Array(a1))] => {
                Some(Value::Array(ValArray::from_iter_exact(
                    a0.iter().cloned().zip(a1.iter().cloned()).map(|p| p.into()),
                )))
            }
            _ => None,
        }
    }
}

pub(super) type Zip = CachedArgs<ZipEv>;

#[derive(Debug, Default)]
pub(super) struct UnzipEv {
    t0: Vec<Value>,
    t1: Vec<Value>,
}

impl EvalCached for UnzipEv {
    const NAME: &str = "array_unzip";
    deftype!("core::array", "fn(Array<('a, 'b)>) -> (Array<'a>, Array<'b>)");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[..] {
            [Some(Value::Array(a))] => {
                for v in a {
                    if let Value::Array(a) = v {
                        match &a[..] {
                            [v0, v1] => {
                                self.t0.push(v0.clone());
                                self.t1.push(v1.clone());
                            }
                            _ => (),
                        }
                    }
                }
                let v0 = Value::Array(ValArray::from_iter_exact(self.t0.drain(..)));
                let v1 = Value::Array(ValArray::from_iter_exact(self.t1.drain(..)));
                Some(Value::Array(ValArray::from_iter_exact([v0, v1].into_iter())))
            }
            _ => None,
        }
    }
}

pub(super) type Unzip = CachedArgs<UnzipEv>;

#[derive(Debug)]
pub(super) struct Group<C: Ctx, E: UserEvent> {
    queue: VecDeque<Value>,
    buf: SmallVec<[Value; 16]>,
    pred: Node<C, E>,
    mftyp: TArc<FnType>,
    etyp: Type,
    ready: bool,
    pid: BindId,
    nid: BindId,
    xid: BindId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Group<C, E> {
    const NAME: &str = "group";
    deftype!("core::array", "fn('a, fn(i64, 'a) -> bool) -> Array<'a>");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, typ, scope, from, top_id| match from {
            [arg, _] => {
                let scope =
                    ModPath(scope.0.append(
                        format_compact!("fn{}", LambdaId::new().inner()).as_str(),
                    ));
                let n_typ = Type::Primitive(Typ::I64.into());
                let etyp = arg.typ().clone();
                let mftyp = match &typ.args[1].typ {
                    Type::Fn(ft) => ft.clone(),
                    t => bail!("expected function not {t}"),
                };
                let (nid, n) = genn::bind(ctx, &scope, "n", n_typ.clone(), top_id);
                let (xid, x) = genn::bind(ctx, &scope, "x", etyp.clone(), top_id);
                let pid = BindId::new();
                let fnode = genn::reference(ctx, pid, Type::Fn(mftyp.clone()), top_id);
                let pred = genn::apply(fnode, vec![n, x], mftyp.clone(), top_id);
                Ok(Box::new(Self {
                    queue: VecDeque::new(),
                    buf: smallvec![],
                    mftyp,
                    etyp,
                    pred,
                    ready: true,
                    pid,
                    nid,
                    xid,
                }))
            }
            _ => bail!("expected two arguments"),
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
        macro_rules! set {
            ($v:expr) => {{
                self.ready = false;
                self.buf.push($v.clone());
                let len = Value::I64(self.buf.len() as i64);
                ctx.cached.insert(self.nid, len.clone());
                event.variables.insert(self.nid, len);
                ctx.cached.insert(self.xid, $v.clone());
                event.variables.insert(self.xid, $v);
            }};
        }
        if let Some(v) = from[0].update(ctx, event) {
            self.queue.push_back(v);
        }
        if let Some(v) = from[1].update(ctx, event) {
            ctx.cached.insert(self.pid, v.clone());
            event.variables.insert(self.pid, v);
        }
        if self.ready && self.queue.len() > 0 {
            let v = self.queue.pop_front().unwrap();
            set!(v);
        }
        loop {
            match self.pred.update(ctx, event) {
                None => break None,
                Some(v) => {
                    self.ready = true;
                    match v {
                        Value::Bool(true) => {
                            break Some(Value::Array(ValArray::from_iter_exact(
                                self.buf.drain(..),
                            )))
                        }
                        _ => match self.queue.pop_front() {
                            None => break None,
                            Some(v) => set!(v),
                        },
                    }
                }
            }
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
        self.etyp.check_contains(&ctx.env, &from[0].typ())?;
        Type::Fn(self.mftyp.clone()).check_contains(&ctx.env, &from[1].typ())?;
        self.pred.typecheck(ctx)?;
        Ok(())
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.cached.remove(&self.nid);
        ctx.cached.remove(&self.pid);
        ctx.cached.remove(&self.xid);
        self.pred.delete(ctx);
    }
}

#[derive(Debug)]
pub(super) struct Iter(BindId, ExprId);

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Iter {
    const NAME: &str = "iter";
    deftype!("core::array", "fn(Array<'a>) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, _, _, _, top_id| {
            let id = BindId::new();
            ctx.user.ref_var(id, top_id);
            Ok(Box::new(Iter(id, top_id)))
        })
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

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.0, self.1)
    }
}

#[derive(Debug)]
pub(super) struct IterQ {
    triggered: usize,
    queue: VecDeque<(usize, ValArray)>,
    id: BindId,
    top_id: ExprId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for IterQ {
    const NAME: &str = "iterq";
    deftype!("core::array", "fn(#clock:Any, Array<'a>) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, _, _, _, top_id| {
            let id = BindId::new();
            ctx.user.ref_var(id, top_id);
            Ok(Box::new(IterQ { triggered: 0, queue: VecDeque::new(), id, top_id }))
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for IterQ {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        if from[0].update(ctx, event).is_some() {
            self.triggered += 1;
        }
        if let Some(Value::Array(a)) = from[1].update(ctx, event) {
            if a.len() > 0 {
                self.queue.push_back((0, a));
            }
        }
        while self.triggered > 0 && self.queue.len() > 0 {
            let (i, a) = self.queue.front_mut().unwrap();
            while self.triggered > 0 && *i < a.len() {
                ctx.user.set_var(self.id, a[*i].clone());
                *i += 1;
                self.triggered -= 1;
            }
            if *i == a.len() {
                self.queue.pop_front();
            }
        }
        event.variables.get(&self.id).cloned()
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.id, self.top_id)
    }
}

pub(super) fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Result<ArcStr> {
    ctx.register_builtin::<Concat>()?;
    ctx.register_builtin::<Filter<C, E>>()?;
    ctx.register_builtin::<FilterMap<C, E>>()?;
    ctx.register_builtin::<Find<C, E>>()?;
    ctx.register_builtin::<FindMap<C, E>>()?;
    ctx.register_builtin::<FlatMap<C, E>>()?;
    ctx.register_builtin::<Enumerate>()?;
    ctx.register_builtin::<Zip>()?;
    ctx.register_builtin::<Unzip>()?;
    ctx.register_builtin::<Flatten>()?;
    ctx.register_builtin::<Fold<C, E>>()?;
    ctx.register_builtin::<Group<C, E>>()?;
    ctx.register_builtin::<Iter>()?;
    ctx.register_builtin::<IterQ>()?;
    ctx.register_builtin::<Len>()?;
    ctx.register_builtin::<Map<C, E>>()?;
    ctx.register_builtin::<PushBack>()?;
    ctx.register_builtin::<PushFront>()?;
    ctx.register_builtin::<Sort>()?;
    ctx.register_builtin::<Window>()?;
    Ok(literal!(include_str!("array.bs")))
}
