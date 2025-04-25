use crate::{
    deftype,
    expr::{ExprId, ModPath},
    node::{genn, Node},
    stdfn::{CachedArgs, CachedVals, EvalCached},
    typ::{FnType, NoRefs, Refs, Type},
    Apply, BindId, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, LambdaId, UserEvent,
};
use anyhow::{anyhow, bail};
use compact_str::format_compact;
use netidx::{publisher::Typ, subscriber::Value, utils::Either};
use netidx_netproto::valarray::ValArray;
use smallvec::{smallvec, SmallVec};
use std::{
    collections::VecDeque,
    iter,
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

pub struct Slot<C: Ctx, E: UserEvent> {
    id: BindId,
    pred: Node<C, E>,
    pub cur: Option<Value>,
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
                scope: ModPath(
                    scope.0.append(format_compact!("fn{}", LambdaId::new().0).as_str()),
                ),
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
            ctx.cached.insert(self.predid, v.clone());
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

pub(super) type Map<C, E> = MapQ<C, E, MapImpl>;

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

pub(super) type FlatMap<C, E> = MapQ<C, E, FlatMapImpl>;

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

pub(super) type FilterMap<C, E> = MapQ<C, E, FilterMapImpl>;

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

pub(super) type Find<C, E> = MapQ<C, E, FindImpl>;

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

pub(super) type FindMap<C, E> = MapQ<C, E, FindMapImpl>;

pub(super) struct Fold<C: Ctx, E: UserEvent> {
    top_id: ExprId,
    fid: BindId,
    initid: BindId,
    binds: Vec<BindId>,
    head: Option<Node<C, E>>,
    mftype: TArc<FnType<NoRefs>>,
    etyp: Type<NoRefs>,
    ityp: Type<NoRefs>,
    init: Option<Value>,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Fold<C, E> {
    const NAME: &str = "array_fold";
    deftype!("fn(Array<'a>, 'b, fn('b, 'a) -> 'b) -> 'b");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_ctx, typ, _, from, top_id| match from {
            [_, _, _] => Ok(Box::new(Self {
                top_id,
                binds: vec![],
                head: None,
                fid: BindId::new(),
                initid: BindId::new(),
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
            None => false,
            Some(Value::Array(a)) if a.len() == self.binds.len() => {
                for (id, v) in self.binds.iter().zip(a.iter()) {
                    event.variables.insert(*id, v.clone());
                }
                false
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
                    genn::reference(ctx, self.initid, self.ityp.clone(), self.top_id);
                for i in 0..self.binds.len() {
                    event.variables.insert(self.binds[i], a[i].clone());
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
                    // CR estokes: evaluating this is not tail recursive
                    n = genn::apply(fnode, vec![n, x], self.mftype.clone(), self.top_id);
                }
                self.head = Some(n);
                true
            }
            _ => false,
        };
        if let Some(v) = from[1].update(ctx, event) {
            event.variables.insert(self.initid, v.clone());
            self.init = Some(v);
        }
        if let Some(v) = from[2].update(ctx, event) {
            ctx.cached.insert(self.fid, v.clone());
            event.variables.insert(self.fid, v);
        }
        let old_init = event.init;
        if init {
            event.init = true;
            if let Some(v) = ctx.cached.get(&self.fid) {
                if !event.variables.contains_key(&self.fid) {
                    event.variables.insert(self.fid, v.clone());
                }
            }
            if let Some(v) = self.init.as_ref() {
                if !event.variables.contains_key(&self.initid) {
                    event.variables.insert(self.initid, v.clone());
                }
            }
        }
        let r = self.head.as_mut().and_then(|n| n.update(ctx, event));
        event.init = old_init;
        r
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
        if let Some(n) = self.head.as_ref() {
            n.refs(f)
        }
    }
}

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

pub(super) struct Group<C: Ctx, E: UserEvent> {
    queue: VecDeque<Value>,
    buf: SmallVec<[Value; 16]>,
    pred: Node<C, E>,
    mftyp: TArc<FnType<NoRefs>>,
    etyp: Type<NoRefs>,
    ready: bool,
    pid: BindId,
    nid: BindId,
    xid: BindId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Group<C, E> {
    const NAME: &str = "group";
    deftype!("fn('a, fn(u64, 'a) -> bool) -> Array<'a>");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, typ, scope, from, top_id| match from {
            [arg, _] => {
                let scope = ModPath(
                    scope.0.append(format_compact!("fn{}", LambdaId::new().0).as_str()),
                );
                let n_typ = Type::Primitive(Typ::U64.into());
                let etyp = arg.typ.clone();
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
                event.variables.insert(self.nid, Value::U64(self.buf.len() as u64));
                event.variables.insert(self.xid, $v);
            }};
        }
        if let Some(v) = from[0].update(ctx, event) {
            self.queue.push_back(v);
        }
        if let Some(v) = from[1].update(ctx, event) {
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
        self.etyp.check_contains(&from[0].typ)?;
        Type::Fn(self.mftyp.clone()).check_contains(&from[1].typ)?;
        self.pred.typecheck(ctx)?;
        Ok(())
    }
}

pub(super) struct Iter(BindId, ExprId);

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Iter {
    const NAME: &str = "iter";
    deftype!("fn(Array<'a>) -> 'a");

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
