use crate::{
    deftype, err, errf,
    expr::{Expr, ExprId},
    node::{genn, Node},
    stdfn::{CachedArgs, CachedVals, EvalCached},
    typ::{FnType, NoRefs},
    Apply, BindId, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, UserEvent,
};
use anyhow::bail;
use arcstr::{literal, ArcStr};
use combine::stream::position::SourcePosition;
use compact_str::format_compact;
use netidx::subscriber::Value;
use std::{collections::VecDeque, sync::Arc};
use triomphe::Arc as TArc;

pub mod array;

struct IsErr;

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for IsErr {
    const NAME: &str = "is_err";
    deftype!("fn(Any) -> bool");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(IsErr)))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for IsErr {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        from[0].update(ctx, event).map(|v| match v {
            Value::Error(_) => Value::Bool(true),
            _ => Value::Bool(false),
        })
    }
}

struct FilterErr;

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for FilterErr {
    const NAME: &str = "filter_err";
    deftype!("fn(Any) -> error");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(FilterErr)))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for FilterErr {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        from[0].update(ctx, event).and_then(|v| match v {
            v @ Value::Error(_) => Some(v),
            _ => None,
        })
    }
}

struct ToError;

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for ToError {
    const NAME: &str = "error";
    deftype!("fn(Any) -> error");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(ToError)))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for ToError {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        from[0].update(ctx, event).map(|v| match v.cast_to::<ArcStr>() {
            Ok(s) => Value::Error(s),
            Err(e) => Value::Error(format_compact!("{e}").as_str().into()),
        })
    }
}

struct Once {
    val: bool,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Once {
    const NAME: &str = "once";
    deftype!("fn('a) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(Once { val: false })))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Once {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        match from {
            [s] => s.update(ctx, event).and_then(|v| {
                if self.val {
                    None
                } else {
                    self.val = true;
                    Some(v)
                }
            }),
            _ => None,
        }
    }
}

#[derive(Default)]
struct AllEv;

impl EvalCached for AllEv {
    const NAME: &str = "all";
    deftype!("fn(@args: Any) -> Any");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [] => None,
            [hd, tl @ ..] => match hd {
                None => None,
                v @ Some(_) => {
                    if tl.into_iter().all(|v1| v1 == v) {
                        v.clone()
                    } else {
                        None
                    }
                }
            },
        }
    }
}

type All = CachedArgs<AllEv>;

fn add_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) | (Some(_), None) => None,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l + r),
    }
}

#[derive(Default)]
struct SumEv;

impl EvalCached for SumEv {
    const NAME: &str = "sum";
    deftype!("fn(@args: [Number, Array<[Number, Array<Number>]>]) -> Number");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => add_vals(res, v.clone()),
        })
    }
}

type Sum = CachedArgs<SumEv>;

#[derive(Default)]
struct ProductEv;

fn prod_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) | (Some(_), None) => None,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l * r),
    }
}

impl EvalCached for ProductEv {
    const NAME: &str = "product";
    deftype!("fn(@args: [Number, Array<[Number, Array<Number>]>]) -> Number");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => prod_vals(res, v.clone()),
        })
    }
}

type Product = CachedArgs<ProductEv>;

#[derive(Default)]
struct DivideEv;

fn div_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) | (Some(_), None) => None,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l / r),
    }
}

impl EvalCached for DivideEv {
    const NAME: &str = "divide";
    deftype!("fn(@args: [Number, Array<[Number, Array<Number>]>]) -> Number");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => div_vals(res, v.clone()),
        })
    }
}

type Divide = CachedArgs<DivideEv>;

#[derive(Default)]
struct MinEv;

impl EvalCached for MinEv {
    const NAME: &str = "min";
    deftype!("fn(@args: Any) -> Any");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        let mut res = None;
        for v in from.flat_iter() {
            match (res, v) {
                (None, None) | (Some(_), None) => return None,
                (None, Some(v)) => {
                    res = Some(v);
                }
                (Some(v0), Some(v)) => {
                    res = if v < v0 { Some(v) } else { Some(v0) };
                }
            }
        }
        res
    }
}

type Min = CachedArgs<MinEv>;

#[derive(Default)]
struct MaxEv;

impl EvalCached for MaxEv {
    const NAME: &str = "max";
    deftype!("fn(@args: Any) -> Any");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        let mut res = None;
        for v in from.flat_iter() {
            match (res, v) {
                (None, None) | (Some(_), None) => return None,
                (None, Some(v)) => {
                    res = Some(v);
                }
                (Some(v0), Some(v)) => {
                    res = if v > v0 { Some(v) } else { Some(v0) };
                }
            }
        }
        res
    }
}

type Max = CachedArgs<MaxEv>;

#[derive(Default)]
struct AndEv;

impl EvalCached for AndEv {
    const NAME: &str = "and";
    deftype!("fn(@args: bool) -> bool");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        let mut res = Some(Value::Bool(true));
        for v in from.flat_iter() {
            match v {
                None => return None,
                Some(Value::Bool(true)) => (),
                Some(_) => {
                    res = Some(Value::Bool(false));
                }
            }
        }
        res
    }
}

type And = CachedArgs<AndEv>;

#[derive(Default)]
struct OrEv;

impl EvalCached for OrEv {
    const NAME: &str = "or";
    deftype!("fn(@args: bool) -> bool");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        let mut res = Some(Value::Bool(false));
        for v in from.flat_iter() {
            match v {
                None => return None,
                Some(Value::Bool(true)) => {
                    res = Some(Value::Bool(true));
                }
                Some(_) => (),
            }
        }
        res
    }
}

type Or = CachedArgs<OrEv>;

struct Filter<C: Ctx, E: UserEvent> {
    ready: bool,
    queue: VecDeque<Value>,
    pred: Node<C, E>,
    typ: TArc<FnType<NoRefs>>,
    top_id: ExprId,
    fid: BindId,
    x: BindId,
    out: BindId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Filter<C, E> {
    const NAME: &str = "filter";
    deftype!("fn('a, fn('a) -> bool) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, typ, scope, from, top_id| match from {
            [arg, fnode] => {
                let (x, xn) = genn::bind(ctx, scope, "x", arg.typ.clone(), top_id);
                let fid = BindId::new();
                let fnode = genn::reference(ctx, fid, fnode.typ.clone(), top_id);
                let typ = TArc::new(typ.clone());
                let pred = genn::apply(fnode, vec![xn], typ.clone(), top_id);
                let queue = VecDeque::new();
                let out = BindId::new();
                ctx.user.ref_var(out, top_id);
                Ok(Box::new(Self { ready: true, queue, pred, typ, fid, x, out, top_id }))
            }
            _ => bail!("expected two arguments"),
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Filter<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        macro_rules! set {
            ($v:expr) => {{
                self.ready = false;
                event.variables.insert(self.x, $v);
            }};
        }
        macro_rules! maybe_cont {
            () => {{
                if let Some(v) = self.queue.front().cloned() {
                    set!(v);
                    continue;
                }
                break;
            }};
        }
        if let Some(v) = from[0].update(ctx, event) {
            self.queue.push_back(v);
        }
        if let Some(v) = from[1].update(ctx, event) {
            ctx.cached.insert(self.fid, v.clone());
            event.variables.insert(self.fid, v);
        }
        if self.ready && self.queue.len() > 0 {
            let v = self.queue.front().unwrap().clone();
            set!(v);
        }
        loop {
            match self.pred.update(ctx, event) {
                None => break,
                Some(v) => {
                    self.ready = true;
                    match v {
                        Value::Bool(true) => {
                            ctx.user.set_var(self.out, self.queue.pop_front().unwrap());
                            maybe_cont!();
                        }
                        _ => {
                            let _ = self.queue.pop_front();
                            maybe_cont!();
                        }
                    }
                }
            }
        }
        event.variables.get(&self.out).map(|v| v.clone())
    }

    fn typecheck(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
    ) -> anyhow::Result<()> {
        for n in from.iter_mut() {
            n.typecheck(ctx)?;
        }
        self.typ.args[0].typ.check_contains(&from[0].typ)?;
        self.typ.args[1].typ.check_contains(&from[1].typ)?;
        self.pred.typecheck(ctx)
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.out, self.top_id)
    }
}

struct Queue {
    triggered: usize,
    queue: VecDeque<Value>,
    id: BindId,
    top_id: ExprId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Queue {
    const NAME: &str = "queue";
    deftype!("fn(#trigger:Any, 'a) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, _, _, from, top_id| match from {
            [_, _] => {
                let id = BindId::new();
                ctx.user.ref_var(id, top_id);
                Ok(Box::new(Self { triggered: 0, queue: VecDeque::new(), id, top_id }))
            }
            _ => bail!("expected two arguments"),
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Queue {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        if from[0].update(ctx, event).is_some() {
            self.triggered += 1;
        }
        if let Some(v) = from[1].update(ctx, event) {
            self.queue.push_back(v);
        }
        while self.triggered > 0 && self.queue.len() > 0 {
            self.triggered -= 1;
            ctx.user.set_var(self.id, self.queue.pop_front().unwrap());
        }
        event.variables.get(&self.id).cloned()
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.id, self.top_id);
    }
}

struct Seq {
    id: BindId,
    top_id: ExprId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Seq {
    const NAME: &str = "seq";
    deftype!("fn(u64) -> u64");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, _, _, from, top_id| match from {
            [_] => {
                let id = BindId::new();
                ctx.user.ref_var(id, top_id);
                Ok(Box::new(Self { id, top_id }))
            }
            _ => bail!("expected one argument"),
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Seq {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        if let Some(Value::U64(i)) = from[0].update(ctx, event) {
            for i in 0..i {
                ctx.user.set_var(self.id, Value::U64(i));
            }
        }
        event.variables.get(&self.id).cloned()
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.id, self.top_id);
    }
}

struct Count {
    count: u64,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Count {
    const NAME: &str = "count";
    deftype!("fn(Any) -> u64");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(Count { count: 0 })))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Count {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        if from.into_iter().fold(false, |u, n| u || n.update(ctx, event).is_some()) {
            self.count += 1;
            Some(Value::U64(self.count))
        } else {
            None
        }
    }
}

struct Sample {
    last: Option<Value>,
    triggered: usize,
    id: BindId,
    top_id: ExprId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Sample {
    const NAME: &str = "sample";
    deftype!("fn(#trigger:Any, 'a) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, _, _, _, top_id| {
            let id = BindId::new();
            ctx.user.ref_var(id, top_id);
            Ok(Box::new(Sample { last: None, triggered: 0, id, top_id }))
        })
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Sample {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        if let Some(_) = from[0].update(ctx, event) {
            self.triggered += 1;
        }
        if let Some(v) = from[1].update(ctx, event) {
            self.last = Some(v);
        }
        let var = event.variables.get(&self.id).cloned();
        let res = if self.triggered > 0 && self.last.is_some() && var.is_none() {
            self.triggered -= 1;
            self.last.clone()
        } else {
            var
        };
        while self.triggered > 0 && self.last.is_some() {
            self.triggered -= 1;
            ctx.user.set_var(self.id, self.last.clone().unwrap());
        }
        res
    }

    fn delete(&mut self, ctx: &mut ExecCtx<C, E>) {
        ctx.user.unref_var(self.id, self.top_id)
    }
}

#[derive(Default)]
struct MeanEv;

impl EvalCached for MeanEv {
    const NAME: &str = "mean";
    deftype!("fn([Number, Array<Number>], @args: [Number, Array<Number>]) -> f64");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        let mut total = 0.;
        let mut samples = 0;
        let mut error = None;
        for v in from.flat_iter() {
            if let Some(v) = v {
                match v.cast_to::<f64>() {
                    Err(e) => error = errf!("{e:?}"),
                    Ok(v) => {
                        total += v;
                        samples += 1;
                    }
                }
            }
        }
        if let Some(e) = error {
            Some(e)
        } else if samples == 0 {
            err!("mean requires at least one argument")
        } else {
            Some(Value::F64(total / samples as f64))
        }
    }
}

type Mean = CachedArgs<MeanEv>;

struct Uniq(Option<Value>);

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Uniq {
    const NAME: &str = "uniq";
    deftype!("fn('a) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(Uniq(None))))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Uniq {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        match from {
            [e] => e.update(ctx, event).and_then(|v| {
                if Some(&v) != self.0.as_ref() {
                    self.0 = Some(v.clone());
                    Some(v)
                } else {
                    None
                }
            }),
            _ => unreachable!(),
        }
    }
}

struct Never;

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Never {
    const NAME: &str = "never";
    deftype!("fn(@args: Any) -> _");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(Never)))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Never {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        for n in from {
            n.update(ctx, event);
        }
        None
    }
}

struct Dbg(SourcePosition);

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Dbg {
    const NAME: &str = "dbg";
    deftype!("fn('a) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, from, _| Ok(Box::new(Dbg(from[0].spec.pos))))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Dbg {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        from[0].update(ctx, event).map(|v| {
            eprintln!("{}: {v}", self.0);
            v
        })
    }
}

const MOD: &str = r#"
pub mod core {
    type Sint = [ i32, z32, i64, z64 ];
    type Uint = [ u32, v32, u64, v64 ];
    type Int = [ Sint, Uint ];
    type Float = [ f32, f64 ];
    type Real = [ Float, decimal ];
    type Number = [ Int, Real ];

    type Any = [
        Number,
        datetime,
        duration,
        bool,
        string,
        bytes,
        error,
        array,
        null
    ];

    pub mod array {
        /// filter returns a new array containing only elements where f returned true
        pub let filter = |a, f| 'array_filter;

        /// filter_map returns a new array containing the outputs of f
        /// that were not null
        pub let filter_map = |a, f| 'array_filter_map;

        /// return a new array where each element is the output of f applied to the
        /// corresponding element in a
        pub let map = |a, f| 'array_map;

        /// return a new array where each element is the output of f applied to the
        /// corresponding element in a, except that if f returns an array then it's
        /// elements will be concatanated to the end of the output instead of nesting.
        pub let flat_map = |a, f| 'array_flat_map;

        /// return the result of f applied to the init and every element of a in
        /// sequence. f(f(f(init, a[0]), a[1]), ...)
        pub let fold = |a, init, f| 'array_fold;

        /// each time v updates group places the value of v in an internal buffer
        /// and calls f with the length of the internal buffer and the value of v.
        /// If f returns true then group returns the internal buffer as an array
        /// otherwise group returns nothing.
        pub let group = |v, f| 'group;

        /// iter produces an update for every value in the array a. updates are produced
        /// in the order they appear in a.
        pub let iter = |a| 'iter;

        /// iterq produces updates for each value in a, but it only produces an update when
        /// trigger updates. If trigger does not update but a does, then iterq will store each a
        /// in an internal fifo queue. If trigger updates but a does not, iterq will record the
        /// number of times it was triggered, and will update immediatly that many times when a
        /// updates.
        pub let iterq = |#trigger, a| 'iterq;

        /// returns the length of a
        pub let len = |a| 'array_len;

        /// concatenates the first array with the scalar values or arrays subsuquently passed.
        /// returns an array containing all the values of all it's arguments
        pub let concat = |x, @args| 'array_concat;

        /// flatten takes an array with two levels of nesting and produces a flat array
        /// with all the nested elements concatenated together.
        pub let flatten = |a| 'array_flatten;

        /// applies f to every element in a and returns the first element for which f
        /// returns true, or null if no element returns true
        pub let find = |a, f| 'array_find;

        /// applies f to every element in a and returns the first non null output of f
        pub let find_map = |a, f| 'array_find_map;

        /// return a new copy of a sorted descending
        pub let sort = |a| 'array_sort
    };

    /// return the first argument when all arguments are equal, otherwise return nothing
    pub let all = |@args| 'all;

    /// return true if all arguments are true, otherwise return false
    pub let and = |@args| 'and;

    /// return the number of times x has updated
    pub let count = |x| 'count;

    /// return the first argument divided by all subsuquent arguments
    pub let divide = |@args| 'divide;

    /// return e only if e is an error
    pub let filter_err = |e| 'filter_err;

    /// return v if f(v) is true, otherwise return nothing
    pub let filter = |v, f| 'filter;

    /// return true if e is an error
    pub let is_err = |e| 'is_err;

    /// construct an error from the specified string
    pub let error = |e| 'error;

    /// return the maximum value of any argument
    pub let max = |@args| 'max;

    /// return the mean of the passed in arguments
    pub let mean = |v, @args| 'mean;

    /// return the minimum value of any argument
    pub let min = |@args| 'min;

    /// return v only once, subsuquent updates to v will be ignored
    /// and once will return nothing
    pub let once = |v| 'once;

    /// seq will update i times from 0 to i - 1 in that order.
    pub let seq = |i| 'seq;

    /// return true if any argument is true
    pub let or = |@args| 'or;

    /// return the product of all arguments
    pub let product = |@args| 'product;

    /// When v updates it's value will be cached internally. When trigger updates
    /// the cached value of v will be returned.
    pub let sample = |#trigger, v| 'sample;

    /// return the sum of all arguments
    pub let sum = |@args| 'sum;

    /// when v updates return v if the new value is different from the previous value,
    /// otherwise return nothing.
    pub let uniq = |v| 'uniq;

    /// when v updates place it's value in an internal fifo queue. when trigger updates
    /// return the oldest value from the fifo queue. If trigger updates and the queue is
    /// empty, record the number of trigger updates, and produce that number of
    /// values from the queue when they are available.
    pub let queue = |#trigger, v| 'queue;

    /// ignore updates to any argument and never return anything
    pub let never = |@args| 'never;

    /// when v updates, return it, but also print it along with the position of the expression
    pub let dbg = |v| 'dbg;

    /// This is the toplevel error sink for the ? operator. If no other lexical binding of errors
    /// exists closer to the error site then errors handled by ? will come here.
    pub let errors: error = never()
}
"#;

pub fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<Queue>().unwrap();
    ctx.register_builtin::<All>().unwrap();
    ctx.register_builtin::<And>().unwrap();
    ctx.register_builtin::<Count>().unwrap();
    ctx.register_builtin::<Divide>().unwrap();
    ctx.register_builtin::<Filter<C, E>>().unwrap();
    ctx.register_builtin::<array::Concat>().unwrap();
    ctx.register_builtin::<array::Len>().unwrap();
    ctx.register_builtin::<array::Flatten>().unwrap();
    ctx.register_builtin::<array::Filter<C, E>>().unwrap();
    ctx.register_builtin::<array::FlatMap<C, E>>().unwrap();
    ctx.register_builtin::<array::Find<C, E>>().unwrap();
    ctx.register_builtin::<array::FindMap<C, E>>().unwrap();
    ctx.register_builtin::<array::Map<C, E>>().unwrap();
    ctx.register_builtin::<array::Fold<C, E>>().unwrap();
    ctx.register_builtin::<array::FilterMap<C, E>>().unwrap();
    ctx.register_builtin::<array::IterQ>().unwrap();
    ctx.register_builtin::<array::Group<C, E>>().unwrap();
    ctx.register_builtin::<array::Sort>().unwrap();
    ctx.register_builtin::<FilterErr>().unwrap();
    ctx.register_builtin::<IsErr>().unwrap();
    ctx.register_builtin::<Max>().unwrap();
    ctx.register_builtin::<Mean>().unwrap();
    ctx.register_builtin::<Min>().unwrap();
    ctx.register_builtin::<Never>().unwrap();
    ctx.register_builtin::<Once>().unwrap();
    ctx.register_builtin::<Seq>().unwrap();
    ctx.register_builtin::<Or>().unwrap();
    ctx.register_builtin::<Product>().unwrap();
    ctx.register_builtin::<Sample>().unwrap();
    ctx.register_builtin::<Sum>().unwrap();
    ctx.register_builtin::<Uniq>().unwrap();
    ctx.register_builtin::<array::Iter>().unwrap();
    ctx.register_builtin::<ToError>().unwrap();
    ctx.register_builtin::<Dbg>().unwrap();
    MOD.parse().unwrap()
}
