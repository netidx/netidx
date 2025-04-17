use crate::{
    deftype, err, errf,
    expr::Expr,
    node::{gen, Node},
    stdfn::{CachedArgs, CachedVals, EvalCached},
    Apply, BindId, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, UserEvent,
};
use anyhow::bail;
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use netidx::subscriber::Value;
use std::sync::Arc;
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

#[derive(Default)]
struct IndexEv;

impl EvalCached for IndexEv {
    const NAME: &str = "index";
    deftype!("fn(Array<'a>, Int) -> ['a, error]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        let i = match &from.0[1] {
            Some(Value::I64(i)) => *i,
            Some(v) => match v.clone().cast_to::<i64>() {
                Ok(i) => i,
                Err(_) => return err!("op::index(array, index): expected an integer"),
            },
            None => return None,
        };
        match &from.0[0] {
            Some(Value::Array(elts)) if i >= 0 => {
                let i = i as usize;
                if i < elts.len() {
                    Some(elts[i].clone())
                } else {
                    err!("array index out of bounds")
                }
            }
            Some(Value::Array(elts)) if i < 0 => {
                let len = elts.len();
                let i = len as i64 + i;
                if i > 0 {
                    Some(elts[i as usize].clone())
                } else {
                    err!("array index out of bounds")
                }
            }
            None => None,
            _ => err!("op::index(array, index): expected an array"),
        }
    }
}

type Index = CachedArgs<IndexEv>;

#[derive(Default)]
struct SliceEv;

impl EvalCached for SliceEv {
    const NAME: &str = "slice";
    deftype!("fn(Array<'a>, [Int, null], [Int, null]) -> [Array<'a>, error]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        macro_rules! number {
            ($e:expr) => {
                match $e.clone().cast_to::<usize>() {
                    Ok(i) => i,
                    Err(_) => return err!("expected a positive number"),
                }
            };
        }
        let (start, end) = match (&from.0[1], &from.0[2]) {
            (None, _) | (_, None) => return None,
            (Some(v0), Some(v1)) => match (v0, v1) {
                (Value::Null, Value::Null) => (None, None),
                (Value::Null, Value::U64(i)) => (None, Some(*i as usize)),
                (Value::Null, v) => (None, Some(number!(v))),
                (Value::U64(i), Value::Null) => (Some(*i as usize), None),
                (v, Value::Null) => (Some(number!(v)), None),
                (Value::U64(v0), Value::U64(v1)) => {
                    (Some(*v0 as usize), Some(*v1 as usize))
                }
                (v0, v1) => (Some(number!(v0)), Some(number!(v1))),
            },
        };
        match &from.0[0] {
            Some(Value::Array(elts)) => match (start, end) {
                (None, None) => Some(Value::Array(elts.clone())),
                (Some(i), Some(j)) => match elts.subslice(i..j) {
                    Ok(a) => Some(Value::Array(a)),
                    Err(e) => Some(Value::Error(e.to_string().into())),
                },
                (Some(i), None) => match elts.subslice(i..) {
                    Ok(a) => Some(Value::Array(a)),
                    Err(e) => Some(Value::Error(e.to_string().into())),
                },
                (None, Some(i)) => match elts.subslice(..i) {
                    Ok(a) => Some(Value::Array(a)),
                    Err(e) => Some(Value::Error(e.to_string().into())),
                },
            },
            Some(_) => err!("expected array"),
            None => None,
        }
    }
}

type Slice = CachedArgs<SliceEv>;

struct Filter<C: Ctx, E: UserEvent> {
    pred: Node<C, E>,
    fid: BindId,
    x: BindId,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Filter<C, E> {
    const NAME: &str = "filter";
    deftype!("fn('a, fn('a) -> bool) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, typ, scope, from, top_id| match from {
            [arg, fnode] => {
                let (x, xn) = gen::bind(ctx, scope, "x", arg.typ.clone(), top_id);
                let fid = BindId::new();
                let fnode = gen::reference(ctx, fid, fnode.typ.clone(), top_id);
                let typ = TArc::new(typ.clone());
                let pred = gen::apply(fnode, vec![xn], typ, top_id);
                Ok(Box::new(Self { pred, fid, x }))
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
        if let Some(v) = from[1].update(ctx, event) {
            event.variables.insert(self.fid, v);
        }
        match from[0].update(ctx, event) {
            None => {
                self.pred.update(ctx, event);
                None
            }
            Some(v) => {
                event.variables.insert(self.x, v.clone());
                match self.pred.update(ctx, event) {
                    Some(Value::Bool(true)) => Some(v),
                    _ => None,
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
            n.typecheck(ctx)?;
        }
        self.pred.typecheck(ctx)
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
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Sample {
    const NAME: &str = "sample";
    deftype!("fn(Any, 'a) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(Sample { last: None })))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Sample {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        match from {
            [trigger, source] => {
                if let Some(v) = source.update(ctx, event) {
                    self.last = Some(v);
                }
                trigger.update(ctx, event).and_then(|_| self.last.clone())
            }
            _ => unreachable!(),
        }
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

const MOD: &str = r#"
pub mod core {
    type Sint = [ i32, z32, i64, z64 ]
    type Uint = [ u32, v32, u64, v64 ]
    type Int = [ Sint, Uint ]
    type Real = [ f32, f64, decimal ]
    type Number = [ Int, Real ]

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
    ]

    pub mod op {
        let index = |a, i| 'index
        let slice = |a, i, j| 'slice
    }

    pub mod array {
        pub let filter = |a, f| 'array_filter
        pub let filter_map = |a, f| 'array_filter_map
        pub let map = |a, f| 'array_map
        pub let flat_map = |a, f| 'array_flat_map
        pub let fold = |a, init, f| 'array_fold
        pub let group = |v, f| 'group
        pub let iter = |a| 'iter
        pub let len = |a| 'array_len
        pub let concat = |x, @args| 'array_concat
        pub let flatten = |a| 'array_flatten
        pub let find = |a, f| 'array_find
        pub let find_map = |a, f| 'array_find_map
    }

    pub let all = |@args| 'all
    pub let and = |@args| 'and
    pub let count = |x| 'count
    pub let divide = |@args| 'divide
    pub let filter_err = |e| 'filter_err
    pub let filter = |v, f| 'filter
    pub let is_err = |e| 'is_err
    pub let error = |e| 'error
    pub let max = |@args| 'max
    pub let mean = |v, @args| 'mean
    pub let min = |@args| 'min
    pub let never = |@args| 'never
    pub let once = |v| 'once
    pub let or = |@args| 'or
    pub let product = |@args| 'product
    pub let sample = |trigger, v| 'sample
    pub let sum = |@args| 'sum
    pub let uniq = |v| 'uniq

    pub let errors: error = never()
}
"#;

pub fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<All>().unwrap();
    ctx.register_builtin::<And>().unwrap();
    ctx.register_builtin::<Count>().unwrap();
    ctx.register_builtin::<Divide>().unwrap();
    ctx.register_builtin::<array::Concat>().unwrap();
    ctx.register_builtin::<array::Len>().unwrap();
    ctx.register_builtin::<array::Flatten>().unwrap();
    ctx.register_builtin::<Filter<C, E>>().unwrap();
    ctx.register_builtin::<array::Filter<C, E>>().unwrap();
    ctx.register_builtin::<array::FlatMap<C, E>>().unwrap();
    ctx.register_builtin::<array::Find<C, E>>().unwrap();
    ctx.register_builtin::<array::FindMap<C, E>>().unwrap();
    ctx.register_builtin::<array::Map<C, E>>().unwrap();
    ctx.register_builtin::<array::Fold<C, E>>().unwrap();
    ctx.register_builtin::<array::FilterMap<C, E>>().unwrap();
    ctx.register_builtin::<FilterErr>().unwrap();
    ctx.register_builtin::<array::Group<C, E>>().unwrap();
    ctx.register_builtin::<Index>().unwrap();
    ctx.register_builtin::<Slice>().unwrap();
    ctx.register_builtin::<IsErr>().unwrap();
    ctx.register_builtin::<Max>().unwrap();
    ctx.register_builtin::<Mean>().unwrap();
    ctx.register_builtin::<Min>().unwrap();
    ctx.register_builtin::<Never>().unwrap();
    ctx.register_builtin::<Once>().unwrap();
    ctx.register_builtin::<Or>().unwrap();
    ctx.register_builtin::<Product>().unwrap();
    ctx.register_builtin::<Sample>().unwrap();
    ctx.register_builtin::<Sum>().unwrap();
    ctx.register_builtin::<Uniq>().unwrap();
    ctx.register_builtin::<array::Iter>().unwrap();
    ctx.register_builtin::<ToError>().unwrap();
    MOD.parse().unwrap()
}
