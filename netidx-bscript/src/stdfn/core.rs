use crate::{
    deftype, err, errf,
    expr::{parser::parse_fn_type, Expr, ExprKind},
    node::{Node, NodeKind},
    stdfn::{CachedArgs, CachedVals, EvalCached},
    typ::{FnType, Refs, Type},
    Apply, ApplyTyped, BindId, BuiltIn, Ctx, Event, ExecCtx, InitFn,
};
use anyhow::bail;
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use netidx::{publisher::Typ, subscriber::Value};
use netidx_netproto::valarray::ValArray;
use smallvec::{smallvec, SmallVec};
use std::{
    fmt::Debug,
    sync::{Arc, LazyLock},
};

struct Any;

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for Any {
    const NAME: &str = "any";
    deftype!("fn(@args: Any) -> Any");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Any)))
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for Any {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        from.iter_mut()
            .filter_map(|s| s.update(ctx, event))
            .fold(None, |r, v| r.or(Some(v)))
    }
}

struct IsErr;

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for IsErr {
    const NAME: &str = "is_error";
    deftype!("fn(Any) -> bool");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(IsErr)))
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for IsErr {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        from[0].update(ctx, event).map(|v| match v {
            Value::Error(_) => Value::Bool(true),
            _ => Value::Bool(false),
        })
    }
}

struct FilterErr;

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for FilterErr {
    const NAME: &str = "filter_err";
    deftype!("fn(Any) -> error");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(FilterErr)))
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for FilterErr {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        from[0].update(ctx, event).and_then(|v| match v {
            v @ Value::Error(_) => Some(v),
            _ => None,
        })
    }
}

struct ToError;

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for ToError {
    const NAME: &str = "error";
    deftype!("fn(Any) -> error");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(ToError)))
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for ToError {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
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

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for Once {
    const NAME: &str = "once";
    deftype!("fn('a) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Once { val: false })))
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for Once {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
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

struct AllEv;

impl EvalCached for AllEv {
    const NAME: &str = "all";
    deftype!("fn(@args: Any) -> Any");

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct ArrayEv;

impl EvalCached for ArrayEv {
    const NAME: &str = "mkarray";
    deftype!("fn(@args: Any) -> Array<Any>");

    fn eval(from: &CachedVals) -> Option<Value> {
        if from.0.iter().all(|v| v.is_some()) {
            let iter = from.0.iter().map(|v| v.as_ref().unwrap().clone());
            Some(Value::Array(ValArray::from_iter_exact(iter)))
        } else {
            None
        }
    }
}

type Array = CachedArgs<ArrayEv>;

fn add_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) | (Some(_), None) => None,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l + r),
    }
}

struct SumEv;

impl EvalCached for SumEv {
    const NAME: &str = "sum";
    deftype!("fn(@args: Number) -> Number");

    fn eval(from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => add_vals(res, v.clone()),
        })
    }
}

type Sum = CachedArgs<SumEv>;

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
    deftype!("fn(@args: Number) -> Number");

    fn eval(from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => prod_vals(res, v.clone()),
        })
    }
}

type Product = CachedArgs<ProductEv>;

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
    deftype!("fn(@args: Number) -> Number");

    fn eval(from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => div_vals(res, v.clone()),
        })
    }
}

type Divide = CachedArgs<DivideEv>;

struct MinEv;

impl EvalCached for MinEv {
    const NAME: &str = "min";
    deftype!("fn(@args: Any) -> Any");

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct MaxEv;

impl EvalCached for MaxEv {
    const NAME: &str = "max";
    deftype!("fn(@args: Any) -> Any");

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct AndEv;

impl EvalCached for AndEv {
    const NAME: &str = "and";
    deftype!("fn(@args: bool) -> bool");

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct OrEv;

impl EvalCached for OrEv {
    const NAME: &str = "or";
    deftype!("fn(@args: bool) -> bool");

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct IndexEv;

impl EvalCached for IndexEv {
    const NAME: &str = "index";
    deftype!("fn(Array<'a>, i64) -> ['a, error]");

    fn eval(from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::Array(elts)), Some(Value::I64(i))) if *i >= 0 => {
                let i = *i as usize;
                if i < elts.len() {
                    Some(elts[i].clone())
                } else {
                    err!("array index out of bounds")
                }
            }
            (Some(Value::Array(elts)), Some(Value::I64(i))) if *i < 0 => {
                let len = elts.len();
                let i = *i;
                let i = len as i64 + i;
                if i > 0 {
                    Some(elts[i as usize].clone())
                } else {
                    err!("array index out of bounds")
                }
            }
            (None, _) | (_, None) => None,
            _ => err!("index(array, index): expected an array"),
        }
    }
}

type Index = CachedArgs<IndexEv>;

struct FilterEv;

impl EvalCached for FilterEv {
    const NAME: &str = "filter";
    deftype!("fn(bool, 'a) -> 'a");

    fn eval(from: &CachedVals) -> Option<Value> {
        let (pred, s) = (&from.0[0], &from.0[1]);
        match pred {
            None => None,
            Some(Value::Bool(true)) => s.clone(),
            Some(Value::Bool(false)) => None,
            _ => err!("filter(predicate, source) expected boolean predicate"),
        }
    }
}

type Filter = CachedArgs<FilterEv>;

struct Count {
    count: u64,
}

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for Count {
    const NAME: &str = "count";
    deftype!("fn(Any) -> u64");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Count { count: 0 })))
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for Count {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
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

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for Sample {
    const NAME: &str = "sample";
    deftype!("fn(Any, 'a) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Sample { last: None })))
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for Sample {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
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

struct MeanEv;

impl EvalCached for MeanEv {
    const NAME: &str = "mean";
    deftype!("fn([Number, Array<Number>], @args: [Number, Array<Number>]) -> f64");

    fn eval(from: &CachedVals) -> Option<Value> {
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

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for Uniq {
    const NAME: &str = "uniq";
    deftype!("fn('a) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Uniq(None))))
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for Uniq {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
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

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for Never {
    const NAME: &str = "never";
    deftype!("fn(@args: Any) -> _");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Never)))
    }
}

impl<C: Ctx, E: Debug + Clone> Apply<C, E> for Never {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        for n in from {
            n.update(ctx, event);
        }
        None
    }
}

struct Group<C: Ctx + 'static, E: Debug + Clone + 'static> {
    buf: SmallVec<[Value; 16]>,
    pred: Box<dyn ApplyTyped<C, E> + Send + Sync>,
    n: BindId,
    x: BindId,
    from: [Node<C, E>; 2],
}

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for Group<C, E> {
    const NAME: &str = "group";
    deftype!("fn('a, fn(u64, 'a) -> bool) -> Array<'a>");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|ctx, scope, from, top_id| match from {
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
                Ok(Box::new(Self { buf: smallvec![], pred, n, x, from }))
            }
            _ => bail!("expected a function"),
        })
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Apply<C, E> for Group<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if let Some(val) = from[0].update(ctx, event) {
            self.buf.push(val.clone());
            ctx.user.set_var(self.n, self.buf.len().into());
            ctx.user.set_var(self.x, val);
        }
        match self.pred.update(ctx, &mut self.from, event) {
            Some(Value::Bool(true)) => {
                Some(Value::Array(ValArray::from_iter_exact(self.buf.drain(..))))
            }
            _ => None,
        }
    }
}

struct Ungroup(BindId);

impl<C: Ctx, E: Debug + Clone> BuiltIn<C, E> for Ungroup {
    const NAME: &str = "ungroup";
    deftype!("fn(Array<'a>) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Ungroup(BindId::new()))))
    }
}

impl<C: Ctx + 'static, E: Debug + Clone + 'static> Apply<C, E> for Ungroup {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from[0].update(ctx, event) {
            Some(Value::Array(a)) => match &*a {
                [] => None,
                [hd, tl @ ..] => {
                    for v in tl {
                        ctx.user.set_var(self.0, v.clone());
                    }
                    Some(hd.clone())
                }
            },
            Some(v) => Some(v),
            None => match event {
                Event::Variable(id, v) if id == &self.0 => Some(v.clone()),
                _ => None,
            },
        }
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
                
    pub let all = |@args| 'all
    pub let and = |@args| 'and
    pub let any = |@args| 'any
    pub let mkarray = |@args| 'mkarray
    pub let count = |@args| 'count
    pub let divide = |@args| 'divide
    pub let filter_err = |e| 'filter_err
    pub let filter = |predicate, v| 'filter
    pub let group = |v, f| 'group
    pub let index = |a, i| 'index
    pub let is_err = |e| 'is_error
    pub let error = |e| 'error
    pub let max = |@args| 'max
    pub let mean = |@args| 'mean
    pub let min = |@args| 'min
    pub let never = |@args| 'never
    pub let once = |v| 'once
    pub let or = |@args| 'or
    pub let product = |@args| 'product
    pub let sample = |trigger, v| 'sample
    pub let sum = |@args| 'sum
    pub let ungroup = |a| 'ungroup
    pub let uniq = |v| 'uniq
}
"#;

pub fn register<C: Ctx, E: Debug + Clone>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<All>();
    ctx.register_builtin::<And>();
    ctx.register_builtin::<Any>();
    ctx.register_builtin::<Array>();
    ctx.register_builtin::<Count>();
    ctx.register_builtin::<Divide>();
    ctx.register_builtin::<Filter>();
    ctx.register_builtin::<FilterErr>();
    ctx.register_builtin::<Group<C, E>>();
    ctx.register_builtin::<Index>();
    ctx.register_builtin::<IsErr>();
    ctx.register_builtin::<Max>();
    ctx.register_builtin::<Mean>();
    ctx.register_builtin::<Min>();
    ctx.register_builtin::<Never>();
    ctx.register_builtin::<Once>();
    ctx.register_builtin::<Or>();
    ctx.register_builtin::<Product>();
    ctx.register_builtin::<Sample>();
    ctx.register_builtin::<Sum>();
    ctx.register_builtin::<Uniq>();
    ctx.register_builtin::<ToError>();
    MOD.parse().unwrap()
}
