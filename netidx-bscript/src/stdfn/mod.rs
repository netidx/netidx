use crate::{
    expr::{Expr, ModPath},
    vm::{Apply, Arity, Ctx, Event, ExecCtx, Init, InitFn, Node},
};
use anyhow::Result;
use netidx::{
    chars::Chars,
    subscriber::{Typ, Value},
};
use netidx_core::utils::Either;
use smallvec::SmallVec;
use std::{iter, marker::PhantomData, sync::Arc};

mod net;
mod str;
mod time;

#[macro_export]
macro_rules! errf {
    ($pat:expr, $($arg:expr),*) => { Some(Value::Error(Chars::from(format!($pat, $($arg),*)))) };
    ($pat:expr) => { Some(Value::Error(Chars::from(format!($pat)))) };
}

#[macro_export]
macro_rules! err {
    ($pat:expr) => {
        Some(Value::Error(Chars::from($pat)))
    };
}

#[macro_export]
macro_rules! arity1 {
    ($from:expr, $updates:expr) => {
        match (&*$from, &*$updates) {
            ([arg], [arg_up]) => (arg, arg_up),
            (_, _) => unreachable!(),
        }
    };
}

#[macro_export]
macro_rules! arity2 {
    ($from:expr, $updates:expr) => {
        match (&*$from, &*$updates) {
            ([arg0, arg1], [arg0_up, arg1_up]) => ((arg0, arg1), (arg0_up, arg1_up)),
            (_, _) => unreachable!(),
        }
    };
}

pub struct CachedVals(pub SmallVec<[Option<Value>; 4]>);

impl CachedVals {
    pub fn new<C: Ctx, E: Clone>(from: &[Node<C, E>]) -> CachedVals {
        CachedVals(from.into_iter().map(|_| None).collect())
    }

    pub fn update<C: Ctx, E: Clone>(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> bool {
        from.into_iter().enumerate().fold(false, |res, (i, src)| {
            match src.update(ctx, event) {
                None => res,
                v @ Some(_) => {
                    self.0[i] = v;
                    true
                }
            }
        })
    }

    pub fn update_diff<C: Ctx, E: Clone>(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> SmallVec<[bool; 4]> {
        from.into_iter()
            .enumerate()
            .map(|(i, src)| match src.update(ctx, event) {
                None => false,
                v @ Some(_) => {
                    self.0[i] = v;
                    true
                }
            })
            .collect()
    }

    pub fn flat_iter<'a>(&'a self) -> impl Iterator<Item = Option<Value>> + 'a {
        self.0.iter().flat_map(|v| match v {
            None => Either::Left(iter::once(None)),
            Some(v) => Either::Right(v.clone().flatten().map(Some)),
        })
    }
}

pub struct Any;

impl<C: Ctx, E: Clone> Init<C, E> for Any {
    const NAME: &str = "any";
    const ARITY: Arity = Arity::Any;

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Any)))
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Any {
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

pub struct Once {
    val: bool,
}

impl<C: Ctx, E: Clone> Init<C, E> for Once {
    const NAME: &str = "once";
    const ARITY: Arity = Arity::Any;

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Once { val: false })))
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Once {
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

pub trait EvalCached {
    const NAME: &str;
    const ARITY: Arity;

    fn eval(from: &CachedVals) -> Option<Value>;
}

pub struct CachedArgs<T: EvalCached + Send + Sync> {
    cached: CachedVals,
    t: PhantomData<T>,
}

impl<C: Ctx, E: Clone, T: EvalCached + Send + Sync + 'static> Init<C, E>
    for CachedArgs<T>
{
    const NAME: &str = T::NAME;
    const ARITY: Arity = T::ARITY;

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, from, _, _| {
            let t = CachedArgs::<T> { cached: CachedVals::new(from), t: PhantomData };
            Ok(Box::new(t))
        })
    }
}

impl<C: Ctx, E: Clone, T: EvalCached + Send + Sync + 'static> Apply<C, E>
    for CachedArgs<T>
{
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if self.cached.update(ctx, from, event) {
            T::eval(&self.cached)
        } else {
            None
        }
    }
}

pub struct AllEv;

impl EvalCached for AllEv {
    const NAME: &str = "all";
    const ARITY: Arity = Arity::Any;

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

pub type All = CachedArgs<AllEv>;

pub struct ArrayEv;

impl EvalCached for ArrayEv {
    const NAME: &str = "array";
    const ARITY: Arity = Arity::Any;

    fn eval(from: &CachedVals) -> Option<Value> {
        if from.0.iter().all(|v| v.is_some()) {
            Some(Value::Array(Arc::from_iter(from.0.iter().filter_map(|v| v.clone()))))
        } else {
            None
        }
    }
}

pub type Array = CachedArgs<ArrayEv>;

fn add_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) | (Some(_), None) => None,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l + r),
    }
}

pub struct SumEv;

impl EvalCached for SumEv {
    const NAME: &str = "sum";
    const ARITY: Arity = Arity::Any;

    fn eval(from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => add_vals(res, v.clone()),
        })
    }
}

pub type Sum = CachedArgs<SumEv>;

pub struct ProductEv;

fn prod_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) | (Some(_), None) => None,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l * r),
    }
}

impl EvalCached for ProductEv {
    const NAME: &str = "product";
    const ARITY: Arity = Arity::Any;

    fn eval(from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => prod_vals(res, v.clone()),
        })
    }
}

pub type Product = CachedArgs<ProductEv>;

pub struct DivideEv;

fn div_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) | (Some(_), None) => None,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l / r),
    }
}

impl EvalCached for DivideEv {
    const NAME: &str = "divide";
    const ARITY: Arity = Arity::Any;

    fn eval(from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => div_vals(res, v.clone()),
        })
    }
}

pub type Divide = CachedArgs<DivideEv>;

pub struct MinEv;

impl EvalCached for MinEv {
    const NAME: &str = "min";
    const ARITY: Arity = Arity::Any;

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

pub type Min = CachedArgs<MinEv>;

pub struct MaxEv;

impl EvalCached for MaxEv {
    const NAME: &str = "max";
    const ARITY: Arity = Arity::Any;

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

pub type Max = CachedArgs<MaxEv>;

pub struct AndEv;

impl EvalCached for AndEv {
    const NAME: &str = "and";
    const ARITY: Arity = Arity::Any;

    fn eval(from: &CachedVals) -> Option<Value> {
        let mut res = Some(Value::True);
        for v in from.flat_iter() {
            match v {
                None => return None,
                Some(Value::True) => (),
                Some(_) => {
                    res = Some(Value::False);
                }
            }
        }
        res
    }
}

pub type And = CachedArgs<AndEv>;

pub struct OrEv;

impl EvalCached for OrEv {
    const NAME: &str = "or";
    const ARITY: Arity = Arity::Any;

    fn eval(from: &CachedVals) -> Option<Value> {
        let mut res = Some(Value::False);
        for v in from.flat_iter() {
            match v {
                None => return None,
                Some(Value::True) => {
                    res = Some(Value::True);
                }
                Some(_) => (),
            }
        }
        res
    }
}

pub type Or = CachedArgs<OrEv>;

pub struct NotEv;

impl EvalCached for NotEv {
    const NAME: &str = "not";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        from.0[0].as_ref().map(|v| !(v.clone()))
    }
}

pub type Not = CachedArgs<NotEv>;

pub struct IsErrEv;

impl EvalCached for IsErrEv {
    const NAME: &str = "is_error";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        from.0[0].as_ref().map(|v| match v {
            Value::Error(_) => Value::True,
            _ => Value::False,
        })
    }
}

pub type IsErr = CachedArgs<IsErrEv>;

pub struct IndexEv;

impl EvalCached for IndexEv {
    const NAME: &str = "index";
    const ARITY: Arity = Arity::Exactly(2);

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
            (None, _) | (_, None) => None,
            _ => err!("index(array, index): expected an array and a positive index"),
        }
    }
}

pub type Index = CachedArgs<IndexEv>;

pub struct CmpEv;

impl EvalCached for CmpEv {
    const NAME: &str = "cmp";
    const ARITY: Arity = Arity::Exactly(3);

    fn eval(from: &CachedVals) -> Option<Value> {
        let (op, v0, v1) = (&from.0[0], &from.0[1], &from.0[2]);
        match op {
            None => None,
            Some(Value::String(op)) => match (v0, v1) {
                (_, None) | (None, _) => None,
                (Some(v0), Some(v1)) => Some(match &**op {
                    "eq" => {
                        if v0 == v1 {
                            Value::True
                        } else {
                            Value::False
                        }
                    }
                    "lt" => {
                        if v0 < v1 {
                            Value::True
                        } else {
                            Value::False
                        }
                    }
                    "gt" => {
                        if v0 > v1 {
                            Value::True
                        } else {
                            Value::False
                        }
                    }
                    "lte" => {
                        if v0 <= v1 {
                            Value::True
                        } else {
                            Value::False
                        }
                    }
                    "gte" => {
                        if v0 >= v1 {
                            Value::True
                        } else {
                            Value::False
                        }
                    }
                    op => Value::Error(Chars::from(format!(
                        "invalid op {op}, expected eq, lt, gt, lte, or gte"
                    ))),
                }),
            },
            Some(_) => err!("cmp(op, v0, v1): expected op to be a string"),
        }
    }
}

pub type Cmp = CachedArgs<CmpEv>;

pub struct IfEv;

impl EvalCached for IfEv {
    const NAME: &str = "if";
    const ARITY: Arity = Arity::AtLeast(2);

    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [cond, b1] => match cond {
                None => None,
                Some(Value::True) => b1.clone(),
                Some(Value::False) => None,
                _ => {
                    err!("if(predicate, caseIf, [caseElse]): expected boolean condition")
                }
            },
            [cond, b1, b2] => match cond {
                None => None,
                Some(Value::True) => b1.clone(),
                Some(Value::False) => b2.clone(),
                _ => {
                    err!("if(predicate, caseIf, [caseElse]): expected boolean condition")
                }
            },
            _ => {
                err!("if(predicate, caseIf, [caseElse]): expected at least 2 arguments")
            }
        }
    }
}

pub type If = CachedArgs<IfEv>;

pub struct FilterEv;

impl EvalCached for FilterEv {
    const NAME: &str = "filter";
    const ARITY: Arity = Arity::Exactly(2);

    fn eval(from: &CachedVals) -> Option<Value> {
        let (pred, s) = (&from.0[0], &from.0[1]);
        match pred {
            None => None,
            Some(Value::True) => s.clone(),
            Some(Value::False) => None,
            _ => err!("filter(predicate, source) expected boolean predicate"),
        }
    }
}

pub type Filter = CachedArgs<FilterEv>;

pub struct FilterErrEv;

impl EvalCached for FilterErrEv {
    const NAME: &str = "filter_err";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            None | Some(Value::Error(_)) => None,
            Some(v) => Some(v.clone()),
        }
    }
}

pub type FilterErr = CachedArgs<FilterErrEv>;

pub struct CastEv;

fn with_typ_prefix(
    from: &CachedVals,
    name: &'static str,
    f: impl Fn(Typ, &Option<Value>) -> Option<Value>,
) -> Option<Value> {
    let (typ, src) = (&from.0[0], &from.0[1]);
    match typ {
        None => None,
        Some(Value::String(s)) => match s.parse::<Typ>() {
            Ok(typ) => f(typ, src),
            Err(e) => errf!("{name}: invalid type {s}, {e}"),
        },
        _ => errf!("{name} expected typ as string"),
    }
}

impl EvalCached for CastEv {
    const NAME: &str = "cast";
    const ARITY: Arity = Arity::Exactly(2);

    fn eval(from: &CachedVals) -> Option<Value> {
        with_typ_prefix(from, "cast(typ, src)", |typ, v| {
            v.as_ref().and_then(|v| v.clone().cast(typ))
        })
    }
}

pub type Cast = CachedArgs<CastEv>;

pub struct IsaEv;

impl EvalCached for IsaEv {
    const NAME: &str = "isa";
    const ARITY: Arity = Arity::Exactly(2);

    fn eval(from: &CachedVals) -> Option<Value> {
        with_typ_prefix(from, "isa(typ, src)", |typ, v| match (typ, v) {
            (_, None) => None,
            (Typ::U32, Some(Value::U32(_))) => Some(Value::True),
            (Typ::U32, Some(_)) => Some(Value::False),
            (Typ::V32, Some(Value::V32(_))) => Some(Value::True),
            (Typ::V32, Some(_)) => Some(Value::False),
            (Typ::I32, Some(Value::I32(_))) => Some(Value::True),
            (Typ::I32, Some(_)) => Some(Value::False),
            (Typ::Z32, Some(Value::Z32(_))) => Some(Value::True),
            (Typ::Z32, Some(_)) => Some(Value::False),
            (Typ::U64, Some(Value::U64(_))) => Some(Value::True),
            (Typ::U64, Some(_)) => Some(Value::False),
            (Typ::V64, Some(Value::V64(_))) => Some(Value::True),
            (Typ::V64, Some(_)) => Some(Value::False),
            (Typ::I64, Some(Value::I64(_))) => Some(Value::True),
            (Typ::I64, Some(_)) => Some(Value::False),
            (Typ::Z64, Some(Value::Z64(_))) => Some(Value::True),
            (Typ::Z64, Some(_)) => Some(Value::False),
            (Typ::F32, Some(Value::F32(_))) => Some(Value::True),
            (Typ::F32, Some(_)) => Some(Value::False),
            (Typ::F64, Some(Value::F64(_))) => Some(Value::True),
            (Typ::F64, Some(_)) => Some(Value::False),
            (Typ::Decimal, Some(Value::Decimal(_))) => Some(Value::True),
            (Typ::Decimal, Some(_)) => Some(Value::False),
            (Typ::Bool, Some(Value::True)) => Some(Value::True),
            (Typ::Bool, Some(Value::False)) => Some(Value::True),
            (Typ::Bool, Some(_)) => Some(Value::False),
            (Typ::String, Some(Value::String(_))) => Some(Value::True),
            (Typ::String, Some(_)) => Some(Value::False),
            (Typ::Bytes, Some(Value::Bytes(_))) => Some(Value::True),
            (Typ::Bytes, Some(_)) => Some(Value::False),
            (Typ::Result, Some(Value::Ok)) => Some(Value::True),
            (Typ::Result, Some(Value::Error(_))) => Some(Value::True),
            (Typ::Result, Some(_)) => Some(Value::False),
            (Typ::Array, Some(Value::Array(_))) => Some(Value::True),
            (Typ::Array, Some(_)) => Some(Value::False),
            (Typ::DateTime, Some(Value::DateTime(_))) => Some(Value::True),
            (Typ::DateTime, Some(_)) => Some(Value::False),
            (Typ::Duration, Some(Value::Duration(_))) => Some(Value::True),
            (Typ::Duration, Some(_)) => Some(Value::False),
            (Typ::Null, Some(Value::Null)) => Some(Value::True),
            (Typ::Null, Some(_)) => Some(Value::False),
        })
    }
}

pub type Isa = CachedArgs<IsaEv>;

pub struct Eval<C: Ctx + 'static, E: Clone + 'static> {
    node: Result<Node<C, E>, Value>,
    scope: ModPath,
}

impl<C: Ctx, E: Clone> Init<C, E> for Eval<C, E> {
    const NAME: &str = "eval";
    const ARITY: Arity = Arity::Exactly(1);

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, scope, _| {
            Ok(Box::new(Eval { node: Err(Value::Null), scope: scope.clone() }))
        })
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Eval<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from[0].update(ctx, event) {
            None => match &mut self.node {
                Ok(node) => node.update(ctx, event),
                Err(e) => Some(e.clone()),
            },
            Some(v) => {
                self.node = match v {
                    Value::String(s) => match s.parse::<Expr>() {
                        Ok(spec) => Ok(Node::compile(ctx, &self.scope, spec)),
                        Err(e) => {
                            let e = format!("eval(src), error parsing {s}, {e}");
                            Err(Value::Error(Chars::from(e)))
                        }
                    },
                    v => {
                        let e = format!("eval(src) expected 1 string argument, not {v}");
                        Err(Value::Error(Chars::from(e)))
                    }
                };
                match &mut self.node {
                    Ok(node) => node.update(ctx, &Event::Init),
                    Err(e) => Some(e.clone()),
                }
            }
        }
    }
}

pub struct Count {
    count: u64,
}

impl<C: Ctx, E: Clone> Init<C, E> for Count {
    const NAME: &str = "count";
    const ARITY: Arity = Arity::Any;

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Count { count: 0 })))
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Count {
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

pub struct Sample {
    last: Option<Value>,
}

impl<C: Ctx, E: Clone> Init<C, E> for Sample {
    const NAME: &str = "sample";
    const ARITY: Arity = Arity::Exactly(2);

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Sample { last: None })))
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Sample {
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

pub struct MeanEv;

impl EvalCached for MeanEv {
    const NAME: &str = "mean";
    const ARITY: Arity = Arity::Any;

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

pub type Mean = CachedArgs<MeanEv>;

pub(crate) struct Uniq(Option<Value>);

impl<C: Ctx, E: Clone> Init<C, E> for Uniq {
    const NAME: &str = "uniq";
    const ARITY: Arity = Arity::Exactly(1);

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _, _| Ok(Box::new(Uniq(None))))
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Uniq {
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
