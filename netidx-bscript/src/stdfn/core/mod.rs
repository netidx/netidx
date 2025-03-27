#[cfg(test)]
use crate::run;
use crate::{
    deftype, err, errf,
    expr::{Expr, ExprKind},
    node::{Node, NodeKind},
    stdfn::{CachedArgs, CachedVals, EvalCached},
    typ::{FnArgType, FnType, NoRefs, Type},
    Apply, BindId, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, UserEvent,
};
use anyhow::bail;
#[cfg(test)]
use anyhow::Result;
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use netidx::subscriber::Value;
use std::sync::Arc;

mod array;

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

#[cfg(test)]
const IS_ERR: &str = r#"
{
  let a = [42, 43, 44];
  let y = a[0]? + a[3]?;
  is_err(errors)
}
"#;

#[cfg(test)]
run!(is_err, IS_ERR, |v: Result<&Value>| match v {
    Ok(Value::Bool(b)) => *b,
    _ => false,
});

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

#[cfg(test)]
const FILTER_ERR: &str = r#"
{
  let a = [42, 43, 44, error("foo")];
  filter_err(array::iter(a))
}
"#;

#[cfg(test)]
run!(filter_err, FILTER_ERR, |v: Result<&Value>| match v {
    Ok(Value::Error(_)) => true,
    _ => false,
});

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

#[cfg(test)]
const ERROR: &str = r#"
{
  error("foo")
}
"#;

#[cfg(test)]
run!(error, ERROR, |v: Result<&Value>| match v {
    Ok(Value::Error(_)) => true,
    _ => false,
});

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

#[cfg(test)]
const ONCE: &str = r#"
{
  let x = [1, 2, 3, 4, 5, 6];
  once(array::iter(x))
}
"#;

#[cfg(test)]
run!(once, ONCE, |v: Result<&Value>| match v {
    Ok(Value::I64(1)) => true,
    _ => false,
});

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

#[cfg(test)]
const ALL: &str = r#"
{
  let x = 1;
  let y = x;
  let z = y;
  all(x, y, z)
}
"#;

#[cfg(test)]
run!(all, ALL, |v: Result<&Value>| match v {
    Ok(Value::I64(1)) => true,
    _ => false,
});

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

#[cfg(test)]
const SUM: &str = r#"
{
  let tweeeeenywon = [1, 2, 3, 4, 5, 6];
  sum(tweeeeenywon)
}
"#;

#[cfg(test)]
run!(sum, SUM, |v: Result<&Value>| match v {
    Ok(Value::I64(21)) => true,
    _ => false,
});

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

#[cfg(test)]
const PRODUCT: &str = r#"
{
  let tweeeeenywon = [5, 2, 2, 1.05];
  product(tweeeeenywon)
}
"#;

#[cfg(test)]
run!(product, PRODUCT, |v: Result<&Value>| match v {
    Ok(Value::F64(21.0)) => true,
    _ => false,
});

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

#[cfg(test)]
const DIVIDE: &str = r#"
{
  let tweeeeenywon = [84, 2, 2];
  divide(tweeeeenywon)
}
"#;

#[cfg(test)]
run!(divide, DIVIDE, |v: Result<&Value>| match v {
    Ok(Value::I64(21)) => true,
    _ => false,
});

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

#[cfg(test)]
const MIN: &str = r#"
{
   min(1, 2, 3, 4, 5, 6, 0)
}
"#;

#[cfg(test)]
run!(min, MIN, |v: Result<&Value>| match v {
    Ok(Value::I64(0)) => true,
    _ => false,
});

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

#[cfg(test)]
const MAX: &str = r#"
{
   max(1, 2, 3, 4, 5, 6, 0)
}
"#;

#[cfg(test)]
run!(max, MAX, |v: Result<&Value>| match v {
    Ok(Value::I64(6)) => true,
    _ => false,
});

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

#[cfg(test)]
const AND: &str = r#"
{
  let x = 1;
  let y = x + 1;
  let z = y + 1;
  and(x < y, y < z, x > 0, z < 10)
}
"#;

#[cfg(test)]
run!(and, AND, |v: Result<&Value>| match v {
    Ok(Value::Bool(true)) => true,
    _ => false,
});

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

#[cfg(test)]
const OR: &str = r#"
{
  or(false, false, true)
}
"#;

#[cfg(test)]
run!(or, OR, |v: Result<&Value>| match v {
    Ok(Value::Bool(true)) => true,
    _ => false,
});

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

#[cfg(test)]
const INDEX: &str = r#"
{
  let a = ["foo", "bar", 1, 2, 3];
  cast<i64>(a[2]?)? + cast<i64>(a[3]?)?
}
"#;

#[cfg(test)]
run!(index, INDEX, |v: Result<&Value>| match v {
    Ok(Value::I64(3)) => true,
    _ => false,
});

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

#[cfg(test)]
const SLICE: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8];
  [sum(a[2..4]?), sum(a[6..]?), sum(a[..2]?)]
}
"#;

#[cfg(test)]
run!(slice, SLICE, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::I64(7), Value::I64(15), Value::I64(3)] => true,
            _ => false,
        },
        _ => false,
    }
});

struct Filter<C: Ctx, E: UserEvent> {
    pred: Box<dyn Apply<C, E> + Send + Sync>,
    typ: FnType<NoRefs>,
    x: BindId,
    from: [Node<C, E>; 1],
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Filter<C, E> {
    const NAME: &str = "filter";
    deftype!("fn('a, fn('a) -> bool) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|ctx, typ, scope, from, top_id| match from {
            [_, Node { spec: _, typ: _, kind: NodeKind::Lambda(lb) }] => {
                let x = ctx.env.bind_variable(scope, "x", from[0].typ.clone()).id;
                ctx.user.ref_var(x, top_id);
                let mut from = [Node {
                    spec: Box::new(ExprKind::Ref { name: ["x"].into() }.to_expr()),
                    typ: from[0].typ.clone(),
                    kind: NodeKind::Ref(x),
                }];
                let pred = (lb.init)(ctx, &mut from, top_id)?;
                Ok(Box::new(Self { pred, typ: typ.clone(), x, from }))
            }
            _ => bail!("expected a function"),
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
        match from[0].update(ctx, event) {
            None => {
                self.pred.update(ctx, &mut self.from, event);
                None
            }
            Some(v) => {
                event.variables.insert(self.x, v.clone());
                match self.pred.update(ctx, &mut self.from, event) {
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
        self.from[0].typ.check_contains(&from[0].typ)?;
        self.pred.typecheck(ctx, from)?;
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
const FILTER: &str = r#"
{
  let a = [1, 2, 3, 4, 5, 6, 7, 8];
  filter(array::iter(a), |x| x > 7)
}
"#;

#[cfg(test)]
run!(filter, FILTER, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(8)) => true,
        _ => false,
    }
});

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

#[cfg(test)]
const COUNT: &str = r#"
{
  let a = [0, 1, 2, 3];
  array::group(count(array::iter(a)), |n, _| n == 4)
}
"#;

#[cfg(test)]
run!(count, COUNT, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::U64(1), Value::U64(2), Value::U64(3), Value::U64(4)] => true,
            _ => false,
        },
        _ => false,
    }
});

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

#[cfg(test)]
const SAMPLE: &str = r#"
{
  let a = [0, 1, 2, 3];
  let x = "tweeeenywon!";
  array::group(sample(array::iter(a), x), |n, _| n == 4)
}
"#;

#[cfg(test)]
run!(sample, SAMPLE, |v: Result<&Value>| {
    match v {
        Ok(Value::Array(a)) => match &a[..] {
            [Value::String(s0), Value::String(s1), Value::String(s2), Value::String(s3)] => {
                s0 == s1 && s1 == s2 && s2 == s3 && &**s3 == "tweeeenywon!"
            }
            _ => false,
        },
        _ => false,
    }
});

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

#[cfg(test)]
const MEAN: &str = r#"
{
  let a = [0, 1, 2, 3];
  mean(a)
}
"#;

#[cfg(test)]
run!(mean, MEAN, |v: Result<&Value>| {
    match v {
        Ok(Value::F64(1.5)) => true,
        _ => false,
    }
});

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

#[cfg(test)]
const UNIQ: &str = r#"
{
  let a = [1, 1, 1, 1, 1, 1, 1];
  uniq(array::iter(a))
}
"#;

#[cfg(test)]
run!(uniq, UNIQ, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(1)) => true,
        _ => false,
    }
});

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

#[cfg(test)]
const NEVER: &str = r#"
{
   let x = never(100);
   any(x, 0)
}
"#;

#[cfg(test)]
run!(never, NEVER, |v: Result<&Value>| {
    match v {
        Ok(Value::I64(0)) => true,
        _ => false,
    }
});

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
