use crate::{
    err, errf,
    expr::{Expr, ExprKind},
    stdfn::{CachedArgs, CachedVals, EvalCached},
    vm::{Apply, Arity, BindId, Ctx, Event, ExecCtx, Init, InitFn, Node, NodeKind},
};
use anyhow::bail;
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use netidx::subscriber::{Typ, Value};
use netidx_netproto::valarray::ValArray;
use smallvec::{smallvec, SmallVec};
use std::{fmt::Debug, sync::Arc};

struct Any;

impl<C: Ctx, E: Debug + Clone> Init<C, E> for Any {
    const NAME: &str = "any";
    const ARITY: Arity = Arity::Any;

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _| Ok(Box::new(Any)))
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

struct Once {
    val: bool,
}

impl<C: Ctx, E: Debug + Clone> Init<C, E> for Once {
    const NAME: &str = "once";
    const ARITY: Arity = Arity::Exactly(1);

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _| Ok(Box::new(Once { val: false })))
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

type All = CachedArgs<AllEv>;

struct ArrayEv;

impl EvalCached for ArrayEv {
    const NAME: &str = "array";
    const ARITY: Arity = Arity::Any;

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
    const ARITY: Arity = Arity::Any;

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
    const ARITY: Arity = Arity::Any;

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
    const ARITY: Arity = Arity::Any;

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

type Min = CachedArgs<MinEv>;

struct MaxEv;

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

type Max = CachedArgs<MaxEv>;

struct AndEv;

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

type And = CachedArgs<AndEv>;

struct OrEv;

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

type Or = CachedArgs<OrEv>;

struct NotEv;

impl EvalCached for NotEv {
    const NAME: &str = "not";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        from.0[0].as_ref().map(|v| !(v.clone()))
    }
}

type Not = CachedArgs<NotEv>;

struct IsErrEv;

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

type IsErr = CachedArgs<IsErrEv>;

struct IndexEv;

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

type Index = CachedArgs<IndexEv>;

struct FilterEv;

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

type Filter = CachedArgs<FilterEv>;

struct FilterErrEv;

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

type FilterErr = CachedArgs<FilterErrEv>;

struct CastEv;

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

type Cast = CachedArgs<CastEv>;

struct IsaEv;

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

type Isa = CachedArgs<IsaEv>;

struct Count {
    count: u64,
}

impl<C: Ctx, E: Debug + Clone> Init<C, E> for Count {
    const NAME: &str = "count";
    const ARITY: Arity = Arity::Any;

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _| Ok(Box::new(Count { count: 0 })))
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

impl<C: Ctx, E: Debug + Clone> Init<C, E> for Sample {
    const NAME: &str = "sample";
    const ARITY: Arity = Arity::Exactly(2);

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _| Ok(Box::new(Sample { last: None })))
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

type Mean = CachedArgs<MeanEv>;

struct Uniq(Option<Value>);

impl<C: Ctx, E: Debug + Clone> Init<C, E> for Uniq {
    const NAME: &str = "uniq";
    const ARITY: Arity = Arity::Exactly(1);

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _| Ok(Box::new(Uniq(None))))
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

impl<C: Ctx, E: Debug + Clone> Init<C, E> for Never {
    const NAME: &str = "never";
    const ARITY: Arity = Arity::Any;

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _| Ok(Box::new(Never)))
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
    pred: Box<dyn Apply<C, E> + Send + Sync>,
    n_id: BindId,
    val_id: BindId,
    from: [Node<C, E>; 2],
}

impl<C: Ctx, E: Debug + Clone> Init<C, E> for Group<C, E> {
    const NAME: &str = "group";
    const ARITY: Arity = Arity::Exactly(2);

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|ctx, from, top_id| match from {
            [_, Node { spec: _, kind: NodeKind::Lambda(init) }] => {
                let n_id = BindId::new();
                let val_id = BindId::new();
                let mut from = [
                    Node {
                        spec: ExprKind::Ref { name: ["n"].into() }.to_expr(),
                        kind: NodeKind::Ref(n_id),
                    },
                    Node {
                        spec: ExprKind::Ref { name: ["x"].into() }.to_expr(),
                        kind: NodeKind::Ref(val_id),
                    },
                ];
                ctx.user.ref_var(n_id, top_id);
                ctx.user.ref_var(val_id, top_id);
                let pred = init(ctx, &mut from, top_id)?;
                Ok(Box::new(Self { buf: smallvec![], pred, n_id, val_id, from }))
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
            ctx.user.set_var(self.n_id, self.buf.len().into());
            ctx.user.set_var(self.val_id, val);
        }
        match self.pred.update(ctx, &mut self.from, event) {
            Some(Value::True) => {
                Some(Value::Array(ValArray::from_iter_exact(self.buf.drain(..))))
            }
            _ => None,
        }
    }
}

struct Ungroup(BindId);

impl<C: Ctx, E: Debug + Clone> Init<C, E> for Ungroup {
    const NAME: &str = "ungroup";
    const ARITY: Arity = Arity::Exactly(1);

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, _| Ok(Box::new(Ungroup(BindId::new()))))
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
    pub let group = |v, f| 'group
    pub let ungroup = |a| 'ungroup
    pub let all = |@args| 'all
    pub let and = |@args| 'and
    pub let any = |@args| 'any
    pub let array = |@args| 'array
    pub let cast = |type, v| 'cast
    pub let count = |@args| 'count
    pub let divide = |@args| 'divide
    pub let filter_err = |e| 'filter_err
    pub let filter = |predicate, v| 'filter
    pub let index = |array, i| 'index
    pub let isa = |type, v| 'isa
    pub let is_err = |e| 'is_error
    pub let max = |@args| 'max
    pub let mean = |@args| 'mean
    pub let min = |@args| 'min
    pub let never = |@args| 'never
    pub let not = |e| 'not
    pub let once = |v| 'once
    pub let or = |@args| 'or
    pub let product = |@args| 'product
    pub let sample = |trigger, v| 'sample
    pub let sum = |@args| 'sum
    pub let uniq = |v| 'uniq
}
"#;

pub fn register<C: Ctx, E: Debug + Clone>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<Group<C, E>>();
    ctx.register_builtin::<All>();
    ctx.register_builtin::<And>();
    ctx.register_builtin::<Any>();
    ctx.register_builtin::<Array>();
    ctx.register_builtin::<Cast>();
    ctx.register_builtin::<Count>();
    ctx.register_builtin::<Divide>();
    ctx.register_builtin::<Filter>();
    ctx.register_builtin::<FilterErr>();
    ctx.register_builtin::<Index>();
    ctx.register_builtin::<Isa>();
    ctx.register_builtin::<IsErr>();
    ctx.register_builtin::<Max>();
    ctx.register_builtin::<Mean>();
    ctx.register_builtin::<Min>();
    ctx.register_builtin::<Never>();
    ctx.register_builtin::<Not>();
    ctx.register_builtin::<Once>();
    ctx.register_builtin::<Or>();
    ctx.register_builtin::<Product>();
    ctx.register_builtin::<Sample>();
    ctx.register_builtin::<Sum>();
    ctx.register_builtin::<Uniq>();
    MOD.parse().unwrap()
}
