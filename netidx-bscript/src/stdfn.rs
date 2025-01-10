use crate::{
    expr::{Expr, ExprId},
    vm::{Apply, Ctx, Event, ExecCtx, InitFn, Node, Register},
};
use anyhow::bail;
use anyhow::{anyhow, Result};
use fxhash::FxHashSet;
use netidx::{
    chars::Chars,
    path::Path,
    publisher::FromValue,
    subscriber::{Dval, Typ, UpdatesFlags, Value},
};
use netidx_core::utils::Either;
use smallvec::SmallVec;
use std::{
    collections::HashSet, iter, marker::PhantomData, ops::SubAssign, sync::Arc,
    time::Duration,
};

macro_rules! errf {
    ($pat:expr, $($arg:expr),*) => { Some(Value::Error(Chars::from(format!($pat, $($arg),*)))) };
    ($pat:expr) => { Some(Value::Error(Chars::from(format!($pat)))) };
}

macro_rules! err {
    ($pat:expr) => {
        Some(Value::Error(Chars::from($pat)))
    };
}

macro_rules! arity1 {
    ($from:expr, $updates:expr, $usage:expr) => {
        match (&*$from, &*$updates) {
            ([arg], [arg_up]) => (arg, arg_up),
            (_, up) if !up.iter().any(|b| *b) => return None,
            (_, _) => return err!($usage),
        }
    };
}

macro_rules! arity2 {
    ($from:expr, $updates:expr, $usage:expr) => {
        match (&*$from, &*$updates) {
            ([arg0, arg1], [arg0_up, arg1_up]) => ((arg0, arg1), (arg0_up, arg1_up)),
            (_, up) if !up.iter().any(|b| *b) => return None,
            (_, _) => return err!($usage),
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

impl<C: Ctx, E: Clone> Register<C, E> for Any {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, _, _, _| Box::new(Any));
        ctx.functions.insert("any".into(), f);
        ctx.user.register_fn(Path::root(), "any".into());
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

impl<C: Ctx, E: Clone> Register<C, E> for Once {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, _, _, _| Box::new(Once { val: false }));
        ctx.functions.insert("once".into(), f);
        ctx.user.register_fn(Path::root(), "once".into());
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
            n => {
                if n.into_iter().fold(false, |u, n| u || n.update(ctx, event).is_some()) {
                    err!("once(v): expected one argument")
                } else {
                    None
                }
            }
        }
    }
}

pub struct Do;

impl<C: Ctx, E: Clone> Register<C, E> for Do {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, _, _, _| Box::new(Do));
        ctx.functions.insert("do".into(), f);
        ctx.user.register_fn(Path::root(), "do".into());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Do {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        from.into_iter().fold(None, |_, src| src.update(ctx, event))
    }
}

pub trait EvalCached {
    fn eval(from: &CachedVals) -> Option<Value>;
    fn name() -> &'static str;
}

pub struct CachedArgs<T: EvalCached + Send + Sync> {
    cached: CachedVals,
    t: PhantomData<T>,
}

impl<C: Ctx, E: Clone, T: EvalCached + Send + Sync + 'static> Register<C, E>
    for CachedArgs<T>
{
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, from, _, _| {
            Box::new(CachedArgs::<T> { cached: CachedVals::new(from), t: PhantomData })
        });
        ctx.functions.insert(T::name().into(), f);
        ctx.user.register_fn(Path::root(), T::name().into());
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

    fn name() -> &'static str {
        "all"
    }
}

pub type All = CachedArgs<AllEv>;

pub struct ArrayEv;

impl EvalCached for ArrayEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        if from.0.iter().all(|v| v.is_some()) {
            Some(Value::Array(Arc::from_iter(from.0.iter().filter_map(|v| v.clone()))))
        } else {
            None
        }
    }

    fn name() -> &'static str {
        "array"
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
    fn eval(from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => add_vals(res, v.clone()),
        })
    }

    fn name() -> &'static str {
        "sum"
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
    fn eval(from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => prod_vals(res, v.clone()),
        })
    }

    fn name() -> &'static str {
        "product"
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
    fn eval(from: &CachedVals) -> Option<Value> {
        from.flat_iter().fold(None, |res, v| match res {
            res @ Some(Value::Error(_)) => res,
            res => div_vals(res, v.clone()),
        })
    }

    fn name() -> &'static str {
        "divide"
    }
}

pub type Divide = CachedArgs<DivideEv>;

pub struct MinEv;

impl EvalCached for MinEv {
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

    fn name() -> &'static str {
        "min"
    }
}

pub type Min = CachedArgs<MinEv>;

pub struct MaxEv;

impl EvalCached for MaxEv {
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

    fn name() -> &'static str {
        "max"
    }
}

pub type Max = CachedArgs<MaxEv>;

pub struct AndEv;

impl EvalCached for AndEv {
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

    fn name() -> &'static str {
        "and"
    }
}

pub type And = CachedArgs<AndEv>;

pub struct OrEv;

impl EvalCached for OrEv {
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

    fn name() -> &'static str {
        "or"
    }
}

pub type Or = CachedArgs<OrEv>;

pub struct NotEv;

impl EvalCached for NotEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [v] => v.as_ref().map(|v| !(v.clone())),
            _ => err!("not expected 1 argument"),
        }
    }

    fn name() -> &'static str {
        "not"
    }
}

pub type Not = CachedArgs<NotEv>;

pub struct IsErrEv;

impl EvalCached for IsErrEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [v] => v.as_ref().map(|v| match v {
                Value::Error(_) => Value::True,
                _ => Value::False,
            }),
            _ => err!("is_error expected 1 argument"),
        }
    }

    fn name() -> &'static str {
        "is_error"
    }
}

pub type IsErr = CachedArgs<IsErrEv>;

// CR estokes: document
pub struct StartsWithEv;

impl EvalCached for StartsWithEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(pfx)), Some(Value::String(val))] => {
                if val.starts_with(&**pfx) {
                    Some(Value::True)
                } else {
                    Some(Value::False)
                }
            }
            [None, _] | [_, None] => None,
            _ => err!("starts_with expected 2 arguments"),
        }
    }

    fn name() -> &'static str {
        "starts_with"
    }
}

pub type StartsWith = CachedArgs<StartsWithEv>;

pub struct IndexEv;

impl EvalCached for IndexEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::Array(elts)), Some(Value::I64(i))] if *i >= 0 => {
                let i = *i as usize;
                if i < elts.len() {
                    Some(elts[i].clone())
                } else {
                    err!("array index out of bounds")
                }
            }
            [None, _] | [_, None] => None,
            _ => err!("index(array, index): expected an array and a positive index"),
        }
    }

    fn name() -> &'static str {
        "index"
    }
}

pub type Index = CachedArgs<IndexEv>;

// CR estokes: document
pub struct EndsWithEv;

impl EvalCached for EndsWithEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(sfx)), Some(Value::String(val))] => {
                if val.ends_with(&**sfx) {
                    Some(Value::True)
                } else {
                    Some(Value::False)
                }
            }
            [None, _] | [_, None] => None,
            _ => err!("ends_with expected 2 arguments"),
        }
    }

    fn name() -> &'static str {
        "ends_with"
    }
}

pub type EndsWith = CachedArgs<EndsWithEv>;

// CR estokes: document
pub struct ContainsEv;

impl EvalCached for ContainsEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(chs)), Some(Value::String(val))] => {
                if val.contains(&**chs) {
                    Some(Value::True)
                } else {
                    Some(Value::False)
                }
            }
            [None, _] | [_, None] => None,
            _ => err!("contains expected 2 arguments"),
        }
    }

    fn name() -> &'static str {
        "contains"
    }
}

pub type Contains = CachedArgs<ContainsEv>;

pub struct StripPrefixEv;

impl EvalCached for StripPrefixEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(pfx)), Some(Value::String(val))] => val
                .strip_prefix(&**pfx)
                .map(|s| Value::String(Chars::from(String::from(s)))),
            [None, _] | [_, None] => None,
            _ => err!("strip_prefix expected 2 arguments"),
        }
    }

    fn name() -> &'static str {
        "strip_prefix"
    }
}

pub type StripPrefix = CachedArgs<StripPrefixEv>;

pub struct StripSuffixEv;

impl EvalCached for StripSuffixEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(sfx)), Some(Value::String(val))] => val
                .strip_suffix(&**sfx)
                .map(|s| Value::String(Chars::from(String::from(s)))),
            [None, _] | [_, None] => None,
            _ => err!("strip_suffix expected 2 arguments"),
        }
    }

    fn name() -> &'static str {
        "strip_suffix"
    }
}

pub type StripSuffix = CachedArgs<StripSuffixEv>;

pub struct TrimEv;

impl EvalCached for TrimEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(val))] => {
                Some(Value::String(Chars::from(String::from(val.trim()))))
            }
            [None] => None,
            _ => err!("trim expected 1 arguments"),
        }
    }

    fn name() -> &'static str {
        "trim"
    }
}

pub type Trim = CachedArgs<TrimEv>;

pub struct TrimStartEv;

impl EvalCached for TrimStartEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(val))] => {
                Some(Value::String(Chars::from(String::from(val.trim_start()))))
            }
            [None] => None,
            _ => err!("trim_start expected 1 arguments"),
        }
    }

    fn name() -> &'static str {
        "trim_start"
    }
}

pub type TrimStart = CachedArgs<TrimStartEv>;

pub struct TrimEndEv;

impl EvalCached for TrimEndEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(val))] => {
                Some(Value::String(Chars::from(String::from(val.trim_end()))))
            }
            [None] => None,
            _ => err!("trim_start expected 1 arguments"),
        }
    }

    fn name() -> &'static str {
        "trim_end"
    }
}

pub type TrimEnd = CachedArgs<TrimEndEv>;

pub struct ReplaceEv;

impl EvalCached for ReplaceEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(pat)), Some(Value::String(rep)), Some(Value::String(val))] => {
                Some(Value::String(Chars::from(String::from(
                    val.replace(&**pat, &**rep),
                ))))
            }
            [None, _, _] | [_, None, _] | [_, _, None] => None,
            _ => err!("replace expected 3 arguments"),
        }
    }

    fn name() -> &'static str {
        "replace"
    }
}

pub type Replace = CachedArgs<ReplaceEv>;

pub struct DirnameEv;

impl EvalCached for DirnameEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(path))] => match Path::dirname(path) {
                None => Some(Value::Null),
                Some(dn) => Some(Value::String(Chars::from(String::from(dn)))),
            },
            [None] => None,
            _ => err!("dirname expected 1 argument"),
        }
    }

    fn name() -> &'static str {
        "dirname"
    }
}

pub type Dirname = CachedArgs<DirnameEv>;

pub struct BasenameEv;

impl EvalCached for BasenameEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(path))] => match Path::basename(path) {
                None => Some(Value::Null),
                Some(dn) => Some(Value::String(Chars::from(String::from(dn)))),
            },
            [None] => None,
            _ => err!("basename expected 1 argument"),
        }
    }

    fn name() -> &'static str {
        "basename"
    }
}

pub type Basename = CachedArgs<BasenameEv>;

pub struct CmpEv;

impl EvalCached for CmpEv {
    fn name() -> &'static str {
        "cmp"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [op, v0, v1] => match op {
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
            },
            _ => err!("cmp(op, v0, v1): expected 3 arguments"),
        }
    }
}

pub type Cmp = CachedArgs<CmpEv>;

pub struct IfEv;

impl EvalCached for IfEv {
    fn name() -> &'static str {
        "if"
    }

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
    fn name() -> &'static str {
        "filter"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [pred, s] => match pred {
                None => None,
                Some(Value::True) => s.clone(),
                Some(Value::False) => None,
                _ => err!("filter(predicate, source) expected boolean predicate"),
            },
            _ => err!("filter(predicate, source): expected 2 arguments"),
        }
    }
}

pub type Filter = CachedArgs<FilterEv>;

pub struct FilterErrEv;

impl EvalCached for FilterErrEv {
    fn name() -> &'static str {
        "filter_err"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [s] => match s {
                None | Some(Value::Error(_)) => None,
                Some(_) => s.clone(),
            },
            _ => err!("filter_err(source): expected 1 argument"),
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
    match &*from.0 {
        [typ, src] => match typ {
            None => None,
            Some(Value::String(s)) => match s.parse::<Typ>() {
                Ok(typ) => f(typ, src),
                Err(e) => errf!("{name}: invalid type {s}, {e}"),
            },
            _ => errf!("{name} expected typ as string"),
        },
        _ => errf!("{name} expected 2 arguments"),
    }
}

impl EvalCached for CastEv {
    fn name() -> &'static str {
        "cast"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        with_typ_prefix(from, "cast(typ, src)", |typ, v| {
            v.as_ref().and_then(|v| v.clone().cast(typ))
        })
    }
}

pub type Cast = CachedArgs<CastEv>;

pub struct IsaEv;

impl EvalCached for IsaEv {
    fn name() -> &'static str {
        "isa"
    }

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

pub struct StringJoinEv;

impl EvalCached for StringJoinEv {
    fn name() -> &'static str {
        "string_join"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        use bytes::BytesMut;
        match &from.0[..] {
            [_] | [] => {
                err!("string_join(sep, s0, s1, ...): expected at least 2 arguments")
            }
            [None, ..] => None,
            [Some(sep), parts @ ..] => {
                // this is fairly common, so we check it before doing any real work
                for p in parts {
                    if p.is_none() {
                        return None;
                    }
                }
                let sep = match sep {
                    Value::String(c) => c.clone(),
                    sep => match sep.clone().cast_to::<Chars>().ok() {
                        Some(c) => c,
                        None => return err!("string_join, separator must be a string"),
                    },
                };
                let mut res = BytesMut::new();
                for p in parts {
                    let c = match p.as_ref().unwrap() {
                        Value::String(c) => c.clone(),
                        v => match v.clone().cast_to::<Chars>().ok() {
                            Some(c) => c,
                            None => {
                                return err!("string_join, components must be strings")
                            }
                        },
                    };
                    if res.is_empty() {
                        res.extend_from_slice(c.bytes());
                    } else {
                        res.extend_from_slice(sep.bytes());
                        res.extend_from_slice(c.bytes());
                    }
                }
                Some(Value::String(Chars::from_bytes(res.freeze()).unwrap()))
            }
        }
    }
}

pub type StringJoin = CachedArgs<StringJoinEv>;

pub struct StringConcatEv;

impl EvalCached for StringConcatEv {
    fn name() -> &'static str {
        "string_concat"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        use bytes::BytesMut;
        match &from.0[..] {
            [] => err!("string_concat: expected at least 1 argument"),
            parts => {
                // this is a fairly common case, so we check it before doing any real work
                for p in parts {
                    if p.is_none() {
                        return None;
                    }
                }
                let mut res = BytesMut::new();
                for p in parts {
                    match p.as_ref().unwrap() {
                        Value::String(c) => res.extend_from_slice(c.bytes()),
                        v => match v.clone().cast_to::<Chars>().ok() {
                            Some(c) => res.extend_from_slice(c.bytes()),
                            None => {
                                return err!("string_concat: arguments must be strings")
                            }
                        },
                    }
                }
                Some(Value::String(Chars::from_bytes(res.freeze()).unwrap()))
            }
        }
    }
}

pub type StringConcat = CachedArgs<StringConcatEv>;

pub struct Eval<C: Ctx, E> {
    node: Result<Node<C, E>, Value>,
    scope: Path,
}

impl<C: Ctx, E: Clone> Register<C, E> for Eval<C, E> {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, _, scope, _| {
            Box::new(Eval { node: Err(Value::Null), scope: scope.clone() })
        });
        ctx.functions.insert("eval".into(), f);
        ctx.user.register_fn(Path::root(), "eval".into());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Eval<C, E> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [source] => match source.update(ctx, event) {
                None => match &mut self.node {
                    Ok(node) => node.update(ctx, event),
                    Err(e) => Some(e.clone()),
                },
                Some(v) => {
                    self.node = match v {
                        Value::String(s) => match s.parse::<Expr>() {
                            Ok(spec) => Ok(Node::compile(ctx, &self.scope, spec)),
                            Err(e) => {
                                let e =
                                    format!("eval(src), error parsing formula {s}, {e}");
                                Err(Value::Error(Chars::from(e)))
                            }
                        },
                        v => {
                            let e =
                                format!("eval(src) expected 1 string argument, not {v}");
                            Err(Value::Error(Chars::from(e)))
                        }
                    };
                    match &mut self.node {
                        Ok(node) => node.update(ctx, &Event::Init),
                        Err(e) => Some(e.clone()),
                    }
                }
            },
            n => {
                if n.into_iter().fold(false, |u, n| u || n.update(ctx, event).is_some()) {
                    err!("eval expects 1 argument")
                } else {
                    None
                }
            }
        }
    }
}

pub struct Count {
    count: u64,
}

impl<C: Ctx, E: Clone> Register<C, E> for Count {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, _, _, _| Box::new(Count { count: 0 }));
        ctx.functions.insert("count".into(), f);
        ctx.user.register_fn(Path::root(), "count".into());
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

impl<C: Ctx, E: Clone> Register<C, E> for Sample {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, _, _, _| Box::new(Sample { last: None }));
        ctx.functions.insert("sample".into(), f);
        ctx.user.register_fn(Path::root(), "sample".into());
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
            n => {
                if n.into_iter().fold(false, |u, n| u || n.update(ctx, event).is_some()) {
                    err!("sample(trigger, source): expected 2 arguments")
                } else {
                    None
                }
            }
        }
    }
}

pub struct MeanEv;

impl EvalCached for MeanEv {
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

    fn name() -> &'static str {
        "mean"
    }
}

pub type Mean = CachedArgs<MeanEv>;

pub(crate) struct Uniq(Option<Value>);

impl<C: Ctx, E: Clone> Register<C, E> for Uniq {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, _, _, _| Box::new(Uniq(None)));
        ctx.functions.insert("uniq".into(), f);
        ctx.user.register_fn(Path::root(), "uniq".into());
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
            n => {
                if n.into_iter().fold(false, |u, n| u || n.update(ctx, event).is_some()) {
                    err!("uniq(e): expected 1 argument")
                } else {
                    None
                }
            }
        }
    }
}

fn as_path(v: Value) -> Option<Path> {
    match v.cast_to::<String>() {
        Err(_) => None,
        Ok(p) => {
            if Path::is_absolute(&p) {
                Some(Path::from(p))
            } else {
                None
            }
        }
    }
}

pub struct Store {
    args: CachedVals,
    top_id: ExprId,
    dv: Either<(Path, Dval), Vec<Value>>,
}

impl<C: Ctx, E: Clone> Register<C, E> for Store {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, from, _, top_id| {
            Box::new(Store {
                args: CachedVals::new(from),
                dv: Either::Right(vec![]),
                top_id,
            })
        });
        ctx.functions.insert("store".into(), f);
        ctx.user.register_fn(Path::root(), "store".into());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Store {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        fn set(dv: &mut Either<(Path, Dval), Vec<Value>>, val: &Value) {
            match dv {
                Either::Right(q) => q.push(val.clone()),
                Either::Left((_, dv)) => {
                    dv.write(val.clone());
                }
            }
        }
        let up = self.args.update_diff(ctx, from, event);
        let (args, updated) =
            arity2!(self.args.0, up, "set(path, val): expected 2 arguments");
        match (args, updated) {
            ((_, _), (false, false)) => (),
            ((_, Some(val)), (false, true)) => set(&mut self.dv, val),
            ((_, None), (false, true)) => (),
            ((None, Some(val)), (true, true)) => set(&mut self.dv, val),
            ((Some(path), Some(val)), (true, true)) if self.same_path(path) => {
                set(&mut self.dv, val)
            }
            ((Some(path), _), (true, false)) if self.same_path(path) => (),
            ((None, _), (true, false)) => (),
            ((None, None), (_, _)) => (),
            ((Some(path), val), (true, _)) => match as_path(path.clone()) {
                None => {
                    if let Either::Left(_) = &self.dv {
                        self.dv = Either::Right(vec![]);
                    }
                    return errf!("set(path, val): invalid path {path:?}");
                }
                Some(path) => {
                    let dv = ctx.user.durable_subscribe(
                        UpdatesFlags::empty(),
                        path.clone(),
                        self.top_id,
                    );
                    match &mut self.dv {
                        Either::Left(_) => (),
                        Either::Right(q) => {
                            for v in q.drain(..) {
                                dv.write(v);
                            }
                        }
                    }
                    self.dv = Either::Left((path, dv));
                    if let Some(val) = val {
                        set(&mut self.dv, val)
                    }
                }
            },
        }
        None
    }
}

impl Store {
    fn same_path(&self, new_path: &Value) -> bool {
        match (new_path, &self.dv) {
            (Value::String(p0), Either::Left((p1, _))) => &**p0 == &**p1,
            _ => false,
        }
    }
}

pub(crate) struct Load {
    args: CachedVals,
    cur: Option<(Path, Dval)>,
    top_id: ExprId,
}

impl<C: Ctx, E: Clone> Register<C, E> for Load {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, from, _, top_id| {
            Box::new(Load { args: CachedVals::new(from), cur: None, top_id })
        });
        ctx.functions.insert("load".into(), f);
        ctx.user.register_fn(Path::root(), "load".into());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Load {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        let up = self.args.update_diff(ctx, from, event);
        let (path, path_up) = arity1!(self.args.0, up, "load(path): expected 1 argument");
        match (path, path_up) {
            (Some(_), false) | (None, false) => (),
            (None, true) => {
                if let Some((path, dv)) = self.cur.take() {
                    ctx.user.unsubscribe(path, dv, self.top_id)
                }
                return None;
            }
            (Some(path), true) => {
                if let Some((path, dv)) = self.cur.take() {
                    ctx.user.unsubscribe(path, dv, self.top_id)
                }
                match as_path(path.clone()) {
                    None => return errf!("load(path): invalid absolute path {path:?}"),
                    Some(path) => {
                        self.cur = Some((
                            path.clone(),
                            ctx.user.durable_subscribe(
                                UpdatesFlags::BEGIN_WITH_LAST,
                                path,
                                self.top_id,
                            ),
                        ));
                    }
                }
            }
        }
        self.cur.as_ref().and_then(|(_, dv)| match event {
            Event::Variable { .. }
            | Event::Rpc(_, _)
            | Event::Timer(_, _)
            | Event::User(_)
            | Event::Init => None,
            Event::Netidx(id, value) if dv.id() == *id => Some(value.clone()),
            Event::Netidx(_, _) => None,
        })
    }
}

atomic_id!(RpcCallId);

pub(crate) struct RpcCall {
    args: CachedVals,
    top_id: ExprId,
    pending: FxHashSet<RpcCallId>,
}

impl<C: Ctx, E: Clone> Register<C, E> for RpcCall {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, from, _, top_id| {
            Box::new(RpcCall {
                args: CachedVals::new(from),
                top_id,
                pending: HashSet::default(),
            })
        });
        ctx.functions.insert("call".into(), f);
        ctx.user.register_fn(Path::root(), "call".into());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for RpcCall {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        fn parse_args(path: &Value, args: &Value) -> Result<(Path, Vec<(Chars, Value)>)> {
            let path = as_path(path.clone()).ok_or_else(|| anyhow!("invalid path"))?;
            let args = match args {
                Value::Array(args) => args
                    .iter()
                    .map(|v| match v {
                        Value::Array(p) => match &**p {
                            [Value::String(name), value] => {
                                Ok((name.clone(), value.clone()))
                            }
                            _ => Err(anyhow!("rpc args expected [name, value] pair")),
                        },
                        _ => Err(anyhow!("rpc args expected [name, value] pair")),
                    })
                    .collect::<Result<Vec<_>>>()?,
                _ => bail!("rpc args expected to be an array"),
            };
            Ok((path, args))
        }
        let up = self.args.update_diff(ctx, from, event);
        let ((path, args), (path_up, args_up)) =
            arity2!(self.args.0, up, "call(path, args): expected 2 arguments");
        match ((path, args), (path_up, args_up)) {
            ((Some(path), Some(args)), (_, true))
            | ((Some(path), Some(args)), (true, _)) => match parse_args(path, args) {
                Err(e) => return errf!("{e}"),
                Ok((path, args)) => {
                    let id = RpcCallId::new();
                    self.pending.insert(id);
                    ctx.user.call_rpc(path, args, self.top_id, id);
                }
            },
            ((None, _), (_, _)) | ((_, None), (_, _)) | ((_, _), (false, false)) => (),
        }
        match event {
            Event::Init
            | Event::Netidx(_, _)
            | Event::Timer(_, _)
            | Event::User(_)
            | Event::Variable { .. } => None,
            Event::Rpc(id, val) => {
                if self.pending.remove(id) {
                    Some(val.clone())
                } else {
                    None
                }
            }
        }
    }
}

atomic_id!(TimerId);

pub(crate) struct AfterIdle {
    args: CachedVals,
    id: Option<TimerId>,
    eid: ExprId,
}

impl<C: Ctx, E: Clone> Register<C, E> for AfterIdle {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, from, _, eid| {
            Box::new(AfterIdle { args: CachedVals::new(from), id: None, eid })
        });
        ctx.functions.insert("after_idle".into(), f);
        ctx.user.register_fn(Path::root(), "after_idle".into());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for AfterIdle {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        let up = self.args.update_diff(ctx, from, event);
        let ((timeout, val), (timeout_up, val_up)) =
            arity2!(self.args.0, up, "after_idle(timeout, cur): expected 2 arguments");
        match ((timeout, val), (timeout_up, val_up)) {
            ((Some(secs), _), (true, _)) => match secs.clone().cast_to::<Duration>() {
                Err(e) => {
                    self.id = None;
                    return errf!("after_idle(timeout, cur): expected duration {e:?}");
                }
                Ok(dur) => {
                    let id = TimerId::new();
                    self.id = Some(id);
                    ctx.user.set_timer(id, dur, self.eid);
                    return None;
                }
            },
            ((Some(_), Some(val)), (false, true)) if self.id.is_none() => {
                return Some(val.clone())
            }
            ((None, _), (_, _))
            | ((_, None), (_, _))
            | ((Some(_), Some(_)), (false, _)) => (),
        };
        match event {
            Event::Init
            | Event::Variable { .. }
            | Event::Netidx(_, _)
            | Event::Rpc(_, _)
            | Event::User(_) => None,
            Event::Timer(id, _) => {
                if self.id != Some(*id) {
                    None
                } else {
                    self.id = None;
                    self.args.0.get(1).and_then(|v| v.clone())
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Repeat {
    Yes,
    No,
    N(u64),
}

impl FromValue for Repeat {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::True => Ok(Repeat::Yes),
            Value::False => Ok(Repeat::No),
            v => match v.cast_to::<u64>() {
                Ok(n) => Ok(Repeat::N(n)),
                Err(_) => bail!("could not cast to repeat"),
            },
        }
    }
}

impl SubAssign<u64> for Repeat {
    fn sub_assign(&mut self, rhs: u64) {
        match self {
            Repeat::Yes | Repeat::No => (),
            Repeat::N(n) => *n -= rhs,
        }
    }
}

impl Repeat {
    fn will_repeat(&self) -> bool {
        match self {
            Repeat::No => false,
            Repeat::Yes => true,
            Repeat::N(n) => *n > 0,
        }
    }
}

pub(crate) struct Timer {
    args: CachedVals,
    timeout: Option<Duration>,
    repeat: Repeat,
    id: Option<TimerId>,
    eid: ExprId,
}

impl<C: Ctx, E: Clone> Register<C, E> for Timer {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|_, from, _, eid| {
            Box::new(Self {
                args: CachedVals::new(from),
                timeout: None,
                repeat: Repeat::No,
                id: None,
                eid,
            })
        });
        ctx.functions.insert("timer".into(), f);
        ctx.user.register_fn(Path::root(), "timer".into());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Timer {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        macro_rules! error {
            () => {{
                self.id = None;
                self.timeout = None;
                self.repeat = Repeat::No;
                return err!("timer(per, rep): expected duration, bool or number >= 0");
            }};
        }
        macro_rules! schedule {
            ($dur:expr) => {{
                let id = TimerId::new();
                self.id = Some(id);
                ctx.user.set_timer(id, $dur, self.eid);
            }};
        }
        let up = self.args.update_diff(ctx, from, event);
        let ((timeout, repeat), (timeout_up, repeat_up)) =
            arity2!(self.args.0, up, "timer(period, repeat): expected 2 arguments");
        match ((timeout, repeat), (timeout_up, repeat_up)) {
            ((None, Some(r)), (true, true)) | ((_, Some(r)), (false, true)) => {
                match r.clone().cast_to::<Repeat>() {
                    Err(_) => error!(),
                    Ok(repeat) => {
                        self.repeat = repeat;
                        if let Some(dur) = self.timeout {
                            if self.id.is_none() && repeat.will_repeat() {
                                schedule!(dur)
                            }
                        }
                    }
                }
            }
            ((Some(s), None), (true, _)) => match s.clone().cast_to::<Duration>() {
                Err(_) => error!(),
                Ok(dur) => self.timeout = Some(dur),
            },
            ((Some(s), Some(r)), (true, _)) => {
                match (s.clone().cast_to::<Duration>(), r.clone().cast_to::<Repeat>()) {
                    (Err(_), _) | (_, Err(_)) => error!(),
                    (Ok(dur), Ok(repeat)) => {
                        self.timeout = Some(dur);
                        self.repeat = repeat;
                        schedule!(dur)
                    }
                }
            }
            ((_, _), (false, false))
            | ((None, None), (_, _))
            | ((None, _), (true, false))
            | ((_, None), (false, true)) => (),
        }
        match event {
            Event::Init
            | Event::Variable { .. }
            | Event::Netidx(_, _)
            | Event::Rpc(_, _)
            | Event::User(_) => None,
            Event::Timer(id, now) => {
                if self.id != Some(*id) {
                    None
                } else {
                    self.id = None;
                    self.repeat -= 1;
                    if let Some(dur) = self.timeout {
                        if self.repeat.will_repeat() {
                            schedule!(dur)
                        }
                    }
                    Some(now.clone())
                }
            }
        }
    }
}
