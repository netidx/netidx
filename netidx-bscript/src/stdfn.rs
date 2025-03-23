use crate::{
    expr::{Expr, ExprId, VNAME},
    vm::{Apply, Ctx, Event, ExecCtx, InitFn, Node, Register},
};
use fxhash::{FxBuildHasher, FxHashSet};
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{self, Dval, Typ, UpdatesFlags, Value},
};
use netidx_core::utils::Either;
use std::{collections::HashSet, iter, marker::PhantomData, sync::Arc};

pub struct CachedVals(pub Vec<Option<Value>>);

impl CachedVals {
    pub fn new<C: Ctx, E: Clone>(
        from: &[Node<C, E>],
        ctx: &mut ExecCtx<C, E>,
    ) -> CachedVals {
        CachedVals(from.into_iter().map(|s| s.current(ctx)).collect())
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

    pub fn flat_iter<'a>(&'a self) -> impl Iterator<Item = Option<Value>> + 'a {
        self.0.iter().flat_map(|v| match v {
            None => Either::Left(iter::once(None)),
            Some(v) => Either::Right(v.clone().flatten().map(Some)),
        })
    }
}

pub struct Any(Option<Value>);

impl<C: Ctx, E: Clone> Register<C, E> for Any {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, _| {
            Box::new(Any(from.iter().find_map(|s| s.current(ctx))))
        });
        ctx.functions.insert("any".into(), f);
        ctx.user.register_fn("any".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Any {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        self.0.clone()
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        let res =
            from.into_iter().filter_map(|s| s.update(ctx, event)).fold(None, |res, v| {
                match res {
                    None => Some(v),
                    Some(_) => res,
                }
            });
        self.0 = res.clone();
        res
    }
}

pub struct Once {
    val: Option<Value>,
    invalid: bool,
}

impl<C: Ctx, E: Clone> Register<C, E> for Once {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, _| match from {
            [s] => Box::new(Once { val: s.current(ctx), invalid: false }),
            _ => Box::new(Once { val: None, invalid: true }),
        });
        ctx.functions.insert("once".into(), f);
        ctx.user.register_fn("once".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Once {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        self.usage()
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [s] => s.update(ctx, event).and_then(|v| match self.val {
                Some(_) => None,
                None => {
                    self.invalid = false;
                    self.val = Some(v);
                    self.usage()
                }
            }),
            exprs => {
                self.invalid = true;
                let mut up = false;
                for e in exprs {
                    up |= e.update(ctx, event).is_some();
                }
                if up {
                    self.usage()
                } else {
                    None
                }
            }
        }
    }
}

impl Once {
    fn usage(&self) -> Option<Value> {
        if self.invalid {
            let e = Chars::from("once(v): expected one argument");
            Some(Value::Error(e))
        } else {
            self.val.clone()
        }
    }
}

pub struct Do(Option<Value>);

impl<C: Ctx, E: Clone> Register<C, E> for Do {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, _| {
            Box::new(Do(from.iter().fold(None, |_, s| s.current(ctx))))
        });
        ctx.functions.insert("do".into(), f);
        ctx.user.register_fn("do".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Do {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        self.0.clone()
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        self.0 = from.into_iter().fold(None, |_, src| src.update(ctx, event));
        self.0.clone()
    }
}

pub trait CachedCurEval {
    fn eval(from: &CachedVals) -> Option<Value>;
    fn name() -> &'static str;
}

pub struct CachedCur<T: CachedCurEval + Send + Sync> {
    cached: CachedVals,
    current: Option<Value>,
    t: PhantomData<T>,
}

impl<C: Ctx, E: Clone, T: CachedCurEval + Send + Sync + 'static> Register<C, E>
    for CachedCur<T>
{
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, _| {
            let cached = CachedVals::new(from, ctx);
            let current = T::eval(&cached);
            Box::new(CachedCur::<T> { cached, current, t: PhantomData })
        });
        ctx.functions.insert(T::name().into(), f);
        ctx.user.register_fn(T::name().into(), Path::root());
    }
}

impl<C: Ctx, E: Clone, T: CachedCurEval + Send + Sync + 'static> Apply<C, E>
    for CachedCur<T>
{
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        self.current.clone()
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if !self.cached.update(ctx, from, event) {
            None
        } else {
            let cur = T::eval(&self.cached);
            self.current = cur.clone();
            cur
        }
    }
}

pub struct AllEv;

impl CachedCurEval for AllEv {
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

pub type All = CachedCur<AllEv>;

pub struct ArrayEv;

impl CachedCurEval for ArrayEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        if from.0.iter().all(|v| v.is_some()) {
            Some(Value::Array(Arc::from_iter(
                from.0.iter().filter_map(|v| v.as_ref().map(|v| v.clone())),
            )))
        } else {
            None
        }
    }

    fn name() -> &'static str {
        "array"
    }
}

pub type Array = CachedCur<ArrayEv>;

fn add_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) | (Some(_), None) => None,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l + r),
    }
}

pub struct SumEv;

impl CachedCurEval for SumEv {
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

pub type Sum = CachedCur<SumEv>;

pub struct ProductEv;

fn prod_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) | (Some(_), None) => None,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l * r),
    }
}

impl CachedCurEval for ProductEv {
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

pub type Product = CachedCur<ProductEv>;

pub struct DivideEv;

fn div_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) | (Some(_), None) => None,
        (None, r @ Some(_)) => r,
        (Some(l), Some(r)) => Some(l / r),
    }
}

impl CachedCurEval for DivideEv {
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

pub type Divide = CachedCur<DivideEv>;

pub struct MinEv;

impl CachedCurEval for MinEv {
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

pub type Min = CachedCur<MinEv>;

pub struct MaxEv;

impl CachedCurEval for MaxEv {
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

pub type Max = CachedCur<MaxEv>;

pub struct AndEv;

impl CachedCurEval for AndEv {
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

pub type And = CachedCur<AndEv>;

pub struct OrEv;

impl CachedCurEval for OrEv {
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

pub type Or = CachedCur<OrEv>;

pub struct NotEv;

impl CachedCurEval for NotEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [v] => v.as_ref().map(|v| !(v.clone())),
            _ => Some(Value::Error(Chars::from("not expected 1 argument"))),
        }
    }

    fn name() -> &'static str {
        "not"
    }
}

pub type Not = CachedCur<NotEv>;

pub struct IsErrEv;

impl CachedCurEval for IsErrEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [v] => v.as_ref().map(|v| match v {
                Value::Error(_) => Value::True,
                _ => Value::False,
            }),
            _ => Some(Value::Error(Chars::from("is_error expected 1 argument"))),
        }
    }

    fn name() -> &'static str {
        "is_error"
    }
}

pub type IsErr = CachedCur<IsErrEv>;

// CR estokes: document
pub struct StartsWithEv;

impl CachedCurEval for StartsWithEv {
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
            _ => Some(Value::Error(Chars::from("starts_with expected 2 arguments"))),
        }
    }

    fn name() -> &'static str {
        "starts_with"
    }
}

pub type StartsWith = CachedCur<StartsWithEv>;

pub struct IndexEv;

impl CachedCurEval for IndexEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::Array(elts)), Some(Value::I64(i))] if *i >= 0 => {
                let i = *i as usize;
                if i < elts.len() {
                    Some(elts[i].clone())
                } else {
                    Some(Value::Error(Chars::from("array index out of bounds")))
                }
            }
            [None, _] | [_, None] => None,
            _ => Some(Value::Error(Chars::from(
                "index(array, index): expected an array and a positive index",
            ))),
        }
    }

    fn name() -> &'static str {
        "index"
    }
}

pub type Index = CachedCur<IndexEv>;

// CR estokes: document
pub struct EndsWithEv;

impl CachedCurEval for EndsWithEv {
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
            _ => Some(Value::Error(Chars::from("ends_with expected 2 arguments"))),
        }
    }

    fn name() -> &'static str {
        "ends_with"
    }
}

pub type EndsWith = CachedCur<EndsWithEv>;

// CR estokes: document
pub struct ContainsEv;

impl CachedCurEval for ContainsEv {
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
            _ => Some(Value::Error(Chars::from("contains expected 2 arguments"))),
        }
    }

    fn name() -> &'static str {
        "contains"
    }
}

pub type Contains = CachedCur<ContainsEv>;

pub struct StripPrefixEv;

impl CachedCurEval for StripPrefixEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(pfx)), Some(Value::String(val))] => val
                .strip_prefix(&**pfx)
                .map(|s| Value::String(Chars::from(String::from(s)))),
            [None, _] | [_, None] => None,
            _ => Some(Value::Error(Chars::from("strip_prefix expected 2 arguments"))),
        }
    }

    fn name() -> &'static str {
        "strip_prefix"
    }
}

pub type StripPrefix = CachedCur<StripPrefixEv>;

pub struct StripSuffixEv;

impl CachedCurEval for StripSuffixEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(sfx)), Some(Value::String(val))] => val
                .strip_suffix(&**sfx)
                .map(|s| Value::String(Chars::from(String::from(s)))),
            [None, _] | [_, None] => None,
            _ => Some(Value::Error(Chars::from("strip_suffix expected 2 arguments"))),
        }
    }

    fn name() -> &'static str {
        "strip_suffix"
    }
}

pub type StripSuffix = CachedCur<StripSuffixEv>;

pub struct TrimEv;

impl CachedCurEval for TrimEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(val))] => {
                Some(Value::String(Chars::from(String::from(val.trim()))))
            }
            [None] => None,
            _ => Some(Value::Error(Chars::from("trim expected 1 arguments"))),
        }
    }

    fn name() -> &'static str {
        "trim"
    }
}

pub type Trim = CachedCur<TrimEv>;

pub struct TrimStartEv;

impl CachedCurEval for TrimStartEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(val))] => {
                Some(Value::String(Chars::from(String::from(val.trim_start()))))
            }
            [None] => None,
            _ => Some(Value::Error(Chars::from("trim_start expected 1 arguments"))),
        }
    }

    fn name() -> &'static str {
        "trim_start"
    }
}

pub type TrimStart = CachedCur<TrimStartEv>;

pub struct TrimEndEv;

impl CachedCurEval for TrimEndEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(val))] => {
                Some(Value::String(Chars::from(String::from(val.trim_end()))))
            }
            [None] => None,
            _ => Some(Value::Error(Chars::from("trim_start expected 1 arguments"))),
        }
    }

    fn name() -> &'static str {
        "trim_end"
    }
}

pub type TrimEnd = CachedCur<TrimEndEv>;

pub struct ReplaceEv;

impl CachedCurEval for ReplaceEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(pat)), Some(Value::String(rep)), Some(Value::String(val))] => {
                Some(Value::String(Chars::from(String::from(
                    val.replace(&**pat, &**rep),
                ))))
            }
            [None, _, _] | [_, None, _] | [_, _, None] => None,
            _ => Some(Value::Error(Chars::from("replace expected 3 arguments"))),
        }
    }

    fn name() -> &'static str {
        "replace"
    }
}

pub type Replace = CachedCur<ReplaceEv>;

pub struct DirnameEv;

impl CachedCurEval for DirnameEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(path))] => match Path::dirname(path) {
                None => Some(Value::Null),
                Some(dn) => Some(Value::String(Chars::from(String::from(dn)))),
            },
            [None] => None,
            _ => Some(Value::Error(Chars::from("dirname expected 1 argument"))),
        }
    }

    fn name() -> &'static str {
        "dirname"
    }
}

pub type Dirname = CachedCur<DirnameEv>;

pub struct BasenameEv;

impl CachedCurEval for BasenameEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [Some(Value::String(path))] => match Path::basename(path) {
                None => Some(Value::Null),
                Some(dn) => Some(Value::String(Chars::from(String::from(dn)))),
            },
            [None] => None,
            _ => Some(Value::Error(Chars::from("basename expected 1 argument"))),
        }
    }

    fn name() -> &'static str {
        "basename"
    }
}

pub type Basename = CachedCur<BasenameEv>;

pub struct CmpEv;

impl CachedCurEval for CmpEv {
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
                            "invalid op {}, expected eq, lt, gt, lte, or gte",
                            op
                        ))),
                    }),
                },
                Some(_) => Some(Value::Error(Chars::from(
                    "cmp(op, v0, v1): expected op to be a string",
                ))),
            },
            _ => Some(Value::Error(Chars::from("cmp(op, v0, v1): expected 3 arguments"))),
        }
    }
}

pub type Cmp = CachedCur<CmpEv>;

pub struct IfEv;

impl CachedCurEval for IfEv {
    fn name() -> &'static str {
        "if"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [cond, b1] => match cond {
                None => None,
                Some(Value::True) => b1.clone(),
                Some(Value::False) => None,
                _ => Some(Value::Error(Chars::from(
                    "if(predicate, caseIf, [caseElse]): expected boolean condition",
                ))),
            },
            [cond, b1, b2] => match cond {
                None => None,
                Some(Value::True) => b1.clone(),
                Some(Value::False) => b2.clone(),
                _ => Some(Value::Error(Chars::from(
                    "if(predicate, caseIf, [caseElse]): expected boolean condition",
                ))),
            },
            _ => Some(Value::Error(Chars::from(
                "if(predicate, caseIf, [caseElse]): expected at least 2 arguments",
            ))),
        }
    }
}

pub type If = CachedCur<IfEv>;

pub struct FilterEv;

impl CachedCurEval for FilterEv {
    fn name() -> &'static str {
        "filter"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [pred, s] => match pred {
                None => None,
                Some(Value::True) => s.clone(),
                Some(Value::False) => None,
                _ => Some(Value::Error(Chars::from(
                    "filter(predicate, source) expected boolean predicate",
                ))),
            },
            _ => Some(Value::Error(Chars::from(
                "filter(predicate, source): expected 2 arguments",
            ))),
        }
    }
}

pub type Filter = CachedCur<FilterEv>;

pub struct FilterErrEv;

impl CachedCurEval for FilterErrEv {
    fn name() -> &'static str {
        "filter_err"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        match &*from.0 {
            [s] => match s {
                None | Some(Value::Error(_)) => None,
                Some(_) => s.clone(),
            },
            _ => {
                Some(Value::Error(Chars::from("filter_err(source): expected 1 argument")))
            }
        }
    }
}

pub type FilterErr = CachedCur<FilterErrEv>;

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
                Err(e) => Some(Value::Error(Chars::from(format!(
                    "{}: invalid type {}, {}",
                    name, s, e
                )))),
            },
            _ => Some(Value::Error(Chars::from(format!(
                "{} expected typ as string",
                name
            )))),
        },
        _ => Some(Value::Error(Chars::from(format!("{} expected 2 arguments", name)))),
    }
}

impl CachedCurEval for CastEv {
    fn name() -> &'static str {
        "cast"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        with_typ_prefix(from, "cast(typ, src)", |typ, v| {
            v.as_ref().and_then(|v| v.clone().cast(typ))
        })
    }
}

pub type Cast = CachedCur<CastEv>;

pub struct IsaEv;

impl CachedCurEval for IsaEv {
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

pub type Isa = CachedCur<IsaEv>;

pub struct StringJoinEv;

impl CachedCurEval for StringJoinEv {
    fn name() -> &'static str {
        "string_join"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        use bytes::BytesMut;
        match &from.0[..] {
            [_] | [] => Some(Value::Error(Chars::from(
                "string_join(sep, s0, s1, ...): expected at least 2 arguments",
            ))),
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
                        None => {
                            return Some(Value::Error(Chars::from(
                                "string_join, separator must be a string",
                            )))
                        }
                    },
                };
                let mut res = BytesMut::new();
                for p in parts {
                    let c = match p.as_ref().unwrap() {
                        Value::String(c) => c.clone(),
                        v => match v.clone().cast_to::<Chars>().ok() {
                            Some(c) => c,
                            None => {
                                return Some(Value::Error(Chars::from(
                                    "string_join, components must be strings",
                                )))
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

pub type StringJoin = CachedCur<StringJoinEv>;

pub struct StringConcatEv;

impl CachedCurEval for StringConcatEv {
    fn name() -> &'static str {
        "string_concat"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        use bytes::BytesMut;
        match &from.0[..] {
            [] => Some(Value::Error(Chars::from(
                "string_concat: expected at least 1 argument",
            ))),
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
                                return Some(Value::Error(Chars::from(
                                    "string_concat: arguments must be strings",
                                )))
                            }
                        },
                    }
                }
                Some(Value::String(Chars::from_bytes(res.freeze()).unwrap()))
            }
        }
    }
}

pub type StringConcat = CachedCur<StringConcatEv>;

pub struct Eval<C: Ctx, E> {
    cached: CachedVals,
    current: Result<Node<C, E>, Value>,
    scope: Path,
}

impl<C: Ctx, E: Clone> Eval<C, E> {
    fn compile(&mut self, ctx: &mut ExecCtx<C, E>) {
        self.current = match &*self.cached.0 {
            [None] => Err(Value::Null),
            [Some(v)] => match v {
                Value::String(s) => match s.parse::<Expr>() {
                    Ok(spec) => Ok(Node::compile(ctx, self.scope.clone(), spec)),
                    Err(e) => {
                        let e = format!("eval(src), error parsing formula {}, {}", s, e);
                        Err(Value::Error(Chars::from(e)))
                    }
                },
                v => {
                    let e = format!("eval(src) expected 1 string argument, not {}", v);
                    Err(Value::Error(Chars::from(e)))
                }
            },
            _ => Err(Value::Error(Chars::from("eval(src) expected 1 argument"))),
        }
    }
}

impl<C: Ctx, E: Clone> Register<C, E> for Eval<C, E> {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, scope, _| {
            let mut t = Eval {
                cached: CachedVals::new(from, ctx),
                current: Err(Value::Null),
                scope,
            };
            t.compile(ctx);
            Box::new(t)
        });
        ctx.functions.insert("eval".into(), f);
        ctx.user.register_fn("eval".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Eval<C, E> {
    fn current(&self, ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        match &self.current {
            Ok(s) => s.current(ctx),
            Err(Value::Null) => None,
            Err(v) => Some(v.clone()),
        }
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if self.cached.update(ctx, from, event) {
            self.compile(ctx);
        }
        match &mut self.current {
            Ok(s) => s.update(ctx, event),
            Err(Value::Null) => None,
            Err(v) => Some(v.clone()),
        }
    }
}

pub struct Count {
    from: CachedVals,
    count: u64,
}

impl<C: Ctx, E: Clone> Register<C, E> for Count {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, _| {
            Box::new(Count { from: CachedVals::new(from, ctx), count: 0 })
        });
        ctx.functions.insert("count".into(), f);
        ctx.user.register_fn("count".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Count {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        match &*self.from.0 {
            [] => Some(Value::Error(Chars::from("count(s): requires 1 argument"))),
            [_] => Some(Value::U64(self.count)),
            _ => Some(Value::Error(Chars::from("count(s): requires 1 argument"))),
        }
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if self.from.update(ctx, from, event) {
            self.count += 1;
            Apply::<C, E>::current(self, ctx)
        } else {
            None
        }
    }
}

pub struct Sample {
    current: Option<Value>,
}

impl<C: Ctx, E: Clone> Register<C, E> for Sample {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, _| {
            let current = match from {
                [trigger, source] => match trigger.current(ctx) {
                    None => None,
                    Some(_) => source.current(ctx),
                },
                _ => Some(Value::Error(Chars::from(
                    "sample(trigger, source): expected 2 arguments",
                ))),
            };
            Box::new(Sample { current })
        });
        ctx.functions.insert("sample".into(), f);
        ctx.user.register_fn("sample".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Sample {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        self.current.clone()
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [trigger, source] => {
                source.update(ctx, event);
                if trigger.update(ctx, event).is_none() {
                    None
                } else {
                    let v = source.current(ctx);
                    self.current = v.clone();
                    v
                }
            }
            _ => {
                let v = Some(Value::Error(Chars::from(
                    "sample(trigger, source): expected 2 arguments",
                )));
                self.current = v.clone();
                v
            }
        }
    }
}

pub struct Mean {
    from: CachedVals,
    total: f64,
    samples: usize,
}

impl<C: Ctx, E: Clone> Register<C, E> for Mean {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, _| {
            Box::new(Mean { from: CachedVals::new(from, ctx), total: 0., samples: 0 })
        });
        ctx.functions.insert("mean".into(), f);
        ctx.user.register_fn("mean".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Mean {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        match &*self.from.0 {
            [] => Some(Value::Error(Chars::from("mean(s): requires 1 argument"))),
            [_] => {
                if self.samples > 0 {
                    Some(Value::F64(self.total / (self.samples as f64)))
                } else {
                    None
                }
            }
            _ => Some(Value::Error(Chars::from("mean(s): requires 1 argument"))),
        }
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if self.from.update(ctx, from, event) {
            for v in self.from.flat_iter() {
                if let Some(v) = v {
                    if let Ok(v) = v.clone().cast_to::<f64>() {
                        self.total += v;
                        self.samples += 1;
                    }
                }
            }
            Apply::<C, E>::current(self, ctx)
        } else {
            None
        }
    }
}

pub(crate) struct Uniq(Option<Value>);

impl<C: Ctx, E: Clone> Register<C, E> for Uniq {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, _| {
            let mut t = Uniq(None);
            match from {
                [e] => t.0 = e.current(ctx),
                _ => t.0 = Uniq::usage(),
            }
            Box::new(t)
        });
        ctx.functions.insert("uniq".into(), f);
        ctx.user.register_fn("uniq".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Uniq {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        self.0.as_ref().cloned()
    }

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
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(ctx, event).is_some() || up;
                }
                self.0 = Uniq::usage();
                if up {
                    Apply::<C, E>::current(self, ctx)
                } else {
                    None
                }
            }
        }
    }
}

impl Uniq {
    fn usage() -> Option<Value> {
        Some(Value::Error(Chars::from("uniq(e): expected 1 argument")))
    }
}

fn pathname(invalid: &mut bool, path: Option<Value>) -> Option<Path> {
    *invalid = false;
    match path.map(|v| v.cast_to::<String>()) {
        None => None,
        Some(Ok(p)) => {
            if Path::is_absolute(&p) {
                Some(Path::from(p))
            } else {
                *invalid = true;
                None
            }
        }
        Some(Err(_)) => {
            *invalid = true;
            None
        }
    }
}

pub struct Store {
    queued: Vec<Value>,
    top_id: ExprId,
    dv: Option<(Path, Dval)>,
    invalid: bool,
}

impl<C: Ctx, E: Clone> Register<C, E> for Store {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, top_id| {
            let mut t = Store { queued: Vec::new(), dv: None, invalid: false, top_id };
            match from {
                [to, val] => {
                    let to = to.current(ctx);
                    let val = val.current(ctx);
                    t.set(ctx, to, val)
                }
                _ => t.invalid = true,
            }
            Box::new(t)
        });
        ctx.functions.insert("store".into(), f);
        ctx.user.register_fn("store".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Store {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        if self.invalid {
            Some(Value::Error(Chars::from(
                "store(tgt: absolute path, val): expected 2 arguments",
            )))
        } else {
            None
        }
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [path, val] => {
                let path = path.update(ctx, event);
                let value = val.update(ctx, event);
                let value = if path.is_some() && !self.same_path(&path) {
                    value.or_else(|| val.current(ctx))
                } else {
                    value
                };
                let up = value.is_some();
                self.set(ctx, path, value);
                if up {
                    Apply::<C, E>::current(self, ctx)
                } else {
                    None
                }
            }
            exprs => {
                let mut up = false;
                for expr in exprs {
                    up = expr.update(ctx, event).is_some() || up;
                }
                self.invalid = true;
                if up {
                    Apply::<C, E>::current(self, ctx)
                } else {
                    None
                }
            }
        }
    }
}

impl Store {
    fn queue(&mut self, v: Value) {
        self.queued.push(v)
    }

    fn set<C: Ctx, E>(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        to: Option<Value>,
        val: Option<Value>,
    ) {
        match (pathname(&mut self.invalid, to), val) {
            (None, None) => (),
            (None, Some(v)) => match self.dv.as_ref() {
                None => self.queue(v),
                Some((_, dv)) => {
                    dv.write(v);
                }
            },
            (Some(path), val) => {
                if let Some(v) = val {
                    self.queue(v)
                }
                match &self.dv {
                    Some((cur, dv)) if &path == cur => {
                        for v in self.queued.drain(..) {
                            dv.write(v);
                        }
                    }
                    None | Some((_, _)) => {
                        if let Some((cur, dv)) = self.dv.take() {
                            ctx.user.unsubscribe(cur, dv, self.top_id);
                        }
                        let dv = ctx.user.durable_subscribe(
                            UpdatesFlags::empty(),
                            path.clone(),
                            self.top_id,
                        );
                        for v in self.queued.drain(..) {
                            dv.write(v);
                        }
                        self.dv = Some((path, dv));
                    }
                }
            }
        }
    }

    fn same_path(&self, new_path: &Option<Value>) -> bool {
        match (new_path.as_ref(), self.dv.as_ref()) {
            (Some(Value::String(p0)), Some((p1, _))) => &**p0 == &**p1,
            _ => false,
        }
    }
}

fn varname(invalid: &mut bool, name: Option<Value>) -> Option<Chars> {
    *invalid = false;
    match name.map(|n| n.cast_to::<Chars>()) {
        None => None,
        Some(Err(_)) => {
            *invalid = true;
            None
        }
        Some(Ok(n)) => {
            if VNAME.is_match(&n) {
                Some(n)
            } else {
                *invalid = true;
                None
            }
        }
    }
}

pub struct Set {
    queued: Vec<Value>,
    local: bool,
    scope: Path,
    name: Option<Chars>,
    invalid: bool,
}

impl<C: Ctx, E: Clone> Register<C, E> for Set {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f = |local| -> InitFn<C, E> {
            Arc::new(move |ctx, from, scope, _| {
                let mut t =
                    Set { queued: Vec::new(), local, scope, name: None, invalid: false };
                match from {
                    [name, value] => {
                        let name = name.current(ctx);
                        let value = value.current(ctx);
                        t.set(ctx, name, value)
                    }
                    _ => t.invalid = true,
                }
                Box::new(t)
            })
        };
        ctx.functions.insert("set".into(), f(false));
        ctx.user.register_fn("set".into(), Path::root());
        ctx.functions.insert("let".into(), f(true));
        ctx.user.register_fn("let".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Set {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        if self.invalid {
            Some(Value::Error(Chars::from(
                "set/let(name: string [a-z][a-z0-9_]+, value): expected 2 arguments",
            )))
        } else {
            None
        }
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [name, val] => {
                let name = name.update(ctx, event);
                let value = val.update(ctx, event);
                let value = if name.is_some() && !self.same_name(&name) {
                    value.or_else(|| val.current(ctx))
                } else {
                    value
                };
                let up = value.is_some();
                self.set(ctx, name, value);
                if up {
                    Apply::<C, E>::current(self, ctx)
                } else {
                    None
                }
            }
            exprs => {
                let mut up = false;
                for expr in exprs {
                    up = expr.update(ctx, event).is_some() || up;
                }
                self.invalid = true;
                if up {
                    Apply::<C, E>::current(self, ctx)
                } else {
                    None
                }
            }
        }
    }
}

impl Set {
    fn queue_set(&mut self, v: Value) {
        self.queued.push(v)
    }

    fn set<C: Ctx, E>(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        name: Option<Value>,
        value: Option<Value>,
    ) {
        if let Some(name) = varname(&mut self.invalid, name) {
            for v in self.queued.drain(..) {
                ctx.user.set_var(
                    &mut ctx.variables,
                    self.local,
                    self.scope.clone(),
                    name.clone(),
                    v,
                )
            }
            self.name = Some(name);
        }
        if let Some(value) = value {
            match self.name.as_ref() {
                None => self.queue_set(value),
                Some(name) => ctx.user.set_var(
                    &mut ctx.variables,
                    self.local,
                    self.scope.clone(),
                    name.clone(),
                    value,
                ),
            }
        }
    }

    fn same_name(&self, new_name: &Option<Value>) -> bool {
        match (new_name, self.name.as_ref()) {
            (Some(Value::String(n0)), Some(n1)) => n0 == n1,
            _ => false,
        }
    }
}

pub(crate) struct Load {
    path: Option<Path>,
    cur: Option<Dval>,
    top_id: ExprId,
    invalid: bool,
}

impl<C: Ctx, E: Clone> Register<C, E> for Load {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, top_id| {
            let mut t = Load { path: None, cur: None, invalid: false, top_id };
            match from {
                [path] => {
                    let path = path.current(ctx);
                    t.subscribe(ctx, path)
                }
                _ => t.invalid = true,
            }
            Box::new(t)
        });
        ctx.functions.insert("load".into(), f);
        ctx.user.register_fn("load".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Load {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        if self.invalid {
            Load::err()
        } else {
            self.cur.as_ref().and_then(|dv| match dv.last() {
                subscriber::Event::Unsubscribed => {
                    Some(Value::Error(Chars::from("#LOST")))
                }
                subscriber::Event::Update(v) => Some(v),
            })
        }
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [name] => {
                let up = match name.update(ctx, event) {
                    None => false,
                    Some(target) => {
                        self.subscribe(ctx, Some(target));
                        true
                    }
                };
                if self.invalid {
                    if up {
                        Load::err()
                    } else {
                        None
                    }
                } else {
                    self.cur.as_ref().and_then(|dv| match event {
                        Event::Variable(_, _, _)
                        | Event::Rpc(_, _)
                        | Event::Timer(_)
                        | Event::User(_) => None,
                        Event::Netidx(id, value) if dv.id() == *id => Some(value.clone()),
                        Event::Netidx(_, _) => None,
                    })
                }
            }
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(ctx, event).is_some() || up;
                }
                self.invalid = true;
                if up {
                    Load::err()
                } else {
                    None
                }
            }
        }
    }
}

impl Load {
    fn subscribe<C: Ctx, E>(&mut self, ctx: &mut ExecCtx<C, E>, name: Option<Value>) {
        if let Some(path) = pathname(&mut self.invalid, name) {
            if Some(&path) != self.path.as_ref() {
                if let (Some(path), Some(dv)) = (self.path.take(), self.cur.take()) {
                    ctx.user.unsubscribe(path, dv, self.top_id);
                }
                self.path = Some(path.clone());
                self.cur = Some(ctx.user.durable_subscribe(
                    UpdatesFlags::BEGIN_WITH_LAST,
                    path,
                    self.top_id,
                ));
            }
        }
    }

    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from(
            "load(expr: path) expected 1 absolute path as argument",
        )))
    }
}

struct BoundVar {
    scope: Path,
    value: Value,
}

pub struct Get {
    scope: Path,
    name: Option<Chars>,
    var: Option<BoundVar>,
    top_id: ExprId,
    invalid: bool,
}

impl<C: Ctx, E: Clone> Register<C, E> for Get {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, scope, top_id| {
            let mut t = Get { scope, name: None, var: None, invalid: false, top_id };
            match from {
                [name] => {
                    let name = name.current(ctx);
                    t.subscribe(ctx, name)
                }
                _ => t.invalid = true,
            }
            Box::new(t)
        });
        ctx.functions.insert("get".into(), f);
        ctx.user.register_fn("get".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Get {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        if self.invalid {
            Get::err()
        } else {
            self.var.as_ref().map(|bv| bv.value.clone())
        }
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [name] => {
                let up = match name.update(ctx, event) {
                    None => false,
                    Some(target) => {
                        self.subscribe(ctx, Some(target));
                        true
                    }
                };
                if self.invalid {
                    if up {
                        Get::err()
                    } else {
                        None
                    }
                } else {
                    match (self.name.as_ref(), self.var.as_mut(), event) {
                        (Some(vn), Some(bv), Event::Variable(es, en, v))
                            if vn == en
                                && Path::is_parent(es, &self.scope)
                                && Path::is_parent(&bv.scope, es) =>
                        {
                            if &bv.scope != es {
                                bv.scope = es.clone();
                            }
                            bv.value = v.clone();
                            Some(v.clone())
                        }
                        (Some(vn), None, Event::Variable(es, en, v))
                            if vn == en && Path::is_parent(es, &self.scope) =>
                        {
                            self.var =
                                Some(BoundVar { scope: es.clone(), value: v.clone() });
                            Some(v.clone())
                        }
                        (None, _, _)
                        | (Some(_), _, Event::Netidx(_, _))
                        | (Some(_), _, Event::User(_))
                        | (Some(_), _, Event::Rpc(_, _))
                        | (Some(_), _, Event::Timer(_))
                        | (Some(_), _, Event::Variable(_, _, _)) => None,
                    }
                }
            }
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(ctx, event).is_some() || up;
                }
                self.invalid = true;
                if up {
                    Get::err()
                } else {
                    None
                }
            }
        }
    }
}

impl Get {
    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from(
            "load_var(expr: variable name): expected 1 variable name as argument",
        )))
    }

    fn subscribe<C: Ctx, E: Clone>(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        name: Option<Value>,
    ) {
        match varname(&mut self.invalid, name) {
            None => {
                self.var = None;
                self.name = None;
                if let Some(old) = self.name.take() {
                    ctx.user.unref_var(old.clone(), self.scope.clone(), self.top_id);
                }
            }
            Some(name) if self.name.as_ref() == Some(&name) => (),
            Some(name) => {
                if let Some(old) = self.name.take() {
                    ctx.user.unref_var(old.clone(), self.scope.clone(), self.top_id);
                }
                self.var = ctx.lookup_var(&self.scope, &name).map(|(scope, value)| {
                    BoundVar { scope: scope.clone(), value: value.clone() }
                });
                ctx.user.ref_var(name.clone(), self.scope.clone(), self.top_id);
                self.name = Some(name);
            }
        }
    }
}

atomic_id!(RpcCallId);

pub(crate) struct RpcCall {
    args: CachedVals,
    top_id: ExprId,
    pending: FxHashSet<RpcCallId>,
    current: Option<Value>,
    triggered: bool,
    invalid: bool,
}

impl<C: Ctx, E: Clone> Register<C, E> for RpcCall {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, top_id| {
            let triggered = match from {
                [trigger, ..] => trigger.current(ctx).is_some(),
                _ => false,
            };
            let mut t = RpcCall {
                args: CachedVals::new(
                    match from {
                        [_, rest @ ..] => rest,
                        [] => &[],
                    },
                    ctx,
                ),
                current: None,
                top_id,
                pending: HashSet::with_hasher(FxBuildHasher::default()),
                triggered,
                invalid: false,
            };
            t.maybe_call(ctx);
            Box::new(t)
        });
        ctx.functions.insert("call".into(), f);
        ctx.user.register_fn("call".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for RpcCall {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        self.current.clone()
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match event {
            Event::Rpc(id, v) if self.pending.remove(&id) => {
                self.current = Some(v.clone());
                Apply::<C, E>::current(self, ctx)
            }
            event => match from {
                [trigger, args @ ..] => {
                    self.args.update(ctx, args, event);
                    if trigger.update(ctx, event).is_some() {
                        self.triggered = true;
                    }
                    let triggered = self.triggered;
                    self.maybe_call(ctx);
                    if triggered && self.invalid {
                        Apply::<C, E>::current(self, ctx)
                    } else {
                        None
                    }
                }
                [] => {
                    self.invalid();
                    Apply::<C, E>::current(self, ctx)
                }
            },
        }
    }
}

impl RpcCall {
    fn invalid(&mut self) {
        let m = "call(trigger, rpc, kwargs): expected at least 2 arguments, and an even number of kwargs";
        self.current = Some(Value::Error(Chars::from(m)));
        self.invalid = true;
    }

    fn get_args(&mut self) -> Option<(Path, Vec<(Chars, Value)>)> {
        self.current = None;
        self.invalid = false;
        let len = self.args.0.len();
        if len == 0 || (len > 1 && len.is_power_of_two()) {
            self.invalid();
            None
        } else if self.args.0.iter().any(|v| v.is_none()) {
            None
        } else {
            match &self.args.0[..] {
                [] => {
                    self.invalid();
                    None
                }
                [path, args @ ..] => {
                    match path.as_ref().unwrap().clone().cast_to::<Chars>() {
                        Err(_) => {
                            self.invalid();
                            None
                        }
                        Ok(name) => {
                            let mut iter = args.into_iter();
                            let mut args = Vec::new();
                            loop {
                                match iter.next() {
                                    None | Some(None) => break,
                                    Some(Some(name)) => {
                                        match name.clone().cast_to::<Chars>() {
                                            Err(_) => {
                                                self.invalid();
                                                return None;
                                            }
                                            Ok(name) => match iter.next() {
                                                None | Some(None) => {
                                                    self.invalid();
                                                    return None;
                                                }
                                                Some(Some(val)) => {
                                                    args.push((name, val.clone()));
                                                }
                                            },
                                        }
                                    }
                                }
                            }
                            Some((Path::from(name), args))
                        }
                    }
                }
            }
        }
    }

    fn maybe_call<C: Ctx, E>(&mut self, ctx: &mut ExecCtx<C, E>) {
        if self.triggered {
            if let Some((name, args)) = self.get_args() {
                self.triggered = false;
                let id = RpcCallId::new();
                self.pending.insert(id);
                ctx.user.call_rpc(Path::from(name), args, self.top_id, id);
            }
        }
    }
}

atomic_id!(TimerId);

pub(crate) struct AfterIdle {
    timeout: Option<Value>,
    cur: Option<Value>,
    updated: bool,
    timer_set: bool,
    id: TimerId,
    eid: ExprId,
    invalid: bool,
}

impl<C: Ctx, E: Clone> Register<C, E> for AfterIdle {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, eid| match from {
            [timeout, cur] => {
                let mut t = AfterIdle {
                    timeout: timeout.current(ctx),
                    cur: cur.current(ctx),
                    updated: false,
                    timer_set: false,
                    id: TimerId::new(),
                    eid,
                    invalid: false,
                };
                t.maybe_set_timer(ctx);
                Box::new(t)
            }
            _ => Box::new(AfterIdle {
                timeout: None,
                cur: None,
                updated: false,
                timer_set: false,
                id: TimerId::new(),
                eid,
                invalid: true,
            }),
        });
        ctx.functions.insert("after_idle".into(), f);
        ctx.user.register_fn("after_idle".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for AfterIdle {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        self.usage()
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [timeout, cur] => {
                if let Some(timeout) = timeout.update(ctx, event) {
                    self.timeout = Some(timeout);
                    self.maybe_set_timer(ctx);
                }
                if let Some(cur) = cur.update(ctx, event) {
                    self.updated = true;
                    self.cur = Some(cur);
                    self.maybe_set_timer(ctx);
                }
                match event {
                    Event::Variable(_, _, _)
                    | Event::Netidx(_, _)
                    | Event::Rpc(_, _)
                    | Event::User(_) => self.usage(),
                    Event::Timer(id) => {
                        if id != &self.id {
                            self.usage()
                        } else {
                            self.timer_set = false;
                            if self.updated {
                                self.maybe_set_timer(ctx);
                                self.usage()
                            } else {
                                self.cur.clone()
                            }
                        }
                    }
                }
            }
            exprs => {
                let mut up = false;
                self.invalid = true;
                for expr in exprs {
                    up |= expr.update(ctx, event).is_some();
                }
                if up {
                    self.usage()
                } else {
                    None
                }
            }
        }
    }
}

impl AfterIdle {
    fn maybe_set_timer<C: Ctx, E>(&mut self, ctx: &mut ExecCtx<C, E>) {
        use std::time::Duration;
        if !self.invalid && !self.timer_set {
            match (&self.timeout, &self.cur) {
                (Some(timeout), Some(_)) => match timeout.clone().cast_to::<f64>() {
                    Err(_) => {
                        self.invalid = true;
                    }
                    Ok(timeout) => {
                        self.invalid = false;
                        self.updated = false;
                        self.timer_set = true;
                        ctx.user.set_timer(
                            self.id,
                            Duration::from_secs_f64(timeout),
                            self.eid,
                        );
                    }
                },
                (_, _) => (),
            }
        }
    }

    fn usage(&self) -> Option<Value> {
        if self.invalid {
            Some(Value::Error(Chars::from(
                "after_idle(timeout: f64, v: any): expected two arguments",
            )))
        } else {
            None
        }
    }
}

pub(crate) struct Timer {
    id: TimerId,
    eid: ExprId,
    timeout: Option<Value>,
    repeat: Option<Value>,
    timer_set: bool,
    invalid: bool,
}

impl<C: Ctx, E: Clone> Register<C, E> for Timer {
    fn register(ctx: &mut ExecCtx<C, E>) {
        let f: InitFn<C, E> = Arc::new(|ctx, from, _, eid| match from {
            [timeout, repeat] => {
                let mut t = Self {
                    id: TimerId::new(),
                    eid,
                    timeout: timeout.current(ctx),
                    repeat: match repeat.current(ctx) {
                        Some(Value::False) => Some(1.into()),
                        v => v,
                    },
                    timer_set: false,
                    invalid: false,
                };
                t.maybe_set_timer(ctx);
                Box::new(t)
            }
            _ => Box::new(Self {
                id: TimerId::new(),
                eid,
                timeout: None,
                repeat: None,
                timer_set: false,
                invalid: true,
            }),
        });
        ctx.functions.insert("timer".into(), f);
        ctx.user.register_fn("timer".into(), Path::root());
    }
}

impl<C: Ctx, E: Clone> Apply<C, E> for Timer {
    fn current(&self, _ctx: &mut ExecCtx<C, E>) -> Option<Value> {
        self.usage()
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [timeout, repeat] => {
                if let Some(timeout) = timeout.update(ctx, event) {
                    self.timeout = Some(timeout);
                    self.maybe_set_timer(ctx);
                }
                if let Some(repeat) = repeat.update(ctx, event) {
                    self.repeat = Some(repeat);
                    self.maybe_set_timer(ctx);
                }
                match event {
                    Event::Variable(_, _, _)
                    | Event::Netidx(_, _)
                    | Event::Rpc(_, _)
                    | Event::User(_) => self.usage(),
                    Event::Timer(id) => {
                        if id != &self.id {
                            self.usage()
                        } else {
                            self.timer_set = false;
                            self.maybe_set_timer(ctx);
                            Some(Value::Null)
                        }
                    }
                }
            }
            exprs => {
                let mut up = false;
                self.invalid = true;
                for expr in exprs {
                    up |= expr.update(ctx, event).is_some();
                }
                if up {
                    self.usage()
                } else {
                    None
                }
            }
        }
    }
}

impl Timer {
    fn maybe_set_timer<C: Ctx, E>(&mut self, ctx: &mut ExecCtx<C, E>) {
        use std::time::Duration;
        if !self.invalid && !self.timer_set {
            match (&self.timeout, &self.repeat) {
                (None, _) | (_, None) => (),
                (Some(timeout), Some(repeat)) => {
                    let timeout = timeout.clone().cast_to::<f64>();
                    let repeat = match repeat {
                        Value::Null => Ok(None),
                        Value::True => Ok(None),
                        Value::False => Ok(Some(0)),
                        v => v.clone().cast_to::<u64>().map(|v| Some(v)),
                    };
                    match (timeout, repeat) {
                        (Ok(timeout), Ok(repeat)) => {
                            let go = match repeat {
                                None => true,
                                Some(n) => {
                                    if n == 0 {
                                        false
                                    } else {
                                        self.repeat = Some((n - 1).into());
                                        true
                                    }
                                }
                            };
                            if go {
                                self.timer_set = true;
                                let d = Duration::from_secs_f64(timeout);
                                ctx.user.set_timer(self.id, d, self.eid);
                            }
                        }
                        (_, _) => {
                            self.invalid = true;
                        }
                    }
                }
            }
        }
    }

    fn usage(&self) -> Option<Value> {
        if self.invalid {
            Some(Value::Error(Chars::from(
                "timer(timeout: f64, repeat): expected two arguments",
            )))
        } else {
            None
        }
    }
}
