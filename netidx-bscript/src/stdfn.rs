use crate::{
    expr::{Expr, VNAME},
    vm::{Event, Apply, Ctx, ExecCtx, InitFn, Node, Register},
};
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{self, Dval, Typ, Value},
};
use std::{
    cell::{Cell, RefCell},
    marker::PhantomData,
    rc::Rc,
};

#[derive(Debug, Clone)]
pub struct CachedVals(pub Rc<RefCell<Vec<Option<Value>>>>);

impl CachedVals {
    pub fn new<C: Ctx, E>(from: &[Node<C, E>]) -> CachedVals {
        CachedVals(Rc::new(RefCell::new(from.into_iter().map(|s| s.current()).collect())))
    }

    pub fn update<C: Ctx, E>(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> bool {
        let mut vals = self.0.borrow_mut();
        from.into_iter().enumerate().fold(false, |res, (i, src)| {
            match src.update(ctx, event) {
                None => res,
                v @ Some(_) => {
                    vals[i] = v;
                    true
                }
            }
        })
    }
}

pub struct Any(Rc<RefCell<Option<Value>>>);

impl<C: Ctx, E> Register<C, E> for Any {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|_ctx, from| {
            Box::new(Any(Rc::new(RefCell::new(from.iter().find_map(|s| s.current())))))
        });
        ctx.functions.borrow_mut().insert("any".into(), f);
    }
}

impl<C: Ctx, E> Apply<C, E> for Any {
    fn current(&self) -> Option<Value> {
        self.0.borrow().clone()
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        let res =
            from.into_iter().filter_map(|s| s.update(ctx, event)).fold(None, |res, v| {
                match res {
                    None => Some(v),
                    Some(_) => res,
                }
            });
        *self.0.borrow_mut() = res.clone();
        res
    }
}

pub trait CachedCurEval {
    fn eval(from: &CachedVals) -> Option<Value>;
    fn name() -> &'static str;
}

pub struct CachedCur<T: CachedCurEval> {
    cached: CachedVals,
    current: RefCell<Option<Value>>,
    t: PhantomData<T>,
}

impl<C: Ctx, E, T: CachedCurEval + 'static> Register<C, E> for CachedCur<T> {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|_ctx, from| {
            let cached = CachedVals::new(from);
            let current = RefCell::new(T::eval(&cached));
            Box::new(CachedCur::<T> { cached, current, t: PhantomData })
        });
        ctx.functions.borrow_mut().insert(T::name().into(), f);
    }
}

impl<C: Ctx, E, T: CachedCurEval + 'static> Apply<C, E> for CachedCur<T> {
    fn current(&self) -> Option<Value> {
        self.current.borrow().clone()
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if !self.cached.update(ctx, from, event) {
            None
        } else {
            let cur = T::eval(&self.cached);
            if cur == *self.current.borrow() {
                None
            } else {
                *self.current.borrow_mut() = cur.clone();
                cur
            }
        }
    }
}

pub struct AllEv;

impl CachedCurEval for AllEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &**from.0.borrow() {
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

fn add_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (Some(l), Some(r)) => Some(l + r),
    }
}

pub struct SumEv;

impl CachedCurEval for SumEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        from.0.borrow().iter().fold(None, |res, v| match res {
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
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (Some(l), Some(r)) => Some(l * r),
    }
}

impl CachedCurEval for ProductEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        from.0.borrow().iter().fold(None, |res, v| match res {
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
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (Some(l), Some(r)) => Some(l / r),
    }
}

impl CachedCurEval for DivideEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        from.0.borrow().iter().fold(None, |res, v| match res {
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
        from.0.borrow().iter().filter_map(|v| v.clone()).fold(None, |res, v| match res {
            None => Some(v),
            Some(v0) => {
                if v < v0 {
                    Some(v)
                } else {
                    Some(v0)
                }
            }
        })
    }

    fn name() -> &'static str {
        "min"
    }
}

pub type Min = CachedCur<MinEv>;

pub struct MaxEv;

impl CachedCurEval for MaxEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        from.0.borrow().iter().filter_map(|v| v.clone()).fold(None, |res, v| match res {
            None => Some(v),
            Some(v0) => {
                if v > v0 {
                    Some(v)
                } else {
                    Some(v0)
                }
            }
        })
    }

    fn name() -> &'static str {
        "max"
    }
}

pub type Max = CachedCur<MaxEv>;

pub struct AndEv;

impl CachedCurEval for AndEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        let res = from.0.borrow().iter().all(|v| match v {
            Some(Value::True) => true,
            _ => false,
        });
        if res {
            Some(Value::True)
        } else {
            Some(Value::False)
        }
    }

    fn name() -> &'static str {
        "and"
    }
}

pub type And = CachedCur<AndEv>;

pub struct OrEv;

impl CachedCurEval for OrEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        let res = from.0.borrow().iter().any(|v| match v {
            Some(Value::True) => true,
            _ => false,
        });
        if res {
            Some(Value::True)
        } else {
            Some(Value::False)
        }
    }

    fn name() -> &'static str {
        "or"
    }
}

pub type Or = CachedCur<OrEv>;

pub struct NotEv;

impl CachedCurEval for NotEv {
    fn eval(from: &CachedVals) -> Option<Value> {
        match &**from.0.borrow() {
            [v] => v.as_ref().map(|v| !(v.clone())),
            _ => Some(Value::Error(Chars::from("not expected 1 argument"))),
        }
    }

    fn name() -> &'static str {
        "not"
    }
}

pub type Not = CachedCur<NotEv>;

pub struct CmpEv;

fn eval_op<T: PartialEq + PartialOrd>(op: &str, v0: T, v1: T) -> Value {
    match op {
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
    }
}

impl CachedCurEval for CmpEv {
    fn name() -> &'static str {
        "cmp"
    }

    fn eval(from: &CachedVals) -> Option<Value> {
        match &**from.0.borrow() {
            [op, v0, v1] => match op {
                None => None,
                Some(Value::String(op)) => match (v0, v1) {
                    (None, None) => Some(Value::False),
                    (_, None) => Some(Value::False),
                    (None, _) => Some(Value::False),
                    (Some(v0), Some(v1)) => match (v0, v1) {
                        (Value::U32(v0), Value::U32(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::U32(v0), Value::V32(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::V32(v0), Value::V32(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::V32(v0), Value::U32(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::I32(v0), Value::I32(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::I32(v0), Value::Z32(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::Z32(v0), Value::Z32(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::Z32(v0), Value::I32(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::U64(v0), Value::U64(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::U64(v0), Value::V64(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::V64(v0), Value::V64(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::V64(v0), Value::U64(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::I64(v0), Value::I64(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::I64(v0), Value::Z64(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::Z64(v0), Value::Z64(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::Z64(v0), Value::I64(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::F32(v0), Value::F32(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::F64(v0), Value::F64(v1)) => Some(eval_op(&*op, v0, v1)),
                        (Value::String(v0), Value::String(v1)) => {
                            Some(eval_op(&*op, v0, v1))
                        }
                        (Value::Bytes(v0), Value::Bytes(v1)) => {
                            Some(eval_op(&*op, v0, v1))
                        }
                        (Value::True, Value::True) => Some(eval_op(&*op, true, true)),
                        (Value::True, Value::False) => Some(eval_op(&*op, true, false)),
                        (Value::False, Value::True) => Some(eval_op(&*op, false, true)),
                        (Value::False, Value::False) => Some(eval_op(&*op, false, false)),
                        (Value::Ok, Value::Ok) => Some(eval_op(&*op, true, true)),
                        (Value::Error(v0), Value::Error(v1)) => {
                            Some(eval_op(&*op, v0, v1))
                        }
                        (Value::Null, Value::Null) => Some(Value::True),
                        (_, _) => Some(Value::False),
                    },
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
        match &**from.0.borrow() {
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
        match &**from.0.borrow() {
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

pub struct CastEv;

fn with_typ_prefix(
    from: &CachedVals,
    name: &'static str,
    f: impl Fn(Typ, &Option<Value>) -> Option<Value>,
) -> Option<Value> {
    match &**from.0.borrow() {
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
        with_typ_prefix(from, "cast(typ, src)", |typ, v| match v {
            None => None,
            Some(v) => v.clone().cast(typ),
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
            (Typ::V32, Some(Value::V32(_))) => Some(Value::True),
            (Typ::I32, Some(Value::I32(_))) => Some(Value::True),
            (Typ::Z32, Some(Value::Z32(_))) => Some(Value::True),
            (Typ::U64, Some(Value::U64(_))) => Some(Value::True),
            (Typ::V64, Some(Value::V64(_))) => Some(Value::True),
            (Typ::I64, Some(Value::I64(_))) => Some(Value::True),
            (Typ::Z64, Some(Value::Z64(_))) => Some(Value::True),
            (Typ::F32, Some(Value::F32(_))) => Some(Value::True),
            (Typ::F64, Some(Value::F64(_))) => Some(Value::True),
            (Typ::Bool, Some(Value::True)) => Some(Value::True),
            (Typ::Bool, Some(Value::False)) => Some(Value::True),
            (Typ::String, Some(Value::String(_))) => Some(Value::True),
            (Typ::Bytes, Some(Value::Bytes(_))) => Some(Value::True),
            (Typ::Result, Some(Value::Ok)) => Some(Value::True),
            (Typ::Result, Some(Value::Error(_))) => Some(Value::True),
            (_, Some(_)) => Some(Value::False),
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
        let vals = from.0.borrow();
        let mut parts = vals
            .iter()
            .filter_map(|v| v.as_ref().cloned().and_then(|v| v.cast_to::<Chars>().ok()));
        match parts.next() {
            None => None,
            Some(sep) => {
                let mut res = BytesMut::new();
                for p in parts {
                    if res.is_empty() {
                        res.extend_from_slice(p.bytes());
                    } else {
                        res.extend_from_slice(sep.bytes());
                        res.extend_from_slice(p.bytes());
                    }
                }
                Some(Value::String(unsafe { Chars::from_bytes_unchecked(res.freeze()) }))
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
        let vals = from.0.borrow();
        let parts = vals
            .iter()
            .filter_map(|v| v.as_ref().cloned().and_then(|v| v.cast_to::<Chars>().ok()));
        let mut res = BytesMut::new();
        for p in parts {
            res.extend_from_slice(p.bytes());
        }
        Some(Value::String(unsafe { Chars::from_bytes_unchecked(res.freeze()) }))
    }
}

pub type StringConcat = CachedCur<StringConcatEv>;

pub struct Eval<C: Ctx, E> {
    cached: CachedVals,
    current: RefCell<Result<Node<C, E>, Value>>,
}

impl<C: Ctx, E> Eval<C, E> {
    fn compile(&self, ctx: &ExecCtx<C, E>) {
        *self.current.borrow_mut() = match &**self.cached.0.borrow() {
            [None] => Err(Value::Null),
            [Some(v)] => match v {
                Value::String(s) => match s.parse::<Expr>() {
                    Ok(spec) => Ok(Node::compile(ctx, spec)),
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

impl<C: Ctx, E> Register<C, E> for Eval<C, E> {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|ctx, from| {
            let t = Eval {
                cached: CachedVals::new(from),
                current: RefCell::new(Err(Value::Null)),
            };
            t.compile(ctx);
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("eval".into(), f);
    }
}

impl<C: Ctx, E> Apply<C, E> for Eval<C, E> {
    fn current(&self) -> Option<Value> {
        match &*self.current.borrow() {
            Ok(s) => s.current(),
            Err(v) => Some(v.clone()),
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if self.cached.update(ctx, from, event) {
            self.compile(ctx);
        }
        match &*self.current.borrow() {
            Ok(s) => s.update(ctx, event),
            Err(v) => Some(v.clone()),
        }
    }
}

pub struct Count {
    from: CachedVals,
    count: Cell<u64>,
}

impl<C: Ctx, E> Register<C, E> for Count {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|_, from| {
            Box::new(Count { from: CachedVals::new(from), count: Cell::new(0) })
        });
        ctx.functions.borrow_mut().insert("count".into(), f);
    }
}

impl<C: Ctx, E> Apply<C, E> for Count {
    fn current(&self) -> Option<Value> {
        match &**self.from.0.borrow() {
            [] => Some(Value::Error(Chars::from("count(s): requires 1 argument"))),
            [_] => Some(Value::U64(self.count.get())),
            _ => Some(Value::Error(Chars::from("count(s): requires 1 argument"))),
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if self.from.update(ctx, from, event) {
            self.count.set(self.count.get() + 1);
            Apply::<C, E>::current(self)
        } else {
            None
        }
    }
}

pub struct Sample {
    current: RefCell<Option<Value>>,
}

impl<C: Ctx, E> Register<C, E> for Sample {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|_, from| {
            let v = match from {
                [trigger, source] => match trigger.current() {
                    None => None,
                    Some(_) => source.current(),
                },
                _ => Some(Value::Error(Chars::from(
                    "sample(trigger, source): expected 2 arguments",
                ))),
            };
            Box::new(Sample { current: RefCell::new(v) })
        });
        ctx.functions.borrow_mut().insert("sample".into(), f);
    }
}

impl<C: Ctx, E> Apply<C, E> for Sample {
    fn current(&self) -> Option<Value> {
        self.current.borrow().clone()
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [trigger, source] => {
                source.update(ctx, event);
                if trigger.update(ctx, event).is_none() {
                    None
                } else {
                    let v = source.current();
                    *self.current.borrow_mut() = v.clone();
                    v
                }
            }
            _ => {
                let v = Some(Value::Error(Chars::from(
                    "sample(trigger, source): expected 2 arguments",
                )));
                *self.current.borrow_mut() = v.clone();
                v
            }
        }
    }
}

pub struct Mean {
    from: CachedVals,
    total: Cell<f64>,
    samples: Cell<usize>,
}

impl<C: Ctx, E> Register<C, E> for Mean {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|_, from| {
            Box::new(Mean {
                from: CachedVals::new(from),
                total: Cell::new(0.),
                samples: Cell::new(0),
            })
        });
        ctx.functions.borrow_mut().insert("mean".into(), f);
    }
}

impl<C: Ctx, E> Apply<C, E> for Mean {
    fn current(&self) -> Option<Value> {
        match &**self.from.0.borrow() {
            [] => Some(Value::Error(Chars::from("mean(s): requires 1 argument"))),
            [_] => {
                if self.samples.get() > 0 {
                    Some(Value::F64(self.total.get() / (self.samples.get() as f64)))
                } else {
                    None
                }
            }
            _ => Some(Value::Error(Chars::from("mean(s): requires 1 argument"))),
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if self.from.update(ctx, from, event) {
            for v in &*self.from.0.borrow() {
                if let Some(v) = v {
                    if let Ok(v) = v.clone().cast_to::<f64>() {
                        self.total.set(self.total.get() + v);
                        self.samples.set(self.samples.get() + 1);
                    }
                }
            }
            Apply::<C, E>::current(self)
        } else {
            None
        }
    }
}

pub(crate) struct Uniq(RefCell<Option<Value>>);

impl<C: Ctx, E> Register<C, E> for Uniq {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|_, from| {
            let t = Uniq(RefCell::new(None));
            match from {
                [e] => *t.0.borrow_mut() = e.current(),
                _ => *t.0.borrow_mut() = Uniq::usage(),
            }
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("uniq".into(), f);
    }
}

impl<C: Ctx, E> Apply<C, E> for Uniq {
    fn current(&self) -> Option<Value> {
        self.0.borrow().as_ref().cloned()
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [e] => e.update(ctx, event).and_then(|v| {
                let cur = &mut *self.0.borrow_mut();
                if Some(&v) != cur.as_ref() {
                    *cur = Some(v.clone());
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
                *self.0.borrow_mut() = Uniq::usage();
                if up {
                    Apply::<C, E>::current(self)
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

fn pathname(invalid: &Cell<bool>, path: Option<Value>) -> Option<Path> {
    invalid.set(false);
    match path.map(|v| v.cast_to::<String>()) {
        None => None,
        Some(Ok(p)) => {
            if Path::is_absolute(&p) {
                Some(Path::from(p))
            } else {
                invalid.set(true);
                None
            }
        }
        Some(Err(_)) => {
            invalid.set(true);
            None
        }
    }
}

pub struct Store {
    queued: RefCell<Vec<Value>>,
    dv: RefCell<Option<(Path, Dval)>>,
    invalid: Cell<bool>,
}

impl<C: Ctx, E> Register<C, E> for Store {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|ctx, from| {
            let t = Store {
                queued: RefCell::new(Vec::new()),
                dv: RefCell::new(None),
                invalid: Cell::new(false),
            };
            match from {
                [to, val] => t.set(ctx, to.current(), val.current()),
                _ => t.invalid.set(true),
            }
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("store".into(), f);
    }
}

impl<C: Ctx, E> Apply<C, E> for Store {
    fn current(&self) -> Option<Value> {
        if self.invalid.get() {
            Some(Value::Error(Chars::from(
                "store(tgt: absolute path, val): expected 2 arguments",
            )))
        } else {
            None
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [path, val] => {
                let path = path.update(ctx, event);
                let value = val.update(ctx, event);
                let value = if path.is_some() && !self.same_path(&path) {
                    value.or_else(|| val.current())
                } else {
                    value
                };
                let up = value.is_some();
                self.set(ctx, path, value);
                if up {
                    Apply::<C, E>::current(self)
                } else {
                    None
                }
            }
            exprs => {
                let mut up = false;
                for expr in exprs {
                    up = expr.update(ctx, event).is_some() || up;
                }
                self.invalid.set(true);
                if up {
                    Apply::<C, E>::current(self)
                } else {
                    None
                }
            }
        }
    }
}

impl Store {
    fn queue(&self, v: Value) {
        self.queued.borrow_mut().push(v)
    }

    fn write(&self, dv: &Dval, v: Value) {
        dv.write(v);
    }

    fn set<C: Ctx, E>(&self, ctx: &ExecCtx<C, E>, to: Option<Value>, val: Option<Value>) {
        match (pathname(&self.invalid, to), val) {
            (None, None) => (),
            (None, Some(v)) => match self.dv.borrow().as_ref() {
                None => self.queue(v),
                Some((_, dv)) => self.write(dv, v),
            },
            (Some(p), val) => {
                let path = Path::from(p);
                let dv = ctx.user.durable_subscribe(path.clone());
                if let Some(v) = val {
                    self.queue(v)
                }
                for v in self.queued.borrow_mut().drain(..) {
                    self.write(&dv, v);
                }
                *self.dv.borrow_mut() = Some((path, dv));
            }
        }
    }

    fn same_path(&self, new_path: &Option<Value>) -> bool {
        match (new_path.as_ref(), self.dv.borrow().as_ref()) {
            (Some(Value::String(p0)), Some((p1, _))) => &**p0 == &**p1,
            _ => false,
        }
    }
}

fn varname(invalid: &Cell<bool>, name: Option<Value>) -> Option<Chars> {
    invalid.set(false);
    match name.map(|n| n.cast_to::<Chars>()) {
        None => None,
        Some(Err(_)) => {
            invalid.set(true);
            None
        }
        Some(Ok(n)) => {
            if VNAME.is_match(&n) {
                Some(n)
            } else {
                invalid.set(true);
                None
            }
        }
    }
}

pub struct StoreVar {
    queued: RefCell<Vec<Value>>,
    name: RefCell<Option<Chars>>,
    invalid: Cell<bool>,
}

impl<C: Ctx, E> Register<C, E> for StoreVar {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|ctx, from| {
            let t = StoreVar {
                queued: RefCell::new(Vec::new()),
                name: RefCell::new(None),
                invalid: Cell::new(false),
            };
            match from {
                [name, value] => t.set(ctx, name.current(), value.current()),
                _ => t.invalid.set(true),
            }
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("store_var".into(), f);
    }
}

impl<C: Ctx, E> Apply<C, E> for StoreVar {
    fn current(&self) -> Option<Value> {
        if self.invalid.get() {
            Some(Value::Error(Chars::from(
                "store_var(name: string [a-z][a-z0-9_]+, value): expected 2 arguments",
            )))
        } else {
            None
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [name, val] => {
                let name = name.update(ctx, event);
                let value = val.update(ctx, event);
                let value = if name.is_some() && !self.same_name(&name) {
                    value.or_else(|| val.current())
                } else {
                    value
                };
                let up = value.is_some();
                self.set(ctx, name, value);
                if up {
                    Apply::<C, E>::current(self)
                } else {
                    None
                }
            }
            exprs => {
                let mut up = false;
                for expr in exprs {
                    up = expr.update(ctx, event).is_some() || up;
                }
                self.invalid.set(true);
                if up {
                    Apply::<C, E>::current(self)
                } else {
                    None
                }
            }
        }
    }
}

impl StoreVar {
    fn queue_set(&self, v: Value) {
        self.queued.borrow_mut().push(v)
    }

    fn set<C: Ctx, E>(
        &self,
        ctx: &ExecCtx<C, E>,
        name: Option<Value>,
        value: Option<Value>,
    ) {
        if let Some(name) = varname(&self.invalid, name) {
            for v in self.queued.borrow_mut().drain(..) {
                ctx.user.set_var(name.clone(), v)
            }
            *self.name.borrow_mut() = Some(name);
        }
        if let Some(value) = value {
            match self.name.borrow().as_ref() {
                None => self.queue_set(value),
                Some(name) => ctx.user.set_var(name.clone(), value),
            }
        }
    }

    fn same_name(&self, new_name: &Option<Value>) -> bool {
        match (new_name, self.name.borrow().as_ref()) {
            (Some(Value::String(n0)), Some(n1)) => n0 == n1,
            _ => false,
        }
    }
}

pub(crate) struct Load {
    cur: RefCell<Option<Dval>>,
    invalid: Cell<bool>,
}

impl<C: Ctx, E> Register<C, E> for Load {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|ctx, from| {
            let t = Load { cur: RefCell::new(None), invalid: Cell::new(false) };
            match from {
                [path] => t.subscribe(ctx, path.current()),
                _ => t.invalid.set(true),
            }
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("load".into(), f);
    }
}

impl<C: Ctx, E> Apply<C, E> for Load {
    fn current(&self) -> Option<Value> {
        if self.invalid.get() {
            Load::err()
        } else {
            self.cur.borrow().as_ref().and_then(|dv| match dv.last() {
                subscriber::Event::Unsubscribed => None,
                subscriber::Event::Update(v) => Some(v),
            })
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [name] => {
                let target = name.update(ctx, event);
                let up = target.is_some();
                self.subscribe(ctx, target);
                if self.invalid.get() {
                    if up {
                        Load::err()
                    } else {
                        None
                    }
                } else {
                    self.cur.borrow().as_ref().and_then(|dv| match event {
                        Event::Variable(_, _) | Event::Rpc(_, _) | Event::User(_) => {
                            None
                        }
                        Event::Netidx(id, value) if dv.id() == *id => {
                            Some(value.clone())
                        }
                        Event::Netidx(_, _) => None,
                    })
                }
            }
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(ctx, event).is_some() || up;
                }
                self.invalid.set(true);
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
    fn subscribe<C: Ctx, E>(&self, ctx: &ExecCtx<C, E>, name: Option<Value>) {
        if let Some(path) = pathname(&self.invalid, name) {
            *self.cur.borrow_mut() = Some(ctx.user.durable_subscribe(path));
        }
    }

    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from(
            "load(expr: path) expected 1 absolute path as argument",
        )))
    }
}

pub struct LoadVar<C, E> {
    name: RefCell<Option<Chars>>,
    ctx: ExecCtx<C, E>,
    invalid: Cell<bool>,
}

impl<C: Ctx, E> Register<C, E> for LoadVar<C, E> {
    fn register(ctx: &ExecCtx<C, E>) {
        let f: InitFn<C, E> = Box::new(|ctx, from| {
            let t = LoadVar {
                name: RefCell::new(None),
                ctx: ctx.clone(),
                invalid: Cell::new(false),
            };
            match from {
                [name] => t.subscribe(name.current()),
                _ => t.invalid.set(true),
            }
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("load_var".into(), f);
    }
}

impl<C: Ctx, E> Apply<C, E> for LoadVar<C, E> {
    fn current(&self) -> Option<Value> {
        if self.invalid.get() {
            LoadVar::err()
        } else {
            self.name
                .borrow()
                .as_ref()
                .and_then(|n| self.ctx.variables.borrow().get(n).cloned())
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        match from {
            [name] => {
                let target = name.update(ctx, event);
                if self.invalid.get() {
                    if target.is_some() {
                        LoadVar::err()
                    } else {
                        None
                    }
                } else {
                    if let Some(target) = target {
                        self.subscribe(Some(target));
                    }
                    match (self.name.borrow().as_ref(), event) {
                        (None, _)
                        | (Some(_), Event::Netidx(_, _))
                        | (Some(_), Event::User(_))
                        | (Some(_), Event::Rpc(_, _)) => None,
                        (Some(vn), Event::Variable(tn, v)) if vn == tn => {
                            Some(v.clone())
                        }
                        (Some(_), Event::Variable(_, _)) => None,
                    }
                }
            }
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(ctx, event).is_some() || up;
                }
                self.invalid.set(true);
                if up {
                    LoadVar::err()
                } else {
                    None
                }
            }
        }
    }
}

impl<C: Ctx, E> LoadVar<C, E> {
    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from(
            "load_var(expr: variable name): expected 1 variable name as argument",
        )))
    }

    fn subscribe(&self, name: Option<Value>) {
        if let Some(name) = varname(&self.invalid, name) {
            *self.name.borrow_mut() = Some(name);
        }
    }
}
