use crate::{
    expr::Expr,
    vm::{Apply, ExecCtx, InitFn, Node, Register, Target},
};
use netidx::{
    chars::Chars,
    subscriber::{Typ, Value},
};
use std::{
    cell::{Cell, RefCell},
    marker::PhantomData,
    rc::Rc,
};

#[derive(Debug, Clone)]
pub struct CachedVals(pub Rc<RefCell<Vec<Option<Value>>>>);

impl CachedVals {
    pub fn new<C: 'static>(from: &[Node<C>]) -> CachedVals {
        CachedVals(Rc::new(RefCell::new(from.into_iter().map(|s| s.current()).collect())))
    }

    pub fn update<C: 'static>(
        &self,
        from: &[Node<C>],
        tgt: Target,
        value: &Value,
    ) -> bool {
        let mut vals = self.0.borrow_mut();
        from.into_iter().enumerate().fold(false, |res, (i, src)| {
            match src.update(tgt, value) {
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

impl<C: 'static> Register<C> for Any {
    fn register(ctx: &ExecCtx<C>) {
        let f: InitFn<C> = Box::new(|_ctx, from| {
            Box::new(Any(Rc::new(RefCell::new(from.iter().find_map(|s| s.current())))))
        });
        ctx.functions.borrow_mut().insert("any".into(), f);
    }
}

impl<C: 'static> Apply<C> for Any {
    fn current(&self) -> Option<Value> {
        self.0.borrow().clone()
    }

    fn update(&self, from: &[Node<C>], tgt: Target, value: &Value) -> Option<Value> {
        let res =
            from.into_iter().filter_map(|s| s.update(tgt, value)).fold(None, |res, v| {
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

pub struct CachedCur<T: CachedCurEval + 'static> {
    cached: CachedVals,
    current: RefCell<Option<Value>>,
    t: PhantomData<T>,
}

impl<C: 'static, T: CachedCurEval + 'static> Register<C> for CachedCur<T> {
    fn register(ctx: &ExecCtx<C>) {
        let f: InitFn<C> = Box::new(|_ctx, from| {
            let cached = CachedVals::new(from);
            let current = RefCell::new(T::eval(&cached));
            Box::new(CachedCur::<T> { cached, current, t: PhantomData })
        });
        ctx.functions.borrow_mut().insert(T::name().into(), f);
    }
}

impl<C: 'static, T: CachedCurEval + 'static> Apply<C> for CachedCur<T> {
    fn current(&self) -> Option<Value> {
        self.current.borrow().clone()
    }

    fn update(&self, from: &[Node<C>], tgt: Target, value: &Value) -> Option<Value> {
        if !self.cached.update(from, tgt, value) {
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

pub struct Eval<C: Clone + 'static> {
    ctx: ExecCtx<C>,
    cached: CachedVals,
    current: RefCell<Result<Node<C>, Value>>,
}

impl<C: Clone + 'static> Eval<C> {
    fn compile(&self) {
        *self.current.borrow_mut() = match &**self.cached.0.borrow() {
            [None] => Err(Value::Null),
            [Some(v)] => match v {
                Value::String(s) => match s.parse::<Expr>() {
                    Ok(spec) => Ok(Node::compile(&self.ctx, spec)),
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

impl<C: Clone + 'static> Register<C> for Eval<C> {
    fn register(ctx: &ExecCtx<C>) {
        let f: InitFn<C> = Box::new(|ctx, from| {
            let t = Eval {
                ctx: ctx.clone(),
                cached: CachedVals::new(from),
                current: RefCell::new(Err(Value::Null)),
            };
            t.compile();
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("eval".into(), f);
    }
}

impl<C: Clone + 'static> Apply<C> for Eval<C> {
    fn current(&self) -> Option<Value> {
        match &*self.current.borrow() {
            Ok(s) => s.current(),
            Err(v) => Some(v.clone()),
        }
    }

    fn update(&self, from: &[Node<C>], tgt: Target, value: &Value) -> Option<Value> {
        if self.cached.update(from, tgt, value) {
            self.compile();
        }
        match &*self.current.borrow() {
            Ok(s) => s.update(tgt, value),
            Err(v) => Some(v.clone()),
        }
    }
}

pub struct Count {
    from: CachedVals,
    count: Cell<u64>,
}

impl<C: 'static> Register<C> for Count {
    fn register(ctx: &ExecCtx<C>) {
        let f: InitFn<C> = Box::new(|_, from| {
            Box::new(Count { from: CachedVals::new(from), count: Cell::new(0) })
        });
        ctx.functions.borrow_mut().insert("count".into(), f);
    }
}

impl<C: 'static> Apply<C> for Count {
    fn current(&self) -> Option<Value> {
        match &**self.from.0.borrow() {
            [] => Some(Value::Error(Chars::from("count(s): requires 1 argument"))),
            [_] => Some(Value::U64(self.count.get())),
            _ => Some(Value::Error(Chars::from("count(s): requires 1 argument"))),
        }
    }

    fn update(&self, from: &[Node<C>], tgt: Target, value: &Value) -> Option<Value> {
        if self.from.update(from, tgt, value) {
            self.count.set(self.count.get() + 1);
            Apply::<C>::current(self)
        } else {
            None
        }
    }
}

pub struct Sample {
    current: RefCell<Option<Value>>,
}

impl<C: 'static> Register<C> for Sample {
    fn register(ctx: &ExecCtx<C>) {
        let f: InitFn<C> = Box::new(|_, from| {
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

impl<C: 'static> Apply<C> for Sample {
    fn current(&self) -> Option<Value> {
        self.current.borrow().clone()
    }

    fn update(&self, from: &[Node<C>], tgt: Target, value: &Value) -> Option<Value> {
        match from {
            [trigger, source] => {
                source.update(tgt, value);
                if trigger.update(tgt, value).is_none() {
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
