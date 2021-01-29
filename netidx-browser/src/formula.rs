use super::{Expr, Target, Vars, WidgetCtx};
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{Dval, Typ, UpdatesFlags, Value},
};
use netidx_protocols::view;
use std::{
    cell::{Cell, RefCell},
    cmp::{PartialEq, PartialOrd},
    collections::HashMap,
    ops::Deref,
    rc::Rc,
    result::Result,
};

#[derive(Debug, Clone)]
pub struct CachedVals(Rc<RefCell<Vec<Option<Value>>>>);

impl CachedVals {
    fn new(from: &[Expr]) -> CachedVals {
        CachedVals(Rc::new(RefCell::new(from.into_iter().map(|s| s.current()).collect())))
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> bool {
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

#[derive(Debug, Clone)]
pub struct All {
    cached: CachedVals,
    current: Rc<RefCell<Option<Value>>>,
}

impl All {
    fn new(from: &[Expr]) -> Self {
        let cached = CachedVals::new(from);
        let current = Rc::new(RefCell::new(All::eval_sources(&cached)));
        All { cached, current }
    }

    fn eval_sources(from: &CachedVals) -> Option<Value> {
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

    fn eval(&self) -> Option<Value> {
        self.current.borrow().clone()
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        if !self.cached.update(from, tgt, value) {
            None
        } else {
            let cur = All::eval_sources(&self.cached);
            if cur == *self.current.borrow() {
                None
            } else {
                *self.current.borrow_mut() = cur.clone();
                cur
            }
        }
    }
}

fn add_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (Some(l), Some(r)) => Some(l + r),
    }
}

fn eval_sum(from: &CachedVals) -> Option<Value> {
    from.0.borrow().iter().fold(None, |res, v| match res {
        res @ Some(Value::Error(_)) => res,
        res => add_vals(res, v.clone()),
    })
}

fn prod_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (Some(l), Some(r)) => Some(l * r),
    }
}

fn eval_product(from: &CachedVals) -> Option<Value> {
    from.0.borrow().iter().fold(None, |res, v| match res {
        res @ Some(Value::Error(_)) => res,
        res => prod_vals(res, v.clone()),
    })
}

fn div_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (Some(l), Some(r)) => Some(l / r),
    }
}

fn eval_divide(from: &CachedVals) -> Option<Value> {
    from.0.borrow().iter().fold(None, |res, v| match res {
        res @ Some(Value::Error(_)) => res,
        res => div_vals(res, v.clone()),
    })
}

#[derive(Debug, Clone)]
pub(super) struct Mean {
    from: CachedVals,
    total: Rc<Cell<f64>>,
    samples: Rc<Cell<usize>>,
}

impl Mean {
    fn new(from: &[Expr]) -> Self {
        Mean {
            from: CachedVals::new(from),
            total: Rc::new(Cell::new(0.)),
            samples: Rc::new(Cell::new(0)),
        }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        if self.from.update(from, tgt, value) {
            for v in &*self.from.0.borrow() {
                if let Some(v) = v {
                    if let Ok(v) = v.clone().cast_to::<f64>() {
                        self.total.set(self.total.get() + v);
                        self.samples.set(self.samples.get() + 1);
                    }
                }
            }
            self.eval()
        } else {
            None
        }
    }

    fn eval(&self) -> Option<Value> {
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
}

#[derive(Debug, Clone)]
pub(super) struct Count {
    from: CachedVals,
    count: Rc<Cell<u64>>,
}

impl Count {
    fn new(from: &[Expr]) -> Self {
        Count { from: CachedVals::new(from), count: Rc::new(Cell::new(0)) }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        if self.from.update(from, tgt, value) {
            for v in &*self.from.0.borrow() {
                if v.is_some() {
                    self.count.set(self.count.get() + 1);
                }
            }
            self.eval()
        } else {
            None
        }
    }

    fn eval(&self) -> Option<Value> {
        match &**self.from.0.borrow() {
            [] => Some(Value::Error(Chars::from("count(s): requires 1 argument"))),
            [_] => Some(Value::U64(self.count.get())),
            _ => Some(Value::Error(Chars::from("count(s): requires 1 argument"))),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Sample {
    current: Rc<RefCell<Option<Value>>>,
}

impl Sample {
    fn new(from: &[Expr]) -> Self {
        let v = match from {
            [trigger, source] => match trigger.current() {
                None => None,
                Some(_) => source.current(),
            },
            _ => Some(Value::Error(Chars::from(
                "sample(trigger, source): expected 2 arguments",
            ))),
        };
        Sample { current: Rc::new(RefCell::new(v)) }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
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
            _ => None,
        }
    }

    fn eval(&self) -> Option<Value> {
        self.current.borrow().clone()
    }
}

fn eval_min(from: &CachedVals) -> Option<Value> {
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

fn eval_max(from: &CachedVals) -> Option<Value> {
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

fn eval_and(from: &CachedVals) -> Option<Value> {
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

fn eval_or(from: &CachedVals) -> Option<Value> {
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

fn eval_not(from: &CachedVals) -> Option<Value> {
    match &**from.0.borrow() {
        [v] => v.as_ref().map(|v| !(v.clone())),
        _ => Some(Value::Error(Chars::from("not expected 1 argument"))),
    }
}

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

fn eval_cmp(from: &CachedVals) -> Option<Value> {
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
                    (Value::String(v0), Value::String(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::Bytes(v0), Value::Bytes(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::True, Value::True) => Some(eval_op(&*op, true, true)),
                    (Value::True, Value::False) => Some(eval_op(&*op, true, false)),
                    (Value::False, Value::True) => Some(eval_op(&*op, false, true)),
                    (Value::False, Value::False) => Some(eval_op(&*op, false, false)),
                    (Value::Ok, Value::Ok) => Some(eval_op(&*op, true, true)),
                    (Value::Error(v0), Value::Error(v1)) => Some(eval_op(&*op, v0, v1)),
                    (Value::Null, Value::Null) => Some(Value::True),
                    (v0, v1) => Some(Value::Error(Chars::from(format!(
                        "can't compare incompatible types {:?} and {:?}",
                        v0, v1
                    )))),
                },
            },
            Some(_) => Some(Value::Error(Chars::from(
                "cmp(op, v0, v1): expected op to be a string",
            ))),
        },
        _ => Some(Value::Error(Chars::from("cmp(op, v0, v1): expected 3 arguments"))),
    }
}

fn eval_if(from: &CachedVals) -> Option<Value> {
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

fn eval_filter(from: &CachedVals) -> Option<Value> {
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

fn eval_cast(from: &CachedVals) -> Option<Value> {
    with_typ_prefix(from, "cast(typ, src)", |typ, v| match v {
        None => None,
        Some(v) => v.clone().cast(typ),
    })
}

fn eval_isa(from: &CachedVals) -> Option<Value> {
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

fn eval_string_join(from: &CachedVals) -> Option<Value> {
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

#[derive(Debug, Clone)]
pub(super) struct Eval {
    ctx: WidgetCtx,
    cached: CachedVals,
    current: RefCell<Result<Expr, Value>>,
    variables: Vars,
}

impl Eval {
    fn new(ctx: &WidgetCtx, variables: &Vars, from: &[Expr]) -> Self {
        let t = Eval {
            ctx: ctx.clone(),
            cached: CachedVals::new(from),
            current: RefCell::new(Err(Value::Null)),
            variables: variables.clone(),
        };
        t.compile();
        t
    }

    fn eval(&self) -> Option<Value> {
        match &*self.current.borrow() {
            Ok(s) => s.current(),
            Err(v) => Some(v.clone()),
        }
    }

    fn compile(&self) {
        *self.current.borrow_mut() = match &**self.cached.0.borrow() {
            [None] => Err(Value::Null),
            [Some(v)] => match v {
                Value::String(s) => match s.parse::<view::Expr>() {
                    Ok(spec) => Ok(Expr::new(&self.ctx, self.variables.clone(), spec)),
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

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        if self.cached.update(from, tgt, value) {
            self.compile();
        }
        match &*self.current.borrow() {
            Ok(s) => s.update(tgt, value),
            Err(v) => Some(v.clone()),
        }
    }
}

fn update_cached(
    eval: impl Fn(&CachedVals) -> Option<Value>,
    cached: &CachedVals,
    from: &[Expr],
    tgt: Target,
    value: &Value,
) -> Option<Value> {
    if cached.update(from, tgt, value) {
        eval(cached)
    } else {
        None
    }
}

#[derive(Debug)]
struct StoreInner {
    queued: RefCell<Vec<Value>>,
    ctx: WidgetCtx,
    dv: RefCell<Option<Dval>>,
    invalid: Cell<bool>,
}

#[derive(Debug, Clone)]
struct Store(Rc<StoreInner>);

impl Deref for Store {
    type Target = StoreInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Store {
    fn new(ctx: &WidgetCtx, from: &[Expr]) -> Self {
        let t = Store(Rc::new(StoreInner {
            queued: RefCell::new(Vec::new()),
            ctx: ctx.clone(),
            dv: RefCell::new(None),
            invalid: Cell::new(false),
        }));
        match from {
            [to, val] => t.set(to.current(), val.current()),
            _ => t.invalid.set(true),
        }
        t
    }

    fn set(&self, to: Option<Value>, val: Option<Value>) {
        self.invalid.set(false);
        let path = match to.map(|v| v.cast_to::<String>()) {
            Some(Ok(p)) => Some(p),
            Some(Err(_)) => {
                self.invalid.set(true);
                None
            }
            None => None,
        };
        match (path, val) {
            (None, None) => (),
            (None, Some(v)) => match self.dv.borrow().as_ref() {
                None => {
                    self.queued.borrow_mut().push(v);
                }
                Some(dv) => {
                    dv.write(v);
                }
            },
            (Some(p), val) => {
                let dv = self.ctx.subscriber.durable_subscribe(Path::from(p));
                dv.updates(UpdatesFlags::BEGIN_WITH_LAST, self.ctx.updates.clone());
                if let Some(v) = val {
                    self.queued.borrow_mut().push(v);
                }
                for v in self.queued.borrow_mut().drain(..) {
                    dv.write(v);
                }
                *self.dv.borrow_mut() = Some(dv);
            }
        }
    }

    fn eval(&self) -> Option<Value> {
        if self.invalid.get() {
            Some(Value::Error(Chars::from(
                "store(tgt: string, val): expected 2 arguments",
            )))
        } else {
            None
        }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        match from {
            [path, val] => {
                let path = path.update(tgt, value);
                let val = val.update(tgt, value);
                self.set(path, val);
                self.eval()
            }
            _ => {
                self.invalid.set(true);
                self.eval()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(super) enum Formula {
    Any(Rc<RefCell<Option<Value>>>),
    All(All),
    Sum(CachedVals),
    Product(CachedVals),
    Divide(CachedVals),
    Mean(Mean),
    Min(CachedVals),
    Max(CachedVals),
    And(CachedVals),
    Or(CachedVals),
    Not(CachedVals),
    Cmp(CachedVals),
    If(CachedVals),
    Filter(CachedVals),
    Cast(CachedVals),
    IsA(CachedVals),
    Eval(Eval),
    Count(Count),
    Sample(Sample),
    StringJoin(CachedVals),
    Store(Store),
    Unknown(String),
}

pub(super) static FORMULAS: [&'static str; 21] = [
    "any",
    "all",
    "sum",
    "product",
    "divide",
    "mean",
    "min",
    "max",
    "and",
    "or",
    "not",
    "cmp",
    "if",
    "filter",
    "cast",
    "isa",
    "eval",
    "count",
    "sample",
    "string_join",
    "store",
];

impl Formula {
    pub(super) fn new(
        ctx: &WidgetCtx,
        variables: &Vars,
        name: &str,
        from: &[Expr],
    ) -> Formula {
        match name {
            "any" => {
                Formula::Any(Rc::new(RefCell::new(from.iter().find_map(|s| s.current()))))
            }
            "all" => Formula::All(All::new(from)),
            "sum" => Formula::Sum(CachedVals::new(from)),
            "product" => Formula::Product(CachedVals::new(from)),
            "divide" => Formula::Divide(CachedVals::new(from)),
            "mean" => Formula::Mean(Mean::new(from)),
            "min" => Formula::Min(CachedVals::new(from)),
            "max" => Formula::Max(CachedVals::new(from)),
            "and" => Formula::And(CachedVals::new(from)),
            "or" => Formula::Or(CachedVals::new(from)),
            "not" => Formula::Not(CachedVals::new(from)),
            "cmp" => Formula::Cmp(CachedVals::new(from)),
            "if" => Formula::If(CachedVals::new(from)),
            "filter" => Formula::Filter(CachedVals::new(from)),
            "cast" => Formula::Cast(CachedVals::new(from)),
            "isa" => Formula::IsA(CachedVals::new(from)),
            "eval" => Formula::Eval(Eval::new(ctx, variables, from)),
            "count" => Formula::Count(Count::new(from)),
            "sample" => Formula::Sample(Sample::new(from)),
            "string_join" => Formula::StringJoin(CachedVals::new(from)),
            "store" => Formula::Store(Store::new(ctx, from)),
            _ => Formula::Unknown(String::from(name)),
        }
    }

    pub(super) fn current(&self) -> Option<Value> {
        match self {
            Formula::Any(c) => c.borrow().clone(),
            Formula::All(c) => c.eval(),
            Formula::Sum(c) => eval_sum(c),
            Formula::Product(c) => eval_product(c),
            Formula::Divide(c) => eval_divide(c),
            Formula::Mean(m) => m.eval(),
            Formula::Min(c) => eval_min(c),
            Formula::Max(c) => eval_max(c),
            Formula::And(c) => eval_and(c),
            Formula::Or(c) => eval_or(c),
            Formula::Not(c) => eval_not(c),
            Formula::Cmp(c) => eval_cmp(c),
            Formula::If(c) => eval_if(c),
            Formula::Filter(c) => eval_filter(c),
            Formula::Cast(c) => eval_cast(c),
            Formula::IsA(c) => eval_isa(c),
            Formula::Eval(e) => e.eval(),
            Formula::Count(c) => c.eval(),
            Formula::Sample(c) => c.eval(),
            Formula::StringJoin(c) => eval_string_join(c),
            Formula::Store(s) => s.eval(),
            Formula::Unknown(s) => {
                Some(Value::Error(Chars::from(format!("unknown formula {}", s))))
            }
        }
    }

    pub(super) fn update(
        &self,
        from: &[Expr],
        tgt: Target,
        value: &Value,
    ) -> Option<Value> {
        match self {
            Formula::Any(c) => {
                let res = from.into_iter().filter_map(|s| s.update(tgt, value)).fold(
                    None,
                    |res, v| match res {
                        None => Some(v),
                        Some(_) => res,
                    },
                );
                *c.borrow_mut() = res.clone();
                res
            }
            Formula::All(c) => c.update(from, tgt, value),
            Formula::Sum(c) => update_cached(eval_sum, c, from, tgt, value),
            Formula::Product(c) => update_cached(eval_product, c, from, tgt, value),
            Formula::Divide(c) => update_cached(eval_divide, c, from, tgt, value),
            Formula::Mean(m) => m.update(from, tgt, value),
            Formula::Min(c) => update_cached(eval_min, c, from, tgt, value),
            Formula::Max(c) => update_cached(eval_max, c, from, tgt, value),
            Formula::And(c) => update_cached(eval_and, c, from, tgt, value),
            Formula::Or(c) => update_cached(eval_or, c, from, tgt, value),
            Formula::Not(c) => update_cached(eval_not, c, from, tgt, value),
            Formula::Cmp(c) => update_cached(eval_cmp, c, from, tgt, value),
            Formula::If(c) => update_cached(eval_if, c, from, tgt, value),
            Formula::Filter(c) => update_cached(eval_filter, c, from, tgt, value),
            Formula::Cast(c) => update_cached(eval_cast, c, from, tgt, value),
            Formula::IsA(c) => update_cached(eval_isa, c, from, tgt, value),
            Formula::Eval(e) => e.update(from, tgt, value),
            Formula::Count(c) => c.update(from, tgt, value),
            Formula::Sample(c) => c.update(from, tgt, value),
            Formula::StringJoin(c) => {
                update_cached(eval_string_join, c, from, tgt, value)
            }
            Formula::Store(s) => s.update(from, tgt, value),
            Formula::Unknown(s) => {
                Some(Value::Error(Chars::from(format!("unknown formula {}", s))))
            }
        }
    }
}
