use super::{util::ask_modal, ToGui, Vars, ViewLoc, WidgetCtx};
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{self, Dval, SubId, Typ, UpdatesFlags, Value},
};
use netidx_protocols::view;
use std::{
    cell::{Cell, RefCell},
    cmp::{PartialEq, PartialOrd},
    fmt, mem,
    ops::Deref,
    rc::Rc,
    result::Result,
};

#[derive(Debug, Clone, Copy)]
pub(crate) enum Target<'a> {
    Event,
    Variable(&'a str),
    Netidx(SubId),
    Rpc(&'a str),
}

#[derive(Debug, Clone)]
pub(crate) struct CachedVals(Rc<RefCell<Vec<Option<Value>>>>);

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
pub(crate) struct All {
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

fn eval_string_concat(from: &CachedVals) -> Option<Value> {
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

#[derive(Debug)]
pub(crate) struct EventInner {
    cur: RefCell<Option<Value>>,
    invalid: Cell<bool>,
}

#[derive(Debug, Clone)]
pub(crate) struct Event(Rc<EventInner>);

impl Deref for Event {
    type Target = EventInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Event {
    fn new(from: &[Expr]) -> Self {
        Event(Rc::new(EventInner {
            cur: RefCell::new(None),
            invalid: Cell::new(from.len() > 0),
        }))
    }

    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from("event(): expected 0 arguments")))
    }

    fn eval(&self) -> Option<Value> {
        if self.invalid.get() {
            Event::err()
        } else {
            self.cur.borrow().as_ref().cloned()
        }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        self.invalid.set(from.len() > 0);
        match tgt {
            Target::Variable(_) | Target::Netidx(_) | Target::Rpc(_) => None,
            Target::Event => {
                *self.cur.borrow_mut() = Some(value.clone());
                self.eval()
            }
        }
    }
}

#[derive(Clone)]
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

pub(crate) struct StoreInner {
    queued: RefCell<Vec<Value>>,
    ctx: WidgetCtx,
    dv: RefCell<Option<(Path, Dval)>>,
    invalid: Cell<bool>,
}

#[derive(Clone)]
pub(crate) struct Store(Rc<StoreInner>);

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

    fn queue(&self, v: Value) {
        self.queued.borrow_mut().push(v)
    }

    fn write(&self, dv: &Dval, v: Value) {
        dv.write(v);
    }

    fn set(&self, to: Option<Value>, val: Option<Value>) {
        match (pathname(&self.invalid, to), val) {
            (None, None) => (),
            (None, Some(v)) => match self.dv.borrow().as_ref() {
                None => self.queue(v),
                Some((_, dv)) => self.write(dv, v),
            },
            (Some(p), val) => {
                let path = Path::from(p);
                let dv = self.ctx.backend.subscriber.durable_subscribe(path.clone());
                dv.updates(
                    UpdatesFlags::BEGIN_WITH_LAST,
                    self.ctx.backend.updates.clone(),
                );
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

    fn eval(&self) -> Option<Value> {
        if self.invalid.get() {
            Some(Value::Error(Chars::from(
                "store(tgt: absolute path, val): expected 2 arguments",
            )))
        } else {
            None
        }
    }

    fn same_path(&self, new_path: &Option<Value>) -> bool {
        match (new_path.as_ref(), self.dv.borrow().as_ref()) {
            (Some(Value::String(p0)), Some((p1, _))) => &**p0 == &**p1,
            _ => false,
        }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        match from {
            [path, val] => {
                let path = path.update(tgt, value);
                let value = val.update(tgt, value);
                let value = if path.is_some() && !self.same_path(&path) {
                    value.or_else(|| val.current())
                } else {
                    value
                };
                let up = value.is_some();
                self.set(path, value);
                if up {
                    self.eval()
                } else {
                    None
                }
            }
            exprs => {
                let mut up = false;
                for expr in exprs {
                    up = expr.update(tgt, value).is_some() || up;
                }
                self.invalid.set(true);
                if up {
                    self.eval()
                } else {
                    None
                }
            }
        }
    }
}

pub(crate) struct StoreVarInner {
    queued: RefCell<Vec<Value>>,
    ctx: WidgetCtx,
    name: RefCell<Option<Chars>>,
    variables: Vars,
    invalid: Cell<bool>,
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
            if view::VNAME.is_match(&n) {
                Some(n)
            } else {
                invalid.set(true);
                None
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct StoreVar(Rc<StoreVarInner>);

impl Deref for StoreVar {
    type Target = StoreVarInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl StoreVar {
    fn new(ctx: &WidgetCtx, from: &[Expr], variables: &Vars) -> Self {
        let t = StoreVar(Rc::new(StoreVarInner {
            queued: RefCell::new(Vec::new()),
            ctx: ctx.clone(),
            name: RefCell::new(None),
            variables: variables.clone(),
            invalid: Cell::new(false),
        }));
        match from {
            [name, value] => t.set(name.current(), value.current()),
            _ => t.invalid.set(true),
        }
        t
    }

    fn set_var(&self, name: Chars, v: Value) {
        self.variables.borrow_mut().insert(name.clone(), v.clone());
        let _: Result<_, _> =
            self.ctx.backend.to_gui.send(ToGui::UpdateVar(name.clone(), v));
    }

    fn queue_set(&self, v: Value) {
        self.queued.borrow_mut().push(v)
    }

    fn set(&self, name: Option<Value>, value: Option<Value>) {
        if let Some(name) = varname(&self.invalid, name) {
            for v in self.queued.borrow_mut().drain(..) {
                self.set_var(name.clone(), v)
            }
            *self.name.borrow_mut() = Some(name);
        }
        if let Some(value) = value {
            match self.name.borrow().as_ref() {
                None => self.queue_set(value),
                Some(name) => self.set_var(name.clone(), value),
            }
        }
    }

    fn eval(&self) -> Option<Value> {
        if self.invalid.get() {
            Some(Value::Error(Chars::from(
                "store_var(name: string [a-z][a-z0-9_]+, value): expected 2 arguments",
            )))
        } else {
            None
        }
    }

    fn same_name(&self, new_name: &Option<Value>) -> bool {
        match (new_name, self.name.borrow().as_ref()) {
            (Some(Value::String(n0)), Some(n1)) => n0 == n1,
            _ => false,
        }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        match from {
            [name, val] => {
                let name = name.update(tgt, value);
                let value = val.update(tgt, value);
                let value = if name.is_some() && !self.same_name(&name) {
                    value.or_else(|| val.current())
                } else {
                    value
                };
                let up = value.is_some();
                self.set(name, value);
                if up {
                    self.eval()
                } else {
                    None
                }
            }
            exprs => {
                let mut up = false;
                for expr in exprs {
                    up = expr.update(tgt, value).is_some() || up;
                }
                self.invalid.set(true);
                if up {
                    self.eval()
                } else {
                    None
                }
            }
        }
    }
}

pub(crate) struct LoadInner {
    cur: RefCell<Option<Dval>>,
    ctx: WidgetCtx,
    invalid: Cell<bool>,
}

#[derive(Clone)]
pub(crate) struct Load(Rc<LoadInner>);

impl Deref for Load {
    type Target = LoadInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Load {
    fn new(ctx: &WidgetCtx, from: &[Expr]) -> Self {
        let t = Load(Rc::new(LoadInner {
            cur: RefCell::new(None),
            ctx: ctx.clone(),
            invalid: Cell::new(false),
        }));
        match from {
            [path] => t.subscribe(path.current()),
            _ => t.invalid.set(true),
        }
        t
    }

    fn subscribe(&self, name: Option<Value>) {
        if let Some(path) = pathname(&self.invalid, name) {
            let dv = self.ctx.backend.subscriber.durable_subscribe(path);
            dv.updates(UpdatesFlags::BEGIN_WITH_LAST, self.ctx.backend.updates.clone());
            *self.cur.borrow_mut() = Some(dv);
        }
    }

    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from(
            "load(expr: path) expected 1 absolute path as argument",
        )))
    }

    fn eval(&self) -> Option<Value> {
        if self.invalid.get() {
            Load::err()
        } else {
            self.cur.borrow().as_ref().and_then(|dv| match dv.last() {
                subscriber::Event::Unsubscribed => None,
                subscriber::Event::Update(v) => Some(v),
            })
        }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        match from {
            [name] => {
                let target = name.update(tgt, value);
                let up = target.is_some();
                self.subscribe(target);
                if self.invalid.get() {
                    if up {
                        Load::err()
                    } else {
                        None
                    }
                } else {
                    self.cur.borrow().as_ref().and_then(|dv| match tgt {
                        Target::Variable(_) | Target::Rpc(_) | Target::Event => None,
                        Target::Netidx(id) if dv.id() == id => Some(value.clone()),
                        Target::Netidx(_) => None,
                    })
                }
            }
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(tgt, value).is_some() || up;
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

pub(crate) struct LoadVarInner {
    name: RefCell<Option<Chars>>,
    variables: Vars,
    invalid: Cell<bool>,
}

#[derive(Clone)]
pub(crate) struct LoadVar(Rc<LoadVarInner>);

impl Deref for LoadVar {
    type Target = LoadVarInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl LoadVar {
    fn new(from: &[Expr], variables: &Vars) -> Self {
        let t = LoadVar(Rc::new(LoadVarInner {
            name: RefCell::new(None),
            variables: variables.clone(),
            invalid: Cell::new(false),
        }));
        match from {
            [name] => t.subscribe(name.current()),
            _ => t.invalid.set(true),
        }
        t
    }

    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from(
            "load_var(expr: variable name): expected 1 variable name as argument",
        )))
    }

    fn eval(&self) -> Option<Value> {
        if self.invalid.get() {
            LoadVar::err()
        } else {
            self.name
                .borrow()
                .as_ref()
                .and_then(|n| self.variables.borrow().get(n).cloned())
        }
    }

    fn subscribe(&self, name: Option<Value>) {
        if let Some(name) = varname(&self.invalid, name) {
            *self.name.borrow_mut() = Some(name);
        }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        match from {
            [name] => {
                let target = name.update(tgt, value);
                let up = target.is_some();
                self.subscribe(target);
                if self.invalid.get() {
                    if up {
                        LoadVar::err()
                    } else {
                        None
                    }
                } else {
                    match (self.name.borrow().as_ref(), tgt) {
                        (None, _)
                        | (Some(_), Target::Netidx(_))
                        | (Some(_), Target::Event)
                        | (Some(_), Target::Rpc(_)) => None,
                        (Some(vn), Target::Variable(tn)) if &**vn == tn => {
                            Some(value.clone())
                        }
                        (Some(_), Target::Variable(_)) => None,
                    }
                }
            }
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(tgt, value).is_some() || up;
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

enum ConfirmState {
    Empty,
    Invalid,
    Ready { message: Option<Value>, value: Value },
}

pub(crate) struct ConfirmInner {
    ctx: WidgetCtx,
    state: RefCell<ConfirmState>,
}

#[derive(Clone)]
pub(crate) struct Confirm(Rc<ConfirmInner>);

impl Deref for Confirm {
    type Target = ConfirmInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Confirm {
    fn new(ctx: &WidgetCtx, from: &[Expr]) -> Self {
        let t = Confirm(Rc::new(ConfirmInner {
            ctx: ctx.clone(),
            state: RefCell::new(ConfirmState::Empty),
        }));
        match from {
            [msg, val] => {
                if let Some(value) = val.current() {
                    *t.state.borrow_mut() =
                        ConfirmState::Ready { message: msg.current(), value };
                }
            }
            [val] => {
                if let Some(value) = val.current() {
                    *t.state.borrow_mut() = ConfirmState::Ready { message: None, value };
                }
            }
            _ => {
                *t.state.borrow_mut() = ConfirmState::Invalid;
            }
        }
        t
    }

    fn usage() -> Option<Value> {
        Some(Value::Error(Chars::from("confirm([msg], val): expected 1 or 2 arguments")))
    }

    fn ask(&self, msg: Option<&Value>, val: &Value) -> bool {
        let default = Value::from("proceed with");
        let msg = msg.unwrap_or(&default);
        ask_modal(&self.ctx.window, &format!("{} {}?", msg, val))
    }

    fn eval(&self) -> Option<Value> {
        match mem::replace(&mut *self.state.borrow_mut(), ConfirmState::Empty) {
            ConfirmState::Empty => None,
            ConfirmState::Invalid => Confirm::usage(),
            ConfirmState::Ready { message, value } => {
                if self.ask(message.as_ref(), &value) {
                    Some(value)
                } else {
                    None
                }
            }
        }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        match from {
            [msg, val] => {
                let m = msg.update(tgt, value).or_else(|| msg.current());
                let v = val.update(tgt, value);
                v.and_then(|v| if self.ask(m.as_ref(), &v) { Some(v) } else { None })
            }
            [val] => {
                let v = val.update(tgt, value);
                v.and_then(|v| if self.ask(None, &v) { Some(v) } else { None })
            }
            exprs => {
                let mut up = false;
                for expr in exprs {
                    up = expr.update(tgt, value).is_some() || up;
                }
                if up {
                    Confirm::usage()
                } else {
                    None
                }
            }
        }
    }
}

enum NavState {
    Normal,
    Invalid,
}

#[derive(Clone)]
pub(crate) struct Navigate {
    ctx: WidgetCtx,
    state: Rc<RefCell<NavState>>,
}

impl Navigate {
    fn new(ctx: &WidgetCtx, from: &[Expr]) -> Self {
        let t =
            Navigate { ctx: ctx.clone(), state: Rc::new(RefCell::new(NavState::Normal)) };
        match from {
            [new_window, to] => t.navigate(new_window.current(), to.current()),
            [to] => t.navigate(None, to.current()),
            _ => *t.state.borrow_mut() = NavState::Invalid,
        }
        t
    }

    fn navigate(&self, new_window: Option<Value>, to: Option<Value>) {
        if let Some(to) = to {
            let new_window =
                new_window.and_then(|v| v.cast_to::<bool>().ok()).unwrap_or(false);
            match to.cast_to::<Chars>() {
                Err(_) => *self.state.borrow_mut() = NavState::Invalid,
                Ok(s) => match s.parse::<ViewLoc>() {
                    Err(()) => *self.state.borrow_mut() = NavState::Invalid,
                    Ok(loc) => {
                        if new_window {
                            let _: Result<_, _> = self
                                .ctx
                                .backend
                                .to_gui
                                .send(ToGui::NavigateInWindow(loc));
                        } else {
                            if self.ctx.view_saved.get()
                                || ask_modal(
                                    &self.ctx.window,
                                    "Unsaved view will be lost.",
                                )
                            {
                                self.ctx.backend.navigate(loc);
                            }
                        }
                    }
                },
            }
        }
    }

    fn usage() -> Option<Value> {
        Some(Value::from("navigate([new_window], to): expected 1 or two arguments where to is e.g. /foo/bar, or netidx:/foo/bar, or, file:/path/to/view"))
    }

    fn eval(&self) -> Option<Value> {
        match &*self.state.borrow() {
            NavState::Normal => None,
            NavState::Invalid => Navigate::usage(),
        }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        let up = match from {
            [new_window, to] => {
                let new_window =
                    new_window.update(tgt, value).or_else(|| new_window.current());
                let target = to.update(tgt, value);
                let up = target.is_some();
                self.navigate(new_window, target);
                up
            }
            [to] => {
                let target = to.update(tgt, value);
                let up = target.is_some();
                self.navigate(None, target);
                up
            }
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(tgt, value).is_some() || up;
                }
                *self.state.borrow_mut() = NavState::Invalid;
                up
            }
        };
        if up {
            self.eval()
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub(crate) struct Uniq(Rc<RefCell<Option<Value>>>);

impl Uniq {
    fn new(from: &[Expr]) -> Self {
        let t = Uniq(Rc::new(RefCell::new(None)));
        match from {
            [e] => *t.0.borrow_mut() = e.current(),
            _ => *t.0.borrow_mut() = Uniq::usage(),
        }
        t
    }

    fn usage() -> Option<Value> {
        Some(Value::Error(Chars::from("uniq(e): expected 1 argument")))
    }

    fn eval(&self) -> Option<Value> {
        self.0.borrow().as_ref().cloned()
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        match from {
            [e] => e.update(tgt, value).and_then(|v| {
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
                    up = e.update(tgt, value).is_some() || up;
                }
                *self.0.borrow_mut() = Uniq::usage();
                if up {
                    self.eval()
                } else {
                    None
                }
            }
        }
    }
}

pub(crate) struct RpcCallInner {
    args: CachedVals,
    invalid: Cell<bool>,
    ctx: WidgetCtx,
}

#[derive(Clone)]
pub(crate) struct RpcCall(Rc<RpcCallInner>);

impl Deref for RpcCall {
    type Target = RpcCallInner;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl RpcCall {
    fn new(ctx: &WidgetCtx, from: &[Expr]) -> Self {
        let t = RpcCall(Rc::new(RpcCallInner {
            args: CachedVals::new(from),
            invalid: Cell::new(true),
            ctx: ctx.clone(),
        }));
        t.maybe_call();
        t
    }

    fn get_args(&self) -> Option<(Path, Vec<(Chars, Value)>)> {
        self.invalid.set(false);
        let len = self.args.0.borrow().len();
        if len == 0 || (len > 1 && len.is_power_of_two()) {
            self.invalid.set(true);
            None
        } else if self.args.0.borrow().iter().any(|v| v.is_none()) {
            None
        } else {
            match &self.args.0.borrow()[..] {
                [] => {
                    self.invalid.set(true);
                    None
                }
                [path, args @ ..] => {
                    match path.as_ref().unwrap().clone().cast_to::<Chars>() {
                        Err(_) => {
                            self.invalid.set(true);
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
                                                self.invalid.set(true);
                                                return None;
                                            }
                                            Ok(name) => match iter.next() {
                                                None | Some(None) => {
                                                    self.invalid.set(true);
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

    fn maybe_call(&self) {
        if let Some((name, args)) = self.get_args() {
            self.ctx.backend.call_rpc(Path::from(name), args);
        }
    }

    fn eval(&self) -> Option<Value> {
        if self.invalid.get() {
            Some(Value::Error(Chars::from(
                "call(rpc: string, kwargs): expected at least 1 argument, and an even number of kwargs",
            )))
        } else {
            None
        }
    }

    fn update(&self, from: &[Expr], tgt: Target, value: &Value) -> Option<Value> {
        if self.args.update(from, tgt, value) {
            self.maybe_call();
            self.eval()
        } else {
            match tgt {
                Target::Netidx(_) | Target::Variable(_) | Target::Event => None,
                Target::Rpc(name) => {
                    let args = self.args.0.borrow();
                    if args.len() == 0 {
                        self.invalid.set(true);
                        self.eval()
                    } else {
                        match args[0]
                            .as_ref()
                            .and_then(|v| v.clone().cast_to::<Chars>().ok())
                        {
                            Some(fname) if &*fname == name => Some(value.clone()),
                            Some(_) => None,
                            None => {
                                self.invalid.set(true);
                                self.eval()
                            }
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum Formula {
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
    Uniq(Uniq),
    StringJoin(CachedVals),
    StringConcat(CachedVals),
    Event(Event),
    Load(Load),
    LoadVar(LoadVar),
    Store(Store),
    StoreVar(StoreVar),
    Confirm(Confirm),
    Navigate(Navigate),
    RpcCall(RpcCall),
    Unknown(String),
}

pub(crate) static FORMULAS: [&'static str; 30] = [
    "load",
    "load_var",
    "store",
    "store_var",
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
    "uniq",
    "string_join",
    "string_concat",
    "event",
    "confirm",
    "navigate",
    "call",
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
            "uniq" => Formula::Uniq(Uniq::new(from)),
            "string_join" => Formula::StringJoin(CachedVals::new(from)),
            "string_concat" => Formula::StringConcat(CachedVals::new(from)),
            "event" => Formula::Event(Event::new(from)),
            "load" => Formula::Load(Load::new(ctx, from)),
            "load_var" => Formula::LoadVar(LoadVar::new(from, variables)),
            "store" => Formula::Store(Store::new(ctx, from)),
            "store_var" => Formula::StoreVar(StoreVar::new(ctx, from, variables)),
            "confirm" => Formula::Confirm(Confirm::new(ctx, from)),
            "navigate" => Formula::Navigate(Navigate::new(ctx, from)),
            "call" => Formula::RpcCall(RpcCall::new(ctx, from)),
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
            Formula::Uniq(c) => c.eval(),
            Formula::StringJoin(c) => eval_string_join(c),
            Formula::StringConcat(c) => eval_string_concat(c),
            Formula::Event(s) => s.eval(),
            Formula::Load(s) => s.eval(),
            Formula::LoadVar(s) => s.eval(),
            Formula::Store(s) => s.eval(),
            Formula::StoreVar(s) => s.eval(),
            Formula::Confirm(s) => s.eval(),
            Formula::Navigate(s) => s.eval(),
            Formula::RpcCall(s) => s.eval(),
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
            Formula::Uniq(c) => c.update(from, tgt, value),
            Formula::StringJoin(c) => {
                update_cached(eval_string_join, c, from, tgt, value)
            }
            Formula::StringConcat(c) => {
                update_cached(eval_string_concat, c, from, tgt, value)
            }
            Formula::Event(s) => s.update(from, tgt, value),
            Formula::Load(s) => s.update(from, tgt, value),
            Formula::LoadVar(s) => s.update(from, tgt, value),
            Formula::Store(s) => s.update(from, tgt, value),
            Formula::StoreVar(s) => s.update(from, tgt, value),
            Formula::Confirm(s) => s.update(from, tgt, value),
            Formula::Navigate(s) => s.update(from, tgt, value),
            Formula::RpcCall(s) => s.update(from, tgt, value),
            Formula::Unknown(s) => {
                Some(Value::Error(Chars::from(format!("unknown formula {}", s))))
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum Expr {
    Constant(view::Expr, Value),
    Apply { spec: view::Expr, ctx: WidgetCtx, args: Vec<Expr>, function: Box<Formula> },
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            Expr::Constant(s, _) => s,
            Expr::Apply { spec, .. } => spec,
        };
        write!(f, "{}", s.to_string())
    }
}

impl Expr {
    pub(crate) fn new(ctx: &WidgetCtx, variables: Vars, spec: view::Expr) -> Self {
        match &spec {
            view::Expr { kind: view::ExprKind::Constant(v), .. } => {
                Expr::Constant(spec.clone(), v.clone())
            }
            view::Expr { kind: view::ExprKind::Apply { args, function }, .. } => {
                let args: Vec<Expr> = args
                    .iter()
                    .map(|spec| Expr::new(ctx, variables.clone(), spec.clone()))
                    .collect();
                let function =
                    Box::new(Formula::new(&*ctx, &variables, function, &*args));
                if let Some(v) = function.current() {
                    ctx.dbg_ctx.borrow_mut().add_event(spec.id, v)
                }
                Expr::Apply { spec, ctx: ctx.clone(), args, function }
            }
        }
    }

    pub(crate) fn current(&self) -> Option<Value> {
        match self {
            Expr::Constant(_, v) => Some(v.clone()),
            Expr::Apply { function, .. } => function.current(),
        }
    }

    pub(crate) fn update(&self, tgt: Target, value: &Value) -> Option<Value> {
        match self {
            Expr::Constant(_, _) => None,
            Expr::Apply { spec, ctx, args, function } => {
                let res = function.update(args, tgt, value);
                if let Some(v) = &res {
                    ctx.dbg_ctx.borrow_mut().add_event(spec.id, v.clone());
                }
                res
            }
        }
    }
}
