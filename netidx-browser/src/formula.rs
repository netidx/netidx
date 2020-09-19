use super::{Source, WidgetCtx};
use indexmap::IndexMap;
use netidx::{
    chars::Chars,
    subscriber::{SubId, Value},
    protocol::publisher::v1::Typ,
};
use netidx_protocols::view;
use std::{
    cell::{Cell, RefCell},
    cmp::{PartialEq, PartialOrd},
    collections::HashMap,
    rc::Rc,
    result::Result,
};

#[derive(Debug, Clone)]
pub struct CachedVals(Rc<RefCell<Vec<Option<Value>>>>);

impl CachedVals {
    fn new(from: &[Source]) -> CachedVals {
        CachedVals(Rc::new(RefCell::new(from.into_iter().map(|s| s.current()).collect())))
    }

    fn update(&self, from: &[Source], id: SubId, value: &Value) -> bool {
        let mut vals = self.0.borrow_mut();
        from.into_iter().enumerate().fold(false, |res, (i, src)| {
            match src.update(id, value) {
                None => res,
                v @ Some(_) => {
                    vals[i] = v;
                    true
                }
            }
        })
    }

    fn update_var(&self, from: &[Source], name: &str, value: &Value) -> bool {
        let mut vals = self.0.borrow_mut();
        from.into_iter().enumerate().fold(false, |res, (i, src)| {
            match src.update_var(name, value) {
                None => res,
                v @ Some(_) => {
                    vals[i] = v;
                    true
                }
            }
        })
    }
}

fn eval_all(from: &CachedVals) -> Option<Value> {
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
    fn new(from: &[Source]) -> Self {
        Mean {
            from: CachedVals::new(from),
            total: Rc::new(Cell::new(0.)),
            samples: Rc::new(Cell::new(0)),
        }
    }

    fn update(&self, from: &[Source], id: SubId, value: &Value) -> Option<Value> {
        if self.from.update(from, id, value) {
            self.eval()
        } else {
            None
        }
    }

    fn update_var(&self, from: &[Source], name: &str, value: &Value) -> Option<Value> {
        if self.from.update_var(from, name, value) {
            self.eval()
        } else {
            None
        }
    }

    fn eval(&self) -> Option<Value> {
        for v in &*self.from.0.borrow() {
            if let Some(v) = v {
                if let Some(v) = v.clone().cast_to_f64() {
                    self.total.set(self.total.get() + v);
                    self.samples.set(self.samples.get() + 1);
                }
            }
        }
        if self.samples.get() > 0 {
            Some(Value::F64(self.total.get() / (self.samples.get() as f64)))
        } else {
            None
        }
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
        [cond, b1, b2] => match cond {
            None => None,
            Some(Value::True) => b1.clone(),
            Some(Value::False) => b2.clone(),
            _ => Some(Value::Error(Chars::from(
                "if(predicate, caseIf, caseElse): expected boolean condition",
            ))),
        },
        _ => Some(Value::Error(Chars::from(
            "if(predicate, caseIf, caseElse): expected 3 arguments",
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

#[derive(Debug, Clone)]
pub(super) struct Eval {
    ctx: WidgetCtx,
    cached: CachedVals,
    current: RefCell<Result<Source, Value>>,
}

impl Eval {
    fn new(ctx: &WidgetCtx, from: &[Source]) -> Self {
        let t = Eval {
            ctx: ctx.clone(),
            cached: CachedVals::new(from),
            current: RefCell::new(Err(Value::Null)),
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
                Value::String(s) => match s.parse::<view::Source>() {
                    Ok(spec) => Ok(Source::new(&self.ctx, &HashMap::new(), spec)),
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

    fn update(&self, from: &[Source], id: SubId, value: &Value) -> Option<Value> {
        if self.cached.update(from, id, value) {
            self.compile();
        }
        match &*self.current.borrow() {
            Ok(s) => s.update(id, value),
            Err(v) => Some(v.clone()),
        }
    }

    fn update_var(&self, from: &[Source], name: &str, value: &Value) -> Option<Value> {
        if self.cached.update_var(from, name, value) {
            self.compile();
        }
        match &*self.current.borrow() {
            Ok(s) => s.update_var(name, value),
            Err(v) => Some(v.clone()),
        }
    }
}

fn update_cached(
    eval: impl Fn(&CachedVals) -> Option<Value>,
    cached: &CachedVals,
    from: &[Source],
    id: SubId,
    value: &Value,
) -> Option<Value> {
    if cached.update(from, id, value) {
        eval(cached)
    } else {
        None
    }
}

fn update_var_cached(
    eval: impl Fn(&CachedVals) -> Option<Value>,
    cached: &CachedVals,
    from: &[Source],
    name: &str,
    value: &Value,
) -> Option<Value> {
    if cached.update_var(from, name, value) {
        eval(cached)
    } else {
        None
    }
}

#[derive(Debug, Clone)]
pub(super) enum Formula {
    Any(RefCell<Option<Value>>),
    All(CachedVals),
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
    Unknown(String),
}

pub(super) static FORMULAS: [&'static str; 17] = [
    "any", "all", "sum", "product", "divide", "mean", "min", "max", "and", "or", "not",
    "cmp", "if", "filter", "cast", "isa", "eval",
];

impl Formula {
    pub(super) fn new(ctx: &WidgetCtx, name: &str, from: &[Source]) -> Formula {
        match name {
            "any" => Formula::Any(RefCell::new(None)),
            "all" => Formula::All(CachedVals::new(from)),
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
            "eval" => Formula::Eval(Eval::new(ctx, from)),
            _ => Formula::Unknown(String::from(name)),
        }
    }

    pub(super) fn current(&self) -> Option<Value> {
        match self {
            Formula::Any(c) => c.borrow().clone(),
            Formula::All(c) => eval_all(c),
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
            Formula::Unknown(s) => {
                Some(Value::Error(Chars::from(format!("unknown formula {}", s))))
            }
        }
    }

    pub(super) fn update(
        &self,
        from: &[Source],
        id: SubId,
        value: &Value,
    ) -> Option<Value> {
        match self {
            Formula::Any(c) => {
                let res = from.into_iter().filter_map(|s| s.update(id, value)).fold(
                    None,
                    |res, v| match res {
                        None => Some(v),
                        Some(_) => res,
                    },
                );
                *c.borrow_mut() = res.clone();
                res
            }
            Formula::All(c) => update_cached(eval_all, c, from, id, value),
            Formula::Sum(c) => update_cached(eval_sum, c, from, id, value),
            Formula::Product(c) => update_cached(eval_product, c, from, id, value),
            Formula::Divide(c) => update_cached(eval_divide, c, from, id, value),
            Formula::Mean(m) => m.update(from, id, value),
            Formula::Min(c) => update_cached(eval_min, c, from, id, value),
            Formula::Max(c) => update_cached(eval_max, c, from, id, value),
            Formula::And(c) => update_cached(eval_and, c, from, id, value),
            Formula::Or(c) => update_cached(eval_or, c, from, id, value),
            Formula::Not(c) => update_cached(eval_not, c, from, id, value),
            Formula::Cmp(c) => update_cached(eval_cmp, c, from, id, value),
            Formula::If(c) => update_cached(eval_if, c, from, id, value),
            Formula::Filter(c) => update_cached(eval_filter, c, from, id, value),
            Formula::Cast(c) => update_cached(eval_cast, c, from, id, value),
            Formula::IsA(c) => update_cached(eval_isa, c, from, id, value),
            Formula::Eval(e) => e.update(from, id, value),
            Formula::Unknown(s) => {
                Some(Value::Error(Chars::from(format!("unknown formula {}", s))))
            }
        }
    }

    pub(super) fn update_var(
        &self,
        from: &[Source],
        name: &str,
        value: &Value,
    ) -> Option<Value> {
        match self {
            Formula::Any(c) => {
                let res = from
                    .into_iter()
                    .filter_map(|s| s.update_var(name, value))
                    .fold(None, |res, v| match res {
                        None => Some(v),
                        Some(_) => res,
                    });
                *c.borrow_mut() = res.clone();
                res
            }
            Formula::All(c) => update_var_cached(eval_all, c, from, name, value),
            Formula::Sum(c) => update_var_cached(eval_sum, c, from, name, value),
            Formula::Product(c) => update_var_cached(eval_product, c, from, name, value),
            Formula::Divide(c) => update_var_cached(eval_divide, c, from, name, value),
            Formula::Mean(m) => m.update_var(from, name, value),
            Formula::Min(c) => update_var_cached(eval_min, c, from, name, value),
            Formula::Max(c) => update_var_cached(eval_max, c, from, name, value),
            Formula::And(c) => update_var_cached(eval_and, c, from, name, value),
            Formula::Or(c) => update_var_cached(eval_or, c, from, name, value),
            Formula::Not(c) => update_var_cached(eval_not, c, from, name, value),
            Formula::Cmp(c) => update_var_cached(eval_cmp, c, from, name, value),
            Formula::If(c) => update_var_cached(eval_if, c, from, name, value),
            Formula::Filter(c) => update_var_cached(eval_filter, c, from, name, value),
            Formula::Cast(c) => update_var_cached(eval_cast, c, from, name, value),
            Formula::IsA(c) => update_var_cached(eval_isa, c, from, name, value),
            Formula::Eval(e) => e.update_var(from, name, value),
            Formula::Unknown(s) => {
                Some(Value::Error(Chars::from(format!("unknown formula {}", s))))
            }
        }
    }
}
