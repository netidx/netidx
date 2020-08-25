use super::Source;
use anyhow::Result;
use indexmap::IndexMap;
use netidx::subscriber::Value;
use std::{cell::Cell, rc::Rc, sync::Arc};

#[derive(Clone, Copy)]
enum Update<'a> {
    Var(&'a str, &'a Value),
    DVal(&'a Arc<IndexMap<SubId, Value>>),
}

fn any_update(from: &[Source], up: Update) -> Option<Value> {
    from.into_iter().fold(None, |res, s| {
        let v = match up {
            Update::Var(name, value) => s.update_var(name, value),
            Update::DVal(changed) => s.update(changed),
        };
        match (v, res) {
            (_, v @ Some(_)) => v,
            (v @ Some(_), None) => v,
            (None, None) => None,
        }
    })
}

fn any_eval(from: &[Source]) -> Option<Value> {
    from.into_iter().find_map(|s| s.current())
}

fn updated_or_cur(s: &Source, up: Update) -> Option<Value> {
    let updated = match up {
        Update::Var(name, value) => s.update_var(name, value),
        Update::DVal(changed) => s.update(changed),
    };
    match updated {
        None => s.current(),
        Some(v) => Some(v),
    }
}

fn all_update(from: &[Source], up: Update) -> Option<Value> {
    match from {
        [] => None,
        [hd, tl @ ..] => match updated_or_cur(hd, up) {
            None => None,
            v @ Some(_) => tl.into_iter().fold(v, |res, s| {
                if res == updated_or_cur(s, up) {
                    res
                } else {
                    None
                }
            }),
        },
    }
}

fn all_eval(from: &[Source]) -> Option<Value> {
    match from {
        [] => None,
        [hd, tl @ ..] => match hd.current() {
            None => None,
            v @ Some(_) => {
                if tl.into_iter().all(|s| s.current() == v) {
                    v
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
        (None, r @ Some(_)) => Some(r),
        (Some(l), Some(r)) => Some(l + r),
    }
}

fn sum_update(from: &[Source], up: Update) -> Option<Value> {
    let changed = from.into_iter().filter_map(|s| match up {
        Update::Var(name, value) => s.update_var(name, value),
        Update::DVal(changed) => s.update(changed)
    }).last().is_some();
    if !changed {
        None
    } else {
        sum_eval(from)
    }
}

fn sum_eval(from: &[Source]) -> Option<Value> {
    from.into_iter().fold(None, |res, s| match res {
        res @ Some(Value::Error(_)) => res,
        res => add_vals(res, s.current()),
    })
}

fn prod_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (None, r @ Some(_)) => Some(r),
        (Some(l), Some(r)) => Some(l * r),
    }
}

fn product_update(from: &[Source], up: Update) -> Option<Value> {
    from.into_iter().fold(None, |res, s| match res {
        res @ Some(Value::Error(_)) => res,
        res => prod_vals(res, updated_or_cur(s, up)),
    })
}

fn product_eval(from: &[Source]) -> Option<Value> {
    from.into_iter().fold(None, |res, s| match res {
        res @ Some(Value::Error(_)) => res,
        res => prod_vals(res, s.current()),
    })
}

fn div_vals(lhs: Option<Value>, rhs: Option<Value>) -> Option<Value> {
    match (lhs, rhs) {
        (None, None) => None,
        (None, r @ Some(_)) => r,
        (r @ Some(_), None) => r,
        (None, r @ Some(_)) => Some(r),
        (Some(l), Some(r)) => Some(l / r),
    }
}

fn divide_update(from: &[Source], up: Update) -> Option<Value> {
    from.into_iter().fold(None, |res, s| match res {
        res @ Some(Value::Error(_)) => res,
        res => div_vals(res, updated_or_cur(s, up)),
    })
}

fn divide_eval(from: &[Source]) -> Option<Value> {
    from.into_iter().fold(None, |res, s| match res {
        res @ Some(Value::Error(_)) => res,
        res => div_vals(res, s.current()),
    })
}

fn cast_update(from: &[Source], up: Update) -> Option<Value> {
    match from {
        [typ, src] => {
            
        }
        _ => Some(Value::Error(format!(
            "cast(typ, src): expected 2 arguments got {}",
            from.len()
        ))),
    }
}

struct Mean {
    total: Cell<f64>,
    samples: Cell<usize>,
}

fn mean_update(from: Source, total: Rc<Cell<f64>>, samples: Rc<Cell<usize>>) {}

enum Formula {
    Any,
    All,
    Sum,
    Product,
    Divide,
    Mean(Rc<Mean>),
    Min,
    Max,
    And,
    Or,
    Not,
    If,
    Case,
    Filter,
    Cast,
    Is,
}

impl Formula {
    fn new(name: &str) -> Result<Formula> {
        match name {
            "any" => Ok(Formula::Any),
            "all" => Ok(Formula::All),
            "sum" => Ok(Formula::Sum),
            "product" => Ok(Formula::Product),
            "divide" => Ok(Formula::Divide),
            "mean" => Ok(Formula::Mean { total: 0., samples: 0 }),
            "min" => Ok(Formula::Min),
            "max" => Ok(Formula::Max),
            "and" => Ok(Formula::And),
            "or" => Ok(Formula::Or),
            "not" => Ok(Formula::Not),
            "if" => Ok(Formula::If),
            "case" => Ok(Formula::Case),
            "filter" => Ok(Formula::Filter),
            "cast" => Ok(Formula::Cast),
            "is" => Ok(Formula::Is),
            name => Err(anyhow!("no such function {}", name)),
        }
    }

    fn update(
        &self,
        from: &[Source],
        changed: &Arc<IndexMap<SubId, Value>>,
    ) -> Option<Value> {
        match self {
            Formula::Any => update_any(from, changed),
            _ => todo!(),
        }
    }
}
