use crate::vm::{Apply, ExecCtx, InitFn, Node, Register, Target};
use netidx::subscriber::Value;
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
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
