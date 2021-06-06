use crate::{
    expr::{Expr, ExprId, ExprKind},
    vm::{Apply, ExecCtx, InitFn, Node, Target},
};
use anyhow::{bail, Result};
use fxhash::FxBuildHasher;
use netidx::{
    chars::Chars,
    subscriber::{SubId, Value},
};
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    fmt,
    ops::Deref,
    rc::{Rc, Weak},
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

impl Any {
    pub fn register<C: 'static>(ctx: &ExecCtx<C>) {
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
