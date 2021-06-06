use crate::expr::{self, ExprId};
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

pub struct DbgCtx {
    events: VecDeque<(ExprId, Value)>,
    watch: HashMap<ExprId, Vec<Weak<dyn Fn(&Value)>>, FxBuildHasher>,
    current: HashMap<ExprId, Value, FxBuildHasher>,
}

impl DbgCtx {
    fn new() -> Self {
        DbgCtx {
            events: VecDeque::new(),
            watch: HashMap::with_hasher(FxBuildHasher::default()),
            current: HashMap::with_hasher(FxBuildHasher::default()),
        }
    }

    fn add_watch(&mut self, id: ExprId, watch: &Rc<dyn Fn(&Value)>) {
        let watches = self.watch.entry(id).or_insert(vec![]);
        if let Some(v) = self.current.get(&id) {
            watch(v);
        }
        watches.push(Rc::downgrade(watch));
    }

    fn add_event(&mut self, id: ExprId, value: Value) {
        const MAX: usize = 1000;
        self.events.push_back((id, value.clone()));
        self.current.insert(id, value.clone());
        if self.events.len() > MAX {
            self.events.pop_front();
            if self.watch.len() > MAX {
                self.watch.retain(|_, vs| {
                    vs.retain(|v| Weak::upgrade(v).is_some());
                    !vs.is_empty()
                });
            }
        }
        if let Some(watch) = self.watch.get_mut(&id) {
            let mut i = 0;
            while i < watch.len() {
                match Weak::upgrade(&watch[i]) {
                    None => {
                        watch.remove(i);
                    }
                    Some(f) => {
                        f(&value);
                        i += 1;
                    }
                }
            }
        }
    }

    fn clear(&mut self) {
        self.events.clear();
        self.current.clear();
        self.watch.clear();
    }
}

pub struct ExecCtxInner<C> {
    functions: 
    variables: RefCell<HashMap<Chars, Value>>,
    dbg_ctx: RefCell<DbgCtx>,
    user: C,
}

#[derive(Clone)]
pub struct ExecCtx<C>(Rc<ExecCtxInner<C>>);

impl<C> Deref for ExecCtx<C> {
    type Target = ExecCtxInner<C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Target<'a> {
    Event,
    Variable(&'a str),
    Netidx(SubId),
    Rpc(&'a str),
}

trait Updateable {
    fn current(&self) -> Option<Value>;
    fn update(&self, tgt: Target, value: &Value) -> Option<Value>;
}

pub enum Expr<C> {
    Constant(expr::Expr, Value),
    Apply {
        spec: expr::Expr,
        ctx: ExecCtx<C>,
        args: Vec<Expr<C>>,
        function: Box<dyn Updateable>,
    },
}

impl<C> fmt::Display for Expr<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            Expr::Constant(s, _) => s,
            Expr::Apply { spec, .. } => spec,
        };
        write!(f, "{}", s.to_string())
    }
}

impl<C> Expr<C>
where
    C: Clone,
{
    pub(crate) fn compile(ctx: &ExecCtx<C>, spec: expr::Expr) -> Self {
        match &spec {
            expr::Expr { kind: expr::ExprKind::Constant(v), id } => {
                ctx.dbg_ctx.borrow_mut().add_event(*id, v.clone());
                Expr::Constant(spec.clone(), v.clone())
            }
            expr::Expr { kind: expr::ExprKind::Apply { args, function }, .. } => {
                let args: Vec<Expr<C>> =
                    args.iter().map(|spec| Expr::compile(ctx, spec.clone())).collect();
                let function = Box::new(Formula::new(ctx, &variables, function, &*args));
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
