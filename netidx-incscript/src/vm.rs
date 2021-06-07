use crate::expr::{Expr, ExprId, ExprKind};
use anyhow::{bail, Result};
use fxhash::FxBuildHasher;
use netidx::{
    chars::Chars,
    subscriber::{SubId, Value},
};
use std::{
    cell::RefCell,
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

    pub fn add_watch(&mut self, id: ExprId, watch: &Rc<dyn Fn(&Value)>) {
        let watches = self.watch.entry(id).or_insert(vec![]);
        if let Some(v) = self.current.get(&id) {
            watch(v);
        }
        watches.push(Rc::downgrade(watch));
    }

    pub fn add_event(&mut self, id: ExprId, value: Value) {
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

    pub fn clear(&mut self) {
        self.events.clear();
        self.current.clear();
        self.watch.clear();
    }
}

pub type InitFn<C> = Box<dyn Fn(&ExecCtx<C>, &[Node<C>]) -> Box<dyn Apply<C>> + 'static>;

pub trait Register<C> {
    fn register(ctx: &ExecCtx<C>);
}

pub struct ExecCtxInner<C> {
    pub functions: RefCell<HashMap<String, InitFn<C>>>,
    pub variables: RefCell<HashMap<Chars, Value>>,
    pub dbg_ctx: RefCell<DbgCtx>,
    pub user: C,
}

pub struct ExecCtx<C>(Rc<ExecCtxInner<C>>);

impl<C> Clone for ExecCtx<C> {
    fn clone(&self) -> Self {
        ExecCtx(Rc::clone(&self.0))
    }
}

impl<C> Deref for ExecCtx<C> {
    type Target = ExecCtxInner<C>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C> ExecCtx<C> {
    pub fn new(user: C) -> Self {
        let inner = ExecCtxInner {
            functions: RefCell::new(HashMap::new()),
            variables: RefCell::new(HashMap::new()),
            dbg_ctx: RefCell::new(DbgCtx::new()),
            user,
        };
        ExecCtx(Rc::new(inner))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Target<'a> {
    Event,
    Variable(&'a str),
    Netidx(SubId),
    Rpc(&'a str),
}

pub trait Apply<C> {
    fn current(&self) -> Option<Value>;
    fn update(&self, from: &[Node<C>], tgt: Target, value: &Value) -> Option<Value>;
}

pub enum Node<C> {
    Constant(Expr, Value),
    Apply { spec: Expr, ctx: ExecCtx<C>, args: Vec<Node<C>>, function: Box<dyn Apply<C>> },
}

impl<C> fmt::Display for Node<C> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            Node::Constant(s, _) => s,
            Node::Apply { spec, .. } => spec,
        };
        write!(f, "{}", s.to_string())
    }
}

impl<C> Node<C>
where
    C: 'static,
{
    pub fn compile(ctx: &ExecCtx<C>, spec: Expr) -> Result<Self> {
        match &spec {
            Expr { kind: ExprKind::Constant(v), id } => {
                ctx.dbg_ctx.borrow_mut().add_event(*id, v.clone());
                Ok(Node::Constant(spec.clone(), v.clone()))
            }
            Expr { kind: ExprKind::Apply { args, function }, .. } => {
                let args: Result<Vec<Node<C>>> =
                    args.iter().map(|spec| Node::compile(ctx, spec.clone())).collect();
                let args = args?;
                let function = match ctx.functions.borrow().get(function) {
                    None => bail!("no such function {}", function),
                    Some(init) => init(ctx, &args),
                };
                if let Some(v) = function.current() {
                    ctx.dbg_ctx.borrow_mut().add_event(spec.id, v)
                }
                Ok(Node::Apply { spec, ctx: ctx.clone(), args, function })
            }
        }
    }

    pub fn current(&self) -> Option<Value> {
        match self {
            Node::Constant(_, v) => Some(v.clone()),
            Node::Apply { function, .. } => function.current(),
        }
    }

    pub fn update(&self, tgt: Target, value: &Value) -> Option<Value> {
        match self {
            Node::Constant(_, _) => None,
            Node::Apply { spec, ctx, args, function } => {
                let res = function.update(&args, tgt, value);
                if let Some(v) = &res {
                    ctx.dbg_ctx.borrow_mut().add_event(spec.id, v.clone());
                }
                res
            }
        }
    }
}
