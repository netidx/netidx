use crate::expr::{Expr, ExprId, ExprKind};
use fxhash::FxBuildHasher;
use netidx::{chars::Chars, subscriber::Value};
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

pub type InitFn<C, E> =
    Box<dyn Fn(&ExecCtx<C, E>, &[Node<C, E>]) -> Box<dyn Apply<C, E>>>;

pub trait Register<C, E> {
    fn register(ctx: &ExecCtx<C, E>);
}

pub trait Apply<C, E> {
    fn current(&self) -> Option<Value>;
    fn update(
        &self,
        ctx: &ExecCtx<C, E>,
        from: &[Node<C, E>],
        event: &E,
    ) -> Option<Value>;
}

pub struct ExecCtxInner<C: 'static, E: 'static> {
    pub functions: RefCell<HashMap<String, InitFn<C, E>>>,
    pub variables: RefCell<HashMap<Chars, Value>>,
    pub dbg_ctx: RefCell<DbgCtx>,
    pub user: C,
}

pub struct ExecCtx<C: 'static, E: 'static>(Rc<ExecCtxInner<C, E>>);

impl<C, E> Clone for ExecCtx<C, E> {
    fn clone(&self) -> Self {
        ExecCtx(Rc::clone(&self.0))
    }
}

impl<C, E> Deref for ExecCtx<C, E> {
    type Target = ExecCtxInner<C, E>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<C, E> ExecCtx<C, E> {
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

pub enum Node<C: 'static, E: 'static> {
    Error(Expr, Value),
    Constant(Expr, Value),
    Apply { spec: Expr, args: Vec<Node<C, E>>, function: Box<dyn Apply<C, E>> },
}

impl<C, E> fmt::Display for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Node::Error(s, _) | Node::Constant(s, _) | Node::Apply { spec: s, .. } => {
                write!(f, "{}", s)
            }
        }
    }
}

impl<C, E> Node<C, E> {
    pub fn compile(ctx: &ExecCtx<C, E>, spec: Expr) -> Self {
        match &spec {
            Expr { kind: ExprKind::Constant(v), id } => {
                ctx.dbg_ctx.borrow_mut().add_event(*id, v.clone());
                Node::Constant(spec.clone(), v.clone())
            }
            Expr { kind: ExprKind::Apply { args, function }, .. } => {
                match ctx.functions.borrow().get(function) {
                    None => Node::Error(
                        spec.clone(),
                        Value::Error(Chars::from(format!(
                            "unknown function {}",
                            function
                        ))),
                    ),
                    Some(init) => {
                        let args: Vec<Node<C, E>> = args
                            .iter()
                            .map(|spec| Node::compile(ctx, spec.clone()))
                            .collect();
                        let function = init(ctx, &args);
                        if let Some(v) = function.current() {
                            ctx.dbg_ctx.borrow_mut().add_event(spec.id, v)
                        }
                        Node::Apply { spec, args, function }
                    }
                }
            }
        }
    }

    pub fn current(&self) -> Option<Value> {
        match self {
            Node::Error(_, v) => Some(v.clone()),
            Node::Constant(_, v) => Some(v.clone()),
            Node::Apply { function, .. } => function.current(),
        }
    }

    pub fn update(&self, ctx: &ExecCtx<C, E>, event: &E) -> Option<Value> {
        match self {
            Node::Error(_, v) => Some(v.clone()),
            Node::Constant(_, _) => None,
            Node::Apply { spec, args, function } => {
                let res = function.update(ctx, &args, event);
                if let Some(v) = &res {
                    ctx.dbg_ctx.borrow_mut().add_event(spec.id, v.clone());
                }
                res
            }
        }
    }
}
