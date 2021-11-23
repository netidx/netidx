pub use crate::stdfn::RpcCallId;
use crate::{
    expr::{Expr, ExprId, ExprKind},
    stdfn,
};
use fxhash::{FxBuildHasher, FxHashMap};
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{Dval, SubId, UpdatesFlags, Value},
};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{Arc, Weak},
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

    pub fn add_watch(&mut self, id: ExprId, watch: &Arc<dyn Fn(&Value)>) {
        let watches = self.watch.entry(id).or_insert_with(Vec::new);
        if let Some(v) = self.current.get(&id) {
            watch(v);
        }
        watches.push(Arc::downgrade(watch));
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
        self.watch.retain(|_, v| {
            v.retain(|w| Weak::strong_count(w) > 0);
            v.len() > 0
        });
    }
}

#[derive(Debug)]
pub enum Event<E> {
    Variable(Path, Chars, Value),
    Netidx(SubId, Value),
    Rpc(RpcCallId, Value),
    User(E),
}

pub type InitFn<C, E> =
    Arc<dyn Fn(&mut ExecCtx<C, E>, &[Node<C, E>], Path, ExprId) -> Box<dyn Apply<C, E>>>;

pub trait Register<C: Ctx, E> {
    fn register(ctx: &mut ExecCtx<C, E>);
}

pub trait Apply<C: Ctx, E> {
    fn current(&self) -> Option<Value>;
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value>;
}

pub trait Ctx {
    fn clear(&mut self);
    fn durable_subscribe(
        &mut self,
        flags: UpdatesFlags,
        path: Path,
        ref_by: ExprId,
    ) -> Dval;
    fn unsubscribe(&mut self, path: Path, dv: Dval, ref_by: ExprId);
    fn ref_var(&mut self, name: Chars, scope: Path, ref_by: ExprId);
    fn unref_var(&mut self, name: Chars, scope: Path, ref_by: ExprId);
    fn set_var(
        &mut self,
        variables: &mut FxHashMap<Path, FxHashMap<Chars, Value>>,
        local: bool,
        scope: Path,
        name: Chars,
        value: Value,
    );

    /// For a given name, this must have at most one outstanding call
    /// at a time, and must preserve the order of the calls. Calls to
    /// different names may execute concurrently.
    fn call_rpc(
        &mut self,
        name: Path,
        args: Vec<(Chars, Value)>,
        ref_by: ExprId,
        id: RpcCallId,
    );
}

pub fn store_var(
    variables: &mut FxHashMap<Path, FxHashMap<Chars, Value>>,
    local: bool,
    scope: &Path,
    name: &Chars,
    value: Value,
) {
    if local {
        variables
            .entry(scope.clone())
            .or_insert_with(|| HashMap::with_hasher(FxBuildHasher::default()))
            .insert(name.clone(), value);
    } else {
        let mut iter = Path::dirnames(scope);
        loop {
            match iter.next_back() {
                Some(scope) => {
                    if let Some(vars) = variables.get_mut(scope) {
                        if let Some(var) = vars.get_mut(name) {
                            break *var = value;
                        }
                    }
                }
                None => break store_var(variables, true, &Path::root(), name, value),
            }
        }
    }
}

pub struct ExecCtx<C: Ctx + 'static, E: 'static> {
    pub functions: FxHashMap<String, InitFn<C, E>>,
    pub variables: FxHashMap<Path, FxHashMap<Chars, Value>>,
    pub dbg_ctx: DbgCtx,
    pub user: C,
}

impl<C: Ctx, E> ExecCtx<C, E> {
    pub fn lookup_var(&self, scope: &Path, name: &Chars) -> Option<(&Path, &Value)> {
        let mut iter = Path::dirnames(scope);
        loop {
            match iter.next_back() {
                Some(scope) => {
                    if let Some((scope, vars)) = self.variables.get_key_value(scope) {
                        if let Some(var) = vars.get(name) {
                            break Some((scope, var));
                        }
                    }
                }
                None => break None,
            }
        }
    }

    pub fn clear(&mut self) {
        self.variables.clear();
        self.dbg_ctx.clear();
        self.user.clear();
    }

    pub fn no_std(user: C) -> Self {
        ExecCtx {
            functions: HashMap::with_hasher(FxBuildHasher::default()),
            variables: HashMap::with_hasher(FxBuildHasher::default()),
            dbg_ctx: DbgCtx::new(),
            user,
        }
    }

    pub fn new(user: C) -> Self {
        let mut t = ExecCtx::no_std(user);
        stdfn::All::register(&mut t);
        stdfn::And::register(&mut t);
        stdfn::Any::register(&mut t);
        stdfn::RpcCall::register(&mut t);
        stdfn::Cast::register(&mut t);
        stdfn::Cmp::register(&mut t);
        stdfn::Contains::register(&mut t);
        stdfn::Count::register(&mut t);
        stdfn::Divide::register(&mut t);
        stdfn::Do::register(&mut t);
        stdfn::EndsWith::register(&mut t);
        stdfn::Eval::register(&mut t);
        stdfn::Filter::register(&mut t);
        stdfn::If::register(&mut t);
        stdfn::Isa::register(&mut t);
        stdfn::IsErr::register(&mut t);
        stdfn::Load::register(&mut t);
        stdfn::LoadVar::register(&mut t);
        stdfn::Max::register(&mut t);
        stdfn::Mean::register(&mut t);
        stdfn::Min::register(&mut t);
        stdfn::Not::register(&mut t);
        stdfn::Or::register(&mut t);
        stdfn::Product::register(&mut t);
        stdfn::Replace::register(&mut t);
        stdfn::Sample::register(&mut t);
        stdfn::StartsWith::register(&mut t);
        stdfn::Store::register(&mut t);
        stdfn::StoreVar::register(&mut t);
        stdfn::StringConcat::register(&mut t);
        stdfn::StringJoin::register(&mut t);
        stdfn::StripPrefix::register(&mut t);
        stdfn::StripSuffix::register(&mut t);
        stdfn::Sum::register(&mut t);
        stdfn::TrimEnd::register(&mut t);
        stdfn::Trim::register(&mut t);
        stdfn::TrimStart::register(&mut t);
        stdfn::Uniq::register(&mut t);
        t
    }
}

pub enum Node<C: Ctx, E> {
    Error(Expr, Value),
    Constant(Expr, Value),
    Apply { spec: Expr, args: Vec<Node<C, E>>, function: Box<dyn Apply<C, E>> },
}

impl<C: Ctx, E> fmt::Display for Node<C, E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Node::Error(s, _) | Node::Constant(s, _) | Node::Apply { spec: s, .. } => {
                write!(f, "{}", s)
            }
        }
    }
}

impl<C: Ctx, E> Node<C, E> {
    pub fn compile_int(
        ctx: &mut ExecCtx<C, E>,
        spec: Expr,
        scope: Path,
        top_id: ExprId,
    ) -> Self {
        match &spec {
            Expr { kind: ExprKind::Constant(v), id } => {
                ctx.dbg_ctx.add_event(*id, v.clone());
                Node::Constant(spec.clone(), v.clone())
            }
            Expr { kind: ExprKind::Apply { args, function }, id } => {
                let scope = if function == "do" && id != &top_id {
                    scope.append(&format!("do{:?}", id))
                } else {
                    scope
                };
                let args: Vec<Node<C, E>> = args
                    .iter()
                    .map(|spec| {
                        Node::compile_int(ctx, spec.clone(), scope.clone(), top_id)
                    })
                    .collect();
                match ctx.functions.get(function).map(Arc::clone) {
                    None => {
                        let e = Value::Error(Chars::from(format!(
                            "unknown function {}",
                            function
                        )));
                        ctx.dbg_ctx.add_event(spec.id, e.clone());
                        Node::Error(spec.clone(), e)
                    }
                    Some(init) => {
                        let function = init(ctx, &args, scope, top_id);
                        if let Some(v) = function.current() {
                            ctx.dbg_ctx.add_event(spec.id, v)
                        }
                        Node::Apply { spec, args, function }
                    }
                }
            }
        }
    }

    pub fn compile(ctx: &mut ExecCtx<C, E>, scope: Path, spec: Expr) -> Self {
        let top_id = spec.id;
        Self::compile_int(ctx, spec, scope, top_id)
    }

    pub fn current(&self) -> Option<Value> {
        match self {
            Node::Error(_, v) => Some(v.clone()),
            Node::Constant(_, v) => Some(v.clone()),
            Node::Apply { function, .. } => function.current(),
        }
    }

    pub fn update(&mut self, ctx: &mut ExecCtx<C, E>, event: &Event<E>) -> Option<Value> {
        match self {
            Node::Error(_, v) => Some(v.clone()),
            Node::Constant(_, _) => None,
            Node::Apply { spec, args, function } => {
                let res = function.update(ctx, args, event);
                if let Some(v) = &res {
                    ctx.dbg_ctx.add_event(spec.id, v.clone());
                }
                res
            }
        }
    }
}
