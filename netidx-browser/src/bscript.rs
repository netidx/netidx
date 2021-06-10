use super::{util::ask_modal, ToGui, Vars, ViewLoc, WidgetCtx};
use netidx::{
    chars::Chars,
    path::Path,
    subscriber::{self, Dval, SubId, UpdatesFlags, Value},
};
use netidx_bscript::{
    expr,
    vm::{Apply, ExecCtx, InitFn, Node, Register},
};
use netidx_protocols::view;
use std::{
    cell::{Cell, RefCell},
    fmt, mem,
    ops::Deref,
    rc::Rc,
    result::Result,
};

#[derive(Debug, Clone)]
pub(crate) enum Target {
    Event(Value),
    Variable(String, Value),
    Netidx(SubId, Value),
    Rpc(String, Value),
}

pub(crate) struct Event {
    cur: RefCell<Option<Value>>,
    invalid: Cell<bool>,
}

impl Register<WidgetCtx, Target> for Event {
    fn register(ctx: &ExecCtx<WidgetCtx, Target>) {
        let f: InitFn<WidgetCtx, Target> = Box::new(|_, from| {
            Box::new(Event {
                cur: RefCell::new(None),
                invalid: Cell::new(from.len() > 0),
            })
        });
        ctx.functions.borrow_mut().insert("event".into(), f);
    }
}

impl Apply<WidgetCtx, Target> for Event {
    fn current(&self) -> Option<Value> {
        if self.invalid.get() {
            Event::err()
        } else {
            self.cur.borrow().as_ref().cloned()
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<WidgetCtx, Target>,
        from: &[Node<WidgetCtx, Target>],
        event: &Target,
    ) -> Option<Value> {
        self.invalid.set(from.len() > 0);
        match event {
            Target::Variable(_, _) | Target::Netidx(_, _) | Target::Rpc(_, _) => None,
            Target::Event(value) => {
                *self.cur.borrow_mut() = Some(value.clone());
                self.current()
            }
        }
    }
}

impl Event {
    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from("event(): expected 0 arguments")))
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

pub(crate) struct Store {
    queued: RefCell<Vec<Value>>,
    dv: RefCell<Option<(Path, Dval)>>,
    invalid: Cell<bool>,
}

impl Register<WidgetCtx, Target> for Store {
    fn register(ctx: &ExecCtx<WidgetCtx, Target>) {
        let f: InitFn<WidgetCtx, Target> = Box::new(|ctx, from| {
            let t = Store {
                queued: RefCell::new(Vec::new()),
                dv: RefCell::new(None),
                invalid: Cell::new(false),
            };
            match from {
                [to, val] => t.set(ctx, to.current(), val.current()),
                _ => t.invalid.set(true),
            }
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("store".into(), f);
    }
}

impl Apply<WidgetCtx, Target> for Store {
    fn current(&self) -> Option<Value> {
        if self.invalid.get() {
            Some(Value::Error(Chars::from(
                "store(tgt: absolute path, val): expected 2 arguments",
            )))
        } else {
            None
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<WidgetCtx, Target>,
        from: &[Node<WidgetCtx, Target>],
        event: &Target,
    ) -> Option<Value> {
        match from {
            [path, val] => {
                let path = path.update(ctx, event);
                let value = val.update(ctx, event);
                let value = if path.is_some() && !self.same_path(&path) {
                    value.or_else(|| val.current())
                } else {
                    value
                };
                let up = value.is_some();
                self.set(ctx, path, value);
                if up {
                    self.current()
                } else {
                    None
                }
            }
            exprs => {
                let mut up = false;
                for expr in exprs {
                    up = expr.update(ctx, event).is_some() || up;
                }
                self.invalid.set(true);
                if up {
                    self.current()
                } else {
                    None
                }
            }
        }
    }
}

impl Store {
    fn queue(&self, v: Value) {
        self.queued.borrow_mut().push(v)
    }

    fn write(&self, dv: &Dval, v: Value) {
        dv.write(v);
    }

    fn set(
        &self,
        ctx: &ExecCtx<WidgetCtx, Target>,
        to: Option<Value>,
        val: Option<Value>,
    ) {
        match (pathname(&self.invalid, to), val) {
            (None, None) => (),
            (None, Some(v)) => match self.dv.borrow().as_ref() {
                None => self.queue(v),
                Some((_, dv)) => self.write(dv, v),
            },
            (Some(p), val) => {
                let path = Path::from(p);
                let dv = ctx.user.backend.subscriber.durable_subscribe(path.clone());
                dv.updates(
                    UpdatesFlags::BEGIN_WITH_LAST,
                    ctx.user.backend.updates.clone(),
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

    fn same_path(&self, new_path: &Option<Value>) -> bool {
        match (new_path.as_ref(), self.dv.borrow().as_ref()) {
            (Some(Value::String(p0)), Some((p1, _))) => &**p0 == &**p1,
            _ => false,
        }
    }
}

pub(crate) struct StoreVar {
    queued: RefCell<Vec<Value>>,
    name: RefCell<Option<Chars>>,
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
            if expr::VNAME.is_match(&n) {
                Some(n)
            } else {
                invalid.set(true);
                None
            }
        }
    }
}

impl Register<WidgetCtx, Target> for StoreVar {
    fn register(ctx: &ExecCtx<WidgetCtx, Target>) {
        let f: InitFn<WidgetCtx, Target> = Box::new(|ctx, from| {
            let t = StoreVar {
                queued: RefCell::new(Vec::new()),
                name: RefCell::new(None),
                invalid: Cell::new(false),
            };
            match from {
                [name, value] => t.set(ctx, name.current(), value.current()),
                _ => t.invalid.set(true),
            }
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("store_var".into(), f);
    }
}

impl Apply<WidgetCtx, Target> for StoreVar {
    fn current(&self) -> Option<Value> {
        if self.invalid.get() {
            Some(Value::Error(Chars::from(
                "store_var(name: string [a-z][a-z0-9_]+, value): expected 2 arguments",
            )))
        } else {
            None
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<WidgetCtx, Target>,
        from: &[Node<WidgetCtx, Target>],
        event: &Target,
    ) -> Option<Value> {
        match from {
            [name, val] => {
                let name = name.update(ctx, event);
                let value = val.update(ctx, event);
                let value = if name.is_some() && !self.same_name(&name) {
                    value.or_else(|| val.current())
                } else {
                    value
                };
                let up = value.is_some();
                self.set(ctx, name, value);
                if up {
                    self.current()
                } else {
                    None
                }
            }
            exprs => {
                let mut up = false;
                for expr in exprs {
                    up = expr.update(ctx, event).is_some() || up;
                }
                self.invalid.set(true);
                if up {
                    self.current()
                } else {
                    None
                }
            }
        }
    }
}

impl StoreVar {
    fn set_var(&self, ctx: &ExecCtx<WidgetCtx, Target>, name: Chars, v: Value) {
        ctx.variables.borrow_mut().insert(name.clone(), v.clone());
        let _: Result<_, _> =
            ctx.user.backend.to_gui.send(ToGui::UpdateVar(name.clone(), v));
    }

    fn queue_set(&self, v: Value) {
        self.queued.borrow_mut().push(v)
    }

    fn set(
        &self,
        ctx: &ExecCtx<WidgetCtx, Target>,
        name: Option<Value>,
        value: Option<Value>,
    ) {
        if let Some(name) = varname(&self.invalid, name) {
            for v in self.queued.borrow_mut().drain(..) {
                self.set_var(ctx, name.clone(), v)
            }
            *self.name.borrow_mut() = Some(name);
        }
        if let Some(value) = value {
            match self.name.borrow().as_ref() {
                None => self.queue_set(value),
                Some(name) => self.set_var(ctx, name.clone(), value),
            }
        }
    }

    fn same_name(&self, new_name: &Option<Value>) -> bool {
        match (new_name, self.name.borrow().as_ref()) {
            (Some(Value::String(n0)), Some(n1)) => n0 == n1,
            _ => false,
        }
    }
}

pub(crate) struct Load {
    cur: RefCell<Option<Dval>>,
    invalid: Cell<bool>,
}

impl Register<WidgetCtx, Target> for Load {
    fn register(ctx: &ExecCtx<WidgetCtx, Target>) {
        let f: InitFn<WidgetCtx, Target> = Box::new(|ctx, from| {
            let t = Load { cur: RefCell::new(None), invalid: Cell::new(false) };
            match from {
                [path] => t.subscribe(ctx, path.current()),
                _ => t.invalid.set(true),
            }
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("load".into(), f);
    }
}

impl Apply<WidgetCtx, Target> for Load {
    fn current(&self) -> Option<Value> {
        if self.invalid.get() {
            Load::err()
        } else {
            self.cur.borrow().as_ref().and_then(|dv| match dv.last() {
                subscriber::Event::Unsubscribed => None,
                subscriber::Event::Update(v) => Some(v),
            })
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<WidgetCtx, Target>,
        from: &[Node<WidgetCtx, Target>],
        event: &Target,
    ) -> Option<Value> {
        match from {
            [name] => {
                let target = name.update(ctx, event);
                let up = target.is_some();
                self.subscribe(ctx, target);
                if self.invalid.get() {
                    if up {
                        Load::err()
                    } else {
                        None
                    }
                } else {
                    self.cur.borrow().as_ref().and_then(|dv| match event {
                        Target::Variable(_, _) | Target::Rpc(_, _) | Target::Event(_) => {
                            None
                        }
                        Target::Netidx(id, value) if dv.id() == *id => {
                            Some(value.clone())
                        }
                        Target::Netidx(_, _) => None,
                    })
                }
            }
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(ctx, event).is_some() || up;
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

impl Load {
    fn subscribe(&self, ctx: &ExecCtx<WidgetCtx, Target>, name: Option<Value>) {
        if let Some(path) = pathname(&self.invalid, name) {
            let dv = ctx.user.backend.subscriber.durable_subscribe(path);
            dv.updates(UpdatesFlags::BEGIN_WITH_LAST, ctx.user.backend.updates.clone());
            *self.cur.borrow_mut() = Some(dv);
        }
    }

    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from(
            "load(expr: path) expected 1 absolute path as argument",
        )))
    }
}

pub(crate) struct LoadVar {
    name: RefCell<Option<Chars>>,
    ctx: ExecCtx<WidgetCtx, Target>,
    invalid: Cell<bool>,
}

impl Register<WidgetCtx, Target> for LoadVar {
    fn register(ctx: &ExecCtx<WidgetCtx, Target>) {
        let f: InitFn<WidgetCtx, Target> = Box::new(|ctx, from| {
            let t = LoadVar {
                name: RefCell::new(None),
                ctx: ctx.clone(),
                invalid: Cell::new(false),
            };
            match from {
                [name] => t.subscribe(name.current()),
                _ => t.invalid.set(true),
            }
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("load_var".into(), f);
    }
}

impl Apply<WidgetCtx, Target> for LoadVar {
    fn current(&self) -> Option<Value> {
        if self.invalid.get() {
            LoadVar::err()
        } else {
            self.name
                .borrow()
                .as_ref()
                .and_then(|n| self.ctx.variables.borrow().get(n).cloned())
        }
    }

    fn update(
        &self,
        ctx: &ExecCtx<WidgetCtx, Target>,
        from: &[Node<WidgetCtx, Target>],
        event: &Target,
    ) -> Option<Value> {
        match from {
            [name] => {
                let target = name.update(ctx, event);
                if self.invalid.get() {
                    if target.is_some() {
                        LoadVar::err()
                    } else {
                        None
                    }
                } else {
                    if let Some(target) = target {
                        self.subscribe(Some(target));
                    }
                    match (self.name.borrow().as_ref(), event) {
                        (None, _)
                        | (Some(_), Target::Netidx(_, _))
                        | (Some(_), Target::Event(_))
                        | (Some(_), Target::Rpc(_, _)) => None,
                        (Some(vn), Target::Variable(tn, v)) if &**vn == tn => {
                            Some(v.clone())
                        }
                        (Some(_), Target::Variable(_, _)) => None,
                    }
                }
            }
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(ctx, event).is_some() || up;
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

impl LoadVar {
    fn err() -> Option<Value> {
        Some(Value::Error(Chars::from(
            "load_var(expr: variable name): expected 1 variable name as argument",
        )))
    }

    fn subscribe(&self, name: Option<Value>) {
        if let Some(name) = varname(&self.invalid, name) {
            *self.name.borrow_mut() = Some(name);
        }
    }
}

enum ConfirmState {
    Empty,
    Invalid,
    Ready { message: Option<Value>, value: Value },
}

pub(crate) struct Confirm {
    ctx: ExecCtx<WidgetCtx, Target>,
    state: RefCell<ConfirmState>,
}

impl Register<WidgetCtx, Target> for Confirm {
    fn register(ctx: &ExecCtx<WidgetCtx, Target>) {
        let f: InitFn<WidgetCtx, Target> = Box::new(|ctx, from| {
            let t =
                Confirm { ctx: ctx.clone(), state: RefCell::new(ConfirmState::Empty) };
            match from {
                [msg, val] => {
                    if let Some(value) = val.current() {
                        *t.state.borrow_mut() =
                            ConfirmState::Ready { message: msg.current(), value };
                    }
                }
                [val] => {
                    if let Some(value) = val.current() {
                        *t.state.borrow_mut() =
                            ConfirmState::Ready { message: None, value };
                    }
                }
                _ => {
                    *t.state.borrow_mut() = ConfirmState::Invalid;
                }
            }
            Box::new(t)
        });
        ctx.functions.borrow_mut().insert("confirm".into(), f);
    }
}

impl Apply<WidgetCtx, Target> for Confirm {
    fn current(&self) -> Option<Value> {
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

    fn update(
        &self,
        ctx: &ExecCtx<WidgetCtx, Target>,
        from: &[Node<WidgetCtx, Target>],
        event: &Target,
    ) -> Option<Value> {
        match from {
            [msg, val] => {
                let m = msg.update(ctx, event).or_else(|| msg.current());
                let v = val.update(ctx, event);
                v.and_then(|v| if self.ask(m.as_ref(), &v) { Some(v) } else { None })
            }
            [val] => {
                let v = val.update(ctx, event);
                v.and_then(|v| if self.ask(None, &v) { Some(v) } else { None })
            }
            exprs => {
                let mut up = false;
                for expr in exprs {
                    up = expr.update(ctx, event).is_some() || up;
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

impl Confirm {
    fn usage() -> Option<Value> {
        Some(Value::Error(Chars::from("confirm([msg], val): expected 1 or 2 arguments")))
    }

    fn ask(&self, msg: Option<&Value>, val: &Value) -> bool {
        let default = Value::from("proceed with");
        let msg = msg.unwrap_or(&default);
        ask_modal(&self.ctx.user.window, &format!("{} {}?", msg, val))
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
            view::Expr { kind: view::ExprKind::Constant(v), id } => {
                ctx.dbg_ctx.borrow_mut().add_event(*id, v.clone());
                Expr::Constant(spec.clone(), v.clone())
            }
            view::Expr { kind: view::ExprKind::Apply { args, function }, .. } => {
                let args: Vec<Expr> = args
                    .iter()
                    .map(|spec| Expr::new(ctx, variables.clone(), spec.clone()))
                    .collect();
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
