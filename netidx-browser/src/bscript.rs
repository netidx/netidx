use super::{util::ask_modal, ToGui, ViewLoc, WidgetCtx};
use glib::thread_guard::ThreadGuard;
use netidx::{chars::Chars, path::Path, resolver_client, subscriber::Value};
use netidx_bscript::vm::{self, Apply, Ctx, ExecCtx, InitFn, Node, Register};
use parking_lot::Mutex;
use std::{cell::RefCell, mem, rc::Rc, result::Result, sync::Arc};

#[derive(Debug)]
pub(crate) enum LocalEvent {
    Event(Value),
    TableResolved(Path, resolver_client::Table),
}

pub(crate) struct Event {
    cur: Option<Value>,
    invalid: bool,
}

impl Register<WidgetCtx, LocalEvent> for Event {
    fn register(ctx: &mut ExecCtx<WidgetCtx, LocalEvent>) {
        let f: InitFn<WidgetCtx, LocalEvent> = Arc::new(|_, from, _, _| {
            Box::new(Event { cur: None, invalid: from.len() > 0 })
        });
        ctx.functions.insert("event".into(), f);
        ctx.user.register_fn("event".into(), Path::root());
    }
}

impl Apply<WidgetCtx, LocalEvent> for Event {
    fn current(&self) -> Option<Value> {
        if self.invalid {
            Event::err()
        } else {
            self.cur.as_ref().cloned()
        }
    }

    fn update(
        &mut self,
        _ctx: &mut ExecCtx<WidgetCtx, LocalEvent>,
        from: &mut [Node<WidgetCtx, LocalEvent>],
        event: &vm::Event<LocalEvent>,
    ) -> Option<Value> {
        self.invalid = from.len() > 0;
        match event {
            vm::Event::Variable(_, _, _)
            | vm::Event::Netidx(_, _)
            | vm::Event::Rpc(_, _)
            | vm::Event::Timer(_)
            | vm::Event::User(LocalEvent::TableResolved(_, _)) => None,
            vm::Event::User(LocalEvent::Event(value)) => {
                self.cur = Some(value.clone());
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

pub(crate) struct CurrentPath(Mutex<ThreadGuard<Rc<RefCell<ViewLoc>>>>);

impl Register<WidgetCtx, LocalEvent> for CurrentPath {
    fn register(ctx: &mut ExecCtx<WidgetCtx, LocalEvent>) {
        let f: InitFn<WidgetCtx, LocalEvent> = Arc::new(|ctx, _, _, _| {
            Box::new(CurrentPath(Mutex::new(ThreadGuard::new(
                ctx.user.current_loc.clone(),
            ))))
        });
        ctx.functions.insert("current_path".into(), f);
        ctx.user.register_fn("current_path".into(), Path::root());
    }
}

impl Apply<WidgetCtx, LocalEvent> for CurrentPath {
    fn current(&self) -> Option<Value> {
        let inner = self.0.lock();
        let inner = inner.get_ref();
        let inner = inner.borrow();
        match &*inner {
            ViewLoc::File(_) => None,
            ViewLoc::Netidx(path) => {
                Some(Value::from(Chars::from(String::from(&**path))))
            }
        }
    }

    fn update(
        &mut self,
        _ctx: &mut ExecCtx<WidgetCtx, LocalEvent>,
        _from: &mut [Node<WidgetCtx, LocalEvent>],
        _event: &vm::Event<LocalEvent>,
    ) -> Option<Value> {
        None
    }
}

enum ConfirmState {
    Empty,
    Invalid,
    Ready { message: Option<Value>, value: Value },
}

pub(crate) struct ConfirmInner {
    window: gtk::ApplicationWindow,
    state: RefCell<ConfirmState>,
}

pub(crate) struct Confirm(Mutex<ThreadGuard<ConfirmInner>>);

impl Register<WidgetCtx, LocalEvent> for Confirm {
    fn register(ctx: &mut ExecCtx<WidgetCtx, LocalEvent>) {
        let f: InitFn<WidgetCtx, LocalEvent> = Arc::new(|ctx, from, _, _| {
            let mut state = ConfirmState::Empty;
            match from {
                [msg, val] => {
                    if let Some(value) = val.current() {
                        state = ConfirmState::Ready { message: msg.current(), value };
                    }
                }
                [val] => {
                    if let Some(value) = val.current() {
                        state = ConfirmState::Ready { message: None, value };
                    }
                }
                _ => {
                    state = ConfirmState::Invalid;
                }
            }
            Box::new(Confirm(Mutex::new(ThreadGuard::new(ConfirmInner {
                window: ctx.user.window.clone(),
                state: RefCell::new(state),
            }))))
        });
        ctx.functions.insert("confirm".into(), f);
        ctx.user.register_fn("confirm".into(), Path::root());
    }
}

impl Apply<WidgetCtx, LocalEvent> for Confirm {
    fn current(&self) -> Option<Value> {
        let inner = self.0.lock();
        let inner = inner.get_ref();
        let c = mem::replace(&mut *inner.state.borrow_mut(), ConfirmState::Empty);
        match c {
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
        &mut self,
        ctx: &mut ExecCtx<WidgetCtx, LocalEvent>,
        from: &mut [Node<WidgetCtx, LocalEvent>],
        event: &vm::Event<LocalEvent>,
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
        let inner = self.0.lock();
        let inner = inner.get_ref();
        ask_modal(&inner.window, &format!("{} {}?", msg, val))
    }
}

pub(crate) enum Navigate {
    Normal,
    Invalid,
}

impl Register<WidgetCtx, LocalEvent> for Navigate {
    fn register(ctx: &mut ExecCtx<WidgetCtx, LocalEvent>) {
        let f: InitFn<WidgetCtx, LocalEvent> = Arc::new(|ctx, from, _, _| {
            let mut t = Navigate::Normal;
            match from {
                [new_window, to] => t.navigate(ctx, new_window.current(), to.current()),
                [to] => t.navigate(ctx, None, to.current()),
                _ => t = Navigate::Invalid,
            }
            Box::new(t)
        });
        ctx.functions.insert("navigate".into(), f);
        ctx.user.register_fn("navigate".into(), Path::root());
    }
}

impl Apply<WidgetCtx, LocalEvent> for Navigate {
    fn current(&self) -> Option<Value> {
        match self {
            Navigate::Normal => None,
            Navigate::Invalid => Navigate::usage(),
        }
    }

    fn update(
        &mut self,
        ctx: &mut ExecCtx<WidgetCtx, LocalEvent>,
        from: &mut [Node<WidgetCtx, LocalEvent>],
        event: &vm::Event<LocalEvent>,
    ) -> Option<Value> {
        let up = match from {
            [new_window, to] => {
                let new_window =
                    new_window.update(ctx, event).or_else(|| new_window.current());
                let target = to.update(ctx, event);
                let up = target.is_some();
                self.navigate(ctx, new_window, target);
                up
            }
            [to] => {
                let target = to.update(ctx, event);
                let up = target.is_some();
                self.navigate(ctx, None, target);
                up
            }
            exprs => {
                let mut up = false;
                for e in exprs {
                    up = e.update(ctx, event).is_some() || up;
                }
                *self = Navigate::Invalid;
                up
            }
        };
        if up {
            self.current()
        } else {
            None
        }
    }
}

impl Navigate {
    fn navigate(
        &mut self,
        ctx: &ExecCtx<WidgetCtx, LocalEvent>,
        new_window: Option<Value>,
        to: Option<Value>,
    ) {
        if let Some(to) = to {
            let new_window =
                new_window.and_then(|v| v.cast_to::<bool>().ok()).unwrap_or(false);
            match to.cast_to::<Chars>() {
                Err(_) => *self = Navigate::Invalid,
                Ok(s) => match s.parse::<ViewLoc>() {
                    Err(()) => *self = Navigate::Invalid,
                    Ok(loc) => {
                        if new_window {
                            let m = ToGui::NavigateInWindow(loc);
                            let _: Result<_, _> = ctx.user.backend.to_gui.send(m);
                        } else {
                            let _: Result<_, _> =
                                ctx.user.backend.to_gui.send(ToGui::Navigate(loc));
                        }
                    }
                },
            }
        }
    }

    fn usage() -> Option<Value> {
        Some(Value::from("navigate([new_window], to): expected 1 or two arguments where to is e.g. /foo/bar, or netidx:/foo/bar, or, file:/path/to/view"))
    }
}

pub(crate) fn create_ctx(ctx: WidgetCtx) -> ExecCtx<WidgetCtx, LocalEvent> {
    let mut t = ExecCtx::new(ctx);
    Event::register(&mut t);
    CurrentPath::register(&mut t);
    Confirm::register(&mut t);
    Navigate::register(&mut t);
    t
}
