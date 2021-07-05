use super::{util::ask_modal, ToGui, ViewLoc, WidgetCtx};
use netidx::{chars::Chars, path::Path, resolver, subscriber::Value};
use netidx_bscript::vm::{self, Apply, ExecCtx, InitFn, Node, Register};
use parking_lot::Mutex;
use std::{
    mem,
    result::Result,
};

pub(crate) enum LocalEvent {
    Event(Value),
    TableResolved(Path, resolver::Table),
}

pub(crate) struct Event {
    cur: Option<Value>,
    invalid: bool,
}

impl Register<WidgetCtx, LocalEvent> for Event {
    fn register(ctx: &ExecCtx<WidgetCtx, LocalEvent>) {
        let f: InitFn<WidgetCtx, LocalEvent> =
            Box::new(|_, from| Box::new(Event { cur: None, invalid: from.len() > 0 }));
        ctx.functions.write().insert("event".into(), f);
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
        _ctx: &ExecCtx<WidgetCtx, LocalEvent>,
        from: &mut [Node<WidgetCtx, LocalEvent>],
        event: &vm::Event<LocalEvent>,
    ) -> Option<Value> {
        self.invalid = from.len() > 0;
        match event {
            vm::Event::Variable(_, _)
            | vm::Event::Netidx(_, _)
            | vm::Event::Rpc(_, _)
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

enum ConfirmState {
    Empty,
    Invalid,
    Ready { message: Option<Value>, value: Value },
}

pub(crate) struct Confirm {
    ctx: ExecCtx<WidgetCtx, LocalEvent>,
    state: Mutex<ConfirmState>,
}

impl Register<WidgetCtx, LocalEvent> for Confirm {
    fn register(ctx: &ExecCtx<WidgetCtx, LocalEvent>) {
        let f: InitFn<WidgetCtx, LocalEvent> = Box::new(|ctx, from| {
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
            Box::new(Confirm { ctx: ctx.clone(), state: Mutex::new(state) })
        });
        ctx.functions.write().insert("confirm".into(), f);
    }
}

impl Apply<WidgetCtx, LocalEvent> for Confirm {
    fn current(&self) -> Option<Value> {
        match mem::replace(&mut *self.state.lock(), ConfirmState::Empty) {
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
        ctx: &ExecCtx<WidgetCtx, LocalEvent>,
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
        ask_modal(&self.ctx.user.window, &format!("{} {}?", msg, val))
    }
}

pub(crate) enum Navigate {
    Normal,
    Invalid,
}

impl Register<WidgetCtx, LocalEvent> for Navigate {
    fn register(ctx: &ExecCtx<WidgetCtx, LocalEvent>) {
        let f: InitFn<WidgetCtx, LocalEvent> = Box::new(|ctx, from| {
            let mut t = Navigate::Normal;
            match from {
                [new_window, to] => t.navigate(ctx, new_window.current(), to.current()),
                [to] => t.navigate(ctx, None, to.current()),
                _ => t = Navigate::Invalid,
            }
            Box::new(t)
        });
        ctx.functions.write().insert("navigate".into(), f);
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
        ctx: &ExecCtx<WidgetCtx, LocalEvent>,
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
                            let _: Result<_, _> = ctx
                                .user
                                .backend
                                .to_gui
                                .send(ToGui::NavigateInWindow(loc));
                        } else {
                            if ctx.user.view_saved.get()
                                || ask_modal(
                                    &ctx.user.window,
                                    "Unsaved view will be lost.",
                                )
                            {
                                ctx.user.backend.navigate(loc);
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

pub(crate) fn create_ctx(ctx: WidgetCtx) -> ExecCtx<WidgetCtx, LocalEvent> {
    let t = ExecCtx::new(ctx);
    Event::register(&t);
    Confirm::register(&t);
    Navigate::register(&t);
    t
}
