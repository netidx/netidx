//! Browser-specific graphix builtins: navigate, confirm
//!
//! These builtins communicate with the winit event loop through
//! an EventLoopProxy stored in LibState.

use crate::{BrowserEvent, ViewLoc};
use anyhow::Result;
use graphix_compiler::{
    expr::ExprId, typ::FnType, Apply, BindId, BuiltIn, Event, ExecCtx,
    Node, Rt, Scope,
};
use graphix_rt::{GXRt, NoExt};
use netidx::publisher::Value;
use winit::event_loop::EventLoopProxy;

type R = GXRt<NoExt>;
type E = graphix_compiler::NoUserEvent;

/// Shared state accessible by all browser builtins via LibState.
pub(crate) struct BrowserLibState {
    pub(crate) proxy: EventLoopProxy<BrowserEvent>,
}

// ---- navigate(path) ----

#[derive(Debug)]
struct NavigateInner {
    last: Option<Value>,
}

impl BuiltIn<R, E> for NavigateInner {
    const NAME: &str = "browser_navigate";
    const NEEDS_CALLSITE: bool = false;

    fn init<'a, 'b, 'c, 'd>(
        _ctx: &'a mut ExecCtx<R, E>,
        _typ: &'a FnType,
        _resolved: Option<&'d FnType>,
        _scope: &'b Scope,
        _from: &'c [Node<R, E>],
        _top_id: ExprId,
    ) -> Result<Box<dyn Apply<R, E>>> {
        Ok(Box::new(NavigateInner { last: None }))
    }
}

impl Apply<R, E> for NavigateInner {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<R, E>,
        from: &mut [Node<R, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        let v = from[0].update(ctx, event)?;
        if self.last.as_ref() == Some(&v) {
            return None;
        }
        self.last = Some(v.clone());
        if let Value::String(s) = &v {
            if let Ok(loc) = s.parse::<ViewLoc>() {
                if let Some(state) = ctx.libstate.get::<BrowserLibState>() {
                    let _ = state.proxy.send_event(BrowserEvent::Navigate(loc));
                }
            }
        }
        Some(Value::Null)
    }

    fn sleep(&mut self, _ctx: &mut ExecCtx<R, E>) {
        self.last = None;
    }
}

pub(crate) type Navigate = NavigateInner;

// ---- navigate_in_window(path) ----

#[derive(Debug)]
struct NavigateInWindowInner {
    last: Option<Value>,
}

impl BuiltIn<R, E> for NavigateInWindowInner {
    const NAME: &str = "browser_navigate_in_window";
    const NEEDS_CALLSITE: bool = false;

    fn init<'a, 'b, 'c, 'd>(
        _ctx: &'a mut ExecCtx<R, E>,
        _typ: &'a FnType,
        _resolved: Option<&'d FnType>,
        _scope: &'b Scope,
        _from: &'c [Node<R, E>],
        _top_id: ExprId,
    ) -> Result<Box<dyn Apply<R, E>>> {
        Ok(Box::new(NavigateInWindowInner { last: None }))
    }
}

impl Apply<R, E> for NavigateInWindowInner {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<R, E>,
        from: &mut [Node<R, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        let v = from[0].update(ctx, event)?;
        if self.last.as_ref() == Some(&v) {
            return None;
        }
        self.last = Some(v.clone());
        if let Value::String(s) = &v {
            if let Ok(loc) = s.parse::<ViewLoc>() {
                if let Some(state) = ctx.libstate.get::<BrowserLibState>() {
                    let _ = state.proxy.send_event(BrowserEvent::NavigateInWindow(loc));
                }
            }
        }
        Some(Value::Null)
    }

    fn sleep(&mut self, _ctx: &mut ExecCtx<R, E>) {
        self.last = None;
    }
}

pub(crate) type NavigateInWindow = NavigateInWindowInner;

// ---- confirm(message, value) ----
// Shows a confirmation dialog. Returns value if confirmed, null if cancelled.
// Uses spawn_var to avoid blocking the graphix runtime.

#[derive(Debug)]
struct ConfirmInner {
    result_bid: BindId,
    top_id: ExprId,
}

impl BuiltIn<R, E> for ConfirmInner {
    const NAME: &str = "browser_confirm";
    const NEEDS_CALLSITE: bool = false;

    fn init<'a, 'b, 'c, 'd>(
        ctx: &'a mut ExecCtx<R, E>,
        _typ: &'a FnType,
        _resolved: Option<&'d FnType>,
        _scope: &'b Scope,
        _from: &'c [Node<R, E>],
        top_id: ExprId,
    ) -> Result<Box<dyn Apply<R, E>>> {
        let result_bid = BindId::new();
        ctx.rt.ref_var(result_bid, top_id);
        Ok(Box::new(ConfirmInner { result_bid, top_id }))
    }
}

impl Apply<R, E> for ConfirmInner {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<R, E>,
        from: &mut [Node<R, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        // Check if our async task returned a result
        if let Some(v) = event.variables.remove(&self.result_bid) {
            return Some(v);
        }
        // Check if arguments updated
        let mut msg = None;
        let mut val = None;
        let mut any_updated = false;
        let n_args = from.len();
        for (i, n) in from.iter_mut().enumerate() {
            if let Some(v) = n.update(ctx, event) {
                any_updated = true;
                if n_args == 2 {
                    if i == 0 {
                        msg = Some(v.clone());
                    } else {
                        val = Some(v);
                    }
                } else {
                    val = Some(v);
                }
            }
        }
        if !any_updated {
            return None;
        }
        let value = match val {
            Some(v) => v,
            None => return None,
        };
        let message = match msg {
            Some(Value::String(s)) => s.to_string(),
            _ => format!("Proceed with {}?", value),
        };
        // Send confirm request via spawn_var
        if let Some(state) = ctx.libstate.get::<BrowserLibState>() {
            let proxy = state.proxy.clone();
            let bid = self.result_bid;
            let value_clone = value.clone();
            ctx.rt.spawn_var(async move {
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                let reply = std::sync::Arc::new(
                    std::sync::Mutex::new(Some(reply_tx)),
                );
                let m = BrowserEvent::Confirm { message, reply };
                if proxy.send_event(m).is_ok() {
                    match reply_rx.await {
                        Ok(true) => (bid, value_clone),
                        _ => (bid, Value::Null),
                    }
                } else {
                    (bid, Value::Null)
                }
            });
        }
        None
    }

    fn sleep(&mut self, _ctx: &mut ExecCtx<R, E>) {}

    fn delete(&mut self, ctx: &mut ExecCtx<R, E>) {
        ctx.rt.unref_var(self.result_bid, self.top_id);
    }
}

pub(crate) type Confirm = ConfirmInner;

/// Register all browser builtins with the execution context.
pub(crate) fn register_builtins(ctx: &mut ExecCtx<R, E>) -> Result<()> {
    ctx.register_builtin::<Navigate>()?;
    ctx.register_builtin::<NavigateInWindow>()?;
    ctx.register_builtin::<Confirm>()?;
    Ok(())
}
