//! Browser-specific graphix builtins: navigate, confirm, poll
//!
//! These builtins communicate with the GTK thread through channels
//! stored in LibState.

use crate::{ToGui, ViewLoc};
use anyhow::Result;
use graphix_compiler::{
    expr::ExprId,
    typ::FnType,
    Apply, BuiltIn, Event, ExecCtx, Node, Scope,
};
use graphix_rt::{GXRt, NoExt};
use netidx::publisher::Value;
type R = GXRt<NoExt>;
type E = graphix_compiler::NoUserEvent;

/// Shared state accessible by all browser builtins via LibState.
/// Set up during backend context creation, before builtins are invoked.
pub(crate) struct BrowserLibState {
    pub(crate) to_gui: glib::Sender<ToGui>,
}

// ---- navigate(path) ----
// When path updates, navigate the browser to that location.

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
        // Avoid navigating if value hasn't changed
        if self.last.as_ref() == Some(&v) {
            return None;
        }
        self.last = Some(v.clone());
        if let Value::String(s) = &v {
            if let Ok(loc) = s.parse::<ViewLoc>() {
                if let Some(state) = ctx.libstate.get::<BrowserLibState>() {
                    let _ = state.to_gui.send(ToGui::Navigate(loc));
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
// Open a new browser window at the given location.

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
                    let _ = state.to_gui.send(ToGui::NavigateInWindow(loc));
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
// Show a confirmation dialog. Returns value if confirmed, null if cancelled.
// This uses a synchronous channel back to the GTK thread because
// confirmation is inherently blocking.

#[derive(Debug)]
struct ConfirmInner {
    last_value: Option<Value>,
}

impl BuiltIn<R, E> for ConfirmInner {
    const NAME: &str = "browser_confirm";
    const NEEDS_CALLSITE: bool = false;

    fn init<'a, 'b, 'c, 'd>(
        _ctx: &'a mut ExecCtx<R, E>,
        _typ: &'a FnType,
        _resolved: Option<&'d FnType>,
        _scope: &'b Scope,
        _from: &'c [Node<R, E>],
        _top_id: ExprId,
    ) -> Result<Box<dyn Apply<R, E>>> {
        Ok(Box::new(ConfirmInner { last_value: None }))
    }
}

impl Apply<R, E> for ConfirmInner {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<R, E>,
        from: &mut [Node<R, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        // confirm takes 1 or 2 args: confirm(value) or confirm(message, value)
        // Track the last argument value
        let mut any_updated = false;
        for n in from.iter_mut() {
            if let Some(v) = n.update(ctx, event) {
                self.last_value = Some(v);
                any_updated = true;
            }
        }
        if !any_updated {
            return None;
        }
        // For now, just pass through the value without showing a dialog.
        // TODO: implement async confirm dialog via GTK thread roundtrip
        self.last_value.clone()
    }

    fn sleep(&mut self, _ctx: &mut ExecCtx<R, E>) {
        self.last_value = None;
    }
}

pub(crate) type Confirm = ConfirmInner;

/// Register all browser builtins with the execution context.
pub(crate) fn register_builtins(
    ctx: &mut ExecCtx<R, E>,
) -> Result<()> {
    ctx.register_builtin::<Navigate>()?;
    ctx.register_builtin::<NavigateInWindow>()?;
    ctx.register_builtin::<Confirm>()?;
    Ok(())
}
