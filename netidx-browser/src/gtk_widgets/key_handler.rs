use super::{compile, CommonProps, CompileCtx, EmptyW, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use glib::Propagation;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, Ref};
use gtk::prelude::*;
use netidx::protocol::valarray::ValArray;
use netidx::publisher::Value;

pub(crate) struct KeyHandlerW<X: GXExt> {
    ctx: CompileCtx<X>,
    event_box: gtk::EventBox,
    common: CommonProps<X>,
    child_ref: Ref<X>,
    child: GtkW<X>,
    on_key_press: Ref<X>,
    on_key_press_callable: Option<Callable<X>>,
}

impl<X: GXExt> KeyHandlerW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: child, common, debug_highlight, on_key_press
        let [(_, child), (_, common), (_, debug_highlight), (_, on_key_press)] =
            source.cast_to::<[(ArcStr, u64); 4]>().context("key_handler flds")?;
        let common_props = CommonProps::compile(&ctx.gx, common, debug_highlight).await?;
        let (child_ref, on_key_press) = tokio::try_join! {
            ctx.gx.compile_ref(child),
            ctx.gx.compile_ref(on_key_press),
        }?;
        let compiled_child = compile_child!(ctx, child_ref, "key_handler child");
        let callable = compile_callable!(ctx.gx, on_key_press, "key_handler on_key_press");
        let event_box = gtk::EventBox::new();
        event_box.set_can_focus(true);
        event_box.add(compiled_child.gtk_widget());
        // Wire up key press signal
        if let Some(ref c) = callable {
            let gx = ctx.gx.clone();
            let cid = c.id();
            event_box.connect_key_press_event(move |_, event| {
                let key_name = format_key_event(event);
                let args = ValArray::from([Value::String(ArcStr::from(key_name.as_str()))]);
                if let Err(e) = gx.call(cid, args) {
                    log::warn!("key_handler on_key_press call failed: {}", e);
                }
                Propagation::Stop
            });
        }
        common_props.apply(&event_box);
        event_box.show_all();
        Ok(Box::new(KeyHandlerW {
            ctx,
            event_box,
            common: common_props,
            child_ref,
            child: compiled_child,
            on_key_press,
            on_key_press_callable: callable,
        }))
    }
}

/// Format a GDK key event as a human-readable string with modifiers.
/// Examples: "a", "Return", "ctrl+s", "ctrl+shift+F5"
fn format_key_event(event: &gdk::EventKey) -> String {
    let mut parts = Vec::new();
    let state = event.state();
    if state.contains(gdk::ModifierType::CONTROL_MASK) {
        parts.push("ctrl");
    }
    if state.contains(gdk::ModifierType::MOD1_MASK) {
        parts.push("alt");
    }
    if state.contains(gdk::ModifierType::SHIFT_MASK) {
        parts.push("shift");
    }
    if state.contains(gdk::ModifierType::SUPER_MASK) {
        parts.push("super");
    }
    let keyval = event.keyval();
    if let Some(name) = keyval.name() {
        parts.push(&name);
        parts.join("+")
    } else {
        parts.push("?");
        parts.join("+")
    }
}

impl<X: GXExt> GtkWidget<X> for KeyHandlerW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        changed |= self.common.handle_update(rt, id, v, &self.event_box)?;
        // Update callback
        update_callable!(self, rt, id, v, on_key_press, on_key_press_callable, "key_handler on_key_press");
        // Update child
        update_child!(self, rt, id, v, changed, child_ref, child, "key_handler child");
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.event_box.upcast_ref()
    }
}
