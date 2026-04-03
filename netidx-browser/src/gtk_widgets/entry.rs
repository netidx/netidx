use super::{CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

pub(crate) struct EntryW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::Entry,
    text: TRef<X, String>,
    on_change: Ref<X>,
    on_change_callable: Option<Callable<X>>,
    on_activate: Ref<X>,
    on_activate_callable: Option<Callable<X>>,
    change_signal: Option<glib::SignalHandlerId>,
    activate_signal: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> EntryW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: on_activate, on_change, text
        let [(_, on_activate), (_, on_change), (_, text)] =
            source.cast_to::<[(ArcStr, u64); 3]>().context("entry flds")?;
        let (on_activate, on_change, text) = tokio::try_join! {
            ctx.gx.compile_ref(on_activate),
            ctx.gx.compile_ref(on_change),
            ctx.gx.compile_ref(text),
        }?;
        let text: TRef<X, String> =
            TRef::new(text).context("entry tref text")?;
        let on_change_callable =
            compile_callable!(ctx.gx, on_change, "entry on_change");
        let on_activate_callable =
            compile_callable!(ctx.gx, on_activate, "entry on_activate");
        let widget = gtk::Entry::new();
        if let Some(ref t) = text.t {
            widget.set_text(t);
        }
        let change_signal =
            connect_on_change(&widget, &ctx.gx, &on_change_callable);
        let activate_signal =
            connect_on_activate(&widget, &ctx.gx, &on_activate_callable);
        widget.show();
        Ok(Box::new(EntryW {
            ctx,
            widget,
            text,
            on_change,
            on_change_callable,
            on_activate,
            on_activate_callable,
            change_signal,
            activate_signal,
        }))
    }
}

fn connect_on_change<X: GXExt>(
    widget: &gtk::Entry,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_changed(move |entry| {
        let text = entry.text().to_string();
        let args = ValArray::from_iter([Value::String(text.into())]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("entry on_change call failed: {}", e);
        }
    }))
}

fn connect_on_activate<X: GXExt>(
    widget: &gtk::Entry,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_activate(move |entry| {
        let text = entry.text().to_string();
        let args = ValArray::from_iter([Value::String(text.into())]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("entry on_activate call failed: {}", e);
        }
    }))
}

impl<X: GXExt> GtkWidget<X> for EntryW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if let Some(t) = self.text.update(id, v).context("entry update text")? {
            // Block the on_change signal while we set text programmatically,
            // so we don't fire a callback for our own update.
            if let Some(ref sig) = self.change_signal {
                self.widget.block_signal(sig);
            }
            self.widget.set_text(t);
            if let Some(ref sig) = self.change_signal {
                self.widget.unblock_signal(sig);
            }
            changed = true;
        }
        if id == self.on_change.id {
            self.on_change.last = Some(v.clone());
            self.on_change_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("entry on_change recompile")?,
            );
            if let Some(sig) = self.change_signal.take() {
                self.widget.disconnect(sig);
            }
            self.change_signal =
                connect_on_change(&self.widget, &self.ctx.gx, &self.on_change_callable);
            changed = true;
        }
        if id == self.on_activate.id {
            self.on_activate.last = Some(v.clone());
            self.on_activate_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("entry on_activate recompile")?,
            );
            if let Some(sig) = self.activate_signal.take() {
                self.widget.disconnect(sig);
            }
            self.activate_signal =
                connect_on_activate(&self.widget, &self.ctx.gx, &self.on_activate_callable);
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}
