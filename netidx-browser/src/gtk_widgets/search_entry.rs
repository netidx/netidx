use super::{CommonProps, CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

pub(crate) struct SearchEntryW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::SearchEntry,
    common: CommonProps<X>,
    text: TRef<X, String>,
    on_search_changed: Ref<X>,
    on_search_changed_callable: Option<Callable<X>>,
    on_activate: Ref<X>,
    on_activate_callable: Option<Callable<X>>,
    search_signal: Option<glib::SignalHandlerId>,
    activate_signal: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> SearchEntryW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: common, debug_highlight, on_activate, on_search_changed, text
        let [(_, common), (_, debug_highlight), (_, on_activate), (_, on_search_changed), (_, text)] =
            source.cast_to::<[(ArcStr, u64); 5]>().context("search_entry flds")?;
        let common_props = CommonProps::compile(&ctx.gx, common, debug_highlight).await?;
        let (on_activate, on_search_changed, text) = tokio::try_join! {
            ctx.gx.compile_ref(on_activate),
            ctx.gx.compile_ref(on_search_changed),
            ctx.gx.compile_ref(text),
        }?;
        let text: TRef<X, String> =
            TRef::new(text).context("search_entry tref text")?;
        let on_search_changed_callable =
            compile_callable!(ctx.gx, on_search_changed, "search_entry on_search_changed");
        let on_activate_callable =
            compile_callable!(ctx.gx, on_activate, "search_entry on_activate");
        let widget = gtk::SearchEntry::new();
        if let Some(ref t) = text.t {
            widget.set_text(t);
        }
        let search_signal =
            connect_on_search_changed(&widget, &ctx.gx, &on_search_changed_callable);
        let activate_signal =
            connect_on_activate(&widget, &ctx.gx, &on_activate_callable);
        common_props.apply(&widget);
        widget.show();
        Ok(Box::new(SearchEntryW {
            ctx,
            widget,
            common: common_props,
            text,
            on_search_changed,
            on_search_changed_callable,
            on_activate,
            on_activate_callable,
            search_signal,
            activate_signal,
        }))
    }
}

fn connect_on_search_changed<X: GXExt>(
    widget: &gtk::SearchEntry,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_search_changed(move |entry| {
        let text = entry.text().to_string();
        let args = ValArray::from_iter([Value::String(text.into())]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("search_entry on_search_changed call failed: {}", e);
        }
    }))
}

fn connect_on_activate<X: GXExt>(
    widget: &gtk::SearchEntry,
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
            log::warn!("search_entry on_activate call failed: {}", e);
        }
    }))
}

impl<X: GXExt> GtkWidget<X> for SearchEntryW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        changed |= self.common.handle_update(rt, id, v, &self.widget)?;
        if let Some(t) = self.text.update(id, v).context("search_entry update text")? {
            // Block search_changed while setting text programmatically
            if let Some(ref sig) = self.search_signal {
                self.widget.block_signal(sig);
            }
            self.widget.set_text(t);
            if let Some(ref sig) = self.search_signal {
                self.widget.unblock_signal(sig);
            }
            changed = true;
        }
        if id == self.on_search_changed.id {
            self.on_search_changed.last = Some(v.clone());
            self.on_search_changed_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("search_entry on_search_changed recompile")?,
            );
            if let Some(sig) = self.search_signal.take() {
                self.widget.disconnect(sig);
            }
            self.search_signal = connect_on_search_changed(
                &self.widget,
                &self.ctx.gx,
                &self.on_search_changed_callable,
            );
            changed = true;
        }
        if id == self.on_activate.id {
            self.on_activate.last = Some(v.clone());
            self.on_activate_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("search_entry on_activate recompile")?,
            );
            if let Some(sig) = self.activate_signal.take() {
                self.widget.disconnect(sig);
            }
            self.activate_signal = connect_on_activate(
                &self.widget,
                &self.ctx.gx,
                &self.on_activate_callable,
            );
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}
