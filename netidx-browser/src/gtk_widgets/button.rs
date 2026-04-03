use super::{CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

pub(crate) struct ButtonW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::Button,
    label: TRef<X, String>,
    on_click: Ref<X>,
    on_click_callable: Option<Callable<X>>,
    signal_id: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> ButtonW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: image, label, on_click
        let [(_, image), (_, label), (_, on_click)] =
            source.cast_to::<[(ArcStr, u64); 3]>().context("button flds")?;
        let (_image, label, on_click) = tokio::try_join! {
            ctx.gx.compile_ref(image),
            ctx.gx.compile_ref(label),
            ctx.gx.compile_ref(on_click),
        }?;
        let label: TRef<X, String> =
            TRef::new(label).context("button tref label")?;
        let on_click_callable = compile_callable!(ctx.gx, on_click, "button on_click");
        let widget = gtk::Button::new();
        if let Some(ref t) = label.t {
            widget.set_label(t);
        }
        let signal_id = connect_on_click(&widget, &ctx.gx, &on_click_callable);
        widget.show();
        Ok(Box::new(ButtonW {
            ctx,
            widget,
            label,
            on_click,
            on_click_callable,
            signal_id,
        }))
    }
}

fn connect_on_click<X: GXExt>(
    widget: &gtk::Button,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_clicked(move |_| {
        if let Err(e) = gx.call(id, ValArray::from_iter([Value::Null])) {
            log::warn!("button on_click call failed: {}", e);
        }
    }))
}

impl<X: GXExt> GtkWidget<X> for ButtonW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if let Some(t) = self.label.update(id, v).context("button update label")? {
            self.widget.set_label(t);
            changed = true;
        }
        if id == self.on_click.id {
            self.on_click.last = Some(v.clone());
            self.on_click_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("button on_click recompile")?,
            );
            // Reconnect signal handler with new callable
            if let Some(sig) = self.signal_id.take() {
                self.widget.disconnect(sig);
            }
            self.signal_id =
                connect_on_click(&self.widget, &self.ctx.gx, &self.on_click_callable);
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}
