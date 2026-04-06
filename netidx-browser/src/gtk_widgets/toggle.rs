use super::{CommonProps, CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

pub(crate) struct ToggleButtonW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::ToggleButton,
    common: CommonProps<X>,
    value: TRef<X, bool>,
    label: TRef<X, String>,
    on_change: Ref<X>,
    on_change_callable: Option<Callable<X>>,
    signal_id: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> ToggleButtonW<X> {
    pub(crate) async fn compile_toggle(
        ctx: CompileCtx<X>,
        source: Value,
    ) -> Result<GtkW<X>> {
        // Fields sorted: common, debug_highlight, image, label, on_change, value
        let [(_, common), (_, debug_highlight), (_, _image), (_, label), (_, on_change), (_, value)] =
            source.cast_to::<[(ArcStr, u64); 6]>().context("toggle flds")?;
        let common_props = CommonProps::compile(&ctx.gx, common, debug_highlight).await?;
        let (_image, label, on_change, value) = tokio::try_join! {
            ctx.gx.compile_ref(_image),
            ctx.gx.compile_ref(label),
            ctx.gx.compile_ref(on_change),
            ctx.gx.compile_ref(value),
        }?;
        let value: TRef<X, bool> =
            TRef::new(value).context("toggle tref value")?;
        let label: TRef<X, String> =
            TRef::new(label).context("toggle tref label")?;
        let on_change_callable =
            compile_callable!(ctx.gx, on_change, "toggle on_change");
        let widget = gtk::ToggleButton::new();
        if let Some(ref t) = label.t {
            widget.set_label(t);
        }
        if let Some(v) = value.t {
            widget.set_active(v);
        }
        let signal_id = connect_on_change(&widget, &ctx.gx, &on_change_callable);
        common_props.apply(&widget);
        widget.show();
        Ok(Box::new(ToggleButtonW {
            ctx,
            widget,
            common: common_props,
            value,
            label,
            on_change,
            on_change_callable,
            signal_id,
        }))
    }

    pub(crate) async fn compile_check(
        ctx: CompileCtx<X>,
        source: Value,
    ) -> Result<GtkW<X>> {
        // Fields sorted: common, debug_highlight, label, on_change, value
        let [(_, common), (_, debug_highlight), (_, label), (_, on_change), (_, value)] =
            source.cast_to::<[(ArcStr, u64); 5]>().context("check flds")?;
        let common_props = CommonProps::compile(&ctx.gx, common, debug_highlight).await?;
        let (label, on_change, value) = tokio::try_join! {
            ctx.gx.compile_ref(label),
            ctx.gx.compile_ref(on_change),
            ctx.gx.compile_ref(value),
        }?;
        let value: TRef<X, bool> =
            TRef::new(value).context("check tref value")?;
        let label: TRef<X, String> =
            TRef::new(label).context("check tref label")?;
        let on_change_callable =
            compile_callable!(ctx.gx, on_change, "check on_change");
        let check = gtk::CheckButton::with_label(
            label.t.as_deref().unwrap_or(""),
        );
        if let Some(v) = value.t {
            check.set_active(v);
        }
        // CheckButton is a subclass of ToggleButton
        let widget: gtk::ToggleButton = check.upcast();
        let signal_id = connect_on_change(&widget, &ctx.gx, &on_change_callable);
        common_props.apply(&widget);
        widget.show();
        Ok(Box::new(ToggleButtonW {
            ctx,
            widget,
            common: common_props,
            value,
            label,
            on_change,
            on_change_callable,
            signal_id,
        }))
    }
}

fn connect_on_change<X: GXExt>(
    widget: &gtk::ToggleButton,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_toggled(move |btn| {
        let args = ValArray::from_iter([Value::Bool(btn.is_active())]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("toggle on_change call failed: {}", e);
        }
    }))
}

impl<X: GXExt> GtkWidget<X> for ToggleButtonW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        changed |= self.common.handle_update(rt, id, v, &self.widget)?;
        if let Some(val) = self.value.update(id, v).context("toggle update value")? {
            if let Some(ref sig) = self.signal_id {
                self.widget.block_signal(sig);
            }
            self.widget.set_active(*val);
            if let Some(ref sig) = self.signal_id {
                self.widget.unblock_signal(sig);
            }
            changed = true;
        }
        if let Some(t) = self.label.update(id, v).context("toggle update label")? {
            self.widget.set_label(t);
            changed = true;
        }
        if id == self.on_change.id {
            self.on_change.last = Some(v.clone());
            self.on_change_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("toggle on_change recompile")?,
            );
            if let Some(sig) = self.signal_id.take() {
                self.widget.disconnect(sig);
            }
            self.signal_id =
                connect_on_change(&self.widget, &self.ctx.gx, &self.on_change_callable);
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}
