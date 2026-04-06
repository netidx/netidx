use super::{CommonProps, CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

pub(crate) struct SwitchW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::Switch,
    common: CommonProps<X>,
    value: TRef<X, bool>,
    on_change: Ref<X>,
    on_change_callable: Option<Callable<X>>,
    signal_id: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> SwitchW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: common, debug_highlight, on_change, value
        let [(_, common), (_, debug_highlight), (_, on_change), (_, value)] =
            source.cast_to::<[(ArcStr, u64); 4]>().context("switch flds")?;
        let common_props = CommonProps::compile(&ctx.gx, common, debug_highlight).await?;
        let (on_change, value) = tokio::try_join! {
            ctx.gx.compile_ref(on_change),
            ctx.gx.compile_ref(value),
        }?;
        let value: TRef<X, bool> =
            TRef::new(value).context("switch tref value")?;
        let on_change_callable =
            compile_callable!(ctx.gx, on_change, "switch on_change");
        let widget = gtk::Switch::new();
        if let Some(v) = value.t {
            widget.set_active(v);
        }
        let signal_id = connect_on_change(&widget, &ctx.gx, &on_change_callable);
        common_props.apply(&widget);
        widget.show();
        Ok(Box::new(SwitchW {
            ctx,
            widget,
            common: common_props,
            value,
            on_change,
            on_change_callable,
            signal_id,
        }))
    }
}

fn connect_on_change<X: GXExt>(
    widget: &gtk::Switch,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_state_set(move |_, state| {
        let args = ValArray::from_iter([Value::Bool(state)]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("switch on_change call failed: {}", e);
        }
        glib::Propagation::Proceed
    }))
}

impl<X: GXExt> GtkWidget<X> for SwitchW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        changed |= self.common.handle_update(rt, id, v, &self.widget)?;
        if let Some(val) = self.value.update(id, v).context("switch update value")? {
            if let Some(ref sig) = self.signal_id {
                self.widget.block_signal(sig);
            }
            self.widget.set_active(*val);
            if let Some(ref sig) = self.signal_id {
                self.widget.unblock_signal(sig);
            }
            changed = true;
        }
        if id == self.on_change.id {
            self.on_change.last = Some(v.clone());
            self.on_change_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("switch on_change recompile")?,
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
