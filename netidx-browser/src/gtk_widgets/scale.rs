use super::{CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

pub(crate) struct ScaleW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::Scale,
    value: TRef<X, f64>,
    min: TRef<X, f64>,
    max: TRef<X, f64>,
    on_change: Ref<X>,
    on_change_callable: Option<Callable<X>>,
    signal_id: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> ScaleW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: max, min, on_change, value
        let [(_, max), (_, min), (_, on_change), (_, value)] =
            source.cast_to::<[(ArcStr, u64); 4]>().context("scale flds")?;
        let (max, min, on_change, value) = tokio::try_join! {
            ctx.gx.compile_ref(max),
            ctx.gx.compile_ref(min),
            ctx.gx.compile_ref(on_change),
            ctx.gx.compile_ref(value),
        }?;
        let value: TRef<X, f64> =
            TRef::new(value).context("scale tref value")?;
        let min: TRef<X, f64> =
            TRef::new(min).context("scale tref min")?;
        let max: TRef<X, f64> =
            TRef::new(max).context("scale tref max")?;
        let on_change_callable =
            compile_callable!(ctx.gx, on_change, "scale on_change");
        let min_val = min.t.unwrap_or(0.0);
        let max_val = max.t.unwrap_or(1.0);
        let widget = gtk::Scale::with_range(
            gtk::Orientation::Horizontal,
            min_val,
            max_val,
            (max_val - min_val) / 100.0,
        );
        if let Some(v) = value.t {
            widget.set_value(v);
        }
        let signal_id = connect_on_change(&widget, &ctx.gx, &on_change_callable);
        widget.show();
        Ok(Box::new(ScaleW {
            ctx,
            widget,
            value,
            min,
            max,
            on_change,
            on_change_callable,
            signal_id,
        }))
    }
}

fn connect_on_change<X: GXExt>(
    widget: &gtk::Scale,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_value_changed(move |scale| {
        let args = ValArray::from_iter([Value::F64(scale.value())]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("scale on_change call failed: {}", e);
        }
    }))
}

impl<X: GXExt> GtkWidget<X> for ScaleW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if let Some(val) = self.value.update(id, v).context("scale update value")? {
            if let Some(ref sig) = self.signal_id {
                self.widget.block_signal(sig);
            }
            self.widget.set_value(*val);
            if let Some(ref sig) = self.signal_id {
                self.widget.unblock_signal(sig);
            }
            changed = true;
        }
        if let Some(mn) = self.min.update(id, v).context("scale update min")? {
            let mx = self.max.t.unwrap_or(1.0);
            self.widget.set_range(*mn, mx);
            changed = true;
        }
        if let Some(mx) = self.max.update(id, v).context("scale update max")? {
            let mn = self.min.t.unwrap_or(0.0);
            self.widget.set_range(mn, *mx);
            changed = true;
        }
        if id == self.on_change.id {
            self.on_change.last = Some(v.clone());
            self.on_change_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("scale on_change recompile")?,
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
