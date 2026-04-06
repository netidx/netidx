use super::{CommonProps, CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static RADIO_GROUPS: RefCell<HashMap<String, gtk::RadioButton>> =
        RefCell::new(HashMap::new());
}

pub(crate) struct RadioButtonW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::RadioButton,
    common: CommonProps<X>,
    group: TRef<X, String>,
    label: TRef<X, String>,
    on_toggled: Ref<X>,
    on_toggled_callable: Option<Callable<X>>,
    value: TRef<X, bool>,
    signal_id: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> RadioButtonW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: common, debug_highlight, group, label, on_toggled, value
        let [(_, common), (_, debug_highlight), (_, group), (_, label), (_, on_toggled), (_, value)] =
            source.cast_to::<[(ArcStr, u64); 6]>().context("radio flds")?;
        let common_props = CommonProps::compile(&ctx.gx, common, debug_highlight).await?;
        let (group, label, on_toggled, value) = tokio::try_join! {
            ctx.gx.compile_ref(group),
            ctx.gx.compile_ref(label),
            ctx.gx.compile_ref(on_toggled),
            ctx.gx.compile_ref(value),
        }?;
        let group: TRef<X, String> =
            TRef::new(group).context("radio tref group")?;
        let label: TRef<X, String> =
            TRef::new(label).context("radio tref label")?;
        let value: TRef<X, bool> =
            TRef::new(value).context("radio tref value")?;
        let on_toggled_callable =
            compile_callable!(ctx.gx, on_toggled, "radio on_toggled");
        let widget = gtk::RadioButton::with_label(
            label.t.as_deref().unwrap_or(""),
        );
        // Join the radio group if one is specified.
        let group_name = group.t.as_deref().unwrap_or("");
        if !group_name.is_empty() {
            RADIO_GROUPS.with(|groups| {
                let mut groups = groups.borrow_mut();
                if let Some(first) = groups.get(group_name) {
                    widget.join_group(Some(first));
                } else {
                    groups.insert(group_name.to_string(), widget.clone());
                }
            });
        }
        if let Some(v) = value.t {
            widget.set_active(v);
        }
        let signal_id = connect_on_toggled(&widget, &ctx.gx, &on_toggled_callable);
        common_props.apply(&widget);
        widget.show();
        Ok(Box::new(RadioButtonW {
            ctx,
            widget,
            common: common_props,
            group,
            label,
            on_toggled,
            on_toggled_callable,
            value,
            signal_id,
        }))
    }
}

fn connect_on_toggled<X: GXExt>(
    widget: &gtk::RadioButton,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_toggled(move |btn| {
        let args = ValArray::from_iter([Value::Bool(btn.is_active())]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("radio on_toggled call failed: {}", e);
        }
    }))
}

impl<X: GXExt> GtkWidget<X> for RadioButtonW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        changed |= self.common.handle_update(rt, id, v, &self.widget)?;
        if let Some(val) = self.value.update(id, v).context("radio update value")? {
            if let Some(ref sig) = self.signal_id {
                self.widget.block_signal(sig);
            }
            self.widget.set_active(*val);
            if let Some(ref sig) = self.signal_id {
                self.widget.unblock_signal(sig);
            }
            changed = true;
        }
        if let Some(t) = self.label.update(id, v).context("radio update label")? {
            self.widget.set_label(t);
            changed = true;
        }
        if id == self.on_toggled.id {
            self.on_toggled.last = Some(v.clone());
            self.on_toggled_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("radio on_toggled recompile")?,
            );
            if let Some(sig) = self.signal_id.take() {
                self.widget.disconnect(sig);
            }
            self.signal_id =
                connect_on_toggled(&self.widget, &self.ctx.gx, &self.on_toggled_callable);
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}
