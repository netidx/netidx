use super::{CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, GXHandle, Ref, TRef};
use gtk::prelude::*;
use netidx::{protocol::valarray::ValArray, publisher::Value};

pub(crate) struct ComboBoxW<X: GXExt> {
    ctx: CompileCtx<X>,
    widget: gtk::ComboBoxText,
    choices: Ref<X>,
    selected: TRef<X, String>,
    on_change: Ref<X>,
    on_change_callable: Option<Callable<X>>,
    signal_id: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> ComboBoxW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: choices, on_change, selected
        let [(_, choices), (_, on_change), (_, selected)] =
            source.cast_to::<[(ArcStr, u64); 3]>().context("combo flds")?;
        let (choices, on_change, selected) = tokio::try_join! {
            ctx.gx.compile_ref(choices),
            ctx.gx.compile_ref(on_change),
            ctx.gx.compile_ref(selected),
        }?;
        let selected: TRef<X, String> =
            TRef::new(selected).context("combo tref selected")?;
        let on_change_callable =
            compile_callable!(ctx.gx, on_change, "combo on_change");
        let widget = gtk::ComboBoxText::new();
        if let Some(ref v) = choices.last {
            rebuild_choices(&widget, v);
        }
        if let Some(ref s) = selected.t {
            widget.set_active_id(Some(s.as_str()));
        }
        let signal_id = connect_on_change(&widget, &ctx.gx, &on_change_callable);
        widget.show();
        Ok(Box::new(ComboBoxW {
            ctx,
            widget,
            choices,
            selected,
            on_change,
            on_change_callable,
            signal_id,
        }))
    }
}

/// Rebuild the combo box entries from a Value.
/// Expects an array of {id: string, label: string} structs,
/// which arrive as [(id_name, id_val), (label_name, label_val)].
fn rebuild_choices(widget: &gtk::ComboBoxText, v: &Value) {
    widget.remove_all();
    if let Ok(items) = v.clone().cast_to::<Vec<Value>>() {
        for item in items {
            // Each ComboChoice is a struct {id: string, label: string}
            // Fields sorted: id, label
            if let Ok([(_, id_val), (_, label_val)]) =
                item.cast_to::<[(ArcStr, Value); 2]>()
            {
                let id = id_val.cast_to::<String>().unwrap_or_default();
                let label = label_val.cast_to::<String>().unwrap_or_default();
                widget.append(Some(&id), &label);
            }
        }
    }
}

fn connect_on_change<X: GXExt>(
    widget: &gtk::ComboBoxText,
    gx: &GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    Some(widget.connect_changed(move |combo| {
        if let Some(active_id) = combo.active_id() {
            let args = ValArray::from_iter([
                Value::String(active_id.to_string().into()),
            ]);
            if let Err(e) = gx.call(id, args) {
                log::warn!("combo on_change call failed: {}", e);
            }
        }
    }))
}

impl<X: GXExt> GtkWidget<X> for ComboBoxW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if id == self.choices.id {
            self.choices.last = Some(v.clone());
            if let Some(ref sig) = self.signal_id {
                self.widget.block_signal(sig);
            }
            rebuild_choices(&self.widget, v);
            // Restore selection after rebuilding
            if let Some(ref s) = self.selected.t {
                self.widget.set_active_id(Some(s.as_str()));
            }
            if let Some(ref sig) = self.signal_id {
                self.widget.unblock_signal(sig);
            }
            changed = true;
        }
        if let Some(s) = self
            .selected
            .update(id, v)
            .context("combo update selected")?
        {
            if let Some(ref sig) = self.signal_id {
                self.widget.block_signal(sig);
            }
            self.widget.set_active_id(Some(s.as_str()));
            if let Some(ref sig) = self.signal_id {
                self.widget.unblock_signal(sig);
            }
            changed = true;
        }
        if id == self.on_change.id {
            self.on_change.last = Some(v.clone());
            self.on_change_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("combo on_change recompile")?,
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
