use super::{CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{GXExt, Ref, TRef};
use gtk::prelude::*;
use netidx::publisher::Value;

pub(crate) struct ProgressBarW<X: GXExt> {
    widget: gtk::ProgressBar,
    fraction: TRef<X, Option<f64>>,
    pulse: Ref<X>,
    text: TRef<X, Option<String>>,
}

impl<X: GXExt> ProgressBarW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: fraction, pulse, text
        let [(_, fraction), (_, pulse), (_, text)] =
            source.cast_to::<[(ArcStr, u64); 3]>().context("progress flds")?;
        let (fraction, pulse, text) = tokio::try_join! {
            ctx.gx.compile_ref(fraction),
            ctx.gx.compile_ref(pulse),
            ctx.gx.compile_ref(text),
        }?;
        let fraction: TRef<X, Option<f64>> =
            TRef::new(fraction).context("progress tref fraction")?;
        let text: TRef<X, Option<String>> =
            TRef::new(text).context("progress tref text")?;
        let widget = gtk::ProgressBar::new();
        if let Some(Some(f)) = fraction.t {
            widget.set_fraction(f);
        }
        if let Some(Some(ref t)) = text.t {
            widget.set_text(Some(t.as_str()));
            widget.set_show_text(true);
        }
        widget.show();
        Ok(Box::new(ProgressBarW { widget, fraction, pulse, text }))
    }
}

impl<X: GXExt> GtkWidget<X> for ProgressBarW<X> {
    fn handle_update(
        &mut self,
        _rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        if let Some(f) = self
            .fraction
            .update(id, v)
            .context("progress update fraction")?
        {
            match f {
                Some(f) => self.widget.set_fraction(*f),
                None => self.widget.set_fraction(0.0),
            }
            changed = true;
        }
        if let Some(t) = self
            .text
            .update(id, v)
            .context("progress update text")?
        {
            match t {
                Some(t) => {
                    self.widget.set_text(Some(t.as_str()));
                    self.widget.set_show_text(true);
                }
                None => {
                    self.widget.set_text(None);
                    self.widget.set_show_text(false);
                }
            }
            changed = true;
        }
        if self.pulse.update(id, v).is_some() {
            self.widget.pulse();
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}
