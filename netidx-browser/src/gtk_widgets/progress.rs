use super::{CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{GXExt, Ref, TRef};
use gtk::prelude::*;
use netidx::publisher::Value;

fn parse_ellipsize_mode(s: &str) -> pango::EllipsizeMode {
    match s {
        "Start" => pango::EllipsizeMode::Start,
        "Middle" => pango::EllipsizeMode::Middle,
        "End" => pango::EllipsizeMode::End,
        _ => pango::EllipsizeMode::None,
    }
}

pub(crate) struct ProgressBarW<X: GXExt> {
    widget: gtk::ProgressBar,
    ellipsize: TRef<X, String>,
    fraction: TRef<X, Option<f64>>,
    pulse: Ref<X>,
    show_text: TRef<X, bool>,
    text: TRef<X, Option<String>>,
}

impl<X: GXExt> ProgressBarW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields sorted: ellipsize, fraction, pulse, show_text, text
        let [(_, ellipsize), (_, fraction), (_, pulse), (_, show_text), (_, text)] =
            source.cast_to::<[(ArcStr, u64); 5]>().context("progress flds")?;
        let (ellipsize, fraction, pulse, show_text, text) = tokio::try_join! {
            ctx.gx.compile_ref(ellipsize),
            ctx.gx.compile_ref(fraction),
            ctx.gx.compile_ref(pulse),
            ctx.gx.compile_ref(show_text),
            ctx.gx.compile_ref(text),
        }?;
        let ellipsize: TRef<X, String> =
            TRef::new(ellipsize).context("progress tref ellipsize")?;
        let fraction: TRef<X, Option<f64>> =
            TRef::new(fraction).context("progress tref fraction")?;
        let show_text: TRef<X, bool> =
            TRef::new(show_text).context("progress tref show_text")?;
        let text: TRef<X, Option<String>> =
            TRef::new(text).context("progress tref text")?;
        let widget = gtk::ProgressBar::new();
        if let Some(Some(f)) = fraction.t {
            widget.set_fraction(f);
        }
        widget.set_show_text(show_text.t.unwrap_or(false));
        widget.set_ellipsize(parse_ellipsize_mode(
            ellipsize.t.as_deref().unwrap_or("None"),
        ));
        if let Some(Some(ref t)) = text.t {
            widget.set_text(Some(t.as_str()));
            if !show_text.t.unwrap_or(false) {
                widget.set_show_text(true);
            }
        }
        widget.show();
        Ok(Box::new(ProgressBarW { widget, ellipsize, fraction, pulse, show_text, text }))
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
        if let Some(e) = self
            .ellipsize
            .update(id, v)
            .context("progress update ellipsize")?
        {
            self.widget.set_ellipsize(parse_ellipsize_mode(e));
            changed = true;
        }
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
        if let Some(st) = self
            .show_text
            .update(id, v)
            .context("progress update show_text")?
        {
            self.widget.set_show_text(*st);
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
                }
                None => {
                    self.widget.set_text(None);
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
