use super::{CommonProps, CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use graphix_compiler::expr::ExprId;
use graphix_rt::{GXExt, TRef};
use gtk::prelude::*;
use netidx::publisher::Value;

pub(crate) struct LabelW<X: GXExt> {
    widget: gtk::Label,
    common: CommonProps<X>,
    text: TRef<X, String>,
    width: TRef<X, Option<i64>>,
    ellipsize: TRef<X, String>,
    selectable: TRef<X, bool>,
    single_line: TRef<X, bool>,
}

impl<X: GXExt> LabelW<X> {
    pub(crate) async fn compile(ctx: CompileCtx<X>, source: Value) -> Result<GtkW<X>> {
        // Fields arrive in alphabetical order:
        // common, debug_highlight, ellipsize, selectable, single_line, text, width
        let [(_, common), (_, debug_highlight), (_, ellipsize), (_, selectable), (_, single_line), (_, text), (_, width)] =
            source.cast_to::<[(ArcStr, u64); 7]>().context("label flds")?;
        let common_props = CommonProps::compile(&ctx.gx, common, debug_highlight).await?;
        let (ellipsize, selectable, single_line, text, width) = tokio::try_join! {
            ctx.gx.compile_ref(ellipsize),
            ctx.gx.compile_ref(selectable),
            ctx.gx.compile_ref(single_line),
            ctx.gx.compile_ref(text),
            ctx.gx.compile_ref(width),
        }?;
        let text: TRef<X, String> =
            TRef::new(text).context("label tref text")?;
        let width: TRef<X, Option<i64>> =
            TRef::new(width).context("label tref width")?;
        let ellipsize: TRef<X, String> =
            TRef::new(ellipsize).context("label tref ellipsize")?;
        let selectable: TRef<X, bool> =
            TRef::new(selectable).context("label tref selectable")?;
        let single_line: TRef<X, bool> =
            TRef::new(single_line).context("label tref single_line")?;
        let widget = gtk::Label::new(None);
        if let Some(ref t) = text.t {
            widget.set_text(t);
        }
        if let Some(Some(w)) = width.t {
            widget.set_width_chars(w as i32);
        }
        widget.set_ellipsize(parse_ellipsize_mode(
            ellipsize.t.as_deref().unwrap_or("None"),
        ));
        widget.set_selectable(selectable.t.unwrap_or(false));
        widget.set_single_line_mode(single_line.t.unwrap_or(false));
        if single_line.t.unwrap_or(false) {
            widget.set_lines(1);
        }
        common_props.apply(&widget);
        widget.show();
        Ok(Box::new(LabelW { widget, common: common_props, text, width, ellipsize, selectable, single_line }))
    }
}

fn parse_ellipsize_mode(s: &str) -> pango::EllipsizeMode {
    match s {
        "Start" => pango::EllipsizeMode::Start,
        "Middle" => pango::EllipsizeMode::Middle,
        "End" => pango::EllipsizeMode::End,
        _ => pango::EllipsizeMode::None,
    }
}

impl<X: GXExt> GtkWidget<X> for LabelW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        changed |= self.common.handle_update(rt, id, v, &self.widget)?;
        if let Some(t) = self.text.update(id, v).context("label update text")? {
            self.widget.set_text(t);
            changed = true;
        }
        if let Some(w) = self.width.update(id, v).context("label update width")? {
            match w {
                Some(w) => self.widget.set_width_chars(*w as i32),
                None => self.widget.set_width_chars(-1),
            }
            changed = true;
        }
        if let Some(e) = self
            .ellipsize
            .update(id, v)
            .context("label update ellipsize")?
        {
            self.widget.set_ellipsize(parse_ellipsize_mode(e));
            changed = true;
        }
        if let Some(s) = self
            .selectable
            .update(id, v)
            .context("label update selectable")?
        {
            self.widget.set_selectable(*s);
            changed = true;
        }
        if let Some(sl) = self
            .single_line
            .update(id, v)
            .context("label update single_line")?
        {
            self.widget.set_single_line_mode(*sl);
            if *sl {
                self.widget.set_lines(1);
            }
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.widget.upcast_ref()
    }
}
