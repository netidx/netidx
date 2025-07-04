use super::{into_borrowed_line, GuiW, GuiWidget, LineV, StyleV, TRef};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{expr::ExprId, rt::{BSHandle, Ref}};
use ratatui::{
    layout::Rect,
    symbols,
    widgets::LineGauge,
    Frame,
};
use tokio::try_join;

#[derive(Clone, Copy)]
struct LineSetV(symbols::line::Set);

impl FromValue for LineSetV {
    fn from_value(v: Value) -> Result<Self> {
        let s = match &*v.cast_to::<ArcStr>()? {
            "Normal" => symbols::line::NORMAL,
            "Rounded" => symbols::line::ROUNDED,
            "Double" => symbols::line::DOUBLE,
            "Thick" => symbols::line::THICK,
            s => bail!("invalid line set {s}"),
        };
        Ok(Self(s))
    }
}

pub(super) struct LineGaugeW {
    filled_style: TRef<Option<StyleV>>,
    label: TRef<Option<LineV>>,
    line_set: TRef<Option<LineSetV>>,
    ratio: TRef<f64>,
    style: TRef<Option<StyleV>>,
    unfilled_style: TRef<Option<StyleV>>,
}

impl LineGaugeW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [(_, filled_style), (_, label), (_, line_set), (_, ratio), (_, style), (_, unfilled_style)] =
            v.cast_to::<[(ArcStr, u64); 6]>()?;
        let (filled_style, label, line_set, ratio, style, unfilled_style) = try_join! {
            bs.compile_ref(filled_style),
            bs.compile_ref(label),
            bs.compile_ref(line_set),
            bs.compile_ref(ratio),
            bs.compile_ref(style),
            bs.compile_ref(unfilled_style)
        }?;
        Ok(Box::new(Self {
            filled_style: TRef::new(filled_style).context("line_gauge tref filled_style")?,
            label: TRef::new(label).context("line_gauge tref label")?,
            line_set: TRef::new(line_set).context("line_gauge tref line_set")?,
            ratio: TRef::new(ratio).context("line_gauge tref ratio")?,
            style: TRef::new(style).context("line_gauge tref style")?,
            unfilled_style: TRef::new(unfilled_style).context("line_gauge tref unfilled_style")?,
        }))
    }
}

#[async_trait]
impl GuiWidget for LineGaugeW {
    async fn handle_event(&mut self, _e: Event) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        self.filled_style
            .update(id, &v)
            .context("line_gauge update filled_style")?;
        self.label
            .update(id, &v)
            .context("line_gauge update label")?;
        self.line_set
            .update(id, &v)
            .context("line_gauge update line_set")?;
        self.ratio.update(id, &v).context("line_gauge update ratio")?;
        self.style.update(id, &v).context("line_gauge update style")?;
        self.unfilled_style
            .update(id, &v)
            .context("line_gauge update unfilled_style")?;
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let mut g = LineGauge::default().ratio(self.ratio.t.unwrap_or(0.0));
        if let Some(Some(LineV(l))) = &self.label.t {
            g = g.label(into_borrowed_line(l));
        }
        if let Some(Some(ls)) = self.line_set.t {
            g = g.line_set(ls.0);
        }
        if let Some(Some(s)) = &self.style.t {
            g = g.style(s.0);
        }
        if let Some(Some(s)) = &self.filled_style.t {
            g = g.filled_style(s.0);
        }
        if let Some(Some(s)) = &self.unfilled_style.t {
            g = g.unfilled_style(s.0);
        }
        frame.render_widget(g, rect);
        Ok(())
    }
}
