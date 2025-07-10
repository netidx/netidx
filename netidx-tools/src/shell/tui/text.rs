use super::{AlignmentV, LinesV, StyleV, TRef, TuiW, TuiWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use netidx::publisher::Value;
use netidx_bscript::{expr::ExprId, rt::BSHandle};
use ratatui::{layout::Rect, style::Style, text::Text, Frame};
use std::mem;
use tokio::try_join;

pub(super) struct TextW {
    alignment: TRef<Option<AlignmentV>>,
    lines: TRef<LinesV>,
    style: TRef<StyleV>,
    text: Text<'static>,
}

impl TextW {
    pub(super) async fn compile(bs: BSHandle, source: Value) -> Result<TuiW> {
        let [(_, alignment), (_, lines), (_, style)] =
            source.cast_to::<[(ArcStr, u64); 3]>().context("text flds")?;
        let (alignment, lines, style) = try_join! {
            bs.compile_ref(alignment),
            bs.compile_ref(lines),
            bs.compile_ref(style)
        }?;
        let alignment =
            TRef::<Option<AlignmentV>>::new(alignment).context("text tref alignment")?;
        let mut lines = TRef::<LinesV>::new(lines).context("text tref lines")?;
        let style = TRef::<StyleV>::new(style).context("text tref style")?;
        let text = Text {
            alignment: alignment.t.as_ref().and_then(|a| a.map(|a| a.0)),
            style: style.t.as_ref().map(|s| s.0).unwrap_or(Style::new()),
            lines: lines.t.take().map(|l| l.0).unwrap_or(vec![]),
        };
        Ok(Box::new(Self { alignment, lines, style, text }))
    }
}

#[async_trait]
impl TuiWidget for TextW {
    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        frame.render_widget(&self.text, rect);
        Ok(())
    }

    async fn handle_event(&mut self, _: Event, _: Value) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self { alignment, lines, style, text } = self;
        if let Some(a) = alignment.update(id, &v).context("text update alignment")? {
            text.alignment = a.map(|a| a.0);
        }
        if let Some(l) = lines.update(id, &v).context("text update lines")? {
            text.lines = mem::take(&mut l.0);
        }
        if let Some(s) = style.update(id, &v).context("text update style")? {
            text.style = s.0;
        }
        Ok(())
    }
}
