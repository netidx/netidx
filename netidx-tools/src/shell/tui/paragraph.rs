use super::{
    into_borrowed_lines, AlignmentV, LinesV, ScrollV, StyleV, TRef, TuiW, TuiWidget,
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use netidx::publisher::Value;
use netidx_bscript::{expr::ExprId, rt::BSHandle};
use ratatui::{
    layout::Rect,
    widgets::{Paragraph, Wrap},
    Frame,
};
use tokio::try_join;

pub(super) struct ParagraphW {
    alignment: TRef<Option<AlignmentV>>,
    lines: TRef<LinesV>,
    scroll: TRef<ScrollV>,
    style: TRef<StyleV>,
    trim: TRef<bool>,
}

impl ParagraphW {
    pub(super) async fn compile(bs: BSHandle, source: Value) -> Result<TuiW> {
        let [(_, alignment), (_, lines), (_, scroll), (_, style), (_, trim)] =
            source.cast_to::<[(ArcStr, u64); 5]>().context("paragraph flds")?;
        let (alignment, lines, scroll, style, trim) = try_join! {
            bs.compile_ref(alignment),
            bs.compile_ref(lines),
            bs.compile_ref(scroll),
            bs.compile_ref(style),
            bs.compile_ref(trim)
        }?;
        let alignment: TRef<Option<AlignmentV>> =
            TRef::new(alignment).context("paragraph tref alignment")?;
        let lines: TRef<LinesV> = TRef::new(lines).context("paragraph tref lines")?;
        let scroll: TRef<ScrollV> = TRef::new(scroll).context("paragraph tref scroll")?;
        let style: TRef<StyleV> = TRef::new(style).context("paragraph tref style")?;
        let trim: TRef<bool> = TRef::new(trim).context("paragraph tref trim")?;
        Ok(Box::new(Self { alignment, lines, scroll, style, trim }))
    }
}

#[async_trait]
impl TuiWidget for ParagraphW {
    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let lines = self.lines.t.as_ref().map(|l| &l.0[..]).unwrap_or(&[]);
        let mut p = Paragraph::new(into_borrowed_lines(lines));
        if let Some(Some(a)) = self.alignment.t {
            p = p.alignment(a.0);
        }
        if let Some(s) = self.style.t {
            p = p.style(s.0);
        }
        if let Some(trim) = self.trim.t {
            p = p.wrap(Wrap { trim });
        }
        if let Some(s) = self.scroll.t {
            p = p.scroll(s.0)
        }
        frame.render_widget(p, rect);
        Ok(())
    }

    async fn handle_event(&mut self, _: Event, _: Value) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self { alignment, lines, scroll, style, trim } = self;
        alignment.update(id, &v).context("paragraph update alignment")?;
        lines.update(id, &v).context("paragraph update lines")?;
        scroll.update(id, &v).context("paragraph update scroll")?;
        style.update(id, &v).context("paragraph update style")?;
        trim.update(id, &v).context("paragraph update trim")?;
        Ok(())
    }
}
