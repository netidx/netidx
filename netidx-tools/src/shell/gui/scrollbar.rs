use super::{compile, EmptyW, GuiW, GuiWidget, StyleV, TRef};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::Event;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{
    layout::Rect,
    widgets::{Scrollbar, ScrollbarOrientation, ScrollbarState},
    Frame,
};
use tokio::try_join;

#[derive(Clone)]
struct ScrollbarOrientationV(ScrollbarOrientation);

impl FromValue for ScrollbarOrientationV {
    fn from_value(v: Value) -> Result<Self> {
        let v = match &*v.cast_to::<ArcStr>()? {
            "VerticalRight" => ScrollbarOrientation::VerticalRight,
            "VerticalLeft" => ScrollbarOrientation::VerticalLeft,
            "HorizontalBottom" => ScrollbarOrientation::HorizontalBottom,
            "HorizontalTop" => ScrollbarOrientation::HorizontalTop,
            s => bail!("invalid ScrollBarOrientation {s}"),
        };
        Ok(Self(v))
    }
}

pub(super) struct ScrollbarW {
    bs: BSHandle,
    begin_style: TRef<Option<StyleV>>,
    begin_symbol: TRef<Option<ArcStr>>,
    child: GuiW,
    child_ref: Ref,
    content_length: TRef<Option<usize>>,
    viewport_length: TRef<Option<usize>>,
    end_style: TRef<Option<StyleV>>,
    end_symbol: TRef<Option<ArcStr>>,
    orientation: TRef<Option<ScrollbarOrientationV>>,
    position: TRef<Option<u16>>,
    style: TRef<Option<StyleV>>,
    thumb_style: TRef<Option<StyleV>>,
    thumb_symbol: TRef<Option<ArcStr>>,
    track_style: TRef<Option<StyleV>>,
    track_symbol: TRef<Option<ArcStr>>,
    state: ScrollbarState,
}

impl ScrollbarW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [(_, begin_style), (_, begin_symbol), (_, child), (_, content_length), (_, end_style), (_, end_symbol), (_, orientation), (_, position), (_, style), (_, thumb_style), (_, thumb_symbol), (_, track_style), (_, track_symbol), (_, viewport_length)] =
            v.cast_to::<[(ArcStr, u64); 14]>().context("scrollbar flds")?;
        let (
            begin_style,
            begin_symbol,
            mut child_ref,
            content_length,
            end_style,
            end_symbol,
            orientation,
            position,
            style,
            thumb_style,
            thumb_symbol,
            track_style,
            track_symbol,
            viewport_length,
        ) = try_join! {
            bs.compile_ref(begin_style),
            bs.compile_ref(begin_symbol),
            bs.compile_ref(child),
            bs.compile_ref(content_length),
            bs.compile_ref(end_style),
            bs.compile_ref(end_symbol),
            bs.compile_ref(orientation),
            bs.compile_ref(position),
            bs.compile_ref(style),
            bs.compile_ref(thumb_style),
            bs.compile_ref(thumb_symbol),
            bs.compile_ref(track_style),
            bs.compile_ref(track_symbol),
            bs.compile_ref(viewport_length)
        }?;
        let begin_style = TRef::<Option<StyleV>>::new(begin_style)
            .context("scrollbar tref begin_style")?;
        let begin_symbol = TRef::<Option<ArcStr>>::new(begin_symbol)
            .context("scrollbar tref begin_symbol")?;
        let child = match child_ref.last.take() {
            Some(v) => compile(bs.clone(), v).await?,
            None => Box::new(EmptyW),
        };
        let content_length = TRef::<Option<usize>>::new(content_length)
            .context("scrollbar tref content_length")?;
        let end_style =
            TRef::<Option<StyleV>>::new(end_style).context("scrollbar tref end_style")?;
        let end_symbol = TRef::<Option<ArcStr>>::new(end_symbol)
            .context("scrollbar tref end_symbol")?;
        let orientation = TRef::<Option<ScrollbarOrientationV>>::new(orientation)
            .context("scrollbar tref orientation")?;
        let position =
            TRef::<Option<u16>>::new(position).context("scrollbar tref position")?;
        let style = TRef::<Option<StyleV>>::new(style).context("scrollbar tref style")?;
        let thumb_style = TRef::<Option<StyleV>>::new(thumb_style)
            .context("scrollbar tref thumb_style")?;
        let thumb_symbol = TRef::<Option<ArcStr>>::new(thumb_symbol)
            .context("scrollbar tref thumb_symbol")?;
        let track_style = TRef::<Option<StyleV>>::new(track_style)
            .context("scrollbar tref track_style")?;
        let track_symbol = TRef::<Option<ArcStr>>::new(track_symbol)
            .context("scrollbar tref track_symbol")?;
        let viewport_length = TRef::<Option<usize>>::new(viewport_length)
            .context("scrollbar tref viewport_length")?;
        let state = ScrollbarState::new(content_length.t.and_then(|t| t).unwrap_or(50));
        Ok(Box::new(Self {
            begin_style,
            begin_symbol,
            child_ref,
            child,
            content_length,
            end_style,
            end_symbol,
            orientation,
            position,
            style,
            thumb_symbol,
            thumb_style,
            bs,
            track_style,
            track_symbol,
            viewport_length,
            state,
        }))
    }
}

#[async_trait]
impl GuiWidget for ScrollbarW {
    async fn handle_event(&mut self, e: Event) -> Result<()> {
        self.child.handle_event(e).await
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            bs,
            begin_style,
            begin_symbol,
            child,
            child_ref,
            content_length,
            end_style,
            end_symbol,
            orientation,
            position,
            style,
            thumb_style,
            thumb_symbol,
            track_style,
            track_symbol,
            viewport_length,
            state: _,
        } = self;
        begin_style.update(id, &v).context("scrollbar update begin_style")?;
        begin_symbol.update(id, &v).context("scrollbar update begin_symbol")?;
        if child_ref.id == id {
            *child = compile(bs.clone(), v.clone()).await?;
        }
        end_style.update(id, &v).context("scrollbar update end_style")?;
        end_symbol.update(id, &v).context("scrollbar update end_symbol")?;
        orientation.update(id, &v).context("scrollbar update orientation")?;
        dbg!(position.update(id, &v).context("scrollbar update position"))?;
        style.update(id, &v).context("scrollbar update style")?;
        thumb_style.update(id, &v).context("scrollbar update thumb_style")?;
        thumb_symbol.update(id, &v).context("scrollbar update thumb_symbol")?;
        track_style.update(id, &v).context("scrollbar update track_style")?;
        track_symbol.update(id, &v).context("scrollbar update track_symbol")?;
        content_length.update(id, &v).context("scrollbar update content_length")?;
        viewport_length.update(id, &v).context("scrollbar update viewport_length")?;
        child.handle_update(id, v).await
    }

    fn draw(&mut self, frame: &mut Frame, mut rect: Rect) -> Result<()> {
        let Self {
            bs: _,
            begin_style,
            begin_symbol,
            child,
            child_ref: _,
            content_length,
            end_style,
            end_symbol,
            orientation,
            position,
            style,
            thumb_style,
            thumb_symbol,
            track_style,
            track_symbol,
            viewport_length,
            state,
        } = self;
        let orientation = orientation
            .t
            .as_ref()
            .and_then(|t| t.as_ref().map(|t| t.0.clone()))
            .unwrap_or(ScrollbarOrientation::VerticalRight);
        let mut bar = Scrollbar::new(orientation.clone());
        if let Some(Some(s)) = begin_style.t {
            bar = bar.begin_style(s.0);
        }
        if let Some(s) = &begin_symbol.t {
            bar = bar.begin_symbol(s.as_ref().map(|s| s.as_str()));
        }
        if let Some(Some(s)) = end_style.t {
            bar = bar.end_style(s.0);
        }
        if let Some(s) = &end_symbol.t {
            bar = bar.end_symbol(s.as_ref().map(|s| s.as_str()));
        }
        if let Some(Some(p)) = position.t {
            *state = state.position(p as usize);
        }
        if let Some(Some(s)) = style.t {
            bar = bar.style(s.0);
        }
        if let Some(Some(s)) = thumb_style.t {
            bar = bar.thumb_style(s.0);
        }
        if let Some(Some(s)) = &thumb_symbol.t {
            bar = bar.thumb_symbol(s);
        }
        if let Some(Some(s)) = track_style.t {
            bar = bar.track_style(s.0);
        }
        if let Some(s) = &track_symbol.t {
            bar = bar.track_symbol(s.as_ref().map(|s| s.as_str()));
        }
        if let Some(Some(l)) = content_length.t.take() {
            *state = state.content_length(l);
        }
        if let Some(Some(l)) = viewport_length.t.take() {
            *state = state.viewport_content_length(l);
        }
        frame.render_stateful_widget(bar, rect, state);
        match orientation {
            ScrollbarOrientation::HorizontalBottom => {
                if rect.height > 0 {
                    rect.height -= 1
                }
            }
            ScrollbarOrientation::HorizontalTop => {
                if rect.height > 0 && rect.y < u16::MAX {
                    rect.height -= 1;
                    rect.y += 1
                }
            }
            ScrollbarOrientation::VerticalLeft => {
                if rect.width > 0 && rect.x < u16::MAX {
                    rect.width -= 1;
                    rect.x += 1
                }
            }
            ScrollbarOrientation::VerticalRight => {
                if rect.width > 0 {
                    rect.width -= 1
                }
            }
        };
        child.draw(frame, rect)
    }
}
