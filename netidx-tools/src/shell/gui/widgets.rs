use super::{
    compile, into_borrowed_line, into_borrowed_lines, AlignmentV, BorderTypeV, BordersV,
    ConstraintV, DirectionV, FlexV, GuiW, GuiWidget, LineV, LinesV, PaddingV, PositionV,
    ScrollV, ScrollbarOrientationV, SpacingV, StyleV, TRef,
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::{Event, KeyCode};
use futures::future;
use log::debug;
use netidx::publisher::Value;
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::Style,
    text::Text,
    widgets::{Block, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap},
    Frame,
};
use smallvec::SmallVec;
use std::mem;
use tokio::try_join;

pub(super) struct EmptyW;

#[async_trait]
impl GuiWidget for EmptyW {
    async fn handle_event(&mut self, _e: Event) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, _id: ExprId, _v: Value) -> Result<()> {
        Ok(())
    }

    fn draw(&mut self, _frame: &mut Frame, _rect: Rect) -> Result<()> {
        Ok(())
    }
}

pub(super) struct TextW {
    alignment: TRef<Option<AlignmentV>>,
    lines: TRef<LinesV>,
    style: TRef<StyleV>,
    text: Text<'static>,
}

impl TextW {
    pub(super) async fn compile(bs: BSHandle, source: Value) -> Result<GuiW> {
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
impl GuiWidget for TextW {
    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        frame.render_widget(&self.text, rect);
        Ok(())
    }

    async fn handle_event(&mut self, _: Event) -> Result<()> {
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

pub(super) struct ParagraphW {
    alignment: TRef<Option<AlignmentV>>,
    lines: TRef<LinesV>,
    scroll: TRef<ScrollV>,
    style: TRef<StyleV>,
    trim: TRef<bool>,
}

impl ParagraphW {
    pub(super) async fn compile(bs: BSHandle, source: Value) -> Result<GuiW> {
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
impl GuiWidget for ParagraphW {
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

    async fn handle_event(&mut self, _: Event) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self { alignment, lines, scroll, style, trim } = self;
        alignment.update(id, &v).context("paragraph update alignment")?;
        lines.update(id, &v).context("paragraph update lines")?;
        debug!("scroll: {:?}", scroll.update(id, &v).context("paragraph update scroll")?);
        style.update(id, &v).context("paragraph update style")?;
        trim.update(id, &v).context("paragraph update trim")?;
        Ok(())
    }
}

pub(super) struct BlockW {
    bs: BSHandle,
    border: TRef<Option<BordersV>>,
    border_style: TRef<Option<StyleV>>,
    border_type: TRef<Option<BorderTypeV>>,
    child_ref: Ref,
    child: GuiW,
    padding: TRef<Option<PaddingV>>,
    style: TRef<Option<StyleV>>,
    title: TRef<Option<LineV>>,
    title_alignment: TRef<Option<AlignmentV>>,
    title_bottom: TRef<Option<LineV>>,
    title_position: TRef<Option<PositionV>>,
    title_style: TRef<Option<StyleV>>,
    title_top: TRef<Option<LineV>>,
}

impl BlockW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [(_, border), (_, border_style), (_, border_type), (_, child), (_, padding), (_, style), (_, title), (_, title_alignment), (_, title_bottom), (_, title_position), (_, title_style), (_, title_top)] =
            v.cast_to::<[(ArcStr, u64); 12]>().context("block flds")?;
        let (
            border,
            border_style,
            border_type,
            mut child_ref,
            padding,
            style,
            title,
            title_alignment,
            title_bottom,
            title_position,
            title_style,
            title_top,
        ) = try_join! {
            bs.compile_ref(border),
            bs.compile_ref(border_style),
            bs.compile_ref(border_type),
            bs.compile_ref(child),
            bs.compile_ref(padding),
            bs.compile_ref(style),
            bs.compile_ref(title),
            bs.compile_ref(title_alignment),
            bs.compile_ref(title_bottom),
            bs.compile_ref(title_position),
            bs.compile_ref(title_style),
            bs.compile_ref(title_top),
        }?;
        let border =
            TRef::<Option<BordersV>>::new(border).context("block tref border")?;
        let border_style = TRef::<Option<StyleV>>::new(border_style)
            .context("block tref border_style")?;
        let border_type = TRef::<Option<BorderTypeV>>::new(border_type)
            .context("block tref border_type")?;
        let padding =
            TRef::<Option<PaddingV>>::new(padding).context("block tref padding")?;
        let style = TRef::<Option<StyleV>>::new(style).context("block tref style")?;
        let title = TRef::<Option<LineV>>::new(title).context("block tref title")?;
        let title_alignment = TRef::<Option<AlignmentV>>::new(title_alignment)
            .context("block tref title_alignment")?;
        let title_bottom = TRef::<Option<LineV>>::new(title_bottom)
            .context("block tref title_bottom")?;
        let title_position = TRef::<Option<PositionV>>::new(title_position)
            .context("block tref title_position")?;
        let title_style =
            TRef::<Option<StyleV>>::new(title_style).context("block tref title_style")?;
        let title_top =
            TRef::<Option<LineV>>::new(title_top).context("block tref title_top")?;
        let child = match child_ref.last.take() {
            None => Box::new(EmptyW),
            Some(v) => compile(bs.clone(), v).await.context("block compile child")?,
        };
        let t = Self {
            bs,
            border,
            border_style,
            border_type,
            padding,
            style,
            title,
            title_alignment,
            title_bottom,
            title_position,
            title_style,
            title_top,
            child_ref,
            child,
        };
        Ok(Box::new(t))
    }
}

#[async_trait]
impl GuiWidget for BlockW {
    async fn handle_event(&mut self, e: Event) -> Result<()> {
        self.child.handle_event(e).await
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            bs,
            border,
            border_style,
            border_type,
            child_ref,
            child,
            padding,
            style,
            title,
            title_alignment,
            title_bottom,
            title_position,
            title_style,
            title_top,
        } = self;
        border.update(id, &v).context("block border update")?;
        border_style.update(id, &v).context("block border_style update")?;
        border_type.update(id, &v).context("block border_type update")?;
        padding.update(id, &v).context("block padding update")?;
        style.update(id, &v).context("block style update")?;
        title.update(id, &v).context("block title update")?;
        title_alignment.update(id, &v).context("block title_alignment update")?;
        title_bottom.update(id, &v).context("block title_bottom update")?;
        title_position.update(id, &v).context("block title_position update")?;
        title_style.update(id, &v).context("block title_style update")?;
        title_top.update(id, &v).context("block title_top update")?;
        if id == child_ref.id {
            *child =
                compile(bs.clone(), v.clone()).await.context("block child compile")?;
        }
        child.handle_update(id, v).await?;
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let Self {
            bs: _,
            border,
            border_style,
            border_type,
            child_ref: _,
            child,
            padding,
            style,
            title,
            title_alignment,
            title_bottom,
            title_position,
            title_style,
            title_top,
        } = self;
        let mut block = Block::new();
        if let Some(Some(b)) = border.t {
            block = block.borders(b.0);
        }
        if let Some(Some(s)) = border_style.t {
            block = block.border_style(s.0);
        }
        if let Some(Some(t)) = border_type.t {
            block = block.border_type(t.0);
        }
        if let Some(Some(p)) = padding.t {
            block = block.padding(p.0);
        }
        if let Some(Some(s)) = style.t {
            block = block.style(s.0);
        }
        if let Some(Some(LineV(l))) = &title.t {
            block = block.title(into_borrowed_line(l));
        }
        if let Some(Some(a)) = title_alignment.t {
            block = block.title_alignment(a.0);
        }
        if let Some(Some(LineV(l))) = &title_bottom.t {
            block = block.title_bottom(into_borrowed_line(l));
        }
        if let Some(Some(p)) = title_position.t {
            block = block.title_position(p.0);
        }
        if let Some(Some(s)) = title_style.t {
            block = block.title_style(s.0);
        }
        if let Some(Some(LineV(l))) = &title_top.t {
            block = block.title_top(into_borrowed_line(l));
        }
        let child_rect = block.inner(rect);
        frame.render_widget(block, rect);
        child.draw(frame, child_rect)?;
        Ok(())
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
        enum Act {
            Inc,
            Dec,
        }
        if let Some(e) = e.as_key_event() {
            let o = self
                .orientation
                .t
                .as_ref()
                .and_then(|o| o.as_ref().map(|o| o.0.clone()))
                .unwrap_or(ScrollbarOrientation::VerticalRight);
            let action = match o {
                ScrollbarOrientation::HorizontalBottom
                | ScrollbarOrientation::HorizontalTop => match e.code {
                    KeyCode::Left => Some(Act::Dec),
                    KeyCode::Right => Some(Act::Inc),
                    _ => None,
                },
                ScrollbarOrientation::VerticalLeft
                | ScrollbarOrientation::VerticalRight => match e.code {
                    KeyCode::Up | KeyCode::PageUp => Some(Act::Dec),
                    KeyCode::Down | KeyCode::PageDown => Some(Act::Inc),
                    _ => None,
                },
            };
            let pos = self.position.t.and_then(|v| v).unwrap_or(0);
            match action {
                None => (),
                Some(Act::Inc) if pos < u16::MAX => {
                    self.position.set_deref(Some(pos + 1))?
                }
                Some(Act::Dec) if pos > 0 => self.position.set_deref(Some(pos - 1))?,
                Some(Act::Inc) | Some(Act::Dec) => (),
            }
        }
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
        position.update(id, &v).context("scrollbar update position")?;
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

pub(super) struct LayoutW {
    bs: BSHandle,
    children: Vec<(Constraint, GuiW)>,
    children_ref: Ref,
    direction: TRef<Option<DirectionV>>,
    flex: TRef<Option<FlexV>>,
    horizontal_margin: TRef<Option<u16>>,
    margin: TRef<Option<u16>>,
    spacing: TRef<Option<SpacingV>>,
    vertical_margin: TRef<Option<u16>>,
}

impl LayoutW {
    pub(super) async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [(_, children), (_, direction), (_, flex), (_, horizontal_margin), (_, margin), (_, spacing), (_, vertical_margin)] =
            v.cast_to::<[(ArcStr, u64); 7]>().context("layout fields")?;
        let (
            children_ref,
            direction,
            flex,
            horizontal_margin,
            margin,
            spacing,
            vertical_margin,
        ) = try_join! {
            bs.compile_ref(children),
            bs.compile_ref(direction),
            bs.compile_ref(flex),
            bs.compile_ref(horizontal_margin),
            bs.compile_ref(margin),
            bs.compile_ref(spacing),
            bs.compile_ref(vertical_margin)
        }?;
        let direction = TRef::<Option<DirectionV>>::new(direction)
            .context("layout tref direction")?;
        let flex = TRef::<Option<FlexV>>::new(flex).context("layout tref flex")?;
        let horizontal_margin = TRef::<Option<u16>>::new(horizontal_margin)
            .context("layout tref horizontal_margin")?;
        let margin = TRef::<Option<u16>>::new(margin).context("layout tref margin")?;
        let spacing =
            TRef::<Option<SpacingV>>::new(spacing).context("layout tref spacing")?;
        let vertical_margin = TRef::<Option<u16>>::new(vertical_margin)
            .context("layout tref vertical_margin")?;
        let mut t = Self {
            bs,
            children: vec![],
            children_ref,
            direction,
            flex,
            horizontal_margin,
            margin,
            spacing,
            vertical_margin,
        };
        if let Some(v) = t.children_ref.last.take() {
            t.set_children(v).await?;
        }
        Ok(Box::new(t))
    }

    async fn set_children(&mut self, v: Value) -> Result<()> {
        self.children = future::join_all(
            v.cast_to::<SmallVec<[(ConstraintV, Value); 8]>>()?.into_iter().map(
                |(c, v)| {
                    let bs = self.bs.clone();
                    async move {
                        let child = compile(bs, v).await?;
                        Ok((c.0, child))
                    }
                },
            ),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        Ok(())
    }
}

#[async_trait]
impl GuiWidget for LayoutW {
    async fn handle_event(&mut self, e: Event) -> Result<()> {
        future::try_join_all(self.children.iter_mut().map(|(_, c)| {
            let e = e.clone();
            async move { c.handle_event(e).await }
        }))
        .await?;
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self {
            bs: _,
            children: _,
            children_ref,
            direction,
            flex,
            horizontal_margin,
            margin,
            spacing,
            vertical_margin,
        } = self;
        direction.update(id, &v).context("layout direction update")?;
        flex.update(id, &v).context("layout flex update")?;
        horizontal_margin.update(id, &v).context("layout horizontal_margin update")?;
        margin.update(id, &v).context("layout margin update")?;
        spacing.update(id, &v).context("layout spacing update")?;
        vertical_margin.update(id, &v).context("layout vertical_margin update")?;
        if children_ref.id == id {
            self.set_children(v.clone()).await?;
        }
        for (_, c) in &mut self.children {
            c.handle_update(id, v.clone()).await?
        }
        Ok(())
    }

    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let Self {
            bs: _,
            children,
            children_ref: _,
            direction,
            flex,
            horizontal_margin,
            margin,
            spacing,
            vertical_margin,
        } = self;
        let mut layout = Layout::default();
        if let Some(Some(d)) = direction.t {
            layout = layout.direction(d.0);
        }
        if let Some(Some(f)) = flex.t {
            layout = layout.flex(f.0);
        }
        if let Some(Some(m)) = horizontal_margin.t {
            layout = layout.horizontal_margin(m);
        }
        if let Some(Some(m)) = margin.t {
            layout = layout.margin(m);
        }
        if let Some(Some(s)) = &spacing.t {
            layout = layout.spacing(s.0.clone());
        }
        if let Some(Some(m)) = vertical_margin.t {
            layout = layout.vertical_margin(m);
        }
        layout = layout.constraints(children.iter().map(|(c, _)| *c));
        let areas = layout.split(rect);
        for (rect, (_, child)) in areas.iter().zip(children.iter_mut()) {
            child.draw(frame, *rect)?
        }
        Ok(())
    }
}
