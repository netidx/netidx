use anyhow::{Context, Result};
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use futures::{channel::mpsc, future, SinkExt, StreamExt};
use log::error;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, CompExp, Ref},
};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Flex, Layout, Rect, Spacing},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        block::Position, Block, BorderType, Borders, Padding, Paragraph, Scrollbar,
        ScrollbarOrientation, ScrollbarState, Wrap,
    },
    Frame,
};
use reedline::Signal;
use smallvec::SmallVec;
use std::{borrow::Cow, future::Future, mem, pin::Pin};
use tokio::{join, select, sync::oneshot, task, try_join};

#[derive(Clone, Copy)]
struct AlignmentV(Alignment);

impl FromValue for AlignmentV {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::String(s) => match &*s {
                "Left" => Ok(AlignmentV(Alignment::Left)),
                "Right" => Ok(AlignmentV(Alignment::Right)),
                "Center" => Ok(AlignmentV(Alignment::Center)),
                s => bail!("invalid alignment {s}"),
            },
            v => bail!("invalid alignment {v}"),
        }
    }
}

#[derive(Clone, Copy)]
struct ColorV(Color);

impl FromValue for ColorV {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::String(s) => match &*s {
                "Reset" => Ok(Self(Color::Reset)),
                "Black" => Ok(Self(Color::Black)),
                "Red" => Ok(Self(Color::Red)),
                "Green" => Ok(Self(Color::Green)),
                "Yellow" => Ok(Self(Color::Yellow)),
                "Blue" => Ok(Self(Color::Blue)),
                "Magenta" => Ok(Self(Color::Magenta)),
                "Cyan" => Ok(Self(Color::Cyan)),
                "Gray" => Ok(Self(Color::Gray)),
                "DarkGray" => Ok(Self(Color::DarkGray)),
                "LightRed" => Ok(Self(Color::LightRed)),
                "LightGreen" => Ok(Self(Color::LightGreen)),
                "LightYellow" => Ok(Self(Color::LightYellow)),
                "LightBlue" => Ok(Self(Color::LightBlue)),
                "LightMagenta" => Ok(Self(Color::LightMagenta)),
                "LightCyan" => Ok(Self(Color::LightCyan)),
                "White" => Ok(Self(Color::White)),
                s => bail!("invalid color name {s}"),
            },
            v => match v.cast_to::<(ArcStr, Value)>()? {
                (s, v) if &*s == "Rgb" => {
                    let [(_, b), (_, g), (_, r)] = v.cast_to::<[(ArcStr, u8); 3]>()?;
                    Ok(Self(Color::Rgb(r, g, b)))
                }
                (s, v) if &*s == "Indexed" => {
                    Ok(Self(Color::Indexed(v.cast_to::<u8>()?)))
                }
                (s, v) => bail!("invalid color ({s} {v})"),
            },
        }
    }
}

#[derive(Clone, Copy)]
struct ModifierV(Modifier);

impl FromValue for ModifierV {
    fn from_value(v: Value) -> Result<Self> {
        let mut m = Modifier::empty();
        if let Some(o) = v.cast_to::<Option<SmallVec<[ArcStr; 2]>>>()? {
            for s in o {
                match &*s {
                    "Bold" => m |= Modifier::BOLD,
                    "Italic" => m |= Modifier::ITALIC,
                    s => bail!("invalid modifier {s}"),
                }
            }
        }
        Ok(Self(m))
    }
}

#[derive(Clone, Copy)]
struct StyleV(Style);

impl FromValue for StyleV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, add_modifier), (_, bg), (_, fg), (_, sub_modifier), (_, underline_color)] =
            v.cast_to::<[(ArcStr, Value); 5]>()?;
        let add_modifier = add_modifier.cast_to::<ModifierV>()?.0;
        let bg = bg.cast_to::<Option<ColorV>>()?.map(|c| c.0);
        let fg = fg.cast_to::<Option<ColorV>>()?.map(|c| c.0);
        let sub_modifier = sub_modifier.cast_to::<ModifierV>()?.0;
        let underline_color = underline_color.cast_to::<Option<ColorV>>()?.map(|c| c.0);
        Ok(Self(Style { fg, bg, underline_color, add_modifier, sub_modifier }))
    }
}

struct SpanV(Span<'static>);

impl FromValue for SpanV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, content), (_, style)] = v.cast_to::<[(ArcStr, Value); 2]>()?;
        Ok(Self(Span {
            content: Cow::Owned(content.cast_to::<String>()?),
            style: style.cast_to::<StyleV>()?.0,
        }))
    }
}

struct LineV(Line<'static>);

impl FromValue for LineV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, alignment), (_, spans), (_, style)] =
            v.cast_to::<[(ArcStr, Value); 3]>()?;
        let alignment = alignment.cast_to::<Option<AlignmentV>>()?.map(|a| a.0);
        let spans = match spans {
            Value::String(s) => vec![Span::raw(String::from(&*s))],
            v => v
                .clone()
                .cast_to::<Vec<SpanV>>()?
                .into_iter()
                .map(|s| s.0)
                .collect::<Vec<_>>(),
        };
        let style = style.cast_to::<StyleV>()?.0;
        Ok(Self(Line { style, alignment, spans }))
    }
}

struct LinesV(Vec<Line<'static>>);

impl FromValue for LinesV {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::String(s) => Ok(Self(vec![Line::raw(String::from(s.as_str()))])),
            v => Ok(Self(v.cast_to::<Vec<LineV>>()?.into_iter().map(|l| l.0).collect())),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ScrollV((u16, u16));

impl FromValue for ScrollV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, x), (_, y)] = v.cast_to::<[(ArcStr, u16); 2]>()?;
        Ok(Self((y, x)))
    }
}

#[derive(Clone, Copy)]
struct BordersV(Borders);

impl FromValue for BordersV {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::String(s) => match &*s {
                "All" => Ok(Self(Borders::all())),
                "None" => Ok(Self(Borders::empty())),
                s => bail!("invalid borders {s}"),
            },
            v => {
                let mut res = Borders::empty();
                for b in v.cast_to::<SmallVec<[ArcStr; 4]>>()? {
                    match &*b {
                        "Top" => res.insert(Borders::TOP),
                        "Right" => res.insert(Borders::RIGHT),
                        "Bottom" => res.insert(Borders::BOTTOM),
                        "Left" => res.insert(Borders::LEFT),
                        s => bail!("invalid border {s}"),
                    }
                }
                Ok(Self(res))
            }
        }
    }
}

#[derive(Clone, Copy)]
struct BorderTypeV(BorderType);

impl FromValue for BorderTypeV {
    fn from_value(v: Value) -> Result<Self> {
        match &*v.cast_to::<ArcStr>()? {
            "Plain" => Ok(Self(BorderType::Plain)),
            "Rounded" => Ok(Self(BorderType::Rounded)),
            "Double" => Ok(Self(BorderType::Double)),
            "Thick" => Ok(Self(BorderType::Thick)),
            "QuadrantInside" => Ok(Self(BorderType::QuadrantInside)),
            "QuadrantOutside" => Ok(Self(BorderType::QuadrantOutside)),
            s => bail!("invalid border type {s}"),
        }
    }
}

#[derive(Clone, Copy)]
struct PaddingV(Padding);

impl FromValue for PaddingV {
    fn from_value(v: Value) -> Result<Self> {
        let [(_, bottom), (_, left), (_, right), (_, top)] =
            v.cast_to::<[(ArcStr, u16); 4]>()?;
        Ok(Self(Padding { bottom, left, right, top }))
    }
}

#[derive(Clone, Copy)]
struct PositionV(Position);

impl FromValue for PositionV {
    fn from_value(v: Value) -> Result<Self> {
        match &*v.cast_to::<ArcStr>()? {
            "Top" => Ok(Self(Position::Top)),
            "Bottom" => Ok(Self(Position::Bottom)),
            s => bail!("invalid position {s}"),
        }
    }
}

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

#[derive(Clone, Copy)]
struct ConstraintV(Constraint);

impl FromValue for ConstraintV {
    fn from_value(v: Value) -> Result<Self> {
        let t = match &v.cast_to::<SmallVec<[Value; 3]>>()?[..] {
            [Value::String(s), Value::U32(p)] => match &**s {
                "Min" => Constraint::Min(*p as u16),
                "Max" => Constraint::Max(*p as u16),
                "Percentage" => Constraint::Percentage(*p as u16),
                "Fill" => Constraint::Fill(*p as u16),
                s => bail!("invalid constraint tag {s}"),
            },
            [Value::String(s), Value::U32(n), Value::U32(d)] if &**s == "Ratio" => {
                Constraint::Ratio(*n, *d)
            }
            v => bail!("invalid constraint {v:?}"),
        };
        Ok(Self(t))
    }
}

#[derive(Clone, Copy)]
struct DirectionV(Direction);

impl FromValue for DirectionV {
    fn from_value(v: Value) -> Result<Self> {
        let t = match &*v.cast_to::<ArcStr>()? {
            "Horizontal" => Direction::Horizontal,
            "Vertical" => Direction::Vertical,
            s => bail!("invalid direction tag {s}"),
        };
        Ok(Self(t))
    }
}

#[derive(Clone, Copy)]
struct FlexV(Flex);

impl FromValue for FlexV {
    fn from_value(v: Value) -> Result<Self> {
        let t = match &*v.cast_to::<ArcStr>()? {
            "Legacy" => Flex::Legacy,
            "Start" => Flex::Start,
            "End" => Flex::End,
            "Center" => Flex::Center,
            "SpaceBetween" => Flex::SpaceBetween,
            "SpaceAround" => Flex::SpaceAround,
        };
        Ok(Self(t))
    }
}

#[derive(Clone, Copy)]
struct SpacingV(Spacing);

impl FromValue for SpacingV {
    fn from_value(v: Value) -> Result<Self> {
        let t = match v.cast_to::<(ArcStr, u16)>()? {
            (s, p) if &*s == "Space" => Spacing::Space(p),
            (s, p) if &*s == "Overlap" => Spacing::Overlap(p),
            (s, _) => bail!("invalid spacing tag {s}"),
        };
        Ok(Self(t))
    }
}

fn into_borrowed_line<'a>(line: &'a Line<'static>) -> Line<'a> {
    let spans = line
        .spans
        .iter()
        .map(|s| {
            let content = match &s.content {
                Cow::Owned(s) => Cow::Borrowed(s.as_str()),
                Cow::Borrowed(s) => Cow::Borrowed(*s),
            };
            Span { content, style: s.style }
        })
        .collect();
    Line { alignment: line.alignment, style: line.style, spans }
}

fn into_borrowed_lines<'a>(lines: &'a [Line<'static>]) -> Vec<Line<'a>> {
    lines.iter().map(|l| into_borrowed_line(l)).collect::<Vec<_>>()
}

struct TRef<T: FromValue> {
    r: Ref,
    t: Option<T>,
}

impl<T: FromValue> TRef<T> {
    fn new(mut r: Ref) -> Result<Self> {
        let t = r.last.take().map(|v| v.cast_to()).transpose()?;
        Ok(TRef { r, t })
    }

    fn update(&mut self, id: ExprId, v: &Value) -> Result<Option<&mut T>> {
        if self.r.id == id {
            let v = v.clone().cast_to()?;
            self.t = Some(v);
            Ok(self.t.as_mut())
        } else {
            Ok(None)
        }
    }
}

impl<T: Into<Value> + FromValue + Clone> TRef<T> {
    #[allow(dead_code)]
    fn set(&mut self, t: T) -> Result<()> {
        self.t = Some(t.clone());
        self.r.set(t.into())
    }

    #[allow(dead_code)]
    fn set_deref(&mut self, t: T) -> Result<()> {
        self.t = Some(t.clone());
        self.r.set_deref(t.into())
    }
}

#[async_trait]
trait GuiWidget {
    async fn handle_event(&mut self, e: Event) -> Result<()>;
    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()>;
    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()>;
}

type GuiW = Box<dyn GuiWidget + Send + Sync + 'static>;
type CompRes = Pin<Box<dyn Future<Output = Result<GuiW>> + Send + Sync + 'static>>;

struct EmptyW;

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

struct TextW {
    alignment: TRef<Option<AlignmentV>>,
    lines: TRef<LinesV>,
    style: TRef<StyleV>,
    text: Text<'static>,
}

impl TextW {
    async fn compile(bs: BSHandle, source: Value) -> Result<GuiW> {
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

struct ParagraphW {
    alignment: TRef<Option<AlignmentV>>,
    lines: TRef<LinesV>,
    scroll: TRef<ScrollV>,
    style: TRef<StyleV>,
    trim: TRef<bool>,
}

impl ParagraphW {
    async fn compile(bs: BSHandle, source: Value) -> Result<GuiW> {
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
        scroll.update(id, &v).context("paragraph update scroll")?;
        style.update(id, &v).context("paragraph update style")?;
        trim.update(id, &v).context("paragraph update trim")?;
        Ok(())
    }
}

struct BlockW {
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
    async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [(_, border), (_, border_style), (_, border_type), (_, child), (_, padding), (_, style), (_, title), (_, title_alignment), (_, title_bottom), (_, title_position), (_, title_style), (_, title_top)] =
            v.cast_to::<[(ArcStr, u64); 12]>().context("block flds")?;
        let (
            border,
            border_style,
            border_type,
            child_ref,
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

struct ScrollbarW {
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
    async fn compile(bs: BSHandle, v: Value) -> Result<GuiW> {
        let [(_, begin_style), (_, begin_symbol), (_, child), (_, content_length), (_, end_style), (_, end_symbol), (_, orientation), (_, position), (_, style), (_, thumb_style), (_, thumb_symbol), (_, track_style), (_, track_symbol), (_, viewport_length)] =
            v.cast_to::<[(ArcStr, u64); 14]>().context("scrollbar flds")?;
        let (
            begin_style,
            begin_symbol,
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

struct LayoutW {
    bs: BSHandle,
    layout: Layout,
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
    async fn compile(bs: BSHandle, source: Value) -> Result<GuiW> {
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
            layout: Layout::default(),
            children: vec![],
            children_ref,
            direction,
            flex,
            horizontal_margin,
            margin,
            spacing,
            vertical_margin,
        };
        t.maybe_set_children().await?;
        Ok(t)
    }

    async fn maybe_set_children(&mut self) -> Result<()> {
        if let Some(v) = self.children_ref.last.take() {
            self.children = future::join_all(
                v.cast_to::<SmallVec<[(ConstraintV, Value); 8]>>()?.into_iter().map(
                    |(c, v)| async {
                        let child = compile(self.bs.clone(), v).await?;
                        Ok((c.0, child))
                    },
                ),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        }
    }
}

fn compile(bs: BSHandle, source: Value) -> CompRes {
    Box::pin(async move {
        match source.cast_to::<(ArcStr, Value)>()? {
            (s, v) if &s == "Text" => TextW::compile(bs, v).await,
            (s, v) if &s == "Paragraph" => ParagraphW::compile(bs, v).await,
            (s, v) if &s == "Block" => BlockW::compile(bs, v).await,
            (s, v) if &s == "Scrollbar" => ScrollbarW::compile(bs, v).await,
            (s, v) => bail!("invalid widget type `{s}({v})"),
        }
    })
}

enum ToGui {
    Update(ExprId, Value),
    Stop(oneshot::Sender<()>),
}

enum FromGui {
    CtrlC,
}

#[derive(Debug)]
pub(super) struct Gui {
    to: mpsc::Sender<ToGui>,
    from: mpsc::UnboundedReceiver<FromGui>,
}

impl Gui {
    pub(super) fn start(bs: &BSHandle, root: CompExp) -> Gui {
        let bs = bs.clone();
        let (to_tx, to_rx) = mpsc::channel(3);
        let (from_tx, from_rx) = mpsc::unbounded();
        task::spawn(async move {
            if let Err(e) = run(bs, root, to_rx, from_tx).await {
                error!("gui::run returned {e:?}")
            }
        });
        Self { to: to_tx, from: from_rx }
    }

    pub(super) async fn stop(&mut self) {
        let (tx, rx) = oneshot::channel();
        let _ = self.to.send(ToGui::Stop(tx)).await;
        let _ = rx.await;
    }

    pub(super) async fn update(&mut self, id: ExprId, v: Value) {
        if let Err(_) = self.to.send(ToGui::Update(id, v)).await {
            error!("could not send update because gui task died")
        }
    }

    pub(super) async fn wait_signal(&mut self) -> Signal {
        match self.from.next().await {
            None => Signal::CtrlC,
            Some(FromGui::CtrlC) => Signal::CtrlC,
        }
    }
}

fn is_ctrl_c(e: &Event) -> bool {
    e.as_key_press_event()
        .map(|e| match e.code {
            KeyCode::Char('c') if e.modifiers == KeyModifiers::CONTROL.into() => true,
            _ => false,
        })
        .unwrap_or(false)
}

async fn run(
    bs: BSHandle,
    root_exp: CompExp,
    mut to_rx: mpsc::Receiver<ToGui>,
    from_tx: mpsc::UnboundedSender<FromGui>,
) -> Result<()> {
    let mut terminal = ratatui::init();
    let mut events = EventStream::new().fuse();
    let mut root: GuiW = Box::new(EmptyW);
    let notify = loop {
        terminal.draw(|f| {
            if let Err(e) = root.draw(f, f.area()) {
                error!("error drawing {e:?}")
            }
        })?;
        select! {
            m = to_rx.next() => match m {
                None => break oneshot::channel().0,
                Some(ToGui::Stop(tx)) => break tx,
                Some(ToGui::Update(id, v)) => {
                    if id == root_exp.id {
                        match compile(bs.clone(), v).await {
                            Err(e) => error!("invalid widget specification {e:?}"),
                            Ok(w) => root = w,
                        }
                    } else {
                        if let Err(e) = root.handle_update(id, v).await {
                            error!("error handling update {e:?}")
                        }
                    }
                },
            },
            e = events.select_next_some() => match e {
                Ok(e) if is_ctrl_c(&e) => {
                    if let Err(_) = from_tx.unbounded_send(FromGui::CtrlC) {
                        error!("main application has died, quitting qui");
                        break oneshot::channel().0
                    }
                }
                Ok(e) => if let Err(e) = root.handle_event(e).await {
                    error!("error handling event {e:?}")
                },
                Err(e) => {
                    error!("error reading event from terminal {e:?}");
                    break oneshot::channel().0
                }
            }
        }
    };
    ratatui::restore();
    let _ = notify.send(());
    Ok(())
}
