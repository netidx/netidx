use anyhow::Result;
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use futures::{channel::mpsc, SinkExt, StreamExt};
use log::error;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, Ref},
};
use ratatui::{
    layout::{Alignment, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Paragraph, Wrap},
    Frame,
};
use reedline::Signal;
use smallvec::SmallVec;
use std::borrow::Cow;
use tokio::{select, sync::oneshot, task};

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
                    let v = v.cast_to::<[(ArcStr, u8); 3]>()?;
                    Ok(Self(Color::Rgb(v[2].1, v[1].1, v[0].1)))
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
        let flds = v.cast_to::<[(ArcStr, Value); 5]>()?;
        let add_modifier = flds[0].1.clone().cast_to::<ModifierV>()?.0;
        let bg = flds[1].1.clone().cast_to::<Option<ColorV>>()?.map(|c| c.0);
        let fg = flds[2].1.clone().cast_to::<Option<ColorV>>()?.map(|c| c.0);
        let sub_modifier = flds[3].1.clone().cast_to::<ModifierV>()?.0;
        let underline_color = flds[4].1.clone().cast_to::<Option<ColorV>>()?.map(|c| c.0);
        Ok(Self(Style { fg, bg, underline_color, add_modifier, sub_modifier }))
    }
}

struct SpanV(Span<'static>);

impl FromValue for SpanV {
    fn from_value(v: Value) -> Result<Self> {
        let flds = v.cast_to::<[(ArcStr, Value); 2]>()?;
        Ok(Self(Span {
            content: Cow::Owned(flds[0].1.clone().cast_to::<String>()?),
            style: flds[1].1.clone().cast_to::<StyleV>()?.0,
        }))
    }
}

struct LineV(Line<'static>);

impl FromValue for LineV {
    fn from_value(v: Value) -> Result<Self> {
        let flds = v.cast_to::<[(ArcStr, Value); 3]>()?;
        let alignment = flds[0].1.clone().cast_to::<Option<AlignmentV>>()?.map(|a| a.0);
        let spans = match &flds[1].1 {
            Value::String(s) => vec![Span::raw(String::from(&**s))],
            v => v
                .clone()
                .cast_to::<Vec<SpanV>>()?
                .into_iter()
                .map(|s| s.0)
                .collect::<Vec<_>>(),
        };
        let style = flds[2].1.clone().cast_to::<StyleV>()?.0;
        Ok(Self(Line { style, alignment, spans }))
    }
}

#[derive(Clone, Copy)]
struct ScrollV((u16, u16));

impl FromValue for ScrollV {
    fn from_value(v: Value) -> Result<Self> {
        let ((_, x), (_, y)) = v.cast_to::<((ArcStr, u16), (ArcStr, u16))>()?;
        Ok(Self((y, x)))
    }
}

fn into_borrowed_lines<'a>(lines: &'a [Line<'static>]) -> Vec<Line<'a>> {
    lines
        .iter()
        .map(|l| {
            let spans = l
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
            Line { alignment: l.alignment, style: l.style, spans }
        })
        .collect::<Vec<_>>()
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

#[async_trait]
trait GuiWidget {
    async fn handle_event(&mut self, e: Event) -> Result<()>;
    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()>;
    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()>;
}

type GuiW = Box<dyn GuiWidget + Send + Sync + 'static>;

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
    alignment: TRef<AlignmentV>,
    lines: TRef<Vec<LineV>>,
    style: TRef<StyleV>,
    text: Text<'static>,
}

impl TextW {
    async fn compile(bs: &BSHandle, source: Value) -> Result<GuiW> {
        let flds = source.cast_to::<[(ArcStr, u64); 3]>()?;
        let alignment = TRef::<AlignmentV>::new(bs.compile_ref(flds[0].1).await?)?;
        let mut lines = TRef::<Vec<LineV>>::new(bs.compile_ref(flds[1].1).await?)?;
        let style = TRef::<StyleV>::new(bs.compile_ref(flds[2].1).await?)?;
        let text = Text {
            alignment: alignment.t.as_ref().map(|a| a.0),
            style: style.t.as_ref().map(|s| s.0).unwrap_or(Style::new()),
            lines: lines
                .t
                .take()
                .map(|l| l.into_iter().map(|l| l.0).collect())
                .unwrap_or(vec![]),
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
        if let Some(a) = alignment.update(id, &v)? {
            text.alignment = Some(a.0);
        }
        if let Some(l) = lines.update(id, &v)? {
            text.lines = l.drain(..).map(|l| l.0).collect();
        }
        if let Some(s) = style.update(id, &v)? {
            text.style = s.0;
        }
        Ok(())
    }
}

struct ParagraphW {
    alignment: TRef<Option<AlignmentV>>,
    lines: TRef<Vec<LineV>>,
    scroll: TRef<ScrollV>,
    style: TRef<StyleV>,
    trim: TRef<bool>,
    text: Vec<Line<'static>>,
}

impl ParagraphW {
    async fn compile(bs: &BSHandle, source: Value) -> Result<GuiW> {
        let flds = source.cast_to::<[(ArcStr, u64); 5]>()?;
        let alignment: TRef<Option<AlignmentV>> =
            TRef::new(bs.compile_ref(flds[0].1).await?)?;
        let lines: TRef<Vec<LineV>> = TRef::new(bs.compile_ref(flds[1].1).await?)?;
        let scroll: TRef<ScrollV> = TRef::new(bs.compile_ref(flds[2].1).await?)?;
        let style: TRef<StyleV> = TRef::new(bs.compile_ref(flds[3].1).await?)?;
        let trim: TRef<bool> = TRef::new(bs.compile_ref(flds[4].1).await?)?;
        let text = vec![];
        Ok(Box::new(Self { alignment, lines, scroll, style, trim, text }))
    }
}

#[async_trait]
impl GuiWidget for ParagraphW {
    fn draw(&mut self, frame: &mut Frame, rect: Rect) -> Result<()> {
        let mut p = Paragraph::new(into_borrowed_lines(&self.text));
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
        let Self { alignment, lines, scroll, style, trim, text } = self;
        let _ = alignment.update(id, &v)?;
        if let Some(l) = lines.update(id, &v)? {
            text.clear();
            text.extend(l.drain(..).map(|l| l.0))
        }
        let _ = scroll.update(id, &v)?;
        let _ = style.update(id, &v)?;
        let _ = trim.update(id, &v)?;
        Ok(())
    }
}

/*
struct BlockW {
    border_id: ExprId,
    border: Option<Borders>,
    border_type_id: ExprId,
    border_type: Option<BorderType>,
    border_style_id: ExprId,
    border_style: Option<Style>,
    padding_id: ExprId,
    padding: Option<Padding>,
    sytle_id: ExprId,
    style: Option<Style>,
    title: Option<Line<'static>>,
    title_id: ExprId,
    title_alignment_id: ExprId,
    title_alignment: Option<Alignment>,
    title_bottom_id: ExprId,
    title_bottom: Option<Line<'static>>,
}
*/

async fn compile(bs: &BSHandle, source: Value) -> Result<GuiW> {
    match source.cast_to::<(ArcStr, Value)>()? {
        (s, v) if &s == "Text" => TextW::compile(bs, v).await,
        (s, v) if &s == "Paragraph" => ParagraphW::compile(bs, v).await,
        //        (s, v) if &s == "Block" => BlockW::compile(bs, v).await,
        (s, v) => bail!("invalid widget type `{s}({v})"),
    }
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
    pub(super) fn start(bs: &BSHandle, root: ExprId) -> Gui {
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
    root_expr: ExprId,
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
                    if id == root_expr {
                        match compile(&bs, v).await {
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
