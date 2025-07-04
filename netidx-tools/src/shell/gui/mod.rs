use anyhow::Result;
use arcstr::ArcStr;
use async_trait::async_trait;
use barchart::BarChartW;
use chart::ChartW;
use block::BlockW;
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use futures::{channel::mpsc, SinkExt, StreamExt};
use layout::LayoutW;
use log::error;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{
    expr::ExprId,
    rt::{BSHandle, CompExp, Ref},
};
use paragraph::ParagraphW;
use ratatui::{
    layout::{Alignment, Direction, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::block::Position,
    Frame,
};
use reedline::Signal;
use scrollbar::ScrollbarW;
use sparkline::SparklineW;
use smallvec::SmallVec;
use std::{borrow::Cow, future::Future, pin::Pin};
use text::TextW;
use tokio::{select, sync::oneshot, task};

mod barchart;
mod block;
mod chart;
mod layout;
mod paragraph;
mod scrollbar;
mod text;
mod sparkline;

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

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug)]
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

fn compile(bs: BSHandle, source: Value) -> CompRes {
    Box::pin(async move {
        match source.cast_to::<(ArcStr, Value)>()? {
            (s, v) if &s == "Text" => TextW::compile(bs, v).await,
            (s, v) if &s == "Paragraph" => ParagraphW::compile(bs, v).await,
            (s, v) if &s == "Block" => BlockW::compile(bs, v).await,
            (s, v) if &s == "Scrollbar" => ScrollbarW::compile(bs, v).await,
            (s, v) if &s == "Layout" => LayoutW::compile(bs, v).await,
            (s, v) if &s == "BarChart" => BarChartW::compile(bs, v).await,
            (s, v) if &s == "Chart" => ChartW::compile(bs, v).await,
            (s, v) if &s == "Sparkline" => SparklineW::compile(bs, v).await,
            (s, v) => bail!("invalid widget type `{s}({v})"),
        }
    })
}

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
