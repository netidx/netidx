use anyhow::Result;
use arcstr::ArcStr;
use async_trait::async_trait;
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use futures::{channel::mpsc, future, SinkExt, StreamExt};
use log::error;
use netidx::publisher::{FromValue, Value};
use netidx_bscript::{expr::ExprId, rt::BSHandle};
use ratatui::{
    layout::Alignment,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    Frame,
};
use reedline::Signal;
use smallvec::SmallVec;
use std::borrow::Cow;
use tokio::{select, sync::oneshot, task};

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

#[async_trait]
trait GuiWidget {
    async fn handle_event(&mut self, e: Event) -> Result<()>;
    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()>;
    fn draw(&mut self, frame: &mut Frame) -> Result<()>;
    async fn delete(&mut self);
}

type GuiW = Box<dyn GuiWidget + Send + Sync + 'static>;

struct EmptyW;

#[async_trait]
impl GuiWidget for EmptyW {
    async fn delete(&mut self) {}

    async fn handle_event(&mut self, _e: Event) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, _id: ExprId, _v: Value) -> Result<()> {
        Ok(())
    }

    fn draw(&mut self, _frame: &mut Frame) -> Result<()> {
        Ok(())
    }
}

struct TextW {
    bs: BSHandle,
    alignment: ExprId,
    lines: ExprId,
    style: ExprId,
    text: Text<'static>,
}

impl TextW {
    async fn compile(bs: &BSHandle, source: Value) -> Result<GuiW> {
        let (alignment_id, lines_id, style_id) =
            match &source.cast_to::<SmallVec<[(ArcStr, u64); 3]>>()?[..] {
                [(s0, id0), (s1, id1), (s2, id2)]
                    if &*s0 == "alignment" && &*s1 == "lines" && &*s2 == "style" =>
                {
                    (*id0, *id1, *id2)
                }
                _ => bail!("expected struct Text"),
            };
        let (alignment_id, alignment) = bs.compile_ref(Value::U64(alignment_id)).await?;
        let (lines_id, lines) = bs.compile_ref(Value::U64(lines_id)).await?;
        let (style_id, style) = bs.compile_ref(Value::U64(style_id)).await?;
        let mut t = Self {
            bs: bs.clone(),
            alignment: alignment_id,
            lines: lines_id,
            style: style_id,
            text: Text { alignment: None, style: Style::new(), lines: vec![] },
        };
        if let Some(v) = alignment {
            t.set_alignment(v)?
        }
        if let Some(v) = style {
            t.set_style(v)?
        }
        if let Some(v) = lines {
            t.set_lines(v)?
        }
        Ok(Box::new(t))
    }

    fn set_alignment(&mut self, v: Value) -> Result<()> {
        self.text.alignment = v.cast_to::<Option<AlignmentV>>()?.map(|a| a.0);
        Ok(())
    }

    fn set_style(&mut self, v: Value) -> Result<()> {
        self.text.style = v.cast_to::<StyleV>()?.0;
        Ok(())
    }

    fn set_lines(&mut self, v: Value) -> Result<()> {
        self.text.lines = match v {
            Value::String(s) => vec![Line::from(String::from(&*s))],
            v => v.cast_to::<Vec<LineV>>()?.into_iter().map(|l| l.0).collect(),
        };
        Ok(())
    }
}

#[async_trait]
impl GuiWidget for TextW {
    async fn delete(&mut self) {
        let Self { bs, alignment, lines, style, text: _ } = self;
        let _ = future::join_all([
            bs.delete(*alignment),
            bs.delete(*lines),
            bs.delete(*style),
        ])
        .await;
    }

    fn draw(&mut self, frame: &mut Frame) -> Result<()> {
        frame.render_widget(&self.text, frame.area());
        todo!()
    }

    async fn handle_event(&mut self, _: Event) -> Result<()> {
        Ok(())
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) -> Result<()> {
        let Self { bs: _, alignment, lines, style, text: _ } = self;
        if id == *alignment {
            self.set_alignment(v)?;
        } else if id == *lines {
            self.set_lines(v)?
        } else if id == *style {
            self.set_style(v)?
        }
        Ok(())
    }
}

async fn compile(bs: &BSHandle, source: Value) -> Result<GuiW> {
    match source.cast_to::<(ArcStr, Value)>()? {
        (s, v) if &s == "Text" => TextW::compile(bs, v).await,
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
            if let Err(e) = root.draw(f) {
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
                            Ok(w) => {
                                root.delete().await;
                                root = w
                            },
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
