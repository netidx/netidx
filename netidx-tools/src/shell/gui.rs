use std::borrow::Cow;

use anyhow::Result;
use arcstr::ArcStr;
use async_trait::async_trait;
use compact_str::CompactString;
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use futures::{channel::mpsc, future, SinkExt, StreamExt};
use log::error;
use netidx::{
    protocol::value::NakedValue,
    publisher::{FromValue, Value},
};
use netidx_bscript::{expr::ExprId, rt::BSHandle};
use ratatui::{
    layout::Alignment,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    Frame,
};
use reedline::Signal;
use smallvec::SmallVec;
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
        for o in v.cast_to::<Option<SmallVec<[ArcStr; 2]>>>()? {
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
        let flds = v.cast_to::<(ArcStr, Value); 3>()?;
        let alignment = flds[0].1.clone().cast_to::<Option<AlignmentV>>()?.map(|a| a.0);
        let spans = match &flds[1].1 {
            Value::String(s) => vec![Span::raw(s)],
            v => v.clone().cast_to::<Vec<SpanV>>()?.into_iter().map(|s| s.0).collect::<Vec<_>>()
        };
        let style = flds[2].1.clone().cast_to::<StyleV>()?.0;
        Ok(Self(Line { style, alignment, spans }))
    }
}

#[async_trait]
trait GuiWidget {
    async fn handle_event(&mut self, e: Event);
    async fn handle_update(&mut self, id: ExprId, v: Value);
    fn draw(&mut self, frame: &mut Frame);
    async fn delete(&mut self);
}

type GuiW = Box<dyn GuiWidget + Send + Sync + 'static>;

struct EmptyW;

#[async_trait]
impl GuiWidget for EmptyW {
    async fn delete(&mut self) {
        ()
    }

    async fn handle_event(&mut self, _e: Event) {
        ()
    }

    async fn handle_update(&mut self, _id: ExprId, _v: Value) {
        ()
    }

    fn draw(&mut self, _frame: &mut Frame) {
        ()
    }
}

struct TextW {
    bs: BSHandle,
    alignment: Value,
    alignment_id: ExprId,
    lines: Value,
    lines_id: ExprId,
    style: Value,
    style_id: ExprId,
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
        Ok(Box::new(Self {
            bs: bs.clone(),
            alignment: alignment.unwrap_or(Value::Null),
            alignment_id,
            lines: lines.unwrap_or(Value::Null),
            lines_id,
            style: style.unwrap_or(Value::Null),
            style_id,
        }))
    }
}

#[async_trait]
impl GuiWidget for TextW {
    async fn delete(&mut self) {
        let Self {
            bs,
            alignment: _,
            alignment_id,
            lines: _,
            lines_id,
            style: _,
            style_id,
        } = self;
        let _ = future::join_all([
            bs.delete(*alignment_id),
            bs.delete(*lines_id),
            bs.delete(*style_id),
        ])
        .await;
    }

    fn draw(&mut self, frame: &mut Frame) {
        let text = Text::raw(&self.text);
        frame.render_widget(text, frame.area());
    }

    async fn handle_event(&mut self, _: Event) {
        ()
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) {
        let Self { bs: _, alignment, alignment_id, lines, lines_id, style, style_id } =
            self;
        if id == *alignment_id {
            *alignment = v;
        } else if id == *lines_id {
            *lines = v;
        } else if id == *style_id {
            *style = v;
        }
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
        terminal.draw(|f| root.draw(f))?;
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
                        root.handle_update(id, v).await
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
                Ok(e) => root.handle_event(e).await,
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
