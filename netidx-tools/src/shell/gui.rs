use anyhow::Result;
use arcstr::ArcStr;
use async_trait::async_trait;
use compact_str::CompactString;
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use futures::{channel::mpsc, future, SinkExt, StreamExt};
use log::error;
use netidx::{protocol::value::NakedValue, publisher::Value};
use netidx_bscript::{expr::ExprId, rt::BSHandle};
use ratatui::{
    layout::Alignment,
    style::Style,
    text::{Line, Text},
    Frame,
};
use reedline::Signal;
use smallvec::SmallVec;
use tokio::{select, sync::oneshot, task};

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
