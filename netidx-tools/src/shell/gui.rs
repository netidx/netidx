use anyhow::Result;
use arcstr::ArcStr;
use async_trait::async_trait;
use compact_str::CompactString;
use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use futures::{channel::mpsc, SinkExt, StreamExt};
use log::error;
use netidx::{protocol::value::NakedValue, publisher::Value};
use netidx_bscript::{expr::ExprId, rt::BSHandle};
use ratatui::{text::Text, Frame};
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
    text: CompactString,
    source: ExprId,
}

impl TextW {
    async fn compile(bs: &BSHandle, source: Value) -> Result<GuiW> {
        let id = match &source.cast_to::<SmallVec<[(ArcStr, u64); 1]>>()?[..] {
            [(s, id)] if &*s == "text" => *id,
            _ => bail!("expected struct {{ text: &Any }}"),
        };
        let (source, current) = bs.compile_ref(Value::U64(id)).await?;
        let mut t = Self { bs: bs.clone(), text: CompactString::from(""), source };
        if let Some(v) = current {
            t.set(v)
        }
        Ok(Box::new(t))
    }

    fn set(&mut self, v: Value) {
        use std::fmt::Write;
        self.text.clear();
        match &v {
            Value::String(s) => write!(self.text, "{s}"),
            v => write!(self.text, "{}", NakedValue(v)),
        }
        .unwrap()
    }
}

#[async_trait]
impl GuiWidget for TextW {
    async fn delete(&mut self) {
        let _ = self.bs.delete(self.source).await;
    }

    fn draw(&mut self, frame: &mut Frame) {
        let text = Text::raw(&self.text);
        frame.render_widget(text, frame.area());
    }

    async fn handle_event(&mut self, _: Event) {
        ()
    }

    async fn handle_update(&mut self, id: ExprId, v: Value) {
        if id == self.source {
            self.set(v)
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
