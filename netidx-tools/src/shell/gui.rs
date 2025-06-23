use anyhow::Result;
use arcstr::ArcStr;
use async_trait::async_trait;
use compact_str::CompactString;
use crossterm::event::{Event, EventStream};
use futures::{channel::mpsc, StreamExt};
use log::error;
use netidx::{protocol::value::NakedValue, publisher::Value};
use netidx_bscript::{expr::ExprId, rt::BSHandle};
use ratatui::{prelude::CrosstermBackend, text::Text, DefaultTerminal, Frame, Terminal};
use smallvec::SmallVec;
use tokio::{select, sync::oneshot, task};

enum ToGui {
    Update(ExprId, Value),
    Stop(oneshot::Sender<()>),
}

pub(super) struct Gui(mpsc::Sender<ToGui>);

impl Gui {
    pub(super) fn start(bs: &BSHandle, root: ExprId) -> Gui {
        let bs = bs.clone();
        let (tx, rx) = mpsc::channel(3);
        task::spawn(async move {
            if let Err(e) = run(bs, root, rx).await {
                error!("gui::run returned {e:?}")
            }
        });
        Self(tx)
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
    text: CompactString,
    source: ExprId,
}

impl TextW {
    async fn new(bs: &BSHandle, source: Value) -> Result<Self> {
        let id = match &source.cast_to::<SmallVec<[(ArcStr, u64); 1]>>()?[..] {
            [(s, id)] if &*s == "text" => *id,
            _ => bail!("expected struct {{ text: &Any }}"),
        };
        let (source, current) = bs.compile_ref(Value::U64(id)).await?;
        let mut t = Self { bs: bs.clone(), text: CompactString::from(""), source };
        if let Some(v) = current {
            t.set(v)
        }
        Ok(t)
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

async fn compile(bs: BSHandle, root: Value) -> Result<GuiW> {
    todo!()
}

async fn run(
    bs: BSHandle,
    root_expr: ExprId,
    mut rx: mpsc::Receiver<ToGui>,
) -> Result<()> {
    let mut terminal = ratatui::init();
    let mut events = EventStream::new().fuse();
    let mut root: GuiW = Box::new(EmptyW);
    loop {
        terminal.draw(|f| root.draw(f))?;
        select! {
            m = rx.next() => match m {
                None | Some(ToGui::Stop(_)) => break,
                Some(ToGui::Update(id, v)) => {
                    if id == root_expr {

                    }
                },
            },
            e = events.select_next_some() => todo!()
        }
    }
    ratatui::restore();
    Ok(())
}
