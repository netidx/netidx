use super::{default_view, FromGui, RawBatch, ToGui, ViewLoc, WidgetPath};
use crate::{util::OneShot, view};
use anyhow::{anyhow, Error, Result};
use futures::{
    channel::{mpsc, oneshot},
    future::{pending, FutureExt},
    select_biased,
    stream::StreamExt,
};
use glib;
use log::{info, warn};
use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    pool::{Pool, Pooled},
    resolver::{Auth, ResolverRead},
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags, Value},
};
use netidx_protocols::{rpc::client as rpc, view as protocol_view};
use std::{
    collections::HashMap,
    fs, mem,
    path::PathBuf,
    result,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::{runtime::Runtime, task};

lazy_static! {
    static ref UPDATES: Pool<Vec<(SubId, Value)>> = Pool::new(5, 100000);
}

macro_rules! break_err {
    ($e:expr) => {
        match $e {
            Ok(x) => x,
            Err(_) => break,
        }
    };
}

#[derive(Clone, Debug)]
pub(crate) struct Ctx {
    pub(crate) subscriber: Subscriber,
    pub(crate) resolver: ResolverRead,
    pub(crate) to_gui: glib::Sender<ToGui>,
    pub(crate) from_gui: mpsc::UnboundedSender<FromGui>,
    pub(crate) updates: mpsc::Sender<RawBatch>,
}

impl Ctx {
    pub(crate) fn navigate(&self, loc: ViewLoc) {
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::Navigate(loc));
    }

    pub(crate) async fn save(
        &self,
        loc: ViewLoc,
        spec: protocol_view::View,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::Save(loc, spec, tx));
        Ok(rx.await??)
    }

    pub(crate) fn terminate(&self) {
        let _: result::Result<_, _> = self.from_gui.unbounded_send(FromGui::Terminate);
    }

    pub(crate) fn updated(&self) {
        let _: result::Result<_, _> = self.from_gui.unbounded_send(FromGui::Updated);
    }

    pub(crate) fn render(&self, spec: protocol_view::View) {
        let _: result::Result<_, _> = self.from_gui.unbounded_send(FromGui::Render(spec));
    }

    pub(crate) fn highlight(&self, paths: Vec<WidgetPath>) {
        let _: result::Result<_, _> = self.to_gui.send(ToGui::Highlight(paths));
    }
}

#[derive(Debug)]
struct CtxInner {
    subscriber: Subscriber,
    resolver: ResolverRead,
    updates: mpsc::Receiver<RawBatch>,
    from_gui: mpsc::UnboundedReceiver<FromGui>,
    to_gui: glib::Sender<ToGui>,
    raw_view: Arc<AtomicBool>,
    view_path: Option<Path>,
    rx_view: Option<mpsc::Receiver<RawBatch>>,
    dv_view: Option<Dval>,
    rpcs: HashMap<Path, (Instant, rpc::Proc)>,
    last_rpc_gc: Instant,
    changed: Pooled<Vec<(SubId, Value)>>,
    refreshing: bool,
}

impl CtxInner {
    fn new(
        subscriber: Subscriber,
        to_gui: glib::Sender<ToGui>,
        raw_view: Arc<AtomicBool>,
    ) -> Ctx {
        let (tx_updates, rx_updates) = mpsc::channel(2);
        let (tx_from_gui, rx_from_gui) = mpsc::unbounded();
        let inner = CtxInner {
            subscriber: subscriber.clone(),
            resolver: subscriber.resolver(),
            updates: rx_updates,
            from_gui: rx_from_gui,
            to_gui: to_gui.clone(),
            raw_view,
            view_path: None,
            rx_view: None,
            dv_view: None,
            rpcs: HashMap::new(),
            last_rpc_gc: Instant::now(),
            changed: UPDATES.take(),
            refreshing: false,
        };
        task::spawn(inner.run());
        Ctx {
            resolver: subscriber.resolver(),
            subscriber,
            to_gui,
            from_gui: tx_from_gui,
            updates: tx_updates,
        }
    }

    async fn navigate_path(&mut self, base_path: Path) -> Result<()> {
        self.rx_view = None;
        self.dv_view = None;
        match self.resolver.table(base_path.clone()).await {
            Err(e) => {
                let m = format!("can't fetch table spec for {}, {}", base_path, e);
                self.to_gui.send(ToGui::ShowError(m))?
            }
            Ok(spec) => {
                let raeified_default = view::View {
                    variables: HashMap::new(),
                    root: view::Widget {
                        props: None,
                        kind: view::WidgetKind::Table(
                            view::Table {
                                path: base_path.clone(),
                                default_sort_column: None,
                                columns: view::ColumnSpec::Auto,
                            },
                            spec,
                        ),
                    },
                };
                let raw = self.raw_view.load(Ordering::Relaxed);
                let m = ToGui::View {
                    loc: Some(ViewLoc::Netidx(base_path.clone())),
                    original: default_view(base_path.clone()),
                    raeified: raeified_default,
                    generated: true,
                };
                self.to_gui.send(m)?;
                if !raw {
                    let s = self.subscriber.durable_subscribe(base_path.append(".view"));
                    let (tx, rx) = mpsc::channel(2);
                    s.updates(UpdatesFlags::BEGIN_WITH_LAST, tx);
                    self.view_path = Some(base_path.clone());
                    self.rx_view = Some(rx);
                    self.dv_view = Some(s);
                }
            }
        }
        Ok(())
    }

    async fn navigate_file(&mut self, file: PathBuf) -> Result<()> {
        self.rx_view = None;
        self.dv_view = None;
        match fs::read_to_string(&file) {
            Err(e) => {
                let m = format!("can't load view from file {:?}, {}", file, e);
                self.to_gui.send(ToGui::ShowError(m))?;
            }
            Ok(s) => match serde_json::from_str::<protocol_view::View>(&s) {
                Err(e) => {
                    let m = format!("invalid view: {:?}, {}", file, e);
                    self.to_gui.send(ToGui::ShowError(m))?;
                }
                Ok(v) => match view::View::new(&self.resolver, v.clone()).await {
                    Err(e) => {
                        let m = format!("error building view: {}", e);
                        self.to_gui.send(ToGui::ShowError(m))?;
                    }
                    Ok(r) => {
                        let m = ToGui::View {
                            loc: Some(ViewLoc::File(file)),
                            original: v,
                            raeified: r,
                            generated: false,
                        };
                        self.to_gui.send(m)?;
                    }
                },
            },
        }
        Ok(())
    }

    async fn save_view_netidx(
        &self,
        path: Path,
        view: protocol_view::View,
        fin: oneshot::Sender<Result<()>>,
    ) {
        let to = Some(Duration::from_secs(10));
        match self.subscriber.subscribe_one(path, to).await {
            Err(e) => {
                let _ = fin.send(Err(e));
            }
            Ok(val) => match serde_json::to_string(&view) {
                Err(e) => {
                    let _ = fin.send(Err(Error::from(e)));
                }
                Ok(s) => {
                    let v = Value::String(Chars::from(s));
                    match val.write_with_recipt(v).await {
                        Err(e) => {
                            let _ = fin.send(Err(Error::from(e)));
                        }
                        Ok(v) => {
                            let _ = fin.send(match v {
                                Value::Error(s) => Err(anyhow!(String::from(&*s))),
                                _ => Ok(()),
                            });
                        }
                    }
                }
            },
        }
    }

    fn save_view_file(
        &self,
        file: PathBuf,
        view: protocol_view::View,
        fin: oneshot::Sender<Result<()>>,
    ) {
        match serde_json::to_string(&view) {
            Err(e) => {
                let _ = fin.send(Err(Error::from(e)));
            }
            Ok(s) => match fs::write(file, s) {
                Err(e) => {
                    let _ = fin.send(Err(Error::from(e)));
                }
                Ok(()) => {
                    let _ = fin.send(Ok(()));
                }
            },
        }
    }

    async fn load_custom_view(&mut self, view: Option<RawBatch>) -> Result<()> {
        match view {
            None => {
                self.view_path = None;
                self.rx_view = None;
                self.dv_view = None;
            }
            Some(mut batch) => {
                if let Some((_, view)) = batch.pop() {
                    match view {
                        Event::Update(Value::String(s)) => {
                            match serde_json::from_str::<protocol_view::View>(&*s) {
                                Err(e) => warn!("error parsing view definition {}", e),
                                Ok(view) => {
                                    if let Some(path) = &self.view_path {
                                        match view::View::new(
                                            &self.resolver,
                                            view.clone(),
                                        )
                                        .await
                                        {
                                            Err(e) => {
                                                warn!("failed to raeify view {}", e)
                                            }
                                            Ok(v) => {
                                                let m = ToGui::View {
                                                    loc: Some(ViewLoc::Netidx(
                                                        path.clone(),
                                                    )),
                                                    original: view,
                                                    raeified: v,
                                                    generated: false,
                                                };
                                                self.to_gui.send(m)?;
                                                info!("updated gui view")
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        v => warn!("unexpected type of view definition {:?}", v),
                    }
                }
            }
        }
        Ok(())
    }

    async fn render_view(&mut self, view: protocol_view::View) -> Result<()> {
        self.view_path = None;
        self.rx_view = None;
        self.dv_view = None;
        match view::View::new(&self.resolver, view.clone()).await {
            Err(e) => warn!("failed to raeify view {}", e),
            Ok(v) => {
                let m = ToGui::View {
                    loc: None,
                    original: view,
                    raeified: v,
                    generated: false,
                };
                self.to_gui.send(m)?;
                info!("updated gui view (render)")
            }
        }
        Ok(())
    }

    fn process_updates(&mut self, mut batch: RawBatch) -> Result<()> {
        for (id, ev) in batch.drain(..) {
            match ev {
                Event::Update(v) => self.changed.push((id, v)),
                Event::Unsubscribed => {
                    self.changed.push((id, Value::String(Chars::from("#LOST"))))
                }
            }
        }
        self.refresh()
    }

    fn refresh(&mut self) -> Result<()> {
        if !self.refreshing && !self.changed.is_empty() {
            self.refreshing = true;
            self.to_gui
                .send(ToGui::Update(mem::replace(&mut self.changed, UPDATES.take())))?
        }
        Ok(())
    }

    async fn run(mut self) {
        async fn read_view(
            rx_view: &mut Option<mpsc::Receiver<RawBatch>>,
        ) -> Option<RawBatch> {
            match rx_view {
                None => pending().await,
                Some(rx_view) => rx_view.next().await,
            }
        }
        loop {
            select_biased! {
                m = self.from_gui.next() => match m {
                    None => break,
                    Some(FromGui::Terminate) => break,
                    Some(FromGui::Updated) => {
                        self.refreshing = false;
                        break_err!(self.refresh())
                    },
                    Some(FromGui::Render(view)) => {
                        break_err!(self.render_view(view).await)
                    },
                    Some(FromGui::Save(ViewLoc::Netidx(path), view, fin)) =>
                        self.save_view_netidx(path, view, fin).await,
                    Some(FromGui::Save(ViewLoc::File(file), view, fin)) => {
                        self.save_view_file(file, view, fin)
                    },
                    Some(FromGui::Navigate(ViewLoc::Netidx(path))) =>
                        break_err!(self.navigate_path(path).await),
                    Some(FromGui::Navigate(ViewLoc::File(file))) =>
                        break_err!(self.navigate_file(file).await),
                },
                b = self.updates.next() => if let Some(batch) = b {
                    break_err!(self.process_updates(batch))
                },
                m = read_view(&mut self.rx_view).fuse() => {
                    break_err!(self.load_custom_view(m).await)
                },
            }
        }
        let _: result::Result<_, _> = self.to_gui.send(ToGui::Terminate);
    }
}

enum ToBackend {
    CreateCtx {
        to_gui: glib::Sender<ToGui>,
        raw_view: Arc<AtomicBool>,
        reply: OneShot<Ctx>,
    },
}

pub(crate) struct Backend(mpsc::UnboundedSender<ToBackend>);

impl Backend {
    pub(crate) fn new(cfg: Config, auth: Auth) -> (thread::JoinHandle<()>, Backend) {
        let (tx_create_ctx, mut rx_create_ctx) = mpsc::unbounded();
        let join_handle = {
            thread::spawn(move || {
                let rt = Runtime::new().expect("failed to create tokio runtime");
                rt.block_on(async move {
                    let sub = Subscriber::new(cfg, auth).unwrap();
                    while let Some(ToBackend::CreateCtx { to_gui, raw_view, reply }) =
                        rx_create_ctx.next().await
                    {
                        reply.send(CtxInner::new(sub.clone(), to_gui, raw_view))
                    }
                });
            })
        };
        (join_handle, Backend(tx_create_ctx))
    }

    pub(crate) fn create_ctx(
        &self,
        to_gui: glib::Sender<ToGui>,
        raw_view: Arc<AtomicBool>,
    ) -> Result<Ctx> {
        let reply = OneShot::new();
        self.0.unbounded_send(ToBackend::CreateCtx {
            to_gui,
            raw_view,
            reply: reply.clone(),
        })?;
        Ok(reply.wait())
    }
}
