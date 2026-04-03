use crate::{view, ToGui, ViewLoc};
use anyhow::{anyhow, Error, Result};
use arcstr::ArcStr;
use futures::{
    channel::{mpsc, oneshot},
    future::pending,
    select_biased,
    stream::StreamExt,
    FutureExt,
};
use graphix_compiler::{
    BindId, ExecCtx,
};
use graphix_rt::{
    GXConfig, GXEvent, GXHandle, GXRt, NoExt,
};
use log::{info, warn};
use netidx::{
    config::Config,
    path::Path,
    publisher::{Publisher, PublisherBuilder, Value},
    resolver_client::DesiredAuth,
    subscriber::{Dval, Event, Subscriber, UpdatesFlags},
};
use netidx::pool::global::GPooled;
use std::{
    fs,
    path::PathBuf,
    result,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use tokio::{
    runtime::Runtime,
    sync::mpsc as tmpsc,
    task,
};

type RawBatch = GPooled<Vec<(netidx::subscriber::SubId, Event)>>;

/// Messages from the GUI thread to the backend
#[derive(Debug)]
pub(crate) enum FromGui {
    Navigate(ViewLoc),
    Save(ViewLoc, view::Widget, oneshot::Sender<Result<()>>),
    Render(view::Widget),
    ResolveTable(Path),
    Terminate,
}

/// Per-window context shared with the GUI thread.
/// The GUI uses this to communicate with the backend and the graphix runtime.
#[derive(Clone)]
pub(crate) struct Ctx {
    pub(crate) gx: GXHandle<NoExt>,
    pub(crate) to_gui: glib::Sender<ToGui>,
    pub(crate) from_gui: mpsc::UnboundedSender<FromGui>,
    pub(crate) rt_handle: tokio::runtime::Handle,
    pub(crate) subscriber: Subscriber,
}

impl Ctx {
    pub(crate) fn navigate(&self, loc: ViewLoc) {
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::Navigate(loc));
    }

    pub(crate) async fn save(
        &self,
        loc: ViewLoc,
        spec: view::Widget,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::Save(loc, spec, tx));
        Ok(rx.await??)
    }

    pub(crate) fn terminate(&self) {
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::Terminate);
    }

    pub(crate) fn render(&self, spec: view::Widget) {
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::Render(spec));
    }

    pub(crate) fn resolve_table(&self, path: Path) {
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::ResolveTable(path));
    }


    /// Compile a graphix expression synchronously from the GTK thread.
    /// This blocks the GTK main loop briefly for compilation only.
    pub(crate) fn compile(
        &self,
        text: &str,
    ) -> Result<graphix_rt::CompRes<NoExt>> {
        self.rt_handle.block_on(self.gx.compile(ArcStr::from(text)))
    }

    /// Compile a callable expression synchronously from the GTK thread.
    pub(crate) fn compile_callable(
        &self,
        id: Value,
    ) -> Result<graphix_rt::Callable<NoExt>> {
        self.rt_handle.block_on(self.gx.compile_callable(id))
    }

    /// Set a variable in the graphix runtime
    pub(crate) fn set_var(&self, id: BindId, v: Value) -> Result<()> {
        self.gx.set(id, v)
    }
}

/// Inner backend state running in the tokio runtime.
/// Handles navigation (view loading) and save operations.
/// Subscription updates, RPCs, timers are all handled by graphix.
struct BackendInner {
    subscriber: Subscriber,
    from_gui: mpsc::UnboundedReceiver<FromGui>,
    to_gui: glib::Sender<ToGui>,
    raw_view: Arc<AtomicBool>,
    view_path: Option<Path>,
    rx_view: Option<futures::channel::mpsc::Receiver<RawBatch>>,
    dv_view: Option<Dval>,
}

impl BackendInner {
    async fn navigate_path(&mut self, base_path: Path) -> Result<()> {
        self.rx_view = None;
        self.dv_view = None;
        let m = ToGui::View {
            loc: Some(ViewLoc::Netidx(base_path.clone())),
            spec: crate::default_view(base_path.clone()),
            generated: true,
        };
        self.to_gui.send(m)?;
        if !self.raw_view.load(Ordering::Relaxed) {
            let s = self.subscriber.subscribe(base_path.append(".view"));
            let (tx, rx) = futures::channel::mpsc::channel(2);
            s.updates(UpdatesFlags::BEGIN_WITH_LAST, tx);
            self.view_path = Some(base_path);
            self.rx_view = Some(rx);
            self.dv_view = Some(s);
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
            Ok(s) => match serde_json::from_str::<view::Widget>(&s) {
                Err(e) => {
                    let m = format!("invalid view: {:?}, {}", file, e);
                    self.to_gui.send(ToGui::ShowError(m))?;
                }
                Ok(v) => {
                    let m = ToGui::View {
                        loc: Some(ViewLoc::File(file)),
                        spec: v,
                        generated: false,
                    };
                    self.to_gui.send(m)?;
                }
            },
        }
        Ok(())
    }

    fn save_view_netidx(
        &self,
        path: Path,
        spec: view::Widget,
        fin: oneshot::Sender<Result<()>>,
    ) {
        let subscriber = self.subscriber.clone();
        task::spawn(async move {
            let to = Some(Duration::from_secs(10));
            match subscriber.subscribe_nondurable_one(path, to).await {
                Err(e) => {
                    let _ = fin.send(Err(e));
                }
                Ok(val) => match serde_json::to_string(&spec) {
                    Err(e) => {
                        let _ = fin.send(Err(Error::from(e)));
                    }
                    Ok(s) => {
                        let v = Value::String(ArcStr::from(s.as_str()));
                        match val.write_with_recipt(v).await {
                            Err(e) => {
                                let _ = fin.send(Err(Error::from(e)));
                            }
                            Ok(v) => {
                                let _ = fin.send(match v {
                                    Value::Error(s) => {
                                        Err(anyhow!("{}", s))
                                    }
                                    _ => Ok(()),
                                });
                            }
                        }
                    }
                },
            }
        });
    }

    fn save_view_file(
        file: PathBuf,
        spec: view::Widget,
        fin: oneshot::Sender<Result<()>>,
    ) {
        task::spawn(async move {
            match serde_json::to_string(&spec) {
                Err(e) => {
                    let _ = fin.send(Err(Error::from(e)));
                }
                Ok(s) => match task::block_in_place(|| fs::write(file, s)) {
                    Err(e) => {
                        let _ = fin.send(Err(Error::from(e)));
                    }
                    Ok(()) => {
                        let _ = fin.send(Ok(()));
                    }
                },
            }
        });
    }

    fn load_custom_view(&mut self, view: Option<RawBatch>) -> Result<()> {
        match view {
            None => {
                self.view_path = None;
                self.rx_view = None;
                self.dv_view = None;
            }
            Some(mut batch) => {
                for (_, view) in batch.drain(..) {
                    match view {
                        Event::Update(Value::String(s)) => {
                            match serde_json::from_str::<view::Widget>(&*s) {
                                Err(e) => warn!(
                                    "error parsing view definition {}", e
                                ),
                                Ok(spec) => {
                                    if let Some(path) = &self.view_path {
                                        let m = ToGui::View {
                                            loc: Some(ViewLoc::Netidx(
                                                path.clone(),
                                            )),
                                            spec,
                                            generated: false,
                                        };
                                        self.to_gui.send(m)?;
                                        info!("updated gui view")
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

    fn render_view(&mut self, spec: view::Widget) -> Result<()> {
        self.view_path = None;
        self.rx_view = None;
        self.dv_view = None;
        let m = ToGui::View { loc: None, spec, generated: false };
        self.to_gui.send(m)?;
        info!("updated gui view (render)");
        Ok(())
    }

    fn resolve_table(&self, path: Path) {
        let resolver = self.subscriber.resolver();
        let to_gui = self.to_gui.clone();
        task::spawn(async move {
            let table = match resolver.table(path.clone()).await {
                Ok(table) => table,
                Err(e) => {
                    warn!("failed to resolve table {}, {}", path, e);
                    netidx_netproto::resolver::Table {
                        rows: netidx::pool::global::GPooled::orphan(vec![]),
                        cols: netidx::pool::global::GPooled::orphan(vec![]),
                    }
                }
            };
            let _: result::Result<_, _> =
                to_gui.send(ToGui::TableResolved(path, table));
        });
    }

    async fn run(mut self) {
        async fn read_view(
            rx_view: &mut Option<futures::channel::mpsc::Receiver<RawBatch>>,
        ) -> Option<RawBatch> {
            match rx_view {
                None => pending().await,
                Some(rx) => rx.next().await,
            }
        }
        loop {
            select_biased! {
                m = self.from_gui.next() => match m {
                    None => break,
                    Some(FromGui::Terminate) => break,
                    Some(FromGui::Render(spec)) => {
                        if let Err(e) = self.render_view(spec) {
                            warn!("render_view error: {e}");
                            break;
                        }
                    }
                    Some(FromGui::Save(ViewLoc::Netidx(path), spec, fin)) => {
                        self.save_view_netidx(path, spec, fin)
                    }
                    Some(FromGui::Save(ViewLoc::File(file), spec, fin)) => {
                        Self::save_view_file(file, spec, fin)
                    }
                    Some(FromGui::Navigate(ViewLoc::Netidx(path))) => {
                        if let Err(e) = self.navigate_path(path).await {
                            warn!("navigate error: {e}");
                            break;
                        }
                    }
                    Some(FromGui::Navigate(ViewLoc::File(file))) => {
                        if let Err(e) = self.navigate_file(file).await {
                            warn!("navigate error: {e}");
                            break;
                        }
                    }
                    Some(FromGui::ResolveTable(path)) => {
                        self.resolve_table(path);
                    }
                },
                m = read_view(&mut self.rx_view).fuse() => {
                    if let Err(e) = self.load_custom_view(m) {
                        warn!("load_custom_view error: {e}");
                        break;
                    }
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
        reply: crate::util::OneShot<Result<Ctx>>,
    },
    Stop,
}

#[derive(Clone)]
pub(crate) struct Backend {
    tx: mpsc::UnboundedSender<ToBackend>,
    rt_handle: tokio::runtime::Handle,
}

impl Backend {
    pub(crate) fn new(
        cfg: Config,
        auth: DesiredAuth,
    ) -> (thread::JoinHandle<()>, Backend) {
        let (tx, mut rx) = mpsc::unbounded();
        let (rt_handle_tx, rt_handle_rx) = std::sync::mpsc::channel();
        let join_handle = thread::spawn(move || {
            let rt = Runtime::new().expect("failed to create tokio runtime");
            let handle = rt.handle().clone();
            rt_handle_tx.send(handle).unwrap();
            rt.block_on(async move {
                let subscriber =
                    Subscriber::new(cfg.clone(), auth.clone()).unwrap();
                let publisher = PublisherBuilder::new(cfg)
                    .desired_auth(auth)
                    .build()
                    .await
                    .unwrap();
                while let Some(m) = rx.next().await {
                    match m {
                        ToBackend::Stop => break,
                        ToBackend::CreateCtx { to_gui, raw_view, reply } => {
                            let res = Self::create_ctx_inner(
                                subscriber.clone(),
                                publisher.clone(),
                                to_gui,
                                raw_view,
                            )
                            .await;
                            reply.send(res);
                        }
                    }
                }
            });
        });
        let rt_handle = rt_handle_rx.recv().unwrap();
        (join_handle, Backend { tx, rt_handle })
    }

    async fn create_ctx_inner(
        subscriber: Subscriber,
        publisher: Publisher,
        to_gui: glib::Sender<ToGui>,
        raw_view: Arc<AtomicBool>,
    ) -> Result<Ctx> {
        // Set up the graphix runtime
        let gxrt = GXRt::<NoExt>::new(publisher, subscriber.clone());
        let mut ctx = ExecCtx::new(gxrt)?;
        // Register browser-specific builtins
        crate::builtins::register_builtins(&mut ctx)?;
        // Set up shared state for builtins
        ctx.libstate.set(crate::builtins::BrowserLibState {
            to_gui: to_gui.clone(),
        });

        // Channel for graphix output events
        let (gx_tx, mut gx_rx) = tmpsc::channel(100);

        // Start the graphix runtime with browser prelude types
        let prelude = arcstr::literal!(include_str!("browser_prelude.gx"));
        let gx = GXConfig::builder(ctx, gx_tx)
            .root(prelude)
            .build()?
            .start()
            .await?;

        // Bridge: forward graphix events to the GTK main loop
        let to_gui_bridge = to_gui.clone();
        task::spawn(async move {
            while let Some(batch) = gx_rx.recv().await {
                let to_gui = to_gui_bridge.clone();
                let updates: Vec<_> = batch
                    .iter()
                    .filter_map(|ev| match ev {
                        GXEvent::Updated(id, v) => Some((*id, v.clone())),
                        GXEvent::Env(_) => None,
                    })
                    .collect();
                if !updates.is_empty() {
                    glib::idle_add_once(move || {
                        let _ = to_gui.send(ToGui::Update(updates));
                    });
                }
            }
        });

        // Channel for navigation/save requests
        let (tx_from_gui, rx_from_gui) = mpsc::unbounded();

        // Spawn the backend inner task (handles navigation and saves)
        let inner = BackendInner {
            subscriber: subscriber.clone(),
            from_gui: rx_from_gui,
            to_gui: to_gui.clone(),
            raw_view,
            view_path: None,
            rx_view: None,
            dv_view: None,
        };
        task::spawn(inner.run());

        let rt_handle = tokio::runtime::Handle::current();
        Ok(Ctx {
            gx,
            to_gui,
            from_gui: tx_from_gui,
            rt_handle,
            subscriber,
        })
    }

    pub(crate) fn stop(&self) {
        let _: result::Result<_, _> = self.tx.unbounded_send(ToBackend::Stop);
    }

    pub(crate) fn create_ctx(
        &self,
        to_gui: glib::Sender<ToGui>,
        raw_view: Arc<AtomicBool>,
    ) -> Result<Ctx> {
        let reply = crate::util::OneShot::new();
        self.tx.unbounded_send(ToBackend::CreateCtx {
            to_gui,
            raw_view,
            reply: reply.clone(),
        })?;
        reply.wait()
    }
}
