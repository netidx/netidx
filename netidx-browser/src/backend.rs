use crate::{BrowserEvent, ViewLoc};
use anyhow::{anyhow, Error, Result};
use arcstr::ArcStr;
use futures::{
    channel::{mpsc, oneshot},
    future::pending,
    select_biased,
    stream::StreamExt,
    FutureExt,
};
use graphix_compiler::{BindId, ExecCtx};
use graphix_package::Package;
use graphix_rt::{GXConfig, GXEvent, GXHandle, GXRt, NoExt};
use log::{info, warn};
use netidx::pool::global::GPooled;
use netidx::{
    config::Config,
    path::Path,
    publisher::{Publisher, PublisherBuilder, Value},
    resolver_client::DesiredAuth,
    subscriber::{Dval, Event, Subscriber, UpdatesFlags},
};
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
use tokio::{runtime::Runtime, sync::mpsc as tmpsc, task};
use winit::event_loop::EventLoopProxy;

type RawBatch = GPooled<Vec<(netidx::subscriber::SubId, Event)>>;

/// Messages from the GUI thread to the backend
#[derive(Debug)]
pub(crate) enum FromGui {
    Navigate(ViewLoc),
    Save(ViewLoc, ArcStr, oneshot::Sender<Result<()>>),
    Render(ArcStr),
    Terminate,
}

/// Per-window context shared with the GUI thread.
#[derive(Clone)]
pub(crate) struct Ctx {
    pub(crate) gx: GXHandle<NoExt>,
    pub(crate) proxy: EventLoopProxy<BrowserEvent>,
    pub(crate) from_gui: mpsc::UnboundedSender<FromGui>,
    pub(crate) rt_handle: tokio::runtime::Handle,
    pub(crate) subscriber: Subscriber,
    current_path_bid: graphix_compiler::BindId,
    pub(crate) debug_highlighted_bid: graphix_compiler::BindId,
}

impl Ctx {
    /// Create a new backend context. Starts a tokio runtime on a background
    /// thread, creates Subscriber, graphix runtime, and bridges events
    /// to the winit event loop via the proxy.
    pub(crate) fn new(
        cfg: Config,
        auth: DesiredAuth,
        proxy: EventLoopProxy<BrowserEvent>,
    ) -> Result<Self> {
        let (ctx_tx, ctx_rx) = std::sync::mpsc::channel();
        let proxy_clone = proxy.clone();
        thread::spawn(move || {
            let rt = Runtime::new().expect("failed to create tokio runtime");
            rt.block_on(async move {
                match Self::init_inner(cfg, auth, proxy_clone).await {
                    Ok(ctx) => {
                        let _ = ctx_tx.send(Ok(ctx));
                        // Keep runtime alive as long as graphix needs it
                        pending::<()>().await;
                    }
                    Err(e) => {
                        let _ = ctx_tx.send(Err(e));
                    }
                }
            });
        });
        ctx_rx.recv().map_err(|_| anyhow!("backend thread died"))?
    }

    async fn init_inner(
        cfg: Config,
        auth: DesiredAuth,
        proxy: EventLoopProxy<BrowserEvent>,
    ) -> Result<Self> {
        let subscriber = Subscriber::new(cfg.clone(), auth.clone())?;
        let publisher =
            PublisherBuilder::new(cfg).desired_auth(auth).build().await?;

        // Set up the graphix runtime
        let gxrt = GXRt::<NoExt>::new(publisher, subscriber.clone());
        let mut ctx = ExecCtx::new(gxrt)?;

        // Register browser-specific builtins
        crate::builtins::register_builtins(&mut ctx)?;

        // Set up shared state for builtins
        ctx.libstate.set(crate::builtins::BrowserLibState {
            proxy: proxy.clone(),
        });

        // Channel for graphix output events
        let (gx_tx, mut gx_rx) = tmpsc::channel(100);

        // Register packages
        let mut vfs = fxhash::FxHashMap::default();
        let mut root_mods = graphix_package::IndexSet::new();
        graphix_package_core::P::register(&mut ctx, &mut vfs, &mut root_mods)?;
        graphix_package_sys::P::register(&mut ctx, &mut vfs, &mut root_mods)?;
        graphix_package_gui::P::register(&mut ctx, &mut vfs, &mut root_mods)?;

        // Set up the browser package as a VFS module so views can `use browser;`
        vfs.insert(
            Path::from("/browser/mod.gx"),
            arcstr::literal!(include_str!("graphix/mod.gx")),
        );
        vfs.insert(
            Path::from("/browser/mod.gxi"),
            arcstr::literal!(include_str!("graphix/mod.gxi")),
        );
        let resolvers = vec![graphix_compiler::expr::ModuleResolver::VFS(vfs)];

        // Build root text: registered packages + browser + current_path
        let mut root_parts = Vec::new();
        for name in &root_mods {
            if name == "core" {
                root_parts.push(format!("mod core;\nuse core"));
            } else {
                root_parts.push(format!("mod {name}"));
            }
        }
        root_parts.push("mod browser".to_string());
        let root = ArcStr::from(format!(
            "{};\nlet current_path = \"/\";\nlet debug_highlighted = 0;",
            root_parts.join(";\n"),
        ));
        let gx = GXConfig::builder(ctx, gx_tx)
            .root(root)
            .resolvers(resolvers)
            .build()?
            .start()
            .await?;

        // Compile a ref to current_path so we can set it from Rust on navigation
        let env = gx.get_env().await?;
        let scope = graphix_compiler::Scope::root();
        let name = graphix_compiler::expr::ModPath::from(["current_path"]);
        let current_path_ref = gx.compile_ref_by_name(&env, &scope, &name).await?;
        let current_path_bid = current_path_ref.bid;
        std::mem::forget(current_path_ref);

        let dh_name = graphix_compiler::expr::ModPath::from(["debug_highlighted"]);
        let dh_ref = gx.compile_ref_by_name(&env, &scope, &dh_name).await?;
        let debug_highlighted_bid = dh_ref.bid;
        std::mem::forget(dh_ref);

        // Bridge: forward graphix events to the winit event loop
        let proxy_bridge = proxy.clone();
        task::spawn(async move {
            while let Some(batch) = gx_rx.recv().await {
                let mut updates = Vec::new();
                let mut latest_env = None;
                for ev in batch.iter() {
                    match ev {
                        GXEvent::Updated(id, v) => updates.push((*id, v.clone())),
                        GXEvent::Env(e) => latest_env = Some(e.clone()),
                    }
                }
                if !updates.is_empty() {
                    let _ = proxy_bridge.send_event(BrowserEvent::Update(updates));
                }
                if let Some(env) = latest_env {
                    let _ = proxy_bridge.send_event(BrowserEvent::EnvUpdate(env));
                }
            }
        });

        // Channel for navigation/save requests
        let (tx_from_gui, rx_from_gui) = mpsc::unbounded();

        // Spawn the backend inner task (handles navigation and saves)
        let inner = BackendInner {
            subscriber: subscriber.clone(),
            from_gui: rx_from_gui,
            proxy: proxy.clone(),
            raw_view: Arc::new(AtomicBool::new(false)),
            view_path: None,
            rx_view: None,
            dv_view: None,
        };
        task::spawn(inner.run());

        let rt_handle = tokio::runtime::Handle::current();
        Ok(Ctx {
            gx,
            proxy,
            from_gui: tx_from_gui,
            rt_handle,
            subscriber,
            current_path_bid,
            debug_highlighted_bid,
        })
    }

    /// Update the current_path graphix variable on navigation
    pub(crate) fn set_current_path(&self, loc: &ViewLoc) {
        let path_str = match loc {
            ViewLoc::Netidx(p) => ArcStr::from(&**p),
            ViewLoc::File(f) => ArcStr::from(format!("file:{}", f.display()).as_str()),
        };
        let _ = self.gx.set(self.current_path_bid, Value::String(path_str));
    }

    pub(crate) fn navigate(&self, loc: ViewLoc) {
        log::info!("Ctx::navigate({:?})", loc);
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::Navigate(loc));
    }

    pub(crate) async fn save(&self, loc: ViewLoc, source: ArcStr) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::Save(loc, source, tx));
        Ok(rx.await??)
    }

    pub(crate) fn terminate(&self) {
        let _: result::Result<_, _> = self.from_gui.unbounded_send(FromGui::Terminate);
    }

    pub(crate) fn render(&self, source: ArcStr) {
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::Render(source));
    }

    /// Compile a graphix expression synchronously.
    pub(crate) fn compile(&self, text: &str) -> Result<graphix_rt::CompRes<NoExt>> {
        self.rt_handle.block_on(self.gx.compile(ArcStr::from(text)))
    }

    /// Compile a callable expression synchronously.
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
struct BackendInner {
    subscriber: Subscriber,
    from_gui: mpsc::UnboundedReceiver<FromGui>,
    proxy: EventLoopProxy<BrowserEvent>,
    raw_view: Arc<AtomicBool>,
    view_path: Option<Path>,
    rx_view: Option<futures::channel::mpsc::Receiver<RawBatch>>,
    dv_view: Option<Dval>,
}

impl BackendInner {
    async fn navigate_path(&mut self, base_path: Path) -> Result<()> {
        log::info!("BackendInner::navigate_path({})", base_path);
        self.rx_view = None;
        self.dv_view = None;
        let source = crate::default_view_source(&base_path);
        log::info!("default_view_source generated {} bytes", source.len());
        let _ = self.proxy.send_event(BrowserEvent::View {
            loc: Some(ViewLoc::Netidx(base_path.clone())),
            source,
            generated: true,
        });
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
                let _ = self.proxy.send_event(BrowserEvent::ShowError(m));
            }
            Ok(s) => {
                let _ = self.proxy.send_event(BrowserEvent::View {
                    loc: Some(ViewLoc::File(file)),
                    source: ArcStr::from(s.as_str()),
                    generated: false,
                });
            }
        }
        Ok(())
    }

    fn save_view_netidx(
        &self,
        path: Path,
        source: ArcStr,
        fin: oneshot::Sender<Result<()>>,
    ) {
        let subscriber = self.subscriber.clone();
        task::spawn(async move {
            let to = Some(Duration::from_secs(10));
            match subscriber.subscribe_nondurable_one(path, to).await {
                Err(e) => {
                    let _ = fin.send(Err(e));
                }
                Ok(val) => {
                    let v = Value::String(source);
                    match val.write_with_recipt(v).await {
                        Err(e) => {
                            let _ = fin.send(Err(Error::from(e)));
                        }
                        Ok(v) => {
                            let _ = fin.send(match v {
                                Value::Error(s) => Err(anyhow!("{}", s)),
                                _ => Ok(()),
                            });
                        }
                    }
                }
            }
        });
    }

    fn save_view_file(file: PathBuf, source: ArcStr, fin: oneshot::Sender<Result<()>>) {
        task::spawn(async move {
            match task::block_in_place(|| fs::write(file, source.as_str())) {
                Err(e) => {
                    let _ = fin.send(Err(Error::from(e)));
                }
                Ok(()) => {
                    let _ = fin.send(Ok(()));
                }
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
                            if let Some(path) = &self.view_path {
                                let _ = self.proxy.send_event(BrowserEvent::View {
                                    loc: Some(ViewLoc::Netidx(path.clone())),
                                    source: s,
                                    generated: false,
                                });
                                info!("updated gui view")
                            }
                        }
                        v => warn!("unexpected type of view definition {:?}", v),
                    }
                }
            }
        }
        Ok(())
    }

    fn render_view(&mut self, source: ArcStr) -> Result<()> {
        self.view_path = None;
        self.rx_view = None;
        self.dv_view = None;
        let _ = self.proxy.send_event(BrowserEvent::View {
            loc: None,
            source,
            generated: false,
        });
        info!("updated gui view (render)");
        Ok(())
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
                    Some(FromGui::Render(source)) => {
                        if let Err(e) = self.render_view(source) {
                            warn!("render_view error: {e}");
                            break;
                        }
                    }
                    Some(FromGui::Save(ViewLoc::Netidx(path), source, fin)) => {
                        self.save_view_netidx(path, source, fin)
                    }
                    Some(FromGui::Save(ViewLoc::File(file), source, fin)) => {
                        Self::save_view_file(file, source, fin)
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
                },
                m = read_view(&mut self.rx_view).fuse() => {
                    if let Err(e) = self.load_custom_view(m) {
                        warn!("load_custom_view error: {e}");
                        break;
                    }
                },
            }
        }
        let _ = self.proxy.send_event(BrowserEvent::Terminate);
    }
}
