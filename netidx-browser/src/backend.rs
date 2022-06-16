use super::{default_view, FromGui, RawBatch, ToGui, ViewLoc, WidgetPath};
use crate::util::OneShot;
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
    protocol::resolver,
    resolver_client::{DesiredAuth, ResolverRead},
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags, Value},
};
use netidx_bscript::vm::{RpcCallId, TimerId};
use netidx_protocols::{rpc::client as rpc, view};
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
use tokio::{runtime::Runtime, task, time};

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
    pub(crate) to_gui: glib::Sender<ToGui>,
    pub(crate) from_gui: mpsc::UnboundedSender<FromGui>,
    pub(crate) updates: mpsc::Sender<RawBatch>,
}

impl Ctx {
    pub(crate) fn navigate(&self, loc: ViewLoc) {
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::Navigate(loc));
    }

    pub(crate) async fn save(&self, loc: ViewLoc, spec: view::Widget) -> Result<()> {
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

    pub(crate) fn render(&self, spec: view::Widget) {
        let _: result::Result<_, _> = self.from_gui.unbounded_send(FromGui::Render(spec));
    }

    pub(crate) fn set_timer(&self, id: TimerId, timeout: Duration) {
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::SetTimer(id, timeout));
    }

    pub(crate) fn resolve_table(&self, path: Path) {
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::ResolveTable(path));
    }

    pub(crate) fn call_rpc(&self, name: Path, args: Vec<(Chars, Value)>, id: RpcCallId) {
        let _: result::Result<_, _> =
            self.from_gui.unbounded_send(FromGui::CallRpc(name, args, id));
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
    rpcs:
        HashMap<Path, (Instant, mpsc::UnboundedSender<(Vec<(Chars, Value)>, RpcCallId)>)>,
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
            changed: UPDATES.take(),
            refreshing: false,
        };
        task::spawn(inner.run());
        Ctx { subscriber, to_gui, from_gui: tx_from_gui, updates: tx_updates }
    }

    async fn navigate_path(&mut self, base_path: Path) -> Result<()> {
        self.rx_view = None;
        self.dv_view = None;
        let m = ToGui::View {
            loc: Some(ViewLoc::Netidx(base_path.clone())),
            spec: default_view(base_path.clone()),
            generated: true,
        };
        self.to_gui.send(m)?;
        if !self.raw_view.load(Ordering::Relaxed) {
            let s = self.subscriber.durable_subscribe(base_path.append(".view"));
            let (tx, rx) = mpsc::channel(2);
            s.updates(UpdatesFlags::BEGIN_WITH_LAST, tx);
            self.view_path = Some(base_path.clone());
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

    async fn resolve_table(&self, path: Path) {
        let table = match self.resolver.table(path.clone()).await {
            Ok(table) => table,
            Err(e) => {
                warn!("failed to resolve table {},  {}", path, e);
                resolver::Table {
                    rows: Pooled::orphan(vec![]),
                    cols: Pooled::orphan(vec![]),
                }
            }
        };
        let _: result::Result<_, _> = self.to_gui.send(ToGui::TableResolved(path, table));
    }

    async fn save_view_netidx(
        &self,
        path: Path,
        spec: view::Widget,
        fin: oneshot::Sender<Result<()>>,
    ) {
        let to = Some(Duration::from_secs(10));
        match self.subscriber.subscribe_one(path, to).await {
            Err(e) => {
                let _ = fin.send(Err(e));
            }
            Ok(val) => match serde_json::to_string(&spec) {
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
        spec: view::Widget,
        fin: oneshot::Sender<Result<()>>,
    ) {
        match serde_json::to_string(&spec) {
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
                                Err(e) => warn!("error parsing view definition {}", e),
                                Ok(spec) => {
                                    if let Some(path) = &self.view_path {
                                        let m = ToGui::View {
                                            loc: Some(ViewLoc::Netidx(path.clone())),
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

    fn process_updates(&mut self, mut batch: RawBatch) -> Result<()> {
        for (id, ev) in batch.drain(..) {
            match ev {
                Event::Update(v) => self.changed.push((id, v)),
                Event::Unsubscribed => {
                    self.changed.push((id, Value::Error(Chars::from("#LOST"))))
                }
            }
        }
        self.refresh()
    }

    fn get_rpc_proc(
        &mut self,
        name: &Path,
    ) -> mpsc::UnboundedSender<(Vec<(Chars, Value)>, RpcCallId)> {
        async fn rpc_task(
            to_gui: glib::Sender<ToGui>,
            subscriber: Subscriber,
            name: Path,
            mut rx: mpsc::UnboundedReceiver<(Vec<(Chars, Value)>, RpcCallId)>,
        ) -> Result<()> {
            let proc = rpc::Proc::new(&subscriber, name.clone()).await?;
            while let Some((args, id)) = rx.next().await {
                let res = proc.call(args).await?;
                to_gui.send(ToGui::UpdateRpc(id, res))?
            }
            Ok(())
        }
        match self.rpcs.get_mut(name) {
            Some((ref mut last, ref proc)) => {
                *last = Instant::now();
                proc.clone()
            }
            None => {
                let (tx, rx) = mpsc::unbounded();
                task::spawn({
                    let to_gui = self.to_gui.clone();
                    let sub = self.subscriber.clone();
                    let name = name.clone();
                    async move {
                        let _: Result<_, _> =
                            rpc_task(to_gui.clone(), sub, name.clone(), rx).await;
                    }
                });
                self.rpcs.insert(name.clone(), (Instant::now(), tx.clone()));
                tx
            }
        }
    }

    fn call_rpc(
        &mut self,
        name: Path,
        mut args: Vec<(Chars, Value)>,
        id: RpcCallId,
    ) -> Result<()> {
        for _ in 1..3 {
            let proc = self.get_rpc_proc(&name);
            match proc.unbounded_send((mem::replace(&mut args, vec![]), id)) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    self.rpcs.remove(&name);
                    args = e.into_inner().0;
                }
            }
        }
        let e = Value::Error(Chars::from("failed to call rpc"));
        self.to_gui.send(ToGui::UpdateRpc(id, e))?;
        Ok(())
    }

    fn set_timer(&self, id: TimerId, timeout: Duration) {
        let to_gui = self.to_gui.clone();
        task::spawn(async move {
            time::sleep(timeout).await;
            let _: result::Result<_, _> = to_gui.send(ToGui::UpdateTimer(id));
        });
    }

    fn gc_rpcs(&mut self) {
        static MAX_RPC_AGE: Duration = Duration::from_secs(120);
        let now = Instant::now();
        self.rpcs.retain(|_, (last, _)| now - *last < MAX_RPC_AGE);
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
        async fn read_updates(
            updates: &mut mpsc::Receiver<RawBatch>,
            nchanged: usize,
            refreshing: bool,
        ) -> Option<RawBatch> {
            if nchanged >= 100_000 && refreshing {
                pending().await
            } else {
                updates.next().await
            }
        }
        let mut gc_rpcs = time::interval(Duration::from_secs(60));
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
                        break_err!(self.render_view(view))
                    },
                    Some(FromGui::ResolveTable(path)) =>
                        self.resolve_table(path).await,
                    Some(FromGui::Save(ViewLoc::Netidx(path), view, fin)) =>
                        self.save_view_netidx(path, view, fin).await,
                    Some(FromGui::Save(ViewLoc::File(file), view, fin)) => {
                        self.save_view_file(file, view, fin)
                    },
                    Some(FromGui::Navigate(ViewLoc::Netidx(path))) =>
                        break_err!(self.navigate_path(path).await),
                    Some(FromGui::Navigate(ViewLoc::File(file))) =>
                        break_err!(self.navigate_file(file).await),
                    Some(FromGui::CallRpc(path, args, id)) =>
                        break_err!(self.call_rpc(path, args, id)),
                    Some(FromGui::SetTimer(id, timeout)) => self.set_timer(id, timeout),
                },
                b = read_updates(
                    &mut self.updates,
                    self.changed.len(),
                    self.refreshing
                ).fuse() => {
                    if let Some(batch) = b {
                        break_err!(self.process_updates(batch))
                    }
                },
                m = read_view(&mut self.rx_view).fuse() => {
                    break_err!(self.load_custom_view(m))
                },
                _ = gc_rpcs.tick().fuse() => self.gc_rpcs(),
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
    Stop,
}

#[derive(Clone)]
pub(crate) struct Backend(mpsc::UnboundedSender<ToBackend>);

impl Backend {
    pub(crate) fn new(
        cfg: Config,
        auth: DesiredAuth,
    ) -> (thread::JoinHandle<()>, Backend) {
        let (tx_create_ctx, mut rx_create_ctx) = mpsc::unbounded();
        let join_handle = {
            thread::spawn(move || {
                let rt = Runtime::new().expect("failed to create tokio runtime");
                rt.block_on(async move {
                    let sub = Subscriber::new(cfg, auth).unwrap();
                    while let Some(m) = rx_create_ctx.next().await {
                        match m {
                            ToBackend::Stop => break,
                            ToBackend::CreateCtx { to_gui, raw_view, reply } => {
                                reply.send(CtxInner::new(sub.clone(), to_gui, raw_view))
                            }
                        }
                    }
                });
            })
        };
        (join_handle, Backend(tx_create_ctx))
    }

    pub(crate) fn stop(&self) {
        let _: result::Result<_, _> = self.0.unbounded_send(ToBackend::Stop);
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
