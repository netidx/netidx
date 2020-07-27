mod table;
mod view;
use futures::{
    channel::mpsc,
    future::{join_all, pending},
    prelude::*,
    select_biased,
};
use gdk::keys;
use gio::prelude::*;
use glib::{self, clone, signal::Inhibit, source::PRIORITY_LOW};
use gtk::{
    prelude::*, Application, ApplicationWindow, Box as GtkBox, Orientation,
    Widget as GtkWidget,
};
use indexmap::IndexMap;
use log::{info, warn};
use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    pool::Pooled,
    resolver::{Auth, ResolverRead},
    subscriber::{DvState, Dval, SubId, Subscriber, Value},
};
use netidx_protocols::view as protocol_view;
use std::{
    mem,
    pin::Pin,
    process,
    result,
    sync::mpsc as smpsc,
    sync::Arc,
    thread,
    time::Duration,
};
use table::Table;
use tokio::{runtime::Runtime, time};

type Batch = Pooled<Vec<(SubId, Value)>>;

#[derive(Debug, Clone)]
enum ToGui {
    View(Path, view::View),
    Update(Arc<IndexMap<SubId, Value>>),
}

#[derive(Debug, Clone)]
enum FromGui {
    Navigate(Path),
    Updated,
}

#[derive(Clone)]
struct WidgetCtx {
    subscriber: Subscriber,
    resolver: ResolverRead,
    updates: mpsc::Sender<Batch>,
    state_updates: mpsc::UnboundedSender<(SubId, DvState)>,
    to_gui: mpsc::UnboundedSender<ToGui>,
    from_gui: mpsc::UnboundedSender<FromGui>,
}

struct Container {
    spec: view::Container,
    root: GtkBox,
    children: Vec<Widget>,
}

impl Container {
    fn new(ctx: WidgetCtx, spec: view::Container) -> Container {
        let dir = match spec.direction {
            view::Direction::Horizontal => Orientation::Horizontal,
            view::Direction::Vertical => Orientation::Vertical,
        };
        let root = GtkBox::new(dir, 0);
        let mut children = Vec::new();
        for s in spec.children.iter() {
            let w = Widget::new(ctx.clone(), s.widget.clone());
            if let Some(r) = w.root() {
                root.pack_start(r, s.expand, s.fill, s.padding as u32);
            }
            children.push(w);
        }
        root.connect_key_press_event(clone!(@strong ctx, @strong spec => move |_, k| {
            let target = {
                if k.get_keyval() == keys::constants::BackSpace {
                    &spec.drill_up_target
                } else if k.get_keyval() == keys::constants::Return {
                    &spec.drill_down_target
                } else {
                    &None
                }
            };
            match target {
                None => Inhibit(false),
                Some(target) => {
                    let m = FromGui::Navigate(target.clone());
                    let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
                    Inhibit(true)
                }
            }
        }));
        Container { spec, root, children }
    }

    async fn update(&self, updates: Arc<IndexMap<SubId, Value>>) {
        join_all(self.children.iter().map(|c| c.update(updates.clone()))).await;
    }

    fn root(&self) -> &GtkWidget {
        self.root.upcast_ref()
    }
}

enum Widget {
    Table(Table),
    Container(Container),
}

impl Widget {
    fn new(ctx: WidgetCtx, spec: view::Widget) -> Widget {
        match spec {
            view::Widget::StaticTable(_) => todo!(),
            view::Widget::Table(base_path, spec) => {
                Widget::Table(Table::new(ctx.clone(), base_path, spec))
            }
            view::Widget::Label(_) => todo!(),
            view::Widget::Action(_) => todo!(),
            view::Widget::Button(_) => todo!(),
            view::Widget::Toggle(_) => todo!(),
            view::Widget::ComboBox(_) => todo!(),
            view::Widget::Radio(_) => todo!(),
            view::Widget::Entry(_) => todo!(),
            view::Widget::Container(s) => Widget::Container(Container::new(ctx, s)),
        }
    }

    fn root(&self) -> Option<&GtkWidget> {
        match self {
            Widget::Table(t) => Some(t.root()),
            Widget::Container(t) => Some(t.root()),
        }
    }

    fn update<'a>(
        &'a self,
        changed: Arc<IndexMap<SubId, Value>>,
    ) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async move {
            match self {
                Widget::Table(t) => t.update(changed).await,
                Widget::Container(c) => c.update(changed).await,
            }
        })
    }
}

struct View {
    spec: view::View,
    root: Widget,
}

impl View {
    fn new(ctx: WidgetCtx, spec: view::View) -> View {
        let root = Widget::new(ctx, spec.root.clone());
        View { spec, root }
    }

    fn root(&self) -> Option<&GtkWidget> {
        self.root.root()
    }

    async fn update(&self, changed: Arc<IndexMap<SubId, Value>>) {
        self.root.update(changed).await;
    }
}

async fn netidx_main(
    cfg: Config,
    auth: Auth,
    mut updates: mpsc::Receiver<Batch>,
    mut state_updates: mpsc::UnboundedReceiver<(SubId, DvState)>,
    mut from_gui: mpsc::UnboundedReceiver<FromGui>,
    to_gui: mpsc::UnboundedSender<ToGui>,
    to_init: smpsc::Sender<(Subscriber, ResolverRead)>,
) {
    async fn read_view(rx_view: &mut Option<mpsc::Receiver<Batch>>) -> Option<Batch> {
        match rx_view {
            None => pending().await,
            Some(rx_view) => rx_view.next().await,
        }
    }
    let subscriber = Subscriber::new(cfg, auth).unwrap();
    let resolver = subscriber.resolver();
    let _: result::Result<_, _> = to_init.send((subscriber.clone(), resolver.clone()));
    let mut view_path: Option<Path> = None;
    let mut rx_view: Option<mpsc::Receiver<Batch>> = None;
    let mut _dv_view: Option<Dval> = None;
    let mut changed = IndexMap::new();
    let mut refresh = time::interval(Duration::from_secs(1)).fuse();
    let mut refreshing = false;
    loop {
        select_biased! {
            _ = refresh.next() => {
                if !refreshing && changed.len() > 0 {
                    refreshing = true;
                    let b = Arc::new(mem::replace(&mut changed, IndexMap::new()));
                    match to_gui.unbounded_send(ToGui::Update(b)) {
                        Ok(()) => (),
                        Err(e) => break
                    }
                }
            },
            b = updates.next() => if let Some(mut batch) = b {
                for (id, v) in batch.drain(..) {
                    changed.insert(id, v);
                }
            },
            s = state_updates.next() => if let Some((id, st)) = s {
                match st {
                    DvState::Subscribed => (),
                    DvState::Unsubscribed => {
                        changed.insert(id, Value::String(Chars::from("#SUB")));
                    }
                    DvState::FatalError(_) => {
                        changed.insert(id, Value::String(Chars::from("#ERR")));
                    }
                }
            },
            m = read_view(&mut rx_view).fuse() => match m {
                None => {
                    view_path = None;
                    rx_view = None;
                    _dv_view = None;
                },
                Some(mut batch) => if let Some((_, view)) = batch.pop() {
                    match view {
                        Value::String(s) => {
                            match serde_json::from_str::<protocol_view::View>(&*s) {
                                Err(e) => warn!("error parsing view definition {}", e),
                                Ok(v) => if let Some(path) = &view_path {
                                    match view::View::new(&resolver, v).await {
                                        Err(e) => warn!("failed to raeify view {}", e),
                                        Ok(v) => {
                                            let m = ToGui::View(path.clone(), v);
                                            match to_gui.unbounded_send(m) {
                                                Err(_) => break,
                                                Ok(()) => info!("updated gui view")
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        v => warn!("unexpected type of view definition {}", v),
                    }
                }
            },
            m = from_gui.next() => match m {
                None => break,
                Some(FromGui::Updated) => { refreshing = false; },
                Some(FromGui::Navigate(base_path)) => {
                    match resolver.table(base_path.clone()).await {
                        Err(e) => warn!("can't fetch table spec for {}, {}", base_path, e),
                        Ok(spec) => {
                            let default = view::View {
                                scripts: vec![],
                                root: view::Widget::Table(base_path.clone(), spec)
                            };
                            let m = ToGui::View(base_path.clone(), default);
                            match to_gui.unbounded_send(m) {
                                Err(_) => break,
                                Ok(()) => {
                                    let s = subscriber
                                        .durable_subscribe(base_path.append(".view"));
                                    let (tx, rx) = mpsc::channel(2);
                                    s.updates(true, tx);
                                    view_path = Some(base_path.clone());
                                    rx_view = Some(rx);
                                    _dv_view = Some(s);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn run_netidx(
    cfg: Config,
    auth: Auth,
    updates: mpsc::Receiver<Batch>,
    state_updates: mpsc::UnboundedReceiver<(SubId, DvState)>,
    from_gui: mpsc::UnboundedReceiver<FromGui>,
    to_gui: mpsc::UnboundedSender<ToGui>,
) -> (Subscriber, ResolverRead) {
    let (tx, rx) = smpsc::channel();
    thread::spawn(move || {
        let mut rt = Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(netidx_main(cfg, auth, updates, state_updates, from_gui, to_gui, tx));
    });
    rx.recv().unwrap()
}

fn run_gui(
    ctx: WidgetCtx,
    app: &Application,
    mut to_gui: mpsc::UnboundedReceiver<ToGui>,
) {
    let main_context = glib::MainContext::default();
    let app = app.clone();
    let window = ApplicationWindow::new(&app);
    window.set_title("Netidx browser");
    window.set_default_size(800, 600);
    window.show_all();
    window.connect_destroy(move |_| process::exit(0));
    main_context.spawn_local_with_priority(PRIORITY_LOW, async move {
        let mut current: Option<View> = None;
        while let Some(m) = to_gui.next().await {
            match m {
                ToGui::Update(batch) => {
                    if let Some(root) = &current {
                        root.update(batch).await;
                        let _: result::Result<_, _> =
                            ctx.from_gui.unbounded_send(FromGui::Updated);
                    }
                }
                ToGui::View(path, view) => {
                    let cur = View::new(ctx.clone(), view);
                    if let Some(root) = cur.root() {
                        if let Some(cur) = current.take() {
                            if let Some(r) = cur.root() {
                                window.remove(r);
                            }
                        }
                        window.set_title(&format!("Netidx Browser {}", path));
                        window.add(root);
                        window.show_all();
                        current = Some(cur);
                        let m = ToGui::Update(Arc::new(IndexMap::new()));
                        let _: result::Result<_, _> = ctx.to_gui.unbounded_send(m);
                    }
                }
            }
        }
    })
}

pub(crate) fn run(cfg: Config, auth: Auth, path: Path) {
    let application = Application::new(Some("org.netidx.browser"), Default::default())
        .expect("failed to initialize GTK application");
    application.connect_activate(move |app| {
        let (tx_updates, rx_updates) = mpsc::channel(2);
        let (tx_state_updates, rx_state_updates) = mpsc::unbounded();
        let (tx_to_gui, rx_to_gui) = mpsc::unbounded();
        let (tx_from_gui, rx_from_gui) = mpsc::unbounded();
        // navigate to the initial location
        tx_from_gui.unbounded_send(FromGui::Navigate(path.clone())).unwrap();
        let (subscriber, resolver) = run_netidx(
            cfg.clone(),
            auth.clone(),
            rx_updates,
            rx_state_updates,
            rx_from_gui,
            tx_to_gui.clone(),
        );
        let ctx = WidgetCtx {
            subscriber,
            resolver,
            updates: tx_updates,
            state_updates: tx_state_updates,
            to_gui: tx_to_gui,
            from_gui: tx_from_gui,
        };
        run_gui(ctx, app, rx_to_gui)
    });
    application.run(&[]);
}
