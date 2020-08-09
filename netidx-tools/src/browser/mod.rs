mod containers;
mod script;
mod table;
mod view;
mod widgets;
use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    future::{pending, FutureExt},
    select_biased,
    stream::StreamExt,
};
use gdk::{self, prelude::*};
use gio::prelude::*;
use glib::{self, clone, source::PRIORITY_LOW};
use gluon::{vm::api::function::FunctionRef, RootedThread, ThreadExt};
use gtk::{self, prelude::*, Adjustment, Application, ApplicationWindow};
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
use once_cell::unsync::Lazy;
use std::{
    boxed,
    cell::RefCell,
    collections::HashMap,
    mem, process,
    rc::Rc,
    result,
    sync::{mpsc as smpsc, Arc},
    thread,
};
use tokio::runtime::Runtime;

type Batch = Pooled<Vec<(SubId, Value)>>;

#[derive(Debug, Clone)]
enum ToGui {
    View(Path, view::View),
    Update(Arc<IndexMap<SubId, Value>>),
    UpdateVar(String, Value),
}

#[derive(Debug, Clone)]
enum FromGui {
    Navigate(Path),
    Updated,
}

#[derive(Debug, Clone)]
struct WidgetCtx {
    subscriber: Subscriber,
    resolver: ResolverRead,
    updates: mpsc::Sender<Batch>,
    state_updates: mpsc::UnboundedSender<(SubId, DvState)>,
    to_gui: glib::Sender<ToGui>,
    from_gui: mpsc::UnboundedSender<FromGui>,
}

fn call_gluon_function(vm: &RootedThread, f: &str, v: Value) -> Option<Value> {
    use script::GluVal as V;
    match vm.get_global::<FunctionRef<fn(V) -> Option<V>>>(f) {
        Err(e) => {
            warn!("no such function {} matching type Value -> Option Value, {}", f, e);
            None
        }
        Ok(mut code) => match code.call(script::GluVal(v)) {
            Ok(gv) => gv.map(|v| v.0),
            Err(e) => {
                warn!("error calling function {}, {}", f, e);
                None
            }
        },
    }
}

#[derive(Debug, Clone)]
enum Source {
    Constant(Value),
    Load(Dval),
    Variable(String, Rc<RefCell<Value>>),
    Any(Vec<Source>),
    Map { vm: Rc<Lazy<RootedThread>>, from: boxed::Box<Source>, function: String },
}

impl Source {
    fn new(
        ctx: &WidgetCtx,
        vm: &Rc<Lazy<RootedThread>>,
        variables: &HashMap<String, Value>,
        spec: view::Source,
    ) -> Self {
        match spec {
            view::Source::Constant(v) => Source::Constant(v),
            view::Source::Load(path) => {
                let dv = ctx.subscriber.durable_subscribe(path);
                dv.updates(true, ctx.updates.clone());
                dv.state_updates(true, ctx.state_updates.clone());
                Source::Load(dv)
            }
            view::Source::Variable(name) => {
                let v = variables.get(&name).cloned().unwrap_or(Value::Null);
                Source::Variable(name, Rc::new(RefCell::new(v)))
            }
            view::Source::Any(srcs) => Source::Any(
                srcs.into_iter()
                    .map(|spec| Source::new(ctx, vm, variables, spec))
                    .collect(),
            ),
            view::Source::Map { from, function } => Source::Map {
                vm: vm.clone(),
                from: boxed::Box::new(Source::new(ctx, vm, variables, (*from).clone())),
                function,
            },
        }
    }

    fn current(&self) -> Option<Value> {
        match self {
            Source::Constant(v) => Some(v.clone()),
            Source::Load(dv) => dv.last(),
            Source::Variable(_, v) => Some(v.borrow().clone()),
            Source::Any(srcs) => srcs.iter().find_map(|src| src.current()),
            Source::Map { vm, from, function } => {
                from.current().and_then(|v| call_gluon_function(&**vm, function, v))
            }
        }
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) -> Option<Value> {
        match self {
            Source::Constant(_) | Source::Variable(_, _) => None,
            Source::Load(dv) => changed.get(&dv.id()).cloned(),
            Source::Any(srcs) => srcs.iter().find_map(|s| s.update(changed)),
            Source::Map { vm, from, function } => {
                from.update(changed).and_then(|v| call_gluon_function(&**vm, function, v))
            }
        }
    }

    fn update_var(&self, name: &str, value: &Value) -> Option<Value> {
        match self {
            Source::Load(_) | Source::Constant(_) => None,
            Source::Variable(our_name, cur) => {
                if name == our_name {
                    *cur.borrow_mut() = value.clone();
                    Some(value.clone())
                } else {
                    None
                }
            }
            Source::Any(srcs) => srcs.iter().find_map(|s| s.update_var(name, value)),
            Source::Map { vm, from, function } => from
                .update_var(name, value)
                .and_then(|v| call_gluon_function(&**vm, function, v)),
        }
    }
}

#[derive(Clone)]
enum Sink {
    Store(Dval),
    Variable(String),
    All(Vec<Sink>),
    Map { vm: Rc<Lazy<RootedThread>>, to: boxed::Box<Sink>, function: String },
}

impl Sink {
    fn new(ctx: &WidgetCtx, vm: &Rc<Lazy<RootedThread>>, spec: view::Sink) -> Self {
        match spec {
            view::Sink::Variable(name) => Sink::Variable(name),
            view::Sink::Store(path) => {
                Sink::Store(ctx.subscriber.durable_subscribe(path))
            }
            view::Sink::All(sinks) => Sink::All(
                sinks.into_iter().map(|spec| Sink::new(ctx, vm, spec)).collect(),
            ),
            view::Sink::Map { to, function } => Sink::Map {
                vm: vm.clone(),
                to: boxed::Box::new(Sink::new(ctx, vm, (*to).clone())),
                function,
            },
        }
    }

    fn set(&self, ctx: &WidgetCtx, v: Value) {
        match self {
            Sink::Store(dv) => {
                dv.write(v);
            }
            Sink::Variable(name) => {
                let _: result::Result<_, _> =
                    ctx.to_gui.send(ToGui::UpdateVar(name.clone(), v));
            }
            Sink::All(sinks) => {
                for sink in sinks {
                    sink.set(ctx, v.clone())
                }
            }
            Sink::Map { vm, to, function: f } => {
                if let Some(v) = call_gluon_function(&**vm, f, v.clone()) {
                    to.set(ctx, v)
                }
            }
        }
    }
}

fn val_to_bool(v: &Value) -> bool {
    match v {
        Value::False | Value::Null => false,
        _ => true,
    }
}

fn align_to_gtk(a: view::Align) -> gtk::Align {
    match a {
        view::Align::Fill => gtk::Align::Fill,
        view::Align::Start => gtk::Align::Start,
        view::Align::End => gtk::Align::End,
        view::Align::Center => gtk::Align::Center,
        view::Align::Baseline => gtk::Align::Baseline,
    }
}

enum Widget {
    Table(table::Table),
    Label(widgets::Label),
    Button(widgets::Button),
    Toggle(widgets::Toggle),
    Selector(widgets::Selector),
    Entry(widgets::Entry),
    Box(containers::Box),
    Grid(containers::Grid),
}

impl Widget {
    fn new(
        ctx: WidgetCtx,
        vm: &Rc<Lazy<RootedThread>>,
        variables: &HashMap<String, Value>,
        spec: view::Widget,
        selected_path: gtk::Label,
    ) -> Widget {
        match spec {
            view::Widget::Table(base_path, spec) => Widget::Table(table::Table::new(
                ctx.clone(),
                base_path,
                spec,
                selected_path,
            )),
            view::Widget::Label(spec) => Widget::Label(widgets::Label::new(
                ctx.clone(),
                vm,
                variables,
                spec,
                selected_path,
            )),
            view::Widget::Button(spec) => Widget::Button(widgets::Button::new(
                ctx.clone(),
                vm,
                variables,
                spec,
                selected_path,
            )),
            view::Widget::Toggle(spec) => Widget::Toggle(widgets::Toggle::new(
                ctx.clone(),
                vm,
                variables,
                spec,
                selected_path,
            )),
            view::Widget::Selector(spec) => Widget::Selector(widgets::Selector::new(
                ctx.clone(),
                vm,
                variables,
                spec,
                selected_path,
            )),
            view::Widget::Entry(spec) => Widget::Entry(widgets::Entry::new(
                ctx.clone(),
                vm,
                variables,
                spec,
                selected_path,
            )),
            view::Widget::Box(s) => {
                Widget::Box(containers::Box::new(ctx, vm, variables, s, selected_path))
            }
            view::Widget::Grid(spec) => Widget::Grid(containers::Grid::new(
                ctx,
                vm,
                variables,
                spec,
                selected_path,
            )),
        }
    }

    fn root(&self) -> Option<&gtk::Widget> {
        match self {
            Widget::Table(t) => Some(t.root()),
            Widget::Label(t) => Some(t.root()),
            Widget::Button(t) => Some(t.root()),
            Widget::Toggle(t) => Some(t.root()),
            Widget::Selector(t) => Some(t.root()),
            Widget::Entry(t) => Some(t.root()),
            Widget::Box(t) => Some(t.root()),
            Widget::Grid(t) => Some(t.root()),
        }
    }

    fn update<'a, 'b: 'a>(
        &'a self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        changed: &'b Arc<IndexMap<SubId, Value>>,
    ) {
        match self {
            Widget::Table(t) => t.update(waits, changed),
            Widget::Label(t) => t.update(changed),
            Widget::Button(t) => t.update(changed),
            Widget::Toggle(t) => t.update(changed),
            Widget::Selector(t) => t.update(changed),
            Widget::Entry(t) => t.update(changed),
            Widget::Box(t) => t.update(waits, changed),
            Widget::Grid(t) => t.update(waits, changed),
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        match self {
            Widget::Table(_) => (),
            Widget::Label(t) => t.update_var(name, value),
            Widget::Button(t) => t.update_var(name, value),
            Widget::Toggle(t) => t.update_var(name, value),
            Widget::Selector(t) => t.update_var(name, value),
            Widget::Entry(t) => t.update_var(name, value),
            Widget::Box(t) => t.update_var(name, value),
            Widget::Grid(t) => t.update_var(name, value),
        }
    }
}

fn make_crumbs(ctx: &WidgetCtx, path: &Path) -> gtk::Box {
    let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
    for target in Path::dirnames(&path) {
        let name = match Path::basename(target) {
            None => " / ",
            Some(name) => {
                root.add(&gtk::Label::new(Some(" > ")));
                name
            }
        };
        let target = glib::markup_escape_text(target);
        let lbl = gtk::Label::new(None);
        lbl.set_markup(&format!(r#"<a href="{}">{}</a>"#, &*target, &*name));
        lbl.connect_activate_link(clone!(@strong ctx => move |_, uri| {
            let m = FromGui::Navigate(Path::from(String::from(uri)));
            let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
            Inhibit(false)
        }));
        root.add(&lbl);
    }
    root
}

struct View {
    root: gtk::Box,
    widget: Widget,
}

impl View {
    fn new(ctx: WidgetCtx, path: &Path, spec: view::View) -> View {
        let vm = {
            let f: fn() -> RootedThread = script::new;
            Rc::new(Lazy::new(f))
        };
        for (module, script) in &spec.scripts {
            match base64::decode(script) {
                Err(e) => {
                    warn!("failed to base64 decode the script module {}, {}", module, e)
                }
                Ok(octets) => match String::from_utf8(octets) {
                    Err(e) => warn!("script module {} is not unicode {}", module, e),
                    Ok(script) => match (***vm).load_script(module, script.as_str()) {
                        Ok(()) => (),
                        Err(e) => warn!("compile error module {}, {}", module, e),
                    },
                },
            }
        }
        let selected_path = gtk::Label::new(None);
        selected_path.set_halign(gtk::Align::Start);
        selected_path.set_margin_start(0);
        selected_path.set_selectable(true);
        selected_path.set_single_line_mode(true);
        let selected_path_window =
            gtk::ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
        selected_path_window
            .set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Never);
        selected_path_window.add(&selected_path);
        let widget = Widget::new(
            ctx.clone(),
            &vm,
            &spec.variables,
            spec.root.clone(),
            selected_path.clone(),
        );
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        root.add(&make_crumbs(&ctx, path));
        root.add(&gtk::Separator::new(gtk::Orientation::Horizontal));
        if let Some(wroot) = widget.root() {
            root.add(wroot);
            root.set_child_packing(wroot, true, true, 1, gtk::PackType::Start);
        }
        root.add(&gtk::Separator::new(gtk::Orientation::Horizontal));
        root.add(&selected_path_window);
        root.set_child_packing(&selected_path, false, false, 1, gtk::PackType::End);
        View { root, widget }
    }

    fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    fn update(
        &self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        changed: &Arc<IndexMap<SubId, Value>>,
    ) {
        self.widget.update(waits, changed);
    }

    fn update_var(&self, name: &str, value: &Value) {
        self.widget.update_var(name, value)
    }
}

macro_rules! break_err {
    ($e:expr) => {
        match $e {
            Ok(x) => x,
            Err(_) => break,
        }
    };
}

async fn netidx_main(
    cfg: Config,
    auth: Auth,
    mut updates: mpsc::Receiver<Batch>,
    mut state_updates: mpsc::UnboundedReceiver<(SubId, DvState)>,
    mut from_gui: mpsc::UnboundedReceiver<FromGui>,
    to_gui: glib::Sender<ToGui>,
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
    let mut refreshing = false;
    fn refresh(
        refreshing: &mut bool,
        changed: &mut IndexMap<SubId, Value>,
        to_gui: &glib::Sender<ToGui>,
    ) -> Result<()> {
        if !*refreshing && changed.len() > 0 {
            *refreshing = true;
            let b = Arc::new(mem::replace(changed, IndexMap::new()));
            to_gui.send(ToGui::Update(b))?
        }
        Ok(())
    }
    loop {
        select_biased! {
            b = updates.next() => if let Some(mut batch) = b {
                for (id, v) in batch.drain(..) {
                    changed.insert(id, v);
                }
                break_err!(refresh(&mut refreshing, &mut changed, &to_gui))
            },
            s = state_updates.next() => if let Some((id, st)) = s {
                match st {
                    DvState::Subscribed => (),
                    DvState::Unsubscribed => {
                        changed.insert(id, Value::String(Chars::from("#SUB")));
                        break_err!(refresh(&mut refreshing, &mut changed, &to_gui))
                    }
                    DvState::FatalError(_) => {
                        changed.insert(id, Value::String(Chars::from("#ERR")));
                        break_err!(refresh(&mut refreshing, &mut changed, &to_gui))
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
                                            match to_gui.send(m) {
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
                Some(FromGui::Updated) => {
                    refreshing = false;
                    break_err!(refresh(&mut refreshing, &mut changed, &to_gui))
                },
                Some(FromGui::Navigate(base_path)) => {
                    match resolver.table(base_path.clone()).await {
                        Err(e) => warn!("can't fetch table spec for {}, {}", base_path, e),
                        Ok(spec) => {
                            let default = view::View {
                                variables: HashMap::new(),
                                scripts: Vec::new(),
                                root: view::Widget::Table(base_path.clone(), spec)
                            };
                            let m = ToGui::View(base_path.clone(), default);
                            match to_gui.send(m) {
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
    to_gui: glib::Sender<ToGui>,
) -> (Subscriber, ResolverRead) {
    let (tx, rx) = smpsc::channel();
    thread::spawn(move || {
        let mut rt = Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(netidx_main(cfg, auth, updates, state_updates, from_gui, to_gui, tx));
    });
    rx.recv().unwrap()
}

fn run_gui(ctx: WidgetCtx, app: &Application, to_gui: glib::Receiver<ToGui>) {
    let app = app.clone();
    let window = ApplicationWindow::new(&app);
    window.set_title("Netidx browser");
    window.set_default_size(800, 600);
    window.show_all();
    window.connect_destroy(move |_| process::exit(0));
    let mut current: Option<View> = None;
    to_gui.attach(None, move |m| {
        match m {
            ToGui::Update(batch) => {
                if let Some(root) = &mut current {
                    let mut waits = Vec::new();
                    root.update(&mut waits, &batch);
                    if waits.len() == 0 {
                        let _: result::Result<_, _> =
                            ctx.from_gui.unbounded_send(FromGui::Updated);
                    } else {
                        let from_gui = ctx.from_gui.clone();
                        glib::MainContext::default().spawn_local(async move {
                            for r in waits {
                                let _: result::Result<_, _> = r.await;
                            }
                            let _: result::Result<_, _> =
                                from_gui.unbounded_send(FromGui::Updated);
                        });
                    }
                }
            }
            ToGui::View(path, view) => {
                let cur = View::new(ctx.clone(), &path, view);
                if let Some(cur) = current.take() {
                    window.remove(cur.root());
                }
                window.set_title(&format!("Netidx Browser {}", path));
                window.add(cur.root());
                window.show_all();
                current = Some(cur);
                let m = ToGui::Update(Arc::new(IndexMap::new()));
                let _: result::Result<_, _> = ctx.to_gui.send(m);
            }
            ToGui::UpdateVar(name, value) => {
                if let Some(root) = &mut current {
                    root.update_var(&name, &value);
                }
            }
        }
        Continue(true)
    });
}

pub(crate) fn run(cfg: Config, auth: Auth, path: Path) {
    let application = Application::new(Some("org.netidx.browser"), Default::default())
        .expect("failed to initialize GTK application");
    application.connect_activate(move |app| {
        let (tx_updates, rx_updates) = mpsc::channel(2);
        let (tx_state_updates, rx_state_updates) = mpsc::unbounded();
        let (tx_to_gui, rx_to_gui) = glib::MainContext::channel(PRIORITY_LOW);
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
