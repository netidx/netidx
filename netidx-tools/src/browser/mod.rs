mod combobox;
mod container;
mod grid;
mod table;
mod toggle;
mod view;
use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    future::{pending, FutureExt},
    select_biased,
    stream::StreamExt,
};
use gdk::{self, prelude::*};
use gio::prelude::*;
use glib::{self, clone, signal::Inhibit, source::PRIORITY_LOW};
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
use std::{
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
    to_gui: mpsc::UnboundedSender<ToGui>,
    from_gui: mpsc::UnboundedSender<FromGui>,
}

#[derive(Debug, Clone)]
enum Source {
    Constant(Value),
    Load(Dval),
    Variable(String, Rc<RefCell<Value>>),
}

impl Source {
    fn new(
        ctx: &WidgetCtx,
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
        }
    }

    fn current(&self) -> Value {
        match self {
            Source::Constant(v) => v.clone(),
            Source::Variable(_, v) => v.borrow().clone(),
            Source::Load(dv) => match dv.last() {
                None => Value::String(Chars::from("#SUB")),
                Some(v) => v.clone(),
            },
        }
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) -> Option<Value> {
        match self {
            Source::Constant(_) | Source::Variable(_, _) => None,
            Source::Load(dv) => changed.get(&dv.id()).cloned(),
        }
    }

    fn update_var(&self, name: &str, value: &Value) -> bool {
        match self {
            Source::Load(_) | Source::Constant(_) => false,
            Source::Variable(our_name, cur) => {
                if name == our_name {
                    *cur.borrow_mut() = value.clone();
                    true
                } else {
                    false
                }
            }
        }
    }
}

#[derive(Clone)]
enum Sink {
    Store(Dval),
    Variable(String),
}

impl Sink {
    fn new(ctx: &WidgetCtx, spec: view::Sink) -> Self {
        match spec {
            view::Sink::Variable(name) => Sink::Variable(name),
            view::Sink::Store(path) => {
                Sink::Store(ctx.subscriber.durable_subscribe(path))
            }
        }
    }

    fn set(&self, ctx: &WidgetCtx, v: Value) {
        match self {
            Sink::Store(dv) => {
                dv.write(v);
            }
            Sink::Variable(name) => {
                let _: result::Result<_, _> =
                    ctx.to_gui.unbounded_send(ToGui::UpdateVar(name.clone(), v));
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

struct Label {
    label: gtk::Label,
    source: Source,
}

impl Label {
    fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Source,
        selected_path: gtk::Label,
    ) -> Label {
        let source = Source::new(&ctx, variables, spec.clone());
        let label = gtk::Label::new(Some(&format!("{}", source.current())));
        label.set_selectable(true);
        label.set_single_line_mode(true);
        label.connect_button_press_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(&format!("{:?}", spec));
                Inhibit(false)
            }),
        );
        label.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(&format!("{:?}", spec));
            Inhibit(false)
        }));
        Label { source, label }
    }

    fn root(&self) -> &gtk::Widget {
        self.label.upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.source.update(changed) {
            self.label.set_label(&format!("{}", new));
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if self.source.update_var(name, value) {
            self.label.set_label(&format!("{}", value));
        }
    }
}

struct Button {
    enabled: Source,
    label: Source,
    source: Source,
    button: gtk::Button,
}

impl Button {
    fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Button,
        selected_path: gtk::Label,
    ) -> Self {
        let button = gtk::Button::new();
        let enabled = Source::new(&ctx, variables, spec.enabled.clone());
        let label = Source::new(&ctx, variables, spec.label.clone());
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        button.set_sensitive(val_to_bool(&enabled.current()));
        button.set_label(&format!("{}", label.current()));
        button.connect_clicked(clone!(@strong ctx, @strong source, @strong sink =>
        move |_| {
            sink.set(&ctx, source.current());
        }));
        button.connect_focus(clone!(@strong selected_path, @strong spec => move |_, _| {
            selected_path.set_label(
                &format!("source: {:?}, sink: {:?}", spec.source, spec.sink)
            );
            Inhibit(false)
        }));
        button.connect_enter_notify_event(
            clone!(@strong selected_path, @strong spec => move |_, _| {
                selected_path.set_label(
                    &format!("source: {:?}, sink: {:?}", spec.source, spec.sink)
                );
                Inhibit(false)
            }),
        );
        Button { enabled, label, source, button }
    }

    fn root(&self) -> &gtk::Widget {
        self.button.upcast_ref()
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.enabled.update(changed) {
            self.button.set_sensitive(val_to_bool(&new));
        }
        if let Some(new) = self.label.update(changed) {
            self.button.set_label(&format!("{}", new));
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if self.enabled.update_var(name, value) {
            self.button.set_sensitive(val_to_bool(value));
        }
        if self.label.update_var(name, value) {
            self.button.set_label(&format!("{}", value));
        }
        self.source.update_var(name, value);
    }
}

struct Action {
    source: Source,
    sink: Sink,
    ctx: WidgetCtx,
}

impl Action {
    fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Action,
    ) -> Self {
        let source = Source::new(&ctx, variables, spec.source.clone());
        let sink = Sink::new(&ctx, spec.sink.clone());
        Action { source, sink, ctx }
    }

    fn update(&self, changed: &Arc<IndexMap<SubId, Value>>) {
        if let Some(new) = self.source.update(changed) {
            self.sink.set(&self.ctx, new);
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        if self.source.update_var(name, value) {
            self.sink.set(&self.ctx, value.clone())
        }
    }
}

enum Widget {
    Table(table::Table),
    Label(Label),
    Action(Action),
    Button(Button),
    Toggle(toggle::Toggle),
    ComboBox(combobox::ComboBox),
    Container(container::Container),
    Grid(grid::Grid),
}

impl Widget {
    fn new(
        ctx: WidgetCtx,
        variables: &HashMap<String, Value>,
        spec: view::Widget,
        selected_path: gtk::Label,
    ) -> Widget {
        match spec {
            view::Widget::StaticTable(_) => todo!(),
            view::Widget::Table(base_path, spec) => Widget::Table(table::Table::new(
                ctx.clone(),
                base_path,
                spec,
                selected_path,
            )),
            view::Widget::Label(spec) => {
                Widget::Label(Label::new(ctx.clone(), variables, spec, selected_path))
            }
            view::Widget::Action(spec) => {
                Widget::Action(Action::new(ctx.clone(), variables, spec))
            }
            view::Widget::Button(spec) => {
                Widget::Button(Button::new(ctx.clone(), variables, spec, selected_path))
            }
            view::Widget::Toggle(spec) => Widget::Toggle(toggle::Toggle::new(
                ctx.clone(),
                variables,
                spec,
                selected_path,
            )),
            view::Widget::ComboBox(spec) => Widget::ComboBox(combobox::ComboBox::new(
                ctx.clone(),
                variables,
                spec,
                selected_path,
            )),
            view::Widget::Radio(_) => todo!(),
            view::Widget::Entry(_) => todo!(),
            view::Widget::Container(s) => Widget::Container(container::Container::new(
                ctx,
                variables,
                s,
                selected_path,
            )),
            view::Widget::Grid(spec) => {
                Widget::Grid(grid::Grid::new(ctx, variables, spec, selected_path))
            }
        }
    }

    fn root(&self) -> Option<&gtk::Widget> {
        match self {
            Widget::Table(t) => Some(t.root()),
            Widget::Label(t) => Some(t.root()),
            Widget::Action(_) => None,
            Widget::Button(t) => Some(t.root()),
            Widget::Toggle(t) => Some(t.root()),
            Widget::ComboBox(t) => Some(t.root()),
            Widget::Container(t) => Some(t.root()),
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
            Widget::Action(t) => t.update(changed),
            Widget::Button(t) => t.update(changed),
            Widget::Toggle(t) => t.update(changed),
            Widget::ComboBox(t) => t.update(changed),
            Widget::Container(t) => t.update(waits, changed),
            Widget::Grid(t) => t.update(waits, changed),
        }
    }

    fn update_var(&self, name: &str, value: &Value) {
        match self {
            Widget::Table(_) => (),
            Widget::Label(t) => t.update_var(name, value),
            Widget::Action(t) => t.update_var(name, value),
            Widget::Button(t) => t.update_var(name, value),
            Widget::Toggle(t) => t.update_var(name, value),
            Widget::ComboBox(t) => t.update_var(name, value),
            Widget::Container(t) => t.update_var(name, value),
            Widget::Grid(t) => t.update_var(name, value),
        }
    }
}

struct View {
    root: gtk::Box,
    widget: Widget,
}

impl View {
    fn new(ctx: WidgetCtx, spec: view::View) -> View {
        let selected_path = gtk::Label::new(None);
        selected_path.set_halign(gtk::Align::Start);
        selected_path.set_margin_start(0);
        selected_path.set_selectable(true);
        selected_path.set_single_line_mode(true);
        let selected_path_window =
            gtk::ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
        selected_path_window.add(&selected_path);
        let widget =
            Widget::new(ctx, &spec.variables, spec.root.clone(), selected_path.clone());
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        if let Some(wroot) = widget.root() {
            root.add(wroot);
            root.set_child_packing(wroot, true, true, 1, gtk::PackType::Start);
        }
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
    let mut refreshing = false;
    fn refresh(
        refreshing: &mut bool,
        changed: &mut IndexMap<SubId, Value>,
        to_gui: &mpsc::UnboundedSender<ToGui>,
    ) -> Result<()> {
        if !*refreshing && changed.len() > 0 {
            *refreshing = true;
            let b = Arc::new(mem::replace(changed, IndexMap::new()));
            to_gui.unbounded_send(ToGui::Update(b))?
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
        let mut waits = Vec::new();
        while let Some(m) = to_gui.next().await {
            match m {
                ToGui::Update(batch) => {
                    if let Some(root) = &current {
                        root.update(&mut waits, &batch);
                        for r in waits.drain(..) {
                            let _: result::Result<_, _> = r.await;
                        }
                        let _: result::Result<_, _> =
                            ctx.from_gui.unbounded_send(FromGui::Updated);
                    }
                }
                ToGui::View(path, view) => {
                    let cur = View::new(ctx.clone(), view);
                    if let Some(cur) = current.take() {
                        window.remove(cur.root());
                    }
                    window.set_title(&format!("Netidx Browser {}", path));
                    window.add(cur.root());
                    window.show_all();
                    current = Some(cur);
                    let m = ToGui::Update(Arc::new(IndexMap::new()));
                    let _: result::Result<_, _> = ctx.to_gui.unbounded_send(m);
                }
                ToGui::UpdateVar(name, value) => {
                    if let Some(root) = &current {
                        root.update_var(&name, &value);
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
