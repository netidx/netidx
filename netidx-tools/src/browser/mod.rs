mod containers;
mod editor;
mod script;
mod table;
mod view;
mod widgets;
use anyhow::{anyhow, Error, Result};
use editor::Editor;
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
use once_cell::{sync::OnceCell, unsync::Lazy};
use std::{
    boxed,
    cell::{Cell, RefCell},
    collections::HashMap,
    fmt, mem,
    rc::Rc,
    result,
    sync::{mpsc as smpsc, Arc},
    thread,
    time::Duration,
};
use tokio::{runtime::Runtime, task};

type Batch = Pooled<Vec<(SubId, Value)>>;

#[derive(Debug, Clone, Copy)]
enum WidgetPath {
    Leaf,
    Box(usize),
    Grid(usize, usize),
}

#[derive(Debug, Clone)]
enum ViewLoc {
    File(String),
    Netidx(Path),
}

impl fmt::Display for ViewLoc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViewLoc::File(s) => write!(f, "file:{}", s),
            ViewLoc::Netidx(s) => write!(f, "netidx:{}", s),
        }
    }
}

#[derive(Debug, Clone)]
enum ToGui {
    View {
        loc: Option<ViewLoc>,
        original: protocol_view::View,
        raeified: view::View,
        generated: bool,
    },
    Highlight(Vec<WidgetPath>),
    Update(Arc<IndexMap<SubId, Value>>),
    UpdateVar(String, Value),
    Terminate,
}

#[derive(Debug)]
enum FromGui {
    Navigate(ViewLoc),
    Render(protocol_view::View),
    Save(ViewLoc, protocol_view::View, oneshot::Sender<Result<()>>),
    Updated,
    Terminate,
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
            view::Widget::BoxChild(view::BoxChild { widget, .. }) => {
                Widget::new(ctx, vm, variables, (&*widget).clone(), selected_path)
            }
            view::Widget::Grid(spec) => Widget::Grid(containers::Grid::new(
                ctx,
                vm,
                variables,
                spec,
                selected_path,
            )),
            view::Widget::GridChild(view::GridChild { widget, .. }) => {
                Widget::new(ctx, vm, variables, (&*widget).clone(), selected_path)
            }
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

    fn set_highlight(&self, mut path: impl Iterator<Item = WidgetPath>, h: bool) {
        fn set<T: WidgetExt>(w: &T, h: bool) {
            let s = w.get_style_context();
            if h {
                s.add_class("highlighted");
            } else {
                s.remove_class("highlighted");
            }
        }
        match path.next() {
            None => (),
            Some(p) => match (p, self) {
                (WidgetPath::Box(i), Widget::Box(w)) => {
                    if i < w.children.len() {
                        w.children[i].set_highlight(path, h)
                    }
                }
                (WidgetPath::Grid(_, _), Widget::Grid(_)) => todo!(),
                (WidgetPath::Box(_), _) => (),
                (WidgetPath::Grid(_, _), _) => (),
                (WidgetPath::Leaf, Widget::Box(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Grid(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Table(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Label(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Button(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Toggle(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Selector(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Entry(w)) => set(w.root(), h),
            },
        }
    }
}

fn toplevel<W: WidgetExt>(w: &W) -> gtk::Window {
    w.get_toplevel()
        .expect("modal dialog must have a toplevel window")
        .downcast::<gtk::Window>()
        .expect("not a window")
}

fn ask_modal<W: WidgetExt>(w: &W, msg: &str) -> bool {
    let d = gtk::MessageDialog::new(
        Some(&toplevel(w)),
        gtk::DialogFlags::MODAL,
        gtk::MessageType::Question,
        gtk::ButtonsType::YesNo,
        msg,
    );
    let resp = d.run();
    d.close();
    resp == gtk::ResponseType::Yes
}

fn make_crumbs(ctx: &WidgetCtx, loc: &ViewLoc, saved: Rc<Cell<bool>>) -> gtk::Box {
    let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
    let ask_saved = Rc::new(clone!(@weak root => @default-return true, move || {
        if !saved.get() {
            ask_modal(&root, "Unsaved custom view will be lost. continue?")
        } else {
            true
        }
    }));
    match loc {
        ViewLoc::Netidx(path) => {
            for target in Path::dirnames(&path) {
                let lbl = gtk::Label::new(None);
                let name = match Path::basename(target) {
                    None => {
                        root.add(&lbl);
                        lbl.set_margin_start(10);
                        " / "
                    }
                    Some(name) => {
                        root.add(&gtk::Label::new(Some(" > ")));
                        root.add(&lbl);
                        name
                    }
                };
                let target = glib::markup_escape_text(target);
                lbl.set_markup(&format!(r#"<a href="{}">{}</a>"#, &*target, &*name));
                lbl.connect_activate_link(clone!(
                @strong ctx,
                @strong ask_saved,
                @weak root => @default-return Inhibit(false), move |_, uri| {
                    if !ask_saved() {
                        return Inhibit(false)
                    }
                    let loc = ViewLoc::Netidx(Path::from(String::from(uri)));
                    let m = FromGui::Navigate(loc);
                    let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
                    Inhibit(false)
                }));
            }
        }
        ViewLoc::File(name) => {
            let rl = gtk::Label::new(None);
            root.add(&rl);
            rl.set_margin_start(10);
            rl.set_markup(r#"<a href=""> / </a>"#);
            rl.connect_activate_link(
                clone!(@strong ctx, @strong ask_saved => move |_, _| {
                    if !ask_saved() {
                        return Inhibit(false)
                    }
                    let m = FromGui::Navigate(ViewLoc::Netidx(Path::from("/")));
                    let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
                    Inhibit(false)
                }),
            );
            let sep = gtk::Label::new(Some(" > "));
            root.add(&sep);
            let fl = gtk::Label::new(None);
            root.add(&fl);
            fl.set_margin_start(10);
            fl.set_markup(&format!(r#"<a href="">file:{}</a>"#, name));
            fl.connect_activate_link(
                clone!(@strong ctx, @strong ask_saved, @strong name => move |_, _| {
                    if !ask_saved() {
                        return Inhibit(false)
                    }
                    let m = FromGui::Navigate(ViewLoc::File(name.clone()));
                    let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
                    Inhibit(false)
                }),
            );
        }
    }
    root
}

struct View {
    root: gtk::Box,
    widget: Widget,
}

impl View {
    fn new(
        ctx: WidgetCtx,
        path: &ViewLoc,
        saved: Rc<Cell<bool>>,
        spec: view::View,
    ) -> View {
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
        root.add(&make_crumbs(&ctx, path, saved));
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

fn default_view(path: Path) -> protocol_view::View {
    protocol_view::View {
        variables: HashMap::new(),
        keybinds: Vec::new(),
        scripts: Vec::new(),
        root: protocol_view::Widget::Table(path),
    }
}

#[derive(Debug)]
struct StartNetidx {
    cfg: Config,
    auth: Auth,
    subscriber: Arc<OnceCell<Subscriber>>,
    updates: mpsc::Receiver<Batch>,
    state_updates: mpsc::UnboundedReceiver<(SubId, DvState)>,
    from_gui: mpsc::UnboundedReceiver<FromGui>,
    to_gui: glib::Sender<ToGui>,
    to_init: smpsc::Sender<(Subscriber, ResolverRead)>,
}

async fn netidx_main(mut ctx: StartNetidx) {
    async fn read_view(rx_view: &mut Option<mpsc::Receiver<Batch>>) -> Option<Batch> {
        match rx_view {
            None => pending().await,
            Some(rx_view) => rx_view.next().await,
        }
    }
    let cfg = ctx.cfg;
    let auth = ctx.auth;
    let subscriber =
        ctx.subscriber.get_or_init(move || Subscriber::new(cfg, auth).unwrap()).clone();
    let resolver = subscriber.resolver();
    let _: result::Result<_, _> =
        ctx.to_init.send((subscriber.clone(), resolver.clone()));
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
            m = ctx.from_gui.next() => match m {
                None => break,
                Some(FromGui::Terminate) => break,
                Some(FromGui::Updated) => {
                    refreshing = false;
                    break_err!(refresh(&mut refreshing, &mut changed, &ctx.to_gui))
                },
                Some(FromGui::Render(view)) => {
                    view_path = None;
                    rx_view = None;
                    _dv_view = None;
                    match view::View::new(&resolver, view.clone()).await {
                        Err(e) => warn!("failed to raeify view {}", e),
                        Ok(v) => {
                            let m = ToGui::View {
                                loc: None,
                                original: view,
                                raeified: v,
                                generated: false,
                            };
                            break_err!(ctx.to_gui.send(m));
                            info!("updated gui view (render)")
                        }
                    }
                },
                Some(FromGui::Save(ViewLoc::Netidx(path), view, fin)) => {
                    let to = Some(Duration::from_secs(10));
                    match subscriber.subscribe_one(path, to).await {
                        Err(e) => { let _ = fin.send(Err(e)); }
                        Ok(val) => match serde_json::to_string(&view) {
                            Err(e) => { let _ = fin.send(Err(Error::from(e))); }
                            Ok(s) => {
                                let v = Value::String(Chars::from(s));
                                match val.write_with_recipt(v).await {
                                    Err(e) => { let _ = fin.send(Err(Error::from(e))); }
                                    Ok(v) => {
                                        let _ = fin.send(match v {
                                            Value::Error(s) =>
                                                Err(anyhow!(String::from(&*s))),
                                            _ => Ok(())
                                        });
                                    }
                                }
                            }
                        }
                    }
                },
                Some(FromGui::Save(ViewLoc::File(_file), _view, _fin)) => todo!(),
                Some(FromGui::Navigate(ViewLoc::Netidx(base_path))) => {
                    match resolver.table(base_path.clone()).await {
                        Err(e) => warn!("can't fetch table spec for {}, {}", base_path, e),
                        Ok(spec) => {
                            let raeified_default = view::View {
                                variables: HashMap::new(),
                                scripts: Vec::new(),
                                root: view::Widget::Table(base_path.clone(), spec)
                            };
                            let m = ToGui::View {
                                loc: Some(ViewLoc::Netidx(base_path.clone())),
                                original: default_view(base_path.clone()),
                                raeified: raeified_default,
                                generated: true,
                            };
                            break_err!(ctx.to_gui.send(m));
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
                Some(FromGui::Navigate(ViewLoc::File(_file))) => todo!(),
            },
            b = ctx.updates.next() => if let Some(mut batch) = b {
                for (id, v) in batch.drain(..) {
                    changed.insert(id, v);
                }
                break_err!(refresh(&mut refreshing, &mut changed, &ctx.to_gui))
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
                                Ok(view) => if let Some(path) = &view_path {
                                    match view::View::new(&resolver, view.clone()).await {
                                        Err(e) => warn!("failed to raeify view {}", e),
                                        Ok(v) => {
                                            let m = ToGui::View {
                                                loc: Some(ViewLoc::Netidx(path.clone())),
                                                original: view,
                                                raeified: v,
                                                generated: false
                                            };
                                            break_err!(ctx.to_gui.send(m));
                                            info!("updated gui view")
                                        }
                                    }
                                }
                            }
                        }
                        v => warn!("unexpected type of view definition {}", v),
                    }
                }
            },
            s = ctx.state_updates.next() => if let Some((id, st)) = s {
                match st {
                    DvState::Subscribed => (),
                    DvState::Unsubscribed => {
                        changed.insert(id, Value::String(Chars::from("#SUB")));
                        break_err!(refresh(&mut refreshing, &mut changed, &ctx.to_gui))
                    }
                    DvState::FatalError(_) => {
                        changed.insert(id, Value::String(Chars::from("#ERR")));
                        break_err!(refresh(&mut refreshing, &mut changed, &ctx.to_gui))
                    }
                }
            }
        }
    }
    let _: result::Result<_, _> = ctx.to_gui.send(ToGui::Terminate);
}

fn run_tokio(mut start_netidx: mpsc::UnboundedReceiver<StartNetidx>) {
    thread::spawn(move || {
        let mut rt = Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(async move {
            while let Some(ctx) = start_netidx.next().await {
                task::spawn(netidx_main(ctx));
            }
        });
    });
}

fn setup_css(screen: &gdk::Screen) {
    let style = gtk::CssProvider::new();
    style
        .load_from_data(
            r#"*.highlighted {
    border-width: 2px;
    border-style: solid;
    border-color: blue;
}"#
            .as_bytes(),
        )
        .unwrap();
    gtk::StyleContext::add_provider_for_screen(screen, &style, 800);
}

fn run_gui(ctx: WidgetCtx, app: &Application, to_gui: glib::Receiver<ToGui>) {
    let app = app.clone();
    let window = ApplicationWindow::new(&app);
    let headerbar = gtk::HeaderBar::new();
    let mainbox = gtk::Paned::new(gtk::Orientation::Horizontal);
    let design_mode = gtk::ToggleButton::new();
    let design_img = gtk::Image::from_icon_name(
        Some("document-page-setup"),
        gtk::IconSize::SmallToolbar,
    );
    let save_img =
        gtk::Image::from_icon_name(Some("media-floppy"), gtk::IconSize::SmallToolbar);
    let save_button = gtk::ToolButton::new(Some(&save_img), None);
    save_button.set_sensitive(false);
    design_mode.set_image(Some(&design_img));
    headerbar.set_show_close_button(true);
    headerbar.pack_start(&design_mode);
    headerbar.pack_start(&save_button);
    window.set_titlebar(Some(&headerbar));
    window.set_title("Netidx browser");
    window.set_default_size(800, 600);
    window.add(&mainbox);
    window.show_all();
    if let Some(screen) = window.get_screen() {
        setup_css(&screen);
    }
    window.connect_delete_event(clone!(@strong ctx, @weak app =>
    @default-return Inhibit(false), move |_, _| {
        let _: result::Result<_, _> = ctx.from_gui.unbounded_send(FromGui::Terminate);
        Inhibit(false)
    }));
    let ctx = Rc::new(ctx);
    let saved = Rc::new(Cell::new(true));
    let save_loc: Rc<RefCell<Option<ViewLoc>>> = Rc::new(RefCell::new(None));
    let current_loc: Rc<RefCell<ViewLoc>> =
        Rc::new(RefCell::new(ViewLoc::Netidx(Path::from("/"))));
    let current_spec: Rc<RefCell<protocol_view::View>> =
        Rc::new(RefCell::new(default_view(Path::from("/"))));
    let current: Rc<RefCell<Option<View>>> = Rc::new(RefCell::new(None));
    let editor: Rc<RefCell<Option<Editor>>> = Rc::new(RefCell::new(None));
    let highlight: Rc<RefCell<Vec<WidgetPath>>> = Rc::new(RefCell::new(vec![]));
    design_mode.connect_toggled(clone!(
        @weak mainbox,
        @strong editor,
        @strong highlight,
        @strong current,
        @strong current_spec,
        @strong ctx => move |b| {
        if b.get_active() {
            if let Some(editor) = editor.borrow_mut().take() {
                mainbox.remove(editor.root());
            }
            let s = current_spec.borrow().clone();
            let e = Editor::new(ctx.from_gui.clone(), ctx.to_gui.clone(), s);
            mainbox.add1(e.root());
            mainbox.show_all();
            *editor.borrow_mut() = Some(e);
        } else {
            if let Some(editor) = editor.borrow_mut().take() {
                mainbox.remove(editor.root());
                if let Some(cur) = &*current.borrow() {
                    let hl = highlight.borrow();
                    cur.widget.set_highlight(hl.iter().copied(), false);
                }
                highlight.borrow_mut().clear();
            }
        }
    }));
    to_gui.attach(None, move |m| match m {
        ToGui::Update(batch) => {
            if let Some(root) = &mut *current.borrow_mut() {
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
            Continue(true)
        }
        ToGui::View { loc, original, raeified, generated } => {
            match loc {
                None => {
                    saved.set(false);
                    save_button.set_sensitive(true);
                }
                Some(loc) => {
                    saved.set(true);
                    save_button.set_sensitive(false);
                    if !generated {
                        *save_loc.borrow_mut() = Some(loc.clone());
                    }
                    *current_loc.borrow_mut() = loc;
                    if design_mode.get_active() {
                        design_mode.set_active(false);
                    }
                }
            }
            *current_spec.borrow_mut() = original.clone();
            let cur = View::new(
                (*ctx).clone(),
                &*current_loc.borrow(),
                Rc::clone(&saved),
                raeified,
            );
            if let Some(cur) = current.borrow_mut().take() {
                mainbox.remove(cur.root());
            }
            window.set_title(&format!("Netidx Browser {}", &*current_loc.borrow()));
            mainbox.add2(cur.root());
            mainbox.show_all();
            let hl = highlight.borrow();
            cur.widget.set_highlight(hl.iter().copied(), true);
            *current.borrow_mut() = Some(cur);
            let m = ToGui::Update(Arc::new(IndexMap::new()));
            let _: result::Result<_, _> = ctx.to_gui.send(m);
            Continue(true)
        }
        ToGui::Highlight(path) => {
            if let Some(cur) = &*current.borrow() {
                let mut hl = highlight.borrow_mut();
                cur.widget.set_highlight(hl.iter().copied(), false);
                *hl = path;
                cur.widget.set_highlight(hl.iter().copied(), true);
            }
            Continue(true)
        }
        ToGui::UpdateVar(name, value) => {
            if let Some(root) = &mut *current.borrow_mut() {
                root.update_var(&name, &value);
            }
            Continue(true)
        }
        ToGui::Terminate => Continue(false),
    });
}

pub(crate) fn run(cfg: Config, auth: Auth, path: Path) {
    let application = Application::new(Some("org.netidx.browser"), Default::default())
        .expect("failed to initialize GTK application");
    let subscriber = Arc::new(OnceCell::new());
    let (tx_tokio, rx_tokio) = mpsc::unbounded();
    run_tokio(rx_tokio);
    application.connect_activate(move |app| {
        let (tx_updates, rx_updates) = mpsc::channel(2);
        let (tx_state_updates, rx_state_updates) = mpsc::unbounded();
        let (tx_to_gui, rx_to_gui) = glib::MainContext::channel(PRIORITY_LOW);
        let (tx_from_gui, rx_from_gui) = mpsc::unbounded();
        let (tx_init, rx_init) = smpsc::channel();
        // navigate to the initial location
        tx_from_gui
            .unbounded_send(FromGui::Navigate(ViewLoc::Netidx(path.clone())))
            .unwrap();
        tx_tokio
            .unbounded_send(StartNetidx {
                cfg: cfg.clone(),
                auth: auth.clone(),
                subscriber: Arc::clone(&subscriber),
                updates: rx_updates,
                state_updates: rx_state_updates,
                from_gui: rx_from_gui,
                to_gui: tx_to_gui.clone(),
                to_init: tx_init,
            })
            .unwrap();
        let (subscriber, resolver) = rx_init.recv().unwrap();
        let ctx = WidgetCtx {
            subscriber,
            resolver,
            updates: tx_updates,
            state_updates: tx_state_updates,
            to_gui: tx_to_gui,
            from_gui: tx_from_gui,
        };
        run_gui(ctx, app, rx_to_gui);
    });
    application.run(&[]);
}
