#![recursion_limit = "2048"]
#[macro_use]
extern crate glib;
#[macro_use]
extern crate lazy_static;
mod containers;
mod editor;
mod formula;
mod table;
mod util;
mod view;
mod widgets;
use anyhow::{anyhow, Error, Result};
use editor::Editor;
use formula::Target;
use futures::{
    channel::{mpsc, oneshot},
    future::{pending, FutureExt},
    select_biased,
    stream::StreamExt,
};
use gdk::{self, prelude::*};
use gio::{self, prelude::*};
use glib::{clone, idle_add_local, source::PRIORITY_LOW};
use gtk::{self, prelude::*, Adjustment, Application, ApplicationWindow};
use log::{info, warn};
use netidx::{
    chars::Chars,
    config::{self, Config},
    path::Path,
    pool::{Pool, Pooled},
    resolver::{Auth, ResolverRead},
    subscriber::{Dval, Event, SubId, Subscriber, UpdatesFlags, Value},
};
use netidx_protocols::view as protocol_view;
use once_cell::sync::OnceCell;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    fmt, fs, mem,
    ops::Deref,
    path::PathBuf,
    rc::{Rc, Weak},
    result, str,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc as smpsc, Arc,
    },
    thread,
    time::Duration,
};
use structopt::StructOpt;
use tokio::{runtime::Runtime, task};
use util::{ask_modal, err_modal};

type Batch = Pooled<Vec<(SubId, Value)>>;
type RawBatch = Pooled<Vec<(SubId, Event)>>;
type Vars = Rc<RefCell<HashMap<Chars, Value>>>;

#[derive(Debug, Clone, Copy)]
enum WidgetPath {
    Leaf,
    Box(usize),
    GridItem(usize, usize),
    GridRow(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ViewLoc {
    File(PathBuf),
    Netidx(Path),
}

impl str::FromStr for ViewLoc {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if Path::is_absolute(s) {
            Ok(ViewLoc::Netidx(Path::from(Arc::from(s))))
        } else if s.starts_with("netidx:") && Path::is_absolute(&s[7..]) {
            Ok(ViewLoc::Netidx(Path::from(Arc::from(&s[7..]))))
        } else if s.starts_with("file:") {
            let mut buf = PathBuf::new();
            buf.push(&s[5..]);
            Ok(ViewLoc::File(buf))
        } else {
            Err(())
        }
    }
}

impl fmt::Display for ViewLoc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ViewLoc::File(s) => write!(f, "file:{:?}", s),
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
    OpenNewWindow,
    Highlight(Vec<WidgetPath>),
    Update(Batch),
    UpdateVar(Chars, Value),
    ShowError(String),
    SaveError(String),
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

#[derive(Debug)]
struct WidgetCtxInner {
    subscriber: Subscriber,
    resolver: ResolverRead,
    updates: mpsc::Sender<RawBatch>,
    to_gui: glib::Sender<ToGui>,
    from_gui: mpsc::UnboundedSender<FromGui>,
    raw_view: Arc<AtomicBool>,
    window: gtk::ApplicationWindow,
    new_window_loc: Rc<RefCell<ViewLoc>>,
}

#[derive(Debug, Clone)]
struct WidgetCtx(Rc<WidgetCtxInner>);

impl glib::clone::Downgrade for WidgetCtx {
    type Weak = WidgetCtxWeak;

    fn downgrade(&self) -> Self::Weak {
        WidgetCtxWeak(Rc::downgrade(&self.0))
    }
}

impl Deref for WidgetCtx {
    type Target = WidgetCtxInner;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

struct WidgetCtxWeak(Weak<WidgetCtxInner>);

impl glib::clone::Upgrade for WidgetCtxWeak {
    type Strong = WidgetCtx;

    fn upgrade(&self) -> Option<Self::Strong> {
        Weak::upgrade(&self.0).map(WidgetCtx)
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

fn set_common_props<T: IsA<gtk::Widget> + 'static>(props: view::WidgetProps, t: &T) {
    t.set_halign(align_to_gtk(props.halign));
    t.set_valign(align_to_gtk(props.valign));
    t.set_hexpand(props.hexpand);
    t.set_vexpand(props.vexpand);
    t.set_margin_top(props.margin_top as i32);
    t.set_margin_bottom(props.margin_bottom as i32);
    t.set_margin_start(props.margin_start as i32);
    t.set_margin_end(props.margin_end as i32);
}

enum Widget {
    Action(widgets::Action),
    Table(table::Table),
    Label(widgets::Label),
    Button(widgets::Button),
    Toggle(widgets::Toggle),
    Selector(widgets::Selector),
    Entry(widgets::Entry),
    Box(containers::Box),
    Grid(containers::Grid),
    LinePlot(widgets::LinePlot),
}

impl Widget {
    fn new(
        ctx: WidgetCtx,
        variables: &Vars,
        spec: view::Widget,
        selected_path: gtk::Label,
    ) -> Widget {
        let w =
            match spec.kind {
                view::WidgetKind::Action(spec) => {
                    Widget::Action(widgets::Action::new(ctx.clone(), variables, spec))
                }
                view::WidgetKind::Table(base_path, spec) => {
                    let tbl =
                        table::Table::new(ctx.clone(), base_path, spec, selected_path);
                    // force the initial update/subscribe
                    tbl.start_update_task(None);
                    Widget::Table(tbl)
                }
                view::WidgetKind::Label(spec) => Widget::Label(widgets::Label::new(
                    ctx.clone(),
                    variables,
                    spec,
                    selected_path,
                )),
                view::WidgetKind::Button(spec) => Widget::Button(widgets::Button::new(
                    ctx.clone(),
                    variables,
                    spec,
                    selected_path,
                )),
                view::WidgetKind::Toggle(spec) => Widget::Toggle(widgets::Toggle::new(
                    ctx.clone(),
                    variables,
                    spec,
                    selected_path,
                )),
                view::WidgetKind::Selector(spec) => Widget::Selector(
                    widgets::Selector::new(ctx.clone(), variables, spec, selected_path),
                ),
                view::WidgetKind::Entry(spec) => Widget::Entry(widgets::Entry::new(
                    ctx.clone(),
                    variables,
                    spec,
                    selected_path,
                )),
                view::WidgetKind::Box(s) => {
                    Widget::Box(containers::Box::new(ctx, variables, s, selected_path))
                }
                view::WidgetKind::BoxChild(view::BoxChild { widget, .. }) => {
                    Widget::new(ctx, variables, (&*widget).clone(), selected_path)
                }
                view::WidgetKind::Grid(spec) => Widget::Grid(containers::Grid::new(
                    ctx,
                    variables,
                    spec,
                    selected_path,
                )),
                view::WidgetKind::GridChild(view::GridChild { widget, .. }) => {
                    Widget::new(ctx, variables, (&*widget).clone(), selected_path)
                }
                view::WidgetKind::GridRow(_) => {
                    let s = Value::String(Chars::from("orphaned grid row"));
                    let spec = view::Expr::Constant(s);
                    Widget::Label(widgets::Label::new(
                        ctx.clone(),
                        variables,
                        spec,
                        selected_path,
                    ))
                }
                view::WidgetKind::LinePlot(spec) => Widget::LinePlot(
                    widgets::LinePlot::new(ctx, variables, spec, selected_path),
                ),
            };
        if let Some(r) = w.root() {
            set_common_props(spec.props.unwrap_or(DEFAULT_PROPS), r);
        }
        w
    }

    fn root(&self) -> Option<&gtk::Widget> {
        match self {
            Widget::Action(_) => None,
            Widget::Table(t) => Some(t.root()),
            Widget::Label(t) => Some(t.root()),
            Widget::Button(t) => Some(t.root()),
            Widget::Toggle(t) => Some(t.root()),
            Widget::Selector(t) => Some(t.root()),
            Widget::Entry(t) => Some(t.root()),
            Widget::Box(t) => Some(t.root()),
            Widget::Grid(t) => Some(t.root()),
            Widget::LinePlot(t) => Some(t.root()),
        }
    }

    fn update<'a, 'b: 'a>(
        &'a self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        tgt: Target,
        value: &Value,
    ) {
        match self {
            Widget::Action(t) => t.update(tgt, value),
            Widget::Table(t) => t.update(waits, tgt, value),
            Widget::Label(t) => t.update(tgt, value),
            Widget::Button(t) => t.update(tgt, value),
            Widget::Toggle(t) => t.update(tgt, value),
            Widget::Selector(t) => t.update(tgt, value),
            Widget::Entry(t) => t.update(tgt, value),
            Widget::Box(t) => t.update(waits, tgt, value),
            Widget::Grid(t) => t.update(waits, tgt, value),
            Widget::LinePlot(t) => t.update(tgt, value),
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
                (WidgetPath::GridItem(i, j), Widget::Grid(w)) => {
                    if i < w.children.len() && j < w.children[i].len() {
                        w.children[i][j].set_highlight(path, h)
                    }
                }
                (WidgetPath::GridRow(i), Widget::Grid(w)) => {
                    if i < w.children.len() {
                        for c in &w.children[i] {
                            if let Some(r) = c.root() {
                                set(r, h);
                            }
                        }
                    }
                }
                (WidgetPath::Box(_), _) => (),
                (WidgetPath::GridItem(_, _), _) => (),
                (WidgetPath::GridRow(_), _) => (),
                (WidgetPath::Leaf, Widget::Box(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Grid(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Action(_)) => (),
                (WidgetPath::Leaf, Widget::Table(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Label(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Button(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Toggle(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Selector(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Entry(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::LinePlot(w)) => set(w.root(), h),
            },
        }
    }
}

fn make_crumbs(ctx: &WidgetCtx, loc: &ViewLoc, saved: &Rc<Cell<bool>>) -> gtk::Box {
    let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
    let ask_saved =
        Rc::new(clone!(@strong saved, @weak root => @default-return true, move || {
            if !saved.get() {
                ask_modal(&root, "Unsaved view will be lost.")
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
                @weak ctx,
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
            rl.connect_activate_link(clone!(
                @weak ctx,
                @strong ask_saved => @default-return Inhibit(false), move |_, _| {
                    if !ask_saved() {
                        return Inhibit(false)
                    }
                    let loc = ViewLoc::Netidx(Path::from("/"));
                    let m = FromGui::Navigate(loc);
                    let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
                    Inhibit(false)
            }));
            let sep = gtk::Label::new(Some(" > "));
            root.add(&sep);
            let fl = gtk::Label::new(None);
            root.add(&fl);
            fl.set_margin_start(10);
            fl.set_markup(&format!(r#"<a href="">file:{:?}</a>"#, name));
            fl.connect_activate_link(clone!(
                @weak ctx,
                @strong ask_saved,
                @strong name => @default-return Inhibit(false), move |_, _| {
                    if !ask_saved() {
                        return Inhibit(false)
                    }
                    let loc = ViewLoc::File(name.clone());
                    let m = FromGui::Navigate(loc);
                    let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
                    Inhibit(false)
            }));
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
        variables: &Vars,
        path: &ViewLoc,
        saved: &Rc<Cell<bool>>,
        spec: view::View,
    ) -> View {
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
        let widget =
            Widget::new(ctx.clone(), variables, spec.root.clone(), selected_path.clone());
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        root.set_property_margin(2);
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

    fn update(&self, waits: &mut Vec<oneshot::Receiver<()>>, tgt: Target, value: &Value) {
        self.widget.update(waits, tgt, value);
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

static DEFAULT_PROPS: view::WidgetProps = view::WidgetProps {
    halign: view::Align::Fill,
    valign: view::Align::Fill,
    hexpand: false,
    vexpand: false,
    margin_top: 0,
    margin_bottom: 0,
    margin_start: 0,
    margin_end: 0,
};

fn default_view(path: Path) -> protocol_view::View {
    protocol_view::View {
        variables: HashMap::new(),
        keybinds: Vec::new(),
        root: protocol_view::Widget {
            kind: protocol_view::WidgetKind::Table(protocol_view::Table {
                path,
                default_sort_column: None,
                columns: protocol_view::ColumnSpec::Auto,
            }),
            props: None,
        },
    }
}

#[derive(Debug)]
struct StartNetidx {
    cfg: Config,
    auth: Auth,
    subscriber: Arc<OnceCell<Subscriber>>,
    updates: mpsc::Receiver<RawBatch>,
    from_gui: mpsc::UnboundedReceiver<FromGui>,
    to_gui: glib::Sender<ToGui>,
    to_init: smpsc::Sender<(Subscriber, ResolverRead)>,
    raw_view: Arc<AtomicBool>,
}

lazy_static! {
    static ref UPDATES: Pool<Vec<(SubId, Value)>> = Pool::new(5, 100000);
}

async fn netidx_main(mut ctx: StartNetidx) {
    async fn read_view(
        rx_view: &mut Option<mpsc::Receiver<RawBatch>>,
    ) -> Option<RawBatch> {
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
    let mut rx_view: Option<mpsc::Receiver<RawBatch>> = None;
    let mut _dv_view: Option<Dval> = None;
    let mut changed = UPDATES.take();
    let mut refreshing = false;
    fn refresh(
        refreshing: &mut bool,
        changed: &mut Batch,
        to_gui: &glib::Sender<ToGui>,
    ) -> Result<()> {
        if !*refreshing && !changed.is_empty() {
            *refreshing = true;
            to_gui.send(ToGui::Update(mem::replace(changed, UPDATES.take())))?
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
                Some(FromGui::Save(ViewLoc::File(file), view, fin)) => {
                    match serde_json::to_string(&view) {
                        Err(e) => { let _ = fin.send(Err(Error::from(e))); }
                        Ok(s) => match fs::write(file, s) {
                            Err(e) => { let _ = fin.send(Err(Error::from(e))); }
                            Ok(()) => { let _ = fin.send(Ok(())); }
                        }
                    }
                },
                Some(FromGui::Navigate(ViewLoc::Netidx(base_path))) => {
                    match resolver.table(base_path.clone()).await {
                        Err(e) => {
                            let m =
                                format!("can't fetch table spec for {}, {}", base_path, e);
                            break_err!(ctx.to_gui.send(ToGui::ShowError(m)));
                        },
                        Ok(spec) => {
                            let raeified_default = view::View {
                                variables: HashMap::new(),
                                root: view::Widget {
                                    props: None,
                                    kind: view::WidgetKind::Table(view::Table {
                                        path: base_path.clone(),
                                        default_sort_column: None,
                                        columns: view::ColumnSpec::Auto
                                    }, spec)
                                }
                            };
                            let raw = ctx.raw_view.load(Ordering::Relaxed);
                            let m = ToGui::View {
                                loc: Some(ViewLoc::Netidx(base_path.clone())),
                                original: default_view(base_path.clone()),
                                raeified: raeified_default,
                                generated: true,
                            };
                            break_err!(ctx.to_gui.send(m));
                            if !raw {
                                let s = subscriber
                                    .durable_subscribe(base_path.append(".view"));
                                let (tx, rx) = mpsc::channel(2);
                                s.updates(UpdatesFlags::BEGIN_WITH_LAST, tx);
                                view_path = Some(base_path.clone());
                                rx_view = Some(rx);
                                _dv_view = Some(s);
                            }
                        }
                    }
                },
                Some(FromGui::Navigate(ViewLoc::File(file))) => {
                    // CR estokes: async!
                    match fs::read_to_string(&file) {
                        Err(e) => {
                            let m =
                                format!("can't load view from file {:?}, {}", file, e);
                            break_err!(ctx.to_gui.send(ToGui::ShowError(m)));
                        },
                        Ok(s) => match serde_json::from_str::<protocol_view::View>(&s) {
                            Err(e) => {
                                let m = format!("invalid view: {:?}, {}", file, e);
                                break_err!(ctx.to_gui.send(ToGui::ShowError(m)));
                            },
                            Ok(v) => match view::View::new(&resolver, v.clone()).await {
                                Err(e) => {
                                    let m = format!("error building view: {}", e);
                                    break_err!(ctx.to_gui.send(ToGui::ShowError(m)));
                                },
                                Ok(r) => {
                                    let m = ToGui::View {
                                        loc: Some(ViewLoc::File(file)),
                                        original: v,
                                        raeified: r,
                                        generated: false,
                                    };
                                    break_err!(ctx.to_gui.send(m));
                                },
                            },
                        }
                    }
                },
            },
            b = ctx.updates.next() => if let Some(mut batch) = b {
                for (id, ev) in batch.drain(..) {
                    match ev {
                        Event::Update(v) => changed.push((id, v)),
                        Event::Unsubscribed =>
                            changed.push((id, Value::String(Chars::from("#LOST")))),
                    }
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
                        Event::Update(Value::String(s)) => {
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
                                                generated: false,
                                            };
                                            break_err!(ctx.to_gui.send(m));
                                            info!("updated gui view")
                                        }
                                    }
                                }
                            }
                        }
                        v => warn!("unexpected type of view definition {:?}", v),
                    }
                }
            },
        }
    }
    let _: result::Result<_, _> = ctx.to_gui.send(ToGui::Terminate);
}

fn run_tokio(
    mut start_netidx: mpsc::UnboundedReceiver<StartNetidx>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let rt = Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(async move {
            while let Some(ctx) = start_netidx.next().await {
                task::spawn(netidx_main(ctx));
            }
        });
    })
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

fn choose_location(parent: &gtk::ApplicationWindow, save: bool) -> Option<ViewLoc> {
    enum W {
        File(gtk::FileChooserWidget),
        Netidx(gtk::Box),
    }
    let d = gtk::Dialog::with_buttons(
        Some("Choose Location"),
        Some(parent),
        gtk::DialogFlags::MODAL | gtk::DialogFlags::USE_HEADER_BAR,
        &[("Cancel", gtk::ResponseType::Cancel), ("Choose", gtk::ResponseType::Accept)],
    );
    let loc: Rc<RefCell<Option<ViewLoc>>> = Rc::new(RefCell::new(None));
    let mainw: Rc<RefCell<Option<W>>> = Rc::new(RefCell::new(None));
    let root = d.get_content_area();
    let cb = gtk::ComboBoxText::new();
    cb.append(Some("Netidx"), "Netidx");
    cb.append(Some("File"), "File");
    root.add(&cb);
    root.add(&gtk::Separator::new(gtk::Orientation::Vertical));
    cb.connect_changed(clone!(@weak root, @strong loc, @strong mainw => move |cb| {
        if let Some(mw) = mainw.borrow_mut().take() {
            match mw {
                W::File(w) => root.remove(&w),
                W::Netidx(w) => root.remove(&w),
            }
        }
        let id = cb.get_active_id();
        match id.as_ref().map(|i| &**i) {
            Some("File") => {
                let w = gtk::FileChooserWidget::new(
                    if save {
                        gtk::FileChooserAction::Save
                    } else {
                        gtk::FileChooserAction::Open
                    });
                w.connect_selection_changed(clone!(@strong loc => move |w| {
                    *loc.borrow_mut() = w.get_filename().map(|f| ViewLoc::File(f));
                }));
                root.add(&w);
                *mainw.borrow_mut() = Some(W::File(w));
            }
            Some("Netidx") | None => {
                let b = gtk::Box::new(gtk::Orientation::Horizontal, 10);
                let l = gtk::Label::new(Some("Netidx Path:"));
                let e = gtk::Entry::new();
                b.pack_start(&l, true, true, 5);
                b.pack_start(&e, true, true, 5);
                root.add(&b);
                e.connect_changed(clone!(@strong loc => move |e| {
                    let p = Path::from(String::from(e.get_text()));
                    *loc.borrow_mut() = Some(ViewLoc::Netidx(p));
                }));
                *mainw.borrow_mut() = Some(W::Netidx(b));
            }
            Some(_) => unreachable!(),
        }
        root.show_all();
    }));
    cb.set_active_id(Some("Netidx"));
    let res = match d.run() {
        gtk::ResponseType::Accept => loc.borrow_mut().take(),
        gtk::ResponseType::Cancel | _ => None,
    };
    unsafe {
        d.destroy();
    }
    res
}

fn save_view(
    ctx: &Rc<WidgetCtx>,
    saved: &Rc<Cell<bool>>,
    save_loc: &Rc<RefCell<Option<ViewLoc>>>,
    current_spec: &Rc<RefCell<protocol_view::View>>,
    save_button: &gtk::ToolButton,
    save_as: bool,
) {
    let do_save = |loc: ViewLoc| {
        let (tx, rx) = oneshot::channel();
        let spec = current_spec.borrow().clone();
        let m = FromGui::Save(loc.clone(), spec, tx);
        let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
        glib::MainContext::default().spawn_local({
            let save_button = save_button.clone();
            let saved = saved.clone();
            let save_loc = save_loc.clone();
            let ctx = ctx.clone();
            async move {
                match rx.await {
                    Err(e) => {
                        let _: result::Result<_, _> = ctx.to_gui.send(ToGui::SaveError(
                            format!("error saving to: {:?}, {}", &*save_loc.borrow(), e),
                        ));
                        *save_loc.borrow_mut() = None;
                    }
                    Ok(Err(e)) => {
                        let _: result::Result<_, _> = ctx.to_gui.send(ToGui::SaveError(
                            format!("error saving to: {:?}, {}", &*save_loc.borrow(), e),
                        ));
                        *save_loc.borrow_mut() = None;
                    }
                    Ok(Ok(())) => {
                        saved.set(true);
                        save_button.set_sensitive(false);
                        let mut sl = save_loc.borrow_mut();
                        if sl.as_ref() != Some(&loc) {
                            *sl = Some(loc.clone());
                            let _: result::Result<_, _> = ctx
                                .from_gui
                                .unbounded_send(FromGui::Navigate(loc.clone()));
                        }
                    }
                }
            }
        });
    };
    let sl = save_loc.borrow_mut();
    match &*sl {
        Some(loc) if !save_as => do_save(loc.clone()),
        _ => match choose_location(&ctx.window, true) {
            None => (),
            Some(loc) => do_save(loc),
        },
    }
}

fn run_gui(ctx: WidgetCtx, app: &Application, to_gui: glib::Receiver<ToGui>) {
    let app = app.clone();
    let group = gtk::WindowGroup::new();
    group.add_window(&ctx.window);
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
    let prefs_button = gtk::MenuButton::new();
    let menu_img =
        gtk::Image::from_icon_name(Some("open-menu"), gtk::IconSize::SmallToolbar);
    prefs_button.set_image(Some(&menu_img));
    let main_menu = gio::Menu::new();
    main_menu.append(Some("Go"), Some("win.go"));
    main_menu.append(Some("Save View As"), Some("win.save_as"));
    main_menu.append(Some("Raw View"), Some("win.raw_view"));
    main_menu.append(Some("New Window"), Some("win.new_window"));
    prefs_button.set_use_popover(true);
    prefs_button.set_menu_model(Some(&main_menu));
    save_button.set_sensitive(false);
    design_mode.set_image(Some(&design_img));
    headerbar.set_show_close_button(true);
    headerbar.pack_start(&design_mode);
    headerbar.pack_start(&save_button);
    headerbar.pack_end(&prefs_button);
    ctx.window.set_titlebar(Some(&headerbar));
    ctx.window.set_title("Netidx browser");
    ctx.window.set_default_size(800, 600);
    ctx.window.add(&mainbox);
    ctx.window.show_all();
    if let Some(screen) = ctx.window.get_screen() {
        setup_css(&screen);
    }
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
    let variables: Vars = Rc::new(RefCell::new(HashMap::new()));
    ctx.window.connect_delete_event(clone!(
        @weak ctx,
        @strong saved => @default-return Inhibit(false), move |w, _| {
            if saved.get() || ask_modal(w, "Unsaved view will be lost.") {
                let _: result::Result<_, _> =
                    ctx.from_gui.unbounded_send(FromGui::Terminate);
                Inhibit(false)
            } else {
                Inhibit(true)
            }
    }));
    design_mode.connect_toggled(clone!(
    @weak mainbox,
    @strong variables,
    @strong editor,
    @strong highlight,
    @strong current,
    @strong current_spec,
    @weak ctx => move |b| {
    if b.get_active() {
        if let Some(editor) = editor.borrow_mut().take() {
            mainbox.remove(editor.root());
        }
        let s = current_spec.borrow().clone();
        let e = Editor::new(WidgetCtx::clone(&*ctx), &variables, s);
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
    save_button.connect_clicked(clone!(
        @strong saved,
        @strong save_loc,
        @strong current_spec,
        @weak ctx => move |b| {
            save_view(&ctx, &saved, &save_loc, &current_spec, b, false)
        }
    ));
    let go_act = gio::SimpleAction::new("go", None);
    ctx.window.add_action(&go_act);
    go_act.connect_activate(
        clone!(@weak ctx, @strong saved, @weak design_mode => move |_, _| {
            if saved.get() || ask_modal(&ctx.window, "Unsaved view will be lost.") {
                if let Some(loc) = choose_location(&ctx.window, false) {
                    let m = FromGui::Navigate(loc);
                    let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
                }
            }
        }),
    );
    let save_as_act = gio::SimpleAction::new("save_as", None);
    ctx.window.add_action(&save_as_act);
    save_as_act.connect_activate(clone!(
        @strong saved,
        @strong save_loc,
        @strong current_spec,
        @weak ctx,
        @strong save_button => move |_, _| {
            save_view(&ctx, &saved, &save_loc, &current_spec, &save_button, true)
        }
    ));
    let raw_view_act =
        gio::SimpleAction::new_stateful("raw_view", None, &false.to_variant());
    ctx.window.add_action(&raw_view_act);
    raw_view_act.connect_activate(clone!(
        @strong saved,
        @weak ctx,
        @strong current_loc  => move |a, _| {
        if let Some(v) = a.get_state() {
            let new_v = !v.get::<bool>().expect("invalid state");
            let m = "Unsaved view will be lost.";
            if !new_v || saved.get() || ask_modal(&ctx.window, m) {
                ctx.raw_view.store(new_v, Ordering::Relaxed);
                a.change_state(&new_v.to_variant());
                let m = FromGui::Navigate(current_loc.borrow().clone());
                let _: result::Result<_, _> = ctx.from_gui.unbounded_send(m);
            }
        }
    }));
    let new_window_act = gio::SimpleAction::new("new_window", None);
    ctx.window.add_action(&new_window_act);
    new_window_act.connect_activate(clone!(@weak app => move |_, _| app.activate()));
    to_gui.attach(None, move |m| match m {
        ToGui::OpenNewWindow => {
            app.activate();
            Continue(true)
        }
        ToGui::Update(mut batch) => {
            if let Some(root) = &mut *current.borrow_mut() {
                let mut waits = Vec::new();
                let ed = editor.borrow();
                for (id, value) in batch.drain(..) {
                    root.update(&mut waits, Target::Netidx(id), &value);
                    if let Some(editor) = &*ed {
                        editor.update(Target::Netidx(id), &value)
                    }
                }
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
                        *save_loc.borrow_mut() = Some(match loc.clone() {
                            v @ ViewLoc::File(_) => v,
                            ViewLoc::Netidx(p) => ViewLoc::Netidx(p.append(".view")),
                        });
                    } else {
                        *save_loc.borrow_mut() = None;
                    }
                    *current_loc.borrow_mut() = loc;
                    if design_mode.get_active() {
                        design_mode.set_active(false);
                    }
                }
            }
            if let Some(cur) = current.borrow_mut().take() {
                mainbox.remove(cur.root());
            }
            variables.borrow_mut().clear();
            *current_spec.borrow_mut() = original.clone();
            let cur = View::new(
                (*ctx).clone(),
                &variables,
                &*current_loc.borrow(),
                &saved,
                raeified,
            );
            ctx.window.set_title(&format!("Netidx Browser {}", &*current_loc.borrow()));
            mainbox.add2(cur.root());
            mainbox.show_all();
            let hl = highlight.borrow();
            cur.widget.set_highlight(hl.iter().copied(), true);
            *current.borrow_mut() = Some(cur);
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
                let mut waits = Vec::new();
                root.update(&mut waits, Target::Variable(&name), &value);
            }
            if let Some(editor) = &*editor.borrow() {
                editor.update(Target::Variable(&name), &value);
            }
            Continue(true)
        }
        ToGui::ShowError(s) => {
            err_modal(&ctx.window, &s);
            Continue(true)
        }
        ToGui::SaveError(s) => {
            err_modal(&ctx.window, &s);
            idle_add_local(clone!(
                @weak ctx,
                @strong saved,
                @strong save_loc,
                @strong current_spec,
                @strong save_button => @default-return Continue(false), move || {
                    save_view(
                        &ctx,
                        &saved,
                        &save_loc,
                        &current_spec,
                        &save_button,
                        true,
                    );
                    Continue(false)
            }));
            Continue(true)
        }
        ToGui::Terminate => Continue(false),
    });
}

#[derive(StructOpt, Debug)]
#[structopt(name = "netidx")]
struct Opt {
    #[structopt(
        short = "c",
        long = "config",
        help = "override the default config file location (~/.config/netidx.json)"
    )]
    config: Option<String>,
    #[structopt(short = "a", long = "anonymous", help = "disable Kerberos 5")]
    anon: bool,
    #[structopt(long = "upn", help = "krb5 use <upn> instead of the current user")]
    upn: Option<String>,
    #[structopt(long = "path", help = "navigate to <path> on startup")]
    path: Option<Path>,
    #[structopt(long = "file", help = "navigate to view file on startup")]
    file: Option<PathBuf>,
}

fn main() {
    env_logger::init();
    let opt = Opt::from_args();
    let cfg = match &opt.config {
        None => Config::load_default().unwrap(),
        Some(path) => Config::load(path).unwrap(),
    };
    let auth = if opt.anon {
        Auth::Anonymous
    } else {
        match cfg.auth {
            config::Auth::Anonymous => Auth::Anonymous,
            config::Auth::Krb5(_) => Auth::Krb5 { upn: opt.upn.clone(), spn: None },
        }
    };
    let application = Application::new(
        Some("org.netidx.browser"),
        gio::ApplicationFlags::NON_UNIQUE | Default::default(),
    )
    .expect("failed to initialize GTK application");
    let subscriber = Arc::new(OnceCell::new());
    let (tx_tokio, rx_tokio) = mpsc::unbounded();
    let jh = run_tokio(rx_tokio);
    let new_window_loc = Rc::new(RefCell::new(match &opt.path {
        Some(path) => ViewLoc::Netidx(path.clone()),
        None => match &opt.file {
            Some(file) => ViewLoc::File(file.clone()),
            None => ViewLoc::Netidx(Path::from("/")),
        },
    }));
    application.connect_activate(move |app| {
        let (tx_updates, rx_updates) = mpsc::channel(2);
        let (tx_to_gui, rx_to_gui) = glib::MainContext::channel(PRIORITY_LOW);
        let (tx_from_gui, rx_from_gui) = mpsc::unbounded();
        let (tx_init, rx_init) = smpsc::channel();
        let raw_view = Arc::new(AtomicBool::new(false));
        // navigate to the initial location
        tx_from_gui
            .unbounded_send(FromGui::Navigate(new_window_loc.borrow().clone()))
            .unwrap();
        tx_tokio
            .unbounded_send(StartNetidx {
                cfg: cfg.clone(),
                auth: auth.clone(),
                subscriber: Arc::clone(&subscriber),
                updates: rx_updates,
                from_gui: rx_from_gui,
                to_gui: tx_to_gui.clone(),
                to_init: tx_init,
                raw_view: raw_view.clone(),
            })
            .unwrap();
        let (subscriber, resolver) = rx_init.recv().unwrap();
        let window = ApplicationWindow::new(app);
        let ctx = WidgetCtx(Rc::new(WidgetCtxInner {
            subscriber,
            resolver,
            updates: tx_updates,
            to_gui: tx_to_gui,
            from_gui: tx_from_gui,
            raw_view,
            window: window.clone(),
            new_window_loc: new_window_loc.clone(),
        }));
        run_gui(ctx, app, rx_to_gui);
    });
    application.run(&[]);
    drop(application);
    let _: result::Result<_, _> = jh.join();
}
