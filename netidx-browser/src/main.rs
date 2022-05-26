#![recursion_limit = "2048"]
#[macro_use]
extern crate glib;
#[macro_use]
extern crate lazy_static;

mod backend;
mod bscript;
mod cairo_backend;
mod containers;
mod editor;
mod table;
mod util;
mod widgets;

use anyhow::Result;
use arcstr::ArcStr;
use bscript::LocalEvent;
use editor::Editor;
use futures::channel::oneshot;
use fxhash::FxHashMap;
use gdk::{self, prelude::*};
use glib::{clone, idle_add_local, source::PRIORITY_LOW};
use gtk::{self, prelude::*, Adjustment, Application, ApplicationWindow};
use netidx::{
    chars::Chars,
    config::Config,
    path::Path,
    pool::{Pool, Pooled},
    resolver_client,
    subscriber::{DesiredAuth, Dval, Event, SubId, UpdatesFlags, Value},
};
use netidx_bscript::{
    expr::{ExprId, ExprKind},
    vm::{self, ExecCtx, Node, RpcCallId},
};
use netidx_protocols::view;
use radix_trie::Trie;
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    fmt, mem,
    path::PathBuf,
    rc::Rc,
    result, str,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use util::{ask_modal, err_modal};

struct WVal<'a>(&'a Value);

impl<'a> fmt::Display for WVal<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt_naked(f)
    }
}

type BSNode = Node<WidgetCtx, LocalEvent>;
type BSCtx = Rc<RefCell<ExecCtx<WidgetCtx, LocalEvent>>>;
type BSCtxRef<'a> = &'a mut ExecCtx<WidgetCtx, LocalEvent>;
type Batch = Pooled<Vec<(SubId, Value)>>;
type RawBatch = Pooled<Vec<(SubId, Event)>>;

fn default_view(path: Path) -> view::View {
    view::View {
        variables: HashMap::new(),
        keybinds: Vec::new(),
        root: view::Widget {
            kind: view::WidgetKind::Table(view::Table {
                path: ExprKind::Constant(Value::String(Chars::from(String::from(
                    &*path,
                ))))
                .to_expr(),
                sort_mode: ExprKind::Constant(Value::Null).to_expr(),
                column_filter: ExprKind::Constant(Value::Null).to_expr(),
                row_filter: ExprKind::Constant(Value::Null).to_expr(),
                column_editable: ExprKind::Constant(Value::False).to_expr(),
                column_widths: ExprKind::Constant(Value::Null).to_expr(),
                columns_resizable: ExprKind::Constant(Value::True).to_expr(),
                multi_select: ExprKind::Constant(Value::False).to_expr(),
                show_row_name: ExprKind::Constant(Value::True).to_expr(),
                on_select: ExprKind::Constant(Value::Null).to_expr(),
                on_edit: ExprKind::Constant(Value::Null).to_expr(),
                on_activate: ExprKind::Apply {
                    function: "navigate".into(),
                    args: vec![ExprKind::Apply {
                        function: "event".into(),
                        args: vec![],
                    }
                    .to_expr()],
                }
                .to_expr(),
                on_header_click: ExprKind::Constant(Value::Null).to_expr(),
            }),
            props: None,
        },
    }
}

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
            Ok(ViewLoc::Netidx(Path::from(ArcStr::from(s))))
        } else if s.starts_with("netidx:") && Path::is_absolute(&s[7..]) {
            Ok(ViewLoc::Netidx(Path::from(ArcStr::from(&s[7..]))))
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
    View { loc: Option<ViewLoc>, spec: view::View, generated: bool },
    NavigateInWindow(ViewLoc),
    Highlight(Vec<WidgetPath>),
    Update(Batch),
    UpdateVar(Path, Chars, Value),
    UpdateRpc(RpcCallId, Value),
    TableResolved(Path, resolver_client::Table),
    ShowError(String),
    SaveError(String),
    Terminate,
}

#[derive(Debug)]
enum FromGui {
    Navigate(ViewLoc),
    Render(view::View),
    ResolveTable(Path),
    Save(ViewLoc, view::View, oneshot::Sender<Result<()>>),
    CallRpc(Path, Vec<(Chars, Value)>, RpcCallId),
    Updated,
    Terminate,
}

struct WidgetCtx {
    backend: backend::Ctx,
    raw_view: Arc<AtomicBool>,
    window: gtk::ApplicationWindow,
    new_window_loc: Rc<RefCell<ViewLoc>>,
    current_loc: Rc<RefCell<ViewLoc>>,
    view_saved: Cell<bool>,
    fns: Trie<String, ()>,
    vars: Trie<String, Trie<String, ()>>,
}

impl vm::Ctx for WidgetCtx {
    fn clear(&mut self) {}

    fn durable_subscribe(
        &mut self,
        flags: UpdatesFlags,
        path: Path,
        _ref_id: ExprId,
    ) -> Dval {
        let dv = self.backend.subscriber.durable_subscribe(path);
        dv.updates(flags, self.backend.updates.clone());
        dv
    }

    fn unsubscribe(&mut self, _path: Path, _dv: Dval, _ref_id: ExprId) {}

    fn ref_var(&mut self, _name: Chars, _scope: Path, _ref_id: ExprId) {}

    fn unref_var(&mut self, _name: Chars, _scope: Path, _ref_id: ExprId) {}

    fn register_fn(&mut self, name: Chars, _scope: Path) {
        self.fns.insert(name.into(), ());
    }

    fn set_var(
        &mut self,
        variables: &mut FxHashMap<Path, FxHashMap<Chars, Value>>,
        local: bool,
        scope: Path,
        name: Chars,
        value: Value,
    ) {
        let (new, scope) = vm::store_var(variables, local, &scope, &name, value.clone());
        if new {
            match self.vars.get_mut(&*name) {
                Some(scopes) => {
                    scopes.insert(scope.to_string(), ());
                }
                None => {
                    let mut scopes = Trie::new();
                    scopes.insert(scope.to_string(), ());
                    self.vars.insert(name.to_string(), scopes);
                }
            }
        }
        let _: Result<_, _> =
            self.backend.to_gui.send(ToGui::UpdateVar(scope, name, value));
    }

    fn call_rpc(
        &mut self,
        name: Path,
        args: Vec<(Chars, Value)>,
        _ref_id: ExprId,
        id: RpcCallId,
    ) {
        self.backend.call_rpc(name, args, id)
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
    Image(widgets::Image),
    Label(widgets::Label),
    Button(widgets::Button),
    LinkButton(widgets::LinkButton),
    Toggle(widgets::Toggle),
    Selector(widgets::Selector),
    Entry(widgets::Entry),
    Frame(containers::Frame),
    Box(containers::Box),
    Grid(containers::Grid),
    Paned(containers::Paned),
    Notebook(containers::Notebook),
    LinePlot(widgets::LinePlot),
}

impl Widget {
    fn new(
        ctx: &BSCtx,
        spec: view::Widget,
        scope: Path,
        selected_path: gtk::Label,
    ) -> Widget {
        let w = match spec.kind {
            view::WidgetKind::Action(spec) => {
                Widget::Action(widgets::Action::new(ctx, scope, spec))
            }
            view::WidgetKind::Table(spec) => {
                Widget::Table(table::Table::new(ctx.clone(), spec, scope, selected_path))
            }
            view::WidgetKind::Image(spec) => {
                Widget::Image(widgets::Image::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::Label(spec) => {
                Widget::Label(widgets::Label::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::Button(spec) => {
                Widget::Button(widgets::Button::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::LinkButton(spec) => Widget::LinkButton(
                widgets::LinkButton::new(ctx, spec, scope, selected_path),
            ),
            view::WidgetKind::Toggle(spec) => {
                Widget::Toggle(widgets::Toggle::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::Selector(spec) => {
                Widget::Selector(widgets::Selector::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::Entry(spec) => {
                Widget::Entry(widgets::Entry::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::Frame(spec) => {
                Widget::Frame(containers::Frame::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::Box(spec) => {
                Widget::Box(containers::Box::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::BoxChild(view::BoxChild { widget, .. }) => {
                Widget::new(ctx, (&*widget).clone(), scope, selected_path)
            }
            view::WidgetKind::Grid(spec) => {
                Widget::Grid(containers::Grid::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::GridChild(view::GridChild { widget, .. }) => {
                Widget::new(ctx, (&*widget).clone(), scope, selected_path)
            }
            view::WidgetKind::GridRow(_) => {
                let s = Value::String(Chars::from("orphaned grid row"));
                let spec = ExprKind::Constant(s).to_expr();
                Widget::Label(widgets::Label::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::Paned(spec) => {
                Widget::Paned(containers::Paned::new(ctx, spec, scope, selected_path))
            }
            view::WidgetKind::NotebookPage(view::NotebookPage { widget, .. }) => {
                Widget::new(ctx, (&*widget).clone(), scope, selected_path)
            }
            view::WidgetKind::Notebook(spec) => Widget::Notebook(
                containers::Notebook::new(ctx, spec, scope, selected_path),
            ),
            view::WidgetKind::LinePlot(spec) => {
                Widget::LinePlot(widgets::LinePlot::new(ctx, spec, scope, selected_path))
            }
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
            Widget::Image(t) => Some(t.root()),
            Widget::Label(t) => Some(t.root()),
            Widget::Button(t) => Some(t.root()),
            Widget::LinkButton(t) => Some(t.root()),
            Widget::Toggle(t) => Some(t.root()),
            Widget::Selector(t) => Some(t.root()),
            Widget::Entry(t) => Some(t.root()),
            Widget::Frame(t) => Some(t.root()),
            Widget::Box(t) => Some(t.root()),
            Widget::Grid(t) => Some(t.root()),
            Widget::Paned(t) => Some(t.root()),
            Widget::Notebook(t) => Some(t.root()),
            Widget::LinePlot(t) => Some(t.root()),
        }
    }

    fn update(
        &mut self,
        ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        match self {
            Widget::Action(t) => t.update(ctx, event),
            Widget::Table(t) => t.update(ctx, waits, event),
            Widget::Image(t) => t.update(ctx, event),
            Widget::Label(t) => t.update(ctx, event),
            Widget::Button(t) => t.update(ctx, event),
            Widget::LinkButton(t) => t.update(ctx, event),
            Widget::Toggle(t) => t.update(ctx, event),
            Widget::Selector(t) => t.update(ctx, event),
            Widget::Entry(t) => t.update(ctx, event),
            Widget::Frame(t) => t.update(ctx, waits, event),
            Widget::Box(t) => t.update(ctx, waits, event),
            Widget::Grid(t) => t.update(ctx, waits, event),
            Widget::Paned(t) => t.update(ctx, waits, event),
            Widget::Notebook(t) => t.update(ctx, waits, event),
            Widget::LinePlot(t) => t.update(ctx, event),
        }
    }

    fn set_highlight(&self, mut path: impl Iterator<Item = WidgetPath>, h: bool) {
        fn set<T: WidgetExt>(w: &T, h: bool) {
            let s = w.style_context();
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
                (WidgetPath::Box(i), Widget::Notebook(w)) => {
                    if i < w.children.len() {
                        w.children[i].set_highlight(path, h)
                    }
                }
                (WidgetPath::Box(i), Widget::Frame(w)) => {
                    if i == 0 {
                        if let Some(c) = &w.child {
                            c.set_highlight(path, h)
                        }
                    }
                }
                (WidgetPath::Box(i), Widget::Paned(w)) => {
                    let c = if i == 0 {
                        &w.first_child
                    } else if i == 1 {
                        &w.second_child
                    } else {
                        &None
                    };
                    if let Some(c) = c {
                        c.set_highlight(path, h)
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
                (WidgetPath::Leaf, Widget::Paned(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Frame(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Notebook(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Box(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Grid(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Action(_)) => (),
                (WidgetPath::Leaf, Widget::Table(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Image(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Label(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Button(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::LinkButton(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Toggle(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Selector(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::Entry(w)) => set(w.root(), h),
                (WidgetPath::Leaf, Widget::LinePlot(w)) => set(w.root(), h),
            },
        }
    }
}

fn make_crumbs(ctx: &BSCtx, loc: &ViewLoc) -> gtk::Box {
    let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
    let ask_saved =
        Rc::new(clone!(@weak ctx, @weak root => @default-return true, move || {
            if !ctx.borrow().user.view_saved.get() {
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
                @strong ask_saved => @default-return Inhibit(false), move |_, uri| {
                    if !ask_saved() {
                        return Inhibit(false)
                    }
                    ctx.borrow().user.backend.navigate(
                        ViewLoc::Netidx(Path::from(String::from(uri)))
                    );
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
                    ctx.borrow().user.backend.navigate(ViewLoc::Netidx(Path::from("/")));
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
                    ctx.borrow().user.backend.navigate(ViewLoc::File(name.clone()));
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
    fn new(ctx: &BSCtx, path: &ViewLoc, spec: view::View) -> View {
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
            Widget::new(ctx, spec.root.clone(), Path::root(), selected_path.clone());
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        root.set_margin(2);
        root.add(&make_crumbs(ctx, path));
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
        &mut self,
        ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        self.widget.update(ctx, waits, event);
    }
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
    let root = d.content_area();
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
        let id = cb.active_id();
        match id.as_ref().map(|i| &**i) {
            Some("File") => {
                let w = gtk::FileChooserWidget::new(
                    if save {
                        gtk::FileChooserAction::Save
                    } else {
                        gtk::FileChooserAction::Open
                    });
                w.connect_selection_changed(clone!(@strong loc => move |w| {
                    *loc.borrow_mut() = w.filename().map(|f| ViewLoc::File(f));
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
                    let p = Path::from(String::from(e.text()));
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
    ctx: &BSCtx,
    save_loc: &Rc<RefCell<Option<ViewLoc>>>,
    current_spec: &Rc<RefCell<view::View>>,
    save_button: &gtk::ToolButton,
    save_as: bool,
) {
    let do_save = |loc: ViewLoc| {
        glib::MainContext::default().spawn_local({
            let save_button = save_button.clone();
            let save_loc = save_loc.clone();
            let spec = current_spec.borrow().clone();
            let ctx = ctx.clone();
            async move {
                let ctx_r = ctx.borrow();
                match ctx_r.user.backend.save(loc.clone(), spec).await {
                    Err(e) => {
                        let _: result::Result<_, _> =
                            ctx_r.user.backend.to_gui.send(ToGui::SaveError(format!(
                                "error saving to: {:?}, {}",
                                &*save_loc.borrow(),
                                e
                            )));
                        *save_loc.borrow_mut() = None;
                    }
                    Ok(()) => {
                        ctx_r.user.view_saved.set(true);
                        save_button.set_sensitive(false);
                        let mut sl = save_loc.borrow_mut();
                        if sl.as_ref() != Some(&loc) {
                            *sl = Some(loc.clone());
                            ctx_r.user.backend.navigate(loc.clone());
                        }
                    }
                }
            }
        });
    };
    let sl = save_loc.borrow_mut();
    match &*sl {
        Some(loc) if !save_as => do_save(loc.clone()),
        _ => match choose_location(&ctx.borrow().user.window, true) {
            None => (),
            Some(loc) => do_save(loc),
        },
    }
}

lazy_static! {
    static ref WAITS: Pool<Vec<oneshot::Receiver<()>>> = Pool::new(10, 100);
}

fn update_single(
    current: &Rc<RefCell<Option<View>>>,
    ctx: BSCtxRef,
    event: &vm::Event<LocalEvent>,
) {
    if let Some(root) = &mut *current.borrow_mut() {
        let mut waits = Vec::new();
        root.update(ctx, &mut waits, event);
        assert!(waits.is_empty());
    }
}

fn run_gui(ctx: BSCtx, app: Application, to_gui: glib::Receiver<ToGui>) {
    let group = gtk::WindowGroup::new();
    group.add_window(&ctx.borrow().user.window);
    let headerbar = gtk::HeaderBar::new();
    let design_mode = gtk::ToggleButton::new();
    let design_img = gtk::Image::from_icon_name(
        Some("document-page-setup"),
        gtk::IconSize::SmallToolbar,
    );
    let save_img = gtk::Image::from_icon_name(
        Some("media-floppy-symbolic"),
        gtk::IconSize::SmallToolbar,
    );
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
    {
        let w = &ctx.borrow().user.window;
        w.set_titlebar(Some(&headerbar));
        w.set_title("Netidx browser");
        w.set_default_size(800, 600);
        w.show_all();
        if let Some(screen) = w.screen() {
            setup_css(&screen);
        }
    }
    let save_loc: Rc<RefCell<Option<ViewLoc>>> = Rc::new(RefCell::new(None));
    let current_loc: Rc<RefCell<ViewLoc>> = ctx.borrow().user.current_loc.clone();
    let current_spec: Rc<RefCell<view::View>> =
        Rc::new(RefCell::new(default_view(Path::from("/"))));
    let current: Rc<RefCell<Option<View>>> = Rc::new(RefCell::new(None));
    let editor: Rc<RefCell<Option<Editor>>> = Rc::new(RefCell::new(None));
    let editor_window: Rc<RefCell<Option<gtk::Window>>> = Rc::new(RefCell::new(None));
    let highlight: Rc<RefCell<Vec<WidgetPath>>> = Rc::new(RefCell::new(vec![]));
    ctx.borrow().user.window.connect_delete_event(clone!(
        @weak ctx => @default-return Inhibit(false), move |w, _| {
            let saved = ctx.borrow().user.view_saved.get();
            if saved || ask_modal(w, "Unsaved view will be lost.") {
                ctx.borrow().user.backend.terminate();
                Inhibit(false)
            } else {
                Inhibit(true)
            }
    }));
    design_mode.connect_toggled(clone!(
    @strong editor_window,
    @strong editor,
    @strong highlight,
    @strong current,
    @strong current_spec,
    @weak ctx => move |b| {
        if b.is_active() {
            editor_window.borrow_mut().take();
            editor.borrow_mut().take();
            let s = current_spec.borrow().clone();
            let e = Editor::new(ctx, Path::root(), s);
            let win = gtk::Window::builder()
                .default_width(800)
                .default_height(600)
                .type_(gtk::WindowType::Toplevel)
                .visible(true)
                .build();
            win.connect_destroy(clone!(@strong b, @strong editor_window => move |_| {
                editor_window.borrow_mut().take();
                b.set_active(false);
            }));
            win.add(e.root());
            win.show_all();
            *editor_window.borrow_mut() = Some(win);
            *editor.borrow_mut() = Some(e);
        } else {
            if let Some(win) = editor_window.borrow_mut().take() {
                win.close();
            }
            editor.borrow_mut().take();
            if let Some(cur) = &*current.borrow() {
                let hl = highlight.borrow();
                cur.widget.set_highlight(hl.iter().copied(), false);
            }
            highlight.borrow_mut().clear();
        }
    }));
    save_button.connect_clicked(clone!(
        @strong save_loc,
        @strong current_spec,
        @weak ctx => move |b| {
            save_view(&ctx, &save_loc, &current_spec, b, false)
        }
    ));
    let go_act = gio::SimpleAction::new("go", None);
    ctx.borrow().user.window.add_action(&go_act);
    go_act.connect_activate(clone!(@weak ctx => move |_, _| {
        let u = &ctx.borrow().user;
        if u.view_saved.get()
            || ask_modal(&u.window, "Unsaved view will be lost.") {
            if let Some(loc) = choose_location(&u.window, false) {
                u.backend.navigate(loc);
            }
        }
    }));
    let save_as_act = gio::SimpleAction::new("save_as", None);
    ctx.borrow().user.window.add_action(&save_as_act);
    save_as_act.connect_activate(clone!(
        @strong save_loc,
        @strong current_spec,
        @weak ctx,
        @strong save_button => move |_, _| {
            save_view(&ctx, &save_loc, &current_spec, &save_button, true)
        }
    ));
    let raw_view_act =
        gio::SimpleAction::new_stateful("raw_view", None, &false.to_variant());
    ctx.borrow().user.window.add_action(&raw_view_act);
    raw_view_act.connect_activate(clone!(
        @weak ctx, @strong current_loc  => move |a, _| {
        if let Some(v) = a.state() {
            let new_v = !v.get::<bool>().expect("invalid state");
            let m = "Unsaved view will be lost.";
            let u = &ctx.borrow().user;
            if !new_v || u.view_saved.get() || ask_modal(&u.window, m) {
                u.raw_view.store(new_v, Ordering::Relaxed);
                a.change_state(&new_v.to_variant());
                u.backend.navigate(current_loc.borrow().clone());
            }
        }
    }));
    let new_window_act = gio::SimpleAction::new("new_window", None);
    ctx.borrow().user.window.add_action(&new_window_act);
    new_window_act.connect_activate(clone!(@weak app => move |_, _| app.activate()));
    to_gui.attach(None, move |m| match m {
        ToGui::UpdateVar(scope, name, value) => {
            update_single(
                &current,
                &mut ctx.borrow_mut(),
                &vm::Event::Variable(scope, name, value),
            );
            Continue(true)
        }
        ToGui::UpdateRpc(id, value) => {
            update_single(&current, &mut ctx.borrow_mut(), &vm::Event::Rpc(id, value));
            Continue(true)
        }
        ToGui::Update(mut batch) => {
            if let Some(root) = &mut *current.borrow_mut() {
                let mut waits = WAITS.take();
                for (id, value) in batch.drain(..) {
                    root.update(
                        &mut ctx.borrow_mut(),
                        &mut *waits,
                        &vm::Event::Netidx(id, value),
                    );
                }
                if waits.len() == 0 {
                    ctx.borrow().user.backend.updated()
                } else {
                    let ctx = ctx.clone();
                    glib::MainContext::default().spawn_local(async move {
                        for r in waits.drain(..) {
                            let _: result::Result<_, _> = r.await;
                        }
                        ctx.borrow().user.backend.updated();
                    });
                }
            }
            Continue(true)
        }
        ToGui::TableResolved(path, table) => {
            let e = vm::Event::User(LocalEvent::TableResolved(path, table));
            update_single(&current, &mut ctx.borrow_mut(), &e);
            Continue(true)
        }
        ToGui::NavigateInWindow(loc) => {
            *ctx.borrow().user.new_window_loc.borrow_mut() = loc;
            app.activate();
            Continue(true)
        }
        ToGui::View { loc, spec, generated } => {
            match loc {
                None => {
                    ctx.borrow().user.view_saved.set(false);
                    save_button.set_sensitive(true);
                }
                Some(loc) => {
                    ctx.borrow().user.view_saved.set(true);
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
                    if design_mode.is_active() {
                        design_mode.set_active(false);
                    }
                }
            }
            if let Some(cur) = current.borrow_mut().take() {
                ctx.borrow().user.window.remove(cur.root());
            }
            ctx.borrow_mut().clear();
            *current_spec.borrow_mut() = spec.clone();
            let cur = View::new(&ctx, &*current_loc.borrow(), spec);
            let window = ctx.borrow().user.window.clone();
            window.set_title(&format!("Netidx Browser {}", &*current_loc.borrow()));
            window.add(cur.root());
            window.show_all();
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
        ToGui::ShowError(s) => {
            err_modal(&ctx.borrow().user.window, &s);
            Continue(true)
        }
        ToGui::SaveError(s) => {
            err_modal(&ctx.borrow().user.window, &s);
            idle_add_local(clone!(
                @weak ctx,
                @strong save_loc,
                @strong current_spec,
                @strong save_button => @default-return Continue(false), move || {
                    save_view(
                        &ctx,
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

fn add_local_options(application: &gtk::Application) {
    application.add_main_option(
        "config",
        glib::Char::from(b'c'),
        glib::OptionFlags::empty(),
        glib::OptionArg::String,
        "use the specified config file instead of the default",
        Some("the config file to use"),
    );
    application.add_main_option(
        "auth",
        glib::Char::from(b'a'),
        glib::OptionFlags::empty(),
        glib::OptionArg::String,
        "override the default auth mechanism (krb5)",
        Some("[krb5, local, or anonymous]"),
    );
    application.add_main_option(
        "upn",
        glib::Char::from(b'u'),
        glib::OptionFlags::empty(),
        glib::OptionArg::String,
        "set the krb5 user principal name (the current user)",
        Some("the name"),
    );
    application.add_main_option(
        "path",
        glib::Char::from(b'p'),
        glib::OptionFlags::empty(),
        glib::OptionArg::String,
        "navigate to the specified path on load (/)",
        Some("path"),
    );
    application.add_main_option(
        "file",
        glib::Char::from(b'f'),
        glib::OptionFlags::empty(),
        glib::OptionArg::String,
        "load the specified view file on load",
        Some("file"),
    );
}

fn main() {
    env_logger::init();
    let application = Application::new(
        Some("org.netidx.browser"),
        gio::ApplicationFlags::NON_UNIQUE | Default::default(),
    );
    add_local_options(&application);
    application.connect_handle_local_options(|application, opts| {
        let cfg = match opts.lookup_value("config", Some(&glib::VariantTy::STRING)) {
            None => Config::load_default().unwrap(),
            Some(path) => Config::load(path.get::<String>().unwrap()).unwrap(),
        };
        let auth = match opts.lookup_value("auth", Some(&glib::VariantTy::STRING)) {
            None => DesiredAuth::Krb5 { upn: None, spn: None },
            Some(auth) => match auth
                .get::<String>()
                .unwrap()
                .parse::<DesiredAuth>()
                .expect("invalid auth mechanism")
            {
                auth @ (DesiredAuth::Local | DesiredAuth::Anonymous) => auth,
                DesiredAuth::Krb5 { .. } => {
                    match opts.lookup_value("upn", Some(&glib::VariantTy::STRING)) {
                        None => DesiredAuth::Krb5 { upn: None, spn: None },
                        Some(upn) => {
                            DesiredAuth::Krb5 { upn: upn.get::<String>(), spn: None }
                        }
                    }
                }
            },
        };
        let default_loc = match opts.lookup_value("path", Some(&glib::VariantTy::STRING))
        {
            Some(path) => ViewLoc::Netidx(Path::from(path.get::<String>().unwrap())),
            None => match opts.lookup_value("file", Some(&glib::VariantTy::STRING)) {
                Some(file) => ViewLoc::File(PathBuf::from(file.get::<String>().unwrap())),
                None => ViewLoc::Netidx(Path::from("/")),
            },
        };
        let (jh, backend) = backend::Backend::new(cfg, auth);
        let new_window_loc = Rc::new(RefCell::new(default_loc.clone()));
        application.connect_activate({
            let backend = backend.clone();
            move |app| {
                let app = app.clone();
                let (tx_to_gui, rx_to_gui) = glib::MainContext::channel(PRIORITY_LOW);
                let raw_view = Arc::new(AtomicBool::new(false));
                let backend = backend.create_ctx(tx_to_gui, raw_view.clone()).unwrap();
                let _ = backend.from_gui.unbounded_send(FromGui::Navigate(mem::replace(
                    &mut *new_window_loc.borrow_mut(),
                    default_loc.clone(),
                )));
                let window = ApplicationWindow::new(&app);
                let ctx = Rc::new(RefCell::new(bscript::create_ctx(WidgetCtx {
                    backend,
                    raw_view,
                    window: window.clone(),
                    new_window_loc: new_window_loc.clone(),
                    current_loc: Rc::new(RefCell::new(default_loc.clone())),
                    view_saved: Cell::new(true),
                    fns: Trie::new(),
                    vars: Trie::new(),
                })));
                run_gui(ctx, app, rx_to_gui);
            }
        });
        let jh = RefCell::new(Some(jh));
        application.connect_shutdown(move |_| {
            backend.stop();
            if let Some(jh) = jh.borrow_mut().take() {
                let _: result::Result<_, _> = jh.join();
            }
        });
        -1
    });
    application.run();
}
