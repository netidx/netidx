#![recursion_limit = "2048"]
#[macro_use]
extern crate glib;
#[macro_use]
extern crate lazy_static;

mod backend;
mod bscript;
mod containers;
mod editor;
mod table;
mod util;
mod view;
mod widgets;

use anyhow::Result;
use bscript::Target;
use editor::Editor;
use futures::channel::oneshot;
use gdk::{self, prelude::*};
use gio::{self, prelude::*};
use glib::{clone, idle_add_local, source::PRIORITY_LOW};
use gtk::{self, prelude::*, Adjustment, Application, ApplicationWindow};
use netidx::{
    chars::Chars,
    config::{self, Config},
    path::Path,
    pool::{Pool, Pooled},
    resolver::Auth,
    subscriber::{Event, SubId, Value},
};
use netidx_bscript::{
    expr::ExprKind,
    vm::{ExecCtx, Node},
};
use netidx_protocols::view as protocol_view;
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
use structopt::StructOpt;
use util::{ask_modal, err_modal};

type BSNode = Node<WidgetCtx, Target>;
type BSCtx = ExecCtx<WidgetCtx, Target>;
type Batch = Pooled<Vec<(SubId, Value)>>;
type RawBatch = Pooled<Vec<(SubId, Event)>>;

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
    NavigateInWindow(ViewLoc),
    Highlight(Vec<WidgetPath>),
    Update(Batch),
    UpdateVar(Chars, Value),
    UpdateRpc(Path, Value),
    ShowError(String),
    SaveError(String),
    Terminate,
}

#[derive(Debug)]
enum FromGui {
    Navigate(ViewLoc),
    Render(protocol_view::View),
    Save(ViewLoc, protocol_view::View, oneshot::Sender<Result<()>>),
    CallRpc(Path, Vec<(Chars, Value)>),
    Updated,
    Terminate,
}

struct WidgetCtx {
    backend: backend::Ctx,
    raw_view: Arc<AtomicBool>,
    window: gtk::ApplicationWindow,
    new_window_loc: Rc<RefCell<ViewLoc>>,
    view_saved: Cell<bool>,
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
    fn new(ctx: &BSCtx, spec: view::Widget, selected_path: gtk::Label) -> Widget {
        let w = match spec.kind {
            view::WidgetKind::Action(spec) => {
                Widget::Action(widgets::Action::new(ctx, spec))
            }
            view::WidgetKind::Table(base_path, spec) => {
                let tbl = table::Table::new(ctx.clone(), base_path, spec, selected_path);
                // force the initial update/subscribe
                tbl.start_update_task(None);
                Widget::Table(tbl)
            }
            view::WidgetKind::Label(spec) => {
                Widget::Label(widgets::Label::new(ctx, spec, selected_path))
            }
            view::WidgetKind::Button(spec) => {
                Widget::Button(widgets::Button::new(ctx, spec, selected_path))
            }
            view::WidgetKind::Toggle(spec) => {
                Widget::Toggle(widgets::Toggle::new(ctx, spec, selected_path))
            }
            view::WidgetKind::Selector(spec) => {
                Widget::Selector(widgets::Selector::new(ctx, spec, selected_path))
            }
            view::WidgetKind::Entry(spec) => {
                Widget::Entry(widgets::Entry::new(ctx, spec, selected_path))
            }
            view::WidgetKind::Box(s) => {
                Widget::Box(containers::Box::new(ctx, s, selected_path))
            }
            view::WidgetKind::BoxChild(view::BoxChild { widget, .. }) => {
                Widget::new(ctx, (&*widget).clone(), selected_path)
            }
            view::WidgetKind::Grid(spec) => {
                Widget::Grid(containers::Grid::new(ctx, spec, selected_path))
            }
            view::WidgetKind::GridChild(view::GridChild { widget, .. }) => {
                Widget::new(ctx, (&*widget).clone(), selected_path)
            }
            view::WidgetKind::GridRow(_) => {
                let s = Value::String(Chars::from("orphaned grid row"));
                let spec = ExprKind::Constant(s).to_expr();
                Widget::Label(widgets::Label::new(ctx, spec, selected_path))
            }
            view::WidgetKind::LinePlot(spec) => {
                Widget::LinePlot(widgets::LinePlot::new(ctx, spec, selected_path))
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

    fn update(
        &self,
        ctx: &BSCtx,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &Target,
    ) {
        match self {
            Widget::Action(t) => t.update(ctx, event),
            Widget::Table(t) => t.update(ctx, waits, event),
            Widget::Label(t) => t.update(ctx, event),
            Widget::Button(t) => t.update(ctx, event),
            Widget::Toggle(t) => t.update(ctx, event),
            Widget::Selector(t) => t.update(ctx, event),
            Widget::Entry(t) => t.update(ctx, event),
            Widget::Box(t) => t.update(ctx, waits, event),
            Widget::Grid(t) => t.update(ctx, waits, event),
            Widget::LinePlot(t) => t.update(ctx, event),
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

fn make_crumbs(ctx: &BSCtx, loc: &ViewLoc) -> gtk::Box {
    let root = gtk::Box::new(gtk::Orientation::Horizontal, 5);
    let ask_saved =
        Rc::new(clone!(@weak ctx, @weak root => @default-return true, move || {
            if !ctx.user.view_saved.get() {
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
                    ctx.user.backend.navigate(
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
                    ctx.user.backend.navigate(ViewLoc::Netidx(Path::from("/")));
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
                    ctx.user.backend.navigate(ViewLoc::File(name.clone()));
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
        let widget = Widget::new(ctx, spec.root.clone(), selected_path.clone());
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        root.set_property_margin(2);
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
        &self,
        ctx: &BSCtx,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &Target,
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
    ctx: &BSCtx,
    save_loc: &Rc<RefCell<Option<ViewLoc>>>,
    current_spec: &Rc<RefCell<protocol_view::View>>,
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
                match ctx.user.backend.save(loc.clone(), spec).await {
                    Err(e) => {
                        let _: result::Result<_, _> =
                            ctx.user.backend.to_gui.send(ToGui::SaveError(format!(
                                "error saving to: {:?}, {}",
                                &*save_loc.borrow(),
                                e
                            )));
                        *save_loc.borrow_mut() = None;
                    }
                    Ok(()) => {
                        ctx.user.view_saved.set(true);
                        save_button.set_sensitive(false);
                        let mut sl = save_loc.borrow_mut();
                        if sl.as_ref() != Some(&loc) {
                            *sl = Some(loc.clone());
                            ctx.user.backend.navigate(loc.clone());
                        }
                    }
                }
            }
        });
    };
    let sl = save_loc.borrow_mut();
    match &*sl {
        Some(loc) if !save_as => do_save(loc.clone()),
        _ => match choose_location(&ctx.user.window, true) {
            None => (),
            Some(loc) => do_save(loc),
        },
    }
}

lazy_static! {
    static ref WAITS: Pool<Vec<oneshot::Receiver<()>>> = Pool::new(10, 100);
}

fn update_single(current: &Rc<RefCell<Option<View>>>, ctx: &BSCtx, event: &Target) {
    if let Some(root) = &mut *current.borrow_mut() {
        let mut waits = Vec::new();
        root.update(ctx, &mut waits, event);
        assert!(waits.is_empty());
    }
}

fn run_gui(ctx: BSCtx, app: Application, to_gui: glib::Receiver<ToGui>) {
    let group = gtk::WindowGroup::new();
    group.add_window(&ctx.user.window);
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
    ctx.user.window.set_titlebar(Some(&headerbar));
    ctx.user.window.set_title("Netidx browser");
    ctx.user.window.set_default_size(800, 600);
    ctx.user.window.add(&mainbox);
    ctx.user.window.show_all();
    if let Some(screen) = ctx.user.window.get_screen() {
        setup_css(&screen);
    }
    let save_loc: Rc<RefCell<Option<ViewLoc>>> = Rc::new(RefCell::new(None));
    let current_loc: Rc<RefCell<ViewLoc>> =
        Rc::new(RefCell::new(ViewLoc::Netidx(Path::from("/"))));
    let current_spec: Rc<RefCell<protocol_view::View>> =
        Rc::new(RefCell::new(default_view(Path::from("/"))));
    let current: Rc<RefCell<Option<View>>> = Rc::new(RefCell::new(None));
    let editor: Rc<RefCell<Option<Editor>>> = Rc::new(RefCell::new(None));
    let highlight: Rc<RefCell<Vec<WidgetPath>>> = Rc::new(RefCell::new(vec![]));
    ctx.user.window.connect_delete_event(clone!(
        @weak ctx => @default-return Inhibit(false), move |w, _| {
            if ctx.user.view_saved.get() || ask_modal(w, "Unsaved view will be lost.") {
                ctx.user.backend.terminate();
                Inhibit(false)
            } else {
                Inhibit(true)
            }
    }));
    design_mode.connect_toggled(clone!(
    @weak mainbox,
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
            let e = Editor::new(ctx, s);
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
        @strong save_loc,
        @strong current_spec,
        @weak ctx => move |b| {
            save_view(&ctx, &save_loc, &current_spec, b, false)
        }
    ));
    let go_act = gio::SimpleAction::new("go", None);
    ctx.user.window.add_action(&go_act);
    go_act.connect_activate(clone!(@weak ctx => move |_, _| {
        if ctx.user.view_saved.get()
            || ask_modal(&ctx.user.window, "Unsaved view will be lost.") {
            if let Some(loc) = choose_location(&ctx.user.window, false) {
                ctx.user.backend.navigate(loc);
            }
        }
    }));
    let save_as_act = gio::SimpleAction::new("save_as", None);
    ctx.user.window.add_action(&save_as_act);
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
    ctx.user.window.add_action(&raw_view_act);
    raw_view_act.connect_activate(clone!(
        @weak ctx, @strong current_loc  => move |a, _| {
        if let Some(v) = a.get_state() {
            let new_v = !v.get::<bool>().expect("invalid state");
            let m = "Unsaved view will be lost.";
            if !new_v || ctx.user.view_saved.get() || ask_modal(&ctx.user.window, m) {
                ctx.user.raw_view.store(new_v, Ordering::Relaxed);
                a.change_state(&new_v.to_variant());
                ctx.user.backend.navigate(current_loc.borrow().clone());
            }
        }
    }));
    let new_window_act = gio::SimpleAction::new("new_window", None);
    ctx.user.window.add_action(&new_window_act);
    new_window_act.connect_activate(clone!(@weak app => move |_, _| app.activate()));
    to_gui.attach(None, move |m| match m {
        ToGui::UpdateVar(name, value) => {
            update_single(&current, &ctx, &Target::Variable(name, value));
            Continue(true)
        }
        ToGui::UpdateRpc(path, value) => {
            let name = Chars::from(String::from(&*path));
            update_single(&current, &ctx, &Target::Rpc(name, value));
            Continue(true)
        }
        ToGui::Update(mut batch) => {
            if let Some(root) = &mut *current.borrow_mut() {
                let mut waits = WAITS.take();
                for (id, value) in batch.drain(..) {
                    root.update(&ctx, &mut *waits, &Target::Netidx(id, value));
                }
                if waits.len() == 0 {
                    ctx.user.backend.updated()
                } else {
                    let ctx = ctx.clone();
                    glib::MainContext::default().spawn_local(async move {
                        for r in waits.drain(..) {
                            let _: result::Result<_, _> = r.await;
                        }
                        ctx.user.backend.updated();
                    });
                }
            }
            Continue(true)
        }
        ToGui::NavigateInWindow(loc) => {
            *ctx.user.new_window_loc.borrow_mut() = loc;
            app.activate();
            Continue(true)
        }
        ToGui::View { loc, original, raeified, generated } => {
            match loc {
                None => {
                    ctx.user.view_saved.set(false);
                    save_button.set_sensitive(true);
                }
                Some(loc) => {
                    ctx.user.view_saved.set(true);
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
            ctx.variables.borrow_mut().clear();
            *current_spec.borrow_mut() = original.clone();
            ctx.dbg_ctx.borrow_mut().clear();
            let cur = View::new(&ctx, &*current_loc.borrow(), raeified);
            ctx.user
                .window
                .set_title(&format!("Netidx Browser {}", &*current_loc.borrow()));
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
        ToGui::ShowError(s) => {
            err_modal(&ctx.user.window, &s);
            Continue(true)
        }
        ToGui::SaveError(s) => {
            err_modal(&ctx.user.window, &s);
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
    let (jh, backend) = backend::Backend::new(cfg, auth);
    let default_loc = match &opt.path {
        Some(path) => ViewLoc::Netidx(path.clone()),
        None => match &opt.file {
            Some(file) => ViewLoc::File(file.clone()),
            None => ViewLoc::Netidx(Path::from("/")),
        },
    };
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
            let ctx = bscript::create_ctx(WidgetCtx {
                backend,
                raw_view,
                window: window.clone(),
                new_window_loc: new_window_loc.clone(),
                view_saved: Cell::new(true),
            });
            run_gui(ctx, app, rx_to_gui);
        }
    });
    application.run(&[]);
    backend.stop();
    drop(application);
    let _: result::Result<_, _> = jh.join();
}
