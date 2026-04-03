#![recursion_limit = "2048"]

mod backend;
mod builtins;
mod cairo_backend;
mod containers;
mod editor;
mod lineplot;
mod table;
mod util;
mod view;
mod widgets;

use anyhow::{anyhow, bail, Result};
use arcstr::ArcStr;
use bytes::Bytes;
use editor::Editor;
use fxhash::{FxBuildHasher, FxHashMap};
use gdk::{self, prelude::*};
use glib::{
    clone, idle_add_local, idle_add_local_once, source::Priority, ControlFlow,
    Propagation,
};
use graphix_compiler::expr::ExprId;
use graphix_rt::{CompExp, NoExt};
use gtk::{self, prelude::*, Adjustment, Application, ApplicationWindow};
use indexmap::IndexSet;
use netidx::{
    config::Config,
    path::Path,
    protocol::value::FromValue,
    publisher::Value,
    subscriber::DesiredAuth,
};
use netidx_netproto::resolver;
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

/// Shared browser context accessible from widgets
pub(crate) type BCtx = Rc<RefCell<BrowserCtx>>;

pub(crate) struct BrowserCtx {
    pub(crate) backend: backend::Ctx,
    pub(crate) raw_view: Arc<AtomicBool>,
    pub(crate) window: gtk::ApplicationWindow,
    pub(crate) new_window_loc: Rc<RefCell<ViewLoc>>,
    pub(crate) current_loc: Rc<RefCell<ViewLoc>>,
    pub(crate) view_saved: Cell<bool>,
    pub(crate) radio_groups:
        FxHashMap<String, (Rc<Cell<bool>>, IndexSet<gtk::RadioButton, FxBuildHasher>)>,
}

/// A compiled graphix expression bound to a widget property.
/// When dropped, the graphix runtime cleans up the expression.
pub(crate) struct GxProp {
    pub(crate) exp: CompExp<NoExt>,
    pub(crate) last: Option<Value>,
}

impl GxProp {
    pub(crate) fn id(&self) -> ExprId {
        self.exp.id
    }

    /// Compile a GxExpr string into a live graphix expression.
    /// Returns None if the expression is null/empty.
    pub(crate) fn compile(
        ctx: &BCtx,
        expr: &view::GxExpr,
    ) -> Option<Self> {
        if expr.0.is_empty() {
            return None;
        }
        match ctx.borrow().backend.compile(&expr.0) {
            Ok(mut res) => {
                if res.exprs.is_empty() {
                    None
                } else {
                    Some(GxProp {
                        exp: res.exprs.remove(0),
                        last: None,
                    })
                }
            }
            Err(e) => {
                log::warn!("failed to compile expression '{}': {}", expr.0, e);
                None
            }
        }
    }

    /// Check if this update is for us and update the cached value
    pub(crate) fn update(&mut self, id: ExprId, value: &Value) -> bool {
        if self.exp.id == id {
            self.last = Some(value.clone());
            true
        } else {
            false
        }
    }
}

/// A compiled graphix callback - an expression that evaluates to a function.
/// When a GTK event fires, the callback is invoked with typed arguments
/// via the graphix Callable interface.
pub(crate) struct GxCallback {
    /// The compiled expression. When it evaluates to a function value,
    /// we compile a Callable from it.
    prop: GxProp,
    /// The compiled callable, ready to invoke. None if the expression
    /// hasn't evaluated to a function yet (late binding).
    callable: Option<graphix_rt::Callable<NoExt>>,
    /// Tokio runtime handle for spawning async calls from GTK thread
    rt_handle: tokio::runtime::Handle,
    /// GXHandle for compiling callables when the expression resolves
    gx: graphix_rt::GXHandle<NoExt>,
}

impl GxCallback {
    /// Compile a callback expression. Returns None if the expression
    /// is empty/null.
    pub(crate) fn compile(ctx: &BCtx, expr: &view::GxExpr) -> Option<Self> {
        let prop = GxProp::compile(ctx, expr)?;
        let ctx_ref = ctx.borrow();
        let rt_handle = ctx_ref.backend.rt_handle.clone();
        let gx = ctx_ref.backend.gx.clone();
        // Try to compile callable immediately if we already have a value
        let callable = prop.last.as_ref().and_then(|v| {
            rt_handle.block_on(gx.compile_callable(v.clone())).ok()
        });
        Some(GxCallback { prop, callable, rt_handle, gx })
    }

    /// Handle an update from the graphix runtime. If the expression
    /// value changed and is a function, recompile the Callable.
    pub(crate) fn update(&mut self, id: ExprId, value: &Value) -> bool {
        if self.prop.update(id, value) {
            // The expression value changed - it should be a function.
            // Recompile the callable.
            match self.rt_handle.block_on(self.gx.compile_callable(value.clone())) {
                Ok(c) => { self.callable = Some(c); }
                Err(e) => {
                    log::warn!("callback expression didn't produce a callable: {}", e);
                    self.callable = None;
                }
            }
            true
        } else if let Some(ref c) = self.callable {
            // Check if this is a return value from our callable
            c.update(id, value).is_some()
        } else {
            false
        }
    }

    /// Fire the callback with the given arguments.
    /// This spawns the async call on the tokio runtime.
    pub(crate) fn fire(&self, args: netidx::protocol::valarray::ValArray) {
        if let Some(ref callable) = self.callable {
            let callable_id = callable.id();
            let rt = self.gx.clone();
            let args = args.clone();
            self.rt_handle.spawn(async move {
                if let Err(e) = rt.call(callable_id, args) {
                    log::warn!("callback invocation failed: {}", e);
                }
            });
        }
    }

    pub(crate) fn id(&self) -> ExprId {
        self.prop.id()
    }
}

/// Messages from the backend/graphix to the GUI thread
#[derive(Debug, Clone)]
pub(crate) enum ToGui {
    View {
        loc: Option<ViewLoc>,
        spec: view::Widget,
        generated: bool,
    },
    Navigate(ViewLoc),
    NavigateInWindow(ViewLoc),
    Highlight(Vec<WidgetPath>),
    /// Batch of expression value updates from the graphix runtime
    Update(Vec<(ExprId, Value)>),
    /// Table structure resolved from netidx
    TableResolved(Path, netidx_netproto::resolver::Table),
    ShowError(String),
    SaveError(String),
    Terminate,
}

fn default_view(path: Path) -> view::Widget {
    view::Widget {
        props: None,
        kind: view::WidgetKind::Table(view::Table {
            path: view::GxExpr(format!("{:?}", &*path)),
            column_editable: view::GxExpr("false".into()),
            columns_resizable: view::GxExpr("true".into()),
            selection_mode: view::GxExpr("`Single".into()),
            on_activate: view::GxExpr("|path: string| browser_navigate(path)".into()),
            ..Default::default()
        }),
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum WidgetPath {
    Leaf,
    Box(usize),
    GridItem(usize, usize),
    GridRow(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ViewLoc {
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

#[derive(Debug)]
pub(crate) enum ImageSpec {
    Icon { name: ArcStr, size: gtk::IconSize },
    PixBuf { bytes: Bytes, width: Option<u32>, height: Option<u32>, keep_aspect: bool },
}

impl ImageSpec {
    fn get_pixbuf(&self) -> Option<gdk_pixbuf::Pixbuf> {
        match self {
            Self::Icon { .. } => None,
            Self::PixBuf { bytes, width, height, keep_aspect } => {
                let width = width.map(|i| i as i32).unwrap_or(-1);
                let height = height.map(|i| i as i32).unwrap_or(-1);
                let bytes = glib::Bytes::from_owned(bytes.clone());
                let stream = gio::MemoryInputStream::from_bytes(&bytes);
                gdk_pixbuf::Pixbuf::from_stream_at_scale(
                    &stream,
                    width,
                    height,
                    *keep_aspect,
                    gio::Cancellable::NONE,
                )
                .ok()
            }
        }
    }

    pub(crate) fn get(&self) -> gtk::Image {
        let image = gtk::Image::new();
        self.apply(&image);
        image
    }

    pub(crate) fn apply(&self, image: &gtk::Image) {
        match self {
            Self::Icon { name, size } => image.set_from_icon_name(Some(&**name), *size),
            Self::PixBuf { .. } => image.set_from_pixbuf(self.get_pixbuf().as_ref()),
        }
    }
}

impl FromValue for ImageSpec {
    fn from_value(v: Value) -> Result<Self> {
        match v {
            Value::String(name) => {
                Ok(Self::Icon { name, size: gtk::IconSize::SmallToolbar })
            }
            Value::Bytes(bytes) => {
                Ok(Self::PixBuf { bytes: bytes.into(), width: None, height: None, keep_aspect: true })
            }
            Value::Array(elts) => match &*elts {
                [Value::String(name), Value::String(size)] => {
                    let size = match &**size {
                        "menu" => gtk::IconSize::Menu,
                        "small-toolbar" => gtk::IconSize::SmallToolbar,
                        "large-toolbar" => gtk::IconSize::LargeToolbar,
                        "dnd" => gtk::IconSize::Dnd,
                        "dialog" => gtk::IconSize::Dialog,
                        _ => bail!("invalid size"),
                    };
                    Ok(Self::Icon { name: name.clone(), size })
                }
                _ => {
                    let mut alist =
                        Value::Array(elts).cast_to::<HashMap<ArcStr, Value>>()?;
                    let bytes = alist
                        .remove("image")
                        .ok_or_else(|| anyhow!("missing bytes"))?
                        .cast_to::<Bytes>()?;
                    let width =
                        alist.remove("width").and_then(|v: Value| v.cast_to::<u32>().ok());
                    let height =
                        alist.remove("height").and_then(|v: Value| v.cast_to::<u32>().ok());
                    let keep_aspect = alist
                        .remove("keep-aspect")
                        .and_then(|v: Value| v.cast_to::<bool>().ok())
                        .unwrap_or(true);
                    Ok(Self::PixBuf { bytes, width, height, keep_aspect })
                }
            },
            _ => bail!("expected bytes or array"),
        }
    }

    fn get(v: Value) -> Option<Self> {
        <Self as FromValue>::from_value(v).ok()
    }
}

pub(crate) fn val_to_bool(v: &Value) -> bool {
    match v {
        Value::Bool(false) | Value::Null => false,
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

fn set_common_props<T: IsA<gtk::Widget> + 'static>(props: &view::WidgetProps, t: &T) {
    t.set_halign(align_to_gtk(props.halign));
    t.set_valign(align_to_gtk(props.valign));
    t.set_hexpand(props.hexpand);
    t.set_vexpand(props.vexpand);
    t.set_margin_top(props.margin_top as i32);
    t.set_margin_bottom(props.margin_bottom as i32);
    t.set_margin_start(props.margin_start as i32);
    t.set_margin_end(props.margin_end as i32);
}

pub(crate) trait BWidget {
    /// Dispatch a graphix expression value update.
    /// Returns true if this widget handled the update.
    fn update(&mut self, id: ExprId, value: &Value) -> bool;
    fn root(&self) -> Option<&gtk::Widget>;

    /// Forward a table-resolved event to any table widgets in the
    /// tree. Returns true if any table handled it.
    fn table_resolved(
        &mut self,
        _path: &Path,
        _table: &resolver::Table,
    ) -> bool {
        false
    }

    fn set_visible(&self, v: bool) {
        if let Some(w) = self.root() {
            if v { w.show() } else { w.hide() }
        }
    }

    fn set_sensitive(&self, e: bool) {
        if let Some(w) = self.root() {
            w.set_sensitive(e);
        }
    }

    fn set_highlight(&self, mut path: std::slice::Iter<WidgetPath>, h: bool) {
        if let (Some(WidgetPath::Leaf), Some(w)) = (path.next(), self.root()) {
            util::set_highlight(w, h);
        }
    }
}

/// Wraps a BWidget with common properties (sensitive, visible)
pub(crate) struct Widget {
    sensitive: Option<GxProp>,
    visible: Option<GxProp>,
    widget: Box<dyn BWidget>,
}

static DEFAULT_PROPS: once_cell::sync::Lazy<view::WidgetProps> =
    once_cell::sync::Lazy::new(|| view::WidgetProps {
        halign: view::Align::Fill,
        valign: view::Align::Fill,
        hexpand: false,
        vexpand: false,
        margin_top: 0,
        margin_bottom: 0,
        margin_start: 0,
        margin_end: 0,
        keybinds: vec![],
        sensitive: view::GxExpr("true".into()),
        visible: view::GxExpr("true".into()),
    });

impl Widget {
    pub(crate) fn new(
        ctx: &BCtx,
        spec: view::Widget,
        selected_path: gtk::Label,
    ) -> Self {
        let widget: Box<dyn BWidget> = match spec.kind {
            view::WidgetKind::Table(spec) => {
                Box::new(table::Table::new(ctx, spec))
            }
            view::WidgetKind::Label(spec) => {
                Box::new(widgets::Label::new(ctx, spec))
            }
            view::WidgetKind::Button(spec) => {
                Box::new(widgets::Button::new(ctx, spec))
            }
            view::WidgetKind::BScript(spec) => {
                Box::new(widgets::BScript::new(ctx, spec))
            }
            view::WidgetKind::LinkButton(spec) => {
                Box::new(widgets::LinkButton::new(ctx, spec))
            }
            view::WidgetKind::Switch(spec) => {
                Box::new(widgets::Switch::new(ctx, spec))
            }
            view::WidgetKind::ToggleButton(spec) => {
                Box::new(widgets::ToggleButton::new(
                    ctx, spec, || gtk::ToggleButton::new(),
                ))
            }
            view::WidgetKind::CheckButton(spec) => {
                Box::new(widgets::ToggleButton::new(
                    ctx, spec, || gtk::CheckButton::new().upcast::<gtk::ToggleButton>(),
                ))
            }
            view::WidgetKind::ProgressBar(spec) => {
                Box::new(widgets::ProgressBar::new(ctx, spec))
            }
            view::WidgetKind::Scale(spec) => {
                Box::new(widgets::Scale::new(ctx, spec))
            }
            view::WidgetKind::ComboBox(spec) => {
                Box::new(widgets::ComboBox::new(ctx, spec))
            }
            view::WidgetKind::RadioButton(spec) => {
                Box::new(widgets::RadioButton::new(ctx, spec))
            }
            view::WidgetKind::Entry(spec) => {
                Box::new(widgets::Entry::new(ctx, spec))
            }
            view::WidgetKind::SearchEntry(spec) => {
                Box::new(widgets::SearchEntry::new(ctx, spec))
            }
            view::WidgetKind::Image(spec) => {
                Box::new(widgets::Image::new(ctx, spec))
            }
            view::WidgetKind::Frame(spec) => {
                Box::new(containers::Frame::new(ctx, spec, selected_path.clone()))
            }
            view::WidgetKind::Box(spec) => {
                Box::new(containers::Box::new(ctx, spec, selected_path.clone()))
            }
            view::WidgetKind::BoxChild(view::BoxChild { widget: w, .. }) => {
                Box::new(Widget::new(ctx, (*w).clone(), selected_path.clone()))
            }
            view::WidgetKind::Grid(spec) => {
                Box::new(containers::Grid::new(ctx, spec, selected_path.clone()))
            }
            view::WidgetKind::GridChild(view::GridChild { widget: w, .. }) => {
                Box::new(Widget::new(ctx, (*w).clone(), selected_path.clone()))
            }
            view::WidgetKind::GridRow(_) => {
                let spec = view::Label {
                    text: view::GxExpr(r#""orphaned grid row""#.into()),
                    selectable: view::GxExpr("true".into()),
                    single_line: view::GxExpr("true".into()),
                    ..Default::default()
                };
                Box::new(widgets::Label::new(ctx, spec))
            }
            view::WidgetKind::Paned(spec) => {
                Box::new(containers::Paned::new(ctx, spec, selected_path.clone()))
            }
            view::WidgetKind::NotebookPage(view::NotebookPage { widget: w, .. }) => {
                Box::new(Widget::new(ctx, (*w).clone(), selected_path.clone()))
            }
            view::WidgetKind::Notebook(spec) => {
                Box::new(containers::Notebook::new(ctx, spec, selected_path.clone()))
            }
            view::WidgetKind::LinePlot(spec) => {
                Box::new(lineplot::LinePlot::new(ctx, spec))
            }
        };
        let props = spec.props.as_ref().unwrap_or(&DEFAULT_PROPS);
        if let Some(r) = widget.root() {
            set_common_props(props, r);
        }
        let sensitive = GxProp::compile(ctx, &props.sensitive);
        let visible = GxProp::compile(ctx, &props.visible);
        if let Some(ref p) = sensitive {
            if let Some(ref v) = p.last {
                widget.set_sensitive(val_to_bool(v));
            }
        }
        if let Some(ref p) = visible {
            if let Some(ref v) = p.last {
                widget.set_visible(val_to_bool(v));
            }
        }
        Self { sensitive, visible, widget }
    }
}

impl BWidget for Widget {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.sensitive {
            if p.update(id, value) {
                self.widget.set_sensitive(val_to_bool(value));
                handled = true;
            }
        }
        if let Some(ref mut p) = self.visible {
            if p.update(id, value) {
                self.widget.set_visible(val_to_bool(value));
                handled = true;
            }
        }
        if self.widget.update(id, value) {
            handled = true;
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        self.widget.root()
    }

    fn set_visible(&self, v: bool) {
        self.widget.set_visible(v)
    }

    fn set_sensitive(&self, e: bool) {
        self.widget.set_sensitive(e);
    }

    fn table_resolved(
        &mut self,
        path: &Path,
        table: &resolver::Table,
    ) -> bool {
        self.widget.table_resolved(path, table)
    }

    fn set_highlight(&self, path: std::slice::Iter<WidgetPath>, h: bool) {
        self.widget.set_highlight(path, h)
    }
}

/// Placeholder for unimplemented widgets
struct PlaceholderWidget(gtk::Widget);

impl BWidget for PlaceholderWidget {
    fn update(&mut self, _id: ExprId, _value: &Value) -> bool {
        false
    }
    fn root(&self) -> Option<&gtk::Widget> {
        Some(&self.0)
    }
}

fn make_crumbs(ctx: &BCtx, loc: &ViewLoc) -> gtk::ScrolledWindow {
    let root = gtk::ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
    root.set_policy(gtk::PolicyType::Automatic, gtk::PolicyType::Never);
    let hbox = gtk::Box::new(gtk::Orientation::Horizontal, 5);
    root.add(&hbox);
    let ask_saved =
        Rc::new(clone!(@weak ctx, @weak hbox => @default-return true, move || {
            if !ctx.borrow().view_saved.get() {
                ask_modal(&hbox, "Unsaved view will be lost.")
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
                        hbox.add(&lbl);
                        lbl.set_margin_start(10);
                        " / "
                    }
                    Some(name) => {
                        hbox.add(&gtk::Label::new(Some(" > ")));
                        hbox.add(&lbl);
                        name
                    }
                };
                let target = glib::markup_escape_text(target);
                lbl.set_markup(&format!(r#"<a href="{}">{}</a>"#, &*target, &*name));
                lbl.connect_activate_link(clone!(
                    @weak ctx,
                    @strong ask_saved => @default-return Propagation::Proceed, move |_, uri| {
                        if !ask_saved() {
                            return Propagation::Proceed
                        }
                        ctx.borrow().backend.navigate(
                            ViewLoc::Netidx(Path::from(String::from(uri)))
                        );
                        Propagation::Proceed
                    }
                ));
            }
        }
        ViewLoc::File(name) => {
            let rl = gtk::Label::new(None);
            hbox.add(&rl);
            rl.set_margin_start(10);
            rl.set_markup(r#"<a href=""> / </a>"#);
            rl.connect_activate_link(clone!(
                @weak ctx,
                @strong ask_saved => @default-return Propagation::Proceed, move |_, _| {
                    if !ask_saved() {
                        return Propagation::Proceed
                    }
                    ctx.borrow().backend.navigate(ViewLoc::Netidx(Path::from("/")));
                    Propagation::Proceed
                }
            ));
            let sep = gtk::Label::new(Some(" > "));
            hbox.add(&sep);
            let fl = gtk::Label::new(None);
            hbox.add(&fl);
            fl.set_margin_start(10);
            fl.set_markup(&format!(r#"<a href="">file:{:?}</a>"#, name));
            fl.connect_activate_link(clone!(
                @weak ctx,
                @strong ask_saved,
                @strong name => @default-return Propagation::Proceed, move |_, _| {
                    if !ask_saved() {
                        return Propagation::Proceed
                    }
                    ctx.borrow().backend.navigate(ViewLoc::File(name.clone()));
                    Propagation::Proceed
                }
            ));
        }
    }
    idle_add_local_once(clone!(@weak root => move || {
        let a = root.hadjustment();
        a.set_value(a.upper());
    }));
    root
}

struct View {
    root: gtk::Box,
    widget: Widget,
}

impl View {
    fn new(ctx: &BCtx, path: &ViewLoc, spec: view::Widget) -> View {
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
        let widget = Widget::new(ctx, spec.clone(), selected_path.clone());
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

    fn update(&mut self, id: ExprId, value: &Value) {
        self.widget.update(id, value);
    }

    fn table_resolved(&mut self, path: &Path, table: &resolver::Table) {
        self.widget.table_resolved(path, table);
    }
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
    unsafe { d.destroy() };
    res
}

fn save_view(
    ctx: &BCtx,
    save_loc: &Rc<RefCell<Option<ViewLoc>>>,
    current_spec: &Rc<RefCell<view::Widget>>,
    save_button: &gtk::ToolButton,
    save_as: bool,
) {
    let do_save = |loc: ViewLoc| {
        glib::MainContext::default().spawn_local({
            let save_button = save_button.clone();
            let save_loc = save_loc.clone();
            let spec = current_spec.borrow().clone();
            let ctx = ctx.clone();
            let backend = ctx.borrow().backend.clone();
            async move {
                match backend.save(loc.clone(), spec).await {
                    Err(e) => {
                        let _: result::Result<_, _> =
                            backend.to_gui.send(ToGui::SaveError(format!(
                                "error saving to: {:?}, {}",
                                &*save_loc.borrow(),
                                e
                            )));
                        *save_loc.borrow_mut() = None;
                    }
                    Ok(()) => {
                        ctx.borrow().view_saved.set(true);
                        save_button.set_sensitive(false);
                        let mut sl = save_loc.borrow_mut();
                        if sl.as_ref() != Some(&loc) {
                            *sl = Some(loc.clone());
                            backend.navigate(loc.clone());
                        }
                    }
                }
            }
        });
    };
    let sl = save_loc.borrow_mut();
    match &*sl {
        Some(loc) if !save_as => do_save(loc.clone()),
        _ => {
            let window = ctx.borrow().window.clone();
            match choose_location(&window, true) {
                None => (),
                Some(loc) => do_save(loc),
            }
        }
    }
}

fn run_gui(ctx: BCtx, app: Application, to_gui: glib::Receiver<ToGui>) {
    let group = gtk::WindowGroup::new();
    group.add_window(&ctx.borrow().window);
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
        let w = &ctx.borrow().window;
        w.set_titlebar(Some(&headerbar));
        w.set_title("Netidx browser");
        w.set_default_size(800, 600);
        w.show_all();
        if let Some(screen) = WidgetExt::screen(w) {
            setup_css(&screen);
        }
    }
    let save_loc: Rc<RefCell<Option<ViewLoc>>> = Rc::new(RefCell::new(None));
    let current_loc: Rc<RefCell<ViewLoc>> = ctx.borrow().current_loc.clone();
    let current_spec: Rc<RefCell<view::Widget>> =
        Rc::new(RefCell::new(default_view(Path::from("/"))));
    let current: Rc<RefCell<Option<View>>> = Rc::new(RefCell::new(None));
    let editor: Rc<RefCell<Option<Editor>>> = Rc::new(RefCell::new(None));
    let editor_window: Rc<RefCell<Option<gtk::Window>>> = Rc::new(RefCell::new(None));
    let highlight: Rc<RefCell<Vec<WidgetPath>>> = Rc::new(RefCell::new(vec![]));
    ctx.borrow().window.connect_delete_event(clone!(
        @weak ctx => @default-return Propagation::Proceed, move |w, _| {
            let saved = ctx.borrow().view_saved.get();
            if saved || ask_modal(w, "Unsaved view will be lost.") {
                ctx.borrow().backend.terminate();
                Propagation::Proceed
            } else {
                Propagation::Stop
            }
        }
    ));
    design_mode.connect_toggled(clone!(
        @strong editor_window,
        @strong editor,
        @strong highlight,
        @strong current,
        @strong current_spec,
        @strong current_loc,
        @weak ctx => move |b| {
            if b.is_active() {
                let scope = match &*current_loc.borrow() {
                    ViewLoc::Netidx(p) => p.clone(),
                    ViewLoc::File(_) => Path::from("/"),
                };
                let ed = Editor::new(
                    &ctx,
                    scope,
                    current_spec.borrow().clone(),
                );
                let win = gtk::Window::builder()
                    .default_width(640)
                    .default_height(480)
                    .type_(gtk::WindowType::Toplevel)
                    .title("View Editor")
                    .visible(true)
                    .build();
                win.connect_destroy(clone!(@strong b, @strong editor_window => move |_| {
                    editor_window.borrow_mut().take();
                    b.set_active(false);
                }));
                win.add(ed.root());
                win.show_all();
                *editor_window.borrow_mut() = Some(win);
                *editor.borrow_mut() = Some(ed);
            } else {
                if let Some(win) = editor_window.borrow_mut().take() {
                    win.close();
                }
                editor.borrow_mut().take();
                if let Some(cur) = &*current.borrow() {
                    let hl = highlight.borrow();
                    cur.widget.set_highlight(hl.iter(), false);
                }
                highlight.borrow_mut().clear();
            }
        }
    ));
    save_button.connect_clicked(clone!(
        @strong save_loc,
        @strong current_spec,
        @weak ctx => move |b| {
            save_view(&ctx, &save_loc, &current_spec, b, false)
        }
    ));
    let go_act = gio::SimpleAction::new("go", None);
    ctx.borrow().window.add_action(&go_act);
    go_act.connect_activate(clone!(@weak ctx => move |_, _| {
        let (saved, window) = {
            let ctx = ctx.borrow();
            let saved = ctx.view_saved.get();
            let window = ctx.window.clone();
            (saved, window)
        };
        if saved || ask_modal(&window, "Unsaved view will be lost.") {
            if let Some(loc) = choose_location(&window, false) {
                ctx.borrow().backend.navigate(loc);
            }
        }
    }));
    let save_as_act = gio::SimpleAction::new("save_as", None);
    ctx.borrow().window.add_action(&save_as_act);
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
    ctx.borrow().window.add_action(&raw_view_act);
    raw_view_act.connect_activate(clone!(
        @weak ctx, @strong current_loc  => move |a, _| {
            if let Some(v) = a.state() {
                let new_v = !v.get::<bool>().expect("invalid state");
                let m = "Unsaved view will be lost.";
                let (saved, window) = {
                    let ctx = ctx.borrow();
                    let saved = ctx.view_saved.get();
                    let window = ctx.window.clone();
                    (saved, window)
                };
                if !new_v || saved || ask_modal(&window, m) {
                    ctx.borrow().raw_view.store(new_v, Ordering::Relaxed);
                    a.change_state(&new_v.to_variant());
                    ctx.borrow().backend.navigate(current_loc.borrow().clone());
                }
            }
        }
    ));
    let new_window_act = gio::SimpleAction::new("new_window", None);
    ctx.borrow().window.add_action(&new_window_act);
    new_window_act.connect_activate(clone!(@weak app => move |_, _| app.activate()));
    to_gui.attach(None, move |m| match m {
        ToGui::Update(updates) => {
            if let Some(root) = &mut *current.borrow_mut() {
                for (id, value) in &updates {
                    root.update(*id, value);
                }
            }
            ControlFlow::Continue
        }
        ToGui::TableResolved(path, table) => {
            if let Some(root) = &mut *current.borrow_mut() {
                root.table_resolved(&path, &table);
            }
            ControlFlow::Continue
        }
        ToGui::Navigate(loc) => {
            let (saved, window) = {
                let ctx = ctx.borrow();
                let saved = ctx.view_saved.get();
                let window = ctx.window.clone();
                (saved, window)
            };
            if saved || ask_modal(&window, "Unsaved view will be lost") {
                ctx.borrow().backend.navigate(loc)
            }
            ControlFlow::Continue
        }
        ToGui::NavigateInWindow(loc) => {
            *ctx.borrow().new_window_loc.borrow_mut() = loc;
            app.activate();
            ControlFlow::Continue
        }
        ToGui::View { loc, spec, generated } => {
            match loc {
                None => {
                    ctx.borrow().view_saved.set(false);
                    save_button.set_sensitive(true);
                }
                Some(loc) => {
                    ctx.borrow().view_saved.set(true);
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
                ctx.borrow().window.remove(cur.root());
            }
            ctx.borrow_mut().radio_groups.clear();
            *current_spec.borrow_mut() = spec.clone();
            let cur = View::new(&ctx, &*current_loc.borrow(), spec);
            let window = ctx.borrow().window.clone();
            window.set_title(&format!("Netidx Browser {}", &*current_loc.borrow()));
            window.add(cur.root());
            window.show_all();
            let hl = highlight.borrow();
            cur.widget.set_highlight(hl.iter(), true);
            *current.borrow_mut() = Some(cur);
            ControlFlow::Continue
        }
        ToGui::Highlight(path) => {
            if let Some(cur) = &*current.borrow() {
                let mut hl = highlight.borrow_mut();
                cur.widget.set_highlight(hl.iter(), false);
                *hl = path;
                cur.widget.set_highlight(hl.iter(), true);
            }
            ControlFlow::Continue
        }
        ToGui::ShowError(s) => {
            err_modal(&ctx.borrow().window, &s);
            ControlFlow::Continue
        }
        ToGui::SaveError(s) => {
            err_modal(&ctx.borrow().window, &s);
            idle_add_local(clone!(
                @weak ctx,
                @strong save_loc,
                @strong current_spec,
                @strong save_button => @default-return ControlFlow::Break, move || {
                    save_view(
                        &ctx,
                        &save_loc,
                        &current_spec,
                        &save_button,
                        true,
                    );
                    ControlFlow::Break
                }
            ));
            ControlFlow::Continue
        }
        ToGui::Terminate => ControlFlow::Continue,
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
        Some("[tls, krb5, local, or anonymous]"),
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

fn parse_auth(cfg: &Config, opts: &glib::VariantDict) -> DesiredAuth {
    match opts.lookup_value("auth", Some(&glib::VariantTy::STRING)) {
        None => cfg.default_auth(),
        Some(auth) => match auth
            .get::<String>()
            .unwrap()
            .parse::<DesiredAuth>()
            .expect("invalid auth mechanism")
        {
            auth @ (DesiredAuth::Local
            | DesiredAuth::Anonymous
            | DesiredAuth::Tls { .. }) => auth,
            DesiredAuth::Krb5 { .. } => {
                match opts.lookup_value("upn", Some(&glib::VariantTy::STRING)) {
                    None => DesiredAuth::Krb5 { upn: None, spn: None },
                    Some(upn) => {
                        DesiredAuth::Krb5 { upn: upn.get::<String>(), spn: None }
                    }
                }
            }
        },
    }
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
        let auth = parse_auth(&cfg, opts);
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
                #[allow(deprecated)]
                let (tx_to_gui, rx_to_gui) = glib::MainContext::channel(Priority::LOW);
                let raw_view = Arc::new(AtomicBool::new(false));
                let backend_ctx =
                    backend.create_ctx(tx_to_gui, raw_view.clone()).unwrap();
                backend_ctx.navigate(mem::replace(
                    &mut *new_window_loc.borrow_mut(),
                    default_loc.clone(),
                ));
                let window = ApplicationWindow::new(&app);
                let ctx = Rc::new(RefCell::new(BrowserCtx {
                    backend: backend_ctx,
                    raw_view,
                    window: window.clone(),
                    new_window_loc: new_window_loc.clone(),
                    current_loc: Rc::new(RefCell::new(default_loc.clone())),
                    view_saved: Cell::new(true),
                    radio_groups: HashMap::default(),
                }));
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
