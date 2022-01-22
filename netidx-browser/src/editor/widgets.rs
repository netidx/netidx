use super::super::{util::err_modal, BSCtx};
use super::{
    expr_inspector::ExprInspector,
    util::{self, parse_entry, TwoColGrid},
    OnChange,
};
use glib::{clone, prelude::*};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use netidx::subscriber::Value;
use netidx_bscript::expr;
use netidx_protocols::view;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

type DbgExpr = Rc<RefCell<Option<(gtk::Window, ExprInspector)>>>;

fn expr(
    ctx: &BSCtx,
    txt: &str,
    init: &expr::Expr,
    on_change: impl Fn(expr::Expr) + 'static,
) -> (gtk::Label, gtk::Box, DbgExpr) {
    let on_change = Rc::new(on_change);
    let source = Rc::new(RefCell::new(init.clone()));
    let inspector: Rc<RefCell<Option<(gtk::Window, ExprInspector)>>> =
        Rc::new(RefCell::new(None));
    let lbl = gtk::Label::new(Some(txt));
    let ibox = gtk::Box::new(gtk::Orientation::Horizontal, 0);
    let entry = gtk::Entry::new();
    let inspect = gtk::ToggleButton::new();
    let inspect_icon = gtk::Image::from_icon_name(
        Some("preferences-system"),
        gtk::IconSize::SmallToolbar,
    );
    inspect.set_image(Some(&inspect_icon));
    ibox.pack_start(&entry, true, true, 0);
    ibox.pack_end(&inspect, false, false, 0);
    entry.set_text(&source.borrow().to_string());
    entry.set_icon_activatable(gtk::EntryIconPosition::Secondary, true);
    entry.connect_changed(move |e| {
        e.set_icon_from_icon_name(
            gtk::EntryIconPosition::Secondary,
            Some("media-floppy"),
        );
    });
    entry.connect_icon_press(move |e, _, _| e.emit_activate());
    entry.connect_activate(clone!(
        @strong on_change, @strong source, @weak ibox => move |e| {
        match e.text().parse::<expr::Expr>() {
            Err(e) => err_modal(&ibox, &format!("parse error: {}", e)),
            Ok(s) => {
                e.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, None);
                *source.borrow_mut() = s.clone();
                on_change(s);
            }
        }
    }));
    inspect.connect_toggled(clone!(
        @strong ctx,
        @strong inspector,
        @strong source,
        @strong on_change,
        @weak entry => move |b| {
        if !b.is_active() {
            if let Some((w, _)) = inspector.borrow_mut().take() {
                w.close()
            }
        } else {
            let w = gtk::Window::new(gtk::WindowType::Toplevel);
            w.set_default_size(640, 480);
            let on_change = clone!(
                @strong source, @strong entry, @strong on_change => move |s: expr::Expr| {
                    entry.set_text(&s.to_string());
                    entry.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, None);
                    *source.borrow_mut() = s.clone();
                    on_change(s);
                });
            let si = ExprInspector::new(
                ctx.clone(),
                &w,
                on_change,
                source.borrow().clone()
            );
            w.add(si.root());
            si.root().set_margin(5);
            w.connect_delete_event(clone!(@strong inspector, @strong b => move |_, _| {
                *inspector.borrow_mut() = None;
                b.set_active(false);
                Inhibit(false)
            }));
            w.show_all();
            *inspector.borrow_mut() = Some((w, si));
        }
    }));
    (lbl, ibox, inspector)
}

#[derive(Clone)]
pub(super) struct Table {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Table>>,
    _dbg_path: DbgExpr,
    _dbg_default_sort_column: DbgExpr,
    _dbg_column_filter: DbgExpr,
    _dbg_row_filter: DbgExpr,
    _dbg_column_editable: DbgExpr,
    _dbg_on_activate: DbgExpr,
    _dbg_on_select: DbgExpr,
    _dbg_on_edit: DbgExpr,
}

impl Table {
    pub(super) fn new(ctx: &BSCtx, on_change: OnChange, spec: view::Table) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let mut root = TwoColGrid::new();
        let (l, e, _dbg_path) = expr(
            ctx,
            "Path:",
            &spec.borrow().path,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().path = e;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _dbg_default_sort_column) = expr(
            ctx,
            "Default Sort Column:",
            &spec.borrow().default_sort_column,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().default_sort_column = e;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, _dbg_column_filter) = expr(
            ctx,
            "Column Filter:",
            &spec.borrow().column_filter,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().column_filter = e;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _dbg_row_filter) = expr(
            ctx,
            "Row Filter:",
            &spec.borrow().row_filter,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().row_filter = e;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _dbg_column_editable) = expr(
            ctx,
            "Column Editable:",
            &spec.borrow().column_editable,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().column_editable = e;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _dbg_on_activate) = expr(
            ctx,
            "On Activate:",
            &spec.borrow().on_activate,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().on_activate = e;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _dbg_on_select) = expr(
            ctx,
            "On Select:",
            &spec.borrow().on_select,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().on_select = e;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _dbg_on_edit) = expr(
            ctx,
            "On Edit:",
            &spec.borrow().on_edit,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().on_edit = e;
                on_change()
            }),
        );
        root.add((l, e));
        Table {
            root,
            spec,
            _dbg_path,
            _dbg_default_sort_column,
            _dbg_column_filter,
            _dbg_row_filter,
            _dbg_column_editable,
            _dbg_on_activate,
            _dbg_on_select,
            _dbg_on_edit,
        }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Table(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Action {
    root: TwoColGrid,
    spec: Rc<RefCell<expr::Expr>>,
    _expr: DbgExpr,
    iter: Rc<RefCell<gtk::TreeIter>>,
}

impl Action {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: expr::Expr,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let iter = Rc::new(RefCell::new(iter.clone()));
        let update_desc = Rc::new({
            let store = store.clone();
            let iter = iter.clone();
            let spec = spec.clone();
            move || {
                let spec = spec.borrow();
                let desc = format!("{}", &spec);
                store.set_value(&*iter.borrow(), 2, &desc.to_value());
            }
        });
        update_desc();
        let (l, e, _expr) = expr(
            ctx,
            "Action:",
            &*spec.borrow(),
            clone!(@strong update_desc, @strong spec, @strong on_change => move |s| {
                *spec.borrow_mut() = s;
                update_desc();
                on_change()
            }),
        );
        root.add((l, e));
        Action { root, spec, _expr, iter }
    }

    pub(super) fn moved(&self, iter: &gtk::TreeIter) {
        *self.iter.borrow_mut() = iter.clone();
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Action(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Label {
    root: gtk::Box,
    spec: Rc<RefCell<expr::Expr>>,
    _expr: DbgExpr,
}

impl Label {
    pub(super) fn new(ctx: &BSCtx, on_change: OnChange, spec: expr::Expr) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let pathbox = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let spec = Rc::new(RefCell::new(spec));
        root.pack_start(&pathbox, false, false, 0);
        let (l, e, _expr) = expr(
            ctx,
            "Expr:",
            &*spec.borrow(),
            clone!(@strong spec => move |s| {
                *spec.borrow_mut() = s;
                on_change()
            }),
        );
        pathbox.pack_start(&l, false, false, 0);
        pathbox.pack_start(&e, true, true, 0);
        Label { root, spec, _expr }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Label(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Button {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Button>>,
    _enabled_expr: DbgExpr,
    _label_expr: DbgExpr,
    _on_click_expr: DbgExpr,
}

impl Button {
    pub(super) fn new(ctx: &BSCtx, on_change: OnChange, spec: view::Button) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _enabled_expr) = expr(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, _label_expr) = expr(
            ctx,
            "Label:",
            &spec.borrow().label,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().label = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _on_click_expr) = expr(
            ctx,
            "On Click:",
            &spec.borrow().on_click,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().on_click = s;
                on_change()
            }),
        );
        root.add((l, e));
        Button { root, spec, _enabled_expr, _label_expr, _on_click_expr }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Button(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct LinkButton {
    root: TwoColGrid,
    spec: Rc<RefCell<view::LinkButton>>,
    _enabled_expr: DbgExpr,
    _uri_expr: DbgExpr,
    _label_expr: DbgExpr,
    _on_activate_link_expr: DbgExpr,
}

impl LinkButton {
    pub(super) fn new(ctx: &BSCtx, on_change: OnChange, spec: view::LinkButton) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _enabled_expr) = expr(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, _label_expr) = expr(
            ctx,
            "Label:",
            &spec.borrow().label,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().label = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _uri_expr) = expr(
            ctx,
            "URI:",
            &spec.borrow().uri,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().uri = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _on_activate_link_expr) = expr(
            ctx,
            "On Activate Link:",
            &spec.borrow().on_activate_link,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().on_activate_link = s;
                on_change()
            }),
        );
        root.add((l, e));
        LinkButton {
            root,
            spec,
            _enabled_expr,
            _label_expr,
            _uri_expr,
            _on_activate_link_expr,
        }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::LinkButton(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Toggle {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Toggle>>,
    _enabled_expr: DbgExpr,
    _value_expr: DbgExpr,
    _on_change_expr: DbgExpr,
}

impl Toggle {
    pub(super) fn new(ctx: &BSCtx, on_change: OnChange, spec: view::Toggle) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _enabled_expr) = expr(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, _value_expr) = expr(
            ctx,
            "Value:",
            &spec.borrow().value,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().value = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, _on_change_expr) = expr(
            ctx,
            "On Change:",
            &spec.borrow().on_change,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().on_change = s;
                on_change();
            }),
        );
        root.add((l, e));
        Toggle { root, spec, _enabled_expr, _value_expr, _on_change_expr }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Toggle(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Selector {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Selector>>,
    _enabled_expr: DbgExpr,
    _choices_expr: DbgExpr,
    _selected_expr: DbgExpr,
    _on_change_expr: DbgExpr,
}

impl Selector {
    pub(super) fn new(ctx: &BSCtx, on_change: OnChange, spec: view::Selector) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _enabled_expr) = expr(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, _choices_expr) = expr(
            ctx,
            "Choices:",
            &spec.borrow().choices,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().choices = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, _selected_expr) = expr(
            ctx,
            "Selected:",
            &spec.borrow().selected,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().selected = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, _on_change_expr) = expr(
            ctx,
            "On Change:",
            &spec.borrow().on_change,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().on_change = s;
                on_change();
            }),
        );
        root.add((l, e));
        Selector {
            root,
            spec,
            _enabled_expr,
            _choices_expr,
            _selected_expr,
            _on_change_expr,
        }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Selector(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Entry {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Entry>>,
    _enabled_expr: DbgExpr,
    _visible_expr: DbgExpr,
    _text_expr: DbgExpr,
    _on_change_expr: DbgExpr,
    _on_activate_expr: DbgExpr,
}

impl Entry {
    pub(super) fn new(ctx: &BSCtx, on_change: OnChange, spec: view::Entry) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _enabled_expr) = expr(
            ctx,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _visible_expr) = expr(
            ctx,
            "Visible:",
            &spec.borrow().visible,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().visible = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _text_expr) = expr(
            ctx,
            "Text:",
            &spec.borrow().text,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().text = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _on_change_expr) = expr(
            ctx,
            "On Change:",
            &spec.borrow().on_change,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().on_change = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _on_activate_expr) = expr(
            ctx,
            "On Activate:",
            &spec.borrow().on_activate,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().on_activate = s;
                on_change()
            }),
        );
        root.add((l, e));
        Entry {
            root,
            spec,
            _enabled_expr,
            _visible_expr,
            _text_expr,
            _on_change_expr,
            _on_activate_expr,
        }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Entry(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
struct Series {
    _x: DbgExpr,
    _y: DbgExpr,
    spec: Rc<RefCell<view::Series>>,
}

#[derive(Clone)]
pub(super) struct LinePlot {
    root: gtk::Box,
    spec: Rc<RefCell<view::LinePlot>>,
    _x_min: DbgExpr,
    _x_max: DbgExpr,
    _y_min: DbgExpr,
    _y_max: DbgExpr,
    _keep_points: DbgExpr,
    _series: Rc<RefCell<IndexMap<usize, Series>>>,
}

impl LinePlot {
    pub(super) fn new(ctx: &BSCtx, on_change: OnChange, spec: view::LinePlot) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        LinePlot::build_chart_style_editor(&root, &on_change, &spec);
        LinePlot::build_axis_style_editor(&root, &on_change, &spec);
        let (_x_min, _x_max, _y_min, _y_max, _keep_points) =
            LinePlot::build_axis_range_editor(ctx, &root, &on_change, &spec);
        let _series = LinePlot::build_series_editor(ctx, &root, &on_change, &spec);
        LinePlot { root, spec, _x_min, _x_max, _y_min, _y_max, _keep_points, _series }
    }

    fn build_axis_style_editor(
        root: &gtk::Box,
        on_change: &OnChange,
        spec: &Rc<RefCell<view::LinePlot>>,
    ) {
        let axis_exp = gtk::Expander::new(Some("Axis Style"));
        util::expander_touch_enable(&axis_exp);
        let mut axis = TwoColGrid::new();
        root.pack_start(&axis_exp, false, false, 0);
        root.pack_start(&gtk::Separator::new(gtk::Orientation::Horizontal), false, false, 0);
        axis_exp.add(axis.root());
        axis.add(parse_entry(
            "X Axis Label:",
            &spec.borrow().x_label,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_label = s;
                on_change()
            }),
        ));
        axis.add(parse_entry(
            "Y Axis Label:",
            &spec.borrow().y_label,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_label = s;
                on_change()
            }),
        ));
        axis.add(parse_entry(
            "X Labels:",
            &spec.borrow().x_labels,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_labels = s;
                on_change()
            }),
        ));
        axis.add(parse_entry(
            "Y Labels:",
            &spec.borrow().y_labels,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_labels = s;
                on_change()
            }),
        ));
        let x_grid = gtk::CheckButton::with_label("X Axis Grid");
        x_grid.set_active(spec.borrow().x_grid);
        x_grid.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().x_grid = b.is_active();
            on_change()
        }));
        axis.attach(&x_grid, 0, 2, 1);
        let y_grid = gtk::CheckButton::with_label("Y Axis Grid");
        y_grid.set_active(spec.borrow().y_grid);
        y_grid.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().y_grid = b.is_active();
            on_change()
        }));
        axis.attach(&y_grid, 0, 2, 1);
    }

    fn build_axis_range_editor(
        ctx: &BSCtx,
        root: &gtk::Box,
        on_change: &OnChange,
        spec: &Rc<RefCell<view::LinePlot>>,
    ) -> (DbgExpr, DbgExpr, DbgExpr, DbgExpr, DbgExpr) {
        let range_exp = gtk::Expander::new(Some("Axis Range"));
        util::expander_touch_enable(&range_exp);
        let mut range = TwoColGrid::new();
        root.pack_start(&range_exp, false, false, 0);
        root.pack_start(&gtk::Separator::new(gtk::Orientation::Horizontal), false, false, 0);
        range_exp.add(range.root());
        let (l, e, x_min) = expr(
            ctx,
            "x min:",
            &spec.borrow().x_min,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_min = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, x_max) = expr(
            ctx,
            "x max:",
            &spec.borrow().x_max,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_max = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, y_min) = expr(
            ctx,
            "y min:",
            &spec.borrow().y_min,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_min = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, y_max) = expr(
            ctx,
            "y max:",
            &spec.borrow().y_max,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_max = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, keep_points) = expr(
            ctx,
            "Keep Points:",
            &spec.borrow().keep_points,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().keep_points = s;
                on_change()
            }),
        );
        range.add((l, e));
        (x_min, x_max, y_min, y_max, keep_points)
    }

    fn build_chart_style_editor(
        root: &gtk::Box,
        on_change: &OnChange,
        spec: &Rc<RefCell<view::LinePlot>>,
    ) {
        let style_exp = gtk::Expander::new(Some("Chart Style"));
        util::expander_touch_enable(&style_exp);
        let mut style = TwoColGrid::new();
        root.pack_start(&style_exp, false, false, 0);
        root.pack_start(&gtk::Separator::new(gtk::Orientation::Horizontal), false, false, 0);
        style_exp.add(style.root());
        style.add(parse_entry(
            "Title:",
            &spec.borrow().title,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().title = s;
                on_change()
            }),
        ));
        let has_fill = gtk::CheckButton::with_label("Fill");
        let fill_reveal = gtk::Revealer::new();
        let fill_color = gtk::ColorButton::new();
        fill_reveal.add(&fill_color);
        style.add((has_fill.clone(), fill_reveal.clone()));
        if let Some(c) = spec.borrow().fill {
            has_fill.set_active(true);
            fill_reveal.set_reveal_child(true);
            fill_color.set_rgba(&gdk::RGBA::new(c.r, c.g, c.b, 1.));
        }
        has_fill.connect_toggled(clone!(
            @strong on_change,
            @strong spec,
            @weak fill_reveal,
            @weak fill_color => move |b| {
                if b.is_active() {
                    fill_reveal.set_reveal_child(true);
                    let c = fill_color.rgba();
                    let c = view::RGB { r: c.red(), g: c.green(), b: c.blue() };
                    spec.borrow_mut().fill = Some(c);
                } else {
                    fill_reveal.set_reveal_child(false);
                    spec.borrow_mut().fill = None;
                }
                on_change()
        }));
        fill_color.connect_color_set(
            clone!(@strong on_change, @strong spec => move |b| {
                let c = b.rgba();
                let c = view::RGB { r: c.red(), g: c.green(), b: c.blue() };
                spec.borrow_mut().fill = Some(c);
                on_change()
            }),
        );
        style.add(parse_entry(
            "Margin:",
            &spec.borrow().margin,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().margin = s;
                on_change()
            }),
        ));
        style.add(parse_entry(
            "Label Area:",
            &spec.borrow().label_area,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().label_area = s;
                on_change()
            }),
        ))
    }

    fn build_series_editor(
        ctx: &BSCtx,
        root: &gtk::Box,
        on_change: &OnChange,
        spec: &Rc<RefCell<view::LinePlot>>,
    ) -> Rc<RefCell<IndexMap<usize, Series>>> {
        let series_exp = gtk::Expander::new(Some("Series"));
        util::expander_touch_enable(&series_exp);
        let seriesbox = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let addbtn = gtk::Button::with_label("+");
        series_exp.add(&seriesbox);
        root.pack_start(&series_exp, false, false, 0);
        root.pack_start(&gtk::Separator::new(gtk::Orientation::Horizontal), false, false, 0);
        let series_id = Rc::new(Cell::new(0));
        let series: Rc<RefCell<IndexMap<usize, Series>>> =
            Rc::new(RefCell::new(IndexMap::new()));
        let on_change = Rc::new(clone!(
        @strong series, @strong on_change, @strong spec => move || {
            let mut spec = spec.borrow_mut();
            spec.series.clear();
            spec.series.extend(series.borrow().values().map(|s| s.spec.borrow().clone()));
            on_change()
        }));
        seriesbox.pack_start(&addbtn, false, false, 0);
        let build_series = Rc::new(clone!(
            @weak seriesbox,
            @strong ctx,
            @strong on_change,
            @strong series => move |spec: view::Series| {
                let spec = Rc::new(RefCell::new(spec));
                let mut grid = TwoColGrid::new();
                seriesbox.pack_start(grid.root(), false, false, 0);
                let sep = gtk::Separator::new(gtk::Orientation::Vertical);
                grid.attach(&sep, 0, 2, 1);
                grid.add(parse_entry(
                    "Title:",
                    &spec.borrow().title,
                    clone!(@strong spec, @strong on_change => move |s| {
                        spec.borrow_mut().title = s;
                        on_change()
                    })
                ));
                let c = spec.borrow().line_color;
                let rgba = gdk::RGBA::new(c.r, c.g, c.b, 1.);
                let line_color = gtk::ColorButton::with_rgba(&rgba);
                let lbl_line_color = gtk::Label::new(Some("Line Color:"));
                line_color.connect_color_set(clone!(
                    @strong on_change, @strong spec => move |b| {
                        let c = b.rgba();
                        let c = view::RGB { r: c.red(), g: c.green(), b: c.blue() };
                        spec.borrow_mut().line_color = c;
                        on_change()
                    }));
                grid.add((lbl_line_color, line_color));
                let (l, e, _x) = expr(
                    &ctx,
                    "X:",
                    &spec.borrow().x,
                    clone!(@strong spec, @strong on_change => move |s| {
                        spec.borrow_mut().x = s;
                        on_change()
                    })
                );
                grid.add((l, e));
                let (l, e, _y) = expr(
                    &ctx,
                    "Y:",
                    &spec.borrow().y,
                    clone!(@strong spec, @strong on_change => move |s| {
                        spec.borrow_mut().y = s;
                        on_change()
                    })
                );
                grid.add((l, e));
                let remove = gtk::Button::with_label("-");
                grid.attach(&remove, 0, 2, 1);
                let i = series_id.get();
                series_id.set(i + 1);
                series.borrow_mut().insert(i, Series { _x, _y, spec });
                seriesbox.show_all();
                let grid_root = grid.root();
                remove.connect_clicked(clone!(
                    @strong series,
                    @weak grid_root,
                    @weak seriesbox,
                    @strong on_change => move |_| {
                        grid_root.hide();
                        for c in seriesbox.children() {
                            if c == grid_root {
                                seriesbox.remove(&c);
                            }
                        }
                        series.borrow_mut().remove(&i);
                        on_change()
                    }));
        }));
        addbtn.connect_clicked(clone!(@strong build_series => move |_| {
            build_series(view::Series {
                title: String::from("Series"),
                line_color: view::RGB { r: 0., g: 0., b: 0. },
                x: expr::ExprKind::Apply {
                    args: vec![
                        expr::ExprKind::Constant(Value::from("/somewhere/in/netidx/x"))
                            .to_expr()
                    ],
                    function: "load".into()
                }.to_expr(),
                y: expr::ExprKind::Apply {
                    args: vec![
                        expr::ExprKind::Constant(Value::from("/somewhere/in/netidx/y"))
                            .to_expr()
                    ],
                    function: "load".into()
                }.to_expr(),
            })
        }));
        for s in spec.borrow().series.iter() {
            build_series(s.clone())
        }
        series
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::LinePlot(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct BoxChild {
    root: TwoColGrid,
    spec: Rc<RefCell<view::BoxChild>>,
}

impl BoxChild {
    pub(super) fn new(on_change: OnChange, spec: view::BoxChild) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let mut root = TwoColGrid::new();
        let packlbl = gtk::Label::new(Some("Pack:"));
        let packcb = gtk::ComboBoxText::new();
        packcb.append(Some("Start"), "Start");
        packcb.append(Some("End"), "End");
        packcb.set_active_id(Some(match spec.borrow().pack {
            view::Pack::Start => "Start",
            view::Pack::End => "End",
        }));
        packcb.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            spec.borrow_mut().pack = match c.active_id() {
                Some(s) if &*s == "Start" => view::Pack::Start,
                Some(s) if &*s == "End" => view::Pack::End,
                _ => view::Pack::Start
            };
            on_change()
        }));
        root.add((packlbl, packcb));
        root.add(parse_entry(
            "Padding:",
            &spec.borrow().padding,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().padding = s;
                on_change()
            }),
        ));
        BoxChild { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::BoxChild(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

fn dirselect(
    cur: view::Direction,
    on_change: impl Fn(view::Direction) + 'static,
) -> gtk::ComboBoxText {
    let dircb = gtk::ComboBoxText::new();
    dircb.append(Some("Horizontal"), "Horizontal");
    dircb.append(Some("Vertical"), "Vertical");
    match cur {
        view::Direction::Horizontal => dircb.set_active_id(Some("Horizontal")),
        view::Direction::Vertical => dircb.set_active_id(Some("Vertical")),
    };
    dircb.connect_changed(move |c| {
        on_change(match c.active_id() {
            Some(s) if &*s == "Horizontal" => view::Direction::Horizontal,
            Some(s) if &*s == "Vertical" => view::Direction::Vertical,
            _ => view::Direction::Horizontal,
        })
    });
    dircb
}

#[derive(Clone)]
pub(super) struct Paned {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Paned>>,
}

impl Paned {
    pub(super) fn new(on_change: OnChange, spec: view::Paned) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let dircb = dirselect(
            spec.borrow().direction,
            clone!(@strong on_change, @strong spec => move |d| {
                spec.borrow_mut().direction = d;
                on_change()
            }),
        );
        let dirlbl = gtk::Label::new(Some("Direction:"));
        root.add((dirlbl, dircb));
        let wide = gtk::CheckButton::with_label("Wide Handle:");
        root.attach(&wide, 0, 2, 1);
        wide.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().wide_handle = b.is_active();
            on_change()
        }));
        Paned { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Paned(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Frame {
    root: TwoColGrid,
    _label_expr: DbgExpr,
    spec: Rc<RefCell<view::Frame>>,
}

impl Frame {
    pub(super) fn new(ctx: &BSCtx, on_change: OnChange, spec: view::Frame) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _label_expr) = expr(
            ctx,
            "Label:",
            &spec.borrow().label,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().label = e;
                on_change();
            }),
        );
        root.add((l, e));
        root.add(parse_entry(
            "Label Horizontal Align:",
            &spec.borrow().label_align_horizontal,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().label_align_horizontal = s;
                on_change();
            }),
        ));
        root.add(parse_entry(
            "Label Vertical Align:",
            &spec.borrow().label_align_vertical,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().label_align_vertical = s;
                on_change()
            }),
        ));
        Frame { root, _label_expr, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Frame(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct BoxContainer {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Box>>,
}

impl BoxContainer {
    pub(super) fn new(on_change: OnChange, spec: view::Box) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let dircb = dirselect(
            spec.borrow().direction,
            clone!(@strong on_change, @strong spec => move |d| {
                spec.borrow_mut().direction = d;
                on_change()
            }),
        );
        let dirlbl = gtk::Label::new(Some("Direction:"));
        root.add((dirlbl, dircb));
        let homo = gtk::CheckButton::with_label("Homogeneous:");
        root.attach(&homo, 0, 2, 1);
        homo.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().homogeneous = b.is_active();
            on_change()
        }));
        root.add(parse_entry(
            "Spacing:",
            &spec.borrow().spacing,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().spacing = s;
                on_change()
            }),
        ));
        BoxContainer { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Box(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct NotebookPage {
    root: TwoColGrid,
    spec: Rc<RefCell<view::NotebookPage>>,
}

impl NotebookPage {
    pub(super) fn new(on_change: OnChange, spec: view::NotebookPage) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        root.add(parse_entry(
            "Tab Label:",
            &spec.borrow().label,
            clone!(@strong on_change, @strong spec => move |w| {
                spec.borrow_mut().label = w;
                on_change()
            }),
        ));
        let reorderable = gtk::CheckButton::with_label("Reorderable");
        reorderable.set_active(spec.borrow().reorderable);
        reorderable.connect_toggled(clone!(@strong spec, @strong on_change => move |b| {
            spec.borrow_mut().reorderable = b.is_active();
            on_change()
        }));
        root.attach(&reorderable, 0, 2, 1);
        NotebookPage { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::NotebookPage(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Notebook {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Notebook>>,
    _page: DbgExpr,
    _on_switch_page: DbgExpr,
}

impl Notebook {
    pub(super) fn new(ctx: &BSCtx, on_change: OnChange, spec: view::Notebook) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let poscb_lbl = gtk::Label::new(Some("Position:"));
        let poscb = gtk::ComboBoxText::new();
        poscb.append(Some("Top"), "Top");
        poscb.append(Some("Bottom"), "Bottom");
        poscb.append(Some("Left"), "Left");
        poscb.append(Some("Right"), "Right");
        poscb.set_active_id(match spec.borrow().tabs_position {
            view::TabPosition::Top => Some("Top"),
            view::TabPosition::Bottom => Some("Bottom"),
            view::TabPosition::Left => Some("Left"),
            view::TabPosition::Right => Some("Right"),
        });
        poscb.connect_changed(clone!(@strong on_change, @strong spec => move |c| {
            let pos = match c.active_id() {
                Some(s) if &*s == "Top" => view::TabPosition::Top,
                Some(s) if &*s == "Bottom" => view::TabPosition::Bottom,
                Some(s) if &*s == "Left" => view::TabPosition::Left,
                Some(s) if &*s == "Right" => view::TabPosition::Right,
                _ => unreachable!()
            };
            spec.borrow_mut().tabs_position = pos;
            on_change()
        }));
        root.add((poscb_lbl, poscb));
        let tabs_visible = gtk::CheckButton::with_label("Tabs Visible");
        tabs_visible.set_active(spec.borrow().tabs_visible);
        tabs_visible.connect_toggled(
            clone!(@strong on_change, @strong spec => move |b| {
                spec.borrow_mut().tabs_visible = b.is_active();
                on_change();
            }),
        );
        root.attach(&tabs_visible, 0, 2, 1);
        let tabs_scrollable = gtk::CheckButton::with_label("Tabs Scrollable");
        tabs_scrollable.set_active(spec.borrow().tabs_scrollable);
        tabs_scrollable.connect_toggled(
            clone!(@strong on_change, @strong spec => move |b| {
                spec.borrow_mut().tabs_scrollable = b.is_active();
                on_change();
            }),
        );
        root.attach(&tabs_scrollable, 0, 2, 1);
        let tabs_popup = gtk::CheckButton::with_label("Tabs Have Popup Menu");
        tabs_popup.set_active(spec.borrow().tabs_popup);
        tabs_popup.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().tabs_popup = b.is_active();
            on_change()
        }));
        root.attach(&tabs_popup, 0, 2, 1);
        let (l, e, _page) = expr(
            ctx,
            "Page:",
            &spec.borrow().page,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().page = e;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, _on_switch_page) = expr(
            ctx,
            "On Switch Page:",
            &spec.borrow().on_switch_page,
            clone!(@strong spec, @strong on_change => move |e| {
                spec.borrow_mut().on_switch_page = e;
                on_change()
            }),
        );
        root.add((l, e));
        Notebook { root, spec, _page, _on_switch_page }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Notebook(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct GridChild {
    root: TwoColGrid,
    spec: Rc<RefCell<view::GridChild>>,
}

impl GridChild {
    pub(super) fn new(on_change: OnChange, spec: view::GridChild) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        root.add(parse_entry(
            "Width:",
            &spec.borrow().width,
            clone!(@strong on_change, @strong spec => move |w| {
                spec.borrow_mut().width = w;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Height:",
            &spec.borrow().height,
            clone!(@strong on_change, @strong spec => move |h| {
                spec.borrow_mut().height = h;
                on_change()
            }),
        ));
        GridChild { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::GridChild(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Grid {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Grid>>,
}

impl Grid {
    pub(super) fn new(on_change: OnChange, spec: view::Grid) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let homogeneous_columns = gtk::CheckButton::with_label("Homogeneous Columns");
        homogeneous_columns.set_active(spec.borrow().homogeneous_columns);
        homogeneous_columns.connect_toggled(
            clone!(@strong on_change, @strong spec => move |b| {
                spec.borrow_mut().homogeneous_columns = b.is_active();
                on_change()
            }),
        );
        root.attach(&homogeneous_columns, 0, 2, 1);
        let homogeneous_rows = gtk::CheckButton::with_label("Homogeneous Rows");
        homogeneous_rows.set_active(spec.borrow().homogeneous_rows);
        homogeneous_rows.connect_toggled(
            clone!(@strong on_change, @strong spec => move |b| {
                spec.borrow_mut().homogeneous_rows = b.is_active();
                on_change()
            }),
        );
        root.attach(&homogeneous_rows, 0, 2, 1);
        root.add(parse_entry(
            "Column Spacing:",
            &spec.borrow().column_spacing,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().column_spacing = s;
                on_change()
            }),
        ));
        root.add(parse_entry(
            "Row Spacing:",
            &spec.borrow().row_spacing,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().row_spacing = s;
                on_change()
            }),
        ));
        Grid { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Grid(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}
