use super::super::{util::err_modal, BSCtx};
use super::{
    expr_inspector::ExprInspector,
    util::{self, parse_entry, TwoColGrid},
    OnChange, Scope,
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

pub(super) type DbgExpr = Rc<RefCell<Option<(gtk::Window, ExprInspector)>>>;

pub(super) fn expr(
    ctx: &BSCtx,
    txt: &str,
    scope: Scope,
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
                scope.clone(),
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

macro_rules! expr {
    ($ctx:ident, $name:expr, $scope:ident, $spec:ident, $on_change:ident, $field:ident) => {
        expr($ctx, $name, $scope.clone(), &$spec.borrow().$field, {
            let spec = $spec.clone();
            let on_change = $on_change.clone();
            move |e| {
                spec.borrow_mut().$field = e;
                on_change()
            }
        })
    };
}

#[derive(Clone)]
pub(super) struct Table {
    root: gtk::Box,
    spec: Rc<RefCell<view::Table>>,
    _dbg_path: DbgExpr,
    _dbg_sort_mode: DbgExpr,
    _dbg_column_filter: DbgExpr,
    _dbg_row_filter: DbgExpr,
    _dbg_column_editable: DbgExpr,
    _dbg_column_widths: DbgExpr,
    _dbg_columns_resizable: DbgExpr,
    _dbg_column_types: DbgExpr,
    _dbg_selection_mode: DbgExpr,
    _dbg_selection: DbgExpr,
    _dbg_show_row_name: DbgExpr,
    _dbg_refresh: DbgExpr,
    _dbg_on_activate: DbgExpr,
    _dbg_on_select: DbgExpr,
    _dbg_on_edit: DbgExpr,
    _dbg_on_header_click: DbgExpr,
}

impl Table {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::Table,
    ) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let col_config_exp = gtk::Expander::new(Some("Column Config"));
        let row_config_exp = gtk::Expander::new(Some("Row Config"));
        let event_exp = gtk::Expander::new(Some("Events"));
        let mut shared_config = TwoColGrid::new();
        let mut col_config = TwoColGrid::new();
        let mut row_config = TwoColGrid::new();
        let mut event = TwoColGrid::new();
        util::expander_touch_enable(&col_config_exp);
        util::expander_touch_enable(&row_config_exp);
        util::expander_touch_enable(&event_exp);
        root.pack_start(shared_config.root(), false, false, 0);
        root.pack_start(&col_config_exp, false, false, 0);
        root.pack_start(&row_config_exp, false, false, 0);
        root.pack_start(&event_exp, false, false, 0);
        col_config_exp.add(col_config.root());
        row_config_exp.add(row_config.root());
        event_exp.add(event.root());
        let (l, e, _dbg_path) = expr!(ctx, "Path:", scope, spec, on_change, path);
        shared_config.add((l, e));
        let (l, e, _dbg_refresh) =
            expr!(ctx, "Refresh:", scope, spec, on_change, refresh);
        shared_config.add((l, e));
        let (l, e, _dbg_sort_mode) =
            expr!(ctx, "Sort Mode:", scope, spec, on_change, sort_mode);
        shared_config.add((l, e));
        let (l, e, _dbg_column_filter) =
            expr!(ctx, "Column Filter:", scope, spec, on_change, column_filter);
        col_config.add((l, e));
        let (l, e, _dbg_row_filter) =
            expr!(ctx, "Row Filter:", scope, spec, on_change, row_filter);
        row_config.add((l, e));
        let (l, e, _dbg_column_editable) =
            expr!(ctx, "Column Editable:", scope, spec, on_change, column_editable);
        col_config.add((l, e));
        let (l, e, _dbg_column_widths) =
            expr!(ctx, "Column Widths:", scope, spec, on_change, column_widths);
        col_config.add((l, e));
        let (l, e, _dbg_columns_resizable) =
            expr!(ctx, "Columns Resizable:", scope, spec, on_change, columns_resizable);
        col_config.add((l, e));
        let (l, e, _dbg_column_types) =
            expr!(ctx, "Column Types:", scope, spec, on_change, column_types);
        col_config.add((l, e));
        let (l, e, _dbg_selection_mode) =
            expr!(ctx, "Selection Mode:", scope, spec, on_change, selection_mode);
        shared_config.add((l, e));
        let (l, e, _dbg_selection) =
            expr!(ctx, "Selection:", scope, spec, on_change, selection);
        shared_config.add((l, e));
        let (l, e, _dbg_show_row_name) =
            expr!(ctx, "Show Row Name:", scope, spec, on_change, show_row_name);
        row_config.add((l, e));
        let (l, e, _dbg_on_activate) =
            expr!(ctx, "On Activate:", scope, spec, on_change, on_activate);
        event.add((l, e));
        let (l, e, _dbg_on_select) =
            expr!(ctx, "On Select:", scope, spec, on_change, on_select);
        event.add((l, e));
        let (l, e, _dbg_on_edit) =
            expr!(ctx, "On Edit:", scope, spec, on_change, on_edit);
        event.add((l, e));
        let (l, e, _dbg_on_header_click) =
            expr!(ctx, "On Header Click:", scope, spec, on_change, on_header_click);
        event.add((l, e));
        Table {
            root,
            spec,
            _dbg_path,
            _dbg_refresh,
            _dbg_sort_mode,
            _dbg_column_filter,
            _dbg_row_filter,
            _dbg_column_editable,
            _dbg_column_widths,
            _dbg_columns_resizable,
            _dbg_column_types,
            _dbg_selection_mode,
            _dbg_selection,
            _dbg_show_row_name,
            _dbg_on_activate,
            _dbg_on_select,
            _dbg_on_edit,
            _dbg_on_header_click,
        }
    }

    pub(super) fn spec(&self) -> view::Table {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct BScript {
    root: TwoColGrid,
    spec: Rc<RefCell<expr::Expr>>,
    _expr: DbgExpr,
    iter: Rc<RefCell<gtk::TreeIter>>,
}

impl BScript {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        scope: Scope,
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
            scope.clone(),
            &*spec.borrow(),
            clone!(@strong update_desc, @strong spec, @strong on_change => move |s| {
                *spec.borrow_mut() = s;
                update_desc();
                on_change()
            }),
        );
        root.add((l, e));
        Self { root, spec, _expr, iter }
    }

    pub(super) fn moved(&self, iter: &gtk::TreeIter) {
        *self.iter.borrow_mut() = iter.clone();
    }

    pub(super) fn spec(&self) -> expr::Expr {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Image {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Image>>,
    _dbg_expr: DbgExpr,
    _dbg_on_click: DbgExpr,
}

impl Image {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::Image,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _dbg_expr) = expr!(ctx, "Spec:", scope, spec, on_change, spec);
        root.add((l, e));
        let (l, e, _dbg_on_click) =
            expr!(ctx, "On Click:", scope, spec, on_change, on_click);
        root.add((l, e));
        Image { root, spec, _dbg_expr, _dbg_on_click }
    }

    pub(super) fn spec(&self) -> view::Image {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Label {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Label>>,
    _dbg_text: DbgExpr,
    _dbg_width: DbgExpr,
    _dbg_ellipsize: DbgExpr,
    _dbg_single_line: DbgExpr,
    _dbg_selectable: DbgExpr,
}

impl Label {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::Label,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _dbg_text) = expr!(ctx, "Text:", scope, spec, on_change, text);
        root.add((l, e));
        let (l, e, _dbg_width) = expr!(ctx, "Min Width:", scope, spec, on_change, width);
        root.add((l, e));
        let (l, e, _dbg_ellipsize) =
            expr!(ctx, "Ellipsize Mode:", scope, spec, on_change, ellipsize);
        root.add((l, e));
        let (l, e, _dbg_single_line) =
            expr!(ctx, "Single Line:", scope, spec, on_change, single_line);
        root.add((l, e));
        let (l, e, _dbg_selectable) =
            expr!(ctx, "Selectable:", scope, spec, on_change, selectable);
        root.add((l, e));
        Self {
            root,
            spec,
            _dbg_text,
            _dbg_width,
            _dbg_ellipsize,
            _dbg_single_line,
            _dbg_selectable,
        }
    }

    pub(super) fn spec(&self) -> view::Label {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Button {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Button>>,
    _label_expr: DbgExpr,
    _image_expr: DbgExpr,
    _on_click_expr: DbgExpr,
}

impl Button {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::Button,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _label_expr) = expr!(ctx, "Label:", scope, spec, on_change, label);
        root.add((l, e));
        let (l, e, _image_expr) = expr!(ctx, "Image:", scope, spec, on_change, image);
        root.add((l, e));
        let (l, e, _on_click_expr) =
            expr!(ctx, "On Click:", scope, spec, on_change, on_click);
        root.add((l, e));
        Button { root, spec, _label_expr, _image_expr, _on_click_expr }
    }

    pub(super) fn spec(&self) -> view::Button {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct LinkButton {
    root: TwoColGrid,
    spec: Rc<RefCell<view::LinkButton>>,
    _uri_expr: DbgExpr,
    _label_expr: DbgExpr,
    _on_activate_link_expr: DbgExpr,
}

impl LinkButton {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::LinkButton,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _label_expr) = expr!(ctx, "Label:", scope, spec, on_change, label);
        root.add((l, e));
        let (l, e, _uri_expr) = expr!(ctx, "URI:", scope, spec, on_change, uri);
        root.add((l, e));
        let (l, e, _on_activate_link_expr) =
            expr!(ctx, "On Activate Link:", scope, spec, on_change, on_activate_link);
        root.add((l, e));
        LinkButton { root, spec, _label_expr, _uri_expr, _on_activate_link_expr }
    }

    pub(super) fn spec(&self) -> view::LinkButton {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct ToggleButton {
    switch: Switch,
    spec: Rc<RefCell<view::ToggleButton>>,
    _dbg_label: DbgExpr,
    _dbg_image: DbgExpr,
}

impl ToggleButton {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::ToggleButton,
    ) -> Self {
        let mut switch =
            Switch::new(ctx, on_change.clone(), scope.clone(), spec.toggle.clone());
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _dbg_label) = expr!(ctx, "Label:", scope, spec, on_change, label);
        switch.root.add((l, e));
        let (l, e, _dbg_image) = expr!(ctx, "Image:", scope, spec, on_change, image);
        switch.root.add((l, e));
        Self { switch, spec, _dbg_label, _dbg_image }
    }

    pub(super) fn spec(&self) -> view::ToggleButton {
        let mut spec = self.spec.borrow().clone();
        spec.toggle = self.switch.spec();
        spec
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.switch.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Switch {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Switch>>,
    _value_expr: DbgExpr,
    _on_change_expr: DbgExpr,
}

impl Switch {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::Switch,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _value_expr) = expr!(ctx, "Value:", scope, spec, on_change, value);
        root.add((l, e));
        let (l, e, _on_change_expr) =
            expr!(ctx, "On Change:", scope, spec, on_change, on_change);
        root.add((l, e));
        Self { root, spec, _value_expr, _on_change_expr }
    }

    pub(super) fn spec(&self) -> view::Switch {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct ComboBox {
    root: TwoColGrid,
    spec: Rc<RefCell<view::ComboBox>>,
    _choices_expr: DbgExpr,
    _selected_expr: DbgExpr,
    _on_change_expr: DbgExpr,
}

impl ComboBox {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::ComboBox,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _choices_expr) =
            expr!(ctx, "Choices:", scope, spec, on_change, choices);
        root.add((l, e));
        let (l, e, _selected_expr) =
            expr!(ctx, "Selected:", scope, spec, on_change, selected);
        root.add((l, e));
        let (l, e, _on_change_expr) =
            expr!(ctx, "On Change:", scope, spec, on_change, on_change);
        root.add((l, e));
        Self { root, spec, _choices_expr, _selected_expr, _on_change_expr }
    }

    pub(super) fn spec(&self) -> view::ComboBox {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct RadioButton {
    root: TwoColGrid,
    spec: Rc<RefCell<view::RadioButton>>,
    _dbg_label: DbgExpr,
    _dbg_image: DbgExpr,
    _dbg_group: DbgExpr,
    _dbg_value: DbgExpr,
    _dbg_on_toggled: DbgExpr,
}

impl RadioButton {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::RadioButton,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _dbg_label) = expr!(ctx, "Label:", scope, spec, on_change, label);
        root.add((l, e));
        let (l, e, _dbg_image) = expr!(ctx, "Image:", scope, spec, on_change, image);
        root.add((l, e));
        let (l, e, _dbg_group) = expr!(ctx, "Group:", scope, spec, on_change, group);
        root.add((l, e));
        let (l, e, _dbg_value) = expr!(ctx, "Value:", scope, spec, on_change, value);
        root.add((l, e));
        let (l, e, _dbg_on_toggled) =
            expr!(ctx, "On Toggled:", scope, spec, on_change, on_toggled);
        root.add((l, e));
        Self {
            root,
            spec,
            _dbg_label,
            _dbg_image,
            _dbg_group,
            _dbg_value,
            _dbg_on_toggled,
        }
    }

    pub(super) fn spec(&self) -> view::RadioButton {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Entry {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Entry>>,
    _text_expr: DbgExpr,
    _on_change_expr: DbgExpr,
    _on_activate_expr: DbgExpr,
}

impl Entry {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::Entry,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _text_expr) = expr!(ctx, "Text:", scope, spec, on_change, text);
        root.add((l, e));
        let (l, e, _on_change_expr) =
            expr!(ctx, "On Change:", scope, spec, on_change, on_change);
        root.add((l, e));
        let (l, e, _on_activate_expr) =
            expr!(ctx, "On Activate:", scope, spec, on_change, on_activate);
        root.add((l, e));
        Entry { root, spec, _text_expr, _on_change_expr, _on_activate_expr }
    }

    pub(super) fn spec(&self) -> view::Entry {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct SearchEntry {
    root: TwoColGrid,
    spec: Rc<RefCell<view::SearchEntry>>,
    _dbg_text: DbgExpr,
    _dbg_on_search_changed: DbgExpr,
    _dbg_on_activate: DbgExpr,
}

impl SearchEntry {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::SearchEntry,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _dbg_text) = expr!(ctx, "Text:", scope, spec, on_change, text);
        root.add((l, e));
        let (l, e, _dbg_on_search_changed) =
            expr!(ctx, "On Search Changed:", scope, spec, on_change, on_search_changed);
        root.add((l, e));
        let (l, e, _dbg_on_activate) =
            expr!(ctx, "On Activate:", scope, spec, on_change, on_activate);
        root.add((l, e));
        Self { root, spec, _dbg_text, _dbg_on_search_changed, _dbg_on_activate }
    }

    pub(super) fn spec(&self) -> view::SearchEntry {
        self.spec.borrow().clone()
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
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::LinePlot,
    ) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        LinePlot::build_chart_style_editor(&root, &on_change, &spec);
        LinePlot::build_axis_style_editor(&root, &on_change, &spec);
        let (_x_min, _x_max, _y_min, _y_max, _keep_points) =
            LinePlot::build_axis_range_editor(
                ctx,
                &root,
                &on_change,
                scope.clone(),
                &spec,
            );
        let _series = LinePlot::build_series_editor(ctx, &root, &on_change, scope, &spec);
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
        root.pack_start(
            &gtk::Separator::new(gtk::Orientation::Horizontal),
            false,
            false,
            0,
        );
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
        scope: Scope,
        spec: &Rc<RefCell<view::LinePlot>>,
    ) -> (DbgExpr, DbgExpr, DbgExpr, DbgExpr, DbgExpr) {
        let range_exp = gtk::Expander::new(Some("Axis Range"));
        util::expander_touch_enable(&range_exp);
        let mut range = TwoColGrid::new();
        root.pack_start(&range_exp, false, false, 0);
        root.pack_start(
            &gtk::Separator::new(gtk::Orientation::Horizontal),
            false,
            false,
            0,
        );
        range_exp.add(range.root());
        let (l, e, x_min) = expr!(ctx, "x min:", scope, spec, on_change, x_min);
        range.add((l, e));
        let (l, e, x_max) = expr!(ctx, "x max:", scope, spec, on_change, x_max);
        range.add((l, e));
        let (l, e, y_min) = expr!(ctx, "y min:", scope, spec, on_change, y_min);
        range.add((l, e));
        let (l, e, y_max) = expr!(ctx, "y max:", scope, spec, on_change, y_max);
        range.add((l, e));
        let (l, e, keep_points) =
            expr!(ctx, "Keep Points:", scope, spec, on_change, keep_points);
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
        root.pack_start(
            &gtk::Separator::new(gtk::Orientation::Horizontal),
            false,
            false,
            0,
        );
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
        scope: Scope,
        spec: &Rc<RefCell<view::LinePlot>>,
    ) -> Rc<RefCell<IndexMap<usize, Series>>> {
        let series_exp = gtk::Expander::new(Some("Series"));
        util::expander_touch_enable(&series_exp);
        let seriesbox = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let addbtn = gtk::Button::with_label("+");
        series_exp.add(&seriesbox);
        root.pack_start(&series_exp, false, false, 0);
        root.pack_start(
            &gtk::Separator::new(gtk::Orientation::Horizontal),
            false,
            false,
            0,
        );
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
                let _ctx = &ctx;
                let (l, e, _x) = expr!(
                    _ctx,
                    "X:",
                    scope,
                    spec,
                    on_change,
                    x
                );
                grid.add((l, e));
                let (l, e, _y) = expr!(
                    _ctx,
                    "Y:",
                    scope,
                    spec,
                    on_change,
                    y
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
                        series.borrow_mut().swap_remove(&i);
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

    pub(super) fn spec(&self) -> view::LinePlot {
        self.spec.borrow().clone()
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
    pub(super) fn new(on_change: OnChange, _scope: Scope, spec: view::BoxChild) -> Self {
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

    pub(super) fn spec(&self) -> view::BoxChild {
        self.spec.borrow().clone()
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
    pub(super) fn new(on_change: OnChange, _scope: Scope, spec: view::Paned) -> Self {
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
        wide.set_active(spec.borrow().wide_handle);
        root.attach(&wide, 0, 2, 1);
        wide.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().wide_handle = b.is_active();
            on_change()
        }));
        Paned { root, spec }
    }

    pub(super) fn spec(&self) -> view::Paned {
        self.spec.borrow().clone()
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
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::Frame,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _label_expr) = expr!(ctx, "Label:", scope, spec, on_change, label);
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

    pub(super) fn spec(&self) -> view::Frame {
        self.spec.borrow().clone()
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
    pub(super) fn new(on_change: OnChange, _scope: Scope, spec: view::Box) -> Self {
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

    pub(super) fn spec(&self) -> view::Box {
        self.spec.borrow().clone()
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
    pub(super) fn new(
        on_change: OnChange,
        _scope: Scope,
        spec: view::NotebookPage,
    ) -> Self {
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

    pub(super) fn spec(&self) -> view::NotebookPage {
        self.spec.borrow().clone()
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
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::Notebook,
    ) -> Self {
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
        let (l, e, _page) = expr!(ctx, "Page:", scope, spec, on_change, page);
        root.add((l, e));
        let (l, e, _on_switch_page) =
            expr!(ctx, "On Switch Page:", scope, spec, on_change, on_switch_page);
        root.add((l, e));
        Notebook { root, spec, _page, _on_switch_page }
    }

    pub(super) fn spec(&self) -> view::Notebook {
        self.spec.borrow().clone()
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
    pub(super) fn new(on_change: OnChange, _scope: Scope, spec: view::GridChild) -> Self {
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

    pub(super) fn spec(&self) -> view::GridChild {
        self.spec.borrow().clone()
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
    pub(super) fn new(on_change: OnChange, _scope: Scope, spec: view::Grid) -> Self {
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

    pub(super) fn spec(&self) -> view::Grid {
        self.spec.borrow().clone()
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }
}

#[derive(Clone)]
pub(super) struct Scale {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Scale>>,
    _dbg_draw_value: DbgExpr,
    _dbg_marks: DbgExpr,
    _dbg_has_origin: DbgExpr,
    _dbg_value: DbgExpr,
    _dbg_min: DbgExpr,
    _dbg_max: DbgExpr,
    _dbg_on_change: DbgExpr,
}

impl Scale {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::Scale,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let dirlb = gtk::Label::new(Some("Direction:"));
        let dircb = dirselect(
            spec.borrow().direction,
            clone!(@strong on_change, @strong spec => move |d| {
                spec.borrow_mut().direction = d;
                on_change()
            }),
        );
        root.add((dirlb, dircb));
        let (l, e, _dbg_draw_value) =
            expr!(ctx, "Draw Value:", scope, spec, on_change, draw_value);
        root.add((l, e));
        let (l, e, _dbg_marks) = expr!(ctx, "Marks:", scope, spec, on_change, marks);
        root.add((l, e));
        let (l, e, _dbg_has_origin) =
            expr!(ctx, "Has Origin:", scope, spec, on_change, has_origin);
        root.add((l, e));
        let (l, e, _dbg_value) = expr!(ctx, "Value:", scope, spec, on_change, value);
        root.add((l, e));
        let (l, e, _dbg_min) = expr!(ctx, "Min:", scope, spec, on_change, min);
        root.add((l, e));
        let (l, e, _dbg_max) = expr!(ctx, "Max:", scope, spec, on_change, max);
        root.add((l, e));
        let (l, e, _dbg_on_change) =
            expr!(ctx, "On Change:", scope, spec, on_change, on_change);
        root.add((l, e));
        Self {
            root,
            spec,
            _dbg_draw_value,
            _dbg_marks,
            _dbg_min,
            _dbg_max,
            _dbg_has_origin,
            _dbg_value,
            _dbg_on_change,
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn spec(&self) -> view::Scale {
        self.spec.borrow().clone()
    }
}

#[derive(Clone)]
pub(super) struct ProgressBar {
    root: TwoColGrid,
    spec: Rc<RefCell<view::ProgressBar>>,
    _dbg_ellipsize: DbgExpr,
    _dbg_fraction: DbgExpr,
    _dbg_pulse: DbgExpr,
    _dbg_text: DbgExpr,
    _dbg_show_text: DbgExpr,
}

impl ProgressBar {
    pub(super) fn new(
        ctx: &BSCtx,
        on_change: OnChange,
        scope: Scope,
        spec: view::ProgressBar,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, _dbg_ellipsize) =
            expr!(ctx, "Ellipsize:", scope, spec, on_change, ellipsize);
        root.add((l, e));
        let (l, e, _dbg_fraction) =
            expr!(ctx, "Fraction:", scope, spec, on_change, fraction);
        root.add((l, e));
        let (l, e, _dbg_pulse) = expr!(ctx, "Pulse:", scope, spec, on_change, pulse);
        root.add((l, e));
        let (l, e, _dbg_text) = expr!(ctx, "Text:", scope, spec, on_change, text);
        root.add((l, e));
        let (l, e, _dbg_show_text) =
            expr!(ctx, "Show Text:", scope, spec, on_change, show_text);
        root.add((l, e));
        Self {
            root,
            spec,
            _dbg_ellipsize,
            _dbg_fraction,
            _dbg_pulse,
            _dbg_text,
            _dbg_show_text,
        }
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn spec(&self) -> view::ProgressBar {
        self.spec.borrow().clone()
    }
}
