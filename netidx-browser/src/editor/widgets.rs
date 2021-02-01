use super::super::{util::err_modal, Target, WidgetCtx, Vars};
use super::{
    expr_inspector::ExprInspector,
    util::{self, parse_entry, TwoColGrid},
    OnChange,
};
use glib::{clone, prelude::*};
use gtk::{self, prelude::*};
use indexmap::IndexMap;
use netidx::{chars::Chars, subscriber::Value};
use netidx_protocols::view;
use std::{
    boxed::Box,
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::Rc,
};

#[derive(Clone, Debug)]
pub(super) struct Table {
    root: gtk::Box,
    spec: Rc<RefCell<view::Table>>,
}

impl Table {
    pub(super) fn new(on_change: OnChange, path: view::Table) -> Self {
        let spec = Rc::new(RefCell::new(path));
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        let pathbox = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        root.pack_start(&pathbox, false, false, 0);
        let (label, entry) = parse_entry(
            "Path:",
            &spec.borrow().path,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().path = s;
                on_change()
            }),
        );
        pathbox.pack_start(&label, false, false, 0);
        pathbox.pack_start(&entry, true, true, 0);
        let default_sort_frame = gtk::Frame::new(Some("Sort Config"));
        let default_sort_box = gtk::Box::new(gtk::Orientation::Vertical, 5);
        root.pack_start(&default_sort_frame, false, false, 0);
        default_sort_frame.add(&default_sort_box);
        let default_sort_cb = gtk::CheckButton::with_label("Has Default Sort");
        let show_hide_default_sort_gui = Rc::new(clone!(
            @strong spec, @strong on_change, @weak default_sort_box => move |show: bool| {
                if !show {
                    let children = default_sort_box.get_children();
                    if children.len() > 1 {
                        for c in &children[1..] {
                            c.hide();
                            default_sort_box.remove(c);
                        }
                    }
                    spec.borrow_mut().default_sort_column = None;
                    on_change()
                } else {
                    let empty_col = String::new();
                    let spec_ref = spec.borrow();
                    let (current_col, current_dir) = match spec_ref.default_sort_column {
                        None => (&empty_col, view::SortDir::Ascending),
                        Some((ref c, d)) => (c, d)
                    };
                    let col_box = gtk::Box::new(gtk::Orientation::Horizontal, 5);
                    let (lbl, ent) = parse_entry(
                        "Column:",
                        current_col,
                        clone!(@strong spec, @strong on_change => move |c| {
                            let mut spec = spec.borrow_mut();
                            match &mut spec.default_sort_column {
                                Some((ref mut column, _)) => { *column = c; }
                                None => {
                                    spec.default_sort_column =
                                        Some((c, view::SortDir::Ascending));
                                }
                            }
                            on_change()
                        })
                    );
                    col_box.pack_start(&lbl, false, false, 0);
                    col_box.pack_start(&ent, false, false, 0);
                    default_sort_box.pack_start(&col_box, false, false, 0);
                    let cb = gtk::ComboBoxText::new();
                    cb.append(Some("Ascending"), "Ascending");
                    cb.append(Some("Descending"), "Descending");
                    cb.set_active_id(Some(match current_dir {
                        view::SortDir::Ascending => "Ascending",
                        view::SortDir::Descending => "Descending"
                    }));
                    cb.connect_changed(clone!(@strong spec, @strong on_change => move |c| {
                        let dir = match c.get_active_id() {
                            Some(s) if &*s == "Ascending" => view::SortDir::Ascending,
                            Some(s) if &*s == "Descending" => view::SortDir::Descending,
                            _ => view::SortDir::Ascending
                        };
                        let mut spec = spec.borrow_mut();
                        match &mut spec.default_sort_column {
                            Some((_, ref mut d)) => { *d = dir; }
                            None => {
                                spec.default_sort_column = Some(("".into(), dir));
                            }
                        }
                        on_change()
                    }));
                    default_sort_box.pack_start(&cb, false, false, 0);
                    default_sort_box.show_all();
                }
            }
        ));
        default_sort_box.pack_start(&default_sort_cb, false, false, 0);
        default_sort_cb.connect_toggled(
            clone!(@strong show_hide_default_sort_gui => move |b| {
                show_hide_default_sort_gui(b.get_active())
            }),
        );
        if spec.borrow().default_sort_column.is_some() {
            show_hide_default_sort_gui(true);
        }
        let colscb = gtk::ComboBoxText::new();
        colscb.append(Some("Auto"), "Auto");
        colscb.append(Some("Exactly"), "Exactly");
        colscb.append(Some("Hide"), "Hide");
        colscb.set_active_id(match spec.borrow().columns {
            view::ColumnSpec::Auto => Some("Auto"),
            view::ColumnSpec::Hide(_) => Some("Hide"),
            view::ColumnSpec::Exactly(_) => Some("Exactly"),
        });
        let cols_frame = gtk::Frame::new(Some("Column Config"));
        let cols_box = gtk::Box::new(gtk::Orientation::Vertical, 5);
        cols_frame.add(&cols_box);
        root.pack_start(&cols_frame, true, true, 0);
        cols_box.pack_start(&colscb, false, false, 0);
        let cols_gui =
            Rc::new(clone!(@strong spec, @strong on_change, @weak cols_box => move || {
                for c in cols_box.get_children().into_iter().skip(1) {
                    c.hide();
                    cols_box.remove(&c)
                }
                match &spec.borrow().columns {
                    view::ColumnSpec::Auto => on_change(),
                    view::ColumnSpec::Exactly(cols) | view::ColumnSpec::Hide(cols) => {
                        let btnbox = gtk::Box::new(gtk::Orientation::Horizontal, 5);
                        let addbtn = gtk::Button::with_label("+");
                        let delbtn = gtk::Button::with_label("-");
                        btnbox.pack_start(&addbtn, false, false, 0);
                        btnbox.pack_start(&delbtn, false, false, 0);
                        cols_box.pack_start(&btnbox, false, false, 0);
                        let win = gtk::ScrolledWindow::new(
                            None::<&gtk::Adjustment>,
                            None::<&gtk::Adjustment>
                        );
                        win.set_policy(
                            gtk::PolicyType::Automatic,
                            gtk::PolicyType::Automatic
                        );
                        cols_box.pack_start(&win, true, true, 0);
                        let view = gtk::TreeView::new();
                        let store = gtk::ListStore::new(&[String::static_type()]);
                        win.add(&view);
                        for c in cols.iter() {
                            let iter = store.append();
                            store.set_value(&iter, 0, &c.to_value());
                        }
                        view.append_column(&{
                            let column = gtk::TreeViewColumn::new();
                            let cell = gtk::CellRendererText::new();
                            column.pack_start(&cell, true);
                            column.set_title("column");
                            column.add_attribute(&cell, "text", 0);
                            cell.set_property_editable(true);
                            cell.connect_edited(clone!(@weak store => move |_, p, txt| {
                                if let Some(iter) = store.get_iter(&p) {
                                    store.set_value(&iter, 0, &txt.to_value());
                                }
                            }));
                            column
                        });
                        view.get_selection().set_mode(gtk::SelectionMode::Single);
                        view.set_reorderable(true);
                        view.set_model(Some(&store));
                        addbtn.connect_clicked(clone!(
                            @weak view, @weak store => move |_| {
                                let iter = store.append();
                                store.set_value(&iter, 0, &"".to_value());
                                view.get_selection().select_iter(&iter);
                            }));
                        delbtn.connect_clicked(clone!(
                            @weak store, @weak view => move |_| {
                                let selection = view.get_selection();
                                if let Some((_, i)) = selection.get_selected() {
                                    store.remove(&i);
                                }
                            }));
                        let changed = Rc::new(clone!(
                            @weak store,
                            @strong spec,
                            @strong on_change => move || {
                                match &mut spec.borrow_mut().columns {
                                    view::ColumnSpec::Auto => (),
                                    view::ColumnSpec::Exactly(ref mut cols)
                                        | view::ColumnSpec::Hide(ref mut cols) => {
                                            cols.clear();
                                            store.foreach(|store, _, iter| {
                                                let v = store.get_value(&iter, 0);
                                                if let Ok(Some(c)) = v.get::<&str>() {
                                                    cols.push(String::from(c));
                                                }
                                                false
                                            })
                                        }
                                }
                                on_change()
                            }));
                        store.connect_row_changed(
                            clone!(@strong changed => move |_, _, _| changed())
                        );
                        store.connect_row_deleted(
                            clone!(@strong changed => move |_, _| changed())
                        );
                        store.connect_row_inserted(
                            clone!(@strong changed => move |_, _, _| changed())
                        );
                        cols_box.show_all();
                    }
                }
            }));
        colscb.connect_changed(clone!(
            @strong spec, @strong cols_gui => move |b| {
                match b.get_active_id() {
                    Some(s) if &*s == "Auto" => {
                        spec.borrow_mut().columns = view::ColumnSpec::Auto;
                    }
                    Some(s) if &*s == "Exactly" => {
                        spec.borrow_mut().columns = view::ColumnSpec::Exactly(Vec::new());
                    }
                    Some(s) if &*s == "Hide" => {
                        spec.borrow_mut().columns = view::ColumnSpec::Hide(Vec::new());
                    }
                    _ => { spec.borrow_mut().columns = view::ColumnSpec::Auto; }
                }
                cols_gui()
        }));
        cols_gui();
        Table { root, spec }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Table(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}

type DbgExpr = Rc<RefCell<Option<(gtk::Window, ExprInspector)>>>;

fn expr(
    ctx: &WidgetCtx,
    variables: &Vars,
    txt: &str,
    init: &view::Expr,
    on_change: impl Fn(view::Expr) + 'static,
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
        @strong on_change, @strong source, @weak inspect, @weak ibox => move |e| {
        match e.get_text().parse::<view::Expr>() {
            Err(e) => err_modal(&ibox, &format!("parse error: {}", e)),
            Ok(s) => {
                e.set_icon_from_icon_name(gtk::EntryIconPosition::Secondary, None);
                *source.borrow_mut() = s.clone();
                on_change(s);
            }
        }
    }));
    inspect.connect_toggled(clone!(
        @strong variables,
        @strong ctx,
        @strong on_change,
        @strong inspector,
        @strong source,
        @weak entry => move |b| {
        if !b.get_active() {
            if let Some((w, _)) = inspector.borrow_mut().take() {
                w.close()
            }
        } else {
            let w = gtk::Window::new(gtk::WindowType::Toplevel);
            w.set_default_size(640, 480);
            let on_change = {
                let entry = entry.clone();
                move |s: view::Expr| {
                    entry.set_text(&s.to_string());
                    entry.emit_activate();
                }
            };
            let si = ExprInspector::new(
                ctx.clone(),
                &variables,
                on_change,
                source.borrow().clone()
            );
            w.add(si.root());
            si.root().set_property_margin(5);
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

#[derive(Clone, Debug)]
pub(super) struct Action {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Expr>>,
    expr: DbgExpr,
    iter: Rc<RefCell<gtk::TreeIter>>,
}

impl Action {
    pub(super) fn new(
        ctx: &WidgetCtx,
        variables: &Vars,
        on_change: OnChange,
        store: &gtk::TreeStore,
        iter: &gtk::TreeIter,
        spec: view::Expr,
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
        let (l, e, expr) = expr(
            ctx,
            variables,
            "Action:",
            &*spec.borrow(),
            clone!(@strong update_desc, @strong spec, @strong on_change => move |s| {
                *spec.borrow_mut() = s;
                update_desc();
                on_change()
            }),
        );
        root.add((l, e));
        Action { root, spec, expr, iter }
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

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.expr.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Label {
    root: gtk::Box,
    spec: Rc<RefCell<view::Expr>>,
    expr: DbgExpr,
}

impl Label {
    pub(super) fn new(
        ctx: &WidgetCtx,
        variables: &Vars,
        on_change: OnChange,
        spec: view::Expr,
    ) -> Self {
        let root = gtk::Box::new(gtk::Orientation::Vertical, 0);
        let pathbox = gtk::Box::new(gtk::Orientation::Horizontal, 5);
        let spec = Rc::new(RefCell::new(spec));
        root.pack_start(&pathbox, false, false, 0);
        let (l, e, expr) = expr(
            ctx,
            variables,
            "Expr:",
            &*spec.borrow(),
            clone!(@strong spec => move |s| {
                *spec.borrow_mut() = s;
                on_change()
            }),
        );
        pathbox.pack_start(&l, false, false, 0);
        pathbox.pack_start(&e, true, true, 0);
        Label { root, spec, expr }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Label(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.expr.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Button {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Button>>,
    enabled_expr: DbgExpr,
    label_expr: DbgExpr,
    on_click_expr: DbgExpr,
}

impl Button {
    pub(super) fn new(
        ctx: &WidgetCtx,
        variables: &Vars,
        on_change: OnChange,
        spec: view::Button,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_expr) = expr(
            ctx,
            variables,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, label_expr) = expr(
            ctx,
            variables,
            "Label:",
            &spec.borrow().label,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().label = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, on_click_expr) = expr(
            ctx,
            variables,
            "Source:",
            &spec.borrow().on_click,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().on_click = s;
                on_change()
            }),
        );
        root.add((l, e));
        Button { root, spec, enabled_expr, label_expr, on_click_expr }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Button(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_expr.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.label_expr.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.on_click_expr.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Toggle {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Toggle>>,
    enabled_expr: DbgExpr,
    value_expr: DbgExpr,
    on_change_expr: DbgExpr,
}

impl Toggle {
    pub(super) fn new(
        ctx: &WidgetCtx,
        variables: &Vars,
        on_change: OnChange,
        spec: view::Toggle,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_expr) = expr(
            ctx,
            variables,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, value_expr) = expr(
            ctx,
            variables,
            "Value:",
            &spec.borrow().value,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().value = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, on_change_expr) = expr(
            ctx,
            variables,
            "On Change:",
            &spec.borrow().on_change,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().on_change = s;
                on_change();
            }),
        );
        root.add((l, e));
        Toggle { root, spec, enabled_expr, value_expr, on_change_expr }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Toggle(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_expr.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.value_expr.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.on_change_expr.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Selector {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Selector>>,
    enabled_expr: DbgExpr,
    choices_expr: DbgExpr,
    selected_expr: DbgExpr,
    on_change_expr: DbgExpr,
}

impl Selector {
    pub(super) fn new(
        ctx: &WidgetCtx,
        variables: &Vars,
        on_change: OnChange,
        spec: view::Selector,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_expr) = expr(
            ctx,
            variables,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, choices_expr) = expr(
            ctx,
            variables,
            "Choices:",
            &spec.borrow().choices,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().choices = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, selected_expr) = expr(
            ctx,
            variables,
            "Selected:",
            &spec.borrow().selected,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().selected = s;
                on_change();
            }),
        );
        root.add((l, e));
        let (l, e, on_change_expr) = expr(
            ctx,
            variables,
            "On Change:",
            &spec.borrow().on_change,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().on_change = s;
                on_change();
            }),
        );
        root.add((l, e));
        Selector { root, spec, enabled_expr, choices_expr, selected_expr, on_change_expr }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Selector(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_expr.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.choices_expr.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.selected_expr.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.on_change_expr.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Entry {
    root: TwoColGrid,
    spec: Rc<RefCell<view::Entry>>,
    enabled_source: DbgSrc,
    visible_source: DbgSrc,
    source: DbgSrc,
}

impl Entry {
    pub(super) fn new(
        ctx: &WidgetCtx,
        variables: &Rc<RefCell<HashMap<String, Value>>>,
        on_change: OnChange,
        spec: view::Entry,
    ) -> Self {
        let mut root = TwoColGrid::new();
        let spec = Rc::new(RefCell::new(spec));
        let (l, e, enabled_source) = source(
            ctx,
            variables,
            "Enabled:",
            &spec.borrow().enabled,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().enabled = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, visible_source) = source(
            ctx,
            variables,
            "Visible:",
            &spec.borrow().visible,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().visible = s;
                on_change()
            }),
        );
        root.add((l, e));
        let (l, e, source) = source(
            ctx,
            variables,
            "Source:",
            &spec.borrow().source,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().source = s;
                on_change()
            }),
        );
        root.add((l, e));
        root.add(parse_entry(
            "Sink:",
            &spec.borrow().sink,
            clone!(@strong on_change, @strong spec => move |s| {
                spec.borrow_mut().sink = s;
                on_change()
            }),
        ));
        Entry { root, spec, enabled_source, visible_source, source }
    }

    pub(super) fn spec(&self) -> view::WidgetKind {
        view::WidgetKind::Entry(self.spec.borrow().clone())
    }

    pub(super) fn root(&self) -> &gtk::Widget {
        self.root.root().upcast_ref()
    }

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, si)) = &*self.enabled_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.visible_source.borrow() {
            si.update(tgt, value);
        }
        if let Some((_, si)) = &*self.source.borrow() {
            si.update(tgt, value);
        }
    }
}

#[derive(Clone, Debug)]
struct Series {
    x: DbgSrc,
    y: DbgSrc,
    spec: Rc<RefCell<view::Series>>,
}

#[derive(Clone, Debug)]
pub(super) struct LinePlot {
    root: gtk::Box,
    spec: Rc<RefCell<view::LinePlot>>,
    x_min: DbgSrc,
    x_max: DbgSrc,
    y_min: DbgSrc,
    y_max: DbgSrc,
    keep_points: DbgSrc,
    series: Rc<RefCell<IndexMap<usize, Series>>>,
}

impl LinePlot {
    pub(super) fn new(
        ctx: &WidgetCtx,
        variables: &Rc<RefCell<HashMap<String, Value>>>,
        on_change: OnChange,
        spec: view::LinePlot,
    ) -> Self {
        let spec = Rc::new(RefCell::new(spec));
        let root = gtk::Box::new(gtk::Orientation::Vertical, 5);
        LinePlot::build_chart_style_editor(&root, &on_change, &spec);
        LinePlot::build_axis_style_editor(&root, &on_change, &spec);
        let (x_min, x_max, y_min, y_max, keep_points) =
            LinePlot::build_axis_range_editor(ctx, variables, &root, &on_change, &spec);
        let series =
            LinePlot::build_series_editor(ctx, variables, &root, &on_change, &spec);
        LinePlot { root, spec, x_min, x_max, y_min, y_max, keep_points, series }
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
            spec.borrow_mut().x_grid = b.get_active();
            on_change()
        }));
        axis.attach(&x_grid, 0, 2, 1);
        let y_grid = gtk::CheckButton::with_label("Y Axis Grid");
        y_grid.set_active(spec.borrow().y_grid);
        y_grid.connect_toggled(clone!(@strong on_change, @strong spec => move |b| {
            spec.borrow_mut().y_grid = b.get_active();
            on_change()
        }));
        axis.attach(&y_grid, 0, 2, 1);
    }

    fn build_axis_range_editor(
        ctx: &WidgetCtx,
        variables: &Rc<RefCell<HashMap<String, Value>>>,
        root: &gtk::Box,
        on_change: &OnChange,
        spec: &Rc<RefCell<view::LinePlot>>,
    ) -> (DbgSrc, DbgSrc, DbgSrc, DbgSrc, DbgSrc) {
        let range_exp = gtk::Expander::new(Some("Axis Range"));
        util::expander_touch_enable(&range_exp);
        let mut range = TwoColGrid::new();
        root.pack_start(&range_exp, false, false, 0);
        range_exp.add(range.root());
        let (l, e, x_min) = source(
            ctx,
            variables,
            "x min:",
            &spec.borrow().x_min,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_min = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, x_max) = source(
            ctx,
            variables,
            "x max:",
            &spec.borrow().x_max,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().x_max = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, y_min) = source(
            ctx,
            variables,
            "y min:",
            &spec.borrow().y_min,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_min = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, y_max) = source(
            ctx,
            variables,
            "y max:",
            &spec.borrow().y_max,
            clone!(@strong spec, @strong on_change => move |s| {
                spec.borrow_mut().y_max = s;
                on_change()
            }),
        );
        range.add((l, e));
        let (l, e, keep_points) = source(
            ctx,
            variables,
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
            fill_color.set_rgba(&gdk::RGBA {
                red: c.r,
                green: c.g,
                blue: c.b,
                alpha: 1.,
            });
        }
        has_fill.connect_toggled(clone!(
            @strong on_change,
            @strong spec,
            @weak fill_reveal,
            @weak fill_color => move |b| {
                if b.get_active() {
                    fill_reveal.set_reveal_child(true);
                    let c = fill_color.get_rgba();
                    let c = view::RGB { r: c.red, g: c.green, b: c.blue };
                    spec.borrow_mut().fill = Some(c);
                } else {
                    fill_reveal.set_reveal_child(false);
                    spec.borrow_mut().fill = None;
                }
                on_change()
        }));
        fill_color.connect_color_set(
            clone!(@strong on_change, @strong spec => move |b| {
                let c = b.get_rgba();
                let c = view::RGB { r: c.red, g: c.green, b: c.blue };
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
        ctx: &WidgetCtx,
        variables: &Rc<RefCell<HashMap<String, Value>>>,
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
            @strong variables,
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
                let rgba = gdk::RGBA { red: c.r, green: c.g, blue: c.b, alpha: 1.};
                let line_color = gtk::ColorButton::with_rgba(&rgba);
                let lbl_line_color = gtk::Label::new(Some("Line Color:"));
                line_color.connect_color_set(clone!(
                    @strong on_change, @strong spec => move |b| {
                        let c = b.get_rgba();
                        let c = view::RGB { r: c.red, g: c.green, b: c.blue };
                        spec.borrow_mut().line_color = c;
                        on_change()
                    }));
                grid.add((lbl_line_color, line_color));
                let (l, e, x) = source(
                    &ctx,
                    &variables,
                    "X:",
                    &spec.borrow().x,
                    clone!(@strong spec, @strong on_change => move |s| {
                        spec.borrow_mut().x = s;
                        on_change()
                    })
                );
                grid.add((l, e));
                let (l, e, y) = source(
                    &ctx,
                    &variables,
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
                series.borrow_mut().insert(i, Series { x, y, spec });
                seriesbox.show_all();
                let grid_root = grid.root();
                remove.connect_clicked(clone!(
                    @strong series,
                    @weak grid_root,
                    @weak seriesbox,
                    @strong on_change => move |_| {
                        grid_root.hide();
                        for c in seriesbox.get_children() {
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
                x: view::Source::Load(Box::new(view::Source::Constant(
                    Value::String(Chars::from("/somewhere/in/netidx/x")))
                )),
                y: view::Source::Load(Box::new(view::Source::Constant(
                    Value::String(Chars::from("/somewhere/in/netidx/y")))
                )),
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

    pub(super) fn update(&self, tgt: Target, value: &Value) {
        if let Some((_, s)) = &*self.x_min.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.x_max.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.y_min.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.y_max.borrow() {
            s.update(tgt, value);
        }
        if let Some((_, s)) = &*self.keep_points.borrow() {
            s.update(tgt, value);
        }
        for s in self.series.borrow().values() {
            if let Some((_, s)) = &*s.x.borrow() {
                s.update(tgt, value);
            }
            if let Some((_, s)) = &*s.y.borrow() {
                s.update(tgt, value);
            }
        }
    }
}

#[derive(Clone, Debug)]
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
            spec.borrow_mut().pack = match c.get_active_id() {
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
        on_change(match c.get_active_id() {
            Some(s) if &*s == "Horizontal" => view::Direction::Horizontal,
            Some(s) if &*s == "Vertical" => view::Direction::Vertical,
            _ => view::Direction::Horizontal,
        })
    });
    dircb
}

#[derive(Clone, Debug)]
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
            spec.borrow_mut().homogeneous = b.get_active();
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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
                spec.borrow_mut().homogeneous_columns = b.get_active();
                on_change()
            }),
        );
        root.attach(&homogeneous_columns, 0, 2, 1);
        let homogeneous_rows = gtk::CheckButton::with_label("Homogeneous Rows");
        homogeneous_rows.set_active(spec.borrow().homogeneous_rows);
        homogeneous_rows.connect_toggled(
            clone!(@strong on_change, @strong spec => move |b| {
                spec.borrow_mut().homogeneous_rows = b.get_active();
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
