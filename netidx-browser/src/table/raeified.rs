use super::super::{
    util::{err_modal, toplevel},
    BSCtxRef, ImageSpec, WVal,
};
use super::shared::{
    BVal, CTCommonResolved, Color, ColumnSpec, ColumnType, ColumnTypeCombo,
    ColumnTypeCommon, ColumnTypeProgress, ColumnTypeSpin, ColumnTypeText,
    ColumnTypeToggle, IndexDescriptor, OrLoad, SelectionMode, SharedState, SortDir,
    SortSpec, NAME_COL,
};
use crate::bscript::LocalEvent;
use arcstr::ArcStr;
use futures::channel::oneshot;
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use gdk::{keys, EventButton, EventKey, RGBA};
use gio::prelude::*;
use glib::{self, clone, idle_add_local, signal::Inhibit, source::Continue};
use gtk::{
    prelude::*, CellRenderer, CellRendererCombo, CellRendererPixbuf, CellRendererSpin,
    CellRendererText, CellRendererToggle, ListStore, SortColumn, SortType, StateFlags,
    StyleContext, TreeIter, TreeModel, TreePath, TreeView, TreeViewColumn,
    TreeViewColumnSizing,
};
use indexmap::IndexMap;
use netidx::{
    chars::Chars,
    path::Path,
    pool::{Pool, Pooled},
    subscriber::{Dval, Event, SubId, UpdatesFlags, Value},
    utils::Either,
};
use netidx_bscript::vm;
use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fmt::Write,
    ops::Deref,
    rc::{Rc, Weak},
    result,
};

struct Subscription {
    _sub: Dval,
    row: TreeIter,
    col: u32,
}

lazy_static! {
    static ref FORMATTED: Pool<String> = Pool::new(50_000, 1024);
}

fn get_sort_column(store: &ListStore) -> Option<u32> {
    match store.sort_column_id() {
        None | Some((SortColumn::Default, _)) => None,
        Some((SortColumn::Index(c), _)) => {
            if c == 0 {
                None
            } else {
                Some(c)
            }
        }
    }
}

fn compare_row(col: i32, m: &TreeModel, r0: &TreeIter, r1: &TreeIter) -> Ordering {
    let v0_v = m.value(r0, col);
    let v1_v = m.value(r1, col);
    let v0_r = v0_v.get::<&BVal>();
    let v1_r = v1_v.get::<&BVal>();
    match (v0_r, v1_r) {
        (Err(_), Err(_)) => Ordering::Equal,
        (Err(_), _) => Ordering::Greater,
        (_, Err(_)) => Ordering::Less,
        (Ok(v0), Ok(v1)) => v0.value.partial_cmp(&v1.value).unwrap_or(Ordering::Equal),
    }
}

impl clone::Downgrade for RaeifiedTable {
    type Weak = RaeifiedTableWeak;

    fn downgrade(&self) -> Self::Weak {
        RaeifiedTableWeak(Rc::downgrade(&self.0))
    }
}

impl clone::Upgrade for RaeifiedTableWeak {
    type Strong = RaeifiedTable;

    fn upgrade(&self) -> Option<Self::Strong> {
        Weak::upgrade(&self.0).map(RaeifiedTable)
    }
}

pub(super) struct RaeifiedTableInner {
    pub(super) path: Path,
    shared: Rc<SharedState>,
    by_id: RefCell<FxHashMap<SubId, Subscription>>,
    columns_autosizing: Rc<Cell<bool>>,
    descriptor: IndexDescriptor,
    destroyed: Cell<bool>,
    name_column: RefCell<Option<TreeViewColumn>>,
    sort_column: Cell<Option<u32>>,
    sort_temp_disabled: Cell<bool>,
    store: ListStore,
    style: StyleContext,
    subscribed: RefCell<FxHashMap<String, HashSet<u32>>>,
    update: RefCell<IndexMap<SubId, Value, FxBuildHasher>>,
    vector_mode: bool,
    view: TreeView,
    combo_models: RefCell<IndexMap<Value, ListStore, FxBuildHasher>>,
    combo_models_used: Cell<usize>,
}

#[derive(Clone)]
pub(super) struct RaeifiedTable(Rc<RaeifiedTableInner>);

impl Deref for RaeifiedTable {
    type Target = RaeifiedTableInner;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

pub(super) struct RaeifiedTableWeak(Weak<RaeifiedTableInner>);

impl RaeifiedTable {
    fn set_column_properties(
        &self,
        column: &TreeViewColumn,
        name: &Chars,
        common: &Option<CTCommonResolved>,
        sorting_disabled: bool,
    ) {
        let t = self;
        column.set_title(&**name);
        if let Some(CTCommonResolved { source: id, .. }) = common.as_ref() {
            let id = *id;
            column.connect_clicked(clone!(@weak t, @strong name => move |_| {
                t.shared.on_header_click.borrow_mut().update(
                    &mut t.shared.ctx.borrow_mut(),
                    &vm::Event::User(LocalEvent::Event(Value::from(name.clone())))
                );
            }));
            if sorting_disabled {
                column.set_clickable(true);
            } else {
                t.store()
                    .set_sort_func(SortColumn::Index(id as u32), move |m, r0, r1| {
                        compare_row(id, m, r0, r1)
                    });
                column.set_sort_column_id(id);
            }
        }
        column.set_sizing(TreeViewColumnSizing::Fixed);
        column.set_resizable(t.shared.columns_resizable.get());
        let column_widths = &t.shared.column_widths;
        if let Some(w) = column_widths.borrow().get(&**name) {
            column.set_fixed_width(*w);
        }
        let columns_autosizing = t.columns_autosizing.clone();
        column.connect_width_notify(
            clone!(@strong column_widths, @strong name => move |c| {
                if ! columns_autosizing.get() {
                    let mut saved = column_widths.borrow_mut();
                    *saved.entry((&*name).into()).or_insert(1) = c.width();
                }
            }),
        );
    }

    fn setup_editable<T: CellRendererTextExt + CellRendererExt>(
        &self,
        cell: &T,
        common: &CTCommonResolved,
    ) {
        let t = self;
        let editable: Rc<RefCell<Option<gtk::CellEditable>>> =
            Rc::new(RefCell::new(None));
        cell.connect_edited(
            clone!(@weak t, @strong common, @strong editable => move |_, p, v| {
                if let Some(path) = t.path_from_treepath(&p, &*common.source_column) {
                    let v = vec![Value::from(path), Value::from(String::from(v))];
                    t.shared.on_edit.borrow_mut().update(
                        &mut t.shared.ctx.borrow_mut(),
                        &vm::Event::User(LocalEvent::Event(v.into()))
                    );
                }
                let e = editable.borrow_mut().take();
                if let Some(e) = e {
                    unsafe { e.destroy(); }
                }
            }),
        );
        cell.connect_editing_started(clone!(@strong editable => move |_, e, _| {
            *editable.borrow_mut() = Some(e.clone());
            e.connect_editing_done(|e| {
                e.event(&gdk::Event::new(gdk::EventType::FocusChange));
            });
            e.connect_remove_widget(clone!(@strong editable => move |e| {
                if e.is_editing_canceled() {
                    editable.borrow_mut().take();
                    unsafe { e.destroy() }
                }
            }));
        }));
    }

    fn add_text_column(
        &self,
        vector_mode: bool,
        name: &Chars,
        sorting_disabled: bool,
        spec: &ColumnTypeText,
    ) {
        let t = self;
        let column = TreeViewColumn::new();
        let cell = CellRendererText::new();
        column.pack_start(&cell, true);
        let common = spec.common.resolve(vector_mode, name, &self.descriptor);
        if let Some(common) = common.as_ref() {
            let foreground =
                spec.foreground.as_ref().and_then(|v| v.resolve(&t.descriptor));
            if t.shared.column_editable.borrow().is_match(common.source as usize, &**name)
            {
                cell.set_editable(true);
            }
            let f =
                Box::new(clone!(@weak t, @strong cell, @strong name, @strong common =>
                    move |_: &TreeViewColumn,
                _: &CellRenderer,
                _: &TreeModel,
                i: &TreeIter| {
                    t.render_text_cell(&common, &*name, &foreground, &cell, i)
                }));
            TreeViewColumnExt::set_cell_data_func(&column, &cell, Some(f));
            self.setup_editable(&cell, common);
        }
        t.set_column_properties(&column, name, &common, sorting_disabled);
        t.view().append_column(&column);
    }

    fn add_image_column(&self, name: &Chars, spec: &ColumnTypeCommon) {
        let t = self;
        let column = TreeViewColumn::new();
        let cell = CellRendererPixbuf::new();
        column.pack_start(&cell, true);
        let common = spec.resolve(false, name, &self.descriptor);
        if let Some(common) = common.as_ref() {
            let f = Box::new(clone!(
            @weak t, @strong cell, @strong name, @strong common =>
                move |_: &TreeViewColumn,
            _: &CellRenderer,
            _: &TreeModel,
            i: &TreeIter| {
                t.render_image_cell(&common, &*name, &cell, i)
            }));
            TreeViewColumnExt::set_cell_data_func(&column, &cell, Some(f));
        }
        t.set_column_properties(&column, name, &common, true);
        t.view().append_column(&column);
    }

    fn add_toggle_column(
        &self,
        sorting_disabled: bool,
        name: &Chars,
        spec: &ColumnTypeToggle,
    ) {
        let t = self;
        let column = TreeViewColumn::new();
        let cell = CellRendererToggle::new();
        column.pack_start(&cell, true);
        let common = spec.common.resolve(false, name, &self.descriptor);
        if let Some(common) = common.as_ref() {
            let radio = spec.radio.as_ref().and_then(|v| v.resolve(&t.descriptor));
            if t.shared.column_editable.borrow().is_match(common.source as usize, &**name)
            {
                cell.set_activatable(true)
            } else {
                cell.set_activatable(false)
            }
            let f =
                Box::new(clone!(@weak t, @strong cell, @strong name, @strong common =>
                    move |_: &TreeViewColumn,
                _: &CellRenderer,
                _: &TreeModel,
                i: &TreeIter| {
                    t.render_toggle_cell(&common, &*name, &radio, &cell, i)
                }));
            TreeViewColumnExt::set_cell_data_func(&column, &cell, Some(f));
            cell.connect_toggled(clone!(@weak t, @strong common => move |_, p| {
                if let Some(path) = t.path_from_treepath(&p, &*common.source_column) {
                    let val = t.store()
                        .iter(&p)
                        .map(|i| t.store().value(&i, common.source))
                        .and_then(|v| v.get::<&BVal>().ok()
                                       .and_then(|v| v.value.clone().cast_to::<bool>().ok()))
                        .unwrap_or(false);
                    let val = vec![Value::from(path), Value::from(!val)];
                    t.shared.on_edit.borrow_mut().update(
                        &mut t.shared.ctx.borrow_mut(),
                        &vm::Event::User(LocalEvent::Event(val.into()))
                    );
                }
            }));
        }
        t.set_column_properties(&column, name, &common, sorting_disabled);
        t.view().append_column(&column);
    }

    fn add_combo_column(
        &self,
        sorting_disabled: bool,
        name: &Chars,
        spec: &ColumnTypeCombo,
    ) {
        let t = self;
        let column = TreeViewColumn::new();
        let cell = CellRendererCombo::new();
        column.pack_start(&cell, true);
        let common = spec.common.resolve(false, name, &self.descriptor);
        if let Some(common) = common.as_ref() {
            let choices = spec.choices.resolve(&t.descriptor);
            let has_entry =
                spec.has_entry.as_ref().and_then(|v| v.resolve(&t.descriptor));
            if t.shared.column_editable.borrow().is_match(common.source as usize, &**name)
            {
                cell.set_editable(true);
            }
            let f =
                Box::new(clone!(@weak t, @strong cell, @strong name, @strong common =>
                move |_: &TreeViewColumn,
                _: &CellRenderer,
                _: &TreeModel,
                i: &TreeIter| {
                    t.render_combo_cell(&common, &*name, &choices, &has_entry, &cell, i)
                }));
            TreeViewColumnExt::set_cell_data_func(&column, &cell, Some(f));
            self.setup_editable(&cell, common);
        }
        t.set_column_properties(&column, name, &common, sorting_disabled);
        t.view().append_column(&column);
    }

    fn add_spin_column(
        &self,
        sorting_disabled: bool,
        name: &Chars,
        spec: &ColumnTypeSpin,
    ) {
        let t = self;
        let column = TreeViewColumn::new();
        let cell = CellRendererSpin::new();
        column.pack_start(&cell, true);
        let common = spec.common.resolve(false, name, &self.descriptor);
        if let Some(common) = common.as_ref() {
            let min = spec.min.as_ref().and_then(|v| v.resolve(&t.descriptor));
            let max = spec.max.as_ref().and_then(|v| v.resolve(&t.descriptor));
            let increment =
                spec.increment.as_ref().and_then(|v| v.resolve(&t.descriptor));
            let page_increment =
                spec.page_increment.as_ref().and_then(|v| v.resolve(&t.descriptor));
            let climb_rate =
                spec.climb_rate.as_ref().and_then(|v| v.resolve(&t.descriptor));
            let digits = spec.digits.as_ref().and_then(|v| v.resolve(&t.descriptor));
            if t.shared.column_editable.borrow().is_match(common.source as usize, &**name)
            {
                cell.set_editable(true);
                cell.set_adjustment(Some(&gtk::Adjustment::new(
                    0., 0., 1., 0.01, 0.1, 0.,
                )));
            }
            let f =
                Box::new(clone!(@weak t, @strong cell, @strong name, @strong common =>
                move |_: &TreeViewColumn,
                _: &CellRenderer,
                _: &TreeModel,
                i: &TreeIter| {
                    t.render_spin_cell(
                        &common,
                        &*name,
                        &min,
                        &max,
                        &increment,
                        &page_increment,
                        &climb_rate,
                        &digits,
                        &cell,
                        i
                    )
                }));
            TreeViewColumnExt::set_cell_data_func(&column, &cell, Some(f));
            t.setup_editable(&cell, common);
        }
        t.set_column_properties(&column, name, &common, sorting_disabled);
        t.view().append_column(&column);
    }

    fn add_columns(
        &self,
        vector_mode: bool,
        spec: IndexMap<Chars, ColumnSpec, FxBuildHasher>,
        sorting_disabled: bool,
    ) {
        let t = self;
        if t.shared.show_name_column.get() {
            t.view().append_column(&{
                let column = TreeViewColumn::new();
                let cell = CellRendererText::new();
                column.pack_start(&cell, true);
                column.set_title(NAME_COL);
                column.add_attribute(&cell, "text", 0);
                if sorting_disabled {
                    column.set_clickable(true);
                } else {
                    column.set_sort_column_id(0);
                }
                column.set_sizing(TreeViewColumnSizing::Fixed);
                column.set_resizable(t.shared.columns_resizable.get());
                let column_widths = &t.shared.column_widths;
                if let Some(w) = column_widths.borrow().get(NAME_COL) {
                    column.set_fixed_width(*w);
                }
                let columns_autosizing = t.columns_autosizing.clone();
                column.connect_width_notify(clone!(@strong column_widths => move |c| {
                    if !columns_autosizing.get() {
                        *column_widths.borrow_mut().entry(NAME_COL.into()).or_insert(1) = c.width();
                    }
                }));
                *self.name_column.borrow_mut() = Some(column.clone());
                column
            });
        }
        if vector_mode {
            t.add_text_column(
                vector_mode,
                &Chars::from("value"),
                sorting_disabled,
                &ColumnTypeText {
                    common: ColumnTypeCommon { source: None, background: None },
                    foreground: None,
                },
            )
        } else {
            for (name, spec) in spec.iter() {
                match &spec.typ {
                    ColumnType::Text(cs) => {
                        t.add_text_column(vector_mode, &name, sorting_disabled, cs)
                    }
                    ColumnType::Image(cs) => t.add_image_column(&name, cs),
                    ColumnType::Toggle(cs) => {
                        t.add_toggle_column(sorting_disabled, &name, cs)
                    }
                    ColumnType::Combo(cs) => {
                        t.add_combo_column(sorting_disabled, &name, cs)
                    }
                    ColumnType::Spin(cs) => {
                        t.add_spin_column(sorting_disabled, &name, cs)
                    }
                    ColumnType::Progress(_) => unimplemented!(),
                    ColumnType::Hidden => (),
                }
            }
        }
    }

    pub(super) fn new(shared: Rc<SharedState>) -> RaeifiedTable {
        let path = shared.path.borrow().clone();
        let (selection_changed, descriptor) = shared.apply_filters();
        let column_spec = shared.generate_column_spec(&descriptor);
        let view = TreeView::new();
        view.set_no_show_all(true);
        shared.root.add(&view);
        view.selection().set_mode(gtk::SelectionMode::None);
        let vector_mode = descriptor.cols.len() == 0;
        let column_types = if vector_mode {
            vec![String::static_type(), BVal::static_type()]
        } else {
            (0..descriptor.cols.len() + 1)
                .into_iter()
                .map(|i| if i == 0 { String::static_type() } else { BVal::static_type() })
                .collect::<Vec<_>>()
        };
        let store = ListStore::new(&column_types);
        let empty =
            BVal { value: Value::from(""), formatted: Pooled::orphan(String::new()) }
                .to_value();
        for row in descriptor.rows.iter() {
            let iter = store.append();
            store.set_value(&iter, 0, &row.to_value());
            for col in 1..column_types.len() {
                store.set_value(&iter, col as u32, &empty);
            }
        }
        let style = view.style_context();
        let t = RaeifiedTable(Rc::new(RaeifiedTableInner {
            path,
            shared,
            descriptor,
            store,
            style,
            vector_mode,
            view,
            by_id: RefCell::new(HashMap::default()),
            columns_autosizing: Rc::new(Cell::new(false)),
            destroyed: Cell::new(false),
            name_column: RefCell::new(None),
            sort_column: Cell::new(None),
            sort_temp_disabled: Cell::new(false),
            subscribed: RefCell::new(HashMap::default()),
            update: RefCell::new(IndexMap::default()),
            combo_models: RefCell::new(IndexMap::default()),
            combo_models_used: Cell::new(0),
        }));
        if t.shared.column_widths.borrow().len() > 1000 {
            let cols =
                t.descriptor.cols.iter().map(|c| c.0.clone()).collect::<FxHashSet<_>>();
            t.shared
                .column_widths
                .borrow_mut()
                .retain(|name, _| cols.contains(name.as_str()));
        }
        let sorting_disabled = match &*t.shared.sort_mode.borrow() {
            SortSpec::Disabled | SortSpec::External(_) => true,
            SortSpec::Column(_, _) | SortSpec::None => false,
        };
        t.add_columns(vector_mode, column_spec, sorting_disabled);
        t.view().set_model(Some(t.store()));
        t.view().connect_destroy(clone!(@weak t => move |_| t.destroyed.set(true)));
        t.store().connect_sort_column_changed(
            clone!(@weak t => move |_| t.handle_sort_column_changed()),
        );
        t.view().set_fixed_height_mode(true);
        t.view().connect_key_press_event(clone!(
            @weak t => @default-return Inhibit(false), move |_, k| t.handle_key(k)));
        t.view().connect_button_press_event(clone!(
            @weak t => @default-return Inhibit(false), move |_, b| t.handle_button(b)));
        t.view().connect_row_activated(
            clone!(@weak t => move |_, p, _| t.handle_row_activated(p)),
        );
        t.view().connect_cursor_changed(clone!(@weak t => move |_| t.cursor_changed()));
        match &*t.shared.sort_mode.borrow() {
            SortSpec::None => (),
            SortSpec::Disabled => (),
            SortSpec::External(spec) => {
                for col in t.view().columns() {
                    col.set_sort_indicator(false);
                    if let Some(title) = col.title() {
                        if let Some(dir) = spec.get(&*title) {
                            col.set_sort_indicator(true);
                            col.set_sort_order(match dir {
                                SortDir::Ascending => gtk::SortType::Ascending,
                                SortDir::Descending => gtk::SortType::Descending,
                            });
                        }
                    }
                }
            }
            SortSpec::Column(col, dir) => {
                let col = Path::from(col.clone());
                let idx =
                    t.0.descriptor.cols.iter().enumerate().find_map(|(i, (c, _))| {
                        if c == &col {
                            Some(i + 1)
                        } else {
                            None
                        }
                    });
                let dir = match dir {
                    SortDir::Ascending => gtk::SortType::Ascending,
                    SortDir::Descending => gtk::SortType::Descending,
                };
                if let Some(i) = idx {
                    t.store().set_sort_column_id(gtk::SortColumn::Index(i as u32), dir)
                }
            }
        }
        if selection_changed {
            t.handle_selection_changed()
        }
        t
    }

    fn handle_row_activated(&self, p: &TreePath) {
        if let Some(iter) = self.store().iter(&p) {
            if let Ok(row_name) = self.store().value(&iter, 0).get::<&str>() {
                let path = String::from(&*self.path.append(row_name));
                let e = LocalEvent::Event(Value::String(Chars::from(path)));
                self.shared
                    .on_activate
                    .borrow_mut()
                    .update(&mut self.shared.ctx.borrow_mut(), &vm::Event::User(e));
            }
        }
    }

    fn handle_sort_column_changed(&self) {
        if self.sort_temp_disabled.get() {
            return;
        }
        match get_sort_column(self.store()) {
            Some(col) => {
                self.sort_column.set(Some(col));
            }
            None => {
                self.sort_column.set(None);
                self.store().set_unsorted();
            }
        }
        let t = self;
        idle_add_local(clone!(@weak t => @default-return Continue(false), move || {
            t.update_subscriptions();
            Continue(false)
        }));
    }

    fn render_cell_selected<T: CellRendererExt>(
        &self,
        common: &CTCommonResolved,
        cr: &T,
        i: &TreeIter,
        name: &str,
    ) -> bool {
        let sel = self.shared.selected.borrow();
        match self.row_of(Either::Right(i)).as_ref().map(|r| r.get::<&str>().unwrap()) {
            Some(r) if sel.get(r).map(|t| t.contains(name)).unwrap_or(false) => {
                let bg = StyleContextExt::style_property_for_state(
                    &self.style,
                    "background-color",
                    StateFlags::SELECTED,
                )
                .get::<RGBA>()
                .unwrap();
                cr.set_cell_background_rgba(Some(&bg));
                true
            }
            Some(_) | None => {
                let bg = common
                    .background
                    .as_ref()
                    .and_then(|s| s.load(i, self.store()))
                    .map(|c| c.0);
                cr.set_cell_background_rgba(bg.as_ref());
                false
            }
        }
    }

    fn render_text_cell(
        &self,
        common: &CTCommonResolved,
        name: &str,
        foreground: &Option<OrLoad<Color>>,
        cr: &CellRendererText,
        i: &TreeIter,
    ) {
        let bv = self.store().value(i, common.source);
        cr.set_text(match bv.get::<&BVal>() {
            Ok(v) => Some(v.formatted.as_str()),
            Err(_) => None,
        });
        if self.render_cell_selected(common, cr, i, name) {
            let fg = self.style.color(StateFlags::SELECTED);
            cr.set_foreground_rgba(Some(&fg));
        } else {
            let fg =
                foreground.as_ref().and_then(|s| s.load(i, self.store())).map(|c| c.0);
            cr.set_foreground_rgba(fg.as_ref());
        }
    }

    fn render_spin_cell(
        &self,
        common: &CTCommonResolved,
        name: &str,
        min: &Option<OrLoad<f64>>,
        max: &Option<OrLoad<f64>>,
        increment: &Option<OrLoad<f64>>,
        page_increment: &Option<OrLoad<f64>>,
        climb_rate: &Option<OrLoad<f64>>,
        digits: &Option<OrLoad<u32>>,
        cr: &CellRendererSpin,
        i: &TreeIter,
    ) {
        let bv = self.store().value(i, common.source);
        let cur = bv.get::<&BVal>().ok().map(|bv| bv.formatted.as_str());
        let min = min.as_ref().and_then(|v| v.load(i, self.store())).unwrap_or(0.);
        let max = max.as_ref().and_then(|v| v.load(i, self.store())).unwrap_or(1.);
        let increment =
            increment.as_ref().and_then(|v| v.load(i, self.store())).unwrap_or(0.01);
        let page_increment =
            page_increment.as_ref().and_then(|v| v.load(i, self.store())).unwrap_or(0.1);
        let climb_rate =
            climb_rate.as_ref().and_then(|v| v.load(i, self.store())).unwrap_or(0.01);
        let digits = digits.as_ref().and_then(|v| v.load(i, self.store())).unwrap_or(2);
        cr.set_text(cur);
        cr.adjustment().map(|a| {
            a.set_lower(min);
            a.set_upper(max);
            a.set_step_increment(increment);
            a.set_page_increment(page_increment);
        });
        cr.set_climb_rate(climb_rate);
        cr.set_digits(digits);
        self.render_cell_selected(common, cr, i, name);
    }

    fn render_image_cell(
        &self,
        common: &CTCommonResolved,
        name: &str,
        cr: &CellRendererPixbuf,
        i: &TreeIter,
    ) {
        let bv = self.store().value(i, common.source);
        let spec = bv
            .get::<&BVal>()
            .ok()
            .and_then(|v| v.value.clone().cast_to::<ImageSpec>().ok());
        match spec {
            None => {
                cr.set_icon_name(None);
                cr.set_pixbuf(None);
            }
            Some(ImageSpec::Icon { name, size: _ }) => cr.set_icon_name(Some(&*name)),
            Some(spec @ ImageSpec::PixBuf { .. }) => {
                cr.set_pixbuf(spec.get_pixbuf().as_ref())
            }
        }
        self.render_cell_selected(common, cr, i, name);
    }

    fn render_toggle_cell(
        &self,
        common: &CTCommonResolved,
        name: &str,
        radio: &Option<OrLoad<bool>>,
        cr: &CellRendererToggle,
        i: &TreeIter,
    ) {
        let bv = self.store().value(i, common.source);
        let val = bv
            .get::<&BVal>()
            .ok()
            .and_then(|v| v.value.clone().cast_to::<bool>().ok())
            .unwrap_or(false);
        let radio = radio.as_ref().and_then(|s| s.load(i, self.store())).unwrap_or(false);
        cr.set_active(val);
        cr.set_radio(radio);
        self.render_cell_selected(common, cr, i, name);
    }

    fn render_combo_cell(
        &self,
        common: &CTCommonResolved,
        name: &str,
        choices: &Option<OrLoad<Value>>,
        has_entry: &Option<OrLoad<bool>>,
        cr: &CellRendererCombo,
        i: &TreeIter,
    ) {
        let bv = self.store().value(i, common.source);
        let val =
            bv.get::<&BVal>().ok().and_then(|v| v.value.clone().cast_to::<Chars>().ok());
        let choices = choices
            .as_ref()
            .and_then(|s| s.load(i, self.store()))
            .unwrap_or_else(|| Value::from(Vec::<Value>::new()));
        let has_entry =
            has_entry.as_ref().and_then(|s| s.load(i, self.store())).unwrap_or(false);
        let model = {
            let mut models = self.combo_models.borrow_mut();
            let models = &mut *models;
            let (i, model) = match models.get_full(&choices) {
                Some((i, _, model)) => (i, model.clone()),
                None => {
                    let spec = choices
                        .clone()
                        .cast_to::<Vec<Chars>>()
                        .ok()
                        .unwrap_or_else(Vec::new);
                    let model = ListStore::new(&[String::static_type()]);
                    for choice in spec {
                        let iter = model.append();
                        model.set_value(&iter, 0, &choice.to_value());
                    }
                    let (i, _) = models.insert_full(choices, model.clone());
                    (i, model)
                }
            };
            let used = self.combo_models_used.get();
            if i >= used && used < models.len() {
                models.swap_indices(i, used);
                self.combo_models_used.set(used + 1);
            }
            if models.len() > 1000 {
                for _ in self.combo_models_used.get()..models.len() {
                    models.pop();
                }
                self.combo_models_used.set(0);
            }
            model
        };
        cr.set_has_entry(has_entry);
        if cr.model().as_ref() != Some(model.upcast_ref()) {
            cr.set_model(Some(&model));
        }
        cr.set_text_column(0);
        cr.set_text(val.as_ref().map(|v| &**v));
        self.render_cell_selected(common, cr, i, name);
    }

    fn handle_key(&self, key: &EventKey) -> Inhibit {
        let clear_selection = || {
            let n = {
                let mut paths = self.shared.selected.borrow_mut();
                let n = paths.len();
                paths.clear();
                n
            };
            self.shared.selected_path.set_label("");
            if n > 0 {
                self.handle_selection_changed();
            }
        };
        let kv = key.keyval();
        if kv == keys::constants::Escape {
            clear_selection();
            self.view().set_cursor(&TreePath::new(), None::<&TreeViewColumn>, false);
        }
        if kv == keys::constants::Down
            || kv == keys::constants::Up
            || kv == keys::constants::Left
            || kv == keys::constants::Right
        {
            if !key.state().contains(gdk::ModifierType::SHIFT_MASK)
                || self.shared.selection_mode.get() != SelectionMode::Multi
            {
                clear_selection();
            }
        }
        if kv == keys::constants::w
            && key.state().contains(gdk::ModifierType::CONTROL_MASK)
        {
            self.write_dialog()
        }
        Inhibit(false)
    }

    fn handle_button(&self, ev: &EventButton) -> Inhibit {
        let (x, y) = ev.position();
        let n = ev.button();
        let typ = ev.event_type();
        match (self.view().path_at_pos(x as i32, y as i32), typ) {
            (Some((Some(_), Some(_), _, _)), gdk::EventType::ButtonPress) if n == 1 => {
                if !ev.state().contains(gdk::ModifierType::SHIFT_MASK)
                    || self.shared.selection_mode.get() != SelectionMode::Multi
                {
                    self.shared.selected.borrow_mut().clear();
                }
            }
            (None, _) | (Some((_, _, _, _)), _) => (),
        }
        Inhibit(false)
    }

    fn write_dialog(&self) {
        let window = toplevel(self.view());
        let selected = self.shared.selected_path.text();
        if &*selected == "" {
            err_modal(&window, "Select a cell before write");
        } else {
            let path = Path::from(ArcStr::from(&*selected));
            // we should already be subscribed, so we're just looking up the dval by path.
            let dv = self
                .shared
                .ctx
                .borrow_mut()
                .user
                .backend
                .subscriber
                .durable_subscribe(path);
            let val = Rc::new(RefCell::new(match dv.last() {
                Event::Unsubscribed => Some(Value::Null),
                Event::Update(v) => Some(v),
            }));
            let d = gtk::Dialog::with_buttons(
                Some("Write Cell"),
                Some(&window),
                gtk::DialogFlags::MODAL,
                &[
                    ("Cancel", gtk::ResponseType::Cancel),
                    ("Write", gtk::ResponseType::Accept),
                ],
            );
            let root = d.content_area();
            let data_lbl = gtk::Label::new(Some("value:"));
            let data = gtk::Entry::new();
            let data_box = gtk::Box::new(gtk::Orientation::Horizontal, 5);
            data_box.add(&data_lbl);
            data_box.add(&data);
            if let Some(v) = &*val.borrow() {
                data.set_text(&format!("{}", v));
            }
            data.connect_changed(clone!(
                @strong val,
                @strong d => move |data| match data.text().parse::<Value>() {
                    Err(e) => {
                        *val.borrow_mut() = None;
                        data.set_icon_from_icon_name(
                            gtk::EntryIconPosition::Secondary,
                            Some("dialog-error")
                        );
                        data.set_icon_tooltip_text(
                            gtk::EntryIconPosition::Secondary,
                            Some(&format!("{}", e))
                        );
                        if let Some(w) = d.widget_for_response(gtk::ResponseType::Accept) {
                            w.set_sensitive(false);
                        }
                    }
                    Ok(v) => {
                        data.set_icon_from_icon_name(
                            gtk::EntryIconPosition::Secondary,
                            None
                        );
                        *val.borrow_mut() = Some(v);
                        if let Some(w) = d.widget_for_response(gtk::ResponseType::Accept) {
                            w.set_sensitive(true);
                        }
                    }
            }));
            data.connect_activate(clone!(@strong d => move |_| {
                if let Some(w) = d.widget_for_response(gtk::ResponseType::Accept) {
                    if w.is_sensitive() {
                        if let Some(w) = w.downcast_ref::<gtk::Button>() {
                            w.clicked();
                        }
                    }
                }
            }));
            root.add(&data_box);
            root.show_all();
            match d.run() {
                gtk::ResponseType::Accept => match &*val.borrow() {
                    None => {
                        err_modal(&window, "Can't parse value, not written");
                    }
                    Some(v) => {
                        dv.write(v.clone());
                    }
                },
                gtk::ResponseType::Cancel | _ => (),
            }
            d.close();
        }
    }

    fn row_of(&self, row: Either<&gtk::TreePath, &gtk::TreeIter>) -> Option<glib::Value> {
        let row_name = match row {
            Either::Right(i) => Some(self.store().value(i, 0)),
            Either::Left(row) => match self.store().iter(&row) {
                None => None,
                Some(i) => Some(self.store().value(&i, 0)),
            },
        };
        row_name.and_then(|v| if v.get::<&str>().is_ok() { Some(v) } else { None })
    }

    fn path_from_selected(&self, row: &str, title: &str) -> Path {
        let path = &self.path;
        if self.vector_mode {
            path.append(row)
        } else if self.shared.show_name_column.get() && title == NAME_COL {
            path.append(row)
        } else {
            path.append(row).append(title)
        }
    }

    fn path_from_treepath(&self, row: &gtk::TreePath, column: &str) -> Option<Path> {
        let row = self.row_of(Either::Left(row));
        row.as_ref()
            .and_then(|r| r.get::<&str>().ok())
            .map(|r| self.path_from_selected(r, column))
    }

    fn cursor_changed(&self) {
        if let (Some(p), Some(c)) = self.view().cursor() {
            if let Some(row) =
                self.row_of(Either::Left(&p)).as_ref().and_then(|r| r.get::<&str>().ok())
            {
                let title = c.title().map(|t| t.to_string()).unwrap_or_else(String::new);
                let path = self.path_from_selected(row, &title);
                self.shared.selected_path.set_label(path.as_ref());
                match self.shared.selection_mode.get() {
                    SelectionMode::None => (),
                    SelectionMode::Single | SelectionMode::Multi => {
                        self.shared
                            .selected
                            .borrow_mut()
                            .entry(String::from(row))
                            .or_insert_with(HashSet::default)
                            .insert(title);
                        self.handle_selection_changed();
                    }
                }
            }
        }
    }

    fn handle_selection_changed(&self) {
        self.visible_changed();
        let v = Value::from(
            self.shared
                .selected
                .borrow()
                .iter()
                .flat_map(|(row, cols)| {
                    cols.iter().map(|c| self.path_from_selected(row, c))
                })
                .map(|p| Value::from(Chars::from(String::from(&*p))))
                .collect::<Vec<_>>(),
        );
        let ev = vm::Event::User(LocalEvent::Event(v));
        let mut on_select = self.shared.on_select.borrow_mut();
        on_select.update(&mut self.shared.ctx.borrow_mut(), &ev);
    }

    pub(super) fn update_subscriptions(&self) {
        if self.destroyed.get() || !self.view().is_visible() {
            return;
        }
        let ncols = if self.vector_mode { 1 } else { self.descriptor.cols.len() };
        let (mut start, mut end) = match self.view().visible_range() {
            Some((s, e)) => (s, e),
            None => {
                if self.descriptor.rows.len() > 0 {
                    let t = self;
                    idle_add_local(
                        clone!(@weak t => @default-return Continue(false), move || {
                            t.update_subscriptions();
                            Continue(false)
                        }),
                    );
                }
                return;
            }
        };
        for _ in 0..50 {
            start.prev();
            end.next();
        }
        // unsubscribe invisible rows
        self.by_id.borrow_mut().retain(|_, v| match self.store().path(&v.row) {
            None => false,
            Some(p) => {
                let visible =
                    (p >= start && p <= end) || (Some(v.col) == self.sort_column.get());
                if !visible {
                    let row_name_v = self.store().value(&v.row, 0);
                    if let Ok(row_name) = row_name_v.get::<&str>() {
                        let mut subscribed = self.subscribed.borrow_mut();
                        if let Some(set) = subscribed.get_mut(row_name) {
                            set.remove(&v.col);
                            if set.is_empty() {
                                subscribed.remove(row_name);
                            }
                        }
                    }
                }
                visible
            }
        });
        let empty =
            BVal { value: Value::from(""), formatted: Pooled::orphan(String::new()) }
                .to_value();
        let maybe_subscribe_col = |store: &ListStore,
                                   row: &TreeIter,
                                   row_name: &str,
                                   id: u32| {
            let mut subscribed = self.subscribed.borrow_mut();
            if !subscribed.get(row_name).map(|s| s.contains(&id)).unwrap_or(false) {
                store.set_value(row, id, &empty);
                subscribed.entry(row_name.into()).or_insert_with(HashSet::new).insert(id);
                let p = self.path.append(row_name);
                let p = if self.vector_mode {
                    p
                } else {
                    p.append(
                        &self.descriptor.cols.get_index((id - 1) as usize).unwrap().0,
                    )
                };
                let s = {
                    let user_r = &self.shared.ctx.borrow_mut().user;
                    let s = user_r.backend.subscriber.durable_subscribe(p);
                    s.updates(
                        UpdatesFlags::BEGIN_WITH_LAST,
                        user_r.backend.updates.clone(),
                    );
                    s
                };
                self.by_id.borrow_mut().insert(
                    s.id(),
                    Subscription { _sub: s, row: row.clone(), col: id as u32 },
                );
            }
        };
        // subscribe to all the visible rows
        while start < end {
            if let Some(row) = self.store().iter(&start) {
                let row_name_v = self.store().value(&row, 0);
                if let Ok(row_name) = row_name_v.get::<&str>() {
                    for col in 0..ncols {
                        maybe_subscribe_col(
                            &self.store(),
                            &row,
                            row_name,
                            (col + 1) as u32,
                        );
                    }
                }
            }
            start.next();
        }
        // subscribe to all rows in the sort column
        if let Some(id) = self.sort_column.get() {
            if let Some(row) = self.store().iter_first() {
                loop {
                    let row_name_v = self.store().value(&row, 0);
                    if let Ok(row_name) = row_name_v.get::<&str>() {
                        maybe_subscribe_col(&self.store(), &row, row_name, id);
                    }
                    if !self.store().iter_next(&row) {
                        break;
                    }
                }
            }
        }
        self.cursor_changed();
    }

    fn disable_sort(&self) -> Option<(SortColumn, SortType)> {
        self.sort_temp_disabled.set(true);
        let col = self.store().sort_column_id();
        self.store().set_unsorted();
        self.sort_temp_disabled.set(false);
        col
    }

    fn enable_sort(&self, col: Option<(SortColumn, SortType)>) {
        self.sort_temp_disabled.set(true);
        if let Some((col, dir)) = col {
            if self.sort_column.get().is_some() {
                self.store().set_sort_column_id(col, dir);
            }
        }
        self.sort_temp_disabled.set(false);
    }

    pub(super) fn view(&self) -> &TreeView {
        &self.view
    }

    fn store(&self) -> &ListStore {
        &self.store
    }

    pub(super) fn start_update_task(&self, mut tx: Option<oneshot::Sender<()>>) {
        let sctx = self.disable_sort();
        let t = self;
        idle_add_local(clone!(@weak t => @default-return Continue(false), move || {
            if t.0.destroyed.get() {
                Continue(false)
            } else {
                for _ in 0..1000 {
                    match t.0.update.borrow_mut().pop() {
                        None => break,
                        Some((id, v)) => if let Some(sub) = t.0.by_id.borrow().get(&id) {
                            let mut formatted = FORMATTED.take();
                            write!(&mut *formatted, "{}", WVal(&v)).unwrap();
                            let bval = BVal {
                                value: v,
                                formatted,
                            }.to_value();
                            t.store().set_value(&sub.row, sub.col, &bval);
                        }
                    }
                }
                if t.0.update.borrow().len() > 0 {
                    Continue(true)
                } else {
                    if let Some(tx) = tx.take() {
                        let _: result::Result<_, _> = tx.send(());
                    }
                    t.enable_sort(sctx);
                    t.visible_changed();
                    Continue(false)
                }
            }
        }));
    }

    fn visible_changed(&self) {
        if self.view().is_visible() {
            let (mut start, end) = match self.view().visible_range() {
                None => return,
                Some((s, e)) => (s, e),
            };
            while start <= end {
                if let Some(i) = self.store().iter(&start) {
                    self.store().row_changed(&start, &i);
                }
                start.next();
            }
        }
    }

    pub(super) fn update(
        &self,
        _ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        match event {
            vm::Event::User(_) | vm::Event::Variable(_, _, _) | vm::Event::Rpc(_, _) => {
                ()
            }
            vm::Event::Netidx(id, value) => {
                self.update.borrow_mut().insert(*id, value.clone());
                if self.update.borrow().len() == 1 {
                    let (tx, rx) = oneshot::channel();
                    waits.push(rx);
                    self.start_update_task(Some(tx));
                }
            }
        }
    }
}
