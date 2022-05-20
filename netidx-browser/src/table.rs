use super::{
    util::{err_modal, toplevel},
    BSCtx, BSCtxRef, BSNode, WVal,
};
use crate::bscript::LocalEvent;
use arcstr::ArcStr;
use futures::channel::oneshot;
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use gdk::{keys, EventButton, EventKey, RGBA};
use gio::prelude::*;
use glib::{
    self, clone, idle_add_local, signal::Inhibit, source::Continue,
    value::ValueTypeMismatchOrNoneError,
};
use gtk::{
    prelude::*, Adjustment, CellRenderer, CellRendererText, Label, ListStore,
    ScrolledWindow, SelectionMode, SortColumn, SortType, StateFlags, StyleContext,
    TreeIter, TreeModel, TreePath, TreeView, TreeViewColumn, TreeViewColumnSizing,
    Widget as GtkWidget,
};
use indexmap::{IndexMap, IndexSet};
use netidx::{
    chars::Chars,
    path::Path,
    resolver_client,
    subscriber::{Dval, Event, SubId, UpdatesFlags, Value},
    utils::Either,
};
use netidx_bscript::vm;
use netidx_protocols::view;
use regex::RegexSet;
use std::{
    cell::{Cell, RefCell},
    cmp::{Ordering, PartialEq},
    collections::{HashMap, HashSet},
    rc::{Rc, Weak},
    result::{self, Result},
    sync::Arc,
};

struct Subscription {
    _sub: Dval,
    row: TreeIter,
    col: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SortSpec {
    None,
    Disabled,
    Ascending(String),
    Descending(String),
}

impl SortSpec {
    fn new(v: Value) -> Result<Self, String> {
        match v {
            Value::Null => Ok(SortSpec::None),
            Value::False => Ok(SortSpec::Disabled),
            Value::String(col) => Ok(SortSpec::Descending(String::from(&*col))),
            Value::Array(a) if a.len() == 2 => {
                let column =
                    a[0].clone().cast_to::<String>().map_err(|e| format!("{}", e))?;
                match a[1]
                    .clone()
                    .cast_to::<String>()
                    .map_err(|e| format!("{}", e))?
                    .as_str()
                {
                    "ascending" => Ok(SortSpec::Ascending(column)),
                    "descending" => Ok(SortSpec::Descending(column)),
                    _ => Err("invalid mode".into()),
                }
            }
            _ => Err("expected null, false, col, or a pair of [column, mode]".into()),
        }
    }
}

#[derive(Debug, Clone)]
enum Filter {
    All,
    Auto,
    None,
    Include(IndexSet<String, FxBuildHasher>),
    Exclude(IndexSet<String, FxBuildHasher>),
    IncludeMatch(IndexSet<String, FxBuildHasher>, RegexSet),
    ExcludeMatch(IndexSet<String, FxBuildHasher>, RegexSet),
    IncludeRange(Option<usize>, Option<usize>),
    ExcludeRange(Option<usize>, Option<usize>),
}

impl PartialEq for Filter {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Filter::All, Filter::All)
            | (Filter::Auto, Filter::Auto)
            | (Filter::None, Filter::None) => true,
            (Filter::Include(s0), Filter::Include(s1)) => s0 == s1,
            (Filter::Exclude(s0), Filter::Exclude(s1)) => s0 == s1,
            (Filter::IncludeMatch(s0, _), Filter::IncludeMatch(s1, _)) => s0 == s1,
            (Filter::ExcludeMatch(s0, _), Filter::ExcludeMatch(s1, _)) => s0 == s1,
            (Filter::IncludeRange(s0, e0), Filter::IncludeRange(s1, e1)) => {
                s0 == s1 && e0 == e1
            }
            (Filter::ExcludeRange(s0, e0), Filter::ExcludeRange(s1, e1)) => {
                s0 == s1 && e0 == e1
            }
            (Filter::All, _)
            | (_, Filter::All)
            | (Filter::Auto, _)
            | (_, Filter::Auto)
            | (Filter::None, _)
            | (_, Filter::None)
            | (Filter::Include(_), _)
            | (_, Filter::Include(_))
            | (Filter::Exclude(_), _)
            | (_, Filter::Exclude(_))
            | (Filter::IncludeMatch(_, _), _)
            | (_, Filter::IncludeMatch(_, _))
            | (Filter::IncludeRange(_, _), _)
            | (_, Filter::IncludeRange(_, _))
            | (Filter::ExcludeRange(_, _), _)
            | (_, Filter::ExcludeRange(_, _)) => false,
        }
    }
}

impl Filter {
    fn new(v: Value) -> Result<Self, String> {
        match v {
            Value::Null => Ok(Filter::Auto),
            Value::True => Ok(Filter::All),
            Value::False => Ok(Filter::None),
            Value::Array(a) if a.len() == 2 => {
                let mode =
                    a[0].clone().cast_to::<Chars>().map_err(|e| format!("{}", e))?;
                match &*mode {
                    "include" | "exclude" | "include_match" | "exclude_match" => {
                        let i = a[1]
                            .clone()
                            .flatten()
                            .map(|v| v.cast_to::<String>().map_err(|e| format!("{}", e)));
                        if &*mode == "include" {
                            Ok(Filter::Include(i.collect::<Result<IndexSet<_, _>, _>>()?))
                        } else if &*mode == "exclude" {
                            Ok(Filter::Exclude(i.collect::<Result<IndexSet<_, _>, _>>()?))
                        } else if &*mode == "include_match" {
                            let set = i.collect::<Result<IndexSet<_, _>, _>>()?;
                            let matcher =
                                RegexSet::new(&set).map_err(|e| format!("{}", e))?;
                            Ok(Filter::IncludeMatch(set, matcher))
                        } else if &*mode == "exclude_match" {
                            let set = i.collect::<Result<IndexSet<_, _>, _>>()?;
                            let matcher =
                                RegexSet::new(&set).map_err(|e| format!("{}", e))?;
                            Ok(Filter::ExcludeMatch(set, matcher))
                        } else {
                            unreachable!()
                        }
                    }
                    "keep" | "drop" => match &a[1] {
                        Value::Array(a) if a.len() == 2 => {
                            let start = match &a[0] {
                                Value::String(v) if &**v == "start" => None,
                                v => match v.clone().cast_to::<u64>() {
                                    Err(_) => {
                                        return Err(
                                            "expected start or a positive integer".into(),
                                        )
                                    }
                                    Ok(i) => Some(i as usize),
                                },
                            };
                            let end = match &a[1] {
                                Value::String(v) if &**v == "end" => None,
                                v => match v.clone().cast_to::<u64>() {
                                    Err(_) => {
                                        return Err(
                                            "expected end or a positive integer".into()
                                        )
                                    }
                                    Ok(i) => Some(i as usize),
                                },
                            };
                            if &*mode == "keep" {
                                Ok(Filter::IncludeRange(start, end))
                            } else if &*mode == "drop" {
                                Ok(Filter::ExcludeRange(start, end))
                            } else {
                                unreachable!()
                            }
                        }
                        _ => Err("keep/drop expect 2 arguments".into()),
                    },
                    _ => Err("invalid filter mode".into()),
                }
            }
            _ => Err("expected null, true, false, or a pair".into()),
        }
    }

    fn is_match(&self, i: usize, s: &str) -> bool {
        match self {
            Filter::All | Filter::Auto => true,
            Filter::None => false,
            Filter::Include(set) => set.contains(s),
            Filter::Exclude(set) => !set.contains(s),
            Filter::IncludeMatch(_, set) => set.is_match(s),
            Filter::ExcludeMatch(_, set) => !set.is_match(s),
            Filter::IncludeRange(start, end) => {
                let start_ok = start.map(|start| i >= start).unwrap_or(true);
                let end_ok = end.map(|end| i < end).unwrap_or(true);
                start_ok && end_ok
            }
            Filter::ExcludeRange(start, end) => {
                let start_ok = start.map(|start| i < start).unwrap_or(false);
                let end_ok = end.map(|end| i >= end).unwrap_or(false);
                start_ok || end_ok
            }
        }
    }

    fn sort_index(&self, s: &str) -> Option<usize> {
        match self {
            Filter::Include(set) => set.get_index_of(s),
            Filter::All
            | Filter::Auto
            | Filter::None
            | Filter::Exclude(_)
            | Filter::IncludeMatch(_, _)
            | Filter::ExcludeMatch(_, _)
            | Filter::IncludeRange(_, _)
            | Filter::ExcludeRange(_, _) => None,
        }
    }
}

enum TableState {
    Empty,
    Resolving(Path),
    Raeified(RaeifiedTable),
    Refresh { descriptor: Option<resolver_client::Table>, path: Option<Path> },
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
    let v0_r = v0_v.get::<&str>();
    let v1_r = v1_v.get::<&str>();
    match (v0_r, v1_r) {
        (Err(_), Err(_)) => Ordering::Equal,
        (Err(_), _) => Ordering::Greater,
        (_, Err(_)) => Ordering::Less,
        (Ok(v0), Ok(v1)) => match (v0.parse::<f64>(), v1.parse::<f64>()) {
            (Ok(v0f), Ok(v1f)) => v0f.partial_cmp(&v1f).unwrap_or(Ordering::Equal),
            (_, _) => v0.cmp(v1),
        },
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

fn apply_filters(
    original_descriptor: &resolver_client::Table,
    filter_errors: &mut Vec<String>,
    row_filter: Result<Filter, String>,
    column_filter: Result<Filter, String>,
) -> resolver_client::Table {
    let mut descriptor = original_descriptor.clone();
    let row_filter = row_filter.unwrap_or_else(|e| {
        filter_errors.push(format!("invalid row filter: {}", e));
        Filter::All
    });
    let column_filter = column_filter.unwrap_or_else(|e| {
        filter_errors.push(format!("invalid column filter {}", e));
        Filter::Auto
    });
    descriptor.cols.sort_by_key(|(p, _)| p.clone());
    descriptor.rows.sort();
    {
        let mut i = 0;
        descriptor.rows.retain(|row| match Path::basename(&row) {
            None => true,
            Some(row) => {
                let res = row_filter.is_match(i, row);
                i += 1;
                res
            }
        });
    }
    match &column_filter {
        Filter::Auto => {
            let half = descriptor.rows.len() as f32 / 2.;
            descriptor.cols.retain(|(_, i)| i.0 as f32 >= half)
        }
        filter => {
            let mut i = 0;
            descriptor.cols.retain(|col| match Path::basename(&col.0) {
                None => true,
                Some(col) => {
                    let res = filter.is_match(i, &col);
                    i += 1;
                    res
                }
            })
        }
    }
    descriptor.cols.sort_by(|v0, v1| {
        let v0 = Path::basename(&v0.0);
        let v1 = Path::basename(&v1.0);
        let i0 = v0.and_then(|v| column_filter.sort_index(v));
        let i1 = v1.and_then(|v| column_filter.sort_index(v));
        match (i0, i1) {
            (Some(i0), Some(i1)) => i0.cmp(&i1),
            (_, _) => v0.cmp(&v1),
        }
    });
    descriptor.rows.sort_by(|v0, v1| {
        let v0 = Path::basename(&v0);
        let v1 = Path::basename(&v1);
        let i0 = v0.and_then(|v| column_filter.sort_index(v));
        let i1 = v1.and_then(|v| column_filter.sort_index(v));
        match (i0, i1) {
            (Some(i0), Some(i1)) => i0.cmp(&i1),
            (_, _) => v0.cmp(&v1),
        }
    });
    descriptor
}

struct RaeifiedTableInner {
    by_id: RefCell<HashMap<SubId, Subscription>>,
    ctx: BSCtx,
    original_descriptor: resolver_client::Table,
    descriptor: resolver_client::Table,
    destroyed: Cell<bool>,
    on_activate: Rc<RefCell<BSNode>>,
    on_edit: Rc<RefCell<BSNode>>,
    on_select: Rc<RefCell<BSNode>>,
    on_header_click: Rc<RefCell<BSNode>>,
    saved_column_widths: Rc<RefCell<FxHashMap<String, i32>>>,
    path: Path,
    root: ScrolledWindow,
    selected_path: Label,
    selected: RefCell<FxHashMap<String, FxHashSet<TreeViewColumn>>>,
    sort_column: Cell<Option<u32>>,
    sort_temp_disabled: Cell<bool>,
    store: ListStore,
    style: StyleContext,
    subscribed: RefCell<HashMap<String, HashSet<u32>>>,
    update: RefCell<IndexMap<SubId, Value, FxBuildHasher>>,
    vector_mode: bool,
    multi_select: bool,
    view: TreeView,
}

#[derive(Clone)]
struct RaeifiedTable(Rc<RaeifiedTableInner>);

struct RaeifiedTableWeak(Weak<RaeifiedTableInner>);

impl RaeifiedTable {
    fn add_columns(
        &self,
        ncols: usize,
        vector_mode: bool,
        sorting_disabled: bool,
        show_name_column: bool,
        column_editable: &Filter,
    ) {
        let t = self;
        if show_name_column {
            t.view().append_column(&{
                let column = TreeViewColumn::new();
                let cell = CellRendererText::new();
                column.pack_start(&cell, true);
                column.set_title("name");
                column.add_attribute(&cell, "text", 0);
                if sorting_disabled {
                    column.set_clickable(true);
                } else {
                    column.set_sort_column_id(0);
                }
                column.set_sizing(TreeViewColumnSizing::Fixed);
                column.set_resizable(true);
                let saved_column_widths = t.0.saved_column_widths.clone();
                if let Some(w) = saved_column_widths.borrow().get("name") {
                    column.set_fixed_width(*w);
                }
                column.connect_width_notify(clone!(@strong saved_column_widths => move |c| {
                    *saved_column_widths.borrow_mut().entry("name".into()).or_insert(1) = c.width();
                }));
                column
            });
        }
        for col in 0..ncols {
            let id = (col + 1) as i32;
            let column = TreeViewColumn::new();
            let cell = CellRendererText::new();
            let name = if vector_mode {
                Path::from("value")
            } else {
                t.0.descriptor.cols[col].0.clone()
            };
            column.pack_start(&cell, true);
            if column_editable.is_match(col, &*name) {
                cell.set_editable(true);
            }
            let f = Box::new(clone!(@weak t =>
                move |c: &TreeViewColumn,
                      cr: &CellRenderer,
                      _: &TreeModel,
                      i: &TreeIter| t.render_cell(id, c, cr, i)));
            TreeViewColumnExt::set_cell_data_func(&column, &cell, Some(f));
            cell.connect_edited(clone!(@weak t => move |_, _, v| {
                let ev = LocalEvent::Event(Value::String(Chars::from(String::from(v))));
                t.0.on_edit.borrow_mut().update(
                    &mut t.0.ctx.borrow_mut(),
                    &vm::Event::User(ev)
                );
            }));
            column.connect_clicked(clone!(@weak t, @strong name => move |_| {
                let v = Value::String(Chars::from(String::from(&*name)));
                let ev = LocalEvent::Event(v);
                t.0.on_header_click.borrow_mut().update(
                    &mut t.0.ctx.borrow_mut(),
                    &vm::Event::User(ev)
                );
            }));
            column.set_title(&name);
            if sorting_disabled {
                column.set_clickable(true);
            } else {
                t.store()
                    .set_sort_func(SortColumn::Index(id as u32), move |m, r0, r1| {
                        compare_row(id, m, r0, r1)
                    });
                column.set_sort_column_id(id);
            }
            column.set_sizing(TreeViewColumnSizing::Fixed);
            column.set_resizable(true);
            let saved_column_widths = t.0.saved_column_widths.clone();
            if let Some(w) = saved_column_widths.borrow().get(&*name) {
                column.set_fixed_width(*w);
            }
            column.connect_width_notify(clone!(
                @strong saved_column_widths, @strong name => move |c| {
                    let mut saved = saved_column_widths.borrow_mut();
                    *saved.entry((&*name).into()).or_insert(1) = c.width();
            }));
            t.view().append_column(&column);
        }
    }

    fn new(
        ctx: BSCtx,
        root: ScrolledWindow,
        path: Path,
        saved_column_widths: Rc<RefCell<FxHashMap<String, i32>>>,
        sort_mode: Result<SortSpec, String>,
        column_filter: Result<Filter, String>,
        multi_select: bool,
        show_name_column: bool,
        row_filter: Result<Filter, String>,
        column_editable: Result<Filter, String>,
        on_select: Rc<RefCell<BSNode>>,
        on_activate: Rc<RefCell<BSNode>>,
        on_edit: Rc<RefCell<BSNode>>,
        on_header_click: Rc<RefCell<BSNode>>,
        original_descriptor: resolver_client::Table,
        selected_path: Label,
    ) -> RaeifiedTable {
        let mut filter_errors = Vec::new();
        let column_editable = column_editable.unwrap_or_else(|e| {
            filter_errors.push(format!("invalid column editable {}", e));
            Filter::None
        });
        let sort_mode = sort_mode.unwrap_or_else(|e| {
            filter_errors.push(format!("invalid sort mode {}", e));
            SortSpec::None
        });
        let descriptor = apply_filters(
            &original_descriptor,
            &mut filter_errors,
            row_filter,
            column_filter,
        );
        let view = TreeView::new();
        root.add(&view);
        view.selection().set_mode(SelectionMode::None);
        let vector_mode = descriptor.cols.len() == 0;
        let column_types = if vector_mode {
            vec![String::static_type(); 2]
        } else {
            (0..descriptor.cols.len() + 1)
                .into_iter()
                .map(|_| String::static_type())
                .collect::<Vec<_>>()
        };
        let store = ListStore::new(&column_types);
        for row in descriptor.rows.iter() {
            let row_name = Path::basename(row).unwrap_or("").to_value();
            let row = store.append();
            store.set_value(&row, 0, &row_name.to_value());
        }
        let style = view.style_context();
        let ncols = if vector_mode { 1 } else { descriptor.cols.len() };
        let t = RaeifiedTable(Rc::new(RaeifiedTableInner {
            path,
            ctx,
            root,
            view,
            selected_path,
            store,
            original_descriptor,
            descriptor,
            vector_mode,
            multi_select,
            style,
            on_select,
            on_activate,
            on_edit,
            on_header_click,
            saved_column_widths,
            by_id: RefCell::new(HashMap::new()),
            subscribed: RefCell::new(HashMap::new()),
            selected: RefCell::new(HashMap::default()),
            sort_column: Cell::new(None),
            sort_temp_disabled: Cell::new(false),
            update: RefCell::new(IndexMap::with_hasher(FxBuildHasher::default())),
            destroyed: Cell::new(false),
        }));
        t.view().connect_destroy(clone!(@weak t => move |_| t.0.destroyed.set(true)));
        if t.0.saved_column_widths.borrow().len() > 1000 {
            let cols =
                t.0.descriptor.cols.iter().map(|c| c.0.clone()).collect::<FxHashSet<_>>();
            t.0.saved_column_widths
                .borrow_mut()
                .retain(|name, _| cols.contains(name.as_str()));
        }
        let sorting_disabled = match &sort_mode {
            SortSpec::Disabled => true,
            SortSpec::Ascending(_) | SortSpec::Descending(_) | SortSpec::None => false,
        };
        t.add_columns(
            ncols,
            vector_mode,
            sorting_disabled,
            show_name_column,
            &column_editable,
        );
        t.store().connect_sort_column_changed(
            clone!(@weak t => move |_| t.handle_sort_column_changed()),
        );
        t.view().set_fixed_height_mode(true);
        t.view().set_model(Some(t.store()));
        t.view().connect_key_press_event(clone!(
            @weak t => @default-return Inhibit(false), move |_, k| t.handle_key(k)));
        t.view().connect_button_press_event(clone!(
            @weak t => @default-return Inhibit(false), move |_, b| t.handle_button(b)));
        t.view().connect_row_activated(
            clone!(@weak t => move |_, p, _| t.handle_row_activated(p)),
        );
        t.view().connect_cursor_changed(clone!(@weak t => move |_| t.cursor_changed()));
        t.0.root.vadjustment().connect_value_changed(clone!(@weak t => move |_| {
            idle_add_local(clone!(@weak t => @default-return Continue(false), move || {
                t.update_subscriptions();
                Continue(false)
            }));
        }));
        match &sort_mode {
            SortSpec::None => (),
            SortSpec::Disabled => (),
            SortSpec::Ascending(col) | SortSpec::Descending(col) => {
                let col = Path::from(col.clone());
                let idx =
                    t.0.descriptor.cols.iter().enumerate().find_map(|(i, (c, _))| {
                        if c == &col {
                            Some(i + 1)
                        } else {
                            None
                        }
                    });
                let dir = match &sort_mode {
                    SortSpec::Ascending(_) => gtk::SortType::Ascending,
                    SortSpec::Descending(_) => gtk::SortType::Descending,
                    SortSpec::Disabled | SortSpec::None => unreachable!(),
                };
                if let Some(i) = idx {
                    t.store().set_sort_column_id(gtk::SortColumn::Index(i as u32), dir)
                }
            }
        }
        t.0.root.show_all();
        t
    }

    fn handle_row_activated(&self, p: &TreePath) {
        if let Some(iter) = self.store().iter(&p) {
            if let Ok(row_name) = self.store().value(&iter, 0).get::<&str>() {
                let path = String::from(&*self.0.path.append(row_name));
                let e = LocalEvent::Event(Value::String(Chars::from(path)));
                self.0
                    .on_activate
                    .borrow_mut()
                    .update(&mut self.0.ctx.borrow_mut(), &vm::Event::User(e));
            }
        }
    }

    fn handle_sort_column_changed(&self) {
        if self.0.sort_temp_disabled.get() {
            return;
        }
        match get_sort_column(self.store()) {
            Some(col) => {
                self.0.sort_column.set(Some(col));
            }
            None => {
                self.0.sort_column.set(None);
                self.store().set_unsorted();
            }
        }
        let t = self;
        idle_add_local(clone!(@weak t => @default-return Continue(false), move || {
            t.update_subscriptions();
            Continue(false)
        }));
    }

    fn render_cell(&self, id: i32, c: &TreeViewColumn, cr: &CellRenderer, i: &TreeIter) {
        let cr = cr.clone().downcast::<CellRendererText>().unwrap();
        cr.set_text(match self.store().value(i, id).get::<&str>() {
            Ok(v) => Some(v),
            Err(ValueTypeMismatchOrNoneError::UnexpectedNone) => None,
            _ => return,
        });
        let sel = self.0.selected.borrow();
        match self.row_of(Either::Right(i)).as_ref().map(|r| r.get::<&str>().unwrap()) {
            Some(r) if sel.get(r).map(|t| t.contains(c)).unwrap_or(false) => {
                let st = StateFlags::SELECTED;
                let fg = self.0.style.color(st);
                let bg = StyleContextExt::style_property_for_state(
                    &self.0.style,
                    "background-color",
                    st,
                )
                .get::<RGBA>()
                .unwrap();
                cr.set_cell_background_rgba(Some(&bg));
                cr.set_foreground_rgba(Some(&fg));
            }
            Some(_) | None => {
                cr.set_cell_background(None);
                cr.set_foreground(None);
            }
        }
    }

    fn handle_key(&self, key: &EventKey) -> Inhibit {
        let clear_selection = || {
            let n = {
                let mut paths = self.0.selected.borrow_mut();
                let n = paths.len();
                paths.clear();
                n
            };
            self.0.selected_path.set_label("");
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
                || !self.0.multi_select
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
                    || !self.0.multi_select
                {
                    self.0.selected.borrow_mut().clear();
                }
            }
            (None, _) | (Some((_, _, _, _)), _) => (),
        }
        Inhibit(false)
    }

    fn write_dialog(&self) {
        let window = toplevel(self.view());
        let selected = self.0.selected_path.text();
        if &*selected == "" {
            err_modal(&window, "Select a cell before write");
        } else {
            let path = Path::from(ArcStr::from(&*selected));
            // we should already be subscribed, so we're just looking up the dval by path.
            let dv =
                self.0.ctx.borrow_mut().user.backend.subscriber.durable_subscribe(path);
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
        match row_name {
            None => None,
            Some(row_name) => match row_name.get::<&str>() {
                Err(_) => None,
                Ok(_) => Some(row_name),
            },
        }
    }

    fn path_from_selected(&self, row: &str, col: &TreeViewColumn) -> Path {
        if self.0.vector_mode {
            self.0.path.append(row)
        } else {
            match col.title() {
                None => self.0.path.append(row),
                Some(col) => self.0.path.append(row).append(col.as_str()),
            }
        }
    }

    fn cursor_changed(&self) {
        if let (Some(p), Some(c)) = self.view().cursor() {
            if let Some(row) =
                self.row_of(Either::Left(&p)).as_ref().map(|r| r.get::<&str>().unwrap())
            {
                let path = self.path_from_selected(row, &c);
                self.0.selected_path.set_label(path.as_ref());
                self.0
                    .selected
                    .borrow_mut()
                    .entry(String::from(row))
                    .or_insert_with(HashSet::default)
                    .insert(c);
                self.handle_selection_changed();
            }
        }
    }

    fn handle_selection_changed(&self) {
        self.visible_changed();
        let v = Value::from(
            self.0
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
        self.0.on_select.borrow_mut().update(&mut self.0.ctx.borrow_mut(), &ev);
    }

    pub(super) fn update_subscriptions(&self) {
        if self.0.destroyed.get() {
            return;
        }
        let ncols = if self.0.vector_mode { 1 } else { self.0.descriptor.cols.len() };
        let (mut start, mut end) = match self.view().visible_range() {
            Some((s, e)) => (s, e),
            None => {
                if self.0.descriptor.rows.len() > 0 {
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
        self.0.by_id.borrow_mut().retain(|_, v| match self.store().path(&v.row) {
            None => false,
            Some(p) => {
                let visible =
                    (p >= start && p <= end) || (Some(v.col) == self.0.sort_column.get());
                if !visible {
                    let row_name_v = self.store().value(&v.row, 0);
                    if let Ok(row_name) = row_name_v.get::<&str>() {
                        let mut subscribed = self.0.subscribed.borrow_mut();
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
        let maybe_subscribe_col = |row: &TreeIter, row_name: &str, id: u32| {
            let mut subscribed = self.0.subscribed.borrow_mut();
            if !subscribed.get(row_name).map(|s| s.contains(&id)).unwrap_or(false) {
                subscribed.entry(row_name.into()).or_insert_with(HashSet::new).insert(id);
                let p = self.0.path.append(row_name);
                let p = if self.0.vector_mode {
                    p
                } else {
                    p.append(&self.0.descriptor.cols[(id - 1) as usize].0)
                };
                let user_r = &self.0.ctx.borrow_mut().user;
                let s = user_r.backend.subscriber.durable_subscribe(p);
                s.updates(UpdatesFlags::BEGIN_WITH_LAST, user_r.backend.updates.clone());
                self.0.by_id.borrow_mut().insert(
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
                        maybe_subscribe_col(&row, row_name, (col + 1) as u32);
                    }
                }
            }
            start.next();
        }
        // subscribe to all rows in the sort column
        if let Some(id) = self.0.sort_column.get() {
            if let Some(row) = self.store().iter_first() {
                loop {
                    let row_name_v = self.store().value(&row, 0);
                    if let Ok(row_name) = row_name_v.get::<&str>() {
                        maybe_subscribe_col(&row, row_name, id);
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
        self.0.sort_temp_disabled.set(true);
        let col = self.store().sort_column_id();
        self.store().set_unsorted();
        self.0.sort_temp_disabled.set(false);
        col
    }

    fn enable_sort(&self, col: Option<(SortColumn, SortType)>) {
        self.0.sort_temp_disabled.set(true);
        if let Some((col, dir)) = col {
            if self.0.sort_column.get().is_some() {
                self.store().set_sort_column_id(col, dir);
            }
        }
        self.0.sort_temp_disabled.set(false);
    }

    fn view(&self) -> &TreeView {
        &self.0.view
    }

    fn store(&self) -> &ListStore {
        &self.0.store
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
                            let s = &format!("{}", WVal(&v)).to_value();
                            t.store().set_value(&sub.row, sub.col, s);
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
                self.0.update.borrow_mut().insert(*id, value.clone());
                if self.0.update.borrow().len() == 1 {
                    let (tx, rx) = oneshot::channel();
                    waits.push(rx);
                    self.start_update_task(Some(tx));
                }
            }
        }
    }
}

pub(super) struct Table {
    ctx: BSCtx,
    state: RefCell<TableState>,
    root: ScrolledWindow,
    selected_path: Label,
    saved_column_widths: Rc<RefCell<FxHashMap<String, i32>>>,
    path_expr: BSNode,
    path: RefCell<Option<Path>>,
    sort_mode_expr: BSNode,
    sort_mode: RefCell<Result<SortSpec, String>>,
    column_filter_expr: BSNode,
    column_filter: RefCell<Result<Filter, String>>,
    row_filter_expr: BSNode,
    row_filter: RefCell<Result<Filter, String>>,
    column_editable_expr: BSNode,
    column_editable: RefCell<Result<Filter, String>>,
    multi_select_expr: BSNode,
    multi_select: Cell<bool>,
    show_row_name_expr: BSNode,
    show_row_name: Cell<bool>,
    on_select: Rc<RefCell<BSNode>>,
    on_activate: Rc<RefCell<BSNode>>,
    on_edit: Rc<RefCell<BSNode>>,
    on_header_click: Rc<RefCell<BSNode>>,
}

impl Table {
    pub(super) fn new(
        ctx: BSCtx,
        spec: view::Table,
        scope: Path,
        selected_path: Label,
    ) -> Table {
        let path_expr = BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.path);
        let path = RefCell::new(path_expr.current().and_then(|v| match v {
            Value::String(path) => Some(Path::from(ArcStr::from(&*path))),
            _ => None,
        }));
        let sort_mode_expr =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.sort_mode);
        let sort_mode =
            RefCell::new(SortSpec::new(sort_mode_expr.current().unwrap_or(Value::Null)));
        let column_filter_expr =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.column_filter);
        let column_filter = RefCell::new(Filter::new(
            column_filter_expr.current().unwrap_or(Value::Null),
        ));
        let row_filter_expr =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.row_filter);
        let row_filter =
            RefCell::new(Filter::new(row_filter_expr.current().unwrap_or(Value::Null)));
        let column_editable_expr =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.column_editable);
        let column_editable = RefCell::new(Filter::new(
            column_editable_expr.current().unwrap_or(Value::Null),
        ));
        let multi_select_expr =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.multi_select);
        let multi_select =
            Cell::new(match multi_select_expr.current().unwrap_or(Value::False) {
                Value::True => true,
                _ => false,
            });
        let show_row_name_expr =
            BSNode::compile(&mut ctx.borrow_mut(), scope.clone(), spec.show_row_name);
        let show_row_name =
            Cell::new(match show_row_name_expr.current().unwrap_or(Value::True) {
                Value::True => true,
                _ => false,
            });
        let state = RefCell::new(match &*path.borrow() {
            None => TableState::Empty,
            Some(path) => {
                ctx.borrow().user.backend.resolve_table(path.clone());
                TableState::Resolving(path.clone())
            }
        });
        let on_select = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.on_select,
        )));
        let on_activate = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.on_activate,
        )));
        let on_edit = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.on_edit,
        )));
        let on_header_click = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            scope,
            spec.on_header_click,
        )));
        Table {
            ctx,
            state,
            root: ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>),
            selected_path,
            path_expr,
            path,
            saved_column_widths: Rc::new(RefCell::new(HashMap::default())),
            on_select,
            on_activate,
            on_edit,
            on_header_click,
            sort_mode_expr,
            sort_mode,
            column_filter_expr,
            column_filter,
            row_filter_expr,
            row_filter,
            column_editable_expr,
            column_editable,
            multi_select_expr,
            multi_select,
            show_row_name_expr,
            show_row_name,
        }
    }

    fn refresh(&self, ctx: BSCtxRef) {
        let state = &mut *self.state.borrow_mut();
        let path = &*self.path.borrow();
        match state {
            TableState::Resolving(rpath) if path.as_ref() == Some(rpath) => (),
            TableState::Refresh { path: ref p, .. } if p.as_ref() == path.as_ref() => (),
            TableState::Raeified(table) if Some(&table.0.path) == path.as_ref() => {
                let descriptor = Some(table.0.original_descriptor.clone());
                let path = Some(path.clone());
                match path {
                    None => {
                        *state = TableState::Empty;
                    }
                    Some(path) => {
                        *state = TableState::Refresh { descriptor, path };
                    }
                }
            }
            TableState::Resolving(_)
            | TableState::Raeified(_)
            | TableState::Refresh { .. }
            | TableState::Empty => match path {
                None => {
                    *state = TableState::Empty;
                }
                Some(path) => {
                    ctx.user.backend.resolve_table(path.clone());
                    *state = TableState::Resolving(path.clone());
                }
            },
        }
        if let Some(c) = self.root.child() {
            self.root.remove(&c);
        }
    }

    pub(super) fn root(&self) -> &GtkWidget {
        self.root.upcast_ref()
    }

    fn clear_selection(&self, ctx: BSCtxRef) {
        let v = Value::Array(Arc::from([]));
        let ev = vm::Event::User(LocalEvent::Event(v));
        self.on_select.borrow_mut().update(ctx, &ev);
    }

    fn raeify(
        &self,
        ctx: BSCtxRef,
        state: &mut TableState,
        path: Path,
        descriptor: resolver_client::Table,
    ) {
        let table = RaeifiedTable::new(
            self.ctx.clone(),
            self.root.clone(),
            path,
            self.saved_column_widths.clone(),
            self.sort_mode.borrow().clone(),
            self.column_filter.borrow().clone(),
            self.multi_select.get(),
            self.show_row_name.get(),
            self.row_filter.borrow().clone(),
            self.column_editable.borrow().clone(),
            self.on_select.clone(),
            self.on_activate.clone(),
            self.on_edit.clone(),
            self.on_header_click.clone(),
            descriptor.clone(),
            self.selected_path.clone(),
        );
        table.update_subscriptions();
        self.clear_selection(ctx);
        *state = TableState::Raeified(table);
    }

    pub(super) fn update(
        &mut self,
        ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        if let Some(path) = self.path_expr.update(ctx, event) {
            let new_path = match path {
                Value::String(p) => Some(Path::from(ArcStr::from(&*p))),
                _ => None,
            };
            if &*self.path.borrow() != &new_path {
                *self.path.borrow_mut() = new_path;
                self.refresh(ctx);
            }
        }
        if let Some(col) = self.sort_mode_expr.update(ctx, event) {
            let new = SortSpec::new(col);
            if &*self.sort_mode.borrow() != &new {
                *self.sort_mode.borrow_mut() = new;
                self.refresh(ctx);
            }
        }
        if let Some(mode) = self.column_filter_expr.update(ctx, event) {
            let new_filter = Filter::new(mode);
            if &*self.column_filter.borrow() != &new_filter {
                *self.column_filter.borrow_mut() = new_filter;
                self.refresh(ctx);
            }
        }
        if let Some(row_filter) = self.row_filter_expr.update(ctx, event) {
            let new_filter = Filter::new(row_filter);
            if &*self.row_filter.borrow() != &new_filter {
                *self.row_filter.borrow_mut() = new_filter;
                self.refresh(ctx);
            }
        }
        if let Some(v) = self.column_editable_expr.update(ctx, event) {
            let new_ed = Filter::new(v);
            if &*self.column_editable.borrow() != &new_ed {
                *self.column_editable.borrow_mut() = new_ed;
                self.refresh(ctx);
            }
        }
        if let Some(v) = self.multi_select_expr.update(ctx, event) {
            let new = match v {
                Value::True => true,
                _ => false,
            };
            if self.multi_select.get() != new {
                self.multi_select.set(new);
                self.refresh(ctx);
            }
        }
        if let Some(v) = self.show_row_name_expr.update(ctx, event) {
            let new = match v {
                Value::False => false,
                _ => true,
            };
            if self.show_row_name.get() != new {
                self.show_row_name.set(new);
                self.refresh(ctx);
            }
        }
        self.on_activate.borrow_mut().update(ctx, event);
        self.on_select.borrow_mut().update(ctx, event);
        self.on_edit.borrow_mut().update(ctx, event);
        self.on_header_click.borrow_mut().update(ctx, event);
        match &*self.state.borrow() {
            TableState::Empty | TableState::Resolving(_) | TableState::Refresh { .. } => {
                ()
            }
            TableState::Raeified(table) => table.update(ctx, waits, event),
        }
        let state = &mut *self.state.borrow_mut();
        match state {
            TableState::Empty | TableState::Raeified(_) => (),
            TableState::Refresh { descriptor, path } => {
                match (path.take(), descriptor.take()) {
                    (Some(path), Some(descriptor)) => {
                        self.raeify(ctx, state, path, descriptor)
                    }
                    (_, _) => unreachable!(),
                }
            }
            TableState::Resolving(rpath) => match event {
                vm::Event::Netidx(_, _)
                | vm::Event::Rpc(_, _)
                | vm::Event::Variable(_, _, _)
                | vm::Event::User(LocalEvent::Event(_)) => (),
                vm::Event::User(LocalEvent::TableResolved(path, descriptor)) => {
                    if path == rpath {
                        self.raeify(ctx, state, path.clone(), descriptor.clone())
                    }
                }
            },
        }
    }
}
