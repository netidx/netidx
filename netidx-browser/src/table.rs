use super::{
    util::{err_modal, toplevel},
    BSCtx, BSCtxRef, BSNode, WVal,
};
use crate::bscript::LocalEvent;
use arcstr::ArcStr;
use futures::channel::oneshot;
use fxhash::FxBuildHasher;
use gdk::{keys, EventKey, RGBA};
use gio::prelude::*;
use glib::{self, clone, idle_add_local, signal::Inhibit, source::Continue};
use gtk::{
    prelude::*, Adjustment, CellRenderer, CellRendererText, Label, ListStore,
    ScrolledWindow, SelectionMode, SortColumn, SortType, StateFlags, StyleContext,
    TreeIter, TreeModel, TreePath, TreeView, TreeViewColumn, TreeViewColumnSizing,
    Widget as GtkWidget,
};
use indexmap::IndexMap;
use netidx::{
    chars::Chars,
    path::Path,
    resolver,
    subscriber::{Dval, Event, SubId, UpdatesFlags, Value},
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
};

struct Subscription {
    _sub: Dval,
    row: TreeIter,
    col: u32,
}

struct RaeifiedTableInner {
    path: Path,
    ctx: BSCtx,
    root: ScrolledWindow,
    view: TreeView,
    style: StyleContext,
    selected_path: Label,
    store: ListStore,
    by_id: RefCell<HashMap<SubId, Subscription>>,
    subscribed: RefCell<HashMap<String, HashSet<u32>>>,
    focus_column: RefCell<Option<TreeViewColumn>>,
    focus_row: RefCell<Option<String>>,
    descriptor: resolver::Table,
    vector_mode: bool,
    sort_column: Cell<Option<u32>>,
    sort_temp_disabled: Cell<bool>,
    update: RefCell<IndexMap<SubId, Value, FxBuildHasher>>,
    destroyed: Cell<bool>,
    on_select: Rc<RefCell<BSNode>>,
    on_activate: Rc<RefCell<BSNode>>,
    on_edit: Rc<RefCell<BSNode>>,
}

#[derive(Clone)]
struct RaeifiedTable(Rc<RaeifiedTableInner>);

struct RaeifiedTableWeak(Weak<RaeifiedTableInner>);

#[derive(Debug, Clone, PartialEq, Eq)]
enum SortSpec {
    None,
    Ascending(String),
    Descending(String),
}

impl SortSpec {
    fn new(v: Value) -> Result<Self, String> {
        match v {
            Value::Null => Ok(SortSpec::None),
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
            _ => Err("expected null, col, or a pair of [column, mode]".into()),
        }
    }
}

#[derive(Debug, Clone)]
enum Filter {
    All,
    Auto,
    None,
    Include(HashSet<String>),
    Exclude(HashSet<String>),
    IncludeMatch(HashSet<String>, RegexSet),
    ExcludeMatch(HashSet<String>, RegexSet),
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
            | (_, Filter::IncludeMatch(_, _)) => false,
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
                let i = a[1]
                    .clone()
                    .flatten()
                    .map(|v| v.cast_to::<String>().map_err(|e| format!("{}", e)));
                if &*mode == "include" {
                    Ok(Filter::Include(i.collect::<Result<HashSet<_, _>, _>>()?))
                } else if &*mode == "exclude" {
                    Ok(Filter::Exclude(i.collect::<Result<HashSet<_, _>, _>>()?))
                } else if &*mode == "include_match" {
                    let set = i.collect::<Result<HashSet<_>, _>>()?;
                    let matcher = RegexSet::new(&set).map_err(|e| format!("{}", e))?;
                    Ok(Filter::IncludeMatch(set, matcher))
                } else if &*mode == "exclude_match" {
                    let set = i.collect::<Result<HashSet<_>, _>>()?;
                    let matcher = RegexSet::new(&set).map_err(|e| format!("{}", e))?;
                    Ok(Filter::ExcludeMatch(set, matcher))
                } else {
                    Err("invalid filter mode".into())
                }
            }
            _ => Err("expected null, true, false, or a pair".into()),
        }
    }

    fn is_match(&self, s: &str) -> bool {
        match self {
            Filter::All | Filter::Auto => true,
            Filter::None => false,
            Filter::Include(set) => set.contains(s),
            Filter::Exclude(set) => !set.contains(s),
            Filter::IncludeMatch(_, set) => set.is_match(s),
            Filter::ExcludeMatch(_, set) => !set.is_match(s),
        }
    }
}

enum TableState {
    Empty,
    Resolving(Path),
    Raeified(RaeifiedTable),
}

pub(super) struct Table {
    ctx: BSCtx,
    state: RefCell<TableState>,
    root: ScrolledWindow,
    selected_path: Label,
    path_expr: BSNode,
    path: RefCell<Option<Path>>,
    default_sort_column_expr: BSNode,
    default_sort_column: RefCell<Result<SortSpec, String>>,
    column_filter_expr: BSNode,
    column_filter: RefCell<Result<Filter, String>>,
    row_filter_expr: BSNode,
    row_filter: RefCell<Result<Filter, String>>,
    column_editable_expr: BSNode,
    column_editable: RefCell<Result<Filter, String>>,
    on_select: Rc<RefCell<BSNode>>,
    on_activate: Rc<RefCell<BSNode>>,
    on_edit: Rc<RefCell<BSNode>>,
}

fn get_sort_column(store: &ListStore) -> Option<u32> {
    match store.get_sort_column_id() {
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
    let v0_v = m.get_value(r0, col);
    let v1_v = m.get_value(r1, col);
    let v0_r = v0_v.get::<&str>();
    let v1_r = v1_v.get::<&str>();
    match (v0_r, v1_r) {
        (Err(_), Err(_)) => Ordering::Equal,
        (Err(_), _) => Ordering::Greater,
        (_, Err(_)) => Ordering::Less,
        (Ok(None), Ok(None)) => Ordering::Equal,
        (Ok(None), _) => Ordering::Less,
        (_, Ok(None)) => Ordering::Greater,
        (Ok(Some(v0)), Ok(Some(v1))) => match (v0.parse::<f64>(), v1.parse::<f64>()) {
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

impl RaeifiedTable {
    fn new(
        ctx: BSCtx,
        root: ScrolledWindow,
        path: Path,
        default_sort_column: Result<SortSpec, String>,
        column_filter: Result<Filter, String>,
        row_filter: Result<Filter, String>,
        column_editable: Result<Filter, String>,
        on_select: Rc<RefCell<BSNode>>,
        on_activate: Rc<RefCell<BSNode>>,
        on_edit: Rc<RefCell<BSNode>>,
        mut descriptor: resolver::Table,
        selected_path: Label,
    ) -> RaeifiedTable {
        let mut filter_errors = Vec::new();
        let row_filter = row_filter.unwrap_or_else(|e| {
            filter_errors.push(format!("invalid row filter: {}", e));
            Filter::All
        });
        let column_filter = column_filter.unwrap_or_else(|e| {
            filter_errors.push(format!("invalid column filter {}", e));
            Filter::Auto
        });
        let column_editable = column_editable.unwrap_or_else(|e| {
            filter_errors.push(format!("invalid column editable {}", e));
            Filter::None
        });
        let default_sort_column = default_sort_column.unwrap_or_else(|e| {
            filter_errors.push(format!("invalid default sort column {}", e));
            SortSpec::None
        });
        descriptor.cols.sort_by_key(|(p, _)| p.clone());
        descriptor.rows.sort();
        descriptor.rows.retain(|row| match Path::basename(&row) {
            None => true,
            Some(row) => row_filter.is_match(row),
        });
        match column_filter {
            Filter::Auto => {
                let nrows = (descriptor.rows.len() >> 1) as u64;
                descriptor.cols.retain(|(_, i)| i.0 >= nrows)
            }
            filter => descriptor.cols.retain(|col| match Path::basename(&col.0) {
                None => true,
                Some(col) => filter.is_match(&col),
            }),
        }
        let view = TreeView::new();
        root.add(&view);
        view.get_selection().set_mode(SelectionMode::None);
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
        let style = view.get_style_context();
        let ncols = if vector_mode { 1 } else { descriptor.cols.len() };
        let t = RaeifiedTable(Rc::new(RaeifiedTableInner {
            path,
            ctx,
            root,
            view,
            selected_path,
            store,
            descriptor,
            vector_mode,
            style,
            on_select,
            on_activate,
            on_edit,
            by_id: RefCell::new(HashMap::new()),
            subscribed: RefCell::new(HashMap::new()),
            focus_column: RefCell::new(None),
            focus_row: RefCell::new(None),
            sort_column: Cell::new(None),
            sort_temp_disabled: Cell::new(false),
            update: RefCell::new(IndexMap::with_hasher(FxBuildHasher::default())),
            destroyed: Cell::new(false),
        }));
        t.view().connect_destroy(clone!(@weak t => move |_| t.0.destroyed.set(true)));
        t.view().append_column(&{
            let column = TreeViewColumn::new();
            let cell = CellRendererText::new();
            column.pack_start(&cell, true);
            column.set_title("name");
            column.add_attribute(&cell, "text", 0);
            column.set_sort_column_id(0);
            column.set_sizing(TreeViewColumnSizing::Fixed);
            column
        });
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
            if column_editable.is_match(&*name) {
                cell.set_property_editable(true);
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
            column.set_title(&name);
            t.store().set_sort_func(SortColumn::Index(id as u32), move |m, r0, r1| {
                compare_row(id, m, r0, r1)
            });
            column.set_sort_column_id(id);
            column.set_sizing(TreeViewColumnSizing::Fixed);
            t.view().append_column(&column);
        }
        t.store().connect_sort_column_changed(clone!(@weak t => move |_| {
            if t.0.sort_temp_disabled.get() {
                return
            }
            match get_sort_column(t.store()) {
                Some(col) => { t.0.sort_column.set(Some(col)); }
                None => {
                    t.0.sort_column.set(None);
                    t.store().set_unsorted();
                }
            }
            idle_add_local(clone!(@weak t => @default-return Continue(false), move || {
                t.update_subscriptions();
                Continue(false)
            }));
        }));
        t.view().set_fixed_height_mode(true);
        t.view().set_model(Some(t.store()));
        t.view().connect_key_press_event(clone!(
            @weak t => @default-return Inhibit(false), move |_, k| t.handle_key(k)));
        t.view().connect_row_activated(clone!(@weak t => move |_, p, _| {
            if let Some(iter) = t.store().get_iter(&p) {
                if let Ok(Some(row_name)) = t.store().get_value(&iter, 0).get::<&str>() {
                    let path = String::from(&*t.0.path.append(row_name));
                    let e = LocalEvent::Event(Value::String(Chars::from(path)));
                    t.0.on_activate.borrow_mut().update(
                        &mut t.0.ctx.borrow_mut(),
                        &vm::Event::User(e)
                    );
                }
            }
        }));
        t.view().connect_cursor_changed(clone!(@weak t => move |_| t.cursor_changed()));
        t.0.root.get_vadjustment().map(|va| {
            va.connect_value_changed(clone!(@weak t => move |_| {
                idle_add_local(clone!(@weak t => @default-return Continue(false), move || {
                    t.update_subscriptions();
                    Continue(false)
                }));
            }));
        });
        match &default_sort_column {
            SortSpec::None => (),
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
                let dir = match &default_sort_column {
                    SortSpec::Ascending(_) => gtk::SortType::Ascending,
                    SortSpec::Descending(_) => gtk::SortType::Descending,
                    SortSpec::None => unreachable!(),
                };
                if let Some(i) = idx {
                    t.store().set_sort_column_id(gtk::SortColumn::Index(i as u32), dir)
                }
            }
        }
        t.0.root.show_all();
        t
    }

    fn render_cell(&self, id: i32, c: &TreeViewColumn, cr: &CellRenderer, i: &TreeIter) {
        let cr = cr.clone().downcast::<CellRendererText>().unwrap();
        let rn_v = self.store().get_value(i, 0);
        let rn = rn_v.get::<&str>();
        cr.set_property_text(match self.store().get_value(i, id).get::<&str>() {
            Ok(v) => v,
            _ => return,
        });
        match (&*self.0.focus_column.borrow(), &*self.0.focus_row.borrow(), rn) {
            (Some(fc), Some(fr), Ok(Some(rn))) if fc == c && fr.as_str() == rn => {
                let st = StateFlags::SELECTED;
                let fg = self.0.style.get_color(st);
                let bg =
                    StyleContextExt::get_property(&self.0.style, "background-color", st);
                let bg = bg.get::<RGBA>().unwrap();
                cr.set_property_cell_background_rgba(bg.as_ref());
                cr.set_property_foreground_rgba(Some(&fg));
            }
            _ => {
                cr.set_property_cell_background(None);
                cr.set_property_foreground(None);
            }
        }
    }

    fn handle_key(&self, key: &EventKey) -> Inhibit {
        if key.get_keyval() == keys::constants::Escape {
            // unset the focus
            self.view().set_cursor::<TreeViewColumn>(&TreePath::new(), None, false);
            *self.0.focus_column.borrow_mut() = None;
            *self.0.focus_row.borrow_mut() = None;
            self.0.selected_path.set_label("");
        }
        if key.get_keyval() == keys::constants::w
            && key.get_state().contains(gdk::ModifierType::CONTROL_MASK)
        {
            self.write_dialog()
        }
        Inhibit(false)
    }

    fn write_dialog(&self) {
        let window = toplevel(self.view());
        let selected = self.0.selected_path.get_text();
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
            let root = d.get_content_area();
            let data_lbl = gtk::Label::new(Some("value:"));
            let data = gtk::Entry::new();
            let data_box = gtk::Box::new(gtk::Orientation::Horizontal, 5);
            data_box.add(&data_lbl);
            data_box.add(&data);
            let err = gtk::Label::new(None);
            let err_attrs = pango::AttrList::new();
            err_attrs.insert(pango::Attribute::new_foreground(0xFFFFu16, 0, 0).unwrap());
            err.set_attributes(Some(&err_attrs));
            if let Some(v) = &*val.borrow() {
                data.set_text(&format!("{}", WVal(v)));
            }
            data.connect_changed(clone!(
                @strong val,
                @strong err => move |data| match data.get_text().parse::<Value>() {
                    Err(e) => {
                        *val.borrow_mut() = None;
                        err.set_text(&format!("{}", e));
                        data.set_icon_from_icon_name(
                            gtk::EntryIconPosition::Secondary,
                            Some("dialog-error")
                        );
                    }
                    Ok(v) => {
                        err.set_text("");
                        data.set_icon_from_icon_name(
                            gtk::EntryIconPosition::Secondary,
                            None
                        );
                        *val.borrow_mut() = Some(v)
                    }
            }));
            root.add(&data_box);
            root.add(&err);
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

    fn cursor_changed(&self) {
        let (p, c) = self.view().get_cursor();
        let row_name = match p {
            None => None,
            Some(p) => match self.store().get_iter(&p) {
                None => None,
                Some(i) => Some(self.store().get_value(&i, 0)),
            },
        };
        let path = match row_name {
            None => None,
            Some(row_name) => match row_name.get::<&str>().ok().flatten() {
                None => None,
                Some(row_name) => {
                    *self.0.focus_column.borrow_mut() = c.clone();
                    *self.0.focus_row.borrow_mut() = Some(String::from(row_name));
                    let col_name = if self.0.vector_mode {
                        None
                    } else if self.view().get_column(0) == c {
                        None
                    } else {
                        c.as_ref().and_then(|c| c.get_title())
                    };
                    match col_name {
                        None => Some(self.0.path.append(row_name)),
                        Some(col_name) => {
                            Some(self.0.path.append(row_name).append(col_name.as_str()))
                        }
                    }
                }
            },
        };
        self.0.selected_path.set_label(path.as_ref().map(|c| c.as_ref()).unwrap_or(""));
        if let Some(path) = path {
            let v = Value::from(String::from(&*path));
            let ev = vm::Event::User(LocalEvent::Event(v));
            self.0.on_select.borrow_mut().update(&mut self.0.ctx.borrow_mut(), &ev);
        }
        self.view().columns_autosize();
    }

    pub(super) fn update_subscriptions(&self) {
        if self.0.destroyed.get() {
            return;
        }
        let ncols = if self.0.vector_mode { 1 } else { self.0.descriptor.cols.len() };
        let (mut start, mut end) = match self.view().get_visible_range() {
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
        self.0.by_id.borrow_mut().retain(|_, v| match self.store().get_path(&v.row) {
            None => false,
            Some(p) => {
                let visible =
                    (p >= start && p <= end) || (Some(v.col) == self.0.sort_column.get());
                if !visible {
                    let row_name_v = self.store().get_value(&v.row, 0);
                    if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
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
            if let Some(row) = self.store().get_iter(&start) {
                let row_name_v = self.store().get_value(&row, 0);
                if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
                    for col in 0..ncols {
                        maybe_subscribe_col(&row, row_name, (col + 1) as u32);
                    }
                }
            }
            start.next();
        }
        // subscribe to all rows in the sort column
        if let Some(id) = self.0.sort_column.get() {
            if let Some(row) = self.store().get_iter_first() {
                loop {
                    let row_name_v = self.store().get_value(&row, 0);
                    if let Ok(Some(row_name)) = row_name_v.get::<&str>() {
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
        let col = self.store().get_sort_column_id();
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
                    let (mut start, end) = match t.view().get_visible_range() {
                        None => return Continue(false),
                        Some((s, e)) => (s, e),
                    };
                    while start <= end {
                        if let Some(i) = t.store().get_iter(&start) {
                            t.store().row_changed(&start, &i);
                        }
                        start.next();
                    }
                    t.view().columns_autosize();
                    Continue(false)
                }
            }
        }));
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
        let default_sort_column_expr = BSNode::compile(
            &mut ctx.borrow_mut(),
            scope.clone(),
            spec.default_sort_column,
        );
        let default_sort_column = RefCell::new(SortSpec::new(
            default_sort_column_expr.current().unwrap_or(Value::Null),
        ));
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
            scope,
            spec.on_edit,
        )));
        Table {
            ctx,
            state,
            root: ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>),
            selected_path,
            path_expr,
            path,
            on_select,
            on_activate,
            on_edit,
            default_sort_column_expr,
            default_sort_column,
            column_filter_expr,
            column_filter,
            row_filter_expr,
            row_filter,
            column_editable_expr,
            column_editable,
        }
    }

    fn refresh(&self, ctx: BSCtxRef) {
        let state = &mut *self.state.borrow_mut();
        let path = &*self.path.borrow();
        match state {
            TableState::Resolving(rpath) if path.as_ref() == Some(rpath) => (),
            TableState::Resolving(_)
            | TableState::Raeified { .. }
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
        if let Some(c) = self.root.get_child() {
            self.root.remove(&c);
        }
    }

    pub(super) fn root(&self) -> &GtkWidget {
        self.root.upcast_ref()
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
        if let Some(col) = self.default_sort_column_expr.update(ctx, event) {
            let new_col = SortSpec::new(col);
            if &*self.default_sort_column.borrow() != &new_col {
                *self.default_sort_column.borrow_mut() = new_col;
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
        self.on_activate.borrow_mut().update(ctx, event);
        self.on_select.borrow_mut().update(ctx, event);
        self.on_edit.borrow_mut().update(ctx, event);
        match &*self.state.borrow() {
            TableState::Empty | TableState::Resolving(_) => (),
            TableState::Raeified(table) => table.update(ctx, waits, event),
        }
        match event {
            vm::Event::Netidx(_, _)
            | vm::Event::Rpc(_, _)
            | vm::Event::Variable(_, _, _)
            | vm::Event::User(LocalEvent::Event(_)) => (),
            vm::Event::User(LocalEvent::TableResolved(path, descriptor)) => {
                let state = &mut *self.state.borrow_mut();
                match state {
                    TableState::Empty | TableState::Raeified { .. } => (),
                    TableState::Resolving(rpath) => {
                        if path == rpath {
                            let table = RaeifiedTable::new(
                                self.ctx.clone(),
                                self.root.clone(),
                                path.clone(),
                                self.default_sort_column.borrow().clone(),
                                self.column_filter.borrow().clone(),
                                self.row_filter.borrow().clone(),
                                self.column_editable.borrow().clone(),
                                self.on_select.clone(),
                                self.on_activate.clone(),
                                self.on_edit.clone(),
                                descriptor.clone(),
                                self.selected_path.clone(),
                            );
                            table.update_subscriptions();
                            *state = TableState::Raeified(table);
                        }
                    }
                }
            }
        }
    }
}
