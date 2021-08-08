use super::{
    util::{err_modal, toplevel},
    BSCtx, BSCtxRef, BSNode,
};
use crate::bscript::LocalEvent;
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
    subscriber::{Dval, Event, SubId, Typ, UpdatesFlags, Value},
    utils::split_escaped,
};
use netidx_bscript::vm;
use netidx_protocols::view;
use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::{HashMap, HashSet},
    iter::FromIterator,
    rc::{Rc, Weak},
    result,
    str::FromStr,
};
use arcstr::ArcStr;

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
    default_sort_column: RefCell<Option<String>>,
    default_sort_column_direction_expr: BSNode,
    default_sort_column_direction: RefCell<SortDir>,
    column_mode_expr: BSNode,
    column_mode: RefCell<ColumnMode>,
    column_list_expr: BSNode,
    column_list: RefCell<Vec<String>>,
    editable_expr: BSNode,
    editable: RefCell<EditMode>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortDir {
    Ascending,
    Descending,
}

impl SortDir {
    fn new(v: Value) -> Self {
        match v {
            Value::String(c) if &*c == "ascending" => SortDir::Ascending,
            _ => SortDir::Descending,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnMode {
    Auto,
    Hide,
    Exactly,
}

impl ColumnMode {
    fn new(v: Value) -> Self {
        match v {
            Value::String(c) if &*c == "hide" => ColumnMode::Hide,
            Value::String(c) if &*c == "exactly" => ColumnMode::Exactly,
            _ => ColumnMode::Auto,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EditMode {
    Full(bool),
    Partial(HashSet<String>),
}

impl EditMode {
    fn new(v: Value) -> Self {
        match v {
            Value::True => EditMode::Full(true),
            Value::False => EditMode::Full(false),
            Value::String(s) => match serde_json::from_str::<HashSet<String>>(&*s) {
                Ok(cols) => EditMode::Partial(cols),
                Err(_) => EditMode::Partial(
                    split_escaped(&*s, '\\', ',')
                        .map(|s| String::from(s.trim()))
                        .collect(),
                ),
            },
            _ => EditMode::Full(false),
        }
    }
}

fn cols_from_val(v: Value) -> Vec<String> {
    match v {
        Value::String(c) => match serde_json::from_str::<Vec<String>>(&*c) {
            Ok(cols) => cols,
            Err(_) => {
                split_escaped(&*c, '\\', ',').map(|s| String::from(s.trim())).collect()
            }
        },
        _ => vec![],
    }
}

fn apply_spec(mode: ColumnMode, cols: &[String], descr: &mut resolver::Table) {
    fn filter_cols(
        cols: &[String],
        descr: &mut resolver::Table,
        f: impl Fn(bool) -> bool,
    ) {
        let set: HashSet<Path> = HashSet::from_iter(cols.iter().cloned().map(Path::from));
        let mut i = 0;
        while i < descr.cols.len() {
            if f(set.contains(&descr.cols[i].0)) {
                descr.cols.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }
    match mode {
        ColumnMode::Hide => filter_cols(cols, descr, |x| x),
        ColumnMode::Exactly => {
            let order: HashMap<Path, usize> = HashMap::from_iter(
                cols.iter().cloned().map(Path::from).enumerate().map(|(i, c)| (c, i)),
            );
            filter_cols(&cols, descr, |x| !x);
            descr.cols.sort_by(|(c0, _), (c1, _)| order[c0].cmp(&order[c1]));
        }
        _ => (),
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
        default_sort_column: Option<String>,
        default_sort_column_direction: SortDir,
        column_mode: ColumnMode,
        column_list: Vec<String>,
        editable: EditMode,
        on_select: Rc<RefCell<BSNode>>,
        on_activate: Rc<RefCell<BSNode>>,
        on_edit: Rc<RefCell<BSNode>>,
        mut descriptor: resolver::Table,
        selected_path: Label,
    ) -> RaeifiedTable {
        apply_spec(column_mode, &column_list, &mut descriptor);
        let view = TreeView::new();
        root.add(&view);
        let nrows = descriptor.rows.len();
        descriptor.rows.sort();
        match column_mode {
            ColumnMode::Exactly => (),
            ColumnMode::Auto | ColumnMode::Hide => {
                descriptor.cols.sort_by_key(|(p, _)| p.clone());
                descriptor.cols.retain(|(_, i)| i.0 >= (nrows / 2) as u64);
            }
        }
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
            match &editable {
                EditMode::Full(v) => cell.set_property_editable(*v),
                EditMode::Partial(allowed) => {
                    if allowed.contains(&*name) {
                        cell.set_property_editable(true);
                    }
                }
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
        if let Some(col) = &default_sort_column {
            let col = Path::from(col.clone());
            let idx = t.0.descriptor.cols.iter().enumerate().find_map(|(i, (c, _))| {
                if c == &col {
                    Some(i + 1)
                } else {
                    None
                }
            });
            let dir = match default_sort_column_direction {
                SortDir::Ascending => gtk::SortType::Ascending,
                SortDir::Descending => gtk::SortType::Descending,
            };
            if let Some(i) = idx {
                t.store().set_sort_column_id(gtk::SortColumn::Index(i as u32), dir)
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
            let cb_lbl = gtk::Label::new(Some("Type:"));
            let cb = gtk::ComboBoxText::new();
            let cb_box = gtk::Box::new(gtk::Orientation::Horizontal, 5);
            cb_box.add(&cb_lbl);
            cb_box.add(&cb);
            let data_lbl = gtk::Label::new(Some("value:"));
            let data = gtk::Entry::new();
            let data_box = gtk::Box::new(gtk::Orientation::Horizontal, 5);
            data_box.add(&data_lbl);
            data_box.add(&data);
            let err = gtk::Label::new(None);
            let err_attrs = pango::AttrList::new();
            err_attrs.insert(pango::Attribute::new_foreground(0xFFFFu16, 0, 0).unwrap());
            err.set_attributes(Some(&err_attrs));
            for typ in Typ::all() {
                let name = typ.name();
                cb.append(Some(name), name);
            }
            let typ =
                Rc::new(RefCell::new(val.borrow().as_ref().and_then(|v| Typ::get(v))));
            let parse_val = Rc::new(clone!(
                @strong typ,
                @strong val,
                @strong data,
                @strong err => move || {
                match &*typ.borrow() {
                    None => { *val.borrow_mut() = Some(Value::Null); }
                    Some(typ) => match typ.parse(&*data.get_text()) {
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
                    }
                }
            }));
            cb.set_active_id(typ.borrow().map(|t| t.name()));
            cb.connect_changed(clone!(@strong typ, @strong parse_val => move |cb| {
                *typ.borrow_mut() = cb.get_active_id().and_then(|s| {
                    Typ::from_str(&*s).ok()
                });
                parse_val();
            }));
            if let Some(v) = &*val.borrow() {
                data.set_text(&format!("{}", v));
            }
            data.connect_changed(clone!(@strong parse_val => move |_| parse_val()));
            root.add(&cb_box);
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
                            let s = &format!("{}", v).to_value();
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
            vm::Event::User(_) | vm::Event::Variable(_, _) | vm::Event::Rpc(_, _) => (),
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
    pub(super) fn new(ctx: BSCtx, spec: view::Table, selected_path: Label) -> Table {
        let path_expr = BSNode::compile(&mut ctx.borrow_mut(), spec.path);
        let path = RefCell::new(path_expr.current().and_then(|v| match v {
            Value::String(path) => Some(Path::from(ArcStr::from(&*path))),
            _ => None,
        }));
        let default_sort_column_expr =
            BSNode::compile(&mut ctx.borrow_mut(), spec.default_sort_column);
        let default_sort_column =
            RefCell::new(default_sort_column_expr.current().and_then(|v| match v {
                Value::String(c) => Some(String::from(&*c)),
                _ => None,
            }));
        let default_sort_column_direction_expr =
            BSNode::compile(&mut ctx.borrow_mut(), spec.default_sort_column_direction);
        let default_sort_column_direction = RefCell::new(SortDir::new(
            default_sort_column_direction_expr.current().unwrap_or(Value::Null),
        ));
        let column_mode_expr = BSNode::compile(&mut ctx.borrow_mut(), spec.column_mode);
        let column_mode = RefCell::new(ColumnMode::new(
            column_mode_expr.current().unwrap_or(Value::Null),
        ));
        let column_list_expr = BSNode::compile(&mut ctx.borrow_mut(), spec.column_list);
        let column_list = RefCell::new(cols_from_val(
            column_list_expr.current().unwrap_or(Value::Null),
        ));
        let editable_expr = BSNode::compile(&mut ctx.borrow_mut(), spec.editable);
        let editable =
            RefCell::new(EditMode::new(editable_expr.current().unwrap_or(Value::Null)));
        let state = RefCell::new(match &*path.borrow() {
            None => TableState::Empty,
            Some(path) => {
                ctx.borrow().user.backend.resolve_table(path.clone());
                TableState::Resolving(path.clone())
            }
        });
        let on_select =
            Rc::new(RefCell::new(BSNode::compile(&mut ctx.borrow_mut(), spec.on_select)));
        let on_activate = Rc::new(RefCell::new(BSNode::compile(
            &mut ctx.borrow_mut(),
            spec.on_activate,
        )));
        let on_edit =
            Rc::new(RefCell::new(BSNode::compile(&mut ctx.borrow_mut(), spec.on_edit)));
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
            default_sort_column_direction_expr,
            default_sort_column_direction,
            column_mode_expr,
            column_mode,
            column_list_expr,
            column_list,
            editable_expr,
            editable,
        }
    }

    fn refresh(&self) {
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
                    self.ctx.borrow().user.backend.resolve_table(path.clone());
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
                self.refresh();
            }
        }
        if let Some(col) = self.default_sort_column_expr.update(ctx, event) {
            let new_col = match col {
                Value::String(c) => Some(String::from(&*c)),
                _ => None,
            };
            if &*self.default_sort_column.borrow() != &new_col {
                *self.default_sort_column.borrow_mut() = new_col;
                self.refresh();
            }
        }
        if let Some(dir) = self.default_sort_column_direction_expr.update(ctx, event) {
            let new_dir = SortDir::new(dir);
            if &*self.default_sort_column_direction.borrow() != &new_dir {
                *self.default_sort_column_direction.borrow_mut() = new_dir;
                self.refresh();
            }
        }
        if let Some(mode) = self.column_mode_expr.update(ctx, event) {
            let new_mode = ColumnMode::new(mode);
            if &*self.column_mode.borrow() != &new_mode {
                *self.column_mode.borrow_mut() = new_mode;
                self.refresh();
            }
        }
        if let Some(cols) = self.column_list_expr.update(ctx, event) {
            let new_lst = cols_from_val(cols);
            if &*self.column_list.borrow() != &new_lst {
                *self.column_list.borrow_mut() = new_lst;
                self.refresh();
            }
        }
        if let Some(v) = self.editable_expr.update(ctx, event) {
            let new_mode = EditMode::new(v);
            if &*self.editable.borrow() != &new_mode {
                *self.editable.borrow_mut() = new_mode;
                self.refresh();
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
            | vm::Event::Variable(_, _)
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
                                self.default_sort_column_direction.borrow().clone(),
                                self.column_mode.borrow().clone(),
                                self.column_list.borrow().clone(),
                                self.editable.borrow().clone(),
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
