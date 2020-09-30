use super::{FromGui, Target, ViewLoc, WidgetCtx};
use futures::channel::oneshot;
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
    path::Path,
    resolver,
    subscriber::{Dval, SubId, Value},
};
use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::{HashMap, HashSet},
    iter::FromIterator,
    ops::Drop,
    rc::{Rc, Weak},
    result,
};

struct Subscription {
    _sub: Dval,
    row: TreeIter,
    col: u32,
}

struct TableInner {
    ctx: WidgetCtx,
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
    base_path: Path,
    sort_column: Cell<Option<u32>>,
    sort_temp_disabled: Cell<bool>,
    update: RefCell<IndexMap<SubId, Value>>,
}

impl Drop for TableInner {
    fn drop(&mut self) {
        self.view.set_model(None::<&ListStore>)
    }
}

#[derive(Clone)]
pub(super) struct Table(Rc<TableInner>);

pub(super) struct TableWeak(Weak<TableInner>);

impl clone::Downgrade for Table {
    type Weak = TableWeak;

    fn downgrade(&self) -> Self::Weak {
        TableWeak(Rc::downgrade(&self.0))
    }
}

impl clone::Upgrade for TableWeak {
    type Strong = Table;

    fn upgrade(&self) -> Option<Self::Strong> {
        Weak::upgrade(&self.0).map(Table)
    }
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

fn apply_spec(spec: &view::Table, desc: &mut resolver::Table) {
    fn filter_cols(
        cols: &Vec<String>,
        desc: &mut resolver::Table,
        f: impl Fn(bool) -> bool,
    ) {
        let set = HashSet::from_iter(cols.into().cloned().map(Path::from));
        let mut i = 0;
        while i < descr.cols.len() {
            if f(set.contains(&descr.cols[i].0)) {
                descr.cols.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }
    match spec.columns {
        view::ColumnSpec::Auto => (),
        view::Hide(cols) => filter_cols(cols, desc, |x| x),
        view::Exact(cols) => {
            let order = HashMap::from_iter(
                cols.iter().cloned().map(Path::from).enumerate().map(|(i, c)| (c, i)),
            );
            filter_cols(set, desc, |x| !x);
            desc.cols.sort_by(|(c0, _), (c1, _)| order[c0].cmp(order[c1]));
        }
    }
}

impl Table {
    pub(super) fn new(
        ctx: WidgetCtx,
        spec: view::Table,
        mut descriptor: resolver::Table,
        selected_path: Label,
    ) -> Table {
        apply_spec(&spec, &mut descriptor);
        let view = TreeView::new();
        let root = ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
        root.add(&view);
        let nrows = descriptor.rows.len();
        descriptor.rows.sort();
        descriptor.cols.sort_by_key(|(p, _)| p.clone());
        descriptor.cols.retain(|(_, i)| i.0 >= (nrows / 2) as u64);
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
        let t = Table(Rc::new(TableInner {
            ctx,
            root,
            view,
            selected_path,
            store,
            descriptor,
            vector_mode,
            base_path,
            style,
            by_id: RefCell::new(HashMap::new()),
            subscribed: RefCell::new(HashMap::new()),
            focus_column: RefCell::new(None),
            focus_row: RefCell::new(None),
            sort_column: Cell::new(None),
            sort_temp_disabled: Cell::new(false),
            update: RefCell::new(IndexMap::new()),
        }));
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
            column.pack_start(&cell, true);
            let f = Box::new(clone!(@weak t =>
                move |c: &TreeViewColumn,
                      cr: &CellRenderer,
                      _: &TreeModel,
                      i: &TreeIter| t.render_cell(id, c, cr, i)));
            TreeViewColumnExt::set_cell_data_func(&column, &cell, Some(f));
            column.set_title(&if vector_mode {
                Path::from("value")
            } else {
                t.0.descriptor.cols[col].0.clone()
            });
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
                    let path = t.0.base_path.append(row_name);
                    let m = FromGui::Navigate(ViewLoc::Netidx(path));
                    let _: result::Result<_, _> = t.0.ctx.from_gui.unbounded_send(m);
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
        Inhibit(false)
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
            None => Path::from(""),
            Some(row_name) => match row_name.get::<&str>().ok().flatten() {
                None => Path::from(""),
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
                        None => self.0.base_path.append(row_name),
                        Some(col_name) => {
                            self.0.base_path.append(row_name).append(col_name.as_str())
                        }
                    }
                }
            },
        };
        self.0.selected_path.set_label(&*path);
        let (mut start, end) = match self.view().get_visible_range() {
            None => return,
            Some((s, e)) => (s, e),
        };
        self.view().columns_autosize();
        while start <= end {
            if let Some(i) = self.store().get_iter(&start) {
                self.store().row_changed(&start, &i);
            }
            start.next();
        }
    }

    pub(super) fn update_subscriptions(&self) {
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
                let p = self.0.base_path.append(row_name);
                let p = if self.0.vector_mode {
                    p
                } else {
                    p.append(&self.0.descriptor.cols[(id - 1) as usize].0)
                };
                let s = self.0.ctx.subscriber.durable_subscribe(p);
                s.updates(true, self.0.ctx.updates.clone());
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
        self.view().freeze_child_notify();
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

    pub(super) fn root(&self) -> &GtkWidget {
        self.0.root.upcast_ref()
    }

    fn view(&self) -> &TreeView {
        &self.0.view
    }

    fn store(&self) -> &ListStore {
        &self.0.store
    }

    pub(super) fn start_update_task(
        &self,
        mut tx: Option<oneshot::Sender<()>>,
        sctx: Option<(SortColumn, SortType)>,
    ) {
        let t = self;
        idle_add_local(clone!(@weak t => @default-return Continue(false), move || {
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
                t.update_subscriptions();
                Continue(false)
            }
        }));
    }

    pub(super) fn update(
        &self,
        waits: &mut Vec<oneshot::Receiver<()>>,
        tgt: Target,
        value: &Value,
    ) {
        match tgt {
            Target::Variable(_) => (),
            Target::Netidx(id) => {
                self.0.update.borrow_mut().insert(id, value.clone());
                if self.0.update.borrow().len() == 1 {
                    let (tx, rx) = oneshot::channel();
                    waits.push(rx);
                    self.start_update_task(Some(tx), self.disable_sort());
                }
            }
        }
    }
}
