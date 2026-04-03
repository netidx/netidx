use crate::{view, BCtx, BWidget, GxProp, WVal};
use fxhash::FxHashMap;
use futures::channel::mpsc;
use glib::{self, idle_add_local, ControlFlow};
use graphix_compiler::expr::ExprId;
use gtk::{
    self,
    prelude::*,
    Adjustment, CellRendererText, ListStore, ScrolledWindow,
    TreeView, TreeViewColumn,
};
use netidx::{
    path::Path,
    publisher::Value,
    subscriber::{Dval, Event, SubId, UpdatesFlags},
};
use netidx_netproto::resolver;
use std::{
    cell::{Cell, RefCell},
    collections::HashSet,
    rc::Rc,
};

/// A subscription to a single table cell.
struct CellSub {
    _sub: Dval,
    row: gtk::TreeIter,
    col: u32,
}

/// The descriptor of a resolved table as received from the resolver.
struct TableDescriptor {
    path: Path,
    rows: Vec<Path>,
    cols: Vec<Path>,
}

/// The live, raeified table state: a TreeView with subscriptions to
/// visible cells.
struct RaeifiedState {
    descriptor: TableDescriptor,
    store: ListStore,
    view: TreeView,
    by_id: FxHashMap<SubId, CellSub>,
    /// Tracks which (row_index, col_index) pairs are currently subscribed.
    subscribed: HashSet<(usize, u32)>,
    /// Channel for receiving subscription updates.
    rx: mpsc::Receiver<netidx::pool::global::GPooled<Vec<(SubId, Event)>>>,
    tx: mpsc::Sender<netidx::pool::global::GPooled<Vec<(SubId, Event)>>>,
}

enum TableState {
    /// Waiting for the resolver to return the table structure.
    Resolving(Path),
    /// Table is live and showing data.
    Raeified(RaeifiedState),
}

pub(crate) struct Table {
    ctx: BCtx,
    root: ScrolledWindow,
    path: Option<GxProp>,
    sort_mode: Option<GxProp>,
    column_filter: Option<GxProp>,
    row_filter: Option<GxProp>,
    column_editable: Option<GxProp>,
    column_widths: Option<GxProp>,
    columns_resizable: Option<GxProp>,
    column_types: Option<GxProp>,
    selection_mode: Option<GxProp>,
    selection: Option<GxProp>,
    show_row_name: Option<GxProp>,
    refresh: Option<GxProp>,
    on_select: Option<GxProp>,
    on_activate: Option<GxProp>,
    on_edit: Option<GxProp>,
    on_header_click: Option<GxProp>,
    state: Rc<RefCell<TableState>>,
    visible: Cell<bool>,
}

impl Table {
    pub(crate) fn new(ctx: &BCtx, spec: view::Table) -> Self {
        let root = ScrolledWindow::new(
            None::<&Adjustment>,
            None::<&Adjustment>,
        );
        let path = GxProp::compile(ctx, &spec.path);
        let sort_mode = GxProp::compile(ctx, &spec.sort_mode);
        let column_filter = GxProp::compile(ctx, &spec.column_filter);
        let row_filter = GxProp::compile(ctx, &spec.row_filter);
        let column_editable = GxProp::compile(ctx, &spec.column_editable);
        let column_widths = GxProp::compile(ctx, &spec.column_widths);
        let columns_resizable = GxProp::compile(ctx, &spec.columns_resizable);
        let column_types = GxProp::compile(ctx, &spec.column_types);
        let selection_mode = GxProp::compile(ctx, &spec.selection_mode);
        let selection = GxProp::compile(ctx, &spec.selection);
        let show_row_name = GxProp::compile(ctx, &spec.show_row_name);
        let refresh = GxProp::compile(ctx, &spec.refresh);
        let on_select = GxProp::compile(ctx, &spec.on_select);
        let on_activate = GxProp::compile(ctx, &spec.on_activate);
        let on_edit = GxProp::compile(ctx, &spec.on_edit);
        let on_header_click = GxProp::compile(ctx, &spec.on_header_click);
        // If we already have a path value, kick off resolution immediately.
        let initial_path = match &path {
            Some(p) => match &p.last {
                Some(Value::String(s)) => Some(Path::from(String::from(&**s))),
                _ => None,
            },
            None => None,
        };
        let state = match initial_path {
            Some(p) => {
                ctx.borrow().backend.resolve_table(p.clone());
                Rc::new(RefCell::new(TableState::Resolving(p)))
            }
            None => {
                Rc::new(RefCell::new(TableState::Resolving(Path::from("/"))))
            }
        };
        Table {
            ctx: ctx.clone(),
            root,
            path,
            sort_mode,
            column_filter,
            row_filter,
            column_editable,
            column_widths,
            columns_resizable,
            column_types,
            selection_mode,
            selection,
            show_row_name,
            refresh,
            on_select,
            on_activate,
            on_edit,
            on_header_click,
            state,
            visible: Cell::new(true),
        }
    }

    /// Called when the backend resolves a table structure.
    fn apply_table_resolved(
        &mut self,
        path: Path,
        table: resolver::Table,
    ) {
        let resolving_path = match &*self.state.borrow() {
            TableState::Resolving(p) => p.clone(),
            TableState::Raeified(r) => {
                // If we're already raeified with the same path, this
                // is a refresh.
                if r.descriptor.path == path {
                    r.descriptor.path.clone()
                } else {
                    return;
                }
            }
        };
        if resolving_path != path {
            return;
        }
        self.raeify(path, table);
    }

    /// Build the TreeView and ListStore from the resolved table descriptor.
    fn raeify(&mut self, path: Path, table: resolver::Table) {
        // Remove old child if any.
        if let Some(c) = self.root.child() {
            self.root.remove(&c);
        }
        let cols: Vec<Path> = table.cols.iter().map(|(p, _)| p.clone()).collect();
        let rows: Vec<Path> = table.rows.iter().cloned().collect();
        // Build column types for the ListStore: column 0 is the row
        // name (String), then one String column per data column.
        let n_data_cols = cols.len();
        let n_store_cols = 1 + n_data_cols;
        let col_types: Vec<glib::Type> =
            (0..n_store_cols).map(|_| glib::Type::STRING).collect();
        let store = ListStore::new(&col_types);
        // Populate rows. Column 0 = row name (basename of row path).
        for row_path in &rows {
            let row_name = Path::basename(row_path).unwrap_or("");
            let iter = store.append();
            store.set_value(&iter, 0, &row_name.to_value());
        }
        // Build the TreeView.
        let view = TreeView::with_model(&store);
        view.set_fixed_height_mode(true);
        view.set_headers_visible(true);
        // Row name column.
        let show_row_name = self.show_row_name.as_ref()
            .and_then(|p| p.last.as_ref())
            .map(crate::val_to_bool)
            .unwrap_or(true);
        {
            let col = TreeViewColumn::new();
            col.set_title("row");
            col.set_resizable(true);
            col.set_sizing(gtk::TreeViewColumnSizing::Fixed);
            col.set_fixed_width(120);
            let cell = CellRendererText::new();
            gtk::prelude::CellLayoutExt::pack_start(&col, &cell, true);
            gtk::prelude::CellLayoutExt::add_attribute(&col, &cell, "text", 0);
            col.set_visible(show_row_name);
            view.append_column(&col);
        }
        // Data columns.
        for (i, col_path) in cols.iter().enumerate() {
            let col_name =
                Path::basename(col_path).unwrap_or("?");
            let tvc = TreeViewColumn::new();
            tvc.set_title(col_name);
            tvc.set_resizable(true);
            tvc.set_sizing(gtk::TreeViewColumnSizing::Fixed);
            tvc.set_fixed_width(100);
            let cell = CellRendererText::new();
            gtk::prelude::CellLayoutExt::pack_start(&tvc, &cell, true);
            gtk::prelude::CellLayoutExt::add_attribute(
                &tvc, &cell, "text", (i + 1) as i32,
            );
            view.append_column(&tvc);
        }
        self.root.add(&view);
        if self.visible.get() {
            view.show_all();
        }
        let (tx, rx) = mpsc::channel(3);
        let raeified = RaeifiedState {
            descriptor: TableDescriptor { path, rows, cols },
            store,
            view: view.clone(),
            by_id: FxHashMap::default(),
            subscribed: HashSet::new(),
            rx,
            tx,
        };
        *self.state.borrow_mut() = TableState::Raeified(raeified);
        // Wire up scroll events to trigger subscription updates.
        let state = Rc::downgrade(&self.state);
        let ctx = self.ctx.clone();
        self.root.vadjustment().connect_value_changed(
            move |_| {
                let state = state.clone();
                let ctx = ctx.clone();
                idle_add_local(move || {
                    if let Some(state) = state.upgrade() {
                        if let TableState::Raeified(ref mut r) =
                            *state.borrow_mut()
                        {
                            Self::update_subscriptions_inner(r, &ctx);
                        }
                    }
                    ControlFlow::Break
                });
            },
        );
        // Do initial subscription update.
        let state_ref = self.state.clone();
        let ctx = self.ctx.clone();
        idle_add_local(move || {
            if let TableState::Raeified(ref mut r) =
                *state_ref.borrow_mut()
            {
                Self::update_subscriptions_inner(r, &ctx);
            }
            ControlFlow::Break
        });
        // Start the update pump: drain subscription updates and apply
        // them to the store.
        self.start_update_pump();
    }

    /// Subscribe to the cells that are currently visible in the
    /// TreeView, and unsubscribe from cells that have scrolled out of
    /// view.
    fn update_subscriptions_inner(r: &mut RaeifiedState, ctx: &BCtx) {
        if !r.view.is_visible() {
            return;
        }
        let (mut start, mut end) = match r.view.visible_range() {
            Some((s, e)) => (s, e),
            None => {
                if !r.descriptor.rows.is_empty() {
                    // TreeView not yet laid out; try again later.
                    return;
                }
                return;
            }
        };
        // Extend the visible range by a buffer of 50 rows on each side.
        for _ in 0..50 {
            start.prev();
            end.next();
        }
        // Unsubscribe rows that have scrolled out of the visible area.
        r.by_id.retain(|_, cell| {
            match r.store.path(&cell.row) {
                None => false,
                Some(p) => p >= start && p <= end,
            }
        });
        // Rebuild the subscribed set from the retained subscriptions.
        r.subscribed.clear();
        for cell in r.by_id.values() {
            if let Some(p) = r.store.path(&cell.row) {
                if let Some(row_idx) = p.indices().first() {
                    r.subscribed.insert((*row_idx as usize, cell.col));
                }
            }
        }
        let subscriber = ctx.borrow().backend.subscriber.clone();
        let n_cols = r.descriptor.cols.len() as u32;
        // Determine the row index range from the TreePaths.
        let start_idx = start.indices().first()
            .copied().unwrap_or(0).max(0) as usize;
        let end_idx = end.indices().first()
            .copied().unwrap_or(0).max(0) as usize;
        // Subscribe to visible cells that are not yet subscribed.
        for row_idx in start_idx..=end_idx {
            if row_idx >= r.descriptor.rows.len() {
                break;
            }
            let tp = gtk::TreePath::from_indicesv(&[row_idx as i32]);
            let iter = match r.store.iter(&tp) {
                Some(it) => it,
                None => continue,
            };
            let row_path = &r.descriptor.rows[row_idx];
            for col_idx in 0..n_cols {
                if !r.subscribed.contains(&(row_idx, col_idx + 1)) {
                    r.subscribed.insert((row_idx, col_idx + 1));
                    let col_path = &r.descriptor.cols[col_idx as usize];
                    let col_name = Path::basename(col_path).unwrap_or("?");
                    let cell_path = row_path.append(col_name);
                    let sub = subscriber.subscribe_updates(
                        cell_path,
                        [(UpdatesFlags::BEGIN_WITH_LAST, r.tx.clone())],
                    );
                    let sub_id = sub.id();
                    r.by_id.insert(sub_id, CellSub {
                        _sub: sub,
                        row: iter.clone(),
                        col: col_idx + 1,
                    });
                }
            }
        }
    }

    /// Spawn an idle handler that drains the subscription update
    /// channel and applies values to the ListStore.
    fn start_update_pump(&self) {
        let state = Rc::downgrade(&self.state);
        idle_add_local(move || {
            let state = match state.upgrade() {
                Some(s) => s,
                None => return ControlFlow::Break,
            };
            let mut borrow = state.borrow_mut();
            let r = match *borrow {
                TableState::Raeified(ref mut r) => r,
                _ => return ControlFlow::Break,
            };
            // Drain all available batches without blocking.
            loop {
                match r.rx.try_recv() {
                    Ok(batch) => {
                        for (sub_id, event) in batch.iter() {
                            if let Event::Update(value) = event {
                                if let Some(cell) = r.by_id.get(sub_id) {
                                    let display =
                                        format!("{}", WVal(value));
                                    r.store.set_value(
                                        &cell.row,
                                        cell.col,
                                        &display.to_value(),
                                    );
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            ControlFlow::Continue
        });
    }

    /// Handle a path property update: if the path changed, request
    /// new table resolution.
    fn handle_path_update(&mut self, value: &Value) {
        let new_path = match value {
            Value::String(s) => Path::from(String::from(&**s)),
            _ => return,
        };
        let should_resolve = match &*self.state.borrow() {
            TableState::Resolving(p) => *p != new_path,
            TableState::Raeified(r) => r.descriptor.path != new_path,
        };
        if should_resolve {
            *self.state.borrow_mut() = TableState::Resolving(new_path.clone());
            self.ctx.borrow().backend.resolve_table(new_path);
        }
    }

    /// Handle a refresh property update: re-resolve the current path.
    fn handle_refresh(&mut self) {
        let current_path = match &*self.state.borrow() {
            TableState::Resolving(p) => p.clone(),
            TableState::Raeified(r) => r.descriptor.path.clone(),
        };
        *self.state.borrow_mut() = TableState::Resolving(current_path.clone());
        self.ctx.borrow().backend.resolve_table(current_path);
    }
}

impl BWidget for Table {
    fn update(&mut self, id: ExprId, value: &Value) -> bool {
        let mut handled = false;
        if let Some(ref mut p) = self.path {
            if p.update(id, value) {
                self.handle_path_update(value);
                handled = true;
            }
        }
        if let Some(ref mut p) = self.refresh {
            if p.update(id, value) {
                self.handle_refresh();
                handled = true;
            }
        }
        // Dispatch updates to all property GxProps. These are stubs
        // for now -- the values are tracked but not yet acted upon
        // (except path and refresh above).
        if let Some(ref mut p) = self.sort_mode {
            if p.update(id, value) {
                handled = true;
                // TODO: apply sort mode changes
            }
        }
        if let Some(ref mut p) = self.column_filter {
            if p.update(id, value) {
                handled = true;
                // TODO: apply column filter changes
            }
        }
        if let Some(ref mut p) = self.row_filter {
            if p.update(id, value) {
                handled = true;
                // TODO: apply row filter changes
            }
        }
        if let Some(ref mut p) = self.column_editable {
            if p.update(id, value) {
                handled = true;
                // TODO: apply column editable changes
            }
        }
        if let Some(ref mut p) = self.column_widths {
            if p.update(id, value) {
                handled = true;
                // TODO: apply column width changes
            }
        }
        if let Some(ref mut p) = self.columns_resizable {
            if p.update(id, value) {
                handled = true;
                // TODO: apply columns resizable changes
            }
        }
        if let Some(ref mut p) = self.column_types {
            if p.update(id, value) {
                handled = true;
                // TODO: apply column type changes
            }
        }
        if let Some(ref mut p) = self.selection_mode {
            if p.update(id, value) {
                handled = true;
                // TODO: apply selection mode changes
            }
        }
        if let Some(ref mut p) = self.selection {
            if p.update(id, value) {
                handled = true;
                // TODO: apply selection changes
            }
        }
        if let Some(ref mut p) = self.show_row_name {
            if p.update(id, value) {
                handled = true;
                // TODO: apply show_row_name changes
            }
        }
        if let Some(ref mut p) = self.on_select {
            if p.update(id, value) {
                handled = true;
            }
        }
        if let Some(ref mut p) = self.on_activate {
            if p.update(id, value) {
                handled = true;
            }
        }
        if let Some(ref mut p) = self.on_edit {
            if p.update(id, value) {
                handled = true;
            }
        }
        if let Some(ref mut p) = self.on_header_click {
            if p.update(id, value) {
                handled = true;
            }
        }
        handled
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.root.upcast_ref())
    }

    fn table_resolved(
        &mut self,
        path: &Path,
        table: &resolver::Table,
    ) -> bool {
        self.apply_table_resolved(path.clone(), table.clone());
        true
    }

    fn set_visible(&self, v: bool) {
        self.visible.set(v);
        self.root.set_visible(v);
        if v {
            let state = self.state.clone();
            let ctx = self.ctx.clone();
            idle_add_local(move || {
                if let TableState::Raeified(ref mut r) =
                    *state.borrow_mut()
                {
                    r.view.show();
                    Self::update_subscriptions_inner(r, &ctx);
                }
                ControlFlow::Break
            });
        } else if let TableState::Raeified(ref r) = *self.state.borrow() {
            r.view.hide();
        }
    }
}
