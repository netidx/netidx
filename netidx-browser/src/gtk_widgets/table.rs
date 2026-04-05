use super::{CompileCtx, GtkW, GtkWidget};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use fxhash::FxHashMap;
use futures::channel::mpsc;
use glib::{self, ControlFlow};
use graphix_compiler::expr::ExprId;
use graphix_rt::{Callable, GXExt, Ref, TRef};
use gtk::{
    self, prelude::*,
    Adjustment, CellRendererText, CellRendererToggle, CellRendererCombo,
    CellRendererSpin, CellRendererProgress, ListStore, ScrolledWindow,
    SortColumn, TreeModel, TreeView, TreeViewColumn,
};
use netidx::{
    path::Path,
    protocol::valarray::ValArray,
    publisher::Value,
    subscriber::{Dval, Event, SubId, UpdatesFlags},
};
use regex::Regex;
use std::{
    cell::RefCell,
    collections::HashSet,
    rc::Rc,
};

// ── Sort / filter enums ────────────────────────────────────────────

enum SortMode {
    /// No sorting active.
    None,
    /// Sorting UI disabled (columns not clickable/sortable).
    Disabled,
    /// Sort by a named column in the given direction.
    Column { name: String, ascending: bool },
    /// External sort: show sort indicators but don't sort internally.
    External { name: String, ascending: bool },
}

enum ColumnFilter {
    All,
    None,
    Include(Vec<String>),
    Exclude(Vec<String>),
    IncludeMatch(Vec<Regex>),
    ExcludeMatch(Vec<Regex>),
    KeepRange(i64, i64),
    DropRange(i64, i64),
}

enum RowFilter {
    All,
    None,
    Include(Vec<String>),
    Exclude(Vec<String>),
    IncludeMatch(Vec<Regex>),
    ExcludeMatch(Vec<Regex>),
    KeepRange(i64, i64),
    DropRange(i64, i64),
}

enum ColumnType {
    Text,
    Toggle,
    Combo { choices: Vec<(String, String)> },
    Spin { min: f64, max: f64, increment: f64 },
    Progress,
    Hidden,
}

// ── Parsing helpers ────────────────────────────────────────────────

fn parse_sort_mode(v: &Value) -> SortMode {
    let (tag, payload) = match v.clone().cast_to::<(ArcStr, Value)>() {
        Ok(t) => t,
        Err(_) => return SortMode::None,
    };
    match tag.as_str() {
        "None" => SortMode::None,
        "Disabled" => SortMode::Disabled,
        "Column" | "External" => {
            // struct {direction: SortDirection, name: String}
            // fields sorted alphabetically: direction, name
            let (dir_val, name) = match payload
                .cast_to::<[(ArcStr, Value); 2]>()
            {
                Ok([(_, d), (_, n)]) => {
                    let name = n.cast_to::<String>().unwrap_or_default();
                    (d, name)
                }
                Err(_) => return SortMode::None,
            };
            let ascending = match dir_val.cast_to::<(ArcStr, Value)>() {
                Ok((dir_tag, _)) => dir_tag.as_str() == "Ascending",
                Err(_) => true,
            };
            if tag.as_str() == "Column" {
                SortMode::Column { name, ascending }
            } else {
                SortMode::External { name, ascending }
            }
        }
        _ => SortMode::None,
    }
}

fn parse_string_list(v: &Value) -> Vec<String> {
    v.clone().cast_to::<Vec<String>>().unwrap_or_default()
}

fn parse_regex_list(v: &Value) -> Vec<Regex> {
    let strings = parse_string_list(v);
    strings
        .iter()
        .filter_map(|s| Regex::new(s).ok())
        .collect()
}

fn parse_range(v: &Value) -> (i64, i64) {
    // struct {end: i64, start: i64} — fields sorted: end, start
    match v.clone().cast_to::<[(ArcStr, Value); 2]>() {
        Ok([(_, end_val), (_, start_val)]) => {
            let start = start_val.cast_to::<i64>().unwrap_or(0);
            let end = end_val.cast_to::<i64>().unwrap_or(0);
            (start, end)
        }
        Err(_) => (0, 0),
    }
}

fn parse_column_filter(v: &Value) -> ColumnFilter {
    let (tag, payload) = match v.clone().cast_to::<(ArcStr, Value)>() {
        Ok(t) => t,
        Err(_) => return ColumnFilter::All,
    };
    match tag.as_str() {
        "All" => ColumnFilter::All,
        "None" => ColumnFilter::None,
        "Include" => ColumnFilter::Include(parse_string_list(&payload)),
        "Exclude" => ColumnFilter::Exclude(parse_string_list(&payload)),
        "IncludeMatch" => ColumnFilter::IncludeMatch(parse_regex_list(&payload)),
        "ExcludeMatch" => ColumnFilter::ExcludeMatch(parse_regex_list(&payload)),
        "KeepRange" => {
            let (start, end) = parse_range(&payload);
            ColumnFilter::KeepRange(start, end)
        }
        "DropRange" => {
            let (start, end) = parse_range(&payload);
            ColumnFilter::DropRange(start, end)
        }
        _ => ColumnFilter::All,
    }
}

fn parse_row_filter(v: &Value) -> RowFilter {
    let (tag, payload) = match v.clone().cast_to::<(ArcStr, Value)>() {
        Ok(t) => t,
        Err(_) => return RowFilter::All,
    };
    match tag.as_str() {
        "All" => RowFilter::All,
        "None" => RowFilter::None,
        "Include" => RowFilter::Include(parse_string_list(&payload)),
        "Exclude" => RowFilter::Exclude(parse_string_list(&payload)),
        "IncludeMatch" => RowFilter::IncludeMatch(parse_regex_list(&payload)),
        "ExcludeMatch" => RowFilter::ExcludeMatch(parse_regex_list(&payload)),
        "KeepRange" => {
            let (start, end) = parse_range(&payload);
            RowFilter::KeepRange(start, end)
        }
        "DropRange" => {
            let (start, end) = parse_range(&payload);
            RowFilter::DropRange(start, end)
        }
        _ => RowFilter::All,
    }
}

fn parse_column_type(v: &Value) -> ColumnType {
    let (tag, payload) = match v.clone().cast_to::<(ArcStr, Value)>() {
        Ok(t) => t,
        Err(_) => return ColumnType::Text,
    };
    match tag.as_str() {
        "Text" => ColumnType::Text,
        "Toggle" => ColumnType::Toggle,
        "Progress" => ColumnType::Progress,
        "Hidden" => ColumnType::Hidden,
        "Combo" => {
            // payload is struct { choices: array<{id: string, label: string}> }
            // fields sorted: choices
            let choices_val = match payload.clone().cast_to::<[(ArcStr, Value); 1]>() {
                Ok([(_, c)]) => c,
                Err(_) => return ColumnType::Text,
            };
            let items = choices_val
                .clone()
                .cast_to::<Vec<Value>>()
                .unwrap_or_default();
            let mut choices = Vec::new();
            for item in &items {
                // each item is struct { id: string, label: string }
                // fields sorted: id, label
                if let Ok([(_, id_val), (_, label_val)]) =
                    item.clone().cast_to::<[(ArcStr, Value); 2]>()
                {
                    let id = id_val.cast_to::<String>().unwrap_or_default();
                    let label = label_val.cast_to::<String>().unwrap_or_default();
                    choices.push((id, label));
                }
            }
            ColumnType::Combo { choices }
        }
        "Spin" => {
            // payload is struct { increment: f64, max: f64, min: f64 }
            // fields sorted: increment, max, min
            if let Ok([(_, inc_val), (_, max_val), (_, min_val)]) =
                payload.cast_to::<[(ArcStr, Value); 3]>()
            {
                let increment = inc_val.cast_to::<f64>().unwrap_or(1.0);
                let max = max_val.cast_to::<f64>().unwrap_or(100.0);
                let min = min_val.cast_to::<f64>().unwrap_or(0.0);
                ColumnType::Spin { min, max, increment }
            } else {
                ColumnType::Text
            }
        }
        _ => ColumnType::Text,
    }
}

fn parse_column_specs(v: &Value) -> Option<FxHashMap<String, ColumnType>> {
    // v is either null or an array of ColumnSpec
    // ColumnSpec is struct { name: string, typ: ColumnType }
    // fields sorted: name, typ
    let items = v.clone().cast_to::<Vec<Value>>().ok()?;
    let mut map = FxHashMap::default();
    for item in &items {
        if let Ok([(_, name_val), (_, typ_val)]) =
            item.clone().cast_to::<[(ArcStr, Value); 2]>()
        {
            let name = name_val.cast_to::<String>().unwrap_or_default();
            let typ = parse_column_type(&typ_val);
            map.insert(name, typ);
        }
    }
    if map.is_empty() { None } else { Some(map) }
}

fn parse_column_widths(v: &Value) -> Option<FxHashMap<String, i64>> {
    // v is either null or map<string, i64>
    let items = v.clone().cast_to::<Vec<(Value, Value)>>().ok()?;
    let mut map = FxHashMap::default();
    for (k, val) in &items {
        let name = k.clone().cast_to::<String>().unwrap_or_default();
        let width = val.clone().cast_to::<i64>().unwrap_or(100);
        map.insert(name, width);
    }
    if map.is_empty() { None } else { Some(map) }
}

// ── Apply helpers ──────────────────────────────────────────────────

fn find_column_index(cols: &[Path], name: &str) -> Option<usize> {
    cols.iter().position(|p| Path::basename(p).unwrap_or("") == name)
}

fn apply_sort_mode(
    store: &ListStore,
    view: &TreeView,
    cols: &[Path],
    mode: &SortMode,
) {
    match mode {
        SortMode::None => {
            store.set_unsorted();
            for (i, _) in cols.iter().enumerate() {
                if let Some(tvc) = view.column((i + 1) as i32) {
                    tvc.set_sort_indicator(false);
                }
            }
        }
        SortMode::Disabled => {
            store.set_unsorted();
            for (i, _) in cols.iter().enumerate() {
                if let Some(tvc) = view.column((i + 1) as i32) {
                    tvc.set_clickable(false);
                    tvc.set_sort_indicator(false);
                }
            }
        }
        SortMode::Column { name, ascending } => {
            let order = if *ascending {
                gtk::SortType::Ascending
            } else {
                gtk::SortType::Descending
            };
            if let Some(col_idx) = find_column_index(cols, name) {
                let store_col = (col_idx + 1) as u32;
                let sc = SortColumn::Index(store_col);
                // Set a compare function that does string comparison
                // on the store column.
                let col_i = store_col;
                store.set_sort_func(sc, move |model, a, b| {
                    let va: String =
                        model.value(a, col_i as i32).get().unwrap_or_default();
                    let vb: String =
                        model.value(b, col_i as i32).get().unwrap_or_default();
                    va.cmp(&vb)
                });
                store.set_sort_column_id(sc, order);
                // Update sort indicators on columns.
                for (i, _) in cols.iter().enumerate() {
                    if let Some(tvc) = view.column((i + 1) as i32) {
                        if i == col_idx {
                            tvc.set_sort_indicator(true);
                            tvc.set_sort_order(order);
                        } else {
                            tvc.set_sort_indicator(false);
                        }
                    }
                }
            }
        }
        SortMode::External { name, ascending } => {
            // Don't actually sort the store; just show indicators.
            store.set_unsorted();
            let order = if *ascending {
                gtk::SortType::Ascending
            } else {
                gtk::SortType::Descending
            };
            for (i, col_path) in cols.iter().enumerate() {
                if let Some(tvc) = view.column((i + 1) as i32) {
                    let col_name = Path::basename(col_path).unwrap_or("");
                    if col_name == name.as_str() {
                        tvc.set_sort_indicator(true);
                        tvc.set_sort_order(order);
                    } else {
                        tvc.set_sort_indicator(false);
                    }
                }
            }
        }
    }
}

fn col_name_matches(col_path: &Path, filter: &ColumnFilter, idx: usize) -> bool {
    let col_name = Path::basename(col_path).unwrap_or("");
    match filter {
        ColumnFilter::All => true,
        ColumnFilter::None => false,
        ColumnFilter::Include(names) => {
            names.iter().any(|n| n.as_str() == col_name)
        }
        ColumnFilter::Exclude(names) => {
            !names.iter().any(|n| n.as_str() == col_name)
        }
        ColumnFilter::IncludeMatch(patterns) => {
            patterns.iter().any(|r| r.is_match(col_name))
        }
        ColumnFilter::ExcludeMatch(patterns) => {
            !patterns.iter().any(|r| r.is_match(col_name))
        }
        ColumnFilter::KeepRange(start, end) => {
            let i = idx as i64;
            i >= *start && i < *end
        }
        ColumnFilter::DropRange(start, end) => {
            let i = idx as i64;
            !(i >= *start && i < *end)
        }
    }
}

fn apply_column_filter(view: &TreeView, cols: &[Path], filter: &ColumnFilter) {
    for (i, col_path) in cols.iter().enumerate() {
        let visible = col_name_matches(col_path, filter, i);
        // Column 0 is the row name; data columns start at 1.
        if let Some(tvc) = view.column((i + 1) as i32) {
            tvc.set_visible(visible);
        }
    }
}

fn row_matches_filter(
    row_path: &Path,
    idx: usize,
    filter: &RowFilter,
) -> bool {
    let row_name = Path::basename(row_path).unwrap_or("");
    match filter {
        RowFilter::All => true,
        RowFilter::None => false,
        RowFilter::Include(names) => {
            names.iter().any(|n| n.as_str() == row_name)
        }
        RowFilter::Exclude(names) => {
            !names.iter().any(|n| n.as_str() == row_name)
        }
        RowFilter::IncludeMatch(patterns) => {
            patterns.iter().any(|r| r.is_match(row_name))
        }
        RowFilter::ExcludeMatch(patterns) => {
            !patterns.iter().any(|r| r.is_match(row_name))
        }
        RowFilter::KeepRange(start, end) => {
            let i = idx as i64;
            i >= *start && i < *end
        }
        RowFilter::DropRange(start, end) => {
            let i = idx as i64;
            !(i >= *start && i < *end)
        }
    }
}

/// A subscription to a single table cell.
struct CellSub {
    _sub: Dval,
    row: gtk::TreeIter,
    col: u32,
}

/// Descriptor of a resolved table from the resolver.
struct TableDescriptor {
    #[allow(dead_code)]
    path: Path,
    /// The rows currently displayed (after row filter).
    rows: Vec<Path>,
    /// All rows from the resolver (before filtering).
    all_rows: Vec<Path>,
    cols: Vec<Path>,
}

/// Live table state shared between the widget and the idle update pump.
struct TableInner {
    descriptor: TableDescriptor,
    store: ListStore,
    view: TreeView,
    by_id: FxHashMap<SubId, CellSub>,
    subscribed: HashSet<(usize, u32)>,
    rx: mpsc::Receiver<netidx::pool::global::GPooled<Vec<(SubId, Event)>>>,
    tx: mpsc::Sender<netidx::pool::global::GPooled<Vec<(SubId, Event)>>>,
}

enum TableState {
    /// No path has been set yet, or resolution hasn't completed.
    Empty,
    /// Table is live and showing data.
    Live(Rc<RefCell<TableInner>>),
}

pub(crate) struct TableW<X: GXExt> {
    ctx: CompileCtx<X>,
    root: ScrolledWindow,
    path: TRef<X, String>,
    sort_mode: Ref<X>,
    column_filter: Ref<X>,
    column_types: Ref<X>,
    column_widths: Ref<X>,
    row_filter: Ref<X>,
    selection_mode: TRef<X, String>,
    show_row_name: TRef<X, bool>,
    on_select: Ref<X>,
    on_select_callable: Option<Callable<X>>,
    on_activate: Ref<X>,
    on_activate_callable: Option<Callable<X>>,
    on_edit: Ref<X>,
    on_edit_callable: Option<Callable<X>>,
    on_header_click: Ref<X>,
    on_header_click_callable: Option<Callable<X>>,
    state: TableState,
    select_signal: Option<glib::SignalHandlerId>,
    activate_signal: Option<glib::SignalHandlerId>,
}

impl<X: GXExt> TableW<X> {
    pub(crate) async fn compile(
        ctx: CompileCtx<X>,
        source: Value,
    ) -> Result<GtkW<X>> {
        // Fields arrive in alphabetical order:
        // column_filter, column_types, column_widths, on_activate,
        // on_edit, on_header_click, on_select, path, row_filter,
        // selection_mode, show_row_name, sort_mode
        let [
            (_, column_filter),
            (_, column_types),
            (_, column_widths),
            (_, on_activate),
            (_, on_edit),
            (_, on_header_click),
            (_, on_select),
            (_, path),
            (_, row_filter),
            (_, selection_mode),
            (_, show_row_name),
            (_, sort_mode),
        ] = source
            .cast_to::<[(ArcStr, u64); 12]>()
            .context("table flds")?;
        let (
            column_filter,
            column_types,
            column_widths,
            on_activate,
            on_edit,
            on_header_click,
            on_select,
            path,
            row_filter,
            selection_mode,
            show_row_name,
            sort_mode,
        ) = tokio::try_join! {
            ctx.gx.compile_ref(column_filter),
            ctx.gx.compile_ref(column_types),
            ctx.gx.compile_ref(column_widths),
            ctx.gx.compile_ref(on_activate),
            ctx.gx.compile_ref(on_edit),
            ctx.gx.compile_ref(on_header_click),
            ctx.gx.compile_ref(on_select),
            ctx.gx.compile_ref(path),
            ctx.gx.compile_ref(row_filter),
            ctx.gx.compile_ref(selection_mode),
            ctx.gx.compile_ref(show_row_name),
            ctx.gx.compile_ref(sort_mode),
        }?;
        let path: TRef<X, String> =
            TRef::new(path).context("table tref path")?;
        let selection_mode: TRef<X, String> =
            TRef::new(selection_mode).context("table tref selection_mode")?;
        let show_row_name: TRef<X, bool> =
            TRef::new(show_row_name).context("table tref show_row_name")?;
        let on_select_callable =
            compile_callable!(ctx.gx, on_select, "table on_select");
        let on_activate_callable =
            compile_callable!(ctx.gx, on_activate, "table on_activate");
        let on_edit_callable =
            compile_callable!(ctx.gx, on_edit, "table on_edit");
        let on_header_click_callable =
            compile_callable!(ctx.gx, on_header_click, "table on_header_click");
        let root = ScrolledWindow::new(
            None::<&gtk::Adjustment>,
            None::<&gtk::Adjustment>,
        );
        let col_types_map = column_types.last.as_ref()
            .and_then(|v| parse_column_specs(v));
        let col_widths_map = column_widths.last.as_ref()
            .and_then(|v| parse_column_widths(v));
        // If path is already known, resolve the table immediately.
        let state = match path.t.as_deref() {
            Some(p) if !p.is_empty() => {
                let table_path = Path::from(String::from(p));
                match ctx.subscriber.resolver().table(table_path.clone()).await
                {
                    Ok(table) => {
                        let all_rows: Vec<Path> =
                            table.rows.iter().cloned().collect();
                        let cols: Vec<Path> =
                            table.cols.iter().map(|(p, _)| p.clone()).collect();
                        let inner = build_table(
                            &ctx,
                            &root,
                            table_path,
                            all_rows,
                            cols,
                            show_row_name.t.unwrap_or(true),
                            &RowFilter::All,
                            &col_types_map,
                            &col_widths_map,
                            &on_select_callable,
                            &on_activate_callable,
                            &on_edit_callable,
                            &on_header_click_callable,
                        );
                        TableState::Live(inner)
                    }
                    Err(e) => {
                        log::warn!("table resolve failed: {}", e);
                        TableState::Empty
                    }
                }
            }
            _ => TableState::Empty,
        };
        let (select_signal, activate_signal) = match &state {
            TableState::Live(inner) => {
                let borrow = inner.borrow();
                let ss = connect_on_select(
                    &borrow.view, &ctx.gx, &on_select_callable,
                );
                let as_ = connect_on_activate(
                    &borrow.view, &ctx.gx, &on_activate_callable,
                    &borrow.descriptor.path,
                );
                (ss, as_)
            }
            TableState::Empty => (None, None),
        };
        root.show_all();
        Ok(Box::new(TableW {
            ctx,
            root,
            path,
            sort_mode,
            column_filter,
            column_types,
            column_widths,
            row_filter,
            selection_mode,
            show_row_name,
            on_select,
            on_select_callable,
            on_activate,
            on_activate_callable,
            on_edit,
            on_edit_callable,
            on_header_click,
            on_header_click_callable,
            state,
            select_signal,
            activate_signal,
        }))
    }

    /// Resolve and build a new table for the given path.
    fn resolve_and_build(&mut self, rt: &tokio::runtime::Handle, path_str: &str) {
        if path_str.is_empty() {
            return;
        }
        let table_path = Path::from(String::from(path_str));
        let subscriber = self.ctx.subscriber.clone();
        let table = match rt.block_on(subscriber.resolver().table(table_path.clone())) {
            Ok(t) => t,
            Err(e) => {
                log::warn!("table resolve failed: {}", e);
                return;
            }
        };
        // Remove old content.
        if let Some(c) = self.root.child() {
            self.root.remove(&c);
        }
        if let Some(sig) = self.select_signal.take() {
            if let TableState::Live(ref inner) = self.state {
                inner.borrow().view.disconnect(sig);
            }
        }
        if let Some(sig) = self.activate_signal.take() {
            if let TableState::Live(ref inner) = self.state {
                inner.borrow().view.disconnect(sig);
            }
        }
        let all_rows: Vec<Path> = table.rows.iter().cloned().collect();
        let cols: Vec<Path> =
            table.cols.iter().map(|(p, _)| p.clone()).collect();
        let row_filter = self.row_filter.last.as_ref()
            .map(|v| parse_row_filter(v))
            .unwrap_or(RowFilter::All);
        let col_types_map = self.column_types.last.as_ref()
            .and_then(|v| parse_column_specs(v));
        let col_widths_map = self.column_widths.last.as_ref()
            .and_then(|v| parse_column_widths(v));
        let inner = build_table(
            &self.ctx,
            &self.root,
            table_path,
            all_rows,
            cols,
            self.show_row_name.t.unwrap_or(true),
            &row_filter,
            &col_types_map,
            &col_widths_map,
            &self.on_select_callable,
            &self.on_activate_callable,
            &self.on_edit_callable,
            &self.on_header_click_callable,
        );
        {
            let borrow = inner.borrow();
            self.select_signal = connect_on_select(
                &borrow.view, &self.ctx.gx, &self.on_select_callable,
            );
            self.activate_signal = connect_on_activate(
                &borrow.view, &self.ctx.gx, &self.on_activate_callable,
                &borrow.descriptor.path,
            );
        }
        self.state = TableState::Live(inner);
        self.root.show_all();
    }

    /// Rebuild the table from the existing descriptor with the current
    /// settings. Reuses the resolved path, all_rows, and cols without
    /// hitting the resolver again.
    fn rebuild_table(&mut self) {
        let (path, all_rows, cols) = match &self.state {
            TableState::Live(inner) => {
                let b = inner.borrow();
                (
                    b.descriptor.path.clone(),
                    b.descriptor.all_rows.clone(),
                    b.descriptor.cols.clone(),
                )
            }
            TableState::Empty => return,
        };
        // Tear down old signals.
        if let Some(sig) = self.select_signal.take() {
            if let TableState::Live(ref inner) = self.state {
                inner.borrow().view.selection().disconnect(sig);
            }
        }
        if let Some(sig) = self.activate_signal.take() {
            if let TableState::Live(ref inner) = self.state {
                inner.borrow().view.disconnect(sig);
            }
        }
        // Remove old content.
        if let Some(c) = self.root.child() {
            self.root.remove(&c);
        }
        let row_filter = self.row_filter.last.as_ref()
            .map(|v| parse_row_filter(v))
            .unwrap_or(RowFilter::All);
        let col_types_map = self.column_types.last.as_ref()
            .and_then(|v| parse_column_specs(v));
        let col_widths_map = self.column_widths.last.as_ref()
            .and_then(|v| parse_column_widths(v));
        let inner = build_table(
            &self.ctx,
            &self.root,
            path,
            all_rows,
            cols,
            self.show_row_name.t.unwrap_or(true),
            &row_filter,
            &col_types_map,
            &col_widths_map,
            &self.on_select_callable,
            &self.on_activate_callable,
            &self.on_edit_callable,
            &self.on_header_click_callable,
        );
        {
            let borrow = inner.borrow();
            self.select_signal = connect_on_select(
                &borrow.view, &self.ctx.gx, &self.on_select_callable,
            );
            self.activate_signal = connect_on_activate(
                &borrow.view, &self.ctx.gx, &self.on_activate_callable,
                &borrow.descriptor.path,
            );
        }
        self.state = TableState::Live(inner);
        self.root.show_all();
    }

    /// Rebuild the table from the existing descriptor with a new row
    /// filter. Reuses the resolved path, all_rows, and cols without
    /// hitting the resolver again.
    fn rebuild_with_row_filter(&mut self, filter: &RowFilter) {
        let (path, all_rows, cols) = match &self.state {
            TableState::Live(inner) => {
                let b = inner.borrow();
                (
                    b.descriptor.path.clone(),
                    b.descriptor.all_rows.clone(),
                    b.descriptor.cols.clone(),
                )
            }
            TableState::Empty => return,
        };
        // Tear down old signals.
        if let Some(sig) = self.select_signal.take() {
            if let TableState::Live(ref inner) = self.state {
                inner.borrow().view.selection().disconnect(sig);
            }
        }
        if let Some(sig) = self.activate_signal.take() {
            if let TableState::Live(ref inner) = self.state {
                inner.borrow().view.disconnect(sig);
            }
        }
        // Remove old content.
        if let Some(c) = self.root.child() {
            self.root.remove(&c);
        }
        let col_types_map = self.column_types.last.as_ref()
            .and_then(|v| parse_column_specs(v));
        let col_widths_map = self.column_widths.last.as_ref()
            .and_then(|v| parse_column_widths(v));
        let inner = build_table(
            &self.ctx,
            &self.root,
            path,
            all_rows,
            cols,
            self.show_row_name.t.unwrap_or(true),
            filter,
            &col_types_map,
            &col_widths_map,
            &self.on_select_callable,
            &self.on_activate_callable,
            &self.on_edit_callable,
            &self.on_header_click_callable,
        );
        {
            let borrow = inner.borrow();
            self.select_signal = connect_on_select(
                &borrow.view, &self.ctx.gx, &self.on_select_callable,
            );
            self.activate_signal = connect_on_activate(
                &borrow.view, &self.ctx.gx, &self.on_activate_callable,
                &borrow.descriptor.path,
            );
        }
        self.state = TableState::Live(inner);
        self.root.show_all();
    }
}

/// Build the TreeView, ListStore, subscribe visible cells, and start
/// the idle update pump. Returns the shared inner state.
fn build_table<X: GXExt>(
    ctx: &CompileCtx<X>,
    root: &ScrolledWindow,
    path: Path,
    all_rows: Vec<Path>,
    cols: Vec<Path>,
    show_row_name: bool,
    row_filter: &RowFilter,
    col_types_map: &Option<FxHashMap<String, ColumnType>>,
    col_widths_map: &Option<FxHashMap<String, i64>>,
    _on_select: &Option<Callable<X>>,
    _on_activate: &Option<Callable<X>>,
    on_edit: &Option<Callable<X>>,
    on_header_click: &Option<Callable<X>>,
) -> Rc<RefCell<TableInner>> {
    let rows: Vec<Path> = all_rows
        .iter()
        .enumerate()
        .filter(|(i, p)| row_matches_filter(p, *i, row_filter))
        .map(|(_, p)| p.clone())
        .collect();
    let n_data_cols = cols.len();
    let n_store_cols = 1 + n_data_cols;
    let col_types: Vec<glib::Type> =
        (0..n_store_cols).map(|_| glib::Type::STRING).collect();
    let store = ListStore::new(&col_types);
    for row_path in &rows {
        let row_name = Path::basename(row_path).unwrap_or("");
        let iter = store.append();
        store.set_value(&iter, 0, &row_name.to_value());
    }
    let view = TreeView::with_model(&store);
    view.set_fixed_height_mode(true);
    view.set_headers_visible(true);
    // Row name column.
    {
        let col = TreeViewColumn::new();
        col.set_title("row");
        col.set_resizable(true);
        col.set_sizing(gtk::TreeViewColumnSizing::Fixed);
        col.set_fixed_width(120);
        let cell = CellRendererText::new();
        CellLayoutExt::pack_start(&col, &cell, true);
        CellLayoutExt::add_attribute(&col, &cell, "text", 0);
        col.set_visible(show_row_name);
        view.append_column(&col);
    }
    // Data columns.
    let gx_for_header = ctx.gx.clone();
    let header_callable_id = on_header_click.as_ref().map(|c| c.id());
    let gx_for_edit = ctx.gx.clone();
    let edit_callable_id = on_edit.as_ref().map(|c| c.id());
    for (i, col_path) in cols.iter().enumerate() {
        let col_name = Path::basename(col_path).unwrap_or("?");
        let col_type = col_types_map.as_ref()
            .and_then(|m| m.get(col_name));
        let tvc = TreeViewColumn::new();
        tvc.set_title(col_name);
        tvc.set_resizable(true);
        tvc.set_sizing(gtk::TreeViewColumnSizing::Fixed);
        // Apply column width.
        let width = col_widths_map.as_ref()
            .and_then(|m| m.get(col_name).copied())
            .unwrap_or(100) as i32;
        tvc.set_fixed_width(width);
        let store_col = (i + 1) as i32;
        match col_type {
            Some(ColumnType::Toggle) => {
                let cell = CellRendererToggle::new();
                CellLayoutExt::pack_start(&tvc, &cell, true);
                // Use a cell data function to read the string and
                // set the active property.
                let store_ref = store.clone();
                TreeViewColumnExt::set_cell_data_func(
                    &tvc, &cell,
                    Some(Box::new(move |_col, cell, model, iter| {
                        let text: String = model.value(iter, store_col)
                            .get().unwrap_or_default();
                        let active = text == "true"
                            || text == "True"
                            || text == "1";
                        cell.set_property("active", &active);
                    })),
                );
                // Wire up toggled signal for on_edit.
                if let Some(eid) = edit_callable_id {
                    let gx = gx_for_edit.clone();
                    let cp = col_path.clone();
                    let store_clone = store_ref;
                    cell.connect_toggled(move |_cell, tree_path| {
                        if let Some(iter) = store_clone.iter(&tree_path) {
                            let row_name = store_clone
                                .value(&iter, 0)
                                .get::<String>()
                                .unwrap_or_default();
                            let cur: String = store_clone
                                .value(&iter, store_col)
                                .get().unwrap_or_default();
                            let toggled = !(cur == "true"
                                || cur == "True"
                                || cur == "1");
                            let cell_path = cp.append(&row_name);
                            let edit_struct = Value::Array(
                                ValArray::from_iter([
                                    Value::String(
                                        cell_path.to_string().into(),
                                    ),
                                    Value::String(
                                        toggled.to_string().into(),
                                    ),
                                ]),
                            );
                            let args = ValArray::from_iter([edit_struct]);
                            if let Err(e) = gx.call(eid, args) {
                                log::warn!(
                                    "table on_edit call failed: {}", e
                                );
                            }
                        }
                    });
                }
            }
            Some(ColumnType::Progress) => {
                let cell = CellRendererProgress::new();
                CellLayoutExt::pack_start(&tvc, &cell, true);
                TreeViewColumnExt::set_cell_data_func(
                    &tvc, &cell,
                    Some(Box::new(move |_col, cell, model, iter| {
                        let text: String = model.value(iter, store_col)
                            .get().unwrap_or_default();
                        let val: i32 = text.parse().unwrap_or(0);
                        cell.set_property("value", &val);
                    })),
                );
            }
            Some(ColumnType::Combo { choices }) => {
                let combo_store = ListStore::new(&[
                    glib::Type::STRING,
                    glib::Type::STRING,
                ]);
                for (id, label) in choices {
                    let iter = combo_store.append();
                    combo_store.set_value(&iter, 0, &id.to_value());
                    combo_store.set_value(&iter, 1, &label.to_value());
                }
                let cell = CellRendererCombo::builder()
                    .model(&combo_store)
                    .text_column(1)
                    .has_entry(false)
                    .build();
                if edit_callable_id.is_some() {
                    cell.set_editable(true);
                }
                CellLayoutExt::pack_start(&tvc, &cell, true);
                CellLayoutExt::add_attribute(
                    &tvc, &cell, "text", store_col,
                );
                if let Some(eid) = edit_callable_id {
                    let gx = gx_for_edit.clone();
                    let cp = col_path.clone();
                    let store_clone = store.clone();
                    cell.connect_edited(
                        move |_, tree_path, new_text| {
                            if let Some(iter) =
                                store_clone.iter(&tree_path)
                            {
                                let row_name = store_clone
                                    .value(&iter, 0)
                                    .get::<String>()
                                    .unwrap_or_default();
                                let cell_path = cp.append(&row_name);
                                let edit_struct = Value::Array(
                                    ValArray::from_iter([
                                        Value::String(
                                            cell_path.to_string().into(),
                                        ),
                                        Value::String(
                                            new_text.to_string().into(),
                                        ),
                                    ]),
                                );
                                let args = ValArray::from_iter([edit_struct]);
                                if let Err(e) = gx.call(eid, args) {
                                    log::warn!(
                                        "table on_edit call failed: {}",
                                        e
                                    );
                                }
                            }
                        },
                    );
                }
            }
            Some(ColumnType::Spin { min, max, increment }) => {
                let adj = Adjustment::new(
                    0.0, *min, *max, *increment, 0.0, 0.0,
                );
                let cell = CellRendererSpin::builder()
                    .adjustment(&adj)
                    .build();
                if edit_callable_id.is_some() {
                    cell.set_editable(true);
                }
                CellLayoutExt::pack_start(&tvc, &cell, true);
                CellLayoutExt::add_attribute(
                    &tvc, &cell, "text", store_col,
                );
                if let Some(eid) = edit_callable_id {
                    let gx = gx_for_edit.clone();
                    let cp = col_path.clone();
                    let store_clone = store.clone();
                    cell.connect_edited(
                        move |_, tree_path, new_text| {
                            if let Some(iter) =
                                store_clone.iter(&tree_path)
                            {
                                let row_name = store_clone
                                    .value(&iter, 0)
                                    .get::<String>()
                                    .unwrap_or_default();
                                let cell_path = cp.append(&row_name);
                                let edit_struct = Value::Array(
                                    ValArray::from_iter([
                                        Value::String(
                                            cell_path.to_string().into(),
                                        ),
                                        Value::String(
                                            new_text.to_string().into(),
                                        ),
                                    ]),
                                );
                                let args = ValArray::from_iter([edit_struct]);
                                if let Err(e) = gx.call(eid, args) {
                                    log::warn!(
                                        "table on_edit call failed: {}",
                                        e
                                    );
                                }
                            }
                        },
                    );
                }
            }
            Some(ColumnType::Hidden) => {
                let cell = CellRendererText::new();
                CellLayoutExt::pack_start(&tvc, &cell, true);
                CellLayoutExt::add_attribute(
                    &tvc, &cell, "text", store_col,
                );
                tvc.set_visible(false);
            }
            None | Some(ColumnType::Text) => {
                let cell = CellRendererText::new();
                if edit_callable_id.is_some() {
                    cell.set_editable(true);
                }
                CellLayoutExt::pack_start(&tvc, &cell, true);
                CellLayoutExt::add_attribute(
                    &tvc, &cell, "text", store_col,
                );
                // Wire up on_edit for this column's cell renderer.
                if let Some(eid) = edit_callable_id {
                    let gx = gx_for_edit.clone();
                    let cp = col_path.clone();
                    let store_clone = store.clone();
                    cell.connect_edited(
                        move |_, tree_path, new_text| {
                            if let Some(iter) =
                                store_clone.iter(&tree_path)
                            {
                                let row_name = store_clone
                                    .value(&iter, 0)
                                    .get::<String>()
                                    .unwrap_or_default();
                                let cell_path = cp.append(&row_name);
                                let edit_struct = Value::Array(
                                    ValArray::from_iter([
                                        Value::String(
                                            cell_path.to_string().into(),
                                        ),
                                        Value::String(
                                            new_text.to_string().into(),
                                        ),
                                    ]),
                                );
                                let args = ValArray::from_iter([edit_struct]);
                                if let Err(e) = gx.call(eid, args) {
                                    log::warn!(
                                        "table on_edit call failed: {}",
                                        e
                                    );
                                }
                            }
                        },
                    );
                }
            }
        }
        // Wire up on_header_click.
        if let Some(hid) = header_callable_id {
            let gx = gx_for_header.clone();
            let name = String::from(col_name);
            tvc.set_clickable(true);
            tvc.connect_clicked(move |_| {
                let args = ValArray::from_iter([
                    Value::String(name.clone().into()),
                ]);
                if let Err(e) = gx.call(hid, args) {
                    log::warn!("table on_header_click call failed: {}", e);
                }
            });
        }
        view.append_column(&tvc);
    }
    // Remove old child and add the new TreeView.
    if let Some(c) = root.child() {
        root.remove(&c);
    }
    root.add(&view);
    view.show_all();
    let (tx, rx) = mpsc::channel(3);
    let inner = Rc::new(RefCell::new(TableInner {
        descriptor: TableDescriptor { path, rows, all_rows, cols },
        store,
        view: view.clone(),
        by_id: FxHashMap::default(),
        subscribed: HashSet::new(),
        rx,
        tx,
    }));
    // Subscribe to initially visible cells (after layout).
    let weak_inner = Rc::downgrade(&inner);
    let subscriber = ctx.subscriber.clone();
    glib::idle_add_local(move || {
        if let Some(inner) = weak_inner.upgrade() {
            update_subscriptions(&mut inner.borrow_mut(), &subscriber);
        }
        ControlFlow::Break
    });
    // Wire up scroll events.
    let weak_inner = Rc::downgrade(&inner);
    let subscriber = ctx.subscriber.clone();
    root.vadjustment().connect_value_changed(move |_| {
        let weak = weak_inner.clone();
        let sub = subscriber.clone();
        glib::idle_add_local(move || {
            if let Some(inner) = weak.upgrade() {
                update_subscriptions(&mut inner.borrow_mut(), &sub);
            }
            ControlFlow::Break
        });
    });
    // Start the idle update pump.
    start_update_pump(Rc::downgrade(&inner));
    inner
}

/// Subscribe to visible cells, unsubscribe from cells that scrolled
/// out of view.
fn update_subscriptions(
    r: &mut TableInner,
    subscriber: &netidx::subscriber::Subscriber,
) {
    if !r.view.is_visible() {
        return;
    }
    let (mut start, mut end) = match r.view.visible_range() {
        Some((s, e)) => (s, e),
        None => return,
    };
    // Extend visible range by a buffer of 50 rows.
    for _ in 0..50 {
        start.prev();
        end.next();
    }
    // Unsubscribe rows that scrolled out.
    r.by_id.retain(|_, cell| {
        match r.store.path(&cell.row) {
            None => false,
            Some(p) => p >= start && p <= end,
        }
    });
    r.subscribed.clear();
    for cell in r.by_id.values() {
        if let Some(p) = r.store.path(&cell.row) {
            if let Some(row_idx) = p.indices().first() {
                r.subscribed.insert((*row_idx as usize, cell.col));
            }
        }
    }
    let n_cols = r.descriptor.cols.len() as u32;
    let start_idx = start.indices().first()
        .copied().unwrap_or(0).max(0) as usize;
    let end_idx = end.indices().first()
        .copied().unwrap_or(0).max(0) as usize;
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

/// Drain subscription updates from the mpsc channel and apply them to
/// the ListStore.
fn start_update_pump(weak: std::rc::Weak<RefCell<TableInner>>) {
    glib::timeout_add_local(std::time::Duration::from_millis(50), move || {
        let inner = match weak.upgrade() {
            Some(s) => s,
            None => return ControlFlow::Break,
        };
        let mut borrow = inner.borrow_mut();
        loop {
            match borrow.rx.try_recv() {
                Ok(batch) => {
                    for (sub_id, event) in batch.iter() {
                        if let Event::Update(value) = event {
                            if let Some(cell) = borrow.by_id.get(sub_id) {
                                let display = format!("{}", value);
                                borrow.store.set_value(
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

fn connect_on_select<X: GXExt>(
    view: &TreeView,
    gx: &graphix_rt::GXHandle<X>,
    callable: &Option<Callable<X>>,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    let selection = view.selection();
    Some(selection.connect_changed(move |sel| {
        let (paths, model) = sel.selected_rows();
        let names: Vec<Value> = paths.iter().filter_map(|p| {
            model.iter(p).map(|iter| {
                let name: String = model.value(&iter, 0)
                    .get().unwrap_or_default();
                Value::String(name.into())
            })
        }).collect();
        let arr = Value::Array(ValArray::from_iter(names));
        let args = ValArray::from_iter([arr]);
        if let Err(e) = gx.call(id, args) {
            log::warn!("table on_select call failed: {}", e);
        }
    }))
}

fn connect_on_activate<X: GXExt>(
    view: &TreeView,
    gx: &graphix_rt::GXHandle<X>,
    callable: &Option<Callable<X>>,
    base_path: &Path,
) -> Option<glib::SignalHandlerId> {
    let callable = callable.as_ref()?;
    let id = callable.id();
    let gx = gx.clone();
    let base = base_path.clone();
    Some(view.connect_row_activated(move |tv, path, _col| {
        if let Some(model) = tv.model() {
            if let Some(iter) = model.iter(path) {
                let name: String = model.value(&iter, 0)
                    .get().unwrap_or_default();
                let full_path = base.append(&name);
                log::info!("table on_activate: {}", full_path);
                let args = ValArray::from_iter([
                    Value::String(full_path.to_string().into()),
                ]);
                if let Err(e) = gx.call(id, args) {
                    log::warn!("table on_activate call failed: {}", e);
                }
            }
        }
    }))
}

fn parse_selection_mode(s: &str) -> gtk::SelectionMode {
    match s {
        "None" => gtk::SelectionMode::None,
        "Single" => gtk::SelectionMode::Single,
        "Multi" => gtk::SelectionMode::Multiple,
        _ => gtk::SelectionMode::Single,
    }
}

impl<X: GXExt> GtkWidget<X> for TableW<X> {
    fn handle_update(
        &mut self,
        rt: &tokio::runtime::Handle,
        id: ExprId,
        v: &Value,
    ) -> Result<bool> {
        let mut changed = false;
        // Path changed -> re-resolve and rebuild the table.
        let new_path = self.path.update(id, v).context("table update path")?
            .map(|p| p.clone());
        if let Some(ref p) = new_path {
            self.resolve_and_build(rt, p);
            // Re-apply sort and column filter after rebuild.
            if let TableState::Live(ref inner) = self.state {
                let borrow = inner.borrow();
                if let Some(ref sv) = self.sort_mode.last {
                    let mode = parse_sort_mode(sv);
                    apply_sort_mode(
                        &borrow.store,
                        &borrow.view,
                        &borrow.descriptor.cols,
                        &mode,
                    );
                }
                if let Some(ref cv) = self.column_filter.last {
                    let cf = parse_column_filter(cv);
                    apply_column_filter(
                        &borrow.view,
                        &borrow.descriptor.cols,
                        &cf,
                    );
                }
            }
            changed = true;
        }
        // Selection mode changed.
        if let Some(sm) = self
            .selection_mode
            .update(id, v)
            .context("table update selection_mode")?
        {
            if let TableState::Live(ref inner) = self.state {
                inner.borrow().view.selection().set_mode(
                    parse_selection_mode(sm),
                );
            }
            changed = true;
        }
        // Show/hide row name column.
        if let Some(srn) = self
            .show_row_name
            .update(id, v)
            .context("table update show_row_name")?
        {
            if let TableState::Live(ref inner) = self.state {
                let borrow = inner.borrow();
                if let Some(col) = borrow.view.column(0) {
                    col.set_visible(*srn);
                }
            }
            changed = true;
        }
        // Sort mode changed.
        if id == self.sort_mode.id {
            self.sort_mode.last = Some(v.clone());
            if let TableState::Live(ref inner) = self.state {
                let borrow = inner.borrow();
                let mode = parse_sort_mode(v);
                apply_sort_mode(
                    &borrow.store,
                    &borrow.view,
                    &borrow.descriptor.cols,
                    &mode,
                );
            }
            changed = true;
        }
        // Column filter changed.
        if id == self.column_filter.id {
            self.column_filter.last = Some(v.clone());
            if let TableState::Live(ref inner) = self.state {
                let borrow = inner.borrow();
                let filter = parse_column_filter(v);
                apply_column_filter(
                    &borrow.view,
                    &borrow.descriptor.cols,
                    &filter,
                );
            }
            changed = true;
        }
        // Row filter changed — rebuild table with filtered rows.
        if id == self.row_filter.id {
            self.row_filter.last = Some(v.clone());
            let filter = parse_row_filter(v);
            self.rebuild_with_row_filter(&filter);
            // Re-apply sort and column filter after rebuild.
            if let TableState::Live(ref inner) = self.state {
                let borrow = inner.borrow();
                if let Some(ref sv) = self.sort_mode.last {
                    let mode = parse_sort_mode(sv);
                    apply_sort_mode(
                        &borrow.store,
                        &borrow.view,
                        &borrow.descriptor.cols,
                        &mode,
                    );
                }
                if let Some(ref cv) = self.column_filter.last {
                    let cf = parse_column_filter(cv);
                    apply_column_filter(
                        &borrow.view,
                        &borrow.descriptor.cols,
                        &cf,
                    );
                }
            }
            changed = true;
        }
        // Callback ref updates.
        if id == self.on_select.id {
            self.on_select.last = Some(v.clone());
            self.on_select_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("table on_select recompile")?,
            );
            // Reconnect selection signal.
            if let Some(sig) = self.select_signal.take() {
                if let TableState::Live(ref inner) = self.state {
                    inner.borrow().view.selection().disconnect(sig);
                }
            }
            if let TableState::Live(ref inner) = self.state {
                self.select_signal = connect_on_select(
                    &inner.borrow().view,
                    &self.ctx.gx,
                    &self.on_select_callable,
                );
            }
            changed = true;
        }
        if id == self.on_activate.id {
            self.on_activate.last = Some(v.clone());
            self.on_activate_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("table on_activate recompile")?,
            );
            if let Some(sig) = self.activate_signal.take() {
                if let TableState::Live(ref inner) = self.state {
                    inner.borrow().view.disconnect(sig);
                }
            }
            if let TableState::Live(ref inner) = self.state {
                let borrow = inner.borrow();
                self.activate_signal = connect_on_activate(
                    &borrow.view,
                    &self.ctx.gx,
                    &self.on_activate_callable,
                    &borrow.descriptor.path,
                );
            }
            changed = true;
        }
        if id == self.on_edit.id {
            self.on_edit.last = Some(v.clone());
            self.on_edit_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("table on_edit recompile")?,
            );
            // on_edit is wired per-cell-renderer at build time; a
            // full rebuild is needed to rewire it. For now we just
            // track the new callable.
            changed = true;
        }
        if id == self.on_header_click.id {
            self.on_header_click.last = Some(v.clone());
            self.on_header_click_callable = Some(
                rt.block_on(self.ctx.gx.compile_callable(v.clone()))
                    .context("table on_header_click recompile")?,
            );
            changed = true;
        }
        Ok(changed)
    }

    fn gtk_widget(&self) -> &gtk::Widget {
        self.root.upcast_ref()
    }
}
