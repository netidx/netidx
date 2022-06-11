use super::{
    util::{err_modal, toplevel},
    BSCtx, BSCtxRef, BSNode, BWidget, ImageSpec, WVal,
};
use crate::bscript::LocalEvent;
use anyhow::{anyhow, bail};
use arcstr::ArcStr;
use futures::channel::oneshot;
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use gdk::{keys, EventButton, EventKey, RGBA};
use gio::prelude::*;
use glib::{self, clone, idle_add_local, signal::Inhibit, source::Continue};
use gtk::{
    prelude::*, Adjustment, CellRenderer, CellRendererPixbuf, CellRendererText, Label,
    ListStore, ScrolledWindow, SortColumn, SortType, StateFlags, StyleContext, TreeIter,
    TreeModel, TreePath, TreeView, TreeViewColumn, TreeViewColumnSizing,
};
use indexmap::{IndexMap, IndexSet};
use netidx::{
    chars::Chars,
    pack::Z64,
    path::Path,
    pool::{Pool, Pooled},
    protocol::value::FromValue,
    resolver_client,
    subscriber::{Dval, Event, SubId, UpdatesFlags, Value},
    utils::Either,
};
use netidx_bscript::vm;
use netidx_protocols::view;
use rand::{thread_rng, Rng};
use regex::RegexSet;
use std::{
    cell::{Cell, RefCell},
    cmp::{Ordering, PartialEq},
    collections::{HashMap, HashSet},
    fmt::Write,
    ops::Deref,
    rc::{Rc, Weak},
    result::{self, Result},
    str::FromStr,
};

struct Subscription {
    _sub: Dval,
    row: TreeIter,
    col: u32,
}

lazy_static! {
    static ref FORMATTED: Pool<String> = Pool::new(50_000, 1024);
}

#[derive(Debug, Clone, Boxed)]
#[boxed_type(name = "NetidxValue")]
struct BVal {
    value: Value,
    formatted: Pooled<String>,
}

impl Deref for BVal {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SortDir {
    Ascending,
    Descending,
}

impl FromStr for SortDir {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ascending" => Ok(SortDir::Ascending),
            "descending" => Ok(SortDir::Descending),
            _ => anyhow::bail!("invalid sort direction"),
        }
    }
}

impl FromValue for SortDir {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        v.cast_to::<Chars>().and_then(|c| c.parse())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SortSpec {
    None,
    Disabled,
    Column(Chars, SortDir),
    External(FxHashMap<Chars, SortDir>),
}

impl FromValue for SortSpec {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        match v {
            Value::Null => Ok(SortSpec::None),
            Value::False => Ok(SortSpec::Disabled),
            Value::String(col) => Ok(SortSpec::Column(col, SortDir::Descending)),
            Value::Array(a) if a.len() == 2 && a[0] == Value::False => {
                Ok(SortSpec::External(a[1].clone().cast_to()?))
            }
            Value::Array(a) if a.len() == 2 => {
                let column = a[0].clone().cast_to()?;
                let spec = a[1].clone().cast_to()?;
                Ok(SortSpec::Column(column, spec))
            }
            _ => anyhow::bail!("expected null, false, col, or a pair of [column, mode]"),
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
    // CR estokes: Adjust ranges for the name column
    IncludeRange(Option<usize>, Option<usize>),
    ExcludeRange(Option<usize>, Option<usize>),
}

impl PartialEq for Filter {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Filter::All, Filter::All)
            | (Filter::Auto, Filter::Auto)
            | (Filter::None, Filter::None) => true,
            (Filter::Include(s0), Filter::Include(s1)) => s0.iter().eq(s1.iter()), // order matters
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

impl FromValue for Filter {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        match v {
            Value::Null => Ok(Filter::Auto),
            Value::True => Ok(Filter::All),
            Value::False => Ok(Filter::None),
            Value::Array(a) if a.len() == 2 => {
                let mode = a[0].clone().cast_to::<Chars>()?;
                match &*mode {
                    "include" | "exclude" | "include_match" | "exclude_match" => {
                        let i = a[1].clone().flatten().map(|v| v.cast_to::<String>());
                        if &*mode == "include" {
                            Ok(Filter::Include(i.collect::<Result<IndexSet<_, _>, _>>()?))
                        } else if &*mode == "exclude" {
                            Ok(Filter::Exclude(i.collect::<Result<IndexSet<_, _>, _>>()?))
                        } else if &*mode == "include_match" {
                            let set = i.collect::<Result<IndexSet<_, _>, _>>()?;
                            let matcher = RegexSet::new(&set)?;
                            Ok(Filter::IncludeMatch(set, matcher))
                        } else if &*mode == "exclude_match" {
                            let set = i.collect::<Result<IndexSet<_, _>, _>>()?;
                            let matcher = RegexSet::new(&set)?;
                            Ok(Filter::ExcludeMatch(set, matcher))
                        } else {
                            unreachable!()
                        }
                    }
                    "keep" | "drop" => match &a[1] {
                        Value::Array(a) if a.len() == 2 => {
                            let start = match &a[0] {
                                Value::String(v) if &**v == "start" => None,
                                v => Some(v.clone().cast_to::<u64>()? as usize),
                            };
                            let end = match &a[1] {
                                Value::String(v) if &**v == "end" => None,
                                v => Some(v.clone().cast_to::<u64>()? as usize),
                            };
                            if &*mode == "keep" {
                                Ok(Filter::IncludeRange(start, end))
                            } else if &*mode == "drop" {
                                Ok(Filter::ExcludeRange(start, end))
                            } else {
                                unreachable!()
                            }
                        }
                        _ => bail!("keep/drop expect 2 arguments"),
                    },
                    _ => bail!("invalid filter mode"),
                }
            }
            _ => bail!("expected null, true, false, or a pair"),
        }
    }
}

impl Filter {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectionMode {
    None,
    Single,
    Multi,
}

impl FromValue for SelectionMode {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        match v.cast_to::<Chars>()? {
            c if &*c == "none" => Ok(SelectionMode::None),
            c if &*c == "single" => Ok(SelectionMode::Single),
            c if &*c == "multi" => Ok(SelectionMode::Multi),
            _ => bail!("invalid selection mode"),
        }
    }
}

#[derive(Debug, Clone)]
enum OrLoad<T> {
    Static(T),
    Load(i32),
    LoadWithFallback(T, i32),
}

impl<T: FromValue + Clone> OrLoad<T> {
    fn load(&self, row: &TreeIter, store: &ListStore) -> Option<T> {
        match self {
            Self::Static(v) => Some(v.clone()),
            Self::Load(i) => {
                let v = store.value(row, *i);
                match v.get::<&BVal>() {
                    Ok(bv) => bv.value.clone().cast_to::<T>().ok(),
                    Err(_) => None,
                }
            }
            Self::LoadWithFallback(s, i) => {
                let v = store.value(row, *i);
                match v.get::<&BVal>() {
                    Err(_) => Some(s.clone()),
                    Ok(bv) => Some(
                        bv.value.clone().cast_to::<T>().ok().unwrap_or_else(|| s.clone()),
                    ),
                }
            }
        }
    }
}

#[derive(Clone, PartialEq)]
enum OrLoadCol<T> {
    Static(T),
    Load(Chars),
    LoadWithFallback(T, Chars),
}

impl<T: Clone> OrLoadCol<T> {
    fn resolve(&self, descriptor: &IndexDescriptor) -> Option<OrLoad<T>> {
        match self {
            Self::Static(v) => Some(OrLoad::Static(v.clone())),
            Self::Load(col) => descriptor
                .cols
                .get_full(&**col)
                .map(|(i, _, _)| OrLoad::Load((i + 1) as i32)),
            Self::LoadWithFallback(v, col) => {
                let t = descriptor
                    .cols
                    .get_full(&**col)
                    .map(|(i, _, _)| OrLoad::LoadWithFallback(v.clone(), (i + 1) as i32))
                    .unwrap_or_else(|| OrLoad::Static(v.clone()));
                Some(t)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Color(RGBA);

impl PartialEq for Color {
    fn eq(&self, other: &Self) -> bool {
        self.0.red() == other.0.red()
            && self.0.green() == other.0.green()
            && self.0.blue() == other.0.blue()
    }
}

impl FromValue for Color {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        Ok(Color(RGBA::parse(
            &*v.cast_to::<Chars>().map_err(|_| anyhow!("unable to parse color"))?,
        )?))
    }
}

macro_rules! prop {
    ($props:expr, $name:expr, $typ:ty) => {{
        match $props.remove($name) {
            None => None,
            Some(v) => Some(v.cast_to::<$typ>()?),
        }
    }};
}

macro_rules! or_load_prop {
    ($props:expr, $static_name:expr, $col_name:expr, $typ:ty) => {{
        let load = match $props.remove($col_name) {
            None => None,
            Some(v) => Some(v.cast_to::<Chars>()?),
        };
        let val = match $props.remove($static_name) {
            None => None,
            Some(v) => Some(v.cast_to::<$typ>()?),
        };
        match (val, load) {
            (Some(val), Some(col)) => Some(OrLoadCol::LoadWithFallback(val, col)),
            (Some(val), None) => Some(OrLoadCol::Static(val)),
            (None, Some(col)) => Some(OrLoadCol::Load(col)),
            (None, None) => None,
        }
    }};
}

#[derive(Debug, Clone)]
struct CTCommonResolved {
    source: i32,
    background: Option<OrLoad<Color>>,
}

#[derive(Clone, PartialEq)]
struct ColumnTypeCommon {
    source: Option<Chars>,
    background: Option<OrLoadCol<Color>>,
}

impl ColumnTypeCommon {
    fn resolve(
        &self,
        vector_mode: bool,
        name: &str,
        descriptor: &IndexDescriptor,
    ) -> Option<CTCommonResolved> {
        let source = if vector_mode {
            Some(1)
        } else {
            match &self.source {
                None => descriptor.cols.get_full(name).map(|(i, _, _)| (i + 1) as i32),
                Some(src) => {
                    descriptor.cols.get_full(&**src).map(|(i, _, _)| (i + 1) as i32)
                }
            }
        };
        let background = self.background.as_ref().and_then(|v| v.resolve(descriptor));
        source.map(move |source| CTCommonResolved { source, background })
    }

    fn from_props(
        props: &mut FxHashMap<Chars, Value>,
    ) -> anyhow::Result<ColumnTypeCommon> {
        Ok(Self {
            source: prop!(props, "source", Chars),
            background: or_load_prop!(props, "background", "background-column", Color),
        })
    }
}

impl FromValue for ColumnTypeCommon {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        Self::from_props(&mut v.cast_to::<FxHashMap<Chars, Value>>()?)
    }
}

#[derive(Clone, PartialEq)]
struct ColumnTypeText {
    common: ColumnTypeCommon,
    foreground: Option<OrLoadCol<Color>>,
}

impl FromValue for ColumnTypeText {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        let mut props = v.cast_to::<FxHashMap<Chars, Value>>()?;
        Ok(Self {
            common: ColumnTypeCommon::from_props(&mut props)?,
            foreground: or_load_prop!(props, "foreground", "foreground-column", Color),
        })
    }
}

#[derive(Clone, PartialEq)]
struct ColumnTypeToggle {
    common: ColumnTypeCommon,
    radio: Option<OrLoadCol<bool>>,
}

impl FromValue for ColumnTypeToggle {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        let mut props = v.cast_to::<FxHashMap<Chars, Value>>()?;
        Ok(Self {
            common: ColumnTypeCommon::from_props(&mut props)?,
            radio: or_load_prop!(props, "radio", "radio-column", bool),
        })
    }
}

#[derive(Clone, PartialEq)]
struct ColumnTypeCombo {
    common: ColumnTypeCommon,
    choices: OrLoadCol<FxHashMap<Chars, Chars>>,
    has_entry: Option<OrLoadCol<bool>>,
}

impl FromValue for ColumnTypeCombo {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        let mut props = v.cast_to::<FxHashMap<Chars, Value>>()?;
        let choices =
            or_load_prop!(props, "choices", "choices-column", FxHashMap<Chars, Chars>)
                .ok_or_else(|| anyhow!("choices is required"))?;
        Ok(Self {
            common: ColumnTypeCommon::from_props(&mut props)?,
            has_entry: or_load_prop!(props, "has-entry", "has-entry-column", bool),
            choices,
        })
    }
}

#[derive(Clone, PartialEq)]
struct ColumnTypeSpin {
    common: ColumnTypeCommon,
    min: Option<OrLoadCol<f64>>,
    max: Option<OrLoadCol<f64>>,
    climb_rate: Option<OrLoadCol<f64>>,
    digits: Option<OrLoadCol<u32>>,
}

impl FromValue for ColumnTypeSpin {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        let mut props = v.cast_to::<FxHashMap<Chars, Value>>()?;
        Ok(Self {
            common: ColumnTypeCommon::from_props(&mut props)?,
            min: or_load_prop!(props, "min", "min-column", f64),
            max: or_load_prop!(props, "max", "max-column", f64),
            climb_rate: or_load_prop!(props, "climb-rate", "climb-rate-column", f64),
            digits: or_load_prop!(props, "digits", "digits-column", u32),
        })
    }
}

#[derive(Clone, PartialEq)]
struct ColumnTypeProgress {
    common: ColumnTypeCommon,
    activity_mode: Option<OrLoadCol<bool>>,
    text: Option<OrLoadCol<Chars>>,
    text_xalign: Option<OrLoadCol<f64>>,
    text_yalign: Option<OrLoadCol<f64>>,
    inverted: Option<OrLoadCol<bool>>,
}

impl FromValue for ColumnTypeProgress {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        let mut props = v.cast_to::<FxHashMap<Chars, Value>>()?;
        Ok(Self {
            common: ColumnTypeCommon::from_props(&mut props)?,
            activity_mode: or_load_prop!(
                props,
                "activity-mode",
                "activity-mode-column",
                bool
            ),
            text: or_load_prop!(props, "text", "text-column", Chars),
            text_xalign: or_load_prop!(props, "text-xalign", "text-xalign-column", f64),
            text_yalign: or_load_prop!(props, "text-yalign", "text-yalign-column", f64),
            inverted: or_load_prop!(props, "inverted", "inverted-column", bool),
        })
    }
}

#[derive(Clone, PartialEq)]
enum ColumnType {
    Text(ColumnTypeText),
    Toggle(ColumnTypeToggle),
    Image(ColumnTypeCommon),
    Combo(ColumnTypeCombo),
    Spin(ColumnTypeSpin),
    Progress(ColumnTypeProgress),
    Hidden,
}

#[derive(Clone, PartialEq)]
struct ColumnSpec {
    name: Chars,
    typ: ColumnType,
}

impl FromValue for ColumnSpec {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        let (name, typ, props) = v.cast_to::<(Chars, Chars, Value)>()?;
        let typ = match &*typ {
            "text" => ColumnType::Text(props.cast_to::<ColumnTypeText>()?),
            "toggle" => ColumnType::Toggle(props.cast_to::<ColumnTypeToggle>()?),
            "image" => ColumnType::Image(props.cast_to::<ColumnTypeCommon>()?),
            "combo" => ColumnType::Combo(props.cast_to::<ColumnTypeCombo>()?),
            "spin" => ColumnType::Spin(props.cast_to::<ColumnTypeSpin>()?),
            "progress" => ColumnType::Progress(props.cast_to::<ColumnTypeProgress>()?),
            "hidden" => ColumnType::Hidden,
            _ => bail!("invalid column type"),
        };
        Ok(Self { name, typ })
    }
}

enum TableState {
    Resolving(Path),
    Raeified(RaeifiedTable),
    Refresh(Path),
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

const NAME_COL: &str = "name";

#[derive(Debug, Clone)]
struct IndexDescriptor {
    rows: IndexSet<Path>,
    cols: IndexMap<Path, Z64>,
}

impl IndexDescriptor {
    fn from_descriptor(show_name: bool, d: &resolver_client::Table) -> Self {
        let mut t =
            IndexDescriptor {
                cols: IndexMap::from_iter(d.cols.iter().cloned()),
                rows: IndexSet::from_iter(d.rows.iter().filter_map(|p| {
                    Path::basename(p).map(|p| Path::from(ArcStr::from(p)))
                })),
            };
        if show_name && t.cols.contains_key(NAME_COL) {
            let deconf = loop {
                let name = format!("{}_{}", NAME_COL, thread_rng().gen::<u64>());
                if !t.cols.contains_key(&*name) {
                    break Path::from(name);
                }
            };
            if let Some(cnt) = t.cols.remove(NAME_COL) {
                t.cols.insert(deconf, cnt);
            }
        }
        t
    }
}

macro_rules! set_field {
    ($self:expr, $v:expr, $typ:ty, $field:ident, $map:expr) => {
        match $v.and_then(|v| v.cast_to::<$typ>().ok()) {
            None => false,
            Some(val) => match $map(val) {
                None => false,
                Some(val) => {
                    if &*$self.$field.borrow() != &val {
                        *$self.$field.borrow_mut() = val;
                        true
                    } else {
                        false
                    }
                }
            },
        }
    };
}

macro_rules! set_field_cell {
    ($self:expr, $v:expr, $typ:ty, $field:ident) => {
        match $v.and_then(|v| v.cast_to::<$typ>().ok()) {
            None => false,
            Some(val) => {
                if $self.$field.get() != val {
                    $self.$field.set(val);
                    true
                } else {
                    false
                }
            }
        }
    };
}

struct SharedState {
    column_editable: RefCell<Filter>,
    column_filter: RefCell<Filter>,
    columns_resizable: Cell<bool>,
    column_types: RefCell<Option<Vec<ColumnSpec>>>,
    column_widths: RefCell<FxHashMap<String, i32>>,
    ctx: BSCtx,
    on_activate: RefCell<BSNode>,
    on_edit: RefCell<BSNode>,
    on_header_click: RefCell<BSNode>,
    on_select: RefCell<BSNode>,
    original_descriptor: RefCell<resolver_client::Table>,
    path: RefCell<Path>,
    root: ScrolledWindow,
    row_filter: RefCell<Filter>,
    selected_path: Label,
    selected: RefCell<FxHashMap<String, FxHashSet<String>>>,
    selection_mode: Cell<SelectionMode>,
    show_name_column: Cell<bool>,
    sort_mode: RefCell<SortSpec>,
}

impl SharedState {
    fn new(
        ctx: BSCtx,
        selected_path: Label,
        root: ScrolledWindow,
        on_activate: BSNode,
        on_edit: BSNode,
        on_header_click: BSNode,
        on_select: BSNode,
    ) -> Self {
        Self {
            column_editable: RefCell::new(Filter::None),
            column_filter: RefCell::new(Filter::Auto),
            columns_resizable: Cell::new(false),
            column_types: RefCell::new(None),
            column_widths: RefCell::new(HashMap::default()),
            ctx,
            selection_mode: Cell::new(SelectionMode::None),
            on_activate: RefCell::new(on_activate),
            on_edit: RefCell::new(on_edit),
            on_header_click: RefCell::new(on_header_click),
            on_select: RefCell::new(on_select),
            original_descriptor: RefCell::new(resolver_client::Table {
                rows: Pooled::orphan(vec![]),
                cols: Pooled::orphan(vec![]),
            }),
            path: RefCell::new(Path::root()),
            root,
            row_filter: RefCell::new(Filter::All),
            selected_path,
            selected: RefCell::new(HashMap::default()),
            show_name_column: Cell::new(true),
            sort_mode: RefCell::new(SortSpec::None),
        }
    }

    fn set_path(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Path, path, Some)
    }

    fn set_sort_mode(&self, v: Option<Value>) -> bool {
        set_field!(self, v, SortSpec, sort_mode, Some)
    }

    fn set_column_types(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Option<Vec<ColumnSpec>>, column_types, Some)
    }

    fn set_column_filter(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Filter, column_filter, Some)
    }

    fn set_row_filter(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Filter, row_filter, Some)
    }

    fn set_column_editable(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Filter, column_editable, Some)
    }

    fn set_selection_mode(&self, v: Option<Value>) -> bool {
        set_field_cell!(self, v, SelectionMode, selection_mode)
    }

    fn set_selection(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Vec<Path>, selected, |paths: Vec<Path>| {
            let iter = paths.into_iter().filter_map(|p| {
                let col = String::from(Path::basename(&p)?);
                let row = String::from(Path::basename(Path::dirname(&p)?)?);
                Some((row, col))
            });
            let mut res = HashMap::default();
            for (row, col) in iter {
                res.entry(row).or_insert_with(HashSet::default).insert(col);
            }
            Some(res)
        })
    }

    fn set_show_row_name(&self, v: Option<Value>) -> bool {
        set_field_cell!(self, v, bool, show_name_column)
    }

    fn set_columns_resizable(&self, v: Option<Value>) -> bool {
        set_field_cell!(self, v, bool, columns_resizable)
    }

    fn set_column_widths(&self, v: Option<Value>) -> bool {
        set_field!(self, v, FxHashMap<String, i32>, column_widths, Some)
    }

    fn apply_filters(&self) -> (bool, IndexDescriptor) {
        let mut descriptor = IndexDescriptor::from_descriptor(
            self.show_name_column.get(),
            &*self.original_descriptor.borrow(),
        );
        descriptor.cols.sort_by(|p0, _, p1, _| p0.cmp(p1));
        descriptor.rows.sort();
        {
            let mut i = 0;
            let row_filter = self.row_filter.borrow();
            descriptor.rows.retain(|row| {
                let res = row_filter.is_match(i, row);
                i += 1;
                res
            });
        }
        {
            let column_filter = self.column_filter.borrow();
            match &*column_filter {
                Filter::Auto => {
                    let half = descriptor.rows.len() as f32 / 2.;
                    descriptor.cols.retain(|_, i| i.0 as f32 >= half)
                }
                filter => {
                    let mut i = 0;
                    descriptor.cols.retain(|col, _| {
                        let res = filter.is_match(i, &col);
                        i += 1;
                        res
                    })
                }
            }
        }
        {
            let filter = self.column_filter.borrow();
            descriptor.cols.sort_by(|c0, _, c1, _| {
                let i0 = filter.sort_index(c0);
                let i1 = filter.sort_index(c1);
                match (i0, i1) {
                    (Some(i0), Some(i1)) => i0.cmp(&i1),
                    (_, _) => c0.cmp(&c1),
                }
            });
        }
        {
            let filter = self.row_filter.borrow();
            descriptor.rows.sort_by(|r0, r1| {
                let i0 = filter.sort_index(r0);
                let i1 = filter.sort_index(r1);
                match (i0, i1) {
                    (Some(i0), Some(i1)) => i0.cmp(&i1),
                    (_, _) => r0.cmp(&r1),
                }
            });
        }
        let mut selection_changed = false;
        self.selected.borrow_mut().retain(|row, cols| {
            cols.retain(|col| {
                let r = descriptor.cols.contains_key(&**col);
                selection_changed |= !r;
                r
            });
            let r = !cols.is_empty() && descriptor.rows.contains(&**row);
            selection_changed |= !r;
            r
        });
        (selection_changed, descriptor)
    }

    fn generate_column_spec(
        &self,
        descriptor: &IndexDescriptor,
    ) -> IndexMap<Chars, ColumnSpec, FxBuildHasher> {
        let mut spec = self
            .column_types
            .borrow()
            .as_ref()
            .map(|s| {
                s.iter().cloned().map(|s| (s.name.clone(), s)).collect::<IndexMap<
                    Chars,
                    ColumnSpec,
                    FxBuildHasher,
                >>()
            })
            .unwrap_or(IndexMap::default());
        for col in descriptor.cols.keys() {
            if !spec.contains_key(&**col) {
                let name = Chars::from(String::from(&**col));
                let cs = ColumnSpec {
                    name: name.clone(),
                    typ: ColumnType::Text(ColumnTypeText {
                        common: ColumnTypeCommon { source: None, background: None },
                        foreground: None,
                    }),
                };
                spec.insert(name, cs);
            }
        }
        spec
    }
}

struct RaeifiedTableInner {
    path: Path,
    shared: Rc<SharedState>,
    by_id: RefCell<HashMap<SubId, Subscription>>,
    columns_autosizing: Rc<Cell<bool>>,
    descriptor: IndexDescriptor,
    destroyed: Cell<bool>,
    name_column: RefCell<Option<TreeViewColumn>>,
    sort_column: Cell<Option<u32>>,
    sort_temp_disabled: Cell<bool>,
    store: ListStore,
    style: StyleContext,
    subscribed: RefCell<HashMap<String, HashSet<u32>>>,
    update: RefCell<IndexMap<SubId, Value, FxBuildHasher>>,
    vector_mode: bool,
    view: TreeView,
}

#[derive(Clone)]
struct RaeifiedTable(Rc<RaeifiedTableInner>);

impl Deref for RaeifiedTable {
    type Target = RaeifiedTableInner;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

struct RaeifiedTableWeak(Weak<RaeifiedTableInner>);

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
            cell.connect_edited(clone!(@weak t => move |_, _, v| {
                t.shared.on_edit.borrow_mut().update(
                    &mut t.shared.ctx.borrow_mut(),
                    &vm::Event::User(LocalEvent::Event(Value::from(String::from(v))))
                );
            }));
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
                    ColumnType::Toggle(_)
                    | ColumnType::Combo(_)
                    | ColumnType::Spin(_)
                    | ColumnType::Progress(_) => unimplemented!(),
                    ColumnType::Hidden => (),
                }
            }
        }
    }

    fn new(shared: Rc<SharedState>) -> RaeifiedTable {
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
            by_id: RefCell::new(HashMap::new()),
            columns_autosizing: Rc::new(Cell::new(false)),
            destroyed: Cell::new(false),
            name_column: RefCell::new(None),
            sort_column: Cell::new(None),
            sort_temp_disabled: Cell::new(false),
            subscribed: RefCell::new(HashMap::new()),
            update: RefCell::new(IndexMap::with_hasher(FxBuildHasher::default())),
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

    fn path_from_selected(&self, row: &str, title: &String) -> Path {
        let path = &self.path;
        if self.vector_mode {
            path.append(row)
        } else if self.shared.show_name_column.get() && title.as_str() == NAME_COL {
            path.append(row)
        } else {
            path.append(row).append(title)
        }
    }

    fn cursor_changed(&self) {
        if let (Some(p), Some(c)) = self.view().cursor() {
            if let Some(row) =
                self.row_of(Either::Left(&p)).as_ref().map(|r| r.get::<&str>().unwrap())
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

    fn view(&self) -> &TreeView {
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

pub(super) struct Table {
    column_editable: BSNode,
    column_filter: BSNode,
    columns_resizable: BSNode,
    column_types: BSNode,
    column_widths: BSNode,
    selection_mode: BSNode,
    selection: BSNode,
    path: BSNode,
    row_filter: BSNode,
    show_row_name: BSNode,
    sort_mode: BSNode,
    shared: Rc<SharedState>,
    state: Rc<RefCell<TableState>>,
    visible: Cell<bool>,
}

impl Table {
    pub(super) fn new(
        ctx: BSCtx,
        spec: view::Table,
        scope: Path,
        selected_path: Label,
    ) -> Table {
        let path = BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.path);
        let sort_mode =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.sort_mode);
        let column_filter =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.column_filter);
        let row_filter =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.row_filter);
        let column_editable =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.column_editable);
        let selection_mode =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.selection_mode);
        let selection =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.selection);
        let show_row_name =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.show_row_name);
        let columns_resizable = BSNode::compile(
            &mut *ctx.borrow_mut(),
            scope.clone(),
            spec.columns_resizable,
        );
        let column_types =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.column_types);
        let column_widths =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.column_widths);
        let on_select =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.on_select);
        let on_activate =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.on_activate);
        let on_edit =
            BSNode::compile(&mut *ctx.borrow_mut(), scope.clone(), spec.on_edit);
        let on_header_click =
            BSNode::compile(&mut *ctx.borrow_mut(), scope, spec.on_header_click);
        let root = ScrolledWindow::new(None::<&Adjustment>, None::<&Adjustment>);
        let shared = Rc::new(SharedState::new(
            ctx,
            selected_path,
            root,
            on_activate,
            on_edit,
            on_header_click,
            on_select,
        ));
        shared.set_path(path.current());
        shared.set_sort_mode(sort_mode.current());
        shared.set_column_filter(column_filter.current());
        shared.set_row_filter(row_filter.current());
        shared.set_column_editable(column_editable.current());
        shared.set_selection_mode(selection_mode.current());
        shared.set_selection(selection.current());
        shared.set_show_row_name(show_row_name.current());
        shared.set_columns_resizable(columns_resizable.current());
        shared.set_column_types(column_types.current());
        shared.set_column_widths(column_widths.current());
        let state = Rc::new(RefCell::new({
            let path = &*shared.path.borrow();
            shared.ctx.borrow().user.backend.resolve_table(path.clone());
            TableState::Resolving(path.clone())
        }));
        shared.root.vadjustment().connect_value_changed(clone!(@weak state => move |_| {
            idle_add_local(clone!(@weak state => @default-return Continue(false), move || {
                match &*state.borrow() {
                    TableState::Raeified(t) => t.update_subscriptions(),
                    TableState::Refresh {..} | TableState::Resolving(_) => ()
                }
                Continue(false)
            }));
        }));
        Table {
            shared,
            column_editable,
            column_filter,
            columns_resizable,
            column_types,
            column_widths,
            selection_mode,
            selection,
            path,
            row_filter,
            show_row_name,
            sort_mode,
            state,
            visible: Cell::new(true),
        }
    }

    fn refresh(&self, ctx: BSCtxRef) {
        let state = &mut *self.state.borrow_mut();
        let path = &*self.shared.path.borrow();
        match state {
            TableState::Refresh(rpath) if &*path == rpath => (),
            TableState::Resolving(rpath) if &*path == rpath => (),
            TableState::Raeified(table) if &table.path == &*path => {
                *state = TableState::Refresh(path.clone());
            }
            TableState::Refresh(_)
            | TableState::Resolving(_)
            | TableState::Raeified(_) => {
                ctx.user.backend.resolve_table(path.clone());
                *state = TableState::Resolving(path.clone());
            }
        }
    }

    fn raeify(&self) {
        let state = &self.state;
        let visible = &self.visible;
        let shared = &self.shared;
        idle_add_local(
            clone!(@strong state, @strong visible, @strong shared => move || {
                if let Some(c) = shared.root.child() {
                    shared.root.remove(&c);
                }
                let table = RaeifiedTable::new(shared.clone());
                if visible.get() {
                    table.view().show();
                }
                idle_add_local(clone!(@strong table => move || {
                    table.update_subscriptions();
                    Continue(false)
                }));
                *state.borrow_mut() = TableState::Raeified(table);
                Continue(false)
            }),
        );
    }
}

impl BWidget for Table {
    fn update(
        &mut self,
        ctx: BSCtxRef,
        waits: &mut Vec<oneshot::Receiver<()>>,
        event: &vm::Event<LocalEvent>,
    ) {
        let mut re = false;
        re |= self.shared.set_path(self.path.update(ctx, event));
        re |= self.shared.set_sort_mode(self.sort_mode.update(ctx, event));
        re |= self.shared.set_column_filter(self.column_filter.update(ctx, event));
        re |= self.shared.set_row_filter(self.row_filter.update(ctx, event));
        re |= self.shared.set_column_editable(self.column_editable.update(ctx, event));
        re |= self.shared.set_selection_mode(self.selection_mode.update(ctx, event));
        re |= self.shared.set_selection(self.selection.update(ctx, event));
        re |= self.shared.set_show_row_name(self.show_row_name.update(ctx, event));
        re |=
            self.shared.set_columns_resizable(self.columns_resizable.update(ctx, event));
        re |= self.shared.set_column_types(self.column_types.update(ctx, event));
        re |= self.shared.set_column_widths(self.column_widths.update(ctx, event));
        self.shared.on_activate.borrow_mut().update(ctx, event);
        self.shared.on_select.borrow_mut().update(ctx, event);
        self.shared.on_edit.borrow_mut().update(ctx, event);
        self.shared.on_header_click.borrow_mut().update(ctx, event);
        if re {
            self.refresh(ctx);
        }
        match &*self.state.borrow() {
            TableState::Raeified(table) => table.update(ctx, waits, event),
            TableState::Refresh(_) => self.raeify(),
            TableState::Resolving(rpath) => match event {
                vm::Event::Netidx(_, _)
                | vm::Event::Rpc(_, _)
                | vm::Event::Variable(_, _, _)
                | vm::Event::User(LocalEvent::Event(_)) => (),
                vm::Event::User(LocalEvent::TableResolved(path, descriptor)) => {
                    if path == rpath {
                        match self.selection.current() {
                            None | Some(Value::Null) => {
                                self.shared.selected.borrow_mut().clear();
                            }
                            v @ Some(_) => {
                                self.shared.set_selection(v);
                            }
                        }
                        *self.shared.original_descriptor.borrow_mut() =
                            descriptor.clone();
                        self.raeify()
                    }
                }
            },
        }
    }

    fn root(&self) -> Option<&gtk::Widget> {
        Some(self.shared.root.upcast_ref())
    }

    fn set_visible(&self, v: bool) {
        self.visible.set(v);
        self.shared.root.set_visible(v);
        match &mut *self.state.borrow_mut() {
            TableState::Raeified(t) => {
                if v {
                    t.view().show();
                    idle_add_local(clone!(@strong t => move || {
                        t.update_subscriptions();
                        Continue(false)
                    }));
                } else {
                    t.view().hide()
                }
            }
            TableState::Refresh(_) | TableState::Resolving(_) => (),
        }
    }
}
