use super::super::{BSCtx, BSNode};
use anyhow::{anyhow, bail};
use arcstr::ArcStr;
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use gdk::RGBA;
use glib;
use gtk::{prelude::*, Label, ListStore, ScrolledWindow, TreeIter};
use indexmap::{IndexMap, IndexSet};
use netidx::{
    chars::Chars, pack::Z64, path::Path, pool::Pooled, protocol::value::FromValue,
    resolver_client, subscriber::Value,
};
use rand::{thread_rng, Rng};
use regex::RegexSet;
use std::{
    cell::{Cell, RefCell},
    cmp::PartialEq,
    collections::{HashMap, HashSet},
    ops::Deref,
    result::Result,
    str::FromStr,
};

#[derive(Debug, Clone, Boxed)]
#[boxed_type(name = "NetidxValue")]
pub(super) struct BVal {
    pub(super) value: Value,
    pub(super) formatted: Pooled<String>,
}

impl Deref for BVal {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SortDir {
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
pub(super) enum SortSpec {
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
pub(super) enum Filter {
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
    pub(super) fn is_match(&self, i: usize, s: &str) -> bool {
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

    pub(super) fn sort_index(&self, s: &str) -> Option<usize> {
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
pub(super) enum SelectionMode {
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
pub(super) enum OrLoad<T> {
    Static(T),
    Load(i32),
    LoadWithFallback(T, i32),
}

impl<T: FromValue + Clone> OrLoad<T> {
    pub(super) fn load(&self, row: &TreeIter, store: &ListStore) -> Option<T> {
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
pub(super) enum OrLoadCol<T> {
    Static(T),
    Load(Chars),
    LoadWithFallback(T, Chars),
}

impl<T: Clone> OrLoadCol<T> {
    pub(super) fn resolve(&self, descriptor: &IndexDescriptor) -> Option<OrLoad<T>> {
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
pub(super) struct Color(pub(super) RGBA);

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
pub(super) struct CTCommonResolved {
    pub(super) source: i32,
    pub(super) source_column: Chars,
    pub(super) background: Option<OrLoad<Color>>,
}

#[derive(Clone, PartialEq)]
pub(super) struct ColumnTypeCommon {
    pub(super) source: Option<Chars>,
    pub(super) background: Option<OrLoadCol<Color>>,
}

impl ColumnTypeCommon {
    pub(super) fn resolve(
        &self,
        vector_mode: bool,
        name: &Chars,
        descriptor: &IndexDescriptor,
    ) -> Option<CTCommonResolved> {
        let (source, source_column) = if vector_mode {
            (Some(1), name.clone())
        } else {
            match &self.source {
                None => {
                    let id =
                        descriptor.cols.get_full(&**name).map(|(i, _, _)| (i + 1) as i32);
                    (id, name.clone())
                }
                Some(src) => {
                    let id =
                        descriptor.cols.get_full(&**src).map(|(i, _, _)| (i + 1) as i32);
                    (id, src.clone())
                }
            }
        };
        let background = self.background.as_ref().and_then(|v| v.resolve(descriptor));
        source.map(move |source| CTCommonResolved { source, source_column, background })
    }

    pub(super) fn from_props(
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
pub(super) struct ColumnTypeText {
    pub(super) common: ColumnTypeCommon,
    pub(super) foreground: Option<OrLoadCol<Color>>,
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
pub(super) struct ColumnTypeToggle {
    pub(super) common: ColumnTypeCommon,
    pub(super) radio: Option<OrLoadCol<bool>>,
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
pub(super) struct ColumnTypeCombo {
    pub(super) common: ColumnTypeCommon,
    pub(super) choices: OrLoadCol<Value>,
    pub(super) has_entry: Option<OrLoadCol<bool>>,
}

impl FromValue for ColumnTypeCombo {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        let mut props = v.cast_to::<FxHashMap<Chars, Value>>()?;
        let choices = or_load_prop!(props, "choices", "choices-column", Value)
            .ok_or_else(|| anyhow!("choices is required"))?;
        Ok(Self {
            common: ColumnTypeCommon::from_props(&mut props)?,
            has_entry: or_load_prop!(props, "has-entry", "has-entry-column", bool),
            choices,
        })
    }
}

#[derive(Clone, PartialEq)]
pub(super) struct ColumnTypeSpin {
    pub(super) common: ColumnTypeCommon,
    pub(super) min: Option<OrLoadCol<f64>>,
    pub(super) max: Option<OrLoadCol<f64>>,
    pub(super) increment: Option<OrLoadCol<f64>>,
    pub(super) page_increment: Option<OrLoadCol<f64>>,
    pub(super) climb_rate: Option<OrLoadCol<f64>>,
    pub(super) digits: Option<OrLoadCol<u32>>,
}

impl FromValue for ColumnTypeSpin {
    fn from_value(v: Value) -> anyhow::Result<Self> {
        let mut props = v.cast_to::<FxHashMap<Chars, Value>>()?;
        Ok(Self {
            common: ColumnTypeCommon::from_props(&mut props)?,
            min: or_load_prop!(props, "min", "min-column", f64),
            max: or_load_prop!(props, "max", "max-column", f64),
            increment: or_load_prop!(props, "increment", "increment-column", f64),
            page_increment: or_load_prop!(props, "page-increment", "page-increment-column", f64),
            climb_rate: or_load_prop!(props, "climb-rate", "climb-rate-column", f64),
            digits: or_load_prop!(props, "digits", "digits-column", u32),
        })
    }
}

#[derive(Clone, PartialEq)]
pub(super) struct ColumnTypeProgress {
    pub(super) common: ColumnTypeCommon,
    pub(super) activity_mode: Option<OrLoadCol<bool>>,
    pub(super) text: Option<OrLoadCol<Chars>>,
    pub(super) text_xalign: Option<OrLoadCol<f64>>,
    pub(super) text_yalign: Option<OrLoadCol<f64>>,
    pub(super) inverted: Option<OrLoadCol<bool>>,
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
pub(super) enum ColumnType {
    Text(ColumnTypeText),
    Toggle(ColumnTypeToggle),
    Image(ColumnTypeCommon),
    Combo(ColumnTypeCombo),
    Spin(ColumnTypeSpin),
    Progress(ColumnTypeProgress),
    Hidden,
}

#[derive(Clone, PartialEq)]
pub(super) struct ColumnSpec {
    pub(super) name: Chars,
    pub(super) typ: ColumnType,
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

pub(super) const NAME_COL: &str = "name";

#[derive(Debug, Clone)]
pub(super) struct IndexDescriptor {
    pub(super) rows: IndexSet<Path>,
    pub(super) cols: IndexMap<Path, Z64>,
}

impl IndexDescriptor {
    pub(super) fn from_descriptor(show_name: bool, d: &resolver_client::Table) -> Self {
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

pub(super) struct SharedState {
    pub(super) column_editable: RefCell<Filter>,
    pub(super) column_filter: RefCell<Filter>,
    pub(super) columns_resizable: Cell<bool>,
    pub(super) column_types: RefCell<Option<Vec<ColumnSpec>>>,
    pub(super) column_widths: RefCell<FxHashMap<String, i32>>,
    pub(super) ctx: BSCtx,
    pub(super) on_activate: RefCell<BSNode>,
    pub(super) on_edit: RefCell<BSNode>,
    pub(super) on_header_click: RefCell<BSNode>,
    pub(super) on_select: RefCell<BSNode>,
    pub(super) original_descriptor: RefCell<resolver_client::Table>,
    pub(super) path: RefCell<Path>,
    pub(super) root: ScrolledWindow,
    pub(super) row_filter: RefCell<Filter>,
    pub(super) selected_path: Label,
    pub(super) selected: RefCell<FxHashMap<String, FxHashSet<String>>>,
    pub(super) selection_mode: Cell<SelectionMode>,
    pub(super) show_name_column: Cell<bool>,
    pub(super) sort_mode: RefCell<SortSpec>,
}

impl SharedState {
    pub(super) fn new(
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

    pub(super) fn set_path(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Path, path, Some)
    }

    pub(super) fn set_sort_mode(&self, v: Option<Value>) -> bool {
        set_field!(self, v, SortSpec, sort_mode, Some)
    }

    pub(super) fn set_column_types(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Option<Vec<ColumnSpec>>, column_types, Some)
    }

    pub(super) fn set_column_filter(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Filter, column_filter, Some)
    }

    pub(super) fn set_row_filter(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Filter, row_filter, Some)
    }

    pub(super) fn set_column_editable(&self, v: Option<Value>) -> bool {
        set_field!(self, v, Filter, column_editable, Some)
    }

    pub(super) fn set_selection_mode(&self, v: Option<Value>) -> bool {
        set_field_cell!(self, v, SelectionMode, selection_mode)
    }

    pub(super) fn set_selection(&self, v: Option<Value>) -> bool {
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

    pub(super) fn set_show_row_name(&self, v: Option<Value>) -> bool {
        set_field_cell!(self, v, bool, show_name_column)
    }

    pub(super) fn set_columns_resizable(&self, v: Option<Value>) -> bool {
        set_field_cell!(self, v, bool, columns_resizable)
    }

    pub(super) fn set_column_widths(&self, v: Option<Value>) -> bool {
        set_field!(self, v, FxHashMap<String, i32>, column_widths, Some)
    }

    pub(super) fn apply_filters(&self) -> (bool, IndexDescriptor) {
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

    pub(super) fn generate_column_spec(
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
