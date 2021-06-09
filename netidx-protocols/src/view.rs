use netidx::{path::Path, publisher::Value};
use netidx_bscript::expr::Expr;
use std::{
    boxed,
    cmp::{PartialEq, PartialOrd},
    collections::HashMap,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Keybind {
    pub key: String,
    pub action: Expr,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Direction {
    Horizontal,
    Vertical,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SortDir {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnSpec {
    Exactly(Vec<String>),
    Hide(Vec<String>),
    Auto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub path: Path,
    pub default_sort_column: Option<(String, SortDir)>,
    pub columns: ColumnSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Button {
    pub enabled: Expr,
    pub label: Expr,
    pub on_click: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Toggle {
    pub enabled: Expr,
    pub value: Expr,
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Selector {
    pub enabled: Expr,
    pub choices: Expr,
    pub selected: Expr,
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub enabled: Expr,
    pub visible: Expr,
    pub text: Expr,
    pub on_change: Expr,
    pub on_activate: Expr,
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, PartialOrd, Eq, Ord, Deserialize)]
pub enum Align {
    Fill,
    Start,
    End,
    Center,
    Baseline,
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, PartialOrd, Eq, Ord, Deserialize)]
pub enum Pack {
    Start,
    End,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoxChild {
    pub pack: Pack,
    pub padding: u64,
    pub widget: boxed::Box<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Box {
    pub direction: Direction,
    pub homogeneous: bool,
    pub spacing: u32,
    pub children: Vec<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridChild {
    pub width: u32,
    pub height: u32,
    pub widget: boxed::Box<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridRow {
    pub columns: Vec<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grid {
    pub homogeneous_columns: bool,
    pub homogeneous_rows: bool,
    pub column_spacing: u32,
    pub row_spacing: u32,
    pub rows: Vec<Widget>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RGB {
    pub r: f64,
    pub g: f64,
    pub b: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Series {
    pub title: String,
    pub line_color: RGB,
    pub x: Expr,
    pub y: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinePlot {
    pub title: String,
    pub x_label: String,
    pub y_label: String,
    pub x_labels: usize,
    pub y_labels: usize,
    pub x_grid: bool,
    pub y_grid: bool,
    pub fill: Option<RGB>,
    pub margin: u32,
    pub label_area: u32,
    pub x_min: Expr,
    pub x_max: Expr,
    pub y_min: Expr,
    pub y_max: Expr,
    pub keep_points: Expr,
    pub series: Vec<Series>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WidgetKind {
    Action(Expr),
    Table(Table),
    Label(Expr),
    Button(Button),
    Toggle(Toggle),
    Selector(Selector),
    Entry(Entry),
    Box(Box),
    BoxChild(BoxChild),
    Grid(Grid),
    GridChild(GridChild),
    GridRow(GridRow),
    LinePlot(LinePlot),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WidgetProps {
    pub halign: Align,
    pub valign: Align,
    pub hexpand: bool,
    pub vexpand: bool,
    pub margin_top: u32,
    pub margin_bottom: u32,
    pub margin_start: u32,
    pub margin_end: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Widget {
    pub props: Option<WidgetProps>,
    pub kind: WidgetKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub variables: HashMap<String, Value>,
    pub keybinds: Vec<Keybind>,
    pub root: Widget,
}
