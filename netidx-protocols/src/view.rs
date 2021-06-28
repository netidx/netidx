use netidx::publisher::Value;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub path: Expr,
    /// column name or Null, Null means no sorting by default
    pub default_sort_column: Expr,
    /// ascending, descending, or Null (same as descending)
    pub default_sort_column_direction: Expr,
    /// auto, exactly, hide, or Null (same as auto)
    ///
    /// auto: the default, automatically determine the column set, and
    /// show columns that are present in 50% or more of the rows.
    ///
    /// exactly: column_list specifies the exact list of columns that
    /// will appear, no auto discovery will happen, listed columns
    /// will appear even if they do not exist in the table.
    ///
    /// hide: column_list specifies the columns that will be omitted
    /// from the table. All columns not specified in the list will be
    /// shown, reguardless of whether or not they appear in 50% or
    /// more of the rows.
    pub column_mode: Expr,
    /// csv or json list of column names, interpreted according to
    /// column_mode.
    ///
    /// csv form: col1,col2,col\,3,col4 note the escaped
    /// comma
    ///
    /// json form: ["col1", "col2", "col,3", "col4"]
    pub column_list: Expr,
    /// either a boolean, which effects the entire table, or a csv or
    /// json list of columns that that are editable.
    pub editable: Expr,
    /// event() will yield the selected path when the user selects a
    /// cell
    pub on_select: Expr,
    /// event() will yield the row path when the user activates a row
    pub on_activate: Expr,
    /// event() will yield the new value when the user edits a cell
    pub on_edit: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Button {
    pub enabled: Expr,
    pub label: Expr,
    pub on_click: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkButton {
    pub enabled: Expr,
    pub uri: Expr,
    pub label: Expr,
    pub on_activate_link: Expr,
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
pub struct Frame {
    pub label: Expr,
    pub label_align_horizontal: f32,
    pub label_align_vertical: f32,
    pub child: Option<boxed::Box<Widget>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Paned {
    pub direction: Direction,
    pub wide_handle: bool,
    pub first_child: Option<boxed::Box<Widget>>,
    pub second_child: Option<boxed::Box<Widget>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TabPosition {
    Left,
    Right,
    Top,
    Bottom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotebookPage {
    pub label: String,
    pub reorderable: bool,
    pub widget: boxed::Box<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notebook {
    pub tabs_visible: bool,
    pub tabs_position: TabPosition,
    pub tabs_scrollable: bool,
    pub tabs_popup: bool,
    pub children: Vec<Widget>,
    pub page: Expr,
    pub on_switch_page: Expr,
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
    LinkButton(LinkButton),
    Toggle(Toggle),
    Selector(Selector),
    Entry(Entry),
    Frame(Frame),
    Box(Box),
    BoxChild(BoxChild),
    Grid(Grid),
    GridChild(GridChild),
    GridRow(GridRow),
    Paned(Paned),
    Notebook(Notebook),
    NotebookPage(NotebookPage),
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
