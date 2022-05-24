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
    /// expr should resolve to the path of the table you want to
    /// display. If the expr updates, the table will display the
    /// updated path.
    pub path: Expr,
    /// (null | false | external | <column> | spec)
    /// spec: [<column>, ("ascending" | "descending")]
    /// external: [false, spec]
    /// - null: no default sort. Sorting is processed within the
    /// browser and is under the control of the user. Click events will
    /// also be generated when the user clicks on the header button,
    /// see on_header_click.
    /// - false: sorting within the browser is disabled, but
    /// click events will still be generated when the user clicks on
    /// the header buttons. These events could be used to trigger
    /// publisher side sorting, or any other desired action. See,
    /// on_header_click.
    /// - external: just like `false`, however sort indicators will be
    /// shown as specified by the indicator spec. Use this if you
    /// implement sorting in the publisher, but want to give the user
    /// feedback about what is sorted.
    /// - <column>: by default sort by <column> in descending
    /// order. Sorting is processed within the browser and is under
    /// the user's control. Click events will also be generated when
    /// the user clicks on the header button, see on_header_click.
    /// - spec: Same as column, except the sort direction is
    /// explicitly specified.
    pub sort_mode: Expr,
    /// (null | true | false | list | range)
    /// list: [list-mode, (<col> | [<col>, ...])]
    /// range: [range-mode, ([(<n> | "start"), (<m> | "end")])]
    /// list-mode: ("include" | "exclude" | "include_match" | "exclude_match")
    /// range-mode: ("keep" | "drop")
    /// - null: all columns are included
    /// - true: all columns are included
    /// - false: no columns are included
    /// - list: Specify a list of columns or column regexes to include or exclude.
    ///     - "include": col/cols is a list of columns to show. The
    ///     order of the columns in the list controls the order of the
    ///     columns that are displayed.
    ///     - "exclude": col/cols is a list of columns to hide
    ///     - "include_match": col/cols is a list of regex patterns of columns to show
    ///     - "exclude_match": col/cols is a list of regex patterns of columns to hide

    /// - range: Specify columns to keep or drop by numeric
    /// ranges. Range patterns apply to the positions of columns
    /// sorted set.
    ///     - "keep": keep the columns specified by the range, drop
    ///     any others. If the range specifies more columns than exist
    ///     all columns will be kept. Matched items will be >= start
    ///     and < end.
    ///     - "drop": drop the columns specified by the range, keeping
    ///     the rest. If the range specifies more columns than exist
    ///     all the columns will be dropped. Matched items will be <
    ///     start or >= end.
    pub column_filter: Expr,
    /// see column_filter. This is exactly the same, but applies to
    /// the rows instead of the columns.
    pub row_filter: Expr,
    /// Exactly the same format as the column_filter and the row_filter.
    /// - null: not editable
    /// - true: every column is editable
    /// - otherwise: exactly the same semantics as the column/row filter
    /// except it decides whether the column is editable or not.
    pub column_editable: Expr,
    /// (null | widths)
    /// widths: [[<name>, <w>], ...]
    /// - null: initial column widths are automatically determined
    /// - widths: The list of numeric values specify the initial width of the
    /// corresponding column.
    pub column_widths: Expr,
    /// (true | false)
    /// - true: columns may be resized by the user
    /// - false: columns may not be resized by the user
    pub columns_resizable: Expr,
    /// (true | false)
    /// true: multi selection is enabled, multiple rows can be
    /// selected, on_select will produce an array of selected columns
    /// false: only a single cell can be selected at once, on_select
    /// will produce a single string
    pub multi_select: Expr,
    /// (true | false)
    /// true: show the row name column
    /// false: do not show the row name column
    pub show_row_name: Expr,
    /// event() will yield the selected path when the user selects a
    /// cell
    pub on_select: Expr,
    /// event() will yield the row path when the user activates a row,
    /// see multi_select
    pub on_activate: Expr,
    /// event() will yield the new value when the user edits a cell
    pub on_edit: Expr,
    /// event() will yield the name of the column header that was
    /// clicked
    pub on_header_click: Expr,
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
    pub tabs_position: TabPosition,
    pub tabs_visible: bool,
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
