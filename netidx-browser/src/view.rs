use serde::{Serialize, Deserialize};
use std::{
    boxed,
    cmp::{PartialEq, PartialOrd},
    default::Default,
};

/// A graphix expression stored as source text.
/// Compiled at view load time by the graphix compiler.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GxExpr(pub String);

impl Default for GxExpr {
    fn default() -> Self {
        GxExpr("null".into())
    }
}

impl GxExpr {
    pub fn is_default(&self) -> bool {
        self.0 == "null"
    }

    pub fn constant_str(s: &str) -> Self {
        GxExpr(format!("{:?}", s))
    }

    pub fn constant_bool(b: bool) -> Self {
        GxExpr(if b { "true" } else { "false" }.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Keybind {
    /// A string describing the keys that trigger the keybind.
    /// modifier keys are separated by a +. Available modifiers are
    /// ctrl, alt, shift, and super. As well as side specific variants
    /// of those, ctrl_l, ctrl_r, alt_l, alt_r, shift_l, shift_r,
    /// super_l, super_r.
    pub key: String,
    /// event() yields null when the key combo is pressed
    pub expr: GxExpr,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Direction {
    Horizontal,
    Vertical,
}

impl Default for Direction {
    fn default() -> Self {
        Direction::Vertical
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    /// expr should resolve to the path of the table you want to
    /// display. If the expr updates, the table will display the
    /// updated path.
    #[serde(default)]
    pub path: GxExpr,
    /// ```ignore
    /// (null | false | external | <column> | spec)
    /// external: [false, [spec, ...]]
    /// spec: [<column>, ("ascending" | "descending")]
    /// ```
    /// - null: no default sort. Sorting is processed within the
    /// browser and is under the control of the user. Click events will
    /// also be generated when the user clicks on the header button,
    /// see on_header_click.
    /// - false: sorting within the browser is disabled, but
    /// click events will still be generated when the user clicks on
    /// the header buttons. These events could be used to trigger
    /// publisher side sorting, or any other desired action. See,
    /// on_header_click.
    /// - external: just like 'false', however sort indicators will be
    /// shown as specified by the indicator spec. Use this if you
    /// implement sorting in the publisher, but want to give the user
    /// feedback about what is sorted.
    /// - column: by default sort by <column> in descending
    /// order. Sorting is processed within the browser and is under
    /// the user's control. Click events will also be generated when
    /// the user clicks on the header button, see on_header_click.
    /// - spec: Same as column, except the sort direction is
    /// explicitly specified.
    #[serde(default)]
    pub sort_mode: GxExpr,
    /// ```ignore
    /// (null | true | false | list | range)
    /// list: [list-mode, (<col> | [<col>, ...])]
    /// range: [range-mode, ([(<n> | "start"), (<m> | "end")])]
    /// list-mode: ("include" | "exclude" | "include_match" | "exclude_match")
    /// range-mode: ("keep" | "drop")
    /// ```
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
    /// in the sorted set.
    ///     - "keep": keep the columns specified by the range, drop
    ///     any others. If the range specifies more columns than exist
    ///     all columns will be kept. Matched items will be >= start
    ///     and < end.
    ///     - "drop": drop the columns specified by the range, keeping
    ///     the rest. If the range specifies more columns than exist
    ///     all the columns will be dropped. Matched items will be <
    ///     start or >= end.
    #[serde(default)]
    pub column_filter: GxExpr,
    /// see column_filter. This is exactly the same, but applies to
    /// the rows instead of the columns.
    #[serde(default)]
    pub row_filter: GxExpr,
    /// Exactly the same format as the column_filter and the row_filter.
    /// - null: not editable
    /// - true: every column is editable
    /// - otherwise: exactly the same semantics as the column/row filter
    /// except it decides whether the column is editable or not.
    #[serde(default)]
    pub column_editable: GxExpr,
    /// ```ignore
    /// (null | widths)
    /// widths: [[<name>, <w>], ...]
    /// ```
    /// - null: initial column widths are automatically determined
    /// - widths: The list of numeric values specify the initial width of the
    /// corresponding column.
    #[serde(default)]
    pub column_widths: GxExpr,
    /// ```ignore
    /// (true | false)
    /// ```
    /// - true: columns may be resized by the user
    /// - false: columns may not be resized by the user
    #[serde(default)]
    pub columns_resizable: GxExpr,
    /// ```ignore
    /// (null | column_types)
    /// column_types: [[<name>, typename, properties], ...]
    /// typename: ("text" | "toggle" | "image" | "combo" | "spin" | "progress" | "hidden")
    /// ```
    /// See the full column_types documentation for property details.
    /// - null: a default column type specification is generated that
    /// displays all the columns in the filtered model as text.
    #[serde(default)]
    pub column_types: GxExpr,
    /// ```ignore
    /// ("none" | "single" | "multi")
    /// ```
    #[serde(default)]
    pub selection_mode: GxExpr,
    /// ```ignore
    /// (null | selection)
    /// selection: [<path>, ...]
    /// ```
    #[serde(default)]
    pub selection: GxExpr,
    /// ```ignore
    /// (true | false)
    /// true: show the row name column
    /// false: do not show the row name column
    /// ```
    #[serde(default)]
    pub show_row_name: GxExpr,
    /// <any>
    /// when this updates the table structure will be reloaded from
    /// netidx
    #[serde(default)]
    pub refresh: GxExpr,
    /// event() will yield a list of selected paths when the user
    /// changes the selection
    #[serde(default)]
    pub on_select: GxExpr,
    /// event() will yield the row path when the user activates a row
    #[serde(default)]
    pub on_activate: GxExpr,
    /// When the user edits a cell event() will yield an array. The
    /// first element will be the full path to the source of the cell
    /// that was edited. The second element will be the new value of
    /// the cell.
    #[serde(default)]
    pub on_edit: GxExpr,
    /// event() will yield the name of the column header that was
    /// clicked
    #[serde(default)]
    pub on_header_click: GxExpr,
}

impl Default for Table {
    fn default() -> Self {
        Self {
            path: GxExpr(r#""/""#.into()),
            sort_mode: GxExpr::default(),
            column_filter: GxExpr::default(),
            row_filter: GxExpr::default(),
            column_editable: GxExpr::default(),
            column_widths: GxExpr::default(),
            columns_resizable: GxExpr::default(),
            column_types: GxExpr::default(),
            selection_mode: GxExpr::constant_str("single"),
            selection: GxExpr::default(),
            show_row_name: GxExpr::constant_bool(true),
            refresh: GxExpr::default(),
            on_select: GxExpr::default(),
            on_edit: GxExpr::default(),
            on_activate: GxExpr::default(),
            on_header_click: GxExpr::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Image {
    /// (<icon-name> | icon-spec | <image-bytes> | image-spec)
    /// icon-spec: [<icon-name>, icon-size]
    /// icon-size: ("menu" | "small-toolbar" | "large-toolbar" | "dnd" | "dialog")
    /// image-spec: [
    ///    ["image", <image-bytes>],
    ///    ["width", <desired-width>],
    ///    ["height", <desired-height>],
    ///    ["keep-aspect", (true | false)]
    /// ]
    #[serde(default)]
    pub spec: GxExpr,
    /// event will yield null when the image is clicked
    #[serde(default)]
    pub on_click: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Label {
    /// ("none" | "start" | "middle" | "end")
    /// whether and where to draw elipsis if text exceeds width,
    #[serde(default)]
    pub ellipsize: GxExpr,
    /// The text of the label
    #[serde(default)]
    pub text: GxExpr,
    /// (null | <n>)
    /// null: no limit on width
    /// <n>: the desired maximum width in characters
    #[serde(default)]
    pub width: GxExpr,
    /// (true | false)
    /// The label is restricted to a single line, or not.
    #[serde(default)]
    pub single_line: GxExpr,
    /// (true | false)
    /// The label can be selected or not
    #[serde(default)]
    pub selectable: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Button {
    /// The text to be displayed in the button
    #[serde(default)]
    pub label: GxExpr,
    /// see Image::spec
    #[serde(default)]
    pub image: GxExpr,
    /// event() will yield null when the button is clicked
    #[serde(default)]
    pub on_click: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LinkButton {
    /// The uri formatted string of the link
    #[serde(default)]
    pub uri: GxExpr,
    /// The link label
    #[serde(default)]
    pub label: GxExpr,
    /// event() will yield the uri when the link is activated
    #[serde(default)]
    pub on_activate_link: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Switch {
    /// The current state of the toggle
    #[serde(default)]
    pub value: GxExpr,
    /// event() will yield the new state of the toggle when it is
    /// clicked
    #[serde(default)]
    pub on_change: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ToggleButton {
    #[serde(default)]
    pub toggle: Switch,
    /// The text to be displayed in the button
    #[serde(default)]
    pub label: GxExpr,
    /// see Image::spec
    #[serde(default)]
    pub image: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpinButton {
    #[serde(default)]
    pub current: GxExpr,
    /// The minimum value that can be entered
    #[serde(default)]
    pub min: GxExpr,
    /// The maximum value that can be entered
    #[serde(default)]
    pub max: GxExpr,
    /// event() will yield the new value when the spinbutton is
    /// changed.
    #[serde(default)]
    pub on_change: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ComboBox {
    /// [choice, ...]
    /// choice: [<id>, <long-name>]
    #[serde(default)]
    pub choices: GxExpr,
    /// The id of the currently selected choice
    #[serde(default)]
    pub selected: GxExpr,
    /// event() will yield the id of the choice the user just selected
    #[serde(default)]
    pub on_change: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RadioButton {
    /// The text label that will appear next to the radio button
    #[serde(default)]
    pub label: GxExpr,
    /// See Image::spec
    #[serde(default)]
    pub image: GxExpr,
    /// The name of the group that this radio button belongs to. Only
    /// one radio button in a group may be selected at a time.
    #[serde(default)]
    pub group: GxExpr,
    /// Whether this radio button is toggled or not.
    #[serde(default)]
    pub value: GxExpr,
    /// event() will yield true if the radio button is selected and false if it is not
    #[serde(default)]
    pub on_toggled: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Entry {
    /// The currently displayed text
    #[serde(default)]
    pub text: GxExpr,
    /// event() will yield the new text whenever the user makes a
    /// change
    #[serde(default)]
    pub on_change: GxExpr,
    /// event() will yield the new text when the user activates the
    /// entry (e.g. presses <return>)
    #[serde(default)]
    pub on_activate: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SearchEntry {
    /// The currently displayed text
    #[serde(default)]
    pub text: GxExpr,
    /// when the user makes a change event() will yield, after some
    /// delay, the new text.
    #[serde(default)]
    pub on_search_changed: GxExpr,
    /// event() will yield the entry text when the user activates the
    /// entry.
    #[serde(default)]
    pub on_activate: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Scale {
    #[serde(default)]
    pub direction: Direction,
    /// (false | true | [position-spec, (null | <decimal-places>)])
    /// position-spec: ("left" | "right" | "top" | "bottom")
    /// true: draw the value with the default position and decimal places
    /// false: don't draw the current value
    #[serde(default)]
    pub draw_value: GxExpr,
    /// [mark, ...]
    /// mark: [<pos>, position-spec, (null | <text>)]
    #[serde(default)]
    pub marks: GxExpr,
    /// (true | false)
    #[serde(default)]
    pub has_origin: GxExpr,
    /// The current value of the scale.
    #[serde(default)]
    pub value: GxExpr,
    /// The minimum value of the scale, as a float
    #[serde(default)]
    pub min: GxExpr,
    /// The maximum value of the scale, as a float
    #[serde(default)]
    pub max: GxExpr,
    /// event() will yield the new value of the scale when it changes
    #[serde(default)]
    pub on_change: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProgressBar {
    /// ("none" | "start" | "middle" | "end")
    #[serde(default)]
    pub ellipsize: GxExpr,
    /// (null | <percent>)
    #[serde(default)]
    pub fraction: GxExpr,
    /// This will indicate activity every time the pulse expression
    /// updates.
    #[serde(default)]
    pub pulse: GxExpr,
    /// the text that will be shown if show_text is true
    #[serde(default)]
    pub text: GxExpr,
    /// (true | false)
    #[serde(default)]
    pub show_text: GxExpr,
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, PartialOrd, Eq, Ord, Deserialize)]
pub enum Align {
    Fill,
    Start,
    End,
    Center,
    Baseline,
}

impl Default for Align {
    fn default() -> Self {
        Align::Fill
    }
}

#[derive(Debug, Copy, Clone, Serialize, PartialEq, PartialOrd, Eq, Ord, Deserialize)]
pub enum Pack {
    Start,
    End,
}

impl Default for Pack {
    fn default() -> Self {
        Pack::Start
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BoxChild {
    #[serde(default)]
    pub pack: Pack,
    #[serde(default)]
    pub padding: u64,
    #[serde(default)]
    pub widget: boxed::Box<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Frame {
    #[serde(default)]
    pub label: GxExpr,
    #[serde(default)]
    pub label_align_horizontal: f32,
    #[serde(default)]
    pub label_align_vertical: f32,
    #[serde(default)]
    pub child: Option<boxed::Box<Widget>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Box {
    #[serde(default)]
    pub direction: Direction,
    #[serde(default)]
    pub homogeneous: bool,
    #[serde(default)]
    pub spacing: u32,
    #[serde(default)]
    pub children: Vec<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GridChild {
    #[serde(default)]
    pub width: u32,
    #[serde(default)]
    pub height: u32,
    #[serde(default)]
    pub widget: boxed::Box<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GridRow {
    #[serde(default)]
    pub columns: Vec<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Grid {
    #[serde(default)]
    pub homogeneous_columns: bool,
    #[serde(default)]
    pub homogeneous_rows: bool,
    #[serde(default)]
    pub column_spacing: u32,
    #[serde(default)]
    pub row_spacing: u32,
    #[serde(default)]
    pub rows: Vec<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Paned {
    #[serde(default)]
    pub direction: Direction,
    #[serde(default)]
    pub wide_handle: bool,
    #[serde(default)]
    pub first_child: Option<boxed::Box<Widget>>,
    #[serde(default)]
    pub second_child: Option<boxed::Box<Widget>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum TabPosition {
    Left,
    Right,
    Top,
    Bottom,
}

impl Default for TabPosition {
    fn default() -> Self {
        Self::Top
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NotebookPage {
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub reorderable: bool,
    #[serde(default)]
    pub widget: boxed::Box<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Notebook {
    #[serde(default)]
    pub tabs_position: TabPosition,
    #[serde(default)]
    pub tabs_visible: bool,
    #[serde(default)]
    pub tabs_scrollable: bool,
    #[serde(default)]
    pub tabs_popup: bool,
    #[serde(default)]
    pub children: Vec<Widget>,
    #[serde(default)]
    pub page: GxExpr,
    #[serde(default)]
    pub on_switch_page: GxExpr,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct RGB {
    #[serde(default)]
    pub r: f64,
    #[serde(default)]
    pub g: f64,
    #[serde(default)]
    pub b: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Series {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub line_color: RGB,
    #[serde(default)]
    pub x: GxExpr,
    #[serde(default)]
    pub y: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LinePlot {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub x_label: String,
    #[serde(default)]
    pub y_label: String,
    #[serde(default)]
    pub x_labels: usize,
    #[serde(default)]
    pub y_labels: usize,
    #[serde(default)]
    pub x_grid: bool,
    #[serde(default)]
    pub y_grid: bool,
    #[serde(default)]
    pub fill: Option<RGB>,
    #[serde(default)]
    pub margin: u32,
    #[serde(default)]
    pub label_area: u32,
    #[serde(default)]
    pub x_min: GxExpr,
    #[serde(default)]
    pub x_max: GxExpr,
    #[serde(default)]
    pub y_min: GxExpr,
    #[serde(default)]
    pub y_max: GxExpr,
    #[serde(default)]
    pub keep_points: GxExpr,
    #[serde(default)]
    pub series: Vec<Series>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WidgetKind {
    /// A raw graphix expression rendered as a widget.
    BScript(GxExpr),
    Table(Table),
    Label(Label),
    Button(Button),
    LinkButton(LinkButton),
    Switch(Switch),
    ToggleButton(ToggleButton),
    CheckButton(ToggleButton),
    RadioButton(RadioButton),
    ComboBox(ComboBox),
    Entry(Entry),
    SearchEntry(SearchEntry),
    ProgressBar(ProgressBar),
    Scale(Scale),
    Image(Image),
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

impl Default for WidgetKind {
    fn default() -> Self {
        Self::Table(Table::default())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct WidgetProps {
    /// Horizontal alignment
    #[serde(default)]
    pub halign: Align,
    /// Vertical alignment
    #[serde(default)]
    pub valign: Align,
    /// Expand horizontally
    #[serde(default)]
    pub hexpand: bool,
    /// Expand vertically
    #[serde(default)]
    pub vexpand: bool,
    /// Top margin in pixels
    #[serde(default)]
    pub margin_top: u32,
    /// Bottom margin in pixels
    #[serde(default)]
    pub margin_bottom: u32,
    /// Start margin in pixels
    #[serde(default)]
    pub margin_start: u32,
    /// End margin in pixels
    #[serde(default)]
    pub margin_end: u32,
    /// Key bindings
    #[serde(default)]
    pub keybinds: Vec<Keybind>,
    /// (true | false)
    /// true: The widget can be interacted with
    /// false: The widget can't be interacted with
    #[serde(default)]
    pub sensitive: GxExpr,
    /// (true | false)
    /// true: The widget is visible as long as it's parent is visible
    /// false: The widget and all it's children are not visible
    #[serde(default)]
    pub visible: GxExpr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Widget {
    /// layout properties and interfaces shared by all widgets
    #[serde(default)]
    pub props: Option<WidgetProps>,
    #[serde(default)]
    pub kind: WidgetKind,
}
