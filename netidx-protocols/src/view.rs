use netidx_bscript::expr::Expr;
use std::{
    boxed,
    cmp::{PartialEq, PartialOrd},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Keybind {
    /// A string describing the keys that trigger the keybind.
    /// modifier keys are separated by a +. Available modifiers are
    /// ctrl, alt, shift, and super. As well as side specific variants
    /// of those, ctrl_l, ctrl_r, alt_l, alt_r, shift_l, shift_r,
    /// super_l, super_r.
    pub key: String,
    /// event() yields null when the key combo is pressed
    pub expr: Expr,
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
    /// external: [false, spec]
    /// spec: [<column>, ("ascending" | "descending")]
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
pub struct Image {
    /// (<icon-name> | icon-spec | <image-bytes> | image-spec)
    /// icon-spec: [<icon-name>, icon-size]
    /// icon-size: ("menu" | "small-toolbar" | "large-toolbar" | "dnd" | "dialog")
    /// image-spec: [
    ///    ["image", <image-bytes>],
    ///    ["width": <desired-width>],
    ///    ["height", <desired-height>],
    ///    ["keep-aspect", (true | false)]
    /// ]
    /// - <icon-name>: A string naming the stock icon from the current
    /// theme that should be displayed. The default size is "small-toolbar".
    /// - icon-spec: A pair specifying the icon name and the icon size.
    /// - icon-size: The size of the icon
    /// - <image-bytes>: A bytes value containing the image in any
    /// format supported by gdk_pixbuf.
    /// - image-spec: an alist containing the image bytes in any format
    /// supported by gdk_pixbuf and some metadata.
    ///   - image: the image bytes, required.
    ///   - width: optional, if specified the image will be scaled to
    ///   the specified width. If keep-aspect is true then the height
    ///   will also be scaled to keep the image's aspect ratio even if
    ///   height is not specified.
    ///   - height: optional, if specifed the image will be scaled to
    ///   the specified height. If keep-aspect is true then the width
    ///   will also be scaled to keep the image's aspect ratio even if
    ///   width is not specified.
    pub spec: Expr,
    /// event will yield null when the image is clicked
    pub on_click: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Label {
    /// ("none" | "start" | "middle" | "end")
    /// whether and where to draw elipsis if text exceeds width,
    pub ellipsize: Expr,
    /// The text of the label
    pub text: Expr,
    /// (null | <n>)
    /// null: no limit on width
    /// <n>: the desired maximum width in characters
    pub width: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Button {
    /// The text to be displayed in the button
    pub label: Expr,
    /// see Image::spec
    pub image: Expr,
    /// event() will yield null when the button is clicked
    pub on_click: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkButton {
    /// The uri formatted string of the link
    pub uri: Expr,
    /// The link label
    pub label: Expr,
    /// event() will yield the uri when the link is activated
    pub on_activate_link: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Switch {
    /// The current state of the toggle
    pub value: Expr,
    /// event() will yield the new state of the toggle when it is
    /// clicked
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToggleButton {
    toggle: Switch,
    /// The text to be displayed in the button
    label: Expr,
    /// see Image::spec
    image: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpinButton {
    pub current: Expr,
    /// The minimum value that can be entered
    pub min: Expr,
    /// The maximum value that can be entered
    pub max: Expr,
    /// event() will yield the new value when the spinbutton is
    /// changed.
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComboBoxText {
    /// The set of choices. This should be a list of strings.
    pub choices: Expr,
    /// The currently selected choice
    pub selected: Expr,
    /// event() will yield the choice the user just selected
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// The currently displayed text
    pub text: Expr,
    /// event() will yield the new text whenever the user makes a
    /// change
    pub on_change: Expr,
    /// event() will yield the new text when the user activates the
    /// entry (e.g. presses <return>)
    pub on_activate: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scale {
    pub dir: Direction,
    /// The current value of the scale. Should be a float between min
    /// and max
    pub current: Expr,
    /// The minimum value of the scale, as a float
    pub min: Expr,
    /// The maximum value of the scale, as a float
    pub max: Expr,
    /// event() will yield the new value of the scale when it changes
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressBar {
    /// ("none" | "start" | "middle" | "end")
    /// whether and where to draw elipsis if text exceeds width,
    pub ellipsize: Expr,
    /// (null | <percent>)
    /// null: in this case the progress bar will operate in "activity
    /// mode", showing the user that something is happening, but not
    /// giving any indication of when it will be finished. Use pulse
    /// to indicate activity in this mode
    /// <percent>: the progress from 0.0 to 1.0. If set the progress
    /// bar will operate in normal mode, showing how much of a given
    /// task is complete.
    pub fraction: Expr,
    /// This will indicate activity every time the pulse expression
    /// updates. Setting this to anything but null will cause the
    /// progress bar to operate in activity mode.
    pub pulse: Expr,
    /// the text that will be shown if show_text is true
    pub text: Expr,
    /// (true | false)
    /// false: no text is shown
    /// true: if text is a string then it will be shown on the
    /// progressbar, otherwise the value of fraction will be shown.
    pub show_text: Expr,
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
    /// event() will yield null when the view is initialized. Note,
    /// keybinds on BScript widgets will be ignored.
    BScript(Expr),
    Table(Table),
    Label(Label),
    Button(Button),
    LinkButton(LinkButton),
    Switch(Switch),
//    ToggleButton(ToggleButton),
//    CheckButton(ToggleButton),
//    RadioButton(ComboBoxText),
    ComboBoxText(ComboBoxText),
    Entry(Entry),
//    SearchEntry(Entry),
//    ProgressBar(ProgressBar),
//    Scale(Scale),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WidgetProps {
    /// Horizontal alignment
    pub halign: Align,
    /// Vertical alignment
    pub valign: Align,
    /// Expand horizontally
    pub hexpand: bool,
    /// Expand vertically
    pub vexpand: bool,
    /// Top margin in pixels
    pub margin_top: u32,
    /// Bottom margin in pixels
    pub margin_bottom: u32,
    /// Start margin in pixels
    pub margin_start: u32,
    /// End margin in pixels
    pub margin_end: u32,
    /// Key bindings
    pub keybinds: Vec<Keybind>,
    /// (true | false)
    /// true: The widget can be interacted with
    /// false: The widget can't be interacted with
    pub sensitive: Expr,
    /// (true | false)
    /// true: The widget is visible as long as it's parent is visible
    /// false: The widget and all it's children are not visible
    pub visible: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Widget {
    /// layout properties and interfaces shared by all widgets
    pub props: Option<WidgetProps>,
    pub kind: WidgetKind,
}
