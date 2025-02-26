use arcstr::literal;
use netidx::protocol::value::Value;
use netidx_bscript::expr::{Expr, ExprKind};
use std::{
    boxed,
    cmp::{PartialEq, PartialOrd},
    default::Default,
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
    pub path: Expr,
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
    pub sort_mode: Expr,
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
    pub column_filter: Expr,
    /// see column_filter. This is exactly the same, but applies to
    /// the rows instead of the columns.
    #[serde(default)]
    pub row_filter: Expr,
    /// Exactly the same format as the column_filter and the row_filter.
    /// - null: not editable
    /// - true: every column is editable
    /// - otherwise: exactly the same semantics as the column/row filter
    /// except it decides whether the column is editable or not.
    #[serde(default)]
    pub column_editable: Expr,
    /// ```ignore
    /// (null | widths)
    /// widths: [[<name>, <w>], ...]
    /// ```
    /// - null: initial column widths are automatically determined
    /// - widths: The list of numeric values specify the initial width of the
    /// corresponding column.
    #[serde(default)]
    pub column_widths: Expr,
    /// ```ignore
    /// (true | false)
    /// ```
    /// - true: columns may be resized by the user
    /// - false: columns may not be resized by the user
    #[serde(default)]
    pub columns_resizable: Expr,
    /// ```ignore
    /// (null | column_types)
    /// column_types: [[<name>, typename, properties], ...]
    /// typename: ("text" | "toggle" | "image" | "combo" | "spin" | "progress" | "hidden")
    /// properties: match typename
    ///   common:
    ///     ["source", <column-name>],
    ///       optional, the source column that contains the data for
    ///       each row. If not specified the default is this column
    ///       (<name>).

    ///     ["background", <color-string>],
    ///       optional, statically specify the background color of the
    ///       cell. same format as the "foreground" attribute.
    ///
    ///     ["background-column", <column-name>],
    ///       optional, the column containing the background color of
    ///       each row in the same format as described in the
    ///       "foreground" attribute.
    ///
    ///   "text": [
    ///     common,
    ///
    ///     ["foreground", <color-string>],
    ///       optional, statically specify the foreground text
    ///       color. Any string understood by pango_parse_color is
    ///       understood here. That includes css color names, and hex
    ///       strings in various formats.
    ///
    ///     ["foreground-column", <column-name>],
    ///       optional, the column containing the foreground color of
    ///       each row, same format as for the "foreground" attribute.
    ///   ]
    ///   
    ///   "toggle": [
    ///     common,
    ///
    ///     ["radio", (true | false)],
    ///       optional, whether to render the toggle as a check button
    ///       or a radio button.
    ///
    ///       true: the entire column is radio buttons, only one row may
    ///       be selected at any one time
    ///
    ///       false: the entire column is check buttons, which may be
    ///       individually toggled.
    ///
    ///       <column-name>: the specified boolean column controls
    ///       whether or not the toggle in each row is a radio or a
    ///       check button.
    ///
    ///     ["radio-column", <column-name>]
    ///       optional, the column containing the "radio" property for
    ///       each row
    ///   ]
    ///
    ///   "image": [
    ///     common,
    ///   ]
    ///
    ///   "combo": [
    ///     common,
    ///
    ///     ["choices", [<choice>, ...]],
    ///       The available choices globally for the entire
    ///       column. One of choices or choices-column must be
    ///       specified
    ///
    ///     ["choices-column", <column-name>]
    ///       The column containing the "choices" attribute
    ///       for each row. One of choices, or choices-column must be
    ///       specified.
    ///
    ///     ["has-entry", (true | false)],
    ///       optional. Should the column have an entry in addition to
    ///       the combo box menu? If true the user will be able to
    ///       enter any text they want, even if it isn't a valid
    ///       choice. If false only valid choices may be entered.
    ///
    ///     ["has-entry-column", <column-name>]
    ///       optional, the column containing the has-entry attribute for each row.
    ///   ]
    ///
    ///   "spin": [
    ///      common,
    ///
    ///      ["min", <n>],
    ///        optional, if not specified 0 is assumed.
    ///
    ///      ["min-column", <column-name>]
    ///        optional, the column containing the min attribute for each row
    ///
    ///      ["max", <n>],
    ///        optional, if not specified 1 is assumed.
    ///
    ///      ["max-column", <column-name>]
    ///        optonal, the column containing the max attribute for each row
    ///
    ///      ["increment", <n>],
    ///        optional, the amount the value changes with each spin button press
    ///
    ///      ["increment-column", <column-name>],
    ///        optional, the column containg the increment property
    ///
    ///      ["page-increment", <n>]
    ///        optional, the amount the value changes on a page increment
    ///
    ///      ["page-increment-column", <column-name>],
    ///        optional, the column containg the page-increment property
    ///
    ///      ["climb-rate", <n>],
    ///        optional. How fast the value should change if the user
    ///        holds the + or - button down.
    ///
    ///      ["climb-rate-column", <column-name>]
    ///        optional, the column specifying the climb-rate attribute for each row
    ///
    ///      ["digits", <n>],
    ///        optional. The number of decimal places to display.
    ///
    ///      ["digits-column", <column-name>]
    ///        optional. The column specifying the digits attribute for each row.
    ///   ]
    ///
    ///   "progress": [
    ///     common,
    ///       
    ///     ["activity-mode", (true | false)],
    ///       optional, default false. Operate the progressbar in
    ///       activity mode (see the ProgressBar widget).
    ///
    ///     ["activity-mode-column", <column-name>]
    ///       optional, the column specifying the activity mode for each row.
    ///
    ///     ["text", <text>],
    ///       optional, display static text near the progress bar
    ///
    ///     ["text-column", <column-name>],
    ///       optional, display text from <column-name> near the
    ///       progress bar.
    ///
    ///     ["text-xalign", <n>],
    ///       optional, set the horizontal alignment of the displayed
    ///       text. 0 is full left, 1 is full right.
    ///
    ///     ["text-xalign-column", <column-name>]
    ///       optional, the column specifying the text-xalign property for each row
    ///
    ///     ["text-yalign", <n>],
    ///       optional, set the vertical alignment of the displayed
    ///       text. 0 is top, 1 is bottom.
    ///
    ///     ["text-yalign-column", <column-name>]
    ///       optonal, the column specifying the text-yalign property for each row
    ///
    ///     ["inverted", (true | false)],
    ///       optional, invert the meaning of the source data
    ///
    ///     ["inverted-column", <column-name>]
    ///       optional, the column specifying the inverted property for each row
    ///  ]
    ///
    ///  "hidden":
    ///    hidden is a special column type that has no properties. It
    ///    is used to hide data columns that other visible columns
    ///    depend on (so they must appear in the model), but that you
    ///    don't want to show to the user.
    ///
    ///  all the properties of progress are optional. If none are set
    ///  the entire properties array may be omitted
    /// ```
    ///
    /// - null: a default column type specification is generated that
    /// displays all the columns in the filtered model as text.
    ///
    /// ## Notes
    ///
    /// The column type specification need not be total, any column
    /// not given a type will be assumed to be a text column with no
    /// properties assigned.
    ///
    /// The column type specification interacts with the column
    /// filter, in that a column type specification may name another
    /// column as the source of it's data or of a given property and
    /// the column filter may remove that column. If that occurrs the
    /// column filter takes precidence. The specified typed column
    /// will be displayed, but won't get any data if it's underlying
    /// column is filtered out.
    ///
    /// For properties that can be statically specified and loaded
    /// from a column, if both ways are specified then the column will
    /// override the static specifiecation. If the column data is
    /// missing or invalid for a given row, then the static
    /// specification will be used.
    #[serde(default)]
    pub column_types: Expr,
    /// ```ignore
    /// ("none" | "single" | "multi")
    /// ```
    /// - "none": user selection is not allowed. The cursor (text focus)
    /// can still be moved, but cells will not be highlighted and no
    /// on_select events will be generated in response to user
    /// inputs. The selection expression still cause cells to be
    /// selected.
    /// - "single": one cell at a time may be selected.
    /// - "multi": multi selection is enabled, the user may select
    /// multiple cells by shift clicking. on_select will generate an
    /// array of selected cells every time the selection changes.
    #[serde(default)]
    pub selection_mode: Expr,
    /// ```ignore
    /// (null | selection)
    /// selection: [<path>, ...]
    /// ```
    /// - null: The selection is maintained internally under the control
    /// of the user, or completely disabled if the selection_mode is
    /// "none".
    /// - selection: A list of selected paths. on_select events are not
    /// triggered when this expression updates unless the row or
    /// column filters modify the selection by removing selected rows
    /// or columns from the table.
    ///
    #[serde(default)]
    pub selection: Expr,
    /// ```ignore
    /// (true | false)
    /// true: show the row name column
    /// false: do not show the row name column
    /// ```
    #[serde(default)]
    pub show_row_name: Expr,
    /// <any>
    /// when this updates the table structure will be reloaded from
    /// netidx
    #[serde(default)]
    pub refresh: Expr,
    /// event() will yield a list of selected paths when the user
    /// changes the selection
    #[serde(default)]
    pub on_select: Expr,
    /// event() will yield the row path when the user activates a row,
    /// see multi_select
    #[serde(default)]
    pub on_activate: Expr,
    /// When the user edits a cell event() will yield an array. The
    /// first element will be the full path to the source of the cell
    /// that was edited. The second element will be the new value of
    /// the cell.
    #[serde(default)]
    pub on_edit: Expr,
    /// event() will yield the name of the column header that was
    /// clicked
    #[serde(default)]
    pub on_header_click: Expr,
}

impl Default for Table {
    fn default() -> Self {
        Self {
            path: ExprKind::Constant(Value::String(literal!("/"))).to_expr(),
            sort_mode: Expr::default(),
            column_filter: Expr::default(),
            row_filter: Expr::default(),
            column_editable: Expr::default(),
            column_widths: Expr::default(),
            columns_resizable: Expr::default(),
            column_types: Expr::default(),
            selection_mode: ExprKind::Constant(Value::String(literal!("single")))
                .to_expr(),
            selection: Expr::default(),
            show_row_name: ExprKind::Constant(Value::Bool(true)).to_expr(),
            refresh: Expr::default(),
            on_select: Expr::default(),
            on_edit: Expr::default(),
            on_activate: Expr::default(),
            on_header_click: Expr::default(),
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
    ///      required, the image bytes.
    ///
    ///    ["width", <desired-width>],
    ///      optional, if specified the image will be scaled to the
    ///      specified width. If keep-aspect is true then the height
    ///      will also be scaled to keep the image's aspect ratio even
    ///      if height is not specified.
    ///
    ///    ["height", <desired-height>],
    ///      optional, if specifed the image will be scaled to the
    ///      specified height. If keep-aspect is true then the width
    ///      will also be scaled to keep the image's aspect ratio even
    ///      if width is not specified.
    ///
    ///    ["keep-aspect", (true | false)]
    ///      optional, keep the aspect ratio of the image.
    /// ]
    /// - <icon-name>: A string naming the stock icon from the current
    /// theme that should be displayed. The default size is "small-toolbar".
    /// - icon-spec: A pair specifying the icon name and the icon size.
    /// - icon-size: The size of the icon
    /// - <image-bytes>: A bytes value containing the image in any
    /// format supported by gdk_pixbuf.
    /// - image-spec: an alist containing the image bytes in any format
    /// supported by gdk_pixbuf and some metadata.
    #[serde(default)]
    pub spec: Expr,
    /// event will yield null when the image is clicked
    #[serde(default)]
    pub on_click: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Label {
    /// ("none" | "start" | "middle" | "end")
    /// whether and where to draw elipsis if text exceeds width,
    #[serde(default)]
    pub ellipsize: Expr,
    /// The text of the label
    #[serde(default)]
    pub text: Expr,
    /// (null | <n>)
    /// null: no limit on width
    /// <n>: the desired maximum width in characters
    #[serde(default)]
    pub width: Expr,
    /// (true | false)
    /// The label is restricted to a single line, or not.
    #[serde(default)]
    pub single_line: Expr,
    /// (true | false)
    /// The label can be selected or note
    #[serde(default)]
    pub selectable: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Button {
    /// The text to be displayed in the button
    #[serde(default)]
    pub label: Expr,
    /// see Image::spec
    #[serde(default)]
    pub image: Expr,
    /// event() will yield null when the button is clicked
    #[serde(default)]
    pub on_click: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LinkButton {
    /// The uri formatted string of the link
    #[serde(default)]
    pub uri: Expr,
    /// The link label
    #[serde(default)]
    pub label: Expr,
    /// event() will yield the uri when the link is activated
    #[serde(default)]
    pub on_activate_link: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Switch {
    /// The current state of the toggle
    #[serde(default)]
    pub value: Expr,
    /// event() will yield the new state of the toggle when it is
    /// clicked
    #[serde(default)]
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ToggleButton {
    #[serde(default)]
    pub toggle: Switch,
    /// The text to be displayed in the button
    #[serde(default)]
    pub label: Expr,
    /// see Image::spec
    #[serde(default)]
    pub image: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SpinButton {
    #[serde(default)]
    pub current: Expr,
    /// The minimum value that can be entered
    #[serde(default)]
    pub min: Expr,
    /// The maximum value that can be entered
    #[serde(default)]
    pub max: Expr,
    /// event() will yield the new value when the spinbutton is
    /// changed.
    #[serde(default)]
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ComboBox {
    /// [choice, ...]
    /// choice: [<id>, <long-name>]
    #[serde(default)]
    pub choices: Expr,
    /// The id of the currently selected choice
    #[serde(default)]
    pub selected: Expr,
    /// event() will yield the id of the choice the user just selected
    #[serde(default)]
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RadioButton {
    /// The text label that will appear next to the radio button
    #[serde(default)]
    pub label: Expr,
    /// See Image::spec
    #[serde(default)]
    pub image: Expr,
    /// The name of the group that this radio button belongs to. Only
    /// one radio button in a group may be selected at a time.
    #[serde(default)]
    pub group: Expr,
    /// Whether this radio button is toggled or not.
    #[serde(default)]
    pub value: Expr,
    /// event() will yield true if the radio button is selected and false if it is not
    #[serde(default)]
    pub on_toggled: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Entry {
    /// The currently displayed text
    #[serde(default)]
    pub text: Expr,
    /// event() will yield the new text whenever the user makes a
    /// change
    #[serde(default)]
    pub on_change: Expr,
    /// event() will yield the new text when the user activates the
    /// entry (e.g. presses <return>)
    #[serde(default)]
    pub on_activate: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SearchEntry {
    /// The currently displayed text
    #[serde(default)]
    pub text: Expr,
    /// when the user makes a change event() will yield, after some
    /// delay, the new text.
    #[serde(default)]
    pub on_search_changed: Expr,
    /// event() will yield the entry text when the user activates the
    /// entry.
    #[serde(default)]
    pub on_activate: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Scale {
    #[serde(default)]
    pub direction: Direction,
    /// (false | true | [position-spec, (null | <decimal-places>)])
    /// position-spec: ("left" | "right" | "top" | "bottom")
    /// true: draw the value with the default position and decimal places
    /// false: don't draw the current value
    /// if position-spec and decimal places is specified the value
    /// will be rounded to the specified number of decimal places and
    /// displayed at the specified position. Otherwise the value will
    /// not be rounded and will be displayed at the default position.
    #[serde(default)]
    pub draw_value: Expr,
    /// [mark, ...]
    /// mark: [<pos>, position-spec, (null | <text>)]
    /// position-spec: ("left" | "right" | "top" | "bottom")
    /// A list of marks you want to display on the scale, along with
    /// any text you want to display next to the mark.
    #[serde(default)]
    pub marks: Expr,
    /// (true | false)
    /// true: highlight the trough between the origin (bottom left
    /// side) and the current value.
    /// false: don't highlight anything.
    #[serde(default)]
    pub has_origin: Expr,
    /// The current value of the scale. Should be a float between min
    /// and max
    #[serde(default)]
    pub value: Expr,
    /// The minimum value of the scale, as a float
    #[serde(default)]
    pub min: Expr,
    /// The maximum value of the scale, as a float
    #[serde(default)]
    pub max: Expr,
    /// event() will yield the new value of the scale when it changes
    #[serde(default)]
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProgressBar {
    /// ("none" | "start" | "middle" | "end")
    /// whether and where to draw elipsis if text exceeds width,
    #[serde(default)]
    pub ellipsize: Expr,
    /// (null | <percent>)
    /// null: in this case the progress bar will operate in "activity
    /// mode", showing the user that something is happening, but not
    /// giving any indication of when it will be finished. Use pulse
    /// to indicate activity in this mode
    /// <percent>: the progress from 0.0 to 1.0. If set the progress
    /// bar will operate in normal mode, showing how much of a given
    /// task is complete.
    #[serde(default)]
    pub fraction: Expr,
    /// This will indicate activity every time the pulse expression
    /// updates. Setting this to anything but null will cause the
    /// progress bar to operate in activity mode.
    #[serde(default)]
    pub pulse: Expr,
    /// the text that will be shown if show_text is true
    #[serde(default)]
    pub text: Expr,
    /// (true | false)
    /// false: no text is shown
    /// true: if text is a string then it will be shown on the
    /// progressbar, otherwise the value of fraction will be shown.
    #[serde(default)]
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
    pub label: Expr,
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
    pub page: Expr,
    #[serde(default)]
    pub on_switch_page: Expr,
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
    pub x: Expr,
    #[serde(default)]
    pub y: Expr,
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
    pub x_min: Expr,
    #[serde(default)]
    pub x_max: Expr,
    #[serde(default)]
    pub y_min: Expr,
    #[serde(default)]
    pub y_max: Expr,
    #[serde(default)]
    pub keep_points: Expr,
    #[serde(default)]
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
    pub sensitive: Expr,
    /// (true | false)
    /// true: The widget is visible as long as it's parent is visible
    /// false: The widget and all it's children are not visible
    #[serde(default)]
    pub visible: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Widget {
    /// layout properties and interfaces shared by all widgets
    #[serde(default)]
    pub props: Option<WidgetProps>,
    #[serde(default)]
    pub kind: WidgetKind,
}
