use netidx::{path::Path, publisher::Value};
use std::{boxed, collections::HashMap};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Source {
    Constant(Value),
    Load(Path),
    Variable(String),
    /// map the source through the specified gluon script
    Map {
        /// the source we are mapping from
        from: boxed::Box<Source>,
        /// the name of the gluon 'Value -> Option Value' function that
        /// will be called each time the source produces a value. If the
        /// function returns None then no value will be produced by the
        /// source, otherwise the returned value will be produced. You
        /// must define the function in one of the scripts imported by the
        /// view.
        function: String,
    },
    /// the source produces a value when any of the sub sources produce a value
    Any(Vec<Source>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Sink {
    Store(Path),
    Variable(String),
    /// sinked values are sent to all the specified sinks
    All(Vec<Sink>),
    /// sinked values are mapped through the specified gluon script
    Map {
        /// the sink we are mapping to
        to: boxed::Box<Sink>,
        /// the name of the gluon 'Value -> Option Value' function that
        /// will be called each time the sink is set. If the function
        /// returns None, then the sink will not be set, otherwise the
        /// returned value will be set.
        function: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Keybind {
    pub key: String,
    pub source: Source,
    pub sink: Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Direction {
    Horizontal,
    Vertical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Button {
    pub enabled: Source,
    pub label: Source,
    pub source: Source,
    pub sink: Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Toggle {
    pub enabled: Source,
    pub source: Source,
    pub sink: Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Selector {
    pub enabled: Source,
    pub choices: Source,
    pub source: Source,
    pub sink: Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub enabled: Source,
    pub visible: Source,
    pub source: Source,
    pub sink: Sink,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum Align {
    Fill,
    Start,
    End,
    Center,
    Baseline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoxChild {
    pub expand: bool,
    pub fill: bool,
    pub padding: u64,
    pub halign: Option<Align>,
    pub valign: Option<Align>,
    pub widget: Widget,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridChild {
    pub halign: Option<Align>,
    pub valign: Option<Align>,
    pub widget: Widget,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Box {
    pub direction: Direction,
    pub children: Vec<BoxChild>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grid {
    pub homogeneous_columns: bool,
    pub homogeneous_rows: bool,
    pub column_spacing: u32,
    pub row_spacing: u32,
    pub children: Vec<Vec<GridChild>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Widget {
    Table(Path),
    Label(Source),
    Button(Button),
    Toggle(Toggle),
    Selector(Selector),
    Entry(Entry),
    Box(Box),
    Grid(Grid),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub variables: HashMap<String, Value>,
    pub keybinds: Vec<Keybind>,
    pub scripts: Vec<(String, String)>,
    pub root: Widget,
}
