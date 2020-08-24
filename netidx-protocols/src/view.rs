use netidx::{path::Path, publisher::Value, utils};
use anyhow::{Result, Error};
use std::{boxed, collections::HashMap, str::FromStr, result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, PartialEq)]
pub enum Source {
    Constant(Value),
    Load(Path),
    Variable(String),
    Map {
        /// the source we are mapping from
        from: Vec<Source>,
        /// the name of the built-in 'Value -> Option Value' function
        /// that will be called each time the source produces a
        /// value. If the function returns None then no value will be
        /// produced by the source, otherwise the returned value will
        /// be produced. You must define the function in one of the
        /// scripts imported by the view. Note, if the wrapped source
        /// is a group, and the function is an aggregate function then
        /// it will operate on all the values (e.g. sum, mean, ewma,
        /// etc ...), otherwise it will operate on the first value in
        /// the group to update.
        function: String,
    },
}

impl FromStr for Source {
    type Err = &'static str;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, PartialEq)]
pub enum Sink {
    Store(Path),
    Variable(String),
    /// sinked values are mapped through the specified gluon script
    Map {
        /// the sink we are mapping to
        to: Vec<Sink>,
        /// the name of the 'Value -> Option Value' built-in function
        /// that will be called each time the sink is set. If the
        /// function returns None, then the sink will not be set,
        /// otherwise the returned value will be set. Note, if the
        /// wrapped sink is a group, and the function is an aggregate
        /// function then it will operate on all the values (e.g. sum,
        /// mean, ewma, etc ...), otherwise it will operate on the
        /// first value in the group to update.
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
    pub widget: boxed::Box<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridChild {
    pub halign: Option<Align>,
    pub valign: Option<Align>,
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
pub struct Grid {
    pub homogeneous_columns: bool,
    pub homogeneous_rows: bool,
    pub column_spacing: u32,
    pub row_spacing: u32,
    pub children: Vec<Vec<Widget>>,
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
    BoxChild(BoxChild),
    Grid(Grid),
    GridChild(GridChild),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub variables: HashMap<String, Value>,
    pub keybinds: Vec<Keybind>,
    pub root: Widget,
}
