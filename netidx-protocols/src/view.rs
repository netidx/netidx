use netidx::{path::Path, publisher::Value};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Source {
    Constant(Value),
    Load(Path),
    Variable(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Sink {
    Store(Path),
    Variable(String),
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
    Vertical
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    pub source: Source,
    pub sink: Sink
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
    pub lines: Source,
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
    Table(Source),
    Label(Source),
    Action(Action),
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
    pub root: Widget,
}
