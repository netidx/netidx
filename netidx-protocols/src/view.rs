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
    Function(String),
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
pub struct ComboBox {
    pub enabled: Source,
    pub choices: Source,
    pub source: Source,
    pub sink: Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Radio {
    pub enabled: Source,
    pub choices: Source,
    pub value: Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub enabled: Source,
    pub lines: Source,
    pub source: Source,
    pub sink: Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Container {
    pub direction: Direction,
    pub hpct: f32,
    pub vpct: f32,
    pub keybinds: Vec<Keybind>,
    pub variables: HashMap<String, Value>,
    pub children: Vec<Widget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Widget {
    Table(Source),
    Display(Source),
    Action(Action),
    Button(Button),
    Toggle(Toggle),
    ComboBox(ComboBox),
    Radio(Radio),
    Entry(Entry),
    Container(Container),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub scripts: Vec<Source>,
    pub root: Widget,
}
