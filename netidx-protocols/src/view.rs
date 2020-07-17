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
pub enum Widget {
    Table(Source),
    Display(Source),
    Action {
        source: Source,
        sink: Sink
    },
    Button {
        enabled: Source,
        label: Source,
        source: Source,
        sink: Sink,
    },
    Toggle {
        enabled: Source,
        source: Source,
        sink: Sink,
    },
    ComboBox {
        enabled: Source,
        choices: Source,
        source: Source,
        sink: Sink,
    },
    Radio {
        enabled: Source,
        choices: Source,
        value: Sink,
    },
    Entry {
        enabled: Source,
        lines: Source,
        source: Source,
        sink: Sink,
    },
    Container {
        direction: Direction,
        hpct: f32,
        vpct: f32,
        keybinds: Vec<Keybind>,
        variables: HashMap<String, Value>,
        children: Vec<Widget>,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub scripts: Vec<Source>,
    pub root: Widget,
}
