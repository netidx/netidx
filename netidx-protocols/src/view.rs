use netidx::{path::Path, publisher::Value};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Source {
    Constant(Value),
    Load(Path),
    Variable(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Sink {
    Store(Path),
    Variable(String),
    Script(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Keybind {
    key: String,
    source: Source,
    sink: Sink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Direction {
    Horizontal,
    Vertical
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Widget {
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
        keybinds: Vec<Keybind>,
        variables: HashMap<String, Value>,
        children: Vec<Widget>,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct View {
    scripts: Vec<Source>,
    root: Widget,
}
