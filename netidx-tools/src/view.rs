use netidx::{path::Path, protocol::publisher::v1::Value};

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Either<T, U> {
    A(T),
    B(U),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Keybind {
    key: String,
    source: Either<Path, Value>,
    sink: Path,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Direction {
    Horizontal,
    Vertical
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Widget {
    Table(Path),
    Single(Either<Path, Value>),
    Button {
        enabled: Either<Path, Value>,
        label: Either<Path, Value>,
        source: Either<Path, Value>,
        sink: Path,
    },
    Toggle {
        enabled: Either<Path, Value>,
        value: Path,
    },
    ComboBox {
        enabled: Either<Path, Value>,
        choices: Either<Path, Vec<String>>,
        value: Path,
    },
    Radio {
        enabled: Either<Path, Value>,
        choices: Either<Path, Vec<String>>,
        value: Path,
    },
    Entry {
        enabled: Either<Path, Value>,
        lines: Either<Path, Value>,
        value: Path,
    },
    Container {
        direction: Direction,
        keybinds: Vec<Keybind>,
        children: Vec<Widget>,
    }
}
