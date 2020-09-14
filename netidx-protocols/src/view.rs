use crate::parser;
use base64;
use netidx::{path::Path, publisher::Value, utils};
use std::{boxed, collections::HashMap, result, str::FromStr, string::ToString};

#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, PartialEq)]
pub enum Source {
    Constant(Value),
    Load(Path),
    Variable(String),
    Map {
        /// the sources we are mapping from
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
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        parser::parse_source(s)
    }
}

impl ToString for Source {
    fn to_string(&self) -> String {
        match self {
            Source::Constant(v) => match v {
                Value::U32(v) => format!("constant(u32, {})", v),
                Value::V32(v) => format!("constant(v32, {})", v),
                Value::I32(v) => format!("constant(i32, {})", v),
                Value::Z32(v) => format!("constant(z32, {})", v),
                Value::U64(v) => format!("constant(u64, {})", v),
                Value::V64(v) => format!("constant(v64, {})", v),
                Value::I64(v) => format!("constant(i64, {})", v),
                Value::Z64(v) => format!("constant(z64, {})", v),
                Value::F32(v) => format!("constant(f32, {})", v),
                Value::F64(v) => format!("constant(f64, {})", v),
                Value::String(s) => {
                    format!(r#"constant(string, {})"#, utils::escape(&*s, '\\', ')'))
                }
                Value::Bytes(b) => format!("constant(binary, {})", base64::encode(&*b)),
                Value::True => String::from("constant(bool, true)"),
                Value::False => String::from("constant(bool, false)"),
                Value::Null => String::from("constant(null)"),
                Value::Ok => String::from("constant(result, ok)"),
                Value::Error(v) => {
                    format!("constant(result, {})", utils::escape(&*v, '\\', ')'))
                }
            },
            Source::Load(p) => format!("load_path({})", utils::escape(&*p, '\\', ')')),
            Source::Variable(v) => format!("load_var({})", v),
            Source::Map { from, function } => {
                let mut res = format!("{}(", function);
                for i in 0..from.len() {
                    res.push_str(&from[i].to_string());
                    if i < from.len() - 1 {
                        res.push_str(", ")
                    }
                }
                res.push(')');
                res
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, PartialEq)]
pub enum SinkLeaf {
    Store(Path),
    Variable(String),
}

impl ToString for SinkLeaf {
    fn to_string(&self) -> String {
        match self {
            SinkLeaf::Store(p) => format!("store_path({})", p),
            SinkLeaf::Variable(v) => format!("store_var({})", v),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, PartialEq)]
pub enum Sink {
    Leaf(SinkLeaf),
    Map {
        /// the sinks we are mapping from
        from: Vec<Sink>,
        /// the name of the built in function to be applied
        function: String,
    },
}

impl FromStr for Sink {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        parser::parse_sink(s)
    }
}

impl ToString for Sink {
    fn to_string(&self) -> String {
        match self {
            Sink::Leaf(l) => l.to_string(),
            Sink::Map { from, function } => {
                let mut s = format!("{}(", function);
                for i in 0..from.len() {
                    s.push_str(&from[i].to_string());
                    if i < from.len() - 1 {
                        s.push_str(", ");
                    }
                }
                s.push(')');
                s
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Keybind {
    pub key: String,
    pub source: Source,
    pub sink: Sink,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Direction {
    Horizontal,
    Vertical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    pub source: Source,
    pub sink: Sink,
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
pub enum WidgetKind {
    Action(Action),
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
    GridRow(GridRow),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct WidgetProps {
    pub halign: Align,
    pub valign: Align,
    pub hexpand: bool,
    pub vexpand: bool,
    pub margin_top: u32,
    pub margin_bottom: u32,
    pub margin_start: u32,
    pub margin_end: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Widget {
    pub props: WidgetProps,
    pub kind: WidgetKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct View {
    pub variables: HashMap<String, Value>,
    pub keybinds: Vec<Keybind>,
    pub root: Widget,
}
