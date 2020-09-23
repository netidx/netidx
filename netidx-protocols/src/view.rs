use crate::parser;
use base64;
use netidx::{path::Path, publisher::Value, utils};
use std::{boxed, collections::HashMap, fmt, result, str::FromStr};

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

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Source::Constant(v) => match v {
                Value::U32(v) => write!(f, "constant(u32, {})", v),
                Value::V32(v) => write!(f, "constant(v32, {})", v),
                Value::I32(v) => write!(f, "constant(i32, {})", v),
                Value::Z32(v) => write!(f, "constant(z32, {})", v),
                Value::U64(v) => write!(f, "constant(u64, {})", v),
                Value::V64(v) => write!(f, "constant(v64, {})", v),
                Value::I64(v) => write!(f, "constant(i64, {})", v),
                Value::Z64(v) => write!(f, "constant(z64, {})", v),
                Value::F32(v) => write!(f, "constant(f32, {})", v),
                Value::F64(v) => write!(f, "constant(f64, {})", v),
                Value::String(s) => {
                    write!(f, r#"constant(string, "{}")"#, utils::escape(&*s, '\\', '"'))
                }
                Value::Bytes(b) => write!(f, "constant(binary, {})", base64::encode(&*b)),
                Value::True => write!(f, "constant(bool, true)"),
                Value::False => write!(f, "constant(bool, false)"),
                Value::Null => write!(f, "constant(null)"),
                Value::Ok => write!(f, "constant(result, ok)"),
                Value::Error(v) => {
                    write!(f, r#"constant(result, "{}")"#, utils::escape(&*v, '\\', '"'))
                }
            },
            Source::Load(p) => {
                write!(f, r#"load_path("{}")"#, utils::escape(&*p, '\\', '"'))
            }
            Source::Variable(v) => write!(f, "load_var({})", v),
            Source::Map { from, function } => {
                write!(f, "{}(", function)?;
                for i in 0..from.len() {
                    write!(f, "{}", &from[i])?;
                    if i < from.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
        }
    }
}

impl FromStr for Source {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        parser::parse_source(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, PartialEq)]
pub enum Sink {
    Store(Path),
    Variable(String),
    Navigate,
    All(Vec<Sink>),
    Confirm(boxed::Box<Sink>),
}

impl fmt::Display for Sink {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Sink::Store(p) => {
                write!(f, r#"store_path("{}")"#, utils::escape(&*p, '\\', '"'))
            }
            Sink::Variable(v) => write!(f, "store_var({})", v),
            Sink::Navigate => write!(f, "navigate()"),
            Sink::Confirm(sink) => write!(f, "confirm({})", sink),
            Sink::All(sinks) => {
                write!(f, "all(")?;
                for i in 0..sinks.len() {
                    write!(f, "{}", &sinks[i])?;
                    if i < sinks.len() - 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")
            }
        }
    }
}

impl FromStr for Sink {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        parser::parse_sink(s)
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
pub enum PlotColor {
    Red,
    Green,
    Magenta,
    Blue,
    Black,
    Cyan,
    White,
    Yellow,
    RGB(u8, u8, u8),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Series {
    pub title: String,
    pub line_color: PlotColor,
    pub x: Source,
    pub y: Source,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinePlot {
    pub title: String,
    pub grid: bool,
    pub fill: Option<PlotColor>,
    pub margin: usize,
    pub label_area: usize,
    pub x_label: String,
    pub y_label: String,
    pub x_min: Source,
    pub x_max: Source,
    pub y_min: Source,
    pub y_max: Source,
    pub keep_points: Source,
    pub series: Vec<Series>,
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
    LinePlot(LinePlot),
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

#[cfg(test)]
mod tests {
    use super::*;
    use netidx::chars::Chars;

    #[test]
    fn sink_round_trip() {
        let s = Sink::Store(Path::from(r#"/foo bar baz/"(zam)"/_ xyz+ "#));
        assert_eq!(s, s.to_string().parse::<Sink>().unwrap());
        let s = Sink::All(vec![
            Sink::Store(Path::from("/foo/bar")),
            Sink::Variable(String::from("foo")),
        ]);
        assert_eq!(s, s.to_string().parse::<Sink>().unwrap());
        let s = Sink::Navigate;
        assert_eq!(s, s.to_string().parse::<Sink>().unwrap());
        let s = Sink::Confirm(boxed::Box::new(Sink::Navigate));
        assert_eq!(s, s.to_string().parse::<Sink>().unwrap());
    }

    fn check(s: Source) {
        assert_eq!(s, s.to_string().parse::<Source>().unwrap())
    }

    #[test]
    fn source_round_trip() {
        check(Source::Constant(Value::U32(23)));
        check(Source::Constant(Value::V32(42)));
        check(Source::Constant(Value::I32(-10)));
        check(Source::Constant(Value::I32(12321)));
        check(Source::Constant(Value::Z32(-99)));
        check(Source::Constant(Value::U64(100)));
        check(Source::Constant(Value::V64(100)));
        check(Source::Constant(Value::I64(-100)));
        check(Source::Constant(Value::I64(100)));
        check(Source::Constant(Value::Z64(-100)));
        check(Source::Constant(Value::Z64(100)));
        check(Source::Constant(Value::F32(3.1415)));
        check(Source::Constant(Value::F32(3.)));
        check(Source::Constant(Value::F32(3.)));
        check(Source::Constant(Value::F64(3.1415)));
        check(Source::Constant(Value::F64(3.)));
        check(Source::Constant(Value::F64(3.)));
        let c = Chars::from(r#"I've got a lovely "bunch" of (coconuts)"#);
        check(Source::Constant(Value::String(c)));
        check(Source::Constant(Value::True));
        check(Source::Constant(Value::False));
        check(Source::Constant(Value::Null));
        check(Source::Constant(Value::Ok));
        check(Source::Constant(Value::Error(Chars::from("error"))));
        check(Source::Load(Path::from(r#"/foo bar baz/"zam"/)_ xyz+ "#)));
        check(Source::Variable(String::from("sum")));
        check(Source::Map {
            from: vec![
                Source::Constant(Value::F32(1.)),
                Source::Load(Path::from("/foo/bar")),
                Source::Map {
                    from: vec![
                        Source::Constant(Value::F32(0.)),
                        Source::Load(Path::from("/foo/baz")),
                    ],
                    function: String::from("max"),
                },
            ],
            function: String::from("sum"),
        });
    }
}
