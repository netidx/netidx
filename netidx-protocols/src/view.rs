use crate::parser;
use base64;
use netidx::{path::Path, publisher::Value, utils};
use std::{boxed, collections::HashMap, fmt, result, str::FromStr};

#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, PartialEq)]
pub enum Expr {
    Constant(Value),
    Load(boxed::Box<Expr>),
    Variable(boxed::Box<Expr>),
    Map {
        /// the sources we are mapping from
        from: Vec<Expr>,
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

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Expr::Constant(v) => match v {
                Value::U32(v) => write!(f, "u32:{}", v),
                Value::V32(v) => write!(f, "v32:{}", v),
                Value::I32(v) => write!(f, "i32:{}", v),
                Value::Z32(v) => write!(f, "z32:{}", v),
                Value::U64(v) => write!(f, "u64:{}", v),
                Value::V64(v) => write!(f, "v64:{}", v),
                Value::I64(v) => write!(f, "{}", v),
                Value::Z64(v) => write!(f, "z64:{}", v),
                Value::F32(v) => {
                    if v.fract() == 0. {
                        write!(f, "f32:{}.", v)
                    } else {
                        write!(f, "f32:{}", v)
                    }
                }
                Value::F64(v) => {
                    if v.fract() == 0. {
                        write!(f, "{}.", v)
                    } else {
                        write!(f, "{}", v)
                    }
                }
                Value::DateTime(v) => write!(f, r#"datetime:"{}""#, v),
                Value::Duration(v) => {
                    let v = v.as_secs_f64();
                    if v.fract() == 0. {
                        write!(f, r#"duration:{}.s"#, v)
                    } else {
                        write!(f, r#"duration:{}s"#, v)
                    }
                }
                Value::String(s) => {
                    write!(f, r#""{}""#, utils::escape(&*s, '\\', &parser::PATH_ESC))
                }
                Value::Bytes(b) => write!(f, "bytes:{}", base64::encode(&*b)),
                Value::True => write!(f, "true"),
                Value::False => write!(f, "false"),
                Value::Null => write!(f, "null"),
                Value::Ok => write!(f, "ok"),
                Value::Error(v) => {
                    write!(
                        f,
                        r#"error:"{}""#,
                        utils::escape(&*v, '\\', &parser::PATH_ESC)
                    )
                }
            },
            Expr::Load(s) => write!(f, r#"load_path({})"#, s),
            Expr::Variable(s) => write!(f, "load_var({})", s),
            Expr::Map { from, function } => {
                if function == "string_concat" { // it's an interpolation
                    write!(f, "\"")?;
                    for s in from {
                        write!(f, "[{}]", s)?;
                    }
                    write!(f, "\"")
                } else { // it's a normal function
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
}

impl FromStr for Expr {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        parser::parse_expr(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Keybind {
    pub key: String,
    pub action: Expr,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Direction {
    Horizontal,
    Vertical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Action {
    pub action: Expr,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SortDir {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnSpec {
    Exactly(Vec<String>),
    Hide(Vec<String>),
    Auto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub path: Path,
    pub default_sort_column: Option<(String, SortDir)>,
    pub columns: ColumnSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Button {
    pub enabled: Expr,
    pub label: Expr,
    pub on_click: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Toggle {
    pub enabled: Expr,
    pub value: Expr,
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Selector {
    pub enabled: Expr,
    pub choices: Expr,
    pub selected: Expr,
    pub on_change: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub enabled: Expr,
    pub visible: Expr,
    pub text: Expr,
    pub on_change: Expr,
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RGB {
    pub r: f64,
    pub g: f64,
    pub b: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Series {
    pub title: String,
    pub line_color: RGB,
    pub x: Expr,
    pub y: Expr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinePlot {
    pub title: String,
    pub x_label: String,
    pub y_label: String,
    pub x_labels: usize,
    pub y_labels: usize,
    pub x_grid: bool,
    pub y_grid: bool,
    pub fill: Option<RGB>,
    pub margin: u32,
    pub label_area: u32,
    pub x_min: Expr,
    pub x_max: Expr,
    pub y_min: Expr,
    pub y_max: Expr,
    pub keep_points: Expr,
    pub series: Vec<Series>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WidgetKind {
    Action(Action),
    Table(Table),
    Label(Expr),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    pub props: Option<WidgetProps>,
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
    use bytes::Bytes;
    use chrono::{prelude::*, MAX_DATETIME, MIN_DATETIME};
    use netidx::chars::Chars;
    use proptest::{collection, prelude::*};
    use std::time::Duration;

    fn datetime() -> impl Strategy<Value = DateTime<Utc>> {
        (MIN_DATETIME.timestamp()..MAX_DATETIME.timestamp(), 0..1_000_000_000u32)
            .prop_map(|(s, ns)| Utc.timestamp(s, ns))
    }

    fn duration() -> impl Strategy<Value = Duration> {
        (any::<u64>(), 0..1_000_000_000u32).prop_map(|(s, ns)| Duration::new(s, ns))
    }

    fn bytes() -> impl Strategy<Value = Bytes> {
        any::<Vec<u8>>().prop_map(Bytes::from)
    }

    fn chars() -> impl Strategy<Value = Chars> {
        any::<String>().prop_map(Chars::from)
    }

    fn value() -> impl Strategy<Value = Value> {
        prop_oneof![
            any::<u32>().prop_map(Value::U32),
            any::<u32>().prop_map(Value::V32),
            any::<i32>().prop_map(Value::I32),
            any::<i32>().prop_map(Value::Z32),
            any::<u64>().prop_map(Value::U64),
            any::<u64>().prop_map(Value::V64),
            any::<i64>().prop_map(Value::I64),
            any::<i64>().prop_map(Value::Z64),
            any::<f32>().prop_map(Value::F32),
            any::<f64>().prop_map(Value::F64),
            datetime().prop_map(Value::DateTime),
            duration().prop_map(Value::Duration),
            chars().prop_map(Value::String),
            bytes().prop_map(Value::Bytes),
            Just(Value::True),
            Just(Value::False),
            Just(Value::Null),
            Just(Value::Ok),
            chars().prop_map(Value::Error),
        ]
    }

    prop_compose! {
        fn random_fname()(s in "[a-z][a-z0-9_]*".prop_filter("Filter reserved words", |s| {
            s != "ok" && s != "true" && s != "false" && s != "null"
        })) -> String {
            s
        }
    }

    fn valid_fname() -> impl Strategy<Value = String> {
        prop_oneof! [
            Just(String::from("any")),
            Just(String::from("all")),
            Just(String::from("sum")),
            Just(String::from("product")),
            Just(String::from("divide")),
            Just(String::from("mean")),
            Just(String::from("min")),
            Just(String::from("max")),
            Just(String::from("and")),
            Just(String::from("or")),
            Just(String::from("not")),
            Just(String::from("cmp")),
            Just(String::from("if")),
            Just(String::from("filter")),
            Just(String::from("cast")),
            Just(String::from("isa")),
            Just(String::from("eval")),
            Just(String::from("count")),
            Just(String::from("sample")),
            Just(String::from("string_join")),
            Just(String::from("string_concat")),
            Just(String::from("store_var")),
            Just(String::from("store_path")),
            Just(String::from("navigate")),
            Just(String::from("confirm")),
        ]
    }

    fn fname() -> impl Strategy<Value = String> {
        prop_oneof! [
            random_fname(),
            valid_fname(),
        ]
    }
    
    fn expr() -> impl Strategy<Value = Expr> {
        let leaf = value().prop_map(Expr::Constant);
        leaf.prop_recursive(100, 1000000, 10, |inner| {
            prop_oneof![
                inner.clone().prop_map(|s| Expr::Load(boxed::Box::new(s))),
                inner.clone().prop_map(|s| Expr::Variable(boxed::Box::new(s))),
                (collection::vec(inner, (0, 10)), fname())
                    .prop_map(|(s, f)| { Expr::Map { function: f, from: s } })
            ]
        })
    }

    fn check(s0: &Expr, s1: &Expr) -> bool {
        match (s0, s1) {
            (Expr::Constant(v0), Expr::Constant(v1)) => match (v0, v1) {
                (Value::Duration(d0), Value::Duration(d1)) => {
                    let f0 = d0.as_secs_f64();
                    let f1 = d1.as_secs_f64();
                    f0 == f1 || (f0 != 0. && f1 != 0. && ((f0 - f1).abs() / f0) < 1e-8)
                }
                (Value::F32(v0), Value::F32(v1)) => v0 == v1 || (v0 - v1).abs() < 1e-7,
                (Value::F64(v0), Value::F64(v1)) => v0 == v1 || (v0 - v1).abs() < 1e-8,
                (v0, v1) => v0 == v1,
            },
            (Expr::Load(s0), Expr::Load(s1)) => check(&*s0, &*s1),
            (Expr::Variable(s0), Expr::Variable(s1)) => check(&*s0, &*s1),
            (
                Expr::Map { from: srs0, function: f0 },
                Expr::Map { from: srs1, function: f1 },
            ) if f0 == f1 && srs0.len() == srs1.len() => {
                srs0.iter().zip(srs1.iter()).fold(true, |r, (s0, s1)| r && check(s0, s1))
            }
            (_, _) => false,
        }
    }

    proptest! {
        #[test]
        fn expr_round_trip(s in expr()) {
            assert!(check(dbg!(&s), &dbg!(s.to_string()).parse::<Expr>().unwrap()))
        }
    }
}
