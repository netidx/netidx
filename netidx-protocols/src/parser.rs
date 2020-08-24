use crate::view::{Sink, SinkLeaf, Source};
use combine::{
    attempt, between, choice, from_str, many1, optional,
    parser::{
        char::{digit, spaces, string},
        combinator::recognize,
        range::{take_while, take_while1},
        repeat::escaped,
    },
    sep_by1,
    stream::{position, Range},
    token, EasyParser, ParseError, Parser, RangeStream,
};
use netidx::{chars::Chars, path::Path, publisher::Value};

fn unescape(s: String, esc: char) -> String {
    if !s.contains(esc) {
        s
    } else {
        let mut res = String::with_capacity(s.len());
        let mut escaped = false;
        res.extend(s.chars().filter_map(|c| {
            if c == esc && !escaped {
                escaped = true;
                None
            } else {
                escaped = false;
                Some(c)
            }
        }));
        res
    }
}

fn quoted<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces()
        .with(between(
            token('"'),
            token('"'),
            recognize(escaped(take_while1(|c| c != '"' && c != '\\'), '\\', token('"'))),
        ))
        .map(|s| unescape(s, '\\'))
}

fn uint<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    many1(digit())
}

fn int<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((optional(token('-')), take_while1(|c: char| c.is_digit(10))))
}

fn flt<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((digit(), optional(token('.')), take_while(|c: char| c.is_digit(10))))
}

fn fname<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((
        take_while1(|c: char| c.is_alphabetic() && c.is_lowercase()),
        take_while(|c: char| (c.is_alphanumeric() && c.is_lowercase()) || c == '_'),
    ))
}

fn source_<I>() -> impl Parser<I, Output = Source>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        attempt(
            string("u32:")
                .with(from_str(uint()).map(|v| Source::Constant(Value::U32(v)))),
        ),
        attempt(
            string("v32:")
                .with(from_str(uint()).map(|v| Source::Constant(Value::V32(v)))),
        ),
        attempt(
            string("i32:").with(from_str(int()).map(|v| Source::Constant(Value::I32(v)))),
        ),
        attempt(
            string("z32:").with(from_str(int()).map(|v| Source::Constant(Value::Z32(v)))),
        ),
        attempt(
            string("u64:")
                .with(from_str(uint()).map(|v| Source::Constant(Value::U64(v)))),
        ),
        attempt(
            string("v64:")
                .with(from_str(uint()).map(|v| Source::Constant(Value::V64(v)))),
        ),
        attempt(
            string("i64:").with(from_str(int()).map(|v| Source::Constant(Value::I64(v)))),
        ),
        attempt(
            string("z64:").with(from_str(int()).map(|v| Source::Constant(Value::Z64(v)))),
        ),
        attempt(
            string("f32:").with(from_str(flt()).map(|v| Source::Constant(Value::F32(v)))),
        ),
        attempt(
            string("f64:").with(from_str(flt()).map(|v| Source::Constant(Value::F64(v)))),
        ),
        attempt(
            string("string:")
                .with(quoted())
                .map(|v| Source::Constant(Value::String(Chars::from(v)))),
        ),
        attempt(string("true").map(|_| Source::Constant(Value::True))),
        attempt(string("false").map(|_| Source::Constant(Value::False))),
        attempt(string("null").map(|_| Source::Constant(Value::Null))),
        attempt(string("ok").map(|_| Source::Constant(Value::Ok))),
        attempt(
            string("err:")
                .with(quoted())
                .map(|s| Source::Constant(Value::Error(Chars::from(s)))),
        ),
        attempt(string("n:").with(quoted().map(|s| Source::Load(Path::from(s))))),
        attempt(string("v:").with(fname()).map(|s| Source::Variable(s))),
        (
            fname(),
            between(
                spaces().with(token('(')),
                spaces().with(token(')')),
                spaces().with(sep_by1(source(), spaces().with(token(',')))),
            ),
        )
            .map(|(function, from)| Source::Map { function, from }),
    )))
}

parser! {
    fn source[I]()(I) -> Source
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        source_()
    }
}

pub fn parse_source(s: &str) -> anyhow::Result<Source> {
    source()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

fn sink_leaf<I>() -> impl Parser<I, Output = SinkLeaf>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        string("n:").with(quoted()).map(|s| SinkLeaf::Store(Path::from(s))),
        string("v:").with(fname()).map(|s| SinkLeaf::Variable(s)),
    )))
}

fn sink<I>() -> impl Parser<I, Output = Sink>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        sink_leaf().map(Sink::Leaf),
        between(
            spaces().with(token('[')),
            spaces().with(token(']')),
            spaces().with(sep_by1(sink_leaf(), spaces().with(token(',')))),
        )
        .map(Sink::All),
    )))
}

pub fn parse_sink(s: &str) -> anyhow::Result<Sink> {
    sink()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_parse() {
        let p = Path::from(r#"/foo bar baz/"zam"/_ xyz+ "#);
        let s = r#"n:"/foo bar baz/\"zam\"/_ xyz+ ""#;
        assert_eq!(Sink::Store(p), parse_sink(s).unwrap());
        assert_eq!(Sink::Variable(String::from("foo")), parse_sink("v:foo").unwrap());
        let snk = Sink::All(vec![
            Sink::Store(Path::from("/foo/bar")),
            Sink::Variable(String::from("foo")),
        ]);
        let chs = r#"[n:"/foo/bar", v:foo]"#;
        assert_eq!(snk, parse_sink(chs).unwrap());
    }

    #[test]
    fn source_parse() {
        assert_eq!(Source::Constant(Value::U32(23)), parse_source(" u32:23 ").unwrap());
        assert_eq!(Source::Constant(Value::V32(42)), parse_source("v32:42").unwrap());
        assert_eq!(Source::Constant(Value::I32(-10)), parse_source("i32:-10").unwrap());
        assert_eq!(
            Source::Constant(Value::I32(12321)),
            parse_source("i32:12321").unwrap()
        );
        assert_eq!(Source::Constant(Value::Z32(-99)), parse_source("z32:-99").unwrap());
        assert_eq!(Source::Constant(Value::U64(100)), parse_source("u64:100").unwrap());
        assert_eq!(Source::Constant(Value::V64(100)), parse_source("v64:100").unwrap());
        assert_eq!(Source::Constant(Value::I64(-100)), parse_source("i64:-100").unwrap());
        assert_eq!(Source::Constant(Value::I64(100)), parse_source("i64:100").unwrap());
        assert_eq!(Source::Constant(Value::Z64(-100)), parse_source("z64:-100").unwrap());
        assert_eq!(Source::Constant(Value::Z64(100)), parse_source("z64:100").unwrap());
        assert_eq!(
            Source::Constant(Value::F32(3.1415)),
            parse_source("f32:3.1415").unwrap()
        );
        assert_eq!(Source::Constant(Value::F32(3.)), parse_source("f32:3").unwrap());
        assert_eq!(Source::Constant(Value::F32(3.)), parse_source("f32:3.").unwrap());
        assert_eq!(
            Source::Constant(Value::F64(3.1415)),
            parse_source("f64:3.1415").unwrap()
        );
        assert_eq!(Source::Constant(Value::F64(3.)), parse_source("f64:3.").unwrap());
        assert_eq!(Source::Constant(Value::F64(3.)), parse_source("f64:3").unwrap());
        let c = Chars::from(r#"I've got a lovely "bunch" of coconuts"#);
        let s = r#"string:"I've got a lovely \"bunch\" of coconuts""#;
        assert_eq!(Source::Constant(Value::String(c)), parse_source(s).unwrap());
        assert_eq!(Source::Constant(Value::True), parse_source("true").unwrap());
        assert_eq!(Source::Constant(Value::False), parse_source("false").unwrap());
        assert_eq!(Source::Constant(Value::Null), parse_source("null").unwrap());
        assert_eq!(Source::Constant(Value::Ok), parse_source("ok").unwrap());
        assert_eq!(
            Source::Constant(Value::Error(Chars::from("error"))),
            parse_source(r#"err:"error""#).unwrap()
        );
        let p = Path::from(r#"/foo bar baz/"zam"/_ xyz+ "#);
        let s = r#"n:"/foo bar baz/\"zam\"/_ xyz+ ""#;
        assert_eq!(Source::Load(p), parse_source(s).unwrap());
        assert_eq!(Source::Variable(String::from("sum")), parse_source("v:sum").unwrap());
        let src = Source::Map {
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
        };
        let chs = r#"sum(f32:1, n:"/foo/bar", max(f32:0, n:"/foo/baz"))"#;
        assert_eq!(src, parse_source(chs).unwrap());
    }
}
