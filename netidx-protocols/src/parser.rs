use crate::view::{Sink, Source};
use base64;
use bytes::Bytes;
use combine::{
    attempt, between, choice, from_str, many1, none_of, not_followed_by, one_of,
    optional,
    parser::{
        char::{digit, spaces, string},
        combinator::recognize,
        range::{take_while, take_while1},
        repeat::escaped,
    },
    sep_by, sep_by1,
    stream::{position, Range},
    token, EasyParser, ParseError, Parser, RangeStream,
};
use netidx::{chars::Chars, path::Path, publisher::Value};
use std::{boxed, result::Result, str::FromStr, time::Duration};

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

fn escaped_string<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    static ESC: [char; 2] = ['"', '\\'];
    recognize(escaped(
        take_while1(move |c| c != '"' && c != '\\'),
        '\\',
        one_of(ESC[..].iter().copied()),
    ))
    .map(|s| unescape(s, '\\'))
}

fn quoted<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(between(token('"'), token('"'), escaped_string()))
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
    choice((
        attempt(recognize((
            optional(token('-')),
            take_while1(|c: char| c.is_digit(10)),
            optional(token('.')),
            take_while(|c: char| c.is_digit(10)),
            token('e'),
            int(),
        ))),
        attempt(recognize((
            optional(token('-')),
            take_while1(|c: char| c.is_digit(10)),
            token('.'),
            take_while(|c: char| c.is_digit(10)),
        ))),
    ))
}

struct Base64Encoded(Vec<u8>);

impl FromStr for Base64Encoded {
    type Err = base64::DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        base64::decode(s).map(Base64Encoded)
    }
}

fn base64str<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((
        take_while(|c: char| c.is_ascii_alphanumeric() || c == '+' || c == '/'),
        take_while(|c: char| c == '='),
    ))
}

fn fname<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((
        take_while1(|c: char| c.is_alphabetic() && c.is_lowercase()),
        take_while(|c: char| {
            (c.is_alphanumeric() && (c.is_numeric() || c.is_lowercase())) || c == '_'
        }),
    ))
}

fn constant<I>(typ: &'static str) -> impl Parser<I, Output = char>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    string(typ).with(token(':'))
}

fn source_<I>() -> impl Parser<I, Output = Source>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        attempt(from_str(flt()).map(|v| Source::Constant(Value::F64(v)))),
        attempt(from_str(int()).map(|v| Source::Constant(Value::I64(v)))),
        attempt(quoted().map(|v| Source::Constant(Value::String(Chars::from(v))))),
        attempt(
            string("true")
                .skip(not_followed_by(none_of(" ),".chars())))
                .map(|_| Source::Constant(Value::True)),
        ),
        attempt(
            string("false")
                .skip(not_followed_by(none_of(" ),".chars())))
                .map(|_| Source::Constant(Value::False)),
        ),
        attempt(
            string("null")
                .skip(not_followed_by(none_of(" ),".chars())))
                .map(|_| Source::Constant(Value::Null)),
        ),
        attempt(
            constant("u32")
                .with(from_str(uint()))
                .map(|v| Source::Constant(Value::U32(v))),
        ),
        attempt(
            constant("v32")
                .with(from_str(uint()))
                .map(|v| Source::Constant(Value::V32(v))),
        ),
        attempt(
            constant("i32")
                .with(from_str(int()))
                .map(|v| Source::Constant(Value::I32(v))),
        ),
        attempt(
            constant("z32")
                .with(from_str(int()))
                .map(|v| Source::Constant(Value::Z32(v))),
        ),
        attempt(
            constant("u64")
                .with(from_str(uint()))
                .map(|v| Source::Constant(Value::U64(v))),
        ),
        attempt(
            constant("v64")
                .with(from_str(uint()))
                .map(|v| Source::Constant(Value::V64(v))),
        ),
        attempt(
            constant("i64")
                .with(from_str(int()))
                .map(|v| Source::Constant(Value::I64(v))),
        ),
        attempt(
            constant("z64")
                .with(from_str(int()))
                .map(|v| Source::Constant(Value::Z64(v))),
        ),
        attempt(
            constant("f32")
                .with(from_str(flt()))
                .map(|v| Source::Constant(Value::F32(v))),
        ),
        attempt(
            constant("f64")
                .with(from_str(flt()))
                .map(|v| Source::Constant(Value::F64(v))),
        ),
        attempt(
            constant("bytes")
                .with(from_str(base64str()))
                .map(|Base64Encoded(v)| Source::Constant(Value::Bytes(Bytes::from(v)))),
        ),
        attempt(
            string("ok")
                .skip(not_followed_by(none_of(" ),".chars())))
                .map(|_| Source::Constant(Value::Ok)),
        ),
        attempt(
            constant("error")
                .with(quoted())
                .map(|s| Source::Constant(Value::Error(Chars::from(s)))),
        ),
        attempt(
            constant("datetime")
                .with(from_str(quoted()))
                .map(|d| Source::Constant(Value::DateTime(d))),
        ),
        attempt(
            constant("duration")
                .with(from_str(flt()).and(choice((
                    string("ns"),
                    string("us"),
                    string("ms"),
                    string("s"),
                ))))
                .map(|(n, suffix)| {
                    let d = match suffix {
                        "ns" => Duration::from_secs_f64(n / 1e9),
                        "us" => Duration::from_secs_f64(n / 1e6),
                        "ms" => Duration::from_secs_f64(n / 1e3),
                        "s" => Duration::from_secs_f64(n),
                        _ => unreachable!(),
                    };
                    Source::Constant(Value::Duration(d))
                }),
        ),
        attempt(
            string("load_path")
                .with(between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    source(),
                ))
                .map(|s| Source::Load(Box::new(s))),
        ),
        attempt(
            string("load_var")
                .with(between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    source(),
                ))
                .map(|s| Source::Variable(Box::new(s))),
        ),
        (
            fname(),
            between(
                spaces().with(token('(')),
                spaces().with(token(')')),
                spaces().with(sep_by(source(), spaces().with(token(',')))),
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

fn sink_<I>() -> impl Parser<I, Output = Sink>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        attempt(
            string("store_path")
                .with(between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    quoted(),
                ))
                .map(|s| Sink::Store(Path::from(s))),
        ),
        attempt(
            string("store_var")
                .with(between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    spaces().with(fname()),
                ))
                .map(|s| Sink::Variable(s)),
        ),
        attempt(
            string("navigate")
                .with(spaces())
                .with(token('('))
                .with(spaces())
                .with(token(')'))
                .map(|_| Sink::Navigate),
        ),
        attempt(
            string("all")
                .with(between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    spaces().with(sep_by1(sink(), spaces().with(token(',')))),
                ))
                .map(|sinks| Sink::All(sinks)),
        ),
        attempt(
            string("confirm")
                .with(between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    spaces().with(sink()),
                ))
                .map(|s| Sink::Confirm(boxed::Box::new(s))),
        ),
    )))
}

parser! {
    fn sink[I]()(I) -> Sink
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        sink_()
    }
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
    use std::boxed;

    #[test]
    fn sink_parse() {
        let p = Path::from(r#"/foo bar baz/"(zam)"/_ xyz+ "#);
        let s = r#"store_path("/foo bar baz/\"(zam)\"/_ xyz+ ")"#;
        assert_eq!(Sink::Store(p), parse_sink(s).unwrap());
        assert_eq!(
            Sink::Variable(String::from("foo")),
            parse_sink("store_var(foo)").unwrap()
        );

        let snk = Sink::All(vec![
            Sink::Store(Path::from("/foo/bar")),
            Sink::Variable(String::from("foo")),
        ]);
        let chs = r#"all(store_path("/foo/bar"), store_var(foo))"#;
        assert_eq!(snk, parse_sink(chs).unwrap());
        assert_eq!(Sink::Navigate, parse_sink("navigate()").unwrap());
        assert_eq!(
            Sink::Confirm(boxed::Box::new(Sink::Navigate)),
            parse_sink("confirm(navigate())").unwrap()
        );
    }

    #[test]
    fn source_parse() {
        assert_eq!(Source::Constant(Value::U32(23)), parse_source("u32:23").unwrap());
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
        assert_eq!(Source::Constant(Value::I64(-100)), parse_source("-100").unwrap());
        assert_eq!(Source::Constant(Value::I64(100)), parse_source("i64:100").unwrap());
        assert_eq!(Source::Constant(Value::I64(100)), parse_source("100").unwrap());
        assert_eq!(Source::Constant(Value::Z64(-100)), parse_source("z64:-100").unwrap());
        assert_eq!(Source::Constant(Value::Z64(100)), parse_source("z64:100").unwrap());
        assert_eq!(
            Source::Constant(Value::F32(3.1415)),
            parse_source("f32:3.1415").unwrap()
        );
        assert_eq!(
            Source::Constant(Value::F32(675.6)),
            parse_source("f32:675.6").unwrap()
        );
        assert_eq!(
            Source::Constant(Value::F32(42.3435)),
            parse_source("f32:42.3435").unwrap()
        );
        assert_eq!(
            Source::Constant(Value::F32(1.123e9)),
            parse_source("f32:1.123e9").unwrap()
        );
        assert_eq!(Source::Constant(Value::F32(1e9)), parse_source("f32:1e9").unwrap());
        assert_eq!(
            Source::Constant(Value::F32(21.2443e-6)),
            parse_source("f32:21.2443e-6").unwrap()
        );
        assert_eq!(Source::Constant(Value::F32(3.)), parse_source("f32:3.").unwrap());
        assert_eq!(
            Source::Constant(Value::F64(3.1415)),
            parse_source("f64:3.1415").unwrap()
        );
        assert_eq!(Source::Constant(Value::F64(3.1415)), parse_source("3.1415").unwrap());
        assert_eq!(
            Source::Constant(Value::F64(1.123e9)),
            parse_source("1.123e9").unwrap()
        );
        assert_eq!(Source::Constant(Value::F64(1e9)), parse_source("1e9").unwrap());
        assert_eq!(
            Source::Constant(Value::F64(21.2443e-6)),
            parse_source("21.2443e-6").unwrap()
        );
        assert_eq!(Source::Constant(Value::F64(3.)), parse_source("f64:3.").unwrap());
        assert_eq!(Source::Constant(Value::F64(3.)), parse_source("3.").unwrap());
        let c = Chars::from(r#"I've got a lovely "bunch" of (coconuts)"#);
        let s = r#""I've got a lovely \"bunch\" of (coconuts)""#;
        assert_eq!(Source::Constant(Value::String(c)), parse_source(s).unwrap());
        assert_eq!(Source::Constant(Value::True), parse_source("true").unwrap());
        assert_eq!(Source::Constant(Value::False), parse_source("false").unwrap());
        assert_eq!(Source::Constant(Value::Null), parse_source("null").unwrap());
        assert_eq!(Source::Constant(Value::Ok), parse_source("ok").unwrap());
        assert_eq!(
            Source::Constant(Value::Error(Chars::from("error"))),
            parse_source(r#"error:"error""#).unwrap()
        );
        let p = Chars::from(r#"/foo bar baz/"zam"/)_ xyz+ "#);
        let s = r#"load_path("/foo bar baz/\"zam\"/)_ xyz+ ")"#;
        assert_eq!(
            Source::Load(Box::new(Source::Constant(Value::String(p)))),
            parse_source(s).unwrap()
        );
        let s = r#"load_path(concat_path("foo", "bar", load_var("baz")))"#;
        assert_eq!(
            Source::Load(Box::new(Source::Map {
                function: String::from("concat_path"),
                from: vec![
                    Source::Constant(Value::String(Chars::from("foo"))),
                    Source::Constant(Value::String(Chars::from("bar"))),
                    Source::Variable(Box::new(Source::Constant(Value::String(
                        Chars::from("baz")
                    ))))
                ],
            })),
            parse_source(s).unwrap()
        );
        assert_eq!(
            Source::Variable(Box::new(Source::Constant(Value::String(Chars::from(
                "sum"
            ))))),
            parse_source("load_var(\"sum\")").unwrap()
        );
        let src = Source::Map {
            from: vec![
                Source::Constant(Value::F32(1.)),
                Source::Load(Box::new(Source::Constant(Value::String(Chars::from(
                    "/foo/bar",
                ))))),
                Source::Map {
                    from: vec![
                        Source::Constant(Value::F32(675.6)),
                        Source::Load(Box::new(Source::Constant(Value::String(
                            Chars::from("/foo/baz"),
                        )))),
                    ],
                    function: String::from("max"),
                },
                Source::Map { from: vec![], function: String::from("rand") },
            ],
            function: String::from("sum"),
        };
        let chs = r#"sum(f32:1., load_path("/foo/bar"), max(f32:675.6, load_path("/foo/baz")), rand())"#;
        assert_eq!(src, parse_source(chs).unwrap());
    }
}
