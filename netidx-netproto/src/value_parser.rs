use crate::value::Value;
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
    sep_by,
    stream::{position, Range},
    token, EasyParser, ParseError, Parser, RangeStream,
};
use netidx_core::{chars::Chars, utils};
use std::{borrow::Cow, result::Result, str::FromStr, sync::Arc, time::Duration};

pub static BSCRIPT_ESC: [char; 4] = ['"', '\\', '[', ']'];

pub fn escaped_string<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize(escaped(
        take_while1(|c| !BSCRIPT_ESC.contains(&c)),
        '\\',
        one_of(BSCRIPT_ESC.iter().copied()),
    ))
    .map(|s| match utils::unescape(&s, '\\') {
        Cow::Borrowed(_) => s, // it didn't need unescaping, so just return it
        Cow::Owned(s) => s,
    })
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

fn constant<I>(typ: &'static str) -> impl Parser<I, Output = char>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    string(typ).with(token(':'))
}

fn value_<I>() -> impl Parser<I, Output = Value>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        attempt(
            between(token('['), token(']'), sep_by(value(), token(',')))
                .map(|vals: Vec<Value>| Value::Array(Arc::from(vals))),
        ),
        attempt(quoted()).map(|s| Value::String(Chars::from(s))),
        attempt(from_str(flt()).map(|v| Value::F64(v))),
        attempt(from_str(int()).map(|v| Value::I64(v))),
        attempt(
            string("true")
                .skip(not_followed_by(none_of(" ),]".chars())))
                .map(|_| Value::True),
        ),
        attempt(
            string("false")
                .skip(not_followed_by(none_of(" ),]".chars())))
                .map(|_| Value::False),
        ),
        attempt(
            string("null")
                .skip(not_followed_by(none_of(" ),]".chars())))
                .map(|_| Value::Null),
        ),
        attempt(constant("u32").with(from_str(uint())).map(|v| Value::U32(v))),
        attempt(constant("v32").with(from_str(uint())).map(|v| Value::V32(v))),
        attempt(constant("i32").with(from_str(int())).map(|v| Value::I32(v))),
        attempt(constant("z32").with(from_str(int())).map(|v| Value::Z32(v))),
        attempt(constant("u64").with(from_str(uint())).map(|v| Value::U64(v))),
        attempt(constant("v64").with(from_str(uint())).map(|v| Value::V64(v))),
        attempt(constant("i64").with(from_str(int())).map(|v| Value::I64(v))),
        attempt(constant("z64").with(from_str(int())).map(|v| Value::Z64(v))),
        attempt(constant("f32").with(from_str(flt())).map(|v| Value::F32(v))),
        attempt(constant("f64").with(from_str(flt())).map(|v| Value::F64(v))),
        attempt(
            constant("bytes")
                .with(from_str(base64str()))
                .map(|Base64Encoded(v)| Value::Bytes(Bytes::from(v))),
        ),
        attempt(
            string("ok")
                .skip(not_followed_by(none_of(" ),]".chars())))
                .map(|_| Value::Ok),
        ),
        attempt(constant("error").with(quoted()).map(|s| Value::Error(Chars::from(s)))),
        attempt(
            constant("datetime").with(from_str(quoted())).map(|d| Value::DateTime(d)),
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
                    Value::Duration(d)
                }),
        ),
    )))
}

parser! {
    pub fn value[I]()(I) -> Value
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        value_()
    }
}

pub fn parse_value(s: &str) -> anyhow::Result<Value> {
    value()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        assert_eq!(Value::U32(23), parse_value("u32:23").unwrap());
        assert_eq!(Value::V32(42), parse_value("v32:42").unwrap());
        assert_eq!(Value::I32(-10), parse_value("i32:-10").unwrap());
        assert_eq!(Value::I32(12321), parse_value("i32:12321").unwrap());
        assert_eq!(Value::Z32(-99), parse_value("z32:-99").unwrap());
        assert_eq!(Value::U64(100), parse_value("u64:100").unwrap());
        assert_eq!(Value::V64(100), parse_value("v64:100").unwrap());
        assert_eq!(Value::I64(-100), parse_value("i64:-100").unwrap());
        assert_eq!(Value::I64(-100), parse_value("-100").unwrap());
        assert_eq!(Value::I64(100), parse_value("i64:100").unwrap());
        assert_eq!(Value::I64(100), parse_value("100").unwrap());
        assert_eq!(Value::Z64(-100), parse_value("z64:-100").unwrap());
        assert_eq!(Value::Z64(100), parse_value("z64:100").unwrap());
        assert_eq!(Value::F32(3.1415), parse_value("f32:3.1415").unwrap());
        assert_eq!(Value::F32(675.6), parse_value("f32:675.6").unwrap());
        assert_eq!(Value::F32(42.3435), parse_value("f32:42.3435").unwrap());
        assert_eq!(Value::F32(1.123e9), parse_value("f32:1.123e9").unwrap());
        assert_eq!(Value::F32(1e9), parse_value("f32:1e9").unwrap());
        assert_eq!(Value::F32(21.2443e-6), parse_value("f32:21.2443e-6").unwrap());
        assert_eq!(Value::F32(3.), parse_value("f32:3.").unwrap());
        assert_eq!(Value::F64(3.1415), parse_value("f64:3.1415").unwrap());
        assert_eq!(Value::F64(3.1415), parse_value("3.1415").unwrap());
        assert_eq!(Value::F64(1.123e9), parse_value("1.123e9").unwrap());
        assert_eq!(Value::F64(1e9), parse_value("1e9").unwrap());
        assert_eq!(Value::F64(21.2443e-6), parse_value("21.2443e-6").unwrap());
        assert_eq!(Value::F64(3.), parse_value("f64:3.").unwrap());
        assert_eq!(Value::F64(3.), parse_value("3.").unwrap());
        let c = Chars::from(r#"I've got a lovely "bunch" of (coconuts)"#);
        let s = r#""I've got a lovely \"bunch\" of (coconuts)""#;
        assert_eq!(Value::String(c), parse_value(s).unwrap());
        let c = Chars::new();
        assert_eq!(Value::String(c), parse_value(r#""""#).unwrap());
        assert_eq!(Value::True, parse_value("true").unwrap());
        assert_eq!(Value::True, parse_value("true ").unwrap());
        assert_eq!(Value::False, parse_value("false").unwrap());
        assert_eq!(Value::Null, parse_value("null").unwrap());
        assert_eq!(Value::Ok, parse_value("ok").unwrap());
        assert_eq!(
            Value::Error(Chars::from("error")),
            parse_value(r#"error:"error""#).unwrap()
        );
    }
}
