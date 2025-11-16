use crate::{pbuf::PBytes, Abstract, ValArray, Value};
use arcstr::ArcStr;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bytes::Bytes;
use combine::{
    attempt, between, choice, from_str, many1, none_of, not_followed_by, one_of,
    optional, parser,
    parser::{
        char::{alpha_num, digit, spaces, string},
        combinator::recognize,
        range::{take_while, take_while1},
        repeat::escaped,
    },
    sep_by,
    stream::{position, Range},
    token, unexpected_any, EasyParser, ParseError, Parser, RangeStream,
};
use compact_str::CompactString;
use escaping::Escape;
use netidx_core::pack::Pack;
use poolshark::local::LPooled;
use rust_decimal::Decimal;
use std::{borrow::Cow, result::Result, str::FromStr, sync::LazyLock, time::Duration};
use triomphe::Arc;

fn should_escape_generic(c: char) -> bool {
    c.is_control()
}

pub const VAL_MUST_ESC: [char; 2] = ['\\', '"'];
pub static VAL_ESC: LazyLock<Escape> = LazyLock::new(|| {
    Escape::new(
        '\\',
        &['\\', '"', '\n', '\r', '\0', '\t'],
        &[('\n', "n"), ('\r', "r"), ('\t', "t"), ('\0', "0")],
        Some(should_escape_generic),
    )
    .unwrap()
});

pub fn escaped_string<I>(
    must_esc: &'static [char],
    esc: &Escape,
) -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize(escaped(
        take_while1(move |c| !must_esc.contains(&c)),
        esc.get_escape_char(),
        one_of(
            esc.get_tr()
                .iter()
                .filter_map(|(_, s)| s.chars().next())
                .chain(must_esc.iter().copied()),
        ),
    ))
    .map(|s| match esc.unescape(&s) {
        Cow::Borrowed(_) => s, // it didn't need unescaping, so just return it
        Cow::Owned(s) => s,
    })
}

fn quoted<I>(
    must_escape: &'static [char],
    esc: &Escape,
) -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(between(token('"'), token('"'), escaped_string(must_escape, esc)))
}

fn uint<I, T: FromStr + Clone + Copy>() -> impl Parser<I, Output = T>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    many1(digit()).then(|s: CompactString| match s.parse::<T>() {
        Ok(i) => combine::value(i).right(),
        Err(_) => unexpected_any("invalid unsigned integer").left(),
    })
}

pub fn int<I, T: FromStr + Clone + Copy>() -> impl Parser<I, Output = T>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((optional(token('-')), take_while1(|c: char| c.is_digit(10)))).then(
        |s: CompactString| match s.parse::<T>() {
            Ok(i) => combine::value(i).right(),
            Err(_) => unexpected_any("invalid signed integer").left(),
        },
    )
}

fn flt<I, T: FromStr + Clone + Copy>() -> impl Parser<I, Output = T>
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
            optional(token('-')),
            take_while1(|c: char| c.is_digit(10)),
        ))),
        attempt(recognize((
            optional(token('-')),
            take_while1(|c: char| c.is_digit(10)),
            token('.'),
            take_while(|c: char| c.is_digit(10)),
        ))),
    ))
    .then(|s: CompactString| match s.parse::<T>() {
        Ok(i) => combine::value(i).right(),
        Err(_) => unexpected_any("invalid float").left(),
    })
}

struct Base64Encoded(Vec<u8>);

impl FromStr for Base64Encoded {
    type Err = base64::DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        BASE64.decode(s).map(Base64Encoded)
    }
}

fn base64str<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(string("null")).map(|_| String::new()),
        recognize((
            take_while(|c: char| c.is_ascii_alphanumeric() || c == '+' || c == '/'),
            take_while(|c: char| c == '='),
        )),
    ))
}

fn constant<I>(typ: &'static str) -> impl Parser<I, Output = ()>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    string(typ).with(spaces()).with(token(':')).with(spaces()).map(|_| ())
}

pub fn close_expr<I>() -> impl Parser<I, Output = ()>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    not_followed_by(none_of([' ', '\n', '\t', ';', ')', ',', ']', '}', '"']))
}

fn value_<I>(must_escape: &'static [char], esc: &Escape) -> impl Parser<I, Output = Value>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        attempt(
            between(
                token('['),
                token(']'),
                sep_by(value(must_escape, esc), attempt(spaces().with(token(',')))),
            )
            .map(|mut vals: LPooled<Vec<Value>>| {
                Value::Array(ValArray::from_iter_exact(vals.drain(..)))
            }),
        ),
        attempt(between(
            token('{'),
            token('}'),
            sep_by(
                (
                    value(must_escape, esc),
                    spaces().with(string("=>")).with(value(must_escape, esc)),
                ),
                attempt(spaces().with(token(','))),
            )
            .map(|mut vals: LPooled<Vec<(Value, Value)>>| {
                Value::Map(immutable_chunkmap::map::Map::from_iter(vals.drain(..)))
            }),
        )),
        attempt(quoted(must_escape, esc)).map(|s| Value::String(ArcStr::from(s))),
        attempt(flt::<_, f64>()).map(Value::F64),
        attempt(int::<_, i64>()).map(Value::I64),
        attempt(
            string("true").skip(not_followed_by(alpha_num())).map(|_| Value::Bool(true)),
        ),
        attempt(
            string("false")
                .skip(not_followed_by(alpha_num()))
                .map(|_| Value::Bool(false)),
        ),
        attempt(string("null").skip(not_followed_by(alpha_num())).map(|_| Value::Null)),
        attempt(
            constant("decimal")
                .with(flt::<_, Decimal>())
                .map(|d| Value::Decimal(Arc::new(d))),
        ),
        // Integer and float types grouped to stay under choice! limit
        attempt(choice((
            attempt(constant("u8").with(uint::<_, u8>()).map(Value::U8)),
            attempt(constant("i8").with(int::<_, i8>()).map(Value::I8)),
            attempt(constant("u16").with(uint::<_, u16>()).map(Value::U16)),
            attempt(constant("i16").with(int::<_, i16>()).map(Value::I16)),
            attempt(constant("u32").with(uint::<_, u32>()).map(Value::U32)),
            attempt(constant("v32").with(uint::<_, u32>()).map(Value::V32)),
            attempt(constant("i32").with(int::<_, i32>()).map(Value::I32)),
            attempt(constant("z32").with(int::<_, i32>()).map(Value::Z32)),
            attempt(constant("u64").with(uint::<_, u64>()).map(Value::U64)),
            attempt(constant("v64").with(uint::<_, u64>()).map(Value::V64)),
            attempt(constant("i64").with(int::<_, i64>()).map(Value::I64)),
            attempt(constant("z64").with(int::<_, i64>()).map(Value::Z64)),
            attempt(constant("f32").with(flt::<_, f32>()).map(Value::F32)),
            attempt(constant("f64").with(flt::<_, f64>()).map(Value::F64)),
        ))),
        attempt(
            constant("bytes")
                .with(from_str(base64str()))
                .map(|Base64Encoded(v)| Value::Bytes(PBytes::new(Bytes::from(v)))),
        ),
        attempt(constant("abstract").with(from_str(base64str())).then(
            |Base64Encoded(v)| match Abstract::decode(&mut &v[..]) {
                Ok(a) => combine::value(Value::Abstract(a)).right(),
                Err(_) => unexpected_any("failed to unpack abstract").left(),
            },
        )),
        attempt(
            constant("error")
                .with(value(must_escape, esc))
                .map(|v| Value::Error(Arc::new(v))),
        ),
        attempt(
            constant("datetime")
                .with(from_str(quoted(must_escape, esc)))
                .map(|d| Value::DateTime(Arc::new(d))),
        ),
        attempt(
            constant("duration")
                .with(flt::<_, f64>().and(choice((
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
                    Value::Duration(Arc::new(d))
                }),
        ),
    )))
}

parser! {
    pub fn value['a, I](
        must_escape: &'static [char],
        esc: &'a Escape
    )(I) -> Value
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        value_(must_escape, esc)
    }
}

pub fn parse_value(s: &str) -> anyhow::Result<Value> {
    value(&VAL_MUST_ESC, &VAL_ESC)
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[cfg(test)]
mod tests {
    use arcstr::literal;

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
        let c = ArcStr::from(r#"I've got a lovely "bunch" of (coconuts)"#);
        let s = r#""I've got a lovely \"bunch\" of (coconuts)""#;
        assert_eq!(Value::String(c), parse_value(s).unwrap());
        let c = ArcStr::new();
        assert_eq!(Value::String(c), parse_value(r#""""#).unwrap());
        let c = ArcStr::from(r#"""#);
        let s = r#""\"""#;
        assert_eq!(Value::String(c), parse_value(s).unwrap());
        assert_eq!(Value::Bool(true), parse_value("true").unwrap());
        assert_eq!(Value::Bool(true), parse_value("true ").unwrap());
        assert_eq!(Value::Bool(false), parse_value("false").unwrap());
        assert_eq!(Value::Null, parse_value("null").unwrap());
        assert_eq!(
            Value::error(literal!("error")),
            parse_value(r#"error:"error""#).unwrap()
        );
    }
}
