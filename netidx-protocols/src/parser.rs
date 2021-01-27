use crate::view::Expr;
use base64;
use bytes::Bytes;
use combine::{
    attempt, between, choice, from_str, many, many1, none_of, not_followed_by, one_of,
    optional,
    parser::{
        char::{digit, spaces, string},
        combinator::recognize,
        range::{take_while, take_while1},
        repeat::escaped,
    },
    sep_by,
    stream::{position, Range},
    token, unexpected_any, value, EasyParser, ParseError, Parser, RangeStream,
};
use netidx::{chars::Chars, publisher::Value, utils};
use std::{borrow::Cow, result::Result, str::FromStr, time::Duration};

pub(crate) static PATH_ESC: [char; 4] = ['"', '\\', '[', ']'];

fn escaped_string<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize(escaped(
        take_while1(|c| !PATH_ESC.contains(&c)),
        '\\',
        one_of(PATH_ESC.iter().copied()),
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

fn interpolated_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    #[derive(Debug)]
    enum Intp {
        Lit(String),
        Expr(Expr),
    }
    impl Intp {
        fn to_expr(self) -> Expr {
            match self {
                Intp::Lit(s) => Expr::Constant(Value::from(s)),
                Intp::Expr(s) => s,
            }
        }
    }
    spaces()
        .with(between(
            token('"'),
            token('"'),
            many(choice((
                attempt(between(token('['), token(']'), expr())).map(Intp::Expr),
                escaped_string()
                    .then(|s| {
                        if s.is_empty() {
                            unexpected_any("empty string").right()
                        } else {
                            value(s).left()
                        }
                    })
                    .map(Intp::Lit),
            ))),
        ))
        .map(|toks: Vec<Intp>| {
            toks.into_iter()
                .fold(None, |src, tok| -> Option<Expr> {
                    match (src, tok) {
                        (None, t @ Intp::Lit(_)) => Some(t.to_expr()),
                        (None, Intp::Expr(s)) => Some(Expr::Map {
                            from: vec![s],
                            function: "string_concat".into(),
                        }),
                        (Some(src @ Expr::Constant(_)), s) => Some(Expr::Map {
                            from: vec![src, s.to_expr()],
                            function: "string_concat".into(),
                        }),
                        (Some(Expr::Map { mut from, function }), s) => {
                            from.push(s.to_expr());
                            Some(Expr::Map { from, function })
                        }
                        (Some(Expr::Load(_)), _) | (Some(Expr::Variable(_)), _) => {
                            unreachable!()
                        }
                    }
                })
                .unwrap_or(Expr::Constant(Value::from("")))
        })
}

parser! {
    fn interpolated[I]()(I) -> Expr
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        interpolated_()
    }
}

fn constant<I>(typ: &'static str) -> impl Parser<I, Output = char>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    string(typ).with(token(':'))
}

fn expr_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        attempt(interpolated()),
        attempt(from_str(flt()).map(|v| Expr::Constant(Value::F64(v)))),
        attempt(from_str(int()).map(|v| Expr::Constant(Value::I64(v)))),
        attempt(
            string("true")
                .skip(not_followed_by(none_of(" ),".chars())))
                .map(|_| Expr::Constant(Value::True)),
        ),
        attempt(
            string("false")
                .skip(not_followed_by(none_of(" ),".chars())))
                .map(|_| Expr::Constant(Value::False)),
        ),
        attempt(
            string("null")
                .skip(not_followed_by(none_of(" ),".chars())))
                .map(|_| Expr::Constant(Value::Null)),
        ),
        attempt(
            constant("u32").with(from_str(uint())).map(|v| Expr::Constant(Value::U32(v))),
        ),
        attempt(
            constant("v32").with(from_str(uint())).map(|v| Expr::Constant(Value::V32(v))),
        ),
        attempt(
            constant("i32").with(from_str(int())).map(|v| Expr::Constant(Value::I32(v))),
        ),
        attempt(
            constant("z32").with(from_str(int())).map(|v| Expr::Constant(Value::Z32(v))),
        ),
        attempt(
            constant("u64").with(from_str(uint())).map(|v| Expr::Constant(Value::U64(v))),
        ),
        attempt(
            constant("v64").with(from_str(uint())).map(|v| Expr::Constant(Value::V64(v))),
        ),
        attempt(
            constant("i64").with(from_str(int())).map(|v| Expr::Constant(Value::I64(v))),
        ),
        attempt(
            constant("z64").with(from_str(int())).map(|v| Expr::Constant(Value::Z64(v))),
        ),
        attempt(
            constant("f32").with(from_str(flt())).map(|v| Expr::Constant(Value::F32(v))),
        ),
        attempt(
            constant("f64").with(from_str(flt())).map(|v| Expr::Constant(Value::F64(v))),
        ),
        attempt(
            constant("bytes")
                .with(from_str(base64str()))
                .map(|Base64Encoded(v)| Expr::Constant(Value::Bytes(Bytes::from(v)))),
        ),
        attempt(
            string("ok")
                .skip(not_followed_by(none_of(" ),".chars())))
                .map(|_| Expr::Constant(Value::Ok)),
        ),
        attempt(
            constant("error")
                .with(quoted())
                .map(|s| Expr::Constant(Value::Error(Chars::from(s)))),
        ),
        attempt(
            constant("datetime")
                .with(from_str(quoted()))
                .map(|d| Expr::Constant(Value::DateTime(d))),
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
                    Expr::Constant(Value::Duration(d))
                }),
        ),
        attempt(
            string("load_path")
                .with(between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    expr(),
                ))
                .map(|s| Expr::Load(Box::new(s))),
        ),
        attempt(
            string("load_var")
                .with(between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    expr(),
                ))
                .map(|s| Expr::Variable(Box::new(s))),
        ),
        (
            fname(),
            between(
                spaces().with(token('(')),
                spaces().with(token(')')),
                spaces().with(sep_by(expr(), spaces().with(token(',')))),
            ),
        )
            .map(|(function, from)| Expr::Map { function, from }),
    )))
}

parser! {
    fn expr[I]()(I) -> Expr
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        expr_()
    }
}

pub fn parse_expr(s: &str) -> anyhow::Result<Expr> {
    expr()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expr_parse() {
        assert_eq!(Expr::Constant(Value::U32(23)), parse_expr("u32:23").unwrap());
        assert_eq!(Expr::Constant(Value::V32(42)), parse_expr("v32:42").unwrap());
        assert_eq!(Expr::Constant(Value::I32(-10)), parse_expr("i32:-10").unwrap());
        assert_eq!(Expr::Constant(Value::I32(12321)), parse_expr("i32:12321").unwrap());
        assert_eq!(Expr::Constant(Value::Z32(-99)), parse_expr("z32:-99").unwrap());
        assert_eq!(Expr::Constant(Value::U64(100)), parse_expr("u64:100").unwrap());
        assert_eq!(Expr::Constant(Value::V64(100)), parse_expr("v64:100").unwrap());
        assert_eq!(Expr::Constant(Value::I64(-100)), parse_expr("i64:-100").unwrap());
        assert_eq!(Expr::Constant(Value::I64(-100)), parse_expr("-100").unwrap());
        assert_eq!(Expr::Constant(Value::I64(100)), parse_expr("i64:100").unwrap());
        assert_eq!(Expr::Constant(Value::I64(100)), parse_expr("100").unwrap());
        assert_eq!(Expr::Constant(Value::Z64(-100)), parse_expr("z64:-100").unwrap());
        assert_eq!(Expr::Constant(Value::Z64(100)), parse_expr("z64:100").unwrap());
        assert_eq!(Expr::Constant(Value::F32(3.1415)), parse_expr("f32:3.1415").unwrap());
        assert_eq!(Expr::Constant(Value::F32(675.6)), parse_expr("f32:675.6").unwrap());
        assert_eq!(
            Expr::Constant(Value::F32(42.3435)),
            parse_expr("f32:42.3435").unwrap()
        );
        assert_eq!(
            Expr::Constant(Value::F32(1.123e9)),
            parse_expr("f32:1.123e9").unwrap()
        );
        assert_eq!(Expr::Constant(Value::F32(1e9)), parse_expr("f32:1e9").unwrap());
        assert_eq!(
            Expr::Constant(Value::F32(21.2443e-6)),
            parse_expr("f32:21.2443e-6").unwrap()
        );
        assert_eq!(Expr::Constant(Value::F32(3.)), parse_expr("f32:3.").unwrap());
        assert_eq!(Expr::Constant(Value::F64(3.1415)), parse_expr("f64:3.1415").unwrap());
        assert_eq!(Expr::Constant(Value::F64(3.1415)), parse_expr("3.1415").unwrap());
        assert_eq!(Expr::Constant(Value::F64(1.123e9)), parse_expr("1.123e9").unwrap());
        assert_eq!(Expr::Constant(Value::F64(1e9)), parse_expr("1e9").unwrap());
        assert_eq!(
            Expr::Constant(Value::F64(21.2443e-6)),
            parse_expr("21.2443e-6").unwrap()
        );
        assert_eq!(Expr::Constant(Value::F64(3.)), parse_expr("f64:3.").unwrap());
        assert_eq!(Expr::Constant(Value::F64(3.)), parse_expr("3.").unwrap());
        let c = Chars::from(r#"I've got a lovely "bunch" of (coconuts)"#);
        let s = r#""I've got a lovely \"bunch\" of (coconuts)""#;
        assert_eq!(Expr::Constant(Value::String(c)), parse_expr(s).unwrap());
        let c = Chars::new();
        assert_eq!(Expr::Constant(Value::String(c)), parse_expr(r#""""#).unwrap());
        assert_eq!(Expr::Constant(Value::True), parse_expr("true").unwrap());
        assert_eq!(Expr::Constant(Value::False), parse_expr("false").unwrap());
        assert_eq!(Expr::Constant(Value::Null), parse_expr("null").unwrap());
        assert_eq!(Expr::Constant(Value::Ok), parse_expr("ok").unwrap());
        assert_eq!(
            Expr::Constant(Value::Error(Chars::from("error"))),
            parse_expr(r#"error:"error""#).unwrap()
        );
        let p = Chars::from(r#"/foo bar baz/"zam"/)_ xyz+ "#);
        let s = r#"load_path("/foo bar baz/\"zam\"/)_ xyz+ ")"#;
        assert_eq!(
            Expr::Load(Box::new(Expr::Constant(Value::String(p)))),
            parse_expr(s).unwrap()
        );
        let p = Expr::Load(Box::new(Expr::Map {
            from: vec![
                Expr::Constant(Value::from("/foo/")),
                Expr::Variable(Box::new(Expr::Map {
                    from: vec![
                        Expr::Variable(Box::new(Expr::Constant(Value::from("sid")))),
                        Expr::Constant(Value::from("_var"))
                    ],
                    function: "string_concat".into(),
                })),
                Expr::Constant(Value::from("/baz")),
            ],
            function: "string_concat".into(),
        }));
        let s = r#"load_path("/foo/[load_var("[load_var("sid")]_var")]/baz")"#;
        assert_eq!(p, parse_expr(s).unwrap());
        let s = r#"load_path(concat_path("foo", "bar", load_var("baz")))"#;
        assert_eq!(
            Expr::Load(Box::new(Expr::Map {
                function: String::from("concat_path"),
                from: vec![
                    Expr::Constant(Value::String(Chars::from("foo"))),
                    Expr::Constant(Value::String(Chars::from("bar"))),
                    Expr::Variable(Box::new(Expr::Constant(Value::String(Chars::from(
                        "baz"
                    )))))
                ],
            })),
            parse_expr(s).unwrap()
        );
        assert_eq!(
            Expr::Variable(Box::new(Expr::Constant(Value::String(Chars::from("sum"))))),
            parse_expr("load_var(\"sum\")").unwrap()
        );
        let src = Expr::Map {
            from: vec![
                Expr::Constant(Value::F32(1.)),
                Expr::Load(Box::new(Expr::Constant(Value::String(Chars::from(
                    "/foo/bar",
                ))))),
                Expr::Map {
                    from: vec![
                        Expr::Constant(Value::F32(675.6)),
                        Expr::Load(Box::new(Expr::Constant(Value::String(Chars::from(
                            "/foo/baz",
                        ))))),
                    ],
                    function: String::from("max"),
                },
                Expr::Map { from: vec![], function: String::from("rand") },
            ],
            function: String::from("sum"),
        };
        let chs = r#"sum(f32:1., load_path("/foo/bar"), max(f32:675.6, load_path("/foo/baz")), rand())"#;
        assert_eq!(src, parse_expr(chs).unwrap());
    }
}
