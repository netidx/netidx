use crate::expr::{Expr, ExprId, ExprKind};
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
use netidx::{chars::Chars, utils, publisher::Value};
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
                Intp::Lit(s) => {
                    Expr { id: ExprId::new(), kind: ExprKind::Constant(Value::from(s)) }
                }
                Intp::Expr(s) => s,
            }
        }
    }
    spaces()
        .with(between(
            token('"'),
            token('"'),
            many(choice((
                attempt(between(token('['), token(']'), expr()).map(Intp::Expr)),
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
                        (None, Intp::Expr(s)) => Some(
                            ExprKind::Apply {
                                args: vec![s],
                                function: "string_concat".into(),
                            }
                            .to_expr(),
                        ),
                        (Some(src @ Expr { kind: ExprKind::Constant(_), .. }), s) => {
                            Some(
                                ExprKind::Apply {
                                    args: vec![src, s.to_expr()],
                                    function: "string_concat".into(),
                                }
                                .to_expr(),
                            )
                        }
                        (
                            Some(Expr {
                                kind: ExprKind::Apply { mut args, function },
                                ..
                            }),
                            s,
                        ) => {
                            args.push(s.to_expr());
                            Some(ExprKind::Apply { args, function }.to_expr())
                        }
                    }
                })
                .unwrap_or_else(|| ExprKind::Constant(Value::from("")).to_expr())
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
        attempt(from_str(flt()).map(|v| ExprKind::Constant(Value::F64(v)).to_expr())),
        attempt(from_str(int()).map(|v| ExprKind::Constant(Value::I64(v)).to_expr())),
        attempt(
            string("true")
                .skip(not_followed_by(none_of(" ),]".chars())))
                .map(|_| ExprKind::Constant(Value::True).to_expr()),
        ),
        attempt(
            string("false")
                .skip(not_followed_by(none_of(" ),]".chars())))
                .map(|_| ExprKind::Constant(Value::False).to_expr()),
        ),
        attempt(
            string("null")
                .skip(not_followed_by(none_of(" ),]".chars())))
                .map(|_| ExprKind::Constant(Value::Null).to_expr()),
        ),
        attempt(
            constant("u32")
                .with(from_str(uint()))
                .map(|v| ExprKind::Constant(Value::U32(v)).to_expr()),
        ),
        attempt(
            constant("v32")
                .with(from_str(uint()))
                .map(|v| ExprKind::Constant(Value::V32(v)).to_expr()),
        ),
        attempt(
            constant("i32")
                .with(from_str(int()))
                .map(|v| ExprKind::Constant(Value::I32(v)).to_expr()),
        ),
        attempt(
            constant("z32")
                .with(from_str(int()))
                .map(|v| ExprKind::Constant(Value::Z32(v)).to_expr()),
        ),
        attempt(
            constant("u64")
                .with(from_str(uint()))
                .map(|v| ExprKind::Constant(Value::U64(v)).to_expr()),
        ),
        attempt(
            constant("v64")
                .with(from_str(uint()))
                .map(|v| ExprKind::Constant(Value::V64(v)).to_expr()),
        ),
        attempt(
            constant("i64")
                .with(from_str(int()))
                .map(|v| ExprKind::Constant(Value::I64(v)).to_expr()),
        ),
        attempt(
            constant("z64")
                .with(from_str(int()))
                .map(|v| ExprKind::Constant(Value::Z64(v)).to_expr()),
        ),
        attempt(
            constant("f32")
                .with(from_str(flt()))
                .map(|v| ExprKind::Constant(Value::F32(v)).to_expr()),
        ),
        attempt(
            constant("f64")
                .with(from_str(flt()))
                .map(|v| ExprKind::Constant(Value::F64(v)).to_expr()),
        ),
        attempt(constant("bytes").with(from_str(base64str())).map(|Base64Encoded(v)| {
            ExprKind::Constant(Value::Bytes(Bytes::from(v))).to_expr()
        })),
        attempt(
            string("ok")
                .skip(not_followed_by(none_of(" ),]".chars())))
                .map(|_| ExprKind::Constant(Value::Ok).to_expr()),
        ),
        attempt(
            constant("error")
                .with(quoted())
                .map(|s| ExprKind::Constant(Value::Error(Chars::from(s))).to_expr()),
        ),
        attempt(
            constant("datetime")
                .with(from_str(quoted()))
                .map(|d| ExprKind::Constant(Value::DateTime(d)).to_expr()),
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
                    ExprKind::Constant(Value::Duration(d)).to_expr()
                }),
        ),
        attempt(
            (
                fname(),
                between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    spaces().with(sep_by(expr(), spaces().with(token(',')))),
                ),
            )
                .map(|(function, args)| ExprKind::Apply { function, args }.to_expr()),
        ),
        fname().skip(not_followed_by(none_of(" ),]".chars()))).map(|var| {
            ExprKind::Apply {
                function: "load_var".into(),
                args: vec![ExprKind::Constant(Value::String(Chars::from(var))).to_expr()],
            }
            .to_expr()
        }),
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
    fn const_expr_parse() {
        assert_eq!(
            ExprKind::Constant(Value::U32(23)).to_expr(),
            parse_expr("u32:23").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::V32(42)).to_expr(),
            parse_expr("v32:42").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::I32(-10)).to_expr(),
            parse_expr("i32:-10").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::I32(12321)).to_expr(),
            parse_expr("i32:12321").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::Z32(-99)).to_expr(),
            parse_expr("z32:-99").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::U64(100)).to_expr(),
            parse_expr("u64:100").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::V64(100)).to_expr(),
            parse_expr("v64:100").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::I64(-100)).to_expr(),
            parse_expr("i64:-100").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::I64(-100)).to_expr(),
            parse_expr("-100").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::I64(100)).to_expr(),
            parse_expr("i64:100").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::I64(100)).to_expr(),
            parse_expr("100").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::Z64(-100)).to_expr(),
            parse_expr("z64:-100").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::Z64(100)).to_expr(),
            parse_expr("z64:100").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F32(3.1415)).to_expr(),
            parse_expr("f32:3.1415").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F32(675.6)).to_expr(),
            parse_expr("f32:675.6").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F32(42.3435)).to_expr(),
            parse_expr("f32:42.3435").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F32(1.123e9)).to_expr(),
            parse_expr("f32:1.123e9").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F32(1e9)).to_expr(),
            parse_expr("f32:1e9").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F32(21.2443e-6)).to_expr(),
            parse_expr("f32:21.2443e-6").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F32(3.)).to_expr(),
            parse_expr("f32:3.").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F64(3.1415)).to_expr(),
            parse_expr("f64:3.1415").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F64(3.1415)).to_expr(),
            parse_expr("3.1415").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F64(1.123e9)).to_expr(),
            parse_expr("1.123e9").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F64(1e9)).to_expr(),
            parse_expr("1e9").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F64(21.2443e-6)).to_expr(),
            parse_expr("21.2443e-6").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F64(3.)).to_expr(),
            parse_expr("f64:3.").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::F64(3.)).to_expr(),
            parse_expr("3.").unwrap()
        );
        let c = Chars::from(r#"I've got a lovely "bunch" of (coconuts)"#);
        let s = r#""I've got a lovely \"bunch\" of (coconuts)""#;
        assert_eq!(
            ExprKind::Constant(Value::String(c)).to_expr(),
            parse_expr(s).unwrap()
        );
        let c = Chars::new();
        assert_eq!(
            ExprKind::Constant(Value::String(c)).to_expr(),
            parse_expr(r#""""#).unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::True).to_expr(),
            parse_expr("true").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::False).to_expr(),
            parse_expr("false").unwrap()
        );
        assert_eq!(
            ExprKind::Constant(Value::Null).to_expr(),
            parse_expr("null").unwrap()
        );
        assert_eq!(ExprKind::Constant(Value::Ok).to_expr(), parse_expr("ok").unwrap());
        assert_eq!(
            ExprKind::Constant(Value::Error(Chars::from("error"))).to_expr(),
            parse_expr(r#"error:"error""#).unwrap()
        );
    }

    #[test]
    fn interp_parse() {
        let p = Chars::from(r#"/foo bar baz/"zam"/)_ xyz+ "#);
        let s = r#"load("/foo bar baz/\"zam\"/)_ xyz+ ")"#;
        assert_eq!(
            ExprKind::Apply {
                function: "load".into(),
                args: vec![ExprKind::Constant(Value::String(p)).to_expr()]
            }
            .to_expr(),
            parse_expr(s).unwrap()
        );
        let p = ExprKind::Apply {
            function: "load".into(),
            args: vec![ExprKind::Apply {
                args: vec![
                    ExprKind::Constant(Value::from("/foo/")).to_expr(),
                    ExprKind::Apply {
                        function: "load_var".into(),
                        args: vec![ExprKind::Apply {
                            args: vec![
                                ExprKind::Apply {
                                    function: "load_var".into(),
                                    args: vec![
                                        ExprKind::Constant(Value::from("sid")).to_expr()
                                    ],
                                }
                                .to_expr(),
                                ExprKind::Constant(Value::from("_var")).to_expr(),
                            ],
                            function: "string_concat".into(),
                        }
                        .to_expr()],
                    }
                    .to_expr(),
                    ExprKind::Constant(Value::from("/baz")).to_expr(),
                ],
                function: "string_concat".into(),
            }
            .to_expr()],
        }
        .to_expr();
        let s = r#"load("/foo/[load_var("[sid]_var")]/baz")"#;
        assert_eq!(p, parse_expr(s).unwrap());
        let s = r#""[true]""#;
        let p = ExprKind::Apply {
            args: vec![ExprKind::Constant(Value::True).to_expr()],
            function: "string_concat".into(),
        }
        .to_expr();
        assert_eq!(p, parse_expr(s).unwrap());
        let s = r#"a(a(a(load_var("[true]"))))"#;
        let p = ExprKind::Apply {
            args: vec![ExprKind::Apply {
                args: vec![ExprKind::Apply {
                    args: vec![ExprKind::Apply {
                        args: vec![ExprKind::Apply {
                            args: vec![ExprKind::Constant(Value::True).to_expr()],
                            function: "string_concat".into(),
                        }
                        .to_expr()],
                        function: "load_var".into(),
                    }
                    .to_expr()],
                    function: "a".into(),
                }
                .to_expr()],
                function: "a".into(),
            }
            .to_expr()],
            function: "a".into(),
        }
        .to_expr();
        assert_eq!(p, parse_expr(s).unwrap());
    }

    #[test]
    fn expr_parse() {
        let s = r#"load(concat_path("foo", "bar", baz)))"#;
        assert_eq!(
            ExprKind::Apply {
                args: vec![ExprKind::Apply {
                    args: vec![
                        ExprKind::Constant(Value::String(Chars::from("foo"))).to_expr(),
                        ExprKind::Constant(Value::String(Chars::from("bar"))).to_expr(),
                        ExprKind::Apply {
                            args: vec![ExprKind::Constant(Value::String(Chars::from(
                                "baz"
                            )))
                            .to_expr()],
                            function: "load_var".into(),
                        }
                        .to_expr()
                    ],
                    function: String::from("concat_path"),
                }
                .to_expr()],
                function: "load".into(),
            }
            .to_expr(),
            parse_expr(s).unwrap()
        );
        assert_eq!(
            ExprKind::Apply {
                function: "load_var".into(),
                args: vec![
                    ExprKind::Constant(Value::String(Chars::from("sum"))).to_expr()
                ]
            }
            .to_expr(),
            parse_expr("sum").unwrap()
        );
        let src = ExprKind::Apply {
            args: vec![
                ExprKind::Constant(Value::F32(1.)).to_expr(),
                ExprKind::Apply {
                    args: vec![ExprKind::Constant(Value::String(Chars::from(
                        "/foo/bar",
                    )))
                    .to_expr()],
                    function: "load".into(),
                }
                .to_expr(),
                ExprKind::Apply {
                    args: vec![
                        ExprKind::Constant(Value::F32(675.6)).to_expr(),
                        ExprKind::Apply {
                            args: vec![ExprKind::Constant(Value::String(Chars::from(
                                "/foo/baz",
                            )))
                            .to_expr()],
                            function: "load".into(),
                        }
                        .to_expr(),
                    ],
                    function: String::from("max"),
                }
                .to_expr(),
                ExprKind::Apply { args: vec![], function: String::from("rand") }
                    .to_expr(),
            ],
            function: String::from("sum"),
        }
        .to_expr();
        let chs =
            r#"sum(f32:1., load("/foo/bar"), max(f32:675.6, load("/foo/baz")), rand())"#;
        assert_eq!(src, parse_expr(chs).unwrap());
    }
}
