use crate::expr::{Expr, ExprId, ExprKind};
use combine::{
    attempt, between, choice, many,
    parser::{
        char::{spaces, string},
        combinator::recognize,
        range::{take_while, take_while1},
    },
    sep_by,
    stream::{position, Range},
    token, unexpected_any, value, EasyParser, ParseError, Parser, RangeStream,
};
use netidx::{chars::Chars, publisher::Value};
use netidx_netproto::value_parser::{close_expr, escaped_string, value as netidx_value};

pub static BSCRIPT_ESC: [char; 4] = ['"', '\\', '[', ']'];

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
    .then(|s| {
        if s == "true" || s == "false" || s == "ok" || s == "null" {
            unexpected_any("can't use keyword as a function or variable name").left()
        } else {
            value(s).right()
        }
    })
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
                escaped_string(&BSCRIPT_ESC)
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
                        (Some(Expr { kind: ExprKind::Bind { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::Ref { .. }, .. }), _) => {
                            unreachable!()
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

fn expr_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        attempt(
            between(
                spaces().with(token('{')),
                spaces().with(token('}')),
                spaces().with(sep_by(expr(), attempt(spaces().with(token(';'))))),
            )
            .map(|args| ExprKind::Apply { function: "do".into(), args }.to_expr()),
        ),
        attempt(
            between(
                spaces().with(token('[')),
                spaces().with(token(']')),
                spaces().with(sep_by(expr(), attempt(spaces().with(token(','))))),
            )
            .map(|args| ExprKind::Apply { function: "array".into(), args }.to_expr()),
        ),
        attempt(
            (
                fname(),
                between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    spaces().with(sep_by(expr(), attempt(spaces().with(token(','))))),
                ),
            )
                .map(|(function, args)| ExprKind::Apply { function, args }.to_expr()),
        ),
        attempt(
            (string("let"), spaces().with(fname()), spaces().with(string("=")), expr())
                .map(|(_, name, _, value)| {
                    ExprKind::Bind { name, value: Box::new(value) }.to_expr()
                }),
        ),
        attempt(interpolated()),
        attempt(netidx_value(&BSCRIPT_ESC).map(|v| ExprKind::Constant(v).to_expr())),
        fname().skip(close_expr()).map(|name| ExprKind::Ref { name }.to_expr()),
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
                        function: "get".into(),
                        args: vec![ExprKind::Apply {
                            args: vec![
                                ExprKind::Apply {
                                    function: "get".into(),
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
        let s = r#"load("/foo/[get("[sid]_var")]/baz")"#;
        assert_eq!(p, parse_expr(s).unwrap());
        let s = r#""[true]""#;
        let p = ExprKind::Apply {
            args: vec![ExprKind::Constant(Value::True).to_expr()],
            function: "string_concat".into(),
        }
        .to_expr();
        assert_eq!(p, parse_expr(s).unwrap());
        let s = r#"a(a(a(get("[true]"))))"#;
        let p = ExprKind::Apply {
            args: vec![ExprKind::Apply {
                args: vec![ExprKind::Apply {
                    args: vec![ExprKind::Apply {
                        args: vec![ExprKind::Apply {
                            args: vec![ExprKind::Constant(Value::True).to_expr()],
                            function: "string_concat".into(),
                        }
                        .to_expr()],
                        function: "get".into(),
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
        let s = r#"load(concat_path("foo", "bar", baz))"#;
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
                            function: "get".into(),
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
            ExprKind::Ref { name: "sum".into() }.to_expr(),
            parse_expr("sum").unwrap()
        );
        assert_eq!(
            ExprKind::Bind {
                name: "foo".into(),
                value: Box::new(ExprKind::Constant(Value::I64(42)).to_expr())
            }
            .to_expr(),
            parse_expr("let foo = 42").unwrap()
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
