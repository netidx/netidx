use crate::expr::{Expr, ExprKind};
use combine::{
    attempt, between, choice,
    parser::{
        char::spaces,
        combinator::recognize,
        range::{take_while, take_while1},
    },
    sep_by,
    stream::{position, Range},
    token, unexpected_any, value, EasyParser, ParseError, Parser, RangeStream,
};
use netidx_netproto::value_parser::value as netidx_value;

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

fn expr_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        attempt(
            (
                fname(),
                between(
                    spaces().with(token('(')),
                    spaces().with(token(')')),
                    sep_by(expr(), attempt(spaces().with(token(',')))),
                ),
            )
                .map(|(function, args)| ExprKind::Apply { function, args }.to_expr()),
        ),
        netidx_value().map(|v| ExprKind::Constant(v).to_expr()),
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
    use netidx::{chars::Chars, publisher::Value};

    #[test]
    fn expr_parse() {
        let s = "load(\n  concat_path(\"foo\", \"bar\", load_var(\"baz\"))\n)";
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
