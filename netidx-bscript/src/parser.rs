use crate::expr::{Expr, ExprId, ExprKind, ModPath};
use combine::{
    attempt, between, choice, many, many1, optional,
    parser::{
        char::{spaces, string},
        combinator::recognize,
        range::{take_while, take_while1},
    },
    sep_by, sep_by1,
    stream::{position, Range},
    token, unexpected_any, value, EasyParser, ParseError, Parser, RangeStream,
};
use netidx::{chars::Chars, path::Path, publisher::Value, utils::Either};
use netidx_netproto::value_parser::{escaped_string, value as netidx_value};
use triomphe::Arc;

pub static BSCRIPT_ESC: [char; 4] = ['"', '\\', '[', ']'];

fn spstring<'a, I>(s: &'static str) -> impl Parser<I, Output = &'a str>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(string(s))
}

fn fname<I>() -> impl Parser<I, Output = Chars>
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
    .then(|s: String| {
        if s == "true" || s == "false" || s == "ok" || s == "null" {
            unexpected_any("can't use keyword as a function or variable name").left()
        } else {
            value(s).right()
        }
    })
    .map(Chars::from)
}

fn spfname<I>() -> impl Parser<I, Output = Chars>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(fname())
}

fn modpath<I>() -> impl Parser<I, Output = ModPath>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    sep_by1(fname(), string("::")).map(|v: Vec<Chars>| ModPath(Path::from_iter(v)))
}

fn spmodpath<I>() -> impl Parser<I, Output = ModPath>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(modpath())
}

fn csep<I>() -> impl Parser<I, Output = char>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    attempt(spaces().with(token(',')))
}

fn sptoken<I>(t: char) -> impl Parser<I, Output = char>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(token(t))
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
    between(
        token('"'),
        token('"'),
        many(choice((
            attempt(between(token('['), sptoken(']'), expr()).map(Intp::Expr)),
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
    )
    .map(|toks: Vec<Intp>| {
        let mut argvec = vec![];
        toks.into_iter()
            .fold(None, |src, tok| -> Option<Expr> {
                match (src, tok) {
                    (None, t @ Intp::Lit(_)) => Some(t.to_expr()),
                    (None, Intp::Expr(s)) => {
                        argvec.push(s);
                        Some(
                            ExprKind::Apply {
                                args: Arc::from(argvec.clone()),
                                function: ["str", "concat"].into(),
                            }
                            .to_expr(),
                        )
                    }
                    (Some(src @ Expr { kind: ExprKind::Constant(_), .. }), s) => {
                        argvec.extend([src, s.to_expr()]);
                        Some(
                            ExprKind::Apply {
                                args: Arc::from(argvec.clone()),
                                function: ["str", "concat"].into(),
                            }
                            .to_expr(),
                        )
                    }
                    (
                        Some(Expr {
                            kind: ExprKind::Apply { args: _, function }, ..
                        }),
                        s,
                    ) => {
                        argvec.push(s.to_expr());
                        Some(
                            ExprKind::Apply { args: Arc::from(argvec.clone()), function }
                                .to_expr(),
                        )
                    }
                    (Some(Expr { kind: ExprKind::Bind { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Do { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Module { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Use { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Connect { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Ref { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Eq { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Lt { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Gt { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Gte { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Lte { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::And { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Or { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Not { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Select { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Lambda { .. }, .. }), _) => {
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

fn module<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        optional(string("pub")).map(|o| o.is_some()),
        spstring("mod").with(spfname()),
        spaces().with(choice((
            token(';').map(|_| None),
            between(token('{'), sptoken('}'), many(expr()))
                .map(|m: Vec<Expr>| Some(Arc::from(m))),
        ))),
    )
        .map(|(export, name, value)| ExprKind::Module { name, export, value }.to_expr())
}

fn use_module<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    string("use")
        .with(spmodpath())
        .skip(sptoken(';'))
        .map(|name| ExprKind::Use { name }.to_expr())
}

fn alist<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    between(
        token('{'),
        sptoken('}'),
        sep_by((spfname(), sptoken(':').with(expr())), csep()),
    )
    .map(|args: Vec<(Chars, Expr)>| {
        let args = args
            .into_iter()
            .map(|(name, expr)| {
                let key = ExprKind::Constant(name.into()).to_expr();
                let args = [key, expr].into_iter().collect();
                ExprKind::Apply { function: ["array"].into(), args }.to_expr()
            })
            .collect();
        ExprKind::Apply { function: ["array"].into(), args }.to_expr()
    })
}

fn do_block<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    between(token('{'), sptoken('}'), many(expr()))
        .map(|args: Vec<Expr>| ExprKind::Do { exprs: Arc::from(args) }.to_expr())
}

fn array<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    between(token('['), sptoken(']'), sep_by(expr(), csep())).map(|args: Vec<Expr>| {
        ExprKind::Apply { function: ["array"].into(), args: Arc::from(args) }.to_expr()
    })
}

fn apply<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (modpath(), between(sptoken('('), sptoken(')'), sep_by(expr(), csep()))).map(
        |(function, args): (ModPath, Vec<Expr>)| {
            ExprKind::Apply { function, args: Arc::from(args) }.to_expr()
        },
    )
}

fn lambda<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        between(
            token('|'),
            sptoken('|'),
            choice((
                spstring("@args").map(|_| (vec![], true)),
                attempt((
                    sep_by1(spfname(), csep()).skip(sptoken(',')),
                    spstring("@args").map(|_| true),
                )),
                sep_by(spfname(), csep()).map(|args| (args, false)),
            )),
        ),
        choice((
            attempt(sptoken('\'').with(fname()).map(Either::Right)),
            expr().map(Either::Left),
        )),
    )
        .map(|((args, vargs), body)| {
            let args = Arc::from_iter(args.into_iter().map(Chars::from));
            ExprKind::Lambda { args, vargs, body: Arc::new(body) }.to_expr()
        })
}

fn letbind<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        optional(string("pub")).map(|o| o.is_some()),
        spstring("let").with(spfname()).skip(spstring("=")),
        expr().skip(sptoken(';')),
    )
        .map(|(export, name, value)| {
            ExprKind::Bind { export, name, value: Arc::new(value) }.to_expr()
        })
}

fn connect<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (modpath().skip(spstring("<-")), expr().skip(sptoken(';')))
        .map(|(name, e)| ExprKind::Connect { name, value: Arc::new(e) }.to_expr())
}

fn literal<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    netidx_value(&BSCRIPT_ESC).map(|v| ExprKind::Constant(v).to_expr())
}

fn reference<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    modpath().map(|name| ExprKind::Ref { name }.to_expr())
}

fn blang_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        between(token('('), sptoken(')'), blang()),
        attempt((expr(), spstring("=").with(expr()))).map(|(lhs, rhs)| {
            ExprKind::Eq { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
        }),
        attempt((expr(), spstring("!=").with(expr()))).map(|(lhs, rhs)| {
            ExprKind::Ne { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
        }),
        attempt((expr(), spstring(">").with(expr()))).map(|(lhs, rhs)| {
            ExprKind::Gt { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
        }),
        attempt((expr(), spstring("<").with(expr()))).map(|(lhs, rhs)| {
            ExprKind::Lt { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
        }),
        attempt((expr(), spstring(">=").with(expr()))).map(|(lhs, rhs)| {
            ExprKind::Gte { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
        }),
        attempt((expr(), spstring("<=").with(expr()))).map(|(lhs, rhs)| {
            ExprKind::Lte { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
        }),
        attempt((expr(), spstring("&&").with(expr()))).map(|(lhs, rhs)| {
            ExprKind::And { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
        }),
        attempt((expr(), spstring("||").with(expr()))).map(|(lhs, rhs)| {
            ExprKind::Or { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
        }),
        attempt(sptoken('!').with(expr()))
            .map(|expr| ExprKind::Not { expr: Arc::new(expr) }.to_expr()),
    ))
}

parser! {
    fn blang[I]()(I) -> Expr
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        blang_()
    }
}

fn select<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    string("select")
        .with(between(
            sptoken('{'),
            sptoken('}'),
            sep_by1((expr(), spstring("=>").with(expr())), sptoken(',')),
        ))
        .map(|arms: Vec<(Expr, Expr)>| {
            ExprKind::Select { arms: Arc::from(arms) }.to_expr()
        })
}

fn expr_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        attempt(module()),
        attempt(use_module()),
        attempt(alist()),
        attempt(do_block()),
        attempt(array()),
        attempt(apply()),
        attempt(lambda()),
        attempt(letbind()),
        attempt(connect()),
        attempt(interpolated()),
        attempt(literal()),
        attempt(blang()),
        attempt(select()),
        reference(),
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
                function: ["load"].into(),
                args: Arc::from_iter([ExprKind::Constant(Value::String(p)).to_expr()])
            }
            .to_expr(),
            parse_expr(s).unwrap()
        );
        let p = ExprKind::Apply {
            function: ["load"].into(),
            args: Arc::from_iter([ExprKind::Apply {
                args: Arc::from_iter([
                    ExprKind::Constant(Value::from("/foo/")).to_expr(),
                    ExprKind::Apply {
                        function: ["get"].into(),
                        args: Arc::from_iter([ExprKind::Apply {
                            args: Arc::from_iter([
                                ExprKind::Apply {
                                    function: ["get"].into(),
                                    args: Arc::from_iter([ExprKind::Constant(
                                        Value::from("sid"),
                                    )
                                    .to_expr()]),
                                }
                                .to_expr(),
                                ExprKind::Constant(Value::from("_var")).to_expr(),
                            ]),
                            function: ["str", "concat"].into(),
                        }
                        .to_expr()]),
                    }
                    .to_expr(),
                    ExprKind::Constant(Value::from("/baz")).to_expr(),
                ]),
                function: ["str", "concat"].into(),
            }
            .to_expr()]),
        }
        .to_expr();
        let s = r#"load("/foo/[get("[sid]_var")]/baz")"#;
        assert_eq!(p, parse_expr(s).unwrap());
        let s = r#""[true]""#;
        let p = ExprKind::Apply {
            args: Arc::from_iter([ExprKind::Constant(Value::True).to_expr()]),
            function: ["str", "concat"].into(),
        }
        .to_expr();
        assert_eq!(p, parse_expr(s).unwrap());
        let s = r#"a(a(a(get("[true]"))))"#;
        let p = ExprKind::Apply {
            args: Arc::from_iter([ExprKind::Apply {
                args: Arc::from_iter([ExprKind::Apply {
                    args: Arc::from_iter([ExprKind::Apply {
                        args: Arc::from_iter([ExprKind::Apply {
                            args: Arc::from_iter([
                                ExprKind::Constant(Value::True).to_expr()
                            ]),
                            function: ["str", "concat"].into(),
                        }
                        .to_expr()]),
                        function: ["get"].into(),
                    }
                    .to_expr()]),
                    function: ["a"].into(),
                }
                .to_expr()]),
                function: ["a"].into(),
            }
            .to_expr()]),
            function: ["a"].into(),
        }
        .to_expr();
        assert_eq!(p, parse_expr(s).unwrap());
    }

    #[test]
    fn expr_parse() {
        let s = r#"load(concat_path("foo", "bar", baz))"#;
        assert_eq!(
            ExprKind::Apply {
                args: Arc::from_iter([ExprKind::Apply {
                    args: Arc::from_iter([
                        ExprKind::Constant(Value::String(Chars::from("foo"))).to_expr(),
                        ExprKind::Constant(Value::String(Chars::from("bar"))).to_expr(),
                        ExprKind::Apply {
                            args: Arc::from_iter([ExprKind::Constant(Value::String(
                                Chars::from("baz")
                            ))
                            .to_expr()]),
                            function: ["get"].into(),
                        }
                        .to_expr()
                    ]),
                    function: ["path", "concat"].into(),
                }
                .to_expr()]),
                function: ["load"].into(),
            }
            .to_expr(),
            parse_expr(s).unwrap()
        );
        assert_eq!(
            ExprKind::Ref { name: ["sum"].into() }.to_expr(),
            parse_expr("sum").unwrap()
        );
        assert_eq!(
            ExprKind::Bind {
                export: false,
                name: "foo".into(),
                value: Arc::new(ExprKind::Constant(Value::I64(42)).to_expr())
            }
            .to_expr(),
            parse_expr("let foo = 42;").unwrap()
        );
        let src = ExprKind::Apply {
            args: Arc::from_iter([
                ExprKind::Constant(Value::F32(1.)).to_expr(),
                ExprKind::Apply {
                    args: Arc::from_iter([ExprKind::Constant(Value::String(
                        Chars::from("/foo/bar"),
                    ))
                    .to_expr()]),
                    function: ["load"].into(),
                }
                .to_expr(),
                ExprKind::Apply {
                    args: Arc::from_iter([
                        ExprKind::Constant(Value::F32(675.6)).to_expr(),
                        ExprKind::Apply {
                            args: Arc::from_iter([ExprKind::Constant(Value::String(
                                Chars::from("/foo/baz"),
                            ))
                            .to_expr()]),
                            function: ["load"].into(),
                        }
                        .to_expr(),
                    ]),
                    function: ["max"].into(),
                }
                .to_expr(),
                ExprKind::Apply { args: Arc::from_iter([]), function: ["rand"].into() }
                    .to_expr(),
            ]),
            function: ["sum"].into(),
        }
        .to_expr();
        let chs =
            r#"sum(f32:1., load("/foo/bar"), max(f32:675.6, load("/foo/baz")), rand())"#;
        assert_eq!(src, parse_expr(chs).unwrap());
    }
}
