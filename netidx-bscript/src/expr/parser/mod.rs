use super::{FnType, Pattern, Type};
use crate::expr::{Expr, ExprId, ExprKind, ModPath};
use arcstr::ArcStr;
use combine::{
    attempt, between, chainl1, choice, eof, many, many1, optional,
    parser::{
        char::{space, spaces, string},
        combinator::recognize,
        range::{take_while, take_while1},
    },
    sep_by, sep_by1,
    stream::{position, Range},
    token, unexpected_any, value, EasyParser, ParseError, Parser, RangeStream,
};
use enumflags2::BitFlags;
use netidx::{
    path::Path,
    publisher::{Typ, Value},
    utils::Either,
};
use netidx_netproto::value_parser::{escaped_string, value as netidx_value};
use smallvec::SmallVec;
use triomphe::Arc;

#[cfg(test)]
mod test;

pub const BSCRIPT_ESC: [char; 4] = ['"', '\\', '[', ']'];
pub const RESERVED: [&str; 11] =
    ["true", "false", "ok", "null", "mod", "let", "select", "pub", "type", "fn", "cast"];

fn spstring<'a, I>(s: &'static str) -> impl Parser<I, Output = &'a str>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(string(s))
}

fn fname<I>() -> impl Parser<I, Output = ArcStr>
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
        if RESERVED.contains(&s.as_str()) {
            unexpected_any("can't use keyword as a function or variable name").left()
        } else {
            value(s).right()
        }
    })
    .map(ArcStr::from)
}

fn variant_tag<I>() -> impl Parser<I, Output = Typ>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((
        take_while1(|c: char| c.is_alphabetic() && c.is_uppercase()),
        take_while(|c: char| c.is_alphanumeric() || c == '_'),
    ))
    .then(|s: String| match s.parse::<Typ>() {
        Err(_) => unexpected_any("invalid variant tag").left(),
        Ok(typ) => value(typ).right(),
    })
}

fn spfname<I>() -> impl Parser<I, Output = ArcStr>
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
    sep_by1(fname(), string("::"))
        .map(|v: SmallVec<[ArcStr; 4]>| ModPath(Path::from_iter(v)))
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
    .map(|toks: SmallVec<[Intp; 8]>| {
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
                    | (Some(Expr { kind: ExprKind::Ne { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Lt { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Gt { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Gte { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Lte { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::And { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Or { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Not { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Add { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Sub { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Mul { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Div { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Select { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::TypeCast { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::TypeDef { .. }, .. }), _)
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
        optional(string("pub").skip(space())).map(|o| o.is_some()),
        spstring("mod").with(space()).with(spfname()),
        choice((
            attempt(sptoken(';')).map(|_| None),
            between(sptoken('{'), sptoken('}'), many(modexpr()))
                .map(|m: Vec<Expr>| Some(Arc::from(m))),
        )),
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
        .with(space())
        .with(spmodpath())
        .map(|name| ExprKind::Use { name }.to_expr())
}

fn do_block<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    between(token('{'), sptoken('}'), sep_by(expr(), attempt(sptoken(';'))))
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

fn typeprim<I>() -> impl Parser<I, Output = Typ>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spstring("u32").map(|_| Typ::U32)),
        attempt(spstring("v32").map(|_| Typ::V32)),
        attempt(spstring("i32").map(|_| Typ::I32)),
        attempt(spstring("z32").map(|_| Typ::Z32)),
        attempt(spstring("u64").map(|_| Typ::U64)),
        attempt(spstring("v64").map(|_| Typ::V64)),
        attempt(spstring("i64").map(|_| Typ::I64)),
        attempt(spstring("z64").map(|_| Typ::Z64)),
        attempt(spstring("f32").map(|_| Typ::F32)),
        attempt(spstring("f64").map(|_| Typ::F64)),
        attempt(spstring("decimal").map(|_| Typ::Decimal)),
        attempt(spstring("dateTime").map(|_| Typ::DateTime)),
        attempt(spstring("duration").map(|_| Typ::Duration)),
        attempt(spstring("bool").map(|_| Typ::Bool)),
        attempt(spstring("string").map(|_| Typ::String)),
        attempt(spstring("bytes").map(|_| Typ::Bytes)),
        attempt(spstring("result").map(|_| Typ::Result)),
        attempt(spstring("array").map(|_| Typ::Array)),
        attempt(spstring("null").map(|_| Typ::Null)),
    ))
}

fn fntype<I>() -> impl Parser<I, Output = Type>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        spstring("fn").with(between(
            token('('),
            sptoken(')'),
            sep_by(
                choice((
                    attempt(typexp().map(|e| Either::Left(e))),
                    attempt(spstring("@args:").with(typexp()).map(|e| Either::Right(e))),
                )),
                csep(),
            ),
        )),
        spstring("->").with(typexp()),
    )
        .then(|(mut args, rtype): (Vec<Either<Type, Type>>, Type)| {
            let vargs = match args.pop() {
                Some(Either::Right(t)) => t,
                Some(Either::Left(t)) => {
                    args.push(Either::Left(t));
                    Type::Bottom
                }
                None => Type::Bottom,
            };
            if !args.iter().all(|a| a.is_left()) {
                unexpected_any("vargs must appear once at the end of the args").left()
            } else {
                let args = args.into_iter().map(|t| match t {
                    Either::Left(t) => t,
                    Either::Right(_) => unreachable!(),
                });
                value(Type::Fn(Arc::new(FnType {
                    args: Arc::from_iter(args),
                    vargs,
                    rtype,
                })))
                .right()
            }
        })
}

fn typexp_<I>() -> impl Parser<I, Output = Type>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(sptoken('_').map(|_| Type::Bottom)),
        typeprim().map(|typ| Type::Primitive(typ.into())),
        attempt(between(sptoken('['), sptoken(']'), sep_by(typexp(), csep())).map(
            |mut ts: Vec<Type>| {
                let mut prims: BitFlags<Typ> = BitFlags::empty();
                ts.retain(|t| match t {
                    Type::Primitive(s) => {
                        prims.insert(*s);
                        false
                    }
                    _ => true,
                });
                if ts.len() == 0 {
                    Type::Primitive(prims)
                } else {
                    ts.push(Type::Primitive(prims));
                    Type::Set(Arc::from_iter(ts))
                }
            },
        )),
        attempt(fntype()),
        attempt(modpath().map(|n| Type::Ref(n))),
    ))
}

parser! {
    fn typexp[I]()(I) -> Type
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        typexp_()
    }
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
            sep_by(
                (
                    choice((
                        attempt(spfname()),
                        attempt(spstring("@args")).map(ArcStr::from),
                    )),
                    attempt(optional(spstring("->").with(typexp()))),
                ),
                csep(),
            )
            .then(|mut v: SmallVec<[(ArcStr, Option<Type>); 4]>| {
                match v.iter().enumerate().find(|(_, (a, _))| &**a == "@args") {
                    None => value((v, false)).left(),
                    Some((i, _)) => {
                        if i == v.len() - 1 {
                            v.pop();
                            value((v, true)).left()
                        } else {
                            unexpected_any("@args must be the last argument").right()
                        }
                    }
                }
            }),
        ),
        attempt(optional(sptoken(':').with(typexp()))),
        choice((
            attempt(sptoken('\'')).with(fname()).map(Either::Right),
            expr().map(|e| Either::Left(Arc::new(e))),
        )),
    )
        .map(|((args, vargs), rtype, body)| {
            let args = Arc::from_iter(args.into_iter());
            ExprKind::Lambda { args, vargs, rtype, body }.to_expr()
        })
}

fn letbind<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        optional(string("pub").skip(space())).map(|o| o.is_some()),
        spstring("let")
            .with(space())
            .with((spfname(), attempt(optional(sptoken(':').with(typexp())))))
            .skip(spstring("=")),
        expr(),
    )
        .map(|(export, (name, typ), value)| {
            ExprKind::Bind { export, name, typ, value: Arc::new(value) }.to_expr()
        })
}

fn connect<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (modpath().skip(spstring("<-")), expr())
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

fn arith_term<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spaces().with(do_block())),
        attempt(spaces().with(array())),
        attempt(spaces().with(select())),
        attempt(spaces().with(apply())),
        attempt(spaces().with(interpolated())),
        attempt(spaces().with(literal())),
        attempt(spaces().with(reference())),
        attempt(sptoken('!').with(arith()))
            .map(|expr| ExprKind::Not { expr: Arc::new(expr) }.to_expr()),
        attempt(between(sptoken('('), sptoken(')'), arith())),
    ))
    .skip(spaces())
}

fn arith_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(chainl1(
            arith_term(),
            choice((
                attempt(spstring("+")),
                attempt(spstring("-")),
                attempt(spstring("*")),
                attempt(spstring("/")),
                attempt(spstring("==")),
                attempt(spstring("!=")),
                attempt(spstring(">=")),
                attempt(spstring("<=")),
                attempt(spstring(">")),
                attempt(spstring("<")),
                attempt(spstring("&&")),
                attempt(spstring("||")),
            ))
            .map(|op: &str| match op {
                "+" => |lhs, rhs| {
                    ExprKind::Add { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                "-" => |lhs, rhs| {
                    ExprKind::Sub { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                "*" => |lhs, rhs| {
                    ExprKind::Mul { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                "/" => |lhs, rhs| {
                    ExprKind::Div { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                "==" => |lhs, rhs| {
                    ExprKind::Eq { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                "!=" => |lhs, rhs| {
                    ExprKind::Ne { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                ">" => |lhs, rhs| {
                    ExprKind::Gt { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                "<" => |lhs, rhs| {
                    ExprKind::Lt { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                ">=" => |lhs, rhs| {
                    ExprKind::Gte { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                "<=" => |lhs, rhs| {
                    ExprKind::Lte { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                "&&" => |lhs, rhs| {
                    ExprKind::And { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                "||" => |lhs, rhs| {
                    ExprKind::Or { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr()
                },
                _ => unreachable!(),
            }),
        )),
        attempt(sptoken('!').with(arith_term()))
            .map(|expr| ExprKind::Not { expr: Arc::new(expr) }.to_expr()),
        attempt(between(sptoken('('), sptoken(')'), arith())),
    ))
}

parser! {
    fn arith[I]()(I) -> Expr
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        arith_()
    }
}

fn typ_pattern<I>() -> impl Parser<I, Output = (Typ, ArcStr)>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with((variant_tag().skip(sptoken('(')), spfname().skip(sptoken(')'))))
}

fn pattern_guard<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spstring("if").with(space()).with(expr())
}

fn pattern<I>() -> impl Parser<I, Output = Pattern>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(sptoken('_')).map(|_| Pattern::Underscore),
        attempt(
            (spfname(), optional(attempt(space().with(pattern_guard())))).map(
                |(bind, guard)| Pattern::Typ { tag: Arc::from_iter([]), bind, guard },
            ),
        ),
        attempt(
            (
                sep_by1(typ_pattern(), attempt(sptoken('|'))),
                optional(attempt(space().with(pattern_guard()))),
            )
                .then(
                    |(binds, guard): (SmallVec<[(Typ, ArcStr); 4]>, Option<Expr>)| {
                        let (tag, mut binds): (
                            SmallVec<[Typ; 8]>,
                            SmallVec<[ArcStr; 8]>,
                        ) = binds.into_iter().unzip();
                        binds.dedup();
                        if binds.len() != 1 {
                            unexpected_any("all binds must be the same").left()
                        } else {
                            value(Pattern::Typ {
                                tag: Arc::from_iter(tag),
                                bind: binds.pop().unwrap(),
                                guard,
                            })
                            .right()
                        }
                    },
                ),
        ),
    ))
}

fn select<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    string("select")
        .with(space())
        .with((
            expr(),
            between(
                sptoken('{'),
                sptoken('}'),
                sep_by1((pattern(), spstring("=>").with(expr())), csep()),
            ),
        ))
        .map(|(arg, arms): (Expr, Vec<(Pattern, Expr)>)| {
            ExprKind::Select { arg: Arc::new(arg), arms: Arc::from(arms) }.to_expr()
        })
}

fn cast<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        string("cast").with(between(token('<'), sptoken('>'), typeprim())),
        between(sptoken('('), sptoken(')'), expr()),
    )
        .map(|(typ, e)| ExprKind::TypeCast { expr: Arc::new(e), typ }.to_expr())
}

fn typedef<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (string("type").with(spfname()), sptoken('=').with(typexp()))
        .map(|(name, typ)| ExprKind::TypeDef { name, typ }.to_expr())
}

fn expr_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spaces().with(arith())),
        attempt(spaces().with(do_block())),
        attempt(spaces().with(array())),
        attempt(spaces().with(lambda())),
        attempt(spaces().with(letbind())),
        attempt(spaces().with(connect())),
        attempt(spaces().with(select())),
        attempt(spaces().with(cast())),
        attempt(spaces().with(apply())),
        attempt(spaces().with(interpolated())),
        attempt(spaces().with(literal())),
        attempt(spaces().with(reference())),
    ))
}

parser! {
    fn expr[I]()(I) -> Expr
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        expr_()
    }
}

fn modexpr_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spaces().with(module())),
        attempt(spaces().with(use_module())),
        attempt(spaces().with(typedef())),
        attempt(spaces().with(do_block())),
        attempt(spaces().with(array())),
        attempt(spaces().with(letbind())),
        attempt(spaces().with(connect())),
        attempt(spaces().with(interpolated())),
        attempt(spaces().with(apply())),
    ))
}

parser! {
    fn modexpr[I]()(I) -> Expr
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        modexpr_()
    }
}

pub(super) fn parse(s: &str) -> anyhow::Result<Expr> {
    modexpr()
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

/// Parse one or more toplevel module expressions followed by
/// (optional) whitespace and then eof. At least one expression is
/// required otherwise this function will fail.
///
/// if you wish to parse a str containing one and only one module
/// expression just call [str.parse::<Expr>()].
pub fn parse_many(s: &str) -> anyhow::Result<Vec<Expr>> {
    many1(modexpr())
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}
