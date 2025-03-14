use crate::{
    expr::{Arg, Expr, ExprId, ExprKind, ModPath, Pattern, StructurePattern},
    typ::{FnArgType, FnType, Refs, TVar, Type},
};
use anyhow::{bail, Result};
use arcstr::{literal, ArcStr};
use combine::{
    attempt, between, chainl1, choice, eof, look_ahead, many, many1, not_followed_by,
    optional,
    parser::{
        char::{alpha_num, digit, space, spaces, string},
        combinator::recognize,
        range::{take_while, take_while1},
    },
    sep_by, sep_by1,
    stream::{position, Range},
    token, unexpected_any, value, EasyParser, ParseError, Parser, RangeStream,
};
use compact_str::CompactString;
use fxhash::FxHashSet;
use netidx::{
    path::Path,
    publisher::{Typ, Value},
    utils::Either,
};
use netidx_netproto::value_parser::{
    escaped_string, int, value as netidx_value, VAL_ESC,
};
use smallvec::SmallVec;
use std::{marker::PhantomData, sync::LazyLock};
use triomphe::Arc;

#[cfg(test)]
mod test;

pub const BSCRIPT_ESC: [char; 4] = ['"', '\\', '[', ']'];
pub const RESERVED: LazyLock<FxHashSet<&str>> = LazyLock::new(|| {
    FxHashSet::from_iter([
        "true", "false", "ok", "null", "mod", "let", "select", "pub", "type", "fn",
        "cast", "if", "u32", "v32", "i32", "z32", "u64", "v64", "i64", "z64", "f32",
        "f64", "decimal", "datetime", "duration", "bool", "string", "bytes", "result",
        "null", "_", "?", "fn", "Array", "any",
    ])
});

fn spstring<'a, I>(s: &'static str) -> impl Parser<I, Output = &'a str>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(string(s))
}

fn ident<I>(cap: bool) -> impl Parser<I, Output = ArcStr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((
        take_while1(move |c: char| c.is_alphabetic() && cap == c.is_uppercase()),
        take_while(|c: char| c.is_alphanumeric() || c == '_'),
    ))
    .map(|s: CompactString| ArcStr::from(s.as_str()))
}

fn fname<I>() -> impl Parser<I, Output = ArcStr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    ident(false).then(|s| {
        if RESERVED.contains(&s.as_str()) {
            unexpected_any("can't use keyword as a function or variable name").left()
        } else {
            value(s).right()
        }
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

fn typname<I>() -> impl Parser<I, Output = ArcStr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    ident(true).then(|s| {
        if RESERVED.contains(&s.as_str()) {
            unexpected_any("can't use keyword as a type name").left()
        } else {
            value(s).right()
        }
    })
}

fn sptypname<I>() -> impl Parser<I, Output = ArcStr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(typname())
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

fn typath<I>() -> impl Parser<I, Output = ModPath>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    sep_by1(choice((attempt(spfname()), sptypname())), string("::")).then(
        |parts: SmallVec<[ArcStr; 8]>| {
            if parts.len() == 0 {
                unexpected_any("empty type path").left()
            } else {
                match parts.last().unwrap().chars().next() {
                    None => unexpected_any("empty name").left(),
                    Some(c) if c.is_lowercase() => {
                        unexpected_any("type names must be capitalized").left()
                    }
                    Some(_) => value(ModPath::from(parts)).right(),
                }
            }
        },
    )
}

fn sptypath<I>() -> impl Parser<I, Output = ModPath>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(typath())
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
                                args: Arc::from_iter(
                                    argvec.clone().into_iter().map(|a| (None, a)),
                                ),
                                function: ["str", "concat"].into(),
                            }
                            .to_expr(),
                        )
                    }
                    (Some(src @ Expr { kind: ExprKind::Constant(_), .. }), s) => {
                        argvec.extend([src, s.to_expr()]);
                        Some(
                            ExprKind::Apply {
                                args: Arc::from_iter(
                                    argvec.clone().into_iter().map(|a| (None, a)),
                                ),
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
                            ExprKind::Apply {
                                args: Arc::from_iter(
                                    argvec.clone().into_iter().map(|a| (None, a)),
                                ),
                                function,
                            }
                            .to_expr(),
                        )
                    }
                    (Some(Expr { kind: ExprKind::Bind { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::StructWith { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Array { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Any { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::StructRef { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::TupleRef { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Tuple { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Struct { .. }, .. }), _)
                    | (Some(Expr { kind: ExprKind::Qop(_), .. }), _)
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
    between(
        token('{'),
        sptoken('}'),
        sep_by(
            choice((
                attempt(spaces().with(use_module())),
                attempt(spaces().with(typedef())),
                expr(),
            )),
            attempt(sptoken(';')),
        ),
    )
    .map(|args: Vec<Expr>| ExprKind::Do { exprs: Arc::from(args) }.to_expr())
}

fn array<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    between(token('['), sptoken(']'), sep_by(expr(), csep())).map(
        |args: SmallVec<[Expr; 4]>| {
            ExprKind::Array { args: Arc::from_iter(args.into_iter()) }.to_expr()
        },
    )
}

fn structref<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (modpath().skip(sptoken('.')), spfname())
        .map(|(name, field)| ExprKind::StructRef { name, field }.to_expr())
}

fn tupleref<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (modpath().skip(sptoken('.')), int::<_, usize>())
        .map(|(name, field)| ExprKind::TupleRef { name, field }.to_expr())
}

fn arrayref<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        modpath(),
        between(
            token('['),
            sptoken(']'),
            choice((
                attempt(
                    (
                        spaces().with(optional(many1(digit()))).skip(spstring("..")),
                        spaces().with(optional(many1(digit()))),
                    )
                        .skip(look_ahead(sptoken(']'))),
                )
                .map(
                    |(start, end): (Option<CompactString>, Option<CompactString>)| {
                        let start = start
                            .map(|i| Value::U64(i.parse().unwrap()))
                            .unwrap_or(Value::Null);
                        let start = ExprKind::Constant(start).to_expr();
                        let end = end
                            .map(|i| Value::U64(i.parse().unwrap()))
                            .unwrap_or(Value::Null);
                        let end = ExprKind::Constant(end).to_expr();
                        Either::Left((start, end))
                    },
                ),
                attempt((
                    optional(attempt(expr())).skip(spstring("..")),
                    optional(attempt(expr())),
                ))
                .map(|(start, end)| {
                    let start =
                        start.unwrap_or(ExprKind::Constant(Value::Null).to_expr());
                    let end = end.unwrap_or(ExprKind::Constant(Value::Null).to_expr());
                    Either::Left((start, end))
                }),
                attempt(expr()).map(|e| Either::Right(e)),
            )),
        ),
    )
        .map(|(name, args)| {
            let a = ExprKind::Ref { name }.to_expr();
            match args {
                Either::Left((start, end)) => ExprKind::Apply {
                    function: ["op", "slice"].into(),
                    args: Arc::from_iter([(None, a), (None, start), (None, end)]),
                }
                .to_expr(),
                Either::Right(e) => ExprKind::Apply {
                    function: ["op", "index"].into(),
                    args: Arc::from_iter([(None, a), (None, e)]),
                }
                .to_expr(),
            }
        })
}

fn apply<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        modpath(),
        between(
            sptoken('('),
            sptoken(')'),
            sep_by(
                choice((
                    attempt(sptoken('#').with(fname()).skip(not_followed_by(token(':'))))
                        .map(|n| {
                            let e = ExprKind::Ref { name: [n.clone()].into() }.to_expr();
                            (Some(n), e)
                        }),
                    attempt((sptoken('#').with(fname()).skip(token(':')), expr()))
                        .map(|(n, e)| (Some(n), e)),
                    expr().map(|e| (None, e)),
                )),
                csep(),
            ),
        ),
    )
        .then(|(function, args): (ModPath, Vec<(Option<ArcStr>, Expr)>)| {
            let mut anon = false;
            for (a, _) in &args {
                if a.is_some() && anon {
                    return unexpected_any(
                        "labeled arguments must come before anonymous arguments",
                    )
                    .right();
                }
                anon |= a.is_none();
            }
            value((function, args)).left()
        })
        .map(|(function, args): (ModPath, Vec<(Option<ArcStr>, Expr)>)| {
            ExprKind::Apply { function, args: Arc::from(args) }.to_expr()
        })
}

fn any<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    string("any")
        .with(between(sptoken('('), sptoken(')'), sep_by(expr(), csep())))
        .map(|args: Vec<Expr>| ExprKind::Any { args: Arc::from(args) }.to_expr())
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
        attempt(spstring("datetime").map(|_| Typ::DateTime)),
        attempt(spstring("duration").map(|_| Typ::Duration)),
        attempt(spstring("bool").map(|_| Typ::Bool)),
        attempt(spstring("string").map(|_| Typ::String)),
        attempt(spstring("bytes").map(|_| Typ::Bytes)),
        attempt(spstring("error").map(|_| Typ::Error)),
        attempt(spstring("array").map(|_| Typ::Array)),
        attempt(spstring("null").map(|_| Typ::Null)),
    ))
    .skip(not_followed_by(choice((alpha_num(), token('_')))))
}

fn fntype<I>() -> impl Parser<I, Output = FnType<Refs>>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spstring("fn")
        .with((
            optional(attempt(between(
                token('<'),
                sptoken('>'),
                sep_by1((tvar().skip(sptoken(':')), typexp()), csep()),
            )))
            .map(|cs: Option<SmallVec<[(TVar<Refs>, Type<Refs>); 4]>>| {
                match cs {
                    None => Arc::from_iter([]),
                    Some(cs) => Arc::from_iter(cs),
                }
            }),
            between(
                token('('),
                sptoken(')'),
                sep_by(
                    choice((
                        attempt(
                            (
                                spaces()
                                    .with(optional(token('?')).map(|o| o.is_some()))
                                    .skip(token('#')),
                                fname().skip(token(':')),
                                typexp(),
                            )
                                .map(
                                    |(optional, name, typ)| {
                                        Either::Left(FnArgType {
                                            label: Some((name.into(), optional)),
                                            typ,
                                        })
                                    },
                                ),
                        ),
                        attempt(
                            typexp()
                                .map(|typ| Either::Left(FnArgType { label: None, typ })),
                        ),
                        attempt(
                            spstring("@args:").with(typexp()).map(|e| Either::Right(e)),
                        ),
                    )),
                    csep(),
                ),
            ),
            spstring("->").with(typexp()),
        ))
        .then(
            |(constraints, mut args, rtype): (
                Arc<[(TVar<Refs>, Type<Refs>)]>,
                Vec<Either<FnArgType<Refs>, Type<Refs>>>,
                Type<Refs>,
            )| {
                let vargs = match args.pop() {
                    None => None,
                    Some(Either::Right(t)) => Some(t),
                    Some(Either::Left(t)) => {
                        args.push(Either::Left(t));
                        None
                    }
                };
                if !args.iter().all(|a| a.is_left()) {
                    return unexpected_any(
                        "vargs must appear once at the end of the args",
                    )
                    .left();
                }
                let args = Arc::from_iter(args.into_iter().map(|t| match t {
                    Either::Left(t) => t,
                    Either::Right(_) => unreachable!(),
                }));
                let mut anon = false;
                for a in args.iter() {
                    if anon && a.label.is_some() {
                        return unexpected_any(
                            "anonymous args must appear after labeled args",
                        )
                        .left();
                    }
                    anon |= a.label.is_none();
                }
                value(FnType { args, vargs, rtype, constraints }).right()
            },
        )
}

fn tvar<I>() -> impl Parser<I, Output = TVar<Refs>>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    sptoken('\'').with(fname()).map(TVar::empty_named)
}

fn typexp_<I>() -> impl Parser<I, Output = Type<Refs>>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(sptoken('_').map(|_| Type::Bottom(PhantomData))),
        attempt(typeprim()).map(|typ| Type::Primitive(typ.into())),
        attempt(
            between(sptoken('['), sptoken(']'), sep_by(typexp(), csep()))
                .map(|ts: SmallVec<[Type<Refs>; 16]>| Type::flatten_set(ts)),
        ),
        attempt(between(sptoken('('), sptoken(')'), sep_by1(typexp(), csep())).then(
            |exps: SmallVec<[Type<Refs>; 16]>| {
                if exps.len() < 2 {
                    unexpected_any("tuples must have at least 2 elements").left()
                } else {
                    value(Type::Tuple(Arc::from_iter(exps))).right()
                }
            },
        )),
        attempt(
            between(
                sptoken('{'),
                sptoken('}'),
                sep_by1((spfname().skip(sptoken(':')), typexp()), csep()),
            )
            .then(|mut exps: SmallVec<[(ArcStr, Type<Refs>); 16]>| {
                let s = exps.iter().map(|(n, _)| n).collect::<FxHashSet<_>>();
                if s.len() < exps.len() {
                    return unexpected_any("struct field names must be unique").left();
                }
                exps.sort_by_key(|(n, _)| n.clone());
                value(Type::Struct(Arc::from_iter(exps))).right()
            }),
        ),
        attempt(fntype().map(|f| Type::Fn(Arc::new(f)))),
        attempt(spstring("Array").with(between(sptoken('<'), sptoken('>'), typexp())))
            .map(|t| Type::Array(Arc::new(t))),
        attempt(sptypath()).map(|n| Type::Ref(n)),
        attempt(tvar()).map(|tv| Type::TVar(tv)),
    ))
}

parser! {
    fn typexp[I]()(I) -> Type<Refs>
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        typexp_()
    }
}

fn lambda_args<I>() -> impl Parser<I, Output = (Vec<Arg>, Option<Option<Type<Refs>>>)>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    sep_by(
        (
            choice((
                attempt(spaces().with(structure_pattern())).map(|p| (false, p)),
                attempt(spaces().with(token('#').with(fname())))
                    .map(|b| (true, StructurePattern::Bind(b))),
                attempt(spstring("@args"))
                    .map(|s| (false, StructurePattern::Bind(ArcStr::from(s)))),
            )),
            optional(attempt(sptoken(':').with(typexp()))),
            optional(attempt(sptoken('=').with(expr()))),
        ),
        csep(),
    )
    .then(|v: Vec<((bool, StructurePattern), Option<Type<Refs>>, Option<Expr>)>| {
        let args = v
            .into_iter()
            .map(|((labeled, pattern), constraint, default)| {
                if !labeled && default.is_some() {
                    bail!("labeled")
                } else {
                    Ok(Arg { labeled: labeled.then_some(default), pattern, constraint })
                }
            })
            .collect::<Result<Vec<_>>>();
        match args {
            Ok(a) => value(a).right(),
            Err(_) => {
                unexpected_any("only labeled arguments may have a default value").left()
            }
        }
    })
    // @args must be last
    .then(|mut v: Vec<Arg>| {
        match v.iter().enumerate().find(|(_, a)| match &a.pattern {
            StructurePattern::Bind(n) if n == "@args" => true,
            _ => false,
        }) {
            None => value((v, None)).left(),
            Some((i, _)) => {
                if i == v.len() - 1 {
                    let a = v.pop().unwrap();
                    value((v, Some(a.constraint))).left()
                } else {
                    unexpected_any("@args must be the last argument").right()
                }
            }
        }
    })
    // labeled before anonymous args
    .then(|(v, vargs): (Vec<Arg>, Option<Option<Type<Refs>>>)| {
        let mut anon = false;
        for a in &v {
            if a.labeled.is_some() && anon {
                return unexpected_any("labeled args must come before anon args").right();
            }
            anon |= a.labeled.is_none();
        }
        value((v, vargs)).left()
    })
}

fn lambda<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        attempt(sep_by((tvar().skip(sptoken(':')), typexp()), csep()))
            .map(|tvs: SmallVec<[(TVar<Refs>, Type<Refs>); 4]>| Arc::from_iter(tvs)),
        between(sptoken('|'), sptoken('|'), lambda_args()),
        optional(attempt(spstring("->").with(typexp()).skip(space()))),
        choice((
            attempt(sptoken('\'').with(fname()).skip(not_followed_by(sptoken(':'))))
                .map(Either::Right),
            expr().map(|e| Either::Left(Arc::new(e))),
        )),
    )
        .map(|(constraints, (args, vargs), rtype, body)| {
            let args = Arc::from_iter(args);
            ExprKind::Lambda { args, vargs, rtype, constraints, body }.to_expr()
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
            .with((structure_pattern(), optional(attempt(sptoken(':').with(typexp())))))
            .skip(spstring("=")),
        expr(),
    )
        .map(|(export, (pattern, typ), v)| {
            ExprKind::Bind { export, pattern, typ, value: Arc::new(v) }.to_expr()
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
    modpath()
        .skip(not_followed_by(token('[')))
        .map(|name| ExprKind::Ref { name }.to_expr())
}

fn qop<I, P: Parser<I, Output = Expr>>(p: P) -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (p, optional(attempt(sptoken('?')))).map(|(e, qop)| match qop {
        None => e,
        Some(_) => ExprKind::Qop(Arc::new(e)).to_expr(),
    })
}

fn arith_term<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spaces().with(qop(do_block()))),
        attempt(spaces().with(qop(select()))),
        attempt(spaces().with(qop(cast()))),
        attempt(spaces().with(qop(any()))),
        attempt(spaces().with(qop(apply()))),
        attempt(spaces().with(interpolated())),
        attempt(spaces().with(literal())),
        attempt(spaces().with(qop(arrayref()))),
        attempt(spaces().with(qop(tupleref()))),
        attempt(spaces().with(qop(structref()))),
        attempt(spaces().with(qop(reference()))),
        attempt(
            sptoken('!')
                .with(arith())
                .map(|expr| ExprKind::Not { expr: Arc::new(expr) }.to_expr()),
        ),
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

fn slice_pattern<I>() -> impl Parser<I, Output = StructurePattern>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    macro_rules! all_left {
        ($pats:expr) => {{
            let mut err = false;
            let pats: Arc<[StructurePattern]> =
                Arc::from_iter($pats.into_iter().map(|s| match s {
                    Either::Left(s) => s,
                    Either::Right(_) => {
                        err = true;
                        StructurePattern::Ignore
                    }
                }));
            if err {
                return unexpected_any("invalid pattern").left();
            }
            pats
        }};
    }
    (
        optional(attempt(spfname().skip(sptoken('@')))),
        between(
            sptoken('['),
            sptoken(']'),
            sep_by(
                choice((
                    attempt(spstring("..")).map(|_| Either::Right(None)),
                    attempt(spfname().skip(spstring("..")))
                        .map(|n| Either::Right(Some(n))),
                    structure_pattern().map(|p| Either::Left(p)),
                )),
                csep(),
            ),
        ),
    )
        .then(
            |(all, mut pats): (
                Option<ArcStr>,
                SmallVec<[Either<StructurePattern, Option<ArcStr>>; 8]>,
            )| {
                if pats.len() == 0 {
                    value(StructurePattern::Slice { all, binds: Arc::from_iter([]) })
                        .right()
                } else if pats.len() == 1 {
                    match pats.pop().unwrap() {
                        Either::Left(s) => value(StructurePattern::Slice {
                            all,
                            binds: Arc::from_iter([s]),
                        })
                        .right(),
                        Either::Right(_) => {
                            unexpected_any("invalid singular range match").left()
                        }
                    }
                } else {
                    match (&pats[0], &pats[pats.len() - 1]) {
                        (Either::Right(_), Either::Right(_)) => {
                            unexpected_any("invalid pattern").left()
                        }
                        (Either::Right(_), Either::Left(_)) => {
                            let head = pats.remove(0).right().unwrap();
                            let suffix = all_left!(pats);
                            value(StructurePattern::SliceSuffix { all, head, suffix })
                                .right()
                        }
                        (Either::Left(_), Either::Right(_)) => {
                            let tail = pats.pop().unwrap().right().unwrap();
                            let prefix = all_left!(pats);
                            value(StructurePattern::SlicePrefix { all, tail, prefix })
                                .right()
                        }
                        (Either::Left(_), Either::Left(_)) => {
                            value(StructurePattern::Slice { all, binds: all_left!(pats) })
                                .right()
                        }
                    }
                }
            },
        )
}

fn tuple_pattern<I>() -> impl Parser<I, Output = StructurePattern>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        optional(attempt(spfname().skip(sptoken('@')))),
        between(sptoken('('), sptoken(')'), sep_by1(structure_pattern(), csep())),
    )
        .then(|(all, binds): (Option<ArcStr>, SmallVec<[StructurePattern; 8]>)| {
            if binds.len() < 2 {
                unexpected_any("tuples must have at least 2 elements").left()
            } else {
                value(StructurePattern::Tuple { all, binds: Arc::from_iter(binds) })
                    .right()
            }
        })
}

fn struct_pattern<I>() -> impl Parser<I, Output = StructurePattern>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        optional(attempt(spfname().skip(sptoken('@')))),
        between(
            sptoken('{'),
            sptoken('}'),
            sep_by1(
                choice((
                    attempt((spfname().skip(sptoken(':')), structure_pattern()))
                        .map(|(s, p)| (s, p, true)),
                    attempt(spfname()).map(|s| {
                        let p = StructurePattern::Bind(s.clone());
                        (s, p, true)
                    }),
                    spstring("..")
                        .map(|_| (literal!(""), StructurePattern::Ignore, false)),
                )),
                csep(),
            ),
        ),
    )
        .then(
            |(all, mut binds): (
                Option<ArcStr>,
                SmallVec<[(ArcStr, StructurePattern, bool); 8]>,
            )| {
                let mut exhaustive = true;
                binds.retain(|(_, _, ex)| {
                    exhaustive &= *ex;
                    *ex
                });
                binds.sort_by_key(|(s, _, _)| s.clone());
                let s = binds.iter().map(|(s, _, _)| s).collect::<FxHashSet<_>>();
                if s.len() < binds.len() {
                    unexpected_any("struct fields must be unique").left()
                } else {
                    let binds = Arc::from_iter(binds.into_iter().map(|(s, p, _)| (s, p)));
                    value(StructurePattern::Struct { all, exhaustive, binds }).right()
                }
            },
        )
}

fn structure_pattern_<I>() -> impl Parser<I, Output = StructurePattern>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(slice_pattern()),
        attempt(tuple_pattern()),
        attempt(struct_pattern()),
        attempt(netidx_value(&VAL_ESC)).map(|v| StructurePattern::Literal(v)),
        attempt(sptoken('_')).map(|_| StructurePattern::Ignore),
        spfname().map(|name| StructurePattern::Bind(name)),
    ))
}

parser! {
    fn structure_pattern[I]()(I) -> StructurePattern
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        structure_pattern_()
    }
}

fn pattern<I>() -> impl Parser<I, Output = Pattern>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        optional(attempt(typexp().skip(space().with(spstring("as "))))),
        structure_pattern(),
        optional(attempt(space().with(spstring("if").with(space()).with(expr())))),
    )
        .map(
            |(type_predicate, structure_predicate, guard): (
                Option<Type<Refs>>,
                StructurePattern,
                Option<Expr>,
            )| { Pattern { type_predicate, structure_predicate, guard } },
        )
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
        string("cast").with(between(token('<'), sptoken('>'), typexp())),
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
    (string("type").with(sptypname()), sptoken('=').with(typexp()))
        .map(|(name, typ)| ExprKind::TypeDef { name, typ }.to_expr())
}

fn tuple<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    between(token('('), sptoken(')'), sep_by1(expr(), csep())).then(
        |exprs: SmallVec<[Expr; 8]>| {
            if exprs.len() < 2 {
                unexpected_any("tuples must have at least 2 elements").left()
            } else {
                value(ExprKind::Tuple { args: Arc::from_iter(exprs) }.to_expr()).right()
            }
        },
    )
}

fn structure<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    between(
        token('{'),
        sptoken('}'),
        sep_by1((spfname().skip(sptoken(':')), expr()), csep()),
    )
    .then(|mut exprs: SmallVec<[(ArcStr, Expr); 8]>| {
        let s = exprs.iter().map(|(n, _)| n).collect::<FxHashSet<_>>();
        if s.len() < exprs.len() {
            return unexpected_any("struct fields must be unique").left();
        }
        exprs.sort_by_key(|(n, _)| n.clone());
        value(ExprKind::Struct { args: Arc::from_iter(exprs) }.to_expr()).right()
    })
}

fn structwith<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    between(
        token('{'),
        sptoken('}'),
        (
            spmodpath().skip(space()).skip(spstring("with")).skip(space()),
            sep_by1((spfname().skip(sptoken(':')), expr()), csep()),
        ),
    )
    .then(|(name, mut exprs): (ModPath, SmallVec<[(ArcStr, Expr); 8]>)| {
        let s = exprs.iter().map(|(n, _)| n).collect::<FxHashSet<_>>();
        if s.len() < exprs.len() {
            return unexpected_any("struct fields must be unique").left();
        }
        exprs.sort_by_key(|(n, _)| n.clone());
        value(ExprKind::StructWith { name, replace: Arc::from_iter(exprs) }.to_expr())
            .right()
    })
}

fn expr_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spaces().with(array())),
        attempt(spaces().with(arith())),
        attempt(spaces().with(tuple())),
        attempt(spaces().with(structure())),
        attempt(spaces().with(structwith())),
        attempt(spaces().with(qop(do_block()))),
        attempt(spaces().with(lambda())),
        attempt(spaces().with(letbind())),
        attempt(spaces().with(connect())),
        attempt(spaces().with(qop(select()))),
        attempt(spaces().with(qop(cast()))),
        attempt(spaces().with(qop(any()))),
        attempt(spaces().with(qop(apply()))),
        attempt(spaces().with(interpolated())),
        attempt(spaces().with(literal())),
        attempt(spaces().with(qop(arrayref()))),
        attempt(spaces().with(qop(tupleref()))),
        attempt(spaces().with(qop(structref()))),
        attempt(spaces().with(qop(reference()))),
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
        attempt(spaces().with(qop(do_block()))),
        attempt(spaces().with(use_module())),
        attempt(spaces().with(typedef())),
        attempt(spaces().with(letbind())),
        attempt(spaces().with(connect())),
        attempt(spaces().with(qop(any()))),
        attempt(spaces().with(qop(apply()))),
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

/// Parse one or more toplevel module expressions
///
/// followed by (optional) whitespace and then eof. At least one
/// expression is required otherwise this function will fail.
///
/// if you wish to parse a str containing one and only one module
/// expression just call [str.parse::<Expr>()].
pub fn parse_many_modexpr(s: &str) -> anyhow::Result<Vec<Expr>> {
    many1(modexpr())
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

pub fn parse_fn_type(s: &str) -> anyhow::Result<FnType<Refs>> {
    fntype()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{e}")))
}

/// Parse an expression instead of a module expression
pub fn parse_expr(s: &str) -> anyhow::Result<Expr> {
    expr()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{e}")))
}
