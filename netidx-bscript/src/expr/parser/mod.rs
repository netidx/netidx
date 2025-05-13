use crate::{
    expr::{
        Arg, Bind, Expr, ExprId, ExprKind, Lambda, ModPath, ModuleKind, Pattern,
        StructurePattern,
    },
    typ::{FnArgType, FnType, Refs, TVar, Type},
};
use anyhow::{bail, Result};
use arcstr::{literal, ArcStr};
use combine::{
    attempt, between, chainl1, choice, eof, look_ahead, many, many1, none_of,
    not_followed_by, optional,
    parser::{
        char::{alpha_num, digit, space, string},
        combinator::recognize,
        range::{take_while, take_while1},
    },
    position, sep_by, sep_by1,
    stream::{
        position::{self, SourcePosition},
        Range,
    },
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
use parking_lot::RwLock;
use smallvec::{smallvec, SmallVec};
use std::{marker::PhantomData, sync::LazyLock};
use triomphe::Arc;

use super::Origin;

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

fn spaces<I>() -> impl Parser<I, Output = ()>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        combine::parser::char::spaces(),
        string("//")
            .with(not_followed_by(token('/')))
            .with(many(none_of(['\n'])))
            .map(|_: String| ()),
    ))
}

fn doc_comment<I>() -> impl Parser<I, Output = Option<ArcStr>>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    optional(attempt(
        string("///").with(many(none_of(['\n']))).map(|s: String| ArcStr::from(s)),
    ))
}

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

pub(crate) fn modpath<I>() -> impl Parser<I, Output = ModPath>
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
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    #[derive(Debug, Clone)]
    enum Intp {
        Lit(SourcePosition, String),
        Expr(Expr),
    }
    impl Intp {
        fn to_expr(self) -> Expr {
            match self {
                Intp::Lit(pos, s) => Expr {
                    id: ExprId::new(),
                    pos,
                    kind: ExprKind::Constant(Value::from(s)),
                },
                Intp::Expr(s) => s,
            }
        }
    }
    (
        position(),
        between(
            token('"'),
            token('"'),
            many(choice((
                attempt(between(token('['), sptoken(']'), expr()).map(Intp::Expr)),
                (position(), escaped_string(&BSCRIPT_ESC)).then(|(pos, s)| {
                    if s.is_empty() {
                        unexpected_any("empty string").right()
                    } else {
                        value(Intp::Lit(pos, s)).left()
                    }
                }),
            ))),
        ),
    )
        .map(|(pos, toks): (_, SmallVec<[Intp; 8]>)| {
            let mut argvec = vec![];
            toks.into_iter()
                .fold(None, |src, tok| -> Option<Expr> {
                    match (src, tok) {
                        (None, t @ Intp::Lit(_, _)) => Some(t.to_expr()),
                        (None, Intp::Expr(s)) => {
                            argvec.push(s);
                            Some(
                                ExprKind::StringInterpolate {
                                    args: Arc::from_iter(argvec.clone().into_iter()),
                                }
                                .to_expr(pos),
                            )
                        }
                        (Some(src @ Expr { kind: ExprKind::Constant(_), .. }), s) => {
                            argvec.extend([src, s.to_expr()]);
                            Some(
                                ExprKind::StringInterpolate {
                                    args: Arc::from_iter(argvec.clone().into_iter()),
                                }
                                .to_expr(pos),
                            )
                        }
                        (
                            Some(Expr {
                                kind: ExprKind::StringInterpolate { args: _ },
                                ..
                            }),
                            s,
                        ) => {
                            argvec.push(s.to_expr());
                            Some(
                                ExprKind::StringInterpolate {
                                    args: Arc::from_iter(argvec.clone().into_iter()),
                                }
                                .to_expr(pos),
                            )
                        }
                        (Some(Expr { kind: ExprKind::Bind { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::StructWith { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::Array { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::Any { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::StructRef { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::TupleRef { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::Tuple { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::Variant { .. }, .. }), _)
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
                        | (Some(Expr { kind: ExprKind::ArrayRef { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::ArraySlice { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::Apply { .. }, .. }), _)
                        | (Some(Expr { kind: ExprKind::Lambda { .. }, .. }), _) => {
                            unreachable!()
                        }
                    }
                })
                .unwrap_or_else(|| ExprKind::Constant(Value::from("")).to_expr(pos))
        })
}

parser! {
    fn interpolated[I]()(I) -> Expr
    where [I: RangeStream<Token = char, Position = SourcePosition>, I::Range: Range]
    {
        interpolated_()
    }
}

fn module<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        optional(string("pub").skip(space())).map(|o| o.is_some()),
        spstring("mod").with(space()).with(spfname()),
        choice((
            attempt(sptoken(';')).map(|_| ModuleKind::Unresolved),
            between(sptoken('{'), sptoken('}'), sep_by(modexpr(), attempt(sptoken(';'))))
                .map(|m: Vec<Expr>| ModuleKind::Inline(Arc::from(m))),
        )),
    )
        .map(|(pos, export, name, value)| {
            ExprKind::Module { name, export, value }.to_expr(pos)
        })
}

fn use_module<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (position(), string("use").with(space()).with(spmodpath()))
        .map(|(pos, name)| ExprKind::Use { name }.to_expr(pos))
}

fn do_block<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
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
        ),
    )
        .map(|(pos, args): (_, Vec<Expr>)| {
            ExprKind::Do { exprs: Arc::from(args) }.to_expr(pos)
        })
}

fn array<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (position(), between(token('['), sptoken(']'), sep_by(expr(), csep()))).map(
        |(pos, args): (_, SmallVec<[Expr; 4]>)| {
            ExprKind::Array { args: Arc::from_iter(args.into_iter()) }.to_expr(pos)
        },
    )
}

fn apply_pexp<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spaces().with(qop(do_block()))),
        attempt(spaces().with(qop(reference()))),
        between(sptoken('('), sptoken(')'), expr()),
    ))
}

fn ref_pexp<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spaces().with(qop(apply()))),
        attempt(spaces().with(qop(do_block()))),
        attempt(spaces().with(qop(reference()))),
        between(sptoken('('), sptoken(')'), expr()),
    ))
}

fn structref<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (position(), ref_pexp().skip(sptoken('.')), spfname()).map(|(pos, source, field)| {
        ExprKind::StructRef { source: Arc::new(source), field }.to_expr(pos)
    })
}

fn tupleref<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (position(), ref_pexp().skip(sptoken('.')), int::<_, usize>()).map(
        |(pos, source, field)| {
            ExprKind::TupleRef { source: Arc::new(source), field }.to_expr(pos)
        },
    )
}

fn arrayref<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        ref_pexp(),
        between(
            token('['),
            sptoken(']'),
            choice((
                attempt(
                    (
                        position(),
                        spaces().with(optional(many1(digit()))).skip(spstring("..")),
                        spaces().with(optional(many1(digit()))),
                    )
                        .skip(look_ahead(sptoken(']'))),
                )
                .map(
                    |(pos, start, end): (
                        _,
                        Option<CompactString>,
                        Option<CompactString>,
                    )| {
                        let start = start.map(|i| Value::U64(i.parse().unwrap()));
                        let start = start.map(|e| ExprKind::Constant(e).to_expr(pos));
                        let end = end.map(|i| Value::U64(i.parse().unwrap()));
                        let end = end.map(|e| ExprKind::Constant(e).to_expr(pos));
                        Either::Left((start, end))
                    },
                ),
                attempt((
                    optional(attempt(expr())).skip(spstring("..")),
                    optional(attempt(expr())),
                ))
                .map(|(start, end)| Either::Left((start, end))),
                attempt(expr()).map(|e| Either::Right(e)),
            )),
        ),
    )
        .map(|(pos, a, args)| match args {
            Either::Left((start, end)) => ExprKind::ArraySlice {
                source: Arc::new(a),
                start: start.map(Arc::new),
                end: end.map(Arc::new),
            }
            .to_expr(pos),
            Either::Right(i) => {
                ExprKind::ArrayRef { source: Arc::new(a), i: Arc::new(i) }.to_expr(pos)
            }
        })
}

fn apply<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        apply_pexp(),
        between(
            sptoken('('),
            sptoken(')'),
            sep_by(
                choice((
                    attempt((
                        position(),
                        sptoken('#').with(fname()).skip(not_followed_by(token(':'))),
                    ))
                    .map(|(pos, n)| {
                        let e = ExprKind::Ref { name: [n.clone()].into() }.to_expr(pos);
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
        .then(|(pos, function, args): (_, Expr, Vec<(Option<ArcStr>, Expr)>)| {
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
            value((pos, function, args)).left()
        })
        .map(|(pos, function, args): (_, Expr, Vec<(Option<ArcStr>, Expr)>)| {
            ExprKind::Apply { function: Arc::new(function), args: Arc::from(args) }
                .to_expr(pos)
        })
}

fn any<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        string("any").with(between(sptoken('('), sptoken(')'), sep_by(expr(), csep()))),
    )
        .map(|(pos, args): (_, Vec<Expr>)| {
            ExprKind::Any { args: Arc::from(args) }.to_expr(pos)
        })
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
                    Some(cs) => Arc::new(RwLock::new(cs.into_iter().collect())),
                    None => Arc::new(RwLock::new(vec![])),
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
                Arc<RwLock<Vec<(TVar<Refs>, Type<Refs>)>>>,
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
        attempt(
            (
                sptoken('`').with(typname()),
                optional(attempt(between(
                    token('('),
                    sptoken(')'),
                    sep_by1(typexp(), csep()),
                ))),
            )
                .map(
                    |(tag, typs): (ArcStr, Option<SmallVec<[Type<Refs>; 5]>>)| {
                        let t = match typs {
                            None => smallvec![],
                            Some(v) => v,
                        };
                        Type::Variant(tag.clone(), Arc::from_iter(t))
                    },
                ),
        ),
        attempt(fntype().map(|f| Type::Fn(Arc::new(f)))),
        attempt(spstring("Array").with(between(sptoken('<'), sptoken('>'), typexp())))
            .map(|t| Type::Array(Arc::new(t))),
        attempt(sptypath()).map(|n| Type::Ref(n)),
        attempt(typeprim()).map(|typ| Type::Primitive(typ.into())),
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

fn lambda_args<I>(
) -> impl Parser<I, Output = (Vec<Arg<Refs>>, Option<Option<Type<Refs>>>)>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
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
    .then(|mut v: Vec<Arg<Refs>>| {
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
    .then(|(v, vargs): (Vec<Arg<Refs>>, Option<Option<Type<Refs>>>)| {
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
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        attempt(sep_by((tvar().skip(sptoken(':')), typexp()), csep()))
            .map(|tvs: SmallVec<[(TVar<Refs>, Type<Refs>); 4]>| Arc::from_iter(tvs)),
        between(sptoken('|'), sptoken('|'), lambda_args()),
        optional(attempt(spstring("->").with(typexp()).skip(space()))),
        choice((
            attempt(sptoken('\'').with(fname()).skip(not_followed_by(sptoken(':'))))
                .map(Either::Right),
            expr().map(|e| Either::Left(e)),
        )),
    )
        .map(|(pos, constraints, (args, vargs), rtype, body)| {
            let args = Arc::from_iter(args);
            ExprKind::Lambda(Arc::new(Lambda { args, vargs, rtype, constraints, body }))
                .to_expr(pos)
        })
}

fn letbind<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        doc_comment(),
        optional(string("pub").skip(space())).map(|o| o.is_some()),
        spstring("let")
            .with(space())
            .with((structure_pattern(), optional(attempt(sptoken(':').with(typexp())))))
            .skip(spstring("=")),
        expr(),
    )
        .map(|(pos, doc, export, (pattern, typ), value)| {
            ExprKind::Bind(Arc::new(Bind { doc, export, pattern, typ, value }))
                .to_expr(pos)
        })
}

fn connect<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (position(), modpath().skip(spstring("<-")), expr())
        .map(|(pos, name, e)| ExprKind::Connect { name, value: Arc::new(e) }.to_expr(pos))
}

fn literal<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (position(), netidx_value(&BSCRIPT_ESC).skip(not_followed_by(token('_'))))
        .map(|(pos, v)| ExprKind::Constant(v).to_expr(pos))
}

fn reference<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (position(), modpath()).map(|(pos, name)| ExprKind::Ref { name }.to_expr(pos))
}

fn qop<I, P: Parser<I, Output = Expr>>(p: P) -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (position(), p, optional(attempt(sptoken('?')))).map(|(pos, e, qop)| match qop {
        None => e,
        Some(_) => ExprKind::Qop(Arc::new(e)).to_expr(pos),
    })
}

fn arith_term<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spaces().with(raw_string())),
        attempt(spaces().with(array())),
        attempt(spaces().with(qop(arrayref()))),
        attempt(spaces().with(qop(tupleref()))),
        attempt(spaces().with(qop(structref()))),
        attempt(spaces().with(qop(apply()))),
        attempt(spaces().with(qop(do_block()))),
        attempt(spaces().with(qop(select()))),
        attempt(spaces().with(qop(cast()))),
        attempt(spaces().with(qop(any()))),
        attempt(spaces().with(interpolated())),
        attempt(spaces().with(literal())),
        attempt(spaces().with(qop(reference()))),
        attempt(
            (position(), sptoken('!').with(arith()))
                .map(|(pos, expr)| ExprKind::Not { expr: Arc::new(expr) }.to_expr(pos)),
        ),
        attempt(between(sptoken('('), sptoken(')'), arith())),
    ))
    .skip(spaces())
}

fn arith_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
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
                "+" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Add { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                "-" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Sub { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                "*" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Mul { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                "/" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Div { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                "==" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Eq { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                "!=" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Ne { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                ">" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Gt { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                "<" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Lt { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                ">=" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Gte { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                "<=" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Lte { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                "&&" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::And { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                "||" => |lhs: Expr, rhs: Expr| {
                    let pos = lhs.pos;
                    ExprKind::Or { lhs: Arc::new(lhs), rhs: Arc::new(rhs) }.to_expr(pos)
                },
                _ => unreachable!(),
            }),
        )),
        attempt((position(), sptoken('!').with(arith_term())))
            .map(|(pos, expr)| ExprKind::Not { expr: Arc::new(expr) }.to_expr(pos)),
        attempt(between(sptoken('('), sptoken(')'), arith())),
    ))
}

parser! {
    fn arith[I]()(I) -> Expr
    where [I: RangeStream<Token = char, Position = SourcePosition>, I::Range: Range]
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

fn raw_string<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    const ESC: [char; 2] = ['\'', '\\'];
    (position(), between(string("r\'"), token('\''), escaped_string(&ESC))).map(
        |(pos, s): (_, String)| ExprKind::Constant(Value::String(s.into())).to_expr(pos),
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

fn variant_pattern<I>() -> impl Parser<I, Output = StructurePattern>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        optional(attempt(spfname().skip(sptoken('@')))),
        sptoken('`').with(typname()),
        optional(attempt(between(
            sptoken('('),
            sptoken(')'),
            sep_by1(structure_pattern(), csep()),
        ))),
    )
        .map(
            |(all, tag, binds): (
                Option<ArcStr>,
                ArcStr,
                Option<SmallVec<[StructurePattern; 8]>>,
            )| {
                let binds = match binds {
                    None => smallvec![],
                    Some(a) => a,
                };
                StructurePattern::Variant { all, tag, binds: Arc::from_iter(binds) }
            },
        )
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
        attempt(variant_pattern()),
        attempt(netidx_value(&VAL_ESC).skip(not_followed_by(token('_'))))
            .map(|v| StructurePattern::Literal(v)),
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
    I: RangeStream<Token = char, Position = SourcePosition>,
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
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        string("select").with(space()).with((
            expr(),
            between(
                sptoken('{'),
                sptoken('}'),
                sep_by1((pattern(), spstring("=>").with(expr())), csep()),
            ),
        )),
    )
        .map(|(pos, (arg, arms)): (_, (Expr, Vec<(Pattern, Expr)>))| {
            ExprKind::Select { arg: Arc::new(arg), arms: Arc::from(arms) }.to_expr(pos)
        })
}

fn cast<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        string("cast").with(between(token('<'), sptoken('>'), typexp())),
        between(sptoken('('), sptoken(')'), expr()),
    )
        .map(|(pos, typ, e)| ExprKind::TypeCast { expr: Arc::new(e), typ }.to_expr(pos))
}

fn typedef<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (position(), string("type").with(sptypname()), sptoken('=').with(typexp()))
        .map(|(pos, name, typ)| ExprKind::TypeDef { name, typ }.to_expr(pos))
}

fn tuple<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (position(), between(token('('), sptoken(')'), sep_by1(expr(), csep()))).then(
        |(pos, exprs): (_, SmallVec<[Expr; 8]>)| {
            if exprs.len() < 2 {
                unexpected_any("tuples must have at least 2 elements").left()
            } else {
                value(ExprKind::Tuple { args: Arc::from_iter(exprs) }.to_expr(pos))
                    .right()
            }
        },
    )
}

fn structure<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        between(
            token('{'),
            sptoken('}'),
            sep_by1((spfname().skip(sptoken(':')), expr()), csep()),
        ),
    )
        .then(|(pos, mut exprs): (_, SmallVec<[(ArcStr, Expr); 8]>)| {
            let s = exprs.iter().map(|(n, _)| n).collect::<FxHashSet<_>>();
            if s.len() < exprs.len() {
                return unexpected_any("struct fields must be unique").left();
            }
            exprs.sort_by_key(|(n, _)| n.clone());
            value(ExprKind::Struct { args: Arc::from_iter(exprs) }.to_expr(pos)).right()
        })
}

fn variant<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        token('`').with(typname()),
        optional(attempt(between(token('('), sptoken(')'), sep_by1(expr(), csep())))),
    )
        .map(|(pos, tag, args): (_, ArcStr, Option<SmallVec<[Expr; 5]>>)| {
            let args = match args {
                None => smallvec![],
                Some(a) => a,
            };
            ExprKind::Variant { tag, args: Arc::from_iter(args.into_iter()) }.to_expr(pos)
        })
}

fn structwith<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    (
        position(),
        between(
            token('{'),
            sptoken('}'),
            (
                ref_pexp().skip(space()).skip(spstring("with")).skip(space()),
                sep_by1((spfname().skip(sptoken(':')), expr()), csep()),
            ),
        ),
    )
        .then(
            |(pos, (source, mut exprs)): (_, (Expr, SmallVec<[(ArcStr, Expr); 8]>))| {
                let s = exprs.iter().map(|(n, _)| n).collect::<FxHashSet<_>>();
                if s.len() < exprs.len() {
                    return unexpected_any("struct fields must be unique").left();
                }
                exprs.sort_by_key(|(n, _)| n.clone());
                let e = ExprKind::StructWith {
                    source: Arc::new(source),
                    replace: Arc::from_iter(exprs),
                }
                .to_expr(pos);
                value(e).right()
            },
        )
}

fn expr_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spaces().with(raw_string())),
        attempt(spaces().with(array())),
        attempt(spaces().with(arith())),
        attempt(spaces().with(tuple())),
        attempt(spaces().with(structure())),
        attempt(spaces().with(variant())),
        attempt(spaces().with(structwith())),
        attempt(spaces().with(qop(arrayref()))),
        attempt(spaces().with(qop(tupleref()))),
        attempt(spaces().with(qop(structref()))),
        attempt(spaces().with(qop(apply()))),
        attempt(spaces().with(qop(do_block()))),
        attempt(spaces().with(lambda())),
        attempt(spaces().with(letbind())),
        attempt(spaces().with(connect())),
        attempt(spaces().with(qop(select()))),
        attempt(spaces().with(qop(cast()))),
        attempt(spaces().with(qop(any()))),
        attempt(spaces().with(interpolated())),
        attempt(spaces().with(literal())),
        attempt(spaces().with(qop(reference()))),
    ))
}

parser! {
    fn expr[I]()(I) -> Expr
    where [I: RangeStream<Token = char, Position = SourcePosition>, I::Range: Range]
    {
        expr_()
    }
}

fn modexpr_<I>() -> impl Parser<I, Output = Expr>
where
    I: RangeStream<Token = char, Position = SourcePosition>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(spaces().with(qop(apply()))),
        attempt(spaces().with(qop(do_block()))),
        attempt(spaces().with(module())),
        attempt(spaces().with(use_module())),
        attempt(spaces().with(typedef())),
        attempt(spaces().with(letbind())),
        attempt(spaces().with(connect())),
        attempt(spaces().with(qop(any()))),
        attempt(spaces().with(interpolated())),
    ))
}

parser! {
    fn modexpr[I]()(I) -> Expr
    where [I: RangeStream<Token = char, Position = SourcePosition>, I::Range: Range]
    {
        modexpr_()
    }
}

/// Parse one or more toplevel module expressions
///
/// followed by (optional) whitespace and then eof. At least one
/// expression is required otherwise this function will fail.
pub fn parse(name: Option<ArcStr>, s: ArcStr) -> anyhow::Result<Origin> {
    let r: Vec<Expr> = many1(modexpr())
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(&*s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))?;
    Ok(Origin { name, source: s, exprs: Arc::from(r) })
}

/// Parse a fntype
pub fn parse_fn_type(s: &str) -> anyhow::Result<FnType<Refs>> {
    fntype()
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{e}")))
}

/// Parse expressions instead of module expressions
///
/// followed by (optional) whitespace and then eof. At least one
/// expression is required or this function will fail.
pub fn parse_expr(name: Option<ArcStr>, s: ArcStr) -> anyhow::Result<Origin> {
    let r: Vec<Expr> = many1(expr())
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(&*s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{e}")))?;
    Ok(Origin { name, source: s, exprs: Arc::from(r) })
}

/// Parse one and only one expression. Do not wrap it in an origin.
pub fn parse_one_expr(s: &str) -> anyhow::Result<Expr> {
    expr()
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(&*s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{e}")))
}

/// Parse one and only one module expression. Do not wrap it in an origin.
pub fn parse_one_modexpr(s: &str) -> anyhow::Result<Expr> {
    modexpr()
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(&*s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{e}")))
}
