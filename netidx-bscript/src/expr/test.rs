use super::*;
use crate::typ::{FnArgType, FnType, Refs, Type};
use bytes::Bytes;
use chrono::prelude::*;
use enumflags2::BitFlags;
use netidx::protocol::value::Typ;
use netidx_netproto::pbuf::PBytes;
use parser::{parse, RESERVED};
use prop::option;
use proptest::{collection, prelude::*};
use smallvec::SmallVec;
use std::{marker::PhantomData, time::Duration};

const SLEN: usize = 16;

fn datetime() -> impl Strategy<Value = DateTime<Utc>> {
    (
        DateTime::<Utc>::MIN_UTC.timestamp()..DateTime::<Utc>::MAX_UTC.timestamp(),
        0..1_000_000_000u32,
    )
        .prop_map(|(s, ns)| Utc.timestamp_opt(s, ns).unwrap())
}

fn duration() -> impl Strategy<Value = Duration> {
    (any::<u64>(), 0..1_000_000_000u32).prop_map(|(s, ns)| Duration::new(s, ns))
}

fn pbytes() -> impl Strategy<Value = PBytes> {
    any::<Vec<u8>>().prop_map(|b| PBytes::from(Bytes::from(b)))
}

fn arcstr() -> impl Strategy<Value = ArcStr> {
    any::<String>().prop_map(ArcStr::from)
}

fn value() -> impl Strategy<Value = Value> {
    prop_oneof![
        any::<u32>().prop_map(Value::U32),
        any::<u32>().prop_map(Value::V32),
        any::<i32>().prop_map(Value::I32),
        any::<i32>().prop_map(Value::Z32),
        any::<u64>().prop_map(Value::U64),
        any::<u64>().prop_map(Value::V64),
        any::<i64>().prop_map(Value::I64),
        any::<i64>().prop_map(Value::Z64),
        any::<f32>().prop_map(Value::F32),
        any::<f64>().prop_map(Value::F64),
        datetime().prop_map(Value::DateTime),
        duration().prop_map(Value::Duration),
        arcstr().prop_map(Value::String),
        pbytes().prop_map(Value::Bytes),
        Just(Value::Bool(true)),
        Just(Value::Bool(false)),
        Just(Value::Null),
        arcstr().prop_map(Value::Error),
    ]
}

fn random_modpart() -> impl Strategy<Value = String> {
    collection::vec(prop_oneof![Just(b'_'), b'a'..=b'z', b'0'..=b'9'], 1..=SLEN - 1)
        .prop_map(|mut v| unsafe {
            if v[0] == b'_' {
                v[0] = b'a';
            }
            if v[0] >= b'0' && v[0] <= b'9' {
                v[0] += 49;
            }
            String::from_utf8_unchecked(v)
        })
        .prop_filter("Filter reserved words", |s| !RESERVED.contains(s.as_str()))
}

fn typart() -> impl Strategy<Value = ArcStr> {
    collection::vec(prop_oneof![Just(b'_'), b'a'..=b'z', b'0'..=b'9'], 1..=SLEN - 1)
        .prop_map(|mut v| unsafe {
            if v[0] == b'_' {
                v[0] = b'A';
            }
            if v[0] >= b'0' && v[0] <= b'9' {
                v[0] += 17;
            }
            if v[0] >= 97 {
                v[0] -= 32;
            }
            ArcStr::from(String::from_utf8_unchecked(v))
        })
        .prop_filter("Filter reserved words", |s| !RESERVED.contains(s.as_str()))
}

fn valid_fname() -> impl Strategy<Value = ArcStr> {
    prop_oneof![
        Just(ArcStr::from("all")),
        Just(ArcStr::from("sum")),
        Just(ArcStr::from("product")),
        Just(ArcStr::from("divide")),
        Just(ArcStr::from("mean")),
        Just(ArcStr::from("min")),
        Just(ArcStr::from("max")),
        Just(ArcStr::from("and")),
        Just(ArcStr::from("or")),
        Just(ArcStr::from("not")),
        Just(ArcStr::from("cmp")),
        Just(ArcStr::from("filter")),
        Just(ArcStr::from("isa")),
        Just(ArcStr::from("eval")),
        Just(ArcStr::from("count")),
        Just(ArcStr::from("sample")),
        Just(ArcStr::from("join")),
        Just(ArcStr::from("concat")),
        Just(ArcStr::from("navigate")),
        Just(ArcStr::from("confirm")),
        Just(ArcStr::from("load")),
        Just(ArcStr::from("get")),
        Just(ArcStr::from("store")),
        Just(ArcStr::from("set")),
    ]
}

fn random_fname() -> impl Strategy<Value = ArcStr> {
    prop_oneof![random_modpart().prop_map(ArcStr::from), valid_fname()]
}

fn random_modpath() -> impl Strategy<Value = ModPath> {
    collection::vec(random_modpart(), (1, 5)).prop_map(ModPath::from_iter)
}

fn typath() -> impl Strategy<Value = ModPath> {
    (collection::vec(random_modpart(), (0, 4)), typart())
        .prop_map(|(path, typ)| ModPath(ModPath::from_iter(path).0.append(typ.as_str())))
}

fn valid_modpath() -> impl Strategy<Value = ModPath> {
    prop_oneof![
        Just(ModPath::from_iter(["all"])),
        Just(ModPath::from_iter(["sum"])),
        Just(ModPath::from_iter(["product"])),
        Just(ModPath::from_iter(["divide"])),
        Just(ModPath::from_iter(["mean"])),
        Just(ModPath::from_iter(["min"])),
        Just(ModPath::from_iter(["max"])),
        Just(ModPath::from_iter(["and"])),
        Just(ModPath::from_iter(["or"])),
        Just(ModPath::from_iter(["not"])),
        Just(ModPath::from_iter(["cmp"])),
        Just(ModPath::from_iter(["filter"])),
        Just(ModPath::from_iter(["isa"])),
        Just(ModPath::from_iter(["eval"])),
        Just(ModPath::from_iter(["count"])),
        Just(ModPath::from_iter(["sample"])),
        Just(ModPath::from_iter(["str", "join"])),
        Just(ModPath::from_iter(["str", "concat"])),
        Just(ModPath::from_iter(["navigate"])),
        Just(ModPath::from_iter(["confirm"])),
        Just(ModPath::from_iter(["load"])),
        Just(ModPath::from_iter(["get"])),
        Just(ModPath::from_iter(["store"])),
        Just(ModPath::from_iter(["set"])),
    ]
}

fn modpath() -> impl Strategy<Value = ModPath> {
    prop_oneof![random_modpath(), valid_modpath()]
}

fn constant() -> impl Strategy<Value = Expr> {
    value().prop_map(|v| ExprKind::Constant(v).to_expr())
}

fn reference() -> impl Strategy<Value = Expr> {
    modpath().prop_map(|name| ExprKind::Ref { name }.to_expr())
}

fn typ() -> impl Strategy<Value = Typ> {
    prop_oneof![
        Just(Typ::U32),
        Just(Typ::V32),
        Just(Typ::I32),
        Just(Typ::Z32),
        Just(Typ::U64),
        Just(Typ::V64),
        Just(Typ::I64),
        Just(Typ::Z64),
        Just(Typ::F32),
        Just(Typ::F64),
        Just(Typ::Decimal),
        Just(Typ::DateTime),
        Just(Typ::Duration),
        Just(Typ::Bool),
        Just(Typ::String),
        Just(Typ::Bytes),
        Just(Typ::Error),
        Just(Typ::Array),
        Just(Typ::Null),
    ]
}

fn typexp() -> impl Strategy<Value = Type<Refs>> {
    let leaf = prop_oneof![
        Just(Type::Bottom(PhantomData)),
        collection::vec(typ(), (0, 10)).prop_map(|mut prims| {
            prims.sort();
            prims.dedup();
            Type::Primitive(BitFlags::from_iter(prims))
        }),
        typath().prop_map(Type::Ref),
    ];
    leaf.prop_recursive(5, 25, 5, |inner| {
        prop_oneof![
            collection::vec(inner.clone(), (2, 20)).prop_map(|t| Type::Set(Arc::from(t))),
            collection::vec(inner.clone(), (2, 20))
                .prop_map(|t| Type::Tuple(Arc::from(t))),
            (typart(), collection::vec(inner.clone(), (0, 20)))
                .prop_map(|(tag, typs)| Type::Variant(tag, Arc::from_iter(typs))),
            collection::vec((random_fname(), inner.clone()), (1, 20)).prop_map(
                |mut t| {
                    t.sort_by_key(|(n, _)| n.clone());
                    t.dedup_by_key(|(n, _)| n.clone());
                    Type::Struct(Arc::from(t))
                }
            ),
            inner.clone().prop_map(|t| Type::Array(Arc::new(t))),
            random_fname().prop_map(|a| Type::TVar(TVar::empty_named(a))),
            (
                collection::vec(
                    (option::of(random_fname()), any::<bool>(), inner.clone()),
                    (1, 10)
                ),
                option::of(inner.clone()),
                inner.clone(),
                collection::vec((random_fname(), inner.clone()), (0, 4))
            )
                .prop_map(|(mut args, vargs, rtype, constraints)| {
                    args.sort_by(|(k0, _, _), (k1, _, _)| k1.cmp(k0));
                    let args = args.into_iter().map(|(name, optional, typ)| FnArgType {
                        label: name.map(|n| (n, optional)),
                        typ,
                    });
                    Type::Fn(Arc::new(FnType {
                        args: Arc::from_iter(args),
                        vargs,
                        rtype,
                        constraints: Arc::from_iter(
                            constraints
                                .into_iter()
                                .map(|(a, t)| (TVar::empty_named(a), t)),
                        ),
                    }))
                })
        ]
    })
}

fn structure_pattern() -> impl Strategy<Value = StructurePattern> {
    let leaf = prop_oneof![
        value().prop_map(|v| StructurePattern::Literal(v)),
        option::of(random_fname()).prop_map(|name| match name {
            None => StructurePattern::Ignore,
            Some(name) => StructurePattern::Bind(name),
        }),
    ];
    leaf.prop_recursive(5, 10, 20, |inner| {
        prop_oneof![
            (option::of(random_fname()), collection::vec(inner.clone(), (0, 10)))
                .prop_map(|(all, b)| {
                    StructurePattern::Slice { all, binds: Arc::from_iter(b) }
                }),
            (option::of(random_fname()), collection::vec(inner.clone(), (2, 10)))
                .prop_map(|(all, b)| {
                    StructurePattern::Tuple { all, binds: Arc::from_iter(b) }
                }),
            (
                option::of(random_fname()),
                typart(),
                collection::vec(inner.clone(), (0, 10))
            )
                .prop_map(|(all, tag, b)| {
                    StructurePattern::Variant { all, tag, binds: Arc::from_iter(b) }
                }),
            (
                option::of(random_fname()),
                collection::vec((random_fname(), inner.clone()), (1, 10)),
                any::<bool>()
            )
                .prop_map(|(all, mut b, exhaustive)| {
                    b.sort_by_key(|(f, _)| f.clone());
                    b.dedup_by_key(|(f, _)| f.clone());
                    StructurePattern::Struct { all, exhaustive, binds: Arc::from_iter(b) }
                }),
            (
                option::of(random_fname()),
                collection::vec(inner.clone(), (1, 10)),
                option::of(random_fname())
            )
                .prop_map(|(all, p, tail)| StructurePattern::SlicePrefix {
                    all,
                    prefix: Arc::from_iter(p),
                    tail
                }),
            (
                option::of(random_fname()),
                option::of(random_fname()),
                collection::vec(inner.clone(), (1, 10))
            )
                .prop_map(|(all, head, s)| StructurePattern::SliceSuffix {
                    all,
                    head,
                    suffix: Arc::from_iter(s)
                }),
        ]
    })
}

fn pattern() -> impl Strategy<Value = Pattern> {
    (option::of(typexp()), structure_pattern()).prop_map(
        |(type_predicate, structure_predicate)| Pattern {
            type_predicate,
            structure_predicate,
            guard: None,
        },
    )
}

fn build_pattern(arg: Expr, arms: Vec<(Option<Expr>, Pattern, Expr)>) -> Expr {
    let arms = arms.into_iter().map(|(guard, mut pat, expr)| {
        pat.guard = guard;
        (pat, expr)
    });
    ExprKind::Select { arg: Arc::new(arg), arms: Arc::from_iter(arms) }.to_expr()
}

fn usestmt() -> impl Strategy<Value = Expr> {
    modpath().prop_map(|name| ExprKind::Use { name }.to_expr())
}

fn typedef() -> impl Strategy<Value = Expr> {
    (typart(), typexp()).prop_map(|(name, typ)| ExprKind::TypeDef { name, typ }.to_expr())
}

fn structref() -> impl Strategy<Value = Expr> {
    (modpath(), random_fname())
        .prop_map(|(name, field)| ExprKind::StructRef { name, field }.to_expr())
}

fn tupleref() -> impl Strategy<Value = Expr> {
    (modpath(), any::<usize>())
        .prop_map(|(name, field)| ExprKind::TupleRef { name, field }.to_expr())
}

macro_rules! bind {
    ($inner:expr) => {
        ($inner, structure_pattern(), any::<bool>(), option::of(typexp())).prop_map(
            |(e, p, exp, typ)| {
                ExprKind::Bind { export: exp, pattern: p, value: Arc::new(e), typ }
                    .to_expr()
            },
        )
    };
}

macro_rules! qop {
    ($inner:expr) => {
        $inner.prop_map(|e| match &e.kind {
            ExprKind::Do { .. }
            | ExprKind::Select { .. }
            | ExprKind::TypeCast { .. }
            | ExprKind::Ref { .. } => ExprKind::Apply {
                function: ["op", "question"].into(),
                args: Arc::from_iter([(None, e)]),
            }
            .to_expr(),
            ExprKind::Apply { function, .. }
                if function != &["op", "question"] && function != &["str", "concat"] =>
            {
                ExprKind::Apply {
                    function: ["op", "question"].into(),
                    args: Arc::from_iter([(None, e)]),
                }
                .to_expr()
            }
            _ => e,
        })
    };
}

macro_rules! slice {
    ($inner:expr) => {
        (modpath(), option::of($inner), option::of($inner)).prop_map(
            |(name, start, end)| {
                let a = ExprKind::Ref { name }.to_expr();
                let start = start.unwrap_or(ExprKind::Constant(Value::Null).to_expr());
                let end = end.unwrap_or(ExprKind::Constant(Value::Null).to_expr());
                ExprKind::Apply {
                    function: ["op", "slice"].into(),
                    args: Arc::from_iter([(None, a), (None, start), (None, end)]),
                }
                .to_expr()
            },
        )
    };
}

macro_rules! apply {
    ($inner:expr, $concat:literal) => {
        (collection::vec((option::of(random_fname()), $inner), (0, 10)), modpath())
            .prop_map(|(mut s, f)| {
                let f = if $concat && f == ModPath::from(["str", "concat"]) {
                    ["str", "concat1"].into() // str::concat is illegal at the module level
                } else {
                    f
                };
                s.sort_unstable_by(|(n0, _), (n1, _)| n1.cmp(n0));
                ExprKind::Apply { function: f, args: Arc::from(s) }.to_expr()
            })
    };
}

macro_rules! any {
    ($inner:expr) => {
        collection::vec($inner, (0, 10))
            .prop_map(|args| ExprKind::Any { args: Arc::from(args) }.to_expr())
    };
}

macro_rules! do_block {
    ($inner:expr) => {
        collection::vec(prop_oneof![typedef(), usestmt(), $inner], (1, 10))
            .prop_map(|e| ExprKind::Do { exprs: Arc::from(e) }.to_expr())
    };
}

macro_rules! lambda {
    ($inner:expr) => {
        (
            collection::vec(
                (
                    any::<bool>(),
                    random_fname(),
                    structure_pattern(),
                    option::of(typexp()),
                    option::of($inner),
                ),
                (0, 10),
            ),
            option::of(option::of(typexp())),
            option::of(typexp()),
            collection::vec((random_fname(), typexp()), (0, 4)),
            option::of(random_fname()),
            $inner,
        )
            .prop_map(|(mut args, vargs, rtype, constraints, builtin, body)| {
                args.sort_unstable_by(|(k0, _, _, _, _), (k1, _, _, _, _)| k1.cmp(k0));
                let args = args.into_iter().map(
                    |(labeled, name, pattern, constraint, default)| {
                        let pattern =
                            if labeled { StructurePattern::Bind(name) } else { pattern };
                        Arg { labeled: labeled.then_some(default), pattern, constraint }
                    },
                );
                let constraints = Arc::from_iter(
                    constraints.into_iter().map(|(a, t)| (TVar::empty_named(a), t)),
                );
                ExprKind::Lambda {
                    args: Arc::from_iter(args),
                    vargs,
                    rtype,
                    constraints,
                    body: match builtin {
                        None => Either::Left(Arc::new(body)),
                        Some(name) => Either::Right(name),
                    },
                }
                .to_expr()
            })
    };
}

macro_rules! select {
    ($inner:expr) => {
        ($inner, collection::vec((option::of($inner), pattern(), $inner), (1, 10)))
            .prop_map(|(arg, arms)| build_pattern(arg, arms))
    };
}

macro_rules! structure {
    ($inner:expr) => {
        collection::vec((random_fname(), $inner), (1, 10)).prop_map(|mut a| {
            a.sort_by_key(|(n, _)| n.clone());
            a.dedup_by_key(|(n, _)| n.clone());
            ExprKind::Struct { args: Arc::from_iter(a) }.to_expr()
        })
    };
}

macro_rules! variant {
    ($inner:expr) => {
        (typart(), collection::vec($inner, (0, 10))).prop_map(|(tag, a)| {
            ExprKind::Variant { tag, args: Arc::from_iter(a) }.to_expr()
        })
    };
}

macro_rules! connect {
    ($inner:expr) => {
        ($inner, modpath()).prop_map(|(e, n)| {
            ExprKind::Connect { name: n, value: Arc::new(e) }.to_expr()
        })
    };
}

macro_rules! arrayidx {
    ($inner:expr) => {
        (modpath(), $inner).prop_map(|(name, e)| {
            let a = ExprKind::Ref { name }.to_expr();
            ExprKind::Apply {
                function: ["op", "index"].into(),
                args: Arc::from_iter([(None, a), (None, e)]),
            }
            .to_expr()
        })
    };
}

macro_rules! typecast {
    ($inner:expr) => {
        ($inner, typexp()).prop_map(|(expr, typ)| {
            ExprKind::TypeCast { expr: Arc::new(expr), typ }.to_expr()
        })
    };
}

macro_rules! array {
    ($inner:expr) => {
        collection::vec($inner, (0, 10))
            .prop_map(|a| { ExprKind::Array { args: Arc::from_iter(a) } }.to_expr())
    };
}

macro_rules! tuple {
    ($inner:expr) => {
        collection::vec($inner, (2, 10))
            .prop_map(|a| { ExprKind::Tuple { args: Arc::from_iter(a) } }.to_expr())
    };
}

macro_rules! binop {
    ($inner:expr, $op:ident) => {
        ($inner, $inner).prop_map(|(e0, e1)| {
            ExprKind::$op { lhs: Arc::new(e0), rhs: Arc::new(e1) }.to_expr()
        })
    };
}

macro_rules! structwith {
    ($inner:expr) => {
        (modpath(), collection::vec((random_fname(), $inner), (1, 10))).prop_map(
            |(name, mut replace)| {
                replace.sort_by_key(|(f, _)| f.clone());
                replace.dedup_by_key(|(f, _)| f.clone());
                ExprKind::StructWith { name, replace: Arc::from_iter(replace) }.to_expr()
            },
        )
    };
}

fn arithexpr() -> impl Strategy<Value = Expr> {
    let leaf = prop_oneof![constant(), reference(), structref(), tupleref()];
    leaf.prop_recursive(5, 25, 5, |inner| {
        prop_oneof![
            select!(inner.clone()),
            do_block!(inner.clone()),
            any!(inner.clone()),
            apply!(inner.clone(), false),
            typecast!(inner.clone()),
            arrayidx!(inner.clone()),
            binop!(inner.clone(), Eq),
            binop!(inner.clone(), Ne),
            binop!(inner.clone(), Lt),
            binop!(inner.clone(), Gt),
            binop!(inner.clone(), Gte),
            binop!(inner.clone(), Lte),
            binop!(inner.clone(), And),
            binop!(inner.clone(), Or),
            inner.clone().prop_map(|e0| ExprKind::Not { expr: Arc::new(e0) }.to_expr()),
            binop!(inner.clone(), Add),
            binop!(inner.clone(), Sub),
            binop!(inner.clone(), Mul),
            binop!(inner.clone(), Div),
        ]
    })
}

fn expr() -> impl Strategy<Value = Expr> {
    let leaf = prop_oneof![constant(), reference(), structref(), tupleref()];
    leaf.prop_recursive(10, 100, 10, |inner| {
        prop_oneof![
            arrayidx!(inner.clone()),
            qop!(inner.clone()),
            slice!(inner.clone()),
            arithexpr(),
            any!(inner.clone()),
            apply!(inner.clone(), false),
            typecast!(inner.clone()),
            do_block!(inner.clone()),
            lambda!(inner.clone()),
            bind!(inner.clone()),
            connect!(inner.clone()),
            select!(inner.clone()),
            array!(inner.clone()),
            tuple!(inner.clone()),
            variant!(inner.clone()),
            structure!(inner.clone()),
            structwith!(inner.clone()),
        ]
    })
}

fn modexpr() -> impl Strategy<Value = Expr> {
    let leaf = prop_oneof![
        any!(expr()),
        apply!(expr(), true),
        do_block!(expr()),
        usestmt(),
        typedef(),
        bind!(expr()),
        connect!(expr()),
    ];
    leaf.prop_recursive(10, 100, 10, |inner| {
        prop_oneof![
            (any::<bool>(), random_fname(), collection::vec(inner.clone(), (0, 10)))
                .prop_map(|(export, name, body)| ExprKind::Module {
                    export,
                    name,
                    value: Some(Arc::from(body))
                }
                .to_expr()),
            (any::<bool>(), random_fname()).prop_map(|(export, name)| ExprKind::Module {
                export,
                name,
                value: None
            }
            .to_expr()),
        ]
    })
}

fn acc_strings<'a>(args: impl IntoIterator<Item = &'a Expr> + 'a) -> Arc<[Expr]> {
    let mut v: Vec<Expr> = Vec::new();
    for s in args {
        let s = s.clone();
        match s.kind {
            ExprKind::Constant(Value::String(ref c1)) => match v.last_mut() {
                None => v.push(s),
                Some(e0) => match &mut e0.kind {
                    ExprKind::Constant(Value::String(c0))
                        if c1.len() > 0 && c0.len() > 0 =>
                    {
                        let mut st = String::new();
                        st.push_str(&*c0);
                        st.push_str(&*c1);
                        *c0 = ArcStr::from(st);
                    }
                    _ => v.push(s),
                },
            },
            _ => v.push(s),
        }
    }
    Arc::from(v)
}

fn check_type(t0: &Type<Refs>, t1: &Type<Refs>) -> bool {
    t0.normalize() == t1.normalize()
}

fn check_type_opt(t0: &Option<Type<Refs>>, t1: &Option<Type<Refs>>) -> bool {
    match (t0, t1) {
        (Some(t0), Some(t1)) => check_type(&t0, &t1),
        (None, None) => true,
        (_, _) => false,
    }
}

fn check_structure_pattern(pat0: &StructurePattern, pat1: &StructurePattern) -> bool {
    match (pat0, pat1) {
        (
            StructurePattern::Literal(Value::Array(a)),
            StructurePattern::Slice { all: None, binds },
        )
        | (
            StructurePattern::Slice { all: None, binds },
            StructurePattern::Literal(Value::Array(a)),
        ) => {
            binds.iter().all(|n| match n {
                StructurePattern::Literal(_) => true,
                _ => false,
            }) && {
                let binds = binds
                    .iter()
                    .filter_map(|n| match n {
                        StructurePattern::Literal(l) => Some(l),
                        _ => None,
                    })
                    .collect::<SmallVec<[&Value; 16]>>();
                binds.len() == a.len()
                    && binds.iter().zip(a.iter()).all(|(v0, v1)| *v0 == v1)
            }
        }
        (StructurePattern::Bind(n0), StructurePattern::Bind(n1)) => n0 == n1,
        (StructurePattern::Ignore, StructurePattern::Ignore) => true,
        (StructurePattern::Literal(v0), StructurePattern::Literal(v1)) => {
            v0.approx_eq(v1)
        }
        (
            StructurePattern::Slice { all: a0, binds: p0 },
            StructurePattern::Slice { all: a1, binds: p1 },
        ) => {
            a0 == a1
                && p0.len() == p1.len()
                && p0
                    .iter()
                    .zip(p1.iter())
                    .all(|(p0, p1)| check_structure_pattern(p0, p1))
        }
        (
            StructurePattern::SlicePrefix { all: a0, prefix: p0, tail: t0 },
            StructurePattern::SlicePrefix { all: a1, prefix: p1, tail: t1 },
        ) => {
            a0 == a1
                && t0 == t1
                && p0.len() == p1.len()
                && p0
                    .iter()
                    .zip(p1.iter())
                    .all(|(p0, p1)| check_structure_pattern(p0, p1))
        }
        (
            StructurePattern::SliceSuffix { all: a0, head: h0, suffix: p0 },
            StructurePattern::SliceSuffix { all: a1, head: h1, suffix: p1 },
        ) => {
            a0 == a1
                && h0 == h1
                && p0.len() == p1.len()
                && p0
                    .iter()
                    .zip(p1.iter())
                    .all(|(p0, p1)| check_structure_pattern(p0, p1))
        }
        (
            StructurePattern::Tuple { all: a0, binds: p0 },
            StructurePattern::Tuple { all: a1, binds: p1 },
        ) => {
            a0 == a1
                && p0.len() == p1.len()
                && p0
                    .iter()
                    .zip(p1.iter())
                    .all(|(p0, p1)| check_structure_pattern(p0, p1))
        }
        (
            StructurePattern::Variant { all: a0, tag: t0, binds: p0 },
            StructurePattern::Variant { all: a1, tag: t1, binds: p1 },
        ) => {
            a0 == a1
                && t0 == t1
                && p0.len() == p1.len()
                && p0
                    .iter()
                    .zip(p1.iter())
                    .all(|(p0, p1)| check_structure_pattern(p0, p1))
        }
        (
            StructurePattern::Struct { exhaustive: e0, all: a0, binds: p0 },
            StructurePattern::Struct { exhaustive: e1, all: a1, binds: p1 },
        ) => {
            e0 == e1
                && a0 == a1
                && p0.len() == p1.len()
                && p0.iter().zip(p1.iter()).all(|((f0, p0), (f1, p1))| {
                    f0 == f1 && check_structure_pattern(p0, p1)
                })
        }
        (_, _) => false,
    }
}

fn check_pattern(pat0: &Pattern, pat1: &Pattern) -> bool {
    dbg!(check_type_opt(&pat0.type_predicate, &pat1.type_predicate))
        && check_structure_pattern(&pat0.structure_predicate, &pat1.structure_predicate)
        && dbg!(match (&pat0.guard, &pat1.guard) {
            (Some(g0), Some(g1)) => check(g0, g1),
            (None, None) => true,
            (_, _) => false,
        })
}

fn check_args(args0: &[Arg<Refs>], args1: &[Arg<Refs>]) -> bool {
    args0.iter().zip(args1.iter()).fold(true, |r, (a0, a1)| {
        r && dbg!(check_structure_pattern(&a0.pattern, &a1.pattern))
            && dbg!(check_type_opt(&a0.constraint, &a1.constraint))
            && dbg!(match (&a0.labeled, &a1.labeled) {
                (None, None) | (Some(None), Some(None)) => true,
                (Some(Some(d0)), Some(Some(d1))) => check(d0, d1),
                (_, _) => false,
            })
    })
}

fn check(s0: &Expr, s1: &Expr) -> bool {
    match (&s0.kind, &s1.kind) {
        (ExprKind::Constant(v0), ExprKind::Constant(v1)) => match (v0, v1) {
            (Value::Duration(d0), Value::Duration(d1)) => {
                let f0 = d0.as_secs_f64();
                let f1 = d1.as_secs_f64();
                dbg!(
                    dbg!(f0 == f1)
                        || dbg!(f0 != 0. && f1 != 0. && ((f0 - f1).abs() / f0) < 1e-8)
                )
            }
            (Value::F32(v0), Value::F32(v1)) => dbg!(v0 == v1 || (v0 - v1).abs() < 1e-7),
            (Value::F64(v0), Value::F64(v1)) => dbg!(v0 == v1 || (v0 - v1).abs() < 1e-8),
            (v0, v1) => dbg!(v0 == v1),
        },
        (ExprKind::Array { args: a0 }, ExprKind::Array { args: a1 })
        | (ExprKind::Tuple { args: a0 }, ExprKind::Tuple { args: a1 }) => {
            a0.len() == a1.len() && a0.iter().zip(a1.iter()).all(|(e0, e1)| check(e0, e1))
        }
        (
            ExprKind::Variant { tag: t0, args: a0 },
            ExprKind::Variant { tag: t1, args: a1 },
        ) => {
            t0 == t1
                && a0.len() == a1.len()
                && a0.iter().zip(a1.iter()).all(|(e0, e1)| check(e0, e1))
        }
        (ExprKind::Struct { args: a0 }, ExprKind::Struct { args: a1 }) => {
            a0.len() == a1.len()
                && a0
                    .iter()
                    .zip(a1.iter())
                    .all(|((n0, e0), (n1, e1))| n0 == n1 && check(e0, e1))
        }
        (
            ExprKind::StructWith { name: n0, replace: r0 },
            ExprKind::StructWith { name: n1, replace: r1 },
        ) => {
            n0 == n1
                && r0.len() == r1.len()
                && r0
                    .iter()
                    .zip(r1.iter())
                    .all(|((n0, e0), (n1, e1))| n0 == n1 && check(e0, e1))
        }
        (
            ExprKind::TupleRef { name: n0, field: f0 },
            ExprKind::TupleRef { name: n1, field: f1 },
        ) => n0 == n1 && f0 == f1,
        (
            ExprKind::StructRef { name: n0, field: f0 },
            ExprKind::StructRef { name: n1, field: f1 },
        ) => n0 == n1 && f0 == f1,
        (
            ExprKind::Apply { args: srs0, function: fn0 },
            ExprKind::Constant(Value::String(c1)),
        ) if fn0 == &["str", "concat"] => match &acc_strings(srs0.iter().map(|(_, e)| e))
            [..]
        {
            [Expr { kind: ExprKind::Constant(Value::String(c0)), .. }] => dbg!(c0 == c1),
            _ => false,
        },
        (
            ExprKind::Apply { args: srs0, function: fn0 },
            ExprKind::Apply { args: srs1, function: fn1 },
        ) if fn0 == fn1 && fn0 == &["str", "concat"] => {
            let srs0 = acc_strings(srs0.iter().map(|a| &a.1));
            let srs1 = acc_strings(srs1.iter().map(|a| &a.1));
            dbg!(srs0
                .iter()
                .zip(srs1.iter())
                .fold(true, |r, (s0, s1)| r && check(s0, s1)))
        }
        (
            ExprKind::Apply { args: srs0, function: f0 },
            ExprKind::Apply { args: srs1, function: f1 },
        ) if f0 == f1 && srs0.len() == srs1.len() => {
            dbg!(srs0
                .iter()
                .zip(srs1.iter())
                .fold(true, |r, ((n0, s0), (n1, s1))| r && n0 == n1 && check(s0, s1)))
        }
        (
            ExprKind::Add { lhs: lhs0, rhs: rhs0 },
            ExprKind::Add { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::Sub { lhs: lhs0, rhs: rhs0 },
            ExprKind::Sub { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::Mul { lhs: lhs0, rhs: rhs0 },
            ExprKind::Mul { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::Div { lhs: lhs0, rhs: rhs0 },
            ExprKind::Div { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::Eq { lhs: lhs0, rhs: rhs0 },
            ExprKind::Eq { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::Ne { lhs: lhs0, rhs: rhs0 },
            ExprKind::Ne { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::Lt { lhs: lhs0, rhs: rhs0 },
            ExprKind::Lt { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::Gt { lhs: lhs0, rhs: rhs0 },
            ExprKind::Gt { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::Lte { lhs: lhs0, rhs: rhs0 },
            ExprKind::Lte { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::Gte { lhs: lhs0, rhs: rhs0 },
            ExprKind::Gte { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::And { lhs: lhs0, rhs: rhs0 },
            ExprKind::And { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (
            ExprKind::Or { lhs: lhs0, rhs: rhs0 },
            ExprKind::Or { lhs: lhs1, rhs: rhs1 },
        ) => dbg!(dbg!(check(lhs0, lhs1)) && dbg!(check(rhs0, rhs1))),
        (ExprKind::Not { expr: expr0 }, ExprKind::Not { expr: expr1 }) => {
            dbg!(check(expr0, expr1))
        }
        (
            ExprKind::Module { name: name0, export: export0, value: Some(value0) },
            ExprKind::Module { name: name1, export: export1, value: Some(value1) },
        ) => {
            dbg!(
                dbg!(name0 == name1)
                    && dbg!(export0 == export1)
                    && dbg!(value0.len() == value1.len())
                    && dbg!(value0
                        .iter()
                        .zip(value1.iter())
                        .all(|(v0, v1)| check(v0, v1)))
            )
        }
        (
            ExprKind::Module { name: name0, export: export0, value: None },
            ExprKind::Module { name: name1, export: export1, value: None },
        ) => dbg!(dbg!(name0 == name1) && dbg!(export0 == export1)),
        (ExprKind::Do { exprs: exprs0 }, ExprKind::Do { exprs: exprs1 }) => {
            exprs0.len() == exprs1.len()
                && exprs0.iter().zip(exprs1.iter()).all(|(v0, v1)| check(v0, v1))
        }
        (ExprKind::Use { name: name0 }, ExprKind::Use { name: name1 }) => {
            dbg!(name0 == name1)
        }
        (
            ExprKind::Bind { pattern: p0, export: export0, value: value0, typ: typ0 },
            ExprKind::Bind { pattern: p1, export: export1, value: value1, typ: typ1 },
        ) => dbg!(
            dbg!(check_structure_pattern(p0, p1))
                && dbg!(export0 == export1)
                && dbg!(check_type_opt(typ0, typ1))
                && dbg!(check(value0, value1))
        ),
        (
            ExprKind::Connect { name: name0, value: value0 },
            ExprKind::Connect { name: name1, value: value1 },
        ) => dbg!(dbg!(name0 == name1) && dbg!(check(value0, value1))),
        (ExprKind::Ref { name: name0 }, ExprKind::Ref { name: name1 }) => {
            dbg!(name0 == name1)
        }
        (
            ExprKind::Lambda {
                args: args0,
                vargs: vargs0,
                rtype: rtype0,
                constraints: constraints0,
                body: Either::Left(body0),
            },
            ExprKind::Lambda {
                args: args1,
                vargs: vargs1,
                rtype: rtype1,
                constraints: constraints1,
                body: Either::Left(body1),
            },
        ) => dbg!(
            dbg!(check_args(args0, args1))
                && dbg!(match (vargs0, vargs1) {
                    (Some(t0), Some(t1)) => check_type_opt(t0, t1),
                    (None, None) => true,
                    _ => false,
                })
                && dbg!(check_type_opt(rtype0, rtype1))
                && dbg!(constraints0
                    .iter()
                    .zip(constraints1.iter())
                    .all(|((tv0, tc0), (tv1, tc1))| tv0.name == tv1.name
                        && check_type(&tc0, &tc1)))
                && dbg!(check(body0, body1))
        ),
        (
            ExprKind::Lambda {
                args: args0,
                vargs: vargs0,
                rtype: rtype0,
                constraints: constraints0,
                body: Either::Right(b0),
            },
            ExprKind::Lambda {
                args: args1,
                vargs: vargs1,
                rtype: rtype1,
                constraints: constraints1,
                body: Either::Right(b1),
            },
        ) => dbg!(
            dbg!(check_args(args0, args1))
                && dbg!(match (vargs0, vargs1) {
                    (Some(t0), Some(t1)) => check_type_opt(t0, t1),
                    (None, None) => true,
                    _ => false,
                })
                && dbg!(check_type_opt(rtype0, rtype1))
                && dbg!(constraints0
                    .iter()
                    .zip(constraints1.iter())
                    .all(|((tv0, tc0), (tv1, tc1))| tv0.name == tv1.name
                        && check_type(&tc0, &tc1)))
                && dbg!(b0 == b1)
        ),
        (
            ExprKind::Select { arg: arg0, arms: arms0 },
            ExprKind::Select { arg: arg1, arms: arms1 },
        ) => {
            dbg!(
                dbg!(check(arg0, arg1))
                    && dbg!(arms0.len() == arms1.len())
                    && dbg!(arms0
                        .iter()
                        .zip(arms1.iter())
                        .all(|((pat0, b0), (pat1, b1))| check(b0, b1)
                            && dbg!(check_pattern(pat0, pat1))))
            )
        }
        (
            ExprKind::TypeDef { name: name0, typ: typ0 },
            ExprKind::TypeDef { name: name1, typ: typ1 },
        ) => dbg!(name0 == name1) && dbg!(check_type(&typ0, &typ1)),
        (
            ExprKind::TypeCast { expr: expr0, typ: typ0 },
            ExprKind::TypeCast { expr: expr1, typ: typ1 },
        ) => dbg!(check(expr0, expr1)) && dbg!(check_type(&typ0, &typ1)),
        (ExprKind::Any { args: a0 }, ExprKind::Any { args: a1 }) => {
            a0.len() == a1.len() && a0.iter().zip(a1.iter()).all(|(a0, a1)| check(a0, a1))
        }
        (_, _) => false,
    }
}

proptest! {
    #[test]
    fn expr_round_trip(s in modexpr()) {
        let s = dbg!(s);
        let st = dbg!(s.to_string());
        let e = dbg!(parse(st.as_str()).unwrap());
        assert!(check(&s, &e))
    }

    #[test]
    fn expr_pp_round_trip(s in modexpr()) {
        let s = dbg!(s);
        let st = dbg!(s.to_string_pretty(80));
        let e = dbg!(parse(st.as_str()).unwrap());
        assert!(check(&s, &e))
    }
}
