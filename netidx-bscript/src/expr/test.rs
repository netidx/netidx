use super::*;
use bytes::Bytes;
use chrono::prelude::*;
use netidx_netproto::pbuf::PBytes;
use parser::{parse, RESERVED};
use prop::option;
use proptest::{collection, prelude::*};
use std::time::Duration;

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
        Just(Value::True),
        Just(Value::False),
        Just(Value::Null),
        Just(Value::Ok),
        arcstr().prop_map(Value::Error),
    ]
}

prop_compose! {
    fn random_modpart()(s in "[a-z][a-z0-9_]*".prop_filter(
        "Filter reserved words",
        |s| !RESERVED.contains(&s.as_str()))
    ) -> String
    {
        s
    }
}

fn random_fname() -> impl Strategy<Value = ArcStr> {
    random_modpart().prop_map(ArcStr::from)
}

fn random_modpath() -> impl Strategy<Value = ModPath> {
    collection::vec(random_modpart(), (1, 10)).prop_map(ModPath::from_iter)
}

fn valid_modpath() -> impl Strategy<Value = ModPath> {
    prop_oneof![
        Just(ModPath::from_iter(["any"])),
        Just(ModPath::from_iter(["array"])),
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
        Just(ModPath::from_iter(["if"])),
        Just(ModPath::from_iter(["filter"])),
        Just(ModPath::from_iter(["cast"])),
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
        Just(Typ::Result),
        Just(Typ::Array),
        Just(Typ::Null),
    ]
}

fn pattern() -> impl Strategy<Value = Pattern> {
    prop_oneof![
        Just(Pattern::Underscore),
        (collection::vec(typ(), (0, 10)), random_fname()).prop_map(|(tag, bind)| {
            Pattern::Typ { tag: Arc::from(tag), bind, guard: None }
        })
    ]
}

fn build_pattern(arg: Expr, arms: Vec<(Option<Expr>, Pattern, Expr)>) -> Expr {
    let arms = arms.into_iter().map(|(guard, mut pat, expr)| {
        match &mut pat {
            Pattern::Typ { guard: g, .. } => {
                *g = guard;
            }
            Pattern::Underscore => (),
        }
        (pat, expr)
    });
    ExprKind::Select { arg: Arc::new(arg), arms: Arc::from_iter(arms) }.to_expr()
}

fn arithexpr() -> impl Strategy<Value = Expr> {
    let leaf = prop_oneof![constant(), reference()];
    leaf.prop_recursive(5, 100, 5, |inner| {
        prop_oneof![
            (
                inner.clone(),
                collection::vec(
                    (option::of(inner.clone()), pattern(), inner.clone()),
                    (1, 10)
                )
            )
                .prop_map(|(arg, arms)| build_pattern(arg, arms)),
            collection::vec(inner.clone(), (1, 10))
                .prop_map(|e| ExprKind::Do { exprs: Arc::from(e) }.to_expr()),
            (collection::vec(inner.clone(), (0, 10)), modpath()).prop_map(|(s, f)| {
                ExprKind::Apply { function: f, args: Arc::from(s) }.to_expr()
            }),
            inner.clone().prop_map(|e0| ExprKind::Not { expr: Arc::new(e0) }.to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Eq {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Ne {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Lt {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Gt {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Gte {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Lte {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::And {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Or {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Add {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Sub {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Mul {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
            (inner.clone(), inner.clone()).prop_map(|(e0, e1)| ExprKind::Div {
                lhs: Arc::new(e0),
                rhs: Arc::new(e1)
            }
            .to_expr()),
        ]
    })
}

fn expr() -> impl Strategy<Value = Expr> {
    let leaf = prop_oneof![constant(), reference()];
    leaf.prop_recursive(100, 1000, 10, |inner| {
        prop_oneof![
            arithexpr(),
            (collection::vec(inner.clone(), (0, 10)), modpath()).prop_map(|(s, f)| {
                ExprKind::Apply { function: f, args: Arc::from(s) }.to_expr()
            }),
            collection::vec(inner.clone(), (1, 10))
                .prop_map(|e| ExprKind::Do { exprs: Arc::from(e) }.to_expr()),
            (collection::vec(random_fname(), (0, 10)), any::<bool>(), inner.clone())
                .prop_map(|(args, vargs, body)| {
                    ExprKind::Lambda {
                        args: Arc::from_iter(args),
                        vargs,
                        body: Either::Left(Arc::new(body)),
                    }
                    .to_expr()
                }),
            (collection::vec(random_fname(), (0, 10)), any::<bool>(), random_fname())
                .prop_map(|(args, vargs, body)| {
                    ExprKind::Lambda {
                        args: Arc::from_iter(args),
                        vargs,
                        body: Either::Right(body),
                    }
                    .to_expr()
                }),
            (inner.clone(), random_fname(), any::<bool>()).prop_map(|(e, n, exp)| {
                ExprKind::Bind { export: exp, name: n, value: Arc::new(e) }.to_expr()
            }),
            (inner.clone(), modpath()).prop_map(|(e, n)| {
                ExprKind::Connect { name: n, value: Arc::new(e) }.to_expr()
            }),
            (
                inner.clone(),
                collection::vec(
                    (option::of(inner.clone()), pattern(), inner.clone()),
                    (1, 10)
                )
            )
                .prop_map(|(arg, arms)| build_pattern(arg, arms)),
        ]
    })
}

fn modexpr() -> impl Strategy<Value = Expr> {
    let leaf = prop_oneof![
        (collection::vec(expr(), (0, 10)), modpath()).prop_map(|(s, f)| {
            ExprKind::Apply { function: f, args: Arc::from(s) }.to_expr()
        }),
        collection::vec(expr(), (1, 10))
            .prop_map(|e| ExprKind::Do { exprs: Arc::from(e) }.to_expr()),
        modpath().prop_map(|name| ExprKind::Use { name }.to_expr()),
        (expr(), random_fname(), any::<bool>()).prop_map(|(e, n, exp)| {
            ExprKind::Bind { export: exp, name: n, value: Arc::new(e) }.to_expr()
        }),
        (expr(), modpath()).prop_map(|(e, n)| {
            ExprKind::Connect { name: n, value: Arc::new(e) }.to_expr()
        }),
    ];
    leaf.prop_recursive(10, 1000, 100, |inner| {
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

fn acc_strings(args: &[Expr]) -> Arc<[Expr]> {
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

fn check_pattern(pat0: &Pattern, pat1: &Pattern) -> bool {
    match (pat0, pat1) {
        (Pattern::Underscore, Pattern::Underscore) => true,
        (
            Pattern::Typ { tag: tag0, bind: bind0, guard: Some(guard0) },
            Pattern::Typ { tag: tag1, bind: bind1, guard: Some(guard1) },
        ) => tag0 == tag1 && bind0 == bind1 && check(guard0, guard1),
        (
            Pattern::Typ { tag: tag0, bind: bind0, guard: None },
            Pattern::Typ { tag: tag1, bind: bind1, guard: None },
        ) => tag0 == tag1 && bind0 == bind1,
        (_, _) => false,
    }
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
        (
            ExprKind::Apply { args: srs0, function: fn0 },
            ExprKind::Constant(Value::String(c1)),
        ) if fn0 == &["str", "concat"] => match &acc_strings(srs0)[..] {
            [Expr { kind: ExprKind::Constant(Value::String(c0)), .. }] => dbg!(c0 == c1),
            _ => false,
        },
        (
            ExprKind::Apply { args: srs0, function: fn0 },
            ExprKind::Apply { args: srs1, function: fn1 },
        ) if fn0 == fn1 && fn0 == &["str", "concat"] => {
            let srs0 = acc_strings(srs0);
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
                .fold(true, |r, (s0, s1)| r && check(s0, s1)))
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
            ExprKind::Bind { name: name0, export: export0, value: value0 },
            ExprKind::Bind { name: name1, export: export1, value: value1 },
        ) => dbg!(
            dbg!(name0 == name1)
                && dbg!(export0 == export1)
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
            ExprKind::Lambda { args: args0, vargs: vargs0, body: Either::Left(body0) },
            ExprKind::Lambda { args: args1, vargs: vargs1, body: Either::Left(body1) },
        ) => dbg!(
            dbg!(args0 == args1) && dbg!(vargs0 == vargs1) && dbg!(check(body0, body1))
        ),
        (
            ExprKind::Lambda { args: args0, vargs: vargs0, body: Either::Right(b0) },
            ExprKind::Lambda { args: args1, vargs: vargs1, body: Either::Right(b1) },
        ) => dbg!(dbg!(args0 == args1) && dbg!(vargs0 == vargs1) && dbg!(b0 == b1)),
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
