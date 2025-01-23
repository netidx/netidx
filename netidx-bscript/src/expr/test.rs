use super::*;
use bytes::Bytes;
use chrono::prelude::*;
use netidx_core::chars::Chars;
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

fn bytes() -> impl Strategy<Value = Bytes> {
    any::<Vec<u8>>().prop_map(Bytes::from)
}

fn chars() -> impl Strategy<Value = Chars> {
    any::<String>().prop_map(Chars::from)
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
        chars().prop_map(Value::String),
        bytes().prop_map(Value::Bytes),
        Just(Value::True),
        Just(Value::False),
        Just(Value::Null),
        Just(Value::Ok),
        chars().prop_map(Value::Error),
    ]
}

prop_compose! {
    fn random_fname()(s in "[a-z][a-z0-9_]*".prop_filter("Filter reserved words", |s| {
        s != "ok"
            && s != "true"
            && s != "false"
            && s != "null"
            && s != "load"
            && s != "store"
            && s != "load_var"
            && s != "store_var"
    })) -> ModPath {
        ModPath::from_iter([s])
    }
}

fn valid_fname() -> impl Strategy<Value = ModPath> {
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
        Just(ModPath::from_iter(["let"])),
    ]
}

fn fname() -> impl Strategy<Value = ModPath> {
    prop_oneof![random_fname(), valid_fname(),]
}

fn expr() -> impl Strategy<Value = Expr> {
    let leaf = value().prop_map(|v| ExprKind::Constant(v).to_expr());
    leaf.prop_recursive(100, 1000000, 10, |inner| {
        prop_oneof![(collection::vec(inner, (0, 10)), fname()).prop_map(|(s, f)| {
            ExprKind::Apply { function: f, args: Arc::from(s) }.to_expr()
        })]
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
                        *c0 = Chars::from(st);
                    }
                    _ => v.push(s),
                },
            },
            _ => v.push(s),
        }
    }
    Arc::from(v)
}

fn check(s0: &Expr, s1: &Expr) -> bool {
    match (&s0.kind, &s1.kind) {
        (ExprKind::Constant(v0), ExprKind::Constant(v1)) => match (v0, v1) {
            (Value::Duration(d0), Value::Duration(d1)) => {
                let f0 = d0.as_secs_f64();
                let f1 = d1.as_secs_f64();
                f0 == f1 || (f0 != 0. && f1 != 0. && ((f0 - f1).abs() / f0) < 1e-8)
            }
            (Value::F32(v0), Value::F32(v1)) => v0 == v1 || (v0 - v1).abs() < 1e-7,
            (Value::F64(v0), Value::F64(v1)) => v0 == v1 || (v0 - v1).abs() < 1e-8,
            (v0, v1) => dbg!(dbg!(v0) == dbg!(v1)),
        },
        (
            ExprKind::Apply { args: srs0, function: fn0 },
            ExprKind::Constant(Value::String(c1)),
        ) if fn0 == &["str", "concat"] => match &acc_strings(srs0)[..] {
            [Expr { kind: ExprKind::Constant(Value::String(c0)), .. }] => c0 == c1,
            _ => false,
        },
        (
            ExprKind::Apply { args: srs0, function: fn0 },
            ExprKind::Apply { args: srs1, function: fn1 },
        ) if fn0 == fn1 && fn0 == &["str", "concat"] => {
            let srs0 = acc_strings(srs0);
            srs0.iter().zip(srs1.iter()).fold(true, |r, (s0, s1)| r && check(s0, s1))
        }
        (
            ExprKind::Apply { args: srs0, function: f0 },
            ExprKind::Apply { args: srs1, function: f1 },
        ) if f0 == f1 && srs0.len() == srs1.len() => {
            srs0.iter().zip(srs1.iter()).fold(true, |r, (s0, s1)| r && check(s0, s1))
        }
        (_, _) => false,
    }
}

proptest! {
    #[test]
    fn expr_round_trip(s in expr()) {
        assert!(check(dbg!(&s), &dbg!(dbg!(s.to_string()).parse::<Expr>().unwrap())))
    }

    #[test]
    fn expr_pp_round_trip(s in expr()) {
        assert!(check(dbg!(&s), &dbg!(dbg!(s.to_string_pretty(80)).parse::<Expr>().unwrap())))
    }
}
