use super::*;
use crate::typ::{Refs, Type};
use arcstr::literal;

fn parse_expr(s: &str) -> anyhow::Result<Expr> {
    expr()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[allow(unused)]
fn parse_typexpr(s: &str) -> anyhow::Result<Type<Refs>> {
    typexp()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[allow(unused)]
fn parse_structure_pattern(s: &str) -> anyhow::Result<StructurePattern> {
    structure_pattern()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[test]
fn escaped_string() {
    let p = Value::String(literal!(r#"/foo bar baz/"zam"/)_ xyz+ "#));
    let s = r#"load("/foo bar baz/\"zam\"/)_ xyz+ ")"#;
    assert_eq!(
        ExprKind::Apply {
            function: Arc::new(ExprKind::Ref { name: ["load"].into() }.to_expr()),
            args: Arc::from_iter([(None, ExprKind::Constant(p).to_expr())])
        }
        .to_expr(),
        parse(s).unwrap()
    );
}

#[test]
fn interpolated0() {
    let p = ExprKind::Apply {
        function: Arc::new(ExprKind::Ref { name: ["load"].into() }.to_expr()),
        args: Arc::from_iter([(
            None,
            ExprKind::StringInterpolate {
                args: Arc::from_iter([
                    ExprKind::Constant(Value::from("/foo/")).to_expr(),
                    ExprKind::Apply {
                        function: Arc::new(
                            ExprKind::Ref { name: ["get"].into() }.to_expr(),
                        ),
                        args: Arc::from_iter([(
                            None,
                            ExprKind::StringInterpolate {
                                args: Arc::from_iter([
                                    ExprKind::Ref { name: ["sid"].into() }.to_expr(),
                                    ExprKind::Constant(Value::from("_var")).to_expr(),
                                ]),
                            }
                            .to_expr(),
                        )]),
                    }
                    .to_expr(),
                    ExprKind::Constant(Value::from("/baz")).to_expr(),
                ]),
            }
            .to_expr(),
        )]),
    }
    .to_expr();
    let s = r#"load("/foo/[get("[sid]_var")]/baz")"#;
    assert_eq!(p, parse(s).unwrap());
}

#[test]
fn interpolated1() {
    let s = r#"{"[true]"}"#;
    let p = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::StringInterpolate {
            args: Arc::from_iter([ExprKind::Constant(Value::Bool(true)).to_expr()]),
        }
        .to_expr()]),
    }
    .to_expr();
    assert_eq!(p, parse(s).unwrap());
}

#[test]
fn interpolated2() {
    let s = r#"a(a(a(get("[true]"))))"#;
    let p = ExprKind::Apply {
        args: Arc::from_iter([(
            None,
            ExprKind::Apply {
                args: Arc::from_iter([(
                    None,
                    ExprKind::Apply {
                        args: Arc::from_iter([(
                            None,
                            ExprKind::Apply {
                                args: Arc::from_iter([(
                                    None,
                                    ExprKind::StringInterpolate {
                                        args: Arc::from_iter([ExprKind::Constant(
                                            Value::Bool(true),
                                        )
                                        .to_expr()]),
                                    }
                                    .to_expr(),
                                )]),
                                function: Arc::new(
                                    ExprKind::Ref { name: ["get"].into() }.to_expr(),
                                ),
                            }
                            .to_expr(),
                        )]),
                        function: Arc::new(
                            ExprKind::Ref { name: ["a"].into() }.to_expr(),
                        ),
                    }
                    .to_expr(),
                )]),
                function: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
            }
            .to_expr(),
        )]),
        function: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
    }
    .to_expr();
    assert_eq!(p, parse(s).unwrap());
}

#[test]
fn apply_path() {
    let s = r#"load(path::concat("foo", "bar", baz))"#;
    assert_eq!(
        ExprKind::Apply {
            args: Arc::from_iter([(
                None,
                ExprKind::Apply {
                    args: Arc::from_iter([
                        (
                            None,
                            ExprKind::Constant(Value::String(literal!("foo"))).to_expr()
                        ),
                        (
                            None,
                            ExprKind::Constant(Value::String(literal!("bar"))).to_expr()
                        ),
                        (None, ExprKind::Ref { name: ["baz"].into() }.to_expr())
                    ]),
                    function: Arc::new(
                        ExprKind::Ref { name: ["path", "concat"].into() }.to_expr()
                    ),
                }
                .to_expr()
            )]),
            function: Arc::new(ExprKind::Ref { name: ["load"].into() }.to_expr()),
        }
        .to_expr(),
        parse(s).unwrap()
    );
}

#[test]
fn var_ref() {
    assert_eq!(
        ExprKind::Ref { name: ["sum"].into() }.to_expr(),
        parse_expr("sum").unwrap()
    );
}

#[test]
fn letbind() {
    assert_eq!(
        ExprKind::Bind {
            export: false,
            typ: None,
            pattern: StructurePattern::Bind(literal!("foo")),
            value: Arc::new(ExprKind::Constant(Value::I64(42)).to_expr())
        }
        .to_expr(),
        parse("let foo = 42").unwrap()
    );
}

#[test]
fn typed_letbind() {
    assert_eq!(
        ExprKind::Bind {
            export: false,
            typ: Some(Type::Primitive(Typ::I64.into())),
            pattern: StructurePattern::Bind(literal!("foo")),
            value: Arc::new(ExprKind::Constant(Value::I64(42)).to_expr())
        }
        .to_expr(),
        parse("let foo: i64 = 42").unwrap()
    );
}

#[test]
fn nested_apply() {
    let src = ExprKind::Apply {
        args: Arc::from_iter([
            (None, ExprKind::Constant(Value::F32(1.)).to_expr()),
            (
                None,
                ExprKind::Apply {
                    args: Arc::from_iter([(
                        None,
                        ExprKind::Constant(Value::String(literal!("/foo/bar",)))
                            .to_expr(),
                    )]),
                    function: Arc::new(ExprKind::Ref { name: ["load"].into() }.to_expr()),
                }
                .to_expr(),
            ),
            (
                None,
                ExprKind::Apply {
                    args: Arc::from_iter([
                        (None, ExprKind::Constant(Value::F32(675.6)).to_expr()),
                        (
                            None,
                            ExprKind::Apply {
                                args: Arc::from_iter([(
                                    None,
                                    ExprKind::Constant(Value::String(literal!(
                                        "/foo/baz"
                                    )))
                                    .to_expr(),
                                )]),
                                function: Arc::new(
                                    ExprKind::Ref { name: ["load"].into() }.to_expr(),
                                ),
                            }
                            .to_expr(),
                        ),
                    ]),
                    function: Arc::new(ExprKind::Ref { name: ["max"].into() }.to_expr()),
                }
                .to_expr(),
            ),
            (
                None,
                ExprKind::Apply {
                    args: Arc::from_iter([]),
                    function: Arc::new(ExprKind::Ref { name: ["rand"].into() }.to_expr()),
                }
                .to_expr(),
            ),
        ]),
        function: Arc::new(ExprKind::Ref { name: ["sum"].into() }.to_expr()),
    }
    .to_expr();
    let s = r#"sum(f32:1., load("/foo/bar"), max(f32:675.6, load("/foo/baz")), rand())"#;
    assert_eq!(src, parse(s).unwrap());
}

#[test]
fn arith_eq() {
    let exp = ExprKind::Eq {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"a == b"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_ne() {
    let exp = ExprKind::Ne {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"a != b"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_gt() {
    let exp = ExprKind::Gt {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"a > b"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_lt() {
    let exp = ExprKind::Lt {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"a < b"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_gte() {
    let exp = ExprKind::Gte {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"a >= b"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_lte() {
    let exp = ExprKind::Lte {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"a <= b"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_add() {
    let exp = ExprKind::Add {
        lhs: Arc::new(
            ExprKind::Add {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
                rhs: Arc::new(ExprKind::Ref { name: ["b"].into() }.to_expr()),
            }
            .to_expr(),
        ),
        rhs: Arc::new(ExprKind::Ref { name: ["c"].into() }.to_expr()),
    }
    .to_expr();
    let s = r#"a + b + c"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_sub() {
    let exp = ExprKind::Sub {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"a - b"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_mul() {
    let exp = ExprKind::Mul {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"a * b"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_div() {
    let exp = ExprKind::Div {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"a / b"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_paren() {
    let exp = ExprKind::Div {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"a / b"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_nested() {
    let sum = ExprKind::Add {
        lhs: Arc::new(
            ExprKind::Add {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
                rhs: Arc::new(ExprKind::Ref { name: ["b"].into() }.to_expr()),
            }
            .to_expr(),
        ),
        rhs: Arc::new(ExprKind::Ref { name: ["c"].into() }.to_expr()),
    }
    .to_expr();
    let sub = ExprKind::Sub {
        lhs: Arc::new(
            ExprKind::Sub {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
                rhs: Arc::new(ExprKind::Ref { name: ["b"].into() }.to_expr()),
            }
            .to_expr(),
        ),
        rhs: Arc::new(ExprKind::Ref { name: ["c"].into() }.to_expr()),
    }
    .to_expr();
    let eq = ExprKind::Eq { lhs: Arc::new(sum), rhs: Arc::new(sub) }.to_expr();
    let exp = ExprKind::And {
        lhs: Arc::new(eq),
        rhs: Arc::new(
            ExprKind::Not {
                expr: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
            }
            .to_expr(),
        ),
    }
    .to_expr();
    let s = r#"((a + b + c) == (a - b - c)) && !a"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn select0() {
    let arms = Arc::from_iter([
        (
            Pattern {
                type_predicate: Some(Type::Primitive(Typ::I64.into())),
                structure_predicate: StructurePattern::Bind(literal!("a")),
                guard: Some(
                    ExprKind::Lt {
                        lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
                        rhs: Arc::new(ExprKind::Constant(Value::I64(10)).to_expr()),
                    }
                    .to_expr(),
                ),
            },
            ExprKind::Mul {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
                rhs: Arc::new(ExprKind::Constant(Value::I64(2)).to_expr()),
            }
            .to_expr(),
        ),
        (
            Pattern {
                type_predicate: None,
                structure_predicate: StructurePattern::Bind(literal!("a")),
                guard: None,
            },
            ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr(),
        ),
    ]);
    let arg = Arc::new(
        ExprKind::Apply {
            args: Arc::from_iter([(
                None,
                ExprKind::Ref { name: ["b"].into() }.to_expr(),
            )]),
            function: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr()),
        }
        .to_expr(),
    );
    let exp = ExprKind::Select { arg, arms }.to_expr();
    let s = r#"select foo(b) { i64 as a if a < 10 => a * 2, a => a }"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn select1() {
    let arms = Arc::from_iter([
        (
            Pattern {
                type_predicate: Some(Type::Array(Arc::new(Type::Primitive(
                    Typ::I64.into(),
                )))),
                structure_predicate: StructurePattern::Slice {
                    all: None,
                    binds: Arc::from_iter([
                        StructurePattern::Bind(literal!("a")),
                        StructurePattern::Ignore,
                        StructurePattern::Bind(literal!("b")),
                    ]),
                },
                guard: Some(
                    ExprKind::Lt {
                        lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
                        rhs: Arc::new(ExprKind::Constant(Value::I64(10)).to_expr()),
                    }
                    .to_expr(),
                ),
            },
            ExprKind::Mul {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
                rhs: Arc::new(ExprKind::Constant(Value::I64(2)).to_expr()),
            }
            .to_expr(),
        ),
        (
            Pattern {
                type_predicate: Some(Type::Array(Arc::new(Type::Primitive(
                    Typ::I64.into(),
                )))),
                structure_predicate: StructurePattern::SlicePrefix {
                    all: None,
                    prefix: Arc::from_iter([StructurePattern::Bind(literal!("a"))]),
                    tail: Some(literal!("b")),
                },
                guard: None,
            },
            ExprKind::Ref { name: ["a"].into() }.to_expr(),
        ),
        (
            Pattern {
                type_predicate: Some(Type::Array(Arc::new(Type::Primitive(
                    Typ::I64.into(),
                )))),
                structure_predicate: StructurePattern::SliceSuffix {
                    all: None,
                    suffix: Arc::from_iter([StructurePattern::Bind(literal!("b"))]),
                    head: Some(literal!("a")),
                },
                guard: None,
            },
            ExprKind::Ref { name: ["a"].into() }.to_expr(),
        ),
        (
            Pattern {
                type_predicate: Some(Type::Array(Arc::new(Type::Primitive(
                    Typ::I64.into(),
                )))),
                structure_predicate: StructurePattern::Slice {
                    all: None,
                    binds: Arc::from_iter([
                        StructurePattern::Literal(Value::I64(1)),
                        StructurePattern::Literal(Value::I64(2)),
                        StructurePattern::Literal(Value::I64(42)),
                        StructurePattern::Bind(literal!("a")),
                    ]),
                },
                guard: None,
            },
            ExprKind::Ref { name: ["a"].into() }.to_expr(),
        ),
        (
            Pattern {
                type_predicate: Some(Type::Ref(["Foo"].into())),
                structure_predicate: StructurePattern::Struct {
                    all: None,
                    exhaustive: false,
                    binds: Arc::from_iter([
                        (literal!("bar"), StructurePattern::Ignore),
                        (literal!("baz"), StructurePattern::Bind(literal!("baz"))),
                        (literal!("foo"), StructurePattern::Literal(Value::I64(42))),
                        (literal!("foobar"), StructurePattern::Bind(literal!("a"))),
                    ]),
                },
                guard: None,
            },
            ExprKind::Ref { name: ["a"].into() }.to_expr(),
        ),
        (
            Pattern {
                type_predicate: None,
                structure_predicate: StructurePattern::Bind(literal!("a")),
                guard: None,
            },
            ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr(),
        ),
    ]);
    let arg = Arc::new(
        ExprKind::Apply {
            args: Arc::from_iter([(
                None,
                ExprKind::Ref { name: ["b"].into() }.to_expr(),
            )]),
            function: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr()),
        }
        .to_expr(),
    );
    let exp = ExprKind::Select { arg, arms }.to_expr();
    let s = r#"
select foo(b) {
    Array<i64> as [a, _, b] if a < 10 => a * 2,
    Array<i64> as [a, b..] => a,
    Array<i64> as [a.., b] => a,
    Array<i64> as [1, 2, 42, a] => a,
    Foo as { foo: 42, bar: _, baz, foobar: a, .. } => a,
    a => a
}"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn pattern0() {
    let s = r#"i64 as a if a < 10"#;
    dbg!(super::pattern().easy_parse(position::Stream::new(s)).unwrap());
}

#[test]
fn pattern1() {
    let s = r#"[a.., b]"#;
    dbg!(super::slice_pattern().easy_parse(position::Stream::new(s)).unwrap());
}

#[test]
fn connect() {
    let exp = ExprKind::Connect {
        name: ModPath::from(["m", "foo"]),
        value: Arc::new(
            ExprKind::Add {
                lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
                rhs: Arc::new(ExprKind::Constant(Value::I64(1)).to_expr()),
            }
            .to_expr(),
        ),
    }
    .to_expr();
    let s = r#"m::foo <- (a + 1)"#;
    assert_eq!(exp, parse(s).unwrap());
}

#[test]
fn inline_module() {
    let exp = ExprKind::Module {
        name: literal!("foo"),
        export: true,
        value: Some(Arc::from_iter([
            ExprKind::Bind {
                typ: None,
                export: true,
                pattern: StructurePattern::Bind(literal!("z")),
                value: Arc::new(ExprKind::Constant(Value::I64(42)).to_expr()),
            }
            .to_expr(),
            ExprKind::Bind {
                typ: None,
                export: false,
                pattern: StructurePattern::Bind(literal!("m")),
                value: Arc::new(ExprKind::Constant(Value::I64(42)).to_expr()),
            }
            .to_expr(),
        ])),
    }
    .to_expr();
    let s = r#"pub mod foo {
        pub let z = 42
        let m = 42
    }"#;
    assert_eq!(exp, parse(s).unwrap());
}

#[test]
fn external_module() {
    let exp =
        ExprKind::Module { name: literal!("foo"), export: true, value: None }.to_expr();
    let s = r#"pub mod foo;"#;
    assert_eq!(exp, parse(s).unwrap());
}

#[test]
fn usemodule() {
    let exp = ExprKind::Use { name: ModPath::from(["foo"]) }.to_expr();
    let s = r#"use foo"#;
    assert_eq!(exp, parse(s).unwrap());
}

#[test]
fn array() {
    let exp = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::Array {
            args: Arc::from_iter([
                ExprKind::Array {
                    args: Arc::from_iter([
                        ExprKind::Constant(Value::from("foo")).to_expr(),
                        ExprKind::Constant(Value::from(42)).to_expr(),
                    ]),
                }
                .to_expr(),
                ExprKind::Array {
                    args: Arc::from_iter([
                        ExprKind::Constant(Value::from("bar")).to_expr(),
                        ExprKind::Constant(Value::from(42)).to_expr(),
                    ]),
                }
                .to_expr(),
            ]),
        }
        .to_expr()]),
    }
    .to_expr();
    let s = r#"{[["foo", 42], ["bar", 42]]}"#;
    assert_eq!(exp, parse(s).unwrap());
}

#[test]
fn doexpr() {
    let exp = ExprKind::Do {
        exprs: Arc::from_iter([
            ExprKind::Bind {
                typ: None,
                export: false,
                pattern: StructurePattern::Bind(literal!("baz")),
                value: Arc::new(ExprKind::Constant(Value::from(42)).to_expr()),
            }
            .to_expr(),
            ExprKind::Ref { name: ModPath::from(["baz"]) }.to_expr(),
        ]),
    }
    .to_expr();
    let s = r#"{ let baz = 42; baz }"#;
    assert_eq!(exp, parse(s).unwrap());
}

#[test]
fn lambda() {
    let exp = ExprKind::Lambda {
        args: Arc::from_iter([
            Arg {
                labeled: None,
                pattern: StructurePattern::Bind("foo".into()),
                constraint: None,
            },
            Arg {
                labeled: None,
                pattern: StructurePattern::Bind("bar".into()),
                constraint: None,
            },
        ]),
        rtype: None,
        vargs: None,
        constraints: Arc::from_iter([]),
        body: Either::Left(Arc::new(
            ExprKind::Add {
                lhs: Arc::new(
                    ExprKind::Add {
                        lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
                        rhs: Arc::new(ExprKind::Ref { name: ["b"].into() }.to_expr()),
                    }
                    .to_expr(),
                ),
                rhs: Arc::new(ExprKind::Ref { name: ["c"].into() }.to_expr()),
            }
            .to_expr(),
        )),
    }
    .to_expr();
    let s = r#"|foo, bar| (a + b + c)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn nested_lambda() {
    let e = Arc::new(
        ExprKind::Add {
            lhs: Arc::new(
                ExprKind::Add {
                    lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
                    rhs: Arc::new(ExprKind::Ref { name: ["b"].into() }.to_expr()),
                }
                .to_expr(),
            ),
            rhs: Arc::new(ExprKind::Ref { name: ["c"].into() }.to_expr()),
        }
        .to_expr(),
    );
    let exp = ExprKind::Lambda {
        args: Arc::from_iter([]),
        rtype: None,
        vargs: None,
        constraints: Arc::from_iter([]),
        body: Either::Left(Arc::new(
            ExprKind::Lambda {
                args: Arc::from_iter([]),
                rtype: None,
                vargs: None,
                constraints: Arc::from_iter([]),
                body: Either::Left(e),
            }
            .to_expr(),
        )),
    }
    .to_expr();
    let s = r#"|| || (a + b + c)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn apply_lambda() {
    let e = ExprKind::Apply {
        args: Arc::from_iter([(
            None,
            ExprKind::Lambda {
                args: Arc::from_iter([Arg {
                    labeled: None,
                    pattern: StructurePattern::Bind("a".into()),
                    constraint: None,
                }]),
                vargs: Some(None),
                rtype: None,
                constraints: Arc::from_iter([]),
                body: Either::Right("a".into()),
            }
            .to_expr(),
        )]),
        function: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
    }
    .to_expr();
    let s = "a(|a, @args| 'a)";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn apply_typed_lambda() {
    let e = ExprKind::Apply {
        args: Arc::from_iter([(
            None,
            ExprKind::Lambda {
                args: Arc::from_iter([
                    Arg {
                        labeled: None,
                        pattern: StructurePattern::Bind("a".into()),
                        constraint: None,
                    },
                    Arg {
                        labeled: None,
                        pattern: StructurePattern::Bind("b".into()),
                        constraint: Some(Type::Set(Arc::from_iter([
                            Type::Primitive(Typ::Null.into()),
                            Type::Ref(["Number"].into()),
                        ]))),
                    },
                ]),
                vargs: Some(Some(Type::Primitive(Typ::String.into()))),
                rtype: Some(Type::Bottom(PhantomData)),
                constraints: Arc::from_iter([]),
                body: Either::Right("a".into()),
            }
            .to_expr(),
        )]),
        function: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
    }
    .to_expr();
    let s = "a(|a, b: [null, Number], @args: string| -> _ 'a)";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn mod_interpolate() {
    let e = ExprKind::Module {
        name: literal!("a"),
        export: false,
        value: Some(Arc::from_iter([ExprKind::Do {
            exprs: Arc::from_iter([ExprKind::StringInterpolate {
                args: Arc::from_iter([
                    ExprKind::Constant(Value::from("foo_")).to_expr(),
                    ExprKind::Constant(Value::I64(42)).to_expr(),
                ]),
            }
            .to_expr()]),
        }
        .to_expr()])),
    }
    .to_expr();
    let s = "mod a{{\"foo_[42]\"}}";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn multi_line_do() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::Mul {
            lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
            rhs: Arc::new(ExprKind::Constant(Value::U64(1)).to_expr()),
        }
        .to_expr()]),
    }
    .to_expr();
    let s = "{\n  (a *\n  u64:1\n)}\n";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn typed_array() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::Bind {
            export: false,
            pattern: StructurePattern::Bind(literal!("f")),
            typ: None,
            value: Arc::new(
                ExprKind::Lambda {
                    args: Arc::from_iter([Arg {
                        labeled: None,
                        pattern: StructurePattern::Bind("a".into()),
                        constraint: Some(Type::Array(Arc::new(Type::TVar(
                            TVar::empty_named("a".into()),
                        )))),
                    }]),
                    vargs: None,
                    constraints: Arc::from_iter([]),
                    rtype: Some(Type::TVar(TVar::empty_named("a".into()))),
                    body: Either::Left(Arc::new(
                        ExprKind::Ref { name: ["a"].into() }.to_expr(),
                    )),
                }
                .to_expr(),
            ),
        }
        .to_expr()]),
    }
    .to_expr();
    let s = "{let f = |a: Array<'a>| -> 'a a}";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn labeled_argument_lambda() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::Bind {
            export: false,
            pattern: StructurePattern::Bind(literal!("a")),
            typ: Some(Type::Fn(Arc::new(FnType {
                args: Arc::from_iter([
                    FnArgType {
                        label: Some(("foo".into(), true)),
                        typ: Type::Ref(["Number"].into()),
                    },
                    FnArgType {
                        label: Some(("bar".into(), true)),
                        typ: Type::Primitive(Typ::String.into()),
                    },
                    FnArgType {
                        label: Some(("a".into(), false)),
                        typ: Type::Ref(["Any"].into()),
                    },
                    FnArgType { label: None, typ: Type::Ref(["Any"].into()) },
                ]),
                vargs: None,
                rtype: Type::Primitive(Typ::String.into()),
                constraints: Arc::from_iter([]),
            }))),
            value: Arc::new(
                ExprKind::Lambda {
                    args: Arc::from_iter([
                        Arg {
                            pattern: StructurePattern::Bind("foo".into()),
                            labeled: Some(Some(ExprKind::Constant(3.into()).to_expr())),
                            constraint: Some(Type::Ref(["Number"].into())),
                        },
                        Arg {
                            pattern: StructurePattern::Bind("bar".into()),
                            labeled: Some(Some(
                                ExprKind::Constant("hello".into()).to_expr(),
                            )),
                            constraint: None,
                        },
                        Arg {
                            pattern: StructurePattern::Bind("a".into()),
                            labeled: Some(None),
                            constraint: None,
                        },
                        Arg {
                            pattern: StructurePattern::Bind("baz".into()),
                            labeled: None,
                            constraint: None,
                        },
                    ]),
                    vargs: None,
                    rtype: None,
                    constraints: Arc::from_iter([]),
                    body: Either::Right("foo".into()),
                }
                .to_expr(),
            ),
        }
        .to_expr()]),
    }
    .to_expr();
    let s = r#"{
let a: fn(?#foo: Number, ?#bar: string, #a: Any, Any) -> string =
  |#foo: Number = 3, #bar = "hello", #a, baz| 'foo
}"#;
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn arrayref0() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::ArrayRef {
            source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr()),
            i: Arc::new(ExprKind::Constant(Value::I64(3)).to_expr()),
        }
        .to_expr()]),
    }
    .to_expr();
    let s = "{foo[3]}";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn arrayref1() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::ArraySlice {
            source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr()),
            start: None,
            end: None,
        }
        .to_expr()]),
    }
    .to_expr();
    let s = "{foo[..]}";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn arrayref2() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::ArraySlice {
            source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr()),
            start: Some(Arc::new(ExprKind::Constant(Value::U64(1)).to_expr())),
            end: None,
        }
        .to_expr()]),
    }
    .to_expr();
    let s = "{foo[1..]}";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn arrayref3() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::ArraySlice {
            source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr()),
            start: None,
            end: Some(Arc::new(ExprKind::Constant(Value::U64(1)).to_expr())),
        }
        .to_expr()]),
    }
    .to_expr();
    let s = "{foo[..1]}";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn arrayref4() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::ArraySlice {
            source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr()),
            start: Some(Arc::new(ExprKind::Constant(Value::U64(1)).to_expr())),
            end: Some(Arc::new(ExprKind::Constant(Value::U64(10)).to_expr())),
        }
        .to_expr()]),
    }
    .to_expr();
    let s = "{foo[1..10]}";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn qop() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::Qop(Arc::new(
            ExprKind::ArraySlice {
                source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr()),
                start: Some(Arc::new(ExprKind::Constant(Value::U64(1)).to_expr())),
                end: Some(Arc::new(ExprKind::Constant(Value::U64(10)).to_expr())),
            }
            .to_expr(),
        ))
        .to_expr()]),
    }
    .to_expr();
    let s = "{foo[1..10]?}";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn tuple0() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::Tuple {
            args: Arc::from_iter([
                ExprKind::Constant(Value::I64(42)).to_expr(),
                ExprKind::Ref { name: ["a"].into() }.to_expr(),
                ExprKind::Apply {
                    function: Arc::new(ExprKind::Ref { name: ["f"].into() }.to_expr()),
                    args: Arc::from_iter([(
                        None,
                        ExprKind::Ref { name: ["b"].into() }.to_expr(),
                    )]),
                }
                .to_expr(),
            ]),
        }
        .to_expr()]),
    }
    .to_expr();
    let s = "{(42, a, f(b))}";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn tuple1() {
    let e = ExprKind::Bind {
        export: false,
        pattern: StructurePattern::Tuple {
            all: None,
            binds: Arc::from_iter([
                StructurePattern::Ignore,
                StructurePattern::Bind(literal!("x")),
                StructurePattern::Bind(literal!("y")),
            ]),
        },
        typ: None,
        value: Arc::new(
            ExprKind::Tuple {
                args: Arc::from_iter([
                    ExprKind::Constant(Value::I64(42)).to_expr(),
                    ExprKind::Ref { name: ["a"].into() }.to_expr(),
                    ExprKind::Apply {
                        function: Arc::new(
                            ExprKind::Ref { name: ["f"].into() }.to_expr(),
                        ),
                        args: Arc::from_iter([(
                            None,
                            ExprKind::Ref { name: ["b"].into() }.to_expr(),
                        )]),
                    }
                    .to_expr(),
                ]),
            }
            .to_expr(),
        ),
    }
    .to_expr();
    let s = "let (_, x, y) = (42, a, f(b))";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn struct0() {
    let e = ExprKind::Bind {
        export: false,
        pattern: StructurePattern::Bind(literal!("a")),
        typ: None,
        value: Arc::new(
            ExprKind::Struct {
                args: Arc::from_iter([
                    ("bar".into(), ExprKind::Ref { name: ["a"].into() }.to_expr()),
                    (
                        "baz".into(),
                        ExprKind::Apply {
                            function: Arc::new(
                                ExprKind::Ref { name: ["f"].into() }.to_expr(),
                            ),
                            args: Arc::from_iter([(
                                None,
                                ExprKind::Ref { name: ["b"].into() }.to_expr(),
                            )]),
                        }
                        .to_expr(),
                    ),
                    ("foo".into(), ExprKind::Constant(Value::I64(42)).to_expr()),
                ]),
            }
            .to_expr(),
        ),
    }
    .to_expr();
    let s = "let a = { foo: 42, bar: a, baz: f(b) }";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn bindstruct() {
    let e = ExprKind::Bind {
        export: false,
        pattern: StructurePattern::Struct {
            all: None,
            exhaustive: true,
            binds: Arc::from_iter([
                (literal!("bar"), StructurePattern::Ignore),
                (literal!("baz"), StructurePattern::Bind(literal!("zam"))),
                (literal!("foo"), StructurePattern::Bind(literal!("foo"))),
            ]),
        },
        typ: None,
        value: Arc::new(
            ExprKind::Struct {
                args: Arc::from_iter([
                    ("bar".into(), ExprKind::Ref { name: ["a"].into() }.to_expr()),
                    (
                        "baz".into(),
                        ExprKind::Apply {
                            function: Arc::new(
                                ExprKind::Ref { name: ["f"].into() }.to_expr(),
                            ),
                            args: Arc::from_iter([(
                                None,
                                ExprKind::Ref { name: ["b"].into() }.to_expr(),
                            )]),
                        }
                        .to_expr(),
                    ),
                    ("foo".into(), ExprKind::Constant(Value::I64(42)).to_expr()),
                ]),
            }
            .to_expr(),
        ),
    }
    .to_expr();
    let s = "let { foo, bar: _, baz: zam } = { foo: 42, bar: a, baz: f(b) }";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn structref() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::StructRef {
            source: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
            field: literal!("foo"),
        }
        .to_expr()]),
    }
    .to_expr();
    let s = "{ a.foo }";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn tupleref() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::TupleRef {
            source: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr()),
            field: 2,
        }
        .to_expr()]),
    }
    .to_expr();
    let s = "{ a.2 }";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn prop0() {
    let s = "`A";
    dbg!(parse_structure_pattern(s).unwrap());
}
