use super::*;
use crate::typ::Type;
use arcstr::literal;

#[allow(unused)]
fn parse_typexpr(s: &str) -> anyhow::Result<Type> {
    typexp()
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[allow(unused)]
fn parse_doc(s: &str) -> anyhow::Result<Option<ArcStr>> {
    doc_comment()
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[allow(unused)]
fn parse_typath(s: &str) -> anyhow::Result<ModPath> {
    typath()
        .skip(spaces())
        .skip(eof())
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[allow(unused)]
fn parse_structure_pattern(s: &str) -> anyhow::Result<StructurePattern> {
    structure_pattern()
        .skip(spaces())
        .skip(eof())
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
            function: Arc::new(ExprKind::Ref { name: ["load"].into() }.to_expr_nopos()),
            args: Arc::from_iter([(None, ExprKind::Constant(p).to_expr_nopos())])
        }
        .to_expr_nopos(),
        parse_one(s).unwrap()
    );
}

#[test]
fn raw_string() {
    let s = r#"r'[]asd[[][]askj'"#;
    let p = Value::String(literal!(r#"[]asd[[][]askj"#));
    assert_eq!(ExprKind::Constant(p).to_expr_nopos(), parse_one(&s).unwrap());
}

#[test]
fn interpolated0() {
    let p = ExprKind::Apply {
        function: Arc::new(ExprKind::Ref { name: ["load"].into() }.to_expr_nopos()),
        args: Arc::from_iter([(
            None,
            ExprKind::StringInterpolate {
                args: Arc::from_iter([
                    ExprKind::Constant(Value::from("/foo/")).to_expr_nopos(),
                    ExprKind::Apply {
                        function: Arc::new(
                            ExprKind::Ref { name: ["get"].into() }.to_expr_nopos(),
                        ),
                        args: Arc::from_iter([(
                            None,
                            ExprKind::StringInterpolate {
                                args: Arc::from_iter([
                                    ExprKind::Ref { name: ["sid"].into() }
                                        .to_expr_nopos(),
                                    ExprKind::Constant(Value::from("_var"))
                                        .to_expr_nopos(),
                                ]),
                            }
                            .to_expr_nopos(),
                        )]),
                    }
                    .to_expr_nopos(),
                    ExprKind::Constant(Value::from("/baz")).to_expr_nopos(),
                ]),
            }
            .to_expr_nopos(),
        )]),
    }
    .to_expr_nopos();
    let s = r#"load("/foo/[get("[sid]_var")]/baz")"#;
    assert_eq!(p, parse_one(s).unwrap());
}

#[test]
fn interpolated1() {
    let s = r#""[true]""#;
    let p = ExprKind::StringInterpolate {
        args: Arc::from_iter([ExprKind::Constant(Value::Bool(true)).to_expr_nopos()]),
    }
    .to_expr_nopos();
    assert_eq!(p, parse_one(s).unwrap());
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
                                        .to_expr_nopos()]),
                                    }
                                    .to_expr_nopos(),
                                )]),
                                function: Arc::new(
                                    ExprKind::Ref { name: ["get"].into() }
                                        .to_expr_nopos(),
                                ),
                            }
                            .to_expr_nopos(),
                        )]),
                        function: Arc::new(
                            ExprKind::Ref { name: ["a"].into() }.to_expr_nopos(),
                        ),
                    }
                    .to_expr_nopos(),
                )]),
                function: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
            }
            .to_expr_nopos(),
        )]),
        function: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
    }
    .to_expr_nopos();
    assert_eq!(p, parse_one(s).unwrap());
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
                            ExprKind::Constant(Value::String(literal!("foo")))
                                .to_expr_nopos()
                        ),
                        (
                            None,
                            ExprKind::Constant(Value::String(literal!("bar")))
                                .to_expr_nopos()
                        ),
                        (None, ExprKind::Ref { name: ["baz"].into() }.to_expr_nopos())
                    ]),
                    function: Arc::new(
                        ExprKind::Ref { name: ["path", "concat"].into() }.to_expr_nopos()
                    ),
                }
                .to_expr_nopos()
            )]),
            function: Arc::new(ExprKind::Ref { name: ["load"].into() }.to_expr_nopos()),
        }
        .to_expr_nopos(),
        parse_one(s).unwrap()
    );
}

#[test]
fn var_ref() {
    assert_eq!(
        ExprKind::Ref { name: ["sum"].into() }.to_expr_nopos(),
        parse_one("sum").unwrap()
    );
}

#[test]
fn letbind() {
    assert_eq!(
        ExprKind::Bind(Arc::new(Bind {
            doc: None,
            export: false,
            typ: None,
            pattern: StructurePattern::Bind(literal!("foo")),
            value: ExprKind::Constant(Value::I64(42)).to_expr_nopos()
        }))
        .to_expr_nopos(),
        parse_one("let foo = 42").unwrap()
    );
}

#[test]
fn doc() {
    assert_eq!(
        Some(literal!(
            " here is a let bind\n there are many like it\n but this one is mine"
        )),
        parse_doc(
            r#"
/// here is a let bind
/// there are many like it
/// but this one is mine
"#
        )
        .unwrap()
    );
}

#[test]
fn letbinddoc() {
    let doc =
        literal!(" here is a let bind\n there are many like it\n but this one is mine");
    assert_eq!(
        ExprKind::Bind(Arc::new(Bind {
            doc: Some(doc),
            export: false,
            typ: None,
            pattern: StructurePattern::Bind(literal!("foo")),
            value: ExprKind::Constant(Value::I64(42)).to_expr_nopos()
        }))
        .to_expr_nopos(),
        parse_one(
            r#"
/// here is a let bind
/// there are many like it
/// but this one is mine
let foo = 42"#
        )
        .unwrap()
    );
}

#[test]
fn typed_letbind() {
    assert_eq!(
        ExprKind::Bind(Arc::new(Bind {
            doc: None,
            export: false,
            typ: Some(Type::Primitive(Typ::I64.into())),
            pattern: StructurePattern::Bind(literal!("foo")),
            value: ExprKind::Constant(Value::I64(42)).to_expr_nopos()
        }))
        .to_expr_nopos(),
        parse_one("let foo: i64 = 42").unwrap()
    );
}

#[test]
fn nested_apply() {
    let src = ExprKind::Apply {
        args: Arc::from_iter([
            (None, ExprKind::Constant(Value::F32(1.)).to_expr_nopos()),
            (
                None,
                ExprKind::Apply {
                    args: Arc::from_iter([(
                        None,
                        ExprKind::Constant(Value::String(literal!("/foo/bar",)))
                            .to_expr_nopos(),
                    )]),
                    function: Arc::new(
                        ExprKind::Ref { name: ["load"].into() }.to_expr_nopos(),
                    ),
                }
                .to_expr_nopos(),
            ),
            (
                None,
                ExprKind::Apply {
                    args: Arc::from_iter([
                        (None, ExprKind::Constant(Value::F32(675.6)).to_expr_nopos()),
                        (
                            None,
                            ExprKind::Apply {
                                args: Arc::from_iter([(
                                    None,
                                    ExprKind::Constant(Value::String(literal!(
                                        "/foo/baz"
                                    )))
                                    .to_expr_nopos(),
                                )]),
                                function: Arc::new(
                                    ExprKind::Ref { name: ["load"].into() }
                                        .to_expr_nopos(),
                                ),
                            }
                            .to_expr_nopos(),
                        ),
                    ]),
                    function: Arc::new(
                        ExprKind::Ref { name: ["max"].into() }.to_expr_nopos(),
                    ),
                }
                .to_expr_nopos(),
            ),
            (
                None,
                ExprKind::Apply {
                    args: Arc::from_iter([]),
                    function: Arc::new(
                        ExprKind::Ref { name: ["rand"].into() }.to_expr_nopos(),
                    ),
                }
                .to_expr_nopos(),
            ),
        ]),
        function: Arc::new(ExprKind::Ref { name: ["sum"].into() }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"sum(f32:1., load("/foo/bar"), max(f32:675.6, load("/foo/baz")), rand())"#;
    assert_eq!(src, parse_one(s).unwrap());
}

#[test]
fn arith_eq() {
    let exp = ExprKind::Eq {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a == b"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_ne() {
    let exp = ExprKind::Ne {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a != b"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_gt() {
    let exp = ExprKind::Gt {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a > b"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_lt() {
    let exp = ExprKind::Lt {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a < b"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_gte() {
    let exp = ExprKind::Gte {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a >= b"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_lte() {
    let exp = ExprKind::Lte {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a <= b"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_add() {
    let exp = ExprKind::Add {
        lhs: Arc::new(
            ExprKind::Add {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
                rhs: Arc::new(ExprKind::Ref { name: ["b"].into() }.to_expr_nopos()),
            }
            .to_expr_nopos(),
        ),
        rhs: Arc::new(ExprKind::Ref { name: ["c"].into() }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a + b + c"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_sub() {
    let exp = ExprKind::Sub {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a - b"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_mul() {
    let exp = ExprKind::Mul {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a * b"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_div() {
    let exp = ExprKind::Div {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a / b"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_paren() {
    let exp = ExprKind::Div {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = r#"a / b"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn arith_nested() {
    let sum = ExprKind::Add {
        lhs: Arc::new(
            ExprKind::Add {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
                rhs: Arc::new(ExprKind::Ref { name: ["b"].into() }.to_expr_nopos()),
            }
            .to_expr_nopos(),
        ),
        rhs: Arc::new(ExprKind::Ref { name: ["c"].into() }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let sub = ExprKind::Sub {
        lhs: Arc::new(
            ExprKind::Sub {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
                rhs: Arc::new(ExprKind::Ref { name: ["b"].into() }.to_expr_nopos()),
            }
            .to_expr_nopos(),
        ),
        rhs: Arc::new(ExprKind::Ref { name: ["c"].into() }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let eq = ExprKind::Eq { lhs: Arc::new(sum), rhs: Arc::new(sub) }.to_expr_nopos();
    let exp = ExprKind::And {
        lhs: Arc::new(eq),
        rhs: Arc::new(
            ExprKind::Not {
                expr: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
            }
            .to_expr_nopos(),
        ),
    }
    .to_expr_nopos();
    let s = r#"((a + b + c) == (a - b - c)) && !a"#;
    assert_eq!(exp, parse_one(s).unwrap());
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
                        lhs: Arc::new(
                            ExprKind::Ref { name: ["a"].into() }.to_expr_nopos(),
                        ),
                        rhs: Arc::new(ExprKind::Constant(Value::I64(10)).to_expr_nopos()),
                    }
                    .to_expr_nopos(),
                ),
            },
            ExprKind::Mul {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
                rhs: Arc::new(ExprKind::Constant(Value::I64(2)).to_expr_nopos()),
            }
            .to_expr_nopos(),
        ),
        (
            Pattern {
                type_predicate: None,
                structure_predicate: StructurePattern::Bind(literal!("a")),
                guard: None,
            },
            ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos(),
        ),
    ]);
    let arg = Arc::new(
        ExprKind::Apply {
            args: Arc::from_iter([(
                None,
                ExprKind::Ref { name: ["b"].into() }.to_expr_nopos(),
            )]),
            function: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr_nopos()),
        }
        .to_expr_nopos(),
    );
    let exp = ExprKind::Select { arg, arms }.to_expr_nopos();
    let s = r#"select foo(b) { i64 as a if a < 10 => a * 2, a => a }"#;
    assert_eq!(exp, parse_one(s).unwrap());
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
                        lhs: Arc::new(
                            ExprKind::Ref { name: ["a"].into() }.to_expr_nopos(),
                        ),
                        rhs: Arc::new(ExprKind::Constant(Value::I64(10)).to_expr_nopos()),
                    }
                    .to_expr_nopos(),
                ),
            },
            ExprKind::Mul {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
                rhs: Arc::new(ExprKind::Constant(Value::I64(2)).to_expr_nopos()),
            }
            .to_expr_nopos(),
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
            ExprKind::Ref { name: ["a"].into() }.to_expr_nopos(),
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
            ExprKind::Ref { name: ["a"].into() }.to_expr_nopos(),
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
            ExprKind::Ref { name: ["a"].into() }.to_expr_nopos(),
        ),
        (
            Pattern {
                type_predicate: Some(Type::Ref {
                    scope: ModPath::root(),
                    name: ["Foo"].into(),
                    params: Arc::from_iter([]),
                }),
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
            ExprKind::Ref { name: ["a"].into() }.to_expr_nopos(),
        ),
        (
            Pattern {
                type_predicate: None,
                structure_predicate: StructurePattern::Bind(literal!("a")),
                guard: None,
            },
            ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos(),
        ),
    ]);
    let arg = Arc::new(
        ExprKind::Apply {
            args: Arc::from_iter([(
                None,
                ExprKind::Ref { name: ["b"].into() }.to_expr_nopos(),
            )]),
            function: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr_nopos()),
        }
        .to_expr_nopos(),
    );
    let exp = ExprKind::Select { arg, arms }.to_expr_nopos();
    let s = r#"
select foo(b) {
    Array<i64> as [a, _, b] if a < 10 => a * 2,
    Array<i64> as [a, b..] => a,
    Array<i64> as [a.., b] => a,
    Array<i64> as [1, 2, 42, a] => a,
    Foo as { foo: 42, bar: _, baz, foobar: a, .. } => a,
    a => a
}"#;
    assert_eq!(exp, parse_one(s).unwrap());
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
                lhs: Arc::new(
                    ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr_nopos(),
                ),
                rhs: Arc::new(ExprKind::Constant(Value::I64(1)).to_expr_nopos()),
            }
            .to_expr_nopos(),
        ),
        deref: false,
    }
    .to_expr_nopos();
    let s = r#"m::foo <- (a + 1)"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn inline_module() {
    let exp = ExprKind::Module {
        name: literal!("foo"),
        export: true,
        value: ModuleKind::Inline(Arc::from_iter([
            ExprKind::Bind(Arc::new(Bind {
                doc: None,
                typ: None,
                export: true,
                pattern: StructurePattern::Bind(literal!("z")),
                value: ExprKind::Constant(Value::I64(42)).to_expr_nopos(),
            }))
            .to_expr_nopos(),
            ExprKind::Bind(Arc::new(Bind {
                doc: None,
                typ: None,
                export: false,
                pattern: StructurePattern::Bind(literal!("m")),
                value: ExprKind::Constant(Value::I64(42)).to_expr_nopos(),
            }))
            .to_expr_nopos(),
        ])),
    }
    .to_expr_nopos();
    let s = r#"pub mod foo {
        pub let z = 42;
        let m = 42
    }"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn external_module() {
    let exp = ExprKind::Module {
        name: literal!("foo"),
        export: true,
        value: ModuleKind::Unresolved,
    }
    .to_expr_nopos();
    let s = r#"pub mod foo"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn usemodule() {
    let exp = ExprKind::Use { name: ModPath::from(["foo"]) }.to_expr_nopos();
    let s = r#"use foo"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn array() {
    let exp = ExprKind::Array {
        args: Arc::from_iter([
            ExprKind::Array {
                args: Arc::from_iter([
                    ExprKind::Constant(Value::from("foo")).to_expr_nopos(),
                    ExprKind::Constant(Value::from(42)).to_expr_nopos(),
                ]),
            }
            .to_expr_nopos(),
            ExprKind::Array {
                args: Arc::from_iter([
                    ExprKind::Constant(Value::from("bar")).to_expr_nopos(),
                    ExprKind::Constant(Value::from(42)).to_expr_nopos(),
                ]),
            }
            .to_expr_nopos(),
        ]),
    }
    .to_expr_nopos();
    let s = r#"[["foo", 42], ["bar", 42]]"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn doexpr() {
    let exp = ExprKind::Do {
        exprs: Arc::from_iter([
            ExprKind::Bind(Arc::new(Bind {
                doc: None,
                typ: None,
                export: false,
                pattern: StructurePattern::Bind(literal!("baz")),
                value: ExprKind::Constant(Value::from(42)).to_expr_nopos(),
            }))
            .to_expr_nopos(),
            ExprKind::Ref { name: ModPath::from(["baz"]) }.to_expr_nopos(),
        ]),
    }
    .to_expr_nopos();
    let s = r#"{ let baz = 42; baz }"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn lambda() {
    let exp = ExprKind::Lambda(Arc::new(Lambda {
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
        body: Either::Left(
            ExprKind::Add {
                lhs: Arc::new(
                    ExprKind::Add {
                        lhs: Arc::new(
                            ExprKind::Ref { name: ["a"].into() }.to_expr_nopos(),
                        ),
                        rhs: Arc::new(
                            ExprKind::Ref { name: ["b"].into() }.to_expr_nopos(),
                        ),
                    }
                    .to_expr_nopos(),
                ),
                rhs: Arc::new(ExprKind::Ref { name: ["c"].into() }.to_expr_nopos()),
            }
            .to_expr_nopos(),
        ),
    }))
    .to_expr_nopos();
    let s = r#"|foo, bar| (a + b + c)"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn nested_lambda() {
    let e = ExprKind::Add {
        lhs: Arc::new(
            ExprKind::Add {
                lhs: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
                rhs: Arc::new(ExprKind::Ref { name: ["b"].into() }.to_expr_nopos()),
            }
            .to_expr_nopos(),
        ),
        rhs: Arc::new(ExprKind::Ref { name: ["c"].into() }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let exp = ExprKind::Lambda(Arc::new(Lambda {
        args: Arc::from_iter([]),
        rtype: None,
        vargs: None,
        constraints: Arc::from_iter([]),
        body: Either::Left(
            ExprKind::Lambda(Arc::new(Lambda {
                args: Arc::from_iter([]),
                rtype: None,
                vargs: None,
                constraints: Arc::from_iter([]),
                body: Either::Left(e),
            }))
            .to_expr_nopos(),
        ),
    }))
    .to_expr_nopos();
    let s = r#"|| || (a + b + c)"#;
    assert_eq!(exp, parse_one(s).unwrap());
}

#[test]
fn apply_lambda() {
    let e = ExprKind::Apply {
        args: Arc::from_iter([(
            None,
            ExprKind::Lambda(Arc::new(Lambda {
                args: Arc::from_iter([Arg {
                    labeled: None,
                    pattern: StructurePattern::Bind("a".into()),
                    constraint: None,
                }]),
                vargs: Some(None),
                rtype: None,
                constraints: Arc::from_iter([]),
                body: Either::Right("a".into()),
            }))
            .to_expr_nopos(),
        )]),
        function: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = "a(|a, @args| 'a)";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn apply_typed_lambda() {
    let e = ExprKind::Apply {
        args: Arc::from_iter([(
            None,
            ExprKind::Lambda(Arc::new(Lambda {
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
                            Type::Ref {
                                scope: ModPath::root(),
                                name: ["Number"].into(),
                                params: Arc::from_iter([]),
                            },
                        ]))),
                    },
                ]),
                vargs: Some(Some(Type::Primitive(Typ::String.into()))),
                rtype: Some(Type::Bottom),
                constraints: Arc::from_iter([]),
                body: Either::Right("a".into()),
            }))
            .to_expr_nopos(),
        )]),
        function: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = "a(|a, b: [null, Number], @args: string| -> _ 'a)";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn mod_interpolate() {
    let e = ExprKind::Module {
        name: literal!("a"),
        export: false,
        value: ModuleKind::Inline(Arc::from_iter([ExprKind::StringInterpolate {
            args: Arc::from_iter([
                ExprKind::Constant(Value::from("foo_")).to_expr_nopos(),
                ExprKind::Constant(Value::I64(42)).to_expr_nopos(),
            ]),
        }
        .to_expr_nopos()])),
    }
    .to_expr_nopos();
    let s = "mod a{\"foo_[42]\"}";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn typed_array() {
    let e = ExprKind::Bind(Arc::new(Bind {
        doc: None,
        export: false,
        pattern: StructurePattern::Bind(literal!("f")),
        typ: None,
        value: ExprKind::Lambda(Arc::new(Lambda {
            args: Arc::from_iter([Arg {
                labeled: None,
                pattern: StructurePattern::Bind("a".into()),
                constraint: Some(Type::Array(Arc::new(Type::TVar(TVar::empty_named(
                    "a".into(),
                ))))),
            }]),
            vargs: None,
            constraints: Arc::from_iter([]),
            rtype: Some(Type::TVar(TVar::empty_named("a".into()))),
            body: Either::Left(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
        }))
        .to_expr_nopos(),
    }))
    .to_expr_nopos();
    let s = "let f = |a: Array<'a>| -> 'a a";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn labeled_argument_lambda() {
    let e = ExprKind::Bind(Arc::new(Bind {
        doc: None,
        export: false,
        pattern: StructurePattern::Bind(literal!("a")),
        typ: Some(Type::Fn(Arc::new(FnType {
            args: Arc::from_iter([
                FnArgType {
                    label: Some(("foo".into(), true)),
                    typ: Type::Ref {
                        scope: ModPath::root(),
                        name: ["Number"].into(),
                        params: Arc::from_iter([]),
                    },
                },
                FnArgType {
                    label: Some(("bar".into(), true)),
                    typ: Type::Primitive(Typ::String.into()),
                },
                FnArgType { label: Some(("a".into(), false)), typ: Type::Any },
                FnArgType { label: None, typ: Type::Any },
            ]),
            vargs: None,
            rtype: Type::Primitive(Typ::String.into()),
            constraints: Arc::new(RwLock::new(vec![])),
        }))),
        value: ExprKind::Lambda(Arc::new(Lambda {
            args: Arc::from_iter([
                Arg {
                    pattern: StructurePattern::Bind("foo".into()),
                    labeled: Some(Some(ExprKind::Constant(3.into()).to_expr_nopos())),
                    constraint: Some(Type::Ref {
                        scope: ModPath::root(),
                        name: ["Number"].into(),
                        params: Arc::from_iter([]),
                    }),
                },
                Arg {
                    pattern: StructurePattern::Bind("bar".into()),
                    labeled: Some(Some(
                        ExprKind::Constant("hello".into()).to_expr_nopos(),
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
        }))
        .to_expr_nopos(),
    }))
    .to_expr_nopos();
    let s = r#"
let a: fn(?#foo: Number, ?#bar: string, #a: Any, Any) -> string =
  |#foo: Number = 3, #bar = "hello", #a, baz| 'foo
"#;
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn arrayref0() {
    let e = ExprKind::ArrayRef {
        source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr_nopos()),
        i: Arc::new(ExprKind::Constant(Value::I64(3)).to_expr_nopos()),
    }
    .to_expr_nopos();
    let s = "foo[3]";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn arrayref1() {
    let e = ExprKind::ArraySlice {
        source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr_nopos()),
        start: None,
        end: None,
    }
    .to_expr_nopos();
    let s = "foo[..]";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn arrayref2() {
    let e = ExprKind::ArraySlice {
        source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr_nopos()),
        start: Some(Arc::new(ExprKind::Constant(Value::U64(1)).to_expr_nopos())),
        end: None,
    }
    .to_expr_nopos();
    let s = "foo[1..]";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn arrayref3() {
    let e = ExprKind::ArraySlice {
        source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr_nopos()),
        start: None,
        end: Some(Arc::new(ExprKind::Constant(Value::U64(1)).to_expr_nopos())),
    }
    .to_expr_nopos();
    let s = "foo[..1]";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn arrayref4() {
    let e = ExprKind::ArraySlice {
        source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr_nopos()),
        start: Some(Arc::new(ExprKind::Constant(Value::U64(1)).to_expr_nopos())),
        end: Some(Arc::new(ExprKind::Constant(Value::U64(10)).to_expr_nopos())),
    }
    .to_expr_nopos();
    let s = "foo[1..10]";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn qop() {
    let e = ExprKind::Qop(Arc::new(
        ExprKind::ArraySlice {
            source: Arc::new(ExprKind::Ref { name: ["foo"].into() }.to_expr_nopos()),
            start: Some(Arc::new(ExprKind::Constant(Value::U64(1)).to_expr_nopos())),
            end: Some(Arc::new(ExprKind::Constant(Value::U64(10)).to_expr_nopos())),
        }
        .to_expr_nopos(),
    ))
    .to_expr_nopos();
    let s = "foo[1..10]?";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn tuple0() {
    let e = ExprKind::Tuple {
        args: Arc::from_iter([
            ExprKind::Constant(Value::I64(42)).to_expr_nopos(),
            ExprKind::Ref { name: ["a"].into() }.to_expr_nopos(),
            ExprKind::Apply {
                function: Arc::new(ExprKind::Ref { name: ["f"].into() }.to_expr_nopos()),
                args: Arc::from_iter([(
                    None,
                    ExprKind::Ref { name: ["b"].into() }.to_expr_nopos(),
                )]),
            }
            .to_expr_nopos(),
        ]),
    }
    .to_expr_nopos();
    let s = "(42, a, f(b))";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn tuple1() {
    let e = ExprKind::Bind(Arc::new(Bind {
        doc: None,
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
        value: ExprKind::Tuple {
            args: Arc::from_iter([
                ExprKind::Constant(Value::I64(42)).to_expr_nopos(),
                ExprKind::Ref { name: ["a"].into() }.to_expr_nopos(),
                ExprKind::Apply {
                    function: Arc::new(
                        ExprKind::Ref { name: ["f"].into() }.to_expr_nopos(),
                    ),
                    args: Arc::from_iter([(
                        None,
                        ExprKind::Ref { name: ["b"].into() }.to_expr_nopos(),
                    )]),
                }
                .to_expr_nopos(),
            ]),
        }
        .to_expr_nopos(),
    }))
    .to_expr_nopos();
    let s = "let (_, x, y) = (42, a, f(b))";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn struct0() {
    let e = ExprKind::Bind(Arc::new(Bind {
        doc: None,
        export: false,
        pattern: StructurePattern::Bind(literal!("a")),
        typ: None,
        value: ExprKind::Struct {
            args: Arc::from_iter([
                ("bar".into(), ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
                (
                    "baz".into(),
                    ExprKind::Apply {
                        function: Arc::new(
                            ExprKind::Ref { name: ["f"].into() }.to_expr_nopos(),
                        ),
                        args: Arc::from_iter([(
                            None,
                            ExprKind::Ref { name: ["b"].into() }.to_expr_nopos(),
                        )]),
                    }
                    .to_expr_nopos(),
                ),
                ("foo".into(), ExprKind::Constant(Value::I64(42)).to_expr_nopos()),
                ("test".into(), ExprKind::Ref { name: ["test"].into() }.to_expr_nopos()),
            ]),
        }
        .to_expr_nopos(),
    }))
    .to_expr_nopos();
    let s = "let a = { foo: 42, bar: a, baz: f(b), test }";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn bindstruct() {
    let e = ExprKind::Bind(Arc::new(Bind {
        doc: None,
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
        value: ExprKind::Struct {
            args: Arc::from_iter([
                ("bar".into(), ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
                (
                    "baz".into(),
                    ExprKind::Apply {
                        function: Arc::new(
                            ExprKind::Ref { name: ["f"].into() }.to_expr_nopos(),
                        ),
                        args: Arc::from_iter([(
                            None,
                            ExprKind::Ref { name: ["b"].into() }.to_expr_nopos(),
                        )]),
                    }
                    .to_expr_nopos(),
                ),
                ("foo".into(), ExprKind::Constant(Value::I64(42)).to_expr_nopos()),
            ]),
        }
        .to_expr_nopos(),
    }))
    .to_expr_nopos();
    let s = "let { foo, bar: _, baz: zam } = { foo: 42, bar: a, baz: f(b) }";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn structref() {
    let e = ExprKind::StructRef {
        source: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
        field: literal!("foo"),
    }
    .to_expr_nopos();
    let s = "a.foo";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn tupleref() {
    let e = ExprKind::TupleRef {
        source: Arc::new(ExprKind::Ref { name: ["a"].into() }.to_expr_nopos()),
        field: 2,
    }
    .to_expr_nopos();
    let s = "a.2";
    let pe = parse_one(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn prop0() {
    let s = r#"
  re::split(#pat:r'\\s*', r'foo, bar, baz')
"#;
    dbg!(parse_one(s).unwrap());
}
