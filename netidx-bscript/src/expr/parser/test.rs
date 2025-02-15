use super::*;
use arcstr::literal;

fn parse_expr(s: &str) -> anyhow::Result<Expr> {
    expr()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

fn parse_typexpr(s: &str) -> anyhow::Result<Type> {
    typexp()
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
            function: ["load"].into(),
            args: Arc::from_iter([(None, ExprKind::Constant(p).to_expr())])
        }
        .to_expr(),
        parse(s).unwrap()
    );
}

#[test]
fn interpolated0() {
    let p = ExprKind::Apply {
        function: ["load"].into(),
        args: Arc::from_iter([(
            None,
            ExprKind::Apply {
                args: Arc::from_iter([
                    (None, ExprKind::Constant(Value::from("/foo/")).to_expr()),
                    (
                        None,
                        ExprKind::Apply {
                            function: ["get"].into(),
                            args: Arc::from_iter([(
                                None,
                                ExprKind::Apply {
                                    args: Arc::from_iter([
                                        (
                                            None,
                                            ExprKind::Ref { name: ["sid"].into() }
                                                .to_expr(),
                                        ),
                                        (
                                            None,
                                            ExprKind::Constant(Value::from("_var"))
                                                .to_expr(),
                                        ),
                                    ]),
                                    function: ["str", "concat"].into(),
                                }
                                .to_expr(),
                            )]),
                        }
                        .to_expr(),
                    ),
                    (None, ExprKind::Constant(Value::from("/baz")).to_expr()),
                ]),
                function: ["str", "concat"].into(),
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
    let s = r#""[true]""#;
    let p = ExprKind::Apply {
        args: Arc::from_iter([(None, ExprKind::Constant(Value::True).to_expr())]),
        function: ["str", "concat"].into(),
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
                                    ExprKind::Apply {
                                        args: Arc::from_iter([(
                                            None,
                                            ExprKind::Constant(Value::True).to_expr(),
                                        )]),
                                        function: ["str", "concat"].into(),
                                    }
                                    .to_expr(),
                                )]),
                                function: ["get"].into(),
                            }
                            .to_expr(),
                        )]),
                        function: ["a"].into(),
                    }
                    .to_expr(),
                )]),
                function: ["a"].into(),
            }
            .to_expr(),
        )]),
        function: ["a"].into(),
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
                    function: ["path", "concat"].into(),
                }
                .to_expr()
            )]),
            function: ["load"].into(),
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
            name: "foo".into(),
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
            name: "foo".into(),
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
                    function: ["load"].into(),
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
                                function: ["load"].into(),
                            }
                            .to_expr(),
                        ),
                    ]),
                    function: ["max"].into(),
                }
                .to_expr(),
            ),
            (
                None,
                ExprKind::Apply { args: Arc::from_iter([]), function: ["rand"].into() }
                    .to_expr(),
            ),
        ]),
        function: ["sum"].into(),
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
fn select() {
    let arms = Arc::from_iter([
        (
            Pattern::Typ {
                tag: Typ::I64.into(),
                bind: literal!("a"),
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
            Pattern::Typ { tag: BitFlags::empty(), bind: literal!("a"), guard: None },
            ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr(),
        ),
    ]);
    let arg = Arc::new(
        ExprKind::Apply {
            args: Arc::from_iter([(
                None,
                ExprKind::Ref { name: ["b"].into() }.to_expr(),
            )]),
            function: ["foo"].into(),
        }
        .to_expr(),
    );
    let exp = ExprKind::Select { arg, arms }.to_expr();
    let s = r#"select foo(b) { I64(a) if a < 10 => a * 2, a => a }"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn pattern() {
    let s = r#"I64(a) if a < 10"#;
    dbg!(super::pattern().easy_parse(position::Stream::new(s)).unwrap());
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
                name: literal!("z"),
                value: Arc::new(ExprKind::Constant(Value::I64(42)).to_expr()),
            }
            .to_expr(),
            ExprKind::Bind {
                typ: None,
                export: false,
                name: literal!("m"),
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
    let exp = ExprKind::Apply {
        function: ModPath::from(["array"]),
        args: Arc::from_iter([
            (
                None,
                ExprKind::Apply {
                    function: ModPath::from(["array"]),
                    args: Arc::from_iter([
                        (None, ExprKind::Constant(Value::from("foo")).to_expr()),
                        (None, ExprKind::Constant(Value::from(42)).to_expr()),
                    ]),
                }
                .to_expr(),
            ),
            (
                None,
                ExprKind::Apply {
                    function: ModPath::from(["array"]),
                    args: Arc::from_iter([
                        (None, ExprKind::Constant(Value::from("bar")).to_expr()),
                        (None, ExprKind::Constant(Value::from(42)).to_expr()),
                    ]),
                }
                .to_expr(),
            ),
        ]),
    }
    .to_expr();
    let s = r#"[["foo", 42], ["bar", 42]]"#;
    assert_eq!(exp, parse(s).unwrap());
}

#[test]
fn doexpr() {
    let exp = ExprKind::Do {
        exprs: Arc::from_iter([
            ExprKind::Bind {
                typ: None,
                export: false,
                name: literal!("baz"),
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
            Arg { labeled: None, name: "foo".into(), constraint: None },
            Arg { labeled: None, name: "bar".into(), constraint: None },
        ]),
        rtype: None,
        vargs: None,
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
        body: Either::Left(Arc::new(
            ExprKind::Lambda {
                args: Arc::from_iter([]),
                rtype: None,
                vargs: None,
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
                    name: "a".into(),
                    constraint: None,
                }]),
                vargs: Some(None),
                rtype: None,
                body: Either::Right("a".into()),
            }
            .to_expr(),
        )]),
        function: ["a"].into(),
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
                    Arg { labeled: None, name: "a".into(), constraint: None },
                    Arg {
                        labeled: None,
                        name: "b".into(),
                        constraint: Some(Type::Set(Arc::from_iter([
                            Type::Primitive(Typ::Null.into()),
                            Type::Ref(["number"].into()),
                        ]))),
                    },
                ]),
                vargs: Some(Some(Type::Primitive(Typ::String.into()))),
                rtype: Some(Type::Bottom),
                body: Either::Right("a".into()),
            }
            .to_expr(),
        )]),
        function: ["a"].into(),
    }
    .to_expr();
    let s = "a(|a, b: [null, number], @args: string| -> _ 'a)";
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn mod_interpolate() {
    let e = ExprKind::Module {
        name: literal!("a"),
        export: false,
        value: Some(Arc::from_iter([ExprKind::Apply {
            function: ["str", "concat"].into(),
            args: Arc::from_iter([
                (None, ExprKind::Constant(Value::from("foo_")).to_expr()),
                (None, ExprKind::Constant(Value::I64(42)).to_expr()),
            ]),
        }
        .to_expr()])),
    }
    .to_expr();
    let s = "mod a{\"foo_[42]\"}";
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
fn labeled_argument_lambda() {
    let e = ExprKind::Do {
        exprs: Arc::from_iter([ExprKind::Bind {
            export: false,
            name: "a".into(),
            typ: Some(Type::Fn(Arc::new(FnType {
                args: Arc::from_iter([
                    FnArgType {
                        label: Some(("foo".into(), true)),
                        typ: Type::Ref(["number"].into()),
                    },
                    FnArgType {
                        label: Some(("bar".into(), true)),
                        typ: Type::Primitive(Typ::String.into()),
                    },
                    FnArgType {
                        label: Some(("a".into(), false)),
                        typ: Type::Ref(["any"].into()),
                    },
                    FnArgType { label: None, typ: Type::Ref(["any"].into()) },
                ]),
                vargs: None,
                rtype: Type::Primitive(Typ::String.into()),
            }))),
            value: Arc::new(
                ExprKind::Lambda {
                    args: Arc::from_iter([
                        Arg {
                            name: "foo".into(),
                            labeled: Some(Some(ExprKind::Constant(3.into()).to_expr())),
                            constraint: Some(Type::Ref(["number"].into())),
                        },
                        Arg {
                            name: "bar".into(),
                            labeled: Some(Some(
                                ExprKind::Constant("hello".into()).to_expr(),
                            )),
                            constraint: None,
                        },
                        Arg { name: "a".into(), labeled: Some(None), constraint: None },
                        Arg { name: "baz".into(), labeled: None, constraint: None },
                    ]),
                    vargs: None,
                    rtype: None,
                    body: Either::Right("foo".into()),
                }
                .to_expr(),
            ),
        }
        .to_expr()]),
    }
    .to_expr();
    let s = r#"{
let a: fn(?#foo: number, ?#bar: string, #a: any, any) -> string =
  |#foo: number = 3, #bar = "hello", #a, baz| 'foo
}"#;
    let pe = parse(s).unwrap();
    assert_eq!(e, pe)
}

#[test]
fn prop0() {
    let s = r#"{ |#bar = "hello"| 'foo }"#;
    dbg!(parse(s).unwrap());
}
