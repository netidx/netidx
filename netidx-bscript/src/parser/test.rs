use super::*;

#[test]
fn escaped_string() {
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
}

#[test]
fn interpolated0() {
    let p = ExprKind::Apply {
        function: ["load"].into(),
        args: Arc::from_iter([ExprKind::Apply {
            args: Arc::from_iter([
                ExprKind::Constant(Value::from("/foo/")).to_expr(),
                ExprKind::Apply {
                    function: ["get"].into(),
                    args: Arc::from_iter([ExprKind::Apply {
                        args: Arc::from_iter([
                            ExprKind::Ref { name: ["sid"].into() }.to_expr(),
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
}

#[test]
fn interpolated1() {
    let s = r#""[true]""#;
    let p = ExprKind::Apply {
        args: Arc::from_iter([ExprKind::Constant(Value::True).to_expr()]),
        function: ["str", "concat"].into(),
    }
    .to_expr();
    assert_eq!(p, parse_expr(s).unwrap());
}

#[test]
fn interpolated2() {
    let s = r#"a(a(a(get("[true]"))))"#;
    let p = ExprKind::Apply {
        args: Arc::from_iter([ExprKind::Apply {
            args: Arc::from_iter([ExprKind::Apply {
                args: Arc::from_iter([ExprKind::Apply {
                    args: Arc::from_iter([ExprKind::Apply {
                        args: Arc::from_iter([ExprKind::Constant(Value::True).to_expr()]),
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
fn apply_path() {
    let s = r#"load(path::concat("foo", "bar", baz))"#;
    assert_eq!(
        ExprKind::Apply {
            args: Arc::from_iter([ExprKind::Apply {
                args: Arc::from_iter([
                    ExprKind::Constant(Value::String(Chars::from("foo"))).to_expr(),
                    ExprKind::Constant(Value::String(Chars::from("bar"))).to_expr(),
                    ExprKind::Ref { name: ["baz"].into() }.to_expr()
                ]),
                function: ["path", "concat"].into(),
            }
            .to_expr()]),
            function: ["load"].into(),
        }
        .to_expr(),
        parse_expr(s).unwrap()
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
            name: "foo".into(),
            value: Arc::new(ExprKind::Constant(Value::I64(42)).to_expr())
        }
        .to_expr(),
        parse_expr("let foo = 42;").unwrap()
    );
}

#[test]
fn nested_apply() {
    let src = ExprKind::Apply {
        args: Arc::from_iter([
            ExprKind::Constant(Value::F32(1.)).to_expr(),
            ExprKind::Apply {
                args: Arc::from_iter([ExprKind::Constant(Value::String(Chars::from(
                    "/foo/bar",
                )))
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
    let s = r#"sum(f32:1., load("/foo/bar"), max(f32:675.6, load("/foo/baz")), rand())"#;
    assert_eq!(src, parse_expr(s).unwrap());
}

#[test]
fn arith_eq() {
    let exp = ExprKind::Eq {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"(a = b)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_ne() {
    let exp = ExprKind::Ne {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"(a != b)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_gt() {
    let exp = ExprKind::Gt {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"(a > b)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_lt() {
    let exp = ExprKind::Lt {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"(a < b)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_gte() {
    let exp = ExprKind::Gte {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"(a >= b)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_lte() {
    let exp = ExprKind::Lte {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"(a <= b)"#;
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
    let s = r#"(a + b + c)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_sub() {
    let exp = ExprKind::Sub {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"(a - b)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_mul() {
    let exp = ExprKind::Mul {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"(a * b)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_div() {
    let exp = ExprKind::Div {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"(a / b)"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn arith_paren() {
    let exp = ExprKind::Div {
        lhs: Arc::new(ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr()),
        rhs: Arc::new(ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr()),
    }
    .to_expr();
    let s = r#"(a / b)"#;
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
    let s = r#"(((a + b + c) = (a - b - c)) && (!a))"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn select() {
    let arms = Arc::from_iter([
        (
            ExprKind::Ref { name: ModPath::from(["a"]) }.to_expr(),
            ExprKind::Ref { name: ModPath::from(["foo"]) }.to_expr(),
        ),
        (
            ExprKind::Ref { name: ModPath::from(["b"]) }.to_expr(),
            ExprKind::Ref { name: ModPath::from(["bar"]) }.to_expr(),
        ),
    ]);
    let exp = ExprKind::Select { arms }.to_expr();
    let s = r#"select { a => foo, b => bar }"#;
    assert_eq!(exp, parse_expr(s).unwrap());
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
    let s = r#"m::foo <- (a + 1);"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn inline_module() {
    let exp = ExprKind::Module {
        name: Chars::from("foo"),
        export: true,
        value: Some(Arc::from_iter([
            ExprKind::Bind {
                export: true,
                name: Chars::from("z"),
                value: Arc::new(ExprKind::Constant(Value::I64(42)).to_expr()),
            }
            .to_expr(),
            ExprKind::Bind {
                export: false,
                name: Chars::from("m"),
                value: Arc::new(ExprKind::Constant(Value::I64(42)).to_expr()),
            }
            .to_expr(),
        ])),
    }
    .to_expr();
    let s = r#"pub mod foo {
        pub let z = 42;
        let m = 42;
    }"#;
    assert_eq!(exp, parse_modexpr(s).unwrap());
}

#[test]
fn external_module() {
    let exp = ExprKind::Module { name: Chars::from("foo"), export: true, value: None }
        .to_expr();
    let s = r#"pub mod foo;"#;
    assert_eq!(exp, parse_modexpr(s).unwrap());
}

#[test]
fn usemodule() {
    let exp = ExprKind::Use { name: ModPath::from(["foo"]) }.to_expr();
    let s = r#"use foo;"#;
    assert_eq!(exp, parse_modexpr(s).unwrap());
}

#[test]
fn alist() {
    let exp = ExprKind::Apply {
        function: ModPath::from(["array"]),
        args: Arc::from_iter([
            ExprKind::Apply {
                function: ModPath::from(["array"]),
                args: Arc::from_iter([
                    ExprKind::Constant(Value::from("foo")).to_expr(),
                    ExprKind::Constant(Value::from(42)).to_expr(),
                ]),
            }
            .to_expr(),
            ExprKind::Apply {
                function: ModPath::from(["array"]),
                args: Arc::from_iter([
                    ExprKind::Constant(Value::from("bar")).to_expr(),
                    ExprKind::Constant(Value::from(42)).to_expr(),
                ]),
            }
            .to_expr(),
        ]),
    }
    .to_expr();
    let s = r#"{foo: 42, bar: 42}"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn array() {
    let exp = ExprKind::Apply {
        function: ModPath::from(["array"]),
        args: Arc::from_iter([
            ExprKind::Apply {
                function: ModPath::from(["array"]),
                args: Arc::from_iter([
                    ExprKind::Constant(Value::from("foo")).to_expr(),
                    ExprKind::Constant(Value::from(42)).to_expr(),
                ]),
            }
            .to_expr(),
            ExprKind::Apply {
                function: ModPath::from(["array"]),
                args: Arc::from_iter([
                    ExprKind::Constant(Value::from("bar")).to_expr(),
                    ExprKind::Constant(Value::from(42)).to_expr(),
                ]),
            }
            .to_expr(),
        ]),
    }
    .to_expr();
    let s = r#"[["foo", 42], ["bar", 42]]"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn doexpr() {
    let exp = ExprKind::Do {
        exprs: Arc::from_iter([
            ExprKind::Bind {
                export: false,
                name: Chars::from("baz"),
                value: Arc::new(ExprKind::Constant(Value::from(42)).to_expr()),
            }
            .to_expr(),
            ExprKind::Ref { name: ModPath::from(["baz"]) }.to_expr(),
        ]),
    }
    .to_expr();
    let s = r#"{ let baz = 42; baz }"#;
    assert_eq!(exp, parse_expr(s).unwrap());
}

#[test]
fn lambda() {
    let exp = ExprKind::Lambda {
        args: Arc::from_iter(["foo".into(), "bar".into()]),
        vargs: false,
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
        vargs: false,
        body: Either::Left(Arc::new(
            ExprKind::Lambda {
                args: Arc::from_iter([]),
                vargs: false,
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
        args: Arc::from_iter([ExprKind::Lambda {
            args: Arc::from_iter(["a".into()]),
            vargs: true,
            body: Either::Right("a".into()),
        }
        .to_expr()]),
        function: ["a"].into(),
    }
    .to_expr();
    let s = "a(|a, @args| 'a)";
    let pe = parse_expr(s).unwrap();
    assert_eq!(e, pe)
}

/*
#[test]
fn mod_interpolate() {
    let e = ExprKind::Module {
        name: Chars::from("a"),
        export: false,
        value: Some(Arc::from_iter([ExprKind::Apply {
            function: ["str", "concat"].into(),
            args: Arc::from_iter([
                ExprKind::Constant(Value::from("foo_")).to_expr(),
                ExprKind::Constant(Value::I64(42)).to_expr(),
            ]),
        }
        .to_expr()])),
    }
    .to_expr();
    let s = "mod a{\"foo_[42]\"}";
    let pe = parse_modexpr(s).unwrap();
    assert_eq!(e, pe)
}
*/
