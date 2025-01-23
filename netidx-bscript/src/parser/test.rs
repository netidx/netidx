use super::*;

#[test]
fn interp_parse() {
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
    let p = ExprKind::Apply {
        function: ["load"].into(),
        args: Arc::from_iter([ExprKind::Apply {
            args: Arc::from_iter([
                ExprKind::Constant(Value::from("/foo/")).to_expr(),
                ExprKind::Apply {
                    function: ["get"].into(),
                    args: Arc::from_iter([ExprKind::Apply {
                        args: Arc::from_iter([
                            ExprKind::Apply {
                                function: ["get"].into(),
                                args: Arc::from_iter([ExprKind::Constant(
                                    Value::from("sid"),
                                )
                                                      .to_expr()]),
                            }
                            .to_expr(),
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
    let s = r#""[true]""#;
    let p = ExprKind::Apply {
        args: Arc::from_iter([ExprKind::Constant(Value::True).to_expr()]),
        function: ["str", "concat"].into(),
    }
    .to_expr();
    assert_eq!(p, parse_expr(s).unwrap());
    let s = r#"a(a(a(get("[true]"))))"#;
    let p = ExprKind::Apply {
        args: Arc::from_iter([ExprKind::Apply {
            args: Arc::from_iter([ExprKind::Apply {
                args: Arc::from_iter([ExprKind::Apply {
                    args: Arc::from_iter([ExprKind::Apply {
                        args: Arc::from_iter([
                            ExprKind::Constant(Value::True).to_expr()
                        ]),
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
fn expr_parse() {
    let s = r#"load(concat_path("foo", "bar", baz))"#;
    assert_eq!(
        ExprKind::Apply {
            args: Arc::from_iter([ExprKind::Apply {
                args: Arc::from_iter([
                    ExprKind::Constant(Value::String(Chars::from("foo"))).to_expr(),
                    ExprKind::Constant(Value::String(Chars::from("bar"))).to_expr(),
                    ExprKind::Apply {
                        args: Arc::from_iter([ExprKind::Constant(Value::String(
                            Chars::from("baz")
                        ))
                                              .to_expr()]),
                        function: ["get"].into(),
                    }
                    .to_expr()
                ]),
                function: ["path", "concat"].into(),
            }
                                  .to_expr()]),
            function: ["load"].into(),
        }
        .to_expr(),
        parse_expr(s).unwrap()
    );
    assert_eq!(
        ExprKind::Ref { name: ["sum"].into() }.to_expr(),
        parse_expr("sum").unwrap()
    );
    assert_eq!(
        ExprKind::Bind {
            export: false,
            name: "foo".into(),
            value: Arc::new(ExprKind::Constant(Value::I64(42)).to_expr())
        }
        .to_expr(),
        parse_expr("let foo = 42;").unwrap()
    );
    let src = ExprKind::Apply {
        args: Arc::from_iter([
            ExprKind::Constant(Value::F32(1.)).to_expr(),
            ExprKind::Apply {
                args: Arc::from_iter([ExprKind::Constant(Value::String(
                    Chars::from("/foo/bar"),
                ))
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
    let chs =
        r#"sum(f32:1., load("/foo/bar"), max(f32:675.6, load("/foo/baz")), rand())"#;
    assert_eq!(src, parse_expr(chs).unwrap());
}
