use super::*;

fn nested(open: &str, inner: &str, close: &str, n: usize) -> String {
    format!("{}{inner}{}", open.repeat(n), close.repeat(n))
}

/// A deep value parses on a small stack, growing it.
#[test]
fn deep_values_parse_on_a_small_stack() {
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(|| {
            let max = max_nesting();
            for (open, inner, close) in
                [("[", "1", "]"), ("{1 => ", "1", "}"), ("error:", "1", "")]
            {
                assert!(parse_value(&nested(open, inner, close, max - 1)).is_ok());
                let e = parse_value(&nested(open, inner, close, max)).unwrap_err();
                assert!(format!("{e}").contains("value nesting too deep"), "{e}");
            }
        })
        .unwrap()
        .join()
        .unwrap()
}

/// What `parse_value`'s failures say.
#[test]
fn failure_messages() {
    let msg = |s: &str| format!("{}", parse_value(s).unwrap_err());
    assert_eq!(
        msg("[1 2]"),
        "Parse error at line: 1, column: 4\nUnexpected `2`\nExpected `,` or `]`\n"
    );
    assert_eq!(
        msg("[,]"),
        "Parse error at line: 1, column: 2\nUnexpected `,`\nExpected a value or `]`\n"
    );
    assert_eq!(
        msg("\"a\\q\""),
        "Parse error at line: 1, column: 4\nUnexpected `q`\nExpected `n`, `r`, `t`, `0`, `\\` or `\"`\n"
    );
    assert_eq!(
        msg("u8:300"),
        "Parse error at line: 1, column: 7\ninvalid unsigned integer\n"
    );
    assert_eq!(
        msg("u64x"),
        "Parse error at line: 1, column: 4\nUnexpected `x`\nExpected `:`\n"
    );
    assert_eq!(
        msg("1 2"),
        "Parse error at line: 1, column: 3\nUnexpected `2`\nExpected end of input\n"
    );
    assert_eq!(
        msg("[\n1\n2]"),
        "Parse error at line: 3, column: 1\nUnexpected `2`\nExpected `,` or `]`\n"
    );
    assert_eq!(msg("truex"), "Parse error at line: 1, column: 5\nUnexpected `x`\n");
    assert_eq!(
        msg(""),
        "Parse error at line: 1, column: 1\nUnexpected end of input\nExpected a value\n"
    );
}
