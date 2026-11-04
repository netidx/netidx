//! The value grammar over every value the generator makes, printed and
//! read back, hand-picked edge cases, values nested around the limit, and
//! truncated and corrupted copies of all of them.

use super::*;
use crate::{Map, abstract_type::AbstractWrapper};
use bytes::{Buf, BufMut};
use chrono::TimeZone;
use netidx_core::pack::PackError;
use proptest::{
    collection, prelude::*, strategy::ValueTree, string::string_regex,
    test_runner::TestRunner,
};
use std::{
    fmt::{self, Write},
    sync::LazyLock,
};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Opaque(u64, Vec<u8>);

impl Pack for Opaque {
    fn encoded_len(&self) -> usize {
        self.0.encoded_len() + self.1.encoded_len()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        self.0.encode(buf)?;
        self.1.encode(buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        Ok(Opaque(Pack::decode(buf)?, Pack::decode(buf)?))
    }
}

static OPAQUE: LazyLock<AbstractWrapper<Opaque>> = LazyLock::new(|| {
    Abstract::register::<Opaque>(Uuid::from_u128(0x7a1e_9a25_e500_0001)).unwrap()
});

/// Whether `c` prints in a string as a `\u{..}` escape, which the grammar
/// refuses (value-unicode-escape-01).
fn escaped_generically(c: char) -> bool {
    c.is_control() && !"\n\r\t\0".contains(c)
}

/// Every scalar, its strings without the characters that print as
/// `\u{..}` escapes unless `generic`.
fn leaf(generic: bool) -> impl Strategy<Value = Value> {
    let string = move |s: String| {
        let s: String = match generic {
            true => s,
            false => s.chars().filter(|c| !escaped_generically(*c)).collect(),
        };
        Value::String(s.into())
    };
    let datetime = (
        DateTime::<Utc>::MIN_UTC.timestamp()..DateTime::<Utc>::MAX_UTC.timestamp(),
        0..1_000_000_000u32,
    )
        .prop_map(|(s, ns)| Value::DateTime(Arc::new(Utc.timestamp_opt(s, ns).unwrap())));
    let escapes =
        string_regex("[a-z\"\\\\\n\r\t\0\u{1}\u{7f} é💖\\[\\]=]{0,12}").unwrap();
    prop_oneof![
        any::<u8>().prop_map(Value::U8),
        any::<i8>().prop_map(Value::I8),
        any::<u16>().prop_map(Value::U16),
        any::<i16>().prop_map(Value::I16),
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
        any::<[u8; 16]>().prop_map(|a| Value::Decimal(Arc::new(Decimal::deserialize(a)))),
        datetime,
        (any::<u64>(), 0..1_000_000_000u32)
            .prop_map(|(s, ns)| Value::Duration(Arc::new(Duration::new(s, ns)))),
        any::<String>().prop_map(string),
        escapes.prop_map(string),
        any::<Vec<u8>>().prop_map(|b| Value::Bytes(PBytes::new(Bytes::from(b)))),
        Just(Value::Bool(true)),
        Just(Value::Bool(false)),
        Just(Value::Null),
        (any::<u64>(), any::<Vec<u8>>()).prop_map(|(n, b)| OPAQUE.wrap(Opaque(n, b))),
    ]
}

fn tree(generic: bool) -> impl Strategy<Value = Value> {
    leaf(generic).prop_recursive(10, 1000, 100, |inner| {
        prop_oneof![
            collection::vec(inner.clone(), 0..100)
                .prop_map(|e| Value::Array(ValArray::from(e))),
            inner.clone().prop_map(|v| Value::Error(v.into())),
            collection::vec((inner.clone(), inner), 0..100)
                .prop_map(|v| Value::Map(Map::from_iter(v))),
        ]
    })
}

struct NoTypes<'a>(&'a Value);

impl fmt::Display for NoTypes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_notyp(f)
    }
}

/// Every value printed both ways: `leaves` scalars alone, then `trees`
/// nested values.
fn printed(leaves: usize, trees: usize) -> Vec<String> {
    let mut runner = TestRunner::deterministic();
    let mut out = vec![];
    let mut add = |v: Value| {
        out.push(format!("{v}"));
        out.push(format!("{}", NoTypes(&v)));
    };
    let (leaf, tree) = (leaf(true), tree(true));
    for _ in 0..leaves {
        add(leaf.new_tree(&mut runner).unwrap().current())
    }
    for _ in 0..trees {
        add(tree.new_tree(&mut runner).unwrap().current())
    }
    out
}

fn nested(open: &str, inner: &str, close: &str, n: usize) -> String {
    format!("{}{inner}{}", open.repeat(n), close.repeat(n))
}

fn hand_picked() -> Vec<String> {
    [
        // strings and their escapes
        r#""""#,
        r#""abc""#,
        r#""\n\r\t\0\\\"""#,
        r#""\q""#,
        r#""\u{41}""#,
        r#""\"#,
        r#""a\"#,
        "\"a\u{1}b\"",
        "\"\n\"",
        r#""💖é""#,
        r#""abc"#,
        r#""a"b""#,
        r#""\[""#,
        // every typed literal, good and bad
        "u8:5",
        "u16:5",
        "u32:5",
        "u64:5",
        "i8:-5",
        "i16:-5",
        "i32:-5",
        "i64:-5",
        "v32:5",
        "v64:5",
        "z32:-5",
        "z64:-5",
        "f32:1.5",
        "f64:1.5",
        "decimal:1.5",
        "datetime:\"2020-01-02T03:04:05Z\"",
        "datetime:\"2020-01-01 00:00:00 UTC\"",
        "duration:1.5s",
        "bytes:YQ==",
        "abstract:",
        "error:\"e\"",
        "u8 : 5",
        "u8\n:\n5",
        "u8:\u{2003}5",
        "u8",
        "u8 ",
        "u8 x",
        "u6",
        "u64x",
        "u8x:5",
        "i6",
        "i64",
        "dec",
        "decimalx",
        "datetimex:",
        "durationx",
        "bytesx",
        "abstractx",
        "errorx",
        "f3",
        "f32x",
        "f32:x",
        "f64:1",
        "z64:",
        "v64:-1",
        "u8:",
        "u8:x",
        "u8:-1",
        "u8:-0",
        "u8:256",
        "u8:255",
        "i8:128",
        "i8:-128",
        "i8:-129",
        "true",
        "false",
        "null",
        "truex",
        "true_",
        "trueé",
        "true:",
        "true ",
        "nul",
        "nulll",
        "True",
        "f",
        "e",
        "d",
        "b",
        "a",
        "u",
        "i",
        "v",
        "z",
        "n",
        "t",
        // radix prefixes
        "0x",
        "0x1F",
        "0X1f",
        "-0x1F",
        "0b101",
        "0B2",
        "0o17",
        "0o8",
        "u8:0x100",
        "u8:0xff",
        "u8:0XFF",
        "i8:-0x80",
        "i8:0x80",
        "i8:-0x81",
        "u64:0xffffffffffffffff",
        "u64:0x10000000000000000",
        "-0b",
        "-0o7",
        "0x1.5",
        "i64:-0b1111",
        "0xg",
        "u16:0b",
        "u16:0B1010",
        "u32:0O77",
        "i64:0o",
        "-0x",
        "0x-1",
        // floats
        "1.",
        "1.5",
        "-1.5",
        "1e5",
        "1e-5",
        "1.e5",
        "1.5e",
        "1.5e-",
        "1e",
        "1e-",
        "1e+5",
        ".5",
        "-.5",
        "1.5.5",
        "f32:3.4e38",
        "f32:3.5e38",
        "f64:1e309",
        "f64:-1e309",
        "-0.",
        "00.5",
        "1E5",
        "f32:1",
        "f32:-",
        "f64:1.5e",
        "f64:1e",
        "decimal:1e5",
        "decimal:1e400",
        "decimal:-0.0",
        "decimal:5",
        "decimal:79228162514264337593543950336.0",
        "decimal:x",
        "decimal:1.",
        "100000000000000000000",
        "100000000000000000000.",
        "-",
        // integers
        "9223372036854775807",
        "9223372036854775808",
        "-9223372036854775808",
        "-9223372036854775809",
        "u64:18446744073709551616",
        "--1",
        "+1",
        "007",
        "1_000",
        "1x",
        "-x",
        // durations
        "duration:1.ns",
        "duration:1.us",
        "duration:1.µs",
        "duration:1.ms",
        "duration:1.s",
        "duration:1.m",
        "duration:1.h",
        "duration:1.d",
        "duration:1.M",
        "duration:1.y",
        "duration:1.5ms",
        "duration:1e3s",
        "duration:-1.s",
        "duration:-0.s",
        "duration:1.x",
        "duration:1.n",
        "duration:1.u",
        "duration:1.µ",
        "duration:1",
        "duration:1s",
        "duration:1.mx",
        "duration:1.msx",
        "duration:1e99999999y",
        "duration:18446744073709551616.s",
        "duration:0.0000000001ns",
        "duration:584942417355.y",
        "duration:5q",
        "duration:",
        "duration: 1.s",
        "duration:1. s",
        // base64
        "bytes:",
        "bytes:==",
        "bytes:=",
        "bytes:AAAA",
        "bytes:AAA",
        "bytes:AAA=",
        "bytes:@",
        "bytes:YQ",
        "bytes:Y",
        "bytes: YQ==",
        "abstract:AAAA",
        "abstract:=",
        // datetimes
        "datetime:\"x\"",
        "datetime:x",
        "datetime:\"2020",
        "datetime:\"\\q\"",
        "datetime:\"2020-13-01T00:00:00Z\"",
        // errors
        "error:",
        "error",
        "error:1",
        "error:error:error:1",
        "error: \"x\"",
        "error:[1]",
        "error :1",
        // arrays and maps
        "[]",
        "[ ]",
        "[\n]",
        "[1]",
        "[ 1 ]",
        "[1,]",
        "[1, ]",
        "[1 ,]",
        "[1,,]",
        "[,]",
        "[1 2]",
        "[1",
        "[1,",
        "[1, 2",
        "[[]]",
        "[\n1\n,\n2\n]",
        "[",
        "]",
        "[1]]",
        "{}",
        "{ }",
        "{1 => 2}",
        "{1=>2}",
        "{1 = > 2}",
        "{1 =>}",
        "{1}",
        "{1 =}",
        "{1 => 2,}",
        "{1 => 2 3}",
        r#"{"a" => [1, {2 => 3}]}"#,
        "{1 => 2, 1 => 3}",
        "{",
        "{1 => ",
        "{,}",
        "[truex]",
        "[u8x]",
        "[ x]",
        // whitespace, and nothing
        "",
        " ",
        "  1  ",
        "\t1",
        "\u{a0}1",
        "\u{2003}[\u{2003}1\u{2003},\u{2003}]",
        "1\u{2028}",
        "1 2",
        "x",
        "@",
        "\u{1}",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

/// Values nested at, around and well past the limit.
fn deep() -> Vec<String> {
    let max = max_nesting();
    let mut cases = vec![];
    for n in [max - 1, max, max + 1] {
        cases.push(nested("[", "1", "]", n));
        cases.push(nested("[", "", "]", n));
        cases.push(nested("{1 => ", "1", "}", n));
        cases.push(nested("{", "1 => 1", "}", n));
        cases.push(nested("error:", "1", "", n));
        cases.push(nested("[error:{0 => ", "1", "}]", n / 3));
        cases.push(nested("[error:{0 => ", "1", "}]", n / 3 + 1));
        cases.push(nested("[ ", "1", " ]", n));
        cases.push(nested("[", "1", "", n));
        cases.push(nested("[", "x", "]", n));
    }
    cases.push(nested("[", "1", "]", 3 * max));
    cases
}

/// The deterministic random numbers that pick truncations and edits.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

/// `s` cut short and corrupted: every cut, and an edit at every char,
/// of a short `s`; `n` of each of a long one.
fn damaged(s: &str, n: usize, rng: &mut Rng, out: &mut Vec<String>) {
    const CHARS: [char; 16] = [
        ']', '[', ',', '"', '\\', ' ', 'x', '0', '-', '.', 'e', ':', '}', '=', '\n', 'é',
    ];
    let bounds: Vec<usize> = s.char_indices().map(|(i, _)| i).collect();
    if bounds.is_empty() {
        return;
    }
    let mut at = |k: usize| match bounds.len() <= n {
        true => bounds[k % bounds.len()],
        false => bounds[rng.below(bounds.len())],
    };
    let k = n.min(bounds.len());
    for j in 0..k {
        let i = at(j);
        out.push(s[..i].to_string());
        let c = CHARS[(i + j) % CHARS.len()];
        let next = i + s[i..].chars().next().unwrap().len_utf8();
        out.push(format!("{}{c}{}", &s[..i], &s[next..]));
        out.push(format!("{}{c}{}", &s[..i], &s[i..]));
        out.push(format!("{}{}", &s[..i], &s[next..]));
    }
}

/// The same value: equal, and printed alike, which also tells floats'
/// signs and decimals' scales apart.
fn same_value(a: &Value, b: &Value) -> bool {
    a == b && format!("{a}") == format!("{b}")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// A value printed with its types reads back as itself, all of its
    /// text.
    #[test]
    fn printed_values_read_back(v in tree(false)) {
        let s = format!("{v}");
        let (w, n) = value(&s).unwrap_or_else(|f| panic!("{s:?}: {}", f.in_text(&s)));
        prop_assert!(same_value(&v, &w), "{s:?} read as {w}");
        prop_assert_eq!(n, s.len());
    }
}

/// Whether a parse of `s` may stop at `n`.
fn on_char(s: &str, n: usize) -> bool {
    n <= s.len() && s.is_char_boundary(n)
}

/// `value` on `s` stops on a char boundary, and `parse_value` accepts
/// exactly what `value` reads with only whitespace after it, failing
/// with the message of where `value` or the end stopped.
fn check_whole(s: &str) {
    let whole = parse_value(s);
    let failed = match value(s) {
        Ok((v, n)) => {
            assert!(on_char(s, n), "{s:?} {n}");
            match end(&s[n..]) {
                Ok(_) => {
                    let w = whole.unwrap_or_else(|e| panic!("{s:?}: {e}"));
                    return assert!(same_value(&v, &w), "{s:?}");
                }
                Err(f) => f.after(n),
            }
        }
        Err(f) => f,
    };
    assert!(on_char(s, failed.offset), "{s:?} {failed:?}");
    let e = whole.err().unwrap_or_else(|| panic!("{s:?} read whole"));
    assert_eq!(format!("{e}"), format!("{}", failed.in_text(s)), "{s:?}")
}

const GRAPHIX_MUST_ESC: [char; 4] = ['"', '\\', '[', ']'];
static GRAPHIX_ESC: LazyLock<Escape> = LazyLock::new(|| {
    const NAMED: [(char, &str); 4] = [('\n', "n"), ('\r', "r"), ('\t', "t"), ('\0', "0")];
    let esc: Vec<char> =
        GRAPHIX_MUST_ESC.into_iter().chain(NAMED.map(|(c, _)| c)).collect();
    Escape::new('\\', &esc, &NAMED, None).unwrap()
});
const RPC_MUST_ESC: [char; 2] = ['\\', '='];
static RPC_ESC: LazyLock<Escape> =
    LazyLock::new(|| Escape::new('\\', &['\\', '='], &[], None).unwrap());

/// The starts of `s`'s suffixes another parser might hand one of these:
/// after a `:`, quote, bracket, comma, space or arrow.
fn part_starts(s: &str) -> impl Iterator<Item = usize> + '_ {
    std::iter::once(0).chain(
        s.char_indices()
            .filter(|(_, c)| matches!(c, ':' | '"' | '[' | '{' | ',' | ' ' | '>'))
            .map(|(i, c)| i + c.len_utf8()),
    )
}

/// The entry points other parsers call on part of their text, on `s`:
/// each stops on a char boundary, and `escaped_string` where the text
/// ends or at a character it must escape.
fn check_parts(s: &str) {
    fn stop<T>(s: &str, r: Result<(T, usize), Failure>) -> usize {
        let n = match r {
            Ok((_, n)) => n,
            Err(f) => {
                assert!(on_char(s, f.offset), "{s:?} {f:?}");
                let _ = f.in_text(s).to_string();
                f.offset
            }
        };
        assert!(on_char(s, n), "{s:?} {n}");
        n
    }
    stop(s, int::<i64>(s));
    stop(s, int::<i8>(s));
    stop(s, int::<u8>(s));
    stop(s, int::<usize>(s));
    stop(s, duration(s));
    for (must_esc, esc) in [
        (&VAL_MUST_ESC[..], &*VAL_ESC),
        (&GRAPHIX_MUST_ESC[..], &*GRAPHIX_ESC),
        (&RPC_MUST_ESC[..], &*RPC_ESC),
    ] {
        let r = escaped_string(s, must_esc, esc);
        let read = r.is_ok();
        let n = stop(s, r);
        assert!(!read || s[n..].is_empty() || s[n..].starts_with(must_esc), "{s:?} {n}");
    }
}

/// Every text reads consistently: the hand-picked cases, values nested
/// around the limit, every value the generator makes printed with and
/// without types, and truncated and corrupted copies of all of them,
/// whole, and from each place another parser might hand one of them.
/// `NV_PARSER_CORPUS=full` reads a larger corpus.
#[test]
fn every_text_reads_consistently() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let full = std::env::var("NV_PARSER_CORPUS").is_ok_and(|c| c == "full");
    let (leaves, trees, edits, long_edits) =
        if full { (3000, 200, 24, 24) } else { (400, 30, 8, 2) };
    let cases = hand_picked();
    let printed = printed(leaves, trees);
    let mut damage = vec![];
    for s in cases.iter().chain(printed.iter()) {
        damaged(s, if s.len() > 200 { long_edits } else { edits }, &mut rng, &mut damage)
    }
    for s in cases.iter().chain(&deep()).chain(&printed).chain(&damage) {
        check_whole(s)
    }
    let printed_leaves = &printed[..2 * leaves];
    for s in cases.iter().chain(printed_leaves).chain(&damage).filter(|s| s.len() < 200) {
        for i in part_starts(s) {
            check_parts(&s[i..])
        }
    }
}

/// FNV-1a, a digest that is the same on every host and build.
struct Fnv(u64);

impl Fnv {
    fn new() -> Self {
        Fnv(0xcbf2_9ce4_8422_2325)
    }

    fn write(&mut self, s: &str) {
        for b in s.bytes() {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x0100_0000_01b3)
        }
    }

    fn of(s: &str) -> u64 {
        let mut h = Fnv::new();
        h.write(s);
        h.0
    }
}

/// `s` quoted, or, past a line's share, its length and digest.
fn shown(s: &str) -> String {
    match s.len() <= 120 {
        true => format!("{s:?}"),
        false => format!("<{} bytes, fnv {:016x}>", s.len(), Fnv::of(s)),
    }
}

/// What `value` and `parse_value` make of the whole of `s`: the value
/// printed with its types and the bytes `value` consumed, or where
/// `value` stopped; `parse_value`'s message.
fn whole_record(s: &str) -> String {
    let value = match value(s) {
        Ok((v, n)) => format!("ok {n} {}", shown(&v.to_string())),
        Err(f) => format!("err {}", f.offset),
    };
    let whole = match parse_value(s) {
        Ok(_) => "ok".to_string(),
        Err(e) => format!("err {:?}", e.to_string()),
    };
    format!("{} | value {value} | whole {whole}", shown(s))
}

/// What each entry point other parsers call makes of `s`: what it read
/// and the bytes it consumed, or where it stopped and what it expected
/// there; entry points that agree share their result.
fn part_record(s: &str) -> String {
    fn read<T: fmt::Debug>(s: &str, r: Result<(T, usize), Failure>) -> String {
        match r {
            Ok((v, n)) => format!("ok {n} {}", shown(&format!("{v:?}"))),
            Err(f) => {
                let msg = f.in_text(s).to_string();
                let why: Vec<&str> = msg
                    .lines()
                    .skip(1)
                    .filter(|l| !l.starts_with("Unexpected "))
                    .collect();
                format!("err {} {:?}", f.offset, why.join(" / "))
            }
        }
    }
    let escaped = |must_esc, esc| read(s, escaped_string(s, must_esc, esc));
    let reads = [
        ("i64", read(s, int::<i64>(s))),
        ("i8", read(s, int::<i8>(s))),
        ("u8", read(s, int::<u8>(s))),
        ("usize", read(s, int::<usize>(s))),
        ("duration", read(s, duration(s))),
        ("value", escaped(&VAL_MUST_ESC[..], &*VAL_ESC)),
        ("graphix", escaped(&GRAPHIX_MUST_ESC[..], &*GRAPHIX_ESC)),
        ("rpc", escaped(&RPC_MUST_ESC[..], &*RPC_ESC)),
        ("not_prefix", not_prefix(s).to_string()),
        ("whitespace", whitespace(s).to_string()),
    ];
    let mut line = shown(s);
    for (i, (what, r)) in reads.iter().enumerate() {
        let same = |j: usize| reads.get(j).is_some_and(|(_, q)| q == r);
        let sep = if i > 0 && same(i - 1) { " " } else { " | " };
        write!(line, "{sep}{what}").unwrap();
        if !same(i + 1) {
            write!(line, " {r}").unwrap()
        }
    }
    line
}

/// What the hand-picked texts and the values nested around the limit
/// read as, whole, and the hand-picked ones from each place another parser
/// might hand one of them, is `expected.txt`; its last line digests the
/// same for their truncated and corrupted copies. `NV_PARSER_EXPECTED=write`
/// writes the file from this build.
#[test]
fn texts_read_as_recorded() {
    let cases = hand_picked();
    let mut lines: Vec<String> =
        cases.iter().chain(&deep()).map(|s| whole_record(s)).collect();
    for s in &cases {
        lines.extend(part_starts(s).map(|i| part_record(&s[i..])))
    }
    let (mut rng, mut damage) = (Rng(0x9e37_79b9_7f4a_7c15), vec![]);
    for s in &cases {
        damaged(s, 8, &mut rng, &mut damage)
    }
    let mut digest = Fnv::new();
    for s in &damage {
        let parts = part_starts(s).map(|i| part_record(&s[i..]));
        for r in std::iter::once(whole_record(s)).chain(parts) {
            digest.write(&r);
            digest.write("\n")
        }
    }
    lines.push(format!("{} damaged copies: fnv {:016x}", damage.len(), digest.0));
    if std::env::var("NV_PARSER_EXPECTED").is_ok_and(|w| w == "write") {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/parser/expected.txt");
        return std::fs::write(path, lines.join("\n") + "\n").unwrap();
    }
    let want: Vec<&str> = include_str!("expected.txt").lines().collect();
    let changed: Vec<String> = want
        .iter()
        .zip(&lines)
        .filter(|(w, g)| *w != g)
        .take(20)
        .map(|(w, g)| format!("- {w}\n+ {g}"))
        .collect();
    assert!(
        changed.is_empty() && want.len() == lines.len(),
        "{} records, {} expected; the first that differ:\n{}",
        lines.len(),
        want.len(),
        changed.join("\n")
    )
}

/// Arrays, maps or errors nested to the limit parse on a small stack,
/// growing it, and drop there.
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

/// A parse drops the values it read and lost on stack it guaranteed: a
/// sibling as deep as the limit allows, of each mix of kinds, before a
/// failure, as a key with no `=>`, or beside a value under the same key,
/// at every depth, on threads just over the red zone, so that one level
/// starts with the red zone and no more.
#[test]
fn deep_values_a_parse_loses_drop_on_a_small_stack() {
    let max = max_nesting();
    let mixes: [&[(&str, &str)]; 3] = [
        &[("{1 => ", "}"), ("[", "]")],
        &[("{1 => ", "}"), ("error:", ""), ("[", "]"), ("error:", "")],
        &[("[", "]"), ("{", " => 1}")],
    ];
    let deep = |mix: &[(&str, &str)], n: usize| {
        let (mut open, mut close) = (String::new(), String::new());
        for (o, c) in mix.iter().cycle().take(n) {
            open.push_str(o);
            close.insert_str(0, c);
        }
        format!("{open}1{close}")
    };
    for stack in [16, 64].map(|k| RED_ZONE + k * 1024) {
        std::thread::Builder::new()
            .stack_size(stack)
            .spawn(move || {
                for mix in mixes {
                    for p in 0..max - 1 {
                        let d = deep(mix, max - 2 - p);
                        let (open, close) = ("[".repeat(p), "]".repeat(p));
                        for s in [
                            format!("{open}[{d}, @]{close}"),
                            format!("{open}{{{d} @}}{close}"),
                        ] {
                            assert!(parse_value(&s).is_err(), "{stack} {p}")
                        }
                        for s in [
                            format!("{open}{{1 => {d}, 1 => 2}}{close}"),
                            format!("{open}{{1 => 2, 1 => {d}}}{close}"),
                        ] {
                            let kept = parse_value(&s).unwrap();
                            stacker::maybe_grow(4 << 20, 8 << 20, || drop(kept))
                        }
                    }
                }
            })
            .unwrap()
            .join()
            .unwrap()
    }
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
    assert_eq!(
        msg("truex"),
        "Parse error at line: 1, column: 5\nUnexpected `x`\nExpected the end of `true`\n"
    );
    assert_eq!(
        msg("[1, null_]"),
        "Parse error at line: 1, column: 9\nUnexpected `_`\nExpected the end of `null`\n"
    );
    assert_eq!(
        msg("[datetime:\"2020-01-01T00:00:00Z\", datetime:\"2020-02-30T00:00:00Z\"]"),
        "Parse error at line: 1, column: 44\ninvalid datetime: input is out of range\n"
    );
    assert_eq!(
        msg(""),
        "Parse error at line: 1, column: 1\nUnexpected end of input\nExpected a value\n"
    );
}

/// Parses the printed corpus (`NV_PARSER_SET=valid`) or its damaged
/// copies (`failing`) ten times as `NV_PARSER_BENCH` says: `whole` with
/// `parse_value`, `prefix` with `value`, `none` not at all, for the corpus
/// alone. Run under `perf stat` to count the grammar's instructions.
#[test]
#[ignore]
fn parse_speed() {
    use std::hint::black_box;
    let which = std::env::var("NV_PARSER_BENCH").unwrap();
    let printed = printed(3000, 200);
    let corpus = match std::env::var("NV_PARSER_SET").unwrap().as_str() {
        "valid" => printed,
        _ => {
            let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
            let mut damage = vec![];
            for s in &printed {
                damaged(s, 2, &mut rng, &mut damage)
            }
            damage
        }
    };
    for _ in 0..10 {
        for s in &corpus {
            let s = black_box(s.as_str());
            match which.as_str() {
                "whole" => drop(black_box(parse_value(s))),
                "prefix" => drop(black_box(value(s))),
                _ => (),
            }
        }
    }
    println!(
        "{} inputs, {} bytes",
        corpus.len(),
        corpus.iter().map(|s| s.len()).sum::<usize>()
    );
}
