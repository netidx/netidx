//! The hand-written grammar ([`rd`]) against the combine grammar it
//! replaces: the same values, the same bytes consumed and the same
//! verdicts, over every value the generator makes, printed with and
//! without types, hand-picked edge cases, and truncated and corrupted
//! copies of both.

use super::{rd, *};
use crate::{Map, abstract_type::AbstractWrapper};
use bytes::{Buf, BufMut};
use chrono::{DateTime, TimeZone, Utc};
use combine::{
    EasyParser, eof,
    stream::position::{self, SourcePosition},
};
use netidx_core::pack::PackError;
use proptest::{
    collection, prelude::*, strategy::ValueTree, string::string_regex,
    test_runner::TestRunner,
};
use std::{
    collections::HashMap,
    fmt::{self, Write},
    fs,
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

fn leaf() -> impl Strategy<Value = Value> {
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
        any::<String>().prop_map(|s| Value::String(s.into())),
        escapes.prop_map(|s| Value::String(s.into())),
        any::<Vec<u8>>().prop_map(|b| Value::Bytes(PBytes::new(Bytes::from(b)))),
        Just(Value::Bool(true)),
        Just(Value::Bool(false)),
        Just(Value::Null),
        (any::<u64>(), any::<Vec<u8>>()).prop_map(|(n, b)| OPAQUE.wrap(Opaque(n, b))),
    ]
}

fn tree() -> impl Strategy<Value = Value> {
    leaf().prop_recursive(10, 1000, 100, |inner| {
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
    let (leaf, tree) = (leaf(), tree());
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

/// What a parse made of its input: the result and the bytes consumed,
/// or where it failed and its message.
type Verdict<T> = Result<(T, usize), (usize, String)>;

fn offset_of(s: &str, p: SourcePosition) -> usize {
    let (mut line, mut col) = (1, 1);
    for (i, c) in s.char_indices() {
        if (line, col) == (p.line, p.column) {
            return i;
        }
        (line, col) = if c == '\n' { (line + 1, 1) } else { (line, col + 1) };
    }
    s.len()
}

macro_rules! old {
    ($s:expr, $p:expr) => {{
        let s: &str = $s;
        match $p.easy_parse(position::Stream::new(s)) {
            Ok((v, rest)) => Ok((v, s.len() - rest.input.len())),
            Err(e) => Err((offset_of(s, e.position), format!("{e}"))),
        }
    }};
}

fn new<T>(s: &str, r: Result<(T, usize), rd::Failure>) -> Verdict<T> {
    r.map_err(|f| (f.offset, format!("{}", f.in_text(s))))
}

/// The whole text as `parse_value` reads it, the old way.
fn old_whole(s: &str) -> Verdict<Value> {
    old!(s, value(&VAL_MUST_ESC, &VAL_ESC).skip(spaces()).skip(eof()))
}

fn new_whole(s: &str) -> Verdict<Value> {
    match parse_value(s) {
        Ok(v) => Ok((v, s.len())),
        Err(e) => {
            let e = format!("{e}");
            let (line, col) = e
                .strip_prefix("Parse error at line: ")
                .and_then(|r| r.split_once(", column: "))
                .and_then(|(l, r)| {
                    Some((l.parse().ok()?, r.lines().next()?.parse().ok()?))
                })
                .unwrap();
            Err((offset_of(s, SourcePosition { line, column: col }), e))
        }
    }
}

/// The same value: equal, and printed alike, which also tells floats'
/// signs and decimals' scales apart.
fn same_value(a: &Value, b: &Value) -> bool {
    a == b && format!("{a}") == format!("{b}")
}

/// Where the two disagreed only in where a failure stopped, by what, and
/// each pair of messages after their position lines, the token found
/// elided, with how often it was seen and the first input it was seen for.
#[derive(Default)]
struct Tally {
    compared: usize,
    accepted: usize,
    moved: Vec<(&'static str, String, usize, usize, String, String)>,
    messages: HashMap<(String, String), (usize, String)>,
}

/// Whether a failure the combine grammar reported at `old` and the
/// hand-written one at `new` is of a kind the hand-written one reports
/// further on: a type's name not followed by `:`, which combine reported
/// at the name's start, a datetime chrono refused, which combine reported
/// at its opening quote, and a keyword run into an identifier at a list's
/// item, which combine reported at the item.
fn moved_on(old: usize, new: usize, msg: &str) -> bool {
    old < new
        && (msg.ends_with("Expected `:`\n")
            || msg.contains("\ninvalid datetime: ")
            || msg.lines().count() == 2)
}

/// A message without its position line and the token it found.
fn gist(msg: &str) -> String {
    msg.lines()
        .skip(1)
        .map(|l| match l.starts_with("Unexpected `") {
            true => "Unexpected <token>",
            false => l,
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

impl Tally {
    fn check<T: fmt::Debug>(
        &mut self,
        what: &'static str,
        s: &str,
        old: Verdict<T>,
        new: Verdict<T>,
        same: impl Fn(&T, &T) -> bool,
    ) {
        self.compared += 1;
        match (old, new) {
            (Ok((a, n)), Ok((b, m))) => {
                self.accepted += 1;
                assert!(
                    same(&a, &b) && n == m,
                    "{what} {s:?}: {a:?} ({n}) vs {b:?} ({m})"
                )
            }
            (Err((n, e)), Err((m, f))) => {
                let entry = self.messages.entry((gist(&e), gist(&f))).or_default();
                entry.0 += 1;
                if entry.1.is_empty() {
                    entry.1 = s.chars().take(80).collect();
                }
                if n != m {
                    assert!(
                        moved_on(n, m, &f),
                        "{what} {s:?}: old {n} {e:?} new {m} {f:?}"
                    );
                    self.moved.push((what, s.to_string(), n, m, e, f))
                }
            }
            (a, b) => panic!("{what} {s:?}: old {a:?} new {b:?}"),
        }
    }
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

/// Every entry point the two grammars share, on `s`.
fn compare(s: &str, t: &mut Tally) {
    t.check(
        "value",
        s,
        old!(s, value(&VAL_MUST_ESC, &VAL_ESC)),
        new(s, rd::value(s)),
        same_value,
    );
    t.check("parse_value", s, old_whole(s), new_whole(s), same_value);
}

/// The entry points other parsers call on part of their text, on `s`.
fn compare_parts(s: &str, t: &mut Tally) {
    macro_rules! int {
        ($t:ty, $what:literal) => {
            t.check(
                $what,
                s,
                old!(s, int::<_, $t>()),
                new(s, rd::int::<$t>(s)),
                |a, b| a == b,
            )
        };
    }
    int!(i64, "int i64");
    int!(i8, "int i8");
    int!(u8, "int u8");
    int!(usize, "int usize");
    t.check("duration", s, old!(s, duration()), new(s, rd::duration(s)), |a, b| a == b);
    macro_rules! escaped {
        ($must:expr, $esc:expr, $what:literal) => {
            t.check(
                $what,
                s,
                old!(s, escaped_string(&$must, &$esc)),
                new(s, rd::escaped_string(s, &$must, &$esc))
                    .map(|(c, n)| (c.into_owned(), n)),
                |a, b| a == b,
            )
        };
    }
    escaped!(VAL_MUST_ESC, VAL_ESC, "escaped value");
    escaped!(GRAPHIX_MUST_ESC, GRAPHIX_ESC, "escaped graphix");
    escaped!(RPC_MUST_ESC, RPC_ESC, "escaped rpc");
    let old_np = not_prefix().parse(s).is_ok();
    assert_eq!(old_np, rd::not_prefix(s), "not_prefix {s:?}");
}

/// The starts of `s`'s suffixes another parser might hand one of these:
/// after a `:`, quote, bracket, comma, space or arrow.
fn part_starts(s: &str) -> impl Iterator<Item = usize> + '_ {
    std::iter::once(0).chain(
        s.char_indices()
            .filter(|(_, c)| matches!(c, ':' | '"' | '[' | '{' | ',' | ' ' | '>'))
            .map(|(i, c)| i + c.len_utf8()),
    )
}

fn report(t: &Tally) -> String {
    let mut out = String::new();
    let mut messages: Vec<_> = t.messages.iter().collect();
    messages.sort_by_key(|(_, (n, _))| std::cmp::Reverse(*n));
    for ((old, new), (n, s)) in messages {
        writeln!(out, "@@@ {n} {s:?}\nold: {old}\nnew: {new}").unwrap();
    }
    for (what, s, n, m, e, f) in &t.moved {
        let s: String = s.chars().take(120).collect();
        writeln!(out, "### {what} {s:?}: old {n} new {m}\n{e}---\n{f}").unwrap();
    }
    out
}

#[test]
fn hand_written_matches_combine() {
    let mut t = Tally::default();
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
    let deep = deep();
    for s in cases.iter().chain(&deep).chain(&printed).chain(&damage) {
        compare(s, &mut t)
    }
    let printed_leaves = &printed[..2 * leaves];
    let short =
        cases.iter().chain(printed_leaves).chain(damage.iter()).filter(|s| s.len() < 200);
    for s in short {
        for i in part_starts(s) {
            compare_parts(&s[i..], &mut t)
        }
    }
    println!(
        "compared {} ({} accepted) over {} hand-picked, {} deep, {} printed, {} damaged; \
         {} failures stopped elsewhere",
        t.compared,
        t.accepted,
        cases.len(),
        deep.len(),
        printed.len(),
        damage.len(),
        t.moved.len()
    );
    if let Ok(path) = std::env::var("NV_PARSER_DIFF") {
        fs::write(path, report(&t)).unwrap();
    }
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
    for stack in [16, 64].map(|k| rd::RED_ZONE + k * 1024) {
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
    assert_eq!(msg("truex"), "Parse error at line: 1, column: 5\nUnexpected `x`\n");
    assert_eq!(
        msg(""),
        "Parse error at line: 1, column: 1\nUnexpected end of input\nExpected a value\n"
    );
}

/// Parses the printed corpus (`NV_PARSER_SET=valid`) or its damaged
/// copies (`failing`) ten times with the grammar `NV_PARSER_BENCH` names:
/// `old` or `new` for `parse_value`, `old-prefix` or `new-prefix` for a
/// value at the start of the text, `none` for the corpus alone. Run under
/// `perf stat` to count each grammar's instructions.
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
    let old_parse_value = |s: &str| {
        value(&VAL_MUST_ESC, &VAL_ESC)
            .skip(spaces())
            .skip(eof())
            .easy_parse(position::Stream::new(s))
            .map(|(r, _)| r)
            .map_err(|e| anyhow::anyhow!(format!("{}", e)))
    };
    for _ in 0..10 {
        for s in &corpus {
            let s = black_box(s.as_str());
            match which.as_str() {
                "old" => drop(black_box(old_parse_value(s))),
                "new" => drop(black_box(parse_value(s))),
                "old-prefix" => drop(black_box(value(&VAL_MUST_ESC, &VAL_ESC).parse(s))),
                "new-prefix" => drop(black_box(rd::value(s))),
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
