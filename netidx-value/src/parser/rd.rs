//! The value grammar as plain functions over the text: each reads from
//! the start of the `&str` it is given and returns what it read and the
//! bytes it consumed, or a [`Failure`] naming the byte offset where it
//! stopped and what it expected there. A caller in another parser turns
//! the offset into its own position and message.

use super::{FromStrRadix, VAL_ESC, VAL_MUST_ESC, duration_of, max_nesting};
use crate::{Abstract, Typ, ValArray, Value, pbuf::PBytes};
use arcstr::ArcStr;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use compact_str::CompactString;
use escaping::Escape;
use netidx_core::pack::Pack;
use poolshark::local::LPooled;
use rust_decimal::Decimal;
use std::{borrow::Cow, fmt, str::FromStr, time::Duration};
use triomphe::Arc;

/// Stack each nesting level is guaranteed: room for its frames and for
/// dropping the values it read and lost, which Value's drop guards bound
/// to a few hundred KB however deep the value.
pub(super) const RED_ZONE: usize = 1024 * 1024;

/// Size of each fresh stack segment, mmap'd on entry and released on exit.
const SEGMENT: usize = 2 * 1024 * 1024;

/// Why a parse failed: where it stopped and what it wanted there.
#[derive(Debug, Clone, Copy)]
pub struct Failure {
    /// Byte offset into the text the parse began on.
    pub offset: usize,
    pub expected: Expected,
}

/// What a parse wanted where it stopped.
#[derive(Debug, Clone, Copy)]
pub enum Expected {
    /// One of these.
    OneOf(&'static [Item]),
    /// One of the [`escapes`] of this string grammar, after its escape char.
    Escape(&'static [char], &'static Escape),
    /// What was read is refused for this reason: a datetime or a value
    /// nested too deep that starts at the offset, anything else that ends
    /// there.
    Refused(&'static str),
}

/// One thing a parse expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Item {
    Char(char),
    Text(&'static str),
}

impl fmt::Display for Item {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Item::Char(c) => write!(f, "`{c}`"),
            Item::Text(s) => f.write_str(s),
        }
    }
}

const VALUE: Expected = Expected::OneOf(&[Item::Text("a value")]);
const DIGIT: Expected = Expected::OneOf(&[Item::Text("a digit")]);
const SIGN_OR_DIGIT: Expected =
    Expected::OneOf(&[Item::Char('-'), Item::Text("a digit")]);
const NESTING: Expected = Expected::Refused("value nesting too deep");
const BAD_SIGNED: Expected = Expected::Refused("invalid signed integer");
const BAD_UNSIGNED: Expected = Expected::Refused("invalid unsigned integer");
const COLON: Expected = Expected::OneOf(&[Item::Char(':')]);
const QUOTE: Expected = Expected::OneOf(&[Item::Char('"')]);

impl Failure {
    /// This failure shifted `by` bytes, for a parse begun `by` bytes into
    /// a longer text.
    pub fn after(self, by: usize) -> Self {
        Self { offset: self.offset + by, ..self }
    }

    /// The failure in `text`, the text the parse began on: its line and
    /// column, the token found there, and what was expected instead or
    /// why what was read is refused. Another text panics where the offset
    /// is past its end or inside a character.
    pub fn in_text<'a>(&'a self, text: &'a str) -> impl fmt::Display + 'a {
        Report { failure: self, text }
    }
}

struct Report<'a> {
    failure: &'a Failure,
    text: &'a str,
}

impl fmt::Display for Report<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Failure { offset, expected } = *self.failure;
        let (line, column) = self.text[..offset]
            .chars()
            .fold((1, 1), |(l, c), ch| if ch == '\n' { (l + 1, 1) } else { (l, c + 1) });
        writeln!(f, "Parse error at line: {line}, column: {column}")?;
        let found = self.text[offset..].chars().next();
        match expected {
            Expected::Refused(why) => writeln!(f, "{why}"),
            Expected::OneOf(items) => unexpected(f, found, items.iter().copied()),
            Expected::Escape(must_esc, esc) => {
                unexpected(f, found, escapes(must_esc, esc).map(Item::Char))
            }
        }
    }
}

/// The char a failure found, `None` for the end of the text, and what it
/// expected instead.
fn unexpected(
    f: &mut fmt::Formatter<'_>,
    found: Option<char>,
    expected: impl Iterator<Item = Item>,
) -> fmt::Result {
    match found {
        Some(c) => writeln!(f, "Unexpected `{c}`")?,
        None => writeln!(f, "Unexpected end of input")?,
    }
    let mut items = expected.peekable();
    let mut first = true;
    while let Some(i) = items.next() {
        let sep = match (first, items.peek()) {
            (true, _) => "Expected ",
            (false, None) => " or ",
            (false, Some(_)) => ", ",
        };
        write!(f, "{sep}{i}")?;
        first = false;
    }
    if !first {
        writeln!(f)?;
    }
    Ok(())
}

/// The characters an escape char may precede in a string whose
/// unescaped text excludes `must_esc`.
pub fn escapes<'a>(
    must_esc: &'a [char],
    esc: &'a Escape,
) -> impl Iterator<Item = char> + 'a {
    esc.get_tr()
        .iter()
        .filter_map(|(_, s)| s.chars().next())
        .chain(must_esc.iter().copied())
}

/// Whether `s` does not continue an identifier: it is empty or starts
/// with neither `_` nor a letter or digit.
pub fn not_prefix(s: &str) -> bool {
    !s.starts_with(|c: char| c == '_' || c.is_alphanumeric())
}

/// A value, after any whitespace, with the strings in it escaped by
/// [`VAL_ESC`].
pub fn value(s: &str) -> Result<(Value, usize), Failure> {
    let mut c = Cursor::new(s);
    let v = c.value(0)?;
    Ok((v, c.at))
}

/// The bytes of whitespace at the start of `s`.
pub fn whitespace(s: &str) -> usize {
    s.len() - s.trim_start().len()
}

/// The whitespace at the start of `s`, then its end: the bytes of
/// whitespace, or where something else stands.
pub fn end(s: &str) -> Result<usize, Failure> {
    let at = whitespace(s);
    match at == s.len() {
        true => Ok(at),
        false => Err(Failure {
            offset: at,
            expected: Expected::OneOf(&[Item::Text("end of input")]),
        }),
    }
}

/// An integer, `-` allowed, in decimal or with a `0x`, `0b` or `0o`
/// radix prefix.
pub fn int<T: FromStrRadix>(s: &str) -> Result<(T, usize), Failure> {
    let mut c = Cursor::new(s);
    let i = c.int()?;
    Ok((i, c.at))
}

/// A duration's text: a float literal and its unit, ns, us (or µs), ms,
/// s, m, h, d, M or y.
pub fn duration(s: &str) -> Result<(Duration, usize), Failure> {
    let mut c = Cursor::new(s);
    let d = c.duration()?;
    Ok((d, c.at))
}

/// The text of a string up to the first character of `must_esc` that
/// no escape char precedes, unescaped by `esc`: borrowed when nothing
/// in it is escaped. Possibly empty.
pub fn escaped_string<'a>(
    s: &'a str,
    must_esc: &'static [char],
    esc: &'static Escape,
) -> Result<(Cow<'a, str>, usize), Failure> {
    let mut c = Cursor::new(s);
    let raw = c.escaped(must_esc, esc)?;
    Ok((esc.unescape(raw), c.at))
}

/// How a typed literal's text after its prefix and `:` reads.
type Read = fn(&mut Cursor<'_>, usize) -> Result<Value, Failure>;

/// The types a value literal may name before a `:`, no name a prefix of
/// another's.
const TYPED: [(Typ, Read); 20] = [
    (Typ::U8, |c, _| c.uint().map(Value::U8)),
    (Typ::U16, |c, _| c.uint().map(Value::U16)),
    (Typ::U32, |c, _| c.uint().map(Value::U32)),
    (Typ::U64, |c, _| c.uint().map(Value::U64)),
    (Typ::I8, |c, _| c.int().map(Value::I8)),
    (Typ::I16, |c, _| c.int().map(Value::I16)),
    (Typ::I32, |c, _| c.int().map(Value::I32)),
    (Typ::I64, |c, _| c.int().map(Value::I64)),
    (Typ::V32, |c, _| c.uint().map(Value::V32)),
    (Typ::V64, |c, _| c.uint().map(Value::V64)),
    (Typ::Z32, |c, _| c.int().map(Value::Z32)),
    (Typ::Z64, |c, _| c.int().map(Value::Z64)),
    (Typ::F32, |c, _| c.float().map(Value::F32)),
    (Typ::F64, |c, _| c.float().map(Value::F64)),
    (Typ::Decimal, |c, _| c.float().map(|d: Decimal| Value::Decimal(Arc::new(d)))),
    (Typ::DateTime, |c, _| {
        let offset = c.at;
        let s = c.quoted(&VAL_MUST_ESC, &VAL_ESC)?;
        match DateTime::<Utc>::from_str(&s) {
            Ok(d) => Ok(Value::DateTime(Arc::new(d))),
            Err(e) => {
                Err(Failure { offset, expected: Expected::Refused(datetime_refusal(e)) })
            }
        }
    }),
    (Typ::Duration, |c, _| c.duration().map(|d| Value::Duration(Arc::new(d)))),
    (Typ::Bytes, |c, _| {
        let b = c.base64()?;
        Ok(Value::Bytes(PBytes::new(Bytes::from(LPooled::detach(b)))))
    }),
    (Typ::Abstract, |c, _| match Abstract::decode(&mut &c.base64()?[..]) {
        Ok(a) => Ok(Value::Abstract(a)),
        Err(_) => Err(c.fail(Expected::Refused("failed to unpack abstract"))),
    }),
    (Typ::Error, |c, depth| c.value(depth + 1).map(|v| Value::Error(v.into()))),
];

/// Why chrono refused a datetime, in its words.
fn datetime_refusal(e: chrono::ParseError) -> &'static str {
    use chrono::format::ParseErrorKind as K;
    match e.kind() {
        K::OutOfRange => "invalid datetime: input is out of range",
        K::Impossible => "invalid datetime: no possible date and time matching input",
        K::NotEnough => "invalid datetime: input is not enough for unique date and time",
        K::TooShort => "invalid datetime: premature end of input",
        K::TooLong => "invalid datetime: trailing input",
        _ => "invalid datetime: input contains invalid characters",
    }
}

/// `true`, `false` and `null`: the bool each is, none for `null`, and
/// what a letter, digit or `_` right after one fails for.
const KEYWORDS: [(&str, Option<bool>, Expected); 3] = [
    ("true", Some(true), Expected::OneOf(&[Item::Text("the end of `true`")])),
    ("false", Some(false), Expected::OneOf(&[Item::Text("the end of `false`")])),
    ("null", None, Expected::OneOf(&[Item::Text("the end of `null`")])),
];

/// The duration units, each checked before any unit that is its prefix.
const UNITS: [&str; 10] = ["ms", "ns", "us", "µs", "s", "m", "h", "d", "M", "y"];

/// The end of the ASCII digits of `radix` in `b` from `i`.
fn digits(b: &[u8], mut i: usize, radix: u32) -> usize {
    while b.get(i).is_some_and(|d| (*d as char).is_digit(radix)) {
        i += 1
    }
    i
}

struct Cursor<'a> {
    text: &'a str,
    at: usize,
    max_nesting: usize,
}

impl<'a> Cursor<'a> {
    fn new(text: &'a str) -> Self {
        Self { text, at: 0, max_nesting: max_nesting() }
    }

    fn rest(&self) -> &'a str {
        &self.text[self.at..]
    }

    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.at).copied()
    }

    fn fail(&self, expected: Expected) -> Failure {
        Failure { offset: self.at, expected }
    }

    fn skip_ws(&mut self) {
        self.at += whitespace(self.rest())
    }

    fn eat(&mut self, c: u8) -> bool {
        let found = self.peek() == Some(c);
        self.at += found as usize;
        found
    }

    fn value(&mut self, depth: usize) -> Result<Value, Failure> {
        if depth >= self.max_nesting {
            return Err(self.fail(NESTING));
        }
        stacker::maybe_grow(RED_ZONE, SEGMENT, || {
            self.skip_ws();
            match self.peek() {
                Some(b'[') => self.array(depth),
                Some(b'{') => self.map(depth),
                Some(b'"') => Ok(Value::String(ArcStr::from(
                    &*self.quoted(&VAL_MUST_ESC, &VAL_ESC)?,
                ))),
                Some(b'-' | b'0'..=b'9') => self.number(),
                _ => self.word(depth),
            }
        })
    }

    fn array(&mut self, depth: usize) -> Result<Value, Failure> {
        const AT: &[Item] = &[Item::Text("a value"), Item::Char(']')];
        const AFTER: &[Item] = &[Item::Char(','), Item::Char(']')];
        let mut vals: LPooled<Vec<Value>> = LPooled::take();
        self.items(b']', AT, AFTER, |c| Ok(vals.push(c.value(depth + 1)?)))?;
        Ok(Value::Array(ValArray::from_iter_exact(vals.drain(..))))
    }

    fn map(&mut self, depth: usize) -> Result<Value, Failure> {
        const AT: &[Item] = &[Item::Text("a value"), Item::Char('}')];
        const AFTER: &[Item] = &[Item::Char(','), Item::Char('}')];
        const ARROW: Expected = Expected::OneOf(&[Item::Text("`=>`")]);
        let mut vals: LPooled<Vec<(Value, Value)>> = LPooled::take();
        self.items(b'}', AT, AFTER, |c| {
            let k = c.value(depth + 1)?;
            c.skip_ws();
            if !c.rest().starts_with("=>") {
                return Err(c.fail(ARROW));
            }
            c.at += 2;
            Ok(vals.push((k, c.value(depth + 1)?)))
        })?;
        Ok(Value::Map(immutable_chunkmap::map::Map::from_iter(vals.drain(..))))
    }

    /// The items after an open bracket, each read by `item`, to `close`:
    /// separated by commas, whitespace around each comma and before
    /// `close`, a comma after the last allowed. Whitespace right after
    /// the open bracket starts an item, so `[ ]` is refused where `[]`
    /// reads. `at_item` is what an item's start expects, `after_item`
    /// what follows an item.
    fn items(
        &mut self,
        close: u8,
        at_item: &'static [Item],
        after_item: &'static [Item],
        mut item: impl FnMut(&mut Self) -> Result<(), Failure>,
    ) -> Result<(), Failure> {
        self.at += 1;
        loop {
            if self.peek() != Some(close) {
                let at = self.at;
                item(self).map_err(|f| match f.expected {
                    Expected::OneOf(_) if f.offset == at => {
                        Failure { expected: Expected::OneOf(at_item), ..f }
                    }
                    _ => f,
                })?;
            }
            self.skip_ws();
            if self.eat(close) {
                return Ok(());
            }
            if !self.eat(b',') {
                return Err(self.fail(Expected::OneOf(after_item)));
            }
            self.skip_ws();
        }
    }

    fn quoted(
        &mut self,
        must_esc: &'static [char],
        esc: &'static Escape,
    ) -> Result<Cow<'a, str>, Failure> {
        if !self.eat(b'"') {
            return Err(self.fail(QUOTE));
        }
        let raw = self.escaped(must_esc, esc)?;
        match self.eat(b'"') {
            true => Ok(esc.unescape(raw)),
            false => Err(self.fail(QUOTE)),
        }
    }

    fn escaped(
        &mut self,
        must_esc: &'static [char],
        esc: &'static Escape,
    ) -> Result<&'a str, Failure> {
        let start = self.at;
        let esc_char = esc.get_escape_char();
        loop {
            let rest = self.rest();
            self.at += rest.find(must_esc).unwrap_or(rest.len());
            if !self.rest().starts_with(esc_char) {
                return Ok(&self.text[start..self.at]);
            }
            self.at += esc_char.len_utf8();
            match self.rest().chars().next() {
                Some(c) if escapes(must_esc, esc).any(|e| e == c) => {
                    self.at += c.len_utf8()
                }
                _ => return Err(self.fail(Expected::Escape(must_esc, esc))),
            }
        }
    }

    /// `true`, `false`, `null`, or a typed literal: a type's name, `:`
    /// and what that type reads, whitespace around the `:`.
    fn word(&mut self, depth: usize) -> Result<Value, Failure> {
        let rest = self.rest();
        if let Some((kw, v, end)) = KEYWORDS.iter().find(|(kw, ..)| rest.starts_with(kw))
        {
            self.at += kw.len();
            return match not_prefix(self.rest()) {
                true => Ok(v.map_or(Value::Null, Value::Bool)),
                false => Err(self.fail(*end)),
            };
        }
        let Some((typ, read)) = TYPED.iter().find(|(t, _)| rest.starts_with(t.name()))
        else {
            return Err(self.fail(VALUE));
        };
        self.at += typ.name().len();
        self.skip_ws();
        if !self.eat(b':') {
            return Err(self.fail(COLON));
        }
        self.skip_ws();
        read(self, depth)
    }

    /// A float literal, else an integer, `-` allowed in either.
    fn number(&mut self) -> Result<Value, Failure> {
        match self.float_text() {
            Ok(s) => self.parse_float(s).map(Value::F64),
            Err(_) => self.int().map(Value::I64),
        }
    }

    /// A `0x`, `0b` or `0o` prefix at `i` and at least one digit of its
    /// radix: the radix and the digits' span.
    fn radix(&self, i: usize) -> Option<(u32, usize, usize)> {
        let b = self.text.as_bytes();
        let radix = match (b.get(i), b.get(i + 1)) {
            (Some(b'0'), Some(b'x' | b'X')) => 16,
            (Some(b'0'), Some(b'b' | b'B')) => 2,
            (Some(b'0'), Some(b'o' | b'O')) => 8,
            _ => return None,
        };
        let end = digits(b, i + 2, radix);
        (end > i + 2).then_some((radix, i + 2, end))
    }

    fn uint<T: FromStrRadix>(&mut self) -> Result<T, Failure> {
        let (radix, start, end) = match self.radix(self.at) {
            Some(r) => r,
            None => (10, self.at, digits(self.text.as_bytes(), self.at, 10)),
        };
        if end == start {
            return Err(self.fail(DIGIT));
        }
        self.at = end;
        T::from_str_radix(&self.text[start..end], radix)
            .map_err(|_| self.fail(BAD_UNSIGNED))
    }

    fn int<T: FromStrRadix>(&mut self) -> Result<T, Failure> {
        let neg = self.peek() == Some(b'-');
        let i = self.at + neg as usize;
        let r = match self.radix(i) {
            Some((radix, start, end)) => {
                self.at = end;
                let digits = &self.text[start..end];
                match neg {
                    false => T::from_str_radix(digits, radix),
                    true => {
                        let mut s = CompactString::const_new("-");
                        s.push_str(digits);
                        T::from_str_radix(&s, radix)
                    }
                }
            }
            None => {
                let end = digits(self.text.as_bytes(), i, 10);
                if end == i {
                    self.at = i;
                    return Err(self.fail(if neg { DIGIT } else { SIGN_OR_DIGIT }));
                }
                let s = &self.text[self.at..end];
                self.at = end;
                T::from_str_radix(s, 10)
            }
        };
        r.map_err(|_| self.fail(BAD_SIGNED))
    }

    /// The text of a float literal: digits, `-` before them allowed, with
    /// a point, an exponent or both. Nothing is consumed by a failure,
    /// whose offset is where the literal stopped.
    fn float_text(&mut self) -> Result<&'a str, Failure> {
        let b = self.text.as_bytes();
        let start = self.at;
        let i = start + (b.get(start) == Some(&b'-')) as usize;
        let int_end = digits(b, i, 10);
        if int_end == i {
            let expected = if i == start { SIGN_OR_DIGIT } else { DIGIT };
            return Err(Failure { offset: i, expected });
        }
        let point = b.get(int_end) == Some(&b'.');
        let end = if point { digits(b, int_end + 1, 10) } else { int_end };
        if b.get(end) == Some(&b'e') {
            let k = end + 1 + (b.get(end + 1) == Some(&b'-')) as usize;
            let exp_end = digits(b, k, 10);
            if exp_end > k {
                self.at = exp_end;
                return Ok(&self.text[start..exp_end]);
            }
            if !point {
                let expected = if k == end + 1 { SIGN_OR_DIGIT } else { DIGIT };
                return Err(Failure { offset: k, expected });
            }
        } else if !point {
            const POINT: Expected = Expected::OneOf(&[Item::Char('.'), Item::Char('e')]);
            return Err(Failure { offset: end, expected: POINT });
        }
        self.at = end;
        Ok(&self.text[start..end])
    }

    fn float<T: FromStr>(&mut self) -> Result<T, Failure> {
        let s = self.float_text()?;
        self.parse_float(s)
    }

    fn parse_float<T: FromStr>(&self, s: &str) -> Result<T, Failure> {
        s.parse::<T>().map_err(|_| self.fail(Expected::Refused("invalid float")))
    }

    fn duration(&mut self) -> Result<Duration, Failure> {
        const UNIT: Expected = Expected::OneOf(&[Item::Text(
            "a duration unit: ns, us, µs, ms, s, m, h, d, M or y",
        )]);
        let n = self.float_text()?;
        let rest = self.rest();
        let Some(unit) = UNITS.iter().find(|u| rest.starts_with(*u)) else {
            return Err(self.fail(UNIT));
        };
        self.at += unit.len();
        duration_of(n, unit).ok_or_else(|| {
            self.fail(Expected::Refused("a duration that is negative or too long"))
        })
    }

    /// Base64 text, `==` alone the empty bytes, decoded.
    fn base64(&mut self) -> Result<LPooled<Vec<u8>>, Failure> {
        let b = self.text.as_bytes();
        let start = self.at;
        let mut end = start;
        while b
            .get(end)
            .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'+' || *c == b'/')
        {
            end += 1
        }
        while b.get(end) == Some(&b'=') {
            end += 1
        }
        self.at = end;
        let s = match &self.text[start..end] {
            "==" => "",
            s => s,
        };
        let mut buf: LPooled<Vec<u8>> = LPooled::take();
        match BASE64.decode_vec(s, &mut buf) {
            Ok(()) => Ok(buf),
            Err(_) => Err(self.fail(Expected::Refused("base64 decode failed"))),
        }
    }
}
