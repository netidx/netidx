//! netidx's value literal grammar, twice: [`rd`], plain functions over
//! the text, which [`parse_value`] and `Value`'s `FromStr` read with,
//! and the same grammar as combine parsers ([`value`], [`int`],
//! [`duration`], [`escaped_string`], [`not_prefix`]) for parsers built
//! with combine to compose.

use crate::{Abstract, ValArray, Value, pbuf::PBytes};
use arcstr::ArcStr;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use bytes::Bytes;
use combine::{
    ErrorOffset,
    error::{ParseResult, StreamError, Tracked},
    parser::ParseMode,
    stream::{Stream, StreamOnce},
};
use combine::{
    ParseError, Parser, RangeStream, attempt, between, choice, from_str, look_ahead,
    many1, none_of, not_followed_by, one_of, optional, parser,
    parser::{
        char::{alpha_num, digit, spaces, string},
        combinator::recognize,
        range::{take_while, take_while1},
        repeat::escaped,
    },
    satisfy, sep_by, sep_by1,
    stream::Range,
    token, unexpected_any,
};
use compact_str::{CompactString, format_compact};
use escaping::Escape;
use netidx_core::pack::Pack;
use poolshark::local::LPooled;
use rust_decimal::Decimal;
use std::{
    borrow::Cow,
    cell::Cell,
    str::FromStr,
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use triomphe::Arc;

pub mod rd;

/// Default [`max_nesting`].
///
/// A `Value` nests through arrays, maps and abstracts. Anything past
/// this is an attempt to exhaust the parser rather than to express a
/// value — real ones are a handful of levels deep.
pub const DEFAULT_MAX_NESTING: usize = 1000;

static MAX_NESTING: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_NESTING);

/// How deeply a value literal may nest before [`value`] rejects it.
pub fn max_nesting() -> usize {
    MAX_NESTING.load(Ordering::Relaxed)
}

/// Raise or lower [`max_nesting`].
pub fn set_max_nesting(depth: usize) {
    MAX_NESTING.store(depth, Ordering::Relaxed)
}

/// Stack headroom [`grow`] guarantees each nesting level. It must
/// exceed what one level of `value` costs between checks, which in an
/// unoptimized build is a large combine `choice` frame.
const RED_ZONE: usize = 1024 * 1024;

/// Size of each fresh segment, mmap'd on entry and released on exit.
const SEGMENT: usize = 32 * 1024 * 1024;

thread_local! {
    static DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Wrap the recursion knot of a parser so a deeply nested input costs
/// heap segments rather than the parsing thread's stack, and is refused
/// past [`max_nesting`] rather than aborting the process. `Value`
/// literals arrive from config files, the CLI, and embedders parsing
/// text they did not write, so the depth is not ours to trust.
fn grow<P>(p: P) -> GrowStack<P> {
    GrowStack(p)
}

struct GrowStack<P>(P);

impl<Input, P> Parser<Input> for GrowStack<P>
where
    Input: Stream,
    P: Parser<Input>,
{
    type Output = P::Output;
    type PartialState = P::PartialState;

    combine::parse_mode!(Input);

    #[inline]
    fn parse_mode_impl<M>(
        &mut self,
        mode: M,
        input: &mut Input,
        state: &mut Self::PartialState,
    ) -> ParseResult<Self::Output, <Input as StreamOnce>::Error>
    where
        M: ParseMode,
    {
        let depth = DEPTH.with(|d| {
            let n = d.get() + 1;
            d.set(n);
            n
        });
        let r = if depth > max_nesting() {
            ParseResult::CommitErr(<Input as StreamOnce>::Error::from_error(
                input.position(),
                StreamError::message_static_message("value nesting too deep"),
            ))
        } else {
            let Self(p) = self;
            stacker::maybe_grow(RED_ZONE, SEGMENT, || p.parse_mode(mode, input, state))
        };
        DEPTH.with(|d| d.set(d.get() - 1));
        r
    }

    #[inline]
    fn add_error(&mut self, error: &mut Tracked<<Input as StreamOnce>::Error>) {
        self.0.add_error(error)
    }

    #[inline]
    fn add_committed_expected_error(
        &mut self,
        error: &mut Tracked<<Input as StreamOnce>::Error>,
    ) {
        self.0.add_committed_expected_error(error)
    }

    #[inline]
    fn parser_count(&self) -> ErrorOffset {
        self.0.parser_count()
    }
}

// sep_by1, but a separator terminator is allowed, and ignored
pub fn sep_by1_tok<I, O, OC, EP, SP, TP>(
    p: EP,
    sep: SP,
    term: TP,
) -> impl Parser<I, Output = OC>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
    OC: Extend<O> + Default,
    SP: Parser<I>,
    EP: Parser<I, Output = O>,
    TP: Parser<I>,
{
    sep_by1(choice((look_ahead(term).map(|_| None::<O>), p.map(Some))), sep).map(
        |mut e: LPooled<Vec<Option<O>>>| {
            let mut res = OC::default();
            res.extend(e.drain(..).filter_map(|e| e));
            res
        },
    )
}

// sep_by, but a separator terminator is allowed, and ignored
pub fn sep_by_tok<I, O, OC, EP, SP, TP>(
    p: EP,
    sep: SP,
    term: TP,
) -> impl Parser<I, Output = OC>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
    OC: Extend<O> + Default,
    SP: Parser<I>,
    EP: Parser<I, Output = O>,
    TP: Parser<I>,
{
    sep_by(choice((look_ahead(term).map(|_| None::<O>), p.map(Some))), sep).map(
        |mut e: LPooled<Vec<Option<O>>>| {
            let mut res = OC::default();
            res.extend(e.drain(..).filter_map(|e| e));
            res
        },
    )
}

pub fn not_prefix<I>() -> impl Parser<I, Output = ()>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    not_followed_by(choice((token('_'), alpha_num())))
}

fn sptoken<I>(t: char) -> impl Parser<I, Output = char>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(token(t))
}

fn spstring<I>(t: &'static str) -> impl Parser<I, Output = &'static str>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(string(t))
}

fn csep<I>() -> impl Parser<I, Output = char>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    attempt(spaces().with(token(','))).skip(spaces())
}

fn should_escape_generic(c: char) -> bool {
    c.is_control()
}

pub const VAL_MUST_ESC: [char; 2] = ['\\', '"'];
pub static VAL_ESC: LazyLock<Escape> = LazyLock::new(|| {
    Escape::new(
        '\\',
        &['\\', '"', '\n', '\r', '\0', '\t'],
        &[('\n', "n"), ('\r', "r"), ('\t', "t"), ('\0', "0")],
        Some(should_escape_generic),
    )
    .unwrap()
});

pub fn escaped_string<I>(
    must_esc: &'static [char],
    esc: &Escape,
) -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize(escaped(
        take_while1(move |c| !must_esc.contains(&c)),
        esc.get_escape_char(),
        one_of(
            esc.get_tr()
                .iter()
                .filter_map(|(_, s)| s.chars().next())
                .chain(must_esc.iter().copied()),
        ),
    ))
    .map(|s| match esc.unescape(&s) {
        Cow::Borrowed(_) => s, // it didn't need unescaping, so just return it
        Cow::Owned(s) => s,
    })
}

fn quoted<I>(
    must_escape: &'static [char],
    esc: &Escape,
) -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    between(token('"'), token('"'), escaped_string(must_escape, esc))
}

pub trait FromStrRadix: Sized {
    fn from_str_radix(s: &str, radix: u32) -> Result<Self, std::num::ParseIntError>;
}

macro_rules! impl_from_str_radix {
    ($($t:ty),*) => { $(
        impl FromStrRadix for $t {
            fn from_str_radix(s: &str, radix: u32) -> Result<Self, std::num::ParseIntError> {
                <$t>::from_str_radix(s, radix)
            }
        }
    )* };
}

impl_from_str_radix!(u8, i8, u16, i16, u32, i32, u64, i64, usize, isize);

fn radix_prefix<I>() -> impl Parser<I, Output = (u32, CompactString)>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(
            token('0')
                .with(one_of(['x', 'X']))
                .with(many1(satisfy(|c: char| c.is_ascii_hexdigit())))
                .map(|s: CompactString| (16u32, s)),
        ),
        attempt(
            token('0')
                .with(one_of(['b', 'B']))
                .with(many1(satisfy(|c: char| c == '0' || c == '1')))
                .map(|s: CompactString| (2u32, s)),
        ),
        attempt(
            token('0')
                .with(one_of(['o', 'O']))
                .with(many1(satisfy(|c: char| c.is_digit(8))))
                .map(|s: CompactString| (8u32, s)),
        ),
    ))
}

fn uint<I, T: FromStrRadix + Clone + Copy>() -> impl Parser<I, Output = T>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((radix_prefix(), many1(digit()).map(|s: CompactString| (10u32, s)))).then(
        |(radix, digits): (u32, CompactString)| match T::from_str_radix(&digits, radix) {
            Ok(i) => combine::value(i).right(),
            Err(_) => unexpected_any("invalid unsigned integer").left(),
        },
    )
}

pub fn int<I, T: FromStrRadix + Clone + Copy>() -> impl Parser<I, Output = T>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(optional(token('-')).and(radix_prefix())).then(
            |(sign, (radix, digits)): (Option<char>, (u32, CompactString))| {
                let s = if sign.is_some() {
                    let mut s = CompactString::new("-");
                    s.push_str(&digits);
                    s
                } else {
                    digits
                };
                match T::from_str_radix(&s, radix) {
                    Ok(i) => combine::value(i).right(),
                    Err(_) => unexpected_any("invalid signed integer").left(),
                }
            },
        ),
        recognize((optional(token('-')), take_while1(|c: char| c.is_digit(10)))).then(
            |s: CompactString| match T::from_str_radix(&s, 10) {
                Ok(i) => combine::value(i).right(),
                Err(_) => unexpected_any("invalid signed integer").left(),
            },
        ),
    ))
}

/// The text of a float literal: digits with a point, an exponent or both.
fn float_text<I>() -> impl Parser<I, Output = CompactString>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    choice((
        attempt(recognize((
            optional(token('-')),
            take_while1(|c: char| c.is_digit(10)),
            optional(token('.')),
            take_while(|c: char| c.is_digit(10)),
            token('e'),
            optional(token('-')),
            take_while1(|c: char| c.is_digit(10)),
        ))),
        attempt(recognize((
            optional(token('-')),
            take_while1(|c: char| c.is_digit(10)),
            token('.'),
            take_while(|c: char| c.is_digit(10)),
        ))),
    ))
}

fn flt<I, T: FromStr + Clone + Copy>() -> impl Parser<I, Output = T>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    float_text().then(|s: CompactString| match s.parse::<T>() {
        Ok(i) => combine::value(i).right(),
        Err(_) => unexpected_any("invalid float").left(),
    })
}

fn base64<I>() -> impl Parser<I, Output = LPooled<Vec<u8>>>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((
        take_while(|c: char| c.is_ascii_alphanumeric() || c == '+' || c == '/'),
        take_while(|c: char| c == '='),
    ))
    .then(|s: LPooled<String>| {
        let s = if &*s == "==" { LPooled::take() } else { s };
        let mut buf: LPooled<Vec<u8>> = LPooled::take();
        match BASE64.decode_vec(&*s, &mut buf) {
            Ok(()) => combine::value(buf).right(),
            Err(_) => unexpected_any("base64 decode failed").left(),
        }
    })
}

fn constant<I>(typ: &'static str) -> impl Parser<I, Output = ()>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    string(typ).with(spaces()).with(token(':')).with(spaces()).map(|_| ())
}

pub fn close_expr<I>() -> impl Parser<I, Output = ()>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    not_followed_by(none_of([' ', '\n', '\t', ';', ')', ',', ']', '}', '"']))
}

fn value_<I>(must_escape: &'static [char], esc: &Escape) -> impl Parser<I, Output = Value>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        choice((
            attempt(constant("u8")).with(uint::<_, u8>().map(Value::U8)),
            attempt(constant("u16")).with(uint::<_, u16>().map(Value::U16)),
            attempt(constant("u32")).with(uint::<_, u32>().map(Value::U32)),
            constant("u64").with(uint::<_, u64>().map(Value::U64)),
            attempt(constant("i8")).with(int::<_, i8>().map(Value::I8)),
            attempt(constant("i16")).with(int::<_, i16>().map(Value::I16)),
            attempt(constant("i32")).with(int::<_, i32>().map(Value::I32)),
            constant("i64").with(int::<_, i64>().map(Value::I64)),
            attempt(constant("v32")).with(uint::<_, u32>().map(Value::V32)),
            constant("v64").with(uint::<_, u64>().map(Value::V64)),
            attempt(constant("z32")).with(int::<_, i32>().map(Value::Z32)),
            constant("z64").with(int::<_, i64>().map(Value::Z64)),
            attempt(constant("f32")).with(flt::<_, f32>().map(Value::F32)),
            attempt(constant("f64")).with(flt::<_, f64>().map(Value::F64)),
        )),
        between(
            token('['),
            sptoken(']'),
            sep_by_tok(value(must_escape, esc), csep(), token(']')),
        )
        .map(|mut vals: LPooled<Vec<Value>>| {
            Value::Array(ValArray::from_iter_exact(vals.drain(..)))
        }),
        between(
            token('{'),
            sptoken('}'),
            sep_by_tok(
                (value(must_escape, esc), spstring("=>").with(value(must_escape, esc))),
                csep(),
                token('}'),
            )
            .map(|mut vals: LPooled<Vec<(Value, Value)>>| {
                Value::Map(immutable_chunkmap::map::Map::from_iter(vals.drain(..)))
            }),
        ),
        quoted(must_escape, esc).map(|s| Value::String(ArcStr::from(s))),
        flt::<_, f64>().map(Value::F64),
        int::<_, i64>().map(Value::I64),
        attempt(string("true").skip(not_prefix())).map(|_| Value::Bool(true)),
        attempt(string("false").skip(not_prefix())).map(|_| Value::Bool(false)),
        attempt(string("null").skip(not_prefix())).map(|_| Value::Null),
        constant("bytes")
            .with(base64())
            .map(|v| Value::Bytes(PBytes::new(Bytes::from(LPooled::detach(v))))),
        constant("abstract").with(base64()).then(|v| {
            match Abstract::decode(&mut &v[..]) {
                Ok(a) => combine::value(Value::Abstract(a)).right(),
                Err(_) => unexpected_any("failed to unpack abstract").left(),
            }
        }),
        constant("error").with(value(must_escape, esc)).map(|v| Value::Error(v.into())),
        attempt(constant("decimal"))
            .with(flt::<_, Decimal>())
            .map(|d| Value::Decimal(Arc::new(d))),
        attempt(constant("datetime"))
            .with(from_str(quoted(must_escape, esc)))
            .map(|d| Value::DateTime(Arc::new(d))),
        constant("duration").with(duration()).map(|d| Value::Duration(Arc::new(d))),
    )))
}

/// A duration's text, as `duration:` prefixes it and as it prints: a
/// float literal and its unit, ns, us (or µs), ms, s, m, h, d, M or y.
pub fn duration<I>() -> impl Parser<I, Output = Duration>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    float_text()
        .and(choice((
            attempt(string("ms")),
            string("ns"),
            string("us"),
            string("µs").map(|_| "us"),
            string("s"),
            string("m"),
            string("h"),
            string("d"),
            string("M"),
            string("y"),
        )))
        .then(|(n, unit): (CompactString, &'static str)| match duration_of(&n, unit) {
            Some(d) => combine::value(d).right(),
            None => unexpected_any("a duration that is negative or too long").left(),
        })
}

/// The seconds in a minute, hour, day, month and year — a month is
/// 30.44 days and a year 365.25 days, humantime's conventions.
const DURATION_UNITS: [(&str, u64); 5] =
    [("y", 31_557_600), ("M", 2_630_016), ("d", 86_400), ("h", 3_600), ("m", 60)];

const NANOS: u128 = 1_000_000_000;

/// `n`, a float literal's text, of the duration unit `suffix`, exactly,
/// to the nearest nanosecond, a half rounding up, whatever its digits and
/// exponent; `None` when `suffix` is no unit, or the duration is negative
/// or past `Duration::MAX`.
pub fn duration_of(n: &str, suffix: &str) -> Option<Duration> {
    use std::num::IntErrorKind;
    let (unit, places): (u64, i128) = match suffix {
        "ns" => (1, 0),
        "us" | "µs" => (1, 3),
        "ms" => (1, 6),
        "s" => (1, 9),
        unit => (DURATION_UNITS.iter().find(|(u, _)| *u == unit)?.1, 9),
    };
    let (mantissa, exp) = match n.split_once('e') {
        None => (n, 0),
        Some((m, e)) => match e.parse::<i64>() {
            Ok(e) => (m, e),
            Err(e) => match e.kind() {
                IntErrorKind::PosOverflow => (m, i64::MAX),
                IntErrorKind::NegOverflow => (m, i64::MIN),
                _ => return None,
            },
        },
    };
    let (whole, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let negative = whole.starts_with('-');
    let digits = || whole.trim_start_matches('-').bytes().chain(frac.bytes());
    if negative && digits().any(|c| c != b'0') {
        return None;
    }
    // The digits read as an integer, times unit, are the product whose
    // digit of index i weighs 10^(i + weight) ns; `place` is that weight
    // once it is at least 1, `None` past u128.
    let weight = places + exp as i128 - frac.len() as i128;
    let mut place = u32::try_from(weight.max(0)).ok().and_then(|w| 10u128.checked_pow(w));
    let (mut nanos, mut carry, mut half) = (0u128, 0u64, false);
    let mut digits = digits().rev();
    for i in 0.. {
        let d = match digits.next() {
            Some(c) => (c as char).to_digit(10)? as u64,
            None if carry == 0 => break,
            None => 0,
        };
        let product = d * unit + carry;
        let (digit, w) = (product % 10, i + weight);
        carry = product / 10;
        if w == -1 {
            half = digit >= 5
        } else if w >= 0 {
            if digit > 0 {
                nanos = nanos.checked_add(place?.checked_mul(digit as u128)?)?
            }
            place = place.and_then(|p| p.checked_mul(10))
        }
    }
    let nanos = nanos.checked_add(half as u128)?;
    let secs = u64::try_from(nanos / NANOS).ok()?;
    Some(Duration::new(secs, (nanos % NANOS) as u32))
}

/// How a duration prints: the count, written with its point, and the
/// largest literal unit that measures it exactly — whole years down to
/// whole minutes, else whole seconds, else whole milliseconds,
/// microseconds or nanoseconds below a second — and exact fractional
/// seconds when nothing does. Every form parses back to the same
/// duration.
pub fn duration_display(d: Duration) -> (CompactString, &'static str) {
    let (secs, ns) = (d.as_secs(), d.subsec_nanos());
    match (secs, ns) {
        (0, 0) => (CompactString::const_new("0."), "s"),
        (secs, 0) => {
            for (unit, len) in DURATION_UNITS {
                if secs % len == 0 {
                    return (format_compact!("{}.", secs / len), unit);
                }
            }
            (format_compact!("{secs}."), "s")
        }
        (0, ns) if ns % 1_000_000 == 0 => (format_compact!("{}.", ns / 1_000_000), "ms"),
        (0, ns) if ns % 1_000 == 0 => (format_compact!("{}.", ns / 1_000), "us"),
        (0, ns) => (format_compact!("{ns}."), "ns"),
        (secs, ns) => {
            let mut n = format_compact!("{secs}.{ns:09}");
            while n.ends_with('0') {
                n.pop();
            }
            (n, "s")
        }
    }
}

parser! {
    pub fn value['a, I](
        must_escape: &'static [char],
        esc: &'a Escape
    )(I) -> Value
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        grow(value_(must_escape, esc))
    }
}

/// `s`, all of it, as a value; whitespace around it allowed.
pub fn parse_value(s: &str) -> anyhow::Result<Value> {
    rd::value(s)
        .and_then(|(v, n)| rd::end(&s[n..]).map_err(|f| f.after(n)).map(|_| v))
        .map_err(|f| anyhow::anyhow!("{}", f.in_text(s)))
}

#[cfg(test)]
mod test;

#[cfg(test)]
mod tests {
    use arcstr::literal;

    use crate::Map;

    use super::*;

    #[test]
    fn parse() {
        assert_eq!(Value::U32(23), parse_value("u32:23").unwrap());
        assert_eq!(Value::V32(42), parse_value("v32:42").unwrap());
        assert_eq!(Value::I32(-10), parse_value("i32:-10").unwrap());
        assert_eq!(Value::I32(12321), parse_value("i32:12321").unwrap());
        assert_eq!(Value::Z32(-99), parse_value("z32:-99").unwrap());
        assert_eq!(Value::U64(100), parse_value("u64:100").unwrap());
        assert_eq!(Value::V64(100), parse_value("v64:100").unwrap());
        assert_eq!(Value::I64(-100), parse_value("i64:-100").unwrap());
        assert_eq!(Value::I64(-100), parse_value("-100").unwrap());
        assert_eq!(Value::I64(100), parse_value("i64:100").unwrap());
        assert_eq!(Value::I64(100), parse_value("100").unwrap());
        assert_eq!(Value::Z64(-100), parse_value("z64:-100").unwrap());
        assert_eq!(Value::Z64(100), parse_value("z64:100").unwrap());
        assert_eq!(Value::F32(3.1415), parse_value("f32:3.1415").unwrap());
        assert_eq!(Value::F32(675.6), parse_value("f32:675.6").unwrap());
        assert_eq!(Value::F32(42.3435), parse_value("f32:42.3435").unwrap());
        assert_eq!(Value::F32(1.123e9), parse_value("f32:1.123e9").unwrap());
        assert_eq!(Value::F32(1e9), parse_value("f32:1e9").unwrap());
        assert_eq!(Value::F32(21.2443e-6), parse_value("f32:21.2443e-6").unwrap());
        assert_eq!(Value::F32(3.), parse_value("f32:3.").unwrap());
        assert_eq!(Value::F64(3.1415), parse_value("f64:3.1415").unwrap());
        assert_eq!(Value::F64(3.1415), parse_value("3.1415").unwrap());
        assert_eq!(Value::F64(1.123e9), parse_value("1.123e9").unwrap());
        assert_eq!(Value::F64(1e9), parse_value("1e9").unwrap());
        assert_eq!(Value::F64(21.2443e-6), parse_value("21.2443e-6").unwrap());
        assert_eq!(Value::F64(3.), parse_value("f64:3.").unwrap());
        assert_eq!(Value::F64(3.), parse_value("3.").unwrap());
        let c = ArcStr::from(r#"I've got a lovely "bunch" of (coconuts)"#);
        let s = r#""I've got a lovely \"bunch\" of (coconuts)""#;
        assert_eq!(Value::String(c), parse_value(s).unwrap());
        let c = ArcStr::new();
        assert_eq!(Value::String(c), parse_value(r#""""#).unwrap());
        let c = ArcStr::from(r#"""#);
        let s = r#""\"""#;
        assert_eq!(Value::String(c), parse_value(s).unwrap());
        assert_eq!(Value::Bool(true), parse_value("true").unwrap());
        assert_eq!(Value::Bool(true), parse_value("true ").unwrap());
        assert_eq!(Value::Bool(false), parse_value("false").unwrap());
        assert_eq!(Value::Null, parse_value("null").unwrap());
        assert_eq!(
            Value::error(literal!("error")),
            parse_value(r#"error:"error""#).unwrap()
        );
        let a = ValArray::from_iter_exact(
            [Value::I64(42), Value::String(literal!("hello world"))].into_iter(),
        );
        assert_eq!(
            Value::Array(a.clone()),
            parse_value(r#"[42, "hello world", ]"#).unwrap()
        );
        assert_eq!(Value::Array(a), parse_value(r#"[42, "hello world"]"#).unwrap());
        let m = Map::from_iter([
            (Value::I64(42), Value::String(literal!("hello world"))),
            (Value::String(literal!("hello world")), Value::I64(42)),
        ]);
        assert_eq!(
            Value::Map(m.clone()),
            parse_value(r#"{ 42 => "hello world", "hello world" => 42, }"#).unwrap()
        );
        assert_eq!(
            Value::Map(m.clone()),
            parse_value(r#"{ 42 => "hello world", "hello world" => 42}"#).unwrap()
        );
        // hex literals
        assert_eq!(Value::U8(255), parse_value("u8:0xFF").unwrap());
        assert_eq!(Value::U8(255), parse_value("u8:0XFF").unwrap());
        assert_eq!(Value::I32(-31), parse_value("i32:-0x1F").unwrap());
        assert_eq!(Value::U64(0xDEAD), parse_value("u64:0xDEAD").unwrap());
        assert_eq!(Value::I64(255), parse_value("i64:0xFF").unwrap());
        // binary literals
        assert_eq!(Value::U16(10), parse_value("u16:0b1010").unwrap());
        assert_eq!(Value::U16(10), parse_value("u16:0B1010").unwrap());
        assert_eq!(Value::I8(-1), parse_value("i8:-0b1").unwrap());
        // octal literals
        assert_eq!(Value::U32(63), parse_value("u32:0o77").unwrap());
        assert_eq!(Value::U32(63), parse_value("u32:0O77").unwrap());
        assert_eq!(Value::I64(-8), parse_value("i64:-0o10").unwrap());
        // bare hex/bin/oct as i64
        assert_eq!(Value::I64(255), parse_value("0xFF").unwrap());
        assert_eq!(Value::I64(10), parse_value("0b1010").unwrap());
        assert_eq!(Value::I64(63), parse_value("0o77").unwrap());
        assert_eq!(Value::I64(-255), parse_value("-0xFF").unwrap());
    }
}
