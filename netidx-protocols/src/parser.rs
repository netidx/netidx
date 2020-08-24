use crate::view::{Sink, Source};
use combine::{
    between, choice, from_str, many, many1, one_of, optional,
    parser::{
        char::{digit, spaces, string},
        combinator::recognize,
        range::{take_while, take_while1},
        repeat::escaped,
    },
    sep_by1,
    stream::{position, Range},
    token, EasyParser, ParseError, Parser, RangeStream,
};
use netidx::{chars::Chars, path::Path, publisher::Value};

fn quoted<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(between(
        token('"'),
        token('"'),
        recognize(escaped(take_while1(|c| c != '"'), '\\', token('"'))),
    ))
}

fn uint<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    many1(digit())
}

fn int<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((optional(token('-')), take_while1(|c: char| c.is_digit(10))))
}

fn flt<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((digit(), optional(token('.')), take_while(|c: char| c.is_digit(10))))
}

fn fname<I>() -> impl Parser<I, Output = String>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    recognize((
        take_while1(|c| c >= 'a' || c <= 'z'),
        take_while(|c| c >= 'a' || c <= 'z' || c >= '0' || c <= '9' || c == '_'),
    ))
}

fn source_<I>() -> impl Parser<I, Output = Source>
where
    I: RangeStream<Token = char>,
    I::Error: ParseError<I::Token, I::Range, I::Position>,
    I::Range: Range,
{
    spaces().with(choice((
        string("n:").with(quoted().map(|s| Source::Load(Path::from(s)))),
        string("v:").with(fname()).map(|s| Source::Variable(s)),
        string("u32:").with(from_str(uint()).map(|v| Source::Constant(Value::U32(v)))),
        string("v32:").with(from_str(uint()).map(|v| Source::Constant(Value::V32(v)))),
        string("i32:").with(from_str(int()).map(|v| Source::Constant(Value::I32(v)))),
        string("z32:").with(from_str(int()).map(|v| Source::Constant(Value::Z32(v)))),
        string("u64:").with(from_str(uint()).map(|v| Source::Constant(Value::U64(v)))),
        string("v64:").with(from_str(uint()).map(|v| Source::Constant(Value::V64(v)))),
        string("i64:").with(from_str(int()).map(|v| Source::Constant(Value::I64(v)))),
        string("z64:").with(from_str(int()).map(|v| Source::Constant(Value::Z64(v)))),
        string("f32:").with(from_str(flt()).map(|v| Source::Constant(Value::F32(v)))),
        string("f64:").with(from_str(flt()).map(|v| Source::Constant(Value::F32(v)))),
        string("string:")
            .with(quoted())
            .map(|v| Source::Constant(Value::String(Chars::from(v)))),
        string("true").map(|_| Source::Constant(Value::True)),
        string("false").map(|_| Source::Constant(Value::False)),
        string("null").map(|_| Source::Constant(Value::Null)),
        string("ok").map(|_| Source::Constant(Value::Ok)),
        string("err:")
            .with(quoted())
            .map(|s| Source::Constant(Value::Error(Chars::from(s)))),
        (
            fname(),
            spaces().with(between(
                token('('),
                token(')'),
                sep_by1(source(), spaces().with(token(','))),
            )),
        )
            .map(|(function, from)| Source::Map { from, function }),
    )))
}

parser! {
    fn source[I]()(I) -> Source
    where [I: RangeStream<Token = char>, I::Range: Range]
    {
        source_()
    }
}

fn parse_source(s: &str) -> anyhow::Result<Source> {
    source()
        .easy_parse(position::Stream::new(s))
        .map(|(r, _)| r)
        .map_err(|e| anyhow::anyhow!(format!("{}", e)))
}

#[cfg(test)]
mod tests {
    #[test]
    fn source_parse() {
        assert_eq!(Ok(Source::Constant(Value::U32(23))), parse_source(" u32:23 "));
        assert_eq!(Ok(Source::Constant(Value::V32(42))), parse_source("v32:42"));
        assert_eq!(Ok(Source::Constant(Value::I32(-10))), parse_source("i32:-10"));
        assert_eq!(Ok(Source::Constant(Value::I32(12321))), parse_source("i32:12321"));
        assert_eq!(Ok(Source::Constant(Value::Z32(-99))), parse_source("z32:-99"));
        assert_eq!(Ok(Source::Constant(Value::U64(100))), parse_source("u64:100"));
        assert_eq!(Ok(Source::Constant(Value::V64(100))), parse_source("v64:100"));
        assert_eq!(Ok(Source::Constant(Value::I64(-100))), parse_source("i64:-100"));
        assert_eq!(Ok(Source::Constant(Value::I64(-100))), parse_source("i64:100"));
        assert_eq!(Ok(Source::Constant(Value::Z64(-100))), parse_source("z64:-100"));
        assert_eq!(Ok(Source::Constant(Value::Z64(100))), parse_source("z64:100"));
        assert_eq!(Ok(Source::Constant(Value::F32(3.1415))), parse_source("f32:3.1415"));
        assert_eq!(Ok(Source::Constant(Value::F32(3))), parse_source("f32:3"));
        assert_eq!(Ok(Source::Constant(Value::F32(3))), parse_source("f32:3."));
        assert_eq!(Ok(Source::Constant(Value::F64(3.1415))), parse_source("f64:3.1415"));
        assert_eq!(Ok(Source::Constant(Value::F64(3))), parse_source("f64:3."));
        assert_eq!(Ok(Source::Constant(Value::F64(3))), parse_source("f64:3"));
        let c = Chars::from(r#"I've got a lovely "bunch" of coconuts"#);
        let s = r#"string:"I've got a lovely \"bunch\" of coconuts""#;
        assert_eq!(Ok(Source::Constant(Value::String(c))), parse_source(s));
        assert_eq!(Ok(Source::Constant(Value::True)), parse_source("true"));
        assert_eq!(Ok(Source::Constant(Value::False)), parse_source("false"));
        assert_eq!(Ok(Source::Constant(Value::Null)), parse_source("null"));
        assert_eq!(Ok(Source::Constant(Value::Ok)), parse_source("ok"));
        assert_eq!(
            Ok(Source::Constant(Value::Err("error"))),
            parse_source(r#"err:"error""#)
        );
        let p = Path::from(r#"/foo bar baz/"zam"/_ xyz+ "#);
        let s = r#"n:"/foo bar baz/\"zam\"/_ xyz+ ""#;
        assert_eq!(Ok(Source::Load(p)), parse_source(s));
        assert_eq!(Ok(Source::Variable("sum")), parse_source("v:sum"));
        let src = Source::Map {
            from: vec![
                Source::Constant(Value::f32(1)),
                Source::Load(Path::from("/foo/bar")),
                Source::Map {
                    from: vec![
                        Source::Constant(Value::f32(0)),
                        Source::Load(Path::from("/foo/baz"))
                    ],
                    function: String::from("max")
                }
            ],
            function: String::from("sum"),
        };
        let chs = r#"sum(f32:1, n:"/foo/bar", max(f32:0, n:"/foo/baz"))"#
        assert_eq!(Ok(src), parse_source(chs));
    }
}
