use crate::{deftype, CachedArgs, CachedVals, EvalCached};
use anyhow::Result;
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use netidx::{path::Path, subscriber::Value, utils};
use netidx_bscript::{err, errf, Ctx, ExecCtx, UserEvent};
use netidx_value::ValArray;
use smallvec::SmallVec;
use std::cell::RefCell;

#[derive(Debug, Default)]
struct StartsWithEv;

impl EvalCached for StartsWithEv {
    const NAME: &str = "starts_with";
    deftype!("str", "fn(#pfx:string, string) -> bool");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(pfx)), Some(Value::String(val))) => {
                if val.starts_with(&**pfx) {
                    Some(Value::Bool(true))
                } else {
                    Some(Value::Bool(false))
                }
            }
            (None, _) | (_, None) => None,
            _ => err!("starts_with string arguments"),
        }
    }
}

type StartsWith = CachedArgs<StartsWithEv>;

#[derive(Debug, Default)]
struct EndsWithEv;

impl EvalCached for EndsWithEv {
    const NAME: &str = "ends_with";
    deftype!("str", "fn(#sfx:string, string) -> bool");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(sfx)), Some(Value::String(val))) => {
                if val.ends_with(&**sfx) {
                    Some(Value::Bool(true))
                } else {
                    Some(Value::Bool(false))
                }
            }
            (None, _) | (_, None) => None,
            _ => err!("ends_with string arguments"),
        }
    }
}

type EndsWith = CachedArgs<EndsWithEv>;

#[derive(Debug, Default)]
struct ContainsEv;

impl EvalCached for ContainsEv {
    const NAME: &str = "contains";
    deftype!("str", "fn(#part:string, string) -> bool");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(chs)), Some(Value::String(val))) => {
                if val.contains(&**chs) {
                    Some(Value::Bool(true))
                } else {
                    Some(Value::Bool(false))
                }
            }
            (None, _) | (_, None) => None,
            _ => err!("contains expected string"),
        }
    }
}

type Contains = CachedArgs<ContainsEv>;

#[derive(Debug, Default)]
struct StripPrefixEv;

impl EvalCached for StripPrefixEv {
    const NAME: &str = "strip_prefix";
    deftype!("str", "fn(#pfx:string, string) -> [string, null]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(pfx)), Some(Value::String(val))) => val
                .strip_prefix(&**pfx)
                .map(|s| Value::String(s.into()))
                .or(Some(Value::Null)),
            (None, _) | (_, None) => None,
            _ => err!("strip_prefix expected string"),
        }
    }
}

type StripPrefix = CachedArgs<StripPrefixEv>;

#[derive(Debug, Default)]
struct StripSuffixEv;

impl EvalCached for StripSuffixEv {
    const NAME: &str = "strip_suffix";
    deftype!("str", "fn(#sfx:string, string) -> [string, null]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(sfx)), Some(Value::String(val))) => val
                .strip_suffix(&**sfx)
                .map(|s| Value::String(s.into()))
                .or(Some(Value::Null)),
            (None, _) | (_, None) => None,
            _ => err!("strip_suffix expected string"),
        }
    }
}

type StripSuffix = CachedArgs<StripSuffixEv>;

#[derive(Debug, Default)]
struct TrimEv;

impl EvalCached for TrimEv {
    const NAME: &str = "trim";
    deftype!("str", "fn(string) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(val)) => Some(Value::String(val.trim().into())),
            None => None,
            _ => err!("trim expected string"),
        }
    }
}

type Trim = CachedArgs<TrimEv>;

#[derive(Debug, Default)]
struct TrimStartEv;

impl EvalCached for TrimStartEv {
    const NAME: &str = "trim_start";
    deftype!("str", "fn(string) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(val)) => Some(Value::String(val.trim_start().into())),
            None => None,
            _ => err!("trim_start expected string"),
        }
    }
}

type TrimStart = CachedArgs<TrimStartEv>;

#[derive(Debug, Default)]
struct TrimEndEv;

impl EvalCached for TrimEndEv {
    const NAME: &str = "trim_end";
    deftype!("str", "fn(string) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(val)) => Some(Value::String(val.trim_end().into())),
            None => None,
            _ => err!("trim_start expected string"),
        }
    }
}

type TrimEnd = CachedArgs<TrimEndEv>;

#[derive(Debug, Default)]
struct ReplaceEv;

impl EvalCached for ReplaceEv {
    const NAME: &str = "replace";
    deftype!("str", "fn(#pat:string, #rep:string, string) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1], &from.0[2]) {
            (
                Some(Value::String(pat)),
                Some(Value::String(rep)),
                Some(Value::String(val)),
            ) => Some(Value::String(val.replace(&**pat, &**rep).into())),
            (None, _, _) | (_, None, _) | (_, _, None) => None,
            _ => err!("replace expected string"),
        }
    }
}

type Replace = CachedArgs<ReplaceEv>;

#[derive(Debug, Default)]
struct DirnameEv;

impl EvalCached for DirnameEv {
    const NAME: &str = "dirname";
    deftype!("str", "fn(string) -> [null, string]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(path)) => match Path::dirname(path) {
                None if path != "/" => Some(Value::String(literal!("/"))),
                None => Some(Value::Null),
                Some(dn) => Some(Value::String(dn.into())),
            },
            None => None,
            _ => err!("dirname expected string"),
        }
    }
}

type Dirname = CachedArgs<DirnameEv>;

#[derive(Debug, Default)]
struct BasenameEv;

impl EvalCached for BasenameEv {
    const NAME: &str = "basename";
    deftype!("str", "fn(string) -> [null, string]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(path)) => match Path::basename(path) {
                None => Some(Value::Null),
                Some(dn) => Some(Value::String(dn.into())),
            },
            None => None,
            _ => err!("basename expected string"),
        }
    }
}

type Basename = CachedArgs<BasenameEv>;

#[derive(Debug, Default)]
struct StringJoinEv;

impl EvalCached for StringJoinEv {
    const NAME: &str = "string_join";
    deftype!("str", "fn(#sep:string, @args: [string, Array<string>]) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        thread_local! {
            static BUF: RefCell<String> = RefCell::new(String::new());
        }
        match &from.0[..] {
            [_] | [] => None,
            [None, ..] => None,
            [Some(sep), parts @ ..] => {
                // this is fairly common, so we check it before doing any real work
                for p in parts {
                    if p.is_none() {
                        return None;
                    }
                }
                let sep = match sep {
                    Value::String(c) => c.clone(),
                    sep => match sep.clone().cast_to::<ArcStr>().ok() {
                        Some(c) => c,
                        None => return err!("string_join, separator must be a string"),
                    },
                };
                BUF.with_borrow_mut(|buf| {
                    macro_rules! push {
                        ($c:expr) => {
                            if buf.is_empty() {
                                buf.push_str($c.as_str());
                            } else {
                                buf.push_str(sep.as_str());
                                buf.push_str($c.as_str());
                            }
                        };
                    }
                    buf.clear();
                    for p in parts {
                        match p.as_ref().unwrap() {
                            Value::String(c) => push!(c),
                            Value::Array(a) => {
                                for v in a.iter() {
                                    match v {
                                        Value::String(c) => push!(c),
                                        _ => {
                                            return err!(
                                                "string_join, components must be strings"
                                            )
                                        }
                                    }
                                }
                            }
                            _ => return err!("string_join, components must be strings"),
                        }
                    }
                    Some(Value::String(buf.as_str().into()))
                })
            }
        }
    }
}

type StringJoin = CachedArgs<StringJoinEv>;

#[derive(Debug, Default)]
struct StringConcatEv;

impl EvalCached for StringConcatEv {
    const NAME: &str = "string_concat";
    deftype!("str", "fn(@args: [string, Array<string>]) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        thread_local! {
            static BUF: RefCell<String> = RefCell::new(String::new());
        }
        let parts = &from.0[..];
        // this is a fairly common case, so we check it before doing any real work
        for p in parts {
            if p.is_none() {
                return None;
            }
        }
        BUF.with_borrow_mut(|buf| {
            buf.clear();
            for p in parts {
                match p.as_ref().unwrap() {
                    Value::String(c) => buf.push_str(c.as_ref()),
                    Value::Array(a) => {
                        for v in a.iter() {
                            match v {
                                Value::String(c) => buf.push_str(c.as_ref()),
                                _ => {
                                    return err!(
                                        "string_concat: arguments must be strings"
                                    )
                                }
                            }
                        }
                    }
                    _ => return err!("string_concat: arguments must be strings"),
                }
            }
            Some(Value::String(buf.as_str().into()))
        })
    }
}

type StringConcat = CachedArgs<StringConcatEv>;

#[derive(Debug, Default)]
struct StringEscapeEv {
    to_escape: SmallVec<[char; 8]>,
}

impl EvalCached for StringEscapeEv {
    const NAME: &str = "string_escape";
    deftype!("str", "fn(?#to_escape:string, ?#escape:string, string) -> [string, error]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        // this is a fairly common case, so we check it before doing any real work
        for p in &from.0[..] {
            if p.is_none() {
                return None;
            }
        }
        match &from.0[0] {
            Some(Value::String(s)) => {
                self.to_escape.clear();
                self.to_escape.extend(s.chars());
            }
            _ => return err!("escape: expected a string"),
        }
        let ec = match &from.0[1] {
            Some(Value::String(s)) if s.len() == 1 => s.chars().next().unwrap(),
            _ => return err!("escape: expected a single escape char"),
        };
        match &from.0[2] {
            Some(Value::String(s)) => {
                Some(Value::String(utils::escape(s, ec, &self.to_escape).into()))
            }
            _ => err!("escape: expected a string"),
        }
    }
}

type StringEscape = CachedArgs<StringEscapeEv>;

#[derive(Debug, Default)]
struct StringUnescapeEv;

impl EvalCached for StringUnescapeEv {
    const NAME: &str = "string_unescape";
    deftype!("str", "fn(?#escape:string, string) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        // this is a fairly common case, so we check it before doing any real work
        for p in &from.0[..] {
            if p.is_none() {
                return None;
            }
        }
        let ec = match &from.0[0] {
            Some(Value::String(s)) if s.len() == 1 => s.chars().next().unwrap(),
            _ => return err!("escape: expected a single escape char"),
        };
        match &from.0[1] {
            Some(Value::String(s)) => Some(Value::String(utils::unescape(s, ec).into())),
            _ => err!("escape: expected a string"),
        }
    }
}

type StringUnescape = CachedArgs<StringUnescapeEv>;

#[derive(Debug, Default)]
struct StringSplitEv;

impl EvalCached for StringSplitEv {
    const NAME: &str = "string_split";
    deftype!("str", "fn(#pat:string, string) -> Array<string>");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        // this is a fairly common case, so we check it before doing any real work
        for p in &from.0[..] {
            if p.is_none() {
                return None;
            }
        }
        let pat = match &from.0[0] {
            Some(Value::String(s)) => s,
            _ => return err!("split: expected string"),
        };
        match &from.0[1] {
            Some(Value::String(s)) => Some(Value::Array(ValArray::from_iter(
                s.split(&**pat).map(|s| Value::String(ArcStr::from(s))),
            ))),
            _ => err!("split: expected a string"),
        }
    }
}

type StringSplit = CachedArgs<StringSplitEv>;

#[derive(Debug, Default)]
struct StringSplitOnceEv;

impl EvalCached for StringSplitOnceEv {
    const NAME: &str = "string_split_once";
    deftype!("str", "fn(#pat:string, string) -> [null, (string, string)]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        // this is a fairly common case, so we check it before doing any real work
        for p in &from.0[..] {
            if p.is_none() {
                return None;
            }
        }
        let pat = match &from.0[0] {
            Some(Value::String(s)) => s,
            _ => return err!("split_once: expected string"),
        };
        match &from.0[1] {
            Some(Value::String(s)) => match s.split_once(&**pat) {
                None => Some(Value::Null),
                Some((s0, s1)) => Some(Value::Array(ValArray::from([
                    Value::String(s0.into()),
                    Value::String(s1.into()),
                ]))),
            },
            _ => err!("split_once: expected a string"),
        }
    }
}

type StringSplitOnce = CachedArgs<StringSplitOnceEv>;

#[derive(Debug, Default)]
struct StringRSplitOnceEv;

impl EvalCached for StringRSplitOnceEv {
    const NAME: &str = "string_rsplit_once";
    deftype!("str", "fn(#pat:string, string) -> [null, (string, string)]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        // this is a fairly common case, so we check it before doing any real work
        for p in &from.0[..] {
            if p.is_none() {
                return None;
            }
        }
        let pat = match &from.0[0] {
            Some(Value::String(s)) => s,
            _ => return err!("split_once: expected string"),
        };
        match &from.0[1] {
            Some(Value::String(s)) => match s.rsplit_once(&**pat) {
                None => Some(Value::Null),
                Some((s0, s1)) => Some(Value::Array(ValArray::from([
                    Value::String(s0.into()),
                    Value::String(s1.into()),
                ]))),
            },
            _ => err!("split_once: expected a string"),
        }
    }
}

type StringRSplitOnce = CachedArgs<StringRSplitOnceEv>;

#[derive(Debug, Default)]
struct StringToLowerEv;

impl EvalCached for StringToLowerEv {
    const NAME: &str = "string_to_lower";
    deftype!("str", "fn(string) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            None => None,
            Some(Value::String(s)) => Some(Value::String(s.to_lowercase().into())),
            _ => err!("to_lower: expected a string"),
        }
    }
}

type StringToLower = CachedArgs<StringToLowerEv>;

#[derive(Debug, Default)]
struct StringToUpperEv;

impl EvalCached for StringToUpperEv {
    const NAME: &str = "string_to_upper";
    deftype!("str", "fn(string) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            None => None,
            Some(Value::String(s)) => Some(Value::String(s.to_uppercase().into())),
            _ => err!("to_upper: expected a string"),
        }
    }
}

type StringToUpper = CachedArgs<StringToUpperEv>;

#[derive(Debug, Default)]
struct SprintfEv {
    buf: String,
    args: Vec<Value>,
}

impl EvalCached for SprintfEv {
    const NAME: &str = "string_sprintf";
    deftype!("str", "fn(string, @args: Any) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[..] {
            [Some(Value::String(fmt)), args @ ..] => {
                self.buf.clear();
                self.args.clear();
                for v in args {
                    match v {
                        Some(v) => self.args.push(v.clone()),
                        None => return None,
                    }
                }
                match netidx_value::printf(&mut self.buf, fmt, &self.args) {
                    Ok(_) => Some(Value::String(ArcStr::from(&self.buf))),
                    Err(e) => Some(Value::Error(ArcStr::from(e.to_string()))),
                }
            }
            _ => err!("sprintf invalid args"),
        }
    }
}

type Sprintf = CachedArgs<SprintfEv>;

#[derive(Debug, Default)]
struct LenEv;

impl EvalCached for LenEv {
    const NAME: &str = "string_len";
    deftype!("str", "fn(string) -> i64");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(s)) => Some(Value::I64(s.len() as i64)),
            _ => err!("sprintf invalid args"),
        }
    }
}

type Len = CachedArgs<LenEv>;

#[derive(Debug, Default)]
struct SubEv(String);

impl EvalCached for SubEv {
    const NAME: &str = "string_sub";
    deftype!("str", "fn(#start:i64, #len:i64, string) -> [string, error]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[..] {
            [Some(Value::I64(start)), Some(Value::I64(len)), Some(Value::String(s))]
                if *start >= 0 && *len >= 0 =>
            {
                let start = *start as usize;
                let end = start + *len as usize;
                self.0.clear();
                for (i, c) in s.chars().enumerate() {
                    if i >= start && i < end {
                        self.0.push(c);
                    }
                }
                Some(Value::String(ArcStr::from(&self.0)))
            }
            v => errf!("sub args must be non negative {v:?}"),
        }
    }
}

type Sub = CachedArgs<SubEv>;

#[derive(Debug, Default)]
struct ParseEv;

impl EvalCached for ParseEv {
    const NAME: &str = "string_parse";
    deftype!("str", "fn(string) -> Any");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(s)) => match s.parse::<Value>() {
                Ok(v) => Some(v),
                Err(e) => Some(Value::Error(e.to_string().into())),
            },
            _ => None,
        }
    }
}

type Parse = CachedArgs<ParseEv>;

pub(super) fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Result<ArcStr> {
    ctx.register_builtin::<StartsWith>()?;
    ctx.register_builtin::<EndsWith>()?;
    ctx.register_builtin::<Contains>()?;
    ctx.register_builtin::<StripPrefix>()?;
    ctx.register_builtin::<StripSuffix>()?;
    ctx.register_builtin::<Trim>()?;
    ctx.register_builtin::<TrimStart>()?;
    ctx.register_builtin::<TrimEnd>()?;
    ctx.register_builtin::<Replace>()?;
    ctx.register_builtin::<Dirname>()?;
    ctx.register_builtin::<Basename>()?;
    ctx.register_builtin::<StringJoin>()?;
    ctx.register_builtin::<StringConcat>()?;
    ctx.register_builtin::<StringEscape>()?;
    ctx.register_builtin::<StringUnescape>()?;
    ctx.register_builtin::<StringSplit>()?;
    ctx.register_builtin::<StringSplitOnce>()?;
    ctx.register_builtin::<StringRSplitOnce>()?;
    ctx.register_builtin::<StringToLower>()?;
    ctx.register_builtin::<StringToUpper>()?;
    ctx.register_builtin::<Sprintf>()?;
    ctx.register_builtin::<Len>()?;
    ctx.register_builtin::<Sub>()?;
    ctx.register_builtin::<Parse>()?;
    Ok(literal!(include_str!("str.bs")))
}
