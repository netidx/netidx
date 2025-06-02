use crate::{
    deftype, err,
    expr::Expr,
    stdfn::{CachedArgs, CachedVals, EvalCached},
    Ctx, ExecCtx, UserEvent,
};
use arcstr::{literal, ArcStr};
use netidx::{path::Path, subscriber::Value, utils};
use netidx_netproto::valarray::ValArray;
use smallvec::SmallVec;
use std::cell::RefCell;

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
struct DirnameEv;

impl EvalCached for DirnameEv {
    const NAME: &str = "dirname";
    deftype!("str", "fn(string) -> [null, string]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(path)) => match Path::dirname(path) {
                None => Some(Value::Null),
                Some(dn) => Some(Value::String(dn.into())),
            },
            None => None,
            _ => err!("dirname expected string"),
        }
    }
}

type Dirname = CachedArgs<DirnameEv>;

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

const MOD: &str = r#"
pub mod str {
    /// return true if s starts with #pfx, otherwise return false
    pub let starts_with = |#pfx, s| 'starts_with;

    /// return true if s ends with #sfx otherwise return false
    pub let ends_with = |#sfx, s| 'ends_with;

    /// return true if s contains #part, otherwise return false
    pub let contains = |#part, s| 'contains;

    /// if s starts with #pfx then return s with #pfx stripped otherwise return null
    pub let strip_prefix = |#pfx, s| 'strip_prefix;

    /// if s ends with #sfx then return s with #sfx stripped otherwise return null
    pub let strip_suffix = |#sfx, s| 'strip_suffix;

    /// return s with leading and trailing whitespace removed
    pub let trim = |s| 'trim;

    /// return s with leading whitespace removed
    pub let trim_start = |s| 'trim_start;

    /// return s with trailing whitespace removed
    pub let trim_end = |s| 'trim_end;

    /// replace all instances of #pat in s with #rep and return s
    pub let replace = |#pat, #rep, s| 'replace;

    /// return the parent path of s, or null if s does not have a parent path
    pub let dirname = |path| 'dirname;

    /// return the leaf path of s, or null if s is not a path. e.g. /foo/bar -> bar
    pub let basename = |path| 'basename;

    /// return a single string with the arguments concatenated and separated by #sep
    pub let join = |#sep, @args| 'string_join;

    /// concatenate the specified strings into a single string
    pub let concat = |@args| 'string_concat;

    /// escape all the charachters in #to_escape in s with the escape charachter #escape.
    /// The escape charachter must appear in #to_escape
    pub let escape = |#to_escape = "/", #escape = "\\", s| 'string_escape;

    /// unescape all the charachters in s escaped by the specified #escape charachter
    pub let unescape = |#escape = "\\", s| 'string_unescape;

    /// split the string by the specified #pat and return an array of each part
    pub let split = |#pat, s| 'string_split;

    /// split the string once from the beginning by #pat and return a
    /// tuple of strings, or return null if #pat was not found in the string
    pub let split_once = |#pat, s| 'string_split_once;

    /// split the string once from the end by #pat and return a tuple of strings
    /// or return null if #pat was not found in the string    
    pub let rsplit_once = |#pat, s| 'string_rsplit_once;

    /// change the string to lowercase
    pub let to_lower = |s| 'string_to_lower;

    /// change the string to uppercase
    pub let to_upper = |s| 'string_to_upper
}
"#;

pub fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<StartsWith>().unwrap();
    ctx.register_builtin::<EndsWith>().unwrap();
    ctx.register_builtin::<Contains>().unwrap();
    ctx.register_builtin::<StripPrefix>().unwrap();
    ctx.register_builtin::<StripSuffix>().unwrap();
    ctx.register_builtin::<Trim>().unwrap();
    ctx.register_builtin::<TrimStart>().unwrap();
    ctx.register_builtin::<TrimEnd>().unwrap();
    ctx.register_builtin::<Replace>().unwrap();
    ctx.register_builtin::<Dirname>().unwrap();
    ctx.register_builtin::<Basename>().unwrap();
    ctx.register_builtin::<StringJoin>().unwrap();
    ctx.register_builtin::<StringConcat>().unwrap();
    ctx.register_builtin::<StringEscape>().unwrap();
    ctx.register_builtin::<StringUnescape>().unwrap();
    ctx.register_builtin::<StringSplit>().unwrap();
    ctx.register_builtin::<StringSplitOnce>().unwrap();
    ctx.register_builtin::<StringRSplitOnce>().unwrap();
    ctx.register_builtin::<StringToLower>().unwrap();
    ctx.register_builtin::<StringToUpper>().unwrap();
    MOD.parse().unwrap()
}
