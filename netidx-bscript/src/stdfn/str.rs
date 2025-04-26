use crate::{
    deftype, err,
    expr::Expr,
    stdfn::{CachedArgs, CachedVals, EvalCached},
    Ctx, ExecCtx, UserEvent,
};
use arcstr::{literal, ArcStr};
use netidx::{path::Path, subscriber::Value};
use std::cell::RefCell;

#[derive(Default)]
struct StartsWithEv;

impl EvalCached for StartsWithEv {
    const NAME: &str = "starts_with";
    deftype!("fn(#pfx:string, string) -> bool");

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
    deftype!("fn(#sfx:string, string) -> bool");

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
    deftype!("fn(#part:string, string) -> bool");

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
    deftype!("fn(#pfx:string, string) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(pfx)), Some(Value::String(val))) => {
                val.strip_prefix(&**pfx).map(|s| Value::String(s.into()))
            }
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
    deftype!("fn(#sfx:string, string) -> string");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(sfx)), Some(Value::String(val))) => {
                val.strip_suffix(&**sfx).map(|s| Value::String(s.into()))
            }
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
    deftype!("fn(string) -> string");

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
    deftype!("fn(string) -> string");

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
    deftype!("fn(string) -> string");

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
    deftype!("fn(string, string, string) -> string");

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
    deftype!("fn(string) -> [null, string]");

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
    deftype!("fn(string) -> [null, string]");

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
    deftype!("fn(string, @args: string) -> string");

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
                    buf.clear();
                    for p in parts {
                        let c = match p.as_ref().unwrap() {
                            Value::String(c) => c.clone(),
                            v => match v.clone().cast_to::<ArcStr>().ok() {
                                Some(c) => c,
                                None => {
                                    return err!(
                                        "string_join, components must be strings"
                                    )
                                }
                            },
                        };
                        if buf.is_empty() {
                            buf.push_str(c.as_str());
                        } else {
                            buf.push_str(sep.as_str());
                            buf.push_str(c.as_str());
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
    deftype!("fn(@args: Any) -> string");

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
                    v => match v.clone().cast_to::<ArcStr>().ok() {
                        Some(c) => buf.push_str(c.as_ref()),
                        None => return err!("string_concat: arguments must be strings"),
                    },
                }
            }
            Some(Value::String(buf.as_str().into()))
        })
    }
}

type StringConcat = CachedArgs<StringConcatEv>;

const MOD: &str = r#"
pub mod str {
    pub let starts_with = |#pfx, s| 'starts_with;
    pub let ends_with = |#sfx, s| 'ends_with;
    pub let contains = |#part, s| 'contains;
    pub let strip_prefix = |#pfx, s| 'strip_prefix;
    pub let strip_suffix = |#sfx, s| 'strip_suffix;
    pub let trim = |s| 'trim;
    pub let trim_start = |s| 'trim_start;
    pub let trim_end = |s| 'trim_end;
    pub let replace = |pattern, replacement, s| 'replace;
    pub let dirname = |path| 'dirname;
    pub let basename = |path| 'basename;
    pub let join = |sep, @args| 'string_join;
    pub let concat = |@args| 'string_concat
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
    MOD.parse().unwrap()
}
