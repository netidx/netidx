use crate::{
    err,
    expr::Expr,
    stdfn::{CachedArgs, CachedVals, EvalCached},
    vm::{Arity, Ctx, ExecCtx},
};
use arcstr::{literal, ArcStr};
use netidx::{path::Path, subscriber::Value};
use std::{cell::RefCell, fmt::Debug};

struct StartsWithEv;

impl EvalCached for StartsWithEv {
    const NAME: &str = "starts_with";
    const ARITY: Arity = Arity::Exactly(2);

    fn eval(from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(pfx)), Some(Value::String(val))) => {
                if val.starts_with(&**pfx) {
                    Some(Value::True)
                } else {
                    Some(Value::False)
                }
            }
            (None, _) | (_, None) => None,
            _ => err!("starts_with string arguments"),
        }
    }
}

type StartsWith = CachedArgs<StartsWithEv>;

struct EndsWithEv;

impl EvalCached for EndsWithEv {
    const NAME: &str = "ends_with";
    const ARITY: Arity = Arity::Exactly(2);

    fn eval(from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(sfx)), Some(Value::String(val))) => {
                if val.ends_with(&**sfx) {
                    Some(Value::True)
                } else {
                    Some(Value::False)
                }
            }
            (None, _) | (_, None) => None,
            _ => err!("ends_with string arguments"),
        }
    }
}

type EndsWith = CachedArgs<EndsWithEv>;

struct ContainsEv;

impl EvalCached for ContainsEv {
    const NAME: &str = "contains";
    const ARITY: Arity = Arity::Exactly(2);

    fn eval(from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(chs)), Some(Value::String(val))) => {
                if val.contains(&**chs) {
                    Some(Value::True)
                } else {
                    Some(Value::False)
                }
            }
            (None, _) | (_, None) => None,
            _ => err!("contains expected string"),
        }
    }
}

type Contains = CachedArgs<ContainsEv>;

struct StripPrefixEv;

impl EvalCached for StripPrefixEv {
    const NAME: &str = "strip_prefix";
    const ARITY: Arity = Arity::Exactly(2);

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct StripSuffixEv;

impl EvalCached for StripSuffixEv {
    const NAME: &str = "strip_suffix";
    const ARITY: Arity = Arity::Exactly(2);

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct TrimEv;

impl EvalCached for TrimEv {
    const NAME: &str = "trim";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(val)) => Some(Value::String(val.trim().into())),
            None => None,
            _ => err!("trim expected string"),
        }
    }
}

type Trim = CachedArgs<TrimEv>;

struct TrimStartEv;

impl EvalCached for TrimStartEv {
    const NAME: &str = "trim_start";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(val)) => Some(Value::String(val.trim_start().into())),
            None => None,
            _ => err!("trim_start expected string"),
        }
    }
}

type TrimStart = CachedArgs<TrimStartEv>;

struct TrimEndEv;

impl EvalCached for TrimEndEv {
    const NAME: &str = "trim_end";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(val)) => Some(Value::String(val.trim_end().into())),
            None => None,
            _ => err!("trim_start expected string"),
        }
    }
}

type TrimEnd = CachedArgs<TrimEndEv>;

struct ReplaceEv;

impl EvalCached for ReplaceEv {
    const NAME: &str = "replace";
    const ARITY: Arity = Arity::Exactly(3);

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct DirnameEv;

impl EvalCached for DirnameEv {
    const NAME: &str = "dirname";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct BasenameEv;

impl EvalCached for BasenameEv {
    const NAME: &str = "basename";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct StringJoinEv;

impl EvalCached for StringJoinEv {
    const NAME: &str = "string_join";
    const ARITY: Arity = Arity::AtLeast(2);

    fn eval(from: &CachedVals) -> Option<Value> {
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

struct StringConcatEv;

impl EvalCached for StringConcatEv {
    const NAME: &str = "string_concat";
    const ARITY: Arity = Arity::AtLeast(1);

    fn eval(from: &CachedVals) -> Option<Value> {
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
    pub let starts_with = |prefix, s| 'starts_with
    pub let ends_with = |suffix, s| 'ends_with
    pub let contains = |part, s| 'contains
    pub let strip_prefix = |prefix, s| 'strip_prefix
    pub let strip_suffix = |suffix, s| 'strip_suffix
    pub let trim = |s| 'trim
    pub let trim_start = |s| 'trim_start
    pub let trim_end = |s| 'trim_end
    pub let replace = |pattern, replacement, s| 'replace
    pub let dirname = |path| 'dirname
    pub let basename = |path| 'basename
    pub let join = |sep, @args| 'string_join
    pub let concat = |s, @args| 'string_concat
}
"#;

pub fn register<C: Ctx, E: Debug + Clone>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<StartsWith>();
    ctx.register_builtin::<EndsWith>();
    ctx.register_builtin::<Contains>();
    ctx.register_builtin::<StripPrefix>();
    ctx.register_builtin::<StripSuffix>();
    ctx.register_builtin::<Trim>();
    ctx.register_builtin::<TrimStart>();
    ctx.register_builtin::<TrimEnd>();
    ctx.register_builtin::<Replace>();
    ctx.register_builtin::<Dirname>();
    ctx.register_builtin::<Basename>();
    ctx.register_builtin::<StringJoin>();
    ctx.register_builtin::<StringConcat>();
    MOD.parse().unwrap()
}
