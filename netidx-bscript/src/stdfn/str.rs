use crate::{
    err,
    stdfn::{CachedArgs, CachedVals, EvalCached},
    vm::Arity,
};
use netidx::{chars::Chars, path::Path, subscriber::Value};

pub struct StartsWithEv;

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

pub type StartsWith = CachedArgs<StartsWithEv>;

pub struct EndsWithEv;

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

pub type EndsWith = CachedArgs<EndsWithEv>;

pub struct ContainsEv;

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

pub type Contains = CachedArgs<ContainsEv>;

pub struct StripPrefixEv;

impl EvalCached for StripPrefixEv {
    const NAME: &str = "strip_prefix";
    const ARITY: Arity = Arity::Exactly(2);

    fn eval(from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(pfx)), Some(Value::String(val))) => val
                .strip_prefix(&**pfx)
                .map(|s| Value::String(Chars::from(String::from(s)))),
            (None, _) | (_, None) => None,
            _ => err!("strip_prefix expected string"),
        }
    }
}

pub type StripPrefix = CachedArgs<StripPrefixEv>;

pub struct StripSuffixEv;

impl EvalCached for StripSuffixEv {
    const NAME: &str = "strip_suffix";
    const ARITY: Arity = Arity::Exactly(2);

    fn eval(from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1]) {
            (Some(Value::String(sfx)), Some(Value::String(val))) => val
                .strip_suffix(&**sfx)
                .map(|s| Value::String(Chars::from(String::from(s)))),
            (None, _) | (_, None) => None,
            _ => err!("strip_suffix expected string"),
        }
    }
}

pub type StripSuffix = CachedArgs<StripSuffixEv>;

pub struct TrimEv;

impl EvalCached for TrimEv {
    const NAME: &str = "trim";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(val)) => {
                Some(Value::String(Chars::from(String::from(val.trim()))))
            }
            None => None,
            _ => err!("trim expected string"),
        }
    }
}

pub type Trim = CachedArgs<TrimEv>;

pub struct TrimStartEv;

impl EvalCached for TrimStartEv {
    const NAME: &str = "trim_start";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(val)) => {
                Some(Value::String(Chars::from(String::from(val.trim_start()))))
            }
            None => None,
            _ => err!("trim_start expected string"),
        }
    }
}

pub type TrimStart = CachedArgs<TrimStartEv>;

pub struct TrimEndEv;

impl EvalCached for TrimEndEv {
    const NAME: &str = "trim_end";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(val)) => {
                Some(Value::String(Chars::from(String::from(val.trim_end()))))
            }
            None => None,
            _ => err!("trim_start expected string"),
        }
    }
}

pub type TrimEnd = CachedArgs<TrimEndEv>;

pub struct ReplaceEv;

impl EvalCached for ReplaceEv {
    const NAME: &str = "replace";
    const ARITY: Arity = Arity::Exactly(3);

    fn eval(from: &CachedVals) -> Option<Value> {
        match (&from.0[0], &from.0[1], &from.0[2]) {
            (
                Some(Value::String(pat)),
                Some(Value::String(rep)),
                Some(Value::String(val)),
            ) => Some(Value::String(Chars::from(String::from(
                val.replace(&**pat, &**rep),
            )))),
            (None, _, _) | (_, None, _) | (_, _, None) => None,
            _ => err!("replace expected string"),
        }
    }
}

pub type Replace = CachedArgs<ReplaceEv>;

pub struct DirnameEv;

impl EvalCached for DirnameEv {
    const NAME: &str = "dirname";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(path)) => match Path::dirname(path) {
                None => Some(Value::Null),
                Some(dn) => Some(Value::String(Chars::from(String::from(dn)))),
            },
            None => None,
            _ => err!("dirname expected string"),
        }
    }
}

pub type Dirname = CachedArgs<DirnameEv>;

pub struct BasenameEv;

impl EvalCached for BasenameEv {
    const NAME: &str = "basename";
    const ARITY: Arity = Arity::Exactly(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        match &from.0[0] {
            Some(Value::String(path)) => match Path::basename(path) {
                None => Some(Value::Null),
                Some(dn) => Some(Value::String(Chars::from(String::from(dn)))),
            },
            None => None,
            _ => err!("basename expected string"),
        }
    }
}

pub type Basename = CachedArgs<BasenameEv>;

pub struct StringJoinEv;

impl EvalCached for StringJoinEv {
    const NAME: &str = "string_join";
    const ARITY: Arity = Arity::AtLeast(2);

    fn eval(from: &CachedVals) -> Option<Value> {
        use bytes::BytesMut;
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
                    sep => match sep.clone().cast_to::<Chars>().ok() {
                        Some(c) => c,
                        None => return err!("string_join, separator must be a string"),
                    },
                };
                let mut res = BytesMut::new();
                for p in parts {
                    let c = match p.as_ref().unwrap() {
                        Value::String(c) => c.clone(),
                        v => match v.clone().cast_to::<Chars>().ok() {
                            Some(c) => c,
                            None => {
                                return err!("string_join, components must be strings")
                            }
                        },
                    };
                    if res.is_empty() {
                        res.extend_from_slice(c.bytes());
                    } else {
                        res.extend_from_slice(sep.bytes());
                        res.extend_from_slice(c.bytes());
                    }
                }
                Some(Value::String(Chars::from_bytes(res.freeze()).unwrap()))
            }
        }
    }
}

pub type StringJoin = CachedArgs<StringJoinEv>;

pub struct StringConcatEv;

impl EvalCached for StringConcatEv {
    const NAME: &str = "string_concat";
    const ARITY: Arity = Arity::AtLeast(1);

    fn eval(from: &CachedVals) -> Option<Value> {
        use bytes::BytesMut;
        let parts = &from.0[..];
        // this is a fairly common case, so we check it before doing any real work
        for p in parts {
            if p.is_none() {
                return None;
            }
        }
        let mut res = BytesMut::new();
        for p in parts {
            match p.as_ref().unwrap() {
                Value::String(c) => res.extend_from_slice(c.bytes()),
                v => match v.clone().cast_to::<Chars>().ok() {
                    Some(c) => res.extend_from_slice(c.bytes()),
                    None => return err!("string_concat: arguments must be strings"),
                },
            }
        }
        Some(Value::String(Chars::from_bytes(res.freeze()).unwrap()))
    }
}

pub type StringConcat = CachedArgs<StringConcatEv>;
