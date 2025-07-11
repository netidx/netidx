use crate::{deftype, CachedArgs, CachedVals, EvalCached};
use anyhow::Result;
use arcstr::{literal, ArcStr};
use compact_str::format_compact;
use netidx::subscriber::Value;
use netidx_bscript::{errf, Ctx, ExecCtx, UserEvent};
use netidx_value::ValArray;
use regex::Regex;

fn maybe_compile(s: &str, re: &mut Option<Regex>) -> Result<()> {
    let compile = match re {
        None => true,
        Some(re) => re.as_str() != s,
    };
    if compile {
        *re = Some(Regex::new(s)?)
    }
    Ok(())
}

#[derive(Debug, Default)]
struct IsMatchEv {
    re: Option<Regex>,
}

impl EvalCached for IsMatchEv {
    const NAME: &str = "re_is_match";
    deftype!("re", "fn(#pat:string, string) -> [bool, error]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        if let Some(Value::String(s)) = &from.0[0] {
            if let Err(e) = maybe_compile(s, &mut self.re) {
                return errf!("{e:?}");
            }
        }
        if let Some(Value::String(s)) = &from.0[1] {
            if let Some(re) = self.re.as_ref() {
                return Some(Value::Bool(re.is_match(s)));
            }
        }
        None
    }
}

type IsMatch = CachedArgs<IsMatchEv>;

#[derive(Debug, Default)]
struct FindEv {
    re: Option<Regex>,
}

impl EvalCached for FindEv {
    const NAME: &str = "re_find";
    deftype!("re", "fn(#pat:string, string) -> [Array<string>, error]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        if let Some(Value::String(s)) = &from.0[0] {
            if let Err(e) = maybe_compile(s, &mut self.re) {
                return errf!("{e:?}");
            }
        }
        if let Some(Value::String(s)) = &from.0[1] {
            if let Some(re) = self.re.as_ref() {
                let a = ValArray::from_iter(
                    re.find_iter(s).map(|s| Value::String(s.as_str().into())),
                );
                return Some(Value::Array(a));
            }
        }
        None
    }
}

type Find = CachedArgs<FindEv>;

#[derive(Debug, Default)]
struct CapturesEv {
    re: Option<Regex>,
}

impl EvalCached for CapturesEv {
    const NAME: &str = "re_captures";
    deftype!("re", "fn(#pat:string, string) -> [Array<Array<[null, string]>>, error]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        if let Some(Value::String(s)) = &from.0[0] {
            if let Err(e) = maybe_compile(s, &mut self.re) {
                return errf!("{e:?}");
            }
        }
        if let Some(Value::String(s)) = &from.0[1] {
            if let Some(re) = self.re.as_ref() {
                let a = ValArray::from_iter(re.captures_iter(s).map(|c| {
                    let a = ValArray::from_iter(c.iter().map(|m| match m {
                        None => Value::Null,
                        Some(m) => Value::String(m.as_str().into()),
                    }));
                    Value::Array(a)
                }));
                return Some(Value::Array(a));
            }
        }
        None
    }
}

type Captures = CachedArgs<CapturesEv>;

#[derive(Debug, Default)]
struct SplitEv {
    re: Option<Regex>,
}

impl EvalCached for SplitEv {
    const NAME: &str = "re_split";
    deftype!("re", "fn(#pat:string, string) -> [Array<string>, error]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        if let Some(Value::String(s)) = &from.0[0] {
            if let Err(e) = maybe_compile(s, &mut self.re) {
                return errf!("{e:?}");
            }
        }
        if let Some(Value::String(s)) = &from.0[1] {
            if let Some(re) = self.re.as_ref() {
                let a = ValArray::from_iter(re.split(s).map(|s| Value::String(s.into())));
                return Some(Value::Array(a));
            }
        }
        None
    }
}

type Split = CachedArgs<SplitEv>;

#[derive(Debug, Default)]
struct SplitNEv {
    re: Option<Regex>,
    lim: Option<usize>,
}

impl EvalCached for SplitNEv {
    const NAME: &str = "re_splitn";
    deftype!("re", "fn(#pat:string, #limit:i64, string) -> [Array<string>, error]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        if let Some(Value::String(s)) = &from.0[0] {
            if let Err(e) = maybe_compile(s, &mut self.re) {
                return errf!("{e:?}");
            }
        }
        if let Some(Value::I64(lim)) = &from.0[1] {
            self.lim = Some(*lim as usize);
        }
        if let Some(Value::String(s)) = &from.0[2] {
            if let Some(lim) = self.lim {
                if let Some(re) = self.re.as_ref() {
                    let a = ValArray::from_iter(
                        re.splitn(s, lim).map(|s| Value::String(s.into())),
                    );
                    return Some(Value::Array(a));
                }
            }
        }
        None
    }
}

type SplitN = CachedArgs<SplitNEv>;

pub(super) fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Result<ArcStr> {
    ctx.register_builtin::<IsMatch>()?;
    ctx.register_builtin::<Find>()?;
    ctx.register_builtin::<Captures>()?;
    ctx.register_builtin::<Split>()?;
    ctx.register_builtin::<SplitN>()?;
    Ok(literal!(include_str!("re.bs")))
}
