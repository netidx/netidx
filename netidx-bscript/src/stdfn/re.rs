use crate::{
    deftype, errf,
    expr::Expr,
    stdfn::{CachedArgs, CachedVals, EvalCached},
    Ctx, ExecCtx, UserEvent,
};
use anyhow::Result;
use arcstr::ArcStr;
use compact_str::format_compact;
use netidx::subscriber::Value;
use netidx_netproto::valarray::ValArray;
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
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

#[derive(Default)]
struct SplitNEv {
    re: Option<Regex>,
    lim: Option<usize>,
}

impl EvalCached for SplitNEv {
    const NAME: &str = "re_splitn";
    deftype!("re", "fn(#pat:string, #limit:u64, string) -> [Array<string>, error]");

    fn eval(&mut self, from: &CachedVals) -> Option<Value> {
        if let Some(Value::String(s)) = &from.0[0] {
            if let Err(e) = maybe_compile(s, &mut self.re) {
                return errf!("{e:?}");
            }
        }
        if let Some(Value::U64(lim)) = &from.0[1] {
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

const MOD: &str = r#"
pub mod re {
  /// return true if the string is matched by #pat, otherwise return false.
  /// return an error if #pat is invalid.
  pub let is_match = |#pat, s| 're_is_match;

  /// return an array of instances of #pat in s. return an error if #pat is
  /// invalid.
  pub let find = |#pat, s| 're_find;

  /// return an array of captures matched by #pat. The array will have an element for each
  /// capture, regardless of whether it matched or not. If it did not match the corresponding
  /// element will be null. Return an error if #pat is invalid.
  pub let captures = |#pat, s| 're_captures;

  /// return an array of strings split by #pat. return an error if #pat is invalid.
  pub let split = |#pat, s| 're_split;

  /// split the string by #pat at most #limit times and return an array of the parts.
  /// return an error if #pat is invalid
  pub let splitn = |#pat, #limit, s| 're_splitn
}
"#;

pub fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<IsMatch>().unwrap();
    ctx.register_builtin::<Find>().unwrap();
    ctx.register_builtin::<Captures>().unwrap();
    ctx.register_builtin::<Split>().unwrap();
    ctx.register_builtin::<SplitN>().unwrap();
    MOD.parse().unwrap()
}
