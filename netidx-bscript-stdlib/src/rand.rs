use crate::{deftype, CachedVals};
use anyhow::Result;
use arcstr::{literal, ArcStr};
use netidx::subscriber::Value;
use netidx_bscript::{
    Apply, BuiltIn, BuiltInInitFn, Ctx, Event, ExecCtx, Node, UserEvent,
};
use netidx_value::ValArray;
use rand::{rng, seq::SliceRandom, Rng};
use smallvec::{smallvec, SmallVec};
use std::sync::Arc;

#[derive(Debug)]
struct Rand {
    args: CachedVals,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Rand {
    const NAME: &str = "rand";
    deftype!("rand", "fn<'a: [Int, Float]>(?#start:'a, ?#end:'a, #clock:Any) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, from, _| Ok(Box::new(Rand { args: CachedVals::new(from) })))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Rand {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        macro_rules! gen_cases {
            ($start:expr, $end:expr, $($typ:ident),+) => {
                match ($start, $end) {
                    $(
                        (Value::$typ(start), Value::$typ(end)) if start < end => {
                            Some(Value::$typ(rng().random_range(*start..*end)))
                        }
                    ),+
                    _ => None
                }
            };
        }
        let up = self.args.update(ctx, from, event);
        if up {
            match &self.args.0[..] {
                [Some(start), Some(end), Some(_)] => gen_cases!(
                    start, end, F32, F64, I32, I64, Z32, Z64, U32, U64, V32, V64
                ),
                _ => None,
            }
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct Pick;

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Pick {
    const NAME: &str = "rand_pick";
    deftype!("rand", "fn(Array<'a>) -> 'a");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(Pick)))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Pick {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        from[0].update(ctx, event).and_then(|a| match a {
            Value::Array(a) if a.len() > 0 => {
                Some(a[rng().random_range(0..a.len())].clone())
            }
            _ => None,
        })
    }
}

#[derive(Debug)]
struct Shuffle(SmallVec<[Value; 32]>);

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Shuffle {
    const NAME: &str = "rand_shuffle";
    deftype!("rand", "fn(Array<'a>) -> Array<'a>");

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, _, _| Ok(Box::new(Shuffle(smallvec![]))))
    }
}

impl<C: Ctx, E: UserEvent> Apply<C, E> for Shuffle {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        from[0].update(ctx, event).and_then(|a| match a {
            Value::Array(a) => {
                self.0.extend(a.iter().cloned());
                self.0.shuffle(&mut rng());
                Some(Value::Array(ValArray::from_iter_exact(self.0.drain(..))))
            }
            _ => None,
        })
    }
}

pub(super) fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Result<ArcStr> {
    ctx.register_builtin::<Rand>()?;
    ctx.register_builtin::<Pick>()?;
    ctx.register_builtin::<Shuffle>()?;
    Ok(literal!(include_str!("rand.bs")))
}
