use crate::{
    deftype, expr::Expr, node::Node, stdfn::CachedVals, Apply, BuiltIn, BuiltInInitFn,
    Ctx, Event, ExecCtx, UserEvent,
};
use netidx::subscriber::Value;
use netidx_netproto::valarray::ValArray;
use rand::{seq::SliceRandom, thread_rng, Rng};
use smallvec::{smallvec, SmallVec};
use std::sync::Arc;

struct Rand {
    args: CachedVals,
}

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Rand {
    const NAME: &str = "rand";
    deftype!("fn<'a: [Int, Float]>(?#start:'a, ?#end:'a, #trigger:Any) -> 'a");

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
                            Some(Value::$typ(thread_rng().gen_range(*start..*end)))
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

struct Pick;

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Pick {
    const NAME: &str = "rand_pick";
    deftype!("fn(Array<'a>) -> 'a");

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
                Some(a[thread_rng().gen_range(0..a.len())].clone())
            }
            _ => None,
        })
    }
}

struct Shuffle(SmallVec<[Value; 32]>);

impl<C: Ctx, E: UserEvent> BuiltIn<C, E> for Shuffle {
    const NAME: &str = "rand_shuffle";
    deftype!("fn(Array<'a>) -> Array<'a>");

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
                self.0.shuffle(&mut thread_rng());
                Some(Value::Array(ValArray::from_iter_exact(self.0.drain(..))))
            }
            _ => None,
        })
    }
}

const MOD: &str = r#"
pub mod rand {
    /// generate a random number between #start and #end (exclusive)
    /// every time #trigger updates. If start and end are not specified,
    /// they default to 0.0 and 1.0
    pub let rand = |#start = 0.0, #end = 1.0, #trigger| 'rand;

    /// pick a random element from the array and return it. Update
    /// each time the array updates. If the array is empty return
    /// nothing.
    pub let pick = |a| 'rand_pick;

    /// return a shuffled copy of a
    pub let shuffle = |a| 'rand_shuffle
}
"#;

pub fn register<C: Ctx, E: UserEvent>(ctx: &mut ExecCtx<C, E>) -> Expr {
    ctx.register_builtin::<Rand>().unwrap();
    ctx.register_builtin::<Pick>().unwrap();
    ctx.register_builtin::<Shuffle>().unwrap();
    MOD.parse().unwrap()
}
