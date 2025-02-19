use crate::{node::Node, typ::FnType, Apply, BuiltIn, Ctx, Event, ExecCtx, InitFn};
use netidx::subscriber::Value;
use netidx_core::utils::Either;
use smallvec::SmallVec;
use std::{
    fmt::Debug,
    iter,
    marker::PhantomData,
    sync::{Arc, LazyLock},
};

pub mod core;
pub mod net;
pub mod str;
pub mod time;

#[macro_export]
macro_rules! deftype {
    ($s:literal) => {
        const TYP: LazyLock<FnType> =
            LazyLock::new(|| parse_fn_type($s).expect("failed to parse fn type {s}"));
    };
}

#[macro_export]
macro_rules! errf {
    ($pat:expr, $($arg:expr),*) => { Some(Value::Error(ArcStr::from(format_compact!($pat, $($arg),*).as_str()))) };
    ($pat:expr) => { Some(Value::Error(ArcStr::from(format_compact!($pat).as_str()))) };
}

#[macro_export]
macro_rules! err {
    ($pat:literal) => {
        Some(Value::Error(literal!($pat)))
    };
}

#[macro_export]
macro_rules! arity1 {
    ($from:expr, $updates:expr) => {
        match (&*$from, &*$updates) {
            ([arg], [arg_up]) => (arg, arg_up),
            (_, _) => unreachable!(),
        }
    };
}

#[macro_export]
macro_rules! arity2 {
    ($from:expr, $updates:expr) => {
        match (&*$from, &*$updates) {
            ([arg0, arg1], [arg0_up, arg1_up]) => ((arg0, arg1), (arg0_up, arg1_up)),
            (_, _) => unreachable!(),
        }
    };
}

pub struct CachedVals(pub SmallVec<[Option<Value>; 4]>);

impl CachedVals {
    pub fn new<C: Ctx, E: Debug + Clone>(from: &[Node<C, E>]) -> CachedVals {
        CachedVals(from.into_iter().map(|_| None).collect())
    }

    pub fn update<C: Ctx, E: Debug + Clone>(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> bool {
        from.into_iter().enumerate().fold(false, |res, (i, src)| {
            match src.update(ctx, event) {
                None => res,
                v @ Some(_) => {
                    self.0[i] = v;
                    true
                }
            }
        })
    }

    /// Like update, but return the indexes of the nodes that updated
    /// instead of a consolidated bool
    pub fn update_diff<C: Ctx, E: Debug + Clone>(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> SmallVec<[bool; 4]> {
        from.into_iter()
            .enumerate()
            .map(|(i, src)| match src.update(ctx, event) {
                None => false,
                v @ Some(_) => {
                    self.0[i] = v;
                    true
                }
            })
            .collect()
    }

    /// Only update if the value changes, return the indexes of nodes that updated
    pub fn update_changed<C: Ctx, E: Debug + Clone>(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> SmallVec<[bool; 4]> {
        from.into_iter()
            .enumerate()
            .map(|(i, src)| match src.update(ctx, event) {
                v @ Some(_) if self.0[i] != v => {
                    self.0[i] = v;
                    true
                }
                Some(_) | None => false,
            })
            .collect()
    }

    pub fn flat_iter<'a>(&'a self) -> impl Iterator<Item = Option<Value>> + 'a {
        self.0.iter().flat_map(|v| match v {
            None => Either::Left(iter::once(None)),
            Some(v) => Either::Right(v.clone().flatten().map(Some)),
        })
    }
}

pub trait EvalCached {
    const NAME: &str;
    const TYP: LazyLock<FnType>;

    fn eval(from: &CachedVals) -> Option<Value>;
}

pub struct CachedArgs<T: EvalCached + Send + Sync> {
    cached: CachedVals,
    t: PhantomData<T>,
}

impl<C: Ctx, E: Debug + Clone, T: EvalCached + Send + Sync + 'static> BuiltIn<C, E>
    for CachedArgs<T>
{
    const NAME: &str = T::NAME;
    const TYP: LazyLock<FnType> = T::TYP;

    fn init(_: &mut ExecCtx<C, E>) -> InitFn<C, E> {
        Arc::new(|_, _, from, _| {
            let t = CachedArgs::<T> { cached: CachedVals::new(from), t: PhantomData };
            Ok(Box::new(t))
        })
    }
}

impl<C: Ctx, E: Debug + Clone, T: EvalCached + Send + Sync + 'static> Apply<C, E>
    for CachedArgs<T>
{
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &Event<E>,
    ) -> Option<Value> {
        if self.cached.update(ctx, from, event) {
            T::eval(&self.cached)
        } else {
            None
        }
    }
}
