use anyhow::Result;
use arcstr::ArcStr;
use enumflags2::{bitflags, BitFlags};
use fxhash::FxHashMap;
use netidx::{path::Path, subscriber::Value};
use netidx_bscript::{
    expr::ModuleResolver, typ::FnType, Apply, BuiltIn, BuiltInInitFn, Ctx, Event,
    ExecCtx, Node, UserEvent,
};
use netidx_core::utils::Either;
use std::{
    fmt::Debug,
    iter,
    sync::{Arc, LazyLock},
};

mod array;
mod core;
mod net;
mod rand;
mod re;
mod str;
#[cfg(test)]
mod test;
mod time;

#[macro_export]
macro_rules! deftype {
    ($scope:literal, $s:literal) => {
        const TYP: ::std::sync::LazyLock<netidx_bscript::typ::FnType> =
            ::std::sync::LazyLock::new(|| {
                let scope =
                    netidx_bscript::expr::ModPath(::netidx::path::Path::from($scope));
                netidx_bscript::expr::parser::parse_fn_type($s)
                    .expect("failed to parse fn type {s}")
                    .scope_refs(&scope)
            });
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

#[derive(Debug)]
pub struct CachedVals(pub Box<[Option<Value>]>);

impl CachedVals {
    pub fn new<C: Ctx, E: UserEvent>(from: &[Node<C, E>]) -> CachedVals {
        CachedVals(from.into_iter().map(|_| None).collect())
    }

    pub fn update<C: Ctx, E: UserEvent>(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
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
    pub fn update_diff<C: Ctx, E: UserEvent>(
        &mut self,
        up: &mut [bool],
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) {
        for (i, n) in from.iter_mut().enumerate() {
            match n.update(ctx, event) {
                None => (),
                v => {
                    self.0[i] = v;
                    up[i] = true
                }
            }
        }
    }

    pub fn flat_iter<'a>(&'a self) -> impl Iterator<Item = Option<Value>> + 'a {
        self.0.iter().flat_map(|v| match v {
            None => Either::Left(iter::once(None)),
            Some(v) => Either::Right(v.clone().flatten().map(Some)),
        })
    }
}

pub trait EvalCached: Debug + Default + Send + Sync + 'static {
    const NAME: &str;
    const TYP: LazyLock<FnType>;

    fn eval(&mut self, from: &CachedVals) -> Option<Value>;
}

#[derive(Debug)]
pub struct CachedArgs<T: EvalCached> {
    cached: CachedVals,
    t: T,
}

impl<C: Ctx, E: UserEvent, T: EvalCached> BuiltIn<C, E> for CachedArgs<T> {
    const NAME: &str = T::NAME;
    const TYP: LazyLock<FnType> = T::TYP;

    fn init(_: &mut ExecCtx<C, E>) -> BuiltInInitFn<C, E> {
        Arc::new(|_, _, _, from, _| {
            let t = CachedArgs::<T> { cached: CachedVals::new(from), t: T::default() };
            Ok(Box::new(t))
        })
    }
}

impl<C: Ctx, E: UserEvent, T: EvalCached> Apply<C, E> for CachedArgs<T> {
    fn update(
        &mut self,
        ctx: &mut ExecCtx<C, E>,
        from: &mut [Node<C, E>],
        event: &mut Event<E>,
    ) -> Option<Value> {
        if self.cached.update(ctx, from, event) {
            self.t.eval(&self.cached)
        } else {
            None
        }
    }
}

#[bitflags]
#[derive(Clone, Copy)]
#[repr(u64)]
pub enum Module {
    Array,
    NetAndTime,
    Rand,
    Re,
    Str,
}

/// Register selected modules of the standard bscript library and
/// return a root module that will load them along with a module
/// resolver that contains the necessary code. You need both of these
/// for the `rt` module.
///
/// Note, core is always included and registered, all the other
/// modules are optional
///
/// # Example
///
/// ```no_run
/// use netidx::{publisher::Publisher, subscriber::Subscriber};
/// use anyhow::Result;
/// use netidx_core::pool::Pooled;
/// use netidx_bscript::{ExecCtx, rt::{BSCtx, BSConfigBuilder, BSHandle, RtEvent}};
/// use futures::channel::mpsc;
/// use enumflags2::BitFlags;
///
/// async fn start_runtime(
///     publisher: Publisher,
///     subscriber: Subscriber,
///     sub: mpsc::Sender<Pooled<Vec<RtEvent>>>
/// ) -> Result<BSHandle> {
///     let mut ctx = ExecCtx::new(BSCtx::new(publisher, subscriber));
///     let (root, mods) = netidx_bscript_stdlib::register(&mut ctx, BitFlags::all())?;
///     BSConfigBuilder::default()
///        .ctx(ctx)
///        .root(root)
///        .resolvers(vec![mods])
///        .sub(sub)
///        .build()?
///        .start()
///        .await
/// }
/// ```
pub fn register<C: Ctx, E: UserEvent>(
    ctx: &mut ExecCtx<C, E>,
    modules: BitFlags<Module>,
) -> Result<(ArcStr, ModuleResolver)> {
    let mut tbl = FxHashMap::default();
    tbl.insert(Path::from("/core"), core::register(ctx)?);
    let mut root = String::from("pub mod core;\nuse core;\n");
    for module in modules {
        match module {
            Module::Array => {
                root.push_str("pub mod array;\n");
                tbl.insert(Path::from("/array"), array::register(ctx)?);
            }
            Module::NetAndTime => {
                root.push_str("pub mod time;\n");
                tbl.insert(Path::from("/time"), time::register(ctx)?);
                root.push_str("pub mod net;\n");
                tbl.insert(Path::from("/net"), net::register(ctx)?);
            }
            Module::Rand => {
                root.push_str("pub mod rand;\n");
                tbl.insert(Path::from("/rand"), rand::register(ctx)?);
            }
            Module::Re => {
                root.push_str("pub mod re;\n");
                tbl.insert(Path::from("/re"), re::register(ctx)?);
            }
            Module::Str => {
                root.push_str("pub mod str;\n");
                tbl.insert(Path::from("/str"), str::register(ctx)?);
            }
        }
    }
    root.pop();
    root.pop();
    root.push('\n');
    Ok((ArcStr::from(root), ModuleResolver::VFS(tbl)))
}
