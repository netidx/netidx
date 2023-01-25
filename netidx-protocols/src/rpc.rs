use anyhow::Result;
use arcstr::ArcStr;
use futures::{
    channel::{mpsc, oneshot},
    future,
    prelude::*,
    select_biased, stream,
};
use fxhash::{FxBuildHasher, FxHashMap};
use log::{error, info};
use netidx::{
    chars::Chars,
    path::Path,
    pool::{Pool, Pooled},
    protocol::glob::{Glob, GlobSet},
    publisher::{
        ClId, Id, PublishFlags, Publisher, SendResult, Val, Value, WriteRequest,
    },
    subscriber::{Dval, Subscriber, SubscriberId},
};
use parking_lot::Mutex;
use std::{
    borrow::Borrow,
    collections::HashMap,
    iter,
    ops::Drop,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};
use tokio::{sync::Mutex as AsyncMutex, task};

#[macro_use]
pub mod server {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::*;

    atomic_id!(ProcId);

    /// for use in map functions, will reply to the client with an error and return None
    #[macro_export]
    macro_rules! rpc_err {
        ($reply:expr, $msg:expr) => {{
            $reply.send(Value::Error(Chars::from($msg)));
            return None;
        }};
    }

    /// defines a new rpc.
    /// `define_rpc!(publisher, path, doc, mapfn, tx, arg: typ = default; doc, ...)`
    /// see `Proc` for an example
    #[macro_export]
    macro_rules! define_rpc {
        (
            $publisher:expr,
            $path:expr,
            $topdoc:expr,
            $map:expr,
            $tx:expr,
            $($arg:ident: $typ:ty = $default:expr; $doc:expr),*
        ) => {{
            let map = move |mut c: RpcCall| {
                $(
                    let d = Value::from($default);
                    let $arg = match c.args.remove(stringify!($arg)).unwrap_or(d).cast_to::<$typ>() {
                        Ok(t) => t,
                        Err(_) => rpc_err!(c.reply, format!("arg: {} invalid type conversion", stringify!($arg)))
                    };
                )*
                if c.args.len() != 0 {
                    rpc_err!(c.reply, format!("unknown argument specified: {:?}", c.args.keys().collect::<Vec<_>>()))
                }
                $map(c, $($arg),*)
            };
            let args = [
                $(ArgSpec {name: ArcStr::from(stringify!($arg)), default_value: Value::from($default), doc: Value::from($doc)}),*
            ];
            Proc::new($publisher, $path, Value::from($topdoc), args, map, $tx)
        }}
    }

    lazy_static! {
        static ref ARGS: Pool<HashMap<ArcStr, Value>> = Pool::new(10000, 50);
    }

    pub struct RpcReply(Option<SendResult>);

    impl Drop for RpcReply {
        fn drop(&mut self) {
            if let Some(reply) = self.0.take() {
                let _ = reply.send(Value::Error(Chars::from("rpc call failed")));
            }
        }
    }

    impl RpcReply {
        pub fn send(&mut self, m: Value) {
            if let Some(res) = self.0.take() {
                res.send(m);
            }
        }
    }

    #[derive(Debug, Clone)]
    pub struct ArgSpec {
        pub name: ArcStr,
        pub doc: Value,
        pub default_value: Value,
    }

    pub struct RpcCall {
        pub client: ClId,
        pub id: ProcId,
        pub args: Pooled<HashMap<ArcStr, Value>>,
        pub reply: RpcReply,
    }

    struct Arg {
        name: ArcStr,
        _value: Val,
        _doc: Val,
    }

    struct PendingCall {
        args: Pooled<HashMap<ArcStr, Value>>,
        initiated: Instant,
    }

    struct ProcInner<M: FnMut(RpcCall) -> Option<T> + Send + 'static, T: Send + 'static> {
        id: ProcId,
        call: Arc<Val>,
        _doc: Val,
        args: HashMap<Id, Arg, FxBuildHasher>,
        pending: HashMap<ClId, PendingCall, FxBuildHasher>,
        handler: Option<mpsc::Sender<T>>,
        map: M,
        events: stream::Fuse<mpsc::Receiver<Pooled<Vec<WriteRequest>>>>,
        stop: future::Fuse<oneshot::Receiver<()>>,
        last_gc: Instant,
    }

    impl<M, T> ProcInner<M, T>
    where
        M: FnMut(RpcCall) -> Option<T> + Send + 'static,
        T: Send + 'static,
    {
        async fn run(mut self) {
            static GC_FREQ: Duration = Duration::from_secs(1);
            static GC_THRESHOLD: usize = 128;
            fn gc_pending(
                pending: &mut HashMap<ClId, PendingCall, FxBuildHasher>,
                now: Instant,
            ) {
                static STALE: Duration = Duration::from_secs(60);
                pending.retain(|_, pc| now - pc.initiated < STALE);
                pending.shrink_to_fit();
            }
            let mut stop = self.stop;
            loop {
                select_biased! {
                    _ = stop => break,
                    mut batch = self.events.select_next_some() => for req in batch.drain(..) {
                        if req.id == self.call.id() {
                            let args = self.pending.remove(&req.client).map(|pc| pc.args)
                                .unwrap_or_else(|| ARGS.take());
                            let call = RpcCall {
                                client: req.client,
                                id: self.id,
                                args,
                                reply: RpcReply(req.send_result),
                            };
                            let t = match catch_unwind(AssertUnwindSafe(|| (self.map)(call))) {
                                Ok(t) => t,
                                Err(_) => {
                                    error!("rpc map args panic");
                                    continue
                                }
                            };
                            if let Some(t) = t {
                                if let Some(handler) = &mut self.handler {
                                    let _: std::result::Result<_, _> = handler.send(t).await;
                                }
                            }
                        } else {
                            let mut gc = false;
                            let pending = self.pending.entry(req.client)
                                .or_insert_with(|| {
                                    gc = true;
                                    PendingCall {
                                        args: ARGS.take(),
                                        initiated: Instant::now()
                                    }
                                });
                            if let Some(Arg {name, ..}) = self.args.get(&req.id) {
                                pending.args.insert(name.clone(), req.value);
                            }
                            if gc && self.pending.len() > GC_THRESHOLD {
                                let now = Instant::now();
                                if now - self.last_gc > GC_FREQ {
                                    self.last_gc = now;
                                    gc_pending(&mut self.pending, now);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// A remote procedure published in netidx
    pub struct Proc {
        _stop: oneshot::Sender<()>,
        id: ProcId,
    }

    impl Proc {
        /**
        Publish a new remote procedure. If successful this will return
        a `Proc` which, if dropped, will cause the removal of the
        procedure from netidx.

        # Arguments

        * `publisher` - A reference to the publisher that will publish the procedure.
        * `name` - The path of the procedure in netidx.
        * `doc` - The procedure level doc string to be published along with the procedure
        * `args` - An iterator containing the procedure arguments
        * `map` - A function that will map the raw parameters into the type of the channel.
          if it returns None then nothing will be pushed into the channel.
        * `handler` - The channel that will receive the rpc call invocations (if any)

        If you can handle the procedure entirely without async (or blocking) then you only
        need to define map, you don't need to pass a handler channel. Your map function should
        handle the call, reply to the client, and return None.

        If you need to do something async in order to handle the call, then you must pass
        an mpsc channel that will receive the output of your map function. You can define
        as little or as much slack as you desire, however be aware that if the channel fills up
        then clients attempting to call your procedure will wait.

        # Example
        ```no_run
        #[macro_use] extern crate netidx_protocols;
        use netidx::{path::Path, subscriber::Value, chars::Chars};
        use netidx_protocols::rpc::server::{Proc, ArgSpec, RpcCall};
        use arcstr::ArcStr;
        # use anyhow::Result;
        # async fn z() -> Result<()> {
        #   let publisher = unimplemented!();
            let echo = define_rpc!(
                &publisher,
                Path::from("/examples/api/echo"),
                "echos it's argument",
                |mut c: RpcCall, arg: Value| -> Option<()> {
                    c.reply.send(arg);
                    None
                },
                None,
                arg: Value = Value::Null; "argument to echo"
            );
        #   drop(echo);
        #   Ok(())
        # }
        ```

        # Notes

        If more than one publisher is publishing the same compatible
        RPC (same arguments, same name, hopefully the same
        semantics!), then clients will randomly pick one procedure
        from the set at client creation time.

        Arguments with the same key that are specified multiple times
        will overwrite previous versions; the procedure will receive
        only the last version set.
         **/
        pub fn new<T: Send + 'static, F: FnMut(RpcCall) -> Option<T> + Send + 'static>(
            publisher: &Publisher,
            name: Path,
            doc: Value,
            args: impl IntoIterator<Item = ArgSpec>,
            map: F,
            handler: Option<mpsc::Sender<T>>,
        ) -> Result<Proc> {
            let id = ProcId::new();
            let (tx_ev, rx_ev) = mpsc::channel(3);
            let (tx_stop, rx_stop) = oneshot::channel();
            let call = Arc::new(publisher.publish_with_flags(
                PublishFlags::USE_EXISTING,
                name.clone(),
                Value::Null,
            )?);
            let _doc = publisher.publish_with_flags(
                PublishFlags::USE_EXISTING,
                name.append("doc"),
                doc,
            )?;
            publisher.writes(call.id(), tx_ev.clone());
            let args = args
                .into_iter()
                .map(|ArgSpec { name: arg, doc, default_value }| {
                    let base = name.append(&*arg);
                    let _value = publisher
                        .publish_with_flags(
                            PublishFlags::USE_EXISTING,
                            base.append("val"),
                            default_value,
                        )
                        .map(|val| {
                            publisher.writes(val.id(), tx_ev.clone());
                            val
                        })?;
                    let _doc = publisher.publish_with_flags(
                        PublishFlags::USE_EXISTING,
                        base.append("doc"),
                        doc,
                    )?;
                    Ok((_value.id(), Arg { name: arg, _value, _doc }))
                })
                .collect::<Result<HashMap<Id, Arg, FxBuildHasher>>>()?;
            let inner = ProcInner {
                id,
                call,
                _doc,
                args,
                pending: HashMap::with_hasher(FxBuildHasher::default()),
                map,
                handler,
                events: rx_ev.fuse(),
                stop: rx_stop.fuse(),
                last_gc: Instant::now(),
            };
            task::spawn(async move {
                inner.run().await;
                info!("rpc proc {} shutdown", name);
            });
            Ok(Proc { id, _stop: tx_stop })
        }

        /// Get the rpc procedure id
        pub fn id(&self) -> ProcId {
            self.id
        }
    }
}

#[macro_use]
pub mod client {
    use super::*;

    lazy_static! {
        // The same procedure can't be called concurrently from the
        // same subscriber. If it is, the arguments of the two calls
        // could be permuted. This structure ensures that this does
        // not happen.
        static ref PROCS: Mutex<FxHashMap<SubscriberId, FxHashMap<Path, Weak<AsyncMutex<()>>>>> =
            Mutex::new(HashMap::with_hasher(FxBuildHasher::default()));
    }

    /// Convenience macro for calling rpcs.
    /// `call_rpc!(proc, arg0: 3, arg1: "foo", arg2: vec!["foo", "bar", "baz"])`
    #[macro_export]
    macro_rules! call_rpc {
        ($proc:expr, $($name:ident: $arg:expr),*) => {
            $proc.call([
                $(
                    (stringify!($name), Value::try_from($arg)?)
                )*,
            ])
        }
    }

    #[derive(Debug)]
    struct ProcInner {
        name: Path,
        sid: SubscriberId,
        lock: Option<Arc<AsyncMutex<()>>>,
        call: Dval,
        args: HashMap<String, Dval>,
    }

    impl Drop for ProcInner {
        fn drop(&mut self) {
            let mut procs = PROCS.lock();
            drop(self.lock.take());
            if let Some(procs_by_sub) = procs.get_mut(&self.sid) {
                if let Some(weak_lock) = procs_by_sub.get(&self.name) {
                    if weak_lock.strong_count() == 0 {
                        procs_by_sub.remove(&self.name);
                        if procs_by_sub.is_empty() {
                            procs.remove(&self.sid);
                        }
                    }
                }
            }
        }
    }

    #[derive(Debug, Clone)]
    pub struct Proc(Arc<ProcInner>);

    impl Proc {
        /// Subscribe to the procedure specified by `name`, if
        /// successful return a `Proc` structure that may be used to
        /// call the procedure. Dropping the `Proc` structure will
        /// unsubscribe from the procedure and free all associated
        /// resources.
        pub async fn new(subscriber: &Subscriber, name: Path) -> Result<Proc> {
            let sid = subscriber.id();
            let lock = {
                let mut locks = PROCS.lock();
                let lock = locks
                    .entry(sid)
                    .or_insert_with(|| HashMap::with_hasher(FxBuildHasher::default()))
                    .entry(name.clone())
                    .or_insert_with(Weak::new);
                match Weak::upgrade(lock) {
                    Some(lock) => Some(lock),
                    None => {
                        let m = Arc::new(AsyncMutex::new(()));
                        *lock = Arc::downgrade(&m);
                        Some(m)
                    }
                }
            };
            let call = subscriber.subscribe(name.clone());
            let pat = GlobSet::new(
                true,
                iter::once(Glob::new(Chars::from(format!("{}/*/val", name.clone())))?),
            )?;
            let mut args = HashMap::new();
            let mut batches = subscriber.resolver().list_matching(&pat).await?;
            for mut batch in batches.drain(..) {
                for arg_path in batch.drain(..) {
                    let arg_name =
                        Path::basename(Path::dirname(&*arg_path).unwrap()).unwrap();
                    args.insert(String::from(arg_name), subscriber.subscribe(arg_path));
                }
            }
            Ok(Proc(Arc::new(ProcInner { name, sid, lock, call, args })))
        }

        /**
        Call the procedure. `call` may be reused to call the procedure again.

        # Example
        ```no_run
        #[macro_use] extern crate netidx_protocols;
        use netidx::{path::Path, subscriber::Value};
        use netidx_protocols::rpc::client::Proc;
        # use anyhow::Result;
        # async fn z() -> Result<()> {
        #   let subscriber = unimplemented!();
            let echo = Proc::new(subscriber, Path::from("/examples/api/echo")).await?;
            let v = call_rpc!(echo, arg1: "hello echo").await?;
        #   drop(echo);
        #   Ok(())
        # }
        ```

        # Notes

        `call` may safely be called concurrently on multiple
        instances of `Proc` that call the same procedure
        (there is internal syncronization).
        **/
        pub async fn call<I, K>(&self, args: I) -> Result<Value>
        where
            I: IntoIterator<Item = (K, Value)>,
            K: Borrow<str>,
        {
            let result = {
                let _guard = self.0.lock.as_ref().unwrap().lock().await;
                for (name, val) in args {
                    match self.0.args.get(name.borrow()) {
                        None => bail!("no such argument {}", name.borrow()),
                        Some(dv) => {
                            dv.wait_subscribed().await?;
                            dv.write(val);
                        }
                    }
                }
                self.0.call.write_with_recipt(Value::Null)
            };
            Ok(result
                .await
                .map_err(|_| anyhow!("call cancelled before a reply was received"))?)
        }

        /// List the procedures' arguments
        pub fn args(&self) -> impl Iterator<Item = &str> {
            self.0.args.keys().map(|s| s.as_str())
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{channel::test::Ctx, rpc::server::ArgSpec};

    use super::server::*;
    use super::*;
    use tokio::{runtime::Runtime, time};

    #[test]
    fn call_proc() {
        Runtime::new().unwrap().block_on(async move {
            let ctx = Ctx::new().await;
            let proc_name = Path::from("/rpc/procedure");
            let (tx, mut rx) = mpsc::channel(10);
            let _server_proc = define_rpc!(
                &ctx.publisher,
                proc_name.clone(),
                "test rpc procedure",
                |c, a| Some((c, a)),
                Some(tx),
                arg1: Value = Value::Null; "arg1 doc"
            )
            .unwrap();
            task::spawn(async move {
                while let Some((mut c, a)) = rx.next().await {
                    assert_eq!(a, Value::from("hello rpc"));
                    c.reply.send(Value::U32(42))
                }
            });
            time::sleep(Duration::from_millis(100)).await;
            let proc: client::Proc =
                client::Proc::new(&ctx.subscriber, proc_name.clone()).await.unwrap();
            let res = call_rpc!(proc, arg1: "hello rpc").await.unwrap();
            assert_eq!(res, Value::U32(42));
            let args: Vec<(Arc<str>, Value)> = vec![];
            let res = proc.call(args.into_iter()).await.unwrap();
            assert!(match res {
                Value::Error(_) => true,
                _ => false,
            });
            let args = vec![("arg2", Value::from("hello rpc"))];
            assert!(proc.call(args.into_iter()).await.is_err());
            Ok::<(), anyhow::Error>(())
        }).unwrap()
    }
}
