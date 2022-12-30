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

pub mod server {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::*;

    atomic_id!(ProcId);

    lazy_static! {
        static ref ARGS: Pool<HashMap<ArcStr, Value>> = Pool::new(10000, 50);
    }

    pub struct RpcReply(Option<SendResult>);

    impl Drop for RpcReply {
        fn drop(&mut self) {
            if let Some(reply) = self.0.take() {
                reply.send(Value::Error(Chars::from("rpc call failed")))
            }
        }
    }

    impl RpcReply {
        pub fn send(&mut self, m: Value) {
            if let Some(res) = self.0.take() {
                res.send(m)
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
        handler: mpsc::Sender<T>,
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
                                let _: std::result::Result<_, _> = self.handler.send(t).await;
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
        * `handler` - The channel that will receive the rpc call invocations
        # Example
        ```no_run
        use netidx::{path::Path, subscriber::Value, chars::Chars};
        use netidx_protocols::rpc::server::{Proc, ArgSpec};
        use arcstr::ArcStr;
        use std::{sync::Arc, collections::HashMap};
        use anyhow::Result;
        use futures::channel::mpsc;
        # async fn z() -> Result<()> {
        #   let publisher = unimplemented!();
            let (tx, rx) = mpsc::channel(10);
            let echo = Proc::new(
                &publisher,
                Path::from("/examples/api/echo"),
                Value::from("echos it's argument"),
                vec![
                    ArgSpec {
                        name: ArcStr::from("arg"),
                        doc: Value::from("argument to echo"),
                        default_value: Value::Null
                    }
                ],
                tx
            )?;
            while let Ok(c) = rx.next().await {
                let res = match c.args.remove("arg") {
                    None => Value::Error(Chars::from("expected an arg")),
                    Some(mut vals) => match vals.pop() {
                        None => Value::Error(Chars::from("internal error")),
                        Some(v) => v
                    }
                };
                c.send(res).await;
            }
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
            handler: mpsc::Sender<T>,
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
            let call = subscriber.durable_subscribe(name.clone());
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
                    args.insert(
                        String::from(arg_name),
                        subscriber.durable_subscribe(arg_path),
                    );
                }
            }
            Ok(Proc(Arc::new(ProcInner { name, sid, lock, call, args })))
        }

        /// Call the procedure.
        ///
        /// `call` may be reused to call the procedure again.
        ///
        /// `call` may safely be called concurrently on multiple
        /// instances of `Proc` that call the same procedure
        /// (there is internal syncronization).
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
    use crate::rpc::server::ArgSpec;

    use super::*;
    use netidx::{
        config::Config as ClientConfig,
        resolver_client::DesiredAuth,
        resolver_server::{config::Config as ServerConfig, Server},
    };
    use tokio::{runtime::Runtime, time};

    #[test]
    fn call_proc() {
        Runtime::new().unwrap().block_on(async move {
            let cfg = ServerConfig::load("../cfg/simple-server.json")
                .expect("load simple server config");
            let server =
                Server::new(cfg.clone(), false, 0).await.expect("start resolver server");
            let mut cfg = ClientConfig::load("../cfg/simple-client.json")
                .expect("load simple client config");
            cfg.addrs[0].0 = *server.local_addr();
            let publisher = Publisher::new(
                cfg.clone(),
                DesiredAuth::Anonymous,
                "127.0.0.1/32".parse().unwrap(),
            )
            .await
            .unwrap();
            let subscriber = Subscriber::new(cfg, DesiredAuth::Anonymous).unwrap();
            let proc_name = Path::from("/rpc/procedure");
            let (tx, mut rx) = mpsc::channel(10);
            let _server_proc: server::Proc = server::Proc::new(
                &publisher,
                proc_name.clone(),
                Value::from("test rpc procedure"),
                [ArgSpec {
                    name: ArcStr::from("arg1"),
                    default_value: Value::Null,
                    doc: Value::from("arg1 doc"),
                }],
                |a| Some(a),
                tx,
            )
            .unwrap();
            task::spawn(async move {
                while let Some(mut c) = rx.next().await {
                    dbg!(&c.args);
                    assert_eq!(c.args.len(), 1);
                    assert_eq!(c.args["arg1"], Value::from("hello rpc"));
                    c.reply.send(Value::U32(42))
                }
            });
            time::sleep(Duration::from_millis(100)).await;
            let proc: client::Proc =
                client::Proc::new(&subscriber, proc_name.clone()).await.unwrap();
            let args = vec![("arg1", Value::from("hello rpc"))];
            let res = proc.call(args.into_iter()).await.unwrap();
            assert_eq!(res, Value::U32(42));
            let args: Vec<(Arc<str>, Value)> = vec![];
            let res = proc.call(args.into_iter()).await.unwrap();
            assert!(match res {
                Value::Error(_) => true,
                _ => false,
            });
            let args = vec![("arg2", Value::from("hello rpc"))];
            assert!(proc.call(args.into_iter()).await.is_err());
        })
    }
}
