use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    future::{self, BoxFuture},
    prelude::*,
    select_biased, stream,
};
use fxhash::{FxBuildHasher, FxHashMap};
use log::info;
use netidx::{
    chars::Chars,
    path::Path,
    pool::{Pool, Pooled},
    protocol::glob::{Glob, GlobSet},
    publisher::{Id, PublishFlags, Publisher, Val, Value, WriteRequest},
    subscriber::{Dval, Subscriber, SubscriberId},
};
use parking_lot::Mutex;
use std::{
    borrow::Borrow,
    collections::HashMap,
    iter,
    net::SocketAddr,
    ops::Drop,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};
use tokio::{sync::Mutex as AsyncMutex, task};

pub mod server {
    use super::*;

    /// The rpc handler function type
    pub type Handler = Arc<
        dyn Fn(
                SocketAddr,
                Pooled<HashMap<Arc<str>, Pooled<Vec<Value>>>>,
            ) -> BoxFuture<'static, Value>
            + Send
            + Sync
            + 'static,
    >;

    lazy_static! {
        static ref ARG: Pool<Vec<Value>> = Pool::new(10000, 50);
        static ref ARGS: Pool<HashMap<Arc<str>, Pooled<Vec<Value>>>> =
            Pool::new(10000, 50);
    }

    struct Arg {
        name: Arc<str>,
        _value: Val,
        _doc: Val,
    }

    struct PendingCall {
        args: Pooled<HashMap<Arc<str>, Pooled<Vec<Value>>>>,
        initiated: Instant,
    }

    struct ProcInner {
        publisher: Publisher,
        call: Val,
        _doc: Val,
        args: HashMap<Id, Arg, FxBuildHasher>,
        pending: HashMap<SocketAddr, PendingCall, FxBuildHasher>,
        handler: Handler,
        events: stream::Fuse<mpsc::Receiver<Pooled<Vec<WriteRequest>>>>,
        stop: future::Fuse<oneshot::Receiver<()>>,
        last_gc: Instant,
    }

    impl ProcInner {
        async fn run(mut self) {
            static GC_FREQ: Duration = Duration::from_secs(1);
            static GC_THRESHOLD: usize = 128;
            fn gc_pending(
                pending: &mut HashMap<SocketAddr, PendingCall, FxBuildHasher>,
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
                    ev = self.events.next() => match ev {
                        None => break, // publisher died?
                        Some(mut batch) => for req in batch.drain(..) {
                            if req.id == self.call.id() {
                                let args = self.pending.remove(&req.addr).map(|pc| pc.args)
                                    .unwrap_or_else(|| ARGS.take());
                                let handler = self.handler.clone();
                                let call = self.call.clone();
                                let publisher = self.publisher.clone();
                                task::spawn(async move {
                                    let t = task::spawn(handler(req.addr, args));
                                    let r = match t.await {
                                        Ok(v) => v,
                                        Err(e) => {
                                            Value::Error(Chars::from(format!("{}", e)))
                                        }
                                    };
                                    match req.send_result {
                                        None => call.update_subscriber(&req.addr, r),
                                        Some(result) => result.send(r)
                                    }
                                    publisher.flush(None).await
                                });
                            } else {
                                let mut gc = false;
                                let pending = self.pending.entry(req.addr)
                                    .or_insert_with(|| {
                                        gc = true;
                                        PendingCall {
                                            args: ARGS.take(),
                                            initiated: Instant::now()
                                        }
                                    });
                                if let Some(Arg {name, ..}) = self.args.get(&req.id) {
                                    pending.args.entry(name.clone())
                                        .or_insert_with(|| ARG.take())
                                        .push(req.value);
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
    }

    /// A remote procedure published in netidx
    pub struct Proc(oneshot::Sender<()>);

    impl Proc {
        /**
        Publish a new remote procedure. If successful this will return
        a `Proc` which, if dropped, will cause the removal of the
        procedure from netidx.

        # Arguments

        * `publisher` - A reference to the publisher that will publish the procedure.
        * `name` - The path of the procedure in netidx.
        * `doc` - The procedure level doc string to be published along with the procedure
        * `args` - A hashmap containing the allowed arguments to the
          procedure. The key, is the argument name, the first value is
          the default value, and the second argument is the doc string
          for the argument
        * `handler` - The function that will be called each time the
          remote procedure is invoked

        # Example
        ```no_run
        use netidx::{path::Path, subscriber::Value, chars::Chars};
        use netidx_protocols::rpc::server::Proc;
        use std::{sync::Arc, collections::HashMap};
        use anyhow::Result;
        # async fn z() -> Result<()> {
        #   let publisher = unimplemented!();
            let echo = Proc::new(
                &publisher,
                Path::from("/examples/api/echo"),
                Value::from("echos it's argument"),
                vec![(Arc::from("arg"), (Value::Null, Value::from("argument to echo")))]
                    .into_iter()
                    .collect::<HashMap<_, _>>(),
                Arc::new(move |_, mut args| {
                    Box::pin(async move {
                        match args.remove("arg") {
                            None => Value::Error(Chars::from("expected an arg")),
                            Some(mut vals) => match vals.pop() {
                                None => Value::Error(Chars::from("internal error")),
                                Some(v) => v
                            }
                        }
                    })
                }),
            )
            .await?;
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
        are accumulated and passed to the handler in the order they
        were specified

         **/
        pub async fn new(
            publisher: &Publisher,
            name: Path,
            doc: Value,
            args: HashMap<Arc<str>, (Value, Value)>,
            handler: Handler,
        ) -> Result<Proc> {
            let (tx_ev, rx_ev) = mpsc::channel(3);
            let (tx_stop, rx_stop) = oneshot::channel();
            let call = publisher.publish_with_flags(
                PublishFlags::USE_EXISTING,
                name.clone(),
                Value::Null,
            )?;
            let _doc = publisher.publish_with_flags(
                PublishFlags::USE_EXISTING,
                name.append("doc"),
                doc,
            )?;
            call.writes(tx_ev.clone());
            let args = args
                .into_iter()
                .map(|(arg, (def, doc))| {
                    let base = name.append(&*arg);
                    let _value = publisher
                        .publish_with_flags(
                            PublishFlags::USE_EXISTING,
                            base.append("val"),
                            def,
                        )
                        .map(|val| {
                            val.writes(tx_ev.clone());
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
                publisher: publisher.clone(),
                call,
                _doc,
                args,
                pending: HashMap::with_hasher(FxBuildHasher::default()),
                handler,
                events: rx_ev.fuse(),
                stop: rx_stop.fuse(),
                last_gc: Instant::now(),
            };
            task::spawn(async move {
                inner.run().await;
                info!("rpc proc {} shutdown", name);
            });
            publisher.flush(None).await;
            Ok(Proc(tx_stop))
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
    pub struct Proc {
        name: Path,
        sid: SubscriberId,
        lock: Option<Arc<AsyncMutex<()>>>,
        call: Dval,
        args: HashMap<String, Dval>,
    }

    impl Drop for Proc {
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
            Ok(Proc { name, sid, lock, call, args })
        }

        /// Call the procedure. If supported by the procedure,
        /// argument keys may be specified multiple times.
        ///
        /// `call` may be reused to call the procedure again.
        ///
        /// `call` may safely be called concurrently on multiple
        /// instances of `Proc` that call the same underling procedure
        /// (there is internal syncronization).
        pub async fn call<I, K>(&self, args: I) -> Result<Value>
        where
            I: IntoIterator<Item = (K, Value)>,
            K: Borrow<str>,
        {
            let result = {
                let _guard = self.lock.as_ref().unwrap().lock().await;
                for (name, val) in args {
                    match self.args.get(name.borrow()) {
                        None => bail!("no such argument {}", name.borrow()),
                        Some(dv) => {
                            dv.wait_subscribed().await?;
                            dv.write(val);
                        }
                    }
                }
                self.call.write_with_recipt(Value::Null)
            };
            Ok(result
                .await
                .map_err(|_| anyhow!("call cancelled before a reply was received"))?)
        }

        /// List the procedures' arguments
        pub fn args(&self) -> impl Iterator<Item = &str> {
            self.args.keys().map(|s| s.as_str())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use netidx::{config, resolver::Auth, resolver_server::Server};
    use tokio::runtime::Runtime;

    #[test]
    fn call_proc() {
        Runtime::new().unwrap().block_on(async move {
            let mut cfg =
                config::Config::load("../cfg/simple.json").expect("load simple config");
            let server = Server::new(cfg.clone(), config::PMap::default(), false, 0)
                .await
                .expect("start resolver server");
            cfg.addrs[0] = *server.local_addr();
            let publisher = Publisher::new(
                cfg.clone(),
                Auth::Anonymous,
                "127.0.0.1/32".parse().unwrap(),
            )
            .await
            .unwrap();
            let subscriber = Subscriber::new(cfg, Auth::Anonymous).unwrap();
            let proc_name = Path::from("/rpc/procedure");
            let _server_proc: server::Proc = server::Proc::new(
                &publisher,
                proc_name.clone(),
                Value::from("test rpc procedure"),
                vec![(Arc::from("arg1"), (Value::Null, Value::from("arg1 doc")))]
                    .into_iter()
                    .collect(),
                Arc::new(|addr, args| {
                    Box::pin(async move {
                        dbg!(&addr);
                        dbg!(&*args);
                        assert_eq!(args.len(), 1);
                        assert_eq!(args["arg1"].len(), 1);
                        assert_eq!(args["arg1"][0], Value::from("hello rpc"));
                        Value::U32(42)
                    })
                }),
            )
            .await
            .unwrap();
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
