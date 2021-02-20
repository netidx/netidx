use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    future::{self, BoxFuture},
    prelude::*,
    select_biased, stream,
};
use fxhash::FxBuildHasher;
use log::info;
use netidx::{
    chars::Chars,
    path::Path,
    pool::{Pool, Pooled},
    protocol::glob::{Glob, GlobSet},
    publisher::{Id, PublishFlags, Publisher, Val, Value, WriteRequest},
    subscriber::{Dval, Subscriber},
};
use std::{
    borrow::Borrow,
    collections::HashMap,
    iter,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::task;

pub mod server {
    use super::*;

    pub type Handler = Arc<
        dyn Fn(SocketAddr, Pooled<HashMap<Arc<str>, Value>>) -> BoxFuture<'static, Value>
            + Send
            + Sync
            + 'static,
    >;

    lazy_static! {
        static ref ARGS: Pool<HashMap<Arc<str>, Value>> = Pool::new(10000, 50);
    }

    struct Arg {
        name: Arc<str>,
        _value: Val,
        _doc: Val,
    }

    struct PendingCall {
        args: Pooled<HashMap<Arc<str>, Value>>,
        initiated: Instant,
    }

    struct ProcInner {
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
                                task::spawn(async move {
                                    let r = match task::spawn(handler(req.addr, args)).await {
                                        Ok(v) => v,
                                        Err(e) => Value::Error(Chars::from(format!("{}", e)))
                                    };
                                    match req.send_result {
                                        None => call.update_subscriber(&req.addr, r),
                                        Some(result) => result.send(r)
                                    }
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
    }

    pub struct Proc(oneshot::Sender<()>);

    impl Proc {
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

    #[derive(Debug)]
    pub struct Proc {
        call: Dval,
        args: HashMap<String, Dval>,
    }

    impl Proc {
        pub async fn new(subscriber: &Subscriber, name: Path) -> Result<Proc> {
            let call = subscriber.durable_subscribe(name.clone());
            let pat = GlobSet::new(
                true,
                iter::once(Glob::new(Chars::from(format!("{}/*/val", name)))?),
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
            Ok(Proc { call, args })
        }

        pub async fn call<I, K>(&self, args: I) -> Result<Value>
        where
            I: IntoIterator<Item = (K, Value)>,
            K: Borrow<str>,
        {
            for (name, val) in args {
                match self.args.get(name.borrow()) {
                    None => bail!("no such argument {}", name.borrow()),
                    Some(dv) => {
                        dv.wait_subscribed().await?;
                        dv.write(val);
                    }
                }
            }
            Ok(self
                .call
                .write_with_recipt(Value::Null)
                .await
                .map_err(|_| anyhow!("call cancelled before a reply was received"))?)
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
                        assert_eq!(args["arg1"], Value::from("hello rpc"));
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
