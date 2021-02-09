use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    future::{self, BoxFuture},
    prelude::*,
    select_biased, stream,
};
use fxhash::FxBuildHasher;
use netidx::{
    chars::Chars,
    path::Path,
    pool::{Pool, Pooled},
    protocol::glob::{Glob, GlobSet},
    publisher::{Id, PublishFlags, Publisher, Val, Value, WriteRequest},
    subscriber::{Dval, Subscriber},
};
use std::{
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
                                    let r: Value = handler(req.addr, args).await;
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
            task::spawn(async move { inner.run() });
            publisher.flush(None).await;
            Ok(Proc(tx_stop))
        }
    }
}

pub mod client {
    use super::*;

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

        pub async fn call(
            &self,
            args: impl Iterator<Item = (&str, Value)>,
        ) -> Result<Value> {
            for (name, val) in args {
                match self.args.get(name) {
                    None => bail!("no such argument {}", name),
                    Some(dv) => {
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
