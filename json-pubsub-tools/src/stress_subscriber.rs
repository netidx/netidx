use crate::stress_publisher::Data;
use futures::prelude::*;
use json_pubsub::{
    path::Path,
    resolver::{ReadOnly, Resolver},
    subscriber::{Subscriber, DVal},
    config,
};
use std::time::Duration;
use tokio::{runtime::Runtime, time::Instant};

pub(crate) fn run(config: config::Resolver) {
    let mut rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let mut r = Resolver::<ReadOnly>::new_r(config).expect("resolver");
        let paths = r.list(Path::from("/bench")).await.expect("list");
        let subscriber = Subscriber::new(config).unwrap();
        let subs =
            paths.into_iter().map(|path| subscriber.durable_subscribe_val(path))
            .collect::<Vec<DVal<Data>>>();
        let mut vals = stream::select_all(subs.iter().map(|s| s.updates(true)));
        let one_second = Duration::from_secs(1);
        let mut last_stat = Instant::now();
        let mut n = 0;
        while let Some(_) = vals.next().await {
            n += 1;
            if n >= subs.len() {
                let now = Instant::now();
                let elapsed = now - last_stat;
                if elapsed > one_second {
                    println!("rx: {:.0}", n as f64 / elapsed.as_secs_f64());
                    n = 0;
                    last_stat = now;
                }
            }
        }
        future::pending::<()>().await;
    });
}
