use super::ResolverConfig;
use crate::stress_publisher::Data;
use futures::prelude::*;
use json_pubsub::{
    path::Path,
    resolver::{ReadOnly, Resolver},
    subscriber::{Subscriber, Subscription},
};
use std::{result::Result, time::{Instant, Duration}};
use tokio::{runtime::Runtime};

pub(crate) fn run(config: ResolverConfig) {
    let mut rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let mut r = Resolver::<ReadOnly>::new_r(config.bind).expect("resolver");
        let paths = r.list(Path::from("/bench")).await.expect("list");
        let subscriber = Subscriber::new(config.bind).unwrap();
        let subs = subscriber
            .subscribe(paths)
            .await
            .into_iter()
            .map(|(_, s)| s)
            .collect::<Result<Vec<Subscription<Data>>, _>>()
            .expect("subscribe");
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
        drop(subs);
    });
}
