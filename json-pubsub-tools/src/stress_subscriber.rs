use super::ResolverConfig;
use crate::stress_publisher::Data;
use futures::{prelude::*, select};
use json_pubsub::{
    path::Path,
    resolver::{ReadOnly, Resolver},
    subscriber::{Subscriber, Subscription},
};
use std::{result::Result, time::Duration};
use tokio::{runtime::Runtime, time};

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
        let mut vals = stream::select_all(subs.iter().map(|s| s.updates(true))).fuse();
        let mut stat = time::interval(Duration::from_secs(1)).fuse();
        let mut count: usize = 0;
        loop {
            select! {
                _ = stat.next() => {
                    println!("rx: {}", count);
                    count = 0;
                },
                _ = vals.next() => {
                    count += 1;
                },
            }
        }
    });
}
