use crate::stress_publisher::Data;
use futures::prelude::*;
use json_pubsub::{
    path::Path,
    resolver::{ResolverRead, Auth},
    subscriber::{Subscriber, DVal, DVState},
    config::resolver::Config,
};
use std::time::Duration;
use tokio::{runtime::Runtime, time::Instant};

pub(crate) fn run(config: Config, auth: Auth) {
    let mut rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let mut r = ResolverRead::new(config.clone(), auth.clone()).expect("resolver");
        let paths = r.list(Path::from("/bench")).await.expect("list");
        let subscriber = Subscriber::new(config, auth).unwrap();
        let subs =
            paths.into_iter().map(|path| subscriber.durable_subscribe_val(path))
            .collect::<Vec<DVal<Data>>>();
        let mut vals = stream::select_all(subs.iter().map(|s| s.updates(true)));
        let one_second = Duration::from_secs(1);
        let start = Instant::now();
        let mut last_stat = start;
        let mut total = 0;
        let mut n = 0;
        while let Some(_) = vals.next().await {
            total += 1;
            n += 1;
            if n >= subs.len() {
                let now = Instant::now();
                let elapsed = now - last_stat;
                let since_start = now - start;
                if elapsed > one_second {
                    let mut subscribed = 0;
                    let mut unsubscribed = 0;
                    for s in subs.iter() {
                        match s.state() {
                            DVState::Unsubscribed => { unsubscribed += 1; }
                            DVState::Subscribed => { subscribed += 1; }
                        }
                    }
                    println!(
                        "subscribed: {} unsubscribed: {} rx: {:.0} rx(avg): {:.0}",
                        subscribed, unsubscribed, n as f64 / elapsed.as_secs_f64(),
                        total as f64 / since_start.as_secs_f64()
                    );
                    n = 0;
                    last_stat = now;
                }
            }
        }
        future::pending::<()>().await;
    });
}
