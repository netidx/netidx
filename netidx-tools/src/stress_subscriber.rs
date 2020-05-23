use futures::channel::mpsc;
use futures::prelude::*;
use json_pubsub::{
    config::Config,
    path::Path,
    resolver::{Auth, ResolverRead},
    subscriber::{DvState, Dval, Subscriber},
};
use std::time::Duration;
use tokio::{runtime::Runtime, time::Instant};

pub(crate) fn run(config: Config, auth: Auth) {
    let mut rt = Runtime::new().expect("runtime");
    rt.block_on(async {
        let r = ResolverRead::new(config.clone(), auth.clone());
        let paths = r.list(Path::from("/bench")).await.expect("list");
        let subscriber = Subscriber::new(config, auth).unwrap();
        let subs = paths
            .into_iter()
            .map(|path| subscriber.durable_subscribe(path))
            .collect::<Vec<Dval>>();
        let (tx, mut vals) = mpsc::channel(100000);
        for s in subs.iter() {
            s.updates(true, tx.clone())
        }
        let one_second = Duration::from_secs(1);
        let start = Instant::now();
        let mut last_stat = start;
        let mut total: usize = 0;
        let mut n: usize = 0;
        let mut batch_size: usize = 0;
        let mut nbatches: usize = 0;
        while let Some(mut batch) = vals.next().await {
            batch_size += batch.len();
            nbatches += 1;
            for _ in batch.drain(..) {
                total += 1;
                n += 1;
                if n >= subs.len() {
                    let now = Instant::now();
                    let elapsed = now - last_stat;
                    let since_start = now - start;
                    if elapsed > one_second {
                        let mut subscribed = 0;
                        let mut unsubscribed = 0;
                        let mut fatal = 0;
                        for s in subs.iter() {
                            match s.state() {
                                DvState::Unsubscribed => {
                                    unsubscribed += 1;
                                }
                                DvState::Subscribed => {
                                    subscribed += 1;
                                }
                                DvState::FatalError(_) => {
                                    fatal += 1;
                                }
                            }
                        }
                        println!(
                            "subscribed: {} unsubscribed: {}({}) rx: {:.0} rx(avg): {:.0} batch(avg): {:.0}",
                            subscribed,
                            unsubscribed,
                            fatal,
                            n as f64 / elapsed.as_secs_f64(),
                            total as f64 / since_start.as_secs_f64(),
                            batch_size as f64 / nbatches as f64
                        );
                        nbatches = 0;
                        batch_size = 0;
                        n = 0;
                        last_stat = now;
                    }
                }
            }
        }
        future::pending::<()>().await;
    });
}
