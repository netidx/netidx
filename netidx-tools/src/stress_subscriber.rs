use anyhow::{Context, Result};
use futures::channel::mpsc;
use futures::{prelude::*, select_biased};
use netidx::{
    config::Config,
    path::Path,
    resolver_client::{DesiredAuth, ResolverRead},
    subscriber::{Subscriber, UpdatesFlags},
};
use std::time::Duration;
use structopt::StructOpt;
use tokio::time::{self, Instant};

#[derive(StructOpt, Debug)]
pub(super) struct Params {
    #[structopt(long = "base", help = "base path", default_value = "/bench")]
    base: String,
}

pub(super) async fn run(config: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    env_logger::init();
    let r = ResolverRead::new(config.clone(), auth.clone());
    let table = r.table(Path::from(p.base)).await.context("load table")?;
    let subscriber = Subscriber::new(config, auth).context("create subscriber")?;
    let subs = {
        let mut subs = Vec::with_capacity(table.rows.len() * table.cols.len());
        for row in table.rows.iter() {
            for (col, _) in table.cols.iter() {
                subs.push(subscriber.subscribe(row.append(col)));
            }
        }
        subs
    };
    let (tx, mut vals) = mpsc::channel(3);
    for s in subs.iter() {
        s.updates(
            UpdatesFlags::BEGIN_WITH_LAST | UpdatesFlags::STOP_COLLECTING_LAST,
            tx.clone(),
        )
    }
    let start = Instant::now();
    let mut last_stat = start;
    let mut total: usize = 0;
    let mut n: usize = 0;
    let mut batch_size: usize = 0;
    let mut nbatches: usize = 0;
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        select_biased! {
            now = interval.tick().fuse() => {
                let elapsed = now - last_stat;
                let since_start = now - start;
                let stats = subscriber.durable_stats();
                println!(
                    "s: {} p: {} !s: {} rx_i: {:.0} rx_a: {:.0} btch_a: {:.0}",
                    stats.alive,
                    stats.pending,
                    stats.dead,
                    n as f64 / elapsed.as_secs_f64(),
                    total as f64 / since_start.as_secs_f64(),
                    batch_size as f64 / nbatches as f64
                );
                nbatches = 0;
                batch_size = 0;
                n = 0;
                last_stat = now;
            },
            batch = vals.next() => match batch {
                None => break,
                Some(mut batch) => {
                    batch_size += batch.len();
                    nbatches += 1;
                    for _ in batch.drain(..) {
                        total += 1;
                        n += 1;
                    }
                }
            }
        }
    }
    future::pending::<Result<()>>().await
}
