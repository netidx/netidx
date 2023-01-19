use crate::stress_channel_publisher::BatchHeader;
use chrono::prelude::*;
use futures::{prelude::*, select_biased};
use hdrhistogram::Histogram;
use netidx::{
    config::Config,
    path::Path,
    subscriber::{DesiredAuth, Subscriber},
};
use netidx_protocols::pack_channel::client::Connection;
use std::time::Duration;
use structopt::StructOpt;
use tokio::{
    runtime::Runtime,
    time::{self, Instant},
};

#[derive(StructOpt, Debug)]
pub(super) struct Params {
    #[structopt(
        long = "base",
        help = "base path",
        default_value = "/local/channel/bench"
    )]
    base: Path,
    #[structopt(
        long = "delay",
        help = "time in ms to wait between batches",
        default_value = "100"
    )]
    delay: u64,
    #[structopt(name = "batch-size", default_value = "100")]
    batch: usize,
}

async fn run_client(config: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    let delay = if p.delay == 0 { None } else { Some(Duration::from_millis(p.delay)) };
    let subscriber = Subscriber::new(config, auth)?;
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let con = Connection::connect(&subscriber, 500, p.base.clone()).await?;
    let mut total = 0;
    let mut latency = Histogram::new_with_bounds(10, 1_000_000, 3)?;
    let mut buf = Vec::new();
    let mut last_stat = Instant::now();
    loop {
        select_biased! {
            now = interval.tick().fuse() => {
                let elapsed = now - last_stat;
                let rate = (total as f64) / elapsed.as_secs_f64();
                let min = latency.min();
                let max = latency.max();
                let med = latency.value_at_quantile(0.5);
                let ni = latency.value_at_quantile(0.9);
                let nn = latency.value_at_quantile(0.99);
                println!(
                    "{} msgs/s RTT min {}ns, 50th {}ns 90th {}ns 99th {}ns max {}ns",
                    rate, min, med, ni, nn, max
                );
                total = 0;
                last_stat = now;
            }
            hdr = con.recv_one::<BatchHeader>().fuse() => {
                let hdr = hdr?;
                buf.clear();
                let mut j = 0;
                while j < hdr.count {
                    con.recv(|i: u64| {
                        buf.push(i);
                        j += 1;
                        j < hdr.count
                    }).await?
                }
                match (Utc::now() - hdr.timestamp).num_nanoseconds() {
                    Some(ns) if ns > 10 && ns <= 1_000_000 => {
                        total += 1 + j;
                        latency += ns;
                    }
                    Some(_) | None => ()
                }
            }
            default => {
                let mut batch = con.start_batch();
                batch.queue(&BatchHeader {timestamp: Utc::now(), count: p.batch as u32})?;
                for i in 0..p.batch {
                    batch.queue(&(i as u64))?;
                }
                con.send(batch);
                con.flush().await?
            }
        }
    }
}

pub(super) fn run(config: Config, auth: DesiredAuth, params: Params) {
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        if let Err(e) = run_client(config, auth, params).await {
            eprintln!("client stopped with error {}", e);
        }
        // Allow the publisher time to send the clear message
        time::sleep(Duration::from_secs(1)).await;
    });
}
