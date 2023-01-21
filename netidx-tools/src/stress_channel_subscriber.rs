use crate::stress_channel_publisher::BatchHeader;
use anyhow::Result;
use chrono::prelude::*;
use futures::{prelude::*, select_biased};
use hdrhistogram::Histogram;
use netidx::{
    config::Config,
    path::Path,
    subscriber::{DesiredAuth, Subscriber},
};
use netidx_protocols::pack_channel::client::Connection;
use std::{sync::Arc, time::Duration};
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
    #[structopt(long = "latency", help = "test latency not throughput")]
    latency: bool,
    #[structopt(name = "batch-size", default_value = "100")]
    batch: usize,
}

async fn can_send(flushed: bool, delayed: bool) {
    if !flushed || delayed {
        future::pending().await
    }
}

async fn maybe_flush(con: &Connection, latency: bool) -> Result<()> {
    if !latency && con.dirty() {
        con.flush().await
    } else {
        future::pending().await
    }
}

async fn run_client(config: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    let delay = if p.latency {
        None
    } else if p.delay == 0 {
        None
    } else {
        Some(Duration::from_micros(p.delay))
    };
    let subscriber = Subscriber::new(config, auth)?;
    let mut interval = time::interval(Duration::from_secs(1));
    let mut delay_interval = time::interval(delay.unwrap_or(Duration::from_secs(1)));
    let con = Arc::new(Connection::connect(&subscriber, 500, p.base.clone()).await?);
    let mut sum = 0;
    let mut total = 0;
    let mut latency = Histogram::<u64>::new_with_bounds(10, 1_000_000_000, 3)?;
    let mut last_stat = Instant::now();
    let mut flushed = true;
    let mut delayed = false;
    let mut since_flush = 0;
    loop {
        select_biased! {
            now = interval.tick().fuse() => {
                let elapsed = now - last_stat;
                let sum = sum / 1000000000;
                let rate = (total as f64) / elapsed.as_secs_f64();
                let med = latency.value_at_quantile(0.5) / 1000;
                let ni = latency.value_at_quantile(0.9) / 1000;
                let nn = latency.value_at_quantile(0.99) / 1000;
                println!(
                    "{} msgs/s RTT, 50th {}us, 90th {}us, 99th {}us, sum {}bn",
                    rate, med, ni, nn, sum
                );
                total = 0;
                last_stat = now;
            },
            hdr = con.recv_one().fuse() => {
                let hdr: BatchHeader = hdr?;
                let mut j: u32 = 0;
                while j < hdr.count {
                    con.recv(|i: u64| {
                        sum += i;
                        total += 1;
                        j += 1;
                        j < hdr.count
                    })
                        .await?
                }
                assert_eq!(j, hdr.count);
                match (Utc::now() - hdr.timestamp).num_nanoseconds() {
                    Some(ns) if ns >= 10 && ns <= 1_000_000_000 => {
                        latency += ns as u64;
                    }
                    Some(_) | None => (),
                }
                if p.latency {
                    flushed = true;
                }
            },
            r = maybe_flush(&con, p.latency).fuse() => {
                r?;
                flushed = true;
            },
            _ = delay_interval.tick().fuse() => {
                delayed = false;
            },
            () = can_send(flushed, delayed).fuse() => {
                let mut batch = con.start_batch();
                batch.queue(&BatchHeader {
                    timestamp: Utc::now(),
                    count: p.batch as u32
                })?;
                for i in 0..p.batch {
                    batch.queue(&(i as u64))?;
                }
                con.send(batch)?;
                since_flush += 1;
                if p.latency || since_flush > 100 {
                    flushed = false;
                    since_flush = 0;
                }
                if delay.is_some() {
                    delayed = true;
                }
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
    });
}
