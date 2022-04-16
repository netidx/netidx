use futures::{prelude::*, select};
use netidx::{
    config::client::Config,
    path::Path,
    publisher::{BindCfg, Publisher, Value},
    resolver::DesiredAuth,
};
use std::{
    mem,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use tokio::{runtime::Runtime, signal, time};

#[derive(StructOpt, Debug)]
pub(crate) struct Params {
    #[structopt(
        short = "b",
        long = "bind",
        help = "configure the bind address e.g. 192.168.0.0/16, 127.0.0.1:5000"
    )]
    bind: BindCfg,
    #[structopt(
        long = "delay",
        help = "time in ms to wait between batches",
        default_value = "100"
    )]
    delay: u64,
    #[structopt(
        long = "base",
        help = "base path",
        default_value = "/bench"
    )]
    base: String,
    #[structopt(name = "rows", default_value = "100")]
    rows: usize,
    #[structopt(name = "cols", default_value = "10")]
    cols: usize,
}

async fn run_publisher(config: Config, auth: DesiredAuth, p: Params) {
    let delay = if p.delay == 0 { None } else { Some(Duration::from_millis(p.delay)) };
    let publisher =
        Publisher::new(config, auth, p.bind).await.expect("failed to create publisher");
    let mut sent: usize = 0;
    let mut v = 0u64;
    let published = {
        let mut published = Vec::with_capacity(p.rows * p.cols);
        for row in 0..p.rows {
            for col in 0..p.cols {
                let path = Path::from(format!("{}/{}/{}", p.base, row, col));
                published.push(publisher.publish(path, Value::V64(v)).expect("encode"))
            }
        }
        published
    };
    let mut last_stat = Instant::now();
    let mut batch: usize = 0;
    let one_second = Duration::from_secs(1);
    loop {
        let mut updates = publisher.start_batch();
        v += 1;
        for (i, p) in published.iter().enumerate() {
            p.update(&mut updates, Value::V64(v + i as u64));
            sent += 1;
            batch += 1;
            if batch > 10000 {
                batch = 0;
                mem::replace(&mut updates, publisher.start_batch()).commit(None).await;
                if let Some(delay) = delay {
                    time::sleep(delay).await;
                }
            }
        }
        updates.commit(None).await;
        if let Some(delay) = delay {
            time::sleep(delay).await;
        }
        let now = Instant::now();
        let elapsed = now - last_stat;
        if elapsed > one_second {
            select! {
                _ = publisher.wait_any_client().fuse() => (),
                _ = signal::ctrl_c().fuse() => break,
            }
            last_stat = now;
            println!("tx: {:.0}", sent as f64 / elapsed.as_secs_f64());
            sent = 0;
        }
    }
}

pub(crate) fn run(config: Config, auth: DesiredAuth, params: Params) {
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        run_publisher(config, auth, params).await;
        // Allow the publisher time to send the clear message
        time::sleep(Duration::from_secs(1)).await;
    });
}
