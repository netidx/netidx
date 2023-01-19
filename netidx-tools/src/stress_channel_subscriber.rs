use futures::{prelude::*, select_biased};
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
}

async fn run_client(config: Config, auth: DesiredAuth, p: Params) {
    let subscriber = Subscriber::new(config, auth).expect("failed to create subscriber");
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let con =
        Connection::<u64, u64>::connect(&subscriber, 500, p.base.clone()).await.unwrap();
    let mut n = 0;
    let mut batch = Vec::new();
    let mut last_stat = Instant::now();
    loop {
        select_biased! {
            now = interval.tick().fuse() => {
                let elapsed = now - last_stat;
                println!("{} msgs/s", (n as f64) / elapsed.as_secs_f64());
                n = 0;
                last_stat = now;
            }
            r = con.recv(|v| batch.push(v)).fuse() => {
                r.unwrap();
                n += batch.len();
                batch.clear()
            }
        }
    }
}

pub(super) fn run(config: Config, auth: DesiredAuth, params: Params) {
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        run_client(config, auth, params).await;
        // Allow the publisher time to send the clear message
        time::sleep(Duration::from_secs(1)).await;
    });
}
