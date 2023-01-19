use anyhow::Result;
use netidx::{
    config::Config,
    path::Path,
    publisher::{BindCfg, DesiredAuth, Publisher},
};
use netidx_protocols::pack_channel::server::{Connection, Listener};
use std::time::Duration;
use structopt::StructOpt;
use tokio::{runtime::Runtime, task, time};

#[derive(StructOpt, Debug)]
pub(super) struct Params {
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
        default_value = "/local/channel/bench"
    )]
    base: Path,
    #[structopt(name = "batch-size", default_value = "100")]
    batch: usize,
}

async fn handle_client(
    con: Connection<u64, u64>,
    delay: Option<Duration>,
    n: usize,
) -> Result<()> {
    let mut i = 0u64;
    loop {
        let mut batch = con.start_batch();
        for _ in 0..n {
            batch.queue(&i)?;
            i += 1;
        }
        con.send(batch).await?;
        if let Some(delay) = delay {
            time::sleep(delay).await
        }
    }
}

async fn run_publisher(config: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    let delay = if p.delay == 0 { None } else { Some(Duration::from_millis(p.delay)) };
    let publisher =
        Publisher::new(config, auth, p.bind).await.expect("failed to create publisher");
    let mut listener = Listener::new(&publisher, 500, None, p.base.clone()).await?;
    loop {
        let client = listener.accept().await?;
        task::spawn(async move {
            match client.await {
                Err(e) => println!("client accept failed {}", e),
                Ok(client) => match handle_client(client, delay, p.batch).await {
                    Ok(()) => println!("client disconnected"),
                    Err(e) => println!("client disconnected {}", e),
                },
            }
        });
    }
}

pub(super) fn run(config: Config, auth: DesiredAuth, params: Params) {
    let rt = Runtime::new().expect("failed to init runtime");
    rt.block_on(async {
        run_publisher(config, auth, params).await.unwrap();
        // Allow the publisher time to send the clear message
        time::sleep(Duration::from_secs(1)).await;
    });
}
