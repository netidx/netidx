use anyhow::Result;
use bytes::{Buf, BufMut};
use chrono::prelude::*;
use netidx::{
    config::Config,
    pack::{Pack, PackError},
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
        long = "base",
        help = "base path",
        default_value = "/local/channel/bench"
    )]
    base: Path,
}

#[derive(Debug)]
pub(crate) struct BatchHeader {
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) count: u32,
}

impl Pack for BatchHeader {
    fn const_encoded_len() -> Option<usize> {
        let r = <DateTime<Utc> as Pack>::const_encoded_len()?
            + <u32 as Pack>::const_encoded_len()?;
        Some(r)
    }

    fn encoded_len(&self) -> usize {
        self.timestamp.encoded_len() + self.count.encoded_len()
    }

    fn encode(&self, buf: &mut impl BufMut) -> Result<(), PackError> {
        Pack::encode(&self.timestamp, buf)?;
        Pack::encode(&self.count, buf)
    }

    fn decode(buf: &mut impl Buf) -> Result<Self, PackError> {
        let timestamp = Pack::decode(buf)?;
        let count = Pack::decode(buf)?;
        Ok(Self { timestamp, count })
    }
}

async fn handle_client(con: Connection) -> Result<()> {
    let mut buf = Vec::new();
    loop {
        let hdr: BatchHeader = con.recv_one().await?;
        let mut n = 0;
        while n < hdr.count {
            con.recv(|i: u64| {
                buf.push(i);
                n += 1;
                n < hdr.count
            })
            .await?;
        }
        let mut batch = con.start_batch();
        batch.queue(&hdr)?;
        for i in buf.drain(..) {
            batch.queue(&i)?
        }
        con.send(batch).await?;
    }
}

async fn run_publisher(config: Config, auth: DesiredAuth, p: Params) -> Result<()> {
    let publisher =
        Publisher::new(config, auth, p.bind).await.expect("failed to create publisher");
    let mut listener = Listener::new(&publisher, 500, None, p.base.clone()).await?;
    loop {
        let client = listener.accept().await?;
        task::spawn(async move {
            match client.await {
                Err(e) => println!("client accept failed {}", e),
                Ok(client) => match handle_client(client).await {
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
