use anyhow::{Context, Result};
use bytes::BytesMut;
use chrono::prelude::*;
use fxhash::FxHashMap;
use netidx::{
    chars::Chars,
    path::Path,
    resolver_client::{Glob, GlobSet},
    subscriber::{Event, Subscriber, Value},
};
use netidx_archive::{
    logfile::{BatchItem, Seek},
    recorder_client::Client,
};
use netidx_tools_core::ClientParams;
use structopt::StructOpt;
use tokio::io::{stdout, AsyncWriteExt};

use crate::subscriber::Out;

#[derive(StructOpt, Debug)]
pub(crate) struct OneshotParams {
    #[structopt(name = "base", help = "the base path of the recorder instance")]
    base: Path,
    #[structopt(name = "start", help = "the time to start the recording at")]
    start: Option<String>,
    #[structopt(name = "end", help = "the time to end the recording at")]
    end: Option<String>,
    #[structopt(name = "filter", help = "glob pattern(s) to include")]
    filter: Vec<String>,
}

#[derive(StructOpt, Debug)]
pub(crate) enum Cmd {
    #[structopt(name = "oneshot", about = "get a oneshot recording")]
    Oneshot {
        #[structopt(flatten)]
        common: ClientParams,
        #[structopt(flatten)]
        params: OneshotParams,
    },
}

fn parse_bound(s: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    match s {
        None => Ok(None),
        Some(s) => match s.parse::<DateTime<Utc>>() {
            Ok(dt) => Ok(Some(dt)),
            Err(_) => match s.parse::<Seek>()? {
                Seek::Absolute(dt) => Ok(Some(dt)),
                Seek::TimeRelative(dur) => Ok(Some(Utc::now() + dur)),
                Seek::BatchRelative(_) => {
                    bail!("batch relative seek isn't supported for oneshot")
                }
                Seek::Beginning => Ok(None),
                Seek::End => Ok(None),
            },
        },
    }
}

async fn oneshot(subscriber: Subscriber, params: OneshotParams) -> Result<()> {
    let mut stdout = stdout();
    let mut buf = BytesMut::new();
    let start = parse_bound(params.start.as_ref().map(|s| s.as_str()))?;
    let end = parse_bound(params.end.as_ref().map(|s| s.as_str()))?;
    let filter = GlobSet::new(
        true,
        params
            .filter
            .into_iter()
            .map(|g| Glob::new(Chars::from(g)))
            .collect::<Result<Vec<Glob>>>()?,
    )?;
    let client = Client::new(&subscriber, &params.base).await?;
    let mut res = client.oneshot(&start, &end, &filter).await?;
    let paths = res.pathmap.drain(..).collect::<FxHashMap<_, _>>();
    for (id, value) in res.image.drain() {
        Out { raw: false, path: &paths[&id], value }.write(&mut buf)?;
    }
    stdout.write_all_buf(&mut buf).await?;
    for (ts, mut batch) in res.deltas.drain(..) {
        Out { raw: false, path: "timestamp", value: Event::Update(Value::DateTime(ts)) }
            .write(&mut buf)?;
        for BatchItem(id, value) in batch.drain(..) {
            Out { raw: false, path: &paths[&id], value }.write(&mut buf)?;
        }
        stdout.write_all_buf(&mut buf).await?;
    }
    Ok(())
}

pub(super) async fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Oneshot { common, params } => {
            let (cfg, auth) = common.load();
            let subscriber = Subscriber::new(cfg, auth).context("create subscriber")?;
            oneshot(subscriber, params).await
        }
    }
}
