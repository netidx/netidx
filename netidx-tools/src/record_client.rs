use std::{path::PathBuf, collections::HashSet};

use anyhow::{Context, Result};
use bytes::BytesMut;
use chrono::prelude::*;
use netidx::{
    chars::Chars,
    path::Path,
    resolver_client::{Glob, GlobSet},
    subscriber::{Event, Subscriber, Value},
};
use netidx_archive::{
    logfile::{self, ArchiveReader, BatchItem, Cursor, Seek},
    recorder_client::{Client, OneshotReplyShard},
};
use netidx_tools_core::ClientParams;
use structopt::StructOpt;
use tokio::io::{stdout, AsyncWriteExt};

use crate::subscriber::Out;

#[derive(StructOpt, Debug)]
pub(crate) struct OneshotParams {
    #[structopt(long = "base", help = "the base path of the recorder instance")]
    base: Path,
    #[structopt(long = "start", help = "the time to start the recording at")]
    start: Option<String>,
    #[structopt(long = "end", help = "the time to end the recording at")]
    end: Option<String>,
    #[structopt(short = "f", long = "filter", help = "glob pattern(s) to include")]
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
    #[structopt(name = "compress", about = "generate a compressed archive file")]
    Compress {
        #[structopt(long = "keep", help = "don't delete the input file")]
        keep: bool,
        #[structopt(
            long = "window",
            help = "how many batches to compress in parallel",
            default_value = "2"
        )]
        window: usize,
        file: PathBuf,
    },
    #[structopt(name = "index", about = "index the archive file")]
    Index {
        #[structopt(long = "keep", help = "don't delete the input file")]
        keep: bool,
        file: PathBuf,
    },
    #[structopt(name = "dump", about = "print the contents of an archive")]
    Dump {
        file: PathBuf,
        #[structopt(long = "metadata-only", about = "don't print the data")]
        metadata: bool,
	#[structopt(long = "check-index", about = "don't dump data but check all the indexes")]
	check_index: bool
    },
    #[structopt(name = "verify", about = "verify that an archive can be read")]
    Verify { file: PathBuf },
    #[structopt(name = "compressed", about = "if file compressed exit 0, 1 no")]
    Compressed { file: PathBuf },
}

fn parse_bound(s: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    match s {
        None => Ok(None),
        Some(s) => match s.parse::<DateTime<Utc>>() {
            Ok(dt) => Ok(Some(dt)),
            Err(_) => match s.parse::<Seek>() {
                Ok(Seek::TimeRelative(dur)) => Ok(Some(Utc::now() + dur)),
                Ok(Seek::Absolute(dt)) => Ok(Some(dt)),
                Ok(Seek::BatchRelative(_)) => {
                    bail!("batch relative seek isn't supported for oneshot")
                }
                Ok(Seek::Beginning) => Ok(None),
                Ok(Seek::End) => Ok(None),
                Err(_) => {
                    let mut buf = String::from("-");
                    buf.push_str(s);
                    match buf.parse::<Seek>()? {
                        Seek::TimeRelative(dur) => Ok(Some(Utc::now() + dur)),
                        Seek::Absolute(_)
                        | Seek::BatchRelative(_)
                        | Seek::Beginning
                        | Seek::End => {
                            bail!("batch relative seek isn't supported for oneshot")
                        }
                    }
                }
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
    let client = Client::new(&subscriber, &params.base)?;
    let mut res = client.oneshot(&start, &end, &filter).await?;
    for OneshotReplyShard { pathmap, image, .. } in res.0.iter_mut() {
        for (id, value) in image.drain() {
            Out { raw: false, path: &pathmap[&id], value }.write(&mut buf)?;
        }
    }
    stdout.write_all_buf(&mut buf).await?;
    loop {
        let min_ts = res.0.iter().fold(None, |mts, r| match r.deltas.front() {
            None => mts,
            Some((ts, _)) => match mts {
                None => Some(*ts),
                Some(mts) if *ts < mts => Some(*ts),
                Some(mts) => Some(mts),
            },
        });
        match min_ts {
            None => break,
            Some(min_ts) => {
                for OneshotReplyShard { pathmap, deltas, .. } in res.0.iter_mut() {
                    if let Some((ts, _)) = deltas.front() {
                        if min_ts == *ts {
                            let (ts, mut batch) = deltas.pop_front().unwrap();
                            Out {
                                raw: false,
                                path: "timestamp",
                                value: Event::Update(Value::DateTime(ts)),
                            }
                            .write(&mut buf)?;
                            for BatchItem(id, value) in batch.drain(..) {
                                Out { raw: false, path: &pathmap[&id], value }
                                    .write(&mut buf)?;
                            }
                            stdout.write_all_buf(&mut buf).await?;
                        }
                    }
                }
            }
        }
    }
    stdout.flush().await?;
    Ok(())
}

fn verify(file: impl AsRef<std::path::Path>) -> Result<()> {
    let reader = ArchiveReader::open(file)?;
    let mut cursor = Cursor::new();
    loop {
        let (_, batches) = reader.read_deltas(None, &mut cursor, 100)?;
        if batches.is_empty() {
            break;
        }
    }
    let mut cursor = Cursor::new();
    reader.seek(&mut cursor, Seek::Beginning);
    reader.build_image(None, &cursor)?;
    reader.seek(&mut cursor, Seek::End);
    reader.build_image(None, &cursor)?;
    Ok(())
}

async fn compress(file: PathBuf, keep: bool, window: usize) -> Result<()> {
    let hdr = logfile::read_file_header(&file)?;
    if !hdr.indexed {
        index(file.clone(), false).await?
    }
    let reader = ArchiveReader::open(file.clone())?;
    let mut compressed = file.to_string_lossy().into_owned();
    compressed.push_str(".rz");
    reader.compress(window, &compressed).await?;
    if let Err(e) = verify(compressed.clone()) {
        std::fs::remove_file(&compressed)?;
        return Err(e).context("verifying contents");
    }
    if !keep {
        std::fs::rename(compressed, file)?
    }
    Ok(())
}

async fn index(file: PathBuf, keep: bool) -> Result<()> {
    let reader = ArchiveReader::open(file.clone())?;
    let mut indexed = file.to_string_lossy().into_owned();
    indexed.push_str(".ix");
    reader.build_index(&indexed).await?;
    if let Err(e) = verify(indexed.clone()) {
        std::fs::remove_file(&indexed)?;
        return Err(e).context("verifying contents");
    }
    if !keep {
        std::fs::rename(indexed, file)?
    }
    Ok(())
}

fn dump(file: PathBuf, metadata: bool, check_index: bool) -> Result<()> {
    let reader = ArchiveReader::open(file)?;
    reader.check_remap_rescan(false)?;
    if !metadata {
        println!("---------------- pathmap --------------------");
        for (id, path) in reader.index().iter_pathmap() {
            println!("{:?}: {}", id, path);
        }
    }
    println!("--------------- metadata --------------------");
    println!("image batches: {}", reader.image_batches());
    println!("delta batches: {}", reader.delta_batches());
    println!("compressed: {}", reader.is_compressed());
    println!("indexed: {}", reader.is_indexed());
    let filter = if check_index {
	Some(HashSet::default())
    } else {
	None
    };
    if !metadata {
        let mut cursor = Cursor::new();
        loop {
            let (_, batches) = reader.read_deltas(filter.as_ref(), &mut cursor, 100)?;
            if batches.is_empty() {
                return Ok(());
            }
            for (ts, batch) in batches.iter() {
                for BatchItem(id, ev) in batch.iter() {
                    println!("{}: {:?} -> {:?}", ts, id, ev);
                }
            }
        }
    }
    Ok(())
}

fn compressed(file: PathBuf) -> Result<()> {
    let hdr = logfile::read_file_header(file)?;
    if hdr.compressed {
        std::process::exit(0)
    } else {
        std::process::exit(1)
    }
}

pub(super) async fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Oneshot { common, params } => {
            let (cfg, auth) = common.load();
            let subscriber = Subscriber::new(cfg, auth).context("create subscriber")?;
            oneshot(subscriber, params).await
        }
        Cmd::Compress { file, window, keep } => compress(file, keep, window).await,
        Cmd::Dump { file, metadata, check_index } => dump(file, metadata, check_index),
        Cmd::Verify { file } => verify(file),
        Cmd::Compressed { file } => compressed(file),
        Cmd::Index { file, keep } => index(file, keep).await,
    }
}
