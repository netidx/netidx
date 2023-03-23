use crate::logfile::{ArchiveReader, ArchiveWriter, BatchItem, Timestamp};
use anyhow::Result;
use chrono::prelude::*;
use futures::future;
use log::{error, info};
use netidx::{
    chars::Chars, config::Config as NetIdxCfg, path::Path, pool::Pooled, protocol::glob::Glob,
    publisher::BindCfg, resolver_client::DesiredAuth,
};
use serde::{Serialize, Deserialize};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{runtime::Runtime, sync::broadcast, task, time};

mod publish;
mod record;

mod file {
    use super::*;

    pub(super) fn default_max_sessions() -> usize {
        512
    }

    pub(super) fn default_max_sessions_per_client() -> usize {
        64
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct PublishConfig {
        pub(super) base: Path,
        #[serde(default)]
        pub(super) bind: Option<String>,
        #[serde(default = "default_max_sessions")]
        pub(super) max_sessions: usize,
        #[serde(default = "default_max_sessions_per_client")]
        pub(super) max_sessions_per_client: usize,
        #[serde(default)]
        pub(super) shards: Option<usize>,
    }

    pub(super) fn default_poll_interval() -> Duration {
        Duration::from_secs(5)
    }

    pub(super) fn default_image_frequency() -> Option<usize> {
        Some(67108864)
    }

    pub(super) fn default_flush_frequency() -> Option<usize> {
        Some(65534)
    }

    pub(super) fn default_flush_interval() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }

    pub(super) fn default_rotate_interval() -> Option<Duration> {
        Some(Duration::from_secs(86400))
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct RecordConfig {
        pub(super) spec: Vec<Chars>,
        #[serde(default = "default_poll_interval")]
        pub(super) poll_interval: Duration,
        #[serde(default = "default_image_frequency")]
        pub(super) image_frequency: Option<usize>,
        #[serde(default = "default_flush_frequency")]
        pub(super) flush_frequency: Option<usize>,
        #[serde(default = "default_flush_interval")]
        pub(super) flush_interval: Option<Duration>,
        #[serde(default = "default_rotate_interval")]
        pub(super) rotate_interval: Option<Duration>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct Config {
        pub(super) archive_directory: PathBuf,
        #[serde(default)]
        pub(super) record: Option<RecordConfig>,
        #[serde(default)]
        pub(super) publish: Option<PublishConfig>
    }
}

/// Configuration of the publish part of the recorder
#[derive(Debug, Clone)]
pub struct PublishConfig {
    /// The base path to publish under
    pub base: Path,
    /// The publisher bind config. None to use the site default config.
    pub bind: Option<BindCfg>,
    /// The maximum number of client sessions
    pub max_sessions: usize,
    /// The maximum number of sessions per unique client
    pub max_sessions_per_client: usize,
    /// How many shards this recorder instance is divided into
    pub shards: Option<usize>
}

impl Default for PublishConfig {
    fn default() -> Self {
        Self {
            base: Path::from("/example/recorder"),
            bind: None,
            max_sessions: file::default_max_sessions(),
            max_sessions_per_client: file::default_max_sessions_per_client(),
            shards: None,
        }
    }
}

impl TryFrom<file::PublishConfig> for PublishConfig {
    type Error = anyhow::Error;

    fn try_from(f: file::PublishConfig) -> Result<Self> {
        Ok(Self {
            base: f.base,
            bind: f.bind.map(|s| s.parse::<BindCfg>()).transpose()?,
            max_sessions: f.max_sessions,
            max_sessions_per_client: f.max_sessions_per_client,
            shards: f.shards
        })
    }
}

/// Configuration of the record part of the recorder
#[derive(Debug, Clone)]
pub struct RecordConfig {
    /// the path spec globs to record
    pub spec: Vec<Glob>,
    /// how often to poll the resolver
    pub poll_interval: Duration,
    /// how often to write a full image. None means never write
    /// images.
    pub image_frequency: Option<usize>,
    /// flush the file after the specified number of pages have
    /// been written. None means never flush.
    pub flush_frequency: Option<usize>,
    /// flush the file after the specified elapsed time. None means
    /// flush only on shutdown.
    pub flush_interval: Option<Duration>,
    /// rotate the log file at the specified interval. None means
    /// never rotate the file.
    pub rotate_interval: Option<Duration>,
}

impl Default for RecordConfig {
    fn default() -> Self {
        Self {
            spec: vec![Glob::new(Chars::from("/tmp/**")).unwrap()],
            poll_interval: file::default_poll_interval(),
            image_frequency: file::default_image_frequency(),
            flush_frequency: file::default_flush_frequency(),
            flush_interval: file::default_flush_interval(),
            rotate_interval: file::default_rotate_interval(),
        }
    }
}

impl TryFrom<file::RecordConfig> for RecordConfig {
    type Error = anyhow::Error;

    fn try_from(f: file::RecordConfig) -> Result<Self> {
        Ok(Self {
            spec: f.spec.into_iter().map(Glob::new).collect::<Result<Vec<_>>>()?,
            poll_interval: f.poll_interval,
            image_frequency: f.image_frequency,
            flush_frequency: f.flush_frequency,
            flush_interval: f.flush_interval,
            rotate_interval: f.rotate_interval,
        })
    }
}

/// Configuration of the recorder
#[derive(Debug, Clone)]
pub struct Config {
    /// The directory where the archive files live. The current
    /// archive will be called 'current', and previous rotated archive
    /// files will be named with the rfc3339 timestamp that specifies
    /// when they were rotated (and thus when they ended).
    pub archive_directory: PathBuf,
    /// If specifed this recorder will record to the archive
    /// directory. It is possible for the same archiver to both record
    /// and publish. One of record or publish must be specifed.
    pub record: Option<RecordConfig>,
    /// If specified this recorder will publish the archive
    /// directory. It is possible for the same archiver to both record
    /// and publish. One of record or publish must be specifed.
    pub publish: Option<PublishConfig>,
    
}

impl Default for Config {
    fn default() -> Self {
        Self {
        }
    }
}

#[derive(Debug, Clone)]
pub enum BCastMsg {
    LogRotated(DateTime<Utc>),
    Batch(Timestamp, Arc<Pooled<Vec<BatchItem>>>),
    Stop,
}

/*
#[cfg(unix)]
async fn should_exit() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate())?;
    let mut quit = signal(SignalKind::quit())?;
    let mut intr = signal(SignalKind::interrupt())?;
    select_biased! {
        _ = term.recv().fuse() => Ok(()),
        _ = quit.recv().fuse() => Ok(()),
        _ = intr.recv().fuse() => Ok(()),
    }
}

#[cfg(windows)]
async fn should_exit() -> Result<()> {
    Ok(signal::ctrl_c().await?)
}
*/

async fn run_async(
    config: Config,
    publish_args: Option<(Option<BindCfg>, Path)>,
    auth: DesiredAuth,
    image_frequency: Option<usize>,
    poll_interval: Option<time::Duration>,
    flush_frequency: Option<usize>,
    flush_interval: Option<time::Duration>,
    shards: usize,
    max_sessions: usize,
    max_sessions_per_client: usize,
    archive: String,
    spec: Vec<Glob>,
) -> Result<()> {
    let mut wait = Vec::new();
    let (bcast_tx, bcast_rx) = broadcast::channel(100);
    drop(bcast_rx);
    let writer = if spec.is_empty() {
        None
    } else {
        Some(ArchiveWriter::open(archive.as_str()).unwrap())
    };
    if let Some((bind_cfg, publish_base)) = publish_args {
        let reader = writer
            .as_ref()
            .map(|w| w.reader().unwrap())
            .unwrap_or_else(|| ArchiveReader::open(archive.as_str()).unwrap());
        let bcast_tx = bcast_tx.clone();
        let config = config.clone();
        let auth = auth.clone();
        wait.push(task::spawn(async move {
            let res = publish::run(
                bcast_tx,
                reader,
                config,
                auth,
                bind_cfg,
                publish_base,
                shards,
                max_sessions,
                max_sessions_per_client,
            )
            .await;
            match res {
                Ok(()) => info!("archive publisher exited"),
                Err(e) => error!("archive publisher exited with error: {}", e),
            }
        }));
    }
    if !spec.is_empty() {
        let bcast_tx = bcast_tx.clone();
        wait.push(task::spawn(async move {
            let res = record::run(
                bcast_tx,
                writer.unwrap(),
                config,
                auth,
                poll_interval,
                image_frequency,
                flush_frequency,
                flush_interval,
                spec,
            )
            .await;
            match res {
                Ok(()) => info!("archive writer exited"),
                Err(e) => error!("archive writer exited with error: {}", e),
            }
        }));
    }
    future::join_all(wait).await;
    Ok(())
}

struct Recorder(broadcast::Sender<BCastMsg>);

impl Recorder {
    pub async fn start(config: Config, auth: DesiredAuth, params: Params) -> Self {
        let image_frequency =
            if params.image_frequency == 0 { None } else { Some(params.image_frequency) };
        let poll_interval = if params.poll_interval == 0 {
            None
        } else {
            Some(time::Duration::from_secs(params.poll_interval))
        };
        let flush_frequency =
            if params.flush_frequency == 0 { None } else { Some(params.flush_frequency) };
        let flush_interval = if params.flush_interval == 0 {
            None
        } else {
            Some(time::Duration::from_secs(params.flush_interval))
        };
        let publish_args = match (params.bind, params.publish_base) {
            (None, None) => None,
            (None, Some(publish_base)) => Some((None, publish_base)),
            (Some(bind), Some(publish_base)) => {
                match bind {
                    BindCfg::Match { .. } | BindCfg::Local => (),
                    BindCfg::Exact(_) => {
                        panic!("exact bindcfgs are not supported for this publisher")
                    }
                }
                Some((Some(bind), publish_base))
            }
            (Some(_), None) => {
                panic!("you must specify bind and publish_base to publish an archive")
            }
        };
        if params.spec.is_empty() && publish_args.is_none() {
            panic!("you must specify a publish config, some paths to log, or both")
        }
        let spec = params
            .spec
            .into_iter()
            .map(Chars::from)
            .map(Glob::new)
            .collect::<Result<Vec<Glob>>>()
            .unwrap();
        let rt = Runtime::new().expect("failed to init tokio runtime");
        rt.block_on(run_async(
            config,
            publish_args,
            auth,
            image_frequency,
            poll_interval,
            flush_frequency,
            flush_interval,
            params.shards,
            params.max_sessions,
            params.max_sessions_per_client,
            params.archive,
            spec,
        ))
    }
}
