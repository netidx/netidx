use crate::logfile::{ArchiveReader, BatchItem};
use anyhow::Result;
use chrono::prelude::*;
use futures::{
    future::{self, Shared},
    FutureExt,
};
use log::error;
use netidx::{
    chars::Chars, config::Config as NetIdxCfg, path::Path, pool::Pooled,
    protocol::glob::Glob, publisher::BindCfg, resolver_client::DesiredAuth,
    subscriber::Subscriber,
};
use serde::{Deserialize, Serialize};
use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc, time::Duration};
use tokio::{sync::broadcast, task};

pub mod logfile_collection;
mod logfile_index;
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

    impl PublishConfig {
        fn example() -> Self {
            Self {
                base: Path::from("/archive"),
                bind: None,
                max_sessions: default_max_sessions(),
                max_sessions_per_client: default_max_sessions_per_client(),
                shards: Some(0),
            }
        }
    }

    pub(super) fn default_poll_interval() -> Option<Duration> {
        Some(Duration::from_secs(5))
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
        pub(super) poll_interval: Option<Duration>,
        #[serde(default = "default_image_frequency")]
        pub(super) image_frequency: Option<usize>,
        #[serde(default = "default_flush_frequency")]
        pub(super) flush_frequency: Option<usize>,
        #[serde(default = "default_flush_interval")]
        pub(super) flush_interval: Option<Duration>,
        #[serde(default = "default_rotate_interval")]
        pub(super) rotate_interval: Option<Duration>,
    }

    impl RecordConfig {
        fn example() -> Self {
            Self {
                spec: vec![Chars::from("/tmp/**")],
                poll_interval: default_poll_interval(),
                image_frequency: default_image_frequency(),
                flush_frequency: default_flush_frequency(),
                flush_interval: default_flush_interval(),
                rotate_interval: default_rotate_interval(),
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub(super) struct Config {
        pub(super) archive_directory: PathBuf,
        #[serde(default)]
        pub(super) archive_cmds: Option<ArchiveCmds>,
        #[serde(default)]
        pub(super) netidx_config: Option<PathBuf>,
        #[serde(default)]
        pub(super) desired_auth: Option<DesiredAuth>,
        #[serde(default)]
        pub(super) record: Option<RecordConfig>,
        #[serde(default)]
        pub(super) publish: Option<PublishConfig>,
    }

    impl Config {
        pub(super) fn example() -> String {
            serde_json::to_string_pretty(&Self {
                archive_directory: PathBuf::from("/foo/bar"),
                archive_cmds: Some(ArchiveCmds {
                    list: ("cmd_to_list_dates_in_archive".into(), vec![]),
                    get: ("cmd_to_fetch_file_from_archive".into(), vec![]),
                    put: ("cmd_to_put_file_into_archive".into(), vec![]),
                }),
                netidx_config: None,
                desired_auth: None,
                record: Some(RecordConfig::example()),
                publish: Some(PublishConfig::example()),
            })
            .unwrap()
        }
    }
}

/// Configuration of the publish part of the recorder
#[derive(Debug, Clone)]
pub struct PublishConfig {
    /// The base path to publish under
    pub base: Path,
    /// The publisher bind config. None to use the site default config.
    pub bind: BindCfg,
    /// The maximum number of client sessions
    pub max_sessions: usize,
    /// The maximum number of sessions per unique client
    pub max_sessions_per_client: usize,
    /// How many shards this recorder instance is divided into
    pub shards: Option<usize>,
}

impl PublishConfig {
    /// create a new PublishConfig with the specified base path and
    /// all other parameters set to the default values.
    pub fn new(netidx_cfg: &NetIdxCfg, base: Path) -> Self {
        Self {
            base,
            bind: netidx_cfg.default_bind_config.clone(),
            max_sessions: file::default_max_sessions(),
            max_sessions_per_client: file::default_max_sessions_per_client(),
            shards: None,
        }
    }

    fn from_file(netidx_cfg: &NetIdxCfg, f: file::PublishConfig) -> Result<Self> {
        Ok(Self {
            base: f.base,
            bind: f
                .bind
                .map(|s| s.parse::<BindCfg>())
                .unwrap_or_else(|| Ok(netidx_cfg.default_bind_config.clone()))?,
            max_sessions: f.max_sessions,
            max_sessions_per_client: f.max_sessions_per_client,
            shards: f.shards,
        })
    }
}

/// Configuration of the record part of the recorder
#[derive(Debug, Clone)]
pub struct RecordConfig {
    /// the path spec globs to record
    pub spec: Vec<Glob>,
    /// how often to poll the resolver for structure changes. None
    /// means only once at startup.
    pub poll_interval: Option<Duration>,
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

impl RecordConfig {
    /// Create a new RecordConfig logging the specified globs with all
    /// other parameters set to the default.
    pub fn new(spec: Vec<Glob>) -> Self {
        Self {
            spec,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveCmds {
    pub list: (String, Vec<String>),
    pub get: (String, Vec<String>),
    pub put: (String, Vec<String>),
}

/// Configuration of the recorder
#[derive(Debug, Clone)]
pub struct Config {
    /// The directory where the archive files live. The current
    /// archive will be called 'current', and previous rotated archive
    /// files will be named with the rfc3339 timestamp that specifies
    /// when they were rotated (and thus when they ended).
    pub archive_directory: PathBuf,
    pub archive_cmds: Option<ArchiveCmds>,
    /// The netidx config to use
    pub netidx_config: NetIdxCfg,
    /// The netidx desired authentication mechanism to use
    pub desired_auth: DesiredAuth,
    /// If specifed this recorder will record to the archive
    /// directory. It is possible for the same archiver to both record
    /// and publish. One of record or publish must be specifed.
    pub record: Option<RecordConfig>,
    /// If specified this recorder will publish the archive
    /// directory. It is possible for the same archiver to both record
    /// and publish. One of record or publish must be specifed.
    pub publish: Option<PublishConfig>,
}

impl TryFrom<file::Config> for Config {
    type Error = anyhow::Error;

    fn try_from(f: file::Config) -> Result<Self> {
        let netidx_config = f
            .netidx_config
            .map(NetIdxCfg::load)
            .unwrap_or_else(|| NetIdxCfg::load_default())?;
        let desired_auth = f.desired_auth.unwrap_or_else(|| netidx_config.default_auth());
        let publish =
            f.publish.map(|f| PublishConfig::from_file(&netidx_config, f)).transpose()?;
        Ok(Self {
            archive_directory: f.archive_directory,
            archive_cmds: f.archive_cmds,
            netidx_config,
            desired_auth,
            record: f.record.map(RecordConfig::try_from).transpose()?,
            publish,
        })
    }
}

impl Config {
    /// Load the config from the specified file
    pub async fn load<F: AsRef<std::path::Path>>(file: F) -> Result<Config> {
        let s = tokio::fs::read(file).await?;
        let f = serde_json::from_slice::<file::Config>(&s)?;
        Config::try_from(f)
    }

    pub fn example() -> String {
        file::Config::example()
    }
}

#[derive(Debug, Clone)]
enum BCastMsg {
    LogRotated(DateTime<Utc>),
    NewCurrent(ArchiveReader),
    Batch(DateTime<Utc>, Arc<Pooled<Vec<BatchItem>>>),
    Stop,
}

pub struct Recorder {
    tx: broadcast::Sender<BCastMsg>,
    wait: Shared<Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>>,
}

impl Recorder {
    async fn run(
        config: Arc<Config>,
        bcast_tx: broadcast::Sender<BCastMsg>,
    ) -> Result<()> {
        let mut wait = Vec::new();
        let subscriber =
            Subscriber::new(config.netidx_config.clone(), config.desired_auth.clone())?;
        if let Some(publish_config) = config.publish.as_ref() {
            let publish_config = Arc::new(publish_config.clone());
            let subscriber = subscriber.clone();
            let config = config.clone();
            let bcast_tx = bcast_tx.clone();
            let bcast_rx = bcast_tx.subscribe();
            wait.push(task::spawn(async {
                if let Err(e) =
                    publish::run(bcast_tx, bcast_rx, subscriber, config, publish_config)
                        .await
                {
                    error!("publisher stopped on error {}", e)
                }
            }));
        }
        if let Some(record_config) = config.record.as_ref() {
            let record_config = Arc::new(record_config.clone());
            let subscriber = subscriber.clone();
            let config = config.clone();
            let bcast_tx = bcast_tx.clone();
            let bcast_rx = bcast_tx.subscribe();
            wait.push(task::spawn(async move {
                if let Err(e) =
                    record::run(bcast_tx, bcast_rx, subscriber, config, record_config)
                        .await
                {
                    error!("recorder stopped on error {}", e);
                }
            }))
        }
        future::join_all(wait).await;
        Ok(())
    }

    /// Start the recorder
    pub fn start(config: Config) -> Self {
        let config = Arc::new(config);
        let (bcast_tx, _) = broadcast::channel(100);
        let tx = bcast_tx.clone();
        let wait = (Box::pin(async {
            let r = task::spawn(async move {
                if let Err(e) = Self::run(config, bcast_tx).await {
                    error!("recorder shutdown with {}", e)
                }
            })
            .await;
            if let Err(e) = r {
                error!("failed to join {}", e)
            }
        })
            as Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>)
            .shared();
        Self { tx, wait }
    }

    /// This will return when the recorder has shutdown, either
    /// gracefully or not.
    pub async fn wait_shutdown(&self) {
        let _ = self.wait.clone().await;
    }

    /// Initiate a clean recorder shutdown. To wait for it to complete
    /// you should call wait_shutdown
    pub fn shutdown(&self) {
        let _ = self.tx.send(BCastMsg::Stop);
    }
}
