use crate::logfile::{ArchiveReader, ArchiveWriter, BatchItem};
use anyhow::Result;
use arcstr::ArcStr;
use chrono::prelude::*;
use fxhash::FxHashMap;
use log::error;
use netidx::{
    chars::Chars,
    config::Config as NetIdxCfg,
    path::Path,
    pool::Pooled,
    protocol::glob::Glob,
    publisher::{BindCfg, PublisherBuilder},
    resolver_client::DesiredAuth,
    subscriber::Subscriber,
};
use netidx_core::atomic_id;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path as FilePath, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::broadcast, task::JoinSet};

use self::file::RecordShardConfig;

pub mod logfile_collection;
mod logfile_index;
mod oneshot;
mod publish;
mod record;

pub mod file {
    use super::RotateDirective;
    use std::collections::HashMap;

    use super::*;

    pub fn default_max_sessions() -> usize {
        512
    }

    pub fn default_max_sessions_per_client() -> usize {
        64
    }

    pub fn default_oneshot_data_limit() -> usize {
        104857600
    }

    pub fn default_cluster() -> String {
        "cluster".into()
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct PublishConfig {
        pub base: Path,
        #[serde(default)]
        pub bind: Option<String>,
        #[serde(default = "default_max_sessions")]
        pub max_sessions: usize,
        #[serde(default = "default_max_sessions_per_client")]
        pub max_sessions_per_client: usize,
        #[serde(default = "default_oneshot_data_limit")]
        pub oneshot_data_limit: usize,
        #[serde(default)]
        pub cluster_shards: Option<usize>,
        #[serde(default = "default_cluster")]
        pub cluster: String,
    }

    impl PublishConfig {
        pub fn example() -> Self {
            Self {
                base: Path::from("/archive"),
                bind: None,
                max_sessions: default_max_sessions(),
                max_sessions_per_client: default_max_sessions_per_client(),
                oneshot_data_limit: default_oneshot_data_limit(),
                cluster_shards: Some(0),
                cluster: default_cluster(),
            }
        }
    }

    pub fn default_poll_interval() -> Option<Duration> {
        Some(Duration::from_secs(5))
    }

    pub fn default_image_frequency() -> Option<usize> {
        Some(67108864)
    }

    pub fn default_flush_frequency() -> Option<usize> {
        Some(65534)
    }

    pub fn default_flush_interval() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }

    pub fn default_rotate_interval() -> RotateDirective {
        RotateDirective::Interval(Duration::from_secs(86400))
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordShardConfig {
        pub spec: Vec<Chars>,
        pub poll_interval: Option<Duration>,
        pub image_frequency: Option<usize>,
        pub flush_frequency: Option<usize>,
        pub flush_interval: Option<Duration>,
        pub rotate_interval: Option<RotateDirective>,
    }

    impl RecordShardConfig {
        pub fn example() -> Self {
            Self {
                spec: vec![Chars::from("/tmp/**")],
                poll_interval: None,
                image_frequency: None,
                flush_frequency: None,
                flush_interval: None,
                rotate_interval: None,
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordConfig {
        #[serde(default = "default_poll_interval")]
        pub poll_interval: Option<Duration>,
        #[serde(default = "default_image_frequency")]
        pub image_frequency: Option<usize>,
        #[serde(default = "default_flush_frequency")]
        pub flush_frequency: Option<usize>,
        #[serde(default = "default_flush_interval")]
        pub flush_interval: Option<Duration>,
        #[serde(default = "default_rotate_interval")]
        pub rotate_interval: RotateDirective,
        pub shards: HashMap<ArcStr, RecordShardConfig>,
    }

    impl RecordConfig {
        pub fn example() -> Self {
            Self {
                poll_interval: default_poll_interval(),
                image_frequency: default_image_frequency(),
                flush_frequency: default_flush_frequency(),
                flush_interval: default_flush_interval(),
                rotate_interval: default_rotate_interval(),
                shards: HashMap::from([("0".into(), RecordShardConfig::example())]),
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Config {
        pub archive_directory: PathBuf,
        #[serde(default)]
        pub archive_cmds: Option<ArchiveCmds>,
        #[serde(default)]
        pub netidx_config: Option<PathBuf>,
        #[serde(default)]
        pub desired_auth: Option<DesiredAuth>,
        #[serde(default)]
        pub record: Option<RecordConfig>,
        #[serde(default)]
        pub publish: Option<PublishConfig>,
    }

    impl Config {
        pub fn example() -> String {
            serde_json::to_string_pretty(&Self {
                archive_directory: PathBuf::from("/foo/bar"),
                archive_cmds: Some(ArchiveCmds {
                    list: (
                        "cmd_to_list_dates_in_archive".into(),
                        vec!["-s".into(), "{shard}".into()],
                    ),
                    get: (
                        "cmd_to_fetch_file_from_archive".into(),
                        vec!["-s".into(), "{shard}".into()],
                    ),
                    put: (
                        "cmd_to_put_file_into_archive".into(),
                        vec!["-s".into(), "{shard}".into()],
                    ),
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RotateDirective {
    Interval(Duration),
    Size(usize),
    Never,
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
    /// The maximum number of bytes a oneshot will return
    pub oneshot_data_limit: usize,
    /// How many external shards there are. e.g. instances on other
    /// machines. This is used to sync up the cluster.
    pub cluster_shards: Option<usize>,
    /// The cluster name to join, default is "cluster".
    pub cluster: String,
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
            oneshot_data_limit: file::default_oneshot_data_limit(),
            cluster_shards: None,
            cluster: file::default_cluster(),
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
            oneshot_data_limit: f.oneshot_data_limit,
            cluster_shards: f.cluster_shards,
            cluster: f.cluster,
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
    /// rotate the log file at the specified interval or file size or
    /// never.
    pub rotate_interval: RotateDirective,
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

    fn from_file(f: file::RecordConfig) -> Result<HashMap<ArcStr, RecordConfig>> {
        let mut shards = HashMap::default();
        for (name, c) in f.shards {
            let RecordShardConfig {
                spec,
                poll_interval,
                image_frequency,
                flush_frequency,
                flush_interval,
                rotate_interval,
            } = c;
            let res = RecordConfig {
                spec: spec.into_iter().map(Glob::new).collect::<Result<Vec<_>>>()?,
                poll_interval: poll_interval.or(f.poll_interval),
                image_frequency: image_frequency.or(f.image_frequency),
                flush_frequency: flush_frequency.or(f.flush_frequency),
                flush_interval: flush_interval.or(f.flush_interval),
                rotate_interval: rotate_interval.unwrap_or(f.rotate_interval),
            };
            shards.insert(name, res);
        }
        Ok(shards)
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
    /// Record. Each entry in the HashMap is a shard, which will
    /// record independently to an archive directory under the base
    /// directory. E.G. a shard named "0" will record under
    /// ${archive_base}/0. If publish is specified all configured
    /// shards on this instance will be published.
    pub record: HashMap<ArcStr, RecordConfig>,
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
            record: f
                .record
                .map(|r| RecordConfig::from_file(r))
                .transpose()?
                .unwrap_or(HashMap::default()),
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

atomic_id!(ShardId);

#[derive(Debug, Clone)]
enum BCastMsg {
    LogRotated(DateTime<Utc>),
    NewCurrent(ArchiveReader),
    Batch(DateTime<Utc>, Arc<Pooled<Vec<BatchItem>>>),
}

pub(crate) struct Shards {
    by_id: FxHashMap<ShardId, ArcStr>,
    by_name: FxHashMap<ArcStr, ShardId>,
    pathindexes: FxHashMap<ShardId, ArchiveReader>,
    heads: RwLock<FxHashMap<ShardId, ArchiveReader>>,
    bcast: FxHashMap<ShardId, broadcast::Sender<BCastMsg>>,
}

impl Shards {
    fn read(
        path: impl AsRef<FilePath>,
    ) -> Result<(FxHashMap<ShardId, ArchiveWriter>, Arc<Self>)> {
        use std::fs;
        let mut t = Self {
            by_id: HashMap::default(),
            by_name: HashMap::default(),
            pathindexes: HashMap::default(),
            heads: RwLock::new(HashMap::default()),
            bcast: HashMap::default(),
        };
        for ent in fs::read_dir(path.as_ref())? {
            let ent = ent?;
            let name = ArcStr::from(ent.file_name().to_string_lossy());
            if ent.file_type()?.is_dir() && &name != ".." {
                let id = ShardId::new();
                t.by_id.insert(id, name.clone());
                t.by_name.insert(name, id);
                let indexpath = ent.path().join("pathindex");
                t.pathindexes.insert(id, ArchiveReader::open(indexpath)?);
                if let Ok(head) = ArchiveReader::open(ent.path().join("current")) {
                    t.heads.write().insert(id, head);
                }
                let (tx, _) = broadcast::channel(1000);
                t.bcast.insert(id, tx);
            }
        }
        Ok((HashMap::default(), Arc::new(t)))
    }

    fn from_cfg(
        archive_directory: &PathBuf,
        cfg: &HashMap<ArcStr, RecordConfig>,
    ) -> Result<(FxHashMap<ShardId, ArchiveWriter>, Arc<Self>)> {
        use std::fs;
        let mut t = Self {
            by_id: HashMap::default(),
            by_name: HashMap::default(),
            pathindexes: HashMap::default(),
            heads: RwLock::new(HashMap::default()),
            bcast: HashMap::default(),
        };
        let mut writers = HashMap::default();
        for name in cfg.keys() {
            let id = ShardId::new();
            t.by_id.insert(id, name.clone());
            t.by_name.insert(name.clone(), id);
            let dir = archive_directory.join(&**name);
            fs::create_dir_all(&dir)?;
            let writer = ArchiveWriter::open(dir.join("pathindex"))?;
            let reader = writer.reader()?;
            t.pathindexes.insert(id, reader);
            writers.insert(id, writer);
            let (tx, _) = broadcast::channel(1000);
            t.bcast.insert(id, tx);
        }
        Ok((writers, Arc::new(t)))
    }
}

pub struct Recorder {
    config: Arc<Config>,
    wait: JoinSet<()>,
}

impl Recorder {
    async fn start_jobs(&mut self) -> Result<()> {
        let config = self.config.clone();
        let subscriber =
            Subscriber::new(config.netidx_config.clone(), config.desired_auth.clone())?;
        let (mut writers, shards) = if config.record.is_empty() {
            Shards::read(&config.archive_directory)?
        } else {
            Shards::from_cfg(&config.archive_directory, &config.record)?
        };
        if let Some(publish_config) = config.publish.as_ref() {
            let publish_config = Arc::new(publish_config.clone());
            let publisher = PublisherBuilder::new(config.netidx_config.clone())
                .desired_auth(config.desired_auth.clone())
                .bind_cfg(Some(publish_config.bind.clone()))
                .build()
                .await?;
            self.wait.spawn({
                let shards = shards.clone();
                let publish_config = publish_config.clone();
                let subscriber = subscriber.clone();
                let config = config.clone();
                let publisher = publisher.clone();
                async move {
                    let r = publish::run(
                        shards,
                        subscriber,
                        config,
                        publish_config,
                        publisher,
                    )
                    .await;
                    if let Err(e) = r {
                        error!("publisher stopped on error {}", e)
                    }
                }
            });
            self.wait.spawn({
                let subscriber = subscriber.clone();
                let shards = shards.clone();
                let publish_config = publish_config.clone();
                let config = config.clone();
                let publisher = publisher.clone();
                async move {
                    let r = oneshot::run(
                        shards,
                        config,
                        publish_config,
                        publisher,
                        subscriber,
                    )
                    .await;
                    if let Err(e) = r {
                        error!("publisher oneshot stopped on error {}", e)
                    }
                }
            });
        }
        for (name, cfg) in config.record.iter() {
            let name = name.clone();
            let id = shards.by_name[&name];
            let pathindex_writer = writers.remove(&id).unwrap();
            let record_config = Arc::new(cfg.clone());
            let subscriber = subscriber.clone();
            let config = config.clone();
            let shards = shards.clone();
            self.wait.spawn(async move {
                let r = record::run(
                    shards,
                    pathindex_writer,
                    subscriber,
                    config,
                    record_config,
                    id,
                    name,
                )
                    .await;
                if let Err(e) = r {
                    error!("recorder stopped on error {}", e);
                }
            });
        }
        Ok(())
    }

    /// Start the recorder
    pub async fn start(config: Config) -> Result<Self> {
        let config = Arc::new(config);
        let mut t = Self { wait: JoinSet::new(), config };
        t.start_jobs().await?;
        Ok(t)
    }
}
