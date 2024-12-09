use self::file::RecordShardConfig;
use anyhow::Result;
use arcstr::ArcStr;
use derive_builder::Builder;
use netidx::{
    chars::Chars,
    config::Config as NetIdxCfg,
    path::Path,
    protocol::glob::Glob,
    publisher::BindCfg,
    resolver_client::{DesiredAuth, GlobSet},
};
use serde_derive::{Deserialize, Serialize};
use std::{collections::HashMap, path::{PathBuf, Path as FilePath}, time::Duration};

pub mod file {
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

    impl Default for PublishConfig {
        fn default() -> Self {
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

    impl PublishConfig {
        pub fn example() -> Self {
            Self::default()
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

    pub fn default_slack() -> usize {
        100
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RecordShardConfig {
        /// The globs defining the path space this shard will
        /// record. This MUST be disjoint from the path space recorded
        /// by any other shard.
        ///
        /// If spec is empty, then no recorder task will be started
        /// for the shard, however the shard's `ArchiveCollectionWriter`
        /// will be available so you can log to the shard directly.
        pub spec: Vec<Chars>,
        /// override the poll_interval for this shard
        pub poll_interval: Option<Duration>,
        /// override the image_frequency for this shard
        pub image_frequency: Option<usize>,
        /// override the flush_frequency for this shard
        pub flush_frequency: Option<usize>,
        /// override the flush_interval for this shard
        pub flush_interval: Option<Duration>,
        /// override the rotate_interval for this shard
        pub rotate_interval: Option<RotateDirective>,
        /// how much channel slack between subscriber and the recorder
        /// task should this shard have. Higher numbers use more
        /// memory but will reduce pushback on the publisher when the
        /// disk is busy.
        #[serde(default = "default_slack")]
        pub slack: usize,
    }

    impl Default for RecordShardConfig {
        fn default() -> Self {
            Self {
                spec: vec![Chars::from("/tmp/**")],
                poll_interval: None,
                image_frequency: None,
                flush_frequency: None,
                flush_interval: None,
                rotate_interval: None,
                slack: default_slack(),
            }
        }
    }

    impl RecordShardConfig {
        pub fn example() -> Self {
            Self::default()
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

    impl Default for RecordConfig {
        fn default() -> Self {
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

    impl RecordConfig {
        pub fn example() -> Self {
            Self::default()
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

    impl Default for Config {
        fn default() -> Self {
            Self {
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
            }
        }
    }

    impl Config {
        pub fn example() -> String {
            serde_json::to_string_pretty(&Self::default()).unwrap()
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
#[derive(Debug, Clone, Builder)]
pub struct PublishConfig {
    /// The base path to publish under
    #[builder(setter(into))]
    pub(crate) base: Path,
    /// The publisher bind config.
    pub(crate) bind: BindCfg,
    /// The maximum number of client sessions
    #[builder(default = "file::default_max_sessions()")]
    pub(crate) max_sessions: usize,
    /// The maximum number of sessions per unique client
    #[builder(default = "file::default_max_sessions_per_client()")]
    pub(crate) max_sessions_per_client: usize,
    /// The maximum number of bytes a oneshot will return
    #[builder(default = "file::default_oneshot_data_limit()")]
    pub(crate) oneshot_data_limit: usize,
    /// How many external shards there are. e.g. instances on other
    /// machines. This is used to sync up the cluster.
    #[builder(default = "None")]
    pub(crate) cluster_shards: Option<usize>,
    /// The cluster name to join, default is "cluster".
    #[builder(default = "file::default_cluster()")]
    pub(crate) cluster: String,
}

impl PublishConfigBuilder {
    /// Set the bind config to the default bind config from the netidx cfg
    pub fn bind_from_cfg(&mut self, cfg: &NetIdxCfg) -> &mut Self {
        self.bind = Some(cfg.default_bind_config.clone());
        self
    }
}

impl PublishConfig {
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
#[derive(Debug, Clone, Builder)]
pub struct RecordConfig {
    /// The path spec globs to record. If you set this to an empty
    /// `GlobSet` then no record task will be started for this shard.
    /// You can then take the `LogfileCollectionWriter` from `Shards`
    /// and write to it manually.
    #[builder(try_setter, setter, default = "Self::default_spec()")]
    pub(crate) spec: GlobSet,
    /// how often to poll the resolver for structure changes. None
    /// means only once at startup.
    #[builder(default = "file::default_poll_interval()")]
    pub(crate) poll_interval: Option<Duration>,
    /// how often to write a full image. None means never write
    /// images. Ignored if spec is empty.
    #[builder(default = "file::default_image_frequency()")]
    pub(crate) image_frequency: Option<usize>,
    /// flush the file after the specified number of pages have been
    /// written. None means never flush. Ignored if spec is empty.
    #[builder(default = "file::default_flush_frequency()")]
    pub(crate) flush_frequency: Option<usize>,
    /// flush the file after the specified elapsed time. None means
    /// flush only on shutdown. Ignored if spec is empty.
    #[builder(default = "file::default_flush_interval()")]
    pub(crate) flush_interval: Option<Duration>,
    /// rotate the log file at the specified interval or file size or
    /// never. Ignored if spec is empty.
    #[builder(default = "file::default_rotate_interval()")]
    pub(crate) rotate_interval: RotateDirective,
    /// how much channel slack to allocate. Ignored if spec is empty.
    #[builder(default = "file::default_slack()")]
    pub(crate) slack: usize,
}

impl RecordConfigBuilder {
    fn default_spec() -> GlobSet {
        GlobSet::new(true, [Glob::new("/**".into()).unwrap()]).unwrap()
    }
}

impl RecordConfig {
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
                slack,
            } = c;
            let res = RecordConfig {
                spec: GlobSet::new(
                    true,
                    spec.into_iter().map(Glob::new).collect::<Result<Vec<_>>>()?,
                )?,
                poll_interval: poll_interval.or(f.poll_interval),
                image_frequency: image_frequency.or(f.image_frequency),
                flush_frequency: flush_frequency.or(f.flush_frequency),
                flush_interval: flush_interval.or(f.flush_interval),
                rotate_interval: rotate_interval.unwrap_or(f.rotate_interval),
                slack,
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
#[derive(Debug, Clone, Builder)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct Config {
    /// The directory where the archive files live. The current
    /// archive will be called 'current', the path mappings will be
    /// called 'pathmap', and previous rotated archive files will be
    /// named the rfc3339 timestamp that specifies when they were
    /// rotated (and thus when they ended).
    #[builder(setter(into))]
    pub(crate) archive_directory: PathBuf,
    #[builder(setter(strip_option), default)]
    pub(crate) archive_cmds: Option<ArchiveCmds>,
    /// The netidx config to use
    #[builder(setter(strip_option), default)]
    pub(crate) netidx_config: Option<NetIdxCfg>,
    /// The netidx desired authentication mechanism to use
    #[builder(default = "self.default_desired_auth()")]
    pub(crate) desired_auth: DesiredAuth,
    /// Record. Each entry in the HashMap is a shard, which will
    /// record independently to an archive directory under the base
    /// directory. E.G. a shard named "0" will record under
    /// ${archive_base}/0. If publish is specified all configured
    /// shards on this instance will be published.
    #[builder(setter(into), default)]
    pub(crate) record: HashMap<ArcStr, RecordConfig>,
    /// If specified this recorder will publish the archive
    /// directory. It is possible for the same archiver to both record
    /// and publish. One of record or publish must be specifed.
    #[builder(setter(strip_option), default)]
    pub(crate) publish: Option<PublishConfig>,
}

impl ConfigBuilder {
    fn validate(&self) -> std::result::Result<(), String> {
        let record_empty = self.record.as_ref().map(|t| t.is_empty()).unwrap_or(true);
        let publish_empty = self.publish.is_none();
        if record_empty && publish_empty {
            return Err("config must specify at least record, publish, or both".into());
        }
        Ok(())
    }

    fn default_desired_auth(&self) -> DesiredAuth {
        self.netidx_config
            .as_ref()
            .and_then(|n| n.as_ref())
            .map(|c| c.default_auth())
            .unwrap_or(DesiredAuth::Local)
    }
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
            netidx_config: Some(netidx_config),
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

    pub fn archive_directory(&self) -> &FilePath {
        &self.archive_directory
    }
}
