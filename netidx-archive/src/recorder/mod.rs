use crate::{
    config::Config,
    logfile::{ArchiveReader, BatchItem},
    logfile_collection::{self, ArchiveCollectionWriter, ArchiveIndex},
};
use anyhow::{Context, Error, Result};
use arcstr::ArcStr;
use chrono::prelude::*;
use fxhash::FxHashMap;
use log::error;
use netidx::{
    protocol::value::FromValue,
    publisher::{Publisher, PublisherBuilder, Value},
    resolver_client::GlobSet,
    subscriber::Subscriber,
};
use netidx_core::atomic_id;
use netidx_derive::Pack;
use parking_lot::{Mutex, RwLock};
use poolshark::global::GPooled;
use serde_derive::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr, sync::Arc};
use tokio::{sync::broadcast, task::JoinSet};

mod oneshot;
mod publish;
mod record;

atomic_id!(ShardId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Pack)]
#[repr(u8)]
pub enum State {
    Play = 0,
    Pause = 1,
    Tail = 2,
}

impl FromStr for State {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        if s.as_str() == "play" {
            Ok(State::Play)
        } else if s.as_str() == "pause" {
            Ok(State::Pause)
        } else if s.as_str() == "tail" {
            Ok(State::Tail)
        } else {
            bail!("expected state [play, pause, tail]")
        }
    }
}

impl Into<Value> for State {
    fn into(self) -> Value {
        match self {
            Self::Play => "play",
            Self::Pause => "pause",
            Self::Tail => "tail",
        }
        .into()
    }
}

impl FromValue for State {
    fn from_value(v: Value) -> Result<Self> {
        Ok(v.get_as::<ArcStr>()
            .ok_or_else(|| anyhow!("state expected string"))?
            .parse::<State>()?)
    }

    fn get(_: Value) -> Option<Self> {
        None
    }
}

impl State {
    fn play(&self) -> bool {
        match self {
            State::Play => true,
            State::Pause | State::Tail => false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum BCastMsg {
    LogRotated(DateTime<Utc>),
    NewCurrent(ArchiveReader),
    Batch(DateTime<Utc>, Arc<GPooled<Vec<BatchItem>>>),
    TailInvalidated,
}

pub struct Shards {
    config: Arc<Config>,
    pub spec: FxHashMap<ShardId, GlobSet>,
    pub by_id: FxHashMap<ShardId, ArcStr>,
    pub by_name: FxHashMap<ArcStr, ShardId>,
    pub indexes: RwLock<FxHashMap<ShardId, ArchiveIndex>>,
    pub pathindexes: FxHashMap<ShardId, ArchiveReader>,
    pub heads: RwLock<FxHashMap<ShardId, ArchiveReader>>,
    pub bcast: FxHashMap<ShardId, broadcast::Sender<BCastMsg>>,
    pub writers: Mutex<FxHashMap<ShardId, ArchiveCollectionWriter>>,
}

impl Shards {
    fn read(config: &Arc<Config>) -> Result<Arc<Self>> {
        use std::fs;
        let mut t = Self {
            config: config.clone(),
            spec: HashMap::default(),
            by_id: HashMap::default(),
            by_name: HashMap::default(),
            indexes: RwLock::new(HashMap::default()),
            pathindexes: HashMap::default(),
            heads: RwLock::new(HashMap::default()),
            bcast: HashMap::default(),
            writers: Mutex::new(HashMap::default()),
        };
        for ent in fs::read_dir(&config.archive_directory)? {
            let ent = ent?;
            let name = ArcStr::from(ent.file_name().to_string_lossy());
            if ent.file_type()?.is_dir() && &name != ".." && &name != "." {
                let id = ShardId::new();
                t.indexes.write().insert(id, ArchiveIndex::new(&config, &name)?);
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
        Ok(Arc::new(t))
    }

    fn from_cfg(config: &Arc<Config>) -> Result<Arc<Self>> {
        let mut t = Self {
            config: config.clone(),
            spec: HashMap::default(),
            by_id: HashMap::default(),
            by_name: HashMap::default(),
            indexes: RwLock::new(HashMap::default()),
            pathindexes: HashMap::default(),
            heads: RwLock::new(HashMap::default()),
            bcast: HashMap::default(),
            writers: Mutex::new(HashMap::default()),
        };
        let mut writers = t.writers.lock();
        for (name, rcfg) in config.record.iter() {
            let id = ShardId::new();
            t.indexes.write().insert(id, ArchiveIndex::new(&config, &name)?);
            t.by_id.insert(id, name.clone());
            t.by_name.insert(name.clone(), id);
            t.spec.insert(id, rcfg.spec.clone());
            let writer = ArchiveCollectionWriter::open(config.clone(), name.clone())?;
            t.pathindexes.insert(id, writer.pathindex_reader()?);
            t.heads.write().insert(id, writer.current_reader()?);
            writers.insert(id, writer);
            let (tx, _) = broadcast::channel(1000);
            t.bcast.insert(id, tx);
        }
        drop(writers);
        Ok(Arc::new(t))
    }

    fn remap_rescan_pathindex(&self) -> Result<()> {
        for (_, reader) in self.pathindexes.iter() {
            reader.check_remap_rescan(true)?;
        }
        Ok(())
    }

    /// Reopen the specified archive file (None = current)
    pub fn reopen(&self, ts: Option<DateTime<Utc>>) -> Result<()> {
        self.remap_rescan_pathindex()?;
        match ts {
            Some(ts) => logfile_collection::reader::reopen(ts)?,
            None => {
                let mut heads = self.heads.write();
                for (_, reader) in heads.iter_mut() {
                    reader.reopen()?;
                }
            }
        }
        Ok(())
    }

    /// Remap/rescan the specified archive file (None = current)
    pub fn remap_rescan(&self, ts: Option<DateTime<Utc>>) -> Result<()> {
        self.remap_rescan_pathindex()?;
        match ts {
            Some(ts) => logfile_collection::reader::remap_rescan(ts)?,
            None => {
                let heads = self.heads.read();
                for (_, reader) in heads.iter() {
                    reader.check_remap_rescan(true)?;
                }
                for tx in self.bcast.values() {
                    tx.send(BCastMsg::TailInvalidated)?;
                }
            }
        }
        Ok(())
    }

    pub fn reindex(&self, config: &Arc<Config>) -> Result<()> {
        self.remap_rescan_pathindex()?;
        let mut indexes = self.indexes.write();
        for (shard, logfile_index) in indexes.iter_mut() {
            let name = self.by_id.get(shard).unwrap();
            *logfile_index = ArchiveIndex::new(config, name)?;
        }
        Ok(())
    }

    pub fn notify_rotated(
        &self,
        id: ShardId,
        now: DateTime<Utc>,
        reader: ArchiveReader,
    ) -> Result<()> {
        self.heads.write().insert(id, reader.clone());
        let name = &self.by_id.get(&id).ok_or_else(|| anyhow!("missing shard id"))?;
        let index =
            ArchiveIndex::new(&self.config, name).context("opening logfile index")?;
        self.indexes.write().insert(id, index);
        let bcast = &self.bcast[&id];
        let _ = bcast.send(BCastMsg::LogRotated(now));
        let _ = bcast.send(BCastMsg::NewCurrent(reader));
        Ok(())
    }
}

pub struct Recorder {
    wait: JoinSet<()>,
    pub config: Arc<Config>,
    pub shards: Arc<Shards>,
}

impl Recorder {
    async fn start_jobs(
        &mut self,
        publisher: Option<Publisher>,
        subscriber: Option<Subscriber>,
    ) -> Result<()> {
        let config = self.config.clone();
        let shared_subscriber = match &subscriber {
            Some(subscriber) => subscriber.clone(),
            None => Subscriber::new(
                config
                    .netidx_config
                    .as_ref()
                    .ok_or_else(|| anyhow!("netidx config required"))?
                    .clone(),
                config.desired_auth.clone(),
            )?,
        };
        if let Some(publish_config) = config.publish.as_ref() {
            let publish_config = Arc::new(publish_config.clone());
            let publisher = match publisher {
                Some(publisher) => publisher,
                None => {
                    PublisherBuilder::new(
                        config
                            .netidx_config
                            .as_ref()
                            .ok_or_else(|| anyhow!("netidx config is required"))?
                            .clone(),
                    )
                    .desired_auth(config.desired_auth.clone())
                    .bind_cfg(Some(publish_config.bind.clone()))
                    .build()
                    .await?
                }
            };
            let (tx_init, rx_init) = futures::channel::oneshot::channel();
            self.wait.spawn({
                let shards = self.shards.clone();
                let publish_config = publish_config.clone();
                let subscriber = shared_subscriber.clone();
                let config = config.clone();
                let publisher = publisher.clone();
                async move {
                    let r = publish::run(
                        shards,
                        subscriber,
                        config,
                        publish_config,
                        publisher,
                        tx_init,
                    )
                    .await;
                    if let Err(e) = r {
                        error!("publisher stopped on error {}", e)
                    }
                }
            });
            rx_init.await.map_err(|_| anyhow!("publisher init failed"))?;
            let (tx_init, rx_init) = futures::channel::oneshot::channel();
            self.wait.spawn({
                let subscriber = shared_subscriber.clone();
                let shards = self.shards.clone();
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
                        tx_init,
                    )
                    .await;
                    if let Err(e) = r {
                        error!("publisher oneshot stopped on error {}", e)
                    }
                }
            });
            rx_init.await.map_err(|_| anyhow!("oneshot init failed"))?;
        }
        for (name, cfg) in config.record.iter() {
            let name = name.clone();
            let id = self.shards.by_name[&name];
            if !self.shards.spec[&id].is_empty() {
                let subscriber = match &subscriber {
                    Some(subscriber) => subscriber.clone(),
                    None => Subscriber::new(
                        config
                            .netidx_config
                            .as_ref()
                            .ok_or_else(|| anyhow!("netidx config required"))?
                            .clone(),
                        config.desired_auth.clone(),
                    )?,
                };
                let record_config = Arc::new(cfg.clone());
                let shards = self.shards.clone();
                self.wait.spawn(async move {
                    let r =
                        record::run(shards, subscriber, record_config, id, name).await;
                    if let Err(e) = r {
                        error!("recorder stopped on error {:?}", e);
                    }
                });
            }
        }
        Ok(())
    }

    /// Start the recorder with an existing publisher and
    /// subscriber. If subscriber is specified then it will be used by
    /// all record shards. If subscriber is not specified then each
    /// record shard will create it's own subscriber, this has been
    /// observed to perform better under very high load.
    ///
    /// When this future is ready it is guaranteed that if the config
    /// specifies a publisher, then the publisher interfaces are fully
    /// published.
    ///
    /// If both publisher and subscriber are specified then config is
    /// not required to contain the netidx config
    pub async fn start_with(
        config: Config,
        publisher: Option<Publisher>,
        subscriber: Option<Subscriber>,
    ) -> Result<Self> {
        let config = Arc::new(config);
        let shards = if config.record.is_empty() {
            Shards::read(&config)?
        } else {
            Shards::from_cfg(&config)?
        };
        let mut t = Self { wait: JoinSet::new(), config, shards };
        t.start_jobs(publisher, subscriber).await?;
        Ok(t)
    }

    /// Start the recorder. When this future is ready it is guaranteed
    /// that if the config specifies a publisher, then the publisher
    /// interfaces are fully published.
    pub async fn start(config: Config) -> Result<Self> {
        Self::start_with(config, None, None).await
    }
}
