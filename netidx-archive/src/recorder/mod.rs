use crate::{
    config::Config,
    logfile::{ArchiveReader, BatchItem},
    logfile_collection::{self, ArchiveCollectionWriter, ArchiveIndex},
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use chrono::prelude::*;
use fxhash::FxHashMap;
use log::error;
use netidx::{
    pool::Pooled,
    publisher::{Publisher, PublisherBuilder},
    resolver_client::GlobSet,
    subscriber::Subscriber,
};
use netidx_core::atomic_id;
use parking_lot::{Mutex, RwLock};
use serde_derive::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::broadcast, task::JoinSet};

mod oneshot;
mod publish;
mod record;

atomic_id!(ShardId);

#[derive(Debug, Clone)]
pub enum BCastMsg {
    LogRotated(DateTime<Utc>),
    NewCurrent(ArchiveReader),
    Batch(DateTime<Utc>, Arc<Pooled<Vec<BatchItem>>>),
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
        let index = ArchiveIndex::new(&self.config, name)
            .context("opening logfile index")?;
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
        let subscriber = match subscriber {
            Some(subscriber) => subscriber,
            None => Subscriber::new(
                config.netidx_config.clone(),
                config.desired_auth.clone(),
            )?,
        };
        if let Some(publish_config) = config.publish.as_ref() {
            let publish_config = Arc::new(publish_config.clone());
            let publisher = match publisher {
                Some(publisher) => publisher,
                None => {
                    PublisherBuilder::new(config.netidx_config.clone())
                        .desired_auth(config.desired_auth.clone())
                        .bind_cfg(Some(publish_config.bind.clone()))
                        .build()
                        .await?
                }
            };
            self.wait.spawn({
                let shards = self.shards.clone();
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
            let id = self.shards.by_name[&name];
            if !self.shards.spec[&id].is_empty() {
                let record_config = Arc::new(cfg.clone());
                let subscriber = Subscriber::new(
                    config.netidx_config.clone(),
                    config.desired_auth.clone(),
                )?;
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

    /// Start the recorder with an existing publisher and subscriber
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

    /// Start the recorder
    pub async fn start(config: Config) -> Result<Self> {
        Self::start_with(config, None, None).await
    }
}
