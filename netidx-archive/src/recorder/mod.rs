use crate::{
    config::{Config, ArchiveCmds},
    logfile::{ArchiveReader, ArchiveWriter, BatchItem},
    logfile_collection::{self, index::ArchiveIndex},
};
use anyhow::Result;
use arcstr::ArcStr;
use chrono::prelude::*;
use fxhash::FxHashMap;
use log::{debug, error, info, warn};
use netidx::{
    pool::Pooled, publisher::PublisherBuilder, resolver_client::GlobSet,
    subscriber::Subscriber,
};
use netidx_core::atomic_id;
use parking_lot::RwLock;
use serde_derive::{Deserialize, Serialize};
use std::{collections::HashMap, iter, sync::Arc};
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
    spec: FxHashMap<ShardId, GlobSet>,
    by_id: FxHashMap<ShardId, ArcStr>,
    by_name: FxHashMap<ArcStr, ShardId>,
    indexes: RwLock<FxHashMap<ShardId, ArchiveIndex>>,
    pathindexes: FxHashMap<ShardId, ArchiveReader>,
    heads: RwLock<FxHashMap<ShardId, ArchiveReader>>,
    bcast: FxHashMap<ShardId, broadcast::Sender<BCastMsg>>,
}

impl Shards {
    pub fn spec(&self) -> &FxHashMap<ShardId, GlobSet> {
        &self.spec
    }

    pub fn by_id(&self) -> &FxHashMap<ShardId, ArcStr> {
        &self.by_id
    }

    pub fn by_name(&self) -> &FxHashMap<ArcStr, ShardId> {
        &self.by_name
    }

    pub fn indexes(&self) -> &RwLock<FxHashMap<ShardId, ArchiveIndex>> {
        &self.indexes
    }

    pub fn pathindexes(&self) -> &FxHashMap<ShardId, ArchiveReader> {
        &self.pathindexes
    }
    
    pub fn heads(&self) -> &RwLock<FxHashMap<ShardId, ArchiveReader>> {
        &self.heads
    }

    pub fn bcast(&self) -> &FxHashMap<ShardId, broadcast::Sender<BCastMsg>> {
        &self.bcast
    }

    fn read(
        config: &Arc<Config>,
    ) -> Result<(FxHashMap<ShardId, ArchiveWriter>, Arc<Self>)> {
        use std::fs;
        let mut t = Self {
            spec: HashMap::default(),
            by_id: HashMap::default(),
            by_name: HashMap::default(),
            indexes: RwLock::new(HashMap::default()),
            pathindexes: HashMap::default(),
            heads: RwLock::new(HashMap::default()),
            bcast: HashMap::default(),
        };
        for ent in fs::read_dir(&config.archive_directory)? {
            let ent = ent?;
            let name = ArcStr::from(ent.file_name().to_string_lossy());
            if ent.file_type()?.is_dir() && &name != ".." {
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
        Ok((HashMap::default(), Arc::new(t)))
    }

    fn from_cfg(
        config: &Arc<Config>,
    ) -> Result<(FxHashMap<ShardId, ArchiveWriter>, Arc<Self>)> {
        use std::fs;
        let mut t = Self {
            spec: HashMap::default(),
            by_id: HashMap::default(),
            by_name: HashMap::default(),
            indexes: RwLock::new(HashMap::default()),
            pathindexes: HashMap::default(),
            heads: RwLock::new(HashMap::default()),
            bcast: HashMap::default(),
        };
        let mut writers = HashMap::default();
        for (name, rcfg) in config.record.iter() {
            let id = ShardId::new();
            t.indexes.write().insert(id, ArchiveIndex::new(&config, &name)?);
            t.by_id.insert(id, name.clone());
            t.by_name.insert(name.clone(), id);
            t.spec.insert(id, rcfg.spec.clone());
            let dir = config.archive_directory.join(&**name);
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
}

pub struct Recorder {
    wait: JoinSet<()>,
    pub config: Arc<Config>,
    pub shards: Arc<Shards>
}

impl Recorder {
    async fn start_jobs(&mut self, mut writers: FxHashMap<ShardId, ArchiveWriter>) -> Result<()> {
        let config = self.config.clone();
        let subscriber =
            Subscriber::new(config.netidx_config.clone(), config.desired_auth.clone())?;
        if let Some(publish_config) = config.publish.as_ref() {
            let publish_config = Arc::new(publish_config.clone());
            let publisher = PublisherBuilder::new(config.netidx_config.clone())
                .desired_auth(config.desired_auth.clone())
                .bind_cfg(Some(publish_config.bind.clone()))
                .build()
                .await?;
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
            let pathindex_writer = writers.remove(&id).unwrap();
            let record_config = Arc::new(cfg.clone());
            let subscriber = Subscriber::new(
                config.netidx_config.clone(),
                config.desired_auth.clone(),
            )?;
            let config = config.clone();
            let shards = self.shards.clone();
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
                    error!("recorder stopped on error {:?}", e);
                }
            });
        }
        Ok(())
    }

    /// Start the recorder
    pub async fn start(config: Config) -> Result<Self> {
        let config = Arc::new(config);
        let (writers, shards) = if config.record.is_empty() {
            Shards::read(&config)?
        } else {
            Shards::from_cfg(&config)?
        };
        let mut t = Self { wait: JoinSet::new(), config, shards };
        t.start_jobs(writers).await?;
        Ok(t)
    }
}

/// Run archive PUT cmds on the given archive file
pub fn put_file(
    cmds: &Option<ArchiveCmds>,
    shard_name: &str,
    file_name: &str,
) -> Result<()> {
    debug!("would run put, cmd config {:?}", cmds);
    if let Some(cmds) = cmds {
        use std::process::Command;
        info!("running put {:?}", &cmds.put);
        let args =
            cmds.put.1.iter().cloned().map(|arg| arg.replace("{shard}", shard_name));
        let out = Command::new(&cmds.put.0)
            .args(args.chain(iter::once(file_name.to_string())))
            .output();
        match out {
            Err(e) => warn!("archive put failed for {}, {}", file_name, e),
            Ok(o) if !o.status.success() => {
                warn!("archive put failed for {}, {:?}", file_name, o)
            }
            Ok(out) => {
                if out.stdout.len() > 0 {
                    warn!("archive put stdout {}", String::from_utf8_lossy(&out.stdout));
                }
                if out.stderr.len() > 0 {
                    warn!("archive put stderr {}", String::from_utf8_lossy(&out.stderr));
                }
                info!("put completed successfully");
            }
        }
    }
    Ok(())
}
