use crate::{
    config::{ArchiveCmds, Config},
    logfile::{ArchiveReader, ArchiveWriter, BatchItem, Id},
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use chrono::prelude::*;
use log::{debug, info, warn};
use netidx::{path::Path, pool::Pooled};
use std::{fs, iter, path::PathBuf, sync::Arc};

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

pub struct ArchiveCollectionWriter {
    config: Arc<Config>,
    shard: ArcStr,
    base: PathBuf,
    current_path: PathBuf,
    external_lock: bool,
    current: Option<ArchiveWriter>,
    pathindex: ArchiveWriter,
}

impl ArchiveCollectionWriter {
    const LOCK_FILE: &str = "current.lock";

    fn open_full(
        config: Arc<Config>,
        shard: ArcStr,
        external_lock: bool,
    ) -> Result<Self> {
        let base = config.archive_directory.join(shard.as_str());
        let current_path = base.join("current");
        let pathindex_path = base.join("pathindex");
        fs::create_dir_all(&base)?;
        let (current, pathindex) = if external_lock {
            let clock = base.join(Self::LOCK_FILE);
            let plock = base.join("pathindex.lock");
            let current = ArchiveWriter::open_external(&current_path, clock)?;
            let pathindex = ArchiveWriter::open_external(&pathindex_path, plock)?;
            (current, pathindex)
        } else {
            (ArchiveWriter::open(&current_path)?, ArchiveWriter::open(&pathindex_path)?)
        };
        Ok(Self {
            config,
            shard,
            base,
            current_path,
            external_lock,
            current: Some(current),
            pathindex,
        })
    }

    pub fn open(config: Arc<Config>, shard: ArcStr) -> Result<Self> {
        Self::open_full(config, shard, false)
    }

    /// Open the archive collection with an external lock. Use this if
    /// the reading and writing process are not the same. The current
    /// and pathindex files themselves will not be locked, instead
    /// external lock files will be created. This will allow one
    /// process to read and publish while another writes.
    ///
    /// It is important that you always open the same collection for
    /// writing with either `open` or `open_external` but not both,
    /// otherwise concurrent writes will likely corrupt the data in
    /// the log file.
    pub fn open_external(config: Arc<Config>, shard: ArcStr) -> Result<Self> {
        Self::open_full(config, shard, true)
    }

    fn current_mut(&mut self) -> Result<&mut ArchiveWriter> {
        self.current.as_mut().ok_or_else(|| anyhow!("missing current, did rotate fail?"))
    }

    fn current(&self) -> Result<&ArchiveWriter> {
        self.current.as_ref().ok_or_else(|| anyhow!("missing current, did rotate fail?"))
    }

    pub fn flush_all(&mut self) -> Result<()> {
        self.current_mut()?.flush()?;
        self.pathindex.flush()
    }

    pub fn flush_current(&mut self) -> Result<()> {
        self.current_mut()?.flush()
    }

    pub fn flush_pathindex(&mut self) -> Result<()> {
        self.pathindex.flush()
    }

    /// Write new path mappings to the pathmap file. See [ArchiveWriter::add_paths].
    pub fn add_paths<'a>(
        &'a mut self,
        paths: impl IntoIterator<Item = &'a Path>,
    ) -> Result<()> {
        self.pathindex.add_paths(paths)
    }

    /// Write a batch to the current file. See [ArchiveWriter::add_batch].
    pub fn add_batch(
        &mut self,
        image: bool,
        timestamp: DateTime<Utc>,
        batch: &Pooled<Vec<BatchItem>>,
    ) -> Result<()> {
        self.current_mut()?.add_batch(image, timestamp, batch)
    }

    pub fn id_for_path(&self, path: &Path) -> Option<Id> {
        self.pathindex.id_for_path(path)
    }

    pub fn path_for_id(&self, id: &Id) -> Option<&Path> {
        self.pathindex.path_for_id(id)
    }

    pub fn capacity(&self) -> Result<usize> {
        Ok(self.current()?.capacity())
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.current()?.len())
    }

    pub fn block_size(&self) -> Result<usize> {
        Ok(self.current()?.block_size())
    }

    /// Return the reader for the current archive file. See [ArchiveWriter::reader]
    pub fn current_reader(&self) -> Result<ArchiveReader> {
        self.current()?.reader()
    }

    /// Return the reader for the pathindex. See [ArchiveWriter::reader]
    pub fn pathindex_reader(&self) -> Result<ArchiveReader> {
        self.pathindex.reader()
    }

    /// Rotate the current log file, replacing it with a new one, and
    /// running any configured post rotate commands. Returns the path
    /// to the rotated file.
    ///
    /// now must be greater than or equal to the timestamp of the last
    /// record in current, and should ideally be equal to it.
    pub fn rotate(&mut self, now: DateTime<Utc>) -> Result<PathBuf> {
        info!("rotating log file {}", now);
        let now_str = now.to_rfc3339();
        let new_path = self.base.join(&now_str);
        drop(self.current.take());
        fs::rename(&self.current_path, &new_path).context("renaming current")?;
        put_file(&self.config.archive_cmds, &self.shard, &now_str)
            .context("running post rotate commands")?;
        self.current = Some(if self.external_lock {
            ArchiveWriter::open_external(
                &self.current_path,
                self.base.join(Self::LOCK_FILE),
            )
            .context("open_external current for write")?
        } else {
            ArchiveWriter::open(&self.current_path).context("open current for write")?
        });
        Ok(new_path)
    }

    /// Rotate and compress the current log file in one step,
    /// replacing it with a new one, and running any configured post
    /// rotate commands. Returns the path to the rotated file.
    ///
    /// now must be greater than or equal to the timestamp of the last
    /// record in current, and should ideally be equal to it.
    pub async fn rotate_and_compress(
        &mut self,
        now: DateTime<Utc>,
        window: Option<usize>,
    ) -> Result<PathBuf> {
        use tokio::{fs, task};
        info!("rotate and compress log file {}", now);
        let now_str = now.to_rfc3339();
        let new_path = self.base.join(&now_str);
        let reader = task::block_in_place(|| {
            drop(self.current.take());
            ArchiveReader::open(&self.current_path)
        })
        .context("open current for read")?;
        let window = window.unwrap_or(2);
        let tmp_path = new_path.with_extension("rz");
        let _ = fs::remove_file(&tmp_path).await;
        reader.compress(window, &tmp_path).await.context("compressing archive")?;
        fs::rename(&tmp_path, &new_path).await.context("moving tmp file into place")?;
        fs::remove_file(&self.current_path).await.context("removing old current")?;
        self.current = task::block_in_place(|| -> Result<Option<ArchiveWriter>> {
            put_file(&self.config.archive_cmds, &self.shard, &now_str)
                .context("running post rotate commands")?;
            Ok(Some(if self.external_lock {
                ArchiveWriter::open_external(
                    &self.current_path,
                    self.base.join(Self::LOCK_FILE),
                )
                .context("open_external current for write")?
            } else {
                ArchiveWriter::open(&self.current_path)
                    .context("open current for write")?
            }))
        })?;
        Ok(new_path)
    }
}
