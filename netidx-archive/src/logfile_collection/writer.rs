use crate::{
    config::{ArchiveCmds, Config},
    logfile::{ArchiveReader, ArchiveWriter, BatchItem, Id},
};
use anyhow::{Context, Result};
use arcstr::ArcStr;
use chrono::prelude::*;
use log::{debug, info, warn};
use netidx::{path::Path, pool::Pooled};
use std::{
    fs, iter,
    path::{Path as FilePath, PathBuf},
    sync::Arc,
};

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
    shard_name: ArcStr,
    base: PathBuf,
    current_path: PathBuf,
    external_lock: Option<(PathBuf, PathBuf)>,
    current: Option<ArchiveWriter>,
    pathindex: ArchiveWriter,
}

impl ArchiveCollectionWriter {
    fn open_full<F: AsRef<FilePath>>(
        config: Arc<Config>,
        shard_name: ArcStr,
        external_lock: Option<(F, F)>,
    ) -> Result<Self> {
        let base = config.archive_directory.join(shard_name.as_str());
        let current_path = base.join("current");
        let pathindex_path = base.join("pathindex");
        fs::create_dir_all(&base)?;
        let external_lock = external_lock.map(|(clock, plock)| {
            (PathBuf::from(clock.as_ref()), PathBuf::from(plock.as_ref()))
        });
        let (current, pathindex) = if let Some((clock, plock)) = external_lock.as_ref() {
            let current = ArchiveWriter::open_external(&current_path, clock)?;
            let pathindex = ArchiveWriter::open_external(&pathindex_path, plock)?;
            (current, pathindex)
        } else {
            (ArchiveWriter::open(&current_path)?, ArchiveWriter::open(&pathindex_path)?)
        };
        Ok(Self {
            config,
            shard_name,
            base,
            current_path,
            external_lock,
            current: Some(current),
            pathindex,
        })
    }

    pub fn open(config: Arc<Config>, shard_name: ArcStr) -> Result<Self> {
        Self::open_full(config, shard_name, None::<(&FilePath, &FilePath)>)
    }

    /// Open the logfile collection with an external lock file for
    /// both the current file and the path index. This will allow an
    /// external process to write an archive file while another
    /// process reads and/or publishes it.
    ///
    /// This could be dangerous, it is up to the caller to coordinate
    /// the lock file name among writer processes to prevent multiple
    /// concurrent writers.
    pub fn open_external<F: AsRef<FilePath>>(
        config: Arc<Config>,
        shard_name: ArcStr,
        external_lock: (F, F),
    ) -> Result<Self> {
        Self::open_full(config, shard_name, Some(external_lock))
    }

    fn current_mut(&mut self) -> Result<&mut ArchiveWriter> {
        self.current.as_mut().ok_or_else(|| anyhow!("missing current, did rotate fail?"))
    }

    fn current(&self) -> Result<&ArchiveWriter> {
        self.current.as_ref().ok_or_else(|| anyhow!("missing current, did rotate fail?"))
    }

    pub fn flush(&mut self) -> Result<()> {
        self.current_mut()?.flush()?;
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
    /// running any configured post rotate commands.
    pub fn rotate(&mut self, now: DateTime<Utc>) -> Result<()> {
        info!("rotating log file {}", now);
        drop(self.current.take());
        let now_str = now.to_rfc3339();
        fs::rename(&self.current_path, &self.base.join(&now_str))
            .context("renaming current")?;
        put_file(&self.config.archive_cmds, &self.shard_name, &now_str)?;
        self.current = Some(match self.external_lock.as_ref() {
            Some((clock, _)) => ArchiveWriter::open_external(&self.current_path, clock)?,
            None => ArchiveWriter::open(&self.current_path)?,
        });
        Ok(())
    }
}
