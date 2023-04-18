use crate::{
    logfile::{ArchiveReader, BatchItem, Cursor, Id, Seek},
    recorder::{
        logfile_index::{File, LogfileIndex},
        Config, PublishConfig,
    },
};
use anyhow::{Error, Result};
use arcstr::ArcStr;
use chrono::prelude::*;
use futures::{channel::mpsc, future, prelude::*, select_biased};
use fxhash::{FxHashMap, FxHashSet};
use log::{error, info, warn};
use netidx::{
    chars::Chars,
    path::Path,
    pool::Pooled,
    protocol::value::FromValue,
    publisher::{
        self, ClId, PublishFlags, Publisher, PublisherBuilder, UpdateBatch, Val, Value,
        WriteRequest,
    },
    resolver_client::{Glob, GlobSet},
    subscriber::{Event, Subscriber},
};
use netidx_protocols::rpc::server::{RpcCall, RpcReply};
use netidx_protocols::{
    cluster::{uuid_string, Cluster},
    define_rpc,
    rpc::server::{ArgSpec, Proc},
    rpc_err,
};
use parking_lot::Mutex;
use serde_derive::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem,
    ops::Bound,
    pin::Pin,
    str::FromStr,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::broadcast, task, time};
use uuid::Uuid;

struct DataSource {
    file: File,
    archive: ArchiveReader,
    cursor: Cursor,
}

impl DataSource {
    fn new(
        config: &Config,
        file: File,
        head: &Option<ArchiveReader>,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Result<Option<Self>> {
        match file {
            File::Head => match head.as_ref() {
                None => Ok(None),
                Some(head) => {
                    let mut cursor = Cursor::new();
                    cursor.set_start(start);
                    cursor.set_end(end);
                    let head = head.clone();
                    Ok(Some(Self { file: File::Head, archive: head, cursor }))
                }
            },
            File::Historical(ts) => {
                if let Some(cmds) = &config.archive_cmds {
                    use std::{iter, process::Command};
                    let out = task::block_in_place(|| {
                        let now = ts.to_rfc3339();
                        Command::new(&cmds.get.0)
                            .args(cmds.get.1.iter().chain(iter::once(&now)))
                            .output()
                    });
                    match out {
                        Err(e) => warn!("get command failed {}", e),
                        Ok(out) => {
                            if out.stderr.len() > 0 {
                                warn!(
                                    "get command stderr {}",
                                    String::from_utf8_lossy(&out.stderr)
                                );
                            }
                        }
                    }
                }
                let path = file.path(&config.archive_directory);
                let archive = task::block_in_place(|| ArchiveReader::open(path))?;
                let mut cursor = Cursor::new();
                cursor.set_start(start);
                cursor.set_end(end);
                Ok(Some(Self { file, archive, cursor }))
            }
        }
    }
}

pub struct Image {
    pub idx: Pooled<Vec<(Id, Path)>>,
    pub img: Pooled<HashMap<Id, Event>>,
}

pub struct LogfileCollection {
    index: LogfileIndex,
    source: Option<DataSource>,
    head: Option<ArchiveReader>,
    start: Bound<DateTime<Utc>>,
    end: Bound<DateTime<Utc>>,
    config: Arc<Config>,
}

impl LogfileCollection {
    pub async fn new(
        config: Arc<Config>,
        head: Option<ArchiveReader>,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Result<Self> {
        let index = LogfileIndex::new(&config).await?;
        Ok(Self { index, source: None, head, start, end, config })
    }

    /// Attempt to open a source if we don't already have one, return true
    /// on success. If the source is already open, just return true
    fn source(&mut self) -> Result<bool> {
        if self.source.is_some() {
            Ok(true)
        } else {
            self.source = DataSource::new(
                &self.config,
                self.index.first(),
                &self.head,
                self.start,
                self.end,
            )?;
            Ok(self.source.is_some())
        }
    }

    /// move to the next source if possible. Return true if there is a valid source.
    /// if no source is currently open then the first source will be opened
    fn next_source(&mut self) -> Result<bool> {
        if self.source.is_none() {
            self.source()
        } else {
            match self.source.as_ref().unwrap().file {
                File::Head => Ok(true),
                f => {
                    self.source = DataSource::new(
                        &self.config,
                        self.index.next(f),
                        &self.head,
                        self.start,
                        self.end,
                    )?;
                    Ok(self.source.is_some())
                }
            }
        }
    }

    /// read a batch of deltas from the current data source, if it exists. If no data source can be opened
    /// then the outer result will be error. If reading the current data source fails then the inner result
    /// will be error.
    ///
    /// This function will automatically move to the next file in the collection as long as it's timestamp is
    /// within the bounds.
    pub fn read_deltas(
        &mut self,
        read_count: usize,
    ) -> Result<Result<Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>>> {
        if !self.source()? {
            bail!("no data source available")
        } else {
            let (file, batches) = task::block_in_place(|| {
                let ds = self.source.as_mut().unwrap();
                let archive = &ds.archive;
                let cursor = &mut ds.cursor;
                let file = ds.file;
                Ok::<_, anyhow::Error>((file, archive.read_deltas(cursor, read_count)?))
            })?;
            loop {
                if batches.front().is_some() {
                    break Ok(Ok(batches));
                } else {
                    match file {
                        File::Historical(_) => {
                            if !self.next_source()? {
                                bail!("no data source available")
                            }
                        }
                        File::Head => {
                            // this is the end. my only friend. the end.
                            break Ok(Ok(Pooled::orphan(VecDeque::new())));
                        }
                    }
                }
            }
        }
    }

    /// look up the path for a given id
    pub fn path_for_id(&self, id: &Id) -> Option<Path> {
        self.source.as_ref().and_then(|ds| ds.archive.path_for_id(id))
    }

    /// look up the position in the archive, if any
    pub fn position(&self) -> Option<&Cursor> {
        self.source.as_ref().map(|ds| &ds.cursor)
    }

    /// get a mutable reference to the current position if it exists
    pub fn position_mut(&mut self) -> Option<&mut Cursor> {
        self.source.as_mut().map(|ds| &mut ds.cursor)
    }

    /// set the head
    pub fn set_head(&mut self, head: ArchiveReader) {
        self.head = Some(head);
    }

    /// set the start bound
    pub fn set_start(&mut self, start: Bound<DateTime<Utc>>) {
        self.start = start;
        if let Some(ds) = self.source.as_mut() {
            ds.cursor.set_start(start);
        }
    }

    pub fn set_end(&mut self, end: Bound<DateTime<Utc>>) {
        self.end = end;
        if let Some(ds) = self.source.as_mut() {
            ds.cursor.set_end(end);
        }
    }

    /// reimage the file at the current cursor position, returning the path map and the image
    pub fn reimage(&mut self) -> Option<Result<Image>> {
        task::block_in_place(|| {
            self.source.as_mut().map(|ds| {
                let img = ds.archive.build_image(&ds.cursor)?;
                let idx = ds.archive.get_index();
                Ok(Image { idx, img })
            })
        })
    }

    /// tell the collection that the log file has been rotated
    pub async fn log_rotated(&mut self, ts: DateTime<Utc>) -> Result<()> {
        let files = LogfileIndex::new(&self.config).await?;
        if let Some(d) = self.source.as_mut() {
            if d.file == File::Head {
                d.file = File::Historical(ts);
            }
        }
        self.index = files;
        Ok(())
    }

    /// seek to the position in the archive collection specified by the
    /// seek instruction. After seeking you may need to reimage.
    pub fn seek(&mut self, seek: Seek) -> Result<()> {
        match seek {
            Seek::Beginning => {
                let file = self.index.first();
                match self.source.as_ref() {
                    Some(ds) if ds.file == file => (),
                    Some(_) | None => {
                        self.source = DataSource::new(
                            &self.config,
                            file,
                            &self.head,
                            self.start,
                            self.end,
                        )?;
                    }
                }
            }
            Seek::End => {
                let file = self.index.last();
                match self.source.as_ref() {
                    Some(ds) if ds.file == file => (),
                    Some(_) | None => {
                        self.source = DataSource::new(
                            &self.config,
                            File::Head,
                            &self.head,
                            self.start,
                            self.end,
                        )?;
                    }
                }
            }
            Seek::BatchRelative(i) => match self.source.as_ref() {
                None => {
                    let file = self.index.first();
                    self.source = DataSource::new(
                        &self.config,
                        file,
                        &self.head,
                        self.start,
                        self.end,
                    )?;
                }
                Some(ds) => {
                    let mut cursor = ds.cursor;
                    cursor.set_start(Bound::Unbounded);
                    cursor.set_end(Bound::Unbounded);
                    if !ds.archive.index().seek_steps(&mut cursor, i) {
                        if i < 0 {
                            let file = self.index.prev(ds.file);
                            if file != ds.file {
                                self.source = DataSource::new(
                                    &self.config,
                                    file,
                                    &self.head,
                                    self.start,
                                    self.end,
                                )?;
                            }
                        } else {
                            let file = self.index.next(ds.file);
                            if file != ds.file {
                                self.source = DataSource::new(
                                    &self.config,
                                    file,
                                    &self.head,
                                    self.start,
                                    self.end,
                                )?;
                            }
                        }
                    }
                }
            },
            Seek::TimeRelative(offset) => match self.source.as_ref() {
                None => {
                    let file = self.index.first();
                    self.source = DataSource::new(
                        &self.config,
                        file,
                        &self.head,
                        self.start,
                        self.end,
                    )?;
                }
                Some(ds) => {
                    let mut cursor = ds.cursor;
                    cursor.set_start(Bound::Unbounded);
                    cursor.set_end(Bound::Unbounded);
                    let (ok, ts) =
                        ds.archive.index().seek_time_relative(&mut cursor, offset);
                    if !ok {
                        let file = self.index.find(ts);
                        if ds.file != file {
                            self.source = DataSource::new(
                                &self.config,
                                file,
                                &self.head,
                                self.start,
                                self.end,
                            )?;
                        }
                    }
                }
            },
            Seek::Absolute(ts) => {
                let file = self.index.find(ts);
                let cur_ok = match self.source.as_ref() {
                    None => false,
                    Some(ds) => ds.file == file,
                };
                if !cur_ok {
                    self.source = DataSource::new(
                        &self.config,
                        file,
                        &self.head,
                        self.start,
                        self.end,
                    )?;
                }
            }
        }
        if let Some(ds) = self.source.as_mut() {
            ds.archive.seek(&mut ds.cursor, seek);
        }
        Ok(())
    }
}
