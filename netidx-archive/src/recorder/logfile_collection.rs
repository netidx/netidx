use crate::{
    logfile::{ArchiveReader, BatchItem, Cursor, Id, Seek},
    recorder::{
        logfile_index::{File, LogfileIndex},
        BCastMsg, Config, PublishConfig,
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

pub struct LogfileCollection {
    index: LogfileIndex,
    source: Option<DataSource>,
    head: Option<ArchiveReader>,
    start: Bound<DateTime<Utc>>,
    end: Bound<DateTime<Utc>>,
    config: Arc<Config>,
}

impl LogfileCollection {
    pub fn new(
        config: Arc<Config>,
        head: Option<ArchiveReader>,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Result<Self> {
        unimplemented!()
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
            let ds = self.source.as_mut().unwrap();
            let archive = &ds.archive;
            let cursor = &mut ds.cursor;
            let batches = archive.read_deltas(cursor, read_count)?;
            loop {
                if batches.front().is_some() {
                    break Ok(Ok(batches));
                } else {
                    match ds.file {
                        File::Historical(_) => self.next_source()?,
                        File::Head => { 
                            // this is the end. my only friend. the end.
                            break Ok(Ok(Pooled::orphan(VecDeque::new()))) 
                        }
                    }
                }
            }
        }
    }
}
