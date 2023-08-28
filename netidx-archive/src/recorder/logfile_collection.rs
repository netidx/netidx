use crate::{
    logfile::{ArchiveReader, BatchItem, Cursor, Id, Seek, IMG_POOL},
    recorder::{
        logfile_index::{File, LogfileIndex},
        Config,
    },
};
use anyhow::Result;
use arcstr::ArcStr;
use chrono::prelude::*;
use fxhash::{FxHashMap, FxHashSet};
use log::{debug, error, info, warn};
use netidx::{pool::Pooled, subscriber::Event};
use parking_lot::Mutex;
use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    ops::Bound,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::task;

struct Cached {
    timestamp: DateTime<Utc>,
    last_used: DateTime<Utc>,
    reader: ArchiveReader
}

lazy_static! {
    static ref ARCHIVE_READERS: Mutex<FxHashMap<PathBuf, Cached>> =
        Mutex::new(HashMap::default());
    static ref CACHE_FOR: chrono::Duration = chrono::Duration::minutes(10);
}

pub fn reopen(timestamp: DateTime<Utc>) -> Result<()> {
    let mut readers = ARCHIVE_READERS.lock();
    for (_, cached) in readers.iter_mut() {
        if cached.timestamp == timestamp {
            cached.reader.reopen()?;
        }
    }
    Ok(())
}

pub fn remap_rescan(timestamp: DateTime<Utc>) -> Result<()> {
    let mut readers = ARCHIVE_READERS.lock();
    for (_, cached) in readers.iter_mut() {
        if cached.timestamp == timestamp {
            cached.reader.check_remap_rescan(true)?;
        }
    }
    Ok(())
}

struct DataSource {
    file: File,
    archive: ArchiveReader,
}

impl DataSource {
    fn get_file_from_external(
        path: &PathBuf,
        config: &Config,
        ts: DateTime<Utc>,
        shard: &str,
    ) {
        if !path.exists() {
            debug!("would run get, cmd config {:?}", &config.archive_cmds);
            if let Some(cmds) = &config.archive_cmds {
                use std::{iter, process::Command};
                info!("running get {:?}", &cmds.get);
                let out = task::block_in_place(|| {
                    let now = ts.to_rfc3339();
                    let args = cmds
                        .get
                        .1
                        .iter()
                        .cloned()
                        .map(|a| a.replace("{shard}", shard))
                        .chain(iter::once(now));
                    Command::new(&cmds.get.0).args(args).output()
                });
                match out {
                    Err(e) => warn!("failed to execute get command {}", e),
                    Ok(o) if !o.status.success() => {
                        warn!("get command failed {:?}", o)
                    }
                    Ok(out) => {
                        if out.stdout.len() > 0 {
                            let out = String::from_utf8_lossy(&out.stdout);
                            warn!("get command stdout {}", out)
                        }
                        if out.stderr.len() > 0 {
                            let out = String::from_utf8_lossy(&out.stderr);
                            warn!("get command stderr {}", out);
                        }
                        info!("get command succeeded");
                    }
                }
            }
        }
    }

    fn new(
        config: &Config,
        shard: &str,
        file: File,
        head: &Option<ArchiveReader>,
    ) -> Result<Option<Self>> {
        match file {
            File::Head => match head.as_ref() {
                None => Ok(None),
                Some(head) => {
                    let head = head.clone();
                    Ok(Some(Self { file: File::Head, archive: head }))
                }
            },
            File::Historical(ts) => {
                let path = file.path(&config.archive_directory, shard);
                let now = Utc::now();
                debug!("opening log file {:?}", &path);
                let mut readers = ARCHIVE_READERS.lock();
                match readers.get_mut(&path) {
                    Some(cached) => {
                        debug!("log file was cached");
                        cached.last_used = now;
                        Ok(Some(Self { file, archive: cached.reader.clone() }))
                    }
                    None => {
                        debug!("log file was not cached, opening");
                        readers.retain(|_, cached| {
                            cached.reader.strong_count() > 1 ||
                                now - cached.last_used < *CACHE_FOR
                        });
                        drop(readers); // release the lock
                        let rd = task::block_in_place(|| {
                            for _ in 0..3 {
                                Self::get_file_from_external(&path, config, ts, shard);
                                match ArchiveReader::open(&path) {
                                    Ok(rd) => return Ok::<_, anyhow::Error>(rd),
                                    Err(e) => {
                                        error!("could not open archive file {}", e);
                                        std::thread::sleep(Duration::from_secs(1));
                                    }
                                }
                            }
                            bail!("could not open log file")
                        })?;
                        let archive = match ARCHIVE_READERS.lock().entry(path) {
                            Entry::Vacant(e) => {
                                e.insert(Cached {
                                    timestamp: ts,
                                    last_used: now,
                                    reader: rd.clone()
                                });
                                rd
                            },
                            Entry::Occupied(e) => e.get().reader.clone(),
                        };
                        Ok(Some(Self { file, archive }))
                    }
                }
            }
        }
    }
}

pub struct LogfileCollection {
    index: LogfileIndex,
    source: Option<DataSource>,
    head: Option<ArchiveReader>,
    pos: Cursor,
    config: Arc<Config>,
    shard: ArcStr,
}

impl Drop for LogfileCollection {
    fn drop(&mut self) {
        self.source = None;
        self.head = None;
        let now = Utc::now();
        ARCHIVE_READERS
            .lock()
            .retain(|_, cached| cached.reader.strong_count() > 1
                || now - cached.last_used < *CACHE_FOR);
    }
}

impl LogfileCollection {
    pub fn new(
        index: LogfileIndex,
        config: Arc<Config>,
        shard: ArcStr,
        head: Option<ArchiveReader>,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Self {
        let pos = Cursor::create_from(start, end, None);
        Self { index, source: None, head, pos, config, shard }
    }

    /// Attempt to open a source if we don't already have one, return true
    /// on success. If the source is already open, just return true
    fn source(&mut self) -> Result<bool> {
        if self.source.is_some() {
            Ok(true)
        } else {
            self.source = DataSource::new(
                &self.config,
                &self.shard,
                self.index.first(),
                &self.head,
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
                        &self.shard,
                        self.index.next(f),
                        &self.head,
                    )?;
                    Ok(self.source.is_some())
                }
            }
        }
    }

    fn apply_read<
        R: 'static,
        F: FnMut(&ArchiveReader, &mut Cursor) -> Result<R>,
        S: Fn(&R) -> bool,
    >(
        &mut self,
        mut f: F,
        s: S,
        empty: R,
    ) -> Result<R> {
        if !self.source()? {
            bail!("no data source available")
        } else {
            loop {
                let (file, result) = task::block_in_place(|| {
                    let ds = self.source.as_mut().unwrap();
                    let archive = &ds.archive;
                    let file = ds.file;
                    let result = f(archive, &mut self.pos)?;
                    Ok::<_, anyhow::Error>((file, result))
                })?;
                error!("read file {:?} at pos {:?}", file, self.pos);
                if s(&result) {
                    error!("OK");
                    break Ok(result);
                } else {
                    error!("errrrr");
                    match file {
                        File::Head => break Ok(empty),
                        File::Historical(_) => {
                            if !self.next_source()? {
                                bail!("no data source available")
                            }
                        }
                    }
                }
            }
        }
    }

    /// read a batch of deltas from the current data source, if it
    /// exists. If no data source can be opened then the outer result
    /// will be error. If reading the current data source fails then
    /// the inner result will be error.
    ///
    /// This function will automatically move to the next file in the
    /// collection as long as it's timestamp is within the bounds.
    pub fn read_deltas(
        &mut self,
        filter: Option<&FxHashSet<Id>>,
        read_count: usize,
    ) -> Result<(usize, Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>)> {
        self.apply_read(
            |archive, cursor| archive.read_deltas(filter, cursor, read_count),
            |(_, batch)| !batch.is_empty(),
            (0, Pooled::orphan(VecDeque::new())),
        )
    }

    /// read the next batch after the current cursor position, moving
    /// to the next file if necessary.
    pub fn read_next(
        &mut self,
        filter: Option<&FxHashSet<Id>>,
    ) -> Result<Option<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>> {
        self.apply_read(
            |archive, cursor| archive.read_next(filter, cursor),
            |batch| batch.is_some(),
            None,
        )
    }

    /// look up the position in the archive, if any
    pub fn position(&self) -> &Cursor {
        &self.pos
    }

    /// get a mutable reference to the current position if it exists
    pub fn position_mut(&mut self) -> &mut Cursor {
        &mut self.pos
    }

    /// set the head
    pub fn set_head(&mut self, head: ArchiveReader) {
        self.head = Some(head);
    }

    /// set the start bound
    pub fn set_start(&mut self, start: Bound<DateTime<Utc>>) {
        self.pos.set_start(start);
    }

    pub fn set_end(&mut self, end: Bound<DateTime<Utc>>) {
        self.pos.set_end(end);
    }

    pub fn start(&self) -> &Bound<DateTime<Utc>> {
        self.pos.start()
    }

    pub fn end(&self) -> &Bound<DateTime<Utc>> {
        self.pos.end()
    }

    /// reimage the file at the current cursor position, returning the path map and the image
    pub fn reimage(
        &mut self,
        filter: Option<&FxHashSet<Id>>,
    ) -> Result<Pooled<FxHashMap<Id, Event>>> {
        if self.source()? {
            task::block_in_place(|| {
                let ds = self.source.as_mut().unwrap();
                ds.archive.build_image(filter, &self.pos)
            })
        } else {
            Ok(IMG_POOL.take())
        }
    }

    /// tell the collection that the log file has been rotated
    pub fn log_rotated(&mut self, ts: DateTime<Utc>, index: LogfileIndex) {
        if let Some(d) = self.source.as_mut() {
            if d.file == File::Head {
                d.file = File::Historical(ts);
            }
        }
        self.index = index;
    }

    /// seek n batches forward if n is positive or backward if n is negative
    pub fn seek_n(&mut self, mut n: i8) -> Result<()> {
        self.source()?;
        loop {
            match self.source.as_ref() {
                None => break,
                Some(ds) => {
                    let moved = ds.archive.index().seek_steps(&mut self.pos, n);
                    if moved == n {
                        break;
                    }
                    let file = if n < 0 {
                        if self.pos.at_start() {
                            break;
                        } else {
                            self.index.prev(ds.file)
                        }
                    } else {
                        if self.pos.at_end() {
                            break;
                        } else {
                            self.index.next(ds.file)
                        }
                    };
                    if file == ds.file {
                        break;
                    }
                    self.source =
                        DataSource::new(&self.config, &self.shard, file, &self.head)?;
                    n -= moved;
                }
            }
        }
        Ok(())
    }

    /// seek to the position in the archive collection specified by the
    /// seek instruction. After seeking you may need to reimage.
    pub fn seek(&mut self, seek: Seek) -> Result<()> {
        match seek {
            Seek::BatchRelative(i) => self.seek_n(i)?,
            Seek::Beginning => {
                let file = match &self.pos.start() {
                    Bound::Unbounded => self.index.first(),
                    Bound::Excluded(dt) | Bound::Included(dt) => self.index.find(*dt),
                };
                match self.source.as_ref() {
                    Some(ds) if ds.file == file => (),
                    Some(_) | None => {
                        self.source =
                            DataSource::new(&self.config, &self.shard, file, &self.head)?;
                    }
                }
                if let Some(ds) = self.source.as_ref() {
                    ds.archive.seek(&mut self.pos, Seek::Beginning)
                }
            }
            Seek::End => {
                let file = match &self.pos.end() {
                    Bound::Unbounded => self.index.last(),
                    Bound::Excluded(dt) | Bound::Included(dt) => self.index.find(*dt),
                };
                match self.source.as_ref() {
                    Some(ds) if ds.file == file => (),
                    Some(_) | None => {
                        self.source = DataSource::new(
                            &self.config,
                            &self.shard,
                            File::Head,
                            &self.head,
                        )?;
                    }
                }
                if let Some(ds) = self.source.as_ref() {
                    ds.archive.seek(&mut self.pos, Seek::End)
                }
            }
            Seek::TimeRelative(offset) => {
                if self.source.is_none() {
                    self.source()?;
                }
                if let Some(ds) = self.source.as_ref() {
                    let (ok, ts) =
                        ds.archive.index().seek_time_relative(&mut self.pos, offset);
                    if !ok {
                        let file = self.index.find(ts);
                        if ds.file != file {
                            self.source = DataSource::new(
                                &self.config,
                                &self.shard,
                                file,
                                &self.head,
                            )?;
                        }
                        if let Some(ds) = self.source.as_ref() {
                            ds.archive.seek(&mut self.pos, Seek::Absolute(ts));
                        }
                    }
                }
            }
            Seek::Absolute(ts) => {
                let file = self.index.find(ts);
                let cur_ok = match self.source.as_ref() {
                    None => false,
                    Some(ds) => ds.file == file,
                };
                if !cur_ok {
                    self.source =
                        DataSource::new(&self.config, &self.shard, file, &self.head)?;
                }
                if let Some(ds) = self.source.as_ref() {
                    ds.archive.seek(&mut self.pos, Seek::Absolute(ts))
                }
            }
        }
        Ok(())
    }
}
