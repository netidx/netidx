use crate::{
    logfile::{ArchiveReader, BatchItem, Cursor, Id, Seek, IMG_POOL},
    recorder::{
        logfile_index::{File, LogfileIndex},
        Config,
    },
};
use anyhow::Result;
use chrono::prelude::*;
use fxhash::FxHashMap;
use log::{debug, info, warn};
use netidx::{pool::Pooled, subscriber::Event};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, VecDeque},
    ops::Bound,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::task;

static RETAIN_TIME: Duration = Duration::from_secs(900);

lazy_static! {
    static ref ARCHIVE_READERS: Mutex<FxHashMap<PathBuf, (ArchiveReader, Instant)>> =
        Mutex::new(HashMap::default());
}

struct DataSource {
    file: File,
    archive: ArchiveReader,
}

impl DataSource {
    fn new(
        config: &Config,
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
                debug!("would run get, cmd config {:?}", &config.archive_cmds);
                if let Some(cmds) = &config.archive_cmds {
                    use std::{iter, process::Command};
                    info!("running get {:?}", &cmds.get);
                    let out = task::block_in_place(|| {
                        let now = ts.to_rfc3339();
                        Command::new(&cmds.get.0)
                            .args(cmds.get.1.iter().chain(iter::once(&now)))
                            .output()
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
                let path = file.path(&config.archive_directory);
                debug!("opening log file {:?}", &path);
                let archive = {
                    let mut readers = ARCHIVE_READERS.lock();
                    match readers.get_mut(&path) {
                        Some((reader, last)) => {
                            debug!("log file was cached");
                            *last = Instant::now();
                            reader.clone()
                        }
                        None => {
                            debug!("log file was not cached, opening");
                            let now = Instant::now();
                            readers.retain(|_, (r, last)| {
                                r.strong_count() > 1 || (now - *last) < RETAIN_TIME
                            });
                            let rd = task::block_in_place(|| ArchiveReader::open(&path))?;
                            readers.insert(path, (rd.clone(), Instant::now()));
                            debug!("log file opened successfully");
                            rd
                        }
                    }
                };
                Ok(Some(Self { file, archive }))
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
}

impl Drop for LogfileCollection {
    fn drop(&mut self) {
        self.source = None;
        self.head = None;
        let now = Instant::now();
        ARCHIVE_READERS
            .lock()
            .retain(|_, (r, last)| r.strong_count() > 1 || (now - *last) < RETAIN_TIME);
    }
}

impl LogfileCollection {
    pub async fn new(
        config: Arc<Config>,
        head: Option<ArchiveReader>,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Result<Self> {
        let index = LogfileIndex::new(&config).await?;
        let pos = Cursor::create_from(start, end, None);
        Ok(Self { index, source: None, head, pos, config })
    }

    /// Attempt to open a source if we don't already have one, return true
    /// on success. If the source is already open, just return true
    fn source(&mut self) -> Result<bool> {
        if self.source.is_some() {
            Ok(true)
        } else {
            self.source = DataSource::new(&self.config, self.index.first(), &self.head)?;
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
                    self.source =
                        DataSource::new(&self.config, self.index.next(f), &self.head)?;
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
                if s(&result) {
                    break Ok(result);
                } else {
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

    /// read a batch of deltas from the current data source, if it exists. If no data source can be opened
    /// then the outer result will be error. If reading the current data source fails then the inner result
    /// will be error.
    ///
    /// This function will automatically move to the next file in the collection as long as it's timestamp is
    /// within the bounds.
    pub fn read_deltas(
        &mut self,
        read_count: usize,
    ) -> Result<Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>> {
        self.apply_read(
            |archive, cursor| archive.read_deltas(cursor, read_count),
            |batch| batch.is_empty(),
            Pooled::orphan(VecDeque::new()),
        )
    }

    /// read the next batch after the current cursor position, moving
    /// to the next file if necessary.
    pub fn read_next(
        &mut self,
    ) -> Result<Option<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>> {
        self.apply_read(
            |archive, cursor| archive.read_next(cursor),
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
    pub fn reimage(&mut self) -> Result<Pooled<FxHashMap<Id, Event>>> {
        if self.source()? {
            task::block_in_place(|| {
                let ds = self.source.as_mut().unwrap();
                ds.archive.build_image(&self.pos)
            })
        } else {
            Ok(IMG_POOL.take())
        }
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
                    self.source = DataSource::new(&self.config, file, &self.head)?;
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
                        self.source = DataSource::new(&self.config, file, &self.head)?;
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
                        self.source =
                            DataSource::new(&self.config, File::Head, &self.head)?;
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
                            self.source =
                                DataSource::new(&self.config, file, &self.head)?;
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
                    self.source = DataSource::new(&self.config, file, &self.head)?;
                }
                if let Some(ds) = self.source.as_ref() {
                    ds.archive.seek(&mut self.pos, Seek::Absolute(ts))
                }
            }
        }
        Ok(())
    }
}
