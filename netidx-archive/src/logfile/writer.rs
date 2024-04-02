use super::{
    reader::ArchiveIndex, scan_file, ArchiveReader, BatchItem, CompressionHeader,
    FileHeader, Id, MonotonicTimestamper, PathMapping, RecordHeader, RecordIndex,
    RecordTooLarge, RecordTyp, Timestamp, COMMITTED_OFFSET, FILE_VERSION, MAX_RECORD_LEN,
    PM_POOL,
};
use anyhow::Result;
use bytes::BufMut;
use chrono::prelude::*;
use fs3::{allocation_granularity, FileExt};
use fxhash::{FxBuildHasher, FxHashSet, FxHashMap};
use indexmap::IndexMap;
use log::warn;
use memmap2::{Mmap, MmapMut};
use netidx::{pack::Pack, path::Path, pool::Pooled};
use parking_lot::RwLock;
use std::{
    self,
    cmp::max,
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    iter::IntoIterator,
    mem,
    ops::Drop,
    path::Path as FilePath,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

/// This reads and writes the netidx archive format (as written by the
/// "record" command in the tools). The archive format is intended to
/// be a compact format for storing recordings of netidx data for long
/// term storage and access. It uses memory mapped IO for performance
/// and memory efficiency, and as such file size is limited to
/// `usize`.
///
/// Files begin with a file header, which consists of the string
/// "netidx archive" followed by the file format
/// version. Currently there is 1 version, and the version number
/// is 0.
///
/// Following the header are a series of records. Every record begins
/// with a (RecordHeader)[RecordHeader], which is followed by a data
/// item, except in the case of the end of archive record, which is
/// not followed by a data item.
///
/// Items are written to the file using a two phase commit scheme to
/// allow detection of possibly corrupted data. Initially, items are
/// marked as uncommitted, and only upon a successful flush to disk
/// are they then marked as committed.
///
/// When an archive is opened read-only, an index of it's contents is
/// built in memory so that any part of it can be accessed quickly by
/// timestamp. As a result, there is some memory overhead.
///
/// In order to facilitate full reconstruction of the state at any
/// point without requiring to decode the entire file up to that point
/// there are two types of data records, image records contain the
/// entire state of every archived value at a given time, and delta
/// records contain only values that changed since the last delta
/// record. The full state of the values can be constructed at a given
/// time `t` by seeking to the nearest image record that is before
/// `t`, and then processing all the delta records up to `t`.
///
/// Because data sets vary in requirements and size the writing of
/// image records is configurable in the archiver (e.g. write 1 image
/// per 512 MiB of deltas), and it is not required to write any image
/// records, however this will mean that reconstructing the state at
/// any point will require processing the entire file before that
/// point.
///
/// To prevent data corruption the underling file is locked for
/// exclusive access using the advisory file locking mechanism present
/// in the OS (e.g. flock on unix). If the file is modified
/// independent of advisory locking it could cause data corruption.
///
/// The record header is 8 bytes. A data record starts with a LEB128
/// encoded item counter, and then a number of items. Path ids are
/// also LEB128 encoded. So, for example, in an archive containing 1
/// path, a batch with 1 u64 data item would look like.
///    
/// 8 byte header
/// 1 byte item count
/// 1 byte path id
/// 1 byte type tag
/// 8 byte u64
/// ----------------
/// 19 bytes (11 bytes of overhead 57%)
///
/// Better overheads can be achieved with larger batches, as should
/// naturally happen on busier systems. For example a batch of 128
/// u64s looks like.
///
/// 8 byte header
/// 1 byte item count
/// (1 byte path id
///  1 byte type tag
///  8 byte u64) * 128
/// ---------------------
/// 1289 bytes (264 bytes of overhead 20%)
pub struct ArchiveWriter {
    time: MonotonicTimestamper,
    path_by_id: IndexMap<Id, Path, FxBuildHasher>,
    id_by_path: FxHashMap<Path, Id>,
    file: Arc<File>,
    _external_lock: Option<Arc<File>>,
    end: Arc<AtomicUsize>,
    committed: usize,
    next_id: u32,
    block_size: usize,
    mmap: MmapMut,
    indexed: bool,
    index: FxHashSet<Id>,
    index_vec: Vec<Id>,
}

impl Drop for ArchiveWriter {
    fn drop(&mut self) {
        let _ = self.flush();
        let _ = self.mmap.flush(); // for the committed header
    }
}

impl ArchiveWriter {
    pub(super) fn open_full(
        path: impl AsRef<FilePath>,
        indexed: bool,
        compress: Option<Vec<u8>>,
        external_lock: Option<impl AsRef<FilePath>>,
    ) -> Result<Self> {
        if mem::size_of::<usize>() < mem::size_of::<u64>() {
            warn!("archive file size is limited to 4 GiB on this platform")
        }
        let time = MonotonicTimestamper::new();
        let external_lock = if let Some(path) = external_lock {
            let lock = if FilePath::is_file(path.as_ref()) {
                OpenOptions::new().read(true).write(true).open(path.as_ref())?
            } else {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(path.as_ref())?
            };
            lock.try_lock_exclusive()?;
            Some(Arc::new(lock))
        } else {
            None
        };
        if FilePath::is_file(path.as_ref()) {
            if compress.is_some() {
                bail!("can't write to an already compressed file")
            }
            let mut time_basis = DateTime::<Utc>::MIN_UTC;
            let file = OpenOptions::new().read(true).write(true).open(path.as_ref())?;
            if external_lock.is_none() {
                file.try_lock_exclusive()?;
            }
            let block_size = allocation_granularity(path)? as usize;
            let mmap = unsafe { MmapMut::map_mut(&file)? };
            let mut t = ArchiveWriter {
                time,
                path_by_id: IndexMap::with_hasher(FxBuildHasher::default()),
                id_by_path: HashMap::default(),
                file: Arc::new(file),
                _external_lock: external_lock,
                end: Arc::new(AtomicUsize::new(0)),
                committed: 0,
                next_id: 0,
                block_size,
                mmap,
                indexed: false,
                index: HashSet::default(),
                index_vec: Vec::new(),
            };
            let mut compress = None;
            let end = scan_file(
                &mut t.indexed,
                &mut compress,
                &mut t.path_by_id,
                &mut t.id_by_path,
                None,
                None,
                &mut time_basis,
                &mut t.next_id,
                &mut &*t.mmap,
            )?;
            if compress.is_some() {
                bail!("can't write to an already compressed file")
            }
            t.next_id += 1;
            t.end.store(end, Ordering::Relaxed);
            t.committed = end;
            Ok(t)
        } else {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(path.as_ref())?;
            if external_lock.is_none() {
                file.try_lock_exclusive()?;
            }
            let block_size = allocation_granularity(path.as_ref())? as usize;
            let fh_len = <FileHeader as Pack>::const_encoded_len().unwrap();
            let rh_len = <RecordHeader as Pack>::const_encoded_len().unwrap();
            let comp_hdr = compress.map(|dictionary| CompressionHeader { dictionary });
            let ch_len = comp_hdr.as_ref().map(|ch| ch.encoded_len()).unwrap_or(0);
            file.set_len(max(block_size, fh_len + rh_len + ch_len) as u64)?;
            let mut mmap = unsafe { MmapMut::map_mut(&file)? };
            let mut buf = &mut *mmap;
            let committed = (fh_len + ch_len) as u64;
            let fh = FileHeader {
                compressed: comp_hdr.is_some(),
                indexed,
                version: FILE_VERSION,
                committed,
            };
            <FileHeader as Pack>::encode(&fh, &mut buf)?;
            if let Some(hdr) = comp_hdr {
                hdr.encode(&mut buf)?;
            }
            mmap.flush()?;
            Ok(ArchiveWriter {
                time,
                path_by_id: IndexMap::with_hasher(FxBuildHasher::default()),
                id_by_path: HashMap::default(),
                file: Arc::new(file),
                _external_lock: external_lock,
                end: Arc::new(AtomicUsize::new(committed as usize)),
                committed: committed as usize,
                next_id: 0,
                block_size,
                mmap,
                indexed: true,
                index: HashSet::default(),
                index_vec: Vec::new(),
            })
        }
    }

    /// Open the specified archive for read/write access, if the file
    /// does not exist then a new archive will be created.
    pub fn open(path: impl AsRef<FilePath>) -> Result<Self> {
        Self::open_full(path, true, None, None::<&str>)
    }

    /// Open the specified archive with an external sentinel file
    /// for excusive lock.  It is intended to be used so that an external
    /// program can write to an archive while [netidx record] reads and
    /// publishes them at the same time.
    ///
    /// THIS IS POTENTIALLY DANGEROUS!  The protection against more than
    /// one writer, and therefore data corruption, is weaker than with
    /// [open], since writers have to agree on the same external lock
    /// filename.
    pub fn open_external(
        path: impl AsRef<FilePath>,
        external_lock: impl AsRef<FilePath>,
    ) -> Result<Self> {
        Self::open_full(path, true, None, Some(external_lock))
    }

    // remap the file reserving space for at least additional_capacity bytes
    fn reserve(&mut self, additional_capacity: usize) -> Result<()> {
        let len = self.mmap.len();
        let new_len = len + max(len >> 6, additional_capacity);
        let new_blocks = (new_len / self.block_size as usize) + 1;
        let new_size = new_blocks * self.block_size as usize;
        self.file.set_len(new_size as u64)?;
        Ok(drop(mem::replace(&mut self.mmap, unsafe { MmapMut::map_mut(&*self.file)? })))
    }

    fn check_reserve(&mut self, record_length: usize) -> Result<usize> {
        if record_length > MAX_RECORD_LEN as usize {
            bail!(RecordTooLarge);
        }
        let len = <RecordHeader as Pack>::const_encoded_len().unwrap() + record_length;
        if self.mmap.len() - self.end.load(Ordering::Relaxed) < len {
            self.reserve(len)?;
        }
        Ok(len)
    }

    /// flush uncommitted changes to disk, mark all flushed records as
    /// committed, and update the end of archive marker. Does nothing
    /// if everything is already committed.
    pub fn flush(&mut self) -> Result<()> {
        let end = self.end.load(Ordering::Relaxed);
        if self.committed < end {
            self.mmap.flush()?; // first stage commit
            let mut buf = &mut self.mmap[COMMITTED_OFFSET..];
            buf.put_u64(end as u64);
            self.mmap.flush()?; // second stage commit
            self.committed = end;
        }
        Ok(())
    }

    /// allocate path ids for any of the specified paths that don't
    /// already have one, and write a path mappings record containing
    /// the new assignments.
    pub fn add_paths<'a>(
        &'a mut self,
        paths: impl IntoIterator<Item = &'a Path>,
    ) -> Result<()> {
        let mut pms = PM_POOL.take();
        for path in paths {
            if !self.id_by_path.contains_key(path) {
                let id = Id(self.next_id);
                self.next_id += 1;
                self.id_by_path.insert(path.clone(), id);
                self.path_by_id.insert(id, path.clone());
                pms.push(PathMapping(path.clone(), id));
            }
        }
        self.add_raw_pathmappings(pms)
    }

    pub(super) fn add_raw_pathmappings(
        &mut self,
        pms: Pooled<Vec<PathMapping>>,
    ) -> Result<()> {
        if pms.len() > 0 {
            let record_length = <Pooled<Vec<PathMapping>> as Pack>::encoded_len(&pms);
            let len = self.check_reserve(record_length)?;
            let end = self.end.load(Ordering::Relaxed);
            let mut buf = &mut self.mmap[end..];
            let rh = RecordHeader {
                record_type: RecordTyp::PathMappings,
                record_length: record_length as u32,
                timestamp: 0,
            };
            <RecordHeader as Pack>::encode(&rh, &mut buf)?;
            <Pooled<Vec<PathMapping>> as Pack>::encode(&pms, &mut buf)?;
            self.end.fetch_add(len, Ordering::AcqRel);
        }
        Ok(())
    }

    fn add_batch_f<F: FnOnce(&mut &mut [u8]) -> Result<()>>(
        &mut self,
        image: bool,
        timestamp: Timestamp,
        record_length: usize,
        f: F,
    ) -> Result<()> {
        if record_length > MAX_RECORD_LEN as usize {
            bail!(RecordTooLarge)
        }
        match timestamp {
            Timestamp::Offset(_) => (),
            Timestamp::NewBasis(basis) => {
                let record_length = <DateTime<Utc> as Pack>::encoded_len(&basis);
                let rh = RecordHeader {
                    record_type: RecordTyp::Timestamp,
                    record_length: record_length as u32,
                    timestamp: 0,
                };
                let len = self.check_reserve(record_length)?;
                let mut buf = &mut self.mmap[self.end.load(Ordering::Relaxed)..];
                <RecordHeader as Pack>::encode(&rh, &mut buf)?;
                <DateTime<Utc> as Pack>::encode(&basis, &mut buf)?;
                self.end.fetch_add(len, Ordering::AcqRel);
            }
        }
        let len = self.check_reserve(record_length)?;
        let mut buf = &mut self.mmap[self.end.load(Ordering::Relaxed)..];
        let rh = RecordHeader {
            record_type: if image {
                RecordTyp::ImageBatch
            } else {
                RecordTyp::DeltaBatch
            },
            record_length: record_length as u32,
            timestamp: timestamp.offset(),
        };
        <RecordHeader as Pack>::encode(&rh, &mut buf)?;
        f(&mut buf)?;
        self.end.fetch_add(len, Ordering::AcqRel);
        Ok(())
    }

    /// Add a data batch to the archive. If `image` is true then it
    /// will be marked as an image batch, and should contain a value
    /// for every subscriped path whether it changed or not, otherwise
    /// it will be marked as a delta batch, and should contain only
    /// values that changed since the last delta batch. This method
    /// will fail if any of the path ids in the batch are unknown.
    ///
    /// batch timestamps are monotonicly increasing, with the
    /// granularity of 1us. As such, one should avoid writing
    /// "spurious" batches, and generally for efficiency and
    /// correctness write as few batches as possible.
    pub fn add_batch(
        &mut self,
        image: bool,
        timestamp: DateTime<Utc>,
        batch: &Pooled<Vec<BatchItem>>,
    ) -> Result<()> {
        if batch.len() > 0 {
            let timestamp = self.time.timestamp(timestamp);
            let index = if self.indexed {
                if !image {
                    for BatchItem(id, _) in batch.iter() {
                        self.index.insert(*id);
                    }
                }
                self.index_vec.extend(self.index.drain());
                Some(RecordIndex { index: mem::replace(&mut self.index_vec, vec![]) })
            } else {
                None
            };
            let index_length = index
                .as_ref()
                .map(|i| <RecordIndex as Pack>::encoded_len(i))
                .unwrap_or(0);
            let record_length =
                index_length + <Pooled<Vec<BatchItem>> as Pack>::encoded_len(&batch);
            self.add_batch_f(image, timestamp, record_length, |buf| {
                if let Some(index) = &index {
                    <RecordIndex as Pack>::encode(index, buf)?
                }
                Ok(<Pooled<Vec<BatchItem>> as Pack>::encode(&batch, buf)?)
            })?;
            if let Some(index) = index {
                self.index_vec = index.index;
                self.index_vec.clear()
            }
        }
        Ok(())
    }

    // this is used to build a compressed archive
    pub(super) fn add_batch_raw(
        &mut self,
        image: bool,
        timestamp: DateTime<Utc>,
        batch: &[u8],
    ) -> Result<()> {
        use std::io::Write;
        if batch.len() > 0 {
            let timestamp = self.time.timestamp(timestamp);
            self.add_batch_f(image, timestamp, batch.len(), |buf| {
                Ok(BufMut::writer(buf).write_all(batch)?)
            })?
        }
        Ok(())
    }

    pub fn id_for_path(&self, path: &Path) -> Option<Id> {
        self.id_by_path.get(path).copied()
    }

    pub fn path_for_id(&self, id: &Id) -> Option<&Path> {
        self.path_by_id.get(id)
    }

    pub fn capacity(&self) -> usize {
        self.mmap.len()
    }

    pub fn len(&self) -> usize {
        self.end.load(Ordering::Relaxed)
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Create an archive reader from this writer by creating a
    /// read-only duplicate of the memory map.
    ///
    /// If you need lots of readers it's best to create just one using
    /// this method, and then clone it, that way the same memory map
    /// can be shared by all the readers.
    pub fn reader(&self) -> Result<ArchiveReader> {
        Ok(ArchiveReader {
            index: Arc::new(RwLock::new(ArchiveIndex::new())),
            compressed: None,
            indexed: self.indexed,
            file: self.file.clone(),
            end: self.end.clone(),
            mmap: Arc::new(RwLock::new(unsafe { Mmap::map(&*self.file)? })),
        })
    }
}
