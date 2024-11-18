use super::{
    arraymap::ArrayMap, scan_file, scan_header, scan_records, ArchiveWriter, BatchItem, Cursor,
    FileHeader, Id, PathMapping, RecordHeader, Seek, CURSOR_BATCH_POOL, IMG_POOL, PM_POOL,
};
use anyhow::{Context, Result};
use bytes::{Buf, BufMut};
use chrono::prelude::*;
use fs3::FileExt;
use fxhash::{FxBuildHasher, FxHashMap, FxHashSet};
use indexmap::IndexMap;
use log::{error, info};
use memmap2::Mmap;
use netidx::{
    pack::{decode_varint, varint_len, Pack},
    path::Path,
    pool::Pooled,
    subscriber::Event,
};
use parking_lot::{
    lock_api::{RwLockUpgradableReadGuard, RwLockWriteGuard},
    Mutex, RwLock, RwLockReadGuard,
};
use std::{
    self,
    cell::RefCell,
    cmp::max,
    collections::{BTreeMap, HashMap, VecDeque},
    fmt,
    fs::{File, OpenOptions},
    iter::IntoIterator,
    mem,
    ops::Bound,
    path::Path as FilePath,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::task::{self, JoinSet};
use zstd::bulk::{Compressor, Decompressor};

#[derive(Debug)]
pub struct ArchiveIndex {
    path_by_id: IndexMap<Id, Path, FxBuildHasher>,
    id_by_path: FxHashMap<Path, Id>,
    imagemap: ArrayMap<DateTime<Utc>, usize>,
    deltamap: ArrayMap<DateTime<Utc>, usize>,
    time_basis: DateTime<Utc>,
    end: usize,
}

impl ArchiveIndex {
    pub(super) fn new() -> Self {
        ArchiveIndex {
            path_by_id: IndexMap::with_hasher(FxBuildHasher::default()),
            id_by_path: HashMap::default(),
            imagemap: ArrayMap::new(),
            deltamap: ArrayMap::new(),
            time_basis: DateTime::<Utc>::MIN_UTC,
            end: <FileHeader as Pack>::const_encoded_len().unwrap(),
        }
    }

    /// iterate over the pathmap
    pub fn iter_pathmap(&self) -> impl Iterator<Item = (&Id, &Path)> {
        self.path_by_id.iter()
    }

    /// look up the path for a given id
    pub fn path_for_id(&self, id: &Id) -> Option<&Path> {
        self.path_by_id.get(id)
    }

    /// look up the id of a given path
    pub fn id_for_path(&self, path: &Path) -> Option<&Id> {
        self.id_by_path.get(path)
    }

    pub fn deltamap(&self) -> &ArrayMap<DateTime<Utc>, usize> {
        &self.deltamap
    }

    pub fn imagemap(&self) -> &ArrayMap<DateTime<Utc>, usize> {
        &self.imagemap
    }

    /// check if the specifed timestamp could be in the file, meaning
    /// it is equal or after the start and before or equal to the end
    pub fn check_in_file(&self, ts: DateTime<Utc>) -> bool {
        match (
            self.deltamap.first_key_value(),
            self.deltamap.last_key_value(),
        ) {
            (Some((fst, _)), Some((lst, _))) => *fst <= ts && ts <= *lst,
            (_, _) => false,
        }
    }

    /// check if the speficied cursor has any overlap with the records
    /// in the file.
    pub fn has_overlap(&self, cursor: &Cursor) -> bool {
        match (
            self.deltamap.first_key_value(),
            self.deltamap.last_key_value(),
        ) {
            (Some((fst, _)), Some((lst, _))) => {
                let start = match cursor.start {
                    Bound::Unbounded => true,
                    Bound::Included(ts) => ts <= *lst,
                    Bound::Excluded(ts) => ts < *lst,
                };
                let end = match cursor.end {
                    Bound::Unbounded => true,
                    Bound::Included(ts) => ts >= *fst,
                    Bound::Excluded(ts) => ts > *fst,
                };
                start && end
            }
            (_, _) => false,
        }
    }

    /// seek the specified number of batches forward or back in the
    /// file. Return the number of batches moved.
    pub fn seek_steps(&self, cursor: &mut Cursor, steps: i8) -> i8 {
        let mut moved = 0;
        if steps >= 0 {
            let init = cursor.current.map(Bound::Excluded).unwrap_or(cursor.start);
            let mut iter = self.deltamap.range((init, cursor.end));
            for _ in 0..steps {
                match iter.next() {
                    None => break,
                    Some((ts, _)) => {
                        moved += 1;
                        cursor.set_current(*ts);
                        if cursor.at_end() {
                            break;
                        }
                    }
                }
            }
        } else {
            let init = cursor.current.map(Bound::Excluded).unwrap_or(cursor.end);
            let mut iter = self.deltamap.range((cursor.start, init));
            for _ in 0..(steps as i16).abs() {
                match iter.next_back() {
                    None => break,
                    Some((ts, _)) => {
                        moved -= 1;
                        cursor.set_current(*ts);
                        if cursor.at_start() {
                            break;
                        }
                    }
                }
            }
        }
        moved
    }

    /// Seek relative to the current position
    pub fn seek_time_relative(
        &self,
        cursor: &mut Cursor,
        offset: chrono::Duration,
    ) -> (bool, DateTime<Utc>) {
        let ts = match cursor.current() {
            Some(ts) => ts,
            None => self
                .deltamap
                .keys()
                .next()
                .copied()
                .unwrap_or(Utc.from_utc_datetime(&NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                    NaiveTime::from_hms_opt(0, 0, 1).unwrap(),
                ))),
        };
        let new_ts = ts + offset;
        cursor.set_current(new_ts);
        let new_ts = cursor.current.unwrap();
        (self.check_in_file(new_ts), new_ts)
    }
}

#[derive(Clone)]
pub struct ArchiveReader {
    pub(super) index: Arc<RwLock<ArchiveIndex>>,
    pub(super) compressed: Option<Arc<Mutex<Decompressor<'static>>>>,
    pub(super) file: Arc<File>,
    pub(super) end: Arc<AtomicUsize>,
    pub(super) indexed: bool,
    pub(super) mmap: Arc<RwLock<Mmap>>,
}

impl fmt::Debug for ArchiveReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArchiveReader({:?})", &self.file)
    }
}

impl ArchiveReader {
    /// Open the specified archive read only. Note, it is possible to
    /// read and write to an archive simultaneously, however to do so
    /// you must open an [ArchiveWriter](ArchiveWriter) and then use
    /// the [ArchiveWriter::reader](ArchiveWriter::reader) method to
    /// get a reader.
    pub fn open(path: impl AsRef<FilePath>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(path.as_ref())
            .context("open file")?;
        file.try_lock_shared()?;
        Self::open_with(Arc::new(file))
    }

    fn open_with(file: Arc<File>) -> Result<Self> {
        let mmap = unsafe { Mmap::map(&*file).context("mmap file")? };
        let mut index = ArchiveIndex::new();
        let mut max_id = 0;
        let mut compressed = None;
        let mut indexed = false;
        let end = scan_file(
            &mut indexed,
            &mut compressed,
            &mut index.path_by_id,
            &mut index.id_by_path,
            Some(&mut index.imagemap),
            Some(&mut index.deltamap),
            &mut index.time_basis,
            &mut max_id,
            &mut &*mmap,
        )
        .context("scan file")?;
        index.end = end;
        let compressed = compressed
            .map(|dict| {
                let dc = Decompressor::with_dictionary(&dict.dictionary)
                    .context("create decompressor")?;
                Ok::<_, anyhow::Error>(Arc::new(Mutex::new(dc)))
            })
            .transpose()?;
        Ok(ArchiveReader {
            index: Arc::new(RwLock::new(index)),
            indexed,
            compressed,
            file,
            end: Arc::new(AtomicUsize::new(end)),
            mmap: Arc::new(RwLock::new(mmap)),
        })
    }

    pub fn is_compressed(&self) -> bool {
        self.compressed.is_some()
    }

    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    pub(crate) fn strong_count(&self) -> usize {
        Arc::strong_count(&self.index)
    }

    pub fn index(&self) -> RwLockReadGuard<ArchiveIndex> {
        self.index.read()
    }

    pub fn capacity(&self) -> usize {
        self.mmap.read().len()
    }

    pub fn delta_batches(&self) -> usize {
        self.index.read().deltamap.len()
    }

    pub fn image_batches(&self) -> usize {
        self.index.read().imagemap.len()
    }

    /// Only use this if `ArchiveReader` was opened alone and not from
    /// an `ArchiveWriter`.
    pub fn reopen(&mut self) -> Result<()> {
        let t = Self::open_with(self.file.clone())?;
        *self = t;
        Ok(())
    }

    /// Check if the memory map needs to be remapped due to growth,
    /// and check if additional records exist that need to be
    /// indexed. This method is only relevant if this `ArchiveReader`
    /// was created from an `ArchiveWriter`, this method is called
    /// automatically by `read_deltas` and `build_image`.
    pub fn check_remap_rescan(&self, force: bool) -> Result<()> {
        let end = self.end.load(Ordering::Relaxed);
        let mmap = self.mmap.upgradable_read();
        let mmap = if end > mmap.len() || force {
            let mut mmap = RwLockUpgradableReadGuard::upgrade(mmap);
            drop(mem::replace(&mut *mmap, unsafe { Mmap::map(&*self.file)? }));
            RwLockWriteGuard::downgrade_to_upgradable(mmap)
        } else {
            mmap
        };
        if force {
            // rescan header to find end
            let header = scan_header(&mut &mmap[..])?;
            self.end.store(header.committed as usize, Ordering::Relaxed);
        }
        let index = self.index.upgradable_read();
        if index.end < end {
            let mut index = RwLockUpgradableReadGuard::upgrade(index);
            let mut max_id = 0;
            let r = &mut *index;
            r.end = scan_records(
                &mut r.path_by_id,
                &mut r.id_by_path,
                Some(&mut r.imagemap),
                Some(&mut r.deltamap),
                &mut r.time_basis,
                &mut max_id,
                end,
                r.end,
                &mut &mmap[r.end..end],
            )?;
        }
        Ok(())
    }

    /// Move the cursor according to the `Seek` instruction. If the
    /// cursor has no current position then positive offsets begin at
    /// the cursor start, and negative offsets begin at the cursor
    /// end. If the seek instruction would move the cursor out of
    /// bounds, then it will move to the closest in bounds position.
    pub fn seek(&self, cursor: &mut Cursor, seek: Seek) {
        match seek {
            Seek::Beginning => {
                cursor.current = None;
            }
            Seek::End => match self.index.read().deltamap.keys().next_back() {
                None => {
                    cursor.current = None;
                }
                Some(ts) => {
                    cursor.set_current(*ts);
                }
            },
            Seek::Absolute(ts) => {
                let index = self.index.read();
                let first = index.deltamap.keys().next();
                let last = index.deltamap.keys().next_back();
                match (first, last) {
                    (Some(fst), Some(lst)) => {
                        if ts < *fst {
                            cursor.set_current(*fst);
                        } else if ts > *lst {
                            cursor.set_current(*lst);
                        } else {
                            cursor.set_current(ts)
                        }
                    }
                    (_, _) => cursor.set_current(ts),
                }
            }
            Seek::TimeRelative(offset) => {
                let index = self.index.read();
                let (ok, ts) = index.seek_time_relative(cursor, offset);
                if !ok {
                    let first = index.deltamap.keys().next();
                    let last = index.deltamap.keys().next_back();
                    match (first, last) {
                        (Some(first), Some(last)) => {
                            if ts < *first {
                                cursor.set_current(*first);
                            } else {
                                cursor.set_current(*last);
                            }
                        }
                        (None, _) | (_, None) => (),
                    }
                }
            }
            Seek::BatchRelative(steps) => {
                self.index.read().seek_steps(cursor, steps);
            }
        }
    }

    fn scan_index_at(
        index: &FxHashSet<Id>,
        compressed: bool,
        mmap: &Mmap,
        pos: usize,
        end: usize,
    ) -> Result<bool> {
        if pos >= end {
            bail!("record out of bounds")
        }
        let mut buf = &mmap[pos..];
        let rh = <RecordHeader as Pack>::decode(&mut buf).context("reading record header")?;
        if pos + rh.record_length as usize > end {
            bail!("get_batch: error truncated record at {}", pos);
        }
        if compressed {
            buf.advance(4);
        }
        let index_len = decode_varint(&mut buf).context("decoding index length")?;
        let mut buf = buf.take(index_len as usize - varint_len(index_len));
        while buf.has_remaining() {
            let id = decode_varint(&mut buf).context("decoding index element")?;
            if index.contains(&Id(id as u32)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_batch_at(
        indexed: bool,
        compressed: &Option<Arc<Mutex<Decompressor>>>,
        mmap: &Mmap,
        pos: usize,
        end: usize,
    ) -> Result<(usize, Pooled<Vec<BatchItem>>)> {
        thread_local! {
            static BUF: RefCell<Vec<u8>> = RefCell::new(vec![]);
        }
        if pos >= end {
            bail!("record out of bounds")
        }
        let mut buf = &mmap[pos..];
        let rh = <RecordHeader as Pack>::decode(&mut buf).context("reading record header")?;
        if pos + rh.record_length as usize > end {
            bail!("get_batch: error truncated record at {}", pos);
        }
        let pos = pos + <RecordHeader as Pack>::const_encoded_len().unwrap();
        match compressed {
            None => {
                let index_len = if indexed {
                    decode_varint(&mut &mmap[pos..])? as usize
                } else {
                    0
                };
                let pos = pos + index_len;
                let batch = <Pooled<Vec<BatchItem>> as Pack>::decode(&mut &mmap[pos..])
                    .context("decoding batch")?;
                Ok((rh.record_length as usize, batch))
            }
            Some(dcm) => {
                let mut dcm = dcm.lock();
                BUF.with(|compression_buf| {
                    let mut compression_buf = compression_buf.borrow_mut();
                    let uncomp_len = Buf::get_u32(&mut &mmap[pos..]) as usize;
                    let pos = pos + 4;
                    let index_len = if indexed {
                        decode_varint(&mut &mmap[pos..])? as usize
                    } else {
                        0
                    };
                    let pos = pos + index_len;
                    if compression_buf.len() < uncomp_len {
                        compression_buf.resize(uncomp_len, 0u8);
                    }
                    let comp_len = rh.record_length as usize - 4 - index_len;
                    let len = dcm
                        .decompress_to_buffer(&mmap[pos..pos + comp_len], &mut *compression_buf)
                        .context("decompressing to buffer")?;
                    let batch =
                        <Pooled<Vec<BatchItem>> as Pack>::decode(&mut &compression_buf[..len])?;
                    Ok((rh.record_length as usize, batch))
                })
            }
        }
    }

    /// Builds an image corresponding to the state at the cursor, or
    /// if the cursor has no current position then at the beginning of
    /// the cursor. If the cursor has no position and then beginning
    /// is Unbounded, then the image will be empty. If there are no
    /// images in the archive, then all the deltas between the
    /// beginning and the cursor position will be read, otherwise only
    /// the deltas between the closest image that is older, and the
    /// cursor start need to be read.
    pub fn build_image(
        &self,
        filter: Option<&FxHashSet<Id>>,
        cursor: &Cursor,
    ) -> Result<Pooled<FxHashMap<Id, Event>>> {
        self.check_remap_rescan(false)?;
        let pos = match cursor.current {
            None => cursor.start,
            Some(pos) => Bound::Included(pos),
        };
        match pos {
            Bound::Unbounded => Ok(Pooled::orphan(HashMap::default())),
            _ => {
                let mut image = IMG_POOL.take();
                let index = self.index.read();
                let mmap = self.mmap.read();
                let start = match index.imagemap.range((Bound::Unbounded, pos)).next_back() {
                    None => Bound::Unbounded,
                    Some((ts, pos)) => {
                        let (_, mut batch) = ArchiveReader::get_batch_at(
                            self.indexed,
                            &self.compressed,
                            &*mmap,
                            *pos,
                            index.end,
                        )?;
                        image.extend(batch.drain(..).filter_map(
                            |BatchItem(id, up)| match filter {
                                Some(set) if set.contains(&id) => Some((id, up)),
                                Some(_) => None,
                                None => Some((id, up)),
                            },
                        ));
                        Bound::Included(*ts)
                    }
                };
                let matched = Self::matching_idxs(
                    self.indexed,
                    self.compressed.is_some(),
                    &index,
                    &mmap,
                    filter,
                    start,
                    pos,
                );
                for (_, pos) in matched {
                    let (_, mut batch) = ArchiveReader::get_batch_at(
                        self.indexed,
                        &self.compressed,
                        &*mmap,
                        pos as usize,
                        index.end,
                    )?;
                    image.extend(
                        batch
                            .drain(..)
                            .filter_map(|BatchItem(id, up)| match filter {
                                Some(set) if set.contains(&id) => Some((id, up)),
                                Some(_) => None,
                                None => Some((id, up)),
                            }),
                    );
                }
                Ok(image)
            }
        }
    }

    fn matching_idxs<'a, 'b: 'a>(
        indexed: bool,
        compressed: bool,
        index: &'a ArchiveIndex,
        mmap: &'a Mmap,
        filter: Option<&'a FxHashSet<Id>>,
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> impl Iterator<Item = (DateTime<Utc>, usize)> + 'a {
        index
            .deltamap
            .range((start, end))
            .filter_map(move |(ts, pos)| match filter {
                Some(set) if indexed => {
                    match Self::scan_index_at(set, compressed, &*mmap, *pos, index.end) {
                        Ok(true) => Some((*ts, *pos)),
                        Ok(false) => None,
                        Err(e) => {
                            error!("failed to read index entry for {}, {:?}", ts, e);
                            None
                        }
                    }
                }
                None | Some(_) => Some((*ts, *pos)),
            })
    }

    /// read at most `n` delta items from the specified cursor, and
    /// advance it by the number of items read. The cursor will not be
    /// invalidated even if no items can be read, however depending on
    /// it's bounds it may never read any more items.
    pub fn read_deltas(
        &self,
        filter: Option<&FxHashSet<Id>>,
        cursor: &mut Cursor,
        n: usize,
    ) -> Result<(
        usize,
        Pooled<VecDeque<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>>,
    )> {
        self.check_remap_rescan(false)?;
        let mut res = CURSOR_BATCH_POOL.take();
        let start = match cursor.current {
            None => cursor.start,
            Some(dt) => Bound::Excluded(dt),
        };
        let mmap = self.mmap.read();
        let index = self.index.read();
        let mut current = cursor.current;
        let mut total = 0;
        let matched = Self::matching_idxs(
            self.indexed,
            self.compressed.is_some(),
            &*index,
            &*mmap,
            filter,
            start,
            cursor.end,
        )
        .take(n);
        for (ts, pos) in matched {
            let (len, batch) = ArchiveReader::get_batch_at(
                self.indexed,
                &self.compressed,
                &*mmap,
                pos as usize,
                index.end,
            )?;
            current = Some(ts);
            total += len;
            res.push_back((ts, batch));
        }
        cursor.current = current;
        Ok((total, res))
    }

    /// Read the next matching batch after the cursor position without
    /// changing the cursor position.
    pub fn read_next(
        &self,
        filter: Option<&FxHashSet<Id>>,
        cursor: &Cursor,
    ) -> Result<Option<(DateTime<Utc>, Pooled<Vec<BatchItem>>)>> {
        self.check_remap_rescan(false)?;
        let start = match cursor.current {
            None => cursor.start,
            Some(dt) => Bound::Excluded(dt),
        };
        let mmap = self.mmap.read();
        let index = self.index.read();
        let mut matched = Self::matching_idxs(
            self.indexed,
            self.compressed.is_some(),
            &*index,
            &*mmap,
            filter,
            start,
            cursor.end,
        )
        .take(1);
        match matched.next() {
            Some((ts, pos)) => {
                let (_, batch) = ArchiveReader::get_batch_at(
                    self.indexed,
                    &self.compressed,
                    &*mmap,
                    pos as usize,
                    index.end,
                )?;
                Ok(Some((ts, batch)))
            }
            None => Ok(None),
        }
    }

    fn train(&self) -> Result<(usize, Vec<u8>)> {
        let mmap = self.mmap.read();
        let mut lengths: Vec<usize> = Vec::new();
        let mut max_rec_len = 0;
        let fhl = FileHeader::const_encoded_len().unwrap();
        let rhl = RecordHeader::const_encoded_len().unwrap();
        let mut pos = fhl;
        let end = self.end.load(Ordering::Relaxed);
        while pos < end {
            let rh = RecordHeader::decode(&mut &mmap[pos..end])?;
            let len = rhl + rh.record_length as usize;
            max_rec_len = max(len, max_rec_len);
            lengths.push(len);
            pos += len;
        }
        let max_dict_len = end / 10;
        let max_dict_len = if max_dict_len == 0 { end } else { max_dict_len };
        let dict = zstd::dict::from_continuous(&mmap[fhl..end], &lengths[..], max_dict_len)?;
        info!("dictionary of size {} was trained", dict.len());
        Ok((max_rec_len, dict))
    }

    /// Write an indexed copy to the specified file. The copy will not
    /// be compressed, so if you wanted it compressed you will have to
    /// recompress it after writing the index.
    pub async fn build_index(&self, dest: impl AsRef<FilePath>) -> Result<()> {
        if self.indexed {
            bail!("file is already indexed")
        }
        self.check_remap_rescan(false)?;
        let mut unified_index: BTreeMap<DateTime<Utc>, (bool, usize)> = BTreeMap::new();
        let index = self.index.read();
        for (ts, pos) in index.deltamap.iter() {
            unified_index.insert(*ts, (false, *pos));
        }
        for (ts, pos) in index.imagemap.iter() {
            unified_index.insert(*ts, (true, *pos));
        }
        let mut output = ArchiveWriter::open_full(dest, true, None, None::<&str>)?;
        let mut pms = PM_POOL.take();
        for (id, path) in index.path_by_id.iter() {
            pms.push(PathMapping(path.clone(), *id));
        }
        output.add_raw_pathmappings(pms)?;
        let mmap = self.mmap.read();
        for (ts, (image, pos)) in unified_index.iter() {
            let (_, batch) = Self::get_batch_at(false, &self.compressed, &*mmap, *pos, index.end)?;
            output.add_batch(*image, *ts, &batch)?;
        }
        Ok(())
    }

    /// This function will create an archive with compressed batches
    /// and images. Compressed archives can be read as normal, but can
    /// no longer be written.
    pub async fn compress(&self, window: usize, dest: impl AsRef<FilePath>) -> Result<()> {
        struct CompJob {
            ts: DateTime<Utc>,
            image: bool,
            comp: Compressor<'static>,
            cbuf: Vec<u8>,
            pos: usize,
        }
        async fn compress_task(
            indexed: bool,
            mmap: Arc<RwLock<Mmap>>,
            mut job: CompJob,
        ) -> Result<CompJob> {
            task::block_in_place(|| {
                let mmap = mmap.read();
                let pos = job.pos;
                let rh = RecordHeader::decode(&mut &mmap[pos..])?;
                let pos = pos + RecordHeader::const_encoded_len().unwrap();
                let end = pos + rh.record_length as usize;
                (&mut job.cbuf[0..4]).put_u32(rh.record_length);
                let index_len = if indexed {
                    let index_len = decode_varint(&mut &mmap[pos..])? as usize;
                    (&mut job.cbuf[4..4 + index_len]).put_slice(&mmap[pos..pos + index_len]);
                    index_len
                } else {
                    0
                };
                let pos = pos + index_len;
                let len = job
                    .comp
                    .compress_to_buffer(&mmap[pos..end], &mut job.cbuf[4 + index_len..])
                    .context("compress to buffer")?;
                job.pos = len + index_len + 4;
                Ok(job)
            })
        }
        if self.compressed.is_some() {
            bail!("archive is already compressed")
        }
        self.check_remap_rescan(false)?;
        let (max_len, dict) = self.train()?;
        let pdict = Box::leak(Box::new(zstd::dict::EncoderDictionary::copy(&dict, 19))) as *mut _;
        let f = move || async move {
            let mut unified_index: BTreeMap<DateTime<Utc>, (bool, usize)> = BTreeMap::new();
            let index = self.index.read();
            for (ts, pos) in index.deltamap.iter() {
                unified_index.insert(*ts, (false, *pos));
            }
            for (ts, pos) in index.imagemap.iter() {
                unified_index.insert(*ts, (true, *pos));
            }
            let mut output =
                ArchiveWriter::open_full(dest, self.indexed, Some(dict), None::<&str>)?;
            let mut pms = PM_POOL.take();
            for (id, path) in index.path_by_id.iter() {
                pms.push(PathMapping(path.clone(), *id));
            }
            output.add_raw_pathmappings(pms)?;
            let ncpus = num_cpus::get();
            let mut compjobs = (0..ncpus * window)
                .into_iter()
                .map(|_| {
                    Ok(CompJob {
                        ts: DateTime::<Utc>::MIN_UTC,
                        cbuf: vec![0u8; max_len * 2],
                        comp: Compressor::with_prepared_dictionary(unsafe { &*pdict })?,
                        image: false,
                        pos: 0,
                    })
                })
                .collect::<Result<Vec<CompJob>>>()?;
            let mut running_jobs: JoinSet<Result<CompJob>> = JoinSet::new();
            let mut commitq: BTreeMap<DateTime<Utc>, Option<CompJob>> = BTreeMap::new();
            let mut index_iter = unified_index.iter();
            'main: loop {
                while compjobs.is_empty() {
                    let job: CompJob = match running_jobs.join_next().await {
                        None => break 'main,
                        Some(res) => res??,
                    };
                    commitq.insert(job.ts, Some(job));
                    while let Some(mut ent) = commitq.first_entry() {
                        match ent.get_mut().take() {
                            None => break,
                            Some(job) => {
                                ent.remove();
                                output
                                    .add_batch_raw(job.image, job.ts, &job.cbuf[0..job.pos])
                                    .context("add raw batch")?;
                                compjobs.push(job);
                            }
                        }
                    }
                }
                match index_iter.next() {
                    None => compjobs.clear(),
                    Some((ts, (image, pos))) => {
                        let mut job = compjobs.pop().unwrap();
                        job.ts = *ts;
                        job.image = *image;
                        job.pos = *pos;
                        commitq.insert(*ts, None);
                        running_jobs.spawn(compress_task(
                            self.indexed,
                            Arc::clone(&self.mmap),
                            job,
                        ));
                    }
                }
            }
            output.flush()?;
            Ok(())
        };
        let res = f().await;
        // this is safe because everying using pidict has been awaited
        // and dropped before this point by f
        mem::drop(unsafe { Box::from_raw(pdict) });
        res
    }
}
